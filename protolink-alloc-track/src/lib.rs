//! Allocation accounting for tests, on the measuring thread.
//!
//! [`CountingAllocator`] forwards to the system allocator. Inside a [`Window`],
//! it records successful allocations and reallocations, and the requested bytes
//! of each block still live. Blocks allocated before the window (inputs, peers,
//! harness state) are untracked, so freeing them inside the window does not
//! change the live count.
//!
//! This crate does not install the allocator. A binary has one global
//! allocator, so each test binary that measures allocation declares it once:
//!
//! ```
//! use protolink_alloc_track::{CountingAllocator, Window};
//!
//! #[global_allocator]
//! static ALLOCATOR: CountingAllocator = CountingAllocator;
//!
//! let window = Window::open();
//! let block = std::hint::black_box(vec![0u8; 64]);
//! assert!(window.checkpoint().allocations >= 1);
//! drop(block);
//! ```
//!
//! Accounting contract:
//!
//! - `live` is the sum of requested bytes of the currently tracked live blocks.
//! - `peak` is the highest logical `live` total observed during a phase,
//!   including the total at the phase's starting checkpoint.
//! - A successful realloc replaces the old block's bytes with the new size,
//!   once, whether the block moved or was resized in place.
//! - Successfully reallocating an untracked block brings its full new size into
//!   tracking. Freeing an untracked block remains a no-op.
//! - Allocator metadata, rounding, and any transient overlap inside a moving
//!   realloc are not measured.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;

/// Blocks tracked at once. The scenarios here hold only a few.
const TRACKED: usize = 16;

#[derive(Clone, Copy)]
struct Block {
    addr: usize,
    size: usize,
}

#[derive(Clone, Copy)]
struct Tracker {
    active: bool,
    blocks: [Option<Block>; TRACKED],
    live: usize,
    peak: usize,
    allocations: usize,
    reallocations: usize,
    largest: usize,
    overflowed: bool,
}

impl Tracker {
    const IDLE: Self = Self {
        active: false,
        blocks: [None; TRACKED],
        live: 0,
        peak: 0,
        allocations: 0,
        reallocations: 0,
        largest: 0,
        overflowed: false,
    };

    fn allocated(&mut self, addr: usize, size: usize) {
        if self.active {
            self.allocations += 1;
            self.largest = self.largest.max(size);
            self.insert(addr, size);
        }
    }

    fn freed(&mut self, addr: usize) {
        if self.active {
            self.remove(addr);
        }
    }

    /// The old block leaves the live total before the new size enters it, so
    /// the peak is taken from the corrected total in `insert`.
    fn resized(&mut self, old: usize, new: usize, size: usize) {
        if self.active {
            self.reallocations += 1;
            self.largest = self.largest.max(size);
            self.remove(old);
            self.insert(new, size);
        }
    }

    fn insert(&mut self, addr: usize, size: usize) {
        self.live += size;
        self.peak = self.peak.max(self.live);
        match self.blocks.iter_mut().find(|slot| slot.is_none()) {
            Some(slot) => *slot = Some(Block { addr, size }),
            None => self.overflowed = true,
        }
    }

    /// Untracked blocks are not found and do not change the count.
    fn remove(&mut self, addr: usize) {
        let found = self
            .blocks
            .iter_mut()
            .find(|slot| slot.is_some_and(|block| block.addr == addr));
        if let Some(block) = found.and_then(Option::take) {
            self.live -= block.size;
        }
    }

    /// Activity since the previous checkpoint. `live` is absolute.
    fn take_phase(&mut self) -> Phase {
        assert!(!self.overflowed, "more than {TRACKED} live blocks");
        let phase = Phase {
            allocations: self.allocations,
            reallocations: self.reallocations,
            live: self.live,
            peak: self.peak,
            largest: self.largest,
        };
        self.allocations = 0;
        self.reallocations = 0;
        self.largest = 0;
        self.peak = self.live;
        phase
    }
}

thread_local! {
    static TRACKER: RefCell<Tracker> = const { RefCell::new(Tracker::IDLE) };
}

/// Run `f` on this thread's tracker. Returns `None` if the tracker is
/// unavailable (during thread teardown, or re-entered) rather than panicking
/// inside an allocator.
fn tracker<R>(f: impl FnOnce(&mut Tracker) -> R) -> Option<R> {
    TRACKER
        .try_with(|tracker| tracker.try_borrow_mut().ok().map(|mut t| f(&mut t)))
        .ok()
        .flatten()
}

/// Forwards to the system allocator and counts what happens inside a
/// [`Window`]. Install it with `#[global_allocator]`, once per test binary.
pub struct CountingAllocator;

// SAFETY: allocation and deallocation are forwarded unchanged to System. The
// instrumentation only updates a fixed-size, thread-local table without
// allocating.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies the layout required by GlobalAlloc.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let _ = tracker(|t| t.allocated(ptr as usize, layout.size()));
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies the layout required by GlobalAlloc.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            let _ = tracker(|t| t.allocated(ptr as usize, layout.size()));
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let _ = tracker(|t| t.freed(ptr as usize));
        // SAFETY: allocations come from System with the unchanged layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller supplies System's allocation and a valid new size.
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        // A failed realloc leaves the original block allocated and tracked.
        if !new.is_null() {
            let _ = tracker(|t| t.resized(ptr as usize, new as usize, new_size));
        }
        new
    }
}

/// Activity on the measuring thread between checkpoints.
#[derive(Clone, Copy, Debug)]
pub struct Phase {
    /// Successful `alloc` and `alloc_zeroed` calls.
    pub allocations: usize,
    /// Successful `realloc` calls. These are not counted as allocations.
    pub reallocations: usize,
    /// Requested bytes of tracked blocks live at the checkpoint.
    pub live: usize,
    /// Highest logical `live` within the phase, including its starting total.
    pub peak: usize,
    /// Largest successful request within the phase.
    pub largest: usize,
}

/// A measurement window on this thread. Closing it on drop stops tracking.
#[must_use = "measurement stops when the window is dropped"]
pub struct Window;

impl Window {
    /// Starts tracking allocations on this thread.
    ///
    /// # Panics
    ///
    /// Panics if a window is already open on this thread.
    pub fn open() -> Self {
        tracker(|t| {
            assert!(!t.active, "nested allocation measurement");
            *t = Tracker {
                active: true,
                ..Tracker::IDLE
            };
        })
        .expect("allocation tracker available");
        Window
    }

    /// The activity since the previous checkpoint of this window, or since
    /// `open`. Tracking continues.
    pub fn checkpoint(&self) -> Phase {
        tracker(Tracker::take_phase).expect("allocation tracker available")
    }
}

impl Drop for Window {
    fn drop(&mut self) {
        let _ = tracker(|t| {
            t.active = false;
            t.blocks = [None; TRACKED];
        });
    }
}

/// Tracker accounting with synthetic addresses, independent of the system
/// allocator's choice to move or resize a block in place.
#[cfg(test)]
mod tracker_accounting {
    use super::*;

    const A: usize = 0x1000;
    const B: usize = 0x2000;
    const C: usize = 0x3000;

    fn tracking() -> Tracker {
        Tracker {
            active: true,
            ..Tracker::IDLE
        }
    }

    #[test]
    fn in_place_growth_counts_the_new_size_once() {
        let mut t = tracking();
        t.allocated(A, 1024);
        t.take_phase();
        t.resized(A, A, 2048);
        let phase = t.take_phase();
        assert_eq!((phase.live, phase.peak), (2048, 2048));
        assert_eq!((phase.allocations, phase.reallocations), (0, 1));
        assert_eq!(phase.largest, 2048);
    }

    #[test]
    fn in_place_shrink_keeps_the_prior_peak() {
        let mut t = tracking();
        t.allocated(A, 2048);
        t.resized(A, A, 1024);
        let phase = t.take_phase();
        assert_eq!((phase.live, phase.peak), (1024, 2048));
        assert_eq!((phase.allocations, phase.reallocations), (1, 1));
        assert_eq!(phase.largest, 2048);
    }

    #[test]
    fn moving_growth_counts_the_new_size_once() {
        let mut t = tracking();
        t.allocated(A, 1024);
        t.take_phase();
        t.resized(A, B, 2048);
        let phase = t.take_phase();
        assert_eq!((phase.live, phase.peak), (2048, 2048));
        assert_eq!((phase.allocations, phase.reallocations), (0, 1));
        assert_eq!(phase.largest, 2048);

        // The old address left tracking with the move.
        t.freed(A);
        assert_eq!(t.take_phase().live, 2048);
    }

    #[test]
    fn moving_shrink_counts_the_new_size_once() {
        let mut t = tracking();
        t.allocated(A, 2048);
        t.resized(A, B, 1024);
        let phase = t.take_phase();
        assert_eq!((phase.live, phase.peak), (1024, 2048));
        assert_eq!((phase.allocations, phase.reallocations), (1, 1));
    }

    #[test]
    fn resize_keeps_another_live_block_counted_once() {
        for resized_to in [A, B] {
            let mut t = tracking();
            t.allocated(A, 1024);
            t.allocated(C, 512);
            t.take_phase();
            t.resized(A, resized_to, 2048);
            let phase = t.take_phase();
            assert_eq!((phase.live, phase.peak), (2560, 2560), "at {resized_to:#x}");

            t.freed(C);
            assert_eq!(t.take_phase().live, 2048, "at {resized_to:#x}");
        }
    }

    #[test]
    fn resizing_an_untracked_block_tracks_its_full_new_size() {
        let mut t = tracking();
        t.resized(A, B, 2048);
        let phase = t.take_phase();
        assert_eq!((phase.live, phase.peak), (2048, 2048));
        assert_eq!((phase.allocations, phase.reallocations), (0, 1));
        assert_eq!(phase.largest, 2048);

        t.freed(B);
        assert_eq!(t.take_phase().live, 0);
    }

    #[test]
    fn freeing_an_untracked_block_is_a_no_op() {
        let mut t = tracking();
        t.allocated(B, 512);
        t.freed(A);
        assert_eq!(t.take_phase().live, 512);
    }

    #[test]
    fn freeing_a_resized_block_removes_its_current_address() {
        for resized_to in [A, B] {
            let mut t = tracking();
            t.allocated(A, 1024);
            t.allocated(C, 512);
            t.resized(A, resized_to, 2048);

            t.freed(resized_to);
            assert_eq!(t.take_phase().live, 512, "at {resized_to:#x}");
            t.freed(C);
            assert_eq!(t.take_phase().live, 0, "at {resized_to:#x}");
        }
    }

    #[test]
    fn checkpoint_after_a_resize_starts_the_next_phase_at_its_live_total() {
        let mut t = tracking();
        t.allocated(A, 1024);
        t.resized(A, B, 2048);
        t.resized(B, C, 512);
        let first = t.take_phase();
        assert_eq!((first.live, first.peak), (512, 2048));
        assert_eq!((first.allocations, first.reallocations), (1, 2));

        // Counters reset, and the earlier peak does not carry over.
        let second = t.take_phase();
        assert_eq!((second.live, second.peak), (512, 512));
        assert_eq!(
            (second.allocations, second.reallocations, second.largest),
            (0, 0, 0)
        );
    }
}
