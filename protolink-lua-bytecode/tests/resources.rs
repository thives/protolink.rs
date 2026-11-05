//! Resource use of artifact verification: live heap, bytes copied, and stack depth, measured with a counting allocator.
//!
//! Nested artifacts embed their children's bytes, so a naive verifier that decodes each child from a copy, and that
//! recurses once per level, holds a copy of every deeper level at every level, and uses Rust stack per level. These
//! tests measure a chain of `levels` artifacts, each embedding the next, at several depths.
//!
//! The stack figure is the deepest stack address at which the measured thread allocated, relative to where the
//! measurement began. It is a lower bound of the true stack use (the deepest frame may not allocate), reported to show
//! how it grows with depth. It says nothing about a target's stack, which has to be measured on that target.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use protolink_lua_bytecode::{Artifact, ChildLink, Constant, Entry, Instr, Limits, Proto, digest};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static TOTAL: AtomicUsize = AtomicUsize::new(0);
static COUNT: AtomicUsize = AtomicUsize::new(0);
static DEEPEST: AtomicUsize = AtomicUsize::new(usize::MAX);
thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
}
static MEASURE: Mutex<()> = Mutex::new(());

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let marker = 0u8;
        if TRACKING.try_with(Cell::get).unwrap_or(false) {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
            TOTAL.fetch_add(layout.size(), Ordering::Relaxed);
            COUNT.fetch_add(1, Ordering::Relaxed);
            DEEPEST.fetch_min(std::ptr::addr_of!(marker) as usize, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACKING.try_with(Cell::get).unwrap_or(false) {
            // Frees of memory allocated before the measurement would underflow, so saturate.
            #[allow(deprecated)] // `try_update` needs a newer Rust than this crate's minimum.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some(live.saturating_sub(layout.size()))
            });
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What one measured call used.
#[derive(Clone, Copy, Debug)]
struct Used {
    /// Largest live heap during the call, above what was live when it began.
    peak: usize,
    /// Bytes requested in total, including ones freed again.
    total: usize,
    /// Number of allocations.
    allocations: usize,
    /// Deepest stack position at which the call allocated, below where it began.
    stack: usize,
}

fn measure<T>(run: impl FnOnce() -> T) -> (T, Used) {
    let origin = 0u8;
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    TOTAL.store(0, Ordering::Relaxed);
    COUNT.store(0, Ordering::Relaxed);
    DEEPEST.store(usize::MAX, Ordering::Relaxed);
    TRACKING.with(|tracking| tracking.set(true));
    let result = run();
    TRACKING.with(|tracking| tracking.set(false));
    let deepest = DEEPEST.load(Ordering::Relaxed);
    let here = std::ptr::addr_of!(origin) as usize;
    (
        result,
        Used {
            peak: PEAK.load(Ordering::Relaxed),
            total: TOTAL.load(Ordering::Relaxed),
            allocations: COUNT.load(Ordering::Relaxed),
            stack: here.saturating_sub(deepest),
        },
    )
}

fn leaf() -> Artifact {
    Artifact {
        contract: [0x11; 32],
        constants: vec![Constant::Integer(1)],
        natives: Vec::new(),
        protos: vec![Proto {
            params: 0,
            locals: 1,
            max_stack: 2,
            upvalues: Vec::new(),
            code: vec![Instr::Constant(0), Instr::Return(1)],
            lines: vec![1, 1],
        }],
        entries: vec![Entry {
            proto: 0,
            captures: 0,
        }],
        children: Vec::new(),
    }
}

/// A chain in which each artifact embeds the next as its only child. The innermost is at depth `levels`.
fn chain(levels: usize) -> Vec<u8> {
    let mut inner = leaf();
    for _ in 0..levels {
        let bytes = inner.encode();
        let mut parent = leaf();
        parent.children = vec![ChildLink {
            slot: 1,
            digest: digest(&bytes),
            entry: 0,
            bytes: Rc::from(bytes),
        }];
        inner = parent;
    }
    inner.encode()
}

fn limits_for(bytes: usize, depth: u8) -> Limits {
    Limits {
        max_artifact_bytes: bytes,
        max_depth: depth,
        ..Limits::profile()
    }
}

/// Runs `f` on a thread with a stack of `stack` bytes, so the stack the call needs is bounded and the main thread's size
/// does not matter.
fn on_stack<T: Send + 'static>(stack: usize, f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(stack)
        .spawn(f)
        .expect("a thread")
        .join()
        .expect("the call completed without overflowing")
}

fn report(levels: usize) -> (usize, Used, Used) {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    on_stack(64 << 20, move || {
        let bytes = chain(levels);
        let size = bytes.len();
        let limits = limits_for(size + 1024, levels as u8);
        let (verified, decode) = measure(|| Artifact::decode_verified(&bytes, &limits));
        assert!(verified.is_ok(), "depth {levels}: {verified:?}");
        drop(verified);
        let artifact = Artifact::decode(&bytes, &limits).expect("decodes");
        let (checked, verify) = measure(|| artifact.verify(&limits));
        assert!(checked.is_ok(), "depth {levels}: {checked:?}");
        (size, decode, verify)
    })
}

/// Heap and stack use grow with the artifact, not with the nesting depth. Before the verifier read children in place, a
/// 39 KB artifact nested 255 deep peaked at 5.0 MB of heap (129 times its size, growing with the square of the depth) and
/// reached at least 837 KB of stack (growing with the depth). Those figures were measured on a debug build on x86_64.
#[test]
fn nested_verification_is_linear_in_size_and_constant_in_stack() {
    let depths = [8usize, 32, 64, 128, 255];
    let mut stacks = Vec::new();
    for levels in depths {
        let (size, decode, verify) = report(levels);
        println!(
            "RESOURCES depth={levels} bytes={size} decode_verified: peak={} total={} allocs={} stack>={} | verify: peak={} total={} allocs={} stack>={}",
            decode.peak,
            decode.total,
            decode.allocations,
            decode.stack,
            verify.peak,
            verify.total,
            verify.allocations,
            verify.stack
        );
        // Decoding copies each child's bytes once into the artifact it returns, and verification adds one decoded level.
        assert!(
            decode.peak <= 2 * size + 4096,
            "depth {levels}: decode_verified peaked at {} bytes for a {size}-byte artifact",
            decode.peak
        );
        // Verifying an artifact that already holds its children copies nothing but the level being checked.
        assert!(
            verify.peak <= size + 4096,
            "depth {levels}: verify peaked at {} bytes for a {size}-byte artifact",
            verify.peak
        );
        stacks.push(decode.stack.max(verify.stack));
    }
    let (shallowest, deepest) = (stacks[0], stacks[stacks.len() - 1]);
    assert!(
        deepest <= shallowest + 4096,
        "stack use grew with the depth: {stacks:?} for depths {depths:?}"
    );
}

/// The deepest supported nesting decodes and verifies on a small stack. A recursive verifier needed hundreds of kilobytes.
#[test]
fn the_deepest_nesting_verifies_on_a_small_stack() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    let bytes = chain(255);
    let limits = limits_for(bytes.len() + 1024, u8::MAX);
    let verified = on_stack(48 << 10, move || {
        Artifact::decode_verified(&bytes, &limits).is_ok()
    });
    assert!(verified, "255 levels verify on a 48 KiB stack");
}

/// A leaf whose code pops from an empty stack, so verifying it fails at its only instruction.
fn broken_leaf() -> Artifact {
    let mut artifact = leaf();
    artifact.protos[0].code = vec![Instr::Return(1)];
    artifact.protos[0].lines = vec![1];
    artifact
}

/// A chain as [`chain`], whose innermost artifact is [`broken_leaf`].
fn chain_with_broken_leaf(levels: usize) -> Vec<u8> {
    let mut inner = broken_leaf();
    for _ in 0..levels {
        let bytes = inner.encode();
        let mut parent = leaf();
        parent.children = vec![ChildLink {
            slot: 1,
            digest: digest(&bytes),
            entry: 0,
            bytes: Rc::from(bytes),
        }];
        inner = parent;
    }
    inner.encode()
}

/// A root with `children` valid leaf children, and with the last one replaced by a broken leaf when `broken` is set.
fn wide(children: usize, broken: bool) -> Vec<u8> {
    let good = leaf().encode();
    let bad = broken_leaf().encode();
    let mut parent = leaf();
    parent.children = (0..children)
        .map(|index| {
            let bytes = if broken && index + 1 == children {
                bad.clone()
            } else {
                good.clone()
            };
            ChildLink {
                slot: 1,
                digest: digest(&bytes),
                entry: 0,
                bytes: Rc::from(bytes),
            }
        })
        .collect();
    parent.encode()
}

/// Decodes and verifies `bytes` on a thread with `stack` bytes of stack, and measures the call. A failure is formatted and
/// then dropped inside the measured call, so the cost of reporting it is included.
fn measured_decode(
    bytes: Vec<u8>,
    depth: u8,
    children: usize,
    stack: usize,
) -> (Option<String>, Used) {
    on_stack(stack, move || {
        let limits = Limits {
            max_children: children.max(Limits::profile().max_children),
            ..limits_for(bytes.len() + 1024, depth)
        };
        measure(|| match Artifact::decode_verified(&bytes, &limits) {
            Ok(artifact) => {
                drop(artifact);
                None
            }
            Err(error) => Some(format!("{error}")),
        })
    })
}

/// A deep invalid artifact is refused, formatted and dropped on a small stack. The message names the whole path, and the
/// formatting does not recurse with the depth.
#[test]
fn a_deep_invalid_artifact_is_refused_formatted_and_dropped_on_a_small_stack() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    let bytes = chain_with_broken_leaf(usize::from(u8::MAX));
    let limits = limits_for(bytes.len() + 1024, u8::MAX);
    let text = on_stack(48 << 10, move || {
        let error = Artifact::decode_verified(&bytes, &limits).expect_err("the leaf is invalid");
        let text = format!("{error}");
        drop(error);
        text
    });
    let (path, rest) = text
        .strip_prefix("verification failed: ")
        .unwrap_or(&text)
        .strip_prefix("child ")
        .and_then(|text| text.split_once(" is invalid: "))
        .expect("the message names the path to the invalid artifact");
    assert_eq!(
        path.split('.').count(),
        usize::from(u8::MAX),
        "one index for each level down to the leaf"
    );
    assert!(rest.contains("stack underflow"), "{rest}");
}

/// Heap use of a wide tree grows with its children, not faster, and a failure in a wide tree costs no more than the
/// valid tree of the same size. Figures are host figures from a debug build.
#[test]
fn wide_and_invalid_trees_are_measured_beside_the_valid_chain() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    let mut wide_peaks = Vec::new();
    for children in [16usize, 64, 255] {
        let bytes = wide(children, false);
        let (result, used) = measured_decode(bytes.clone(), 2, children, 64 << 20);
        assert!(
            result.is_none(),
            "{children} valid children verify: {result:?}"
        );
        println!(
            "RESOURCES wide children={children} bytes={} verify: peak={} total={} allocs={} stack>={}",
            bytes.len(),
            used.peak,
            used.total,
            used.allocations,
            used.stack
        );
        wide_peaks.push((children, used.peak));
    }
    let (small, large) = (wide_peaks[0], wide_peaks[wide_peaks.len() - 1]);
    // Sixteen times the children costs at most about sixteen times the heap, with a fixed overhead allowed for.
    assert!(
        large.1 <= 20 * small.1 + 64 * 1024,
        "heap grew faster than the children: {wide_peaks:?}"
    );

    let valid_chain = chain(usize::from(u8::MAX));
    let (_, valid) = measured_decode(valid_chain.clone(), u8::MAX, 8, 64 << 20);
    let (invalid_chain, invalid) = measured_decode(
        chain_with_broken_leaf(usize::from(u8::MAX)),
        u8::MAX,
        8,
        64 << 20,
    );
    println!(
        "RESOURCES chain depth=255 valid: peak={} allocs={} | invalid leaf: peak={} allocs={} error={} bytes",
        valid.peak,
        valid.allocations,
        invalid.peak,
        invalid.allocations,
        invalid_chain.as_ref().map_or(0, String::len)
    );
    assert!(
        invalid.peak <= valid.peak + 16 * 1024,
        "the failure path costs more than a valid chain: {} against {}",
        invalid.peak,
        valid.peak
    );

    let (broken_wide, broken) = measured_decode(wide(255, true), 2, 255, 64 << 20);
    println!(
        "RESOURCES wide children=255 invalid last child: peak={} allocs={} error={broken_wide:?}",
        broken.peak, broken.allocations
    );
    let text = broken_wide.expect("the last child is invalid");
    assert!(text.contains("child 254 is invalid"), "{text}");
    assert!(
        broken.peak <= 2 * wide_peaks[wide_peaks.len() - 1].1 + 64 * 1024,
        "{broken:?}"
    );
}
