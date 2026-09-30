//! An async, single-producer single-consumer ring buffer.
//!
//! A [`RingBuffer`] owns the storage but has no way to read or write it. The
//! only access is through [`RingBuffer::split`], which hands out exactly one
//! [`RingTx`] (the writing half) and one [`RingRx`] (the reading half). Split
//! borrows the buffer mutably, so there can never be a second producer or
//! consumer: the SPSC discipline is enforced by the type system.
//!
//! Each half is `Send` (it may be moved to another task or thread, including
//! on a multi-threaded executor) but not `Sync`: a half is used from one place
//! at a time. The halves synchronise through atomics and wake each other with
//! [`AtomicWaker`]s. The optional `portable-atomic-critical-section` feature
//! uses the platform's critical-section implementation to emulate compare-and-
//! swap on targets that lack it natively.
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! use protolink::link::ring::RingBuffer;
//!
//! let mut ring = RingBuffer::<u32, 4>::new();
//! let (mut tx, mut rx) = ring.split();
//! let producer = async {
//!     for i in 0..10 {
//!         tx.push(i).await; // waits while the ring is full
//!     }
//! };
//! let consumer = async {
//!     let mut out = [0; 10];
//!     rx.pop_exact(&mut out).await; // waits while the ring is empty
//!     out
//! };
//! let ((), out) = tokio::join!(producer, consumer);
//! assert_eq!(out, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
//! # }
//! ```
//!
//! Originally based on <https://docs.rs/ring_buffer_no_std>, reworked for
//! enforced SPSC use, bulk copies and async.
#![allow(unsafe_code)]

use core::cell::{Cell, UnsafeCell};
use core::convert::Infallible;
use core::future::poll_fn;
use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::ptr;
#[cfg(not(feature = "portable-atomic"))]
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll};
#[cfg(feature = "portable-atomic")]
use portable_atomic::{AtomicUsize, Ordering};

use atomic_waker::AtomicWaker;
use embedded_io_async::{ErrorType, Read, Write};

/// Error of [`RingTx::try_push`].
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum RingBufferError {
    /// The ring buffer is full.
    Full,
}

impl core::fmt::Display for RingBufferError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Full => f.write_str("ring buffer is full"),
        }
    }
}

impl core::error::Error for RingBufferError {}

/// Fixed-capacity storage for `S` items of type `T`.
///
/// Use [`split`](Self::split) to obtain the writing and reading halves. See the
/// [module docs](self).
///
/// # Invariants
///
/// `head` and `tail` are positions in `0..2 * S`: the slot of a position is
/// `pos % S`, and the doubled range tells a full ring (`head - tail == S`)
/// from an empty one (`head == tail`). `head` is written only by the
/// [`RingTx`], `tail` only by the [`RingRx`]. Slots in `tail..head` are
/// initialised and owned by the reader; all other slots are owned by the
/// writer.
pub struct RingBuffer<T, const S: usize> {
    buf: [UnsafeCell<MaybeUninit<T>>; S],
    /// Position of the next slot to write.
    head: AtomicUsize,
    /// Position of the next slot to read.
    tail: AtomicUsize,
    /// The reader waiting for items.
    data_waker: AtomicWaker,
    /// The writer waiting for free slots.
    space_waker: AtomicWaker,
}

// SAFETY: the slots are only reached through the two halves. `split` takes
// `&mut self`, so there is at most one `RingTx` and one `RingRx`, and they only
// ever touch disjoint slots (see the invariants above), handing slots over with
// release/acquire stores and loads of `head` and `tail`. Items move between
// threads, hence `T: Send`.
unsafe impl<T: Send, const S: usize> Sync for RingBuffer<T, S> {}

impl<T: Copy, const S: usize> RingBuffer<T, S> {
    /// Create an empty ring buffer. `S` must be non-zero.
    pub const fn new() -> Self {
        const {
            assert!(S > 0, "ring buffer capacity must be non-zero");
            assert!(S <= usize::MAX / 4, "ring buffer capacity is too large");
        }
        Self {
            buf: [const { UnsafeCell::new(MaybeUninit::uninit()) }; S],
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            data_waker: AtomicWaker::new(),
            space_waker: AtomicWaker::new(),
        }
    }

    /// Number of items the ring buffer can hold.
    #[inline]
    pub const fn capacity(&self) -> usize {
        S
    }

    /// Discard all items.
    pub fn clear(&mut self) {
        *self.head.get_mut() = 0;
        *self.tail.get_mut() = 0;
    }

    /// Split the ring buffer into its writing and reading halves.
    ///
    /// This is the only way to push or pop items. The halves borrow the ring
    /// buffer, so it cannot be split again until both are dropped. Items left
    /// in the ring are kept; use [`clear`](Self::clear) to discard them.
    pub fn split(&mut self) -> (RingTx<'_, T, S>, RingRx<'_, T, S>) {
        let ring = &*self;
        (
            RingTx {
                ring,
                _not_sync: PhantomData,
            },
            RingRx {
                ring,
                _not_sync: PhantomData,
            },
        )
    }

    /// Number of items in the ring, for positions `head` and `tail`.
    #[inline]
    fn distance(head: usize, tail: usize) -> usize {
        if head >= tail {
            head - tail
        } else {
            head + 2 * S - tail
        }
    }

    /// `pos` moved forward by `n <= S` items.
    #[inline]
    fn advance(pos: usize, n: usize) -> usize {
        let next = pos + n;
        if next >= 2 * S { next - 2 * S } else { next }
    }

    #[inline]
    fn len(&self) -> usize {
        Self::distance(
            self.head.load(Ordering::Acquire),
            self.tail.load(Ordering::Acquire),
        )
    }

    /// Pointer to the first slot. All slots are `UnsafeCell`s, so writing
    /// through it is allowed.
    #[inline]
    fn base(&self) -> *mut T {
        UnsafeCell::raw_get(self.buf.as_ptr()).cast::<T>()
    }

    /// The slot index of `pos` and how many slots follow it before the end
    /// of the storage.
    #[inline]
    fn index(pos: usize) -> (usize, usize) {
        let idx = if pos >= S { pos - S } else { pos };
        (idx, S - idx)
    }

    /// Copy `src` into the slots starting at position `pos`, wrapping around.
    ///
    /// # Safety
    ///
    /// The caller is the writer and owns the `src.len() <= S` slots.
    unsafe fn write_at(&self, pos: usize, src: &[T]) {
        let (idx, to_end) = Self::index(pos);
        let first = src.len().min(to_end);
        // SAFETY: both ranges are inside the storage and owned by the writer;
        // `src` cannot overlap it because the writer never hands slots out.
        unsafe {
            ptr::copy_nonoverlapping(src.as_ptr(), self.base().add(idx), first);
            ptr::copy_nonoverlapping(src.as_ptr().add(first), self.base(), src.len() - first);
        }
    }

    /// The `len` items starting at position `pos`, as up to two slices.
    ///
    /// # Safety
    ///
    /// The caller is the reader, the `len <= S` slots are initialised and owned
    /// by it, and it does not release them while the slices are alive.
    unsafe fn slices_at(&self, pos: usize, len: usize) -> (&[T], &[T]) {
        let (idx, to_end) = Self::index(pos);
        let first = len.min(to_end);
        // SAFETY: as documented; the writer does not touch these slots until
        // the reader moves `tail` past them, which needs `&mut RingRx`.
        unsafe {
            (
                core::slice::from_raw_parts(self.base().add(idx), first),
                core::slice::from_raw_parts(self.base(), len - first),
            )
        }
    }
}

impl<T: Copy, const S: usize> Default for RingBuffer<T, S> {
    fn default() -> Self {
        Self::new()
    }
}

/// `Cell` is `Send` but not `Sync`, which is what the halves should be.
type NotSync = PhantomData<Cell<()>>;

/// The writing half of a [`RingBuffer`], created by [`RingBuffer::split`].
///
/// `Send` but not `Sync`. For `u8` it implements
/// [`embedded_io_async::Write`].
pub struct RingTx<'a, T, const S: usize> {
    ring: &'a RingBuffer<T, S>,
    _not_sync: NotSync,
}

impl<T: Copy, const S: usize> RingTx<'_, T, S> {
    /// Number of items the ring buffer can hold.
    #[inline]
    pub const fn capacity(&self) -> usize {
        S
    }

    /// Number of items in the ring. The reader may concurrently remove items,
    /// so the actual number may be lower.
    #[inline]
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Whether the ring is empty. The reader may concurrently remove items,
    /// so a `false` may be outdated.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the ring is full. The reader may concurrently remove items, so
    /// a `true` may be outdated.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() == S
    }

    /// Number of items that can be pushed without waiting. The reader may
    /// concurrently free more.
    #[inline]
    pub fn remaining_capacity(&self) -> usize {
        S - self.len()
    }

    /// Push one item, or fail if the ring is full.
    pub fn try_push(&mut self, item: T) -> Result<(), RingBufferError> {
        if self.push_slice(&[item]) == 1 {
            Ok(())
        } else {
            Err(RingBufferError::Full)
        }
    }

    /// Push as many items of `data` as fit and return how many that was.
    pub fn push_slice(&mut self, data: &[T]) -> usize {
        let ring = self.ring;
        let head = ring.head.load(Ordering::Relaxed);
        // Acquire: the reader is done with the slots it released.
        let tail = ring.tail.load(Ordering::Acquire);
        let n = data.len().min(S - RingBuffer::<T, S>::distance(head, tail));
        if n == 0 {
            return 0;
        }
        // SAFETY: the `n` slots from `head` are free, so they are owned by
        // the writer, and `self` is the only writer.
        unsafe { ring.write_at(head, &data[..n]) };
        // Release: publish the written slots to the reader.
        ring.head
            .store(RingBuffer::<T, S>::advance(head, n), Ordering::Release);
        ring.data_waker.wake();
        n
    }

    /// Poll for free space: `Ready(n)` with `n > 0` free slots, or `Pending`
    /// until the reader frees some.
    pub fn poll_writable(&mut self, cx: &mut Context<'_>) -> Poll<usize> {
        let free = self.remaining_capacity();
        if free > 0 {
            return Poll::Ready(free);
        }
        self.ring.space_waker.register(cx.waker());
        // Check again: the reader may have freed space before the waker was
        // registered.
        match self.remaining_capacity() {
            0 => Poll::Pending,
            free => Poll::Ready(free),
        }
    }

    /// Push one item, waiting while the ring is full.
    ///
    /// Cancel-safe: if the future is dropped before it completes, nothing was
    /// pushed.
    pub async fn push(&mut self, item: T) {
        poll_fn(|cx| self.poll_writable(cx)).await;
        let pushed = self.push_slice(&[item]);
        debug_assert_eq!(pushed, 1);
    }

    /// Push all of `data`, waiting for space as needed.
    ///
    /// If the future is dropped before it completes, a prefix of `data` may
    /// have been pushed.
    pub async fn push_all(&mut self, mut data: &[T]) {
        while !data.is_empty() {
            poll_fn(|cx| self.poll_writable(cx)).await;
            let n = self.push_slice(data);
            data = &data[n..];
        }
    }
}

impl<const S: usize> ErrorType for RingTx<'_, u8, S> {
    type Error = Infallible;
}

impl<const S: usize> Write for RingTx<'_, u8, S> {
    /// Push as many bytes as fit, waiting while the ring is full.
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
        if buf.is_empty() {
            return Ok(0);
        }
        poll_fn(|cx| self.poll_writable(cx)).await;
        Ok(self.push_slice(buf))
    }

    /// Bytes are visible to the reader once written, so this does nothing.
    async fn flush(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
}

/// The reading half of a [`RingBuffer`], created by [`RingBuffer::split`].
///
/// `Send` but not `Sync`. For `u8` it implements
/// [`embedded_io_async::Read`].
pub struct RingRx<'a, T, const S: usize> {
    ring: &'a RingBuffer<T, S>,
    _not_sync: NotSync,
}

impl<T: Copy, const S: usize> RingRx<'_, T, S> {
    /// Number of items the ring buffer can hold.
    #[inline]
    pub const fn capacity(&self) -> usize {
        S
    }

    /// Number of items in the ring. The writer may concurrently add items, so
    /// the actual number may be higher.
    #[inline]
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Whether the ring is empty. The writer may concurrently add items, so a
    /// `true` may be outdated.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the ring is full. The writer may concurrently add items, so a
    /// `false` may be outdated.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() == S
    }

    /// The items in the ring, oldest first, as two slices: the second is
    /// non-empty only if the items wrap around the end of the storage.
    ///
    /// Together with [`consume`](Self::consume) this reads items in place,
    /// without copying them out first.
    pub fn as_slices(&self) -> (&[T], &[T]) {
        let ring = self.ring;
        let tail = ring.tail.load(Ordering::Relaxed);
        // Acquire: the items the writer published are visible.
        let head = ring.head.load(Ordering::Acquire);
        let len = RingBuffer::<T, S>::distance(head, tail);
        // SAFETY: the `len` slots from `tail` are initialised and owned by
        // the reader. They stay owned until `tail` moves, which needs
        // `&mut self` and so ends the borrow of the slices.
        unsafe { ring.slices_at(tail, len) }
    }

    /// The oldest item, without removing it.
    pub fn peek(&self) -> Option<T> {
        let (first, _) = self.as_slices();
        first.first().copied()
    }

    /// Remove the `n` oldest items.
    ///
    /// # Panics
    ///
    /// If the ring holds fewer than `n` items.
    pub fn consume(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        let ring = self.ring;
        let tail = ring.tail.load(Ordering::Relaxed);
        let head = ring.head.load(Ordering::Acquire);
        assert!(
            n <= RingBuffer::<T, S>::distance(head, tail),
            "consumed more items than the ring holds"
        );
        // Release: the reader is done with the slots before the writer
        // reuses them.
        ring.tail
            .store(RingBuffer::<T, S>::advance(tail, n), Ordering::Release);
        ring.space_waker.wake();
    }

    /// Remove the oldest item, or `None` if the ring is empty.
    pub fn try_pop(&mut self) -> Option<T> {
        let item = self.peek()?;
        self.consume(1);
        Some(item)
    }

    /// Move up to `out.len()` of the oldest items into `out` and return how
    /// many that was.
    pub fn pop_slice(&mut self, out: &mut [T]) -> usize {
        let (first, second) = self.as_slices();
        let a = first.len().min(out.len());
        out[..a].copy_from_slice(&first[..a]);
        let b = second.len().min(out.len() - a);
        out[a..a + b].copy_from_slice(&second[..b]);
        self.consume(a + b);
        a + b
    }

    /// Poll for items: `Ready(n)` with `n > 0` items in the ring, or `Pending`
    /// until the writer pushes some.
    pub fn poll_readable(&mut self, cx: &mut Context<'_>) -> Poll<usize> {
        let len = self.len();
        if len > 0 {
            return Poll::Ready(len);
        }
        self.ring.data_waker.register(cx.waker());
        // Check again: the writer may have pushed before the waker was
        // registered.
        match self.len() {
            0 => Poll::Pending,
            len => Poll::Ready(len),
        }
    }

    /// Remove the oldest item, waiting while the ring is empty.
    ///
    /// Cancel-safe: if the future is dropped before it completes, nothing was
    /// removed.
    pub async fn pop(&mut self) -> T {
        poll_fn(|cx| self.poll_readable(cx)).await;
        self.try_pop().expect("the ring is not empty")
    }

    /// Fill all of `out`, waiting for items as needed.
    ///
    /// If the future is dropped before it completes, a prefix of `out` may
    /// have been filled and removed from the ring.
    pub async fn pop_exact(&mut self, mut out: &mut [T]) {
        while !out.is_empty() {
            poll_fn(|cx| self.poll_readable(cx)).await;
            let n = self.pop_slice(out);
            out = &mut out[n..];
        }
    }
}

impl<const S: usize> ErrorType for RingRx<'_, u8, S> {
    type Error = Infallible;
}

impl<const S: usize> Read for RingRx<'_, u8, S> {
    /// Read at least one byte, waiting while the ring is empty.
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
        if buf.is_empty() {
            return Ok(0);
        }
        poll_fn(|cx| self.poll_readable(cx)).await;
        Ok(self.pop_slice(buf))
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use std::vec::Vec;

    fn assert_send<T: Send>() {}

    /// Compiles only if `T` is not `Sync` (the `static_assertions` trick:
    /// the call is ambiguous if both impls apply).
    fn assert_not_sync<T>() {
        trait AmbiguousIfSync<A> {
            fn check() {}
        }
        impl<T: ?Sized> AmbiguousIfSync<()> for T {}
        impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}
        <T as AmbiguousIfSync<_>>::check();
    }

    #[test]
    fn halves_are_send_but_not_sync() {
        assert_send::<RingTx<'static, u32, 4>>();
        assert_send::<RingRx<'static, u32, 4>>();
        assert_not_sync::<RingTx<'static, u32, 4>>();
        assert_not_sync::<RingRx<'static, u32, 4>>();
    }

    #[test]
    fn push_and_pop() {
        let mut ring = RingBuffer::<u32, 4>::new();
        let (mut tx, mut rx) = ring.split();
        assert!(rx.is_empty() && tx.is_empty());
        assert_eq!(rx.try_pop(), None);
        assert_eq!(rx.peek(), None);

        for i in 0..4 {
            tx.try_push(i).unwrap();
        }
        assert!(tx.is_full() && rx.is_full());
        assert_eq!(tx.try_push(9), Err(RingBufferError::Full));
        assert_eq!(tx.remaining_capacity(), 0);
        assert_eq!(rx.len(), 4);

        assert_eq!(rx.peek(), Some(0));
        assert_eq!(rx.try_pop(), Some(0));
        assert_eq!(rx.try_pop(), Some(1));
        assert_eq!(tx.remaining_capacity(), 2);
        tx.try_push(4).unwrap();
        tx.try_push(5).unwrap();
        let mut out = [0; 8];
        assert_eq!(rx.pop_slice(&mut out), 4);
        assert_eq!(&out[..4], &[2, 3, 4, 5]);
        assert!(rx.is_empty());
    }

    #[test]
    fn slices_wrap_around() {
        let mut ring = RingBuffer::<u8, 5>::new();
        let (mut tx, mut rx) = ring.split();
        assert_eq!(tx.push_slice(b"abc"), 3);
        rx.consume(2);
        // Takes what fits, wrapping around the end of the storage.
        assert_eq!(tx.push_slice(b"defghi"), 4);
        assert_eq!(rx.as_slices(), (&b"cde"[..], &b"fg"[..]));
        rx.consume(4);
        assert_eq!(rx.as_slices(), (&b"g"[..], &b""[..]));
        assert_eq!(tx.push_slice(b""), 0);
    }

    #[test]
    #[should_panic(expected = "consumed more items")]
    fn consuming_too_much_panics() {
        let mut ring = RingBuffer::<u8, 4>::new();
        let (mut tx, mut rx) = ring.split();
        tx.push_slice(b"ab");
        rx.consume(3);
    }

    #[test]
    fn positions_wrap_many_times() {
        // An odd capacity and odd chunk sizes hit every slot/position
        // combination, including the wrap of the position range.
        let mut ring = RingBuffer::<u16, 7>::new();
        let (mut tx, mut rx) = ring.split();
        let mut next_in = 0u16;
        let mut next_out = 0u16;
        for round in 0..if cfg!(miri) { 100usize } else { 1000 } {
            let data: Vec<u16> = (0..(round % 9) as u16).map(|i| next_in + i).collect();
            let n = tx.push_slice(&data);
            next_in += n as u16;
            let mut out = [0u16; 5];
            let n = rx.pop_slice(&mut out[..round % 6]);
            for &v in &out[..n] {
                assert_eq!(v, next_out);
                next_out += 1;
            }
            assert_eq!(rx.len(), (next_in - next_out) as usize);
        }
    }

    #[test]
    fn split_keeps_items_and_clear_discards_them() {
        let mut ring = RingBuffer::<u8, 4>::new();
        ring.split().0.push_slice(b"ab");
        assert_eq!(ring.split().1.try_pop(), Some(b'a'));
        ring.clear();
        assert!(ring.split().1.is_empty());
        assert_eq!(ring.capacity(), 4);
    }

    #[tokio::test]
    async fn async_push_and_pop() {
        let mut ring = RingBuffer::<u32, 16>::new();
        let (mut tx, mut rx) = ring.split();
        tx.push(42).await;
        tx.push(43).await;
        assert_eq!(rx.pop().await, 42);
        assert_eq!(rx.pop().await, 43);

        tx.push_all(&[1, 2, 3, 4]).await;
        let mut out = [0; 4];
        rx.pop_exact(&mut out).await;
        assert_eq!(out, [1, 2, 3, 4]);
        assert!(rx.is_empty());
    }

    #[tokio::test]
    async fn async_backpressure() {
        let mut ring = RingBuffer::<u32, 3>::new();
        let (mut tx, mut rx) = ring.split();
        let producer = async {
            for i in 0..100 {
                tx.push(i).await;
            }
        };
        let consumer = async {
            let mut out = Vec::new();
            for _ in 0..100 {
                out.push(rx.pop().await);
            }
            out
        };
        let ((), out) = tokio::join!(producer, consumer);
        assert_eq!(out, (0..100).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn embedded_io_read_waits_for_data() {
        let mut ring = RingBuffer::<u8, 4>::new();
        let (mut tx, mut rx) = ring.split();
        let data: Vec<u8> = (0..=255).collect();
        let writer = async {
            tx.write_all(&data).await.unwrap();
        };
        let reader = async {
            let mut got = std::vec![0u8; data.len()];
            rx.read_exact(&mut got).await.unwrap();
            got
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(got, data);
    }

    #[test]
    fn halves_on_separate_threads() {
        const N: u32 = if cfg!(miri) { 500 } else { 200_000 };
        let mut ring = RingBuffer::<u32, 7>::new();
        let (mut tx, mut rx) = ring.split();
        std::thread::scope(|s| {
            s.spawn(move || {
                let mut next = 0;
                while next < N {
                    let chunk: Vec<u32> = (next..(next + 5).min(N)).collect();
                    next += tx.push_slice(&chunk) as u32;
                }
            });
            s.spawn(move || {
                let mut expected = 0;
                let mut out = [0; 4];
                while expected < N {
                    let n = rx.pop_slice(&mut out);
                    for &v in &out[..n] {
                        assert_eq!(v, expected);
                        expected += 1;
                    }
                }
            });
        });
    }

    #[test]
    fn async_halves_on_separate_threads() {
        const N: u32 = if cfg!(miri) { 200 } else { 50_000 };
        fn block_on<F: core::future::Future>(fut: F) -> F::Output {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(fut)
        }
        let mut ring = RingBuffer::<u32, 5>::new();
        let (mut tx, mut rx) = ring.split();
        std::thread::scope(|s| {
            s.spawn(move || {
                block_on(async {
                    for i in 0..N {
                        tx.push(i).await;
                    }
                })
            });
            s.spawn(move || {
                block_on(async {
                    for i in 0..N {
                        assert_eq!(rx.pop().await, i);
                    }
                })
            });
        });
    }
}
