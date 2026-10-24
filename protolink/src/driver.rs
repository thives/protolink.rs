//! Persistent I/O workers for the router driver (crate-private).
//!
//! A router connection is driven by one coordinator future, which alone owns
//! the router, its handlers and the clock. Each transport has one worker: a
//! future that borrows the transport and performs one operation at a time. The
//! coordinator asks for a write or a flush through a [`Port`], and the worker
//! reports the completion there; when it has no request it reads, and reports
//! what it read the same way. Reads are only ever dropped for a request, which
//! is the cancel-safety the transport's `read` already needs.
//!
//! Nothing is spawned, and no lock or borrow is held across an await. The
//! coordinator polls every worker from its own poll, so they share its waker,
//! and a worker that is blocked in a write stays blocked (and is not restarted)
//! while the coordinator does other work. A worker is a concrete `async fn`
//! future, so `Send` is preserved when the transport and error types allow it.

use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::Poll;

use embedded_io_async::{Read, Write};

use crate::READ_CHUNK;
use crate::shared::Shared;

/// Output copied for one write, and the largest read.
pub(crate) const CHUNK: usize = READ_CHUNK;

/// A request from the coordinator to a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Request {
    /// Write the first `n` bytes of the port's output.
    Write(usize),
    /// Flush the transport.
    Flush,
}

/// The completion of a [`Request`].
#[derive(Debug)]
pub(crate) enum Reply<E> {
    /// The write accepted this many bytes, or failed.
    Written(Result<usize, E>),
    /// The flush completed, or failed.
    Flushed(Result<(), E>),
}

/// What one worker and its coordinator share. Its contents are only touched in
/// short sections, through [`Shared::with`].
#[derive(Debug)]
pub(crate) struct Port<E> {
    /// Output copied from the core for the next write.
    out: [u8; CHUNK],
    request: Option<Request>,
    reply: Option<Reply<E>>,
    /// Bytes of the last read, `inbox[..n]`, until the coordinator takes them.
    inbox: [u8; CHUNK],
    /// Outcome of the last read, until the coordinator takes it. While it is
    /// present the worker does not read again.
    read: Option<Result<usize, E>>,
}

impl<E> Port<E> {
    pub(crate) fn new() -> Self {
        Self {
            out: [0; CHUNK],
            request: None,
            reply: None,
            inbox: [0; CHUNK],
            read: None,
        }
    }

    /// Ask for a write of up to [`CHUNK`] bytes of `bytes`, copied now. Returns
    /// how many bytes were copied. The bytes stay with the caller until the
    /// write's reply says how many were accepted.
    pub(crate) fn request_write(&mut self, bytes: &[u8]) -> usize {
        let n = bytes.len().min(CHUNK);
        self.out[..n].copy_from_slice(&bytes[..n]);
        self.request = Some(Request::Write(n));
        n
    }

    /// Ask for a flush.
    pub(crate) fn request_flush(&mut self) {
        self.request = Some(Request::Flush);
    }

    /// Whether a reply or a read outcome is waiting for the coordinator.
    pub(crate) fn has_event(&self) -> bool {
        self.reply.is_some() || self.read.is_some()
    }

    /// The completion of the last request, if it has completed and has not been
    /// taken yet.
    pub(crate) fn take_reply(&mut self) -> Option<Reply<E>> {
        self.reply.take()
    }

    /// The outcome of the last read, if one has completed and not been taken. For
    /// `Ok(n)`, the bytes are `inbox[..n]`, which [`inbox`](Self::inbox) gives
    /// until the next read starts.
    pub(crate) fn take_read(&mut self) -> Option<Result<usize, E>> {
        self.read.take()
    }

    /// The bytes of the last read. Only meaningful right after
    /// [`take_read`](Self::take_read) returned `Ok(n)`, for `n` bytes.
    pub(crate) fn inbox(&self) -> &[u8] {
        &self.inbox
    }
}

/// Serve the transport of `port` until the future is dropped. It never
/// completes on its own.
///
/// While a request is waiting, the worker serves it. Reads and requests
/// alternate: after a request, the transport is polled for input once before
/// the next request is served, so a stream of output cannot starve input. A
/// read that is still pending when a request arrives is dropped, and the request
/// is served. After a read completes, the worker reads nothing more until the
/// coordinator has taken the outcome.
pub(crate) async fn run<S: Read + Write>(io: &mut S, port: &Shared<Port<S::Error>>) {
    let mut buf = [0u8; CHUNK];
    let mut read_owed = true;
    loop {
        let turn = poll_fn(|_| {
            let (busy, readable) = port.with(|p| (p.request.is_some(), p.read.is_none()));
            if readable && (read_owed || !busy) {
                Poll::Ready(None)
            } else if busy {
                Poll::Ready(port.with(|p| p.request.take()))
            } else {
                Poll::Pending
            }
        })
        .await;
        match turn {
            Some(Request::Write(len)) => {
                read_owed = true;
                port.with(|p| buf[..len].copy_from_slice(&p.out[..len]));
                let result = io.write(&buf[..len]).await;
                port.with(|p| p.reply = Some(Reply::Written(result)));
            }
            Some(Request::Flush) => {
                read_owed = true;
                let result = io.flush().await;
                port.with(|p| p.reply = Some(Reply::Flushed(result)));
            }
            None => {
                read_owed = false;
                let outcome = {
                    let mut read = pin!(io.read(&mut buf));
                    poll_fn(|cx| match read.as_mut().poll(cx) {
                        Poll::Ready(result) => Poll::Ready(Some(result)),
                        Poll::Pending if port.with(|p| p.request.is_some()) => Poll::Ready(None),
                        Poll::Pending => Poll::Pending,
                    })
                    .await
                };
                if let Some(result) = outcome {
                    let result = result.map(|n| {
                        let n = n.min(CHUNK);
                        port.with(|p| p.inbox[..n].copy_from_slice(&buf[..n]));
                        n
                    });
                    port.with(|p| p.read = Some(result));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use alloc::collections::VecDeque;
    use alloc::rc::Rc;
    use alloc::vec::Vec;
    use core::cell::RefCell;
    use core::convert::Infallible;
    use core::pin::{Pin, pin};
    use core::task::{Context, Poll, Waker};

    use embedded_io_async::{ErrorType, Read, Write};

    use super::*;

    /// What a [`Fake`] transport does, shared with the test.
    #[derive(Default)]
    struct Script {
        /// Writes and flushes do not complete while set.
        stalled: bool,
        /// Write calls that were started.
        writes_started: usize,
        /// Bytes accepted by completed writes, in order.
        written: Vec<u8>,
        /// The chunks the reads return, in order.
        inbound: VecDeque<Vec<u8>>,
    }

    /// A transport whose behaviour the test scripts.
    struct Fake(Rc<RefCell<Script>>);

    impl ErrorType for Fake {
        type Error = Infallible;
    }

    impl Read for Fake {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
            poll_fn(|_| match self.0.borrow_mut().inbound.pop_front() {
                Some(chunk) => {
                    let n = chunk.len().min(buf.len());
                    buf[..n].copy_from_slice(&chunk[..n]);
                    Poll::Ready(Ok(n))
                }
                None => Poll::Pending,
            })
            .await
        }
    }

    impl Write for Fake {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
            self.0.borrow_mut().writes_started += 1;
            poll_fn(|_| {
                let mut script = self.0.borrow_mut();
                if script.stalled {
                    return Poll::Pending;
                }
                script.written.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            })
            .await
        }

        async fn flush(&mut self) -> Result<(), Infallible> {
            poll_fn(|_| {
                if self.0.borrow().stalled {
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(()))
                }
            })
            .await
        }
    }

    fn fake(script: Script) -> (Fake, Rc<RefCell<Script>>) {
        let script = Rc::new(RefCell::new(script));
        (Fake(script.clone()), script)
    }

    /// Poll a future once, as the coordinator would: with no waker to wake.
    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn a_stalled_write_does_not_hold_up_another_transport() {
        let (mut stuck, stuck_script) = fake(Script {
            stalled: true,
            ..Script::default()
        });
        let (mut free, free_script) = fake(Script::default());
        let stuck_port = Shared::new(Port::new());
        let free_port = Shared::new(Port::new());
        let mut stuck_worker = pin!(run(&mut stuck, &stuck_port));
        let mut free_worker = pin!(run(&mut free, &free_port));

        stuck_port.with(|p| p.request_write(b"stuck"));
        free_port.with(|p| p.request_write(b"free"));
        for _ in 0..8 {
            assert!(poll_once(stuck_worker.as_mut()).is_pending());
            assert!(poll_once(free_worker.as_mut()).is_pending());
        }

        assert!(matches!(
            free_port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(4)))
        ));
        assert_eq!(free_script.borrow().written, b"free");
        assert!(stuck_port.with(|p| p.take_reply()).is_none());
        assert_eq!(stuck_script.borrow().writes_started, 1);
    }

    #[test]
    fn a_pending_write_survives_unrelated_wakeups_without_restarting() {
        let (mut stuck, stuck_script) = fake(Script {
            stalled: true,
            ..Script::default()
        });
        let (mut busy, busy_script) = fake(Script::default());
        let stuck_port = Shared::new(Port::new());
        let busy_port = Shared::new(Port::new());
        let mut stuck_worker = pin!(run(&mut stuck, &stuck_port));
        let mut busy_worker = pin!(run(&mut busy, &busy_port));

        stuck_port.with(|p| p.request_write(b"held"));
        assert!(poll_once(stuck_worker.as_mut()).is_pending());
        assert_eq!(stuck_script.borrow().writes_started, 1);

        // Every round wakes the other transport: it reads, then writes.
        for round in 0..16u8 {
            busy_script
                .borrow_mut()
                .inbound
                .push_back(alloc::vec![round]);
            assert!(poll_once(busy_worker.as_mut()).is_pending());
            assert!(matches!(busy_port.with(|p| p.take_read()), Some(Ok(1))));
            busy_port.with(|p| p.request_write(&[round]));
            assert!(poll_once(busy_worker.as_mut()).is_pending());
            assert!(matches!(
                busy_port.with(|p| p.take_reply()),
                Some(Reply::Written(Ok(1)))
            ));
            assert!(poll_once(stuck_worker.as_mut()).is_pending());
        }
        assert_eq!(stuck_script.borrow().writes_started, 1);
        assert!(stuck_script.borrow().written.is_empty());

        // Releasing the transport completes the same write, with its bytes.
        stuck_script.borrow_mut().stalled = false;
        assert!(poll_once(stuck_worker.as_mut()).is_pending());
        assert!(matches!(
            stuck_port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(4)))
        ));
        assert_eq!(stuck_script.borrow().written, b"held");
        assert_eq!(stuck_script.borrow().writes_started, 1);
    }

    #[test]
    fn a_pending_read_is_dropped_for_a_request_and_the_input_is_read_later() {
        let (mut io, script) = fake(Script::default());
        let port = Shared::new(Port::new());
        let mut worker = pin!(run(&mut io, &port));
        assert!(poll_once(worker.as_mut()).is_pending());

        port.with(|p| p.request_write(b"out"));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(3)))
        ));
        assert_eq!(script.borrow().written, b"out");
        assert!(port.with(|p| p.take_read()).is_none());

        // Input that arrives afterwards is not lost with the dropped read.
        script.borrow_mut().inbound.push_back(b"in".to_vec());
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(port.with(|p| p.take_read()), Some(Ok(2))));
        assert_eq!(port.with(|p| p.inbox()[..2].to_vec()), b"in");
    }

    #[test]
    fn a_read_gets_a_turn_between_writes_so_output_cannot_starve_input() {
        let (mut io, script) = fake(Script::default());
        let port = Shared::new(Port::new());
        let mut worker = pin!(run(&mut io, &port));
        assert!(poll_once(worker.as_mut()).is_pending());

        // Output is queued in every round, and so is input for the first two.
        script
            .borrow_mut()
            .inbound
            .extend([b"a".to_vec(), b"b".to_vec()]);
        for input_owed in [true, true, false] {
            port.with(|p| p.request_write(b"w"));
            assert!(poll_once(worker.as_mut()).is_pending());
            assert!(matches!(
                port.with(|p| p.take_reply()),
                Some(Reply::Written(Ok(1)))
            ));
            // The read that was owed ran before the write was served.
            assert_eq!(port.with(|p| p.take_read()).is_some(), input_owed);
        }
        assert_eq!(script.borrow().written, b"www");
    }

    #[test]
    fn output_is_copied_at_request_time_and_each_write_is_bounded() {
        let (mut io, script) = fake(Script::default());
        let port = Shared::new(Port::new());
        let mut worker = pin!(run(&mut io, &port));
        let mut pending = alloc::vec![9u8; CHUNK + 10];
        let copied = port.with(|p| p.request_write(&pending));
        assert_eq!(copied, CHUNK);
        // The caller may reuse its buffer while the write is pending.
        pending.fill(0);
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(n))) if n == CHUNK
        ));
        assert_eq!(script.borrow().written, alloc::vec![9u8; CHUNK]);
    }

    #[test]
    fn a_flush_completes_after_its_writes_and_stays_pending_while_stalled() {
        let (mut io, script) = fake(Script::default());
        let port = Shared::new(Port::new());
        let mut worker = pin!(run(&mut io, &port));
        assert!(poll_once(worker.as_mut()).is_pending());
        port.with(|p| p.request_write(b"a"));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(1)))
        ));

        script.borrow_mut().stalled = true;
        port.with(|p| p.request_flush());
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(port.with(|p| p.take_reply()).is_none());

        script.borrow_mut().stalled = false;
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Flushed(Ok(())))
        ));
    }

    /// The worker's future can be sent to another thread when its transport can
    /// be: a tokio transport on `std`.
    #[cfg(feature = "tokio")]
    #[test]
    fn a_tokio_transport_worker_is_send() {
        fn assert_send<T: Send>(_: &T) {}
        let (a, _b) = tokio::io::duplex(64);
        let mut io = crate::tokio::compat(a);
        let port = Shared::new(Port::new());
        let worker = run(&mut io, &port);
        assert_send(&worker);
    }
}
