//! Persistent I/O workers for the drivers (crate-private).
//!
//! A connection is driven by one coordinator future, which alone owns the
//! protocol state, its handlers and the clock. Each transport has workers that
//! borrow it and report their completions through a [`Port`]; the coordinator
//! asks for work through the port and never touches the transport itself.
//!
//! There are three ways a transport is served:
//!
//! - **Serial** (`run_serial`): one worker performs one operation at a time,
//!   reads and writes alternating. A write that is blocked stops input.
//! - **Duplex** (`run_duplex`): independent read and write halves. The read
//!   half retains its pending read, and the write half retains its pending
//!   write or flush, so input keeps flowing while output is blocked. A read
//!   outcome is held until the coordinator takes it, and a write's reply is
//!   held until it is reaped, so neither half starts another operation early.
//! - **Reliable** (`run_reliable`): the ARQ engine is a single poll-based
//!   object, so both directions are polled from one worker with one context.
//!
//! Nothing is spawned, and no lock or borrow is held across an await. The
//! coordinator polls every worker from its own poll, so they share its waker,
//! and a worker blocked in a write stays blocked (and is not restarted) while
//! the coordinator does other work. Workers are concrete `async fn` futures, so
//! `Send` is preserved when the transport and error types allow it.

use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::Poll;

use embedded_io_async::{Read, Write};

use crate::READ_CHUNK;
use crate::link::{ReliableError, ReliableLink, Timer};
use crate::shared::Shared;

/// Output copied for one write, and the largest read.
pub(crate) const CHUNK: usize = READ_CHUNK;

/// A request from the coordinator to a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    /// Write the first `n` bytes of the port's output.
    Write(usize),
    /// Flush the transport.
    Flush,
}

/// The completion of a [`Request`].
#[derive(Debug)]
pub enum Reply<E> {
    /// The write accepted this many bytes, or failed.
    Written(Result<usize, E>),
    /// The flush completed, or failed.
    Flushed(Result<(), E>),
}

/// What one worker and its coordinator share. Its contents are only touched in
/// short sections, through `Shared::with`.
#[derive(Debug)]
pub struct Port<E> {
    /// Whether a serial worker serves a waiting write before its first read.
    write_first: bool,
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
            write_first: false,
            out: [0; CHUNK],
            request: None,
            reply: None,
            inbox: [0; CHUNK],
            read: None,
        }
    }

    /// Serve a waiting write before the first read of a serial transport.
    ///
    /// A client gives its first operation the output, as it has no input to
    /// wait for before its request is written. The default reads first, so that
    /// the input of a server is read while its first response is in flight.
    pub(crate) fn write_first(mut self) -> Self {
        self.write_first = true;
        self
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

    /// Whether the reply of the last request is waiting for the coordinator.
    pub(crate) fn has_reply(&self) -> bool {
        self.reply.is_some()
    }

    /// Stand in for the worker: complete the request in flight.
    #[cfg(test)]
    pub(crate) fn complete(&mut self, reply: Reply<E>) {
        self.reply = Some(reply);
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

/// Serve the transport of `port` in serial mode, until the future is dropped.
/// It never completes on its own.
///
/// While a request is waiting, the worker serves it. Reads and requests
/// alternate: after a request, the transport is polled for input once before
/// the next request is served, so a stream of output cannot starve input. A
/// read that is still pending when a request arrives is dropped, and the request
/// is served. After a read completes, the worker reads nothing more until the
/// coordinator has taken the outcome.
pub(crate) async fn run_serial<S: Read + Write>(io: &mut S, port: &Shared<Port<S::Error>>) {
    let mut buf = [0u8; CHUNK];
    let mut read_owed = port.with(|p| !p.write_first);
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
                // A write-first transport writes everything it has before it reads.
                read_owed = !port.with(|p| p.write_first);
                port.with(|p| buf[..len].copy_from_slice(&p.out[..len]));
                let result = io.write(&buf[..len]).await;
                port.with(|p| p.reply = Some(Reply::Written(result)));
            }
            Some(Request::Flush) => {
                read_owed = !port.with(|p| p.write_first);
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

/// The read half of a duplex transport, serving `port` until the future is
/// dropped.
///
/// One read is pending at a time, and the buffer it fills is owned by the
/// worker. Its outcome is held in the port until the coordinator takes it, and
/// no read starts before then. The pending read is never dropped for output,
/// since output does not use this half.
pub(crate) async fn run_rx<R: Read>(reader: &mut R, port: &Shared<Port<R::Error>>) {
    let mut buf = [0u8; CHUNK];
    loop {
        poll_fn(|_| {
            if port.with(|p| p.read.is_none()) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        let result = {
            let mut read = pin!(reader.read(&mut buf));
            poll_fn(|cx| read.as_mut().poll(cx)).await
        };
        let result = result.map(|n| {
            let n = n.min(CHUNK);
            port.with(|p| p.inbox[..n].copy_from_slice(&buf[..n]));
            n
        });
        port.with(|p| p.read = Some(result));
    }
}

/// The write half of a duplex transport, serving `port` until the future is
/// dropped.
///
/// It takes one request at a time: the copied output of a write, or a flush.
/// The request is in flight until its reply is reported, and the next one is
/// taken only after that.
pub(crate) async fn run_tx<W: Write>(writer: &mut W, port: &Shared<Port<W::Error>>) {
    let mut buf = [0u8; CHUNK];
    loop {
        let request = poll_fn(|_| match port.with(|p| p.request.take()) {
            Some(request) => Poll::Ready(request),
            None => Poll::Pending,
        })
        .await;
        match request {
            Request::Write(len) => {
                port.with(|p| buf[..len].copy_from_slice(&p.out[..len]));
                let result = writer.write(&buf[..len]).await;
                port.with(|p| p.reply = Some(Reply::Written(result)));
            }
            Request::Flush => {
                let result = writer.flush().await;
                port.with(|p| p.reply = Some(Reply::Flushed(result)));
            }
        }
    }
}

/// Serve both halves of a duplex transport until the future is dropped. Each
/// poll polls the read and the write half once, with the same context, so
/// neither waits for the other.
pub(crate) async fn run_duplex<R, W>(reader: &mut R, writer: &mut W, port: &Shared<Port<R::Error>>)
where
    R: Read,
    W: Write<Error = R::Error>,
{
    let mut rx = pin!(run_rx(reader, port));
    let mut tx = pin!(run_tx(writer, port));
    poll_fn(|cx| {
        let _ = rx.as_mut().poll(cx);
        let _ = tx.as_mut().poll(cx);
        Poll::<()>::Pending
    })
    .await
}

/// Serve a coordinated reliable link until the future is dropped.
///
/// ARQ keeps all of its protocol state in the link, so reads and writes are
/// plain polls of it. The read outcome is held in the port as for a duplex
/// transport. The write is copied once, into a buffer the worker owns, and that
/// copy is offered again on every poll until the engine accepts it, so the
/// engine never sees bytes change under it.
///
/// The engine's first terminal error is the one published: after it the engine
/// reports only `Closed`, so the worker is parked and polls the engine no more.
/// The failed operation's error reaches the coordinator, whatever else it reaps.
pub(crate) async fn run_reliable<S: Read + Write, L: Timer>(
    link: &mut ReliableLink<S, L>,
    port: &Shared<Port<ReliableError<S::Error>>>,
) {
    let mut rx_buf = [0u8; CHUNK];
    let mut tx_buf = [0u8; CHUNK];
    // The request the engine is working on, and its byte count for a write.
    let mut tx: Option<Request> = None;
    // Set once the engine has reported a terminal error. Later polls do nothing.
    let mut failed = false;
    poll_fn(|cx| {
        if failed {
            return Poll::Pending;
        }
        if port.with(|p| p.read.is_none())
            && let Poll::Ready(result) = link.poll_read_bytes(cx, &mut rx_buf)
        {
            failed = result.is_err();
            let result = result.map(|n| {
                let n = n.min(CHUNK);
                port.with(|p| p.inbox[..n].copy_from_slice(&rx_buf[..n]));
                n
            });
            port.with(|p| p.read = Some(result));
            if failed {
                // The pending write or flush must not see the engine's `Closed`.
                return Poll::Pending;
            }
        }
        if tx.is_none() {
            tx = port.with(|p| p.request.take());
            if let Some(Request::Write(len)) = tx {
                port.with(|p| tx_buf[..len].copy_from_slice(&p.out[..len]));
            }
        }
        match tx {
            Some(Request::Write(len)) => {
                if let Poll::Ready(result) = link.poll_write_bytes(cx, &tx_buf[..len]) {
                    tx = None;
                    failed = result.is_err();
                    port.with(|p| p.reply = Some(Reply::Written(result)));
                }
            }
            Some(Request::Flush) => {
                if let Poll::Ready(result) = link.poll_flush_bytes(cx) {
                    tx = None;
                    failed = result.is_err();
                    port.with(|p| p.reply = Some(Reply::Flushed(result)));
                }
            }
            None => {}
        }
        Poll::<()>::Pending
    })
    .await
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
    use core::time::Duration;

    use crate::link::{ArqError, LinkError};
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
        let mut stuck_worker = pin!(run_serial(&mut stuck, &stuck_port));
        let mut free_worker = pin!(run_serial(&mut free, &free_port));

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
        let mut stuck_worker = pin!(run_serial(&mut stuck, &stuck_port));
        let mut busy_worker = pin!(run_serial(&mut busy, &busy_port));

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
        let mut worker = pin!(run_serial(&mut io, &port));
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
        let mut worker = pin!(run_serial(&mut io, &port));
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
        let mut worker = pin!(run_serial(&mut io, &port));
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
        let mut worker = pin!(run_serial(&mut io, &port));
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

    /// The read half of a [`Halves`] transport: the chunks it returns, and how
    /// many reads were started.
    #[derive(Default)]
    struct Input {
        chunks: VecDeque<Vec<u8>>,
        reads_started: usize,
    }

    struct FakeReader(Rc<RefCell<Input>>);

    impl ErrorType for FakeReader {
        type Error = Infallible;
    }

    impl Read for FakeReader {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
            self.0.borrow_mut().reads_started += 1;
            poll_fn(|_| match self.0.borrow_mut().chunks.pop_front() {
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

    /// The write half of a [`Halves`] transport: whether its operations are
    /// stalled, what they accepted, and how many were started.
    #[derive(Default)]
    struct Output {
        stalled: bool,
        written: Vec<u8>,
        writes_started: usize,
        flushes_started: usize,
    }

    struct FakeWriter(Rc<RefCell<Output>>);

    impl ErrorType for FakeWriter {
        type Error = Infallible;
    }

    impl Write for FakeWriter {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
            self.0.borrow_mut().writes_started += 1;
            poll_fn(|_| {
                let mut out = self.0.borrow_mut();
                if out.stalled {
                    return Poll::Pending;
                }
                out.written.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            })
            .await
        }

        async fn flush(&mut self) -> Result<(), Infallible> {
            self.0.borrow_mut().flushes_started += 1;
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

    fn halves() -> (
        FakeReader,
        Rc<RefCell<Input>>,
        FakeWriter,
        Rc<RefCell<Output>>,
    ) {
        let input = Rc::new(RefCell::new(Input::default()));
        let output = Rc::new(RefCell::new(Output::default()));
        (
            FakeReader(input.clone()),
            input,
            FakeWriter(output.clone()),
            output,
        )
    }

    #[test]
    fn duplex_reads_while_write_is_pending() {
        let (mut reader, input, mut writer, output) = halves();
        output.borrow_mut().stalled = true;
        let port = Shared::new(Port::new());
        let mut worker = pin!(run_duplex(&mut reader, &mut writer, &port));
        assert!(poll_once(worker.as_mut()).is_pending());

        port.with(|p| p.request_write(b"held"));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert_eq!(output.borrow().writes_started, 1);

        // Input arrives while the write is blocked: it is read, and the write
        // is still the one request that is in flight.
        input.borrow_mut().chunks.push_back(b"in".to_vec());
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(port.with(|p| p.take_read()), Some(Ok(2))));
        assert_eq!(port.with(|p| p.inbox()[..2].to_vec()), b"in");
        assert!(port.with(|p| p.take_reply()).is_none());
        assert_eq!(output.borrow().writes_started, 1);
    }

    #[test]
    fn duplex_reads_while_flush_is_pending() {
        let (mut reader, input, mut writer, output) = halves();
        let port = Shared::new(Port::new());
        let mut worker = pin!(run_duplex(&mut reader, &mut writer, &port));
        assert!(poll_once(worker.as_mut()).is_pending());
        port.with(|p| p.request_write(b"a"));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(1)))
        ));

        output.borrow_mut().stalled = true;
        port.with(|p| p.request_flush());
        assert!(poll_once(worker.as_mut()).is_pending());
        assert_eq!(output.borrow().flushes_started, 1);

        input.borrow_mut().chunks.push_back(b"in".to_vec());
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(port.with(|p| p.take_read()), Some(Ok(2))));
        assert!(port.with(|p| p.take_reply()).is_none());
        assert_eq!(output.borrow().flushes_started, 1);
    }

    #[test]
    fn rx_wake_does_not_restart_tx() {
        let (mut reader, input, mut writer, output) = halves();
        output.borrow_mut().stalled = true;
        let port = Shared::new(Port::new());
        let mut worker = pin!(run_duplex(&mut reader, &mut writer, &port));
        port.with(|p| p.request_write(b"held"));
        assert!(poll_once(worker.as_mut()).is_pending());

        // Every round delivers input, which wakes the worker, and takes it.
        for round in 0..16u8 {
            input.borrow_mut().chunks.push_back(alloc::vec![round]);
            assert!(poll_once(worker.as_mut()).is_pending());
            assert!(matches!(port.with(|p| p.take_read()), Some(Ok(1))));
        }
        assert_eq!(output.borrow().writes_started, 1);
        assert!(output.borrow().written.is_empty());

        output.borrow_mut().stalled = false;
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(4)))
        ));
        assert_eq!(output.borrow().written, b"held");
        assert_eq!(output.borrow().writes_started, 1);
    }

    /// Counts the wakeups it receives.
    #[derive(Default)]
    struct WakeCount(core::sync::atomic::AtomicUsize);

    impl alloc::task::Wake for WakeCount {
        fn wake(self: alloc::sync::Arc<Self>) {
            self.0.fetch_add(1, core::sync::atomic::Ordering::SeqCst);
        }

        fn wake_by_ref(self: &alloc::sync::Arc<Self>) {
            self.0.fetch_add(1, core::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn idle_duplex_workers_do_not_self_wake() {
        let (mut reader, _input, mut writer, _output) = halves();
        let port = Shared::new(Port::new());
        let count = alloc::sync::Arc::new(WakeCount::default());
        let waker = Waker::from(count.clone());
        let mut cx = Context::from_waker(&waker);
        let mut worker = pin!(run_duplex(&mut reader, &mut writer, &port));
        for _ in 0..16 {
            assert!(worker.as_mut().poll(&mut cx).is_pending());
        }
        assert_eq!(count.0.load(core::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn read_outcome_is_held_until_taken_and_a_read_is_never_repeated() {
        let (mut reader, input, mut writer, _) = halves();
        input.borrow_mut().chunks.push_back(b"one".to_vec());
        input.borrow_mut().chunks.push_back(b"two".to_vec());
        let port = Shared::new(Port::new());
        let mut worker = pin!(run_duplex(&mut reader, &mut writer, &port));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert_eq!(input.borrow().reads_started, 1);
        // The outcome of the first read waits: no second read starts meanwhile.
        for _ in 0..4 {
            assert!(poll_once(worker.as_mut()).is_pending());
        }
        assert_eq!(input.borrow().reads_started, 1);
        assert!(matches!(port.with(|p| p.take_read()), Some(Ok(3))));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert_eq!(input.borrow().reads_started, 2);
        assert!(matches!(port.with(|p| p.take_read()), Some(Ok(3))));
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
        let worker = run_serial(&mut io, &port);
        assert_send(&worker);
    }

    /// A transport error that is not `Clone`, so the original error can only be
    /// passed on by ownership.
    #[derive(Debug)]
    struct Broken(u8);

    impl core::fmt::Display for Broken {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            write!(f, "broken transport {}", self.0)
        }
    }

    impl core::error::Error for Broken {}

    impl embedded_io_async::Error for Broken {
        fn kind(&self) -> embedded_io_async::ErrorKind {
            embedded_io_async::ErrorKind::Other
        }
    }

    /// What a [`Wired`] transport does, scripted by the test. Operations count
    /// the transport calls the engine starts.
    #[derive(Default)]
    struct Wire {
        /// Writes and flushes do not complete while set.
        stalled: core::cell::Cell<bool>,
        /// Writes fail while set.
        write_fails: core::cell::Cell<bool>,
        /// Flushes fail while set.
        flush_fails: core::cell::Cell<bool>,
        /// Reads stay pending until this is set, and then fail.
        read_fails: core::cell::Cell<bool>,
        /// Transport operations started.
        operations: core::cell::Cell<usize>,
        /// Writes started.
        writes: core::cell::Cell<usize>,
    }

    /// A reliable link's transport, driven by a [`Wire`].
    struct Wired(Rc<Wire>);

    impl ErrorType for Wired {
        type Error = Broken;
    }

    impl Read for Wired {
        async fn read(&mut self, _: &mut [u8]) -> Result<usize, Broken> {
            self.0.operations.set(self.0.operations.get() + 1);
            poll_fn(|_| {
                if self.0.read_fails.get() {
                    Poll::Ready(Err(Broken(7)))
                } else {
                    Poll::Pending
                }
            })
            .await
        }
    }

    impl Write for Wired {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Broken> {
            self.0.operations.set(self.0.operations.get() + 1);
            self.0.writes.set(self.0.writes.get() + 1);
            poll_fn(|_| {
                if self.0.write_fails.get() {
                    Poll::Ready(Err(Broken(3)))
                } else if self.0.stalled.get() {
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(buf.len()))
                }
            })
            .await
        }

        async fn flush(&mut self) -> Result<(), Broken> {
            self.0.operations.set(self.0.operations.get() + 1);
            poll_fn(|_| {
                if self.0.stalled.get() {
                    Poll::Pending
                } else if self.0.flush_fails.get() {
                    Poll::Ready(Err(Broken(5)))
                } else {
                    Poll::Ready(Ok(()))
                }
            })
            .await
        }
    }

    /// A retransmission timer that never fires: the acknowledgement never comes.
    struct Idle;

    /// A retransmission timer that fires when the test sets its flag.
    #[derive(Clone)]
    struct Manual(Rc<core::cell::Cell<bool>>);

    impl crate::link::Timer for Idle {
        fn start(&mut self, _: Duration) {}

        fn stop(&mut self) {}

        fn poll_expired(&mut self, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    impl crate::link::Timer for Manual {
        fn start(&mut self, _: Duration) {}

        fn stop(&mut self) {}

        fn poll_expired(&mut self, _: &mut Context<'_>) -> Poll<()> {
            if self.0.replace(false) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    /// Polling a parked worker does nothing: it neither polls the engine nor
    /// publishes anything more.
    fn assert_parked(
        worker: &mut Pin<&mut impl Future<Output = ()>>,
        wire: &Wire,
        port: &Shared<Port<ReliableError<Broken>>>,
    ) {
        let operations = wire.operations.get();
        for _ in 0..3 {
            assert!(poll_once(worker.as_mut()).is_pending());
        }
        assert_eq!(
            wire.operations.get(),
            operations,
            "the engine was polled again"
        );
        assert!(
            !port.with(|p| p.has_event()),
            "a second outcome was published"
        );
    }

    /// A read that fails while a flush is pending: the flush is never polled, so
    /// it does not answer with `Closed`, and the read's error is published.
    #[test]
    fn reliable_worker_publishes_the_read_error_over_a_pending_flush() {
        let wire = Rc::new(Wire::default());
        let mut link = crate::link::reliable(Wired(wire.clone()))
            .with_retransmission_timer(Idle)
            .build();
        let port = Shared::new(Port::new());
        port.with(|p| p.request_write(b"hello"));
        let mut worker = pin!(run_reliable(&mut link, &port));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(5)))
        ));

        // The flush waits for an acknowledgement that never comes.
        port.with(|p| p.request_flush());
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(!port.with(|p| p.has_reply()), "the flush is still pending");

        wire.read_fails.set(true);
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_read()),
            Some(Err(ReliableError(ArqError::Io(LinkError::Io(Broken(7))))))
        ));
        assert!(
            !port.with(|p| p.has_reply()),
            "the flush was polled for `Closed`"
        );
        assert_parked(&mut worker, &wire, &port);
    }

    /// A retransmission while the flush is pending, then a read failure: the
    /// retransmission is an ordinary transport write, and the failure is still
    /// the only outcome published.
    #[test]
    fn reliable_worker_publishes_the_read_error_after_a_retransmission() {
        let wire = Rc::new(Wire::default());
        let expired = Rc::new(core::cell::Cell::new(false));
        let mut link = crate::link::reliable(Wired(wire.clone()))
            .with_retransmission_timer(Manual(expired.clone()))
            .build();
        let port = Shared::new(Port::new());
        port.with(|p| p.request_write(b"hello"));
        let mut worker = pin!(run_reliable(&mut link, &port));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(5)))
        ));

        port.with(|p| p.request_flush());
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(!port.with(|p| p.has_reply()), "the flush is still pending");

        let writes = wire.writes.get();
        expired.set(true);
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(wire.writes.get() > writes, "the timer did not retransmit");
        assert!(!port.with(|p| p.has_reply()), "the flush completed early");

        wire.read_fails.set(true);
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_read()),
            Some(Err(ReliableError(ArqError::Io(LinkError::Io(Broken(7))))))
        ));
        assert!(
            !port.with(|p| p.has_reply()),
            "the flush was polled for `Closed`"
        );
        assert_parked(&mut worker, &wire, &port);
    }

    /// A write that fails on transmission: the engine transmits in its read poll,
    /// so the error is the read's outcome. It is published once, and the pending
    /// flush is not polled for `Closed` afterwards.
    #[test]
    fn reliable_worker_publishes_a_transmit_error_once_and_parks() {
        let wire = Rc::new(Wire::default());
        wire.write_fails.set(true);
        let mut link = crate::link::reliable(Wired(wire.clone()))
            .with_retransmission_timer(Idle)
            .build();
        let port = Shared::new(Port::new());
        port.with(|p| p.request_write(b"hello"));
        let mut worker = pin!(run_reliable(&mut link, &port));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(5)))
        ));

        port.with(|p| p.request_flush());
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_read()),
            Some(Err(ReliableError(ArqError::Io(LinkError::Io(Broken(3))))))
        ));
        assert!(
            !port.with(|p| p.has_reply()),
            "the flush was polled for `Closed`"
        );
        assert_parked(&mut worker, &wire, &port);
    }

    /// The retry limit of the ARQ engine (its default, not configurable here).
    const RETRY_LIMIT: usize = 16;

    /// Acknowledgements never come and the retransmission timer fires each time
    /// it is polled. The engine retransmits up to its retry limit, then fails
    /// with `Timeout`: that is the outcome published, not `Closed` or an I/O
    /// error, and the pending flush gets no reply.
    #[test]
    fn reliable_worker_publishes_retry_exhaustion_as_a_timeout_and_parks() {
        let wire = Rc::new(Wire::default());
        let expired = Rc::new(core::cell::Cell::new(false));
        let mut link = crate::link::reliable(Wired(wire.clone()))
            .with_retransmission_timer(Manual(expired.clone()))
            .build();
        let port = Shared::new(Port::new());
        port.with(|p| p.request_write(b"hello"));
        let mut worker = pin!(run_reliable(&mut link, &port));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(5)))
        ));
        port.with(|p| p.request_flush());
        assert!(poll_once(worker.as_mut()).is_pending());
        // The data frame and the flush's acknowledgement request, before any expiry.
        let sent = wire.writes.get();

        // Each expiry retransmits the one unacknowledged frame, and the expiry
        // after the last retry fails the link.
        for _ in 0..=RETRY_LIMIT + 1 {
            if port.with(|p| p.has_event()) {
                break;
            }
            expired.set(true);
            assert!(poll_once(worker.as_mut()).is_pending());
        }
        assert_eq!(
            wire.writes.get() - sent,
            RETRY_LIMIT,
            "the frame was not retransmitted once per retry"
        );
        assert!(matches!(
            port.with(|p| p.take_read()),
            Some(Err(ReliableError(ArqError::Timeout)))
        ));
        assert!(
            !port.with(|p| p.has_reply()),
            "the flush was polled after the timeout"
        );
        assert_parked(&mut worker, &wire, &port);
    }

    /// A write whose transmission fails inside the write poll. A read outcome is
    /// held so that the worker never polls the receive side, and the first write
    /// only queues its bytes: the second write transmits them and fails. The
    /// error is that write's result, once; the held read is not replaced.
    #[test]
    fn reliable_worker_publishes_a_transmit_error_in_the_write_result() {
        let wire = Rc::new(Wire::default());
        let mut link = crate::link::reliable(Wired(wire.clone()))
            .with_retransmission_timer(Idle)
            .build();
        let port = Shared::new(Port::new());
        port.with(|p| p.read = Some(Ok(0)));
        let mut worker = pin!(run_reliable(&mut link, &port));

        port.with(|p| p.request_write(b"hello"));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(5)))
        ));

        wire.write_fails.set(true);
        port.with(|p| p.request_write(b"world"));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Err(ReliableError(ArqError::Io(
                LinkError::Io(Broken(3))
            )))))
        ));
        assert!(matches!(port.with(|p| p.take_read()), Some(Ok(0))));
        assert_parked(&mut worker, &wire, &port);
    }

    /// A flush whose transport flush fails. Nothing is outstanding, so the engine
    /// flushes the transport inside the flush poll; the error is the flush's
    /// result, once, and the held read is not replaced.
    #[test]
    fn reliable_worker_publishes_a_transport_flush_error_in_the_flush_result() {
        let wire = Rc::new(Wire::default());
        wire.flush_fails.set(true);
        let mut link = crate::link::reliable(Wired(wire.clone()))
            .with_retransmission_timer(Idle)
            .build();
        let port = Shared::new(Port::new());
        port.with(|p| p.read = Some(Ok(0)));
        let mut worker = pin!(run_reliable(&mut link, &port));

        port.with(|p| p.request_flush());
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Flushed(Err(ReliableError(ArqError::Io(
                LinkError::Io(Broken(5))
            )))))
        ));
        assert!(matches!(port.with(|p| p.take_read()), Some(Ok(0))));
        assert_parked(&mut worker, &wire, &port);
    }

    /// Retry exhaustion reached through the flush's result: a read outcome is held,
    /// so the timer is only polled by the flush, and the `Timeout` is its result.
    #[test]
    fn reliable_worker_publishes_retry_exhaustion_in_the_flush_result() {
        let wire = Rc::new(Wire::default());
        let expired = Rc::new(core::cell::Cell::new(false));
        let mut link = crate::link::reliable(Wired(wire.clone()))
            .with_retransmission_timer(Manual(expired.clone()))
            .build();
        let port = Shared::new(Port::new());
        port.with(|p| p.read = Some(Ok(0)));
        let mut worker = pin!(run_reliable(&mut link, &port));
        port.with(|p| p.request_write(b"hello"));
        assert!(poll_once(worker.as_mut()).is_pending());
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Written(Ok(5)))
        ));
        port.with(|p| p.request_flush());
        assert!(poll_once(worker.as_mut()).is_pending());
        let sent = wire.writes.get();

        for _ in 0..=RETRY_LIMIT + 1 {
            if port.with(|p| p.has_reply()) {
                break;
            }
            expired.set(true);
            assert!(poll_once(worker.as_mut()).is_pending());
        }
        assert_eq!(
            wire.writes.get() - sent,
            RETRY_LIMIT,
            "the frame was not retransmitted once per retry"
        );
        assert!(matches!(
            port.with(|p| p.take_reply()),
            Some(Reply::Flushed(Err(ReliableError(ArqError::Timeout))))
        ));
        assert!(matches!(port.with(|p| p.take_read()), Some(Ok(0))));
        assert_parked(&mut worker, &wire, &port);
    }
}
