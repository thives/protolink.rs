//! `blocking::serve_wakeable` polls a `Pending` handler again as soon as it is
//! woken, without peer traffic and without a transport read timeout.
//!
//! The server runs on its own thread over `ChannelIo`, a transport whose reads
//! block on a channel. Waking the server sends a marker through that channel,
//! which also gives the latching the `WakeableRead` contract requires. A lost
//! wake-up shows up as a missing message: every wait of the test peer has a
//! timeout, so a regression fails instead of hanging.
#![cfg(all(feature = "blocking", feature = "std"))]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use embedded_io::{ErrorKind, ErrorType, Read, Write};
use protolink::blocking::{WakeHandle, WakeableRead, serve, serve_wakeable};
use protolink::grpc::Client;
use protolink::{
    CallContext, CallId, ClientConfig, Error, Handler, MethodKind, Next, ServerConfig, Status,
};

const WATCH: &str = "/t.T/Watch";
const TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Transport

enum Msg {
    Data(Vec<u8>),
    Wake,
    Eof,
    Error(ErrorKind),
}

/// The server's end of the connection.
struct ChannelIo {
    rx: Receiver<Msg>,
    wake_tx: Sender<Msg>,
    out: Sender<Vec<u8>>,
    pending: VecDeque<u8>,
}

impl ChannelIo {
    fn copy_pending(&mut self, buf: &mut [u8]) -> usize {
        let n = buf.len().min(self.pending.len());
        for (slot, byte) in buf.iter_mut().zip(self.pending.drain(..n)) {
            *slot = byte;
        }
        n
    }
}

impl ErrorType for ChannelIo {
    type Error = ErrorKind;
}

impl Write for ChannelIo {
    fn write(&mut self, buf: &[u8]) -> Result<usize, ErrorKind> {
        self.out
            .send(buf.to_vec())
            .map_err(|_| ErrorKind::BrokenPipe)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), ErrorKind> {
        Ok(())
    }
}

/// A plain read with a 20 ms timeout, as a socket with `SO_RCVTIMEO` would do.
impl Read for ChannelIo {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, ErrorKind> {
        loop {
            if !self.pending.is_empty() {
                return Ok(self.copy_pending(buf));
            }
            match self.rx.recv_timeout(Duration::from_millis(20)) {
                Ok(Msg::Data(data)) => self.pending.extend(data),
                Ok(Msg::Wake) | Err(RecvTimeoutError::Timeout) => return Err(ErrorKind::TimedOut),
                Ok(Msg::Eof) | Err(RecvTimeoutError::Disconnected) => return Ok(0),
                Ok(Msg::Error(kind)) => return Err(kind),
            }
        }
    }
}

impl WakeableRead for ChannelIo {
    fn wake_handle(&self) -> WakeHandle {
        let tx = self.wake_tx.clone();
        WakeHandle::new(move || {
            let _ = tx.send(Msg::Wake);
        })
    }

    fn read_or_wake(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ErrorKind> {
        loop {
            if !self.pending.is_empty() {
                return Ok(Some(self.copy_pending(buf)));
            }
            match self.rx.recv() {
                Ok(Msg::Data(data)) => self.pending.extend(data),
                Ok(Msg::Wake) => return Ok(None),
                Ok(Msg::Eof) | Err(_) => return Ok(Some(0)),
                Ok(Msg::Error(kind)) => return Err(kind),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers

#[derive(Default)]
struct State {
    events: VecDeque<Vec<u8>>,
    finished: bool,
}

/// Shared between the `Watch` handler (on the server thread) and the test.
#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    waker: Mutex<Option<Waker>>,
    polls: AtomicUsize,
    cancelled: AtomicUsize,
}

impl Shared {
    fn push(&self, message: &[u8]) {
        self.state
            .lock()
            .unwrap()
            .events
            .push_back(message.to_vec());
        self.wake();
    }

    fn finish(&self) {
        self.state.lock().unwrap().finished = true;
        self.wake();
    }

    fn wake(&self) {
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

/// A server-streaming method that answers with whatever the test pushes.
struct Watch(Arc<Shared>);

impl Handler for Watch {
    fn call(&mut self, _: &CallContext<'_>, _: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        None
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        (path == WATCH).then_some(MethodKind::ServerStreaming)
    }

    fn on_message(&mut self, _: &CallContext<'_>, _: &[u8]) -> Result<(), Status> {
        Ok(())
    }

    fn poll_response(&mut self, _: &CallContext<'_>, cx: &mut Context<'_>) -> Poll<Next<Vec<u8>>> {
        self.0.polls.fetch_add(1, SeqCst);
        // The waker is stored while the state is locked, so a push between
        // the check and the store can't be missed.
        let mut state = self.0.state.lock().unwrap();
        if let Some(message) = state.events.pop_front() {
            return Poll::Ready(Next::Message(message));
        }
        if state.finished {
            return Poll::Ready(Next::Done(Ok(())));
        }
        *self.0.waker.lock().unwrap() = Some(cx.waker().clone());
        Poll::Pending
    }

    fn on_cancel(&mut self, _: &CallContext<'_>) {
        self.0.cancelled.fetch_add(1, SeqCst);
    }
}

/// Wakes itself from inside `poll_response` and returns `Pending`, twice.
/// Nothing else wakes the server, so the third poll only happens if a wake
/// that arrives while the server is not reading is latched.
struct SelfWake {
    polls: Arc<AtomicUsize>,
}

impl Handler for SelfWake {
    fn call(&mut self, _: &CallContext<'_>, _: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        None
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        (path == WATCH).then_some(MethodKind::ServerStreaming)
    }

    fn on_message(&mut self, _: &CallContext<'_>, _: &[u8]) -> Result<(), Status> {
        Ok(())
    }

    fn poll_response(&mut self, _: &CallContext<'_>, cx: &mut Context<'_>) -> Poll<Next<Vec<u8>>> {
        match self.polls.fetch_add(1, SeqCst) + 1 {
            1 | 2 => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            3 => Poll::Ready(Next::Message(b"late".to_vec())),
            _ => Poll::Ready(Next::Done(Ok(()))),
        }
    }
}

// ---------------------------------------------------------------------------
// Test peer

type ServerResult = Result<(), Error<ErrorKind>>;

/// The client's end of the connection.
struct Peer {
    to_server: Sender<Msg>,
    from_server: Receiver<Vec<u8>>,
    client: Client,
}

fn connect() -> (ChannelIo, Peer) {
    let (to_server, rx) = mpsc::channel();
    let (out, from_server) = mpsc::channel();
    let io = ChannelIo {
        rx,
        wake_tx: to_server.clone(),
        out,
        pending: VecDeque::new(),
    };
    let peer = Peer {
        to_server,
        from_server,
        client: Client::new(ClientConfig::default()),
    };
    (io, peer)
}

impl Peer {
    fn flush(&mut self) {
        let out = self.client.take_output();
        if !out.is_empty() {
            let _ = self.to_server.send(Msg::Data(out));
        }
    }

    fn feed(&mut self, bytes: &[u8]) {
        self.client.recv(bytes).expect("valid server output");
        self.flush();
    }

    /// Start a call of `path` with one request and half-close it.
    fn start(&mut self, path: &str) -> CallId {
        let id = self.client.start_streaming(path).unwrap();
        self.client.send_message(id, b"request").unwrap();
        self.client.close_send(id).unwrap();
        self.flush();
        id
    }

    /// Exchange handshake traffic until the server has been quiet for a while,
    /// so it is idle in its read afterwards.
    fn settle(&mut self) {
        while let Ok(bytes) = self.from_server.recv_timeout(Duration::from_millis(100)) {
            self.feed(&bytes);
        }
    }

    /// The next message or the end of the call, waiting for server output.
    fn next(&mut self, id: CallId) -> Next<Vec<u8>> {
        loop {
            if let Some(next) = self.client.try_next(id) {
                return next;
            }
            let bytes = self
                .from_server
                .recv_timeout(TIMEOUT)
                .expect("the server sent nothing: a wake-up was lost");
            self.feed(&bytes);
        }
    }

    fn expect_message(&mut self, id: CallId, expected: &[u8]) {
        match self.next(id) {
            Next::Message(message) => assert_eq!(message, expected),
            Next::Done(result) => panic!("expected a message, call ended with {result:?}"),
        }
    }

    fn expect_end(&mut self, id: CallId) {
        match self.next(id) {
            Next::Done(Ok(())) => {}
            Next::Done(Err(status)) => panic!("call failed: {status:?}"),
            Next::Message(_) => panic!("expected the end of the call, got a message"),
        }
    }

    fn eof(&self) {
        let _ = self.to_server.send(Msg::Eof);
    }
}

fn spawn_wakeable<H: Handler + Send + 'static>(
    io: ChannelIo,
    mut handler: H,
) -> JoinHandle<ServerResult> {
    thread::spawn(move || serve_wakeable(io, &mut handler, ServerConfig::default()))
}

fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(start.elapsed() < TIMEOUT, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(2));
    }
}

// ---------------------------------------------------------------------------
// Tests

#[test]
fn wake_without_input_delivers_response() {
    let (io, mut peer) = connect();
    let shared = Arc::new(Shared::default());
    let server = spawn_wakeable(io, Watch(shared.clone()));

    let id = peer.start(WATCH);
    wait_until("the first poll", || shared.polls.load(SeqCst) >= 1);
    // Handshake traffic is over: from here on only a wake can move the server.
    peer.settle();

    shared.push(b"hello");
    peer.expect_message(id, b"hello");
    shared.push(b"again");
    peer.expect_message(id, b"again");
    shared.finish();
    peer.expect_end(id);

    peer.eof();
    server.join().unwrap().unwrap();
    assert_eq!(shared.cancelled.load(SeqCst), 0);
}

#[test]
fn wake_during_poll_is_not_lost() {
    let (io, mut peer) = connect();
    let polls = Arc::new(AtomicUsize::new(0));
    let server = spawn_wakeable(
        io,
        SelfWake {
            polls: polls.clone(),
        },
    );

    let id = peer.start(WATCH);
    // The peer does not read yet, so it can't send anything that would wake
    // the server in the meantime.
    wait_until("three polls", || polls.load(SeqCst) >= 3);

    peer.expect_message(id, b"late");
    peer.expect_end(id);
    peer.eof();
    server.join().unwrap().unwrap();
}

#[test]
fn spurious_wakes_are_harmless() {
    let (io, mut peer) = connect();
    let shared = Arc::new(Shared::default());
    let server = spawn_wakeable(io, Watch(shared.clone()));

    let id = peer.start(WATCH);
    wait_until("the first poll", || shared.polls.load(SeqCst) >= 1);
    peer.settle();

    let polls_before = shared.polls.load(SeqCst);
    for _ in 0..5 {
        let _ = peer.to_server.send(Msg::Wake);
    }
    wait_until("the spurious wakes to be polled", || {
        shared.polls.load(SeqCst) > polls_before
    });

    shared.push(b"still serving");
    peer.expect_message(id, b"still serving");
    shared.finish();
    peer.expect_end(id);
    peer.eof();
    server.join().unwrap().unwrap();
}

#[test]
fn many_wakes_deliver_every_message_in_order() {
    const COUNT: usize = 300;
    let (io, mut peer) = connect();
    let shared = Arc::new(Shared::default());
    let server = spawn_wakeable(io, Watch(shared.clone()));

    let id = peer.start(WATCH);
    wait_until("the first poll", || shared.polls.load(SeqCst) >= 1);
    peer.settle();

    let producer = {
        let shared = shared.clone();
        thread::spawn(move || {
            for i in 0..COUNT {
                shared.push(format!("event {i}").as_bytes());
                if i % 7 == 0 {
                    thread::yield_now();
                }
            }
            shared.finish();
        })
    };
    for i in 0..COUNT {
        peer.expect_message(id, format!("event {i}").as_bytes());
    }
    peer.expect_end(id);
    producer.join().unwrap();
    peer.eof();
    server.join().unwrap().unwrap();
}

#[test]
fn timed_out_read_is_an_idle_tick() {
    let (io, mut peer) = connect();
    let shared = Arc::new(Shared::default());
    let server = spawn_wakeable(io, Watch(shared.clone()));

    let id = peer.start(WATCH);
    wait_until("the first poll", || shared.polls.load(SeqCst) >= 1);
    peer.settle();

    let _ = peer.to_server.send(Msg::Error(ErrorKind::TimedOut));
    shared.push(b"after the tick");
    peer.expect_message(id, b"after the tick");
    shared.finish();
    peer.expect_end(id);
    peer.eof();
    server.join().unwrap().unwrap();
}

#[test]
fn end_of_stream_cancels_active_calls() {
    let (io, mut peer) = connect();
    let shared = Arc::new(Shared::default());
    let server = spawn_wakeable(io, Watch(shared.clone()));

    peer.start(WATCH);
    wait_until("the first poll", || shared.polls.load(SeqCst) >= 1);
    peer.settle();

    peer.eof();
    server.join().unwrap().unwrap();
    assert_eq!(shared.cancelled.load(SeqCst), 1);
}

#[test]
fn read_error_ends_serving_and_cancels_active_calls() {
    let (io, mut peer) = connect();
    let shared = Arc::new(Shared::default());
    let server = spawn_wakeable(io, Watch(shared.clone()));

    peer.start(WATCH);
    wait_until("the first poll", || shared.polls.load(SeqCst) >= 1);
    peer.settle();

    let _ = peer.to_server.send(Msg::Error(ErrorKind::BrokenPipe));
    match server.join().unwrap() {
        Err(Error::Io(ErrorKind::BrokenPipe)) => {}
        other => panic!("unexpected result: {other:?}"),
    }
    assert_eq!(shared.cancelled.load(SeqCst), 1);
}

/// The timeout-driven path of plain `serve` keeps working: the handler's wake
/// goes nowhere, and the response follows on the next read timeout.
#[test]
fn plain_serve_still_progresses_on_read_timeouts() {
    let (io, mut peer) = connect();
    let shared = Arc::new(Shared::default());
    let served = Arc::new(AtomicBool::new(false));
    let server = {
        let shared = shared.clone();
        let served = served.clone();
        thread::spawn(move || {
            let mut handler = Watch(shared);
            let result = serve(io, &mut handler, ServerConfig::default());
            served.store(true, SeqCst);
            result
        })
    };

    let id = peer.start(WATCH);
    wait_until("the first poll", || shared.polls.load(SeqCst) >= 1);
    peer.settle();

    shared.push(b"on a tick");
    peer.expect_message(id, b"on a tick");
    shared.finish();
    peer.expect_end(id);

    assert!(!served.load(SeqCst));
    peer.eof();
    server.join().unwrap().unwrap();
}
