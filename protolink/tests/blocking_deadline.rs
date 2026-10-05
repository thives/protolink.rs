//! Deadlines with the blocking drivers.
//!
//! The server runs on its own thread over `ChannelIo`, whose reads honour the
//! read timeout the driver sets (`ReadTimeout`), like a socket with
//! `SO_RCVTIMEO`. The client tests use `Silent`, a transport whose peer never
//! answers. Every wait has a bound, so a regression fails instead of hanging.
#![cfg(all(feature = "blocking", feature = "std"))]

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use embedded_io::{ErrorKind, ErrorType, Read, Write};
use protolink::blocking::{
    ReadTimeout, WakeHandle, WakeableRead, serve, serve_wakeable_with_clock, serve_with_clock,
};
use protolink::grpc::Client;
use protolink::{
    CallContext, CallOptions, ClientConfig, Clock, Code, Error, Handler, MethodKind, Next,
    ServerConfig, Status,
};

const QUIET: &str = "/t.T/Quiet";
const TIMEOUT: Duration = Duration::from_secs(10);
const DEADLINE: Duration = Duration::from_millis(100);
/// How long a read without a timeout hint waits before reporting `TimedOut`.
const IDLE: Duration = Duration::from_millis(20);

/// A [`Clock`] on the wall clock.
struct Wall(Instant);

impl Wall {
    fn new() -> Self {
        Self(Instant::now())
    }
}

impl Clock for Wall {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}

// ---------------------------------------------------------------------------
// Server

enum Msg {
    Data(Vec<u8>),
    Wake,
}

/// The server's end of the connection.
struct ChannelIo {
    rx: Receiver<Msg>,
    wake_tx: Sender<Msg>,
    out: Sender<Vec<u8>>,
    timeout: Option<Duration>,
    /// Every read timeout the driver set.
    timeouts: Arc<Mutex<Vec<Option<Duration>>>>,
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

impl ChannelIo {
    fn wait(&mut self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, ErrorKind> {
        match self.rx.recv_timeout(timeout.unwrap_or(IDLE)) {
            Ok(Msg::Data(data)) => Ok(Some(data)),
            Ok(Msg::Wake) => Ok(None),
            Err(RecvTimeoutError::Timeout) => Err(ErrorKind::TimedOut),
            Err(RecvTimeoutError::Disconnected) => Ok(Some(Vec::new())),
        }
    }

    fn copy(data: &[u8], buf: &mut [u8]) -> usize {
        assert!(data.len() <= buf.len(), "test messages fit one read");
        buf[..data.len()].copy_from_slice(data);
        data.len()
    }
}

impl Read for ChannelIo {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, ErrorKind> {
        let timeout = self.timeout;
        loop {
            if let Some(data) = self.wait(timeout)? {
                return Ok(Self::copy(&data, buf));
            }
        }
    }
}

impl ReadTimeout for ChannelIo {
    fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.timeouts.lock().unwrap().push(timeout);
        self.timeout = timeout;
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
        self.read_or_wake_timeout(buf, None)
    }

    fn read_or_wake_timeout(
        &mut self,
        buf: &mut [u8],
        timeout: Option<Duration>,
    ) -> Result<Option<usize>, ErrorKind> {
        self.timeouts.lock().unwrap().push(timeout);
        Ok(self.wait(timeout)?.map(|data| Self::copy(&data, buf)))
    }
}

/// A server-streaming method that never answers.
struct Quiet(Arc<AtomicUsize>);

impl Handler for Quiet {
    fn call(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        None
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        (path == QUIET).then_some(MethodKind::ServerStreaming)
    }

    fn on_message(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
        Ok(())
    }

    fn poll_response(
        &mut self,
        _: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        Poll::Pending
    }

    fn on_cancel(&mut self, _: &mut CallContext<'_>) {
        self.0.fetch_add(1, SeqCst);
    }
}

/// The client's end of the connection.
struct Peer {
    to_server: Sender<Msg>,
    from_server: Receiver<Vec<u8>>,
    client: Client,
    cancelled: Arc<AtomicUsize>,
    timeouts: Arc<Mutex<Vec<Option<Duration>>>>,
}

type ServerResult = Result<(), Error<ErrorKind>>;

fn connect() -> (ChannelIo, Peer) {
    let (to_server, rx) = mpsc::channel();
    let (out, from_server) = mpsc::channel();
    let timeouts = Arc::new(Mutex::new(Vec::new()));
    let io = ChannelIo {
        rx,
        wake_tx: to_server.clone(),
        out,
        timeout: None,
        timeouts: timeouts.clone(),
    };
    let peer = Peer {
        to_server,
        from_server,
        client: Client::new(ClientConfig::default()),
        cancelled: Arc::new(AtomicUsize::new(0)),
        timeouts,
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

    fn handler(&self) -> Quiet {
        Quiet(self.cancelled.clone())
    }

    /// Start a `Quiet` call with `grpc-timeout` of `timeout`.
    fn start(&mut self, timeout: Duration) -> protolink::CallId {
        let id = self
            .client
            .start_streaming_with(QUIET, &CallOptions::timeout(timeout))
            .unwrap();
        self.client.send_message(id, b"request").unwrap();
        self.flush();
        id
    }

    /// Wait for the end of the call, at most `limit`; `None` if it didn't end.
    fn end(&mut self, id: protolink::CallId, limit: Duration) -> Option<Result<(), Status>> {
        let start = Instant::now();
        loop {
            match self.client.try_next(id) {
                Some(Next::Done(result)) => return Some(result),
                Some(Next::Message(_)) => panic!("unexpected message"),
                None => {}
            }
            let left = limit.checked_sub(start.elapsed())?;
            match self.from_server.recv_timeout(left) {
                Ok(bytes) => {
                    self.client.recv(&bytes).expect("valid server output");
                    self.flush();
                }
                Err(_) => return None,
            }
        }
    }

    fn eof(&self) {
        let _ = self.to_server.send(Msg::Data(Vec::new()));
    }
}

fn spawn<F>(io: ChannelIo, handler: Quiet, run: F) -> JoinHandle<ServerResult>
where
    F: FnOnce(ChannelIo, &mut Quiet) -> ServerResult + Send + 'static,
{
    let mut handler = handler;
    thread::spawn(move || run(io, &mut handler))
}

fn expired(result: Option<Result<(), Status>>) {
    let status = result
        .expect("the call did not end")
        .expect_err("the call should have failed");
    assert_eq!(status.code, Code::DeadlineExceeded, "{status:?}");
}

#[test]
fn server_with_a_clock_expires_a_quiet_call() {
    let (io, mut peer) = connect();
    let handler = peer.handler();
    let server = spawn(io, handler, |io, h| {
        serve_with_clock(io, h, ServerConfig::default(), Wall::new())
    });
    let started = Instant::now();
    let id = peer.start(DEADLINE);
    expired(peer.end(id, TIMEOUT));
    assert!(started.elapsed() >= DEADLINE, "expired early");
    assert_eq!(peer.cancelled.load(SeqCst), 1);
    // The read timeout was set to the time left until the deadline.
    let timeouts = peer.timeouts.lock().unwrap().clone();
    assert!(
        timeouts.iter().any(|t| t.is_some_and(|t| t <= DEADLINE)),
        "{timeouts:?}"
    );
    peer.eof();
    server.join().unwrap().unwrap();
}

#[test]
fn wakeable_server_with_a_clock_expires_a_quiet_call() {
    let (io, mut peer) = connect();
    let handler = peer.handler();
    let server = spawn(io, handler, |io, h| {
        serve_wakeable_with_clock(io, h, ServerConfig::default(), Wall::new())
    });
    let started = Instant::now();
    let id = peer.start(DEADLINE);
    expired(peer.end(id, TIMEOUT));
    assert!(started.elapsed() >= DEADLINE, "expired early");
    assert_eq!(peer.cancelled.load(SeqCst), 1);
    let timeouts = peer.timeouts.lock().unwrap().clone();
    assert!(
        timeouts.iter().any(|t| t.is_some_and(|t| t <= DEADLINE)),
        "{timeouts:?}"
    );
    peer.eof();
    server.join().unwrap().unwrap();
}

#[test]
fn server_without_a_clock_does_not_enforce_the_deadline() {
    let (io, mut peer) = connect();
    let handler = peer.handler();
    let server = spawn(io, handler, |io, h| serve(io, h, ServerConfig::default()));
    let id = peer.start(DEADLINE);
    assert_eq!(peer.end(id, DEADLINE * 4), None);
    assert_eq!(peer.cancelled.load(SeqCst), 0);
    peer.eof();
    server.join().unwrap().unwrap();
    // The connection ending cancels the call.
    assert_eq!(peer.cancelled.load(SeqCst), 1);
}

#[test]
fn a_call_without_a_timeout_is_not_expired() {
    let (io, mut peer) = connect();
    let handler = peer.handler();
    let server = spawn(io, handler, |io, h| {
        serve_with_clock(io, h, ServerConfig::default(), Wall::new())
    });
    let id = peer.client.start_streaming(QUIET).unwrap();
    peer.client.send_message(id, b"request").unwrap();
    peer.flush();
    assert_eq!(peer.end(id, DEADLINE * 4), None);
    assert_eq!(peer.cancelled.load(SeqCst), 0);
    peer.eof();
    server.join().unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Client

/// A transport whose peer never answers. Reads sleep for the read timeout the
/// driver set, then time out; a read without a timeout would block forever,
/// so it panics.
struct Silent {
    timeout: Option<Duration>,
    written: Arc<AtomicUsize>,
}

impl Silent {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let written = Arc::new(AtomicUsize::new(0));
        let io = Self {
            timeout: None,
            written: written.clone(),
        };
        (io, written)
    }
}

impl ErrorType for Silent {
    type Error = ErrorKind;
}

impl Write for Silent {
    fn write(&mut self, buf: &[u8]) -> Result<usize, ErrorKind> {
        self.written.fetch_add(buf.len(), SeqCst);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), ErrorKind> {
        Ok(())
    }
}

impl Read for Silent {
    fn read(&mut self, _: &mut [u8]) -> Result<usize, ErrorKind> {
        let timeout = self
            .timeout
            .expect("a read that would block forever: no read timeout was set");
        thread::sleep(timeout);
        Err(ErrorKind::TimedOut)
    }
}

impl ReadTimeout for Silent {
    fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
    }
}

#[test]
fn client_unary_times_out() {
    let (io, written) = Silent::new();
    let mut client =
        protolink::blocking::Client::with_clock(io, ClientConfig::default(), Wall::new());
    let started = Instant::now();
    let err = client
        .unary_with("/t.T/Echo", b"hi", CallOptions::timeout(DEADLINE))
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded, "{err:?}");
    assert!(started.elapsed() >= DEADLINE, "expired early");
    assert!(started.elapsed() < TIMEOUT);
    assert!(written.load(SeqCst) > 0, "the request was sent");
    // The reset of the expired call is queued for the next operation.
    assert!(client.with_inner(|c| c.has_output()));
}

#[test]
fn client_streaming_call_times_out_in_message() {
    let (io, _) = Silent::new();
    let client = protolink::blocking::Client::with_clock(io, ClientConfig::default(), Wall::new());
    let mut call = client
        .streaming_with("/t.T/Chat", CallOptions::timeout(DEADLINE))
        .unwrap();
    call.send(b"one").unwrap();
    let err = call.message().unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded, "{err:?}");
    // The outcome is sticky.
    assert_eq!(call.message().unwrap_err().code, Code::DeadlineExceeded);
}

#[test]
fn client_default_timeout_applies() {
    let (io, _) = Silent::new();
    let config = ClientConfig {
        default_timeout: Some(DEADLINE),
        ..ClientConfig::default()
    };
    let mut client = protolink::blocking::Client::with_clock(io, config, Wall::new());
    let err = client.unary("/t.T/Echo", b"hi").unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded, "{err:?}");
}

#[test]
fn client_zero_timeout_fails_without_sending() {
    let (io, written) = Silent::new();
    let mut client =
        protolink::blocking::Client::with_clock(io, ClientConfig::default(), Wall::new());
    let err = client
        .unary_with("/t.T/Echo", b"hi", CallOptions::timeout(Duration::ZERO))
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded, "{err:?}");
    assert_eq!(written.load(SeqCst), 0);
}

#[test]
fn client_without_a_clock_treats_a_timed_out_read_as_failure() {
    struct TimesOut;
    impl ErrorType for TimesOut {
        type Error = ErrorKind;
    }
    impl Write for TimesOut {
        fn write(&mut self, buf: &[u8]) -> Result<usize, ErrorKind> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> Result<(), ErrorKind> {
            Ok(())
        }
    }
    impl Read for TimesOut {
        fn read(&mut self, _: &mut [u8]) -> Result<usize, ErrorKind> {
            Err(ErrorKind::TimedOut)
        }
    }
    let mut client = protolink::blocking::Client::new(TimesOut, ClientConfig::default());
    let err = client
        .unary_with("/t.T/Echo", b"hi", CallOptions::timeout(DEADLINE))
        .unwrap_err();
    assert_eq!(err.code, Code::Unavailable, "{err:?}");
}
