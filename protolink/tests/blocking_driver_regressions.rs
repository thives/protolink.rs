//! Driver regressions also run with the library's `std` feature disabled.
#![cfg(feature = "blocking")]

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use embedded_io::{ErrorKind, ErrorType, Read, Write};
use protolink::blocking::{Client, OutputTimeout, ReadTimeout};
use protolink::grpc::FnHandler;
use protolink::{CallOptions, ClientConfig, Clock, Code, ServerConfig};

const BUDGET: Duration = Duration::from_millis(100);

#[derive(Clone, Default)]
struct Time(Rc<Cell<Duration>>);
impl Clock for Time {
    fn now(&self) -> Duration {
        self.0.get()
    }
}

#[derive(Clone, Copy)]
enum Fault {
    Partial,
    Flush,
    Read,
    Eof,
    Protocol,
    None,
    WriteTimeout,
    FlushTimeout,
}

#[derive(Default)]
struct Trace {
    io: usize,
    accepted: usize,
    reads: Vec<Option<Duration>>,
    writes: Vec<Option<Duration>>,
    flushes: Vec<Option<Duration>>,
}

struct Script {
    fault: Fault,
    time: Time,
    trace: Rc<RefCell<Trace>>,
    write_count: usize,
    timeout: Option<Duration>,
    write_cost: Duration,
    read_cost: Duration,
    peer: protolink::grpc::Server,
}

impl Script {
    fn new(fault: Fault) -> Self {
        Self {
            fault,
            time: Time::default(),
            trace: Rc::default(),
            write_count: 0,
            timeout: None,
            write_cost: Duration::ZERO,
            read_cost: Duration::ZERO,
            peer: protolink::grpc::Server::new(ServerConfig::default()),
        }
    }
    fn advance(&self, cost: Duration) {
        self.time.0.set(self.time.now() + cost);
    }
}
impl ErrorType for Script {
    type Error = ErrorKind;
}
impl Write for Script {
    fn write(&mut self, buf: &[u8]) -> Result<usize, ErrorKind> {
        self.trace.borrow_mut().io += 1;
        self.write_count += 1;
        if matches!(self.fault, Fault::WriteTimeout) {
            self.advance(BUDGET);
            return Err(ErrorKind::TimedOut);
        }
        if matches!(self.fault, Fault::Partial) {
            if self.write_count == 1 {
                self.trace.borrow_mut().accepted += 1;
                return Ok(1);
            }
            return Err(ErrorKind::BrokenPipe);
        }
        self.advance(self.write_cost);
        self.trace.borrow_mut().accepted += buf.len();
        if matches!(self.fault, Fault::None) {
            self.peer
                .recv(
                    buf,
                    &mut FnHandler(|_: &str, msg: &[u8]| Some(Ok(msg.to_vec()))),
                )
                .unwrap();
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> Result<(), ErrorKind> {
        self.trace.borrow_mut().io += 1;
        match self.fault {
            Fault::Flush => Err(ErrorKind::BrokenPipe),
            Fault::FlushTimeout => {
                self.advance(BUDGET);
                Err(ErrorKind::TimedOut)
            }
            _ => Ok(()),
        }
    }
}
impl Read for Script {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, ErrorKind> {
        self.trace.borrow_mut().io += 1;
        match self.fault {
            Fault::Read => Err(ErrorKind::BrokenPipe),
            Fault::Protocol => {
                buf[..9].copy_from_slice(&[0, 0, 0, 4, 0, 0, 0, 0, 1]);
                Ok(9)
            }
            Fault::None => {
                self.advance(self.read_cost);
                let n = self.peer.pending_output().len();
                buf[..n].copy_from_slice(self.peer.pending_output());
                self.peer.consume_output(n);
                Ok(n)
            }
            _ => Ok(0),
        }
    }
}
impl ReadTimeout for Script {
    fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
        self.trace.borrow_mut().reads.push(timeout);
    }
}
impl OutputTimeout for Script {
    fn set_write_timeout(&mut self, timeout: Option<Duration>) {
        self.trace.borrow_mut().writes.push(timeout);
    }
    fn set_flush_timeout(&mut self, timeout: Option<Duration>) {
        self.trace.borrow_mut().flushes.push(timeout);
    }
}

#[test]
fn failures_never_replay_or_reuse_the_transport() {
    for fault in [
        Fault::Partial,
        Fault::Flush,
        Fault::Read,
        Fault::Eof,
        Fault::Protocol,
    ] {
        let io = Script::new(fault);
        let trace = io.trace.clone();
        let mut client = Client::new(io, ClientConfig::default());
        assert_eq!(
            client.unary("/t.T/Echo", b"x").unwrap_err().code,
            Code::Unavailable
        );
        assert!(trace.borrow().accepted > 0);
        let count = trace.borrow().io;
        assert_eq!(
            client.unary("/t.T/Echo", b"again").unwrap_err().code,
            Code::Unavailable
        );
        assert!(client.streaming("/t.T/Chat").is_err());
        assert_eq!(trace.borrow().io, count);
    }
}

#[test]
fn streaming_send_after_failure_terminates_without_io() {
    let io = Script::new(Fault::Flush);
    let trace = io.trace.clone();
    let client = Client::new(io, ClientConfig::default());
    let mut call = client.streaming("/t.T/Chat").unwrap();
    call.send(b"one").unwrap();
    let count = trace.borrow().io;
    call.send(b"two").unwrap();
    call.close_send().unwrap();
    assert_eq!(call.message().unwrap_err().code, Code::Unavailable);
    assert_eq!(trace.borrow().io, count);
}

#[test]
fn read_budget_is_fresh_after_output() {
    let mut io = Script::new(Fault::None);
    io.write_cost = Duration::from_millis(80);
    let trace = io.trace.clone();
    let time = io.time.clone();
    let mut client = Client::with_clock(io, ClientConfig::default(), time);
    assert_eq!(
        client
            .unary_with("/t.T/Echo", b"x", CallOptions::timeout(BUDGET))
            .unwrap()
            .message,
        b"x"
    );
    assert_eq!(trace.borrow().reads, [Some(Duration::from_millis(20))]);
}

#[test]
fn late_response_is_expired_before_recv() {
    let mut io = Script::new(Fault::None);
    io.read_cost = Duration::from_millis(101);
    let time = io.time.clone();
    let mut client = Client::with_clock(io, ClientConfig::default(), time);
    assert_eq!(
        client
            .unary_with("/t.T/Echo", b"x", CallOptions::timeout(BUDGET))
            .unwrap_err()
            .code,
        Code::DeadlineExceeded
    );
}

#[test]
fn output_that_overruns_does_not_start_an_unbounded_read() {
    let mut io = Script::new(Fault::None);
    io.write_cost = Duration::from_millis(101);
    let trace = io.trace.clone();
    let time = io.time.clone();
    let mut client = Client::with_clock(io, ClientConfig::default(), time);
    assert_eq!(
        client
            .unary_with("/t.T/Echo", b"x", CallOptions::timeout(BUDGET))
            .unwrap_err()
            .code,
        Code::DeadlineExceeded
    );
    assert!(trace.borrow().reads.is_empty());
}

#[test]
fn write_and_flush_timeout_hooks_retire_the_connection() {
    for fault in [Fault::WriteTimeout, Fault::FlushTimeout] {
        let io = Script::new(fault);
        let trace = io.trace.clone();
        let time = io.time.clone();
        let mut client = Client::with_io_timeouts(io, ClientConfig::default(), time);
        assert_eq!(
            client
                .unary_with("/t.T/Echo", b"x", CallOptions::timeout(BUDGET))
                .unwrap_err()
                .code,
            Code::DeadlineExceeded
        );
        assert_eq!(trace.borrow().writes, [Some(BUDGET)]);
        if matches!(fault, Fault::FlushTimeout) {
            assert_eq!(trace.borrow().flushes, [Some(BUDGET)]);
        }
        let count = trace.borrow().io;
        assert_eq!(
            client.unary("/t.T/Echo", b"again").unwrap_err().code,
            Code::Unavailable
        );
        assert_eq!(trace.borrow().io, count);
    }
}

struct ServerIo {
    request: Option<Vec<u8>>,
    seen_input: bool,
    script: Script,
}
impl ErrorType for ServerIo {
    type Error = ErrorKind;
}
impl Read for ServerIo {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, ErrorKind> {
        if let Some(request) = self.request.take() {
            buf[..request.len()].copy_from_slice(&request);
            self.seen_input = true;
            Ok(request.len())
        } else {
            Ok(0)
        }
    }
}
impl Write for ServerIo {
    fn write(&mut self, buf: &[u8]) -> Result<usize, ErrorKind> {
        if !self.seen_input {
            return Ok(buf.len());
        }
        // This test transport accepts bytes without parsing the server role.
        if matches!(self.script.fault, Fault::None) {
            self.script.advance(self.script.write_cost);
            Ok(buf.len())
        } else {
            self.script.write(buf)
        }
    }
    fn flush(&mut self) -> Result<(), ErrorKind> {
        if self.seen_input {
            self.script.flush()
        } else {
            Ok(())
        }
    }
}
impl ReadTimeout for ServerIo {
    fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.script.set_read_timeout(timeout);
    }
}
impl OutputTimeout for ServerIo {
    fn set_write_timeout(&mut self, timeout: Option<Duration>) {
        self.script.set_write_timeout(timeout);
    }
    fn set_flush_timeout(&mut self, timeout: Option<Duration>) {
        self.script.set_flush_timeout(timeout);
    }
}
fn server_io(fault: Fault) -> ServerIo {
    let mut client = protolink::grpc::Client::new(ClientConfig::default());
    let id = client
        .start_streaming_with("/t.T/Chat", &CallOptions::timeout(BUDGET))
        .unwrap();
    client.send_message(id, b"x").unwrap();
    ServerIo {
        request: Some(client.take_output()),
        seen_input: false,
        script: Script::new(fault),
    }
}

#[derive(Default)]
struct Quiet {
    cancelled: usize,
}
impl protolink::Handler for Quiet {
    fn call(
        &mut self,
        _: &mut protolink::CallContext<'_>,
        _: &[u8],
    ) -> Option<Result<Vec<u8>, protolink::Status>> {
        None
    }
    fn method_kind(&self, _: &str) -> Option<protolink::MethodKind> {
        Some(protolink::MethodKind::BidiStreaming)
    }
    fn on_message(
        &mut self,
        _: &mut protolink::CallContext<'_>,
        _: &[u8],
    ) -> Result<(), protolink::Status> {
        Ok(())
    }
    fn poll_response(
        &mut self,
        _: &mut protolink::CallContext<'_>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<protolink::Next<Vec<u8>>> {
        std::task::Poll::Pending
    }
    fn on_cancel(&mut self, _: &mut protolink::CallContext<'_>) {
        self.cancelled += 1;
    }
}

#[test]
fn server_read_budget_is_fresh_after_output() {
    let mut io = server_io(Fault::None);
    io.script.write_cost = Duration::from_millis(80);
    let time = io.script.time.clone();
    let trace = io.script.trace.clone();
    let mut handler = Quiet::default();
    protolink::blocking::serve_with_clock(io, &mut handler, ServerConfig::default(), time).unwrap();
    assert_eq!(
        trace.borrow().reads,
        [None, Some(Duration::from_millis(20))]
    );
    assert_eq!(handler.cancelled, 1);
}

#[test]
fn server_write_and_flush_timeout_capabilities_cancel_handlers() {
    for fault in [Fault::WriteTimeout, Fault::FlushTimeout] {
        let io = server_io(fault);
        let time = io.script.time.clone();
        let trace = io.script.trace.clone();
        let mut handler = Quiet::default();
        assert!(matches!(
            protolink::blocking::serve_with_io_timeouts(
                io,
                &mut handler,
                ServerConfig::default(),
                time
            ),
            Err(protolink::Error::Io(ErrorKind::TimedOut))
        ));
        assert_eq!(handler.cancelled, 1);
        assert!(trace.borrow().writes.contains(&Some(BUDGET)));
        if matches!(fault, Fault::FlushTimeout) {
            assert!(trace.borrow().flushes.contains(&Some(BUDGET)));
        }
    }
}

fn completed_unary_io(fault: Fault) -> ServerIo {
    let mut client = protolink::grpc::Client::new(ClientConfig::default());
    client
        .start_unary_with("/t.T/Echo", b"x", &CallOptions::timeout(BUDGET))
        .unwrap();
    ServerIo {
        request: Some(client.take_output()),
        seen_input: false,
        script: Script::new(fault),
    }
}

#[test]
fn completed_unary_responses_keep_write_and_flush_timeout_budgets() {
    for fault in [Fault::WriteTimeout, Fault::FlushTimeout] {
        let io = completed_unary_io(fault);
        let time = io.script.time.clone();
        let trace = io.script.trace.clone();
        let completed = Cell::new(0);
        let mut handler = FnHandler(|_: &str, req: &[u8]| {
            completed.set(completed.get() + 1);
            Some(Ok(req.to_vec()))
        });
        assert!(matches!(
            protolink::blocking::serve_with_io_timeouts(
                io,
                &mut handler,
                ServerConfig::default(),
                time
            ),
            Err(protolink::Error::Io(ErrorKind::TimedOut))
        ));
        assert_eq!(completed.get(), 1);
        assert_eq!(trace.borrow().writes.last(), Some(&Some(BUDGET)));
        if matches!(fault, Fault::FlushTimeout) {
            assert_eq!(
                trace.borrow().flushes.last(),
                Some(&Some(BUDGET)),
                "consuming serialized output must not disarm the flush deadline"
            );
        }
    }
}

#[test]
fn successful_server_flush_disarms_completed_unary_deadline() {
    let mut io = completed_unary_io(Fault::None);
    io.script.write_cost = Duration::from_millis(80);
    let time = io.script.time.clone();
    let trace = io.script.trace.clone();
    let mut handler = FnHandler(|_: &str, req: &[u8]| Some(Ok(req.to_vec())));
    protolink::blocking::serve_with_io_timeouts(io, &mut handler, ServerConfig::default(), time)
        .unwrap();
    assert_eq!(
        trace.borrow().flushes.last(),
        Some(&Some(Duration::from_millis(20)))
    );
    assert_eq!(
        trace.borrow().reads,
        [None, None],
        "completed response deadline must be cleared only after successful flush"
    );
}
