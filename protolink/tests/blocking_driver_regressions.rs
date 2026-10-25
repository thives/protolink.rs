//! Driver regressions also run with the library's `std` feature disabled.
#![cfg(feature = "blocking")]

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use embedded_io::{ErrorKind, ErrorType, Read, Write};
use protolink::blocking::{OutputTimeout, ReadTimeout};
use protolink::grpc::FnHandler;
use protolink::{CallOptions, ClientConfig, Clock, Code, READ_CHUNK, ServerConfig};

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
        let mut client = protolink::blocking::client(io).without_deadlines().build();
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
    let client = protolink::blocking::client(io).without_deadlines().build();
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
    let mut client = protolink::blocking::client(io)
        .with_deadline_clock(time)
        .build();
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
    let mut client = protolink::blocking::client(io)
        .with_deadline_clock(time)
        .build();
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
    let mut client = protolink::blocking::client(io)
        .with_deadline_clock(time)
        .build();
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
        let mut client = protolink::blocking::client(io)
            .with_deadline_clock(time)
            .with_output_timeouts()
            .build();
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
    fn route(&self, _: &str) -> Option<protolink::Route> {
        Some(protolink::Route::new(
            protolink::MethodKind::BidiStreaming,
            0,
        ))
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
    protolink::blocking::server(io)
        .with_deadline_clock(time)
        .build()
        .serve(&mut handler)
        .unwrap();
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
            protolink::blocking::server(io)
                .with_deadline_clock(time)
                .with_output_timeouts()
                .build()
                .serve(&mut handler),
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
            protolink::blocking::server(io)
                .with_deadline_clock(time)
                .with_output_timeouts()
                .build()
                .serve(&mut handler),
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
    protolink::blocking::server(io)
        .with_deadline_clock(time)
        .with_output_timeouts()
        .build()
        .serve(&mut handler)
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

// ---------------------------------------------------------------------------
// Output overload: nothing is read, written or flushed after it
// ---------------------------------------------------------------------------

/// One transport operation, in the order the driver started it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    /// A read that delivered input, or the end of the input.
    Read,
    /// A read that waited out the timeout the driver set.
    Timeout,
    Write,
    Flush,
}

/// What a [`Wire`] delivers on a read.
enum Inbound {
    /// The peer's bytes. From then on writes accept one byte each, so output
    /// stays queued between the driver's operations.
    Request(Vec<u8>),
    /// A read that waits out the timeout the driver set, and gets nothing.
    Timeout,
}

/// A transport that records every operation and delivers the scripted input.
struct Wire {
    inbound: VecDeque<Inbound>,
    /// Largest write accepted, `0` for no limit.
    chunk: usize,
    /// Time each write takes on the test clock.
    write_cost: Duration,
    time: Time,
    timeout: Option<Duration>,
    /// Time each flush takes on the test clock, shared with the test.
    flush_cost: Rc<Cell<Duration>>,
    /// Whether flushes fail, shared with the test.
    flush_fails: Rc<Cell<bool>>,
    /// Time of the successive flushes, before `flush_cost` applies.
    flush_costs: VecDeque<Duration>,
    ops: Rc<RefCell<Vec<Op>>>,
}

impl Wire {
    fn new(time: Time) -> Self {
        Self {
            inbound: VecDeque::new(),
            chunk: 0,
            write_cost: Duration::ZERO,
            time,
            timeout: None,
            flush_cost: Rc::default(),
            flush_fails: Rc::default(),
            flush_costs: VecDeque::new(),
            ops: Rc::default(),
        }
    }

    /// The log of the operations, shared with the test.
    fn ops(&self) -> Rc<RefCell<Vec<Op>>> {
        self.ops.clone()
    }
}

impl ErrorType for Wire {
    type Error = ErrorKind;
}

impl Read for Wire {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, ErrorKind> {
        match self.inbound.pop_front() {
            Some(Inbound::Request(bytes)) => {
                self.ops.borrow_mut().push(Op::Read);
                self.chunk = 1;
                buf[..bytes.len()].copy_from_slice(&bytes);
                Ok(bytes.len())
            }
            Some(Inbound::Timeout) => {
                self.ops.borrow_mut().push(Op::Timeout);
                let timeout = self.timeout.expect("the driver sets a read timeout");
                self.time.0.set(self.time.now() + timeout);
                Err(ErrorKind::TimedOut)
            }
            None => {
                self.ops.borrow_mut().push(Op::Read);
                Ok(0)
            }
        }
    }
}

impl Write for Wire {
    fn write(&mut self, buf: &[u8]) -> Result<usize, ErrorKind> {
        self.ops.borrow_mut().push(Op::Write);
        self.time.0.set(self.time.now() + self.write_cost);
        Ok(match self.chunk {
            0 => buf.len(),
            chunk => buf.len().min(chunk),
        })
    }

    fn flush(&mut self) -> Result<(), ErrorKind> {
        self.ops.borrow_mut().push(Op::Flush);
        let cost = self
            .flush_costs
            .pop_front()
            .unwrap_or_else(|| self.flush_cost.get());
        self.time.0.set(self.time.now() + cost);
        if self.flush_fails.get() {
            return Err(ErrorKind::BrokenPipe);
        }
        Ok(())
    }
}

impl ReadTimeout for Wire {
    fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
    }
}

impl OutputTimeout for Wire {
    // Writes and flushes here never block, so there is nothing to bound.
    fn set_write_timeout(&mut self, _: Option<Duration>) {}
    fn set_flush_timeout(&mut self, _: Option<Duration>) {}
}

/// A caller's streaming request on `/t.T/Chat`, with `timeout` if given.
fn chat_request(timeout: Option<Duration>) -> Vec<u8> {
    let mut caller = protolink::grpc::Client::new(ClientConfig::default());
    let options = timeout.map_or_else(CallOptions::default, CallOptions::timeout);
    caller.start_streaming_with("/t.T/Chat", &options).unwrap();
    caller.take_output()
}

fn output_cap(max_pending_output_bytes: usize) -> protolink::grpc::http2::Config {
    protolink::grpc::http2::Config {
        max_pending_output_bytes,
        ..Default::default()
    }
}

/// Answers with a small message, then, once the transport has accepted a write
/// after it, with a message the output cap refuses.
struct Overflowing {
    small_sent: bool,
    ops: Rc<RefCell<Vec<Op>>>,
    /// Transport operations done when the large message was produced.
    produced_at: Option<usize>,
    cancelled: usize,
}

impl protolink::Handler for Overflowing {
    fn route(&self, _: &str) -> Option<protolink::Route> {
        Some(protolink::Route::new(
            protolink::MethodKind::BidiStreaming,
            0,
        ))
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
        if !self.small_sent {
            self.small_sent = true;
            return std::task::Poll::Ready(protolink::Next::Message(vec![1; 100]));
        }
        let ops = self.ops.borrow();
        if ops.iter().filter(|op| **op == Op::Write).count() < 2 {
            return std::task::Poll::Pending;
        }
        self.produced_at = Some(ops.len());
        std::task::Poll::Ready(protolink::Next::Message(vec![2; 2000]))
    }
    fn on_cancel(&mut self, _: &mut protolink::CallContext<'_>) {
        self.cancelled += 1;
    }
}

#[test]
fn blocking_server_handshake_overflow_is_an_error() {
    let io = Wire::new(Time::default());
    let ops = io.ops();
    let mut handler = Quiet::default();
    let result = protolink::blocking::server(io)
        .without_deadlines()
        .with_http2(output_cap(0))
        .build()
        .serve(&mut handler);
    assert!(
        matches!(
            result,
            Err(protolink::Error::OutputLimitExceeded { limit: 0, .. })
        ),
        "{result:?}"
    );
    assert!(ops.borrow().is_empty(), "{:?}", ops.borrow());
}

#[test]
fn blocking_server_poll_overflow_stops_output() {
    let mut io = Wire::new(Time::default());
    let ops = io.ops();
    io.inbound.push_back(Inbound::Request(chat_request(None)));
    let mut handler = Overflowing {
        small_sent: false,
        ops: ops.clone(),
        produced_at: None,
        cancelled: 0,
    };
    let result = protolink::blocking::server(io)
        .without_deadlines()
        .with_http2(output_cap(1024))
        .build()
        .serve(&mut handler);
    assert!(
        matches!(
            result,
            Err(protolink::Error::OutputLimitExceeded { limit: 1024, .. })
        ),
        "{result:?}"
    );
    let produced_at = handler.produced_at.expect("the large message was produced");
    assert_eq!(
        ops.borrow().len(),
        produced_at,
        "a transport operation followed the overflow"
    );
    assert_eq!(handler.cancelled, 1);
}

/// `calls` timed requests on `/t.T/Chat`, as the client sends them: one call each,
/// in the order they are started.
fn timed_requests(calls: usize) -> Vec<Vec<u8>> {
    let mut caller = protolink::grpc::Client::new(ClientConfig::default());
    (0..calls)
        .map(|_| {
            caller
                .start_streaming_with("/t.T/Chat", &CallOptions::timeout(BUDGET))
                .unwrap();
            caller.take_output()
        })
        .collect()
}

/// The smallest output cap with which the blocking server takes `calls` timed
/// requests, each drained as it arrives, and then reaches the timeout read. Found
/// on the server itself, so it includes everything the setup writes.
fn smallest_serving_cap(calls: usize) -> usize {
    (1..)
        .find(|&cap| {
            let time = Time::default();
            let mut io = Wire::new(time.clone());
            let ops = io.ops();
            io.inbound
                .extend(timed_requests(calls).into_iter().map(Inbound::Request));
            io.inbound.push_back(Inbound::Timeout);
            let mut handler = Quiet::default();
            let _ = protolink::blocking::server(io)
                .with_deadline_clock(time)
                .with_http2(expiry_output(cap, calls))
                .build()
                .serve(&mut handler);
            ops.borrow().contains(&Op::Timeout)
        })
        .unwrap()
}

/// Bytes that one timed call's expiry queues: its trailers, measured on the sans-IO
/// server after the call has been set up and drained.
fn expiry_trailers() -> usize {
    let mut server = protolink::grpc::Server::new(ServerConfig::default());
    let mut handler = Quiet::default();
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    server.take_output();
    server.recv(&timed_requests(1)[0], &mut handler).unwrap();
    server.poll(&mut handler, &mut cx);
    server.take_output();
    server.tick(BUDGET, &mut handler);
    server.take_output().len()
}

/// The output cap and the number of timed calls of the expiry tests. The cap is
/// the smallest one that admits a call's setup, so one call's trailers fit under
/// it, but the trailers of enough calls together do not.
fn expiry_setup() -> (usize, usize) {
    let cap = smallest_serving_cap(1);
    let trailers = expiry_trailers();
    assert!(trailers <= cap, "one call's trailers must fit the cap");
    (cap, cap / trailers + 2)
}

fn expiry_output(cap: usize, calls: usize) -> protolink::grpc::http2::Config {
    protolink::grpc::http2::Config {
        max_pending_output_bytes: cap,
        max_concurrent_streams: calls as u32,
        ..Default::default()
    }
}

#[test]
fn server_read_timeout_expiry_overflows_before_more_output() {
    let (cap, calls) = expiry_setup();
    let time = Time::default();
    let mut io = Wire::new(time.clone());
    let ops = io.ops();
    io.inbound
        .extend(timed_requests(calls).into_iter().map(Inbound::Request));
    io.inbound.push_back(Inbound::Timeout);
    let mut handler = Quiet::default();
    let result = protolink::blocking::server(io)
        .with_deadline_clock(time)
        .with_http2(expiry_output(cap, calls))
        .build()
        .serve(&mut handler);
    assert!(
        matches!(result, Err(protolink::Error::OutputLimitExceeded { .. })),
        "{result:?}"
    );

    // Every request was set up and flushed, then the timeout read ran: the
    // expiry overflowed after it, not during the setup.
    let ops = ops.borrow();
    assert_eq!(
        ops.iter().filter(|op| **op == Op::Flush).count(),
        calls + 1,
        "the setup of every request must be flushed before the timeout read"
    );
    assert_eq!(ops.iter().filter(|op| **op == Op::Timeout).count(), 1);
    assert_eq!(
        ops.last(),
        Some(&Op::Timeout),
        "a transport operation followed the expiry's overflow"
    );
    assert_eq!(handler.cancelled, calls);
}

#[test]
fn server_flush_expiry_overflows_before_more_output() {
    let (cap, calls) = expiry_setup();
    let time = Time::default();
    let mut io = Wire::new(time.clone());
    let ops = io.ops();
    // The flush after the last request takes the clock to the calls' deadline.
    let mut flush_costs = vec![Duration::ZERO; calls];
    flush_costs.push(BUDGET);
    io.flush_costs = flush_costs.into();
    io.inbound
        .extend(timed_requests(calls).into_iter().map(Inbound::Request));
    let mut handler = Quiet::default();
    let result = protolink::blocking::server(io)
        .with_deadline_clock(time)
        .with_http2(expiry_output(cap, calls))
        .build()
        .serve(&mut handler);
    assert!(
        matches!(result, Err(protolink::Error::OutputLimitExceeded { .. })),
        "{result:?}"
    );

    // The setup of every request was flushed, and the flush that reached the
    // deadline is the last operation: no read was started after it.
    let ops = ops.borrow();
    assert_eq!(
        ops.iter().filter(|op| **op == Op::Flush).count(),
        calls + 1,
        "the setup of every request must be flushed before the deadline flush"
    );
    assert_eq!(ops.iter().filter(|op| **op == Op::Timeout).count(), 0);
    assert_eq!(
        ops.last(),
        Some(&Op::Flush),
        "a transport operation followed the flush's expiry"
    );
    assert_eq!(handler.cancelled, calls);
}

#[test]
fn server_expiry_within_the_cap_writes_the_trailers() {
    let (_, calls) = expiry_setup();
    let time = Time::default();
    let mut io = Wire::new(time.clone());
    let ops = io.ops();
    io.inbound
        .extend(timed_requests(calls).into_iter().map(Inbound::Request));
    io.inbound.push_back(Inbound::Timeout);
    let mut handler = Quiet::default();
    let result = protolink::blocking::server(io)
        .with_deadline_clock(time)
        .with_http2(expiry_output(1 << 20, calls))
        .build()
        .serve(&mut handler);
    assert!(result.is_ok(), "{result:?}");

    let ops = ops.borrow();
    let timeout = ops
        .iter()
        .position(|op| *op == Op::Timeout)
        .expect("the timeout read ran");
    assert!(
        ops[timeout..].contains(&Op::Write),
        "the expired calls' trailers were not written"
    );
    assert_eq!(handler.cancelled, calls);
}

#[test]
fn blocking_client_tick_overflow_stops_output() {
    // The sizes come from the sans-IO client: the request as queued, and the
    // reset that cancels it.
    let payload = [7u8; 200];
    let options = CallOptions::timeout(BUDGET);
    let mut probe = protolink::grpc::Client::new(ClientConfig::default());
    let id = probe
        .start_unary_with("/t.T/Echo", &payload, &options)
        .unwrap();
    let queued = probe.pending_output().len();
    probe.cancel(id);
    let reset = probe.pending_output().len() - queued;

    // The first write takes the call past its deadline. The rest of the request
    // is then `queued - 1` bytes, and the reset needs `reset` more: refused.
    let time = Time::default();
    let mut io = Wire::new(time.clone());
    io.chunk = 1;
    io.write_cost = BUDGET;
    let ops = io.ops();
    let mut client = protolink::blocking::client(io)
        .with_deadline_clock(time)
        .with_http2(output_cap(queued + reset - 2))
        .build();
    assert_eq!(
        client
            .unary_with("/t.T/Echo", &payload, options)
            .unwrap_err()
            .code,
        Code::DeadlineExceeded
    );
    let writes = ops.borrow().iter().filter(|op| **op == Op::Write).count();
    assert_eq!(writes, 1, "no write followed the overflow");

    let count = ops.borrow().len();
    assert_eq!(
        client.unary("/t.T/Echo", b"again").unwrap_err().code,
        Code::Unavailable
    );
    assert!(client.streaming("/t.T/Chat").is_err());
    assert_eq!(ops.borrow().len(), count, "later calls performed I/O");
}

#[test]
fn blocking_client_start_overflow_retires_the_transport() {
    let io = Wire::new(Time::default());
    let ops = io.ops();
    let mut client = protolink::blocking::client(io)
        .without_deadlines()
        .with_http2(output_cap(1024))
        .build();
    // A header block that does not fit the cap: the call never starts.
    let mut metadata = protolink::grpc::Metadata::new();
    metadata.insert("x-big", &"a".repeat(2000)).unwrap();
    let status = client
        .unary_with(
            "/t.T/Echo",
            b"x",
            CallOptions::default().with_metadata(metadata),
        )
        .unwrap_err();
    assert_eq!(status.code, Code::Unavailable);
    assert!(
        status.message.contains("output limit"),
        "{}",
        status.message
    );

    let count = ops.borrow().len();
    assert_eq!(
        client.unary("/t.T/Echo", b"again").unwrap_err().code,
        Code::Unavailable
    );
    assert!(client.streaming("/t.T/Chat").is_err());
    assert_eq!(ops.borrow().len(), count, "later calls performed I/O");
}

#[test]
fn blocking_client_send_overflow_fails_other_calls_without_io() {
    let io = Wire::new(Time::default());
    let ops = io.ops();
    let client = protolink::blocking::client(io)
        .without_deadlines()
        .with_http2(output_cap(1024))
        .build();
    let mut first = client.streaming("/t.T/Chat").unwrap();
    let mut second = client.streaming("/t.T/Chat").unwrap();
    // The message's data frame does not fit the cap, which fails every call.
    let status = second.send(&[7; 2000]).unwrap_err();
    assert_eq!(status.code, Code::Unavailable);

    let count = ops.borrow().len();
    assert_eq!(first.message().unwrap_err().code, Code::Unavailable);
    assert_eq!(second.message().unwrap_err().code, Code::Unavailable);
    assert!(client.streaming("/t.T/Chat").is_err());
    assert_eq!(ops.borrow().len(), count, "interrupted calls performed I/O");
}

// ---------------------------------------------------------------------------
// Half-close: an expired call is recorded before END_STREAM is queued
// ---------------------------------------------------------------------------

/// Replies per call. Enough released window updates to admit both calls' setup
/// under the cap, and few enough that all the server's bytes fit one read.
const REPLIES: usize = 18;

/// The server's bytes for a `/t.T/Chat` request (with `timeout` if given): `replies`
/// messages, then the final status if `finish`.
fn chat_replies(timeout: Option<Duration>, replies: usize, finish: bool) -> Vec<u8> {
    struct Replies {
        left: usize,
        finish: bool,
    }
    impl protolink::Handler for Replies {
        fn route(&self, _: &str) -> Option<protolink::Route> {
            Some(protolink::Route::new(
                protolink::MethodKind::BidiStreaming,
                0,
            ))
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
            if self.left > 0 {
                self.left -= 1;
                return std::task::Poll::Ready(protolink::Next::Message(vec![7; 10]));
            }
            if self.finish {
                std::task::Poll::Ready(protolink::Next::Done(Ok(())))
            } else {
                std::task::Poll::Pending
            }
        }
    }
    let mut peer = protolink::grpc::Server::new(ServerConfig::default());
    let mut handler = Replies {
        left: replies,
        finish,
    };
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    peer.recv(&chat_request(timeout), &mut handler).unwrap();
    for _ in 0..=replies {
        peer.poll(&mut handler, &mut cx);
    }
    peer.take_output()
}

/// Serialized sizes of a half-close scenario, measured on the sans-IO client:
/// the setup burst (preface and the headers of two calls), the window updates
/// that `consumed` replies release, and the END_STREAM frame.
struct HalfClose {
    setup: usize,
    released: usize,
    end_stream: usize,
}

fn half_close_sizes(timeout: Option<Duration>, replies: usize, consumed: usize) -> HalfClose {
    let options = timeout.map_or_else(CallOptions::default, CallOptions::timeout);
    let mut probe = protolink::grpc::Client::new(ClientConfig::default());
    let target = probe.start_streaming_with("/t.T/Chat", &options).unwrap();
    probe
        .start_streaming_with("/t.T/Chat", &CallOptions::default())
        .unwrap();
    let setup = probe.take_output().len();
    probe.recv(&chat_replies(timeout, replies, false)).unwrap();
    for _ in 0..consumed {
        assert!(matches!(
            probe.try_next(target),
            Some(protolink::grpc::Next::Message(_))
        ));
    }
    let released = probe.pending_output().len();
    probe.close_send(target).unwrap();
    HalfClose {
        setup,
        released,
        end_stream: probe.pending_output().len() - released,
    }
}

/// A client on `inbound`'s replies with a deadline clock, capped at `cap` bytes of
/// pending output. Returns the client, the clock and the transport's log.
fn half_close_client(
    replies: Vec<u8>,
    cap: usize,
) -> (
    protolink::blocking::Client<Wire, Time>,
    Time,
    Rc<RefCell<Vec<Op>>>,
) {
    assert!(replies.len() <= READ_CHUNK, "the replies must fit one read");
    let time = Time::default();
    let mut io = Wire::new(time.clone());
    let ops = io.ops();
    io.inbound.push_back(Inbound::Request(replies));
    let client = protolink::blocking::client(io)
        .with_deadline_clock(time.clone())
        .with_http2(output_cap(cap))
        .build();
    (client, time, ops)
}

#[test]
fn half_close_after_the_deadline_reports_the_deadline() {
    let sizes = half_close_sizes(Some(BUDGET), REPLIES, REPLIES);
    // END_STREAM does not fit behind the window updates. The expiry's reset (13
    // bytes, larger than END_STREAM) does not fit either, so this also covers
    // a reset that exceeds the cap: the expired call keeps its deadline.
    let cap = sizes.released + sizes.end_stream - 1;
    assert!(sizes.setup <= cap, "the setup must be admitted");
    let (client, time, ops) = half_close_client(chat_replies(Some(BUDGET), REPLIES, false), cap);
    let mut target = client
        .streaming_with("/t.T/Chat", CallOptions::timeout(BUDGET))
        .unwrap();
    let mut other = client.streaming("/t.T/Chat").unwrap();
    for _ in 0..REPLIES {
        assert!(matches!(target.message(), Ok(Some(_))));
    }
    time.0.set(BUDGET);

    let _ = target.close_send();
    let count = ops.borrow().len();
    assert_eq!(
        target.message().unwrap_err().code,
        Code::DeadlineExceeded,
        "the expired call must keep its deadline"
    );
    assert_eq!(other.message().unwrap_err().code, Code::Unavailable);
    assert_eq!(
        ops.borrow().len(),
        count,
        "a transport operation followed the failure"
    );
}

#[test]
fn half_close_before_the_deadline_overflows_as_before() {
    let sizes = half_close_sizes(Some(BUDGET), REPLIES, REPLIES);
    let cap = sizes.released + sizes.end_stream - 1;
    let (client, _time, _ops) = half_close_client(chat_replies(Some(BUDGET), REPLIES, false), cap);
    let mut target = client
        .streaming_with("/t.T/Chat", CallOptions::timeout(BUDGET))
        .unwrap();
    for _ in 0..REPLIES {
        assert!(matches!(target.message(), Ok(Some(_))));
    }
    let status = target.close_send().unwrap_err();
    assert_eq!(status.code, Code::Unavailable);
    assert!(
        status.message.contains("output limit"),
        "{}",
        status.message
    );
    assert_eq!(target.message().unwrap_err().code, Code::Unavailable);
}

#[test]
fn half_close_on_a_healthy_call_writes_end_stream() {
    let sizes = half_close_sizes(Some(BUDGET), REPLIES, REPLIES);
    let (client, _time, ops) = half_close_client(
        chat_replies(Some(BUDGET), REPLIES, false),
        sizes.released + sizes.end_stream + 1024,
    );
    let mut call = client
        .streaming_with("/t.T/Chat", CallOptions::timeout(BUDGET))
        .unwrap();
    for _ in 0..REPLIES {
        assert!(matches!(call.message(), Ok(Some(_))));
    }
    let writes = ops.borrow().iter().filter(|op| **op == Op::Write).count();
    call.close_send().unwrap();
    assert!(
        ops.borrow().iter().filter(|op| **op == Op::Write).count() > writes,
        "END_STREAM was not written"
    );
}

#[test]
fn half_close_on_a_finished_call_is_a_no_op() {
    let mut io = Wire::new(Time::default());
    let ops = io.ops();
    io.inbound
        .push_back(Inbound::Request(chat_replies(None, 3, true)));
    let client = protolink::blocking::client(io).without_deadlines().build();
    let mut call = client.streaming("/t.T/Chat").unwrap();
    for _ in 0..3 {
        assert!(matches!(call.message(), Ok(Some(_))));
    }
    assert!(matches!(call.message(), Ok(None)));
    let count = ops.borrow().len();
    call.close_send().unwrap();
    assert_eq!(ops.borrow().len(), count, "a finished call performed I/O");
}

// ---------------------------------------------------------------------------
// Successful flush: expiry that overloads the output keeps the overload
// ---------------------------------------------------------------------------

/// Timed calls that expire together, more than the cap can hold.
const TIMED_CALLS: usize = 40;

/// The smallest output cap that admits a timed call's setup on a fresh connection,
/// found on the sans-IO client.
fn smallest_admitting_cap() -> usize {
    (1..)
        .find(|&cap| {
            let config = ClientConfig {
                http2: output_cap(cap),
                ..ClientConfig::default()
            };
            let mut probe = protolink::grpc::Client::new(config);
            probe
                .start_streaming_with("/t.T/Chat", &CallOptions::timeout(BUDGET))
                .is_ok()
        })
        .unwrap()
}

/// Bytes that a timed call's expiry queues: its reset, measured by cancelling it.
fn expiry_reset_bytes() -> usize {
    let mut probe = protolink::grpc::Client::new(ClientConfig::default());
    let id = probe
        .start_streaming_with("/t.T/Chat", &CallOptions::timeout(BUDGET))
        .unwrap();
    probe.take_output();
    probe.cancel(id);
    probe.pending_output().len()
}

/// A client with a deadline clock and output timeouts, capped at `cap` bytes.
fn flush_client(io: Wire, time: &Time, cap: usize) -> protolink::blocking::Client<Wire, Time> {
    protolink::blocking::client(io)
        .with_deadline_clock(time.clone())
        .with_output_timeouts()
        // One more than the timed calls, for a call started after the flush.
        .with_max_retained_calls(TIMED_CALLS + 1)
        .with_http2(output_cap(cap))
        .build()
}

/// [`TIMED_CALLS`] calls on `/t.T/Chat`, each drained before the next starts so
/// that every setup is admitted under the cap.
fn drained_timed_calls(
    client: &protolink::blocking::Client<Wire, Time>,
) -> Vec<protolink::blocking::Call<'_, Wire, Time>> {
    (0..TIMED_CALLS)
        .map(|_| {
            let mut call = client
                .streaming_with("/t.T/Chat", CallOptions::timeout(BUDGET))
                .unwrap();
            call.send(b"").unwrap();
            call
        })
        .collect()
}

#[test]
fn successful_flush_expiry_reports_the_output_limit() {
    let cap = smallest_admitting_cap();
    assert!(
        TIMED_CALLS * expiry_reset_bytes() > cap,
        "the expiry resets must not all fit"
    );
    let time = Time::default();
    let io = Wire::new(time.clone());
    let ops = io.ops();
    let flush_cost = io.flush_cost.clone();
    let client = flush_client(io, &time, cap);
    let mut calls = drained_timed_calls(&client);

    // The next flush takes the clock to every deadline. It has drained the
    // output, so the resets that end the calls are what overflows the cap.
    flush_cost.set(BUDGET);
    calls[0].send(b"x").unwrap();
    for call in &mut calls {
        assert_eq!(call.message().unwrap_err().code, Code::DeadlineExceeded);
    }

    let count = ops.borrow().len();
    let Err(status) = client.streaming("/t.T/Chat") else {
        panic!("the connection still started a call");
    };
    assert_eq!(status.code, Code::Unavailable);
    assert!(
        status.message.contains("output limit"),
        "{}",
        status.message
    );
    assert_eq!(
        ops.borrow().len(),
        count,
        "a transport operation followed the failure"
    );
}

#[test]
fn successful_flush_reaching_a_deadline_keeps_the_output_deadline() {
    let time = Time::default();
    let io = Wire::new(time.clone());
    let flush_cost = io.flush_cost.clone();
    let client = flush_client(io, &time, 1 << 20);
    let mut calls = drained_timed_calls(&client);

    flush_cost.set(BUDGET);
    calls[0].send(b"x").unwrap();
    for call in &mut calls {
        assert_eq!(call.message().unwrap_err().code, Code::DeadlineExceeded);
    }
    let Err(status) = client.streaming("/t.T/Chat") else {
        panic!("the connection still started a call");
    };
    assert_eq!(status.code, Code::Unavailable);
    assert!(
        status.message.contains("output deadline exceeded"),
        "{}",
        status.message
    );
}

#[test]
fn healthy_flush_before_the_deadline_keeps_every_call() {
    let time = Time::default();
    let io = Wire::new(time.clone());
    let client = flush_client(io, &time, smallest_admitting_cap());
    let mut calls = drained_timed_calls(&client);

    calls[0].send(b"x").unwrap();
    assert!(
        client.streaming("/t.T/Chat").is_ok(),
        "the connection failed"
    );
}

#[test]
fn flush_error_keeps_buffered_replies() {
    let time = Time::default();
    let mut io = Wire::new(time.clone());
    let ops = io.ops();
    let flush_fails = io.flush_fails.clone();
    io.inbound
        .push_back(Inbound::Request(chat_replies(Some(BUDGET), 2, false)));
    let client = flush_client(io, &time, 1 << 20);
    let mut call = client
        .streaming_with("/t.T/Chat", CallOptions::timeout(BUDGET))
        .unwrap();
    // The first reply is returned; the second stays buffered.
    assert!(matches!(call.message(), Ok(Some(_))));

    flush_fails.set(true);
    call.send(b"x").unwrap();
    let count = ops.borrow().len();
    assert!(matches!(call.message(), Ok(Some(_))), "buffered reply lost");
    let status = call.message().unwrap_err();
    assert_eq!(status.code, Code::Unavailable);
    assert!(
        status.message.contains("flush failed"),
        "{}",
        status.message
    );
    assert_eq!(
        ops.borrow().len(),
        count,
        "a transport operation followed the failure"
    );
}
