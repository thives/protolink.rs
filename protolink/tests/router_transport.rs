//! The gateway router over the tokio driver with transports that the test can
//! stall: a blocked write or flush on one transport must not hold up the others,
//! the coordinator's deadlines, or the gateway's shutdown.
#![cfg(feature = "tokio")]

use std::collections::{BTreeMap, VecDeque};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::task::{Context, Poll};
use std::time::Duration;

use embedded_io_async::{ErrorType, Read, Write};
use protolink::grpc::http2::{Connection, Event};
use protolink::grpc::{
    CallContext, CallId, CallOptions, Client as GrpcClient, ClientConfig as GrpcClientConfig, Code,
    Dispatcher, Handler, MethodDescriptor, MethodKind, Next, Route, Router, Status,
};
use protolink::tokio::{FromTokio, TokioTimer, client, compat};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
use tokio::sync::Notify;

const LOCAL: &[MethodDescriptor] = &[
    MethodDescriptor::new("/t.Local/Upper", MethodKind::Unary, 0),
    MethodDescriptor::new("/t.Local/Echo", MethodKind::BidiStreaming, 1),
    MethodDescriptor::new("/t.Local/Hang", MethodKind::BidiStreaming, 2),
    MethodDescriptor::new("/t.Local/Late", MethodKind::BidiStreaming, 3),
];

/// The methods of the downstream served by `Remote`, on the route to backend B.
const REMOTE: &[MethodDescriptor] = &[
    MethodDescriptor::new("/t.Remote/Unary", MethodKind::Unary, 0),
    MethodDescriptor::new("/t.Remote/Echo", MethodKind::BidiStreaming, 1),
];

/// The methods routed to backend A, which the tests stall.
const OTHER: &[MethodDescriptor] = &[MethodDescriptor::new(
    "/t.Other/Echo",
    MethodKind::BidiStreaming,
    0,
)];

const CAPACITY: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Transports the test controls
// ---------------------------------------------------------------------------

/// Controls one transport from the test.
#[derive(Default)]
struct Gate {
    /// Writes do not complete while set.
    stalled: AtomicBool,
    /// Flushes do not complete while set.
    flush_stalled: AtomicBool,
    /// Largest number of bytes one write accepts; `0` for no limit.
    max_write: AtomicUsize,
    /// Writes started.
    writes: AtomicUsize,
    change: Notify,
}

impl Gate {
    fn stall(&self) {
        self.stalled.store(true, SeqCst);
    }

    fn stall_flush(&self) {
        self.flush_stalled.store(true, SeqCst);
    }

    fn release(&self) {
        self.stalled.store(false, SeqCst);
        self.flush_stalled.store(false, SeqCst);
        self.change.notify_waiters();
    }
}

/// Wait while `flag` is set. The notification is registered before the flag is
/// read, so a release in between is not missed.
async fn wait_while(flag: &AtomicBool, change: &Notify) {
    loop {
        let notified = change.notified();
        if !flag.load(SeqCst) {
            return;
        }
        notified.await;
    }
}

/// A tokio stream as an `embedded_io_async` transport, whose writes and flushes
/// the [`Gate`] can hold back.
struct Gated {
    io: FromTokio<DuplexStream>,
    gate: Arc<Gate>,
}

impl ErrorType for Gated {
    type Error = std::io::Error;
}

impl Read for Gated {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.io.read(buf).await
    }
}

impl Write for Gated {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        self.gate.writes.fetch_add(1, SeqCst);
        wait_while(&self.gate.stalled, &self.gate.change).await;
        let limit = self.gate.max_write.load(SeqCst);
        let n = if limit == 0 {
            buf.len()
        } else {
            buf.len().min(limit)
        };
        self.io.write(&buf[..n]).await
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        wait_while(&self.gate.flush_stalled, &self.gate.change).await;
        self.io.flush().await
    }
}

/// A transport with a gate, and the gate.
fn gated(io: DuplexStream) -> (Gated, Arc<Gate>) {
    let gate = Arc::new(Gate::default());
    (
        Gated {
            io: compat(io),
            gate: gate.clone(),
        },
        gate,
    )
}

// ---------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------

/// The gateway's own service.
#[derive(Default)]
struct Local {
    /// Messages of the echo calls, to answer in order.
    echo: BTreeMap<CallId, (VecDeque<Vec<u8>>, bool)>,
    /// Calls that were cancelled before they completed.
    cancelled: Arc<AtomicUsize>,
    /// `Late` answers only once its caller has half-closed.
    late_ready: bool,
    late_waker: Option<std::task::Waker>,
}

impl Handler for Local {
    fn route(&self, path: &str) -> Option<Route> {
        Route::find(LOCAL, path)
    }

    fn call(&mut self, _: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        Ok(request.to_ascii_uppercase())
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        self.echo
            .entry(ctx.id)
            .or_default()
            .0
            .push_back(message.to_vec());
        Ok(())
    }

    fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        self.echo.entry(ctx.id).or_default().1 = true;
        self.late_ready = true;
        if let Some(waker) = self.late_waker.take() {
            waker.wake();
        }
        Ok(())
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        match ctx.route.method {
            // Echo: each message as it arrives, then the end once half-closed.
            1 => {
                let (messages, half_closed) = self.echo.entry(ctx.id).or_default();
                if let Some(message) = messages.pop_front() {
                    return Poll::Ready(Next::Message(message));
                }
                if *half_closed {
                    self.echo.remove(&ctx.id);
                    return Poll::Ready(Next::Done(Ok(())));
                }
                Poll::Pending
            }
            // Late: nothing until its caller half-closes, and then one message.
            3 => {
                if !self.late_ready {
                    self.late_waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                if self.echo.contains_key(&ctx.id) {
                    self.echo.remove(&ctx.id);
                    return Poll::Ready(Next::Message(b"late".to_vec()));
                }
                Poll::Ready(Next::Done(Ok(())))
            }
            // Hang: never answers.
            _ => Poll::Pending,
        }
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        self.echo.remove(&ctx.id);
        self.cancelled.fetch_add(1, SeqCst);
    }
}

impl Dispatcher for Local {
    fn methods(&self) -> &[MethodDescriptor] {
        LOCAL
    }
}

/// A downstream service: `Unary` answers with its name, and `Echo` (on both
/// routes) echoes each message in order.
struct Remote {
    name: &'static str,
    echo: BTreeMap<CallId, (VecDeque<Vec<u8>>, bool)>,
}

impl Remote {
    fn named(name: &'static str) -> Self {
        Self {
            name,
            echo: BTreeMap::new(),
        }
    }
}

impl Handler for Remote {
    fn route(&self, path: &str) -> Option<Route> {
        Route::find(REMOTE, path).or_else(|| Route::find(OTHER, path))
    }

    fn call(&mut self, _: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        Ok([self.name.as_bytes(), b"pong:", request].concat())
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        self.echo
            .entry(ctx.id)
            .or_default()
            .0
            .push_back(message.to_vec());
        Ok(())
    }

    fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        self.echo.entry(ctx.id).or_default().1 = true;
        Ok(())
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        let (messages, half_closed) = self.echo.entry(ctx.id).or_default();
        if let Some(message) = messages.pop_front() {
            return Poll::Ready(Next::Message(message));
        }
        if *half_closed {
            self.echo.remove(&ctx.id);
            return Poll::Ready(Next::Done(Ok(())));
        }
        Poll::Pending
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        self.echo.remove(&ctx.id);
    }
}

/// A gateway with the local service and one downstream per route in `routes`,
/// in order: `OTHER` is backend A and `REMOTE` is backend B.
fn gateway_router(cancelled: Arc<AtomicUsize>, routes: &[&'static [MethodDescriptor]]) -> Router {
    let mut builder = Router::builder().with_service(Local {
        cancelled,
        ..Local::default()
    });
    for methods in routes {
        builder = builder.with_route(methods, GrpcClient::new(GrpcClientConfig::default()));
    }
    builder.build().expect("routes do not clash")
}

/// Serve `router` on the gateway transport, with `downstream` as its backends
/// in route order.
async fn serve_gateway<IO>(
    gateway_io: IO,
    router: &mut Router,
    downstream: &mut [Gated],
) -> Result<(), protolink::Error<std::io::Error>>
where
    IO: Read + Write<Error = std::io::Error>,
{
    protolink::server(gateway_io)
        .with_deadline_timer(TokioTimer::new())
        .build()
        .serve_router(router, downstream)
        .await
}

/// The caller's end of a gateway connection, whose calls get no local timeout
/// so that the gateway's deadlines are the ones observed.
fn caller_without_deadlines(io: DuplexStream) -> protolink::Client<FromTokio<DuplexStream>> {
    client(io).without_deadlines().build()
}

// ---------------------------------------------------------------------------
// Isolation: a blocked transport does not hold up the others
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_unread_downstream_does_not_stop_a_local_call() {
    let (caller_io, gateway_io) = duplex(CAPACITY);
    let (gateway_down, _backend_io) = duplex(CAPACITY);
    let (down, down_gate) = gated(gateway_down);
    // The downstream never accepts its first bytes, the startup preface.
    down_gate.stall();
    let mut router = gateway_router(Arc::default(), &[OTHER]);
    let gateway = async {
        let (gateway_io, _) = gated(gateway_io);
        serve_gateway(gateway_io, &mut router, &mut [down]).await
    };
    let caller = async move {
        let mut client = client(caller_io).build();
        assert_eq!(client.unary("/t.Local/Upper", b"hi").await.unwrap(), b"HI");
        drop(client);
    };
    let (gateway, ()) = tokio::join!(gateway, caller);
    gateway.expect("gateway ends when the caller hangs up");
    // The downstream's preface write was attempted, and never completed.
    assert!(down_gate.writes.load(SeqCst) >= 1);
}

#[tokio::test]
async fn a_stalled_downstream_write_does_not_hold_up_another_backend() {
    let (caller_io, gateway_io) = duplex(CAPACITY);
    let (a_gw, a_backend) = duplex(CAPACITY);
    let (b_gw, b_backend) = duplex(CAPACITY);
    let (a, a_gate) = gated(a_gw);
    let (b, _b_gate) = gated(b_gw);
    a_gate.stall();
    let mut router = gateway_router(Arc::default(), &[OTHER, REMOTE]);
    let gateway = async {
        let (gateway_io, _) = gated(gateway_io);
        serve_gateway(gateway_io, &mut router, &mut [a, b]).await
    };
    let backend_b = async move {
        let mut service = Remote::named("b/");
        protolink::tokio::server(b_backend)
            .build()
            .serve(&mut service)
            .await
    };
    let caller = async move {
        let client = caller_without_deadlines(caller_io);
        // A call to backend A is stuck behind its stalled write.
        let mut stuck = client.streaming(OTHER[0].path).unwrap();
        stuck.send(b"stuck").await.unwrap();
        // Backend B, and the local service, still progress on the same caller.
        let mut echo = client.streaming(REMOTE[1].path).unwrap();
        echo.send(b"one").await.unwrap();
        echo.close_send().await.unwrap();
        assert_eq!(echo.message().await.unwrap(), Some(b"one".to_vec()));
        assert_eq!(echo.message().await.unwrap(), None);
        let mut upper = client.streaming(LOCAL[0].path).unwrap();
        upper.send(b"local").await.unwrap();
        upper.close_send().await.unwrap();
        assert_eq!(upper.message().await.unwrap(), Some(b"LOCAL".to_vec()));
        drop(stuck);
    };
    let (gateway, _backend, ()) = tokio::join!(gateway, backend_b, caller);
    gateway.expect("gateway ends when the caller hangs up");
    drop(a_backend);
}

#[tokio::test]
async fn a_stalled_downstream_flush_does_not_hold_up_another_backend() {
    let (caller_io, gateway_io) = duplex(CAPACITY);
    let (a_gw, a_backend) = duplex(CAPACITY);
    let (b_gw, b_backend) = duplex(CAPACITY);
    let (a, a_gate) = gated(a_gw);
    let (b, _b_gate) = gated(b_gw);
    a_gate.stall_flush();
    let mut router = gateway_router(Arc::default(), &[OTHER, REMOTE]);
    let gateway = async {
        let (gateway_io, _) = gated(gateway_io);
        serve_gateway(gateway_io, &mut router, &mut [a, b]).await
    };
    let backend_b = async move {
        let mut service = Remote::named("b/");
        protolink::tokio::server(b_backend)
            .build()
            .serve(&mut service)
            .await
    };
    let caller = async move {
        let client = caller_without_deadlines(caller_io);
        let stuck = client.streaming(OTHER[0].path).unwrap();
        let mut echo = client.streaming(REMOTE[1].path).unwrap();
        echo.send(b"flushed").await.unwrap();
        echo.close_send().await.unwrap();
        assert_eq!(echo.message().await.unwrap(), Some(b"flushed".to_vec()));
        assert_eq!(echo.message().await.unwrap(), None);
        drop(stuck);
    };
    let (gateway, _backend, ()) = tokio::join!(gateway, backend_b, caller);
    gateway.expect("gateway ends when the caller hangs up");
    drop(a_backend);
}

#[tokio::test]
async fn a_local_response_reaches_the_caller_while_downstream_output_is_stalled() {
    let (caller_io, gateway_io) = duplex(CAPACITY);
    let (a_gw, a_backend) = duplex(CAPACITY);
    let (a, a_gate) = gated(a_gw);
    a_gate.stall();
    let mut router = gateway_router(Arc::default(), &[OTHER]);
    let gateway = async {
        let (gateway_io, _) = gated(gateway_io);
        serve_gateway(gateway_io, &mut router, &mut [a]).await
    };
    let caller = async move {
        let client = caller_without_deadlines(caller_io);
        let stuck = client.streaming(OTHER[0].path).unwrap();
        // `Late` wakes the gateway's coordinator only when its caller half-closes.
        let mut late = client.streaming(LOCAL[3].path).unwrap();
        late.send(b"x").await.unwrap();
        late.close_send().await.unwrap();
        assert_eq!(late.message().await.unwrap(), Some(b"late".to_vec()));
        assert_eq!(late.message().await.unwrap(), None);
        drop(stuck);
    };
    let (gateway, ()) = tokio::join!(gateway, caller);
    gateway.expect("gateway ends when the caller hangs up");
    drop(a_backend);
}

#[tokio::test]
async fn blocked_downstream_output_resumes_in_byte_order() {
    let (caller_io, gateway_io) = duplex(CAPACITY);
    let (b_gw, b_backend) = duplex(CAPACITY);
    let (b, b_gate) = gated(b_gw);
    let mut router = gateway_router(Arc::default(), &[REMOTE]);
    let gateway = async {
        let (gateway_io, _) = gated(gateway_io);
        serve_gateway(gateway_io, &mut router, &mut [b]).await
    };
    let backend = async move {
        let mut service = Remote::named("b/");
        protolink::tokio::server(b_backend)
            .build()
            .serve(&mut service)
            .await
    };
    let caller = async move {
        let client = caller_without_deadlines(caller_io);
        b_gate.stall();
        let mut echo = client.streaming(REMOTE[1].path).unwrap();
        for message in [&b"first"[..], b"second", b"third"] {
            echo.send(message).await.unwrap();
        }
        echo.close_send().await.unwrap();
        // Let the gateway queue what it has received behind the stalled write.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        b_gate.release();
        let mut replies = Vec::new();
        while let Some(message) = echo.message().await.unwrap() {
            replies.push(message);
        }
        assert_eq!(
            replies,
            [b"first".to_vec(), b"second".to_vec(), b"third".to_vec()]
        );
    };
    let (gateway, _backend, ()) = tokio::join!(gateway, backend, caller);
    gateway.expect("gateway ends when the caller hangs up");
}

// ---------------------------------------------------------------------------
// Deadlines and failure
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_stuck_downstream_does_not_stop_a_gateway_deadline() {
    let (caller_io, gateway_io) = duplex(CAPACITY);
    let (a_gw, a_backend) = duplex(CAPACITY);
    let (a, a_gate) = gated(a_gw);
    a_gate.stall();
    let mut router = gateway_router(Arc::default(), &[OTHER]);
    let gateway = async {
        let (gateway_io, _) = gated(gateway_io);
        serve_gateway(gateway_io, &mut router, &mut [a]).await
    };
    let caller = async move {
        let client = caller_without_deadlines(caller_io);
        // A local call that never answers: only the gateway's deadline ends it.
        let mut hang = client
            .streaming_with(
                LOCAL[2].path,
                CallOptions::timeout(Duration::from_millis(100)),
            )
            .unwrap();
        hang.send(b"x").await.unwrap();
        assert_eq!(
            hang.message().await.unwrap_err().code,
            Code::DeadlineExceeded
        );
    };
    let (gateway, ()) = tokio::join!(gateway, caller);
    gateway.expect("gateway ends when the caller hangs up");
    drop(a_backend);
}

#[tokio::test(start_paused = true)]
async fn a_downstream_output_deadline_retires_only_that_downstream() {
    let (caller_io, gateway_io) = duplex(CAPACITY);
    let (a_gw, a_backend) = duplex(CAPACITY);
    let (b_gw, b_backend) = duplex(CAPACITY);
    let (a, a_gate) = gated(a_gw);
    let (b, _b_gate) = gated(b_gw);
    a_gate.stall();
    let mut router = gateway_router(Arc::default(), &[OTHER, REMOTE]);
    let gateway = async {
        let (gateway_io, _) = gated(gateway_io);
        serve_gateway(gateway_io, &mut router, &mut [a, b]).await
    };
    let backend_b = async move {
        let mut service = Remote::named("b/");
        protolink::tokio::server(b_backend)
            .build()
            .serve(&mut service)
            .await
    };
    let caller = async move {
        let client = caller_without_deadlines(caller_io);
        // Backend A never writes: its call's deadline retires it.
        let mut stuck = client
            .streaming_with(
                OTHER[0].path,
                CallOptions::timeout(Duration::from_millis(100)),
            )
            .unwrap();
        stuck.send(b"x").await.unwrap();
        assert_eq!(
            stuck.message().await.unwrap_err().code,
            Code::DeadlineExceeded
        );

        // Backend B is unaffected.
        let mut echo = client.streaming(REMOTE[1].path).unwrap();
        echo.send(b"after").await.unwrap();
        echo.close_send().await.unwrap();
        assert_eq!(echo.message().await.unwrap(), Some(b"after".to_vec()));
        assert_eq!(echo.message().await.unwrap(), None);

        // Backend A is gone: its route fails.
        let mut retired = client.streaming(OTHER[0].path).unwrap();
        retired.send(b"x").await.unwrap();
        retired.close_send().await.unwrap();
        assert_eq!(retired.message().await.unwrap_err().code, Code::Unavailable);
    };
    let (gateway, _backend, ()) = tokio::join!(gateway, backend_b, caller);
    gateway.expect("gateway ends when the caller hangs up");
    drop(a_backend);
}

#[tokio::test]
async fn a_gateway_hangup_while_downstream_blocks_ends_serving_and_cancels_calls() {
    let (caller_io, gateway_io) = duplex(CAPACITY);
    let (a_gw, a_backend) = duplex(CAPACITY);
    let (a, a_gate) = gated(a_gw);
    a_gate.stall();
    let cancelled = Arc::new(AtomicUsize::new(0));
    let mut router = gateway_router(cancelled.clone(), &[OTHER]);
    let gateway = async {
        let (gateway_io, _) = gated(gateway_io);
        serve_gateway(gateway_io, &mut router, &mut [a]).await
    };
    let caller = async move {
        let client = client(caller_io).without_deadlines().build();
        let mut hang = client.streaming(LOCAL[2].path).unwrap();
        hang.send(b"x").await.unwrap();
        // The caller goes away with its call open and a downstream still stalled.
        drop(hang);
        drop(client);
    };
    let (gateway, ()) = tokio::join!(gateway, caller);
    // Serving ends as the caller's side is gone: at its read's end of stream, or
    // at a write to it that fails first. The stalled downstream does not matter.
    if let Err(error) = &gateway {
        assert!(
            matches!(error, protolink::Error::Io(e) if e.kind() == std::io::ErrorKind::BrokenPipe),
            "{error}"
        );
    }
    assert_eq!(cancelled.load(SeqCst), 1);
    drop(a_backend);
}

// ---------------------------------------------------------------------------
// The future is `Send` and can be spawned
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_owning_gateway_task_can_be_spawned() {
    let (caller_io, gateway_io) = duplex(CAPACITY);
    let (gateway_down, backend_io) = duplex(CAPACITY);
    let (unused_gw, _unused_backend) = duplex(CAPACITY);
    let gateway = tokio::spawn(async move {
        let mut router = gateway_router(Arc::default(), &[OTHER, REMOTE]);
        // Backend A serves the `OTHER` route, which this test does not call.
        let mut downstream = [compat(unused_gw), compat(gateway_down)];
        protolink::tokio::server(gateway_io)
            .build()
            .serve_router(&mut router, &mut downstream)
            .await
    });
    let backend = tokio::spawn(async move {
        let mut service = Remote::named("b/");
        protolink::tokio::server(backend_io)
            .build()
            .serve(&mut service)
            .await
    });
    let mut client = client(caller_io).build();
    assert_eq!(client.unary("/t.Local/Upper", b"ok").await.unwrap(), b"OK");
    assert_eq!(
        client.unary(REMOTE[0].path, b"ping").await.unwrap(),
        b"b/pong:ping"
    );
    drop(client);
    gateway
        .await
        .unwrap()
        .expect("gateway ends when the caller hangs up");
    backend.await.unwrap().ok();
}

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_closed_drained_router_ends_serving_while_the_caller_stays_open() {
    let (caller_io, gateway_io) = duplex(CAPACITY);
    let (gateway_down, _backend_io) = duplex(CAPACITY);
    let mut router = gateway_router(Arc::default(), &[REMOTE]);
    router.shutdown();
    let mut downstream = [compat(gateway_down)];
    let serving = protolink::tokio::server(gateway_io)
        .build()
        .serve_router(&mut router, &mut downstream);
    // The caller never closes its side; serving ends anyway.
    let result = tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("serving ends without the caller hanging up");
    result.expect("a shut-down gateway ends cleanly");
    drop(caller_io);
}

#[tokio::test]
async fn goaway_split_across_partial_writes_is_delivered_before_serving_ends() {
    let (mut caller_io, gateway_io) = duplex(CAPACITY);
    let (gateway_down, _backend_io) = duplex(CAPACITY);
    let (gateway, gate) = gated(gateway_io);
    gate.max_write.store(3, SeqCst);
    let mut router = gateway_router(Arc::default(), &[REMOTE]);
    router.shutdown();
    let mut downstream = [compat(gateway_down)];
    let result = protolink::server(gateway)
        .with_deadline_timer(TokioTimer::new())
        .build()
        .serve_router(&mut router, &mut downstream)
        .await;
    result.expect("a shut-down gateway ends cleanly");
    assert!(gate.writes.load(SeqCst) > 3, "the output was split");

    // The gateway's side is closed now, so the caller reads everything it sent.
    let mut bytes = Vec::new();
    caller_io.read_to_end(&mut bytes).await.unwrap();
    let mut http = Connection::client(Default::default());
    http.recv(&bytes).unwrap();
    let goaway =
        std::iter::from_fn(|| http.poll_event()).any(|event| matches!(event, Event::GoAway { .. }));
    assert!(goaway, "the GOAWAY arrived intact after the split writes");
}

#[tokio::test(start_paused = true)]
async fn goaway_with_a_pending_flush_does_not_end_serving_early() {
    let (_caller_io, gateway_io) = duplex(CAPACITY);
    let (gateway_down, _backend_io) = duplex(CAPACITY);
    let (gateway, gate) = gated(gateway_io);
    gate.stall_flush();
    let mut router = gateway_router(Arc::default(), &[REMOTE]);
    router.shutdown();
    let mut downstream = [compat(gateway_down)];
    let serving = protolink::server(gateway)
        .with_deadline_timer(TokioTimer::new())
        .build()
        .serve_router(&mut router, &mut downstream);
    let mut serving = pin!(serving);
    // The GOAWAY is written, but its flush is pending: serving must wait.
    let early = tokio::time::timeout(Duration::from_millis(50), serving.as_mut()).await;
    assert!(early.is_err(), "serving ended before its flush completed");
    assert!(gate.writes.load(SeqCst) >= 1, "the GOAWAY was written");

    gate.release();
    serving
        .await
        .expect("serving ends once the flush completes");
}

#[tokio::test]
async fn shutdown_lets_active_calls_finish() {
    let (mut caller_io, gateway_io) = duplex(CAPACITY);
    let (gateway_down, _backend_io) = duplex(CAPACITY);
    let mut router = gateway_router(Arc::default(), &[REMOTE]);
    // A call that is still open when the gateway starts shutting down.
    let mut caller = GrpcClient::new(GrpcClientConfig::default());
    let id = caller.start_streaming(LOCAL[1].path).unwrap();
    caller.send_message(id, b"hi").unwrap();
    router.recv(&caller.take_output()).unwrap();
    router.shutdown();

    let mut downstream = [compat(gateway_down)];
    let serving = protolink::tokio::server(gateway_io)
        .build()
        .serve_router(&mut router, &mut downstream);
    let finishing = async {
        // The caller finishes its request, then reads the answers. Once the call
        // has finished, the gateway may already have closed its side, so nothing
        // more is written after the call's end.
        caller.close_send(id).unwrap();
        caller_io.write_all(&caller.take_output()).await.unwrap();
        let mut buf = [0u8; 512];
        let mut replies = Vec::new();
        loop {
            let n = caller_io.read(&mut buf).await.unwrap();
            assert!(n > 0, "the gateway hung up before the call finished");
            caller.recv(&buf[..n]).unwrap();
            while let Some(next) = caller.try_next(id) {
                match next {
                    Next::Message(message) => replies.push(message),
                    Next::Done(result) => {
                        assert!(result.is_ok());
                        return replies;
                    }
                }
            }
            let out = caller.take_output();
            if !out.is_empty() {
                caller_io.write_all(&out).await.unwrap();
            }
        }
    };
    let (serving, replies) = tokio::join!(serving, finishing);
    serving.expect("serving ends after the active call finishes");
    assert_eq!(replies, [b"hi".to_vec()]);
}

// ---------------------------------------------------------------------------
// The gateway's own output deadline
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn gateway_output_blocked_past_a_finished_call_deadline_ends_serving() {
    let (caller_io, gateway_io) = duplex(CAPACITY);
    let (gateway_down, _backend_io) = duplex(CAPACITY);
    let (gateway, gate) = gated(gateway_io);
    let mut router = gateway_router(Arc::default(), &[REMOTE]);
    let (down, _) = gated(gateway_down);
    let gateway = async {
        let mut downstream = [down];
        serve_gateway(gateway, &mut router, &mut downstream).await
    };
    let caller = async {
        let client = caller_without_deadlines(caller_io);
        let mut late = client
            .streaming_with(
                LOCAL[3].path,
                CallOptions::timeout(Duration::from_millis(100)),
            )
            .unwrap();
        late.send(b"x").await.unwrap();
        // Block the gateway's writes, then let the call finish before its deadline.
        // Its response cannot be delivered, and the deadline is the call's.
        tokio::time::sleep(Duration::from_millis(10)).await;
        gate.stall();
        tokio::time::sleep(Duration::from_millis(40)).await;
        late.close_send().await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    let (gateway, ()) = tokio::join!(gateway, caller);
    assert!(
        matches!(gateway, Err(protolink::Error::OutputDeadline)),
        "{gateway:?}"
    );
}
