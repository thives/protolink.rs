//! Deadlines with the async drivers, on tokio's paused clock: time only moves
//! when every task is idle, so the timings are exact and the tests are fast.
#![cfg(feature = "tokio")]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::task::{Context, Poll};
use std::time::Duration;

use protolink::grpc::{CallContext, CallId, Code, Handler, MethodKind, Next, Status};
use protolink::{CallOptions, ClientConfig, ServerConfig};
use tokio::time::Instant;

const ECHO: &str = "/t.T/Echo";
const CHAT: &str = "/t.T/Chat";
const QUIET: &str = "/t.T/Quiet";
const BURST: &str = "/t.T/Burst";

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), f)
        .await
        .expect("test timed out")
}

/// `Echo` (unary) echoes the request. `Chat` (bidi) echoes every message.
/// `Quiet` (bidi) never answers. `Burst` (bidi) answers with two messages and
/// then goes quiet.
#[derive(Default)]
struct Peer {
    queues: BTreeMap<CallId, VecDeque<Vec<u8>>>,
    bursts: BTreeSet<CallId>,
    cancelled: Arc<AtomicUsize>,
}

impl Handler for Peer {
    fn call(
        &mut self,
        ctx: &mut CallContext<'_>,
        request: &[u8],
    ) -> Option<Result<Vec<u8>, Status>> {
        (ctx.path == ECHO).then(|| Ok(request.to_vec()))
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        matches!(path, CHAT | QUIET | BURST).then_some(MethodKind::BidiStreaming)
    }

    fn is_unknown_method(&self, path: &str) -> bool {
        !matches!(path, ECHO | CHAT | QUIET | BURST)
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        if ctx.path == CHAT {
            self.queues
                .entry(ctx.id)
                .or_default()
                .push_back(message.to_vec());
        }
        Ok(())
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        if ctx.path == CHAT
            && let Some(message) = self.queues.get_mut(&ctx.id).and_then(VecDeque::pop_front)
        {
            return Poll::Ready(Next::Message(message));
        }
        if ctx.path == BURST {
            let queue = self
                .queues
                .entry(ctx.id)
                .or_insert_with(|| VecDeque::from([b"one".to_vec(), b"two".to_vec()]));
            if let Some(message) = queue.pop_front() {
                self.bursts.insert(ctx.id);
                return Poll::Ready(Next::Message(message));
            }
        }
        Poll::Pending
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        self.queues.remove(&ctx.id);
        self.cancelled.fetch_add(1, SeqCst);
    }
}

type TokioClient = protolink::Client<
    protolink::tokio::FromTokio<tokio::io::DuplexStream>,
    protolink::tokio::TokioTimer,
>;

/// A client connected to a `Peer` served by `protolink::serve`, which has no
/// timer and so never enforces `grpc-timeout`: only the client can end a call.
fn connect() -> (TokioClient, Arc<AtomicUsize>) {
    let (a, b) = tokio::io::duplex(16 * 1024);
    let cancelled = Arc::new(AtomicUsize::new(0));
    let mut handler = Peer {
        cancelled: cancelled.clone(),
        ..Peer::default()
    };
    tokio::spawn(async move {
        let _ = protolink::serve(
            protolink::tokio::compat(a),
            &mut handler,
            ServerConfig::default(),
        )
        .await;
    });
    (
        protolink::tokio::client(b, ClientConfig::default()),
        cancelled,
    )
}

async fn wait_for(counter: &AtomicUsize, expected: usize) {
    with_timeout(async {
        while counter.load(SeqCst) != expected {
            tokio::time::sleep(ms(1)).await;
        }
    })
    .await;
}

fn assert_elapsed(start: Instant, at_least: Duration, below: Duration) {
    let elapsed = start.elapsed();
    assert!(
        (at_least..below).contains(&elapsed),
        "elapsed {elapsed:?}, expected {at_least:?}..{below:?}"
    );
}

// --- Client side ---------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn unary_times_out_without_an_answer() {
    let (_server, b) = tokio::io::duplex(16 * 1024);
    let mut client = protolink::tokio::client(b, ClientConfig::default());
    let start = Instant::now();
    let err = with_timeout(client.unary_with(ECHO, b"hi", CallOptions::timeout(ms(250))))
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded, "{err:?}");
    assert_elapsed(start, ms(250), ms(300));
}

#[tokio::test(start_paused = true)]
async fn default_timeout_applies_to_every_call() {
    let (_server, b) = tokio::io::duplex(16 * 1024);
    let config = ClientConfig {
        default_timeout: Some(ms(100)),
        ..ClientConfig::default()
    };
    let mut client = protolink::tokio::client(b, config);
    let start = Instant::now();
    let err = with_timeout(client.unary(ECHO, b"hi")).await.unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded);
    assert_elapsed(start, ms(100), ms(150));
    // A call's own timeout wins over the default.
    let start = Instant::now();
    let err = with_timeout(client.unary_with(ECHO, b"hi", CallOptions::timeout(ms(300))))
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded);
    assert_elapsed(start, ms(300), ms(350));
}

#[tokio::test(start_paused = true)]
async fn zero_timeout_fails_without_sending() {
    let (mut server, b) = tokio::io::duplex(16 * 1024);
    let mut client = protolink::tokio::client(b, ClientConfig::default());
    let err = client
        .unary_with(ECHO, b"hi", CallOptions::timeout(Duration::ZERO))
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded);
    let Err(err) = client.streaming_with(CHAT, CallOptions::timeout(Duration::ZERO)) else {
        panic!("a zero timeout should fail");
    };
    assert_eq!(err.code, Code::DeadlineExceeded);
    drop(client);
    // Nothing reached the connection.
    let mut sent = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut server, &mut sent)
        .await
        .unwrap();
    assert!(sent.is_empty(), "{sent:?}");
}

#[tokio::test(start_paused = true)]
async fn a_call_that_finishes_in_time_is_not_expired_later() {
    let (mut client, _) = connect();
    with_timeout(async {
        let reply = client
            .unary_with(ECHO, b"hi", CallOptions::timeout(ms(500)))
            .await
            .unwrap();
        assert_eq!(reply.message, b"hi");
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(client.unary(ECHO, b"again").await.unwrap(), b"again");
        assert_eq!(client.with_inner(|c| c.next_deadline()), None);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn streaming_call_times_out_and_keeps_buffered_messages() {
    let (client, _) = connect();
    with_timeout(async {
        let start = Instant::now();
        let mut call = client
            .streaming_with(BURST, CallOptions::timeout(ms(300)))
            .unwrap();
        call.send(b"go").await.unwrap();
        // The messages that arrived come first, then the failure.
        assert_eq!(call.message().await.unwrap().unwrap(), b"one");
        assert_eq!(call.message().await.unwrap().unwrap(), b"two");
        let err = call.message().await.unwrap_err();
        assert_eq!(err.code, Code::DeadlineExceeded, "{err:?}");
        assert_elapsed(start, ms(300), ms(350));
        // The outcome is sticky.
        assert_eq!(
            call.message().await.unwrap_err().code,
            Code::DeadlineExceeded
        );
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_chatty_peer_cannot_postpone_the_deadline() {
    let (client, _) = connect();
    with_timeout(async {
        let start = Instant::now();
        let mut call = client
            .streaming_with(CHAT, CallOptions::timeout(ms(500)))
            .unwrap();
        let mut echoed = 0u8;
        let err = loop {
            call.send(&[echoed]).await.unwrap();
            match call.message().await {
                Ok(Some(message)) => {
                    assert_eq!(message, [echoed]);
                    echoed += 1;
                }
                Ok(None) => panic!("the call ended normally"),
                Err(err) => break err,
            }
            tokio::time::sleep(ms(100)).await;
        };
        assert_eq!(err.code, Code::DeadlineExceeded, "{err:?}");
        assert_eq!(echoed, 5, "one echo per 100 ms until the deadline");
        assert_elapsed(start, ms(500), ms(600));
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn an_earlier_deadline_on_another_call_wakes_the_pending_reader() {
    let (client, cancelled) = connect();
    with_timeout(async {
        let start = Instant::now();
        // A waits for a response that never comes, and has no deadline: its
        // read sleeps forever.
        let mut a = client.streaming(QUIET).unwrap();
        let mut a_message = std::pin::pin!(a.message());
        tokio::select! {
            biased;
            _ = &mut a_message => panic!("A has no response"),
            () = std::future::ready(()) => {}
        }
        // B starts later but with a deadline: A has to give up its read and
        // sleep until B's deadline instead.
        let mut b = client
            .streaming_with(QUIET, CallOptions::timeout(ms(200)))
            .unwrap();
        let err = tokio::select! {
            biased;
            _ = &mut a_message => panic!("A has no response"),
            result = b.message() => result.unwrap_err(),
        };
        assert_eq!(err.code, Code::DeadlineExceeded, "{err:?}");
        assert_elapsed(start, ms(200), ms(250));
    })
    .await;
    // B's reset reaches the server with the next write.
    assert_eq!(cancelled.load(SeqCst), 0, "A is still running");
}

#[tokio::test(start_paused = true)]
async fn client_expiry_resets_the_stream() {
    // A server without a clock never expires the call itself, so only the
    // client's reset can end it.
    let (a, b) = tokio::io::duplex(16 * 1024);
    let cancelled = Arc::new(AtomicUsize::new(0));
    let mut handler = Peer {
        cancelled: cancelled.clone(),
        ..Peer::default()
    };
    tokio::spawn(async move {
        let _ = protolink::serve(
            protolink::tokio::compat(a),
            &mut handler,
            ServerConfig::default(),
        )
        .await;
    });
    let mut client = protolink::tokio::client(b, ClientConfig::default());
    with_timeout(async {
        {
            let mut call = client
                .streaming_with(QUIET, CallOptions::timeout(ms(100)))
                .unwrap();
            let err = call.message().await.unwrap_err();
            assert_eq!(err.code, Code::DeadlineExceeded);
        }
        assert_eq!(cancelled.load(SeqCst), 0, "not reset yet");
        // The next operation writes the reset.
        assert_eq!(client.unary(ECHO, b"x").await.unwrap(), b"x");
        wait_for(&cancelled, 1).await;
    })
    .await;
}

// --- Server side ---------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn server_enforces_the_deadline_for_a_client_without_a_clock() {
    let (a, b) = tokio::io::duplex(16 * 1024);
    let cancelled = Arc::new(AtomicUsize::new(0));
    let mut handler = Peer {
        cancelled: cancelled.clone(),
        ..Peer::default()
    };
    tokio::spawn(async move {
        let _ = protolink::tokio::serve(a, &mut handler, ServerConfig::default()).await;
    });
    // `Client::new` sends `grpc-timeout` but never expires a call itself.
    let client = protolink::Client::new(protolink::tokio::compat(b), ClientConfig::default());
    with_timeout(async {
        let start = Instant::now();
        let mut call = client
            .streaming_with(QUIET, CallOptions::timeout(ms(200)))
            .unwrap();
        call.send(b"x").await.unwrap();
        let err = call.message().await.unwrap_err();
        assert_eq!(err.code, Code::DeadlineExceeded, "{err:?}");
        assert_elapsed(start, ms(200), ms(250));
        wait_for(&cancelled, 1).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn server_without_a_timer_does_not_enforce_the_deadline() {
    let (a, b) = tokio::io::duplex(16 * 1024);
    let cancelled = Arc::new(AtomicUsize::new(0));
    let mut handler = Peer {
        cancelled: cancelled.clone(),
        ..Peer::default()
    };
    tokio::spawn(async move {
        let _ = protolink::serve(
            protolink::tokio::compat(a),
            &mut handler,
            ServerConfig::default(),
        )
        .await;
    });
    let client = protolink::Client::new(protolink::tokio::compat(b), ClientConfig::default());
    let mut call = client
        .streaming_with(QUIET, CallOptions::timeout(ms(100)))
        .unwrap();
    call.send(b"x").await.unwrap();
    let waited = tokio::time::timeout(Duration::from_secs(5), call.message()).await;
    assert!(waited.is_err(), "the call should still be running");
    assert_eq!(cancelled.load(SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn calls_without_a_timeout_run_unbounded() {
    let (client, cancelled) = connect();
    let mut call = client.streaming(QUIET).unwrap();
    call.send(b"x").await.unwrap();
    let waited = tokio::time::timeout(Duration::from_secs(3600), call.message()).await;
    assert!(waited.is_err(), "the call should still be running");
    assert_eq!(cancelled.load(SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn tokio_timer_duration_max_is_bounded_and_re_evaluated() {
    use protolink::{Clock, Timer};
    let timer = protolink::tokio::TokioTimer::new();
    let mut sleep = std::pin::pin!(timer.sleep_until(Duration::MAX));
    assert!(tokio::time::timeout(ms(1), &mut sleep).await.is_err());
    // Poll past the first bounded sleep. The next interval remains pending,
    // rather than treating a representability clamp as the actual deadline.
    tokio::time::advance(Duration::from_secs(2 * 24 * 60 * 60)).await;
    assert!(tokio::time::timeout(ms(1), &mut sleep).await.is_err());
    timer.sleep_until(timer.now()).await;
    let start = Instant::now();
    timer.sleep_until(timer.now() + ms(10)).await;
    assert_eq!(start.elapsed(), ms(10));
}

#[tokio::test(start_paused = true)]
async fn aborting_server_task_cancels_each_active_call_once() {
    struct AbortHandler {
        started: Arc<AtomicUsize>,
        cancelled: Arc<AtomicUsize>,
    }
    impl Handler for AbortHandler {
        fn call(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Option<Result<Vec<u8>, Status>> {
            None
        }
        fn method_kind(&self, _: &str) -> Option<MethodKind> {
            Some(MethodKind::BidiStreaming)
        }
        fn on_message(&mut self, _: &mut CallContext<'_>, _: &[u8]) -> Result<(), Status> {
            self.started.fetch_add(1, SeqCst);
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
            self.cancelled.fetch_add(1, SeqCst);
        }
    }
    let (a, b) = tokio::io::duplex(16 * 1024);
    let started = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicUsize::new(0));
    let mut handler = AbortHandler {
        started: started.clone(),
        cancelled: cancelled.clone(),
    };
    let server = tokio::spawn(async move {
        protolink::tokio::serve(a, &mut handler, ServerConfig::default()).await
    });
    let client = protolink::tokio::client(b, ClientConfig::default());
    let mut first = client.streaming(QUIET).unwrap();
    let mut second = client.streaming(QUIET).unwrap();
    first.send(b"one").await.unwrap();
    second.send(b"two").await.unwrap();
    wait_for(&started, 2).await;
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    assert_eq!(cancelled.load(SeqCst), 2);
}
