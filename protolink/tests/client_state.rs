//! Behaviour of the high-level clients' shared state: the transport is checked
//! out for each operation and always returned, calls are cleaned up when
//! dropped, and (with `std`) the client and its futures are `Send`.
#![cfg(feature = "tokio")]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::task::{Context, Poll};
use std::time::Duration;

use protolink::grpc::{CallContext, CallId, Code, Handler, MethodKind, Next, Status};
use protolink::{CallOptions, ClientConfig, ServerConfig, StreamingTransport};

const ECHO: &str = "/t.T/Echo";
const CHAT: &str = "/t.T/Chat";
const GATE: &str = "/t.T/Gate";
const OPEN: &str = "/t.T/Open";

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("test timed out")
}

/// `Echo` (unary) echoes the request. `Chat` (bidi) echoes every message.
/// `Open` (bidi) opens the gate when it receives a message; `Gate` (bidi)
/// answers once the gate is open. Every bidi call ends when the client
/// half-closes.
#[derive(Default)]
struct Echoes {
    queues: BTreeMap<CallId, VecDeque<Vec<u8>>>,
    ended: BTreeSet<CallId>,
    open: bool,
    answered: BTreeSet<CallId>,
    cancelled: Arc<AtomicUsize>,
}

impl Handler for Echoes {
    fn call(&mut self, ctx: &CallContext<'_>, request: &[u8]) -> Option<Result<Vec<u8>, Status>> {
        let path = ctx.path;
        (path == ECHO).then(|| Ok(request.to_vec()))
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        matches!(path, CHAT | GATE | OPEN).then_some(MethodKind::BidiStreaming)
    }

    fn is_unknown_method(&self, path: &str) -> bool {
        !matches!(path, ECHO | CHAT | GATE | OPEN)
    }

    fn on_message(&mut self, ctx: &CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        let path = ctx.path;
        let call = ctx.id;
        if path == OPEN {
            self.open = true;
        } else {
            self.queues
                .entry(call)
                .or_default()
                .push_back(message.to_vec());
        }
        Ok(())
    }

    fn on_half_close(&mut self, ctx: &CallContext<'_>) -> Result<(), Status> {
        let call = ctx.id;
        self.ended.insert(call);
        Ok(())
    }

    fn poll_response(&mut self, ctx: &CallContext<'_>, _: &mut Context<'_>) -> Poll<Next<Vec<u8>>> {
        let path = ctx.path;
        let call = ctx.id;
        if let Some(message) = self.queues.get_mut(&call).and_then(VecDeque::pop_front) {
            return Poll::Ready(Next::Message(message));
        }
        if path == GATE && self.open && self.answered.insert(call) {
            return Poll::Ready(Next::Message(b"open".to_vec()));
        }
        if self.ended.remove(&call) {
            self.queues.remove(&call);
            return Poll::Ready(Next::Done(Ok(())));
        }
        Poll::Pending
    }

    fn on_cancel(&mut self, ctx: &CallContext<'_>) {
        let call = ctx.id;
        self.queues.remove(&call);
        self.ended.remove(&call);
        self.cancelled.fetch_add(1, SeqCst);
    }
}

/// A client connected to an `Echoes` server, and the server's cancel counter.
fn connect() -> (
    protolink::Client<
        protolink::tokio::FromTokio<tokio::io::DuplexStream>,
        protolink::tokio::TokioTimer,
    >,
    Arc<AtomicUsize>,
) {
    let (a, b) = tokio::io::duplex(16 * 1024);
    let cancelled = Arc::new(AtomicUsize::new(0));
    let mut handler = Echoes {
        cancelled: cancelled.clone(),
        ..Echoes::default()
    };
    tokio::spawn(async move {
        let _ = protolink::tokio::serve(a, &mut handler, ServerConfig::default()).await;
    });
    (
        protolink::tokio::client(b, ClientConfig::default()),
        cancelled,
    )
}

async fn wait_for(counter: &AtomicUsize, expected: usize) {
    with_timeout(async {
        while counter.load(SeqCst) != expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn unknown_streaming_path_is_answered_before_half_close() {
    let (client, cancelled) = connect();
    with_timeout(async {
        let mut call = client.streaming("/t.T/Nope").unwrap();
        call.send(b"hello").await.unwrap();
        // No `close_send`: the status must not depend on it.
        let err = call.message().await.unwrap_err();
        assert_eq!(err.code, Code::Unimplemented);
        drop(call);

        let mut next = client.streaming(CHAT).unwrap();
        next.send(b"still works").await.unwrap();
        assert_eq!(next.message().await.unwrap().unwrap(), b"still works");
        assert_eq!(cancelled.load(SeqCst), 0);
    })
    .await;
}

#[tokio::test]
async fn concurrent_calls_start_and_interleave_on_one_client() {
    let (client, _) = connect();
    with_timeout(async {
        // Both starting entry points accept a second call.
        let mut first = client.streaming(CHAT).unwrap();
        let mut second = StreamingTransport::start(&client, CHAT, CallOptions::default())
            .await
            .unwrap();
        for i in 0..5u8 {
            first.send(&[1, i]).await.unwrap();
            second.send(&[2, i]).await.unwrap();
            // Replies are read in the opposite order from the requests.
            assert_eq!(second.message().await.unwrap().unwrap(), [2, i]);
            assert_eq!(first.message().await.unwrap().unwrap(), [1, i]);
        }
        first.close_send().await.unwrap();
        second.close_send().await.unwrap();
        assert!(first.message().await.unwrap().is_none());
        assert!(second.message().await.unwrap().is_none());
    })
    .await;
}

#[tokio::test]
async fn concurrent_pending_read_does_not_block_another_call_from_sending() {
    let (client, _) = connect();
    with_timeout(async {
        let mut gate = client.streaming(GATE).unwrap();
        let mut open = client.streaming(OPEN).unwrap();
        // The gate answers only after the other call sent a request. Its read
        // is pending first; the send must still get the transport.
        let (reply, sent) = tokio::join!(gate.message(), async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            open.send(b"go").await
        });
        sent.unwrap();
        assert_eq!(reply.unwrap().unwrap(), b"open");

        gate.close_send().await.unwrap();
        open.close_send().await.unwrap();
        assert!(gate.message().await.unwrap().is_none());
        assert!(open.message().await.unwrap().is_none());
    })
    .await;
}

#[tokio::test]
async fn concurrent_half_close_and_send_get_the_transport_from_a_reader() {
    let (client, _) = connect();
    with_timeout(async {
        let mut gate = client.streaming(GATE).unwrap();
        let mut open = client.streaming(OPEN).unwrap();
        let mut chat = client.streaming(CHAT).unwrap();
        let (a, b, c) = tokio::join!(
            async {
                let reply = gate.message().await.unwrap().unwrap();
                gate.close_send().await.unwrap();
                (reply, gate.message().await.unwrap())
            },
            async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                open.send(b"go").await.unwrap();
                open.close_send().await.unwrap();
                open.message().await.unwrap()
            },
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                chat.send(b"hi").await.unwrap();
                let reply = chat.message().await.unwrap().unwrap();
                chat.close_send().await.unwrap();
                (reply, chat.message().await.unwrap())
            },
        );
        assert_eq!(a, (b"open".to_vec(), None));
        assert_eq!(b, None);
        assert_eq!(c, (b"hi".to_vec(), None));
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_tasks_share_one_client() {
    let (client, _) = connect();
    let client = Arc::new(client);
    let tasks: Vec<_> = (0..8u8)
        .map(|task| {
            let client = client.clone();
            tokio::spawn(async move {
                let mut call = client.streaming(CHAT).unwrap();
                for i in 0..25u8 {
                    let message = [task, i];
                    call.send(&message).await.unwrap();
                    assert_eq!(call.message().await.unwrap().unwrap(), message);
                }
                call.close_send().await.unwrap();
                assert!(call.message().await.unwrap().is_none());
            })
        })
        .collect();
    with_timeout(async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await;
}

#[tokio::test]
async fn concurrent_dropped_call_does_not_stop_the_others() {
    let (client, cancelled) = connect();
    with_timeout(async {
        let mut keep = client.streaming(CHAT).unwrap();
        let mut waiting = client.streaming(GATE).unwrap();
        let mut gone = client.streaming(CHAT).unwrap();
        gone.send(b"x").await.unwrap();

        // A read is pending on `waiting` while `gone` is dropped.
        let pending = tokio::time::timeout(Duration::from_millis(20), async {
            tokio::join!(waiting.message(), async {
                tokio::time::sleep(Duration::from_millis(5)).await;
                drop(gone);
            })
        })
        .await;
        assert!(pending.is_err(), "the gate never opens");

        // The cancellation reached the server although only a read was
        // pending, and the other calls still work.
        wait_for(&cancelled, 1).await;
        keep.send(b"still here").await.unwrap();
        assert_eq!(keep.message().await.unwrap().unwrap(), b"still here");
    })
    .await;
}

#[tokio::test]
async fn concurrent_peer_disconnect_fails_every_call() {
    let (a, b) = tokio::io::duplex(16 * 1024);
    let server = tokio::spawn(async move {
        let mut handler = Echoes::default();
        let _ = protolink::tokio::serve(a, &mut handler, ServerConfig::default()).await;
    });
    let client = protolink::tokio::client(b, ClientConfig::default());
    with_timeout(async {
        let mut first = client.streaming(GATE).unwrap();
        let mut second = client.streaming(GATE).unwrap();
        let (r1, r2, ()) = tokio::join!(first.message(), second.message(), async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            server.abort();
        });
        assert_eq!(r1.unwrap_err().code, Code::Unavailable);
        assert_eq!(r2.unwrap_err().code, Code::Unavailable);
        // Sending on a failed call is harmless.
        first.send(b"late").await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn dropping_a_pending_read_returns_the_transport() {
    let (client, _) = connect();
    with_timeout(async {
        let mut call = client.streaming(CHAT).unwrap();
        // Nothing was sent, so no reply can arrive: the read is pending and
        // is dropped when the timeout elapses.
        let timed_out = tokio::time::timeout(Duration::from_millis(30), call.message()).await;
        assert!(timed_out.is_err());

        // The same call is still usable.
        call.send(b"hello").await.unwrap();
        assert_eq!(call.message().await.unwrap().unwrap(), b"hello");
        call.close_send().await.unwrap();
        assert!(call.message().await.unwrap().is_none());
    })
    .await;
}

#[tokio::test]
async fn dropped_call_is_cancelled_and_the_connection_stays_usable() {
    let (mut client, cancelled) = connect();
    with_timeout(async {
        let mut call = client.streaming(CHAT).unwrap();
        call.send(b"one").await.unwrap();
        assert_eq!(call.message().await.unwrap().unwrap(), b"one");
        drop(call);

        // The reset is written with the next operation.
        assert_eq!(client.unary(ECHO, b"after").await.unwrap(), b"after");
        wait_for(&cancelled, 1).await;

        let mut next = client.streaming(CHAT).unwrap();
        next.send(b"two").await.unwrap();
        assert_eq!(next.message().await.unwrap().unwrap(), b"two");
    })
    .await;
}

#[tokio::test]
async fn dropped_unary_future_does_not_break_the_next_calls() {
    let (mut client, _) = connect();
    with_timeout(async {
        {
            // Polled exactly once: the request is written and the read is
            // pending, because the server hasn't run yet. Then it is dropped.
            let mut lost = std::pin::pin!(client.unary(ECHO, b"lost"));
            let first = std::future::poll_fn(|cx| Poll::Ready(lost.as_mut().poll(cx))).await;
            assert!(first.is_pending());
        }

        assert_eq!(client.unary(ECHO, b"kept").await.unwrap(), b"kept");
        // A streaming call can follow as well.
        let mut call = client.streaming(CHAT).unwrap();
        call.send(b"x").await.unwrap();
        assert_eq!(call.message().await.unwrap().unwrap(), b"x");
        call.close_send().await.unwrap();
        assert!(call.message().await.unwrap().is_none());
    })
    .await;
}

#[test]
fn async_client_and_calls_are_send_and_sync() {
    fn send<T: Send>() {}
    fn sync<T: Sync>() {}
    type Io = protolink::tokio::FromTokio<tokio::io::DuplexStream>;
    send::<protolink::Client<Io>>();
    sync::<protolink::Client<Io>>();
    send::<protolink::Call<'static, Io>>();
}

#[tokio::test]
async fn call_can_be_driven_from_a_spawned_task() {
    let (client, _) = connect();
    // `tokio::spawn` requires the whole future, including the call that
    // borrows the client, to be `Send`.
    let task = tokio::spawn(async move {
        let mut call = client.streaming(CHAT).unwrap();
        for i in 0..5u8 {
            call.send(&[i]).await.unwrap();
            assert_eq!(call.message().await.unwrap().unwrap(), [i]);
        }
        call.close_send().await.unwrap();
        assert!(call.message().await.unwrap().is_none());
    });
    with_timeout(task).await.unwrap();
}

#[cfg(feature = "blocking")]
mod blocking {
    use core::convert::Infallible;

    use embedded_io::{ErrorType, Read, Write};
    use protolink::blocking::Client;
    use protolink::{BlockingStreamingTransport, CallOptions, ClientConfig};

    /// A transport that is never used: starting a call does no I/O.
    #[derive(Debug)]
    struct Idle;

    impl ErrorType for Idle {
        type Error = Infallible;
    }

    impl Read for Idle {
        fn read(&mut self, _: &mut [u8]) -> Result<usize, Infallible> {
            Ok(0)
        }
    }

    impl Write for Idle {
        fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> Result<(), Infallible> {
            Ok(())
        }
    }

    #[test]
    fn concurrent_calls_start_on_one_client() {
        let client = Client::new(Idle, ClientConfig::default());
        let first = client.streaming("/t.T/Chat").unwrap();
        let second =
            BlockingStreamingTransport::start(&client, "/t.T/Chat", CallOptions::default())
                .unwrap();
        assert_ne!(first.id(), second.id());
        drop(first);
        assert!(client.with_inner(|c| c.is_pending(second.id())));
    }

    #[test]
    fn with_inner_reads_the_sans_io_state() {
        let client = Client::new(Idle, ClientConfig::default());
        // The HTTP/2 preface is queued as soon as the client exists.
        assert!(client.with_inner(|c| c.has_output()));
        let call = client.streaming("/t.T/Chat").unwrap();
        let id = call.id();
        assert!(client.with_inner(|c| c.is_pending(id)));
        drop(call);
        assert!(!client.with_inner(|c| c.is_pending(id)));
    }
}
