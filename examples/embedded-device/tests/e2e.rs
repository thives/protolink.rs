//! End-to-end: micropb messages + generated bindings + protolink drivers.

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use embedded_device_example::device::{
    Command, Command_, EventSubscribe, GetStatus, Reply, Reply_, Restart, SetOutput,
};
use embedded_device_example::{
    Device, EVENT_OUTPUT_CHANGED, EVENT_RESTART, ServiceBlockingClient, ServiceClient,
    ServiceDispatch,
};
use embedded_io_adapters::tokio_1::FromTokio;
use protolink::grpc::{ClientConfig, Code, ServerConfig, StreamingTransport, UnaryTransport};
use protolink::link::{ArqError, ReliableError, cobs_split};
use protolink::tokio::TokioTimer;
use protolink::{CallOptions, Metadata};

fn get_status(correlation_id: Option<u32>) -> Command {
    let mut command = Command {
        command: Some(Command_::Command::GetStatus(GetStatus {})),
        ..Command::default()
    };
    if let Some(id) = correlation_id {
        command.set_correlation_id(id);
    }
    command
}

fn restart() -> Command {
    Command {
        command: Some(Command_::Command::Restart(Restart { delay_ms: 5 })),
        ..Command::default()
    }
}

fn correlation_id(reply: &Reply) -> Option<u32> {
    reply.correlation_id().copied()
}

fn set_output(channel: u32, enabled: bool) -> Command {
    Command {
        command: Some(Command_::Command::SetOutput(SetOutput { channel, enabled })),
        ..Command::default()
    }
}

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("test timed out")
}

#[tokio::test]
async fn async_client_and_server_over_duplex() {
    let (a, b) = tokio::io::duplex(1024);
    let server = tokio::spawn(async move {
        let mut handler = ServiceDispatch::new(Device {
            uptime_ms: 12_345,
            temperature_milli_c: 23_450,
            supply_millivolts: 3_300,
            ..Device::default()
        });
        protolink::tokio::server(a)
            .build()
            .serve(&mut handler)
            .await
            .unwrap();
        handler.into_inner()
    });

    with_timeout(async {
        let mut client = ServiceClient::new(protolink::tokio::client(b).build());

        let reply = client.command(&get_status(Some(7))).await.unwrap();
        assert_eq!(reply.correlation_id(), Some(&7));
        assert!(matches!(reply.reply, Some(Reply_::Reply::Status(status))
            if status.uptime_ms == 12_345
                && status.temperature_milli_c == 23_450
                && status.supply_millivolts == 3_300));

        let reply = client.command(&get_status(None)).await.unwrap();
        assert_eq!(reply.correlation_id(), None, "absent optional stays absent");

        let reply = client.command(&set_output(1, true)).await.unwrap();
        assert!(
            matches!(reply.reply, Some(Reply_::Reply::OutputState(output))
            if output.channel == 1 && output.enabled)
        );

        let restart = Command {
            command: Some(Command_::Command::Restart(Restart { delay_ms: 5 })),
            ..Command::default()
        };
        client.command(&restart).await.unwrap();

        // Missing oneof -> application-level INVALID_ARGUMENT.
        let err = client.command(&Command::default()).await.unwrap_err();
        assert_eq!(err.code, Code::InvalidArgument);
        assert_eq!(err.message, "missing command");

        // Out-of-range output channel -> application-level INVALID_ARGUMENT.
        let err = client.command(&set_output(4, true)).await.unwrap_err();
        assert_eq!(err.code, Code::InvalidArgument);
        assert_eq!(err.message, "output channel out of range");

        let transport = client.transport_mut();
        // Unknown method -> UNIMPLEMENTED.
        let err = transport
            .unary("/protolink.examples.embedded.device.Service/Nope", &[])
            .await
            .unwrap_err();
        assert_eq!(err.code, Code::Unimplemented);
        // Malformed protobuf -> INVALID_ARGUMENT.
        let err = transport
            .unary(embedded_device_example::METHOD_COMMAND, &[0xff, 0xff, 0xff])
            .await
            .unwrap_err();
        assert_eq!(err.code, Code::InvalidArgument);

        // Still usable after errors.
        assert!(client.command(&get_status(None)).await.is_ok());
        drop(client);
    })
    .await;

    let device = with_timeout(server).await.unwrap();
    assert_eq!(device.restarts, 1);
    assert!(device.output_states[1]);
}

#[tokio::test]
async fn async_over_tcp() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut handler = ServiceDispatch::new(Device::default());
        protolink::tokio::server(stream)
            .build()
            .serve(&mut handler)
            .await
            .unwrap();
    });
    with_timeout(async {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut client = ServiceClient::new(protolink::tokio::client(stream).build());
        for i in 0..20 {
            let reply = client.command(&get_status(Some(i))).await.unwrap();
            assert_eq!(reply.correlation_id(), Some(&i));
        }
    })
    .await;
}

#[tokio::test]
async fn grpc_over_cobs_framing() {
    use protolink::link::CobsFramed;
    let (a, b) = tokio::io::duplex(64);
    tokio::spawn(async move {
        let mut handler = ServiceDispatch::new(Device::default());
        let _ = protolink::server(CobsFramed::new(FromTokio::new(a)))
            .without_deadlines()
            .build()
            .serve(&mut handler)
            .await;
    });
    with_timeout(async {
        let io = CobsFramed::new(FromTokio::new(b));
        let mut client = ServiceClient::new(protolink::client(io).without_deadlines().build());
        for i in 0..10 {
            let reply = client.command(&get_status(Some(i))).await.unwrap();
            assert_eq!(reply.correlation_id(), Some(&i));
        }
    })
    .await;
}

#[tokio::test]
async fn reliable_link_cobs_arq() {
    let (a, b) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let link = protolink::link::reliable(FromTokio::new(a)).build();
        let mut handler = ServiceDispatch::new(Device::default());
        let _ = protolink::server(link)
            .without_deadlines()
            .build()
            .serve(&mut handler)
            .await;
    });
    with_timeout(async {
        let link = protolink::link::reliable(FromTokio::new(b)).build();
        let mut client = ServiceClient::new(protolink::client(link).without_deadlines().build());
        for i in 0..5 {
            let reply = client.command(&get_status(Some(100 + i))).await.unwrap();
            assert_eq!(reply.correlation_id(), Some(&(100 + i)));
        }
    })
    .await;
}

#[tokio::test]
async fn async_streaming_over_duplex() {
    let (a, b) = tokio::io::duplex(1024);
    let server = tokio::spawn(async move {
        let mut handler = ServiceDispatch::new(Device::default());
        protolink::tokio::server(a)
            .build()
            .serve(&mut handler)
            .await
            .unwrap();
        handler.into_inner()
    });

    with_timeout(async {
        let mut client = ServiceClient::new(protolink::tokio::client(b).build());

        // Server streaming, empty: no events yet.
        let mut events = client.event_subscribe(&EventSubscribe {}).await.unwrap();
        assert!(events.message().await.unwrap().is_none());
        drop(events);

        // Client streaming, empty.
        let batch = client.command_batch().await.unwrap();
        let summary = batch.finish().await.unwrap();
        assert_eq!((summary.accepted, summary.rejected), (0, 0));

        // Client streaming: several commands, one rejected.
        let mut batch = client.command_batch().await.unwrap();
        for command in [
            set_output(0, true),
            set_output(9, true),
            restart(),
            set_output(0, false),
        ] {
            batch.send(&command).await.unwrap();
        }
        let summary = batch.finish().await.unwrap();
        assert_eq!((summary.accepted, summary.rejected), (3, 1));

        // Server streaming replays the event log, then ends.
        let mut events = client.event_subscribe(&EventSubscribe {}).await.unwrap();
        let mut codes = Vec::new();
        while let Some(event) = events.message().await.unwrap() {
            codes.push(event.code);
        }
        assert_eq!(
            codes,
            [EVENT_OUTPUT_CHANGED, EVENT_RESTART, EVENT_OUTPUT_CHANGED]
        );
        assert!(events.message().await.unwrap().is_none(), "end is sticky");
        drop(events);

        // Two bidirectional calls can be active and interleaved on one client.
        let mut first = client.command_stream().await.unwrap();
        let mut second = client.command_stream().await.unwrap();
        first.send(&get_status(Some(71))).await.unwrap();
        second.send(&get_status(Some(72))).await.unwrap();
        first.close_send().await.unwrap();
        second.close_send().await.unwrap();
        // Read in the opposite order from the requests.
        assert_eq!(
            correlation_id(&second.message().await.unwrap().unwrap()),
            Some(72)
        );
        assert_eq!(
            correlation_id(&first.message().await.unwrap().unwrap()),
            Some(71)
        );
        assert!(first.message().await.unwrap().is_none());
        assert!(second.message().await.unwrap().is_none());
        drop(first);
        drop(second);

        // Bidi, ping-pong: each reply arrives before the next request.
        let mut stream = client.command_stream().await.unwrap();
        for i in 0..5 {
            stream.send(&get_status(Some(i))).await.unwrap();
            let reply = stream.message().await.unwrap().unwrap();
            assert_eq!(correlation_id(&reply), Some(i));
        }
        stream.close_send().await.unwrap();
        assert!(stream.message().await.unwrap().is_none());
        drop(stream);

        // Bidi, empty.
        let mut stream = client.command_stream().await.unwrap();
        stream.close_send().await.unwrap();
        assert!(stream.message().await.unwrap().is_none());
        drop(stream);

        // Bidi: an error after replies keeps the replies sent before it.
        let mut stream = client.command_stream().await.unwrap();
        for i in 0..3 {
            stream.send(&get_status(Some(i))).await.unwrap();
        }
        stream.send(&Command::default()).await.unwrap();
        stream.send(&get_status(Some(99))).await.unwrap();
        stream.close_send().await.unwrap();
        for i in 0..3 {
            let reply = stream.message().await.unwrap().unwrap();
            assert_eq!(correlation_id(&reply), Some(i));
        }
        let err = stream.message().await.unwrap_err();
        assert_eq!(err.code, Code::InvalidArgument);
        assert_eq!(err.message, "missing command");
        assert_eq!(stream.message().await.unwrap_err(), err, "error is sticky");
        drop(stream);

        // Cancel by drop, mid-stream, for both streaming directions.
        let mut events = client.event_subscribe(&EventSubscribe {}).await.unwrap();
        assert!(events.message().await.unwrap().is_some());
        drop(events);
        let mut stream = client.command_stream().await.unwrap();
        stream.send(&get_status(Some(1))).await.unwrap();
        assert!(stream.message().await.unwrap().is_some());
        drop(stream);

        // The connection is still usable.
        let reply = client.command(&get_status(Some(2))).await.unwrap();
        assert_eq!(correlation_id(&reply), Some(2));
        let mut events = client.event_subscribe(&EventSubscribe {}).await.unwrap();
        let mut count = 0;
        while events.message().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    })
    .await;

    let device = with_timeout(server).await.unwrap();
    assert_eq!(device.calls.active(), 0, "streaming call state released");
    assert_eq!(device.restarts, 1);
    assert!(!device.output_states[0]);
}

#[test]
fn blocking_streaming_over_tcp() {
    use embedded_io_adapters::std::FromStd;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut handler = ServiceDispatch::new(Device::default());
        protolink::blocking::server(FromStd::new(stream))
            .without_deadlines()
            .build()
            .serve(&mut handler)
            .unwrap();
        handler.into_inner()
    });
    let stream = std::net::TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut client = ServiceBlockingClient::new(
        protolink::blocking::client(FromStd::new(stream))
            .without_deadlines()
            .build(),
    );

    // An empty replay is a trailers-only response: all its metadata is
    // trailing.
    let mut events = client.event_subscribe(&EventSubscribe {}).unwrap();
    assert!(events.message().unwrap().is_none());
    assert!(events.headers().is_none());
    assert_eq!(
        events.trailers().unwrap().get("x-events-replayed"),
        Some("0")
    );
    drop(events);

    // Request metadata is echoed in the response headers.
    let mut metadata = Metadata::new();
    metadata.insert("x-request-id", "req-7").unwrap();
    let reply = client
        .command_with_options(&get_status(Some(3)), CallOptions::metadata(metadata))
        .unwrap();
    assert_eq!(reply.headers.get("x-request-id"), Some("req-7"));
    assert_eq!(reply.trailers.get("x-device-uptime-ms"), Some("0"));

    // A call that fails: its trailers are in the status.
    let err = client
        .command_with_options(&Command::default(), CallOptions::new())
        .unwrap_err();
    assert_eq!(err.code, Code::InvalidArgument);
    assert_eq!(err.metadata.get("x-device-uptime-ms"), Some("0"));

    let mut batch = client.command_batch().unwrap();
    batch.send(&set_output(2, true)).unwrap();
    batch.send(&restart()).unwrap();
    let summary = batch.finish().unwrap();
    assert_eq!((summary.accepted, summary.rejected), (2, 0));

    let mut events = client.event_subscribe(&EventSubscribe {}).unwrap();
    let mut codes = Vec::new();
    while let Some(event) = events.message().unwrap() {
        codes.push(event.code);
    }
    assert_eq!(codes, [EVENT_OUTPUT_CHANGED, EVENT_RESTART]);
    drop(events);

    let mut first = client.command_stream().unwrap();
    let mut second = client.command_stream().unwrap();
    first.send(&get_status(Some(11))).unwrap();
    second.send(&get_status(Some(12))).unwrap();
    first.close_send().unwrap();
    second.close_send().unwrap();
    assert_eq!(correlation_id(&first.message().unwrap().unwrap()), Some(11));
    assert_eq!(
        correlation_id(&second.message().unwrap().unwrap()),
        Some(12)
    );
    assert!(first.message().unwrap().is_none());
    assert!(second.message().unwrap().is_none());
    drop(first);
    drop(second);

    let mut stream = client.command_stream().unwrap();
    stream.send(&get_status(Some(1))).unwrap();
    stream.send(&get_status(Some(2))).unwrap();
    stream.close_send().unwrap();
    let replies: Vec<_> = std::iter::from_fn(|| stream.message().unwrap())
        .map(|r| correlation_id(&r))
        .collect();
    assert_eq!(replies, [Some(1), Some(2)]);
    drop(stream);

    // Cancel by drop, then keep using the connection.
    let mut stream = client.command_stream().unwrap();
    stream.send(&get_status(Some(3))).unwrap();
    assert!(stream.message().unwrap().is_some());
    drop(stream);
    let reply = client.command(&get_status(Some(4))).unwrap();
    assert_eq!(correlation_id(&reply), Some(4));

    drop(client);
    let device = server.join().unwrap();
    assert_eq!(device.calls.active(), 0);
    assert_eq!(device.restarts, 1);
}

#[tokio::test]
async fn streaming_over_cobs_framing() {
    use protolink::link::CobsFramed;
    // The drivers do not read while blocked in a write, so a bidi call that
    // sends many requests before reading needs a transport that buffers the
    // replies in flight (a 64-byte pipe would deadlock both writers).
    let (a, b) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut handler = ServiceDispatch::new(Device::default());
        let _ = protolink::server(CobsFramed::new(FromTokio::new(a)))
            .without_deadlines()
            .build()
            .serve(&mut handler)
            .await;
    });
    with_timeout(async {
        let io = CobsFramed::new(FromTokio::new(b));
        let client = ServiceClient::new(protolink::client(io).without_deadlines().build());
        let mut stream = client.command_stream().await.unwrap();
        for i in 0..10 {
            stream.send(&get_status(Some(i))).await.unwrap();
        }
        stream.send(&restart()).await.unwrap();
        stream.close_send().await.unwrap();
        for i in 0..10 {
            let reply = stream.message().await.unwrap().unwrap();
            assert_eq!(correlation_id(&reply), Some(i));
        }
        let ack = stream.message().await.unwrap().unwrap();
        assert!(matches!(ack.reply, Some(Reply_::Reply::RestartAck(_))));
        assert!(stream.message().await.unwrap().is_none());
        drop(stream);

        let mut events = client.event_subscribe(&EventSubscribe {}).await.unwrap();
        let event = events.message().await.unwrap().unwrap();
        assert_eq!(event.code, EVENT_RESTART);
        assert!(events.message().await.unwrap().is_none());
    })
    .await;
}

/// Deadlines through the generated `*_with_options` methods. The clock is
/// paused, so the 300 ms below take no real time.
#[tokio::test(start_paused = true)]
async fn deadlines_through_generated_clients() {
    let (a, b) = tokio::io::duplex(1024);
    tokio::spawn(async move {
        let mut handler = ServiceDispatch::new(Device::default());
        let _ = protolink::tokio::server(a)
            .build()
            .serve(&mut handler)
            .await;
    });
    with_timeout(async {
        let mut client = ServiceClient::new(protolink::tokio::client(b).build());
        let timeout = |ms| CallOptions::timeout(Duration::from_millis(ms));

        // A call that finishes in time is unaffected.
        let reply = client
            .command_with_options(&get_status(Some(1)), timeout(1000))
            .await
            .unwrap();
        assert_eq!(correlation_id(&reply.message), Some(1));

        // A zero timeout fails at once.
        let err = client
            .command_with_options(&get_status(None), timeout(0))
            .await
            .unwrap_err();
        assert_eq!(err.code, Code::DeadlineExceeded);

        // An idle bidirectional stream runs out of time; replies that were
        // delivered before are not lost.
        let start = tokio::time::Instant::now();
        let mut stream = client
            .command_stream_with_options(timeout(300))
            .await
            .unwrap();
        stream.send(&get_status(Some(2))).await.unwrap();
        let reply = stream.message().await.unwrap().unwrap();
        assert_eq!(correlation_id(&reply), Some(2));
        let err = stream.message().await.unwrap_err();
        assert_eq!(err.code, Code::DeadlineExceeded);
        assert!(start.elapsed() >= Duration::from_millis(300));
        drop(stream);

        // The connection is still usable, and the plain methods have no
        // deadline.
        let reply = client.command(&get_status(Some(3))).await.unwrap();
        assert_eq!(correlation_id(&reply), Some(3));
        let mut events = client
            .event_subscribe_with_options(&EventSubscribe {}, timeout(1000))
            .await
            .unwrap();
        assert!(events.message().await.unwrap().is_none());
    })
    .await;
}

/// A client without a clock still sends `grpc-timeout`, so the server ends
/// the call.
#[tokio::test(start_paused = true)]
async fn server_enforces_the_deadline_for_a_client_without_a_clock() {
    let (a, b) = tokio::io::duplex(1024);
    tokio::spawn(async move {
        let mut handler = ServiceDispatch::new(Device::default());
        let _ = protolink::tokio::server(a)
            .build()
            .serve(&mut handler)
            .await;
    });
    with_timeout(async {
        let client = ServiceClient::new(
            protolink::client(FromTokio::new(b))
                .without_deadlines()
                .build(),
        );
        let mut stream = client
            .command_stream_with_options(CallOptions::timeout(Duration::from_millis(200)))
            .await
            .unwrap();
        stream.send(&get_status(Some(1))).await.unwrap();
        let reply = stream.message().await.unwrap().unwrap();
        assert_eq!(correlation_id(&reply), Some(1));
        let err = stream.message().await.unwrap_err();
        assert_eq!(err.code, Code::DeadlineExceeded);
    })
    .await;
}

/// Custom metadata through the generated clients: request metadata in
/// `CallOptions`, response headers and trailers in `Response`, and the
/// headers of a streaming call from the returned stream.
#[tokio::test]
async fn metadata_through_generated_clients() {
    let (a, b) = tokio::io::duplex(1024);
    tokio::spawn(async move {
        let mut handler = ServiceDispatch::new(Device {
            uptime_ms: 777,
            ..Device::default()
        });
        let _ = protolink::tokio::server(a)
            .build()
            .serve(&mut handler)
            .await;
    });
    with_timeout(async {
        let mut client = ServiceClient::new(protolink::tokio::client(b).build());

        let mut metadata = Metadata::new();
        metadata.insert("x-request-id", "req-42").unwrap();
        metadata.insert_bin("x-trace-bin", &[1, 2, 3]).unwrap();
        let reply = client
            .command_with_options(&get_status(Some(9)), CallOptions::metadata(metadata))
            .await
            .unwrap();
        assert_eq!(correlation_id(&reply.message), Some(9));
        assert_eq!(reply.headers.get("x-request-id"), Some("req-42"));
        assert_eq!(reply.trailers.get("x-device-uptime-ms"), Some("777"));

        // Without request metadata nothing is echoed.
        let reply = client
            .command_with_options(&get_status(None), CallOptions::new())
            .await;
        let reply = reply.unwrap();
        assert!(reply.headers.get("x-request-id").is_none());

        // A streaming call's headers arrive before its messages end.
        client.command(&set_output(0, true)).await.unwrap();
        let mut events = client.event_subscribe(&EventSubscribe {}).await.unwrap();
        assert!(events.message().await.unwrap().is_some());
        assert_eq!(
            events.headers().unwrap().get("x-events-replayed"),
            Some("1")
        );
        assert!(events.trailers().is_none(), "the call is still running");
    })
    .await;
}

// --- Small-pipe matrix ---------------------------------------------------------

/// Run `test` on a thread with a large stack: in debug builds the reliable
/// stack's futures and the matrix's servers are too big for the default 2 MiB
/// test-thread stack.
fn on_big_stack<Fut>(test: impl FnOnce() -> Fut + Send + 'static)
where
    Fut: Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap()
                .block_on(test());
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Pipe capacities of the matrix, in bytes. Every exchange moves far more than
/// one pipe holds, in each direction.
const PIPES: [usize; 3] = [32, 64, 128];
/// Calls of each kind in a matrix exchange.
const MATRIX_CALLS: u32 = 40;

/// Client credit and retained-result capacity for the small pipes: a stream
/// window of a few hundred bytes, a connection window of four of them, and a
/// retained-response budget well above one reply but far below the transfer.
fn matrix_client_config() -> ClientConfig {
    let mut config = ClientConfig::default();
    config.http2.initial_window_size = 256;
    config.http2.connection_window_size = 1024;
    config.max_buffered_response_bytes = 4 * 1024;
    config.max_retained_calls = 8;
    config
}

fn matrix_server_config() -> ServerConfig {
    let mut config = ServerConfig::default();
    config.http2.initial_window_size = 256;
    config.http2.connection_window_size = 1024;
    config
}

/// The finite exchange every stack must complete: unary calls, a batch the
/// server only answers at the end, and a bidirectional call answered one
/// request at a time. Replies must arrive in order, and the last call must
/// still succeed.
async fn finite_exchange<T>(mut client: ServiceClient<T>)
where
    T: UnaryTransport + StreamingTransport,
{
    for id in 0..MATRIX_CALLS {
        let reply = client.command(&get_status(Some(id))).await.unwrap();
        assert_eq!(correlation_id(&reply), Some(id));
    }

    let mut batch = client.command_batch().await.unwrap();
    for i in 0..MATRIX_CALLS {
        batch.send(&set_output(i % 2, i % 2 == 0)).await.unwrap();
    }
    let summary = batch.finish().await.unwrap();
    assert_eq!((summary.accepted, summary.rejected), (MATRIX_CALLS, 0));

    let mut stream = client.command_stream().await.unwrap();
    for id in 0..MATRIX_CALLS {
        stream.send(&get_status(Some(id))).await.unwrap();
        let reply = stream.message().await.unwrap().unwrap();
        assert_eq!(correlation_id(&reply), Some(id));
    }
    stream.close_send().await.unwrap();
    assert!(stream.message().await.unwrap().is_none());
    drop(stream);

    let last = client.command(&get_status(None)).await.unwrap();
    assert!(matches!(last.reply, Some(Reply_::Reply::Status(_))));
}

/// Control: a peer that does not read holds the sender back. The request is
/// too large for the peer's window, so it cannot finish until the server runs.
/// Then the same connection carries the finite exchange.
async fn backpressure_control<T, S>(
    mut client: ServiceClient<T>,
    server: S,
) -> (ServiceClient<T>, tokio::task::JoinHandle<()>)
where
    T: UnaryTransport,
    S: Future<Output = ()> + Send + 'static,
{
    let body = vec![0u8; 4096];
    let (handle, result) = {
        let transport = client.transport_mut();
        let mut call = pin!(transport.unary(
            "/protolink.examples.embedded.device.Service/Nope",
            &body,
            CallOptions::default(),
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), call.as_mut())
                .await
                .is_err(),
            "a request went through a peer that never read"
        );
        let handle = tokio::spawn(server);
        (handle, call.await)
    };
    assert_eq!(result.unwrap_err().code, Code::Unimplemented);
    (client, handle)
}

/// Run the control and then the exchange on one connection, and check that the
/// server ended cleanly: a deadline or overload retirement is not a pass.
async fn small_pipe_case<T, S>(client: ServiceClient<T>, server: S)
where
    T: UnaryTransport + StreamingTransport,
    S: Future<Output = ()> + Send + 'static,
{
    with_timeout(async {
        let (client, handle) = backpressure_control(client, server).await;
        finite_exchange(client).await;
        handle.await.unwrap();
    })
    .await;
}

#[test]
fn small_pipe_matrix_raw_duplex() {
    on_big_stack(|| async {
        for pipe in PIPES {
            let (a, b) = tokio::io::duplex(pipe);
            let server = async move {
                let mut handler = ServiceDispatch::new(Device::default());
                protolink::tokio::server(a)
                    .with_config(matrix_server_config())
                    .build()
                    .serve(&mut handler)
                    .await
                    .unwrap();
            };
            let client = ServiceClient::new(
                protolink::tokio::client(b)
                    .with_config(matrix_client_config())
                    .build(),
            );
            small_pipe_case(client, server).await;
        }
    });
}

#[test]
fn generated_streaming_over_split_cobs_small_pipe() {
    on_big_stack(|| async {
        for pipe in PIPES {
            let (a, b) = tokio::io::duplex(pipe);
            let (a_rd, a_wr) = tokio::io::split(a);
            let (b_rd, b_wr) = tokio::io::split(b);
            let (server_rx, server_tx) = cobs_split(FromTokio::new(a_rd), FromTokio::new(a_wr));
            let (client_rx, client_tx) = cobs_split(FromTokio::new(b_rd), FromTokio::new(b_wr));
            let server = async move {
                let mut handler = ServiceDispatch::new(Device::default());
                protolink::server_split(server_rx, server_tx)
                    .with_deadline_timer(TokioTimer::new())
                    .with_config(matrix_server_config())
                    .build()
                    .serve(&mut handler)
                    .await
                    .unwrap();
            };
            let client = ServiceClient::new(
                protolink::client_split(client_rx, client_tx)
                    .with_deadline_timer(TokioTimer::new())
                    .with_config(matrix_client_config())
                    .build(),
            );
            small_pipe_case(client, server).await;
        }
    });
}

#[test]
fn small_pipe_matrix_coordinated_reliable() {
    on_big_stack(|| async {
        for pipe in PIPES {
            let (a, b) = tokio::io::duplex(pipe);
            let server_link = protolink::link::reliable(FromTokio::new(a)).build();
            let client_link = protolink::link::reliable(FromTokio::new(b)).build();
            let server = async move {
                let mut handler = ServiceDispatch::new(Device::default());
                let result = protolink::server_reliable(server_link)
                    .with_deadline_timer(TokioTimer::new())
                    .with_config(matrix_server_config())
                    .build()
                    .serve(&mut handler)
                    .await;
                // The client hanging up is the only way the exchange ends, and ARQ
                // reports it as a closed link. Anything else, such as a deadline or an
                // overload, fails the test.
                match result {
                    Ok(()) => {}
                    Err(protolink::Error::Io(ReliableError(ArqError::Closed))) => {}
                    Err(e) => panic!("the server ended with {e:?}"),
                }
            };
            let client = ServiceClient::new(
                protolink::client_reliable(client_link)
                    .with_deadline_timer(TokioTimer::new())
                    .with_config(matrix_client_config())
                    .build(),
            );
            small_pipe_case(client, server).await;
        }
    });
}
