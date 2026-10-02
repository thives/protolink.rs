//! End-to-end: micropb messages + generated bindings + protolink drivers.

use std::time::Duration;

use embedded_device_example::device::{
    Command, Command_, EventSubscribe, GetStatus, Reply, Reply_, Restart, SetOutput,
};
use embedded_device_example::{
    Device, EVENT_OUTPUT_CHANGED, EVENT_RESTART, ServiceBlockingClient, ServiceClient,
    ServiceServer,
};
use embedded_io_adapters::tokio_1::FromTokio;
use protolink::grpc::Code;
use protolink::{CallOptions, ClientConfig, ServerConfig};

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
        let mut handler = ServiceServer(Device {
            uptime_ms: 12_345,
            temperature_milli_c: 23_450,
            supply_millivolts: 3_300,
            ..Device::default()
        });
        protolink::tokio::serve(a, &mut handler, ServerConfig::default())
            .await
            .unwrap();
        handler.0
    });

    with_timeout(async {
        let mut client = ServiceClient::new(protolink::tokio::client(b, ClientConfig::default()));

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
        let mut handler = ServiceServer(Device::default());
        protolink::tokio::serve(stream, &mut handler, ServerConfig::default())
            .await
            .unwrap();
    });
    with_timeout(async {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut client =
            ServiceClient::new(protolink::tokio::client(stream, ClientConfig::default()));
        for i in 0..20 {
            let reply = client.command(&get_status(Some(i))).await.unwrap();
            assert_eq!(reply.correlation_id(), Some(&i));
        }
    })
    .await;
}

#[test]
fn blocking_client_and_server_over_tcp() {
    use embedded_io_adapters::std::FromStd;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut handler = ServiceServer(Device::default());
        protolink::blocking::serve(FromStd::new(stream), &mut handler, ServerConfig::default())
            .unwrap();
    });
    let stream = std::net::TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut client = ServiceBlockingClient::new(protolink::blocking::Client::new(
        FromStd::new(stream),
        ClientConfig::default(),
    ));
    let reply = client.command(&get_status(Some(11))).unwrap();
    assert_eq!(reply.correlation_id(), Some(&11));
    drop(client);
    server.join().unwrap();
}

#[tokio::test]
async fn grpc_over_cobs_framing() {
    use protolink::link::CobsFramed;
    let (a, b) = tokio::io::duplex(64);
    tokio::spawn(async move {
        let mut handler = ServiceServer(Device::default());
        let _ = protolink::serve(
            CobsFramed::new(FromTokio::new(a)),
            &mut handler,
            ServerConfig::default(),
        )
        .await;
    });
    with_timeout(async {
        let io = CobsFramed::new(FromTokio::new(b));
        let mut client = ServiceClient::new(protolink::Client::new(io, ClientConfig::default()));
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
        let link = protolink::link::reliable(FromTokio::new(a));
        let mut handler = ServiceServer(Device::default());
        let _ = protolink::serve(link, &mut handler, ServerConfig::default()).await;
    });
    with_timeout(async {
        let link = protolink::link::reliable(FromTokio::new(b));
        let mut client = ServiceClient::new(protolink::Client::new(link, ClientConfig::default()));
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
        let mut handler = ServiceServer(Device::default());
        protolink::tokio::serve(a, &mut handler, ServerConfig::default())
            .await
            .unwrap();
        handler.0
    });

    with_timeout(async {
        let mut client = ServiceClient::new(protolink::tokio::client(b, ClientConfig::default()));

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
        let mut handler = ServiceServer(Device::default());
        protolink::blocking::serve(FromStd::new(stream), &mut handler, ServerConfig::default())
            .unwrap();
        handler.0
    });
    let stream = std::net::TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut client = ServiceBlockingClient::new(protolink::blocking::Client::new(
        FromStd::new(stream),
        ClientConfig::default(),
    ));

    let mut events = client.event_subscribe(&EventSubscribe {}).unwrap();
    assert!(events.message().unwrap().is_none());
    drop(events);

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
        let mut handler = ServiceServer(Device::default());
        let _ = protolink::serve(
            CobsFramed::new(FromTokio::new(a)),
            &mut handler,
            ServerConfig::default(),
        )
        .await;
    });
    with_timeout(async {
        let io = CobsFramed::new(FromTokio::new(b));
        let client = ServiceClient::new(protolink::Client::new(io, ClientConfig::default()));
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
        let mut handler = ServiceServer(Device::default());
        let _ = protolink::tokio::serve(a, &mut handler, ServerConfig::default()).await;
    });
    with_timeout(async {
        let mut client = ServiceClient::new(protolink::tokio::client(b, ClientConfig::default()));
        let timeout = |ms| CallOptions::timeout(Duration::from_millis(ms));

        // A call that finishes in time is unaffected.
        let reply = client
            .command_with_options(&get_status(Some(1)), timeout(1000))
            .await
            .unwrap();
        assert_eq!(correlation_id(&reply), Some(1));

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
        let mut handler = ServiceServer(Device::default());
        let _ = protolink::tokio::serve(a, &mut handler, ServerConfig::default()).await;
    });
    with_timeout(async {
        let client = ServiceClient::new(protolink::Client::new(
            FromTokio::new(b),
            ClientConfig::default(),
        ));
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
