//! End-to-end: micropb messages + generated bindings + protolink drivers.

use std::time::Duration;

use embedded_device_example::device::{Command, Command_, GetStatus, Reply_, Restart, SetOutput};
use embedded_device_example::{
    Device, METHOD_EVENT_SUBSCRIBE, ServiceBlockingClient, ServiceClient, ServiceServer,
};
use embedded_io_adapters::tokio_1::FromTokio;
use protolink::grpc::Code;
use protolink::{ClientConfig, ServerConfig};

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
        // Streaming method -> UNIMPLEMENTED.
        let err = transport
            .unary(METHOD_EVENT_SUBSCRIBE, &[])
            .await
            .unwrap_err();
        assert_eq!(err.code, Code::Unimplemented);
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
