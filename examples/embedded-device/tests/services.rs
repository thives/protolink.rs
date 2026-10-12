//! Assembling generated services: owned and borrowed dispatchers, a registry
//! of independent objects, one object behind two services, and connection-local
//! call state.

use std::time::Duration;

use embedded_device_example::device::{Command, Command_, GetCounters, GetStatus, SetOutput};
use embedded_device_example::{
    Device, Diagnostics, DiagnosticsClient, DiagnosticsDispatch, DiagnosticsMethods,
    EVENT_OUTPUT_CHANGED, METHOD_COMMAND, ServiceClient, ServiceDispatch, ServiceMethods,
};
use protolink::grpc::{CallContext, Code, Status};
use protolink::{Handler, MultiDispatch, Registry, RouteConflict};
use tokio::io::DuplexStream;

type Transport =
    protolink::Client<protolink::tokio::FromTokio<DuplexStream>, protolink::tokio::TokioTimer>;

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("test timed out")
}

fn get_status(id: u32) -> Command {
    let mut command = Command {
        command: Some(Command_::Command::GetStatus(GetStatus {})),
        ..Command::default()
    };
    command.set_correlation_id(id);
    command
}

fn set_output(channel: u32) -> Command {
    Command {
        command: Some(Command_::Command::SetOutput(SetOutput {
            channel,
            enabled: true,
        })),
        ..Command::default()
    }
}

/// A connected client transport, and the server end.
fn connect() -> (Transport, DuplexStream) {
    let (a, b) = tokio::io::duplex(4096);
    (protolink::tokio::client(b).build(), a)
}

/// Serve `services` on `io` until the client disconnects.
async fn serve<H: Handler>(io: DuplexStream, services: &mut H) {
    protolink::tokio::server(io)
        .build()
        .serve(services)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_borrowed_implementation_is_usable_after_the_connection() {
    let (transport, io) = connect();
    let mut device = Device::default();
    let server = async {
        let mut services = ServiceDispatch::new(&mut device);
        serve(io, &mut services).await;
    };
    let client = async move {
        let mut client = ServiceClient::new(transport);
        // Every RPC shape reaches the borrowed device.
        client.command(&set_output(1)).await.unwrap();
        let mut batch = client.command_batch().await.unwrap();
        batch.send(&set_output(2)).await.unwrap();
        batch.send(&set_output(9)).await.unwrap();
        let summary = batch.finish().await.unwrap();
        assert_eq!((summary.accepted, summary.rejected), (1, 1));
        let mut stream = client.command_stream().await.unwrap();
        stream.send(&get_status(5)).await.unwrap();
        assert_eq!(
            stream.message().await.unwrap().unwrap().correlation_id(),
            Some(&5)
        );
        stream.close_send().await.unwrap();
        assert!(stream.message().await.unwrap().is_none());
        drop(stream);
        let mut events = client.event_subscribe(&Default::default()).await.unwrap();
        let mut replayed = 0;
        while events.message().await.unwrap().is_some() {
            replayed += 1;
        }
        assert_eq!(replayed, 2);
    };
    with_timeout(async { tokio::join!(server, client) }).await;

    // The dispatcher's borrow has ended: the device is the application's again.
    assert_eq!(device.output_states, [false, true, true, false]);
    assert_eq!(device.events, [EVENT_OUTPUT_CHANGED, EVENT_OUTPUT_CHANGED]);
    assert_eq!((device.executed, device.rejected), (3, 1));
    assert_eq!(device.calls.active(), 0);
}

#[tokio::test]
async fn an_owned_implementation_is_returned_by_the_dispatcher() {
    let (transport, io) = connect();
    let server = tokio::spawn(async move {
        let mut services = ServiceDispatch::new(Device::default());
        assert_eq!(services.get_ref().executed, 0);
        serve(io, &mut services).await;
        services.into_inner()
    });
    let mut client = ServiceClient::new(transport);
    with_timeout(client.command(&get_status(1))).await.unwrap();
    drop(client);
    let device = with_timeout(server).await.unwrap();
    assert_eq!(device.executed, 1);
}

/// A diagnostics implementation that is not the device.
#[derive(Debug, Default)]
struct Monitor {
    queries: u32,
}

impl Diagnostics for Monitor {
    fn get_counters(
        &mut self,
        _: &mut CallContext<'_>,
        _: GetCounters,
    ) -> Result<embedded_device_example::device::Counters, Status> {
        self.queries += 1;
        Ok(embedded_device_example::device::Counters {
            executed: 1000 + self.queries,
            ..Default::default()
        })
    }
}

#[tokio::test]
async fn a_registry_serves_independent_objects() {
    let (transport, io) = connect();
    let mut device = Device::default();
    let mut monitor = Monitor::default();
    let server = async {
        let mut services = Registry::builder()
            .with_service(ServiceDispatch::new(&mut device))
            .with_service(DiagnosticsDispatch::new(&mut monitor))
            .build()
            .unwrap();
        serve(io, &mut services).await;
    };
    let client = async move {
        let mut service = ServiceClient::new(transport);
        service.command(&get_status(1)).await.unwrap();
        let mut diagnostics = DiagnosticsClient::new(service.into_inner());
        let counters = diagnostics.get_counters(&GetCounters {}).await.unwrap();
        // The monitor answered, not the device.
        assert_eq!(counters.executed, 1001);
    };
    with_timeout(async { tokio::join!(server, client) }).await;
    assert_eq!(device.executed, 1);
    assert_eq!(monitor.queries, 1);
}

#[tokio::test]
async fn one_object_serves_two_services_without_cloning_or_locking() {
    let (transport, io) = connect();
    let mut device = Device::default();
    let server = async {
        let mut services = MultiDispatch::builder(&mut device)
            .with_service(ServiceMethods)
            .with_service(DiagnosticsMethods)
            .build()
            .unwrap();
        serve(io, &mut services).await;
    };
    let client = async move {
        let mut service = ServiceClient::new(transport);
        service.command(&set_output(0)).await.unwrap();
        assert_eq!(
            service.command(&set_output(7)).await.unwrap_err().code,
            Code::InvalidArgument
        );
        let mut diagnostics = DiagnosticsClient::new(service.into_inner());
        // The diagnostics service sees what the command service did, because
        // it is the same object.
        let counters = diagnostics.get_counters(&GetCounters {}).await.unwrap();
        assert_eq!(
            (counters.executed, counters.rejected, counters.events),
            (1, 1, 1)
        );
    };
    with_timeout(async { tokio::join!(server, client) }).await;
    assert_eq!((device.executed, device.rejected), (1, 1));
}

#[test]
fn duplicate_registrations_fail_construction() {
    let err = Registry::builder()
        .with_service(ServiceDispatch::new(Device::default()))
        .with_service(DiagnosticsDispatch::new(Monitor::default()))
        .with_service(ServiceDispatch::new(Device::default()))
        .build()
        .unwrap_err();
    assert!(
        err.path
            .starts_with("/protolink.examples.embedded.device.Service/")
    );

    let mut device = Device::default();
    let err: RouteConflict = MultiDispatch::builder(&mut device)
        .with_service(DiagnosticsMethods)
        .with_service(ServiceMethods)
        .with_service(DiagnosticsMethods)
        .build()
        .unwrap_err();
    assert_eq!(
        err.path,
        "/protolink.examples.embedded.device.Diagnostics/GetCounters"
    );
}

#[test]
fn routes_come_from_the_generated_method_table() {
    let services = ServiceDispatch::new(Device::default());
    let route = services.route(METHOD_COMMAND).unwrap();
    assert_eq!(route.kind, protolink::MethodKind::Unary);
    assert!(
        services
            .route("/protolink.examples.embedded.device.Nope/X")
            .is_none()
    );
}

#[tokio::test]
async fn unregistered_services_are_unimplemented() {
    let (transport, io) = connect();
    let server = tokio::spawn(async move {
        let mut services = ServiceDispatch::new(Device::default());
        serve(io, &mut services).await;
    });
    let mut diagnostics = DiagnosticsClient::new(transport);
    let err = with_timeout(diagnostics.get_counters(&GetCounters {}))
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::Unimplemented);
    drop(diagnostics);
    with_timeout(server).await.unwrap();
}

/// Each connection has its own dispatcher, so the first streaming call of
/// both connections has the same stream id without sharing call state.
#[tokio::test]
async fn separate_connections_keep_separate_call_state() {
    let mut servers = Vec::new();
    let mut clients = Vec::new();
    for _ in 0..2 {
        let (transport, io) = connect();
        servers.push(tokio::spawn(async move {
            let mut device = Device::default();
            let mut services = MultiDispatch::builder(&mut device)
                .with_service(ServiceMethods)
                .with_service(DiagnosticsMethods)
                .build()
                .unwrap();
            serve(io, &mut services).await;
            // End the dispatcher's borrow to hand the device back.
            drop(services);
            device
        }));
        clients.push(ServiceClient::new(transport));
    }
    with_timeout(async {
        let mut first = clients[0].command_stream().await.unwrap();
        let mut second = clients[1].command_stream().await.unwrap();
        // Interleave the two calls, which are stream 1 on both connections.
        for i in 0..3 {
            first.send(&get_status(100 + i)).await.unwrap();
            second.send(&get_status(200 + i)).await.unwrap();
        }
        second.send(&get_status(299)).await.unwrap();
        for i in 0..3 {
            let reply = first.message().await.unwrap().unwrap();
            assert_eq!(reply.correlation_id(), Some(&(100 + i)));
            let reply = second.message().await.unwrap().unwrap();
            assert_eq!(reply.correlation_id(), Some(&(200 + i)));
        }
        let reply = second.message().await.unwrap().unwrap();
        assert_eq!(reply.correlation_id(), Some(&299));
        first.close_send().await.unwrap();
        second.close_send().await.unwrap();
        assert!(first.message().await.unwrap().is_none());
        assert!(second.message().await.unwrap().is_none());
    })
    .await;
    drop(clients);
    let mut executed = Vec::new();
    for server in servers {
        let device = with_timeout(server).await.unwrap();
        assert_eq!(device.calls.active(), 0);
        executed.push(device.executed);
    }
    assert_eq!(executed, [3, 4]);
}
