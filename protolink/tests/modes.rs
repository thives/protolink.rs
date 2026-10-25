//! Compile and construction checks of the transport modes: the factories, the
//! extraction of the transports, and the router assemblies of each mode.
#![cfg(all(feature = "async", feature = "std"))]

use core::convert::Infallible;

use embedded_io_async::{ErrorType, Read, Write};
use protolink::grpc::{Client as GrpcClient, ClientConfig, MethodDescriptor, MethodKind, Router};
use protolink::{CoordinatedReliable, Duplex, Mode, NoTimer, Serial};

/// A transport that never transfers anything: construction must not touch it.
#[derive(Debug)]
struct Silent;

impl ErrorType for Silent {
    type Error = Infallible;
}

impl Read for Silent {
    async fn read(&mut self, _: &mut [u8]) -> Result<usize, Infallible> {
        core::future::pending().await
    }
}

impl Write for Silent {
    async fn write(&mut self, _: &[u8]) -> Result<usize, Infallible> {
        core::future::pending().await
    }

    async fn flush(&mut self) -> Result<(), Infallible> {
        core::future::pending().await
    }
}

const METHODS: &[MethodDescriptor] = &[MethodDescriptor::new(
    "/test.Relay/Unary",
    MethodKind::Unary,
    0,
)];

fn assert_mode<M: Mode>() {}

#[test]
fn factories_build_each_mode_without_io() {
    assert_mode::<Serial<Silent>>();
    assert_mode::<Duplex<Silent, Silent>>();
    assert_mode::<CoordinatedReliable<Silent, protolink::link::StdTimer>>();

    let serial = protolink::client(Silent).without_deadlines().build();
    let (reader, writer) = (Silent, Silent);
    let split = protolink::client_split(reader, writer)
        .without_deadlines()
        .build();
    let link = protolink::link::reliable(Silent).build();
    let reliable = protolink::client_reliable(link).without_deadlines().build();
    assert!(!serial.with_inner(|c| c.is_closed()));
    assert!(!split.with_inner(|c| c.is_closed()));
    assert!(!reliable.with_inner(|c| c.is_closed()));
}

#[test]
fn extraction_returns_the_transports_of_each_mode() {
    let io: Silent = protolink::client(Silent)
        .without_deadlines()
        .build()
        .into_io();
    let _: Silent = io;

    let (_reader, _writer): (Silent, Silent) = protolink::client_split(Silent, Silent)
        .without_deadlines()
        .build()
        .into_parts();

    let link = protolink::client_reliable(protolink::link::reliable(Silent).build())
        .without_deadlines()
        .build()
        .into_link();
    let _: protolink::link::StdReliableLink<Silent> = link;
}

#[test]
fn duplex_and_coordinated_factories_pair_their_parts() {
    let _ = Duplex::new(Silent, Silent).into_parts();
    let link = protolink::link::reliable(Silent).build();
    let _ = CoordinatedReliable::new(link).into_link();
    let _ = Serial::new(Silent).into_inner();
    let server = protolink::server_split(Silent, Silent)
        .without_deadlines()
        .build();
    let _ = server;
    let server = protolink::server_reliable(protolink::link::reliable(Silent).build())
        .without_deadlines()
        .build();
    let _ = server;
}

/// Compile check of every router assembly. It is never run: the types of each
/// assembly and of its transports must agree. The router's backends are
/// flattened in registration order, then pool member order.
#[allow(dead_code)]
async fn router_assemblies(
    router: &mut Router,
    serial: &mut [Silent],
    split: &mut [Duplex<Silent, Silent>],
    reliable: &mut [CoordinatedReliable<Silent, protolink::link::StdTimer>],
) {
    let _ = protolink::server(Silent)
        .without_deadlines()
        .build()
        .serve_router(router, serial)
        .await;
    let _ = protolink::server_split(Silent, Silent)
        .without_deadlines()
        .build()
        .serve_router_split(router, split)
        .await;
    let _ = protolink::server_reliable(protolink::link::reliable(Silent).build())
        .without_deadlines()
        .build()
        .serve_router_reliable(router, reliable)
        .await;
}

#[test]
fn a_pool_of_two_flattens_to_two_backends() {
    let router = Router::builder()
        .with_pool(
            METHODS,
            vec![
                GrpcClient::new(ClientConfig::default()),
                GrpcClient::new(ClientConfig::default()),
            ],
            usize::MAX,
        )
        .build()
        .unwrap();
    assert_eq!(router.backend_count(), 2);
    let _ = router_assemblies;
}

#[cfg(feature = "tokio")]
mod tokio_factories {
    use super::*;

    /// The tokio factories name their modes: duplex by default, serial on request.
    #[allow(dead_code)]
    fn modes<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(io: T, io2: T) {
        let _: protolink::ClientBuilder<
            protolink::tokio::TokioDuplex<T>,
            protolink::tokio::TokioTimer,
        > = protolink::tokio::client(io);
        let _: protolink::ServerBuilder<
            Serial<protolink::tokio::FromTokio<T>>,
            protolink::tokio::TokioTimer,
        > = protolink::tokio::server_serial(io2);
    }
}

#[test]
fn no_deadline_policy_is_chosen_by_default() {
    // A factory without a policy cannot build; these name the policy explicitly.
    let _: protolink::ServerBuilder<Serial<Silent>, NoTimer> =
        protolink::server(Silent).without_deadlines();
}
