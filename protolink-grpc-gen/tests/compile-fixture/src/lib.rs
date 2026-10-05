#![no_std]

// Message generator naming is intentionally unchanged; service names use Rust
// identifiers independently of micropb's message/package naming conventions.
#[allow(non_snake_case, non_camel_case_types, unused, unused_parens)]
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/messages.rs"));
    include!(concat!(env!("OUT_DIR"), "/grpc.rs"));
}

#[allow(non_snake_case, non_camel_case_types, unused, unused_parens)]
pub mod lifecycle {
    include!(concat!(env!("OUT_DIR"), "/lifecycle_messages.rs"));
    include!(concat!(env!("OUT_DIR"), "/lifecycle_grpc.rs"));
}

#[cfg(feature = "unsuffixed-packages")]
use proto::r#fixture::r#type::{Reply, Request_::Inner as Request};
#[cfg(not(feature = "unsuffixed-packages"))]
use proto::fixture_::type_::{Reply, Request_::Inner as Request};

use protolink_grpc::{CallContext, CallOptions, Handler, Status};

#[cfg(feature = "server")]
pub struct Service;

#[cfg(feature = "server")]
impl proto::r#type::Type for Service {
    fn r#type(&mut self, _: &mut CallContext<'_>, request: Request) -> Result<Reply, Status> {
        Ok(Reply {
            value: request.value,
        })
    }
}

#[cfg(feature = "server")]
pub fn server() -> impl Handler {
    proto::r#type::TypeServer::new(Service)
}

// Referencing these identifiers makes the consumer verify escaping, not just
// whether a generated string happens to contain a raw-identifier prefix.
#[cfg(feature = "server")]
pub fn keyword_servers() {
    struct Empty;
    impl proto::r#match::r#match for Empty {}
    impl proto::self_::Self_ for Empty {}
    impl proto::crate_::crate_ for Empty {}
    impl proto::__::__ for Empty {}
    impl proto::extern_::extern_ for Empty {}
    impl proto::r#async::r#async for Empty {}
    impl proto::r#gen::r#gen for Empty {}
    fn handler(_: impl Handler) {}
    handler(proto::r#match::r#matchServer::new(Empty));
    handler(proto::self_::Self_Server::new(Empty));
    handler(proto::crate_::crate_Server::new(Empty));
    handler(proto::__::__Server::new(Empty));
    handler(proto::extern_::extern_Server::new(Empty));
    handler(proto::r#async::r#asyncServer::new(Empty));
    handler(proto::r#gen::r#genServer::new(Empty));
}

pub const QUALIFIED_SERVICE_NAMES: [&str; 2] = [
    proto::alpha_shared::SERVICE_NAME,
    proto::beta_shared::SERVICE_NAME,
];

#[cfg(feature = "async-client")]
pub async fn async_calls<T>(transport: T, request: &Request) -> Result<(), Status>
where
    T: protolink_grpc::UnaryTransport + protolink_grpc::StreamingTransport,
{
    let mut client = proto::r#type::TypeClient::new(transport);
    let _: &mut T = client.transport_mut();
    let _: Reply = client.r#type(request).await?;
    let _: protolink_grpc::Response<Reply> = client
        .type_with_options(request, CallOptions::default())
        .await?;
    drop(client.r#match(request).await?);
    drop(
        client
            .match_with_options(request, CallOptions::default())
            .await?,
    );
    drop(client.r#async().await?);
    drop(client.async_with_options(CallOptions::default()).await?);
    drop(client.r#gen().await?);
    drop(client.gen_with_options(CallOptions::default()).await?);
    let _: T = client.into_inner();
    Ok(())
}

#[cfg(feature = "blocking-client")]
pub fn blocking_calls<T>(transport: T, request: &Request) -> Result<(), Status>
where
    T: protolink_grpc::BlockingUnaryTransport + protolink_grpc::BlockingStreamingTransport,
{
    let mut client = proto::r#type::TypeBlockingClient::new(transport);
    let _: &mut T = client.transport_mut();
    let _: Reply = client.r#type(request)?;
    let _: protolink_grpc::Response<Reply> =
        client.type_with_options(request, CallOptions::default())?;
    drop(client.r#match(request)?);
    drop(client.match_with_options(request, CallOptions::default())?);
    drop(client.r#async()?);
    drop(client.async_with_options(CallOptions::default())?);
    drop(client.r#gen()?);
    drop(client.gen_with_options(CallOptions::default())?);
    let _: T = client.into_inner();
    Ok(())
}

#[cfg(feature = "unsuffixed-packages")]
use lifecycle::r#builtins::Message;
#[cfg(not(feature = "unsuffixed-packages"))]
use lifecycle::builtins_::Message;

pub struct Lifecycle;
impl lifecycle::lifecycle::Lifecycle for Lifecycle {
    fn new(&mut self, _: &mut CallContext<'_>, request: Message) -> Result<Message, Status> {
        Ok(request)
    }
}

pub fn lifecycle_server() -> impl Handler {
    lifecycle::lifecycle::LifecycleServer::new(Lifecycle)
}
