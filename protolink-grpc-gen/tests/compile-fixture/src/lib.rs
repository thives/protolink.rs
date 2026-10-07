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

#[allow(non_snake_case, non_camel_case_types, unused, unused_parens)]
pub mod names {
    include!(concat!(env!("OUT_DIR"), "/names_messages.rs"));
    include!(concat!(env!("OUT_DIR"), "/names_grpc.rs"));
}

#[allow(non_snake_case, non_camel_case_types, unused, unused_parens)]
pub mod collide {
    include!(concat!(env!("OUT_DIR"), "/collide_messages.rs"));
    include!(concat!(env!("OUT_DIR"), "/collide_grpc.rs"));
}

#[allow(non_snake_case, non_camel_case_types, unused, unused_parens)]
pub mod relative {
    pub mod crate_messages {
        include!(concat!(env!("OUT_DIR"), "/relative_messages.rs"));
    }
    include!(concat!(env!("OUT_DIR"), "/relative_grpc.rs"));
}

#[cfg(feature = "unsuffixed-packages")]
use names::r#names::Msg as NamesMsg;
#[cfg(not(feature = "unsuffixed-packages"))]
use names::names_::Msg as NamesMsg;

// Every service gets a usable server, including `S`, which is also the
// generic parameter of the generated wrapper.
#[cfg(feature = "server")]
pub fn name_servers() {
    macro_rules! implement {
        ($($module:ident::$service:ident => $server:ident),*) => {$(
            impl names::$module::$service for Impl {
                fn unary(
                    &mut self,
                    _: &mut CallContext<'_>,
                    request: NamesMsg,
                ) -> Result<NamesMsg, Status> {
                    Ok(request)
                }
            }
            handler(names::$module::$server::new(Impl));
        )*};
    }
    struct Impl;
    fn handler(_: impl Handler) {}
    implement!(
        s::S => SServer,
        status::Status => StatusServer,
        context::Context => ContextServer,
        poll::Poll => PollServer,
        vec::Vec => VecServer
    );
}

#[cfg(feature = "async-client")]
pub async fn name_async_calls<T>(transport: T, request: &NamesMsg) -> Result<(), Status>
where
    T: protolink_grpc::UnaryTransport + protolink_grpc::StreamingTransport,
{
    let mut client = names::status::StatusClient::new(transport);
    let _: NamesMsg = client.unary(request).await?;
    drop(client.stream().await?);
    drop(client.fan(request).await?);
    let mut client = names::vec::VecClient::new(client.into_inner());
    let _: NamesMsg = client.unary(request).await?;
    Ok(())
}

#[cfg(feature = "blocking-client")]
pub fn name_blocking_calls<T>(transport: T, request: &NamesMsg) -> Result<(), Status>
where
    T: protolink_grpc::BlockingUnaryTransport + protolink_grpc::BlockingStreamingTransport,
{
    let mut client = names::poll::PollBlockingClient::new(transport);
    let _: NamesMsg = client.unary(request)?;
    drop(client.stream()?);
    drop(client.fan(request)?);
    let mut client = names::context::ContextBlockingClient::new(client.into_inner());
    let _: NamesMsg = client.unary(request)?;
    Ok(())
}

// Renamed service modules are used under their documented fallback names.
#[cfg(all(feature = "server", not(feature = "unsuffixed-packages")))]
pub fn collision_servers() {
    use collide::foo_::Msg;
    struct Impl;
    impl collide::foo__grpc::Foo_ for Impl {
        fn call(&mut self, _: &mut CallContext<'_>, request: Msg) -> Result<Msg, Status> {
            Ok(request)
        }
    }
    impl collide::foo::Foo for Impl {
        fn call(&mut self, _: &mut CallContext<'_>, request: Msg) -> Result<Msg, Status> {
            Ok(request)
        }
    }
    fn handler(_: impl Handler) {}
    handler(collide::foo__grpc::Foo_Server::new(Impl));
    handler(collide::foo::FooServer::new(Impl));
}

#[cfg(all(feature = "server", feature = "unsuffixed-packages"))]
pub fn collision_servers() {
    use collide::r#foo::Msg;
    struct Impl;
    impl collide::foo_grpc::Foo for Impl {
        fn call(&mut self, _: &mut CallContext<'_>, request: Msg) -> Result<Msg, Status> {
            Ok(request)
        }
    }
    impl collide::foo_::Foo_ for Impl {
        fn call(&mut self, _: &mut CallContext<'_>, request: Msg) -> Result<Msg, Status> {
            Ok(request)
        }
    }
    fn handler(_: impl Handler) {}
    handler(collide::foo_grpc::FooServer::new(Impl));
    handler(collide::foo_::Foo_Server::new(Impl));
}

// Relative paths beginning with `crate` resolve from the inclusion point.
#[cfg(all(feature = "server", feature = "unsuffixed-packages"))]
use relative::crate_messages::r#foo::Msg as RelativeMsg;
#[cfg(all(feature = "server", not(feature = "unsuffixed-packages")))]
use relative::crate_messages::foo_::Msg as RelativeMsg;

#[cfg(feature = "server")]
pub fn relative_server() -> impl Handler {
    struct Impl;
    impl relative::foo::Foo for Impl {
        fn call(
            &mut self,
            _: &mut CallContext<'_>,
            request: RelativeMsg,
        ) -> Result<RelativeMsg, Status> {
            Ok(request)
        }
    }
    relative::foo::FooServer::new(Impl)
}
