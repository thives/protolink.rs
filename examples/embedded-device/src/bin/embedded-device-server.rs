//! TCP server for manual interop testing, e.g. with grpcurl:
//!
//! ```text
//! cargo run -p embedded-device-example --bin embedded-device-server
//! grpcurl -plaintext -import-path examples/embedded-device/proto -proto device.proto \
//!   -d '{"correlation_id": 7, "get_status": {}}' \
//!   127.0.0.1:50051 protolink.examples.embedded.device.Service/Command
//!
//! # Bidirectional streaming: one reply per command.
//! grpcurl -plaintext -import-path examples/embedded-device/proto -proto device.proto \
//!   -d '{"correlation_id": 1, "get_status": {}} {"restart": {}}' \
//!   127.0.0.1:50051 protolink.examples.embedded.device.Service/CommandStream
//!
//! # Server streaming: replay the event log.
//! grpcurl -plaintext -import-path examples/embedded-device/proto -proto device.proto \
//!   -d '{}' 127.0.0.1:50051 protolink.examples.embedded.device.Service/EventSubscribe
//!
//! # The second service of the same device.
//! grpcurl -plaintext -import-path examples/embedded-device/proto -proto device.proto \
//!   -d '{}' 127.0.0.1:50051 protolink.examples.embedded.device.Diagnostics/GetCounters
//! ```
//!
//! Each connection gets its own [`Device`], which serves both generated
//! services through one [`MultiDispatch`](protolink::MultiDispatch). The
//! driver is built per connection and only meets the device in `serve`.

use embedded_device_example::{Device, DiagnosticsMethods, ServiceMethods};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:50051".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("listening on {addr}");
    loop {
        let (stream, peer) = listener.accept().await?;
        stream.set_nodelay(true)?;
        tokio::spawn(async move {
            let mut device = Device::default();
            let mut services = protolink::MultiDispatch::builder(&mut device)
                .with_service(ServiceMethods)
                .with_service(DiagnosticsMethods)
                .build()
                .expect("the services have distinct paths");
            let server = protolink::tokio::server(stream).build();
            if let Err(e) = server.serve(&mut services).await {
                eprintln!("{peer}: {e}");
            }
        });
    }
}
