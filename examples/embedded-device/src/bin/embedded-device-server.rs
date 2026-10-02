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
//! ```

use embedded_device_example::{Device, ServiceServer};
use protolink::ServerConfig;

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
            let mut handler = ServiceServer(Device::default());
            if let Err(e) =
                protolink::tokio::serve(stream, &mut handler, ServerConfig::default()).await
            {
                eprintln!("{peer}: {e}");
            }
        });
    }
}
