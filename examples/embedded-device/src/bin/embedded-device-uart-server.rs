//! The device service over a Linux serial port (Linux only).
//!
//! ```text
//! cargo run -p embedded-device-example --features uart-linux \
//!   --bin embedded-device-uart-server -- \
//!   --device /dev/ttyUSB0 --baud 115200 --transport reliable
//! ```
//!
//! `--transport` has no default: choose the transport the peer uses (`raw`,
//! `cobs` or `reliable`). The server runs until the link fails, for example
//! when the adapter is unplugged, and then exits with an error. It does not
//! end when a client exits: a serial line has no connection teardown. Stop it
//! with Ctrl-C. To reconnect after a failure, start it again.
//!
//! Without hardware, connect two processes through a virtual serial pair. The
//! pair has no baud rate, so this tests the RPC path and not the timing:
//!
//! ```text
//! socat -d -d pty,raw,echo=0,link=/tmp/ttyA pty,raw,echo=0,link=/tmp/ttyB
//! embedded-device-uart-server --device /tmp/ttyA --transport reliable
//! embedded-device-uart-client --device /tmp/ttyB --transport reliable
//! ```
//!
//! See `embedded_device_example::uart_link` for the wire format of each transport.

#[cfg(target_os = "linux")]
const USAGE: &str = "usage: embedded-device-uart-server --device <path> --transport <raw|cobs|reliable> [--baud <n>]";

#[cfg(target_os = "linux")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    use embedded_device_example::uart_link;
    use embedded_device_example::{Device, DiagnosticsMethods, ServiceMethods};

    let options = match uart_link::parse_options(std::env::args().skip(1)) {
        Ok(options) => options,
        Err(e) => {
            eprintln!("{e}\n{USAGE}");
            return std::process::ExitCode::from(2);
        }
    };
    let uart = match uart_link::open_device(&options) {
        Ok(uart) => uart,
        Err(e) => {
            eprintln!("{e}");
            return std::process::ExitCode::FAILURE;
        }
    };
    println!(
        "serving on {} at {} baud, transport {:?}",
        options.device, options.baud, options.transport
    );

    let mut device = Device::default();
    let mut services = protolink::MultiDispatch::builder(&mut device)
        .with_service(ServiceMethods)
        .with_service(DiagnosticsMethods)
        .build()
        .expect("the services have distinct paths");
    match uart_link::serve(uart, options.transport, &mut services).await {
        Ok(()) => {
            println!("connection closed");
            std::process::ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("connection failed: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn main() -> std::process::ExitCode {
    eprintln!("embedded-device-uart-server uses Linux serial ports and runs on Linux only");
    std::process::ExitCode::FAILURE
}
