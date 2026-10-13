//! A client of the device service over a Linux serial port (Linux only).
//!
//! ```text
//! cargo run -p embedded-device-example --features uart-linux \
//!   --bin embedded-device-uart-client -- \
//!   --device /dev/ttyUSB0 --baud 115200 --transport reliable --deadline-ms 2000
//! ```
//!
//! The client makes one call of each shape: `GetStatus` and `SetOutput`
//! (unary), then `EventSubscribe` (server streaming). Every call has the
//! `--deadline-ms` timeout (1000 by default), which the server enforces too.
//! A failed call ends the run with an error, and the client does not reconnect.
//!
//! With `--transport reliable`, a successful run then lingers on the link for
//! up to five seconds, so that a lost final acknowledgement can still be
//! recovered. The run takes that long unless the link ends first. See
//! `uart_link::call`.
//!
//! See `embedded-device-uart-server` for the virtual serial pair and the wire
//! format of each transport.

#[cfg(target_os = "linux")]
const USAGE: &str = "usage: embedded-device-uart-client --device <path> --transport <raw|cobs|reliable> [--baud <n>] [--deadline-ms <n>]";

#[cfg(target_os = "linux")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    use embedded_device_example::uart_link;

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

    match uart_link::call(uart, options.transport, options.deadline).await {
        Ok(session) => {
            println!(
                "uptime_ms={} output0={} events={:?}",
                session.uptime_ms, session.output_enabled, session.events
            );
            std::process::ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("call failed: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn main() -> std::process::ExitCode {
    eprintln!("embedded-device-uart-client uses Linux serial ports and runs on Linux only");
    std::process::ExitCode::FAILURE
}
