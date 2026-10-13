//! The device example over a Linux serial port (feature `uart-linux`).
//!
//! The same generated services run over a serial port, with a [`Transport`]
//! that both ends choose explicitly:
//!
//! | Transport | Stack (bottom to top) | Use it when |
//! |---|---|---|
//! | `raw` | HTTP/2 bytes as they are | the line never drops or corrupts bytes (a loopback, or a test) |
//! | `cobs` | COBS frames, no checksum, no retransmission | frame boundaries must survive, and the line must not corrupt bytes: a corrupted message body is delivered unnoticed |
//! | `reliable` | COBS frames, CRC-checked, with ARQ | the line can lose or corrupt frames and the RPC must survive it |
//!
//! Both ends must use the same transport, the same baud rate, and 8N1 with no
//! flow control. Nothing reconnects: a failed connection is returned as an
//! error, and the binary exits. Start it again to reconnect.
//!
//! The wire format, for an embedded peer:
//!
//! - `raw`: HTTP/2 frames directly on the line, with no delimiter.
//! - `cobs`: each write of up to 256 bytes (the decoded payload maximum) is one
//!   COBS frame (`cobs-io-async` 0.0.4), delimited by a zero byte. The payload
//!   is the HTTP/2 byte stream.
//! - `reliable`: each frame is an ARQ DAT, ACK or FIN (`arq-io-async` 0.0.3),
//!   COBS-encoded and zero-delimited; see `docs/LINK.md`. A DAT carries up to
//!   251 application bytes after its five-byte header. On Linux the ARQ timer
//!   is `StdTimer`. An embedded peer supplies its own `link::Timer`.
//!
//! A successful `reliable` client run then lingers on the link for up to
//! [`RELIABLE_LINGER`] (see [`call`]). That adds up to five seconds to the run,
//! unless the link ends first; `raw` and `cobs` runs add nothing.

use std::fmt::Debug;
use std::time::Duration;

use tokio::time::{Instant, timeout_at};

use embedded_io_async::{Read, Write};
use protolink::Handler;
use protolink::link::{ArqError, CobsFramed, ReliableError};
use protolink::tokio::TokioTimer;
use protolink_adaptors::uart::linux::LinuxUart;

use crate::ServiceClient;
use crate::device::{Command, Command_, EventSubscribe, GetStatus, Reply_, SetOutput};

/// The link stack, chosen explicitly on both ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// HTTP/2 bytes as they are.
    Raw,
    /// COBS frames without retransmission.
    Cobs,
    /// COBS frames with ARQ retransmission.
    Reliable,
}

impl Transport {
    /// The transport named on the command line: `raw`, `cobs` or `reliable`.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "raw" => Some(Self::Raw),
            "cobs" => Some(Self::Cobs),
            "reliable" => Some(Self::Reliable),
            _ => None,
        }
    }
}

/// Command-line options of the two binaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    /// Serial device path, for example `/dev/ttyUSB0`.
    pub device: String,
    /// Baud rate; 115200 unless `--baud` is given.
    pub baud: u32,
    /// The link stack, which has no default.
    pub transport: Transport,
    /// Timeout of each client call (`--deadline-ms`, 1000 unless given). The
    /// server enforces the timeout the client sends, so it ignores this value.
    pub deadline: Duration,
}

/// Parse `--device <path> --transport <raw|cobs|reliable> [--baud <n>]
/// [--deadline-ms <n>]`.
pub fn parse_options(args: impl IntoIterator<Item = String>) -> Result<Options, String> {
    let mut device = None;
    let mut baud = 115_200;
    let mut transport = None;
    let mut deadline = Duration::from_millis(1000);

    let mut args = args.into_iter();
    while let Some(flag) = args.next() {
        // The value is taken only by a flag that takes one.
        let mut value = || args.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--device" => device = Some(value()?),
            "--baud" => {
                let value = value()?;
                baud = value
                    .parse()
                    .map_err(|_| format!("--baud must be a number, not `{value}`"))?;
            }
            "--transport" => {
                let value = value()?;
                transport = Some(
                    Transport::parse(&value)
                        .ok_or_else(|| format!("unknown transport `{value}`"))?,
                );
            }
            "--deadline-ms" => {
                let value = value()?;
                let ms: u64 = value
                    .parse()
                    .map_err(|_| format!("--deadline-ms must be a number, not `{value}`"))?;
                if ms == 0 {
                    return Err("--deadline-ms must be at least 1".into());
                }
                deadline = Duration::from_millis(ms);
            }
            other => return Err(format!("unknown option `{other}`")),
        }
    }

    Ok(Options {
        device: device.ok_or("--device is required")?,
        baud,
        transport: transport.ok_or("--transport is required: raw, cobs or reliable")?,
        deadline,
    })
}

/// Open the serial device named by `options`. The error names the device, the
/// baud rate and the cause, so both binaries report a failure the same way.
pub fn open_device(options: &Options) -> Result<LinuxUart, String> {
    LinuxUart::open(&options.device, options.baud).map_err(|e| {
        format!(
            "cannot open {} at {} baud: {e}",
            options.device, options.baud
        )
    })
}

/// Serve `services` over `uart` until the connection ends.
///
/// Deadlines are enforced with a [`TokioTimer`]: a call whose `grpc-timeout`
/// runs out is ended by the server.
pub async fn serve<H: Handler + ?Sized>(
    uart: LinuxUart,
    transport: Transport,
    services: &mut H,
) -> Result<(), String> {
    match transport {
        Transport::Raw => serve_stream(protolink::tokio::compat(uart), services).await,
        Transport::Cobs => {
            serve_stream(CobsFramed::new(protolink::tokio::compat(uart)), services).await
        }
        Transport::Reliable => {
            let link = protolink::link::reliable(protolink::tokio::compat(uart)).build();
            serve_stream(link, services).await
        }
    }
}

async fn serve_stream<IO, H>(io: IO, services: &mut H) -> Result<(), String>
where
    IO: Read + Write,
    IO::Error: Debug,
    H: Handler + ?Sized,
{
    protolink::server(io)
        .with_deadline_timer(TokioTimer::new())
        .build()
        .serve(services)
        .await
        .map_err(|e| e.to_string())
}

/// What a client session observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    /// Device uptime from `GetStatus`, in milliseconds.
    pub uptime_ms: u64,
    /// State of output 0 after `SetOutput`.
    pub output_enabled: bool,
    /// Event codes that `EventSubscribe` replayed.
    pub events: Vec<u32>,
}

/// How long a successful reliable session keeps servicing the link after its
/// last RPC. A timer budget for this example, not a protocol constant.
pub const RELIABLE_LINGER: Duration = Duration::from_secs(5);

/// Run a short script over `uart`: `GetStatus` and `SetOutput` (unary), then
/// `EventSubscribe` (server streaming). Every call has `deadline`.
///
/// The session ends with the call. The connection is not reopened on failure.
///
/// On the `reliable` transport a successful session then lingers for up to
/// [`RELIABLE_LINGER`]: the client keeps reading the link so that it can
/// acknowledge a retransmission of the server's final frames, if the
/// acknowledgement of them was lost. This is a bounded recovery opportunity.
/// It is not a coordinated close, and it does not show that the final
/// acknowledgement arrived. The linger is separate from the RPC deadlines. An
/// error from the linger is returned after the RPCs have succeeded: it does not
/// undo their effects, so a `SetOutput` that completed is not safe to retry on
/// its account.
pub async fn call(
    uart: LinuxUart,
    transport: Transport,
    deadline: Duration,
) -> Result<Session, String> {
    call_with_linger(uart, transport, deadline, RELIABLE_LINGER).await
}

/// [`call`] with the reliable transport's linger budget chosen by the caller.
/// A zero `grace` returns as soon as the RPCs are done. The raw and COBS
/// transports never linger.
pub async fn call_with_linger(
    uart: LinuxUart,
    transport: Transport,
    deadline: Duration,
    grace: Duration,
) -> Result<Session, String> {
    match transport {
        Transport::Raw => {
            let (session, _io) = call_stream(protolink::tokio::compat(uart), deadline).await?;
            Ok(session)
        }
        Transport::Cobs => {
            let (session, _io) =
                call_stream(CobsFramed::new(protolink::tokio::compat(uart)), deadline).await?;
            Ok(session)
        }
        Transport::Reliable => {
            let link = protolink::link::reliable(protolink::tokio::compat(uart)).build();
            let (session, mut link) = call_stream(link, deadline).await?;
            linger(&mut link, grace).await?;
            Ok(session)
        }
    }
}

/// Keep reading `io` until `grace` has passed, discarding what arrives.
///
/// The server's last frames may need a retransmission if the client's
/// acknowledgement of them was lost, and ARQ answers a retransmission only
/// while somebody reads the link. This is for the one-shot example, where the
/// session's calls have all completed and the discarded bytes are of no use. It
/// is not a way to discard input of a client that can still have calls.
///
/// One absolute deadline is taken when the linger begins, and traffic never
/// extends it. It ends early on end of stream or on [`ArqError::Closed`]; neither
/// shows that the final acknowledgement arrived. Any other error is returned.
async fn linger<IO, E>(io: &mut IO, grace: Duration) -> Result<(), String>
where
    IO: Read<Error = ReliableError<E>>,
    E: Debug,
{
    let deadline = Instant::now() + grace;
    let mut scratch = [0u8; 64];
    while Instant::now() < deadline {
        match timeout_at(deadline, io.read(&mut scratch)).await {
            Err(_elapsed) => break,
            Ok(Ok(0)) => break,
            // Let other tasks run if input keeps arriving.
            Ok(Ok(_)) => tokio::task::yield_now().await,
            Ok(Err(ReliableError(ArqError::Closed))) => break,
            Ok(Err(e)) => return Err(format!("RPCs completed; reliable linger failed: {e}")),
        }
    }
    Ok(())
}

async fn call_stream<IO>(io: IO, deadline: Duration) -> Result<(Session, IO), String>
where
    IO: Read + Write,
    IO::Error: Debug,
{
    let transport = protolink::client(io)
        .with_deadline_timer(TokioTimer::new())
        .with_default_timeout(deadline)
        .build();
    let mut client = ServiceClient::new(transport);

    let reply = client
        .command(&command(Command_::Command::GetStatus(GetStatus {})))
        .await
        .map_err(|e| e.to_string())?;
    let Some(Reply_::Reply::Status(status)) = reply.reply else {
        return Err("GetStatus did not return a status".into());
    };

    let reply = client
        .command(&command(Command_::Command::SetOutput(SetOutput {
            channel: 0,
            enabled: true,
        })))
        .await
        .map_err(|e| e.to_string())?;
    let output_enabled = matches!(reply.reply, Some(Reply_::Reply::OutputState(o)) if o.enabled);

    let mut stream = client
        .event_subscribe(&EventSubscribe {})
        .await
        .map_err(|e| e.to_string())?;
    let mut events = Vec::new();
    while let Some(event) = stream.message().await.map_err(|e| e.to_string())? {
        events.push(event.code);
    }
    drop(stream);

    let session = Session {
        uptime_ms: status.uptime_ms,
        output_enabled,
        events,
    };
    // No call remains, so the exclusive client hands back the very transport it
    // used, with its protocol state: a reliable link needs that state to answer
    // a retransmitted response.
    Ok((session, client.into_inner().into_io()))
}

fn command(command: Command_::Command) -> Command {
    Command {
        command: Some(command),
        ..Command::default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    use embedded_io_async::{ErrorKind, ErrorType};
    use protolink::link::LinkError;
    use tokio::task::JoinHandle;

    use super::*;

    const GRACE: Duration = Duration::from_secs(5);
    type Error = ReliableError<ErrorKind>;

    /// A reader whose behaviour the test controls.
    #[derive(Clone, Copy)]
    enum Script {
        /// Never has data and never ends.
        Idle,
        /// Always has one byte.
        Endless,
        /// Ends the stream at once.
        Eof,
        /// Fails at once.
        Fail(Error),
    }

    struct Reader {
        script: Script,
        reads: Arc<AtomicUsize>,
    }

    impl Reader {
        fn new(script: Script) -> (Self, Arc<AtomicUsize>) {
            let reads = Arc::new(AtomicUsize::new(0));
            let reader = Self {
                script,
                reads: reads.clone(),
            };
            (reader, reads)
        }
    }

    impl ErrorType for Reader {
        type Error = Error;
    }

    impl Read for Reader {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            assert!(!buf.is_empty(), "linger reads with a nonempty buffer");
            self.reads.fetch_add(1, SeqCst);
            match self.script {
                Script::Idle => std::future::pending().await,
                Script::Endless => {
                    buf[0] = 0xAA;
                    Ok(1)
                }
                Script::Eof => Ok(0),
                Script::Fail(e) => Err(e),
            }
        }
    }

    /// Let every runnable task run.
    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    fn spawn_linger(script: Script) -> (JoinHandle<Result<(), String>>, Arc<AtomicUsize>) {
        let (mut reader, reads) = Reader::new(script);
        let task = tokio::spawn(async move { linger(&mut reader, GRACE).await });
        (task, reads)
    }

    async fn run_to_end(script: Script) -> Result<(), String> {
        let started = Instant::now();
        let (mut reader, _) = Reader::new(script);
        let result = linger(&mut reader, GRACE).await;
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the linger ended without waiting for the grace deadline"
        );
        result
    }

    #[tokio::test(start_paused = true)]
    async fn idle_reader_expires_at_the_grace_deadline() {
        let (task, reads) = spawn_linger(Script::Idle);
        settle().await;
        assert_eq!(reads.load(SeqCst), 1, "the linger has started reading");

        tokio::time::advance(GRACE - Duration::from_millis(1)).await;
        settle().await;
        assert!(!task.is_finished(), "still within the grace budget");

        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert!(task.is_finished(), "the deadline ended the linger");
        task.await.unwrap().expect("expiry is success");
    }

    #[tokio::test(start_paused = true)]
    async fn continuous_input_does_not_extend_the_deadline() {
        let (task, reads) = spawn_linger(Script::Endless);
        settle().await;
        assert!(reads.load(SeqCst) > 0, "the linger has started reading");

        // The driver advances the clock; the reader never runs dry.
        tokio::time::advance(GRACE - Duration::from_millis(1)).await;
        settle().await;
        assert!(!task.is_finished(), "still within the grace budget");
        let before = reads.load(SeqCst);
        settle().await;
        assert!(reads.load(SeqCst) > before, "input keeps arriving");

        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert!(task.is_finished(), "input did not move the deadline");
        task.await.unwrap().expect("expiry is success");
    }

    #[tokio::test(start_paused = true)]
    async fn end_of_stream_ends_the_grace_early() {
        run_to_end(Script::Eof).await.expect("EOF is success");
    }

    #[tokio::test(start_paused = true)]
    async fn closed_link_ends_the_grace_early() {
        run_to_end(Script::Fail(ReliableError(ArqError::Closed)))
            .await
            .expect("Closed is success");
    }

    #[tokio::test(start_paused = true)]
    async fn underlying_broken_pipe_is_a_failure_not_a_closure() {
        let error = ReliableError(ArqError::Io(LinkError::Io(ErrorKind::BrokenPipe)));
        let message = run_to_end(Script::Fail(error)).await.unwrap_err();
        assert_eq!(
            message,
            format!("RPCs completed; reliable linger failed: {error}")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn other_failures_keep_their_error_in_the_message() {
        for error in [
            ReliableError(ArqError::Timeout),
            ReliableError(ArqError::Io(LinkError::Io(ErrorKind::InvalidData))),
            ReliableError(ArqError::WriteLength),
        ] {
            let message = run_to_end(Script::Fail(error)).await.unwrap_err();
            assert_eq!(
                message,
                format!("RPCs completed; reliable linger failed: {error}")
            );
        }
    }

    #[test]
    fn open_device_error_names_the_device_baud_and_cause() {
        let options = Options {
            device: "/nonexistent/protolink-uart".into(),
            baud: 9600,
            transport: Transport::Raw,
            deadline: Duration::from_millis(1000),
        };
        let cause = LinuxUart::open(&options.device, options.baud)
            .expect_err("the device does not exist")
            .to_string();

        let error = open_device(&options).expect_err("the device does not exist");

        assert_eq!(
            error,
            format!("cannot open /nonexistent/protolink-uart at 9600 baud: {cause}")
        );
    }
}
