use core::fmt;

#[cfg(any(feature = "async", feature = "blocking"))]
use crate::grpc::Status;
use crate::grpc::http2;

/// Driver error.
#[derive(Debug)]
pub enum Error<E> {
    /// The transport failed.
    Io(E),
    /// A write accepted no bytes. The connection must be closed.
    WriteZero,
    /// A deadline interrupted output; its wire state is indeterminate.
    OutputDeadline,
    /// The peer violated the HTTP/2 protocol; a GOAWAY was sent if possible.
    Protocol(http2::Error),
    /// Serialized output would exceed `max_pending_output_bytes` of the HTTP/2
    /// configuration: the transport is not draining it fast enough, or the
    /// configuration is too small for the traffic. This is a local overload,
    /// not a protocol violation by the peer, and no GOAWAY is written for it.
    OutputLimitExceeded {
        /// The configured limit, in bytes.
        limit: usize,
        /// Bytes already waiting for the transport.
        pending: usize,
        /// Bytes of the refused frame, or of the whole handshake.
        attempted: usize,
    },
}

#[cfg(any(feature = "async", feature = "blocking"))]
impl<E> Error<E> {
    /// The driver error for a fatal error of the HTTP/2 connection.
    pub(crate) fn from_connection(error: http2::Error) -> Self {
        match error {
            http2::Error::OutputLimitExceeded {
                limit,
                pending,
                attempted,
            } => Self::OutputLimitExceeded {
                limit,
                pending,
                attempted,
            },
            other => Self::Protocol(other),
        }
    }
}

impl<E: fmt::Debug> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "transport error: {e:?}"),
            Self::WriteZero => f.write_str("transport write accepted no bytes"),
            Self::OutputDeadline => f.write_str("deadline interrupted transport output"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::OutputLimitExceeded { limit, .. } => {
                write!(f, "HTTP/2 output limit of {limit} bytes exceeded")
            }
        }
    }
}

impl<E: fmt::Debug> core::error::Error for Error<E> {}

/// The terminal status of a client whose connection failed: an overload is
/// reported as such, a peer's protocol violation as a protocol error.
#[cfg(any(feature = "async", feature = "blocking"))]
pub(crate) fn client_failure_status(error: &http2::Error) -> Status {
    match error {
        crate::grpc::http2::Error::OutputLimitExceeded { .. } => {
            crate::grpc::lifecycle::failure_status(error)
        }
        _ => Status::unavailable("connection protocol error"),
    }
}
