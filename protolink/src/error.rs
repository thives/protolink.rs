use core::fmt;

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
}

impl<E: fmt::Debug> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "transport error: {e:?}"),
            Self::WriteZero => f.write_str("transport write accepted no bytes"),
            Self::OutputDeadline => f.write_str("deadline interrupted transport output"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
        }
    }
}

impl<E: fmt::Debug> core::error::Error for Error<E> {}
