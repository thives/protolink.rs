//! gRPC status codes and the `grpc-status` / `grpc-message` mapping.

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;

use protolink_http2::ErrorCode;

/// gRPC status code (`grpc-status` trailer value).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[allow(missing_docs)]
pub enum Code {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl Code {
    /// Numeric value.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse a numeric code. Unknown values map to [`Code::Unknown`].
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Ok,
            1 => Self::Cancelled,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => Self::Unknown,
        }
    }

    /// Code for a non-200 HTTP status, per the gRPC HTTP-to-gRPC mapping.
    pub const fn from_http_status(status: u16) -> Self {
        match status {
            400 => Self::Internal,
            401 => Self::Unauthenticated,
            403 => Self::PermissionDenied,
            404 => Self::Unimplemented,
            429 | 502 | 503 | 504 => Self::Unavailable,
            _ => Self::Unknown,
        }
    }

    /// Code for an HTTP/2 RST_STREAM / GOAWAY error code.
    pub const fn from_h2(code: ErrorCode) -> Self {
        match code {
            ErrorCode::RefusedStream => Self::Unavailable,
            ErrorCode::Cancel => Self::Cancelled,
            ErrorCode::EnhanceYourCalm => Self::ResourceExhausted,
            ErrorCode::InadequateSecurity => Self::PermissionDenied,
            _ => Self::Internal,
        }
    }
}

/// A gRPC error status: code plus optional message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    /// Status code.
    pub code: Code,
    /// Human-readable message (`grpc-message`).
    pub message: String,
}

macro_rules! ctor {
    ($($name:ident => $code:ident),* $(,)?) => {
        $(
            #[doc = concat!("A [`Code::", stringify!($code), "`] status.")]
            pub fn $name(message: impl ToString) -> Self {
                Self::new(Code::$code, message)
            }
        )*
    };
}

impl Status {
    /// New status.
    pub fn new(code: Code, message: impl ToString) -> Self {
        Self {
            code,
            message: message.to_string(),
        }
    }

    ctor! {
        cancelled => Cancelled,
        unknown => Unknown,
        invalid_argument => InvalidArgument,
        deadline_exceeded => DeadlineExceeded,
        not_found => NotFound,
        already_exists => AlreadyExists,
        permission_denied => PermissionDenied,
        resource_exhausted => ResourceExhausted,
        failed_precondition => FailedPrecondition,
        aborted => Aborted,
        out_of_range => OutOfRange,
        unimplemented => Unimplemented,
        internal => Internal,
        unavailable => Unavailable,
        data_loss => DataLoss,
        unauthenticated => Unauthenticated,
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl core::error::Error for Status {}

/// Percent-encode a `grpc-message` value (gRPC HTTP/2 spec).
pub fn encode_message(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    for &b in msg.as_bytes() {
        if (0x20..=0x7e).contains(&b) && b != b'%' {
            out.push(b as char);
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            out.push('%');
            out.push(HEX[usize::from(b >> 4)] as char);
            out.push(HEX[usize::from(b & 0xf)] as char);
        }
    }
    out
}

/// Decode a percent-encoded `grpc-message` value. Invalid escapes are kept verbatim.
pub fn decode_message(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2]))
        {
            out.push(h << 4 | l);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
