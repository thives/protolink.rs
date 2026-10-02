//! gRPC length-prefixed messages (LPM): 1-byte compressed flag, 4-byte
//! big-endian length, payload.

use alloc::vec::Vec;

use crate::Status;

/// Size of the LPM prefix.
pub const HEADER_LEN: usize = 5;

/// Wrap `payload` as one uncompressed length-prefixed message.
pub fn encode(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.push(0);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Extract the single message of a complete unary request/response body.
///
/// Errors follow the reduced compatibility profile:
/// - no message → `INTERNAL`
/// - compressed message → `UNIMPLEMENTED`
/// - message larger than `max_size` → `RESOURCE_EXHAUSTED`
/// - truncated message → `INTERNAL`
/// - more than one message → `INTERNAL`
pub fn decode_unary(body: &[u8], max_size: usize) -> Result<&[u8], Status> {
    if body.is_empty() {
        return Err(Status::internal("missing message"));
    }
    if body.len() < HEADER_LEN {
        return Err(Status::internal("truncated message prefix"));
    }
    check_prefix(body, max_size)?;
    let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    let end = HEADER_LEN + len;
    if body.len() < end {
        return Err(Status::internal("truncated message"));
    }
    if body.len() > end {
        return Err(Status::internal("more than one message in a unary body"));
    }
    Ok(&body[HEADER_LEN..end])
}

/// Validate a complete 5-byte prefix and return the payload length.
fn check_prefix(prefix: &[u8], max_size: usize) -> Result<usize, Status> {
    match prefix[0] {
        0 => {}
        1 => {
            return Err(Status::unimplemented(
                "message compression is not supported",
            ));
        }
        _ => return Err(Status::internal("invalid message flags")),
    }
    let len = u32::from_be_bytes([prefix[1], prefix[2], prefix[3], prefix[4]]) as usize;
    if len > max_size {
        return Err(Status::resource_exhausted("message exceeds maximum size"));
    }
    Ok(len)
}

/// Incremental decoder for a stream of length-prefixed messages.
///
/// Bytes are [`push`](Self::push)ed as they arrive (in DATA events of any
/// size) and complete messages are taken with [`next`](Self::next). A message
/// may span any number of pushes and one push may complete many messages.
///
/// Prefixes are validated as soon as they are complete, so an oversized or
/// compressed message is reported before its payload is buffered: the decoder
/// never holds more than one partial message (at most `max_size + 5` bytes)
/// beyond the complete messages that have not been taken yet. Messages
/// preceding an invalid one are still returned by `next`, followed by the
/// error.
#[derive(Debug, Clone)]
pub struct Decoder {
    buf: Vec<u8>,
    /// Read position: start of the oldest message not yet taken.
    start: usize,
    /// End of the last complete, validated message.
    complete: usize,
    /// Complete messages in `start..complete`.
    count: usize,
    max_size: usize,
    error: Option<Status>,
}

impl Decoder {
    /// New decoder accepting messages of at most `max_size` payload bytes.
    pub fn new(max_size: usize) -> Self {
        Self {
            buf: Vec::new(),
            start: 0,
            complete: 0,
            count: 0,
            max_size,
            error: None,
        }
    }

    /// Append received bytes. Ignored once the decoder has failed.
    pub fn push(&mut self, data: &[u8]) {
        if self.error.is_some() || data.is_empty() {
            return;
        }
        self.buf.extend_from_slice(data);
        self.scan();
    }

    fn scan(&mut self) {
        while self.buf.len() - self.complete >= HEADER_LEN {
            let rest = &self.buf[self.complete..];
            let len = match check_prefix(rest, self.max_size) {
                Ok(len) => len,
                Err(e) => {
                    // Drop the invalid tail; it is never delivered.
                    self.buf.truncate(self.complete);
                    self.error = Some(e);
                    return;
                }
            };
            if rest.len() < HEADER_LEN + len {
                return;
            }
            self.complete += HEADER_LEN + len;
            self.count += 1;
        }
    }

    /// Next complete message, then the decoding error (once, and on every
    /// later call) if the stream contained an invalid message. `None` if more
    /// bytes are needed.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<Vec<u8>, Status>> {
        if self.count == 0 {
            return self.error.clone().map(Err);
        }
        let p = &self.buf[self.start..];
        let len = u32::from_be_bytes([p[1], p[2], p[3], p[4]]) as usize;
        let msg = p[HEADER_LEN..HEADER_LEN + len].to_vec();
        self.start += HEADER_LEN + len;
        self.count -= 1;
        self.compact();
        Some(Ok(msg))
    }

    fn compact(&mut self) {
        if self.start == self.buf.len() {
            self.buf.clear();
        } else if self.start > self.buf.len() / 2 {
            self.buf.drain(..self.start);
        } else {
            return;
        }
        self.complete -= self.start;
        self.start = 0;
    }

    /// Whether a complete message (or the error) is ready.
    pub fn has_next(&self) -> bool {
        self.count > 0 || self.error.is_some()
    }

    /// Number of complete messages not yet taken.
    pub fn message_count(&self) -> usize {
        self.count
    }

    /// Bytes of complete messages not yet taken, prefixes included.
    pub fn complete_len(&self) -> usize {
        self.complete - self.start
    }

    /// Bytes buffered, prefixes and the partial message included.
    pub fn buffered(&self) -> usize {
        self.buf.len() - self.start
    }

    /// The decoding error, if an invalid message was received.
    pub fn error(&self) -> Option<&Status> {
        self.error.as_ref()
    }

    /// Check the end of the stream: fails with `INTERNAL` if a partial
    /// message is buffered, or with the decoding error.
    pub fn finish(&self) -> Result<(), Status> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        if self.buf.len() > self.complete {
            return Err(Status::internal("truncated message"));
        }
        Ok(())
    }
}
