//! gRPC length-prefixed messages (LPM): 1-byte compressed flag, 4-byte
//! big-endian length, payload.
//!
//! A message with the flag set is compressed with the call's `grpc-encoding`
//! (see [`compression`](crate::compression)). Its length prefix counts the
//! compressed bytes, while the message size limits apply to the decompressed
//! message.

use alloc::vec::Vec;

use crate::Status;
use crate::compression::Codec;

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

/// Wrap `payload` as one length-prefixed message, compressed with `codec`.
///
/// The message is sent uncompressed if there is no codec, if it is shorter
/// than `min_size`, if the codec fails, or if compression would not make it
/// smaller.
pub fn frame(payload: &[u8], codec: Option<&dyn Codec>, min_size: usize) -> Vec<u8> {
    if let Some(codec) = codec
        && payload.len() >= min_size
    {
        let mut body = Vec::new();
        if codec.compress(payload, &mut body).is_ok() && body.len() < payload.len() {
            let mut out = Vec::with_capacity(HEADER_LEN + body.len());
            out.push(1);
            out.extend_from_slice(&(body.len() as u32).to_be_bytes());
            out.extend_from_slice(&body);
            return out;
        }
    }
    encode(payload)
}

/// Largest compressed message accepted for decompressed messages of at most
/// `max_size` bytes: incompressible data grows a little when compressed. The
/// decompressed size is limited to `max_size` regardless.
pub fn wire_limit(max_size: usize) -> usize {
    max_size.saturating_add(max_size / 16).saturating_add(64)
}

/// Extract the single message of a complete unary request/response body.
///
/// `codec` decompresses compressed messages; without one they are rejected.
///
/// Errors:
/// - no message → `INTERNAL`
/// - compressed message without a codec → `INTERNAL`
/// - message (decompressed) larger than `max_size` → `RESOURCE_EXHAUSTED`
/// - truncated message → `INTERNAL`
/// - more than one message → `INTERNAL`
pub fn decode_unary(
    body: &[u8],
    max_size: usize,
    codec: Option<&'static dyn Codec>,
) -> Result<Vec<u8>, Status> {
    let mut decoder = Decoder::new(max_size);
    decoder.set_codec(codec);
    decoder.push(body);
    let message = match decoder.next() {
        Some(message) => message?,
        None if body.is_empty() => return Err(Status::internal("missing message")),
        None => return Err(Status::internal("truncated message")),
    };
    decoder.finish()?;
    if decoder.has_next() {
        return Err(Status::internal("more than one message in a unary body"));
    }
    Ok(message)
}

/// Validate a complete 5-byte prefix and return the payload length.
fn check_prefix(prefix: &[u8], max_size: usize, has_codec: bool) -> Result<usize, Status> {
    let limit = match prefix[0] {
        0 => max_size,
        1 if has_codec => wire_limit(max_size),
        1 => {
            return Err(Status::internal(
                "compressed message without a message encoding",
            ));
        }
        _ => return Err(Status::internal("invalid message flags")),
    };
    let len = u32::from_be_bytes([prefix[1], prefix[2], prefix[3], prefix[4]]) as usize;
    if len > limit {
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
/// Prefixes are validated as soon as they are complete, so an oversized
/// message, or a compressed one when no codec is set, is reported before its
/// payload is buffered: the decoder never holds more than one partial message
/// (at most [`wire_limit`]`(max_size) + 5` bytes) beyond the complete messages
/// that have not been taken yet. Messages preceding an invalid one are still
/// returned by `next`, followed by the error.
///
/// Compressed messages stay compressed in the buffer and are decompressed by
/// `next`, with the decompressed size limited to `max_size`. A message that
/// fails to decompress ends the stream: `next` returns the error, and the
/// messages after it are dropped.
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
    codec: Option<&'static dyn Codec>,
    error: Option<Status>,
}

impl Decoder {
    /// New decoder accepting messages of at most `max_size` payload bytes.
    /// Compressed messages are rejected until a codec is set.
    pub fn new(max_size: usize) -> Self {
        Self {
            buf: Vec::new(),
            start: 0,
            complete: 0,
            count: 0,
            max_size,
            codec: None,
            error: None,
        }
    }

    /// The codec for compressed messages. Set it before pushing the first
    /// bytes: it is needed to validate the prefix of a compressed message.
    pub fn set_codec(&mut self, codec: Option<&'static dyn Codec>) {
        self.codec = codec;
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
            let len = match check_prefix(rest, self.max_size, self.codec.is_some()) {
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
        Some(self.next_framed()?.map(|(message, _)| message))
    }

    /// Like [`next`](Self::next), and the number of bytes the message took
    /// on the wire, prefix included: the amount of flow-control credit it
    /// accounts for.
    pub(crate) fn next_framed(&mut self) -> Option<Result<(Vec<u8>, usize), Status>> {
        if self.count == 0 {
            return self.error.clone().map(Err);
        }
        let p = &self.buf[self.start..];
        let compressed = p[0] == 1;
        let len = u32::from_be_bytes([p[1], p[2], p[3], p[4]]) as usize;
        let wire_len = HEADER_LEN + len;
        let payload = &p[HEADER_LEN..wire_len];
        let message = if compressed {
            decompress(self.codec, payload, self.max_size)
        } else {
            Ok(payload.to_vec())
        };
        match message {
            Ok(message) => {
                self.start += wire_len;
                self.count -= 1;
                self.compact();
                Some(Ok((message, wire_len)))
            }
            Err(status) => {
                // Nothing after a broken message can be trusted.
                self.buf.clear();
                self.start = 0;
                self.complete = 0;
                self.count = 0;
                self.error = Some(status.clone());
                Some(Err(status))
            }
        }
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

fn decompress(
    codec: Option<&'static dyn Codec>,
    payload: &[u8],
    max_size: usize,
) -> Result<Vec<u8>, Status> {
    let codec =
        codec.ok_or_else(|| Status::internal("compressed message without a message encoding"))?;
    let mut message = Vec::new();
    codec
        .decompress(payload, &mut message, max_size)
        .map_err(|e| e.into_status())?;
    // Do not rely on the codec honouring the limit.
    if message.len() > max_size {
        return Err(Status::resource_exhausted("message exceeds maximum size"));
    }
    Ok(message)
}
