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
///
/// Returns `RESOURCE_EXHAUSTED` if the payload does not fit the wire's `u32`
/// length, the frame cannot fit a `Vec` on this target, or allocation fails.
pub fn encode(payload: &[u8]) -> Result<Vec<u8>, Status> {
    encode_flagged(payload, 0)
}

fn frame_lengths(payload_len: usize) -> Result<(u32, usize), Status> {
    let wire_len = u32::try_from(payload_len)
        .map_err(|_| Status::resource_exhausted("message length exceeds wire u32"))?;
    let frame_len = payload_len
        .checked_add(HEADER_LEN)
        .filter(|&len| len <= isize::MAX as usize)
        .ok_or_else(|| Status::resource_exhausted("message frame exceeds target capacity"))?;
    Ok((wire_len, frame_len))
}

fn encode_flagged(payload: &[u8], flag: u8) -> Result<Vec<u8>, Status> {
    let (wire_len, frame_len) = frame_lengths(payload.len())?;
    let mut out = Vec::new();
    out.try_reserve_exact(frame_len)
        .map_err(|_| Status::resource_exhausted("message frame allocation failed"))?;
    out.push(flag);
    out.extend_from_slice(&wire_len.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// Wrap `payload` as one length-prefixed message, compressed with `codec`.
///
/// The message is sent uncompressed if there is no codec, if it is shorter
/// than `min_size`, if the codec fails, or if compression would not make it
/// smaller. Returns `RESOURCE_EXHAUSTED` if the chosen wire payload cannot
/// be framed (see [`encode`]).
pub fn frame(
    payload: &[u8],
    codec: Option<&dyn Codec>,
    min_size: usize,
) -> Result<Vec<u8>, Status> {
    if let Some(codec) = codec
        && payload.len() >= min_size
    {
        let mut body = Vec::new();
        if codec.compress(payload, &mut body).is_ok() && body.len() < payload.len() {
            return encode_flagged(&body, 1);
        }
    }
    encode(payload)
}

/// Largest compressed message accepted for decompressed messages of at most
/// `max_size` bytes: incompressible data grows a little when compressed. The
/// decompressed size is limited to `max_size` regardless. This calculation
/// saturates for permissive configurations; actual wire lengths are still
/// limited to `u32` and frames to the target's `Vec` capacity.
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

/// Validate a complete 5-byte prefix and return the total frame length.
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
    let len = usize::try_from(u32::from_be_bytes([
        prefix[1], prefix[2], prefix[3], prefix[4],
    ]))
    .map_err(|_| Status::resource_exhausted("message length exceeds target capacity"))?;
    if len > limit {
        return Err(Status::resource_exhausted("message exceeds maximum size"));
    }
    frame_lengths(len).map(|(_, frame_len)| frame_len)
}

/// Incremental decoder for a stream of length-prefixed messages.
///
/// Bytes are [`push`](Self::push)ed as they arrive (in DATA events of any
/// size) and complete messages are taken with [`next`](Self::next). A message
/// may span any number of pushes and one push may complete many messages.
///
/// Prefixes are validated as soon as they are complete, so an oversized
/// message, or a compressed one when no codec is set, is reported without
/// waiting for its payload, and the invalid tail is discarded. The decoder
/// never holds more than one partial message
/// (at most [`wire_limit`]`(max_size) + 5` bytes) beyond the complete messages
/// that have not been taken yet. Messages preceding an invalid one are still
/// returned by `next`, followed by the error.
///
/// An empty decoder releases its buffer allocation, without losing its codec
/// or size configuration. With only a partial frame remaining, retained capacity
/// is trimmed when it exceeds twice the live bytes or a 1024-byte reuse allowance
/// (whichever is larger), capped by the single-partial-message wire budget.
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
    /// Compressed messages are rejected until a codec is set. Larger limits
    /// are permitted, but each wire payload must fit `u32`, and its prefix
    /// plus payload must fit the target's `Vec` capacity (`isize::MAX`).
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
        if self.count == 0 {
            self.compact();
        }
    }

    fn scan(&mut self) {
        while self.buf.len() - self.complete >= HEADER_LEN {
            let rest = &self.buf[self.complete..];
            let wire_len = match check_prefix(rest, self.max_size, self.codec.is_some()) {
                Ok(len) => len,
                Err(e) => {
                    // Drop the invalid tail; it is never delivered.
                    self.buf.truncate(self.complete);
                    self.error = Some(e);
                    self.compact();
                    return;
                }
            };
            if rest.len() < wire_len {
                return;
            }
            // The full frame fits in the remaining buffer, so this cannot overflow.
            self.complete += wire_len;
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
        let wire_len = match check_prefix(p, self.max_size, self.codec.is_some()) {
            Ok(wire_len) => wire_len,
            Err(status) => {
                self.buf = Vec::new();
                self.start = 0;
                self.complete = 0;
                self.count = 0;
                self.error = Some(status.clone());
                return Some(Err(status));
            }
        };
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
                self.buf = Vec::new();
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
            self.buf = Vec::new();
        } else if self.count == 0 || self.start > self.buf.len() / 2 {
            self.buf.drain(..self.start);
        } else {
            return;
        }
        self.complete -= self.start;
        self.start = 0;
        if self.count == 0 {
            let scratch_limit = wire_limit(self.max_size).saturating_add(HEADER_LEN);
            let reuse_limit = self
                .buf
                .len()
                .saturating_mul(2)
                .max(1024)
                .min(scratch_limit);
            if self.buf.capacity() > reuse_limit {
                self.buf.shrink_to(reuse_limit);
            }
        }
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

#[cfg(test)]
mod boundary_tests {
    use alloc::vec;

    use super::*;
    use crate::Code;
    use crate::compression::CodecError;

    #[derive(Debug)]
    struct TestCodec;

    impl Codec for TestCodec {
        fn name(&self) -> &'static str {
            "test"
        }

        fn compress(&self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError> {
            out.extend_from_slice(input);
            Ok(())
        }

        fn decompress(
            &self,
            input: &[u8],
            out: &mut Vec<u8>,
            limit: usize,
        ) -> Result<(), CodecError> {
            if input.first() == Some(&255) {
                return Err(CodecError::Corrupt);
            }
            if input.len() > limit {
                return Err(CodecError::TooLarge);
            }
            out.extend_from_slice(input);
            Ok(())
        }
    }

    static TEST_CODEC: TestCodec = TestCodec;

    #[test]
    fn consumed_messages_release_each_active_decoders_high_water_allocation() {
        let max_size = 65_536;
        let mut framed = encode(&vec![7; max_size]).unwrap();
        framed[0] = 1;
        let mut decoders = vec![Decoder::new(max_size); 32];
        for decoder in &mut decoders {
            decoder.set_codec(Some(&TEST_CODEC));
            decoder.push(&framed);
            assert!(decoder.buf.capacity() >= framed.len());
        }
        for decoder in &mut decoders {
            assert_eq!(decoder.next().unwrap().unwrap().len(), max_size);
            assert_eq!(decoder.buf.capacity(), 0);
            assert_eq!(decoder.buffered(), 0);
            assert_eq!(decoder.complete_len(), 0);
            assert_eq!(decoder.max_size, max_size);
            assert_eq!(decoder.codec.unwrap().name(), "test");
            assert_eq!(decoder.finish(), Ok(()));
            decoder.push(&[1, 0, 0, 0, 1, 42]);
            assert_eq!(decoder.next(), Some(Ok(vec![42])));
            assert_eq!(decoder.buf.capacity(), 0);
        }
        assert_eq!(
            decoders
                .iter()
                .map(|decoder| decoder.buf.capacity())
                .sum::<usize>(),
            0
        );
    }

    #[test]
    fn draining_to_a_small_partial_reclaims_large_capacity_and_preserves_framing() {
        let mut decoder = Decoder::new(65_536);
        let first = encode(&vec![7; 65_536]).unwrap();
        let next = encode(b"next").unwrap();
        decoder.push(&first);
        decoder.push(&next[..3]);
        assert!(decoder.buf.capacity() > first.len());
        let (message, wire_len) = decoder.next_framed().unwrap().unwrap();
        assert_eq!(message.len(), 65_536);
        assert_eq!(wire_len, first.len());
        assert_eq!(decoder.buf, next[..3]);
        assert_eq!(decoder.start, 0);
        assert_eq!(decoder.complete_len(), 0);
        assert!(decoder.buf.capacity() <= 1024);
        decoder.push(&next[3..]);
        assert_eq!(decoder.next(), Some(Ok(b"next".to_vec())));
        assert_eq!(decoder.buf.capacity(), 0);
        assert_eq!(decoder.finish(), Ok(()));
    }

    #[test]
    fn partial_only_compaction_does_not_wait_for_half_the_buffer_to_be_consumed() {
        let mut decoder = Decoder::new(4096);
        decoder.push(&encode(b"first").unwrap());
        let next = encode(&vec![3; 4096]).unwrap();
        decoder.push(&next[..2000]);
        assert_eq!(decoder.next(), Some(Ok(b"first".to_vec())));
        assert_eq!(decoder.start, 0);
        assert_eq!(decoder.buf, next[..2000]);
        assert_eq!(decoder.complete_len(), 0);
        decoder.push(&next[2000..]);
        assert_eq!(decoder.next(), Some(Ok(vec![3; 4096])));
        assert_eq!(decoder.buf.capacity(), 0);
    }

    #[test]
    fn incremental_partial_capacity_stays_within_the_single_frame_budget() {
        let max_size = 1000;
        let framed = encode(&vec![3; max_size]).unwrap();
        let mut decoder = Decoder::new(max_size);
        for byte in &framed {
            decoder.push(core::slice::from_ref(byte));
            if decoder.message_count() == 0 {
                assert!(decoder.buf.capacity() <= wire_limit(max_size) + HEADER_LEN);
                assert_eq!(decoder.start, 0);
            }
        }
        assert_eq!(decoder.next(), Some(Ok(vec![3; max_size])));
        assert_eq!(decoder.buf.capacity(), 0);
    }

    #[test]
    fn decoder_errors_release_discarded_buffer_capacity() {
        let mut decoder = Decoder::new(4096);
        decoder.push(&[255; 65_536]);
        assert_eq!(decoder.next().unwrap().unwrap_err().code, Code::Internal);
        assert_eq!(decoder.buf.capacity(), 0);

        let mut decoder = Decoder::new(65_536);
        decoder.set_codec(Some(&TEST_CODEC));
        let mut corrupt = encode(&vec![255; 65_536]).unwrap();
        corrupt[0] = 1;
        decoder.push(&corrupt);
        assert!(decoder.buf.capacity() >= corrupt.len());
        assert_eq!(decoder.next().unwrap().unwrap_err().code, Code::Internal);
        assert_eq!(decoder.buf.capacity(), 0);
        assert_eq!(decoder.codec.unwrap().name(), "test");

        let mut decoder = Decoder::new(4096);
        decoder.push(&encode(b"before").unwrap());
        decoder.push(&[255; 65_536]);
        assert_eq!(decoder.next(), Some(Ok(b"before".to_vec())));
        assert_eq!(decoder.buf.capacity(), 0);
        assert_eq!(decoder.next().unwrap().unwrap_err().code, Code::Internal);
    }

    #[test]
    fn frame_lengths_are_checked_without_allocating() {
        assert_eq!(frame_lengths(0), Ok((0, HEADER_LEN)));
        assert_eq!(frame_lengths(10), Ok((10, HEADER_LEN + 10)));
        let max_payload = (u32::MAX as usize).min(isize::MAX as usize - HEADER_LEN);
        assert_eq!(
            frame_lengths(max_payload),
            Ok((
                u32::try_from(max_payload).unwrap(),
                max_payload + HEADER_LEN
            ))
        );
        for len in [max_payload + 1, usize::MAX - HEADER_LEN + 1, usize::MAX] {
            assert_eq!(
                frame_lengths(len).unwrap_err().code,
                Code::ResourceExhausted
            );
        }
        assert_eq!(wire_limit(usize::MAX), usize::MAX);
    }

    #[test]
    fn empty_outbound_frame_round_trips_at_zero_limit() {
        let framed = encode(b"").unwrap();
        assert_eq!(framed, [0; HEADER_LEN]);
        assert_eq!(frame(b"", None, usize::MAX), Ok(framed.clone()));
        assert_eq!(decode_unary(&framed, 0, None), Ok(Vec::new()));
    }

    #[test]
    fn maximum_wire_prefix_is_checked_before_its_payload() {
        let prefix = [0, 255, 255, 255, 255];
        let mut decoder = Decoder::new(usize::MAX);
        decoder.push(&prefix);
        if usize::BITS <= 32 {
            assert_eq!(
                decoder.next().unwrap().unwrap_err().code,
                Code::ResourceExhausted
            );
            assert_eq!(decoder.finish().unwrap_err().code, Code::ResourceExhausted);
            assert_eq!(decoder.buffered(), 0);
        } else {
            assert!(decoder.next().is_none());
            assert_eq!(decoder.buffered(), HEADER_LEN);
            assert_eq!(decoder.finish().unwrap_err().code, Code::Internal);
        }
    }

    #[test]
    fn maximum_prefix_keeps_preceding_messages_in_order() {
        let mut decoder = Decoder::new(usize::MAX);
        decoder.push(&encode(b"before").unwrap());
        for byte in [0, 255, 255, 255, 255] {
            decoder.push(&[byte]);
        }
        assert_eq!(decoder.next(), Some(Ok(b"before".to_vec())));
        if usize::BITS <= 32 {
            assert_eq!(
                decoder.next().unwrap().unwrap_err().code,
                Code::ResourceExhausted
            );
            decoder.push(&encode(b"after").unwrap());
            assert_eq!(decoder.buffered(), 0);
        } else {
            assert!(decoder.next().is_none());
        }
    }
}
