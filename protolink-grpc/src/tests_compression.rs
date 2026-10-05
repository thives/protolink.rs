//! Tests of message compression: the codec traits, the gzip container,
//! length-prefixed framing, negotiation and flow-control accounting.
//!
//! Most tests run on two test-only backends, so they cover the trait path in
//! every feature combination: [`Rle`], a trivial non-DEFLATE [`Codec`], and
//! [`Stored`], a DEFLATE backend that only emits stored blocks.

extern crate std;

use alloc::collections::{BTreeMap, VecDeque};
use alloc::vec;
use alloc::vec::Vec;
use core::task::{Context, Poll, Waker};

use super::*;
use crate::compression::{Codec, CodecError, Compression, Deflate, Gzip};
use protolink_http2::{Connection, Event, HeaderField};

// ---------------------------------------------------------------------------
// Test backends
// ---------------------------------------------------------------------------

/// Run-length codec: `(count, byte)` pairs. Shrinks runs, doubles anything else.
#[derive(Debug)]
struct Rle {
    honor_limit: bool,
}

impl Codec for Rle {
    fn name(&self) -> &'static str {
        "rle"
    }

    fn compress(&self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError> {
        let mut i = 0;
        while i < input.len() {
            let byte = input[i];
            let mut n = 1;
            while i + n < input.len() && input[i + n] == byte && n < 255 {
                n += 1;
            }
            out.extend_from_slice(&[n as u8, byte]);
            i += n;
        }
        Ok(())
    }

    fn decompress(&self, input: &[u8], out: &mut Vec<u8>, limit: usize) -> Result<(), CodecError> {
        if input.len() & 1 != 0 {
            return Err(CodecError::Corrupt);
        }
        let start = out.len();
        for pair in input.chunks(2) {
            let n = usize::from(pair[0]);
            if self.honor_limit && out.len() - start + n > limit {
                return Err(CodecError::TooLarge);
            }
            out.resize(out.len() + n, pair[1]);
        }
        Ok(())
    }
}

static RLE: Rle = Rle { honor_limit: true };
/// A broken codec that does not enforce the limit it is given.
static RLE_NO_LIMIT: Rle = Rle { honor_limit: false };
static ACCEPT_RLE: [&dyn Codec; 1] = [&RLE];

/// Accept and send `rle`; compress everything from 8 bytes.
fn rle() -> Compression {
    Compression::new(&ACCEPT_RLE).send(&RLE).min_size(8)
}

/// DEFLATE backend using stored (uncompressed) blocks only.
#[derive(Debug)]
struct Stored;

impl Deflate for Stored {
    fn deflate(&self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError> {
        if input.is_empty() {
            out.extend_from_slice(&[1, 0, 0, 0xff, 0xff]);
            return Ok(());
        }
        let mut chunks = input.chunks(65_535).peekable();
        while let Some(chunk) = chunks.next() {
            out.push(u8::from(chunks.peek().is_none()));
            let len = chunk.len() as u16;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&(!len).to_le_bytes());
            out.extend_from_slice(chunk);
        }
        Ok(())
    }

    fn inflate(&self, input: &[u8], out: &mut Vec<u8>, limit: usize) -> Result<(), CodecError> {
        let start = out.len();
        let mut pos = 0;
        loop {
            let head = *input.get(pos).ok_or(CodecError::Corrupt)?;
            if head & 6 != 0 {
                return Err(CodecError::Unsupported);
            }
            let sizes = input.get(pos + 1..pos + 5).ok_or(CodecError::Corrupt)?;
            let len = u16::from_le_bytes([sizes[0], sizes[1]]);
            if len != !u16::from_le_bytes([sizes[2], sizes[3]]) {
                return Err(CodecError::Corrupt);
            }
            let data = input
                .get(pos + 5..pos + 5 + usize::from(len))
                .ok_or(CodecError::Corrupt)?;
            if out.len() - start + data.len() > limit {
                return Err(CodecError::TooLarge);
            }
            out.extend_from_slice(data);
            pos += 5 + usize::from(len);
            if head & 1 == 1 {
                break;
            }
        }
        if pos == input.len() {
            Ok(())
        } else {
            Err(CodecError::Corrupt)
        }
    }
}

static STORED_GZIP: Gzip<Stored> = Gzip::new(Stored);

/// A backend that can only decompress, like some ROM libraries.
#[derive(Debug)]
struct InflateOnly;

impl Deflate for InflateOnly {
    fn deflate(&self, _: &[u8], _: &mut Vec<u8>) -> Result<(), CodecError> {
        Err(CodecError::Unsupported)
    }

    fn inflate(&self, input: &[u8], out: &mut Vec<u8>, limit: usize) -> Result<(), CodecError> {
        Stored.inflate(input, out, limit)
    }
}

static INFLATE_ONLY_GZIP: Gzip<InflateOnly> = Gzip::new(InflateOnly);

fn compress(codec: &dyn Codec, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    codec.compress(data, &mut out).unwrap();
    out
}

fn decompress(codec: &dyn Codec, data: &[u8], limit: usize) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    codec.decompress(data, &mut out, limit)?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// CRC-32 and the gzip container
// ---------------------------------------------------------------------------

#[test]
fn crc32_matches_the_standard_check_value() {
    assert_eq!(Stored.crc32(0, b"123456789"), 0xcbf4_3926);
    assert_eq!(Stored.crc32(0, b""), 0);
    let partial = Stored.crc32(0, b"1234");
    assert_eq!(Stored.crc32(partial, b"56789"), 0xcbf4_3926);
}

#[test]
fn gzip_round_trips_every_size() {
    for size in [0, 1, 100, 65_535, 65_536, 70_000] {
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let packed = compress(&STORED_GZIP, &data);
        assert_eq!(&packed[..3], [0x1f, 0x8b, 8]);
        assert_eq!(decompress(&STORED_GZIP, &packed, size), Ok(data), "{size}");
    }
}

#[test]
fn gzip_enforces_the_decompression_limit() {
    let packed = compress(&STORED_GZIP, &[3; 1000]);
    assert_eq!(
        decompress(&STORED_GZIP, &packed, 999),
        Err(CodecError::TooLarge)
    );
    assert_eq!(decompress(&STORED_GZIP, &packed, 1000), Ok(vec![3; 1000]));
}

#[test]
fn gzip_rejects_corrupt_input() {
    let good = compress(&STORED_GZIP, b"some message");
    let n = good.len();
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();

    let mut bad = good.clone();
    bad[n - 8] ^= 1;
    cases.push(("crc", bad));
    let mut bad = good.clone();
    bad[n - 4] ^= 1;
    cases.push(("size", bad));
    let mut bad = good.clone();
    bad[0] = 0;
    cases.push(("magic", bad));
    let mut bad = good.clone();
    bad[2] = 7;
    cases.push(("method", bad));
    let mut bad = good.clone();
    bad[3] = 0x20;
    cases.push(("reserved flag", bad));
    let mut bad = good.clone();
    bad[12] ^= 0xff;
    cases.push(("deflate body", bad));
    cases.push(("empty", Vec::new()));
    cases.push(("header only", good[..10].to_vec()));
    cases.push(("no trailer", good[..n - 8].to_vec()));
    cases.push(("truncated trailer", good[..n - 1].to_vec()));
    let mut bad = good.clone();
    bad.insert(n - 8, 0);
    cases.push(("trailing body byte", bad));

    for (name, input) in cases {
        assert_eq!(
            decompress(&STORED_GZIP, &input, 1 << 20),
            Err(CodecError::Corrupt),
            "{name}"
        );
    }
}

/// A gzip member as other implementations write them, with optional header
/// fields.
fn gzip_with_fields(data: &[u8], flags: u8, corrupt_header_crc: bool) -> Vec<u8> {
    let mut out = vec![0x1f, 0x8b, 8, flags, 1, 2, 3, 4, 0, 3];
    if flags & 4 != 0 {
        out.extend_from_slice(&[3, 0, 1, 2, 3]);
    }
    if flags & 8 != 0 {
        out.extend_from_slice(b"file name\0");
    }
    if flags & 16 != 0 {
        out.extend_from_slice(b"comment\0");
    }
    if flags & 2 != 0 {
        let crc = Stored.crc32(0, &out) as u16 ^ u16::from(corrupt_header_crc);
        out.extend_from_slice(&crc.to_le_bytes());
    }
    Stored.deflate(data, &mut out).unwrap();
    out.extend_from_slice(&Stored.crc32(0, data).to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out
}

#[test]
fn gzip_accepts_optional_header_fields() {
    for flags in 0..32u8 {
        if flags & 0xe0 != 0 {
            continue;
        }
        let packed = gzip_with_fields(b"payload", flags, false);
        assert_eq!(
            decompress(&STORED_GZIP, &packed, 100),
            Ok(b"payload".to_vec()),
            "flags {flags:#x}"
        );
    }
    let bad = gzip_with_fields(b"payload", 2, true);
    assert_eq!(
        decompress(&STORED_GZIP, &bad, 100),
        Err(CodecError::Corrupt)
    );
    // Header fields cut short.
    let packed = gzip_with_fields(b"payload", 4 | 8 | 16 | 2, false);
    for cut in 10..28 {
        assert!(
            decompress(&STORED_GZIP, &packed[..cut], 100).is_err(),
            "{cut}"
        );
    }
}

#[test]
fn gzip_container_is_understood_by_flate2() {
    use std::io::Read;

    let data: Vec<u8> = (0..70_000).map(|i| (i % 253) as u8).collect();
    let packed = compress(&STORED_GZIP, &data);
    let mut unpacked = Vec::new();
    flate2::read::GzDecoder::new(&packed[..])
        .read_to_end(&mut unpacked)
        .unwrap();
    assert_eq!(unpacked, data);
}

// ---------------------------------------------------------------------------
// Configuration and negotiation helpers
// ---------------------------------------------------------------------------

static ACCEPT_TWO: [&dyn Codec; 2] = [&RLE, &STORED_GZIP];

#[test]
fn accept_header_lists_codecs_then_identity() {
    assert_eq!(Compression::NONE.accept_header(), None);
    assert_eq!(rle().accept_header().as_deref(), Some("rle,identity"));
    assert_eq!(
        Compression::new(&ACCEPT_TWO).accept_header().as_deref(),
        Some("rle,gzip,identity")
    );
    // Sending alone advertises nothing: we cannot decode anything.
    assert_eq!(Compression::NONE.send(&RLE).accept_header(), None);
}

#[test]
fn request_encoding_must_be_configured_or_identity() {
    let c = Compression::new(&ACCEPT_TWO);
    for identity in [None, Some(""), Some("identity")] {
        assert!(matches!(c.decoder_for(identity), Ok(None)), "{identity:?}");
    }
    assert_eq!(c.decoder_for(Some("gzip")).unwrap().unwrap().name(), "gzip");
    assert_eq!(c.decoder_for(Some(" rle ")).unwrap().unwrap().name(), "rle");
    assert!(c.decoder_for(Some("zstd")).is_err());
    assert!(rle().decoder_for(Some("gzip")).is_err());
    assert!(Compression::NONE.decoder_for(Some("rle")).is_err());
}

#[test]
fn response_encoding_needs_the_peer_to_accept_it() {
    let c = rle();
    for accepted in ["rle", "gzip,rle", "gzip, rle ,identity", "rle,identity"] {
        assert_eq!(c.encoder_for(Some(accepted)).unwrap().name(), "rle");
    }
    for refused in [None, Some(""), Some("gzip"), Some("identity"), Some("rle2")] {
        assert!(c.encoder_for(refused).is_none(), "{refused:?}");
    }
    assert!(
        Compression::new(&ACCEPT_RLE)
            .encoder_for(Some("rle"))
            .is_none()
    );
}

#[test]
fn defaults_leave_compression_off() {
    assert!(!ClientConfig::default().compression.is_enabled());
    assert!(!ServerConfig::default().compression.is_enabled());
    assert!(rle().is_enabled());
}

#[test]
#[should_panic(expected = "invalid compression codec name")]
#[cfg(debug_assertions)]
fn reserved_codec_names_are_rejected() {
    #[derive(Debug)]
    struct Fake;
    impl Codec for Fake {
        fn name(&self) -> &'static str {
            "identity"
        }
        fn compress(&self, _: &[u8], _: &mut Vec<u8>) -> Result<(), CodecError> {
            Ok(())
        }
        fn decompress(&self, _: &[u8], _: &mut Vec<u8>, _: usize) -> Result<(), CodecError> {
            Ok(())
        }
    }
    static FAKE: Fake = Fake;
    static ACCEPT: [&dyn Codec; 1] = [&FAKE];
    let _ = Client::new(ClientConfig {
        compression: Compression::new(&ACCEPT),
        ..ClientConfig::default()
    });
}

// ---------------------------------------------------------------------------
// Length-prefixed framing
// ---------------------------------------------------------------------------

#[test]
fn frame_compresses_only_when_worthwhile() {
    // Compressible: flag set, shorter, and the prefix counts compressed bytes.
    let data = vec![7; 100];
    let framed = lpm::frame(&data, Some(&RLE), 8).unwrap();
    assert_eq!(framed[0], 1);
    assert!(framed.len() < data.len());
    let len = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]) as usize;
    assert_eq!(framed.len(), lpm::HEADER_LEN + len);

    // Below `min_size`, no codec, incompressible, or a failing codec.
    assert_eq!(lpm::frame(&data, Some(&RLE), 101), lpm::encode(&data));
    assert_eq!(lpm::frame(&data, None, 0), lpm::encode(&data));
    let noise: Vec<u8> = (0..100).collect();
    assert_eq!(lpm::frame(&noise, Some(&RLE), 0), lpm::encode(&noise));
    assert_eq!(
        lpm::frame(&data, Some(&INFLATE_ONLY_GZIP), 0),
        lpm::encode(&data)
    );
    assert_eq!(lpm::frame(b"", Some(&RLE), 0), lpm::encode(b""));
}

fn decoder(max_size: usize, codec: Option<&'static dyn Codec>) -> lpm::Decoder {
    let mut d = lpm::Decoder::new(max_size);
    d.set_codec(codec);
    d
}

fn take_all(d: &mut lpm::Decoder) -> Vec<Vec<u8>> {
    core::iter::from_fn(|| d.next().map(Result::unwrap)).collect()
}

#[test]
fn decoder_handles_every_split_point_with_compressed_messages() {
    let messages: [Vec<u8>; 5] = [
        vec![7; 100],
        b"short".to_vec(),
        vec![1; 300],
        Vec::new(),
        (0..=255).collect(),
    ];
    let bytes: Vec<u8> = messages
        .iter()
        .flat_map(|m| lpm::frame(m, Some(&RLE), 8).unwrap())
        .collect();
    assert!(bytes.len() < messages.iter().map(Vec::len).sum::<usize>());
    for split in 0..=bytes.len() {
        let mut d = decoder(1024, Some(&RLE));
        d.push(&bytes[..split]);
        let mut got = take_all(&mut d);
        d.push(&bytes[split..]);
        got.extend(take_all(&mut d));
        assert_eq!(got, messages, "split at {split}");
        assert_eq!(d.finish(), Ok(()));
        assert_eq!(d.buffered(), 0);
    }
}

#[test]
fn decoder_reports_wire_length_for_flow_control() {
    let data = vec![9; 1000];
    let framed = lpm::frame(&data, Some(&RLE), 0).unwrap();
    let plain = lpm::encode(b"hello").unwrap();
    let mut d = decoder(4096, Some(&RLE));
    d.push(&framed);
    d.push(&plain);
    assert_eq!(d.next_framed(), Some(Ok((data, framed.len()))));
    assert_eq!(d.next_framed(), Some(Ok((b"hello".to_vec(), plain.len()))));
    assert!(framed.len() < 100, "message did not shrink");
}

#[test]
fn compressed_messages_stay_compressed_until_taken() {
    let framed = lpm::frame(&[5; 4000], Some(&RLE), 0).unwrap();
    let mut d = decoder(4096, Some(&RLE));
    for _ in 0..50 {
        d.push(&framed);
    }
    assert_eq!(d.message_count(), 50);
    assert_eq!(d.buffered(), 50 * framed.len());
}

#[test]
fn decoder_rejects_compressed_message_without_codec() {
    let framed = lpm::frame(&[5; 100], Some(&RLE), 0).unwrap();
    let mut d = decoder(1024, None);
    d.push(&framed);
    assert_eq!(d.next().unwrap().unwrap_err().code, Code::Internal);
    assert_eq!(d.buffered(), 0);
}

#[test]
fn decompression_bomb_is_resource_exhausted() {
    // 10 000 bytes in about 80 bytes of input, for a 100-byte limit.
    let bomb = lpm::frame(&[0; 10_000], Some(&RLE), 0).unwrap();
    assert!(bomb.len() < lpm::wire_limit(100));
    for codec in [&RLE, &RLE_NO_LIMIT] {
        let mut d = decoder(100, Some(codec));
        d.push(&lpm::frame(&[1; 50], Some(codec), 0).unwrap());
        d.push(&bomb);
        d.push(&lpm::encode(b"after").unwrap());
        // Whatever precedes the bomb is delivered; nothing follows it.
        assert_eq!(d.next(), Some(Ok(vec![1; 50])));
        assert_eq!(d.next().unwrap().unwrap_err().code, Code::ResourceExhausted);
        assert_eq!(d.next().unwrap().unwrap_err().code, Code::ResourceExhausted);
        assert_eq!(d.buffered(), 0);
        d.push(&[0; 100]);
        assert_eq!(d.buffered(), 0, "input after an error is ignored");
    }
}

#[test]
fn corrupt_compressed_message_is_internal() {
    let mut d = decoder(100, Some(&RLE));
    // An odd number of bytes is not a sequence of pairs.
    d.push(&[1, 0, 0, 0, 3, 1, 2, 3]);
    assert_eq!(d.next().unwrap().unwrap_err().code, Code::Internal);
}

#[test]
fn oversized_compressed_prefix_is_rejected_before_the_payload() {
    let mut d = decoder(100, Some(&RLE));
    let too_big = lpm::wire_limit(100) + 1;
    let mut prefix = vec![1];
    prefix.extend_from_slice(&(too_big as u32).to_be_bytes());
    d.push(&prefix);
    assert_eq!(d.next().unwrap().unwrap_err().code, Code::ResourceExhausted);
    // A compressed message may exceed the plain limit a little.
    let mut d = decoder(100, Some(&RLE));
    let mut prefix = vec![1];
    prefix.extend_from_slice(&(lpm::wire_limit(100) as u32).to_be_bytes());
    d.push(&prefix);
    assert!(d.next().is_none());
    // An uncompressed one may not.
    let mut d = decoder(100, Some(&RLE));
    d.push(&[0, 0, 0, 0, 101]);
    assert_eq!(d.next().unwrap().unwrap_err().code, Code::ResourceExhausted);
}

#[test]
fn decode_unary_handles_compressed_and_malformed_bodies() {
    let framed = lpm::frame(&[8; 200], Some(&RLE), 0).unwrap();
    assert_eq!(
        lpm::decode_unary(&framed, 1024, Some(&RLE)),
        Ok(vec![8; 200])
    );
    assert_eq!(
        lpm::decode_unary(&lpm::encode(b"x").unwrap(), 1024, None),
        Ok(b"x".to_vec())
    );
    let code = |body: &[u8], codec| lpm::decode_unary(body, 1024, codec).unwrap_err().code;
    assert_eq!(code(b"", None), Code::Internal);
    assert_eq!(code(&framed[..3], Some(&RLE)), Code::Internal);
    assert_eq!(
        code(&framed[..framed.len() - 1], Some(&RLE)),
        Code::Internal
    );
    assert_eq!(code(&framed, None), Code::Internal);
    let two = [framed.clone(), framed].concat();
    assert_eq!(code(&two, Some(&RLE)), Code::Internal);
    assert_eq!(
        lpm::decode_unary(
            &lpm::frame(&[8; 200], Some(&RLE), 0).unwrap(),
            100,
            Some(&RLE)
        )
        .unwrap_err()
        .code,
        Code::ResourceExhausted
    );
}

// ---------------------------------------------------------------------------
// Client and server
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct CallState {
    queue: VecDeque<Vec<u8>>,
    ended: bool,
}

/// `/t.T/Echo`: unary echo. `/t.T/Stream`: bidirectional echo.
#[derive(Debug, Default)]
struct TestHandler {
    calls: BTreeMap<CallId, CallState>,
    cancelled: Vec<CallId>,
}

impl Handler for TestHandler {
    fn call(
        &mut self,
        ctx: &mut CallContext<'_>,
        request: &[u8],
    ) -> Option<Result<Vec<u8>, Status>> {
        let path = ctx.path;
        (path == "/t.T/Echo").then(|| Ok(request.to_vec()))
    }

    fn method_kind(&self, path: &str) -> Option<MethodKind> {
        match path {
            "/t.T/Stream" => Some(MethodKind::BidiStreaming),
            "/t.T/Echo" => Some(MethodKind::Unary),
            _ => None,
        }
    }

    fn on_message(&mut self, ctx: &mut CallContext<'_>, message: &[u8]) -> Result<(), Status> {
        let call = ctx.id;
        self.calls
            .entry(call)
            .or_default()
            .queue
            .push_back(message.to_vec());
        Ok(())
    }

    fn on_half_close(&mut self, ctx: &mut CallContext<'_>) -> Result<(), Status> {
        let call = ctx.id;
        self.calls.entry(call).or_default().ended = true;
        Ok(())
    }

    fn poll_response(
        &mut self,
        ctx: &mut CallContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Next<Vec<u8>>> {
        let call = ctx.id;
        let state = self.calls.entry(call).or_default();
        match state.queue.pop_front() {
            Some(message) => Poll::Ready(Next::Message(message)),
            None if state.ended => {
                self.calls.remove(&call);
                Poll::Ready(Next::Done(Ok(())))
            }
            None => Poll::Pending,
        }
    }

    fn on_cancel(&mut self, ctx: &mut CallContext<'_>) {
        let call = ctx.id;
        self.cancelled.push(call);
        self.calls.remove(&call);
    }
}

struct Pair {
    client: Client,
    server: Server,
    handler: TestHandler,
}

fn pair(client: Compression, server: Compression) -> Pair {
    Pair {
        client: Client::new(ClientConfig {
            compression: client,
            ..ClientConfig::default()
        }),
        server: Server::new(ServerConfig {
            compression: server,
            ..ServerConfig::default()
        }),
        handler: TestHandler::default(),
    }
}

impl Pair {
    /// Exchange bytes until both sides are idle; the number of bytes that
    /// went from the client to the server and back.
    fn pump(&mut self) -> (usize, usize) {
        let cx = &mut Context::from_waker(Waker::noop());
        let (mut up, mut down) = (0, 0);
        for _ in 0..256 {
            self.server.poll(&mut self.handler, cx);
            if !self.client.has_output() && !self.server.has_output() {
                return (up, down);
            }
            let out = self.client.take_output();
            up += out.len();
            self.server.recv(&out, &mut self.handler).unwrap();
            self.server.poll(&mut self.handler, cx);
            let out = self.server.take_output();
            down += out.len();
            self.client.recv(&out).unwrap();
        }
        panic!("connection did not settle");
    }

    /// One unary echo; the result and the bytes sent up and down.
    fn unary(&mut self, request: &[u8]) -> (Result<Vec<u8>, Status>, usize, usize) {
        let id = self.client.start_unary("/t.T/Echo", request).unwrap();
        let (up, down) = self.pump();
        (
            self.client
                .take_response(id)
                .unwrap()
                .map(Response::into_message),
            up,
            down,
        )
    }
}

/// Handshake bytes in each direction, to subtract from measurements.
fn baseline(client: Compression, server: Compression) -> (usize, usize) {
    let mut p = pair(client, server);
    let id = p.client.start_unary("/t.T/Echo", b"").unwrap();
    let sizes = p.pump();
    assert_eq!(
        p.client
            .take_response(id)
            .map(|r| r.map(Response::into_message)),
        Some(Ok(Vec::new()))
    );
    sizes
}

const BIG: usize = 3000;

#[test]
fn unary_is_compressed_in_both_directions() {
    let mut p = pair(rle(), rle());
    let (reply, up, down) = p.unary(&[7; BIG]);
    assert_eq!(reply, Ok(vec![7; BIG]));
    assert!(up < 500, "request was not compressed: {up} bytes");
    assert!(down < 500, "response was not compressed: {down} bytes");
}

#[test]
fn compression_is_off_unless_configured() {
    let mut p = pair(Compression::NONE, Compression::NONE);
    let (reply, up, down) = p.unary(&[7; BIG]);
    assert_eq!(reply, Ok(vec![7; BIG]));
    assert!(up > BIG && down > BIG);
}

#[test]
fn server_compresses_only_what_the_client_accepts() {
    // The client compresses requests but did not say it can read responses.
    let send_only = Compression::NONE.send(&RLE).min_size(8);
    let mut p = pair(send_only, rle());
    let (reply, up, down) = p.unary(&[7; BIG]);
    assert_eq!(reply, Ok(vec![7; BIG]));
    assert!(up < 500, "{up}");
    assert!(
        down > BIG,
        "response was compressed for a client that cannot read it"
    );

    // The client accepts rle but sends plain requests; a server that can
    // decode but not compress still works.
    let accept_only = Compression::new(&ACCEPT_RLE);
    let mut p = pair(accept_only, accept_only);
    let (reply, up, down) = p.unary(&[7; BIG]);
    assert_eq!(reply, Ok(vec![7; BIG]));
    assert!(up > BIG && down > BIG);

    // A plain client against a compressing server.
    let mut p = pair(Compression::NONE, rle());
    let (reply, up, down) = p.unary(&[7; BIG]);
    assert_eq!(reply, Ok(vec![7; BIG]));
    assert!(up > BIG && down > BIG);
}

#[test]
fn small_and_incompressible_messages_are_sent_as_is() {
    let mut p = pair(rle(), rle());
    for message in [b"tiny".to_vec(), (0..=255).collect(), Vec::new()] {
        let (reply, ..) = p.unary(&message);
        assert_eq!(reply, Ok(message));
    }
}

#[test]
fn server_without_the_encoding_answers_unimplemented() {
    for server in [Compression::NONE, Compression::NONE.send(&RLE)] {
        let mut p = pair(rle(), server);
        let (reply, ..) = p.unary(&[7; BIG]);
        assert_eq!(reply.unwrap_err().code, Code::Unimplemented);
        assert!(p.handler.cancelled.is_empty());
        assert_eq!(p.server.active_calls(), 0);
    }
}

#[test]
fn streaming_calls_compress_each_message() {
    let mut p = pair(rle(), rle());
    let id = p.client.start_streaming("/t.T/Stream").unwrap();
    let messages: Vec<Vec<u8>> = vec![
        vec![1; BIG],
        b"tiny".to_vec(),
        (0..=255).collect(),
        vec![2; BIG],
        Vec::new(),
    ];
    for m in &messages {
        p.client.send_message(id, m).unwrap();
    }
    p.client.close_send(id).unwrap();
    let (up, down) = p.pump();
    assert!(up < 1500 && down < 1500, "{up} {down}");
    let mut got = Vec::new();
    let status = loop {
        match p.client.try_next(id).expect("call finished") {
            Next::Message(m) => got.push(m),
            Next::Done(status) => break status,
        }
    };
    assert_eq!(status, Ok(()));
    assert_eq!(got, messages);
}

#[test]
fn compressed_streams_use_flow_control_by_wire_size() {
    // Far more than a window of decompressed data, and more than a window of
    // compressed data: credit that disagrees with the bytes received would
    // either stall the stream or break the window.
    const COUNT: usize = 3000;
    let mut p = pair(rle(), rle());
    let id = p.client.start_streaming("/t.T/Stream").unwrap();
    let mut received = 0;
    let mut wire_up = 0;
    for i in 0..COUNT {
        let message = vec![i as u8; 4000];
        p.client.send_message(id, &message).unwrap();
        if i % 25 == 24 {
            wire_up += p.pump().0;
            while let Some(next) = p.client.try_next(id) {
                let Next::Message(m) = next else {
                    panic!("call ended early: {next:?}");
                };
                assert_eq!(m, vec![received as u8; 4000]);
                received += 1;
            }
        }
    }
    p.client.close_send(id).unwrap();
    wire_up += p.pump().0;
    while let Some(next) = p.client.try_next(id) {
        match next {
            Next::Message(m) => {
                assert_eq!(m, vec![received as u8; 4000]);
                received += 1;
            }
            Next::Done(status) => assert_eq!(status, Ok(())),
        }
    }
    assert_eq!(received, COUNT);
    assert!(wire_up > 65_535, "test must exceed one window: {wire_up}");
    assert!(
        wire_up < COUNT * 400,
        "requests were not compressed: {wire_up}"
    );
    assert_eq!(p.server.active_calls(), 0);
    assert!(p.handler.cancelled.is_empty());
}

#[test]
fn compressed_request_buffering_stays_within_the_window() {
    // The server only takes requests while its responses can be sent. The
    // client never reads, so after a few echoes the server holds the rest of
    // the requests, compressed, and must keep withholding their credit by
    // their size on the wire. Crediting the decompressed size instead would
    // let the client send far more than one window.
    let server = Compression::new(&ACCEPT_RLE);
    let mut p = pair(rle(), server);
    let id = p.client.start_streaming("/t.T/Stream").unwrap();
    for i in 0..6000 {
        p.client.send_message(id, &vec![i as u8; 4000]).unwrap();
    }
    p.pump();
    let window = 65_535;
    let buffered = p.server.buffered_request_bytes(id).unwrap();
    assert!(buffered > window / 2, "the window was not used: {buffered}");
    assert!(
        buffered <= window + lpm::wire_limit(4096) + lpm::HEADER_LEN,
        "server buffers {buffered} bytes"
    );
    assert!(
        p.client.queued_request_bytes(id).unwrap() > 0,
        "client sent everything"
    );
}

#[test]
fn client_failures_are_reported_per_call() {
    // The server answers with an encoding the client did not enable.
    let mut client = Client::new(ClientConfig::default());
    let mut server = Connection::server(Default::default());
    let id = client.start_unary("/t.T/Echo", b"hi").unwrap();
    server.recv(&client.take_output()).unwrap();
    let stream = core::iter::from_fn(|| server.poll_event())
        .find_map(|e| match e {
            Event::Headers { stream_id, .. } => Some(stream_id),
            _ => None,
        })
        .unwrap();
    let reply = lpm::frame(&[1; 100], Some(&RLE), 0).unwrap();
    server
        .send_headers(
            stream,
            vec![
                hf(":status", "200"),
                hf("content-type", "application/grpc"),
                hf("grpc-encoding", "rle"),
            ],
            false,
        )
        .unwrap();
    server.send_data(stream, reply, false).unwrap();
    server
        .send_headers(stream, vec![hf("grpc-status", "0")], true)
        .unwrap();
    client.recv(&server.take_output()).unwrap();
    let status = client.take_response(id).unwrap().unwrap_err();
    assert_eq!(status.code, Code::Internal, "{status}");
    assert!(status.message.contains("grpc-encoding"), "{status}");
}

// ---------------------------------------------------------------------------
// Raw peer: exact headers and framing
// ---------------------------------------------------------------------------

fn hf(name: &str, value: &str) -> HeaderField {
    HeaderField {
        name: name.into(),
        value: value.into(),
    }
}

fn request(path: &str, extra: &[(&str, &str)]) -> Vec<HeaderField> {
    let mut headers = vec![
        hf(":method", "POST"),
        hf(":scheme", "http"),
        hf(":path", path),
        hf(":authority", "localhost"),
        hf("content-type", "application/grpc"),
        hf("te", "trailers"),
    ];
    headers.extend(extra.iter().map(|(n, v)| hf(n, v)));
    headers
}

fn value<'a>(headers: &'a [HeaderField], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| h.name == name)
        .map(|h| h.value.as_str())
}

/// The events a raw HTTP/2 peer sees after sending `headers` and `body`.
fn raw_call(server: Compression, headers: Vec<HeaderField>, body: &[u8]) -> Vec<Event> {
    let mut conn = Connection::client(Default::default());
    let mut server = Server::new(ServerConfig {
        compression: server,
        ..ServerConfig::default()
    });
    let mut handler = TestHandler::default();
    let id = conn.open_stream(headers, false).unwrap();
    conn.send_data(id, body.to_vec(), true).unwrap();
    for _ in 0..8 {
        server.recv(&conn.take_output(), &mut handler).unwrap();
        conn.recv(&server.take_output()).unwrap();
    }
    core::iter::from_fn(|| conn.poll_event()).collect()
}

fn final_status(events: &[Event]) -> Option<&str> {
    events.iter().rev().find_map(|e| match e {
        Event::Headers { headers, .. } => value(headers, "grpc-status"),
        _ => None,
    })
}

fn response_data(events: &[Event]) -> Vec<u8> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Data { data, .. } => Some(data.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

#[test]
fn unsupported_request_encoding_lists_what_is_accepted() {
    let ev = raw_call(
        rle(),
        request("/t.T/Echo", &[("grpc-encoding", "gzip")]),
        &lpm::encode(b"x").unwrap(),
    );
    let Event::Headers {
        headers,
        end_stream: true,
        ..
    } = &ev[0]
    else {
        panic!("{ev:?}");
    };
    assert_eq!(value(headers, "grpc-status"), Some("12"));
    assert_eq!(value(headers, "grpc-accept-encoding"), Some("rle,identity"));
    assert_eq!(ev.len(), 1, "{ev:?}");

    // With nothing configured there is nothing to list.
    let ev = raw_call(
        Compression::NONE,
        request("/t.T/Echo", &[("grpc-encoding", "rle")]),
        &lpm::encode(b"x").unwrap(),
    );
    let Event::Headers { headers, .. } = &ev[0] else {
        panic!("{ev:?}");
    };
    assert_eq!(value(headers, "grpc-status"), Some("12"));
    assert_eq!(value(headers, "grpc-accept-encoding"), None);
}

#[test]
fn identity_and_uncompressed_messages_are_always_accepted() {
    for server in [Compression::NONE, rle()] {
        for encoding in ["identity", "rle"] {
            if encoding == "rle" && !server.is_enabled() {
                continue;
            }
            let ev = raw_call(
                server,
                request("/t.T/Echo", &[("grpc-encoding", encoding)]),
                &lpm::encode(b"plain").unwrap(),
            );
            assert_eq!(final_status(&ev), Some("0"), "{encoding}: {ev:?}");
            assert_eq!(response_data(&ev), lpm::encode(b"plain").unwrap());
        }
    }
}

#[test]
fn compressed_request_gets_a_compressed_response() {
    let data = vec![3; 500];
    let ev = raw_call(
        rle(),
        request(
            "/t.T/Echo",
            &[
                ("grpc-encoding", "rle"),
                ("grpc-accept-encoding", "gzip, rle"),
            ],
        ),
        &lpm::frame(&data, Some(&RLE), 8).unwrap(),
    );
    let Event::Headers { headers, .. } = &ev[0] else {
        panic!("{ev:?}");
    };
    assert_eq!(value(headers, ":status"), Some("200"));
    assert_eq!(value(headers, "grpc-encoding"), Some("rle"));
    assert_eq!(value(headers, "grpc-accept-encoding"), Some("rle,identity"));
    assert_eq!(final_status(&ev), Some("0"));
    let wire = response_data(&ev);
    assert_eq!(wire[0], 1, "response message is not flagged as compressed");
    assert!(wire.len() < 50);
    assert_eq!(lpm::decode_unary(&wire, 4096, Some(&RLE)), Ok(data));
}

#[test]
fn response_stays_plain_when_the_client_does_not_accept_the_encoding() {
    for accept in [&[][..], &[("grpc-accept-encoding", "gzip")]] {
        let mut extra = vec![("grpc-encoding", "rle")];
        extra.extend_from_slice(accept);
        let ev = raw_call(
            rle(),
            request("/t.T/Echo", &extra),
            &lpm::frame(&[3; 500], Some(&RLE), 8).unwrap(),
        );
        let Event::Headers { headers, .. } = &ev[0] else {
            panic!("{ev:?}");
        };
        assert_eq!(value(headers, "grpc-encoding"), None, "{accept:?}");
        assert_eq!(response_data(&ev), lpm::encode(&[3; 500]).unwrap());
    }
}

#[test]
fn compressed_flag_needs_an_encoding() {
    let flagged = lpm::frame(&[3; 100], Some(&RLE), 0).unwrap();
    assert_eq!(flagged[0], 1);
    for extra in [&[][..], &[("grpc-encoding", "identity")]] {
        let ev = raw_call(rle(), request("/t.T/Echo", extra), &flagged);
        assert_eq!(final_status(&ev), Some("13"), "{extra:?}: {ev:?}");
    }
}

#[test]
fn bad_compressed_requests_fail_the_call() {
    let encoding = [("grpc-encoding", "rle")];
    // Corrupt payload.
    let ev = raw_call(rle(), request("/t.T/Echo", &encoding), &[1, 0, 0, 0, 1, 9]);
    assert_eq!(final_status(&ev), Some("13"), "{ev:?}");
    // A bomb: small on the wire, huge when decompressed.
    let ev = raw_call(
        rle(),
        request("/t.T/Echo", &encoding),
        &lpm::frame(&[0; 100_000], Some(&RLE), 0).unwrap(),
    );
    assert_eq!(final_status(&ev), Some("8"), "{ev:?}");
    // A codec that ignores the limit is stopped all the same.
    static ACCEPT_BROKEN: [&dyn Codec; 1] = [&RLE_NO_LIMIT];
    let ev = raw_call(
        Compression::new(&ACCEPT_BROKEN),
        request("/t.T/Echo", &encoding),
        &lpm::frame(&[0; 100_000], Some(&RLE_NO_LIMIT), 0).unwrap(),
    );
    assert_eq!(final_status(&ev), Some("8"), "{ev:?}");
}

#[test]
fn broken_compressed_stream_message_cancels_the_streaming_call() {
    let mut conn = Connection::client(Default::default());
    let mut server = Server::new(ServerConfig {
        compression: rle(),
        ..ServerConfig::default()
    });
    let mut handler = TestHandler::default();
    let id = conn
        .open_stream(request("/t.T/Stream", &[("grpc-encoding", "rle")]), false)
        .unwrap();
    let mut body = lpm::frame(&[4; 100], Some(&RLE), 0).unwrap();
    body.extend_from_slice(&[1, 0, 0, 0, 1, 9]);
    conn.send_data(id, body, false).unwrap();
    let cx = &mut Context::from_waker(Waker::noop());
    for _ in 0..8 {
        server.poll(&mut handler, cx);
        server.recv(&conn.take_output(), &mut handler).unwrap();
        server.poll(&mut handler, cx);
        conn.recv(&server.take_output()).unwrap();
    }
    let ev: Vec<Event> = core::iter::from_fn(|| conn.poll_event()).collect();
    assert_eq!(final_status(&ev), Some("13"), "{ev:?}");
    assert_eq!(handler.cancelled, [id]);
    assert_eq!(server.active_calls(), 0);
}

// ---------------------------------------------------------------------------
// The stock backend
// ---------------------------------------------------------------------------

#[cfg(feature = "miniz-oxide")]
mod miniz {
    use std::io::{Read, Write};

    use super::*;
    use crate::compression::GZIP;

    /// Text-like data that DEFLATE compresses well, and that is not a run.
    fn text(len: usize) -> Vec<u8> {
        b"the quick brown fox jumps over the lazy dog. "
            .iter()
            .copied()
            .cycle()
            .take(len)
            .collect()
    }

    #[test]
    fn compression_gzip_accepts_and_sends_gzip() {
        let c = Compression::gzip();
        assert_eq!(c.accept_header().as_deref(), Some("gzip,identity"));
        assert_eq!(c.send.unwrap().name(), "gzip");
        assert!(c.is_enabled());
    }

    #[test]
    fn round_trips_every_level_and_size() {
        for level in [0, 1, 6, 9, 10, 99] {
            let codec = Gzip::new(crate::compression::MinizOxide::new(level));
            for size in [0, 1, 100, 4096, 70_000] {
                let data = text(size);
                let packed = compress(&codec, &data);
                assert_eq!(
                    decompress(&codec, &packed, size),
                    Ok(data),
                    "{level} {size}"
                );
            }
        }
    }

    #[test]
    fn interoperates_with_flate2() {
        for size in [0, 10, 3000, 100_000] {
            let data = text(size);

            // Ours, read by flate2.
            let packed = compress(&GZIP, &data);
            let mut unpacked = Vec::new();
            flate2::read::GzDecoder::new(&packed[..])
                .read_to_end(&mut unpacked)
                .unwrap();
            assert_eq!(unpacked, data, "decoding ours, {size}");

            // flate2's, read by ours.
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&data).unwrap();
            let packed = encoder.finish().unwrap();
            assert_eq!(
                decompress(&GZIP, &packed, size),
                Ok(data),
                "encoding, {size}"
            );
        }
    }

    #[test]
    fn gzip_bomb_and_corruption_are_detected() {
        let bomb = compress(&GZIP, &vec![0; 10_000_000]);
        assert!(bomb.len() < 20_000);
        assert_eq!(decompress(&GZIP, &bomb, 4096), Err(CodecError::TooLarge));

        let good = compress(&GZIP, &text(1000));
        let n = good.len();
        let mut bad = good.clone();
        bad[n - 8] ^= 1;
        assert_eq!(decompress(&GZIP, &bad, 4096), Err(CodecError::Corrupt));
        assert_eq!(
            decompress(&GZIP, &good[..n - 20], 4096),
            Err(CodecError::Corrupt)
        );
        let mut bad = good.clone();
        bad[15] ^= 0xff;
        assert_eq!(decompress(&GZIP, &bad, 4096), Err(CodecError::Corrupt));
        assert_eq!(
            decompress(&GZIP, b"not gzip at all", 4096),
            Err(CodecError::Corrupt)
        );
    }

    #[test]
    fn gzip_rejects_suffixes_and_concatenated_identical_members() {
        for data in [Vec::new(), text(1000)] {
            let good = compress(&GZIP, &data);
            let mut suffix = good.clone();
            suffix.splice(good.len() - 8..good.len() - 8, [0, 1, 2, 3]);
            let concatenated = [good.clone(), good].concat();
            for bad in [suffix, concatenated] {
                assert_eq!(
                    decompress(&GZIP, &bad, data.len()),
                    Err(CodecError::Corrupt)
                );
                let mut framed = vec![1];
                framed.extend_from_slice(&u32::try_from(bad.len()).unwrap().to_be_bytes());
                framed.extend_from_slice(&bad);
                let mut decoder = decoder(4096, Some(&GZIP));
                decoder.push(&framed);
                decoder.push(&lpm::encode(b"after").unwrap());
                assert_eq!(decoder.next().unwrap().unwrap_err().code, Code::Internal);
                assert_eq!(decoder.message_count(), 0);
                assert_eq!(decoder.buffered(), 0);
                let events = raw_call(
                    Compression::gzip(),
                    request("/t.T/Echo", &[("grpc-encoding", "gzip")]),
                    &framed,
                );
                assert_eq!(final_status(&events), Some("13"), "{events:?}");
                assert!(
                    response_data(&events).is_empty(),
                    "truncated message delivered"
                );
            }
        }
    }

    #[test]
    fn gzip_empty_exact_limit_and_truncated_deflate() {
        for level in [0, 1, 6, 10] {
            let codec = Gzip::new(crate::compression::MinizOxide::new(level));
            for size in [0, 1, 2, 31, 32, 33, 1000, 4096] {
                let data = text(size);
                let packed = compress(&codec, &data);
                let mut out = b"prefix".to_vec();
                codec.decompress(&packed, &mut out, size).unwrap();
                assert_eq!(&out[..6], b"prefix");
                assert_eq!(&out[6..], data);
                if size > 0 {
                    assert_eq!(
                        decompress(&codec, &packed, size - 1),
                        Err(CodecError::TooLarge)
                    );
                }
                for body_end in 10..packed.len() - 8 {
                    let bad = [&packed[..body_end], &packed[packed.len() - 8..]].concat();
                    assert_eq!(
                        decompress(&codec, &bad, size),
                        Err(CodecError::Corrupt),
                        "{level} {size} {body_end}"
                    );
                }
            }
        }
    }

    #[test]
    fn unary_calls_use_gzip() {
        let mut p = pair(Compression::gzip(), Compression::gzip());
        let data = text(BIG);
        let (reply, up, down) = p.unary(&data);
        assert_eq!(reply, Ok(data));
        let (base_up, base_down) = baseline(Compression::gzip(), Compression::gzip());
        assert!(up - base_up < BIG / 4, "request not compressed: {up}");
        assert!(
            down - base_down < BIG / 4,
            "response not compressed: {down}"
        );
    }

    #[test]
    fn server_reads_gzip_written_by_flate2() {
        let data = text(2000);
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&data).unwrap();
        let packed = encoder.finish().unwrap();
        let mut body = vec![1];
        body.extend_from_slice(&(packed.len() as u32).to_be_bytes());
        body.extend_from_slice(&packed);

        let ev = raw_call(
            Compression::gzip(),
            request(
                "/t.T/Echo",
                &[("grpc-encoding", "gzip"), ("grpc-accept-encoding", "gzip")],
            ),
            &body,
        );
        assert_eq!(final_status(&ev), Some("0"), "{ev:?}");
        // The response is gzip as well, and flate2 can read it.
        let wire = response_data(&ev);
        assert_eq!(wire[0], 1);
        let mut unpacked = Vec::new();
        flate2::read::GzDecoder::new(&wire[5..])
            .read_to_end(&mut unpacked)
            .unwrap();
        assert_eq!(unpacked, data);
    }
}
