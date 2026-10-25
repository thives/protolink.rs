//! Contracts of the pull API that the differential oracle cannot see: where the
//! frame boundary falls, when END_STREAM is published, how credit moves when a
//! stream is cancelled part way through a frame, and how many notices a GOAWAY
//! can produce at once.

use protolink_http2::{
    Config, Connection, Error, ErrorCode, Event, FlowControl, HeaderField, Step,
};

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const DATA: u8 = 0x0;
const HEADERS: u8 = 0x1;
const RST_STREAM: u8 = 0x3;
const SETTINGS: u8 = 0x4;
const PING: u8 = 0x6;
const GOAWAY: u8 = 0x7;
const WINDOW_UPDATE: u8 = 0x8;
const CONTINUATION: u8 = 0x9;

const END_STREAM: u8 = 0x1;
const ACK: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
const PADDED: u8 = 0x8;

fn frame(kind: u8, flags: u8, id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    let mut out = vec![(len >> 16) as u8, (len >> 8) as u8, len as u8, kind, flags];
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// HPACK literals without indexing, as a plain peer would send them.
fn block(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, value) in fields {
        out.push(0x00);
        for s in [name, value] {
            out.push(s.len() as u8);
            out.extend_from_slice(s.as_bytes());
        }
    }
    out
}

const REQUEST: &[(&str, &str)] = &[
    (":method", "POST"),
    (":scheme", "http"),
    (":path", "/m.S/Call"),
    (":authority", "localhost"),
];

fn field(name: &str, value: &str) -> HeaderField {
    HeaderField {
        name: name.into(),
        value: value.into(),
    }
}

/// A server with the peer's preface and SETTINGS received, and a request on
/// stream 1 (headers only, not ended).
fn server(config: Config) -> Connection {
    let mut s = Connection::server(config);
    let mut wire = PREFACE.to_vec();
    wire.extend(frame(SETTINGS, 0, 0, &[]));
    wire.extend(frame(HEADERS, END_HEADERS, 1, &block(REQUEST)));
    s.recv(&wire).unwrap();
    while s.poll_event().is_some() {}
    s.take_output();
    s
}

/// Every event `recv_step` yields for `input`, as `(kind, stream, bytes, end)`
/// tuples, and the number of bytes consumed in each call.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Data(u32, Vec<u8>),
    DataEnd(u32, bool),
    Reset(u32, ErrorCode),
    Other,
}

fn step(
    conn: &mut Connection,
    input: &[u8],
) -> Result<(usize, Option<Seen>), protolink_http2::Error> {
    let Step { consumed, event } = conn.recv_step(input)?;
    let seen = event.map(|e| match e {
        Event::Data { stream_id, data } => Seen::Data(stream_id, data.into_owned()),
        Event::DataEnd {
            stream_id,
            end_stream,
        } => Seen::DataEnd(stream_id, end_stream),
        Event::Reset {
            stream_id,
            error_code,
        } => Seen::Reset(stream_id, error_code),
        _ => Seen::Other,
    });
    Ok((consumed, seen))
}

/// Pull every event from `input`, asserting that each call uses the input it
/// reports, and that a call without an event uses all of it.
fn drain(conn: &mut Connection, mut input: &[u8]) -> Vec<Seen> {
    let mut out = Vec::new();
    loop {
        let (consumed, seen) = step(conn, input).unwrap();
        assert!(consumed <= input.len());
        input = &input[consumed..];
        match seen {
            Some(seen) => out.push(seen),
            None => {
                assert!(
                    input.is_empty(),
                    "no event, yet {} bytes unused",
                    input.len()
                );
                return out;
            }
        }
    }
}

/// `(type, flags, stream, payload)` of each frame in `output`.
fn frames(output: &[u8]) -> Vec<(u8, u8, u32, Vec<u8>)> {
    let mut out = Vec::new();
    let mut rest = output;
    while !rest.is_empty() {
        assert!(rest.len() >= 9, "truncated output frame header: {rest:?}");
        let len = (usize::from(rest[0]) << 16) | (usize::from(rest[1]) << 8) | usize::from(rest[2]);
        assert!(rest.len() >= 9 + len, "truncated output frame payload");
        let id = u32::from_be_bytes([rest[5] & 0x7f, rest[6], rest[7], rest[8]]);
        out.push((rest[3], rest[4], id, rest[9..9 + len].to_vec()));
        rest = &rest[9 + len..];
    }
    out
}

fn window_credit(output: &[u8], stream: u32) -> u32 {
    frames(output)
        .iter()
        .filter(|(kind, _, id, _)| *kind == WINDOW_UPDATE && *id == stream)
        .map(|(_, _, _, p)| u32::from_be_bytes([p[0], p[1], p[2], p[3]]) & 0x7fff_ffff)
        .sum()
}

#[test]
fn data_completes_after_its_trailing_padding() {
    let mut s = server(Config::default());
    // Pad length 5, body 3 bytes, padding 5 bytes. END_STREAM on the frame.
    let mut payload = vec![5, 1, 2, 3];
    payload.extend([0; 5]);
    let wire = frame(DATA, PADDED | END_STREAM, 1, &payload);
    // Stop after the body: the padding is still on the wire.
    let split = 9 + 1 + 3;
    let before = drain(&mut s, &wire[..split]);
    assert_eq!(before, vec![Seen::Data(1, vec![1, 2, 3])]);

    // The server ends its side now. The stream stays only if the peer's END_STREAM
    // has not been published yet, so this shows when it is.
    s.send_headers(1, vec![field(":status", "200")], false)
        .unwrap();
    s.send_data(1, Vec::new(), true).unwrap();
    assert!(
        s.has_stream(1),
        "END_STREAM is not published before the frame ends"
    );

    let after = drain(&mut s, &wire[split..]);
    assert_eq!(after, vec![Seen::DataEnd(1, true)]);
    assert!(!s.has_stream(1), "closed once both sides have ended");
}

#[test]
fn empty_and_padding_only_frames_complete_without_body() {
    let mut s = server(Config::default());
    let mut wire = frame(DATA, 0, 1, &[]);
    wire.extend(frame(DATA, PADDED, 1, &[2, 0, 0]));
    wire.extend(frame(DATA, PADDED | END_STREAM, 1, &[1, 0]));
    let got = drain(&mut s, &wire);
    assert_eq!(
        got,
        vec![
            Seen::DataEnd(1, false),
            Seen::DataEnd(1, false),
            Seen::DataEnd(1, true),
        ]
    );
}

#[test]
fn body_is_lent_in_pieces_and_the_frame_completes_once() {
    let mut s = server(Config::default());
    let wire = frame(DATA, 0, 1, &[7; 40]);
    let mut got = Vec::new();
    for piece in wire.chunks(5) {
        got.extend(drain(&mut s, piece));
    }
    let body: Vec<u8> = got
        .iter()
        .filter_map(|e| match e {
            Seen::Data(1, bytes) => Some(bytes.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(body, vec![7; 40]);
    let completions = got
        .iter()
        .filter(|e| matches!(e, Seen::DataEnd(..)))
        .count();
    assert_eq!(completions, 1);
}

#[test]
fn cancelling_mid_frame_returns_each_byte_of_credit_once() {
    let mut s = server(Config {
        flow_control: FlowControl::Manual,
        ..Config::default()
    });
    s.take_output();
    while s.poll_event().is_some() {}

    let data = frame(DATA, 0, 1, &[9; 300]);
    let (head, tail) = data.split_at(9 + 100);
    let first = drain(&mut s, head);
    assert_eq!(first, vec![Seen::Data(1, vec![9; 100])]);
    // The application gives up on the stream while the frame is half delivered.
    s.reset_stream(1, ErrorCode::Cancel).unwrap();
    // The reset is reported by the next receive, and the discarded tail is silent.
    let rest = drain(&mut s, tail);
    assert_eq!(rest, vec![Seen::Reset(1, ErrorCode::Cancel)]);

    let output = s.take_output();
    // The 100 delivered bytes return when the stream goes, and the 200 still on
    // the wire are discarded: their deferred credit is returned once when the
    // frame completes. 300 in all, never twice.
    assert_eq!(window_credit(&output, 0), 300);
    assert_eq!(window_credit(&output, 1), 0);
    // Bounded output frames, independent of chunking: the reset, the delivered
    // credit, and the deferred tail each serialize once.
    assert_eq!(frames(&output).len(), 3);
}

#[test]
fn settings_publish_nothing_until_the_frame_is_complete() {
    let mut s = server(Config::default());
    let mut payload = Vec::new();
    payload.extend(4u16.to_be_bytes());
    payload.extend(1000u32.to_be_bytes());
    payload.extend(2u16.to_be_bytes());
    payload.extend(2u32.to_be_bytes());
    let wire = frame(SETTINGS, 0, 0, &payload);
    let before = s.peer_settings().initial_window_size;

    // The first tuple is valid and complete; the second is not yet.
    let (consumed, seen) = step(&mut s, &wire[..9 + 6]).unwrap();
    assert_eq!((consumed, seen), (9 + 6, None));
    assert_eq!(s.peer_settings().initial_window_size, before);
    assert!(
        s.pending_output().is_empty(),
        "no ACK before the frame ends"
    );

    // The second tuple is ENABLE_PUSH = 2: invalid, so nothing is applied.
    let err = s.recv(&wire[9 + 6..]).unwrap_err();
    assert!(
        format!("{err:?}").contains("invalid SETTINGS value"),
        "{err:?}"
    );
    assert_eq!(s.peer_settings().initial_window_size, before);
    assert!(
        frames(&s.take_output())
            .iter()
            .all(|(kind, flags, _, _)| *kind != SETTINGS || *flags & ACK == 0)
    );
}

#[test]
fn a_valid_settings_frame_is_acknowledged_once_when_complete() {
    let mut s = server(Config::default());
    let mut payload = Vec::new();
    payload.extend(4u16.to_be_bytes());
    payload.extend(70_000u32.to_be_bytes());
    payload.extend(1u16.to_be_bytes());
    payload.extend(256u32.to_be_bytes());
    let wire = frame(SETTINGS, 0, 0, &payload);
    for piece in wire.chunks(3) {
        drain(&mut s, piece);
    }
    assert_eq!(s.peer_settings().initial_window_size, 70_000);
    assert_eq!(s.peer_settings().header_table_size, 256);
    let acks = frames(&s.take_output())
        .iter()
        .filter(|(kind, flags, _, _)| *kind == SETTINGS && *flags & ACK != 0)
        .count();
    assert_eq!(acks, 1);
}

#[test]
fn goaway_refuses_one_stream_per_step() {
    let mut c = Connection::client(Config::default());
    c.take_output();
    for _ in 0..50 {
        c.open_stream(
            vec![
                field(":method", "POST"),
                field(":scheme", "http"),
                field(":path", "/m.S/Call"),
                field(":authority", "localhost"),
            ],
            false,
        )
        .unwrap();
    }
    let mut wire = frame(SETTINGS, 0, 0, &[]);
    let mut payload = 1u32.to_be_bytes().to_vec();
    payload.extend(0u32.to_be_bytes());
    wire.extend(frame(GOAWAY, 0, 0, &payload));

    let mut refused = Vec::new();
    let mut input = wire.as_slice();
    loop {
        let (consumed, seen) = step(&mut c, input).unwrap();
        input = &input[consumed..];
        match seen {
            Some(Seen::Reset(id, ErrorCode::RefusedStream)) => refused.push(id),
            Some(_) => {}
            None => break,
        }
    }
    // Every stream above the last one the peer processed (stream 1).
    assert_eq!(refused.len(), 49);
    assert!(refused.iter().all(|id| *id > 1));
    // Stream 1 was processed by the peer and is still open, so the connection
    // is not yet done.
    assert_eq!(c.stream_count(), 1);
    assert!(!c.is_closed());
}

#[test]
fn an_oversized_frame_is_refused_from_its_header_alone() {
    let mut s = server(Config::default());
    let header = [0x00, 0x40, 0x01, DATA, 0, 0, 0, 0, 1];
    let err = s.recv(&header).unwrap_err();
    assert!(format!("{err:?}").contains("frame exceeds"), "{err:?}");
}

#[test]
fn an_oversized_header_block_is_refused_before_its_fragment() {
    let mut s = server(Config::default());
    // The first fragment fills one frame with table size updates, which cost
    // no decoded-list budget; the continuation declares more than the encoded
    // limit allows, and its header alone is enough to refuse it.
    let mut wire = frame(HEADERS, 0, 3, &vec![0x20u8; 16_384]);
    wire.extend([0x00, 0x04, 0x40, CONTINUATION, END_HEADERS, 0, 0, 0, 3]);
    let err = s.recv(&wire).unwrap_err();
    assert!(
        format!("{err:?}").contains("header block too large"),
        "{err:?}"
    );
}

#[test]
fn reset_frame_is_applied_when_its_payload_completes() {
    let mut s = server(Config::default());
    let rst = frame(RST_STREAM, 0, 1, &8u32.to_be_bytes());
    let (consumed, seen) = step(&mut s, &rst[..9 + 2]).unwrap();
    assert_eq!((consumed, seen), (9 + 2, None));
    assert!(s.has_stream(1));
    let got = drain(&mut s, &rst[9 + 2..]);
    assert_eq!(got, vec![Seen::Reset(1, ErrorCode::Cancel)]);
    assert!(!s.has_stream(1));
}

#[test]
fn ping_is_acknowledged_only_once_complete() {
    let mut s = server(Config::default());
    let ping = frame(PING, 0, 0, b"12345678");
    let (_, seen) = step(&mut s, &ping[..9 + 4]).unwrap();
    assert_eq!(seen, None);
    assert!(s.pending_output().is_empty());
    drain(&mut s, &ping[9 + 4..]);
    let out = frames(&s.take_output());
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].3, b"12345678".to_vec());
}

#[test]
fn unknown_frame_with_a_large_payload_is_skipped_across_pieces() {
    let mut s = server(Config::default());
    let mut wire = frame(0x42, 0, 0, &vec![1u8; 16_000]);
    wire.extend(frame(DATA, END_STREAM, 1, &[5; 4]));
    let mut got = Vec::new();
    for piece in wire.chunks(1000) {
        got.extend(drain(&mut s, piece));
    }
    assert_eq!(
        got.iter()
            .filter(|e| matches!(e, Seen::Data(1, bytes) if bytes == &vec![5u8; 4]))
            .count(),
        1
    );
    assert!(got.contains(&Seen::DataEnd(1, true)));
}

#[test]
fn cancelled_data_tail_returns_credit_once_when_fed_bytewise() {
    // Manual flow control with a 1 KiB pending-output budget and two usable
    // streams. A cancelled DATA tail that arrives one byte at a time must not
    // turn into one WINDOW_UPDATE per byte (the original per-chunk release
    // exhausted the budget after roughly 76 discarded bytes and failed both
    // streams).
    let mut s = two_streams(Config {
        flow_control: FlowControl::Manual,
        max_pending_output_bytes: 1024,
        ..Config::default()
    });

    // One 16,384-byte DATA frame on stream 1, without END_STREAM. Deliver exactly
    // one body byte, then the application gives up on the stream.
    let body = vec![9; 16_384];
    let data = frame(DATA, 0, 1, &body);
    let (consumed, first) = step(&mut s, &data[..9 + 1]).unwrap();
    assert_eq!(consumed, 9 + 1);
    assert_eq!(first, Some(Seen::Data(1, vec![9])));

    s.reset_stream(1, ErrorCode::Cancel).unwrap();

    // The reset notice surfaces first (consuming no input), then each remaining
    // body byte arrives on its own transport read, without draining output in
    // between.
    let (consumed, notice) = step(&mut s, &[]).unwrap();
    assert_eq!(consumed, 0);
    assert_eq!(notice, Some(Seen::Reset(1, ErrorCode::Cancel)));
    let mut resets = 1;
    let mut tail_events = 0;
    let tail = &data[9 + 1..];
    assert_eq!(tail.len(), 16_383);
    for i in 0..tail.len() {
        let (consumed, seen) = step(&mut s, &tail[i..i + 1]).unwrap();
        assert_eq!(consumed, 1);
        match seen {
            Some(Seen::Reset(1, ErrorCode::Cancel)) => {
                resets += 1;
            }
            Some(Seen::Data(..)) | Some(Seen::DataEnd(..)) => {
                tail_events += 1;
            }
            _ => {}
        }
    }
    assert_eq!(resets, 1, "exactly one reset notice");
    assert_eq!(tail_events, 0, "the discarded tail publishes nothing");
    assert!(
        s.failure().is_none(),
        "bounded credit must not hit the 1 KiB output limit"
    );

    let output = s.take_output();
    // One delivered byte plus 16,383 discarded body bytes: 16,384 returned exactly
    // once, all at connection level because stream 1 is already gone.
    assert_eq!(window_credit(&output, 0), 16_384);
    assert_eq!(window_credit(&output, 1), 0);
    // Constant across chunkings: the reset, the delivered credit, and one deferred
    // tail update. Nothing grows with the discarded byte count.
    assert_eq!(frames(&output).len(), 3);

    // The unrelated stream 3 stays usable for later traffic.
    assert!(s.has_stream(3));
}

#[test]
fn cancelled_tail_returns_credit_at_frame_completion_not_response_end() {
    // Manual flow control, no END_STREAM. A cancelled stream's first DATA frame
    // must hand its credit back once that frame completes, so a peer waiting on
    // that credit is not blocked until the whole response arrives, and a second
    // frame on the same (already removed) stream is still admitted and credited
    // exactly once.
    let mut s = server(Config {
        flow_control: FlowControl::Manual,
        ..Config::default()
    });

    // First DATA frame on stream 1: one byte delivered, then cancelled.
    let f1 = frame(DATA, 0, 1, &vec![9; 300]);
    let (consumed, first) = step(&mut s, &f1[..9 + 1]).unwrap();
    assert_eq!(consumed, 9 + 1);
    assert_eq!(first, Some(Seen::Data(1, vec![9])));
    s.reset_stream(1, ErrorCode::Cancel).unwrap();
    let (consumed, notice) = step(&mut s, &[]).unwrap();
    assert_eq!(
        (consumed, notice),
        (0, Some(Seen::Reset(1, ErrorCode::Cancel)))
    );
    // Feed the 299-byte tail of the frame one byte at a time.
    let tail = &f1[9 + 1..];
    assert_eq!(tail.len(), 299);
    for i in 0..tail.len() {
        let (consumed, _) = step(&mut s, &tail[i..i + 1]).unwrap();
        assert_eq!(consumed, 1);
    }
    // Frame 1 is complete: its credit is already out, one boundary early.
    let first_phase = s.take_output();
    assert_eq!(window_credit(&first_phase, 0), 300);
    assert!(s.failure().is_none());

    // A second frame on the same, already-removed stream is admitted at once
    // and credited exactly once at admission.
    let f2 = frame(DATA, 0, 1, &vec![7; 400]);
    step(&mut s, &f2).unwrap();
    let second_phase = s.take_output();
    assert_eq!(window_credit(&second_phase, 0), 400);
    assert!(s.failure().is_none());
}

// Ownership of abandoned DATA credit across admission, release, reset and completion.

/// Where the application gives up on the stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CancelAt {
    /// The frame is admitted, and no body octet has been delivered.
    Admission,
    /// Half of the body has been delivered.
    MidBody,
}

/// How much of the delivered prefix the application releases first.
#[derive(Clone, Copy, Debug)]
enum Prefix {
    None,
    Partial,
    Full,
}

/// How the remaining input is sliced.
#[derive(Clone, Copy, Debug)]
enum Cutting {
    Bytewise,
    Fixed,
    Whole,
    Irregular,
}

#[derive(Clone, Copy, Debug)]
struct FrameCase {
    manual: bool,
    retained: bool,
    padded: bool,
    pad: usize,
    body: usize,
    end_stream: bool,
    cancel: CancelAt,
    prefix: Prefix,
    cutting: Cutting,
}

#[test]
fn cancelled_data_frame_owns_credit_phase_by_phase() {
    // 64 named scenarios: flow-control mode, retained credit, padding shape,
    // END_STREAM and cancellation position are the Cartesian axes. Release and
    // slicing styles rotate across them for representative coverage, not every
    // pairwise combination.
    let prefixes = [Prefix::None, Prefix::Partial, Prefix::Full];
    let cuttings = [
        Cutting::Bytewise,
        Cutting::Fixed,
        Cutting::Whole,
        Cutting::Irregular,
    ];
    let pad_specs = [
        (false, 0usize),
        (true, 0usize),
        (true, 7usize),
        (true, 255usize),
    ];
    let mut rotation = 0;
    for manual in [true, false] {
        for retained in [true, false] {
            for (padded, pad) in pad_specs {
                for end_stream in [false, true] {
                    for cancel_at in [CancelAt::Admission, CancelAt::MidBody] {
                        let prefix = prefixes[rotation % 3];
                        let cutting = cuttings[rotation % 4];
                        rotation += 1;
                        cancelled_frame_owns_credit(FrameCase {
                            manual,
                            retained,
                            padded,
                            pad,
                            body: 40,
                            end_stream,
                            cancel: cancel_at,
                            prefix,
                            cutting,
                        });
                    }
                }
            }
        }
    }
    assert_eq!(rotation, 64, "the sweep must run its whole grid");
}

fn cancelled_frame_owns_credit(case: FrameCase) {
    let name = format!("{case:?}");
    let flow = if case.manual {
        FlowControl::Manual
    } else {
        FlowControl::Automatic
    };
    let mut s = two_streams(Config {
        flow_control: flow,
        ..Config::default()
    });
    if case.retained {
        s.retain_receive_capacity(1).unwrap();
    }

    let body = case.body;
    let padding = if case.padded { 1 + case.pad } else { 0 };
    let total = body + padding;
    // Credit returned when the frame is admitted: everything at once in
    // automatic mode, and in manual mode only the padding, which is never
    // handed to the application.
    let admission = if case.manual { padding } else { total };
    let mid = if case.cancel == CancelAt::MidBody {
        body / 2
    } else {
        0
    };

    let mut payload = Vec::new();
    if case.padded {
        payload.push(case.pad as u8);
    }
    payload.resize(payload.len() + body, 7);
    payload.resize(total, 0);
    let flags =
        (if case.padded { PADDED } else { 0 }) | (if case.end_stream { END_STREAM } else { 0 });
    let wire = frame(DATA, flags, 1, &payload);

    let mut conn_seen: u32 = 0;
    let mut stream_seen: u32 = 0;

    // Phase 1: the frame is admitted, and nothing of it is published.
    let admit_at = 9 + (if case.padded { 1 } else { 0 });
    let events = feed(&mut s, &wire[..admit_at], case.cutting);
    assert!(events.is_empty(), "{name}: admission publishes nothing");
    let out = s.take_output();
    assert_credit(
        &out,
        admission,
        if case.end_stream { 0 } else { admission },
        &format!("{name}: admission"),
    );
    conn_seen += window_credit(&out, 0);
    stream_seen += window_credit(&out, 1);
    assert_eq!(
        s.unreleased_recv_bytes(1),
        Some(0),
        "{name}: padding is never owed as body credit",
    );

    // Phase 2: the prefix is delivered as body, nothing else.
    let prefix_at = admit_at + mid;
    let got = feed(&mut s, &wire[admit_at..prefix_at], case.cutting);
    assert_eq!(delivered(&got), vec![7; mid], "{name}: delivered prefix");
    assert_eq!(completions(&got), 0, "{name}: no early completion");
    assert!(
        s.take_output().is_empty(),
        "{name}: delivery alone credits nothing"
    );

    // Phase 3: the application releases part of what it was handed.
    let released = match case.prefix {
        Prefix::None => 0,
        Prefix::Partial => mid / 2,
        Prefix::Full => mid,
    };
    if released > 0 {
        s.release_capacity(1, released);
    }
    let out = s.take_output();
    // Automatic mode already paid at admission; manual release pays both windows.
    let paid = if case.manual { released } else { 0 };
    assert_credit(&out, paid, paid, &format!("{name}: application release"));
    conn_seen += window_credit(&out, 0);
    stream_seen += window_credit(&out, 1);
    let held_by_app = if case.manual { mid - released } else { 0 };
    assert_eq!(
        s.unreleased_recv_bytes(1),
        Some(held_by_app),
        "{name}: unreleased prefix stays with the stream",
    );

    // Phase 4: the application gives up on the stream.
    s.reset_stream(1, ErrorCode::Cancel).unwrap();
    let out = s.take_output();
    assert_credit(
        &out,
        if case.retained { 0 } else { held_by_app },
        0,
        &format!("{name}: cleanup owns only unretained delivered bytes"),
    );
    assert_eq!(resets(&out, 1), 1, "{name}: one reset frame");
    conn_seen += window_credit(&out, 0);
    stream_seen += window_credit(&out, 1);
    assert_eq!(
        s.unreleased_recv_bytes(1),
        (case.retained && held_by_app > 0).then_some(held_by_app),
        "{name}: only retained ownership survives removal",
    );

    assert_reset_notice(&mut s, &name);

    // Phase 5: the tail arrives, still short of the frame's end.
    let tail_at = wire.len() - 1;
    let got = feed(&mut s, &wire[prefix_at..tail_at], case.cutting);
    assert!(
        got.is_empty(),
        "{name}: an incomplete discarded tail is silent"
    );
    let out = s.take_output();
    assert_credit(&out, 0, 0, &format!("{name}: incomplete tail"));
    conn_seen += window_credit(&out, 0);
    stream_seen += window_credit(&out, 1);
    for _ in 0..3 {
        assert_idle(&mut s, &name);
    }

    // Phase 6: the last octet completes the frame.
    let got = feed(&mut s, &wire[tail_at..], case.cutting);
    assert!(
        got.is_empty(),
        "{name}: discarded frame completion is silent"
    );
    let out = s.take_output();
    let tail = if case.manual { body - mid } else { 0 };
    assert_credit(&out, tail, 0, &format!("{name}: completed tail"));
    conn_seen += window_credit(&out, 0);
    stream_seen += window_credit(&out, 1);
    assert!(!s.has_stream(1), "{name}: the stream stays gone");

    // Retained credit belongs to the application even after removal.
    if case.retained && held_by_app > 0 {
        s.release_capacity(1, held_by_app);
        let out = s.take_output();
        assert_credit(&out, held_by_app, 0, &format!("{name}: retained release"));
        conn_seen += window_credit(&out, 0);
        stream_seen += window_credit(&out, 1);
        s.release_capacity(1, held_by_app);
        assert!(
            s.take_output().is_empty(),
            "{name}: a second retained release fabricates nothing"
        );
        assert_eq!(
            s.unreleased_recv_bytes(1),
            None,
            "{name}: the retained entry is gone with its credit"
        );
    }

    // Check aggregate credit as well as each phase's exact update vector.
    assert_eq!(
        conn_seen, total as u32,
        "{name}: every payload octet is credited once at connection level",
    );
    let stream_total = if !case.manual {
        if case.end_stream { 0 } else { total }
    } else if case.end_stream {
        released
    } else {
        padding + released
    };
    assert_eq!(
        stream_seen, stream_total as u32,
        "{name}: stream credit is admission plus application release",
    );

    // Isolation: stream 3 completes a real response after the abandoned frame.
    s.send_headers(3, vec![field(":status", "200")], false)
        .unwrap();
    s.send_data(3, b"still working".to_vec(), true).unwrap();
    assert!(
        s.failure().is_none(),
        "{name}: an abandoned frame must not cost the connection"
    );
    assert_eq!(
        s.queued_send_bytes(3),
        Some(0),
        "{name}: stream 3 is not blocked"
    );
    let out = s.take_output();
    assert_eq!(
        frames(&out)
            .iter()
            .filter(|(kind, _, id, _)| *kind == DATA && *id == 3)
            .map(|(_, _, _, payload)| payload.to_vec())
            .collect::<Vec<_>>(),
        vec![b"still working".to_vec()],
        "{name}: stream 3's payload survives",
    );
    assert_eq!(resets(&out, 1), 0, "{name}: nothing resets again later");
}

/// Like `two_streams`, but the peer advertises a smaller receive window, so a
/// local response can stay queued inside its stream instead of inside the output
/// buffer: that is how a reset ends up deferred rather than immediate.
fn two_streams_peer_window(config: Config, window: usize) -> Connection {
    let mut settings = vec![0, 4]; // SETTINGS_INITIAL_WINDOW_SIZE
    settings.extend((window as u32).to_be_bytes());
    two_streams_settings(config, &settings)
}

/// A server with the peer's preface, SETTINGS, and request headers on streams
/// 1 and 3, neither of them ended.
fn two_streams(config: Config) -> Connection {
    two_streams_settings(config, &[])
}

fn two_streams_settings(config: Config, settings: &[u8]) -> Connection {
    let mut s = Connection::server(config);
    let mut wire = PREFACE.to_vec();
    wire.extend(frame(SETTINGS, 0, 0, settings));
    wire.extend(frame(HEADERS, END_HEADERS, 1, &block(REQUEST)));
    wire.extend(frame(HEADERS, END_HEADERS, 3, &block(REQUEST)));
    s.recv(&wire).unwrap();
    while s.poll_event().is_some() {}
    s.take_output();
    assert!(s.failure().is_none());
    s
}

/// Queue an ended response against a 1024-byte peer window, leaving 176 bytes
/// blocked in the stream rather than in pending output.
#[track_caller]
fn queue_blocked_response(conn: &mut Connection, label: &str) {
    conn.send_headers(1, vec![field(":status", "200")], false)
        .unwrap();
    conn.send_data(1, vec![b'y'; 1200], true).unwrap();
    assert!(
        conn.failure().is_none(),
        "{label}: queued, not output-limited"
    );
    assert_eq!(conn.send_capacity(1), Some(0), "{label}: peer window spent");
    assert_eq!(
        conn.queued_send_bytes(1),
        Some(176),
        "{label}: response blocked"
    );
    conn.take_output();
}

/// Feed every byte, cycling awkward sizes for `Irregular` without allocating a
/// chunk-size list. Each drain still checks exact consumption within its chunk.
fn feed(conn: &mut Connection, mut bytes: &[u8], cutting: Cutting) -> Vec<Seen> {
    let mut out = Vec::new();
    let mut irregular = [7, 2, 13, 1, 29, 4].into_iter().cycle();
    while !bytes.is_empty() {
        let size = match cutting {
            Cutting::Bytewise => 1,
            Cutting::Fixed => 3,
            Cutting::Whole => bytes.len(),
            Cutting::Irregular => irregular.next().unwrap(),
        };
        let (chunk, rest) = bytes.split_at(size.min(bytes.len()));
        out.extend(drain(conn, chunk));
        bytes = rest;
    }
    out
}

/// The body bytes `events` delivered, in order.
fn delivered(events: &[Seen]) -> Vec<u8> {
    let mut out = Vec::new();
    for event in events {
        if let Seen::Data(_, bytes) = event {
            out.extend(bytes);
        }
    }
    out
}

/// DATA completion events in `events`.
fn completions(events: &[Seen]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, Seen::DataEnd(..)))
        .count()
}

/// WINDOW_UPDATE increments for `stream`, one entry per serialized frame.
fn update_incs(output: &[u8], stream: u32) -> Vec<u32> {
    frames(output)
        .iter()
        .filter(|(kind, _, id, _)| *kind == WINDOW_UPDATE && *id == stream)
        .map(|(_, _, _, payload)| {
            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff
        })
        .collect()
}

/// Assert exactly one update per nonzero credit, and none for zero credit.
#[track_caller]
fn assert_credit(output: &[u8], connection: usize, stream: usize, label: &str) {
    for (id, amount) in [(0, connection), (1, stream)] {
        let expected = (amount > 0).then_some(amount as u32);
        assert_eq!(
            update_incs(output, id),
            expected.as_slice(),
            "{label}: WINDOW_UPDATE stream={id}"
        );
    }
}

#[track_caller]
fn assert_reset_notice(conn: &mut Connection, label: &str) {
    assert_eq!(
        step(conn, &[]).unwrap(),
        (0, Some(Seen::Reset(1, ErrorCode::Cancel))),
        "{label}: one reset notice"
    );
}

#[track_caller]
fn assert_idle(conn: &mut Connection, label: &str) {
    assert_eq!(step(conn, &[]).unwrap(), (0, None), "{label}: empty poll");
    assert!(conn.take_output().is_empty(), "{label}: no extra output");
}

/// RST_STREAM frames for `stream` in `output`.
fn resets(output: &[u8], stream: u32) -> usize {
    frames(output)
        .iter()
        .filter(|(kind, _, id, _)| *kind == RST_STREAM && *id == stream)
        .count()
}

#[test]
fn a_frame_on_a_removed_stream_is_credited_whole_at_admission() {
    // A frame that arrives on an already-gone stream id is discarded as soon as
    // it is known, so its whole payload (padding included) is returned at admission
    // and completion owes nothing further. No reset may be manufactured for a
    // stream that is already gone.
    let mut s = two_streams(Config {
        flow_control: FlowControl::Manual,
        ..Config::default()
    });
    s.reset_stream(1, ErrorCode::Cancel).unwrap();
    assert_reset_notice(&mut s, "removed stream");
    let cancelled = s.take_output();
    assert!(
        update_incs(&cancelled, 0).is_empty(),
        "nothing had been delivered, so cleanup owes no credit"
    );
    assert_eq!(
        resets(&cancelled, 1),
        1,
        "the reset itself is serialized once"
    );

    // One pad byte, three body bytes, seven padding octets: 11 payload octets.
    let wire = frame(
        DATA,
        PADDED | END_STREAM,
        1,
        &[7, 1, 2, 3, 0, 0, 0, 0, 0, 0, 0],
    );
    let events = feed(&mut s, &wire[..9 + 1], Cutting::Whole);
    assert!(
        events.is_empty(),
        "a gone stream publishes nothing at admission"
    );
    let owed = s.take_output();
    assert_eq!(
        update_incs(&owed, 0),
        vec![11u32],
        "the whole payload is paid at admission",
    );
    assert!(
        update_incs(&owed, 1).is_empty(),
        "a gone stream gets no stream update"
    );
    assert_eq!(
        resets(&owed, 1),
        0,
        "no reset is manufactured for a stream that is already gone",
    );

    let rest = feed(&mut s, &wire[9 + 1..], Cutting::Bytewise);
    assert!(
        rest.is_empty(),
        "a discarded frame on a gone stream completes silently"
    );
    assert!(
        s.take_output().is_empty(),
        "admission already paid the whole payload"
    );
}

/// Cancel after one byte of a 300-byte frame; stop one byte before completion.
/// Keep output pending so the caller can test the final update against the cap.
fn cancelled_tail_ready(limit: usize) -> (Connection, Vec<u8>, Vec<u8>) {
    let mut s = two_streams(Config {
        flow_control: FlowControl::Manual,
        max_pending_output_bytes: limit,
        ..Config::default()
    });
    let wire = frame(DATA, 0, 1, &vec![9; 300]);
    let (consumed, first) = step(&mut s, &wire[..9 + 1]).unwrap();
    assert_eq!(
        (consumed, first),
        (10usize, Some(Seen::Data(1, vec![9]))),
        "the first octet is delivered",
    );
    s.reset_stream(1, ErrorCode::Cancel).unwrap();
    assert_reset_notice(&mut s, "cancelled tail setup");
    let tail = &wire[9 + 1..];
    assert_eq!(tail.len(), 299, "the rest of the frame is unread");
    let events = drain(&mut s, &tail[..298]);
    assert!(delivered(&events).is_empty(), "the tail is discarded");
    let prefix = s.pending_output().to_vec();
    assert_eq!(
        update_incs(&prefix, 0),
        vec![1],
        "the delivered byte is already owed",
    );
    assert_eq!(
        frames(&prefix).len(),
        2,
        "one reset frame and one credit frame",
    );
    (s, tail[298..].to_vec(), prefix)
}

#[test]
fn the_deferred_tail_update_needs_its_whole_frame() {
    // A WINDOW_UPDATE frame costs 9 + 4 = 13 bytes. Measuring the serialized
    // prefix once lets the same scenario be replayed with exactly that much room,
    // and then one byte less, where the limit latches instead of the credit being
    // lost, duplicated, or half-written.
    let (mut room_to_spare, last, prefix) = cancelled_tail_ready(usize::MAX);
    assert_eq!(
        last.len(),
        1,
        "the scenario stops one octet short of completion",
    );
    assert_eq!(prefix.len(), 26, "one reset frame plus the delivered byte");
    assert_eq!(
        room_to_spare.take_output(),
        prefix,
        "the measured prefix serializes first"
    );
    let (consumed, note) = step(&mut room_to_spare, &last).unwrap();
    assert_eq!((consumed, note), (1, None), "the frame completes silently");
    let owed = room_to_spare.take_output();
    assert_eq!(
        owed.len(),
        13,
        "the deferred tail update is exactly one 13-byte frame",
    );
    assert_eq!(
        update_incs(&owed, 0),
        vec![299],
        "the unread tail is credited once",
    );

    // Exactly enough room for that frame.
    let (mut fits, last_fits, prefix_fits) = cancelled_tail_ready(prefix.len() + 13);
    assert_eq!(
        prefix_fits, prefix,
        "the scenario must be identical up to the boundary",
    );
    assert_eq!(
        fits.take_output(),
        prefix_fits,
        "the same prefix serializes under the tight budget"
    );
    let (consumed, note) = step(&mut fits, &last_fits).unwrap();
    assert_eq!((consumed, note), (1, None));
    assert!(fits.failure().is_none(), "13 bytes of room is enough");
    assert_eq!(fits.take_output(), owed, "and serializes the same update");

    // One byte short.
    let (mut short, last_short, prefix_short) = cancelled_tail_ready(prefix.len() + 12);
    assert_eq!(prefix_short, prefix, "identical up to the boundary again");
    let expected = Error::OutputLimitExceeded {
        limit: prefix.len() + 12,
        pending: prefix.len(),
        attempted: 13,
    };
    assert_eq!(
        step(&mut short, &last_short).unwrap_err(),
        expected.clone(),
        "the tail update is refused whole",
    );
    assert_eq!(
        short.take_output(),
        prefix,
        "the serialized prefix is intact, byte for byte",
    );
    assert_eq!(
        short.failure(),
        Some(&expected),
        "the failure is sticky after draining",
    );
    assert_eq!(
        step(&mut short, &[]).unwrap_err(),
        expected.clone(),
        "draining cannot revive the connection",
    );
    assert!(
        update_incs(&short.take_output(), 0).is_empty(),
        "and the refused credit is never re-emitted"
    );
}

#[test]
fn an_unfinished_frame_never_flushes_its_snapshot_on_retirement() {
    // The tail snapshot belongs to the frame, not to the connection. Retiring the
    // transport while the frame is incomplete must not write that credit, and must
    // not invent a completion for bytes that never arrive.
    let mut s = server(Config {
        flow_control: FlowControl::Manual,
        ..Config::default()
    });
    let wire = frame(DATA, 0, 1, &vec![9; 300]);
    let (consumed, first) = step(&mut s, &wire[..9 + 1]).unwrap();
    assert_eq!(
        (consumed, first),
        (10usize, Some(Seen::Data(1, vec![9]))),
        "the first octet is delivered"
    );
    s.reset_stream(1, ErrorCode::Cancel).unwrap();
    assert_reset_notice(&mut s, "unfinished frame");

    // Half of the tail arrives. The snapshot is the original 299 unread body
    // octets at the discard transition, not just the bytes discarded so far.
    assert!(drain(&mut s, &wire[9 + 1..9 + 151]).is_empty());
    let out = s.take_output();
    assert_eq!(
        update_incs(&out, 0),
        vec![1],
        "cleanup owns only the delivered byte"
    );

    assert!(update_incs(&out, 1).is_empty());
    assert_eq!(resets(&out, 1), 1);
    assert_eq!(frames(&out).len(), 2);

    // No streams remain, so GOAWAY really closes this connection. No more
    // transport bytes arrive, and the incomplete frame never earns its credit.
    s.go_away(ErrorCode::NoError);
    assert!(s.is_closed(), "retirement completes graceful shutdown");
    assert!(s.failure().is_none());
    let out = s.take_output();
    assert_eq!(
        out,
        frame(GOAWAY, 0, 0, &[0, 0, 0, 1, 0, 0, 0, 0]),
        "one real GOAWAY, with no pending snapshot credit"
    );
    assert_eq!(frames(&out).len(), 1);
    for _ in 0..3 {
        assert_idle(&mut s, "retired connection");
    }
}

#[test]
fn empty_and_padding_only_frames_owe_their_padding_once() {
    // Padding is payload for flow-control purposes even when there is no body
    // at all: it is paid when the frame is admitted, and completing the frame
    // adds nothing.
    for manual in [true, false] {
        for end_stream in [false, true] {
            for (padded, pad) in [
                (false, 0usize),
                (true, 0usize),
                (true, 7usize),
                (true, 255usize),
            ] {
                let label =
                    format!("manual={manual} end_stream={end_stream} padded={padded} pad={pad}");
                let flow = if manual {
                    FlowControl::Manual
                } else {
                    FlowControl::Automatic
                };
                let mut s = two_streams(Config {
                    flow_control: flow,
                    ..Config::default()
                });
                let mut payload: Vec<u8> = Vec::new();
                if padded {
                    payload.push(pad as u8);
                    payload.resize(1 + pad, 0);
                }
                let flags =
                    (if padded { PADDED } else { 0 }) | (if end_stream { END_STREAM } else { 0 });
                let wire = frame(DATA, flags, 1, &payload);
                let total = if padded { 1 + pad } else { 0 };

                // Admission: everything the payload is worth, in one update per
                // level, is already owed once the pad byte is in.
                let admit_at = 9 + (if padded { 1 } else { 0 });
                let mut events = feed(&mut s, &wire[..admit_at], Cutting::Bytewise);
                let owed = s.take_output();
                assert_credit(&owed, total, if end_stream { 0 } else { total }, &label);
                events.extend(feed(&mut s, &wire[admit_at..], Cutting::Bytewise));
                assert_eq!(
                    events,
                    vec![Seen::DataEnd(1, end_stream)],
                    "{}: an empty body publishes only the completion",
                    label,
                );
                assert!(
                    s.take_output().is_empty(),
                    "{}: completion owes nothing further",
                    label,
                );
            }
        }
    }
}

#[test]
fn cancelling_during_trailing_padding_owes_nothing_extra() {
    // The body arrived in full, so what is left on the wire is padding only, and
    // that was already paid at admission. Nothing may snapshot unread body, and
    // a removed stream must not be resurrected to publish the completion.
    for pad in [5usize, 255usize] {
        let mut s = two_streams(Config {
            flow_control: FlowControl::Manual,
            ..Config::default()
        });
        let body: usize = 6;
        let mut payload = vec![pad as u8];
        payload.resize(1 + body, 7);
        payload.resize(1 + body + pad, 0);
        let wire = frame(DATA, PADDED, 1, &payload);
        let delivered_to = 9 + 1 + body;
        let events = feed(&mut s, &wire[..delivered_to], Cutting::Bytewise);
        assert_eq!(
            delivered(&events),
            vec![7; body],
            "pad={}: the body is delivered",
            pad,
        );
        assert_eq!(
            completions(&events),
            0,
            "pad={}: the padding is still on the wire",
            pad,
        );
        assert_eq!(
            update_incs(&s.take_output(), 0),
            vec![(1 + pad) as u32],
            "pad={}: the padding is paid at admission",
            pad,
        );

        s.reset_stream(1, ErrorCode::Cancel).unwrap();
        let out = s.take_output();
        assert_eq!(
            update_incs(&out, 0),
            vec![body as u32],
            "pad={}: cleanup owns the delivered body",
            pad,
        );
        assert!(
            update_incs(&out, 1).is_empty(),
            "pad={}: the removed stream gets no stream update",
            pad
        );
        assert_eq!(resets(&out, 1), 1, "pad={}: one reset frame", pad);

        assert_reset_notice(&mut s, &format!("pad={pad}"));

        let rest = feed(&mut s, &wire[delivered_to..], Cutting::Bytewise);
        assert!(
            rest.is_empty(),
            "pad={}: a removed stream is not resurrected for a completion event",
            pad
        );
        let out = s.take_output();
        assert!(
            update_incs(&out, 0).is_empty(),
            "pad={}: the padding owes nothing a second time",
            pad
        );
        assert!(!s.has_stream(1), "pad={}: the stream stays gone", pad);
    }
}

#[test]
fn aborting_a_deferred_reset_preserves_its_tail_snapshot_and_retained_prefix() {
    for abort_in_padding in [false, true] {
        for end_stream in [false, true] {
            let label = format!("abort_in_padding={abort_in_padding} end_stream={end_stream}");
            let mut s = two_streams_peer_window(
                Config {
                    flow_control: FlowControl::Manual,
                    ..Config::default()
                },
                1024,
            );
            s.retain_receive_capacity(1).unwrap();
            queue_blocked_response(&mut s, &label);

            let mut payload = vec![7];
            payload.extend([9; 40]);
            payload.extend([0; 7]);
            let wire = frame(
                DATA,
                PADDED | if end_stream { END_STREAM } else { 0 },
                1,
                &payload,
            );
            let prefix_at = 9 + 1 + 6;
            assert_eq!(
                step(&mut s, &wire[..prefix_at]).unwrap(),
                (prefix_at, Some(Seen::Data(1, vec![9; 6]))),
                "{label}"
            );
            let out = s.take_output();
            assert_credit(&out, 8, if end_stream { 0 } else { 8 }, &label);
            assert_eq!(
                frames(&out).len(),
                if end_stream { 1 } else { 2 },
                "{label}"
            );
            assert_eq!(s.unreleased_recv_bytes(1), Some(6), "{label}");

            s.reset_stream_after_flush(1, ErrorCode::NoError).unwrap();
            assert!(s.has_stream(1), "{label}: the reset really is deferred");
            assert_eq!(s.queued_send_bytes(1), Some(176), "{label}");
            assert!(s.take_output().is_empty(), "{label}");
            assert_eq!(step(&mut s, &[]).unwrap(), (0, None), "{label}");

            // Force the Deliver -> Discard transition before aborting: its
            // snapshot is all 34 unread body bytes, not the 33 left afterward.
            let snapshot_at = prefix_at + 1;
            assert_eq!(
                step(&mut s, &wire[prefix_at..snapshot_at]).unwrap(),
                (1, None),
                "{label}"
            );
            assert!(s.take_output().is_empty(), "{label}: no early tail credit");
            s.release_capacity(1, 2);
            let out = s.take_output();
            assert_credit(&out, 2, 2, &format!("{label}: prefix during tail"));
            assert_eq!(frames(&out).len(), 2, "{label}");
            assert_eq!(s.unreleased_recv_bytes(1), Some(4), "{label}");

            let padding_at = 9 + 1 + 40 + 3;
            let abort_at = if abort_in_padding {
                padding_at
            } else {
                snapshot_at
            };
            assert!(feed(&mut s, &wire[snapshot_at..abort_at], Cutting::Bytewise).is_empty());
            assert!(s.take_output().is_empty(), "{label}");
            s.reset_stream(1, ErrorCode::Cancel).unwrap();
            let out = s.take_output();
            assert_credit(&out, 0, 0, &format!("{label}: prefix stays retained"));
            assert_eq!(resets(&out, 1), 1, "{label}");
            assert_eq!(frames(&out).len(), 1, "{label}");
            assert!(!s.has_stream(1), "{label}");
            assert_eq!(s.queued_send_bytes(1), None, "{label}");
            assert_eq!(s.unreleased_recv_bytes(1), Some(4), "{label}");
            assert_reset_notice(&mut s, &label);
            assert_eq!(step(&mut s, &[]).unwrap(), (0, None), "{label}");

            assert!(feed(&mut s, &wire[abort_at..padding_at], Cutting::Bytewise).is_empty());
            assert!(
                s.take_output().is_empty(),
                "{label}: padding has not completed"
            );
            s.release_capacity(1, 4);
            let out = s.take_output();
            assert_credit(&out, 4, 0, &format!("{label}: prefix during padding"));
            assert_eq!(frames(&out).len(), 1, "{label}");
            assert_eq!(s.unreleased_recv_bytes(1), None, "{label}");
            s.release_capacity(1, 6);
            assert!(feed(&mut s, &wire[padding_at..wire.len() - 1], Cutting::Bytewise).is_empty());
            for _ in 0..3 {
                assert_idle(&mut s, &label);
            }

            assert_eq!(
                step(&mut s, &wire[wire.len() - 1..]).unwrap(),
                (1, None),
                "{label}"
            );
            let out = s.take_output();
            assert_credit(&out, 34, 0, &format!("{label}: original snapshot once"));
            assert_eq!(frames(&out).len(), 1, "{label}: no second reset");
            // Connection increments 8 + 2 + 4 + 34 cover all 48 payload bytes.
            for _ in 0..3 {
                s.release_capacity(1, 6);
                assert_idle(&mut s, &label);
            }
            assert!(s.failure().is_none(), "{label}");
            assert_eq!(
                feed(
                    &mut s,
                    &frame(DATA, END_STREAM, 3, b"alive"),
                    Cutting::Whole
                ),
                vec![Seen::Data(3, b"alive".to_vec()), Seen::DataEnd(3, true)],
                "{label}: another stream still receives a body"
            );
        }
    }
}

#[test]
fn cancelling_after_the_last_unpadded_data_event_suppresses_data_end() {
    for manual in [false, true] {
        for retained in [false, true] {
            for end_stream in [false, true] {
                let label = format!("manual={manual} retained={retained} end_stream={end_stream}");
                let mut s = two_streams(Config {
                    flow_control: if manual {
                        FlowControl::Manual
                    } else {
                        FlowControl::Automatic
                    },
                    ..Config::default()
                });
                if retained {
                    s.retain_receive_capacity(1).unwrap();
                }
                let wire = frame(DATA, if end_stream { END_STREAM } else { 0 }, 1, &[9; 6]);
                // Do not use drain/feed: its empty poll would consume DataEnd
                // before the application has a chance to cancel at this boundary.
                assert_eq!(
                    step(&mut s, &wire).unwrap(),
                    (wire.len(), Some(Seen::Data(1, vec![9; 6]))),
                    "{label}"
                );
                let out = s.take_output();
                assert_credit(
                    &out,
                    if manual { 0 } else { 6 },
                    if manual || end_stream { 0 } else { 6 },
                    &format!("{label}: admission"),
                );
                assert_eq!(
                    frames(&out).len(),
                    if manual {
                        0
                    } else if end_stream {
                        1
                    } else {
                        2
                    },
                    "{label}"
                );
                assert_eq!(
                    s.unreleased_recv_bytes(1),
                    Some(if manual { 6 } else { 0 }),
                    "{label}"
                );

                s.reset_stream(1, ErrorCode::Cancel).unwrap();
                let out = s.take_output();
                let cleanup_owns_prefix = manual && !retained;
                assert_credit(
                    &out,
                    if cleanup_owns_prefix { 6 } else { 0 },
                    0,
                    &format!("{label}: cleanup"),
                );
                assert_eq!(resets(&out, 1), 1, "{label}");
                assert_eq!(
                    frames(&out).len(),
                    if cleanup_owns_prefix { 2 } else { 1 },
                    "{label}"
                );
                assert!(!s.has_stream(1), "{label}");
                assert_reset_notice(&mut s, &label);
                for _ in 0..3 {
                    assert_idle(&mut s, &label);
                }

                let held = manual && retained;
                assert_eq!(
                    s.unreleased_recv_bytes(1),
                    if held { Some(6) } else { None },
                    "{label}"
                );
                s.release_capacity(1, 6);
                let out = s.take_output();
                assert_credit(&out, if held { 6 } else { 0 }, 0, &label);
                assert_eq!(frames(&out).len(), usize::from(held), "{label}");
                assert_eq!(s.unreleased_recv_bytes(1), None, "{label}");
                for _ in 0..3 {
                    s.release_capacity(1, 6);
                    assert_idle(&mut s, &label);
                }
                assert!(s.failure().is_none(), "{label}");
                assert_eq!(
                    feed(
                        &mut s,
                        &frame(DATA, END_STREAM, 3, b"alive"),
                        Cutting::Whole
                    ),
                    vec![Seen::Data(3, b"alive".to_vec()), Seen::DataEnd(3, true)],
                    "{label}: another stream still receives a body"
                );
            }
        }
    }
}

#[test]
fn deferred_reset_credits_the_tail_and_closes_without_a_reset() {
    // A queued response defers the reset. Discarded DATA still owes its tail
    // credit at frame completion; normal closure later abandons the reset.
    let mut s = two_streams_peer_window(
        Config {
            flow_control: FlowControl::Manual,
            ..Config::default()
        },
        1024,
    );
    queue_blocked_response(&mut s, "deferred reset");

    // The frame starts as delivered: one octet arrives before the application
    // defers its reset, so the discard has to take over in mid-frame.
    let wire = frame(DATA, END_STREAM, 1, &[9; 40]);
    let (consumed, first) = step(&mut s, &wire[..9 + 1]).unwrap();
    assert_eq!(consumed, 10usize);
    assert_eq!(
        first,
        Some(Seen::Data(1, vec![9])),
        "the octet before the reset is delivered"
    );
    assert!(
        s.take_output().is_empty(),
        "an unpadded frame owes nothing at admission"
    );
    assert_eq!(
        s.unreleased_recv_bytes(1),
        Some(1usize),
        "the delivered byte is still owed by the application"
    );

    s.reset_stream_after_flush(1, ErrorCode::NoError).unwrap();
    let out = s.take_output();
    assert_eq!(
        resets(&out, 1),
        0,
        "the reset waits behind the queued response"
    );
    assert!(
        update_incs(&out, 0).is_empty(),
        "a deferred reset spends no credit of its own"
    );
    assert!(
        s.has_stream(1),
        "the stream is still alive while its reset is pending"
    );
    assert_eq!(
        s.unreleased_recv_bytes(1),
        Some(1usize),
        "a deferred reset keeps the stream's owed byte"
    );

    let mid = feed(&mut s, &wire[9 + 1..wire.len() - 1], Cutting::Bytewise);
    assert!(
        delivered(&mid).is_empty(),
        "the discarded tail publishes no body"
    );
    assert_eq!(completions(&mid), 0, "the frame has not completed");
    assert!(
        update_incs(&s.take_output(), 0).is_empty(),
        "an incomplete tail owes nothing"
    );

    let (consumed, last) = step(&mut s, &wire[wire.len() - 1..]).unwrap();
    assert_eq!(consumed, 1usize);
    assert!(
        last.is_none(),
        "no completion event is fabricated for discarded bytes"
    );
    let out = s.take_output();
    assert_credit(&out, 39, 0, "deferred reset: completed tail");
    assert_eq!(
        resets(&out, 1),
        0,
        "the deferred reset is still not written"
    );
    assert!(s.has_stream(1), "the stream waits for its queued response");

    // The peer makes room; the queued response lands, and both halves have now
    // ended normally, which abandons the pending reset instead of writing it.
    let outstanding = s.queued_send_bytes(1).unwrap_or(0usize);
    assert!(outstanding > 0, "the response is still queued");
    let room = frame(WINDOW_UPDATE, 0, 1, &(outstanding as u32).to_be_bytes());
    feed(&mut s, &room, Cutting::Whole);
    step(&mut s, &[]).unwrap();
    let written = s.take_output();
    assert_eq!(s.queued_send_bytes(1), None, "the queued response landed");
    assert_eq!(
        resets(&written, 1),
        0,
        "a normally closed stream never manufactures its reset"
    );
    assert!(!s.has_stream(1), "the closed stream is cleaned up");
    // The deferred reset never spent the delivered byte; the stream's eventual
    // removal does, once at connection level for a closed stream id.
    assert_credit(&written, 1, 0, "deferred reset: cleanup");
    assert!(
        s.failure().is_none(),
        "draining the queued response must not fail the connection"
    );
}
