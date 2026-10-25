//! Differential oracle for the receive path. Each scenario is fed through the
//! public `recv` / `poll_event` API under several transport chunkings. The
//! normalized events, the result and the serialized output must not depend on
//! chunking. With `RX_TRACE_OUT` set, the whole-slice trace is written to that
//! path, so two implementations can be compared by diffing the files.
//!
//! Normalization merges adjacent body bytes of one stream and reports END_STREAM
//! as its own marker, so an implementation may split DATA into chunks or report
//! frame completion separately without changing the comparison.

use protolink_http2::{Config, Connection, Event, FlowControl, HeaderField};

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const DATA: u8 = 0x0;
const HEADERS: u8 = 0x1;
const PRIORITY: u8 = 0x2;
const RST_STREAM: u8 = 0x3;
const SETTINGS: u8 = 0x4;
const PUSH_PROMISE: u8 = 0x5;
const PING: u8 = 0x6;
const GOAWAY: u8 = 0x7;
const WINDOW_UPDATE: u8 = 0x8;
const CONTINUATION: u8 = 0x9;
const UNKNOWN: u8 = 0x42;

const END_STREAM: u8 = 0x1;
const ACK: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
const PADDED: u8 = 0x8;
const PRIORITY_FLAG: u8 = 0x20;

fn frame(kind: u8, flags: u8, id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    let mut out = vec![(len >> 16) as u8, (len >> 8) as u8, len as u8, kind, flags];
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn settings(tuples: &[(u16, u32)]) -> Vec<u8> {
    tuples
        .iter()
        .flat_map(|(id, value)| {
            let mut t = id.to_be_bytes().to_vec();
            t.extend_from_slice(&value.to_be_bytes());
            t
        })
        .collect()
}

/// HPACK without indexing or Huffman coding: every field is a literal.
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
const RESPONSE: &[(&str, &str)] = &[(":status", "200"), ("content-type", "application/grpc")];
const TRAILERS: &[(&str, &str)] = &[("grpc-status", "0")];

fn request_fields() -> Vec<HeaderField> {
    REQUEST
        .iter()
        .map(|(name, value)| HeaderField {
            name: (*name).into(),
            value: (*value).into(),
        })
        .collect()
}

/// The client preface and our empty SETTINGS, as a peer sends them.
fn server_base() -> Vec<u8> {
    let mut wire = PREFACE.to_vec();
    wire.extend(frame(SETTINGS, 0, 0, &[]));
    wire
}

/// A server connection that receives `build`'s bytes after the handshake.
fn server_wire(config: Config, build: impl FnOnce(&mut Vec<u8>)) -> (Connection, Vec<u8>) {
    let mut wire = server_base();
    build(&mut wire);
    (Connection::server(config), wire)
}

fn request_headers(wire: &mut Vec<u8>, id: u32, end: bool) {
    let flags = END_HEADERS | if end { END_STREAM } else { 0 };
    wire.extend(frame(HEADERS, flags, id, &block(REQUEST)));
}

/// A client connection that has opened streams 1 and 3, with its own output
/// discarded, and the server bytes to feed it.
fn client_with_streams(config: Config, wire: Vec<u8>) -> (Connection, Vec<u8>) {
    let mut client = Connection::client(config);
    client.take_output();
    client.open_stream(request_fields(), false).unwrap();
    client.open_stream(request_fields(), false).unwrap();
    client.take_output();
    (client, wire)
}

fn manual() -> Config {
    Config {
        flow_control: FlowControl::Manual,
        ..Config::default()
    }
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario::new("headers_only", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(HEADERS, END_HEADERS | END_STREAM, 1, &block(REQUEST)));
            })
        }),
        Scenario::new("data_stream", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                w.extend(frame(DATA, 0, 1, &[7; 100]));
                w.extend(frame(DATA, 0, 1, &[]));
                w.extend(frame(DATA, END_STREAM, 1, &[9; 50]));
            })
        }),
        Scenario::new("data_padded", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                let mut payload = vec![5];
                payload.extend([3; 10]);
                payload.extend([0; 5]);
                w.extend(frame(DATA, PADDED, 1, &payload));
                w.extend(frame(DATA, PADDED | END_STREAM, 1, &[2, 4, 0, 0]));
            })
        }),
        Scenario::new("padding_only_end", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                w.extend(frame(DATA, PADDED | END_STREAM, 1, &[3, 0, 0, 0]));
            })
        }),
        Scenario::new("empty_end", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                w.extend(frame(DATA, END_STREAM, 1, &[]));
            })
        }),
        Scenario::new("headers_priority_padded", || {
            server_wire(Config::default(), |w| {
                let mut payload = vec![2, 0, 0, 0, 0, 15];
                payload.extend(block(REQUEST));
                payload.extend([0, 0]);
                w.extend(frame(
                    HEADERS,
                    PADDED | PRIORITY_FLAG | END_HEADERS | END_STREAM,
                    1,
                    &payload,
                ));
            })
        }),
        Scenario::new("continuation_split", || {
            server_wire(Config::default(), |w| {
                let b = block(REQUEST);
                let (first, rest) = b.split_at(b.len() / 2);
                w.extend(frame(HEADERS, 0, 1, first));
                w.extend(frame(CONTINUATION, END_HEADERS, 1, rest));
                w.extend(frame(DATA, END_STREAM, 1, &[1; 8]));
            })
        }),
        Scenario::new("continuation_three_pieces", || {
            server_wire(Config::default(), |w| {
                let b = block(REQUEST);
                let third = b.len() / 3;
                w.extend(frame(HEADERS, END_STREAM, 1, &b[..third]));
                w.extend(frame(CONTINUATION, 0, 1, &b[third..2 * third]));
                w.extend(frame(CONTINUATION, END_HEADERS, 1, &b[2 * third..]));
            })
        }),
        Scenario::new("controls", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                w.extend(frame(PING, 0, 0, b"12345678"));
                w.extend(frame(PING, ACK, 0, b"abcdefgh"));
                w.extend(frame(UNKNOWN, 0, 0, &[1; 20]));
                w.extend(frame(UNKNOWN, 0, 1, &[1; 3]));
                w.extend(frame(PRIORITY, 0, 3, &[0, 0, 0, 0, 16]));
                w.extend(frame(WINDOW_UPDATE, 0, 0, &1000u32.to_be_bytes()));
                w.extend(frame(WINDOW_UPDATE, 0, 1, &500u32.to_be_bytes()));
                w.extend(frame(DATA, 0, 1, &[2; 30]));
                w.extend(frame(RST_STREAM, 0, 1, &8u32.to_be_bytes()));
                w.extend(frame(DATA, 0, 1, &[2; 5]));
                w.extend(frame(WINDOW_UPDATE, 0, 1, &5u32.to_be_bytes()));
            })
        }),
        Scenario::new("settings_tuples", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(
                    SETTINGS,
                    0,
                    0,
                    &settings(&[(1, 1000), (2, 0), (0x99, 3), (6, 9000), (4, 1000)]),
                ));
                w.extend(frame(SETTINGS, ACK, 0, &[]));
                w.extend(frame(PING, 0, 0, b"12345678"));
            })
        }),
        Scenario::new("settings_window_grows", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                w.extend(frame(SETTINGS, 0, 0, &settings(&[(4, 0x7fff_0000)])));
            })
        }),
        Scenario::new("settings_transient_overflow", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                w.extend(frame(
                    WINDOW_UPDATE,
                    0,
                    1,
                    &(0x7fff_0000u32 - 65_535).to_be_bytes(),
                ));
                w.extend(frame(
                    SETTINGS,
                    0,
                    0,
                    &settings(&[(4, 0x7fff_ffff), (4, 65_535)]),
                ));
            })
        }),
        Scenario::new("settings_invalid_late", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(SETTINGS, 0, 0, &settings(&[(6, 9000), (2, 2)])));
            })
        }),
        Scenario::new("settings_ack_with_payload", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(SETTINGS, ACK, 0, &[0; 6]));
            })
        }),
        Scenario::new("data_after_end_stream", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(HEADERS, END_HEADERS | END_STREAM, 1, &block(REQUEST)));
                w.extend(frame(DATA, 0, 1, &[5; 5]));
            })
        }),
        Scenario::new("many_small_frames", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                for i in 0..300u32 {
                    w.extend(frame(DATA, 0, 1, &[i as u8]));
                }
                w.extend(frame(DATA, END_STREAM, 1, &[]));
            })
        }),
        Scenario::new("large_message", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                for _ in 0..4 {
                    w.extend(frame(DATA, 0, 1, &[0xab; 16_384]));
                }
                w.extend(frame(DATA, END_STREAM, 1, &[0xcd; 1000]));
            })
        }),
        Scenario::new("manual_flow_release", || {
            server_wire(manual(), |w| {
                request_headers(w, 1, false);
                w.extend(frame(DATA, 0, 1, &[1; 100]));
                w.extend(frame(DATA, PADDED | END_STREAM, 1, &[4, 2, 0, 0, 0, 0]));
            })
        }),
        Scenario::new("refused_by_concurrency", || {
            server_wire(
                Config {
                    max_concurrent_streams: 0,
                    ..Config::default()
                },
                |w| request_headers(w, 1, true),
            )
        }),
        Scenario::new("stream_zero_data", || {
            server_wire(Config::default(), |w| w.extend(frame(DATA, 0, 0, &[5; 5])))
        }),
        Scenario::new("bad_padding", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                w.extend(frame(DATA, PADDED, 1, &[9, 1, 2]));
            })
        }),
        Scenario::new("settings_length", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(SETTINGS, 0, 0, &[0; 5]));
            })
        }),
        Scenario::new("oversized_frame", || {
            let mut wire = server_base();
            wire.extend([0x00, 0x40, 0x01, DATA, 0, 0, 0, 0, 1]);
            (Connection::server(Config::default()), wire)
        }),
        Scenario::new("first_frame_not_settings", || {
            let mut wire = PREFACE.to_vec();
            wire.extend(frame(PING, 0, 0, b"12345678"));
            (Connection::server(Config::default()), wire)
        }),
        Scenario::new("continuation_wrong_stream", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(HEADERS, 0, 1, &block(REQUEST)));
                w.extend(frame(CONTINUATION, END_HEADERS, 3, &[]));
            })
        }),
        Scenario::new("interleaved_during_continuation", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(HEADERS, 0, 1, &block(REQUEST)));
                w.extend(frame(PING, 0, 0, b"12345678"));
            })
        }),
        Scenario::new("unknown_during_continuation", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(HEADERS, 0, 1, &block(REQUEST)));
                w.extend(frame(UNKNOWN, 0, 0, &[1; 4]));
            })
        }),
        Scenario::new("priority_wrong_length_resets", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(HEADERS, END_HEADERS, 1, &block(REQUEST)));
                w.extend(frame(PRIORITY, 0, 3, &[0; 4]));
            })
        }),
        Scenario::new("rst_wrong_length", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                w.extend(frame(RST_STREAM, 0, 1, &[0; 3]));
            })
        }),
        Scenario::new("zero_window_update", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                w.extend(frame(WINDOW_UPDATE, 0, 0, &0u32.to_be_bytes()));
            })
        }),
        Scenario::new("content_length_mismatch", || {
            server_wire(Config::default(), |w| {
                let fields: &[(&str, &str)] = &[
                    (":method", "POST"),
                    (":scheme", "http"),
                    (":path", "/m.S/Call"),
                    (":authority", "localhost"),
                    ("content-length", "5"),
                ];
                w.extend(frame(HEADERS, END_HEADERS, 1, &block(fields)));
                w.extend(frame(DATA, END_STREAM, 1, &[1; 3]));
            })
        }),
        Scenario::new("headers_short_priority", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(HEADERS, PRIORITY_FLAG | END_HEADERS, 1, &[0; 3]));
            })
        }),
        Scenario::new("push_promise", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(PUSH_PROMISE, END_HEADERS, 1, &[0, 0, 0, 2]));
            })
        }),
        Scenario::new("data_on_idle_stream", || {
            server_wire(Config::default(), |w| w.extend(frame(DATA, 0, 5, &[1; 5])))
        }),
        Scenario::new("ping_wrong_length", || {
            server_wire(Config::default(), |w| w.extend(frame(PING, 0, 0, &[0; 4])))
        }),
        Scenario::new("hpack_index_zero", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(HEADERS, END_HEADERS | END_STREAM, 1, &[0x80]));
            })
        }),
        Scenario::new("goaway_short", || {
            server_wire(Config::default(), |w| {
                w.extend(frame(GOAWAY, 0, 0, &[0; 4]))
            })
        }),
        Scenario::new("settings_bad_value_after_request", || {
            server_wire(Config::default(), |w| {
                request_headers(w, 1, false);
                w.extend(frame(SETTINGS, 0, 0, &settings(&[(6, 9000), (2, 2)])));
            })
        }),
        Scenario::new("goaway_refuses_later_streams", || {
            let mut wire = frame(SETTINGS, 0, 0, &[]);
            let mut payload = 1u32.to_be_bytes().to_vec();
            payload.extend(0u32.to_be_bytes());
            payload.extend(&b"bye"[..]);
            wire.extend(frame(GOAWAY, 0, 0, &payload));
            client_with_streams(Config::default(), wire)
        }),
        Scenario::new("response_with_trailers", || {
            let mut wire = frame(SETTINGS, 0, 0, &[]);
            wire.extend(frame(HEADERS, END_HEADERS, 1, &block(RESPONSE)));
            wire.extend(frame(DATA, 0, 1, &[3; 300]));
            wire.extend(frame(DATA, 0, 1, &[]));
            wire.extend(frame(
                HEADERS,
                END_HEADERS | END_STREAM,
                1,
                &block(TRAILERS),
            ));
            client_with_streams(Config::default(), wire)
        }),
        Scenario::new("reset_then_closed_stream_data", || {
            let mut wire = frame(SETTINGS, 0, 0, &[]);
            wire.extend(frame(RST_STREAM, 0, 1, &8u32.to_be_bytes()));
            wire.extend(frame(DATA, 0, 1, &[3; 10]));
            wire.extend(frame(WINDOW_UPDATE, 0, 1, &9u32.to_be_bytes()));
            client_with_streams(Config::default(), wire)
        }),
        Scenario::new("manual_client_response", || {
            let mut wire = frame(SETTINGS, 0, 0, &[]);
            wire.extend(frame(HEADERS, END_HEADERS, 1, &block(RESPONSE)));
            wire.extend(frame(DATA, 0, 1, &[3; 16_384]));
            wire.extend(frame(DATA, END_STREAM, 3, &[4; 7]));
            client_with_streams(manual(), wire)
        }),
    ]
}

/// How the wire bytes are cut into `recv` calls.
#[derive(Clone, Copy, Debug)]
enum Chunking {
    Fixed(usize),
    Seeded(u64),
}

impl Chunking {
    fn pieces(self, wire: &[u8]) -> Vec<&[u8]> {
        match self {
            Chunking::Fixed(n) => wire.chunks(n).collect(),
            Chunking::Seeded(mut seed) => {
                let mut out = Vec::new();
                let mut rest = wire;
                while !rest.is_empty() {
                    seed = seed
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    let n = (((seed >> 33) % 40) as usize + 1).min(rest.len());
                    let (head, tail) = rest.split_at(n);
                    out.push(head);
                    rest = tail;
                }
                out
            }
        }
    }
}

const CHUNKINGS: &[Chunking] = &[
    Chunking::Fixed(1),
    Chunking::Fixed(2),
    Chunking::Fixed(3),
    Chunking::Fixed(7),
    Chunking::Fixed(64),
    Chunking::Fixed(512),
    Chunking::Seeded(1),
    Chunking::Seeded(2),
    Chunking::Seeded(3),
];

/// One normalized observation: body bytes, END_STREAM, or any other event.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Norm {
    Body(u32, Vec<u8>),
    End(u32),
    Other(String),
}

struct Scenario {
    name: &'static str,
    build: fn() -> (Connection, Vec<u8>),
}

impl Scenario {
    fn new(name: &'static str, build: fn() -> (Connection, Vec<u8>)) -> Self {
        Self { name, build }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    result: Result<(), String>,
    events: Vec<Norm>,
    output: Vec<u8>,
}

/// The text after `key` in a Debug rendering, up to the first `end`.
fn field<'a>(text: &'a str, key: &str, end: char) -> &'a str {
    let start = text.find(key).unwrap() + key.len();
    let rest = &text[start..];
    &rest[..rest.find(end).unwrap()]
}

fn parse_bytes(list: &str) -> Vec<u8> {
    list.trim()
        .trim_start_matches('[')
        .split(", ")
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap())
        .collect()
}

/// Normalize through the Debug rendering, so the owned `Data` shape of one
/// implementation and the separate completion notice of another compare equal.
fn normalize(event: &Event, out: &mut Vec<Norm>) {
    let text = format!("{event:?}");
    if let Some(rest) = text.strip_prefix("Data ") {
        let id: u32 = field(rest, "stream_id: ", ',').parse().unwrap();
        let data = parse_bytes(field(rest, "data: ", ']'));
        push_body(out, id, &data);
        if rest.contains("end_stream: true") {
            out.push(Norm::End(id));
        }
    } else if let Some(rest) = text.strip_prefix("DataEnd ") {
        if rest.contains("end_stream: true") {
            let id: u32 = field(rest, "stream_id: ", ',').parse().unwrap();
            out.push(Norm::End(id));
        }
    } else {
        out.push(Norm::Other(text));
    }
}

fn push_body(out: &mut Vec<Norm>, id: u32, data: &[u8]) {
    if data.is_empty() {
        return;
    }
    if let Some(Norm::Body(last, bytes)) = out.last_mut()
        && *last == id
    {
        bytes.extend_from_slice(data);
        return;
    }
    out.push(Norm::Body(id, data.to_vec()));
}

fn run(scenario: &Scenario, pieces: &[&[u8]]) -> Outcome {
    let (mut conn, _) = (scenario.build)();
    let mut events = Vec::new();
    let mut output = Vec::new();
    let mut result = Ok(());
    for piece in pieces {
        let outcome = conn.recv(piece);
        while let Some(event) = conn.poll_event() {
            normalize(&event, &mut events);
        }
        output.extend(conn.take_output());
        if let Err(e) = outcome {
            result = Err(format!("{e:?}"));
            break;
        }
    }
    if result.is_ok() {
        // Credit everything the application still holds, as a consumer would.
        for id in (1..=7).step_by(2) {
            if let Some(held) = conn.unreleased_recv_bytes(id)
                && held > 0
            {
                conn.release_capacity(id, held);
            }
        }
        output.extend(conn.take_output());
    }
    Outcome {
        result,
        events,
        output,
    }
}

fn wire_of(scenario: &Scenario) -> Vec<u8> {
    (scenario.build)().1
}

fn fnv(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100_0000_01b3)
    })
}

#[test]
fn chunking_does_not_change_events_or_output() {
    let mut trace = String::new();
    for scenario in scenarios() {
        let wire = wire_of(&scenario);
        let whole = run(&scenario, &[wire.as_slice()]);
        for &chunking in CHUNKINGS {
            let pieces = chunking.pieces(&wire);
            let other = run(&scenario, &pieces);
            assert_eq!(
                other.result, whole.result,
                "{} result under {chunking:?}",
                scenario.name
            );
            assert_eq!(
                other.events, whole.events,
                "{} events under {chunking:?}",
                scenario.name
            );
            assert_eq!(
                other.output, whole.output,
                "{} output under {chunking:?}",
                scenario.name
            );
        }
        trace.push_str(&format!(
            "{} result={:?} output={}:{:016x}\n",
            scenario.name,
            whole.result,
            whole.output.len(),
            fnv(&whole.output)
        ));
        for event in &whole.events {
            trace.push_str(&format!("  {event:?}\n"));
        }
    }
    if let Some(path) = std::env::var_os("RX_TRACE_OUT") {
        std::fs::write(path, trace).unwrap();
    }
}
