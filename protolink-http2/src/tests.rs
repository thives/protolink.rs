extern crate std;

use super::*;
use alloc::string::String;
use alloc::vec;

#[path = "regressions.rs"]
mod regressions;
#[path = "review_regressions.rs"]
mod review_regressions;

fn hf(name: &str, value: &str) -> HeaderField {
    HeaderField {
        name: name.into(),
        value: value.into(),
    }
}

fn request_headers() -> Vec<HeaderField> {
    vec![
        hf(":method", "POST"),
        hf(":scheme", "http"),
        hf(":path", "/pkg.Service/Method"),
        hf(":authority", "localhost"),
        hf("content-type", "application/grpc"),
    ]
}

/// Move all pending output from `a` to `b`, optionally in `chunk`-sized pieces.
fn pump(a: &mut Connection, b: &mut Connection, chunk: usize) -> Result<(), Error> {
    let out = a.take_output();
    for piece in out.chunks(chunk.max(1)) {
        b.recv(piece)?;
    }
    Ok(())
}

fn exchange(c: &mut Connection, s: &mut Connection) {
    for _ in 0..16 {
        if !c.has_output() && !s.has_output() {
            return;
        }
        pump(c, s, usize::MAX).unwrap();
        pump(s, c, usize::MAX).unwrap();
    }
}

fn events(c: &mut Connection) -> Vec<Event> {
    core::iter::from_fn(|| c.poll_event()).collect()
}

/// `(type, flags, stream id, payload)` of every frame in `out`.
fn frames(out: &[u8]) -> Vec<(u8, u8, StreamId, Vec<u8>)> {
    let mut res = Vec::new();
    let mut pos = 0;
    while pos + 9 <= out.len() {
        let len = (usize::from(out[pos]) << 16)
            | (usize::from(out[pos + 1]) << 8)
            | usize::from(out[pos + 2]);
        let id = u32::from_be_bytes([out[pos + 5], out[pos + 6], out[pos + 7], out[pos + 8]]);
        res.push((
            out[pos + 3],
            out[pos + 4],
            id & 0x7fff_ffff,
            out[pos + 9..pos + 9 + len].to_vec(),
        ));
        pos += 9 + len;
    }
    res
}

fn data_frame(id: StreamId, len: usize, flags: u8) -> Vec<u8> {
    let mut f = vec![
        (len >> 16) as u8,
        (len >> 8) as u8,
        len as u8,
        FrameType::Data as u8,
        flags,
    ];
    f.extend_from_slice(&id.to_be_bytes());
    f.resize(9 + len, 0x5A);
    f
}

fn data_len(ev: &[Event], id: StreamId) -> usize {
    ev.iter()
        .map(|e| match e {
            Event::Data {
                stream_id, data, ..
            } if *stream_id == id => data.len(),
            _ => 0,
        })
        .sum()
}

fn manual(initial_window_size: u32, connection_window_size: u32) -> Config {
    Config {
        initial_window_size,
        connection_window_size,
        flow_control: FlowControl::Manual,
        ..Config::default()
    }
}

#[test]
fn unary_round_trip() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    let id = c.open_stream(request_headers(), false).unwrap();
    assert_eq!(id, 1);
    c.send_data(id, b"hello".to_vec(), true).unwrap();
    exchange(&mut c, &mut s);

    let ev = events(&mut s);
    assert_eq!(ev.len(), 2, "{ev:?}");
    match &ev[0] {
        Event::Headers {
            stream_id,
            headers,
            end_stream,
        } => {
            assert_eq!(*stream_id, 1);
            assert!(!end_stream);
            assert!(
                headers
                    .iter()
                    .any(|h| h.name == ":path" && h.value == "/pkg.Service/Method")
            );
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(
        ev[1],
        Event::Data {
            stream_id: 1,
            data: b"hello".to_vec(),
            end_stream: true
        }
    );

    s.send_headers(1, vec![hf(":status", "200")], false)
        .unwrap();
    s.send_data(1, b"world".to_vec(), false).unwrap();
    s.send_headers(1, vec![hf("grpc-status", "0")], true)
        .unwrap();
    assert_eq!(s.stream_count(), 0, "stream closed after both sides ended");
    exchange(&mut c, &mut s);

    let ev = events(&mut c);
    assert_eq!(ev.len(), 3, "{ev:?}");
    assert!(
        matches!(&ev[2], Event::Headers { end_stream: true, headers, .. } if headers[0].value == "0")
    );
    assert_eq!(c.stream_count(), 0);
}

#[test]
fn byte_by_byte_delivery() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    let id = c.open_stream(request_headers(), false).unwrap();
    c.send_data(id, vec![7; 300], true).unwrap();
    pump(&mut c, &mut s, 1).unwrap();
    let ev = events(&mut s);
    assert!(matches!(&ev[1], Event::Data { data, end_stream: true, .. } if data.len() == 300));
}

#[test]
fn settings_and_ping_are_acked() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    pump(&mut c, &mut s, usize::MAX).unwrap();
    let out = s.take_output();
    // Server SETTINGS followed by a SETTINGS ACK.
    assert_eq!(out[3], FrameType::Settings as u8);
    let first_len = 9 + ((usize::from(out[1]) << 8) | usize::from(out[2]));
    assert_eq!(out[first_len + 3], FrameType::Settings as u8);
    assert_eq!(out[first_len + 4], Flags::ACK);

    let mut ping = vec![0, 0, 8, FrameType::Ping as u8, 0, 0, 0, 0, 0];
    ping.extend_from_slice(b"12345678");
    s.recv(&ping).unwrap();
    let out = s.take_output();
    assert_eq!(out[3], FrameType::Ping as u8);
    assert_eq!(out[4], Flags::ACK);
    assert_eq!(&out[9..], b"12345678");
}

#[test]
fn large_body_respects_flow_control() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    let body = vec![0xAB; 200_000];
    c.send_data(id, body.clone(), true).unwrap();
    // Only the initial 65535-byte window can be sent before WINDOW_UPDATEs.
    let first = c.take_output();
    assert!(first.len() < 70_000, "sent {} bytes", first.len());
    s.recv(&first).unwrap();
    exchange(&mut c, &mut s);
    let received: usize = events(&mut s)
        .iter()
        .map(|e| match e {
            Event::Data { data, .. } => data.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(received, body.len());
}

#[test]
fn large_header_block_uses_continuation() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config {
        max_header_list_size: 64 * 1024,
        ..Config::default()
    });
    let mut headers = request_headers();
    headers.push(hf("x-big", &String::from_utf8(vec![b'a'; 20_000]).unwrap()));
    c.open_stream(headers, true).unwrap();
    exchange(&mut c, &mut s);
    let ev = events(&mut s);
    assert!(
        matches!(&ev[0], Event::Headers { headers, end_stream: true, .. } if headers.iter().any(|h| h.value.len() == 20_000))
    );
}

#[test]
fn unknown_frame_types_are_ignored() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    s.recv(&[0, 0, 2, 0xEE, 0, 0, 0, 0, 0, 1, 2]).unwrap();
}

#[test]
fn bad_preface_is_connection_error() {
    let mut s = Connection::server(Config::default());
    s.take_output();
    let err = s.recv(b"GET / HTTP/1.1\r\n\r\n").unwrap_err();
    assert!(matches!(
        err,
        Error::Connection {
            code: ErrorCode::ProtocolError,
            ..
        }
    ));
    // GOAWAY is queued.
    assert_eq!(s.pending_output()[3], FrameType::GoAway as u8);
    assert!(s.is_closed());
}

#[test]
fn concurrent_stream_limit_refuses() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config {
        max_concurrent_streams: 1,
        ..Config::default()
    });
    c.open_stream(request_headers(), false).unwrap();
    c.open_stream(request_headers(), false).unwrap();
    exchange(&mut c, &mut s);
    let ev = events(&mut c);
    assert!(ev.contains(&Event::Reset {
        stream_id: 3,
        error_code: ErrorCode::RefusedStream
    }));
}

#[test]
fn goaway_refuses_unprocessed_local_streams() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    s.go_away(ErrorCode::NoError);
    c.open_stream(request_headers(), true).unwrap();
    pump(&mut s, &mut c, usize::MAX).unwrap();
    let ev = events(&mut c);
    assert!(ev.contains(&Event::Reset {
        stream_id: 1,
        error_code: ErrorCode::RefusedStream
    }));
    assert_eq!(
        c.open_stream(request_headers(), true),
        Err(Error::GoingAway)
    );
}

#[test]
fn queued_send_bytes_track_window_updates() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    assert_eq!(c.queued_send_bytes(id), Some(0));
    assert_eq!(c.send_capacity(id), Some(65_535));

    c.send_data(id, vec![1; 100_000], false).unwrap();
    assert_eq!(c.queued_send_bytes(id), Some(100_000 - 65_535));
    assert_eq!(c.send_capacity(id), Some(0));
    assert_eq!(c.poll_send_ready(), None, "own sends are not reported");

    // The server replenishes the windows; until the client sees that, the
    // remainder stays queued.
    pump(&mut c, &mut s, usize::MAX).unwrap();
    assert_eq!(c.queued_send_bytes(id), Some(100_000 - 65_535));
    pump(&mut s, &mut c, usize::MAX).unwrap();
    assert_eq!(c.queued_send_bytes(id), Some(0));
    assert_eq!(c.send_capacity(id), Some(65_535 - (100_000 - 65_535)));
    assert_eq!(c.poll_send_ready(), Some(id));
    assert_eq!(c.poll_send_ready(), None);

    pump(&mut c, &mut s, usize::MAX).unwrap();
    assert_eq!(data_len(&events(&mut s), id), 100_000);
    assert_eq!(c.queued_send_bytes(99), None);
    assert_eq!(c.send_capacity(99), None);
}

#[test]
fn send_ready_not_reported_after_local_close() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    c.send_data(id, vec![1; 100_000], true).unwrap();
    assert_eq!(c.send_capacity(id), Some(0));
    exchange(&mut c, &mut s);
    assert_eq!(c.queued_send_bytes(id), Some(0));
    assert_eq!(c.poll_send_ready(), None);
}

#[test]
fn automatic_flow_control_is_unchanged() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    c.send_data(id, vec![1; 1000], false).unwrap();
    pump(&mut c, &mut s, usize::MAX).unwrap();
    let updates: Vec<_> = frames(&s.take_output())
        .into_iter()
        .filter(|f| f.0 == FrameType::WindowUpdate as u8)
        .map(|f| (f.2, f.3))
        .collect();
    assert_eq!(
        updates,
        vec![
            (0, 1000u32.to_be_bytes().to_vec()),
            (id, 1000u32.to_be_bytes().to_vec())
        ]
    );
    assert_eq!(s.unreleased_recv_bytes(id), Some(0));
    s.release_capacity(id, 1000);
    assert!(!s.has_output(), "release_capacity is a no-op");
}

#[test]
fn manual_flow_control_waits_for_release() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(manual(16_384, 1 << 20));
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    c.send_data(id, vec![3; 50_000], true).unwrap();

    let mut total = 0;
    let mut rounds = 0;
    loop {
        exchange(&mut c, &mut s);
        let ev = events(&mut s);
        let n = data_len(&ev, id);
        assert!(n <= 16_384, "received {n} bytes without release");
        total += n;
        if ev.iter().any(|e| {
            matches!(
                e,
                Event::Data {
                    end_stream: true,
                    ..
                }
            )
        }) {
            break;
        }
        assert!(n > 0, "peer stalled");
        assert_eq!(s.unreleased_recv_bytes(id), Some(n));
        assert_eq!(c.queued_send_bytes(id), Some(50_000 - total));
        s.release_capacity(id, n);
        assert_eq!(s.unreleased_recv_bytes(id), Some(0));
        rounds += 1;
    }
    assert_eq!(total, 50_000);
    assert_eq!(rounds, 3);
}

#[test]
fn manual_flow_control_stream_overrun_resets_stream() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(manual(100, 1 << 20));
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    exchange(&mut c, &mut s);
    events(&mut s);

    // Exactly the advertised window is fine.
    s.recv(&data_frame(id, 100, 0)).unwrap();
    assert_eq!(data_len(&events(&mut s), id), 100);
    s.take_output();

    s.recv(&data_frame(id, 1, 0)).unwrap();
    assert_eq!(
        events(&mut s),
        vec![Event::Reset {
            stream_id: id,
            error_code: ErrorCode::FlowControlError
        }]
    );
    let out = frames(&s.take_output());
    assert!(out.iter().any(|f| f.0 == FrameType::RstStream as u8
        && f.2 == id
        && f.3 == (ErrorCode::FlowControlError as u32).to_be_bytes()));
    assert!(!s.is_closed());
}

#[test]
fn manual_flow_control_connection_overrun_is_connection_error() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(manual(1 << 20, 65_535));
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    exchange(&mut c, &mut s);
    for _ in 0..3 {
        s.recv(&data_frame(id, 16_384, 0)).unwrap();
    }
    s.recv(&data_frame(id, 65_535 - 3 * 16_384, 0)).unwrap();
    let err = s.recv(&data_frame(id, 1, 0)).unwrap_err();
    assert!(matches!(
        err,
        Error::Connection {
            code: ErrorCode::FlowControlError,
            ..
        }
    ));
}

#[test]
fn manual_flow_control_does_not_block_other_streams() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(manual(16_384, 1 << 20));
    exchange(&mut c, &mut s);
    let a = c.open_stream(request_headers(), false).unwrap();
    let b = c.open_stream(request_headers(), false).unwrap();
    c.send_data(a, vec![1; 100_000], false).unwrap();
    c.send_data(b, vec![2; 10_000], true).unwrap();
    exchange(&mut c, &mut s);
    let ev = events(&mut s);
    assert_eq!(data_len(&ev, a), 16_384);
    assert_eq!(data_len(&ev, b), 10_000);
}

#[test]
fn manual_flow_control_returns_connection_credit_on_reset() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(manual(65_535, 65_535));
    exchange(&mut c, &mut s);
    let a = c.open_stream(request_headers(), false).unwrap();
    c.send_data(a, vec![1; 65_535], false).unwrap();
    exchange(&mut c, &mut s);
    assert_eq!(s.unreleased_recv_bytes(a), Some(65_535));
    assert_eq!(c.send_capacity(a), Some(0));

    c.reset_stream(a, ErrorCode::Cancel).unwrap();
    exchange(&mut c, &mut s);
    let b = c.open_stream(request_headers(), false).unwrap();
    assert_eq!(c.send_capacity(b), Some(65_535));
}

#[test]
fn manual_flow_control_credits_padding() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(manual(65_535, 65_535));
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    exchange(&mut c, &mut s);
    events(&mut s);
    // 1 pad-length byte + 3 data bytes + 6 padding bytes.
    let mut f = data_frame(id, 10, Flags::PADDED);
    f[9] = 6;
    s.recv(&f).unwrap();
    assert_eq!(data_len(&events(&mut s), id), 3);
    assert_eq!(s.unreleased_recv_bytes(id), Some(3));
    let updates: Vec<_> = frames(&s.take_output())
        .into_iter()
        .filter(|f| f.0 == FrameType::WindowUpdate as u8)
        .map(|f| f.2)
        .collect();
    assert_eq!(updates, vec![id, 0]);
}

#[test]
fn manual_flow_control_applies_initial_window_on_settings_ack() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(manual(1000, 1 << 20));
    // The client sends under the default 65 535 window before it has seen
    // the server's smaller SETTINGS_INITIAL_WINDOW_SIZE.
    let id = c.open_stream(request_headers(), false).unwrap();
    c.send_data(id, vec![1; 5000], false).unwrap();
    pump(&mut c, &mut s, usize::MAX).unwrap();
    assert_eq!(data_len(&events(&mut s), id), 5000);
    exchange(&mut c, &mut s);
    assert_eq!(c.send_capacity(id), Some(0), "window is now -4000");

    s.release_capacity(id, 5000);
    exchange(&mut c, &mut s);
    assert_eq!(c.send_capacity(id), Some(1000));
    c.send_data(id, vec![1; 1000], false).unwrap();
    exchange(&mut c, &mut s);
    assert_eq!(data_len(&events(&mut s), id), 1000);
    assert!(s.has_stream(id));
}

#[test]
fn trailers_delivered_before_deferred_reset() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    // The client never half-closes.
    let id = c.open_stream(request_headers(), false).unwrap();
    exchange(&mut c, &mut s);
    events(&mut s);

    s.send_headers(id, vec![hf(":status", "200")], false)
        .unwrap();
    s.send_data(id, vec![9; 150_000], false).unwrap();
    s.send_headers(id, vec![hf("grpc-status", "0")], true)
        .unwrap();
    s.reset_stream_after_flush(id, ErrorCode::NoError).unwrap();
    assert!(s.has_stream(id), "reset deferred while data is queued");
    assert_eq!(
        s.send_data(id, vec![1], false),
        Err(Error::StreamClosed(id))
    );
    assert_eq!(s.send_capacity(id), Some(0));
    // Request data arriving meanwhile is discarded.
    c.send_data(id, b"late".to_vec(), false).unwrap();

    exchange(&mut c, &mut s);
    let ev = events(&mut c);
    assert_eq!(data_len(&ev, id), 150_000);
    let n = ev.len();
    assert!(
        matches!(&ev[n - 2], Event::Headers { end_stream: true, headers, .. }
            if headers[0].name == "grpc-status"),
        "{:?}",
        ev[n - 2]
    );
    assert_eq!(
        ev[n - 1],
        Event::Reset {
            stream_id: id,
            error_code: ErrorCode::NoError
        }
    );
    assert_eq!(
        events(&mut s),
        vec![Event::Reset {
            stream_id: id,
            error_code: ErrorCode::NoError
        }]
    );
    assert!(!s.has_stream(id));
    assert!(!c.has_stream(id));
}

#[test]
fn deferred_reset_with_nothing_queued_is_immediate() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    exchange(&mut c, &mut s);
    events(&mut s);
    s.send_headers(id, vec![hf(":status", "200")], false)
        .unwrap();
    s.reset_stream_after_flush(id, ErrorCode::Cancel).unwrap();
    assert!(!s.has_stream(id));
    let out = frames(&s.take_output());
    assert_eq!(out.last().unwrap().0, FrameType::RstStream as u8);
}

#[test]
fn deferred_reset_skipped_when_stream_closes_normally() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    exchange(&mut c, &mut s);
    s.send_headers(id, vec![hf(":status", "200")], false)
        .unwrap();
    s.send_data(id, vec![9; 100_000], false).unwrap();
    s.send_headers(id, vec![hf("grpc-status", "0")], true)
        .unwrap();
    s.reset_stream_after_flush(id, ErrorCode::NoError).unwrap();
    // The client half-closes before the server's queue drains.
    c.send_data(id, vec![], true).unwrap();
    exchange(&mut c, &mut s);
    assert!(!s.has_stream(id));
    assert!(
        !events(&mut s)
            .iter()
            .any(|e| matches!(e, Event::Reset { .. }))
    );
    let ev = events(&mut c);
    assert_eq!(data_len(&ev, id), 100_000);
    assert!(matches!(
        ev.last(),
        Some(Event::Headers {
            end_stream: true,
            ..
        })
    ));
}
