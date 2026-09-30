extern crate std;

use super::*;
use alloc::string::String;
use alloc::vec;

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
