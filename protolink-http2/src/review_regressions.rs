use super::*;
use alloc::string::ToString;

fn wire_frame(kind: FrameType, id: StreamId, flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0; 9 + payload.len()];
    encode_frame(
        &FrameHeader {
            length: payload.len() as u32,
            frame_type: kind,
            flags: Flags(flags),
            stream_id: id,
        },
        payload,
        &mut out,
        u32::MAX,
    )
    .unwrap();
    out
}

fn wire_block(conn: &mut Connection, id: StreamId, block: &[u8], end: bool) {
    conn.recv(&wire_frame(
        FrameType::Headers,
        id,
        Flags::END_HEADERS | if end { Flags::END_STREAM } else { 0 },
        block,
    ))
    .unwrap();
}

fn wire_headers(
    conn: &mut Connection,
    encoder: &mut Encoder,
    id: StreamId,
    headers: &[HeaderField],
    end: bool,
) {
    wire_block(conn, id, &encoder.encode(headers), end);
}

fn assert_protocol_reset(conn: &mut Connection, id: StreamId) {
    assert_eq!(
        events(conn),
        vec![Event::Reset {
            stream_id: id,
            error_code: ErrorCode::ProtocolError
        }]
    );
    assert!(!conn.is_closed());
    assert!(
        !frames(&conn.take_output())
            .iter()
            .any(|f| f.0 == FrameType::GoAway as u8)
    );
}

fn raw_string(bytes: &[u8], huffman: bool) -> Vec<u8> {
    let bytes = if huffman {
        zerodds_hpack::huffman::encode(bytes)
    } else {
        bytes.to_vec()
    };
    let mut out =
        zerodds_hpack::encode_integer(bytes.len() as u64, 7, if huffman { 0x80 } else { 0 });
    out.extend(bytes);
    out
}

fn raw_literal(name: &[u8], value: &[u8], huffman: bool) -> Vec<u8> {
    let mut out = vec![0x40];
    out.extend(raw_string(name, huffman));
    out.extend(raw_string(value, huffman));
    out
}

#[test]
fn non_utf8_hpack_names_and_values_are_stream_local_after_full_table_update() {
    for huffman in [false, true] {
        for invalid_name in [false, true] {
            let mut c = Connection::client(Config::default());
            let mut s = Connection::server(Config::default());
            exchange(&mut c, &mut s);
            // Static POST, http, /; then a rejected octet field followed by a
            // valid dynamic entry. Both entries must be retained, without loss.
            let mut block = vec![0x83, 0x86, 0x84];
            block.extend(if invalid_name {
                raw_literal(&[0xff], b"ok", huffman)
            } else {
                raw_literal(b"x-obs-text", &[0xff], huffman)
            });
            block.extend(raw_literal(b"x-good", b"synced", huffman));
            // Fragment to ensure HTTP conversion occurs only after END_HEADERS.
            let split = block.len() / 2;
            s.recv(&wire_frame(
                FrameType::Headers,
                1,
                Flags::END_STREAM,
                &block[..split],
            ))
            .unwrap();
            assert!(events(&mut s).is_empty());
            s.recv(&wire_frame(
                FrameType::Continuation,
                1,
                Flags::END_HEADERS,
                &block[split..],
            ))
            .unwrap();
            assert_protocol_reset(&mut s, 1);
            // Index 62 is the entry that followed the rejected field.
            wire_block(&mut s, 3, &[0x83, 0x86, 0x84, 0xbe], true);
            assert!(matches!(&events(&mut s)[0], Event::Headers { headers, .. }
                if headers.last() == Some(&hf("x-good", "synced"))));
            // Referencing the rejected entry still decodes successfully at the
            // HPACK layer and rejects only HTTP stream 5, not the connection.
            wire_block(&mut s, 5, &[0x83, 0x86, 0x84, 0xbf], true);
            assert_protocol_reset(&mut s, 5);
            wire_block(&mut s, 7, &[0x83, 0x86, 0x84, 0xbe], true);
            assert!(matches!(
                &events(&mut s)[0],
                Event::Headers { stream_id: 7, .. }
            ));
        }
    }
}

#[test]
fn non_utf8_indexed_names_are_reused_as_octets_and_not_compression_errors() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let mut block = vec![0x83, 0x86, 0x84];
    block.extend(raw_literal(&[0xff], b"value", false));
    wire_block(&mut s, 1, &block, true);
    assert_protocol_reset(&mut s, 1);
    let mut block = vec![0x83, 0x86, 0x84, 0x7e]; // Incremental literal, indexed name 62.
    block.extend(raw_string(b"new value", false));
    block.extend(raw_literal(b"x-valid", b"after-invalid-name", false));
    wire_block(&mut s, 3, &block, true);
    assert_protocol_reset(&mut s, 3);
    wire_block(&mut s, 5, &[0x83, 0x86, 0x84, 0xbe], true);
    assert!(matches!(&events(&mut s)[0], Event::Headers { headers, .. }
        if headers.last() == Some(&hf("x-valid", "after-invalid-name"))));
}

#[test]
fn octet_strings_on_discarded_streams_still_update_the_table() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    exchange(&mut c, &mut s);
    c.reset_stream(id, ErrorCode::Cancel).unwrap();
    exchange(&mut c, &mut s);
    events(&mut c);
    let mut block = raw_literal(&[0xff], b"bad-name", false);
    block.extend(raw_literal(b"x-good", b"after-discard", false));
    wire_block(&mut c, id, &block, true);
    assert!(events(&mut c).is_empty());
    let next = c.open_stream(request_headers(), true).unwrap();
    wire_block(&mut c, next, &[0x88, 0xbe], true); // status 200 and new dynamic field.
    assert!(matches!(&events(&mut c)[0], Event::Headers { headers, .. }
        if headers.last() == Some(&hf("x-good", "after-discard"))));
}

#[test]
fn http_https_paths_reject_relative_targets_and_fragments_inbound_and_outbound() {
    for scheme in ["http", "https", "HTTP", "HTTPS"] {
        for (method, path, valid) in [
            ("POST", "relative", false),
            ("POST", "relative#fragment", false),
            ("POST", "/path#fragment", false),
            ("POST", "/?q=a#fragment", false),
            ("POST", "?q=a", false),
            ("POST", "https://host/path", false),
            ("POST", "*", false),
            ("OPTIONS", "*", true),
            ("POST", "/", true),
            ("POST", "/path?query=1", true),
            ("POST", "/%23fragment?q=%23", true),
        ] {
            let mut c = Connection::client(Config::default());
            let mut s = Connection::server(Config::default());
            exchange(&mut c, &mut s);
            let headers = vec![
                hf(":method", method),
                hf(":scheme", scheme),
                hf(":path", path),
            ];
            let result = c.open_stream(headers.clone(), true);
            if valid {
                assert_eq!(result.unwrap(), 1);
            } else {
                assert!(
                    matches!(result, Err(Error::InvalidHeaders(_))),
                    "{scheme} {method} {path}"
                );
                assert_eq!(c.next_local_id, 1);
                assert_eq!(c.stream_count(), 0);
                assert!(!c.has_output());
            }
            wire_headers(&mut s, &mut Encoder::new(), 1, &headers, true);
            if valid {
                assert!(matches!(
                    &events(&mut s)[0],
                    Event::Headers {
                        end_stream: true,
                        ..
                    }
                ));
            } else {
                assert_protocol_reset(&mut s, 1);
            }
        }
    }
}

fn receiving(request: bool, mode: FlowControl) -> (Connection, StreamId) {
    let config = Config {
        flow_control: mode,
        ..Config::default()
    };
    let mut c = Connection::client(config);
    let mut s = Connection::server(config);
    exchange(&mut c, &mut s);
    if request {
        (s, 1)
    } else {
        let id = c.open_stream(request_headers(), true).unwrap();
        exchange(&mut c, &mut s);
        (c, id)
    }
}

fn initial(request: bool, length: u64) -> Vec<HeaderField> {
    let mut headers = if request {
        request_headers()
    } else {
        vec![hf(":status", "200")]
    };
    headers.push(hf("content-length", &length.to_string()));
    headers
}

#[test]
fn content_length_initial_end_stream_checks_requests_and_responses() {
    for request in [false, true] {
        for mode in [FlowControl::Automatic, FlowControl::Manual] {
            for length in [0, 1] {
                let (mut conn, id) = receiving(request, mode);
                wire_headers(
                    &mut conn,
                    &mut Encoder::new(),
                    id,
                    &initial(request, length),
                    true,
                );
                if length == 0 {
                    assert!(matches!(
                        &events(&mut conn)[0],
                        Event::Headers {
                            end_stream: true,
                            ..
                        }
                    ));
                } else {
                    assert_protocol_reset(&mut conn, id);
                }
            }
        }
    }
    let mut c = Connection::client(Config::default());
    c.take_output();
    assert!(matches!(
        c.open_stream(initial(true, 1), true),
        Err(Error::InvalidHeaders(_))
    ));
    assert_eq!(c.next_local_id, 1);
    assert_eq!(c.stream_count(), 0);
    assert!(!c.has_output());
}

#[test]
fn inbound_body_overruns_and_terminal_underruns_are_stream_local() {
    for request in [false, true] {
        for mode in [FlowControl::Automatic, FlowControl::Manual] {
            for (len, end) in [(4, false), (4, true), (2, true), (0, true)] {
                let (mut conn, id) = receiving(request, mode);
                let mut encoder = Encoder::new();
                wire_headers(&mut conn, &mut encoder, id, &initial(request, 3), false);
                events(&mut conn);
                conn.recv(&data_frame(
                    id,
                    len,
                    if end { Flags::END_STREAM } else { 0 },
                ))
                .unwrap();
                assert_protocol_reset(&mut conn, id);
                assert_eq!(conn.conn_recv_window, DEFAULT_WINDOW);
                // Another valid field section on the same compression context.
                let next = if request {
                    id + 2
                } else {
                    conn.open_stream(request_headers(), true).unwrap()
                };
                wire_headers(&mut conn, &mut encoder, next, &initial(request, 0), true);
                assert!(matches!(&events(&mut conn)[0], Event::Headers { .. }));
            }
        }
    }
}

#[test]
fn body_counts_accumulate_and_trailers_validate_terminal_length() {
    for request in [false, true] {
        for mode in [FlowControl::Automatic, FlowControl::Manual] {
            for (len, trailers, valid) in [(2, true, false), (3, true, true), (3, false, true)] {
                let (mut conn, id) = receiving(request, mode);
                let mut encoder = Encoder::new();
                wire_headers(&mut conn, &mut encoder, id, &initial(request, 3), false);
                events(&mut conn);
                conn.recv(&data_frame(id, 1, 0)).unwrap();
                assert_eq!(data_len(&events(&mut conn), id), 1);
                conn.recv(&data_frame(
                    id,
                    len - 1,
                    if trailers { 0 } else { Flags::END_STREAM },
                ))
                .unwrap();
                if trailers {
                    assert_eq!(data_len(&events(&mut conn), id), len - 1);
                    wire_headers(&mut conn, &mut encoder, id, &[hf("x-trailer", "end")], true);
                }
                if valid {
                    assert!(events(&mut conn).iter().any(|e| matches!(
                        e,
                        Event::Headers {
                            end_stream: true,
                            ..
                        } | Event::Data {
                            end_stream: true,
                            ..
                        }
                    )));
                } else {
                    assert_protocol_reset(&mut conn, id);
                }
            }
            let (mut conn, id) = receiving(request, mode);
            wire_headers(
                &mut conn,
                &mut Encoder::new(),
                id,
                &initial(request, 3),
                false,
            );
            events(&mut conn);
            conn.recv(&data_frame(id, 2, 0)).unwrap();
            assert_eq!(data_len(&events(&mut conn), id), 2);
            conn.recv(&data_frame(id, 2, 0)).unwrap();
            assert_protocol_reset(&mut conn, id);
            assert_eq!(conn.conn_recv_window, DEFAULT_WINDOW);
        }
    }
}

#[test]
fn content_length_counts_body_without_padding() {
    for request in [false, true] {
        for mode in [FlowControl::Automatic, FlowControl::Manual] {
            let (mut conn, id) = receiving(request, mode);
            wire_headers(
                &mut conn,
                &mut Encoder::new(),
                id,
                &initial(request, 3),
                false,
            );
            events(&mut conn);
            let mut frame = data_frame(id, 6, Flags::PADDED | Flags::END_STREAM);
            frame[9] = 2; // One length byte, three body bytes, two padding bytes.
            conn.recv(&frame).unwrap();
            assert_eq!(data_len(&events(&mut conn), id), 3);
            assert!(!conn.is_closed());
        }
    }
}

#[test]
fn outbound_body_validation_is_atomic_and_counts_queued_bytes() {
    for request in [false, true] {
        let mut c = Connection::client(Config {
            initial_window_size: 0,
            ..Config::default()
        });
        let mut s = Connection::server(Config {
            initial_window_size: 0,
            ..Config::default()
        });
        exchange(&mut c, &mut s);
        let id = c
            .open_stream(
                if request {
                    initial(true, 3)
                } else {
                    request_headers()
                },
                !request,
            )
            .unwrap();
        exchange(&mut c, &mut s);
        let conn = if request { &mut c } else { &mut s };
        if !request {
            conn.send_headers(id, initial(false, 3), false).unwrap();
        }
        conn.take_output();
        conn.send_data(id, vec![1; 2], false).unwrap();
        assert_eq!(conn.queued_send_bytes(id), Some(2));
        let body = conn.streams[&id].send_body;
        let state = conn.streams[&id].state;
        let queued = conn.streams[&id].outbound.len();
        for (data, end) in [(vec![1; 2], false), (vec![1; 2], true), (vec![], true)] {
            assert!(matches!(
                conn.send_data(id, data, end),
                Err(Error::InvalidHeaders(_))
            ));
            assert_eq!(conn.streams[&id].send_body, body);
            assert_eq!(conn.streams[&id].state, state);
            assert_eq!(conn.streams[&id].send_phase, Phase::Body);
            assert_eq!(conn.streams[&id].outbound.len(), queued);
            assert_eq!(conn.queued_send_bytes(id), Some(2));
            assert!(!conn.has_output());
        }
        assert!(matches!(
            conn.send_headers(id, vec![], true),
            Err(Error::InvalidHeaders(_))
        ));
        assert_eq!(conn.streams[&id].outbound.len(), queued);
        assert_eq!(conn.streams[&id].send_body, body);
        assert!(!conn.has_output());
        conn.send_data(id, vec![1], false).unwrap();
        conn.send_headers(id, vec![], true).unwrap();
        assert_eq!(conn.queued_send_bytes(id), Some(3));
    }
}

#[test]
fn outbound_response_header_end_stream_mismatch_does_not_mutate_stream() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    let id = c.open_stream(request_headers(), true).unwrap();
    exchange(&mut c, &mut s);
    let body = s.streams[&id].send_body;
    let state = s.streams[&id].state;
    assert!(matches!(
        s.send_headers(id, initial(false, 1), true),
        Err(Error::InvalidHeaders(_))
    ));
    assert_eq!(s.streams[&id].send_body, body);
    assert_eq!(s.streams[&id].state, state);
    assert_eq!(s.streams[&id].send_phase, Phase::Initial);
    assert!(!s.has_output());
    assert!(s.encoder_update_pending);
    s.send_headers(id, initial(false, 0), true).unwrap();
    exchange(&mut c, &mut s);
    assert!(matches!(
        &events(&mut c)[0],
        Event::Headers {
            end_stream: true,
            ..
        }
    ));
}

#[test]
fn head_and_304_lengths_describe_metadata_not_a_response_body() {
    for (method, status) in [("HEAD", "200"), ("GET", "304"), ("HEAD", "304")] {
        for header_end in [false, true] {
            let mut c = Connection::client(Config::default());
            let mut s = Connection::server(Config::default());
            let mut headers = request_headers();
            headers[0] = hf(":method", method);
            let id = c.open_stream(headers, true).unwrap();
            exchange(&mut c, &mut s);
            s.send_headers(
                id,
                vec![hf(":status", status), hf("content-length", "999")],
                header_end,
            )
            .unwrap();
            if !header_end {
                s.send_data(id, vec![], true).unwrap();
            }
            exchange(&mut c, &mut s);
            assert!(
                events(&mut c)
                    .iter()
                    .any(|e| matches!(e, Event::Headers { .. }))
            );
            assert!(!c.has_stream(id));
        }
        let mut c = Connection::client(Config::default());
        let mut s = Connection::server(Config::default());
        let mut headers = request_headers();
        headers[0] = hf(":method", method);
        let id = c.open_stream(headers, true).unwrap();
        exchange(&mut c, &mut s);
        s.send_headers(
            id,
            vec![hf(":status", status), hf("content-length", "999")],
            false,
        )
        .unwrap();
        exchange(&mut c, &mut s);
        events(&mut c);
        assert!(matches!(
            s.send_data(id, vec![1], false),
            Err(Error::InvalidHeaders(_))
        ));
        assert!(!s.has_output());
        c.recv(&data_frame(id, 1, 0)).unwrap();
        assert_protocol_reset(&mut c, id);
    }
}

#[test]
fn successful_connect_ignores_lengths_in_both_tunnel_halves() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    let id = c
        .open_stream(
            vec![
                hf(":method", "CONNECT"),
                hf(":authority", "host:443"),
                hf("content-length", "0"),
            ],
            false,
        )
        .unwrap();
    exchange(&mut c, &mut s);
    events(&mut s);
    s.send_headers(
        id,
        vec![hf(":status", "200"), hf("content-length", "999")],
        false,
    )
    .unwrap();
    exchange(&mut c, &mut s);
    events(&mut c);
    c.send_data(id, vec![1; 5], true).unwrap();
    s.send_data(id, vec![2; 7], true).unwrap();
    exchange(&mut c, &mut s);
    assert_eq!(data_len(&events(&mut s), id), 5);
    assert_eq!(data_len(&events(&mut c), id), 7);
    assert!(!c.has_stream(id));
    assert!(!s.has_stream(id));
}

#[test]
fn unsuccessful_connect_uses_normal_body_length_validation() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    let id = c
        .open_stream(
            vec![
                hf(":method", "CONNECT"),
                hf(":authority", "host:443"),
                hf("content-length", "0"),
            ],
            false,
        )
        .unwrap();
    exchange(&mut c, &mut s);
    assert!(matches!(
        s.send_headers(
            id,
            vec![hf(":status", "403"), hf("content-length", "1")],
            true
        ),
        Err(Error::InvalidHeaders(_))
    ));
    s.send_headers(
        id,
        vec![hf(":status", "403"), hf("content-length", "0")],
        true,
    )
    .unwrap();
    exchange(&mut c, &mut s);
    assert!(matches!(
        c.send_data(id, vec![1], true),
        Err(Error::InvalidHeaders(_))
    ));
    c.send_data(id, vec![], true).unwrap();
    exchange(&mut c, &mut s);
    assert!(!c.has_stream(id));
}

#[test]
fn informational_and_204_statuses_reject_content_length_and_204_has_no_body() {
    for status in ["100", "103", "204"] {
        let mut c = Connection::client(Config::default());
        let mut s = Connection::server(Config::default());
        let id = c.open_stream(request_headers(), true).unwrap();
        exchange(&mut c, &mut s);
        let headers = vec![hf(":status", status), hf("content-length", "0")];
        assert!(matches!(
            s.send_headers(id, headers.clone(), false),
            Err(Error::InvalidHeaders(_))
        ));
        wire_headers(&mut c, &mut Encoder::new(), id, &headers, false);
        assert_protocol_reset(&mut c, id);
    }
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    let id = c.open_stream(request_headers(), true).unwrap();
    exchange(&mut c, &mut s);
    s.send_headers(id, vec![hf(":status", "204")], false)
        .unwrap();
    assert!(matches!(
        s.send_data(id, vec![1], true),
        Err(Error::InvalidHeaders(_))
    ));
    s.send_data(id, vec![], true).unwrap();
    exchange(&mut c, &mut s);
    assert!(!c.has_stream(id));
}
