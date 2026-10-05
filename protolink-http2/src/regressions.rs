use super::*;

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

fn wire_headers(
    conn: &mut Connection,
    encoder: &mut Encoder,
    id: StreamId,
    fields: &[HeaderField],
    end: bool,
) {
    let block = encoder.encode(fields);
    conn.recv(&wire_frame(
        FrameType::Headers,
        id,
        Flags::END_HEADERS | if end { Flags::END_STREAM } else { 0 },
        &block,
    ))
    .unwrap();
}

fn setting(conn: &mut Connection, id: SettingId, value: u32) {
    conn.recv(&wire_frame(
        FrameType::Settings,
        0,
        0,
        &encode_settings(&[Setting { id, value }]),
    ))
    .unwrap();
}

fn assert_reset(conn: &mut Connection, id: StreamId, code: ErrorCode) {
    assert_eq!(
        events(conn),
        vec![Event::Reset {
            stream_id: id,
            error_code: code
        }]
    );
    assert!(!conn.is_closed());
}

#[test]
fn admission_obeys_zero_one_reductions_increases_and_completion() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config {
        max_concurrent_streams: 0,
        ..Config::default()
    });
    exchange(&mut c, &mut s);
    let next = c.next_local_id;
    assert_eq!(
        c.open_stream(request_headers(), true),
        Err(Error::StreamLimit)
    );
    assert_eq!(c.next_local_id, next);
    assert!(!c.has_output());
    setting(&mut c, SettingId::MaxConcurrentStreams, 1);
    c.take_output();
    let id = c.open_stream(request_headers(), true).unwrap();
    let before = c.pending_output().to_vec();
    assert_eq!(
        c.open_stream(request_headers(), true),
        Err(Error::StreamLimit)
    );
    assert_eq!(c.pending_output(), before);
    assert_eq!(c.next_local_id, 3);
    setting(&mut c, SettingId::MaxConcurrentStreams, 0);
    assert_eq!(
        c.open_stream(request_headers(), true),
        Err(Error::StreamLimit)
    );
    setting(&mut c, SettingId::MaxConcurrentStreams, 2);
    let second = c.open_stream(request_headers(), true).unwrap();
    let mut encoder = Encoder::new();
    wire_headers(&mut c, &mut encoder, id, &[hf(":status", "200")], true);
    assert!(!c.has_stream(id));
    setting(&mut c, SettingId::MaxConcurrentStreams, 1);
    assert_eq!(
        c.open_stream(request_headers(), true),
        Err(Error::StreamLimit)
    );
    wire_headers(&mut c, &mut encoder, second, &[hf(":status", "200")], true);
    assert_eq!(c.open_stream(request_headers(), true).unwrap(), 5);
}

#[test]
fn last_stream_id_is_used_once_without_wire_wrap_or_mutation() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    c.next_local_id = 0x7fff_ffff;
    let id = c.open_stream(request_headers(), true).unwrap();
    assert_eq!(id, 0x7fff_ffff);
    let before = c.pending_output().to_vec();
    assert_eq!(frames(&before)[0].2, id);
    assert_eq!(
        c.open_stream(request_headers(), true),
        Err(Error::StreamIdExhausted)
    );
    assert_eq!(c.pending_output(), before);
    assert_eq!(c.next_local_id, 0x8000_0001);
    let mut encoder = Encoder::new();
    wire_headers(&mut c, &mut encoder, id, &[hf(":status", "200")], true);
    assert!(
        !c.has_stream(id),
        "existing stream finishes after exhaustion"
    );
}

#[test]
fn initial_zero_encoder_update_precedes_fragmentation_and_is_not_repeated() {
    for initial_zero in [false, true] {
        let mut c = Connection::client(Config::default());
        let mut s = Connection::server(Config {
            max_header_list_size: 64 * 1024,
            ..Config::default()
        });
        if initial_zero {
            // Put zero in the very first SETTINGS, not in a later settings frame.
            s.output.clear();
            s.write_frame(
                FrameType::Settings,
                0,
                0,
                &encode_settings(&[
                    Setting {
                        id: SettingId::HeaderTableSize,
                        value: 0,
                    },
                    Setting {
                        id: SettingId::MaxHeaderListSize,
                        value: 64 * 1024,
                    },
                ]),
            );
        }
        exchange(&mut c, &mut s);
        let mut headers = request_headers();
        headers.push(hf("x-big", &"a".repeat(20_000)));
        c.open_stream(headers.clone(), true).unwrap();
        let first = c.take_output();
        let f = frames(&first);
        assert_eq!(f[0].3[0], 0x20);
        assert_eq!(f[0].0, FrameType::Headers as u8);
        assert_eq!(f[1].0, FrameType::Continuation as u8);
        let block: Vec<_> = f.iter().flat_map(|f| f.3.iter().copied()).collect();
        let mut strict = hpack::Decoder::new();
        assert_eq!(
            strict
                .decode(&block, 64 * 1024)
                .unwrap()
                .into_iter()
                .map(|h| h.into_field().unwrap())
                .collect::<Vec<_>>(),
            headers
        );
        s.recv(&first).unwrap();
        assert_eq!(s.decoder.decode(&[], 1).unwrap(), vec![]);
        // A later reduction needs no further update: the synchronized table is already zero.
        setting(&mut c, SettingId::HeaderTableSize, 0);
        c.take_output();
        c.open_stream(request_headers(), true).unwrap();
        let f = frames(&c.take_output());
        assert_ne!(f[0].3[0], 0x20);
        let block: Vec<_> = f.iter().flat_map(|f| f.3.iter().copied()).collect();
        strict.decode(&block, 8192).unwrap();
    }
}

#[test]
fn oversized_hpack_updates_and_amplification_are_connection_errors() {
    for amplification in [false, true] {
        let mut c = Connection::client(Config::default());
        let mut s = Connection::server(Config::default());
        exchange(&mut c, &mut s);
        let mut encoder = Encoder::new();
        let block = if amplification {
            let mut headers = request_headers();
            headers.push(hf("x-big", &"v".repeat(3900)));
            wire_headers(&mut s, &mut encoder, 1, &headers, true);
            events(&mut s);
            let mut block = encoder.encode(&request_headers());
            block.extend_from_slice(&vec![0xbe; 16_000]);
            block
        } else {
            zerodds_hpack::encode_integer(4097, 5, 0x20)
        };
        let err = s
            .recv(&wire_frame(
                FrameType::Headers,
                3,
                Flags::END_HEADERS,
                &block,
            ))
            .unwrap_err();
        assert!(
            matches!(err, Error::Connection { code, .. } if code == if amplification { ErrorCode::EnhanceYourCalm } else { ErrorCode::CompressionError })
        );
        assert!(events(&mut s).is_empty(), "no oversized section published");
        assert!(s.decoder.materialized <= 8192);
        assert!(
            frames(&s.take_output())
                .iter()
                .any(|f| f.0 == FrameType::GoAway as u8)
        );
    }
}

fn malformed_requests() -> Vec<Vec<HeaderField>> {
    let valid = request_headers();
    let mut cases = vec![vec![], vec![hf(":method", "GET"), hf(":path", "/")]];
    for extra in [
        hf(":method", "GET"),
        hf(":unknown", "x"),
        hf(":status", "200"),
        hf("Upper", "x"),
        hf("bad name", "x"),
        hf("", "x"),
        hf("x", "a\rb"),
        hf("x", "a\nb"),
        hf("x", "a\0b"),
        hf("x", "a\u{7f}b"),
        hf("x", " leading"),
        hf("x", "trailing\t"),
        hf("connection", "close"),
        hf("proxy-connection", "close"),
        hf("keep-alive", "yes"),
        hf("transfer-encoding", "chunked"),
        hf("upgrade", "h2c"),
        hf("te", "gzip"),
        hf("content-length", "-1"),
    ] {
        let mut h = valid.clone();
        h.push(extra);
        cases.push(h);
    }
    for (index, value) in [(0, ""), (1, ""), (2, "")] {
        let mut h = valid.clone();
        h[index].value = value.into();
        cases.push(h);
    }
    let mut late = vec![hf("x", "y")];
    late.extend(valid.clone());
    cases.push(late);
    let mut lengths = valid;
    lengths.extend([hf("content-length", "1"), hf("content-length", "2")]);
    cases.push(lengths);
    cases
}

#[test]
fn invalid_requests_reset_only_the_stream_and_preserve_hpack_sync() {
    for malformed in malformed_requests() {
        let mut c = Connection::client(Config::default());
        let mut s = Connection::server(Config::default());
        exchange(&mut c, &mut s);
        let mut encoder = Encoder::new();
        wire_headers(&mut s, &mut encoder, 1, &malformed, true);
        assert_reset(&mut s, 1, ErrorCode::ProtocolError);
        // Reuse dynamic entries learned from the rejected field section.
        let mut valid = request_headers();
        valid.push(hf("x-good", "same-compression-context"));
        wire_headers(&mut s, &mut encoder, 3, &valid, true);
        assert_eq!(
            events(&mut s),
            vec![Event::Headers {
                stream_id: 3,
                headers: valid,
                end_stream: true
            }]
        );
    }
}

#[test]
fn invalid_outbound_requests_leave_id_output_and_stream_state_unchanged() {
    let mut c = Connection::client(Config::default());
    c.take_output();
    for malformed in malformed_requests() {
        assert!(matches!(
            c.open_stream(malformed, false),
            Err(Error::InvalidHeaders(_))
        ));
        assert_eq!(c.next_local_id, 1);
        assert_eq!(c.stream_count(), 0);
        assert!(!c.has_output());
        assert!(c.encoder_update_pending);
    }
    assert_eq!(c.open_stream(request_headers(), false).unwrap(), 1);
}

#[test]
fn informational_responses_final_headers_body_and_trailers() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), true).unwrap();
    exchange(&mut c, &mut s);
    events(&mut s);
    for status in ["103", "100"] {
        s.send_headers(id, vec![hf(":status", status)], false)
            .unwrap();
        assert!(matches!(
            s.send_data(id, vec![1], false),
            Err(Error::InvalidHeaders(_))
        ));
    }
    s.send_headers(id, vec![hf(":status", "200"), hf("te", "trailers")], false)
        .unwrap();
    s.send_data(id, vec![1], false).unwrap();
    s.send_headers(id, vec![hf("x-trailer", "ok")], true)
        .unwrap();
    exchange(&mut c, &mut s);
    let ev = events(&mut c);
    assert_eq!(ev.len(), 5);
    assert!(matches!(&ev[0], Event::Headers { headers, .. } if headers[0].value == "103"));
    assert!(matches!(&ev[1], Event::Headers { headers, .. } if headers[0].value == "100"));
    assert!(matches!(
        &ev[4],
        Event::Headers {
            end_stream: true,
            ..
        }
    ));
    assert!(!c.has_stream(id));
}

#[test]
fn data_before_final_response_headers_resets_in_both_flow_modes() {
    for mode in [FlowControl::Automatic, FlowControl::Manual] {
        for informational in [false, true] {
            let mut c = Connection::client(Config {
                flow_control: mode,
                ..Config::default()
            });
            let mut s = Connection::server(Config::default());
            exchange(&mut c, &mut s);
            let id = c.open_stream(request_headers(), false).unwrap();
            if informational {
                wire_headers(
                    &mut c,
                    &mut Encoder::new(),
                    id,
                    &[hf(":status", "103")],
                    false,
                );
                events(&mut c);
            }
            c.recv(&data_frame(id, 1, 0)).unwrap();
            assert_reset(&mut c, id, ErrorCode::ProtocolError);
            assert_eq!(c.conn_recv_window, DEFAULT_WINDOW);
        }
    }
}

#[test]
fn response_and_trailer_validation_is_atomic_outbound_and_stream_local_inbound() {
    let invalid_sections = vec![
        (vec![], false, false),
        (
            vec![hf(":status", "200"), hf(":status", "200")],
            false,
            false,
        ),
        (vec![hf(":status", "101")], false, false),
        (vec![hf(":status", "99")], false, false),
        (vec![hf(":status", "600")], false, false),
        (vec![hf(":status", "103")], true, false),
        (vec![hf(":method", "GET")], false, false),
        (vec![hf("x", "y"), hf(":status", "200")], false, false),
        (vec![hf("x-trailer", "ok")], false, true),
        (vec![hf(":status", "200")], true, true),
    ];
    for (fields, end, final_first) in invalid_sections {
        let mut c = Connection::client(Config::default());
        let mut s = Connection::server(Config::default());
        exchange(&mut c, &mut s);
        let id = c.open_stream(request_headers(), true).unwrap();
        exchange(&mut c, &mut s);
        events(&mut s);
        if final_first {
            s.send_headers(id, vec![hf(":status", "200")], false)
                .unwrap();
            exchange(&mut c, &mut s);
            events(&mut c);
        }
        let old_phase = s.streams[&id].send_phase;
        let old_state = s.streams[&id].state;
        assert!(matches!(
            s.send_headers(id, fields.clone(), end),
            Err(Error::InvalidHeaders(_))
        ));
        assert_eq!(s.streams[&id].send_phase, old_phase);
        assert_eq!(s.streams[&id].state, old_state);
        assert!(!s.has_output());
        wire_headers(&mut c, &mut Encoder::new(), id, &fields, end);
        assert_reset(&mut c, id, ErrorCode::ProtocolError);
    }
}

#[test]
fn connect_and_empty_terminal_trailers_are_accepted() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let id = c
        .open_stream(
            vec![hf(":method", "CONNECT"), hf(":authority", "host:443")],
            false,
        )
        .unwrap();
    c.send_headers(id, vec![], true).unwrap();
    exchange(&mut c, &mut s);
    assert_eq!(events(&mut s).len(), 2);
    s.send_headers(id, vec![hf(":status", "200")], false)
        .unwrap();
    s.send_headers(id, vec![], true).unwrap();
    exchange(&mut c, &mut s);
    assert_eq!(events(&mut c).len(), 2);
    assert!(!c.has_stream(id));
}

#[test]
fn automatic_receive_windows_reject_zero_small_and_padded_overruns() {
    for (window, len, padded) in [(0, 1, false), (100, 101, false), (100, 101, true)] {
        let mut c = Connection::client(Config::default());
        let mut s = Connection::server(Config {
            initial_window_size: window,
            ..Config::default()
        });
        exchange(&mut c, &mut s);
        let id = c.open_stream(request_headers(), false).unwrap();
        exchange(&mut c, &mut s);
        events(&mut s);
        let mut frame = data_frame(id, len, if padded { Flags::PADDED } else { 0 });
        if padded {
            frame[9] = 100;
        }
        s.recv(&frame).unwrap();
        assert_reset(&mut s, id, ErrorCode::FlowControlError);
        assert_eq!(s.conn_recv_window, DEFAULT_WINDOW);
    }
}

#[test]
fn automatic_small_window_valid_data_and_padding_are_replenished() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config {
        initial_window_size: 100,
        ..Config::default()
    });
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    exchange(&mut c, &mut s);
    events(&mut s);
    for _ in 0..3 {
        let mut frame = data_frame(id, 100, Flags::PADDED);
        frame[9] = 6;
        s.recv(&frame).unwrap();
        assert_eq!(data_len(&events(&mut s), id), 93);
        assert_eq!(s.streams[&id].recv_window, 100);
        assert_eq!(s.conn_recv_window, DEFAULT_WINDOW);
        let updates = frames(&s.take_output());
        assert_eq!(updates.len(), 2);
        assert!(
            updates
                .iter()
                .all(|f| f.0 == FrameType::WindowUpdate as u8 && f.3 == 100u32.to_be_bytes())
        );
    }
}

#[test]
fn automatic_settings_ack_adjusts_existing_streams() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config {
        initial_window_size: 100,
        ..Config::default()
    });
    let id = c.open_stream(request_headers(), false).unwrap();
    c.send_data(id, vec![1; 500], false).unwrap();
    pump(&mut c, &mut s, usize::MAX).unwrap();
    assert_eq!(s.streams[&id].recv_window, DEFAULT_WINDOW);
    exchange(&mut c, &mut s);
    events(&mut s);
    assert_eq!(s.streams[&id].recv_window, 100);
    s.recv(&data_frame(id, 101, 0)).unwrap();
    assert_reset(&mut s, id, ErrorCode::FlowControlError);
}

#[test]
fn retained_credit_survives_completion_and_reset_and_releases_exactly_once() {
    for reset in [false, true] {
        let mut c = Connection::client(manual(65_535, 65_535));
        let mut s = Connection::server(Config::default());
        exchange(&mut c, &mut s);
        let id = c.open_stream(request_headers(), true).unwrap();
        c.retain_receive_capacity(id).unwrap();
        exchange(&mut c, &mut s);
        s.send_headers(id, vec![hf(":status", "200")], false)
            .unwrap();
        s.send_data(id, vec![9; 1000], !reset).unwrap();
        if reset {
            s.reset_stream(id, ErrorCode::Cancel).unwrap();
        }
        exchange(&mut c, &mut s);
        assert!(!c.has_stream(id));
        assert_eq!(c.stream_count(), 0);
        assert_eq!(c.unreleased_recv_bytes(id), Some(1000));
        assert_eq!(c.conn_recv_window, DEFAULT_WINDOW - 1000);
        c.release_capacity(id, 400);
        assert_eq!(c.unreleased_recv_bytes(id), Some(600));
        assert_eq!(c.conn_recv_window, DEFAULT_WINDOW - 600);
        c.release_capacity(id, usize::MAX);
        assert_eq!(c.unreleased_recv_bytes(id), None);
        assert_eq!(c.conn_recv_window, DEFAULT_WINDOW);
        let out = c.take_output();
        let updates = frames(&out);
        assert_eq!(updates.len(), 2);
        assert!(
            updates
                .iter()
                .all(|f| f.0 == FrameType::WindowUpdate as u8 && f.2 == 0)
        );
        c.release_capacity(id, 1000);
        assert!(!c.has_output());
    }
}

#[test]
fn repeated_completed_unread_streams_remain_bounded_by_connection_credit() {
    let mut c = Connection::client(manual(65_535, 65_535));
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let mut completed = Vec::new();
    for _ in 0..2 {
        let id = c.open_stream(request_headers(), true).unwrap();
        c.retain_receive_capacity(id).unwrap();
        exchange(&mut c, &mut s);
        s.send_headers(id, vec![hf(":status", "200")], false)
            .unwrap();
        s.send_data(id, vec![1; 30_000], true).unwrap();
        exchange(&mut c, &mut s);
        assert_eq!(data_len(&events(&mut c), id), 30_000);
        assert!(!c.has_stream(id));
        completed.push(id);
    }
    assert_eq!(c.conn_recv_window, 5535);
    let id = c.open_stream(request_headers(), true).unwrap();
    c.retain_receive_capacity(id).unwrap();
    exchange(&mut c, &mut s);
    s.send_headers(id, vec![hf(":status", "200")], false)
        .unwrap();
    s.send_data(id, vec![1; 30_000], true).unwrap();
    exchange(&mut c, &mut s);
    assert_eq!(data_len(&events(&mut c), id), 5535);
    assert_eq!(c.conn_recv_window, 0);
    assert_eq!(s.queued_send_bytes(id), Some(24_465));
    c.release_capacity(completed[0], 30_000);
    exchange(&mut c, &mut s);
    assert_eq!(data_len(&events(&mut c), id), 24_465);
    assert!(!c.has_stream(id));
    c.release_capacity(completed[1], usize::MAX);
    c.release_capacity(id, usize::MAX);
    assert_eq!(c.conn_recv_window, DEFAULT_WINDOW);
    assert!(c.retained_credit.is_empty());
}
