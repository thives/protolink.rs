//! Output admission: the serialized prefix, the handshake boundary, floods of
//! replies, and terminal failures that must not be overwritten.

use super::*;

/// A connection that may hold `limit` bytes of serialized output.
fn limited(role: Role, limit: usize) -> Connection {
    Connection::new(
        role,
        Config {
            max_pending_output_bytes: limit,
            ..Config::default()
        },
    )
}

/// Bytes of the handshake batch of `role` under `config`.
fn handshake_len(role: Role, config: Config) -> usize {
    Connection::new(
        role,
        Config {
            max_pending_output_bytes: usize::MAX,
            ..config
        },
    )
    .pending_output()
    .len()
}

fn ping(payload: [u8; 8]) -> Vec<u8> {
    wire_frame(FrameType::Ping, 0, 0, &payload)
}

/// A server with `limit` bytes of output that has received the client's
/// preface and SETTINGS, so its SETTINGS ACK is queued too.
fn server_after_handshake(limit: usize) -> Connection {
    let mut c = Connection::client(Config::default());
    let mut s = limited(Role::Server, limit);
    s.recv(&c.take_output()).unwrap();
    s
}

#[test]
fn output_limit_preface_boundaries() {
    let configs = [
        Config::default(),
        // Adds a connection-level WINDOW_UPDATE to the batch.
        Config {
            connection_window_size: 1 << 20,
            ..Config::default()
        },
    ];
    for config in configs {
        for role in [Role::Client, Role::Server] {
            let required = handshake_len(role, config);
            assert!(required > 0);

            let exact = Connection::new(
                role,
                Config {
                    max_pending_output_bytes: required,
                    ..config
                },
            );
            assert!(exact.failure().is_none(), "{role:?} fits exactly");
            assert_eq!(exact.pending_output().len(), required);
            let spare = Connection::new(
                role,
                Config {
                    max_pending_output_bytes: required + 1,
                    ..config
                },
            );
            assert!(spare.failure().is_none(), "{role:?} has a byte to spare");
            assert_eq!(spare.pending_output(), exact.pending_output());

            // One byte short, and no room at all: no partial handshake is
            // written, and the failure is terminal from the start.
            for limit in [0, required - 1] {
                let mut c = Connection::new(
                    role,
                    Config {
                        max_pending_output_bytes: limit,
                        ..config
                    },
                );
                let expected = Error::OutputLimitExceeded {
                    limit,
                    pending: 0,
                    attempted: required,
                };
                assert_eq!(c.failure(), Some(&expected), "{role:?} limit {limit}");
                assert!(c.pending_output().is_empty());
                assert_eq!(c.recv(CLIENT_PREFACE), Err(expected.clone()));
                if role == Role::Client {
                    assert_eq!(c.open_stream(request_headers(), true), Err(expected));
                }
                assert!(c.is_closed());
            }
        }
    }
}

#[test]
fn output_limit_ping_and_settings_floods() {
    let before = server_after_handshake(usize::MAX).pending_output().len();

    // Each PING is answered with a 17-byte frame: five fit, and the sixth is
    // refused, with ten bytes to spare that a single frame cannot use.
    let limit = before + 5 * 17 + 10;
    let mut s = server_after_handshake(limit);
    for i in 0..5u8 {
        s.recv(&ping([i; 8])).unwrap();
    }
    let prefix = s.pending_output().to_vec();
    assert_eq!(prefix.len(), before + 5 * 17);

    let expected = Error::OutputLimitExceeded {
        limit,
        pending: prefix.len(),
        attempted: 17,
    };
    assert_eq!(s.recv(&ping([9; 8])), Err(expected.clone()));
    assert_eq!(
        s.pending_output(),
        prefix,
        "the refused reply is not appended"
    );

    // Further input is refused without growth, and the failure outlives draining.
    assert_eq!(s.recv(&ping([10; 8])), Err(expected.clone()));
    assert_eq!(
        s.recv(&wire_frame(FrameType::Settings, 0, 0, &[])),
        Err(expected.clone())
    );
    assert_eq!(s.pending_output(), prefix);
    assert_eq!(s.take_output(), prefix);
    assert_eq!(s.failure(), Some(&expected));
    assert_eq!(s.recv(&ping([11; 8])), Err(expected));
    assert!(s.pending_output().is_empty());
}

#[test]
fn output_limit_settings_flood_is_bounded_by_acks() {
    let before = server_after_handshake(usize::MAX).pending_output().len();
    let limit = before + 3 * 9;
    let mut s = server_after_handshake(limit);
    assert_eq!(s.pending_output().len(), before);
    for _ in 0..3 {
        s.recv(&wire_frame(FrameType::Settings, 0, 0, &[])).unwrap();
    }
    assert_eq!(s.pending_output().len(), limit);
    let refused = s.recv(&wire_frame(FrameType::Settings, 0, 0, &[]));
    assert_eq!(
        refused,
        Err(Error::OutputLimitExceeded {
            limit,
            pending: limit,
            attempted: 9,
        })
    );
    assert_eq!(s.pending_output().len(), limit);
}

#[test]
fn output_limit_is_sticky_and_goaway_cannot_exceed_it() {
    let before = server_after_handshake(usize::MAX).pending_output().len();
    // Three PINGs leave sixteen bytes of room, which a GOAWAY (17 bytes) cannot
    // use.
    let limit = before + 3 * 17 + 16;
    // DATA on stream 0 is a protocol error.
    let protocol = wire_frame(FrameType::Data, 0, 0, b"x");

    let mut s = server_after_handshake(limit);
    for i in 0..3u8 {
        s.recv(&ping([i; 8])).unwrap();
    }
    let prefix = s.pending_output().to_vec();

    // The GOAWAY does not fit, so the output limit is the failure reported,
    // not the protocol error that asked for it.
    let expected = Error::OutputLimitExceeded {
        limit,
        pending: prefix.len(),
        attempted: 17,
    };
    assert_eq!(s.recv(&protocol), Err(expected.clone()));
    assert_eq!(s.failure(), Some(&expected));
    assert_eq!(
        s.pending_output(),
        prefix,
        "no protocol error bytes were appended"
    );

    // Later input cannot replace the first failure, and draining keeps it.
    assert_eq!(s.recv(&ping([0; 8])), Err(expected.clone()));
    assert_eq!(s.take_output(), prefix);
    assert_eq!(s.failure(), Some(&expected));
    assert_eq!(s.recv(&protocol), Err(expected.clone()));
    assert!(!s.has_output());
    assert_eq!(
        s.send_headers(1, vec![hf(":status", "200")], true),
        Err(expected)
    );

    // With room for the GOAWAY, the same protocol error is reported as such and
    // its GOAWAY is the last frame written.
    let mut s = server_after_handshake(limit + 17);
    for i in 0..3u8 {
        s.recv(&ping([i; 8])).unwrap();
    }
    let err = s.recv(&protocol).unwrap_err();
    assert!(matches!(
        err,
        Error::Connection {
            code: ErrorCode::ProtocolError,
            ..
        }
    ));
    let out = s.take_output();
    assert_eq!(
        frames(&out[prefix.len()..]).last().unwrap().0,
        FrameType::GoAway as u8
    );
    assert!(matches!(s.failure(), Some(Error::Connection { .. })));
}

#[test]
fn serialized_prefix_survives_recv_reset_and_goaway() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    let id = c.open_stream(request_headers(), false).unwrap();
    c.send_data(id, b"request".to_vec(), true).unwrap();
    exchange(&mut c, &mut s);
    assert_eq!(events(&mut s).len(), 3);
    s.send_headers(id, vec![hf(":status", "200")], false)
        .unwrap();
    s.send_data(id, vec![5; 300], false).unwrap();
    let prefix = s.pending_output().to_vec();
    assert!(!prefix.is_empty());

    // Input that resets the stream, answers a PING and then fails the
    // connection leaves the bytes already serialized untouched.
    c.reset_stream(id, ErrorCode::Cancel).unwrap();
    s.recv(&c.take_output()).unwrap();
    assert!(s.pending_output().starts_with(&prefix));
    let after_reset = s.pending_output().to_vec();

    s.recv(&ping([1; 8])).unwrap();
    assert!(s.pending_output().starts_with(&after_reset));
    assert_eq!(s.pending_output().len(), after_reset.len() + 17);
    let after_ping = s.pending_output().to_vec();

    assert!(s.recv(&wire_frame(FrameType::Data, 0, 0, b"x")).is_err());
    assert!(s.pending_output().starts_with(&after_ping));
    let tail = frames(&s.pending_output()[after_ping.len()..]);
    assert_eq!(tail[0].0, FrameType::GoAway as u8);

    // Only explicit consumption removes bytes from the front.
    s.consume_output(prefix.len());
    assert!(s.pending_output().starts_with(&after_ping[prefix.len()..]));
    assert_eq!(
        s.pending_output().len(),
        after_ping.len() - prefix.len() + 17
    );
}

/// The transport accepts a prefix and its flush completes, while input fails
/// the connection in the same wake-up. Each event is accounted once: the bytes
/// left are the unwritten output followed by the GOAWAY, and no response is
/// retired by the flush while output is still waiting.
#[test]
fn simultaneous_rx_error_and_tx_completion_are_accounted() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    let id = c.open_stream(request_headers(), false).unwrap();
    c.send_data(id, b"request".to_vec(), true).unwrap();
    exchange(&mut c, &mut s);
    s.send_headers(id, vec![hf(":status", "200")], false)
        .unwrap();
    s.send_data(id, vec![5; 300], false).unwrap();
    let queued = s.pending_output().to_vec();
    let written = 100;

    assert!(s.recv(&wire_frame(FrameType::Data, 0, 0, b"x")).is_err());
    s.consume_output(written);

    let remaining = s.pending_output();
    assert!(remaining.starts_with(&queued[written..]));
    let tail = frames(&remaining[queued.len() - written..]);
    assert_eq!(
        tail.len(),
        1,
        "only the GOAWAY follows the unwritten output"
    );
    assert_eq!(tail[0].0, FrameType::GoAway as u8);
    assert_eq!(remaining.len(), queued.len() - written + 17);
}

#[test]
fn output_limit_refused_header_block_leaves_no_partial_frames() {
    let mut headers = request_headers();
    headers.push(hf("x-big", &"a".repeat(20_000)));
    let bound = header_block_bound(&headers, HPACK_TABLE_UPDATE_LEN, 16_384);
    let handshake = handshake_len(Role::Client, Config::default());

    // The block spans two frames. Admitting it needs its whole bound: one byte
    // less refuses it outright, with no HEADERS frame written first.
    let limit = handshake + bound - 1;
    let mut c = limited(Role::Client, limit);
    assert_eq!(
        c.open_stream(headers.clone(), true),
        Err(Error::OutputLimitExceeded {
            limit,
            pending: handshake,
            attempted: bound,
        })
    );
    assert_eq!(c.pending_output().len(), handshake);

    let mut c = limited(Role::Client, handshake + bound);
    c.open_stream(headers, true).unwrap();
    let out = c.take_output();
    let block = out.len() - handshake;
    assert!(block <= bound, "the bound is an upper bound on the block");
    let f = frames(&out[handshake..]);
    assert_eq!(f[0].0, FrameType::Headers as u8);
    assert_eq!(f.last().unwrap().0, FrameType::Continuation as u8);
}

#[test]
fn output_limit_refused_data_frame_is_not_copied_or_consumed() {
    let mut reference = Connection::client(Config::default());
    reference.open_stream(request_headers(), false).unwrap();
    let headers_out = reference.pending_output().len();

    // Room for the headers and for all but one byte of the DATA frame.
    let limit = headers_out + 9 + 5000 - 1;
    let mut c = limited(Role::Client, limit);
    let id = c.open_stream(request_headers(), false).unwrap();
    assert_eq!(c.pending_output().len(), headers_out);
    let before = c.pending_output().to_vec();

    assert_eq!(
        c.send_data(id, vec![7; 5000], true),
        Err(Error::OutputLimitExceeded {
            limit,
            pending: headers_out,
            attempted: 9 + 5000,
        })
    );
    assert_eq!(c.pending_output(), before, "no partial DATA was written");
    assert_eq!(
        c.queued_send_bytes(id),
        Some(5000),
        "the data was not consumed"
    );
    assert!(c.failure().is_some());
}

#[test]
fn data_serialization_preserves_partial_windows_and_end_stream() {
    let mut c = Connection::client(Config::default());
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let id = c.open_stream(request_headers(), false).unwrap();
    c.take_output();
    c.conn_send_window = 5;
    c.streams.get_mut(&id).unwrap().send_window = 3;

    c.send_data(id, b"abcdef".to_vec(), true).unwrap();
    assert_eq!(
        frames(&c.take_output()),
        vec![(FrameType::Data as u8, 0, id, b"abc".to_vec())]
    );
    assert_eq!(c.conn_send_window, 2);
    assert_eq!(c.streams[&id].send_window, 0);
    assert_eq!(c.queued_send_bytes(id), Some(3));

    c.recv(&wire_frame(
        FrameType::WindowUpdate,
        id,
        0,
        &encode_window_update(3),
    ))
    .unwrap();
    assert_eq!(
        frames(&c.take_output()),
        vec![(FrameType::Data as u8, 0, id, b"de".to_vec())]
    );
    assert_eq!(c.conn_send_window, 0);
    assert_eq!(c.streams[&id].send_window, 1);
    assert_eq!(c.queued_send_bytes(id), Some(1));

    c.recv(&wire_frame(
        FrameType::WindowUpdate,
        0,
        0,
        &encode_window_update(1),
    ))
    .unwrap();
    assert_eq!(
        frames(&c.take_output()),
        vec![(FrameType::Data as u8, Flags::END_STREAM, id, b"f".to_vec())]
    );
    assert_eq!(c.conn_send_window, 0);
    assert_eq!(c.streams[&id].send_window, 0);
    assert_eq!(c.queued_send_bytes(id), Some(0));
    assert_eq!(c.poll_send_ready(), None);
}

#[test]
fn empty_data_serializes_with_zero_windows() {
    for end_stream in [false, true] {
        let mut c = Connection::client(Config::default());
        let id = c.open_stream(request_headers(), false).unwrap();
        c.take_output();
        c.conn_send_window = 0;
        c.streams.get_mut(&id).unwrap().send_window = 0;

        c.send_data(id, Vec::new(), end_stream).unwrap();
        assert_eq!(
            frames(&c.take_output()),
            vec![(
                FrameType::Data as u8,
                if end_stream { Flags::END_STREAM } else { 0 },
                id,
                Vec::new(),
            )]
        );
        assert_eq!(c.conn_send_window, 0);
        assert_eq!(c.streams[&id].send_window, 0);
        assert_eq!(c.queued_send_bytes(id), Some(0));
    }
}

#[test]
fn flushing_completed_streams_does_not_skip_later_streams() {
    let mut c = Connection::client(Config {
        initial_window_size: 0,
        ..Config::default()
    });
    let mut s = Connection::server(Config::default());
    exchange(&mut c, &mut s);
    let ids: Vec<_> = (0..3)
        .map(|_| c.open_stream(request_headers(), true).unwrap())
        .collect();
    exchange(&mut c, &mut s);
    for &id in &ids {
        s.send_headers(id, vec![hf(":status", "200")], false)
            .unwrap();
        s.send_data(id, vec![id as u8], true).unwrap();
        assert_eq!(s.queued_send_bytes(id), Some(1));
    }
    s.take_output();

    s.recv(&wire_frame(
        FrameType::Settings,
        0,
        0,
        &encode_settings(&[Setting {
            id: SettingId::InitialWindowSize,
            value: 1,
        }]),
    ))
    .unwrap();

    let out = frames(&s.take_output());
    assert_eq!(out[0], (FrameType::Settings as u8, Flags::ACK, 0, vec![]));
    let expected: Vec<_> = ids
        .iter()
        .map(|&id| (FrameType::Data as u8, Flags::END_STREAM, id, vec![id as u8]))
        .collect();
    assert_eq!(out[1..], expected);
    assert_eq!(s.stream_count(), 0);
}

/// A client with one open stream whose output is full: the headers are
/// admitted under their bound, and a DATA frame fills the room they leave, so
/// the stream has nothing queued and no more output fits. Returns the stream
/// and the limit.
fn full_stream() -> (Connection, StreamId, usize) {
    let headers = request_headers();
    let handshake = handshake_len(Role::Client, Config::default());
    let mut reference = Connection::client(Config::default());
    reference.open_stream(headers.clone(), false).unwrap();
    let exact = reference.pending_output().len() - handshake;
    let bound = header_block_bound(&headers, HPACK_TABLE_UPDATE_LEN, 16_384);
    let limit = handshake + bound;
    let mut c = limited(Role::Client, limit);
    let id = c.open_stream(headers, false).unwrap();
    // A frame is admitted by its exact size, so this one ends exactly at the limit.
    c.send_data(id, alloc::vec![1; bound - exact - 9], false)
        .unwrap();
    assert_eq!(c.pending_output().len(), limit);
    assert_eq!(c.queued_send_bytes(id), Some(0));
    (c, id, limit)
}

#[test]
fn deferred_reset_reports_new_output_limit() {
    let (mut c, id, limit) = full_stream();
    assert_eq!(
        c.reset_stream_after_flush(id, ErrorCode::NoError),
        Err(Error::OutputLimitExceeded {
            limit,
            pending: limit,
            attempted: 13,
        })
    );
    assert_eq!(
        c.pending_output().len(),
        limit,
        "no partial reset was written"
    );
}

#[test]
fn immediate_reset_reports_new_output_limit() {
    let (mut c, id, limit) = full_stream();
    assert_eq!(
        c.reset_stream(id, ErrorCode::Cancel),
        Err(Error::OutputLimitExceeded {
            limit,
            pending: limit,
            attempted: 13,
        })
    );
    // The stream is forgotten all the same: cleanup is not refused.
    assert!(!c.has_stream(id));
}
