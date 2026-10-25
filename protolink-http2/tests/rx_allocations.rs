//! Allocation measurements on the HTTP/2 receive path.
//!
//! The `borrowed_*` and `ignored_*` tests are gates: the pull API
//! ([`Connection::recv_step`]) must not allocate for body bytes, control
//! payloads or skipped payloads, whatever their size. The `baseline_*` tests
//! measure the owned convenience path ([`Connection::recv`]) and report only.
//! Each phase is reported on stderr. Only the measuring thread is counted, and
//! only inside a `Window`.

use protolink_alloc_track::{CountingAllocator, Phase, Window};
use protolink_http2::{Config, Connection, ErrorCode, Event, FlowControl, HeaderField, Step};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const HEADERS: u8 = 0x1;
const PING: u8 = 0x6;
const SETTINGS: u8 = 0x4;
const UNKNOWN: u8 = 0x42;

fn frame(kind: u8, flags: u8, id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    let mut out = vec![(len >> 16) as u8, (len >> 8) as u8, len as u8, kind, flags];
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn field(name: &str, value: &str) -> HeaderField {
    HeaderField {
        name: name.into(),
        value: value.into(),
    }
}

fn request(path: &str, extra: Option<(&str, usize)>) -> Vec<HeaderField> {
    let mut fields = vec![
        field(":method", "POST"),
        field(":scheme", "http"),
        field(":path", path),
        field(":authority", "localhost"),
    ];
    if let Some((name, len)) = extra {
        fields.push(field(name, &"v".repeat(len)));
    }
    fields
}

/// A server that has completed the handshake with a client and has received
/// the request headers of stream `id`. Its output is written out, but its
/// capacity is kept, so that a measured receive does not grow it.
fn open_server(config: Config, extra: Option<(&str, usize)>) -> (Connection, Connection, u32) {
    let mut client = Connection::client(Config::default());
    let mut server = Connection::server(config);
    server.recv(&client.take_output()).unwrap();
    client.recv(&server.take_output()).unwrap();
    let id = client
        .open_stream(request("/m.S/Call", extra), false)
        .unwrap();
    server.recv(&client.take_output()).unwrap();
    drain(&mut server);
    // Grow the output buffer once, then keep its capacity.
    let mut pings = Vec::new();
    for _ in 0..64 {
        pings.extend(frame(PING, 0, 0, b"12345678"));
    }
    server.recv(&pings).unwrap();
    drain(&mut server);
    let len = server.pending_output().len();
    server.consume_output(len);
    (server, client, id)
}

fn drain(conn: &mut Connection) -> usize {
    let mut events = 0;
    while let Some(event) = conn.poll_event() {
        if let Event::Data { .. } = event {
            events += 1;
        }
    }
    events
}

/// Feed `input` through the pull API, dropping each event as the caller would
/// once it has used it. Returns the number of body events.
fn pull_all(conn: &mut Connection, mut input: &[u8]) -> usize {
    let mut events = 0;
    loop {
        let Step { consumed, event } = conn.recv_step(input).unwrap();
        input = &input[consumed..];
        match event {
            Some(Event::Data { .. }) => events += 1,
            Some(_) => {}
            None => return events,
        }
    }
}

fn report(label: &str, phase: Phase) {
    eprintln!(
        "{label}: allocations {}, reallocations {}, peak {} B, live {} B, largest {} B",
        phase.allocations, phase.reallocations, phase.peak, phase.live, phase.largest
    );
}

#[test]
fn borrowed_data_frames_allocate_nothing() {
    for size in [1024usize, 16_384] {
        let (mut server, mut client, id) = open_server(Config::default(), None);
        client.send_data(id, vec![0x5a; size], false).unwrap();
        let wire = client.take_output();

        let window = Window::open();
        let events = pull_all(&mut server, &wire);
        let phase = window.checkpoint();
        report(&format!("borrowed data {size}"), phase);
        assert_eq!(events, 1, "size {size}");
        assert_eq!(phase.allocations, 0, "size {size}");
        assert_eq!(phase.reallocations, 0, "size {size}");
    }
}

#[test]
fn borrowed_bytewise_data_allocates_nothing() {
    let (mut server, mut client, id) = open_server(Config::default(), None);
    client.send_data(id, vec![0x5a; 16_384], false).unwrap();
    let wire = client.take_output();

    let window = Window::open();
    let mut events = 0;
    for byte in wire.chunks(1) {
        events += pull_all(&mut server, byte);
    }
    let phase = window.checkpoint();
    report("borrowed data 16384 bytewise", phase);
    assert!(events >= 1);
    assert_eq!(phase.allocations, 0);
}

#[test]
fn borrowed_batch_of_frames_is_not_copied() {
    let (mut server, mut client, id) = open_server(Config::default(), None);
    client.send_data(id, vec![0x5a; 64 * 1024], false).unwrap();
    let wire = client.take_output();

    let window = Window::open();
    pull_all(&mut server, &wire);
    let phase = window.checkpoint();
    report("borrowed 64 KiB in one slice", phase);
    assert_eq!(phase.allocations, 0);
}

#[test]
fn ignored_payloads_allocate_nothing() {
    let (mut server, _client, _id) = open_server(Config::default(), None);
    let mut wire = frame(UNKNOWN, 0, 0, &vec![0u8; 16_384]);
    wire.extend(frame(SETTINGS, 0, 0, &[0, 4, 0, 1, 0, 0]));
    wire.extend(frame(PING, 0, 0, b"12345678"));
    let window = Window::open();
    pull_all(&mut server, &wire);
    let phase = window.checkpoint();
    report("unknown 16 KiB frame, SETTINGS and PING", phase);
    assert_eq!(phase.allocations, 0);
}

#[test]
fn baseline_single_data_frame_owned() {
    for size in [1024usize, 16_384] {
        let (mut server, mut client, id) = open_server(Config::default(), None);
        client.send_data(id, vec![0x5a; size], false).unwrap();
        let wire = client.take_output();

        let window = Window::open();
        server.recv(&wire).unwrap();
        let received = window.checkpoint();
        let events = drain(&mut server);
        let drained = window.checkpoint();
        report(&format!("owned data {size} recv"), received);
        report(&format!("owned data {size} drain"), drained);
        assert_eq!(events, 1, "size {size}");
    }
}

#[test]
fn baseline_data_frame_bytewise_owned() {
    let (mut server, mut client, id) = open_server(Config::default(), None);
    client.send_data(id, vec![0x5a; 16_384], false).unwrap();
    let wire = client.take_output();

    let window = Window::open();
    for byte in wire.chunks(1) {
        server.recv(byte).unwrap();
        drain(&mut server);
    }
    report("owned data 16384 bytewise recv", window.checkpoint());
}

#[test]
fn baseline_continued_header_block() {
    let config = Config {
        max_header_list_size: 32 * 1024,
        ..Config::default()
    };
    let mut client = Connection::client(Config::default());
    let mut server = Connection::server(config);
    server.recv(&client.take_output()).unwrap();
    client.recv(&server.take_output()).unwrap();
    let id = client
        .open_stream(request("/m.S/Call", Some(("x-large", 20_000))), false)
        .unwrap();
    let wire = client.take_output();
    assert!(
        wire.len() > 16_384 + 9,
        "the block spans CONTINUATION frames"
    );

    let window = Window::open();
    pull_all(&mut server, &wire);
    report("20 KiB header block over CONTINUATION", window.checkpoint());
    assert!(server.has_stream(id));
}

#[test]
fn header_fragment_keeps_the_decoded_field_not_the_encoded_octets() {
    // A literal value of 8000 octets arrives in 1-octet pieces. Its decoded value
    // and name are held, with the decoded field list (one `Vec` of four entries
    // for the first field). An encoded copy of the 8005 octets would add to that.
    let (mut server, _client, _id) = open_server(Config::default(), None);
    let mut block = vec![0x00, 1, b'x'];
    block.extend(zerodds_hpack::encode_integer(8_000, 7, 0));
    block.extend(std::iter::repeat_n(b'a', 8_000));
    let wire = frame(HEADERS, 0, 3, &block);
    let (head, body) = wire.split_at(9);
    let mut events = pull_all(&mut server, head);
    let window = Window::open();
    for byte in body.chunks(1) {
        events += pull_all(&mut server, byte);
    }
    let phase = window.checkpoint();
    report("8000-octet literal, one octet at a time", phase);
    assert_eq!(events, 0);
    assert!(phase.live < 8_001 + 4_000, "live {}", phase.live);
    assert!(phase.allocations <= 4, "allocations {}", phase.allocations);
}

/// Tail processing adds no heap storage, regardless of payload size or cut.
/// Frame admission and reset are outside the window; only output capacity is
/// warmed by [`open_server`]. Inline connection state is not heap usage.
#[test]
fn cancelled_data_tail_allocates_nothing_however_it_is_cut() {
    eprintln!(
        "inline Connection: {} B; separate from measured heap usage",
        std::mem::size_of::<Connection>()
    );
    let manual = Config {
        flow_control: FlowControl::Manual,
        ..Config::default()
    };
    for size in [1_024usize, 16_384] {
        for chunk in [usize::MAX, 512usize, 1usize] {
            let (mut server, mut client, id) = open_server(manual, None);
            client.send_data(id, vec![0x5a; size], false).unwrap();
            let wire = client.take_output();
            assert_eq!(wire.len(), 9 + size, "size {size}");

            // The frame is admitted and its first third delivered before the
            // application gives up on the stream; admission and the reset itself
            // are deliberately outside the window.
            let delivered = 9 + size / 3;
            pull_all(&mut server, &wire[..delivered]);
            server.reset_stream(id, ErrorCode::Cancel).unwrap();
            drain(&mut server);
            let tail = &wire[delivered..];
            assert!(!tail.is_empty(), "size {size}");
            let blocked = server.pending_output().len();
            assert!(blocked > 0, "reset remains queued throughout the tail");

            let phase = {
                let window = Window::open();
                for piece in tail.chunks(chunk) {
                    assert_eq!(pull_all(&mut server, piece), 0, "no discarded DATA events");
                }
                window.checkpoint()
            };
            report(
                &format!("cancelled tail {size}, {chunk}-byte pieces"),
                phase,
            );
            assert_eq!(
                (
                    phase.allocations,
                    phase.reallocations,
                    phase.largest,
                    phase.live,
                    phase.peak
                ),
                (0, 0, 0, 0, 0),
                "no tail allocation or retained/transient heap growth, size {size}, cut {chunk}"
            );
            assert_eq!(
                &server.pending_output()[blocked..],
                frame(0x8, 0, 0, &((size - size / 3) as u32).to_be_bytes()),
                "exactly one connection update for the completed tail"
            );
        }
    }
}
