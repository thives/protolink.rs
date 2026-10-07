//! Small helpers shared by the sans-IO test modules.
//!
//! Only what several modules need: a bounded client/server exchange, header
//! construction and lookup, and collection of events and statuses. Peers with
//! their own timing or flow-control needs (stalled output, manual flush
//! acknowledgement) drive the connection themselves.

extern crate std;

use alloc::vec::Vec;
use core::task::{Context, Waker};

use super::*;
use protolink_http2::{Connection, Event, HeaderField};

pub(crate) use crate::fields::{field as hf, header};

/// Every event `conn` has queued.
pub(crate) fn events(conn: &mut Connection) -> Vec<Event> {
    core::iter::from_fn(|| conn.poll_event()).collect()
}

/// The `grpc-status` of the last field section that carries one.
pub(crate) fn final_status(events: &[Event]) -> Option<&str> {
    events.iter().rev().find_map(|e| match e {
        Event::Headers { headers, .. } => header(headers, "grpc-status"),
        _ => None,
    })
}

pub(crate) fn poll_server(server: &mut Server, handler: &mut impl Handler) {
    server.poll(handler, &mut Context::from_waker(Waker::noop()));
}

/// Exchange bytes until both sides are idle, splitting every transfer into
/// `chunk`-byte pieces. Returns the bytes that went up (client to server) and
/// down, and panics if the connection does not settle.
pub(crate) fn pump_chunked(
    client: &mut Client,
    server: &mut Server,
    handler: &mut impl Handler,
    chunk: usize,
) -> (usize, usize) {
    let (mut up, mut down) = (0, 0);
    for _ in 0..256 {
        poll_server(server, handler);
        if !client.has_output() && !server.has_output() {
            return (up, down);
        }
        let out = client.take_output();
        up += out.len();
        for piece in out.chunks(chunk) {
            server.recv(piece, handler).unwrap();
        }
        poll_server(server, handler);
        let out = server.take_output();
        down += out.len();
        for piece in out.chunks(chunk) {
            client.recv(piece).unwrap();
        }
    }
    panic!("connection did not settle");
}

pub(crate) fn pump(
    client: &mut Client,
    server: &mut Server,
    handler: &mut impl Handler,
) -> (usize, usize) {
    pump_chunked(client, server, handler, usize::MAX)
}

/// Take every available response; the final status if the call completed.
pub(crate) fn drain(client: &mut Client, id: CallId) -> (Vec<Vec<u8>>, Option<Result<(), Status>>) {
    let mut messages = Vec::new();
    while let Some(next) = client.try_next(id) {
        match next {
            Next::Message(m) => messages.push(m),
            Next::Done(r) => return (messages, Some(r)),
        }
    }
    (messages, None)
}

/// A request from a raw HTTP/2 client, so that requests the real client would
/// never send can be made: send `headers` and `body` (half-closing), and
/// return what the server answered.
pub(crate) fn send_raw(
    server: &mut Server,
    handler: &mut impl Handler,
    headers: Vec<HeaderField>,
    body: &[u8],
) -> Vec<Event> {
    let mut conn = Connection::client(Default::default());
    let id = conn.open_stream(headers, false).unwrap();
    conn.send_data(id, body.to_vec(), true).unwrap();
    for _ in 0..8 {
        server.recv(&conn.take_output(), handler).unwrap();
        conn.recv(&server.take_output()).unwrap();
    }
    events(&mut conn)
}
