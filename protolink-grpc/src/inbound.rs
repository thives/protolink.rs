//! Received message buffering with flow-control crediting, shared by the
//! client and server.

use alloc::vec::Vec;

use protolink_http2::{Connection, StreamId};

use crate::Status;
use crate::lpm::{self, Decoder};

/// Decodes the messages of one call and credits received bytes back to the
/// peer (with [`FlowControl::Manual`](protolink_http2::FlowControl::Manual))
/// only once they are no longer held for the application.
///
/// Credit is withheld exactly for complete messages that have not been taken
/// yet. Bytes of a partial message are credited back while no complete message
/// is waiting, so a message larger than the stream window can still arrive.
/// Buffering per call is therefore bounded by about
/// `initial_window_size + max_message_size`, whatever the peer does.
#[derive(Debug)]
pub(crate) struct Inbound {
    decoder: Decoder,
    /// Bytes at the front of the buffer already credited back to the peer.
    credited: usize,
    /// Received bytes not credited back yet.
    held: usize,
}

impl Inbound {
    pub(crate) fn new(max_message_size: usize) -> Self {
        Self {
            decoder: Decoder::new(max_message_size),
            credited: 0,
            held: 0,
        }
    }

    /// Buffer a DATA payload.
    pub(crate) fn push(&mut self, conn: &mut Connection, id: StreamId, data: &[u8]) {
        self.held += data.len();
        self.decoder.push(data);
        self.release_partial(conn, id);
    }

    /// Next complete message (or the decoding error), crediting its bytes back.
    pub(crate) fn next(
        &mut self,
        conn: &mut Connection,
        id: StreamId,
    ) -> Option<Result<Vec<u8>, Status>> {
        let item = self.decoder.next()?;
        if let Ok(msg) = &item {
            let n = msg.len() + lpm::HEADER_LEN;
            let already = n.min(self.credited);
            self.credited -= already;
            let release = (n - already).min(self.held);
            self.held -= release;
            conn.release_capacity(id, release);
        }
        self.release_partial(conn, id);
        Some(item)
    }

    /// While no complete message is waiting, everything buffered is (part of)
    /// a single partial message: credit it back so the peer can finish it.
    fn release_partial(&mut self, conn: &mut Connection, id: StreamId) {
        if self.decoder.message_count() == 0 && self.held > 0 {
            conn.release_capacity(id, self.held);
            self.held = 0;
            self.credited = self.decoder.buffered();
        }
    }

    pub(crate) fn has_next(&self) -> bool {
        self.decoder.has_next()
    }

    pub(crate) fn message_count(&self) -> usize {
        self.decoder.message_count()
    }

    pub(crate) fn error(&self) -> Option<&Status> {
        self.decoder.error()
    }

    pub(crate) fn finish(&self) -> Result<(), Status> {
        self.decoder.finish()
    }

    pub(crate) fn buffered(&self) -> usize {
        self.decoder.buffered()
    }
}
