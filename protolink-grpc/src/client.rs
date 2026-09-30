use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use protolink_http2::{Config, Connection, Error, ErrorCode, Event, HeaderField, StreamId};

use crate::status::decode_message;
use crate::{Code, DEFAULT_MAX_MESSAGE_SIZE, Status, lpm};

/// Identifies an in-flight call on a [`Client`].
pub type CallId = StreamId;

/// Client configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// HTTP/2 connection limits.
    pub http2: Config,
    /// Largest accepted response and sent request message, in bytes.
    pub max_message_size: usize,
    /// `:authority` sent with every request.
    pub authority: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            http2: Config::default(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            authority: "localhost".into(),
        }
    }
}

#[derive(Debug, Default)]
struct Pending {
    headers_received: bool,
    body: Vec<u8>,
}

/// Sans-IO unary gRPC client for one connection.
///
/// Start calls with [`start_unary`](Self::start_unary), write
/// [`pending_output`](Self::pending_output), feed received bytes to
/// [`recv`](Self::recv) and collect results with
/// [`take_response`](Self::take_response).
#[derive(Debug)]
pub struct Client {
    conn: Connection,
    pending: BTreeMap<CallId, Pending>,
    done: BTreeMap<CallId, Result<Vec<u8>, Status>>,
    max_message_size: usize,
    authority: String,
}

impl Client {
    /// New client connection. The HTTP/2 preface is queued immediately.
    pub fn new(config: ClientConfig) -> Self {
        Self {
            conn: Connection::client(config.http2),
            pending: BTreeMap::new(),
            done: BTreeMap::new(),
            max_message_size: config.max_message_size,
            authority: config.authority,
        }
    }

    /// Queue a unary call of `path` with the encoded `request` message.
    pub fn start_unary(&mut self, path: &str, request: &[u8]) -> Result<CallId, Status> {
        if request.len() > self.max_message_size {
            return Err(Status::resource_exhausted("request message too large"));
        }
        let headers = vec![
            field(":method", "POST"),
            field(":scheme", "http"),
            field(":path", path),
            field(":authority", &self.authority),
            field("content-type", "application/grpc"),
            field("te", "trailers"),
            field("user-agent", "protolink"),
        ];
        let id = self.conn.open_stream(headers, false).map_err(|e| match e {
            Error::GoingAway => Status::unavailable("connection is going away"),
            _ => Status::unavailable("connection failed"),
        })?;
        self.conn
            .send_data(id, lpm::encode(request), true)
            .map_err(|_| Status::internal("failed to queue request"))?;
        self.pending.insert(id, Pending::default());
        Ok(id)
    }

    /// Process received bytes.
    ///
    /// On `Err` the connection is unusable and every in-flight call completes
    /// with `UNAVAILABLE`; write the pending output and close the transport.
    pub fn recv(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let result = self.conn.recv(bytes);
        while let Some(event) = self.conn.poll_event() {
            self.on_event(event);
        }
        if result.is_err() {
            self.fail_all(Status::unavailable("connection error"));
        }
        result
    }

    /// Result of a finished call, removing it from the client.
    pub fn take_response(&mut self, id: CallId) -> Option<Result<Vec<u8>, Status>> {
        self.done.remove(&id)
    }

    /// Whether the call is still waiting for its response.
    pub fn is_pending(&self, id: CallId) -> bool {
        self.pending.contains_key(&id)
    }

    /// Abort an in-flight call (RST_STREAM CANCEL).
    pub fn cancel(&mut self, id: CallId) {
        if self.pending.remove(&id).is_some() {
            let _ = self.conn.reset_stream(id, ErrorCode::Cancel);
        }
        self.done.remove(&id);
    }

    /// Mark every in-flight call as failed with `status`, e.g. when the
    /// transport closed.
    pub fn fail_all(&mut self, status: Status) {
        for (id, _) in core::mem::take(&mut self.pending) {
            self.done.insert(id, Err(status.clone()));
        }
    }

    /// Bytes that must be written to the transport.
    pub fn pending_output(&self) -> &[u8] {
        self.conn.pending_output()
    }

    /// Mark `n` bytes of [`pending_output`](Self::pending_output) as written.
    pub fn consume_output(&mut self, n: usize) {
        self.conn.consume_output(n);
    }

    /// Take all pending output.
    pub fn take_output(&mut self) -> Vec<u8> {
        self.conn.take_output()
    }

    /// Whether output is waiting to be written.
    pub fn has_output(&self) -> bool {
        self.conn.has_output()
    }

    /// The connection failed or the server is shutting it down.
    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    fn finish(&mut self, id: CallId, result: Result<Vec<u8>, Status>) {
        if self.pending.remove(&id).is_some() {
            self.done.insert(id, result);
        }
    }

    fn on_event(&mut self, event: Event) {
        match event {
            Event::Headers {
                stream_id,
                headers,
                end_stream,
            } => {
                let Some(call) = self.pending.get_mut(&stream_id) else {
                    return;
                };
                let first = !call.headers_received;
                call.headers_received = true;
                if first {
                    let http = header(&headers, ":status").and_then(|s| s.parse::<u16>().ok());
                    if http != Some(200) {
                        let code = http.map_or(Code::Internal, Code::from_http_status);
                        let msg = alloc::format!("unexpected HTTP status {}", http.unwrap_or(0));
                        self.finish(stream_id, Err(Status::new(code, msg)));
                        let _ = self.conn.reset_stream(stream_id, ErrorCode::Cancel);
                        return;
                    }
                    if !end_stream {
                        return;
                    }
                }
                if !end_stream {
                    self.finish(stream_id, Err(Status::internal("unexpected header block")));
                    let _ = self.conn.reset_stream(stream_id, ErrorCode::ProtocolError);
                    return;
                }
                let status = trailers_status(&headers);
                let body = self
                    .pending
                    .get(&stream_id)
                    .map(|c| c.body.as_slice())
                    .unwrap_or(&[]);
                let result = match status {
                    Ok(()) => lpm::decode_unary(body, self.max_message_size).map(<[u8]>::to_vec),
                    Err(s) => Err(s),
                };
                self.finish(stream_id, result);
            }
            Event::Data {
                stream_id,
                data,
                end_stream,
            } => {
                let Some(call) = self.pending.get_mut(&stream_id) else {
                    return;
                };
                call.body.extend_from_slice(&data);
                if call.body.len() > self.max_message_size + lpm::HEADER_LEN {
                    self.finish(
                        stream_id,
                        Err(Status::resource_exhausted("response message too large")),
                    );
                    let _ = self.conn.reset_stream(stream_id, ErrorCode::Cancel);
                } else if end_stream {
                    self.finish(
                        stream_id,
                        Err(Status::internal("response ended without trailers")),
                    );
                }
            }
            Event::Reset {
                stream_id,
                error_code,
            } => {
                self.finish(
                    stream_id,
                    Err(Status::new(Code::from_h2(error_code), "stream reset")),
                );
            }
            Event::GoAway { .. } => {}
        }
    }
}

fn trailers_status(headers: &[HeaderField]) -> Result<(), Status> {
    let Some(code) = header(headers, "grpc-status") else {
        return Err(Status::internal("missing grpc-status"));
    };
    let code = code.parse::<u8>().map_or(Code::Unknown, Code::from_u8);
    if code == Code::Ok {
        return Ok(());
    }
    let message = header(headers, "grpc-message")
        .map(decode_message)
        .unwrap_or_default();
    Err(Status::new(code, message))
}

fn header<'a>(headers: &'a [HeaderField], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| h.name == name)
        .map(|h| h.value.as_str())
}

fn field(name: &str, value: &str) -> HeaderField {
    HeaderField {
        name: name.into(),
        value: value.into(),
    }
}
