use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;

use protolink_http2::{Config, Connection, Error, ErrorCode, Event, HeaderField, StreamId};

use crate::status::encode_message;
use crate::{DEFAULT_MAX_MESSAGE_SIZE, Handler, Status, lpm};

/// Server configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerConfig {
    /// HTTP/2 connection limits.
    pub http2: Config,
    /// Largest accepted request and produced response message, in bytes.
    pub max_message_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http2: Config::default(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }
}

#[derive(Debug, Default)]
struct Call {
    path: String,
    body: Vec<u8>,
}

/// Sans-IO unary gRPC server for one connection.
///
/// Feed received bytes and a [`Handler`] to [`recv`](Self::recv); complete
/// requests are dispatched synchronously and their responses queued in
/// [`pending_output`](Self::pending_output).
#[derive(Debug)]
pub struct Server {
    conn: Connection,
    calls: BTreeMap<StreamId, Call>,
    max_message_size: usize,
}

impl Server {
    /// New server connection. Our HTTP/2 SETTINGS are queued immediately.
    pub fn new(config: ServerConfig) -> Self {
        Self {
            conn: Connection::server(config.http2),
            calls: BTreeMap::new(),
            max_message_size: config.max_message_size,
        }
    }

    /// Process received bytes, dispatching complete requests to `handler`.
    ///
    /// On `Err` the connection is unusable; write the pending output (it
    /// contains a GOAWAY) and close the transport.
    pub fn recv<H: Handler + ?Sized>(
        &mut self,
        bytes: &[u8],
        handler: &mut H,
    ) -> Result<(), Error> {
        let result = self.conn.recv(bytes);
        while let Some(event) = self.conn.poll_event() {
            self.on_event(event, handler);
        }
        result
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

    /// Start a graceful shutdown (GOAWAY). In-flight calls still complete.
    pub fn shutdown(&mut self) {
        self.conn.go_away(ErrorCode::NoError);
    }

    /// The connection failed or finished shutting down.
    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    fn on_event<H: Handler + ?Sized>(&mut self, event: Event, handler: &mut H) {
        match event {
            Event::Headers {
                stream_id,
                headers,
                end_stream,
            } => {
                if let alloc::collections::btree_map::Entry::Vacant(slot) =
                    self.calls.entry(stream_id)
                {
                    match validate_request(&headers) {
                        Ok(path) => {
                            slot.insert(Call {
                                path,
                                body: Vec::new(),
                            });
                        }
                        Err(http_status) => {
                            let _ = self.conn.send_headers(
                                stream_id,
                                vec![field(":status", http_status)],
                                true,
                            );
                            let _ = self.conn.reset_stream(stream_id, ErrorCode::NoError);
                            return;
                        }
                    }
                }
                if end_stream {
                    self.dispatch(stream_id, handler);
                }
            }
            Event::Data {
                stream_id,
                data,
                end_stream,
            } => {
                let Some(call) = self.calls.get_mut(&stream_id) else {
                    return;
                };
                call.body.extend_from_slice(&data);
                if call.body.len() > self.max_message_size + lpm::HEADER_LEN {
                    self.calls.remove(&stream_id);
                    self.respond_error(
                        stream_id,
                        Status::resource_exhausted("request message too large"),
                    );
                    // Stop the client from uploading the rest.
                    let _ = self.conn.reset_stream(stream_id, ErrorCode::NoError);
                    return;
                }
                if end_stream {
                    self.dispatch(stream_id, handler);
                }
            }
            Event::Reset { stream_id, .. } => {
                self.calls.remove(&stream_id);
            }
            Event::GoAway { .. } => {}
        }
    }

    fn dispatch<H: Handler + ?Sized>(&mut self, stream_id: StreamId, handler: &mut H) {
        let Some(call) = self.calls.remove(&stream_id) else {
            return;
        };
        let result = lpm::decode_unary(&call.body, self.max_message_size).and_then(|msg| {
            handler.call(&call.path, msg).unwrap_or_else(|| {
                Err(Status::unimplemented(alloc::format!(
                    "unknown method {}",
                    call.path
                )))
            })
        });
        match result {
            Ok(reply) if reply.len() > self.max_message_size => {
                self.respond_error(
                    stream_id,
                    Status::resource_exhausted("response message too large"),
                );
            }
            Ok(reply) => {
                let headers = vec![
                    field(":status", "200"),
                    field("content-type", "application/grpc"),
                ];
                let _ = self.conn.send_headers(stream_id, headers, false);
                let _ = self.conn.send_data(stream_id, lpm::encode(&reply), false);
                let _ = self
                    .conn
                    .send_headers(stream_id, vec![field("grpc-status", "0")], true);
            }
            Err(status) => self.respond_error(stream_id, status),
        }
    }

    /// Trailers-only response carrying `status`.
    fn respond_error(&mut self, stream_id: StreamId, status: Status) {
        let mut headers = vec![
            field(":status", "200"),
            field("content-type", "application/grpc"),
            field("grpc-status", &status.code.as_u8().to_string()),
        ];
        if !status.message.is_empty() {
            headers.push(field("grpc-message", &encode_message(&status.message)));
        }
        let _ = self.conn.send_headers(stream_id, headers, true);
    }
}

/// Validate request headers; returns the `:path` or an HTTP status to reply with.
fn validate_request(headers: &[HeaderField]) -> Result<String, &'static str> {
    let get = |name: &str| {
        headers
            .iter()
            .find(|h| h.name == name)
            .map(|h| h.value.as_str())
    };
    if get(":method") != Some("POST") {
        return Err("405");
    }
    let content_type = get("content-type").unwrap_or("");
    let grpc = content_type == "application/grpc"
        || content_type.starts_with("application/grpc+")
        || content_type.starts_with("application/grpc;");
    if !grpc {
        return Err("415");
    }
    match get(":path") {
        Some(p) if p.starts_with('/') => Ok(p.into()),
        _ => Err("400"),
    }
}

fn field(name: &str, value: &str) -> HeaderField {
    HeaderField {
        name: name.into(),
        value: value.into(),
    }
}
