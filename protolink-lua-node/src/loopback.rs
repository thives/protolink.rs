//! A synchronous Protolink link between two nodes in one thread.
//!
//! The client and the server are sans-IO Protolink halves. Bytes written by one are read by the other, and the loop
//! repeats until the response is complete. The link is a stand-in for a transport, so it models framing and routing
//! but not timing, loss or interleaving on a physical bus. Both halves cap messages at the served node's
//! `max_request_bytes`, so an oversized message is refused during reassembly, before it is decoded.

use alloc::format;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::RefCell;

use protolink::grpc::{
    CallContext, Client, ClientConfig, Code, Handler, MethodKind, Route, Server, ServerConfig,
    Status,
};

use crate::link::{ImmediateCall, LinkError};
use crate::node::Node;

/// One direction-pair of Protolink bytes to a node that serves the call.
pub struct Loopback {
    client: Client,
    server: Server,
    node: Rc<RefCell<Node>>,
}

impl Loopback {
    /// Creates a link that serves its calls with `node`.
    pub fn new(node: Rc<RefCell<Node>>) -> Self {
        let cap = node.borrow().config().max_request_bytes;
        Self {
            client: Client::new(ClientConfig {
                max_message_size: cap,
                ..ClientConfig::default()
            }),
            server: Server::new(ServerConfig {
                max_message_size: cap,
                ..ServerConfig::default()
            }),
            node,
        }
    }

    /// The node this link serves.
    pub fn node(&self) -> &Rc<RefCell<Node>> {
        &self.node
    }

    /// Sends one unary call and returns the served response bytes.
    pub fn call(&mut self, method: &str, request: &[u8]) -> Result<Vec<u8>, Status> {
        let id = self.client.start_unary(method, request)?;
        loop {
            let outbound = self.client.take_output();
            let mut handler = ServingNode { node: &self.node };
            if !outbound.is_empty() {
                self.server.recv(&outbound, &mut handler).map_err(|e| {
                    Status::new(Code::Internal, format!("server rejected bytes: {e:?}"))
                })?;
            }
            let inbound = self.server.take_output();
            if !inbound.is_empty() {
                self.client.recv(&inbound).map_err(|e| {
                    Status::new(Code::Internal, format!("client rejected bytes: {e:?}"))
                })?;
            }
            if let Some(result) = self.client.take_response(id) {
                return result.map(|response| response.message);
            }
            if outbound.is_empty() && inbound.is_empty() {
                return Err(Status::new(Code::Internal, "link made no progress"));
            }
        }
    }
}

impl ImmediateCall for Loopback {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        Loopback::call(self, path, request).map_err(|status| match status.code {
            Code::Unavailable => LinkError::Unavailable(status.message),
            code => LinkError::Status {
                code,
                message: status.message,
            },
        })
    }

    fn drive(&mut self, rounds: u32) {
        self.node.borrow_mut().drive(rounds);
    }
}

/// Serves Protolink calls by handing them to a node.
struct ServingNode<'a> {
    node: &'a Rc<RefCell<Node>>,
}

impl Handler for ServingNode<'_> {
    fn route(&self, _path: &str) -> Option<Route> {
        Some(Route::new(MethodKind::Unary, 0))
    }

    fn call(&mut self, ctx: &mut CallContext<'_>, request: &[u8]) -> Result<Vec<u8>, Status> {
        self.node.borrow_mut().handle(ctx.path, request)
    }
}
