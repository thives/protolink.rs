//! The downstream transport of a parent node: one link per child slot, carrying unary calls one at a time.
//!
//! A link is pollable, so a slow or silent child never blocks the parent's scheduler. The parent bounds every call with
//! its own clock and aborts a call that misses its deadline. An aborted or unanswered call is `Lost`: it may have been
//! delivered, so its effect is unknown.

use alloc::string::String;
use alloc::vec::Vec;
use core::task::Poll;

use protolink::grpc::Code;

/// Why a link could not complete a call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkError {
    /// The link already carries a call. Nothing was sent.
    Busy,
    /// The link is down. Nothing was sent.
    Unavailable(String),
    /// The child answered with a status. The call was refused as a whole.
    Status {
        /// The child's status code.
        code: Code,
        /// The child's message.
        message: String,
    },
    /// The call may have been delivered, and no answer came back.
    Lost(String),
}

/// One link to a child node.
pub trait ChildLink {
    /// Sends one unary call to the full method path. At most one call is in flight; a second send is refused with
    /// [`LinkError::Busy`].
    fn send(&mut self, path: &str, request: &[u8]) -> Result<(), LinkError>;

    /// Polls the call in flight. It must return promptly and never block.
    fn poll(&mut self) -> Poll<Result<Vec<u8>, LinkError>>;

    /// Gives up the call in flight. Its effect is unknown to the caller.
    fn abort(&mut self);

    /// Lets a simulated child make progress. A real transport does nothing here.
    fn drive(&mut self, _rounds: u32) {}
}

/// A transport that completes a call in one bounded step, such as an in-process simulation.
pub trait ImmediateCall {
    /// Performs one unary call to completion.
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError>;

    /// Lets a simulated child make progress.
    fn drive(&mut self, _rounds: u32) {}
}

/// Bounded-immediate adapter: performs the call at `send` and returns its result at the first `poll`. Use it only for
/// a transport whose call is bounded and never blocks, such as the in-process [`Loopback`](crate::Loopback).
pub struct Immediate<T> {
    inner: T,
    result: Option<Result<Vec<u8>, LinkError>>,
}

impl<T> Immediate<T> {
    /// Wraps `inner`.
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            result: None,
        }
    }

    /// The wrapped transport.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// The wrapped transport, mutably.
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: ImmediateCall> ChildLink for Immediate<T> {
    fn send(&mut self, path: &str, request: &[u8]) -> Result<(), LinkError> {
        if self.result.is_some() {
            return Err(LinkError::Busy);
        }
        self.result = Some(self.inner.call(path, request));
        Ok(())
    }

    fn poll(&mut self) -> Poll<Result<Vec<u8>, LinkError>> {
        match self.result.take() {
            Some(result) => Poll::Ready(result),
            None => Poll::Ready(Err(LinkError::Unavailable(String::from(
                "no call is in flight",
            )))),
        }
    }

    fn abort(&mut self) {
        self.result = None;
    }

    fn drive(&mut self, rounds: u32) {
        self.inner.drive(rounds);
    }
}
