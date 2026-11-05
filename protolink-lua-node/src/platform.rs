//! Native operations: the poll-based interface the node drives, and the bounded-immediate adapter for providers whose
//! work completes at once.
//!
//! A native runs outside the node's scheduling path. The node starts it, then polls it once per scheduler round, so a
//! slow native never blocks control calls, timers or other executions. Cancelling a native asks the provider to stop;
//! a provider that cannot interrupt its work says so, and the node keeps the operation unresolved until it completes or
//! the cleanup deadline passes, which makes the outcome uncertain. The VM's own cancellation never stops hardware.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::task::Poll;

use protolink_lua::value::Value;

/// Handle of one started native operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NativeTicket(pub u64);

/// One native call, owned by the provider while it runs.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeCall {
    /// Module identifier in the contract.
    pub module: u16,
    /// Operation identifier in the module.
    pub operation: u16,
    /// Arguments, already checked against the wire limits.
    pub arguments: Vec<Value>,
    /// Number of results the operation declares: zero or one.
    pub results: u8,
}

/// What a provider says when asked to stop an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeStop {
    /// The operation stopped, and its effect is known. Nothing more happens.
    Stopped,
    /// The operation cannot be interrupted. Keep polling it; it is unresolved until it completes.
    Running,
}

/// Native operations of the contract. The embedding implements them with the hardware it owns.
pub trait Platform {
    /// Starts a native operation. A refusal is raised in Lua at the call, and nothing ran.
    fn start(&mut self, call: NativeCall) -> Result<NativeTicket, String>;

    /// Reports the result once available: one value for an operation that declares one result, `None` for one that
    /// declares none, or an error that is raised in Lua at the call. It must return promptly and never block.
    fn poll(&mut self, ticket: NativeTicket) -> Poll<Result<Option<Value>, String>>;

    /// Asks the provider to stop an operation that has not completed. After `Stopped`, the ticket is never polled again.
    fn cancel(&mut self, ticket: NativeTicket) -> NativeStop;
}

/// A synchronous native function: `(module, operation, arguments) -> result`.
pub type SyncNative = dyn FnMut(u16, u16, &[Value]) -> Result<Option<Value>, String>;

/// Bounded-immediate adapter: runs a synchronous provider at `start` and returns its result at the first `poll`.
///
/// Use it only for a provider whose every operation is bounded and non-blocking, such as a register read or an
/// in-memory computation. Blocking I/O must not be wrapped in it, because `start` runs on the node's scheduling path.
pub struct Immediate {
    run: alloc::boxed::Box<SyncNative>,
    done: BTreeMap<u64, Result<Option<Value>, String>>,
    next: u64,
}

impl Immediate {
    /// Wraps `run`, which must be bounded and non-blocking.
    pub fn new(
        run: impl FnMut(u16, u16, &[Value]) -> Result<Option<Value>, String> + 'static,
    ) -> Self {
        Self {
            run: alloc::boxed::Box::new(run),
            done: BTreeMap::new(),
            next: 1,
        }
    }
}

impl Platform for Immediate {
    fn start(&mut self, call: NativeCall) -> Result<NativeTicket, String> {
        let ticket = self.next;
        self.next += 1;
        let result = (self.run)(call.module, call.operation, &call.arguments);
        self.done.insert(ticket, result);
        Ok(NativeTicket(ticket))
    }

    fn poll(&mut self, ticket: NativeTicket) -> Poll<Result<Option<Value>, String>> {
        match self.done.remove(&ticket.0) {
            Some(result) => Poll::Ready(result),
            None => Poll::Ready(Err(String::from("unknown native ticket"))),
        }
    }

    fn cancel(&mut self, ticket: NativeTicket) -> NativeStop {
        // The work already ran at `start`; its result is simply dropped.
        self.done.remove(&ticket.0);
        NativeStop::Stopped
    }
}

/// A platform with no natives. Every call is refused before anything runs.
pub struct NoNatives;

impl Platform for NoNatives {
    fn start(&mut self, call: NativeCall) -> Result<NativeTicket, String> {
        Err(alloc::format!(
            "module {} operation {} is not provided on this node",
            call.module,
            call.operation
        ))
    }

    fn poll(&mut self, _ticket: NativeTicket) -> Poll<Result<Option<Value>, String>> {
        Poll::Ready(Err(String::from("unknown native ticket")))
    }

    fn cancel(&mut self, _ticket: NativeTicket) -> NativeStop {
        NativeStop::Stopped
    }
}
