//! The child client: how a parent node drives a child execution.
//!
//! The interface is poll-based and needs no async runtime, so the same trait serves the agent, the DUT and host tests.
//! An implementation performs the protocol calls. It never runs Lua itself.
//!
//! Admission is bounded: [`ChildClient::submit`] refuses with [`ChildError::Busy`] when the client has no room, and sends
//! nothing. Every operation carries an absolute deadline on the caller's clock. The parent names each child with a
//! [`ChildId`] before anything is sent, so a spawn whose reply is lost still has a known identity, and the client can
//! reconcile that identity by protocol-safe replay instead of starting a replacement.

use alloc::rc::Rc;
use alloc::vec::Vec;
use core::task::Poll;

use crate::lifecycle::{Disposition, Observation, Phase};
use crate::value::Tuple;

/// Handle of one submitted child operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ticket(pub u64);

/// The parent's name for one child execution. The parent chooses it, unique among its live children, before the spawn
/// is sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChildId(pub u64);

/// One operation a parent asks of a child.
#[derive(Clone, Debug, PartialEq)]
pub enum ChildOp {
    /// Upload if needed, then create a `Ready` child with the snapshot captures and saved arguments.
    Spawn {
        /// The parent's name for the new child.
        child: ChildId,
        /// Stable child-slot id of the edge, which selects the route. A contract digest never selects a route.
        slot: u32,
        /// Verified-by-recipient artifact bytes, shared with the parent's artifact rather than copied.
        artifact: Rc<[u8]>,
        /// Digest of `artifact`, which the child re-checks.
        digest: [u8; 32],
        /// Entry index inside the artifact.
        entry: u32,
        /// Spawn-time capture snapshot.
        captures: Tuple,
        /// Saved initial arguments.
        arguments: Tuple,
        /// Absolute lease in milliseconds of the child's clock, or zero for none.
        lease_ms: u32,
    },
    /// Admit one resume turn of the child.
    Resume {
        /// The child.
        child: ChildId,
        /// Turn number.
        turn: u32,
        /// Reply values for a yield, or empty for the first turn.
        values: Tuple,
    },
    /// Read the retained outcome of one turn.
    Query {
        /// The child.
        child: ChildId,
        /// Turn number.
        turn: u32,
    },
    /// Read an outcome as part of cancellation or cleanup, using cleanup admission.
    CleanupQuery {
        /// The child.
        child: ChildId,
        /// Turn number.
        turn: u32,
    },
    /// Read a live child's status without changing its mutation sequence.
    KeepAlive {
        /// The child.
        child: ChildId,
    },
    /// Request cancellation.
    Cancel {
        /// The child.
        child: ChildId,
    },
    /// Release a terminal child. A released child is forgotten by the client.
    Release {
        /// The child.
        child: ChildId,
    },
}

impl ChildOp {
    /// The child the operation is about.
    pub fn child(&self) -> ChildId {
        match self {
            Self::Spawn { child, .. }
            | Self::Resume { child, .. }
            | Self::Query { child, .. }
            | Self::CleanupQuery { child, .. }
            | Self::KeepAlive { child }
            | Self::Cancel { child }
            | Self::Release { child } => *child,
        }
    }
}

/// The successful reply to a child operation.
#[derive(Clone, Debug, PartialEq)]
pub enum ChildReply {
    /// The child was created in `Ready`.
    Spawned,
    /// The child admitted the resume turn.
    Accepted {
        /// Child phase at the time of the reply.
        phase: Phase,
    },
    /// What the child reported about a turn.
    Observed {
        /// Child phase at the time of the reply.
        phase: Phase,
        /// The retained outcome with its disposition, if the requested turn produced one.
        observation: Option<Observation>,
        /// The child's disposition, whether or not an outcome is available.
        disposition: Disposition,
    },
    /// Cancellation was started. The phase may still be `Cancelling`.
    Cancelling {
        /// Child phase at the time of the reply.
        phase: Phase,
        /// The child's disposition.
        disposition: Disposition,
    },
    /// The child was released.
    Released,
}

/// A failed child operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChildError {
    /// The client has no room for another operation or child. Nothing was sent.
    Busy,
    /// No route to the child's slot, or the link is down before anything was sent. Nothing is known to have happened.
    Unavailable,
    /// The child refused the operation. Nothing ran on the child.
    Rejected {
        /// Bounded diagnostic text from the child.
        message: Vec<u8>,
    },
    /// The operation may or may not have taken effect, and the client could not settle it before its deadline. This is
    /// uncertainty, not a retryable error.
    Unknown,
    /// The client does not know this child.
    UnknownChild,
    /// The parent gave up on this child, and the client keeps it quarantined. Nothing was sent. Only recovery frees its
    /// slot, so the operation is refused rather than attempted.
    Quarantined,
}

/// Poll-based child client.
///
/// After a mutation reports [`ChildError::Unknown`], an implementation must retain its exact identity and refuse a
/// different mutation on that child until it has reconciled the pending one. A successful [`ChildOp::Query`] or
/// [`ChildOp::CleanupQuery`] for that child must not be returned while the mutation remains unresolved: it must first
/// validate the matching receipt or exact replay. Missing or mismatched evidence must return an error instead. This
/// contract lets cleanup use a successful read as permission to send its next mutation without guessing a sequence.
///
/// Quarantine is part of the contract, not an option. Every method that reports or changes quarantine has no default, so
/// a client cannot inherit a silent no-op. A client without a quarantine table does not compile:
///
/// ```compile_fail
/// use protolink_lua::child::{ChildClient, ChildError, ChildId, ChildOp, ChildReply, Ticket};
/// use core::task::Poll;
///
/// struct WithoutQuarantine;
///
/// impl ChildClient for WithoutQuarantine {
///     fn submit(&mut self, _op: ChildOp, _deadline_ms: u64) -> Result<Ticket, ChildError> {
///         Err(ChildError::Busy)
///     }
///     fn poll(&mut self, _ticket: Ticket, _now_ms: u64) -> Poll<Result<ChildReply, ChildError>> {
///         Poll::Pending
///     }
///     fn abandon(&mut self, _ticket: Ticket) {}
///     fn forget(&mut self, _child: ChildId) {}
///     fn liveness(&self, _child: ChildId) -> Option<(u32, u64)> {
///         None
///     }
/// }
/// ```
pub trait ChildClient {
    /// Starts an operation without waiting for it. `deadline_ms` is an absolute time on the caller's clock; the client
    /// stops working on the operation then and reports what it knows. A refused submit sends nothing.
    fn submit(&mut self, op: ChildOp, deadline_ms: u64) -> Result<Ticket, ChildError>;

    /// Makes bounded progress on the operation and reports its result once available. `now_ms` is the caller's clock.
    fn poll(&mut self, ticket: Ticket, now_ms: u64) -> Poll<Result<ChildReply, ChildError>>;

    /// Advertised idle timeout and last successful matching contact in the parent's clock, or `None` when the child
    /// has no live session to watch. A client must answer; `None` means the parent has no heartbeat for the child.
    fn liveness(&self, child: ChildId) -> Option<(u32, u64)>;

    /// Stops tracking a ticket. The operation itself is not cancelled by this call, and its child stays known. A
    /// dispatched mutation's identity remains pending until matching evidence reconciles it, even after abandonment.
    fn abandon(&mut self, ticket: Ticket);

    /// Forgets a child whose outcome the parent no longer needs, freeing its identity. It is not a release: a child
    /// that was not released stays on its node until that node's own cleanup ends it. A quarantined child is not
    /// forgotten: the call changes nothing, and the child leaves only through [`ChildClient::recover_lost`] or
    /// [`ChildClient::recover_slot`].
    fn forget(&mut self, child: ChildId);

    /// The parent has given up on this child: its outcome is uncertain, and no further call will be made. The client
    /// quarantines it: the child keeps its slot, and the slot is **not** released because time passed. A child's idle
    /// timeout is a claim by the child, read on another clock. It does not prove that the child's work stopped: an
    /// uninterruptible native outlives the session that started it, and the child's clock may not have advanced at all.
    /// Operations in progress on the child are dropped, and [`ChildClient::submit`] refuses every later operation on it
    /// with [`ChildError::Quarantined`]. A quarantined child leaves only through [`ChildClient::recover_lost`] or
    /// [`ChildClient::recover_slot`]. The quarantine is bounded by the client's child limit, so it never grows without
    /// bound.
    fn lost(&mut self, child: ChildId, now_ms: u64);

    /// Children the client has quarantined. Their identities and slots are not free.
    fn lost_children(&self) -> usize;

    /// The quarantined children, with the stable slot id of each one's route. At most the client's child limit.
    fn quarantined(&self) -> Vec<(ChildId, u32)>;

    /// Releases the slot of one quarantined child on explicit evidence from the embedding: an operator decision, or a
    /// check outside the node. Returns `false` when `child` is not quarantined. This **does not** establish that the
    /// child's effects stopped, and the client keeps no claim that it did; the caller owns the decision and must keep
    /// the uncertain disposition visible.
    fn recover_lost(&mut self, child: ChildId) -> bool;

    /// Releases every quarantined child on route `slot`, after the embedding reset the child node behind it, so that
    /// nothing the node ran survives. Returns how many were released. The client also forgets what it learned about that
    /// route, so the next spawn describes the child again and takes its new epoch before it uploads. The reset is the
    /// embedding's evidence: the client cannot verify it.
    fn recover_slot(&mut self, slot: u32) -> usize;

    /// Operations in progress, including cleanup reads and cancels. A bounded figure.
    fn operations(&self) -> usize;

    /// Lets simulated children make progress. A real transport does nothing here.
    fn drive(&mut self, _slices: u32) {}
}
