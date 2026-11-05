//! [`LinkChildren`]: the child client of a parent node, speaking `lua.execution.v1` over one [`ChildLink`] per slot.
//!
//! Every operation is a small state machine that makes at most one link exchange per poll. Each slot's link carries one
//! call at a time, so operations on one slot take turns. Every operation has a deadline on the parent's clock.
//!
//! A call whose answer is lost is retransmitted with identical bytes, under the same session and sequence, so the
//! child's ledger answers it by replay and never runs it twice. A spawn draws its session identity before anything is
//! sent; if its `Start` stays lost, the child is reported unknown rather than started again under a new identity.
//! Uploads change nothing but the child's artifact store, so a failed upload leaves nothing that needs cleanup.
//!
//! A child the parent gave up on is *quarantined*: it keeps its slot, and neither clock frees it. Its idle timeout is the child's
//! own claim, and an uninterruptible native outlives the session that started it. Only [`ChildClient::recover_lost`] and
//! [`ChildClient::recover_slot`], called on the embedding's evidence, release a quarantined child. The quarantine is part of
//! the child table, so it holds at most `max_children` entries.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::task::Poll;

use micropb::MessageDecode;
use protolink::grpc::Code;
use protolink_lua::PROTOCOL_REVISION;
use protolink_lua::child::{ChildClient, ChildError, ChildId, ChildOp, ChildReply, Ticket};
use protolink_lua::codec;
use protolink_lua::flow::{Flow, Stamp};
use protolink_lua::frame::{self, Shape};
use protolink_lua::ledger::{RESERVED_SESSION_ID, Receipt, mutation_fingerprint};
use protolink_lua::lifecycle::{
    Disposition, Failure, FailureCode, Observation, Outcome, Phase, bounded_diagnostic,
};
use protolink_lua::value::{Limits as WireLimits, Tuple};
use protolink_lua::wire::v1;

use crate::link::{ChildLink, LinkError};

/// gRPC path prefix of the execution service.
const PREFIX: &str = "/lua.execution.v1.Execution/";

/// Bounds of a [`LinkChildren`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChildSettings {
    /// Tuple limits applied to values a child returns.
    pub wire: WireLimits,
    /// Largest reply accepted from a child, checked before it is decoded.
    pub max_reply_bytes: usize,
    /// Most children known at once.
    pub max_children: usize,
    /// Most operations in progress at once.
    pub max_ops: usize,
    /// Identical retransmissions of one call whose answer was lost.
    pub retransmits: u32,
}

/// What a child node said about itself.
#[derive(Clone, Copy, Debug)]
struct Described {
    chunk: usize,
    epoch: u64,
    idle_ms: u32,
}

struct Route {
    slot: u32,
    link: Box<dyn ChildLink>,
    described: Option<Described>,
    next_session: u64,
    /// The operation whose call the link carries.
    owner: Option<u64>,
}

struct Child {
    route: usize,
    /// Set when the parent gave up on the child. The child is quarantined: its slot is kept until the embedding recovers
    /// it. Time does not release it, because the child's idle timeout is the child's own claim and proves nothing about
    /// work that outlives its session.
    quarantined: bool,
    /// The execution's session, drawn before its `Start` is first sent.
    flow: Option<Flow>,
    execution: Option<u64>,
    /// Exact last ambiguous request, retained until a matching receipt settles it.
    pending: Option<(&'static str, Vec<u8>)>,
    idle_ms: u32,
    last_contact: u64,
}

#[derive(Clone, Copy, Debug)]
enum Stage {
    Describe,
    Begin {
        flow: Flow,
    },
    Chunk {
        flow: Flow,
        upload: u64,
        offset: usize,
    },
    Commit {
        flow: Flow,
        upload: u64,
    },
    Start,
    Resume,
    Query,
    KeepAlive,
    Cancel,
    Release,
}

impl Stage {
    /// True for a call whose effect matters beyond the artifact store, so an unanswered one leaves the child unknown.
    fn effectful(self) -> bool {
        matches!(
            self,
            Self::Start | Self::Resume | Self::Cancel | Self::Release
        )
    }

    /// True for any call that changes the child. The upload calls count, because the child keeps the bytes they stage.
    fn mutates(self) -> bool {
        self.effectful()
            || matches!(
                self,
                Self::Begin { .. } | Self::Chunk { .. } | Self::Commit { .. }
            )
    }
}

struct Op {
    op: ChildOp,
    child: ChildId,
    route: usize,
    deadline: u64,
    stage: Stage,
    /// An effectful call of this operation may have reached the child.
    dispatched: bool,
    /// A mutation was sent and its reply has not been accepted, so the child may have taken it.
    in_doubt: bool,
    /// The call being sent, kept so a lost answer is retransmitted with identical bytes.
    request: Option<(&'static str, Vec<u8>)>,
    in_flight: bool,
    retransmitted: u32,
    reuploaded: bool,
    done: Option<Result<ChildReply, ChildError>>,
}

type Step = Option<Result<ChildReply, ChildError>>;

/// The child client of a parent node.
pub struct LinkChildren {
    settings: ChildSettings,
    routes: Vec<Route>,
    children: BTreeMap<ChildId, Child>,
    ops: BTreeMap<u64, Op>,
    next_ticket: u64,
}

impl LinkChildren {
    /// Creates a client over one link per slot id.
    pub fn new(settings: ChildSettings, links: Vec<(u32, Box<dyn ChildLink>)>) -> Self {
        Self {
            settings,
            routes: links
                .into_iter()
                .map(|(slot, link)| Route {
                    slot,
                    link,
                    described: None,
                    next_session: 1,
                    owner: None,
                })
                .collect(),
            children: BTreeMap::new(),
            ops: BTreeMap::new(),
            next_ticket: 1,
        }
    }

    /// Number of children known.
    pub fn known_children(&self) -> usize {
        self.children.len()
    }

    /// Children the parent gave up on that still hold a slot. They leave only through [`ChildClient::recover_lost`] or
    /// [`ChildClient::recover_slot`], or when the client is dropped with the node.
    pub fn lost_children(&self) -> usize {
        self.children
            .values()
            .filter(|child| child.quarantined)
            .count()
    }

    /// Number of operations in progress.
    pub fn operations(&self) -> usize {
        self.ops.len()
    }

    /// The error of an operation that could not complete: unknown when an effectful call may have reached the child.
    fn failed(op: &Op, cause: ChildError) -> ChildError {
        if op.dispatched || op.in_doubt {
            ChildError::Unknown
        } else {
            cause
        }
    }

    /// Runs one step of operation `ticket`: at most one call is sent or answered.
    fn step(&mut self, ticket: u64, now: u64) -> Step {
        let op = self.ops.get(&ticket)?;
        if now >= op.deadline {
            return Some(Err(Self::failed(op, ChildError::Unavailable)));
        }
        if self.routes[op.route]
            .owner
            .is_some_and(|owner| owner != ticket)
        {
            return None;
        }
        if let Some(finished) = self.prepare(ticket) {
            return Some(finished);
        }
        let op = self.ops.get_mut(&ticket)?;
        let route = &mut self.routes[op.route];
        if !op.in_flight {
            let (method, bytes) = op.request.as_ref()?;
            let path = format!("{PREFIX}{method}");
            match route.link.send(&path, bytes) {
                Ok(()) => {
                    op.in_flight = true;
                    route.owner = Some(ticket);
                    op.in_doubt |= op.stage.mutates();
                    if op.stage.effectful() {
                        op.dispatched = true;
                    }
                }
                Err(LinkError::Busy) => return None,
                Err(LinkError::Lost(_)) => {
                    op.in_doubt |= op.stage.mutates();
                    if op.stage.effectful() {
                        op.dispatched = true;
                    }
                    return self.lost(ticket);
                }
                Err(_) => {
                    route.described = None;
                    return Some(Err(Self::failed(op, ChildError::Unavailable)));
                }
            }
        }
        let polled = route.link.poll();
        let Poll::Ready(result) = polled else {
            return None;
        };
        op.in_flight = false;
        route.owner = None;
        match result {
            Ok(bytes) => {
                let child = op.child;
                let result = self.answered(ticket, &bytes);
                if matches!(&result, Some(Ok(_)))
                    && let Some(child) = self.children.get_mut(&child)
                {
                    child.last_contact = now;
                }
                result
            }
            Err(LinkError::Lost(_)) => self.lost(ticket),
            Err(LinkError::Status { code, message }) => self.refused(ticket, code, message),
            Err(LinkError::Unavailable(_) | LinkError::Busy) => {
                route.described = None;
                Some(Err(Self::failed(op, ChildError::Unavailable)))
            }
        }
    }

    /// Builds the next call of operation `ticket` unless one is already built for retransmission.
    fn prepare(&mut self, ticket: u64) -> Step {
        let op = self.ops.get_mut(&ticket)?;
        if op.request.is_some() {
            return None;
        }
        let route = &mut self.routes[op.route];
        let request = match op.stage {
            Stage::Describe => ("Describe", codec::encode(&v1::DescribeRequest::default())),
            Stage::Begin { flow } => {
                let ChildOp::Spawn {
                    artifact, digest, ..
                } = &op.op
                else {
                    return Some(Err(ChildError::Unavailable));
                };
                let mut begin = v1::BeginUploadRequest {
                    r#digest: digest.to_vec(),
                    r#total_length: artifact.len() as u32,
                    ..v1::BeginUploadRequest::default()
                };
                begin.set_context(context(flow.pending()));
                ("BeginUpload", codec::encode(&begin))
            }
            Stage::Chunk {
                flow,
                upload,
                offset,
            } => {
                let ChildOp::Spawn { artifact, .. } = &op.op else {
                    return Some(Err(ChildError::Unavailable));
                };
                let chunk = route.described.map_or(1, |d| d.chunk);
                let end = (offset + chunk).min(artifact.len());
                let mut put = v1::PutChunkRequest {
                    r#upload_id: upload,
                    r#offset: offset as u32,
                    r#data: artifact[offset..end].to_vec(),
                    ..v1::PutChunkRequest::default()
                };
                put.set_context(context(flow.pending()));
                ("PutChunk", codec::encode(&put))
            }
            Stage::Commit { flow, upload } => {
                let mut commit = v1::CommitArtifactRequest {
                    r#upload_id: upload,
                    ..v1::CommitArtifactRequest::default()
                };
                commit.set_context(context(flow.pending()));
                ("CommitArtifact", codec::encode(&commit))
            }
            Stage::Start => {
                let ChildOp::Spawn {
                    digest,
                    entry,
                    captures,
                    arguments,
                    lease_ms,
                    ..
                } = &op.op
                else {
                    return Some(Err(ChildError::Unavailable));
                };
                let Some(child) = self.children.get_mut(&op.child) else {
                    return Some(Err(ChildError::UnknownChild));
                };
                let flow = match child.flow {
                    Some(flow) => flow,
                    None => match draw(route) {
                        Some(flow) => {
                            child.flow = Some(flow);
                            flow
                        }
                        // Without a description the route cannot issue a session. That happens after a slot recovery
                        // invalidated it, and nothing has been sent yet.
                        None if route.described.is_none() => {
                            return Some(Err(ChildError::Unavailable));
                        }
                        None => return Some(Err(no_session())),
                    },
                };
                let mut start = v1::StartRequest {
                    r#digest: digest.to_vec(),
                    r#entry: *entry,
                    r#lease_ms: *lease_ms,
                    ..v1::StartRequest::default()
                };
                start.set_context(context(flow.pending()));
                start.set_captures(captures.to_wire());
                start.set_arguments(arguments.to_wire());
                ("Start", codec::encode(&start))
            }
            Stage::Resume | Stage::Query | Stage::KeepAlive | Stage::Cancel | Stage::Release => {
                let Some(child) = self.children.get(&op.child) else {
                    return Some(Err(ChildError::UnknownChild));
                };
                let (Some(flow), Some(execution)) = (child.flow, child.execution) else {
                    return Some(Err(ChildError::UnknownChild));
                };
                match &op.op {
                    ChildOp::Resume { turn, values, .. } => {
                        let mut resume = v1::ResumeRequest {
                            r#execution_id: execution,
                            r#turn: *turn,
                            ..v1::ResumeRequest::default()
                        };
                        resume.set_context(context(flow.pending()));
                        resume.set_values(values.to_wire());
                        ("Resume", codec::encode(&resume))
                    }
                    ChildOp::Query { turn, .. } | ChildOp::CleanupQuery { turn, .. } => {
                        let mut query = v1::QueryOutcomeRequest {
                            r#execution_id: execution,
                            r#turn: *turn,
                            ..v1::QueryOutcomeRequest::default()
                        };
                        query.set_context(context(flow.reading()));
                        ("QueryOutcome", codec::encode(&query))
                    }
                    ChildOp::KeepAlive { .. } => {
                        let mut status = v1::StatusRequest {
                            r#execution_id: execution,
                            ..v1::StatusRequest::default()
                        };
                        status.set_context(context(flow.reading()));
                        ("Status", codec::encode(&status))
                    }
                    ChildOp::Cancel { .. } => {
                        let mut cancel = v1::CancelRequest {
                            r#execution_id: execution,
                            ..v1::CancelRequest::default()
                        };
                        cancel.set_context(context(flow.pending()));
                        ("Cancel", codec::encode(&cancel))
                    }
                    ChildOp::Release { .. } => {
                        let mut release = v1::ReleaseRequest {
                            r#execution_id: execution,
                            ..v1::ReleaseRequest::default()
                        };
                        release.set_context(context(flow.pending()));
                        ("Release", codec::encode(&release))
                    }
                    ChildOp::Spawn { .. } => return Some(Err(ChildError::Unavailable)),
                }
            }
        };
        op.request = Some(request);
        None
    }

    /// A call's answer was lost. Retransmit it unchanged, or give up when the budget is spent.
    fn lost(&mut self, ticket: u64) -> Step {
        let op = self.ops.get_mut(&ticket)?;
        if op.retransmitted < self.settings.retransmits {
            op.retransmitted += 1;
            return None;
        }
        Some(Err(Self::failed(op, ChildError::Unavailable)))
    }

    /// Moves to the next stage of an operation after an answered call.
    fn next_stage(&mut self, ticket: u64, stage: Stage) -> Step {
        let op = self.ops.get_mut(&ticket)?;
        op.stage = stage;
        op.request = None;
        op.in_doubt = false;
        op.retransmitted = 0;
        None
    }

    /// Applies an answer to operation `ticket`.
    fn answered(&mut self, ticket: u64, bytes: &[u8]) -> Step {
        let settings = self.settings;
        let op = self.ops.get(&ticket)?;
        let shape = if matches!(op.stage, Stage::Query) {
            Shape::QueryReply
        } else {
            Shape::Plain
        };
        if frame::check(
            shape,
            bytes,
            settings.max_reply_bytes,
            settings.wire.max_items(),
        )
        .is_err()
        {
            // An answer that cannot be checked is treated as lost: the call may have taken effect.
            return self.lost(ticket);
        }
        match op.stage {
            Stage::Describe => {
                let Ok(described) = decode::<v1::DescribeResponse>(bytes) else {
                    return self.lost(ticket);
                };
                self.described(ticket, &described)
            }
            Stage::Begin { mut flow } => {
                let Ok(opened) = decode::<v1::BeginUploadResponse>(bytes) else {
                    return self.lost(ticket);
                };
                if opened.context() != Some(&context(flow.pending())) || opened.r#upload_id == 0 {
                    return self.lost(ticket);
                }
                flow.advance();
                self.next_stage(
                    ticket,
                    Stage::Chunk {
                        flow,
                        upload: opened.r#upload_id,
                        offset: 0,
                    },
                )
            }
            Stage::Chunk {
                mut flow,
                upload,
                offset,
            } => {
                let Ok(put) = decode::<v1::PutChunkResponse>(bytes) else {
                    return self.lost(ticket);
                };
                let ChildOp::Spawn { artifact, .. } = &op.op else {
                    return Some(Err(ChildError::Unavailable));
                };
                let chunk = self.routes[op.route].described.map_or(1, |d| d.chunk);
                let end = (offset + chunk).min(artifact.len());
                if put.context() != Some(&context(flow.pending()))
                    || usize::try_from(put.r#received).ok() != Some(end)
                {
                    return self.lost(ticket);
                }
                flow.advance();
                let stage = if end >= artifact.len() {
                    Stage::Commit { flow, upload }
                } else {
                    Stage::Chunk {
                        flow,
                        upload,
                        offset: end,
                    }
                };
                self.next_stage(ticket, stage)
            }
            Stage::Commit { flow, .. } => {
                let Ok(committed) = decode::<v1::CommitArtifactResponse>(bytes) else {
                    return self.lost(ticket);
                };
                let ChildOp::Spawn { digest, .. } = &op.op else {
                    return Some(Err(ChildError::Unavailable));
                };
                if committed.context() != Some(&context(flow.pending()))
                    || committed.r#digest.as_slice() != digest.as_slice()
                {
                    return self.lost(ticket);
                }
                self.next_stage(ticket, Stage::Start)
            }
            Stage::Start => {
                let Ok(started) = decode::<v1::StartResponse>(bytes) else {
                    return self.lost(ticket);
                };
                let child = self.children.get_mut(&op.child)?;
                let expected = context(child.flow?.pending());
                if started.context() != Some(&expected) || started.r#execution_id == 0 {
                    return self.lost(ticket);
                }
                child.idle_ms = self.routes[op.route].described?.idle_ms;
                child.execution = Some(started.r#execution_id);
                if let Some(flow) = child.flow.as_mut() {
                    flow.advance();
                }
                Some(Ok(ChildReply::Spawned))
            }
            Stage::Resume => {
                let Ok(response) = decode::<v1::ResumeResponse>(bytes) else {
                    return self.lost(ticket);
                };
                let expected = context(self.children.get(&op.child)?.flow?.pending());
                if !response.r#accepted || response.context() != Some(&expected) {
                    return self.lost(ticket);
                }
                self.advance_flow(op.child);
                Some(Ok(ChildReply::Accepted {
                    phase: phase(response.r#state),
                }))
            }
            Stage::Query => {
                let Ok(response) = decode::<v1::QueryOutcomeResponse>(bytes) else {
                    return self.lost(ticket);
                };
                let child = self.children.get_mut(&op.child)?;
                let flow = child.flow.as_mut()?;
                if response.context() != Some(&context(flow.reading())) {
                    return self.lost(ticket);
                }
                if let Some((method, bytes)) = child.pending.as_ref() {
                    let fingerprint = mutation_fingerprint(method, bytes);
                    let receipt = response.last_mutation().and_then(Receipt::from_wire);
                    if !flow.reconcile(
                        flow.reading(),
                        flow.pending(),
                        &fingerprint,
                        receipt.as_ref(),
                    ) {
                        return Some(Err(ChildError::Unknown));
                    }
                    child.pending = None;
                }
                let mut disposition = Disposition::from_flag(response.r#uncertain);
                let observation = if response.r#available {
                    let Some(observation) = response
                        .outcome()
                        .and_then(|outcome| observation_from_wire(outcome, settings.wire))
                    else {
                        return self.lost(ticket);
                    };
                    disposition.absorb(observation.disposition);
                    Some(observation)
                } else {
                    None
                };
                Some(Ok(ChildReply::Observed {
                    phase: phase(response.r#state),
                    observation,
                    disposition,
                }))
            }
            Stage::KeepAlive => {
                let Ok(response) = decode::<v1::StatusResponse>(bytes) else {
                    return self.lost(ticket);
                };
                let expected = context(self.children.get(&op.child)?.flow?.reading());
                let Some(observed_phase) = Phase::from_wire(response.r#state) else {
                    return self.lost(ticket);
                };
                if response.context() != Some(&expected) || observed_phase == Phase::Released {
                    return self.lost(ticket);
                }
                Some(Ok(ChildReply::Observed {
                    phase: observed_phase,
                    observation: None,
                    disposition: Disposition::from_flag(response.r#uncertain),
                }))
            }
            Stage::Cancel => {
                let Ok(response) = decode::<v1::CancelResponse>(bytes) else {
                    return self.lost(ticket);
                };
                let expected = context(self.children.get(&op.child)?.flow?.pending());
                if response.context() != Some(&expected) {
                    return self.lost(ticket);
                }
                self.advance_flow(op.child);
                Some(Ok(ChildReply::Cancelling {
                    phase: phase(response.r#state),
                    disposition: Disposition::from_flag(response.r#uncertain),
                }))
            }
            Stage::Release => {
                let Ok(response) = decode::<v1::ReleaseResponse>(bytes) else {
                    return self.lost(ticket);
                };
                let Some(flow) = self.children.get(&op.child).and_then(|child| child.flow) else {
                    return Some(Err(ChildError::Unknown));
                };
                let expected = context(flow.pending());
                if !response.r#released || response.context() != Some(&expected) {
                    return self.lost(ticket);
                }
                if response.r#uncertain {
                    return Some(Err(ChildError::Unknown));
                }
                self.children.remove(&op.child);
                Some(Ok(ChildReply::Released))
            }
        }
    }

    fn described(&mut self, ticket: u64, described: &v1::DescribeResponse) -> Step {
        let op = self.ops.get(&ticket)?;
        if described.r#protocol_revision != PROTOCOL_REVISION {
            return Some(Err(ChildError::Rejected {
                message: format!(
                    "the child implements protocol revision {}, not {PROTOCOL_REVISION}",
                    described.r#protocol_revision
                )
                .into_bytes(),
            }));
        }
        if described.r#session_idle_ms == 0 {
            return Some(Err(ChildError::Rejected {
                message: b"child did not advertise a session idle timeout".to_vec(),
            }));
        }
        if described.r#session_floor == RESERVED_SESSION_ID {
            return Some(Err(no_session()));
        }
        if let ChildOp::Spawn { artifact, .. } = &op.op
            && artifact.len() > described.r#max_artifact_bytes as usize
        {
            return Some(Err(ChildError::Rejected {
                message: b"the child artifact is larger than the child accepts".to_vec(),
            }));
        }
        let route = &mut self.routes[op.route];
        route.described = Some(Described {
            chunk: (described.r#max_chunk_bytes as usize).max(1),
            epoch: described.context().map_or(0, |c| c.r#epoch),
            idle_ms: described.r#session_idle_ms,
        });
        // New sessions stay above every id the child has seen and above its admission floor.
        route.next_session = route
            .next_session
            .max(described.r#session_floor)
            .max(described.r#admission_floor);
        let Some(flow) = draw(route) else {
            return Some(Err(no_session()));
        };
        self.next_stage(ticket, Stage::Begin { flow })
    }

    /// Applies a refusal to operation `ticket`. Nothing the refused call asked for ran.
    fn refused(&mut self, ticket: u64, code: Code, message: String) -> Step {
        let op = self.ops.get_mut(&ticket)?;
        // A refusal may come from a stale epoch, so the next operation describes the child again.
        self.routes[op.route].described = None;
        let retired = matches!(code, Code::Aborted | Code::OutOfRange);
        match op.stage {
            _ if op.stage.effectful()
                && (op.retransmitted > 0 || retired || !matches!(op.stage, Stage::Start)) =>
            {
                Some(Err(ChildError::Unknown))
            }
            _ if op.stage.mutates() && op.retransmitted > 0 => Some(Err(ChildError::Unknown)),
            Stage::Start if code == Code::NotFound && !op.reuploaded => {
                // The child evicted the artifact after the commit. Upload once more; the Start keeps its session,
                // because the refused Start consumed no sequence number.
                op.reuploaded = true;
                self.next_stage(ticket, Stage::Describe)
            }
            _ if retired && op.retransmitted > 0 => Some(Err(ChildError::Unknown)),
            _ => Some(Err(ChildError::Rejected {
                message: bounded_diagnostic(message.as_bytes()),
            })),
        }
    }

    fn advance_flow(&mut self, child: ChildId) {
        if let Some(flow) = self.children.get_mut(&child).and_then(|c| c.flow.as_mut()) {
            flow.advance();
        }
    }

    fn abort(&mut self, ticket: u64) {
        if let Some(op) = self.ops.remove(&ticket)
            && op.in_flight
        {
            let route = &mut self.routes[op.route];
            route.link.abort();
            route.owner = None;
        }
    }

    /// The operations on `child`, in ticket order.
    fn tickets_of(&self, child: ChildId) -> Vec<u64> {
        self.ops
            .iter()
            .filter(|(_, op)| op.child == child)
            .map(|(&ticket, _)| ticket)
            .collect()
    }

    /// Drops the operations on `child` and forgets the child, freeing its identity and its slot. The callers decide
    /// whether that is allowed: only recovery and a finished release reach this.
    fn remove_child(&mut self, child: ChildId) {
        for ticket in self.tickets_of(child) {
            self.abort(ticket);
        }
        self.children.remove(&child);
    }
}

impl ChildClient for LinkChildren {
    fn lost_children(&self) -> usize {
        LinkChildren::lost_children(self)
    }

    fn submit(&mut self, op: ChildOp, deadline_ms: u64) -> Result<Ticket, ChildError> {
        let required = frame::min_reply_bytes(
            self.settings.wire.max_items(),
            self.settings.wire.max_total_bytes(),
        );
        if self.settings.max_reply_bytes < required {
            return Err(ChildError::Rejected {
                message: b"child reply bound is too small for revision 3".to_vec(),
            });
        }
        let child = op.child();
        // A quarantined child gets no further call, so no operation on it can free its slot or its identity.
        if self
            .children
            .get(&child)
            .is_some_and(|known| known.quarantined)
        {
            return Err(ChildError::Quarantined);
        }
        if self.ops.len() >= self.settings.max_ops {
            return Err(ChildError::Busy);
        }
        if self.ops.values().any(|pending| pending.child == child) {
            return Err(ChildError::Busy);
        }
        let candidate_route = match &op {
            ChildOp::Spawn { slot, .. } => self
                .routes
                .iter()
                .position(|route| route.slot == *slot)
                .ok_or(ChildError::Unavailable)?,
            _ => {
                self.children
                    .get(&child)
                    .ok_or(ChildError::UnknownChild)?
                    .route
            }
        };
        // Keep one cleanup and one heartbeat slot for each active route. A slot is already supplied when an operation
        // of that class is admitted on that route; all other traffic must leave the remaining headroom untouched. A route
        // whose children are all quarantined is not active: nothing will be sent on it, so it reserves nothing.
        let mut missing = 0usize;
        for route in 0..self.routes.len() {
            let active = self
                .children
                .values()
                .any(|child| child.route == route && !child.quarantined);
            if route != candidate_route && !active {
                continue;
            }
            let has_cleanup = (route == candidate_route && is_cleanup(&op))
                || self
                    .ops
                    .values()
                    .any(|pending| pending.route == route && is_cleanup(&pending.op));
            let has_heartbeat = (route == candidate_route && is_heartbeat(&op))
                || self
                    .ops
                    .values()
                    .any(|pending| pending.route == route && is_heartbeat(&pending.op));
            missing += usize::from(!has_cleanup) + usize::from(!has_heartbeat);
        }
        if self.ops.len().saturating_add(1).saturating_add(missing) > self.settings.max_ops {
            return Err(ChildError::Busy);
        }
        let (route, stage) = match &op {
            ChildOp::Spawn { slot, .. } => {
                if self.children.contains_key(&child) {
                    return Err(ChildError::Rejected {
                        message: b"the child id is already in use".to_vec(),
                    });
                }
                if self.children.len() >= self.settings.max_children {
                    return Err(ChildError::Busy);
                }
                let route = self
                    .routes
                    .iter()
                    .position(|r| r.slot == *slot)
                    .ok_or(ChildError::Unavailable)?;
                let stage = match draw(&mut self.routes[route]) {
                    Some(flow) => Stage::Begin { flow },
                    None => Stage::Describe,
                };
                self.children.insert(
                    child,
                    Child {
                        route,
                        quarantined: false,
                        flow: None,
                        execution: None,
                        pending: None,
                        idle_ms: 0,
                        last_contact: 0,
                    },
                );
                (route, stage)
            }
            other => {
                let known = self.children.get(&child).ok_or(ChildError::UnknownChild)?;
                if known.execution.is_none() {
                    return Err(ChildError::UnknownChild);
                }
                if known.pending.is_some()
                    && !matches!(other, ChildOp::Query { .. } | ChildOp::CleanupQuery { .. })
                {
                    return Err(ChildError::Unknown);
                }
                let stage = match other {
                    ChildOp::Resume { .. } => Stage::Resume,
                    ChildOp::Query { .. } | ChildOp::CleanupQuery { .. } => Stage::Query,
                    ChildOp::KeepAlive { .. } => Stage::KeepAlive,
                    ChildOp::Cancel { .. } => Stage::Cancel,
                    _ => Stage::Release,
                };
                (known.route, stage)
            }
        };
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        self.ops.insert(
            ticket,
            Op {
                op,
                child,
                route,
                deadline: deadline_ms,
                stage,
                dispatched: false,
                in_doubt: false,
                request: None,
                in_flight: false,
                retransmitted: 0,
                reuploaded: false,
                done: None,
            },
        );
        Ok(Ticket(ticket))
    }

    fn poll(&mut self, ticket: Ticket, now_ms: u64) -> Poll<Result<ChildReply, ChildError>> {
        let Some(op) = self.ops.get(&ticket.0) else {
            return Poll::Ready(Err(ChildError::UnknownChild));
        };
        let result = match op.done.clone() {
            Some(done) => Some(done),
            None => self.step(ticket.0, now_ms),
        };
        let Some(result) = result else {
            return Poll::Pending;
        };
        let child = self
            .ops
            .get(&ticket.0)
            .map(|op| (op.child, matches!(op.op, ChildOp::Spawn { .. })));
        if matches!(result, Err(ChildError::Unknown))
            && let Some(op) = self.ops.get(&ticket.0)
            && matches!(op.stage, Stage::Resume | Stage::Cancel | Stage::Release)
            && let Some(request) = op.request.clone()
            && let Some(child) = self.children.get_mut(&op.child)
        {
            child.pending = Some(request);
        }
        self.abort(ticket.0);
        // A spawn that was refused or never sent created nothing, so its identity is free again.
        if let Some((child, true)) = child
            && matches!(
                result,
                Err(ChildError::Rejected { .. } | ChildError::Unavailable)
            )
        {
            self.children.remove(&child);
        }
        Poll::Ready(result)
    }

    fn liveness(&self, child: ChildId) -> Option<(u32, u64)> {
        let child = self.children.get(&child)?;
        (child.execution.is_some() && child.idle_ms > 0)
            .then_some((child.idle_ms, child.last_contact))
    }

    fn abandon(&mut self, ticket: Ticket) {
        if let Some(op) = self.ops.get(&ticket.0)
            && op.dispatched
            && matches!(op.stage, Stage::Resume | Stage::Cancel | Stage::Release)
            && let Some(request) = op.request.clone()
            && let Some(child) = self.children.get_mut(&op.child)
        {
            child.pending = Some(request);
        }
        self.abort(ticket.0);
    }

    fn lost(&mut self, child: ChildId, _now_ms: u64) {
        // Quarantine only. No timer is armed: see the trait documentation for why elapsed time proves nothing. The child
        // gets no further call, so the operations it still had are dropped here.
        for ticket in self.tickets_of(child) {
            self.abort(ticket);
        }
        if let Some(entry) = self.children.get_mut(&child) {
            entry.quarantined = true;
        }
    }

    fn quarantined(&self) -> Vec<(ChildId, u32)> {
        self.children
            .iter()
            .filter(|(_, child)| child.quarantined)
            .map(|(&id, child)| (id, self.routes[child.route].slot))
            .collect()
    }

    fn recover_lost(&mut self, child: ChildId) -> bool {
        if !self
            .children
            .get(&child)
            .is_some_and(|entry| entry.quarantined)
        {
            return false;
        }
        self.remove_child(child);
        true
    }

    fn recover_slot(&mut self, slot: u32) -> usize {
        let lost: Vec<ChildId> = self
            .children
            .iter()
            .filter(|(_, child)| child.quarantined && self.routes[child.route].slot == slot)
            .map(|(&id, _)| id)
            .collect();
        for &child in &lost {
            self.remove_child(child);
        }
        // The embedding reset the node behind this slot, so what the client learned about it is stale: its epoch, its
        // session floor and its chunk size. The next spawn describes the node again before it uploads.
        for route in self.routes.iter_mut().filter(|route| route.slot == slot) {
            route.described = None;
        }
        lost.len()
    }

    fn operations(&self) -> usize {
        self.ops.len()
    }

    fn forget(&mut self, child: ChildId) {
        // A quarantined child keeps its identity and its slot. Only recovery frees them.
        if self
            .children
            .get(&child)
            .is_some_and(|entry| entry.quarantined)
        {
            return;
        }
        self.remove_child(child);
    }

    fn drive(&mut self, rounds: u32) {
        for route in &mut self.routes {
            route.link.drive(rounds);
        }
    }
}

/// Draws a new session on a described route. The reserved id is never issued.
fn is_cleanup(op: &ChildOp) -> bool {
    matches!(
        op,
        ChildOp::Cancel { .. } | ChildOp::Release { .. } | ChildOp::CleanupQuery { .. }
    )
}

fn is_heartbeat(op: &ChildOp) -> bool {
    matches!(op, ChildOp::KeepAlive { .. })
}

fn draw(route: &mut Route) -> Option<Flow> {
    let described = route.described?;
    let session = route.next_session;
    if session == RESERVED_SESSION_ID {
        return None;
    }
    route.next_session = session + 1;
    Some(Flow::new(described.epoch, session))
}

fn no_session() -> ChildError {
    ChildError::Rejected {
        message: b"the child node has no session id left to issue".to_vec(),
    }
}

fn context(stamp: Stamp) -> v1::Context {
    v1::Context {
        r#epoch: stamp.epoch,
        r#session_id: stamp.session_id,
        r#sequence: stamp.sequence,
    }
}

fn decode<M: MessageDecode + Default>(bytes: &[u8]) -> Result<M, codec::DecodeFailed> {
    codec::decode(bytes)
}

fn phase(state: v1::State) -> Phase {
    Phase::from_wire(state).unwrap_or(Phase::Running)
}

/// Converts a wire outcome under `limits`. `None` when it is malformed or over the limits.
pub(crate) fn observation_from_wire(wire: &v1::Outcome, limits: WireLimits) -> Option<Observation> {
    use v1::Outcome_::Kind;
    let outcome = match &wire.r#kind {
        Some(Kind::Yielded(tuple)) => Outcome::Yielded(Tuple::from_wire(tuple, limits).ok()?),
        Some(Kind::Returned(tuple)) => Outcome::Returned(Tuple::from_wire(tuple, limits).ok()?),
        Some(Kind::Failed(failed)) => Outcome::Failed(Failure::new(
            FailureCode::from_wire(failed.r#code),
            &failed.r#message,
        )),
        None => return None,
    };
    Some(Observation {
        outcome,
        disposition: Disposition::from_flag(wire.r#uncertain),
    })
}
