//! The node: request handling, uploads, the ledger, and the scheduler that runs the VM.
//!
//! [`Node::handle`] answers one call and never runs Lua. [`Node::poll`] does one bounded round of work: one fuel slice
//! for each runnable execution and one step of each native or child operation in progress. [`Node::tick`] moves the
//! node's monotonic clock, which expires idle sessions and execution leases. Time comes only from `tick`.
//!
//! Every execution has exactly one disposition, kept by its lifecycle record. A child's disposition is absorbed when its
//! outcome is observed, whatever the outcome's kind, so a returned value or a yield cannot hide an uncertain descendant.
//!
//! An execution that ends, is cancelled, expires or loses its session first cleans up the children it still owns:
//! cancel, observe a terminal state, release. Cleanup has a deadline. A child that cannot be shown to have stopped by
//! then leaves the execution uncertain. An uncertain execution whose caller is gone is kept as an unresolved record,
//! which holds its execution slot until the embedding discards it with [`Node::discard_uncertain`] (or the node restarts), so
//! admission fails closed rather than forgetting it. A child that cleanup could not settle is quarantined by the child client
//! and keeps its child slot until [`Node::recover_lost_child`] or [`Node::recover_lost_slot`]; time never frees it.
//!
//! A native the node could not stop is parked when its execution's cleanup deadline passes. The node keeps polling it
//! until it completes. Each native holds a slot while it is in flight or parked, and a new native starts only while a
//! slot is free, so the parked set is bounded by `max_executions`.

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::rc::Rc;
use alloc::string::ToString;
use alloc::vec::Vec;
use core::task::Poll;

use micropb::{MessageDecode, MessageEncode};
use protolink::grpc::{Code, Status};
use protolink_lua::PROTOCOL_REVISION;
use protolink_lua::child::{ChildClient, ChildError, ChildId, ChildOp, ChildReply, Ticket};
use protolink_lua::codec;
use protolink_lua::frame::{self, FrameError, Shape};
use protolink_lua::ledger::{
    Admission, Answer, BoundedAnswer, Ledger, MAX_MUTATION_ANSWER_BYTES, RESERVED_SESSION_ID,
    mutation_fingerprint,
};
use protolink_lua::lifecycle::{
    Delivery, Disposition, Event, Execution, Failure, FailureCode, Observation, Outcome, Phase,
};
use protolink_lua::value::Tuple;
use protolink_lua::wire::v1;
use protolink_lua_bytecode::{AdmitError, Artifact, admit, contract_of, digest};
use protolink_lua_vm::{Effect, EffectResult, Machine, Program, Step};

use crate::config::{ConfigError, NodeConfig};
use crate::platform::{NativeCall, NativeStop, NativeTicket, Platform};

/// The gRPC method paths of `lua.execution.v1.Execution`.
pub(crate) mod path {
    /// Service prefix.
    pub const PREFIX: &str = "/lua.execution.v1.Execution/";
}

/// What a state-changing call does to its session once it has run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ends {
    /// The session continues. Most calls are of this kind.
    Never,
    /// The session is released when the call succeeds. A release that the node refuses, because the execution is not
    /// finished, keeps the session so the caller can cancel and release again.
    OnSuccess,
    /// The session is released whatever the outcome. Only the commit of the caller's own upload uses it: that commit
    /// consumes the upload whatever its result, so the session has nothing left to hold, and releasing it makes a
    /// retransmission answer `Aborted` instead of running. A commit that names another session's upload, or none, is
    /// refused before anything is reserved and leaves the caller's session alone.
    Always,
}

/// One upload in progress.
#[derive(Debug)]
struct Upload {
    session: u64,
    digest: [u8; 32],
    total: usize,
    received: Vec<u8>,
}

/// An artifact accepted by commit.
#[derive(Debug)]
struct Stored {
    artifact: Rc<Artifact>,
    /// Clock value of the last commit or start that used the artifact. The least recently used one is evicted first.
    last_used: u64,
}

/// Where a child task stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TaskState {
    /// The spawn is in progress.
    Spawning(Ticket),
    /// The child exists in `Ready`. Nothing ran on it.
    Ready,
    /// The first resume is in progress.
    Resuming(Ticket),
    /// The resume may have run; its exact receipt must be reconciled before another mutation.
    ReconcileResume,
    /// A receipt-bearing read for the uncertain resume is in progress.
    ReconcilingResume(Ticket),
    /// A known child needs cleanup after a failed observation or liveness check.
    NeedsCleanup,
    /// The child was resumed. Its outcome is read next.
    Running,
    /// A read of the outcome is in progress.
    Querying(Ticket),
    /// The child yielded. It stays owned until it is cancelled.
    Yielded,
    /// The child is terminal. It is released next.
    Terminal,
    /// The release is in progress.
    Releasing(Ticket),
    /// Cleanup asked the child to cancel.
    Cancelling(Ticket),
    /// The child acknowledged the cancel. Its terminal state is read next.
    CancelWait,
    /// Cleanup reads the child's state.
    Checking(Ticket),
    /// Nothing is left on the child, or nothing was ever created.
    Done,
    /// The child may exist, and nothing more can be learned about it.
    Lost,
}

/// One child an execution spawned.
#[derive(Debug)]
struct Task {
    id: ChildId,
    slot: u32,
    state: TaskState,
    /// The child's outcome once known, kept so a repeated await gets the same answer.
    settled: Option<Observation>,
    /// Why the last await of the task failed before an outcome existed.
    failure: Option<Vec<u8>>,
    /// At most one background status read for this child.
    heartbeat: Option<Ticket>,
}

/// An effect the machine waits on.
#[derive(Debug)]
enum Pending {
    Native(NativeTicket),
    Spawn(usize),
    Await(usize),
}

/// Why cleanup runs, and what it finishes with.
#[derive(Debug)]
enum Reason {
    /// The entry returned or failed. The event is recorded once the children are settled.
    Exit(Event),
    /// Cancellation, by request, lease expiry or abandonment.
    Cancel,
}

#[derive(Debug)]
struct Cleanup {
    reason: Reason,
    deadline: u64,
    /// A native that could not be interrupted. It is polled until it completes or the deadline passes.
    native: Option<NativeTicket>,
}

/// One execution record with its machine.
struct Exec {
    session: u64,
    lifecycle: Execution,
    machine: Option<Machine>,
    /// Digest of the artifact the execution runs. While the machine runs, the artifact stays in the store.
    digest: [u8; 32],
    pending: Option<Pending>,
    tasks: Vec<Task>,
    cleanup: Option<Cleanup>,
    /// Absolute lease deadline on the node's clock.
    lease: Option<u64>,
    /// The caller's session is gone, so nobody will release the record.
    orphaned: bool,
}

/// What a node holds, by component. See [`Node::usage`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    /// Execution records, unresolved ones included. At most `max_executions`.
    pub executions: usize,
    /// Records whose caller is gone and whose outcome is uncertain.
    pub unresolved: usize,
    /// Child handles across every execution. At most `max_executions * max_child_tasks`.
    pub child_tasks: usize,
    /// Uploads in progress. At most `max_uploads`.
    pub uploads: usize,
    /// Bytes reserved by uploads in progress. At most `max_uploads * max_artifact_bytes`.
    pub upload_bytes: usize,
    /// Stored artifacts. At most `max_artifacts`.
    pub artifacts: usize,
    /// Live ledger sessions. At most `max_sessions`.
    pub sessions: usize,
    /// Bytes of the retirement window. Fixed by `retired_window`.
    pub retirement_bytes: usize,
    /// Natives that could not be stopped before their execution ended. They are polled until they complete. Each one
    /// holds a native slot, so `parked_natives` and the natives in flight together stay within `max_executions`.
    pub parked_natives: usize,
    /// Children the node gave up on and still quarantines. Each one holds a child slot. Time never releases it: only
    /// [`Node::recover_lost_child`] or [`Node::recover_lost_slot`] does, and a restart discards it with the node. At most
    /// the child client's child limit.
    pub lost_children: usize,
    /// Executions whose cleanup is still running. Each one holds the cancel, query and release traffic it needs, and
    /// [`Node::discard_uncertain`] refuses it.
    pub cleanups: usize,
    /// Child operations in progress, cleanup traffic included. At most the child client's operation limit.
    pub child_operations: usize,
    /// Quarantined children the embedding recovered since the node started. Their effects were never proved to have
    /// stopped. See [`Node::recover_lost_child`].
    pub recovered_children: u64,
}

/// One execution node.
pub struct Node {
    config: NodeConfig,
    ledger: Ledger,
    uploads: BTreeMap<u64, Upload>,
    next_upload: u64,
    artifacts: BTreeMap<[u8; 32], Stored>,
    artifact_clock: u64,
    executions: BTreeMap<u64, Exec>,
    next_execution: u64,
    platform: Box<dyn Platform>,
    children: Option<Box<dyn ChildClient>>,
    next_child: u64,
    now: u64,
    /// Where the next scheduler round starts, so every execution gets its turn.
    cursor: u64,
    /// Natives whose execution ended before they completed. The node keeps polling them, so the provider's work is
    /// never forgotten. Each one holds a native slot taken when it started.
    parked_natives: Vec<NativeTicket>,
    /// Uncertain orphans discarded by the embedding since the node started. See [`Node::discard_uncertain`].
    discarded_uncertain: u64,
    /// Quarantined children recovered by the embedding since the node started. See [`Node::recover_lost_child`].
    recovered_children: u64,
}

impl core::fmt::Debug for Node {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Node")
            .field("contract", &self.config.contract)
            .field("epoch", &self.config.epoch)
            .field("executions", &self.executions.len())
            .finish()
    }
}

fn context_of(request_context: Option<&v1::Context>) -> v1::Context {
    request_context.copied().unwrap_or_default()
}

fn invalid(message: impl ToString) -> Status {
    Status::new(Code::InvalidArgument, message)
}

fn precondition(message: impl ToString) -> Status {
    Status::new(Code::FailedPrecondition, message)
}

/// Why an orphaned record cannot be discarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiscardError {
    /// No execution record has this id.
    UnknownExecution,
    /// The record is not an orphan whose outcome is uncertain. Records with a caller are released by the caller.
    NotUncertainOrphan,
    /// The record still holds a native whose provider has not completed. Its slot stays taken until it does.
    NativeInFlight,
    /// Cleanup or child reconciliation is still running for the record. Discarding now would strand its operations and
    /// children outside any tracked state. Poll until cleanup ends: children it could not settle are then quarantined,
    /// and the record can be discarded.
    CleanupActive,
}

/// Why a quarantined child cannot be recovered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoverError {
    /// No quarantined child has this id. A child that is still being cleaned up is not quarantined yet.
    NotQuarantined,
    /// The node has no child client.
    NoChildren,
}

impl core::fmt::Display for RecoverError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotQuarantined => f.write_str("no quarantined child has this id"),
            Self::NoChildren => f.write_str("the node has no child client"),
        }
    }
}

impl core::fmt::Display for DiscardError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownExecution => f.write_str("no execution record has this id"),
            Self::NotUncertainOrphan => {
                f.write_str("the record is not an orphan with an uncertain outcome")
            }
            Self::NativeInFlight => {
                f.write_str("the record still holds a native that has not completed")
            }
            Self::CleanupActive => f.write_str("cleanup of the record's children is still running"),
        }
    }
}

fn unknown_execution(id: u64) -> Status {
    Status::new(Code::NotFound, format!("no execution {id}"))
}

fn no_artifact_slot() -> Status {
    Status::new(Code::ResourceExhausted, "no artifact slot is free")
}

/// The refusal for an upload id that the caller's session does not hold. Missing and foreign ids look the same, so a
/// caller learns nothing about another session's uploads.
fn no_such_upload() -> Status {
    Status::new(Code::NotFound, "no such upload")
}

/// The status of a refused admission. `Aborted` means the session was released and the call will never run;
/// `OutOfRange` means the id is below the admission floor and the call will never run.
fn admission_status(admission: &Admission<'_>, context: &v1::Context) -> Status {
    match admission {
        Admission::New | Admission::Replay(_) => Status::new(Code::Internal, "admission"),
        Admission::Conflict => precondition("sequence reused with a different payload"),
        Admission::InFlight => precondition(format!(
            "session {} has a call in flight; retry after it completes",
            context.session_id
        )),
        Admission::OutOfOrder { expected } => precondition(format!(
            "sequence {} is out of order, expected {expected}",
            context.sequence
        )),
        Admission::StaleEpoch { current } => {
            precondition(format!("stale epoch, node epoch is {current}"))
        }
        Admission::Capacity => Status::new(Code::ResourceExhausted, "session capacity exhausted"),
        // A refusal says only that this attempt is not admitted. It does not say whether an earlier attempt of the same
        // call ran, so a caller that lost a reply reconciles instead of retrying.
        Admission::Retired => Status::new(
            Code::Aborted,
            format!(
                "session {} has ended; this call is refused and does not run. An earlier call on it may have run",
                context.session_id
            ),
        ),
        Admission::BelowWindow { floor } => Status::new(
            Code::OutOfRange,
            format!(
                "session {} is below the admission floor {floor}; this call is refused and does not run",
                context.session_id
            ),
        ),
        Admission::Reserved => invalid(format!(
            "session id {RESERVED_SESSION_ID} is reserved and cannot be used"
        )),
        Admission::SequenceExhausted => precondition(format!(
            "session {} has used every sequence number and cannot take another call; it ends when it goes idle",
            context.session_id
        )),
    }
}

impl Node {
    /// Creates a node whose clock starts at `now_ms`. `children` is the client that reaches the node's child slots; a
    /// node with routes needs one.
    pub fn new(
        config: NodeConfig,
        platform: Box<dyn Platform>,
        children: Option<Box<dyn ChildClient>>,
        now_ms: u64,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        if !config.routes.is_empty() && children.is_none() {
            return Err(ConfigError::Zero(
                "a child client for the configured routes",
            ));
        }
        let mut ledger = Ledger::new(
            config.epoch,
            config.max_sessions,
            config.max_answer_bytes,
            config.retired_window,
        )
        .map_err(ConfigError::Ledger)?;
        ledger.tick(now_ms, u64::from(config.session_idle_ms));
        Ok(Self {
            config,
            ledger,
            uploads: BTreeMap::new(),
            next_upload: 1,
            artifacts: BTreeMap::new(),
            artifact_clock: 0,
            executions: BTreeMap::new(),
            next_execution: 1,
            platform,
            children,
            next_child: 1,
            now: now_ms,
            cursor: 0,
            parked_natives: Vec::new(),
            discarded_uncertain: 0,
            recovered_children: 0,
        })
    }

    /// The node's contract name.
    pub fn contract(&self) -> &str {
        &self.config.contract
    }

    /// The node's configuration.
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// Executions the node holds, unresolved records included.
    pub fn execution_count(&self) -> usize {
        self.executions.len()
    }

    /// What the node holds now, by component. Every figure is bounded by the configuration.
    pub fn usage(&self) -> Usage {
        Usage {
            executions: self.executions.len(),
            unresolved: self.unresolved_count(),
            child_tasks: self.executions.values().map(|exec| exec.tasks.len()).sum(),
            uploads: self.uploads.len(),
            upload_bytes: self
                .uploads
                .values()
                .map(|upload| upload.received.capacity())
                .sum(),
            artifacts: self.artifacts.len(),
            sessions: self.ledger.session_count(),
            retirement_bytes: self.ledger.retirement_bytes(),
            parked_natives: self.parked_natives.len(),
            lost_children: self
                .children
                .as_ref()
                .map_or(0, |children| children.lost_children()),
            cleanups: self
                .executions
                .values()
                .filter(|exec| exec.cleanup.is_some())
                .count(),
            child_operations: self
                .children
                .as_ref()
                .map_or(0, |children| children.operations()),
            recovered_children: self.recovered_children,
        }
    }

    /// Records whose caller is gone and whose disposition is uncertain. They hold their slots until the embedding discards
    /// them with [`Node::discard_uncertain`], or until the node restarts.
    pub fn unresolved_count(&self) -> usize {
        self.executions
            .values()
            .filter(|exec| exec.orphaned && exec.lifecycle.disposition().is_uncertain())
            .count()
    }

    /// Discards an orphaned record whose outcome is uncertain, so that its slot is free again.
    ///
    /// This is the explicit recovery path for an uncertain orphan. Calling it is a decision the embedding or its operator
    /// takes, after the effects were checked outside the node, or after they were accepted as unknown. The node never
    /// makes that decision itself, and discarding does not change what happened: nothing reports the outcome as certain,
    /// and the count of discards is kept in [`Node::discarded_uncertain`]. The discard is refused while the record still
    /// holds a native, because that native's slot is accounted for until the provider completes.
    pub fn discard_uncertain(&mut self, execution: u64) -> Result<(), DiscardError> {
        let Some(exec) = self.executions.get(&execution) else {
            return Err(DiscardError::UnknownExecution);
        };
        if !(exec.orphaned && exec.lifecycle.disposition().is_uncertain()) {
            return Err(DiscardError::NotUncertainOrphan);
        }
        let native_in_flight = matches!(exec.pending, Some(Pending::Native(_)))
            || exec
                .cleanup
                .as_ref()
                .is_some_and(|cleanup| cleanup.native.is_some());
        if native_in_flight {
            return Err(DiscardError::NativeInFlight);
        }
        // Children the node could not settle are quarantined in the child client, which is a tracked, bounded state, so a
        // record whose tasks are all settled can go. A task still being worked on holds a client operation that only this
        // record would drive and release, so the discard waits for it.
        if exec.cleanup.is_some() || exec.tasks.iter().any(|task| !settled(task.state)) {
            return Err(DiscardError::CleanupActive);
        }
        self.executions.remove(&execution);
        self.discarded_uncertain += 1;
        Ok(())
    }

    /// Uncertain orphans the embedding has discarded since the node started.
    pub fn discarded_uncertain(&self) -> u64 {
        self.discarded_uncertain
    }

    /// The children the node gave up on and keeps in quarantine, with the slot id of each one's route. Each holds a child
    /// slot. At most the child client's child limit.
    pub fn lost_child_ids(&self) -> Vec<(ChildId, u32)> {
        self.children
            .as_ref()
            .map_or_else(Vec::new, |children| children.quarantined())
    }

    /// Frees the slot of one quarantined child, on the embedding's decision.
    ///
    /// **What the caller accepts.** A child is quarantined because the node lost contact before it learned how the child
    /// ended. The node cannot prove that the child stopped: the child's idle timeout is its own claim on its own clock, and
    /// an uninterruptible native outlives its session. Recovering the child frees the slot for new work, so the caller
    /// takes the risk that the old child is still running, and that its effects may still land. Use it after checking the
    /// child outside the node, or after accepting that risk.
    ///
    /// **What the node keeps.** The execution record that owned the child stays uncertain; nothing reports any outcome as
    /// certain. The recovery is counted in [`Node::recovered_children`] and [`Usage::recovered_children`], so a caller can
    /// tell that capacity was recovered by decision and not by proof.
    pub fn recover_lost_child(&mut self, child: ChildId) -> Result<(), RecoverError> {
        let children = self.children.as_mut().ok_or(RecoverError::NoChildren)?;
        if !children.recover_lost(child) {
            return Err(RecoverError::NotQuarantined);
        }
        self.recovered_children += 1;
        Ok(())
    }

    /// Frees every quarantined child on route `slot`, after the embedding reset the child node behind it, for example by
    /// power-cycling the device. The reset is the embedding's evidence: the node cannot check it. A reset that did not
    /// really stop the child's work is the caller's error. Returns how many children were released and counts them in
    /// [`Node::recovered_children`].
    pub fn recover_lost_slot(&mut self, slot: u32) -> usize {
        let Some(children) = self.children.as_mut() else {
            return 0;
        };
        let released = children.recover_slot(slot);
        self.recovered_children += released as u64;
        released
    }

    /// Quarantined children recovered by the embedding since the node started.
    pub fn recovered_children(&self) -> u64 {
        self.recovered_children
    }

    /// Answers one unary call. Lua never runs here. A request larger than `max_request_bytes`, or with more tuple values
    /// than the node accepts, is refused before it is decoded.
    pub fn handle(&mut self, method: &str, request: &[u8]) -> Result<Vec<u8>, Status> {
        let Some(name) = method.strip_prefix(path::PREFIX) else {
            return Err(Status::new(
                Code::Unimplemented,
                format!("unknown service {method}"),
            ));
        };
        frame::check(
            Shape::of_request(name),
            request,
            self.config.max_request_bytes,
            self.config.vm.wire.max_items(),
        )
        .map_err(|error| match error {
            FrameError::TooLarge { .. } => Status::new(Code::ResourceExhausted, error.to_string()),
            _ => invalid(error.to_string()),
        })?;
        match name {
            "Describe" => self.describe(request),
            "BeginUpload" => {
                self.mutate::<v1::BeginUploadRequest, _>(name, request, Ends::Never, |n, r| {
                    n.begin_upload(r)
                })
            }
            "PutChunk" => {
                self.mutate::<v1::PutChunkRequest, _>(name, request, Ends::Never, |n, r| {
                    n.put_chunk(r)
                })
            }
            "CommitArtifact" => {
                self.mutate::<v1::CommitArtifactRequest, _>(name, request, Ends::Always, |n, r| {
                    n.commit(r)
                })
            }
            "Start" => {
                self.mutate::<v1::StartRequest, _>(name, request, Ends::Never, |n, r| n.start(r))
            }
            "Resume" => {
                self.mutate::<v1::ResumeRequest, _>(name, request, Ends::Never, |n, r| n.resume(r))
            }
            "QueryOutcome" => {
                self.query::<v1::QueryOutcomeRequest, _>(request, |n, r| n.query_outcome(r))
            }
            "Status" => self.query::<v1::StatusRequest, _>(request, |n, r| n.status(r)),
            "Cancel" => {
                self.mutate::<v1::CancelRequest, _>(name, request, Ends::Never, |n, r| n.cancel(r))
            }
            "Release" => {
                self.mutate::<v1::ReleaseRequest, _>(name, request, Ends::OnSuccess, |n, r| {
                    n.release(r)
                })
            }
            other => Err(Status::new(
                Code::Unimplemented,
                format!("unknown method {other}"),
            )),
        }
    }

    fn describe(&mut self, request: &[u8]) -> Result<Vec<u8>, Status> {
        let _: v1::DescribeRequest = codec::decode(request).map_err(invalid)?;
        let mut response = v1::DescribeResponse {
            r#contract: self.config.contract.clone(),
            r#artifact_format: u32::from(protolink_lua_bytecode::FORMAT_VERSION),
            r#profile: u32::from(protolink_lua_bytecode::PROFILE_VERSION),
            r#max_artifact_bytes: self.config.bytecode.max_artifact_bytes as u32,
            r#max_chunk_bytes: self.config.max_chunk_bytes,
            r#max_tuple_items: self.config.vm.wire.max_items() as u32,
            r#max_string_bytes: self.config.vm.wire.max_string_bytes() as u32,
            r#max_executions: self.config.max_executions as u32,
            r#has_child: !self.config.routes.is_empty(),
            r#session_floor: self.ledger.session_floor(),
            r#protocol_revision: PROTOCOL_REVISION,
            r#admission_floor: self.ledger.admission_floor(),
            r#retired_window: self.ledger.retired_window(),
            r#max_request_bytes: self.config.max_request_bytes as u32,
            r#session_idle_ms: self.config.session_idle_ms,
            r#final_replay_slots: self.ledger.final_replay_slots() as u32,
            ..v1::DescribeResponse::default()
        };
        response.set_context(v1::Context {
            r#epoch: self.config.epoch,
            ..v1::Context::default()
        });
        Ok(codec::encode(&response))
    }

    /// Runs a state-changing call through the ledger. The session slot and the answer storage are reserved before the
    /// call runs, so a call that changed state always has its answer recorded. A retransmission returns the recorded
    /// answer, and a conflicting payload is refused without running. `ends` says what the call does to its session.
    ///
    /// Admission comes first, so a retransmission is answered from the ledger even after its upload is gone. The role
    /// and upload checks come next, and refuse before anything is reserved: a refused call leaves no trace.
    fn mutate<M, F>(
        &mut self,
        method: &str,
        request: &[u8],
        ends: Ends,
        run: F,
    ) -> Result<Vec<u8>, Status>
    where
        M: Envelope + NamesUpload + MessageDecode + Default,
        F: FnOnce(&mut Self, &M) -> Result<Answer, Status>,
    {
        let message: M = codec::decode(request).map_err(invalid)?;
        let context = message.envelope();
        let session = context.session_id;
        let digest = mutation_fingerprint(method, request);
        match self.ledger.admit(&context, &digest) {
            Admission::New => {}
            Admission::Replay(answer) => {
                // A retransmission is activity: a caller that retries keeps its session alive.
                let answer = answer.to_vec();
                self.ledger.touch(session);
                return Ok(answer);
            }
            other => return Err(admission_status(&other, &context)),
        }
        // An execution needs a fresh session. A session that opened an upload has spent sequence 1, so it cannot start one
        // either: the ledger admits only the successor of its last call.
        if method == "Start"
            && (context.sequence != 1 || self.executions.values().any(|e| e.session == session))
        {
            return Err(precondition("an execution requires a fresh session"));
        }
        if matches!(method, "BeginUpload" | "PutChunk" | "CommitArtifact")
            && self.executions.values().any(|e| e.session == session)
        {
            return Err(precondition("session belongs to an execution"));
        }
        // A session holds at most one upload, so ending the session never strands a second one.
        if method == "BeginUpload" && self.uploads.values().any(|u| u.session == session) {
            return Err(precondition("session already holds an upload"));
        }
        // An upload is changed only by the session that opened it, and a refusal here touches nothing.
        if let Some(upload) = message.named_upload()
            && !self
                .uploads
                .get(&upload)
                .is_some_and(|staged| staged.session == session)
        {
            return Err(no_such_upload());
        }
        let reservation = self
            .ledger
            .reserve(&context, digest)
            .map_err(|refused| admission_status(&refused, &context))?;
        // The reply is a copy of the answer, made after the effect. Its storage is reserved here, before the call runs, so
        // a failure to allocate refuses the call while nothing has changed.
        let mut reply: Vec<u8> = Vec::new();
        if reply.try_reserve_exact(MAX_MUTATION_ANSWER_BYTES).is_err() {
            self.ledger.abandon(reservation);
            return Err(Status::new(
                Code::ResourceExhausted,
                "no room to reply to the call",
            ));
        }
        match run(self, &message) {
            Ok(answer) => {
                // A final call ends its session, and its answer moves to the ledger's final table for replay. The reservation
                // is taken within this call, so it cannot be stale, and the ledger reports that as `false` regardless.
                let recorded = if ends == Ends::Never {
                    self.ledger.record(reservation, &answer)
                } else {
                    self.uploads.retain(|_, upload| upload.session != session);
                    self.ledger.finish(reservation, &answer)
                };
                debug_assert!(recorded, "a reservation taken within one call is recorded");
                reply.extend_from_slice(answer.as_slice());
                Ok(reply)
            }
            Err(status) => {
                self.ledger.abandon(reservation);
                if ends == Ends::Always {
                    self.retire(session);
                }
                Err(status)
            }
        }
    }

    /// Runs a call that does not change state. It is checked for epoch only, and it keeps its session alive.
    fn query<M, F>(&mut self, request: &[u8], run: F) -> Result<Vec<u8>, Status>
    where
        M: Envelope + MessageDecode + Default,
        F: FnOnce(&mut Self, &M) -> Result<Vec<u8>, Status>,
    {
        let message: M = codec::decode(request).map_err(invalid)?;
        let context = message.envelope();
        if context.epoch != self.config.epoch {
            return Err(precondition(format!(
                "stale epoch, node epoch is {}",
                self.config.epoch
            )));
        }
        if context.session_id == RESERVED_SESSION_ID {
            return Err(invalid(format!(
                "session id {RESERVED_SESSION_ID} is reserved and cannot be used"
            )));
        }
        let answer = run(self, &message)?;
        self.ledger.touch(context.session_id);
        Ok(answer)
    }

    /// Encodes the answer of a mutating call. The response types are bounded by the schema, so the answer always fits the
    /// ledger's storage: the check happens at compile time, before any call can run.
    fn answer<M: BoundedAnswer + Envelope>(&self, mut message: M, context: v1::Context) -> Answer {
        message.set_envelope(v1::Context {
            r#epoch: self.config.epoch,
            ..context
        });
        Answer::encode(&message)
    }

    fn respond<M: MessageEncode + Envelope>(
        &self,
        mut message: M,
        context: v1::Context,
    ) -> Vec<u8> {
        message.set_envelope(v1::Context {
            r#epoch: self.config.epoch,
            ..context
        });
        codec::encode(&message)
    }

    fn begin_upload(&mut self, request: &v1::BeginUploadRequest) -> Result<Answer, Status> {
        let total = request.r#total_length as usize;
        if self.uploads.len() >= self.config.max_uploads {
            return Err(Status::new(
                Code::ResourceExhausted,
                "too many uploads open",
            ));
        }
        if total == 0 || total > self.config.bytecode.max_artifact_bytes {
            return Err(invalid("upload length is outside the artifact bound"));
        }
        let digest: [u8; 32] = request
            .r#digest
            .as_slice()
            .try_into()
            .map_err(|_| invalid("digest must be 32 bytes"))?;
        if !self.has_artifact_room(digest) {
            return Err(no_artifact_slot());
        }
        let id = self.next_upload;
        self.next_upload += 1;
        let context = context_of(request.context());
        self.uploads.insert(
            id,
            Upload {
                session: context.r#session_id,
                digest,
                total,
                received: Vec::with_capacity(total),
            },
        );
        let response = v1::BeginUploadResponse {
            r#upload_id: id,
            ..v1::BeginUploadResponse::default()
        };
        Ok(self.answer(response, context))
    }

    fn put_chunk(&mut self, request: &v1::PutChunkRequest) -> Result<Answer, Status> {
        let upload = self
            .uploads
            .get_mut(&request.r#upload_id)
            .ok_or_else(no_such_upload)?;
        if request.r#offset as usize != upload.received.len() {
            return Err(precondition(format!(
                "chunk offset {} is not {}",
                request.r#offset,
                upload.received.len()
            )));
        }
        if request.r#data.len() > self.config.max_chunk_bytes as usize {
            return Err(invalid("chunk exceeds the advertised size"));
        }
        if upload.received.len() + request.r#data.len() > upload.total {
            return Err(invalid("chunk overruns the declared length"));
        }
        upload.received.extend_from_slice(&request.r#data);
        let received = upload.received.len() as u32;
        let context = context_of(request.context());
        Ok(self.answer(
            v1::PutChunkResponse {
                received,
                ..v1::PutChunkResponse::default()
            },
            context,
        ))
    }

    /// Consumes the caller's upload. The upload leaves the store before any check, so every outcome reclaims its staged
    /// bytes: a complete, verified and admitted artifact is stored, and every refusal drops the bytes. The session then
    /// ends, as `Ends::Always` says.
    fn commit(&mut self, request: &v1::CommitArtifactRequest) -> Result<Answer, Status> {
        let upload = self
            .uploads
            .remove(&request.r#upload_id)
            .ok_or_else(no_such_upload)?;
        if upload.received.len() != upload.total {
            return Err(precondition("upload is incomplete"));
        }
        if digest(&upload.received) != upload.digest {
            return Err(invalid(
                "artifact digest does not match the declared digest",
            ));
        }
        // The same admission the host compiler ran with this node's limits.
        let artifact = admit(
            &upload.received,
            &self.config.bytecode,
            &self.config.contract_digest,
            &self.config.operations,
        )
        .map_err(|error| match error {
            AdmitError::WrongContract => invalid("artifact was compiled for another contract"),
            other => invalid(other.to_string()),
        })?;
        self.store(upload.digest, artifact)?;
        let context = context_of(request.context());
        Ok(Answer::commit(
            v1::Context {
                r#epoch: self.config.epoch,
                ..context
            },
            &upload.digest,
        ))
    }

    fn start(&mut self, request: &v1::StartRequest) -> Result<Answer, Status> {
        let digest: [u8; 32] = request
            .r#digest
            .as_slice()
            .try_into()
            .map_err(|_| invalid("digest must be 32 bytes"))?;
        let shared = {
            let stored = self.artifacts.get(&digest).ok_or_else(|| {
                Status::new(Code::NotFound, "artifact is not committed on this node")
            })?;
            Rc::clone(&stored.artifact)
        };
        for edge in &shared.children {
            let route = self.config.route(edge.slot).ok_or_else(|| {
                invalid(format!(
                    "a child edge names slot {}, which this node does not route",
                    edge.slot
                ))
            })?;
            if contract_of(&edge.bytes) != Some(route.contract_digest) {
                return Err(invalid(
                    "a child artifact was compiled for another contract",
                ));
            }
        }
        if self.executions.len() >= self.config.max_executions {
            return Err(Status::new(
                Code::ResourceExhausted,
                "no execution slot is free",
            ));
        }
        let wire = self.config.vm.wire;
        // An absent tuple is the empty tuple. The request is borrowed rather than cloned, so a refused tuple is never copied.
        let empty = v1::Tuple::default();
        let captures =
            Tuple::from_wire(request.captures().unwrap_or(&empty), wire).map_err(invalid)?;
        let arguments =
            Tuple::from_wire(request.arguments().unwrap_or(&empty), wire).map_err(invalid)?;
        if captures.payload_bytes() > self.config.max_capture_bytes {
            return Err(invalid(format!(
                "captures are {} bytes; the node accepts {}",
                captures.payload_bytes(),
                self.config.max_capture_bytes
            )));
        }
        let program = Program::verified_shared(shared, &self.config.bytecode).map_err(invalid)?;
        let machine = Machine::start(
            program,
            request.r#entry as usize,
            captures.values(),
            arguments.values(),
            self.config.vm,
        )
        .map_err(|e| invalid(format!("cannot start the entry: {e:?}")))?;
        self.artifact_clock += 1;
        if let Some(stored) = self.artifacts.get_mut(&digest) {
            stored.last_used = self.artifact_clock;
        }
        let id = self.next_execution;
        self.next_execution += 1;
        let context = context_of(request.context());
        let lease = (request.r#lease_ms > 0).then(|| self.now + u64::from(request.r#lease_ms));
        self.executions.insert(
            id,
            Exec {
                session: context.r#session_id,
                lifecycle: Execution::new(arguments),
                machine: Some(machine),
                digest,
                pending: None,
                tasks: Vec::new(),
                cleanup: None,
                lease,
                orphaned: false,
            },
        );
        Ok(self.answer(
            v1::StartResponse {
                r#execution_id: id,
                r#state: Phase::Ready.to_wire(),
                ..v1::StartResponse::default()
            },
            context,
        ))
    }

    fn check_owner(&self, id: u64, session: u64) -> Result<(), Status> {
        let exec = self
            .executions
            .get(&id)
            .ok_or_else(|| unknown_execution(id))?;
        if exec.session != session {
            return Err(precondition("execution belongs to another session"));
        }
        Ok(())
    }

    fn receipt(&self, session: u64) -> Option<v1::MutationReceipt> {
        self.ledger
            .receipt(session)
            .map(|receipt| v1::MutationReceipt {
                r#sequence: receipt.sequence,
                r#fingerprint: receipt.fingerprint.to_vec(),
            })
    }

    fn resume(&mut self, request: &v1::ResumeRequest) -> Result<Answer, Status> {
        self.check_owner(
            request.r#execution_id,
            context_of(request.context()).r#session_id,
        )?;
        let wire = self.config.vm.wire;
        let exec = self
            .executions
            .get_mut(&request.r#execution_id)
            .ok_or_else(|| unknown_execution(request.r#execution_id))?;
        let empty = v1::Tuple::default();
        let values = Tuple::from_wire(request.values().unwrap_or(&empty), wire).map_err(invalid)?;
        if exec.lifecycle.phase() == Phase::LuaYielded {
            // Checked before the record changes, so a refused reply leaves the execution yielded.
            let machine = exec
                .machine
                .as_mut()
                .ok_or_else(|| precondition("execution has no machine"))?;
            let mut probe = exec.lifecycle.clone();
            let delivery = probe
                .admit(request.r#turn, values.clone())
                .map_err(|e| precondition(format!("resume refused: {e:?}")))?;
            if let Delivery::YieldReply(reply) = delivery {
                machine
                    .resume_yield(reply.values())
                    .map_err(|_| precondition("resume values are refused"))?;
            }
            exec.lifecycle = probe;
        } else {
            exec.lifecycle
                .admit(request.r#turn, values)
                .map_err(|e| precondition(format!("resume refused: {e:?}")))?;
        }
        let phase = exec.lifecycle.phase();
        let context = context_of(request.context());
        Ok(self.answer(
            v1::ResumeResponse {
                r#accepted: true,
                r#state: phase.to_wire(),
                ..v1::ResumeResponse::default()
            },
            context,
        ))
    }

    fn query_outcome(&mut self, request: &v1::QueryOutcomeRequest) -> Result<Vec<u8>, Status> {
        self.check_owner(
            request.r#execution_id,
            context_of(request.context()).r#session_id,
        )?;
        let exec = self
            .executions
            .get(&request.r#execution_id)
            .ok_or_else(|| unknown_execution(request.r#execution_id))?;
        let disposition = exec.lifecycle.disposition();
        let observation = exec.lifecycle.observation(request.r#turn);
        let mut response = v1::QueryOutcomeResponse {
            r#state: exec.lifecycle.phase().to_wire(),
            r#available: observation.is_some(),
            r#uncertain: disposition.is_uncertain(),
            ..v1::QueryOutcomeResponse::default()
        };
        if let Some(receipt) = self.receipt(exec.session) {
            response.set_last_mutation(receipt);
        }
        if let Some(observation) = observation {
            response.set_outcome(observation_to_wire(&observation));
        }
        let context = context_of(request.context());
        Ok(self.respond(response, context))
    }

    fn status(&mut self, request: &v1::StatusRequest) -> Result<Vec<u8>, Status> {
        self.check_owner(
            request.r#execution_id,
            context_of(request.context()).r#session_id,
        )?;
        let exec = self
            .executions
            .get(&request.r#execution_id)
            .ok_or_else(|| unknown_execution(request.r#execution_id))?;
        let context = context_of(request.context());
        let mut response = v1::StatusResponse {
            r#state: exec.lifecycle.phase().to_wire(),
            r#turn: exec.lifecycle.turn(),
            r#uncertain: exec.lifecycle.disposition().is_uncertain(),
            ..v1::StatusResponse::default()
        };
        if let Some(receipt) = self.receipt(exec.session) {
            response.set_last_mutation(receipt);
        }
        Ok(self.respond(response, context))
    }

    /// Starts cancellation. The answer is `Cancelling` while cleanup runs, or the terminal state when nothing needed
    /// cleaning. Cancelling a terminal or cancelling execution changes nothing and reports its state.
    fn cancel(&mut self, request: &v1::CancelRequest) -> Result<Answer, Status> {
        self.check_owner(
            request.r#execution_id,
            context_of(request.context()).r#session_id,
        )?;
        let id = request.r#execution_id;
        let mut exec = self
            .executions
            .remove(&id)
            .ok_or_else(|| unknown_execution(id))?;
        self.begin_cancel(&mut exec);
        self.advance_cleanup(&mut exec);
        let state = exec.lifecycle.phase().to_wire();
        let uncertain = exec.lifecycle.disposition().is_uncertain();
        self.executions.insert(id, exec);
        let context = context_of(request.context());
        Ok(self.answer(
            v1::CancelResponse {
                state,
                uncertain,
                ..v1::CancelResponse::default()
            },
            context,
        ))
    }

    fn release(&mut self, request: &v1::ReleaseRequest) -> Result<Answer, Status> {
        self.check_owner(
            request.r#execution_id,
            context_of(request.context()).r#session_id,
        )?;
        let exec = self
            .executions
            .get_mut(&request.r#execution_id)
            .ok_or_else(|| unknown_execution(request.r#execution_id))?;
        exec.lifecycle
            .release()
            .map_err(|e| precondition(format!("{e:?}")))?;
        // Disposal transfers reconciliation to the caller; uncertainty is not cleared by freeing the record.
        let uncertain = exec.lifecycle.disposition().is_uncertain();
        self.executions.remove(&request.r#execution_id);
        let context = context_of(request.context());
        Ok(self.answer(
            v1::ReleaseResponse {
                r#released: true,
                uncertain,
                ..v1::ReleaseResponse::default()
            },
            context,
        ))
    }

    /// Stores a verified artifact. A digest already stored is only touched. A full store evicts its least recently used
    /// artifact that no running execution uses, and refuses when every artifact is in use.
    fn store(&mut self, digest: [u8; 32], artifact: Artifact) -> Result<(), Status> {
        self.artifact_clock += 1;
        let clock = self.artifact_clock;
        if let Some(stored) = self.artifacts.get_mut(&digest) {
            stored.last_used = clock;
            return Ok(());
        }
        if self.artifacts.len() >= self.config.max_artifacts {
            let victim = self.evictable().ok_or_else(no_artifact_slot)?;
            self.artifacts.remove(&victim);
        }
        self.artifacts.insert(
            digest,
            Stored {
                artifact: Rc::new(artifact),
                last_used: clock,
            },
        );
        Ok(())
    }

    /// True when `digest` is already stored, the store has a free slot, or a stored artifact can be evicted.
    fn has_artifact_room(&self, digest: [u8; 32]) -> bool {
        self.artifacts.contains_key(&digest)
            || self.artifacts.len() < self.config.max_artifacts
            || self.evictable().is_some()
    }

    /// The least recently used stored artifact that no running execution uses.
    fn evictable(&self) -> Option<[u8; 32]> {
        let running: BTreeSet<[u8; 32]> = self
            .executions
            .values()
            .filter(|exec| exec.machine.is_some())
            .map(|exec| exec.digest)
            .collect();
        self.artifacts
            .iter()
            .filter(|(digest, _)| !running.contains(*digest))
            .min_by_key(|(_, stored)| stored.last_used)
            .map(|(digest, _)| *digest)
    }

    // -- time --------------------------------------------------------------------------------------------------------

    /// Moves the node's clock to `now_ms`, from a monotonic source. Sessions idle for `session_idle_ms` are abandoned:
    /// their uploads are discarded and their executions are cancelled through cleanup. Executions whose lease has passed
    /// are cancelled. A clock that goes back is ignored.
    pub fn tick(&mut self, now_ms: u64) {
        self.now = self.now.max(now_ms);
        for session in self
            .ledger
            .tick(now_ms, u64::from(self.config.session_idle_ms))
        {
            self.abandon(session);
        }
        let expired: Vec<u64> = self
            .executions
            .iter()
            .filter(|(_, exec)| exec.lease.is_some_and(|deadline| self.now >= deadline))
            .map(|(&id, _)| id)
            .collect();
        for id in expired {
            if let Some(mut exec) = self.executions.remove(&id) {
                exec.lease = None;
                self.begin_cancel(&mut exec);
                self.advance_cleanup(&mut exec);
                self.keep(id, exec);
            }
        }
    }

    /// Ends a session: frees its ledger slot, retires its id, and discards any upload it still holds. Every session end
    /// goes through here, so staged bytes never outlive the session that staged them.
    fn retire(&mut self, session: u64) {
        self.uploads.retain(|_, upload| upload.session != session);
        self.ledger.release(session);
    }

    /// Discards what an idle session still holds: its staged upload, and its execution, which is cancelled through
    /// cleanup. A record that ends uncertain is kept as unresolved.
    fn abandon(&mut self, session: u64) {
        self.uploads.retain(|_, upload| upload.session != session);
        let owned: Vec<u64> = self
            .executions
            .iter()
            .filter(|(_, exec)| exec.session == session)
            .map(|(&id, _)| id)
            .collect();
        for id in owned {
            if let Some(mut exec) = self.executions.remove(&id) {
                exec.orphaned = true;
                self.begin_cancel(&mut exec);
                self.advance_cleanup(&mut exec);
                self.keep(id, exec);
            }
        }
    }

    /// Native slots in use: one per execution with a native in flight, plus every parked native. A new native starts only
    /// while this is below `max_executions`.
    fn native_slots_in_use(&self) -> usize {
        let in_flight = self
            .executions
            .values()
            .filter(|exec| {
                matches!(exec.pending, Some(Pending::Native(_)))
                    || exec
                        .cleanup
                        .as_ref()
                        .is_some_and(|cleanup| cleanup.native.is_some())
            })
            .count();
        self.parked_natives.len() + in_flight
    }

    /// Puts a record back, unless it is an orphan that ended with every effect known.
    fn keep(&mut self, id: u64, exec: Exec) {
        let finished = exec.lifecycle.phase().is_terminal() && exec.cleanup.is_none();
        if exec.orphaned && finished && !exec.lifecycle.disposition().is_uncertain() {
            return;
        }
        self.executions.insert(id, exec);
    }

    // -- scheduling --------------------------------------------------------------------------------------------------

    /// Runs one bounded round: one fuel slice for each runnable execution, one step of each native or child operation in
    /// progress, and one round of any simulated child. Control calls are answered by [`Node::handle`] in between.
    pub fn poll(&mut self) {
        if let Some(children) = self.children.as_mut() {
            children.drive(1);
        }
        let mut ids: Vec<u64> = self.executions.keys().copied().collect();
        let split = ids.partition_point(|&id| id < self.cursor);
        ids.rotate_left(split);
        for id in ids {
            if let Some(mut exec) = self.executions.remove(&id) {
                self.maintain_children(&mut exec);
                self.step(&mut exec);
                self.keep(id, exec);
            }
        }
        self.parked_natives
            .retain(|&ticket| self.platform.poll(ticket).is_pending());
        self.cursor = self.cursor.wrapping_add(1);
    }

    /// Runs `rounds` rounds of [`Node::poll`] at the current clock. A convenience for simulations.
    pub fn drive(&mut self, rounds: u32) {
        for _ in 0..rounds {
            self.poll();
        }
    }

    /// Refreshes owned children independently of the parent's current VM instruction. A missed idle deadline is
    /// uncertainty, not proof that the child stopped. Service depends on the embedding polling and the link progressing.
    fn maintain_children(&mut self, exec: &mut Exec) {
        if exec.cleanup.is_some() || exec.orphaned || exec.lifecycle.phase().is_terminal() {
            return;
        }
        let Some(children) = self.children.as_mut() else {
            return;
        };
        for (index, task) in exec.tasks.iter_mut().enumerate() {
            if let Some(ticket) = task.heartbeat {
                match children.poll(ticket, self.now) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(ChildReply::Observed { disposition, .. })) => {
                        task.heartbeat = None;
                        exec.lifecycle.absorb(disposition);
                    }
                    Poll::Ready(_) => {
                        task.heartbeat = None;
                        task.state = TaskState::NeedsCleanup;
                        exec.lifecycle.absorb(Disposition::Uncertain);
                    }
                }
            }
            if task.heartbeat.is_some()
                || !matches!(
                    task.state,
                    TaskState::Ready
                        | TaskState::Running
                        | TaskState::Yielded
                        | TaskState::Terminal
                )
            {
                continue;
            }
            if matches!(exec.pending, Some(Pending::Spawn(handle) | Pending::Await(handle)) if handle == index)
            {
                continue;
            }
            let Some((idle_ms, last_contact)) = children.liveness(task.id) else {
                continue;
            };
            let idle = u64::from(idle_ms);
            if self.now >= last_contact.saturating_add(idle) {
                task.state = TaskState::NeedsCleanup;
                exec.lifecycle.absorb(Disposition::Uncertain);
            } else if self.now >= last_contact.saturating_add((idle / 3).max(1))
                && let Ok(ticket) = children.submit(
                    ChildOp::KeepAlive { child: task.id },
                    self.now
                        .saturating_add(u64::from(self.config.call_timeout_ms))
                        .min(last_contact.saturating_add(idle)),
                )
            {
                task.heartbeat = Some(ticket);
            }
        }
    }

    fn stop_heartbeats(&mut self, exec: &mut Exec) {
        if let Some(children) = self.children.as_mut() {
            for task in &mut exec.tasks {
                if let Some(ticket) = task.heartbeat.take() {
                    children.abandon(ticket);
                }
            }
        }
    }

    fn step(&mut self, exec: &mut Exec) {
        if exec.cleanup.is_some() {
            self.advance_cleanup(exec);
            return;
        }
        if exec.pending.is_some() {
            self.advance_pending(exec);
            return;
        }
        match exec.lifecycle.phase() {
            Phase::Running => {}
            Phase::FuelSuspended => {
                if exec.lifecycle.observe(Event::Resumed).is_err() {
                    return;
                }
            }
            _ => return,
        }
        let fuel = self.config.slice_fuel;
        let Some(machine) = exec.machine.as_mut() else {
            return;
        };
        match machine.run(fuel) {
            Step::Exhausted => {
                let _ = exec.lifecycle.observe(Event::FuelSuspended);
            }
            Step::Yielded(tuple) => {
                // A yield keeps the children: the execution still owns them until it resumes, is cancelled or ends.
                let _ = exec.lifecycle.observe(Event::Yielded(tuple));
            }
            Step::Returned(tuple) => self.exit(exec, Event::Returned(tuple)),
            Step::Failed { code, message } => {
                self.exit(exec, Event::Failed(Failure::new(code, &message)))
            }
            Step::BudgetExhausted => self.exit(
                exec,
                Event::Failed(Failure::new(
                    FailureCode::BudgetExhausted,
                    b"total instruction budget exhausted",
                )),
            ),
            Step::Pending(effect) => self.begin_effect(exec, effect),
        }
    }

    /// The entry ended. Children it still owns are cleaned up before the outcome is recorded.
    fn exit(&mut self, exec: &mut Exec, event: Event) {
        self.stop_heartbeats(exec);
        exec.machine = None;
        if exec.tasks.iter().all(|task| settled(task.state)) {
            let _ = exec.lifecycle.observe(event);
            return;
        }
        let _ = exec.lifecycle.observe(Event::NativeWait);
        exec.cleanup = Some(Cleanup {
            reason: Reason::Exit(event),
            deadline: self.now + u64::from(self.config.cleanup_ms),
            native: None,
        });
        self.advance_cleanup(exec);
    }

    fn begin_effect(&mut self, exec: &mut Exec, effect: Effect) {
        match effect {
            Effect::Native { index, arguments } => {
                let Some(native) = exec
                    .machine
                    .as_ref()
                    .and_then(|m| m.artifact().natives.get(usize::from(index)).cloned())
                else {
                    complete(exec, EffectResult::Raise(b"native out of range".to_vec()));
                    return;
                };
                let call = NativeCall {
                    module: native.module,
                    operation: native.operation,
                    arguments,
                    results: native.results,
                };
                if self.native_slots_in_use() >= self.config.max_executions {
                    complete(
                        exec,
                        EffectResult::Raise(
                            b"no native slot is free: unresolved natives hold them".to_vec(),
                        ),
                    );
                    return;
                }
                match self.platform.start(call) {
                    Ok(ticket) => {
                        exec.pending = Some(Pending::Native(ticket));
                        let _ = exec.lifecycle.observe(Event::NativeWait);
                        self.advance_pending(exec);
                    }
                    Err(message) => complete(exec, EffectResult::Raise(message.into_bytes())),
                }
            }
            Effect::Spawn {
                child,
                captures,
                arguments,
            } => self.spawn(exec, child, captures, arguments),
            Effect::Await { task } => {
                let handle = task as usize;
                if handle >= exec.tasks.len() {
                    complete(exec, EffectResult::Raise(b"unknown child handle".to_vec()));
                    return;
                }
                exec.pending = Some(Pending::Await(handle));
                let _ = exec.lifecycle.observe(Event::NativeWait);
                self.advance_pending(exec);
            }
        }
    }

    /// Submits the spawn of edge `edge` of the running artifact.
    fn spawn(
        &mut self,
        exec: &mut Exec,
        edge: u16,
        captures: Vec<protolink_lua::value::Value>,
        arguments: Vec<protolink_lua::value::Value>,
    ) {
        let Some(link) = exec
            .machine
            .as_ref()
            .and_then(|machine| machine.artifact().children.get(usize::from(edge)))
            .cloned()
        else {
            complete(
                exec,
                EffectResult::Raise(b"no child is linked to this site".to_vec()),
            );
            return;
        };
        let Some(route) = self.config.route(link.slot).copied() else {
            complete(
                exec,
                EffectResult::Raise(b"the child slot has no route".to_vec()),
            );
            return;
        };
        if exec.tasks.len() >= self.config.max_child_tasks {
            complete(
                exec,
                EffectResult::Raise(
                    format!(
                        "an execution may own at most {} child tasks",
                        self.config.max_child_tasks
                    )
                    .into_bytes(),
                ),
            );
            return;
        }
        let wire = self.config.vm.wire;
        let captures = match Tuple::new(captures, wire) {
            Ok(tuple) => tuple,
            Err(e) => return complete(exec, EffectResult::Raise(e.to_string().into_bytes())),
        };
        let size = captures.payload_bytes();
        if size > route.bounds.max_capture_bytes {
            complete(
                exec,
                EffectResult::Raise(
                    format!(
                        "spawn captures {size} bytes; the child slot allows {}",
                        route.bounds.max_capture_bytes
                    )
                    .into_bytes(),
                ),
            );
            return;
        }
        let arguments = match Tuple::new(arguments, wire) {
            Ok(tuple) => tuple,
            Err(e) => return complete(exec, EffectResult::Raise(e.to_string().into_bytes())),
        };
        let Some(children) = self.children.as_mut() else {
            complete(
                exec,
                EffectResult::Raise(b"no child client is configured".to_vec()),
            );
            return;
        };
        let id = ChildId(self.next_child);
        self.next_child += 1;
        let op = ChildOp::Spawn {
            child: id,
            slot: link.slot,
            artifact: link.bytes.clone(),
            digest: link.digest,
            entry: link.entry,
            captures,
            arguments,
            lease_ms: 0,
        };
        match children.submit(op, self.now + u64::from(self.config.call_timeout_ms)) {
            Ok(ticket) => {
                exec.tasks.push(Task {
                    id,
                    slot: link.slot,
                    state: TaskState::Spawning(ticket),
                    settled: None,
                    failure: None,
                    heartbeat: None,
                });
                exec.pending = Some(Pending::Spawn(exec.tasks.len() - 1));
                let _ = exec.lifecycle.observe(Event::NativeWait);
                self.advance_pending(exec);
            }
            Err(error) => complete(exec, EffectResult::Raise(child_message(&error))),
        }
    }

    fn advance_pending(&mut self, exec: &mut Exec) {
        match exec.pending {
            Some(Pending::Native(ticket)) => match self.platform.poll(ticket) {
                Poll::Pending => {}
                Poll::Ready(result) => {
                    exec.pending = None;
                    let _ = exec.lifecycle.observe(Event::Resumed);
                    complete(
                        exec,
                        match result {
                            Ok(Some(value)) => EffectResult::Value(value),
                            Ok(None) => EffectResult::Nothing,
                            Err(message) => EffectResult::Raise(message.into_bytes()),
                        },
                    );
                }
            },
            Some(Pending::Spawn(handle)) => {
                self.advance_task(exec, handle, Goal::Await);
                let task = &exec.tasks[handle];
                if !matches!(task.state, TaskState::Spawning(_)) {
                    exec.pending = None;
                    let _ = exec.lifecycle.observe(Event::Resumed);
                    let result = match &task.failure {
                        Some(message) if task.state != TaskState::Ready => {
                            EffectResult::Raise(message.clone())
                        }
                        _ => EffectResult::Task(handle as u32),
                    };
                    exec.tasks[handle].failure = None;
                    complete(exec, result);
                }
            }
            Some(Pending::Await(handle)) => {
                self.advance_task(exec, handle, Goal::Await);
                if let Some(result) = self.await_result(exec, handle) {
                    exec.pending = None;
                    let _ = exec.lifecycle.observe(Event::Resumed);
                    complete(exec, result);
                }
            }
            None => {}
        }
    }

    /// The result an await delivers now, if it can deliver one.
    fn await_result(&mut self, exec: &mut Exec, handle: usize) -> Option<EffectResult> {
        let task = &mut exec.tasks[handle];
        if let Some(message) = task.failure.take() {
            return Some(EffectResult::Raise(message));
        }
        match task.state {
            TaskState::Done | TaskState::Yielded => {}
            TaskState::Lost | TaskState::NeedsCleanup => {
                return Some(EffectResult::Raise(
                    b"the child's outcome is unknown".to_vec(),
                ));
            }
            _ => return None,
        }
        let observation = task.settled.clone()?;
        Some(match observation.outcome {
            Outcome::Returned(tuple) => EffectResult::Tuple(tuple),
            Outcome::Failed(failure) => EffectResult::Raise(failure.message),
            Outcome::Yielded(_) => {
                EffectResult::Raise(b"child yielded where a return was awaited".to_vec())
            }
        })
    }

    /// Makes one step of task `handle` towards `goal`: one operation is submitted or polled.
    fn advance_task(&mut self, exec: &mut Exec, handle: usize, goal: Goal) {
        let now = self.now;
        let deadline = now + u64::from(self.config.call_timeout_ms);
        let bounds = exec
            .tasks
            .get(handle)
            .and_then(|task| self.config.route(task.slot))
            .map(|route| route.bounds);
        let Some(children) = self.children.as_mut() else {
            return;
        };
        let Some(task) = exec.tasks.get_mut(handle) else {
            return;
        };
        let id = task.id;
        let mut disposition = Disposition::Certain;
        match task.state {
            TaskState::Spawning(ticket) => match children.poll(ticket, now) {
                Poll::Pending => {}
                Poll::Ready(Ok(_)) => task.state = TaskState::Ready,
                Poll::Ready(Err(ChildError::Unknown)) => {
                    // The child may exist, but its execution id was never learned. Its node cancels it when its session
                    // goes idle.
                    task.state = TaskState::Lost;
                    children.lost(task.id, now);
                    task.failure = Some(b"the spawn's outcome is unknown".to_vec());
                    disposition = Disposition::Uncertain;
                }
                Poll::Ready(Err(error)) => {
                    task.state = TaskState::Done;
                    task.failure = Some(child_message(&error));
                }
            },
            TaskState::Ready
            | TaskState::Running
            | TaskState::Yielded
            | TaskState::NeedsCleanup
                if goal == Goal::Cleanup =>
            {
                if let Ok(ticket) = children.submit(ChildOp::Cancel { child: id }, deadline) {
                    task.state = TaskState::Cancelling(ticket);
                }
            }
            TaskState::Ready => {
                let resume = ChildOp::Resume {
                    child: id,
                    turn: 0,
                    values: Tuple::empty(),
                };
                if let Ok(ticket) = children.submit(resume, deadline) {
                    task.state = TaskState::Resuming(ticket);
                }
            }
            TaskState::Resuming(ticket) => match children.poll(ticket, now) {
                Poll::Pending => {}
                Poll::Ready(Ok(_)) => task.state = TaskState::Running,
                Poll::Ready(Err(ChildError::Unknown)) => {
                    // A different mutation cannot use this sequence until a matching receipt resolves the Resume.
                    task.state = TaskState::ReconcileResume;
                }
                Poll::Ready(Err(error)) => {
                    task.state = TaskState::Ready;
                    task.failure = Some(child_message(&error));
                }
            },
            TaskState::ReconcileResume => {
                let query = if goal == Goal::Cleanup {
                    ChildOp::CleanupQuery { child: id, turn: 0 }
                } else {
                    ChildOp::Query { child: id, turn: 0 }
                };
                if let Ok(ticket) = children.submit(query, deadline) {
                    task.state = TaskState::ReconcilingResume(ticket);
                }
            }
            TaskState::ReconcilingResume(ticket) => match children.poll(ticket, now) {
                Poll::Pending => {}
                Poll::Ready(Ok(ChildReply::Observed {
                    phase,
                    observation,
                    disposition: seen,
                })) => {
                    disposition = seen;
                    if let Some(observation) = observation {
                        let observation = bound_result(observation, bounds);
                        task.state = match observation.outcome {
                            Outcome::Yielded(_) => TaskState::Yielded,
                            _ => TaskState::Terminal,
                        };
                        task.settled = Some(observation);
                    } else if matches!(phase, Phase::Returned | Phase::Failed) {
                        task.state = TaskState::Terminal;
                    } else {
                        task.state = TaskState::Running;
                    }
                }
                Poll::Ready(_) => {
                    task.state = TaskState::ReconcileResume;
                    disposition = Disposition::Uncertain;
                    if goal == Goal::Await {
                        task.failure = Some(b"the child's resume could not be reconciled".to_vec());
                    }
                }
            },
            TaskState::Running => {
                if let Ok(ticket) = children.submit(ChildOp::Query { child: id, turn: 0 }, deadline)
                {
                    task.state = TaskState::Querying(ticket);
                }
            }
            TaskState::Querying(ticket) => match children.poll(ticket, now) {
                Poll::Pending => {}
                Poll::Ready(Ok(ChildReply::Observed {
                    observation,
                    disposition: seen,
                    ..
                })) => {
                    disposition = seen;
                    match observation {
                        None => task.state = TaskState::Running,
                        Some(observation) => {
                            let observation = bound_result(observation, bounds);
                            task.state = match observation.outcome {
                                Outcome::Yielded(_) => TaskState::Yielded,
                                _ => TaskState::Terminal,
                            };
                            task.settled = Some(observation);
                        }
                    }
                }
                Poll::Ready(Ok(_)) => task.state = TaskState::Running,
                Poll::Ready(Err(ChildError::Busy)) => task.state = TaskState::Running,
                Poll::Ready(Err(_)) => {
                    // A read failure does not prove a known child stopped. Keep its identity for bounded cleanup.
                    task.state = TaskState::NeedsCleanup;
                    task.failure = Some(b"the child's outcome could not be read".to_vec());
                    disposition = Disposition::Uncertain;
                }
            },
            TaskState::Terminal => {
                if let Ok(ticket) = children.submit(ChildOp::Release { child: id }, deadline) {
                    task.state = TaskState::Releasing(ticket);
                }
            }
            TaskState::Releasing(ticket) => match children.poll(ticket, now) {
                Poll::Pending => {}
                Poll::Ready(Ok(ChildReply::Released)) => {
                    task.state = TaskState::Done;
                    children.forget(id);
                }
                Poll::Ready(_) => {
                    // A lost or uncertain release cannot prove the final disposition or disposal.
                    task.state = TaskState::Lost;
                    children.lost(id, now);
                    disposition = Disposition::Uncertain;
                }
            },
            TaskState::Cancelling(ticket) => match children.poll(ticket, now) {
                Poll::Pending => {}
                Poll::Ready(Ok(ChildReply::Cancelling {
                    phase,
                    disposition: seen,
                })) => {
                    disposition = seen;
                    task.state = if phase.is_terminal() {
                        TaskState::Terminal
                    } else {
                        TaskState::CancelWait
                    };
                }
                Poll::Ready(Ok(_)) => {
                    task.state = TaskState::CancelWait;
                    disposition = Disposition::Uncertain;
                }
                Poll::Ready(Err(ChildError::Unknown | ChildError::UnknownChild)) => {
                    // Cancel may have run. A receipt-bearing read must settle its sequence before another mutation.
                    task.state = TaskState::CancelWait;
                    disposition = Disposition::Uncertain;
                }
                Poll::Ready(Err(ChildError::Quarantined)) => {
                    // The client has given the child up, so no further Cancel or read can be sent. Its outcome stays uncertain.
                    task.state = TaskState::Lost;
                    task.failure = Some(child_message(&ChildError::Quarantined));
                    disposition = Disposition::Uncertain;
                }
                Poll::Ready(Err(
                    ChildError::Busy | ChildError::Unavailable | ChildError::Rejected { .. },
                )) => {
                    // These errors prove this attempt did not run; cleanup may retry the same Cancel.
                    task.state = TaskState::NeedsCleanup;
                }
            },
            TaskState::CancelWait => {
                if let Ok(ticket) =
                    children.submit(ChildOp::CleanupQuery { child: id, turn: 0 }, deadline)
                {
                    task.state = TaskState::Checking(ticket);
                }
            }
            TaskState::Checking(ticket) => match children.poll(ticket, now) {
                Poll::Pending => {}
                Poll::Ready(Ok(ChildReply::Observed {
                    phase,
                    observation,
                    disposition: seen,
                })) => {
                    disposition = seen;
                    if let Some(observation) = observation {
                        task.settled.get_or_insert(observation);
                    }
                    task.state = if matches!(phase, Phase::Returned | Phase::Failed) {
                        TaskState::Terminal
                    } else {
                        if phase == Phase::Released {
                            disposition = Disposition::Uncertain;
                        }
                        TaskState::CancelWait
                    };
                }
                Poll::Ready(_) => {
                    // A failed reconciliation or observation does not prove either admission or termination.
                    task.state = TaskState::CancelWait;
                    disposition = Disposition::Uncertain;
                }
            },
            TaskState::Yielded | TaskState::NeedsCleanup | TaskState::Done | TaskState::Lost => {}
        }
        exec.lifecycle.absorb(disposition);
    }

    /// Starts cancellation of a live execution: the machine stops, a pending native is asked to stop, and cleanup of
    /// the children begins. A terminal or already cancelling execution is left as it is.
    fn begin_cancel(&mut self, exec: &mut Exec) {
        let phase = exec.lifecycle.phase();
        if phase.is_terminal() || phase == Phase::Cancelling {
            return;
        }
        if let Some(Cleanup {
            reason: Reason::Exit(_),
            ..
        }) = exec.cleanup
        {
            // The entry already ended and its children are being settled; that cleanup finishes the record.
            return;
        }
        self.stop_heartbeats(exec);
        let _ = exec.lifecycle.cancel();
        exec.machine = None;
        // A pending spawn or await is settled by the children's cleanup; a pending native is asked to stop.
        let mut native = None;
        if let Some(Pending::Native(ticket)) = exec.pending.take()
            && self.platform.cancel(ticket) == NativeStop::Running
        {
            native = Some(ticket);
        }
        exec.cleanup = Some(Cleanup {
            reason: Reason::Cancel,
            deadline: self.now + u64::from(self.config.cleanup_ms),
            native,
        });
    }

    /// Makes one round of cleanup and finishes the record when every child is settled, or when the deadline passes,
    /// which leaves it uncertain.
    fn advance_cleanup(&mut self, exec: &mut Exec) {
        let Some(cleanup) = exec.cleanup.as_mut() else {
            return;
        };
        if let Some(ticket) = cleanup.native
            && self.platform.poll(ticket).is_ready()
        {
            cleanup.native = None;
        }
        let expired = self.now >= cleanup.deadline;
        for handle in 0..exec.tasks.len() {
            if !settled(exec.tasks[handle].state) {
                self.advance_task(exec, handle, Goal::Cleanup);
            }
        }
        let Some(cleanup) = exec.cleanup.as_mut() else {
            return;
        };
        let native_open = cleanup.native.is_some();
        let children_open = exec.tasks.iter().any(|task| !settled(task.state));
        if (native_open || children_open) && !expired {
            return;
        }
        if native_open || children_open {
            // The deadline passed. Nothing more can be proved, so whatever is still open stays unknown.
            exec.lifecycle.absorb(Disposition::Uncertain);
            if let Some(ticket) = cleanup.native.take() {
                // The native's slot was taken when it started, so parking it keeps the count within bounds.
                self.parked_natives.push(ticket);
            }
            if let Some(children) = self.children.as_mut() {
                for task in &mut exec.tasks {
                    if !settled(task.state) {
                        if let Some(ticket) = ticket_of(task.state) {
                            children.abandon(ticket);
                        }
                        task.state = TaskState::Lost;
                        children.lost(task.id, self.now);
                    }
                }
            }
        }
        if exec.tasks.iter().any(|task| task.state == TaskState::Lost) {
            exec.lifecycle.absorb(Disposition::Uncertain);
        }
        let Some(cleanup) = exec.cleanup.take() else {
            return;
        };
        let disposition = exec.lifecycle.disposition();
        match cleanup.reason {
            Reason::Exit(event) => {
                let _ = exec.lifecycle.observe(Event::Resumed);
                let _ = exec.lifecycle.observe(event);
            }
            Reason::Cancel => {
                let _ = exec.lifecycle.finish_cancel(disposition);
            }
        }
    }
}

/// What a task step works towards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Goal {
    /// The outcome, for an await.
    Await,
    /// A stopped, released child, for cleanup.
    Cleanup,
}

/// True when nothing more needs doing for a task.
fn settled(state: TaskState) -> bool {
    matches!(state, TaskState::Done | TaskState::Lost)
}

fn ticket_of(state: TaskState) -> Option<Ticket> {
    match state {
        TaskState::Spawning(t)
        | TaskState::Resuming(t)
        | TaskState::ReconcilingResume(t)
        | TaskState::Querying(t)
        | TaskState::Releasing(t)
        | TaskState::Cancelling(t)
        | TaskState::Checking(t) => Some(t),
        _ => None,
    }
}

fn complete(exec: &mut Exec, result: EffectResult) {
    if let Some(machine) = exec.machine.as_mut() {
        let _ = machine.complete(result);
    }
}

fn child_message(error: &ChildError) -> Vec<u8> {
    match error {
        ChildError::Busy => b"the child client has no room for another operation".to_vec(),
        ChildError::Unavailable => b"the child is unavailable; nothing ran".to_vec(),
        ChildError::Rejected { message } => message.clone(),
        ChildError::Unknown => b"the child operation's outcome is unknown".to_vec(),
        ChildError::UnknownChild => b"the child is not known".to_vec(),
        ChildError::Quarantined => {
            b"the child is quarantined: its outcome is uncertain, and it is not called again"
                .to_vec()
        }
    }
}

/// Holds a child's returned result to the slot's bound. A larger result fails the await, as a runtime error. The
/// disposition is kept.
fn bound_result(
    observation: Observation,
    bounds: Option<crate::config::ChildBounds>,
) -> Observation {
    let (Some(bounds), Outcome::Returned(tuple)) = (bounds, &observation.outcome) else {
        return observation;
    };
    let size = tuple.payload_bytes();
    if size <= bounds.max_result_bytes {
        return observation;
    }
    Observation {
        outcome: Outcome::Failed(Failure::new(
            FailureCode::RuntimeError,
            format!(
                "child result of {size} bytes exceeds the slot's {} bytes",
                bounds.max_result_bytes
            )
            .as_bytes(),
        )),
        disposition: observation.disposition,
    }
}

/// Messages that carry a `Context` envelope.
pub trait Envelope {
    /// The envelope, or the default when absent.
    fn envelope(&self) -> v1::Context;
    /// Sets the envelope.
    fn set_envelope(&mut self, context: v1::Context);
}

macro_rules! envelope {
    ($($message:ty),* $(,)?) => {
        $(
            impl Envelope for $message {
                fn envelope(&self) -> v1::Context {
                    self.context().copied().unwrap_or_default()
                }
                fn set_envelope(&mut self, context: v1::Context) {
                    self.set_context(context);
                }
            }
        )*
    };
}

envelope!(
    v1::BeginUploadRequest,
    v1::PutChunkRequest,
    v1::CommitArtifactRequest,
    v1::StartRequest,
    v1::ResumeRequest,
    v1::QueryOutcomeRequest,
    v1::StatusRequest,
    v1::CancelRequest,
    v1::ReleaseRequest,
    v1::BeginUploadResponse,
    v1::PutChunkResponse,
    v1::CommitArtifactResponse,
    v1::StartResponse,
    v1::ResumeResponse,
    v1::QueryOutcomeResponse,
    v1::StatusResponse,
    v1::CancelResponse,
    v1::ReleaseResponse,
);

/// Calls that name an upload by id. `mutate` checks that the caller's session holds the upload before the call reserves
/// anything.
trait NamesUpload {
    /// The upload the call names, if it names one.
    fn named_upload(&self) -> Option<u64>;
}

impl NamesUpload for v1::PutChunkRequest {
    fn named_upload(&self) -> Option<u64> {
        Some(self.r#upload_id)
    }
}

impl NamesUpload for v1::CommitArtifactRequest {
    fn named_upload(&self) -> Option<u64> {
        Some(self.r#upload_id)
    }
}

macro_rules! names_no_upload {
    ($($message:ty),* $(,)?) => {
        $(
            impl NamesUpload for $message {
                fn named_upload(&self) -> Option<u64> {
                    None
                }
            }
        )*
    };
}

names_no_upload!(
    v1::BeginUploadRequest,
    v1::StartRequest,
    v1::ResumeRequest,
    v1::CancelRequest,
    v1::ReleaseRequest,
);

fn observation_to_wire(observation: &Observation) -> v1::Outcome {
    let mut wire = v1::Outcome {
        r#uncertain: observation.disposition.is_uncertain(),
        ..v1::Outcome::default()
    };
    use v1::Outcome_::Kind;
    wire.r#kind = Some(match &observation.outcome {
        Outcome::Yielded(tuple) => Kind::Yielded(tuple.to_wire()),
        Outcome::Returned(tuple) => Kind::Returned(tuple.to_wire()),
        Outcome::Failed(failure) => Kind::Failed(v1::Failure {
            r#code: failure.code.to_wire(),
            r#message: failure.message.clone(),
        }),
    });
    wire
}
