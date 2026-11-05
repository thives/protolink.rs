//! Regressions for the readiness remediation at the node: dispositions (F01–F03), cleanup and ownership (F01, F18),
//! lost replies and deadlines (F16), replay reservation (F19), ingress caps (F20) and slot routing (F13).

use std::cell::Cell;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::task::Poll;

use super::*;
use crate::{ConfigError, DiscardError, RecoverError};
use protolink_lua::ledger::LedgerConfigError;

/// R08: byte-identical requests to different methods must never replay across methods.
#[test]
fn cancel_and_release_with_the_same_wire_body_conflict() {
    let (mut client, _node, execution) = started(
        "remote:spawn(function() return 1 end)\n",
        agent_node(52, None),
    );
    let mut cancel = v1::CancelRequest {
        r#execution_id: execution,
        ..v1::CancelRequest::default()
    };
    cancel.set_context(wire(client.flow_of(execution).pending()));
    let _: v1::CancelResponse = client.try_raw("Cancel", &cancel).expect("cancel");
    let mut release = v1::ReleaseRequest {
        r#execution_id: execution,
        ..v1::ReleaseRequest::default()
    };
    release.set_context(wire(client.flow_of(execution).pending()));
    assert_eq!(codec::encode(&cancel), codec::encode(&release));
    assert!(
        client
            .try_raw::<_, v1::ReleaseResponse>("Release", &release)
            .is_err()
    );
}

/// R06/R08: reads reveal only the owner's live committed receipt, and cannot consume a flow.
#[test]
fn execution_session_ownership_and_receipt_are_enforced() {
    let artifact = site_zero("remote:spawn(function() return 1 end)\n").sites[0]
        .artifact
        .clone();
    let node = agent_node(52, None);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).unwrap();
    let execution = client.start(digest(&artifact), vec![], vec![]).unwrap();
    let flow = client.flow_of(execution);
    let before = node.borrow().usage().sessions;
    let mut wrong = wire(flow.pending());
    wrong.r#session_id += 100;
    let mut status = v1::StatusRequest {
        r#execution_id: execution,
        ..Default::default()
    };
    status.set_context(wrong);
    assert!(
        client
            .try_raw::<_, v1::StatusResponse>("Status", &status)
            .is_err()
    );
    let mut query = v1::QueryOutcomeRequest {
        r#execution_id: execution,
        ..Default::default()
    };
    query.set_context(wrong);
    assert!(
        client
            .try_raw::<_, v1::QueryOutcomeResponse>("QueryOutcome", &query)
            .is_err()
    );
    let mut resume = v1::ResumeRequest {
        r#execution_id: execution,
        ..Default::default()
    };
    resume.set_context(wrong);
    assert!(
        client
            .try_raw::<_, v1::ResumeResponse>("Resume", &resume)
            .is_err()
    );
    let mut cancel = v1::CancelRequest {
        r#execution_id: execution,
        ..Default::default()
    };
    cancel.set_context(wrong);
    assert!(
        client
            .try_raw::<_, v1::CancelResponse>("Cancel", &cancel)
            .is_err()
    );
    let mut release = v1::ReleaseRequest {
        r#execution_id: execution,
        ..Default::default()
    };
    release.set_context(wrong);
    assert!(
        client
            .try_raw::<_, v1::ReleaseResponse>("Release", &release)
            .is_err()
    );
    assert_eq!(node.borrow().usage().sessions, before);
    let mut second = v1::StartRequest {
        r#digest: digest(&artifact).to_vec(),
        ..Default::default()
    };
    second.set_context(wire(flow.pending()));
    assert!(
        client
            .try_raw::<_, v1::StartResponse>("Start", &second)
            .is_err()
    );
    resume.set_context(wire(flow.pending()));
    let encoded = codec::encode(&resume);
    let _: v1::ResumeResponse = client.try_raw("Resume", &resume).unwrap();
    let status = client.status(execution);
    let receipt = status.last_mutation().unwrap();
    assert_eq!(receipt.r#sequence, flow.pending().sequence);
    assert_eq!(
        receipt.r#fingerprint,
        protolink_lua::ledger::mutation_fingerprint("Resume", &encoded)
    );
    let query = client.query(execution, 0).unwrap();
    assert_eq!(
        query.last_mutation().unwrap().r#fingerprint,
        receipt.r#fingerprint
    );
}

/// R02: release reports the terminal disposition rather than silently discarding it.
#[test]
fn direct_release_exposes_uncertain_terminal_disposition() {
    let dut = dut_node(33);
    let agent = agent_node(32, Some(immediate(MarksUncertain(Loopback::new(dut)))));
    let (mut client, agent, execution) = started(AWAIT_ONE, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(observed.disposition, Disposition::Uncertain);
    let response = client.release(execution).unwrap();
    assert!(response.r#released && response.r#uncertain);
}

#[test]
fn undersized_child_reply_cap_is_refused_before_spawn_admission() {
    use protolink_lua::child::{ChildError, ChildId, ChildOp};
    let mut settings = child_settings();
    settings.max_reply_bytes = protolink_lua::frame::min_reply_bytes(
        settings.wire.max_items(),
        settings.wire.max_total_bytes(),
    ) - 1;
    let mut children = LinkChildren::new(settings, vec![(1, immediate(Unreachable))]);
    let spawn = ChildOp::Spawn {
        child: ChildId(1),
        slot: 1,
        artifact: Rc::from(&b"artifact"[..]),
        digest: [0; 32],
        entry: 0,
        captures: Tuple::empty(),
        arguments: Tuple::empty(),
        lease_ms: 0,
    };
    assert!(matches!(
        children.submit(spawn, 100),
        Err(ChildError::Rejected { .. })
    ));
    assert_eq!(children.known_children(), 0);
}

#[test]
fn child_operation_pool_reserves_heartbeat_and_cleanup_under_data_saturation() {
    use protolink_lua::child::{ChildError, ChildId, ChildOp, ChildReply};
    let artifact = site_zero("remote:spawn(function() return 1 end)\n").sites[0]
        .artifact
        .clone();
    let target = agent_node(52, None);
    let mut settings = child_settings();
    settings.max_ops = 3;
    let mut children = LinkChildren::new(settings, vec![(1, immediate(Loopback::new(target)))]);
    for id in 1..=3 {
        let op = ChildOp::Spawn {
            child: ChildId(id),
            slot: 1,
            artifact: Rc::from(artifact.clone()),
            digest: digest(&artifact),
            entry: 0,
            captures: Tuple::empty(),
            arguments: Tuple::empty(),
            lease_ms: 0,
        };
        let ticket = children.submit(op, 1000).unwrap();
        let mut done = None;
        for _ in 0..500 {
            children.drive(1);
            if let Poll::Ready(result) = children.poll(ticket, 0) {
                done = Some(result);
                break;
            }
        }
        assert!(matches!(done, Some(Ok(ChildReply::Spawned))));
    }
    let _data = children
        .submit(
            ChildOp::Query {
                child: ChildId(1),
                turn: 0,
            },
            1000,
        )
        .unwrap();
    assert!(matches!(
        children.submit(
            ChildOp::Query {
                child: ChildId(2),
                turn: 0
            },
            1000
        ),
        Err(ChildError::Busy)
    ));
    let heartbeat = children
        .submit(ChildOp::KeepAlive { child: ChildId(2) }, 1000)
        .unwrap();
    let cleanup = children
        .submit(ChildOp::Cancel { child: ChildId(3) }, 1000)
        .unwrap();
    assert_eq!(children.operations(), 3);
    assert!(matches!(
        children.poll(cleanup, 0),
        Poll::Ready(Ok(ChildReply::Cancelling { .. }))
    ));
    assert!(matches!(
        children.poll(heartbeat, 0),
        Poll::Ready(Ok(ChildReply::Observed { .. }))
    ));
}

#[test]
fn each_active_route_retains_cleanup_and_heartbeat_capacity() {
    use protolink_lua::child::{ChildId, ChildOp, ChildReply};
    let artifact = site_zero("remote:spawn(function() return 1 end)\n").sites[0]
        .artifact
        .clone();
    let mut settings = child_settings();
    settings.max_ops = 5;
    let mut children = LinkChildren::new(
        settings,
        vec![
            (1, immediate(Loopback::new(agent_node(52, None)))),
            (2, immediate(Loopback::new(agent_node(53, None)))),
        ],
    );
    for id in 1..=5 {
        let slot = if id <= 3 { 1 } else { 2 };
        let ticket = children
            .submit(
                ChildOp::Spawn {
                    child: ChildId(id),
                    slot,
                    artifact: Rc::from(artifact.clone()),
                    digest: digest(&artifact),
                    entry: 0,
                    captures: Tuple::empty(),
                    arguments: Tuple::empty(),
                    lease_ms: 0,
                },
                1000,
            )
            .unwrap();
        let mut done = None;
        for _ in 0..500 {
            children.drive(1);
            if let Poll::Ready(result) = children.poll(ticket, 0) {
                done = Some(result);
                break;
            }
        }
        assert!(matches!(done, Some(Ok(ChildReply::Spawned))));
    }
    let _data = children
        .submit(
            ChildOp::Query {
                child: ChildId(1),
                turn: 0,
            },
            1000,
        )
        .unwrap();
    let heartbeat1 = children
        .submit(ChildOp::KeepAlive { child: ChildId(2) }, 1000)
        .unwrap();
    let cleanup1 = children
        .submit(ChildOp::Cancel { child: ChildId(3) }, 1000)
        .unwrap();
    let heartbeat2 = children
        .submit(ChildOp::KeepAlive { child: ChildId(4) }, 1000)
        .unwrap();
    let cleanup2 = children
        .submit(ChildOp::Cancel { child: ChildId(5) }, 1000)
        .unwrap();
    assert_eq!(children.operations(), 5);
    for ticket in [cleanup1, cleanup2] {
        assert!(matches!(
            children.poll(ticket, 0),
            Poll::Ready(Ok(ChildReply::Cancelling { .. }))
        ));
    }
    for ticket in [heartbeat1, heartbeat2] {
        assert!(matches!(
            children.poll(ticket, 0),
            Poll::Ready(Ok(ChildReply::Observed { .. }))
        ));
    }
}

/// A DUT whose chip.model counts its calls.
fn counting_dut(epoch: u64, calls: Rc<Cell<u32>>) -> Rc<RefCell<Node>> {
    let settings = config(
        "example.fixture.remote",
        contract_digest(DUT_ID),
        Vec::new(),
        epoch,
    );
    build(
        settings,
        Box::new(ImmediateNatives::new(move |_, _, _| {
            calls.set(calls.get() + 1);
            Ok(Some(Value::Boolean(true)))
        })),
        None,
    )
}

/// Sets the uncertain flag on every outcome the child reports, as a child with an unresolved descendant would.
struct MarksUncertain(Loopback);

impl ImmediateCall for MarksUncertain {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        let reply = ImmediateCall::call(&mut self.0, path, request)?;
        if !path.ends_with("QueryOutcome") {
            return Ok(reply);
        }
        let mut response: v1::QueryOutcomeResponse = codec::decode(&reply).expect("reply");
        response.r#uncertain = true;
        if let Some(outcome) = response.mut_outcome() {
            outcome.r#uncertain = true;
        }
        Ok(codec::encode(&response))
    }

    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut self.0, rounds);
    }
}

/// Delivers each named call, but loses the reply of its first delivery.
struct LosesFirstReply {
    inner: Loopback,
    methods: Vec<&'static str>,
    lost: BTreeSet<String>,
    delivered: Rc<RefCell<Vec<String>>>,
}

impl ImmediateCall for LosesFirstReply {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        let method = path.rsplit('/').next().unwrap_or_default().to_string();
        self.delivered.borrow_mut().push(method.clone());
        let reply = ImmediateCall::call(&mut self.inner, path, request);
        if self.methods.contains(&method.as_str()) && self.lost.insert(method) {
            return Err(LinkError::Lost("reply lost after delivery".to_string()));
        }
        reply
    }

    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut self.inner, rounds);
    }
}

/// Delivers a bounded number of calls but loses their replies, then lets the link recover.
struct LosesReplies {
    inner: Loopback,
    method: &'static str,
    remaining: u32,
    delivered: Rc<RefCell<Vec<String>>>,
}
impl ImmediateCall for LosesReplies {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        let method = path.rsplit('/').next().unwrap_or_default();
        self.delivered.borrow_mut().push(method.to_string());
        let reply = ImmediateCall::call(&mut self.inner, path, request);
        if method == self.method && self.remaining > 0 && reply.is_ok() {
            self.remaining -= 1;
            return Err(LinkError::Lost("delivered reply lost".to_string()));
        }
        reply
    }
    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut self.inner, rounds);
    }
}

struct CleanupNative {
    starts: Rc<Cell<u32>>,
    cancels: Rc<Cell<u32>>,
    finish: Rc<Cell<bool>>,
    interruptible: bool,
}
impl Platform for CleanupNative {
    fn start(&mut self, _: crate::NativeCall) -> Result<crate::NativeTicket, String> {
        self.starts.set(self.starts.get() + 1);
        Ok(crate::NativeTicket(1))
    }
    fn poll(&mut self, _: crate::NativeTicket) -> Poll<Result<Option<Value>, String>> {
        if self.finish.get() {
            Poll::Ready(Ok(Some(Value::Boolean(true))))
        } else {
            Poll::Pending
        }
    }
    fn cancel(&mut self, _: crate::NativeTicket) -> crate::NativeStop {
        self.cancels.set(self.cancels.get() + 1);
        if self.interruptible {
            crate::NativeStop::Stopped
        } else {
            crate::NativeStop::Running
        }
    }
}

#[test]
fn failed_outcome_read_keeps_known_child_for_cleanup() {
    let (starts, cancels, finish) = (
        Rc::new(Cell::new(0)),
        Rc::new(Cell::new(0)),
        Rc::new(Cell::new(false)),
    );
    let settings = config(
        "example.fixture.remote",
        contract_digest(DUT_ID),
        Vec::new(),
        61,
    );
    let dut = build(
        settings,
        Box::new(CleanupNative {
            starts: starts.clone(),
            cancels: cancels.clone(),
            finish,
            interruptible: true,
        }),
        None,
    );
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let link = LosesReplies {
        inner: Loopback::new(dut.clone()),
        method: "QueryOutcome",
        remaining: 3,
        delivered: delivered.clone(),
    };
    let agent = agent_node(60, Some(immediate(link)));
    let source = "remote:spawn(function() local ok = pcall(function() return remote:spawn(function() return chip.model() end):await() end) return ok end)\n";
    let (mut client, agent, execution) = started(source, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(returned(observed.clone()), vec![Value::Boolean(false)]);
    assert_eq!(observed.disposition, Disposition::Uncertain);
    assert_eq!(starts.get(), 1);
    assert_eq!(
        cancels.get(),
        1,
        "a failed read must not abandon the known running child"
    );
    assert_eq!(dut.borrow().execution_count(), 0);
    assert!(delivered.borrow().iter().any(|method| method == "Release"));
}

#[test]
fn lost_cancel_reconciles_then_waits_for_a_running_native_before_release() {
    let (starts, cancels, finish) = (
        Rc::new(Cell::new(0)),
        Rc::new(Cell::new(0)),
        Rc::new(Cell::new(false)),
    );
    let settings = config(
        "example.fixture.remote",
        contract_digest(DUT_ID),
        Vec::new(),
        61,
    );
    let dut = build(
        settings,
        Box::new(CleanupNative {
            starts: starts.clone(),
            cancels: cancels.clone(),
            finish: finish.clone(),
            interruptible: false,
        }),
        None,
    );
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let link = LosesReplies {
        inner: Loopback::new(dut.clone()),
        method: "Cancel",
        remaining: 3,
        delivered: delivered.clone(),
    };
    let agent = agent_node(60, Some(immediate(link)));
    let source = "remote:spawn(function() return remote:spawn(function() return chip.model() end):await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    agent.borrow_mut().drive(100);
    assert_eq!(starts.get(), 1);
    client.cancel(execution).unwrap();
    agent.borrow_mut().drive(200);
    assert_eq!(
        client.status(execution).r#state,
        v1::State::Cancelling,
        "the native is still running"
    );
    assert_eq!(
        cancels.get(),
        1,
        "retransmission must not duplicate cancellation"
    );
    assert_eq!(dut.borrow().execution_count(), 1);
    assert!(
        delivered
            .borrow()
            .iter()
            .any(|method| method == "QueryOutcome")
    );
    finish.set(true);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(observed.disposition, Disposition::Uncertain);
    assert!(
        matches!(observed.outcome, Outcome::Failed(ref failure) if failure.code == FailureCode::Cancelled)
    );
    assert_eq!(cancels.get(), 1);
    assert_eq!(dut.borrow().execution_count(), 0);
    assert!(delivered.borrow().iter().any(|method| method == "Release"));
}

#[test]
fn cleanup_retries_lost_cancel_reconciliation_reads_until_the_link_recovers() {
    struct CancelAndReadLoss {
        inner: Loopback,
        cancel_remaining: u32,
        read_remaining: u32,
        cancel_started: bool,
        reads_after_cancel: Rc<Cell<u32>>,
    }
    impl ImmediateCall for CancelAndReadLoss {
        fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
            let reply = ImmediateCall::call(&mut self.inner, path, request);
            if path.ends_with("Cancel") {
                self.cancel_started = true;
                if self.cancel_remaining > 0 && reply.is_ok() {
                    self.cancel_remaining -= 1;
                    return Err(LinkError::Lost("cancel reply lost".to_string()));
                }
            }
            if path.ends_with("QueryOutcome") && self.cancel_started {
                self.reads_after_cancel
                    .set(self.reads_after_cancel.get() + 1);
                if self.read_remaining > 0 && reply.is_ok() {
                    self.read_remaining -= 1;
                    return Err(LinkError::Lost("reconciliation reply lost".to_string()));
                }
            }
            reply
        }
        fn drive(&mut self, rounds: u32) {
            ImmediateCall::drive(&mut self.inner, rounds);
        }
    }
    let (starts, cancels, finish) = (
        Rc::new(Cell::new(0)),
        Rc::new(Cell::new(0)),
        Rc::new(Cell::new(false)),
    );
    let settings = config(
        "example.fixture.remote",
        contract_digest(DUT_ID),
        Vec::new(),
        61,
    );
    let dut = build(
        settings,
        Box::new(CleanupNative {
            starts: starts.clone(),
            cancels: cancels.clone(),
            finish: finish.clone(),
            interruptible: false,
        }),
        None,
    );
    let reads = Rc::new(Cell::new(0));
    let link = CancelAndReadLoss {
        inner: Loopback::new(dut.clone()),
        cancel_remaining: 3,
        read_remaining: 3,
        cancel_started: false,
        reads_after_cancel: reads.clone(),
    };
    let agent = agent_node(60, Some(immediate(link)));
    let source = "remote:spawn(function() return remote:spawn(function() return chip.model() end):await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    agent.borrow_mut().drive(100);
    client.cancel(execution).unwrap();
    agent.borrow_mut().drive(100);
    assert!(
        reads.get() >= 4,
        "cleanup must retry a failed reconciliation read"
    );
    assert_eq!(client.status(execution).r#state, v1::State::Cancelling);
    assert_eq!((starts.get(), cancels.get()), (1, 1));
    finish.set(true);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(observed.disposition, Disposition::Uncertain);
    assert_eq!(dut.borrow().execution_count(), 0);
}

#[test]
fn unmatched_cancel_receipt_never_authorizes_release_before_cleanup_deadline() {
    struct CorruptReceipt(LosesReplies);
    impl ImmediateCall for CorruptReceipt {
        fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
            let reply = ImmediateCall::call(&mut self.0, path, request);
            if path.ends_with("QueryOutcome")
                && let Ok(bytes) = &reply
            {
                let mut response: v1::QueryOutcomeResponse = codec::decode(bytes).unwrap();
                if let Some(receipt) = response.mut_last_mutation() {
                    receipt.r#fingerprint = vec![0; 32];
                }
                return Ok(codec::encode(&response));
            }
            reply
        }
        fn drive(&mut self, rounds: u32) {
            ImmediateCall::drive(&mut self.0, rounds);
        }
    }
    let (starts, cancels, finish) = (
        Rc::new(Cell::new(0)),
        Rc::new(Cell::new(0)),
        Rc::new(Cell::new(false)),
    );
    let settings = config(
        "example.fixture.remote",
        contract_digest(DUT_ID),
        Vec::new(),
        61,
    );
    let dut = build(
        settings,
        Box::new(CleanupNative {
            starts: starts.clone(),
            cancels: cancels.clone(),
            finish,
            interruptible: false,
        }),
        None,
    );
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let link = CorruptReceipt(LosesReplies {
        inner: Loopback::new(dut.clone()),
        method: "Cancel",
        remaining: 3,
        delivered: delivered.clone(),
    });
    let agent = agent_node(60, Some(immediate(link)));
    let source = "remote:spawn(function() return remote:spawn(function() return chip.model() end):await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    agent.borrow_mut().drive(100);
    client.cancel(execution).unwrap();
    agent.borrow_mut().drive(100);
    let status = client.status(execution);
    assert_eq!(status.r#state, v1::State::Cancelling);
    assert!(status.r#uncertain);
    agent.borrow_mut().tick(4_999);
    agent.borrow_mut().drive(20);
    assert_eq!(client.status(execution).r#state, v1::State::Cancelling);
    agent.borrow_mut().tick(5_000);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(observed.disposition, Disposition::Uncertain);
    assert_eq!((starts.get(), cancels.get()), (1, 1));
    assert_eq!(dut.borrow().execution_count(), 1);
    let log = delivered.borrow();
    assert_eq!(log.iter().filter(|method| *method == "Cancel").count(), 3);
    assert!(!log.iter().any(|method| method == "Release"));
}

#[test]
fn abandoning_dispatched_cancel_keeps_its_identity_until_receipt_reconciliation() {
    use protolink_lua::child::{ChildError, ChildId, ChildOp, ChildReply};
    struct HoldCancel {
        inner: ImmediateLink<Loopback>,
        held: bool,
    }
    impl ChildLink for HoldCancel {
        fn send(&mut self, path: &str, request: &[u8]) -> Result<(), LinkError> {
            self.inner.send(path, request)?;
            self.held = path.ends_with("Cancel");
            Ok(())
        }
        fn poll(&mut self) -> Poll<Result<Vec<u8>, LinkError>> {
            if self.held {
                Poll::Pending
            } else {
                self.inner.poll()
            }
        }
        fn abort(&mut self) {
            self.held = false;
            self.inner.abort();
        }
        fn drive(&mut self, rounds: u32) {
            self.inner.drive(rounds);
        }
    }
    let artifact = site_zero("remote:spawn(function() return 1 end)\n").sites[0]
        .artifact
        .clone();
    let target = agent_node(52, None);
    let link = HoldCancel {
        inner: ImmediateLink::new(Loopback::new(target.clone())),
        held: false,
    };
    let mut children = LinkChildren::new(child_settings(), vec![(1, Box::new(link))]);
    let child = ChildId(1);
    let spawn = ChildOp::Spawn {
        child,
        slot: 1,
        artifact: Rc::from(artifact.clone()),
        digest: digest(&artifact),
        entry: 0,
        captures: Tuple::empty(),
        arguments: Tuple::empty(),
        lease_ms: 0,
    };
    let ticket = children.submit(spawn, 1000).unwrap();
    let mut spawned = None;
    for _ in 0..500 {
        children.drive(1);
        if let Poll::Ready(result) = children.poll(ticket, 0) {
            spawned = Some(result);
            break;
        }
    }
    assert!(matches!(spawned, Some(Ok(ChildReply::Spawned))));
    let cancel = children.submit(ChildOp::Cancel { child }, 1000).unwrap();
    assert!(matches!(children.poll(cancel, 0), Poll::Pending));
    children.abandon(cancel);
    assert!(
        matches!(
            children.submit(ChildOp::Release { child }, 1000),
            Err(ChildError::Unknown)
        ),
        "an abandoned dispatched Cancel must retain its pending mutation"
    );
    let query = children
        .submit(ChildOp::CleanupQuery { child, turn: 0 }, 1000)
        .unwrap();
    assert!(matches!(
        children.poll(query, 0),
        Poll::Ready(Ok(ChildReply::Observed { .. }))
    ));
    let release = children.submit(ChildOp::Release { child }, 1000).unwrap();
    assert!(matches!(
        children.poll(release, 0),
        Poll::Ready(Ok(ChildReply::Released))
    ));
    assert_eq!(target.borrow().execution_count(), 0);
}

/// Delivers every Resume but loses every response; reads remain available for receipt reconciliation.
struct LosesResumeReplies(Loopback);

impl ImmediateCall for LosesResumeReplies {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        let reply = ImmediateCall::call(&mut self.0, path, request);
        if path.ends_with("Resume") && reply.is_ok() {
            return Err(LinkError::Lost("resume response lost".to_string()));
        }
        reply
    }
    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut self.0, rounds);
    }
}

#[test]
fn all_resume_replies_lost_reconciles_receipt_before_release() {
    let calls = Rc::new(Cell::new(0));
    let dut = counting_dut(61, calls.clone());
    let agent = agent_node(
        60,
        Some(immediate(LosesResumeReplies(Loopback::new(dut.clone())))),
    );
    let source = "remote:spawn(function() return remote:spawn(function() return chip.model() end):await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(returned(observed.clone()), vec![Value::Boolean(true)]);
    assert_eq!(observed.disposition, Disposition::Certain);
    assert_eq!(calls.get(), 1);
    assert_eq!(dut.borrow().execution_count(), 0);
}

#[test]
fn lost_resume_reconciles_before_cancelling_running_native() {
    struct Interruptible {
        starts: Rc<Cell<u32>>,
        stops: Rc<Cell<u32>>,
    }
    impl Platform for Interruptible {
        fn start(&mut self, _: crate::NativeCall) -> Result<crate::NativeTicket, String> {
            self.starts.set(self.starts.get() + 1);
            Ok(crate::NativeTicket(1))
        }
        fn poll(&mut self, _: crate::NativeTicket) -> Poll<Result<Option<Value>, String>> {
            Poll::Pending
        }
        fn cancel(&mut self, _: crate::NativeTicket) -> crate::NativeStop {
            self.stops.set(self.stops.get() + 1);
            crate::NativeStop::Stopped
        }
    }
    let (starts, stops) = (Rc::new(Cell::new(0)), Rc::new(Cell::new(0)));
    let settings = config(
        "example.fixture.remote",
        contract_digest(DUT_ID),
        Vec::new(),
        61,
    );
    let dut = build(
        settings,
        Box::new(Interruptible {
            starts: starts.clone(),
            stops: stops.clone(),
        }),
        None,
    );
    let agent = agent_node(
        60,
        Some(immediate(LosesResumeReplies(Loopback::new(dut.clone())))),
    );
    let source = "remote:spawn(function() return remote:spawn(function() return chip.model() end):await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    agent.borrow_mut().drive(100);
    assert_eq!(starts.get(), 1);
    client.cancel(execution).unwrap();
    let outcome = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(outcome.disposition, Disposition::Certain);
    assert_eq!(stops.get(), 1);
    assert_eq!(dut.borrow().execution_count(), 0);
}

/// Holds every delivered Resume response until cancellation starts, then loses it.
struct HeldResumeReplies {
    inner: ImmediateLink<Loopback>,
    release: Rc<Cell<bool>>,
    sent: Rc<Cell<u32>>,
    held: bool,
    log: Rc<RefCell<Vec<String>>>,
}
impl ChildLink for HeldResumeReplies {
    fn send(&mut self, path: &str, request: &[u8]) -> Result<(), LinkError> {
        let name = path.rsplit('/').next().unwrap_or_default();
        self.log.borrow_mut().push(name.to_string());
        self.inner.send(path, request)?;
        if name == "Resume" {
            self.sent.set(self.sent.get() + 1);
            self.held = true;
        }
        Ok(())
    }
    fn poll(&mut self) -> Poll<Result<Vec<u8>, LinkError>> {
        if self.held {
            if !self.release.get() {
                return Poll::Pending;
            }
            self.held = false;
            let _ = self.inner.poll();
            return Poll::Ready(Err(LinkError::Lost(
                "delivered Resume reply lost".to_string(),
            )));
        }
        self.inner.poll()
    }
    fn abort(&mut self) {
        self.held = false;
        self.inner.abort();
    }
    fn drive(&mut self, rounds: u32) {
        self.inner.drive(rounds);
    }
}

#[test]
fn cleanup_reconciles_a_resume_that_becomes_unknown_after_parent_cancel() {
    struct Interruptible {
        starts: Rc<Cell<u32>>,
        stops: Rc<Cell<u32>>,
    }
    impl Platform for Interruptible {
        fn start(&mut self, _: crate::NativeCall) -> Result<crate::NativeTicket, String> {
            self.starts.set(self.starts.get() + 1);
            Ok(crate::NativeTicket(1))
        }
        fn poll(&mut self, _: crate::NativeTicket) -> Poll<Result<Option<Value>, String>> {
            Poll::Pending
        }
        fn cancel(&mut self, _: crate::NativeTicket) -> crate::NativeStop {
            self.stops.set(self.stops.get() + 1);
            crate::NativeStop::Stopped
        }
    }
    let (starts, stops) = (Rc::new(Cell::new(0)), Rc::new(Cell::new(0)));
    let settings = config(
        "example.fixture.remote",
        contract_digest(DUT_ID),
        Vec::new(),
        61,
    );
    let dut = build(
        settings,
        Box::new(Interruptible {
            starts: starts.clone(),
            stops: stops.clone(),
        }),
        None,
    );
    let (release, sent, log) = (
        Rc::new(Cell::new(false)),
        Rc::new(Cell::new(0)),
        Rc::new(RefCell::new(Vec::new())),
    );
    let link = HeldResumeReplies {
        inner: ImmediateLink::new(Loopback::new(dut.clone())),
        release: release.clone(),
        sent: sent.clone(),
        held: false,
        log: log.clone(),
    };
    let agent = agent_node(60, Some(Box::new(link)));
    let source = "remote:spawn(function() return remote:spawn(function() return chip.model() end):await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    agent.borrow_mut().drive(100);
    assert_eq!(sent.get(), 1);
    assert_eq!(starts.get(), 1);
    client.cancel(execution).unwrap();
    assert_eq!(client.status(execution).r#state, v1::State::Cancelling);
    release.set(true);
    agent.borrow_mut().drive(300);
    assert_eq!(
        stops.get(),
        1,
        "cleanup must reconcile before cancelling the native"
    );
    assert_eq!(
        starts.get(),
        1,
        "Resume replay must not duplicate the native effect"
    );
    assert_eq!(dut.borrow().execution_count(), 0);
    let log = log.borrow();
    let query = log
        .iter()
        .position(|method| method == "QueryOutcome")
        .expect("receipt query");
    let cancel = log
        .iter()
        .position(|method| method == "Cancel")
        .expect("child cancel");
    assert!(query < cancel);
}

struct LostResumeWrongReceipt {
    inner: Loopback,
    cleanup_mutations: Rc<Cell<u32>>,
}
impl ImmediateCall for LostResumeWrongReceipt {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        if path.ends_with("Cancel") || path.ends_with("Release") {
            self.cleanup_mutations.set(self.cleanup_mutations.get() + 1);
        }
        let reply = ImmediateCall::call(&mut self.inner, path, request);
        if path.ends_with("Resume") && reply.is_ok() {
            return Err(LinkError::Lost("resume response lost".to_string()));
        }
        if path.ends_with("QueryOutcome") {
            let mut response: v1::QueryOutcomeResponse =
                codec::decode(reply.as_ref().unwrap()).unwrap();
            if let Some(receipt) = response.mut_last_mutation() {
                receipt.r#fingerprint = vec![0; 32];
            }
            return Ok(codec::encode(&response));
        }
        reply
    }
    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut self.inner, rounds);
    }
}

#[test]
fn mismatched_resume_receipt_never_advances_to_release() {
    let calls = Rc::new(Cell::new(0));
    let dut = counting_dut(61, calls.clone());
    let cleanup_mutations = Rc::new(Cell::new(0));
    let agent = agent_node(
        60,
        Some(immediate(LostResumeWrongReceipt {
            inner: Loopback::new(dut.clone()),
            cleanup_mutations: cleanup_mutations.clone(),
        })),
    );
    let source = "remote:spawn(function() local ok = pcall(function() return remote:spawn(function() return chip.model() end):await() end) return ok end)\n";
    let (mut client, agent, execution) = started(source, agent);
    agent.borrow_mut().drive(100);
    assert!(client.status(execution).r#uncertain);
    assert_eq!(
        dut.borrow().execution_count(),
        1,
        "a mismatched receipt cannot authorize cleanup mutations"
    );
    agent.borrow_mut().tick(5_000);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(observed.disposition, Disposition::Uncertain);
    assert_eq!(calls.get(), 1);
    assert_eq!(dut.borrow().execution_count(), 1);
    assert_eq!(
        cleanup_mutations.get(),
        0,
        "no Cancel or Release may reuse the unresolved sequence"
    );
}

#[test]
fn reconciliation_recovers_during_cleanup_without_clearing_uncertainty() {
    struct WrongReceiptOnce {
        inner: Loopback,
        corrupted: bool,
    }
    impl ImmediateCall for WrongReceiptOnce {
        fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
            let reply = ImmediateCall::call(&mut self.inner, path, request);
            if path.ends_with("Resume") && reply.is_ok() {
                return Err(LinkError::Lost("resume response lost".to_string()));
            }
            if path.ends_with("QueryOutcome") && !self.corrupted {
                let mut response: v1::QueryOutcomeResponse =
                    codec::decode(reply.as_ref().unwrap()).unwrap();
                response.mut_last_mutation().unwrap().r#fingerprint = vec![0; 32];
                self.corrupted = true;
                return Ok(codec::encode(&response));
            }
            reply
        }
        fn drive(&mut self, rounds: u32) {
            ImmediateCall::drive(&mut self.inner, rounds);
        }
    }
    let calls = Rc::new(Cell::new(0));
    let dut = counting_dut(61, calls.clone());
    let agent = agent_node(
        60,
        Some(immediate(WrongReceiptOnce {
            inner: Loopback::new(dut.clone()),
            corrupted: false,
        })),
    );
    let source = "remote:spawn(function() local ok = pcall(function() return remote:spawn(function() return chip.model() end):await() end) return ok end)\n";
    let (mut client, agent, execution) = started(source, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(returned(observed.clone()), vec![Value::Boolean(false)]);
    assert_eq!(observed.disposition, Disposition::Uncertain);
    assert_eq!(calls.get(), 1);
    assert_eq!(dut.borrow().execution_count(), 0);
}

struct MonitorHeartbeats {
    inner: Loopback,
    statuses: Rc<RefCell<Vec<u64>>>,
    resumes: Rc<Cell<u32>>,
}
impl ImmediateCall for MonitorHeartbeats {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        if path.ends_with("Status") {
            let status: v1::StatusRequest = codec::decode(request).unwrap();
            self.statuses.borrow_mut().push(status.r#execution_id);
        }
        if path.ends_with("Resume") {
            self.resumes.set(self.resumes.get() + 1);
        }
        ImmediateCall::call(&mut self.inner, path, request)
    }
    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut self.inner, rounds);
    }
}

/// R14: a contacted yielded parent keeps its Ready child alive without resuming it.
#[test]
fn contacted_yielded_parent_keeps_ready_child_alive() {
    let dut = dut_node(61);
    let statuses = Rc::new(RefCell::new(Vec::new()));
    let resumes = Rc::new(Cell::new(0));
    let link = MonitorHeartbeats {
        inner: Loopback::new(dut.clone()),
        statuses: statuses.clone(),
        resumes: resumes.clone(),
    };
    let agent = agent_node(60, Some(immediate(link)));
    let source = "remote:spawn(function() local task = remote:spawn(function() return 9 end) coroutine.yield(1) return task:await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    let yielded = client.run_to_outcome(&agent, execution, 0);
    assert!(matches!(yielded.outcome, Outcome::Yielded(_)));
    for now in (10_000..=90_000).step_by(10_000) {
        dut.borrow_mut().tick(now);
        agent.borrow_mut().tick(now);
        let _ = client.status(execution);
        agent.borrow_mut().drive(20);
        assert_eq!(dut.borrow().execution_count(), 1, "child expired at {now}");
    }
    assert!(statuses.borrow().len() >= 3);
    assert_eq!(resumes.get(), 0, "keepalives must not resume the child");
    client.resume(execution, 1, vec![]).unwrap();
    let result = client.run_to_outcome(&agent, execution, 1);
    assert_eq!(result.disposition, Disposition::Certain);
    assert_eq!(returned(result), vec![Value::Integer(9)]);
    assert_eq!(resumes.get(), 1);
}

#[test]
fn child_is_kept_alive_while_parent_waits_on_a_native() {
    let dut = dut_node(61);
    let statuses = Rc::new(RefCell::new(Vec::new()));
    let resumes = Rc::new(Cell::new(0));
    let link = MonitorHeartbeats {
        inner: Loopback::new(dut.clone()),
        statuses: statuses.clone(),
        resumes: resumes.clone(),
    };
    let finish = Rc::new(Cell::new(false));
    let calls = Rc::new(Cell::new(0));
    let config = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        vec![route(contract_digest(DUT_ID))],
        60,
    );
    let agent = build(
        config,
        Box::new(Stubborn {
            finish: finish.clone(),
            started: calls.clone(),
        }),
        Some(children_on(immediate(link))),
    );
    let source = "remote:spawn(function() local child = remote:spawn(function() return 9 end) reset.restart() return child:await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    agent.borrow_mut().drive(100);
    assert_eq!(calls.get(), 1);
    assert_eq!(client.status(execution).r#state, v1::State::NativeWait);
    for now in (10_000..=90_000).step_by(10_000) {
        dut.borrow_mut().tick(now);
        agent.borrow_mut().tick(now);
        let _ = client.status(execution);
        agent.borrow_mut().drive(20);
        assert_eq!(dut.borrow().execution_count(), 1);
    }
    assert!(statuses.borrow().len() >= 3);
    assert_eq!(resumes.get(), 0);
    finish.set(true);
    let result = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(result.disposition, Disposition::Certain);
    assert_eq!(returned(result), vec![Value::Integer(9)]);
}

#[test]
fn heartbeat_interval_uses_the_child_timeout_not_the_parent_timeout() {
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(DUT_ID),
        Vec::new(),
        61,
    );
    settings.session_idle_ms = 9_000;
    let dut = build(settings, gpio(), None);
    let statuses = Rc::new(RefCell::new(Vec::new()));
    let resumes = Rc::new(Cell::new(0));
    let link = MonitorHeartbeats {
        inner: Loopback::new(dut.clone()),
        statuses: statuses.clone(),
        resumes,
    };
    let agent = agent_node(60, Some(immediate(link)));
    let source = "remote:spawn(function() local child = remote:spawn(function() return 9 end) coroutine.yield(1) return child:await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    assert!(matches!(
        client.run_to_outcome(&agent, execution, 0).outcome,
        Outcome::Yielded(_)
    ));
    for now in (3_000..=30_000).step_by(3_000) {
        dut.borrow_mut().tick(now);
        agent.borrow_mut().tick(now);
        let _ = client.status(execution);
        agent.borrow_mut().drive(20);
        assert_eq!(dut.borrow().execution_count(), 1, "child expired at {now}");
    }
    assert!(statuses.borrow().len() >= 9);
}

#[test]
fn due_heartbeats_make_progress_for_multiple_children_with_reserved_capacity() {
    let dut = dut_node(61);
    let statuses = Rc::new(RefCell::new(Vec::new()));
    let resumes = Rc::new(Cell::new(0));
    let link = MonitorHeartbeats {
        inner: Loopback::new(dut.clone()),
        statuses: statuses.clone(),
        resumes: resumes.clone(),
    };
    let mut settings = child_settings();
    settings.max_ops = 3;
    let client = LinkChildren::new(settings, vec![(1, immediate(link))]);
    let config = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        vec![route(contract_digest(DUT_ID))],
        60,
    );
    let agent = build(config, Box::new(NoNatives), Some(Box::new(client)));
    let source = "remote:spawn(function() local a = remote:spawn(function() return 1 end) local b = remote:spawn(function() return 2 end) local c = remote:spawn(function() return 3 end) coroutine.yield(1) return a:await(), b:await(), c:await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    assert!(matches!(
        client.run_to_outcome(&agent, execution, 0).outcome,
        Outcome::Yielded(_)
    ));
    for now in (10_000..=90_000).step_by(10_000) {
        dut.borrow_mut().tick(now);
        agent.borrow_mut().tick(now);
        let _ = client.status(execution);
        agent.borrow_mut().drive(100);
        assert_eq!(dut.borrow().execution_count(), 3, "child expired at {now}");
    }
    let reached: BTreeSet<_> = statuses.borrow().iter().copied().collect();
    assert_eq!(reached.len(), 3);
    assert_eq!(resumes.get(), 0);
    client.resume(execution, 1, vec![]).unwrap();
    let result = client.run_to_outcome(&agent, execution, 1);
    assert_eq!(result.disposition, Disposition::Certain);
    assert_eq!(
        returned(result),
        vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
    );
}

#[test]
fn silent_keepalive_is_bounded_and_marks_the_owner_uncertain() {
    let dut = dut_node(61);
    let sent = Rc::new(Cell::new(0));
    let link = SilentFor {
        inner: ImmediateLink::new(Loopback::new(dut)),
        silent: "Status",
        waiting: false,
        sent: sent.clone(),
    };
    let agent = tuned_agent_with(|settings| settings.call_timeout_ms = 1_000, Box::new(link));
    let source = "remote:spawn(function() local child = remote:spawn(function() return 9 end) coroutine.yield(1) return child:await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    assert!(matches!(
        client.run_to_outcome(&agent, execution, 0).outcome,
        Outcome::Yielded(_)
    ));
    agent.borrow_mut().tick(10_000);
    agent.borrow_mut().drive(10);
    assert_eq!(sent.get(), 1);
    agent.borrow_mut().tick(11_000);
    agent.borrow_mut().drive(10);
    client.resume(execution, 1, vec![]).unwrap();
    let result = client.run_to_outcome(&agent, execution, 1);
    assert_eq!(result.disposition, Disposition::Uncertain);
}

#[test]
fn parent_cleanup_cancels_known_child_after_keepalive_timeout() {
    let dut = dut_node(61);
    let sent = Rc::new(Cell::new(0));
    let link = SilentFor {
        inner: ImmediateLink::new(Loopback::new(dut.clone())),
        silent: "Status",
        waiting: false,
        sent: sent.clone(),
    };
    let agent = tuned_agent_with(|settings| settings.call_timeout_ms = 1_000, Box::new(link));
    let source = "remote:spawn(function() local child = remote:spawn(function() return 9 end) coroutine.yield(1) return child:await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    assert!(matches!(
        client.run_to_outcome(&agent, execution, 0).outcome,
        Outcome::Yielded(_)
    ));
    agent.borrow_mut().tick(10_000);
    agent.borrow_mut().drive(10);
    assert_eq!(sent.get(), 1);
    agent.borrow_mut().tick(11_000);
    agent.borrow_mut().drive(10);
    assert!(client.status(execution).r#uncertain);
    client.cancel(execution).unwrap();
    agent.borrow_mut().drive(200);
    assert_eq!(
        dut.borrow().execution_count(),
        0,
        "a failed read must not forget a known child"
    );
    let status = client.status(execution);
    assert_eq!(status.r#state, v1::State::Failed);
    assert!(
        status.r#uncertain,
        "successful cleanup must not erase earlier uncertainty"
    );
}

#[test]
fn parent_cleanup_still_cancels_child_after_missed_contact_deadline() {
    let dut = dut_node(61);
    let agent = agent_node(60, Some(immediate(Loopback::new(dut.clone()))));
    let source = "remote:spawn(function() local child = remote:spawn(function() return 9 end) coroutine.yield(1) return child:await() end)\n";
    let (mut client, agent, execution) = started(source, agent);
    assert!(matches!(
        client.run_to_outcome(&agent, execution, 0).outcome,
        Outcome::Yielded(_)
    ));
    // The embedding advances the parent's clock and receives contact, but fails to poll its scheduler. The child's clock
    // has not advanced, so a missed local deadline cannot prove that its known execution has stopped.
    for now in [10_000, 20_000, 30_000] {
        agent.borrow_mut().tick(now);
        let _ = client.status(execution);
    }
    agent.borrow_mut().drive(10);
    assert!(client.status(execution).r#uncertain);
    assert_eq!(dut.borrow().execution_count(), 1);
    client.cancel(execution).unwrap();
    agent.borrow_mut().drive(200);
    assert_eq!(dut.borrow().execution_count(), 0);
    let status = client.status(execution);
    assert_eq!(status.r#state, v1::State::Failed);
    assert!(status.r#uncertain);
}

#[test]
fn parent_lease_stops_heartbeats_and_cleans_up_its_child() {
    let dut = dut_node(61);
    let statuses = Rc::new(RefCell::new(Vec::new()));
    let resumes = Rc::new(Cell::new(0));
    let link = MonitorHeartbeats {
        inner: Loopback::new(dut.clone()),
        statuses: statuses.clone(),
        resumes,
    };
    let agent = agent_node(60, Some(immediate(link)));
    let source = "remote:spawn(function() local child = remote:spawn(function() return 9 end) coroutine.yield(1) return child:await() end)\n";
    let artifact = site_zero(source).sites[0].artifact.clone();
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).unwrap();
    let execution = client
        .start_with_lease(digest(&artifact), vec![], vec![], 15_000)
        .unwrap();
    client.resume(execution, 0, vec![]).unwrap();
    assert!(matches!(
        client.run_to_outcome(&agent, execution, 0).outcome,
        Outcome::Yielded(_)
    ));
    dut.borrow_mut().tick(10_000);
    agent.borrow_mut().tick(10_000);
    agent.borrow_mut().drive(20);
    assert!(!statuses.borrow().is_empty());
    dut.borrow_mut().tick(15_000);
    agent.borrow_mut().tick(15_000);
    agent.borrow_mut().drive(100);
    assert_eq!(dut.borrow().execution_count(), 0);
    assert_eq!(client.status(execution).r#state, v1::State::Failed);
    let count = statuses.borrow().len();
    agent.borrow_mut().tick(20_000);
    agent.borrow_mut().drive(20);
    assert_eq!(statuses.borrow().len(), count);
}

struct SendLostStart {
    inner: ImmediateLink<Loopback>,
    sent: bool,
}
impl ChildLink for SendLostStart {
    fn send(&mut self, path: &str, request: &[u8]) -> Result<(), LinkError> {
        if path.ends_with("Start") {
            if self.sent {
                return Err(LinkError::Status {
                    code: Code::ResourceExhausted,
                    message: "retry refused".to_string(),
                });
            }
            self.sent = true;
            self.inner.send(path, request)?;
            let _ = self.inner.poll();
            return Err(LinkError::Lost("send lost after delivery".to_string()));
        }
        self.inner.send(path, request)
    }
    fn poll(&mut self) -> Poll<Result<Vec<u8>, LinkError>> {
        self.inner.poll()
    }
    fn abort(&mut self) {
        self.inner.abort();
    }
    fn drive(&mut self, rounds: u32) {
        self.inner.drive(rounds);
    }
}

#[test]
fn send_time_lost_start_then_refusal_stays_uncertain() {
    let dut = dut_node(61);
    let link = SendLostStart {
        inner: ImmediateLink::new(Loopback::new(dut.clone())),
        sent: false,
    };
    let agent = agent_node(60, Some(Box::new(link)));
    let source = "remote:spawn(function() local ok = pcall(function() remote:spawn(function() return 1 end) end) return ok end)\n";
    let (mut client, agent, execution) = started(source, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(observed.disposition, Disposition::Uncertain);
    assert_eq!(dut.borrow().execution_count(), 1);
}

/// Never delivers `Cancel`, as a link that fails at the moment it matters.
struct DropsCancel(Loopback);

impl ImmediateCall for DropsCancel {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        if path.ends_with("Cancel") {
            return Err(LinkError::Lost("cancel not delivered".to_string()));
        }
        ImmediateCall::call(&mut self.0, path, request)
    }

    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut self.0, rounds);
    }
}

/// A link that serves calls from a loopback, except that the named method is sent and then never answered.
struct SilentFor {
    inner: ImmediateLink<Loopback>,
    silent: &'static str,
    waiting: bool,
    sent: Rc<Cell<u32>>,
}

impl ChildLink for SilentFor {
    fn send(&mut self, path: &str, request: &[u8]) -> Result<(), LinkError> {
        if path.ends_with(self.silent) {
            self.sent.set(self.sent.get() + 1);
            self.waiting = true;
            return Ok(());
        }
        self.inner.send(path, request)
    }

    fn poll(&mut self) -> Poll<Result<Vec<u8>, LinkError>> {
        if self.waiting {
            return Poll::Pending;
        }
        self.inner.poll()
    }

    fn abort(&mut self) {
        self.waiting = false;
        self.inner.abort();
    }

    fn drive(&mut self, rounds: u32) {
        self.inner.drive(rounds);
    }
}

/// Uploads, starts and resumes the agent artifact of `source`, and returns the client, the agent and the execution.
fn started(source: &str, agent: Rc<RefCell<Node>>) -> (Client, Rc<RefCell<Node>>, u64) {
    let artifact = site_zero(source).sites[0].artifact.clone();
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    (client, agent, execution)
}

const AWAIT_ONE: &str = "remote:spawn(function()\n    local v = remote:spawn(function() return 1 end)\n    return v:await()\nend)\n";

const BUSY_CHILD: &str = "remote:spawn(function()\n    local t = remote:spawn(function() while true do chip.model() end end)\n    t:await()\nend)\n";

// -- F02, F03: one disposition, carried across hops -------------------------------------------------------------------

#[test]
fn an_uncertain_child_return_survives_the_parent_a_second_await_and_the_release() {
    let dut = dut_node(31);
    let agent = agent_node(30, Some(immediate(MarksUncertain(Loopback::new(dut)))));
    let (mut client, agent, execution) = started(TWICE_AWAITED, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(
        returned(observed.clone()),
        vec![Value::Integer(7), Value::Integer(7)]
    );
    assert_eq!(
        observed.disposition,
        Disposition::Uncertain,
        "a returned value does not clear the child's uncertainty, and the cached second await keeps it"
    );
    assert!(client.status(execution).r#uncertain);
    let cancel = client
        .cancel(execution)
        .expect("cancel of a terminal record");
    assert!(
        cancel.r#uncertain,
        "a cancel reply never says certain while the record is uncertain"
    );
}

#[test]
fn a_caught_failure_cannot_clear_an_uncertain_child() {
    let dut = dut_node(33);
    let agent = agent_node(32, Some(immediate(MarksUncertain(Loopback::new(dut)))));
    let source = "remote:spawn(function()\n    local t = remote:spawn(function() error(\"boom\") end)\n    local ok = pcall(function() return t:await() end)\n    return ok\nend)\n";
    let (mut client, agent, execution) = started(source, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(returned(observed.clone()), vec![Value::Boolean(false)]);
    assert_eq!(observed.disposition, Disposition::Uncertain);
}

#[test]
fn an_uncertain_yield_and_a_result_bound_error_keep_the_disposition() {
    let dut = dut_node(35);
    let agent = agent_node(34, Some(immediate(MarksUncertain(Loopback::new(dut)))));
    let source = "remote:spawn(function()\n    local t = remote:spawn(function() coroutine.yield(1) end)\n    local ok = pcall(function() return t:await() end)\n    return ok\nend)\n";
    let (mut client, agent, execution) = started(source, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(returned(observed.clone()), vec![Value::Boolean(false)]);
    assert_eq!(
        observed.disposition,
        Disposition::Uncertain,
        "the yielded child's uncertainty survives"
    );

    let dut = dut_node(37);
    let agent = tuned_agent_with(
        |settings| settings.routes[0].bounds.max_result_bytes = 4,
        immediate(MarksUncertain(Loopback::new(dut))),
    );
    let source = "remote:spawn(function()\n    return remote:spawn(function() return \"0123456789\" end):await()\nend)\n";
    let (mut client, agent, execution) = started(source, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert!(failure_message(observed.clone()).contains("exceeds the slot's 4 bytes"));
    assert_eq!(observed.disposition, Disposition::Uncertain);
}

fn tuned_agent_with(
    tune: impl FnOnce(&mut NodeConfig),
    link: Box<dyn ChildLink>,
) -> Rc<RefCell<Node>> {
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        vec![route(contract_digest(DUT_ID))],
        5,
    );
    tune(&mut settings);
    build(settings, Box::new(NoNatives), Some(children_on(link)))
}

// -- F01, F18: cancellation and exit clean up the children they own ---------------------------------------------------

#[test]
fn cancelling_a_parent_stops_its_running_child_before_the_cancel_completes() {
    let calls = Rc::new(Cell::new(0));
    let dut = counting_dut(41, calls.clone());
    let agent = agent_node(40, Some(immediate(Loopback::new(dut.clone()))));
    let (mut client, agent, execution) = started(BUSY_CHILD, agent);
    agent.borrow_mut().drive(60);
    assert!(calls.get() > 0, "the child is running natives");
    assert_eq!(dut.borrow().execution_count(), 1);

    let cancel = client.cancel(execution).expect("cancel");
    assert_eq!(
        cancel.r#state,
        v1::State::Cancelling,
        "the acknowledgement is not proof that the child stopped"
    );
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert!(matches!(observed.outcome, Outcome::Failed(ref f) if f.code == FailureCode::Cancelled));
    assert_eq!(
        observed.disposition,
        Disposition::Certain,
        "the child was shown to stop"
    );
    assert_eq!(
        dut.borrow().execution_count(),
        0,
        "the child was cancelled and released"
    );
    let after = calls.get();
    agent.borrow_mut().drive(100);
    dut.borrow_mut().drive(100);
    assert_eq!(calls.get(), after, "no native runs after quiescence");
}

#[test]
fn a_cancel_that_cannot_reach_the_child_leaves_the_parent_uncertain() {
    let calls = Rc::new(Cell::new(0));
    let dut = counting_dut(43, calls.clone());
    let agent = agent_node(42, Some(immediate(DropsCancel(Loopback::new(dut.clone())))));
    let (mut client, agent, execution) = started(BUSY_CHILD, agent);
    agent.borrow_mut().drive(60);
    let cancel = client.cancel(execution).expect("cancel");
    assert_eq!(cancel.r#state, v1::State::Cancelling);
    // Without a matching Cancel receipt, cleanup must remain pending until its deadline rather than guessing the sequence.
    agent.borrow_mut().drive(100);
    let status = client.status(execution);
    assert_eq!(status.r#state, v1::State::Cancelling);
    assert!(status.r#uncertain);
    agent.borrow_mut().tick(4_999);
    agent.borrow_mut().drive(20);
    assert_eq!(client.status(execution).r#state, v1::State::Cancelling);
    agent.borrow_mut().tick(5_000);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert!(matches!(observed.outcome, Outcome::Failed(ref f) if f.code == FailureCode::Cancelled));
    assert_eq!(
        observed.disposition,
        Disposition::Uncertain,
        "the child may still be running, so the cancellation is not certain"
    );
    assert_eq!(
        dut.borrow().execution_count(),
        1,
        "the unreachable child is still there"
    );
}

#[test]
fn an_early_return_settles_every_child_it_still_owns() {
    let calls = Rc::new(Cell::new(0));
    let dut = counting_dut(45, calls.clone());
    let agent = agent_node(44, Some(immediate(Loopback::new(dut.clone()))));
    let early = "remote:spawn(function()\n    local ready = remote:spawn(function() return chip.model() end)\n    return 5\nend)\n";
    let (mut client, agent, execution) = started(early, agent.clone());
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(returned(observed.clone()), vec![Value::Integer(5)]);
    assert_eq!(observed.disposition, Disposition::Certain);
    assert_eq!(
        dut.borrow().execution_count(),
        0,
        "the never-resumed child was cancelled and released"
    );
    assert_eq!(calls.get(), 0, "the Ready child never ran");
}

#[test]
fn a_yield_keeps_the_children_until_the_parent_is_cancelled() {
    let dut = dut_node(47);
    let agent = agent_node(46, Some(immediate(Loopback::new(dut.clone()))));
    let source = "remote:spawn(function()\n    local t = remote:spawn(function() return 1 end)\n    coroutine.yield(0)\n    return t:await()\nend)\n";
    let (mut client, agent, execution) = started(source, agent);
    let yielded = client.run_to_outcome(&agent, execution, 0);
    assert!(matches!(yielded.outcome, Outcome::Yielded(_)));
    assert_eq!(
        dut.borrow().execution_count(),
        1,
        "a yield keeps the child owned"
    );
    client.cancel(execution).expect("cancel");
    for _ in 0..50 {
        agent.borrow_mut().drive(10);
    }
    assert!(matches!(
        client.status(execution).r#state,
        v1::State::Failed
    ));
    assert_eq!(dut.borrow().execution_count(), 0);
}

#[test]
fn idle_expiry_cleans_up_children_before_the_parent_record_goes() {
    let calls = Rc::new(Cell::new(0));
    let dut = counting_dut(49, calls.clone());
    let agent = tuned_agent_with(
        |settings| settings.session_idle_ms = 1_000,
        immediate(Loopback::new(dut.clone())),
    );
    let (mut client, agent, execution) = started(BUSY_CHILD, agent);
    agent.borrow_mut().drive(60);
    agent.borrow_mut().tick(1_000);
    for _ in 0..50 {
        agent.borrow_mut().drive(10);
    }
    let gone = client
        .query(execution, 0)
        .expect_err("the abandoned record is removed once certain");
    assert_eq!(gone.code, Code::NotFound);
    assert_eq!(
        dut.borrow().execution_count(),
        0,
        "its child was cancelled first"
    );
    assert_eq!(agent.borrow().unresolved_count(), 0);
}

#[test]
fn an_uncertain_orphan_is_kept_and_holds_its_slot() {
    let dut = dut_node(51);
    let agent = tuned_agent_with(
        |settings| {
            settings.session_idle_ms = 1_000;
            settings.max_executions = 1;
            settings.max_artifacts = 2;
        },
        immediate(DropsCancel(Loopback::new(dut))),
    );
    let (mut client, agent, execution) = started(BUSY_CHILD, agent);
    agent.borrow_mut().drive(60);
    agent.borrow_mut().tick(1_000);
    agent.borrow_mut().drive(20);
    agent.borrow_mut().tick(7_000);
    agent.borrow_mut().drive(5);
    assert_eq!(
        agent.borrow().unresolved_count(),
        1,
        "the uncertain record is not forgotten"
    );
    let status = client.status(execution);
    assert!(status.r#uncertain);
    assert_eq!(status.r#state, v1::State::Failed);
    let artifact = site_zero(AWAIT_ONE).sites[0].artifact.clone();
    client.upload(&artifact).expect("upload");
    let refused = client
        .start(digest(&artifact), vec![], vec![])
        .expect_err("the unresolved record holds the only slot, so admission fails closed");
    assert_eq!(refused.code, Code::ResourceExhausted);
}

#[test]
fn a_lease_cancels_the_execution_and_reads_do_not_extend_it() {
    let workflow =
        site_zero("remote:spawn(function() local n = 0 while true do n = n + 1 end end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node(52, None);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start_with_lease(digest(&artifact), vec![], vec![], 1_000)
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    for now in [300, 600, 999] {
        node.borrow_mut().tick(now);
        node.borrow_mut().drive(2);
        let _ = client.query(execution, 0).expect("a read");
        assert_eq!(client.status(execution).r#state, v1::State::FuelSuspended);
    }
    node.borrow_mut().tick(1_000);
    let observed = client.run_to_outcome(&node, execution, 0);
    assert!(matches!(observed.outcome, Outcome::Failed(ref f) if f.code == FailureCode::Cancelled));
}

#[test]
fn child_tasks_per_execution_are_bounded() {
    let dut = dut_node(55);
    let agent = tuned_agent_with(
        |settings| settings.max_child_tasks = 2,
        immediate(Loopback::new(dut)),
    );
    let (mut client, agent, execution) = started(SEQUENTIAL_SPAWNS, agent);
    let message = failure_message(client.run_to_outcome(&agent, execution, 0));
    assert!(message.contains("at most 2 child tasks"), "{message}");
}

// -- F16: lost replies and deadlines on the child hop -----------------------------------------------------------------

#[test]
fn lost_replies_never_start_a_second_child_or_resume_one_twice() {
    let calls = Rc::new(Cell::new(0));
    let dut = counting_dut(61, calls.clone());
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let link = LosesFirstReply {
        inner: Loopback::new(dut.clone()),
        methods: vec!["Start", "Resume", "QueryOutcome"],
        lost: BTreeSet::new(),
        delivered: delivered.clone(),
    };
    let agent = agent_node(60, Some(immediate(link)));
    let source = "remote:spawn(function()\n    local t = remote:spawn(function() return chip.model() end)\n    return t:await()\nend)\n";
    let (mut client, agent, execution) = started(source, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(returned(observed.clone()), vec![Value::Boolean(true)]);
    assert_eq!(
        observed.disposition,
        Disposition::Certain,
        "every lost reply was settled by replay"
    );
    assert_eq!(calls.get(), 1, "the child ran its native exactly once");
    let delivered = delivered.borrow();
    let starts = delivered.iter().filter(|m| *m == "Start").count();
    let resumes = delivered.iter().filter(|m| *m == "Resume").count();
    assert_eq!(
        (starts, resumes),
        (2, 2),
        "each lost reply was retransmitted once, unchanged"
    );
    assert_eq!(
        dut.borrow().execution_count(),
        0,
        "one execution was created and released"
    );
}

/// A lost commit reply is settled by replay, as a lost release is. The parent retransmits the identical commit, which is a
/// final call, so the child returns the answer it recorded, and the parent starts the artifact it already stored.
#[test]
fn a_lost_commit_reply_is_settled_by_replay() {
    let dut = dut_node(62);
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let link = LosesFirstReply {
        inner: Loopback::new(dut.clone()),
        methods: vec!["CommitArtifact"],
        lost: BTreeSet::new(),
        delivered: delivered.clone(),
    };
    let agent = agent_node(63, Some(immediate(link)));
    let (mut client, agent, execution) = started(AWAIT_ONE, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(returned(observed.clone()), vec![Value::Integer(1)]);
    assert_eq!(observed.disposition, Disposition::Certain);
    let commits = delivered
        .borrow()
        .iter()
        .filter(|m| *m == "CommitArtifact")
        .count();
    assert_eq!(
        commits, 2,
        "the lost commit was retransmitted once, unchanged"
    );
}

/// A lost release reply is settled by replay: the retransmission is the identical final call, so the node returns the
/// answer it recorded, and that answer carries the child's true disposition. The release is certain.
#[test]
fn a_lost_release_reply_is_settled_by_replay() {
    let dut = dut_node(61);
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let link = LosesFirstReply {
        inner: Loopback::new(dut.clone()),
        methods: vec!["Release"],
        lost: BTreeSet::new(),
        delivered: delivered.clone(),
    };
    let agent = agent_node(60, Some(immediate(link)));
    let (mut client, agent, execution) = started(AWAIT_ONE, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(observed.disposition, Disposition::Certain);
    let releases = delivered
        .borrow()
        .iter()
        .filter(|m| *m == "Release")
        .count();
    assert_eq!(
        releases, 2,
        "the lost release was retransmitted once, unchanged"
    );
    assert_eq!(dut.borrow().execution_count(), 0);
}

#[test]
fn missing_release_confirmation_cannot_become_a_certain_success() {
    struct EmptyRelease(Loopback);
    impl ImmediateCall for EmptyRelease {
        fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
            if path.ends_with("Release") {
                return Ok(codec::encode(&v1::ReleaseResponse::default()));
            }
            ImmediateCall::call(&mut self.0, path, request)
        }
        fn drive(&mut self, rounds: u32) {
            ImmediateCall::drive(&mut self.0, rounds);
        }
    }
    let dut = dut_node(63);
    let agent = agent_node(62, Some(immediate(EmptyRelease(Loopback::new(dut)))));
    let (mut client, agent, execution) = started(AWAIT_ONE, agent);
    assert_eq!(
        client.run_to_outcome(&agent, execution, 0).disposition,
        Disposition::Uncertain
    );
}

#[test]
fn a_silent_child_fails_the_operation_at_its_deadline() {
    let dut = dut_node(63);
    let sent = Rc::new(Cell::new(0));
    let link = SilentFor {
        inner: ImmediateLink::new(Loopback::new(dut)),
        silent: "Describe",
        waiting: false,
        sent: sent.clone(),
    };
    let agent = tuned_agent_with(|settings| settings.call_timeout_ms = 20, Box::new(link));
    let (mut client, agent, execution) = started(AWAIT_ONE, agent);
    agent.borrow_mut().drive(10);
    agent.borrow_mut().tick(19);
    agent.borrow_mut().drive(10);
    assert_eq!(
        client.status(execution).r#state,
        v1::State::NativeWait,
        "still within the deadline"
    );
    agent.borrow_mut().tick(20);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert!(failure_message(observed.clone()).contains("unavailable; nothing ran"));
    assert_eq!(
        observed.disposition,
        Disposition::Certain,
        "the Describe has no effect"
    );
    assert_eq!(sent.get(), 1, "the silent call was not repeated");
}

#[test]
fn a_start_that_is_never_answered_leaves_the_parent_uncertain() {
    let dut = dut_node(65);
    let link = SilentFor {
        inner: ImmediateLink::new(Loopback::new(dut)),
        silent: "Start",
        waiting: false,
        sent: Rc::new(Cell::new(0)),
    };
    let agent = tuned_agent_with(|settings| settings.call_timeout_ms = 20, Box::new(link));
    let (mut client, agent, execution) = started(AWAIT_ONE, agent);
    agent.borrow_mut().drive(30);
    agent.borrow_mut().tick(200);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert!(failure_message(observed.clone()).contains("spawn's outcome is unknown"));
    assert_eq!(
        observed.disposition,
        Disposition::Uncertain,
        "the Start may have reached the child"
    );
}

#[test]
fn a_child_on_another_protocol_revision_is_refused_before_any_upload() {
    struct OldChild(Loopback, Rc<RefCell<Vec<String>>>);
    impl ImmediateCall for OldChild {
        fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
            self.1
                .borrow_mut()
                .push(path.rsplit('/').next().unwrap_or_default().to_string());
            let reply = ImmediateCall::call(&mut self.0, path, request)?;
            if path.ends_with("Describe") {
                let mut described: v1::DescribeResponse = codec::decode(&reply).expect("describe");
                described.r#protocol_revision = 2;
                return Ok(codec::encode(&described));
            }
            Ok(reply)
        }
    }
    let calls = Rc::new(RefCell::new(Vec::new()));
    let agent = agent_node(
        66,
        Some(immediate(OldChild(
            Loopback::new(dut_node(67)),
            calls.clone(),
        ))),
    );
    let (mut client, agent, execution) = started(AWAIT_ONE, agent);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert!(failure_message(observed.clone()).contains("protocol revision 2"));
    assert_eq!(
        *calls.borrow(),
        vec!["Describe".to_string()],
        "nothing was uploaded or started"
    );
}

// -- F19: replay storage is reserved before mutation ------------------------------------------------------------------

#[test]
fn an_answer_bound_below_the_protocol_is_refused_at_construction() {
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        Vec::new(),
        5,
    );
    settings.max_answer_bytes = 71;
    assert_eq!(
        Node::new(settings, Box::new(NoNatives), None, 0).err(),
        Some(ConfigError::Ledger(
            LedgerConfigError::AnswerBoundTooSmall { required: 72 }
        ))
    );
}

#[test]
fn repeated_identical_refused_calls_create_nothing_and_can_be_retried() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        Vec::new(),
        5,
    );
    settings.max_executions = 1;
    settings.max_artifacts = 2;
    let node = build(settings, Box::new(NoNatives), None);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    let first = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    let flow = client.open_flow();
    let mut request = v1::StartRequest {
        r#digest: digest(&artifact).to_vec(),
        ..v1::StartRequest::default()
    };
    request.set_context(wire(flow.pending()));
    for _ in 0..5 {
        let refused = client
            .try_raw::<_, v1::StartResponse>("Start", &request)
            .expect_err("no execution slot");
        assert_eq!(refused.code, Code::ResourceExhausted);
        assert_eq!(
            node.borrow().execution_count(),
            1,
            "a refused start creates nothing"
        );
    }
    client.cancel(first).expect("cancel");
    client.release(first).expect("release");
    let started: v1::StartResponse = client.try_raw("Start", &request).expect(
        "the same call is admitted once there is room, since the refusals recorded nothing",
    );
    assert!(started.r#execution_id > first);
}

#[test]
fn a_refused_commit_ends_its_session_so_a_retransmission_never_runs() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let mut corrupt = workflow.sites[0].artifact.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0x55;
    let node = agent_node(68, None);
    let mut client = Client::new(node);
    let mut flow = client.open_flow();
    let mut begin = v1::BeginUploadRequest {
        r#digest: digest(&corrupt).to_vec(),
        r#total_length: corrupt.len() as u32,
        ..v1::BeginUploadRequest::default()
    };
    begin.set_context(wire(flow.pending()));
    let opened: v1::BeginUploadResponse = client.raw("BeginUpload", &begin);
    flow.advance();
    for (index, chunk) in corrupt.chunks(64).enumerate() {
        let mut put = v1::PutChunkRequest {
            r#upload_id: opened.r#upload_id,
            r#offset: (index * 64) as u32,
            r#data: chunk.to_vec(),
            ..v1::PutChunkRequest::default()
        };
        put.set_context(wire(flow.pending()));
        let _: v1::PutChunkResponse = client.raw("PutChunk", &put);
        flow.advance();
    }
    let mut commit = v1::CommitArtifactRequest {
        r#upload_id: opened.r#upload_id,
        ..v1::CommitArtifactRequest::default()
    };
    commit.set_context(wire(flow.pending()));
    let refused = client
        .try_raw::<_, v1::CommitArtifactResponse>("CommitArtifact", &commit)
        .expect_err("the artifact is refused");
    assert_eq!(refused.code, Code::InvalidArgument);
    let again = client
        .try_raw::<_, v1::CommitArtifactResponse>("CommitArtifact", &commit)
        .expect_err("the retransmission is refused as retired");
    assert_eq!(again.code, Code::Aborted);
}

// -- F20: requests are bounded before they are decoded ----------------------------------------------------------------

#[test]
fn an_oversized_or_amplified_request_is_refused_before_it_is_decoded() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node(70, None);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    let flow = client.open_flow();
    let mut start = v1::StartRequest {
        r#digest: digest(&artifact).to_vec(),
        ..v1::StartRequest::default()
    };
    start.set_context(wire(flow.pending()));
    // 400 nil values: 1.6 KB on the wire, far more once decoded.
    start.set_captures(v1::Tuple {
        r#values: vec![
            v1::Value {
                r#kind: Some(v1::Value_::Kind::Nil(v1::Nil {})),
            };
            400
        ],
    });
    let amplified = client
        .try_raw::<_, v1::StartResponse>("Start", &start)
        .expect_err("too many values for the node's tuples");
    assert_eq!(amplified.code, Code::InvalidArgument);

    let bytes = codec::encode(&start);
    let mut big = v1::PutChunkRequest {
        r#upload_id: 1,
        r#data: vec![0; 5_000],
        ..v1::PutChunkRequest::default()
    };
    big.set_context(wire(flow.pending()));
    let refused = node
        .borrow_mut()
        .handle("/lua.execution.v1.Execution/PutChunk", &codec::encode(&big))
        .expect_err("larger than max_request_bytes");
    assert_eq!(refused.code, Code::ResourceExhausted);
    assert!(bytes.len() < 4096);
    let mut retry = start.clone();
    retry.set_captures(Tuple::empty().to_wire());
    let started: v1::StartResponse = client
        .try_raw("Start", &retry)
        .expect("the refused calls consumed no sequence");
    assert!(started.r#execution_id > 0);
}

#[test]
fn the_loopback_refuses_an_oversized_message_during_reassembly() {
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        Vec::new(),
        5,
    );
    settings.max_request_bytes = 1_600;
    let node = build(settings, Box::new(NoNatives), None);
    let mut link = Loopback::new(node);
    let refused = link
        .call("/lua.execution.v1.Execution/Describe", &vec![0u8; 1_601])
        .expect_err("the transport cap refuses it before the node sees it");
    assert_ne!(refused.code, Code::Ok);
}

#[test]
fn describe_reports_the_revision_and_the_admission_window() {
    let mut client = Client::new(agent_node(71, None));
    let described: v1::DescribeResponse = client.raw("Describe", &v1::DescribeRequest::default());
    assert_eq!(
        described.r#protocol_revision,
        protolink_lua::PROTOCOL_REVISION
    );
    assert_eq!(described.r#retired_window, 256);
    assert_eq!(described.r#admission_floor, 0);
    assert_eq!(described.r#max_request_bytes, 4096);
}

#[test]
fn the_node_and_the_generator_agree_on_the_protocol_bounds() {
    assert_eq!(
        protolink_lua_plugin_gen::MIN_ANSWER_BYTES as usize,
        protolink_lua::ledger::MAX_MUTATION_ANSWER_BYTES
    );
    let limits = protolink_lua_plugin_gen::Limits {
        max_string_bytes: 256,
        max_tuple_items: 8,
        max_capture_bytes: 512,
        max_result_bytes: 512,
        max_spawn_depth: 2,
    };
    assert_eq!(
        protolink_lua_plugin_gen::min_request_bytes(&limits, 256) as usize,
        protolink_lua::frame::min_request_bytes(8, 512, 256)
    );
}

// -- F13: child edges are routed by slot ------------------------------------------------------------------------------

/// A copy of the BSP whose agent has two slots, `left` (id 5, index 1) and `right` (id 9, index 2), on the DUT contract.
fn two_slot_graph() -> protolink_lua_plugin_gen::Graph {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bsp");
    let dir: PathBuf =
        std::env::temp_dir().join(format!("lua-node-two-slots-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    for rel in [
        "host/bsp.toml",
        "agent/bsp.toml",
        "dut/bsp.toml",
        "base-esp32c6/bsp.toml",
        "base-esp32/bsp.toml",
        "ota/bsp.toml",
        "profiles/lua54-bounded-v1.toml",
    ] {
        let target = dir.join(rel);
        fs::create_dir_all(target.parent().expect("parent")).expect("dirs");
        fs::copy(source.join(rel), &target).expect("copy");
    }
    let agent_path = dir.join("agent/bsp.toml");
    let agent = fs::read_to_string(&agent_path).expect("agent");
    let start = agent.find("[[child_execution]]").expect("slot");
    let end = agent.find("[resources]").expect("resources");
    let slots = "[[child_execution]]\nname = \"left\"\nid = 5\nindex = 1\nchild_contract = \"remote\"\nintrinsic = \"task.spawn.v1\"\nrequires = [\"remote.execute\"]\ndelegates = []\nmax_capture_bytes = 512\nmax_result_bytes = 512\n\n[[child_execution]]\nname = \"right\"\nid = 9\nindex = 2\nchild_contract = \"remote\"\nintrinsic = \"task.spawn.v1\"\nrequires = [\"remote.execute\"]\ndelegates = []\nmax_capture_bytes = 512\nmax_result_bytes = 512\n\n";
    fs::write(
        &agent_path,
        format!("{}{slots}{}", &agent[..start], &agent[end..]),
    )
    .expect("write");
    Resolver::new(&dir, dir.join("profiles"))
        .expect("resolver")
        .resolve(dir.join("host/bsp.toml"))
        .expect("two-slot tree resolves")
}

#[test]
fn two_children_of_one_contract_are_reached_through_their_own_slots() {
    let graph = two_slot_graph();
    let source = "return remote:spawn(function()\n    local a = left:spawn(function() return chip.model(), 1 end):await()\n    local b, c = right:spawn(function() return chip.model(), 2 end):await()\n    return b, c\nend)\n";
    let workflow = protolink_lua_compiler::compile(source, &graph, HOST).expect("compiles");
    let artifact = workflow.sites[0].artifact.clone();
    let agent_contract = graph.contract(AGENT_ID).expect("agent");
    let dut_digest = graph.contract(DUT_ID).expect("dut").digest;
    let left_calls = Rc::new(Cell::new(0));
    let right_calls = Rc::new(Cell::new(0));
    let left = counting_dut(81, left_calls.clone());
    let right = counting_dut(82, right_calls.clone());
    let routes = vec![
        ChildRoute {
            slot: 5,
            index: 1,
            contract_digest: dut_digest,
            bounds: route(dut_digest).bounds,
        },
        ChildRoute {
            slot: 9,
            index: 2,
            contract_digest: dut_digest,
            bounds: route(dut_digest).bounds,
        },
    ];
    let mut settings = config("example.fixture.remote", agent_contract.digest, routes, 80);
    settings.operations = agent_contract.native_operations();
    settings.retired_window = 256;
    let children = LinkChildren::new(
        child_settings(),
        vec![
            (5, immediate(Loopback::new(left.clone()))),
            (9, immediate(Loopback::new(right.clone()))),
        ],
    );
    let agent = build(settings, Box::new(NoNatives), Some(Box::new(children)));
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(
        returned(observed),
        vec![Value::Boolean(true), Value::Integer(2)]
    );
    assert_eq!(
        (left_calls.get(), right_calls.get()),
        (1, 1),
        "each child ran on its own slot's node"
    );

    // A node that routes only one of the two slots refuses the artifact at Start rather than guessing a link.
    let mut settings = config(
        "example.fixture.remote",
        agent_contract.digest,
        vec![ChildRoute {
            slot: 5,
            index: 1,
            contract_digest: dut_digest,
            bounds: route(dut_digest).bounds,
        }],
        83,
    );
    settings.retired_window = 256;
    let lonely = build(
        settings,
        Box::new(NoNatives),
        Some(Box::new(LinkChildren::new(
            child_settings(),
            vec![(5, immediate(Loopback::new(dut_node(84))))],
        ))),
    );
    let mut client = Client::new(lonely);
    client.upload(&artifact).expect("upload");
    let refused = client
        .start(digest(&artifact), vec![], vec![])
        .expect_err("slot 9 has no route");
    assert_eq!(refused.code, Code::InvalidArgument);
    assert!(refused.message.contains("slot 9"), "{}", refused.message);
}

// -- P2.7: a native that cannot be interrupted ------------------------------------------------------------------------

/// A native provider whose operations finish only when the test says so, and which cannot interrupt them.
struct Stubborn {
    finish: Rc<Cell<bool>>,
    started: Rc<Cell<u32>>,
}

impl Platform for Stubborn {
    fn start(&mut self, _call: crate::NativeCall) -> Result<crate::NativeTicket, String> {
        self.started.set(self.started.get() + 1);
        Ok(crate::NativeTicket(1))
    }

    fn poll(&mut self, _ticket: crate::NativeTicket) -> Poll<Result<Option<Value>, String>> {
        if self.finish.get() {
            Poll::Ready(Ok(None))
        } else {
            Poll::Pending
        }
    }

    fn cancel(&mut self, _ticket: crate::NativeTicket) -> crate::NativeStop {
        crate::NativeStop::Running
    }
}

fn stubborn_agent(finish: Rc<Cell<bool>>, started: Rc<Cell<u32>>) -> Rc<RefCell<Node>> {
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        Vec::new(),
        90,
    );
    settings.cleanup_ms = 1_000;
    build(settings, Box::new(Stubborn { finish, started }), None)
}

const NATIVE_CALL: &str = "remote:spawn(function()\n    reset.restart()\n    return 1\nend)\n";

#[test]
fn cancelling_an_uninterruptible_native_waits_for_it_and_ends_certain_when_it_completes() {
    let (finish, calls) = (Rc::new(Cell::new(false)), Rc::new(Cell::new(0)));
    let agent = stubborn_agent(finish.clone(), calls.clone());
    let (mut client, agent, execution) = started(NATIVE_CALL, agent);
    agent.borrow_mut().drive(10);
    assert_eq!(calls.get(), 1);
    assert_eq!(client.status(execution).r#state, v1::State::NativeWait);
    let cancel = client.cancel(execution).expect("cancel");
    assert_eq!(
        cancel.r#state,
        v1::State::Cancelling,
        "the native is still running"
    );
    agent.borrow_mut().drive(10);
    assert_eq!(client.status(execution).r#state, v1::State::Cancelling);
    finish.set(true);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert!(matches!(observed.outcome, Outcome::Failed(ref f) if f.code == FailureCode::Cancelled));
    assert_eq!(
        observed.disposition,
        Disposition::Certain,
        "the native's effect is known once it completed"
    );
}

#[test]
fn an_uninterruptible_native_still_running_at_the_cleanup_deadline_leaves_the_record_uncertain() {
    let (finish, calls) = (Rc::new(Cell::new(false)), Rc::new(Cell::new(0)));
    let agent = stubborn_agent(finish.clone(), calls);
    let (mut client, agent, execution) = started(NATIVE_CALL, agent);
    agent.borrow_mut().drive(10);
    client.cancel(execution).expect("cancel");
    agent.borrow_mut().tick(1_000);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert!(matches!(observed.outcome, Outcome::Failed(ref f) if f.code == FailureCode::Cancelled));
    assert_eq!(
        observed.disposition,
        Disposition::Uncertain,
        "the VM's cancellation did not stop the hardware, and nothing proved it stopped"
    );
    let release = client.release(execution).expect("release");
    assert!(release.r#released && release.r#uncertain);
    assert!(!finish.get(), "release did not stop the native provider");
}

// -- P9: sustained mixed work stays within the configured bounds ------------------------------------------------------

/// The largest figure of each component seen so far.
#[derive(Default)]
struct HighWater(crate::Usage);

impl HighWater {
    fn observe(&mut self, node: &Rc<RefCell<Node>>) {
        let now = node.borrow().usage();
        let peak = &mut self.0;
        peak.executions = peak.executions.max(now.executions);
        peak.unresolved = peak.unresolved.max(now.unresolved);
        peak.child_tasks = peak.child_tasks.max(now.child_tasks);
        peak.uploads = peak.uploads.max(now.uploads);
        peak.upload_bytes = peak.upload_bytes.max(now.upload_bytes);
        peak.artifacts = peak.artifacts.max(now.artifacts);
        peak.sessions = peak.sessions.max(now.sessions);
        peak.retirement_bytes = peak.retirement_bytes.max(now.retirement_bytes);
    }
}

fn awaiting(value: u64) -> String {
    format!(
        "remote:spawn(function()\n    local v = remote:spawn(function() return {value} end)\n    return v:await()\nend)\n"
    )
}

const EARLY_RETURN: &str = "remote:spawn(function()\n    local ready = remote:spawn(function() return chip.model() end)\n    return 5\nend)\n";

const YIELD_THEN_AWAIT: &str = "remote:spawn(function()\n    local t = remote:spawn(function() return 1 end)\n    coroutine.yield(0)\n    return t:await()\nend)\n";

#[test]
fn sustained_mixed_work_on_sparse_sessions_stays_within_the_configured_bounds() {
    let dut = dut_node(61);
    let agent = tuned_agent_with(
        |settings| {
            settings.max_executions = 2;
            settings.max_artifacts = 3;
            settings.session_idle_ms = 1_000;
        },
        immediate(Loopback::new(dut.clone())),
    );
    let (agent_window, dut_window) = (
        agent.borrow().usage().retirement_bytes,
        dut.borrow().usage().retirement_bytes,
    );
    let mut artifacts: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut artifact_of = |source: &str| {
        artifacts
            .entry(source.to_string())
            .or_insert_with(|| site_zero(source).sites[0].artifact.clone())
            .clone()
    };
    let (mut agent_peak, mut dut_peak) = (HighWater::default(), HighWater::default());
    let mut client = Client::new(agent.clone());
    let mut now = 0u64;
    for round in 0..300u64 {
        client.next_session += 1 + (round * 7_919) % 97;
        now += 1;
        agent.borrow_mut().tick(now);
        let source = match round % 5 {
            0 => awaiting(round % 6),
            1 | 4 => BUSY_CHILD.to_string(),
            2 => EARLY_RETURN.to_string(),
            _ => YIELD_THEN_AWAIT.to_string(),
        };
        let artifact = artifact_of(&source);
        client.upload(&artifact).expect("upload");
        let execution = client
            .start(digest(&artifact), vec![], vec![])
            .expect("start");
        client.resume(execution, 0, vec![]).expect("resume");
        agent.borrow_mut().drive(30);
        agent_peak.observe(&agent);
        dut_peak.observe(&dut);
        match round % 5 {
            0 => {
                let observed = client.run_to_outcome(&agent, execution, 0);
                assert_eq!(returned(observed), vec![Value::Integer((round % 6) as i64)]);
            }
            1 | 3 => {
                client.cancel(execution).expect("cancel");
                let observed = client.run_to_outcome(&agent, execution, 0);
                assert!(
                    matches!(observed.outcome, Outcome::Failed(ref f) if f.code == FailureCode::Cancelled)
                );
                assert_eq!(observed.disposition, Disposition::Certain);
            }
            2 => {
                let observed = client.run_to_outcome(&agent, execution, 0);
                assert_eq!(returned(observed), vec![Value::Integer(5)]);
            }
            _ => {
                now += 1_000;
                agent.borrow_mut().tick(now);
                for _ in 0..20 {
                    agent.borrow_mut().drive(10);
                }
                agent_peak.observe(&agent);
                dut_peak.observe(&dut);
                continue;
            }
        }
        agent_peak.observe(&agent);
        dut_peak.observe(&dut);
        client.release(execution).expect("release");
    }

    for (node, window) in [(&agent, agent_window), (&dut, dut_window)] {
        let usage = node.borrow().usage();
        assert_eq!(
            (usage.executions, usage.unresolved, usage.child_tasks),
            (0, 0, 0),
            "every execution and child handle was settled"
        );
        assert_eq!(
            (usage.uploads, usage.upload_bytes, usage.sessions),
            (0, 0, 0)
        );
        assert_eq!(
            usage.retirement_bytes, window,
            "the retirement window never grew"
        );
    }
    let defaults = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        Vec::new(),
        0,
    );
    for peak in [&agent_peak.0, &dut_peak.0] {
        assert!(peak.executions <= defaults.max_executions);
        assert!(peak.child_tasks <= defaults.max_executions * defaults.max_child_tasks);
        assert!(peak.uploads <= defaults.max_uploads);
        assert!(peak.sessions <= defaults.max_sessions);
        assert_eq!(peak.unresolved, 0);
    }
    assert!(agent_peak.0.executions <= 2);
    assert!(agent_peak.0.artifacts <= 3, "the agent store evicts");
    assert!(dut_peak.0.artifacts <= defaults.max_artifacts);
    std::println!("agent high water {:?}", agent_peak.0);
    std::println!("dut high water {:?}", dut_peak.0);
}

/// Counts the native calls that reach the provider. A refused artifact must leave the count at zero.
fn counting_platform(starts: Rc<Cell<u32>>) -> Box<dyn Platform> {
    Box::new(ImmediateNatives::new(
        move |_module, _operation, _arguments| {
            starts.set(starts.get() + 1);
            Ok(Some(Value::Boolean(true)))
        },
    ))
}

/// The DUT artifact of [`NESTED`], which calls the DUT's native `chip.model`.
fn dut_artifact_of_nested() -> Vec<u8> {
    let agent = site_zero(NESTED).sites[0].artifact.clone();
    protolink_lua_bytecode::Artifact::decode(&agent, &BytecodeLimits::profile())
        .expect("decodes")
        .children[0]
        .bytes
        .to_vec()
}

/// Re-encodes `bytes` with its first native reference changed by `edit`. Only the reference differs.
fn with_forged_native(
    bytes: &[u8],
    edit: impl FnOnce(&mut protolink_lua_bytecode::NativeRef),
) -> Vec<u8> {
    let mut artifact = protolink_lua_bytecode::Artifact::decode(bytes, &BytecodeLimits::profile())
        .expect("decodes");
    edit(&mut artifact.natives[0]);
    artifact.encode()
}

#[test]
fn a_forged_or_mismatched_native_reference_is_refused_at_upload_before_any_provider_starts() {
    let dut_bytes = dut_artifact_of_nested();
    let starts = Rc::new(Cell::new(0));
    let dut = build(
        config(
            "example.fixture.remote",
            contract_digest(DUT_ID),
            Vec::new(),
            61,
        ),
        counting_platform(starts.clone()),
        None,
    );
    let mut client = Client::new(dut);

    let undeclared_module = with_forged_native(&dut_bytes, |native| native.module = 99);
    let refused = client
        .upload(&undeclared_module)
        .expect_err("a module the contract does not declare is refused");
    assert_eq!(refused.code, Code::InvalidArgument);

    let wrong_arity = with_forged_native(&dut_bytes, |native| native.argc += 1);
    client
        .upload(&wrong_arity)
        .expect_err("a reference with another argument count is refused");

    let wrong_results = with_forged_native(&dut_bytes, |native| native.results ^= 1);
    client
        .upload(&wrong_results)
        .expect_err("a reference with another result count is refused");

    assert_eq!(starts.get(), 0, "no refused artifact reaches the provider");
    client
        .upload(&dut_bytes)
        .expect("the declared operation is admitted");
}

#[test]
fn an_embedding_discard_frees_an_uncertain_orphan_slot_and_never_certifies_it() {
    let dut = dut_node(52);
    let agent = tuned_agent_with(
        |settings| {
            settings.session_idle_ms = 1_000;
            settings.max_executions = 1;
            settings.max_artifacts = 2;
        },
        immediate(DropsCancel(Loopback::new(dut))),
    );
    let (mut client, agent, execution) = started(BUSY_CHILD, agent);
    agent.borrow_mut().drive(60);
    agent.borrow_mut().tick(1_000);
    agent.borrow_mut().drive(20);
    agent.borrow_mut().tick(7_000);
    agent.borrow_mut().drive(5);
    assert_eq!(agent.borrow().unresolved_count(), 1);
    assert_eq!(
        agent.borrow_mut().discard_uncertain(execution + 1),
        Err(DiscardError::UnknownExecution),
    );
    agent
        .borrow_mut()
        .discard_uncertain(execution)
        .expect("the embedding discards the uncertain orphan");
    assert_eq!(agent.borrow().unresolved_count(), 0);
    assert_eq!(agent.borrow().discarded_uncertain(), 1);
    assert_eq!(
        agent.borrow_mut().discard_uncertain(execution),
        Err(DiscardError::UnknownExecution),
        "a discarded record cannot be discarded twice"
    );
    let gone = client.status_result(execution);
    assert_eq!(
        gone.expect_err("the record is gone").code,
        Code::NotFound,
        "after the discard nothing reports the outcome"
    );
    let artifact = site_zero(AWAIT_ONE).sites[0].artifact.clone();
    client.upload(&artifact).expect("upload");
    client
        .start(digest(&artifact), vec![], vec![])
        .expect("the freed slot admits a new execution");
}

// -- Recovery of uncertain children and orphans ----------------------------------------------------------------------------

/// An orphan whose cancel never reached the child, driven until its cleanup deadline has passed.
fn lost_cancel_orphan() -> (Client, Rc<RefCell<Node>>, Rc<RefCell<Node>>, u64) {
    let dut = dut_node(52);
    let agent = tuned_agent_with(
        |settings| {
            settings.session_idle_ms = 1_000;
            settings.max_executions = 1;
            settings.max_artifacts = 2;
        },
        immediate(DropsCancel(Loopback::new(dut.clone()))),
    );
    let (client, agent, execution) = started(BUSY_CHILD, agent);
    agent.borrow_mut().drive(60);
    (client, agent, dut, execution)
}

#[test]
fn parent_time_alone_does_not_free_a_lost_childs_slot_or_certify_the_record() {
    let (client, agent, dut, _execution) = lost_cancel_orphan();
    agent.borrow_mut().tick(1_000);
    agent.borrow_mut().drive(20);
    agent.borrow_mut().tick(7_000);
    agent.borrow_mut().drive(5);
    assert_eq!(agent.borrow().usage().lost_children, 1);

    // Only the parent's clock moves. The child's clock is where it was, and the child is still running.
    for clock in [100_000, 10_000_000, u64::MAX / 2] {
        agent.borrow_mut().tick(clock);
        agent.borrow_mut().drive(5);
    }
    let usage = agent.borrow().usage();
    assert_eq!(usage.lost_children, 1, "{usage:?}");
    assert_eq!(usage.recovered_children, 0);
    assert_eq!(dut.borrow().execution_count(), 1, "the child still runs");
    assert_eq!(agent.borrow().unresolved_count(), 1);
    drop(client);
}

#[test]
fn explicit_recovery_frees_the_slot_but_the_record_stays_uncertain() {
    let (mut client, agent, _dut, execution) = lost_cancel_orphan();
    agent.borrow_mut().tick(1_000);
    agent.borrow_mut().drive(20);
    agent.borrow_mut().tick(7_000);
    agent.borrow_mut().drive(5);

    let lost = agent.borrow().lost_child_ids();
    assert_eq!(lost.len(), 1);
    agent
        .borrow_mut()
        .recover_lost_child(lost[0].0)
        .expect("the child is quarantined");

    let usage = agent.borrow().usage();
    assert_eq!(usage.lost_children, 0);
    assert_eq!(usage.recovered_children, 1);
    assert_eq!(
        agent.borrow().unresolved_count(),
        1,
        "capacity was recovered, the outcome was not certified"
    );
    let status = client.status_result(execution).expect("record is kept");
    assert!(
        status.r#uncertain,
        "the disposition stays visible: {status:?}"
    );
    assert_eq!(
        agent.borrow_mut().recover_lost_child(lost[0].0),
        Err(RecoverError::NotQuarantined)
    );
}

#[test]
fn discard_is_refused_while_cancellation_reconciliation_is_pending() {
    let (_client, agent, _dut, execution) = lost_cancel_orphan();
    // The session goes idle, the cancel is sent and lost, and cleanup is retrying before its deadline.
    agent.borrow_mut().tick(1_000);
    agent.borrow_mut().drive(20);
    let usage = agent.borrow().usage();
    assert_eq!(usage.cleanups, 1, "{usage:?}");
    assert_eq!(
        agent.borrow_mut().discard_uncertain(execution),
        Err(DiscardError::CleanupActive),
        "discarding now would strand the cancel and its reconciliation"
    );
    assert_eq!(agent.borrow().execution_count(), 1, "the record is intact");

    // Once cleanup ends, the unsettled child is quarantined, and the record can go without losing track of it.
    agent.borrow_mut().tick(7_000);
    agent.borrow_mut().drive(5);
    agent
        .borrow_mut()
        .discard_uncertain(execution)
        .expect("cleanup is over");
    let usage = agent.borrow().usage();
    assert_eq!(usage.cleanups, 0);
    assert_eq!(usage.child_operations, 0, "no operation is stranded");
    assert_eq!(usage.lost_children, 1, "the child stays quarantined");
}
