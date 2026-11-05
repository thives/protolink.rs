//! Protocol and chain tests. Artifacts come from the compiler, so every test runs the same bytes the host produces.

mod quarantine;
mod remediation;
mod review;

use std::boxed::Box;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::format;
use std::path::Path;
use std::rc::Rc;
use std::string::{String, ToString};
use std::vec;
use std::vec::Vec;

use protolink::grpc::{Code, Status};
use protolink_lua::child::ChildClient;
use protolink_lua::codec;
use protolink_lua::flow::{Flow, Stamp};
use protolink_lua::lifecycle::{Disposition, Failure, FailureCode, Observation, Outcome};
use protolink_lua::value::{Limits as WireLimits, Tuple, Value};
use protolink_lua::wire::v1;
use protolink_lua_bytecode::{Limits as BytecodeLimits, NativeRef, digest};
use protolink_lua_compiler::Workflow;
use protolink_lua_plugin_gen::Resolver;
use protolink_lua_vm::VmLimits;

use crate::{
    ChildBounds, ChildLink, ChildRoute, ChildSettings, ImmediateCall, ImmediateLink,
    ImmediateNatives, LinkChildren, LinkError, Loopback, NoNatives, Node, NodeConfig, Platform,
};

pub(crate) const HOST: &str = "1b0a8e2c-7d4f-4e6a-9c1b-5a2f3e4d6c70";
pub(crate) const AGENT_ID: &str = "2bdbdeef-600a-46de-82b5-1936195c2d11";
pub(crate) const DUT_ID: &str = "78240354-a109-42a2-9b91-e0497fbd55d0";
pub(crate) const SEQUENTIAL_SPAWNS: &str = "remote:spawn(function()\n    local first = remote:spawn(function() return 1 end)\n    local a = first:await()\n    local second = remote:spawn(function() return 2 end)\n    local b = second:await()\n    local third = remote:spawn(function() return 3 end)\n    local c = third:await()\n    return a, b, c\nend)\n";
pub(crate) const TWICE_AWAITED: &str = "remote:spawn(function()\n    local task = remote:spawn(function() return 7 end)\n    local first = task:await()\n    local second = task:await()\n    return first, second\nend)\n";
pub(crate) const NESTED: &str = "local count = 4\nlocal task = remote:spawn(function()\n    local value = remote:spawn(function()\n        local enabled = chip.model()\n        return enabled, count, \"sample\"\n    end)\n    return value:await()\nend)\nreturn task\n";

pub(crate) fn bsp_graph() -> protolink_lua_plugin_gen::Graph {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bsp");
    Resolver::new(&root, root.join("profiles"))
        .expect("resolver")
        .resolve(root.join("host/bsp.toml"))
        .expect("bsp resolves")
}

pub(crate) fn contract_digest(id: &str) -> [u8; 32] {
    bsp_graph()
        .contract(id)
        .expect("contract is in the graph")
        .digest
}

/// Stands in for the DUT's chip.model. Any other native is refused.
pub(crate) fn gpio() -> Box<dyn Platform> {
    Box::new(ImmediateNatives::new(|module, _operation, _arguments| {
        if module == 1 {
            Ok(Some(Value::Boolean(true)))
        } else {
            Err("no such native".to_string())
        }
    }))
}

/// A child that is never reachable, which is how a link that is down looks to the parent.
pub(crate) struct Unreachable;

impl ImmediateCall for Unreachable {
    fn call(&mut self, _path: &str, _request: &[u8]) -> Result<Vec<u8>, LinkError> {
        Err(LinkError::Unavailable("link is down".to_string()))
    }
}

/// A child that accepts and runs, but whose outcome replies are lost on the way back.
pub(crate) struct LosesOutcomes(pub(crate) Loopback);

impl ImmediateCall for LosesOutcomes {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        if path.ends_with("QueryOutcome") {
            return Err(LinkError::Lost("reply lost".to_string()));
        }
        ImmediateCall::call(&mut self.0, path, request)
    }

    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut self.0, rounds);
    }
}

/// A child whose floor is the reserved `u64::MAX`, so it reports that no session id is left. It otherwise runs normally.
pub(crate) struct ExhaustedChild(pub(crate) Loopback);

impl ImmediateCall for ExhaustedChild {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        if path.ends_with("Describe") {
            let described = v1::DescribeResponse {
                r#session_floor: u64::MAX,
                r#protocol_revision: protolink_lua::PROTOCOL_REVISION,
                r#max_artifact_bytes: 65_536,
                r#max_chunk_bytes: 64,
                r#session_idle_ms: 30_000,
                ..v1::DescribeResponse::default()
            };
            return Ok(codec::encode(&described));
        }
        ImmediateCall::call(&mut self.0, path, request)
    }

    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut self.0, rounds);
    }
}

pub(crate) fn limits() -> WireLimits {
    WireLimits::new(8, 64, 256)
}

pub(crate) fn route(contract: [u8; 32]) -> ChildRoute {
    ChildRoute {
        slot: 1,
        index: 1,
        contract_digest: contract,
        bounds: ChildBounds {
            max_capture_bytes: 1 << 20,
            max_result_bytes: 1 << 20,
        },
    }
}

/// The native operation table of the BSP contract with `contract_digest`, as the generator emits it into the node. A digest
/// outside the BSP has no table here: a test on its own graph sets `operations` from that graph. A node without the table
/// refuses every artifact with a native reference, so the omission cannot pass silently.
pub(crate) fn operations_of(contract_digest: [u8; 32]) -> Vec<NativeRef> {
    let graph = bsp_graph();
    graph
        .contracts()
        .find(|contract| contract.digest == contract_digest)
        .map(|contract| contract.native_operations())
        .unwrap_or_default()
}

pub(crate) fn config(
    contract: &str,
    contract_digest: [u8; 32],
    routes: Vec<ChildRoute>,
    epoch: u64,
) -> NodeConfig {
    NodeConfig {
        contract: contract.to_string(),
        contract_digest,
        epoch,
        bytecode: BytecodeLimits::profile(),
        operations: operations_of(contract_digest),
        vm: VmLimits {
            max_frames: 16,
            max_objects: 2048,
            max_bytes: 1 << 16,
            wire: limits(),
            total_fuel: 1_000_000,
        },
        slice_fuel: 64,
        max_answer_bytes: 4096,
        max_sessions: 32,
        max_uploads: 4,
        max_executions: 8,
        max_chunk_bytes: 64,
        session_idle_ms: 30_000,
        max_capture_bytes: 1 << 20,
        max_artifacts: 9,
        retired_window: 256,
        max_child_tasks: 8,
        call_timeout_ms: 2_000,
        cleanup_ms: 5_000,
        max_request_bytes: 4096,
        routes,
    }
}

pub(crate) fn child_settings() -> ChildSettings {
    ChildSettings {
        wire: limits(),
        max_reply_bytes: 4096,
        max_children: 16,
        max_ops: 16,
        retransmits: 2,
    }
}

/// The child client of a parent with one link on slot 1.
pub(crate) fn children_on(link: Box<dyn ChildLink>) -> Box<dyn ChildClient> {
    Box::new(LinkChildren::new(child_settings(), vec![(1, link)]))
}

pub(crate) fn immediate<T: ImmediateCall + 'static>(transport: T) -> Box<dyn ChildLink> {
    Box::new(ImmediateLink::new(transport))
}

pub(crate) fn build(
    config: NodeConfig,
    platform: Box<dyn Platform>,
    children: Option<Box<dyn ChildClient>>,
) -> Rc<RefCell<Node>> {
    Rc::new(RefCell::new(
        Node::new(config, platform, children, 0).expect("valid configuration"),
    ))
}

pub(crate) fn agent_node(epoch: u64, child: Option<Box<dyn ChildLink>>) -> Rc<RefCell<Node>> {
    let routes = child
        .as_ref()
        .map_or_else(Vec::new, |_| vec![route(contract_digest(DUT_ID))]);
    agent_node_with_routes(epoch, child, routes)
}

pub(crate) fn agent_node_with_routes(
    epoch: u64,
    child: Option<Box<dyn ChildLink>>,
    routes: Vec<ChildRoute>,
) -> Rc<RefCell<Node>> {
    build(
        config(
            "example.fixture.remote",
            contract_digest(AGENT_ID),
            routes,
            epoch,
        ),
        Box::new(NoNatives),
        child.map(children_on),
    )
}

pub(crate) fn dut_node(epoch: u64) -> Rc<RefCell<Node>> {
    dut_node_with_sessions(epoch, 32)
}

pub(crate) fn dut_node_with_sessions(epoch: u64, max_sessions: usize) -> Rc<RefCell<Node>> {
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(DUT_ID),
        Vec::new(),
        epoch,
    );
    settings.max_sessions = max_sessions;
    settings.retired_window = settings.retired_window.max(max_sessions as u64);
    build(settings, gpio(), None)
}

pub(crate) fn agent_node_with_sessions(max_sessions: usize) -> Rc<RefCell<Node>> {
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        Vec::new(),
        5,
    );
    settings.max_sessions = max_sessions;
    build(settings, Box::new(NoNatives), None)
}

/// The protocol context of one call, as the wire carries it.
pub(crate) fn wire(stamp: Stamp) -> v1::Context {
    v1::Context {
        r#epoch: stamp.epoch,
        r#session_id: stamp.session_id,
        r#sequence: stamp.sequence,
    }
}

/// Compiles one host workflow and returns site 0.
pub(crate) fn site_zero(source: &str) -> Workflow {
    let graph = bsp_graph();
    protolink_lua_compiler::compile(source, &graph, HOST).expect("workflow compiles")
}

/// A client that keeps one session per upload and per execution, the way the central gateway does. A session takes one
/// of the node's `max_sessions` until its final call releases it.
pub(crate) struct Client {
    pub(crate) link: Loopback,
    pub(crate) epoch: u64,
    pub(crate) next_session: u64,
    /// The session of each execution this client started.
    pub(crate) flows: BTreeMap<u64, Flow>,
}

impl Client {
    pub(crate) fn new(node: Rc<RefCell<Node>>) -> Self {
        let mut client = Self {
            link: Loopback::new(node),
            epoch: 0,
            next_session: 0,
            flows: BTreeMap::new(),
        };
        let described: v1::DescribeResponse =
            client.raw("Describe", &v1::DescribeRequest::default());
        client.epoch = described.context().map_or(0, |c| c.r#epoch);
        client
    }

    pub(crate) fn open_flow(&mut self) -> Flow {
        self.next_session += 1;
        Flow::new(self.epoch, self.next_session)
    }

    pub(crate) fn flow_of(&self, execution: u64) -> Flow {
        *self
            .flows
            .get(&execution)
            .expect("the execution was started by this client")
    }

    /// The context of a read. A read takes no session slot, so it carries the execution's session unchanged.
    pub(crate) fn reading(&self, execution: u64) -> v1::Context {
        wire(self.flow_of(execution).reading())
    }

    pub(crate) fn raw<Req: micropb::MessageEncode, Resp: micropb::MessageDecode + Default>(
        &mut self,
        method: &str,
        request: &Req,
    ) -> Resp {
        self.try_raw(method, request)
            .unwrap_or_else(|s| panic!("{method} failed: {s:?}"))
    }

    pub(crate) fn try_raw<Req: micropb::MessageEncode, Resp: micropb::MessageDecode + Default>(
        &mut self,
        method: &str,
        request: &Req,
    ) -> Result<Resp, Status> {
        let path = format!("/lua.execution.v1.Execution/{method}");
        let bytes = self.link.call(&path, &codec::encode(request))?;
        Ok(codec::decode(&bytes).expect("response decodes"))
    }

    pub(crate) fn upload(&mut self, artifact: &[u8]) -> Result<(), Status> {
        let mut flow = self.open_flow();
        let mut begin = v1::BeginUploadRequest {
            r#digest: digest(artifact).to_vec(),
            r#total_length: artifact.len() as u32,
            ..v1::BeginUploadRequest::default()
        };
        begin.set_context(wire(flow.pending()));
        let opened: v1::BeginUploadResponse = self.try_raw("BeginUpload", &begin)?;
        flow.advance();
        let mut offset = 0usize;
        while offset < artifact.len() {
            let end = (offset + 64).min(artifact.len());
            let mut chunk = v1::PutChunkRequest {
                r#upload_id: opened.r#upload_id,
                r#offset: offset as u32,
                r#data: artifact[offset..end].to_vec(),
                ..v1::PutChunkRequest::default()
            };
            chunk.set_context(wire(flow.pending()));
            let _: v1::PutChunkResponse = self.try_raw("PutChunk", &chunk)?;
            flow.advance();
            offset = end;
        }
        let mut commit = v1::CommitArtifactRequest {
            r#upload_id: opened.r#upload_id,
            ..v1::CommitArtifactRequest::default()
        };
        commit.set_context(wire(flow.pending()));
        let _: v1::CommitArtifactResponse = self.try_raw("CommitArtifact", &commit)?;
        Ok(())
    }

    pub(crate) fn start(
        &mut self,
        digest: [u8; 32],
        captures: Vec<Value>,
        arguments: Vec<Value>,
    ) -> Result<u64, Status> {
        self.start_with_lease(digest, captures, arguments, 0)
    }

    pub(crate) fn start_with_lease(
        &mut self,
        digest: [u8; 32],
        captures: Vec<Value>,
        arguments: Vec<Value>,
        lease_ms: u32,
    ) -> Result<u64, Status> {
        let mut flow = self.open_flow();
        let mut request = v1::StartRequest {
            r#digest: digest.to_vec(),
            r#entry: 0,
            lease_ms,
            ..v1::StartRequest::default()
        };
        request.set_context(wire(flow.pending()));
        request.set_captures(
            Tuple::new(captures, limits())
                .expect("captures fit")
                .to_wire(),
        );
        request.set_arguments(
            Tuple::new(arguments, limits())
                .expect("arguments fit")
                .to_wire(),
        );
        let started: v1::StartResponse = self.try_raw("Start", &request)?;
        flow.advance();
        self.flows.insert(started.r#execution_id, flow);
        Ok(started.r#execution_id)
    }

    pub(crate) fn resume(
        &mut self,
        execution: u64,
        turn: u32,
        values: Vec<Value>,
    ) -> Result<v1::ResumeResponse, Status> {
        let mut flow = self.flow_of(execution);
        let mut request = v1::ResumeRequest {
            r#execution_id: execution,
            turn,
            ..v1::ResumeRequest::default()
        };
        request.set_context(wire(flow.pending()));
        request.set_values(Tuple::new(values, limits()).expect("values fit").to_wire());
        let response: v1::ResumeResponse = self.try_raw("Resume", &request)?;
        flow.advance();
        self.flows.insert(execution, flow);
        Ok(response)
    }

    pub(crate) fn query(
        &mut self,
        execution: u64,
        turn: u32,
    ) -> Result<v1::QueryOutcomeResponse, Status> {
        let mut request = v1::QueryOutcomeRequest {
            r#execution_id: execution,
            turn,
            ..v1::QueryOutcomeRequest::default()
        };
        request.set_context(self.reading(execution));
        self.try_raw("QueryOutcome", &request)
    }

    pub(crate) fn status(&mut self, execution: u64) -> v1::StatusResponse {
        let mut request = v1::StatusRequest {
            r#execution_id: execution,
            ..v1::StatusRequest::default()
        };
        request.set_context(self.reading(execution));
        self.raw("Status", &request)
    }

    pub(crate) fn status_result(&mut self, execution: u64) -> Result<v1::StatusResponse, Status> {
        let mut request = v1::StatusRequest {
            r#execution_id: execution,
            ..v1::StatusRequest::default()
        };
        request.set_context(self.reading(execution));
        self.try_raw("Status", &request)
    }

    pub(crate) fn cancel(&mut self, execution: u64) -> Result<v1::CancelResponse, Status> {
        let mut flow = self.flow_of(execution);
        let mut request = v1::CancelRequest {
            r#execution_id: execution,
            ..v1::CancelRequest::default()
        };
        request.set_context(wire(flow.pending()));
        let response: v1::CancelResponse = self.try_raw("Cancel", &request)?;
        flow.advance();
        self.flows.insert(execution, flow);
        Ok(response)
    }

    /// The final call of an execution. It frees the execution's session on the node.
    pub(crate) fn release(&mut self, execution: u64) -> Result<v1::ReleaseResponse, Status> {
        let mut flow = self.flow_of(execution);
        let mut request = v1::ReleaseRequest {
            r#execution_id: execution,
            ..v1::ReleaseRequest::default()
        };
        request.set_context(wire(flow.pending()));
        let response: v1::ReleaseResponse = self.try_raw("Release", &request)?;
        flow.advance();
        self.flows.insert(execution, flow);
        Ok(response)
    }

    /// Drives the node in rounds and polls the outcome until it is available.
    pub(crate) fn run_to_outcome(
        &mut self,
        node: &Rc<RefCell<Node>>,
        execution: u64,
        turn: u32,
    ) -> Observation {
        for _ in 0..400 {
            node.borrow_mut().drive(50);
            let response = self.query(execution, turn).expect("query");
            if response.r#available {
                return outcome_of(&response);
            }
        }
        panic!("execution did not finish");
    }
}

pub(crate) fn outcome_of(response: &v1::QueryOutcomeResponse) -> Observation {
    use v1::Outcome_::Kind;
    let outcome = response.outcome().cloned().expect("an outcome");
    let language = match outcome.r#kind.clone().expect("a kind") {
        Kind::Returned(tuple) => {
            Outcome::Returned(Tuple::from_wire(&tuple, limits()).expect("tuple"))
        }
        Kind::Yielded(tuple) => {
            Outcome::Yielded(Tuple::from_wire(&tuple, limits()).expect("tuple"))
        }
        Kind::Failed(failure) => Outcome::Failed(Failure::new(
            FailureCode::from_wire(failure.r#code),
            &failure.r#message,
        )),
    };
    assert_eq!(
        outcome.r#uncertain, response.r#uncertain,
        "the outcome and the reply carry one disposition"
    );
    Observation {
        outcome: language,
        disposition: Disposition::from_flag(outcome.r#uncertain),
    }
}

pub(crate) fn returned(observation: Observation) -> Vec<Value> {
    match observation.outcome {
        Outcome::Returned(tuple) => tuple.values().to_vec(),
        other => panic!("expected a return, got {other:?}"),
    }
}

pub(crate) fn failure_message(observation: Observation) -> String {
    match observation.outcome {
        Outcome::Failed(failure) => String::from_utf8_lossy(&failure.message).into_owned(),
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[test]
fn a_describe_reports_the_contract_and_the_node_epoch() {
    let node = agent_node(7, None);
    let mut client = Client::new(node);
    assert_eq!(client.epoch, 7);
    let described: v1::DescribeResponse = client.raw("Describe", &v1::DescribeRequest::default());
    assert_eq!(described.r#contract, "example.fixture.remote");
    assert!(!described.r#has_child);
}

#[test]
fn a_returned_tuple_keeps_nil_and_bytes_through_upload_start_resume_and_query() {
    let workflow = site_zero("remote:spawn(function() return nil, \"x\", true end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node(3, None);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");

    let mut status_request = v1::StatusRequest {
        r#execution_id: execution,
        ..v1::StatusRequest::default()
    };
    status_request.set_context(client.reading(execution));
    let status: v1::StatusResponse = client.raw("Status", &status_request);
    assert_eq!(
        status.r#state,
        v1::State::Ready,
        "start creates a ready record"
    );

    client.resume(execution, 0, vec![]).expect("resume");
    let outcome = client.run_to_outcome(&node, execution, 0);
    assert_eq!(
        returned(outcome),
        vec![
            Value::Nil,
            Value::Bytes(b"x".to_vec()),
            Value::Boolean(true)
        ]
    );
}

#[test]
fn a_chain_runs_the_child_and_returns_its_tuple_with_a_host_capture() {
    let workflow = site_zero(
        "local count = 4\nlocal task = remote:spawn(function()\n    local value = remote:spawn(function()\n        local enabled = chip.model()\n        return enabled, count, \"sample\"\n    end)\n    return value:await()\nend)\nreturn task\n",
    );
    assert_eq!(workflow.sites[0].captures, vec!["count".to_string()]);
    let artifact = workflow.sites[0].artifact.clone();

    let dut = dut_node(11);
    let agent = agent_node(5, Some(immediate(Loopback::new(dut.clone()))));
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![Value::Integer(4)], vec![])
        .expect("start with the host snapshot");
    client.resume(execution, 0, vec![]).expect("resume");
    let outcome = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(
        returned(outcome),
        vec![
            Value::Boolean(true),
            Value::Integer(4),
            Value::Bytes(b"sample".to_vec())
        ]
    );
}

#[test]
fn a_yield_stops_the_execution_and_the_reply_becomes_the_yield_result() {
    // Yield is the reduced profile's coroutine.yield at the entry, so the bytecode is the workflow's.
    let workflow = site_zero(
        "remote:spawn(function()\n    local v = coroutine.yield(1)\n    return v\nend)\n",
    );
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node(2, None);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("first resume");
    match client.run_to_outcome(&node, execution, 0).outcome {
        Outcome::Yielded(tuple) => assert_eq!(tuple.values(), &[Value::Integer(1)]),
        other => panic!("expected a yield, got {other:?}"),
    }
    client
        .resume(execution, 1, vec![Value::Integer(9)])
        .expect("resume with a reply");
    assert_eq!(
        returned(client.run_to_outcome(&node, execution, 1)),
        vec![Value::Integer(9)]
    );
}

#[test]
fn a_replayed_start_returns_the_recorded_answer_and_creates_no_second_execution() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node(4, None);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    let flow = client.open_flow();
    let request = {
        let mut r = v1::StartRequest {
            r#digest: digest(&artifact).to_vec(),
            ..v1::StartRequest::default()
        };
        r.set_context(wire(flow.pending()));
        r.set_captures(Tuple::empty().to_wire());
        r.set_arguments(Tuple::empty().to_wire());
        r
    };
    let first: v1::StartResponse = client.raw("Start", &request);
    let second: v1::StartResponse = client.raw("Start", &request);
    assert_eq!(
        first.r#execution_id, second.r#execution_id,
        "the retransmission replays the answer"
    );
    assert_eq!(node.borrow().contract(), "example.fixture.remote");
}

#[test]
fn a_conflicting_payload_on_a_used_sequence_is_refused() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node(4, None);
    let mut client = Client::new(node);
    client.upload(&artifact).expect("upload");
    let flow = client.open_flow();
    let mut first = v1::StartRequest {
        r#digest: digest(&artifact).to_vec(),
        ..v1::StartRequest::default()
    };
    first.set_context(wire(flow.pending()));
    first.set_captures(Tuple::empty().to_wire());
    first.set_arguments(Tuple::empty().to_wire());
    let _: v1::StartResponse = client.raw("Start", &first);
    let mut changed = first.clone();
    changed.set_arguments(
        Tuple::new(vec![Value::Integer(1)], limits())
            .expect("fits")
            .to_wire(),
    );
    let refused = client
        .try_raw::<_, v1::StartResponse>("Start", &changed)
        .expect_err("conflict");
    assert_eq!(refused.code, Code::FailedPrecondition);
}

#[test]
fn a_stale_epoch_is_refused_and_a_corrupt_artifact_never_becomes_executable() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node(9, None);
    let mut client = Client::new(node);
    let mut stale = v1::StatusRequest {
        r#execution_id: 1,
        ..v1::StatusRequest::default()
    };
    stale.set_context(v1::Context {
        r#epoch: 8,
        r#session_id: 1,
        r#sequence: 0,
    });
    let refused = client
        .try_raw::<_, v1::StatusResponse>("Status", &stale)
        .expect_err("stale");
    assert_eq!(refused.code, Code::FailedPrecondition);

    let mut corrupt = artifact.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0x55;
    let mut bad_upload = Client::new(agent_node(9, None));
    let refused = bad_upload.upload(&corrupt).expect_err("digest mismatch");
    assert_eq!(refused.code, Code::InvalidArgument);
}

#[test]
fn an_extra_first_resume_value_is_refused_and_the_record_is_unchanged() {
    let workflow = site_zero("remote:spawn(function(a) return a end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node(6, None);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    let refused = client
        .resume(execution, 0, vec![Value::Integer(1)])
        .expect_err("extra arguments");
    assert_eq!(refused.code, Code::FailedPrecondition);
    let mut status_request = v1::StatusRequest {
        r#execution_id: execution,
        ..v1::StatusRequest::default()
    };
    status_request.set_context(client.reading(execution));
    let status: v1::StatusResponse = client.raw("Status", &status_request);
    assert_eq!(status.r#state, v1::State::Ready);
}

#[test]
fn a_child_that_cannot_be_created_is_a_plain_failure_because_nothing_ran() {
    let workflow = site_zero(
        "remote:spawn(function()\n    local v = remote:spawn(function() return 1 end)\n    return v:await()\nend)\n",
    );
    let artifact = workflow.sites[0].artifact.clone();
    let agent = agent_node(8, Some(immediate(Unreachable)));
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    let observed = client.run_to_outcome(&agent, execution, 0);
    match observed.outcome {
        Outcome::Failed(failure) => {
            assert!(
                !observed.disposition.is_uncertain(),
                "a child that was never created has no side effect to reconcile"
            );
            assert!(
                failure
                    .message
                    .starts_with(b"the child is unavailable; nothing ran"),
                "{:?}",
                String::from_utf8_lossy(&failure.message)
            );
        }
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[test]
fn a_spawn_on_a_child_with_no_session_id_left_fails_before_anything_runs() {
    let workflow = site_zero(
        "remote:spawn(function()\n    local v = remote:spawn(function() return 1 end)\n    return v:await()\nend)\n",
    );
    let artifact = workflow.sites[0].artifact.clone();
    let child = ExhaustedChild(Loopback::new(dut_node(3)));
    let agent = agent_node(8, Some(immediate(child)));
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    let observed = client.run_to_outcome(&agent, execution, 0);
    match observed.outcome {
        Outcome::Failed(failure) => {
            assert!(
                !observed.disposition.is_uncertain(),
                "no child session was opened, so no side effect can have happened"
            );
            let message = String::from_utf8_lossy(&failure.message);
            assert!(message.contains("no session id left"), "{message}");
        }
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[test]
fn a_lost_outcome_after_the_child_started_leaves_the_result_uncertain() {
    let workflow = site_zero(
        "remote:spawn(function()\n    local v = remote:spawn(function() return 1 end)\n    return v:await()\nend)\n",
    );
    let artifact = workflow.sites[0].artifact.clone();
    let dut = dut_node(13);
    let agent = agent_node(8, Some(immediate(LosesOutcomes(Loopback::new(dut)))));
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    let observed = client.run_to_outcome(&agent, execution, 0);
    match observed.outcome {
        Outcome::Failed(failure) => {
            assert!(
                observed.disposition.is_uncertain(),
                "the child may have run, so the result is unknown"
            );
            assert_eq!(failure.code, FailureCode::RuntimeError);
        }
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[test]
fn cancel_ends_a_running_execution_and_it_cannot_be_resumed() {
    let workflow =
        site_zero("remote:spawn(function() local n = 0 while true do n = n + 1 end end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node(12, None);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    node.borrow_mut().drive(3);
    let cancelled = client.cancel(execution).expect("cancel");
    assert_eq!(cancelled.r#state, v1::State::Failed);
    match client.run_to_outcome(&node, execution, 0).outcome {
        Outcome::Failed(failure) => assert_eq!(failure.code, FailureCode::Cancelled),
        other => panic!("expected a cancellation, got {other:?}"),
    }
}

#[test]
fn an_artifact_compiled_for_another_contract_is_refused_at_upload() {
    let workflow = site_zero(NESTED);
    let agent_bytes = workflow.sites[0].artifact.clone();
    let dut_bytes =
        protolink_lua_bytecode::Artifact::decode(&agent_bytes, &BytecodeLimits::profile())
            .expect("decodes")
            .children[0]
            .bytes
            .clone();
    let mut client = Client::new(agent_node(4, None));
    let refused = client
        .upload(&dut_bytes)
        .expect_err("the DUT artifact is not the agent's contract");
    assert_eq!(refused.code, Code::InvalidArgument);
    client
        .upload(&agent_bytes)
        .expect("the agent's own artifact is admitted");
}

#[test]
fn a_child_compiled_for_another_contract_is_refused_at_start() {
    let workflow = site_zero(NESTED);
    let artifact = workflow.sites[0].artifact.clone();
    let dut = dut_node(11);
    let agent = agent_node_with_routes(
        5,
        Some(immediate(Loopback::new(dut))),
        vec![route(contract_digest(AGENT_ID))],
    );
    let mut client = Client::new(agent);
    client.upload(&artifact).expect("upload");
    let refused = client
        .start(digest(&artifact), vec![], vec![])
        .expect_err("the child's contract is not the one the parent declares");
    assert_eq!(refused.code, Code::InvalidArgument);
}

#[test]
fn an_execution_holds_its_session_until_it_is_released() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node_with_sessions(1);
    let mut client = Client::new(node.clone());
    client
        .upload(&artifact)
        .expect("the upload is released at its commit");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start takes the only session");
    let refused = client
        .upload(&artifact)
        .expect_err("the only session is held by the execution");
    assert_eq!(refused.code, Code::ResourceExhausted);
    client
        .release(execution)
        .expect_err("a live execution cannot be released, and its session is kept");
    client.cancel(execution).expect("cancel");
    client.release(execution).expect("release after cancel");
    client
        .upload(&artifact)
        .expect("the released session frees the slot");
}

#[test]
fn a_released_session_refuses_any_further_call() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node_with_sessions(4);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    client.run_to_outcome(&node, execution, 0);
    client.release(execution).expect("release");
    let refused = client
        .release(execution)
        .expect_err("the session is retired");
    assert_eq!(
        refused.code,
        Code::Aborted,
        "a retired session never runs again"
    );
    assert!(refused.message.contains("has ended"), "{refused:?}");
}

#[test]
fn a_node_with_two_sessions_serves_many_executions_because_each_is_released() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node_with_sessions(2);
    let mut client = Client::new(node.clone());
    for round in 0..5 {
        client
            .upload(&artifact)
            .unwrap_or_else(|s| panic!("upload {round} failed: {s:?}"));
        let execution = client
            .start(digest(&artifact), vec![], vec![])
            .unwrap_or_else(|s| panic!("start {round} failed: {s:?}"));
        client.resume(execution, 0, vec![]).expect("resume");
        assert_eq!(
            returned(client.run_to_outcome(&node, execution, 0)),
            vec![Value::Integer(1)]
        );
        client.release(execution).expect("release");
    }
}

#[test]
fn a_refused_upload_releases_its_session_so_the_next_upload_is_admitted() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let mut corrupt = artifact.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0x55;
    let node = agent_node_with_sessions(1);
    let mut client = Client::new(node);
    let refused = client
        .upload(&corrupt)
        .expect_err("the corrupt artifact is refused at commit");
    assert_eq!(refused.code, Code::InvalidArgument);
    client
        .upload(&artifact)
        .expect("the refused commit still released its session");
}

#[test]
fn a_child_session_is_released_after_each_outcome_so_a_small_child_serves_many_spawns() {
    let workflow = site_zero(SEQUENTIAL_SPAWNS);
    let artifact = workflow.sites[0].artifact.clone();
    let dut = dut_node_with_sessions(11, 2);
    let agent = agent_node(5, Some(immediate(Loopback::new(dut.clone()))));
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    assert_eq!(
        returned(client.run_to_outcome(&agent, execution, 0)),
        vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
    );
}

#[test]
fn a_second_await_of_a_child_returns_its_settled_outcome() {
    let workflow = site_zero(TWICE_AWAITED);
    let artifact = workflow.sites[0].artifact.clone();
    let dut = dut_node_with_sessions(13, 1);
    let agent = agent_node(5, Some(immediate(Loopback::new(dut.clone()))));
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    assert_eq!(
        returned(client.run_to_outcome(&agent, execution, 0)),
        vec![Value::Integer(7), Value::Integer(7)]
    );
}

#[test]
fn a_released_execution_frees_its_record_so_a_single_slot_node_serves_again() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        Vec::new(),
        5,
    );
    settings.max_executions = 1;
    let node = build(settings, Box::new(NoNatives), None);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    for round in 0..3 {
        let execution = client
            .start(digest(&artifact), vec![], vec![])
            .unwrap_or_else(|s| panic!("start {round} failed: {s:?}"));
        client.resume(execution, 0, vec![]).expect("resume");
        assert_eq!(
            returned(client.run_to_outcome(&node, execution, 0)),
            vec![Value::Integer(1)]
        );
        client.release(execution).expect("release");
    }
}

#[test]
fn a_node_reports_a_session_floor_above_every_session_it_has_seen() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node_with_sessions(4);
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    client.run_to_outcome(&node, execution, 0);
    client.release(execution).expect("release");
    let described: v1::DescribeResponse = client.raw("Describe", &v1::DescribeRequest::default());
    assert_eq!(
        described.r#session_floor, 3,
        "the upload and the execution used sessions 1 and 2"
    );
}

/// A wire tuple of `strings` byte strings of 60 bytes each. Every string is within the test limit of 64 bytes, but the
/// total is over the limit of 256 bytes for `strings` of five or more, so only the aggregate bound refuses it.
fn strings_over_the_total(strings: usize) -> v1::Tuple {
    v1::Tuple {
        r#values: (0..strings)
            .map(|_| v1::Value {
                r#kind: Some(v1::Value_::Kind::Bytes(vec![0; 60])),
            })
            .collect(),
    }
}

/// Sends a raw Start, which the client does not check, and returns the refusal.
fn refused_start(
    client: &mut Client,
    artifact: &[u8],
    captures: v1::Tuple,
    arguments: v1::Tuple,
) -> Status {
    let flow = client.open_flow();
    let mut start = v1::StartRequest {
        r#digest: digest(artifact).to_vec(),
        ..v1::StartRequest::default()
    };
    start.set_context(wire(flow.pending()));
    start.set_captures(captures);
    start.set_arguments(arguments);
    client
        .try_raw::<_, v1::StartResponse>("Start", &start)
        .expect_err("the request is refused")
}

#[test]
fn start_tuples_whose_strings_add_up_past_the_total_are_refused_before_they_run() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let mut client = Client::new(agent_node(5, None));
    client.upload(&artifact).expect("upload");

    let captures = refused_start(
        &mut client,
        &artifact,
        strings_over_the_total(5),
        v1::Tuple::default(),
    );
    assert_eq!(captures.code, Code::InvalidArgument);
    let arguments = refused_start(
        &mut client,
        &artifact,
        v1::Tuple::default(),
        strings_over_the_total(5),
    );
    assert_eq!(arguments.code, Code::InvalidArgument);
}

#[test]
fn resume_values_whose_strings_add_up_past_the_total_are_refused_and_leave_no_gap() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let mut client = Client::new(agent_node(5, None));
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");

    let flow = client.flow_of(execution);
    let mut resume = v1::ResumeRequest {
        r#execution_id: execution,
        r#turn: 0,
        ..v1::ResumeRequest::default()
    };
    resume.set_context(wire(flow.pending()));
    resume.set_values(strings_over_the_total(5));
    let refused = client
        .try_raw::<_, v1::ResumeResponse>("Resume", &resume)
        .expect_err("the values are over the total");
    assert_eq!(refused.code, Code::InvalidArgument);

    // A refused call does not advance the session, so the next resume takes the sequence the refused one used.
    client
        .resume(execution, 0, vec![])
        .expect("the next resume is admitted");
}

/// A raw BeginUpload on `session` at its first sequence. The client does not choose the session, so tests can use any id.
fn begin_upload(epoch: u64, artifact: &[u8], session: u64) -> v1::BeginUploadRequest {
    let mut begin = v1::BeginUploadRequest {
        r#digest: digest(artifact).to_vec(),
        r#total_length: artifact.len() as u32,
        ..v1::BeginUploadRequest::default()
    };
    begin.set_context(wire(Flow::new(epoch, session).pending()));
    begin
}

#[test]
fn the_reserved_session_id_is_refused_and_the_top_usable_id_exhausts_the_floor() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let mut client = Client::new(agent_node(5, None));
    let epoch = client.epoch;

    let reserved = client
        .try_raw::<_, v1::BeginUploadResponse>(
            "BeginUpload",
            &begin_upload(epoch, &artifact, u64::MAX),
        )
        .expect_err("u64::MAX is reserved and is never a session");
    assert_eq!(reserved.code, Code::InvalidArgument);

    // A read carrying the reserved id is refused too, so the rule holds on every path.
    let mut read = v1::QueryOutcomeRequest {
        r#execution_id: 1,
        r#turn: 0,
        ..v1::QueryOutcomeRequest::default()
    };
    read.set_context(wire(Flow::new(epoch, u64::MAX).reading()));
    let refused_read = client
        .try_raw::<_, v1::QueryOutcomeResponse>("QueryOutcome", &read)
        .expect_err("a read on the reserved id is refused");
    assert_eq!(refused_read.code, Code::InvalidArgument);

    let top = begin_upload(epoch, &artifact, u64::MAX - 1);
    let opened: v1::BeginUploadResponse = client
        .try_raw("BeginUpload", &top)
        .expect("the highest usable id is a valid session");
    let again: v1::BeginUploadResponse = client
        .try_raw("BeginUpload", &top)
        .expect("a retransmission on the top usable id is replayed");
    assert_eq!(again.r#upload_id, opened.r#upload_id);

    let described: v1::DescribeResponse = client.raw("Describe", &v1::DescribeRequest::default());
    assert_eq!(
        described.r#session_floor,
        u64::MAX,
        "every usable id is recorded, so the floor is the reserved id"
    );

    let late = client
        .upload(&artifact)
        .expect_err("a delayed first call below the admission floor is refused");
    assert_eq!(late.code, Code::OutOfRange);
    assert_eq!(described.r#admission_floor, u64::MAX - 256);
}

#[test]
fn a_top_usable_session_that_goes_idle_stays_retired_and_the_floor_stays_reserved() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = agent_node(5, None);
    let mut client = Client::new(node.clone());
    node.borrow_mut().tick(0);
    let epoch = client.epoch;
    let begin = begin_upload(epoch, &artifact, u64::MAX - 1);
    client
        .try_raw::<_, v1::BeginUploadResponse>("BeginUpload", &begin)
        .expect("opened on the top usable id");

    // The idle timeout in `config` is 30 seconds, so this releases the session.
    node.borrow_mut().tick(30_000);
    let refused = client
        .try_raw::<_, v1::BeginUploadResponse>("BeginUpload", &begin)
        .expect_err("a released id never runs again");
    assert_eq!(refused.code, Code::Aborted);

    let described: v1::DescribeResponse = client.raw("Describe", &v1::DescribeRequest::default());
    assert_eq!(described.r#session_floor, u64::MAX);
}

/// An agent node with one upload slot and one execution slot, and the given idle timeout.
fn single_slot_agent(idle_ms: u32) -> Rc<RefCell<Node>> {
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        Vec::new(),
        5,
    );
    settings.max_uploads = 1;
    settings.max_executions = 1;
    settings.max_artifacts = 2;
    settings.session_idle_ms = idle_ms;
    build(settings, Box::new(NoNatives), None)
}

#[test]
fn an_upload_that_goes_silent_is_discarded_and_frees_its_slot() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = single_slot_agent(1_000);
    let mut client = Client::new(node.clone());
    node.borrow_mut().tick(0);

    // Open an upload and never send its remaining calls.
    let flow = client.open_flow();
    let mut begin = v1::BeginUploadRequest {
        r#digest: digest(&artifact).to_vec(),
        r#total_length: artifact.len() as u32,
        ..v1::BeginUploadRequest::default()
    };
    begin.set_context(wire(flow.pending()));
    let _: v1::BeginUploadResponse = client.raw("BeginUpload", &begin);

    node.borrow_mut().tick(999);
    let refused = client
        .upload(&artifact)
        .expect_err("the silent upload still holds the only slot");
    assert_eq!(refused.code, Code::ResourceExhausted);

    node.borrow_mut().tick(1_000);
    client
        .upload(&artifact)
        .expect("the abandoned upload was discarded, so its slot is free");
}

#[test]
fn an_execution_that_goes_silent_is_cancelled_and_its_slot_is_freed() {
    let workflow =
        site_zero("remote:spawn(function() local n = 0 while true do n = n + 1 end end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = single_slot_agent(1_000);
    let mut client = Client::new(node.clone());
    node.borrow_mut().tick(0);
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    node.borrow_mut().drive(3);

    node.borrow_mut().tick(1_000);
    let gone = client
        .query(execution, 0)
        .expect_err("the abandoned execution was removed");
    assert_eq!(gone.code, Code::NotFound);
    client
        .start(digest(&artifact), vec![], vec![])
        .expect("the removed execution freed its slot");
}

#[test]
fn a_poll_keeps_an_execution_alive_until_its_caller_goes_quiet() {
    let workflow =
        site_zero("remote:spawn(function() local n = 0 while true do n = n + 1 end end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = single_slot_agent(1_000);
    let mut client = Client::new(node.clone());
    node.borrow_mut().tick(0);
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");

    node.borrow_mut().tick(900);
    client
        .query(execution, 0)
        .expect("the poll reaches the execution");
    node.borrow_mut().tick(1_800);
    client
        .query(execution, 0)
        .expect("900 ms of silence is within the timeout");
    node.borrow_mut().tick(2_800);
    let gone = client
        .query(execution, 0)
        .expect_err("1000 ms of silence is past the timeout");
    assert_eq!(gone.code, Code::NotFound);
}

/// An agent node built from the test config, with `tune` applied before it is built.
fn tuned_agent(
    tune: impl FnOnce(&mut NodeConfig),
    child: Option<Box<dyn ChildLink>>,
) -> Rc<RefCell<Node>> {
    let routes = child
        .as_ref()
        .map_or_else(Vec::new, |_| vec![route(contract_digest(DUT_ID))]);
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        routes,
        5,
    );
    tune(&mut settings);
    build(settings, Box::new(NoNatives), child.map(children_on))
}

/// A busy-looping workflow, so an execution started from it keeps running until it is cancelled.
const BUSY: &str = "remote:spawn(function() local n = 0 while true do n = n + 1 end end)\n";

#[test]
fn the_store_evicts_the_least_recently_used_artifact_that_no_execution_runs() {
    let first = site_zero("remote:spawn(function() return 1 end)\n").sites[0]
        .artifact
        .clone();
    let second = site_zero("remote:spawn(function() return 2 end)\n").sites[0]
        .artifact
        .clone();
    let third = site_zero("remote:spawn(function() return 3 end)\n").sites[0]
        .artifact
        .clone();
    let node = tuned_agent(
        |settings| {
            settings.max_artifacts = 2;
            settings.max_executions = 1;
        },
        None,
    );
    let mut client = Client::new(node);
    client.upload(&first).expect("first");
    client.upload(&second).expect("second");
    client
        .upload(&third)
        .expect("the third upload evicts the least recently used artifact");

    let evicted = client
        .start(digest(&first), vec![], vec![])
        .expect_err("the first artifact was evicted");
    assert_eq!(evicted.code, Code::NotFound);
    let second_run = client
        .start(digest(&second), vec![], vec![])
        .expect("the second artifact is still stored");
    client.cancel(second_run).expect("cancel");
    client
        .release(second_run)
        .expect("release frees the only execution slot");
    client
        .start(digest(&third), vec![], vec![])
        .expect("the third artifact is stored");
}

#[test]
fn a_running_execution_keeps_its_artifact_in_the_store() {
    let busy = site_zero(BUSY).sites[0].artifact.clone();
    let other = site_zero("remote:spawn(function() return 2 end)\n").sites[0]
        .artifact
        .clone();
    let third = site_zero("remote:spawn(function() return 3 end)\n").sites[0]
        .artifact
        .clone();
    let node = tuned_agent(
        |settings| {
            settings.max_artifacts = 2;
            settings.max_executions = 1;
        },
        None,
    );
    let mut client = Client::new(node.clone());
    client.upload(&busy).expect("busy");
    let execution = client.start(digest(&busy), vec![], vec![]).expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    node.borrow_mut().drive(3);

    client.upload(&other).expect("other");
    client
        .upload(&third)
        .expect("the third upload evicts the idle artifact, not the running one");
    let full = client
        .start(digest(&busy), vec![], vec![])
        .expect_err("the only execution slot is busy");
    assert_eq!(
        full.code,
        Code::ResourceExhausted,
        "the running artifact is still stored"
    );
    let gone = client
        .start(digest(&other), vec![], vec![])
        .expect_err("the idle artifact was evicted");
    assert_eq!(gone.code, Code::NotFound);
}

#[test]
fn a_store_that_could_fill_with_running_artifacts_is_refused_at_construction() {
    // With no more artifact slots than executions, every slot could hold a running artifact, so a commit could have no
    // victim. Such a node is never built.
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(AGENT_ID),
        Vec::new(),
        5,
    );
    settings.max_artifacts = 1;
    settings.max_executions = 2;
    assert_eq!(
        Node::new(settings, Box::new(NoNatives), None, 0).err(),
        Some(crate::ConfigError::ArtifactStore)
    );
}

#[test]
fn a_spawn_whose_captures_exceed_the_child_slot_raises_in_the_parent() {
    let workflow = site_zero(
        "local text = \"0123456789\"\nlocal task = remote:spawn(function()\n    local value = remote:spawn(function() return text end)\n    return value:await()\nend)\nreturn task\n",
    );
    let artifact = workflow.sites[0].artifact.clone();
    let dut = dut_node(11);
    let agent = tuned_agent(
        |settings| {
            settings.routes[0].bounds = ChildBounds {
                max_capture_bytes: 4,
                max_result_bytes: 1 << 20,
            }
        },
        Some(immediate(Loopback::new(dut))),
    );
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(
            digest(&artifact),
            vec![Value::Bytes(b"0123456789".to_vec())],
            vec![],
        )
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    let message = failure_message(client.run_to_outcome(&agent, execution, 0));
    assert!(
        message.contains("spawn captures 10 bytes; the child slot allows 4"),
        "{message}"
    );
}

#[test]
fn a_child_result_over_the_slot_bound_fails_the_await() {
    let workflow = site_zero(
        "remote:spawn(function()\n    local value = remote:spawn(function() return \"0123456789\" end)\n    return value:await()\nend)\n",
    );
    let artifact = workflow.sites[0].artifact.clone();
    let dut = dut_node(13);
    let agent = tuned_agent(
        |settings| {
            settings.routes[0].bounds = ChildBounds {
                max_capture_bytes: 1 << 20,
                max_result_bytes: 4,
            }
        },
        Some(immediate(Loopback::new(dut))),
    );
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    let message = failure_message(client.run_to_outcome(&agent, execution, 0));
    assert!(
        message.contains("child result of 10 bytes exceeds the slot's 4 bytes"),
        "{message}"
    );
}

#[test]
fn a_start_whose_captures_exceed_the_node_bound_is_refused() {
    let workflow = site_zero("remote:spawn(function() return 1 end)\n");
    let artifact = workflow.sites[0].artifact.clone();
    let node = tuned_agent(|settings| settings.max_capture_bytes = 4, None);
    let mut client = Client::new(node);
    client.upload(&artifact).expect("upload");
    let refused = client
        .start(
            digest(&artifact),
            vec![Value::Bytes(b"0123456789".to_vec())],
            vec![],
        )
        .expect_err("the captures are larger than the node accepts");
    assert_eq!(refused.code, Code::InvalidArgument);
}

#[test]
fn a_chain_deeper_than_the_contract_allows_is_refused_at_upload() {
    let workflow = site_zero(NESTED);
    let agent_bytes = workflow.sites[0].artifact.clone();
    // NESTED embeds one child, so the agent's artifact nests one level. A node allowed no nesting refuses it.
    let node = tuned_agent(
        |settings| settings.bytecode.max_depth = 0,
        Some(immediate(Loopback::new(dut_node(17)))),
    );
    let mut client = Client::new(node);
    let refused = client
        .upload(&agent_bytes)
        .expect_err("the embedded chain is deeper than the contract allows");
    assert_eq!(refused.code, Code::InvalidArgument);
}
