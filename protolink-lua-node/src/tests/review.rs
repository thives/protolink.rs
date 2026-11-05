//! Regressions for the 2026-11-03 distributed review. Package A covers upload ownership and session retirement (N01,
//! N02). Package B covers natives that outlive their execution (N04) and lost children (N05). Package C covers upload
//! replies at the child client (N07).
//!
//! Each test sends raw calls with explicit contexts. A refused call does not consume its sequence number, as the
//! gateway's flow does not advance on a refusal, so a caller retries the same sequence after a refusal.

use std::cell::Cell;
use std::task::Poll;

use super::*;
use crate::{NativeCall, NativeStop, NativeTicket, RecoverError};

/// The chunk size `config` advertises.
const CHUNK: usize = 64;

fn artifact() -> Vec<u8> {
    let workflow = site_zero(SEQUENTIAL_SPAWNS);
    let artifact = workflow.sites[0].artifact.clone();
    assert!(
        artifact.len() > 2 * CHUNK,
        "the test needs at least two chunks"
    );
    artifact
}

fn context(epoch: u64, session: u64, sequence: u64) -> v1::Context {
    v1::Context {
        r#epoch,
        r#session_id: session,
        r#sequence,
    }
}

fn begin_request(context: v1::Context, artifact: &[u8]) -> v1::BeginUploadRequest {
    let mut request = v1::BeginUploadRequest {
        r#digest: digest(artifact).to_vec(),
        r#total_length: artifact.len() as u32,
        ..v1::BeginUploadRequest::default()
    };
    request.set_context(context);
    request
}

fn put_request(
    context: v1::Context,
    upload_id: u64,
    offset: usize,
    data: &[u8],
) -> v1::PutChunkRequest {
    let mut request = v1::PutChunkRequest {
        r#upload_id,
        r#offset: offset as u32,
        r#data: data.to_vec(),
        ..v1::PutChunkRequest::default()
    };
    request.set_context(context);
    request
}

fn commit_request(context: v1::Context, upload_id: u64) -> v1::CommitArtifactRequest {
    let mut request = v1::CommitArtifactRequest {
        r#upload_id,
        ..v1::CommitArtifactRequest::default()
    };
    request.set_context(context);
    request
}

/// Opens an upload on `session` at sequence 1 and returns its id.
fn begin(client: &mut Client, epoch: u64, session: u64, artifact: &[u8]) -> Result<u64, Status> {
    let response: v1::BeginUploadResponse = client.try_raw(
        "BeginUpload",
        &begin_request(context(epoch, session, 1), artifact),
    )?;
    Ok(response.r#upload_id)
}

/// Sends the chunks that cover `artifact[from..to]` on `session`, one call per chunk, numbering from `sequence`.
/// Returns the sequence the next call would use.
#[allow(clippy::too_many_arguments)]
fn send_range(
    client: &mut Client,
    epoch: u64,
    session: u64,
    mut sequence: u64,
    upload_id: u64,
    artifact: &[u8],
    from: usize,
    to: usize,
) -> Result<u64, Status> {
    for offset in (from..to).step_by(CHUNK) {
        let end = (offset + CHUNK).min(to);
        let request = put_request(
            context(epoch, session, sequence),
            upload_id,
            offset,
            &artifact[offset..end],
        );
        let _: v1::PutChunkResponse = client.try_raw("PutChunk", &request)?;
        sequence += 1;
    }
    Ok(sequence)
}

#[test]
fn a_foreign_session_cannot_write_into_another_sessions_upload() {
    let artifact = artifact();
    let mut client = Client::new(agent_node(5, None));
    let epoch = client.epoch;
    let upload = begin(&mut client, epoch, 1, &artifact).expect("the owner opens its upload");

    let stranger = client
        .try_raw::<_, v1::PutChunkResponse>(
            "PutChunk",
            &put_request(context(epoch, 2, 1), upload, 0, &artifact[..CHUNK]),
        )
        .expect_err("a session that did not open the upload cannot write to it");
    assert_eq!(stranger.code, Code::NotFound);

    // The refused chunk changed nothing, so the owner's first chunk still lands at offset 0.
    let landed: v1::PutChunkResponse = client.raw(
        "PutChunk",
        &put_request(context(epoch, 1, 2), upload, 0, &artifact[..CHUNK]),
    );
    assert_eq!(landed.r#received, CHUNK as u32);
}

#[test]
fn a_foreign_commit_neither_consumes_the_upload_nor_stores_an_artifact() {
    let artifact = artifact();
    let node = agent_node(5, None);
    let mut client = Client::new(node.clone());
    let epoch = client.epoch;
    let upload = begin(&mut client, epoch, 1, &artifact).expect("the owner opens its upload");
    let next = send_range(
        &mut client,
        epoch,
        1,
        2,
        upload,
        &artifact,
        0,
        artifact.len(),
    )
    .expect("every chunk lands");

    let stranger = client
        .try_raw::<_, v1::CommitArtifactResponse>(
            "CommitArtifact",
            &commit_request(context(epoch, 2, 1), upload),
        )
        .expect_err("a session that did not open the upload cannot commit it");
    assert_eq!(stranger.code, Code::NotFound);
    assert_eq!(node.borrow().usage().artifacts, 0, "nothing was stored");
    assert_eq!(
        node.borrow().usage().uploads,
        1,
        "the owner's complete upload is still staged"
    );

    let committed: v1::CommitArtifactResponse = client
        .try_raw(
            "CommitArtifact",
            &commit_request(context(epoch, 1, next), upload),
        )
        .expect("the owner commits its complete upload");
    assert_eq!(committed.r#digest, digest(&artifact).to_vec());
    assert_eq!(node.borrow().usage().artifacts, 1);
}

#[test]
fn a_commit_of_a_missing_upload_leaves_the_callers_own_upload_open() {
    let artifact = artifact();
    let node = agent_node(5, None);
    let mut client = Client::new(node.clone());
    let epoch = client.epoch;
    let upload = begin(&mut client, epoch, 1, &artifact).expect("the owner opens its upload");
    let next = send_range(&mut client, epoch, 1, 2, upload, &artifact, 0, CHUNK)
        .expect("the first chunk lands");

    let missing = client
        .try_raw::<_, v1::CommitArtifactResponse>(
            "CommitArtifact",
            &commit_request(context(epoch, 1, next), 9_999),
        )
        .expect_err("no upload has that id");
    assert_eq!(missing.code, Code::NotFound);

    // The refusal must not retire the session that still holds a live upload.
    let rest = send_range(
        &mut client,
        epoch,
        1,
        next,
        upload,
        &artifact,
        CHUNK,
        artifact.len(),
    )
    .expect("the owner's session is still live, so the rest of its upload lands");
    client
        .try_raw::<_, v1::CommitArtifactResponse>(
            "CommitArtifact",
            &commit_request(context(epoch, 1, rest), upload),
        )
        .expect("the owner commits its complete upload");
    let usage = node.borrow().usage();
    assert_eq!(usage.uploads, 0, "no staged upload is left behind");
    assert_eq!(usage.upload_bytes, 0);
}

#[test]
fn a_session_holds_one_upload_so_a_second_begin_is_refused_and_nothing_outlives_its_session() {
    let artifact = artifact();
    let node = agent_node(5, None);
    let mut client = Client::new(node.clone());
    let epoch = client.epoch;
    let first = begin(&mut client, epoch, 1, &artifact).expect("the first upload opens");

    let second = client
        .try_raw::<_, v1::BeginUploadResponse>(
            "BeginUpload",
            &begin_request(context(epoch, 1, 2), &artifact),
        )
        .expect_err("a session that holds an upload cannot open another");
    assert_eq!(second.code, Code::FailedPrecondition);
    assert_eq!(node.borrow().usage().uploads, 1);

    let next = send_range(
        &mut client,
        epoch,
        1,
        2,
        first,
        &artifact,
        0,
        artifact.len(),
    )
    .expect("the first upload completes");
    client
        .try_raw::<_, v1::CommitArtifactResponse>(
            "CommitArtifact",
            &commit_request(context(epoch, 1, next), first),
        )
        .expect("the commit succeeds");
    let usage = node.borrow().usage();
    assert_eq!(usage.uploads, 0, "the committed upload is gone");
    assert_eq!(usage.sessions, 0, "the session ended with its commit");
}

#[test]
fn a_refused_commit_reclaims_its_staged_bytes_and_ends_its_session() {
    let artifact = artifact();
    let node = agent_node(5, None);
    let mut client = Client::new(node.clone());
    let epoch = client.epoch;

    // An incomplete commit consumes the upload.
    let early = begin(&mut client, epoch, 1, &artifact).expect("open");
    let next = send_range(&mut client, epoch, 1, 2, early, &artifact, 0, CHUNK).expect("chunk");
    let incomplete = client
        .try_raw::<_, v1::CommitArtifactResponse>(
            "CommitArtifact",
            &commit_request(context(epoch, 1, next), early),
        )
        .expect_err("the upload is incomplete");
    assert_eq!(incomplete.code, Code::FailedPrecondition);
    let usage = node.borrow().usage();
    assert_eq!((usage.uploads, usage.upload_bytes), (0, 0));
    let after = client
        .try_raw::<_, v1::PutChunkResponse>(
            "PutChunk",
            &put_request(
                context(epoch, 1, next + 1),
                early,
                CHUNK,
                &artifact[CHUNK..2 * CHUNK],
            ),
        )
        .expect_err("the session ended with its commit");
    assert_eq!(after.code, Code::Aborted);

    // A commit whose bytes do not match the declared digest consumes the upload too.
    let mut corrupt = artifact.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0x55;
    let mismatched = begin(&mut client, epoch, 2, &artifact).expect("open");
    let sent = send_range(
        &mut client,
        epoch,
        2,
        2,
        mismatched,
        &corrupt,
        0,
        corrupt.len(),
    )
    .expect("the corrupt bytes fit the declared length");
    let refused = client
        .try_raw::<_, v1::CommitArtifactResponse>(
            "CommitArtifact",
            &commit_request(context(epoch, 2, sent), mismatched),
        )
        .expect_err("the digest does not match");
    assert_eq!(refused.code, Code::InvalidArgument);
    let usage = node.borrow().usage();
    assert_eq!(
        (usage.uploads, usage.upload_bytes, usage.sessions),
        (0, 0, 0)
    );
    assert_eq!(usage.artifacts, 0, "a refused artifact is never stored");
}

#[test]
fn a_retransmitted_chunk_gets_its_recorded_answer_and_a_changed_one_conflicts() {
    let artifact = artifact();
    let mut client = Client::new(agent_node(5, None));
    let epoch = client.epoch;
    let upload = begin(&mut client, epoch, 1, &artifact).expect("open");

    let request = put_request(context(epoch, 1, 2), upload, 0, &artifact[..CHUNK]);
    let first: v1::PutChunkResponse = client.raw("PutChunk", &request);
    let replay: v1::PutChunkResponse = client.raw("PutChunk", &request);
    assert_eq!(first.r#received, CHUNK as u32);
    assert_eq!(
        replay.r#received, CHUNK as u32,
        "the replay did not write the chunk again"
    );

    let changed = put_request(context(epoch, 1, 2), upload, 0, &artifact[CHUNK..2 * CHUNK]);
    let conflict = client
        .try_raw::<_, v1::PutChunkResponse>("PutChunk", &changed)
        .expect_err("a changed payload on a used sequence is refused");
    assert_eq!(conflict.code, Code::FailedPrecondition);
}

#[test]
fn a_session_that_holds_an_upload_cannot_start_an_execution() {
    let artifact = artifact();
    let node = agent_node(5, None);
    let mut client = Client::new(node.clone());
    let epoch = client.epoch;
    let _upload = begin(&mut client, epoch, 1, &artifact).expect("open");

    let start = |sequence: u64| {
        let mut request = v1::StartRequest {
            r#digest: digest(&artifact).to_vec(),
            ..v1::StartRequest::default()
        };
        request.set_context(context(epoch, 1, sequence));
        request
    };
    let reused = client
        .try_raw::<_, v1::StartResponse>("Start", &start(1))
        .expect_err("sequence 1 of this session was the upload");
    assert_eq!(reused.code, Code::FailedPrecondition);
    let fresh_only = client
        .try_raw::<_, v1::StartResponse>("Start", &start(2))
        .expect_err("an execution needs a fresh session");
    assert_eq!(fresh_only.code, Code::FailedPrecondition);
    assert_eq!(node.borrow().usage().executions, 0);
}

#[test]
fn idle_expiry_discards_only_the_idle_sessions_upload() {
    let artifact = artifact();
    let node = agent_node(5, None);
    let mut client = Client::new(node.clone());
    let epoch = client.epoch;
    node.borrow_mut().tick(0);
    let idle = begin(&mut client, epoch, 1, &artifact).expect("the idle session opens");
    let busy = begin(&mut client, epoch, 2, &artifact).expect("the busy session opens");

    node.borrow_mut().tick(20_000);
    let busy_next = send_range(&mut client, epoch, 2, 2, busy, &artifact, 0, CHUNK)
        .expect("the busy session writes");

    // Thirty seconds of silence expires session 1 only.
    node.borrow_mut().tick(31_000);
    assert_eq!(
        node.borrow().usage().uploads,
        1,
        "only the idle session's upload was discarded"
    );
    let expired = client
        .try_raw::<_, v1::PutChunkResponse>(
            "PutChunk",
            &put_request(context(epoch, 1, 2), idle, 0, &artifact[..CHUNK]),
        )
        .expect_err("the expired session never runs again");
    assert_eq!(expired.code, Code::Aborted);
    send_range(
        &mut client,
        epoch,
        2,
        busy_next,
        busy,
        &artifact,
        CHUNK,
        artifact.len(),
    )
    .expect("the busy upload continues and completes");
}

// -- Package B1 (N04): a native the node could not stop stays owned after its execution ends -----------------------

/// A native that never completes on its own, and whose cancel says it is still running.
struct RunningNative {
    polls: Rc<Cell<u32>>,
    finish: Rc<Cell<bool>>,
}

impl Platform for RunningNative {
    fn start(&mut self, _call: NativeCall) -> Result<NativeTicket, String> {
        Ok(NativeTicket(1))
    }

    fn poll(&mut self, _ticket: NativeTicket) -> Poll<Result<Option<Value>, String>> {
        self.polls.set(self.polls.get() + 1);
        if self.finish.get() {
            Poll::Ready(Ok(Some(Value::Boolean(true))))
        } else {
            Poll::Pending
        }
    }

    fn cancel(&mut self, _ticket: NativeTicket) -> NativeStop {
        NativeStop::Running
    }
}

/// Uploads site 0 of `source` to `node`, starts it and resumes it once, returning the client and the execution id.
fn start_on(node: &Rc<RefCell<Node>>, source: &str) -> (Client, u64) {
    let artifact = site_zero(source).sites[0].artifact.clone();
    let mut client = Client::new(node.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    (client, execution)
}

#[test]
fn a_native_the_node_could_not_stop_is_still_polled_after_its_execution_is_released() {
    let polls = Rc::new(Cell::new(0));
    let finish = Rc::new(Cell::new(false));
    let settings = config(
        "example.fixture.remote",
        contract_digest(DUT_ID),
        Vec::new(),
        71,
    );
    let dut = build(
        settings,
        Box::new(RunningNative {
            polls: polls.clone(),
            finish: finish.clone(),
        }),
        None,
    );
    let agent = agent_node(70, Some(immediate(Loopback::new(dut.clone()))));
    let (mut client, execution) = start_on(
        &agent,
        "remote:spawn(function() return remote:spawn(function() return chip.model() end):await() end)\n",
    );
    agent.borrow_mut().drive(50);
    assert!(polls.get() > 0, "the DUT's native was started and polled");

    client.cancel(execution).expect("cancel");
    agent.borrow_mut().drive(20);
    assert!(
        polls.get() > 0,
        "the cancel reached the DUT while its native was still running"
    );
    // The DUT's cleanup deadline is `cleanup_ms` after its cancel, which happened at its clock 0.
    dut.borrow_mut().tick(5_000);
    let observed = client.run_to_outcome(&agent, execution, 0);
    assert_eq!(observed.disposition, Disposition::Uncertain);
    client.release(execution).expect("release");
    agent.borrow_mut().drive(50);
    assert_eq!(
        dut.borrow().execution_count(),
        0,
        "the DUT's record was released by the agent"
    );

    let at_release = polls.get();
    agent.borrow_mut().drive(20);
    assert!(
        polls.get() > at_release,
        "the node keeps polling the native it could not stop: {at_release} polls at release, {} after",
        polls.get()
    );
    assert_eq!(
        dut.borrow().usage().parked_natives,
        1,
        "the parked native is counted"
    );

    finish.set(true);
    dut.borrow_mut().drive(5);
    assert_eq!(
        dut.borrow().usage().parked_natives,
        0,
        "a native that completes leaves the parked store"
    );
}

/// A native that counts its starts and never completes on its own. Its cancel says it is still running.
struct CountingRunningNative {
    starts: Rc<Cell<u32>>,
}

impl Platform for CountingRunningNative {
    fn start(&mut self, _call: NativeCall) -> Result<NativeTicket, String> {
        self.starts.set(self.starts.get() + 1);
        Ok(NativeTicket(u64::from(self.starts.get())))
    }

    fn poll(&mut self, _ticket: NativeTicket) -> Poll<Result<Option<Value>, String>> {
        Poll::Pending
    }

    fn cancel(&mut self, _ticket: NativeTicket) -> NativeStop {
        NativeStop::Running
    }
}

#[test]
fn a_native_is_refused_while_unresolved_natives_hold_every_slot_and_nothing_starts() {
    let starts = Rc::new(Cell::new(0));
    let mut settings = config(
        "example.fixture.remote",
        contract_digest(DUT_ID),
        Vec::new(),
        81,
    );
    settings.max_executions = 1;
    let dut = build(
        settings,
        Box::new(CountingRunningNative {
            starts: starts.clone(),
        }),
        None,
    );
    let agent = agent_node(80, Some(immediate(Loopback::new(dut.clone()))));
    let source = "remote:spawn(function() return remote:spawn(function() return chip.model() end):await() end)\n";

    let (mut first, execution) = start_on(&agent, source);
    agent.borrow_mut().drive(50);
    assert_eq!(starts.get(), 1);
    first.cancel(execution).expect("cancel");
    agent.borrow_mut().drive(20);
    dut.borrow_mut().tick(5_000);
    first.run_to_outcome(&agent, execution, 0);
    first.release(execution).expect("release");
    agent.borrow_mut().drive(50);
    assert_eq!(dut.borrow().usage().parked_natives, 1);
    assert_eq!(dut.borrow().execution_count(), 0, "the DUT's slot is free");

    let retry_artifact = site_zero(source).sites[0].artifact.clone();
    first.upload(&retry_artifact).expect("upload");
    let retried = first
        .start(digest(&retry_artifact), vec![], vec![])
        .expect("start");
    first.resume(retried, 0, vec![]).expect("resume");
    let observed = first.run_to_outcome(&agent, retried, 0);
    assert_eq!(
        starts.get(),
        1,
        "the new native was not started while the parked one holds the only slot"
    );
    assert!(
        matches!(observed.outcome, Outcome::Failed(_)),
        "the refused native fails its execution: {observed:?}"
    );
}

// -- Package C1 (N07): upload replies are checked before the flow advances, and unanswered uploads are unknown ----------

/// Which upload reply a [`TamperChild`] corrupts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tamper {
    /// BeginUpload answers with upload id 0.
    BadUploadId,
    /// The first PutChunk answers with a received count that is not the end of its chunk.
    ShortChunk,
    /// CommitArtifact answers with a digest other than the artifact's.
    BadDigest,
    /// Every PutChunk reply is lost after the child has received the chunk.
    LoseChunk,
}

/// Forwards to a real child and corrupts one reply as `mode` says. It logs every call, with the offset of each chunk.
struct TamperChild {
    inner: Loopback,
    mode: Tamper,
    log: Rc<RefCell<Vec<String>>>,
    chunks_answered: u32,
}

impl ImmediateCall for TamperChild {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        let method = path.rsplit('/').next().unwrap_or(path).to_string();
        let entry = match codec::decode::<v1::PutChunkRequest>(request) {
            Ok(put) if method == "PutChunk" => format!("PutChunk@{}", put.r#offset),
            _ => method.clone(),
        };
        self.log.borrow_mut().push(entry);
        let mut reply = ImmediateCall::call(&mut self.inner, path, request)?;
        match (self.mode, method.as_str()) {
            (Tamper::BadUploadId, "BeginUpload") => {
                let mut opened: v1::BeginUploadResponse = codec::decode(&reply).expect("begin");
                opened.r#upload_id = 0;
                reply = codec::encode(&opened);
            }
            (Tamper::ShortChunk, "PutChunk") => {
                self.chunks_answered += 1;
                if self.chunks_answered == 1 {
                    let mut put: v1::PutChunkResponse = codec::decode(&reply).expect("chunk");
                    put.r#received = 0;
                    reply = codec::encode(&put);
                }
            }
            (Tamper::BadDigest, "CommitArtifact") => {
                let mut committed: v1::CommitArtifactResponse =
                    codec::decode(&reply).expect("commit");
                committed.r#digest = vec![0; 32];
                reply = codec::encode(&committed);
            }
            (Tamper::LoseChunk, "PutChunk") => {
                return Err(LinkError::Lost(
                    "reply lost after the child received it".to_string(),
                ));
            }
            _ => {}
        }
        Ok(reply)
    }

    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut self.inner, rounds);
    }
}

/// A DUT placed under an agent, with a child whose artifact needs more than one chunk.
const MULTI_CHUNK_CHILD: &str = "local task = remote:spawn(function()\n    local value = remote:spawn(function()\n        local a = chip.model()\n        local b = chip.model()\n        local c = chip.model()\n        local d = chip.model()\n        local e = chip.model()\n        local f = chip.model()\n        return a, b, c, d, e, f\n    end)\n    return value:await()\nend)\nreturn task\n";

/// Runs [`MULTI_CHUNK_CHILD`] on an agent whose child link corrupts one upload reply as `mode` says.
fn run_with_tampered_child(mode: Tamper) -> (Rc<RefCell<Vec<String>>>, Observation) {
    let log = Rc::new(RefCell::new(Vec::new()));
    let dut = dut_node(72);
    let link = TamperChild {
        inner: Loopback::new(dut),
        mode,
        log: log.clone(),
        chunks_answered: 0,
    };
    let agent = agent_node(73, Some(immediate(link)));
    let (mut client, execution) = start_on(&agent, MULTI_CHUNK_CHILD);
    let observed = client.run_to_outcome(&agent, execution, 0);
    (log, observed)
}

#[test]
fn a_begin_reply_without_an_upload_id_is_never_used_for_a_chunk() {
    let (log, observed) = run_with_tampered_child(Tamper::BadUploadId);
    assert!(
        !log.borrow().iter().any(|call| call.starts_with("PutChunk")),
        "no chunk is sent for an upload id the child never issued: {:?}",
        log.borrow()
    );
    assert_eq!(observed.disposition, Disposition::Uncertain);
}

#[test]
fn a_chunk_reply_that_does_not_count_its_bytes_does_not_advance_the_upload() {
    let (log, observed) = run_with_tampered_child(Tamper::ShortChunk);
    let puts: Vec<String> = log
        .borrow()
        .iter()
        .filter(|call| call.starts_with("PutChunk"))
        .cloned()
        .collect();
    assert!(puts.len() >= 2, "the artifact needs two chunks: {puts:?}");
    assert_eq!(
        puts[1], puts[0],
        "the chunk is repeated, not sent at the next offset: {puts:?}"
    );
    assert_eq!(
        observed.disposition,
        Disposition::Certain,
        "the honest retransmission answers, so the run completes"
    );
}

#[test]
fn a_commit_reply_with_another_digest_never_starts_the_child() {
    let (log, observed) = run_with_tampered_child(Tamper::BadDigest);
    assert!(
        !log.borrow().iter().any(|call| call == "Start"),
        "no Start is sent for an artifact whose digest the child did not confirm: {:?}",
        log.borrow()
    );
    assert_eq!(observed.disposition, Disposition::Uncertain);
}

#[test]
fn a_lost_chunk_reply_after_send_is_unknown_not_unavailable() {
    let (_log, observed) = run_with_tampered_child(Tamper::LoseChunk);
    assert_eq!(
        observed.disposition,
        Disposition::Uncertain,
        "a chunk that was sent may have been received: {observed:?}"
    );
}

// -- Package B2 (N05): lost children keep their pool slots, and a full pool fails closed --------------------------------

/// Forwards every call, but loses the reply to every Release, as a link that drops that answer would. Counts Starts.
/// With `zero_idle`, the child also advertises no idle timeout.
struct LosesEveryRelease {
    inner: Loopback,
    starts: Rc<Cell<u32>>,
    zero_idle: bool,
}

impl ImmediateCall for LosesEveryRelease {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        if path.ends_with("Start") {
            self.starts.set(self.starts.get() + 1);
        }
        let reply = ImmediateCall::call(&mut self.inner, path, request);
        if path.ends_with("Release") {
            return Err(LinkError::Lost("release reply lost".to_string()));
        }
        if self.zero_idle && path.ends_with("Describe") {
            let mut described: v1::DescribeResponse = codec::decode(&reply?).expect("describe");
            described.r#session_idle_ms = 0;
            return Ok(codec::encode(&described));
        }
        reply
    }

    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut self.inner, rounds);
    }
}

/// Six sequential children per execution, each awaited, so the child client sees six operations per run.
const SIX_CHILDREN: &str = "local task = remote:spawn(function()\n    local count = 0\n    for i = 1, 6 do\n        local value = remote:spawn(function() return chip.model() end)\n        if pcall(function() return value:await() end) then\n            count = count + 1\n        end\n    end\n    return count\nend)\nreturn task\n";

/// Runs `rounds` executions of [`SIX_CHILDREN`] on one client, so its sessions keep counting up, and returns the Starts
/// the link saw and each execution's outcome.
fn run_six_child_rounds(
    client: &mut Client,
    agent: &Rc<RefCell<Node>>,
    starts: &Rc<Cell<u32>>,
    rounds: u32,
) -> (u32, Vec<Observation>) {
    let artifact = site_zero(SIX_CHILDREN).sites[0].artifact.clone();
    let mut observed = Vec::new();
    for _ in 0..rounds {
        client.upload(&artifact).expect("upload");
        let execution = client
            .start(digest(&artifact), vec![], vec![])
            .expect("start");
        client.resume(execution, 0, vec![]).expect("resume");
        let outcome = client.run_to_outcome(agent, execution, 0);
        client.release(execution).expect("release");
        observed.push(outcome);
    }
    (starts.get(), observed)
}

#[test]
fn lost_children_hold_their_slots_until_the_embedding_recovers_them() {
    let dut = dut_node(91);
    let starts = Rc::new(Cell::new(0));
    let link = LosesEveryRelease {
        inner: Loopback::new(dut),
        starts: starts.clone(),
        zero_idle: false,
    };
    let agent = agent_node(90, Some(immediate(link)));
    let mut client = Client::new(agent.clone());
    let (started, outcomes) = run_six_child_rounds(&mut client, &agent, &starts, 4);

    assert_eq!(
        started, 16,
        "the pool holds sixteen children whose release was lost, so the next spawns are refused"
    );
    assert_eq!(
        returned(outcomes[0].clone()),
        vec![Value::Integer(0)],
        "an await of a child whose release was lost is refused as unknown, so the round counts nothing"
    );
    assert_eq!(returned(outcomes[1].clone()), vec![Value::Integer(0)]);
    for outcome in &outcomes[2..] {
        assert!(
            matches!(outcome.outcome, Outcome::Failed(_)),
            "a full pool refuses the spawn, and the execution fails: {outcome:?}"
        );
        assert!(
            failure_message(outcome.clone()).contains("no room"),
            "the refusal names the full client: {}",
            failure_message(outcome.clone())
        );
    }
    for outcome in &outcomes[..3] {
        assert_eq!(
            outcome.disposition,
            Disposition::Uncertain,
            "a round that created a lost child is never falsely clean: {outcome:?}"
        );
    }
    assert_eq!(
        outcomes[3].disposition,
        Disposition::Certain,
        "the last round started nothing, so its refusal is a plain failure"
    );

    let (still_full, _) = run_six_child_rounds(&mut client, &agent, &starts, 1);
    assert_eq!(still_full, 16, "the lost children still hold their slots");

    // The parent's clock runs far past the child's advertised idle timeout of 30 seconds, and the child's clock does not
    // move. Elapsed time on the parent proves nothing about the child, so no slot is released.
    agent.borrow_mut().tick(30_001);
    agent.borrow_mut().tick(10_000_000);
    let (after_time, _) = run_six_child_rounds(&mut client, &agent, &starts, 1);
    assert_eq!(after_time, 16, "time alone released no slot");
    assert_eq!(agent.borrow().usage().lost_children, 16);
    assert_eq!(agent.borrow().usage().recovered_children, 0);

    // One child is recovered on the embedding's decision: exactly one slot is free, and it is counted as recovered.
    let quarantined = agent.borrow().lost_child_ids();
    assert_eq!(quarantined.len(), 16);
    let (first, slot) = quarantined[0];
    agent
        .borrow_mut()
        .recover_lost_child(first)
        .expect("quarantined");
    assert_eq!(
        agent.borrow_mut().recover_lost_child(first),
        Err(RecoverError::NotQuarantined),
        "a recovered child cannot be recovered twice"
    );
    assert_eq!(agent.borrow().usage().lost_children, 15);
    assert_eq!(agent.borrow().usage().recovered_children, 1);

    // A reset of the child node behind the slot releases the rest.
    assert_eq!(agent.borrow_mut().recover_lost_slot(slot), 15);
    assert_eq!(agent.borrow().usage().lost_children, 0);
    assert_eq!(agent.borrow().recovered_children(), 16);
    let (refilled, _) = run_six_child_rounds(&mut client, &agent, &starts, 1);
    assert_eq!(
        refilled, 22,
        "once the embedding recovered the slots, six more children start"
    );
}

#[test]
fn children_whose_release_completes_free_their_pool_slots() {
    let agent = agent_node(94, Some(immediate(Loopback::new(dut_node(93)))));
    let mut client = Client::new(agent.clone());
    let starts = Rc::new(Cell::new(0));
    let (_, observed) = run_six_child_rounds(&mut client, &agent, &starts, 4);
    let returned_rounds = observed
        .iter()
        .filter(|outcome| matches!(outcome.outcome, Outcome::Returned(_)))
        .count();
    assert_eq!(
        returned_rounds, 4,
        "every round returns: released children do not hold the pool"
    );
}

// -- Package G (bounds across repeated rounds) ----------------------------------------------------------------------------

/// A native-calling child under an agent, the same shape the N04 tests use.
const NATIVE_CHILD: &str = "remote:spawn(function() return remote:spawn(function() return chip.model() end):await() end)\n";

#[test]
fn parked_natives_never_exceed_their_slots_however_many_rounds_run() {
    let starts = Rc::new(Cell::new(0));
    let dut = build(
        config(
            "example.fixture.remote",
            contract_digest(DUT_ID),
            Vec::new(),
            101,
        ),
        Box::new(CountingRunningNative {
            starts: starts.clone(),
        }),
        None,
    );
    let agent = agent_node(100, Some(immediate(Loopback::new(dut.clone()))));
    let artifact = site_zero(NATIVE_CHILD).sites[0].artifact.clone();
    let mut client = Client::new(agent.clone());
    let slots = dut.borrow().config().max_executions;
    let mut clock = 0;
    for cycle in 0..(slots + 4) {
        client.upload(&artifact).expect("upload");
        let execution = client
            .start(digest(&artifact), vec![], vec![])
            .expect("start");
        client.resume(execution, 0, vec![]).expect("resume");
        agent.borrow_mut().drive(50);
        client.cancel(execution).expect("cancel");
        agent.borrow_mut().drive(20);
        clock += 5_000;
        dut.borrow_mut().tick(clock);
        client.run_to_outcome(&agent, execution, 0);
        client.release(execution).expect("release");
        agent.borrow_mut().drive(20);

        let parked = dut.borrow().usage().parked_natives;
        assert!(
            parked <= slots,
            "cycle {cycle}: {parked} parked natives, but only {slots} slots"
        );
    }
    assert_eq!(
        starts.get() as usize,
        slots,
        "once every slot holds a parked native, no further native starts"
    );
    assert_eq!(dut.borrow().usage().parked_natives, slots);
}

#[test]
fn lost_children_refill_the_pool_once_per_recovery_across_repeated_cycles() {
    let dut = dut_node(102);
    let starts = Rc::new(Cell::new(0));
    let link = LosesEveryRelease {
        inner: Loopback::new(dut),
        starts: starts.clone(),
        zero_idle: false,
    };
    let agent = agent_node(103, Some(immediate(link)));
    let mut client = Client::new(agent.clone());
    let mut previous = 0;
    let mut clock = 0;
    for cycle in 0..3 {
        run_six_child_rounds(&mut client, &agent, &starts, 4);
        let started = starts.get();
        assert_eq!(
            started - previous,
            16,
            "cycle {cycle}: the pool of sixteen is filled exactly once"
        );
        let usage = agent.borrow().usage();
        assert!(usage.executions <= 8, "cycle {cycle}: {usage:?}");
        assert!(usage.child_tasks <= 8 * 8, "cycle {cycle}: {usage:?}");
        assert_eq!(usage.lost_children, 16, "cycle {cycle}: {usage:?}");
        previous = started;
        clock += 31_001;
        agent.borrow_mut().tick(clock);
        assert_eq!(
            agent.borrow().usage().lost_children,
            16,
            "cycle {cycle}: a tick frees nothing"
        );
        let (_, slot) = agent.borrow().lost_child_ids()[0];
        assert_eq!(agent.borrow_mut().recover_lost_slot(slot), 16);
    }
    assert_eq!(agent.borrow().recovered_children(), 48);
}

#[test]
fn a_lease_that_expires_parks_a_native_the_node_cannot_stop() {
    let starts = Rc::new(Cell::new(0));
    let dut = build(
        config(
            "example.fixture.remote",
            contract_digest(DUT_ID),
            Vec::new(),
            105,
        ),
        Box::new(CountingRunningNative {
            starts: starts.clone(),
        }),
        None,
    );
    let agent = agent_node(104, Some(immediate(Loopback::new(dut.clone()))));
    let artifact = site_zero(NATIVE_CHILD).sites[0].artifact.clone();
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start_with_lease(digest(&artifact), vec![], vec![], 1_000)
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    agent.borrow_mut().drive(50);
    assert_eq!(starts.get(), 1, "the child's native started");

    agent.borrow_mut().tick(1_000);
    agent.borrow_mut().drive(20);
    dut.borrow_mut().tick(5_000);
    client.run_to_outcome(&agent, execution, 0);
    client.release(execution).expect("release");
    agent.borrow_mut().drive(20);

    assert_eq!(
        dut.borrow().usage().parked_natives,
        1,
        "the expired lease cancelled the child, and its native is parked, not dropped"
    );
}

#[test]
fn a_child_that_advertises_no_idle_timeout_is_refused_before_anything_starts() {
    let dut = dut_node(106);
    let starts = Rc::new(Cell::new(0));
    let link = LosesEveryRelease {
        inner: Loopback::new(dut),
        starts: starts.clone(),
        zero_idle: true,
    };
    let agent = agent_node(107, Some(immediate(link)));
    let mut client = Client::new(agent.clone());
    let (started, outcomes) = run_six_child_rounds(&mut client, &agent, &starts, 1);
    assert_eq!(started, 0, "no Start reaches a child with no idle timeout");
    assert!(
        failure_message(outcomes[0].clone()).contains("idle timeout"),
        "the refusal names the missing idle timeout: {}",
        failure_message(outcomes[0].clone())
    );
}

// -- Recovery of quarantined children: a session timeout is not proof that the child's work stopped -----------------------

#[test]
fn a_child_session_that_expires_while_its_native_runs_does_not_free_the_parents_slot() {
    let starts = Rc::new(Cell::new(0));
    let dut = build(
        config(
            "example.fixture.remote",
            contract_digest(DUT_ID),
            Vec::new(),
            111,
        ),
        Box::new(CountingRunningNative {
            starts: starts.clone(),
        }),
        None,
    );
    // A client that tracks one child, so a quarantined child fills it.
    let settings = ChildSettings {
        max_children: 1,
        ..child_settings()
    };
    let link = immediate(Loopback::new(dut.clone()));
    let agent = build(
        config(
            "example.fixture.remote",
            contract_digest(AGENT_ID),
            vec![route(contract_digest(DUT_ID))],
            110,
        ),
        Box::new(NoNatives),
        Some(Box::new(LinkChildren::new(settings, vec![(1, link)]))),
    );
    let artifact = site_zero(NATIVE_CHILD).sites[0].artifact.clone();
    let mut client = Client::new(agent.clone());
    client.upload(&artifact).expect("upload");
    let execution = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(execution, 0, vec![]).expect("resume");
    agent.borrow_mut().drive(50);
    assert_eq!(starts.get(), 1, "the child's native started");

    // The parent cancels. The child acknowledges, but its native cannot be stopped, so the child never reaches a terminal
    // state, and the parent gives up when its cleanup deadline passes.
    client.cancel(execution).expect("cancel");
    agent.borrow_mut().drive(20);
    agent.borrow_mut().tick(5_000);
    agent.borrow_mut().drive(20);
    assert_eq!(agent.borrow().usage().lost_children, 1);
    assert_eq!(agent.borrow().usage().cleanups, 0, "cleanup ended");

    // The child's own session now expires, and the native still runs. The parent's clock runs far past the child's idle
    // timeout as well.
    dut.borrow_mut().tick(5_000);
    dut.borrow_mut().tick(1_000_000);
    agent.borrow_mut().drive(20);
    agent.borrow_mut().tick(1_000_000);
    agent.borrow_mut().drive(20);
    assert_eq!(
        dut.borrow().usage().parked_natives,
        1,
        "the native outlived the session that started it"
    );
    assert_eq!(
        agent.borrow().usage().lost_children,
        1,
        "the parent still quarantines the child"
    );

    // New work is not admitted on the assumption that the native stopped. The first session went idle with the clock, so
    // the second execution opens a new session on the same client.
    client.upload(&artifact).expect("upload");
    let second = client
        .start(digest(&artifact), vec![], vec![])
        .expect("start");
    client.resume(second, 0, vec![]).expect("resume");
    let refused = client.run_to_outcome(&agent, second, 0);
    assert!(
        failure_message(refused).contains("no room"),
        "the spawn is refused while the child is quarantined"
    );

    // Recovery is the embedding's decision, and it frees the slot without claiming that the native stopped.
    let (child, _) = agent.borrow().lost_child_ids()[0];
    agent
        .borrow_mut()
        .recover_lost_child(child)
        .expect("quarantined");
    assert_eq!(agent.borrow().usage().lost_children, 0);
    assert_eq!(agent.borrow().usage().recovered_children, 1);
    assert_eq!(
        dut.borrow().usage().parked_natives,
        1,
        "recovery does not stop the native"
    );
}
