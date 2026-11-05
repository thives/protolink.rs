//! Quarantine at the child client: what the parent may and may not do with a child it has given up on.
//!
//! These tests drive [`LinkChildren`] directly, through the [`ChildClient`] trait, with immediate loopback links to real
//! child nodes. Each `poll` makes one protocol step, so every spawn settles in a known number of polls.

use std::cell::Cell;
use std::task::Poll;

use protolink_lua::child::{ChildError, ChildId, ChildOp, ChildReply, Ticket};
use protolink_lua::value::Tuple;
use protolink_lua_bytecode::Artifact;

use super::*;

/// A child whose native the test DUT provides. The agent's artifact for it has one child edge, which is the DUT artifact.
const EDGE_SOURCE: &str = "remote:spawn(function() return remote:spawn(function() return chip.model() end):await() end)\n";

/// The child artifact of the first child edge of [`EDGE_SOURCE`], as a parent forwards it to a child.
struct Edge {
    bytes: Rc<[u8]>,
    digest: [u8; 32],
    entry: u32,
}

fn edge() -> Edge {
    let agent = site_zero(EDGE_SOURCE).sites[0].artifact.clone();
    let artifact =
        Artifact::decode(&agent, &BytecodeLimits::profile()).expect("the agent artifact decodes");
    let child = &artifact.children[0];
    Edge {
        bytes: child.bytes.clone(),
        digest: child.digest,
        entry: child.entry,
    }
}

/// Counts the calls a link sends, so a test can show that a refused operation sent nothing.
struct Counting {
    inner: Box<dyn ChildLink>,
    sends: Rc<Cell<u32>>,
}

impl ChildLink for Counting {
    fn send(&mut self, path: &str, request: &[u8]) -> Result<(), LinkError> {
        self.sends.set(self.sends.get() + 1);
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

/// A loopback to a child node that can be replaced, as a reset replaces the node behind a slot. It records each path.
#[derive(Clone)]
struct Swappable {
    current: Rc<RefCell<Loopback>>,
    paths: Rc<RefCell<Vec<String>>>,
}

impl ImmediateCall for Swappable {
    fn call(&mut self, path: &str, request: &[u8]) -> Result<Vec<u8>, LinkError> {
        self.paths.borrow_mut().push(path.to_string());
        ImmediateCall::call(&mut *self.current.borrow_mut(), path, request)
    }

    fn drive(&mut self, rounds: u32) {
        ImmediateCall::drive(&mut *self.current.borrow_mut(), rounds);
    }
}

fn settings(max_children: usize, max_ops: usize) -> ChildSettings {
    ChildSettings {
        max_children,
        max_ops,
        ..child_settings()
    }
}

/// Runs one operation to its reply. The immediate link answers at once, so this takes a few polls at most.
fn settle(
    client: &mut LinkChildren,
    ticket: Ticket,
    now: &mut u64,
) -> Result<ChildReply, ChildError> {
    for _ in 0..1_000 {
        *now += 1;
        if let Poll::Ready(result) = client.poll(ticket, *now) {
            return result;
        }
    }
    panic!("the operation did not settle");
}

fn spawn(
    client: &mut LinkChildren,
    edge: &Edge,
    child: u64,
    slot: u32,
    now: &mut u64,
) -> Result<(), ChildError> {
    let ticket = client.submit(
        ChildOp::Spawn {
            child: ChildId(child),
            slot,
            artifact: edge.bytes.clone(),
            digest: edge.digest,
            entry: edge.entry,
            captures: Tuple::empty(),
            arguments: Tuple::empty(),
            lease_ms: 0,
        },
        *now + 10_000,
    )?;
    settle(client, ticket, now).map(|_| ())
}

fn child_on(dut: &Rc<RefCell<Node>>) -> Box<dyn ChildLink> {
    immediate(Loopback::new(dut.clone()))
}

#[test]
fn a_quarantined_child_keeps_its_slot_through_forget_and_refuses_every_operation() {
    let edge = edge();
    let dut = dut_node(91);
    let sends = Rc::new(Cell::new(0));
    let link = Counting {
        inner: child_on(&dut),
        sends: sends.clone(),
    };
    let mut client = LinkChildren::new(settings(1, 16), vec![(1, Box::new(link))]);
    let mut now = 0;
    spawn(&mut client, &edge, 1, 1, &mut now).expect("the child starts");
    assert_eq!(client.known_children(), 1);

    client.lost(ChildId(1), now);
    assert_eq!(client.lost_children(), 1);
    assert_eq!(client.quarantined(), vec![(ChildId(1), 1)]);

    // Forgetting a quarantined child changes nothing: it is not released, and its identity stays.
    client.forget(ChildId(1));
    assert_eq!(
        client.lost_children(),
        1,
        "forget does not free a quarantined slot"
    );
    assert_eq!(client.known_children(), 1);

    // Every operation on the child is refused, and none of them reaches the link.
    let sent_before = sends.get();
    for op in [
        ChildOp::Release { child: ChildId(1) },
        ChildOp::Cancel { child: ChildId(1) },
        ChildOp::KeepAlive { child: ChildId(1) },
        ChildOp::Query {
            child: ChildId(1),
            turn: 0,
        },
        ChildOp::CleanupQuery {
            child: ChildId(1),
            turn: 0,
        },
        ChildOp::Resume {
            child: ChildId(1),
            turn: 1,
            values: Tuple::empty(),
        },
    ] {
        assert_eq!(
            client.submit(op.clone(), now + 10),
            Err(ChildError::Quarantined),
            "{op:?}"
        );
    }
    assert_eq!(
        sends.get(),
        sent_before,
        "a refused operation sends nothing"
    );
    assert_eq!(client.operations(), 0);

    // The slot is still held, so a new child is refused for capacity.
    assert_eq!(
        spawn(&mut client, &edge, 2, 1, &mut now),
        Err(ChildError::Busy)
    );
    assert_eq!(client.lost_children(), 1);

    // Only explicit recovery frees it, and then a new child starts.
    assert!(client.recover_lost(ChildId(1)));
    assert!(
        !client.recover_lost(ChildId(1)),
        "a recovered child is not quarantined again"
    );
    assert_eq!(client.lost_children(), 0);
    spawn(&mut client, &edge, 2, 1, &mut now).expect("the freed slot starts a child");
}

#[test]
fn losing_a_child_with_an_operation_in_flight_drops_that_operation() {
    let edge = edge();
    let dut = dut_node(91);
    let mut client = LinkChildren::new(settings(4, 16), vec![(1, child_on(&dut))]);
    let mut now = 0;
    spawn(&mut client, &edge, 1, 1, &mut now).expect("the child starts");
    let ticket = client
        .submit(ChildOp::Cancel { child: ChildId(1) }, now + 10_000)
        .expect("admitted");
    assert_eq!(client.operations(), 1);
    client.lost(ChildId(1), now);
    assert_eq!(
        client.operations(),
        0,
        "the quarantined child has no operation left"
    );
    assert_eq!(
        client.poll(ticket, now),
        Poll::Ready(Err(ChildError::UnknownChild))
    );
}

#[test]
fn the_first_spawn_after_a_reset_describes_the_new_epoch_before_it_uploads() {
    let edge = edge();
    let first = dut_node(91);
    let current = Rc::new(RefCell::new(Loopback::new(first)));
    let paths = Rc::new(RefCell::new(Vec::new()));
    let swap = Swappable {
        current: current.clone(),
        paths: paths.clone(),
    };
    let mut client = LinkChildren::new(settings(4, 16), vec![(1, immediate(swap))]);
    let mut now = 0;
    spawn(&mut client, &edge, 1, 1, &mut now).expect("the child starts at epoch 91");
    client.lost(ChildId(1), now);

    // The child node behind the slot is replaced by one with a new epoch. The client still holds epoch 91.
    *current.borrow_mut() = Loopback::new(dut_node(92));

    // The embedding recovers the slot after the reset. The client forgets its description of the slot.
    assert_eq!(client.recover_slot(1), 1);
    assert_eq!(client.lost_children(), 0);

    paths.borrow_mut().clear();
    spawn(&mut client, &edge, 2, 1, &mut now).expect("the first spawn after the reset succeeds");
    let sent = paths.borrow().clone();
    assert_eq!(
        sent.first().map(String::as_str),
        Some("/lua.execution.v1.Execution/Describe"),
        "the first call after recovery describes the child: {sent:?}"
    );
    let describe = sent
        .iter()
        .position(|path| path.ends_with("Describe"))
        .expect("described");
    let upload = sent
        .iter()
        .position(|path| path.ends_with("BeginUpload"))
        .expect("uploaded");
    assert!(
        describe < upload,
        "the epoch is learned before anything is uploaded: {sent:?}"
    );
}

#[test]
fn a_route_with_only_quarantined_children_reserves_no_headroom() {
    let edge = edge();
    let first = dut_node(91);
    let second = dut_node(92);
    // Two routes, and four operations in all. A route that is active holds two (cleanup and heartbeat) for later use.
    let mut client = LinkChildren::new(
        settings(4, 4),
        vec![(1, child_on(&first)), (2, child_on(&second))],
    );
    let mut now = 0;
    spawn(&mut client, &edge, 1, 1, &mut now).expect("route one starts a child");
    client.lost(ChildId(1), now);

    // Route one has nothing active, so a spawn on route two needs only its own operation and its two headroom slots.
    spawn(&mut client, &edge, 2, 2, &mut now)
        .expect("route two is not starved by a quarantined route");
    assert_eq!(client.lost_children(), 1);
}

#[test]
fn an_active_route_keeps_its_cleanup_and_heartbeat_headroom() {
    let edge = edge();
    let first = dut_node(91);
    let second = dut_node(92);
    let mut client = LinkChildren::new(
        settings(4, 4),
        vec![(1, child_on(&first)), (2, child_on(&second))],
    );
    let mut now = 0;
    spawn(&mut client, &edge, 1, 1, &mut now).expect("route one starts a child");
    client.lost(ChildId(1), now);
    spawn(&mut client, &edge, 2, 2, &mut now).expect("route two starts a child");

    // Route two is active. A spawn on route one now needs its own slot, plus two for route one and two for route two,
    // which is five of four. It is refused, so route two keeps its cleanup and heartbeat headroom.
    assert_eq!(
        spawn(&mut client, &edge, 3, 1, &mut now),
        Err(ChildError::Busy),
        "an active route keeps its headroom"
    );
}
