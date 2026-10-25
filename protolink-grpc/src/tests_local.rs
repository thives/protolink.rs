//! Traversal of the local calls: receive and polling order, and removals and
//! admissions between steps. Calls are inserted directly into the map, which
//! is what the traversal reads.

use super::*;
use alloc::vec::Vec;
use protolink_alloc_track::{Phase, Window};

fn admit(calls: &mut LocalCalls, id: StreamId) {
    calls.accept(
        id,
        Route::new(MethodKind::BidiStreaming, 0),
        String::new(),
        Metadata::default(),
        None,
    );
}

fn calls_with(ids: &[StreamId]) -> LocalCalls {
    let mut calls = LocalCalls::default();
    for &id in ids {
        admit(&mut calls, id);
    }
    calls
}

/// The ids of one complete polling pass.
fn poll_pass(calls: &mut LocalCalls) -> Vec<StreamId> {
    let mut traversal = calls.poll_traversal();
    core::iter::from_fn(|| calls.next_id(&mut traversal)).collect()
}

/// The ids of one complete receive pass.
fn receive_pass(calls: &mut LocalCalls) -> Vec<StreamId> {
    let mut traversal = calls.receive_traversal();
    core::iter::from_fn(|| calls.next_id(&mut traversal)).collect()
}

#[test]
fn empty_map_yields_nothing_and_keeps_the_cursor() {
    let mut calls = LocalCalls::default();
    assert!(poll_pass(&mut calls).is_empty());
    assert!(receive_pass(&mut calls).is_empty());
    assert_eq!(calls.poll_cursor, 0);
}

#[test]
fn single_call_is_visited_by_every_pass() {
    let mut calls = calls_with(&[5]);
    for _ in 0..3 {
        assert_eq!(poll_pass(&mut calls), [5]);
        assert_eq!(calls.poll_cursor, 5);
    }
    assert_eq!(receive_pass(&mut calls), [5]);
}

#[test]
fn receive_pass_is_ascending_and_keeps_the_polling_rotation() {
    let mut calls = calls_with(&[5, 1, 3]);
    calls.poll_cursor = 3;
    assert_eq!(receive_pass(&mut calls), [1, 3, 5]);
    assert_eq!(calls.poll_cursor, 3);
}

#[test]
fn polling_passes_rotate_through_every_call() {
    let mut calls = calls_with(&[1, 3, 5]);
    let passes: [[StreamId; 3]; 4] = [[1, 3, 5], [3, 5, 1], [5, 1, 3], [1, 3, 5]];
    for (pass, expected) in passes.into_iter().enumerate() {
        assert_eq!(poll_pass(&mut calls), expected, "pass {pass}");
        // The cursor is the first call visited, not the last.
        assert_eq!(calls.poll_cursor, expected[0], "pass {pass}");
    }
}

#[test]
fn polling_starts_after_a_cursor_whose_call_was_removed() {
    let mut calls = calls_with(&[1, 3, 5]);
    calls.poll_cursor = 3;
    calls.calls.remove(&3);
    assert_eq!(poll_pass(&mut calls), [5, 1]);
    assert_eq!(calls.poll_cursor, 5);

    // A cursor that was never a call of this map behaves the same way.
    let mut calls = calls_with(&[1, 3, 5]);
    calls.poll_cursor = 4;
    assert_eq!(poll_pass(&mut calls), [5, 1, 3]);
    assert_eq!(calls.poll_cursor, 5);
}

#[test]
fn removing_the_current_call_mid_pass_skips_nothing() {
    // Cursor 3 visits 5 and 7, then wraps to 1 and 3. The call just visited is
    // removed each time, which is what finishing a call does.
    let mut calls = calls_with(&[1, 3, 5, 7]);
    calls.poll_cursor = 3;
    let mut traversal = calls.poll_traversal();
    let mut visited = Vec::new();
    while let Some(id) = calls.next_id(&mut traversal) {
        visited.push(id);
        calls.calls.remove(&id);
    }
    assert_eq!(visited, [5, 7, 1, 3]);
    assert_eq!(calls.poll_cursor, 5);
    assert!(calls.calls.is_empty());
}

#[test]
fn removing_the_next_call_mid_pass_skips_only_that_call() {
    let mut calls = calls_with(&[1, 3, 5, 7]);
    let mut traversal = calls.poll_traversal();
    let mut visited = Vec::new();
    while let Some(id) = calls.next_id(&mut traversal) {
        visited.push(id);
        if id == 3 {
            calls.calls.remove(&5);
        }
    }
    assert_eq!(visited, [1, 3, 7]);

    // Across the wrap: 5 and 7 come first, and the call after 7 is 1.
    let mut calls = calls_with(&[1, 3, 5, 7]);
    calls.poll_cursor = 3;
    let mut traversal = calls.poll_traversal();
    let mut visited = Vec::new();
    while let Some(id) = calls.next_id(&mut traversal) {
        visited.push(id);
        if id == 7 {
            calls.calls.remove(&1);
        }
    }
    assert_eq!(visited, [5, 7, 3]);
}

#[test]
fn removing_the_wrap_boundary_neither_ends_nor_repeats_the_wrap() {
    // Cursor 3 visits 5 and 7, then wraps to 1 and 3. The cursor's own call and
    // the first call after the wrap are both removed mid-pass in turn.
    for (at, gone, expected) in [(5, 3, [5, 7, 1]), (7, 1, [5, 7, 3])] {
        let mut calls = calls_with(&[1, 3, 5, 7]);
        calls.poll_cursor = 3;
        let mut traversal = calls.poll_traversal();
        let mut visited = Vec::new();
        while let Some(id) = calls.next_id(&mut traversal) {
            visited.push(id);
            if id == at {
                calls.calls.remove(&gone);
            }
        }
        assert_eq!(visited, expected, "removing {gone} at {at}");
    }
}

#[test]
fn clearing_the_map_midway_ends_the_pass() {
    let mut calls = calls_with(&[1, 3, 5, 7]);
    let mut traversal = calls.poll_traversal();
    let mut visited = Vec::new();
    while let Some(id) = calls.next_id(&mut traversal) {
        visited.push(id);
        if id == 3 {
            calls.calls.clear();
        }
    }
    assert_eq!(visited, [1, 3]);
    assert_eq!(calls.poll_cursor, 1);
    assert!(poll_pass(&mut calls).is_empty());
    assert_eq!(calls.poll_cursor, 1);
}

/// Every pair of removals, at every step of the pass and at every cursor, of a
/// small map. Each call that survives is visited exactly once, no call is
/// visited twice, the cursor is the first call visited, and every walk ends.
#[test]
fn wraparound_visits_each_surviving_call_once_and_terminates() {
    const IDS: [StreamId; 5] = [1, 3, 5, 7, 9];
    // Removing `victim` after the `step`-th call of the pass is yielded.
    let mut moves: Vec<Option<(usize, StreamId)>> = alloc::vec![None];
    for step in 0..IDS.len() {
        moves.extend(IDS.iter().map(|&victim| Some((step, victim))));
    }
    for cursor in 0..=10 {
        for first in &moves {
            for second in &moves {
                let mut calls = calls_with(&IDS);
                calls.poll_cursor = cursor;
                let mut traversal = calls.poll_traversal();
                let mut visited = Vec::new();
                let mut gone = Vec::new();
                while let Some(id) = calls.next_id(&mut traversal) {
                    visited.push(id);
                    assert!(
                        visited.len() <= IDS.len(),
                        "cursor {cursor}: walk did not terminate"
                    );
                    for (step, victim) in [first, second].into_iter().flatten() {
                        if *step + 1 == visited.len() && calls.calls.remove(victim).is_some() {
                            gone.push(*victim);
                        }
                    }
                }
                let case = alloc::format!("cursor {cursor}, removals {first:?} and {second:?}");
                let mut distinct = visited.clone();
                distinct.sort_unstable();
                distinct.dedup();
                assert_eq!(distinct.len(), visited.len(), "duplicate visit: {case}");
                for id in IDS.iter().filter(|id| !gone.contains(id)) {
                    assert!(visited.contains(id), "survivor {id} skipped: {case}");
                }
                if let Some(first) = visited.first() {
                    assert_eq!(calls.poll_cursor, *first, "{case}");
                }
            }
        }
    }
}

#[test]
fn calls_admitted_between_passes_join_the_rotation() {
    let mut calls = calls_with(&[1, 3]);
    assert_eq!(poll_pass(&mut calls), [1, 3]);
    admit(&mut calls, 2);
    assert_eq!(poll_pass(&mut calls), [2, 3, 1]);
    admit(&mut calls, 0);
    assert_eq!(poll_pass(&mut calls), [3, 0, 1, 2]);
}

#[test]
fn a_call_admitted_mid_pass_is_visited_only_if_it_is_ahead_of_the_walk() {
    // 3 sorts after the last id visited, so this pass reaches it.
    let mut calls = calls_with(&[1, 5, 9]);
    let mut traversal = calls.poll_traversal();
    let mut visited = Vec::new();
    while let Some(id) = calls.next_id(&mut traversal) {
        visited.push(id);
        if id == 1 {
            admit(&mut calls, 3);
        }
    }
    assert_eq!(visited, [1, 3, 5, 9]);

    // 3 sorts before the last id visited, so this pass does not, and the next
    // pass from cursor 1 does.
    let mut calls = calls_with(&[1, 5, 9]);
    let mut traversal = calls.poll_traversal();
    let mut visited = Vec::new();
    while let Some(id) = calls.next_id(&mut traversal) {
        visited.push(id);
        if id == 5 {
            admit(&mut calls, 3);
        }
    }
    assert_eq!(visited, [1, 5, 9]);
    assert_eq!(poll_pass(&mut calls), [3, 5, 9, 1]);
}

/// Asserts that a closed window saw no allocation and no reallocation. The
/// message is formatted only on failure.
fn assert_no_allocation(phase: Phase, pass: &str) {
    assert_eq!(
        (phase.allocations, phase.reallocations),
        (0, 0),
        "{pass}: {phase:?}"
    );
}

/// Ids a cold traversal can visit. The tests here visit four calls.
const VISITED: usize = 8;

/// Exhausts `traversal` into fixed storage, so the walk itself has no
/// allocations to measure.
fn exhaust(calls: &mut LocalCalls, mut traversal: Traversal) -> ([StreamId; VISITED], usize) {
    let mut visited = [0; VISITED];
    let mut count = 0;
    while let Some(id) = calls.next_id(&mut traversal) {
        visited[count] = id;
        count += 1;
    }
    (visited, count)
}

// The cold traversal tests each use a fresh map, so no traversal warms up
// another's storage. The window opens before the traversal is created, and
// closes before any assertion, so the measurement covers construction and the
// complete walk and nothing else.

#[test]
fn first_receive_traversal_allocates_nothing() {
    let mut calls = calls_with(&[5, 1, 3, 7]);
    calls.poll_cursor = 3;

    let window = Window::open();
    let traversal = calls.receive_traversal();
    let (visited, count) = exhaust(&mut calls, traversal);
    let phase = window.checkpoint();
    drop(window);

    assert_no_allocation(phase, "first receive traversal");
    assert_eq!(&visited[..count], [1, 3, 5, 7]);
    assert_eq!(calls.poll_cursor, 3, "receive leaves the polling rotation");
}

#[test]
fn first_polling_traversal_allocates_nothing_and_wraps() {
    let mut calls = calls_with(&[1, 3, 5, 7]);
    calls.poll_cursor = 3;

    let window = Window::open();
    let traversal = calls.poll_traversal();
    let (visited, count) = exhaust(&mut calls, traversal);
    let phase = window.checkpoint();
    drop(window);

    assert_no_allocation(phase, "first polling traversal");
    assert_eq!(&visited[..count], [5, 7, 1, 3]);
    assert_eq!(calls.poll_cursor, 5, "the first call visited is the cursor");
}
