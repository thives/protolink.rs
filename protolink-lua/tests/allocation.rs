//! Allocation after the artifact is stored. A test thread can ask the allocator to refuse every allocation it makes; the
//! answer path must still build and record a commit answer, so none of it allocates after the effect.
//!
//! The refusal is a failure injection, not a measurement: with it on, any allocation on the measured thread returns null,
//! and the global allocator turns that into an abort, which fails the test. Other test threads are not affected.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use protolink_lua::ledger::{Answer, DIGEST_BYTES, Ledger, MAX_MUTATION_ANSWER_BYTES};
use protolink_lua::wire::v1::{self, Context};

thread_local! {
    static REFUSE: Cell<bool> = const { Cell::new(false) };
}

struct Injecting;

unsafe impl GlobalAlloc for Injecting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if REFUSE.try_with(Cell::get).unwrap_or(false) {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Injecting = Injecting;

/// Runs `f` with every allocation on this thread refused.
fn refusing<T>(f: impl FnOnce() -> T) -> T {
    REFUSE.with(|refuse| refuse.set(true));
    let result = f();
    REFUSE.with(|refuse| refuse.set(false));
    result
}

fn context(session: u64, sequence: u64) -> Context {
    Context {
        epoch: 1,
        session_id: session,
        sequence,
    }
}

#[test]
fn recording_a_commit_answer_allocates_nothing_after_the_store() {
    let mut ledger = Ledger::new(1, 4, MAX_MUTATION_ANSWER_BYTES, 64).expect("bounds are valid");
    let digest = [0x5a; DIGEST_BYTES];
    let ctx = context(9, 1);
    // The reservation holds the answer storage, and it is taken before the effect, as the node does.
    let reservation = ledger.reserve(&ctx, digest).expect("admitted");
    let recorded = refusing(|| {
        // The effect has happened by this point. Building and recording the answer must not need memory.
        let answer = Answer::commit(v1::Context { epoch: 1, ..ctx }, &digest);
        ledger.record(reservation, &answer)
    });
    assert!(recorded, "the genuine reservation records");
}

#[test]
fn finishing_a_final_commit_allocates_nothing_after_the_store() {
    let mut ledger = Ledger::new(1, 4, MAX_MUTATION_ANSWER_BYTES, 64).expect("bounds are valid");
    let digest = [0x3c; DIGEST_BYTES];
    let ctx = context(10, 1);
    let reservation = ledger.reserve(&ctx, digest).expect("admitted");
    let finished = refusing(|| {
        let answer = Answer::commit(ctx, &digest);
        ledger.finish(reservation, &answer)
    });
    assert!(finished);
    // The final table keeps the answer for replay, and that replay needs no allocation either.
    let replay = refusing(|| ledger.admit(&ctx, &digest).clone());
    assert!(matches!(replay, protolink_lua::ledger::Admission::Replay(bytes) if !bytes.is_empty()));
}

#[test]
fn every_bounded_answer_is_built_without_allocating() {
    let refused_context = Context {
        epoch: u64::MAX,
        session_id: u64::MAX - 1,
        sequence: u64::MAX,
    };
    let mut begin = v1::BeginUploadResponse {
        r#upload_id: u64::MAX,
        ..Default::default()
    };
    begin.set_context(refused_context);
    let mut chunk = v1::PutChunkResponse {
        r#received: u32::MAX,
        ..Default::default()
    };
    chunk.set_context(refused_context);
    let answers = refusing(|| {
        [
            Answer::encode(&begin),
            Answer::encode(&chunk),
            Answer::commit(refused_context, &[0xff; DIGEST_BYTES]),
        ]
    });
    assert!(
        answers
            .iter()
            .all(|answer| answer.as_slice().len() <= MAX_MUTATION_ANSWER_BYTES)
    );
}
