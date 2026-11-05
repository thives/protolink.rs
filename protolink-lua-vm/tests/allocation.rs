//! Allocation bounds at the VM boundary, measured with a counting global allocator.
//!
//! An export that breaks the wire limits must be refused before any value is copied. The fixture returns many references
//! to one large string, so a copy-first implementation would allocate the string once per reference before it noticed.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use protolink_lua::value::{Limits as WireLimits, Tuple, Value};
use protolink_lua_bytecode::{
    Artifact, ChildLink, Constant, Entry, Instr, Limits as BytecodeLimits, NativeRef, Proto, digest,
};
use protolink_lua_vm::{
    ControlError, Effect, EffectResult, Machine, Program, StartError, Step, VmLimits,
};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static LARGEST: AtomicUsize = AtomicUsize::new(0);
static GENERATION: AtomicUsize = AtomicUsize::new(0);
static ACTIVE: AtomicUsize = AtomicUsize::new(0);
static COUNTERS_LOCKED: AtomicBool = AtomicBool::new(false);
/// Allocations made by the measured thread so far, and the index of the one that fails. `usize::MAX` fails none.
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static FAIL_AT: AtomicUsize = AtomicUsize::new(usize::MAX);
thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
}
/// Tests run on several threads, and the counters are global, so measurements take turns.
static MEASURE: Mutex<()> = Mutex::new(());

// This lock must not allocate: it is also taken inside the global allocator.
fn with_counters<T>(f: impl FnOnce() -> T) -> T {
    while COUNTERS_LOCKED.swap(true, Ordering::Acquire) {
        std::hint::spin_loop();
    }
    let result = f();
    COUNTERS_LOCKED.store(false, Ordering::Release);
    result
}

// Store the measurement generation ahead of every allocation, including fixtures. This lets a
// free during measurement distinguish an old fixture from memory allocated in this measurement.
fn allocation_layout(layout: Layout) -> Option<(Layout, usize)> {
    Layout::new::<usize>().extend(layout).ok()
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let Some((full, offset)) = allocation_layout(layout) else {
            return std::ptr::null_mut();
        };
        // Fault injection: the measured thread's allocation number `FAIL_AT` is refused, as an exhausted allocator would.
        if TRACKING.try_with(Cell::get).unwrap_or(false)
            && ALLOCATIONS.fetch_add(1, Ordering::Relaxed) == FAIL_AT.load(Ordering::Relaxed)
        {
            return std::ptr::null_mut();
        }
        with_counters(|| {
            let base = unsafe { System.alloc(full) };
            if base.is_null() {
                return base;
            }
            let generation = if TRACKING.try_with(Cell::get).unwrap_or(false) {
                ACTIVE.load(Ordering::Relaxed)
            } else {
                0
            };
            unsafe { base.cast::<usize>().write(generation) };
            if generation != 0 {
                let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
                PEAK.fetch_max(live, Ordering::Relaxed);
                LARGEST.fetch_max(layout.size(), Ordering::Relaxed);
            }
            unsafe { base.add(offset) }
        })
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let (full, offset) = allocation_layout(layout).expect("valid allocation layout");
        let base = unsafe { ptr.sub(offset) };
        with_counters(|| {
            let generation = unsafe { base.cast::<usize>().read() };
            if generation != 0 && generation == ACTIVE.load(Ordering::Relaxed) {
                LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            }
            unsafe { System.dealloc(base, full) };
        });
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

struct Measurement;

impl Drop for Measurement {
    fn drop(&mut self) {
        TRACKING.with(|tracking| tracking.set(false));
        with_counters(|| ACTIVE.store(0, Ordering::Relaxed));
    }
}

/// Peak simultaneously live requested bytes allocated during `run`, and largest single request.
fn peak_of<T>(run: impl FnOnce() -> T) -> (T, usize, usize) {
    with_counters(|| {
        let mut generation = GENERATION.load(Ordering::Relaxed).wrapping_add(1);
        if generation == 0 {
            generation = 1;
        }
        GENERATION.store(generation, Ordering::Relaxed);
        LIVE.store(0, Ordering::Relaxed);
        PEAK.store(0, Ordering::Relaxed);
        LARGEST.store(0, Ordering::Relaxed);
        ACTIVE.store(generation, Ordering::Relaxed);
    });
    TRACKING.with(|tracking| tracking.set(true));
    let measurement = Measurement;
    let result = run();
    TRACKING.with(|tracking| tracking.set(false));
    let (peak, largest) = with_counters(|| {
        ACTIVE.store(0, Ordering::Relaxed);
        (
            PEAK.load(Ordering::Relaxed),
            LARGEST.load(Ordering::Relaxed),
        )
    });
    drop(measurement);
    (result, peak, largest)
}

#[test]
fn measurement_counts_live_bytes_not_cumulative_bytes_or_fixture_frees() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    let fixture_layout = Layout::from_size_align(4096, 8).unwrap();
    let layout = Layout::from_size_align(1024, 8).unwrap();
    let fixture = unsafe { std::alloc::alloc(fixture_layout) };
    assert!(!fixture.is_null());
    let (retained, peak, largest) = peak_of(|| unsafe {
        std::alloc::dealloc(fixture, fixture_layout);
        let first = std::alloc::alloc(layout);
        assert!(!first.is_null());
        std::hint::black_box(first);
        std::alloc::dealloc(first, layout);
        let second = std::alloc::alloc(layout);
        assert!(!second.is_null());
        std::hint::black_box(second)
    });
    assert_eq!((peak, largest), (1024, 1024));

    // An allocation retained from an earlier measurement is another pre-existing fixture.
    let (_, peak, largest) = peak_of(|| unsafe {
        std::alloc::dealloc(retained, layout);
        let first = std::alloc::alloc(layout);
        let second = std::alloc::alloc(layout);
        assert!(!first.is_null() && !second.is_null());
        std::hint::black_box((first, second));
        std::alloc::dealloc(first, layout);
        std::alloc::dealloc(second, layout);
    });
    assert_eq!((peak, largest), (2048, 1024));
}

#[test]
fn measurement_ignores_allocations_on_other_threads() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let layout = Layout::from_size_align(4096, 8).unwrap();
            barrier.wait();
            let ptr = unsafe { std::alloc::alloc(layout) };
            assert!(!ptr.is_null());
            std::hint::black_box(ptr);
            unsafe { std::alloc::dealloc(ptr, layout) };
            barrier.wait();
        });
        let (_, peak, largest) = peak_of(|| {
            barrier.wait();
            barrier.wait();
        });
        assert_eq!((peak, largest), (0, 0));
    });
}

#[test]
fn measurement_counts_reallocation_and_preserves_alignment() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    let layout = Layout::from_size_align(128, 64).unwrap();
    let grown = Layout::from_size_align(512, 64).unwrap();
    let (_, peak, largest) = peak_of(|| unsafe {
        let ptr = std::alloc::alloc_zeroed(layout);
        assert!(!ptr.is_null());
        assert_eq!(ptr as usize % layout.align(), 0);
        assert_eq!(*ptr, 0);
        let ptr = std::alloc::realloc(ptr, layout, grown.size());
        assert!(!ptr.is_null());
        assert_eq!(ptr as usize % grown.align(), 0);
        std::hint::black_box(ptr);
        std::alloc::dealloc(ptr, grown);
    });
    // The default GlobalAlloc::realloc allocates the replacement before freeing the old block.
    assert_eq!((peak, largest), (128 + 512, 512));
}

const STRING: usize = 256;
const REFERENCES: u8 = 127;

/// `local s = <256 bytes>; return s, s, ... (127 times)`: 32 KiB if every reference were copied.
fn many_references() -> Machine {
    let mut code = vec![Instr::Constant(0), Instr::SetLocal(0)];
    for _ in 0..REFERENCES {
        code.push(Instr::GetLocal(0));
    }
    code.push(Instr::Return(REFERENCES));
    let artifact = Artifact {
        contract: [0; 32],
        constants: vec![Constant::Bytes(vec![b'x'; STRING])],
        natives: Vec::new(),
        protos: vec![Proto {
            params: 0,
            locals: 1,
            max_stack: 128,
            upvalues: Vec::new(),
            lines: vec![1; code.len()],
            code,
        }],
        entries: vec![Entry {
            proto: 0,
            captures: 0,
        }],
        children: Vec::new(),
    };
    let program = Program::verified(artifact, &BytecodeLimits::profile()).expect("verifies");
    let limits = VmLimits {
        max_frames: 4,
        max_objects: 64,
        max_bytes: 8 * 1024,
        // The item bound is generous here, so only the byte bounds refuse the export.
        wire: WireLimits::new(255, 4096, 1024),
        total_fuel: 10_000,
    };
    Machine::start(program, 0, &[], &[], limits).expect("starts")
}

#[test]
fn an_oversized_export_is_refused_before_any_payload_is_copied() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    let mut machine = many_references();
    // Run up to the return, so only the return and its export are measured: the constant, the store and the 127 loads.
    assert_eq!(machine.run(2 + u32::from(REFERENCES)), Step::Exhausted);
    let (step, peak, largest) = peak_of(|| machine.run(1));
    match step {
        Step::Failed { message, .. } => assert!(
            String::from_utf8_lossy(&message).contains("returned values are refused"),
            "{}",
            String::from_utf8_lossy(&message)
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }
    let copied = usize::from(REFERENCES) * STRING;
    // The declared transient budget of a refused export: the popped value handles and a bounded diagnostic, never the
    // payload. A value handle is 16 bytes.
    let budget = usize::from(REFERENCES) * 16 + 512;
    assert!(
        peak < budget && largest < budget,
        "the refused export allocated a peak of {peak} bytes (largest {largest}); copying first would allocate {copied}"
    );
}

fn startup_program(captures: u8, params: u8, constants: Vec<Constant>) -> Program {
    let artifact = Artifact {
        contract: [0; 32],
        constants,
        natives: Vec::new(),
        protos: vec![Proto {
            params,
            locals: u16::from(params),
            max_stack: 1,
            upvalues: Vec::new(),
            lines: vec![1],
            code: vec![Instr::Return(0)],
        }],
        entries: vec![Entry {
            proto: 0,
            captures: u16::from(captures),
        }],
        children: Vec::new(),
    };
    let limits = BytecodeLimits {
        max_string_bytes: 2 * 1024 * 1024,
        ..BytecodeLimits::profile()
    };
    Program::verified(artifact, &limits).expect("verifies")
}

fn startup_limits(wire: WireLimits) -> VmLimits {
    VmLimits {
        max_frames: 4,
        max_objects: 64,
        max_bytes: 128,
        wire,
        total_fuel: 100,
    }
}

#[test]
fn startup_refuses_large_inputs_and_constants_without_payload_sized_allocations() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    const BIG: usize = 1024 * 1024;
    let value = [Value::Bytes(vec![b'x'; BIG])];
    let small_wire = WireLimits::new(1, 4, 4);
    let large_wire = WireLimits::new(4, BIG, BIG);
    for (captures, wire, expected) in [
        (false, small_wire, StartError::ValueRejected),
        (true, small_wire, StartError::ValueRejected),
        (false, large_wire, StartError::Capacity),
        (true, large_wire, StartError::Capacity),
    ] {
        let program = startup_program(u8::from(captures), 1, vec![]);
        let (result, peak, largest) = peak_of(|| {
            let (caps, args) = if captures {
                (&value[..], &[][..])
            } else {
                (&[][..], &value[..])
            };
            Machine::start(program, 0, caps, args, startup_limits(wire)).map(|_| ())
        });
        assert_eq!(result, Err(expected));
        assert!(
            peak < 8192 && largest < 8192,
            "peak {peak}, largest {largest}"
        );
    }
    let program = startup_program(0, 0, vec![Constant::Bytes(vec![b'x'; BIG])]);
    let (result, peak, largest) =
        peak_of(|| Machine::start(program, 0, &[], &[], startup_limits(large_wire)).map(|_| ()));
    assert_eq!(result, Err(StartError::Capacity));
    assert!(
        peak < 8192 && largest < 8192,
        "peak {peak}, largest {largest}"
    );

    let program = startup_program(1, 1, vec![]);
    let invalid_float = [Value::Float(f64::NAN)];
    let (result, peak, largest) = peak_of(|| {
        Machine::start(
            program,
            0,
            &value,
            &invalid_float,
            startup_limits(large_wire),
        )
        .map(|_| ())
    });
    assert_eq!(result, Err(StartError::ValueRejected));
    assert!(
        peak < 8192 && largest < 8192,
        "peak {peak}, largest {largest}"
    );

    let program = startup_program(0, 0, vec![Constant::Bytes(vec![b'x'; BIG])]);
    let invalid = [Value::Bytes(vec![b'x'; 5])];
    let (result, peak, largest) = peak_of(|| {
        Machine::start(program, 0, &[], &invalid, startup_limits(small_wire)).map(|_| ())
    });
    assert_eq!(result, Err(StartError::ValueRejected));
    assert!(
        peak < 8192 && largest < 8192,
        "peak {peak}, largest {largest}"
    );

    let program = startup_program(
        0,
        0,
        vec![Constant::Bytes(vec![]), Constant::Bytes(vec![b'x'; BIG])],
    );
    let limits = VmLimits {
        max_objects: 1,
        max_bytes: BIG * 2,
        ..startup_limits(large_wire)
    };
    let (result, peak, largest) =
        peak_of(|| Machine::start(program, 0, &[], &[], limits).map(|_| ()));
    assert_eq!(result, Err(StartError::Capacity));
    assert!(
        peak < 8192 && largest < 8192,
        "peak {peak}, largest {largest}"
    );
}

fn effect_machine(code: Vec<Instr>, natives: Vec<NativeRef>, wire: WireLimits) -> Machine {
    let artifact = Artifact {
        contract: [0; 32],
        constants: vec![],
        natives,
        protos: vec![Proto {
            params: 0,
            locals: 0,
            max_stack: 2,
            upvalues: vec![],
            lines: vec![1; code.len()],
            code,
        }],
        entries: vec![Entry {
            proto: 0,
            captures: 0,
        }],
        children: vec![],
    };
    Machine::start(
        Program::verified(artifact, &BytecodeLimits::profile()).unwrap(),
        0,
        &[],
        &[],
        startup_limits(wire),
    )
    .unwrap()
}

#[test]
fn resume_and_native_imports_admit_before_copying() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    const BIG: usize = 1024 * 1024;
    let wire = WireLimits::new(BIG, BIG, BIG);
    let mut vm = effect_machine(vec![Instr::Yield(0), Instr::Return(0)], vec![], wire);
    assert!(matches!(vm.run(10), Step::Yielded(_)));
    let input = [Value::Bytes(vec![b'x'; BIG])];
    let (result, peak, largest) = peak_of(|| vm.resume_yield(&input));
    assert_eq!(result, Err(ControlError::Rejected));
    assert!(
        peak < 8192 && largest < 8192,
        "peak {peak}, largest {largest}"
    );

    let many = vec![Value::Nil; BIG];
    let (result, peak, largest) = peak_of(|| vm.resume_yield(&many));
    assert_eq!(result, Err(ControlError::Rejected));
    assert!(
        peak < 8192 && largest < 8192,
        "peak {peak}, largest {largest}"
    );

    let native = NativeRef {
        module: 0,
        operation: 0,
        argc: 0,
        results: 1,
    };
    let mut vm = effect_machine(vec![Instr::Native(0), Instr::Return(1)], vec![native], wire);
    assert!(matches!(vm.run(10), Step::Pending(Effect::Native { .. })));
    let input = EffectResult::Value(Value::Bytes(vec![b'x'; BIG]));
    let (result, peak, largest) = peak_of(|| vm.complete(input));
    assert_eq!(result, Ok(()));
    assert!(
        peak < 8192 && largest < 8192,
        "peak {peak}, largest {largest}"
    );
    assert!(matches!(
        vm.run(10),
        Step::Failed {
            code: protolink_lua::lifecycle::FailureCode::Capacity,
            ..
        }
    ));
}

#[test]
fn child_result_import_admits_before_copying() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    const BIG: usize = 1024 * 1024;
    let wire = WireLimits::new(4, BIG, BIG);
    let child = Artifact {
        contract: [0; 32],
        constants: vec![],
        natives: vec![],
        protos: vec![Proto {
            params: 0,
            locals: 0,
            max_stack: 1,
            upvalues: vec![],
            lines: vec![1],
            code: vec![Instr::Return(0)],
        }],
        entries: vec![Entry {
            proto: 0,
            captures: 0,
        }],
        children: vec![],
    }
    .encode();
    let link = ChildLink {
        slot: 1,
        digest: digest(&child),
        entry: 0,
        bytes: child.into(),
    };
    let code = vec![
        Instr::Spawn {
            child: 0,
            argc: 0,
            captures: 0,
        },
        Instr::Await,
        Instr::Return(1),
    ];
    let artifact = Artifact {
        contract: [0; 32],
        constants: vec![],
        natives: vec![],
        protos: vec![Proto {
            params: 0,
            locals: 0,
            max_stack: 2,
            upvalues: vec![],
            lines: vec![1; code.len()],
            code,
        }],
        entries: vec![Entry {
            proto: 0,
            captures: 0,
        }],
        children: vec![link],
    };
    let mut vm = Machine::start(
        Program::verified(artifact, &BytecodeLimits::profile()).unwrap(),
        0,
        &[],
        &[],
        startup_limits(wire),
    )
    .unwrap();
    assert!(matches!(vm.run(10), Step::Pending(Effect::Spawn { .. })));
    vm.complete(EffectResult::Task(1)).unwrap();
    assert!(matches!(vm.run(10), Step::Pending(Effect::Await { .. })));
    let result =
        EffectResult::Tuple(Tuple::new(vec![Value::Bytes(vec![b'x'; BIG])], wire).unwrap());
    let (result, peak, largest) = peak_of(|| vm.complete(result));
    assert_eq!(result, Ok(()));
    assert!(
        peak < 8192 && largest < 8192,
        "peak {peak}, largest {largest}"
    );
    assert!(matches!(
        vm.run(10),
        Step::Failed {
            code: protolink_lua::lifecycle::FailureCode::Capacity,
            ..
        }
    ));
}

/// Two operands of `each` bytes, preloaded at start, and a program that concatenates them.
fn concat_machine(each: usize, max_bytes: usize) -> Machine {
    let code = vec![
        Instr::Constant(0),
        Instr::Constant(1),
        Instr::Concat,
        Instr::Return(1),
    ];
    let artifact = Artifact {
        contract: [0; 32],
        constants: vec![
            Constant::Bytes(vec![b'a'; each]),
            Constant::Bytes(vec![b'b'; each]),
        ],
        natives: Vec::new(),
        protos: vec![Proto {
            params: 0,
            locals: 0,
            max_stack: 4,
            upvalues: Vec::new(),
            lines: vec![1; code.len()],
            code,
        }],
        entries: vec![Entry {
            proto: 0,
            captures: 0,
        }],
        children: Vec::new(),
    };
    let program = Program::verified(artifact, &BytecodeLimits::profile()).expect("verifies");
    let limits = VmLimits {
        max_frames: 4,
        max_objects: 16,
        max_bytes,
        wire: WireLimits::new(8, 4096, 1024),
        total_fuel: 10_000,
    };
    // Two operands of `each` bytes each are charged 16 bytes of overhead apiece, so they fit a budget of `max_bytes`
    // while their concatenation does not.
    Machine::start(program, 0, &[], &[], limits).expect("the operands fit the budget")
}

#[test]
fn an_oversized_concatenation_is_refused_before_its_result_is_allocated() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    let each = 400;
    // The operands charge 2 × (400 + 16) = 832 bytes. The result would be 800 bytes plus 16 of overhead, over 1000.
    let mut machine = concat_machine(each, 1000);
    let (step, peak, largest) = peak_of(|| machine.run(4));
    assert!(
        matches!(
            step,
            Step::Failed {
                code: protolink_lua::lifecycle::FailureCode::Capacity,
                ..
            }
        ),
        "the concatenation is refused as a capacity fault, got {step:?}"
    );
    assert!(
        largest < 2 * each,
        "the refused concatenation allocated a block of {largest} bytes (peak {peak}); copying the operands first would \
         allocate the {}-byte result",
        2 * each
    );
}

#[test]
fn a_concatenation_within_the_budget_allocates_its_result_once() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    let each = 400;
    let mut machine = concat_machine(each, 4096);
    let (step, _peak, largest) = peak_of(|| machine.run(4));
    assert!(
        matches!(step, Step::Returned(_)),
        "the concatenation completes, got {step:?}"
    );
    assert!(
        largest >= 2 * each && largest < 2 * each + 64,
        "the result is one buffer of exactly its length, not a clone of each operand: {largest} bytes"
    );
}

// -- Closures and protected calls admit before they reserve ---------------------------------------------------------------

fn limits_with(max_objects: usize, max_bytes: usize) -> VmLimits {
    VmLimits {
        max_frames: 8,
        max_objects,
        max_bytes,
        wire: WireLimits::new(64, 4096, 4096),
        total_fuel: 100_000,
    }
}

fn plain_proto(
    max_stack: u16,
    upvalues: Vec<protolink_lua_bytecode::UpvalueSource>,
    code: Vec<Instr>,
) -> Proto {
    Proto {
        params: 0,
        locals: 0,
        max_stack,
        upvalues,
        lines: vec![1; code.len()],
        code,
    }
}

fn verified(protos: Vec<Proto>, constants: Vec<Constant>, captures: u16) -> Program {
    let artifact = Artifact {
        contract: [0; 32],
        constants,
        natives: Vec::new(),
        protos,
        entries: vec![Entry { proto: 0, captures }],
        children: Vec::new(),
    };
    Program::verified(artifact, &BytecodeLimits::profile()).expect("verifies")
}

/// An entry with sixty-four captures that creates a closure over all of them: 16 + 64 × 4 = 272 bytes of capture list.
fn closure_machine(max_objects: usize, max_bytes: usize) -> Machine {
    let sources = (0..64)
        .map(protolink_lua_bytecode::UpvalueSource::Upvalue)
        .collect();
    let program = verified(
        vec![
            plain_proto(2, Vec::new(), vec![Instr::Closure(1), Instr::Return(0)]),
            plain_proto(1, sources, vec![Instr::Return(0)]),
        ],
        Vec::new(),
        64,
    );
    let captures = vec![protolink_lua::value::Value::Nil; 64];
    Machine::start(
        program,
        0,
        &captures,
        &[],
        limits_with(max_objects, max_bytes),
    )
    .expect("starts")
}

#[test]
fn a_rejected_closure_allocates_no_capture_list() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    // The entry closure (16 + 256) and 64 cells (16 each) charge 1296. The inner closure needs 272 more.
    for (max_objects, max_bytes) in [(1_000, 1_400), (65, 1_000_000)] {
        let mut machine = closure_machine(max_objects, max_bytes);
        let (step, peak, largest) = peak_of(|| machine.run(4));
        assert!(
            matches!(
                step,
                Step::Failed {
                    code: protolink_lua::lifecycle::FailureCode::Capacity,
                    ..
                }
            ),
            "{step:?}"
        );
        assert!(
            largest < 256 && peak < 512,
            "the refused closure allocated a block of {largest} bytes (peak {peak}); reserving its 256-byte capture list \
             first would show here"
        );
    }
    let mut roomy = closure_machine(1_000, 1_000_000);
    let (step, _, largest) = peak_of(|| roomy.run(4));
    assert!(matches!(step, Step::Returned(_)), "{step:?}");
    assert!(
        largest >= 256,
        "the capture list is one block of its size: {largest}"
    );
}

/// `pcall(function() return 7, 7, ... end)` with `count` results.
fn protected_result_machine(count: u8, max_objects: usize, max_bytes: usize) -> Machine {
    let mut code: Vec<Instr> = (0..count).map(|_| Instr::Constant(0)).collect();
    code.push(Instr::Return(count));
    let program = verified(
        vec![
            plain_proto(
                4,
                Vec::new(),
                vec![
                    Instr::Closure(1),
                    Instr::Pcall(0),
                    Instr::Unpack(1),
                    Instr::Return(1),
                ],
            ),
            plain_proto(u16::from(count).max(1), Vec::new(), code),
        ],
        vec![Constant::Integer(7)],
        0,
    );
    Machine::start(program, 0, &[], &[], limits_with(max_objects, max_bytes)).expect("starts")
}

#[test]
fn a_rejected_protected_result_allocates_no_result_buffer() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    // Sixty results make a tuple of 61 elements, 976 bytes of handles and 992 charged.
    for (max_objects, max_bytes) in [(1_000, 200), (2, 1_000_000)] {
        let mut machine = protected_result_machine(60, max_objects, max_bytes);
        let (step, peak, largest) = peak_of(|| machine.run(200));
        assert!(
            matches!(
                step,
                Step::Failed {
                    code: protolink_lua::lifecycle::FailureCode::Capacity,
                    ..
                }
            ),
            "{step:?}"
        );
        // What is legitimately live: the operand stack, which grew to 64 handles (1024 bytes), and the 60 popped results
        // (960). The wrapped tuple would add a block of 976 more, for a peak near 2960.
        assert!(
            largest <= 1024 && peak < 2400,
            "the refused result allocated a block of {largest} bytes (peak {peak})"
        );
    }
}

#[test]
fn an_error_value_that_cannot_be_built_allocates_nothing_large_and_is_a_capacity_failure() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    let program = verified(
        vec![
            plain_proto(
                4,
                Vec::new(),
                vec![
                    Instr::Closure(1),
                    Instr::Pcall(0),
                    Instr::Unpack(2),
                    Instr::Return(2),
                ],
            ),
            plain_proto(4, Vec::new(), vec![Instr::Constant(0), Instr::Raise]),
        ],
        vec![Constant::Bytes(vec![b'e'; 200])],
        0,
    );
    // The entry closure, the 200-byte message and the thrower charge 16 + 216 + 16. The error needs its text (216, moved)
    // and a two-element tuple (48).
    let mut machine =
        Machine::start(program, 0, &[], &[], limits_with(1_000, 290)).expect("starts");
    let (step, _peak, _largest) = peak_of(|| machine.run(50));
    assert!(
        matches!(
            step,
            Step::Failed {
                code: protolink_lua::lifecycle::FailureCode::Capacity,
                ..
            }
        ),
        "{step:?}"
    );
}

// -- Fault injection ------------------------------------------------------------------------------------------------------

/// Runs `build` and `run` once for every allocation the run makes, failing exactly that one. A path that reserves its
/// memory fallibly ends the execution with a capacity fault or completes. A path that allocates unconditionally aborts
/// the process, which fails the test run. `run` returns true when the execution ended normally.
fn sweep(label: &str, mut machine: impl FnMut() -> Machine) {
    // First, count the allocations of an unfailed run.
    FAIL_AT.store(usize::MAX, Ordering::Relaxed);
    ALLOCATIONS.store(0, Ordering::Relaxed);
    let mut baseline = machine();
    let (clean, _, _) = peak_of(|| baseline.run(1_000));
    assert!(
        matches!(clean, Step::Returned(_)),
        "{label}: the baseline completes, got {clean:?}"
    );
    let total = ALLOCATIONS.load(Ordering::Relaxed);
    assert!(total > 0, "{label}: the run allocates");
    for failing in 0..total {
        let mut vm = machine();
        ALLOCATIONS.store(0, Ordering::Relaxed);
        FAIL_AT.store(failing, Ordering::Relaxed);
        let (step, _, _) = peak_of(|| vm.run(1_000));
        FAIL_AT.store(usize::MAX, Ordering::Relaxed);
        match step {
            Step::Returned(_) => {}
            Step::Failed { code, .. } => assert_eq!(
                code,
                protolink_lua::lifecycle::FailureCode::Capacity,
                "{label}: allocation {failing} of {total} failed and the execution reported {code:?}"
            ),
            other => panic!("{label}: allocation {failing} of {total}: {other:?}"),
        }
    }
}

#[test]
fn refused_allocations_on_the_changed_paths_are_capacity_faults_and_never_aborts() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    sweep("closure", || closure_machine(1_000, 1_000_000));
    sweep("protected result", || {
        protected_result_machine(20, 1_000, 1_000_000)
    });
    sweep("float concatenation", || float_concat_machine(1_000_000));
}

fn float_concat_machine(max_bytes: usize) -> Machine {
    let code = vec![
        Instr::Constant(0),
        Instr::Constant(1),
        Instr::Concat,
        Instr::Return(1),
    ];
    let program = verified(
        vec![plain_proto(4, Vec::new(), code)],
        vec![Constant::Float(0.1), Constant::Bytes(vec![b'z'; 100])],
        0,
    );
    Machine::start(program, 0, &[], &[], limits_with(1_000, max_bytes)).expect("starts")
}

#[test]
fn a_float_concatenation_allocates_its_result_once_and_refuses_over_the_budget() {
    let _turn = MEASURE.lock().unwrap_or_else(|e| e.into_inner());
    // `0.1 .. <100 bytes>` is 103 bytes, in one block. The float's text is held inline, not in a temporary string.
    let mut machine = float_concat_machine(1_000_000);
    let (step, _peak, largest) = peak_of(|| machine.run(10));
    assert!(matches!(step, Step::Returned(_)), "{step:?}");
    assert!(
        (103..103 + 64).contains(&largest),
        "one result block: {largest}"
    );

    // The constant charges 116. The 103-byte result would charge 119 more.
    let mut tight = float_concat_machine(200);
    let (step, _peak, largest) = peak_of(|| tight.run(10));
    assert!(
        matches!(
            step,
            Step::Failed {
                code: protolink_lua::lifecycle::FailureCode::Capacity,
                ..
            }
        ),
        "{step:?}"
    );
    assert!(
        largest < 103,
        "the refused result was never allocated: {largest}"
    );
}
