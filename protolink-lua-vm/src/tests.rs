//! Behavioural tests of the machine over hand-built, verified artifacts.

use alloc::format;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use protolink_lua::lifecycle::FailureCode;
use protolink_lua::value::{Limits as WireLimits, Tuple, Value};
use protolink_lua_bytecode::{
    Artifact, ChildLink, Constant, Entry, Instr, Limits as BytecodeLimits, NativeRef, Proto,
    RESULTS_MULTI, UpvalueSource, digest,
};

use crate::{ControlError, Effect, EffectResult, Machine, Program, StartError, Step, VmLimits};

fn wire() -> WireLimits {
    WireLimits::new(8, 64, 128)
}

fn limits() -> VmLimits {
    VmLimits {
        max_frames: 8,
        max_objects: 256,
        max_bytes: 4096,
        wire: wire(),
        total_fuel: 10_000,
    }
}

fn proto(
    params: u8,
    locals: u16,
    max_stack: u16,
    upvalues: Vec<UpvalueSource>,
    code: Vec<Instr>,
) -> Proto {
    let lines = vec![1; code.len()];
    Proto {
        params,
        locals,
        max_stack,
        upvalues,
        code,
        lines,
    }
}

fn artifact(
    constants: Vec<Constant>,
    natives: Vec<NativeRef>,
    protos: Vec<Proto>,
    children: Vec<ChildLink>,
) -> Artifact {
    Artifact {
        contract: [0; 32],
        constants,
        natives,
        protos,
        entries: vec![Entry {
            proto: 0,
            captures: 0,
        }],
        children,
    }
}

fn program_of(artifact: Artifact) -> Program {
    Program::verified(artifact, &BytecodeLimits::profile()).expect("test artifact verifies")
}

fn machine(program: Program, args: &[Value]) -> Machine {
    Machine::start(program, 0, &[], args, limits()).expect("starts")
}

fn values(tuple: &Tuple) -> Vec<Value> {
    tuple.values().to_vec()
}

fn returned(step: Step) -> Vec<Value> {
    match step {
        Step::Returned(tuple) => values(&tuple),
        other => panic!("expected a return, got {other:?}"),
    }
}

fn int(i: i64) -> Constant {
    Constant::Integer(i)
}

fn text(s: &str) -> Constant {
    Constant::Bytes(s.as_bytes().to_vec())
}

fn run_to_end(machine: &mut Machine) -> Step {
    loop {
        match machine.run(1000) {
            Step::Exhausted => continue,
            other => return other,
        }
    }
}

#[test]
fn integer_and_float_arithmetic_follow_lua_rules() {
    let code = vec![
        Instr::Constant(0),
        Instr::Constant(1),
        Instr::IDiv,
        Instr::Constant(0),
        Instr::Constant(1),
        Instr::Mod,
        Instr::Return(2),
    ];
    let program = program_of(artifact(
        vec![int(-7), int(2)],
        vec![],
        vec![proto(0, 0, 4, vec![], code)],
        vec![],
    ));
    // -7 // 2 is -4 and -7 % 2 is 1 under Lua's floor rules.
    assert_eq!(
        returned(run_to_end(&mut machine(program, &[]))),
        vec![Value::Integer(-4), Value::Integer(1)]
    );
}

/// Float floor division keeps the sign of a zero result, as C's `floor` does. Both operands are built at run time: the first
/// is `-(0.0)`, and the second quotient is `0.0 // -1.0`. Stock Lua 5.4 gives `-0.0` for both.
#[test]
fn float_floor_division_keeps_the_sign_of_zero() {
    let code = vec![
        Instr::Constant(0),
        Instr::Neg,
        Instr::Constant(1),
        Instr::IDiv,
        Instr::Constant(0),
        Instr::Constant(2),
        Instr::IDiv,
        Instr::Return(2),
    ];
    let program = program_of(artifact(
        vec![
            Constant::Float(0.0),
            Constant::Float(1.0),
            Constant::Float(-1.0),
        ],
        vec![],
        vec![proto(0, 0, 4, vec![], code)],
        vec![],
    ));
    let results = returned(run_to_end(&mut machine(program, &[])));
    assert_eq!(results.len(), 2);
    for result in results {
        match result {
            Value::Float(f) => {
                assert!(f == 0.0 && f.is_sign_negative(), "expected -0.0, got {f:?}")
            }
            other => panic!("expected a float, got {other:?}"),
        }
    }
}

#[test]
fn integer_arithmetic_wraps_and_division_by_zero_fails() {
    let wrap = program_of(artifact(
        vec![int(i64::MAX), int(1)],
        vec![],
        vec![proto(
            0,
            0,
            4,
            vec![],
            vec![
                Instr::Constant(0),
                Instr::Constant(1),
                Instr::Add,
                Instr::Return(1),
            ],
        )],
        vec![],
    ));
    assert_eq!(
        returned(run_to_end(&mut machine(wrap, &[]))),
        vec![Value::Integer(i64::MIN)]
    );

    let by_zero = program_of(artifact(
        vec![int(7), int(0)],
        vec![],
        vec![proto(
            0,
            0,
            4,
            vec![],
            vec![
                Instr::Constant(0),
                Instr::Constant(1),
                Instr::IDiv,
                Instr::Return(1),
            ],
        )],
        vec![],
    ));
    match run_to_end(&mut machine(by_zero, &[])) {
        Step::Failed { code, message } => {
            assert_eq!(code, FailureCode::RuntimeError);
            assert_eq!(
                message,
                b"attempt to divide by zero".to_vec(),
                "the message stock Lua 5.4 gives"
            );
        }
        other => panic!("expected failure, got {other:?}"),
    }
}

#[test]
fn equality_and_order_cover_mixed_numbers_and_strings() {
    let code = vec![
        Instr::Constant(0),
        Instr::Constant(1),
        Instr::Eq,
        Instr::Constant(2),
        Instr::Constant(3),
        Instr::Lt,
        Instr::Return(2),
    ];
    let program = program_of(artifact(
        vec![int(1), Constant::Float(1.0), text("a"), text("b")],
        vec![],
        vec![proto(0, 0, 4, vec![], code)],
        vec![],
    ));
    assert_eq!(
        returned(run_to_end(&mut machine(program, &[]))),
        vec![Value::Boolean(true), Value::Boolean(true)]
    );
}

#[test]
fn concatenation_and_length_work_on_byte_strings() {
    let code = vec![
        Instr::Constant(0),
        Instr::Constant(1),
        Instr::Concat,
        Instr::Len,
        Instr::Return(1),
    ];
    let program = program_of(artifact(
        vec![text("ab"), text("cd")],
        vec![],
        vec![proto(0, 0, 4, vec![], code)],
        vec![],
    ));
    assert_eq!(
        returned(run_to_end(&mut machine(program, &[]))),
        vec![Value::Integer(4)]
    );
}

#[test]
fn a_closure_shares_a_cell_with_the_frame_that_created_it() {
    // local x = 5; local f = function() return x end; return f()
    let entry = proto(
        0,
        1,
        4,
        vec![],
        vec![
            Instr::Constant(0),
            Instr::NewCell(0),
            Instr::Closure(1),
            Instr::Call {
                argc: 0,
                results: 1,
            },
            Instr::Return(1),
        ],
    );
    let inner = proto(
        0,
        0,
        2,
        vec![UpvalueSource::Local(0)],
        vec![Instr::GetUpvalue(0), Instr::Return(1)],
    );
    let program = program_of(artifact(vec![int(5)], vec![], vec![entry, inner], vec![]));
    assert_eq!(
        returned(run_to_end(&mut machine(program, &[]))),
        vec![Value::Integer(5)]
    );
}

#[test]
fn a_captured_value_is_a_snapshot_cell_on_the_entry() {
    // The entry's capture is an upvalue cell. Reading it must see the snapshot passed at start.
    let entry = proto(
        0,
        0,
        2,
        vec![],
        vec![Instr::GetUpvalue(0), Instr::Return(1)],
    );
    let mut art = artifact(vec![], vec![], vec![entry], vec![]);
    art.entries = vec![Entry {
        proto: 0,
        captures: 1,
    }];
    let program = program_of(art);
    let mut vm = Machine::start(program, 0, &[Value::Bytes(b"snap".to_vec())], &[], limits())
        .expect("starts");
    assert_eq!(
        returned(run_to_end(&mut vm)),
        vec![Value::Bytes(b"snap".to_vec())]
    );
}

#[test]
fn a_call_passes_arguments_and_keeps_fixed_result_counts() {
    // return add(3, 4) where add(a, b) = a + b, asking for one result.
    let entry = proto(
        0,
        0,
        4,
        vec![],
        vec![
            Instr::Closure(1),
            Instr::Constant(0),
            Instr::Constant(1),
            Instr::Call {
                argc: 2,
                results: 1,
            },
            Instr::Return(1),
        ],
    );
    let add = proto(
        2,
        2,
        4,
        vec![],
        vec![
            Instr::GetLocal(0),
            Instr::GetLocal(1),
            Instr::Add,
            Instr::Return(1),
        ],
    );
    let program = program_of(artifact(
        vec![int(3), int(4)],
        vec![],
        vec![entry, add],
        vec![],
    ));
    assert_eq!(
        returned(run_to_end(&mut machine(program, &[]))),
        vec![Value::Integer(7)]
    );
}

#[test]
fn a_multi_result_return_forwards_every_value() {
    // return two() where two() returns 1, 2.
    let entry = proto(
        0,
        0,
        4,
        vec![],
        vec![
            Instr::Closure(1),
            Instr::Call {
                argc: 0,
                results: RESULTS_MULTI,
            },
            Instr::ReturnMulti(0),
        ],
    );
    let two = proto(
        0,
        0,
        4,
        vec![],
        vec![Instr::Constant(0), Instr::Constant(1), Instr::Return(2)],
    );
    let program = program_of(artifact(
        vec![int(1), int(2)],
        vec![],
        vec![entry, two],
        vec![],
    ));
    assert_eq!(
        returned(run_to_end(&mut machine(program, &[]))),
        vec![Value::Integer(1), Value::Integer(2)]
    );
}

#[test]
fn pcall_catches_an_ordinary_error_and_returns_status_and_message() {
    // local ok, msg = pcall(function() error("boom") end); return ok, msg
    let entry = proto(
        0,
        0,
        4,
        vec![],
        vec![
            Instr::Closure(1),
            Instr::Pcall(0),
            Instr::Unpack(2),
            Instr::Return(2),
        ],
    );
    let thrower = proto(0, 0, 4, vec![], vec![Instr::Constant(0), Instr::Raise]);
    let program = program_of(artifact(
        vec![text("boom")],
        vec![],
        vec![entry, thrower],
        vec![],
    ));
    assert_eq!(
        returned(run_to_end(&mut machine(program, &[]))),
        vec![Value::Boolean(false), Value::Bytes(b"boom".to_vec())]
    );
}

#[test]
fn an_uncaught_error_fails_the_execution() {
    let program = program_of(artifact(
        vec![text("nope")],
        vec![],
        vec![proto(
            0,
            0,
            4,
            vec![],
            vec![Instr::Constant(0), Instr::Raise],
        )],
        vec![],
    ));
    match run_to_end(&mut machine(program, &[])) {
        Step::Failed { code, message } => {
            assert_eq!(code, FailureCode::RuntimeError);
            assert_eq!(message, b"nope".to_vec());
        }
        other => panic!("expected failure, got {other:?}"),
    }
}

#[test]
fn a_yield_suspends_and_the_reply_becomes_the_yield_result() {
    // local v = coroutine.yield(1); return v
    let program = program_of(artifact(
        vec![int(1)],
        vec![],
        vec![proto(
            0,
            0,
            4,
            vec![],
            vec![
                Instr::Constant(0),
                Instr::Yield(1),
                Instr::Unpack(1),
                Instr::Return(1),
            ],
        )],
        vec![],
    ));
    let mut vm = machine(program, &[]);
    match vm.run(1000) {
        Step::Yielded(tuple) => assert_eq!(values(&tuple), vec![Value::Integer(1)]),
        other => panic!("expected yield, got {other:?}"),
    }
    assert_eq!(
        vm.complete(EffectResult::Value(Value::Nil)),
        Err(ControlError::NotPending)
    );
    vm.resume_yield(&[Value::Integer(9)]).expect("resume");
    assert_eq!(returned(run_to_end(&mut vm)), vec![Value::Integer(9)]);
}

#[test]
fn a_native_call_is_a_pending_effect_completed_by_the_embedding() {
    let program = program_of(artifact(
        vec![int(0)],
        vec![NativeRef {
            module: 1,
            operation: 2,
            argc: 1,
            results: 1,
        }],
        vec![proto(
            0,
            0,
            4,
            vec![],
            vec![Instr::Constant(0), Instr::Native(0), Instr::Return(1)],
        )],
        vec![],
    ));
    let mut vm = machine(program, &[]);
    assert_eq!(
        vm.run(1000),
        Step::Pending(Effect::Native {
            index: 0,
            arguments: vec![Value::Integer(0)],
        })
    );
    // While pending, the machine refuses to run and reports why.
    assert!(matches!(vm.run(1), Step::Failed { .. }));
    vm.complete(EffectResult::Value(Value::Integer(42)))
        .expect("complete");
    assert_eq!(returned(run_to_end(&mut vm)), vec![Value::Integer(42)]);
}

#[test]
fn spawn_and_await_pass_captures_and_return_the_child_tuple() {
    let child_bytes = artifact(
        vec![int(7)],
        vec![],
        vec![proto(
            0,
            0,
            2,
            vec![],
            vec![Instr::Constant(0), Instr::Return(1)],
        )],
        vec![],
    )
    .encode();
    let child = ChildLink {
        slot: 1,
        digest: digest(&child_bytes),
        entry: 0,
        bytes: child_bytes.into(),
    };
    let parent = artifact(
        vec![],
        vec![],
        vec![proto(
            0,
            0,
            4,
            vec![],
            vec![
                Instr::Spawn {
                    child: 0,
                    argc: 0,
                    captures: 0,
                },
                Instr::Await,
                Instr::Unpack(1),
                Instr::Return(1),
            ],
        )],
        vec![child],
    );
    let mut vm = machine(program_of(parent), &[]);
    assert_eq!(
        vm.run(1000),
        Step::Pending(Effect::Spawn {
            child: 0,
            captures: vec![],
            arguments: vec![],
        })
    );
    vm.complete(EffectResult::Task(3)).expect("spawn completes");
    assert_eq!(vm.run(1000), Step::Pending(Effect::Await { task: 3 }));
    vm.complete(EffectResult::Tuple(
        Tuple::new(vec![Value::Integer(7)], wire()).expect("tuple"),
    ))
    .expect("await completes");
    assert_eq!(returned(run_to_end(&mut vm)), vec![Value::Integer(7)]);
}

#[test]
fn a_child_failure_raised_into_the_parent_can_be_caught() {
    // The embedding reports the child failed. The parent catches it with pcall, as ordinary Lua would.
    let child_bytes = artifact(
        vec![int(7)],
        vec![],
        vec![proto(
            0,
            0,
            2,
            vec![],
            vec![Instr::Constant(0), Instr::Return(1)],
        )],
        vec![],
    )
    .encode();
    let child = ChildLink {
        slot: 1,
        digest: digest(&child_bytes),
        entry: 0,
        bytes: child_bytes.into(),
    };
    let parent = artifact(
        vec![],
        vec![],
        vec![
            proto(
                0,
                0,
                4,
                vec![],
                vec![
                    Instr::Closure(1),
                    Instr::Pcall(0),
                    Instr::Unpack(2),
                    Instr::Return(2),
                ],
            ),
            proto(
                0,
                0,
                4,
                vec![],
                vec![
                    Instr::Spawn {
                        child: 0,
                        argc: 0,
                        captures: 0,
                    },
                    Instr::Await,
                    Instr::Unpack(1),
                    Instr::Return(1),
                ],
            ),
        ],
        vec![child],
    );
    let mut vm = machine(program_of(parent), &[]);
    assert!(matches!(vm.run(1000), Step::Pending(Effect::Spawn { .. })));
    vm.complete(EffectResult::Raise(b"child unknown".to_vec()))
        .expect("raise");
    assert_eq!(
        returned(run_to_end(&mut vm)),
        vec![
            Value::Boolean(false),
            Value::Bytes(b"child unknown".to_vec())
        ]
    );
}

#[test]
fn slices_exhaust_without_losing_state_and_the_total_budget_is_final() {
    let program = program_of(artifact(
        vec![int(1)],
        vec![],
        vec![proto(
            0,
            0,
            4,
            vec![],
            vec![Instr::Constant(0), Instr::Pop, Instr::Jump(0)],
        )],
        vec![],
    ));
    let mut vm = Machine::start(
        program,
        0,
        &[],
        &[],
        VmLimits {
            total_fuel: 50,
            ..limits()
        },
    )
    .expect("starts");
    assert_eq!(vm.run(10), Step::Exhausted);
    assert!(vm.fuel_remaining() < 50);
    assert_eq!(run_to_end(&mut vm), Step::BudgetExhausted);
}

#[test]
fn pcall_cannot_catch_budget_exhaustion() {
    // An infinite loop inside pcall. The budget must still end the execution.
    let entry = proto(
        0,
        0,
        4,
        vec![],
        vec![Instr::Closure(1), Instr::Pcall(0), Instr::Return(0)],
    );
    let spin = proto(0, 0, 4, vec![], vec![Instr::Jump(0)]);
    let program = program_of(artifact(vec![], vec![], vec![entry, spin], vec![]));
    let mut vm = Machine::start(
        program,
        0,
        &[],
        &[],
        VmLimits {
            total_fuel: 200,
            ..limits()
        },
    )
    .expect("starts");
    assert_eq!(run_to_end(&mut vm), Step::BudgetExhausted);
}

#[test]
fn a_local_non_finite_result_cannot_leave_the_node() {
    // 1 / 0 is infinity locally, which is allowed, but returning it is not remote data.
    let program = program_of(artifact(
        vec![int(1), int(0)],
        vec![],
        vec![proto(
            0,
            0,
            4,
            vec![],
            vec![
                Instr::Constant(0),
                Instr::Constant(1),
                Instr::Div,
                Instr::Return(1),
            ],
        )],
        vec![],
    ));
    assert!(matches!(
        run_to_end(&mut machine(program, &[])),
        Step::Failed { .. }
    ));
}

#[test]
fn startup_validates_complete_capture_and_argument_tuples() {
    fn start(
        captures: Vec<Value>,
        args: Vec<Value>,
        cap_count: u8,
        wire: WireLimits,
    ) -> Result<(), StartError> {
        let mut art = artifact(
            vec![],
            vec![],
            vec![proto(1, 1, 2, vec![], vec![Instr::Return(0)])],
            vec![],
        );
        art.entries[0].captures = u16::from(cap_count);
        Machine::start(
            program_of(art),
            0,
            &captures,
            &args,
            VmLimits { wire, ..limits() },
        )
        .map(|_| ())
    }
    let bounds = WireLimits::new(2, 4, 6);
    let bytes = |n| Value::Bytes(vec![b'x'; n]);
    assert_eq!(start(vec![], vec![bytes(4), bytes(2)], 0, bounds), Ok(()));
    assert_eq!(
        start(vec![], vec![bytes(5)], 0, bounds),
        Err(StartError::ValueRejected)
    );
    assert_eq!(
        start(vec![], vec![bytes(4), bytes(3)], 0, bounds),
        Err(StartError::ValueRejected)
    );
    assert_eq!(
        start(vec![], vec![Value::Nil, Value::Nil, Value::Nil], 0, bounds),
        Err(StartError::ValueRejected)
    );
    assert_eq!(
        start(vec![], vec![Value::Nil, Value::Float(f64::NAN)], 0, bounds),
        Err(StartError::ValueRejected)
    );
    assert_eq!(
        start(vec![bytes(4), bytes(3)], vec![], 2, bounds),
        Err(StartError::ValueRejected)
    );
    assert_eq!(
        start(vec![Value::Float(f64::INFINITY)], vec![], 1, bounds),
        Err(StartError::ValueRejected)
    );
    assert_eq!(
        start(vec![], vec![], 1, bounds),
        Err(StartError::CaptureCountMismatch)
    );
}

#[test]
fn heap_admission_rejects_overflow_without_charging() {
    let mut heap = crate::heap::Heap::new(1, usize::MAX);
    assert!(matches!(
        heap.admit_tuple(usize::MAX),
        Err(crate::machine::Fault::Capacity(_))
    ));
    assert!(heap.alloc_bytes(b"ok").is_ok());
    assert!(matches!(
        heap.alloc_bytes(b"another"),
        Err(crate::machine::Fault::Capacity(_))
    ));
}

#[test]
fn startup_heap_exact_bound_and_missing_arguments() {
    fn make() -> Program {
        program_of(artifact(
            vec![],
            vec![],
            vec![proto(
                2,
                2,
                2,
                vec![],
                vec![Instr::GetLocal(0), Instr::GetLocal(1), Instr::Return(2)],
            )],
            vec![],
        ))
    }
    let args = [Value::Bytes(vec![1; 4])];
    let tight = VmLimits {
        max_bytes: 36,
        ..limits()
    };
    let mut vm = Machine::start(make(), 0, &[], &args, tight).unwrap();
    assert_eq!(
        returned(run_to_end(&mut vm)),
        vec![args[0].clone(), Value::Nil]
    );
    assert_eq!(
        Machine::start(
            make(),
            0,
            &[],
            &args,
            VmLimits {
                max_bytes: 35,
                ..tight
            }
        )
        .map(|_| ()),
        Err(StartError::Capacity)
    );
    assert_eq!(
        Machine::start(
            make(),
            0,
            &[],
            &args,
            VmLimits {
                max_objects: 1,
                ..limits()
            }
        )
        .map(|_| ()),
        Err(StartError::Capacity)
    );
}

#[test]
fn rejected_resume_does_not_consume_heap_budget() {
    let program = program_of(artifact(
        vec![],
        vec![],
        vec![proto(
            0,
            0,
            2,
            vec![],
            vec![Instr::Yield(0), Instr::Unpack(1), Instr::Return(1)],
        )],
        vec![],
    ));
    // Entry closure costs 16 bytes; the first rejected resume would consume two byte objects without rollback.
    let mut vm = Machine::start(
        program,
        0,
        &[],
        &[],
        VmLimits {
            max_bytes: 100,
            ..limits()
        },
    )
    .unwrap();
    assert!(matches!(vm.run(100), Step::Yielded(_)));
    let reply = [Value::Bytes(vec![1; 20]), Value::Bytes(vec![2; 20])];
    assert_eq!(vm.resume_yield(&reply), Err(ControlError::Rejected));
    assert_eq!(vm.resume_yield(&[Value::Bytes(vec![3; 20])]), Ok(()));
    assert_eq!(
        returned(run_to_end(&mut vm)),
        vec![Value::Bytes(vec![3; 20])]
    );
}

#[test]
fn the_object_budget_is_enforced_at_start() {
    let program = program_of(artifact(
        vec![text("a"), text("b")],
        vec![],
        vec![proto(
            0,
            0,
            2,
            vec![],
            vec![Instr::Constant(0), Instr::Return(1)],
        )],
        vec![],
    ));
    let tight = VmLimits {
        max_objects: 1,
        ..limits()
    };
    assert_eq!(
        Machine::start(program, 0, &[], &[], tight).map(|_| ()),
        Err(StartError::Capacity)
    );
}

#[test]
fn the_frame_budget_stops_runaway_recursion() {
    // local function f() return f() end; return f()
    // The closure reaches itself through a cell its creator fills in, as Lua's `local function` does. A closure may not
    // create a closure of its own proto, which the verifier refuses.
    let entry = proto(
        0,
        1,
        4,
        vec![],
        vec![
            Instr::Nil,
            Instr::NewCell(0),
            Instr::Closure(1),
            Instr::SetCell(0),
            Instr::GetCell(0),
            Instr::Call {
                argc: 0,
                results: 1,
            },
            Instr::Return(1),
        ],
    );
    let inner = proto(
        0,
        0,
        4,
        vec![UpvalueSource::Local(0)],
        vec![
            Instr::GetUpvalue(0),
            Instr::Call {
                argc: 0,
                results: 1,
            },
            Instr::Return(1),
        ],
    );
    let program = program_of(artifact(vec![], vec![], vec![entry, inner], vec![]));
    let mut vm = Machine::start(
        program,
        0,
        &[],
        &[],
        VmLimits {
            max_frames: 4,
            ..limits()
        },
    )
    .expect("starts");
    match run_to_end(&mut vm) {
        Step::Failed { code, .. } => assert_eq!(code, FailureCode::Capacity),
        other => panic!("expected capacity failure, got {other:?}"),
    }
}

#[test]
fn control_calls_are_refused_in_the_wrong_state() {
    let program = program_of(artifact(
        vec![],
        vec![],
        vec![proto(0, 0, 2, vec![], vec![Instr::Nil, Instr::Return(1)])],
        vec![],
    ));
    let mut vm = machine(program, &[]);
    assert_eq!(vm.resume_yield(&[]), Err(ControlError::NotYielded));
    assert_eq!(
        vm.complete(EffectResult::Task(1)),
        Err(ControlError::NotPending)
    );
    assert_eq!(returned(run_to_end(&mut vm)), vec![Value::Nil]);
    let name: String = String::from("done");
    assert_eq!(name.len(), 4);
}

fn native(argc: u8, results: u8) -> NativeRef {
    NativeRef {
        module: 1,
        operation: 1,
        argc,
        results,
    }
}

#[test]
fn a_zero_result_native_pushes_nothing_and_a_mismatched_completion_is_raised() {
    let code = vec![Instr::Native(0), Instr::Constant(0), Instr::Return(1)];
    let program = || {
        program_of(artifact(
            vec![int(5)],
            vec![native(0, 0)],
            vec![proto(0, 0, 4, vec![], code.clone())],
            vec![],
        ))
    };
    let mut vm = machine(program(), &[]);
    assert!(matches!(vm.run(100), Step::Pending(Effect::Native { .. })));
    vm.complete(EffectResult::Nothing).expect("complete");
    assert_eq!(returned(run_to_end(&mut vm)), vec![Value::Integer(5)]);

    let mut vm = machine(program(), &[]);
    assert!(matches!(vm.run(100), Step::Pending(Effect::Native { .. })));
    vm.complete(EffectResult::Value(Value::Integer(1)))
        .expect("complete");
    match run_to_end(&mut vm) {
        Step::Failed { message, .. } => assert!(
            String::from_utf8_lossy(&message).contains("different number of results"),
            "{}",
            String::from_utf8_lossy(&message)
        ),
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[test]
fn a_native_result_over_the_string_bound_is_refused_before_it_is_copied() {
    let program = program_of(artifact(
        vec![],
        vec![native(0, 1)],
        vec![proto(
            0,
            0,
            4,
            vec![],
            vec![Instr::Native(0), Instr::Return(1)],
        )],
        vec![],
    ));
    let mut vm = machine(program, &[]);
    assert!(matches!(vm.run(100), Step::Pending(Effect::Native { .. })));
    vm.complete(EffectResult::Value(Value::Bytes(vec![0; 65])))
        .expect("complete");
    assert!(matches!(run_to_end(&mut vm), Step::Failed { .. }));
}

#[test]
fn exports_are_checked_for_count_and_total_before_copying() {
    // Nine references to one string: one over the item bound.
    let mut code = vec![Instr::Constant(0), Instr::SetLocal(0)];
    for _ in 0..9 {
        code.push(Instr::GetLocal(0));
    }
    code.push(Instr::Return(9));
    let program = program_of(artifact(
        vec![text("abc")],
        vec![],
        vec![proto(0, 1, 16, vec![], code)],
        vec![],
    ));
    match run_to_end(&mut machine(program, &[])) {
        Step::Failed { message, .. } => {
            assert!(
                String::from_utf8_lossy(&message)
                    .contains("returned values are refused: too many values")
            )
        }
        other => panic!("expected a refusal, got {other:?}"),
    }

    // Three references to one 50-byte string: within the item and string bounds, over the 128-byte total.
    let code = vec![
        Instr::Constant(0),
        Instr::SetLocal(0),
        Instr::GetLocal(0),
        Instr::GetLocal(0),
        Instr::GetLocal(0),
        Instr::Native(0),
        Instr::Return(1),
    ];
    let program = program_of(artifact(
        vec![Constant::Bytes(vec![b'x'; 50])],
        vec![native(3, 1)],
        vec![proto(0, 1, 16, vec![], code)],
        vec![],
    ));
    match run_to_end(&mut machine(program, &[])) {
        Step::Failed { message, .. } => assert!(
            String::from_utf8_lossy(&message)
                .contains("native arguments are refused: the byte strings are too long in total"),
            "{}",
            String::from_utf8_lossy(&message)
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn a_multi_result_call_feeds_the_fixed_arguments_and_the_tail_of_the_next_call() {
    // return g(0, f()) where f returns 1, 2 and g returns all its arguments.
    let f = proto(
        0,
        0,
        4,
        vec![],
        vec![Instr::Constant(1), Instr::Constant(2), Instr::Return(2)],
    );
    let g = proto(
        3,
        3,
        4,
        vec![],
        vec![
            Instr::GetLocal(0),
            Instr::GetLocal(1),
            Instr::GetLocal(2),
            Instr::Return(3),
        ],
    );
    let entry = proto(
        0,
        0,
        8,
        vec![],
        vec![
            Instr::Closure(2),
            Instr::Constant(0),
            Instr::Closure(1),
            Instr::Call {
                argc: 0,
                results: RESULTS_MULTI,
            },
            Instr::CallMulti {
                fixed: 1,
                results: RESULTS_MULTI,
            },
            Instr::ReturnMulti(0),
        ],
    );
    let program = program_of(artifact(
        vec![int(0), int(1), int(2)],
        vec![],
        vec![entry, f, g],
        vec![],
    ));
    assert_eq!(
        returned(run_to_end(&mut machine(program, &[]))),
        vec![Value::Integer(0), Value::Integer(1), Value::Integer(2)]
    );
}

#[test]
fn unpack_multi_keeps_every_value_of_a_tuple_including_trailing_nils() {
    // return 9, pcall(f) where f returns nil, nil.
    let f = proto(
        0,
        0,
        4,
        vec![],
        vec![Instr::Nil, Instr::Nil, Instr::Return(2)],
    );
    let entry = proto(
        0,
        0,
        8,
        vec![],
        vec![
            Instr::Constant(0),
            Instr::Closure(1),
            Instr::Pcall(0),
            Instr::UnpackMulti,
            Instr::ReturnMulti(1),
        ],
    );
    let program = program_of(artifact(vec![int(9)], vec![], vec![entry, f], vec![]));
    assert_eq!(
        returned(run_to_end(&mut machine(program, &[]))),
        vec![
            Value::Integer(9),
            Value::Boolean(true),
            Value::Nil,
            Value::Nil
        ]
    );
}

#[test]
fn pack_multi_preserves_nil_arity_and_obeys_heap_budgets() {
    let make = || {
        artifact(
            vec![],
            vec![],
            vec![
                proto(
                    0,
                    0,
                    8,
                    vec![],
                    vec![
                        Instr::Closure(1),
                        Instr::Call {
                            argc: 0,
                            results: RESULTS_MULTI,
                        },
                        Instr::PackMulti(0),
                        Instr::UnpackMulti,
                        Instr::ReturnMulti(0),
                    ],
                ),
                proto(
                    0,
                    0,
                    4,
                    vec![],
                    vec![Instr::Nil, Instr::Nil, Instr::Return(2)],
                ),
            ],
            vec![],
        )
    };
    let mut exact = limits();
    exact.max_bytes = 80; // entry closure, producer closure, and a two-value tuple
    exact.max_objects = 3;
    let mut vm = Machine::start(program_of(make()), 0, &[], &[], exact).unwrap();
    assert_eq!(returned(run_to_end(&mut vm)), vec![Value::Nil, Value::Nil]);
    for constrained in [
        VmLimits {
            max_bytes: 79,
            ..exact
        },
        VmLimits {
            max_objects: 2,
            ..exact
        },
    ] {
        let mut vm = Machine::start(program_of(make()), 0, &[], &[], constrained).unwrap();
        assert!(matches!(
            run_to_end(&mut vm),
            Step::Failed {
                code: FailureCode::Capacity,
                ..
            }
        ));
    }
}

#[test]
fn dynamic_results_cannot_exceed_the_verified_stack_bound() {
    let program = program_of(artifact(
        vec![],
        vec![],
        vec![
            proto(
                0,
                0,
                2,
                vec![],
                vec![
                    Instr::Closure(1),
                    Instr::Call {
                        argc: 0,
                        results: RESULTS_MULTI,
                    },
                    Instr::PackMulti(0),
                    Instr::ReturnTuple,
                ],
            ),
            proto(
                0,
                0,
                2,
                vec![],
                vec![Instr::Nil, Instr::Nil, Instr::Return(2)],
            ),
        ],
        vec![],
    ));
    assert!(matches!(
        run_to_end(&mut machine(program, &[])),
        Step::Failed {
            code: FailureCode::Capacity,
            ..
        }
    ));
}

/// Runs `for i = start, limit, step do n = n + 1; last = i end; return n, last` with the loop instructions.
fn count_loop(start: Constant, limit: Constant, step: Constant) -> Step {
    // Locals: 0 start, 1 limit, 2 step, 3 i, 4 n, 5 last.
    let code = vec![
        Instr::Constant(0),
        Instr::SetLocal(0),
        Instr::Constant(1),
        Instr::SetLocal(1),
        Instr::Constant(2),
        Instr::SetLocal(2),
        Instr::Constant(3),
        Instr::SetLocal(4),
        Instr::ForPrep { slot: 0, exit: 18 },
        Instr::GetLocal(0),
        Instr::SetLocal(3),
        Instr::GetLocal(4),
        Instr::Constant(4),
        Instr::Add,
        Instr::SetLocal(4),
        Instr::GetLocal(3),
        Instr::SetLocal(5),
        Instr::ForLoop { slot: 0, body: 9 },
        Instr::GetLocal(4),
        Instr::GetLocal(5),
        Instr::Return(2),
    ];
    let program = program_of(artifact(
        vec![start, limit, step, int(0), int(1)],
        vec![],
        vec![proto(0, 6, 4, vec![], code)],
        vec![],
    ));
    run_to_end(&mut machine(program, &[]))
}

#[test]
fn integer_for_loops_stop_at_the_extreme_limits_without_wrapping() {
    assert_eq!(
        returned(count_loop(int(i64::MAX - 2), int(i64::MAX), int(1))),
        vec![Value::Integer(3), Value::Integer(i64::MAX)]
    );
    assert_eq!(
        returned(count_loop(int(i64::MIN + 2), int(i64::MIN), int(-1))),
        vec![Value::Integer(3), Value::Integer(i64::MIN)]
    );
    assert_eq!(
        returned(count_loop(int(i64::MAX - 5), int(i64::MAX), int(4))),
        vec![Value::Integer(2), Value::Integer(i64::MAX - 1)]
    );
    assert_eq!(
        returned(count_loop(int(1), int(0), int(1))),
        vec![Value::Integer(0), Value::Nil],
        "an empty loop runs zero times"
    );
    assert_eq!(
        returned(count_loop(int(0), int(i64::MIN), int(i64::MIN))),
        vec![Value::Integer(2), Value::Integer(i64::MIN)]
    );
}

#[test]
fn a_float_limit_is_clipped_for_an_integer_loop_and_floats_make_a_float_loop() {
    assert_eq!(
        returned(count_loop(int(1), Constant::Float(3.5), int(1))),
        vec![Value::Integer(3), Value::Integer(3)]
    );
    assert_eq!(
        returned(count_loop(int(3), Constant::Float(0.5), int(-1))),
        vec![Value::Integer(3), Value::Integer(1)]
    );
    assert_eq!(
        returned(count_loop(
            int(i64::MAX - 1),
            Constant::Float(1e300),
            int(1)
        )),
        vec![Value::Integer(2), Value::Integer(i64::MAX)]
    );
    assert_eq!(
        returned(count_loop(int(0), Constant::Float(-1e300), int(1))),
        vec![Value::Integer(0), Value::Nil]
    );
    assert_eq!(
        returned(count_loop(
            Constant::Float(0.0),
            int(1),
            Constant::Float(0.25)
        )),
        vec![Value::Integer(5), Value::Float(1.0)]
    );
}

#[test]
fn a_zero_step_and_a_non_number_are_runtime_errors() {
    for (start, limit, step, expected) in [
        (int(1), int(2), int(0), "'for' step is zero"),
        (
            Constant::Float(1.0),
            int(2),
            Constant::Float(0.0),
            "'for' step is zero",
        ),
        (int(1), text("2"), int(1), "'for' limit must be a number"),
        (
            text("1"),
            int(2),
            Constant::Float(1.0),
            "'for' initial value must be a number",
        ),
        (
            text("1"),
            Constant::Float(2.0),
            Constant::Float(1.0),
            "'for' initial value must be a number",
        ),
        (int(1), int(2), text("x"), "'for' step must be a number"),
    ] {
        match count_loop(start, limit, step) {
            Step::Failed { message, .. } => {
                assert_eq!(String::from_utf8_lossy(&message), expected)
            }
            other => panic!("expected {expected}, got {other:?}"),
        }
    }
}

/// Runs `left .. right` for two constants. It returns the result, or the failure text.
fn concat_of(left: Constant, right: Constant) -> Result<Value, String> {
    let code = vec![
        Instr::Constant(0),
        Instr::Constant(1),
        Instr::Concat,
        Instr::Return(1),
    ];
    let program = program_of(artifact(
        vec![left, right],
        vec![],
        vec![proto(0, 0, 4, vec![], code)],
        vec![],
    ));
    match run_to_end(&mut machine(program, &[])) {
        Step::Returned(tuple) => Ok(values(&tuple).remove(0)),
        Step::Failed { message, .. } => Err(String::from_utf8_lossy(&message).into_owned()),
        other => panic!("expected a result or a failure, got {other:?}"),
    }
}

/// Golden values of `..` for integers and strings. Stock Lua 5.4 gives the same bytes for each pair, in both operand
/// orders; the central crate's differential test checks them against `mlua`.
#[test]
fn integer_and_string_concatenation_matches_stock_lua_in_both_orders() {
    let cases: &[(Constant, Constant, &[u8])] = &[
        (text("ab"), int(1), b"ab1"),
        (int(1), text("ab"), b"1ab"),
        (int(i64::MIN), text(""), b"-9223372036854775808"),
        (int(i64::MAX), text("x"), b"9223372036854775807x"),
        (int(-1), int(-1), b"-1-1"),
        (int(0), int(0), b"00"),
        (int(10), int(10), b"1010"),
        (text("\0\u{ff}"), int(-7), b"\0\xc3\xbf-7"),
        (text(""), text(""), b""),
    ];
    for (left, right, expected) in cases {
        assert_eq!(
            concat_of(left.clone(), right.clone()),
            Ok(Value::Bytes(expected.to_vec())),
            "{left:?} .. {right:?}"
        );
    }
}

/// Golden values of `..` with a float operand. Each text is what stock Lua 5.4 gives, in both operand orders; the central
/// crate's differential test compares a much larger matrix against `mlua` byte for byte.
#[test]
fn float_concatenation_matches_stock_lua_in_both_orders() {
    let cases: &[(f64, &str)] = &[
        (1.0, "1.0"),
        (-1.0, "-1.0"),
        (0.0, "0.0"),
        (-0.0, "-0.0"),
        (0.5, "0.5"),
        (0.1, "0.1"),
        (100.0, "100.0"),
        (1e14, "1e+14"),
        (1e15, "1e+15"),
        (99_999_999_999_999.0, "99999999999999.0"),
        (123_456.789, "123456.789"),
        (0.0001, "0.0001"),
        (0.00001, "1e-05"),
        (1e100, "1e+100"),
        (f64::MAX, "1.7976931348623e+308"),
        (f64::MIN_POSITIVE, "2.2250738585072e-308"),
        (5e-324, "4.9406564584125e-324"),
        (1.0 / 3.0, "0.33333333333333"),
        (9_007_199_254_740_992.0, "9.007199254741e+15"),
    ];
    for &(value, expected) in cases {
        let float = Constant::Float(value);
        assert_eq!(
            concat_of(float.clone(), text("x")),
            Ok(Value::Bytes(format!("{expected}x").into_bytes())),
            "{value:?} .. \"x\""
        );
        assert_eq!(
            concat_of(text("x"), float.clone()),
            Ok(Value::Bytes(format!("x{expected}").into_bytes())),
            "\"x\" .. {value:?}"
        );
    }
    // Integers keep their form, and a mix of the two uses each one's own.
    assert_eq!(
        concat_of(Constant::Float(2.0), int(2)),
        Ok(Value::Bytes(b"2.02".to_vec()))
    );
    assert_eq!(
        concat_of(int(2), Constant::Float(2.0)),
        Ok(Value::Bytes(b"22.0".to_vec()))
    );
    assert_eq!(
        concat_of(Constant::Float(1.5), Constant::Float(-2.5)),
        Ok(Value::Bytes(b"1.5-2.5".to_vec()))
    );
}

/// Non-finite floats cannot be constants, but arithmetic produces them at run time: `1/0`, `-1/0`, `0/0` and `-(0/0)`.
#[test]
fn non_finite_results_concatenate_as_the_c_library_prints_them() {
    use core::hint::black_box;
    let divide = |a: i64, b: i64, negate: bool| {
        let mut code = vec![Instr::Constant(0), Instr::Constant(1), Instr::Div];
        if negate {
            code.push(Instr::Neg);
        }
        code.extend([Instr::Constant(2), Instr::Concat, Instr::Return(1)]);
        let program = program_of(artifact(
            vec![int(a), int(b), text("|")],
            vec![],
            vec![proto(0, 0, 4, vec![], code)],
            vec![],
        ));
        returned(run_to_end(&mut machine(program, &[])))
    };
    // The reference NaN comes from the same hardware division, so its sign bit is the one the VM computes.
    let zero = black_box(0.0_f64);
    let nan = zero / black_box(0.0_f64);
    let expect = |value: f64| {
        let mut text = crate::number::float_text(value).as_bytes().to_vec();
        text.push(b'|');
        vec![Value::Bytes(text)]
    };
    assert_eq!(divide(1, 0, false), vec![Value::Bytes(b"inf|".to_vec())]);
    assert_eq!(divide(-1, 0, false), vec![Value::Bytes(b"-inf|".to_vec())]);
    assert_eq!(divide(0, 0, false), expect(nan));
    assert_eq!(divide(0, 0, true), expect(-nan));
}

/// A NaN prints with its sign bit, as the C library does, so the text matches stock Lua built on the same platform.
#[test]
fn a_nan_prints_with_its_sign_as_the_c_library_does() {
    let positive = f64::from_bits(0x7ff8_0000_0000_0000);
    let negative = f64::from_bits(0xfff8_0000_0000_0000);
    // The artifact verifier refuses non-finite constants, so the values arise at run time, as `0/0` does.
    for (value, expected) in [(positive, "nan"), (negative, "-nan")] {
        assert_eq!(
            crate::number::float_text(value).as_bytes(),
            expected.as_bytes()
        );
    }
}

/// A float operand obeys the same admission as any operand: the result is sized with checked arithmetic and refused
/// against the heap budget before its buffer is reserved.
#[test]
fn a_float_concatenation_over_the_budget_is_a_capacity_fault() {
    let each = text(&"a".repeat(40));
    let code = vec![
        Instr::Constant(0),
        Instr::Constant(1),
        Instr::Concat,
        Instr::Return(1),
    ];
    let program = program_of(artifact(
        vec![each, Constant::Float(0.1)],
        vec![],
        vec![proto(0, 0, 4, vec![], code)],
        vec![],
    ));
    // The 40-byte constant charges 56. The result of 43 bytes would charge 59 more.
    let mut machine = Machine::start(
        program,
        0,
        &[],
        &[],
        VmLimits {
            max_bytes: 100,
            ..limits()
        },
    )
    .expect("the constant fits");
    match run_to_end(&mut machine) {
        Step::Failed { code, message } => {
            assert_eq!(code, FailureCode::Capacity);
            assert_eq!(message, b"heap budget exhausted");
        }
        other => panic!("expected a capacity failure, got {other:?}"),
    }
}

/// Starting needs one frame for the entry, so a machine that permits none is refused whatever the program is.
#[test]
fn a_machine_that_permits_no_frames_is_refused_at_start() {
    let trivial = || {
        program_of(artifact(
            vec![],
            vec![],
            vec![proto(0, 0, 1, vec![], vec![Instr::Return(0)])],
            vec![],
        ))
    };
    let refused = Machine::start(
        trivial(),
        0,
        &[],
        &[],
        VmLimits {
            max_frames: 0,
            ..limits()
        },
    );
    assert_eq!(refused.err(), Some(StartError::Capacity));

    // One frame runs the entry, and a call from it then exceeds the depth.
    let mut one = Machine::start(
        trivial(),
        0,
        &[],
        &[],
        VmLimits {
            max_frames: 1,
            ..limits()
        },
    )
    .expect("one frame runs the entry");
    assert_eq!(returned(run_to_end(&mut one)), vec![]);
}

fn call_into_a_second_frame() -> Program {
    program_of(artifact(
        vec![],
        vec![],
        vec![
            proto(
                0,
                0,
                2,
                vec![],
                vec![
                    Instr::Closure(1),
                    Instr::Pcall(0),
                    Instr::Unpack(2),
                    Instr::Return(2),
                ],
            ),
            proto(0, 0, 1, vec![], vec![Instr::Return(0)]),
        ],
        vec![],
    ))
}

#[test]
fn a_call_beyond_the_permitted_depth_is_a_capacity_fault() {
    let mut machine = Machine::start(
        call_into_a_second_frame(),
        0,
        &[],
        &[],
        VmLimits {
            max_frames: 1,
            ..limits()
        },
    )
    .expect("starts");
    match run_to_end(&mut machine) {
        Step::Failed { code, message } => {
            assert_eq!(code, FailureCode::Capacity);
            assert_eq!(message, b"call depth exhausted");
        }
        other => panic!("expected a capacity failure, got {other:?}"),
    }
}

/// `pcall(function() error("boom") end)` under `limits`, with the error text as a constant.
fn failing_pcall(limits: VmLimits) -> Step {
    let entry = proto(
        0,
        0,
        4,
        vec![],
        vec![
            Instr::Closure(1),
            Instr::Pcall(0),
            Instr::Unpack(2),
            Instr::Return(2),
        ],
    );
    let thrower = proto(0, 0, 4, vec![], vec![Instr::Constant(0), Instr::Raise]);
    let program = program_of(artifact(
        vec![text("boom")],
        vec![],
        vec![entry, thrower],
        vec![],
    ));
    run_to_end(&mut Machine::start(program, 0, &[], &[], limits).expect("starts"))
}

/// The entry closure charges 16, the constant 20 and the thrower 16. The caught error then needs its text (20) and a
/// two-element tuple (48). A budget that fits the text but not the tuple, or an object count that does the same, makes
/// the delivery of the error fail. That is a capacity failure. It must not be reported as the `boom` that was caught.
#[test]
fn a_protected_error_that_cannot_be_built_is_a_capacity_failure_not_the_caught_error() {
    for limits in [
        VmLimits {
            max_bytes: 100,
            ..limits()
        },
        VmLimits {
            max_objects: 4,
            ..limits()
        },
    ] {
        match failing_pcall(limits) {
            Step::Failed { code, message } => {
                assert_eq!(
                    code,
                    FailureCode::Capacity,
                    "{}",
                    String::from_utf8_lossy(&message)
                );
                assert_eq!(message, b"heap budget exhausted");
            }
            other => panic!("expected a capacity failure, got {other:?}"),
        }
    }
    // With room, the same program delivers the caught error.
    assert_eq!(
        returned(failing_pcall(limits())),
        vec![Value::Boolean(false), Value::Bytes(b"boom".to_vec())]
    );
}

/// The `{true, ...}` a protected call delivers is a tuple charged against both budgets, like any other.
fn pcall_returning(count: u8, limits: VmLimits) -> Step {
    let entry = proto(
        0,
        0,
        4,
        vec![],
        vec![
            Instr::Closure(1),
            Instr::Pcall(0),
            Instr::Unpack(1),
            Instr::Return(1),
        ],
    );
    let mut code: Vec<Instr> = (0..count).map(|_| Instr::Constant(0)).collect();
    code.push(Instr::Return(count));
    let callee = proto(0, 0, u16::from(count).max(1), vec![], code);
    let program = program_of(artifact(vec![int(7)], vec![], vec![entry, callee], vec![]));
    run_to_end(&mut Machine::start(program, 0, &[], &[], limits).expect("starts"))
}

#[test]
fn a_protected_result_over_the_budget_is_a_capacity_failure() {
    // Thirty results make a tuple of 31 elements: 16 + 31 × 16 = 512 bytes.
    for limits in [
        VmLimits {
            max_bytes: 100,
            ..limits()
        },
        VmLimits {
            max_objects: 2,
            ..limits()
        },
    ] {
        match pcall_returning(30, limits) {
            Step::Failed { code, message } => {
                assert_eq!(code, FailureCode::Capacity);
                assert_eq!(message, b"heap budget exhausted");
            }
            other => panic!("expected a capacity failure, got {other:?}"),
        }
    }
}

/// A closure whose capture list does not fit the budget is refused as a capacity fault.
#[test]
fn a_closure_over_the_budget_is_a_capacity_failure() {
    let sources: Vec<UpvalueSource> = (0..8).map(UpvalueSource::Upvalue).collect();
    let entry = proto(0, 0, 2, vec![], vec![Instr::Closure(1), Instr::Return(0)]);
    let inner = proto(0, 0, 1, sources, vec![Instr::Return(0)]);
    let mut art = artifact(vec![], vec![], vec![entry, inner], vec![]);
    art.entries = vec![Entry {
        proto: 0,
        captures: 8,
    }];
    let captures = vec![Value::Nil; 8];
    // The entry closure (16 + 8 × 4 = 48) and eight cells (8 × 16) charge 176. The inner closure needs 48 more.
    for limits in [
        VmLimits {
            max_bytes: 200,
            ..limits()
        },
        VmLimits {
            max_objects: 9,
            ..limits()
        },
    ] {
        let mut machine =
            Machine::start(program_of(art.clone()), 0, &captures, &[], limits).expect("starts");
        match run_to_end(&mut machine) {
            Step::Failed { code, message } => {
                assert_eq!(code, FailureCode::Capacity);
                assert_eq!(message, b"heap budget exhausted");
            }
            other => panic!("expected a capacity failure, got {other:?}"),
        }
    }
    let mut roomy = Machine::start(program_of(art), 0, &captures, &[], limits()).expect("starts");
    assert_eq!(returned(run_to_end(&mut roomy)), vec![]);
}

/// An allocator refusal on the concatenation's buffer, or on a frame's locals, is a capacity fault. It is not an abort.
#[test]
fn allocator_refusals_on_the_hardened_paths_are_capacity_faults() {
    use crate::heap::{Val, byte_buffer, filled};
    use crate::machine::Fault;
    assert!(matches!(
        byte_buffer(usize::MAX),
        Err(Fault::Capacity("heap allocation failed"))
    ));
    assert!(matches!(
        filled(Val::Nil, usize::MAX),
        Err(Fault::Capacity("frame allocation failed"))
    ));
    assert!(byte_buffer(3).is_ok_and(|buffer| buffer.capacity() >= 3));
}
