//! Round-trip, bounds and verifier tests. Artifacts are built by hand, so each rejection is exercised in isolation.

use alloc::vec;
use alloc::vec::Vec;

use crate::format::{
    DIGEST_LEN, Entry, Instr, Limits, NativeRef, Proto, RESULTS_MULTI, UpvalueSource,
};
use crate::{Artifact, ChildLink, Constant, DecodeError, VerifyError, digest};

const CONTRACT: [u8; DIGEST_LEN] = [0x11; DIGEST_LEN];

fn proto(code: Vec<Instr>) -> Proto {
    let lines = vec![1; code.len()];
    Proto {
        params: 0,
        locals: 4,
        max_stack: 8,
        upvalues: Vec::new(),
        code,
        lines,
    }
}

fn artifact(protos: Vec<Proto>) -> Artifact {
    Artifact {
        contract: CONTRACT,
        constants: vec![Constant::Integer(1), Constant::Bytes(b"ok".to_vec())],
        natives: Vec::new(),
        protos,
        entries: vec![Entry {
            proto: 0,
            captures: 0,
        }],
        children: Vec::new(),
    }
}

fn limits() -> Limits {
    Limits::profile()
}

fn valid_program() -> Artifact {
    artifact(vec![proto(vec![Instr::Constant(0), Instr::Return(1)])])
}

#[test]
fn a_valid_artifact_round_trips_and_verifies() {
    let original = valid_program();
    let bytes = original.encode();
    let decoded = Artifact::decode_verified(&bytes, &limits()).expect("valid");
    assert_eq!(decoded, original);
    assert_eq!(decoded.encode(), bytes, "encoding is canonical");
}

#[test]
fn equal_artifacts_have_equal_digests_and_changed_ones_do_not() {
    let bytes = valid_program().encode();
    assert_eq!(digest(&bytes), digest(&valid_program().encode()));
    let mut other = valid_program();
    other.constants[0] = Constant::Integer(2);
    assert_ne!(digest(&bytes), digest(&other.encode()));
}

#[test]
fn bad_magic_and_versions_are_refused() {
    let mut bytes = valid_program().encode();
    bytes[0] = b'X';
    assert_eq!(
        Artifact::decode(&bytes, &limits()),
        Err(DecodeError::BadMagic)
    );

    let mut bytes = valid_program().encode();
    bytes[4] = 9;
    assert_eq!(
        Artifact::decode(&bytes, &limits()),
        Err(DecodeError::UnsupportedVersion {
            field: "format",
            found: 9
        })
    );
}

#[test]
fn old_abi_is_rejected_even_when_the_layout_is_unchanged() {
    let mut old_format = valid_program().encode();
    old_format[4..6].copy_from_slice(&3u16.to_le_bytes());
    old_format[8..10].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        Artifact::decode(&old_format, &limits()),
        Err(DecodeError::UnsupportedVersion {
            field: "format",
            found: 3
        })
    );

    let mut bytes = valid_program().encode();
    bytes[8..10].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        Artifact::decode(&bytes, &limits()),
        Err(DecodeError::UnsupportedVersion {
            field: "abi",
            found: 2
        })
    );
}

#[test]
fn an_old_abi_child_is_rejected_even_with_a_matching_digest() {
    let mut child = valid_program().encode();
    child[8..10].copy_from_slice(&2u16.to_le_bytes());
    let mut parent = valid_program();
    parent.children.push(ChildLink {
        slot: 1,
        digest: digest(&child),
        entry: 0,
        bytes: child.into(),
    });
    assert!(
        matches!(parent.verify(&limits()), Err(VerifyError::ChildInvalid { ref path, ref cause }) if path == &[0] && matches!(**cause, DecodeError::UnsupportedVersion { field: "abi", found: 2 }))
    );
}

#[test]
fn new_marker_consumers_round_trip_and_check_stack_and_jumps() {
    for consumer in [Instr::PcallList, Instr::PackMulti(0)] {
        let art = artifact(vec![proto(vec![
            Instr::Nil,
            Instr::UnpackMulti,
            consumer,
            Instr::Return(1),
        ])]);
        let bytes = art.encode();
        assert_eq!(Artifact::decode_verified(&bytes, &limits()).unwrap(), art);
        let jumped = artifact(vec![proto(vec![
            Instr::Nil,
            Instr::UnpackMulti,
            consumer,
            Instr::Return(1),
            Instr::Jump(2),
        ])]);
        assert!(matches!(
            jumped.verify(&limits()),
            Err(VerifyError::MultiResultPattern { .. })
        ));
    }
    let underflow = artifact(vec![proto(vec![
        Instr::Nil,
        Instr::UnpackMulti,
        Instr::PackMulti(1),
        Instr::Return(1),
    ])]);
    assert!(matches!(
        underflow.verify(&limits()),
        Err(VerifyError::StackUnderflow { .. })
    ));
}

#[test]
fn truncation_and_trailing_bytes_are_refused() {
    let bytes = valid_program().encode();
    for cut in 0..bytes.len() {
        assert!(
            Artifact::decode(&bytes[..cut], &limits()).is_err(),
            "prefix of {cut} bytes must not decode"
        );
    }
    let mut extra = bytes.clone();
    extra.push(0);
    assert_eq!(
        Artifact::decode(&extra, &limits()),
        Err(DecodeError::TrailingBytes)
    );
}

#[test]
fn counts_larger_than_the_input_are_refused_before_allocation() {
    let mut bytes = valid_program().encode();
    // The constant count sits right after the 42-byte header: magic, three versions and the contract digest.
    bytes[42..46].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        Artifact::decode(&bytes, &limits()),
        Err(DecodeError::LimitExceeded {
            what: "constants",
            ..
        })
    ));
    bytes[42..46].copy_from_slice(&1000u32.to_le_bytes());
    assert_eq!(
        Artifact::decode(&bytes, &limits()),
        Err(DecodeError::Truncated)
    );
}

#[test]
fn an_unknown_opcode_and_a_non_finite_float_are_refused() {
    // The first instruction of the valid program is `Constant(0)`: opcode 4, then its operand. Replace the opcode.
    let mut bytes = valid_program().encode();
    let needle = [4u8, 0, 0, 0];
    let at = bytes
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("constant instruction present");
    bytes[at] = 200;
    assert_eq!(
        Artifact::decode(&bytes, &limits()),
        Err(DecodeError::UnknownOpcode(200))
    );

    let mut floats = valid_program();
    floats.constants = vec![Constant::Float(f64::NAN)];
    assert_eq!(
        Artifact::decode(&floats.encode(), &limits()),
        Err(DecodeError::NonFiniteFloat)
    );
}

#[test]
fn stack_underflow_is_refused() {
    let bad = artifact(vec![proto(vec![
        Instr::Constant(0),
        Instr::Add,
        Instr::Return(1),
    ])]);
    assert!(matches!(
        bad.verify(&limits()),
        Err(VerifyError::StackUnderflow { at: 1, .. })
    ));
}

#[test]
fn inconsistent_heights_at_a_join_are_refused() {
    // The fall-through path reaches index 5 with one value on the stack. The jump at index 4 reaches it with none.
    let code = vec![
        Instr::True,
        Instr::JumpIfFalse(4),
        Instr::Constant(0),
        Instr::Jump(5),
        Instr::Jump(5),
        Instr::Return(0),
    ];
    let bad = artifact(vec![proto(code)]);
    assert!(matches!(
        bad.verify(&limits()),
        Err(VerifyError::InconsistentStack { at: 5, .. })
    ));
}

#[test]
fn falling_off_the_end_is_refused() {
    let bad = artifact(vec![proto(vec![Instr::Constant(0)])]);
    assert_eq!(
        bad.verify(&limits()),
        Err(VerifyError::FallsOffEnd { proto: 0 })
    );
}

#[test]
fn stack_overflow_beyond_the_declared_bound_is_refused() {
    let mut p = proto(vec![
        Instr::Constant(0),
        Instr::Constant(0),
        Instr::Return(2),
    ]);
    p.max_stack = 1;
    assert!(matches!(
        artifact(vec![p]).verify(&limits()),
        Err(VerifyError::StackOverflow { .. })
    ));
}

#[test]
fn out_of_range_operands_are_refused() {
    let constant = artifact(vec![proto(vec![Instr::Constant(9), Instr::Return(1)])]);
    assert!(matches!(
        constant.verify(&limits()),
        Err(VerifyError::ConstantOutOfRange { .. })
    ));

    let local = artifact(vec![proto(vec![
        Instr::Constant(0),
        Instr::SetLocal(40),
        Instr::Return(0),
    ])]);
    assert!(matches!(
        local.verify(&limits()),
        Err(VerifyError::LocalOutOfRange { .. })
    ));

    let upvalue = artifact(vec![proto(vec![Instr::GetUpvalue(0), Instr::Return(1)])]);
    assert!(matches!(
        upvalue.verify(&limits()),
        Err(VerifyError::UpvalueOutOfRange { .. })
    ));

    let jump = artifact(vec![proto(vec![Instr::Jump(7)])]);
    assert!(matches!(
        jump.verify(&limits()),
        Err(VerifyError::JumpOutOfRange { .. })
    ));

    let native = artifact(vec![proto(vec![Instr::Native(0)])]);
    assert!(matches!(
        native.verify(&limits()),
        Err(VerifyError::NativeOutOfRange { .. })
    ));
}

#[test]
fn a_multi_result_call_needs_its_return_and_a_return_needs_its_call() {
    let no_return = artifact(vec![proto(vec![
        Instr::Constant(0),
        Instr::Call {
            argc: 0,
            results: RESULTS_MULTI,
        },
        Instr::Return(0),
    ])]);
    assert!(matches!(
        no_return.verify(&limits()),
        Err(VerifyError::MultiResultPattern { .. })
    ));

    let stray = artifact(vec![proto(vec![Instr::Constant(0), Instr::ReturnMulti(0)])]);
    assert!(matches!(
        stray.verify(&limits()),
        Err(VerifyError::MultiResultPattern { .. })
    ));

    let paired = artifact(vec![
        proto(vec![
            Instr::Constant(0),
            Instr::Call {
                argc: 0,
                results: RESULTS_MULTI,
            },
            Instr::ReturnMulti(0),
        ]),
        proto(vec![Instr::Constant(0), Instr::Return(1)]),
    ]);
    // Proto 1 is never a closure target, so it is a legal extra proto; the paired proto verifies.
    assert_eq!(paired.verify(&limits()), Ok(()));
}

#[test]
fn closures_must_target_a_later_proto_and_entries_must_be_roots() {
    let backwards = artifact(vec![proto(vec![Instr::Closure(0), Instr::Return(1)])]);
    assert!(matches!(
        backwards.verify(&limits()),
        Err(VerifyError::ClosureTargetOrder { proto: 0, at: 0 })
    ));

    let two_roots = artifact(vec![
        proto(vec![Instr::Constant(0), Instr::Return(1)]),
        proto(vec![Instr::Constant(0), Instr::Return(1)]),
    ]);
    let mut roots = two_roots.clone();
    roots.entries = vec![
        Entry {
            proto: 0,
            captures: 0,
        },
        Entry {
            proto: 1,
            captures: 0,
        },
    ];
    assert_eq!(roots.verify(&limits()), Ok(()));

    let mut shared = two_roots.clone();
    shared.entries = vec![
        Entry {
            proto: 0,
            captures: 0,
        },
        Entry {
            proto: 0,
            captures: 0,
        },
    ];
    assert_eq!(
        shared.verify(&limits()),
        Err(VerifyError::DuplicateEntry { proto: 0 })
    );

    // Proto 1 is created by a closure, so it cannot also be a root.
    let created = artifact(vec![
        proto(vec![Instr::Closure(1), Instr::Return(1)]),
        proto(vec![Instr::Constant(0), Instr::Return(1)]),
    ]);
    let mut as_root = created.clone();
    as_root.entries = vec![Entry {
        proto: 1,
        captures: 0,
    }];
    assert_eq!(
        as_root.verify(&limits()),
        Err(VerifyError::EntryIsClosureTarget { proto: 1 })
    );
}

#[test]
fn a_closure_with_captures_verifies_and_a_bad_source_is_refused() {
    let mut inner = proto(vec![Instr::GetUpvalue(0), Instr::Return(1)]);
    inner.upvalues = vec![UpvalueSource::Local(0)];
    let good = artifact(vec![
        proto(vec![
            Instr::Constant(0),
            Instr::NewCell(0),
            Instr::Closure(1),
            Instr::Return(1),
        ]),
        inner,
    ]);
    assert_eq!(good.verify(&limits()), Ok(()));

    let mut bad_inner = good.protos[1].clone();
    bad_inner.upvalues = vec![UpvalueSource::Local(99)];
    let bad = artifact(vec![good.protos[0].clone(), bad_inner]);
    assert!(matches!(
        bad.verify(&limits()),
        Err(VerifyError::ClosureSourceOutOfRange { .. })
    ));
}

#[test]
fn a_spawn_needs_a_present_child_with_a_matching_digest() {
    let child = valid_program();
    let child_bytes = child.encode();
    let mut parent = artifact(vec![proto(vec![
        Instr::Constant(0),
        Instr::Spawn {
            child: 0,
            argc: 0,
            captures: 0,
        },
        Instr::Await,
        Instr::Return(1),
    ])]);
    parent.children = vec![ChildLink {
        slot: 1,
        digest: digest(&child_bytes),
        entry: 0,
        bytes: child_bytes.clone().into(),
    }];
    assert_eq!(parent.verify(&limits()), Ok(()));

    let mut wrong_digest = parent.clone();
    wrong_digest.children[0].digest = [7; 32];
    assert_eq!(
        wrong_digest.verify(&limits()),
        Err(VerifyError::ChildDigestWrong { child: 0 })
    );

    let mut missing = parent.clone();
    missing.children.clear();
    assert!(matches!(
        missing.verify(&limits()),
        Err(VerifyError::ChildOutOfRange { .. })
    ));
}

#[test]
fn nesting_beyond_the_depth_limit_is_refused() {
    let mut level = valid_program();
    for _ in 0..4 {
        let bytes = level.encode();
        level = artifact(vec![proto(vec![
            Instr::Constant(0),
            Instr::Spawn {
                child: 0,
                argc: 0,
                captures: 0,
            },
            Instr::Await,
            Instr::Return(1),
        ])]);
        level.children = vec![ChildLink {
            slot: 1,
            digest: digest(&bytes),
            entry: 0,
            bytes: bytes.into(),
        }];
    }
    let bytes = level.encode();
    let err = Artifact::decode_verified(&bytes, &limits()).expect_err("too deep");
    assert_eq!(root_cause(&err), &DecodeError::DepthExceeded);
}

/// Follows nested child failures down to the innermost error.
fn root_cause(err: &DecodeError) -> &DecodeError {
    match err {
        DecodeError::Verify(VerifyError::ChildInvalid { cause, .. }) => root_cause(cause),
        other => other,
    }
}

#[test]
fn a_child_that_is_invalid_is_reported_with_its_index() {
    let bad_child = artifact(vec![proto(vec![Instr::Constant(0)])]).encode();
    let mut parent = artifact(vec![proto(vec![
        Instr::Constant(0),
        Instr::Spawn {
            child: 0,
            argc: 0,
            captures: 0,
        },
        Instr::Await,
        Instr::Return(1),
    ])]);
    parent.children = vec![ChildLink {
        slot: 1,
        digest: digest(&bad_child),
        entry: 0,
        bytes: bad_child.into(),
    }];
    assert!(matches!(
        parent.verify(&limits()),
        Err(VerifyError::ChildInvalid { ref path, .. }) if path == &[0]
    ));
}

#[test]
fn an_empty_or_entry_less_artifact_is_refused() {
    let mut none = valid_program();
    none.entries.clear();
    assert_eq!(none.verify(&limits()), Err(VerifyError::NoEntry));
}

#[test]
fn the_contract_digest_is_encoded_and_decoded() {
    let mut other = valid_program();
    other.contract = [0x22; DIGEST_LEN];
    let bytes = valid_program().encode();
    assert_ne!(
        bytes,
        other.encode(),
        "a different contract changes the bytes"
    );
    let decoded = Artifact::decode(&bytes, &limits()).expect("decodes");
    assert_eq!(decoded.contract, CONTRACT);
}

#[test]
fn a_format_version_one_artifact_is_refused() {
    let mut bytes = valid_program().encode();
    bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
    assert_eq!(
        Artifact::decode(&bytes, &limits()),
        Err(DecodeError::UnsupportedVersion {
            field: "format",
            found: 1
        })
    );
}

#[test]
fn canonical_decoding_accepts_the_encoder_output_and_refuses_trailing_bytes() {
    let bytes = valid_program().encode();
    let decoded = Artifact::decode_canonical(&bytes, &limits()).expect("canonical bytes decode");
    assert_eq!(decoded.encode(), bytes);

    let mut padded = bytes.clone();
    padded.push(0);
    assert_eq!(
        Artifact::decode_canonical(&padded, &limits()),
        Err(DecodeError::TrailingBytes)
    );
}

#[test]
fn every_marker_consumer_needs_a_producer_right_before_it() {
    let consumers = [
        Instr::CallMulti {
            fixed: 0,
            results: 1,
        },
        Instr::PcallMulti(0),
        Instr::PcallList,
        Instr::PackMulti(0),
        Instr::YieldMulti(0),
    ];
    for consumer in consumers {
        let stray = artifact(vec![proto(vec![
            Instr::Constant(0),
            Instr::Constant(0),
            consumer,
            Instr::Return(1),
        ])]);
        assert!(
            matches!(
                stray.verify(&limits()),
                Err(VerifyError::MultiResultPattern { .. })
            ),
            "{consumer:?} without a producer"
        );
        let paired = artifact(vec![proto(vec![
            Instr::Constant(0),
            Instr::Constant(0),
            Instr::Call {
                argc: 0,
                results: RESULTS_MULTI,
            },
            consumer,
            Instr::Return(1),
        ])]);
        assert_eq!(
            paired.verify(&limits()),
            Ok(()),
            "{consumer:?} after a producer"
        );
    }
}

#[test]
fn a_producer_must_hand_its_marker_to_a_consumer() {
    let lost = artifact(vec![proto(vec![
        Instr::Constant(0),
        Instr::Unpack(0),
        Instr::Constant(0),
        Instr::UnpackMulti,
        Instr::Pop,
        Instr::Return(0),
    ])]);
    assert!(matches!(
        lost.verify(&limits()),
        Err(VerifyError::MultiResultPattern { at: 3, .. })
    ));
}

#[test]
fn a_consumer_that_is_a_jump_target_is_refused() {
    let jumped = artifact(vec![proto(vec![
        Instr::Constant(0),
        Instr::Constant(0),
        Instr::UnpackMulti,
        Instr::ReturnMulti(0),
        Instr::Jump(3),
    ])]);
    assert!(matches!(
        jumped.verify(&limits()),
        Err(VerifyError::MultiResultPattern { .. })
    ));
}

#[test]
fn numeric_for_instructions_check_their_slots_and_targets() {
    let body = vec![
        Instr::Constant(0),
        Instr::SetLocal(0),
        Instr::Constant(0),
        Instr::SetLocal(1),
        Instr::Constant(0),
        Instr::SetLocal(2),
        Instr::ForPrep { slot: 0, exit: 9 },
        Instr::GetLocal(0),
        Instr::Pop,
        Instr::ForLoop { slot: 0, body: 7 },
        Instr::Return(0),
    ];
    let mut fixed = body.clone();
    fixed[6] = Instr::ForPrep { slot: 0, exit: 10 };
    assert_eq!(
        artifact(vec![proto(fixed.clone())]).verify(&limits()),
        Ok(())
    );
    let round = artifact(vec![proto(fixed)]);
    let bytes = round.encode();
    assert_eq!(Artifact::decode_verified(&bytes, &limits()), Ok(round));

    let mut out_of_slots = body.clone();
    out_of_slots[6] = Instr::ForPrep { slot: 2, exit: 10 };
    assert!(matches!(
        artifact(vec![proto(out_of_slots)]).verify(&limits()),
        Err(VerifyError::LocalOutOfRange { at: 6, .. })
    ));
    let mut out_of_code = body;
    out_of_code[9] = Instr::ForLoop { slot: 0, body: 99 };
    assert!(matches!(
        artifact(vec![proto(out_of_code)]).verify(&limits()),
        Err(VerifyError::JumpOutOfRange { at: 9, .. })
    ));
}

#[test]
fn a_native_with_more_results_than_the_abi_is_refused() {
    let mut program = artifact(vec![proto(vec![Instr::Native(0), Instr::Return(1)])]);
    program.natives = vec![crate::NativeRef {
        module: 1,
        operation: 1,
        argc: 0,
        results: 1,
    }];
    assert_eq!(program.verify(&limits()), Ok(()));
    program.natives[0].results = 2;
    assert_eq!(
        program.verify(&limits()),
        Err(VerifyError::NativeResultsUnsupported { native: 0 })
    );
    program.natives[0].results = 0;
    assert!(
        matches!(
            program.verify(&limits()),
            Err(VerifyError::StackUnderflow { .. })
        ),
        "a zero-result native pushes nothing"
    );
}

#[test]
fn the_slot_of_a_child_edge_survives_encoding() {
    let child_bytes = valid_program().encode();
    let mut parent = artifact(vec![proto(vec![
        Instr::Spawn {
            child: 0,
            argc: 0,
            captures: 0,
        },
        Instr::Return(1),
    ])]);
    parent.children = vec![ChildLink {
        slot: 7,
        digest: digest(&child_bytes),
        entry: 0,
        bytes: child_bytes.into(),
    }];
    let decoded = Artifact::decode_verified(&parent.encode(), &limits()).expect("valid");
    assert_eq!(decoded.children[0].slot, 7);
}

#[test]
fn admission_checks_canonical_bytes_verification_and_the_contract() {
    let bytes = valid_program().encode();
    assert!(crate::admit(&bytes, &limits(), &CONTRACT, &[]).is_ok());
    assert_eq!(
        crate::admit(&bytes, &limits(), &[0; 32], &[]),
        Err(crate::AdmitError::WrongContract)
    );
    let tight = Limits {
        max_artifact_bytes: bytes.len() - 1,
        ..limits()
    };
    assert!(matches!(
        crate::admit(&bytes, &tight, &CONTRACT, &[]),
        Err(crate::AdmitError::Invalid(
            DecodeError::LimitExceeded { .. }
        ))
    ));
    let mut unverifiable = artifact(vec![proto(vec![Instr::Return(1)])]).encode();
    assert!(matches!(
        crate::admit(&unverifiable, &limits(), &CONTRACT, &[]),
        Err(crate::AdmitError::Invalid(DecodeError::Verify(_)))
    ));
    unverifiable.push(0);
    assert!(matches!(
        crate::admit(&unverifiable, &limits(), &CONTRACT, &[]),
        Err(crate::AdmitError::Invalid(DecodeError::TrailingBytes))
    ));
}

/// A root that calls native 0 with one argument and returns its one result.
fn native_program(reference: NativeRef) -> Artifact {
    let mut program = artifact(vec![proto(vec![
        Instr::Constant(0),
        Instr::Native(0),
        Instr::Return(1),
    ])]);
    program.natives = vec![reference];
    program
}

fn declared() -> NativeRef {
    NativeRef {
        module: 3,
        operation: 9,
        argc: 1,
        results: 1,
    }
}

#[test]
fn a_native_reference_is_admitted_only_when_the_table_declares_it_exactly() {
    let bytes = native_program(declared()).encode();
    let table = [declared()];
    assert!(crate::admit(&bytes, &limits(), &CONTRACT, &table).is_ok());
    let refused = |table: &[NativeRef]| crate::admit(&bytes, &limits(), &CONTRACT, table);
    let expected = Err(crate::AdmitError::NotInContract { native: 0 });
    assert_eq!(refused(&[]), expected, "an empty table declares nothing");
    for changed in [
        NativeRef {
            module: 4,
            ..declared()
        },
        NativeRef {
            operation: 10,
            ..declared()
        },
        NativeRef {
            argc: 2,
            ..declared()
        },
        NativeRef {
            results: 0,
            ..declared()
        },
    ] {
        assert_eq!(
            refused(&[changed]),
            expected,
            "a table entry differing in one field"
        );
    }
}

#[test]
fn every_native_of_the_root_must_be_declared() {
    let mut program = native_program(declared());
    program.natives.push(NativeRef {
        module: 5,
        operation: 1,
        argc: 0,
        results: 0,
    });
    let bytes = program.encode();
    let table = [declared()];
    assert_eq!(
        crate::admit(&bytes, &limits(), &CONTRACT, &table),
        Err(crate::AdmitError::NotInContract { native: 1 }),
        "the first reference that is not declared is named"
    );
}

/// A chain in which each artifact spawns the next: the root is depth 0 and the innermost artifact is depth `levels`.
fn nested_chain(levels: usize) -> Artifact {
    let mut inner = valid_program();
    for _ in 0..levels {
        let bytes = inner.encode();
        let mut parent = artifact(vec![proto(vec![
            Instr::Spawn {
                child: 0,
                argc: 0,
                captures: 0,
            },
            Instr::Return(1),
        ])]);
        parent.children = vec![ChildLink {
            slot: 1,
            digest: digest(&bytes),
            entry: 0,
            bytes: bytes.into(),
        }];
        inner = parent;
    }
    inner
}

#[test]
fn nesting_is_admitted_up_to_the_depth_limit_and_refused_beyond_it() {
    let deep = Limits {
        max_artifact_bytes: 8 << 20,
        max_depth: u8::MAX,
        ..limits()
    };
    let at_limit = nested_chain(usize::from(u8::MAX)).encode();
    assert!(
        Artifact::decode_verified(&at_limit, &deep).is_ok(),
        "depth 255 is the limit"
    );
    let beyond = nested_chain(usize::from(u8::MAX) + 1);
    let bytes = beyond.encode();
    let decoded =
        Artifact::decode_verified(&bytes, &deep).expect_err("a child past 255 is refused");
    assert_eq!(
        root_cause(&decoded),
        &DecodeError::DepthExceeded,
        "a child one level past 255 is refused, and nothing wraps"
    );
    let in_memory = beyond
        .verify(&deep)
        .expect_err("the in-memory check refuses the same nesting");
    assert_eq!(
        root_cause(&DecodeError::Verify(in_memory)),
        &DecodeError::DepthExceeded
    );
    let shallow = Limits {
        max_depth: 2,
        ..deep
    };
    assert!(nested_chain(2).verify(&shallow).is_ok());
    assert!(
        nested_chain(3).verify(&shallow).is_err(),
        "a third level exceeds depth 2"
    );
}

#[test]
fn in_memory_artifacts_meet_the_same_bounds_as_decoded_ones() {
    let tight = Limits {
        max_constants: 2,
        max_string_bytes: 2,
        max_natives: 1,
        max_protos: 1,
        max_code: 2,
        max_upvalues: 1,
        max_entries: 1,
        max_children: 0,
        ..limits()
    };
    assert!(
        valid_program().verify(&tight).is_ok(),
        "the boundary values are allowed"
    );

    let mut constants = valid_program();
    constants.constants.push(Constant::Nil);
    assert_eq!(
        constants.verify(&tight),
        Err(VerifyError::LimitExceeded {
            what: "constants",
            limit: 2
        })
    );

    let mut string = valid_program();
    string.constants[1] = Constant::Bytes(b"abc".to_vec());
    assert_eq!(
        string.verify(&tight),
        Err(VerifyError::LimitExceeded {
            what: "string bytes",
            limit: 2
        })
    );

    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut float = valid_program();
        float.constants.push(Constant::Float(value));
        assert_eq!(
            float.verify(&limits()),
            Err(VerifyError::NonFiniteFloat { constant: 2 }),
            "a non-finite float cannot enter memory any more than bytes"
        );
    }

    let mut natives = native_program(declared());
    natives.natives.push(declared());
    assert_eq!(
        natives.verify(&tight),
        Err(VerifyError::LimitExceeded {
            what: "natives",
            limit: 1
        })
    );

    let mut protos = valid_program();
    protos.protos.push(proto(vec![Instr::Return(0)]));
    assert_eq!(
        protos.verify(&tight),
        Err(VerifyError::LimitExceeded {
            what: "protos",
            limit: 1
        })
    );

    let mut code = valid_program();
    code.protos[0] = proto(vec![
        Instr::Constant(0),
        Instr::Constant(0),
        Instr::Return(1),
    ]);
    assert_eq!(
        code.verify(&tight),
        Err(VerifyError::LimitExceeded {
            what: "instructions",
            limit: 2
        })
    );

    let mut entries = valid_program();
    entries.entries.push(Entry {
        proto: 0,
        captures: 0,
    });
    assert!(matches!(
        entries.verify(&tight),
        Err(VerifyError::LimitExceeded {
            what: "entries",
            ..
        } | VerifyError::DuplicateEntry { .. })
    ));

    let mut captures = valid_program();
    captures.entries[0].captures = 2;
    assert_eq!(
        captures.verify(&tight),
        Err(VerifyError::LimitExceeded {
            what: "captures",
            limit: 1
        })
    );

    let mut children = valid_program();
    children.children.push(ChildLink {
        slot: 0,
        digest: digest(&valid_program().encode()),
        entry: 0,
        bytes: valid_program().encode().into(),
    });
    assert_eq!(
        children.verify(&tight),
        Err(VerifyError::LimitExceeded {
            what: "children",
            limit: 0
        })
    );
}

#[test]
fn destination_limits_take_the_artifact_bound_and_one_level_less_than_the_spawn_depth() {
    let agent = Limits::for_destination(16_384, 2);
    assert_eq!((agent.max_artifact_bytes, agent.max_depth), (16_384, 1));
    assert_eq!(Limits::for_destination(8, 1).max_depth, 0);
    assert_eq!(Limits::for_destination(8, 0).max_depth, 0);
    assert_eq!(Limits::for_destination(8, 1_000).max_depth, u8::MAX);
    assert_eq!(agent.max_constants, Limits::profile().max_constants);
}

/// Mutated and random bytes never panic the decoder or the verifier, and decoding never accepts more than the limits.
#[test]
fn adversarial_bytes_never_panic_and_stay_within_the_limits() {
    let mut state = 0x853c_49e6_748f_ea9bu64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let tight = Limits {
        max_artifact_bytes: 512,
        max_constants: 8,
        max_protos: 4,
        max_code: 64,
        ..limits()
    };
    let seed = {
        let mut program = valid_program();
        program.protos.push(proto(vec![
            Instr::Constant(0),
            Instr::Constant(0),
            Instr::UnpackMulti,
            Instr::ReturnMulti(1),
        ]));
        program.encode()
    };
    for round in 0..20_000 {
        let mut bytes = seed.clone();
        let flips = 1 + next() % 4;
        for _ in 0..flips {
            let at = (next() as usize) % bytes.len();
            bytes[at] = next() as u8;
        }
        if round % 7 == 0 {
            bytes.truncate((next() as usize) % bytes.len());
        }
        if let Ok(artifact) = Artifact::decode(&bytes, &tight) {
            assert!(artifact.constants.len() <= tight.max_constants);
            assert!(artifact.protos.len() <= tight.max_protos);
            assert!(
                artifact
                    .protos
                    .iter()
                    .all(|p| p.code.len() <= tight.max_code)
            );
            let _ = artifact.verify(&tight);
        }
        let random: Vec<u8> = (0..(next() % 96)).map(|_| next() as u8).collect();
        let _ = Artifact::decode_verified(&random, &tight);
    }
}

// -- Traversal order, error paths and depth ---------------------------------------------------------------------------------

/// An artifact that fails verification: its code pops from an empty stack.
fn broken() -> Artifact {
    artifact(vec![proto(vec![Instr::Pop, Instr::Return(0)])])
}

fn with_children(mut parent: Artifact, children: Vec<(Artifact, u32)>) -> Artifact {
    parent.children = children
        .into_iter()
        .map(|(child, entry)| {
            let bytes = child.encode();
            ChildLink {
                slot: 1,
                digest: digest(&bytes),
                entry,
                bytes: bytes.into(),
            }
        })
        .collect();
    parent
}

/// Splits a located fault into the path to the faulty artifact and its verification error. The error is one flat record,
/// so there is nothing to unwrap beyond the path.
fn unwrap_children(error: VerifyError) -> (Vec<usize>, VerifyError) {
    match error {
        VerifyError::ChildInvalid { path, cause } => match *cause {
            DecodeError::Verify(inner) => (path, inner),
            other => panic!(
                "a located fault at {path:?} is a decode error, not a verification error: {other:?}"
            ),
        },
        other => (Vec::new(), other),
    }
}

#[test]
fn a_deep_invalid_chain_reports_the_whole_path_without_overflow() {
    let deep = Limits {
        max_artifact_bytes: 8 << 20,
        max_depth: u8::MAX,
        ..limits()
    };
    // The innermost artifact is broken and sits at the deepest permitted level.
    let mut inner = broken();
    for _ in 0..usize::from(u8::MAX) {
        inner = with_children(valid_program(), vec![(inner, 0)]);
    }
    let error = inner.verify(&deep).expect_err("the leaf is invalid");
    let (path, cause) = unwrap_children(error);
    assert_eq!(path.len(), usize::from(u8::MAX), "one layer for each level");
    assert!(path.iter().all(|&child| child == 0));
    assert!(
        matches!(cause, VerifyError::StackUnderflow { .. }),
        "{cause:?}"
    );

    // One more level is refused for its depth, and the leaf is never reached.
    let beyond = with_children(valid_program(), vec![(inner, 0)]);
    // The refused child sits one level past the limit, so its path names every level down to it.
    let error = beyond.verify(&deep).expect_err("too deep");
    match error {
        VerifyError::ChildInvalid { path, cause } => {
            assert_eq!(
                path.len(),
                usize::from(u8::MAX) + 1,
                "one entry for each level, the refused one included"
            );
            assert!(path.iter().all(|&child| child == 0));
            assert_eq!(*cause, DecodeError::DepthExceeded);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_first_failure_in_tree_order_is_reported() {
    // Child 0 is invalid inside its own subtree, and child 1 has a wrong digest. The digests of a level are checked before
    // any child is verified, so the later sibling's digest is the failure reported, not the subtree of child 0.
    let mut parent = with_children(valid_program(), vec![(broken(), 0), (valid_program(), 0)]);
    parent.children[1].digest = [0xee; DIGEST_LEN];
    assert_eq!(
        parent.verify(&limits()),
        Err(VerifyError::ChildDigestWrong { child: 1 }),
        "a sibling's digest fault is reported before any subtree"
    );

    // With the digests intact, the subtree of child 0 is checked first, so its failure is the one reported.
    let parent = with_children(valid_program(), vec![(broken(), 0), (valid_program(), 0)]);
    let (path, cause) = unwrap_children(parent.verify(&limits()).expect_err("invalid"));
    assert_eq!(path, vec![0]);
    assert!(
        matches!(cause, VerifyError::StackUnderflow { .. }),
        "{cause:?}"
    );

    // With child 0 valid, the wrong digest of child 1 is found, at the parent.
    let mut parent = with_children(
        valid_program(),
        vec![(valid_program(), 0), (valid_program(), 0)],
    );
    parent.children[1].digest = [0xee; DIGEST_LEN];
    assert_eq!(
        parent.verify(&limits()),
        Err(VerifyError::ChildDigestWrong { child: 1 })
    );
}

#[test]
fn a_child_entry_is_checked_after_the_subtree_it_names() {
    // Child 0 names an entry it does not have and also holds an invalid grandchild. The subtree is checked first, as it
    // is when the child is verified recursively, so the grandchild's failure is reported.
    let invalid_below = with_children(valid_program(), vec![(broken(), 0)]);
    let parent = with_children(valid_program(), vec![(invalid_below, 9)]);
    let (path, cause) = unwrap_children(parent.verify(&limits()).expect_err("invalid"));
    assert_eq!(path, vec![0, 0]);
    assert!(
        matches!(cause, VerifyError::StackUnderflow { .. }),
        "{cause:?}"
    );

    // With a valid subtree, the entry is what fails, and it is reported at the parent.
    let parent = with_children(valid_program(), vec![(valid_program(), 9)]);
    assert_eq!(
        parent.verify(&limits()),
        Err(VerifyError::ChildEntryOutOfRange { child: 0 })
    );
}

#[test]
fn a_child_with_trailing_bytes_is_refused_at_its_parent_with_its_index() {
    // The digest matches the changed bytes, so the digest check passes and decoding is what refuses the child.
    let mut parent = with_children(
        valid_program(),
        vec![(valid_program(), 0), (valid_program(), 0)],
    );
    let mut bytes = parent.children[1].bytes.to_vec();
    bytes.push(0);
    parent.children[1].digest = digest(&bytes);
    parent.children[1].bytes = bytes.into();
    assert_eq!(
        parent.verify(&limits()),
        Err(VerifyError::ChildInvalid {
            path: vec![1],
            cause: alloc::boxed::Box::new(DecodeError::TrailingBytes),
        })
    );
}

#[test]
fn canonical_comparison_matches_an_explicit_encode() {
    let artifact = nested_chain(5);
    let bytes = artifact.encode();
    assert!(
        Artifact::decode_canonical(
            &bytes,
            &Limits {
                max_depth: 8,
                max_artifact_bytes: 1 << 20,
                ..limits()
            }
        )
        .is_ok()
    );
    // A single changed byte, or a truncated or extended input, is not canonical or does not decode.
    for index in [0, 7, bytes.len() / 2, bytes.len() - 1] {
        let mut changed = bytes.clone();
        changed[index] ^= 1;
        let wide = Limits {
            max_depth: 8,
            max_artifact_bytes: 1 << 20,
            ..limits()
        };
        assert!(
            Artifact::decode_canonical(&changed, &wide).is_err(),
            "byte {index}"
        );
    }
}
