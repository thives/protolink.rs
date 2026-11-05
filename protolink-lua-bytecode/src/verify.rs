//! Verification of a decoded artifact before any of its code runs.
//!
//! The checks are structural (indices, layouts, jump targets, closure order, the multi-result pattern) and
//! stack-based (every path has one consistent height, no underflow, no overflow of the declared bound). Children are
//! decoded and verified under the same limits, one level at a time from a work list, and must match their recorded
//! digest.

use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;

use crate::decode;
use crate::digest;
use crate::encode;
use crate::error::{DecodeError, VerifyError};
use crate::format::{
    Artifact, Constant, Instr, Limits, MAX_NATIVE_ARGS, MAX_NATIVE_RESULTS, Proto, RESULTS_MULTI,
    UpvalueSource,
};

/// Checks `artifact` and every nested child against `limits` and returns the first violation. `depth` is the artifact's
/// nesting level, at most `limits.max_depth`. The limits are the ones decoding enforces, so an artifact built in memory is
/// held to the same bounds as one read from bytes.
///
/// # Order of checks
///
/// Faults are reported in one fixed order, which is the order decoding uses for a level read from bytes:
///
/// 1. the digest of every child of the artifact, in index order, before any child is verified;
/// 2. then each child in index order: its own structure and canonical form, its own level, its children in the same order,
///    and finally the entry index the parent names in it.
///
/// So a digest fault in a later sibling is reported before a fault inside an earlier sibling's subtree. The order is not
/// strict tree order, and the tests pin the order above.
///
/// # Resource use
///
/// The traversal is iterative. It keeps its own work list, so the Rust stack does not grow with the nesting depth. A child
/// is read in place: its bytes are a slice of its parent's bytes, and it is decoded *without* copying its own children.
/// Canonical form is checked by comparing the encoder's output with the bytes as it is produced, so no second buffer is
/// built. Heap use is the largest decoded level, plus one `Node` for each artifact visited so far (the arena that locates a
/// fault), plus the work list of pending siblings. Every one of these is bounded by the artifact size limit, because each
/// child occupies at least its own bytes.
pub(crate) fn verify(artifact: &Artifact, limits: &Limits, depth: u8) -> Result<(), VerifyError> {
    // Decoding checks a level's child digests before it verifies any child. An in-memory artifact is checked the same way.
    for (child, link) in artifact.children.iter().enumerate() {
        if digest(&link.bytes) != link.digest {
            return Err(VerifyError::ChildDigestWrong { child });
        }
    }
    verify_level(artifact, limits)?;
    let mut nodes = vec![Node {
        parent: None,
        child: 0,
    }];
    let mut work: Vec<Work<'_>> = Vec::new();
    push_children(
        &mut work,
        0,
        depth,
        artifact
            .children
            .iter()
            .map(|link| (&link.bytes[..], link.entry)),
    );
    while let Some(item) = work.pop() {
        match item {
            Work::Child {
                parent,
                parent_depth,
                child,
                bytes,
                entry,
            } => {
                // The depth test happens before the child is decoded. `checked_add` keeps the arithmetic exact at
                // `u8::MAX`, so a nesting at the limit is refused without wrapping to depth zero.
                let Some(depth) = parent_depth
                    .checked_add(1)
                    .filter(|&nested| nested <= limits.max_depth)
                else {
                    return Err(decode_fault(
                        &nodes,
                        parent,
                        child,
                        DecodeError::DepthExceeded,
                    ));
                };
                let (shape, spans) = decode::decode_shallow(bytes, limits, depth)
                    .map_err(|cause| decode_fault(&nodes, parent, child, cause))?;
                if !encode::encodes_to(&shape, &|k| &bytes[spans[k].clone()], bytes) {
                    return Err(fault_at(
                        &nodes,
                        parent,
                        VerifyError::ChildNotCanonical { child },
                    ));
                }
                nodes.push(Node {
                    parent: Some(parent),
                    child,
                });
                let node = nodes.len() - 1;
                verify_level(&shape, limits).map_err(|cause| fault_at(&nodes, node, cause))?;
                // The entry check follows the child's whole subtree, as it does when the subtree is checked first.
                work.push(Work::Entry {
                    parent,
                    child,
                    entry,
                    entries: shape.entries.len(),
                });
                push_children(
                    &mut work,
                    node,
                    depth,
                    shape
                        .children
                        .iter()
                        .zip(&spans)
                        .map(|(link, span)| (&bytes[span.clone()], link.entry)),
                );
            }
            Work::Entry {
                parent,
                child,
                entry,
                entries,
            } => {
                if entry as usize >= entries {
                    return Err(fault_at(
                        &nodes,
                        parent,
                        VerifyError::ChildEntryOutOfRange { child },
                    ));
                }
            }
        }
    }
    Ok(())
}

/// One artifact in the tree being checked: which child of which parent it is. The root is node 0.
struct Node {
    parent: Option<usize>,
    child: usize,
}

/// A step of the traversal.
enum Work<'a> {
    /// Check child `child` of node `parent`, whose bytes are `bytes`. Its digest was checked when `parent` was read.
    Child {
        parent: usize,
        parent_depth: u8,
        child: usize,
        bytes: &'a [u8],
        entry: u32,
    },
    /// After the subtree of child `child` of node `parent`: its entry index must exist in the child.
    Entry {
        parent: usize,
        child: usize,
        entry: u32,
        entries: usize,
    },
}

/// Queues the children of `parent` so that the first child is checked first.
fn push_children<'a>(
    work: &mut Vec<Work<'a>>,
    parent: usize,
    parent_depth: u8,
    children: impl Iterator<Item = (&'a [u8], u32)>,
) {
    // Collected in order, pushed in reverse, so the work list pops the first child first.
    let queued: Vec<_> = children.collect();
    for (child, (bytes, entry)) in queued.into_iter().enumerate().rev() {
        work.push(Work::Child {
            parent,
            parent_depth,
            child,
            bytes,
            entry,
        });
    }
}

/// The child indices from the root down to `node`, outermost first. Empty for the root.
fn path_to(nodes: &[Node], mut node: usize) -> Vec<usize> {
    let mut path = Vec::new();
    while let Some(parent) = nodes[node].parent {
        path.push(nodes[node].child);
        node = parent;
    }
    path.reverse();
    path
}

/// A fault found by the level of `node`. The root's own faults are returned as they are. A descendant's fault is located
/// by its path, so the error is one flat record: its size grows with the depth, but nothing nests.
fn fault_at(nodes: &[Node], node: usize, fault: VerifyError) -> VerifyError {
    if nodes[node].parent.is_none() {
        return fault;
    }
    VerifyError::ChildInvalid {
        path: path_to(nodes, node),
        cause: Box::new(DecodeError::Verify(fault)),
    }
}

/// A child of `parent` that could not be read or whose depth is refused, so its own level was never checked.
fn decode_fault(nodes: &[Node], parent: usize, child: usize, cause: DecodeError) -> VerifyError {
    let mut path = path_to(nodes, parent);
    path.push(child);
    VerifyError::ChildInvalid {
        path,
        cause: Box::new(cause),
    }
}

/// Everything about one artifact except its children: entries, limits, protos and natives.
fn verify_level(artifact: &Artifact, limits: &Limits) -> Result<(), VerifyError> {
    if artifact.entries.is_empty() {
        return Err(VerifyError::NoEntry);
    }
    check_limits(artifact, limits)?;
    let proto_count = artifact.protos.len();
    let mut closure_target = vec![false; proto_count];
    for (index, proto) in artifact.protos.iter().enumerate() {
        for (at, instr) in proto.code.iter().enumerate() {
            if let Instr::Closure(target) = *instr {
                let target = target as usize;
                if target <= index || target >= proto_count {
                    return Err(VerifyError::ClosureTargetOrder { proto: index, at });
                }
                closure_target[target] = true;
            }
        }
    }

    // Upvalues of an entry come from its captures. Upvalues of any other proto come from its sources.
    let mut upvalue_counts: Vec<usize> = artifact.protos.iter().map(|p| p.upvalues.len()).collect();
    let mut entry_proto = vec![false; proto_count];
    for (entry, record) in artifact.entries.iter().enumerate() {
        let proto = record.proto as usize;
        if proto >= proto_count {
            return Err(VerifyError::EntryProtoOutOfRange { entry });
        }
        if closure_target[proto] {
            return Err(VerifyError::EntryIsClosureTarget { proto });
        }
        if entry_proto[proto] {
            return Err(VerifyError::DuplicateEntry { proto });
        }
        entry_proto[proto] = true;
        upvalue_counts[proto] = usize::from(record.captures);
    }

    for (index, proto) in artifact.protos.iter().enumerate() {
        verify_proto(artifact, index, proto, &upvalue_counts, limits)?;
    }

    for (native, reference) in artifact.natives.iter().enumerate() {
        if reference.argc > MAX_NATIVE_ARGS {
            return Err(VerifyError::NativeArityTooLarge { native });
        }
        if reference.results > MAX_NATIVE_RESULTS {
            return Err(VerifyError::NativeResultsUnsupported { native });
        }
    }
    Ok(())
}

/// Count, string and constant bounds, the ones decoding checks while it reads. Repeated here so that in-memory artifacts,
/// such as the host compiler's output and `Program::verified`'s input, meet the same bounds without an encode pass.
fn check_limits(artifact: &Artifact, limits: &Limits) -> Result<(), VerifyError> {
    within("constants", artifact.constants.len(), limits.max_constants)?;
    within("natives", artifact.natives.len(), limits.max_natives)?;
    within("protos", artifact.protos.len(), limits.max_protos)?;
    within("entries", artifact.entries.len(), limits.max_entries)?;
    within("children", artifact.children.len(), limits.max_children)?;
    for (index, constant) in artifact.constants.iter().enumerate() {
        match constant {
            Constant::Float(value) if !value.is_finite() => {
                return Err(VerifyError::NonFiniteFloat { constant: index });
            }
            Constant::Bytes(bytes) => within("string bytes", bytes.len(), limits.max_string_bytes)?,
            _ => {}
        }
    }
    for proto in &artifact.protos {
        within("upvalues", proto.upvalues.len(), limits.max_upvalues)?;
        within("instructions", proto.code.len(), limits.max_code)?;
        within("lines", proto.lines.len(), limits.max_code)?;
    }
    for entry in &artifact.entries {
        within("captures", usize::from(entry.captures), limits.max_upvalues)?;
    }
    for link in &artifact.children {
        within("child bytes", link.bytes.len(), limits.max_artifact_bytes)?;
    }
    Ok(())
}

fn within(what: &'static str, count: usize, limit: usize) -> Result<(), VerifyError> {
    if count > limit {
        Err(VerifyError::LimitExceeded { what, limit })
    } else {
        Ok(())
    }
}

fn verify_proto(
    artifact: &Artifact,
    index: usize,
    proto: &Proto,
    upvalue_counts: &[usize],
    limits: &Limits,
) -> Result<(), VerifyError> {
    if u16::from(proto.params) > proto.locals || proto.locals > limits.max_locals {
        return Err(VerifyError::BadLocalLayout { proto: index });
    }
    if proto.max_stack > limits.max_stack {
        return Err(VerifyError::StackBoundTooLarge { proto: index });
    }
    if proto.code.is_empty() {
        return Err(VerifyError::EmptyCode { proto: index });
    }
    if proto.lines.len() != proto.code.len() {
        return Err(VerifyError::LineTableMismatch { proto: index });
    }

    let code = &proto.code;
    let upvalues = upvalue_counts[index];
    let mut targets = vec![false; code.len()];
    for (at, instr) in code.iter().enumerate() {
        match *instr {
            Instr::Constant(i) if i as usize >= artifact.constants.len() => {
                return Err(VerifyError::ConstantOutOfRange { proto: index, at });
            }
            Instr::GetLocal(slot)
            | Instr::SetLocal(slot)
            | Instr::GetCell(slot)
            | Instr::SetCell(slot)
            | Instr::NewCell(slot)
                if slot >= proto.locals =>
            {
                return Err(VerifyError::LocalOutOfRange { proto: index, at });
            }
            Instr::ForPrep { slot, exit: target } | Instr::ForLoop { slot, body: target } => {
                if u32::from(slot) + 2 >= u32::from(proto.locals) {
                    return Err(VerifyError::LocalOutOfRange { proto: index, at });
                }
                if target as usize >= code.len() {
                    return Err(VerifyError::JumpOutOfRange { proto: index, at });
                }
                targets[target as usize] = true;
            }
            Instr::GetUpvalue(i) | Instr::SetUpvalue(i) if i as usize >= upvalues => {
                return Err(VerifyError::UpvalueOutOfRange { proto: index, at });
            }
            Instr::Closure(target) => {
                // The target was range-checked against the artifact before this pass.
                let Some(child) = artifact.protos.get(target as usize) else {
                    return Err(VerifyError::ClosureTargetOrder { proto: index, at });
                };
                for source in &child.upvalues {
                    let valid = match *source {
                        UpvalueSource::Local(slot) => slot < proto.locals,
                        UpvalueSource::Upvalue(i) => (i as usize) < upvalues,
                    };
                    if !valid {
                        return Err(VerifyError::ClosureSourceOutOfRange { proto: index });
                    }
                }
            }
            Instr::Jump(target)
            | Instr::JumpIfFalse(target)
            | Instr::JumpIfFalseKeep(target)
            | Instr::JumpIfTrueKeep(target) => {
                if target as usize >= code.len() {
                    return Err(VerifyError::JumpOutOfRange { proto: index, at });
                }
                targets[target as usize] = true;
            }
            Instr::Native(i) if i as usize >= artifact.natives.len() => {
                return Err(VerifyError::NativeOutOfRange { proto: index, at });
            }
            Instr::Spawn { child, .. } | Instr::SpawnMulti { child, .. }
                if child as usize >= artifact.children.len() =>
            {
                return Err(VerifyError::ChildOutOfRange { proto: index, at });
            }
            _ => {}
        }
    }

    // A marker producer is followed at once by a consumer, and a consumer is reached only from its producer, so a count
    // marker never meets any other instruction.
    for (at, instr) in code.iter().enumerate() {
        if produces_marker(*instr) && !code.get(at + 1).is_some_and(|next| consumes_marker(*next)) {
            return Err(VerifyError::MultiResultPattern { proto: index, at });
        }
        if consumes_marker(*instr) {
            let produced = at > 0 && produces_marker(code[at - 1]);
            if !produced || targets[at] {
                return Err(VerifyError::MultiResultPattern { proto: index, at });
            }
        }
    }

    check_stack(artifact, index, proto)
}

/// The stack effect of one instruction on its fall-through edge and, for jumps, on its jump edge.
struct Effect {
    falls: bool,
    pops: u32,
    pushes: u32,
    jump: Option<(usize, u32, u32)>,
}

fn fixed(pops: u32, pushes: u32) -> Effect {
    Effect {
        falls: true,
        pops,
        pushes,
        jump: None,
    }
}

fn terminal(pops: u32) -> Effect {
    Effect {
        falls: false,
        pops,
        pushes: 0,
        jump: None,
    }
}

/// True when `instr` leaves a variable number of values and a count marker.
fn produces_marker(instr: Instr) -> bool {
    matches!(
        instr,
        Instr::Call {
            results: RESULTS_MULTI,
            ..
        } | Instr::CallMulti {
            results: RESULTS_MULTI,
            ..
        } | Instr::UnpackMulti
    )
}

/// True when `instr` takes a count marker and the values it announces.
fn consumes_marker(instr: Instr) -> bool {
    matches!(
        instr,
        Instr::ReturnMulti(_)
            | Instr::CallMulti { .. }
            | Instr::PcallMulti(_)
            | Instr::PcallList
            | Instr::PackMulti(_)
            | Instr::YieldMulti(_)
            | Instr::SpawnMulti { .. }
    )
}

/// Values a call keeps on the static stack: its result count, or one marker.
fn kept(results: u8) -> u32 {
    if results == RESULTS_MULTI {
        1
    } else {
        u32::from(results)
    }
}

fn effect(artifact: &Artifact, instr: Instr) -> Effect {
    match instr {
        Instr::Nil
        | Instr::True
        | Instr::False
        | Instr::Constant(_)
        | Instr::GetLocal(_)
        | Instr::GetCell(_)
        | Instr::GetUpvalue(_)
        | Instr::Closure(_) => fixed(0, 1),
        Instr::Pop
        | Instr::SetLocal(_)
        | Instr::SetCell(_)
        | Instr::SetUpvalue(_)
        | Instr::NewCell(_) => fixed(1, 0),
        Instr::Add
        | Instr::Sub
        | Instr::Mul
        | Instr::Div
        | Instr::IDiv
        | Instr::Mod
        | Instr::Concat
        | Instr::Eq
        | Instr::Ne
        | Instr::Lt
        | Instr::Le
        | Instr::Gt
        | Instr::Ge => fixed(2, 1),
        Instr::Neg | Instr::Not | Instr::Len => fixed(1, 1),
        Instr::Jump(target) => Effect {
            falls: false,
            pops: 0,
            pushes: 0,
            jump: Some((target as usize, 0, 0)),
        },
        Instr::JumpIfFalse(target) => Effect {
            falls: true,
            pops: 1,
            pushes: 0,
            jump: Some((target as usize, 1, 0)),
        },
        Instr::JumpIfFalseKeep(target) | Instr::JumpIfTrueKeep(target) => Effect {
            falls: true,
            pops: 1,
            pushes: 0,
            jump: Some((target as usize, 0, 0)),
        },
        Instr::Call { argc, results } => fixed(u32::from(argc) + 1, kept(results)),
        Instr::CallMulti {
            fixed: count,
            results,
        } => fixed(u32::from(count) + 2, kept(results)),
        Instr::Return(count) => terminal(u32::from(count)),
        Instr::ReturnMulti(count) => terminal(u32::from(count) + 1),
        Instr::ReturnTuple | Instr::Raise => terminal(1),
        Instr::Spawn { argc, captures, .. } => fixed(u32::from(argc) + u32::from(captures), 1),
        Instr::SpawnMulti {
            fixed: count,
            captures,
            ..
        } => fixed(u32::from(count) + 1 + u32::from(captures), 1),
        Instr::Await => fixed(1, 1),
        Instr::Unpack(count) => fixed(1, u32::from(count)),
        Instr::UnpackMulti => fixed(1, 1),
        Instr::PackMulti(count) => fixed(u32::from(count) + 1, 1),
        Instr::PcallList => fixed(1, 1),
        Instr::Native(index) => {
            let (argc, results) = artifact
                .natives
                .get(index as usize)
                .map_or((0, 0), |n| (u32::from(n.argc), u32::from(n.results)));
            fixed(argc, results)
        }
        Instr::Yield(argc) => fixed(u32::from(argc), 1),
        Instr::YieldMulti(count) => fixed(u32::from(count) + 1, 1),
        Instr::Pcall(argc) => fixed(u32::from(argc) + 1, 1),
        Instr::PcallMulti(count) => fixed(u32::from(count) + 2, 1),
        Instr::ForPrep { exit: target, .. } | Instr::ForLoop { body: target, .. } => Effect {
            falls: true,
            pops: 0,
            pushes: 0,
            jump: Some((target as usize, 0, 0)),
        },
    }
}

fn check_stack(artifact: &Artifact, index: usize, proto: &Proto) -> Result<(), VerifyError> {
    let code = &proto.code;
    let max = u32::from(proto.max_stack);
    let mut heights: Vec<Option<u32>> = vec![None; code.len()];
    let mut work = vec![0usize];
    heights[0] = Some(0);

    let apply = |at: usize, height: u32, pops: u32, pushes: u32| -> Result<u32, VerifyError> {
        if pops > height {
            return Err(VerifyError::StackUnderflow { proto: index, at });
        }
        let next = height - pops + pushes;
        if next > max {
            return Err(VerifyError::StackOverflow { proto: index, at });
        }
        Ok(next)
    };

    while let Some(at) = work.pop() {
        let height = heights[at].unwrap_or(0);
        let step = effect(artifact, code[at]);
        if step.falls {
            if at + 1 >= code.len() {
                return Err(VerifyError::FallsOffEnd { proto: index });
            }
            let after = apply(at, height, step.pops, step.pushes)?;
            propagate(&mut heights, &mut work, at + 1, after, index)?;
        } else if step.jump.is_none() {
            // A terminal instruction: its operands must be available and nothing follows it.
            apply(at, height, step.pops, 0)?;
        }
        if let Some((target, pops, pushes)) = step.jump {
            let after = apply(at, height, pops, pushes)?;
            propagate(&mut heights, &mut work, target, after, index)?;
        }
    }
    Ok(())
}

fn propagate(
    heights: &mut [Option<u32>],
    work: &mut Vec<usize>,
    target: usize,
    height: u32,
    proto: usize,
) -> Result<(), VerifyError> {
    match heights[target] {
        Some(existing) if existing != height => {
            Err(VerifyError::InconsistentStack { proto, at: target })
        }
        Some(_) => Ok(()),
        None => {
            heights[target] = Some(height);
            work.push(target);
            Ok(())
        }
    }
}
