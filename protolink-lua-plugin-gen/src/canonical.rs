//! Canonical binary encoding of a contract. Equal semantics give equal bytes, whatever the declaration order, comments
//! or file paths. Lists are sorted by stable identifier, except parameter lists, whose order is part of the signature.

use sha2::{Digest, Sha256};

use crate::model::{Contract, Execution, Module, Type};

/// Version of the canonical encoding. It is part of every digest. Version 5 adds the profile's version and semantic
/// fingerprint, and the retirement, child, deadline and request bounds of the resource envelope.
pub const CANONICAL_VERSION: u16 = 5;

/// Domain separator of contract digests.
pub const DIGEST_DOMAIN: &[u8] = b"protolink-lua-plugin-gen/contract";

/// The contract digest: SHA-256 over the domain, the encoding version and the canonical bytes.
pub(crate) fn digest(contract: &Contract) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(DIGEST_DOMAIN);
    hasher.update(CANONICAL_VERSION.to_le_bytes());
    hasher.update(encode(contract));
    hasher.finalize().into()
}

/// The canonical bytes of a contract. The contract's own digest field is not part of them.
pub(crate) fn encode(contract: &Contract) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, &contract.name);
    put_str(&mut out, &contract.id);
    put_str(&mut out, &contract.version);
    put_str(&mut out, &contract.language_profile);
    put_str(&mut out, &contract.profile_version);
    out.extend_from_slice(&contract.profile_fingerprint);
    put_str(&mut out, &contract.remote_data_profile);
    put_strs(&mut out, &sorted(&contract.privileges));
    put_u32(&mut out, contract.limits.max_string_bytes);
    put_u32(&mut out, contract.limits.max_tuple_items);
    put_u32(&mut out, contract.limits.max_capture_bytes);
    put_u32(&mut out, contract.limits.max_result_bytes);
    put_u32(&mut out, contract.limits.max_spawn_depth);
    match &contract.resources {
        None => out.push(0),
        Some(r) => {
            out.push(1);
            put_u32(&mut out, r.max_artifact_bytes);
            put_u32(&mut out, r.max_chunk_bytes);
            put_u32(&mut out, r.max_uploads);
            put_u32(&mut out, r.max_sessions);
            put_u32(&mut out, r.max_executions);
            put_u32(&mut out, r.max_answer_bytes);
            put_u32(&mut out, r.slice_fuel);
            out.extend_from_slice(&r.total_fuel.to_le_bytes());
            put_u32(&mut out, r.max_frames);
            put_u32(&mut out, r.max_objects);
            put_u32(&mut out, r.max_heap_bytes);
            put_u32(&mut out, r.session_idle_ms);
            put_u32(&mut out, r.max_artifacts);
            put_u32(&mut out, r.retired_window);
            put_u32(&mut out, r.max_child_tasks);
            put_u32(&mut out, r.call_timeout_ms);
            put_u32(&mut out, r.cleanup_ms);
            put_u32(&mut out, r.max_request_bytes);
        }
    }

    let mut modules: Vec<&Module> = contract.modules.iter().collect();
    modules.sort_by_key(|m| m.id);
    put_u32(&mut out, modules.len() as u32);
    for module in modules {
        put_str(&mut out, &module.name);
        put_u32(&mut out, module.id);
        let mut errors = module.errors.clone();
        errors.sort_by_key(|(_, id)| *id);
        put_u32(&mut out, errors.len() as u32);
        for (name, id) in &errors {
            put_str(&mut out, name);
            put_u32(&mut out, *id);
        }
        let mut enums = module.enums.clone();
        enums.sort_by_key(|e| e.id);
        put_u32(&mut out, enums.len() as u32);
        for enumeration in &enums {
            put_str(&mut out, &enumeration.name);
            put_u32(&mut out, enumeration.id);
            let mut variants = enumeration.variants.clone();
            variants.sort_by_key(|v| v.id);
            put_u32(&mut out, variants.len() as u32);
            for variant in &variants {
                put_str(&mut out, &variant.name);
                put_u32(&mut out, variant.id);
                put_str(&mut out, &variant.symbol);
            }
        }
        let mut operations = module.operations.clone();
        operations.sort_by_key(|o| o.id);
        put_u32(&mut out, operations.len() as u32);
        for operation in &operations {
            put_str(&mut out, &operation.name);
            put_u32(&mut out, operation.id);
            out.push(match operation.execution {
                Execution::Sync => 0,
                Execution::Async => 1,
            });
            put_params(&mut out, &operation.params);
            put_params(&mut out, &operation.results);
            put_strs(&mut out, &sorted(&operation.errors));
            put_strs(&mut out, &sorted(&operation.requires));
        }
    }

    let mut children = contract.children.clone();
    children.sort_by_key(|c| c.id);
    put_u32(&mut out, children.len() as u32);
    for child in &children {
        put_str(&mut out, &child.name);
        put_u32(&mut out, child.id);
        out.push(child.index);
        put_str(&mut out, &child.child);
        out.extend_from_slice(&child.child_digest);
        put_str(&mut out, &child.intrinsic);
        put_strs(&mut out, &sorted(&child.requires));
        put_strs(&mut out, &sorted(&child.delegates));
        put_u32(&mut out, child.max_capture_bytes);
        put_u32(&mut out, child.max_result_bytes);
    }
    out
}

fn sorted(values: &[String]) -> Vec<String> {
    let mut copy = values.to_vec();
    copy.sort();
    copy
}

fn put_params(out: &mut Vec<u8>, params: &[crate::model::Param]) {
    put_u32(out, params.len() as u32);
    for param in params {
        put_str(out, &param.name);
        put_type(out, &param.ty);
    }
}

fn put_type(out: &mut Vec<u8>, ty: &Type) {
    match ty {
        Type::Nil => out.push(0),
        Type::Boolean => out.push(1),
        Type::Integer => out.push(2),
        Type::Float => out.push(3),
        Type::Bytes { max_bytes } => {
            out.push(4);
            put_u32(out, *max_bytes);
        }
        Type::Enum { reference } => {
            out.push(5);
            put_str(out, reference);
        }
    }
}

fn put_str(out: &mut Vec<u8>, value: &str) {
    put_u32(out, value.len() as u32);
    out.extend_from_slice(value.as_bytes());
}

fn put_strs(out: &mut Vec<u8>, values: &[String]) {
    put_u32(out, values.len() as u32);
    for value in values {
        put_str(out, value);
    }
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}
