//! Host-side compiler for remote Lua regions.
//!
//! [`compile`] takes one host workflow, the resolved contract graph and the name of the host contract. It checks every
//! remote region against its destination's language profile, capabilities and resource envelope, and compiles each
//! spawn body and all of its nested bodies into artifact bytes that pass the destination's own admission check. It rewrites each placement so that the live closure the host creates
//! is registered with its artifact and capture accessor. Host code outside a placement is left as written, and line
//! numbers are preserved.
//!
//! Nothing here runs Lua. The compiler never runs on a remote node.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod codegen;
mod diag;
mod scan;

use std::collections::BTreeSet;

use protolink_lua_plugin_gen::Graph;
use sha2::{Digest, Sha256};

pub use diag::{Diagnostic, code};

/// The host function that registers a placed closure. The central host must define it before running the workflow.
pub const REGISTER_FUNCTION: &str = "__lua_remote_register";

/// Where a workflow's artifacts came from. It is kept beside the artifacts for audit. It is not part of any digest and is
/// never sent to a remote node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provenance {
    /// SHA-256 of the host source the workflow was compiled from.
    pub source_sha256: [u8; 32],
    /// Compiler name and version.
    pub compiler: String,
    /// Id of the host contract the workflow was checked against.
    pub host_contract: String,
}

/// A compiled placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Site {
    /// Identifier passed to the registration function, equal to the site's index.
    pub id: u32,
    /// Name of the host's child slot the site was placed on.
    pub slot: String,
    /// Stable id of that child slot.
    pub slot_id: u32,
    /// Id of the destination contract that runs the body.
    pub contract: String,
    /// The encoded artifact of the body. Entry 0 is the body.
    pub artifact: Vec<u8>,
    /// Digest of `artifact`, which is the identity a node stores and starts it by.
    pub digest: [u8; 32],
    /// Captured host locals, in the order the accessor returns them.
    pub captures: Vec<String>,
}

/// A checked workflow: rewritten host source, its compiled placements and where they came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Workflow {
    /// Host source with each placement wrapped in a registration call. Line numbers match the original.
    pub host_source: String,
    /// The compiled placements, indexed by id.
    pub sites: Vec<Site>,
    /// Audit record of the compilation.
    pub provenance: Provenance,
}

/// Checks and compiles `source`. `host` names the host contract in `graph`. Each artifact is bounded by the resource
/// envelope and limits of the contract it runs on.
pub fn compile(source: &str, graph: &Graph, host: &str) -> Result<Workflow, Vec<Diagnostic>> {
    let ast = full_moon::parse(source).map_err(|errors| {
        errors
            .iter()
            .map(|error| {
                let (position, _) = error.range();
                Diagnostic::new(
                    code::PARSE,
                    position.line(),
                    position.character(),
                    error.error_message().to_string(),
                )
            })
            .collect::<Vec<_>>()
    })?;
    let Some(host_rules) = graph.checker_rules(host) else {
        return Err(vec![Diagnostic::new(
            code::INTERNAL,
            1,
            1,
            format!("host contract '{host}' is not in the graph"),
        )]);
    };
    let slots: BTreeSet<String> = host_rules.children.keys().cloned().collect();
    let cells = scan::cells(&ast);
    let mut compiler = codegen::Compiler::new(graph, cells);
    let mut sites = Vec::new();
    let mut edits = Vec::new();
    let mut diagnostics = scan::host_sites(&ast, &slots, |site| {
        let child = &host_rules.children[&site.slot];
        let destination = child.child.clone();
        let (artifact, captures) = compiler.site(site.visible, &destination, site.function)?;
        let id = sites.len() as u32;
        let digest = protolink_lua_bytecode::digest(&artifact);
        sites.push(Site {
            id,
            slot: site.slot.clone(),
            slot_id: child.slot_id,
            contract: destination,
            artifact,
            digest,
            captures: captures.clone(),
        });
        edits.push(Edit {
            start: site.start,
            end: site.end,
            id,
            captures,
        });
        Ok(())
    });
    diagnostics.sort_by_key(|d| (d.line, d.column));
    if !diagnostics.is_empty() {
        return Err(diagnostics);
    }
    let mut source_sha256 = [0u8; 32];
    source_sha256.copy_from_slice(&Sha256::digest(source.as_bytes()));
    Ok(Workflow {
        host_source: rewrite(source, &edits),
        sites,
        provenance: Provenance {
            source_sha256,
            compiler: format!("lua_compiler {}+w6-abi3", env!("CARGO_PKG_VERSION")),
            host_contract: host.to_string(),
        },
    })
}

/// One placement's source edit.
struct Edit {
    start: usize,
    end: usize,
    id: u32,
    captures: Vec<String>,
}

/// Wraps each placed literal in a registration call. Insertions add no newlines, so line numbers do not move.
fn rewrite(source: &str, edits: &[Edit]) -> String {
    let mut inserts: Vec<(usize, String)> = Vec::new();
    for edit in edits {
        inserts.push((edit.start, format!("{REGISTER_FUNCTION}({}, ", edit.id)));
        let returned = edit.captures.join(", ");
        let accessor = if returned.is_empty() {
            "function() end)".to_string()
        } else {
            format!("function() return {returned} end)")
        };
        inserts.push((edit.end, format!(", {accessor}")));
    }
    inserts.sort_by_key(|(position, _)| *position);
    let mut out = String::with_capacity(source.len() + inserts.len() * 64);
    let mut cursor = 0;
    for (position, text) in &inserts {
        out.push_str(&source[cursor..*position]);
        out.push_str(text);
        cursor = *position;
    }
    out.push_str(&source[cursor..]);
    out
}
