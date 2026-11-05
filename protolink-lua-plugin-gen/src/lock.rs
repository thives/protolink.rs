//! The lock file. It records the exact resolved contracts a build used, so a build never picks up a newer compatible
//! version by accident. A stale or mismatched lock is an error.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::diag::{Code, Diagnostic};
use crate::resolve::Graph;

/// The lock file schema version. Version 3 records each profile's semantic fingerprint beside its file checksum.
const LOCK_VERSION: u32 = 3;

/// The whole lock file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lock {
    /// Lock schema version.
    pub lock_version: u32,
    /// One entry per resolved contract, in id order.
    #[serde(rename = "contract")]
    pub contracts: Vec<LockEntry>,
}

/// One resolved contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockEntry {
    /// Contract name, for people. Names need not be unique; `id` is the identity.
    pub name: String,
    /// Contract identity, the key of the lock.
    pub id: String,
    /// Contract version.
    pub version: String,
    /// Contract digest, as `sha256:<hex>`.
    pub digest: String,
    /// Manifest path relative to the approved root.
    pub source: String,
    /// SHA-256 of the manifest bytes, as hex.
    pub source_sha256: String,
    /// Language profile name.
    pub language_profile: String,
    /// SHA-256 of the profile bytes, as hex. Provenance: formatting changes it.
    pub profile_sha256: String,
    /// Semantic fingerprint of the profile, as hex. Identity: only a policy change changes it.
    pub profile_fingerprint: String,
    /// Imports as `alias=contract id`, sorted.
    #[serde(default)]
    pub imports: Vec<String>,
}

impl Lock {
    /// Builds the lock for the contracts of several roots. A contract that more than one root reaches must resolve to
    /// the same entry in each, or the roots disagree about it and no lock is built.
    pub fn from_graphs(graphs: &[Graph], file: &Path) -> Result<Self, Vec<Diagnostic>> {
        let mut by_id: BTreeMap<String, LockEntry> = BTreeMap::new();
        let mut diagnostics = Vec::new();
        for graph in graphs {
            for entry in entries(graph) {
                match by_id.get(&entry.id) {
                    Some(existing) if *existing != entry => diagnostics.push(Diagnostic::new(
                        Code::DuplicateId,
                        file,
                        format!(
                            "contract '{}' ({}) resolves differently in two roots",
                            entry.name, entry.id
                        ),
                    )),
                    Some(_) => {}
                    None => {
                        by_id.insert(entry.id.clone(), entry);
                    }
                }
            }
        }
        if !diagnostics.is_empty() {
            return Err(diagnostics);
        }
        Ok(Self {
            lock_version: LOCK_VERSION,
            contracts: by_id.into_values().collect(),
        })
    }

    /// Renders the lock as TOML.
    pub fn to_toml(&self) -> String {
        toml::to_string(self).unwrap_or_default()
    }

    /// Parses a lock file.
    pub fn parse(text: &str, file: &Path) -> Result<Self, Diagnostic> {
        let lock: Self =
            toml::from_str(text).map_err(|e| Diagnostic::new(Code::Parse, file, e.to_string()))?;
        if lock.lock_version != LOCK_VERSION {
            return Err(Diagnostic::new(
                Code::UnsupportedSchema,
                file,
                "unsupported lock version",
            ));
        }
        Ok(lock)
    }

    /// Checks that `graphs` are exactly what this lock records. Every difference is reported.
    pub fn verify(&self, graphs: &[Graph], file: &Path) -> Result<(), Vec<Diagnostic>> {
        let expected = Lock::from_graphs(graphs, file)?;
        if expected == *self {
            return Ok(());
        }
        let mut diagnostics = Vec::new();
        for entry in &expected.contracts {
            match self.contracts.iter().find(|c| c.id == entry.id) {
                None => diagnostics.push(Diagnostic::new(
                    Code::StaleLock,
                    file,
                    format!("contract '{}' ({}) is not in the lock", entry.name, entry.id),
                )),
                Some(recorded) if recorded != entry => diagnostics.push(Diagnostic::new(
                    Code::StaleLock,
                    file,
                    format!(
                        "contract '{}' ({}) differs from the lock (digest, source, imports or profile)",
                        entry.name, entry.id
                    ),
                )),
                Some(_) => {}
            }
        }
        for recorded in &self.contracts {
            if !expected.contracts.iter().any(|c| c.id == recorded.id) {
                diagnostics.push(Diagnostic::new(
                    Code::StaleLock,
                    file,
                    format!(
                        "lock records contract '{}' ({}), which is no longer resolved",
                        recorded.name, recorded.id
                    ),
                ));
            }
        }
        Err(diagnostics)
    }
}

impl Lock {
    /// Checks that every contract `graph` reaches is recorded in this lock exactly as resolved. A lock that covers more
    /// roots than `graph` is accepted, so one root of a multi-root lock can be checked on its own, as a host that runs
    /// workflows for one root does before it runs anything.
    pub fn verify_root(&self, graph: &Graph, file: &Path) -> Result<(), Vec<Diagnostic>> {
        let diagnostics: Vec<Diagnostic> = entries(graph)
            .into_iter()
            .filter_map(|entry| match self.contracts.iter().find(|c| c.id == entry.id) {
                None => Some(Diagnostic::new(
                    Code::StaleLock,
                    file,
                    format!("contract '{}' ({}) is not in the lock", entry.name, entry.id),
                )),
                Some(recorded) if *recorded != entry => Some(Diagnostic::new(
                    Code::StaleLock,
                    file,
                    format!(
                        "contract '{}' ({}) differs from the lock (digest, source, imports or profile)",
                        entry.name, entry.id
                    ),
                )),
                Some(_) => None,
            })
            .collect();
        if diagnostics.is_empty() {
            Ok(())
        } else {
            Err(diagnostics)
        }
    }
}

/// The lock entries of one resolved root, one per contract it reaches.
fn entries(graph: &Graph) -> Vec<LockEntry> {
    graph
        .contracts()
        .map(|contract| {
            let source = graph.source(&contract.id);
            let profile = graph.profile(&contract.language_profile);
            LockEntry {
                name: contract.name.clone(),
                id: contract.id.clone(),
                version: contract.version.clone(),
                digest: format!("sha256:{}", hex(&contract.digest)),
                source: source.map(|s| s.path.clone()).unwrap_or_default(),
                source_sha256: source.map(|s| hex(&s.sha256)).unwrap_or_default(),
                language_profile: contract.language_profile.clone(),
                profile_sha256: profile.map(|p| hex(&p.sha256)).unwrap_or_default(),
                profile_fingerprint: hex(&contract.profile_fingerprint),
                imports: source
                    .map(|s| {
                        s.imports
                            .iter()
                            .map(|(alias, id)| format!("{alias}={id}"))
                            .collect()
                    })
                    .unwrap_or_default(),
            }
        })
        .collect()
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
