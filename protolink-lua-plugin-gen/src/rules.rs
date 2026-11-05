//! Checker rules: the facts the remote-language checker needs, derived from the resolved graph and a language profile.
//!
//! The compiler asks these rules, and nothing else, whether a name, module operation, enum symbol or child spawn is
//! valid in a region that runs on a given contract. The same model renders the Rust bindings and the LuaLS definitions.

use std::collections::{BTreeMap, BTreeSet};

use protolink_lua_bytecode::NativeRef;

use crate::model::{Contract, Execution, Limits, Resources, Type};
use crate::profile::NativeAbi;
use crate::resolve::Graph;

/// The checker's view of one contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckerRules {
    /// Contract id, the identity of the contract.
    pub id: String,
    /// Digest of the contract. An artifact compiled for this contract carries it.
    pub digest: [u8; 32],
    /// Contract name, for people.
    pub contract: String,
    /// Language profile name.
    pub profile: String,
    /// Language profile version.
    pub profile_version: String,
    /// Semantic fingerprint of the profile, which the contract digest includes.
    pub profile_fingerprint: [u8; 32],
    /// Features the profile allows in remote regions.
    pub allowed_features: BTreeSet<String>,
    /// The consumer's native ABI, when it restricts native signatures.
    pub native_abi: Option<NativeAbi>,
    /// The contract's value and nesting limits.
    pub limits: Limits,
    /// The resource envelope of the node that runs the contract, or `None` for a contract that no node runs.
    pub resources: Option<Resources>,
    /// The destination's native operation table, from [`Contract::native_operations`]. The compiler admits its own output
    /// against exactly this table, so compiled code is accepted by the node that runs it.
    pub natives: Vec<NativeRef>,
    /// Native modules by name.
    pub modules: BTreeMap<String, ModuleRules>,
    /// Child slots by name.
    pub children: BTreeMap<String, ChildRules>,
}

/// Rules for one native module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleRules {
    /// Stable module identifier.
    pub id: u32,
    /// Enumerations by name.
    pub enums: BTreeMap<String, EnumRules>,
    /// Operations by name.
    pub operations: BTreeMap<String, OperationRules>,
}

/// Rules for one enumeration: variant name to Lua symbol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumRules {
    /// Variants by name, each with its symbol.
    pub variants: BTreeMap<String, String>,
}

/// Rules for one operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationRules {
    /// Stable operation identifier, unique within its module.
    pub id: u32,
    /// Number of parameters.
    pub arity: usize,
    /// Parameter types, in order.
    pub params: Vec<Type>,
    /// Result types, in order. The full signature is kept even where a consumer's ABI carries fewer.
    pub results: Vec<Type>,
    /// Synchronous or asynchronous.
    pub execution: Execution,
    /// Privileges the caller must hold, sorted.
    pub requires: Vec<String>,
}

/// Rules for one child slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildRules {
    /// Stable slot identifier.
    pub slot_id: u32,
    /// Compile-time address of the remote node on this slot's link.
    pub index: u8,
    /// Id of the child contract, which the checker resolves in the same graph.
    pub child: String,
    /// Privileges the child needs.
    pub requires: Vec<String>,
    /// Privileges the child may delegate.
    pub delegates: Vec<String>,
    /// Largest capture snapshot in bytes.
    pub max_capture_bytes: u32,
    /// Largest result the child may return, in bytes.
    pub max_result_bytes: u32,
}

impl CheckerRules {
    /// True when `name` is a module or child slot of this contract. Those names are the remote region's capabilities.
    pub fn is_capability(&self, name: &str) -> bool {
        self.modules.contains_key(name) || self.children.contains_key(name)
    }
}

impl Graph {
    /// The checker rules of the contract with id `id`, or `None` when the contract is not in the graph.
    pub fn checker_rules(&self, id: &str) -> Option<CheckerRules> {
        let contract = self.contract(id)?;
        let profile = self.profile(&contract.language_profile)?;
        Some(rules_of(
            contract,
            profile.name.clone(),
            profile.allowed.clone(),
            profile.native_abi.clone(),
        ))
    }
}

fn rules_of(
    contract: &Contract,
    profile: String,
    allowed_features: BTreeSet<String>,
    native_abi: Option<NativeAbi>,
) -> CheckerRules {
    let modules = contract
        .modules
        .iter()
        .map(|module| {
            let enums = module
                .enums
                .iter()
                .map(|e| {
                    let variants = e
                        .variants
                        .iter()
                        .map(|v| (v.name.clone(), v.symbol.clone()))
                        .collect();
                    (e.name.clone(), EnumRules { variants })
                })
                .collect();
            let operations = module
                .operations
                .iter()
                .map(|op| {
                    (
                        op.name.clone(),
                        OperationRules {
                            id: op.id,
                            arity: op.params.len(),
                            params: op.params.iter().map(|p| p.ty.clone()).collect(),
                            results: op.results.iter().map(|r| r.ty.clone()).collect(),
                            execution: op.execution,
                            requires: op.requires.clone(),
                        },
                    )
                })
                .collect();
            (
                module.name.clone(),
                ModuleRules {
                    id: module.id,
                    enums,
                    operations,
                },
            )
        })
        .collect();
    let children = contract
        .children
        .iter()
        .map(|child| {
            (
                child.name.clone(),
                ChildRules {
                    slot_id: child.id,
                    index: child.index,
                    child: child.child.clone(),
                    requires: child.requires.clone(),
                    delegates: child.delegates.clone(),
                    max_capture_bytes: child.max_capture_bytes,
                    max_result_bytes: child.max_result_bytes,
                },
            )
        })
        .collect();
    CheckerRules {
        id: contract.id.clone(),
        digest: contract.digest,
        contract: contract.name.clone(),
        profile,
        profile_version: contract.profile_version.clone(),
        profile_fingerprint: contract.profile_fingerprint,
        allowed_features,
        native_abi,
        limits: contract.limits,
        resources: contract.resources,
        natives: contract.native_operations(),
        modules,
        children,
    }
}
