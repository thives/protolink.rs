//! Resolution of a root manifest and its imports into a validated contract graph.
//!
//! Imports resolve relative to the importing file and must stay inside the approved root. Cycles are refused. A child
//! import must match the identity and exact version it names. Every privilege a contract requires or delegates must be
//! one the relevant contract declares. Validation collects every problem it can find before it fails.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::canonical;
use crate::diag::{Code, Diagnostic};
use crate::model::{
    ChildSlot, Contract, Enumeration, Execution, Limits, Module, Operation, Param, Resources, Type,
    Variant,
};
use crate::profile::Profile;
use crate::schema::{
    RawChild, RawEnum, RawManifest, RawModule, RawOperation, RawParam, RawResources, SCHEMA_VERSION,
};

/// The only intrinsic this generator binds.
const TASK_SPAWN_INTRINSIC: &str = "task.spawn.v1";

/// Stable identifiers live in `1..=65535`.
const ID_MAX: u32 = 65_535;

/// Resolves manifests under one approved root, reading language profiles from one directory.
#[derive(Debug)]
pub struct Resolver {
    root: PathBuf,
    profile_dir: PathBuf,
}

impl Resolver {
    /// Creates a resolver. Both paths must exist.
    pub fn new(
        approved_root: impl AsRef<Path>,
        profile_dir: impl AsRef<Path>,
    ) -> Result<Self, Diagnostic> {
        let root = canonical_dir(approved_root.as_ref())?;
        let profile_dir = canonical_dir(profile_dir.as_ref())?;
        Ok(Self { root, profile_dir })
    }

    /// Resolves `manifest` and every import it names.
    pub fn resolve(&self, manifest: impl AsRef<Path>) -> Result<Graph, Vec<Diagnostic>> {
        let mut loader = Loader {
            root: self.root.clone(),
            profile_dir: self.profile_dir.clone(),
            contracts: BTreeMap::new(),
            sources: BTreeMap::new(),
            by_path: BTreeMap::new(),
            stack: Vec::new(),
            profiles: BTreeMap::new(),
            diagnostics: Vec::new(),
        };
        let path = match manifest.as_ref().canonicalize() {
            Ok(path) => path,
            Err(e) => {
                return Err(vec![Diagnostic::new(
                    Code::Parse,
                    manifest.as_ref(),
                    e.to_string(),
                )]);
            }
        };
        let root = loader.load(&path);
        if !loader.diagnostics.is_empty() {
            return Err(loader.diagnostics);
        }
        let root =
            root.ok_or_else(|| vec![Diagnostic::new(Code::Parse, &path, "manifest did not load")])?;
        Ok(Graph {
            root,
            contracts: loader.contracts,
            profiles: loader.profiles,
            sources: loader.sources,
        })
    }
}

/// The resolved contracts of one root and everything it imports.
#[derive(Debug)]
pub struct Graph {
    root: String,
    contracts: BTreeMap<String, Contract>,
    profiles: BTreeMap<String, Profile>,
    sources: BTreeMap<String, Source>,
}

/// Where a contract came from, for the lock file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    /// Path relative to the approved root, with `/` separators.
    pub path: String,
    /// SHA-256 of the manifest bytes.
    pub sha256: [u8; 32],
    /// Imports as alias to contract id.
    pub imports: BTreeMap<String, String>,
}

impl Graph {
    /// The root contract.
    pub fn root(&self) -> &Contract {
        &self.contracts[&self.root]
    }

    /// A contract by id. Ids are unique; names are not.
    pub fn contract(&self, id: &str) -> Option<&Contract> {
        self.contracts.get(id)
    }

    /// All contracts by id, in id order.
    pub fn contracts(&self) -> impl Iterator<Item = &Contract> {
        self.contracts.values()
    }

    /// A language profile by name.
    pub fn profile(&self, name: &str) -> Option<&Profile> {
        self.profiles.get(name)
    }

    /// The source record of a contract, by id.
    pub fn source(&self, id: &str) -> Option<&Source> {
        self.sources.get(id)
    }
}

struct Loader {
    root: PathBuf,
    profile_dir: PathBuf,
    contracts: BTreeMap<String, Contract>,
    sources: BTreeMap<String, Source>,
    by_path: BTreeMap<PathBuf, String>,
    stack: Vec<PathBuf>,
    profiles: BTreeMap<String, Profile>,
    diagnostics: Vec<Diagnostic>,
}

impl Loader {
    /// Loads one manifest and, first, its imports. Returns the contract id, or `None` after recording a problem.
    fn load(&mut self, path: &Path) -> Option<String> {
        if let Some(id) = self.by_path.get(path) {
            return Some(id.clone());
        }
        if self.stack.iter().any(|p| p == path) {
            self.diagnostics.push(Diagnostic::new(
                Code::ImportCycle,
                path,
                "imports form a cycle",
            ));
            return None;
        }
        if !path.starts_with(&self.root) {
            self.diagnostics.push(Diagnostic::new(
                Code::PathEscapesRoot,
                path,
                "manifest is outside the approved root",
            ));
            return None;
        }
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) => {
                self.diagnostics
                    .push(Diagnostic::new(Code::Parse, path, e.to_string()));
                return None;
            }
        };
        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text,
            Err(_) => {
                self.diagnostics
                    .push(Diagnostic::new(Code::Parse, path, "manifest is not UTF-8"));
                return None;
            }
        };
        let raw: RawManifest = match toml::from_str(text) {
            Ok(raw) => raw,
            Err(e) => {
                self.diagnostics
                    .push(Diagnostic::new(Code::Parse, path, e.to_string()));
                return None;
            }
        };
        if raw.schema_version != SCHEMA_VERSION {
            self.diagnostics.push(Diagnostic::new(
                Code::UnsupportedSchema,
                path,
                format!("schema version {} is not supported", raw.schema_version),
            ));
            return None;
        }

        self.stack.push(path.to_path_buf());
        let dir = path.parent().unwrap_or(&self.root).to_path_buf();
        let mut imports = BTreeMap::new();
        let mut aliases = BTreeSet::new();
        for import in &raw.imports {
            if !aliases.insert(import.alias.clone()) {
                self.diagnostics.push(Diagnostic::new(
                    Code::DuplicateName,
                    path,
                    format!("import alias '{}' is declared twice", import.alias),
                ));
                continue;
            }
            let target = match dir.join(&import.path).canonicalize() {
                Ok(target) => target,
                Err(e) => {
                    self.diagnostics.push(Diagnostic::new(
                        Code::Parse,
                        path,
                        format!("import '{}' cannot be read: {e}", import.alias),
                    ));
                    continue;
                }
            };
            let Some(child) = self.load(&target) else {
                continue;
            };
            let identity_ok = self.contracts.get(&child).is_some_and(|c| {
                c.id == import.contract_id
                    && import.version.strip_prefix('=') == Some(c.version.as_str())
            });
            if !identity_ok {
                // The alias is still bound, so its child slots do not cascade into unknown-alias errors. The mismatch
                // itself stops this manifest from building.
                self.diagnostics.push(Diagnostic::new(
                    Code::ImportIdentityMismatch,
                    path,
                    format!(
                        "import '{}' does not match contract '{child}' (id and exact version '=x.y.z' must agree)",
                        import.alias
                    ),
                ));
            }
            imports.insert(import.alias.clone(), child);
        }

        let built = self.build(path, &raw, &imports, &bytes);
        self.stack.pop();
        let contract = built?;
        let id = contract.id.clone();
        if self.contracts.contains_key(&id) {
            self.diagnostics.push(Diagnostic::new(
                Code::DuplicateId,
                path,
                format!("contract id '{id}' is already declared by another manifest"),
            ));
            return None;
        }
        self.by_path.insert(path.to_path_buf(), id.clone());
        self.contracts.insert(id.clone(), contract);
        Some(id)
    }

    fn profile(&mut self, file: &Path, name: &str) -> Option<Profile> {
        if let Some(profile) = self.profiles.get(name) {
            return Some(profile.clone());
        }
        let profile_path = self.profile_dir.join(format!("{name}.toml"));
        let bytes = match fs::read(&profile_path) {
            Ok(bytes) => bytes,
            Err(_) => {
                self.diagnostics.push(Diagnostic::new(
                    Code::Parse,
                    file,
                    format!("language profile '{name}' is not available in the profile directory"),
                ));
                return None;
            }
        };
        let text = String::from_utf8_lossy(&bytes).into_owned();
        match Profile::parse(&text, &bytes, &profile_path) {
            Ok(profile) => {
                self.profiles.insert(name.to_string(), profile.clone());
                Some(profile)
            }
            Err(diag) => {
                self.diagnostics.push(diag);
                None
            }
        }
    }

    fn build(
        &mut self,
        path: &Path,
        raw: &RawManifest,
        imports: &BTreeMap<String, String>,
        bytes: &[u8],
    ) -> Option<Contract> {
        let before = self.diagnostics.len();
        let mut diagnostics = Vec::new();
        let c = &raw.contract;
        if c.name.is_empty() || c.id.is_empty() || c.version.is_empty() {
            diagnostics.push(Diagnostic::new(
                Code::Parse,
                path,
                "contract name, id and version must be non-empty",
            ));
        }
        let limits = Limits {
            max_string_bytes: c.limits.max_string_bytes,
            max_tuple_items: c.limits.max_tuple_items,
            max_capture_bytes: c.limits.max_capture_bytes,
            max_result_bytes: c.limits.max_result_bytes,
            max_spawn_depth: c.limits.max_spawn_depth,
        };
        let limit_values = [
            limits.max_string_bytes,
            limits.max_tuple_items,
            limits.max_capture_bytes,
            limits.max_result_bytes,
            limits.max_spawn_depth,
        ];
        if limit_values.contains(&0) {
            diagnostics.push(Diagnostic::new(
                Code::InvalidLimit,
                path,
                "every limit must be greater than zero",
            ));
        }
        let privileges = unique_sorted(path, "privileges", &c.privileges, &mut diagnostics);
        let privilege_set: BTreeSet<&str> = privileges.iter().map(String::as_str).collect();

        // A missing or invalid profile is recorded by `profile`, which the check below relies on.
        let profile = self.profile(path, &c.language_profile);
        let profile_ok = profile.is_some();

        let mut module_names = BTreeSet::new();
        let mut module_ids = BTreeSet::new();
        let mut modules = Vec::new();
        for raw_module in &raw.modules {
            if !module_names.insert(raw_module.name.clone()) {
                diagnostics.push(Diagnostic::new(
                    Code::DuplicateName,
                    path,
                    format!("module '{}' is declared twice", raw_module.name),
                ));
            }
            check_id(
                path,
                &format!("module '{}'", raw_module.name),
                raw_module.id,
                &mut module_ids,
                &mut diagnostics,
            );
            if let Some(module) = build_module(path, raw_module, &privilege_set, &mut diagnostics) {
                modules.push(module);
            }
        }
        // An imported module is copied into this surface unchanged, so its digest and its identifiers are part of this
        // contract. Its operations need privileges this contract declares, because this contract exposes them.
        for raw_import in &raw.module_imports {
            let Some(source_name) = imports.get(&raw_import.alias) else {
                diagnostics.push(Diagnostic::new(
                    Code::UnknownImport,
                    path,
                    format!(
                        "module '{}' is imported through alias '{}', which is not imported",
                        raw_import.module, raw_import.alias
                    ),
                ));
                continue;
            };
            let Some(source) = self
                .contracts
                .get(source_name)
                .and_then(|c| c.module(&raw_import.module))
            else {
                diagnostics.push(Diagnostic::new(
                    Code::UnknownModule,
                    path,
                    format!(
                        "contract '{source_name}' does not declare module '{}'",
                        raw_import.module
                    ),
                ));
                continue;
            };
            if !module_names.insert(source.name.clone()) {
                diagnostics.push(Diagnostic::new(
                    Code::DuplicateName,
                    path,
                    format!("module '{}' is declared twice", source.name),
                ));
            }
            check_id(
                path,
                &format!("module '{}'", source.name),
                source.id,
                &mut module_ids,
                &mut diagnostics,
            );
            for operation in &source.operations {
                for privilege in &operation.requires {
                    if !privilege_set.contains(privilege.as_str()) {
                        diagnostics.push(Diagnostic::new(
                            Code::UndeclaredPrivilege,
                            path,
                            format!(
                                "imported module '{}' operation '{}' requires '{privilege}', which the contract does not declare",
                                source.name, operation.name
                            ),
                        ));
                    }
                }
            }
            modules.push(source.clone());
        }
        modules.sort_by_key(|m| m.id);
        // Every operation the contract exposes, its own and the imported ones, must fit the consumer's native ABI, used
        // or not: the generated adapters cover them all.
        if let Some(abi) = profile.as_ref().and_then(|p| p.native_abi.as_ref()) {
            for module in &modules {
                for operation in &module.operations {
                    if operation.results.len() > abi.max_results as usize {
                        diagnostics.push(Diagnostic::new(
                            Code::UnsupportedSignature,
                            path,
                            format!(
                                "operation '{}.{}' returns {} results; the native ABI '{}' of profile '{}' carries at most {}",
                                module.name,
                                operation.name,
                                operation.results.len(),
                                abi.name,
                                c.language_profile,
                                abi.max_results
                            ),
                        ));
                    }
                }
            }
        }

        let mut children = Vec::new();
        let mut child_names = BTreeSet::new();
        let mut child_ids = BTreeSet::new();
        let mut child_indices = BTreeSet::new();
        for raw_child in &raw.child_execution {
            if !child_names.insert(raw_child.name.clone()) || module_names.contains(&raw_child.name)
            {
                diagnostics.push(Diagnostic::new(
                    Code::DuplicateName,
                    path,
                    format!("child slot '{}' collides with another name", raw_child.name),
                ));
            }
            check_id(
                path,
                &format!("child slot '{}'", raw_child.name),
                raw_child.id,
                &mut child_ids,
                &mut diagnostics,
            );
            if !child_indices.insert(raw_child.index) {
                diagnostics.push(Diagnostic::new(
                    Code::DuplicateId,
                    path,
                    format!(
                        "child slot '{}' reuses index {}",
                        raw_child.name, raw_child.index
                    ),
                ));
            }
            if let Some(child) =
                self.build_child(path, raw_child, imports, &privilege_set, &mut diagnostics)
            {
                children.push(child);
            }
        }
        children.sort_by_key(|c| c.id);

        let resources = raw
            .resources
            .as_ref()
            .and_then(|r| build_resources(path, r, &limits, &mut diagnostics));

        // Keep every problem found in this manifest, and build nothing from it when any was found.
        self.diagnostics.extend(diagnostics);
        let profile = profile.filter(|_| self.diagnostics.len() == before && profile_ok)?;

        let mut contract = Contract {
            name: c.name.clone(),
            id: c.id.clone(),
            version: c.version.clone(),
            language_profile: c.language_profile.clone(),
            profile_version: profile.version.clone(),
            profile_fingerprint: profile.fingerprint,
            remote_data_profile: c.remote_data_profile.clone(),
            privileges,
            limits,
            modules,
            children,
            resources,
            digest: [0; 32],
        };
        contract.digest = canonical::digest(&contract);
        let mut sha256 = [0u8; 32];
        sha256.copy_from_slice(&Sha256::digest(bytes));
        let relative = path
            .strip_prefix(&self.root)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default();
        self.sources.insert(
            contract.id.clone(),
            Source {
                path: relative,
                sha256,
                imports: imports.clone(),
            },
        );
        Some(contract)
    }

    fn build_child(
        &self,
        path: &Path,
        raw: &RawChild,
        imports: &BTreeMap<String, String>,
        parent_privileges: &BTreeSet<&str>,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<ChildSlot> {
        let Some(child_id) = imports.get(&raw.child_contract) else {
            diagnostics.push(Diagnostic::new(
                Code::UnknownChildContract,
                path,
                format!(
                    "child slot '{}' refers to alias '{}', which is not imported",
                    raw.name, raw.child_contract
                ),
            ));
            return None;
        };
        let child = self.contracts.get(child_id)?;
        if raw.intrinsic != TASK_SPAWN_INTRINSIC {
            diagnostics.push(Diagnostic::new(
                Code::UnknownIntrinsic,
                path,
                format!(
                    "child slot '{}' uses unknown intrinsic '{}'",
                    raw.name, raw.intrinsic
                ),
            ));
            return None;
        }
        let requires = unique_sorted(path, "requires", &raw.requires, diagnostics);
        let delegates = unique_sorted(path, "delegates", &raw.delegates, diagnostics);
        for privilege in requires.iter().chain(delegates.iter()) {
            if !child.privileges.contains(privilege) {
                diagnostics.push(Diagnostic::new(
                    Code::UndeclaredPrivilege,
                    path,
                    format!(
                        "child slot '{}' names privilege '{privilege}', which contract '{child_id}' does not declare",
                        raw.name
                    ),
                ));
            }
        }
        for privilege in &delegates {
            if !parent_privileges.contains(privilege.as_str()) {
                diagnostics.push(Diagnostic::new(
                    Code::UndeclaredPrivilege,
                    path,
                    format!(
                        "child slot '{}' delegates privilege '{privilege}', which this contract does not declare",
                        raw.name
                    ),
                ));
            }
        }
        if raw.max_capture_bytes == 0 || raw.max_result_bytes == 0 {
            diagnostics.push(Diagnostic::new(
                Code::InvalidLimit,
                path,
                format!("child slot '{}' has a zero byte limit", raw.name),
            ));
        }
        Some(ChildSlot {
            name: raw.name.clone(),
            id: raw.id,
            index: raw.index,
            child: child_id.clone(),
            child_digest: child.digest,
            intrinsic: raw.intrinsic.clone(),
            requires,
            delegates,
            max_capture_bytes: raw.max_capture_bytes,
            max_result_bytes: raw.max_result_bytes,
        })
    }
}

fn build_module(
    path: &Path,
    raw: &RawModule,
    privileges: &BTreeSet<&str>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Module> {
    let owner = format!("module '{}'", raw.name);
    let mut error_names = BTreeSet::new();
    let mut error_ids = BTreeSet::new();
    let mut errors = Vec::new();
    for error in &raw.errors {
        if !error_names.insert(error.name.clone()) {
            diagnostics.push(Diagnostic::new(
                Code::DuplicateName,
                path,
                format!("{owner} repeats error '{}'", error.name),
            ));
        }
        check_id(
            path,
            &format!("{owner} error '{}'", error.name),
            error.id,
            &mut error_ids,
            diagnostics,
        );
        errors.push((error.name.clone(), error.id));
    }
    errors.sort_by_key(|(_, id)| *id);

    let mut enums = Vec::new();
    let mut enum_names = BTreeSet::new();
    let mut enum_ids = BTreeSet::new();
    for raw_enum in &raw.enums {
        if !enum_names.insert(raw_enum.name.clone()) {
            diagnostics.push(Diagnostic::new(
                Code::DuplicateName,
                path,
                format!("{owner} repeats enum '{}'", raw_enum.name),
            ));
        }
        check_id(
            path,
            &format!("{owner} enum '{}'", raw_enum.name),
            raw_enum.id,
            &mut enum_ids,
            diagnostics,
        );
        if let Some(enumeration) = build_enum(path, &owner, raw_enum, diagnostics) {
            enums.push(enumeration);
        }
    }
    enums.sort_by_key(|e| e.id);

    let mut op_names = BTreeSet::new();
    let mut op_ids = BTreeSet::new();
    let mut operations = Vec::new();
    for raw_op in &raw.operations {
        if !op_names.insert(raw_op.name.clone()) {
            diagnostics.push(Diagnostic::new(
                Code::DuplicateName,
                path,
                format!("{owner} repeats operation '{}'", raw_op.name),
            ));
        }
        check_id(
            path,
            &format!("{owner} operation '{}'", raw_op.name),
            raw_op.id,
            &mut op_ids,
            diagnostics,
        );
        if let Some(operation) = build_operation(
            path,
            &owner,
            &raw.name,
            raw_op,
            &enums,
            &error_names,
            privileges,
            diagnostics,
        ) {
            operations.push(operation);
        }
    }
    operations.sort_by_key(|o| o.id);

    Some(Module {
        name: raw.name.clone(),
        id: raw.id,
        errors,
        enums,
        operations,
    })
}

fn build_enum(
    path: &Path,
    owner: &str,
    raw: &RawEnum,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Enumeration> {
    let mut names = BTreeSet::new();
    let mut ids = BTreeSet::new();
    let mut symbols = BTreeSet::new();
    let mut variants = Vec::new();
    for variant in &raw.variants {
        if !names.insert(variant.name.clone()) {
            diagnostics.push(Diagnostic::new(
                Code::DuplicateName,
                path,
                format!(
                    "{owner} enum '{}' repeats variant '{}'",
                    raw.name, variant.name
                ),
            ));
        }
        if !symbols.insert(variant.symbol.clone()) || variant.symbol.is_empty() {
            diagnostics.push(Diagnostic::new(
                Code::DuplicateName,
                path,
                format!(
                    "{owner} enum '{}' has an empty or repeated symbol",
                    raw.name
                ),
            ));
        }
        check_id(
            path,
            &format!("{owner} enum '{}' variant '{}'", raw.name, variant.name),
            variant.id,
            &mut ids,
            diagnostics,
        );
        variants.push(Variant {
            name: variant.name.clone(),
            id: variant.id,
            symbol: variant.symbol.clone(),
        });
    }
    variants.sort_by_key(|v| v.id);
    Some(Enumeration {
        name: raw.name.clone(),
        id: raw.id,
        variants,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_operation(
    path: &Path,
    owner: &str,
    module: &str,
    raw: &RawOperation,
    enums: &[Enumeration],
    errors: &BTreeSet<String>,
    privileges: &BTreeSet<&str>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Operation> {
    let execution = match raw.execution.as_str() {
        "sync" => Execution::Sync,
        "async" => Execution::Async,
        other => {
            diagnostics.push(Diagnostic::new(
                Code::UnknownExecution,
                path,
                format!(
                    "{owner} operation '{}' has unknown execution '{other}'",
                    raw.name
                ),
            ));
            return None;
        }
    };
    let mut ok = true;
    let params = build_params(
        path,
        owner,
        module,
        &raw.name,
        &raw.params,
        enums,
        &mut ok,
        diagnostics,
    );
    let results = build_params(
        path,
        owner,
        module,
        &raw.name,
        &raw.results,
        enums,
        &mut ok,
        diagnostics,
    );
    for error in &raw.errors {
        if !errors.contains(error) {
            ok = false;
            diagnostics.push(Diagnostic::new(
                Code::UnknownError,
                path,
                format!(
                    "{owner} operation '{}' names undeclared error '{error}'",
                    raw.name
                ),
            ));
        }
    }
    let requires = unique_sorted(path, "requires", &raw.requires, diagnostics);
    for privilege in &requires {
        if !privileges.contains(privilege.as_str()) {
            ok = false;
            diagnostics.push(Diagnostic::new(
                Code::UndeclaredPrivilege,
                path,
                format!("{owner} operation '{}' requires '{privilege}', which the contract does not declare", raw.name),
            ));
        }
    }
    if !ok {
        return None;
    }
    Some(Operation {
        name: raw.name.clone(),
        id: raw.id,
        execution,
        params,
        results,
        errors: raw.errors.clone(),
        requires,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_params(
    path: &Path,
    owner: &str,
    module: &str,
    operation: &str,
    raw: &[RawParam],
    enums: &[Enumeration],
    ok: &mut bool,
    diagnostics: &mut Vec<Diagnostic>,
) -> Vec<Param> {
    let mut out = Vec::new();
    let mut names = BTreeSet::new();
    for param in raw {
        if !names.insert(param.name.clone()) {
            *ok = false;
            diagnostics.push(Diagnostic::new(
                Code::DuplicateName,
                path,
                format!(
                    "{owner} operation '{operation}' repeats parameter '{}'",
                    param.name
                ),
            ));
        }
        match parse_type(module, param, enums) {
            Ok(ty) => out.push(Param {
                name: param.name.clone(),
                ty,
            }),
            Err(message) => {
                *ok = false;
                diagnostics.push(Diagnostic::new(
                    if message.contains("enum") {
                        Code::UnresolvedEnum
                    } else {
                        Code::UnknownType
                    },
                    path,
                    format!(
                        "{owner} operation '{operation}' parameter '{}': {message}",
                        param.name
                    ),
                ));
            }
        }
    }
    out
}

fn parse_type(module: &str, param: &RawParam, enums: &[Enumeration]) -> Result<Type, String> {
    match param.ty.as_str() {
        "nil" => Ok(Type::Nil),
        "boolean" => Ok(Type::Boolean),
        "integer" => Ok(Type::Integer),
        "float" => Ok(Type::Float),
        "bytes" => match param.max_bytes {
            Some(max_bytes) if max_bytes > 0 => Ok(Type::Bytes { max_bytes }),
            _ => Err("bytes requires a positive max_bytes".to_string()),
        },
        "enum" => {
            let reference = param
                .reference
                .as_deref()
                .ok_or_else(|| "enum requires a ref".to_string())?;
            let (owner, name) = match reference.split_once('.') {
                Some((owner, name)) => (owner, name),
                None => (module, reference),
            };
            if owner != module {
                return Err(format!(
                    "enum '{reference}' must be declared in module '{module}'"
                ));
            }
            if enums.iter().any(|e| e.name == name) {
                Ok(Type::Enum {
                    reference: format!("{module}.{name}"),
                })
            } else {
                Err(format!(
                    "enum '{reference}' is not declared (unresolved enum)"
                ))
            }
        }
        other => Err(format!("unknown type '{other}'")),
    }
}

/// The largest answer of any mutating `lua.execution.v1` call, in encoded bytes. The node refuses a smaller answer bound,
/// so the generator does too. `protolink_lua_node`'s tests check that this equals the protocol crate's constant.
pub const MIN_ANSWER_BYTES: u32 = 72;

/// The smallest request bound that admits every request an envelope allows: a full `Start` with two tuples at the
/// contract's limits, a full `Resume`, and a full `PutChunk`. `protolink_lua_node`'s tests check that this equals the protocol
/// crate's own computation.
pub fn min_request_bytes(limits: &Limits, max_chunk_bytes: u32) -> u64 {
    let tuple = 6 + u64::from(limits.max_tuple_items) * 17 + u64::from(limits.max_result_bytes);
    let start = 81 + 2 * tuple;
    let resume = 52 + tuple;
    let chunk = 58 + u64::from(max_chunk_bytes);
    start.max(resume).max(chunk).max(128)
}

/// Checks a resource envelope. Every budget must be positive, and the budgets must be able to work together: an upload
/// chunk fits an artifact, a protocol answer fits the replay bound, a full request fits the request bound, the retirement
/// window covers the live sessions, and the artifact store can always evict.
fn build_resources(
    path: &Path,
    raw: &RawResources,
    limits: &Limits,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Resources> {
    let values = [
        u64::from(raw.max_artifact_bytes),
        u64::from(raw.max_chunk_bytes),
        u64::from(raw.max_uploads),
        u64::from(raw.max_sessions),
        u64::from(raw.max_executions),
        u64::from(raw.max_answer_bytes),
        u64::from(raw.slice_fuel),
        raw.total_fuel,
        u64::from(raw.max_frames),
        u64::from(raw.max_objects),
        u64::from(raw.max_heap_bytes),
        u64::from(raw.session_idle_ms),
        u64::from(raw.max_artifacts),
        u64::from(raw.retired_window),
        u64::from(raw.max_child_tasks),
        u64::from(raw.call_timeout_ms),
        u64::from(raw.cleanup_ms),
        u64::from(raw.max_request_bytes),
    ];
    if values.contains(&0) {
        diagnostics.push(Diagnostic::new(
            Code::InvalidLimit,
            path,
            "every resource must be greater than zero",
        ));
        return None;
    }
    if raw.max_artifacts <= raw.max_executions {
        diagnostics.push(Diagnostic::new(
            Code::InvalidLimit,
            path,
            "max_artifacts must exceed max_executions, so that a commit can always evict an artifact no execution runs",
        ));
        return None;
    }
    if raw.max_chunk_bytes > raw.max_artifact_bytes {
        diagnostics.push(Diagnostic::new(
            Code::InvalidLimit,
            path,
            "max_chunk_bytes cannot exceed max_artifact_bytes",
        ));
        return None;
    }
    if raw.max_answer_bytes < MIN_ANSWER_BYTES {
        diagnostics.push(Diagnostic::new(
            Code::InvalidLimit,
            path,
            format!(
                "max_answer_bytes must be at least {MIN_ANSWER_BYTES}, the largest answer of a mutating call"
            ),
        ));
        return None;
    }
    if raw.retired_window < raw.max_sessions {
        diagnostics.push(Diagnostic::new(
            Code::InvalidLimit,
            path,
            "retired_window must be at least max_sessions, so the window covers every live session",
        ));
        return None;
    }
    let required = min_request_bytes(limits, raw.max_chunk_bytes);
    if u64::from(raw.max_request_bytes) < required {
        diagnostics.push(Diagnostic::new(
            Code::InvalidLimit,
            path,
            format!(
                "max_request_bytes must be at least {required}, the largest request these limits allow"
            ),
        ));
        return None;
    }
    Some(Resources {
        max_artifact_bytes: raw.max_artifact_bytes,
        max_chunk_bytes: raw.max_chunk_bytes,
        max_uploads: raw.max_uploads,
        max_sessions: raw.max_sessions,
        max_executions: raw.max_executions,
        max_answer_bytes: raw.max_answer_bytes,
        slice_fuel: raw.slice_fuel,
        total_fuel: raw.total_fuel,
        max_frames: raw.max_frames,
        max_objects: raw.max_objects,
        max_heap_bytes: raw.max_heap_bytes,
        session_idle_ms: raw.session_idle_ms,
        max_artifacts: raw.max_artifacts,
        retired_window: raw.retired_window,
        max_child_tasks: raw.max_child_tasks,
        call_timeout_ms: raw.call_timeout_ms,
        cleanup_ms: raw.cleanup_ms,
        max_request_bytes: raw.max_request_bytes,
    })
}

fn check_id(
    path: &Path,
    owner: &str,
    id: u32,
    seen: &mut BTreeSet<u32>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if id == 0 || id > ID_MAX {
        diagnostics.push(Diagnostic::new(
            Code::IdOutOfRange,
            path,
            format!("{owner} id {id} is outside 1..={ID_MAX}"),
        ));
    } else if !seen.insert(id) {
        diagnostics.push(Diagnostic::new(
            Code::DuplicateId,
            path,
            format!("{owner} reuses id {id}"),
        ));
    }
}

fn unique_sorted(
    path: &Path,
    field: &str,
    values: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value.clone()) {
            diagnostics.push(Diagnostic::new(
                Code::DuplicateName,
                path,
                format!("{field} lists '{value}' twice"),
            ));
        }
    }
    seen.into_iter().collect()
}

fn canonical_dir(path: &Path) -> Result<PathBuf, Diagnostic> {
    path.canonicalize().map_err(|e| {
        Diagnostic::new(
            Code::Parse,
            path,
            format!("directory is not available: {e}"),
        )
    })
}
