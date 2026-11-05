//! Resolution, validation, digest and lock tests. Each test works in its own temporary tree.

use std::fs;
use std::path::{Path, PathBuf};

use protolink_lua_plugin_gen::{Code, Lock, Resolver, lua_definitions, rust_bindings};

const PROFILE: &str = r#"
schema_version = 1
name = "synthetic-v1"
version = "1.0.0"
[features]
allowed = ["local_assignment", "call", "return", "arithmetic"]
"#;

fn tree(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("pl-gen-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("profiles")).expect("profiles dir");
    fs::write(dir.join("profiles/synthetic-v1.toml"), PROFILE).expect("profile");
    dir
}

fn write(dir: &Path, rel: &str, text: &str) -> PathBuf {
    let path = dir.join(rel);
    fs::create_dir_all(path.parent().expect("parent")).expect("dirs");
    fs::write(&path, text).expect("write");
    path
}

fn resolve(
    dir: &Path,
    manifest: &Path,
) -> Result<protolink_lua_plugin_gen::Graph, Vec<protolink_lua_plugin_gen::Diagnostic>> {
    Resolver::new(dir, dir.join("profiles"))
        .expect("resolver")
        .resolve(manifest)
}

fn codes(
    result: Result<protolink_lua_plugin_gen::Graph, Vec<protolink_lua_plugin_gen::Diagnostic>>,
) -> Vec<Code> {
    match result {
        Ok(_) => panic!("expected diagnostics"),
        Err(diagnostics) => diagnostics.iter().map(|d| d.code).collect(),
    }
}

/// A synthetic, non-BSP plugin: a sensor with a `probe` child and one native module.
fn sensor(extra_root: &str, module_body: &str) -> String {
    format!(
        r#"
schema_version = 1

[contract]
name = "synthetic.sensor"
id = "11111111-2222-4333-8444-555555555555"
version = "1.0.0"
language_profile = "synthetic-v1"
remote_data_profile = "remote-data-v1"
privileges = ["sensor.read", "probe.run"]
{extra_root}

[contract.limits]
max_string_bytes = 64
max_tuple_items = 4
max_capture_bytes = 128
max_result_bytes = 128
max_spawn_depth = 2

[[imports]]
alias = "probe"
path = "../probe/plugin.toml"
contract_id = "99999999-8888-4777-8666-555555555555"
version = "=1.0.0"

[[modules]]
name = "meter"
id = 1
{module_body}

[[child_execution]]
name = "probe"
id = 1
index = 1
child_contract = "probe"
intrinsic = "task.spawn.v1"
requires = ["probe.run"]
delegates = ["probe.run"]
max_capture_bytes = 64
max_result_bytes = 64
"#
    )
}

const PROBE: &str = r#"
schema_version = 1

[contract]
name = "synthetic.probe"
id = "99999999-8888-4777-8666-555555555555"
version = "1.0.0"
language_profile = "synthetic-v1"
remote_data_profile = "remote-data-v1"
privileges = ["probe.run"]

[contract.limits]
max_string_bytes = 64
max_tuple_items = 4
max_capture_bytes = 64
max_result_bytes = 64
max_spawn_depth = 1

[[modules]]
name = "clock"
id = 1

[[modules.operations]]
name = "tick"
id = 1
execution = "sync"
results = [{ name = "count", type = "integer" }]
requires = ["probe.run"]
"#;

const METER_OPS: &str = r#"
[[modules.enums]]
name = "Unit"
id = 1
variants = [
  { name = "volts", id = 1, symbol = "volts" },
  { name = "amps", id = 2, symbol = "amps" },
]

[[modules.operations]]
name = "read"
id = 1
execution = "sync"
params = [{ name = "unit", type = "enum", ref = "Unit" }]
results = [{ name = "value", type = "float" }]
requires = ["sensor.read"]
"#;

fn valid_tree(name: &str) -> (PathBuf, PathBuf) {
    let dir = tree(name);
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(&dir, "sensor/plugin.toml", &sensor("", METER_OPS));
    (dir, manifest)
}

#[test]
fn a_synthetic_plugin_with_a_child_resolves_and_renders() {
    let (dir, manifest) = valid_tree("valid");
    let graph = resolve(&dir, &manifest).expect("resolves");
    let root = graph.root();
    assert_eq!(root.name, "synthetic.sensor");
    assert_eq!(
        root.children[0].child,
        "99999999-8888-4777-8666-555555555555"
    );

    let rust = rust_bindings(root);
    assert!(rust.contains("pub trait MeterApi"), "{rust}");
    assert!(
        rust.contains("fn read(&mut self, unit: MeterUnit) -> Result<f64, MeterError>;"),
        "{rust}"
    );
    assert!(rust.contains("pub trait ProbeChildBackend"), "{rust}");

    let lua = lua_definitions(root);
    assert!(lua.contains("function meter.read(unit) end"), "{lua}");
    assert!(
        lua.contains("---@alias meter.Unit \"volts\"|\"amps\""),
        "{lua}"
    );

    let rules = graph.checker_rules(SENSOR_ID).expect("rules");
    assert_eq!(rules.modules["meter"].operations["read"].arity, 1);
    assert_eq!(
        rules.modules["meter"].enums["Unit"].variants["volts"],
        "volts"
    );
    assert_eq!(rules.modules["meter"].enums["Unit"].variants.len(), 2);
    assert_eq!(rules.modules["meter"].operations["read"].id, 1);
    assert_eq!(
        rules.children["probe"].child,
        "99999999-8888-4777-8666-555555555555"
    );
    assert!(rules.allowed_features.contains("call"));
    assert!(!rules.allowed_features.contains("table_constructor"));
}

#[test]
fn reordering_declarations_and_comments_keeps_the_digest() {
    let dir_a = tree("order-a");
    write(&dir_a, "probe/plugin.toml", PROBE);
    let a = write(&dir_a, "sensor/plugin.toml", &sensor("", METER_OPS));
    let digest_a = resolve(&dir_a, &a).expect("a").root().digest;

    let reordered = METER_OPS
        .replace(
            r#"[[modules.enums]]
name = "Unit"
id = 1
variants = [
  { name = "volts", id = 1, symbol = "volts" },
  { name = "amps", id = 2, symbol = "amps" },
]
"#,
            "",
        )
        .replace(
            "[[modules.operations]]",
            "# a comment\n[[modules.operations]]",
        )
        .to_string()
        + r#"
[[modules.enums]]
name = "Unit"
id = 1
variants = [
  { name = "amps", id = 2, symbol = "amps" },
  { name = "volts", id = 1, symbol = "volts" },
]
"#;
    let dir_b = tree("order-b");
    write(&dir_b, "probe/plugin.toml", PROBE);
    let b = write(&dir_b, "sensor/plugin.toml", &sensor("", &reordered));
    let digest_b = resolve(&dir_b, &b).expect("b").root().digest;
    assert_eq!(digest_a, digest_b);
}

#[test]
fn a_changed_signature_changes_the_digest() {
    let dir = tree("changed");
    write(&dir, "probe/plugin.toml", PROBE);
    let a = write(&dir, "sensor/plugin.toml", &sensor("", METER_OPS));
    let before = resolve(&dir, &a).expect("before").root().digest;
    let changed = METER_OPS.replace(
        r#"results = [{ name = "value", type = "float" }]"#,
        r#"results = [{ name = "value", type = "integer" }]"#,
    );
    write(&dir, "sensor/plugin.toml", &sensor("", &changed));
    let after = resolve(&dir, &a).expect("after").root().digest;
    assert_ne!(before, after);
}

#[test]
fn a_child_privilege_the_child_does_not_declare_is_rejected() {
    let dir = tree("child-privilege");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &sensor("", METER_OPS).replace(r#"requires = ["probe.run"]"#, r#"requires = ["nope"]"#),
    );
    assert_eq!(
        codes(resolve(&dir, &manifest)),
        vec![Code::UndeclaredPrivilege]
    );
}

#[test]
fn an_operation_requiring_an_undeclared_privilege_is_rejected() {
    let dir = tree("op-privilege");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &sensor(
            "",
            &METER_OPS.replace(r#"requires = ["sensor.read"]"#, r#"requires = ["ghost"]"#),
        ),
    );
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::UndeclaredPrivilege));
}

#[test]
fn an_import_outside_the_approved_root_is_rejected() {
    let dir = tree("escape");
    let inner = dir.join("inner");
    fs::create_dir_all(&inner).expect("inner");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(
        &inner,
        "sensor/plugin.toml",
        &sensor("", METER_OPS).replace("../probe/plugin.toml", "../../probe/plugin.toml"),
    );
    write(&inner, "probe/plugin.toml", PROBE);
    // The approved root is `inner`, so reaching `dir/probe` escapes it.
    let resolver = Resolver::new(&inner, dir.join("profiles")).expect("resolver");
    let result = resolver.resolve(&manifest);
    assert!(matches!(result, Err(d) if d.iter().any(|x| x.code == Code::PathEscapesRoot)));
}

#[test]
fn an_import_cycle_is_rejected() {
    let dir = tree("cycle");
    let probe = PROBE.replace(
        "[[modules]]",
        r#"[[imports]]
alias = "sensor"
path = "../sensor/plugin.toml"
contract_id = "11111111-2222-4333-8444-555555555555"
version = "=1.0.0"

[[modules]]"#,
    );
    write(&dir, "probe/plugin.toml", &probe);
    let manifest = write(&dir, "sensor/plugin.toml", &sensor("", METER_OPS));
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::ImportCycle));
}

#[test]
fn an_import_with_the_wrong_identity_is_rejected() {
    let dir = tree("identity");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &sensor("", METER_OPS).replace(r#"version = "=1.0.0""#, r#"version = "=2.0.0""#),
    );
    assert_eq!(
        codes(resolve(&dir, &manifest)),
        vec![Code::ImportIdentityMismatch]
    );
}

#[test]
fn an_unknown_child_alias_is_rejected() {
    let dir = tree("alias");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &sensor("", METER_OPS)
            .replace(r#"child_contract = "probe""#, r#"child_contract = "ghost""#),
    );
    assert_eq!(
        codes(resolve(&dir, &manifest)),
        vec![Code::UnknownChildContract]
    );
}

#[test]
fn duplicate_and_out_of_range_identifiers_are_rejected() {
    let dir = tree("ids");
    write(&dir, "probe/plugin.toml", PROBE);
    let duplicate = METER_OPS.replace(r#"name = "amps", id = 2"#, r#"name = "amps", id = 1"#);
    let manifest = write(&dir, "sensor/plugin.toml", &sensor("", &duplicate));
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::DuplicateId));

    let out_of_range = METER_OPS.replace(
        "id = 1\nexecution = \"sync\"",
        "id = 70000\nexecution = \"sync\"",
    );
    write(&dir, "sensor/plugin.toml", &sensor("", &out_of_range));
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::IdOutOfRange));
}

#[test]
fn unresolved_types_enums_errors_and_executions_are_rejected() {
    let dir = tree("refs");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(&dir, "sensor/plugin.toml", &sensor("", METER_OPS));
    let bad_enum = METER_OPS.replace(r#"ref = "Unit""#, r#"ref = "Missing""#);
    write(&dir, "sensor/plugin.toml", &sensor("", &bad_enum));
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::UnresolvedEnum));

    let bad_type = METER_OPS.replace(r#"type = "float""#, r#"type = "table""#);
    write(&dir, "sensor/plugin.toml", &sensor("", &bad_type));
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::UnknownType));

    let bad_execution = METER_OPS.replace(r#"execution = "sync""#, r#"execution = "later""#);
    write(&dir, "sensor/plugin.toml", &sensor("", &bad_execution));
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::UnknownExecution));

    let bad_error = METER_OPS.replace(
        "execution = \"sync\"\nparams",
        "execution = \"sync\"\nerrors = [\"Ghost\"]\nparams",
    );
    write(&dir, "sensor/plugin.toml", &sensor("", &bad_error));
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::UnknownError));
}

#[test]
fn an_unknown_key_is_rejected_so_a_typo_cannot_change_a_contract() {
    let dir = tree("typo");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &sensor("", METER_OPS).replace("max_spawn_depth = 2", "max_spawn_depht = 2"),
    );
    assert_eq!(codes(resolve(&dir, &manifest)), vec![Code::Parse]);
}

#[test]
fn a_missing_profile_is_reported() {
    let dir = tree("profile");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &sensor("", METER_OPS).replace(
            r#"language_profile = "synthetic-v1""#,
            r#"language_profile = "absent-v9""#,
        ),
    );
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::Parse));
}

#[test]
fn a_stale_lock_is_detected_and_a_fresh_lock_verifies() {
    let (dir, manifest) = valid_tree("lock");
    let graph = resolve(&dir, &manifest).expect("resolves");
    let lock_path = dir.join("plugin.lock");
    let lock = Lock::from_graphs(std::slice::from_ref(&graph), &lock_path).expect("lock");
    fs::write(&lock_path, lock.to_toml()).expect("write lock");
    let parsed =
        Lock::parse(&fs::read_to_string(&lock_path).expect("read"), &lock_path).expect("parse");
    assert!(
        parsed
            .verify(std::slice::from_ref(&graph), &lock_path)
            .is_ok()
    );

    let changed = METER_OPS.replace(
        r#"results = [{ name = "value", type = "float" }]"#,
        r#"results = [{ name = "value", type = "integer" }]"#,
    );
    write(&dir, "sensor/plugin.toml", &sensor("", &changed));
    let stale_graph = resolve(&dir, &manifest).expect("changed resolves");
    let diagnostics = parsed
        .verify(std::slice::from_ref(&stale_graph), &lock_path)
        .expect_err("stale");
    assert!(diagnostics.iter().all(|d| d.code == Code::StaleLock));
    assert!(!diagnostics.is_empty());
}

const SENSOR_ID: &str = "11111111-2222-4333-8444-555555555555";

/// A sensor whose local module does not reuse id 1, which the imported `clock` module already has.
fn sensor_beside_clock() -> String {
    sensor("", METER_OPS).replace("name = \"meter\"\nid = 1", "name = \"meter\"\nid = 2")
}

const CLOCK_IMPORT: &str = r#"
[[module_imports]]
alias = "probe"
module = "clock"
"#;

#[test]
fn a_module_imported_from_another_contract_is_exposed_and_rendered() {
    let dir = tree("module-import");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &format!("{}{CLOCK_IMPORT}", sensor_beside_clock()),
    );
    let graph = resolve(&dir, &manifest).expect("resolves");
    let root = graph.root();
    assert!(root.module("clock").is_some());
    assert!(rust_bindings(root).contains("pub trait ClockApi"));
    assert!(lua_definitions(root).contains("function clock.tick() end"));
    let rules = graph.checker_rules(SENSOR_ID).expect("rules");
    assert_eq!(rules.modules["clock"].operations["tick"].id, 1);
    assert!(
        rules.modules["clock"].operations["tick"]
            .requires
            .contains(&"probe.run".to_string())
    );
}

#[test]
fn a_module_import_needs_the_privileges_its_operations_require() {
    let dir = tree("module-privilege");
    write(&dir, "probe/plugin.toml", PROBE);
    let text = sensor_beside_clock().replace(
        r#"privileges = ["sensor.read", "probe.run"]"#,
        r#"privileges = ["sensor.read"]"#,
    );
    let manifest = write(&dir, "sensor/plugin.toml", &format!("{text}{CLOCK_IMPORT}"));
    // The child edge also delegates probe.run, so the same missing privilege is reported twice.
    let found = codes(resolve(&dir, &manifest));
    assert!(
        !found.is_empty() && found.iter().all(|code| *code == Code::UndeclaredPrivilege),
        "{found:?}"
    );
}

#[test]
fn an_artifact_slot_must_stay_free_of_running_executions() {
    let dir = tree("artifact-slots");
    write(&dir, "probe/plugin.toml", PROBE);
    let tight = format!(
        "{}{}",
        sensor("", METER_OPS),
        RESOURCES.replace("max_artifacts = 3", "max_artifacts = 2")
    );
    let manifest = write(&dir, "sensor/plugin.toml", &tight);
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::InvalidLimit));
}

#[test]
fn a_child_edge_may_delegate_only_privileges_the_parent_declares() {
    let dir = tree("delegation");
    write(&dir, "probe/plugin.toml", PROBE);
    let text = sensor("", METER_OPS).replace(
        r#"privileges = ["sensor.read", "probe.run"]"#,
        r#"privileges = ["sensor.read"]"#,
    );
    let manifest = write(&dir, "sensor/plugin.toml", &text);
    let found = codes(resolve(&dir, &manifest));
    assert!(found.contains(&Code::UndeclaredPrivilege), "{found:?}");
}

#[test]
fn a_module_import_names_a_real_alias_and_a_real_module() {
    let dir = tree("module-unknown");
    write(&dir, "probe/plugin.toml", PROBE);
    let base = sensor("", METER_OPS);
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &format!("{base}{}", CLOCK_IMPORT.replace("clock", "ghost")),
    );
    assert_eq!(codes(resolve(&dir, &manifest)), vec![Code::UnknownModule]);
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &format!(
            "{base}{}",
            CLOCK_IMPORT.replace(r#"alias = "probe""#, r#"alias = "nope""#)
        ),
    );
    assert_eq!(codes(resolve(&dir, &manifest)), vec![Code::UnknownImport]);
}

#[test]
fn a_module_import_cannot_reuse_a_name_or_an_identifier_of_the_contract() {
    let dir = tree("module-collide");
    write(&dir, "probe/plugin.toml", PROBE);
    // The local module `meter` has id 1, and the imported `clock` also has id 1.
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &format!("{}{CLOCK_IMPORT}", sensor("", METER_OPS)),
    );
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::DuplicateId));
    let twice = format!("{}{CLOCK_IMPORT}{CLOCK_IMPORT}", sensor("", METER_OPS));
    let manifest = write(&dir, "sensor/plugin.toml", &twice);
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::DuplicateName));
}

#[test]
fn an_imported_module_is_part_of_the_importer_digest() {
    let dir = tree("module-digest");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &format!("{}{CLOCK_IMPORT}", sensor_beside_clock()),
    );
    let before = resolve(&dir, &manifest).expect("before").root().digest;
    let changed = PROBE.replace(
        r#"results = [{ name = "count", type = "integer" }]"#,
        r#"results = [{ name = "count", type = "float" }]"#,
    );
    write(&dir, "probe/plugin.toml", &changed);
    let after = resolve(&dir, &manifest).expect("after").root().digest;
    assert_ne!(before, after);
}

const RESOURCES: &str = r#"
[resources]
max_artifact_bytes = 4096
max_chunk_bytes = 256
max_uploads = 2
max_sessions = 4
max_executions = 2
max_answer_bytes = 512
slice_fuel = 1000
total_fuel = 1000000
max_frames = 8
max_objects = 256
max_heap_bytes = 16384
session_idle_ms = 30000
max_artifacts = 3
retired_window = 64
max_child_tasks = 4
call_timeout_ms = 2000
cleanup_ms = 5000
max_request_bytes = 1024
"#;

#[test]
fn two_contracts_may_share_a_name_when_their_ids_differ() {
    let dir = tree("shared-name");
    write(
        &dir,
        "probe/plugin.toml",
        &PROBE.replace("synthetic.probe", "synthetic.sensor"),
    );
    let manifest = write(&dir, "sensor/plugin.toml", &sensor("", METER_OPS));
    let graph = resolve(&dir, &manifest).expect("both contracts resolve");
    assert_eq!(graph.contracts().count(), 2);
    let names: Vec<&str> = graph.contracts().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["synthetic.sensor", "synthetic.sensor"]);
}

#[test]
fn two_manifests_declaring_one_contract_id_are_rejected() {
    let dir = tree("dup-contract-id");
    write(&dir, "probe/plugin.toml", PROBE);
    write(&dir, "probe_copy/plugin.toml", PROBE);
    let text = format!(
        "{}\n[[imports]]\nalias = \"copy\"\npath = \"../probe_copy/plugin.toml\"\ncontract_id = \"99999999-8888-4777-8666-555555555555\"\nversion = \"=1.0.0\"\n",
        sensor("", METER_OPS)
    );
    let manifest = write(&dir, "sensor/plugin.toml", &text);
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::DuplicateId));
}

#[test]
fn a_child_index_used_twice_in_one_manifest_is_rejected() {
    let dir = tree("dup-index");
    write(&dir, "probe/plugin.toml", PROBE);
    let text = format!(
        "{}\n[[child_execution]]\nname = \"second\"\nid = 2\nindex = 1\nchild_contract = \"probe\"\nintrinsic = \"task.spawn.v1\"\nrequires = [\"probe.run\"]\nmax_capture_bytes = 64\nmax_result_bytes = 64\n",
        sensor("", METER_OPS)
    );
    let manifest = write(&dir, "sensor/plugin.toml", &text);
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::DuplicateId));
}

#[test]
fn resources_must_be_positive_and_chunks_must_fit_an_artifact() {
    let dir = tree("resources");
    write(&dir, "probe/plugin.toml", PROBE);
    let zero = format!(
        "{}{}",
        sensor("", METER_OPS),
        RESOURCES.replace("max_sessions = 4", "max_sessions = 0")
    );
    let manifest = write(&dir, "sensor/plugin.toml", &zero);
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::InvalidLimit));

    let oversized = format!(
        "{}{}",
        sensor("", METER_OPS),
        RESOURCES.replace("max_chunk_bytes = 256", "max_chunk_bytes = 8192")
    );
    write(&dir, "sensor/plugin.toml", &oversized);
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::InvalidLimit));
}

#[test]
fn resources_are_emitted_and_are_part_of_the_digest() {
    let dir = tree("resources-digest");
    write(&dir, "probe/plugin.toml", PROBE);
    let without = write(&dir, "sensor/plugin.toml", &sensor("", METER_OPS));
    let without_digest = resolve(&dir, &without).expect("resolves").root().digest;
    let with = write(
        &dir,
        "sensor/plugin.toml",
        &format!("{}{RESOURCES}", sensor("", METER_OPS)),
    );
    let graph = resolve(&dir, &with).expect("resolves");
    assert_ne!(without_digest, graph.root().digest);
    let rust = rust_bindings(graph.root());
    assert!(rust.contains("pub const MAX_SESSIONS: u32 = 4;"), "{rust}");
    assert!(
        rust.contains("pub const TOTAL_FUEL: u64 = 1000000;"),
        "{rust}"
    );
    assert!(rust.contains("pub const PROBE_INDEX: u8 = 1;"), "{rust}");
}

#[test]
fn resource_budgets_that_cannot_work_together_are_rejected() {
    let dir = tree("resource-coherence");
    write(&dir, "probe/plugin.toml", PROBE);
    for (from, to, why) in [
        (
            "max_answer_bytes = 512",
            "max_answer_bytes = 71",
            "a protocol answer must fit the replay bound",
        ),
        (
            "retired_window = 64",
            "retired_window = 3",
            "the window must cover the live sessions",
        ),
        (
            "max_request_bytes = 1024",
            "max_request_bytes = 300",
            "a full request must fit the request bound",
        ),
        (
            "max_child_tasks = 4",
            "max_child_tasks = 0",
            "a budget must be positive",
        ),
        (
            "cleanup_ms = 5000",
            "cleanup_ms = 0",
            "a deadline must be positive",
        ),
    ] {
        let text = format!("{}{}", sensor("", METER_OPS), RESOURCES.replace(from, to));
        let manifest = write(&dir, "sensor/plugin.toml", &text);
        assert!(
            codes(resolve(&dir, &manifest)).contains(&Code::InvalidLimit),
            "{why}"
        );
    }
    let required = protolink_lua_plugin_gen::min_request_bytes(
        &protolink_lua_plugin_gen::Limits {
            max_string_bytes: 64,
            max_tuple_items: 4,
            max_capture_bytes: 128,
            max_result_bytes: 128,
            max_spawn_depth: 2,
        },
        256,
    );
    let exact = format!(
        "{}{}",
        sensor("", METER_OPS),
        RESOURCES.replace(
            "max_request_bytes = 1024",
            &format!("max_request_bytes = {required}")
        )
    );
    let manifest = write(&dir, "sensor/plugin.toml", &exact);
    assert!(
        resolve(&dir, &manifest).is_ok(),
        "the exact bound is accepted"
    );
    let under = format!(
        "{}{}",
        sensor("", METER_OPS),
        RESOURCES.replace(
            "max_request_bytes = 1024",
            &format!("max_request_bytes = {}", required - 1)
        )
    );
    write(&dir, "sensor/plugin.toml", &under);
    assert!(
        codes(resolve(&dir, &manifest)).contains(&Code::InvalidLimit),
        "one byte under is refused"
    );
}

const SINGLE_VALUE_PROFILE: &str = r#"
schema_version = 1
name = "synthetic-v1"
version = "1.0.0"
[features]
allowed = ["local_assignment", "call", "return", "arithmetic"]
[native_abi]
name = "single-value-v1"
max_results = 1
"#;

const TWO_RESULTS: &str = r#"
[[modules.operations]]
name = "pair"
id = 1
execution = "sync"
results = [{ name = "low", type = "integer" }, { name = "high", type = "integer" }]
requires = ["sensor.read"]
"#;

#[test]
fn a_multi_result_signature_is_truthful_and_a_single_value_abi_refuses_it() {
    let dir = tree("abi");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(&dir, "sensor/plugin.toml", &sensor("", TWO_RESULTS));
    let graph = resolve(&dir, &manifest).expect("a generic profile accepts any result count");
    let rust = rust_bindings(graph.root());
    assert!(
        rust.contains("fn pair(&mut self) -> Result<(i64, i64), MeterError>;"),
        "{rust}"
    );
    let rules = graph.checker_rules(&graph.root().id).expect("rules");
    let pair = &rules.modules["meter"].operations["pair"];
    assert_eq!(pair.results.len(), 2, "the rules keep the full signature");

    fs::write(dir.join("profiles/synthetic-v1.toml"), SINGLE_VALUE_PROFILE).expect("profile");
    assert!(codes(resolve(&dir, &manifest)).contains(&Code::UnsupportedSignature));
}

#[test]
fn profile_formatting_keeps_the_digest_and_a_policy_change_does_not() {
    let (dir, manifest) = valid_tree("profile-identity");
    let original = resolve(&dir, &manifest).expect("resolves").root().digest;

    let reformatted = format!(
        "# a comment\n{}",
        PROFILE.replace(
            r#"allowed = ["local_assignment", "call", "return", "arithmetic"]"#,
            "allowed = [\n  \"return\",\n  \"call\",\n  \"arithmetic\",\n  \"local_assignment\",\n]"
        )
    );
    fs::write(dir.join("profiles/synthetic-v1.toml"), &reformatted).expect("profile");
    let graph = resolve(&dir, &manifest).expect("resolves");
    assert_eq!(graph.root().digest, original, "formatting is not semantics");
    assert_ne!(
        graph.profile("synthetic-v1").expect("profile").sha256,
        [0; 32],
        "the file checksum is still kept for provenance"
    );

    let narrower = PROFILE.replace(r#", "arithmetic""#, "");
    fs::write(dir.join("profiles/synthetic-v1.toml"), narrower).expect("profile");
    assert_ne!(
        resolve(&dir, &manifest).expect("resolves").root().digest,
        original,
        "a feature removal is a new contract"
    );

    let with_abi = PROFILE.replace(
        "[features]",
        "[native_abi]\nname = \"single-value-v1\"\nmax_results = 1\n[features]",
    );
    fs::write(dir.join("profiles/synthetic-v1.toml"), with_abi).expect("profile");
    assert_ne!(
        resolve(&dir, &manifest).expect("resolves").root().digest,
        original,
        "an ABI change is a new contract"
    );
}

#[test]
fn checker_rules_carry_the_limits_the_envelope_and_the_profile_identity() {
    let dir = tree("rules-envelope");
    write(&dir, "probe/plugin.toml", PROBE);
    let manifest = write(
        &dir,
        "sensor/plugin.toml",
        &format!("{}{RESOURCES}", sensor("", METER_OPS)),
    );
    let graph = resolve(&dir, &manifest).expect("resolves");
    let root = graph.root();
    let rules = graph.checker_rules(&root.id).expect("rules");
    assert_eq!(rules.limits.max_spawn_depth, 2);
    assert_eq!(rules.resources.expect("envelope").retired_window, 64);
    assert_eq!(rules.profile_fingerprint, root.profile_fingerprint);
    assert_eq!(rules.profile_version, "1.0.0");
    assert_eq!(rules.children["probe"].slot_id, 1);
    let read = &rules.modules["meter"].operations["read"];
    assert_eq!(read.params.len(), 1);
    assert_eq!(read.results, vec![protolink_lua_plugin_gen::Type::Float]);
}
