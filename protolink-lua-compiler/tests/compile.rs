//! End-to-end compiler tests against the consumer BSP graph (host → agent → DUT).

use std::path::Path;

use protolink_lua::value::{Limits as WireLimits, Value};
use protolink_lua_bytecode::{Artifact, Instr, Limits};
use protolink_lua_compiler::{Diagnostic, Workflow, code, compile};
use protolink_lua_plugin_gen::{Graph, Resolver};
use protolink_lua_vm::{EffectResult, Machine, Program, Step, VmLimits};

const HOST: &str = "1b0a8e2c-7d4f-4e6a-9c1b-5a2f3e4d6c70";

fn graph() -> Graph {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bsp");
    Resolver::new(&root, root.join("profiles"))
        .expect("resolver")
        .resolve(root.join("host/bsp.toml"))
        .expect("bsp resolves")
}

fn ok(source: &str) -> Workflow {
    compile(source, &graph(), HOST).unwrap_or_else(|d| panic!("expected success, got {d:?}"))
}

fn err(source: &str) -> Vec<Diagnostic> {
    match compile(source, &graph(), HOST) {
        Ok(_) => panic!("expected diagnostics"),
        Err(diagnostics) => diagnostics,
    }
}

fn codes(diagnostics: &[Diagnostic]) -> Vec<&'static str> {
    diagnostics.iter().map(|d| d.code).collect()
}

fn decode(bytes: &[u8]) -> Artifact {
    Artifact::decode_verified(bytes, &Limits::profile()).expect("artifact decodes and verifies")
}

fn run(artifact: Artifact) -> Step {
    let program = Program::verified(artifact, &Limits::profile()).expect("verifies");
    let limits = VmLimits {
        max_frames: 16,
        max_objects: 1024,
        max_bytes: 65536,
        wire: WireLimits::new(8, 64, 256),
        total_fuel: 100_000,
    };
    let mut machine = Machine::start(program, 0, &[], &[], limits).expect("starts");
    loop {
        match machine.run(10_000) {
            Step::Exhausted => continue,
            other => return other,
        }
    }
}

fn returned_values(step: Step) -> Vec<Value> {
    match step {
        Step::Returned(tuple) => tuple.values().to_vec(),
        other => panic!("expected a return, got {other:?}"),
    }
}

/// The agent body of a workflow, which is entry 0 of site 0.
fn agent_artifact(workflow: &Workflow) -> Artifact {
    decode(&workflow.sites[0].artifact)
}

const WORKFLOW: &str = r#"local count = 4
local t = { a = 1 }
local agent_task = remote:spawn(function()
    local value = remote:spawn(function()
        local enabled = chip.model()
        return enabled, count, "sample"
    end)
    local a, b, c = value:await()
    return a, b, c
end)
local x, y, z = agent_task:await()
return x, y, z
"#;

#[test]
fn a_workflow_with_nested_placements_compiles_and_keeps_its_line_numbers() {
    let workflow = ok(WORKFLOW);
    assert_eq!(workflow.sites.len(), 1);
    assert_eq!(
        workflow.sites[0].contract,
        "2bdbdeef-600a-46de-82b5-1936195c2d11"
    );
    assert_eq!(
        workflow.sites[0].captures,
        vec!["count".to_string()],
        "the agent captures count for its child"
    );
    assert_eq!(
        workflow.host_source.lines().count(),
        WORKFLOW.lines().count()
    );
    assert!(
        workflow
            .host_source
            .contains("__lua_remote_register(0, function()"),
        "{}",
        workflow.host_source
    );
    assert!(
        workflow
            .host_source
            .contains("function() return count end)"),
        "{}",
        workflow.host_source
    );

    let agent = agent_artifact(&workflow);
    assert_eq!(
        agent.children.len(),
        1,
        "the agent forwards one compiled child"
    );
    let dut = decode(&agent.children[0].bytes);
    assert_eq!(dut.natives.len(), 1);
}

#[test]
fn host_only_lua_outside_placements_is_left_alone() {
    let source = "local t = { 1, 2, 3 }\nfor _, v in ipairs(t) do end\nreturn t\n";
    let workflow = ok(source);
    assert!(workflow.sites.is_empty());
    assert_eq!(workflow.host_source, source);
}

#[test]
fn a_shadowed_child_name_is_an_ordinary_host_call_and_is_not_compiled() {
    let source = "local remote = { spawn = function(self, f) return f end }\nlocal f = remote:spawn(function() local x = {} return x end)\n";
    let workflow = ok(source);
    assert!(
        workflow.sites.is_empty(),
        "shadowed name is host Lua, so no placement and no remote check"
    );
}

#[test]
fn a_remote_construct_the_profile_does_not_allow_is_reported_with_its_position() {
    let source = "local t = remote:spawn(function()\n    local x = {}\n    return x\nend)\n";
    let diagnostics = err(source);
    assert_eq!(codes(&diagnostics), vec![code::UNSUPPORTED]);
    assert_eq!((diagnostics[0].line, diagnostics[0].column), (2, 15));
    assert!(
        diagnostics[0].message.contains("table_constructor"),
        "{}",
        diagnostics[0].message
    );
}

#[test]
fn unknown_names_and_capabilities_used_as_values_are_rejected() {
    let unknown = err("remote:spawn(function() return missing end)\n");
    assert_eq!(codes(&unknown), vec![code::UNKNOWN_NAME]);

    let capability = err("remote:spawn(function() return chip end)\n");
    assert_eq!(codes(&capability), vec![code::CAPABILITY_VALUE]);
}

#[test]
fn native_arity_and_undeclared_operations_are_checked() {
    let arity =
        err("remote:spawn(function() remote:spawn(function() return chip.model(1) end) end)\n");
    assert_eq!(codes(&arity), vec![code::ARITY]);

    let undeclared = err("remote:spawn(function() return nope.op() end)\n");
    assert_eq!(codes(&undeclared), vec![code::UNDECLARED]);
}

#[test]
fn a_spawn_body_must_be_a_literal_and_a_break_must_be_in_a_loop() {
    let dynamic = err("local f = function() end\nremote:spawn(f)\n");
    assert_eq!(codes(&dynamic), vec![code::SPAWN_BODY]);

    let stray_break = err("remote:spawn(function() break end)\n");
    assert_eq!(codes(&stray_break), vec![code::BREAK]);
}

#[test]
fn a_parse_error_is_reported_with_a_line() {
    let diagnostics = err("local x = \n local = 3\n");
    assert!(
        !diagnostics.is_empty() && diagnostics.iter().all(|d| d.code == code::PARSE),
        "{diagnostics:?}"
    );
    assert!(
        diagnostics.iter().all(|d| d.line >= 1 && d.column >= 1),
        "every parse error has a position"
    );
}

#[test]
fn compiled_arithmetic_and_multiple_returns_run_on_the_vm() {
    let workflow = ok("remote:spawn(function() return 1 + 2 * 3, \"x\" end)\n");
    let values = returned_values(run(agent_artifact(&workflow)));
    assert_eq!(values, vec![Value::Integer(7), Value::Bytes(b"x".to_vec())]);
}

#[test]
fn a_local_closure_shares_a_cell_when_it_runs() {
    let workflow = ok(
        "remote:spawn(function()\n    local n = 1\n    local function inc() n = n + 1 return n end\n    return inc(), inc()\nend)\n",
    );
    assert_eq!(
        returned_values(run(agent_artifact(&workflow))),
        vec![Value::Integer(2), Value::Integer(3)]
    );
}

#[test]
fn numeric_for_while_and_if_run_on_the_vm() {
    let workflow = ok(
        "remote:spawn(function()\n    local s = 0\n    for i = 1, 4 do s = s + i end\n    local w = 0\n    while w < 3 do w = w + 1 end\n    if s == 10 then return s, w else return 0, 0 end\nend)\n",
    );
    assert_eq!(
        returned_values(run(agent_artifact(&workflow))),
        vec![Value::Integer(10), Value::Integer(3)]
    );
}

#[test]
fn a_child_await_suspends_on_the_vm_and_resumes_with_the_reply() {
    let workflow = ok(
        "remote:spawn(function()\n    local v = remote:spawn(function() return 9 end)\n    local a = v:await()\n    return a\nend)\n",
    );
    let agent = agent_artifact(&workflow);
    let program = Program::verified(agent, &Limits::profile()).expect("verifies");
    let limits = VmLimits {
        max_frames: 8,
        max_objects: 256,
        max_bytes: 4096,
        wire: WireLimits::new(8, 64, 128),
        total_fuel: 10_000,
    };
    let mut machine = Machine::start(program, 0, &[], &[], limits).expect("starts");
    assert!(
        matches!(machine.run(10_000), Step::Pending(_)),
        "the spawn is an effect"
    );
    machine
        .complete(EffectResult::Task(1))
        .expect("spawn completes");
    assert!(
        matches!(machine.run(10_000), Step::Pending(_)),
        "the await is an effect"
    );
    machine
        .complete(EffectResult::Tuple(
            protolink_lua::value::Tuple::new(vec![Value::Integer(9)], WireLimits::new(8, 64, 128))
                .expect("tuple"),
        ))
        .expect("await completes");
    assert_eq!(
        returned_values(loop_run(&mut machine)),
        vec![Value::Integer(9)]
    );
}

fn loop_run(machine: &mut Machine) -> Step {
    loop {
        match machine.run(10_000) {
            Step::Exhausted => continue,
            other => return other,
        }
    }
}

#[test]
fn the_artifact_has_a_spawn_instruction_for_each_nested_placement() {
    let workflow = ok(WORKFLOW);
    let agent = agent_artifact(&workflow);
    let spawns = agent.protos[0]
        .code
        .iter()
        .filter(|i| matches!(i, Instr::Spawn { .. }))
        .count();
    assert_eq!(spawns, 1);
}

const DUT_ID: &str = "78240354-a109-42a2-9b91-e0497fbd55d0";

#[test]
fn each_artifact_carries_its_destination_digest_and_the_workflow_has_provenance() {
    let graph = graph();
    let workflow = ok(WORKFLOW);
    let site = &workflow.sites[0];
    let destination = graph
        .contract(&site.contract)
        .expect("destination contract");
    assert_eq!(decode(&site.artifact).contract, destination.digest);
    assert_eq!(site.digest, protolink_lua_bytecode::digest(&site.artifact));

    let agent = agent_artifact(&workflow);
    let dut = graph.contract(DUT_ID).expect("dut contract");
    assert_eq!(decode(&agent.children[0].bytes).contract, dut.digest);

    assert_eq!(workflow.provenance.host_contract, HOST);
    assert!(workflow.provenance.compiler.starts_with("lua_compiler "));
}
