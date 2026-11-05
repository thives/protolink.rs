//! Regressions for the readiness remediation (compiler findings F06–F15 and the Lua semantics findings F09–F12).
//!
//! Each test names the behaviour it pins. The synthetic trees are written to a temporary directory, so a profile or an
//! envelope can be changed without touching the BSP fixture.

use std::fs;
use std::path::{Path, PathBuf};

use protolink_lua::value::{Limits as WireLimits, Value};
use protolink_lua_bytecode::{AdmitError, Artifact, Limits, admit};
use protolink_lua_compiler::{Diagnostic, Workflow, code, compile};
use protolink_lua_plugin_gen::{Graph, Resolver};
use protolink_lua_vm::{EffectResult, Machine, Program, Step, VmLimits};

const HOST: &str = "1b0a8e2c-7d4f-4e6a-9c1b-5a2f3e4d6c70";

fn bsp() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bsp")
}

fn bsp_graph() -> Graph {
    let root = bsp();
    Resolver::new(&root, root.join("profiles"))
        .expect("resolver")
        .resolve(root.join("host/bsp.toml"))
        .expect("bsp resolves")
}

fn ok_in(graph: &Graph, host: &str, source: &str) -> Workflow {
    compile(source, graph, host).unwrap_or_else(|d| panic!("expected success, got {d:?}"))
}

fn err_in(graph: &Graph, host: &str, source: &str) -> Vec<Diagnostic> {
    match compile(source, graph, host) {
        Ok(_) => panic!("expected diagnostics for:\n{source}"),
        Err(diagnostics) => diagnostics,
    }
}

fn ok(source: &str) -> Workflow {
    ok_in(&bsp_graph(), HOST, source)
}

fn err(source: &str) -> Vec<Diagnostic> {
    err_in(&bsp_graph(), HOST, source)
}

fn codes(diagnostics: &[Diagnostic]) -> Vec<&'static str> {
    diagnostics.iter().map(|d| d.code).collect()
}

/// A copy of the BSP fixture in a temporary directory, with `edit` applied to the profile text.
fn bsp_copy(name: &str, edit: impl Fn(&str) -> String) -> Graph {
    let dir = std::env::temp_dir().join(format!("lua-compiler-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    for rel in [
        "host/bsp.toml",
        "agent/bsp.toml",
        "dut/bsp.toml",
        "base-esp32c6/bsp.toml",
        "base-esp32/bsp.toml",
        "ota/bsp.toml",
    ] {
        let target = dir.join(rel);
        fs::create_dir_all(target.parent().expect("parent")).expect("dirs");
        fs::copy(bsp().join(rel), &target).expect("copy manifest");
    }
    fs::create_dir_all(dir.join("profiles")).expect("profiles");
    let profile =
        fs::read_to_string(bsp().join("profiles/lua54-bounded-v1.toml")).expect("profile");
    fs::write(dir.join("profiles/lua54-bounded-v1.toml"), edit(&profile)).expect("write");
    Resolver::new(&dir, dir.join("profiles"))
        .expect("resolver")
        .resolve(dir.join("host/bsp.toml"))
        .expect("copy resolves")
}

fn without_feature(feature: &str) -> Graph {
    let quoted = format!("    \"{feature}\",\n");
    bsp_copy(&format!("no-{feature}"), |profile| {
        assert!(profile.contains(&quoted), "the profile allows {feature}");
        profile.replace(&quoted, "")
    })
}

fn run_artifact(bytes: &[u8]) -> Step {
    let artifact = Artifact::decode_verified(bytes, &Limits::profile()).expect("verifies");
    let program = Program::verified(artifact, &Limits::profile()).expect("verifies");
    let limits = VmLimits {
        max_frames: 16,
        max_objects: 1024,
        max_bytes: 65536,
        wire: WireLimits::new(8, 64, 256),
        total_fuel: 1_000_000,
    };
    let mut machine = Machine::start(program, 0, &[], &[], limits).expect("starts");
    loop {
        match machine.run(10_000) {
            Step::Exhausted => continue,
            other => return other,
        }
    }
}

/// Compiles `body` as the agent region of a workflow and runs it on the VM. The body must not spawn or call natives.
fn agent_returns(body: &str) -> Vec<Value> {
    let source = format!("return remote:spawn(function()\n{body}\nend)\n");
    let workflow = ok(&source);
    match run_artifact(&workflow.sites[0].artifact) {
        Step::Returned(tuple) => tuple.values().to_vec(),
        other => panic!("expected a return from:\n{body}\ngot {other:?}"),
    }
}

fn agent_fails(body: &str) -> String {
    let source = format!("return remote:spawn(function()\n{body}\nend)\n");
    let workflow = ok(&source);
    match run_artifact(&workflow.sites[0].artifact) {
        Step::Failed { message, .. } => String::from_utf8_lossy(&message).into_owned(),
        other => panic!("expected a failure from:\n{body}\ngot {other:?}"),
    }
}

fn int(i: i64) -> Value {
    Value::Integer(i)
}

fn bytes(text: &str) -> Value {
    Value::Bytes(text.as_bytes().to_vec())
}

// -- F08: capability aliases and escapes -------------------------------------------------------------------------------

#[test]
fn aliased_and_parenthesized_placements_are_preflighted_before_any_host_effect() {
    for source in [
        "local r = remote\nlocal t = r:spawn(function() local x = {} end)\n",
        "local t = (remote):spawn(function() local x = {} end)\n",
        "local r = remote\nlocal r2 = (r)\nlocal t = r2:spawn(function() local x = {} end)\n",
        "local r = remote\nlocal function later() return r:spawn(function() local x = {} end) end\n",
    ] {
        let diagnostics = err(source);
        assert_eq!(codes(&diagnostics), vec![code::UNSUPPORTED], "{source}");
        assert!(diagnostics[0].message.contains("table_constructor"));
    }
    let workflow =
        ok("local r = remote\nlocal t = (r):spawn(function() return 1 end)\nreturn t:await()\n");
    assert_eq!(workflow.sites.len(), 1);
    assert_eq!(workflow.sites[0].slot, "remote");
    assert_eq!(workflow.sites[0].slot_id, 1);
}

#[test]
fn every_other_use_of_a_child_slot_is_refused() {
    for source in [
        "print(remote)\n",
        "local t = { remote }\n",
        "remote = nil\n",
        "local r = remote\nr = 1\n",
        "remote:cancel()\n",
        "local function f() return remote end\n",
        "local x = remote.spawn\n",
        "function remote.extra() end\n",
        "local r = remote\nlocal ok = pcall(function() return r end)\n",
        "if remote then end\n",
    ] {
        let diagnostics = err(source);
        assert!(
            codes(&diagnostics).contains(&code::CAPABILITY_ESCAPE),
            "{source}: {diagnostics:?}"
        );
    }
}

#[test]
fn ordinary_objects_and_shadowed_names_stay_host_lua() {
    for source in [
        "local obj = { spawn = function(self, f) return f end }\nobj:spawn(function() local t = {} return t end)\n",
        "local remote = 5\nprint(remote)\n",
        "for remote = 1, 2 do print(remote) end\n",
        "for _, remote in ipairs({}) do print(remote) end\n",
        "local function f(remote) return remote end\n",
        "local r = remote\nlocal r = 7\nprint(r)\n",
    ] {
        let workflow = ok(source);
        assert!(workflow.sites.is_empty(), "{source}");
        assert_eq!(workflow.host_source, source);
    }
}

#[test]
fn repeat_condition_retains_its_body_scope() {
    for source in [
        "repeat local r = remote until r:spawn(function() local x = {} end)",
        "repeat local r = remote local s = (r) until s:spawn(function() local x = {} end)",
        "local r = remote repeat local s = r repeat local t = s until t:spawn(function() local x = {} end) until true",
        "local r = remote repeat local r = {} until true return r:spawn(function() local x = {} end)",
    ] {
        let diagnostics = err(source);
        assert!(
            codes(&diagnostics).contains(&code::UNSUPPORTED),
            "{source}: {diagnostics:?}"
        );
    }
    for source in [
        "repeat local remote = { spawn = function(self, f) return f end } until remote:spawn(function() local x = {} end)",
        "repeat local r = { spawn = function(self, f) return f end } repeat local r = 1 until true until r:spawn(function() local x = {} end)",
    ] {
        assert!(ok(source).sites.is_empty(), "{source}");
    }
}

#[test]
fn remote_await_rejects_all_explicit_arguments() {
    for expression in [
        "t:await({})",
        "t:await(error('must run'))",
        "t:await 'text'",
        "t:await{}",
        "remote:spawn(function() end):await(1)",
        "remote:spawn(function() end):await 'text'",
    ] {
        for context in ["return {e}", "local x = {e}", "return ({e})", "{e}"] {
            let statement = context.replace("{e}", expression);
            let source = format!(
                "return remote:spawn(function() local t = remote:spawn(function() end) {statement} end)"
            );
            let diagnostics = err(&source);
            assert!(
                diagnostics
                    .iter()
                    .any(|d| d.code == code::ARITY || d.code == code::UNSUPPORTED),
                "{statement}: {diagnostics:?}"
            );
        }
    }
    let source = "local t = { await = function(self, x) return x end } return t:await(1)";
    assert_eq!(ok(source).host_source, source);
}

#[test]

fn spawn_snapshots_after_dynamic_argument_effects() {
    use protolink_lua_vm::Effect;
    for (args, expected) in [
        ("change()", vec![int(1), int(2)]),
        ("(change())", vec![int(1)]),
    ] {
        let source = format!(
            "return remote:spawn(function() local n = 0 local function change() n = n + 1 return n, 2 end return remote:spawn(function() return n end, {args}) end)"
        );
        let workflow = ok(&source);
        match run_artifact(&workflow.sites[0].artifact) {
            Step::Pending(Effect::Spawn {
                captures,
                arguments,
                ..
            }) => {
                assert_eq!(captures, vec![int(1)], "{args}");
                assert_eq!(arguments, expected, "{args}");
            }
            other => panic!("expected spawn: {other:?}"),
        }
    }
}

#[test]
fn dynamic_spawn_preserves_order_nil_arity_and_destination_admission() {
    use protolink_lua_vm::Effect;
    let graph = bsp_graph();
    for (returns, expected) in [
        ("", vec![int(1)]),
        (
            "return nil, 3, nil",
            vec![int(1), Value::Nil, int(3), Value::Nil],
        ),
    ] {
        let source = format!(
            "return remote:spawn(function() local n = 0 local function first() n = n + 1 return n end local function last() n = n + 1 {returns} end return remote:spawn(function() return n end, first(), last()) end)"
        );
        let workflow = ok_in(&graph, HOST, &source);
        let agent = graph.contract(&workflow.sites[0].contract).unwrap();
        let agent_limits = Limits::for_destination(
            agent.resources.unwrap().max_artifact_bytes as usize,
            agent.limits.max_spawn_depth,
        );
        let artifact = admit(
            &workflow.sites[0].artifact,
            &agent_limits,
            &agent.digest,
            &agent.native_operations(),
        )
        .expect("agent admission");
        let dut = graph
            .contract("78240354-a109-42a2-9b91-e0497fbd55d0")
            .unwrap();
        let dut_limits = Limits::for_destination(
            dut.resources.unwrap().max_artifact_bytes as usize,
            dut.limits.max_spawn_depth,
        );
        admit(
            &artifact.children[0].bytes,
            &dut_limits,
            &dut.digest,
            &dut.native_operations(),
        )
        .expect("child admission");
        match run_artifact(&workflow.sites[0].artifact) {
            Step::Pending(Effect::Spawn {
                captures,
                arguments,
                ..
            }) => {
                assert_eq!(captures, vec![int(2)]);
                assert_eq!(arguments, expected);
            }
            other => panic!("expected spawn: {other:?}"),
        }
    }
    let source = "return remote:spawn(function() local n = 0 local function fail() n = 1 error('before spawn') end return remote:spawn(function() return n end, fail()) end)";
    assert!(matches!(
        run_artifact(&ok(source).sites[0].artifact),
        Step::Failed { .. }
    ));
}

#[test]
fn dynamic_spawn_snapshots_multiple_captures_after_every_argument() {
    use protolink_lua_vm::Effect;
    let source = "return remote:spawn(function() local a, b = 0, 0 local function first() a = 1 return 7 end local function last() b = a + 1 return 8, nil end return remote:spawn(function() return a, b end, first(), last()) end)";
    match run_artifact(&ok(source).sites[0].artifact) {
        Step::Pending(Effect::Spawn {
            captures,
            arguments,
            ..
        }) => {
            assert_eq!(captures, vec![int(1), int(2)]);
            assert_eq!(arguments, vec![int(7), int(8), Value::Nil]);
        }
        other => panic!("expected spawn: {other:?}"),
    }
}

#[test]
fn dynamic_spawn_temporary_respects_local_and_wire_limits() {
    let max = usize::from(Limits::profile().max_locals);
    let source = |extra: usize| {
        let locals: String = (0..extra).map(|i| format!("local x{i} = 0\n")).collect();
        format!(
            "return remote:spawn(function() local n = 1 local function args() return n end {locals} return remote:spawn(function() return n end, args()) end)"
        )
    };
    ok(&source(max - 3));
    let diagnostics = err(&source(max - 2));
    assert!(
        diagnostics
            .iter()
            .any(|d| d.code == code::LIMIT && d.message.contains("local slots")),
        "{diagnostics:?}"
    );

    let source = "return remote:spawn(function() local n = 0 local function args() n = 1 return 1,2,3,4,5,6,7,8 end return remote:spawn(function() return n end, 0, args()) end)";
    assert!(matches!(
        run_artifact(&ok(source).sites[0].artifact),
        Step::Failed { .. }
    ));
}

#[test]
fn dynamic_spawn_without_captures_remains_supported() {
    use protolink_lua_vm::Effect;
    for (returns, expected) in [("", vec![]), ("return 1, nil", vec![int(1), Value::Nil])] {
        let source = format!(
            "return remote:spawn(function() local function args() {returns} end return remote:spawn(function() return 1 end, args()) end)"
        );
        let workflow = ok(&source);
        match run_artifact(&workflow.sites[0].artifact) {
            Step::Pending(Effect::Spawn {
                captures,
                arguments,
                ..
            }) => {
                assert!(captures.is_empty());
                assert_eq!(arguments, expected);
            }
            other => panic!("expected spawn: {other:?}"),
        }
    }
}

#[test]
fn fixed_spawn_arguments_are_evaluated_before_capture_snapshot() {
    use protolink_lua_vm::Effect;
    let source = "return remote:spawn(function() local n = 0 local function change() n = n + 1 return n end return remote:spawn(function() return n end, change(), (change())) end)";
    let workflow = ok(source);
    match run_artifact(&workflow.sites[0].artifact) {
        Step::Pending(Effect::Spawn {
            captures,
            arguments,
            ..
        }) => {
            assert_eq!(captures, vec![int(2)]);
            assert_eq!(arguments, vec![int(1), int(2)]);
        }
        other => panic!("expected spawn: {other:?}"),
    }
}

// -- F15: one feature check for every intrinsic, in every syntactic context -------------------------------------------

#[test]
fn tail_and_non_tail_await_are_both_refused_when_the_profile_disallows_it() {
    let graph = without_feature("method_call_await");
    for context in [
        "return t:await()",
        "local v = t:await()",
        "t:await()",
        "return 1, t:await()",
        "return g(t:await())",
        "if t:await() then end",
        "return remote:spawn(function() return 1 end):await()",
    ] {
        let source = format!(
            "return remote:spawn(function()\n    local function g(a) return a end\n    local t = remote:spawn(function() return 1 end)\n    {context}\nend)\n"
        );
        let diagnostics = err_in(&graph, HOST, &source);
        assert_eq!(codes(&diagnostics), vec![code::UNSUPPORTED], "{context}");
        assert!(
            diagnostics[0].message.contains("method_call_await"),
            "{context}: {}",
            diagnostics[0].message
        );
    }
}

#[test]
fn every_intrinsic_is_checked_in_every_syntactic_context() {
    let intrinsics = [
        ("call", "f()"),
        ("pcall", "pcall(f)"),
        ("coroutine_yield", "coroutine.yield(1)"),
        ("error_call", "error(\"x\")"),
        ("native_call", "chip.model()"),
        ("spawn", "remote:spawn(function() return 1 end)"),
    ];
    let contexts = [
        "{e}",
        "local v = {e}",
        "return {e}",
        "return {e}, 1",
        "local w = g({e})",
        "if {e} then end",
        "local u = 1 + g(g({e}))",
    ];
    for (feature, expression) in intrinsics {
        let graph = without_feature(feature);
        for context in contexts {
            let statement = context.replace("{e}", expression);
            let source = format!(
                "return remote:spawn(function()\n    local function f() return 1 end\n    local function g(a) return a end\n    {statement}\nend)\n"
            );
            let diagnostics = err_in(&graph, HOST, &source);
            assert!(
                diagnostics
                    .iter()
                    .any(|d| d.code == code::UNSUPPORTED && d.message.contains(feature)),
                "{feature} in `{statement}`: {diagnostics:?}"
            );
        }
    }
}

// -- F09: Lua result adjustment ----------------------------------------------------------------------------------------

#[test]
fn a_trailing_call_expands_and_parentheses_truncate() {
    let f = "local function f() return 1, 2 end\n";
    assert_eq!(
        agent_returns(&format!("{f}return 0, f()")),
        vec![int(0), int(1), int(2)]
    );
    assert_eq!(agent_returns(&format!("{f}return (f())")), vec![int(1)]);
    assert_eq!(
        agent_returns(&format!("{f}return f(), 10")),
        vec![int(1), int(10)]
    );
    assert_eq!(
        agent_returns(&format!(
            "{f}local function g(a, b, c) return c, b, a end\nreturn g(f())"
        )),
        vec![Value::Nil, int(2), int(1)]
    );
    assert_eq!(
        agent_returns(&format!(
            "{f}local function id(a, b) return a, b end\nreturn id(9, f())"
        )),
        vec![int(9), int(1)]
    );
    assert_eq!(
        agent_returns(&format!("{f}local a, b, c = f()\nreturn a, b, c")),
        vec![int(1), int(2), Value::Nil]
    );
    assert_eq!(
        agent_returns("local function f() end\nreturn f()"),
        Vec::<Value>::new()
    );
    assert_eq!(
        agent_returns("local function f() return nil, nil end\nreturn f()"),
        vec![Value::Nil, Value::Nil],
        "trailing nils keep their arity"
    );
}

#[test]

fn pcall_expands_a_factory_into_callable_and_arguments() {
    assert_eq!(
        agent_returns(
            "local function f(a, b) return a, b end local function factory() return f, 11, 22 end return pcall(factory())"
        ),
        vec![Value::Boolean(true), int(11), int(22)]
    );
}

#[test]
fn pcall_results_are_not_truncated() {
    assert_eq!(
        agent_returns("local function f(a) return a, a end\nreturn pcall(f, 3)"),
        vec![Value::Boolean(true), int(3), int(3)]
    );
    assert_eq!(
        agent_returns("local function f() error(\"boom\") end\nreturn pcall(f)"),
        vec![Value::Boolean(false), bytes("boom")]
    );
    assert_eq!(
        agent_returns(
            "local function f() return 1, 2 end\nlocal function g(a, b) return b, a end\nreturn pcall(g, f())"
        ),
        vec![Value::Boolean(true), int(2), int(1)]
    );
    assert_eq!(
        agent_returns("local function f() end\nreturn 7, pcall(f)"),
        vec![int(7), Value::Boolean(true)]
    );
}

#[test]
fn every_expression_is_evaluated_once_and_in_order() {
    assert_eq!(
        agent_returns(
            "local n = 0\nlocal function inc() n = n + 1 return n end\nlocal function three(a, b, c) return a, b, c end\nreturn three(inc(), inc(), inc())"
        ),
        vec![int(1), int(2), int(3)]
    );
    assert_eq!(
        agent_returns(
            "local n = 0\nlocal function inc() n = n + 1 return n, n end\nlocal a, b = inc(), inc()\nreturn a, b, n"
        ),
        vec![int(1), int(2), int(2)]
    );
}

// -- F10: call shorthand -------------------------------------------------------------------------------------------------

#[test]
fn string_shorthand_passes_one_argument_and_table_shorthand_is_refused() {
    let f = "local function f(s) return s end\n";
    assert_eq!(
        agent_returns(&format!("{f}return f\"x\"")),
        vec![bytes("x")]
    );
    assert_eq!(
        agent_returns(&format!("{f}return f[[y]]")),
        vec![bytes("y")]
    );
    assert_eq!(
        agent_returns(&format!("{f}return f'z', 1")),
        vec![bytes("z"), int(1)]
    );
    for body in [
        format!("{f}return f{{}}"),
        format!("{f}return f{{ 1, 2 }}"),
        format!("{f}local x = f({{}})"),
    ] {
        let diagnostics = err(&format!("return remote:spawn(function()\n{body}\nend)\n"));
        assert_eq!(codes(&diagnostics), vec![code::UNSUPPORTED], "{body}");
        assert!(diagnostics[0].message.contains("table_constructor"));
    }
}

// -- F11: long strings -------------------------------------------------------------------------------------------------

#[test]
fn long_strings_are_bytes_as_written_without_escapes() {
    assert_eq!(agent_returns(r"return [[a\nb]]"), vec![bytes(r"a\nb")]);
    assert_eq!(
        agent_returns("return [==[\nline]] two]==]"),
        vec![bytes("line]] two")]
    );
    assert_eq!(agent_returns("return [[\r\nx\ry]]"), vec![bytes("x\ny")]);
    assert_eq!(
        agent_returns(r#"return "a\x41\u{42}\z   c""#),
        vec![bytes("aABc")]
    );
}

// -- F12: numeric for --------------------------------------------------------------------------------------------------

#[test]
fn integer_loops_terminate_at_the_extremes() {
    assert_eq!(
        agent_returns(
            "local n, last = 0, 0\nfor i = 9223372036854775805, 9223372036854775807 do n = n + 1 last = i end\nreturn n, last"
        ),
        vec![int(3), int(i64::MAX)]
    );
    assert_eq!(
        agent_returns(
            "local n, last = 0, 0\nlocal low = -9223372036854775807 - 1\nfor i = low + 2, low, -1 do n = n + 1 last = i end\nreturn n, last"
        ),
        vec![int(3), int(i64::MIN)]
    );
    assert_eq!(
        agent_returns("local n = 0\nfor i = 1, 10, 3 do n = n + i end\nreturn n"),
        vec![int(22)]
    );
    assert_eq!(
        agent_returns("local n = 0\nfor i = 1, 2.5 do n = n + 1 end\nreturn n"),
        vec![int(2)]
    );
    assert_eq!(
        agent_returns("local last\nfor i = 1, 2, 0.5 do last = i end\nreturn last"),
        vec![Value::Float(2.0)]
    );
    assert_eq!(agent_fails("for i = 1, 3, 0 do end"), "'for' step is zero");
    assert_eq!(
        agent_returns(
            "local fs = 0\nlocal step = 2\nfor i = 10, 1, -step do fs = fs + i end\nreturn fs"
        ),
        vec![int(30)]
    );
}

#[test]
fn a_loop_variable_captured_by_a_closure_is_fresh_on_each_iteration() {
    assert_eq!(
        agent_returns(
            "local first\nfor i = 1, 3 do\n    local function get() return i end\n    if i == 1 then first = get end\nend\nreturn first()"
        ),
        vec![int(1)]
    );
}

// -- F06, F07: destination limits are compile diagnostics -------------------------------------------------------------

#[test]
fn a_string_constant_is_bounded_by_the_destination_at_the_exact_limit() {
    let at = "x".repeat(Limits::profile().max_string_bytes);
    ok(&format!(
        "return remote:spawn(function() return \"{at}\" end)\n"
    ));
    let over = "x".repeat(Limits::profile().max_string_bytes + 1);
    let diagnostics = err(&format!(
        "return remote:spawn(function()\n    return \"{over}\"\nend)\n"
    ));
    assert_eq!(codes(&diagnostics), vec![code::LIMIT]);
    assert_eq!(diagnostics[0].line, 2, "reported at the literal");
}

#[test]
fn the_child_count_is_bounded_by_the_destination() {
    let max = Limits::profile().max_children;
    let spawns = |count: usize| {
        let mut body = String::new();
        for _ in 0..count {
            body.push_str("    remote:spawn(function() return 1 end)\n");
        }
        format!("return remote:spawn(function()\n{body}end)\n")
    };
    ok(&spawns(max));
    let diagnostics = err(&spawns(max + 1));
    assert_eq!(codes(&diagnostics), vec![code::LIMIT]);
}

#[test]
fn every_compiled_artifact_passes_its_recipient_admission() {
    let graph = bsp_graph();
    let workflow = ok(
        "local c = 3\nreturn remote:spawn(function()\n    local t = remote:spawn(function() return chip.model(), c end)\n    return t:await()\nend)\n",
    );
    let agent = graph.contract(&workflow.sites[0].contract).expect("agent");
    let agent_resources = agent.resources.expect("agent envelope");
    let agent_limits = Limits::for_destination(
        agent_resources.max_artifact_bytes as usize,
        agent.limits.max_spawn_depth,
    );
    let artifact = admit(
        &workflow.sites[0].artifact,
        &agent_limits,
        &agent.digest,
        &agent.native_operations(),
    )
    .expect("agent admits");
    let dut_id = &agent.child("remote").expect("slot").child;
    let dut = graph.contract(dut_id).expect("dut");
    let dut_resources = dut.resources.expect("dut envelope");
    let dut_limits = Limits::for_destination(
        dut_resources.max_artifact_bytes as usize,
        dut.limits.max_spawn_depth,
    );
    admit(
        &artifact.children[0].bytes,
        &dut_limits,
        &dut.digest,
        &dut.native_operations(),
    )
    .expect("dut admits");
    assert_eq!(
        artifact.children[0].slot,
        agent.child("remote").expect("slot").id,
        "the edge names the agent's slot"
    );
}

// -- Synthetic trees: envelope size, depth, signatures and slots -------------------------------------------------------

const PROFILE: &str = r#"
schema_version = 1
name = "synthetic-v1"
version = "1.0.0"
[features]
allowed = ["local_assignment", "call", "pcall", "return", "arithmetic", "spawn", "method_call_await", "native_call",
           "number_literal", "string_literal", "function_literal", "local_function"]
"#;

struct Node<'a> {
    file: &'a str,
    name: &'a str,
    id: &'a str,
    depth: u32,
    artifact_bytes: u32,
    children: &'a [(&'a str, u32, u8, &'a str)],
    modules: &'a str,
}

fn manifest(node: &Node<'_>) -> String {
    let mut imports = String::new();
    let mut slots = String::new();
    let mut seen = Vec::new();
    for (slot, id, index, child_file) in node.children {
        let child_id = child_file.split('/').next().expect("dir");
        if !seen.contains(child_file) {
            seen.push(*child_file);
            imports.push_str(&format!(
                "[[imports]]\nalias = \"{child_id}\"\npath = \"../{child_file}\"\ncontract_id = \"{}\"\nversion = \"=1.0.0\"\n\n",
                id_of(child_id)
            ));
        }
        slots.push_str(&format!(
            "[[child_execution]]\nname = \"{slot}\"\nid = {id}\nindex = {index}\nchild_contract = \"{child_id}\"\nintrinsic = \"task.spawn.v1\"\nrequires = []\ndelegates = []\nmax_capture_bytes = 64\nmax_result_bytes = 64\n\n"
        ));
    }
    let resources = if node.artifact_bytes == 0 {
        String::new()
    } else {
        format!(
            "[resources]\nmax_artifact_bytes = {}\nmax_chunk_bytes = 16\nmax_uploads = 2\nmax_sessions = 4\nmax_executions = 2\nmax_answer_bytes = 128\nslice_fuel = 100\ntotal_fuel = 100000\nmax_frames = 8\nmax_objects = 256\nmax_heap_bytes = 16384\nsession_idle_ms = 1000\nmax_artifacts = 3\nretired_window = 16\nmax_child_tasks = 4\ncall_timeout_ms = 100\ncleanup_ms = 500\nmax_request_bytes = 1024\n",
            node.artifact_bytes
        )
    };
    format!(
        "schema_version = 1\n\n[contract]\nname = \"{}\"\nid = \"{}\"\nversion = \"1.0.0\"\nlanguage_profile = \"synthetic-v1\"\nremote_data_profile = \"remote-data-v1\"\nprivileges = []\n\n[contract.limits]\nmax_string_bytes = 64\nmax_tuple_items = 4\nmax_capture_bytes = 64\nmax_result_bytes = 64\nmax_spawn_depth = {}\n\n{imports}{}\n{slots}{resources}",
        node.name, node.id, node.depth, node.modules
    )
}

fn id_of(dir: &str) -> String {
    let digit = match dir {
        "host" => '1',
        "mid" => '2',
        "leaf" => '3',
        _ => '4',
    };
    format!(
        "{d}{d}{d}{d}{d}{d}{d}{d}-{d}{d}{d}{d}-4{d}{d}{d}-8{d}{d}{d}-{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}",
        d = digit
    )
}

fn synthetic(name: &str, nodes: &[Node<'_>], profile: &str) -> (Graph, String) {
    let dir = std::env::temp_dir().join(format!("lua-compiler-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("profiles")).expect("profiles");
    fs::write(dir.join("profiles/synthetic-v1.toml"), profile).expect("profile");
    for node in nodes {
        let path = dir.join(node.file);
        fs::create_dir_all(path.parent().expect("parent")).expect("dirs");
        fs::write(&path, manifest(node)).expect("manifest");
    }
    let graph = Resolver::new(&dir, dir.join("profiles"))
        .expect("resolver")
        .resolve(dir.join(nodes[0].file))
        .expect("synthetic tree resolves");
    let host = graph.root().id.clone();
    (graph, host)
}

const LEAF_MODULE: &str = "[[modules]]\nname = \"io\"\nid = 1\n\n[[modules.operations]]\nname = \"poke\"\nid = 1\nexecution = \"sync\"\n\n[[modules.operations]]\nname = \"pair\"\nid = 2\nexecution = \"sync\"\nresults = [{ name = \"a\", type = \"integer\" }, { name = \"b\", type = \"integer\" }]\n";

fn chain(name: &str, mid_bytes: u32, mid_depth: u32, leaf_children: bool) -> (Graph, String) {
    let leaf_slots: &[(&str, u32, u8, &str)] = if leaf_children {
        &[("deeper", 1, 1, "end/plugin.toml")]
    } else {
        &[]
    };
    let mut nodes = vec![
        Node {
            file: "host/plugin.toml",
            name: "synthetic.host",
            id: &HOST_ID,
            depth: 4,
            artifact_bytes: 0,
            children: &[("mid", 1, 0, "mid/plugin.toml")],
            modules: "",
        },
        Node {
            file: "mid/plugin.toml",
            name: "synthetic.mid",
            id: &MID_ID,
            depth: mid_depth,
            artifact_bytes: mid_bytes,
            children: &[("leaf", 1, 1, "leaf/plugin.toml")],
            modules: "",
        },
        Node {
            file: "leaf/plugin.toml",
            name: "synthetic.leaf",
            id: &LEAF_ID,
            depth: 2,
            artifact_bytes: 4096,
            children: leaf_slots,
            modules: LEAF_MODULE,
        },
    ];
    if leaf_children {
        nodes.push(Node {
            file: "end/plugin.toml",
            name: "synthetic.end",
            id: &END_ID,
            depth: 1,
            artifact_bytes: 4096,
            children: &[],
            modules: "",
        });
    }
    synthetic(name, &nodes, PROFILE)
}

static HOST_ID: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| id_of("host"));
static MID_ID: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| id_of("mid"));
static LEAF_ID: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| id_of("leaf"));
static END_ID: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| id_of("end"));

#[test]
fn the_encoded_size_is_bounded_by_the_destination_at_the_exact_limit() {
    let source = "return mid:spawn(function() local a = 1 local b = 2 return a + b end)\n";
    let (graph, host) = chain("size-probe", 4096, 2, false);
    let size = ok_in(&graph, &host, source).sites[0].artifact.len() as u32;
    let (graph, host) = chain("size-exact", size, 2, false);
    ok_in(&graph, &host, source);
    let (graph, host) = chain("size-under", size - 1, 2, false);
    let diagnostics = err_in(&graph, &host, source);
    assert_eq!(codes(&diagnostics), vec![code::LIMIT]);
    assert!(
        diagnostics[0]
            .message
            .contains("accepts artifacts of at most"),
        "{}",
        diagnostics[0].message
    );
}

#[test]
fn nesting_deeper_than_a_destination_allows_is_a_compile_diagnostic() {
    let source = "return mid:spawn(function()\n    return leaf:spawn(function()\n        return deeper:spawn(function() return 1 end):await()\n    end):await()\nend)\n";
    let (graph, host) = chain("depth-ok", 4096, 3, true);
    ok_in(&graph, &host, source);
    let (graph, host) = chain("depth-over", 4096, 2, true);
    let diagnostics = err_in(&graph, &host, source);
    assert_eq!(codes(&diagnostics), vec![code::LIMIT]);
    assert!(
        diagnostics[0]
            .message
            .contains("nests 2 levels below a region that runs on 'synthetic.mid'"),
        "{}",
        diagnostics[0].message
    );
    assert_eq!(diagnostics[0].line, 3, "reported at the nested spawn");
}

#[test]
fn a_multi_result_native_has_a_truthful_signature_and_is_refused_by_the_vm_profile() {
    let (graph, host) = chain("signature", 4096, 2, false);
    let leaf = graph.checker_rules(&LEAF_ID).expect("leaf rules");
    assert_eq!(leaf.modules["io"].operations["pair"].results.len(), 2);
    let diagnostics = err_in(
        &graph,
        &host,
        "return mid:spawn(function() return leaf:spawn(function() return io.pair() end):await() end)\n",
    );
    assert_eq!(codes(&diagnostics), vec![code::UNSUPPORTED]);
    assert!(
        diagnostics[0].message.contains("returns 2 results"),
        "{}",
        diagnostics[0].message
    );
}

#[test]
fn a_zero_result_native_leaves_no_value() {
    let (graph, host) = chain("zero-results", 4096, 2, false);
    let workflow = ok_in(
        &graph,
        &host,
        "return mid:spawn(function() return leaf:spawn(function() return io.poke() end):await() end)\n",
    );
    let mid =
        Artifact::decode_verified(&workflow.sites[0].artifact, &Limits::profile()).expect("mid");
    let leaf = Artifact::decode_verified(&mid.children[0].bytes, &Limits::profile()).expect("leaf");
    assert_eq!(leaf.natives[0].results, 0);
    let program = Program::verified(leaf, &Limits::profile()).expect("verifies");
    let limits = VmLimits {
        max_frames: 4,
        max_objects: 64,
        max_bytes: 4096,
        wire: WireLimits::new(4, 64, 64),
        total_fuel: 1000,
    };
    let mut machine = Machine::start(program, 0, &[], &[], limits).expect("starts");
    assert!(matches!(machine.run(100), Step::Pending(_)));
    machine.complete(EffectResult::Nothing).expect("complete");
    assert_eq!(
        machine.run(100),
        Step::Returned(Default::default()),
        "`return io.poke()` returns nothing"
    );
}

#[test]
fn zero_result_native_as_the_only_pcall_argument_raises_after_the_effect() {
    let (graph, host) = chain("pcall-zero-native", 4096, 2, false);
    let workflow = ok_in(
        &graph,
        &host,
        "return mid:spawn(function() return leaf:spawn(function() return pcall(function() return pcall(io.poke()) end) end) end)",
    );
    let mid = Artifact::decode_verified(&workflow.sites[0].artifact, &Limits::profile()).unwrap();
    let leaf = Artifact::decode_verified(&mid.children[0].bytes, &Limits::profile()).unwrap();
    let program = Program::verified(leaf, &Limits::profile()).unwrap();
    let limits = VmLimits {
        max_frames: 4,
        max_objects: 64,
        max_bytes: 4096,
        wire: WireLimits::new(4, 64, 64),
        total_fuel: 1000,
    };
    let mut machine = Machine::start(program, 0, &[], &[], limits).unwrap();
    assert!(matches!(machine.run(100), Step::Pending(_)));
    machine.complete(EffectResult::Nothing).unwrap();
    match machine.run(100) {
        Step::Returned(tuple) => assert_eq!(
            tuple.values(),
            &[
                Value::Boolean(false),
                bytes("bad argument #1 to 'pcall' (value expected)")
            ]
        ),
        other => panic!("expected protected missing-argument error: {other:?}"),
    }
}

#[test]
fn two_slots_of_one_contract_give_distinct_edges() {
    let nodes = [
        Node {
            file: "host/plugin.toml",
            name: "synthetic.host",
            id: &HOST_ID,
            depth: 3,
            artifact_bytes: 0,
            children: &[("mid", 1, 0, "mid/plugin.toml")],
            modules: "",
        },
        Node {
            file: "mid/plugin.toml",
            name: "synthetic.mid",
            id: &MID_ID,
            depth: 2,
            artifact_bytes: 4096,
            children: &[
                ("left", 5, 1, "leaf/plugin.toml"),
                ("right", 9, 2, "leaf/plugin.toml"),
            ],
            modules: "",
        },
        Node {
            file: "leaf/plugin.toml",
            name: "synthetic.leaf",
            id: &LEAF_ID,
            depth: 1,
            artifact_bytes: 4096,
            children: &[],
            modules: "",
        },
    ];
    let (graph, host) = synthetic("two-slots", &nodes, PROFILE);
    let workflow = ok_in(
        &graph,
        &host,
        "return mid:spawn(function()\n    local a = left:spawn(function() return 1 end):await()\n    local b = right:spawn(function() return 2 end):await()\n    return a + b\nend)\n",
    );
    let mid =
        Artifact::decode_verified(&workflow.sites[0].artifact, &Limits::profile()).expect("mid");
    let slots: Vec<u32> = mid.children.iter().map(|edge| edge.slot).collect();
    assert_eq!(
        slots,
        vec![5, 9],
        "each edge names its own slot, not just the shared contract"
    );
    assert_eq!(
        protolink_lua_bytecode::contract_of(&mid.children[0].bytes),
        protolink_lua_bytecode::contract_of(&mid.children[1].bytes),
        "both edges run the same contract"
    );
}

#[test]
fn an_artifact_compiled_before_a_profile_change_is_refused_after_it() {
    let before = bsp_graph();
    let workflow = ok_in(
        &before,
        HOST,
        "return remote:spawn(function() return 1 end)\n",
    );
    let after = without_feature("while");
    let site = &workflow.sites[0];
    let old_agent = before.contract(&site.contract).expect("agent");
    let new_agent = after
        .contract(&site.contract)
        .expect("agent after the change");
    assert_ne!(
        old_agent.digest, new_agent.digest,
        "a policy change is a new contract"
    );
    let limits = Limits::for_destination(16_384, 2);
    assert!(
        admit(
            &site.artifact,
            &limits,
            &old_agent.digest,
            &old_agent.native_operations()
        )
        .is_ok()
    );
    assert_eq!(
        admit(
            &site.artifact,
            &limits,
            &new_agent.digest,
            &new_agent.native_operations()
        )
        .err(),
        Some(AdmitError::WrongContract),
        "the re-locked node refuses the old artifact"
    );
}
