//! Code generation for one remote region and its nested regions.
//!
//! A region is one spawn body. Its artifact holds the body as entry 0, the closures it creates, and one child link per
//! nested spawn. Each child is a complete artifact, so the parent forwards the exact bytes it compiled. Names resolve in
//! this order: the region's own declarations, then the destination's capabilities, and only then the enclosing scopes,
//! where a name becomes a capture. A capture is a value snapshot taken when the spawn runs.
//!
//! Every artifact is compiled against its own destination: the destination's resource envelope and limits bound it
//! while it is generated, and the encoded bytes then pass the same admission check the destination node runs. So a
//! workflow that compiles is admitted by every node it reaches, for the locked configuration.
//!
//! Results follow Lua's adjustment rules. The last unparenthesized call of a return list or an argument list expands to
//! all its values, an assignment adjusts to its targets, and parentheses force one value. Native operations carry the
//! number of results their signature declares, zero or one under the VM's ABI.

use std::collections::{BTreeSet, HashSet};

use full_moon::ast::{
    AnonymousFunction, Block, Call, Expression, FunctionArgs, FunctionBody, FunctionCall, Index,
    LastStmt, LocalAssignment, LocalFunction, NumericFor, Parameter, Prefix, Stmt, Suffix, UnOp,
    Var, VarExpression,
};
use full_moon::tokenizer::{StringLiteralQuoteType, TokenReference, TokenType};
use protolink_lua_bytecode::{
    Artifact, ChildLink, Constant, DIGEST_LEN, Entry, Instr, Limits, MAX_NATIVE_RESULTS, NativeRef,
    Proto, RESULTS_MULTI, UpvalueSource,
};
use protolink_lua_plugin_gen::{CheckerRules, Graph};
use sha2::{Digest, Sha256};

use crate::diag::{Diagnostic, code};
use crate::scan::{self, text};

pub(crate) type Result<T> = std::result::Result<T, Diagnostic>;

/// What a function frame is.
enum Kind {
    /// The host chunk, which is not compiled. It only supplies the visible names.
    Host,
    /// A spawn body. Its upvalues are snapshotted captures, listed by name.
    Entry { captures: Vec<String> },
    /// A closure. Its upvalues come from its creator, and each source is recorded.
    Closure {
        names: Vec<String>,
        sources: Vec<UpvalueSource>,
    },
}

/// A local declared in a frame.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Local {
    name: String,
    slot: u16,
    cell: bool,
}

/// Where a name's value lives, from the point of view of the frame that uses it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Access {
    Slot { slot: u16, cell: bool },
    Upvalue(u16),
}

struct Frame {
    kind: Kind,
    unit: usize,
    scopes: Vec<Vec<Local>>,
    slots: u16,
    code: Vec<Instr>,
    lines: Vec<u32>,
    loops: Vec<Vec<usize>>,
}

/// One artifact under construction.
struct Unit {
    rules: CheckerRules,
    /// The bounds the destination node decodes and verifies this artifact with.
    limits: Limits,
    constants: Vec<Constant>,
    natives: Vec<NativeRef>,
    protos: Vec<Option<Proto>>,
    children: Vec<ChildLink>,
}

/// How a call expression is compiled.
enum CallKind {
    Lua,
    Native { module: String, op: String },
    Spawn { slot: String },
    Await,
    Yield,
    Pcall,
    Error,
}

impl CallKind {
    /// The profile feature this kind of call needs. Every call site is classified here, so no syntactic context can
    /// use an intrinsic without its feature.
    fn feature(&self) -> &'static str {
        match self {
            Self::Lua => "call",
            Self::Native { .. } => "native_call",
            Self::Spawn { .. } => "spawn",
            Self::Await => "method_call_await",
            Self::Yield => "coroutine_yield",
            Self::Pcall => "pcall",
            Self::Error => "error_call",
        }
    }
}

/// How many values a call site wants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Results {
    /// Exactly this many values.
    Fixed(usize),
    /// Every value, followed by a count marker for the next instruction.
    Multi,
}

/// What the last expression of a list contributes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tail {
    /// One value: any expression that is not a call, or a parenthesized call.
    Single,
    /// A fixed number of values that is not one: a native with zero results.
    Fixed(usize),
    /// A variable number of values, left with a count marker.
    Dynamic,
}

/// What an expression list left on the stack: `fixed` values, then, when `dynamic`, a variable tail and its marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Shape {
    fixed: usize,
    dynamic: bool,
}

pub(crate) struct Compiler<'g> {
    graph: &'g Graph,
    cells: HashSet<usize>,
    frames: Vec<Frame>,
    units: Vec<Unit>,
    line: usize,
    column: usize,
}

impl<'g> Compiler<'g> {
    pub(crate) fn new(graph: &'g Graph, cells: HashSet<usize>) -> Self {
        Self {
            graph,
            cells,
            frames: Vec::new(),
            units: Vec::new(),
            line: 1,
            column: 1,
        }
    }

    /// Compiles one host placement. `visible` is the set of host locals in scope at the call.
    pub(crate) fn site(
        &mut self,
        visible: BTreeSet<String>,
        destination: &str,
        function: &AnonymousFunction,
    ) -> Result<(Vec<u8>, Vec<String>)> {
        let scope = visible
            .into_iter()
            .map(|name| Local {
                name,
                slot: 0,
                cell: false,
            })
            .collect();
        self.frames = vec![Frame {
            kind: Kind::Host,
            unit: usize::MAX,
            scopes: vec![scope],
            slots: 0,
            code: Vec::new(),
            lines: Vec::new(),
            loops: Vec::new(),
        }];
        self.units.clear();
        self.position(function.function_token());
        let result = self.entry(destination, function.body());
        self.frames.clear();
        self.units.clear();
        result
    }

    fn position(&mut self, token: &TokenReference) {
        self.line = token.start_position().line();
        self.column = token.start_position().character();
    }

    fn fail<T>(&self, code: &'static str, message: impl Into<String>) -> Result<T> {
        Err(Diagnostic::new(code, self.line, self.column, message))
    }

    fn unsupported<T>(&self, feature: &str) -> Result<T> {
        self.fail(
            code::UNSUPPORTED,
            format!("'{feature}' is not supported by the language profile of this region"),
        )
    }

    /// Fails unless the destination profile allows `feature`.
    fn feature(&self, feature: &str) -> Result<()> {
        let allowed = self
            .units
            .last()
            .is_some_and(|unit| unit.rules.allowed_features.contains(feature));
        if allowed {
            Ok(())
        } else {
            self.unsupported(feature)
        }
    }

    fn frame(&mut self) -> &mut Frame {
        let last = self.frames.len() - 1;
        &mut self.frames[last]
    }

    fn unit(&self) -> &Unit {
        &self.units[self.frames[self.frames.len() - 1].unit]
    }

    fn unit_mut(&mut self) -> &mut Unit {
        let index = self.frames[self.frames.len() - 1].unit;
        &mut self.units[index]
    }

    fn rules(&self) -> &CheckerRules {
        &self.unit().rules
    }

    fn limits(&self) -> Limits {
        self.unit().limits
    }

    fn emit(&mut self, instr: Instr) -> Result<usize> {
        let (line, max) = (self.line, self.limits().max_code);
        let frame = self.frame();
        if frame.code.len() >= max {
            return self.fail(
                code::LIMIT,
                format!(
                    "region exceeds the destination's limit of {max} instructions per function"
                ),
            );
        }
        frame.code.push(instr);
        frame.lines.push(u32::try_from(line).unwrap_or(u32::MAX));
        Ok(frame.code.len() - 1)
    }

    fn here(&mut self) -> u32 {
        self.frame().code.len() as u32
    }

    fn patch(&mut self, at: usize, target: u32) {
        let frame = self.frame();
        frame.code[at] = match frame.code[at] {
            Instr::Jump(_) => Instr::Jump(target),
            Instr::JumpIfFalse(_) => Instr::JumpIfFalse(target),
            Instr::JumpIfFalseKeep(_) => Instr::JumpIfFalseKeep(target),
            Instr::JumpIfTrueKeep(_) => Instr::JumpIfTrueKeep(target),
            Instr::ForPrep { slot, .. } => Instr::ForPrep { slot, exit: target },
            other => other,
        };
    }

    fn constant(&mut self, value: Constant) -> Result<u32> {
        let limits = self.limits();
        if let Constant::Bytes(bytes) = &value
            && bytes.len() > limits.max_string_bytes
        {
            return self.fail(
                code::LIMIT,
                format!(
                    "string constant of {} bytes exceeds the destination's limit of {}",
                    bytes.len(),
                    limits.max_string_bytes
                ),
            );
        }
        if let Some(index) = self.unit().constants.iter().position(|c| *c == value) {
            return Ok(index as u32);
        }
        if self.unit().constants.len() >= limits.max_constants {
            return self.fail(
                code::LIMIT,
                format!(
                    "region exceeds the destination's limit of {} constants",
                    limits.max_constants
                ),
            );
        }
        let unit = self.unit_mut();
        unit.constants.push(value);
        Ok((unit.constants.len() - 1) as u32)
    }

    fn native(&mut self, reference: NativeRef) -> Result<u16> {
        if let Some(index) = self.unit().natives.iter().position(|n| *n == reference) {
            return Ok(index as u16);
        }
        let max = self.limits().max_natives;
        if self.unit().natives.len() >= max {
            return self.fail(
                code::LIMIT,
                format!("region exceeds the destination's limit of {max} native operations"),
            );
        }
        let unit = self.unit_mut();
        unit.natives.push(reference);
        Ok((unit.natives.len() - 1) as u16)
    }

    fn slot(&mut self) -> Result<u16> {
        let max = self.limits().max_locals;
        let frame = self.frame();
        if frame.slots >= max {
            return self.fail(
                code::LIMIT,
                format!("region exceeds the destination's limit of {max} local slots"),
            );
        }
        frame.slots += 1;
        Ok(frame.slots - 1)
    }

    fn declare(&mut self, token: &TokenReference) -> Result<Local> {
        let slot = self.slot()?;
        let local = Local {
            name: text(token),
            slot,
            cell: self.cells.contains(&scan::start(token)),
        };
        if let Some(scope) = self.frame().scopes.last_mut() {
            scope.push(local.clone());
        }
        Ok(local)
    }

    // -- name resolution -----------------------------------------------------------------------------------------------

    fn find_local(&self, depth: usize, name: &str) -> Option<Local> {
        self.frames[depth]
            .scopes
            .iter()
            .rev()
            .flat_map(|scope| scope.iter().rev())
            .find(|local| local.name == name)
            .cloned()
    }

    fn find_upvalue(&self, depth: usize, name: &str) -> Option<u16> {
        let position = match &self.frames[depth].kind {
            Kind::Entry { captures } => captures.iter().position(|n| n == name),
            Kind::Closure { names, .. } => names.iter().position(|n| n == name),
            Kind::Host => None,
        };
        position.map(|p| p as u16)
    }

    /// Resolves `name` as seen from frame `depth`. `None` means the name is not a local or a capture; it may be a
    /// capability, which the caller checks.
    fn resolve(&mut self, depth: usize, name: &str) -> Result<Option<Access>> {
        if let Some(local) = self.find_local(depth, name) {
            return Ok(Some(Access::Slot {
                slot: local.slot,
                cell: local.cell,
            }));
        }
        if let Some(index) = self.find_upvalue(depth, name) {
            return Ok(Some(Access::Upvalue(index)));
        }
        if depth == 0 {
            return Ok(None);
        }
        if matches!(self.frames[depth].kind, Kind::Entry { .. }) {
            // A capability of the destination wins over any enclosing local of the same name.
            let unit = self.frames[depth].unit;
            if self.units[unit].rules.is_capability(name) {
                return Ok(None);
            }
        }
        let Some(outer) = self.resolve(depth - 1, name)? else {
            return Ok(None);
        };
        let max = self.units[self.frames[depth].unit].limits.max_upvalues;
        match &mut self.frames[depth].kind {
            Kind::Entry { captures } => {
                if captures.len() >= max {
                    return self.fail(
                        code::LIMIT,
                        format!("region exceeds the destination's limit of {max} captures"),
                    );
                }
                captures.push(name.to_string());
                Ok(Some(Access::Upvalue((captures.len() - 1) as u16)))
            }
            Kind::Closure { names, sources } => {
                let source = match outer {
                    Access::Slot { slot, cell: true } => UpvalueSource::Local(slot),
                    Access::Slot { cell: false, .. } => {
                        return self.fail(
                            code::INTERNAL,
                            format!("captured local '{name}' was not placed in a cell"),
                        );
                    }
                    Access::Upvalue(index) => UpvalueSource::Upvalue(index),
                };
                if names.len() >= max {
                    return self.fail(code::LIMIT, "closure exceeds the upvalue limit");
                }
                names.push(name.to_string());
                sources.push(source);
                Ok(Some(Access::Upvalue((names.len() - 1) as u16)))
            }
            Kind::Host => Ok(None),
        }
    }

    /// Resolves a name used by the current frame.
    fn name_access(&mut self, token: &TokenReference) -> Result<Option<Access>> {
        self.position(token);
        let name = text(token);
        let top = self.frames.len() - 1;
        self.resolve(top, &name)
    }

    fn load(&mut self, access: Access) -> Result<()> {
        match access {
            Access::Slot { slot, cell: false } => self.emit(Instr::GetLocal(slot))?,
            Access::Slot { slot, cell: true } => self.emit(Instr::GetCell(slot))?,
            Access::Upvalue(index) => self.emit(Instr::GetUpvalue(index))?,
        };
        Ok(())
    }

    fn store(&mut self, access: Access) -> Result<()> {
        match access {
            Access::Slot { slot, cell: false } => self.emit(Instr::SetLocal(slot))?,
            Access::Slot { slot, cell: true } => self.emit(Instr::SetCell(slot))?,
            Access::Upvalue(index) => self.emit(Instr::SetUpvalue(index))?,
        };
        Ok(())
    }

    // -- regions and functions -----------------------------------------------------------------------------------------

    /// Compiles a spawn body for `destination`, returning its admitted artifact bytes and its capture names in order.
    fn entry(&mut self, destination: &str, body: &FunctionBody) -> Result<(Vec<u8>, Vec<String>)> {
        let site = (self.line, self.column);
        let Some(rules) = self.graph.checker_rules(destination) else {
            return self.fail(
                code::INTERNAL,
                format!("contract '{destination}' is not in the graph"),
            );
        };
        let Some(resources) = rules.resources else {
            return self.fail(
                code::PLACEMENT,
                format!(
                    "contract '{}' declares no resource envelope, so no node runs it",
                    rules.contract
                ),
            );
        };
        let limits = Limits::for_destination(
            resources.max_artifact_bytes as usize,
            rules.limits.max_spawn_depth,
        );
        // This region is embedded in every enclosing region's artifact, one level deeper than its parent, and each
        // enclosing destination decodes that whole nesting under its own depth bound.
        let level = self.units.len();
        for (index, unit) in self.units.iter().enumerate() {
            let relative = level - index;
            if relative > usize::from(unit.limits.max_depth) {
                return self.fail(
                    code::LIMIT,
                    format!(
                        "spawn nests {relative} levels below a region that runs on '{}', which allows {}",
                        unit.rules.contract, unit.limits.max_depth
                    ),
                );
            }
        }
        let contract_digest = rules.digest;
        let contract_name = rules.contract.clone();
        let unit = self.units.len();
        self.units.push(Unit {
            rules,
            limits,
            constants: Vec::new(),
            natives: Vec::new(),
            protos: vec![None],
            children: Vec::new(),
        });
        self.frames.push(Frame {
            kind: Kind::Entry {
                captures: Vec::new(),
            },
            unit,
            scopes: vec![Vec::new()],
            slots: 0,
            code: Vec::new(),
            lines: Vec::new(),
            loops: Vec::new(),
        });
        let outcome = self.function_body(body);
        let frame = self.frames.pop();
        let unit_state = self.units.pop();
        let (Some(frame), Some(mut unit_state)) = (frame, unit_state) else {
            return self.fail(code::INTERNAL, "frame stack is unbalanced");
        };
        let params = outcome?;
        let captures = match frame.kind {
            Kind::Entry { captures } => captures,
            _ => return self.fail(code::INTERNAL, "entry frame has the wrong kind"),
        };
        unit_state.protos[0] = Some(Proto {
            params: params.len() as u8,
            locals: frame.slots,
            max_stack: limits.max_stack,
            upvalues: Vec::new(),
            code: frame.code,
            lines: frame.lines,
        });
        let protos: Vec<Proto> = unit_state.protos.into_iter().flatten().collect();
        let artifact = Artifact {
            contract: contract_digest,
            constants: unit_state.constants,
            natives: unit_state.natives,
            protos,
            entries: vec![Entry {
                proto: 0,
                captures: captures.len() as u16,
            }],
            children: unit_state.children,
        };
        let bytes = artifact.encode();
        (self.line, self.column) = site;
        if bytes.len() > limits.max_artifact_bytes {
            return self.fail(
                code::LIMIT,
                format!(
                    "region compiles to {} bytes; '{contract_name}' accepts artifacts of at most {}",
                    bytes.len(),
                    limits.max_artifact_bytes
                ),
            );
        }
        // The recipient's own admission check, with its own limits. Compiling succeeds only if the node would admit it.
        if let Err(error) = protolink_lua_bytecode::admit(
            &bytes,
            &limits,
            &contract_digest,
            &unit_state.rules.natives,
        ) {
            return self.fail(
                code::LIMIT,
                format!("'{contract_name}' would refuse the compiled region: {error}"),
            );
        }
        Ok((bytes, captures))
    }

    /// Compiles a closure body into a new proto of the current unit and returns its index.
    fn closure(&mut self, body: &FunctionBody) -> Result<u32> {
        let unit = self.frames[self.frames.len() - 1].unit;
        let index = self.units[unit].protos.len();
        let max = self.units[unit].limits.max_protos;
        if index >= max {
            return self.fail(
                code::LIMIT,
                format!("region exceeds the destination's limit of {max} functions"),
            );
        }
        self.units[unit].protos.push(None);
        self.frames.push(Frame {
            kind: Kind::Closure {
                names: Vec::new(),
                sources: Vec::new(),
            },
            unit,
            scopes: vec![Vec::new()],
            slots: 0,
            code: Vec::new(),
            lines: Vec::new(),
            loops: Vec::new(),
        });
        let outcome = self.function_body(body);
        let frame = self.frames.pop();
        let Some(frame) = frame else {
            return self.fail(code::INTERNAL, "frame stack is unbalanced");
        };
        let params = outcome?;
        let upvalues = match frame.kind {
            Kind::Closure { sources, .. } => sources,
            _ => return self.fail(code::INTERNAL, "closure frame has the wrong kind"),
        };
        let max_stack = self.units[unit].limits.max_stack;
        self.units[unit].protos[index] = Some(Proto {
            params: params.len() as u8,
            locals: frame.slots,
            max_stack,
            upvalues,
            code: frame.code,
            lines: frame.lines,
        });
        Ok(index as u32)
    }

    /// Declares the parameters, emits their cell prologue, compiles the block and appends a final return.
    fn function_body(&mut self, body: &FunctionBody) -> Result<Vec<Local>> {
        let mut params = Vec::new();
        for parameter in body.parameters() {
            match parameter {
                Parameter::Name(token) => {
                    self.position(token);
                    if params.len() >= usize::from(u8::MAX) {
                        return self.fail(code::LIMIT, "a function takes at most 255 parameters");
                    }
                    params.push(self.declare(token)?);
                }
                Parameter::Ellipsis(token) => {
                    self.position(token);
                    return self.unsupported("varargs");
                }
                _ => return self.unsupported("parameter"),
            }
        }
        for local in &params {
            if local.cell {
                self.emit(Instr::GetLocal(local.slot))?;
                self.emit(Instr::NewCell(local.slot))?;
            }
        }
        self.block(body.block())?;
        self.emit(Instr::Return(0))?;
        Ok(params)
    }

    // -- statements -----------------------------------------------------------------------------------------------------

    fn block(&mut self, block: &Block) -> Result<()> {
        self.frame().scopes.push(Vec::new());
        for stmt in block.stmts() {
            self.stmt(stmt)?;
        }
        if let Some(last) = block.last_stmt() {
            self.last(last)?;
        }
        self.frame().scopes.pop();
        Ok(())
    }

    fn stmt(&mut self, stmt: &Stmt) -> Result<()> {
        match stmt {
            Stmt::LocalAssignment(assignment) => self.local_assignment(assignment),
            Stmt::LocalFunction(function) => self.local_function(function),
            Stmt::Assignment(assignment) => {
                self.feature("assignment")?;
                let targets: Vec<Access> = assignment
                    .variables()
                    .iter()
                    .map(|var| self.assign_target(var))
                    .collect::<Result<_>>()?;
                self.explist(
                    &assignment.expressions().iter().collect::<Vec<_>>(),
                    targets.len(),
                )?;
                for target in targets.into_iter().rev() {
                    self.store(target)?;
                }
                Ok(())
            }
            Stmt::FunctionCall(call) => self.call(call, Results::Fixed(0)),
            Stmt::If(branch) => {
                self.feature("if")?;
                self.position(branch.if_token());
                let mut ends = Vec::new();
                self.expr(branch.condition())?;
                let mut skip = self.emit(Instr::JumpIfFalse(0))?;
                self.block(branch.block())?;
                let has_more = branch.else_if().is_some_and(|v| !v.is_empty())
                    || branch.else_block().is_some();
                if has_more {
                    ends.push(self.emit(Instr::Jump(0))?);
                }
                let next = self.here();
                self.patch(skip, next);
                for else_if in branch.else_if().into_iter().flatten() {
                    self.position(else_if.else_if_token());
                    self.expr(else_if.condition())?;
                    skip = self.emit(Instr::JumpIfFalse(0))?;
                    self.block(else_if.block())?;
                    ends.push(self.emit(Instr::Jump(0))?);
                    let next = self.here();
                    self.patch(skip, next);
                }
                if let Some(otherwise) = branch.else_block() {
                    self.block(otherwise)?;
                }
                let end = self.here();
                for at in ends {
                    self.patch(at, end);
                }
                Ok(())
            }
            Stmt::While(loop_stmt) => {
                self.feature("while")?;
                self.position(loop_stmt.while_token());
                let start = self.here();
                self.expr(loop_stmt.condition())?;
                let exit = self.emit(Instr::JumpIfFalse(0))?;
                self.frame().loops.push(Vec::new());
                self.block(loop_stmt.block())?;
                self.emit(Instr::Jump(start))?;
                let end = self.here();
                self.patch(exit, end);
                let breaks = self.frame().loops.pop().unwrap_or_default();
                for at in breaks {
                    self.patch(at, end);
                }
                Ok(())
            }
            Stmt::NumericFor(numeric) => self.numeric_for(numeric),
            Stmt::Do(_) => self.unsupported("do_block"),
            Stmt::Repeat(_) => self.unsupported("repeat_until"),
            Stmt::GenericFor(_) => self.unsupported("generic_for"),
            Stmt::FunctionDeclaration(_) => self.unsupported("function_declaration"),
            Stmt::Goto(_) | Stmt::Label(_) => self.unsupported("goto"),
            _ => self.unsupported("statement"),
        }
    }

    fn assign_target(&mut self, var: &Var) -> Result<Access> {
        match var {
            Var::Name(token) => {
                self.position(token);
                match self.name_access(token)? {
                    Some(access) => Ok(access),
                    None => self.fail(
                        code::ASSIGNMENT,
                        format!(
                            "'{}' is not a remote local and cannot be assigned",
                            text(token)
                        ),
                    ),
                }
            }
            Var::Expression(_) => self.fail(
                code::ASSIGNMENT,
                "assignment to a field is not supported in remote regions",
            ),
            _ => self.unsupported("assignment"),
        }
    }

    fn local_assignment(&mut self, assignment: &LocalAssignment) -> Result<()> {
        self.feature("local_assignment")?;
        if assignment.attributes().any(|a| a.is_some()) {
            return self.unsupported("attributes");
        }
        self.position(assignment.local_token());
        let expressions: Vec<&Expression> = assignment.expressions().iter().collect();
        self.explist(&expressions, assignment.names().len())?;
        let mut locals = Vec::new();
        for name in assignment.names() {
            locals.push(self.declare(name)?);
        }
        for local in locals.into_iter().rev() {
            if local.cell {
                self.emit(Instr::NewCell(local.slot))?;
            } else {
                self.emit(Instr::SetLocal(local.slot))?;
            }
        }
        Ok(())
    }

    fn local_function(&mut self, function: &LocalFunction) -> Result<()> {
        self.feature("local_function")?;
        self.position(function.local_token());
        let local = self.declare(function.name())?;
        if local.cell {
            self.emit(Instr::Nil)?;
            self.emit(Instr::NewCell(local.slot))?;
        }
        let index = self.closure(function.body())?;
        self.emit(Instr::Closure(index))?;
        if local.cell {
            self.emit(Instr::SetCell(local.slot))?;
        } else {
            self.emit(Instr::SetLocal(local.slot))?;
        }
        Ok(())
    }

    /// `for v = start, limit, step do ... end`, lowered to Lua 5.4's prepare-and-loop pair. The start, the limit and
    /// the step are evaluated once, in that order, into three hidden locals. An integer loop counts its iterations in
    /// advance, so it stops at the extreme integers instead of wrapping; a float loop compares each time; a zero step
    /// or a non-number fails at run time as it does in Lua.
    fn numeric_for(&mut self, numeric: &NumericFor) -> Result<()> {
        self.feature("numeric_for")?;
        self.position(numeric.for_token());
        let base = self.slot()?;
        self.slot()?;
        self.slot()?;
        self.expr(numeric.start())?;
        self.emit(Instr::SetLocal(base))?;
        self.expr(numeric.end())?;
        self.emit(Instr::SetLocal(base + 1))?;
        match numeric.step() {
            Some(step) => self.expr(step)?,
            None => {
                let one = self.constant(Constant::Integer(1))?;
                self.emit(Instr::Constant(one))?;
            }
        }
        self.emit(Instr::SetLocal(base + 2))?;
        self.position(numeric.for_token());
        let prepare = self.emit(Instr::ForPrep {
            slot: base,
            exit: 0,
        })?;
        let body = self.here();

        self.frame().scopes.push(Vec::new());
        let variable = self.declare(numeric.index_variable())?;
        self.emit(Instr::GetLocal(base))?;
        if variable.cell {
            self.emit(Instr::NewCell(variable.slot))?;
        } else {
            self.emit(Instr::SetLocal(variable.slot))?;
        }
        self.frame().loops.push(Vec::new());
        self.block(numeric.block())?;
        self.frame().scopes.pop();

        self.position(numeric.for_token());
        self.emit(Instr::ForLoop { slot: base, body })?;
        let end = self.here();
        self.patch(prepare, end);
        let breaks = self.frame().loops.pop().unwrap_or_default();
        for at in breaks {
            self.patch(at, end);
        }
        Ok(())
    }

    fn last(&mut self, last: &LastStmt) -> Result<()> {
        match last {
            LastStmt::Break(token) => {
                self.feature("break")?;
                self.position(token);
                if self.frame().loops.is_empty() {
                    return self.fail(code::BREAK, "break outside a loop");
                }
                let at = self.emit(Instr::Jump(0))?;
                if let Some(loop_breaks) = self.frame().loops.last_mut() {
                    loop_breaks.push(at);
                }
                Ok(())
            }
            LastStmt::Return(ret) => {
                self.feature("return")?;
                self.position(ret.token());
                let values: Vec<&Expression> = ret.returns().iter().collect();
                let shape = self.expand(&values)?;
                let fixed = self.count_u8(shape.fixed, "returned values")?;
                if shape.dynamic {
                    self.emit(Instr::ReturnMulti(fixed))?;
                } else {
                    self.check_tuple_items(shape.fixed, "a return")?;
                    self.emit(Instr::Return(fixed))?;
                }
                Ok(())
            }
            _ => self.unsupported("statement"),
        }
    }

    // -- expressions ---------------------------------------------------------------------------------------------------

    /// Pushes exactly `want` values from `expressions`, adjusting like Lua: extra values are evaluated and discarded,
    /// and missing values are nil. A trailing call supplies its values directly.
    fn explist(&mut self, expressions: &[&Expression], want: usize) -> Result<()> {
        let mut pushed = 0usize;
        let count = expressions.len();
        for (index, expression) in expressions.iter().enumerate() {
            if index + 1 == count && pushed < want {
                self.multi(expression, want - pushed)?;
                pushed = want;
            } else if pushed < want {
                self.expr(expression)?;
                pushed += 1;
            } else {
                self.multi(expression, 0)?;
            }
        }
        while pushed < want {
            self.emit(Instr::Nil)?;
            pushed += 1;
        }
        Ok(())
    }

    /// Pushes every value of `expressions` the way a return list or an argument list does: each expression gives one
    /// value, except an unparenthesized call at the end, which gives all of its values.
    fn expand(&mut self, expressions: &[&Expression]) -> Result<Shape> {
        let Some((last, leading)) = expressions.split_last() else {
            return Ok(Shape {
                fixed: 0,
                dynamic: false,
            });
        };
        for expression in leading {
            self.expr(expression)?;
        }
        let fixed = leading.len();
        match self.tail(last)? {
            Tail::Single => {
                self.expr(last)?;
                Ok(Shape {
                    fixed: fixed + 1,
                    dynamic: false,
                })
            }
            Tail::Fixed(count) => {
                self.multi(last, count)?;
                Ok(Shape {
                    fixed: fixed + count,
                    dynamic: false,
                })
            }
            Tail::Dynamic => {
                let Expression::FunctionCall(call) = last else {
                    return self.fail(code::INTERNAL, "a dynamic tail is not a call");
                };
                self.call(call, Results::Multi)?;
                Ok(Shape {
                    fixed,
                    dynamic: true,
                })
            }
        }
    }

    /// What the last expression of a list contributes.
    fn tail(&mut self, expression: &Expression) -> Result<Tail> {
        let Expression::FunctionCall(call) = expression else {
            return Ok(Tail::Single);
        };
        Ok(match self.call_kind(call)? {
            CallKind::Lua | CallKind::Await | CallKind::Yield | CallKind::Pcall => Tail::Dynamic,
            CallKind::Native { module, op } => match self.native_results(&module, &op)? {
                1 => Tail::Single,
                count => Tail::Fixed(usize::from(count)),
            },
            CallKind::Spawn { .. } | CallKind::Error => Tail::Single,
        })
    }

    fn count_u8(&self, count: usize, what: &str) -> Result<u8> {
        u8::try_from(count)
            .ok()
            .filter(|&count| count != RESULTS_MULTI)
            .map_or_else(
                || self.fail(code::LIMIT, format!("too many {what} in one list")),
                Ok,
            )
    }

    /// Fails when a fixed tuple can never fit the destination's tuples.
    fn check_tuple_items(&self, count: usize, what: &str) -> Result<()> {
        let max = self.rules().limits.max_tuple_items as usize;
        if count > max {
            return self.fail(
                code::LIMIT,
                format!(
                    "{what} carries {count} values; the destination's tuples hold at most {max}"
                ),
            );
        }
        Ok(())
    }

    /// Pushes exactly `want` values from one expression.
    fn multi(&mut self, expression: &Expression, want: usize) -> Result<()> {
        if let Expression::FunctionCall(call) = expression {
            return self.call(call, Results::Fixed(want));
        }
        self.expr(expression)?;
        if want == 0 {
            self.emit(Instr::Pop)?;
        }
        for _ in 1..want {
            self.emit(Instr::Nil)?;
        }
        Ok(())
    }

    /// Pushes exactly one value. The expression's own start position is the diagnostic position.
    fn expr(&mut self, expression: &Expression) -> Result<()> {
        if let Some(position) = full_moon::node::Node::start_position(expression) {
            self.line = position.line();
            self.column = position.character();
        }
        match expression {
            Expression::Number(token) => {
                self.position(token);
                self.feature("number_literal")?;
                let constant = match number_constant(token) {
                    Some(value) => value,
                    None => {
                        return self.fail(
                            code::LITERAL,
                            "number literal cannot be represented in the profile",
                        );
                    }
                };
                let index = self.constant(constant)?;
                self.emit(Instr::Constant(index))?;
                Ok(())
            }
            Expression::String(token) => {
                self.position(token);
                self.feature("string_literal")?;
                let bytes = match string_constant(token) {
                    Ok(bytes) => bytes,
                    Err(message) => return self.fail(code::LITERAL, message),
                };
                let index = self.constant(Constant::Bytes(bytes))?;
                self.emit(Instr::Constant(index))?;
                Ok(())
            }
            Expression::Symbol(token) => {
                self.position(token);
                let TokenType::Symbol { symbol } = token.token_type() else {
                    return self.fail(code::INTERNAL, "symbol token has the wrong type");
                };
                match symbol {
                    full_moon::tokenizer::Symbol::True => {
                        self.feature("boolean_literal")?;
                        self.emit(Instr::True)?;
                    }
                    full_moon::tokenizer::Symbol::False => {
                        self.feature("boolean_literal")?;
                        self.emit(Instr::False)?;
                    }
                    full_moon::tokenizer::Symbol::Nil => {
                        self.feature("nil_literal")?;
                        self.emit(Instr::Nil)?;
                    }
                    _ => return self.unsupported("expression"),
                }
                Ok(())
            }
            Expression::Var(var) => self.var_value(var),
            Expression::FunctionCall(call) => self.call(call, Results::Fixed(1)),
            Expression::Function(function) => {
                self.feature("function_literal")?;
                self.position(function.function_token());
                let index = self.closure(function.body())?;
                self.emit(Instr::Closure(index))?;
                Ok(())
            }
            Expression::Parentheses { expression, .. } => self.expr(expression),
            Expression::UnaryOperator { unop, expression } => {
                let instr = match unop {
                    UnOp::Minus(token) => {
                        self.position(token);
                        self.feature("arithmetic")?;
                        Instr::Neg
                    }
                    UnOp::Not(token) => {
                        self.position(token);
                        self.feature("logic")?;
                        Instr::Not
                    }
                    UnOp::Hash(token) => {
                        self.position(token);
                        self.feature("length")?;
                        Instr::Len
                    }
                    _ => return self.unsupported("unary operator"),
                };
                self.expr(expression)?;
                self.emit(instr)?;
                Ok(())
            }
            Expression::BinaryOperator { lhs, binop, rhs } => self.binary(lhs, binop, rhs),
            Expression::TableConstructor(_) => self.unsupported("table_constructor"),
            _ => self.unsupported("expression"),
        }
    }

    fn binary(
        &mut self,
        lhs: &Expression,
        op: &full_moon::ast::BinOp,
        rhs: &Expression,
    ) -> Result<()> {
        use full_moon::ast::BinOp;
        if let BinOp::And(token) | BinOp::Or(token) = op {
            self.position(token);
            self.feature("logic")?;
            self.expr(lhs)?;
            let keep = if matches!(op, BinOp::And(_)) {
                self.emit(Instr::JumpIfFalseKeep(0))?
            } else {
                self.emit(Instr::JumpIfTrueKeep(0))?
            };
            self.expr(rhs)?;
            let end = self.here();
            self.patch(keep, end);
            return Ok(());
        }
        let instr = match op {
            BinOp::Plus(t) => {
                self.feature_at(t, "arithmetic")?;
                Instr::Add
            }
            BinOp::Minus(t) => {
                self.feature_at(t, "arithmetic")?;
                Instr::Sub
            }
            BinOp::Star(t) => {
                self.feature_at(t, "arithmetic")?;
                Instr::Mul
            }
            BinOp::Slash(t) => {
                self.feature_at(t, "arithmetic")?;
                Instr::Div
            }
            BinOp::DoubleSlash(t) => {
                self.feature_at(t, "arithmetic")?;
                Instr::IDiv
            }
            BinOp::Percent(t) => {
                self.feature_at(t, "arithmetic")?;
                Instr::Mod
            }
            BinOp::TwoDots(t) => {
                self.feature_at(t, "concat")?;
                Instr::Concat
            }
            BinOp::TwoEqual(t) => {
                self.feature_at(t, "comparison")?;
                Instr::Eq
            }
            BinOp::TildeEqual(t) => {
                self.feature_at(t, "comparison")?;
                Instr::Ne
            }
            BinOp::LessThan(t) => {
                self.feature_at(t, "comparison")?;
                Instr::Lt
            }
            BinOp::LessThanEqual(t) => {
                self.feature_at(t, "comparison")?;
                Instr::Le
            }
            BinOp::GreaterThan(t) => {
                self.feature_at(t, "comparison")?;
                Instr::Gt
            }
            BinOp::GreaterThanEqual(t) => {
                self.feature_at(t, "comparison")?;
                Instr::Ge
            }
            _ => return self.unsupported("operator"),
        };
        self.expr(lhs)?;
        self.expr(rhs)?;
        self.emit(instr)?;
        Ok(())
    }

    /// Records the position of an operator and checks its feature.
    fn feature_at(&mut self, token: &TokenReference, feature: &str) -> Result<()> {
        self.position(token);
        self.feature(feature)
    }

    /// Value of `Name` or `Name.Enum.variant` used as an expression.
    fn var_value(&mut self, var: &Var) -> Result<()> {
        match var {
            Var::Name(token) => {
                self.position(token);
                match self.name_access(token)? {
                    Some(access) => {
                        self.load(access)?;
                        Ok(())
                    }
                    None => {
                        let name = text(token);
                        if self.rules().is_capability(&name) {
                            self.fail(
                                code::CAPABILITY_VALUE,
                                format!(
                                    "capability '{name}' can only be used through a call or a field"
                                ),
                            )
                        } else {
                            self.fail(
                                code::UNKNOWN_NAME,
                                format!(
                                    "'{name}' is not a local or a capability of this destination"
                                ),
                            )
                        }
                    }
                }
            }
            Var::Expression(expression) => {
                let symbol = self.enum_symbol(expression)?;
                let index = self.constant(Constant::Bytes(symbol.into_bytes()))?;
                self.emit(Instr::Constant(index))?;
                Ok(())
            }
            _ => self.unsupported("expression"),
        }
    }

    fn enum_symbol(&mut self, expression: &VarExpression) -> Result<String> {
        let Prefix::Name(module_token) = expression.prefix() else {
            return self.unsupported("field access");
        };
        self.position(module_token);
        let module_name = text(module_token);
        let fields: Vec<String> = expression
            .suffixes()
            .filter_map(|suffix| match suffix {
                Suffix::Index(index) => dot_name(index),
                _ => None,
            })
            .collect();
        if fields.len() != 2 || expression.suffixes().count() != 2 {
            return self.unsupported("field access");
        }
        self.feature("enum_symbol")?;
        let Some(module) = self.rules().modules.get(&module_name).cloned() else {
            return self.fail(
                code::UNDECLARED,
                format!("module '{module_name}' is not declared by this destination"),
            );
        };
        let Some(enumeration) = module.enums.get(&fields[0]) else {
            return self.fail(
                code::UNDECLARED,
                format!("enum '{module_name}.{}' is not declared", fields[0]),
            );
        };
        match enumeration.variants.get(&fields[1]) {
            Some(symbol) => Ok(symbol.clone()),
            None => self.fail(
                code::UNDECLARED,
                format!(
                    "variant '{}' is not declared by '{module_name}.{}'",
                    fields[1], fields[0]
                ),
            ),
        }
    }

    /// Classifies a call from its shape and checks the profile feature of its kind. The classification uses the current
    /// scopes, so a local named `error` or `pcall` is an ordinary call. A parenthesized name receiver is the name.
    fn call_kind(&mut self, call: &FunctionCall) -> Result<CallKind> {
        let Some(head) = scan::prefix_name(call.prefix()) else {
            return self.unsupported("call");
        };
        self.position(head);
        let name = text(head);
        let suffixes: Vec<&Suffix> = call.suffixes().collect();
        let local = self.name_access(head)?.is_some();
        let kind = match suffixes.as_slice() {
            [Suffix::Call(Call::AnonymousCall(_))] => {
                if local {
                    CallKind::Lua
                } else if name == "pcall" {
                    CallKind::Pcall
                } else if name == "error" {
                    CallKind::Error
                } else {
                    CallKind::Lua
                }
            }
            [Suffix::Index(index), Suffix::Call(Call::AnonymousCall(_))] => {
                let field = dot_name(index).unwrap_or_default();
                if !local && name == "coroutine" && field == "yield" {
                    CallKind::Yield
                } else if !local && self.rules().modules.contains_key(&name) {
                    CallKind::Native {
                        module: name,
                        op: field,
                    }
                } else {
                    return self.fail(
                        code::UNDECLARED,
                        format!("'{name}.{field}' is not a capability operation"),
                    );
                }
            }
            [Suffix::Call(Call::MethodCall(method))] => match scan::text(method.name()).as_str() {
                scan::SPAWN if !local && self.rules().children.contains_key(&name) => {
                    CallKind::Spawn { slot: name }
                }
                scan::SPAWN => {
                    return self.fail(
                        code::PLACEMENT,
                        format!("'{name}' is not a child slot of this destination"),
                    );
                }
                scan::AWAIT => CallKind::Await,
                other => {
                    return self.fail(
                        code::UNSUPPORTED,
                        format!("method '{other}' is not supported in remote regions"),
                    );
                }
            },
            [.., Suffix::Call(Call::MethodCall(method))]
                if scan::text(method.name()) == scan::AWAIT =>
            {
                CallKind::Await
            }
            _ => return self.unsupported("call"),
        };
        self.feature(kind.feature())?;
        if matches!(kind, CallKind::Await)
            && let Some(Suffix::Call(Call::MethodCall(method))) = suffixes.last()
        {
            let empty = matches!(method.args(), FunctionArgs::Parentheses { arguments, .. } if arguments.is_empty());
            if !empty {
                return self.fail(code::ARITY, "remote await takes no explicit arguments");
            }
        }
        Ok(kind)
    }

    /// Number of results native `module.op` declares, refused when the VM's native ABI cannot carry them.
    fn native_results(&mut self, module: &str, op: &str) -> Result<u8> {
        let Some(operation) = self
            .rules()
            .modules
            .get(module)
            .and_then(|m| m.operations.get(op))
        else {
            return self.fail(
                code::UNDECLARED,
                format!("operation '{module}.{op}' is not declared"),
            );
        };
        let count = operation.results.len();
        match u8::try_from(count) {
            Ok(count) if count <= MAX_NATIVE_RESULTS => Ok(count),
            _ => self.fail(
                code::UNSUPPORTED,
                format!(
                    "'{module}.{op}' returns {count} results; the remote VM carries at most {MAX_NATIVE_RESULTS}"
                ),
            ),
        }
    }

    /// Compiles a call that wants `results` values.
    fn call(&mut self, call: &FunctionCall, results: Results) -> Result<()> {
        let Some(head) = scan::prefix_name(call.prefix()) else {
            return self.unsupported("call");
        };
        self.position(head);
        let kind = self.call_kind(call)?;
        let args = self.call_arguments(call)?;
        let args: Vec<&Expression> = args.iter().collect();
        match kind {
            CallKind::Lua => {
                let access = self.name_access(head)?;
                let Some(access) = access else {
                    return self.fail(
                        code::UNKNOWN_NAME,
                        format!("'{}' is not a callable local", text(head)),
                    );
                };
                self.load(access)?;
                let shape = self.expand(&args)?;
                let fixed = self.count_u8(shape.fixed, "arguments")?;
                let wanted = match results {
                    Results::Fixed(count) => self.count_u8(count, "results")?,
                    Results::Multi => RESULTS_MULTI,
                };
                if shape.dynamic {
                    self.emit(Instr::CallMulti {
                        fixed,
                        results: wanted,
                    })?;
                } else {
                    self.emit(Instr::Call {
                        argc: fixed,
                        results: wanted,
                    })?;
                }
                Ok(())
            }
            CallKind::Native { module, op } => {
                let rules = self.rules().clone();
                let Some(module_rules) = rules.modules.get(&module) else {
                    return self.fail(
                        code::UNDECLARED,
                        format!("module '{module}' is not declared"),
                    );
                };
                let Some(operation) = module_rules.operations.get(&op) else {
                    return self.fail(
                        code::UNDECLARED,
                        format!("operation '{module}.{op}' is not declared"),
                    );
                };
                if operation.arity != args.len() {
                    return self.fail(
                        code::ARITY,
                        format!(
                            "'{module}.{op}' takes {} arguments, not {}",
                            operation.arity,
                            args.len()
                        ),
                    );
                }
                let produced = self.native_results(&module, &op)?;
                // A native has fixed parameters, so each argument is adjusted to one value.
                for arg in &args {
                    self.expr(arg)?;
                }
                let index = self.native(NativeRef {
                    module: module_rules.id as u16,
                    operation: operation.id as u16,
                    argc: args.len() as u8,
                    results: produced,
                })?;
                self.emit(Instr::Native(index))?;
                self.adjust(usize::from(produced), results)
            }
            CallKind::Spawn { slot } => {
                let Some((function, rest)) = scan::spawn_call(call) else {
                    return self.fail(
                        code::SPAWN_BODY,
                        format!("spawn of '{slot}' needs a function literal as its body"),
                    );
                };
                self.spawn_value(&slot, function, &rest)?;
                self.adjust(1, results)
            }
            CallKind::Await => {
                self.await_tuple(call)?;
                self.tuple_result(results)
            }
            CallKind::Yield => {
                let shape = self.expand(&args)?;
                let fixed = self.count_u8(shape.fixed, "yielded values")?;
                if shape.dynamic {
                    self.emit(Instr::YieldMulti(fixed))?;
                } else {
                    self.check_tuple_items(shape.fixed, "a yield")?;
                    self.emit(Instr::Yield(fixed))?;
                }
                self.tuple_result(results)
            }
            CallKind::Pcall => {
                if args.is_empty() {
                    self.missing_pcall_argument()?;
                } else if args.len() == 1 {
                    match self.tail(args[0])? {
                        Tail::Dynamic => {
                            self.expand(&args)?;
                            self.emit(Instr::PcallList)?;
                        }
                        Tail::Fixed(count) => {
                            self.multi(args[0], count)?;
                            if count == 0 {
                                self.missing_pcall_argument()?;
                            } else {
                                let argc = self.count_u8(count - 1, "arguments")?;
                                self.emit(Instr::Pcall(argc))?;
                            }
                        }
                        Tail::Single => {
                            self.expr(args[0])?;
                            self.emit(Instr::Pcall(0))?;
                        }
                    }
                } else {
                    self.expr(args[0])?;
                    let shape = self.expand(&args[1..])?;
                    let fixed = self.count_u8(shape.fixed, "arguments")?;
                    if shape.dynamic {
                        self.emit(Instr::PcallMulti(fixed))?;
                    } else {
                        self.emit(Instr::Pcall(fixed))?;
                    }
                }
                self.tuple_result(results)
            }
            CallKind::Error => {
                if args.len() != 1 {
                    return self.fail(
                        code::UNSUPPORTED,
                        "error takes exactly one value in remote regions",
                    );
                }
                self.expr(args[0])?;
                self.emit(Instr::Raise)?;
                // Unreachable: it only keeps the stack shape the caller expects after the call.
                self.adjust(0, results)
            }
        }
    }

    /// An empty pcall argument list raises before a protected call is established.
    fn missing_pcall_argument(&mut self) -> Result<()> {
        let message = self.constant(Constant::Bytes(
            b"bad argument #1 to 'pcall' (value expected)".to_vec(),
        ))?;
        self.emit(Instr::Constant(message))?;
        self.emit(Instr::Raise)?;
        // Unreachable placeholder for the tuple that the caller's result adjustment expects.
        self.emit(Instr::Nil)?;
        Ok(())
    }

    /// The arguments of a call, with Lua's string shorthand `f "text"` as one string argument. The table shorthand
    /// `f { ... }` is refused, never compiled as a call without arguments.
    fn call_arguments(&mut self, call: &FunctionCall) -> Result<Vec<Expression>> {
        let arguments = call.suffixes().last().and_then(|suffix| match suffix {
            Suffix::Call(Call::AnonymousCall(args)) => Some(args.as_ref()),
            Suffix::Call(Call::MethodCall(method)) => Some(method.args()),
            _ => None,
        });
        match arguments {
            Some(FunctionArgs::Parentheses { arguments, .. }) => {
                Ok(arguments.iter().cloned().collect())
            }
            Some(FunctionArgs::String(token)) => Ok(vec![Expression::String(token.clone())]),
            Some(FunctionArgs::TableConstructor(_)) => self.unsupported("table_constructor"),
            Some(_) => self.unsupported("call"),
            None => Ok(Vec::new()),
        }
    }

    /// Leaves `results` values from a call that produced exactly `produced` values.
    fn adjust(&mut self, produced: usize, results: Results) -> Result<()> {
        let Results::Fixed(want) = results else {
            return self.fail(
                code::INTERNAL,
                "a fixed-result call was asked for all values",
            );
        };
        for _ in want..produced {
            self.emit(Instr::Pop)?;
        }
        for _ in produced..want {
            self.emit(Instr::Nil)?;
        }
        Ok(())
    }

    /// Pushes the tuple produced by `await` on the receiver of `call`.
    fn await_tuple(&mut self, call: &FunctionCall) -> Result<()> {
        let suffixes: Vec<&Suffix> = call.suffixes().collect();
        let Some(head) = scan::prefix_name(call.prefix()) else {
            return self.unsupported("await");
        };
        match suffixes.as_slice() {
            [Suffix::Call(Call::MethodCall(method))]
                if scan::text(method.name()) == scan::AWAIT =>
            {
                let access = self.name_access(head)?;
                let Some(access) = access else {
                    return self.fail(code::UNKNOWN_NAME, "await needs a task held in a local");
                };
                self.load(access)?;
            }
            [
                Suffix::Call(Call::MethodCall(spawn)),
                Suffix::Call(Call::MethodCall(await_method)),
            ] if scan::text(spawn.name()) == scan::SPAWN
                && scan::text(await_method.name()) == scan::AWAIT =>
            {
                let slot = text(head);
                if !self.rules().children.contains_key(&slot) || self.name_access(head)?.is_some() {
                    return self.fail(
                        code::PLACEMENT,
                        format!("'{slot}' is not a child slot of this destination"),
                    );
                }
                let Some((function, rest)) = spawn_from_method(spawn) else {
                    return self.fail(
                        code::SPAWN_BODY,
                        format!("spawn of '{slot}' needs a function literal as its body"),
                    );
                };
                self.spawn_value(&slot, function, &rest)?;
            }
            _ => return self.unsupported("await"),
        }
        self.emit(Instr::Await)?;
        Ok(())
    }

    /// Pushes the task handle of a spawn of child `slot` with body `function` and initial arguments `rest`.
    fn spawn_value(
        &mut self,
        slot: &str,
        function: &AnonymousFunction,
        rest: &[&Expression],
    ) -> Result<()> {
        self.feature("spawn")?;
        let rules = self.rules().clone();
        let Some(child) = rules.children.get(slot) else {
            return self.fail(
                code::PLACEMENT,
                format!("'{slot}' is not a child slot of this destination"),
            );
        };
        let destination = child.child.clone();
        let link = self.unit().children.len();
        let max_children = self.limits().max_children;
        if link >= max_children {
            return self.fail(
                code::LIMIT,
                format!(
                    "region spawns more than the destination's limit of {max_children} children"
                ),
            );
        }
        self.position(function.function_token());
        let (bytes, captures) = self.entry(&destination, function.body())?;
        let child_items = self
            .graph
            .checker_rules(&destination)
            .map_or(usize::MAX, |r| r.limits.max_tuple_items as usize);
        if captures.len() > child_items {
            return self.fail(
                code::LIMIT,
                format!(
                    "the spawn captures {} values; the child's tuples hold at most {child_items}",
                    captures.len()
                ),
            );
        }
        let digest: [u8; DIGEST_LEN] = Sha256::digest(&bytes).into();
        self.unit_mut().children.push(ChildLink {
            slot: child.slot_id,
            digest,
            entry: 0,
            bytes: bytes.into(),
        });
        let capture_count = u16::try_from(captures.len())
            .map_or_else(|_| self.fail(code::LIMIT, "too many captures"), Ok)?;
        let dynamic = match rest.last() {
            Some(last) => self.tail(last)? == Tail::Dynamic,
            None => false,
        };
        if dynamic {
            let shape = self.expand(rest)?;
            if shape.fixed > child_items {
                return self.fail(
                    code::LIMIT,
                    "the fixed spawn arguments exceed the child's tuple limit",
                );
            }
            let fixed = self.count_u8(shape.fixed, "spawn arguments")?;
            if captures.is_empty() {
                self.emit(Instr::SpawnMulti {
                    child: link as u16,
                    fixed,
                    captures: 0,
                })?;
            } else {
                // Preserve the exact argument list while capture loads observe all argument effects.
                self.emit(Instr::PackMulti(fixed))?;
                let arguments = self.slot()?;
                self.emit(Instr::SetLocal(arguments))?;
                for name in &captures {
                    let access = self.resolve_for_capture(name)?;
                    self.load(access)?;
                }
                self.emit(Instr::GetLocal(arguments))?;
                self.emit(Instr::UnpackMulti)?;
                self.emit(Instr::SpawnMulti {
                    child: link as u16,
                    fixed: 0,
                    captures: capture_count,
                })?;
            }
        } else {
            let shape = self.expand(rest)?;
            if shape.fixed > child_items {
                return self.fail(
                    code::LIMIT,
                    format!(
                        "the spawn passes {} arguments; the child's tuples hold at most {child_items}",
                        shape.fixed
                    ),
                );
            }
            let argc = self.count_u8(shape.fixed, "spawn arguments")?;
            for name in &captures {
                let access = self.resolve_for_capture(name)?;
                self.load(access)?;
            }
            self.emit(Instr::Spawn {
                child: link as u16,
                argc,
                captures: capture_count,
            })?;
        }
        Ok(())
    }

    /// Resolves a capture name in the current frame for a spawn snapshot.
    fn resolve_for_capture(&mut self, name: &str) -> Result<Access> {
        let top = self.frames.len() - 1;
        match self.resolve(top, name)? {
            Some(access) => Ok(access),
            None => self.fail(
                code::INTERNAL,
                format!("capture '{name}' is not visible at the spawn"),
            ),
        }
    }

    /// Leaves `results` values from a tuple on the stack.
    fn tuple_result(&mut self, results: Results) -> Result<()> {
        match results {
            Results::Fixed(0) => {
                self.emit(Instr::Pop)?;
            }
            Results::Fixed(count) => {
                let count = self.count_u8(count, "results")?;
                self.emit(Instr::Unpack(count))?;
            }
            Results::Multi => {
                self.emit(Instr::UnpackMulti)?;
            }
        }
        Ok(())
    }
}

fn spawn_from_method(
    method: &full_moon::ast::MethodCall,
) -> Option<(&AnonymousFunction, Vec<&Expression>)> {
    let FunctionArgs::Parentheses { arguments, .. } = method.args() else {
        return None;
    };
    let mut args = arguments.iter();
    let Some(Expression::Function(function)) = args.next() else {
        return None;
    };
    Some((function, args.collect()))
}

fn dot_name(index: &Index) -> Option<String> {
    match index {
        Index::Dot { name, .. } => Some(text(name)),
        _ => None,
    }
}

fn number_constant(token: &TokenReference) -> Option<Constant> {
    let TokenType::Number { text } = token.token_type() else {
        return None;
    };
    let raw = text.as_str();
    if raw.starts_with("0x") || raw.starts_with("0X") {
        return None;
    }
    if raw.contains(['.', 'e', 'E']) {
        let value: f64 = raw.parse().ok()?;
        return value.is_finite().then_some(Constant::Float(value));
    }
    match raw.parse::<i64>() {
        Ok(value) => Some(Constant::Integer(value)),
        Err(_) => {
            let value: f64 = raw.parse().ok()?;
            value.is_finite().then_some(Constant::Float(value))
        }
    }
}

/// The bytes of a string literal token: a long bracket string as written, a short string with its escapes decoded.
fn string_constant(token: &TokenReference) -> std::result::Result<Vec<u8>, String> {
    let TokenType::StringLiteral {
        literal,
        quote_type,
        ..
    } = token.token_type()
    else {
        return Err("string token has the wrong type".to_string());
    };
    match quote_type {
        StringLiteralQuoteType::Brackets => Ok(decode_long_string(literal.as_str())),
        _ => decode_short_string(literal.as_str()),
    }
}

/// Decodes the body of a long bracket string as Lua 5.4 does: no escapes are processed, a newline right after the
/// opening bracket is dropped, and every newline sequence (`\n`, `\r`, `\r\n` or `\n\r`) becomes `\n`.
pub(crate) fn decode_long_string(body: &str) -> Vec<u8> {
    let bytes = body.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = newline_length(bytes, 0);
    while index < bytes.len() {
        let newline = newline_length(bytes, index);
        if newline > 0 {
            out.push(b'\n');
            index += newline;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    out
}

/// Length of the newline sequence at `index`: 2 for `\r\n` or `\n\r`, 1 for a lone `\n` or `\r`, 0 otherwise.
fn newline_length(bytes: &[u8], index: usize) -> usize {
    match (bytes.get(index), bytes.get(index + 1)) {
        (Some(b'\n'), Some(b'\r')) | (Some(b'\r'), Some(b'\n')) => 2,
        (Some(b'\n' | b'\r'), _) => 1,
        _ => 0,
    }
}

/// Decodes the escapes of a short Lua 5.4 string literal. Every Lua 5.4 escape is supported, and a malformed one is
/// refused, as Lua refuses it.
pub(crate) fn decode_short_string(raw: &str) -> std::result::Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(raw.len());
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            out.push(bytes[index]);
            index += 1;
            continue;
        }
        index += 1;
        let Some(&escape) = bytes.get(index) else {
            return Err("string ends in a backslash".to_string());
        };
        match escape {
            b'a' => out.push(0x07),
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(0x0b),
            b'\\' => out.push(b'\\'),
            b'"' => out.push(b'"'),
            b'\'' => out.push(b'\''),
            b'\n' | b'\r' => {
                out.push(b'\n');
                index += newline_length(bytes, index);
                continue;
            }
            b'z' => {
                index += 1;
                while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
                    index += 1;
                }
                continue;
            }
            b'x' => {
                let digits = bytes.get(index + 1..index + 3).filter(|d| d.len() == 2);
                let value = digits
                    .and_then(|d| std::str::from_utf8(d).ok())
                    .and_then(|d| u8::from_str_radix(d, 16).ok())
                    .ok_or_else(|| "hexadecimal escape needs exactly two digits".to_string())?;
                out.push(value);
                index += 3;
                continue;
            }
            b'u' => {
                let rest = &bytes[index + 1..];
                let Some(close) = rest.iter().position(|&b| b == b'}') else {
                    return Err("UTF-8 escape is missing '}'".to_string());
                };
                if rest.first() != Some(&b'{') || close < 2 {
                    return Err("UTF-8 escape needs '{' and at least one digit".to_string());
                }
                let digits = std::str::from_utf8(&rest[1..close]).map_err(|e| e.to_string())?;
                let value = u32::from_str_radix(digits, 16)
                    .ok()
                    .filter(|&v| v <= 0x7FFF_FFFF)
                    .ok_or_else(|| "UTF-8 escape value is too large".to_string())?;
                encode_utf8_extended(value, &mut out);
                index += 1 + close + 1;
                continue;
            }
            b'0'..=b'9' => {
                let mut value: u32 = u32::from(escape - b'0');
                let mut digits = 1;
                while digits < 3 && bytes.get(index + 1).is_some_and(u8::is_ascii_digit) {
                    index += 1;
                    value = value * 10 + u32::from(bytes[index] - b'0');
                    digits += 1;
                }
                let byte = u8::try_from(value)
                    .map_err(|_| "decimal escape is out of range".to_string())?;
                out.push(byte);
            }
            other => {
                return Err(format!(
                    "escape '\\{}' is not a valid Lua escape",
                    other as char
                ));
            }
        }
        index += 1;
    }
    Ok(out)
}

/// Encodes `value` the way Lua 5.4's `\u{...}` escape does: UTF-8 extended to 31 bits, with up to six bytes.
fn encode_utf8_extended(value: u32, out: &mut Vec<u8>) {
    if value < 0x80 {
        out.push(value as u8);
        return;
    }
    let mut buffer = [0u8; 8];
    let mut first_max: u32 = 0x3f;
    let mut n = 1;
    let mut x = value;
    while x > first_max {
        buffer[8 - n] = 0x80 | (x & 0x3f) as u8;
        n += 1;
        x >>= 6;
        first_max >>= 1;
    }
    buffer[8 - n] = ((!first_max << 1) as u8) | x as u8;
    out.extend_from_slice(&buffer[8 - n..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_strings_drop_the_first_newline_and_normalize_newlines_without_escapes() {
        assert_eq!(decode_long_string("\nabc"), b"abc");
        assert_eq!(decode_long_string("\r\nabc"), b"abc");
        assert_eq!(decode_long_string("\n\rabc"), b"abc");
        assert_eq!(decode_long_string("\n\nabc"), b"\nabc");
        assert_eq!(
            decode_long_string("a\\nb"),
            b"a\\nb",
            "a backslash is literal"
        );
        assert_eq!(decode_long_string("a\r\nb\rc\n\rd"), b"a\nb\nc\nd");
        assert_eq!(decode_long_string(""), b"");
    }

    #[test]
    fn short_strings_decode_every_lua_escape() {
        assert_eq!(
            decode_short_string(r#"\a\b\f\n\r\t\v\\\"\'"#).expect("valid"),
            b"\x07\x08\x0c\n\r\t\x0b\\\"'"
        );
        assert_eq!(decode_short_string(r"\x41\x7a").expect("valid"), b"Az");
        assert_eq!(decode_short_string(r"\65\066\0").expect("valid"), b"AB\0");
        assert_eq!(
            decode_short_string(r"\u{48}\u{e9}").expect("valid"),
            "Hé".as_bytes()
        );
        assert_eq!(
            decode_short_string(r"\u{7FFFFFFF}").expect("valid"),
            [0xfd, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf]
        );
        assert_eq!(decode_short_string("a\\z  \n  b").expect("valid"), b"ab");
        assert_eq!(decode_short_string("a\\\r\nb").expect("valid"), b"a\nb");
        assert!(decode_short_string(r"\256").is_err());
        assert!(decode_short_string(r"\x4").is_err());
        assert!(decode_short_string(r"\q").is_err());
        assert!(decode_short_string(r"\u{80000000}").is_err());
    }
}
