//! Lexical scans over the parsed workflow.
//!
//! Three scans run before code generation. `spawn_bodies` finds the function literals that are spawn bodies, which are
//! snapshotted rather than closed over. `cells` finds the local declarations that a non-spawn closure references, so
//! they must live in a shared cell. `host_sites` finds the placements that the host runs through registration.
//! Declarations are identified by the byte offset of their name token, which is the same key in every scan.

use std::collections::BTreeSet;
use std::collections::HashSet;

use full_moon::ast::{
    AnonymousFunction, Assignment, Ast, Call, Expression, FunctionArgs, FunctionBody, FunctionCall,
    FunctionDeclaration, GenericFor, LocalAssignment, LocalFunction, NumericFor, Parameter, Prefix,
    Repeat, Suffix, Var,
};
use full_moon::tokenizer::TokenReference;
use full_moon::visitors::Visitor;

use crate::diag::{Diagnostic, code};

/// The method that turns a function literal into a remote spawn.
pub(crate) const SPAWN: &str = "spawn";

/// The method that waits for a child's return value.
pub(crate) const AWAIT: &str = "await";

/// Byte offset of a token's start.
pub(crate) fn start(token: &TokenReference) -> usize {
    token.start_position().bytes()
}

/// Name of an identifier token.
pub(crate) fn text(token: &TokenReference) -> String {
    token.token().to_string()
}

/// If `call` is `receiver:method(function ... end, ...)`, returns the function literal and the remaining arguments.
pub(crate) fn spawn_call(call: &FunctionCall) -> Option<(&AnonymousFunction, Vec<&Expression>)> {
    let mut suffixes = call.suffixes();
    let Some(Suffix::Call(Call::MethodCall(method))) = suffixes.next() else {
        return None;
    };
    if suffixes.next().is_some() || text(method.name()) != SPAWN {
        return None;
    }
    let FunctionArgs::Parentheses { arguments, .. } = method.args() else {
        return None;
    };
    let mut args = arguments.iter();
    let Some(Expression::Function(function)) = args.next() else {
        return None;
    };
    Some((function, args.collect()))
}

/// The name a call's prefix stands for, looking through parentheses: `f`, `(f)` and `((f))` all name `f`.
pub(crate) fn prefix_name(prefix: &Prefix) -> Option<&TokenReference> {
    match prefix {
        Prefix::Name(token) => Some(token),
        Prefix::Expression(expression) => expression_name(expression),
        _ => None,
    }
}

/// The name an expression stands for, looking through parentheses.
pub(crate) fn expression_name(expression: &Expression) -> Option<&TokenReference> {
    match expression {
        Expression::Var(Var::Name(token)) => Some(token),
        Expression::Parentheses { expression, .. } => expression_name(expression),
        _ => None,
    }
}

/// The byte range of a function literal, from `function` to its `end`.
pub(crate) fn literal_range(function: &AnonymousFunction) -> (usize, usize) {
    (
        start(function.function_token()),
        function.body().end_token().end_position().bytes(),
    )
}

/// Start offsets of every function literal that is the body of a `:spawn` call.
pub(crate) fn spawn_bodies(ast: &Ast) -> HashSet<usize> {
    #[derive(Default)]
    struct Collect(HashSet<usize>);
    impl Visitor for Collect {
        fn visit_function_call(&mut self, call: &FunctionCall) {
            if let Some((function, _)) = spawn_call(call) {
                self.0.insert(start(function.function_token()));
            }
        }
    }
    let mut collect = Collect::default();
    collect.visit_ast(ast);
    collect.0
}

/// Declarations, by name-token offset, that a non-spawn closure references across its boundary.
pub(crate) fn cells(ast: &Ast) -> HashSet<usize> {
    let spawn = spawn_bodies(ast);
    let mut scan = CellScan {
        spawn,
        frames: vec![false],
        scopes: vec![Vec::new()],
        repeat_blocks: HashSet::new(),
        cells: HashSet::new(),
    };
    scan.visit_ast(ast);
    scan.cells
}

struct CellScan {
    spawn: HashSet<usize>,
    /// One entry per function, the host chunk first. `true` marks a closure frame.
    frames: Vec<bool>,
    /// Scopes of `(name, declaration offset, frame index)`.
    scopes: Vec<Vec<(String, usize, usize)>>,
    cells: HashSet<usize>,
    repeat_blocks: HashSet<usize>,
}

impl CellScan {
    fn declare(&mut self, token: &TokenReference) {
        let frame = self.frames.len() - 1;
        if let Some(scope) = self.scopes.last_mut() {
            scope.push((text(token), start(token), frame));
        }
    }

    fn reference(&mut self, name: &str) {
        let current = self.frames.len() - 1;
        let found = self
            .scopes
            .iter()
            .rev()
            .flat_map(|scope| scope.iter().rev())
            .find(|(n, _, _)| n == name)
            .cloned();
        let Some((_, offset, depth)) = found else {
            return;
        };
        if self.frames[depth + 1..=current]
            .iter()
            .any(|closure| *closure)
        {
            self.cells.insert(offset);
        }
    }
}

impl Visitor for CellScan {
    fn visit_block(&mut self, _: &full_moon::ast::Block) {
        self.scopes.push(Vec::new());
    }
    fn visit_block_end(&mut self, block: &full_moon::ast::Block) {
        if !self.repeat_blocks.contains(&(block as *const _ as usize)) {
            self.scopes.pop();
        }
    }
    fn visit_repeat(&mut self, node: &Repeat) {
        self.repeat_blocks.insert(node.block() as *const _ as usize);
    }
    fn visit_repeat_end(&mut self, node: &Repeat) {
        self.repeat_blocks
            .remove(&(node.block() as *const _ as usize));
        self.scopes.pop();
    }
    fn visit_local_assignment_end(&mut self, node: &LocalAssignment) {
        for name in node.names() {
            self.declare(name);
        }
    }
    fn visit_local_function(&mut self, node: &LocalFunction) {
        self.declare(node.name());
        self.frames.push(true);
    }
    fn visit_local_function_end(&mut self, _: &LocalFunction) {
        self.frames.pop();
    }
    fn visit_anonymous_function(&mut self, node: &AnonymousFunction) {
        let closure = !self.spawn.contains(&start(node.function_token()));
        self.frames.push(closure);
    }
    fn visit_anonymous_function_end(&mut self, _: &AnonymousFunction) {
        self.frames.pop();
    }
    fn visit_function_body(&mut self, node: &FunctionBody) {
        self.scopes.push(Vec::new());
        for parameter in node.parameters() {
            if let Parameter::Name(token) = parameter {
                self.declare(token);
            }
        }
    }
    fn visit_function_body_end(&mut self, _: &FunctionBody) {
        self.scopes.pop();
    }
    fn visit_numeric_for(&mut self, node: &NumericFor) {
        self.scopes.push(Vec::new());
        self.declare(node.index_variable());
    }
    fn visit_numeric_for_end(&mut self, _: &NumericFor) {
        self.scopes.pop();
    }
    fn visit_prefix(&mut self, node: &Prefix) {
        if let Prefix::Name(token) = node {
            self.reference(&text(token));
        }
    }
    fn visit_var(&mut self, node: &Var) {
        if let Var::Name(token) = node {
            self.reference(&text(token));
        }
    }
}

/// A placement the host performs: `slot:spawn(function ... end, ...)` where `slot` is a child of the host contract, or a
/// local alias of one, possibly in parentheses.
pub(crate) struct HostSiteRef<'a> {
    /// Child slot name.
    pub slot: String,
    /// The function literal that is the spawn body.
    pub function: &'a AnonymousFunction,
    /// Byte range of the function literal.
    pub start: usize,
    /// End of the function literal.
    pub end: usize,
    /// Host locals visible at the site, the candidates for capture. Aliases of child slots are not values, so they are
    /// not candidates.
    pub visible: BTreeSet<String>,
}

/// What a host name is bound to.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Binding {
    /// An ordinary host local.
    Plain,
    /// A local that holds a child slot, as in `local r = remote`.
    Alias(String),
}

/// Finds host placements and hands each to `on_site` at the point the scan reaches it, and refuses every other use of a
/// child slot.
///
/// A child slot name that no host local shadows is a capability. So is a local declared as `local r = remote`, or as an
/// alias of another alias, with or without parentheses. A capability may only be the receiver of `:spawn(function ...
/// end, ...)` or the whole initializer of such an alias. Any other use, such as passing it, returning it, storing it,
/// calling another method on it or assigning to it, would let host code reach a slot the checker cannot follow, so it is
/// reported. Ordinary objects that have their own `spawn` method are not capabilities and are left alone. Spawns nested
/// inside a placed body belong to that body and are not host sites. Returns the problems the scan itself found;
/// `on_site` reports its own through its result.
pub(crate) fn host_sites<F>(ast: &Ast, slots: &BTreeSet<String>, on_site: F) -> Vec<Diagnostic>
where
    F: for<'a> FnMut(HostSiteRef<'a>) -> std::result::Result<(), Diagnostic>,
{
    let mut scan = HostScan {
        slots: slots.clone(),
        scopes: vec![Vec::new()],
        bodies: Vec::new(),
        allowed: HashSet::new(),
        pending: Vec::new(),
        repeat_blocks: HashSet::new(),
        diagnostics: Vec::new(),
        on_site,
    };
    scan.visit_ast(ast);
    scan.diagnostics
}

struct HostScan<F> {
    slots: BTreeSet<String>,
    scopes: Vec<Vec<(String, Binding)>>,
    bodies: Vec<(usize, usize)>,
    /// Token offsets of capability references that are a placement receiver or an alias initializer.
    allowed: HashSet<usize>,
    /// Names a loop declares for its body block, which must not be visible in the loop's own header expressions.
    pending: Vec<Vec<(String, Binding)>>,
    /// Repeat body blocks remain in scope until after their conditions.
    repeat_blocks: HashSet<usize>,
    diagnostics: Vec<Diagnostic>,
    on_site: F,
}

impl<F> HostScan<F> {
    fn declare(&mut self, token: &TokenReference, binding: Binding) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.push((text(token), binding));
        }
    }

    /// The child slot `name` stands for here, if it is a capability.
    fn capability(&self, name: &str) -> Option<String> {
        let bound = self
            .scopes
            .iter()
            .rev()
            .flat_map(|scope| scope.iter().rev())
            .find(|(n, _)| n == name);
        match bound {
            Some((_, Binding::Alias(slot))) => Some(slot.clone()),
            Some((_, Binding::Plain)) => None,
            None => self.slots.contains(name).then(|| name.to_string()),
        }
    }

    fn inside_placed_body(&self, offset: usize) -> bool {
        self.bodies.iter().any(|(s, e)| offset >= *s && offset < *e)
    }

    fn visible(&self) -> BTreeSet<String> {
        let mut visible = BTreeSet::new();
        for scope in &self.scopes {
            for (name, binding) in scope {
                match binding {
                    Binding::Plain => {
                        visible.insert(name.clone());
                    }
                    Binding::Alias(_) => {
                        visible.remove(name);
                    }
                }
            }
        }
        visible
    }

    fn escape(&mut self, token: &TokenReference, slot: &str, what: &str) {
        let name = text(token);
        let shown = if name == slot {
            format!("child slot '{slot}'")
        } else {
            format!("'{name}', an alias of child slot '{slot}',")
        };
        self.diagnostics.push(Diagnostic::new(
            code::CAPABILITY_ESCAPE,
            token.start_position().line(),
            token.start_position().character(),
            format!(
                "{shown} {what}; a child slot may only be used as `{name}:spawn(function ... end, ...)` or bound with `local name = {name}`"
            ),
        ));
    }

    /// Checks one name reference in host code.
    fn reference(&mut self, token: &TokenReference) {
        let offset = start(token);
        if self.allowed.contains(&offset) || self.inside_placed_body(offset) {
            return;
        }
        if let Some(slot) = self.capability(&text(token)) {
            self.escape(token, &slot, "is used as a value");
        }
    }
}

impl<F> Visitor for HostScan<F>
where
    F: for<'a> FnMut(HostSiteRef<'a>) -> std::result::Result<(), Diagnostic>,
{
    fn visit_block(&mut self, _: &full_moon::ast::Block) {
        let names = self.pending.pop().unwrap_or_default();
        self.scopes.push(names);
    }
    fn visit_block_end(&mut self, block: &full_moon::ast::Block) {
        if !self.repeat_blocks.contains(&(block as *const _ as usize)) {
            self.scopes.pop();
        }
    }
    fn visit_repeat(&mut self, node: &Repeat) {
        self.repeat_blocks.insert(node.block() as *const _ as usize);
    }
    fn visit_repeat_end(&mut self, node: &Repeat) {
        self.repeat_blocks
            .remove(&(node.block() as *const _ as usize));
        self.scopes.pop();
    }
    fn visit_local_assignment(&mut self, node: &LocalAssignment) {
        if self.inside_placed_body(start(node.local_token())) {
            return;
        }
        // An initializer that is a capability makes its name an alias. It is checked before the names are declared, so
        // `local remote = remote` binds the new local to the slot.
        for expression in node.expressions() {
            if let Some(token) = expression_name(expression)
                && self.capability(&text(token)).is_some()
            {
                self.allowed.insert(start(token));
            }
        }
    }
    fn visit_local_assignment_end(&mut self, node: &LocalAssignment) {
        if self.inside_placed_body(start(node.local_token())) {
            return;
        }
        let expressions: Vec<&Expression> = node.expressions().iter().collect();
        let bindings: Vec<Binding> = node
            .names()
            .iter()
            .enumerate()
            .map(|(index, _)| {
                expressions
                    .get(index)
                    .and_then(|expression| expression_name(expression))
                    .and_then(|token| {
                        // Resolve against the scope before this declaration.
                        self.capability(&text(token))
                    })
                    .map_or(Binding::Plain, Binding::Alias)
            })
            .collect();
        for (name, binding) in node.names().iter().zip(bindings) {
            self.declare(name, binding);
        }
    }
    fn visit_local_function(&mut self, node: &LocalFunction) {
        self.declare(node.name(), Binding::Plain);
    }
    fn visit_function_body(&mut self, node: &FunctionBody) {
        // The body's own block takes this empty marker, so a loop whose header contains a function literal still hands
        // its loop names to its own body block.
        self.pending.push(Vec::new());
        self.scopes.push(Vec::new());
        for parameter in node.parameters() {
            if let Parameter::Name(token) = parameter {
                self.declare(token, Binding::Plain);
            }
        }
    }
    fn visit_function_body_end(&mut self, _: &FunctionBody) {
        self.scopes.pop();
    }
    fn visit_numeric_for(&mut self, node: &NumericFor) {
        self.pending
            .push(vec![(text(node.index_variable()), Binding::Plain)]);
    }
    fn visit_generic_for(&mut self, node: &GenericFor) {
        self.pending.push(
            node.names()
                .iter()
                .map(|name| (text(name), Binding::Plain))
                .collect(),
        );
    }
    fn visit_assignment(&mut self, node: &Assignment) {
        for var in node.variables() {
            if let Var::Name(token) = var
                && !self.inside_placed_body(start(token))
                && let Some(slot) = self.capability(&text(token))
            {
                self.allowed.insert(start(token));
                self.escape(token, &slot, "cannot be assigned");
            }
        }
    }
    fn visit_function_declaration(&mut self, node: &FunctionDeclaration) {
        let Some(token) = node.name().names().iter().next() else {
            return;
        };
        if !self.inside_placed_body(start(token))
            && let Some(slot) = self.capability(&text(token))
        {
            self.escape(token, &slot, "cannot be given functions or fields");
        }
    }
    fn visit_prefix(&mut self, node: &Prefix) {
        if let Prefix::Name(token) = node {
            self.reference(token);
        }
    }
    fn visit_var(&mut self, node: &Var) {
        if let Var::Name(token) = node {
            self.reference(token);
        }
    }
    fn visit_function_call(&mut self, call: &FunctionCall) {
        let Some(receiver) = prefix_name(call.prefix()) else {
            return;
        };
        let offset = start(receiver);
        if self.inside_placed_body(offset) {
            return;
        }
        let Some(slot) = self.capability(&text(receiver)) else {
            return;
        };
        let line = receiver.start_position().line();
        let column = receiver.start_position().character();
        let Some(Suffix::Call(Call::MethodCall(method))) = call.suffixes().next() else {
            // Indexing or calling a slot reaches the facade outside the placement form; `reference` reports it.
            return;
        };
        if text(method.name()) != SPAWN {
            self.allowed.insert(offset);
            self.escape(
                receiver,
                &slot,
                &format!("has no method '{}'", text(method.name())),
            );
            return;
        }
        self.allowed.insert(offset);
        let first = match method.args() {
            FunctionArgs::Parentheses { arguments, .. } => arguments.iter().next(),
            _ => None,
        };
        match first {
            Some(Expression::Function(function)) => {
                let (from, to) = literal_range(function);
                self.bodies.push((from, to));
                let site = HostSiteRef {
                    slot,
                    function,
                    start: from,
                    end: to,
                    visible: self.visible(),
                };
                if let Err(diagnostic) = (self.on_site)(site) {
                    self.diagnostics.push(diagnostic);
                }
            }
            _ => self.diagnostics.push(Diagnostic::new(
                code::SPAWN_BODY,
                line,
                column,
                format!("spawn of child '{slot}' needs a function literal as its body"),
            )),
        }
    }
}
