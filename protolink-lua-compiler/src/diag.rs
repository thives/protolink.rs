//! Compiler diagnostics. Each carries a stable code and the source position of the construct that caused it.

use std::fmt;

/// One problem in a workflow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostic {
    /// Stable code, such as `LUA010`.
    pub code: &'static str,
    /// One-based line.
    pub line: usize,
    /// One-based character column.
    pub column: usize,
    /// What is wrong, in terms of the profile and the contract.
    pub message: String,
}

impl Diagnostic {
    pub(crate) fn new(
        code: &'static str,
        line: usize,
        column: usize,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            line,
            column,
            message: message.into(),
        }
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}: {}: {}",
            self.line, self.column, self.code, self.message
        )
    }
}

impl std::error::Error for Diagnostic {}

/// Stable codes.
pub mod code {
    /// The workflow does not parse.
    pub const PARSE: &str = "LUA001";
    /// A construct is not allowed by the destination's language profile.
    pub const UNSUPPORTED: &str = "LUA010";
    /// A name in a remote region is neither local nor a capability of its destination.
    pub const UNKNOWN_NAME: &str = "LUA011";
    /// A capability was used as a plain value.
    pub const CAPABILITY_VALUE: &str = "LUA012";
    /// A spawn target is not a child slot of the destination, or is dynamic.
    pub const PLACEMENT: &str = "LUA013";
    /// A native call has the wrong number of arguments.
    pub const ARITY: &str = "LUA014";
    /// A module operation, enum or variant is not declared.
    pub const UNDECLARED: &str = "LUA015";
    /// An assignment targets something that is not a remote local.
    pub const ASSIGNMENT: &str = "LUA016";
    /// `break` outside a loop.
    pub const BREAK: &str = "LUA017";
    /// A bounded limit of the artifact format is exceeded.
    pub const LIMIT: &str = "LUA018";
    /// A literal cannot be represented in the remote profile.
    pub const LITERAL: &str = "LUA019";
    /// Spawn bodies must be function literals.
    pub const SPAWN_BODY: &str = "LUA020";
    /// Host code uses a child slot, or an alias of one, in a way the checker cannot follow.
    pub const CAPABILITY_ESCAPE: &str = "LUA021";
    /// An internal invariant failed. This is a compiler bug, not a workflow error.
    pub const INTERNAL: &str = "LUA099";
}
