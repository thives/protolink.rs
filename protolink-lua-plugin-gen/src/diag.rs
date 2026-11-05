//! Diagnostics with stable codes.

use std::fmt;
use std::path::PathBuf;

/// Stable diagnostic codes. A code keeps its meaning across releases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Code {
    /// The file could not be read or parsed.
    Parse,
    /// The schema version is not supported.
    UnsupportedSchema,
    /// A name is declared twice in one scope.
    DuplicateName,
    /// An identifier is declared twice in one scope, or reused.
    DuplicateId,
    /// An identifier is outside `1..=65535`.
    IdOutOfRange,
    /// A type is not one of the supported kinds.
    UnknownType,
    /// An enum reference does not resolve.
    UnresolvedEnum,
    /// An execution kind is not `sync` or `async`.
    UnknownExecution,
    /// An import path leaves the approved root.
    PathEscapesRoot,
    /// Imports form a cycle.
    ImportCycle,
    /// An import's identity or version does not match the imported contract.
    ImportIdentityMismatch,
    /// A child refers to an alias that is not imported.
    UnknownChildContract,
    /// A child uses an intrinsic this generator does not know.
    UnknownIntrinsic,
    /// A privilege is required but not declared by the relevant contract.
    UndeclaredPrivilege,
    /// An operation refers to an error that is not declared.
    UnknownError,
    /// A limit is zero or inconsistent.
    InvalidLimit,
    /// A lock does not match the resolved contracts.
    StaleLock,
    /// A module import names an alias that the manifest does not import.
    UnknownImport,
    /// A module import names a module that the imported contract does not declare.
    UnknownModule,
    /// An operation's signature cannot be carried by the profile's native ABI.
    UnsupportedSignature,
}

impl Code {
    /// The stable code string, such as `PG010`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Parse => "PG001",
            Self::UnsupportedSchema => "PG002",
            Self::DuplicateName => "PG003",
            Self::DuplicateId => "PG004",
            Self::IdOutOfRange => "PG005",
            Self::UnknownType => "PG006",
            Self::UnresolvedEnum => "PG007",
            Self::UnknownExecution => "PG008",
            Self::PathEscapesRoot => "PG009",
            Self::ImportCycle => "PG010",
            Self::ImportIdentityMismatch => "PG011",
            Self::UnknownChildContract => "PG012",
            Self::UnknownIntrinsic => "PG013",
            Self::UndeclaredPrivilege => "PG014",
            Self::UnknownError => "PG015",
            Self::InvalidLimit => "PG016",
            Self::StaleLock => "PG017",
            Self::UnknownImport => "PG018",
            Self::UnknownModule => "PG019",
            Self::UnsupportedSignature => "PG020",
        }
    }
}

/// One problem found in a manifest or profile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostic {
    /// Stable code.
    pub code: Code,
    /// File that contains the problem.
    pub file: PathBuf,
    /// What is wrong and where, in the manifest's own terms.
    pub message: String,
}

impl Diagnostic {
    pub(crate) fn new(code: Code, file: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self {
            code,
            file: file.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {}: {}",
            self.file.display(),
            self.code.as_str(),
            self.message
        )
    }
}

impl std::error::Error for Diagnostic {}
