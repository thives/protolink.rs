//! Errors of decoding and verification.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt;

/// Why bytes were refused before verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The bytes end before a required field.
    Truncated,
    /// The magic bytes are not `LUAB`.
    BadMagic,
    /// A format, profile or ABI version is not supported.
    UnsupportedVersion {
        /// Which version field was unsupported.
        field: &'static str,
        /// The value found.
        found: u16,
    },
    /// A count or length exceeds its bound.
    LimitExceeded {
        /// The bounded quantity.
        what: &'static str,
        /// The bound.
        limit: usize,
    },
    /// An opcode byte is not part of the instruction set.
    UnknownOpcode(u8),
    /// A constant tag is unknown.
    UnknownConstantTag(u8),
    /// An upvalue-source tag is unknown.
    UnknownUpvalueTag(u8),
    /// A float constant is NaN or infinite.
    NonFiniteFloat,
    /// The child nesting depth exceeds the limit.
    DepthExceeded,
    /// A child's bytes do not match the digest recorded by its parent.
    ChildDigestMismatch,
    /// The bytes decode, but encoding the result does not reproduce them. Only canonical bytes are admitted, so that
    /// the digest depends on the program alone.
    NotCanonical,
    /// Bytes remain after the artifact ends.
    TrailingBytes,
    /// A decoded artifact failed verification.
    Verify(VerifyError),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => f.write_str("artifact is truncated"),
            Self::BadMagic => f.write_str("artifact magic is not LUAB"),
            Self::UnsupportedVersion { field, found } => write!(f, "unsupported {field} {found}"),
            Self::LimitExceeded { what, limit } => write!(f, "{what} exceeds {limit}"),
            Self::UnknownOpcode(op) => write!(f, "unknown opcode {op}"),
            Self::UnknownConstantTag(tag) => write!(f, "unknown constant tag {tag}"),
            Self::UnknownUpvalueTag(tag) => write!(f, "unknown upvalue source tag {tag}"),
            Self::NonFiniteFloat => f.write_str("float constant is not finite"),
            Self::DepthExceeded => f.write_str("child nesting exceeds the depth limit"),
            Self::ChildDigestMismatch => {
                f.write_str("child artifact does not match its recorded digest")
            }
            Self::NotCanonical => f.write_str("artifact bytes are not in canonical form"),
            Self::TrailingBytes => f.write_str("bytes remain after the artifact"),
            Self::Verify(err) => write!(f, "verification failed: {err}"),
        }
    }
}

impl core::error::Error for DecodeError {}

/// Why a decoded artifact was refused. Each variant names the proto and instruction where it was found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VerifyError {
    /// The artifact has no entries, so there is nothing to start.
    NoEntry,
    /// An entry names a proto that does not exist.
    EntryProtoOutOfRange {
        /// Entry index.
        entry: usize,
    },
    /// An entry's proto is also the target of a closure. Entry protos must be roots.
    EntryIsClosureTarget {
        /// Proto index.
        proto: usize,
    },
    /// Two entries name the same proto. Each proto is the root of at most one entry.
    DuplicateEntry {
        /// Proto index.
        proto: usize,
    },
    /// A proto has more parameters than locals, or more locals than the limit.
    BadLocalLayout {
        /// Proto index.
        proto: usize,
    },
    /// A proto declares a stack bound above the limit.
    StackBoundTooLarge {
        /// Proto index.
        proto: usize,
    },
    /// A proto has no code.
    EmptyCode {
        /// Proto index.
        proto: usize,
    },
    /// A proto's line table does not match its code.
    LineTableMismatch {
        /// Proto index.
        proto: usize,
    },
    /// An instruction refers to a constant that does not exist.
    ConstantOutOfRange {
        /// Proto index.
        proto: usize,
        /// Instruction index.
        at: usize,
    },
    /// An instruction refers to a local slot outside the proto's locals.
    LocalOutOfRange {
        /// Proto index.
        proto: usize,
        /// Instruction index.
        at: usize,
    },
    /// An instruction refers to an upvalue outside the proto's upvalues.
    UpvalueOutOfRange {
        /// Proto index.
        proto: usize,
        /// Instruction index.
        at: usize,
    },
    /// A jump leaves the code or targets a non-instruction.
    JumpOutOfRange {
        /// Proto index.
        proto: usize,
        /// Instruction index.
        at: usize,
    },
    /// A closure refers to a proto that is not later than its creator. This rules out cycles.
    ClosureTargetOrder {
        /// Proto index.
        proto: usize,
        /// Instruction index.
        at: usize,
    },
    /// A closure upvalue source refers outside the creator's locals or upvalues.
    ClosureSourceOutOfRange {
        /// Proto index.
        proto: usize,
    },
    /// A native reference is outside the artifact's native table.
    NativeOutOfRange {
        /// Proto index.
        proto: usize,
        /// Instruction index.
        at: usize,
    },
    /// A spawn refers to a child that does not exist.
    ChildOutOfRange {
        /// Proto index.
        proto: usize,
        /// Instruction index.
        at: usize,
    },
    /// A child's entry index is outside the child's entries.
    ChildEntryOutOfRange {
        /// Child index.
        child: usize,
    },
    /// A count marker producer is not followed at once by a consumer, or a consumer is not preceded by a producer or is a
    /// jump target.
    MultiResultPattern {
        /// Proto index.
        proto: usize,
        /// Instruction index.
        at: usize,
    },
    /// Execution can run past the end of the code.
    FallsOffEnd {
        /// Proto index.
        proto: usize,
    },
    /// Two paths reach one instruction with different stack heights.
    InconsistentStack {
        /// Proto index.
        proto: usize,
        /// Instruction index.
        at: usize,
    },
    /// An instruction pops more values than the path provides.
    StackUnderflow {
        /// Proto index.
        proto: usize,
        /// Instruction index.
        at: usize,
    },
    /// An instruction pushes beyond the declared stack bound.
    StackOverflow {
        /// Proto index.
        proto: usize,
        /// Instruction index.
        at: usize,
    },
    /// A child's recorded digest is not the digest of its bytes.
    ChildDigestWrong {
        /// Child index.
        child: usize,
    },
    /// A fault inside a descendant artifact.
    ///
    /// The error is one flat record, not a chain with a layer per level. `path` lists the child index taken at each level
    /// from the root, outermost first, so its length is the depth of the faulty artifact. `cause` is the fault found there:
    /// a decode failure, or a verification failure of that artifact or of its child edge. It is never another
    /// `ChildInvalid`, so formatting and dropping the error take a bounded amount of stack whatever the depth.
    ChildInvalid {
        /// Child indices from the root to the faulty artifact. Never empty.
        path: Vec<usize>,
        /// The fault found at the end of `path`. Boxed so the variant stays small.
        cause: Box<DecodeError>,
    },
    /// A native reference declares more arguments than the instruction set allows.
    NativeArityTooLarge {
        /// Native index.
        native: usize,
    },
    /// A native reference declares more results than this VM's native ABI supports.
    NativeResultsUnsupported {
        /// Native index.
        native: usize,
    },
    /// A count or length exceeds its bound. The same bound applies to decoded and to in-memory artifacts.
    LimitExceeded {
        /// The bounded quantity.
        what: &'static str,
        /// The bound.
        limit: usize,
    },
    /// A float constant is NaN or infinite.
    NonFiniteFloat {
        /// Constant index.
        constant: usize,
    },
    /// A child's bytes decode, but do not encode to the same bytes. Only canonical children are admitted.
    ChildNotCanonical {
        /// Child index.
        child: usize,
    },
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoEntry => f.write_str("artifact has no entry"),
            Self::EntryProtoOutOfRange { entry } => {
                write!(f, "entry {entry} names a missing proto")
            }
            Self::EntryIsClosureTarget { proto } => {
                write!(f, "entry proto {proto} is also a closure target")
            }
            Self::DuplicateEntry { proto } => write!(f, "proto {proto} is named by two entries"),
            Self::BadLocalLayout { proto } => {
                write!(f, "proto {proto} has an invalid local layout")
            }
            Self::StackBoundTooLarge { proto } => {
                write!(f, "proto {proto} declares too large a stack")
            }
            Self::EmptyCode { proto } => write!(f, "proto {proto} has no code"),
            Self::LineTableMismatch { proto } => {
                write!(f, "proto {proto} line table does not match its code")
            }
            Self::ConstantOutOfRange { proto, at } => {
                write!(f, "proto {proto} instruction {at}: constant out of range")
            }
            Self::LocalOutOfRange { proto, at } => {
                write!(f, "proto {proto} instruction {at}: local out of range")
            }
            Self::UpvalueOutOfRange { proto, at } => {
                write!(f, "proto {proto} instruction {at}: upvalue out of range")
            }
            Self::JumpOutOfRange { proto, at } => {
                write!(f, "proto {proto} instruction {at}: jump out of range")
            }
            Self::ClosureTargetOrder { proto, at } => write!(
                f,
                "proto {proto} instruction {at}: closure target is not later"
            ),
            Self::ClosureSourceOutOfRange { proto } => {
                write!(f, "proto {proto}: closure source out of range")
            }
            Self::NativeOutOfRange { proto, at } => {
                write!(f, "proto {proto} instruction {at}: native out of range")
            }
            Self::ChildOutOfRange { proto, at } => {
                write!(f, "proto {proto} instruction {at}: child out of range")
            }
            Self::ChildEntryOutOfRange { child } => write!(f, "child {child} entry out of range"),
            Self::MultiResultPattern { proto, at } => write!(
                f,
                "proto {proto} instruction {at}: multi-result pattern broken"
            ),
            Self::FallsOffEnd { proto } => write!(f, "proto {proto} can run past its code"),
            Self::InconsistentStack { proto, at } => write!(
                f,
                "proto {proto} instruction {at}: inconsistent stack height"
            ),
            Self::StackUnderflow { proto, at } => {
                write!(f, "proto {proto} instruction {at}: stack underflow")
            }
            Self::StackOverflow { proto, at } => {
                write!(f, "proto {proto} instruction {at}: stack overflow")
            }
            Self::ChildDigestWrong { child } => write!(f, "child {child} digest is wrong"),
            Self::ChildInvalid { path, cause } => {
                f.write_str("child ")?;
                for (position, index) in path.iter().enumerate() {
                    if position > 0 {
                        f.write_str(".")?;
                    }
                    write!(f, "{index}")?;
                }
                write!(f, " is invalid: {cause}")
            }
            Self::NativeArityTooLarge { native } => {
                write!(f, "native {native} declares too many arguments")
            }
            Self::NativeResultsUnsupported { native } => {
                write!(
                    f,
                    "native {native} declares more results than the native ABI supports"
                )
            }
            Self::LimitExceeded { what, limit } => write!(f, "{what} exceeds {limit}"),
            Self::NonFiniteFloat { constant } => {
                write!(f, "constant {constant} is a float that is not finite")
            }
            Self::ChildNotCanonical { child } => {
                write!(f, "child {child} is not in canonical form")
            }
        }
    }
}

impl core::error::Error for VerifyError {}
