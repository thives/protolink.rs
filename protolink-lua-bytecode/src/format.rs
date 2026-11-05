//! The artifact model.
//!
//! An artifact holds one root entry, its protos and the children it may spawn. Each child is a complete artifact,
//! carried as bytes and bound to its parent by digest, so a relay forwards the exact host-compiled bytes. Each child edge
//! also names the stable child slot it runs on, so a node routes a spawn by slot, never by contract digest.

use alloc::rc::Rc;
use alloc::vec::Vec;

/// Magic bytes of an encoded artifact.
pub const MAGIC: [u8; 4] = *b"LUAB";
/// Encoding version. Decoding refuses any other value. Version 4 adds the ABI 3 dynamic-list opcodes.
pub const FORMAT_VERSION: u16 = 4;
/// Remote language profile the instruction set implements. This is the implemented subset of `lua54-bounded-v1`.
pub const PROFILE_VERSION: u16 = 1;
/// Instruction-set and VM ABI version. Version 3 adds `PackMulti` and `PcallList` and fixes dynamic argument semantics.
pub const ABI_VERSION: u16 = 3;
/// Most results a native operation may declare under this VM's native ABI. The VM pushes one value or none.
pub const MAX_NATIVE_RESULTS: u8 = 1;
/// Most arguments a native operation may declare. Verification refuses a reference that declares more.
pub const MAX_NATIVE_ARGS: u8 = 32;
/// Length of a SHA-256 digest.
pub const DIGEST_LEN: usize = 32;
/// Length of the fixed header: magic, the three version numbers and the contract digest.
pub const HEADER_LEN: usize = MAGIC.len() + 3 * 2 + DIGEST_LEN;
/// Result count of a `Call` whose values are all kept for an immediately following `ReturnMulti`.
pub const RESULTS_MULTI: u8 = 255;

/// One constant of the constant pool.
#[derive(Clone, Debug, PartialEq)]
pub enum Constant {
    /// Lua `nil`.
    Nil,
    /// Lua boolean.
    Boolean(bool),
    /// Lua integer.
    Integer(i64),
    /// Finite float. Decoding rejects NaN and infinities.
    Float(f64),
    /// Byte string.
    Bytes(Vec<u8>),
}

/// A native operation the code may invoke, as the compiler binds it: module id, operation id, argument count and result
/// count.
///
/// The same value type is the destination's operation table. A reference is admitted only when a table entry equals it
/// in all four fields (see [`crate::admit`]). Matching the table is a compatibility fact; it grants no authority to run
/// the operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeRef {
    /// Module identifier in the contract.
    pub module: u16,
    /// Operation identifier in the module.
    pub operation: u16,
    /// Number of arguments the operation takes.
    pub argc: u8,
    /// Number of results the operation returns: zero or one under this ABI. The VM pushes exactly this many values.
    pub results: u8,
}

/// Where a closure takes one upvalue from, in the frame that creates it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpvalueSource {
    /// A local slot of the creator that holds a cell.
    Local(u16),
    /// An upvalue of the creator.
    Upvalue(u16),
}

/// One function prototype.
#[derive(Clone, Debug, PartialEq)]
pub struct Proto {
    /// Number of parameters. They occupy the first local slots.
    pub params: u8,
    /// Total local slots, including parameters.
    pub locals: u16,
    /// Declared maximum operand-stack height.
    pub max_stack: u16,
    /// Upvalue sources, used when a `Closure` instruction creates this proto. Empty for entries.
    pub upvalues: Vec<UpvalueSource>,
    /// Instructions. Jump targets are indices into this list.
    pub code: Vec<Instr>,
    /// Source line of each instruction, for diagnostics. Same length as `code`.
    pub lines: Vec<u32>,
}

/// A root that can be started. Its upvalues are the captures supplied at start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Index of the entry's proto.
    pub proto: u32,
    /// Number of capture cells supplied when the entry starts.
    pub captures: u16,
}

/// A child artifact a parent may spawn, with the entry to start in it and the slot it runs on.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildLink {
    /// Stable id of the parent's child slot this edge runs on. The node resolves it to a link through its route table.
    pub slot: u32,
    /// Digest of `bytes`, recorded by the parent.
    pub digest: [u8; DIGEST_LEN],
    /// Entry index inside the child artifact.
    pub entry: u32,
    /// The child's complete encoded artifact. It is shared, so a spawn forwards it without copying.
    pub bytes: Rc<[u8]>,
}

/// A complete artifact.
#[derive(Clone, Debug, PartialEq)]
pub struct Artifact {
    /// Digest of the contract this artifact was compiled against. A node admits the artifact only when it equals the
    /// digest of the node's own contract.
    pub contract: [u8; DIGEST_LEN],
    /// Constant pool.
    pub constants: Vec<Constant>,
    /// Native operations referenced by `Native` instructions.
    pub natives: Vec<NativeRef>,
    /// Function prototypes.
    pub protos: Vec<Proto>,
    /// Startable roots. Entry 0 is the default root.
    pub entries: Vec<Entry>,
    /// Child artifacts referenced by `Spawn` instructions.
    pub children: Vec<ChildLink>,
}

/// One instruction. Operands are fixed width, and each jump target is an absolute instruction index.
///
/// Values on the operand stack are Lua values. `Tuple` results (from `Await`, `Pcall` and `Yield`) are one stack value
/// that `Unpack`, `UnpackMulti` or `ReturnTuple` expands.
///
/// A variable number of values is passed with a count marker. A marker producer (`Call` or `CallMulti` with
/// `RESULTS_MULTI`, or `UnpackMulti`) leaves the values and a marker. The very next instruction must be a marker consumer
/// (`ReturnMulti`, `CallMulti`, `PcallMulti`, `PcallList`, `PackMulti`, `YieldMulti` or `SpawnMulti`), which takes its fixed operands below the
/// values. The verifier checks the pairing, and the VM checks the dynamic count against the frame's stack bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Instr {
    /// Push `nil`.
    Nil,
    /// Push `true`.
    True,
    /// Push `false`.
    False,
    /// Discard the top value.
    Pop,
    /// Push constant `index`.
    Constant(u32),
    /// Push a plain local.
    GetLocal(u16),
    /// Pop into a plain local.
    SetLocal(u16),
    /// Push the value of the cell held in a local.
    GetCell(u16),
    /// Pop into the cell held in a local.
    SetCell(u16),
    /// Pop a value into a new cell stored in a local.
    NewCell(u16),
    /// Push the value of an upvalue cell.
    GetUpvalue(u16),
    /// Pop into an upvalue cell.
    SetUpvalue(u16),
    /// Create a closure of proto `index`, capturing its upvalues.
    Closure(u32),
    /// Pop two, push `a + b` with wrapping integer arithmetic.
    Add,
    /// Pop two, push `a - b`.
    Sub,
    /// Pop two, push `a * b`.
    Mul,
    /// Pop two, push `a / b` as a float.
    Div,
    /// Pop two, push floor division.
    IDiv,
    /// Pop two, push Lua modulo.
    Mod,
    /// Pop one, push its negation.
    Neg,
    /// Pop one, push its logical negation.
    Not,
    /// Pop two, push their concatenation of byte strings.
    Concat,
    /// Pop one, push its byte length.
    Len,
    /// Pop two, push equality.
    Eq,
    /// Pop two, push inequality.
    Ne,
    /// Pop two, push `a < b`.
    Lt,
    /// Pop two, push `a <= b`.
    Le,
    /// Pop two, push `a > b`.
    Gt,
    /// Pop two, push `a >= b`.
    Ge,
    /// Jump to `target`.
    Jump(u32),
    /// Pop one; jump to `target` if it is falsy.
    JumpIfFalse(u32),
    /// Jump to `target` keeping the value if it is falsy, otherwise pop it. Implements `and`.
    JumpIfFalseKeep(u32),
    /// Jump to `target` keeping the value if it is truthy, otherwise pop it. Implements `or`.
    JumpIfTrueKeep(u32),
    /// Call the value below `argc` arguments. `results` values are kept, or `RESULTS_MULTI` for the marker form.
    Call {
        /// Number of arguments.
        argc: u8,
        /// Number of results to keep, or `RESULTS_MULTI`.
        results: u8,
    },
    /// Return the top `n` values.
    Return(u8),
    /// Return `n` fixed values followed by the values announced by the count marker on top. A marker consumer.
    ReturnMulti(u8),
    /// Return the values of the tuple on top.
    ReturnTuple,
    /// Pop captures and arguments and create a child execution. Push its handle.
    Spawn {
        /// Index into the artifact's children.
        child: u16,
        /// Number of arguments.
        argc: u8,
        /// Number of capture values.
        captures: u16,
    },
    /// Pop a child handle and wait for its return value. Push the returned tuple.
    Await,
    /// Pop a tuple and push exactly `n` values, padding with nil or truncating.
    Unpack(u8),
    /// Invoke native `index` with the arguments on top. Push its declared number of results, zero or one.
    Native(u16),
    /// Pop `argc` values and yield them as a tuple. Push the tuple of resume values when resumed.
    Yield(u8),
    /// Pop a function and `argc` arguments and call it protected. Push a tuple of status and results or the error.
    Pcall(u8),
    /// Pop a value and raise it as a runtime error.
    Raise,
    /// Call the value below `fixed` arguments and a variable tail announced by the marker on top. A marker consumer.
    CallMulti {
        /// Number of fixed arguments below the variable tail.
        fixed: u8,
        /// Number of results to keep, or `RESULTS_MULTI`, which makes it a marker producer too.
        results: u8,
    },
    /// Like `Pcall`, with `fixed` arguments followed by a variable tail. A marker consumer.
    PcallMulti(u8),
    /// Like `Yield`, with `fixed` values followed by a variable tail. A marker consumer.
    YieldMulti(u8),
    /// Like `Spawn`, with the captures pushed first and the arguments after them, the last ones a variable tail. A
    /// marker consumer.
    SpawnMulti {
        /// Index into the artifact's children.
        child: u16,
        /// Number of fixed arguments below the variable tail.
        fixed: u8,
        /// Number of capture values, below the arguments.
        captures: u16,
    },
    /// Pop a tuple and push all its values and a count marker. A marker producer.
    UnpackMulti,
    /// Consume `fixed` values and a variable tail, preserving their exact count in an internal tuple.
    PackMulti(u8),
    /// Consume an expanded argument list, calling its first value protected with the remaining values. An empty list
    /// raises the missing-argument error outside the protected call. A marker consumer.
    PcallList,
    /// Prepare a numeric for loop over locals `slot` (start), `slot + 1` (limit) and `slot + 2` (step), following Lua
    /// 5.4: an integer loop precomputes its iteration count, so it never overflows, and a float loop compares each time.
    /// Jump to `exit` when the loop runs zero times.
    ForPrep {
        /// First of the three loop locals.
        slot: u16,
        /// Instruction after the loop.
        exit: u32,
    },
    /// Advance the loop prepared by `ForPrep` on the same `slot` and jump to `body` while it runs.
    ForLoop {
        /// First of the three loop locals.
        slot: u16,
        /// First instruction of the loop body.
        body: u32,
    },
}

/// Bounds applied while decoding and verifying. Every count is checked before allocation.
///
/// [`Limits::profile`] gives illustrative defaults for host tests. Real node budgets are measured on hardware before a
/// deployment depends on them (distributed plan section 8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Largest encoded artifact, including nested children.
    pub max_artifact_bytes: usize,
    /// Largest constant pool.
    pub max_constants: usize,
    /// Largest byte string constant or tuple string.
    pub max_string_bytes: usize,
    /// Largest native table.
    pub max_natives: usize,
    /// Largest number of protos.
    pub max_protos: usize,
    /// Largest instruction count of one proto.
    pub max_code: usize,
    /// Largest number of local slots of one proto.
    pub max_locals: u16,
    /// Largest declared operand-stack height.
    pub max_stack: u16,
    /// Largest number of upvalues of one proto.
    pub max_upvalues: usize,
    /// Largest number of entries.
    pub max_entries: usize,
    /// Largest number of child artifacts.
    pub max_children: usize,
    /// Deepest child nesting. The root is depth 0.
    pub max_depth: u8,
}

impl Limits {
    /// The bounds of a node that serves a contract with this envelope: its artifact bound, and its spawn depth, which
    /// counts the node's own level, so its embedded children may nest one level less. Every other bound is the profile's.
    ///
    /// The compiler and every node composition use this one translation, so a compiled artifact is admitted by its
    /// recipient exactly when the compiler accepted it.
    pub const fn for_destination(max_artifact_bytes: usize, max_spawn_depth: u32) -> Self {
        let nested = max_spawn_depth.saturating_sub(1);
        let max_depth = if nested > u8::MAX as u32 {
            u8::MAX
        } else {
            nested as u8
        };
        Self {
            max_artifact_bytes,
            max_depth,
            ..Self::profile()
        }
    }

    /// Illustrative defaults for host tests of the reduced profile.
    pub const fn profile() -> Self {
        Self {
            max_artifact_bytes: 65_536,
            max_constants: 1024,
            max_string_bytes: 1024,
            max_natives: 64,
            max_protos: 256,
            max_code: 8192,
            max_locals: 200,
            max_stack: 128,
            max_upvalues: 64,
            max_entries: 16,
            max_children: 8,
            max_depth: 2,
        }
    }
}
