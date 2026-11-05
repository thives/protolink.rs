//! Canonical encoding. Equal artifacts always produce equal bytes.
//!
//! The encoder writes into a [`Sink`]. A `Vec<u8>` collects the bytes. A [`Compare`] checks them against bytes that
//! already exist, without holding a copy, which is how canonical form is checked without a second buffer.

use alloc::vec::Vec;

use crate::format::{
    ABI_VERSION, Artifact, Constant, FORMAT_VERSION, Instr, MAGIC, PROFILE_VERSION, UpvalueSource,
};

/// Where encoded bytes go.
pub(crate) trait Sink {
    fn push(&mut self, byte: u8);
    fn extend_from_slice(&mut self, bytes: &[u8]);
}

impl Sink for Vec<u8> {
    fn push(&mut self, byte: u8) {
        Vec::push(self, byte);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        Vec::extend_from_slice(self, bytes);
    }
}

/// A sink that holds nothing and compares every byte it is given with `expected`.
struct Compare<'a> {
    expected: &'a [u8],
    at: usize,
    equal: bool,
}

impl Sink for Compare<'_> {
    fn push(&mut self, byte: u8) {
        self.extend_from_slice(&[byte]);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        if !self.equal {
            return;
        }
        match self
            .at
            .checked_add(bytes.len())
            .and_then(|end| self.expected.get(self.at..end))
        {
            Some(window) if window == bytes => self.at += bytes.len(),
            _ => self.equal = false,
        }
    }
}

/// The canonical bytes of `artifact`, with its children's bytes as stored.
pub(crate) fn encode(artifact: &Artifact) -> Vec<u8> {
    let mut out = Vec::new();
    write(artifact, &|index| &artifact.children[index].bytes, &mut out);
    out
}

/// True when the canonical encoding of `artifact` is exactly `expected`. `children` gives the bytes of each child, so a
/// caller that holds them elsewhere, as slices of `expected`, need not copy them into the artifact. Nothing is allocated.
pub(crate) fn encodes_to<'c>(
    artifact: &Artifact,
    children: &dyn Fn(usize) -> &'c [u8],
    expected: &[u8],
) -> bool {
    let mut compare = Compare {
        expected,
        at: 0,
        equal: true,
    };
    write(artifact, children, &mut compare);
    compare.equal && compare.at == expected.len()
}

fn write<'c>(artifact: &Artifact, children: &dyn Fn(usize) -> &'c [u8], out: &mut dyn Sink) {
    out.extend_from_slice(&MAGIC);
    put_u16(out, FORMAT_VERSION);
    put_u16(out, PROFILE_VERSION);
    put_u16(out, ABI_VERSION);
    out.extend_from_slice(&artifact.contract);

    put_len(out, artifact.constants.len());
    for constant in &artifact.constants {
        match constant {
            Constant::Nil => out.push(0),
            Constant::Boolean(false) => out.push(1),
            Constant::Boolean(true) => out.push(2),
            Constant::Integer(value) => {
                out.push(3);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Constant::Float(value) => {
                out.push(4);
                out.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            Constant::Bytes(bytes) => {
                out.push(5);
                put_len(out, bytes.len());
                out.extend_from_slice(bytes);
            }
        }
    }

    put_len(out, artifact.natives.len());
    for native in &artifact.natives {
        put_u16(out, native.module);
        put_u16(out, native.operation);
        out.push(native.argc);
        out.push(native.results);
    }

    put_len(out, artifact.protos.len());
    for proto in &artifact.protos {
        out.push(proto.params);
        put_u16(out, proto.locals);
        put_u16(out, proto.max_stack);
        put_len(out, proto.upvalues.len());
        for source in &proto.upvalues {
            match source {
                UpvalueSource::Local(slot) => {
                    out.push(0);
                    put_u16(out, *slot);
                }
                UpvalueSource::Upvalue(index) => {
                    out.push(1);
                    put_u16(out, *index);
                }
            }
        }
        put_len(out, proto.code.len());
        for instr in &proto.code {
            put_instr(out, *instr);
        }
        put_len(out, proto.lines.len());
        for line in &proto.lines {
            put_u32(out, *line);
        }
    }

    put_len(out, artifact.entries.len());
    for entry in &artifact.entries {
        put_u32(out, entry.proto);
        put_u16(out, entry.captures);
    }

    put_len(out, artifact.children.len());
    for (index, child) in artifact.children.iter().enumerate() {
        let bytes = children(index);
        put_u32(out, child.slot);
        out.extend_from_slice(&child.digest);
        put_u32(out, child.entry);
        put_len(out, bytes.len());
        out.extend_from_slice(bytes);
    }
}

fn put_instr(out: &mut dyn Sink, instr: Instr) {
    match instr {
        Instr::Nil => out.push(0),
        Instr::True => out.push(1),
        Instr::False => out.push(2),
        Instr::Pop => out.push(3),
        Instr::Constant(index) => {
            out.push(4);
            put_u32(out, index);
        }
        Instr::GetLocal(slot) => op_u16(out, 5, slot),
        Instr::SetLocal(slot) => op_u16(out, 6, slot),
        Instr::GetCell(slot) => op_u16(out, 7, slot),
        Instr::SetCell(slot) => op_u16(out, 8, slot),
        Instr::NewCell(slot) => op_u16(out, 9, slot),
        Instr::GetUpvalue(index) => op_u16(out, 10, index),
        Instr::SetUpvalue(index) => op_u16(out, 11, index),
        Instr::Closure(proto) => {
            out.push(12);
            put_u32(out, proto);
        }
        Instr::Add => out.push(13),
        Instr::Sub => out.push(14),
        Instr::Mul => out.push(15),
        Instr::Div => out.push(16),
        Instr::IDiv => out.push(17),
        Instr::Mod => out.push(18),
        Instr::Neg => out.push(19),
        Instr::Not => out.push(20),
        Instr::Concat => out.push(21),
        Instr::Len => out.push(22),
        Instr::Eq => out.push(23),
        Instr::Ne => out.push(24),
        Instr::Lt => out.push(25),
        Instr::Le => out.push(26),
        Instr::Gt => out.push(27),
        Instr::Ge => out.push(28),
        Instr::Jump(target) => op_u32(out, 29, target),
        Instr::JumpIfFalse(target) => op_u32(out, 30, target),
        Instr::JumpIfFalseKeep(target) => op_u32(out, 31, target),
        Instr::JumpIfTrueKeep(target) => op_u32(out, 32, target),
        Instr::Call { argc, results } => {
            out.push(33);
            out.push(argc);
            out.push(results);
        }
        Instr::Return(count) => {
            out.push(34);
            out.push(count);
        }
        Instr::ReturnMulti(fixed) => {
            out.push(35);
            out.push(fixed);
        }
        Instr::ReturnTuple => out.push(36),
        Instr::Spawn {
            child,
            argc,
            captures,
        } => {
            out.push(37);
            put_u16(out, child);
            out.push(argc);
            put_u16(out, captures);
        }
        Instr::Await => out.push(38),
        Instr::Unpack(count) => {
            out.push(39);
            out.push(count);
        }
        Instr::Native(index) => op_u16(out, 40, index),
        Instr::Yield(argc) => {
            out.push(41);
            out.push(argc);
        }
        Instr::Pcall(argc) => {
            out.push(42);
            out.push(argc);
        }
        Instr::Raise => out.push(43),
        Instr::CallMulti { fixed, results } => {
            out.push(44);
            out.push(fixed);
            out.push(results);
        }
        Instr::PcallMulti(fixed) => {
            out.push(45);
            out.push(fixed);
        }
        Instr::YieldMulti(fixed) => {
            out.push(46);
            out.push(fixed);
        }
        Instr::SpawnMulti {
            child,
            fixed,
            captures,
        } => {
            out.push(47);
            put_u16(out, child);
            out.push(fixed);
            put_u16(out, captures);
        }
        Instr::UnpackMulti => out.push(48),
        Instr::PackMulti(fixed) => {
            out.push(51);
            out.push(fixed);
        }
        Instr::PcallList => out.push(52),
        Instr::ForPrep { slot, exit } => {
            out.push(49);
            put_u16(out, slot);
            put_u32(out, exit);
        }
        Instr::ForLoop { slot, body } => {
            out.push(50);
            put_u16(out, slot);
            put_u32(out, body);
        }
    }
}

fn op_u16(out: &mut dyn Sink, opcode: u8, operand: u16) {
    out.push(opcode);
    put_u16(out, operand);
}

fn op_u32(out: &mut dyn Sink, opcode: u8, operand: u32) {
    out.push(opcode);
    put_u32(out, operand);
}

fn put_u16(out: &mut dyn Sink, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut dyn Sink, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Lengths and counts are written as `u32`. The encoder is only given artifacts the host built within bounds.
fn put_len(out: &mut dyn Sink, len: usize) {
    put_u32(out, u32::try_from(len).unwrap_or(u32::MAX));
}
