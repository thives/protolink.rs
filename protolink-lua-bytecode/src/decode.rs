//! Bounded decoding. Every count and length is checked against its limit and the remaining input before allocation.

use alloc::rc::Rc;
use alloc::vec::Vec;
use core::ops::Range;

use crate::digest;
use crate::error::DecodeError;
use crate::format::{
    ABI_VERSION, Artifact, ChildLink, Constant, DIGEST_LEN, Entry, FORMAT_VERSION, Instr, Limits,
    MAGIC, NativeRef, PROFILE_VERSION, Proto, UpvalueSource,
};

/// Smallest encoded size of one item of each kind, used to reject counts the input cannot hold.
const CONSTANT_MIN: usize = 1;
const NATIVE_MIN: usize = 6;
const PROTO_MIN: usize = 11;
const UPVALUE_MIN: usize = 3;
const INSTR_MIN: usize = 1;
const LINE_MIN: usize = 4;
const ENTRY_MIN: usize = 6;
const CHILD_MIN: usize = 4 + DIGEST_LEN + 8;

/// Decodes `bytes`. Each child's bytes are copied into the artifact, which then owns them.
pub(crate) fn decode(bytes: &[u8], limits: &Limits, depth: u8) -> Result<Artifact, DecodeError> {
    decode_with(bytes, limits, depth, true).map(|(artifact, _)| artifact)
}

/// Decodes `bytes` without copying the children. The returned artifact's children carry an empty byte string, and the
/// ranges say where each child's bytes lie in `bytes`. A caller that only verifies reads the children in place, so a
/// level never holds a copy of the levels below it.
pub(crate) fn decode_shallow(
    bytes: &[u8],
    limits: &Limits,
    depth: u8,
) -> Result<(Artifact, Vec<Range<usize>>), DecodeError> {
    decode_with(bytes, limits, depth, false)
}

fn decode_with(
    bytes: &[u8],
    limits: &Limits,
    depth: u8,
    retain: bool,
) -> Result<(Artifact, Vec<Range<usize>>), DecodeError> {
    if depth > limits.max_depth {
        return Err(DecodeError::DepthExceeded);
    }
    if bytes.len() > limits.max_artifact_bytes {
        return Err(DecodeError::LimitExceeded {
            what: "artifact bytes",
            limit: limits.max_artifact_bytes,
        });
    }
    let mut reader = Reader::new(bytes);
    if reader.take(MAGIC.len())? != MAGIC {
        return Err(DecodeError::BadMagic);
    }
    check_version(reader.u16()?, FORMAT_VERSION, "format")?;
    check_version(reader.u16()?, PROFILE_VERSION, "profile")?;
    check_version(reader.u16()?, ABI_VERSION, "abi")?;
    let mut contract = [0u8; DIGEST_LEN];
    contract.copy_from_slice(reader.take(DIGEST_LEN)?);

    let constant_count = reader.count("constants", limits.max_constants, CONSTANT_MIN)?;
    let mut constants = Vec::with_capacity(constant_count);
    for _ in 0..constant_count {
        constants.push(read_constant(&mut reader, limits)?);
    }

    let native_count = reader.count("natives", limits.max_natives, NATIVE_MIN)?;
    let mut natives = Vec::with_capacity(native_count);
    for _ in 0..native_count {
        natives.push(NativeRef {
            module: reader.u16()?,
            operation: reader.u16()?,
            argc: reader.u8()?,
            results: reader.u8()?,
        });
    }

    let proto_count = reader.count("protos", limits.max_protos, PROTO_MIN)?;
    let mut protos = Vec::with_capacity(proto_count);
    for _ in 0..proto_count {
        protos.push(read_proto(&mut reader, limits)?);
    }

    let entry_count = reader.count("entries", limits.max_entries, ENTRY_MIN)?;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries.push(Entry {
            proto: reader.u32()?,
            captures: reader.u16()?,
        });
    }

    let child_count = reader.count("children", limits.max_children, CHILD_MIN)?;
    let mut children = Vec::with_capacity(child_count);
    let mut spans = Vec::with_capacity(if retain { 0 } else { child_count });
    let empty: Rc<[u8]> = Rc::from(&[][..]);
    for _ in 0..child_count {
        let slot = reader.u32()?;
        let mut recorded = [0u8; DIGEST_LEN];
        recorded.copy_from_slice(reader.take(DIGEST_LEN)?);
        let entry = reader.u32()?;
        let len = reader.bounded_len("child bytes", limits.max_artifact_bytes)?;
        let start = reader.pos;
        let child_bytes = reader.take(len)?;
        if digest(child_bytes) != recorded {
            return Err(DecodeError::ChildDigestMismatch);
        }
        let child_bytes: Rc<[u8]> = if retain {
            Rc::from(child_bytes)
        } else {
            spans.push(start..start + len);
            empty.clone()
        };
        children.push(ChildLink {
            slot,
            digest: recorded,
            entry,
            bytes: child_bytes,
        });
    }

    if !reader.is_empty() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok((
        Artifact {
            contract,
            constants,
            natives,
            protos,
            entries,
            children,
        },
        spans,
    ))
}

fn check_version(found: u16, expected: u16, field: &'static str) -> Result<(), DecodeError> {
    if found == expected {
        Ok(())
    } else {
        Err(DecodeError::UnsupportedVersion { field, found })
    }
}

fn read_constant(reader: &mut Reader<'_>, limits: &Limits) -> Result<Constant, DecodeError> {
    match reader.u8()? {
        0 => Ok(Constant::Nil),
        1 => Ok(Constant::Boolean(false)),
        2 => Ok(Constant::Boolean(true)),
        3 => Ok(Constant::Integer(reader.i64()?)),
        4 => {
            let value = f64::from_bits(reader.u64()?);
            if value.is_finite() {
                Ok(Constant::Float(value))
            } else {
                Err(DecodeError::NonFiniteFloat)
            }
        }
        5 => {
            let len = reader.bounded_len("string bytes", limits.max_string_bytes)?;
            Ok(Constant::Bytes(reader.take(len)?.to_vec()))
        }
        tag => Err(DecodeError::UnknownConstantTag(tag)),
    }
}

fn read_proto(reader: &mut Reader<'_>, limits: &Limits) -> Result<Proto, DecodeError> {
    let params = reader.u8()?;
    let locals = reader.u16()?;
    let max_stack = reader.u16()?;
    let upvalue_count = reader.count("upvalues", limits.max_upvalues, UPVALUE_MIN)?;
    let mut upvalues = Vec::with_capacity(upvalue_count);
    for _ in 0..upvalue_count {
        let source = match reader.u8()? {
            0 => UpvalueSource::Local(reader.u16()?),
            1 => UpvalueSource::Upvalue(reader.u16()?),
            tag => return Err(DecodeError::UnknownUpvalueTag(tag)),
        };
        upvalues.push(source);
    }

    let code_count = reader.count("instructions", limits.max_code, INSTR_MIN)?;
    let mut code = Vec::with_capacity(code_count);
    for _ in 0..code_count {
        code.push(read_instr(reader)?);
    }

    let line_count = reader.count("lines", limits.max_code, LINE_MIN)?;
    let mut lines = Vec::with_capacity(line_count);
    for _ in 0..line_count {
        lines.push(reader.u32()?);
    }
    Ok(Proto {
        params,
        locals,
        max_stack,
        upvalues,
        code,
        lines,
    })
}

fn read_instr(reader: &mut Reader<'_>) -> Result<Instr, DecodeError> {
    let opcode = reader.u8()?;
    let instr = match opcode {
        0 => Instr::Nil,
        1 => Instr::True,
        2 => Instr::False,
        3 => Instr::Pop,
        4 => Instr::Constant(reader.u32()?),
        5 => Instr::GetLocal(reader.u16()?),
        6 => Instr::SetLocal(reader.u16()?),
        7 => Instr::GetCell(reader.u16()?),
        8 => Instr::SetCell(reader.u16()?),
        9 => Instr::NewCell(reader.u16()?),
        10 => Instr::GetUpvalue(reader.u16()?),
        11 => Instr::SetUpvalue(reader.u16()?),
        12 => Instr::Closure(reader.u32()?),
        13 => Instr::Add,
        14 => Instr::Sub,
        15 => Instr::Mul,
        16 => Instr::Div,
        17 => Instr::IDiv,
        18 => Instr::Mod,
        19 => Instr::Neg,
        20 => Instr::Not,
        21 => Instr::Concat,
        22 => Instr::Len,
        23 => Instr::Eq,
        24 => Instr::Ne,
        25 => Instr::Lt,
        26 => Instr::Le,
        27 => Instr::Gt,
        28 => Instr::Ge,
        29 => Instr::Jump(reader.u32()?),
        30 => Instr::JumpIfFalse(reader.u32()?),
        31 => Instr::JumpIfFalseKeep(reader.u32()?),
        32 => Instr::JumpIfTrueKeep(reader.u32()?),
        33 => Instr::Call {
            argc: reader.u8()?,
            results: reader.u8()?,
        },
        34 => Instr::Return(reader.u8()?),
        35 => Instr::ReturnMulti(reader.u8()?),
        36 => Instr::ReturnTuple,
        37 => Instr::Spawn {
            child: reader.u16()?,
            argc: reader.u8()?,
            captures: reader.u16()?,
        },
        38 => Instr::Await,
        39 => Instr::Unpack(reader.u8()?),
        40 => Instr::Native(reader.u16()?),
        41 => Instr::Yield(reader.u8()?),
        42 => Instr::Pcall(reader.u8()?),
        43 => Instr::Raise,
        44 => Instr::CallMulti {
            fixed: reader.u8()?,
            results: reader.u8()?,
        },
        45 => Instr::PcallMulti(reader.u8()?),
        46 => Instr::YieldMulti(reader.u8()?),
        47 => Instr::SpawnMulti {
            child: reader.u16()?,
            fixed: reader.u8()?,
            captures: reader.u16()?,
        },
        48 => Instr::UnpackMulti,
        49 => Instr::ForPrep {
            slot: reader.u16()?,
            exit: reader.u32()?,
        },
        50 => Instr::ForLoop {
            slot: reader.u16()?,
            body: reader.u32()?,
        },
        51 => Instr::PackMulti(reader.u8()?),
        52 => Instr::PcallList,
        other => return Err(DecodeError::UnknownOpcode(other)),
    };
    Ok(instr)
}

/// Cursor over the input. Every read is bounds-checked and returns `Truncated` rather than panicking.
struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        if len > self.remaining() {
            return Err(DecodeError::Truncated);
        }
        let slice = &self.bytes[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    /// Reads a count and checks it against `limit` and against the bytes that remain, given the smallest item size.
    fn count(
        &mut self,
        what: &'static str,
        limit: usize,
        min_item: usize,
    ) -> Result<usize, DecodeError> {
        let count = self.u32()? as usize;
        if count > limit {
            return Err(DecodeError::LimitExceeded { what, limit });
        }
        if count.saturating_mul(min_item) > self.remaining() {
            return Err(DecodeError::Truncated);
        }
        Ok(count)
    }

    /// Reads a byte length, checked against `limit` and the remaining input.
    fn bounded_len(&mut self, what: &'static str, limit: usize) -> Result<usize, DecodeError> {
        let len = self.u32()? as usize;
        if len > limit {
            return Err(DecodeError::LimitExceeded { what, limit });
        }
        if len > self.remaining() {
            return Err(DecodeError::Truncated);
        }
        Ok(len)
    }
}
