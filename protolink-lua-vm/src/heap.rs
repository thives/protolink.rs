//! The bounded object arena. Object ids are indices owned by one machine, so a stale or forged id cannot reach another
//! machine. Every access is checked and reports an error instead of panicking.

use alloc::vec::Vec;

use crate::machine::Fault;

/// Index of an object in the arena.
pub(crate) type ObjId = u32;

/// One VM value. Copying a value never copies an object.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Val {
    Nil,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Object(ObjId),
    /// Handle of a child execution spawned by this machine.
    Task(u32),
    /// Result count marker left by `Call` with `RESULTS_MULTI`. Never visible to Lua code.
    Count(u32),
}

/// A heap object.
#[derive(Debug)]
pub(crate) enum Object {
    Bytes(Vec<u8>),
    Closure { proto: u32, upvalues: Vec<ObjId> },
    Cell(Val),
    Tuple(Vec<Val>),
}

/// The arena with its object and byte budgets.
#[derive(Debug)]
pub(crate) struct Heap {
    objects: Vec<Object>,
    bytes: usize,
    max_objects: usize,
    max_bytes: usize,
}

impl Heap {
    pub(crate) fn new(max_objects: usize, max_bytes: usize) -> Self {
        Self {
            objects: Vec::new(),
            bytes: 0,
            max_objects,
            max_bytes,
        }
    }

    /// True when one more object of `size` bytes fits both budgets. It changes nothing.
    fn fits(&self, size: usize) -> bool {
        self.objects.len() < self.max_objects
            && self
                .bytes
                .checked_add(size)
                .is_some_and(|total| total <= self.max_bytes)
    }

    /// Checks the charge and reserves the arena slot before any borrowed payload is copied.
    fn admit(&mut self, size: usize) -> Result<ObjId, Fault> {
        if !self.fits(size) {
            return Err(Fault::Capacity("heap budget exhausted"));
        }
        let id = u32::try_from(self.objects.len())
            .map_err(|_| Fault::Capacity("object ids exhausted"))?;
        self.objects
            .try_reserve(1)
            .map_err(|_| Fault::Capacity("heap allocation failed"))?;
        Ok(id)
    }

    pub(crate) fn alloc(&mut self, object: Object) -> Result<ObjId, Fault> {
        let size = size_of(&object)?;
        let id = self.admit(size)?;
        self.objects.push(object);
        self.bytes += size;
        Ok(id)
    }

    pub(crate) fn admit_tuple(&mut self, len: usize) -> Result<(), Fault> {
        self.admit(charge(len, 16)?).map(|_| ())
    }

    pub(crate) fn admit_closure(&mut self, len: usize) -> Result<(), Fault> {
        self.admit(charge(len, 4)?).map(|_| ())
    }

    /// Checks that a byte string of `len` bytes fits the budgets, before the caller reserves its buffer. It changes nothing.
    pub(crate) fn check_bytes(&self, len: usize) -> Result<(), Fault> {
        if self.fits(charge(len, 1)?) {
            Ok(())
        } else {
            Err(Fault::Capacity("heap budget exhausted"))
        }
    }

    /// Stores a byte string the caller has built within the budget checked by [`Heap::check_bytes`]. The budget is
    /// checked again here, so a caller that skipped the first check still cannot exceed it.
    pub(crate) fn adopt_bytes(&mut self, bytes: Vec<u8>) -> Result<ObjId, Fault> {
        let size = charge(bytes.len(), 1)?;
        let id = self.admit(size)?;
        self.objects.push(Object::Bytes(bytes));
        self.bytes += size;
        Ok(id)
    }

    pub(crate) fn alloc_bytes(&mut self, bytes: &[u8]) -> Result<ObjId, Fault> {
        let size = charge(bytes.len(), 1)?;
        let id = self.admit(size)?;
        let copy = copy_bytes(bytes)?;
        self.objects.push(Object::Bytes(copy));
        self.bytes += size;
        Ok(id)
    }

    pub(crate) fn checkpoint(&self) -> (usize, usize) {
        (self.objects.len(), self.bytes)
    }

    pub(crate) fn rollback(&mut self, (objects, bytes): (usize, usize)) {
        self.objects.truncate(objects);
        self.bytes = bytes;
    }

    pub(crate) fn get(&self, id: ObjId) -> Result<&Object, Fault> {
        self.objects
            .get(id as usize)
            .ok_or_else(|| Fault::Raise(b"internal: dangling object".to_vec()))
    }

    pub(crate) fn get_mut(&mut self, id: ObjId) -> Result<&mut Object, Fault> {
        self.objects
            .get_mut(id as usize)
            .ok_or_else(|| Fault::Raise(b"internal: dangling object".to_vec()))
    }
}

/// Reserves an empty buffer of `len` bytes. An allocator refusal is a capacity fault, so the machine stops the execution
/// and nothing aborts.
pub(crate) fn byte_buffer(len: usize) -> Result<Vec<u8>, Fault> {
    let mut out = Vec::new();
    out.try_reserve_exact(len)
        .map_err(|_| Fault::Capacity("heap allocation failed"))?;
    Ok(out)
}

/// A copy of `bytes`, reserved fallibly. Call it after the budgets were checked for the copy.
pub(crate) fn copy_bytes(bytes: &[u8]) -> Result<Vec<u8>, Fault> {
    let mut copy = byte_buffer(bytes.len())?;
    copy.extend_from_slice(bytes);
    Ok(copy)
}

/// A copy of `values`, reserved fallibly.
pub(crate) fn copy_values(values: &[Val]) -> Result<Vec<Val>, Fault> {
    let mut copy = Vec::new();
    copy.try_reserve_exact(values.len())
        .map_err(|_| Fault::Capacity("heap allocation failed"))?;
    copy.extend_from_slice(values);
    Ok(copy)
}

/// A vector of `count` copies of `value`, reserved fallibly.
pub(crate) fn filled(value: Val, count: usize) -> Result<Vec<Val>, Fault> {
    let mut out = Vec::new();
    out.try_reserve_exact(count)
        .map_err(|_| Fault::Capacity("frame allocation failed"))?;
    out.resize(count, value);
    Ok(out)
}

fn charge(len: usize, element_size: usize) -> Result<usize, Fault> {
    len.checked_mul(element_size)
        .and_then(|size| size.checked_add(16))
        .ok_or(Fault::Capacity("heap object size overflow"))
}

fn size_of(object: &Object) -> Result<usize, Fault> {
    match object {
        Object::Bytes(bytes) => charge(bytes.len(), 1),
        Object::Closure { upvalues, .. } => charge(upvalues.len(), 4),
        Object::Cell(_) => charge(0, 1),
        Object::Tuple(values) => charge(values.len(), 16),
    }
}
