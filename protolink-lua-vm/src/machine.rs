//! The machine: frames, the operand stack, instruction dispatch and the effect protocol.
//!
//! Frames live in a vector, so calls never recurse on the Rust stack. All operand stacks share one vector, and each
//! frame's region starts at its `base`. A frame may never pop below its base, and it may never exceed its declared
//! maximum stack. Both are checked at run time, because a dynamic result count is not provable statically.

use alloc::format;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cmp::Ordering;

use protolink_lua::lifecycle::{FailureCode, bounded_diagnostic};
use protolink_lua::value::{
    Limits as WireLimits, Tuple as WireTuple, Value as Wire, check_value, check_values,
};
use protolink_lua_bytecode::{
    Artifact, Constant, Instr, Limits as BytecodeLimits, RESULTS_MULTI, UpvalueSource, VerifyError,
};

use crate::arith::{cmp_num, floor, idiv_int, mod_float, mod_int, number};
use crate::heap::{Heap, ObjId, Object, Val, byte_buffer, copy_bytes, copy_values, filled};
use crate::number::{FloatText, float_text};

/// Verified bytecode ready to run.
#[derive(Debug)]
pub struct Program {
    artifact: Rc<Artifact>,
}

impl Program {
    /// Verifies `artifact` under `limits` and wraps it. An unverified artifact cannot be run.
    ///
    /// The check is [`Artifact::verify`]: structure, stack heights, the count, string, finite-float and depth bounds of
    /// `limits`, and the digest and canonical form of each child. It does not check the artifact's contract or its native
    /// table, which only admission decides. An artifact that did not pass `admit` must not be wrapped as a program for a
    /// node. Runtime capture and argument values are checked by [`Machine::start`].
    pub fn verified(artifact: Artifact, limits: &BytecodeLimits) -> Result<Self, VerifyError> {
        Self::verified_shared(Rc::new(artifact), limits)
    }

    /// Verifies a shared artifact and wraps it. Executions of one artifact can share its code instead of copying it.
    pub fn verified_shared(
        artifact: Rc<Artifact>,
        limits: &BytecodeLimits,
    ) -> Result<Self, VerifyError> {
        artifact.verify(limits)?;
        Ok(Self { artifact })
    }

    /// The verified artifact.
    pub fn artifact(&self) -> &Artifact {
        &self.artifact
    }
}

/// Bounds of one machine. Every count is checked before it is allocated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VmLimits {
    /// Deepest call nesting, counting the entry.
    pub max_frames: usize,
    /// Most heap objects.
    pub max_objects: usize,
    /// Largest heap footprint in bytes.
    pub max_bytes: usize,
    /// Bounds applied to tuples that cross the node boundary.
    pub wire: WireLimits,
    /// Total instruction budget across all slices. Protected calls cannot replenish it.
    pub total_fuel: u64,
}

/// What the machine did during one slice.
#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    /// The entry coroutine yielded these values.
    Yielded(WireTuple),
    /// The entry returned these values.
    Returned(WireTuple),
    /// The machine waits for [`Machine::complete`] before it can run again.
    Pending(Effect),
    /// The slice ended before the machine stopped. Call [`Machine::run`] again.
    Exhausted,
    /// An uncaught failure ended the execution.
    Failed {
        /// Failure class.
        code: FailureCode,
        /// Diagnostic text.
        message: Vec<u8>,
    },
    /// The total instruction budget is spent. `pcall` does not catch this.
    BudgetExhausted,
}

/// An operation the embedding must perform for the machine.
#[derive(Clone, Debug, PartialEq)]
pub enum Effect {
    /// Invoke native `index` of the artifact's native table.
    Native {
        /// Index into the native table.
        index: u16,
        /// Arguments in order.
        arguments: Vec<Wire>,
    },
    /// Create a child execution from child `child` with the given captures and arguments.
    Spawn {
        /// Index into the artifact's child table.
        child: u16,
        /// Spawn-time capture snapshot, in order.
        captures: Vec<Wire>,
        /// Saved initial arguments, in order.
        arguments: Vec<Wire>,
    },
    /// Wait for the outcome of a child created by `Spawn`.
    Await {
        /// Handle returned by the spawn.
        task: u32,
    },
}

/// The embedding's answer to an effect.
#[derive(Clone, Debug, PartialEq)]
pub enum EffectResult {
    /// The result of a native that declares one result, pushed as one value.
    Value(Wire),
    /// The completion of a native that declares no result. Nothing is pushed.
    Nothing,
    /// The new child handle, pushed as a task value.
    Task(u32),
    /// The returned tuple of an awaited child, pushed as one tuple value.
    Tuple(WireTuple),
    /// The effect failed. The message is raised at the calling instruction.
    Raise(Vec<u8>),
}

/// Why a machine could not start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartError {
    /// The entry index is not in the artifact.
    NoSuchEntry,
    /// The capture count does not match the entry.
    CaptureCountMismatch,
    /// A capture or argument violates the wire limits or cannot be represented.
    ValueRejected,
    /// The initial objects exceed the machine's budget.
    Capacity,
}

/// Why a control call was refused. A refused call changes nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlError {
    /// `complete` was called without a pending effect.
    NotPending,
    /// `resume_yield` was called without a yielded machine.
    NotYielded,
    /// The resume values cannot be represented or do not fit the budget.
    Rejected,
}

/// Internal failure of one instruction.
#[derive(Debug)]
pub(crate) enum Fault {
    /// A Lua-level error that protected calls may catch.
    Raise(Vec<u8>),
    /// A bounded resource is full. This is not catchable.
    Capacity(&'static str),
}

/// How a call's results are delivered to its caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Want {
    /// Exactly this many values.
    Fixed(u8),
    /// All values, followed by a count marker.
    Multi,
}

#[derive(Debug)]
struct Frame {
    closure: ObjId,
    proto: usize,
    pc: usize,
    locals: Vec<Val>,
    base: usize,
    want: Want,
    protected: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Ready,
    Running,
    Pending(Awaiting),
    Yielded,
    Finished,
}

/// What a pending effect must be completed with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Awaiting {
    /// A native with this many declared results.
    Native(u8),
    /// A spawn, completed with a task handle.
    Spawn,
    /// An await, completed with the child's returned tuple.
    Await,
}

enum Flow {
    Continue,
    Pending(Effect),
    Yield(WireTuple),
    Return(WireTuple),
}

/// One running root execution.
pub struct Machine {
    program: Program,
    limits: VmLimits,
    constants: Vec<Val>,
    heap: Heap,
    stack: Vec<Val>,
    frames: Vec<Frame>,
    fuel: u64,
    state: State,
    failure: Option<(FailureCode, Vec<u8>)>,
}

impl core::fmt::Debug for Machine {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Machine")
            .field("state", &self.state)
            .field("frames", &self.frames.len())
            .field("fuel", &self.fuel)
            .finish()
    }
}

impl Machine {
    /// Creates a machine that will run entry `entry` with the given captures and saved arguments.
    ///
    /// Captures become cells, so the entry's upvalues are shared with its own closures. The arguments fill the entry's
    /// parameters; missing parameters are nil and excess arguments are ignored after validation. Captures and arguments
    /// are each checked as a complete tuple against the wire limits before any input or constant is imported. Invalid
    /// input returns [`StartError::ValueRejected`], including invalid excess arguments. No Lua runs during start.
    pub fn start(
        program: Program,
        entry: usize,
        captures: &[Wire],
        arguments: &[Wire],
        limits: VmLimits,
    ) -> Result<Self, StartError> {
        let record = *program
            .artifact
            .entries
            .get(entry)
            .ok_or(StartError::NoSuchEntry)?;
        if captures.len() != usize::from(record.captures) {
            return Err(StartError::CaptureCountMismatch);
        }
        // The entry runs in a frame, so a machine that permits no frame cannot run anything. `enter` refuses the same way
        // when the depth is spent.
        if limits.max_frames == 0 {
            return Err(StartError::Capacity);
        }
        check_values(captures, limits.wire).map_err(|_| StartError::ValueRejected)?;
        check_values(arguments, limits.wire).map_err(|_| StartError::ValueRejected)?;
        let proto_index = record.proto as usize;
        let mut machine = Self {
            program,
            limits,
            constants: Vec::new(),
            heap: Heap::new(limits.max_objects, limits.max_bytes),
            stack: Vec::new(),
            frames: Vec::new(),
            fuel: limits.total_fuel,
            state: State::Ready,
            failure: None,
        };
        machine.constants =
            intern_constants(&mut machine.heap, &machine.program.artifact.constants)
                .map_err(start_error)?;

        machine
            .heap
            .admit_closure(captures.len())
            .map_err(start_error)?;
        let mut cells = Vec::new();
        cells
            .try_reserve_exact(captures.len())
            .map_err(|_| StartError::Capacity)?;
        for value in captures {
            let imported = import(&mut machine.heap, value).map_err(start_error)?;
            cells.push(
                machine
                    .heap
                    .alloc(Object::Cell(imported))
                    .map_err(start_error)?,
            );
        }
        let closure = machine
            .heap
            .alloc(Object::Closure {
                proto: record.proto,
                upvalues: cells,
            })
            .map_err(start_error)?;

        let proto = machine
            .program
            .artifact
            .protos
            .get(proto_index)
            .ok_or(StartError::NoSuchEntry)?;
        let mut locals =
            filled(Val::Nil, usize::from(proto.locals)).map_err(|_| StartError::Capacity)?;
        for (slot, value) in arguments.iter().take(usize::from(proto.params)).enumerate() {
            locals[slot] = import(&mut machine.heap, value).map_err(start_error)?;
        }
        machine
            .frames
            .try_reserve(1)
            .map_err(|_| StartError::Capacity)?;
        machine.frames.push(Frame {
            closure,
            proto: proto_index,
            pc: 0,
            locals,
            base: 0,
            want: Want::Fixed(0),
            protected: false,
        });
        Ok(machine)
    }

    /// Runs at most `slice` instructions and reports how the slice ended.
    pub fn run(&mut self, slice: u32) -> Step {
        if let Some((code, message)) = self.failure.take() {
            self.state = State::Finished;
            return Step::Failed { code, message };
        }
        match self.state {
            State::Ready => self.state = State::Running,
            State::Running => {}
            State::Pending(_) => return refused(b"an effect is pending"),
            State::Yielded => return refused(b"the machine is yielded"),
            State::Finished => return refused(b"the execution has finished"),
        }
        let mut left = slice;
        while left > 0 {
            if self.fuel == 0 {
                self.state = State::Finished;
                return Step::BudgetExhausted;
            }
            self.fuel -= 1;
            left -= 1;
            match self.step() {
                Ok(Flow::Continue) => {}
                Ok(Flow::Pending(effect)) => {
                    let awaiting = match &effect {
                        Effect::Native { index, .. } => Awaiting::Native(
                            self.program
                                .artifact
                                .natives
                                .get(usize::from(*index))
                                .map_or(0, |native| native.results),
                        ),
                        Effect::Spawn { .. } => Awaiting::Spawn,
                        Effect::Await { .. } => Awaiting::Await,
                    };
                    self.state = State::Pending(awaiting);
                    return Step::Pending(effect);
                }
                Ok(Flow::Yield(tuple)) => {
                    self.state = State::Yielded;
                    return Step::Yielded(tuple);
                }
                Ok(Flow::Return(tuple)) => {
                    self.state = State::Finished;
                    return Step::Returned(tuple);
                }
                Err(Fault::Raise(message)) => match self.unwind(message) {
                    Ok(()) => {}
                    Err(Unwound::Uncaught(message)) => {
                        self.state = State::Finished;
                        return Step::Failed {
                            code: FailureCode::RuntimeError,
                            message: bounded_diagnostic(&message),
                        };
                    }
                    // Building the error for a protected call ran out of room. That is a capacity failure, not the
                    // runtime error that was being caught.
                    Err(Unwound::Capacity(what)) => {
                        self.state = State::Finished;
                        return Step::Failed {
                            code: FailureCode::Capacity,
                            message: what.as_bytes().to_vec(),
                        };
                    }
                },
                Err(Fault::Capacity(what)) => {
                    self.state = State::Finished;
                    return Step::Failed {
                        code: FailureCode::Capacity,
                        message: what.as_bytes().to_vec(),
                    };
                }
            }
        }
        Step::Exhausted
    }

    /// Completes the pending effect with `result`. The next [`run`](Self::run) continues the execution.
    ///
    /// A result that does not match the pending effect, such as a value for a native that declares no result, or a value
    /// that breaks the wire limits, is refused before anything is copied into the machine and raised as an error.
    pub fn complete(&mut self, result: EffectResult) -> Result<(), ControlError> {
        let State::Pending(awaiting) = self.state else {
            return Err(ControlError::NotPending);
        };
        self.state = State::Running;
        let wire = self.limits.wire;
        let delivered = match (awaiting, result) {
            (_, EffectResult::Raise(message)) => Err(Fault::Raise(bounded_diagnostic(&message))),
            (Awaiting::Native(1), EffectResult::Value(value)) => match check_value(&value, wire) {
                Ok(()) => import(&mut self.heap, &value).and_then(|v| self.push(v)),
                Err(e) => Err(Fault::Raise(
                    format!("native result is refused: {e}").into_bytes(),
                )),
            },
            (Awaiting::Native(0), EffectResult::Nothing) => Ok(()),
            (Awaiting::Native(_), _) => Err(Fault::Raise(
                b"native returned a different number of results than it declares".to_vec(),
            )),
            (Awaiting::Spawn, EffectResult::Task(task)) => self.push(Val::Task(task)),
            (Awaiting::Await, EffectResult::Tuple(tuple)) => match tuple.check(wire) {
                Ok(()) => import_tuple(&mut self.heap, tuple.values()).and_then(|v| self.push(v)),
                Err(e) => Err(Fault::Raise(
                    format!("child result is refused: {e}").into_bytes(),
                )),
            },
            (_, _) => Err(Fault::Raise(
                b"the effect was completed with the wrong kind of result".to_vec(),
            )),
        };
        match delivered {
            Ok(()) => Ok(()),
            Err(Fault::Raise(message)) => {
                match self.unwind(message) {
                    Ok(()) => {}
                    Err(Unwound::Uncaught(message)) => {
                        self.failure = Some((FailureCode::RuntimeError, message));
                    }
                    Err(Unwound::Capacity(what)) => {
                        self.failure = Some((FailureCode::Capacity, what.as_bytes().to_vec()));
                    }
                }
                Ok(())
            }
            Err(Fault::Capacity(what)) => {
                self.failure = Some((FailureCode::Capacity, what.as_bytes().to_vec()));
                Ok(())
            }
        }
    }

    /// Resumes a yielded machine with `values`, which become the results of the `coroutine.yield` expression. Values
    /// that break the wire limits are refused before anything is copied, and the machine stays yielded.
    pub fn resume_yield(&mut self, values: &[Wire]) -> Result<(), ControlError> {
        if self.state != State::Yielded {
            return Err(ControlError::NotYielded);
        }
        check_values(values, self.limits.wire).map_err(|_| ControlError::Rejected)?;
        let checkpoint = self.heap.checkpoint();
        let imported = import_tuple(&mut self.heap, values);
        let tuple = match imported {
            Ok(tuple) => tuple,
            Err(_) => {
                self.heap.rollback(checkpoint);
                return Err(ControlError::Rejected);
            }
        };
        self.state = State::Running;
        match self.push(tuple) {
            Ok(()) => Ok(()),
            Err(_) => {
                self.failure = Some((FailureCode::Capacity, b"operand stack exhausted".to_vec()));
                Ok(())
            }
        }
    }

    /// Remaining instruction budget.
    pub fn fuel_remaining(&self) -> u64 {
        self.fuel
    }

    /// The verified artifact this machine runs.
    pub fn artifact(&self) -> &Artifact {
        self.program.artifact()
    }

    fn frame(&self) -> Result<&Frame, Fault> {
        self.frames
            .last()
            .ok_or_else(|| internal("no active frame"))
    }

    fn frame_mut(&mut self) -> Result<&mut Frame, Fault> {
        self.frames
            .last_mut()
            .ok_or_else(|| internal("no active frame"))
    }

    fn push(&mut self, value: Val) -> Result<(), Fault> {
        let frame = self.frame()?;
        let height = self.stack.len() - frame.base;
        let max = self
            .program
            .artifact
            .protos
            .get(frame.proto)
            .map_or(0, |proto| usize::from(proto.max_stack));
        if height >= max {
            return Err(Fault::Capacity("operand stack exhausted"));
        }
        // Growth is reserved fallibly, so an allocator refusal is a capacity fault and not an abort.
        self.stack
            .try_reserve(1)
            .map_err(|_| Fault::Capacity("heap allocation failed"))?;
        self.stack.push(value);
        Ok(())
    }

    fn pop(&mut self) -> Result<Val, Fault> {
        let base = self.frame()?.base;
        if self.stack.len() <= base {
            return Err(internal("operand stack underflow"));
        }
        self.stack
            .pop()
            .ok_or_else(|| internal("operand stack underflow"))
    }

    /// Pops `count` values, returned in push order.
    fn pop_many(&mut self, count: usize) -> Result<Vec<Val>, Fault> {
        let base = self.frame()?.base;
        if self.stack.len() < base + count {
            return Err(internal("operand stack underflow"));
        }
        let start = self.stack.len() - count;
        let popped = copy_values(&self.stack[start..])?;
        self.stack.truncate(start);
        Ok(popped)
    }

    fn local(&self, slot: u16) -> Result<Val, Fault> {
        self.frame()?
            .locals
            .get(usize::from(slot))
            .copied()
            .ok_or_else(|| internal("local slot out of range"))
    }

    fn set_local(&mut self, slot: u16, value: Val) -> Result<(), Fault> {
        let local = self
            .frame_mut()?
            .locals
            .get_mut(usize::from(slot))
            .ok_or_else(|| internal("local slot out of range"))?;
        *local = value;
        Ok(())
    }

    fn tuple_value(&mut self, values: Vec<Val>) -> Result<Val, Fault> {
        let id = self.heap.alloc(Object::Tuple(values))?;
        Ok(Val::Object(id))
    }

    /// Builds a tuple of `head` followed by `tail`. The object and byte budgets are checked, and the arena slot is
    /// reserved, before the element buffer is reserved or filled, so a rejected tuple allocates nothing.
    fn tuple_prefixed(&mut self, head: Val, tail: &[Val]) -> Result<Val, Fault> {
        let len = tail
            .len()
            .checked_add(1)
            .ok_or(Fault::Capacity("heap object size overflow"))?;
        self.heap.admit_tuple(len)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(len)
            .map_err(|_| Fault::Capacity("heap allocation failed"))?;
        values.push(head);
        values.extend_from_slice(tail);
        self.tuple_value(values)
    }

    fn step(&mut self) -> Result<Flow, Fault> {
        let (proto_index, pc) = {
            let frame = self.frame()?;
            (frame.proto, frame.pc)
        };
        let instr = *self
            .program
            .artifact
            .protos
            .get(proto_index)
            .and_then(|p| p.code.get(pc))
            .ok_or_else(|| internal("program counter out of range"))?;
        self.frame_mut()?.pc = pc + 1;

        match instr {
            Instr::Nil => self.push(Val::Nil)?,
            Instr::True => self.push(Val::Boolean(true))?,
            Instr::False => self.push(Val::Boolean(false))?,
            Instr::Pop => {
                self.pop()?;
            }
            Instr::Constant(index) => {
                let value = *self
                    .constants
                    .get(index as usize)
                    .ok_or_else(|| internal("constant out of range"))?;
                self.push(value)?;
            }
            Instr::GetLocal(slot) => {
                let value = self.local(slot)?;
                self.push(value)?;
            }
            Instr::SetLocal(slot) => {
                let value = self.pop()?;
                self.set_local(slot, value)?;
            }
            Instr::GetCell(slot) => {
                let cell = self.cell_of_local(slot)?;
                let value = self.read_cell(cell)?;
                self.push(value)?;
            }
            Instr::SetCell(slot) => {
                let value = self.pop()?;
                let cell = self.cell_of_local(slot)?;
                self.write_cell(cell, value)?;
            }
            Instr::NewCell(slot) => {
                let value = self.pop()?;
                let cell = self.heap.alloc(Object::Cell(value))?;
                self.set_local(slot, Val::Object(cell))?;
            }
            Instr::GetUpvalue(index) => {
                let cell = self.upvalue(index)?;
                let value = self.read_cell(cell)?;
                self.push(value)?;
            }
            Instr::SetUpvalue(index) => {
                let value = self.pop()?;
                let cell = self.upvalue(index)?;
                self.write_cell(cell, value)?;
            }
            Instr::Closure(target) => {
                let count = self
                    .program
                    .artifact
                    .protos
                    .get(target as usize)
                    .ok_or_else(|| internal("closure target out of range"))?
                    .upvalues
                    .len();
                // Both budgets are checked, and the arena slot is reserved, before the capture list is reserved. The
                // list is then filled from the artifact one source at a time, so nothing is cloned first.
                self.heap.admit_closure(count)?;
                let mut upvalues = Vec::new();
                upvalues
                    .try_reserve_exact(count)
                    .map_err(|_| Fault::Capacity("heap allocation failed"))?;
                for index in 0..count {
                    let source = self.program.artifact.protos[target as usize].upvalues[index];
                    let cell = match source {
                        UpvalueSource::Local(slot) => self.cell_of_local(slot)?,
                        UpvalueSource::Upvalue(index) => self.upvalue(index)?,
                    };
                    upvalues.push(cell);
                }
                let closure = self.heap.alloc(Object::Closure {
                    proto: target,
                    upvalues,
                })?;
                self.push(Val::Object(closure))?;
            }
            Instr::Add => self.binary_arith(Arith::Add)?,
            Instr::Sub => self.binary_arith(Arith::Sub)?,
            Instr::Mul => self.binary_arith(Arith::Mul)?,
            Instr::Div => self.binary_arith(Arith::Div)?,
            Instr::IDiv => self.binary_arith(Arith::IDiv)?,
            Instr::Mod => self.binary_arith(Arith::Mod)?,
            Instr::Neg => {
                let value = self.pop()?;
                let result = match value {
                    Val::Integer(i) => Val::Integer(i.wrapping_neg()),
                    Val::Float(f) => Val::Float(-f),
                    _ => return Err(arith_error()),
                };
                self.push(result)?;
            }
            Instr::Not => {
                let value = self.pop()?;
                self.push(Val::Boolean(!truthy(value)))?;
            }
            Instr::Concat => {
                let right = self.pop()?;
                let left = self.pop()?;
                let id = self.concat(left, right)?;
                self.push(Val::Object(id))?;
            }
            Instr::Len => {
                let value = self.pop()?;
                let length = match value {
                    Val::Object(id) => match self.heap.get(id)? {
                        Object::Bytes(bytes) => bytes.len() as i64,
                        _ => {
                            return Err(Fault::Raise(
                                b"attempt to get length of a non-string value".to_vec(),
                            ));
                        }
                    },
                    _ => {
                        return Err(Fault::Raise(
                            b"attempt to get length of a non-string value".to_vec(),
                        ));
                    }
                };
                self.push(Val::Integer(length))?;
            }
            Instr::Eq | Instr::Ne => {
                let right = self.pop()?;
                let left = self.pop()?;
                let equal = values_equal(&self.heap, left, right)?;
                let result = if instr == Instr::Eq { equal } else { !equal };
                self.push(Val::Boolean(result))?;
            }
            Instr::Lt => self.compare(false, false)?,
            Instr::Le => self.compare(false, true)?,
            Instr::Gt => self.compare(true, false)?,
            Instr::Ge => self.compare(true, true)?,
            Instr::Jump(target) => self.frame_mut()?.pc = target as usize,
            Instr::JumpIfFalse(target) => {
                let value = self.pop()?;
                if !truthy(value) {
                    self.frame_mut()?.pc = target as usize;
                }
            }
            Instr::JumpIfFalseKeep(target) => {
                let value = *self
                    .stack
                    .last()
                    .ok_or_else(|| internal("operand stack underflow"))?;
                if truthy(value) {
                    self.pop()?;
                } else {
                    self.frame_mut()?.pc = target as usize;
                }
            }
            Instr::JumpIfTrueKeep(target) => {
                let value = *self
                    .stack
                    .last()
                    .ok_or_else(|| internal("operand stack underflow"))?;
                if truthy(value) {
                    self.frame_mut()?.pc = target as usize;
                } else {
                    self.pop()?;
                }
            }
            Instr::Call { argc, results } => {
                let args = self.pop_many(usize::from(argc))?;
                let callee = self.pop()?;
                self.enter(callee, &args, want(results), false)?;
            }
            Instr::Return(count) => {
                let values = self.pop_many(usize::from(count))?;
                return self.return_values(values);
            }
            Instr::ReturnMulti(fixed) => {
                let count = self.pop_marker()?;
                let values = self.pop_many(usize::from(fixed) + count)?;
                return self.return_values(values);
            }
            Instr::CallMulti { fixed, results } => {
                let count = self.pop_marker()?;
                let args = self.pop_many(usize::from(fixed) + count)?;
                let callee = self.pop()?;
                self.enter(callee, &args, want(results), false)?;
            }
            Instr::PcallMulti(fixed) => {
                let count = self.pop_marker()?;
                let args = self.pop_many(usize::from(fixed) + count)?;
                let callee = self.pop()?;
                self.pcall(callee, &args)?;
            }
            Instr::PcallList => {
                let count = self.pop_marker()?;
                let values = self.pop_many(count)?;
                let Some((callee, args)) = values.split_first() else {
                    return Err(Fault::Raise(
                        b"bad argument #1 to 'pcall' (value expected)".to_vec(),
                    ));
                };
                self.pcall(*callee, args)?;
            }
            Instr::PackMulti(fixed) => {
                let count = self.pop_marker()?;
                let count = usize::from(fixed)
                    .checked_add(count)
                    .ok_or(Fault::Capacity("operand stack exhausted"))?;
                let base = self.frame()?.base;
                let start = self
                    .stack
                    .len()
                    .checked_sub(count)
                    .filter(|&start| start >= base)
                    .ok_or_else(|| internal("operand stack underflow"))?;
                self.heap.admit_tuple(count)?;
                let mut values = Vec::new();
                values
                    .try_reserve_exact(count)
                    .map_err(|_| Fault::Capacity("heap allocation failed"))?;
                values.extend_from_slice(&self.stack[start..]);
                self.stack.truncate(start);
                let tuple = self.tuple_value(values)?;
                self.push(tuple)?;
            }
            Instr::YieldMulti(fixed) => {
                let count = self.pop_marker()?;
                let values = self.pop_many(usize::from(fixed) + count)?;
                return self.yield_values(&values);
            }
            Instr::SpawnMulti {
                child,
                fixed,
                captures,
            } => {
                let count = self.pop_marker()?;
                let argument_values = self.pop_many(usize::from(fixed) + count)?;
                let capture_values = self.pop_many(usize::from(captures))?;
                return self.spawn(child, &capture_values, &argument_values);
            }
            Instr::UnpackMulti => {
                let tuple = self.pop()?;
                let values = self.tuple_values(tuple)?;
                let count = u32::try_from(values.len())
                    .map_err(|_| Fault::Capacity("operand stack exhausted"))?;
                for value in values {
                    self.push(value)?;
                }
                self.push(Val::Count(count))?;
            }
            Instr::ForPrep { slot, exit } => self.for_prep(slot, exit)?,
            Instr::ForLoop { slot, body } => self.for_loop(slot, body)?,
            Instr::ReturnTuple => {
                let tuple = self.pop()?;
                let values = self.tuple_values(tuple)?;
                return self.return_values(values);
            }
            Instr::Spawn {
                child,
                argc,
                captures,
            } => {
                let capture_values = self.pop_many(usize::from(captures))?;
                let argument_values = self.pop_many(usize::from(argc))?;
                return self.spawn(child, &capture_values, &argument_values);
            }
            Instr::Await => match self.pop()? {
                Val::Task(task) => return Ok(Flow::Pending(Effect::Await { task })),
                _ => {
                    return Err(Fault::Raise(
                        b"attempt to await a value that is not a task".to_vec(),
                    ));
                }
            },
            Instr::Unpack(count) => {
                let tuple = self.pop()?;
                let values = self.tuple_values(tuple)?;
                self.adjust_and_push(&values, usize::from(count))?;
            }
            Instr::Native(index) => {
                let argc = self
                    .program
                    .artifact
                    .natives
                    .get(usize::from(index))
                    .ok_or_else(|| internal("native out of range"))?
                    .argc;
                let values = self.pop_many(usize::from(argc))?;
                let arguments = export(&self.heap, &values, self.limits.wire, "native arguments")?;
                return Ok(Flow::Pending(Effect::Native {
                    index,
                    arguments: arguments.into_values(),
                }));
            }
            Instr::Yield(argc) => {
                let values = self.pop_many(usize::from(argc))?;
                return self.yield_values(&values);
            }
            Instr::Pcall(argc) => {
                let args = self.pop_many(usize::from(argc))?;
                let callee = self.pop()?;
                self.pcall(callee, &args)?;
            }
            Instr::Raise => {
                let value = self.pop()?;
                let message = match value {
                    Val::Object(id) => match self.heap.get(id)? {
                        Object::Bytes(bytes) => bounded_diagnostic(bytes),
                        _ => b"error object".to_vec(),
                    },
                    _ => b"error value".to_vec(),
                };
                return Err(Fault::Raise(message));
            }
        }
        Ok(Flow::Continue)
    }

    /// Pops the count marker a producer left on top.
    fn pop_marker(&mut self) -> Result<usize, Fault> {
        match self.pop()? {
            Val::Count(count) => Ok(count as usize),
            _ => Err(internal("missing result count")),
        }
    }

    fn pcall(&mut self, callee: Val, args: &[Val]) -> Result<(), Fault> {
        if self.is_closure(callee) {
            self.enter(callee, args, Want::Fixed(1), true)
        } else {
            let message = b"attempt to call a non-function value".to_vec();
            let error = self.error_tuple(message)?;
            self.push(error)
        }
    }

    fn yield_values(&mut self, values: &[Val]) -> Result<Flow, Fault> {
        let tuple = export(&self.heap, values, self.limits.wire, "yielded values")?;
        Ok(Flow::Yield(tuple))
    }

    fn spawn(&mut self, child: u16, captures: &[Val], arguments: &[Val]) -> Result<Flow, Fault> {
        let captures = export(&self.heap, captures, self.limits.wire, "spawn captures")?;
        let arguments = export(&self.heap, arguments, self.limits.wire, "spawn arguments")?;
        Ok(Flow::Pending(Effect::Spawn {
            child,
            captures: captures.into_values(),
            arguments: arguments.into_values(),
        }))
    }

    /// Prepares a numeric for loop as Lua 5.4's `forprep` does.
    fn for_prep(&mut self, slot: u16, exit: u32) -> Result<(), Fault> {
        let init = self.local(slot)?;
        let limit = self.local(slot + 1)?;
        let step = self.local(slot + 2)?;
        if let (Val::Integer(init), Val::Integer(step)) = (init, step) {
            if step == 0 {
                return Err(Fault::Raise(b"'for' step is zero".to_vec()));
            }
            let Some(limit) = for_limit(init, limit, step)? else {
                self.frame_mut()?.pc = exit as usize;
                return Ok(());
            };
            // The iteration count is computed in unsigned arithmetic, so the loop never overflows the index.
            let count = if step > 0 {
                (limit as u64).wrapping_sub(init as u64) / (step as u64)
            } else {
                (init as u64).wrapping_sub(limit as u64) / ((-(step + 1)) as u64 + 1)
            };
            return self.set_local(slot + 1, Val::Integer(count as i64));
        }
        let limit = for_number(limit, b"'for' limit must be a number")?;
        let step = for_number(step, b"'for' step must be a number")?;
        let init = for_number(init, b"'for' initial value must be a number")?;
        if step == 0.0 {
            return Err(Fault::Raise(b"'for' step is zero".to_vec()));
        }
        let skip = if 0.0 < step {
            limit < init
        } else {
            init < limit
        };
        if skip {
            self.frame_mut()?.pc = exit as usize;
            return Ok(());
        }
        self.set_local(slot, Val::Float(init))?;
        self.set_local(slot + 1, Val::Float(limit))?;
        self.set_local(slot + 2, Val::Float(step))
    }

    /// Advances a numeric for loop as Lua 5.4's `forloop` does.
    fn for_loop(&mut self, slot: u16, body: u32) -> Result<(), Fault> {
        match (
            self.local(slot)?,
            self.local(slot + 1)?,
            self.local(slot + 2)?,
        ) {
            (Val::Integer(index), Val::Integer(count), Val::Integer(step)) => {
                let count = count as u64;
                if count > 0 {
                    self.set_local(slot + 1, Val::Integer((count - 1) as i64))?;
                    self.set_local(slot, Val::Integer(index.wrapping_add(step)))?;
                    self.frame_mut()?.pc = body as usize;
                }
                Ok(())
            }
            (Val::Float(index), Val::Float(limit), Val::Float(step)) => {
                let next = index + step;
                let runs = if 0.0 < step {
                    next <= limit
                } else {
                    limit <= next
                };
                if runs {
                    self.set_local(slot, Val::Float(next))?;
                    self.frame_mut()?.pc = body as usize;
                }
                Ok(())
            }
            _ => Err(internal("numeric for state is not prepared")),
        }
    }

    fn is_closure(&self, value: Val) -> bool {
        match value {
            Val::Object(id) => matches!(self.heap.get(id), Ok(Object::Closure { .. })),
            _ => false,
        }
    }

    fn enter(
        &mut self,
        callee: Val,
        args: &[Val],
        want: Want,
        protected: bool,
    ) -> Result<(), Fault> {
        let Val::Object(closure) = callee else {
            return Err(Fault::Raise(
                b"attempt to call a non-function value".to_vec(),
            ));
        };
        let proto_index = match self.heap.get(closure)? {
            Object::Closure { proto, .. } => *proto as usize,
            _ => {
                return Err(Fault::Raise(
                    b"attempt to call a non-function value".to_vec(),
                ));
            }
        };
        if self.frames.len() >= self.limits.max_frames {
            return Err(Fault::Capacity("call depth exhausted"));
        }
        let proto = self
            .program
            .artifact
            .protos
            .get(proto_index)
            .ok_or_else(|| internal("closure proto out of range"))?;
        let mut locals = filled(Val::Nil, usize::from(proto.locals))?;
        for (slot, value) in args.iter().take(usize::from(proto.params)).enumerate() {
            locals[slot] = *value;
        }
        let base = self.stack.len();
        self.frames
            .try_reserve(1)
            .map_err(|_| Fault::Capacity("heap allocation failed"))?;
        self.frames.push(Frame {
            closure,
            proto: proto_index,
            pc: 0,
            locals,
            base,
            want,
            protected,
        });
        Ok(())
    }

    /// Finishes the top frame with `values`, delivering them to its caller or ending the execution.
    fn return_values(&mut self, values: Vec<Val>) -> Result<Flow, Fault> {
        let frame = self
            .frames
            .pop()
            .ok_or_else(|| internal("no frame to return from"))?;
        self.stack.truncate(frame.base);
        if self.frames.is_empty() {
            let tuple = export(&self.heap, &values, self.limits.wire, "returned values")?;
            return Ok(Flow::Return(tuple));
        }
        // A protected call delivers `{true, ...}`. The tuple is admitted against both budgets before its buffer is
        // reserved, and the values are copied from the borrowed result, so nothing is cloned first.
        let wrapped;
        let results: &[Val] = if frame.protected {
            wrapped = [self.tuple_prefixed(Val::Boolean(true), &values)?];
            &wrapped
        } else {
            &values
        };
        match frame.want {
            Want::Fixed(count) => self.adjust_and_push(results, usize::from(count))?,
            Want::Multi => {
                let count = results.len() as u32;
                for value in results {
                    self.push(*value)?;
                }
                self.push(Val::Count(count))?;
            }
        }
        Ok(Flow::Continue)
    }

    fn adjust_and_push(&mut self, values: &[Val], count: usize) -> Result<(), Fault> {
        for index in 0..count {
            let value = values.get(index).copied().unwrap_or(Val::Nil);
            self.push(value)?;
        }
        Ok(())
    }

    /// Unwinds to the nearest protected call and delivers `{false, message}` to it. With none, the message comes back as
    /// [`Unwound::Uncaught`]. If the error value cannot be built or pushed for lack of room, the failure is
    /// [`Unwound::Capacity`]: running out of room while reporting an error is a capacity failure, and it is never
    /// reported as the error that was being caught.
    fn unwind(&mut self, message: Vec<u8>) -> Result<(), Unwound> {
        let Some(index) = self.frames.iter().rposition(|frame| frame.protected) else {
            return Err(Unwound::Uncaught(message));
        };
        let base = self.frames[index].base;
        self.frames.truncate(index);
        self.stack.truncate(base);
        let delivered = self.error_tuple(message).and_then(|error| self.push(error));
        match delivered {
            Ok(()) => Ok(()),
            Err(Fault::Capacity(what)) => Err(Unwound::Capacity(what)),
            // An internal inconsistency while delivering. Nothing caught it, so it ends the execution as an error.
            Err(Fault::Raise(message)) => Err(Unwound::Uncaught(message)),
        }
    }

    /// Builds `{false, message}`. A failure rolls the heap back, so a half-built error leaves no object behind.
    fn error_tuple(&mut self, message: Vec<u8>) -> Result<Val, Fault> {
        let checkpoint = self.heap.checkpoint();
        let built = self
            .heap
            .alloc(Object::Bytes(message))
            .and_then(|text| self.tuple_prefixed(Val::Boolean(false), &[Val::Object(text)]));
        if built.is_err() {
            self.heap.rollback(checkpoint);
        }
        built
    }

    fn upvalue(&self, index: u16) -> Result<ObjId, Fault> {
        let closure = self.frame()?.closure;
        match self.heap.get(closure)? {
            Object::Closure { upvalues, .. } => upvalues
                .get(usize::from(index))
                .copied()
                .ok_or_else(|| internal("upvalue out of range")),
            _ => Err(internal("frame closure is not a closure")),
        }
    }

    fn cell_of_local(&self, slot: u16) -> Result<ObjId, Fault> {
        match self.local(slot)? {
            Val::Object(id) => match self.heap.get(id)? {
                Object::Cell(_) => Ok(id),
                _ => Err(internal("local is not a cell")),
            },
            _ => Err(internal("local is not a cell")),
        }
    }

    fn read_cell(&self, cell: ObjId) -> Result<Val, Fault> {
        match self.heap.get(cell)? {
            Object::Cell(value) => Ok(*value),
            _ => Err(internal("not a cell")),
        }
    }

    fn write_cell(&mut self, cell: ObjId, value: Val) -> Result<(), Fault> {
        match self.heap.get_mut(cell)? {
            Object::Cell(slot) => {
                *slot = value;
                Ok(())
            }
            _ => Err(internal("not a cell")),
        }
    }

    fn tuple_values(&self, tuple: Val) -> Result<Vec<Val>, Fault> {
        match tuple {
            Val::Object(id) => match self.heap.get(id)? {
                Object::Tuple(values) => copy_values(values),
                _ => Err(Fault::Raise(b"expected a tuple".to_vec())),
            },
            _ => Err(Fault::Raise(b"expected a tuple".to_vec())),
        }
    }

    fn binary_arith(&mut self, op: Arith) -> Result<(), Fault> {
        let right = self.pop()?;
        let left = self.pop()?;
        let result = arith(op, left, right)?;
        self.push(result)
    }

    /// Concatenates two operands into a new byte string.
    ///
    /// The result length is summed with checked arithmetic. The heap budget is checked before the buffer is reserved, and
    /// the buffer is reserved fallibly before any byte is copied. The operands are then copied straight from the heap, so
    /// neither one is cloned into a temporary. A float operand has the text Lua 5.4 gives it: see [`crate::number`].
    fn concat(&mut self, left: Val, right: Val) -> Result<ObjId, Fault> {
        let left = self.concat_operand(left)?;
        let right = self.concat_operand(right)?;
        let total = self
            .operand_len(&left)?
            .checked_add(self.operand_len(&right)?)
            .ok_or(Fault::Capacity("concatenation size overflow"))?;
        self.heap.check_bytes(total)?;
        let mut out = byte_buffer(total)?;
        self.append_operand(&mut out, &left)?;
        self.append_operand(&mut out, &right)?;
        self.heap.adopt_bytes(out)
    }

    fn concat_operand(&self, value: Val) -> Result<ConcatOperand, Fault> {
        match value {
            Val::Object(id) => match self.heap.get(id)? {
                Object::Bytes(_) => Ok(ConcatOperand::Bytes(id)),
                _ => Err(Fault::Raise(
                    b"attempt to concatenate a non-string value".to_vec(),
                )),
            },
            Val::Integer(value) => {
                let (digits, len) = decimal(value);
                Ok(ConcatOperand::Digits(digits, len))
            }
            Val::Float(value) => Ok(ConcatOperand::Float(float_text(value))),
            _ => Err(Fault::Raise(
                b"attempt to concatenate a non-string value".to_vec(),
            )),
        }
    }

    fn operand_len(&self, operand: &ConcatOperand) -> Result<usize, Fault> {
        match operand {
            ConcatOperand::Bytes(id) => match self.heap.get(*id)? {
                Object::Bytes(bytes) => Ok(bytes.len()),
                _ => Err(internal("concatenation operand is not a string")),
            },
            ConcatOperand::Digits(_, len) => Ok(*len),
            ConcatOperand::Float(text) => Ok(text.as_bytes().len()),
        }
    }

    fn append_operand(&self, out: &mut Vec<u8>, operand: &ConcatOperand) -> Result<(), Fault> {
        match operand {
            ConcatOperand::Bytes(id) => match self.heap.get(*id)? {
                Object::Bytes(bytes) => out.extend_from_slice(bytes),
                _ => return Err(internal("concatenation operand is not a string")),
            },
            ConcatOperand::Digits(digits, len) => out.extend_from_slice(&digits[..*len]),
            ConcatOperand::Float(text) => out.extend_from_slice(text.as_bytes()),
        }
        Ok(())
    }

    /// Pops two operands and pushes a boolean. `swap` compares the right operand first, which implements `>` and `>=`.
    /// `or_equal` accepts equality, which implements `<=` and `>=`.
    fn compare(&mut self, swap: bool, or_equal: bool) -> Result<(), Fault> {
        let right = self.pop()?;
        let left = self.pop()?;
        let (a, b) = if swap { (right, left) } else { (left, right) };
        let ordering = order(&self.heap, a, b)?;
        let result = match ordering {
            None => false,
            Some(Ordering::Less) => true,
            Some(Ordering::Equal) => or_equal,
            Some(Ordering::Greater) => false,
        };
        self.push(Val::Boolean(result))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arith {
    Add,
    Sub,
    Mul,
    Div,
    IDiv,
    Mod,
}

fn arith(op: Arith, left: Val, right: Val) -> Result<Val, Fault> {
    if let (Val::Integer(a), Val::Integer(b)) = (left, right) {
        return match op {
            Arith::Add => Ok(Val::Integer(a.wrapping_add(b))),
            Arith::Sub => Ok(Val::Integer(a.wrapping_sub(b))),
            Arith::Mul => Ok(Val::Integer(a.wrapping_mul(b))),
            Arith::Div => Ok(Val::Float(a as f64 / b as f64)),
            Arith::IDiv => idiv_int(a, b)
                .map(Val::Integer)
                .ok_or_else(|| Fault::Raise(b"attempt to divide by zero".to_vec())),
            Arith::Mod => mod_int(a, b)
                .map(Val::Integer)
                .ok_or_else(|| Fault::Raise(b"attempt to perform 'n%0'".to_vec())),
        };
    }
    let (Some(a), Some(b)) = (number(left), number(right)) else {
        return Err(arith_error());
    };
    let (x, y) = (a.as_f64(), b.as_f64());
    Ok(Val::Float(match op {
        Arith::Add => x + y,
        Arith::Sub => x - y,
        Arith::Mul => x * y,
        Arith::Div => x / y,
        Arith::IDiv => floor(x / y),
        Arith::Mod => mod_float(x, y),
    }))
}

fn arith_error() -> Fault {
    Fault::Raise(b"attempt to perform arithmetic on a non-number value".to_vec())
}

/// Orders two values. `None` means unordered, which is only possible with NaN.
fn order(heap: &Heap, a: Val, b: Val) -> Result<Option<Ordering>, Fault> {
    if let (Some(x), Some(y)) = (number(a), number(b)) {
        return Ok(cmp_num(x, y));
    }
    if let (Val::Object(x), Val::Object(y)) = (a, b)
        && let (Object::Bytes(left), Object::Bytes(right)) = (heap.get(x)?, heap.get(y)?)
    {
        return Ok(Some(left.as_slice().cmp(right.as_slice())));
    }
    Err(Fault::Raise(
        b"attempt to compare values of incompatible types".to_vec(),
    ))
}

fn values_equal(heap: &Heap, a: Val, b: Val) -> Result<bool, Fault> {
    if let (Some(x), Some(y)) = (number(a), number(b)) {
        return Ok(cmp_num(x, y) == Some(Ordering::Equal));
    }
    Ok(match (a, b) {
        (Val::Nil, Val::Nil) => true,
        (Val::Boolean(x), Val::Boolean(y)) => x == y,
        (Val::Task(x), Val::Task(y)) => x == y,
        (Val::Object(x), Val::Object(y)) => {
            if x == y {
                true
            } else {
                match (heap.get(x)?, heap.get(y)?) {
                    (Object::Bytes(left), Object::Bytes(right)) => left == right,
                    _ => false,
                }
            }
        }
        _ => false,
    })
}

fn truthy(value: Val) -> bool {
    !matches!(value, Val::Nil | Val::Boolean(false))
}

/// The decimal form of an integer, in a fixed buffer. The longest form, `i64::MIN` with its sign, is 20 bytes.
fn decimal(value: i64) -> ([u8; 20], usize) {
    let mut out = [0u8; 20];
    let mut at = out.len();
    let negative = value < 0;
    let mut n = value.unsigned_abs();
    loop {
        at -= 1;
        out[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    if negative {
        at -= 1;
        out[at] = b'-';
    }
    let len = out.len() - at;
    out.copy_within(at.., 0);
    (out, len)
}

/// Why an error could not be delivered to a protected call.
enum Unwound {
    /// No protected call is active. The error ends the execution.
    Uncaught(Vec<u8>),
    /// Delivering the error ran out of room. The execution ends with a capacity failure.
    Capacity(&'static str),
}

/// One operand of `..`, held without a copy. A number's text is held inline, at most [`FLOAT_TEXT_BYTES`] bytes, so
/// converting an operand allocates nothing. The result is sized and admitted before any byte is copied.
enum ConcatOperand {
    Bytes(ObjId),
    Digits([u8; 20], usize),
    Float(FloatText),
}

fn want(results: u8) -> Want {
    if results == RESULTS_MULTI {
        Want::Multi
    } else {
        Want::Fixed(results)
    }
}

/// Converts a numeric-for limit for an integer loop, as Lua 5.4's `forlimit` does. `None` means the loop runs zero times.
fn for_limit(init: i64, limit: Val, step: i64) -> Result<Option<i64>, Fault> {
    let clipped = match limit {
        Val::Integer(limit) => limit,
        Val::Float(f) => match float_to_integer(f, step < 0) {
            Some(limit) => limit,
            None => {
                // A float outside the integer range, or NaN, which compares false like Lua's own check.
                if 0.0 < f {
                    if step < 0 {
                        return Ok(None);
                    }
                    i64::MAX
                } else {
                    if step > 0 {
                        return Ok(None);
                    }
                    i64::MIN
                }
            }
        },
        _ => return Err(Fault::Raise(b"'for' limit must be a number".to_vec())),
    };
    let skip = if step > 0 {
        init > clipped
    } else {
        init < clipped
    };
    Ok((!skip).then_some(clipped))
}

/// Floor (or ceiling when `ceil`) of a float as an integer, when it is in range. NaN is out of range.
fn float_to_integer(f: f64, ceil: bool) -> Option<i64> {
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    let mut rounded = floor(f);
    if ceil && rounded != f {
        rounded += 1.0;
    }
    (-TWO_POW_63..TWO_POW_63)
        .contains(&rounded)
        .then_some(rounded as i64)
}

fn for_number(value: Val, message: &'static [u8]) -> Result<f64, Fault> {
    match value {
        Val::Integer(i) => Ok(i as f64),
        Val::Float(f) => Ok(f),
        _ => Err(Fault::Raise(message.to_vec())),
    }
}

fn refused(message: &'static [u8]) -> Step {
    Step::Failed {
        code: FailureCode::RuntimeError,
        message: message.to_vec(),
    }
}

fn internal(message: &'static str) -> Fault {
    Fault::Raise(message.as_bytes().to_vec())
}

fn start_error(fault: Fault) -> StartError {
    match fault {
        Fault::Raise(_) => StartError::ValueRejected,
        Fault::Capacity(_) => StartError::Capacity,
    }
}

fn intern_constants(heap: &mut Heap, constants: &[Constant]) -> Result<Vec<Val>, Fault> {
    let mut out = Vec::new();
    out.try_reserve_exact(constants.len())
        .map_err(|_| Fault::Capacity("heap allocation failed"))?;
    for constant in constants {
        out.push(match constant {
            Constant::Nil => Val::Nil,
            Constant::Boolean(b) => Val::Boolean(*b),
            Constant::Integer(i) => Val::Integer(*i),
            Constant::Float(f) => Val::Float(*f),
            Constant::Bytes(bytes) => Val::Object(heap.alloc_bytes(bytes)?),
        });
    }
    Ok(out)
}

/// Imports an already validated tuple, admitting its handle buffer before allocating it.
fn import_tuple(heap: &mut Heap, values: &[Wire]) -> Result<Val, Fault> {
    heap.admit_tuple(values.len())?;
    let mut imported = Vec::new();
    imported
        .try_reserve_exact(values.len())
        .map_err(|_| Fault::Capacity("heap allocation failed"))?;
    for value in values {
        imported.push(import(heap, value)?);
    }
    Ok(Val::Object(heap.alloc(Object::Tuple(imported))?))
}

/// Converts a wire value into the heap.
fn import(heap: &mut Heap, value: &Wire) -> Result<Val, Fault> {
    Ok(match value {
        Wire::Nil => Val::Nil,
        Wire::Boolean(b) => Val::Boolean(*b),
        Wire::Integer(i) => Val::Integer(*i),
        Wire::Float(f) => {
            if !f.is_finite() {
                return Err(Fault::Raise(
                    b"non-finite float cannot enter the machine".to_vec(),
                ));
            }
            Val::Float(*f)
        }
        Wire::Bytes(bytes) => Val::Object(heap.alloc_bytes(bytes)?),
    })
}

/// Converts values that leave the machine to a checked wire tuple. Every bound is checked on the borrowed values first:
/// the count, the types, finite floats, each byte string and their checked total. Nothing is copied until all checks
/// pass, so a refused export costs no allocation that depends on the values.
fn export(heap: &Heap, values: &[Val], limits: WireLimits, what: &str) -> Result<WireTuple, Fault> {
    check_export(heap, values, limits)
        .map_err(|reason| Fault::Raise(format!("{what} are refused: {reason}").into_bytes()))?;
    let copied = export_all(heap, values)?;
    WireTuple::new(copied, limits)
        .map_err(|e| Fault::Raise(format!("{what} are refused: {e}").into_bytes()))
}

fn check_export(heap: &Heap, values: &[Val], limits: WireLimits) -> Result<(), &'static str> {
    if values.len() > limits.max_items() {
        return Err("too many values");
    }
    let mut total = 0usize;
    for value in values {
        match value {
            Val::Nil | Val::Boolean(_) | Val::Integer(_) => {}
            Val::Float(f) if f.is_finite() => {}
            Val::Float(_) => return Err("a non-finite float cannot cross a node boundary"),
            Val::Object(id) => match heap.get(*id) {
                Ok(Object::Bytes(bytes)) => {
                    if bytes.len() > limits.max_string_bytes() {
                        return Err("a byte string is too long");
                    }
                    total = total
                        .checked_add(bytes.len())
                        .filter(|&sum| sum <= limits.max_total_bytes())
                        .ok_or("the byte strings are too long in total")?;
                }
                _ => return Err("a value cannot cross a node boundary"),
            },
            Val::Task(_) | Val::Count(_) => return Err("a value cannot cross a node boundary"),
        }
    }
    Ok(())
}

/// Copies values to wire values. Only nil, booleans, numbers and byte strings are remote data. Closures, tuples, cells
/// and task handles stay local, so they are refused here. Call it only after `check_export`.
fn export_all(heap: &Heap, values: &[Val]) -> Result<Vec<Wire>, Fault> {
    let mut out = Vec::new();
    out.try_reserve_exact(values.len())
        .map_err(|_| Fault::Capacity("heap allocation failed"))?;
    for value in values {
        out.push(match value {
            Val::Nil => Wire::Nil,
            Val::Boolean(b) => Wire::Boolean(*b),
            Val::Integer(i) => Wire::Integer(*i),
            Val::Float(f) => Wire::float(*f).map_err(|_| {
                Fault::Raise(b"non-finite float cannot cross a node boundary".to_vec())
            })?,
            Val::Object(id) => match heap.get(*id)? {
                Object::Bytes(bytes) => Wire::Bytes(copy_bytes(bytes)?),
                _ => return Err(Fault::Raise(b"value cannot cross a node boundary".to_vec())),
            },
            Val::Task(_) | Val::Count(_) => {
                return Err(Fault::Raise(b"value cannot cross a node boundary".to_vec()));
            }
        });
    }
    Ok(out)
}
