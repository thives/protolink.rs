//! Lifecycle of one execution record on a node.
//!
//! The transitions follow `docs/LUA-DISTRIBUTED-EXECUTION-PLAN.md` section 3. A resume is a numbered turn. The first
//! turn consumes the saved arguments exactly once. A later turn delivers values to the suspended `coroutine.yield`.
//! Creating the record runs no Lua, and a yield is never resumed automatically.
//!
//! A language result and operational certainty are kept apart. [`Outcome`] says what the Lua code produced. The record's
//! [`Disposition`] says whether every side effect of the record and its descendants is known. The disposition only ever
//! moves from [`Disposition::Certain`] to [`Disposition::Uncertain`], so a return value, a `pcall`, a cancellation or a
//! release cannot clear it.

use alloc::vec::Vec;

use crate::value::Tuple;
use crate::wire::v1;

/// Phase of an execution record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Created with saved arguments. No Lua has run.
    Ready,
    /// A turn is executing.
    Running,
    /// A turn waits on a native or child operation.
    NativeWait,
    /// A turn was suspended for fuel. This is not a Lua yield.
    FuelSuspended,
    /// The entry coroutine yielded and waits for the next resume.
    LuaYielded,
    /// Cancellation was requested and cleanup is still running. An acknowledgement of `Cancel` is not proof that the
    /// work has stopped; this phase lasts until it has, or until the cleanup deadline ends it as uncertain.
    Cancelling,
    /// The entry returned. The outcome is retained.
    Returned,
    /// The turn failed. The failure is retained.
    Failed,
    /// The retained outcome was released.
    Released,
}

impl Phase {
    /// The wire state for this phase.
    pub fn to_wire(self) -> v1::State {
        match self {
            Self::Ready => v1::State::Ready,
            Self::Running => v1::State::Running,
            Self::NativeWait => v1::State::NativeWait,
            Self::FuelSuspended => v1::State::FuelSuspended,
            Self::LuaYielded => v1::State::LuaYielded,
            Self::Cancelling => v1::State::Cancelling,
            Self::Returned => v1::State::Returned,
            Self::Failed => v1::State::Failed,
            Self::Released => v1::State::Released,
        }
    }

    /// The phase of a wire state. `None` for an unspecified or unknown state.
    pub fn from_wire(state: v1::State) -> Option<Self> {
        Some(match state {
            v1::State::Ready => Self::Ready,
            v1::State::Running => Self::Running,
            v1::State::NativeWait => Self::NativeWait,
            v1::State::FuelSuspended => Self::FuelSuspended,
            v1::State::LuaYielded => Self::LuaYielded,
            v1::State::Cancelling => Self::Cancelling,
            v1::State::Returned => Self::Returned,
            v1::State::Failed => Self::Failed,
            v1::State::Released => Self::Released,
            _ => return None,
        })
    }

    /// True when no further turn can run.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Returned | Self::Failed | Self::Released)
    }
}

/// Whether every side effect of an execution and its descendants is known.
///
/// It is monotonic: [`Disposition::merge`] never turns `Uncertain` back into `Certain`. A returned value may coexist
/// with an uncertain disposition, and a caller must not report such a run as an ordinary success.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Disposition {
    /// Every side effect is known.
    #[default]
    Certain,
    /// A side effect of unknown outcome may have happened.
    Uncertain,
}

impl Disposition {
    /// The disposition carried by a wire `uncertain` flag.
    pub const fn from_flag(uncertain: bool) -> Self {
        if uncertain {
            Self::Uncertain
        } else {
            Self::Certain
        }
    }

    /// True when a side effect of unknown outcome may have happened.
    pub const fn is_uncertain(self) -> bool {
        matches!(self, Self::Uncertain)
    }

    /// The stronger of two dispositions. Uncertainty always wins.
    #[must_use]
    pub fn merge(self, other: Self) -> Self {
        self.max(other)
    }

    /// Escalates in place. Nothing can lower it again.
    pub fn absorb(&mut self, other: Self) {
        *self = self.merge(other);
    }
}

/// Class of a runtime failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureCode {
    /// An ordinary Lua error.
    RuntimeError,
    /// A native operation failed.
    NativeFailed,
    /// A descendant's outcome is unknown.
    ChildUnknown,
    /// Cancellation or lease expiry stopped the work.
    Cancelled,
    /// The total fuel or deadline budget was exhausted.
    BudgetExhausted,
    /// A bounded resource was full.
    Capacity,
}

impl FailureCode {
    /// The wire code for this failure class.
    pub fn to_wire(self) -> v1::FailureCode {
        match self {
            Self::RuntimeError => v1::FailureCode::RuntimeError,
            Self::NativeFailed => v1::FailureCode::NativeFailed,
            Self::ChildUnknown => v1::FailureCode::ChildUnknown,
            Self::Cancelled => v1::FailureCode::Cancelled,
            Self::BudgetExhausted => v1::FailureCode::BudgetExhausted,
            Self::Capacity => v1::FailureCode::Capacity,
        }
    }

    /// The failure class of a wire code. An unspecified or unknown code is an ordinary runtime error.
    pub fn from_wire(code: v1::FailureCode) -> Self {
        match code {
            v1::FailureCode::NativeFailed => Self::NativeFailed,
            v1::FailureCode::ChildUnknown => Self::ChildUnknown,
            v1::FailureCode::Cancelled => Self::Cancelled,
            v1::FailureCode::BudgetExhausted => Self::BudgetExhausted,
            v1::FailureCode::Capacity => Self::Capacity,
            _ => Self::RuntimeError,
        }
    }
}

/// Longest diagnostic text a failure keeps. Longer text is cut, so a failure never grows a record or a reply without
/// bound.
pub const MAX_DIAGNOSTIC_BYTES: usize = 160;

/// A structured failure of one turn. Its certainty is the record's [`Disposition`], not a field of the failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    /// Failure class.
    pub code: FailureCode,
    /// Bounded diagnostic text. It is not a Lua error object.
    pub message: Vec<u8>,
}

impl Failure {
    /// A failure whose message is cut to [`MAX_DIAGNOSTIC_BYTES`].
    pub fn new(code: FailureCode, message: &[u8]) -> Self {
        Self {
            code,
            message: bounded_diagnostic(message),
        }
    }
}

/// Copies at most [`MAX_DIAGNOSTIC_BYTES`] of `message`. The copy is bounded before it is made.
pub fn bounded_diagnostic(message: &[u8]) -> Vec<u8> {
    message[..message.len().min(MAX_DIAGNOSTIC_BYTES)].to_vec()
}

/// The tagged outcome of one turn.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// The entry coroutine yielded these values.
    Yielded(Tuple),
    /// The entry returned these values.
    Returned(Tuple),
    /// The turn failed.
    Failed(Failure),
}

/// What an observer learned about one turn: the language outcome and the disposition at the time of the observation.
/// It is owned, so it can be cached, forwarded and delivered without borrowing the record.
#[derive(Clone, Debug, PartialEq)]
pub struct Observation {
    /// The language outcome.
    pub outcome: Outcome,
    /// Whether every side effect behind it is known.
    pub disposition: Disposition,
}

/// Something the engine observed while a turn ran.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    /// A native or child operation is pending.
    NativeWait,
    /// The slice ran out of fuel.
    FuelSuspended,
    /// The pending operation completed or a slice resumed.
    Resumed,
    /// The entry coroutine yielded.
    Yielded(Tuple),
    /// The entry returned.
    Returned(Tuple),
    /// The turn failed.
    Failed(Failure),
}

/// What a newly admitted turn delivers into the engine.
#[derive(Clone, Debug, PartialEq)]
pub enum Delivery {
    /// The saved arguments of the first turn, consumed once.
    EntryArguments(Tuple),
    /// Values that become the results of the suspended `coroutine.yield`.
    YieldReply(Tuple),
}

/// Why a lifecycle transition was refused. A refused transition changes no state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransitionError {
    /// The first resume carried values. They are rejected, never appended to the saved arguments.
    ExtraArgumentsOnFirstResume,
    /// The record is not in a phase that accepts this operation.
    NotAllowed(Phase),
    /// The turn number does not match the next turn.
    TurnMismatch {
        /// The turn the record expects next.
        expected: u32,
        /// The turn the caller sent.
        got: u32,
    },
}

/// One execution record: phase, turn counter, saved arguments, the retained outcome and the one authoritative
/// disposition.
#[derive(Clone, Debug, PartialEq)]
pub struct Execution {
    phase: Phase,
    turn: u32,
    saved: Option<Tuple>,
    outcome: Option<(u32, Outcome)>,
    disposition: Disposition,
}

impl Execution {
    /// Creates a `Ready` record that holds the initial arguments.
    pub fn new(arguments: Tuple) -> Self {
        Self {
            phase: Phase::Ready,
            turn: 0,
            saved: Some(arguments),
            outcome: None,
            disposition: Disposition::Certain,
        }
    }

    /// Current phase.
    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// The number of the next turn the record accepts.
    pub fn turn(&self) -> u32 {
        self.turn
    }

    /// The record's disposition. It covers the record and every descendant it has learned about.
    pub fn disposition(&self) -> Disposition {
        self.disposition
    }

    /// Escalates the disposition. Nothing can lower it again.
    pub fn absorb(&mut self, disposition: Disposition) {
        self.disposition.absorb(disposition);
    }

    /// Admits resume turn `turn` carrying `values`.
    ///
    /// Turn 0 is the first resume. It must carry no values and returns the saved arguments once. A later turn must
    /// follow a yield and returns the values as the reply to that yield.
    pub fn admit(&mut self, turn: u32, values: Tuple) -> Result<Delivery, TransitionError> {
        if turn != self.turn {
            return Err(TransitionError::TurnMismatch {
                expected: self.turn,
                got: turn,
            });
        }
        match self.phase {
            Phase::Ready => {
                if !values.is_empty() {
                    return Err(TransitionError::ExtraArgumentsOnFirstResume);
                }
                let arguments = self.saved.take().unwrap_or_else(Tuple::empty);
                self.begin(Phase::Running);
                Ok(Delivery::EntryArguments(arguments))
            }
            Phase::LuaYielded => {
                self.begin(Phase::Running);
                Ok(Delivery::YieldReply(values))
            }
            other => Err(TransitionError::NotAllowed(other)),
        }
    }

    fn begin(&mut self, phase: Phase) {
        self.phase = phase;
        self.turn += 1;
        self.outcome = None;
    }

    /// Records an engine event. A turn-ending event retains its outcome under the turn that produced it.
    pub fn observe(&mut self, event: Event) -> Result<(), TransitionError> {
        match event {
            Event::Resumed => match self.phase {
                Phase::NativeWait | Phase::FuelSuspended => self.phase = Phase::Running,
                other => return Err(TransitionError::NotAllowed(other)),
            },
            Event::NativeWait => match self.phase {
                Phase::Running => self.phase = Phase::NativeWait,
                other => return Err(TransitionError::NotAllowed(other)),
            },
            Event::FuelSuspended => match self.phase {
                Phase::Running => self.phase = Phase::FuelSuspended,
                other => return Err(TransitionError::NotAllowed(other)),
            },
            Event::Yielded(values) => {
                self.finish_turn(Phase::LuaYielded, Outcome::Yielded(values))?
            }
            Event::Returned(values) => {
                self.finish_turn(Phase::Returned, Outcome::Returned(values))?
            }
            Event::Failed(failure) => self.finish_turn(Phase::Failed, Outcome::Failed(failure))?,
        }
        Ok(())
    }

    fn finish_turn(&mut self, phase: Phase, outcome: Outcome) -> Result<(), TransitionError> {
        match self.phase {
            Phase::Running | Phase::NativeWait | Phase::FuelSuspended => {}
            other => return Err(TransitionError::NotAllowed(other)),
        }
        self.phase = phase;
        self.outcome = Some((self.turn - 1, outcome));
        Ok(())
    }

    /// Returns the retained outcome of turn `turn`, if that turn has produced one and it is still retained.
    pub fn outcome(&self, turn: u32) -> Option<&Outcome> {
        match &self.outcome {
            Some((produced, outcome)) if *produced == turn => Some(outcome),
            _ => None,
        }
    }

    /// Returns the retained outcome of turn `turn` with the record's current disposition.
    pub fn observation(&self, turn: u32) -> Option<Observation> {
        self.outcome(turn).map(|outcome| Observation {
            outcome: outcome.clone(),
            disposition: self.disposition,
        })
    }

    /// Starts cancellation. The record stays `Cancelling` until [`Execution::finish_cancel`]. Cancelling a terminal or
    /// already cancelling record is refused.
    pub fn cancel(&mut self) -> Result<(), TransitionError> {
        if self.phase.is_terminal() || self.phase == Phase::Cancelling {
            return Err(TransitionError::NotAllowed(self.phase));
        }
        self.phase = Phase::Cancelling;
        Ok(())
    }

    /// Completes cancellation with a retained failure. `disposition` says whether cleanup proved that the work stopped.
    /// Only a cancelling record may finish cancellation.
    pub fn finish_cancel(&mut self, disposition: Disposition) -> Result<(), TransitionError> {
        if self.phase != Phase::Cancelling {
            return Err(TransitionError::NotAllowed(self.phase));
        }
        let message: &[u8] = if disposition.is_uncertain() {
            b"cancelled; cleanup did not prove that every effect stopped"
        } else {
            b"cancelled"
        };
        self.absorb(disposition);
        self.phase = Phase::Failed;
        self.saved = None;
        self.outcome = Some((
            self.turn.saturating_sub(1),
            Outcome::Failed(Failure::new(FailureCode::Cancelled, message)),
        ));
        Ok(())
    }

    /// Releases the retained outcome. Only a terminal record can be released, so a live task is never detached. The
    /// disposition survives the release.
    pub fn release(&mut self) -> Result<(), TransitionError> {
        match self.phase {
            Phase::Returned | Phase::Failed => {
                self.phase = Phase::Released;
                self.outcome = None;
                Ok(())
            }
            other => Err(TransitionError::NotAllowed(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{Limits, Value};
    use alloc::vec;

    const LIMITS: Limits = Limits::new(8, 64, 256);

    fn tuple(values: Vec<Value>) -> Tuple {
        Tuple::new(values, LIMITS).expect("within limits")
    }

    #[test]
    fn first_resume_consumes_saved_arguments_once() {
        let mut record = Execution::new(tuple(vec![Value::Integer(7)]));
        assert_eq!(record.phase(), Phase::Ready);
        let delivery = record.admit(0, Tuple::empty()).expect("first resume");
        assert_eq!(
            delivery,
            Delivery::EntryArguments(tuple(vec![Value::Integer(7)]))
        );
        assert_eq!(record.phase(), Phase::Running);
        assert_eq!(record.turn(), 1);
        record
            .observe(Event::Returned(Tuple::empty()))
            .expect("return");
        assert_eq!(
            record.admit(1, Tuple::empty()),
            Err(TransitionError::NotAllowed(Phase::Returned))
        );
    }

    #[test]
    fn extra_first_resume_arguments_leave_state_unchanged() {
        let mut record = Execution::new(tuple(vec![Value::Integer(1)]));
        let before = record.clone();
        let refused = record.admit(0, tuple(vec![Value::Nil]));
        assert_eq!(refused, Err(TransitionError::ExtraArgumentsOnFirstResume));
        assert_eq!(record, before);
    }

    #[test]
    fn yield_then_resume_delivers_reply_values_not_arguments() {
        let mut record = Execution::new(Tuple::empty());
        record.admit(0, Tuple::empty()).expect("first");
        record
            .observe(Event::Yielded(tuple(vec![Value::Boolean(true)])))
            .expect("yield");
        assert_eq!(record.phase(), Phase::LuaYielded);
        assert!(matches!(record.outcome(0), Some(Outcome::Yielded(_))));
        let delivery = record
            .admit(1, tuple(vec![Value::Nil, Value::Nil]))
            .expect("resume");
        assert_eq!(
            delivery,
            Delivery::YieldReply(tuple(vec![Value::Nil, Value::Nil]))
        );
        assert_eq!(record.phase(), Phase::Running);
    }

    #[test]
    fn turn_numbers_must_match_exactly() {
        let mut record = Execution::new(Tuple::empty());
        assert_eq!(
            record.admit(1, Tuple::empty()),
            Err(TransitionError::TurnMismatch {
                expected: 0,
                got: 1
            })
        );
    }

    #[test]
    fn native_and_fuel_waits_do_not_produce_outcomes() {
        let mut record = Execution::new(Tuple::empty());
        record.admit(0, Tuple::empty()).expect("first");
        record.observe(Event::NativeWait).expect("wait");
        assert_eq!(record.phase(), Phase::NativeWait);
        record.observe(Event::Resumed).expect("resume");
        record.observe(Event::FuelSuspended).expect("fuel");
        record.observe(Event::Resumed).expect("resume");
        assert!(record.outcome(0).is_none());
        assert_eq!(record.phase(), Phase::Running);
    }

    #[test]
    fn an_uncertain_disposition_survives_a_later_return_and_the_release() {
        let mut record = Execution::new(Tuple::empty());
        record.admit(0, Tuple::empty()).expect("first");
        record.absorb(Disposition::Uncertain);
        record
            .observe(Event::Returned(tuple(vec![Value::Integer(1)])))
            .expect("return");
        let observed = record.observation(0).expect("retained");
        assert!(matches!(observed.outcome, Outcome::Returned(_)));
        assert_eq!(observed.disposition, Disposition::Uncertain);
        record.absorb(Disposition::Certain);
        assert_eq!(
            record.disposition(),
            Disposition::Uncertain,
            "never lowered"
        );
        record.release().expect("release");
        assert_eq!(record.disposition(), Disposition::Uncertain);
    }

    #[test]
    fn disposition_merge_is_monotonic() {
        use Disposition::{Certain, Uncertain};
        for (a, b, merged) in [
            (Certain, Certain, Certain),
            (Certain, Uncertain, Uncertain),
            (Uncertain, Certain, Uncertain),
            (Uncertain, Uncertain, Uncertain),
        ] {
            assert_eq!(a.merge(b), merged);
        }
        assert_eq!(Disposition::from_flag(true), Uncertain);
        assert!(!Disposition::from_flag(false).is_uncertain());
    }

    #[test]
    fn cancel_waits_in_cancelling_until_cleanup_finishes() {
        let mut record = Execution::new(Tuple::empty());
        record.admit(0, Tuple::empty()).expect("first");
        record.cancel().expect("cancel");
        assert_eq!(record.phase(), Phase::Cancelling);
        assert!(record.outcome(0).is_none(), "nothing is final yet");
        assert_eq!(
            record.cancel(),
            Err(TransitionError::NotAllowed(Phase::Cancelling))
        );
        record
            .finish_cancel(Disposition::Uncertain)
            .expect("finish");
        let observed = record.observation(0).expect("retained");
        assert!(
            matches!(observed.outcome, Outcome::Failed(ref f) if f.code == FailureCode::Cancelled)
        );
        assert_eq!(observed.disposition, Disposition::Uncertain);
    }

    #[test]
    fn release_only_applies_to_terminal_records() {
        let mut record = Execution::new(Tuple::empty());
        record.admit(0, Tuple::empty()).expect("first");
        record
            .observe(Event::Yielded(Tuple::empty()))
            .expect("yield");
        assert_eq!(
            record.release(),
            Err(TransitionError::NotAllowed(Phase::LuaYielded))
        );
        record.cancel().expect("cancel");
        assert_eq!(
            record.release(),
            Err(TransitionError::NotAllowed(Phase::Cancelling))
        );
        record.finish_cancel(Disposition::Certain).expect("finish");
        assert!(
            matches!(record.outcome(0), Some(Outcome::Failed(f)) if f.code == FailureCode::Cancelled)
        );
        record.release().expect("release terminal");
        assert_eq!(record.phase(), Phase::Released);
    }

    #[test]
    fn cancel_after_terminal_is_refused() {
        let mut record = Execution::new(Tuple::empty());
        record.admit(0, Tuple::empty()).expect("first");
        record
            .observe(Event::Returned(Tuple::empty()))
            .expect("return");
        assert_eq!(
            record.cancel(),
            Err(TransitionError::NotAllowed(Phase::Returned))
        );
    }

    #[test]
    fn diagnostics_are_bounded_before_they_are_copied() {
        let long = [b'x'; MAX_DIAGNOSTIC_BYTES * 3];
        let failure = Failure::new(FailureCode::RuntimeError, &long);
        assert_eq!(failure.message.len(), MAX_DIAGNOSTIC_BYTES);
        assert_eq!(
            Failure::new(FailureCode::RuntimeError, b"ok").message,
            b"ok"
        );
    }

    #[test]
    fn wire_codes_and_states_round_trip() {
        for code in [
            FailureCode::RuntimeError,
            FailureCode::NativeFailed,
            FailureCode::ChildUnknown,
            FailureCode::Cancelled,
            FailureCode::BudgetExhausted,
            FailureCode::Capacity,
        ] {
            assert_eq!(FailureCode::from_wire(code.to_wire()), code);
        }
        for phase in [
            Phase::Ready,
            Phase::Running,
            Phase::NativeWait,
            Phase::FuelSuspended,
            Phase::LuaYielded,
            Phase::Cancelling,
            Phase::Returned,
            Phase::Failed,
            Phase::Released,
        ] {
            assert_eq!(Phase::from_wire(phase.to_wire()), Some(phase));
        }
        assert_eq!(Phase::from_wire(v1::State::Unspecified), None);
    }
}
