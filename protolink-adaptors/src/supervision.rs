//! Deadline policy for a supervised slave: one absolute deadline for an open
//! transfer, and a separate one for rearming.
//!
//! This is an optional utility. It has no clock, executor or hardware: the
//! caller supplies the time of each observation and the [`OperationState`] of
//! the pending operation. It is independent of protolink's RPC deadlines and
//! of the mailbox protocol.
//!
//! - The transfer deadline starts at the first observation that sees a transfer
//!   open. Further activity does not renew it. An observation that sees the
//!   transfer closed ends it.
//! - The rearm deadline starts at the first observation that sees the operation
//!   rearming: a slave that is disarmed and waits for the transfer on the wire to
//!   end. Ticks do not renew it. Reaching armed idle or an open transfer ends it.
//!   A slave that is armed and idle is healthy and unlimited, so it has no
//!   deadline at all.
//!
//! Cleanup is not part of the policy. When a deadline expires, the caller
//! disarms the hardware and clears the bus through its own backend, and it must
//! state how far that cancels the operation it supervised.

use core::time::Duration;

/// What one observation concludes about the open transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// No transfer is open, so nothing is supervised.
    Idle,
    /// A transfer is open and has been open for `elapsed`, which is within the
    /// limit.
    Active {
        /// Time since the first observation that saw the transfer open.
        elapsed: Duration,
    },
    /// A transfer has been open for `elapsed`, which is more than the limit.
    Expired {
        /// Time since the first observation that saw the transfer open.
        elapsed: Duration,
    },
}

/// The absolute deadline of the open transfer, measured from the first
/// observation that saw it open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferDeadline {
    limit: Duration,
    first_seen: Option<Duration>,
}

impl TransferDeadline {
    /// A deadline that allows an open transfer to last up to `limit` before it
    /// expires.
    pub const fn new(limit: Duration) -> Self {
        Self {
            limit,
            first_seen: None,
        }
    }

    /// The longest an open transfer may last.
    pub const fn limit(&self) -> Duration {
        self.limit
    }

    /// Judges the transfer at `now`. `open` says whether a transfer addressed to
    /// the slave is open or has data waiting.
    ///
    /// A closed transfer ends the deadline, so the next transfer starts a new
    /// one. The verdict depends only on the first observation that saw the
    /// transfer open, so repeated observations do not extend it.
    pub fn observe(&mut self, now: Duration, open: bool) -> Verdict {
        if !open {
            self.first_seen = None;
            return Verdict::Idle;
        }
        let first_seen = *self.first_seen.get_or_insert(now);
        let elapsed = now.saturating_sub(first_seen);
        if elapsed > self.limit {
            Verdict::Expired { elapsed }
        } else {
            Verdict::Active { elapsed }
        }
    }

    /// Ends the deadline without an observation. Call it when the slave is
    /// disarmed or the operation that held the transfer was abandoned, so that a
    /// later transfer does not inherit this one's start.
    pub fn forget(&mut self) {
        self.first_seen = None;
    }
}

/// What a pending slave operation is doing, as far as supervision is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationState {
    /// The slave is armed and no transfer is addressed to it. This is healthy
    /// listening, and it has no limit.
    Idle,
    /// The slave is disarmed and waits for the transfer on the wire to end, so
    /// that it can be armed at a boundary.
    Rearming,
    /// A transfer addressed to the slave is open, or has data or a latched event
    /// waiting.
    TransferOpen,
}

/// The absolute deadline of a rearm, measured from the first observation that
/// saw the operation rearming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RearmDeadline {
    limit: Duration,
    first_seen: Option<Duration>,
}

impl RearmDeadline {
    /// A deadline that allows a rearm to last up to `limit` before it expires.
    pub const fn new(limit: Duration) -> Self {
        Self {
            limit,
            first_seen: None,
        }
    }

    /// The longest a rearm may last.
    pub const fn limit(&self) -> Duration {
        self.limit
    }

    /// Judges the rearm at `now`. `rearming` says whether the operation is
    /// waiting to rearm.
    ///
    /// Anything else ends the deadline, so the next rearm starts a new one. The
    /// verdict depends only on the first observation that saw the rearm, so
    /// repeated observations do not extend it.
    pub fn observe(&mut self, now: Duration, rearming: bool) -> Verdict {
        if !rearming {
            self.first_seen = None;
            return Verdict::Idle;
        }
        let first_seen = *self.first_seen.get_or_insert(now);
        let elapsed = now.saturating_sub(first_seen);
        if elapsed > self.limit {
            Verdict::Expired { elapsed }
        } else {
            Verdict::Active { elapsed }
        }
    }

    /// Ends the deadline without an observation.
    pub fn forget(&mut self) {
        self.first_seen = None;
    }
}

/// The transfer deadline and the rearm deadline of one supervised slave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deadlines {
    transfer: TransferDeadline,
    rearm: RearmDeadline,
}

impl Deadlines {
    /// Deadlines that allow an open transfer `transfer` and a rearm `rearm`.
    /// Zero limits expire on the first observation, so use
    /// [`checked`](Self::checked) when the limits come from configuration.
    pub const fn new(transfer: Duration, rearm: Duration) -> Self {
        Self {
            transfer: TransferDeadline::new(transfer),
            rearm: RearmDeadline::new(rearm),
        }
    }

    /// As [`new`](Self::new), but `None` when either limit is zero: a limit of
    /// zero cannot tell a slow peer from a dead one.
    pub fn checked(transfer: Duration, rearm: Duration) -> Option<Self> {
        if transfer.is_zero() || rearm.is_zero() {
            return None;
        }
        Some(Self::new(transfer, rearm))
    }

    /// The longest an open transfer may last.
    pub const fn transfer_limit(&self) -> Duration {
        self.transfer.limit()
    }

    /// The longest a rearm may last.
    pub const fn rearm_limit(&self) -> Duration {
        self.rearm.limit()
    }

    /// Ends both deadlines. Call it when the slave is disarmed or the operation
    /// that held them was abandoned, so that a later operation does not inherit
    /// their start.
    pub fn forget(&mut self) {
        self.transfer.forget();
        self.rearm.forget();
    }
}

/// The result of polling one operation under [`Deadlines`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step<T> {
    /// The operation finished with `T`. A finished result is returned even if a
    /// deadline has passed in the same poll.
    Ready(T),
    /// The operation is pending, and every running deadline is within its limit.
    Pending,
    /// The operation is pending and the transfer has been open for `elapsed`,
    /// which is more than the limit.
    TransferExpired {
        /// Time since the first observation that saw the transfer open.
        elapsed: Duration,
    },
    /// The operation is pending and has been rearming for `elapsed`, which is
    /// more than the limit.
    RearmExpired {
        /// Time since the first observation that saw the operation rearming.
        elapsed: Duration,
    },
}

/// Settles one poll of an operation against its deadlines.
///
/// `ready` is the result of polling the operation, or `None` while it is
/// pending. The operation is observed on every poll, so a deadline that starts
/// while an operation finishes is kept by the next operation of the same
/// transfer. A finished result is returned in preference to expiry. A transfer
/// that has closed by `now` counts as completed even when its limit has passed,
/// because the observation decides that.
///
/// Rearming and an open transfer exclude each other: each observation runs only
/// the deadline of its own state and ends the other.
pub fn step<T>(
    deadlines: &mut Deadlines,
    now: Duration,
    state: OperationState,
    ready: Option<T>,
) -> Step<T> {
    let transfer = deadlines
        .transfer
        .observe(now, state == OperationState::TransferOpen);
    let rearm = deadlines
        .rearm
        .observe(now, state == OperationState::Rearming);
    match (ready, transfer, rearm) {
        (Some(result), _, _) => Step::Ready(result),
        (None, Verdict::Expired { elapsed }, _) => Step::TransferExpired { elapsed },
        (None, _, Verdict::Expired { elapsed }) => Step::RearmExpired { elapsed },
        (None, _, _) => Step::Pending,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: fn(u64) -> Duration = Duration::from_millis;

    #[test]
    fn a_transfer_expires_only_after_its_limit() {
        let mut deadline = TransferDeadline::new(MS(10));
        assert_eq!(
            deadline.observe(MS(100), true),
            Verdict::Active { elapsed: MS(0) }
        );
        assert_eq!(
            deadline.observe(MS(110), true),
            Verdict::Active { elapsed: MS(10) },
            "exactly at the limit is still within it"
        );
        assert_eq!(
            deadline.observe(MS(111), true),
            Verdict::Expired { elapsed: MS(11) }
        );
    }

    #[test]
    fn repeated_observations_do_not_renew_a_transfer() {
        let mut deadline = TransferDeadline::new(MS(10));
        deadline.observe(MS(0), true);
        for now in 1..10 {
            deadline.observe(MS(now), true);
        }
        assert!(matches!(
            deadline.observe(MS(11), true),
            Verdict::Expired { .. }
        ));
    }

    #[test]
    fn a_closed_transfer_starts_a_new_deadline_for_the_next() {
        let mut deadline = TransferDeadline::new(MS(10));
        deadline.observe(MS(0), true);
        assert_eq!(deadline.observe(MS(50), false), Verdict::Idle);
        assert_eq!(
            deadline.observe(MS(60), true),
            Verdict::Active { elapsed: MS(0) }
        );
    }

    #[test]
    fn forgetting_drops_the_start_of_an_abandoned_transfer() {
        let mut deadline = TransferDeadline::new(MS(10));
        deadline.observe(MS(0), true);
        deadline.forget();
        assert_eq!(
            deadline.observe(MS(100), true),
            Verdict::Active { elapsed: MS(0) }
        );
    }

    #[test]
    fn a_rearm_is_limited_but_armed_idle_is_not() {
        let mut deadline = RearmDeadline::new(MS(5));
        assert_eq!(deadline.observe(MS(0), false), Verdict::Idle);
        assert_eq!(deadline.observe(MS(1000), false), Verdict::Idle);
        deadline.observe(MS(2000), true);
        assert_eq!(
            deadline.observe(MS(2006), true),
            Verdict::Expired { elapsed: MS(6) }
        );
    }

    #[test]
    fn zero_limits_are_refused_by_the_checked_constructor() {
        assert!(Deadlines::checked(Duration::ZERO, MS(5)).is_none());
        assert!(Deadlines::checked(MS(5), Duration::ZERO).is_none());
        let deadlines = Deadlines::checked(MS(5), MS(7)).unwrap();
        assert_eq!(deadlines.transfer_limit(), MS(5));
        assert_eq!(deadlines.rearm_limit(), MS(7));
    }

    #[test]
    fn a_finished_operation_wins_over_an_expired_deadline() {
        let mut deadlines = Deadlines::new(MS(1), MS(1));
        assert_eq!(
            step(
                &mut deadlines,
                MS(0),
                OperationState::TransferOpen,
                None::<u8>
            ),
            Step::Pending
        );
        // Past the limit, but the operation finished in the same poll.
        assert_eq!(
            step(
                &mut deadlines,
                MS(5),
                OperationState::TransferOpen,
                Some(7u8)
            ),
            Step::Ready(7)
        );
        // The transfer is still open since its first observation, so the next
        // pending poll reports the expiry.
        assert_eq!(
            step(
                &mut deadlines,
                MS(5),
                OperationState::TransferOpen,
                None::<u8>
            ),
            Step::TransferExpired { elapsed: MS(5) }
        );
    }

    #[test]
    fn expiry_is_reported_per_state_and_the_other_deadline_is_reset() {
        let mut deadlines = Deadlines::new(MS(10), MS(10));
        assert_eq!(
            step(&mut deadlines, MS(0), OperationState::Rearming, None::<()>),
            Step::Pending
        );
        assert_eq!(
            step(&mut deadlines, MS(20), OperationState::Rearming, None::<()>),
            Step::RearmExpired { elapsed: MS(20) }
        );
        // An open transfer is judged on its own, so it is not charged for the rearm.
        assert_eq!(
            step(
                &mut deadlines,
                MS(21),
                OperationState::TransferOpen,
                None::<()>
            ),
            Step::Pending
        );
    }

    #[test]
    fn an_idle_slave_is_never_expired() {
        let mut deadlines = Deadlines::new(MS(1), MS(1));
        for now in 0..100 {
            assert_eq!(
                step(&mut deadlines, MS(now), OperationState::Idle, None::<()>),
                Step::Pending
            );
        }
    }
}
