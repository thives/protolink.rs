//! Time sources for deadlines.
//!
//! The sans-IO cores in [`grpc`](crate::grpc) never read a clock. The drivers
//! in this crate do it for them through these traits: a [`Clock`] tells the
//! time, and a [`Timer`] can also wait for it, which the async drivers need to
//! wake up when a deadline passes.
//!
//! Without a clock a driver still *sends* `grpc-timeout` for calls that have a
//! timeout (so a peer that enforces it works), but it doesn't enforce any
//! deadline itself: use [`NoTimer`] (the default) or pick a driver
//! constructor that takes a clock or timer.
//!
//! With `tokio`, [`tokio::TokioTimer`](crate::tokio::TokioTimer) is provided.

use core::future::Future;
use core::time::Duration;

/// A monotonic clock.
///
/// [`now`](Self::now) is the time elapsed since any fixed point (typically the
/// moment the clock was created). It must never go backwards and must come
/// from the same origin on every call.
pub trait Clock {
    /// Time since the clock's origin.
    fn now(&self) -> Duration;
}

/// A [`Clock`] that can also wait.
///
/// Used by the async drivers to wake up when the earliest deadline of a call
/// is reached.
pub trait Timer: Clock {
    /// Complete once [`now`](Clock::now) has reached `deadline`. Returns
    /// immediately if it already has.
    ///
    /// The future is dropped without completing whenever the driver has
    /// something else to do first.
    fn sleep_until(&self, deadline: Duration) -> impl Future<Output = ()>;
}

/// The absence of a clock: time never advances and nothing ever wakes up.
///
/// Calls started through a driver with `NoTimer` still send `grpc-timeout`,
/// but are never failed locally, and a server never expires a call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoTimer;

impl Clock for NoTimer {
    fn now(&self) -> Duration {
        Duration::ZERO
    }
}

impl Timer for NoTimer {
    fn sleep_until(&self, _: Duration) -> impl Future<Output = ()> {
        core::future::pending()
    }
}

impl<T: Clock + ?Sized> Clock for &T {
    fn now(&self) -> Duration {
        (**self).now()
    }
}

impl<T: Timer + ?Sized> Timer for &T {
    fn sleep_until(&self, deadline: Duration) -> impl Future<Output = ()> {
        (**self).sleep_until(deadline)
    }
}
