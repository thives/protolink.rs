//! Monotonic time for scheduling route retries.
//!
//! This is the same model as `protolink::timer`: a [`Clock`] tells the time, and
//! a [`Timer`] can also wait for a point in it. The adaptors keep their own copy
//! so that they depend on no RPC crate. Times are the [`Duration`] elapsed since
//! a fixed origin, which must be the same on every call and must never go
//! backwards.

use core::future::Future;
use core::time::Duration;

/// A monotonic clock.
pub trait Clock {
    /// Time since the clock's origin.
    fn now(&self) -> Duration;
}

/// A [`Clock`] that can also wait.
///
/// The controller runner waits with it when no route can make progress: until
/// the earliest retry deadline of a route, at most one poll interval away, or an
/// application event, whichever comes first.
pub trait Timer: Clock {
    /// Completes once [`now`](Clock::now) has reached `deadline`, and at once if
    /// it already has.
    ///
    /// The future is dropped without completing whenever the runner has
    /// something else to do first.
    fn sleep_until(&self, deadline: Duration) -> impl Future<Output = ()>;
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
