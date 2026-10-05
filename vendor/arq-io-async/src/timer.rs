use core::task::{Context, Poll};
use core::time::Duration;

/// A one-shot timer that schedules retransmissions in [`Arq`](crate::Arq).
///
/// `Arq` decides when to start and stop the timer and how long each timeout
/// is; the timer only has to measure time and wake the polling task. This
/// keeps retransmission independent of any runtime: implement the trait on
/// top of your platform's timer, or use `StdTimer` with the `std` feature.
///
/// Implementations must not block. `Arq` polls the timer from its own
/// `poll_*` methods with the context of whichever operation is being driven.
///
/// # Example
///
/// An implementation on top of [`embassy-time`](https://docs.rs/embassy-time):
///
/// ```
/// use core::future::Future;
/// use core::pin::Pin;
/// use core::task::{Context, Poll};
///
/// #[derive(Default)]
/// struct EmbassyTimer(Option<embassy_time::Timer>);
///
/// impl arq_io_async::Timer for EmbassyTimer {
///     fn start(&mut self, timeout: core::time::Duration) {
///         let us = u64::try_from(timeout.as_micros()).unwrap_or(u64::MAX);
///         self.0 = Some(embassy_time::Timer::after_micros(us));
///     }
///
///     fn stop(&mut self) {
///         self.0 = None;
///     }
///
///     fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<()> {
///         match &mut self.0 {
///             Some(timer) => Pin::new(timer).poll(cx),
///             None => Poll::Pending,
///         }
///     }
/// }
/// #
/// # use arq_io_async::Timer;
/// # use std::sync::Arc;
/// # use std::sync::atomic::{AtomicBool, Ordering};
/// # use std::task::{Wake, Waker};
/// # use std::time::{Duration, Instant};
/// #
/// # struct Flag(AtomicBool);
/// # impl Wake for Flag {
/// #     fn wake(self: Arc<Self>) {
/// #         self.0.store(true, Ordering::SeqCst);
/// #     }
/// # }
/// #
/// # let flag = Arc::new(Flag(AtomicBool::new(false)));
/// # let waker = Waker::from(flag.clone());
/// # let mut cx = Context::from_waker(&waker);
/// # let mut timer = EmbassyTimer::default();
/// # assert!(timer.poll_expired(&mut cx).is_pending(), "stopped timer expired");
/// #
/// # let started = Instant::now();
/// # timer.start(Duration::from_millis(20));
/// # assert!(timer.poll_expired(&mut cx).is_pending(), "expired early");
/// # while !flag.0.load(Ordering::SeqCst) {
/// #     assert!(started.elapsed() < Duration::from_secs(5), "waker never woken");
/// #     std::thread::sleep(Duration::from_millis(1));
/// # }
/// # assert!(started.elapsed() >= Duration::from_millis(20), "woken early");
/// # assert!(timer.poll_expired(&mut cx).is_ready(), "not expired after wake");
/// # assert!(timer.poll_expired(&mut cx).is_ready(), "expiry must persist");
/// #
/// # timer.stop();
/// # assert!(timer.poll_expired(&mut cx).is_pending(), "expired after stop");
/// ```
pub trait Timer {
    /// Arms the timer to expire `timeout` from now, replacing any previous
    /// deadline.
    fn start(&mut self, timeout: Duration);

    /// Disarms the timer. [`Timer::poll_expired`] must return
    /// [`Poll::Pending`] until the timer is started again.
    fn stop(&mut self);

    /// Returns [`Poll::Ready`] once the deadline set by [`Timer::start`] has
    /// passed, and keeps returning it until the timer is started or stopped
    /// again.
    ///
    /// While the deadline has not passed, returns [`Poll::Pending`] and
    /// arranges for the waker in `cx` to be woken when it does. Only the
    /// waker from the most recent call needs to be woken.
    fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<()>;
}

#[cfg(feature = "std")]
pub use self::std_timer::StdTimer;

#[cfg(feature = "std")]
mod std_timer {
    use core::task::{Context, Poll, Waker};
    use core::time::Duration;
    use std::sync::{Arc, Condvar, Mutex, MutexGuard};
    use std::time::Instant;

    use super::Timer;

    #[derive(Debug, Default)]
    struct Inner {
        deadline: Option<Instant>,
        waker: Option<Waker>,
        closed: bool,
    }

    #[derive(Debug, Default)]
    struct Shared {
        inner: Mutex<Inner>,
        cv: Condvar,
    }

    impl Shared {
        fn lock(&self) -> MutexGuard<'_, Inner> {
            self.inner.lock().unwrap_or_else(|e| e.into_inner())
        }
    }

    /// A [`Timer`] based on [`std::time::Instant`] that works with any
    /// executor.
    ///
    /// The deadline is checked against the system clock on every poll. To wake
    /// the task when the deadline passes, the timer starts a helper thread the
    /// first time it returns [`Poll::Pending`]. The thread sleeps until the
    /// deadline and exits when the timer is dropped. Each timer has at most
    /// one helper thread.
    ///
    /// Requires the `std` feature, which `tokio` enables.
    #[derive(Debug, Default)]
    pub struct StdTimer {
        deadline: Option<Instant>,
        shared: Arc<Shared>,
        spawned: bool,
    }

    impl StdTimer {
        /// Creates a stopped timer. No thread is started until needed.
        pub fn new() -> Self {
            Self::default()
        }

        fn set_deadline(&mut self, deadline: Option<Instant>) {
            self.deadline = deadline;
            if self.spawned {
                self.shared.lock().deadline = deadline;
                self.shared.cv.notify_one();
            }
        }

        fn spawn(&mut self) {
            if self.spawned {
                return;
            }
            self.spawned = true;
            self.shared.lock().deadline = self.deadline;
            let shared = self.shared.clone();
            std::thread::spawn(move || {
                let mut inner = shared.lock();
                loop {
                    if inner.closed {
                        return;
                    }
                    match (inner.deadline, inner.waker.is_some()) {
                        (Some(deadline), true) => {
                            let now = Instant::now();
                            if now >= deadline {
                                let waker = inner.waker.take();
                                drop(inner);
                                if let Some(w) = waker {
                                    w.wake();
                                }
                                inner = shared.lock();
                            } else {
                                inner = shared
                                    .cv
                                    .wait_timeout(inner, deadline - now)
                                    .unwrap_or_else(|e| e.into_inner())
                                    .0;
                            }
                        }
                        _ => {
                            inner = shared.cv.wait(inner).unwrap_or_else(|e| e.into_inner());
                        }
                    }
                }
            });
        }
    }

    impl Timer for StdTimer {
        fn start(&mut self, timeout: Duration) {
            self.set_deadline(Some(Instant::now() + timeout));
        }

        fn stop(&mut self) {
            self.set_deadline(None);
        }

        fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<()> {
            let Some(deadline) = self.deadline else {
                return Poll::Pending;
            };
            if Instant::now() >= deadline {
                return Poll::Ready(());
            }
            self.spawn();
            let mut inner = self.shared.lock();
            match &inner.waker {
                Some(w) if w.will_wake(cx.waker()) => {}
                _ => inner.waker = Some(cx.waker().clone()),
            }
            drop(inner);
            self.shared.cv.notify_one();
            Poll::Pending
        }
    }

    impl Drop for StdTimer {
        fn drop(&mut self) {
            if self.spawned {
                self.shared.lock().closed = true;
                self.shared.cv.notify_one();
            }
        }
    }
}
