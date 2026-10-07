//! [`StdTimer`], an executor-independent ARQ retransmission timer.

use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

use super::Timer;

#[derive(Debug, Default)]
struct State {
    /// The armed deadline. `None` when stopped, expired, or too far away to
    /// represent as an `Instant`.
    deadline: Option<Instant>,
    /// Set once the deadline has passed; stays set until `start` or `stop`.
    expired: bool,
    /// The waker of the latest pending `poll_expired`.
    waker: Option<Waker>,
    /// The timer was dropped; the worker must exit.
    closed: bool,
}

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<State>,
    cv: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// A [`Timer`] based on [`std::time::Instant`] that works with any executor.
///
/// All timer state lives behind one lock, so `start`, `stop` and the worker
/// agree on a single current deadline: starting replaces the previous deadline
/// (which can then no longer expire the timer), stopping disarms it, and an
/// expiry stays ready until the timer is started or stopped again.
///
/// The deadline is checked against the system clock on every poll. To wake the
/// task when it passes, the timer starts one helper thread the first time it
/// returns [`Poll::Pending`] and reuses it for every later deadline. The
/// thread sleeps until the deadline, wakes the latest registered waker outside
/// the lock, and exits when the timer is dropped.
///
/// Requires the `std` feature, which `tokio` enables.
#[derive(Debug, Default)]
pub struct StdTimer {
    shared: Arc<Shared>,
    spawned: bool,
}

impl StdTimer {
    /// Creates a stopped timer. No thread is started until needed.
    pub fn new() -> Self {
        Self::default()
    }

    fn spawn(&mut self) {
        if self.spawned {
            return;
        }
        let shared = self.shared.clone();
        std::thread::Builder::new()
            .name("protolink-arq-timer".into())
            .spawn(move || worker(&shared))
            .expect("failed to spawn the ARQ timer thread");
        self.spawned = true;
    }
}

fn worker(shared: &Shared) {
    let mut state = shared.lock();
    loop {
        if state.closed {
            return;
        }
        let Some(deadline) = state.deadline else {
            state = shared.cv.wait(state).unwrap_or_else(|e| e.into_inner());
            continue;
        };
        let now = Instant::now();
        if now < deadline {
            state = shared
                .cv
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
            continue;
        }
        // The deadline read above is still current: it is only replaced under
        // this lock, which has been held since.
        state.deadline = None;
        state.expired = true;
        let waker = state.waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
        state = shared.lock();
    }
}

impl Timer for StdTimer {
    fn start(&mut self, timeout: Duration) {
        let mut state = self.shared.lock();
        state.deadline = Instant::now().checked_add(timeout);
        state.expired = false;
        drop(state);
        if self.spawned {
            self.shared.cv.notify_one();
        }
    }

    fn stop(&mut self) {
        let mut state = self.shared.lock();
        state.deadline = None;
        state.expired = false;
        drop(state);
        if self.spawned {
            self.shared.cv.notify_one();
        }
    }

    fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.shared.lock();
        if state.expired {
            return Poll::Ready(());
        }
        let Some(deadline) = state.deadline else {
            return Poll::Pending;
        };
        if Instant::now() >= deadline {
            state.deadline = None;
            state.expired = true;
            return Poll::Ready(());
        }
        match &state.waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => state.waker = Some(cx.waker().clone()),
        }
        drop(state);
        self.spawn();
        // The worker may already be waiting for an earlier deadline.
        self.shared.cv.notify_one();
        Poll::Pending
    }
}

impl Drop for StdTimer {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.closed = true;
        state.deadline = None;
        let waker = state.waker.take();
        drop(state);
        self.shared.cv.notify_one();
        // Dropping a waker may run arbitrary code; do it outside the lock.
        drop(waker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Wake;

    #[derive(Default)]
    struct Counter(AtomicUsize);

    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counter() -> (Arc<Counter>, Waker) {
        let c = Arc::new(Counter::default());
        (c.clone(), Waker::from(c))
    }

    fn wakes(c: &Counter) -> usize {
        c.0.load(Ordering::SeqCst)
    }

    fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
        let start = Instant::now();
        while !cond() {
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "timed out: {what}"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn dropping_the_timer_stops_its_worker() {
        let (_, waker) = counter();
        let mut cx = Context::from_waker(&waker);
        let mut timer = StdTimer::new();
        timer.start(Duration::from_secs(3600));
        assert!(timer.poll_expired(&mut cx).is_pending());
        let shared = timer.shared.clone();
        // The timer and the worker each hold one reference besides ours.
        wait_for("worker start", || Arc::strong_count(&shared) == 3);
        drop(timer);
        wait_for("worker exit", || Arc::strong_count(&shared) == 1);
    }

    #[test]
    fn one_worker_serves_every_deadline() {
        let (c, waker) = counter();
        let mut cx = Context::from_waker(&waker);
        let mut timer = StdTimer::new();
        for _ in 0..20 {
            timer.start(Duration::from_millis(1));
            while timer.poll_expired(&mut cx).is_pending() {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        assert!(wakes(&c) > 0);
        assert_eq!(Arc::strong_count(&timer.shared), 2);
    }

    #[test]
    fn wakers_run_outside_the_lock() {
        struct Probe {
            shared: Arc<Shared>,
            lock_was_free: AtomicBool,
            woken: AtomicBool,
        }
        impl Wake for Probe {
            fn wake(self: Arc<Self>) {
                let free = self.shared.state.try_lock().is_ok();
                self.lock_was_free.store(free, Ordering::SeqCst);
                self.woken.store(true, Ordering::SeqCst);
            }
        }

        let mut timer = StdTimer::new();
        let probe = Arc::new(Probe {
            shared: timer.shared.clone(),
            lock_was_free: AtomicBool::new(false),
            woken: AtomicBool::new(false),
        });
        let waker = Waker::from(probe.clone());
        let mut cx = Context::from_waker(&waker);
        timer.start(Duration::from_millis(10));
        assert!(timer.poll_expired(&mut cx).is_pending());
        wait_for("wake", || probe.woken.load(Ordering::SeqCst));
        assert!(probe.lock_was_free.load(Ordering::SeqCst));
    }
}
