//! Behavior of `link::StdTimer`, the executor-independent ARQ timer.
#![cfg(all(feature = "std", feature = "async"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use protolink::link::{StdTimer, Timer};

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

fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "timed out: {what}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

const SHORT: Duration = Duration::from_millis(30);
const LONG: Duration = Duration::from_secs(3600);
const SETTLE: Duration = Duration::from_millis(250);

#[test]
fn a_stopped_timer_never_expires() {
    let (c, waker) = counter();
    let mut cx = Context::from_waker(&waker);
    let mut timer = StdTimer::new();
    assert!(timer.poll_expired(&mut cx).is_pending());
    std::thread::sleep(SHORT);
    assert!(timer.poll_expired(&mut cx).is_pending());
    assert_eq!(wakes(&c), 0);
}

#[test]
fn expiry_wakes_the_task_and_stays_ready() {
    let (c, waker) = counter();
    let mut cx = Context::from_waker(&waker);
    let mut timer = StdTimer::new();
    let started = Instant::now();
    timer.start(SHORT);
    assert!(timer.poll_expired(&mut cx).is_pending());
    wait_until("wake", || wakes(&c) > 0);
    assert!(started.elapsed() >= SHORT, "woken early");
    for _ in 0..3 {
        assert_eq!(timer.poll_expired(&mut cx), Poll::Ready(()));
    }
}

#[test]
fn start_replaces_the_previous_deadline() {
    let (c, waker) = counter();
    let mut cx = Context::from_waker(&waker);
    let mut timer = StdTimer::new();
    timer.start(SHORT);
    assert!(timer.poll_expired(&mut cx).is_pending());
    timer.start(LONG);
    std::thread::sleep(SHORT + SETTLE);
    assert!(
        timer.poll_expired(&mut cx).is_pending(),
        "old deadline fired"
    );
    assert_eq!(wakes(&c), 0);
    // Shortening works too.
    timer.start(SHORT);
    assert!(timer.poll_expired(&mut cx).is_pending());
    wait_until("wake", || wakes(&c) > 0);
    assert!(timer.poll_expired(&mut cx).is_ready());
}

#[test]
fn stop_disarms_a_pending_deadline_and_an_expiry() {
    let (c, waker) = counter();
    let mut cx = Context::from_waker(&waker);
    let mut timer = StdTimer::new();
    timer.start(SHORT);
    assert!(timer.poll_expired(&mut cx).is_pending());
    timer.stop();
    std::thread::sleep(SHORT + SETTLE);
    assert!(timer.poll_expired(&mut cx).is_pending());
    assert_eq!(wakes(&c), 0);

    timer.start(SHORT);
    assert!(timer.poll_expired(&mut cx).is_pending());
    wait_until("wake", || wakes(&c) > 0);
    assert!(timer.poll_expired(&mut cx).is_ready());
    timer.stop();
    assert!(
        timer.poll_expired(&mut cx).is_pending(),
        "expiry survived stop"
    );
}

#[test]
fn restarting_after_expiry_clears_it_and_old_deadlines_cannot_expire_the_new_one() {
    let (c, waker) = counter();
    let mut cx = Context::from_waker(&waker);
    let mut timer = StdTimer::new();
    timer.start(SHORT);
    assert!(timer.poll_expired(&mut cx).is_pending());
    wait_until("wake", || wakes(&c) > 0);
    assert!(timer.poll_expired(&mut cx).is_ready());

    timer.start(LONG);
    assert!(timer.poll_expired(&mut cx).is_pending(), "stale expiry");
    std::thread::sleep(SHORT + SETTLE);
    assert!(timer.poll_expired(&mut cx).is_pending());
    assert_eq!(wakes(&c), 1);
}

#[test]
fn repolling_replaces_the_registered_waker() {
    let (old, old_waker) = counter();
    let (new, new_waker) = counter();
    let mut timer = StdTimer::new();
    timer.start(SHORT);
    assert!(
        timer
            .poll_expired(&mut Context::from_waker(&old_waker))
            .is_pending()
    );
    assert!(
        timer
            .poll_expired(&mut Context::from_waker(&new_waker))
            .is_pending()
    );
    wait_until("wake", || wakes(&new) > 0);
    std::thread::sleep(SETTLE);
    assert_eq!(wakes(&old), 0, "replaced waker was woken");
    assert_eq!(wakes(&new), 1);
}

#[test]
fn dropping_a_timer_releases_its_waker() {
    let (c, waker) = counter();
    let mut timer = StdTimer::new();
    timer.start(LONG);
    assert!(
        timer
            .poll_expired(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    assert_eq!(Arc::strong_count(&c), 2);
    drop(timer);
    assert_eq!(Arc::strong_count(&c), 1, "timer kept the waker alive");
}

#[test]
fn the_timer_can_move_between_threads() {
    fn assert_send<T: Send>() {}
    assert_send::<StdTimer>();
}
