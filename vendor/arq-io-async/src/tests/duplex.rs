use core::future::Future;
use core::pin::{Pin, pin};
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Wake;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

use super::{Clock, Crc16X25, ManualTimer, new_arq};
use crate::ack_codec::BchAckCodec;
use crate::{Arq, ArqLayer, r};

type DuplexArq = Arq<4, 16, { r::<4>() }, DuplexStream, Crc16X25, BchAckCodec, ManualTimer>;

struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

pub(super) fn run_pair<A: Future, B: Future>(
    a: A,
    b: B,
    mut on_stall: impl FnMut() -> bool,
    max_polls: usize,
) -> (A::Output, B::Output) {
    let mut a = pin!(a);
    let mut b = pin!(b);
    let fa = Arc::new(WakeFlag(AtomicBool::new(true)));
    let fb = Arc::new(WakeFlag(AtomicBool::new(true)));
    let wa = Waker::from(fa.clone());
    let wb = Waker::from(fb.clone());
    let mut ra = None;
    let mut rb = None;
    for _ in 0..max_polls {
        let mut polled = false;
        if ra.is_none() && fa.0.swap(false, Ordering::SeqCst) {
            polled = true;
            if let Poll::Ready(v) = a.as_mut().poll(&mut Context::from_waker(&wa)) {
                ra = Some(v);
            }
        }
        if rb.is_none() && fb.0.swap(false, Ordering::SeqCst) {
            polled = true;
            if let Poll::Ready(v) = b.as_mut().poll(&mut Context::from_waker(&wb)) {
                rb = Some(v);
            }
        }
        if let (Some(_), Some(_)) = (&ra, &rb) {
            return (ra.take().unwrap(), rb.take().unwrap());
        }
        if !polled && !on_stall() {
            panic!(
                "stalled: no task woken (a done: {}, b done: {})",
                ra.is_some(),
                rb.is_some()
            );
        }
    }
    panic!("no completion within {max_polls} polls");
}

fn pair() -> (DuplexArq, DuplexArq) {
    let (a_ch, b_ch) = tokio::io::duplex(256);
    (new_arq(a_ch), new_arq(b_ch))
}

#[test]
fn duplex_request_response_flush() {
    let (mut a, mut b) = pair();
    let client = async {
        a.write_all(b"ping").await.unwrap();
        a.flush().await.unwrap();
        let mut buf = [0u8; 4];
        a.read_exact(&mut buf).await.unwrap();
        buf
    };
    let server = async {
        let mut buf = [0u8; 4];
        b.read_exact(&mut buf).await.unwrap();
        b.write_all(b"pong").await.unwrap();
        b.flush().await.unwrap();
        buf
    };
    let (resp, req) = run_pair(client, server, || false, 10_000);
    assert_eq!(&req, b"ping");
    assert_eq!(&resp, b"pong");
}

#[test]
fn duplex_repeated_request_response_reuses_flush() {
    let (mut a, mut b) = pair();
    let client = async {
        let mut got = Vec::new();
        for i in 0..3u8 {
            a.write_all(&[b'q', i]).await.unwrap();
            a.flush().await.unwrap();
            let mut buf = [0u8; 2];
            a.read_exact(&mut buf).await.unwrap();
            got.push(buf);
        }
        got
    };
    let server = async {
        let mut got = Vec::new();
        for _ in 0..3u8 {
            let mut buf = [0u8; 2];
            b.read_exact(&mut buf).await.unwrap();
            b.write_all(&[b'r', buf[1]]).await.unwrap();
            b.flush().await.unwrap();
            got.push(buf);
        }
        got
    };
    let (resp, req) = run_pair(client, server, || false, 10_000);
    assert_eq!(req, vec![[b'q', 0], [b'q', 1], [b'q', 2]]);
    assert_eq!(resp, vec![[b'r', 0], [b'r', 1], [b'r', 2]]);
}

struct Lossy {
    inner: DuplexStream,
    drop_writes: Vec<usize>,
    writes: usize,
}

impl Lossy {
    fn new(inner: DuplexStream, drop_writes: &[usize]) -> Self {
        Self {
            inner,
            drop_writes: drop_writes.to_vec(),
            writes: 0,
        }
    }
}

impl AsyncRead for Lossy {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Lossy {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let idx = self.writes;
        if self.drop_writes.contains(&idx) {
            self.writes += 1;
            return Poll::Ready(Ok(buf.len()));
        }
        let res = Pin::new(&mut self.inner).poll_write(cx, buf);
        if res.is_ready() {
            self.writes += 1;
        }
        res
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

async fn request<S: AsyncRead + AsyncWrite + Unpin>(s: &mut S) -> [u8; 4] {
    s.write_all(b"ping").await.unwrap();
    s.flush().await.unwrap();
    let mut buf = [0u8; 4];
    s.read_exact(&mut buf).await.unwrap();
    s.shutdown().await.unwrap();
    buf
}

async fn respond<S: AsyncRead + AsyncWrite + Unpin>(s: &mut S) -> [u8; 4] {
    let mut buf = [0u8; 4];
    s.read_exact(&mut buf).await.unwrap();
    s.write_all(b"pong").await.unwrap();
    s.flush().await.unwrap();
    let mut rest = Vec::new();
    s.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty());
    buf
}

#[test]
fn duplex_lost_frames_recovered_after_timeout() {
    let (a_ch, b_ch) = tokio::io::duplex(256);
    let clock = Clock::default();
    let layer = ArqLayer::<4, Crc16X25, BchAckCodec>::new();
    let mut a: Arq<4, 16, { r::<4>() }, _, _, _, _> =
        layer.build_with_timer(Lossy::new(a_ch, &[0]), clock.timer());
    let mut b: Arq<4, 16, { r::<4>() }, _, _, _, _> =
        layer.build_with_timer(Lossy::new(b_ch, &[0, 1]), clock.timer());
    let (resp, req) = run_pair(
        request(&mut a),
        respond(&mut b),
        || clock.advance_to_next(),
        10_000,
    );
    assert_eq!(&req, b"ping");
    assert_eq!(&resp, b"pong");
    assert!(
        clock.0.borrow().now > Duration::ZERO,
        "recovery must be timer driven"
    );
}

#[test]
fn duplex_lost_frame_recovered_with_std_timer() {
    let (a_ch, b_ch) = tokio::io::duplex(256);
    let layer = ArqLayer::<4, Crc16X25, BchAckCodec>::new()
        .with_retransmit_timeout(Duration::from_millis(5), Duration::from_millis(50));
    let mut a: Arq<4, 16, { r::<4>() }, _, _, _, _> = layer.build(Lossy::new(a_ch, &[0]));
    let mut b: Arq<4, 16, { r::<4>() }, _, _, _, _> = layer.build(b_ch);
    let (resp, req) = run_pair(
        request(&mut a),
        respond(&mut b),
        || {
            std::thread::sleep(Duration::from_millis(1));
            true
        },
        5_000,
    );
    assert_eq!(&req, b"ping");
    assert_eq!(&resp, b"pong");
}
