//! Transport-layer tests for the COBS framing and the ARQ link stack.
#![cfg(feature = "tokio")]

use std::time::Duration;

use embedded_io_async::{Read, Write};
use protolink::link::{CobsFramed, reliable};
use protolink::tokio::FromTokio;

async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("timed out")
}

#[tokio::test]
async fn cobs_frames_round_trip() {
    let (a, b) = tokio::io::duplex(64);
    let mut tx = CobsFramed::new(FromTokio::new(a));
    let mut rx = CobsFramed::new(FromTokio::new(b));
    with_timeout(async {
        let writer = async {
            tx.write_all(&[1, 0, 2, 0, 0, 3]).await.unwrap();
            tx.write_all(&[0; 300]).await.unwrap();
            tx.flush().await.unwrap();
        };
        let reader = async {
            let mut got = Vec::new();
            let mut buf = [0u8; 100];
            while got.len() < 306 {
                let n = rx.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
            }
            got
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(&got[..6], &[1, 0, 2, 0, 0, 3]);
        assert!(got[6..].iter().all(|&b| b == 0));
    })
    .await;
}

#[tokio::test]
async fn arq_raw_round_trip() {
    let (a, b) = tokio::io::duplex(4096);
    let layer = arq_io_async::ArqLayer::<8, _, _>::new();
    let mut x = layer.build_with_timer::<16, { arq_io_async::r::<8>() }, _, _>(
        arq_io_async::embedded_io::EiaLower(FromTokio::new(a)),
        arq_io_async::StdTimer::new(),
    );
    let mut y = layer.build_with_timer::<16, { arq_io_async::r::<8>() }, _, _>(
        arq_io_async::embedded_io::EiaLower(FromTokio::new(b)),
        arq_io_async::StdTimer::new(),
    );
    with_timeout(async {
        let writer = async {
            x.write_all(b"hello").await.unwrap();
            x.flush().await.unwrap();
        };
        let reader = async {
            let mut buf = [0u8; 5];
            y.read_exact(&mut buf).await.unwrap();
            buf
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(&got, b"hello");
    })
    .await;
}

#[tokio::test]
async fn reliable_link_round_trip() {
    let (a, b) = tokio::io::duplex(4096);
    let mut x = reliable(FromTokio::new(a));
    let mut y = reliable(FromTokio::new(b));
    with_timeout(async {
        let writer = async {
            x.write_all(b"hello over cobs + arq").await.unwrap();
            x.flush().await.unwrap();
        };
        let reader = async {
            let mut buf = [0u8; 21];
            y.read_exact(&mut buf).await.unwrap();
            buf
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(&got, b"hello over cobs + arq");
    })
    .await;
}

#[tokio::test]
async fn reliable_with_timer_round_trip() {
    use protolink::link::{StdTimer, reliable_with_timer};

    let (a, b) = tokio::io::duplex(4096);
    let mut x = reliable_with_timer(FromTokio::new(a), StdTimer::new());
    let mut y = reliable_with_timer(FromTokio::new(b), StdTimer::new());
    with_timeout(async {
        let writer = async {
            x.write_all(b"explicit timer").await.unwrap();
            x.flush().await.unwrap();
        };
        let reader = async {
            let mut buf = [0u8; 14];
            y.read_exact(&mut buf).await.unwrap();
            buf
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(&got, b"explicit timer");
    })
    .await;
}
