use std::collections::VecDeque;

use super::*;
use crate::{AckCodec, MAX_FRAME};

fn dat(sn: u16, payload: &[u8]) -> Vec<u8> {
    let mut bytes = [0; MAX_FRAME];
    let n = DatFrame::new_dat(&crc16(), sn, payload).to_bytes(&mut bytes);
    bytes[..n].to_vec()
}

fn ack(an: u16) -> Vec<u8> {
    BchAckCodec::encode_ack(AckFrame::new(&crc16(), an).unwrap())
        .unwrap()
        .to_vec()
}

fn decode(bytes: &[u8]) -> Result<Frame, FrameError> {
    Frame::from_complete_bytes::<BchAckCodec, 16, _>(&crc16(), bytes)
}

#[test]
fn complete_data_checks_exact_length_type_and_crc() {
    let bytes = dat(0, &[0x55; 125]);
    assert!(matches!(decode(&bytes), Ok(Frame::Dat(_))));
    for len in [253, 61, 127] {
        let mut damaged = bytes.clone();
        damaged[2] = len;
        assert!(decode(&damaged).is_err());
    }
    let mut damaged = bytes.clone();
    damaged[42] ^= 0x80;
    assert!(matches!(decode(&damaged), Err(FrameError::CrcMismatch(..))));
    let mut damaged = bytes.clone();
    damaged[0] = (damaged[0] & !3) | 1;
    assert!(matches!(decode(&damaged), Err(FrameError::TypeMismatch)));
    assert!(decode(&bytes[..3]).is_err());
    let mut damaged = bytes;
    damaged.push(0);
    assert!(decode(&damaged).is_err());
    // Maximum payload and a DAT whose wire length equals the ACK length.
    assert!(matches!(decode(&dat(1, &[0; 251])), Ok(Frame::Dat(_))));
    assert!(matches!(decode(&dat(1, &[0; 11])), Ok(Frame::Dat(_))));
}

#[test]
fn complete_ack_checks_exact_codeword_bch_type_and_crc() {
    let bytes = ack(7);
    assert!(matches!(decode(&bytes), Ok(Frame::Ack(a)) if a.an() == 7));
    let mut correctable = bytes.clone();
    correctable[0] ^= 1;
    assert!(matches!(decode(&correctable), Ok(Frame::Ack(a)) if a.an() == 7));
    assert!(decode(&[0; 16]).is_err());
    assert!(decode(&bytes[..15]).is_err());
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(decode(&extra).is_err(), "a valid ACK prefix is not a frame");
    // A perfectly valid BCH encoding of the wrong CRC must still be rejected.
    let mut wrong_crc = bytes;
    let crc = AckFrame::new(&crc16(), 7).unwrap().crc() ^ 1;
    wrong_crc[8..].copy_from_slice(&crate::bch::encode(crc).to_le_bytes());
    assert!(decode(&wrong_crc).is_err());
}

struct Frames(VecDeque<Vec<u8>>);

impl FrameIo for Frames {
    type Error = Infallible;
    const FRAMED_RECV: bool = true;

    fn poll_recv(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        assert_eq!(buf.len(), MAX_FRAME);
        let Some(frame) = self.0.pop_front() else {
            return Poll::Pending;
        };
        buf[..frame.len()].copy_from_slice(&frame);
        Poll::Ready(Ok(frame.len()))
    }

    fn poll_send(
        &mut self,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<Result<usize, Self::Error>> {
        unreachable!()
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        unreachable!()
    }
}

#[test]
fn invalid_frames_are_not_retained_or_cross_consumed() {
    for len in [253, 61, 127] {
        let mut damaged = dat(0, &[0x55; 125]);
        damaged[2] = len;
        let mut extra_ack = ack(1);
        extra_ack.push(0);
        let frames = VecDeque::from([
            damaged,
            extra_ack,
            dat(1, b"next"),
            dat(0, b"retry"),
            ack(2),
        ]);
        let mut arq = new_arq(Frames(frames));
        let mut cx = noop_cx();
        assert!(
            matches!(arq.poll_recv_frame(&mut cx), Poll::Ready(Ok(Frame::Dat(d)))
            if d.sn() == 1 && d.payload() == b"next")
        );
        assert_eq!(arq.rx_len, 0);
        assert_eq!(arq.channel.0.len(), 2);
        assert!(
            matches!(arq.poll_recv_frame(&mut cx), Poll::Ready(Ok(Frame::Dat(d)))
            if d.sn() == 0 && d.payload() == b"retry")
        );
        assert!(
            matches!(arq.poll_recv_frame(&mut cx), Poll::Ready(Ok(Frame::Ack(a)))
            if a.an() == 2)
        );
        assert!(arq.poll_recv_frame(&mut cx).is_pending());
        assert_eq!(arq.rx_len, 0);
    }
}

#[test]
fn corrupt_frame_flood_yields_and_resumes_without_buffering() {
    let mut frames = VecDeque::from(vec![vec![0xff]; 40]);
    frames.push_back(dat(0, b"valid"));
    let mut arq = new_arq(Frames(frames));
    let mut cx = noop_cx();
    assert!(arq.poll_recv_frame(&mut cx).is_pending());
    assert_eq!(arq.channel.0.len(), 9);
    assert_eq!(arq.rx_len, 0);
    assert!(
        matches!(arq.poll_recv_frame(&mut cx), Poll::Ready(Ok(Frame::Dat(d)))
        if d.payload() == b"valid")
    );
}
