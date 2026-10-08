//! Both codec backends against each other: same wire bytes, same status
//! mapping for malformed messages.

use alloc::string::String;
use alloc::vec::Vec;
use core::task::Poll;

use micropb::size::{sizeof_int32, sizeof_len_record, sizeof_tag};
use micropb::{
    DecodeError, MessageDecode, MessageEncode, PbDecoder, PbEncoder, PbRead, PbWrite, Presence,
    Tag, WIRE_TYPE_LEN, WIRE_TYPE_VARINT,
};

use crate::codec::{self, Micropb, Prost};
use crate::{Code, Next};

/// `message M { int32 value = 1; string text = 2; }` for micropb.
#[derive(Debug, Default, Clone, PartialEq)]
struct MicropbMsg {
    value: i32,
    text: String,
}

impl MessageEncode for MicropbMsg {
    const MAX_SIZE: Result<usize, &'static str> = Err("unbounded");

    fn encode<W: PbWrite>(&self, encoder: &mut PbEncoder<W>) -> Result<(), W::Error> {
        if self.value != 0 {
            encoder.encode_tag(Tag::from_parts(1, WIRE_TYPE_VARINT))?;
            encoder.encode_int32(self.value)?;
        }
        if !self.text.is_empty() {
            encoder.encode_tag(Tag::from_parts(2, WIRE_TYPE_LEN))?;
            encoder.encode_string(&self.text)?;
        }
        Ok(())
    }

    fn compute_size(&self) -> usize {
        let mut size = 0;
        if self.value != 0 {
            size += sizeof_tag(Tag::from_parts(1, WIRE_TYPE_VARINT)) + sizeof_int32(self.value);
        }
        if !self.text.is_empty() {
            size +=
                sizeof_tag(Tag::from_parts(2, WIRE_TYPE_LEN)) + sizeof_len_record(self.text.len());
        }
        size
    }
}

impl MessageDecode for MicropbMsg {
    fn decode<R: PbRead>(
        &mut self,
        decoder: &mut PbDecoder<R>,
        len: usize,
    ) -> Result<(), DecodeError<R::Error>> {
        let end = decoder.bytes_read() + len;
        while decoder.bytes_read() < end {
            let tag = decoder.decode_tag()?;
            match tag.field_num() {
                1 => self.value = decoder.decode_int32()?,
                2 => decoder.decode_string(&mut self.text, Presence::Implicit)?,
                _ => decoder.skip_wire_value(tag.wire_type())?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstMsg {
    #[prost(int32, tag = "1")]
    value: i32,
    #[prost(string, tag = "2")]
    text: String,
}

fn sample() -> (MicropbMsg, ProstMsg) {
    (
        MicropbMsg {
            value: -7,
            text: "héllo".into(),
        },
        ProstMsg {
            value: -7,
            text: "héllo".into(),
        },
    )
}

#[test]
fn backends_produce_identical_bytes() {
    let (mb, pb) = sample();
    let from_micropb = codec::encode::<Micropb, _>(&mb).unwrap();
    let from_prost = codec::encode::<Prost, _>(&pb).unwrap();
    assert_eq!(from_micropb, from_prost);
    assert!(!from_micropb.is_empty());
}

#[test]
fn each_backend_decodes_the_other_ones_bytes() {
    let (mb, pb) = sample();
    let bytes = codec::encode::<Micropb, _>(&mb).unwrap();
    assert_eq!(
        codec::decode_request::<Prost, ProstMsg>(&bytes).unwrap(),
        pb
    );
    let bytes = codec::encode::<Prost, _>(&pb).unwrap();
    assert_eq!(
        codec::decode_response::<Micropb, MicropbMsg>(&bytes).unwrap(),
        mb
    );
}

#[test]
fn malformed_messages_map_to_the_same_codes() {
    // A length-delimited field that claims more bytes than there are.
    let truncated: &[u8] = &[0x12, 0x05, b'a'];
    let status = codec::decode_request::<Micropb, MicropbMsg>(truncated).unwrap_err();
    assert_eq!(status.code, Code::InvalidArgument);
    let status = codec::decode_request::<Prost, ProstMsg>(truncated).unwrap_err();
    assert_eq!(status.code, Code::InvalidArgument);
    let status = codec::decode_response::<Micropb, MicropbMsg>(truncated).unwrap_err();
    assert_eq!(status.code, Code::Internal);
    let status = codec::decode_response::<Prost, ProstMsg>(truncated).unwrap_err();
    assert_eq!(status.code, Code::Internal);
}

#[test]
fn server_helpers_work_with_either_backend() {
    let (mb, pb) = sample();
    let request = codec::encode::<Micropb, _>(&mb).unwrap();

    // A prost server answering a micropb client.
    let reply = codec::unary::<Prost, ProstMsg, ProstMsg>(&request, |req| {
        assert_eq!(req, pb);
        Ok(req)
    })
    .unwrap();
    assert_eq!(reply, request);

    let status = codec::unary::<Prost, ProstMsg, ProstMsg>(&[0x0a], Ok).unwrap_err();
    assert_eq!(status.code, Code::InvalidArgument);

    let mut seen = Vec::new();
    codec::message::<Micropb, MicropbMsg>(&request, |req| {
        seen.push(req);
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, core::slice::from_ref(&mb));

    match codec::poll_stream::<Prost, ProstMsg>(Poll::Ready(Next::Message(pb.clone()))) {
        Poll::Ready(Next::Message(bytes)) => assert_eq!(bytes, request),
        other => panic!("{other:?}"),
    }
    match codec::poll_single::<Micropb, MicropbMsg>(Poll::Ready(Ok(mb))) {
        Poll::Ready(Next::Message(bytes)) => assert_eq!(bytes, request),
        other => panic!("{other:?}"),
    }
}
