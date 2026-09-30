//! micropb codec helpers used by generated service code.

use alloc::vec::Vec;

use micropb::{MessageDecode, MessageEncode, PbDecoder, PbEncoder};

use crate::{Code, Status};

/// Encode a micropb message to protobuf bytes.
pub fn encode<M: MessageEncode>(msg: &M) -> Result<Vec<u8>, Status> {
    let mut encoder = PbEncoder::new(Vec::with_capacity(msg.compute_size()));
    match msg.encode(&mut encoder) {
        Ok(()) => Ok(encoder.into_writer()),
        Err(never) => match never {},
    }
}

/// Decode protobuf bytes into a micropb message, mapping failures to `code`.
pub fn decode<M: MessageDecode + Default>(bytes: &[u8], code: Code) -> Result<M, Status> {
    let mut msg = M::default();
    let mut decoder = PbDecoder::new(bytes);
    msg.decode(&mut decoder, bytes.len())
        .map_err(|_| Status::new(code, "malformed protobuf message"))?;
    Ok(msg)
}

/// Decode a request message (failures map to `INVALID_ARGUMENT`).
pub fn decode_request<M: MessageDecode + Default>(bytes: &[u8]) -> Result<M, Status> {
    decode(bytes, Code::InvalidArgument)
}

/// Decode a response message (failures map to `INTERNAL`).
pub fn decode_response<M: MessageDecode + Default>(bytes: &[u8]) -> Result<M, Status> {
    decode(bytes, Code::Internal)
}

/// Decode `request`, run `f`, encode its reply. Used by generated servers.
pub fn unary<Req, Resp>(
    request: &[u8],
    f: impl FnOnce(Req) -> Result<Resp, Status>,
) -> Result<Vec<u8>, Status>
where
    Req: MessageDecode + Default,
    Resp: MessageEncode,
{
    let reply = f(decode_request(request)?)?;
    encode(&reply)
}
