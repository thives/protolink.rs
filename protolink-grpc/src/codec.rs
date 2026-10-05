//! micropb codec helpers used by generated service code, and typed wrappers
//! for streaming calls.

use alloc::vec::Vec;
use core::marker::PhantomData;
use core::task::Poll;

use micropb::{MessageDecode, MessageEncode, PbDecoder, PbEncoder};

use crate::{BlockingStreamingCall, Code, Metadata, Next, Response, Status, StreamingCall};

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

/// Decode one request message of a streaming call and pass it to `f`. Used
/// by generated servers.
pub fn message<Req>(request: &[u8], f: impl FnOnce(Req) -> Result<(), Status>) -> Result<(), Status>
where
    Req: MessageDecode + Default,
{
    f(decode_request(request)?)
}

/// Encode a polled response of a server-streaming or bidirectional call.
/// Used by generated servers.
pub fn poll_stream<Resp: MessageEncode>(poll: Poll<Next<Resp>>) -> Poll<Next<Vec<u8>>> {
    poll.map(|next| match next {
        Next::Message(msg) => match encode(&msg) {
            Ok(bytes) => Next::Message(bytes),
            Err(status) => Next::Done(Err(status)),
        },
        Next::Done(result) => Next::Done(result),
    })
}

/// Encode the polled response of a client-streaming call. Used by generated
/// servers.
pub fn poll_single<Resp: MessageEncode>(poll: Poll<Result<Resp, Status>>) -> Poll<Next<Vec<u8>>> {
    poll.map(|result| match result.and_then(|msg| encode(&msg)) {
        Ok(bytes) => Next::Message(bytes),
        Err(status) => Next::Done(Err(status)),
    })
}

fn decode_next<Resp: MessageDecode + Default>(
    bytes: Option<Vec<u8>>,
) -> Result<Option<Resp>, Status> {
    bytes.map(|b| decode_response(&b)).transpose()
}

fn single<Resp: MessageDecode + Default>(
    first: Option<Vec<u8>>,
    rest: Option<Vec<u8>>,
) -> Result<Resp, Status> {
    let Some(bytes) = first else {
        return Err(Status::internal("missing response message"));
    };
    if rest.is_some() {
        return Err(Status::internal(
            "more than one response message for client streaming call",
        ));
    }
    decode_response(&bytes)
}

macro_rules! wrapper_common {
    ($name:ident <$($p:ident),+>) => {
        impl<C, $($p),+> $name<C, $($p),+> {
            /// Wrap an untyped call.
            pub fn new(call: C) -> Self {
                Self {
                    call,
                    _types: PhantomData,
                }
            }

            /// The untyped call.
            pub fn call_mut(&mut self) -> &mut C {
                &mut self.call
            }

            /// Unwrap the untyped call.
            pub fn into_inner(self) -> C {
                self.call
            }
        }
    };
}

macro_rules! metadata_accessors {
    ($name:ident <$($p:ident),+>, $bound:ident) => {
        impl<C: $bound, $($p),+> $name<C, $($p),+> {
            /// Metadata of the response headers, once the server has sent
            /// them. `None` before that, and for a trailers-only response.
            pub fn headers(&self) -> Option<Metadata> {
                self.call.headers()
            }

            /// Metadata of the response trailers. `None` until the call has
            /// completed.
            pub fn trailers(&self) -> Option<Metadata> {
                self.call.trailers()
            }
        }
    };
}

/// Responses of a server-streaming call.
#[derive(Debug)]
pub struct ServerStreaming<C, Resp> {
    call: C,
    _types: PhantomData<fn() -> Resp>,
}
wrapper_common!(ServerStreaming<Resp>);
metadata_accessors!(ServerStreaming<Resp>, StreamingCall);

impl<C: StreamingCall, Resp: MessageDecode + Default> ServerStreaming<C, Resp> {
    /// Next response; `Ok(None)` once the stream ended successfully.
    pub async fn message(&mut self) -> Result<Option<Resp>, Status> {
        decode_next(self.call.message().await?)
    }
}

/// Requests of a client-streaming call, completed by
/// [`finish`](Self::finish).
#[derive(Debug)]
pub struct ClientStreaming<C, Req, Resp> {
    call: C,
    _types: PhantomData<fn(Req) -> Resp>,
}
wrapper_common!(ClientStreaming<Req, Resp>);
metadata_accessors!(ClientStreaming<Req, Resp>, StreamingCall);

impl<C, Req, Resp> ClientStreaming<C, Req, Resp>
where
    C: StreamingCall,
    Req: MessageEncode,
    Resp: MessageDecode + Default,
{
    /// Send one request.
    pub async fn send(&mut self, request: &Req) -> Result<(), Status> {
        self.call.send(&encode(request)?).await
    }

    /// Half-close and wait for the single response and the final status.
    pub async fn finish(self) -> Result<Resp, Status> {
        self.finish_with_metadata()
            .await
            .map(Response::into_message)
    }

    /// [`finish`](Self::finish), keeping the response's metadata. The trailers
    /// of a failed call are in [`Status::metadata`].
    pub async fn finish_with_metadata(mut self) -> Result<Response<Resp>, Status> {
        self.call.close_send().await?;
        let first = self.call.message().await?;
        let rest = match first {
            Some(_) => self.call.message().await?,
            None => None,
        };
        Ok(Response {
            message: single(first, rest)?,
            headers: self.call.headers().unwrap_or_default(),
            trailers: self.call.trailers().unwrap_or_default(),
        })
    }
}

/// A bidirectional streaming call.
#[derive(Debug)]
pub struct BidiStreaming<C, Req, Resp> {
    call: C,
    _types: PhantomData<fn(Req) -> Resp>,
}
wrapper_common!(BidiStreaming<Req, Resp>);
metadata_accessors!(BidiStreaming<Req, Resp>, StreamingCall);

impl<C, Req, Resp> BidiStreaming<C, Req, Resp>
where
    C: StreamingCall,
    Req: MessageEncode,
    Resp: MessageDecode + Default,
{
    /// Send one request.
    pub async fn send(&mut self, request: &Req) -> Result<(), Status> {
        self.call.send(&encode(request)?).await
    }

    /// Half-close: no more requests. Responses keep flowing.
    pub async fn close_send(&mut self) -> Result<(), Status> {
        self.call.close_send().await
    }

    /// Next response; `Ok(None)` once the stream ended successfully.
    pub async fn message(&mut self) -> Result<Option<Resp>, Status> {
        decode_next(self.call.message().await?)
    }
}

/// Blocking counterpart of [`ServerStreaming`].
#[derive(Debug)]
pub struct BlockingServerStreaming<C, Resp> {
    call: C,
    _types: PhantomData<fn() -> Resp>,
}
wrapper_common!(BlockingServerStreaming<Resp>);
metadata_accessors!(BlockingServerStreaming<Resp>, BlockingStreamingCall);

impl<C: BlockingStreamingCall, Resp: MessageDecode + Default> BlockingServerStreaming<C, Resp> {
    /// Next response; `Ok(None)` once the stream ended successfully.
    pub fn message(&mut self) -> Result<Option<Resp>, Status> {
        decode_next(self.call.message()?)
    }
}

/// Blocking counterpart of [`ClientStreaming`].
#[derive(Debug)]
pub struct BlockingClientStreaming<C, Req, Resp> {
    call: C,
    _types: PhantomData<fn(Req) -> Resp>,
}
wrapper_common!(BlockingClientStreaming<Req, Resp>);
metadata_accessors!(BlockingClientStreaming<Req, Resp>, BlockingStreamingCall);

impl<C, Req, Resp> BlockingClientStreaming<C, Req, Resp>
where
    C: BlockingStreamingCall,
    Req: MessageEncode,
    Resp: MessageDecode + Default,
{
    /// Send one request.
    pub fn send(&mut self, request: &Req) -> Result<(), Status> {
        self.call.send(&encode(request)?)
    }

    /// Half-close and wait for the single response and the final status.
    pub fn finish(self) -> Result<Resp, Status> {
        self.finish_with_metadata().map(Response::into_message)
    }

    /// [`finish`](Self::finish), keeping the response's metadata. The trailers
    /// of a failed call are in [`Status::metadata`].
    pub fn finish_with_metadata(mut self) -> Result<Response<Resp>, Status> {
        self.call.close_send()?;
        let first = self.call.message()?;
        let rest = match first {
            Some(_) => self.call.message()?,
            None => None,
        };
        Ok(Response {
            message: single(first, rest)?,
            headers: self.call.headers().unwrap_or_default(),
            trailers: self.call.trailers().unwrap_or_default(),
        })
    }
}

/// Blocking counterpart of [`BidiStreaming`].
#[derive(Debug)]
pub struct BlockingBidiStreaming<C, Req, Resp> {
    call: C,
    _types: PhantomData<fn(Req) -> Resp>,
}
wrapper_common!(BlockingBidiStreaming<Req, Resp>);
metadata_accessors!(BlockingBidiStreaming<Req, Resp>, BlockingStreamingCall);

impl<C, Req, Resp> BlockingBidiStreaming<C, Req, Resp>
where
    C: BlockingStreamingCall,
    Req: MessageEncode,
    Resp: MessageDecode + Default,
{
    /// Send one request.
    pub fn send(&mut self, request: &Req) -> Result<(), Status> {
        self.call.send(&encode(request)?)
    }

    /// Half-close: no more requests. Responses keep flowing.
    pub fn close_send(&mut self) -> Result<(), Status> {
        self.call.close_send()
    }

    /// Next response; `Ok(None)` once the stream ended successfully.
    pub fn message(&mut self) -> Result<Option<Resp>, Status> {
        decode_next(self.call.message()?)
    }
}
