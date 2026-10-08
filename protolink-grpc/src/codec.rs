//! Message codec abstraction used by generated service code, and typed
//! wrappers for streaming calls.
//!
//! Generated code names a backend explicitly: [`Micropb`] (feature `micropb`)
//! or [`Prost`] (feature `prost`). Both implement [`Encode`] and [`Decode`], so
//! the helpers and streaming wrappers below are shared.

use alloc::vec::Vec;
use core::marker::PhantomData;
use core::task::Poll;

use crate::{BlockingStreamingCall, Code, Metadata, Next, Response, Status, StreamingCall};

/// Encodes messages of type `M` to protobuf bytes.
pub trait Encode<M> {
    /// Encode `msg`.
    fn encode(msg: &M) -> Result<Vec<u8>, Status>;
}

/// Decodes protobuf bytes into messages of type `M`.
pub trait Decode<M> {
    /// Decode `bytes`, mapping failures to a status with `code`.
    fn decode(bytes: &[u8], code: Code) -> Result<M, Status>;
}

#[cfg(any(feature = "micropb", feature = "prost"))]
fn malformed(code: Code) -> Status {
    Status::new(code, "malformed protobuf message")
}

/// The [micropb](https://docs.rs/micropb) backend.
#[cfg(feature = "micropb")]
#[derive(Debug, Clone, Copy, Default)]
pub struct Micropb;

#[cfg(feature = "micropb")]
impl<M: micropb::MessageEncode> Encode<M> for Micropb {
    fn encode(msg: &M) -> Result<Vec<u8>, Status> {
        let mut encoder = micropb::PbEncoder::new(Vec::with_capacity(msg.compute_size()));
        match msg.encode(&mut encoder) {
            Ok(()) => Ok(encoder.into_writer()),
            Err(never) => match never {},
        }
    }
}

#[cfg(feature = "micropb")]
impl<M: micropb::MessageDecode + Default> Decode<M> for Micropb {
    fn decode(bytes: &[u8], code: Code) -> Result<M, Status> {
        let mut msg = M::default();
        let mut decoder = micropb::PbDecoder::new(bytes);
        msg.decode(&mut decoder, bytes.len())
            .map_err(|_| malformed(code))?;
        Ok(msg)
    }
}

/// The [prost](https://docs.rs/prost) backend.
#[cfg(feature = "prost")]
#[derive(Debug, Clone, Copy, Default)]
pub struct Prost;

#[cfg(feature = "prost")]
impl<M: prost::Message> Encode<M> for Prost {
    fn encode(msg: &M) -> Result<Vec<u8>, Status> {
        Ok(msg.encode_to_vec())
    }
}

#[cfg(feature = "prost")]
impl<M: prost::Message + Default> Decode<M> for Prost {
    fn decode(bytes: &[u8], code: Code) -> Result<M, Status> {
        M::decode(bytes).map_err(|_| malformed(code))
    }
}

/// Encode a message to protobuf bytes with codec `K`.
pub fn encode<K: Encode<M>, M>(msg: &M) -> Result<Vec<u8>, Status> {
    K::encode(msg)
}

/// Decode protobuf bytes with codec `K`, mapping failures to `code`.
pub fn decode<K: Decode<M>, M>(bytes: &[u8], code: Code) -> Result<M, Status> {
    K::decode(bytes, code)
}

/// Decode a request message (failures map to `INVALID_ARGUMENT`).
pub fn decode_request<K: Decode<M>, M>(bytes: &[u8]) -> Result<M, Status> {
    decode::<K, M>(bytes, Code::InvalidArgument)
}

/// Decode a response message (failures map to `INTERNAL`).
pub fn decode_response<K: Decode<M>, M>(bytes: &[u8]) -> Result<M, Status> {
    decode::<K, M>(bytes, Code::Internal)
}

/// Decode `request`, run `f`, encode its reply. Used by generated servers.
pub fn unary<K, Req, Resp>(
    request: &[u8],
    f: impl FnOnce(Req) -> Result<Resp, Status>,
) -> Result<Vec<u8>, Status>
where
    K: Decode<Req> + Encode<Resp>,
{
    let reply = f(decode_request::<K, Req>(request)?)?;
    encode::<K, Resp>(&reply)
}

/// Decode one request message of a streaming call and pass it to `f`. Used
/// by generated servers.
pub fn message<K, Req>(
    request: &[u8],
    f: impl FnOnce(Req) -> Result<(), Status>,
) -> Result<(), Status>
where
    K: Decode<Req>,
{
    f(decode_request::<K, Req>(request)?)
}

/// Encode a polled response of a server-streaming or bidirectional call.
/// Used by generated servers.
pub fn poll_stream<K: Encode<Resp>, Resp>(poll: Poll<Next<Resp>>) -> Poll<Next<Vec<u8>>> {
    poll.map(|next| match next {
        Next::Message(msg) => match encode::<K, Resp>(&msg) {
            Ok(bytes) => Next::Message(bytes),
            Err(status) => Next::Done(Err(status)),
        },
        Next::Done(result) => Next::Done(result),
    })
}

/// Encode the polled response of a client-streaming call. Used by generated
/// servers.
pub fn poll_single<K: Encode<Resp>, Resp>(poll: Poll<Result<Resp, Status>>) -> Poll<Next<Vec<u8>>> {
    poll.map(
        |result| match result.and_then(|msg| encode::<K, Resp>(&msg)) {
            Ok(bytes) => Next::Message(bytes),
            Err(status) => Next::Done(Err(status)),
        },
    )
}

fn decode_next<K: Decode<Resp>, Resp>(bytes: Option<Vec<u8>>) -> Result<Option<Resp>, Status> {
    bytes.map(|b| decode_response::<K, Resp>(&b)).transpose()
}

fn single<K: Decode<Resp>, Resp>(
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
    decode_response::<K, Resp>(&bytes)
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
pub struct ServerStreaming<C, Resp, K> {
    call: C,
    _types: PhantomData<fn() -> (Resp, K)>,
}
wrapper_common!(ServerStreaming<Resp, K>);
metadata_accessors!(ServerStreaming<Resp, K>, StreamingCall);

impl<C: StreamingCall, Resp, K: Decode<Resp>> ServerStreaming<C, Resp, K> {
    /// Next response; `Ok(None)` once the stream ended successfully.
    pub async fn message(&mut self) -> Result<Option<Resp>, Status> {
        decode_next::<K, Resp>(self.call.message().await?)
    }
}

/// Requests of a client-streaming call, completed by
/// [`finish`](Self::finish).
#[derive(Debug)]
pub struct ClientStreaming<C, Req, Resp, K> {
    call: C,
    _types: PhantomData<fn(Req, K) -> Resp>,
}
wrapper_common!(ClientStreaming<Req, Resp, K>);
metadata_accessors!(ClientStreaming<Req, Resp, K>, StreamingCall);

impl<C, Req, Resp, K> ClientStreaming<C, Req, Resp, K>
where
    C: StreamingCall,
    K: Encode<Req> + Decode<Resp>,
{
    /// Send one request.
    pub async fn send(&mut self, request: &Req) -> Result<(), Status> {
        self.call.send(&encode::<K, Req>(request)?).await
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
            message: single::<K, Resp>(first, rest)?,
            headers: self.call.headers().unwrap_or_default(),
            trailers: self.call.trailers().unwrap_or_default(),
        })
    }
}

/// A bidirectional streaming call.
#[derive(Debug)]
pub struct BidiStreaming<C, Req, Resp, K> {
    call: C,
    _types: PhantomData<fn(Req, K) -> Resp>,
}
wrapper_common!(BidiStreaming<Req, Resp, K>);
metadata_accessors!(BidiStreaming<Req, Resp, K>, StreamingCall);

impl<C, Req, Resp, K> BidiStreaming<C, Req, Resp, K>
where
    C: StreamingCall,
    K: Encode<Req> + Decode<Resp>,
{
    /// Send one request.
    pub async fn send(&mut self, request: &Req) -> Result<(), Status> {
        self.call.send(&encode::<K, Req>(request)?).await
    }

    /// Half-close: no more requests. Responses keep flowing.
    pub async fn close_send(&mut self) -> Result<(), Status> {
        self.call.close_send().await
    }

    /// Next response; `Ok(None)` once the stream ended successfully.
    pub async fn message(&mut self) -> Result<Option<Resp>, Status> {
        decode_next::<K, Resp>(self.call.message().await?)
    }
}

/// Blocking counterpart of [`ServerStreaming`].
#[derive(Debug)]
pub struct BlockingServerStreaming<C, Resp, K> {
    call: C,
    _types: PhantomData<fn() -> (Resp, K)>,
}
wrapper_common!(BlockingServerStreaming<Resp, K>);
metadata_accessors!(BlockingServerStreaming<Resp, K>, BlockingStreamingCall);

impl<C: BlockingStreamingCall, Resp, K: Decode<Resp>> BlockingServerStreaming<C, Resp, K> {
    /// Next response; `Ok(None)` once the stream ended successfully.
    pub fn message(&mut self) -> Result<Option<Resp>, Status> {
        decode_next::<K, Resp>(self.call.message()?)
    }
}

/// Blocking counterpart of [`ClientStreaming`].
#[derive(Debug)]
pub struct BlockingClientStreaming<C, Req, Resp, K> {
    call: C,
    _types: PhantomData<fn(Req, K) -> Resp>,
}
wrapper_common!(BlockingClientStreaming<Req, Resp, K>);
metadata_accessors!(BlockingClientStreaming<Req, Resp, K>, BlockingStreamingCall);

impl<C, Req, Resp, K> BlockingClientStreaming<C, Req, Resp, K>
where
    C: BlockingStreamingCall,
    K: Encode<Req> + Decode<Resp>,
{
    /// Send one request.
    pub fn send(&mut self, request: &Req) -> Result<(), Status> {
        self.call.send(&encode::<K, Req>(request)?)
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
            message: single::<K, Resp>(first, rest)?,
            headers: self.call.headers().unwrap_or_default(),
            trailers: self.call.trailers().unwrap_or_default(),
        })
    }
}

/// Blocking counterpart of [`BidiStreaming`].
#[derive(Debug)]
pub struct BlockingBidiStreaming<C, Req, Resp, K> {
    call: C,
    _types: PhantomData<fn(Req, K) -> Resp>,
}
wrapper_common!(BlockingBidiStreaming<Req, Resp, K>);
metadata_accessors!(BlockingBidiStreaming<Req, Resp, K>, BlockingStreamingCall);

impl<C, Req, Resp, K> BlockingBidiStreaming<C, Req, Resp, K>
where
    C: BlockingStreamingCall,
    K: Encode<Req> + Decode<Resp>,
{
    /// Send one request.
    pub fn send(&mut self, request: &Req) -> Result<(), Status> {
        self.call.send(&encode::<K, Req>(request)?)
    }

    /// Half-close: no more requests. Responses keep flowing.
    pub fn close_send(&mut self) -> Result<(), Status> {
        self.call.close_send()
    }

    /// Next response; `Ok(None)` once the stream ended successfully.
    pub fn message(&mut self) -> Result<Option<Resp>, Status> {
        decode_next::<K, Resp>(self.call.message()?)
    }
}
