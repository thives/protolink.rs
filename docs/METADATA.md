# gRPC Custom Metadata: Design and Implementation Record

gRPC custom metadata is the set of application headers and trailers that ride along with a call, next
to the protobuf messages: request headers sent by the client, response headers and trailers sent by the
server.

**Status: done** for unary and all streaming shapes, in the sans-IO cores, the async, blocking and tokio
drivers, and the generated code. Interoperability is tested against the `h2` crate.

## The `Metadata` type

`protolink_grpc::Metadata` is an ordered list of entries. A key may appear more than once, and the order
is kept.

```rust
let mut md = Metadata::new();
md.insert("x-request-id", "req-42")?;      // text
md.insert("x-tag", "a")?;
md.insert("x-tag", "b")?;                  // repeats the key
md.insert_bin("x-trace-bin", &[1, 2, 3])?; // bytes; the key must end in `-bin`

md.get("x-tag");                           // Some("a")
md.get_all("x-tag");                       // "a", "b"
md.get_bin("x-trace-bin");                 // Some(&[1, 2, 3])
```

Rules, checked when an entry is inserted (`InvalidMetadata` says which one was broken):

- Keys are lowercase ASCII: `[0-9a-z_.-]`, not empty. Lookups ignore case.
- Text values are printable ASCII (`0x20..=0x7e`).
- A key ending in `-bin` carries bytes and must be inserted with `insert_bin`; any other key must be text.
  Binary values are base64 on the wire. They are sent unpadded, and padded or unpadded input is accepted.
- These names are reserved for HTTP/2 and gRPC and can't be used: pseudo-headers (`:…`), everything
  starting with `grpc-`, `content-type`, `te`, `host`, `content-length` and the hop-by-hop headers
  (`connection`, `keep-alive`, `proxy-connection`, `transfer-encoding`, `upgrade`, `trailer`).
  `user-agent` is **not** reserved: a client sends `protolink` unless the call's metadata sets its own.

Because invalid metadata can't be constructed, nothing that is sent can break the protocol.

## Client

Request metadata goes in `CallOptions::metadata` (`CallOptions::metadata(md)`, or
`options.with_metadata(md)`). It is sent after the headers gRPC defines. `CallOptions` is no longer
`Copy`.

What comes back:

| Call | Response headers | Response trailers |
|---|---|---|
| unary, generated `<method>_with_options` | `Response::headers` | `Response::trailers` |
| unary, failed | – | `Status::metadata` |
| streaming | `call.headers()`, once the server sent them | `call.trailers()`, once the call ended |
| client streaming | `finish_with_metadata()` returns a `Response` | |
| sans-IO `Client` | `response_headers(id)` while active | `take_metadata(id)` after `Next::Done` |

The plain generated methods (`client.command(&request)`) are unchanged and drop the response metadata.
Only `<method>_with_options` of a unary method changed its return type, to `Response<T>`
(`message`, `headers`, `trailers`; `into_message()` drops the metadata). The unary
`unary` / `unary_with` of the drivers and the `UnaryTransport` traits follow.

A **trailers-only** response (a call that failed before any response headers were sent) has a single
header block. All of its metadata is reported as trailers and `headers()` is `None`.

Metadata that breaks the rules in a response (a malformed `-bin` value, a control character) is skipped
rather than failing the call.

The sans-IO client keeps the headers and trailers of a finished streaming call until
`take_metadata(id)` takes them (the drivers do that when the call ends) or `cancel(id)` is called.

## Server

Every `Handler` method now takes `&mut CallContext`. Besides `path`, `id` and `deadline` it has:

- `metadata() -> &Metadata`: the request's custom metadata.
- `initial_metadata_mut() -> Option<&mut Metadata>`: metadata for the response headers. It is `None`
  once the headers have been sent (with the first response message, or when `poll_response` returned
  `Poll::Pending`), because later changes could no longer reach the client.
- `trailing_metadata_mut() -> &mut Metadata`: metadata for the response trailers.

A `Status` returned by the handler adds its own `Status::metadata` to the trailers
(`Status::aborted("…").with_metadata(md)`).

Trailing metadata is sent whatever way the call ends, including `DEADLINE_EXCEEDED`, so what a handler
set before that is not lost. A trailers-only response carries the initial metadata, the trailing metadata
and the status's metadata in its single block, in that order.

A request whose metadata breaks the rules (a malformed `-bin` value, a control character, an invalid
key) is answered with `INVALID_ARGUMENT` and never reaches the handler.

Handlers can be tested without a server: `CallContext::new(path, id, deadline, &request_metadata,
&mut response_metadata)` builds a context, and the `ResponseMetadata` shows what the handler set.

## Wire format

- Metadata is sent as ordinary HTTP/2 headers: request metadata after the standard gRPC request headers,
  response metadata after `content-type` (and the encoding headers), trailing metadata after
  `grpc-status` / `grpc-message`.
- When received, pseudo-headers and the reserved names above are not metadata and are skipped.

## Limits

- The peer's `SETTINGS_MAX_HEADER_LIST_SIZE` is not tracked when sending. Metadata is meant to be small;
  a receiver resets a call whose headers exceed its limit (`Config::max_header_list_size`, 8 KiB by
  default).
- Metadata is buffered per call until the call is done, like other call state.

## Breaking changes

- `Handler` and generated service methods take `&mut CallContext<'_>` (was `&CallContext<'_>`).
  `CallContext` can no longer be built with a struct literal; use `CallContext::new`.
- `CallOptions` is no longer `Copy`, and `Status` has a `metadata` field.
- `UnaryTransport::unary`, `BlockingUnaryTransport::unary`, the drivers' `unary_with` and the unary
  `<method>_with_options` return `Response<…>`; `Client::take_response` of the sans-IO client likewise.
- `StreamingCall` and `BlockingStreamingCall` have the required methods `headers` and `trailers`.
