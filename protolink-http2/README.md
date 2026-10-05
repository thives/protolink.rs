> [!WARNING]
> This crate is in early development. The API is not yet stable and may change.

> [!CAUTION]
> This crate is not yet production-ready. It has not been widely tested and may contain bugs.

## Protocol limits

- Decoded HPACK field sections are bounded incrementally, including indexed
  references and Huffman literals. Exceeding `Config::max_header_list_size`
  terminates the connection with `ENHANCE_YOUR_CALM`; table-size updates above
  the negotiated default of 4,096 bytes are `COMPRESSION_ERROR`.
- The encoder permanently uses a zero-sized dynamic table and synchronizes that
  policy with a size update at the start of its first field block.
- Local admission obeys the peer's concurrent-stream limit. `Error::StreamLimit`
  is retryable after streams finish or peer settings increase;
  `Error::StreamIdExhausted` requires a new connection. Failed opens consume no ID
  and queue no output.
- Initial headers, informational responses, body data, and terminating trailers
  are validated independently in both directions. Invalid outbound sections
  return `Error::InvalidHeaders` without changing stream state. Invalid inbound
  HTTP fields reset the stream after HPACK decoding, preserving compression
  synchronization for other streams. HTTP(S) request targets require an absolute
  slash path (with optional query), or `*` for OPTIONS; literal fragments are rejected.
- Content-length is checked against accumulated unpadded DATA bytes in each
  direction, including END_STREAM on initial headers, DATA, and trailers.
  Outbound checks include queued, flow-control-stalled data and are atomic on
  rejection. HEAD/304 response lengths describe metadata rather than body bytes;
  these responses reject nonempty DATA. Successful CONNECT responses switch both
  halves to tunnel accounting, ignoring content-length. Informational and 204
  responses reject content-length, and 204 responses have no body.
- Both flow-control modes enforce connection and stream windows, including
  padding and the initial SETTINGS acknowledgement. Automatic mode credits valid
  DATA immediately; manual mode credits delivered body bytes when released.

## Header octet limitation

The bounded HPACK decoder and dynamic table preserve names and values as bytes,
including non-UTF-8 octets. HTTP/API validation occurs only after the entire
block is decoded, so rejected fields do not corrupt another stream's compression
context. Invalid non-UTF-8 names cause a stream-local `PROTOCOL_ERROR`, not a
connection-level compression failure.

The public `HeaderField` still uses UTF-8 `String`s. Legal non-UTF-8 `obs-text`
values therefore cannot be published through this API and are rejected with a
stream-local `PROTOCOL_ERROR` after synchronization. No lossy conversion is
performed. Supporting those values requires a byte-compatible public header API.

## Retaining receive credit after completion

By default, removing a manual-flow stream (completion or reset) returns all its
remaining connection credit. Calling `release_capacity` on the removed stream
then does nothing.

An application retaining completed, unread data can call
`retain_receive_capacity(stream_id)` **before the stream can close**, normally
immediately after opening it. Unreleased body bytes then survive protocol stream
removal and remain visible through `unreleased_recv_bytes(stream_id)`.
`release_capacity(stream_id, n)` continues to release them, clamped to the credit
still held; only connection WINDOW_UPDATEs are emitted after removal. Repeated
release cannot credit the same bytes twice. Cancellation/discard must release
retained bytes explicitly as well. Retained credit does not occupy a concurrent
stream slot.

This is a wire-byte credit primitive, not an application memory budget: upper
layers must separately bound decompressed messages, partial-message credit, and
completed results. The API is a no-op in automatic flow-control mode.
