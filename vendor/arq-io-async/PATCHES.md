# Local ARQ patch

Baseline: crates.io `arq-io-async` 0.0.2, repository
<https://github.com/thives/arq-io-async.rs>. Source, upstream tests, manifest,
README, and licenses are retained from the published crate.

This patch adds an opt-in, frame-oriented embedded-I/O receive path:

- `MAX_FRAME` is public so framing transports can use the same fixed bound.
- `embedded_io::ReadFrame` returns one complete nonempty frame in a fixed-size
  buffer (zero means EOF); implementations must skip empty frames and preserve
  state across cancelled pending futures.
- `embedded_io::EiaFramed` selects `FrameIo::FRAMED_RECV`. Sending and flushing
  retain the existing lower adapter behavior.
- The framed receive branch bypasses byte-stream length inference and buffering.
  `Frame::from_complete_bytes` tries BCH/CRC ACK decoding only for an exact
  codeword-length frame; otherwise DAT/FIN decoding validates exact length,
  type, maximum payload, and CRC. Invalid frames are wholly discarded. After
  32 invalid frames in a poll it yields and schedules a wake to allow timer
  and send progress.
- `src/tests/framed.rs` covers exact validation, corrupt lengths, CRC/type
  failures, short/long frames, BCH correction and reconstructed-CRC failure,
  authoritative boundary handling, and bounded invalid-frame work.

`EiaLower` and Tokio's byte-stream adapters keep their upstream behavior and
limitations. The wire format, ACK codec, retransmission policy, and dependencies
are unchanged. Protolink's COBS-backed reliable constructor opts into the new
path; its standalone generic COBS byte-stream reader is not ARQ-specific.

When updating the dependency, preserve these changes or replace the local copy
with an upstream version offering equivalent authoritative-frame validation.

The root workspace excludes this local dependency; run its tests with an explicit
`--manifest-path vendor/arq-io-async/Cargo.toml`. Before publishing protolink to
crates.io, upstream these APIs or publish a renamed/versioned patched dependency
and update the manifest. The published upstream 0.0.2 cannot substitute for this
copy: Cargo removes dependency paths when preparing a registry package.
