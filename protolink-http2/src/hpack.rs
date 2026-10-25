//! Bounded, resumable HPACK decoding (RFC 7541).
//!
//! A header block is decoded as its fragments arrive. The state of the
//! representation in progress carries across fragments and CONTINUATION frames,
//! and so does the field under construction. The encoded octets are not stored:
//! only the fields finished so far, which the list budget bounds. Every declared
//! length is checked against that budget before its octets are kept, and list
//! overflow is fatal, so no discard-mode state is needed.

use alloc::collections::VecDeque;
use alloc::string::String;
use alloc::vec::Vec;
use zerodds_hpack::{HeaderField, STATIC_TABLE};

use super::huffman::Bits;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DecodeError {
    Compression,
    Limit,
}

/// HPACK strings are octets, not UTF-8. Keep them raw until the entire field
/// section has updated the compression context, even for invalid HTTP fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawHeader {
    pub(crate) name: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl RawHeader {
    pub(crate) fn into_field(self) -> Result<HeaderField, &'static str> {
        Ok(HeaderField {
            name: String::from_utf8(self.name).map_err(|_| "non-UTF-8 field name")?,
            value: String::from_utf8(self.value)
                .map_err(|_| "non-UTF-8 value unsupported by HeaderField API")?,
        })
    }
}

/// The table size the peer may use: our SETTINGS_HEADER_TABLE_SIZE, which is
/// never changed from the default.
const NEGOTIATED_MAX: usize = 4096;

/// Octets a field is charged against the list budget before its contents.
const FIELD_OVERHEAD: usize = 32;

/// The representation an integer belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Repr {
    Indexed,
    Literal { incremental: bool },
    SizeUpdate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Name,
    Value,
}

/// What a prefixed integer that is still arriving will mean once complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum What {
    Repr(Repr),
    Length { role: Role, huffman: bool },
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    /// Between representations: the next octet starts one.
    Start,
    /// The first octet of a string's length prefix, which follows a name or
    /// representation.
    StringStart { role: Role },
    /// Continuation octets of a prefixed integer, with its value so far.
    Integer { what: What, value: u64, shift: u32 },
    /// String octets still to come. `huffman` holds the decoding state of a
    /// Huffman-coded string and is `None` for a raw one.
    Text {
        role: Role,
        huffman: Option<Bits>,
        remaining: usize,
    },
}

/// The field under construction.
#[derive(Debug, Default)]
struct Field {
    name: Vec<u8>,
    value: Vec<u8>,
    incremental: bool,
}

/// The header block being decoded.
#[derive(Debug)]
struct Block {
    /// Octets left under the header list limit.
    budget: usize,
    /// A field representation has been seen, so table size updates are now
    /// illegal.
    seen_field: bool,
    state: State,
    field: Field,
    fields: Vec<RawHeader>,
}

pub(crate) struct Decoder {
    dynamic: VecDeque<RawHeader>,
    size: usize,
    max: usize,
    block: Option<Block>,
    #[cfg(test)]
    pub(crate) materialized: usize,
}

impl Decoder {
    pub(crate) fn new() -> Self {
        Self {
            dynamic: VecDeque::new(),
            size: 0,
            max: NEGOTIATED_MAX,
            block: None,
            #[cfg(test)]
            materialized: 0,
        }
    }

    /// Start a header block whose decoded list may take at most `limit` octets.
    /// Replaces any block not yet finished.
    pub(crate) fn begin(&mut self, limit: usize) {
        #[cfg(test)]
        {
            self.materialized = 0;
        }
        self.block = Some(Block {
            budget: limit,
            seen_field: false,
            state: State::Start,
            field: Field::default(),
            fields: Vec::new(),
        });
    }

    /// Decode the next fragment of the current block. Fragments may end in the
    /// middle of a representation.
    pub(crate) fn push(&mut self, input: &[u8]) -> Result<(), DecodeError> {
        let mut block = self.block.take().ok_or(DecodeError::Compression)?;
        let result = self.feed(&mut block, input);
        self.block = Some(block);
        result
    }

    /// Decode one complete block in a single fragment.
    #[cfg(test)]
    pub(crate) fn decode(
        &mut self,
        block: &[u8],
        limit: usize,
    ) -> Result<Vec<RawHeader>, DecodeError> {
        self.begin(limit);
        self.push(block)?;
        self.finish()
    }

    /// End the current block and return its fields. Fails if the block ends in
    /// the middle of a representation.
    pub(crate) fn finish(&mut self) -> Result<Vec<RawHeader>, DecodeError> {
        let block = self.block.take().ok_or(DecodeError::Compression)?;
        if block.state != State::Start {
            return Err(DecodeError::Compression);
        }
        Ok(block.fields)
    }

    fn lookup(&self, index: usize) -> Result<(&[u8], &[u8]), DecodeError> {
        if index == 0 {
            return Err(DecodeError::Compression);
        }
        if let Some(h) = STATIC_TABLE.get(index - 1) {
            return Ok((h.name.as_bytes(), h.value.as_bytes()));
        }
        let h = self
            .dynamic
            .get(index - STATIC_TABLE.len() - 1)
            .ok_or(DecodeError::Compression)?;
        Ok((&h.name, &h.value))
    }

    fn evict(&mut self) {
        while self.size > self.max {
            let h = self.dynamic.pop_back().unwrap();
            self.size -= h.name.len() + h.value.len() + 32;
        }
    }

    fn feed(&mut self, block: &mut Block, mut input: &[u8]) -> Result<(), DecodeError> {
        while !input.is_empty() {
            match block.state {
                State::Start => {
                    let first = input[0];
                    input = &input[1..];
                    let (repr, bits) = if first & 0xe0 == 0x20 {
                        (Repr::SizeUpdate, 5)
                    } else if first & 0x80 != 0 {
                        (Repr::Indexed, 7)
                    } else if first & 0xc0 == 0x40 {
                        (Repr::Literal { incremental: true }, 6)
                    } else {
                        (Repr::Literal { incremental: false }, 4)
                    };
                    if repr != Repr::SizeUpdate {
                        // Charged before the index is read, as the fields are.
                        block.budget = block
                            .budget
                            .checked_sub(FIELD_OVERHEAD)
                            .ok_or(DecodeError::Limit)?;
                        block.seen_field = true;
                    }
                    let mask = (1u8 << bits) - 1;
                    let value = u64::from(first & mask);
                    if value < u64::from(mask) {
                        self.integer_done(block, What::Repr(repr), value)?;
                    } else {
                        block.state = State::Integer {
                            what: What::Repr(repr),
                            value: u64::from(mask),
                            shift: 0,
                        };
                    }
                }
                State::StringStart { role } => {
                    let first = input[0];
                    input = &input[1..];
                    let huffman = first & 0x80 != 0;
                    let value = u64::from(first & 0x7f);
                    if value < 127 {
                        self.length_done(block, role, huffman, value)?;
                    } else {
                        block.state = State::Integer {
                            what: What::Length { role, huffman },
                            value: 127,
                            shift: 0,
                        };
                    }
                }
                State::Integer { what, value, shift } => {
                    let byte = input[0];
                    input = &input[1..];
                    // RFC 7541 §5.1 leaves integers unbounded; a value that needs
                    // more than eight continuation octets cannot be used.
                    if shift >= 56 {
                        return Err(DecodeError::Compression);
                    }
                    let value = value
                        .checked_add(u64::from(byte & 0x7f) << shift)
                        .ok_or(DecodeError::Compression)?;
                    if byte & 0x80 == 0 {
                        self.integer_done(block, what, value)?;
                    } else {
                        block.state = State::Integer {
                            what,
                            value,
                            shift: shift + 7,
                        };
                    }
                }
                State::Text {
                    role,
                    huffman,
                    remaining,
                } => {
                    let n = remaining.min(input.len());
                    let (chunk, rest) = input.split_at(n);
                    input = rest;
                    let mut huffman = huffman;
                    let out = match role {
                        Role::Name => &mut block.field.name,
                        Role::Value => &mut block.field.value,
                    };
                    match huffman.as_mut() {
                        Some(bits) => {
                            for byte in chunk {
                                bits.push(*byte, out, &mut block.budget)?;
                            }
                        }
                        None => {
                            // The length was checked against the budget when it arrived.
                            block.budget = block
                                .budget
                                .checked_sub(chunk.len())
                                .ok_or(DecodeError::Limit)?;
                            out.extend_from_slice(chunk);
                        }
                    }
                    let remaining = remaining - n;
                    if remaining == 0 {
                        if let Some(bits) = huffman {
                            bits.finish()?;
                        }
                        self.text_done(block, role);
                    } else {
                        block.state = State::Text {
                            role,
                            huffman,
                            remaining,
                        };
                    }
                }
            }
        }
        Ok(())
    }

    /// A prefixed integer is complete: act on what it is for.
    fn integer_done(
        &mut self,
        block: &mut Block,
        what: What,
        value: u64,
    ) -> Result<(), DecodeError> {
        match what {
            What::Length { role, huffman } => self.length_done(block, role, huffman, value),
            What::Repr(Repr::SizeUpdate) => {
                let size = usize::try_from(value).map_err(|_| DecodeError::Compression)?;
                if block.seen_field || size > NEGOTIATED_MAX {
                    return Err(DecodeError::Compression);
                }
                self.max = size;
                self.evict();
                block.state = State::Start;
                Ok(())
            }
            What::Repr(Repr::Indexed) => {
                let index = usize::try_from(value).map_err(|_| DecodeError::Compression)?;
                // Borrow first: amplification must be rejected before either
                // string is cloned.
                let (name, field_value) = self.lookup(index)?;
                block.budget = block
                    .budget
                    .checked_sub(name.len())
                    .and_then(|n| n.checked_sub(field_value.len()))
                    .ok_or(DecodeError::Limit)?;
                let h = RawHeader {
                    name: name.into(),
                    value: field_value.into(),
                };
                block.state = State::Start;
                self.finish_field(block, h, false);
                Ok(())
            }
            What::Repr(Repr::Literal { incremental }) => {
                block.field = Field {
                    incremental,
                    ..Field::default()
                };
                if value == 0 {
                    block.state = State::StringStart { role: Role::Name };
                    return Ok(());
                }
                let index = usize::try_from(value).map_err(|_| DecodeError::Compression)?;
                let (name, _) = self.lookup(index)?;
                block.budget = block
                    .budget
                    .checked_sub(name.len())
                    .ok_or(DecodeError::Limit)?;
                block.field.name = name.into();
                block.state = State::StringStart { role: Role::Value };
                Ok(())
            }
        }
    }

    /// The length of a string is known. A raw string's octets are checked against
    /// the budget now, before any of them is kept; a Huffman string's are checked
    /// as they decode.
    fn length_done(
        &mut self,
        block: &mut Block,
        role: Role,
        huffman: bool,
        value: u64,
    ) -> Result<(), DecodeError> {
        let len = usize::try_from(value).map_err(|_| DecodeError::Compression)?;
        if !huffman && len > block.budget {
            return Err(DecodeError::Limit);
        }
        let out = match role {
            Role::Name => &mut block.field.name,
            Role::Value => &mut block.field.value,
        };
        if !huffman {
            out.reserve_exact(len);
        }
        block.state = State::Text {
            role,
            huffman: huffman.then(Bits::default),
            remaining: len,
        };
        if len == 0 {
            // An empty string has no octets to wait for.
            if let State::Text {
                huffman: Some(bits),
                ..
            } = block.state
            {
                bits.finish()?;
            }
            self.text_done(block, role);
        }
        Ok(())
    }

    /// A string is complete: a name is followed by the value, a value completes
    /// the field.
    fn text_done(&mut self, block: &mut Block, role: Role) {
        match role {
            Role::Name => block.state = State::StringStart { role: Role::Value },
            Role::Value => {
                let field = core::mem::take(&mut block.field);
                let h = RawHeader {
                    name: field.name,
                    value: field.value,
                };
                block.state = State::Start;
                self.finish_field(block, h, field.incremental);
            }
        }
    }

    /// Keep a finished field, and insert it into the dynamic table if the
    /// representation asked for that.
    fn finish_field(&mut self, block: &mut Block, h: RawHeader, incremental: bool) {
        #[cfg(test)]
        {
            self.materialized += h.name.len() + h.value.len() + 32;
        }
        if incremental {
            let size = h.name.len() + h.value.len() + 32;
            if size > self.max {
                self.dynamic.clear();
                self.size = 0;
            } else {
                self.size += size;
                self.evict();
                self.dynamic.push_front(h.clone());
            }
        }
        block.fields.push(h);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use zerodds_hpack::{Encoder, encode_integer};

    fn decode(
        decoder: &mut Decoder,
        block: &[u8],
        limit: usize,
    ) -> Result<Vec<RawHeader>, DecodeError> {
        decoder.decode(block, limit)
    }

    fn fields(raw: Vec<RawHeader>) -> Vec<HeaderField> {
        raw.into_iter().map(|h| h.into_field().unwrap()).collect()
    }

    #[test]
    fn rejects_index_amplification_before_cloning() {
        let mut encoder = Encoder::new();
        let mut decoder = Decoder::new();
        let h = HeaderField {
            name: "x".into(),
            value: "v".repeat(3967),
        };
        decode(
            &mut decoder,
            &encoder.encode(core::slice::from_ref(&h)),
            8192,
        )
        .unwrap();
        assert_eq!(
            decode(&mut decoder, &vec![0xbe; 16_384], 8192),
            Err(DecodeError::Limit)
        );
        assert_eq!(
            decoder.materialized, 8000,
            "only two 4000-byte fields were cloned"
        );
        assert_eq!(decoder.size, 4000);
    }

    #[test]
    fn table_updates_obey_negotiated_max_and_block_position() {
        let mut decoder = Decoder::new();
        assert_eq!(
            decode(&mut decoder, &encode_integer(4097, 5, 0x20), 8192),
            Err(DecodeError::Compression)
        );
        assert_eq!(decoder.max, 4096);
        decode(&mut decoder, &[0x20], 8192).unwrap();
        decode(&mut decoder, &encode_integer(4096, 5, 0x20), 8192).unwrap();
        assert_eq!(
            decode(&mut decoder, &[0x82, 0x20], 8192),
            Err(DecodeError::Compression)
        );
    }

    #[test]
    fn literal_forms_and_exact_limits_round_trip() {
        for prefix in [0x00, 0x10, 0x40] {
            let mut decoder = Decoder::new();
            let block = [prefix, 1, b'x', 1, b'y'];
            assert_eq!(
                fields(decode(&mut decoder, &block, 34).unwrap()),
                vec![HeaderField {
                    name: "x".into(),
                    value: "y".into()
                }]
            );
            assert_eq!(decode(&mut decoder, &block, 33), Err(DecodeError::Limit));
        }
        for huffman in [false, true] {
            let mut encoder = Encoder::new();
            encoder.use_huffman = huffman;
            let headers = vec![HeaderField {
                name: "x-name".into(),
                value: "value".repeat(100),
            }];
            let limit = 32 + 6 + 500;
            let block = encoder.encode(&headers);
            let mut decoder = Decoder::new();
            assert_eq!(
                fields(decode(&mut decoder, &block, limit).unwrap()),
                headers
            );
            assert_eq!(
                fields(decode(&mut decoder, &encoder.encode(&headers), limit).unwrap()),
                headers
            );
        }
    }

    #[test]
    fn invalid_indices_truncation_and_huge_sizes_do_not_panic() {
        for block in [
            vec![0x80],
            vec![0xff, 0xff, 0xff, 0x7f],
            vec![0x40, 0x7f],
            vec![0x40, 2, b'x'],
            encode_integer(u64::MAX, 5, 0x20),
        ] {
            assert_eq!(
                decode(&mut Decoder::new(), &block, 8192),
                Err(DecodeError::Compression)
            );
        }
    }

    #[test]
    fn literal_and_huffman_allocations_are_bounded() {
        for huffman in [false, true] {
            let mut encoder = Encoder::new();
            encoder.use_huffman = huffman;
            let block = encoder.encode(&[HeaderField {
                name: "x".into(),
                value: "a".repeat(20_000),
            }]);
            let mut decoder = Decoder::new();
            assert_eq!(decode(&mut decoder, &block, 100), Err(DecodeError::Limit));
            assert_eq!(decoder.materialized, 0);
            assert_eq!(decoder.size, 0);
        }
    }
}

#[cfg(test)]
mod equivalence {
    use super::*;
    use zerodds_hpack::{encode_integer, huffman};

    /// The one-shot decoder this module replaced, kept as the reference for the
    /// resumable one: same rules, applied to a complete block at once.
    pub(super) mod reference {
        use super::super::{DecodeError, NEGOTIATED_MAX, RawHeader};
        use alloc::collections::VecDeque;
        use alloc::vec::Vec;
        use zerodds_hpack::{STATIC_TABLE, decode_integer};

        pub(crate) struct Reference {
            pub(super) dynamic: VecDeque<RawHeader>,
            pub(super) size: usize,
            pub(super) max: usize,
        }

        impl Reference {
            pub(crate) fn new() -> Self {
                Self {
                    dynamic: VecDeque::new(),
                    size: 0,
                    max: NEGOTIATED_MAX,
                }
            }

            fn lookup(&self, index: usize) -> Result<(&[u8], &[u8]), DecodeError> {
                if index == 0 {
                    return Err(DecodeError::Compression);
                }
                if let Some(h) = STATIC_TABLE.get(index - 1) {
                    return Ok((h.name.as_bytes(), h.value.as_bytes()));
                }
                let h = self
                    .dynamic
                    .get(index - STATIC_TABLE.len() - 1)
                    .ok_or(DecodeError::Compression)?;
                Ok((&h.name, &h.value))
            }

            fn evict(&mut self) {
                while self.size > self.max {
                    let h = self.dynamic.pop_back().unwrap();
                    self.size -= h.name.len() + h.value.len() + 32;
                }
            }

            pub(crate) fn decode(
                &mut self,
                mut input: &[u8],
                limit: usize,
            ) -> Result<Vec<RawHeader>, DecodeError> {
                let mut out = Vec::new();
                let mut remaining = limit;
                let mut seen_field = false;
                while let Some(&first) = input.first() {
                    if first & 0xe0 == 0x20 {
                        let size = integer(&mut input, 5)?;
                        if seen_field || size > NEGOTIATED_MAX {
                            return Err(DecodeError::Compression);
                        }
                        self.max = size;
                        self.evict();
                        continue;
                    }
                    seen_field = true;
                    remaining = remaining.checked_sub(32).ok_or(DecodeError::Limit)?;
                    let indexed = first & 0x80 != 0;
                    let incremental = first & 0xc0 == 0x40;
                    let index = integer(
                        &mut input,
                        if indexed {
                            7
                        } else if incremental {
                            6
                        } else {
                            4
                        },
                    )?;
                    let h = if indexed {
                        // Borrow first: amplification must be rejected before either string is cloned.
                        let (name, value) = self.lookup(index)?;
                        remaining = remaining
                            .checked_sub(name.len())
                            .and_then(|n| n.checked_sub(value.len()))
                            .ok_or(DecodeError::Limit)?;
                        RawHeader {
                            name: name.into(),
                            value: value.into(),
                        }
                    } else {
                        let name = if index == 0 {
                            string(&mut input, &mut remaining)?
                        } else {
                            let (name, _) = self.lookup(index)?;
                            remaining = remaining
                                .checked_sub(name.len())
                                .ok_or(DecodeError::Limit)?;
                            name.into()
                        };
                        let value = string(&mut input, &mut remaining)?;
                        RawHeader { name, value }
                    };
                    if incremental {
                        let size = h.name.len() + h.value.len() + 32;
                        if size > self.max {
                            self.dynamic.clear();
                            self.size = 0;
                        } else {
                            self.size += size;
                            self.evict();
                            self.dynamic.push_front(h.clone());
                        }
                    }
                    out.push(h);
                }
                Ok(out)
            }
        }

        fn integer(input: &mut &[u8], bits: u8) -> Result<usize, DecodeError> {
            let (n, used) = decode_integer(input, bits).map_err(|_| DecodeError::Compression)?;
            *input = &input[used..];
            usize::try_from(n).map_err(|_| DecodeError::Compression)
        }

        fn string(input: &mut &[u8], remaining: &mut usize) -> Result<Vec<u8>, DecodeError> {
            let huffman = input.first().ok_or(DecodeError::Compression)? & 0x80 != 0;
            let len = integer(input, 7)?;
            let raw = input.get(..len).ok_or(DecodeError::Compression)?;
            *input = &input[len..];
            let bytes = if huffman {
                crate::huffman::decode_bounded(raw, *remaining)?
            } else {
                // Validate the length before allocating, not after decode_string().
                if len > *remaining {
                    return Err(DecodeError::Limit);
                }
                raw.to_vec()
            };
            *remaining -= bytes.len();
            Ok(bytes)
        }
    }

    /// xorshift64: deterministic, so a failing round reproduces.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// A string: raw or Huffman-coded, and sometimes Huffman-flagged garbage.
    fn random_text(rng: &mut Rng, out: &mut Vec<u8>) {
        let len = rng.below(40) as usize;
        let text: Vec<u8> = (0..len)
            .map(|_| {
                if rng.below(8) == 0 {
                    rng.below(256) as u8
                } else {
                    b'a' + rng.below(26) as u8
                }
            })
            .collect();
        match rng.below(10) {
            0..=4 => {
                out.extend(encode_integer(text.len() as u64, 7, 0x00));
                out.extend(text);
            }
            5..=8 => {
                let coded = huffman::encode(&text);
                out.extend(encode_integer(coded.len() as u64, 7, 0x80));
                out.extend(coded);
            }
            _ => {
                out.extend(encode_integer(text.len() as u64, 7, 0x80));
                out.extend(text);
            }
        }
    }

    /// A block of representations, and whether it has a junk octet, which breaks
    /// its structure on purpose.
    fn random_block(rng: &mut Rng) -> (Vec<u8>, bool) {
        let mut out = Vec::new();
        let mut junk = false;
        if rng.below(4) == 0 {
            out.extend(encode_integer(rng.below(4200), 5, 0x20));
        }
        for _ in 0..rng.below(8) {
            match rng.below(13) {
                0..=3 => out.extend(encode_integer(1 + rng.below(70), 7, 0x80)),
                4..=6 => {
                    let index = if rng.below(3) == 0 {
                        0
                    } else {
                        1 + rng.below(70)
                    };
                    out.extend(encode_integer(index, 6, 0x40));
                    if index == 0 {
                        random_text(rng, &mut out);
                    }
                    random_text(rng, &mut out);
                }
                7..=8 => {
                    let index = if rng.below(3) == 0 {
                        0
                    } else {
                        1 + rng.below(70)
                    };
                    out.extend(encode_integer(index, 4, 0x00));
                    if index == 0 {
                        random_text(rng, &mut out);
                    }
                    random_text(rng, &mut out);
                }
                9 => {
                    let index = if rng.below(3) == 0 {
                        0
                    } else {
                        1 + rng.below(70)
                    };
                    out.extend(encode_integer(index, 4, 0x10));
                    if index == 0 {
                        random_text(rng, &mut out);
                    }
                    random_text(rng, &mut out);
                }
                10 => {
                    junk = true;
                    out.push(rng.below(256) as u8);
                }
                _ => out.extend(encode_integer(rng.below(4200), 5, 0x20)),
            }
        }
        (out, junk)
    }

    /// Decode `block` in fragments of random size, as a connection receives it.
    fn streamed(
        decoder: &mut Decoder,
        block: &[u8],
        rng: &mut Rng,
        limit: usize,
    ) -> Result<Vec<RawHeader>, DecodeError> {
        decoder.begin(limit);
        let mut rest = block;
        while !rest.is_empty() {
            let n = (1 + rng.below(9) as usize).min(rest.len());
            let (piece, tail) = rest.split_at(n);
            decoder.push(piece)?;
            rest = tail;
        }
        decoder.finish()
    }

    #[test]
    fn resumable_decoding_matches_the_one_shot_reference() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut reference = reference::Reference::new();
        let mut one_shot = Decoder::new();
        let mut chunked = Decoder::new();
        let (mut decoded, mut limits, mut compression) = (0, 0, 0);
        for round in 0..4_000 {
            let (mut block, junk) = random_block(&mut rng);
            let cut = rng.below(10) == 0;
            if cut && !block.is_empty() {
                let at = rng.below(block.len() as u64) as usize;
                block.truncate(at);
            }
            let limit = 40 + rng.below(800) as usize;

            let expected = reference.decode(&block, limit);
            let got = one_shot.decode(&block, limit);
            let fragments = streamed(&mut chunked, &block, &mut rng, limit);
            assert_eq!(
                fragments, got,
                "round {round}: limit {limit} cut {cut} block {block:?}"
            );
            // A well-formed block must decode identically. A cut or junk block
            // may not be well formed, and then only its outcome is compared: a
            // declared string that runs past the end of the block is refused for
            // its length (Limit) before its octets are seen (Compression), since
            // a streaming decoder cannot tell where the block ends.
            if cut || junk {
                assert_eq!(expected.is_ok(), got.is_ok(), "round {round}: {block:?}");
            } else {
                assert_eq!(got, expected, "round {round}: {block:?}");
            }
            assert_eq!(one_shot.dynamic, reference.dynamic, "round {round}");
            assert_eq!(one_shot.size, reference.size, "round {round}");
            assert_eq!(one_shot.max, reference.max, "round {round}");
            assert_eq!(chunked.dynamic, reference.dynamic, "round {round}");
            assert_eq!(chunked.size, reference.size, "round {round}");
            assert_eq!(chunked.max, reference.max, "round {round}");
            match got {
                Ok(fields) if !fields.is_empty() => decoded += 1,
                Err(DecodeError::Limit) => limits += 1,
                Err(DecodeError::Compression) => compression += 1,
                Ok(_) => {}
            }
        }
        // The generator must reach every outcome, or the comparison proves little.
        assert!(decoded > 300, "decoded {decoded}");
        assert!(limits > 300, "limit errors {limits}");
        assert!(compression > 300, "compression errors {compression}");
    }
}
