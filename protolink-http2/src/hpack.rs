//! Bounded HPACK decoding. List overflow is fatal, so no discard-mode state is needed.

use alloc::collections::VecDeque;
use alloc::string::String;
use alloc::vec::Vec;
use zerodds_hpack::{HeaderField, STATIC_TABLE, decode_integer};

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

pub(crate) struct Decoder {
    dynamic: VecDeque<RawHeader>,
    size: usize,
    max: usize,
    negotiated_max: usize,
    #[cfg(test)]
    pub(crate) materialized: usize,
}

impl Decoder {
    pub(crate) fn new() -> Self {
        Self {
            dynamic: VecDeque::new(),
            size: 0,
            max: 4096,
            negotiated_max: 4096,
            #[cfg(test)]
            materialized: 0,
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
        #[cfg(test)]
        {
            self.materialized = 0;
        }
        while let Some(&first) = input.first() {
            if first & 0xe0 == 0x20 {
                let size = integer(&mut input, 5)?;
                if seen_field || size > self.negotiated_max {
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
        super::huffman::decode_bounded(raw, *remaining)?
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use zerodds_hpack::{Encoder, encode_integer};

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
        decoder
            .decode(&encoder.encode(core::slice::from_ref(&h)), 8192)
            .unwrap();
        assert_eq!(
            decoder.decode(&vec![0xbe; 16_384], 8192),
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
            decoder.decode(&encode_integer(4097, 5, 0x20), 8192),
            Err(DecodeError::Compression)
        );
        assert_eq!(decoder.max, 4096);
        decoder.decode(&[0x20], 8192).unwrap();
        decoder
            .decode(&encode_integer(4096, 5, 0x20), 8192)
            .unwrap();
        assert_eq!(
            decoder.decode(&[0x82, 0x20], 8192),
            Err(DecodeError::Compression)
        );
    }

    #[test]
    fn literal_forms_and_exact_limits_round_trip() {
        for prefix in [0x00, 0x10, 0x40] {
            let mut decoder = Decoder::new();
            let block = [prefix, 1, b'x', 1, b'y'];
            assert_eq!(
                fields(decoder.decode(&block, 34).unwrap()),
                vec![HeaderField {
                    name: "x".into(),
                    value: "y".into()
                }]
            );
            assert_eq!(decoder.decode(&block, 33), Err(DecodeError::Limit));
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
            assert_eq!(fields(decoder.decode(&block, limit).unwrap()), headers);
            assert_eq!(
                fields(decoder.decode(&encoder.encode(&headers), limit).unwrap()),
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
                Decoder::new().decode(&block, 8192),
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
            assert_eq!(decoder.decode(&block, 100), Err(DecodeError::Limit));
            assert_eq!(decoder.materialized, 0);
            assert_eq!(decoder.size, 0);
        }
    }
}
