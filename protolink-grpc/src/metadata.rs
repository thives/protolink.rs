//! Custom gRPC metadata: the application headers and trailers of a call.
//!
//! [`Metadata`] is an ordered list of entries that may repeat a key. Keys are
//! lowercase ASCII (`[0-9a-z_.-]`). A value is either ASCII text (printable
//! characters, `0x20..=0x7e`, without leading or trailing spaces, which HTTP/2
//! field values can't carry; inserting such a value is an error, not trimmed) or, for keys ending in `-bin`, arbitrary bytes
//! that are base64-encoded on the wire.
//!
//! Names that belong to HTTP/2 or gRPC themselves can't be used: pseudo-headers
//! (`:`), everything starting with `grpc-`, and `content-type`, `te`, `host`,
//! `content-length` and the hop-by-hop headers. `user-agent` is not reserved: a
//! client sends `protolink` unless the call's metadata sets its own.
//!
//! Received metadata is limited to 128 expanded entries and 8192 owned bytes
//! per header/trailer block, independently of the HTTP/2 header-list limit.
//! Every copied key and decoded binary or ASCII value counts toward the byte
//! limit; entry storage overhead is separately bounded by the entry limit.
//! These checks precede key/value allocation, including comma-separated empty
//! binary values. Strict parsing rejects an over-budget element; lossy parsing
//! discards it and continues with later elements that fit. Application-created
//! metadata (`insert`/`insert_bin`) is not subject to these receive-side limits.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use protolink_http2::HeaderField;

/// Suffix of keys whose values are binary.
const BIN_SUFFIX: &str = "-bin";

const MAX_RECEIVED_ENTRIES: usize = 128;
const MAX_RECEIVED_BYTES: usize = 8192;

/// A metadata value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataValue {
    /// Printable ASCII text.
    Ascii(String),
    /// Arbitrary bytes, for keys ending in `-bin`.
    Binary(Vec<u8>),
}

/// Why a metadata entry was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InvalidMetadata {
    /// The key is empty or has characters outside `[0-9a-z_.-]`.
    Key,
    /// The key is reserved for HTTP/2 or gRPC.
    ReservedKey,
    /// The ASCII value has characters outside `0x20..=0x7e`, or starts or ends
    /// with a space.
    Value,
    /// A text value was given for a key ending in `-bin`, or a binary value
    /// for a key that doesn't.
    BinarySuffix,
    /// A received `-bin` value is not valid base64.
    Base64,
    /// Received metadata exceeds 128 expanded entries or 8192 owned key/value bytes.
    TooLarge,
}

impl fmt::Display for InvalidMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Key => "invalid metadata key",
            Self::ReservedKey => "reserved metadata key",
            Self::Value => {
                "invalid metadata value: only printable ASCII, without leading or trailing spaces"
            }
            Self::BinarySuffix => "binary values need a key ending in `-bin`, and only those",
            Self::Base64 => "malformed base64 in a binary metadata value",
            Self::TooLarge => "received metadata exceeds entry or owned-byte limits",
        })
    }
}

impl core::error::Error for InvalidMetadata {}

/// Ordered custom metadata of a call.
///
/// Keys are lowercase ASCII (`[0-9a-z_.-]`); values are printable ASCII, or
/// bytes for keys ending in `-bin`. Names reserved for HTTP/2 and gRPC are
/// refused. A key may repeat, and the order of entries is kept.
///
/// Received header/trailer blocks are independently capped at 128 expanded
/// entries and 8192 owned key/value bytes (including each repeated key copy).
/// Strict request parsing rejects entries beyond either limit; lossy response
/// parsing skips over-budget elements while retaining later siblings that fit.
/// Application-created metadata is not subject to these receive-side limits.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Metadata {
    entries: Vec<(String, MetadataValue)>,
}

impl Metadata {
    /// No entries.
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Number of entries (repeated keys count once per value).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there are no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Append a text entry. A key already present gets another value.
    ///
    /// The value must be printable ASCII (`0x20..=0x7e`) and must not start or
    /// end with a space: HTTP/2 can't carry boundary whitespace, and the value
    /// is rejected with [`InvalidMetadata::Value`] rather than trimmed.
    /// Empty values and internal spaces are fine. A rejected insertion leaves
    /// the metadata unchanged.
    pub fn insert(&mut self, key: &str, value: &str) -> Result<(), InvalidMetadata> {
        check_key(key)?;
        if key.ends_with(BIN_SUFFIX) {
            return Err(InvalidMetadata::BinarySuffix);
        }
        check_ascii(value)?;
        self.entries
            .push((key.into(), MetadataValue::Ascii(value.into())));
        Ok(())
    }

    /// Append a text entry without validating the value, to exercise defensive
    /// outbound checks with input the public API refuses.
    #[cfg(test)]
    pub(crate) fn insert_unchecked_for_test(&mut self, key: &str, value: &str) {
        self.entries
            .push((key.into(), MetadataValue::Ascii(value.into())));
    }

    /// Append a binary entry; `key` must end in `-bin`.
    pub fn insert_bin(&mut self, key: &str, value: &[u8]) -> Result<(), InvalidMetadata> {
        check_key(key)?;
        if !key.ends_with(BIN_SUFFIX) {
            return Err(InvalidMetadata::BinarySuffix);
        }
        self.entries
            .push((key.into(), MetadataValue::Binary(value.into())));
        Ok(())
    }

    /// First text value of `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries.iter().find_map(|(k, v)| match v {
            MetadataValue::Ascii(s) if k.eq_ignore_ascii_case(key) => Some(s.as_str()),
            _ => None,
        })
    }

    /// Every text value of `key`, in order.
    pub fn get_all<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.entries.iter().filter_map(move |(k, v)| match v {
            MetadataValue::Ascii(s) if k.eq_ignore_ascii_case(key) => Some(s.as_str()),
            _ => None,
        })
    }

    /// First binary value of `key`.
    pub fn get_bin(&self, key: &str) -> Option<&[u8]> {
        self.entries.iter().find_map(|(k, v)| match v {
            MetadataValue::Binary(b) if k.eq_ignore_ascii_case(key) => Some(b.as_slice()),
            _ => None,
        })
    }

    /// Every binary value of `key`, in order.
    pub fn get_all_bin<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.entries.iter().filter_map(move |(k, v)| match v {
            MetadataValue::Binary(b) if k.eq_ignore_ascii_case(key) => Some(b.as_slice()),
            _ => None,
        })
    }

    /// Whether any entry has `key`.
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case(key))
    }

    /// Remove every entry of `key`. Returns how many were removed.
    pub fn remove(&mut self, key: &str) -> usize {
        let before = self.entries.len();
        self.entries.retain(|(k, _)| !k.eq_ignore_ascii_case(key));
        before - self.entries.len()
    }

    /// Remove every entry.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// The entries in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &MetadataValue)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Append the wire form of every entry to `out`.
    pub(crate) fn append_fields(&self, out: &mut Vec<HeaderField>) {
        for (key, value) in &self.entries {
            out.push(HeaderField {
                name: key.clone(),
                value: match value {
                    MetadataValue::Ascii(s) => s.clone(),
                    MetadataValue::Binary(b) => base64_encode(b),
                },
            });
        }
    }

    /// The custom metadata among received header `fields`: pseudo-headers and
    /// reserved names are skipped. Binary fields are split on commas, with
    /// surrounding spaces and tabs removed from each element. Empty elements
    /// represent empty binary values. Repeated fields and elements retain
    /// their wire order. Fails if any entry or element violates the rules.
    pub(crate) fn from_headers(fields: &[HeaderField]) -> Result<Self, InvalidMetadata> {
        let mut md = Self::new();
        let mut owned_bytes = 0;
        for field in fields {
            parse_field(field, &mut md.entries, &mut owned_bytes, false)?;
        }
        Ok(md)
    }

    /// Like [`from_headers`](Self::from_headers), but skips invalid entries.
    /// For combined binary fields, only malformed elements are skipped;
    /// valid siblings are retained in order. Elements exceeding the receive-side
    /// entry or owned-byte limits are also skipped, before key/value allocation.
    pub(crate) fn from_headers_lossy(fields: &[HeaderField]) -> Self {
        let mut md = Self::new();
        let mut owned_bytes = 0;
        for field in fields {
            let _ = parse_field(field, &mut md.entries, &mut owned_bytes, true);
        }
        md
    }
}

fn parse_field(
    field: &HeaderField,
    entries: &mut Vec<(String, MetadataValue)>,
    owned_bytes: &mut usize,
    lossy: bool,
) -> Result<(), InvalidMetadata> {
    let key = field.name.as_str();
    if key.starts_with(':') || is_reserved(key) {
        return Ok(());
    }
    check_key(key)?;
    if key.ends_with(BIN_SUFFIX) {
        for element in field.value.split(',') {
            let element = element.trim_matches([' ', '\t']);
            let next_bytes = base64_decoded_len(element)
                .ok_or(InvalidMetadata::Base64)
                .and_then(|len| received_budget(entries.len(), *owned_bytes, key.len(), len));
            let next_bytes = match next_bytes {
                Ok(bytes) => bytes,
                Err(_) if lossy => continue,
                Err(error) => return Err(error),
            };
            match base64_decode(element) {
                Some(value) => {
                    entries.push((key.into(), MetadataValue::Binary(value)));
                    *owned_bytes = next_bytes;
                }
                None if lossy => continue,
                None => return Err(InvalidMetadata::Base64),
            }
        }
    } else {
        check_ascii(&field.value)?;
        let next_bytes =
            received_budget(entries.len(), *owned_bytes, key.len(), field.value.len())?;
        entries.push((key.into(), MetadataValue::Ascii(field.value.clone())));
        *owned_bytes = next_bytes;
    }
    Ok(())
}

fn received_budget(
    entries: usize,
    owned_bytes: usize,
    key_len: usize,
    value_len: usize,
) -> Result<usize, InvalidMetadata> {
    if entries >= MAX_RECEIVED_ENTRIES {
        return Err(InvalidMetadata::TooLarge);
    }
    owned_bytes
        .checked_add(key_len)
        .and_then(|bytes| bytes.checked_add(value_len))
        .filter(|&bytes| bytes <= MAX_RECEIVED_BYTES)
        .ok_or(InvalidMetadata::TooLarge)
}

fn is_reserved(key: &str) -> bool {
    key.starts_with("grpc-")
        || matches!(
            key,
            "content-type"
                | "te"
                | "host"
                | "content-length"
                | "connection"
                | "keep-alive"
                | "proxy-connection"
                | "transfer-encoding"
                | "upgrade"
                | "trailer"
        )
}

fn check_key(key: &str) -> Result<(), InvalidMetadata> {
    let valid = !key.is_empty()
        && key
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'z' | b'_' | b'.' | b'-'));
    if !valid {
        return Err(InvalidMetadata::Key);
    }
    if is_reserved(key) {
        return Err(InvalidMetadata::ReservedKey);
    }
    Ok(())
}

fn check_ascii(value: &str) -> Result<(), InvalidMetadata> {
    // HTTP/2 field values can't start or end with whitespace, so such values
    // could never be sent.
    if value.bytes().all(|b| (0x20..=0x7e).contains(&b))
        && !value.starts_with(' ')
        && !value.ends_with(' ')
    {
        Ok(())
    } else {
        Err(InvalidMetadata::Value)
    }
}

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 without padding, as gRPC implementations send it.
fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity((data.len() * 4).div_ceil(3));
    for chunk in data.chunks(3) {
        let n = u32::from(chunk[0]) << 16
            | u32::from(chunk.get(1).copied().unwrap_or(0)) << 8
            | u32::from(chunk.get(2).copied().unwrap_or(0));
        for i in 0..=chunk.len() {
            out.push(char::from(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize]));
        }
    }
    out
}

/// Decoded length from base64's shape, without allocating the decoded value.
fn base64_decoded_len(s: &str) -> Option<usize> {
    let unpadded = s.trim_end_matches('=');
    let padding = s.len() - unpadded.len();
    if unpadded.len() % 4 == 1
        || (padding != 0 && (!s.len().is_multiple_of(4) || padding != (4 - unpadded.len() % 4) % 4))
    {
        return None;
    }
    Some(unpadded.len() / 4 * 3 + unpadded.len() % 4 * 3 / 4)
}

/// Standard base64, with or without padding.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let len = base64_decoded_len(s)?;
    let s = s.trim_end_matches('=');
    let mut out = Vec::with_capacity(len);
    let (mut acc, mut bits) = (0u32, 0u32);
    for b in s.bytes() {
        let v = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        };
        acc = acc << 6 | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    // Unused bits in the last base64 symbol must be zero.
    (acc == 0).then_some(out)
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use super::*;

    fn hf(n: &str, v: &str) -> HeaderField {
        HeaderField {
            name: n.into(),
            value: v.into(),
        }
    }

    #[test]
    fn keys_are_validated() {
        let mut md = Metadata::new();
        assert_eq!(md.insert("", "x"), Err(InvalidMetadata::Key));
        assert_eq!(md.insert("Upper", "x"), Err(InvalidMetadata::Key));
        assert_eq!(md.insert("sp ace", "x"), Err(InvalidMetadata::Key));
        assert_eq!(md.insert(":path", "x"), Err(InvalidMetadata::Key));
        assert_eq!(
            md.insert("grpc-timeout", "1S"),
            Err(InvalidMetadata::ReservedKey)
        );
        assert_eq!(
            md.insert("content-type", "x"),
            Err(InvalidMetadata::ReservedKey)
        );
        assert_eq!(md.insert("te", "x"), Err(InvalidMetadata::ReservedKey));
        assert!(md.insert("x-a_b.c-1", "x").is_ok());
        assert_eq!(md.len(), 1);
    }

    #[test]
    fn user_agent_is_not_reserved() {
        let mut md = Metadata::new();
        md.insert("user-agent", "mine/1").unwrap();
        assert_eq!(md.get("user-agent"), Some("mine/1"));
    }

    #[test]
    fn values_are_validated() {
        let mut md = Metadata::new();
        assert_eq!(md.insert("k", "a\nb"), Err(InvalidMetadata::Value));
        assert_eq!(md.insert("k", "caf\u{e9}"), Err(InvalidMetadata::Value));
        assert!(md.insert("k", "a b~").is_ok());
        assert!(md.insert("k", "").is_ok());
    }

    #[test]
    fn binary_suffix_is_enforced_both_ways() {
        let mut md = Metadata::new();
        assert_eq!(md.insert("k-bin", "x"), Err(InvalidMetadata::BinarySuffix));
        assert_eq!(md.insert_bin("k", b"x"), Err(InvalidMetadata::BinarySuffix));
        assert!(md.insert_bin("k-bin", b"\x00\xff").is_ok());
    }

    #[test]
    fn repeated_keys_keep_their_order() {
        let mut md = Metadata::new();
        md.insert("a", "1").unwrap();
        md.insert("b", "x").unwrap();
        md.insert("a", "2").unwrap();
        assert_eq!(md.get("a"), Some("1"));
        assert_eq!(md.get_all("a").collect::<Vec<_>>(), ["1", "2"]);
        assert_eq!(md.get("A"), Some("1"), "lookups ignore case");
        let keys: Vec<_> = md.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, ["a", "b", "a"]);
        assert_eq!(md.remove("a"), 2);
        assert_eq!(md.len(), 1);
        assert!(!md.contains_key("a"));
    }

    #[test]
    fn base64_round_trips_every_length() {
        let data: Vec<u8> = (0..=255).collect();
        for len in 0..data.len() {
            let enc = base64_encode(&data[..len]);
            assert!(!enc.contains('='));
            assert_eq!(base64_decode(&enc).as_deref(), Some(&data[..len]));
        }
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"), "TWE");
        assert_eq!(base64_encode(b"M"), "TQ");
    }

    #[test]
    fn base64_accepts_padding_and_rejects_garbage() {
        assert_eq!(base64_decode("TWE=").as_deref(), Some(&b"Ma"[..]));
        assert_eq!(base64_decode("TQ==").as_deref(), Some(&b"M"[..]));
        assert_eq!(base64_decode("T"), None);
        assert_eq!(base64_decode("TW!u"), None);
    }

    #[test]
    fn combined_binary_fields_preserve_element_and_field_order() {
        let fields = vec![
            hf("trace-bin", " AQ== ,\tAg\t, "),
            hf("plain", "keep, commas"),
            hf("trace-bin", "Aw==,, BA"),
        ];
        let strict = Metadata::from_headers(&fields).unwrap();
        assert_eq!(strict, Metadata::from_headers_lossy(&fields));
        assert_eq!(
            strict.get_all_bin("trace-bin").collect::<Vec<_>>(),
            [&[1][..], &[2][..], &[], &[3][..], &[], &[4][..]]
        );
        assert_eq!(strict.get("plain"), Some("keep, commas"));
        assert_eq!(
            strict.iter().map(|(key, _)| key).collect::<Vec<_>>(),
            [
                "trace-bin",
                "trace-bin",
                "trace-bin",
                "plain",
                "trace-bin",
                "trace-bin",
                "trace-bin"
            ]
        );
    }

    #[test]
    fn combined_binary_fields_handle_malformed_elements_individually() {
        for bad in [
            "!", "A", "AQ===", "Ag=", "===", "AB", "A Q", "\nAQ", "\u{a0}AQ",
        ] {
            for value in [
                alloc::format!("{bad},AQ==,Ag"),
                alloc::format!("AQ==,{bad},Ag"),
                alloc::format!("AQ==,Ag,{bad}"),
            ] {
                let fields = vec![hf("trace-bin", &value), hf("trace-bin", "Aw==")];
                assert_eq!(
                    Metadata::from_headers(&fields),
                    Err(InvalidMetadata::Base64),
                    "{value:?}"
                );
                let lossy = Metadata::from_headers_lossy(&fields);
                assert_eq!(
                    lossy.get_all_bin("trace-bin").collect::<Vec<_>>(),
                    [&[1][..], &[2][..], &[3][..]],
                    "{value:?}"
                );
            }
        }
    }

    fn owned_bytes(md: &Metadata) -> usize {
        md.entries
            .iter()
            .map(|(key, value)| {
                key.capacity()
                    + match value {
                        MetadataValue::Ascii(value) => value.capacity(),
                        MetadataValue::Binary(value) => value.capacity(),
                    }
            })
            .sum()
    }

    fn assert_receive_bounds(md: &Metadata) {
        assert!(md.len() <= MAX_RECEIVED_ENTRIES);
        assert!(md.entries.capacity() <= MAX_RECEIVED_ENTRIES);
        assert!(owned_bytes(md) <= MAX_RECEIVED_BYTES);
    }

    #[test]
    fn long_key_empty_sibling_amplification_is_bounded_before_materialization() {
        let key = alloc::format!("{}-bin", "a".repeat(3896));
        let fields = vec![hf(&key, &",".repeat(3900))];
        assert!(key.len() + fields[0].value.len() + 32 <= 8115);
        assert_eq!(
            Metadata::from_headers(&fields),
            Err(InvalidMetadata::TooLarge)
        );
        let lossy = Metadata::from_headers_lossy(&fields);
        assert_receive_bounds(&lossy);
        assert_eq!(lossy.len(), MAX_RECEIVED_BYTES / key.len());
        assert_eq!(owned_bytes(&lossy), 7800);
        assert!(lossy.get_all_bin(&key).all(<[u8]>::is_empty));

        let valid = vec![hf(&key, "AQ==, Ag")];
        let strict = Metadata::from_headers(&valid).unwrap();
        assert_receive_bounds(&strict);
        assert_eq!(strict, Metadata::from_headers_lossy(&valid));
        assert_eq!(
            strict.get_all_bin(&key).collect::<Vec<_>>(),
            [&[1][..], &[2][..]]
        );
    }

    #[test]
    fn expanded_entry_limit_applies_across_combined_and_repeated_fields() {
        let exact = vec![hf("k-bin", &",".repeat(MAX_RECEIVED_ENTRIES - 1))];
        let strict = Metadata::from_headers(&exact).unwrap();
        assert_eq!(strict.len(), MAX_RECEIVED_ENTRIES);
        assert_receive_bounds(&strict);
        assert_eq!(strict, Metadata::from_headers_lossy(&exact));

        for fields in [
            vec![hf("k-bin", &",".repeat(MAX_RECEIVED_ENTRIES))],
            vec![hf("k-bin", &",".repeat(64)), hf("k-bin", &",".repeat(64))],
        ] {
            assert_eq!(
                Metadata::from_headers(&fields),
                Err(InvalidMetadata::TooLarge)
            );
            let lossy = Metadata::from_headers_lossy(&fields);
            assert_receive_bounds(&lossy);
            assert_eq!(lossy.len(), MAX_RECEIVED_ENTRIES);
        }
    }

    #[test]
    fn owned_byte_limit_counts_keys_ascii_and_decoded_binary_values() {
        let ascii = vec![hf("k", &"a".repeat(MAX_RECEIVED_BYTES - 1))];
        let strict = Metadata::from_headers(&ascii).unwrap();
        assert_eq!(owned_bytes(&strict), MAX_RECEIVED_BYTES);
        assert_receive_bounds(&strict);
        assert_eq!(strict, Metadata::from_headers_lossy(&ascii));

        let binary = vec![hf(
            "k-bin",
            &base64_encode(&vec![0; MAX_RECEIVED_BYTES - 5]),
        )];
        let strict = Metadata::from_headers(&binary).unwrap();
        assert_eq!(owned_bytes(&strict), MAX_RECEIVED_BYTES);
        assert_receive_bounds(&strict);
        assert_eq!(strict, Metadata::from_headers_lossy(&binary));

        for fields in [
            vec![hf("k", &"a".repeat(MAX_RECEIVED_BYTES)), hf("ok", "v")],
            vec![
                hf("k-bin", &base64_encode(&vec![0; MAX_RECEIVED_BYTES - 4])),
                hf("ok", "v"),
            ],
            vec![hf(&"a".repeat(MAX_RECEIVED_BYTES + 1), ""), hf("ok", "v")],
        ] {
            assert_eq!(
                Metadata::from_headers(&fields),
                Err(InvalidMetadata::TooLarge)
            );
            let lossy = Metadata::from_headers_lossy(&fields);
            assert_receive_bounds(&lossy);
            assert_eq!(lossy.len(), 1);
            assert_eq!(lossy.get("ok"), Some("v"));
        }
        assert_eq!(
            received_budget(0, 0, usize::MAX, 1),
            Err(InvalidMetadata::TooLarge)
        );
        assert_eq!(
            received_budget(0, usize::MAX, 1, 0),
            Err(InvalidMetadata::TooLarge)
        );
    }

    #[test]
    fn lossy_byte_budget_skips_large_elements_but_keeps_smaller_later_siblings() {
        let large = base64_encode(&[0; 20]);
        let fields = vec![
            hf("k", &"a".repeat(MAX_RECEIVED_BYTES - 13)),
            hf("k-bin", &alloc::format!("{large},AB,AQ==,,Ag")),
        ];
        assert_eq!(
            Metadata::from_headers(&fields),
            Err(InvalidMetadata::TooLarge)
        );
        let lossy = Metadata::from_headers_lossy(&fields);
        assert_receive_bounds(&lossy);
        assert_eq!(
            lossy.get_all_bin("k-bin").collect::<Vec<_>>(),
            [&[1][..], &[]]
        );
        assert_eq!(owned_bytes(&lossy), MAX_RECEIVED_BYTES - 1);
    }

    #[test]
    fn binary_values_use_base64_on_the_wire() {
        let mut md = Metadata::new();
        md.insert_bin("trace-bin", &[0, 1, 2, 250]).unwrap();
        md.insert("plain", "v").unwrap();
        let mut fields = Vec::new();
        md.append_fields(&mut fields);
        assert_eq!(fields, vec![hf("trace-bin", "AAEC+g"), hf("plain", "v")]);
        assert_eq!(Metadata::from_headers(&fields).unwrap(), md);
        assert_eq!(md.get_bin("trace-bin"), Some(&[0, 1, 2, 250][..]));
    }

    #[test]
    fn received_headers_skip_protocol_names() {
        let fields = vec![
            hf(":method", "POST"),
            hf("content-type", "application/grpc"),
            hf("te", "trailers"),
            hf("grpc-timeout", "1S"),
            hf("x-a", "1"),
            hf("user-agent", "ua"),
        ];
        let md = Metadata::from_headers(&fields).unwrap();
        assert_eq!(md.len(), 2);
        assert_eq!(md.get("x-a"), Some("1"));
        assert_eq!(md.get("user-agent"), Some("ua"));
    }

    #[test]
    fn malformed_received_entries_fail_strict_and_are_skipped_lossy() {
        let bad_b64 = vec![hf("x-a", "1"), hf("k-bin", "T!"), hf("x-b", "2")];
        assert_eq!(
            Metadata::from_headers(&bad_b64),
            Err(InvalidMetadata::Base64)
        );
        let md = Metadata::from_headers_lossy(&bad_b64);
        assert_eq!(
            md.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            ["x-a", "x-b"]
        );

        let bad_value = vec![hf("x-a", "a\u{1}b")];
        assert_eq!(
            Metadata::from_headers(&bad_value),
            Err(InvalidMetadata::Value)
        );
        let bad_key = vec![hf("X-Upper", "a")];
        assert_eq!(Metadata::from_headers(&bad_key), Err(InvalidMetadata::Key));
    }
}
