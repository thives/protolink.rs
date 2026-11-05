//! Remote-data v1: the bounded tuples of Lua values that cross node boundaries.
//!
//! A tuple keeps its arity. Nil is an ordinary value, so zero values, one nil and several trailing nils are distinct.
//! Limits are checked before any allocation that depends on wire input.

use alloc::vec::Vec;
use core::fmt;

use crate::wire::v1;

/// Bounds for one tuple. Callers choose them from the node's advertised limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    max_items: usize,
    max_string_bytes: usize,
    max_total_bytes: usize,
}

impl Limits {
    /// Creates limits for at most `max_items` values, `max_string_bytes` per byte string and `max_total_bytes` across all
    /// byte strings of one tuple.
    pub const fn new(max_items: usize, max_string_bytes: usize, max_total_bytes: usize) -> Self {
        Self {
            max_items,
            max_string_bytes,
            max_total_bytes,
        }
    }

    /// Maximum number of values in one tuple.
    pub const fn max_items(self) -> usize {
        self.max_items
    }

    /// Maximum length of one byte string.
    pub const fn max_string_bytes(self) -> usize {
        self.max_string_bytes
    }

    /// Maximum total length of byte strings in one tuple.
    pub const fn max_total_bytes(self) -> usize {
        self.max_total_bytes
    }
}

/// One remote-data v1 value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// Lua `nil`.
    Nil,
    /// Lua boolean.
    Boolean(bool),
    /// Lua integer, signed 64-bit with wrapping arithmetic on the sender.
    Integer(i64),
    /// Finite IEEE-754 binary64. Non-finite values are rejected at every boundary.
    Float(f64),
    /// Byte string. It may contain NUL and non-UTF-8 bytes.
    Bytes(Vec<u8>),
}

impl Value {
    /// Creates a float value, rejecting NaN and infinities.
    pub fn float(value: f64) -> Result<Self, ValueError> {
        if value.is_finite() {
            Ok(Self::Float(value))
        } else {
            Err(ValueError::NonFiniteFloat)
        }
    }
}

/// Why a value or tuple was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueError {
    /// The tuple has more values than the limit allows.
    TooManyItems {
        /// The limit that was exceeded.
        limit: usize,
    },
    /// One byte string is longer than the limit.
    StringTooLong {
        /// The limit that was exceeded.
        limit: usize,
    },
    /// The byte strings of one tuple together exceed the limit.
    TotalTooLarge {
        /// The limit that was exceeded.
        limit: usize,
    },
    /// A float is NaN or infinite.
    NonFiniteFloat,
    /// A wire value has no kind selected. An absent union is not nil.
    MissingKind,
}

impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyItems { limit } => write!(f, "tuple has more than {limit} values"),
            Self::StringTooLong { limit } => write!(f, "byte string is longer than {limit} bytes"),
            Self::TotalTooLarge { limit } => {
                write!(f, "byte strings exceed {limit} bytes in total")
            }
            Self::NonFiniteFloat => f.write_str("float is not finite"),
            Self::MissingKind => f.write_str("value has no kind"),
        }
    }
}

impl core::error::Error for ValueError {}

/// An ordered tuple of values with its arity preserved.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Tuple {
    values: Vec<Value>,
}

impl Tuple {
    /// The tuple with no values.
    pub const fn empty() -> Self {
        Self { values: Vec::new() }
    }

    /// Creates a tuple after checking every bound.
    pub fn new(values: Vec<Value>, limits: Limits) -> Result<Self, ValueError> {
        check(&values, limits)?;
        Ok(Self { values })
    }

    /// Total length of the byte strings in the tuple. A capture or a result is bounded by this measure.
    pub fn payload_bytes(&self) -> usize {
        self.values
            .iter()
            .map(|value| match value {
                Value::Bytes(bytes) => bytes.len(),
                _ => 0,
            })
            .sum()
    }

    /// The values in order.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Consumes the tuple and returns its values.
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    /// Number of values, including nils.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// True when the tuple has no values. This is different from a tuple holding one nil.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Re-checks the tuple against `limits`.
    pub fn check(&self, limits: Limits) -> Result<(), ValueError> {
        check(&self.values, limits)
    }

    /// Converts a decoded wire tuple. Every bound is checked before any byte string is copied, so a refused tuple costs
    /// no copies. The size of the frame that carried the tuple is not checked here: protobuf decoding has already
    /// allocated what the frame held by the time this runs.
    pub fn from_wire(wire: &v1::Tuple, limits: Limits) -> Result<Self, ValueError> {
        check_wire(&wire.values, limits)?;
        let mut values = Vec::with_capacity(wire.values.len());
        for item in &wire.values {
            values.push(value_from_wire(item)?);
        }
        Ok(Self { values })
    }

    /// Converts to the wire representation.
    pub fn to_wire(&self) -> v1::Tuple {
        v1::Tuple {
            r#values: self.values.iter().map(value_to_wire).collect(),
        }
    }
}

/// Checks values against `limits` without copying them: the item count, finite floats, each byte string and their
/// checked total. A boundary runs this before it copies anything.
pub fn check_values(values: &[Value], limits: Limits) -> Result<(), ValueError> {
    check(values, limits)
}

/// Checks one value against the per-string bound of `limits` and rejects a non-finite float.
pub fn check_value(value: &Value, limits: Limits) -> Result<(), ValueError> {
    match value {
        Value::Float(f) if !f.is_finite() => Err(ValueError::NonFiniteFloat),
        Value::Bytes(bytes) => add_string(0, bytes.len(), limits).map(|_| ()),
        _ => Ok(()),
    }
}

fn check(values: &[Value], limits: Limits) -> Result<(), ValueError> {
    if values.len() > limits.max_items {
        return Err(ValueError::TooManyItems {
            limit: limits.max_items,
        });
    }
    let mut total = 0usize;
    for value in values {
        match value {
            Value::Float(f) if !f.is_finite() => return Err(ValueError::NonFiniteFloat),
            Value::Bytes(bytes) => total = add_string(total, bytes.len(), limits)?,
            _ => {}
        }
    }
    Ok(())
}

/// Checks a wire tuple against `limits` without copying anything. It applies the same rules as `check`, and also
/// rejects a value with no kind, which the wire type allows.
fn check_wire(values: &[v1::Value], limits: Limits) -> Result<(), ValueError> {
    use v1::Value_::Kind;
    if values.len() > limits.max_items {
        return Err(ValueError::TooManyItems {
            limit: limits.max_items,
        });
    }
    let mut total = 0usize;
    for item in values {
        match &item.r#kind {
            None => return Err(ValueError::MissingKind),
            Some(Kind::Float(f)) if !f.is_finite() => return Err(ValueError::NonFiniteFloat),
            Some(Kind::Bytes(bytes)) => total = add_string(total, bytes.len(), limits)?,
            Some(_) => {}
        }
    }
    Ok(())
}

/// Adds one byte string of `len` bytes to the running total of a tuple. The sum is checked, so it cannot wrap, and a
/// string is refused when it or the total is over its limit.
fn add_string(total: usize, len: usize, limits: Limits) -> Result<usize, ValueError> {
    if len > limits.max_string_bytes {
        return Err(ValueError::StringTooLong {
            limit: limits.max_string_bytes,
        });
    }
    total
        .checked_add(len)
        .filter(|&sum| sum <= limits.max_total_bytes)
        .ok_or(ValueError::TotalTooLarge {
            limit: limits.max_total_bytes,
        })
}

/// Copies one wire value. Call it only after `check_wire` has accepted the tuple.
fn value_from_wire(item: &v1::Value) -> Result<Value, ValueError> {
    use v1::Value_::Kind;
    match &item.r#kind {
        None => Err(ValueError::MissingKind),
        Some(Kind::Nil(_)) => Ok(Value::Nil),
        Some(Kind::Boolean(b)) => Ok(Value::Boolean(*b)),
        Some(Kind::Integer(i)) => Ok(Value::Integer(*i)),
        Some(Kind::Float(f)) => Value::float(*f),
        Some(Kind::Bytes(bytes)) => Ok(Value::Bytes(bytes.clone())),
    }
}

fn value_to_wire(value: &Value) -> v1::Value {
    use v1::Value_::Kind;
    let kind = match value {
        Value::Nil => Kind::Nil(v1::Nil {}),
        Value::Boolean(b) => Kind::Boolean(*b),
        Value::Integer(i) => Kind::Integer(*i),
        Value::Float(f) => Kind::Float(*f),
        Value::Bytes(bytes) => Kind::Bytes(bytes.clone()),
    };
    v1::Value { r#kind: Some(kind) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec;
    use alloc::vec;

    const LIMITS: Limits = Limits::new(4, 8, 12);

    #[test]
    fn nil_positions_and_trailing_nils_survive_a_round_trip() {
        for values in [
            vec![],
            vec![Value::Nil],
            vec![Value::Integer(1), Value::Nil, Value::Nil],
        ] {
            let tuple = Tuple::new(values.clone(), LIMITS).expect("within limits");
            let bytes = codec::encode(&tuple.to_wire());
            let wire: v1::Tuple = codec::decode(&bytes).expect("decodes");
            let back = Tuple::from_wire(&wire, LIMITS).expect("converts");
            assert_eq!(back.values(), values.as_slice());
        }
    }

    #[test]
    fn zero_values_and_one_nil_are_different_tuples() {
        let none = Tuple::empty();
        let one_nil = Tuple::new(vec![Value::Nil], LIMITS).expect("ok");
        assert_ne!(none, one_nil);
        assert_ne!(
            codec::encode(&none.to_wire()),
            codec::encode(&one_nil.to_wire())
        );
    }

    #[test]
    fn items_string_and_total_bounds_are_enforced() {
        assert_eq!(
            Tuple::new(vec![Value::Nil; 5], LIMITS),
            Err(ValueError::TooManyItems { limit: 4 })
        );
        assert_eq!(
            Tuple::new(vec![Value::Bytes(vec![0; 9])], LIMITS),
            Err(ValueError::StringTooLong { limit: 8 })
        );
        assert_eq!(
            Tuple::new(
                vec![Value::Bytes(vec![0; 7]), Value::Bytes(vec![0; 6])],
                LIMITS
            ),
            Err(ValueError::TotalTooLarge { limit: 12 })
        );
    }

    #[test]
    fn non_finite_floats_are_rejected_both_ways() {
        assert_eq!(Value::float(f64::NAN), Err(ValueError::NonFiniteFloat));
        assert_eq!(Value::float(f64::INFINITY), Err(ValueError::NonFiniteFloat));
        assert_eq!(
            Tuple::new(vec![Value::Float(f64::NEG_INFINITY)], LIMITS),
            Err(ValueError::NonFiniteFloat)
        );
        let wire = v1::Tuple {
            r#values: vec![v1::Value {
                r#kind: Some(v1::Value_::Kind::Float(f64::NAN)),
            }],
        };
        assert_eq!(
            Tuple::from_wire(&wire, LIMITS),
            Err(ValueError::NonFiniteFloat)
        );
    }

    #[test]
    fn an_absent_union_is_not_nil() {
        let wire = v1::Tuple {
            r#values: vec![v1::Value { r#kind: None }],
        };
        assert_eq!(
            Tuple::from_wire(&wire, LIMITS),
            Err(ValueError::MissingKind)
        );
    }

    fn wire_bytes(lengths: &[usize]) -> v1::Tuple {
        v1::Tuple {
            r#values: lengths
                .iter()
                .map(|&len| v1::Value {
                    r#kind: Some(v1::Value_::Kind::Bytes(vec![0; len])),
                })
                .collect(),
        }
    }

    #[test]
    fn a_wire_aggregate_exactly_at_the_total_limit_converts() {
        // Two strings of 6 bytes each: each is within the string limit, and together they meet the total exactly.
        let wire = wire_bytes(&[6, 6]);
        let tuple = Tuple::from_wire(&wire, LIMITS).expect("at the limit");
        assert_eq!(tuple.payload_bytes(), 12);
    }

    #[test]
    fn a_wire_aggregate_one_byte_over_the_total_limit_is_refused() {
        assert_eq!(
            Tuple::from_wire(&wire_bytes(&[7, 6]), LIMITS),
            Err(ValueError::TotalTooLarge { limit: 12 })
        );
    }

    #[test]
    fn several_valid_strings_whose_sum_is_over_the_total_are_refused() {
        // Each string is within its own limit. Only the running total exceeds the bound.
        assert_eq!(
            Tuple::from_wire(&wire_bytes(&[5, 5, 5]), LIMITS),
            Err(ValueError::TotalTooLarge { limit: 12 })
        );
    }

    #[test]
    fn a_refused_wire_tuple_is_refused_whichever_value_breaks_the_bound() {
        // The bad value is last, after valid strings that would already be copied by a convert-as-you-go loop.
        let mut wire = wire_bytes(&[4, 4]);
        wire.r#values.push(v1::Value {
            r#kind: Some(v1::Value_::Kind::Bytes(vec![0; 9])),
        });
        assert_eq!(
            Tuple::from_wire(&wire, LIMITS),
            Err(ValueError::StringTooLong { limit: 8 })
        );
        let mut missing = wire_bytes(&[4]);
        missing.r#values.push(v1::Value { r#kind: None });
        assert_eq!(
            Tuple::from_wire(&missing, LIMITS),
            Err(ValueError::MissingKind)
        );
    }

    #[test]
    fn wire_tuples_longer_than_the_limit_are_refused_before_copying() {
        let wire = v1::Tuple {
            r#values: vec![
                v1::Value {
                    r#kind: Some(v1::Value_::Kind::Nil(v1::Nil {})),
                };
                5
            ],
        };
        assert_eq!(
            Tuple::from_wire(&wire, LIMITS),
            Err(ValueError::TooManyItems { limit: 4 })
        );
    }
}
