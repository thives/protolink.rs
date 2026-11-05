//! Lua 5.4 numeric and comparison rules for the implemented subset.
//!
//! Integers wrap. Mixed integer and float comparisons are exact. Float results may be non-finite locally; the boundary
//! rejects them when they would cross a node, which is the local policy the plans require.

use core::cmp::Ordering;

use crate::heap::Val;

const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
const TWO_POW_52: f64 = 4_503_599_627_370_496.0;

/// Largest integer not greater than `f`. `core` has no `f64::floor` without `std`, so this is computed directly. Values
/// of magnitude at least 2^52 are already integral, and NaN and infinities are returned unchanged. Zero is returned with
/// its own sign: the C `floor` of `-0.0` is `-0.0`, and the integer conversion below would lose that sign.
pub(crate) fn floor(f: f64) -> f64 {
    if !f.is_finite() || f.abs() >= TWO_POW_52 || f == 0.0 {
        return f;
    }
    let truncated = f as i64 as f64;
    if truncated > f {
        truncated - 1.0
    } else {
        truncated
    }
}

/// Exact comparison of an integer with a float. `None` when the float is NaN.
pub(crate) fn cmp_int_float(i: i64, f: f64) -> Option<Ordering> {
    if f.is_nan() {
        return None;
    }
    if f >= TWO_POW_63 {
        return Some(Ordering::Less);
    }
    if f < -TWO_POW_63 {
        return Some(Ordering::Greater);
    }
    let floor = floor(f);
    let as_int = floor as i64;
    Some(match i.cmp(&as_int) {
        Ordering::Equal if f > floor => Ordering::Less,
        other => other,
    })
}

/// Floor division on integers. `None` for division by zero.
pub(crate) fn idiv_int(a: i64, b: i64) -> Option<i64> {
    if b == 0 {
        return None;
    }
    let q = a.wrapping_div(b);
    let r = a.wrapping_rem(b);
    if r != 0 && ((r < 0) != (b < 0)) {
        Some(q.wrapping_sub(1))
    } else {
        Some(q)
    }
}

/// Lua modulo on integers. `None` for modulo by zero.
pub(crate) fn mod_int(a: i64, b: i64) -> Option<i64> {
    if b == 0 {
        return None;
    }
    let r = a.wrapping_rem(b);
    if r != 0 && ((r ^ b) < 0) {
        Some(r.wrapping_add(b))
    } else {
        Some(r)
    }
}

/// Lua modulo on floats.
pub(crate) fn mod_float(a: f64, b: f64) -> f64 {
    let m = a % b;
    if (m > 0.0 && b < 0.0) || (m < 0.0 && b > 0.0 && b != m) {
        m + b
    } else {
        m
    }
}

/// Numeric view of a value, if it is a number.
pub(crate) fn number(value: Val) -> Option<Num> {
    match value {
        Val::Integer(i) => Some(Num::Int(i)),
        Val::Float(f) => Some(Num::Float(f)),
        _ => None,
    }
}

/// A Lua number.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Num {
    Int(i64),
    Float(f64),
}

impl Num {
    pub(crate) fn as_f64(self) -> f64 {
        match self {
            Num::Int(i) => i as f64,
            Num::Float(f) => f,
        }
    }
}

/// Ordering of two numbers, or `None` when unordered (NaN involved).
pub(crate) fn cmp_num(a: Num, b: Num) -> Option<Ordering> {
    match (a, b) {
        (Num::Int(x), Num::Int(y)) => Some(x.cmp(&y)),
        (Num::Float(x), Num::Float(y)) => x.partial_cmp(&y),
        (Num::Int(x), Num::Float(y)) => cmp_int_float(x, y),
        (Num::Float(x), Num::Int(y)) => cmp_int_float(y, x).map(Ordering::reverse),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_matches_c_floor_including_the_sign_of_zero() {
        for (input, expected) in [
            (0.0_f64, 0.0_f64),
            (-0.0, -0.0),
            (0.5, 0.0),
            (-0.5, -1.0),
            (-1e-300, -1.0),
            (-1.0, -1.0),
            (2.75, 2.0),
            (-2.75, -3.0),
        ] {
            let got = floor(input);
            assert_eq!(got, expected, "floor({input:?})");
            assert_eq!(
                got.is_sign_negative(),
                expected.is_sign_negative(),
                "sign of floor({input:?})"
            );
        }
        assert!(floor(f64::NAN).is_nan());
        assert_eq!(floor(f64::NEG_INFINITY), f64::NEG_INFINITY);
    }
}
