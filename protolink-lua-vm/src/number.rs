//! Float to string conversion, byte for byte as Lua 5.4 does it for `..` and `tostring`.
//!
//! # Reference configuration
//!
//! The reference is the stock Lua 5.4 `lobject.c` (`tostringbuff`) built with the default `luaconf.h`:
//!
//! * `lua_Number` is `double` (`LUA_FLOAT_DOUBLE`), and `LUAI_NUMFFORMAT` is `"%.14g"`.
//! * The result goes through the C library's `snprintf`. The decimal point is the one of the C locale, `'.'`, because
//!   the Lua library never calls `setlocale`. An embedder that calls `setlocale` with another decimal point changes the
//!   stock result, and this implementation does not follow it.
//! * When the text contains only `-` and digits (it "looks like an integer"), Lua appends `.0`. So `1.0` is `1.0` and
//!   `1e15` is `1e+15`.
//! * Infinities and NaNs take the C library's spelling: `inf`, `-inf`, `nan` and `-nan`. The sign of a NaN is its sign
//!   bit, as glibc and musl print it. A C library that prints `NaN` or `1.#INF` (Windows) gives other text.
//!
//! A Lua built with `LUA_32BITS` (`float` numbers and `"%.7g"`), with `LUA_NUMBER` set to `long double`, or with a
//! changed `LUAI_NUMFFORMAT`, is a different reference and is not supported.
//!
//! # Method
//!
//! `%.14g` is "14 significant digits, correctly rounded, then choose between fixed and exponent notation by the decimal
//! exponent, then drop trailing zeros". Rust's `{:.13e}` is the exactly rounded 14-significant-digit decimal expansion of
//! the binary value, which is what a correct `printf` rounds. The digits and the exponent are taken from it, and the
//! `%g` layout is applied here. This needs no allocation and no dependency: it runs on `core::fmt` only.

use core::fmt::{self, Write};

/// Significant digits of Lua's float format (`%.14g`).
const PRECISION: usize = 14;

/// The longest text of a float: a sign, 14 digits and a point in fixed notation, or a sign, 14 digits, a point and
/// `e+308` in exponent notation, plus the `.0` Lua may append. Padded to a round figure.
pub(crate) const FLOAT_TEXT_BYTES: usize = 32;

/// A fixed buffer that `core::fmt` writes into.
struct Scratch<const N: usize> {
    bytes: [u8; N],
    len: usize,
}

impl<const N: usize> Write for Scratch<N> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let end = self.len.checked_add(text.len()).ok_or(fmt::Error)?;
        let room = self.bytes.get_mut(self.len..end).ok_or(fmt::Error)?;
        room.copy_from_slice(text.as_bytes());
        self.len = end;
        Ok(())
    }
}

/// A float's text, held inline.
pub(crate) struct FloatText {
    bytes: [u8; FLOAT_TEXT_BYTES],
    len: usize,
}

impl FloatText {
    fn push(&mut self, byte: u8) {
        // The longest text is below `FLOAT_TEXT_BYTES`; the unit tests cover the extremes.
        self.bytes[self.len] = byte;
        self.len += 1;
    }

    fn push_all(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.push(byte);
        }
    }

    /// The text.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// The text Lua 5.4 gives a float: `lua_Number2str` with `%.14g`, then `.0` when the text looks like an integer.
pub(crate) fn float_text(value: f64) -> FloatText {
    let mut out = FloatText {
        bytes: [0; FLOAT_TEXT_BYTES],
        len: 0,
    };
    if value.is_sign_negative() {
        out.push(b'-');
    }
    if value.is_nan() {
        out.push_all(b"nan");
        return out;
    }
    if value.is_infinite() {
        out.push_all(b"inf");
        return out;
    }
    let (digits, exponent) = significant_digits(value.abs());
    layout(&mut out, &digits, exponent);
    // `tostringbuff`: text of only '-' and digits gets ".0".
    if out
        .as_bytes()
        .iter()
        .all(|b| *b == b'-' || b.is_ascii_digit())
    {
        out.push_all(b".0");
    }
    out
}

/// The 14 significant digits of a finite, non-negative `value` and the decimal exponent of the first one, after rounding.
/// Zero has digits `0` and exponent 0.
fn significant_digits(value: f64) -> ([u8; PRECISION], i32) {
    // `{:.13e}` writes `d.ddddddddddddde<exp>`: at most 1 + 1 + 13 + 1 + 1 + 3 bytes.
    let mut scratch = Scratch::<24> {
        bytes: [0; 24],
        len: 0,
    };
    // A 24-byte buffer holds every `{:.13e}` of an f64, so the write cannot fail.
    let written = write!(scratch, "{value:.13e}");
    debug_assert!(written.is_ok());
    let text = &scratch.bytes[..scratch.len];
    let mut digits = [b'0'; PRECISION];
    let mut count = 0;
    let mut at = 0;
    while at < text.len() && text[at] != b'e' {
        if text[at].is_ascii_digit() {
            digits[count] = text[at];
            count += 1;
        }
        at += 1;
    }
    debug_assert_eq!(count, PRECISION);
    // After the `e`: an optional '-' and the decimal exponent.
    let rest = &text[(at + 1).min(text.len())..];
    let (negative, rest) = match rest.split_first() {
        Some((b'-', tail)) => (true, tail),
        _ => (false, rest),
    };
    let mut exponent = 0i32;
    for &byte in rest {
        exponent = exponent * 10 + i32::from(byte - b'0');
    }
    if negative {
        exponent = -exponent;
    }
    (digits, exponent)
}

/// Applies the `%g` rule for precision 14: fixed notation when `-4 <= exponent < 14`, exponent notation otherwise, and
/// trailing zeros and a bare point dropped in both.
fn layout(out: &mut FloatText, digits: &[u8; PRECISION], exponent: i32) {
    // Digits that remain after dropping trailing zeros. At least one: `%g` of zero is `0`.
    let mut kept = PRECISION;
    while kept > 1 && digits[kept - 1] == b'0' {
        kept -= 1;
    }
    let precision = PRECISION as i32;
    if (-4..precision).contains(&exponent) {
        if exponent < 0 {
            // 0.000ddd
            out.push_all(b"0.");
            for _ in 0..(-exponent - 1) {
                out.push(b'0');
            }
            out.push_all(&digits[..kept]);
        } else {
            let whole = exponent as usize + 1;
            out.push_all(&digits[..kept.min(whole)]);
            for _ in kept..whole {
                out.push(b'0');
            }
            if kept > whole {
                out.push(b'.');
                out.push_all(&digits[whole..kept]);
            }
        }
    } else {
        out.push(digits[0]);
        if kept > 1 {
            out.push(b'.');
            out.push_all(&digits[1..kept]);
        }
        out.push(b'e');
        out.push(if exponent < 0 { b'-' } else { b'+' });
        let magnitude = exponent.unsigned_abs();
        // At least two digits, as C prints them: `e+05`, `e+100`.
        if magnitude >= 100 {
            out.push(b'0' + (magnitude / 100) as u8);
        }
        out.push(b'0' + (magnitude / 10 % 10) as u8);
        out.push(b'0' + (magnitude % 10) as u8);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::String;
    use alloc::vec::Vec;

    fn text(value: f64) -> String {
        String::from_utf8(float_text(value).as_bytes().to_vec()).expect("ascii")
    }

    #[test]
    fn the_layout_follows_percent_g_with_fourteen_digits() {
        let cases: &[(f64, &str)] = &[
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (10.0, "10.0"),
            (0.5, "0.5"),
            (-0.5, "-0.5"),
            (0.1 + 0.2, "0.3"),
            (123_456.789, "123456.789"),
            (1e13, "10000000000000.0"),
            (99_999_999_999_999.0, "99999999999999.0"),
            // Rounding to 14 digits carries into the next power of ten, which moves the notation.
            (99_999_999_999_999.5, "1e+14"),
            (999_999_999_999_999.0, "1e+15"),
            (1e14, "1e+14"),
            (1.5e14, "1.5e+14"),
            (0.0001, "0.0001"),
            (0.000_123_45, "0.00012345"),
            (0.00001, "1e-05"),
            (0.000_012_5, "1.25e-05"),
            (1e-100, "1e-100"),
            (1e100, "1e+100"),
            (2f64.powi(53), "9.007199254741e+15"),
            (f64::MAX, "1.7976931348623e+308"),
            (f64::MIN_POSITIVE, "2.2250738585072e-308"),
            (5e-324, "4.9406564584125e-324"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
        ];
        for &(value, expected) in cases {
            assert_eq!(text(value), expected, "{value:?}");
        }
    }

    #[test]
    fn the_sign_of_a_nan_is_its_sign_bit() {
        assert_eq!(text(f64::from_bits(0x7ff8_0000_0000_0000)), "nan");
        assert_eq!(text(f64::from_bits(0xfff8_0000_0000_0000)), "-nan");
        // A payload NaN prints the same.
        assert_eq!(text(f64::from_bits(0x7ff0_0000_0000_0001)), "nan");
    }

    /// No float can need more room than the buffer has. The extremes of every notation are checked.
    #[test]
    fn the_longest_texts_fit_the_buffer() {
        let longest: Vec<f64> = alloc::vec![
            -1.234_567_890_123_4e-5,
            -1.234_567_890_123_4e-300,
            -1.234_567_890_123_4e+300,
            -0.000_123_456_789_012_34,
            -12_345_678_901_234.0,
            -f64::MAX,
            -5e-324,
        ];
        for value in longest {
            let size = float_text(value).as_bytes().len();
            assert!(size < FLOAT_TEXT_BYTES, "{value:?} needs {size} bytes");
        }
    }

    /// Every 14-digit result is at most 14 significant digits, and parses back to within the precision `%.14g` promises.
    #[test]
    fn generated_values_keep_fourteen_significant_digits() {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20_000 {
            let value = f64::from_bits(next());
            if !value.is_finite() {
                continue;
            }
            let rendered = text(value);
            let digits = rendered
                .split('e')
                .next()
                .expect("a mantissa")
                .chars()
                .filter(char::is_ascii_digit)
                .collect::<String>();
            let significant = digits.trim_start_matches('0').len();
            assert!(
                significant <= 14 || rendered.ends_with(".0"),
                "{rendered} for {value:?}"
            );
            let back: f64 = rendered.parse().expect("parses");
            let tolerance = value.abs() * 1e-13;
            assert!(
                (back - value).abs() <= tolerance,
                "{rendered} is too far from {value:?}"
            );
        }
    }
}
