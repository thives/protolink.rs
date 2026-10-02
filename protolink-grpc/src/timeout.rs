//! The `grpc-timeout` header: 1 to 8 ASCII digits followed by a unit.

use alloc::string::{String, ToString};
use core::time::Duration;

/// Largest value that fits the header (8 digits).
const MAX_VALUE: u128 = 99_999_999;

/// Units from finest to coarsest, with their length in nanoseconds.
const UNITS: [(u8, u128); 6] = [
    (b'n', 1),
    (b'u', 1_000),
    (b'm', 1_000_000),
    (b'S', 1_000_000_000),
    (b'M', 60 * 1_000_000_000),
    (b'H', 3_600 * 1_000_000_000),
];

/// Parse a `grpc-timeout` value. `None` if it is malformed.
///
/// A value of zero is accepted and means the deadline has already passed.
pub(crate) fn parse(value: &str) -> Option<Duration> {
    let (&unit, digits) = value.as_bytes().split_last()?;
    if digits.is_empty() || digits.len() > 8 {
        return None;
    }
    let (_, size) = UNITS.iter().find(|(c, _)| *c == unit)?;
    let mut number: u128 = 0;
    for &d in digits {
        if !d.is_ascii_digit() {
            return None;
        }
        number = number * 10 + u128::from(d - b'0');
    }
    let total = number * size;
    Some(Duration::new(
        u64::try_from(total / 1_000_000_000).ok()?,
        (total % 1_000_000_000) as u32,
    ))
}

/// Format `timeout` as a `grpc-timeout` value.
///
/// Uses the finest unit that fits in 8 digits, rounding up so the budget is
/// never shortened. Larger budgets are clamped to `99999999H`.
pub(crate) fn format(timeout: Duration) -> String {
    let nanos = timeout.as_nanos();
    for (unit, size) in UNITS {
        let value = nanos.div_ceil(size);
        if value <= MAX_VALUE {
            let mut out = value.to_string();
            out.push(char::from(unit));
            return out;
        }
    }
    String::from("99999999H")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_unit() {
        assert_eq!(parse("5n"), Some(Duration::from_nanos(5)));
        assert_eq!(parse("5u"), Some(Duration::from_micros(5)));
        assert_eq!(parse("5m"), Some(Duration::from_millis(5)));
        assert_eq!(parse("5S"), Some(Duration::from_secs(5)));
        assert_eq!(parse("5M"), Some(Duration::from_secs(300)));
        assert_eq!(parse("5H"), Some(Duration::from_secs(18_000)));
    }

    #[test]
    fn parses_limits() {
        assert_eq!(parse("0n"), Some(Duration::ZERO));
        assert_eq!(parse("00000001S"), Some(Duration::from_secs(1)));
        assert_eq!(
            parse("99999999H"),
            Some(Duration::from_secs(99_999_999 * 3600))
        );
        assert_eq!(parse("99999999n"), Some(Duration::from_nanos(99_999_999)));
    }

    #[test]
    fn rejects_malformed_values() {
        for bad in [
            "",
            "S",
            "1",
            "123456789S",
            "1s",
            "1x",
            "-1S",
            "+1S",
            "1 S",
            " 1S",
            "1S ",
            "1.5S",
            "S1",
            "1SS",
            "\u{e6}",
            "1\u{e6}",
        ] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn formats_with_the_finest_unit_that_fits() {
        assert_eq!(format(Duration::ZERO), "0n");
        assert_eq!(format(Duration::from_nanos(99_999_999)), "99999999n");
        assert_eq!(format(Duration::from_nanos(100_000_000)), "100000u");
        assert_eq!(format(Duration::from_secs(1)), "1000000u");
        assert_eq!(format(Duration::from_secs(100)), "100000m");
        assert_eq!(format(Duration::from_secs(100_000)), "100000S");
        assert_eq!(format(Duration::from_secs(100_000_000)), "1666667M");
    }

    #[test]
    fn formatting_rounds_up_and_clamps() {
        assert_eq!(format(Duration::from_nanos(100_000_001)), "100001u");
        assert_eq!(
            format(Duration::from_secs(100_000) + Duration::from_nanos(1)),
            "100001S"
        );
        assert_eq!(format(Duration::MAX), "99999999H");
    }

    #[test]
    fn round_trips_without_shortening() {
        for nanos in [
            0u64,
            1,
            999,
            12_345_678,
            99_999_999,
            100_000_000,
            1_234_567_891,
            86_400_000_000_000,
            3_599_999_999_999_999,
        ] {
            let timeout = Duration::from_nanos(nanos);
            let back = parse(&format(timeout)).unwrap();
            assert!(back >= timeout, "{timeout:?} -> {back:?}");
            assert!(back - timeout <= timeout / 1000 + Duration::from_nanos(1000));
        }
    }
}
