//! Helpers for downstream-selected compile-time geometry.
//!
//! Cargo exposes values declared under a consumer's `.cargo/config.toml`
//! `[env]` table to every crate compiled in that build graph. Semantic Orbit
//! crates use [`usize_from_env`] with `option_env!` so the selected geometry is
//! part of Cargo's compilation fingerprint without requiring a custom build
//! command.

/// Parse an optional decimal `usize` selected at compile time.
///
/// Underscores are accepted as visual separators. Invalid or overflowing
/// values fail constant evaluation and therefore stop the build.
pub const fn usize_from_env(value: Option<&str>, default: usize) -> usize {
    let Some(value) = value else {
        return default;
    };
    let bytes = value.as_bytes();
    assert!(
        !bytes.is_empty(),
        "Orbit compile-time value cannot be empty"
    );

    let mut parsed = 0_usize;
    let mut index = 0;
    let mut saw_digit = false;
    let mut previous_was_separator = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'_' {
            assert!(
                saw_digit && !previous_was_separator && index + 1 < bytes.len(),
                "Orbit compile-time value has an invalid separator"
            );
            previous_was_separator = true;
            index += 1;
            continue;
        }
        assert!(
            byte >= b'0' && byte <= b'9',
            "Orbit compile-time value must be a decimal usize"
        );
        let digit = (byte - b'0') as usize;
        assert!(
            parsed <= (usize::MAX - digit) / 10,
            "Orbit compile-time value overflows usize"
        );
        parsed = parsed * 10 + digit;
        saw_digit = true;
        previous_was_separator = false;
        index += 1;
    }
    assert!(
        !previous_was_separator,
        "Orbit compile-time value cannot end with a separator"
    );
    parsed
}

#[cfg(test)]
mod tests {
    use super::usize_from_env;

    #[test]
    fn parses_decimal_values_and_separators() {
        assert_eq!(usize_from_env(None, 512), 512);
        assert_eq!(usize_from_env(Some("1024"), 0), 1_024);
        assert_eq!(usize_from_env(Some("16_384"), 0), 16_384);
    }

    #[test]
    #[should_panic(expected = "decimal usize")]
    fn rejects_non_decimal_values() {
        let _ = usize_from_env(Some("1KiB"), 0);
    }

    #[test]
    #[should_panic(expected = "invalid separator")]
    fn rejects_invalid_separators() {
        let _ = usize_from_env(Some("_512"), 0);
    }
}
