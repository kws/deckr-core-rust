use std::io;

use ring::digest::{digest, SHA256};
use serde::Serialize;
use serde_json::ser::Formatter;
use serde_json::{Map, Value};

use crate::Result;

pub fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)?;
    canonical_json_bytes_value(&value)
}

pub fn canonical_json_bytes_value(value: &Value) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut serializer = serde_json::Serializer::with_formatter(&mut bytes, TermsFormatter);
    sort_json_value(value.clone()).serialize(&mut serializer)?;
    Ok(bytes)
}

// Preserve integer tokens exactly and apply the upstream v1 binary64 formatting
// rules only to decimal/exponent tokens. Arbitrary-precision JSON parsing keeps
// that distinction and avoids rounding large integers before hashing.
struct TermsFormatter;

impl Formatter for TermsFormatter {
    fn write_number_str<W>(&mut self, writer: &mut W, token: &str) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        if !token.contains(['.', 'e', 'E']) {
            return writer.write_all(if token == "-0" {
                b"0"
            } else {
                token.as_bytes()
            });
        }
        let value = token
            .parse::<f64>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if !value.is_finite() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Concord terms require finite binary64 values",
            ));
        }
        let scientific = format!("{value:e}");
        let (significand, exponent) = scientific.split_once('e').expect("scientific float");
        let exponent: i32 = exponent.parse().expect("decimal exponent");
        let rendered = if value != 0.0 && !(-4..16).contains(&exponent) {
            format!("{significand}e{exponent:+03}")
        } else {
            let mut fixed = value.to_string();
            if !fixed.contains('.') {
                fixed.push_str(".0");
            }
            fixed
        };
        writer.write_all(rendered.as_bytes())
    }
}

pub fn canonical_json_hash<T: Serialize>(value: &T) -> Result<String> {
    Ok(terms_hash_bytes(&canonical_json_bytes(value)?))
}

pub fn canonical_json_hash_value(value: &Value) -> Result<String> {
    Ok(terms_hash_bytes(&canonical_json_bytes_value(value)?))
}

fn terms_hash_bytes(bytes: &[u8]) -> String {
    let hash = digest(&SHA256, bytes);
    let mut output = String::from("sha256:");
    for byte in hash.as_ref() {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn sort_json_value(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(sort_json_value).collect()),
        Value::Object(map) => {
            let mut items = map.into_iter().collect::<Vec<_>>();
            items.sort_by(|left, right| left.0.cmp(&right.0));
            let mut sorted = Map::new();
            for (key, value) in items {
                sorted.insert(key, sort_json_value(value));
            }
            Value::Object(sorted)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retains_integer_precision_and_decimal_token_identity() {
        let value: Value = serde_json::from_str(
            r#"{"integer":-0,"decimal":2e0,"large":18446744073709551616001,"zero":-0.0}"#,
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(canonical_json_bytes_value(&value).unwrap()).unwrap(),
            r#"{"decimal":2.0,"integer":0,"large":18446744073709551616001,"zero":-0.0}"#
        );
    }

    #[test]
    fn formats_binary64_values_at_notation_boundaries() {
        for (input, expected) in [
            ("-0.0001", "-0.0001"),
            ("-0.00001", "-1e-05"),
            ("2e15", "2000000000000000.0"),
            ("2e16", "2e+16"),
            ("5e-324", "5e-324"),
            ("1.7976931348623157e308", "1.7976931348623157e+308"),
        ] {
            let value: Value = serde_json::from_str(input).unwrap();
            assert_eq!(
                canonical_json_bytes_value(&value).unwrap(),
                expected.as_bytes()
            );
        }
    }

    #[test]
    fn rejects_decimal_values_outside_finite_binary64_range() {
        let value: Value = serde_json::from_str(r#"{"n":1e309}"#).unwrap();
        assert!(canonical_json_bytes_value(&value).is_err());
    }
}
