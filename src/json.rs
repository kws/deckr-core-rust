//! JSON input checks shared by wire adapters, before typed validation.

use std::fmt;

use serde::de::{self, DeserializeSeed, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

use crate::{Error, Result};

/// Parse unique object members and preserve arbitrary-precision integer tokens.
/// Objects are decoded explicitly so Serde's private numeric marker is ordinary
/// application data, not an alternate representation of a JSON number.
pub fn parse(bytes: &[u8]) -> Result<Value> {
    let raw: &RawValue = serde_json::from_slice(bytes)?;
    let value = parse_raw(raw, 0)?;
    validate_numbers(&value)?;
    Ok(value)
}

fn parse_raw(raw: &RawValue, depth: usize) -> Result<Value> {
    if depth > 128 {
        return Err(Error::Invalid("JSON nesting exceeds 128 levels".into()));
    }
    match raw.get().as_bytes().first() {
        Some(b'{') => {
            let mut reader = serde_json::Deserializer::from_str(raw.get());
            let value = Object(depth).deserialize(&mut reader)?;
            reader.end()?;
            Ok(value)
        }
        Some(b'[') => {
            let elements: Vec<&RawValue> = serde_json::from_str(raw.get())?;
            elements
                .into_iter()
                .map(|element| parse_raw(element, depth + 1))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array)
        }
        _ => Ok(serde_json::from_str(raw.get())?),
    }
}
struct Object(usize);
impl<'de> DeserializeSeed<'de> for Object {
    type Value = Value;
    fn deserialize<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for Object {
    type Value = Value;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object with unique members")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> std::result::Result<Value, M::Error> {
        let mut values = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom("duplicate JSON object member"));
            }
            let raw: &RawValue = map.next_value()?;
            let value = parse_raw(raw, self.0 + 1).map_err(de::Error::custom)?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

pub(crate) fn deserialize_value<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Value, D::Error> {
    let raw = Box::<RawValue>::deserialize(deserializer)?;
    parse(raw.get().as_bytes()).map_err(de::Error::custom)
}
pub(crate) fn deserialize_optional_value<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Value>, D::Error> {
    Option::<Box<RawValue>>::deserialize(deserializer)?
        .map(|raw| parse(raw.get().as_bytes()).map_err(de::Error::custom))
        .transpose()
}
pub(crate) fn deserialize_object<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Map<String, Value>, D::Error> {
    match deserialize_value(deserializer)? {
        Value::Object(object) => Ok(object),
        _ => Err(de::Error::custom("expected a JSON object")),
    }
}
pub(crate) fn deserialize_ordered_object<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<std::collections::BTreeMap<String, Value>, D::Error> {
    deserialize_object(deserializer).map(|object| object.into_iter().collect())
}
pub(crate) fn deserialize_optional_object<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Map<String, Value>>, D::Error> {
    deserialize_optional_value(deserializer)?
        .map(|value| match value {
            Value::Object(object) => Ok(object),
            _ => Err(de::Error::custom("expected a JSON object")),
        })
        .transpose()
}

/// Integer tokens retain arbitrary precision; fractional/exponent tokens must
/// have a finite binary64 representation under the shared JSON input rules.
pub fn validate_numbers(value: &Value) -> Result<()> {
    match value {
        Value::Number(number) => {
            let text = number.to_string();
            if text.contains(['.', 'e', 'E']) && !text.parse::<f64>().is_ok_and(f64::is_finite) {
                return Err(Error::Invalid(
                    "JSON decimal is outside finite binary64 range".into(),
                ));
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_numbers(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_numbers(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}
