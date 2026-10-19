use std::fmt;
use std::str::FromStr;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Error, Result};

pub const CONTROLLER_FAMILY: &str = "controller";
pub const HARDWARE_MANAGER_FAMILY: &str = "hardware_manager";
pub const ACTION_PROVIDER_FAMILY: &str = "action_provider";
pub const SERVICE_FAMILY: &str = "service";

const CORE_ENDPOINT_FAMILIES: &[&str] = &[
    ACTION_PROVIDER_FAMILY,
    CONTROLLER_FAMILY,
    HARDWARE_MANAGER_FAMILY,
    SERVICE_FAMILY,
];
const RESERVED_ACTION_PROVIDER_INSTANCE_IDS: &[&str] = &["dev.deckr.controller.builtin"];

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EndpointAddress {
    address: String,
    family: String,
    endpoint_id: String,
}

impl EndpointAddress {
    pub fn parse(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        if value.trim() != value {
            return Err(Error::Invalid(
                "endpoint address must not contain leading or trailing whitespace".to_string(),
            ));
        }
        let Some((family, endpoint_id)) = value.split_once(':') else {
            return Err(Error::Invalid(
                "endpoint address must use <family>:<id>".to_string(),
            ));
        };
        if !CORE_ENDPOINT_FAMILIES.contains(&family) {
            return Err(Error::Invalid(format!(
                "unsupported endpoint family {family:?}"
            )));
        }
        if endpoint_id.is_empty() || endpoint_id.trim() != endpoint_id || endpoint_id.contains(':')
        {
            return Err(Error::Invalid(
                "endpoint id must be non-empty, trimmed, and must not contain ':'".to_string(),
            ));
        }
        if family == ACTION_PROVIDER_FAMILY
            && (!is_provider_instance_id(endpoint_id)
                || RESERVED_ACTION_PROVIDER_INSTANCE_IDS.contains(&endpoint_id))
        {
            return Err(Error::Invalid(format!(
                "invalid action provider endpoint id {endpoint_id:?}"
            )));
        }
        Ok(Self {
            address: value.to_string(),
            family: family.to_string(),
            endpoint_id: endpoint_id.to_string(),
        })
    }

    pub fn family(&self) -> &str {
        &self.family
    }

    pub fn endpoint_id(&self) -> &str {
        &self.endpoint_id
    }

    pub fn as_str(&self) -> &str {
        &self.address
    }
}

impl fmt::Debug for EndpointAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("EndpointAddress")
            .field(&self.address)
            .finish()
    }
}

impl fmt::Display for EndpointAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.address)
    }
}

impl FromStr for EndpointAddress {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for EndpointAddress {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

impl Serialize for EndpointAddress {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.address)
    }
}

impl<'de> Deserialize<'de> for EndpointAddress {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(D::Error::custom)
    }
}

pub fn controller_address(controller_id: &str) -> String {
    format!("{CONTROLLER_FAMILY}:{controller_id}")
}

pub fn hardware_manager_address(manager_id: &str) -> String {
    format!("{HARDWARE_MANAGER_FAMILY}:{manager_id}")
}

pub fn action_provider_address(provider_instance_id: &str) -> String {
    format!("{ACTION_PROVIDER_FAMILY}:{provider_instance_id}")
}

pub fn service_address(service_id: &str) -> String {
    format!("{SERVICE_FAMILY}:{service_id}")
}

fn is_provider_instance_id(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphanumeric())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_core_families() {
        assert_eq!(
            EndpointAddress::parse("hardware_manager:main")
                .unwrap()
                .endpoint_id(),
            "main"
        );
        assert!(EndpointAddress::parse("driver:main").is_err());
        assert!(EndpointAddress::parse("controller:").is_err());
        assert!(EndpointAddress::parse(" controller:main").is_err());
        assert!(EndpointAddress::parse("action_provider:dev.deckr.controller.builtin").is_err());
    }
}
