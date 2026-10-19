use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::beacon::AdvertisementRecord;
use crate::endpoint::{
    hardware_manager_address, EndpointAddress, CONTROLLER_FAMILY, HARDWARE_MANAGER_FAMILY,
};
use crate::lanes::{DeviceDescriptor, DeviceRef};
use crate::{Error, Result};

pub const HARDWARE_PROFILE_ID: &str = "dev.deckr.profile.hardware.v1";
pub const HARDWARE_CLAIM_PROFILE_ID: &str = "dev.deckr.profile.hardware_claim.v1";
pub const HARDWARE_FEATURE_ID: &str = "dev.deckr.hardware";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProfileCapacity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_instances: Option<u64>,
    #[serde(default)]
    pub claimed_instances: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_instances: Option<u64>,
}

impl ProfileCapacity {
    pub fn validate(&self) -> Result<()> {
        if let (Some(total), Some(available)) = (self.total_instances, self.available_instances) {
            if available != total.saturating_sub(self.claimed_instances) {
                return Err(Error::Invalid(
                    "availableInstances must match totalInstances - claimedInstances".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HardwareAdvertisementDevice {
    #[serde(default)]
    pub capacity: ProfileCapacity,
    pub device_ref: DeviceRef,
    pub descriptor: DeviceDescriptor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HardwareBeaconPayload {
    #[serde(default = "hardware_profile_id")]
    pub profile: String,
    pub manager_id: String,
    pub manager_endpoint: EndpointAddress,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub devices: BTreeMap<String, HardwareAdvertisementDevice>,
}

impl HardwareBeaconPayload {
    pub fn from_value(value: Value) -> Result<Self> {
        let payload: Self = serde_json::from_value(value)?;
        payload.validate()?;
        Ok(payload)
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(serde_json::to_value(self)?)
    }

    pub fn validate(&self) -> Result<()> {
        if self.profile != HARDWARE_PROFILE_ID {
            return Err(Error::Invalid(format!(
                "hardware Beacon payload profile must be {HARDWARE_PROFILE_ID}"
            )));
        }
        require_text(&self.manager_id, "hardware profile field")?;
        require_text(&self.session_id, "hardware profile field")?;
        if self.manager_endpoint.as_str() != hardware_manager_address(&self.manager_id) {
            return Err(Error::Invalid(
                "managerEndpoint must equal hardware_manager:<managerId>".to_string(),
            ));
        }
        for (key, device) in &self.devices {
            device.capacity.validate()?;
            device.device_ref.validate()?;
            device.descriptor.validate()?;
            if device.device_ref.manager_id != self.manager_id {
                return Err(Error::Invalid(
                    "deviceRef.managerId must match managerId".to_string(),
                ));
            }
            if &device.device_ref.device_id != key {
                return Err(Error::Invalid(
                    "device map keys must match deviceRef.deviceId".to_string(),
                ));
            }
            if device.descriptor.device_id != device.device_ref.device_id {
                return Err(Error::Invalid(
                    "descriptor.deviceId must match deviceRef.deviceId".to_string(),
                ));
            }
            if device
                .device_ref
                .fingerprint
                .as_ref()
                .is_some_and(|fingerprint| fingerprint != &device.descriptor.fingerprint)
            {
                return Err(Error::Invalid(
                    "deviceRef fingerprint must match descriptor fingerprint".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HardwareClaimDevice {
    pub device_ref: DeviceRef,
    pub instance_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HardwareClaimTerms {
    #[serde(default = "hardware_claim_profile_id")]
    pub profile: String,
    pub claim_id: String,
    pub controller_endpoint: EndpointAddress,
    pub manager_endpoint: EndpointAddress,
    pub devices: Vec<HardwareClaimDevice>,
}

impl HardwareClaimTerms {
    pub fn from_value(value: Value) -> Result<Self> {
        let terms: Self = serde_json::from_value(value)?;
        terms.validate()?;
        Ok(terms)
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(serde_json::to_value(self)?)
    }

    pub fn validate(&self) -> Result<()> {
        if self.profile != HARDWARE_CLAIM_PROFILE_ID {
            return Err(Error::Invalid(format!(
                "hardware claim profile must be {HARDWARE_CLAIM_PROFILE_ID}"
            )));
        }
        require_text(&self.claim_id, "hardware claim field")?;
        if self.controller_endpoint.family() != CONTROLLER_FAMILY {
            return Err(Error::Invalid(
                "controllerEndpoint must use controller:<id>".to_string(),
            ));
        }
        if self.manager_endpoint.family() != HARDWARE_MANAGER_FAMILY {
            return Err(Error::Invalid(
                "managerEndpoint must use hardware_manager:<id>".to_string(),
            ));
        }
        if self.devices.is_empty() {
            return Err(Error::Invalid(
                "hardware claims require at least one device".to_string(),
            ));
        }
        let mut device_ids = BTreeSet::new();
        for device in &self.devices {
            if device.instance_count == 0 {
                return Err(Error::Invalid(
                    "instanceCount must be greater than zero".to_string(),
                ));
            }
            if device.device_ref.manager_id != self.manager_endpoint.endpoint_id() {
                return Err(Error::Invalid(
                    "deviceRef.managerId must match managerEndpoint".to_string(),
                ));
            }
            if !device_ids.insert(device.device_ref.device_id.clone()) {
                return Err(Error::Invalid(
                    "device ids in one claim must be unique".to_string(),
                ));
            }
        }
        Ok(())
    }
}

pub fn hardware_payload_from_advertisement(
    advertisement: &AdvertisementRecord,
) -> Result<HardwareBeaconPayload> {
    if advertisement.feature_id != HARDWARE_FEATURE_ID {
        return Err(Error::Invalid(
            "advertisement featureId is not dev.deckr.hardware".to_string(),
        ));
    }
    let Some(payload) = &advertisement.payload else {
        return Err(Error::Invalid(
            "hardware advertisement requires payload".to_string(),
        ));
    };
    let payload = HardwareBeaconPayload::from_value(payload.clone())?;
    if payload.session_id != advertisement.session_id {
        return Err(Error::Invalid(
            "hardware payload sessionId must match advertisement sessionId".to_string(),
        ));
    }
    if payload.manager_endpoint != advertisement.endpoint {
        return Err(Error::Invalid(
            "hardware payload managerEndpoint must match advertisement endpoint".to_string(),
        ));
    }
    if payload.manager_id != advertisement.endpoint.endpoint_id() {
        return Err(Error::Invalid(
            "hardware payload managerId must be the hardware-manager endpoint id".to_string(),
        ));
    }
    Ok(payload)
}

pub fn hardware_claim_conflicts(
    existing_claims: &[HardwareClaimTerms],
    proposed_claim: &HardwareClaimTerms,
) -> Vec<HardwareClaimTerms> {
    let proposed = hardware_claim_device_keys(proposed_claim);
    existing_claims
        .iter()
        .filter(|claim| claim.claim_id != proposed_claim.claim_id)
        .filter(|claim| !hardware_claim_device_keys(claim).is_disjoint(&proposed))
        .cloned()
        .collect()
}

fn hardware_claim_device_keys(claim: &HardwareClaimTerms) -> BTreeSet<(String, String)> {
    claim
        .devices
        .iter()
        .map(|device| {
            (
                device.device_ref.manager_id.clone(),
                device.device_ref.device_id.clone(),
            )
        })
        .collect()
}

fn require_text(value: &str, field_name: &str) -> Result<()> {
    if value.trim() != value || value.is_empty() {
        return Err(Error::Invalid(format!(
            "{field_name} must be non-empty with no leading or trailing whitespace"
        )));
    }
    Ok(())
}

fn hardware_profile_id() -> String {
    HARDWARE_PROFILE_ID.to_string()
}

fn hardware_claim_profile_id() -> String {
    HARDWARE_CLAIM_PROFILE_ID.to_string()
}
