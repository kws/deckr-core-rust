use std::collections::{BTreeMap, BTreeSet};

use crate::authority::ContractPointer;
use crate::concord::{ConcordManagedContract, ContractValidityStatus};
use crate::endpoint::EndpointAddress;
use crate::lanes::{DeviceDescriptor, DeviceRef};
use crate::profiles::hardware::{
    HardwareAdvertisementDevice, HardwareBeaconPayload, HardwareClaimTerms, ProfileCapacity,
};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
struct HardwareClaimRoute {
    controller_endpoint: EndpointAddress,
    controller_session_id: String,
    contract: ContractPointer,
    contract_key: String,
    claim_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HardwareClaimRecipient<'a> {
    endpoint: &'a EndpointAddress,
    session_id: &'a str,
    contract: &'a ContractPointer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HardwareClaimReconcile {
    reset_devices: BTreeSet<String>,
}

#[derive(Debug, Clone, Default)]
struct HardwareClaimRouting {
    claims: BTreeMap<String, HardwareClaimRoute>,
}

impl HardwareClaimRouting {
    fn claim_recipient(&self, device_id: &str) -> Option<HardwareClaimRecipient<'_>> {
        let claim = self.claims.get(device_id)?;
        Some(HardwareClaimRecipient {
            endpoint: &claim.controller_endpoint,
            session_id: &claim.controller_session_id,
            contract: &claim.contract,
        })
    }

    fn claimed_device_ids(&self) -> BTreeSet<String> {
        self.claims.keys().cloned().collect()
    }

    fn remove_device(&mut self, device_id: &str) {
        self.claims.remove(device_id);
    }

    fn reconcile_claims(
        &mut self,
        managed_contracts: &[ConcordManagedContract],
        manager_endpoint: &EndpointAddress,
        known_devices: &BTreeSet<String>,
    ) -> HardwareClaimReconcile {
        let mut next_claims = BTreeMap::<String, HardwareClaimRoute>::new();
        let mut invalid_claim_devices = BTreeSet::<String>::new();
        let mut ordered = managed_contracts.iter().collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            let left_existing = self.route_for_contract(&left.contract.key).is_none();
            let right_existing = self.route_for_contract(&right.contract.key).is_none();
            left_existing
                .cmp(&right_existing)
                .then_with(|| left.contract.key.cmp(&right.contract.key))
        });

        for managed in ordered {
            if !managed.contract.participants.contains(manager_endpoint) {
                continue;
            }
            let Some(terms_value) = managed.record.terms.clone() else {
                continue;
            };
            let terms = match HardwareClaimTerms::from_value(terms_value) {
                Ok(terms) => terms,
                Err(_) => continue,
            };
            if &terms.manager_endpoint != manager_endpoint {
                continue;
            }
            if managed.validity.status != ContractValidityStatus::Valid {
                for device in &terms.devices {
                    let device_id = &device.device_ref.device_id;
                    if known_devices.contains(device_id) {
                        invalid_claim_devices.insert(device_id.clone());
                    }
                }
                continue;
            }
            if !managed
                .contract
                .participants
                .contains(&terms.controller_endpoint)
            {
                continue;
            }
            let controller_key = terms.controller_endpoint.to_string();
            let Some(controller_token) = managed.validity.tokens.get(&controller_key) else {
                continue;
            };
            for device in &terms.devices {
                let device_id = &device.device_ref.device_id;
                if !known_devices.contains(device_id) || next_claims.contains_key(device_id) {
                    continue;
                }
                next_claims.insert(
                    device_id.clone(),
                    HardwareClaimRoute {
                        controller_endpoint: terms.controller_endpoint.clone(),
                        controller_session_id: controller_token.session_id.clone(),
                        contract: ContractPointer {
                            contract_id: managed.contract.contract_id.clone(),
                            generation: managed.contract.generation,
                        },
                        contract_key: managed.contract.key.clone(),
                        claim_id: terms.claim_id.clone(),
                    },
                );
            }
        }

        invalid_claim_devices.retain(|device_id| !next_claims.contains_key(device_id));
        let reset_devices =
            self.devices_to_reset_for_snapshot(&next_claims, &invalid_claim_devices);
        self.claims = next_claims;
        HardwareClaimReconcile { reset_devices }
    }

    fn route_for_contract(&self, contract_key: &str) -> Option<&HardwareClaimRoute> {
        self.claims
            .values()
            .find(|claim| claim.contract_key == contract_key)
    }

    fn devices_to_reset_for_snapshot(
        &self,
        next_claims: &BTreeMap<String, HardwareClaimRoute>,
        invalid_claim_devices: &BTreeSet<String>,
    ) -> BTreeSet<String> {
        let mut devices_to_reset = invalid_claim_devices.clone();
        for (device_id, old_claim) in &self.claims {
            let Some(next_claim) = next_claims.get(device_id) else {
                devices_to_reset.insert(device_id.clone());
                continue;
            };
            if claim_route_identity(old_claim) != claim_route_identity(next_claim) {
                devices_to_reset.insert(device_id.clone());
            }
        }
        devices_to_reset
    }
}

fn hardware_beacon_payload(
    manager_id: &str,
    manager_endpoint: EndpointAddress,
    session_id: &str,
    labels: BTreeMap<String, String>,
    devices: &BTreeMap<String, DeviceDescriptor>,
    claimed_devices: &BTreeSet<String>,
) -> Result<HardwareBeaconPayload> {
    let payload = HardwareBeaconPayload {
        profile: crate::profiles::hardware::HARDWARE_PROFILE_ID.to_string(),
        manager_id: manager_id.to_string(),
        manager_endpoint,
        session_id: session_id.to_string(),
        labels,
        devices: devices
            .iter()
            .map(|(device_id, descriptor)| {
                let claimed = claimed_devices.contains(device_id);
                (
                    device_id.clone(),
                    HardwareAdvertisementDevice {
                        capacity: ProfileCapacity {
                            total_instances: Some(1),
                            claimed_instances: if claimed { 1 } else { 0 },
                            available_instances: Some(if claimed { 0 } else { 1 }),
                        },
                        device_ref: DeviceRef {
                            manager_id: manager_id.to_string(),
                            device_id: device_id.clone(),
                            fingerprint: Some(descriptor.fingerprint.clone()),
                        },
                        descriptor: descriptor.clone(),
                    },
                )
            })
            .collect(),
    };
    payload.validate()?;
    Ok(payload)
}

fn claim_route_identity(
    claim: &HardwareClaimRoute,
) -> (&EndpointAddress, &str, &str, u64, &str, &str) {
    (
        &claim.controller_endpoint,
        &claim.controller_session_id,
        &claim.contract.contract_id,
        claim.contract.generation,
        &claim.contract_key,
        &claim.claim_id,
    )
}

pub mod runtime {
    use std::collections::{BTreeMap, BTreeSet};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use futures_core::Stream;
    use futures_util::StreamExt;
    use serde_json::Value;
    use tokio::sync::Mutex;
    use tokio::task::JoinSet;
    use tokio::time;
    use uuid::Uuid;

    use crate::authority::ContractPointer;
    use crate::beacon::{AdvertisementHandle, BeaconAdvertiser};
    use crate::concord::{
        ConcordContractNotification, ConcordCoordinator, ConcordManagedContract,
        ConcordParticipantManager, ContractHandle, ContractRecord,
    };
    use crate::endpoint::{hardware_manager_address, EndpointAddress};
    use crate::lanes::{DeckrMessage, DeviceDescriptor, HardwareMessageBody};
    use crate::profiles::hardware::{
        HardwareClaimTerms, HARDWARE_CLAIM_PROFILE_ID, HARDWARE_FEATURE_ID,
    };
    use crate::state::{MaterializedStateStore, StateMaintenancePolicy, StateStore};
    use crate::{Error, Result};

    use super::{hardware_beacon_payload, HardwareClaimReconcile, HardwareClaimRouting};

    #[derive(Debug, Clone, PartialEq)]
    enum HardwareCommandDecision {
        Authorized,
        Rejected {
            reason: String,
            reply: Option<Box<DeckrMessage>>,
            refresh_claims: bool,
        },
        Ignored,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum HardwareCommandOutcome {
        Handled,
        Unsupported,
        Stale,
        Rejected,
    }

    impl HardwareCommandOutcome {
        fn rejection(&self) -> Option<&'static str> {
            match self {
                Self::Handled => None,
                Self::Unsupported => Some("unsupported"),
                Self::Stale => Some("stale"),
                Self::Rejected => Some("rejected"),
            }
        }
    }

    pub type HardwareCommandFuture<'a> =
        Pin<Box<dyn Future<Output = Result<HardwareCommandOutcome>> + Send + 'a>>;

    pub trait HardwareCommandHandler: Send + Sync + 'static {
        fn handle_hardware_command<'a>(
            &'a self,
            message: DeckrMessage,
        ) -> HardwareCommandFuture<'a>;
    }

    impl<F, Fut> HardwareCommandHandler for F
    where
        F: Fn(DeckrMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<HardwareCommandOutcome>> + Send + 'static,
    {
        fn handle_hardware_command<'a>(
            &'a self,
            message: DeckrMessage,
        ) -> HardwareCommandFuture<'a> {
            Box::pin((self)(message))
        }
    }

    pub type HardwareResetFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    pub trait HardwareResetHandler: Send + Sync + 'static {
        fn reset_hardware_device<'a>(&'a self, device_id: &'a str) -> HardwareResetFuture<'a>;
    }

    impl<F, Fut> HardwareResetHandler for F
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        fn reset_hardware_device<'a>(&'a self, device_id: &'a str) -> HardwareResetFuture<'a> {
            Box::pin((self)(device_id.to_string()))
        }
    }

    pub type HardwareMessageStream =
        Pin<Box<dyn Stream<Item = Result<DeckrMessage>> + Send + 'static>>;

    pub trait HardwareLaneTransport: Clone + Send + Sync + 'static {
        fn publish_hardware_message(
            &self,
            message: DeckrMessage,
        ) -> impl Future<Output = Result<()>> + Send + '_;

        fn subscribe_hardware_messages<'a>(
            &'a self,
            endpoint: &'a EndpointAddress,
        ) -> impl Future<Output = Result<HardwareMessageStream>> + Send + 'a;
    }

    #[derive(Debug, Clone)]
    struct HardwareManagerState {
        manager_id: String,
        endpoint: EndpointAddress,
        session_id: String,
        labels: BTreeMap<String, String>,
        devices: BTreeMap<String, DeviceDescriptor>,
        routing: HardwareClaimRouting,
    }

    impl HardwareManagerState {
        fn new(manager_id: impl Into<String>, session_id: impl Into<String>) -> Result<Self> {
            let manager_id = manager_id.into();
            Self::with_endpoint(
                manager_id.clone(),
                EndpointAddress::parse(hardware_manager_address(&manager_id))?,
                session_id,
            )
        }

        fn with_endpoint(
            manager_id: impl Into<String>,
            endpoint: EndpointAddress,
            session_id: impl Into<String>,
        ) -> Result<Self> {
            let manager_id = manager_id.into();
            let session_id = session_id.into();
            if endpoint.family() != crate::endpoint::HARDWARE_MANAGER_FAMILY {
                return Err(Error::Invalid(
                    "hardware manager runtime endpoint must be hardware_manager".to_string(),
                ));
            }
            if endpoint.endpoint_id() != manager_id {
                return Err(Error::Invalid(
                    "hardware manager endpoint id must match manager id".to_string(),
                ));
            }
            if session_id.trim() != session_id || session_id.is_empty() {
                return Err(Error::Invalid(
                    "hardware manager session id must be non-empty with no leading or trailing whitespace"
                        .to_string(),
                ));
            }
            Ok(Self {
                manager_id,
                endpoint,
                session_id,
                labels: BTreeMap::new(),
                devices: BTreeMap::new(),
                routing: HardwareClaimRouting::default(),
            })
        }

        fn endpoint(&self) -> &EndpointAddress {
            &self.endpoint
        }

        fn session_id(&self) -> &str {
            &self.session_id
        }

        fn devices(&self) -> &BTreeMap<String, DeviceDescriptor> {
            &self.devices
        }

        fn set_labels(&mut self, labels: BTreeMap<String, String>) {
            self.labels = labels;
        }

        fn advertisement_payload(
            &self,
        ) -> Result<crate::profiles::hardware::HardwareBeaconPayload> {
            hardware_beacon_payload(
                &self.manager_id,
                self.endpoint.clone(),
                &self.session_id,
                self.labels.clone(),
                &self.devices,
                &self.routing.claimed_device_ids(),
            )
        }

        fn set_device(&mut self, descriptor: DeviceDescriptor) -> Result<()> {
            descriptor.validate()?;
            self.devices
                .insert(descriptor.device_id.clone(), descriptor);
            Ok(())
        }

        fn remove_device(&mut self, device_id: &str) {
            self.devices.remove(device_id);
            self.routing.remove_device(device_id);
        }

        fn accept_current_hardware_claim(
            &self,
            contract: &ContractHandle,
            record: &ContractRecord,
        ) -> Result<bool> {
            accept_current_hardware_claim(
                contract,
                record,
                &self.endpoint,
                &self.manager_id,
                &self.devices,
            )
        }

        fn reconcile_claims(
            &mut self,
            managed_contracts: &[ConcordManagedContract],
        ) -> HardwareClaimReconcile {
            let known_devices = self.devices.keys().cloned().collect::<BTreeSet<_>>();
            self.routing
                .reconcile_claims(managed_contracts, &self.endpoint, &known_devices)
        }

        fn route_hardware_message(
            &self,
            body: HardwareMessageBody,
        ) -> Result<Option<DeckrMessage>> {
            if !matches!(
                body,
                HardwareMessageBody::ControlInput { .. }
                    | HardwareMessageBody::CapabilityStateChanged { .. }
            ) {
                return Ok(None);
            }
            let device_ref = body.device_ref().clone();
            if device_ref.manager_id != self.manager_id {
                return Ok(None);
            }
            let Some(recipient) = self.routing.claim_recipient(&device_ref.device_id) else {
                return Ok(None);
            };
            DeckrMessage::hardware_input_to(
                &self.manager_id,
                &self.session_id,
                &device_ref.device_id,
                recipient.endpoint.as_str(),
                recipient.session_id,
                recipient.contract.clone(),
                body,
            )
            .map(Some)
        }

        fn authorize_command(&self, envelope: DeckrMessage) -> Result<HardwareCommandDecision> {
            if envelope.is_expired() {
                return Ok(HardwareCommandDecision::Rejected {
                    reason: "expired".to_string(),
                    reply: self.command_rejection_reply(&envelope, "expired")?,
                    refresh_claims: false,
                });
            }
            if envelope.contract.is_none() {
                if envelope.recipient_endpoint() != Some(self.endpoint.as_str())
                    || envelope
                        .recipient_session_id
                        .as_deref()
                        .is_some_and(|session_id| session_id != self.session_id)
                {
                    return Ok(HardwareCommandDecision::Ignored);
                }
                return Ok(HardwareCommandDecision::Rejected {
                    reason: "unauthorized".to_string(),
                    reply: None,
                    refresh_claims: false,
                });
            }
            if !envelope.is_directly_deliverable_to(&self.endpoint, &self.session_id)? {
                return Ok(HardwareCommandDecision::Ignored);
            }
            let body = match envelope.hardware_body() {
                Ok(body) => body,
                Err(error) => {
                    let _ = error;
                    return Ok(HardwareCommandDecision::Rejected {
                        reason: "malformed".to_string(),
                        reply: None,
                        refresh_claims: false,
                    });
                }
            };
            if !body.is_command() {
                return Ok(HardwareCommandDecision::Ignored);
            }
            let device_id = body.device_ref().device_id.clone();
            if body.device_ref().manager_id != self.manager_id
                || envelope.subject.manager_id() != Some(self.manager_id.as_str())
            {
                return Ok(HardwareCommandDecision::Rejected {
                    reason: "stale".to_string(),
                    reply: self.command_rejection_reply(&envelope, "stale")?,
                    refresh_claims: false,
                });
            }
            if !self.devices.contains_key(&device_id) {
                return Ok(HardwareCommandDecision::Rejected {
                    reason: "stale".to_string(),
                    reply: self.command_rejection_reply(&envelope, "stale")?,
                    refresh_claims: false,
                });
            }
            let Some(recipient) = self.routing.claim_recipient(&device_id) else {
                return Ok(HardwareCommandDecision::Rejected {
                    reason: "unauthorized".to_string(),
                    reply: self.command_rejection_reply(&envelope, "unauthorized")?,
                    refresh_claims: true,
                });
            };
            if recipient.endpoint.as_str() != envelope.sender
                || recipient.session_id != envelope.sender_session_id
                || envelope.contract.as_ref() != Some(recipient.contract)
            {
                return Ok(HardwareCommandDecision::Rejected {
                    reason: "unauthorized".to_string(),
                    reply: self.command_rejection_reply(&envelope, "unauthorized")?,
                    refresh_claims: false,
                });
            }
            EndpointAddress::parse(&envelope.sender)?;
            Ok(HardwareCommandDecision::Authorized)
        }

        fn rejection_reply_to(
            &self,
            recipient_endpoint: &EndpointAddress,
            recipient_session_id: &str,
            contract: ContractPointer,
            body: HardwareMessageBody,
            reason: &str,
        ) -> Result<Option<DeckrMessage>> {
            let reply_body = match body {
                HardwareMessageBody::ControlCommand {
                    device_ref,
                    control_id,
                    capability_id,
                    command_type,
                    ..
                } => HardwareMessageBody::CommandRejected {
                    device_ref,
                    control_id,
                    capability_id,
                    command_type,
                    reason: reason.to_string(),
                    message: Some(format!("Hardware command {reason}")),
                },
                HardwareMessageBody::CapabilityStateRequest {
                    device_ref,
                    control_id,
                    capability_id,
                    state_type,
                    ..
                } => HardwareMessageBody::CapabilityStateReply {
                    device_ref,
                    control_id,
                    capability_id,
                    state_type,
                    status: match reason {
                        "unsupported" => "unsupported",
                        _ => "rejected",
                    }
                    .to_string(),
                    value: None,
                    error: Some(format!("Hardware state request {reason}")),
                },
                _ => return Ok(None),
            };
            let device_id = reply_body.device_ref().device_id.clone();
            DeckrMessage::hardware_input_to(
                &self.manager_id,
                &self.session_id,
                &device_id,
                recipient_endpoint.as_str(),
                recipient_session_id,
                contract,
                reply_body,
            )
            .map(Some)
        }

        fn command_rejection_reply(
            &self,
            envelope: &DeckrMessage,
            reason: &str,
        ) -> Result<Option<Box<DeckrMessage>>> {
            let Some(contract) = envelope.contract.clone() else {
                return Ok(None);
            };
            self.rejection_reply_to(
                &EndpointAddress::parse(&envelope.sender)?,
                &envelope.sender_session_id,
                contract,
                envelope.hardware_body()?,
                reason,
            )
            .map(|reply| reply.map(Box::new))
        }
    }

    fn accept_current_hardware_claim(
        contract: &ContractHandle,
        record: &ContractRecord,
        manager_endpoint: &EndpointAddress,
        manager_id: &str,
        current_devices: &BTreeMap<String, DeviceDescriptor>,
    ) -> Result<bool> {
        if record.profile.as_deref() != Some(HARDWARE_CLAIM_PROFILE_ID) {
            return Ok(false);
        }
        if !contract.participants.contains(manager_endpoint) {
            return Ok(false);
        }
        let Some(terms_value) = record.terms.clone() else {
            return Ok(false);
        };
        let Ok(terms) = HardwareClaimTerms::from_value(terms_value) else {
            return Ok(false);
        };
        if &terms.manager_endpoint != manager_endpoint {
            return Ok(false);
        }
        if terms.manager_endpoint.endpoint_id() != manager_id {
            return Ok(false);
        }
        if !contract.participants.contains(&terms.controller_endpoint) {
            return Ok(false);
        }
        for device in &terms.devices {
            let device_ref = &device.device_ref;
            if device_ref.manager_id != manager_id {
                return Ok(false);
            }
            let Some(current) = current_devices.get(&device_ref.device_id) else {
                return Ok(false);
            };
            if device_ref
                .fingerprint
                .as_ref()
                .is_some_and(|fingerprint| fingerprint != &current.fingerprint)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub struct HardwareManagerRuntimeSpec<B, C, T, L>
    where
        B: StateStore,
        C: StateStore,
        T: StateStore,
        L: HardwareLaneTransport,
    {
        pub manager_id: String,
        pub session_id: String,
        pub labels: BTreeMap<String, String>,
        pub beacon_state: B,
        pub concord_contract_state: C,
        pub concord_token_state: T,
        pub lane: L,
        pub maintenance_policy: StateMaintenancePolicy,
        pub command_handler: Arc<dyn HardwareCommandHandler>,
        pub reset_handler: Option<Arc<dyn HardwareResetHandler>>,
    }

    type MaterializedConcord<C, T> =
        ConcordCoordinator<MaterializedStateStore<C>, MaterializedStateStore<T>>;
    type MaterializedParticipantManager<C, T> =
        ConcordParticipantManager<MaterializedStateStore<C>, MaterializedStateStore<T>>;

    pub struct HardwareManagerRuntime<B, C, T, L>
    where
        B: StateStore,
        C: StateStore,
        T: StateStore,
        L: HardwareLaneTransport,
    {
        inner: Arc<Mutex<HardwareManagerRuntimeInner<B, C, T, L>>>,
    }

    impl<B, C, T, L> Clone for HardwareManagerRuntime<B, C, T, L>
    where
        B: StateStore,
        C: StateStore,
        T: StateStore,
        L: HardwareLaneTransport,
    {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }

    struct HardwareManagerRuntimeInner<B, C, T, L>
    where
        B: StateStore,
        C: StateStore,
        T: StateStore,
        L: HardwareLaneTransport,
    {
        state: HardwareManagerState,
        beacon_state: B,
        advertisement_id: String,
        advertisement_handle: Option<AdvertisementHandle>,
        advertised_payload: Option<Value>,
        concord: MaterializedConcord<C, T>,
        claim_manager: MaterializedParticipantManager<C, T>,
        lane: L,
        maintenance_policy: StateMaintenancePolicy,
        command_handler: Arc<dyn HardwareCommandHandler>,
        reset_handler: Option<Arc<dyn HardwareResetHandler>>,
        closed: bool,
    }

    impl<B, C, T, L> HardwareManagerRuntime<B, C, T, L>
    where
        B: StateStore,
        C: StateStore,
        T: StateStore,
        L: HardwareLaneTransport,
    {
        pub async fn new(spec: HardwareManagerRuntimeSpec<B, C, T, L>) -> Result<Self> {
            let mut state = HardwareManagerState::new(&spec.manager_id, &spec.session_id)?;
            state.set_labels(spec.labels);
            let concord = ConcordCoordinator::new(
                MaterializedStateStore::start(
                    spec.concord_contract_state,
                    crate::keys::concord_contracts_prefix(),
                )
                .await?,
                MaterializedStateStore::start(
                    spec.concord_token_state,
                    crate::keys::concord_contracts_prefix(),
                )
                .await?,
            );
            let claim_manager = ConcordParticipantManager::new(
                concord.clone(),
                state.endpoint().clone(),
                state.session_id().to_string(),
            )?
            .profile(HARDWARE_CLAIM_PROFILE_ID.to_string())
            .token_refresh_interval(spec.maintenance_policy.concord_token_refresh_interval);
            Ok(Self {
                inner: Arc::new(Mutex::new(HardwareManagerRuntimeInner {
                    state,
                    beacon_state: spec.beacon_state,
                    advertisement_id: format!("hardware-{}", Uuid::new_v4()),
                    advertisement_handle: None,
                    advertised_payload: None,
                    concord,
                    claim_manager,
                    lane: spec.lane,
                    maintenance_policy: spec.maintenance_policy,
                    command_handler: spec.command_handler,
                    reset_handler: spec.reset_handler,
                    closed: false,
                })),
            })
        }

        pub async fn start(&self, tasks: &mut JoinSet<Result<()>>) -> Result<()> {
            {
                let inner = self.inner.lock().await;
                inner.cleanup_stale_advertisement().await?;
            }
            self.reconcile_claims("startup").await?;
            {
                let mut inner = self.inner.lock().await;
                inner.publish_advertisement_if_changed(true).await?;
            }

            let runtime = self.clone();
            tasks.spawn(async move { runtime.hardware_command_loop().await });
            let runtime = self.clone();
            tasks.spawn(async move { runtime.beacon_refresh_loop().await });
            let runtime = self.clone();
            tasks.spawn(async move { runtime.concord_watch_loop().await });
            let runtime = self.clone();
            tasks.spawn(async move { runtime.concord_token_refresh_loop().await });
            let runtime = self.clone();
            tasks.spawn(async move { runtime.periodic_reconcile_loop().await });
            Ok(())
        }

        pub async fn stop(&self) -> Result<()> {
            let mut inner = self.inner.lock().await;
            inner.closed = true;
            let managed_keys = inner
                .claim_manager
                .managed_contracts()
                .into_iter()
                .map(|managed| managed.contract.key)
                .collect::<Vec<_>>();
            for key in managed_keys {
                inner.claim_manager.release_withdraw(&key).await?;
            }
            inner.state.routing = HardwareClaimRouting::default();
            inner.withdraw_advertisement().await
        }

        pub async fn set_device(&self, descriptor: DeviceDescriptor) -> Result<()> {
            let device_id = descriptor.device_id.clone();
            let replaced_claim_keys = {
                let mut inner = self.inner.lock().await;
                let replaced = inner
                    .state
                    .devices()
                    .get(&device_id)
                    .is_some_and(|current| current.fingerprint != descriptor.fingerprint);
                let replaced_claim_keys = if replaced {
                    inner.claim_keys_for_device(&device_id)
                } else {
                    BTreeSet::new()
                };
                inner.state.set_device(descriptor)?;
                replaced_claim_keys
            };
            self.cancel_contract_keys(
                replaced_claim_keys,
                &format!("hardware device {device_id} replaced"),
            )
            .await?;
            self.reconcile_claims("device inventory").await?;
            Ok(())
        }

        pub async fn remove_device(&self, device_id: &str, reason: &str) -> Result<()> {
            let claim_keys = {
                let mut inner = self.inner.lock().await;
                let claim_keys = inner.claim_keys_for_device(device_id);
                inner.state.remove_device(device_id);
                inner.publish_advertisement_if_changed(false).await?;
                claim_keys
            };
            self.cancel_contract_keys(claim_keys, &format!("hardware device {device_id} {reason}"))
                .await?;
            self.reconcile_claims("device inventory").await?;
            Ok(())
        }

        pub async fn handle_hardware_message(&self, body: HardwareMessageBody) -> Result<()> {
            let (message, lane) = {
                let inner = self.inner.lock().await;
                (
                    inner.state.route_hardware_message(body)?,
                    inner.lane.clone(),
                )
            };
            if let Some(message) = message {
                lane.publish_hardware_message(message).await?;
            }
            Ok(())
        }

        async fn handle_command(&self, envelope: DeckrMessage) -> Result<()> {
            let mut decision = {
                let inner = self.inner.lock().await;
                inner.state.authorize_command(envelope.clone())?
            };
            if matches!(
                decision,
                HardwareCommandDecision::Rejected {
                    refresh_claims: true,
                    ..
                }
            ) {
                self.reconcile_claims("command authorization").await?;
                decision = {
                    let inner = self.inner.lock().await;
                    inner.state.authorize_command(envelope.clone())?
                };
            }

            match decision {
                HardwareCommandDecision::Authorized => {
                    let handler = {
                        let inner = self.inner.lock().await;
                        inner.command_handler.clone()
                    };
                    let outcome = handler.handle_hardware_command(envelope.clone()).await?;
                    self.publish_command_outcome(envelope, outcome).await
                }
                HardwareCommandDecision::Rejected { reply, .. } => {
                    if let Some(reply) = reply {
                        let lane = {
                            let inner = self.inner.lock().await;
                            inner.lane.clone()
                        };
                        lane.publish_hardware_message(*reply).await?;
                    }
                    Ok(())
                }
                HardwareCommandDecision::Ignored => Ok(()),
            }
        }

        async fn reconcile_claims(&self, _reason: &str) -> Result<HardwareClaimReconcile> {
            let (reconcile, reset_handler) = {
                let mut inner = self.inner.lock().await;
                if inner.closed {
                    return Ok(HardwareClaimReconcile {
                        reset_devices: BTreeSet::new(),
                    });
                }
                let state_snapshot = inner.state.clone();
                let mut selected_devices = state_snapshot.routing.claimed_device_ids();
                let managed = inner
                    .claim_manager
                    .reconcile_cached(
                        |contract, record| {
                            if !state_snapshot.accept_current_hardware_claim(contract, record)? {
                                return Ok(false);
                            }
                            let Some(terms_value) = record.terms.clone() else {
                                return Ok(false);
                            };
                            let terms = HardwareClaimTerms::from_value(terms_value)?;
                            let device_ids = terms
                                .devices
                                .iter()
                                .map(|device| device.device_ref.device_id.clone())
                                .collect::<BTreeSet<_>>();
                            if state_snapshot
                                .routing
                                .route_for_contract(&contract.key)
                                .is_some()
                            {
                                selected_devices.extend(device_ids);
                                return Ok(true);
                            }
                            if device_ids
                                .iter()
                                .any(|device_id| selected_devices.contains(device_id))
                            {
                                return Ok(false);
                            }
                            selected_devices.extend(device_ids);
                            Ok(true)
                        },
                        None,
                    )
                    .await?;
                let reconcile = inner.state.reconcile_claims(&managed);
                inner.publish_advertisement_if_changed(false).await?;
                (reconcile, inner.reset_handler.clone())
            };
            reset_devices(reset_handler, &reconcile.reset_devices).await?;
            Ok(reconcile)
        }

        async fn publish_command_outcome(
            &self,
            envelope: DeckrMessage,
            outcome: HardwareCommandOutcome,
        ) -> Result<()> {
            let Some(reason) = outcome.rejection() else {
                return Ok(());
            };
            let reply = {
                let inner = self.inner.lock().await;
                inner.state.rejection_reply_to(
                    &EndpointAddress::parse(&envelope.sender)?,
                    &envelope.sender_session_id,
                    envelope.contract.clone().ok_or_else(|| {
                        crate::Error::Invalid("hardware command missing contract".to_string())
                    })?,
                    envelope.hardware_body()?,
                    reason,
                )?
            };
            if let Some(reply) = reply {
                let lane = {
                    let inner = self.inner.lock().await;
                    inner.lane.clone()
                };
                lane.publish_hardware_message(reply).await?;
            }
            Ok(())
        }

        async fn cancel_contract_keys(
            &self,
            contract_keys: BTreeSet<String>,
            reason: &str,
        ) -> Result<()> {
            if contract_keys.is_empty() {
                return Ok(());
            }
            let mut inner = self.inner.lock().await;
            let contracts = inner.claim_manager.managed_contracts();
            for managed in contracts {
                if !contract_keys.contains(&managed.contract.key) {
                    continue;
                }
                inner
                    .claim_manager
                    .cancel(&managed.contract, Some(reason.to_string()))
                    .await?;
                inner
                    .claim_manager
                    .release_withdraw(&managed.contract.key)
                    .await?;
            }
            Ok(())
        }

        async fn reconcile_notification(
            &self,
            notification: &ConcordContractNotification,
        ) -> Result<HardwareClaimReconcile> {
            let (reconcile, reset_handler) = {
                let mut inner = self.inner.lock().await;
                if inner.closed {
                    return Ok(HardwareClaimReconcile {
                        reset_devices: BTreeSet::new(),
                    });
                }
                let state_snapshot = inner.state.clone();
                let mut selected_devices = state_snapshot.routing.claimed_device_ids();
                let managed = inner
                    .claim_manager
                    .reconcile_notification_cached(
                        notification,
                        |contract, record| {
                            if !state_snapshot.accept_current_hardware_claim(contract, record)? {
                                return Ok(false);
                            }
                            let Some(terms_value) = record.terms.clone() else {
                                return Ok(false);
                            };
                            let terms = HardwareClaimTerms::from_value(terms_value)?;
                            let device_ids = terms
                                .devices
                                .iter()
                                .map(|device| device.device_ref.device_id.clone())
                                .collect::<BTreeSet<_>>();
                            if state_snapshot
                                .routing
                                .route_for_contract(&contract.key)
                                .is_some()
                            {
                                selected_devices.extend(device_ids);
                                return Ok(true);
                            }
                            if device_ids
                                .iter()
                                .any(|device_id| selected_devices.contains(device_id))
                            {
                                return Ok(false);
                            }
                            selected_devices.extend(device_ids);
                            Ok(true)
                        },
                        None,
                    )
                    .await?;
                let reconcile = inner.state.reconcile_claims(&managed);
                inner.publish_advertisement_if_changed(false).await?;
                (reconcile, inner.reset_handler.clone())
            };
            reset_devices(reset_handler, &reconcile.reset_devices).await?;
            Ok(reconcile)
        }

        async fn refresh_managed_tokens(&self) -> Result<()> {
            let (reconcile, reset_handler) = {
                let mut inner = self.inner.lock().await;
                if inner.closed {
                    return Ok(());
                }
                let managed = inner.claim_manager.reconcile_managed_cached(None).await?;
                let reconcile = inner.state.reconcile_claims(&managed);
                inner.publish_advertisement_if_changed(false).await?;
                (reconcile, inner.reset_handler.clone())
            };
            reset_devices(reset_handler, &reconcile.reset_devices).await
        }

        async fn hardware_command_loop(&self) -> Result<()> {
            let (endpoint, lane) = {
                let inner = self.inner.lock().await;
                (inner.state.endpoint().clone(), inner.lane.clone())
            };
            let mut messages = lane.subscribe_hardware_messages(&endpoint).await?;
            while let Some(message) = messages.next().await {
                self.handle_command(message?).await?;
            }
            Ok(())
        }

        async fn beacon_refresh_loop(&self) -> Result<()> {
            loop {
                let interval = {
                    let inner = self.inner.lock().await;
                    if inner.closed {
                        return Ok(());
                    }
                    inner.maintenance_policy.renewal_interval
                };
                time::sleep(interval).await;
                let mut inner = self.inner.lock().await;
                if inner.closed {
                    return Ok(());
                }
                inner.publish_advertisement_if_changed(true).await?;
            }
        }

        async fn concord_watch_loop(&self) -> Result<()> {
            let mut notifications = {
                let inner = self.inner.lock().await;
                inner
                    .concord
                    .watch_contract_notifications_cached(
                        Some(HARDWARE_CLAIM_PROFILE_ID),
                        Some(inner.state.endpoint()),
                    )
                    .await?
            };
            loop {
                let notification = notifications.next().await?;
                self.reconcile_notification(&notification).await?;
            }
        }

        async fn concord_token_refresh_loop(&self) -> Result<()> {
            loop {
                let interval = {
                    let inner = self.inner.lock().await;
                    if inner.closed {
                        return Ok(());
                    }
                    inner.maintenance_policy.concord_token_check_interval()
                };
                time::sleep(interval).await;
                self.refresh_managed_tokens().await?;
            }
        }

        async fn periodic_reconcile_loop(&self) -> Result<()> {
            loop {
                let interval = {
                    let inner = self.inner.lock().await;
                    if inner.closed {
                        return Ok(());
                    }
                    inner.maintenance_policy.reconcile_interval
                };
                time::sleep(interval).await;
                self.reconcile_claims("periodic snapshot").await?;
            }
        }
    }

    impl<B, C, T, L> HardwareManagerRuntimeInner<B, C, T, L>
    where
        B: StateStore,
        C: StateStore,
        T: StateStore,
        L: HardwareLaneTransport,
    {
        fn claim_keys_for_device(&self, device_id: &str) -> BTreeSet<String> {
            self.state
                .routing
                .claims
                .get(device_id)
                .map(|claim| BTreeSet::from([claim.contract_key.clone()]))
                .unwrap_or_default()
        }

        async fn publish_advertisement_if_changed(&mut self, force: bool) -> Result<bool> {
            if self.closed {
                return Ok(false);
            }
            let payload = self.advertisement_payload_value()?;
            if !force
                && self.advertisement_handle.is_some()
                && self.advertised_payload.as_ref() == Some(&payload)
            {
                return Ok(false);
            }
            let advertiser = self.advertiser_for_payload(payload.clone());
            let handle = advertiser
                .publish_or_refresh(self.advertisement_handle.as_ref())
                .await?;
            self.advertisement_handle = Some(handle);
            self.advertised_payload = Some(payload);
            Ok(true)
        }

        async fn cleanup_stale_advertisement(&self) -> Result<usize> {
            let payload = self.advertisement_payload_value()?;
            self.advertiser_for_payload(payload)
                .cleanup_stale_same_endpoint()
                .await
        }

        async fn withdraw_advertisement(&mut self) -> Result<()> {
            let Some(handle) = self.advertisement_handle.take() else {
                return Ok(());
            };
            let payload = self
                .advertised_payload
                .take()
                .unwrap_or(Value::Object(Default::default()));
            self.advertiser_for_payload(payload).withdraw(&handle).await
        }

        fn advertisement_payload_value(&self) -> Result<Value> {
            self.state.advertisement_payload()?.to_value()
        }

        fn advertiser_for_payload(&self, payload: Value) -> BeaconAdvertiser<B> {
            BeaconAdvertiser::new(
                self.beacon_state.clone(),
                HARDWARE_FEATURE_ID,
                self.state.endpoint().clone(),
                self.state.session_id().to_string(),
            )
            .advertisement_id(self.advertisement_id.clone())
            .labels(self.state.labels.clone())
            .refresh_interval(self.maintenance_policy.renewal_interval)
            .payload(payload)
        }
    }

    async fn reset_devices(
        reset_handler: Option<Arc<dyn HardwareResetHandler>>,
        reset_devices: &BTreeSet<String>,
    ) -> Result<()> {
        let Some(reset_handler) = reset_handler else {
            return Ok(());
        };
        for device_id in reset_devices {
            reset_handler.reset_hardware_device(device_id).await?;
        }
        Ok(())
    }

    #[cfg(feature = "nats")]
    impl HardwareLaneTransport for crate::nats::NatsDeckrRuntime {
        async fn publish_hardware_message(&self, message: DeckrMessage) -> Result<()> {
            self.publish(&message).await
        }

        async fn subscribe_hardware_messages<'a>(
            &'a self,
            endpoint: &'a EndpointAddress,
        ) -> Result<HardwareMessageStream> {
            let runtime = self.clone();
            let subscriber = self.subscribe_endpoint_hardware_messages(endpoint).await?;
            let stream: HardwareMessageStream = Box::pin(futures_util::stream::unfold(
                (runtime, subscriber),
                |(runtime, mut subscriber)| async move {
                    let message = subscriber.next().await?;
                    let envelope = runtime.message_from_nats(message);
                    Some((envelope, (runtime, subscriber)))
                },
            ));
            Ok(stream)
        }
    }

    #[cfg(feature = "nats")]
    impl
        HardwareManagerRuntime<
            crate::nats::NatsStateStore,
            crate::nats::NatsStateStore,
            crate::nats::NatsStateStore,
            crate::nats::NatsDeckrRuntime,
        >
    {
        pub async fn from_nats_runtime(
            runtime: crate::nats::NatsDeckrRuntime,
            manager_id: impl Into<String>,
            session_id: impl Into<String>,
            labels: BTreeMap<String, String>,
            command_handler: Arc<dyn HardwareCommandHandler>,
            reset_handler: Option<Arc<dyn HardwareResetHandler>>,
            maintenance_policy: StateMaintenancePolicy,
        ) -> Result<Self> {
            Self::new(HardwareManagerRuntimeSpec {
                manager_id: manager_id.into(),
                session_id: session_id.into(),
                labels,
                beacon_state: runtime.beacon_advertisements().clone(),
                concord_contract_state: runtime.concord_contracts().clone(),
                concord_token_state: runtime.concord_tokens().clone(),
                lane: runtime,
                maintenance_policy,
                command_handler,
                reset_handler,
            })
            .await
        }

        pub async fn from_deckr_runtime(
            runtime: &crate::nats::DeckrRuntime,
            manager_id: impl Into<String>,
            session_id: impl Into<String>,
            labels: BTreeMap<String, String>,
            command_handler: Arc<dyn HardwareCommandHandler>,
            reset_handler: Option<Arc<dyn HardwareResetHandler>>,
            maintenance_policy: StateMaintenancePolicy,
        ) -> Result<Self> {
            Self::from_nats_runtime(
                runtime.nats().clone(),
                manager_id,
                session_id,
                labels,
                command_handler,
                reset_handler,
                maintenance_policy,
            )
            .await
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::lanes::DeviceRef;

        fn contract_pointer(contract_id: &str) -> ContractPointer {
            ContractPointer {
                contract_id: contract_id.to_string(),
                generation: 1,
            }
        }

        fn command(
            controller_id: &str,
            controller_session_id: &str,
            contract: Option<ContractPointer>,
        ) -> DeckrMessage {
            let mut message = DeckrMessage::hardware_command(
                controller_id,
                controller_session_id,
                "manager-main",
                "manager-session",
                "deck",
                contract_pointer("claim-a"),
                HardwareMessageBody::ControlCommand {
                    device_ref: DeviceRef {
                        manager_id: "manager-main".to_string(),
                        device_id: "deck".to_string(),
                        fingerprint: None,
                    },
                    control_id: Some("screen".to_string()),
                    capability_id: "raster.bitmap".to_string(),
                    command_type: "clear".to_string(),
                    params: Default::default(),
                },
            )
            .unwrap();
            message.contract = contract;
            message
        }

        fn state_with_device() -> HardwareManagerState {
            let mut state = HardwareManagerState::new("manager-main", "manager-session").unwrap();
            state
                .set_device(DeviceDescriptor {
                    device_id: "deck".to_string(),
                    fingerprint: "fingerprint:deck".to_string(),
                    display_name: "Deck".to_string(),
                    manufacturer: None,
                    model: None,
                    serial_number: None,
                    controls: Vec::new(),
                    capabilities: Vec::new(),
                })
                .unwrap();
            state
        }

        fn state_with_claim() -> HardwareManagerState {
            let mut state = state_with_device();
            state.routing.claims.insert(
                "deck".to_string(),
                super::super::HardwareClaimRoute {
                    controller_endpoint: EndpointAddress::parse("controller:main").unwrap(),
                    controller_session_id: "controller-session".to_string(),
                    contract: contract_pointer("claim-a"),
                    contract_key: "contracts.claim-a.1.meta".to_string(),
                    claim_id: "claim-a".to_string(),
                },
            );
            state
        }

        fn assert_unauthorized_refresh(decision: HardwareCommandDecision, expected_refresh: bool) {
            match decision {
                HardwareCommandDecision::Rejected {
                    reason,
                    refresh_claims,
                    ..
                } => {
                    assert_eq!(reason, "unauthorized");
                    assert_eq!(refresh_claims, expected_refresh);
                }
                other => panic!("expected unauthorized rejection, got {other:?}"),
            }
        }

        #[test]
        fn command_authorization_marks_missing_cached_claim_refresh_needed() {
            let state = state_with_device();
            let decision = state
                .authorize_command(command(
                    "main",
                    "controller-session",
                    Some(contract_pointer("claim-a")),
                ))
                .unwrap();

            assert_unauthorized_refresh(decision, true);
        }

        #[test]
        fn command_authorization_final_rejects_known_route_mismatches() {
            for message in [
                command(
                    "other",
                    "controller-session",
                    Some(contract_pointer("claim-a")),
                ),
                command("main", "other-session", Some(contract_pointer("claim-a"))),
                command(
                    "main",
                    "controller-session",
                    Some(contract_pointer("claim-other")),
                ),
                command("main", "controller-session", None),
            ] {
                let decision = state_with_claim().authorize_command(message).unwrap();
                assert_unauthorized_refresh(decision, false);
            }
        }
    }
}
