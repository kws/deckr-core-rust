use std::collections::{BTreeMap, BTreeSet};

use crate::authority::ContractPointer;
use crate::concord::{ConcordManagedContract, ContractValidityStatus};
use crate::endpoint::EndpointAddress;
use crate::lanes::{DeviceDescriptor, DeviceRef};
use crate::profiles::hardware::{
    HardwareAdvertisementDevice, HardwareBeaconPayload, HardwareClaimTerms, ProfileCapacity,
};
use crate::Result;

pub mod client;
pub mod validation;

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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use futures_core::Stream;
    use futures_util::{FutureExt, StreamExt};
    use serde_json::Value;
    use tokio::sync::{watch, Mutex};
    use tokio::task::JoinSet;
    use tokio::time;
    use uuid::Uuid;

    use crate::authority::ContractPointer;
    use crate::beacon::{
        AdvertisementContent, AdvertisementState, BeaconAdvertiser, ManagedAdvertisement,
    };
    use crate::concord::{
        AgreementContext, ConcordCoordinator, ConcordManagedContract, ContractHandle,
        ContractRecord, ManagedParticipant, ParticipantCandidates, ParticipantState,
    };
    use crate::endpoint::{hardware_manager_address, EndpointAddress};
    use crate::lanes::{DeckrMessage, DeviceDescriptor, HardwareMessageBody};
    use crate::lifecycle::{Cancellation, Clock, Failure, Phase, Snapshot, StateCell, StateWatch};
    use crate::messaging::{Delivery, DeliveryStream, MessageTransport};
    use crate::profiles::hardware::{
        HardwareClaimTerms, HARDWARE_CLAIM_PROFILE_ID, HARDWARE_FEATURE_ID,
    };
    use crate::state::{MaterializedStateStore, StateMaintenancePolicy, StateStore};
    use crate::{Error, Result};

    use super::validation::ValidatedDevice;
    use super::{hardware_beacon_payload, HardwareClaimReconcile, HardwareClaimRouting};
    use crate::schema::SchemaRegistry;

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

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct HardwareManagerDiagnostics {
        pub discovery_error: Option<String>,
        pub last_message_error: Option<String>,
        pub rejected_messages: u64,
    }

    /// Complete observed state of this manager. Discovery and participation
    /// retain separate readiness and failure information: discovery loss alone
    /// does not revoke a claim whose agreement still has local authority.
    #[derive(Debug, Clone, PartialEq)]
    pub struct HardwareRuntimeState {
        pub endpoint: EndpointAddress,
        pub session_id: String,
        pub devices: BTreeMap<String, DeviceDescriptor>,
        pub participation: Option<Snapshot<ParticipantState>>,
        pub discovery: Option<Snapshot<AdvertisementState>>,
        pub diagnostics: HardwareManagerDiagnostics,
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
    type HardwareReply = Arc<
        dyn Fn(HardwareMessageBody) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
            + Send
            + Sync,
    >;

    /// Driver work scoped to the validated request, stable device descriptor
    /// and exact agreement. Recheck after awaiting device/application work.
    #[derive(Clone)]
    pub struct HardwareCommandContext {
        message: DeckrMessage,
        device: Arc<ValidatedDevice>,
        authority: Arc<dyn Fn() -> Result<()> + Send + Sync>,
        reply: HardwareReply,
    }
    impl HardwareCommandContext {
        pub fn message(&self) -> &DeckrMessage {
            &self.message
        }
        pub fn device(&self) -> &DeviceDescriptor {
            self.device.descriptor()
        }
        pub fn authorize(&self) -> Result<()> {
            (self.authority)()
        }
        /// Send a response to this request, preserving its route, exact pointer,
        /// sessions and correlation. The body must match the requested device,
        /// capability and command/state operation. Authority is checked again
        /// after payload validation and before publication.
        pub async fn reply(&self, body: HardwareMessageBody) -> Result<()> {
            self.authorize()?;
            (self.reply)(body).await?;
            self.authorize()
        }
    }

    pub trait HardwareCommandHandler: Send + Sync + 'static {
        /// Compatibility callback for immediate driver operations.
        fn handle_hardware_command<'a>(
            &'a self,
            _message: DeckrMessage,
        ) -> HardwareCommandFuture<'a> {
            Box::pin(async { Ok(HardwareCommandOutcome::Unsupported) })
        }

        /// Override this for asynchronous driver work. The context permanently
        /// loses authority when its agreement ends or the device is replaced.
        fn handle_authorized_hardware_command<'a>(
            &'a self,
            context: HardwareCommandContext,
        ) -> HardwareCommandFuture<'a> {
            Box::pin(async move {
                context.authorize()?;
                self.handle_hardware_command(context.message().clone())
                    .await
            })
        }
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

        /// Compatibility transports without return routes use ordinary lane replies.
        fn subscribe_hardware_deliveries<'a>(
            &'a self,
            endpoint: &'a EndpointAddress,
        ) -> impl Future<Output = Result<DeliveryStream>> + Send + 'a {
            async move {
                let source = self.subscribe_hardware_messages(endpoint).await?;
                Ok(Box::pin(source.map(|message| {
                    message.map(|message| Delivery {
                        message,
                        reply_address: None,
                    })
                })) as DeliveryStream)
            }
        }

        fn publish_hardware_reply(
            &self,
            message: DeckrMessage,
            reply_address: Option<String>,
        ) -> impl Future<Output = Result<()>> + Send + '_ {
            async move {
                if reply_address.is_some() {
                    return Err(Error::Unsupported(
                        "hardware transport does not support reply inboxes".into(),
                    ));
                }
                self.publish_hardware_message(message).await
            }
        }
    }

    /// Compose a host's ordinary message adapter with the managed hardware
    /// boundary. The boundary retains application authorization and validation.
    #[derive(Clone)]
    pub struct HardwareTransport<T: MessageTransport>(pub T);
    impl<T: MessageTransport> HardwareLaneTransport for HardwareTransport<T> {
        async fn publish_hardware_message(&self, message: DeckrMessage) -> Result<()> {
            self.0.publish(&message, None, None).await
        }
        async fn publish_hardware_reply(
            &self,
            message: DeckrMessage,
            reply_address: Option<String>,
        ) -> Result<()> {
            self.0
                .publish(&message, reply_address.as_deref(), None)
                .await
        }
        async fn subscribe_hardware_messages<'a>(
            &'a self,
            endpoint: &'a EndpointAddress,
        ) -> Result<HardwareMessageStream> {
            let stream = self.subscribe_hardware_deliveries(endpoint).await?;
            Ok(Box::pin(
                stream.map(|delivery| delivery.map(|delivery| delivery.message)),
            ))
        }
        async fn subscribe_hardware_deliveries<'a>(
            &'a self,
            endpoint: &'a EndpointAddress,
        ) -> Result<DeliveryStream> {
            self.0
                .subscribe(crate::lanes::HARDWARE_MESSAGES_LANE, endpoint)
                .await
        }
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

        fn set_device(&mut self, descriptor: DeviceDescriptor) {
            self.devices
                .insert(descriptor.device_id.clone(), descriptor);
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

        fn authorize_command(
            &self,
            envelope: DeckrMessage,
            now: chrono::DateTime<chrono::Utc>,
        ) -> Result<HardwareCommandDecision> {
            if envelope.recipient_endpoint() != Some(self.endpoint.as_str())
                || envelope
                    .recipient_session_id
                    .as_deref()
                    .is_some_and(|id| id != self.session_id)
                || envelope.is_expired_at(now)
            {
                return Ok(HardwareCommandDecision::Ignored);
            }
            if envelope.contract.is_none() {
                return Ok(HardwareCommandDecision::Rejected {
                    reason: "unauthorized".to_string(),
                    reply: None,
                    refresh_claims: false,
                });
            }
            envelope.validate()?;
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
            // This manager exposes one instance of each connected device.
            if device.instance_count != 1 {
                return Ok(false);
            }
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
        /// Compatibility configuration for callers of the original runtime.
        /// Managed publication and token workers now schedule from store TTLs;
        /// these intervals do not control their lifetimes or run periodic repair.
        pub maintenance_policy: StateMaintenancePolicy,
        pub command_handler: Arc<dyn HardwareCommandHandler>,
        pub reset_handler: Option<Arc<dyn HardwareResetHandler>>,
    }

    type MaterializedConcord<C, T> =
        ConcordCoordinator<MaterializedStateStore<C>, MaterializedStateStore<T>>;
    type MaterializedTermination<C, T> = AgreementContext<C, T>;

    #[derive(Clone, PartialEq)]
    struct DiscoveryIntent {
        version: u64,
        content: AdvertisementContent,
    }
    struct HardwareDiscovery {
        desired: watch::Sender<DiscoveryIntent>,
        attempted: watch::Sender<Option<u64>>,
        publication: std::sync::Mutex<Option<ManagedAdvertisement>>,
        stop: Cancellation,
    }
    impl HardwareDiscovery {
        fn new() -> Self {
            Self {
                desired: watch::channel(DiscoveryIntent {
                    version: 0,
                    content: AdvertisementContent::default(),
                })
                .0,
                attempted: watch::channel(None).0,
                publication: std::sync::Mutex::new(None),
                stop: Cancellation::default(),
            }
        }
        fn update(&self, content: AdvertisementContent) {
            self.desired.send_if_modified(|intent| {
                if intent.content == content {
                    return false;
                }
                intent.version += 1;
                intent.content = content;
                true
            });
        }
        fn publication(&self) -> Option<ManagedAdvertisement> {
            self.publication
                .lock()
                .expect("hardware publication lock poisoned")
                .clone()
        }
        fn request_close(&self) {
            self.stop.cancel();
            if let Some(publication) = self.publication() {
                publication.request_close();
            }
        }
        async fn synchronize(&self) {
            if self.publication().is_none() {
                return;
            }
            let version = self.desired.borrow().version;
            let mut attempted = self.attempted.subscribe();
            loop {
                if attempted
                    .borrow_and_update()
                    .is_some_and(|done| done >= version)
                {
                    return;
                }
                tokio::select! {
                    _ = self.stop.cancelled() => return,
                    _ = attempted.changed() => {},
                }
            }
        }
    }

    pub struct HardwareManagerRuntime<B, C, T, L>
    where
        B: StateStore,
        C: StateStore,
        T: StateStore,
        L: HardwareLaneTransport,
    {
        inner: Arc<Mutex<HardwareManagerRuntimeInner<B, C, T, L>>>,
        lifecycle: Arc<HardwareLifecycle>,
        sources: MaterializedConcord<C, T>,
        owns_sources: bool,
        discovery: Arc<HardwareDiscovery>,
        observed: StateCell<HardwareRuntimeState>,
        schemas: SchemaRegistry,
    }

    struct HardwareLifecycle {
        closed: AtomicBool,
        started: AtomicBool,
        claims_ready: AtomicBool,
        initialized: AtomicBool,
        failure: std::sync::Mutex<Option<Failure>>,
        cleanup_started: AtomicBool,
        cleanup_status: watch::Sender<Option<std::result::Result<(), String>>>,
        active: AtomicUsize,
        workers: std::sync::Mutex<Vec<tokio::task::AbortHandle>>,
        changed: tokio::sync::Notify,
    }
    impl Default for HardwareLifecycle {
        fn default() -> Self {
            Self {
                closed: AtomicBool::new(false),
                started: AtomicBool::new(false),
                claims_ready: AtomicBool::new(false),
                initialized: AtomicBool::new(false),
                failure: std::sync::Mutex::new(None),
                cleanup_started: AtomicBool::new(false),
                cleanup_status: watch::channel(None).0,
                active: AtomicUsize::new(0),
                workers: std::sync::Mutex::new(Vec::new()),
                changed: tokio::sync::Notify::new(),
            }
        }
    }
    struct WorkerGuard(Arc<HardwareLifecycle>);
    impl Drop for WorkerGuard {
        fn drop(&mut self) {
            self.0.active.fetch_sub(1, Ordering::SeqCst);
            self.0.changed.notify_waiters();
        }
    }
    struct StartupGuard<F: FnOnce()>(Option<F>);
    impl<F: FnOnce()> Drop for StartupGuard<F> {
        fn drop(&mut self) {
            if let Some(cleanup) = self.0.take() {
                cleanup();
            }
        }
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
                lifecycle: self.lifecycle.clone(),
                sources: self.sources.clone(),
                owns_sources: self.owns_sources,
                discovery: self.discovery.clone(),
                observed: self.observed.clone(),
                schemas: self.schemas.clone(),
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
        validated_devices: BTreeMap<String, Arc<ValidatedDevice>>,
        beacon_state: B,
        advertisement_id: String,
        discovery: Arc<HardwareDiscovery>,
        concord: MaterializedConcord<C, T>,
        claim_manager: Option<ManagedParticipant<C, T>>,
        lane: L,
        command_handler: Arc<dyn HardwareCommandHandler>,
        reset_handler: Option<Arc<dyn HardwareResetHandler>>,
        diagnostics: HardwareManagerDiagnostics,
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
            Self::new_with_schemas(spec, SchemaRegistry::default()).await
        }

        pub async fn new_with_schemas(
            spec: HardwareManagerRuntimeSpec<B, C, T, L>,
            schemas: SchemaRegistry,
        ) -> Result<Self> {
            let concord = ConcordCoordinator::new(
                MaterializedStateStore::start(
                    spec.concord_contract_state.clone(),
                    crate::keys::concord_contracts_prefix(),
                )
                .await?,
                MaterializedStateStore::start(
                    spec.concord_token_state.clone(),
                    crate::keys::concord_contracts_prefix(),
                )
                .await?,
            );
            Self::build(spec, concord, true, schemas)
        }

        /// Borrow host-owned sources. Stopping this manager does not close them.
        pub fn with_concord(
            spec: HardwareManagerRuntimeSpec<B, C, T, L>,
            concord: crate::runtime::MaterializedConcord<C, T>,
        ) -> Result<Self> {
            Self::with_concord_and_schemas(spec, concord, SchemaRegistry::default())
        }

        pub fn with_concord_and_schemas(
            spec: HardwareManagerRuntimeSpec<B, C, T, L>,
            concord: crate::runtime::MaterializedConcord<C, T>,
            schemas: SchemaRegistry,
        ) -> Result<Self> {
            Self::build(spec, concord, false, schemas)
        }

        fn build(
            spec: HardwareManagerRuntimeSpec<B, C, T, L>,
            concord: MaterializedConcord<C, T>,
            owns_sources: bool,
            schemas: SchemaRegistry,
        ) -> Result<Self> {
            let mut state = HardwareManagerState::new(&spec.manager_id, &spec.session_id)?;
            state.set_labels(spec.labels);
            let discovery = Arc::new(HardwareDiscovery::new());
            let observed = StateCell::new(HardwareRuntimeState {
                endpoint: state.endpoint().clone(),
                session_id: state.session_id().to_string(),
                devices: state.devices().clone(),
                participation: None,
                discovery: None,
                diagnostics: HardwareManagerDiagnostics::default(),
            });
            Ok(Self {
                lifecycle: Arc::default(),
                sources: concord.clone(),
                owns_sources,
                discovery: discovery.clone(),
                observed,
                schemas,
                inner: Arc::new(Mutex::new(HardwareManagerRuntimeInner {
                    state,
                    validated_devices: BTreeMap::new(),
                    beacon_state: spec.beacon_state,
                    advertisement_id: format!("hardware-{}", Uuid::new_v4()),
                    discovery,
                    concord,
                    claim_manager: None,
                    lane: spec.lane,
                    command_handler: spec.command_handler,
                    reset_handler: spec.reset_handler,
                    diagnostics: HardwareManagerDiagnostics::default(),
                    closed: false,
                })),
            })
        }

        pub async fn start(&self, tasks: &mut JoinSet<Result<()>>) -> Result<()> {
            if self.lifecycle.closed.load(Ordering::SeqCst) {
                return Err(Error::Closed("hardware runtime closed".into()));
            }
            if self.lifecycle.started.swap(true, Ordering::SeqCst) {
                return Err(Error::Invalid("hardware runtime already started".into()));
            }
            let runtime = self.clone();
            let mut startup = StartupGuard(Some(move || {
                tokio::spawn(async move {
                    let _ = runtime.stop().await;
                });
            }));
            let result = self.start_inner(tasks).await;
            if let Err(error) = &result {
                self.record_failure("hardware startup", error);
                let _ = self.stop().await;
            }
            startup.0 = None;
            result
        }

        async fn start_inner(&self, tasks: &mut JoinSet<Result<()>>) -> Result<()> {
            let (advertiser, endpoint, session_id, lane) = {
                let inner = self.inner.lock().await;
                (
                    inner.advertiser_for_payload(inner.advertisement_payload_value()?),
                    inner.state.endpoint().clone(),
                    inner.state.session_id().to_string(),
                    inner.lane.clone(),
                )
            };
            let session = crate::session::EndpointSession::with_id(endpoint.clone(), session_id)?;
            self.starting_step(self.sources.bootstrap(
                &session,
                Some(HARDWARE_CLAIM_PROFILE_ID),
                true,
            ))
            .await?;
            // Listening precedes discovery publication, and network I/O never
            // holds the authority lock.
            let messages = self
                .starting_step(lane.subscribe_hardware_deliveries(&endpoint))
                .await?;
            self.starting_step(advertiser.cleanup_stale_same_endpoint())
                .await?;
            let weak = Arc::downgrade(&self.inner);
            let participant = self.sources.participate(
                session,
                Some(HARDWARE_CLAIM_PROFILE_ID.into()),
                move |candidates: ParticipantCandidates| {
                    let weak = weak.clone();
                    async move {
                        let inner = weak
                            .upgrade()
                            .ok_or_else(|| Error::Closed("hardware manager dropped".into()))?;
                        let state = inner.lock().await.state.clone();
                        let selected: BTreeSet<_> = candidates
                            .selected
                            .iter()
                            .map(|pointer| {
                                crate::concord::concord_contract_key(
                                    &pointer.contract_id,
                                    pointer.generation,
                                )
                            })
                            .collect();
                        let mut contracts = candidates.contracts;
                        contracts.sort_by(|left, right| {
                            (!selected.contains(&left.contract.key))
                                .cmp(&!selected.contains(&right.contract.key))
                                .then_with(|| left.contract.key.cmp(&right.contract.key))
                        });
                        let mut devices = BTreeSet::new();
                        let mut accepted = Vec::new();
                        for candidate in contracts {
                            let Some(record) = candidate.validity.contract else {
                                continue;
                            };
                            if !state.accept_current_hardware_claim(&candidate.contract, &record)? {
                                continue;
                            }
                            let terms = HardwareClaimTerms::from_value(
                                record.terms.expect("accepted claim has terms"),
                            )?;
                            let requested: BTreeSet<_> = terms
                                .devices
                                .iter()
                                .map(|device| device.device_ref.device_id.clone())
                                .collect();
                            if !requested.is_disjoint(&devices) {
                                continue;
                            }
                            devices.extend(requested);
                            accepted.push(ContractPointer {
                                contract_id: candidate.contract.contract_id,
                                generation: candidate.contract.generation,
                            });
                        }
                        Ok(accepted)
                    }
                },
            )?;
            self.inner.lock().await.claim_manager = Some(participant.clone());
            let ready = self
                .starting_step(participant.wait_for(
                    |state| matches!(state.phase, Phase::Ready | Phase::Terminal | Phase::Closed),
                    None,
                ))
                .await?;
            if ready.phase != Phase::Ready {
                return Err(Error::StateUnavailable(
                    "hardware participation bootstrap failed".into(),
                ));
            }
            self.lifecycle.claims_ready.store(true, Ordering::SeqCst);
            self.starting_step(self.reconcile_claims("startup")).await?;
            let content = self.discovery.desired.borrow().content.clone();
            let publication = {
                let mut slot = self
                    .discovery
                    .publication
                    .lock()
                    .expect("hardware publication lock poisoned");
                if self.discovery.stop.is_cancelled() {
                    return Err(Error::Closed("hardware startup stopped".into()));
                }
                let publication = advertiser
                    .payload(content.payload.clone().unwrap_or(Value::Null))
                    .manage()?;
                *slot = Some(publication.clone());
                publication
            };
            let runtime = self.clone();
            self.spawn_worker(tasks, async move {
                runtime.hardware_command_loop(messages).await
            })?;
            let runtime = self.clone();
            self.spawn_worker(tasks, async move { runtime.discovery_update_loop().await })?;
            let runtime = self.clone();
            self.spawn_worker(tasks, async move { runtime.concord_watch_loop().await })?;
            let runtime = self.clone();
            self.spawn_worker(tasks, async move { runtime.state_watch_loop().await })?;
            publication
                .wait_ready(Some(std::time::Duration::from_secs(5)))
                .await?;
            self.discovery.synchronize().await;
            if self.lifecycle.closed.load(Ordering::SeqCst) {
                return Err(Error::Closed("hardware startup stopped".into()));
            }
            self.lifecycle.initialized.store(true, Ordering::SeqCst);
            self.state().await;
            Ok(())
        }

        async fn starting_step<V>(&self, future: impl Future<Output = Result<V>>) -> Result<V> {
            tokio::select! {
                biased;
                _ = self.discovery.stop.cancelled() => Err(Error::Closed("hardware startup stopped".into())),
                result = time::timeout(std::time::Duration::from_secs(5), future) => result.map_err(|_| Error::Deadline("hardware startup step exceeded deadline".into()))?,
            }
        }

        fn spawn_worker(
            &self,
            tasks: &mut JoinSet<Result<()>>,
            future: impl Future<Output = Result<()>> + Send + 'static,
        ) -> Result<()> {
            let mut workers = self.lifecycle.workers.lock().expect("worker lock poisoned");
            if self.lifecycle.closed.load(Ordering::SeqCst) {
                return Err(Error::Closed("hardware startup stopped".into()));
            }
            self.lifecycle.active.fetch_add(1, Ordering::SeqCst);
            let guard = WorkerGuard(self.lifecycle.clone());
            let runtime = self.clone();
            let handle = tasks.spawn(async move {
                let result = std::panic::AssertUnwindSafe(future)
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| {
                        Err(Error::StateUnavailable("hardware worker panicked".into()))
                    });
                drop(guard);
                if let Err(error) = &result {
                    runtime.record_failure("hardware worker", error);
                    runtime.lifecycle.closed.store(true, Ordering::SeqCst);
                    tokio::spawn(async move {
                        let _ = runtime.stop().await;
                    });
                }
                result
            });
            workers.push(handle);
            Ok(())
        }

        pub async fn stop(&self) -> Result<()> {
            self.lifecycle.closed.store(true, Ordering::SeqCst);
            self.discovery.request_close();
            self.state().await;
            for worker in self
                .lifecycle
                .workers
                .lock()
                .expect("worker lock poisoned")
                .drain(..)
            {
                worker.abort();
            }
            let mut completed = self.lifecycle.cleanup_status.subscribe();
            if !self.lifecycle.cleanup_started.swap(true, Ordering::SeqCst) {
                let runtime = self.clone();
                // One cleanup task serves every close caller and survives
                // cancellation of any caller waiting for it.
                tokio::spawn(async move {
                    let result = time::timeout(std::time::Duration::from_secs(5), async {
                        loop {
                            let changed = runtime.lifecycle.changed.notified();
                            tokio::pin!(changed);
                            changed.as_mut().enable();
                            if runtime.lifecycle.active.load(Ordering::SeqCst) == 0 {
                                break;
                            }
                            changed.await;
                        }
                        runtime.stop_inner().await
                    })
                    .await
                    .map_err(|_| Error::Deadline("hardware cleanup deadline exceeded".into()))
                    .and_then(|result| result);
                    if runtime.owns_sources {
                        runtime.sources.close_sources_and_wait().await;
                    }
                    if let Err(error) = &result {
                        runtime.record_failure("hardware cleanup", error);
                    }
                    runtime
                        .lifecycle
                        .cleanup_status
                        .send_replace(Some(result.map_err(|error| error.to_string())));
                    runtime.state().await;
                });
            }
            loop {
                let result = completed.borrow_and_update().clone();
                if let Some(result) = result {
                    self.state().await;
                    return result.map_err(Error::StateUnavailable);
                }
                completed
                    .changed()
                    .await
                    .map_err(|_| Error::Closed("hardware cleanup channel closed".into()))?;
            }
        }

        async fn stop_inner(&self) -> Result<()> {
            let participant = {
                let mut inner = self.inner.lock().await;
                if inner.closed {
                    return Ok(());
                }
                inner.closed = true;
                inner.state.routing = HardwareClaimRouting::default();
                inner.claim_manager.clone()
            };
            if let Some(participant) = &participant {
                for member in participant.state().value.contracts.values() {
                    if let Some(contract) = &member.value.contract {
                        let pointer = ContractPointer {
                            contract_id: contract.contract_id.clone(),
                            generation: contract.generation,
                        };
                        if let Some(context) = participant.agreement(&pointer) {
                            context.request_withdraw();
                        }
                    }
                }
                participant.request_close();
            }
            let publication = self.discovery.publication();
            let withdrawal = async {
                if let Some(publication) = publication {
                    publication.close().await?;
                }
                Ok::<_, Error>(())
            };
            let claims = async {
                if let Some(participant) = participant {
                    participant.close().await?;
                }
                Ok::<_, Error>(())
            };
            let (claims, discovery) = futures_util::future::join(claims, withdrawal).await;
            claims.and(discovery)
        }

        pub async fn set_device(&self, descriptor: DeviceDescriptor) -> Result<()> {
            // Compile outside the authority lock. No schema or raster work
            // should delay revocation or inventory updates for other devices.
            let candidate = descriptor.clone();
            let schemas = self.schemas.clone();
            let validated = Arc::new(
                tokio::task::spawn_blocking(move || ValidatedDevice::new(candidate, &schemas))
                    .await
                    .map_err(|error| {
                        Error::StateUnavailable(format!(
                            "descriptor validation worker failed: {error}"
                        ))
                    })??,
            );
            let device_id = descriptor.device_id.clone();
            let (terminations, devices_to_reset, reset_handler) = {
                let mut inner = self.inner.lock().await;
                if self.lifecycle.closed.load(Ordering::SeqCst) {
                    return Err(Error::Closed("hardware runtime closed".into()));
                }
                let replaced = inner
                    .state
                    .devices()
                    .get(&device_id)
                    .is_some_and(|current| material_descriptor_change(current, &descriptor));
                let replaced_claim_keys = if replaced {
                    inner.claim_keys_for_device(&device_id)
                } else {
                    BTreeSet::new()
                };
                let (terminations, reset_devices) = inner.terminate_claims(
                    &replaced_claim_keys,
                    &format!("hardware device {device_id} replaced"),
                );
                inner.state.set_device(descriptor);
                inner.validated_devices.insert(device_id.clone(), validated);
                inner.refresh_discovery()?;
                if let Some(participant) = &inner.claim_manager {
                    participant.wake_policy();
                }
                (terminations, reset_devices, inner.reset_handler.clone())
            };
            self.state().await;
            let (cleanup, reset) = futures_util::future::join(
                cleanup_claims(
                    terminations,
                    format!("hardware device {device_id} replaced"),
                ),
                reset_devices(reset_handler, &devices_to_reset),
            )
            .await;
            if self.lifecycle.claims_ready.load(Ordering::SeqCst) {
                self.reconcile_claims("device inventory").await?;
            }
            self.discovery.synchronize().await;
            self.state().await;
            cleanup.and(reset)
        }

        pub async fn remove_device(&self, device_id: &str, reason: &str) -> Result<()> {
            let terminations = {
                let mut inner = self.inner.lock().await;
                if self.lifecycle.closed.load(Ordering::SeqCst) {
                    return Err(Error::Closed("hardware runtime closed".into()));
                }
                let claim_keys = inner.claim_keys_for_device(device_id);
                let (terminations, _) = inner.terminate_claims(
                    &claim_keys,
                    &format!("hardware device {device_id} {reason}"),
                );
                inner.state.remove_device(device_id);
                inner.validated_devices.remove(device_id);
                inner.refresh_discovery()?;
                if let Some(participant) = &inner.claim_manager {
                    participant.wake_policy();
                }
                terminations
            };
            self.state().await;
            let cleanup = cleanup_claims(
                terminations,
                format!("hardware device {device_id} {reason}"),
            )
            .await;
            if self.lifecycle.claims_ready.load(Ordering::SeqCst) {
                self.reconcile_claims("device inventory").await?;
            }
            self.discovery.synchronize().await;
            self.state().await;
            cleanup
        }

        fn record_failure(&self, operation: &str, error: &Error) {
            let mut failure = self
                .lifecycle
                .failure
                .lock()
                .expect("hardware failure lock poisoned");
            // Preserve the original worker/startup failure when cleanup also fails.
            if failure.is_none() {
                *failure = Some(Failure::from_error(operation, error));
            }
        }

        /// Immutable cached state. This never performs store or transport I/O.
        pub async fn state(&self) -> Snapshot<HardwareRuntimeState> {
            let inner = self.inner.lock().await;
            let participation = inner.claim_manager.as_ref().map(ManagedParticipant::state);
            let discovery = self
                .discovery
                .publication()
                .map(|publication| publication.state());
            let mut diagnostics = inner.diagnostics.clone();
            diagnostics.discovery_error = discovery
                .as_ref()
                .and_then(|state| state.failure.as_ref())
                .map(|failure| failure.detail.clone());
            let value = HardwareRuntimeState {
                endpoint: inner.state.endpoint().clone(),
                session_id: inner.state.session_id().to_string(),
                devices: inner.state.devices().clone(),
                participation,
                discovery,
                diagnostics,
            };
            let closing = self.lifecycle.closed.load(Ordering::SeqCst);
            let closed = self.lifecycle.cleanup_status.borrow().is_some();
            let current = !closing
                && value
                    .participation
                    .as_ref()
                    .is_some_and(|state| state.current)
                && value.discovery.as_ref().is_some_and(|state| state.current);
            let ready = current
                && value
                    .participation
                    .as_ref()
                    .is_some_and(|state| state.phase == Phase::Ready)
                && value
                    .discovery
                    .as_ref()
                    .is_some_and(|state| state.phase == Phase::Ready);
            let phase = if closed {
                Phase::Closed
            } else if closing {
                Phase::Closing
            } else if !self.lifecycle.initialized.load(Ordering::SeqCst) {
                Phase::Starting
            } else if ready {
                Phase::Ready
            } else {
                Phase::Unavailable
            };
            let failure = self
                .lifecycle
                .failure
                .lock()
                .expect("hardware failure lock poisoned")
                .clone()
                .or_else(|| {
                    value
                        .participation
                        .as_ref()
                        .and_then(|state| state.failure.clone())
                })
                .or_else(|| {
                    value
                        .discovery
                        .as_ref()
                        .and_then(|state| state.failure.clone())
                });
            self.observed.modify(|snapshot| {
                if snapshot.phase == phase
                    && snapshot.current == current
                    && snapshot.value == value
                    && snapshot.failure == failure
                {
                    return false;
                }
                snapshot.changed = snapshot
                    .value
                    .devices
                    .keys()
                    .chain(value.devices.keys())
                    .cloned()
                    .collect();
                for participation in [
                    snapshot.value.participation.as_ref(),
                    value.participation.as_ref(),
                ]
                .into_iter()
                .flatten()
                {
                    snapshot
                        .changed
                        .extend(participation.value.contracts.keys().cloned());
                }
                snapshot.phase = phase;
                snapshot.current = current;
                snapshot.value = value;
                snapshot.failure = failure;
                true
            });
            self.observed.snapshot()
        }

        /// A bounded subscription that starts with the complete current state.
        pub async fn watch_state(&self) -> Result<StateWatch<HardwareRuntimeState>> {
            self.state().await;
            self.observed.watch()
        }

        pub async fn wait_for(
            &self,
            predicate: impl FnMut(&Snapshot<HardwareRuntimeState>) -> bool,
            timeout: Option<std::time::Duration>,
        ) -> Result<Snapshot<HardwareRuntimeState>> {
            self.state().await;
            self.observed
                .wait_for(predicate, timeout, self.sources.runtime_clock().as_ref())
                .await
        }

        pub async fn diagnostics(&self) -> HardwareManagerDiagnostics {
            self.state().await.value.diagnostics
        }

        pub async fn handle_hardware_message(&self, body: HardwareMessageBody) -> Result<()> {
            if self.lifecycle.closed.load(Ordering::SeqCst) {
                return Ok(());
            }
            let (message, lane, validated, context) = {
                let inner = self.inner.lock().await;
                let claim_key = inner
                    .state
                    .routing
                    .claims
                    .get(&body.device_ref().device_id)
                    .map(|route| route.contract_key.clone());
                if !claim_key.is_some_and(|key| inner.has_current_authority(&key)) {
                    return Ok(());
                }
                let Some(validated) = inner
                    .validated_devices
                    .get(&body.device_ref().device_id)
                    .cloned()
                else {
                    return Ok(());
                };
                let route = &inner.state.routing.claims[&body.device_ref().device_id];
                let Some(context) = inner.agreement(&route.contract_key) else {
                    return Ok(());
                };
                (
                    inner.state.route_hardware_message(body)?,
                    inner.lane.clone(),
                    validated,
                    context,
                )
            };
            if let Some(mut message) = message {
                validate_operation(validated, message.hardware_body()?).await?;
                if self.lifecycle.closed.load(Ordering::SeqCst) {
                    return Err(Error::Closed("hardware manager closed".into()));
                }
                context.authorize()?;
                message.created_at = context.utc_now().to_rfc3339();
                lane.publish_hardware_message(message).await?;
            }
            Ok(())
        }

        async fn handle_command(&self, delivery: Delivery) -> Result<()> {
            let envelope = &delivery.message;
            if self.lifecycle.closed.load(Ordering::SeqCst) {
                return Ok(());
            }
            let mut decision = {
                let inner = self.inner.lock().await;
                inner
                    .state
                    .authorize_command(envelope.clone(), inner.concord.runtime_clock().utc_now())?
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
                    inner.state.authorize_command(
                        envelope.clone(),
                        inner.concord.runtime_clock().utc_now(),
                    )?
                };
            }

            match decision {
                HardwareCommandDecision::Authorized => {
                    let (handler, validated, context, lane) = {
                        let inner = self.inner.lock().await;
                        let Some(pointer) = &envelope.contract else {
                            return Ok(());
                        };
                        let key = crate::concord::concord_contract_key(
                            &pointer.contract_id,
                            pointer.generation,
                        );
                        if self.lifecycle.closed.load(Ordering::SeqCst)
                            || !inner.has_current_authority(&key)
                        {
                            return Ok(());
                        }
                        let body = envelope.hardware_body()?;
                        let Some(validated) = inner
                            .validated_devices
                            .get(&body.device_ref().device_id)
                            .cloned()
                        else {
                            return Ok(());
                        };
                        let Some(context) = inner.agreement(&key) else {
                            return Ok(());
                        };
                        (
                            inner.command_handler.clone(),
                            validated,
                            context,
                            inner.lane.clone(),
                        )
                    };
                    if let Err(error) =
                        validate_operation(validated.clone(), envelope.hardware_body()?).await
                    {
                        let outcome = match &error {
                            Error::Unsupported(_) => HardwareCommandOutcome::Unsupported,
                            Error::Unauthorized(_) => HardwareCommandOutcome::Stale,
                            _ => HardwareCommandOutcome::Rejected,
                        };
                        {
                            let mut inner = self.inner.lock().await;
                            inner.diagnostics.rejected_messages =
                                inner.diagnostics.rejected_messages.saturating_add(1);
                            inner.diagnostics.last_message_error = Some(error.to_string());
                        }
                        self.state().await;
                        return self.publish_command_outcome(&delivery, outcome).await;
                    }
                    context.authorize()?;
                    let response_session = context.session().id().to_string();
                    let lifecycle = self.lifecycle.clone();
                    let request = envelope.clone();
                    let authority: Arc<dyn Fn() -> Result<()> + Send + Sync> =
                        Arc::new(move || {
                            if lifecycle.closed.load(Ordering::SeqCst) {
                                return Err(Error::Closed("hardware manager closed".into()));
                            }
                            if request.is_expired_at(context.utc_now()) {
                                return Err(Error::Unauthorized("hardware request expired".into()));
                            }
                            context.authorize()
                        });
                    let response_device = validated.clone();
                    let response_clock = self.sources.runtime_clock();
                    let response_authority = authority.clone();
                    let response_delivery = delivery.clone();
                    let reply: HardwareReply = Arc::new(move |body| {
                        let lane = lane.clone();
                        let device = response_device.clone();
                        let authorize = response_authority.clone();
                        let delivery = response_delivery.clone();
                        let session_id = response_session.clone();
                        let clock = response_clock.clone();
                        Box::pin(async move {
                            authorize()?;
                            let request = delivery.message.hardware_body()?;
                            super::validation::validate_reply(&request, &body)?;
                            validate_operation(device, body.clone()).await?;
                            authorize()?;
                            let device_ref = body.device_ref().clone();
                            let mut reply = DeckrMessage::hardware_input_to(
                                &device_ref.manager_id,
                                &session_id,
                                &device_ref.device_id,
                                &delivery.message.sender,
                                &delivery.message.sender_session_id,
                                delivery.message.contract.clone().ok_or_else(|| {
                                    Error::Unauthorized("hardware request lacks contract".into())
                                })?,
                                body,
                            )?;
                            reply.in_reply_to = Some(delivery.message.message_id.clone());
                            publish_hardware_response(
                                &lane,
                                reply,
                                delivery.reply_address,
                                clock.as_ref(),
                            )
                            .await?;
                            authorize()
                        })
                    });
                    let driver_context = HardwareCommandContext {
                        message: envelope.clone(),
                        device: validated,
                        authority,
                        reply,
                    };
                    let outcome = handler
                        .handle_authorized_hardware_command(driver_context.clone())
                        .await?;
                    driver_context.authorize()?;
                    self.publish_command_outcome(&delivery, outcome).await
                }
                HardwareCommandDecision::Rejected { reply, .. } => {
                    if let Some(mut reply) = reply {
                        let lane = {
                            let inner = self.inner.lock().await;
                            inner.lane.clone()
                        };
                        reply.in_reply_to = Some(envelope.message_id.clone());
                        if !envelope.is_expired_at(self.sources.runtime_clock().utc_now()) {
                            publish_hardware_response(
                                &lane,
                                *reply,
                                delivery.reply_address,
                                self.sources.runtime_clock().as_ref(),
                            )
                            .await?;
                        }
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
                let managed = inner.managed_claims();
                let reconcile = inner.state.reconcile_claims(&managed);
                inner.refresh_discovery()?;
                (reconcile, inner.reset_handler.clone())
            };
            self.state().await;
            reset_devices(reset_handler, &reconcile.reset_devices).await?;
            Ok(reconcile)
        }

        async fn publish_command_outcome(
            &self,
            delivery: &Delivery,
            outcome: HardwareCommandOutcome,
        ) -> Result<()> {
            let envelope = &delivery.message;
            if envelope.is_expired_at(self.sources.runtime_clock().utc_now())
                || self.lifecycle.closed.load(Ordering::SeqCst)
            {
                return Ok(());
            }
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
            if let Some(mut reply) = reply {
                let lane = {
                    let inner = self.inner.lock().await;
                    inner.lane.clone()
                };
                reply.in_reply_to = Some(envelope.message_id.clone());
                if !envelope.is_expired_at(self.sources.runtime_clock().utc_now()) {
                    publish_hardware_response(
                        &lane,
                        reply,
                        delivery.reply_address.clone(),
                        self.sources.runtime_clock().as_ref(),
                    )
                    .await?;
                }
            }
            Ok(())
        }

        async fn hardware_command_loop(&self, mut messages: DeliveryStream) -> Result<()> {
            while let Some(message) = messages.next().await {
                let result = match message {
                    Ok(message) => self.handle_command(message).await,
                    Err(error) => Err(error),
                };
                match result {
                    Ok(()) => {}
                    Err(
                        error @ (Error::Invalid(_)
                        | Error::Json(_)
                        | Error::Unauthorized(_)
                        | Error::Unsupported(_)),
                    ) => {
                        let mut inner = self.inner.lock().await;
                        inner.diagnostics.rejected_messages =
                            inner.diagnostics.rejected_messages.saturating_add(1);
                        inner.diagnostics.last_message_error = Some(error.to_string());
                        drop(inner);
                        self.state().await;
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(Error::Closed("hardware message subscription ended".into()))
        }

        async fn discovery_update_loop(&self) -> Result<()> {
            let publication = self
                .discovery
                .publication()
                .ok_or_else(|| Error::Invalid("hardware publication not started".into()))?;
            let mut desired = self.discovery.desired.subscribe();
            loop {
                let intent = desired.borrow_and_update().clone();
                // Managed Beacon retries transient failures and owns its TTL
                // schedule. Errors here affect discovery diagnostics only.
                let _ = publication.update(intent.content).await;
                self.discovery.attempted.send_replace(Some(intent.version));
                tokio::select! {
                    _ = self.discovery.stop.cancelled() => return Ok(()),
                    changed = desired.changed() => { if changed.is_err() { return Ok(()); } },
                }
            }
        }

        async fn concord_watch_loop(&self) -> Result<()> {
            let participant = self
                .inner
                .lock()
                .await
                .claim_manager
                .clone()
                .ok_or_else(|| Error::Invalid("hardware participation not started".into()))?;
            let mut states = participant.watch_state()?;
            while let Some(state) = states.next().await {
                self.reconcile_claims("managed participation").await?;
                if state.phase == Phase::Closed {
                    break;
                }
            }
            if self.lifecycle.closed.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(Error::Closed("hardware participation ended".into()))
            }
        }

        async fn state_watch_loop(&self) -> Result<()> {
            let publication = self
                .discovery
                .publication()
                .ok_or_else(|| Error::Invalid("hardware publication not started".into()))?;
            let participant = self
                .inner
                .lock()
                .await
                .claim_manager
                .clone()
                .ok_or_else(|| Error::Invalid("hardware participation not started".into()))?;
            let mut discovery = publication.watch_state()?;
            let mut participation = participant.watch_state()?;
            let mut discovery_open = true;
            let mut participation_open = true;
            while discovery_open || participation_open {
                // Observability is independent of device-reset callbacks in
                // the routing worker, and never polls or scans either store.
                tokio::select! {
                    next = discovery.next(), if discovery_open => discovery_open = next.is_some(),
                    next = participation.next(), if participation_open => participation_open = next.is_some(),
                }
                self.state().await;
            }
            Ok(())
        }
    }

    impl<B, C, T, L> HardwareManagerRuntimeInner<B, C, T, L>
    where
        B: StateStore,
        C: StateStore,
        T: StateStore,
        L: HardwareLaneTransport,
    {
        fn terminate_claims(
            &mut self,
            keys: &BTreeSet<String>,
            reason: &str,
        ) -> (Vec<MaterializedTermination<C, T>>, BTreeSet<String>) {
            let mut reset_devices = BTreeSet::new();
            self.state.routing.claims.retain(|device_id, route| {
                if keys.contains(&route.contract_key) {
                    reset_devices.insert(device_id.clone());
                    false
                } else {
                    true
                }
            });
            let terminations = keys
                .iter()
                .filter_map(|key| self.agreement(key))
                .inspect(|context| context.request_close_with_reason(reason))
                .collect();
            (terminations, reset_devices)
        }

        fn refresh_discovery(&self) -> Result<()> {
            self.discovery.update(AdvertisementContent {
                payload: Some(self.advertisement_payload_value()?),
                labels: self.state.labels.clone(),
                ..Default::default()
            });
            Ok(())
        }

        fn agreement(&self, key: &str) -> Option<AgreementContext<C, T>> {
            let (contract_id, generation) = crate::keys::parse_concord_contract_key(key)?;
            self.claim_manager.as_ref()?.agreement(&ContractPointer {
                contract_id,
                generation,
            })
        }

        fn has_current_authority(&self, key: &str) -> bool {
            self.agreement(key)
                .is_some_and(|context| context.has_authority())
        }

        fn managed_claims(&self) -> Vec<ConcordManagedContract> {
            let Some(participant) = &self.claim_manager else {
                return Vec::new();
            };
            participant
                .state()
                .value
                .contracts
                .into_values()
                .filter_map(|member| {
                    if !member.value.local_authority {
                        return None;
                    }
                    let validity = member.value.validity?;
                    Some(ConcordManagedContract {
                        record: validity.contract.clone()?,
                        contract: member.value.contract?,
                        validity,
                        token: member.value.token,
                    })
                })
                .collect()
        }

        fn claim_keys_for_device(&self, device_id: &str) -> BTreeSet<String> {
            let Some(participant) = &self.claim_manager else {
                return BTreeSet::new();
            };
            participant
                .state()
                .value
                .contracts
                .values()
                .filter_map(|member| {
                    let contract = member.value.contract.as_ref()?;
                    let record = self.concord.contract_record_cached(contract).ok()??;
                    let terms = HardwareClaimTerms::from_value(record.terms?).ok()?;
                    terms
                        .devices
                        .iter()
                        .any(|device| device.device_ref.device_id == device_id)
                        .then_some(contract.key.clone())
                })
                .collect()
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
            .clock(self.concord.runtime_clock())
            .advertisement_id(self.advertisement_id.clone())
            .labels(self.state.labels.clone())
            .payload(payload)
        }
    }

    fn material_descriptor_change(current: &DeviceDescriptor, next: &DeviceDescriptor) -> bool {
        let mut comparable = current.clone();
        comparable.display_name.clone_from(&next.display_name);
        &comparable != next
    }

    async fn validate_operation(
        device: Arc<ValidatedDevice>,
        message: HardwareMessageBody,
    ) -> Result<()> {
        // Image decoding and schema evaluation must not occupy the async
        // scheduler responsible for the agreement's heartbeat deadline.
        tokio::task::spawn_blocking(move || device.validate_message(&message))
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("hardware validation worker failed: {error}"))
            })?
    }

    async fn publish_hardware_response<L: HardwareLaneTransport>(
        lane: &L,
        mut reply: DeckrMessage,
        route: Option<String>,
        clock: &dyn Clock,
    ) -> Result<()> {
        reply.created_at = clock.utc_now().to_rfc3339();
        reply.validate()?;
        crate::lifecycle::deadline(
            clock,
            std::time::Duration::from_secs(5),
            "hardware reply publication",
            lane.publish_hardware_reply(reply, route),
        )
        .await
    }

    async fn cleanup_claims<C: StateStore, T: StateStore>(
        terminations: Vec<MaterializedTermination<C, T>>,
        _reason: String,
    ) -> Result<()> {
        for result in
            futures_util::future::join_all(terminations.iter().map(|context| context.close())).await
        {
            result?;
        }
        Ok(())
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

        async fn publish_hardware_reply(
            &self,
            message: DeckrMessage,
            reply_address: Option<String>,
        ) -> Result<()> {
            self.message_transport()
                .publish(&message, reply_address.as_deref(), None)
                .await
        }

        async fn subscribe_hardware_deliveries<'a>(
            &'a self,
            endpoint: &'a EndpointAddress,
        ) -> Result<DeliveryStream> {
            self.message_transport()
                .subscribe(crate::lanes::HARDWARE_MESSAGES_LANE, endpoint)
                .await
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
            let concord = runtime.materialized_concord().await?;
            Self::with_concord(
                HardwareManagerRuntimeSpec {
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
                },
                concord,
            )
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
            state.set_device(DeviceDescriptor {
                device_id: "deck".to_string(),
                fingerprint: "fingerprint:deck".to_string(),
                display_name: "Deck".to_string(),
                manufacturer: None,
                model: None,
                serial_number: None,
                controls: Vec::new(),
                capabilities: Vec::new(),
                ..Default::default()
            });
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
                .authorize_command(
                    command(
                        "main",
                        "controller-session",
                        Some(contract_pointer("claim-a")),
                    ),
                    chrono::Utc::now(),
                )
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
                let decision = state_with_claim()
                    .authorize_command(message, chrono::Utc::now())
                    .unwrap();
                assert_unauthorized_refresh(decision, false);
            }
        }
    }
}
