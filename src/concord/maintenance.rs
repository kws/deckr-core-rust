//! Explicit, exact-store maintenance. Construction starts no work or watches.

use super::*;
use crate::lifecycle::{deadline, Clock, Failure, SystemClock};
use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use std::{future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc, time::Duration};

mod managed;
pub use managed::*;

pub const CONCORD_STALE_OBSERVATION_SCHEMA_ID: &str = "dev.deckr.concord.stale-observation.v1";
pub const CONCORD_TOKEN_CLEANUP_SCHEMA_ID: &str = "dev.deckr.concord.token-cleanup.v1";
pub const CONCORD_REAPER_STALE_REASON: &str = "concord_reaper_stale_contract";
fn stale_schema() -> String {
    CONCORD_STALE_OBSERVATION_SCHEMA_ID.into()
}
fn cleanup_schema() -> String {
    CONCORD_TOKEN_CLEANUP_SCHEMA_ID.into()
}

pub fn concord_stale_observation_key(id: &str, generation: u64) -> String {
    format!("stale.{}.{}", crate::keys::encode_key_token(id), generation)
}
pub fn concord_token_cleanup_key(id: &str, generation: u64) -> String {
    format!(
        "cleanup.{}.{}",
        crate::keys::encode_key_token(id),
        generation
    )
}
fn marker_pointer(key: &str, prefix: &str) -> Result<ContractPointer> {
    let parts: Vec<_> = key.split('.').collect();
    if parts.len() != 3 || parts[0] != prefix {
        return Err(Error::Invalid("invalid Concord maintenance key".into()));
    }
    let pointer = ContractPointer {
        contract_id: crate::keys::decode_key_token(parts[1])?,
        generation: parts[2]
            .parse()
            .map_err(|_| Error::Invalid("invalid maintenance generation".into()))?,
    };
    pointer.validate()?;
    if key
        != format!(
            "{prefix}.{}.{}",
            crate::keys::encode_key_token(&pointer.contract_id),
            pointer.generation
        )
    {
        return Err(Error::Invalid("noncanonical maintenance key".into()));
    }
    Ok(pointer)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StaleObservationRecord {
    #[serde(default = "stale_schema", rename = "schema")]
    pub schema_id: String,
    pub contract_id: String,
    pub generation: u64,
    pub first_observed_stale_at: String,
    pub status: ContractValidityStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}
impl StaleObservationRecord {
    pub fn from_value(value: Value) -> Result<Self> {
        let record: Self = serde_json::from_str(&value.to_string())?;
        record.validate()?;
        Ok(record)
    }
    pub fn from_text(text: &str) -> Result<Self> {
        Self::from_value(crate::json::parse(text.as_bytes())?)
    }
    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(serde_json::to_value(self)?)
    }
    pub fn validate(&self) -> Result<()> {
        if self.schema_id != CONCORD_STALE_OBSERVATION_SCHEMA_ID {
            return Err(Error::Invalid("wrong stale-observation schema".into()));
        }
        ContractPointer {
            contract_id: self.contract_id.clone(),
            generation: self.generation,
        }
        .validate()?;
        timestamp(&self.first_observed_stale_at)?;
        if let Some(reason) = &self.reason {
            require_text(reason, "stale observation reason")?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TokenCleanupRecord {
    #[serde(default = "cleanup_schema", rename = "schema")]
    pub schema_id: String,
    pub contract_id: String,
    pub generation: u64,
    pub contract_revision: u64,
    pub cleanup_started_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terms_hash: Option<String>,
}
impl TokenCleanupRecord {
    pub fn from_value(value: Value) -> Result<Self> {
        let record: Self = serde_json::from_str(&value.to_string())?;
        record.validate()?;
        Ok(record)
    }
    pub fn from_text(text: &str) -> Result<Self> {
        Self::from_value(crate::json::parse(text.as_bytes())?)
    }
    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(serde_json::to_value(self)?)
    }
    pub fn validate(&self) -> Result<()> {
        if self.schema_id != CONCORD_TOKEN_CLEANUP_SCHEMA_ID {
            return Err(Error::Invalid("wrong token-cleanup schema".into()));
        }
        ContractPointer {
            contract_id: self.contract_id.clone(),
            generation: self.generation,
        }
        .validate()?;
        timestamp(&self.cleanup_started_at)?;
        if let Some(hash) = &self.terms_hash {
            require_text(hash, "cleanup terms hash")?;
        }
        Ok(())
    }
}
fn timestamp(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|_| Error::Invalid("maintenance timestamp must be RFC 3339".into()))
}

#[derive(Debug, Clone)]
pub struct MaintenanceOptions {
    pub stale_grace: Duration,
    pub retention: Duration,
    pub operation_timeout: Duration,
}
impl Default for MaintenanceOptions {
    fn default() -> Self {
        Self {
            stale_grace: Duration::from_secs(900),
            retention: Duration::from_secs(3600),
            operation_timeout: Duration::from_secs(5),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MaintenanceTokenSummary {
    pub key: String,
    pub revision: u64,
    pub participant: EndpointAddress,
    pub session_id: String,
    pub token_id: String,
    pub refresh_seq: u64,
    pub ttl_seconds: u64,
    pub terms_hash: Option<String>,
}
/// Metadata only; full contract terms and token observations are excluded.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ContractDeletionAudit {
    pub contract: ContractPointer,
    pub contract_revision: u64,
    pub record_contract_id: Option<String>,
    pub record_generation: Option<u64>,
    pub profile: Option<String>,
    pub participants: Vec<EndpointAddress>,
    pub attached_participants: Vec<EndpointAddress>,
    pub created_at: Option<String>,
    pub created_by: Option<String>,
    pub cancelled_at: String,
    pub cancelled_by: Option<String>,
    pub cancel_reason: Option<String>,
    pub cancel_revision: Option<u64>,
    pub supersedes: Option<ContractPointer>,
    pub terms_hash: Option<String>,
    pub status: ContractValidityStatus,
    pub reason: Option<ValidityReason>,
    pub listed_token_keys: usize,
    pub tokens: Vec<MaintenanceTokenSummary>,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum MaintenanceAuditEvent {
    BeforeContractDeletion(Box<ContractDeletionAudit>),
    Conflict { key: String, operation: String },
}
pub type MaintenanceAuditFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
/// The host's audit sink. Deletion waits for successful recording of the audit.
pub trait MaintenanceAudit: Send + Sync + 'static {
    fn record(&self, event: MaintenanceAuditEvent) -> MaintenanceAuditFuture<'_>;
}
#[derive(Debug, Clone, PartialEq)]
pub struct MaintenanceFailure {
    pub key: String,
    pub failure: Failure,
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MaintenanceReport {
    pub contracts_scanned: usize,
    pub stale_observations_created: usize,
    pub stale_observations_removed: usize,
    pub contracts_cancelled: usize,
    pub contracts_deleted: usize,
    pub tokens_deleted: usize,
    pub cleanups_completed: usize,
    pub cleanup_pending: BTreeSet<String>,
    pub failures: Vec<MaintenanceFailure>,
    pub omitted_failures: usize,
}
impl MaintenanceReport {
    fn failed(&mut self, key: &str, operation: &str, error: &Error) {
        if self.failures.len() < 256 {
            self.failures.push(MaintenanceFailure {
                key: key.into(),
                failure: Failure::from_error(operation, error),
            });
        } else {
            self.omitted_failures += 1;
        }
    }
}

pub struct ConcordMaintenance<C: StateStore, T: StateStore, M: StateStore> {
    contracts: C,
    tokens: T,
    maintenance: M,
    clock: Arc<dyn Clock>,
    options: MaintenanceOptions,
    scan_gate: Arc<tokio::sync::Mutex<()>>,
}
impl<C: StateStore, T: StateStore, M: StateStore> Clone for ConcordMaintenance<C, T, M> {
    fn clone(&self) -> Self {
        Self {
            contracts: self.contracts.clone(),
            tokens: self.tokens.clone(),
            maintenance: self.maintenance.clone(),
            clock: self.clock.clone(),
            options: self.options.clone(),
            scan_gate: self.scan_gate.clone(),
        }
    }
}
impl<C: StateStore, T: StateStore, M: StateStore> ConcordMaintenance<C, T, M> {
    /// Requires dedicated raw stores. No reads, watchers or workers are started.
    pub fn new(contracts: C, tokens: T, maintenance: M) -> Self {
        Self {
            contracts,
            tokens,
            maintenance,
            clock: Arc::new(SystemClock),
            options: MaintenanceOptions::default(),
            scan_gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    pub fn with_options(mut self, options: MaintenanceOptions) -> Result<Self> {
        if options.operation_timeout.is_zero()
            || self
                .clock
                .now()
                .checked_add(options.operation_timeout)
                .is_none()
            || chrono::Duration::from_std(options.stale_grace).is_err()
            || chrono::Duration::from_std(options.retention).is_err()
        {
            return Err(Error::Invalid(
                "maintenance durations are out of range".into(),
            ));
        }
        self.options = options;
        Ok(self)
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
    async fn io<R>(&self, operation: &str, work: impl Future<Output = Result<R>>) -> Result<R> {
        deadline(
            self.clock.as_ref(),
            self.options.operation_timeout,
            operation,
            work,
        )
        .await
    }
    async fn audit(&self, sink: &dyn MaintenanceAudit, event: MaintenanceAuditEvent) -> Result<()> {
        self.io("Concord maintenance audit", async {
            sink.record(event).await
        })
        .await
    }
    /// Run one serialized exact scan. Dropping this future cancels the scan;
    /// persistent markers preserve any cleanup already begun. There are no
    /// runtime subscriptions or Beacon inputs.
    pub async fn scan(&self, audit: &dyn MaintenanceAudit) -> Result<MaintenanceReport> {
        let _guard = self.scan_gate.lock().await;
        AssertUnwindSafe(self.scan_inner(audit))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                Err(Error::StateUnavailable(
                    "Concord maintenance adapter or audit panicked".into(),
                ))
            })
    }

    async fn scan_inner(&self, audit: &dyn MaintenanceAudit) -> Result<MaintenanceReport> {
        if self
            .io("contract TTL", self.contracts.ttl_seconds())
            .await?
            .is_some()
            || self
                .io("maintenance TTL", self.maintenance.ttl_seconds())
                .await?
                .is_some()
            || self
                .io("token TTL", self.tokens.ttl_seconds())
                .await?
                .is_none_or(|ttl| ttl == 0)
        {
            return Err(Error::Invalid(
                "maintenance requires persistent contracts/markers and TTL-bound tokens".into(),
            ));
        }
        let mut report = MaintenanceReport::default();
        let markers = self
            .io("list cleanup markers", self.maintenance.items("cleanup."))
            .await
            .unwrap_or_else(|error| {
                report.failed("cleanup.", "list cleanup markers", &error);
                Vec::new()
            });
        for marker in markers {
            report.cleanup_pending.insert(marker.key.clone());
            if let Err(error) = self.cleanup(marker.clone(), &mut report, audit).await {
                report.failed(&marker.key, "resume token cleanup", &error);
            }
        }
        let entries = self
            .io(
                "list contracts",
                self.contracts.items(concord_contracts_prefix()),
            )
            .await
            .unwrap_or_else(|error| {
                report.failed("contracts.", "list contracts", &error);
                Vec::new()
            });
        for listed in entries {
            report.contracts_scanned += 1;
            if let Err(error) = self.process_contract(&listed.key, &mut report, audit).await {
                if error.is_state_conflict() {
                    if let Err(audit_error) = self
                        .audit(
                            audit,
                            MaintenanceAuditEvent::Conflict {
                                key: listed.key.clone(),
                                operation: "contract maintenance".into(),
                            },
                        )
                        .await
                    {
                        report.failed(&listed.key, "record mutation conflict", &audit_error);
                    }
                }
                report.failed(&listed.key, "contract maintenance", &error);
            }
        }
        // Independent exact reads prevent a stale list from treating a live
        // generation as orphaned. Malformed or mismatched markers are retained.
        let stale = self
            .io("list stale observations", self.maintenance.items("stale."))
            .await
            .unwrap_or_else(|error| {
                report.failed("stale.", "list stale observations", &error);
                Vec::new()
            });
        for listed in stale {
            let result = async {
                let pointer = marker_pointer(&listed.key, "stale")?;
                let key = make_concord_contract_key(&pointer.contract_id, pointer.generation);
                if self
                    .io("check orphan contract", self.contracts.get(&key))
                    .await?
                    .is_none()
                {
                    self.remove_stale(&pointer, &mut report).await?;
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                report.failed(&listed.key, "remove orphan stale observation", &error);
            }
        }
        Ok(report)
    }
    async fn process_contract(
        &self,
        key: &str,
        report: &mut MaintenanceReport,
        audit: &dyn MaintenanceAudit,
    ) -> Result<()> {
        let pointer = contract_pointer(key)?;
        let Some(entry) = self.io("read contract", self.contracts.get(key)).await? else {
            return Ok(());
        };
        check_contract_entry(&entry, &pointer)?;
        let value = &entry.value;
        match value.get("state").and_then(Value::as_str) {
            Some("cancelled") => {
                self.remove_stale(&pointer, report).await?;
                let cancelled = value
                    .get("cancelledAt")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Error::Invalid("retained contract has no cancellation timestamp".into())
                    })?;
                if elapsed(
                    self.clock.utc_now(),
                    timestamp(cancelled)?,
                    self.options.retention,
                ) {
                    self.delete_retained(entry, &pointer, report, audit).await?;
                }
            }
            Some("open") | None if value.get("state").is_none_or(|v| v == "open") => {
                let validity = self.validity(&entry, &pointer).await?;
                if !stale(&validity) {
                    self.remove_stale(&pointer, report).await?;
                    return Ok(());
                }
                let marker_key =
                    concord_stale_observation_key(&pointer.contract_id, pointer.generation);
                let observed = self
                    .io("read stale observation", self.maintenance.get(&marker_key))
                    .await?;
                let first = if let Some(observed) = observed {
                    let record = checked_stale(&observed, &pointer)?;
                    timestamp(&record.first_observed_stale_at)?
                } else {
                    let now = self.clock.utc_now();
                    let record = StaleObservationRecord {
                        schema_id: stale_schema(),
                        contract_id: pointer.contract_id.clone(),
                        generation: pointer.generation,
                        first_observed_stale_at: now.to_rfc3339(),
                        status: validity.status,
                        contract_revision: Some(entry.revision),
                        reason: validity.reason.clone(),
                    };
                    self.io(
                        "create stale observation",
                        self.maintenance
                            .create(&marker_key, record.to_value()?, None),
                    )
                    .await?;
                    report.stale_observations_created += 1;
                    now
                };
                if elapsed(self.clock.utc_now(), first, self.options.stale_grace) {
                    // Recollect exact token observations immediately before the
                    // guarded cancellation. A refresh can have arrived during
                    // marker I/O. A changed contract is left for the next scan.
                    let Some(current) = self
                        .io("recheck stale contract", self.contracts.get(key))
                        .await?
                    else {
                        return Ok(());
                    };
                    check_contract_entry(&current, &pointer)?;
                    if current != entry {
                        return Err(Error::StateConflict(
                            "contract changed during stale observation".into(),
                        ));
                    }
                    if !stale(&self.validity(&current, &pointer).await?) {
                        self.remove_stale(&pointer, report).await?;
                        return Ok(());
                    }
                    let mut cancelled = current.value.clone();
                    cancelled["state"] = Value::String("cancelled".into());
                    cancelled["cancelledAt"] = Value::String(self.clock.utc_now().to_rfc3339());
                    cancelled["cancelledBy"] = Value::String("concord:maintenance".into());
                    cancelled["cancelReason"] = Value::String(CONCORD_REAPER_STALE_REASON.into());
                    cancelled["cancelRevision"] = current.revision.into();
                    self.io(
                        "cancel stale contract",
                        self.contracts
                            .update(key, cancelled, current.revision, None),
                    )
                    .await?;
                    report.contracts_cancelled += 1;
                    self.remove_stale(&pointer, report).await?;
                }
            }
            _ => {
                return Err(Error::Invalid(
                    "contract state is neither open nor cancelled".into(),
                ))
            }
        }
        Ok(())
    }
    async fn validity(
        &self,
        entry: &StateEntry,
        pointer: &ContractPointer,
    ) -> Result<ContractValidity> {
        let handle = contract_handle_from_entry(entry.clone()).unwrap_or_else(|| ContractHandle {
            key: entry.key.clone(),
            contract_id: pointer.contract_id.clone(),
            generation: pointer.generation,
            participants: Vec::new(),
            attached_participants: Vec::new(),
            revision: entry.revision,
            state: ContractState::Open,
            profile: None,
            terms_hash: None,
        });
        let mut tokens = BTreeMap::new();
        // Invalid contracts are still evaluated as raw observations, but never
        // use unvalidated participant/identity fields to choose token keys.
        if let Ok(record) = ContractRecord::from_value(entry.value.clone()) {
            if record.contract_id != pointer.contract_id || record.generation != pointer.generation
            {
                return Ok(evaluate_observations(
                    &handle,
                    Some(entry.clone()),
                    &tokens,
                    None,
                ));
            }
            for participant in record.participants {
                let key = make_concord_participant_token_key(
                    &pointer.contract_id,
                    pointer.generation,
                    &participant,
                );
                if let Some(token) = self
                    .io("validate exact token", self.tokens.get(&key))
                    .await?
                {
                    tokens.insert(key, token);
                }
            }
        }
        Ok(evaluate_observations(
            &handle,
            Some(entry.clone()),
            &tokens,
            None,
        ))
    }
    async fn remove_stale(
        &self,
        pointer: &ContractPointer,
        report: &mut MaintenanceReport,
    ) -> Result<()> {
        let key = concord_stale_observation_key(&pointer.contract_id, pointer.generation);
        if let Some(entry) = self
            .io("read stale observation", self.maintenance.get(&key))
            .await?
        {
            checked_stale(&entry, pointer)?;
            self.io(
                "remove stale observation",
                self.maintenance.delete(&key, Some(entry.revision)),
            )
            .await?;
            report.stale_observations_removed += 1;
        }
        Ok(())
    }
    async fn delete_retained(
        &self,
        entry: StateEntry,
        pointer: &ContractPointer,
        report: &mut MaintenanceReport,
        audit: &dyn MaintenanceAudit,
    ) -> Result<()> {
        let terms_hash = raw_terms_hash(&entry.value)?;
        let token_entries = self.list_tokens(pointer, &terms_hash).await?;
        let validity = self.validity(&entry, pointer).await?;
        let audit_record = deletion_audit(
            &entry,
            pointer,
            &validity,
            &token_entries,
            terms_hash.clone(),
        )?;
        self.audit(
            audit,
            MaintenanceAuditEvent::BeforeContractDeletion(Box::new(audit_record)),
        )
        .await?;
        let key = concord_token_cleanup_key(&pointer.contract_id, pointer.generation);
        let expected = TokenCleanupRecord {
            schema_id: cleanup_schema(),
            contract_id: pointer.contract_id.clone(),
            generation: pointer.generation,
            contract_revision: entry.revision,
            cleanup_started_at: self.clock.utc_now().to_rfc3339(),
            terms_hash,
        };
        let marker = if let Some(existing) = self
            .io("read cleanup marker", self.maintenance.get(&key))
            .await?
        {
            let record = checked_cleanup(&existing)?;
            if record.contract_id != expected.contract_id
                || record.generation != expected.generation
                || record.terms_hash != expected.terms_hash
                || record.contract_revision > expected.contract_revision
            {
                return Err(Error::Invalid(
                    "cleanup marker disagrees with retained contract identity".into(),
                ));
            }
            if record.contract_revision == expected.contract_revision {
                existing
            } else {
                // A prior CAS conflict left the cancelled record present. Only
                // its revision can advance; identity and terms remain fenced.
                self.io(
                    "advance cleanup revision",
                    self.maintenance
                        .update(&key, expected.to_value()?, existing.revision, None),
                )
                .await?
            }
        } else {
            self.io(
                "create cleanup marker",
                self.maintenance.create(&key, expected.to_value()?, None),
            )
            .await?
        };
        report.cleanup_pending.insert(key.clone());
        let written = checked_cleanup(&marker)?;
        if written.contract_id != expected.contract_id
            || written.generation != expected.generation
            || written.contract_revision != expected.contract_revision
            || written.terms_hash != expected.terms_hash
        {
            return Err(Error::Invalid(
                "written cleanup marker disagrees with retained contract".into(),
            ));
        }
        if self
            .io("recheck retained contract", self.contracts.get(&entry.key))
            .await?
            .as_ref()
            != Some(&entry)
            || self
                .io("recheck cleanup marker", self.maintenance.get(&key))
                .await?
                .as_ref()
                != Some(&marker)
        {
            return Err(Error::StateConflict(
                "retained contract or cleanup marker changed before deletion".into(),
            ));
        }
        self.io(
            "delete retained contract",
            self.contracts.delete(&entry.key, Some(entry.revision)),
        )
        .await?;
        report.contracts_deleted += 1;
        self.cleanup(marker, report, audit).await
    }
    async fn list_tokens(
        &self,
        pointer: &ContractPointer,
        terms_hash: &Option<String>,
    ) -> Result<Vec<(StateEntry, ParticipantTokenRecord)>> {
        let prefix = format!(
            "{}participants.",
            crate::keys::concord_contract_prefix(&pointer.contract_id, pointer.generation)
        );
        let entries = self
            .io("list generation tokens", self.tokens.items(&prefix))
            .await?;
        let mut seen = BTreeSet::new();
        entries
            .into_iter()
            .map(|entry| {
                let token = ParticipantTokenRecord::from_value(entry.value.clone())?;
                if !entry.key.starts_with(&prefix)
                    || !seen.insert(entry.key.clone())
                    || entry.key
                        != make_concord_participant_token_key(
                            &pointer.contract_id,
                            pointer.generation,
                            &token.participant,
                        )
                    || token.contract_id != pointer.contract_id
                    || token.generation != pointer.generation
                    || token.terms_hash != *terms_hash
                {
                    return Err(Error::Invalid(
                        "listed token disagrees with the requested cleanup identity".into(),
                    ));
                }
                Ok((entry, token))
            })
            .collect()
    }
    async fn cleanup(
        &self,
        marker: StateEntry,
        report: &mut MaintenanceReport,
        audit: &dyn MaintenanceAudit,
    ) -> Result<()> {
        let record = checked_cleanup(&marker)?;
        let pointer = ContractPointer {
            contract_id: record.contract_id.clone(),
            generation: record.generation,
        };
        let contract_key = make_concord_contract_key(&pointer.contract_id, pointer.generation);
        // At most eight mutation passes, then one final exact emptiness check.
        for pass in 0..=8 {
            if self
                .io(
                    "check cleanup contract absence",
                    self.contracts.get(&contract_key),
                )
                .await?
                .is_some()
            {
                return Ok(());
            }
            if self
                .io(
                    "check cleanup marker ownership",
                    self.maintenance.get(&marker.key),
                )
                .await?
                .as_ref()
                != Some(&marker)
            {
                return Err(Error::StateConflict("cleanup marker changed".into()));
            }
            let entries = self.list_tokens(&pointer, &record.terms_hash).await?;
            if entries.is_empty() {
                self.io(
                    "complete token cleanup",
                    self.maintenance.delete(&marker.key, Some(marker.revision)),
                )
                .await?;
                report.cleanup_pending.remove(&marker.key);
                report.cleanups_completed += 1;
                return Ok(());
            }
            if pass == 8 {
                return Ok(());
            }
            for (entry, _) in entries {
                match self
                    .io(
                        "delete cleanup token",
                        self.tokens.delete(&entry.key, Some(entry.revision)),
                    )
                    .await
                {
                    Ok(()) => report.tokens_deleted += 1,
                    Err(error) if error.is_state_conflict() => {
                        self.audit(
                            audit,
                            MaintenanceAuditEvent::Conflict {
                                key: entry.key.clone(),
                                operation: "token cleanup".into(),
                            },
                        )
                        .await?;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }
}

fn contract_pointer(key: &str) -> Result<ContractPointer> {
    let (contract_id, generation) = parse_concord_contract_key(key)
        .ok_or_else(|| Error::Invalid("invalid contract key".into()))?;
    let pointer = ContractPointer {
        contract_id,
        generation,
    };
    pointer.validate()?;
    if key != make_concord_contract_key(&pointer.contract_id, pointer.generation) {
        return Err(Error::Invalid("noncanonical contract key".into()));
    }
    Ok(pointer)
}
fn check_contract_entry(entry: &StateEntry, pointer: &ContractPointer) -> Result<()> {
    // Maintenance must also cancel invalid_contract observations. A record
    // naming another generation is evaluated as invalid; all mutations and
    // token lookups stay anchored to the canonical requested storage key.
    if entry.key != make_concord_contract_key(&pointer.contract_id, pointer.generation)
        || !entry.value.is_object()
    {
        return Err(Error::Invalid(
            "exact contract read returned a different key or a non-object record".into(),
        ));
    }
    Ok(())
}
fn checked_stale(entry: &StateEntry, pointer: &ContractPointer) -> Result<StaleObservationRecord> {
    let record = StaleObservationRecord::from_value(entry.value.clone())?;
    if entry.key != concord_stale_observation_key(&pointer.contract_id, pointer.generation)
        || record.contract_id != pointer.contract_id
        || record.generation != pointer.generation
    {
        return Err(Error::Invalid(
            "stale observation identity disagrees".into(),
        ));
    }
    Ok(record)
}
fn checked_cleanup(entry: &StateEntry) -> Result<TokenCleanupRecord> {
    let pointer = marker_pointer(&entry.key, "cleanup")?;
    let record = TokenCleanupRecord::from_value(entry.value.clone())?;
    if record.contract_id != pointer.contract_id || record.generation != pointer.generation {
        return Err(Error::Invalid("cleanup marker identity disagrees".into()));
    }
    Ok(record)
}
fn elapsed(now: DateTime<Utc>, since: DateTime<Utc>, duration: Duration) -> bool {
    (now - since)
        .to_std()
        .is_ok_and(|elapsed| elapsed >= duration)
}
fn stale(validity: &ContractValidity) -> bool {
    use ContractValidityStatus::*;
    matches!(
        validity.status,
        MissingToken
            | InvalidToken
            | SessionMismatch
            | TermsHashMismatch
            | GenerationMismatch
            | InvalidContract
    ) || (validity.status == NotYetFulfilled && validity.tokens.is_empty())
}
fn raw_terms_hash(value: &Value) -> Result<Option<String>> {
    match value.get("termsHash") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(hash)) => {
            require_text(hash, "cleanup terms hash")?;
            Ok(Some(hash.clone()))
        }
        _ => Err(Error::Invalid("invalid retained contract termsHash".into())),
    }
}
fn deletion_audit(
    entry: &StateEntry,
    pointer: &ContractPointer,
    validity: &ContractValidity,
    tokens: &[(StateEntry, ParticipantTokenRecord)],
    terms_hash: Option<String>,
) -> Result<ContractDeletionAudit> {
    let value = &entry.value;
    let string = |key| value.get(key).and_then(Value::as_str).map(str::to_owned);
    let endpoints = |key| {
        value
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|value| {
                value
                    .as_str()
                    .and_then(|text| EndpointAddress::parse_protocol(text).ok())
            })
            .collect()
    };
    Ok(ContractDeletionAudit {
        contract: pointer.clone(),
        contract_revision: entry.revision,
        record_contract_id: string("contractId"),
        record_generation: value.get("generation").and_then(Value::as_u64),
        profile: string("profile"),
        participants: endpoints("participants"),
        attached_participants: endpoints("attachedParticipants"),
        created_at: string("createdAt"),
        created_by: string("createdBy"),
        cancelled_at: string("cancelledAt")
            .ok_or_else(|| Error::Invalid("missing cancellation timestamp".into()))?,
        cancelled_by: string("cancelledBy"),
        cancel_reason: string("cancelReason"),
        cancel_revision: value.get("cancelRevision").and_then(Value::as_u64),
        supersedes: value
            .get("supersedes")
            .cloned()
            .and_then(|v| serde_json::from_value::<ContractPointer>(v).ok())
            .filter(|p| p.validate().is_ok()),
        terms_hash,
        status: validity.status,
        reason: validity.reason_code,
        listed_token_keys: tokens.len(),
        tokens: tokens
            .iter()
            .map(|(entry, token)| MaintenanceTokenSummary {
                key: entry.key.clone(),
                revision: entry.revision,
                participant: token.participant.clone(),
                session_id: token.session_id.clone(),
                token_id: token.token_id.clone(),
                refresh_seq: token.refresh_seq,
                ttl_seconds: token.ttl_seconds,
                terms_hash: token.terms_hash.clone(),
            })
            .collect(),
    })
}
