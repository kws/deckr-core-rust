//! Optional host-owned recurring scans; never part of agreement readiness.

use super::*;
use crate::lifecycle::{Cancellation, Phase, Snapshot, StateCell, StateWatch};
use tokio::sync::Notify;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MaintenanceState {
    pub scans_completed: u64,
    pub scanning: bool,
    pub last_report: Option<MaintenanceReport>,
}
struct Owner {
    stop: Cancellation,
    wake: Arc<Notify>,
    state: StateCell<MaintenanceState>,
    clock: Arc<dyn Clock>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
/// Explicitly started by the host. Closing cancels pending I/O and audit work;
/// durable cleanup markers are retained for a later scan. Borrowed stores stay open.
#[derive(Clone)]
pub struct ManagedConcordMaintenance {
    owner: Arc<Owner>,
}
impl<C: StateStore, T: StateStore, M: StateStore> ConcordMaintenance<C, T, M> {
    pub fn manage(
        &self,
        audit: Arc<dyn MaintenanceAudit>,
        interval: Duration,
    ) -> Result<ManagedConcordMaintenance> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("managed maintenance requires a Tokio runtime".into()))?;
        if interval.is_zero() || self.clock.now().checked_add(interval).is_none() {
            return Err(Error::Invalid(
                "maintenance scan interval is out of range".into(),
            ));
        }
        let stop = Cancellation::default();
        let wake = Arc::new(Notify::new());
        let state = StateCell::new(MaintenanceState::default());
        let owner = Arc::new(Owner {
            stop: stop.clone(),
            wake: wake.clone(),
            state: state.clone(),
            clock: self.clock.clone(),
        });
        let capability = self.clone();
        tokio::spawn(async move {
            let mut current = MaintenanceState::default();
            loop {
                if stop.is_cancelled() {
                    break;
                }
                current.scanning = true;
                publish_running(&state, Phase::Starting, false, current.clone(), None);
                let result = tokio::select! { biased;
                    _=stop.cancelled()=>break,
                    result=capability.scan(audit.as_ref())=>result,
                };
                current.scanning = false;
                let failure = match result {
                    Ok(report) => {
                        current.scans_completed = current.scans_completed.saturating_add(1);
                        let failure = report
                            .failures
                            .first()
                            .map(|failure| failure.failure.clone());
                        current.last_report = Some(report);
                        failure
                    }
                    Err(error) => Some(Failure::from_error("Concord maintenance scan", &error)),
                };
                publish_running(
                    &state,
                    if failure.is_some() {
                        Phase::Unavailable
                    } else {
                        Phase::Ready
                    },
                    failure.is_none(),
                    current.clone(),
                    failure,
                );
                let Some(next) = capability.clock.now().checked_add(interval) else {
                    break;
                };
                tokio::select! { biased;
                    _=stop.cancelled()=>break,
                    _=wake.notified()=>{},
                    _=capability.clock.sleep_until(next)=>{},
                }
            }
            current.scanning = false;
            state.publish(Phase::Closed, false, current, None, BTreeSet::new());
        });
        Ok(ManagedConcordMaintenance { owner })
    }
}
fn publish_running(
    state: &StateCell<MaintenanceState>,
    phase: Phase,
    current: bool,
    value: MaintenanceState,
    failure: Option<Failure>,
) {
    state.modify(|snapshot| {
        if matches!(snapshot.phase, Phase::Closing | Phase::Closed) {
            return false;
        }
        snapshot.phase = phase;
        snapshot.current = current;
        snapshot.value = value;
        snapshot.failure = failure;
        snapshot.changed.clear();
        true
    });
}
impl ManagedConcordMaintenance {
    pub fn state(&self) -> Snapshot<MaintenanceState> {
        self.owner.state.snapshot()
    }
    pub fn watch_state(&self) -> Result<StateWatch<MaintenanceState>> {
        self.owner.state.watch()
    }
    pub async fn wait_for(
        &self,
        predicate: impl FnMut(&Snapshot<MaintenanceState>) -> bool,
        timeout: Option<Duration>,
    ) -> Result<Snapshot<MaintenanceState>> {
        self.owner
            .state
            .wait_for(predicate, timeout, self.owner.clock.as_ref())
            .await
    }
    pub fn request_scan(&self) -> Result<()> {
        if self.owner.stop.is_cancelled() {
            return Err(Error::Closed("Concord maintenance closed".into()));
        }
        self.owner.wake.notify_one();
        Ok(())
    }
    pub fn request_close(&self) {
        self.owner.stop.cancel();
        self.owner.state.modify(|state| {
            if matches!(state.phase, Phase::Closing | Phase::Closed) {
                return false;
            }
            state.phase = Phase::Closing;
            state.current = false;
            true
        });
    }
    pub async fn close(&self) -> Result<()> {
        self.request_close();
        self.owner.state.wait_closed().await;
        Ok(())
    }
}
