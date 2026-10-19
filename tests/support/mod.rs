use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use deckr::lifecycle::{Clock, Sleep};
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub struct ManualClock {
    start: Instant,
    utc: DateTime<Utc>,
    elapsed: Arc<Mutex<Duration>>,
    changed: Arc<Notify>,
}

impl Default for ManualClock {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            utc: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            elapsed: Arc::default(),
            changed: Arc::default(),
        }
    }
}

impl ManualClock {
    pub fn advance(&self, duration: Duration) {
        *self.elapsed.lock().unwrap() += duration;
        self.changed.notify_waiters();
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.start + *self.elapsed.lock().unwrap()
    }
    fn utc_now(&self) -> DateTime<Utc> {
        self.utc + chrono::Duration::from_std(*self.elapsed.lock().unwrap()).unwrap()
    }
    fn sleep_until(&self, deadline: Instant) -> Sleep<'_> {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.now() >= deadline {
                    return;
                }
                changed.await;
            }
        })
    }
    fn jitter(&self) -> f64 {
        0.5
    }
}
