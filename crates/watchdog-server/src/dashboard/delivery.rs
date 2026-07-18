use std::{fmt, sync::Arc};

use thiserror::Error;
use watchdog_domain::Clock;
use watchdog_store::{OutboxDestination, StoreError, WatchdogStore};

use super::{DashboardError, DashboardService};

/// Durable SSE outbox delivery that coalesces events into a current snapshot.
#[derive(Clone)]
pub struct DashboardOutboxDispatcher {
    store: WatchdogStore,
    dashboard: DashboardService,
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for DashboardOutboxDispatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DashboardOutboxDispatcher")
            .finish_non_exhaustive()
    }
}

impl DashboardOutboxDispatcher {
    /// Construct a dispatcher over the shared store and dashboard projection.
    #[must_use]
    pub fn new(store: WatchdogStore, dashboard: DashboardService, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            dashboard,
            clock,
        }
    }

    /// Publish one current snapshot and acknowledge a bounded pending SSE batch.
    ///
    /// Re-delivery after a partial acknowledgement is harmless because browser
    /// snapshots carry the latest durable revision.
    ///
    /// # Errors
    ///
    /// Returns [`DashboardOutboxError`] when the snapshot or store operation fails.
    pub async fn deliver_pending(&self, limit: u32) -> Result<usize, DashboardOutboxError> {
        let mut pending = self
            .store
            .pending_outbox_for(OutboxDestination::Sse, limit)
            .await?;
        pending.extend(
            self.store
                .pending_outbox_for(OutboxDestination::Browser, limit)
                .await?,
        );
        if pending.is_empty() {
            return Ok(0);
        }
        self.dashboard.publish().await?;
        let delivered_at = self.clock.now().wall_time();
        let mut delivered = 0;
        for entry in pending {
            if self
                .store
                .acknowledge_outbox(entry.outbox_id(), delivered_at)
                .await?
            {
                delivered += 1;
            }
        }
        Ok(delivered)
    }
}

/// Dashboard durable-delivery failure.
#[derive(Debug, Error)]
pub enum DashboardOutboxError {
    /// Durable outbox read or acknowledgement failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Current dashboard projection or serialization failed.
    #[error(transparent)]
    Dashboard(#[from] DashboardError),
}
