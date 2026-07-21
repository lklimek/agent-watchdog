use std::collections::BTreeMap;

use watchdog_domain::{RuntimeKind, SessionIdentity};

/// Independently supervised service component.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ComponentId {
    /// Runtime-specific discovery and state adapter.
    Adapter(RuntimeKind),
    /// Linux process evidence sampler.
    ProcessSampler,
    /// Transactional persistence.
    Store,
    /// Pure state-reduction lane.
    Reducer,
    /// MCP and HTTP authorization boundary.
    Authorization,
    /// Filesystem invalidation watcher.
    Watcher,
    /// Bounded reconciliation after filesystem uncertainty.
    FilesystemReconciliation,
    /// Per-session observation admission queue.
    ObservationQueue,
    /// Durable dashboard outbox and live snapshot delivery.
    DashboardDelivery,
    /// Best-effort human webhook delivery.
    Notifications,
    /// Conservative child-only termination scheduler.
    TerminationAutomation,
}

impl ComponentId {
    const fn is_critical(self) -> bool {
        matches!(
            self,
            Self::ProcessSampler | Self::Store | Self::Reducer | Self::Authorization
        )
    }
}

/// Current bounded health classification.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ComponentStatus {
    /// Component completed its latest reconciliation successfully.
    Healthy,
    /// Best-effort monitoring continues with explicit uncertainty.
    Degraded,
    /// Component cannot perform its required function.
    Failed,
}

/// Sessions affected by one component health report.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum HealthScope {
    /// Every monitored session.
    Global,
    /// Sessions from one runtime.
    Runtime(RuntimeKind),
    /// One normalized session only.
    Session(SessionIdentity),
}

/// One typed component status update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComponentHealth {
    component: ComponentId,
    status: ComponentStatus,
    scope: HealthScope,
}

impl ComponentHealth {
    /// Construct a typed health update.
    #[must_use]
    pub const fn new(component: ComponentId, status: ComponentStatus, scope: HealthScope) -> Self {
        Self {
            component,
            status,
            scope,
        }
    }
}

/// Latest health by component and affected scope.
#[derive(Clone, Debug, Default)]
pub struct HealthRegistry {
    reports: BTreeMap<(ComponentId, HealthScope), ComponentStatus>,
}

impl HealthRegistry {
    /// Replace the latest report for the component and scope.
    pub fn record(&mut self, health: ComponentHealth) {
        self.reports
            .insert((health.component, health.scope), health.status);
    }

    /// Return the latest exact component and scope report.
    #[must_use]
    pub fn status(&self, component: ComponentId, scope: HealthScope) -> Option<ComponentStatus> {
        self.reports.get(&(component, scope)).copied()
    }

    /// Critical global failures fail readiness; isolated adapters remain contained.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        !self.reports.iter().any(|((component, scope), status)| {
            component.is_critical()
                && *scope == HealthScope::Global
                && *status == ComponentStatus::Failed
        })
    }

    /// Whether every health gate affecting a session permits destructive automation.
    #[must_use]
    pub fn destructive_automation_allowed(
        &self,
        runtime: RuntimeKind,
        session: SessionIdentity,
    ) -> bool {
        [
            ComponentId::Store,
            ComponentId::Watcher,
            ComponentId::FilesystemReconciliation,
            ComponentId::ObservationQueue,
            ComponentId::ProcessSampler,
            ComponentId::Adapter(runtime),
        ]
        .into_iter()
        .all(|required| {
            self.reports
                .iter()
                .filter(|((component, scope), _status)| {
                    *component == required && scope_applies(*scope, runtime, session)
                })
                .map(|(_key, status)| *status)
                .max()
                == Some(ComponentStatus::Healthy)
        })
    }
}

fn scope_applies(scope: HealthScope, runtime: RuntimeKind, session: SessionIdentity) -> bool {
    match scope {
        HealthScope::Global => true,
        HealthScope::Runtime(affected) => affected == runtime,
        HealthScope::Session(affected) => affected == session,
    }
}
