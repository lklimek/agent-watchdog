use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use axum::{
    Json, Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Serialize;
use watchdog_domain::{BoundedText, Clock, RuntimeKind, SessionIdentity};
use watchdog_runtime::{
    ComponentHealth, ComponentId, ComponentStatus, HealthRegistry, HealthScope,
};

use crate::BasicAuthenticator;

const MAX_HEALTH_MESSAGE_BYTES: usize = 1_024;

/// Operator-facing overall or component health level.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthLevel {
    /// Component completed its latest operation successfully.
    Healthy,
    /// Best-effort operation continues with an actionable warning.
    Degraded,
    /// A critical function is unavailable.
    Failed,
}

/// One bounded component report in the authenticated health response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HealthComponentView {
    /// Stable subsystem identifier.
    pub component: String,
    /// Latest bounded health level.
    pub status: HealthLevel,
    /// Whether a failure makes overall readiness false.
    pub critical: bool,
    /// Latest update wall time in Unix milliseconds.
    pub updated_at_ms: i64,
    /// Actionable bounded message without native payload or secrets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Authenticated detailed health projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HealthSnapshot {
    /// Aggregate status across all recorded components and configuration reload.
    pub status: HealthLevel,
    /// False only when a critical global component has failed.
    pub ready: bool,
    /// Stable component reports sorted by subsystem identity.
    pub components: Vec<HealthComponentView>,
    /// Most recent rejected configuration reload, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration_warning: Option<String>,
}

#[derive(Clone, Debug)]
struct HealthEntry {
    updated_at_ms: i64,
    message: Option<BoundedText<MAX_HEALTH_MESSAGE_BYTES>>,
}

#[derive(Debug, Default)]
struct HealthState {
    registry: HealthRegistry,
    global_components: BTreeMap<ComponentId, HealthEntry>,
}

/// Shared bounded health registry for HTTP reporting and task supervision.
#[derive(Clone)]
pub struct HealthService {
    clock: Arc<dyn Clock>,
    state: Arc<RwLock<HealthState>>,
    configuration_warning: Arc<RwLock<Option<BoundedText<MAX_HEALTH_MESSAGE_BYTES>>>>,
}

impl std::fmt::Debug for HealthService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HealthService")
            .field(
                "component_count",
                &self.read_state().global_components.len(),
            )
            .finish_non_exhaustive()
    }
}

impl HealthService {
    /// Construct an initially empty registry over the process clock.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            state: Arc::new(RwLock::new(HealthState::default())),
            configuration_warning: Arc::new(RwLock::new(None)),
        }
    }

    /// Replace one component's global report.
    pub fn record(&self, component: ComponentId, status: ComponentStatus, message: Option<&str>) {
        self.record_scoped(component, status, HealthScope::Global, message);
    }

    /// Replace one component report for a global, runtime, or session scope.
    pub fn record_scoped(
        &self,
        component: ComponentId,
        status: ComponentStatus,
        scope: HealthScope,
        message: Option<&str>,
    ) {
        let message = message.and_then(|message| {
            BoundedText::new("health_message", message)
                .ok()
                .or_else(|| {
                    BoundedText::new(
                        "health_message",
                        "Component error exceeded the reporting limit",
                    )
                    .ok()
                })
        });
        let mut state = self.write_state();
        state
            .registry
            .record(ComponentHealth::new(component, status, scope));
        if scope == HealthScope::Global {
            state.global_components.insert(
                component,
                HealthEntry {
                    updated_at_ms: self.clock.now().wall_time().value(),
                    message,
                },
            );
        }
    }

    /// Read the latest global component status for safety-gate evaluation.
    #[must_use]
    pub fn component_status(&self, component: ComponentId) -> Option<ComponentStatus> {
        self.read_state()
            .registry
            .status(component, HealthScope::Global)
    }

    /// Whether all required scoped health reports permit destructive automation.
    #[must_use]
    pub fn destructive_automation_allowed(
        &self,
        runtime: RuntimeKind,
        session: SessionIdentity,
    ) -> bool {
        self.read_state()
            .registry
            .destructive_automation_allowed(runtime, session)
    }

    /// Replace or clear the last rejected configuration reload warning.
    pub fn record_configuration_warning(&self, warning: Option<&str>) {
        *self.write_configuration_warning() = warning.and_then(|warning| {
            BoundedText::new("configuration_warning", warning)
                .ok()
                .or_else(|| {
                    BoundedText::new(
                        "configuration_warning",
                        "Configuration reload failed with an oversized diagnostic",
                    )
                    .ok()
                })
        });
    }

    /// Build a deterministic detailed health snapshot.
    #[must_use]
    pub fn snapshot(&self) -> HealthSnapshot {
        let state = self.read_state();
        let components = state
            .global_components
            .iter()
            .map(|(component, entry)| HealthComponentView {
                component: component_name(*component),
                status: level(
                    state
                        .registry
                        .status(*component, HealthScope::Global)
                        .unwrap_or(ComponentStatus::Failed),
                ),
                critical: critical(*component),
                updated_at_ms: entry.updated_at_ms,
                message: entry
                    .message
                    .as_ref()
                    .map(|message| message.as_str().to_owned()),
            })
            .collect::<Vec<_>>();
        let configuration_warning = self
            .read_configuration_warning()
            .as_ref()
            .map(|warning| warning.as_str().to_owned());
        let ready = state.registry.is_ready();
        let status = if !ready {
            HealthLevel::Failed
        } else if configuration_warning.is_some()
            || components
                .iter()
                .any(|component| component.status != HealthLevel::Healthy)
        {
            HealthLevel::Degraded
        } else {
            HealthLevel::Healthy
        };
        HealthSnapshot {
            status,
            ready,
            components,
            configuration_warning,
        }
    }

    fn read_state(&self) -> RwLockReadGuard<'_, HealthState> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, HealthState> {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read_configuration_warning(
        &self,
    ) -> RwLockReadGuard<'_, Option<BoundedText<MAX_HEALTH_MESSAGE_BYTES>>> {
        self.configuration_warning
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_configuration_warning(
        &self,
    ) -> RwLockWriteGuard<'_, Option<BoundedText<MAX_HEALTH_MESSAGE_BYTES>>> {
        self.configuration_warning
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Build minimal public liveness and authenticated detailed health routes.
pub fn health_router(service: HealthService, authenticator: BasicAuthenticator) -> Router {
    let detailed = Router::new()
        .route("/health", get(health))
        .with_state(service)
        .layer(middleware::from_fn(
            move |request: Request<Body>, next: Next| {
                let authenticator = authenticator.clone();
                async move {
                    let authorized = request
                        .headers()
                        .get(header::AUTHORIZATION)
                        .is_some_and(|value| authenticator.authorize(Some(value.as_bytes())));
                    if !authorized {
                        tracing::warn!(
                            event = "auth.rejected",
                            route = "/health",
                            "Health endpoint basic credential rejected"
                        );
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    next.run(request).await
                }
            },
        ));
    Router::new()
        .route("/health/live", get(liveness))
        .merge(detailed)
        .layer(middleware::from_fn(no_store))
}

async fn liveness() -> &'static str {
    "ok"
}

async fn health(State(service): State<HealthService>) -> Response {
    let snapshot = service.snapshot();
    let status = if snapshot.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(snapshot)).into_response()
}

async fn no_store(request: Request<Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn level(status: ComponentStatus) -> HealthLevel {
    match status {
        ComponentStatus::Healthy => HealthLevel::Healthy,
        ComponentStatus::Degraded => HealthLevel::Degraded,
        ComponentStatus::Failed => HealthLevel::Failed,
    }
}

fn critical(component: ComponentId) -> bool {
    matches!(
        component,
        ComponentId::ProcessSampler
            | ComponentId::Store
            | ComponentId::Reducer
            | ComponentId::Authorization
    )
}

fn component_name(component: ComponentId) -> String {
    match component {
        ComponentId::Adapter(runtime) => format!("adapter.{}", runtime.as_str()),
        ComponentId::ProcessSampler => "process_sampler".to_owned(),
        ComponentId::Store => "store".to_owned(),
        ComponentId::Reducer => "reducer".to_owned(),
        ComponentId::Authorization => "authorization".to_owned(),
        ComponentId::Watcher => "watcher".to_owned(),
        ComponentId::FilesystemReconciliation => "filesystem_reconciliation".to_owned(),
        ComponentId::ObservationQueue => "observation_queue".to_owned(),
        ComponentId::DashboardDelivery => "dashboard_delivery".to_owned(),
        ComponentId::Notifications => "notifications".to_owned(),
        ComponentId::TerminationAutomation => "termination_automation".to_owned(),
    }
}
