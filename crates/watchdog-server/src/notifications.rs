use std::{fmt, sync::Arc, time::Duration};

use reqwest::{Client, Url, redirect, retry};
use serde::Serialize;
use thiserror::Error;
use watchdog_domain::{
    BoundedText, Clock, DetailedState, DomainEvent, DomainEventKind, DomainInputError, EventId,
    SessionIdentity, TerminationStage,
};
use watchdog_store::{
    NotificationAttemptRecord, NotificationChannel, NotificationOutcome, OutboxDestination,
    PendingOutboxEntry, StoreError, WatchdogStore,
};

const MAX_WEBHOOK_URL_BYTES: usize = 4_096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(5);
const ATTEMPT_LOOKBACK: u32 = 100;

/// Human-facing event payload that intentionally excludes agent diagnostics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HumanNotification {
    issue: BoundedText<128>,
    title: BoundedText<512>,
    startup_directory: BoundedText<4_096>,
}

impl HumanNotification {
    /// Construct the only fields permitted on human notification channels.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] for empty or oversized fields.
    pub fn new(
        issue: impl Into<String>,
        title: impl Into<String>,
        startup_directory: impl Into<String>,
    ) -> Result<Self, DomainInputError> {
        let issue = BoundedText::new("notification_issue", issue)?;
        let title = BoundedText::new("notification_title", title)?;
        let startup_directory =
            BoundedText::new("notification_startup_directory", startup_directory)?;
        for (field, value) in [
            ("notification_issue", issue.as_str()),
            ("notification_title", title.as_str()),
            ("notification_startup_directory", startup_directory.as_str()),
        ] {
            if value.is_empty() {
                return Err(DomainInputError::Empty { field });
            }
        }
        Ok(Self {
            issue,
            title,
            startup_directory,
        })
    }
}

/// Validated operator-controlled HTTP(S) webhook destination with redacted Debug.
#[derive(Clone)]
pub struct WebhookEndpoint(Url);

impl WebhookEndpoint {
    /// Parse a bounded HTTP(S) destination without permitting URL userinfo.
    ///
    /// Private-network hosts and secret path/query components are intentionally
    /// supported for Home Assistant and operator-managed integrations.
    ///
    /// # Errors
    ///
    /// Returns [`NotificationConfigError`] for invalid or unsupported URLs.
    pub fn new(value: impl AsRef<str>) -> Result<Self, NotificationConfigError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(NotificationConfigError::EmptyEndpoint);
        }
        if value.len() > MAX_WEBHOOK_URL_BYTES {
            return Err(NotificationConfigError::EndpointTooLong);
        }
        let url = Url::parse(value).map_err(|_| NotificationConfigError::InvalidEndpoint)?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(NotificationConfigError::UnsupportedEndpoint);
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(NotificationConfigError::EndpointUserinfo);
        }
        Ok(Self(url))
    }
}

impl fmt::Debug for WebhookEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WebhookEndpoint([REDACTED])")
    }
}

/// Optional one-attempt human webhook destinations.
#[derive(Clone, Debug, Default)]
pub struct NotificationEndpoints {
    home_assistant: Option<WebhookEndpoint>,
    webhook: Option<WebhookEndpoint>,
}

impl NotificationEndpoints {
    /// Construct independently optional Home Assistant and generic endpoints.
    #[must_use]
    pub const fn new(
        home_assistant: Option<WebhookEndpoint>,
        webhook: Option<WebhookEndpoint>,
    ) -> Self {
        Self {
            home_assistant,
            webhook,
        }
    }
}

/// Terminal result for one configured human-notification channel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NotificationDelivery {
    /// Attempted channel.
    pub channel: NotificationChannel,
    /// Delivered, failed, or timed out without retry.
    pub outcome: NotificationOutcome,
}

/// One-attempt human webhook delivery with durable attempt auditing.
#[derive(Clone)]
pub struct HumanNotifier {
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
    endpoints: NotificationEndpoints,
    client: Client,
}

impl HumanNotifier {
    /// Construct a no-redirect, no-retry HTTP client with bounded timeouts.
    ///
    /// # Errors
    ///
    /// Returns [`NotificationConfigError`] if the TLS/client backend cannot initialize.
    pub fn new(
        store: WatchdogStore,
        clock: Arc<dyn Clock>,
        endpoints: NotificationEndpoints,
    ) -> Result<Self, NotificationConfigError> {
        let client = Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(TOTAL_TIMEOUT)
            .redirect(redirect::Policy::none())
            .retry(retry::never())
            .no_proxy()
            .user_agent("agent-watchdog/0.1")
            .build()
            .map_err(|_| NotificationConfigError::ClientInitialization)?;
        Ok(Self {
            store,
            clock,
            endpoints,
            client,
        })
    }

    /// Attempt every configured channel once and durably record each outcome.
    ///
    /// Network failures are successful best-effort attempts with a failed or
    /// timed-out outcome. They never enter a retry queue.
    ///
    /// # Errors
    ///
    /// Returns [`NotificationDeliveryError`] if the audit record cannot persist.
    pub async fn deliver(
        &self,
        event_id: EventId,
        notification: &HumanNotification,
    ) -> Result<Vec<NotificationDelivery>, NotificationDeliveryError> {
        let mut deliveries = Vec::with_capacity(2);
        for (channel, endpoint) in [
            (
                NotificationChannel::HomeAssistant,
                self.endpoints.home_assistant.as_ref(),
            ),
            (
                NotificationChannel::Webhook,
                self.endpoints.webhook.as_ref(),
            ),
        ] {
            let Some(endpoint) = endpoint else {
                continue;
            };
            let (outcome, message) = self.attempt(endpoint, notification).await;
            self.store
                .record_notification_attempt(&NotificationAttemptRecord {
                    event_id,
                    channel,
                    attempted_at: self.clock.now().wall_time(),
                    outcome,
                    message: message
                        .map(|message| BoundedText::new("notification_result", message))
                        .transpose()?,
                })
                .await?;
            deliveries.push(NotificationDelivery { channel, outcome });
        }
        Ok(deliveries)
    }

    async fn deliver_channel(
        &self,
        event_id: EventId,
        channel: NotificationChannel,
        notification: &HumanNotification,
    ) -> Result<Option<NotificationDelivery>, NotificationDeliveryError> {
        let endpoint = match channel {
            NotificationChannel::HomeAssistant => self.endpoints.home_assistant.as_ref(),
            NotificationChannel::Webhook => self.endpoints.webhook.as_ref(),
            NotificationChannel::Browser => None,
        };
        let Some(endpoint) = endpoint else {
            return Ok(None);
        };
        let (outcome, message) = self.attempt(endpoint, notification).await;
        self.store
            .record_notification_attempt(&NotificationAttemptRecord {
                event_id,
                channel,
                attempted_at: self.clock.now().wall_time(),
                outcome,
                message: message
                    .map(|message| BoundedText::new("notification_result", message))
                    .transpose()?,
            })
            .await?;
        Ok(Some(NotificationDelivery { channel, outcome }))
    }

    async fn attempt(
        &self,
        endpoint: &WebhookEndpoint,
        notification: &HumanNotification,
    ) -> (NotificationOutcome, Option<String>) {
        match self
            .client
            .post(endpoint.0.clone())
            .json(notification)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                (NotificationOutcome::Delivered, None)
            }
            Ok(response) => (
                NotificationOutcome::Failed,
                Some(format!(
                    "Webhook returned HTTP {}",
                    response.status().as_u16()
                )),
            ),
            Err(error) if error.is_timeout() => (
                NotificationOutcome::TimedOut,
                Some("Webhook attempt timed out".to_owned()),
            ),
            Err(_) => (
                NotificationOutcome::Failed,
                Some("Webhook request failed".to_owned()),
            ),
        }
    }
}

/// Durable one-shot delivery worker for Home Assistant and generic webhooks.
#[derive(Clone)]
pub struct HumanOutboxDispatcher {
    store: WatchdogStore,
    clock: Arc<dyn Clock>,
    notifier: HumanNotifier,
}

impl HumanOutboxDispatcher {
    /// Construct a dispatcher over the durable human-channel outbox.
    #[must_use]
    pub fn new(store: WatchdogStore, clock: Arc<dyn Clock>, notifier: HumanNotifier) -> Self {
        Self {
            store,
            clock,
            notifier,
        }
    }

    /// Deliver and acknowledge a bounded batch for both webhook channels.
    ///
    /// An existing channel audit record suppresses delivery after restart.
    /// Disabled channels are acknowledged without creating a network attempt.
    ///
    /// # Errors
    ///
    /// Returns [`NotificationDeliveryError`] for corrupt outbox payloads,
    /// missing session projections, or audit/acknowledgement failures.
    pub async fn deliver_pending(&self, limit: u32) -> Result<usize, NotificationDeliveryError> {
        let mut delivered = 0;
        for (destination, channel) in [
            (
                OutboxDestination::HomeAssistant,
                NotificationChannel::HomeAssistant,
            ),
            (OutboxDestination::Webhook, NotificationChannel::Webhook),
        ] {
            for entry in self.store.pending_outbox_for(destination, limit).await? {
                self.deliver_entry(&entry, channel).await?;
                if self
                    .store
                    .acknowledge_outbox(entry.outbox_id(), self.clock.now().wall_time())
                    .await?
                {
                    delivered += 1;
                }
            }
        }
        Ok(delivered)
    }

    async fn deliver_entry(
        &self,
        entry: &PendingOutboxEntry,
        channel: NotificationChannel,
    ) -> Result<(), NotificationDeliveryError> {
        if self
            .store
            .notification_attempts(entry.event_id(), ATTEMPT_LOOKBACK)
            .await?
            .iter()
            .any(|attempt| attempt.channel == channel)
        {
            return Ok(());
        }
        let event: DomainEvent = serde_json::from_slice(entry.payload())
            .map_err(|_| NotificationDeliveryError::InvalidOutboxPayload)?;
        let notification = self.notification_for(&event).await?;
        self.notifier
            .deliver_channel(entry.event_id(), channel, &notification)
            .await?;
        Ok(())
    }

    async fn notification_for(
        &self,
        event: &DomainEvent,
    ) -> Result<HumanNotification, NotificationDeliveryError> {
        let session = SessionIdentity::Main(event.root());
        let record = self
            .store
            .session(session)
            .await?
            .ok_or(NotificationDeliveryError::MissingMainSession)?;
        let metadata = self.store.session_metadata(session).await?;
        let startup_directory = metadata
            .as_ref()
            .and_then(watchdog_store::SessionMetadataRecord::startup_directory)
            .unwrap_or("(unknown startup directory)")
            .to_owned();
        let title = metadata
            .as_ref()
            .and_then(watchdog_store::SessionMetadataRecord::title)
            .map_or_else(
                || fallback_title(&startup_directory, record.native.native_id()),
                ToOwned::to_owned,
            );
        HumanNotification::new(issue(event.kind()), title, startup_directory).map_err(Into::into)
    }
}

impl fmt::Debug for HumanOutboxDispatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HumanOutboxDispatcher")
            .finish_non_exhaustive()
    }
}

fn issue(kind: &DomainEventKind) -> &'static str {
    match kind {
        DomainEventKind::StateChanged {
            to: DetailedState::Completed,
            ..
        } => "Session completed",
        DomainEventKind::StateChanged {
            to: DetailedState::WaitingForUser,
            ..
        } => "Session is waiting for user",
        DomainEventKind::AlertDue => "Session requires attention",
        DomainEventKind::ReminderDue => "Session still requires attention",
        DomainEventKind::TerminationChanged {
            stage: TerminationStage::WarningGrace,
            ..
        } => "Sub-agent termination warning",
        _ => "Session status changed",
    }
}

fn fallback_title(startup_directory: &str, native_id: &str) -> String {
    std::path::Path::new(startup_directory)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(native_id)
        .to_owned()
}

impl fmt::Debug for HumanNotifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HumanNotifier")
            .field("endpoints", &self.endpoints)
            .finish_non_exhaustive()
    }
}

/// Invalid human-notification endpoint or client configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum NotificationConfigError {
    /// Empty endpoint values are not usable destinations.
    #[error("Webhook endpoint must not be empty")]
    EmptyEndpoint,
    /// Endpoint inputs are bounded before parsing.
    #[error("Webhook endpoint is too long")]
    EndpointTooLong,
    /// URL syntax is invalid.
    #[error("Webhook endpoint is invalid")]
    InvalidEndpoint,
    /// Only HTTP(S) destinations with a host are supported.
    #[error("Webhook endpoint must be an HTTP(S) URL with a host")]
    UnsupportedEndpoint,
    /// URL userinfo is disallowed because it is easily leaked by intermediaries.
    #[error("Webhook endpoint must not contain URL userinfo")]
    EndpointUserinfo,
    /// Reqwest/TLS client initialization failed without exposing configuration.
    #[error("Webhook HTTP client could not initialize")]
    ClientInitialization,
}

/// Human-notification audit persistence failure.
#[derive(Debug, Error)]
pub enum NotificationDeliveryError {
    /// A bounded diagnostic unexpectedly failed validation.
    #[error(transparent)]
    Input(#[from] DomainInputError),
    /// The terminal one-shot audit record could not persist.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A durable outbox record was not a valid bounded domain event.
    #[error("Human notification outbox payload is invalid")]
    InvalidOutboxPayload,
    /// The event's owning main-session projection is missing.
    #[error("Human notification main session is missing")]
    MissingMainSession,
}
