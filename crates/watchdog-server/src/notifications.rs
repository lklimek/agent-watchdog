use std::{fmt, sync::Arc, time::Duration};

use reqwest::{Client, Url, redirect, retry};
use serde::Serialize;
use thiserror::Error;
use watchdog_domain::{BoundedText, Clock, DomainInputError, EventId};
use watchdog_store::{
    NotificationAttemptRecord, NotificationChannel, NotificationOutcome, StoreError, WatchdogStore,
};

const MAX_WEBHOOK_URL_BYTES: usize = 4_096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(5);

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
}
