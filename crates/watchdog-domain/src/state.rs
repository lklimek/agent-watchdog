use serde::{Deserialize, Serialize};

/// Runtime-neutral detailed state exposed through MCP and JSON APIs.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DetailedState {
    /// Session was discovered but has not begun useful work.
    Starting,
    /// Session is performing or ready to perform work.
    Running,
    /// Session is waiting for a delegated agent.
    WaitingForAgent,
    /// Session is waiting for a tool or external operation.
    WaitingForTool,
    /// Session requires human input or approval.
    WaitingForUser,
    /// Session is alive but has no recent attributable activity.
    Idle,
    /// Session crossed its configured progress threshold.
    Stalled,
    /// Session completed successfully.
    Completed,
    /// Session completed with failure.
    Failed,
    /// Session was intentionally cancelled.
    Cancelled,
    /// Session runtime vanished without establishing the job's terminal outcome.
    Disappeared,
    /// Material sources conflict or cannot establish a trustworthy state.
    Unknown,
}

/// Compact dashboard state, independent from child aggregate badges.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactState {
    /// Starting or running.
    Active,
    /// Waiting for an agent, tool, or user.
    Waiting,
    /// Alive without recent attributable activity.
    Idle,
    /// Progress threshold crossed.
    Stalled,
    /// Successful or intentional terminal state.
    Finished,
    /// Failed or disappeared presentation state; disappearance remains outcome-uncertain.
    Failed,
    /// Trustworthy projection is unavailable.
    Unknown,
}

impl DetailedState {
    /// Project the detailed state into the dashboard vocabulary.
    #[must_use]
    pub const fn compact(self) -> CompactState {
        match self {
            Self::Starting | Self::Running => CompactState::Active,
            Self::WaitingForAgent | Self::WaitingForTool | Self::WaitingForUser => {
                CompactState::Waiting
            }
            Self::Idle => CompactState::Idle,
            Self::Stalled => CompactState::Stalled,
            Self::Completed | Self::Cancelled => CompactState::Finished,
            Self::Failed | Self::Disappeared => CompactState::Failed,
            Self::Unknown => CompactState::Unknown,
        }
    }

    /// Return whether elapsed inactivity may advance the stall timer.
    #[must_use]
    pub const fn stall_timer_runs(self) -> bool {
        !matches!(
            self,
            Self::WaitingForUser
                | Self::Completed
                | Self::Failed
                | Self::Cancelled
                | Self::Disappeared
                | Self::Unknown
        )
    }

    /// Return whether the child-only termination timer may advance.
    #[must_use]
    pub const fn termination_timer_runs(self) -> bool {
        matches!(self, Self::Stalled)
    }

    /// Return whether the session already established its final outcome.
    ///
    /// `Disappeared` is excluded: its outcome stayed unknown, so the session may
    /// still be reunited with a runtime that reappears under a new native ID.
    #[must_use]
    pub const fn outcome_established(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}
