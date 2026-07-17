//! Native Codex CLI observation adapter.

mod app_server;
mod hooks;
mod rollout;
mod state;

pub use app_server::{
    CodexAppServerParser, CodexEventEvidence, CodexParseError, MAX_APP_SERVER_BYTES,
};
pub use hooks::{CodexHookEvidence, CodexHookParser, MAX_HOOK_BYTES};
pub use rollout::{CodexRolloutEvidence, CodexRolloutParser, MAX_ROLLOUT_RECORD_BYTES};
pub use state::{CodexStateError, CodexStateReader, CodexThread};
