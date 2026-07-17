use maud::{DOCTYPE, Markup, html};
use watchdog_domain::{CompactState, DetailedState};

use super::model::{DashboardCard, DashboardScope, DashboardSnapshot, DashboardSort};

pub(super) fn dashboard(snapshot: &DashboardSnapshot) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                meta name="color-scheme" content="light dark";
                title { "Agent Watchdog" }
                link rel="stylesheet" href="/ui/assets/app.css";
                script defer src="/ui/assets/app.js" {}
            }
            body {
                a class="skip-link" href="#sessions" { "Skip to sessions" }
                header class="site-header" {
                    div {
                        h1 { "Agent Watchdog" }
                        p { "Monitoring sub-agents in multi-agent orchestration sessions" }
                    }
                    div class="header-actions" {
                        span id="connection-status" class="connection live" role="status" aria-live="polite" {
                            span aria-hidden="true" { "●" }
                            " Live"
                        }
                        button id="notifications" type="button" { "Notifications" }
                    }
                }
                @if !snapshot.warnings.is_empty() {
                    aside class="page-warnings" aria-label="Monitoring warnings" {
                        @for warning in &snapshot.warnings {
                            p {
                                strong { (warning.badge) }
                                " " (warning.message)
                            }
                        }
                    }
                }
                form class="controls" action="/ui" method="get" aria-label="Dashboard view" {
                    label for="scope" { "Show" }
                    select id="scope" name="scope" {
                        option value="active" selected[snapshot.scope == DashboardScope::Active] { "Active sessions" }
                        option value="all" selected[snapshot.scope == DashboardScope::All] { "All retained sessions" }
                    }
                    label for="sort" { "Sort" }
                    select id="sort" name="sort" {
                        option value="attention" selected[snapshot.sort == DashboardSort::Attention] { "Attention first" }
                        option value="directory" selected[snapshot.sort == DashboardSort::Directory] { "Directory A–Z" }
                    }
                    button class="apply" type="submit" { "Apply" }
                }
                main id="sessions" tabindex="-1" {
                    h2 class="visually-hidden" { "Orchestration sessions" }
                    p id="session-summary" class="session-summary" aria-live="polite" {
                        (session_summary(snapshot.sessions.len(), snapshot.scope))
                    }
                    div id="session-list" class="session-list" data-revision=(snapshot.revision) {
                        @if snapshot.sessions.is_empty() {
                            (empty_state(snapshot.scope))
                        } @else {
                            @for card in &snapshot.sessions {
                                (session_card(card, snapshot.generated_at_ms))
                            }
                        }
                    }
                }
                div id="urgent-updates" class="visually-hidden" aria-live="assertive" aria-atomic="true" {}
                noscript {
                    p class="noscript" { "Live updates require JavaScript. This durable snapshot remains available." }
                }
            }
        }
    }
}

fn session_card(card: &DashboardCard, generated_at_ms: i64) -> Markup {
    let (glyph, label) = state_presentation(card.state);
    let age = relative_age(generated_at_ms, card.last_activity_ms);
    html! {
        article class=(format!("session-card state-{}", compact_name(card.compact_state))) data-session-id=(card.session_id.session_id()) {
            div class="state-row" {
                p class="state-label" {
                    span class="state-glyph" aria-hidden="true" { (glyph) }
                    (label)
                    @if let Some(warning) = &card.warning {
                        span class="warning-badge" title=(warning.message) aria-label=(warning.message) {
                            (warning.badge)
                        }
                    }
                }
                p class="age" { "Last activity " (age) }
            }
            h3 { (card.title) }
            p class="directory" { (card.startup_directory) }
            p class="repository" {
                @if let (Some(number), Some(url)) = (card.pull_request_number, &card.pull_request_url) {
                    a href=(url) rel="noopener noreferrer" { "PR #" (number) " ↗" }
                } @else if let Some(branch) = &card.branch {
                    (branch)
                } @else {
                    "Branch unavailable"
                }
            }
            (child_summary(card))
        }
    }
}

fn child_summary(card: &DashboardCard) -> Markup {
    let total: u32 = card.child_counts.values().sum();
    html! {
        div class="children" aria-label=(format!("{total} sub-agents")) {
            strong { (total) " " (plural(total, "sub-agent", "sub-agents")) }
            @if total > 0 {
                ul {
                    @for state in [
                        CompactState::Active,
                        CompactState::Waiting,
                        CompactState::Idle,
                        CompactState::Stalled,
                        CompactState::Finished,
                        CompactState::Failed,
                        CompactState::Unknown,
                    ] {
                        @if let Some(count) = card.child_counts.get(&state).filter(|count| **count > 0) {
                            li { (count) " " (child_state_name(state)) }
                        }
                    }
                }
            }
        }
    }
}

fn empty_state(scope: DashboardScope) -> Markup {
    html! {
        section class="empty-state" {
            @if scope == DashboardScope::Active {
                h3 { "No active orchestration sessions found." }
            } @else {
                h3 { "No retained orchestration sessions found." }
            }
            p { "Agent Watchdog is monitoring the configured Claude and Codex locations." }
        }
    }
}

fn session_summary(count: usize, scope: DashboardScope) -> String {
    let qualifier = if scope == DashboardScope::Active {
        "active"
    } else {
        "retained"
    };
    format!(
        "{count} {qualifier} {}",
        plural_usize(count, "session", "sessions")
    )
}

const fn state_presentation(state: DetailedState) -> (&'static str, &'static str) {
    match state {
        DetailedState::Starting | DetailedState::Running => ("▶", "ACTIVE"),
        DetailedState::WaitingForAgent => ("!", "WAITING FOR AGENT"),
        DetailedState::WaitingForTool => ("!", "WAITING FOR TOOL"),
        DetailedState::WaitingForUser => ("!", "WAITING FOR YOU"),
        DetailedState::Idle => ("○", "IDLE"),
        DetailedState::Stalled => ("×", "STALLED"),
        DetailedState::Completed => ("✓", "FINISHED"),
        DetailedState::Failed => ("×", "FAILED"),
        DetailedState::Cancelled => ("✓", "CANCELLED"),
        DetailedState::Disappeared => ("×", "DISAPPEARED"),
        DetailedState::Unknown => ("?", "UNKNOWN"),
    }
}

const fn compact_name(state: CompactState) -> &'static str {
    match state {
        CompactState::Active => "active",
        CompactState::Waiting => "waiting",
        CompactState::Idle => "idle",
        CompactState::Stalled => "stalled",
        CompactState::Finished => "finished",
        CompactState::Failed => "failed",
        CompactState::Unknown => "unknown",
    }
}

const fn child_state_name(state: CompactState) -> &'static str {
    match state {
        CompactState::Active => "active",
        CompactState::Waiting => "waiting",
        CompactState::Idle => "idle",
        CompactState::Stalled => "stalled",
        CompactState::Finished => "finished",
        CompactState::Failed => "failed",
        CompactState::Unknown => "unknown",
    }
}

fn relative_age(now_ms: i64, then_ms: i64) -> String {
    let elapsed = now_ms.saturating_sub(then_ms).max(0) / 1_000;
    if elapsed < 5 {
        "now".to_owned()
    } else if elapsed < 60 {
        format!("{elapsed}s ago")
    } else if elapsed < 3_600 {
        format!("{}m ago", elapsed / 60)
    } else if elapsed < 86_400 {
        format!("{}h ago", elapsed / 3_600)
    } else {
        format!("{}d ago", elapsed / 86_400)
    }
}

const fn plural<'a>(count: u32, one: &'a str, many: &'a str) -> &'a str {
    if count == 1 { one } else { many }
}

const fn plural_usize<'a>(count: usize, one: &'a str, many: &'a str) -> &'a str {
    if count == 1 { one } else { many }
}
