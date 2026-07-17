"use strict";
const sessionList = requiredElement("session-list");
const summary = requiredElement("session-summary");
const connection = requiredElement("connection-status");
const urgentUpdates = requiredElement("urgent-updates");
const scopeControl = requiredSelect("scope");
const sortControl = requiredSelect("sort");
const notificationButton = requiredButton("notifications");
const controls = document.querySelector("form.controls");
let latest = null;
let priorStates = new Map();
let eventSource = null;
let reconnectAttempt = 0;
let reconnectTimer = null;
let staleSince = null;
/** Start a bounded-backoff same-origin SSE connection. */
function connect() {
    if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
    eventSource?.close();
    const source = new EventSource("/api/v1/events");
    eventSource = source;
    setConnection("reconnecting");
    source.addEventListener("open", () => {
        reconnectAttempt = 0;
        staleSince = null;
        setConnection("live");
    });
    source.addEventListener("snapshot", (event) => {
        let candidate;
        try {
            candidate = JSON.parse(event.data);
        }
        catch {
            scheduleReconnect();
            return;
        }
        if (!isSnapshot(candidate)) {
            scheduleReconnect();
            return;
        }
        notifyImportantChanges(candidate);
        latest = candidate;
        render();
    });
    source.addEventListener("resync_required", scheduleReconnect);
    source.addEventListener("error", scheduleReconnect);
}
/** Close the failed stream and reconnect with exponential backoff and jitter. */
function scheduleReconnect() {
    eventSource?.close();
    eventSource = null;
    staleSince ??= new Date();
    setConnection(latest === null ? "reconnecting" : "stale");
    if (reconnectTimer !== null)
        return;
    const base = Math.min(30_000, 1_000 * (2 ** Math.min(reconnectAttempt, 5)));
    const delay = Math.round(base * (0.8 + Math.random() * 0.4));
    reconnectAttempt += 1;
    reconnectTimer = window.setTimeout(connect, delay);
}
/** Apply the current local scope and sort without polling or mutating server state. */
function render() {
    if (latest === null)
        return;
    const scope = selectedScope();
    const sort = selectedSort();
    const cards = latest.sessions.filter((card) => scope === "all" || !isTerminal(card.state));
    cards.sort((left, right) => compareCards(left, right, sort));
    sessionList.replaceChildren(...(cards.length > 0 ? cards.map(createCard) : [createEmpty(scope)]));
    sessionList.dataset.revision = String(latest.revision);
    summary.textContent = `${cards.length} ${scope === "active" ? "active" : "retained"} ${cards.length === 1 ? "session" : "sessions"}`;
}
/** Build one semantic, non-interactive main-session card using text nodes only. */
function createCard(card) {
    const article = document.createElement("article");
    article.className = `session-card state-${card.compact_state}`;
    article.dataset.sessionId = card.session_id;
    const stateRow = element("div", "state-row");
    const stateLabel = element("p", "state-label");
    const [glyph, label] = statePresentation(card.state);
    const glyphNode = element("span", "state-glyph", glyph);
    glyphNode.setAttribute("aria-hidden", "true");
    stateLabel.append(glyphNode, document.createTextNode(label));
    if (card.warning !== null) {
        const badge = element("span", "warning-badge", card.warning.badge);
        badge.title = card.warning.message;
        badge.setAttribute("aria-label", card.warning.message);
        stateLabel.append(badge);
    }
    stateRow.append(stateLabel, element("p", "age", `Last activity ${relativeAge(card.last_activity_ms)}`));
    const title = document.createElement("h3");
    title.textContent = card.title;
    const directory = element("p", "directory", card.startup_directory);
    const repository = element("p", "repository");
    const safeUrl = safePullRequestUrl(card.pull_request_url);
    if (card.pull_request_number !== null && safeUrl !== null) {
        const link = document.createElement("a");
        link.href = safeUrl;
        link.rel = "noopener noreferrer";
        link.textContent = `PR #${card.pull_request_number} ↗`;
        repository.append(link);
    }
    else {
        repository.textContent = card.branch ?? "Branch unavailable";
    }
    article.append(stateRow, title, directory, repository, createChildSummary(card.child_counts));
    return article;
}
/** Build compact child counts in the approved stable order. */
function createChildSummary(counts) {
    const container = element("div", "children");
    const total = Object.values(counts).reduce((sum, count) => sum + (count ?? 0), 0);
    container.setAttribute("aria-label", `${total} sub-agents`);
    const strong = document.createElement("strong");
    strong.textContent = `${total} ${total === 1 ? "sub-agent" : "sub-agents"}`;
    container.append(strong);
    if (total === 0)
        return container;
    const list = document.createElement("ul");
    for (const state of compactStateOrder) {
        const count = counts[state] ?? 0;
        if (count === 0)
            continue;
        const item = document.createElement("li");
        item.textContent = `${count} ${state}`;
        list.append(item);
    }
    container.append(list);
    return container;
}
/** Notify only for approved main-session human events, never child diagnostics. */
function notifyImportantChanges(snapshot) {
    if (priorStates.size === 0) {
        priorStates = new Map(snapshot.sessions.map((card) => [card.session_id, card.state]));
        return;
    }
    for (const card of snapshot.sessions) {
        const prior = priorStates.get(card.session_id);
        if (prior === card.state)
            continue;
        if (humanEvent(card.state)) {
            urgentUpdates.textContent = `${card.title}: ${statePresentation(card.state)[1]}`;
            if ("Notification" in window && Notification.permission === "granted") {
                new Notification(`Agent Watchdog: ${statePresentation(card.state)[1].toLowerCase()}`, {
                    body: `${card.title}\n${card.startup_directory}`,
                    tag: `agent-watchdog-${card.session_id}-${card.state}`,
                });
            }
        }
    }
    priorStates = new Map(snapshot.sessions.map((card) => [card.session_id, card.state]));
}
/** Request and display browser notification permission without blocking monitoring. */
async function configureNotifications() {
    if (!("Notification" in window)) {
        notificationButton.textContent = "Notifications unsupported";
        notificationButton.disabled = true;
        return;
    }
    if (Notification.permission === "default")
        await Notification.requestPermission();
    updateNotificationButton();
}
/** Reflect notification permission in a keyboard-accessible button. */
function updateNotificationButton() {
    if (!("Notification" in window)) {
        notificationButton.textContent = "Notifications unsupported";
        notificationButton.disabled = true;
        return;
    }
    notificationButton.textContent = `Notifications ${Notification.permission}`;
}
/** Update visible connection state without removing the last durable snapshot. */
function setConnection(state) {
    connection.className = `connection ${state}`;
    if (state === "live") {
        connection.textContent = "● Live";
    }
    else if (state === "reconnecting") {
        connection.textContent = "◌ Reconnecting";
    }
    else {
        connection.textContent = `◌ Stale since ${(staleSince ?? new Date()).toLocaleTimeString()}`;
    }
}
/** Narrow untrusted JSON to the fields required by the browser. */
function isSnapshot(value) {
    if (typeof value !== "object" || value === null)
        return false;
    const candidate = value;
    return typeof candidate.revision === "number"
        && typeof candidate.generated_at_ms === "number"
        && Array.isArray(candidate.sessions)
        && candidate.sessions.every(isCard);
}
function isCard(value) {
    if (typeof value !== "object" || value === null)
        return false;
    const candidate = value;
    return typeof candidate.session_id === "string"
        && typeof candidate.title === "string"
        && typeof candidate.startup_directory === "string"
        && optionalString(candidate.branch)
        && optionalNumber(candidate.pull_request_number)
        && optionalString(candidate.pull_request_url)
        && detailedStates.has(candidate.state)
        && compactStates.has(candidate.compact_state)
        && typeof candidate.last_activity_ms === "number"
        && isChildCounts(candidate.child_counts)
        && isWarning(candidate.warning);
}
function isChildCounts(value) {
    if (typeof value !== "object" || value === null || Array.isArray(value))
        return false;
    return Object.entries(value).every(([state, count]) => compactStates.has(state) && typeof count === "number" && count >= 0);
}
function isWarning(value) {
    if (value === null)
        return true;
    if (typeof value !== "object")
        return false;
    const candidate = value;
    return typeof candidate.badge === "string" && typeof candidate.message === "string";
}
function optionalString(value) { return value === null || typeof value === "string"; }
function optionalNumber(value) { return value === null || typeof value === "number"; }
/** Create an element whose content is always assigned through textContent. */
function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className !== undefined)
        node.className = className;
    if (text !== undefined)
        node.textContent = text;
    return node;
}
/** Require a server-rendered element and fail visibly on template drift. */
function requiredElement(id) {
    const node = document.getElementById(id);
    if (node === null)
        throw new Error(`Missing required element: ${id}`);
    return node;
}
/** Require a select control. */
function requiredSelect(id) {
    const node = document.getElementById(id);
    if (!(node instanceof HTMLSelectElement))
        throw new Error(`Missing required select: ${id}`);
    return node;
}
/** Require a button control. */
function requiredButton(id) {
    const node = document.getElementById(id);
    if (!(node instanceof HTMLButtonElement))
        throw new Error(`Missing required button: ${id}`);
    return node;
}
function createEmpty(scope) {
    const section = element("section", "empty-state");
    const heading = document.createElement("h3");
    heading.textContent = scope === "active"
        ? "No active orchestration sessions found."
        : "No retained orchestration sessions found.";
    section.append(heading, element("p", undefined, "Agent Watchdog is monitoring the configured Claude and Codex locations."));
    return section;
}
function selectedScope() { return scopeControl.value === "all" ? "all" : "active"; }
function selectedSort() { return sortControl.value === "directory" ? "directory" : "attention"; }
function compareCards(left, right, sort) {
    const directory = left.startup_directory.localeCompare(right.startup_directory, undefined, { sensitivity: "base" });
    if (sort === "directory")
        return directory || left.session_id.localeCompare(right.session_id);
    return attentionRank(left.compact_state) - attentionRank(right.compact_state)
        || directory
        || left.session_id.localeCompare(right.session_id);
}
function attentionRank(state) {
    if (state === "waiting" || state === "stalled")
        return 0;
    if (state === "idle")
        return 1;
    return 2;
}
function isTerminal(state) {
    return state === "completed" || state === "failed" || state === "cancelled" || state === "disappeared";
}
function humanEvent(state) {
    return state === "waiting_for_user" || state === "stalled" || state === "completed";
}
function statePresentation(state) {
    const presentations = {
        starting: ["▶", "ACTIVE"], running: ["▶", "ACTIVE"], waiting_for_agent: ["!", "WAITING FOR AGENT"],
        waiting_for_tool: ["!", "WAITING FOR TOOL"], waiting_for_user: ["!", "WAITING FOR YOU"], idle: ["○", "IDLE"],
        stalled: ["×", "STALLED"], completed: ["✓", "FINISHED"], failed: ["×", "FAILED"], cancelled: ["✓", "CANCELLED"],
        disappeared: ["×", "DISAPPEARED"], unknown: ["?", "UNKNOWN"],
    };
    return presentations[state];
}
function relativeAge(thenMs) {
    const seconds = Math.max(0, Math.floor((Date.now() - thenMs) / 1_000));
    if (seconds < 5)
        return "now";
    if (seconds < 60)
        return `${seconds}s ago`;
    if (seconds < 3_600)
        return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86_400)
        return `${Math.floor(seconds / 3_600)}h ago`;
    return `${Math.floor(seconds / 86_400)}d ago`;
}
function safePullRequestUrl(raw) {
    if (raw === null)
        return null;
    try {
        const url = new URL(raw);
        return url.protocol === "https:"
            && url.hostname === "github.com"
            && url.port === ""
            && url.username === ""
            && url.password === ""
            && url.search === ""
            && url.hash === ""
            ? url.href
            : null;
    }
    catch {
        return null;
    }
}
const compactStateOrder = ["active", "waiting", "idle", "stalled", "finished", "failed", "unknown"];
const compactStates = new Set(compactStateOrder);
const detailedStates = new Set([
    "starting", "running", "waiting_for_agent", "waiting_for_tool", "waiting_for_user", "idle", "stalled",
    "completed", "failed", "cancelled", "disappeared", "unknown",
]);
controls?.addEventListener("submit", (event) => {
    event.preventDefault();
    const query = new URLSearchParams({ scope: selectedScope(), sort: selectedSort() });
    history.replaceState(null, "", `/ui?${query.toString()}`);
    render();
});
scopeControl.addEventListener("change", render);
sortControl.addEventListener("change", render);
notificationButton.addEventListener("click", () => { void configureNotifications(); });
window.addEventListener("beforeunload", () => eventSource?.close());
updateNotificationButton();
connect();
