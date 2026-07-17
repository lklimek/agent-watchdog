use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use futures::{StreamExt, future::BoxFuture};
use reqwest::{
    Client, Url,
    header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue},
    redirect, retry,
};
use serde::Deserialize;
use thiserror::Error;
use watchdog_domain::{BoundedText, Clock, DomainInputError, DurationMs};

const MAX_GITHUB_COMPONENT_BYTES: usize = 100;
const MAX_GITHUB_TOKEN_BYTES: usize = 4_096;
const MAX_GITHUB_RESPONSE_BYTES: usize = 64 * 1_024;
const CACHE_TTL: DurationMs = DurationMs::new(5 * 60 * 1_000);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(5);
const GITHUB_API_VERSION: &str = "2026-03-10";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GitHubRepository {
    owner: BoundedText<MAX_GITHUB_COMPONENT_BYTES>,
    name: BoundedText<MAX_GITHUB_COMPONENT_BYTES>,
}

/// Branch metadata with optional cached open-pull-request enrichment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitHubEnrichment {
    branch: BoundedText<512>,
    pull_request_number: Option<u64>,
    pull_request_url: Option<BoundedText<2_048>>,
}

impl GitHubEnrichment {
    /// Current branch, retained even when GitHub is unavailable.
    #[must_use]
    pub fn branch(&self) -> &str {
        self.branch.as_str()
    }

    /// Open pull request number when one was resolved.
    #[must_use]
    pub const fn pull_request_number(&self) -> Option<u64> {
        self.pull_request_number
    }

    /// Locally constructed, validated GitHub pull request URL.
    #[must_use]
    pub fn pull_request_url(&self) -> Option<&str> {
        self.pull_request_url.as_ref().map(BoundedText::as_str)
    }
}

/// Cached, best-effort GitHub pull request resolver.
#[derive(Clone)]
pub struct GitHubEnricher {
    clock: Arc<dyn Clock>,
    source: Arc<dyn PullRequestSource>,
    cache_ttl: DurationMs,
    cache: Arc<Mutex<HashMap<CacheKey, CacheEntry>>>,
}

impl GitHubEnricher {
    /// Construct an enricher fixed to GitHub's public API.
    ///
    /// The optional token is copied only into a sensitive HTTP header. Requests
    /// never follow redirects, use environment proxies, or retry.
    ///
    /// # Errors
    ///
    /// Returns [`GitHubConfigError`] when the token or HTTP client is invalid.
    pub fn new(clock: Arc<dyn Clock>, token: Option<&str>) -> Result<Self, GitHubConfigError> {
        Ok(Self::with_source(
            clock,
            Arc::new(GitHubApiSource::new(token)?),
            CACHE_TTL,
        ))
    }

    fn with_source(
        clock: Arc<dyn Clock>,
        source: Arc<dyn PullRequestSource>,
        cache_ttl: DurationMs,
    ) -> Self {
        Self {
            clock,
            source,
            cache_ttl,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Resolve an open pull request, preserving branch-only metadata on every
    /// unsupported-remote, offline, authorization, rate-limit, or schema failure.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] only when the local branch metadata is empty
    /// or exceeds its storage bound. GitHub failures are cached fallbacks.
    pub async fn enrich(
        &self,
        remote: &str,
        branch: &str,
    ) -> Result<GitHubEnrichment, DomainInputError> {
        let branch = BoundedText::new("branch", branch)?;
        if branch.is_empty() {
            return Err(DomainInputError::Empty { field: "branch" });
        }
        let Some(repository) = parse_github_remote(remote) else {
            return Ok(enrichment(branch, None, None));
        };
        let key = CacheKey {
            repository: repository.clone(),
            branch: branch.clone(),
        };
        let now_ms = self.clock.now().monotonic_ms();
        if let Some(entry) = self
            .lock_cache()
            .get(&key)
            .filter(|entry| now_ms < entry.expires_at_ms)
            .copied()
        {
            return Ok(enrichment(branch, Some(&repository), entry.pull_request));
        }

        let pull_request = self
            .source
            .lookup(repository.clone(), branch.clone())
            .await
            .unwrap_or(None);
        self.lock_cache().insert(
            key,
            CacheEntry {
                pull_request,
                expires_at_ms: now_ms.saturating_add(self.cache_ttl.value()),
            },
        );
        Ok(enrichment(branch, Some(&repository), pull_request))
    }

    fn lock_cache(&self) -> MutexGuard<'_, HashMap<CacheKey, CacheEntry>> {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl fmt::Debug for GitHubEnricher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GitHubEnricher")
            .field("cache_ttl", &self.cache_ttl)
            .finish_non_exhaustive()
    }
}

fn enrichment(
    branch: BoundedText<512>,
    repository: Option<&GitHubRepository>,
    pull_request: Option<PullRequest>,
) -> GitHubEnrichment {
    let (pull_request_number, pull_request_url) =
        repository
            .zip(pull_request)
            .map_or((None, None), |(repository, pull)| {
                let url = format!(
                    "https://github.com/{}/{}/pull/{}",
                    repository.owner.as_str(),
                    repository.name.as_str(),
                    pull.number
                );
                (
                    Some(pull.number),
                    BoundedText::new("pull_request_url", url).ok(),
                )
            });
    GitHubEnrichment {
        branch,
        pull_request_number,
        pull_request_url,
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    repository: GitHubRepository,
    branch: BoundedText<512>,
}

#[derive(Clone, Copy, Debug)]
struct CacheEntry {
    pull_request: Option<PullRequest>,
    expires_at_ms: u64,
}

#[derive(Clone, Debug)]
struct GitHubApiSource {
    client: Client,
    base_url: Url,
}

impl GitHubApiSource {
    fn new(token: Option<&str>) -> Result<Self, GitHubConfigError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github+json"),
        );
        headers.insert(
            "x-github-api-version",
            HeaderValue::from_static(GITHUB_API_VERSION),
        );
        if let Some(token) = token {
            if token.is_empty() || token.len() > MAX_GITHUB_TOKEN_BYTES {
                return Err(GitHubConfigError::InvalidToken);
            }
            let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| GitHubConfigError::InvalidToken)?;
            authorization.set_sensitive(true);
            headers.insert(AUTHORIZATION, authorization);
        }
        let client = Client::builder()
            .default_headers(headers)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(TOTAL_TIMEOUT)
            .redirect(redirect::Policy::none())
            .retry(retry::never())
            .no_proxy()
            .user_agent("agent-watchdog/0.1")
            .build()
            .map_err(|_| GitHubConfigError::ClientInitialization)?;
        let base_url = Url::parse("https://api.github.com/")
            .map_err(|_| GitHubConfigError::ClientInitialization)?;
        Ok(Self { client, base_url })
    }

    async fn request(
        &self,
        repository: GitHubRepository,
        branch: BoundedText<512>,
    ) -> Result<Option<PullRequest>, SourceError> {
        let mut url = self.base_url.clone();
        url.set_path(&format!(
            "repos/{}/{}/pulls",
            repository.owner.as_str(),
            repository.name.as_str()
        ));
        url.query_pairs_mut()
            .append_pair("state", "open")
            .append_pair(
                "head",
                &format!("{}:{}", repository.owner.as_str(), branch.as_str()),
            )
            .append_pair("per_page", "1");

        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| SourceError::Unavailable)?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > MAX_GITHUB_RESPONSE_BYTES as u64)
        {
            return Err(SourceError::Unavailable);
        }
        let mut body = Vec::new();
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|_| SourceError::Unavailable)?;
            if body.len().saturating_add(chunk.len()) > MAX_GITHUB_RESPONSE_BYTES {
                return Err(SourceError::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        let pulls: Vec<ApiPullRequest> =
            serde_json::from_slice(&body).map_err(|_| SourceError::InvalidResponse)?;
        Ok(pulls.first().map(|pull| PullRequest {
            number: pull.number,
        }))
    }
}

trait PullRequestSource: Send + Sync {
    fn lookup(
        &self,
        repository: GitHubRepository,
        branch: BoundedText<512>,
    ) -> BoxFuture<'static, Result<Option<PullRequest>, SourceError>>;
}

impl PullRequestSource for GitHubApiSource {
    fn lookup(
        &self,
        repository: GitHubRepository,
        branch: BoundedText<512>,
    ) -> BoxFuture<'static, Result<Option<PullRequest>, SourceError>> {
        let source = self.clone();
        Box::pin(async move { source.request(repository, branch).await })
    }
}

#[derive(Clone, Copy, Debug)]
struct PullRequest {
    number: u64,
}

#[derive(Clone, Copy, Debug, Error)]
enum SourceError {
    #[error("GitHub is unavailable")]
    Unavailable,
    #[error("GitHub response exceeded the size limit")]
    ResponseTooLarge,
    #[error("GitHub returned an invalid response")]
    InvalidResponse,
}

#[derive(Deserialize)]
struct ApiPullRequest {
    number: u64,
}

/// Invalid GitHub enrichment configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GitHubConfigError {
    /// A configured token is empty, oversized, or invalid as an HTTP header.
    #[error("GitHub token is invalid")]
    InvalidToken,
    /// The fixed GitHub API client could not be initialized.
    #[error("GitHub API client initialization failed")]
    ClientInitialization,
}

fn parse_github_remote(remote: &str) -> Option<GitHubRepository> {
    let path = if let Some(path) = remote.strip_prefix("git@github.com:") {
        path.to_owned()
    } else {
        let url = Url::parse(remote).ok()?;
        let supported = match url.scheme() {
            "https" => url.username().is_empty(),
            "ssh" => url.username() == "git",
            _ => false,
        };
        if !supported
            || url.host_str() != Some("github.com")
            || url.password().is_some()
            || url.port().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return None;
        }
        url.path().to_owned()
    };

    let path = path.strip_prefix('/').unwrap_or(&path);
    let (owner, name) = path.split_once('/')?;
    let name = name.strip_suffix(".git").unwrap_or(name);
    if owner.is_empty()
        || name.is_empty()
        || name.contains('/')
        || !valid_owner(owner)
        || !valid_repository_name(name)
    {
        return None;
    }
    Some(GitHubRepository {
        owner: BoundedText::new("github_owner", owner).ok()?,
        name: BoundedText::new("github_repository", name).ok()?,
    })
}

fn valid_owner(value: &str) -> bool {
    value.len() <= 39
        && !value.starts_with('-')
        && !value.ends_with('-')
        && !value.contains("--")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn valid_repository_name(value: &str) -> bool {
    value.len() <= MAX_GITHUB_COMPONENT_BYTES
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use futures::future::{BoxFuture, FutureExt};
    use watchdog_domain::{DurationMs, TimePoint, WallTimeMs};
    use watchdog_testkit::FakeClock;

    use super::{
        GitHubConfigError, GitHubEnricher, GitHubRepository, PullRequest, PullRequestSource,
        SourceError, parse_github_remote,
    };

    fn repository(owner: &str, name: &str) -> GitHubRepository {
        GitHubRepository {
            owner: watchdog_domain::BoundedText::new("owner", owner).expect("valid owner"),
            name: watchdog_domain::BoundedText::new("name", name).expect("valid repository"),
        }
    }

    #[test]
    fn parses_supported_github_remote_forms() {
        for remote in [
            "https://github.com/lklimek/agent-watchdog.git",
            "git@github.com:lklimek/agent-watchdog.git",
            "ssh://git@github.com/lklimek/agent-watchdog.git",
        ] {
            assert_eq!(
                parse_github_remote(remote),
                Some(repository("lklimek", "agent-watchdog")),
                "remote {remote}"
            );
        }
    }

    #[test]
    fn rejects_non_github_or_ambiguous_remotes() {
        for remote in [
            "https://gitlab.com/lklimek/agent-watchdog.git",
            "https://github.com.evil.example/lklimek/agent-watchdog.git",
            "https://user:secret@github.com/lklimek/agent-watchdog.git",
            "https://github.com/lklimek/agent-watchdog/extra",
            "git@github.com:lklimek/agent-watchdog/extra.git",
            "https://github.com/lklimek/..",
            "file:///home/user/agent-watchdog",
            "",
        ] {
            assert_eq!(parse_github_remote(remote), None, "remote {remote}");
        }
    }

    #[test]
    fn rejects_invalid_tokens_and_redacts_valid_tokens_from_debug() {
        let clock = Arc::new(FakeClock::new(TimePoint::new(WallTimeMs::new(1), 1)));
        assert_eq!(
            GitHubEnricher::new(Arc::clone(&clock) as Arc<_>, Some(""))
                .expect_err("empty token must fail"),
            GitHubConfigError::InvalidToken
        );
        assert_eq!(
            GitHubEnricher::new(Arc::clone(&clock) as Arc<_>, Some("bad\ntoken"))
                .expect_err("invalid header value must fail"),
            GitHubConfigError::InvalidToken
        );

        let enricher =
            GitHubEnricher::new(clock, Some("github_pat_secret")).expect("valid GitHub client");
        assert!(!format!("{enricher:?}").contains("github_pat_secret"));
    }

    #[tokio::test]
    async fn caches_successful_enrichment_by_repository_and_branch() {
        let clock = Arc::new(FakeClock::new(TimePoint::new(WallTimeMs::new(1), 1)));
        let source = Arc::new(FixedSource::new(Ok(Some(PullRequest { number: 42 }))));
        let enricher = GitHubEnricher::with_source(
            Arc::clone(&clock) as Arc<_>,
            source.clone(),
            DurationMs::new(60_000),
        );

        let first = enricher
            .enrich("git@github.com:lklimek/agent-watchdog.git", "feat/cache")
            .await
            .expect("valid enrichment input");
        let second = enricher
            .enrich("https://github.com/lklimek/agent-watchdog", "feat/cache")
            .await
            .expect("valid enrichment input");

        assert_eq!(first, second);
        assert_eq!(first.branch(), "feat/cache");
        assert_eq!(first.pull_request_number(), Some(42));
        assert_eq!(
            first.pull_request_url(),
            Some("https://github.com/lklimek/agent-watchdog/pull/42")
        );
        assert_eq!(source.calls(), 1);
    }

    #[tokio::test]
    async fn caches_offline_fallback_then_refreshes_after_ttl() {
        let clock = Arc::new(FakeClock::new(TimePoint::new(WallTimeMs::new(1), 1)));
        let source = Arc::new(FixedSource::new(Err(SourceError::Unavailable)));
        let enricher = GitHubEnricher::with_source(
            Arc::clone(&clock) as Arc<_>,
            source.clone(),
            DurationMs::new(60_000),
        );

        for _ in 0..2 {
            let result = enricher
                .enrich("https://github.com/lklimek/agent-watchdog.git", "main")
                .await
                .expect("valid enrichment input");
            assert_eq!(result.branch(), "main");
            assert_eq!(result.pull_request_number(), None);
            assert_eq!(result.pull_request_url(), None);
        }
        assert_eq!(source.calls(), 1);

        clock.advance(DurationMs::new(60_001));
        enricher
            .enrich("https://github.com/lklimek/agent-watchdog.git", "main")
            .await
            .expect("valid enrichment input");
        assert_eq!(source.calls(), 2);
    }

    #[derive(Clone)]
    struct FixedSource {
        calls: Arc<AtomicUsize>,
        result: Arc<Mutex<Result<Option<PullRequest>, SourceError>>>,
    }

    impl FixedSource {
        fn new(result: Result<Option<PullRequest>, SourceError>) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                result: Arc::new(Mutex::new(result)),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl PullRequestSource for FixedSource {
        fn lookup(
            &self,
            _repository: GitHubRepository,
            _branch: watchdog_domain::BoundedText<512>,
        ) -> BoxFuture<'static, Result<Option<PullRequest>, SourceError>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let result = *self
                .result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            async move { result }.boxed()
        }
    }
}
