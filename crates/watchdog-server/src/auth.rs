use base64::{Engine as _, engine::general_purpose::STANDARD};
use watchdog_domain::SecretText;

/// Invalid shared Bearer-token configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BearerAuthError {
    /// Empty tokens would make authentication meaningless.
    #[error("Bearer token must not be empty")]
    Empty,
    /// Unbounded credentials waste memory and make request handling less robust.
    #[error("Bearer token exceeds {MAX_AUTHORIZATION_BYTES} bytes")]
    TooLong,
}

/// Invalid shared Basic-auth configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BasicAuthError {
    /// Browser credentials must contain both fields.
    #[error("Basic-auth username and password must not be empty")]
    Empty,
    /// A colon would make the username ambiguous in RFC 7617 encoding.
    #[error("Basic-auth username must not contain a colon")]
    UsernameContainsColon,
    /// Encoded credentials exceed the request boundary.
    #[error("Basic-auth credential exceeds {MAX_AUTHORIZATION_BYTES} bytes")]
    TooLong,
}

/// Maximum accepted Authorization header length.
pub const MAX_AUTHORIZATION_BYTES: usize = 4_096;
const PREFIX: &[u8] = b"Bearer ";

/// Redacted strict Bearer-token verifier for the shared MCP/API credential.
#[derive(Clone, Debug)]
pub struct BearerAuthenticator {
    token: SecretText,
}

impl BearerAuthenticator {
    /// Construct a verifier without enabling secret serialization or Debug.
    ///
    /// # Errors
    ///
    /// Returns [`BearerAuthError`] for empty or oversized configuration.
    pub fn new(token: impl Into<String>) -> Result<Self, BearerAuthError> {
        let token = token.into();
        if token.is_empty() {
            return Err(BearerAuthError::Empty);
        }
        if token.len() > MAX_AUTHORIZATION_BYTES - PREFIX.len() {
            return Err(BearerAuthError::TooLong);
        }
        Ok(Self {
            token: SecretText::new(token),
        })
    }

    /// Validate one raw Authorization header, failing closed for absent,
    /// malformed, empty, or oversized input.
    #[must_use]
    pub fn authorize(&self, authorization: Option<&[u8]>) -> bool {
        let Some(header) = authorization.filter(|value| value.len() <= MAX_AUTHORIZATION_BYTES)
        else {
            return false;
        };
        let Some(candidate) = header
            .strip_prefix(PREFIX)
            .filter(|value| !value.is_empty())
        else {
            return false;
        };
        self.token.constant_time_eq(candidate)
    }
}

/// Redacted strict Basic-auth verifier for the browser and read-only API.
#[derive(Clone, Debug)]
pub struct BasicAuthenticator {
    expected: SecretText,
}

impl BasicAuthenticator {
    /// Construct the exact expected RFC 7617 Authorization value.
    ///
    /// # Errors
    ///
    /// Returns [`BasicAuthError`] for empty, ambiguous, or oversized values.
    pub fn new(username: &str, password: &str) -> Result<Self, BasicAuthError> {
        if username.is_empty() || password.is_empty() {
            return Err(BasicAuthError::Empty);
        }
        if username.contains(':') {
            return Err(BasicAuthError::UsernameContainsColon);
        }
        let encoded = STANDARD.encode(format!("{username}:{password}"));
        let expected = format!("Basic {encoded}");
        if expected.len() > MAX_AUTHORIZATION_BYTES {
            return Err(BasicAuthError::TooLong);
        }
        Ok(Self {
            expected: SecretText::new(expected),
        })
    }

    /// Validate one raw Authorization header exactly and fail closed.
    #[must_use]
    pub fn authorize(&self, authorization: Option<&[u8]>) -> bool {
        authorization
            .filter(|value| value.len() <= MAX_AUTHORIZATION_BYTES)
            .is_some_and(|value| self.expected.constant_time_eq(value))
    }
}
