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
    /// Bearer credentials must be representable in an HTTP header.
    #[error("Bearer token contains invalid HTTP-header characters")]
    InvalidCharacters,
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
        if !token.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
            return Err(BearerAuthError::InvalidCharacters);
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
