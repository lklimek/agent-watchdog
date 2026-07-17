use std::fmt;

use secrecy::{ExposeSecret, SecretString};
use subtle::ConstantTimeEq;

/// Heap-backed secret that zeroizes on drop and never reveals itself via Debug.
#[derive(Clone)]
pub struct SecretText(SecretString);

impl SecretText {
    /// Wrap secret text without enabling serialization.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(SecretString::from(value.into()))
    }

    /// Compare a candidate without exposing the stored secret to callers.
    ///
    /// The comparison primitive is constant-time for equal-length values.
    #[must_use]
    pub fn constant_time_eq(&self, candidate: &[u8]) -> bool {
        bool::from(self.0.expose_secret().as_bytes().ct_eq(candidate))
    }
}

impl ExposeSecret<str> for SecretText {
    fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for SecretText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretText([REDACTED])")
    }
}
