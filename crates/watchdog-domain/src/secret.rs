use std::fmt;

use secrecy::{ExposeSecret, SecretString};

/// Heap-backed secret that zeroizes on drop and never reveals itself via Debug.
#[derive(Clone)]
pub struct SecretText(SecretString);

impl SecretText {
    /// Wrap secret text without enabling serialization.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(SecretString::from(value.into()))
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
