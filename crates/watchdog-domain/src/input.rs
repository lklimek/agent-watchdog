use std::{fmt, ops::Deref};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

/// A validation error that never includes the rejected input value.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DomainInputError {
    /// A required field was empty.
    #[error("Field {field} must not be empty")]
    Empty {
        /// Stable field name safe for logs and responses.
        field: &'static str,
    },
    /// A field exceeded its UTF-8 byte budget.
    #[error("Field {field} is {actual_bytes} bytes; maximum is {max_bytes}")]
    TooLong {
        /// Stable field name safe for logs and responses.
        field: &'static str,
        /// Configured maximum UTF-8 byte count.
        max_bytes: usize,
        /// Actual UTF-8 byte count, without the rejected value.
        actual_bytes: usize,
    },
}

/// UTF-8 text whose allocation is capped at compile time.
///
/// JSON Schema `maxLength` counts characters, so for non-ASCII text the
/// advertised bound is looser than the UTF-8 byte budget enforced here; it is
/// never tighter, so a schema-valid value is never rejected by the schema alone.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, schemars::JsonSchema)]
pub struct BoundedText<const MAX_BYTES: usize>(#[schemars(length(max = MAX_BYTES))] Box<str>);

impl<const MAX_BYTES: usize> BoundedText<MAX_BYTES> {
    /// Validate and store a bounded UTF-8 value.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError::TooLong`] without echoing the rejected value
    /// when its UTF-8 representation exceeds `MAX_BYTES`.
    pub fn new(field: &'static str, value: impl Into<String>) -> Result<Self, DomainInputError> {
        let value = value.into();
        if value.len() > MAX_BYTES {
            return Err(DomainInputError::TooLong {
                field,
                max_bytes: MAX_BYTES,
                actual_bytes: value.len(),
            });
        }
        Ok(Self(value.into_boxed_str()))
    }

    /// Borrow the validated text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<const MAX_BYTES: usize> fmt::Debug for BoundedText<MAX_BYTES> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("BoundedText").field(&self.0).finish()
    }
}

impl<const MAX_BYTES: usize> Deref for BoundedText<MAX_BYTES> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl<const MAX_BYTES: usize> Serialize for BoundedText<MAX_BYTES> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de, const MAX_BYTES: usize> Deserialize<'de> for BoundedText<MAX_BYTES> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new("bounded_text", value).map_err(de::Error::custom)
    }
}
