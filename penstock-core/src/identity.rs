use std::{fmt, sync::Arc};

use thiserror::Error;

/// Stable identity shared by a pipeline's metrics and durable checkpoint storage.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PipelineId(Arc<str>);

impl PipelineId {
    pub const MAX_LEN: usize = 255;

    pub fn new(value: impl Into<String>) -> Result<Self, PipelineIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(PipelineIdError::Empty);
        }
        if value.len() > Self::MAX_LEN {
            return Err(PipelineIdError::TooLong);
        }
        if let Some((index, character)) = value
            .char_indices()
            .find(|(_, character)| !is_valid_character(*character))
        {
            return Err(PipelineIdError::InvalidCharacter { index, character });
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn unnamed() -> Self {
        Self(Arc::from("unnamed"))
    }
}

impl fmt::Debug for PipelineId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("PipelineId").field(&self.0).finish()
    }
}

impl fmt::Display for PipelineId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for PipelineId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

const fn is_valid_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':' | '/')
}

/// An invalid stable pipeline identity.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PipelineIdError {
    #[error("pipeline id must not be empty")]
    Empty,
    #[error("pipeline id must not exceed 255 bytes")]
    TooLong,
    #[error("pipeline id contains invalid character {character:?} at byte {index}")]
    InvalidCharacter { index: usize, character: char },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_length() {
        assert_eq!(PipelineId::new(""), Err(PipelineIdError::Empty));
        assert!(PipelineId::new("x".repeat(PipelineId::MAX_LEN)).is_ok());
        assert_eq!(
            PipelineId::new("x".repeat(PipelineId::MAX_LEN + 1)),
            Err(PipelineIdError::TooLong)
        );
    }

    #[test]
    fn accepts_portable_characters_and_rejects_everything_else() {
        let id = PipelineId::new("ethereum/mainnet:blocks.v2_archive-1").unwrap();
        assert_eq!(id.as_str(), "ethereum/mainnet:blocks.v2_archive-1");
        assert_eq!(
            PipelineId::new("orders to warehouse"),
            Err(PipelineIdError::InvalidCharacter {
                index: 6,
                character: ' ',
            })
        );
        assert_eq!(
            PipelineId::new("pipeline-☃"),
            Err(PipelineIdError::InvalidCharacter {
                index: 9,
                character: '☃',
            })
        );
    }
}
