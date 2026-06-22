use std::fmt::{Display, Formatter};

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_NAME_LEN: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NameError {
    #[error("name must not be empty")]
    Empty,
    #[error("name length {actual} exceeds max length {max}")]
    TooLong { max: usize, actual: usize },
    #[error("invalid character '{ch}' in name")]
    InvalidChar { ch: char },
    #[error("dot '.' is not allowed")]
    DotNotAllowed,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct Identifier(String);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct Domain(String);

impl Identifier {
    pub fn parse(raw: &str) -> Result<Self, Report<NameError>> {
        validate(raw, true).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Domain {
    pub fn parse(raw: &str) -> Result<Self, Report<NameError>> {
        validate(raw, false).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Identifier {
    type Error = NameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate(&value, true)
            .map(Self)
            .map_err(|e| e.current_context().clone())
    }
}

impl TryFrom<&str> for Identifier {
    type Error = NameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate(value, true)
            .map(Self)
            .map_err(|e| e.current_context().clone())
    }
}

impl TryFrom<String> for Domain {
    type Error = NameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate(&value, false)
            .map(Self)
            .map_err(|e| e.current_context().clone())
    }
}

impl TryFrom<&str> for Domain {
    type Error = NameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate(value, false)
            .map(Self)
            .map_err(|e| e.current_context().clone())
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Display for Domain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn validate(raw: &str, allow_dot: bool) -> Result<String, Report<NameError>> {
    if raw.is_empty() {
        return Err(Report::new(NameError::Empty));
    }

    if raw.len() > MAX_NAME_LEN {
        return Err(Report::new(NameError::TooLong {
            max: MAX_NAME_LEN,
            actual: raw.len(),
        }));
    }

    for ch in raw.chars() {
        if ch == '.' && !allow_dot {
            return Err(Report::new(NameError::DotNotAllowed));
        }

        let allowed = ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '~' | '.');
        if !allowed {
            return Err(Report::new(NameError::InvalidChar { ch }));
        }
    }

    Ok(raw.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::{Domain, Identifier, MAX_NAME_LEN, NameError};

    #[test]
    fn identifier_parse_lowercases_and_display_roundtrips() {
        let identifier = Identifier::parse("Tenant_A").expect("valid identifier");
        assert_eq!(identifier.as_str(), "tenant_a");
        assert_eq!(identifier.to_string(), "tenant_a");
    }

    #[test]
    fn domain_parse_lowercases_and_display_roundtrips() {
        let domain = Domain::parse("Prod").expect("valid domain");
        assert_eq!(domain.as_str(), "prod");
        assert_eq!(domain.to_string(), "prod");
    }

    #[test]
    fn identifier_allows_dot_but_domain_rejects_it() {
        let identifier = Identifier::parse("topic.main").expect("identifier allows dots");
        assert_eq!(identifier.as_str(), "topic.main");

        let err = Domain::parse("topic.main").expect_err("domain rejects dots");
        assert!(matches!(err.current_context(), NameError::DotNotAllowed));
    }

    #[test]
    fn validate_rejects_invalid_characters() {
        let err = Identifier::parse("bad/slash").expect_err("slash is invalid");
        assert!(matches!(
            err.current_context(),
            NameError::InvalidChar { ch: '/' }
        ));
    }

    #[test]
    fn validate_enforces_name_length_boundary() {
        let max = "a".repeat(MAX_NAME_LEN);
        assert_eq!(
            Identifier::parse(&max)
                .expect("max length is valid")
                .as_str(),
            max
        );

        let too_long = "a".repeat(MAX_NAME_LEN + 1);
        let err = Identifier::parse(&too_long).expect_err("must reject too long");
        assert!(matches!(
            err.current_context(),
            NameError::TooLong {
                max: MAX_NAME_LEN,
                actual
            } if *actual == MAX_NAME_LEN + 1
        ));
    }
}
