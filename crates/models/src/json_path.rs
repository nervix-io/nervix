//! The path a JSON extraction follows from the root of a document to the value it reads.
//!
//! A path is written as text inside an NSPL string literal, such as `'$.orders[0]["unit price"]'`,
//! and parsed once, when the statement is parsed, into the steps it takes. Execution follows the
//! steps and never reads the text again, and canonical rendering writes the steps back.

use std::fmt::{self, Display, Formatter};

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A path from the root of a JSON document, written `$` followed by its steps.
///
/// `.name` and `["name"]` step into the member of an object with that name, and `[n]` steps into
/// the element of an array at the zero-based index `n`. A path with no steps names the whole
/// document.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct JsonPath {
    steps: Vec<JsonPathStep>,
}

/// One step of a [`JsonPath`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum JsonPathStep {
    /// The member of an object with this name.
    Member(String),
    /// The element of an array at this zero-based index.
    Element(u32),
}

/// Why text is not a JSON path.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JsonPathError {
    #[error("a path starts with '$'")]
    MissingRoot,
    #[error("expected '.' or '[' at byte {at}")]
    ExpectedStep { at: usize },
    #[error(
        "expected a member name of letters, digits and underscores not starting with a digit at \
         byte {at}; write any other name as [\"name\"]"
    )]
    InvalidMemberName { at: usize },
    #[error("expected an array index or a double-quoted member name at byte {at}")]
    InvalidBracket { at: usize },
    #[error(
        "array index at byte {at} is not an integer from 0 to 4294967295 without leading zeros"
    )]
    InvalidIndex { at: usize },
    #[error("member name at byte {at} is not a valid JSON string")]
    InvalidQuotedName { at: usize },
    #[error("expected ']' at byte {at}")]
    UnclosedBracket { at: usize },
    #[error("a path takes at most {max} steps")]
    TooManySteps { max: usize },
}

impl JsonPath {
    /// The most steps a path may take.
    pub const MAX_STEPS: usize = 64;

    /// Parses the written form of a path.
    pub fn parse(text: &str) -> Result<Self, Report<JsonPathError>> {
        let Some(mut rest) = text.strip_prefix('$') else {
            return Err(Report::new(JsonPathError::MissingRoot));
        };
        let mut steps = Vec::new();
        while !rest.is_empty() {
            let at = text.len() - rest.len();
            let (step, remaining) = if let Some(after_dot) = rest.strip_prefix('.') {
                Self::parse_member_name(after_dot, at + 1)?
            } else if let Some(after_bracket) = rest.strip_prefix('[') {
                Self::parse_bracket(after_bracket, at + 1)?
            } else {
                return Err(Report::new(JsonPathError::ExpectedStep { at }));
            };
            steps.push(step);
            rest = remaining;
        }
        Self::new(steps)
    }

    /// A path taking `steps` from the root.
    pub fn new(steps: Vec<JsonPathStep>) -> Result<Self, Report<JsonPathError>> {
        if steps.len() > Self::MAX_STEPS {
            return Err(Report::new(JsonPathError::TooManySteps {
                max: Self::MAX_STEPS,
            }));
        }
        Ok(Self { steps })
    }

    pub fn steps(&self) -> &[JsonPathStep] {
        &self.steps
    }

    /// Reads the identifier after a `.`, which is at byte `at` of the whole path.
    fn parse_member_name(
        text: &str,
        at: usize,
    ) -> Result<(JsonPathStep, &str), Report<JsonPathError>> {
        let length = text
            .bytes()
            .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
            .count();
        let (name, rest) = text.split_at(length);
        let starts_with_digit = name
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_digit());
        if name.is_empty() || starts_with_digit {
            return Err(Report::new(JsonPathError::InvalidMemberName { at }));
        }
        Ok((JsonPathStep::Member(name.to_string()), rest))
    }

    /// Reads what follows a `[`, which is at byte `at` of the whole path, through its `]`.
    fn parse_bracket(text: &str, at: usize) -> Result<(JsonPathStep, &str), Report<JsonPathError>> {
        let (step, rest) = match text.bytes().next() {
            Some(b'"') => Self::parse_quoted_name(text, at)?,
            Some(byte) if byte.is_ascii_digit() => Self::parse_index(text, at)?,
            Some(_) | None => return Err(Report::new(JsonPathError::InvalidBracket { at })),
        };
        let Some(rest) = rest.strip_prefix(']') else {
            let closing = at + (text.len() - rest.len());
            return Err(Report::new(JsonPathError::UnclosedBracket { at: closing }));
        };
        Ok((step, rest))
    }

    fn parse_index(text: &str, at: usize) -> Result<(JsonPathStep, &str), Report<JsonPathError>> {
        let length = text.bytes().take_while(u8::is_ascii_digit).count();
        let (digits, rest) = text.split_at(length);
        if digits.len() > 1 && digits.starts_with('0') {
            return Err(Report::new(JsonPathError::InvalidIndex { at }));
        }
        let index = digits
            .parse::<u32>()
            .map_err(|_| Report::new(JsonPathError::InvalidIndex { at }))?;
        Ok((JsonPathStep::Element(index), rest))
    }

    /// Reads one JSON string, escapes and all, from the start of `text`.
    fn parse_quoted_name(
        text: &str,
        at: usize,
    ) -> Result<(JsonPathStep, &str), Report<JsonPathError>> {
        let mut names = serde_json::Deserializer::from_str(text).into_iter::<String>();
        let Some(Ok(name)) = names.next() else {
            return Err(Report::new(JsonPathError::InvalidQuotedName { at }));
        };
        let consumed = names.byte_offset();
        Ok((JsonPathStep::Member(name), &text[consumed..]))
    }
}

impl JsonPathStep {
    /// Whether `name` can be written after a `.`, rather than only as `["name"]`.
    fn is_bare_member_name(name: &str) -> bool {
        let mut bytes = name.bytes();
        let Some(first) = bytes.next() else {
            return false;
        };
        let starts_well = first.is_ascii_alphabetic() || first == b'_';
        starts_well && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }
}

impl Display for JsonPath {
    /// Writes the canonical form: `.name` where a member name allows it, `["name"]` otherwise.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("$")?;
        for step in &self.steps {
            match step {
                JsonPathStep::Member(name) if JsonPathStep::is_bare_member_name(name) => {
                    write!(formatter, ".{name}")?;
                }
                JsonPathStep::Member(name) => {
                    let quoted = serde_json::to_string(name).map_err(|_| fmt::Error)?;
                    write!(formatter, "[{quoted}]")?;
                }
                JsonPathStep::Element(index) => write!(formatter, "[{index}]")?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(name: &str) -> JsonPathStep {
        JsonPathStep::Member(name.to_string())
    }

    fn parsed(text: &str) -> JsonPath {
        JsonPath::parse(text).unwrap_or_else(|error| panic!("`{text}` must parse: {error:?}"))
    }

    fn defect(text: &str) -> JsonPathError {
        JsonPath::parse(text)
            .expect_err("the path must be rejected")
            .current_context()
            .clone()
    }

    #[test]
    fn parses_member_element_and_quoted_steps() {
        assert_eq!(parsed("$").steps(), &[]);
        assert_eq!(
            parsed("$.orders[12].unit_price").steps(),
            &[
                member("orders"),
                JsonPathStep::Element(12),
                member("unit_price")
            ]
        );
        assert_eq!(
            parsed(r#"$["odd key"]["café \"q\""][0]"#).steps(),
            &[
                member("odd key"),
                member("café \"q\""),
                JsonPathStep::Element(0)
            ]
        );
        assert_eq!(
            parsed("$[4294967295]").steps(),
            &[JsonPathStep::Element(u32::MAX)]
        );
    }

    #[test]
    fn rejects_malformed_paths_at_the_defect() {
        assert_eq!(defect(""), JsonPathError::MissingRoot);
        assert_eq!(defect("amount"), JsonPathError::MissingRoot);
        assert_eq!(defect("$amount"), JsonPathError::ExpectedStep { at: 1 });
        assert_eq!(defect("$."), JsonPathError::InvalidMemberName { at: 2 });
        assert_eq!(defect("$.1st"), JsonPathError::InvalidMemberName { at: 2 });
        assert_eq!(defect("$.a b"), JsonPathError::ExpectedStep { at: 3 });
        assert_eq!(defect("$[-1]"), JsonPathError::InvalidBracket { at: 2 });
        assert_eq!(defect("$['a']"), JsonPathError::InvalidBracket { at: 2 });
        assert_eq!(defect("$[01]"), JsonPathError::InvalidIndex { at: 2 });
        assert_eq!(
            defect("$[4294967296]"),
            JsonPathError::InvalidIndex { at: 2 }
        );
        assert_eq!(defect("$[1"), JsonPathError::UnclosedBracket { at: 3 });
        assert_eq!(
            defect(r#"$["a"x]"#),
            JsonPathError::UnclosedBracket { at: 5 }
        );
        assert_eq!(
            defect(r#"$["a]"#),
            JsonPathError::InvalidQuotedName { at: 2 }
        );
        assert_eq!(
            defect(r#"$["\q"]"#),
            JsonPathError::InvalidQuotedName { at: 2 }
        );
        assert_eq!(
            defect(&format!("${}", ".a".repeat(JsonPath::MAX_STEPS + 1))),
            JsonPathError::TooManySteps {
                max: JsonPath::MAX_STEPS
            }
        );
        assert!(JsonPath::parse(&format!("${}", "[0]".repeat(JsonPath::MAX_STEPS))).is_ok());
    }

    #[test]
    fn renders_the_canonical_form_that_parses_back_to_the_same_steps() {
        for (written, canonical) in [
            ("$", "$"),
            ("$.a[0]._b2", "$.a[0]._b2"),
            (r#"$["plain"]"#, "$.plain"),
            (r#"$["odd key"][3]"#, r#"$["odd key"][3]"#),
            (r#"$["café"]"#, r#"$["café"]"#),
            (r#"$["q\"\\"]"#, r#"$["q\"\\"]"#),
            (r#"$["1st"]"#, r#"$["1st"]"#),
            (r#"$[""]"#, r#"$[""]"#),
        ] {
            let path = parsed(written);
            assert_eq!(path.to_string(), canonical, "rendering {written}");
            assert_eq!(parsed(canonical), path, "reparsing {canonical}");
        }
    }
}
