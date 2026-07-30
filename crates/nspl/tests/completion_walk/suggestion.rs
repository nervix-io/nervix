//! Turning a completion label back into text a user could actually type.
//!
//! The completion API returns bare `String` labels with no machine-readable kind, so the walker
//! reconstructs the convention: `ref:*` is a semantic reference, an uppercase label is a keyword
//! (or a space-separated keyword phrase) to emit verbatim, a label with no alphanumerics is
//! punctuation, and a lowercase snake_case label is a placeholder that has to be filled in.
//!
//! Every placeholder needs an entry here. A label with no entry is reported rather than guessed at,
//! so a grammar label added without a completion contract shows up in the report instead of
//! silently truncating the walk.

use ahash_compile_time::HashMap;
use thiserror::Error;

/// Prefix for every synthesized name, chosen so it lexes as an unknown word and can never collide
/// with a language keyword.
const SYNTHETIC_PREFIX: &str = "nx_";

/// Placeholders that stand for a literal rather than a name.
///
/// `alter_operation_separator` is a comma that only matches with an operation keyword behind it, so
/// applying it alone cannot parse. That is left to be reported like anything else rather than
/// special-cased away: completion offering a token you cannot type on its own is the wart.
const LITERAL_FILLERS: &[(&str, &str)] = &[
    ("alter_operation_separator", ","),
    ("array_length", "4"),
    ("batch_size", "1000"),
    ("byte_size_literal", "1MiB"),
    ("instance_count", "2"),
    ("integer_literal", "1"),
    ("max_in_flight", "10"),
    ("mqtt_qos", "1"),
    ("resource_version", "1"),
    // Deliberately constant: a conflict target has to name a column the VALUES record already
    // maps, so a fresh name per occurrence would never match.
    ("column_name", "'nx_column'"),
    ("config_key", "'nx.key'"),
    ("config_value", "'nx_value'"),
    ("duration_literal", "100ms"),
    ("endpoint_path", "'/nx/path'"),
    ("hostname", "nx-host"),
    ("iceberg_location", "'s3://nx/table'"),
    ("message_count", "100"),
    ("node_id", "nx-node"),
    ("number", "1"),
    ("number_literal", "1"),
    ("relay_capacity", "1024"),
    ("string", "'nx_value'"),
    ("string_literal", "'nx_value'"),
    ("time_rate", "1.0"),
    ("timestamp", "'2026-01-01T00:00:00Z'"),
    ("word", "nx_word"),
];

/// Placeholders that stand for a free-form expression region: the enclosing parser swallows tokens
/// up to a boundary and re-parses them through the separate `vm_program` grammar, so completion
/// inside them is not grammar-derived and the walker has to supply a body itself.
const FREE_FORM_FILLERS: &[(&str, &str)] = &[
    ("correlate_expression", "left.nx_field = right.nx_field"),
    ("default_assignments", "nx_field = 1"),
    ("deduplicate_on", "input.nx_field"),
    ("inherit_targets", "ALL"),
    ("invocations", "nx_udf()"),
    ("reorder_by", "input.nx_field"),
    ("set_assignments", "nx_out = 1"),
    ("value_expression", "1"),
    ("where_expression", "1 = 1"),
];

/// Placeholders that name something but do not end in `_name`.
const EXTRA_NAME_LABELS: &[&str] = &["consumer_group", "mqtt_topic_filter", "queue_group"];

/// What shape of continuation a completion label describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuggestionClass {
    /// A keyword or space-separated keyword phrase, emitted verbatim.
    Keyword,
    /// Punctuation, emitted verbatim.
    Punctuation,
    /// A `ref:*` semantic reference to an existing object.
    Reference,
    /// A lowercase snake_case placeholder that must be filled in.
    Placeholder,
}

impl SuggestionClass {
    /// Classify a raw completion label, or `None` if it matches no known shape.
    pub fn classify(label: &str) -> Option<Self> {
        let first = label.chars().next()?;
        if label.starts_with("ref:") {
            Some(Self::Reference)
        } else if first.is_ascii_uppercase() {
            Some(Self::Keyword)
        } else if first.is_ascii_lowercase() {
            Some(Self::Placeholder)
        } else if !label.chars().any(|ch| ch.is_ascii_alphanumeric()) {
            Some(Self::Punctuation)
        } else {
            None
        }
    }
}

/// Text the walker will append for one suggestion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Materialized {
    /// The text to append, already whitespace-joinable.
    pub text: String,
    /// Whether the text carries a canned free-form expression body. A parse failure over one of
    /// those is the walker's own fault, not a completion defect, and is reported separately.
    pub free_form: bool,
}

/// Why a label could not be turned into text.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MaterializeError {
    #[error("label matches no known completion shape")]
    UnknownShape,
    #[error("placeholder has no registered filler")]
    UnfilledPlaceholder,
}

/// Owns the label-to-text tables.
#[derive(Debug)]
pub struct Materializer {
    literals: HashMap<&'static str, &'static str>,
    free_form: HashMap<&'static str, &'static str>,
}

impl Materializer {
    pub fn new() -> Self {
        Self {
            literals: LITERAL_FILLERS.iter().copied().collect(),
            free_form: FREE_FORM_FILLERS.iter().copied().collect(),
        }
    }

    /// Turn one completion label into appendable text.
    ///
    /// `occurrence` is how many times this label has already been filled along the current path.
    /// Synthesized names carry it as a suffix so a statement that names two things does not name
    /// them the same: `ON CONFLICT (c) DO UPDATE` over a record whose only field is `c` has nothing
    /// left to update, and that would be reported against the grammar rather than the walker.
    pub fn materialize(
        &self,
        label: &str,
        occurrence: usize,
    ) -> Result<Materialized, MaterializeError> {
        let Some(class) = SuggestionClass::classify(label) else {
            return Err(MaterializeError::UnknownShape);
        };

        match class {
            SuggestionClass::Keyword | SuggestionClass::Punctuation => {
                Ok(Materialized::verbatim(label))
            }
            SuggestionClass::Reference => Ok(Materialized::verbatim(&synthetic_name(
                label.trim_start_matches("ref:"),
                occurrence,
            ))),
            SuggestionClass::Placeholder => self.fill_placeholder(label, occurrence),
        }
    }

    fn fill_placeholder(
        &self,
        label: &str,
        occurrence: usize,
    ) -> Result<Materialized, MaterializeError> {
        if let Some(filler) = self.literals.get(label) {
            return Ok(Materialized::verbatim(filler));
        }
        if let Some(body) = self.free_form.get(label) {
            return Ok(Materialized::free_form(body));
        }
        if let Some(stem) = label.strip_suffix("_name") {
            return Ok(Materialized::verbatim(&synthetic_name(stem, occurrence)));
        }
        if EXTRA_NAME_LABELS.contains(&label) {
            return Ok(Materialized::verbatim(&synthetic_name(label, occurrence)));
        }
        Err(MaterializeError::UnfilledPlaceholder)
    }
}

impl Default for Materializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Materialized {
    fn verbatim(text: &str) -> Self {
        Self {
            text: text.to_string(),
            free_form: false,
        }
    }

    pub fn free_form(text: &str) -> Self {
        Self {
            text: text.to_string(),
            free_form: true,
        }
    }
}

/// A name derived from the label it fills, so a failing reproduction says which slot it came from.
fn synthetic_name(stem: &str, occurrence: usize) -> String {
    match occurrence {
        0 => format!("{SYNTHETIC_PREFIX}{stem}"),
        n => format!("{SYNTHETIC_PREFIX}{stem}_{}", n + 1),
    }
}
