//! Bounded columnar string splitting, joining, search, and Unicode normalization.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Building Arrow string and list outputs for text functions and bounding pattern sets.
//! - **Depends on.** Arrow arrays, the linear-time Aho-Corasick matcher, and VM row errors.
//! - **Must not know.** NSPL Models, graph ownership, branches, or external connectors.

use std::{
    hash::{Hash, Hasher},
    num::NonZeroUsize,
};

use aho_corasick::AhoCorasick;
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, ListArray, StringArray,
    builder::{BooleanBuilder, ListBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field};
use arrow_string::like::{ilike, like};
use error_stack::Report;
use indexmap::{Equivalent, IndexMap};
use meticulous::OptionExt as _;
use triomphe::Arc;
use unicode_normalization::UnicodeNormalization;

use crate::{
    TypedArray,
    error::{RowErrors, RuntimeError, SideError, SideErrorReason, TextOperation},
    operand::Operand,
    program::Span,
    text_column::TextColumnBuilder,
};

const MAX_SPLIT_PARTS: usize = 65_536;
const MAX_PATTERNS: usize = 128;
const MAX_PATTERN_BYTES: usize = 64 * 1024;
const PATTERN_SETS_PER_BATCH: usize = 64;
const LIKE_PATTERN_BYTES_LIMIT: usize = 4 * 1024;

/// A borrowed cache query over one Arrow list. Hashing matches `Vec<String>` exactly, including
/// the number of non-null patterns, so a hit needs no owned strings or temporary vector.
struct PatternLookup<'a> {
    items: &'a StringArray,
}

impl PatternLookup<'_> {
    fn count(&self) -> usize {
        self.items
            .len()
            .checked_sub(self.items.null_count())
            .assured("Arrow null count cannot exceed array length")
    }
}

impl Hash for PatternLookup<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.count().hash(state);
        for pattern in self.items.iter().flatten() {
            pattern.hash(state);
        }
    }
}

impl Equivalent<Vec<String>> for PatternLookup<'_> {
    fn equivalent(&self, key: &Vec<String>) -> bool {
        if self.count() != key.len() {
            return false;
        }
        self.items
            .iter()
            .flatten()
            .zip(key)
            .all(|(pattern, cached)| pattern == cached)
    }
}

struct JoinedSize {
    parts: usize,
    bytes: Option<usize>,
}

impl JoinedSize {
    fn new() -> Self {
        Self {
            parts: 0,
            bytes: Some(0),
        }
    }

    fn add(&mut self, length: usize) {
        self.parts = self
            .parts
            .checked_add(1)
            .assured("a function's input count fits memory and therefore usize");
        self.bytes = match self.bytes {
            Some(bytes) => bytes.checked_add(length),
            None => None,
        };
    }

    fn total(&self, separator_bytes: usize) -> Option<usize> {
        let bytes = self.bytes?;
        let separators = if self.parts > 1 {
            self.parts
                .checked_sub(1)
                .verified("the branch above established at least two parts")
        } else {
            0
        };
        let extra = separator_bytes.checked_mul(separators)?;
        bytes.checked_add(extra)
    }
}

/// One multi-pattern search, with literal pattern sets prepared once with the program.
#[derive(Debug, Clone)]
pub enum ContainsAnyCall {
    Constant {
        patterns: Vec<String>,
        matcher: Option<Arc<AhoCorasick>>,
    },
    Dynamic,
}

impl PartialEq for ContainsAnyCall {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Constant { patterns: left, .. },
                Self::Constant {
                    patterns: right, ..
                },
            ) => left == right,
            (Self::Dynamic, Self::Dynamic) => true,
            _ => false,
        }
    }
}

impl Eq for ContainsAnyCall {}

impl ContainsAnyCall {
    pub(crate) fn constant(patterns: Vec<String>) -> Self {
        let bytes = patterns
            .iter()
            .try_fold(0_usize, |total, pattern| total.checked_add(pattern.len()));
        let within_bytes = match bytes {
            Some(bytes) => bytes <= MAX_PATTERN_BYTES,
            None => false,
        };
        let matcher = if patterns.is_empty() || patterns.len() > MAX_PATTERNS || !within_bytes {
            None
        } else {
            match AhoCorasick::new(&patterns) {
                Ok(matcher) => Some(Arc::new(matcher)),
                Err(_) => None,
            }
        };
        Self::Constant { patterns, matcher }
    }
}

fn failure(row: usize, reason: SideErrorReason, errors: &mut RowErrors, span: Span) {
    errors.push(row, SideError { reason, span });
}

fn list_row(input: &TypedArray, row: usize) -> error_stack::Result<Option<ArrayRef>, RuntimeError> {
    let array = input.as_array();
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return Ok(Some(list.value(row)));
    }
    if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        return Ok(Some(list.value(row)));
    }
    Err(Report::new(RuntimeError::CollectionExpectedList {
        actual: array.data_type().clone(),
    }))
}

pub(crate) fn wildcard_match(
    text: Operand<'_, StringArray>,
    pattern: Operand<'_, StringArray>,
    rows: usize,
    insensitive: bool,
    errors: &mut RowErrors,
    span: Span,
) -> error_stack::Result<BooleanArray, RuntimeError> {
    let mut oversized = false;
    for row in 0..rows {
        if pattern.is_null(row) {
            continue;
        }
        let value = pattern.array().value(pattern.index(row));
        if value.len() > LIKE_PATTERN_BYTES_LIMIT {
            oversized = true;
            failure(row, SideErrorReason::LikePatternTooLong, errors, span);
        }
    }
    if oversized {
        let bounded = StringArray::from_iter((0..rows).map(|row| {
            if pattern.is_null(row) {
                None
            } else {
                let value = pattern.array().value(pattern.index(row));
                if value.len() > LIKE_PATTERN_BYTES_LIMIT {
                    None
                } else {
                    Some(value)
                }
            }
        }));
        let bounded = Operand::Column(&bounded);
        if insensitive {
            return ilike(&text, &bounded).map_err(|source| {
                Report::new(RuntimeError::TextKernel {
                    operation: "ilike",
                    source,
                })
            });
        }
        return like(&text, &bounded).map_err(|source| {
            Report::new(RuntimeError::TextKernel {
                operation: "like",
                source,
            })
        });
    }
    if insensitive {
        ilike(&text, &pattern).map_err(|source| {
            Report::new(RuntimeError::TextKernel {
                operation: "ilike",
                source,
            })
        })
    } else {
        like(&text, &pattern).map_err(|source| {
            Report::new(RuntimeError::TextKernel {
                operation: "like",
                source,
            })
        })
    }
}

pub(crate) fn concat_ws(
    parts: &[Operand<'_, StringArray>],
    rows: usize,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> StringArray {
    let mut output = TextColumnBuilder::new(StringBuilder::new(), rows_per_value);
    let mut value = String::new();
    for row in 0..rows {
        if parts[0].is_null(row) {
            output.append_null();
            continue;
        }
        let separator = parts[0].array().value(parts[0].index(row));
        let mut size = JoinedSize::new();
        for part in &parts[1..] {
            if part.is_null(row) {
                continue;
            }
            let text = part.array().value(part.index(row));
            size.add(text.len());
        }
        let fits = match size.total(separator.len()) {
            Some(size) => output.fits(size),
            None => false,
        };
        if !fits {
            output.append_null();
            failure(
                row,
                SideErrorReason::TextTooLong(TextOperation::ConcatWs),
                errors,
                span,
            );
            continue;
        }
        value.clear();
        let mut first = true;
        for part in &parts[1..] {
            if part.is_null(row) {
                continue;
            }
            if !first {
                value.push_str(separator);
            }
            value.push_str(part.array().value(part.index(row)));
            first = false;
        }
        if !output.append_value(&value) {
            output.append_null();
            failure(
                row,
                SideErrorReason::TextTooLong(TextOperation::ConcatWs),
                errors,
                span,
            );
        }
    }
    output.finish()
}

pub(crate) fn split(
    text: &StringArray,
    delimiter: Operand<'_, StringArray>,
    errors: &mut RowErrors,
    span: Span,
) -> ListArray {
    let item = Field::new("item", DataType::Utf8, false);
    let mut output = ListBuilder::new(StringBuilder::new()).with_field(item);
    let mut values = 0_usize;
    let mut bytes = 0_usize;
    for row in 0..text.len() {
        if text.is_null(row) || delimiter.is_null(row) {
            output.append(false);
            continue;
        }
        let source = text.value(row);
        let separator = delimiter.array().value(delimiter.index(row));
        let part_count = if separator.is_empty() {
            1
        } else {
            source.split(separator).count()
        };
        if part_count > MAX_SPLIT_PARTS {
            output.append(false);
            failure(row, SideErrorReason::TooManySplitParts, errors, span);
            continue;
        }
        let next_values = values.checked_add(part_count);
        let next_bytes = bytes.checked_add(source.len());
        let (Some(next_values), Some(next_bytes)) = (next_values, next_bytes) else {
            output.append(false);
            failure(
                row,
                SideErrorReason::TextTooLong(TextOperation::Split),
                errors,
                span,
            );
            continue;
        };
        if i32::try_from(next_values).is_err() || i32::try_from(next_bytes).is_err() {
            output.append(false);
            failure(
                row,
                SideErrorReason::TextTooLong(TextOperation::Split),
                errors,
                span,
            );
            continue;
        }
        values = next_values;
        bytes = next_bytes;
        if separator.is_empty() {
            output.values().append_value(source);
        } else {
            for part in source.split(separator) {
                output.values().append_value(part);
            }
        }
        output.append(true);
    }
    output.finish()
}

pub(crate) fn join(
    lists: &TypedArray,
    separator: Operand<'_, StringArray>,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> error_stack::Result<StringArray, RuntimeError> {
    let mut output = TextColumnBuilder::new(StringBuilder::new(), rows_per_value);
    let mut joined = String::new();
    for row in 0..lists.len() {
        if separator.is_null(row) {
            output.append_null();
            continue;
        }
        let Some(items) = list_row(lists, row)? else {
            output.append_null();
            continue;
        };
        let Some(items) = items.as_any().downcast_ref::<StringArray>() else {
            return Err(Report::new(RuntimeError::CollectionTypeMismatch {
                operation: "join",
                expected: DataType::Utf8,
                actual: items.data_type().clone(),
            }));
        };
        let separator = separator.array().value(separator.index(row));
        let mut size = JoinedSize::new();
        for item in items.iter().flatten() {
            size.add(item.len());
        }
        let fits = match size.total(separator.len()) {
            Some(size) => output.fits(size),
            None => false,
        };
        if !fits {
            output.append_null();
            failure(
                row,
                SideErrorReason::TextTooLong(TextOperation::Join),
                errors,
                span,
            );
            continue;
        }
        joined.clear();
        let mut first = true;
        for item in items.iter().flatten() {
            if !first {
                joined.push_str(separator);
            }
            joined.push_str(item);
            first = false;
        }
        if !output.append_value(&joined) {
            output.append_null();
            failure(
                row,
                SideErrorReason::TextTooLong(TextOperation::Join),
                errors,
                span,
            );
        }
    }
    Ok(output.finish())
}

pub(crate) fn normalize_nfc(
    input: &StringArray,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> StringArray {
    let mut output = TextColumnBuilder::new(StringBuilder::new(), rows_per_value);
    let mut normalized = String::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        normalized.clear();
        let mut too_long = false;
        for character in input.value(row).nfc() {
            let next = normalized.len().checked_add(character.len_utf8());
            let fits = match next {
                Some(length) => output.fits(length),
                None => false,
            };
            if !fits {
                too_long = true;
                break;
            }
            normalized.push(character);
        }
        let appended = if too_long {
            false
        } else {
            output.append_value(&normalized)
        };
        if !appended {
            output.append_null();
            failure(
                row,
                SideErrorReason::TextTooLong(TextOperation::NormalizeNfc),
                errors,
                span,
            );
        }
    }
    output.finish()
}

pub(crate) fn contains_any(
    call: &ContainsAnyCall,
    text: &StringArray,
    lists: Option<&TypedArray>,
    errors: &mut RowErrors,
    span: Span,
) -> error_stack::Result<BooleanArray, RuntimeError> {
    let mut output = BooleanBuilder::with_capacity(text.len());
    if let ContainsAnyCall::Constant { patterns, matcher } = call {
        for row in 0..text.len() {
            if text.is_null(row) {
                output.append_null();
            } else if patterns.is_empty() {
                output.append_value(false);
            } else if let Some(matcher) = matcher {
                output.append_value(matcher.is_match(text.value(row)));
            } else {
                output.append_null();
                failure(row, SideErrorReason::PatternSetTooLarge, errors, span);
            }
        }
        return Ok(output.finish());
    }
    let lists = lists.verified("a dynamic contains_any call retains its list input");
    let mut cache: IndexMap<Vec<String>, AhoCorasick> = IndexMap::new();
    for row in 0..text.len() {
        if text.is_null(row) {
            output.append_null();
            continue;
        }
        let Some(items) = list_row(lists, row)? else {
            output.append_null();
            continue;
        };
        let Some(items) = items.as_any().downcast_ref::<StringArray>() else {
            return Err(Report::new(RuntimeError::CollectionTypeMismatch {
                operation: "contains_any",
                expected: DataType::Utf8,
                actual: items.data_type().clone(),
            }));
        };
        let mut pattern_count = 0_usize;
        let mut bytes = 0_usize;
        let mut too_large = false;
        for pattern in items.iter().flatten() {
            let Some(next_bytes) = bytes.checked_add(pattern.len()) else {
                too_large = true;
                break;
            };
            bytes = next_bytes;
            if pattern_count == MAX_PATTERNS || bytes > MAX_PATTERN_BYTES {
                too_large = true;
                break;
            }
            pattern_count += 1;
        }
        if too_large {
            output.append_null();
            failure(row, SideErrorReason::PatternSetTooLarge, errors, span);
            continue;
        }
        if pattern_count == 0 {
            output.append_value(false);
            continue;
        }
        let lookup = PatternLookup { items };
        if let Some(matcher) = cache.get(&lookup) {
            output.append_value(matcher.is_match(text.value(row)));
            continue;
        }
        let patterns: Vec<String> = items.iter().flatten().map(str::to_owned).collect();
        let Ok(matcher) = AhoCorasick::new(&patterns) else {
            output.append_null();
            failure(row, SideErrorReason::PatternSetTooLarge, errors, span);
            continue;
        };
        let matched = matcher.is_match(text.value(row));
        if cache.len() == PATTERN_SETS_PER_BATCH {
            cache.shift_remove_index(0);
        }
        cache.insert(patterns, matcher);
        output.append_value(matched);
    }
    Ok(output.finish())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        num::NonZeroUsize,
        sync::Arc as StdArc,
    };

    use arrow_array::{
        Array, StringArray,
        builder::{ListBuilder, StringBuilder},
    };
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::{
        ContainsAnyCall, PatternLookup, concat_ws, contains_any, join, normalize_nfc, split,
        wildcard_match,
    };
    use crate::{RowErrors, TypedArray, operand::Operand, program::Span};

    const SPAN: Span = Span { start: 0, end: 1 };

    #[test]
    fn split_and_join_preserve_empty_parts_and_unicode() {
        let text = StringArray::from(vec![Some("a,,b"), Some("cafe\u{301}"), None]);
        let delimiter = StringArray::from(vec![Some(","), Some(""), Some(",")]);
        let mut errors = RowErrors::new(3);
        let parts = split(&text, Operand::Column(&delimiter), &mut errors, SPAN);
        assert!(errors.is_error_free());
        let first = parts.value(0);
        let first = first
            .as_any()
            .downcast_ref::<StringArray>()
            .assured("split builds STRING items");
        assert_eq!(
            first.iter().collect::<Vec<_>>(),
            vec![Some("a"), Some(""), Some("b")]
        );
        let second = parts.value(1);
        let second = second
            .as_any()
            .downcast_ref::<StringArray>()
            .assured("split builds STRING items");
        assert_eq!(second.iter().collect::<Vec<_>>(), vec![Some("cafe\u{301}")]);
        assert!(parts.is_null(2));

        let separator = StringArray::from(vec!["|", "|", "|"]);
        let joined = join(
            &TypedArray::Generic(StdArc::new(parts)),
            Operand::Column(&separator),
            NonZeroUsize::MIN,
            &mut errors,
            SPAN,
        )
        .assured("split output is a STRING list");
        assert_eq!(
            joined.iter().collect::<Vec<_>>(),
            vec![Some("a||b"), Some("cafe\u{301}"), None]
        );
    }

    #[test]
    fn concat_ws_skips_null_values_but_keeps_empty_ones() {
        let separator = StringArray::from(vec![Some("-"), None]);
        let first = StringArray::from(vec![Some(""), Some("x")]);
        let second = StringArray::from(vec![Some("b"), Some("y")]);
        let parts = [
            Operand::Column(&separator),
            Operand::Column(&first),
            Operand::Column(&second),
        ];
        let mut errors = RowErrors::new(2);
        let joined = concat_ws(&parts, 2, NonZeroUsize::MIN, &mut errors, SPAN);
        assert_eq!(joined.iter().collect::<Vec<_>>(), vec![Some("-b"), None]);
        assert!(errors.is_error_free());
    }

    #[test]
    fn normalization_and_multi_pattern_search_keep_distinct_contracts() {
        let text = StringArray::from(vec!["cafe\u{301}", "HELLO", "nothing"]);
        let normalized = normalize_nfc(&text, NonZeroUsize::MIN, &mut RowErrors::new(3), SPAN);
        assert_eq!(normalized.value(0), "café");
        let call = ContainsAnyCall::constant(vec!["café".to_string(), "HELLO".to_string()]);
        let matches = contains_any(&call, &text, None, &mut RowErrors::new(3), SPAN)
            .assured("constant patterns need no list input");
        assert_eq!(
            matches.iter().collect::<Vec<_>>(),
            vec![Some(false), Some(true), Some(false)]
        );
    }

    #[test]
    fn dynamic_pattern_sets_are_bounded_and_evaluate_per_row() {
        let text = StringArray::from(vec!["needle", "hay", "needle"]);
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("needle");
        builder.append(true);
        builder.values().append_value("");
        builder.append(true);
        for _ in 0..129 {
            builder.values().append_value("x");
        }
        builder.append(true);
        let lists = TypedArray::Generic(StdArc::new(builder.finish()));
        let mut errors = RowErrors::new(3);
        let result = contains_any(
            &ContainsAnyCall::Dynamic,
            &text,
            Some(&lists),
            &mut errors,
            SPAN,
        )
        .assured("dynamic patterns are a STRING list");
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(true), None]
        );
        assert!(!errors.is_error_free());
    }

    #[test]
    fn borrowed_pattern_lookup_matches_owned_cache_keys() {
        let items = StringArray::from(vec![Some("é"), None, Some("needle")]);
        let lookup = PatternLookup { items: &items };
        let key = vec!["é".to_owned(), "needle".to_owned()];
        let mut borrowed_hash = DefaultHasher::new();
        lookup.hash(&mut borrowed_hash);
        let mut owned_hash = DefaultHasher::new();
        key.hash(&mut owned_hash);
        assert_eq!(borrowed_hash.finish(), owned_hash.finish());
        assert!(indexmap::Equivalent::equivalent(&lookup, &key));
        assert!(!indexmap::Equivalent::equivalent(
            &lookup,
            &vec!["needle".to_owned(), "é".to_owned()]
        ));
    }

    #[test]
    fn dynamic_pattern_sets_reuse_matching_keys_with_null_items() {
        let text = StringArray::from(vec!["éclair", "alpha", "alpha", "beta", "alpha"]);
        let mut builder = ListBuilder::new(StringBuilder::new());
        for pattern in ["é", "x", "alpha", "é", "x"] {
            builder.values().append_null();
            builder.values().append_value(pattern);
            builder.append(true);
        }
        let lists = TypedArray::Generic(StdArc::new(builder.finish()));
        let mut errors = RowErrors::new(text.len());
        let result = contains_any(
            &ContainsAnyCall::Dynamic,
            &text,
            Some(&lists),
            &mut errors,
            SPAN,
        )
        .assured("the list holds bounded STRING pattern sets");
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                Some(false)
            ]
        );
        assert!(errors.is_error_free());
    }

    #[test]
    fn like_escapes_wildcards_and_limits_pattern_bytes() {
        let text = StringArray::from(vec!["a%b", "e\u{301}", "x"]);
        let long = "x".repeat(4_097);
        let pattern = StringArray::from_iter(vec![Some("a\\%b"), Some("_"), Some(long.as_str())]);
        let mut errors = RowErrors::new(3);
        let result = wildcard_match(
            Operand::Column(&text),
            Operand::Column(&pattern),
            3,
            false,
            &mut errors,
            SPAN,
        )
        .assured("bounded LIKE patterns have valid Arrow types");
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), None]
        );
        assert!(!errors.is_error_free());
    }

    #[test]
    fn ilike_uses_unicode_loose_case_matching() {
        let text = StringArray::from(vec!["CAFÉ", "ß"]);
        let pattern = StringArray::from(vec!["café", "SS"]);
        let result = wildcard_match(
            Operand::Column(&text),
            Operand::Column(&pattern),
            2,
            true,
            &mut RowErrors::new(2),
            SPAN,
        )
        .assured("bounded ILIKE patterns have valid Arrow types");
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false)]
        );
    }

    #[test]
    fn string_outputs_refuse_values_outside_the_arrow_column_budget() {
        let separator = StringArray::from(vec!["-"]);
        let value = StringArray::from(vec!["a"]);
        let parts = [Operand::Column(&separator), Operand::Column(&value)];
        let mut concat_errors = RowErrors::new(1);
        let concat = concat_ws(&parts, 1, NonZeroUsize::MAX, &mut concat_errors, SPAN);
        assert!(concat.is_null(0));
        assert!(!concat_errors.is_error_free());

        let mut list = ListBuilder::new(StringBuilder::new());
        list.values().append_value("a");
        list.append(true);
        let mut join_errors = RowErrors::new(1);
        let joined = join(
            &TypedArray::Generic(StdArc::new(list.finish())),
            Operand::Column(&separator),
            NonZeroUsize::MAX,
            &mut join_errors,
            SPAN,
        )
        .assured("the list holds STRING items");
        assert!(joined.is_null(0));
        assert!(!join_errors.is_error_free());

        let mut normalize_errors = RowErrors::new(1);
        let normalized = normalize_nfc(&value, NonZeroUsize::MAX, &mut normalize_errors, SPAN);
        assert!(normalized.is_null(0));
        assert!(!normalize_errors.is_error_free());
    }

    #[test]
    fn split_and_literal_pattern_sets_enforce_limits() {
        let text = StringArray::from(vec![",".repeat(65_536)]);
        let separator = StringArray::from(vec![","]);
        let mut split_errors = RowErrors::new(1);
        let parts = split(&text, Operand::Column(&separator), &mut split_errors, SPAN);
        assert!(parts.is_null(0));
        assert!(!split_errors.is_error_free());

        let patterns = vec!["x".to_string(); 129];
        let call = ContainsAnyCall::constant(patterns);
        let mut pattern_errors = RowErrors::new(1);
        let searched = contains_any(&call, &text, None, &mut pattern_errors, SPAN)
            .assured("a constant pattern set has no list input");
        assert!(searched.is_null(0));
        assert!(!pattern_errors.is_error_free());
    }
}
