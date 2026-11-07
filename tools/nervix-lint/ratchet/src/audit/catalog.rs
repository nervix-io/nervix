//! Repository tooling, outside the product layer order.
//! Owns: the historical counter spellings and the counts each audit pattern may implicate.
//! Depends on: maintained regular expressions and resolved audit patterns.
//! Must not know: current product source classifications or enforcement policy.

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_lint_report::audit::AuditPattern;
use regex::Regex;

use super::candidates::Pattern;

pub(super) struct Rule {
    pub name: &'static str,
    pub spelling: Option<Regex>,
    pub scoped: bool,
    start_word: bool,
    end_word: bool,
    word: Regex,
}

impl Rule {
    fn new(name: &'static str, spelling: Option<&str>, scoped: bool) -> Self {
        // These counter expressions used Python's Unicode word/space categories. Rust regex's
        // word category also includes marks and join controls; preserve the counter's categories
        // explicitly while the maintained regex engine still parses and matches every spelling.
        let start_word = spelling.is_some_and(|text| text.starts_with(r"\b"));
        let end_word = spelling.is_some_and(|text| text.ends_with(r"\b"));
        let spelling = if let Some(text) = spelling {
            let text = text.strip_prefix(r"\b").unwrap_or(text);
            let text = text.strip_suffix(r"\b").unwrap_or(text);
            let text = text
                .replace(r"\w", r"[\p{L}\p{N}_]")
                .replace(r"\s", r"[\s\x1c-\x1f]");
            Some(Regex::new(&text).assured("the checked-in historical expression is valid"))
        } else {
            None
        };
        let word =
            Regex::new(r"\A[\p{L}\p{N}_]\z").assured("the historical word category is fixed");
        Self {
            name,
            spelling,
            scoped,
            start_word,
            end_word,
            word,
        }
    }

    pub fn count(&self, text: &str) -> usize {
        let Some(spelling) = &self.spelling else {
            return 0;
        };
        let mut count = 0_usize;
        let mut cursor = 0;
        while let Some(found) = spelling.find_at(text, cursor) {
            if self.boundaries_match(text, &found) {
                count = count
                    .checked_add(1)
                    .assured("accepted matches occupy disjoint bytes in the input string");
                cursor = found.end();
            } else {
                // A rejected outer match can contain a valid inner spelling. Resume after its
                // first character, as an expression with the boundary in it would search.
                let first = text[found.start()..]
                    .chars()
                    .next()
                    .verified("the fixed counter expressions never match an empty string");
                cursor = found
                    .start()
                    .checked_add(first.len_utf8())
                    .verified("the first matched character is inside the input");
            }
        }
        count
    }

    fn boundaries_match(&self, text: &str, found: &regex::Match<'_>) -> bool {
        if self.start_word
            && let Some(previous) = text[..found.start()].chars().next_back()
            && self.is_word(previous)
        {
            return false;
        }
        if self.end_word
            && let Some(next) = text[found.end()..].chars().next()
            && self.is_word(next)
        {
            return false;
        }
        true
    }

    fn is_word(&self, character: char) -> bool {
        let mut buffer = [0_u8; 4];
        self.word.is_match(character.encode_utf8(&mut buffer))
    }

    pub fn count_lines(&self, lines: &[String]) -> usize {
        let mut total = 0_usize;
        for line in lines {
            total = total
                .checked_add(self.count(line))
                .assured("matches occupy disjoint bytes in the allocated input lines");
        }
        total
    }
}

pub(super) struct Catalog {
    rules: BTreeMap<&'static str, Rule>,
}

impl Catalog {
    pub fn new() -> Self {
        let rules = [
            Rule::new(
                "data_plane_lock_acquisitions",
                Some(r"\.\s*(?:lock|read|write|entry)\s*\("),
                true,
            ),
            Rule::new(
                "bare_unwrap_and_expect",
                Some(r"\.\s*(?:unwrap\s*\(\s*\)|expect\s*\()"),
                false,
            ),
            Rule::new(
                "clamped_arithmetic",
                Some(r"\b(?:saturating|wrapping)_[a-z_]+\s*\("),
                false,
            ),
            Rule::new(
                "as_casts",
                Some(r"\bas\s+(?:[ui](?:8|16|32|64|128|size)|f32|f64)\b"),
                false,
            ),
            Rule::new("discarded_results", Some(r"\blet\s+_\s*="), false),
            Rule::new(
                "result_string_errors",
                Some(r"\bResult\s*<[^;{}]*,\s*String\s*>"),
                false,
            ),
            Rule::new(
                "bare_error_signatures",
                Some(
                    r"->\s*(?:[A-Za-z_]\w*\s*::\s*)*Result\s*<[^;{}]*,\s*(?:[A-Za-z_]\w*\s*::\s*)*[A-Z]\w*Error\s*>",
                ),
                false,
            ),
            Rule::new(
                "combinator_control_flow",
                Some(
                    r"\.\s*(?:map_or_else|map_or|and_then|ok_or_else|unwrap_or_else|or_else)\s*\(",
                ),
                false,
            ),
            Rule::new("model_matches_in_data_plane", Some(r"\bModel\s*::"), false),
            Rule::new(
                "parser_imports_outside_edges",
                Some(r"\bnervix_plexus\s*::"),
                false,
            ),
            Rule::new("files_over_3000_lines", None, false),
            Rule::new("functions_over_complexity_30", None, false),
        ];
        Self {
            rules: rules.into_iter().map(|rule| (rule.name, rule)).collect(),
        }
    }

    pub fn rules(&self) -> impl Iterator<Item = &Rule> {
        self.rules.values()
    }

    pub fn owners(&self, pattern: &Pattern) -> impl Iterator<Item = &Rule> {
        let names: &[&str] = match pattern {
            Pattern::Resolved(
                AuditPattern::RepeatedLookup { .. } | AuditPattern::KeyAllocation { .. },
            )
            | Pattern::Relocated(_) => &["data_plane_lock_acquisitions"],
            Pattern::Resolved(AuditPattern::BorrowedCopy { .. }) => &[
                "data_plane_lock_acquisitions",
                "bare_unwrap_and_expect",
                "combinator_control_flow",
            ],
            Pattern::Resolved(AuditPattern::DuplicatedBranch { .. }) => &[
                "data_plane_lock_acquisitions",
                "combinator_control_flow",
                "bare_unwrap_and_expect",
            ],
            Pattern::Resolved(AuditPattern::ForwardingWrapper { .. }) => &[
                "files_over_3000_lines",
                "functions_over_complexity_30",
                "data_plane_lock_acquisitions",
                "model_matches_in_data_plane",
                "parser_imports_outside_edges",
            ],
            Pattern::Resolved(AuditPattern::DiscardedError { .. }) => &[
                "bare_unwrap_and_expect",
                "discarded_results",
                "result_string_errors",
                "bare_error_signatures",
                "as_casts",
            ],
            Pattern::Resolved(AuditPattern::DisguisedClamp { .. }) => {
                &["clamped_arithmetic", "as_casts"]
            }
        };
        names.iter().map(move |name| &self.rules[name])
    }
}
