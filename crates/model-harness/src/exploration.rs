//! The identity of a checked invariant and the search every model is explored under.

use std::fmt;

/// The stable name of the invariant a model checks, such as `execution.cancellation.publication`.
///
/// `just test-loom` tracks invariants by this name in the model inventory: a renamed test keeps its
/// invariant, and a model that disappears, is ignored or stops completing is noticed because its
/// name is missing from the run. A name is two or more dot-separated words of lowercase ASCII
/// letters, digits and hyphens. Declare it as a constant, so a malformed name fails to compile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvariantId(&'static str);

impl InvariantId {
    pub const fn new(name: &'static str) -> Self {
        assert!(
            is_invariant_name(name),
            "an invariant name is two or more dot-separated words of lowercase ASCII letters, \
             digits and hyphens"
        );
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for InvariantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

const fn is_invariant_name(name: &str) -> bool {
    let mut remaining = name.as_bytes();
    let mut current_word_is_empty = true;
    let mut has_separator = false;
    while let [byte, rest @ ..] = remaining {
        if *byte == b'.' {
            if current_word_is_empty {
                return false;
            }
            has_separator = true;
            current_word_is_empty = true;
        } else if byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-' {
            current_word_is_empty = false;
        } else {
            return false;
        }
        remaining = rest;
    }
    has_separator && !current_word_is_empty
}

/// The most thread switches one Loom execution may take. An execution that needs more fails its
/// check; it never ends the search early. This is Loom's own default, stated here so that a run
/// reports the limit it was explored under.
pub const LOOM_BRANCH_LIMIT: usize = 1_000;

/// The settings Loom reads from the environment that would change the search a model declares: a
/// preemption bound that skips schedules, a permutation or time budget that ends the search before
/// it is complete, and a different branch limit.
pub const REFUSED_LOOM_SETTINGS: [&str; 4] = [
    "LOOM_MAX_PREEMPTIONS",
    "LOOM_MAX_PERMUTATIONS",
    "LOOM_MAX_DURATION",
    "LOOM_MAX_BRANCHES",
];

/// The refused settings that `is_set` reports as present, in the order they are listed.
pub fn refused_loom_settings(is_set: impl Fn(&str) -> bool) -> Vec<&'static str> {
    REFUSED_LOOM_SETTINGS
        .into_iter()
        .filter(|setting| is_set(setting))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invariant_name_is_dot_separated_lowercase_words() {
        const PUBLICATION: InvariantId = InvariantId::new("execution.cancellation.publication");
        const HYPHENATED: InvariantId = InvariantId::new("execution.cancel-on-drop");
        assert_eq!(PUBLICATION.as_str(), "execution.cancellation.publication");
        assert_eq!(HYPHENATED.to_string(), "execution.cancel-on-drop");
    }

    #[test]
    fn a_malformed_invariant_name_is_refused() {
        for name in [
            "",
            "execution",
            "execution.",
            ".execution",
            "execution..cancellation",
            "Execution.cancellation",
            "execution.cancellation publication",
            "execution/cancellation",
        ] {
            assert!(!is_invariant_name(name), "{name:?} must be refused");
        }
    }

    #[test]
    #[should_panic(expected = "an invariant name is two or more dot-separated words")]
    fn a_malformed_invariant_name_does_not_construct() {
        InvariantId::new("execution");
    }

    #[test]
    fn every_setting_that_would_change_the_search_is_refused() {
        let refused = refused_loom_settings(|setting| {
            matches!(setting, "LOOM_MAX_PREEMPTIONS" | "LOOM_MAX_DURATION")
        });
        assert_eq!(refused, ["LOOM_MAX_PREEMPTIONS", "LOOM_MAX_DURATION"]);
    }

    #[test]
    fn evidence_settings_are_not_refused() {
        let evidence = [
            "LOOM_CHECKPOINT_FILE",
            "LOOM_CHECKPOINT_INTERVAL",
            "LOOM_LOG",
            "LOOM_LOCATION",
        ];
        let refused = refused_loom_settings(|setting| evidence.contains(&setting));
        assert!(refused.is_empty());
    }
}
