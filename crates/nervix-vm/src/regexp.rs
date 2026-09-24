//! Regular-expression calls: patterns prepared once per compiled program and searched without a
//! lock per row.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Compiling a pattern into a regex under explicit size limits, the per-regex pool of
//!   search caches an execution borrows once per batch, the constant pattern a call compiles once
//!   when the program is compiled, and the bounded cache a call keeps for the patterns it reads
//!   from a field.
//! - **Depends on.** The regex engine and the `regex` error vocabulary the VM reports.
//! - **Must not know.** Registers, batches, row errors, or which rows a pattern applies to.

use std::{
    collections::hash_map::Entry,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use ahash::{HashMap, HashMapExt};
use arc_swap::ArcSwapOption;
use indexmap::IndexMap;
use regex_automata::{
    Input, MatchKind,
    meta::{self, BuildError, Cache},
    util::{
        captures::Captures,
        iter::Searcher,
        pool::{Pool, PoolGuard},
        syntax,
    },
};

/// The most bytes a compiled pattern may occupy. A pattern past this limit is not compiled and
/// reports `CompiledTooBig`, whatever its source. It is the limit the `regex` crate applies.
pub const COMPILED_PATTERN_SIZE_LIMIT: usize = 10 * (1 << 20);

/// The most bytes one search cache may hold for the lazy DFA of a pattern. A search that would
/// exceed it falls back to a slower engine rather than growing the cache. It is the capacity the
/// `regex` crate applies.
pub const SEARCH_CACHE_CAPACITY: usize = 2 * (1 << 20);

/// How many distinct patterns one call keeps compiled for the patterns it reads from a field.
/// When a further pattern arrives, the pattern compiled longest ago is evicted.
pub const DYNAMIC_PATTERN_CACHE_CAPACITY: usize = 64;

/// The regular-expression builtins, which share one pattern-preparation contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegexpFunction {
    /// `regexp_like(text, pattern)`.
    Like,
    /// `regexp_replace(text, pattern, replacement)`.
    Replace,
    /// `regexp_substr(text, pattern)`.
    Substr,
    /// `regexp_extract(text, pattern, group)`.
    Extract,
}

/// One regular-expression call as the compiler lowered it: which function it is, and where its
/// pattern comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegexpCall {
    pub function: RegexpFunction,
    pub pattern: PatternSource,
}

impl RegexpCall {
    /// A call whose pattern is read from its second argument on every row.
    pub fn reading_pattern_argument(function: RegexpFunction) -> Self {
        Self {
            function,
            pattern: PatternSource::Argument(DynamicPatterns::default()),
        }
    }

    /// A call whose pattern is known when the program is compiled. The pattern is compiled here,
    /// once, and a pattern that does not compile keeps its error to report per row.
    pub fn with_constant_pattern(function: RegexpFunction, pattern: &str) -> Self {
        Self {
            function,
            pattern: PatternSource::Constant(ConstantPattern::compile(pattern)),
        }
    }
}

/// Where a call's pattern comes from.
#[derive(Debug, Clone)]
pub enum PatternSource {
    /// A pattern known when the program was compiled, compiled once and shared by every batch.
    Constant(ConstantPattern),
    /// A pattern read from the call's second argument, compiled when a row first uses it and kept
    /// in the call's bounded cache.
    Argument(DynamicPatterns),
}

impl PartialEq for PatternSource {
    /// Two sources are the same when they read the same thing: the same constant text, or the
    /// argument. What a cache currently holds is execution state, not part of the program.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Constant(left), Self::Constant(right)) => left.text == right.text,
            (Self::Argument(_), Self::Argument(_)) => true,
            (Self::Constant(_), Self::Argument(_)) | (Self::Argument(_), Self::Constant(_)) => {
                false
            }
        }
    }
}

impl Eq for PatternSource {}

/// A pattern compiled once, when the program was compiled.
#[derive(Debug, Clone)]
pub struct ConstantPattern {
    text: Box<str>,
    outcome: Arc<PatternOutcome>,
}

impl ConstantPattern {
    fn compile(text: &str) -> Self {
        Self {
            text: Box::from(text),
            outcome: Arc::new(PatternOutcome::compile(text)),
        }
    }

    /// The pattern as written.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Whether the pattern compiled. An invalid pattern reports its error on every row that
    /// evaluates the call.
    pub fn is_valid(&self) -> bool {
        match self.outcome.as_ref() {
            PatternOutcome::Regex(_) => true,
            PatternOutcome::Invalid(_) => false,
        }
    }

    pub(crate) fn outcome(&self) -> &Arc<PatternOutcome> {
        &self.outcome
    }
}

/// What compiling one pattern produced.
#[derive(Debug)]
pub enum PatternOutcome {
    Regex(PreparedRegex),
    /// The pattern is not a regular expression, or compiles past the size limit.
    Invalid(regex::Error),
}

impl PatternOutcome {
    fn compile(pattern: &str) -> Self {
        match PreparedRegex::compile(pattern) {
            Ok(regex) => Self::Regex(regex),
            Err(error) => Self::Invalid(error),
        }
    }

    /// Takes this outcome up for one batch: a regex borrows a search cache for the batch, and an
    /// invalid pattern reports its error.
    pub(crate) fn activate(&self) -> ActivePattern<'_> {
        match self {
            Self::Regex(regex) => ActivePattern::Regex(regex.activate()),
            Self::Invalid(error) => ActivePattern::Invalid(error),
        }
    }
}

/// A pattern taken up for one batch.
pub(crate) enum ActivePattern<'a> {
    Regex(ActiveRegex<'a>),
    Invalid(&'a regex::Error),
}

/// Builds the search cache a regex needs, once per thread that searches with it.
type CreateCache = Box<dyn Fn() -> Cache + Send + Sync>;

/// A compiled regex and the pool of search caches it is searched with.
///
/// Searching with a regex needs a mutable cache. The `regex` crate takes one from an internal pool
/// on every search, which on any thread but the pool's owner is a mutex acquisition per row. This
/// pool is borrowed from once per batch instead, so a batch pays at most one acquisition however
/// many rows it searches.
#[derive(Debug)]
pub struct PreparedRegex {
    regex: Arc<meta::Regex>,
    caches: Pool<Cache, CreateCache>,
}

impl PreparedRegex {
    /// Compiles `pattern` exactly as the `regex` crate compiles a `Regex`: Unicode syntax,
    /// leftmost-first matching, empty matches kept on character boundaries, and the size limits
    /// above.
    fn compile(pattern: &str) -> Result<Self, regex::Error> {
        let config = meta::Config::new()
            .match_kind(MatchKind::LeftmostFirst)
            .utf8_empty(true)
            .nfa_size_limit(Some(COMPILED_PATTERN_SIZE_LIMIT))
            .hybrid_cache_capacity(SEARCH_CACHE_CAPACITY);
        let syntax = syntax::Config::new().utf8(true);
        let regex = meta::Builder::new()
            .configure(config)
            .syntax(syntax)
            .build(pattern)
            .map_err(regex_error)?;
        let regex = Arc::new(regex);
        let create: CreateCache = {
            let regex = Arc::clone(&regex);
            Box::new(move || regex.create_cache())
        };
        Ok(Self {
            regex,
            caches: Pool::new(create),
        })
    }

    /// Borrows a search cache for one batch.
    pub(crate) fn activate(&self) -> ActiveRegex<'_> {
        ActiveRegex {
            regex: &self.regex,
            cache: self.caches.get(),
            captures: None,
        }
    }
}

/// The `regex` crate's error for a build failure: the size limit a pattern exceeded, or the text
/// of its syntax error, which is what the crate itself reports for the same pattern.
fn regex_error(error: BuildError) -> regex::Error {
    if let Some(limit) = error.size_limit() {
        return regex::Error::CompiledTooBig(limit);
    }
    if let Some(syntax) = error.syntax_error() {
        return regex::Error::Syntax(syntax.to_string());
    }
    regex::Error::Syntax(error.to_string())
}

/// A regex with a search cache borrowed for one batch. Every search reuses the cache, so no row
/// takes a lock or allocates a cache of its own.
pub(crate) struct ActiveRegex<'a> {
    regex: &'a meta::Regex,
    cache: PoolGuard<'a, Cache, CreateCache>,
    /// Capture groups, allocated the first time a replacement refers to one.
    captures: Option<Captures>,
}

impl ActiveRegex<'_> {
    /// Whether the regex matches anywhere in `text`.
    pub(crate) fn is_match(&mut self, text: &str) -> bool {
        let input = Input::new(text).earliest(true);
        self.regex
            .search_half_with(&mut self.cache, &input)
            .is_some()
    }

    /// The leftmost-first match in `text`.
    pub(crate) fn find<'t>(&mut self, text: &'t str) -> Option<&'t str> {
        let input = Input::new(text);
        let matched = self.regex.search_with(&mut self.cache, &input)?;
        Some(&text[matched.range()])
    }

    /// The numbered capture of the first leftmost match, or no value when either is absent.
    pub(crate) fn extract<'t>(&mut self, text: &'t str, group: usize) -> Option<&'t str> {
        let captures = self
            .captures
            .get_or_insert_with(|| self.regex.create_captures());
        self.regex
            .search_captures_with(&mut self.cache, &Input::new(text), captures);
        let span = captures.get_group(group)?;
        Some(&text[span.range()])
    }

    /// Appends `text` to `output` with every match replaced by `replacement`, where `$1`,
    /// `${name}` and `$$` in the replacement expand as the `regex` crate expands them. A
    /// replacement without `$` is copied without resolving capture groups.
    pub(crate) fn replace_all_into(&mut self, text: &str, replacement: &str, output: &mut String) {
        let mut searcher = Searcher::new(Input::new(text));
        let mut copied_until = 0;
        if replacement.contains('$') {
            let captures = self
                .captures
                .get_or_insert_with(|| self.regex.create_captures());
            while let Some(matched) = searcher.advance(|input| {
                self.regex
                    .search_captures_with(&mut self.cache, input, captures);
                Ok(captures.get_match())
            }) {
                output.push_str(&text[copied_until..matched.start()]);
                captures.interpolate_string_into(text, replacement, output);
                copied_until = matched.end();
            }
        } else {
            while let Some(matched) =
                searcher.advance(|input| Ok(self.regex.search_with(&mut self.cache, input)))
            {
                output.push_str(&text[copied_until..matched.start()]);
                output.push_str(replacement);
                copied_until = matched.end();
            }
        }
        output.push_str(&text[copied_until..]);
    }
}

/// Compiled patterns keyed by their text, in the order they were compiled.
type PatternTable = IndexMap<Box<str>, Arc<PatternOutcome>, ahash::RandomState>;

/// The bounded cache one call keeps for the patterns it reads from a field.
///
/// Readers load one immutable snapshot per batch and resolve every row against it without a lock.
/// A pattern the snapshot lacks is compiled and published in a new snapshot; publication retries
/// on a concurrent publication, so a compilation is never repeated for the retry. The cache holds
/// at most [`DYNAMIC_PATTERN_CACHE_CAPACITY`] patterns and evicts the one compiled longest ago.
///
/// The cache belongs to the compiled program that created it: a program compiled again starts with
/// an empty cache, and a clone of the program shares the cache with its original.
#[derive(Debug, Clone, Default)]
pub struct DynamicPatterns {
    shared: Arc<PatternCache>,
}

#[derive(Debug, Default)]
struct PatternCache {
    table: ArcSwapOption<PatternTable>,
    compilations: AtomicUsize,
    evictions: AtomicUsize,
}

/// How a call's dynamic-pattern cache has been used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DynamicPatternStatistics {
    /// How many patterns the cache compiled since the program was compiled.
    pub compiled: usize,
    /// How many compiled patterns were evicted to stay within capacity.
    pub evicted: usize,
    /// How many compiled patterns the cache holds now.
    pub cached: usize,
}

impl DynamicPatterns {
    pub fn statistics(&self) -> DynamicPatternStatistics {
        let cached = match self.shared.table.load().as_ref() {
            Some(table) => table.len(),
            None => 0,
        };
        DynamicPatternStatistics {
            compiled: self.shared.compilations.load(Ordering::Relaxed),
            evicted: self.shared.evictions.load(Ordering::Relaxed),
            cached,
        }
    }

    /// Resolves the pattern of every row of one batch, compiling each distinct pattern the cache
    /// lacks once. A `None` pattern is a null, which resolves to no pattern.
    pub(crate) fn resolve_rows<'p>(
        &self,
        patterns: impl Iterator<Item = Option<&'p str>>,
    ) -> BatchPatterns {
        let snapshot = self.shared.table.load();
        let mut outcomes = Vec::new();
        let mut slots_by_text = HashMap::new();
        let mut rows = Vec::with_capacity(patterns.size_hint().0);
        let mut previous: Option<(&'p str, usize)> = None;
        for pattern in patterns {
            let Some(text) = pattern else {
                rows.push(None);
                continue;
            };
            // A pattern column usually repeats the previous row's pattern, which is decided by
            // one comparison rather than a hash lookup.
            if let Some((previous_text, slot)) = previous
                && previous_text == text
            {
                rows.push(Some(slot));
                continue;
            }
            let slot = match slots_by_text.entry(text) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let outcome = self.shared.resolve(snapshot.as_ref(), text);
                    outcomes.push(outcome);
                    let slot = outcomes.len() - 1;
                    entry.insert(slot);
                    slot
                }
            };
            previous = Some((text, slot));
            rows.push(Some(slot));
        }
        BatchPatterns {
            outcomes,
            rows: RowPatterns::PerRow(rows),
        }
    }
}

impl PatternCache {
    fn resolve(&self, snapshot: Option<&Arc<PatternTable>>, text: &str) -> Arc<PatternOutcome> {
        if let Some(table) = snapshot
            && let Some(outcome) = table.get(text)
        {
            return Arc::clone(outcome);
        }
        let outcome = Arc::new(PatternOutcome::compile(text));
        self.compilations.fetch_add(1, Ordering::Relaxed);
        let mut evicted = false;
        self.table.rcu(|current| {
            let mut next = match current {
                Some(table) => PatternTable::clone(table),
                None => PatternTable::default(),
            };
            // The closure runs again after a concurrent publication, so the eviction is decided
            // afresh each time and counted once, after the publication that stuck.
            evicted = false;
            if !next.contains_key(text) {
                if next.len() >= DYNAMIC_PATTERN_CACHE_CAPACITY {
                    next.shift_remove_index(0);
                    evicted = true;
                }
                next.insert(Box::from(text), Arc::clone(&outcome));
            }
            Some(Arc::new(next))
        });
        if evicted {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
        outcome
    }
}

/// The patterns one batch of rows uses, each distinct pattern resolved once.
pub(crate) struct BatchPatterns {
    outcomes: Vec<Arc<PatternOutcome>>,
    rows: RowPatterns,
}

enum RowPatterns {
    /// Every row uses the one resolved pattern.
    Shared,
    /// Each row names the resolved pattern it uses, or no pattern when its pattern is null.
    PerRow(Vec<Option<usize>>),
}

impl BatchPatterns {
    /// The patterns of a batch whose rows all use one pattern.
    pub(crate) fn shared(outcome: Arc<PatternOutcome>) -> Self {
        Self {
            outcomes: vec![outcome],
            rows: RowPatterns::Shared,
        }
    }

    /// The distinct patterns of the batch, in the order rows first used them.
    pub(crate) fn outcomes(&self) -> &[Arc<PatternOutcome>] {
        &self.outcomes
    }

    /// The index into [`Self::outcomes`] of the pattern `row` uses, or `None` when the row's
    /// pattern is null.
    pub(crate) fn slot(&self, row: usize) -> Option<usize> {
        match &self.rows {
            RowPatterns::Shared => Some(0),
            RowPatterns::PerRow(rows) => rows[row],
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::{
        ActivePattern, BatchPatterns, DYNAMIC_PATTERN_CACHE_CAPACITY, DynamicPatternStatistics,
        DynamicPatterns, PatternOutcome, PatternSource, RegexpCall, RegexpFunction,
    };

    fn active(outcome: &PatternOutcome) -> super::ActiveRegex<'_> {
        match outcome.activate() {
            ActivePattern::Regex(regex) => regex,
            ActivePattern::Invalid(error) => panic!("pattern must compile: {error}"),
        }
    }

    #[test]
    fn captures_include_full_optional_and_empty_matches() {
        let outcome = PatternOutcome::compile("(a)?(b)");
        let mut regex = active(&outcome);
        assert_eq!(regex.extract("b", 0), Some("b"));
        assert_eq!(regex.extract("b", 1), None);
        assert_eq!(regex.extract("b", 2), Some("b"));
        assert_eq!(regex.extract("x", 0), None);
        assert_eq!(regex.extract("ab", 1), Some("a"));
        assert_eq!(regex.extract("ab", 3), None);

        let empty = PatternOutcome::compile("");
        assert_eq!(active(&empty).extract("é", 0), Some(""));
    }

    #[rstest]
    #[case("h[a-z]+", "hello world", "XX")]
    #[case("(?P<first>\\w)(\\w*)", "hello big world", "${first}_$2!")]
    #[case("o", "foo boo", "$$")]
    #[case("a*", "baaac", "-")]
    #[case("", "héllo", "|")]
    #[case("(\\d+)", "a1b22c333", "<$1>")]
    #[case("x", "no match here", "y")]
    #[case("ß", "straße", "ss")]
    fn searches_agree_with_the_regex_crate(
        #[case] pattern: &str,
        #[case] text: &str,
        #[case] replacement: &str,
    ) {
        let expected = regex::Regex::new(pattern).expect("pattern must compile");
        let outcome = PatternOutcome::compile(pattern);
        let mut regex = active(&outcome);

        assert_eq!(regex.is_match(text), expected.is_match(text));
        assert_eq!(
            regex.find(text),
            expected.find(text).map(|matched| matched.as_str())
        );
        let mut replaced = String::new();
        regex.replace_all_into(text, replacement, &mut replaced);
        assert_eq!(replaced, expected.replace_all(text, replacement));
    }

    #[test]
    fn build_errors_carry_the_regex_crate_vocabulary() {
        let unclosed = String::from("(");
        let expected =
            regex::Regex::new(&unclosed).expect_err("an unclosed group must not compile");
        let PatternOutcome::Invalid(error) = PatternOutcome::compile(&unclosed) else {
            panic!("an unclosed group must not compile");
        };
        assert_eq!(error, expected);
        assert_eq!(error.to_string(), expected.to_string());

        let too_big = "(?:\\pL{1000}){1000}";
        let expected = regex::Regex::new(too_big).expect_err("the pattern must exceed the limit");
        let PatternOutcome::Invalid(error) = PatternOutcome::compile(too_big) else {
            panic!("the pattern must exceed the limit");
        };
        assert_eq!(error, expected);
        assert!(matches!(error, regex::Error::CompiledTooBig(_)));
    }

    #[test]
    fn constant_patterns_keep_their_text_and_validity() {
        let valid = RegexpCall::with_constant_pattern(RegexpFunction::Like, "a+");
        let PatternSource::Constant(pattern) = &valid.pattern else {
            panic!("a literal pattern is constant");
        };
        assert_eq!(pattern.text(), "a+");
        assert!(pattern.is_valid());

        let invalid = RegexpCall::with_constant_pattern(RegexpFunction::Substr, "(");
        let PatternSource::Constant(pattern) = &invalid.pattern else {
            panic!("a literal pattern is constant");
        };
        assert!(!pattern.is_valid());

        assert_eq!(
            valid,
            RegexpCall::with_constant_pattern(RegexpFunction::Like, "a+")
        );
        assert_ne!(
            valid,
            RegexpCall::with_constant_pattern(RegexpFunction::Like, "b+")
        );
        assert_ne!(
            valid,
            RegexpCall::reading_pattern_argument(RegexpFunction::Like)
        );
        assert_eq!(
            RegexpCall::reading_pattern_argument(RegexpFunction::Like),
            RegexpCall::reading_pattern_argument(RegexpFunction::Like)
        );
    }

    #[test]
    fn rows_resolve_each_distinct_pattern_once() {
        let cache = DynamicPatterns::default();

        let batch = cache.resolve_rows(
            [
                Some("a+"),
                Some("a+"),
                Some("b+"),
                None,
                Some("a+"),
                Some("("),
            ]
            .into_iter(),
        );

        assert_eq!(batch.outcomes().len(), 3);
        assert_eq!(batch.slot(0), Some(0));
        assert_eq!(batch.slot(1), Some(0));
        assert_eq!(batch.slot(2), Some(1));
        assert_eq!(batch.slot(3), None);
        assert_eq!(batch.slot(4), Some(0));
        assert_eq!(batch.slot(5), Some(2));
        assert!(matches!(
            batch.outcomes()[2].as_ref(),
            PatternOutcome::Invalid(_)
        ));
        assert_eq!(
            cache.statistics(),
            DynamicPatternStatistics {
                compiled: 3,
                evicted: 0,
                cached: 3,
            }
        );

        let again = cache.resolve_rows([Some("b+"), Some("a+")].into_iter());
        assert!(Arc::ptr_eq(&again.outcomes()[0], &batch.outcomes()[1]));
        assert!(Arc::ptr_eq(&again.outcomes()[1], &batch.outcomes()[0]));
        assert_eq!(cache.statistics().compiled, 3);
    }

    #[test]
    fn dynamic_patterns_evict_the_pattern_compiled_longest_ago() {
        let cache = DynamicPatterns::default();
        let patterns = (0..=DYNAMIC_PATTERN_CACHE_CAPACITY)
            .map(|index| format!("p{index}"))
            .collect::<Vec<_>>();

        for pattern in &patterns {
            cache.resolve_rows([Some(pattern.as_str())].into_iter());
        }

        assert_eq!(
            cache.statistics(),
            DynamicPatternStatistics {
                compiled: DYNAMIC_PATTERN_CACHE_CAPACITY + 1,
                evicted: 1,
                cached: DYNAMIC_PATTERN_CACHE_CAPACITY,
            }
        );

        cache.resolve_rows([Some("p1")].into_iter());
        assert_eq!(
            cache.statistics().compiled,
            DYNAMIC_PATTERN_CACHE_CAPACITY + 1
        );

        cache.resolve_rows([Some("p0")].into_iter());
        assert_eq!(
            cache.statistics().compiled,
            DYNAMIC_PATTERN_CACHE_CAPACITY + 2
        );
        assert_eq!(cache.statistics().evicted, 2);
        assert_eq!(cache.statistics().cached, DYNAMIC_PATTERN_CACHE_CAPACITY);
    }

    #[test]
    fn a_clone_shares_the_cache_and_a_new_call_starts_empty() {
        let call = RegexpCall::reading_pattern_argument(RegexpFunction::Like);
        let PatternSource::Argument(cache) = &call.pattern else {
            panic!("the call reads its pattern argument");
        };
        cache.resolve_rows([Some("a+")].into_iter());

        let clone = call.clone();
        let PatternSource::Argument(shared) = &clone.pattern else {
            panic!("the clone reads its pattern argument");
        };
        assert_eq!(shared.statistics().compiled, 1);

        let recompiled = RegexpCall::reading_pattern_argument(RegexpFunction::Like);
        let PatternSource::Argument(fresh) = &recompiled.pattern else {
            panic!("the call reads its pattern argument");
        };
        assert_eq!(fresh.statistics().compiled, 0);
    }

    #[test]
    fn shared_batch_patterns_answer_slot_zero_for_every_row() {
        let outcome = Arc::new(PatternOutcome::compile("a"));
        let batch = BatchPatterns::shared(outcome);
        assert_eq!(batch.slot(0), Some(0));
        assert_eq!(batch.slot(1_000), Some(0));
        assert_eq!(batch.outcomes().len(), 1);
    }
}
