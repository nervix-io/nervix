//! Bolero properties of the continuations that completion and choice pages issue.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The round-trip and malformed-cursor properties of each page basis: an issued
//!   continuation keeps every part through its text, paging with issued continuations returns
//!   every candidate once and in order, and cursor text a client returns continues no page but the
//!   one it was issued for. Also the continuation decoder's typed refusal of each other shape.
//! - **Depends on.** The session service's page bases and continuation, the client wire's
//!   suggestion and choice messages and the shared bounded generators.
//! - **Must not know.** Sessions, how revisions advance, the grammar or how candidates are
//!   resolved.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "page continuation properties remain outside product execution"
    )
)]

use std::num::NonZeroUsize;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_client_wire::{
    Choice, ChoiceLookupRequest, ChoiceOutcome, ChoicePresentation, ChoiceSelection, ChoiceStatus,
    ChoiceTarget, ChoiceValue, DomainPaceChoice, SuggestOutcome, SuggestRequest, Suggestion,
    SuggestionKind, SuggestionStatus, TextEdit,
};
use nervix_models::{DomainName, ModelKind, ModelName, NodeRef, PlacementPolicy};

use super::super::{
    ChoicePageBasis, CompletionPageBasis, PageContinuation, PageContinuationRefusal,
};

/// The most candidates a generated page set holds: several pages at small page sizes, and less
/// than one page at large ones.
const MAX_CANDIDATES: usize = 24;

/// The most selections a generated choice question depends on.
const MAX_DEPENDENCIES: usize = 3;

/// Text that damages a continuation where it lands: the separator, signs, digits, hexadecimal and
/// other letters, whitespace, a NUL and a two-byte character. The empty piece deletes.
const DAMAGE: [&str; 13] = [
    "", ":", "::", "+", "-", "0", "9", "a", "f", "g", " ", "\u{0}", "é",
];

/// Text appended to a whole continuation: each lengthens its candidate digest or adds a part.
const SUFFIXES: [&str; 6] = [":", "0", "a", " ", "\u{0}", "é"];

/// Targets whose cursor tags the generated choice questions carry.
const TARGETS: [ChoiceTarget; 8] = [
    ChoiceTarget::DomainPace,
    ChoiceTarget::PlacementPolicy,
    ChoiceTarget::Schema,
    ChoiceTarget::Relay,
    ChoiceTarget::RelayField,
    ChoiceTarget::Resource,
    ChoiceTarget::CompletedResourceVersion,
    ChoiceTarget::PlacementMember,
];

/// Kinds of the Models that generated choices name.
const MODEL_KINDS: [ModelKind; 6] = [
    ModelKind::Schema,
    ModelKind::Relay,
    ModelKind::Reorderer,
    ModelKind::Correlator,
    ModelKind::Deduplicator,
    ModelKind::WindowProcessor,
];

#[test]
fn a_page_continuation_is_four_parts_with_decimal_numbers_and_every_other_text_is_refused() {
    let query = "0".repeat(64);
    let candidates = "f".repeat(64);
    let canonical = format!("7:2:{query}:{candidates}");
    assert_eq!(
        PageContinuation::decode(&canonical).assured("four parts with decimal numbers decode"),
        PageContinuation {
            revision: 7,
            end: 2,
            query_digest: &query,
            candidate_digest: &candidates,
        }
    );

    let three_parts = format!("7:2:{query}");
    let five_parts = format!("{canonical}:");
    let six_parts = format!("{canonical}:{candidates}:");
    for text in ["", "7", "7:", "7:2", &three_parts, &five_parts, &six_parts] {
        assert!(
            matches!(
                PageContinuation::decode(text),
                Err(PageContinuationRefusal::Shape)
            ),
            "{text:?}"
        );
    }
    for revision in ["", "-7", "seven", " 7", "7 ", "18446744073709551616"] {
        let text = format!("{revision}:2:{query}:{candidates}");
        assert!(
            matches!(
                PageContinuation::decode(&text),
                Err(PageContinuationRefusal::Revision(_))
            ),
            "{text:?}"
        );
    }
    for position in ["", "-2", "two", " 2", "2 ", "18446744073709551616"] {
        let text = format!("7:{position}:{query}:{candidates}");
        assert!(
            matches!(
                PageContinuation::decode(&text),
                Err(PageContinuationRefusal::Position(_))
            ),
            "{text:?}"
        );
    }
}

#[test]
fn bolero_completion_page_continuations_round_trip() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let position = any_position(&mut arbitrary);
            let case = CompletionCase::generate(&mut arbitrary);
            assert_continuations_round_trip(&case, position);
        });
}

#[test]
fn bolero_completion_page_cursor_text_continues_only_its_own_page() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let recipe = CursorRecipe::generate(&mut arbitrary);
            let case = CompletionCase::generate(&mut arbitrary);
            let cursor = recipe.cursor(&case);
            assert_cursor_continues_only_its_page(&case, &cursor);
        });
}

#[test]
fn bolero_choice_page_continuations_round_trip() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let position = any_position(&mut arbitrary);
            let case = ChoiceCase::generate(&mut arbitrary);
            assert_continuations_round_trip(&case, position);
        });
}

#[test]
fn bolero_choice_page_cursor_text_continues_only_its_own_page() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let recipe = CursorRecipe::generate(&mut arbitrary);
            let case = ChoiceCase::generate(&mut arbitrary);
            let cursor = recipe.cursor(&case);
            assert_cursor_continues_only_its_page(&case, &cursor);
        });
}

/// Asserts that the continuation `case`'s basis issues at `position` keeps every part through its
/// text, and that paging from the first page with each issued continuation returns every
/// candidate once and in order.
fn assert_continuations_round_trip<C: PagedCase>(case: &C, position: usize) {
    let issued = case.issued(position);
    let text = issued.encode();
    let decoded = PageContinuation::decode(&text).assured("issued continuation text decodes");
    assert_eq!(decoded, issued);
    assert_eq!(decoded.revision, case.revision());
    assert_eq!(decoded.end, position);
    assert_eq!(decoded.encode(), text);

    let candidates = case.candidates();
    let mut returned: Vec<C::Candidate> = Vec::with_capacity(candidates.len());
    let mut cursor: Option<String> = None;
    let mut pages = 0_usize;
    loop {
        let expected = expected_page(case, returned.len());
        assert_eq!(case.answer(cursor.as_deref()), Some(expected.clone()));
        pages = pages
            .checked_add(1)
            .assured("a bounded candidate set takes at most one page per candidate");
        returned.extend(expected.candidates);
        let Some(next) = expected.continuation else {
            break;
        };
        let continued = PageContinuation::decode(&next).assured("issued continuation text decodes");
        assert_eq!(continued.revision, case.revision());
        assert_eq!(continued.end, returned.len());
        assert!(returned.len() < candidates.len());
        cursor = Some(next);
    }
    assert_eq!(returned, candidates);
    assert_eq!(pages, candidates.len().div_ceil(case.page_size()).max(1));
}

/// Asserts that `cursor` continues its basis's page exactly when it is a continuation that basis
/// issued within its candidates, and that such a cursor continues no other basis's page.
fn assert_cursor_continues_only_its_page<C: PagedCase>(case: &C, cursor: &CursorText) {
    let candidates = case.candidates().len();
    let position = match PageContinuation::decode(&cursor.text) {
        Ok(decoded) if decoded == case.issued(decoded.end) && decoded.end <= candidates => {
            Some(decoded.end)
        }
        Ok(_) | Err(_) => None,
    };
    match cursor.expected {
        Expected::Continues(start) => assert_eq!(position, Some(start), "{cursor:?}"),
        Expected::Refused => assert_eq!(position, None, "{cursor:?}"),
        Expected::Either => {}
    }
    let expected = position.map(|start| expected_page(case, start));
    assert_eq!(case.answer(Some(&cursor.text)), expected, "{cursor:?}");
    if position.is_some() {
        for other in case.other_answers(&cursor.text) {
            assert_eq!(other, None, "another basis continued {cursor:?}");
        }
    }
}

/// The page `case`'s basis answers from `start`: at most a page of its candidates from there,
/// and the continuation it issues while candidates remain.
fn expected_page<C: PagedCase>(case: &C, start: usize) -> Page<C::Candidate> {
    let candidates = case.candidates();
    let end = start
        .checked_add(case.page_size())
        .assured("a position within bounded candidates leaves room for a page")
        .min(candidates.len());
    let continuation = if end < candidates.len() {
        Some(case.issued(end).encode())
    } else {
        None
    };
    Page {
        candidates: candidates[start..end].to_vec(),
        continuation,
    }
}

/// A page a basis answered `Ready`.
#[derive(Debug, Clone, PartialEq)]
struct Page<T> {
    candidates: Vec<T>,
    continuation: Option<String>,
}

/// The page a completion outcome holds, or `None` for `StaleContext`, which holds nothing.
fn completion_page(outcome: SuggestOutcome) -> Option<Page<Suggestion>> {
    if outcome.status == SuggestionStatus::Ready {
        return Some(Page {
            candidates: outcome.suggestions,
            continuation: outcome.continuation,
        });
    }
    assert_eq!(outcome.status, SuggestionStatus::StaleContext);
    assert!(outcome.suggestions.is_empty());
    assert_eq!(outcome.continuation, None);
    None
}

/// The page a choice outcome holds, or `None` for `StaleContext`, which holds nothing.
fn choice_page(outcome: ChoiceOutcome) -> Option<Page<Choice>> {
    if outcome.status == ChoiceStatus::Ready {
        return Some(Page {
            candidates: outcome.choices,
            continuation: outcome.page_cursor,
        });
    }
    assert_eq!(outcome.status, ChoiceStatus::StaleContext);
    assert!(outcome.choices.is_empty());
    assert_eq!(outcome.page_cursor, None);
    None
}

/// One generated basis with its question, page size and candidates, answered through the basis's
/// production `page`.
trait PagedCase {
    type Candidate: Clone + PartialEq + std::fmt::Debug;

    fn revision(&self) -> u64;

    fn page_size(&self) -> usize;

    fn candidates(&self) -> &[Self::Candidate];

    /// The continuation the basis issues for a page of its candidates that ends at `end`.
    fn issued(&self, end: usize) -> PageContinuation<'_>;

    /// The basis's page when the question returns `cursor`.
    fn answer(&self, cursor: Option<&str>) -> Option<Page<Self::Candidate>>;

    /// The pages answered to `cursor` by bases that each differ from this one in one bound part:
    /// the revision, the question or the candidates.
    fn other_answers(&self, cursor: &str) -> Vec<Option<Page<Self::Candidate>>>;
}

/// A completion basis over a generated input, cursor, domain, revision and suggestions.
struct CompletionCase {
    input: String,
    cursor: usize,
    domain: Option<DomainName>,
    page_size: u16,
    candidates: Vec<Suggestion>,
    basis: CompletionPageBasis,
    candidate_digest: String,
}

impl CompletionCase {
    fn generate(arbitrary: &mut Arbitrary<'_>) -> Self {
        let revision = arbitrary.entropy().any_u64();
        let input = arbitrary.string();
        let mut boundaries: Vec<usize> = input.char_indices().map(|(index, _)| index).collect();
        boundaries.push(input.len());
        let options = NonZeroUsize::new(boundaries.len())
            .verified("the input's end is one of its boundaries");
        let cursor = boundaries[arbitrary.entropy().index(options)];
        let domain = if arbitrary.entropy().flag() {
            Some(arbitrary.name::<DomainName>())
        } else {
            None
        };
        let page_size = page_size(arbitrary);
        let count = arbitrary.entropy().count(MAX_CANDIDATES);
        let mut candidates = Vec::with_capacity(count);
        for _ in 0..count {
            candidates.push(suggestion(arbitrary));
        }
        let question = SuggestRequest::new(input.clone(), cursor, domain.clone())
            .verified("the cursor is one of the input's character boundaries");
        let basis = CompletionPageBasis::new(&question, revision);
        let candidate_digest = CompletionPageBasis::candidate_digest(&candidates);
        Self {
            input,
            cursor,
            domain,
            page_size,
            candidates,
            basis,
            candidate_digest,
        }
    }

    /// The question for `input` at this case's cursor, domain and page size, returning
    /// `continuation`.
    fn question(&self, input: &str, continuation: Option<&str>) -> SuggestRequest {
        SuggestRequest::new(input.to_string(), self.cursor, self.domain.clone())
            .assured("the cursor is a character boundary of the input and of every extension of it")
            .with_page(self.page_size, continuation.map(str::to_string))
            .assured("generated page sizes are from 1 through 100")
    }
}

impl PagedCase for CompletionCase {
    type Candidate = Suggestion;

    fn revision(&self) -> u64 {
        self.basis.revision
    }

    fn page_size(&self) -> usize {
        usize::from(self.page_size)
    }

    fn candidates(&self) -> &[Suggestion] {
        &self.candidates
    }

    fn issued(&self, end: usize) -> PageContinuation<'_> {
        self.basis.continuation(end, &self.candidate_digest)
    }

    fn answer(&self, cursor: Option<&str>) -> Option<Page<Suggestion>> {
        let question = self.question(&self.input, cursor);
        completion_page(self.basis.page(&question, self.candidates.clone()))
    }

    fn other_answers(&self, cursor: &str) -> Vec<Option<Page<Suggestion>>> {
        let question = self.question(&self.input, Some(cursor));
        // Flipping the lowest bit always names another revision.
        let other_revision = CompletionPageBasis::new(&question, self.basis.revision ^ 1);
        let longer_input = format!("{} ", self.input);
        let other_question = self.question(&longer_input, Some(cursor));
        let other_query = CompletionPageBasis::new(&other_question, self.basis.revision);
        let mut more_candidates = self.candidates.clone();
        more_candidates.push(Suggestion {
            value: "further".to_string(),
            kind: SuggestionKind::Text,
            edit: TextEdit {
                start: 0,
                end: 0,
                replacement: "further".to_string(),
            },
        });
        vec![
            completion_page(other_revision.page(&question, self.candidates.clone())),
            completion_page(other_query.page(&other_question, self.candidates.clone())),
            completion_page(self.basis.page(&question, more_candidates)),
        ]
    }
}

/// A choice basis over a generated question, content digest, revision and choices.
struct ChoiceCase {
    target: ChoiceTarget,
    dependencies: Vec<ChoiceSelection>,
    search: String,
    content_digest: Option<String>,
    page_size: u16,
    candidates: Vec<Choice>,
    basis: ChoicePageBasis,
    candidate_digest: String,
}

impl ChoiceCase {
    fn generate(arbitrary: &mut Arbitrary<'_>) -> Self {
        let revision = arbitrary.entropy().any_u64();
        let target = arbitrary.entropy().pick(TARGETS);
        let count = arbitrary.entropy().count(MAX_DEPENDENCIES);
        let mut dependencies = Vec::with_capacity(count);
        for _ in 0..count {
            dependencies.push(ChoiceSelection {
                value: choice_value(arbitrary),
            });
        }
        let search = arbitrary.string();
        let content_digest = optional_text(arbitrary);
        let page_size = page_size(arbitrary);
        let count = arbitrary.entropy().count(MAX_CANDIDATES);
        let mut candidates = Vec::with_capacity(count);
        for _ in 0..count {
            candidates.push(choice(arbitrary));
        }
        let question = ChoiceLookupRequest::new(target, dependencies.clone(), search.clone());
        let basis = Self::basis(&question, revision, content_digest.as_deref());
        let candidate_digest = basis.candidate_digest(&candidates);
        Self {
            target,
            dependencies,
            search,
            content_digest,
            page_size,
            candidates,
            basis,
            candidate_digest,
        }
    }

    /// The basis of `question` at `revision`, bound to `content_digest` when there is one.
    fn basis(
        question: &ChoiceLookupRequest,
        revision: u64,
        content_digest: Option<&str>,
    ) -> ChoicePageBasis {
        let basis = ChoicePageBasis::new(question, revision);
        match content_digest {
            Some(digest) => basis.with_content_digest(digest.to_string()),
            None => basis,
        }
    }

    /// The question searching `search`, with this case's target, dependencies and page size,
    /// returning `cursor`.
    fn question(&self, search: &str, cursor: Option<&str>) -> ChoiceLookupRequest {
        ChoiceLookupRequest::new(self.target, self.dependencies.clone(), search.to_string())
            .with_page(self.page_size, cursor.map(str::to_string))
            .assured("generated page sizes are from 1 through 100")
    }
}

impl PagedCase for ChoiceCase {
    type Candidate = Choice;

    fn revision(&self) -> u64 {
        self.basis.revision
    }

    fn page_size(&self) -> usize {
        usize::from(self.page_size)
    }

    fn candidates(&self) -> &[Choice] {
        &self.candidates
    }

    fn issued(&self, end: usize) -> PageContinuation<'_> {
        self.basis.continuation(end, &self.candidate_digest)
    }

    fn answer(&self, cursor: Option<&str>) -> Option<Page<Choice>> {
        let question = self.question(&self.search, cursor);
        choice_page(self.basis.page(&question, self.candidates.clone()))
    }

    fn other_answers(&self, cursor: &str) -> Vec<Option<Page<Choice>>> {
        let question = self.question(&self.search, Some(cursor));
        let revision = self.basis.revision;
        let content = self.content_digest.as_deref();
        // Flipping the lowest bit always names another revision.
        let other_revision = Self::basis(&question, revision ^ 1, content);
        let longer_search = format!("{} ", self.search);
        let other_question = self.question(&longer_search, Some(cursor));
        let other_query = Self::basis(&other_question, revision, content);
        let other_content = match content {
            Some(digest) => format!("{digest} "),
            None => String::new(),
        };
        let other_content = Self::basis(&question, revision, Some(&other_content));
        let mut more_candidates = self.candidates.clone();
        more_candidates.push(Choice {
            value: ChoiceValue::DomainPace(DomainPaceChoice::Paced),
            presentation: ChoicePresentation {
                label: "further".to_string(),
                detail: None,
                group: None,
            },
        });
        vec![
            choice_page(other_revision.page(&question, self.candidates.clone())),
            choice_page(other_query.page(&other_question, self.candidates.clone())),
            choice_page(other_content.page(&question, self.candidates.clone())),
            choice_page(self.basis.page(&question, more_candidates)),
        ]
    }
}

/// What a malformed-cursor case does to the continuation its basis issues.
#[derive(Debug, Clone, Copy)]
enum Damage {
    /// Arbitrary text instead of a continuation.
    Arbitrary,
    /// The issued continuation, whole.
    Issued,
    /// A strict prefix of it.
    Truncated,
    /// It with a suffix.
    Extended,
    /// It with one character replaced or deleted.
    Replaced,
    /// It with a piece inserted.
    Inserted,
    /// The same continuation of another revision.
    OtherRevision,
    /// The same continuation of another question or candidate set.
    OtherDigest,
    /// It with a number spelled with a sign or a leading zero.
    Respelled,
}

const DAMAGES: [Damage; 9] = [
    Damage::Arbitrary,
    Damage::Issued,
    Damage::Truncated,
    Damage::Extended,
    Damage::Replaced,
    Damage::Inserted,
    Damage::OtherRevision,
    Damage::OtherDigest,
    Damage::Respelled,
];

/// What a cursor text must do to the page of the basis it was made for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expected {
    /// Continue the page from this position.
    Continues(usize),
    /// Answer `StaleContext`.
    Refused,
    /// Either, while a page it continues is still that basis's page.
    Either,
}

/// Cursor text a client returns, and what it must do.
#[derive(Debug)]
struct CursorText {
    text: String,
    expected: Expected,
}

/// How to make one cursor text, drawn before its case so the case's candidates cannot use up the
/// entropy it needs.
struct CursorRecipe {
    damage: Damage,
    /// The text of `Damage::Arbitrary`.
    arbitrary_text: String,
    /// Selects the issued position, from the first candidate to two past the last.
    position: u64,
    /// Selects where in the issued text a damage lands.
    index: u64,
    piece: &'static str,
    suffix: &'static str,
    /// Selects the digest `Damage::OtherDigest` changes or the number `Damage::Respelled`
    /// respells.
    variant: u8,
}

impl CursorRecipe {
    fn generate(arbitrary: &mut Arbitrary<'_>) -> Self {
        let damage = arbitrary.entropy().pick(DAMAGES);
        let arbitrary_text = arbitrary.string();
        let position = arbitrary.entropy().any_u64();
        let index = arbitrary.entropy().any_u64();
        let piece = arbitrary.entropy().pick(DAMAGE);
        let suffix = arbitrary.entropy().pick(SUFFIXES);
        let variant = arbitrary.entropy().byte();
        Self {
            damage,
            arbitrary_text,
            position,
            index,
            piece,
            suffix,
            variant,
        }
    }

    /// The cursor text this recipe makes for `case`, and what it must do there.
    fn cursor<C: PagedCase>(&self, case: &C) -> CursorText {
        let candidates = case.candidates().len();
        let positions = candidates
            .checked_add(3)
            .assured("bounded candidates leave room for three positions past them");
        let position = Self::select(self.position, positions);
        let issued = case.issued(position);
        let text = issued.encode();
        let issued_expectation = if position <= candidates {
            Expected::Continues(position)
        } else {
            Expected::Refused
        };
        match self.damage {
            Damage::Arbitrary => CursorText {
                text: self.arbitrary_text.clone(),
                expected: Expected::Either,
            },
            Damage::Issued => CursorText {
                text,
                expected: issued_expectation,
            },
            Damage::Truncated => {
                // The text is ASCII, so every index is a character boundary. A strict prefix leaves
                // a part out or the candidate digest short. Half the prefixes end beside a
                // separator, before or after it, so every count of parts is reached.
                let length = if self.variant.is_multiple_of(2) {
                    let separators: Vec<usize> =
                        text.match_indices(':').map(|(index, _)| index).collect();
                    let separator = separators[Self::select(self.index, separators.len())];
                    if (self.variant & 2) == 0 {
                        separator
                    } else {
                        separator
                            .checked_add(1)
                            .assured("a separator lies inside a short continuation text")
                    }
                } else {
                    Self::select(self.index, text.len())
                };
                CursorText {
                    text: text[..length].to_string(),
                    expected: Expected::Refused,
                }
            }
            Damage::Extended => CursorText {
                text: format!("{text}{}", self.suffix),
                expected: Expected::Refused,
            },
            Damage::Replaced => {
                let index = Self::select(self.index, text.len());
                let mut damaged = text;
                damaged.replace_range(index..=index, self.piece);
                CursorText {
                    text: damaged,
                    expected: Expected::Either,
                }
            }
            Damage::Inserted => {
                let positions = text
                    .len()
                    .checked_add(1)
                    .assured("a continuation's text is far shorter than usize::MAX");
                let index = Self::select(self.index, positions);
                let mut damaged = text;
                damaged.insert_str(index, self.piece);
                CursorText {
                    text: damaged,
                    expected: Expected::Either,
                }
            }
            Damage::OtherRevision => {
                // Flipping the lowest bit always names another revision.
                let other = PageContinuation {
                    revision: issued.revision ^ 1,
                    ..issued
                };
                CursorText {
                    text: other.encode(),
                    expected: Expected::Refused,
                }
            }
            Damage::OtherDigest => {
                let query_digest = other_digest(issued.query_digest);
                let candidate_digest = other_digest(issued.candidate_digest);
                let other = if self.variant.is_multiple_of(2) {
                    PageContinuation {
                        query_digest: &query_digest,
                        ..issued
                    }
                } else {
                    PageContinuation {
                        candidate_digest: &candidate_digest,
                        ..issued
                    }
                };
                CursorText {
                    text: other.encode(),
                    expected: Expected::Refused,
                }
            }
            Damage::Respelled => {
                let PageContinuation {
                    revision,
                    end,
                    query_digest,
                    candidate_digest,
                } = issued;
                let text = match self.variant % 4 {
                    0 => format!("+{revision}:{end}:{query_digest}:{candidate_digest}"),
                    1 => format!("0{revision}:{end}:{query_digest}:{candidate_digest}"),
                    2 => format!("{revision}:+{end}:{query_digest}:{candidate_digest}"),
                    _ => format!("{revision}:0{end}:{query_digest}:{candidate_digest}"),
                };
                CursorText {
                    text,
                    expected: Expected::Either,
                }
            }
        }
    }

    /// One of `count` choices, selected by `seed`.
    fn select(seed: u64, count: usize) -> usize {
        let count = u64::try_from(count).assured("supported targets address at most 64 bits");
        let selected = seed.checked_rem(count).assured(
            "callers select among the positions up to past the candidates or the characters of a \
             continuation's text, never among none",
        );
        usize::try_from(selected).verified("the selection is below a usize count")
    }
}

/// Another digest of the same shape, its first hexadecimal digit changed.
fn other_digest(digest: &str) -> String {
    let first = if digest.starts_with('0') { "1" } else { "0" };
    let mut other = digest.to_string();
    other.replace_range(..1, first);
    other
}

/// A page size the wire admits, from 1 through 100, small often enough to page through a few
/// candidates.
fn page_size(arbitrary: &mut Arbitrary<'_>) -> u16 {
    let size = if arbitrary.entropy().flag() {
        arbitrary.entropy().boundary_biased(1..=100)
    } else {
        arbitrary.entropy().between(1..=4)
    };
    u16::try_from(size).verified("both ranges end at or below 100")
}

/// Any position a continuation can name, at its extremes as often as elsewhere.
fn any_position(arbitrary: &mut Arbitrary<'_>) -> usize {
    let largest = u64::try_from(usize::MAX).assured("supported targets address at most 64 bits");
    let position = arbitrary.entropy().boundary_biased(0..=largest);
    usize::try_from(position).verified("the range ends at usize::MAX")
}

/// A suggestion with any value, kind and edit.
fn suggestion(arbitrary: &mut Arbitrary<'_>) -> Suggestion {
    let value = arbitrary.string();
    let kind = arbitrary
        .entropy()
        .pick([SuggestionKind::Text, SuggestionKind::LocalDirectoryLookup]);
    let start = edit_offset(arbitrary);
    let end = edit_offset(arbitrary);
    let replacement = arbitrary.string();
    Suggestion {
        value,
        kind,
        edit: TextEdit {
            start,
            end,
            replacement,
        },
    }
}

/// A byte offset as the wire carries one, at its extremes as often as elsewhere.
fn edit_offset(arbitrary: &mut Arbitrary<'_>) -> u32 {
    let offset = arbitrary.entropy().boundary_biased(0..=u64::from(u32::MAX));
    u32::try_from(offset).verified("the range ends at u32::MAX")
}

/// A choice with any value and presentation.
fn choice(arbitrary: &mut Arbitrary<'_>) -> Choice {
    let value = choice_value(arbitrary);
    let label = arbitrary.string();
    let detail = optional_text(arbitrary);
    let group = optional_text(arbitrary);
    Choice {
        value,
        presentation: ChoicePresentation {
            label,
            detail,
            group,
        },
    }
}

/// A choice value of any variant.
fn choice_value(arbitrary: &mut Arbitrary<'_>) -> ChoiceValue {
    match arbitrary.entropy().byte() % 7 {
        0 => ChoiceValue::DomainPace(
            arbitrary
                .entropy()
                .pick([DomainPaceChoice::Unpaced, DomainPaceChoice::Paced]),
        ),
        1 => ChoiceValue::PlacementPolicy(arbitrary.entropy().pick([
            PlacementPolicy::RequireColocation,
            PlacementPolicy::PreferColocation,
            PlacementPolicy::Neutral,
            PlacementPolicy::SuggestSeparation,
        ])),
        2 => ChoiceValue::Domain(arbitrary.name()),
        3 => ChoiceValue::Resource(arbitrary.name()),
        4 => ChoiceValue::ResourceVersion(arbitrary.requested_version()),
        5 => {
            let kind = arbitrary.entropy().pick(MODEL_KINDS);
            ChoiceValue::Model(NodeRef::new(kind, arbitrary.name::<ModelName>()))
        }
        _ => ChoiceValue::Field(arbitrary.name()),
    }
}

/// Any text, or none.
fn optional_text(arbitrary: &mut Arbitrary<'_>) -> Option<String> {
    if arbitrary.entropy().flag() {
        Some(arbitrary.string())
    } else {
        None
    }
}
