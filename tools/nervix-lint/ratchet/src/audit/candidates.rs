//! Repository tooling, outside the product layer order.
//! Owns: merged current candidates, presentation derived from their spans and stable identities.
//! Depends on: typed compiler evidence and the audit's maintained JSON encoding.
//! Must not know: Git history or attribution policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use aws_lc_rs::digest::{SHA256, digest};
use error_stack::ResultExt as _;
use indexmap::IndexMap;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_lint_report::{
    Acquisition, Context, Expansion, SourceSpan,
    audit::{AuditCandidate, AuditPattern},
};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct as _};
use serde_json::{Value, json};

use super::encoding::Encoding;
use crate::{
    error::{Error, Result},
    evidence::Completion,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub(super) enum Pattern {
    Resolved(AuditPattern),
    Relocated(Relocated),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Relocated {
    RelocatedAcquisition {
        receiver: String,
        operation: String,
        acquisition: Acquisition,
    },
}

impl Pattern {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Resolved(AuditPattern::RepeatedLookup { .. }) => "repeated_lookup",
            Self::Resolved(AuditPattern::KeyAllocation { .. }) => "key_allocation",
            Self::Resolved(AuditPattern::BorrowedCopy { .. }) => "borrowed_copy",
            Self::Resolved(AuditPattern::DuplicatedBranch { .. }) => "duplicated_branch",
            Self::Resolved(AuditPattern::ForwardingWrapper { .. }) => "forwarding_wrapper",
            Self::Resolved(AuditPattern::DiscardedError { .. }) => "discarded_error",
            Self::Resolved(AuditPattern::DisguisedClamp { .. }) => "disguised_clamp",
            Self::Relocated(_) => "relocated_acquisition",
        }
    }

    pub fn semantic_fields(&self) -> Result<Value> {
        let mut value = serde_json::to_value(self).change_context(Error::Json {
            path: "audit pattern".into(),
        })?;
        let fields = value
            .as_object_mut()
            .assured("both current pattern variants serialize as objects");
        match self {
            Self::Resolved(AuditPattern::RepeatedLookup { operations, .. }) => {
                let semantic: Vec<_> = operations
                    .iter()
                    .map(|operation| SemanticOperation {
                        operation: &operation.operation,
                        source: &operation.span.source,
                    })
                    .collect();
                fields.insert(
                    "operations".into(),
                    serde_json::to_value(semantic).change_context(Error::Json {
                        path: "audit operations".into(),
                    })?,
                );
            }
            Self::Resolved(AuditPattern::DuplicatedBranch { occurrences, .. }) => {
                let sources: Vec<_> = occurrences.iter().map(|span| &span.source).collect();
                fields.insert(
                    "occurrences".into(),
                    serde_json::to_value(sources).change_context(Error::Json {
                        path: "audit occurrences".into(),
                    })?,
                );
            }
            _ => {}
        }
        Ok(value)
    }

    pub fn spans<'a>(&'a self, primary: &'a SourceSpan) -> Vec<&'a SourceSpan> {
        let mut spans = vec![primary];
        match self {
            Self::Resolved(AuditPattern::RepeatedLookup { operations, .. }) => {
                spans.extend(operations.iter().map(|operation| &operation.span))
            }
            Self::Resolved(AuditPattern::DuplicatedBranch { occurrences, .. }) => {
                spans.extend(occurrences)
            }
            _ => {}
        }
        spans
    }
}

#[derive(Serialize)]
struct SemanticOperation<'a> {
    operation: &'a str,
    source: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CandidateFacts {
    pub owner: String,
    pub contexts: Vec<Option<Context>>,
    pub configurations: Vec<String>,
    pub pattern: Pattern,
    pub span: SourceSpan,
    pub expansion: Vec<Expansion>,
}

impl CandidateFacts {
    pub fn path(&self) -> &str {
        &self.span.site.path
    }
    pub fn kind(&self) -> &'static str {
        self.pattern.kind()
    }
    pub fn column(&self) -> u64 {
        u64::from(self.span.column)
            .checked_add(1)
            .assured("a u32 column plus one fits u64")
    }

    pub fn lines(&self) -> BTreeSet<u32> {
        let mut lines = BTreeSet::new();
        for span in self.pattern.spans(&self.span) {
            lines.extend(span.line..=span.end_line);
        }
        lines
    }

    pub fn fragments(&self) -> Vec<String> {
        let mut found = BTreeSet::new();
        let mut fragments = Vec::new();
        for span in self.pattern.spans(&self.span) {
            let first = span
                .source
                .split([
                    '\n', '\r', '\u{b}', '\u{c}', '\u{1c}', '\u{1d}', '\u{1e}', '\u{85}',
                    '\u{2028}', '\u{2029}',
                ])
                .next()
                .unwrap_or("");
            let text = normalize(first);
            if text.chars().count() >= 8 && found.insert(text.clone()) {
                fragments.push(text);
            }
        }
        fragments
    }

    fn order(&self) -> CandidateOrder<'_> {
        CandidateOrder {
            kind: self.kind(),
            path: self.path(),
            line: self.span.line,
            column: self.column(),
            owner: &self.owner,
        }
    }

    fn identity(&self, encoding: &Encoding) -> Result<String> {
        let payload = json!([
            self.kind(),
            self.path(),
            self.owner,
            self.span.source,
            self.pattern.semantic_fields()?
        ]);
        let encoded = encoding.identity(&payload)?;
        // This is a non-security fingerprint of the current report's semantic identity preimage.
        let hashed = digest(&SHA256, encoded.as_bytes());
        let mut identity = String::with_capacity(12);
        for byte in &hashed.as_ref()[..6] {
            write!(identity, "{byte:02x}").assured("writing to a String cannot fail");
        }
        Ok(identity)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Candidate {
    pub id: String,
    pub facts: CandidateFacts,
}

impl std::ops::Deref for Candidate {
    type Target = CandidateFacts;
    fn deref(&self) -> &Self::Target {
        &self.facts
    }
}

impl Candidate {
    pub fn identify(mut facts: Vec<CandidateFacts>, encoding: &Encoding) -> Result<Vec<Self>> {
        facts.sort_by(|left, right| left.order().cmp(&right.order()));
        let mut seen = BTreeMap::<String, usize>::new();
        let mut records = Vec::with_capacity(facts.len());
        for facts in facts {
            let digest = facts.identity(encoding)?;
            let ordinal = seen.entry(digest.clone()).or_default();
            let id = if *ordinal == 0 {
                digest
            } else {
                format!("{digest}-{ordinal}")
            };
            *ordinal = ordinal
                .checked_add(1)
                .assured("each occurrence occupies one entry of the input slice");
            records.push(Self { id, facts });
        }
        Ok(records)
    }
}

impl Serialize for Candidate {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Candidate", 12)?;
        state.serialize_field("kind", self.kind())?;
        state.serialize_field("path", self.path())?;
        state.serialize_field("line", &self.span.line)?;
        state.serialize_field("column", &self.column())?;
        state.serialize_field("owner", &self.owner)?;
        state.serialize_field("source", &self.span.source)?;
        state.serialize_field("contexts", &self.contexts)?;
        state.serialize_field("configurations", &self.configurations)?;
        state.serialize_field("pattern", &self.pattern)?;
        state.serialize_field("span", &self.span)?;
        state.serialize_field("expansion", &self.expansion)?;
        state.serialize_field("id", &self.id)?;
        state.end()
    }
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct CandidateOrder<'a> {
    kind: &'a str,
    path: &'a str,
    line: u32,
    column: u64,
    owner: &'a str,
}

struct Merged {
    candidate: AuditCandidate,
    configurations: BTreeSet<String>,
    contexts: BTreeMap<String, Option<Context>>,
}

pub(super) struct Collector {
    encoding: Encoding,
}

impl Collector {
    pub fn new() -> Self {
        Self {
            encoding: Encoding::new(),
        }
    }

    pub fn collect(&self, evidence: Vec<Completion>) -> Result<Vec<Candidate>> {
        let mut merged = IndexMap::<String, Merged>::new();
        // The first report at one site owns its source/spans. Contexts and configuration labels
        // merge independently; map traversal keeps the current declared report order.
        for entry in evidence {
            let configuration = entry.configuration.name;
            for report in entry.reports.into_values() {
                for candidate in report.audit {
                    let semantic =
                        Pattern::Resolved(candidate.pattern.clone()).semantic_fields()?;
                    let key = self.encoding.identity(&json!([
                        Pattern::Resolved(candidate.pattern.clone()).kind(),
                        candidate.span.site.path,
                        candidate.span.site.start,
                        candidate.owner,
                        semantic
                    ]))?;
                    let context_key = self.encoding.identity(&candidate.context)?;
                    let context = candidate.context.clone();
                    let record = match merged.entry(key.clone()) {
                        indexmap::map::Entry::Vacant(vacant) => vacant.insert(Merged {
                            candidate,
                            configurations: BTreeSet::new(),
                            contexts: BTreeMap::new(),
                        }),
                        indexmap::map::Entry::Occupied(occupied) => occupied.into_mut(),
                    };
                    record
                        .configurations
                        .insert(format!("{configuration}:{}", report.crate_name));
                    record.contexts.insert(context_key, context);
                }
            }
        }
        let mut records = Vec::with_capacity(merged.len());
        for record in merged.into_values() {
            records.push(CandidateFacts {
                owner: record.candidate.owner,
                contexts: record.contexts.into_values().collect(),
                configurations: record.configurations.into_iter().collect(),
                pattern: Pattern::Resolved(record.candidate.pattern),
                span: record.candidate.span,
                expansion: record.candidate.expansion,
            });
        }
        Candidate::identify(records, &self.encoding)
    }
}

pub(super) fn normalize(text: &str) -> String {
    let whitespace =
        |character: char| character.is_whitespace() || matches!(character, '\u{1c}'..='\u{1f}');
    let mut words = Vec::new();
    for word in text.split(whitespace) {
        if !word.is_empty() {
            words.push(word);
        }
    }
    words.join(" ")
}
