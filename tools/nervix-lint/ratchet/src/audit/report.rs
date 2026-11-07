//! Repository tooling, outside the product layer order.
//! Owns: complete audit reports and their typed attribution and summary vocabulary.
//! Depends on: current candidate facts and compiler span/context types at the JSON boundary.
//! Must not know: Git process execution or enforcement decisions.

use std::collections::BTreeMap;

use error_stack::Report as ErrorReport;
use meticulous::OptionExt as _;
use nervix_lint_report::{Context, Expansion, SourceSpan};
use serde::{Deserialize, Serialize};

use super::{
    candidates::{Candidate, CandidateFacts, Pattern},
    history::Commit,
};
use crate::error::{AuditRecord, Error};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum Level {
    Suspected,
    Possible,
    Unattributed,
    PreExisting,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Rewrite {
    pub sha: String,
    pub removed: Vec<String>,
    pub added: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RuleEvidence {
    pub rule: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub event: Option<String>,
    pub spelling_removed: bool,
    pub file_spelling_removed: bool,
    pub commit_spelling_removed: bool,
    pub written_while_counted: bool,
    pub predates_rule: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull_request_rewrite: Option<Rewrite>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Origin {
    pub sha: String,
    pub date: String,
    pub subject: String,
    pub rules: Vec<RuleEvidence>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Committed {
    pub commit: Commit,
    pub commits: Vec<String>,
    pub before: Vec<String>,
    pub after: Vec<String>,
    pub rules: Vec<RuleEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Uncommitted {
    pub commit: (),
    pub rules: [RuleEvidence; 0],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "suggestion", rename_all = "kebab-case")]
pub(super) enum Attribution {
    Suspected(Committed),
    Possible(Committed),
    Unattributed(Committed),
    PreExisting(Committed),
    Uncommitted(Uncommitted),
}

impl Attribution {
    pub fn uncommitted() -> Self {
        Self::Uncommitted(Uncommitted {
            commit: (),
            rules: [],
        })
    }
    pub fn committed(level: Level, data: Committed) -> Self {
        match level {
            Level::Suspected => Self::Suspected(data),
            Level::Possible => Self::Possible(data),
            Level::Unattributed => Self::Unattributed(data),
            Level::PreExisting => Self::PreExisting(data),
        }
    }
    pub fn name(&self) -> &'static str {
        match self {
            Self::Suspected(_) => "suspected",
            Self::Possible(_) => "possible",
            Self::Unattributed(_) => "unattributed",
            Self::PreExisting(_) => "pre-existing",
            Self::Uncommitted(_) => "uncommitted",
        }
    }
    pub fn data(&self) -> Option<&Committed> {
        match self {
            Self::Suspected(data)
            | Self::Possible(data)
            | Self::Unattributed(data)
            | Self::PreExisting(data) => Some(data),
            Self::Uncommitted(_) => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "CandidateProjection")]
pub(super) struct AttributedCandidate {
    #[serde(flatten)]
    pub candidate: Candidate,
    pub attribution: Attribution,
}

/// The established external report repeats location/source/kind for presentation. Internally
/// they are always derived from one span/pattern; only this boundary receives those projections.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CandidateProjection {
    id: String,
    kind: String,
    path: String,
    line: u32,
    column: u64,
    owner: String,
    source: String,
    contexts: Vec<Option<Context>>,
    configurations: Vec<String>,
    pattern: Pattern,
    span: SourceSpan,
    expansion: Vec<Expansion>,
    attribution: Attribution,
}

impl TryFrom<CandidateProjection> for AttributedCandidate {
    type Error = ErrorReport<Error>;
    fn try_from(projection: CandidateProjection) -> std::result::Result<Self, Self::Error> {
        let facts = CandidateFacts {
            owner: projection.owner,
            contexts: projection.contexts,
            configurations: projection.configurations,
            pattern: projection.pattern,
            span: projection.span,
            expansion: projection.expansion,
        };
        if projection.id.is_empty()
            || projection.kind != facts.kind()
            || projection.path != facts.path()
            || projection.line != facts.span.line
            || projection.column != facts.column()
            || projection.source != facts.span.source
        {
            return Err(ErrorReport::new(Error::AuditRecord {
                record: AuditRecord::Candidate,
            }));
        }
        Ok(Self {
            candidate: Candidate {
                id: projection.id,
                facts,
            },
            attribution: projection.attribution,
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Counts {
    pub total: usize,
    pub recurring: usize,
    pub suspected: usize,
    pub possible: usize,
    pub unattributed: usize,
    #[serde(rename = "pre-existing")]
    pub pre_existing: usize,
    pub uncommitted: usize,
}

impl Counts {
    fn add(&mut self, record: &AttributedCandidate) {
        self.total = self
            .total
            .checked_add(1)
            .assured("each observed candidate is in the input slice");
        if record
            .candidate
            .contexts
            .iter()
            .flatten()
            .any(Context::is_recurring)
        {
            self.recurring = self
                .recurring
                .checked_add(1)
                .assured("recurring candidates are a subset of the input slice");
        }
        let count = match record.attribution {
            Attribution::Suspected(_) => &mut self.suspected,
            Attribution::Possible(_) => &mut self.possible,
            Attribution::Unattributed(_) => &mut self.unattributed,
            Attribution::PreExisting(_) => &mut self.pre_existing,
            Attribution::Uncommitted(_) => &mut self.uncommitted,
        };
        *count = count
            .checked_add(1)
            .assured("each suggestion is counted at most once per input candidate");
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(super) struct Summary(pub BTreeMap<String, Counts>);

impl Summary {
    pub fn of(candidates: &[AttributedCandidate]) -> Self {
        let mut summary = Self::default();
        for candidate in candidates {
            summary
                .0
                .entry(candidate.candidate.kind().into())
                .or_default()
                .add(candidate);
        }
        summary
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Introduction {
    pub introduced: String,
    pub date: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Report {
    pub revision: String,
    pub source_identity: String,
    pub matrix_complete: bool,
    pub configurations: Vec<String>,
    pub pull_requests: bool,
    pub rules: BTreeMap<String, Introduction>,
    pub summary: Summary,
    pub candidates: Vec<AttributedCandidate>,
    #[serde(
        serialize_with = "serialize_complete",
        deserialize_with = "deserialize_complete"
    )]
    pub complete: (),
}

fn serialize_complete<S: serde::Serializer>(
    _: &(),
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_bool(true)
}
fn deserialize_complete<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<(), D::Error> {
    if bool::deserialize(deserializer)? {
        Ok(())
    } else {
        Err(serde::de::Error::custom(Error::AuditIncomplete))
    }
}
