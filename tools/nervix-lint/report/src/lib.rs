//! Repository tooling, outside the product layer order.
//! Owns: generated compiler evidence, source contracts and finite resolved API recognition.
//! Depends on: serialization and typed error reporting; no compiler internals.
//! Must not know: runtime graph execution or manually maintained facts about product source.

use std::collections::BTreeMap;

use indexmap::{IndexMap, map::Entry};
use serde::{Deserialize, Serialize};

pub mod rules;

#[derive(Debug, thiserror::Error)]
pub enum ReportError {
    #[error("invalid source contract: {problem}")]
    InvalidContract { problem: ContractProblem },
    #[error("conflicting findings at {path}:{start}")]
    ConflictingFinding { path: String, start: u32 },
    #[error("compiler pass did not complete for {configuration}:{crate_name}")]
    IncompleteReport {
        configuration: String,
        crate_name: String,
    },
}

/// Source contract failures retain their classification until the diagnostic boundary.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum ContractProblem {
    #[error("context requires a context kind and reason")]
    MissingArguments,
    #[error("contract arguments must be named")]
    UnnamedArgument,
    #[error("contract argument names must be single identifiers")]
    QualifiedArgument,
    #[error("a context has exactly one kind")]
    MultipleKinds,
    #[error("contract values must be strings: {argument}")]
    NonStringValue { argument: String },
    #[error("duplicate contract argument: {argument}")]
    DuplicateArgument { argument: String },
    #[error("context requires its kind")]
    MissingKind,
    #[error("context requires a reason")]
    MissingReason,
    #[error("unknown context argument: {argument}")]
    UnknownArgument { argument: String },
    #[error("every contract requires a meaningful reason")]
    EmptyReason,
    #[error("bounded protocols require a key")]
    MissingKey,
    #[error("bounded protocols require a capacity or deadline")]
    MissingBound,
    #[error("bounded protocol keys and bounds cannot be empty")]
    EmptyCoordinate,
    #[error("only bounded protocols accept key and bound")]
    UnexpectedCoordinate,
    #[error("expected recurring, lifecycle, observer, outside or bounded; found {kind}")]
    UnknownKind { kind: String },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Acquisition {
    Shared,
    Exclusive,
    TryShared,
    TryExclusive,
}

/// A source-owned execution contract, read from compiler metadata. Narrower contracts override
/// inherited defaults. Recurring callers are checked against lifecycle-only entry points.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "context", rename_all = "snake_case", deny_unknown_fields)]
pub enum Context {
    Recurring {
        reason: String,
    },
    Lifecycle {
        reason: String,
    },
    Observer {
        reason: String,
    },
    Outside {
        reason: String,
    },
    Bounded {
        reason: String,
        key: String,
        bound: String,
    },
}

impl Context {
    pub fn from_parts(
        kind: &str,
        reason: String,
        key: Option<String>,
        bound: Option<String>,
    ) -> Result<Self, error_stack::Report<ReportError>> {
        let invalid = |problem| error_stack::Report::new(ReportError::InvalidContract { problem });
        if reason.trim().is_empty() {
            return Err(invalid(ContractProblem::EmptyReason));
        }
        if kind == "bounded" {
            let Some(key) = key else {
                return Err(invalid(ContractProblem::MissingKey));
            };
            let Some(bound) = bound else {
                return Err(invalid(ContractProblem::MissingBound));
            };
            if key.trim().is_empty() || bound.trim().is_empty() {
                return Err(invalid(ContractProblem::EmptyCoordinate));
            }
            return Ok(Self::Bounded { reason, key, bound });
        }
        if key.is_some() || bound.is_some() {
            return Err(invalid(ContractProblem::UnexpectedCoordinate));
        }
        match kind {
            "recurring" => Ok(Self::Recurring { reason }),
            "lifecycle" => Ok(Self::Lifecycle { reason }),
            "observer" => Ok(Self::Observer { reason }),
            "outside" => Ok(Self::Outside { reason }),
            _ => Err(invalid(ContractProblem::UnknownKind { kind: kind.into() })),
        }
    }

    pub fn is_recurring(&self) -> bool {
        matches!(self, Self::Recurring { .. } | Self::Bounded { .. })
    }

    pub fn permits_acquisition(&self) -> bool {
        !matches!(self, Self::Recurring { .. })
    }

    pub fn is_lifecycle(&self) -> bool {
        matches!(self, Self::Lifecycle { .. })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SiteId {
    pub path: String,
    pub start: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSpan {
    pub site: SiteId,
    pub end: u32,
    pub line: u32,
    pub column: u32,
    pub end_line: u32,
    pub end_column: u32,
    pub source: String,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Expansion {
    pub macro_name: String,
    pub call_site: String,
    pub definition: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Finding {
    pub rule: String,
    pub receiver: String,
    pub receiver_type: String,
    pub receiver_expression: String,
    pub operation: String,
    pub definition: String,
    pub owner: String,
    pub acquisition: Acquisition,
    #[serde(deserialize_with = "Option::deserialize")]
    pub context: Option<Context>,
    pub span: SourceSpan,
    pub expansion: Vec<Expansion>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerReport {
    pub compiler: String,
    pub identity: String,
    pub configuration: String,
    pub crate_name: String,
    pub crate_source: String,
    pub arguments: Vec<String>,
    pub findings: Vec<Finding>,
    pub excluded_generated: u32,
    pub excluded_external: u32,
    pub complete: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoredSite {
    pub site: SiteId,
    pub configurations: BTreeMap<String, Vec<Finding>>,
}

#[derive(Eq, Hash, PartialEq)]
struct InstanceId {
    owner: String,
    definition: String,
    expansion: Vec<Expansion>,
}

struct SiteInstances {
    site: SiteId,
    configurations: BTreeMap<String, IndexMap<InstanceId, Finding>>,
}

impl SiteInstances {
    fn finish(self) -> AuthoredSite {
        let configurations = self
            .configurations
            .into_iter()
            .map(|(configuration, instances)| (configuration, instances.into_values().collect()))
            .collect();
        AuthoredSite {
            site: self.site,
            configurations,
        }
    }
}

pub fn aggregate(
    reports: &[CompilerReport],
) -> Result<Vec<AuthoredSite>, error_stack::Report<ReportError>> {
    let mut sites: BTreeMap<SiteId, SiteInstances> = BTreeMap::new();
    for report in reports {
        if !report.complete {
            return Err(error_stack::Report::new(ReportError::IncompleteReport {
                configuration: report.configuration.clone(),
                crate_name: report.crate_name.clone(),
            }));
        }
        for finding in &report.findings {
            let key = finding.span.site.clone();
            let configuration = format!("{}:{}", report.configuration, report.crate_name);
            let site = sites.entry(key.clone()).or_insert_with(|| SiteInstances {
                site: key.clone(),
                configurations: BTreeMap::new(),
            });
            let instances = site.configurations.entry(configuration).or_default();
            let identity = InstanceId {
                owner: finding.owner.clone(),
                definition: finding.definition.clone(),
                expansion: finding.expansion.clone(),
            };
            match instances.entry(identity) {
                Entry::Occupied(previous) => {
                    if previous.get() != finding {
                        return Err(error_stack::Report::new(ReportError::ConflictingFinding {
                            path: key.path.clone(),
                            start: key.start,
                        }));
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(finding.clone());
                }
            }
        }
    }
    Ok(sites.into_values().map(SiteInstances::finish).collect())
}

#[cfg(test)]
mod tests;
