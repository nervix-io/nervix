//! Repository tooling, outside the product layer order.
//! Owns: compiler acquisition reports, API classification and reviewed site scopes.
//! Depends on: serialization and typed error reporting; no compiler internals.
//! Must not know: graph execution, model-checker scheduling or runtime policy implementation.

use std::collections::{BTreeMap, BTreeSet};

use indexmap::{IndexMap, map::Entry};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ReportError {
    #[error("invalid analysis JSON")]
    Json,
    #[error("catalog repeats receiver {receiver} operation {operation}")]
    DuplicateApi { receiver: String, operation: String },
    #[error("scope repeats authored site {path}:{start}")]
    DuplicateScope { path: String, start: u32 },
    #[error("scope lacks its owner, frequency or rationale at {path}:{start}")]
    IncompleteScope { path: String, start: u32 },
    #[error("bounded protocol scope lacks its key or bound at {path}:{start}")]
    UnboundedProtocol { path: String, start: u32 },
    #[error("conflicting findings at {path}:{start}")]
    ConflictingFinding { path: String, start: u32 },
    #[error("compiler pass did not complete for {configuration}:{crate_name}")]
    IncompleteReport {
        configuration: String,
        crate_name: String,
    },
    #[error("missing reviewed acquisition scope at {path}:{start}")]
    MissingScope { path: String, start: u32 },
    #[error("stale reviewed acquisition scope at {path}:{start}")]
    StaleScope { path: String, start: u32 },
    #[error("scope at {path}:{start} was not observed in any declared configuration")]
    UnobservedScope { path: String, start: u32 },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Acquisition {
    Shared,
    Exclusive,
    TryShared,
    TryExclusive,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Api {
    pub receivers: Vec<String>,
    pub defining_crates: Vec<String>,
    pub operations: BTreeMap<String, Acquisition>,
    pub rationale: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub apis: Vec<Api>,
}

impl Catalog {
    pub fn parse(bytes: &[u8]) -> Result<Self, error_stack::Report<ReportError>> {
        let catalog: Self = serde_json::from_slice(bytes).map_err(|error| {
            error_stack::Report::new(ReportError::Json).attach(error.to_string())
        })?;
        let mut identities = BTreeMap::new();
        for api in &catalog.apis {
            for receiver in &api.receivers {
                for operation in api.operations.keys() {
                    let key = (receiver.clone(), operation.clone());
                    if identities.insert(key, ()).is_some() {
                        return Err(error_stack::Report::new(ReportError::DuplicateApi {
                            receiver: receiver.clone(),
                            operation: operation.clone(),
                        }));
                    }
                }
            }
        }
        Ok(catalog)
    }

    pub fn acquisition(
        &self,
        receiver: &str,
        defining_crate: &str,
        operation: &str,
    ) -> Option<Acquisition> {
        // This is the finite reviewed API catalog, not graph state: currently eight families,
        // at most three receiver identities and three defining crates in any family.
        for api in &self.apis {
            if !api.receivers.iter().any(|name| name == receiver) {
                continue;
            }
            if !api
                .defining_crates
                .iter()
                .any(|name| name == defining_crate)
            {
                continue;
            }
            if let Some(acquisition) = api.operations.get(operation) {
                return Some(*acquisition);
            }
        }
        None
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
#[serde(tag = "class", rename_all = "snake_case", deny_unknown_fields)]
pub enum Disposition {
    OutsideDataPlane { boundary: String },
    Lifecycle,
    Observer,
    RetainedState,
    BoundedProtocol { key: String, bound: String },
    Debt { delivery: String },
}

impl Disposition {
    pub fn is_debt(&self) -> bool {
        matches!(self, Self::Debt { .. })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub site: SiteId,
    pub source_sha256: String,
    pub operation: String,
    pub owners: BTreeSet<String>,
    pub frequency: String,
    pub rationale: String,
    pub disposition: Disposition,
}

impl Scope {
    pub fn validate(&self) -> Result<(), error_stack::Report<ReportError>> {
        if self.owners.is_empty()
            || self.owners.iter().any(|owner| owner.trim().is_empty())
            || self.frequency.trim().is_empty()
            || self.rationale.trim().is_empty()
        {
            return Err(error_stack::Report::new(ReportError::IncompleteScope {
                path: self.site.path.clone(),
                start: self.site.start,
            }));
        }
        if let Disposition::OutsideDataPlane { boundary } = &self.disposition
            && boundary.trim().is_empty()
        {
            return Err(error_stack::Report::new(ReportError::IncompleteScope {
                path: self.site.path.clone(),
                start: self.site.start,
            }));
        }
        if let Disposition::BoundedProtocol { key, bound } = &self.disposition
            && (key.trim().is_empty() || bound.trim().is_empty())
        {
            return Err(error_stack::Report::new(ReportError::UnboundedProtocol {
                path: self.site.path.clone(),
                start: self.site.start,
            }));
        }
        Ok(())
    }
}

pub fn parse_scopes(
    bytes: &[u8],
) -> Result<BTreeMap<SiteId, Scope>, error_stack::Report<ReportError>> {
    let scopes: Vec<Scope> = serde_json::from_slice(bytes)
        .map_err(|error| error_stack::Report::new(ReportError::Json).attach(error.to_string()))?;
    let mut result = BTreeMap::new();
    for scope in scopes {
        scope.validate()?;
        let site = scope.site.clone();
        if result.insert(site.clone(), scope).is_some() {
            return Err(error_stack::Report::new(ReportError::DuplicateScope {
                path: site.path,
                start: site.start,
            }));
        }
    }
    Ok(result)
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyInput {
    pub reports: Vec<CompilerReport>,
    pub scopes: Vec<Scope>,
    pub source_sha256: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClassifiedSite {
    pub acquisition: AuthoredSite,
    pub scope: Scope,
}

impl PolicyInput {
    pub fn classify(&self) -> Result<Vec<ClassifiedSite>, error_stack::Report<ReportError>> {
        let bytes = serde_json::to_vec(&self.scopes).map_err(|error| {
            error_stack::Report::new(ReportError::Json).attach(error.to_string())
        })?;
        let scopes = parse_scopes(&bytes)?;
        let authored = aggregate(&self.reports)?;
        let mut observed = BTreeSet::new();
        let mut result = Vec::new();
        for site in authored {
            let key = &site.site;
            let Some(scope) = scopes.get(key) else {
                return Err(error_stack::Report::new(ReportError::MissingScope {
                    path: key.path.clone(),
                    start: key.start,
                })
                .attach(format!("resolved acquisition: {site:#?}")));
            };
            let owners = site
                .configurations
                .values()
                .flatten()
                .map(|finding| finding.owner.clone())
                .collect::<BTreeSet<_>>();
            if self.source_sha256.get(&key.path) != Some(&scope.source_sha256)
                || owners != scope.owners
                || site
                    .configurations
                    .values()
                    .flatten()
                    .any(|finding| finding.operation != scope.operation)
            {
                return Err(error_stack::Report::new(ReportError::StaleScope {
                    path: key.path.clone(),
                    start: key.start,
                })
                .attach(format!(
                    "review: {scope:#?}; resolved acquisition: {site:#?}"
                )));
            }
            observed.insert(key.clone());
            result.push(ClassifiedSite {
                acquisition: site,
                scope: scope.clone(),
            });
        }
        for key in scopes.keys() {
            if !observed.contains(key) {
                return Err(error_stack::Report::new(ReportError::UnobservedScope {
                    path: key.path.clone(),
                    start: key.start,
                }));
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
