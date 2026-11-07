//! Repository tooling, outside the product layer order.
//! Owns: baseline count changes, first introductions and the textual lock counter's scope.
//! Depends on: typed Git observations and the external flat count history.
//! Must not know: current product classifications or candidate dispositions.

use std::collections::{BTreeMap, BTreeSet};

use error_stack::ResultExt as _;
use serde::{Deserialize, Serialize};

use super::git::Git;
use crate::error::{AuditRecord, Error, Result};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Commit {
    pub sha: String,
    pub date: String,
    pub subject: String,
}

#[derive(Clone, Copy, Debug)]
enum CountChange {
    Introduced(u64),
    Retired(u64),
    Adjusted { before: u64, after: u64 },
}

impl CountChange {
    fn event(self) -> String {
        match self {
            Self::Introduced(after) => format!("introduced at {after}"),
            Self::Retired(before) => format!("retired from {before}"),
            Self::Adjusted { before, after } if after < before => {
                format!("lowered {before}->{after}")
            }
            Self::Adjusted { before, after } => format!("raised {before}->{after}"),
        }
    }

    fn lowered(self) -> bool {
        matches!(self, Self::Adjusted { before, after } if after < before)
    }
}

pub(super) struct History {
    changes: BTreeMap<String, BTreeMap<String, CountChange>>,
    pub introduced: BTreeMap<String, Commit>,
    pub commits: BTreeMap<String, Commit>,
    pub counted_commit: String,
    pub resolved_commit: String,
}

impl History {
    pub fn load(git: &Git) -> Result<Self> {
        let log = git.text(&[
            "log",
            "--reverse",
            "--format=%H%x09%ad%x09%s",
            "--date=short",
            "--",
            "debt-baseline.json",
        ])?;
        let mut history = Self {
            changes: BTreeMap::new(),
            introduced: BTreeMap::new(),
            commits: BTreeMap::new(),
            counted_commit: "364aa492".into(),
            resolved_commit: "a1d9050d".into(),
        };
        let mut previous = BTreeMap::<String, u64>::new();
        for line in Git::lines(&log) {
            let mut fields = line.splitn(3, '\t');
            let sha = fields.next().ok_or_else(|| {
                error_stack::Report::new(Error::AuditRecord {
                    record: AuditRecord::Commit,
                })
            })?;
            let date = fields.next().ok_or_else(|| {
                error_stack::Report::new(Error::AuditRecord {
                    record: AuditRecord::Commit,
                })
            })?;
            let subject = fields.next().ok_or_else(|| {
                error_stack::Report::new(Error::AuditRecord {
                    record: AuditRecord::Commit,
                })
            })?;
            let commit = Commit {
                sha: sha.into(),
                date: date.into(),
                subject: subject.into(),
            };
            let current = Self::baseline(git, sha)?;
            let names: BTreeSet<_> = previous.keys().chain(current.keys()).cloned().collect();
            let mut changes = BTreeMap::new();
            for name in names {
                let before = previous.get(&name).copied();
                let after = current.get(&name).copied();
                if before == after {
                    continue;
                }
                let change = match (before, after) {
                    (None, Some(after)) => CountChange::Introduced(after),
                    (Some(before), None) => CountChange::Retired(before),
                    (Some(before), Some(after)) => CountChange::Adjusted { before, after },
                    (None, None) => continue,
                };
                changes.insert(name.clone(), change);
                if before.is_none() && after.is_some() {
                    history
                        .introduced
                        .entry(name)
                        .or_insert_with(|| commit.clone());
                }
            }
            history.changes.insert(sha.into(), changes);
            history.commits.insert(sha.into(), commit);
            previous = current;
        }
        Ok(history)
    }

    fn baseline(git: &Git, sha: &str) -> Result<BTreeMap<String, u64>> {
        let name = format!("{sha}:debt-baseline.json");
        let output = git.output(&["show", &name])?;
        // History intentionally treats a missing file or syntactically invalid historical JSON
        // as no declared counts. Current count data is decoded without defaulting a bad value.
        if !output.status.success() {
            return Ok(BTreeMap::new());
        }
        let text = Git::decode(output.stdout).change_context(Error::AuditRecord {
            record: AuditRecord::Baseline,
        })?;
        let value: serde_json::Value = match serde_json::from_str(&text) {
            Ok(value) => value,
            Err(_) => return Ok(BTreeMap::new()),
        };
        serde_json::from_value(value).change_context(Error::AuditRecord {
            record: AuditRecord::Baseline,
        })
    }

    pub fn event(&self, sha: &str, rule: &str) -> Option<String> {
        let changes = self.changes.get(sha)?;
        Some(changes.get(rule)?.event())
    }

    pub fn lowered(&self, sha: &str, rule: &str) -> bool {
        let Some(changes) = self.changes.get(sha) else {
            return false;
        };
        let Some(change) = changes.get(rule) else {
            return false;
        };
        change.lowered()
    }

    pub fn predates(&self, date: &str, rule: &str) -> bool {
        match self.introduced.get(rule) {
            Some(introduced) => date < introduced.date.as_str(),
            None => true,
        }
    }

    pub fn commit(&self, prefix: &str) -> Option<&Commit> {
        // The catalog's two fixed prefixes are unique in the baseline's Git history.
        let (sha, commit) = self.commits.range(prefix.to_owned()..).next()?;
        if sha.starts_with(prefix) {
            Some(commit)
        } else {
            None
        }
    }

    pub fn lock_scope(&self, path: &str, date: &str) -> bool {
        if [
            "src/runtime/",
            "crates/interconnect/src/",
            "src/runtime_ack.rs",
            "src/metrics.rs",
        ]
        .iter()
        .any(|prefix| path.starts_with(prefix))
        {
            return true;
        }
        if !path.starts_with("crates/connectors/") {
            return false;
        }
        match self.commit(&self.counted_commit) {
            Some(commit) => date >= commit.date.as_str(),
            None => false,
        }
    }

    pub fn lock_textual(&self, date: &str) -> bool {
        let Some(introduced) = self.introduced.get("data_plane_lock_acquisitions") else {
            return false;
        };
        if date < introduced.date.as_str() {
            return false;
        }
        match self.commit(&self.resolved_commit) {
            Some(resolved) => date <= resolved.date.as_str(),
            None => true,
        }
    }
}
