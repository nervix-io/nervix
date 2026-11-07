//! Repository tooling, outside the product layer order.
//! Owns: introducing-commit hunk locations and bounded report excerpts.
//! Depends on: Git's zero-context diff, maintained regex parsing and typed audit failures.
//! Must not know: candidate attribution or enforcement policy.

use std::collections::BTreeMap;

use error_stack::ResultExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use regex::Regex;
use serde::{Deserialize, Serialize};

use super::git::Git;
use crate::error::{AuditRecord, Error, Result};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct Hunk {
    pub start: u64,
    pub length: u64,
    pub before: Vec<String>,
    pub after: Vec<String>,
}

impl Hunk {
    pub fn contains(&self, line: u64) -> bool {
        let length = self.length.max(1);
        if line < self.start {
            return false;
        }
        line.checked_sub(self.start)
            .verified("the line is at or after the hunk start")
            < length
    }

    pub fn excerpt(&self, line: u64, reach: usize) -> Self {
        let twice = reach
            .checked_mul(2)
            .assured("the fixed audit excerpt is bounded");
        let width = twice
            .checked_add(1)
            .assured("the fixed audit excerpt is bounded");
        let distance = line
            .checked_sub(self.start)
            .assured("the selected hunk contains its candidate");
        let first =
            distance.saturating_sub(u64::try_from(reach).assured("the fixed reach fits u64"));
        // The excerpt starts at most reach lines before the candidate; it is clamped to the hunk.
        let remaining = self.length.saturating_sub(first);
        Self {
            start: self
                .start
                .checked_add(first)
                .assured("the first excerpt line is inside the Git hunk"),
            length: remaining.min(u64::try_from(width).assured("the fixed width fits u64")),
            before: Self::window(&self.before, first, width),
            after: Self::window(&self.after, first, width),
        }
    }

    fn window(lines: &[String], first: u64, width: usize) -> Vec<String> {
        // Keep the end of a shorter side visible when the candidate is far along the longer side.
        let latest = lines.len().saturating_sub(width);
        let first = first.min(u64::try_from(latest).assured("a native slice length fits u64"));
        let start = usize::try_from(first).verified("the index is at most the native slice length");
        let available = lines
            .len()
            .checked_sub(start)
            .verified("the start is inside the slice");
        let end = start
            .checked_add(available.min(width))
            .verified("the end is inside the slice");
        lines[start..end].to_vec()
    }
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct FileRevision {
    sha: String,
    path: String,
}

pub(super) struct Hunks {
    header: Regex,
    diffs: BTreeMap<FileRevision, Vec<Hunk>>,
}

impl Hunks {
    pub fn new() -> Self {
        Self {
            header: Regex::new(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
                .assured("the Git hunk expression is fixed"),
            diffs: BTreeMap::new(),
        }
    }

    pub fn parse(&self, diff: &str) -> Result<Vec<Hunk>> {
        let mut hunks = Vec::<Hunk>::new();
        for line in Git::lines(diff) {
            if let Some(header) = self.header.captures(line) {
                let start = header[3].parse().change_context(Error::AuditRecord {
                    record: AuditRecord::Hunk,
                })?;
                let length = match header.get(4) {
                    Some(length) => length.as_str().parse().change_context(Error::AuditRecord {
                        record: AuditRecord::Hunk,
                    })?,
                    None => 1,
                };
                hunks.push(Hunk {
                    start,
                    length,
                    before: Vec::new(),
                    after: Vec::new(),
                });
                continue;
            }
            let Some(hunk) = hunks.last_mut() else {
                continue;
            };
            if !line.starts_with("---")
                && let Some(before) = line.strip_prefix('-')
            {
                hunk.before.push(before.into());
            } else if !line.starts_with("+++")
                && let Some(after) = line.strip_prefix('+')
            {
                hunk.after.push(after.into());
            }
        }
        Ok(hunks)
    }

    pub fn around(&mut self, git: &Git, sha: &str, path: &str, line: u64) -> Result<Option<&Hunk>> {
        let revision = FileRevision {
            sha: sha.into(),
            path: path.into(),
        };
        if !self.diffs.contains_key(&revision) {
            let diff = git.text(&["show", "--format=", "--unified=0", sha, "--", path])?;
            let hunks = self.parse(&diff)?;
            self.diffs.insert(revision, hunks);
        }
        let revision = FileRevision {
            sha: sha.into(),
            path: path.into(),
        };
        // A diff is a sequence of disjoint hunks; containing-line lookup retains Git's order.
        for hunk in &self.diffs[&revision] {
            if hunk.contains(line) {
                return Ok(Some(hunk));
            }
        }
        Ok(None)
    }
}
