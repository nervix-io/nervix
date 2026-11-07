//! Repository tooling, outside the product layer order.
//! Owns: cached introducing-line histories in Git's newest-first order.
//! Depends on: finite candidate ranges and bounded local Git reads.
//! Must not know: candidate classification or current enforcement policy.

use std::collections::{BTreeMap, BTreeSet};

use super::{candidates::Candidate, git::Git};
use crate::error::Result;

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct LineRange {
    pub path: String,
    pub start: u32,
    pub end: u32,
}

impl LineRange {
    pub fn of(candidate: &Candidate) -> Self {
        Self {
            path: candidate.path().into(),
            start: candidate.span.line,
            end: candidate.span.end_line,
        }
    }
}

#[derive(Clone)]
pub(super) struct LineChange {
    pub sha: String,
    pub subject: String,
}

pub(super) struct LineHistory {
    changes: BTreeMap<LineRange, Vec<LineChange>>,
}

impl LineHistory {
    pub fn new() -> Self {
        Self {
            changes: BTreeMap::new(),
        }
    }

    pub fn load(&mut self, git: &Git, ranges: BTreeSet<LineRange>) -> Result<()> {
        let mut missing = Vec::new();
        for range in ranges {
            if !self.changes.contains_key(&range) {
                missing.push(range);
            }
        }
        let histories = git.read_all(&missing, |range| {
            let selection = format!("-L{},{}:{}", range.start, range.end, range.path);
            let text = git.text(&["log", &selection, "-s", "--format=%H%x09%s"])?;
            let mut changes = Vec::new();
            for line in Git::lines(&text) {
                let (sha, subject) = match line.split_once('\t') {
                    Some(parts) => parts,
                    None => (line, ""),
                };
                if Git::is_sha(sha) {
                    changes.push(LineChange {
                        sha: sha.into(),
                        subject: subject.into(),
                    });
                }
            }
            Ok(changes)
        })?;
        for (range, history) in missing.into_iter().zip(histories) {
            self.changes.insert(range, history);
        }
        Ok(())
    }

    pub fn of(&self, range: &LineRange) -> &[LineChange] {
        match self.changes.get(range) {
            Some(changes) => changes,
            None => &[],
        }
    }
}
