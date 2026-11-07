//! Repository tooling, outside the product layer order.
//! Owns: each introducing commit's net removal of the historical governed spellings.
//! Depends on: the fixed rule catalog, baseline scope history and Git diffs.
//! Must not know: current compiler findings or candidate dispositions.

use std::collections::BTreeMap;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use regex::Regex;

use super::{
    catalog::{Catalog, Rule},
    git::Git,
    history::History,
};
use crate::error::{AuditRecord, Error, Result};

type FileCounts = BTreeMap<String, BTreeMap<String, i64>>;

pub(super) struct Spellings {
    header: Regex,
    removed: BTreeMap<String, FileCounts>,
}

impl Spellings {
    pub fn new() -> Self {
        Self {
            header: Regex::new(r"^diff --git a/(?P<before>\S+) b/(?P<after>\S+)$")
                .assured("the checked-in Git header expression is valid"),
            removed: BTreeMap::new(),
        }
    }

    fn prepare(
        &mut self,
        git: &Git,
        history: &History,
        catalog: &Catalog,
        sha: &str,
    ) -> Result<()> {
        if self.removed.contains_key(sha) {
            return Ok(());
        }
        let date = git.text(&["show", "-s", "--format=%ad", "--date=short", sha])?;
        let diff = git.text(&["show", "--format=", "--unified=0", sha, "--", "*.rs"])?;
        let mut files = FileCounts::new();
        let mut path = None::<String>;
        for line in Git::lines(&diff) {
            if let Some(header) = self.header.captures(line) {
                let current = header["after"].to_owned();
                files.insert(
                    current.clone(),
                    catalog
                        .rules()
                        .map(|rule| (rule.name.to_owned(), 0))
                        .collect(),
                );
                path = Some(current);
                continue;
            }
            let removed = line.starts_with('-') && !line.starts_with("---");
            let added = line.starts_with('+') && !line.starts_with("+++");
            if !removed && !added {
                continue;
            }
            let path = path.as_ref().ok_or_else(|| {
                Report::new(Error::AuditRecord {
                    record: AuditRecord::Hunk,
                })
            })?;
            let counts = files
                .get_mut(path)
                .verified("a parsed path installed its count map");
            for rule in catalog.rules() {
                if rule.spelling.is_none()
                    || (rule.scoped && !history.lock_scope(path, date.trim()))
                {
                    continue;
                }
                let count = i64::try_from(rule.count(line))
                    .assured("matches occupy bytes in one allocated native string");
                let current = counts
                    .get_mut(rule.name)
                    .verified("every catalog rule has its count slot");
                let next = if removed {
                    current.checked_add(count)
                } else {
                    current.checked_sub(count)
                };
                *current = next.assured(
                    "the absolute net count is bounded by the allocated complete diff's byte \
                     length",
                );
            }
        }
        self.removed.insert(sha.into(), files);
        Ok(())
    }

    pub fn removals(
        &mut self,
        git: &Git,
        history: &History,
        catalog: &Catalog,
        sha: &str,
        rule: &Rule,
        path: &str,
    ) -> Result<Removals> {
        if rule.spelling.is_none() {
            return Ok(Removals {
                commit: false,
                file: false,
            });
        }
        self.prepare(git, history, catalog, sha)?;
        let files = &self.removed[sha];
        let mut total = 0_i64;
        for counts in files.values() {
            total = total
                .checked_add(counts[rule.name])
                .assured("all net removals are bounded by the same allocated complete diff");
        }
        let file = match files.get(path) {
            Some(counts) => counts[rule.name] > 0,
            None => false,
        };
        Ok(Removals {
            commit: total > 0,
            file,
        })
    }
}

pub(super) struct Removals {
    pub commit: bool,
    pub file: bool,
}
