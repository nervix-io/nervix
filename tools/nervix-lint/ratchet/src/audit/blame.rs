//! Repository tooling, outside the product layer order.
//! Owns: cached working-tree line provenance with metadata retained once per Git observation.
//! Depends on: bounded local Git reads and its current porcelain records.
//! Must not know: attribution suggestions or product state ownership.

use std::collections::{BTreeMap, BTreeSet};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};

use super::git::Git;
use crate::error::{AuditRecord, Error, Result};

#[derive(Clone, Eq, PartialEq)]
struct Metadata {
    path: String,
    author_time: i64,
    subject: String,
}
struct Line {
    sha: String,
    original: u64,
    metadata: usize,
}
struct Header {
    sha: String,
    original: u64,
}

#[derive(Default)]
struct Patch {
    path: Option<String>,
    author_time: Option<i64>,
    subject: Option<String>,
}

impl Patch {
    fn matches(&self, metadata: &Metadata) -> bool {
        if let Some(path) = &self.path
            && *path != metadata.path
        {
            return false;
        }
        if let Some(time) = self.author_time
            && time != metadata.author_time
        {
            return false;
        }
        if let Some(subject) = &self.subject
            && *subject != metadata.subject
        {
            return false;
        }
        true
    }

    fn apply(self, previous: Option<&Metadata>, path: &str) -> Result<Metadata> {
        let path = match self.path {
            Some(path) => path,
            None => match previous {
                Some(previous) => previous.path.clone(),
                None => path.into(),
            },
        };
        let author_time = match self.author_time {
            Some(time) => time,
            None => {
                previous
                    .ok_or_else(|| {
                        Report::new(Error::AuditRecord {
                            record: AuditRecord::Blame,
                        })
                    })?
                    .author_time
            }
        };
        let subject = match self.subject {
            Some(subject) => subject,
            None => previous
                .ok_or_else(|| {
                    Report::new(Error::AuditRecord {
                        record: AuditRecord::Blame,
                    })
                })?
                .subject
                .clone(),
        };
        Ok(Metadata {
            path,
            author_time,
            subject,
        })
    }
}

struct FileBlame {
    lines: Vec<Line>,
    metadata: Vec<Metadata>,
}

pub(super) struct BlameLine<'a> {
    pub sha: &'a str,
    pub original: u64,
    pub path: &'a str,
    pub author_time: i64,
    pub subject: &'a str,
}

impl BlameLine<'_> {
    pub fn committed(&self) -> bool {
        !self.sha.starts_with("0000000")
    }
}

impl FileBlame {
    fn parse(text: &str, path: &str) -> Result<Self> {
        let mut file = Self {
            lines: Vec::new(),
            metadata: Vec::new(),
        };
        let mut commits = BTreeMap::<String, usize>::new();
        let mut header = None;
        let mut patch = Patch::default();
        for line in Git::lines(text) {
            if line.starts_with('\t') {
                let current = header.as_ref().ok_or_else(|| {
                    Report::new(Error::AuditRecord {
                        record: AuditRecord::Blame,
                    })
                })?;
                let Header { sha, original } = current;
                let previous_index = commits.get(sha).copied();
                let index = match previous_index {
                    Some(index) if patch.matches(&file.metadata[index]) => {
                        patch = Patch::default();
                        index
                    }
                    _ => {
                        let previous = previous_index.map(|index| &file.metadata[index]);
                        let metadata = std::mem::take(&mut patch).apply(previous, path)?;
                        let index = file.metadata.len();
                        file.metadata.push(metadata);
                        commits.insert(sha.clone(), index);
                        index
                    }
                };
                file.lines.push(Line {
                    sha: sha.clone(),
                    original: *original,
                    metadata: index,
                });
                continue;
            }
            let mut fields = line.split(' ');
            let first = fields.next().unwrap_or("");
            if Git::is_sha(first)
                && let Some(original) = fields.next()
                && fields.next().is_some()
            {
                header = Some(Header {
                    sha: first.into(),
                    original: original.parse().change_context(Error::AuditRecord {
                        record: AuditRecord::Blame,
                    })?,
                });
                patch = Patch::default();
                continue;
            }
            if let Some((key, value)) = line.split_once(' ') {
                match key {
                    "filename" => patch.path = Some(value.into()),
                    "summary" => patch.subject = Some(value.into()),
                    "author-time" => {
                        patch.author_time =
                            Some(value.parse().change_context(Error::AuditRecord {
                                record: AuditRecord::Blame,
                            })?)
                    }
                    _ => {}
                }
            }
        }
        Ok(file)
    }

    fn line(&self, number: u32) -> Option<BlameLine<'_>> {
        let index = usize::try_from(number.checked_sub(1)?)
            .assured("a u32 line fits every supported native tooling target");
        let line = self.lines.get(index)?;
        let metadata = self
            .metadata
            .get(line.metadata)
            .verified("a parsed line names metadata in the same immutable file");
        Some(BlameLine {
            sha: &line.sha,
            original: line.original,
            path: &metadata.path,
            author_time: metadata.author_time,
            subject: &metadata.subject,
        })
    }
}

pub(super) struct Blame {
    files: BTreeMap<String, FileBlame>,
    dates: BTreeMap<String, String>,
}

impl Blame {
    pub fn new() -> Self {
        Self {
            files: BTreeMap::new(),
            dates: BTreeMap::new(),
        }
    }

    pub fn load(&mut self, git: &Git, paths: BTreeSet<String>) -> Result<()> {
        let mut missing = Vec::new();
        for path in paths {
            if !self.files.contains_key(&path) {
                missing.push(path);
            }
        }
        let files = git.read_all(&missing, |path| {
            let text = git.text(&["blame", "--porcelain", "--", path])?;
            FileBlame::parse(&text, path)
        })?;
        for (path, file) in missing.into_iter().zip(files) {
            self.files.insert(path, file);
        }
        Ok(())
    }

    pub fn line(&self, path: &str, number: u32) -> Option<BlameLine<'_>> {
        self.files.get(path)?.line(number)
    }

    pub fn date(&mut self, git: &Git, sha: &str) -> Result<String> {
        let date = match self.dates.entry(sha.into()) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let date = git.text(&["show", "-s", "--format=%ad", "--date=short", sha])?;
                entry.insert(date.trim().into())
            }
        };
        Ok(date.clone())
    }
}
