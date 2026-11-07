//! Repository tooling, outside the product layer order.
//! Owns: bounded retry of GitHub reads and attribution through squash-folded commits.
//! Depends on: the gh command, maintained JSON decoding and the rule catalog.
//! Must not know: current compiler diagnostics or product execution.

use std::{
    collections::BTreeMap,
    process::{Command, Output},
    time::Duration,
};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::thread::sleep;
use regex::Regex;
use serde::Deserialize;

use super::{candidates::normalize, catalog::Rule, git::Git, report::Rewrite};
use crate::error::{Error, Result};

pub(super) trait GithubRead {
    fn read(&mut self, path: &str) -> Result<Vec<u8>>;
}

pub(super) struct Api {
    transient: Regex,
}

impl Api {
    pub fn new() -> Self {
        Self {
            transient: Regex::new(
                r"\bHTTP 5\d\d\b|connection reset|i/o timeout|TLS handshake timeout",
            )
            .assured("the checked-in gh retry expression is valid"),
        }
    }

    pub fn read_using(
        &self,
        path: &str,
        mut read: impl FnMut() -> std::io::Result<Output>,
        mut pause: impl FnMut(Duration),
    ) -> Result<Vec<u8>> {
        let mut failures = Vec::new();
        for attempt in 0..4_u32 {
            let output = read().change_context(Error::Io {
                operation: "start audit GitHub read",
                path: "gh".into(),
            })?;
            if output.status.success() {
                return Ok(output.stdout);
            }
            let stderr = Git::decode(output.stderr).change_context(Error::Io {
                operation: "decode audit GitHub failure",
                path: "gh".into(),
            })?;
            let transient = self.transient.is_match(&stderr);
            failures.push(stderr.trim().to_owned());
            if !transient {
                break;
            }
            if attempt < 3 {
                let seconds = 2_u64
                    .checked_shl(attempt)
                    .assured("four reads schedule only the fixed 2/4/8-second backoffs");
                pause(Duration::from_secs(seconds));
            }
        }
        Err(Report::new(Error::AuditGithub {
            path: path.into(),
            attempts: failures.len(),
        })
        .attach(failures.join("\n")))
    }
}

impl GithubRead for Api {
    fn read(&mut self, path: &str) -> Result<Vec<u8>> {
        self.read_using(
            path,
            || {
                Command::new("gh")
                    .args(["api", "--paginate", "--slurp", path])
                    .output()
            },
            sleep,
        )
    }
}

#[derive(Deserialize)]
struct Pull {
    number: u64,
    merged_at: Option<String>,
}
#[derive(Deserialize)]
struct Parent {}
#[derive(Deserialize)]
struct PullCommit {
    sha: String,
    parents: Vec<Parent>,
}
#[derive(Deserialize)]
struct CommitDetails {
    #[serde(default)]
    files: Vec<ChangedFile>,
}
#[derive(Deserialize)]
struct ChangedFile {
    filename: String,
    #[serde(default)]
    patch: String,
}

pub(super) struct PullRequests {
    repository: String,
    reader: Box<dyn GithubRead>,
    subject: Regex,
    changes: BTreeMap<u64, BTreeMap<String, Vec<Rewrite>>>,
    merged: BTreeMap<String, Option<u64>>,
}

impl PullRequests {
    pub fn remote(git: &Git) -> Result<Self> {
        let remote = git.text(&["remote", "get-url", "origin"])?;
        let expression =
            Regex::new(r"github\.com[:/](?P<owner>[^/]+)/(?P<repository>[^/]+?)(?:\.git)?$")
                .assured("the checked-in GitHub origin expression is valid");
        let captures = expression.captures(remote.trim()).ok_or_else(|| {
            Report::new(Error::AuditRemote {
                remote: remote.trim().into(),
            })
        })?;
        let repository = format!("{}/{}", &captures["owner"], &captures["repository"]);
        Ok(Self::new(repository, Box::new(Api::new())))
    }

    pub fn new(repository: String, reader: Box<dyn GithubRead>) -> Self {
        Self {
            repository,
            reader,
            subject: Regex::new(r"\(#(\d+)\)[\s\x1c-\x1f]*$")
                .assured("the checked-in squash subject expression is valid"),
            changes: BTreeMap::new(),
            merged: BTreeMap::new(),
        }
    }

    fn pages<T: serde::de::DeserializeOwned>(&mut self, path: &str) -> Result<Vec<T>> {
        let bytes = self.reader.read(path)?;
        serde_json::from_slice(&bytes).change_context(Error::Json { path: path.into() })
    }

    pub fn number(&mut self, subject: &str, sha: &str) -> Result<Option<u64>> {
        if let Some(found) = self.subject.captures(subject) {
            let digits = dec_from_char::normalize_decimals(&found[1]);
            let number = digits.parse().change_context(Error::AuditRecord {
                record: crate::error::AuditRecord::Commit,
            })?;
            return Ok(Some(number));
        }
        if let Some(number) = self.merged.get(sha) {
            return Ok(*number);
        }
        let path = format!("repos/{}/commits/{sha}/pulls", self.repository);
        let mut merged = None;
        for page in self.pages::<Vec<Pull>>(&path)? {
            for pull in page {
                if merged.is_none() && pull.merged_at.is_some() {
                    merged = Some(pull.number);
                }
            }
        }
        self.merged.insert(sha.into(), merged);
        Ok(merged)
    }

    fn history(&mut self, number: u64) -> Result<&BTreeMap<String, Vec<Rewrite>>> {
        if !self.changes.contains_key(&number) {
            let mut files = BTreeMap::<String, Vec<Rewrite>>::new();
            let path = format!("repos/{}/pulls/{number}/commits", self.repository);
            for page in self.pages::<Vec<PullCommit>>(&path)? {
                for commit in page {
                    if commit.parents.len() != 1 {
                        continue;
                    }
                    let path = format!("repos/{}/commits/{}", self.repository, commit.sha);
                    let details = self.pages::<CommitDetails>(&path)?;
                    let first = details.into_iter().next().ok_or_else(|| {
                        Report::new(Error::AuditRecord {
                            record: crate::error::AuditRecord::Commit,
                        })
                    })?;
                    for changed in first.files {
                        let mut removed = Vec::new();
                        let mut added = Vec::new();
                        let patch = Git::decode(changed.patch.into_bytes())
                            .assured("ASCII newline normalization preserves a UTF-8 JSON string");
                        for line in Git::lines(&patch) {
                            if !line.starts_with("---")
                                && let Some(value) = line.strip_prefix('-')
                            {
                                removed.push(value.into());
                            } else if !line.starts_with("+++")
                                && let Some(value) = line.strip_prefix('+')
                            {
                                added.push(value.into());
                            }
                        }
                        files.entry(changed.filename).or_default().push(Rewrite {
                            sha: commit.sha.clone(),
                            removed,
                            added,
                        });
                    }
                }
            }
            self.changes.insert(number, files);
        }
        Ok(&self.changes[&number])
    }

    pub fn rewrite(
        &mut self,
        subject: &str,
        sha: &str,
        path: &str,
        rule: &Rule,
        fragments: &[String],
    ) -> Result<Option<Rewrite>> {
        let Some(number) = self.number(subject, sha)? else {
            return Ok(None);
        };
        if rule.spelling.is_none() {
            return Ok(None);
        }
        let Some(changes) = self.history(number)?.get(path) else {
            return Ok(None);
        };
        for change in changes {
            let removed = rule.count_lines(&change.removed);
            let added = rule.count_lines(&change.added);
            if removed <= added {
                continue;
            }
            for line in &change.added {
                let written = normalize(line);
                if fragments.iter().any(|fragment| written.contains(fragment)) {
                    return Ok(Some(change.clone()));
                }
            }
        }
        Ok(None)
    }
}
