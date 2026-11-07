//! Repository tooling, outside the product layer order.
//! Owns: combining typed provenance into review suggestions and relocated acquisitions.
//! Depends on: candidate facts, baseline history and the audit's cached Git/GitHub readers.
//! Must not know: enforcement decisions or product runtime state.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
};

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_lint_report::AuthoredSite;

use super::{
    blame::Blame,
    candidates::{Candidate, CandidateFacts, Pattern, Relocated},
    catalog::Catalog,
    encoding::Encoding,
    git::Git,
    history::{Commit, History},
    hunks::{Hunk, Hunks},
    line_history::{LineHistory, LineRange},
    pull_requests::PullRequests,
    report::{Attribution, Committed, Level, Origin, RuleEvidence},
    spellings::Spellings,
};
use crate::error::{AuditRecord, Error, Result};

struct Evidence {
    rules: Vec<RuleEvidence>,
    level: Level,
}

pub(super) struct Attributor {
    git: Git,
    pub history: History,
    blame: Blame,
    hunks: Hunks,
    spellings: Spellings,
    lines: LineHistory,
    catalog: Catalog,
    pub(super) pull_requests: Option<PullRequests>,
}

impl Attributor {
    pub fn new(git: Git, pull_requests: bool) -> Result<Self> {
        let history = History::load(&git)?;
        let pull_requests = if pull_requests {
            Some(PullRequests::remote(&git)?)
        } else {
            None
        };
        Ok(Self {
            git,
            history,
            blame: Blame::new(),
            hunks: Hunks::new(),
            spellings: Spellings::new(),
            lines: LineHistory::new(),
            catalog: Catalog::new(),
            pull_requests,
        })
    }

    pub fn prepare(&mut self, candidates: &[Candidate]) -> Result<()> {
        self.blame.load(
            &self.git,
            candidates
                .iter()
                .map(|candidate| candidate.path().into())
                .collect(),
        )?;
        self.lines
            .load(&self.git, candidates.iter().map(LineRange::of).collect())
    }

    pub fn relocated(&mut self, sites: Vec<AuthoredSite>) -> Result<Vec<Candidate>> {
        self.blame.load(
            &self.git,
            sites.iter().map(|site| site.site.path.clone()).collect(),
        )?;
        let encoding = Encoding::new();
        let mut records = Vec::new();
        for site in sites {
            let mut findings = site.configurations.values().flatten();
            let first = findings.next().ok_or_else(|| {
                Report::new(Error::AuditRecord {
                    record: AuditRecord::Candidate,
                })
            })?;
            let Some(line) = self.blame.line(&site.site.path, first.span.line) else {
                continue;
            };
            if !self
                .history
                .lowered(line.sha, "data_plane_lock_acquisitions")
            {
                continue;
            }
            let commit = self
                .history
                .commits
                .get(line.sha)
                .verified("a lowered count has its baseline commit");
            if self.history.lock_scope(&site.site.path, &commit.date) {
                continue;
            }
            let mut contexts = BTreeMap::new();
            for finding in std::iter::once(first).chain(findings) {
                contexts.insert(
                    encoding.identity(&finding.context)?,
                    finding.context.clone(),
                );
            }
            records.push(CandidateFacts {
                owner: first.owner.clone(),
                contexts: contexts.into_values().collect(),
                configurations: site.configurations.keys().cloned().collect(),
                pattern: Pattern::Relocated(Relocated::RelocatedAcquisition {
                    receiver: first.receiver.clone(),
                    operation: first.operation.clone(),
                    acquisition: first.acquisition,
                }),
                span: first.span.clone(),
                expansion: first.expansion.clone(),
            });
        }
        Candidate::identify(records, &encoding)
    }

    pub fn attribute(&mut self, candidate: &Candidate) -> Result<Attribution> {
        let provenance: Vec<_> = candidate
            .lines()
            .into_iter()
            .filter_map(|number| self.blame.line(candidate.path(), number))
            .collect();
        if provenance.is_empty() || provenance.iter().any(|line| !line.committed()) {
            return Ok(Attribution::uncommitted());
        }
        // Select the latest commit, keeping the first candidate line when its lines share a
        // commit. That line anchors both the removed-spelling evidence and the report excerpt.
        let introducing = provenance
            .iter()
            .min_by_key(|line| Reverse((line.author_time, line.sha)))
            .verified("nonempty provenance has a most recent line");
        let sha = introducing.sha.to_owned();
        let subject = introducing.subject.to_owned();
        let path = introducing.path.to_owned();
        let original = introducing.original;
        let commits: BTreeSet<_> = provenance
            .iter()
            .map(|line| line.sha[..12].to_owned())
            .collect();
        let hunk = self.hunks.around(&self.git, &sha, &path, original)?;
        let mut sources = EvidenceSources {
            git: &self.git,
            history: &self.history,
            blame: &mut self.blame,
            spellings: &mut self.spellings,
            catalog: &self.catalog,
            pull_requests: &mut self.pull_requests,
        };
        let evidence = sources.evidence(candidate, &sha, &subject, &path, hunk)?;
        let (before, after) = match hunk {
            Some(hunk) => {
                let shown = hunk.excerpt(original, 12);
                (shown.before, shown.after)
            }
            None => (Vec::new(), Vec::new()),
        };
        let mut level = evidence.level;
        let mut data = Committed {
            commit: Commit {
                date: sources.blame.date(&self.git, &sha)?,
                sha: sha.clone(),
                subject,
            },
            commits: commits.into_iter().collect(),
            before,
            after,
            rules: evidence.rules,
            origin: None,
        };
        if level != Level::Suspected {
            let range = LineRange::of(candidate);
            let changes = self.lines.of(&range);
            for change in changes {
                if change.sha == sha {
                    continue;
                }
                let earlier = sources.evidence(
                    candidate,
                    &change.sha,
                    &change.subject,
                    candidate.path(),
                    None,
                )?;
                if earlier.level >= level
                    || !matches!(earlier.level, Level::Suspected | Level::Possible)
                {
                    continue;
                }
                level = earlier.level;
                data.origin = Some(Origin {
                    date: sources.blame.date(&self.git, &change.sha)?,
                    sha: change.sha.clone(),
                    subject: change.subject.clone(),
                    rules: earlier.rules,
                });
                if level == Level::Suspected {
                    break;
                }
            }
        }
        Ok(Attribution::committed(level, data))
    }
}

/// Separate the caches updated by evidence lookup from immutable line-history and hunk storage,
/// so attribution borrows those potentially large observations without copying them.
struct EvidenceSources<'a> {
    git: &'a Git,
    history: &'a History,
    blame: &'a mut Blame,
    spellings: &'a mut Spellings,
    catalog: &'a Catalog,
    pull_requests: &'a mut Option<PullRequests>,
}

impl EvidenceSources<'_> {
    fn evidence(
        &mut self,
        candidate: &Candidate,
        sha: &str,
        subject: &str,
        path: &str,
        hunk: Option<&Hunk>,
    ) -> Result<Evidence> {
        let date = self.blame.date(self.git, sha)?;
        let mut rules = Vec::new();
        let mut strongest = Level::PreExisting;
        let fragments = candidate.fragments();
        for rule in self.catalog.owners(&candidate.pattern) {
            let event = self.history.event(sha, rule.name);
            let removed = match hunk {
                Some(hunk) => {
                    rule.count(&hunk.before.join("\n")) > rule.count(&hunk.after.join("\n"))
                }
                None => false,
            };
            let removals =
                self.spellings
                    .removals(self.git, self.history, self.catalog, sha, rule, path)?;
            let rewrite = match &mut self.pull_requests {
                Some(pulls) => pulls.rewrite(subject, sha, path, rule, &fragments)?,
                None => None,
            };
            let predates = self.history.predates(&date, rule.name);
            let avoided = rule.scoped
                && self.history.lock_scope(path, &date)
                && self.history.lock_textual(&date);
            if !predates {
                let explained = removed
                    || removals.file
                    || rule.spelling.is_none()
                    || (rule.scoped && removals.commit);
                let level = if rewrite.is_some() || (event.is_some() && explained) {
                    Level::Suspected
                } else if event.is_some() || removed || removals.file || removals.commit || avoided
                {
                    Level::Possible
                } else {
                    Level::Unattributed
                };
                strongest = strongest.min(level);
            }
            rules.push(RuleEvidence {
                rule: rule.name.into(),
                event,
                spelling_removed: removed,
                file_spelling_removed: removals.file,
                commit_spelling_removed: removals.commit,
                written_while_counted: avoided,
                predates_rule: predates,
                pull_request_rewrite: rewrite,
            });
        }
        Ok(Evidence {
            rules,
            level: strongest,
        })
    }
}
