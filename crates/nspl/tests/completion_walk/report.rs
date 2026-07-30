//! Findings, their deduplication, the rendered report, and the checked-in baseline.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    path::Path,
};

use serde::Serialize;
use strum::AsRefStr;
use thiserror::Error;

/// How many recent labels a finding keeps as context. Enough to tell two grammar positions apart
/// without making the signature depend on the whole path that reached it.
pub const CONTEXT_LABELS: usize = 2;

/// How many leading labels name the statement shape a walk is working on.
pub const SHAPE_LABELS: usize = 2;

/// What kind of completion defect was observed. Ordered most to least severe, which is also the
/// order the report groups them in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, AsRefStr, Serialize)]
#[strum(serialize_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub enum FindingKind {
    /// The statement does not parse and completion offers nothing: the user is stuck.
    DeadEnd,
    /// The parser panicked on a walked input.
    Panic,
    /// A suggestion was offered that the parser then refused at the position it was inserted.
    RejectedSuggestion,
    /// Applying a suggestion produced input that no longer lexes.
    LexFailure,
    /// A suggestion label matches no known shape, or is a placeholder with no registered filler.
    UnknownLabel,
    /// Typing the first characters of a suggestion made that suggestion disappear.
    PrefixFilterDrop,
    /// The same suggestion was offered twice at one position.
    DuplicateSuggestion,
    /// Suggestions came back out of order, breaking the documented contract.
    UnsortedSuggestions,
    /// A canned free-form body the walker injected was rejected. A defect in this tool, not in the
    /// grammar, but it bounds coverage so it is reported rather than swallowed.
    CannedBodyRejected,
}

/// One observed defect, with everything needed to reproduce it.
#[derive(Debug, Clone, Serialize)]
pub struct Finding {
    pub kind: FindingKind,
    /// The suggestion label the finding is about, when there is one.
    pub label: Option<String>,
    /// The labels applied just before the failure.
    pub context: Vec<String>,
    /// The exact NSPL text that reproduces it.
    pub statement: String,
    /// Every label applied to reach the statement.
    pub path: Vec<String>,
    /// What completion offered at the failure point.
    pub suggestions: Vec<String>,
    /// Parser diagnostic or other explanation.
    pub detail: String,
}

impl Finding {
    pub fn new(
        kind: FindingKind,
        label: Option<String>,
        statement: String,
        path: Vec<String>,
        suggestions: Vec<String>,
        detail: String,
    ) -> Self {
        let context = Self::context_from(&path, label.as_deref());
        Self {
            kind,
            label,
            context,
            statement,
            path,
            suggestions,
            detail,
        }
    }

    /// The stable identity used for deduplication and for the baseline file. Deliberately excludes
    /// the reproduction text, so one root cause collapses to one line however many statements
    /// reach it.
    pub fn signature(&self) -> String {
        format!(
            "{}\t{}\t{}",
            self.kind.as_ref(),
            self.label.as_deref().unwrap_or("-"),
            if self.context.is_empty() {
                "<start>".to_string()
            } else {
                self.context.join(" > ")
            }
        )
    }

    fn context_from(path: &[String], label: Option<&str>) -> Vec<String> {
        // When the finding is about the label that was just applied, the context is what came
        // before it; otherwise the whole tail describes where the walk stands.
        let tail = match label {
            Some(label) if path.last().map(String::as_str) == Some(label) => {
                &path[..path.len() - 1]
            }
            _ => path,
        };
        tail.iter()
            .rev()
            .take(CONTEXT_LABELS)
            .rev()
            .cloned()
            .collect()
    }
}

/// Counters describing how much of the graph the walk covered.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Stats {
    pub levels: usize,
    pub states_evaluated: usize,
    pub states_expanded: usize,
    pub completed_statements: usize,
    pub deepest_path: usize,
}

/// Everything one walk produced.
#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub stats: Stats,
    findings: Vec<Finding>,
    /// Statement shapes the walk drove to a parsing statement, and how many ways it got there.
    /// This is the coverage counterpart to the findings: a family that never appears here was
    /// never completed by following suggestions alone.
    completed: BTreeMap<String, usize>,
    /// Statement shapes the walk stood in at least once, completed or not.
    reached: BTreeSet<String>,
    /// Free-form regions the walker had to step over, where completion is not grammar-derived.
    degenerate_regions: BTreeSet<String>,
    /// Places the walk stopped short of exhausting the graph. Never silent.
    truncations: Vec<String>,
}

impl Report {
    pub fn push(&mut self, finding: Finding) {
        self.findings.push(finding);
    }

    /// A path too short to name a shape is a prefix every family shares, so it is not a family that
    /// can be said to have been entered.
    pub fn note_reached(&mut self, path: &[String]) {
        if path.len() >= SHAPE_LABELS {
            self.reached.insert(Self::shape(path));
        }
    }

    fn shape(path: &[String]) -> String {
        match path.len() {
            0 => "<seed>".to_string(),
            _ => path[..path.len().min(SHAPE_LABELS)].join(" "),
        }
    }

    /// Shapes the walk entered but never drove to a parsing statement. Either following suggestions
    /// genuinely cannot finish them, or deduplication pruned the one path that could.
    pub fn unfinished(&self) -> Vec<&str> {
        self.reached
            .iter()
            .filter(|shape| !self.completed.contains_key(*shape))
            .map(String::as_str)
            .collect()
    }

    pub fn note_completed(&mut self, path: &[String]) {
        self.stats.completed_statements += 1;
        *self.completed.entry(Self::shape(path)).or_default() += 1;
    }

    pub fn note_degenerate_region(&mut self, opener: &str, context: &[String]) {
        let context = if context.is_empty() {
            "<start>".to_string()
        } else {
            context.join(" > ")
        };
        self.degenerate_regions
            .insert(format!("{opener} after {context}"));
    }

    pub fn note_truncation(&mut self, detail: String) {
        self.truncations.push(detail);
    }

    /// One finding per signature, keeping the first seen. Breadth-first order means that is also
    /// the shortest reproduction.
    pub fn unique_findings(&self) -> Vec<&Finding> {
        let mut seen = BTreeSet::new();
        let mut unique: Vec<&Finding> = self
            .findings
            .iter()
            .filter(|finding| seen.insert(finding.signature()))
            .collect();
        unique.sort_by_key(|finding| (finding.kind, finding.signature()));
        unique
    }

    pub fn signatures(&self) -> BTreeSet<String> {
        self.findings
            .iter()
            .map(Finding::signature)
            .collect::<BTreeSet<_>>()
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        let unique = self.unique_findings();

        writeln!(out, "== NSPL completion walk ==").expect("string write");
        writeln!(
            out,
            "levels {}, states evaluated {}, expanded {}, complete statements {}, deepest path {}",
            self.stats.levels,
            self.stats.states_evaluated,
            self.stats.states_expanded,
            self.stats.completed_statements,
            self.stats.deepest_path,
        )
        .expect("string write");
        writeln!(
            out,
            "{} findings ({} unique)",
            self.findings.len(),
            unique.len()
        )
        .expect("string write");

        let mut counts: BTreeMap<FindingKind, usize> = BTreeMap::new();
        for finding in &unique {
            *counts.entry(finding.kind).or_default() += 1;
        }
        for (kind, count) in &counts {
            writeln!(out, "  {:<20} {count}", kind.as_ref()).expect("string write");
        }

        let mut current = None;
        for finding in &unique {
            if current != Some(finding.kind) {
                current = Some(finding.kind);
                writeln!(out, "\n-- {} --", finding.kind.as_ref()).expect("string write");
            }
            writeln!(out, "\n  {}", finding.signature().replace('\t', "  ")).expect("string write");
            writeln!(out, "    statement:   {}", finding.statement).expect("string write");
            writeln!(out, "    path:        {}", finding.path.join(" > ")).expect("string write");
            if !finding.suggestions.is_empty() {
                writeln!(out, "    suggestions: {}", finding.suggestions.join(", "))
                    .expect("string write");
            }
            if !finding.detail.is_empty() {
                writeln!(out, "    detail:      {}", finding.detail).expect("string write");
            }
        }

        if !self.completed.is_empty() {
            writeln!(
                out,
                "\n-- statement shapes completed ({}) --",
                self.completed.len()
            )
            .expect("string write");
            for (shape, count) in &self.completed {
                writeln!(out, "  {shape:<40} {count}").expect("string write");
            }
        }

        let unfinished = self.unfinished();
        if !unfinished.is_empty() {
            writeln!(
                out,
                "\n-- shapes entered but never completed ({}) --\n(either following suggestions \
                 cannot finish them, or deduplication pruned\nthe one path that could: re-check \
                 with a larger --signature-window before\ntreating one of these as a defect)",
                unfinished.len()
            )
            .expect("string write");
            for shape in unfinished {
                writeln!(out, "  {shape}").expect("string write");
            }
        }

        if !self.degenerate_regions.is_empty() {
            writeln!(
                out,
                "\n-- free-form regions stepped over ({}) --",
                self.degenerate_regions.len()
            )
            .expect("string write");
            for region in &self.degenerate_regions {
                writeln!(out, "  {region}").expect("string write");
            }
        }

        if !self.truncations.is_empty() {
            writeln!(out, "\n-- coverage limits hit --").expect("string write");
            for truncation in &self.truncations {
                writeln!(out, "  {truncation}").expect("string write");
            }
        }

        out
    }
}

/// The accepted set of findings, so the target fails only on something new.
#[derive(Debug, Default)]
pub struct Baseline {
    signatures: BTreeSet<String>,
}

#[derive(Debug, Error)]
pub enum BaselineError {
    #[error("failed to read baseline at {path}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write baseline at {path}")]
    Write {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

impl Baseline {
    /// Load the baseline, treating a missing file as empty so the first run reports everything.
    pub fn load(path: &Path) -> Result<Self, BaselineError> {
        let contents = match std::fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(source) => {
                return Err(BaselineError::Read {
                    path: path.display().to_string(),
                    source,
                });
            }
        };

        Ok(Self {
            signatures: contents
                .lines()
                .map(str::trim_end)
                .filter(|line| !line.is_empty() && !line.starts_with('#'))
                .map(str::to_string)
                .collect(),
        })
    }

    pub fn store(path: &Path, report: &Report) -> Result<(), BaselineError> {
        let mut contents = String::from(
            "# Accepted NSPL completion-walk findings, one signature per line.\n# Format: \
             kind<TAB>label<TAB>context. Regenerate with --update-baseline.\n#\n# Empty is the \
             intended state: every branch completion offers can be completed. A new\n# entry is a \
             defect -- fix it rather than accept it.\n#\n# `just nspl-completion-walk` digs past \
             this budget and reports one more, where a\n# MongoDB conflict target has to name a \
             column the VALUES record maps and still leave\n# another field to update. That \
             constraint relates the values at two positions rather\n# than their shapes, so the \
             grammar cannot express it and no naming scheme the walk\n# can apply satisfies both \
             halves at once.\n",
        );
        for signature in report.signatures() {
            contents.push_str(&signature);
            contents.push('\n');
        }
        std::fs::write(path, contents).map_err(|source| BaselineError::Write {
            path: path.display().to_string(),
            source,
        })
    }

    /// Findings whose signature is not accepted yet.
    pub fn new_findings<'report>(&self, report: &'report Report) -> Vec<&'report Finding> {
        report
            .unique_findings()
            .into_iter()
            .filter(|finding| !self.signatures.contains(&finding.signature()))
            .collect()
    }

    /// Accepted signatures the walk no longer produces, which usually means they were fixed.
    pub fn stale(&self, report: &Report) -> Vec<&str> {
        let produced = report.signatures();
        self.signatures
            .iter()
            .filter(|signature| !produced.contains(*signature))
            .map(String::as_str)
            .collect()
    }
}
