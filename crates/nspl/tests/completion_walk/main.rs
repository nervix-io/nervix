//! Walks the NSPL completion graph and reports every branch a user cannot complete.
//!
//! Completion is derived from parse expectations, so the question this answers is: if someone only
//! ever accepts suggestions the system itself offers, can they always reach a statement that
//! parses? Starting from the probed top-level keywords, every suggestion at every reachable state
//! is applied and re-checked, and each broken branch is reported with the text that reproduces it.
//!
//! Run the default bounded walk, which gates against the checked-in baseline:
//!     cargo test -p nervix-nspl --test completion_walk
//! Dig deeper without gating, to look for branches the default budget does not reach:
//!     just nspl-completion-walk

mod grammar;
mod report;
mod suggestion;
mod walker;

use std::{
    io::Write as _,
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::Parser as _;

use crate::{
    grammar::Grammar,
    report::{Baseline, Report},
    walker::{Budget, Walker},
};

const BASELINE_PATH: &str = "tests/completion_walk/baseline.txt";

#[derive(Debug, clap::Parser)]
#[command(
    name = "completion_walk",
    about = "Walk the NSPL completion graph and report branches that cannot be completed"
)]
struct Cli {
    /// Grammar whose completion surface is walked.
    #[arg(long, value_enum, default_value = "client")]
    grammar: Grammar,

    /// Start from this statement prefix instead of the empty input.
    #[arg(long, default_value = "")]
    seed: String,

    /// Maximum number of suggestions applied along one path.
    #[arg(long, default_value_t = 40)]
    max_depth: usize,

    /// Maximum number of times one label may repeat along a path.
    #[arg(long, default_value_t = 2)]
    max_label_repeats: usize,

    /// Stop after evaluating this many states.
    #[arg(long, default_value_t = 200_000)]
    max_states: usize,

    /// Maximum width of one breadth-first level.
    #[arg(long, default_value_t = 50_000)]
    max_frontier: usize,

    /// How many recent labels identify a grammar position for deduplication. Raising this
    /// deduplicates less and so covers more, at a steep cost in states.
    #[arg(long, default_value_t = 4)]
    signature_window: usize,

    /// Worker threads used to evaluate a level.
    #[arg(long)]
    jobs: Option<usize>,

    /// Print the report and succeed regardless of what was found.
    #[arg(long)]
    report_only: bool,

    /// Rewrite the baseline from this run instead of checking against it.
    #[arg(long)]
    update_baseline: bool,

    /// Also write the report as JSON here.
    #[arg(long)]
    json: Option<PathBuf>,

    /// Print what completion offers after this input and how it parses, then exit without walking.
    /// This is how a reported finding gets triaged: it shows the state the walk was standing in.
    #[arg(long)]
    inspect: Option<String>,
}

impl Cli {
    fn budget(&self) -> Budget {
        Budget {
            max_depth: self.max_depth,
            max_label_repeats: self.max_label_repeats,
            max_states: self.max_states,
            max_frontier: self.max_frontier,
            signature_window: self.signature_window,
        }
    }

    fn jobs(&self) -> usize {
        self.jobs.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1)
        })
    }

    /// The baseline lives beside the walker, which cargo runs from the package root.
    fn baseline_path(&self) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join(BASELINE_PATH)
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if let Some(input) = &cli.inspect {
        let suggestions = cli.grammar.suggest(input);
        println!("input:  {input}");
        println!("parse:  {:?}", cli.grammar.parse(input));
        println!("offers: {}", suggestions.len());
        for suggestion in suggestions {
            println!("  {suggestion}");
        }
        let diagnostics = cli.grammar.diagnostics(input);
        println!("diagnostics: {}", diagnostics.len());
        for diagnostic in diagnostics {
            println!(
                "  {}..{}  {}",
                diagnostic.span.start, diagnostic.span.end, diagnostic.message
            );
        }
        return ExitCode::SUCCESS;
    }

    let report = Walker::new(cli.grammar, cli.budget(), cli.jobs()).run(&cli.seed);

    print!("{}", report.render());

    if let Some(path) = &cli.json
        && let Err(error) = write_json(path, &report)
    {
        eprintln!("failed to write {}: {error}", path.display());
        return ExitCode::FAILURE;
    }

    let baseline_path = cli.baseline_path();

    if cli.update_baseline {
        return match Baseline::store(&baseline_path, &report) {
            Ok(()) => {
                println!("\nbaseline rewritten at {}", baseline_path.display());
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        };
    }

    let baseline = match Baseline::load(&baseline_path) {
        Ok(baseline) => baseline,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };

    let stale = baseline.stale(&report);
    if !stale.is_empty() {
        println!(
            "\n{} baseline entr{} no longer reproduced (rerun with --update-baseline to drop \
             them):",
            stale.len(),
            if stale.len() == 1 { "y" } else { "ies" }
        );
        for signature in stale {
            println!("  {}", signature.replace('\t', "  "));
        }
    }

    let new_findings = baseline.new_findings(&report);
    if new_findings.is_empty() {
        println!("\nno completion findings outside the baseline");
        return ExitCode::SUCCESS;
    }

    println!("\n{} finding(s) not in the baseline:", new_findings.len());
    for finding in &new_findings {
        println!("  {}", finding.signature().replace('\t', "  "));
        println!("    {}", finding.statement);
    }

    if cli.report_only {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn write_json(path: &Path, report: &Report) -> std::io::Result<()> {
    let encoded = serde_json::to_vec_pretty(report)?;
    let mut file = std::fs::File::create(path)?;
    file.write_all(&encoded)?;
    file.write_all(b"\n")
}
