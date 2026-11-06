//! Repository tooling, outside the product layer order.
//! Owns: bounded concurrent Cargo children and declared-order matrix evidence.
//! Depends on: one-configuration analysis and typed report aggregation.
//! Must not know: product graph execution or compiler internals.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    path::PathBuf,
    process::{Child, Command},
    time::Duration,
};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_primitives::thread::sleep;

use crate::{
    analysis::{Analysis, Preparation},
    configuration::{Configuration, Declaration},
    error::{Error, Issue, Result},
    evidence::{Completion, MatrixReport},
    io,
    runner::Runner,
};

#[derive(Clone, Debug, clap::Args)]
pub struct Analyze {
    #[arg(long)]
    pub root: Option<PathBuf>,
    #[arg(long)]
    pub target_dir: Option<PathBuf>,
    #[arg(long)]
    pub configuration: Vec<String>,
    #[arg(long, hide = true)]
    pub turmoil_child: bool,
    #[arg(long, value_parser = ["ordinary", "shuttle", "loom", "turmoil", "deloxide"])]
    pub fixture_mode: Option<String>,
    #[arg(long)]
    pub fresh: bool,
    #[arg(long)]
    pub recompile: bool,
    #[arg(long)]
    pub inventory: bool,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long)]
    pub show: bool,
    #[arg(long)]
    pub jobs: Option<NonZeroUsize>,
}

impl Analyze {
    pub fn root(&self) -> PathBuf {
        match &self.root {
            Some(root) => root.clone(),
            None => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .ancestors()
                .nth(3)
                .assured("the runner crate is tools/nervix-lint/ratchet")
                .to_owned(),
        }
    }

    pub fn target(&self) -> PathBuf {
        match &self.target_dir {
            Some(target) => target.clone(),
            None => match std::env::var_os("CARGO_TARGET_DIR") {
                Some(target) => target.into(),
                None => self.root().join("target"),
            },
        }
    }

    pub fn jobs(&self) -> Result<usize> {
        let value = std::env::var("NERVIX_TYPED_RATCHET_JOBS");
        self.jobs_from(value)
    }

    fn jobs_from(&self, value: std::result::Result<String, std::env::VarError>) -> Result<usize> {
        if let Some(jobs) = self.jobs {
            return Ok(jobs.get());
        }
        match value {
            Ok(value) if !value.is_empty() => value
                .parse::<NonZeroUsize>()
                .map(NonZeroUsize::get)
                .change_context(Error::Options { issue: Issue::Jobs }),
            Ok(_) | Err(std::env::VarError::NotPresent) => Ok(4),
            Err(error) => {
                Err(Report::new(error).change_context(Error::Options { issue: Issue::Jobs }))
            }
        }
    }

    fn configurations(&self, runner: &Runner) -> Result<Vec<Configuration>> {
        let configurations = if let Some(mode) = &self.fixture_mode {
            vec![Configuration::fixture(mode)]
        } else {
            Declaration::load(&runner.root)?.configurations
        };
        let configurations = if !self.configuration.is_empty() && self.fixture_mode.is_none() {
            if !self.inventory && !self.turmoil_child {
                return Err(Report::new(Error::Options {
                    issue: Issue::PartialGate,
                }));
            }
            let requested: BTreeSet<_> = self.configuration.iter().collect();
            let selected: Vec<_> = configurations
                .into_iter()
                .filter(|configuration| requested.contains(&configuration.name))
                .collect();
            let names: BTreeSet<_> = selected
                .iter()
                .map(|configuration| &configuration.name)
                .collect();
            if names != requested {
                return Err(Report::new(Error::Options {
                    issue: Issue::Configuration,
                }));
            }
            selected
        } else {
            configurations
        };
        if self.turmoil_child
            && (self.fixture_mode.is_some()
                || self.configuration.is_empty()
                || configurations.len() != 1
                || !configurations[0]
                    .features
                    .iter()
                    .any(|feature| feature == "turmoil")
                || !runner
                    .environment
                    .get("RUSTFLAGS")
                    .is_some_and(|flags| flags.contains("--cfg tokio_unstable")))
        {
            return Err(Report::new(Error::Options {
                issue: Issue::TurmoilChild,
            }));
        }
        Ok(configurations)
    }

    pub fn run(&self) -> Result<MatrixReport> {
        let runner = Runner::new(&self.root(), &self.target(), self.inventory)?;
        let configurations = self.configurations(&runner)?;
        let source_identity = io::fingerprint(&runner.inputs)?;
        let evidence = self.collect(&runner, &configurations, &source_identity)?;
        let reports: Vec<_> = evidence
            .iter()
            .flat_map(|entry| entry.reports.values().cloned())
            .collect();
        let findings = nervix_lint_report::aggregate(&reports).change_context(Error::Aggregate)?;
        runner.inputs_current()?;
        let report = MatrixReport {
            identity: runner.identity.clone(),
            source_identity,
            compiler: runner.compiler.clone(),
            root: runner.root.clone(),
            configurations: evidence
                .iter()
                .map(|entry| entry.configuration.clone())
                .collect(),
            evidence,
            findings,
            complete: true,
            matrix_complete: self.fixture_mode.is_none() && self.configuration.is_empty(),
            diagnostics_checked: !self.inventory,
        };
        if self.show {
            for site in &report.findings {
                crate::render::finding(site);
            }
        }
        let destination = match &self.output {
            Some(path) => path.clone(),
            None => runner.work.join("inventory.json"),
        };
        io::atomic_json(&destination, &report)?;
        println!(
            "typed ratchet: {} authored acquisition sites; inventory: {}",
            report.findings.len(),
            destination.display()
        );
        Ok(report)
    }

    fn collect(
        &self,
        runner: &Runner,
        configurations: &[Configuration],
        source_identity: &str,
    ) -> Result<Vec<Completion>> {
        let jobs = self.jobs()?.min(configurations.len());
        schedule(
            configurations,
            jobs,
            |configuration| self.start(runner, configuration),
            |job| job.poll(runner, source_identity, self.inventory),
            || sleep(Duration::from_millis(10)),
        )
    }

    fn start(&self, runner: &Runner, configuration: &Configuration) -> Result<Started<Job>> {
        let turmoil = configuration
            .features
            .iter()
            .any(|feature| feature == "turmoil");
        let unstable = match runner.environment.get("RUSTFLAGS") {
            Some(flags) => flags.contains("tokio_unstable"),
            None => false,
        };
        if turmoil && !unstable {
            return self.start_turmoil(runner, configuration);
        }
        match runner.prepare(configuration, self.fresh || self.recompile, self.recompile)? {
            Preparation::Complete(evidence) => Ok(Started::Complete(evidence)),
            Preparation::Running(analysis) => Ok(Started::Running(Job::Analysis(analysis))),
        }
    }

    fn start_turmoil(
        &self,
        runner: &Runner,
        configuration: &Configuration,
    ) -> Result<Started<Job>> {
        let output = runner.work.join("turmoil.json");
        io::remove_if_present(&output)?;
        let mut args = vec![
            "typed-ratchet-turmoil".into(),
            "--root".into(),
            runner.root.display().to_string(),
            "--target-dir".into(),
            runner.target.display().to_string(),
        ];
        if self.fixture_mode.is_some() {
            args.extend(["--fixture-mode".into(), "turmoil".into()]);
        } else {
            args.extend([
                "--turmoil-child".into(),
                "--configuration".into(),
                configuration.name.clone(),
            ]);
        }
        args.extend(["--output".into(), output.display().to_string()]);
        if self.inventory {
            args.push("--inventory".into());
        }
        if self.fresh {
            args.push("--fresh".into());
        }
        if self.recompile {
            args.push("--recompile".into());
        }
        let child = Command::new("just")
            .args(args)
            .current_dir(&runner.root)
            .spawn()
            .change_context(Error::Io {
                operation: "start Turmoil analysis",
                path: "just typed-ratchet-turmoil".into(),
            })?;
        Ok(Started::Running(Job::Turmoil {
            child,
            configuration: configuration.clone(),
            output,
        }))
    }
}

fn schedule<J>(
    configurations: &[Configuration],
    jobs: usize,
    mut start: impl FnMut(&Configuration) -> Result<Started<J>>,
    mut poll: impl FnMut(&mut J) -> Result<Option<Completion>>,
    mut pause: impl FnMut(),
) -> Result<Vec<Completion>> {
    let mut next = 0;
    let mut active = Vec::<Active<J>>::new();
    let mut complete = BTreeMap::new();
    let mut failure = None;
    while next < configurations.len() || !active.is_empty() {
        if failure.is_none() && active.len() < jobs && next < configurations.len() {
            match start(&configurations[next]) {
                Ok(Started::Complete(evidence)) => {
                    complete.insert(next, evidence);
                }
                Ok(Started::Running(job)) => active.push(Active { index: next, job }),
                Err(error) => failure = Some(error),
            }
            next += 1;
        }
        let mut index = 0;
        while index < active.len() {
            match poll(&mut active[index].job) {
                Ok(Some(evidence)) => {
                    let finished = active.swap_remove(index);
                    complete.insert(finished.index, evidence);
                }
                Ok(None) => index += 1,
                Err(error) => {
                    active.swap_remove(index);
                    if failure.is_none() {
                        failure = Some(error);
                    } else {
                        eprintln!("typed ratchet: {error:?}");
                    }
                }
            }
        }
        if active.is_empty() && failure.is_some() {
            break;
        }
        if !active.is_empty()
            && (failure.is_some() || active.len() >= jobs || next == configurations.len())
        {
            pause();
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(complete.into_values().collect())
}

#[cfg(test)]
mod tests;

struct Active<J> {
    index: usize,
    job: J,
}
#[expect(
    clippy::large_enum_variant,
    reason = "the generic lint assumes a zero-sized J; production jobs own the larger running \
              analysis, and boxing cached completion would add an allocation to reuse"
)]
enum Started<J> {
    Complete(Completion),
    Running(J),
}
enum Job {
    Analysis(Analysis),
    Turmoil {
        child: Child,
        configuration: Configuration,
        output: PathBuf,
    },
}

impl Job {
    fn poll(
        &mut self,
        runner: &Runner,
        source_identity: &str,
        inventory: bool,
    ) -> Result<Option<Completion>> {
        match self {
            Self::Analysis(analysis) => analysis.poll(runner),
            Self::Turmoil {
                child,
                configuration,
                output,
            } => {
                let Some(status) = child.try_wait().change_context(Error::Io {
                    operation: "wait for Turmoil analysis",
                    path: output.clone(),
                })?
                else {
                    return Ok(None);
                };
                if !status.success() {
                    return Err(runner.failure(configuration, Issue::Compilation, output));
                }
                let mut report: MatrixReport = io::read(output)?;
                if !report.complete
                    || report.compiler != runner.compiler
                    || report.root != runner.root
                    || report.diagnostics_checked != !inventory
                    || report.source_identity != source_identity
                    || report.evidence.len() != 1
                    || report.configurations != [configuration.clone()]
                    || report.evidence[0].configuration != *configuration
                {
                    return Err(runner.failure(configuration, Issue::TurmoilEvidence, output));
                }
                Ok(report.evidence.pop())
            }
        }
    }
}
