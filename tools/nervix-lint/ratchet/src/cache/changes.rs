//! Repository qualification tooling, outside the product layer order.
//! Owns: changed source, annotation, location, rule, dependency and mode probes.
//! Depends on: current fixture sources and production analysis evidence.
//! Must not know: product runtime behavior or cache implementation details.

use std::{
    fs,
    path::{Path, PathBuf},
};

use error_stack::ResultExt as _;
use nervix_lint_report::Finding;
use serde::Serialize;

use super::Problem;
use crate::{
    configuration::Configuration,
    error::{Error, Issue, Result},
    evidence::Completion,
    process,
    runner::Runner,
};

#[derive(Serialize)]
pub struct Changes {
    changed_source: Completion,
    changed_annotation_rejected: bool,
    bounded_annotation: Completion,
    moved_and_renamed_source: Completion,
    changed_rule: Completion,
    changed_dependency: Completion,
    changed_configuration: Completion,
}

fn findings(evidence: &Completion) -> Vec<&Finding> {
    evidence
        .reports
        .values()
        .flat_map(|report| &report.findings)
        .collect()
}

fn read(path: &Path) -> Result<String> {
    fs::read_to_string(path).change_context(Error::Qualification)
}

fn write(path: &Path, source: &str) -> Result<()> {
    fs::write(path, source).change_context(Error::Qualification)
}

pub fn qualify(root: &Path, target: &Path) -> Result<Changes> {
    let probe = Probe::new(root, target)?;
    let added = probe.source()?;
    probe.reject_recurring()?;
    let annotated = probe.annotated()?;
    let moved = probe.moved()?;
    let rule_changed = probe.rule(&moved)?;
    let dependency_changed = probe.dependency(&rule_changed)?;
    let configured = probe.configuration()?;
    Ok(Changes {
        changed_source: added,
        changed_annotation_rejected: true,
        bounded_annotation: annotated,
        moved_and_renamed_source: moved,
        changed_rule: rule_changed,
        changed_dependency: dependency_changed,
        changed_configuration: configured,
    })
}

struct Probe<'a> {
    root: &'a Path,
    target: &'a Path,
    source: PathBuf,
    baseline: String,
    ordinary: Configuration,
}

impl<'a> Probe<'a> {
    const OPERATION: &str = "pub fn current_effect(map: &MapAlias) { drop(map.get(&1)); }\n";
    const BOUNDED: &str = r#"#[cfg_attr(nervix_lint, nervix::context(bounded, reason = "one retained protocol", key = "this map", bound = "one key lookup"))]
"#;

    fn new(root: &'a Path, target: &'a Path) -> Result<Self> {
        let source = root.join("tools/nervix-lint/fixtures/src/lib.rs");
        let baseline = read(&source)?;
        Ok(Self {
            root,
            target,
            source,
            baseline,
            ordinary: Configuration::fixture("ordinary"),
        })
    }

    fn analyze(&self, configuration: &Configuration) -> Result<Completion> {
        let runner = Runner::new(self.root, self.target, false)?;
        runner.analyze_one(configuration, false)
    }

    fn source(&self) -> Result<Completion> {
        let previous = Runner::new(self.root, self.target, false)?;
        let original = previous.analyze_one(&self.ordinary, false)?;
        write(
            &self.source,
            &format!("{}\n{}", self.baseline, Self::OPERATION),
        )?;
        let changed = Runner::new(self.root, self.target, false)?;
        let added = changed.analyze_one(&self.ordinary, false)?;
        Problem::ChangedInput.require(changed.identity != previous.identity)?;
        Problem::ChangedInput
            .require(findings(&added).len().checked_sub(1) == Some(findings(&original).len()))?;
        Ok(added)
    }

    fn reject_recurring(&self) -> Result<()> {
        let recurring = "#[cfg_attr(nervix_lint, nervix::context(recurring, reason = \"handles \
                         each batch\"))]\n";
        write(
            &self.source,
            &format!("{}\n{recurring}{}", self.baseline, Self::OPERATION),
        )?;
        let rejected = Runner::new(self.root, self.target, false)?;
        let result = rejected.analyze_one(&self.ordinary, false);
        let failed = matches!(result, Err(ref error) if matches!(error.current_context(), Error::Evidence { issue: Issue::Compilation, .. }));
        Problem::ChangedInput.require(failed)?;
        let directory = rejected.work.join(&self.ordinary.name);
        Problem::ChangedInput.require(
            read(&directory.join("cargo.jsonl"))?.contains("nervix::sync_acquisition")
                && !directory.join("completion.json").exists(),
        )
    }

    fn annotated(&self) -> Result<Completion> {
        write(
            &self.source,
            &format!("{}\n{}{}", self.baseline, Self::BOUNDED, Self::OPERATION),
        )?;
        let annotated = self.analyze(&self.ordinary)?;
        Problem::ChangedInput.require(findings(&annotated).iter().any(|finding| {
            matches!(
                finding.context,
                Some(nervix_lint_report::Context::Bounded { .. })
            )
        }))?;
        Ok(annotated)
    }

    fn moved(&self) -> Result<Completion> {
        let moved_source = self.source.with_file_name("current_effect.rs");
        let renamed = Self::OPERATION.replace("current_effect", "renamed_effect");
        write(
            &moved_source,
            &format!("use super::MapAlias;\n{}{renamed}", Self::BOUNDED),
        )?;
        write(
            &self.source,
            &format!("{}\nmod current_effect;\n", self.baseline),
        )?;
        let moved = self.analyze(&self.ordinary)?;
        Problem::ChangedInput.require(
            findings(&moved)
                .iter()
                .any(|finding| finding.span.site.path.ends_with("/current_effect.rs")),
        )?;
        Ok(moved)
    }

    fn rule(&self, moved: &Completion) -> Result<Completion> {
        let rule = self.root.join("tools/nervix-lint/report/src/rules.rs");
        write(
            &rule,
            &format!(
                "{}\n// Cache qualification revisits the current rule source.\n",
                read(&rule)?
            ),
        )?;
        let runner = Runner::new(self.root, self.target, false)?;
        process::output(
            "just",
            &["typed-ratchet-build".into()],
            self.root,
            &runner.environment,
        )?;
        let rule_changed = self.analyze(&self.ordinary)?;
        Problem::ChangedInput.require(rule_changed.cache_namespace != moved.cache_namespace)?;
        Ok(rule_changed)
    }

    fn dependency(&self, rule_changed: &Completion) -> Result<Completion> {
        let dependency = self
            .root
            .join("tools/nervix-lint/fixture-macros/src/lib.rs");
        write(
            &dependency,
            &format!(
                "{}\n// Changed authored macro dependency.\n",
                read(&dependency)?
            ),
        )?;
        let dependency_changed = self.analyze(&self.ordinary)?;
        Problem::ChangedInput
            .require(dependency_changed.cache_namespace != rule_changed.cache_namespace)?;
        Ok(dependency_changed)
    }

    fn configuration(&self) -> Result<Completion> {
        let shuttle = Configuration::fixture("shuttle");
        let configured = self.analyze(&shuttle)?;
        Problem::ChangedInput.require(
            configured.configuration.features == ["shuttle"]
                && findings(&configured)
                    .iter()
                    .any(|finding| finding.receiver.contains("shuttle")),
        )?;
        Ok(configured)
    }
}
