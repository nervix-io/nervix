//! Repository tooling, outside the product layer order.
//! Owns: comparison of declared diagnostic lock owners with typed compiler evidence.
//! Depends on: the matrix/report vocabulary and the lane's owner declarations.
//! Must not know: workload supervision, runtime locks or diagnostic execution.

use std::{collections::BTreeSet, fs, path::PathBuf};

use error_stack::{Report, ResultExt as _};
use serde::Deserialize;

use crate::{
    error::{Error, Issue, Result},
    evidence::MatrixReport,
    io,
};

const INVENTORY: &str = "tests/deloxide-inventory.toml";

#[derive(clap::Args)]
pub struct Options {
    #[arg(long)]
    pub root: PathBuf,
    #[arg(long)]
    pub catalog: PathBuf,
}

// The Python lane validates the complete workload declaration before invoking this command.
// Only its owner projection participates in the compiler-evidence comparison.
#[derive(Deserialize)]
struct Inventory {
    owner: Vec<Owner>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Owner {
    path: String,
    #[serde(default)]
    workloads: Vec<String>,
    gap: Option<String>,
}

impl MatrixReport {
    fn diagnostic_owners(&self, catalog: &std::path::Path) -> Result<BTreeSet<String>> {
        let mut diagnostic = BTreeSet::new();
        for configuration in &self.configurations {
            if configuration.features.iter().any(|feature| {
                matches!(
                    feature.as_str(),
                    "deloxide" | "deloxide-order" | "deloxide-stress"
                )
            }) {
                diagnostic.insert(configuration.name.as_str());
            }
        }
        if diagnostic.is_empty() {
            return Err(Report::new(Error::Evidence {
                configuration: "deloxide applicability".into(),
                issue: Issue::DiagnosticConfiguration,
                path: catalog.into(),
            }));
        }
        let mut owners = BTreeSet::new();
        for site in &self.findings {
            for (configuration, findings) in &site.configurations {
                let Some((name, _)) = configuration.split_once(':') else {
                    continue;
                };
                if !diagnostic.contains(name) {
                    continue;
                }
                for finding in findings {
                    if !matches!(
                        finding.receiver.as_str(),
                        "nervix_primitives::sync::blocking::tracked::Mutex"
                            | "nervix_primitives::sync::blocking::tracked::RwLock"
                    ) {
                        continue;
                    }
                    if finding.span.site.path.is_empty() {
                        return Err(Report::new(Error::Evidence {
                            configuration: name.into(),
                            issue: Issue::TrackedOwnerPath,
                            path: catalog.into(),
                        }));
                    }
                    owners.insert(finding.span.site.path.clone());
                }
            }
        }
        Ok(owners)
    }
}

impl Options {
    pub fn run(&self) -> Result<i32> {
        let path = self.root.join(INVENTORY);
        let text =
            fs::read_to_string(&path).change_context(Error::Declaration { path: path.clone() })?;
        let inventory: Inventory =
            toml::from_str(&text).change_context(Error::Declaration { path: path.clone() })?;
        let report: MatrixReport = io::read(&self.catalog)?;
        let owners = report.diagnostic_owners(&self.catalog)?;
        let declared: BTreeSet<_> = inventory
            .owner
            .iter()
            .map(|owner| owner.path.clone())
            .collect();
        let mut missing = false;
        for path in owners.difference(&declared) {
            missing = true;
            eprintln!(
                "deloxide applicability: {path} acquires tracked blocking locks in a diagnostic \
                 build and has no [[owner]] record in {INVENTORY}: name the workloads that reach \
                 its locks, or the path the lane does not reach and why"
            );
        }
        for path in declared.difference(&owners) {
            missing = true;
            eprintln!(
                "deloxide applicability: {INVENTORY} records the owner {path}, which acquires no \
                 tracked lock"
            );
        }
        if missing {
            return Ok(1);
        }
        let reached = inventory
            .owner
            .iter()
            .filter(|owner| !owner.workloads.is_empty())
            .count();
        let gaps = inventory
            .owner
            .iter()
            .filter(|owner| owner.gap.is_some())
            .count();
        println!(
            "deloxide applicability: {} owners of tracked locks, {reached} reached by registered \
             workloads, {gaps} with a recorded gap",
            declared.len()
        );
        Ok(0)
    }
}
