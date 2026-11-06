//! Repository tooling, outside the product layer order.
//! Owns: the declared compiler matrix and validation of its admission policy.
//! Depends on: typed declarations, filesystem input and current execution-mode names.
//! Must not know: running compilers, evidence reuse or product graph policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use error_stack::{Report, ResultExt as _};
use serde::{Deserialize, Serialize};

use crate::{
    admission::{Compiled, GIB, Policy},
    admission_queue::host_directory,
    error::{Error, Result},
};

pub const TOOLCHAIN: &str = "nightly-2026-09-17";
pub const TOOLING: &str = "tools/nervix-lint";

#[derive(Debug, thiserror::Error)]
pub enum DeclarationProblem {
    #[error("no declared configurations")]
    NoConfigurations,
    #[error("duplicate configuration {name}")]
    DuplicateConfiguration { name: String },
    #[error("unsupported targets in {name}: {kinds:?}")]
    UnsupportedTargets { name: String, kinds: Vec<String> },
    #[error("incompatible modes in {name}: {modes:?}")]
    IncompatibleModes { name: String, modes: Vec<String> },
    #[error("the admission threshold and wait limit are positive whole numbers")]
    AdmissionBounds,
    #[error("an admission reservation names a crate and a lib or bin kind")]
    ReservationIdentity,
    #[error("duplicate admission reservation for {crate_name} ({kind})")]
    DuplicateReservation { crate_name: String, kind: String },
    #[error(
        "the admission reservation of {crate_name} ({kind}) is a whole number of GiB above the \
         {threshold} GiB threshold"
    )]
    ReservationBound {
        crate_name: String,
        kind: String,
        threshold: u64,
    },
    #[error("{field} exceeds its byte or seconds coordinate")]
    Overflow { field: &'static str },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub name: String,
    pub manifest: String,
    pub packages: Vec<String>,
    pub kinds: Vec<String>,
    pub features: Vec<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub target: Option<String>,
}

fn library_kind() -> Vec<String> {
    vec!["lib".into()]
}

impl Configuration {
    pub fn fixture(mode: &str) -> Self {
        let features = if mode == "ordinary" {
            Vec::new()
        } else {
            vec![mode.into()]
        };
        Self {
            name: format!("fixture-{mode}"),
            manifest: format!("{TOOLING}/fixtures/Cargo.toml"),
            packages: Vec::new(),
            kinds: library_kind(),
            features,
            target: None,
        }
    }

    pub fn cargo_arguments(&self) -> Vec<String> {
        let mut args = vec!["--manifest-path".into(), self.manifest.clone()];
        if self.packages.is_empty() {
            args.push("--workspace".into());
        } else {
            for package in &self.packages {
                args.extend(["--package".into(), package.clone()]);
            }
        }
        for kind in &self.kinds {
            args.push(if kind == "bin" { "--bins" } else { "--lib" }.into());
        }
        if !self.features.is_empty() {
            args.extend(["--features".into(), self.features.join(" ")]);
        }
        if let Some(target) = &self.target {
            args.extend(["--target".into(), target.clone()]);
        }
        args
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionDeclaration {
    threshold_gib: u64,
    wait_limit_minutes: u64,
    reservation: Vec<DeclaredReservation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeclaredReservation {
    #[serde(rename = "crate")]
    crate_name: String,
    kind: String,
    gib: u64,
}

pub struct Declaration {
    pub configurations: Vec<Configuration>,
    admission: AdmissionDeclaration,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeclarationFile {
    configuration: Vec<ConfigurationDeclaration>,
    admission: AdmissionDeclaration,
}

/// Optional declaration syntax is normalized once; completed evidence requires every field.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigurationDeclaration {
    name: String,
    manifest: String,
    packages: Vec<String>,
    #[serde(default = "library_kind")]
    kinds: Vec<String>,
    #[serde(default)]
    features: Vec<String>,
    target: Option<String>,
}

impl From<ConfigurationDeclaration> for Configuration {
    fn from(value: ConfigurationDeclaration) -> Self {
        Self {
            name: value.name,
            manifest: value.manifest,
            packages: value.packages,
            kinds: value.kinds,
            features: value.features,
            target: value.target,
        }
    }
}

impl Declaration {
    pub fn load(root: &Path) -> Result<Self> {
        let path = root.join(TOOLING).join("configurations.toml");
        let source =
            fs::read_to_string(&path).change_context(Error::Declaration { path: path.clone() })?;
        let input: DeclarationFile =
            toml::from_str(&source).change_context(Error::Declaration { path: path.clone() })?;
        let declaration = Self {
            configurations: input
                .configuration
                .into_iter()
                .map(Configuration::from)
                .collect(),
            admission: input.admission,
        };
        let mut names = BTreeSet::new();
        if declaration.configurations.is_empty() {
            return Err(Report::new(DeclarationProblem::NoConfigurations)
                .change_context(Error::Declaration { path }));
        }
        for configuration in &declaration.configurations {
            let modes: BTreeSet<_> = configuration
                .features
                .iter()
                .filter(|feature| {
                    matches!(
                        feature.as_str(),
                        "loom" | "shuttle" | "turmoil" | "deloxide"
                    )
                })
                .collect();
            let problem = if !names.insert(&configuration.name) {
                Some(DeclarationProblem::DuplicateConfiguration {
                    name: configuration.name.clone(),
                })
            } else if configuration.kinds.is_empty()
                || configuration
                    .kinds
                    .iter()
                    .any(|kind| !matches!(kind.as_str(), "lib" | "bin"))
            {
                Some(DeclarationProblem::UnsupportedTargets {
                    name: configuration.name.clone(),
                    kinds: configuration.kinds.clone(),
                })
            } else if modes.len() > 1 {
                Some(DeclarationProblem::IncompatibleModes {
                    name: configuration.name.clone(),
                    modes: modes.into_iter().cloned().collect(),
                })
            } else {
                None
            };
            if let Some(problem) = problem {
                return Err(Report::new(problem).change_context(Error::Declaration { path }));
            }
        }
        declaration.policy(root)?;
        Ok(declaration)
    }

    pub fn policy(&self, root: &Path) -> Result<Policy> {
        let path = root.join(TOOLING).join("configurations.toml");
        let invalid = |problem| {
            Report::new(problem).change_context(Error::Declaration { path: path.clone() })
        };
        if self.admission.threshold_gib == 0 || self.admission.wait_limit_minutes == 0 {
            return Err(invalid(DeclarationProblem::AdmissionBounds));
        }
        let threshold_bytes = self
            .admission
            .threshold_gib
            .checked_mul(GIB)
            .ok_or_else(|| {
                invalid(DeclarationProblem::Overflow {
                    field: "admission.threshold_gib",
                })
            })?;
        let wait_seconds = self
            .admission
            .wait_limit_minutes
            .checked_mul(60)
            .ok_or_else(|| {
                invalid(DeclarationProblem::Overflow {
                    field: "admission.wait_limit_minutes",
                })
            })?;
        let mut names = BTreeSet::new();
        let mut reservations = BTreeMap::new();
        for reservation in &self.admission.reservation {
            if reservation.crate_name.is_empty()
                || !matches!(reservation.kind.as_str(), "lib" | "bin")
            {
                return Err(invalid(DeclarationProblem::ReservationIdentity));
            }
            if !names.insert((&reservation.crate_name, &reservation.kind)) {
                return Err(invalid(DeclarationProblem::DuplicateReservation {
                    crate_name: reservation.crate_name.clone(),
                    kind: reservation.kind.clone(),
                }));
            }
            if reservation.gib <= self.admission.threshold_gib {
                return Err(invalid(DeclarationProblem::ReservationBound {
                    crate_name: reservation.crate_name.clone(),
                    kind: reservation.kind.clone(),
                    threshold: self.admission.threshold_gib,
                }));
            }
            let bytes = reservation.gib.checked_mul(GIB).ok_or_else(|| {
                invalid(DeclarationProblem::Overflow {
                    field: "admission.reservation.gib",
                })
            })?;
            reservations.insert(
                Compiled {
                    crate_name: reservation.crate_name.clone(),
                    kind: reservation.kind.clone(),
                },
                bytes,
            );
        }
        use nervix_approx_into::ApproxInto as _;
        Ok(Policy {
            directory: host_directory(),
            threshold_bytes,
            wait_limit_seconds: wait_seconds.approx_into(),
            reservations,
        })
    }
}

#[cfg(test)]
mod tests;
