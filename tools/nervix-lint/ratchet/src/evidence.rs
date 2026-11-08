//! Repository tooling, outside the product layer order.
//! Owns: typed Cargo observations, compilation records and complete analysis evidence.
//! Depends on: report types, current matrix declarations and admission records.
//! Must not know: process scheduling or diagnostic policy.

use std::{collections::BTreeMap, path::PathBuf};

use nervix_lint_report::{AuthoredSite, CompilerReport};
use serde::{Deserialize, Serialize};

use crate::{admission::AdmissionRecord, configuration::Configuration};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Ending {
    Exit {
        status: i32,
    },
    Signal {
        signal: i32,
        #[serde(deserialize_with = "Option::deserialize")]
        name: Option<String>,
    },
}

impl Ending {
    pub fn succeeded(&self) -> bool {
        matches!(self, Self::Exit { status: 0 })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Compilation {
    #[serde(deserialize_with = "Option::deserialize")]
    pub crate_name: Option<String>,
    pub crate_types: Vec<String>,
    pub extra_filename: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub source: Option<PathBuf>,
    pub ending: Ending,
    pub peak_resident_bytes: u64,
    pub started_unix_ns: i128,
    pub seconds: f64,
    #[serde(deserialize_with = "Option::deserialize")]
    pub oom_kills: Option<i128>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub admission: Option<AdmissionRecord>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Measurement {
    pub crate_name: String,
    pub kind: String,
    pub peak_resident_bytes: u64,
    pub started_unix_ns: i128,
    pub seconds: f64,
    #[serde(deserialize_with = "Option::deserialize")]
    pub admission: Option<AdmissionRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedTarget {
    pub package: String,
    pub source: PathBuf,
    pub kind: String,
    pub required: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub configuration: Configuration,
    pub expected: BTreeMap<String, ExpectedTarget>,
    pub reports: BTreeMap<String, CompilerReport>,
    pub files: BTreeMap<PathBuf, String>,
    pub complete: bool,
    pub build_directory: PathBuf,
    pub report_directory: PathBuf,
    pub cache_namespace: String,
    pub cargo_log: PathBuf,
    pub compilations: BTreeMap<String, Measurement>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MatrixReport<E = Completion> {
    pub identity: String,
    pub source_identity: String,
    pub compiler: String,
    pub root: PathBuf,
    pub configurations: Vec<Configuration>,
    // A reader that uses only aggregate findings can skip completion payloads without defining
    // another matrix schema or allocating the compiler reports it never inspects.
    pub evidence: Vec<E>,
    pub findings: Vec<AuthoredSite>,
    pub complete: bool,
    pub matrix_complete: bool,
    pub diagnostics_checked: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CargoTarget {
    pub name: String,
    pub kind: Vec<String>,
    pub src_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CargoArtifact {
    pub package_id: String,
    pub target: CargoTarget,
    pub filenames: Vec<PathBuf>,
    pub fresh: bool,
}

impl CargoArtifact {
    pub fn key(&self) -> String {
        format!(
            "{}::{}::{}",
            self.package_id,
            self.target.name,
            crate::admission::target_kind(&self.target.kind)
        )
    }
    pub fn crate_name(&self) -> String {
        self.target.name.replace('-', "_")
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct DiagnosticCode {
    pub code: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Diagnostic {
    pub level: String,
    pub code: Option<DiagnosticCode>,
    pub rendered: Option<String>,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "reason", rename_all = "kebab-case")]
pub enum CargoMessage {
    CompilerArtifact(CargoArtifact),
    CompilerMessage {
        target: CargoTarget,
        message: Diagnostic,
    },
    BuildFinished {
        success: bool,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
pub struct CargoPackage {
    pub id: String,
    pub name: String,
    pub targets: Vec<CargoTarget>,
}

#[derive(Deserialize)]
pub struct Metadata {
    pub packages: Vec<CargoPackage>,
    pub workspace_members: Vec<String>,
}
