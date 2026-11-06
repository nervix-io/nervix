//! Repository test harness, outside the product layer order.
//! Owns: complete round-trip oracles for the runner's current serialized representations.
//! Depends on: the production wire/evidence types and bounded Bolero input.
//! Must not know: real compiler processes, host state or historical representations.

use std::{collections::BTreeMap, fmt::Debug, path::PathBuf};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_lint_report::{
    Acquisition, CompilerReport, Context, Expansion, Finding, SiteId, SourceSpan, aggregate,
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    admission::{AdmissionOutcome, AdmissionRecord, Compiled, GIB, Lease, Policy},
    configuration::Configuration,
    evidence::{Compilation, Completion, Ending, ExpectedTarget, MatrixReport, Measurement},
    identity::Identity,
};

struct Case {
    seed: u32,
    wide: u64,
    optional: bool,
    text: String,
}

impl Case {
    fn from_bytes(bytes: &[u8; 32]) -> Self {
        let seed = u32::from_le_bytes(
            bytes[..4]
                .try_into()
                .assured("the prefix contains four bytes"),
        );
        Self {
            seed,
            wide: u64::from_le_bytes(
                bytes[8..16]
                    .try_into()
                    .assured("the wide coordinate contains eight bytes"),
            ),
            optional: bytes[4].is_multiple_of(2),
            text: format!("owner-{}-λ-\"\\", bytes[5]),
        }
    }

    fn admission(&self) -> AdmissionRecord {
        let outcome = match self.seed % 5 {
            0 => AdmissionOutcome::Fit,
            1 => AdmissionOutcome::Alone,
            2 => AdmissionOutcome::WaitLimit,
            3 => AdmissionOutcome::QueueLocked,
            _ => AdmissionOutcome::Unavailable,
        };
        AdmissionRecord {
            reservation_bytes: self.wide,
            outcome,
            waited_seconds: f64::from(self.seed % 1000) / 10.0,
            left_bytes: if self.optional {
                Some(-i128::from(self.wide))
            } else {
                None
            },
            holders: usize::from(self.optional),
            error: if outcome == AdmissionOutcome::Unavailable {
                Some(self.text.clone())
            } else {
                None
            },
        }
    }

    fn lease(&self) -> Lease {
        Lease {
            identifier: self.text.clone(),
            pid: self.seed,
            crate_name: "example".into(),
            kind: "lib".into(),
            configuration: self.text.clone(),
            root: format!("/work/{}", self.text),
            reservation_bytes: self.wide,
            cgroup: if self.optional {
                Some(format!("/scope/{}", self.text))
            } else {
                None
            },
            enqueued_ns: i128::from(self.seed),
            polled_ns: i128::from(self.seed) + 1,
            admitted_ns: if self.optional {
                Some(i128::from(self.seed) + 2)
            } else {
                None
            },
            driver_pid: if self.optional { Some(self.seed) } else { None },
        }
    }

    fn policy(&self) -> Policy {
        Policy {
            directory: format!("/tmp/{}", self.text).into(),
            threshold_bytes: 4 * GIB,
            wait_limit_seconds: f64::from(self.seed % 1800) + 1.0,
            reservations: BTreeMap::from([(
                Compiled {
                    crate_name: self.text.clone(),
                    kind: "lib".into(),
                },
                17 * GIB + u64::from(self.seed),
            )]),
        }
    }

    fn compilation(&self) -> Compilation {
        Compilation {
            crate_name: if self.optional {
                Some(self.text.clone())
            } else {
                None
            },
            crate_types: vec!["lib".into(), "rlib".into()],
            extra_filename: format!("-{:x}", self.seed),
            source: if self.optional {
                Some(PathBuf::from(format!("src/{}.rs", self.text)))
            } else {
                None
            },
            ending: if self.optional {
                Ending::Exit {
                    status: i32::try_from(self.seed % 128).assured("the status is below 128"),
                }
            } else {
                Ending::Signal {
                    signal: 9,
                    name: Some("SIGKILL".into()),
                }
            },
            peak_resident_bytes: self.wide,
            started_unix_ns: i128::from(self.wide),
            seconds: f64::from(self.seed % 1000) / 10.0,
            oom_kills: if self.optional {
                Some(i128::from(self.seed))
            } else {
                None
            },
            admission: if self.optional {
                Some(self.admission())
            } else {
                None
            },
        }
    }

    fn identity(&self) -> Identity {
        Identity {
            mode: if self.optional { "gate" } else { "inventory" }.into(),
            root: format!("/work/{}", self.text).into(),
            compiler: self.text.clone(),
            driver: self.text.clone(),
            workspace_wrapper: self.text.clone(),
            inputs: BTreeMap::from([(format!("src/{}.rs", self.text), self.seed.to_string())]),
            cargo_configuration: BTreeMap::from([(
                PathBuf::from("/work/.cargo/config.toml"),
                self.text.clone(),
            )]),
            environment: BTreeMap::from([("RUSTFLAGS".into(), self.text.clone())]),
        }
    }

    fn completion(&self) -> Completion {
        let configuration = Configuration {
            name: self.text.clone(),
            manifest: "Cargo.toml".into(),
            packages: vec!["example".into()],
            kinds: vec!["lib".into(), "bin".into()],
            features: vec!["testing".into()],
            target: if self.optional {
                Some("wasm32-unknown-unknown".into())
            } else {
                None
            },
        };
        let report = CompilerReport {
            compiler: self.text.clone(),
            identity: self.text.clone(),
            configuration: configuration.name.clone(),
            crate_name: "example".into(),
            crate_source: "src/lib.rs".into(),
            arguments: vec![self.text.clone()],
            findings: vec![Finding {
                rule: "nervix::sync_acquisition".into(),
                receiver: self.text.clone(),
                receiver_type: self.text.clone(),
                receiver_expression: self.text.clone(),
                operation: "get".into(),
                definition: self.text.clone(),
                owner: self.text.clone(),
                acquisition: Acquisition::Shared,
                context: if self.optional {
                    Some(Context::Bounded {
                        reason: self.text.clone(),
                        key: self.text.clone(),
                        bound: self.text.clone(),
                    })
                } else {
                    None
                },
                span: SourceSpan {
                    site: SiteId {
                        path: format!("src/{}.rs", self.text),
                        start: self.seed,
                    },
                    end: self.seed,
                    line: self.seed,
                    column: self.seed,
                    end_line: self.seed,
                    end_column: self.seed,
                    source: self.text.clone(),
                },
                expansion: vec![Expansion {
                    macro_name: self.text.clone(),
                    call_site: self.text.clone(),
                    definition: if self.optional {
                        Some(self.text.clone())
                    } else {
                        None
                    },
                }],
            }],
            audit: Vec::new(),
            excluded_generated: self.seed,
            excluded_external: self.seed,
            complete: true,
        };
        Completion {
            configuration,
            expected: BTreeMap::from([(
                "example::example::lib".into(),
                ExpectedTarget {
                    package: "example".into(),
                    source: "src/lib.rs".into(),
                    kind: "lib".into(),
                    required: self.optional,
                },
            )]),
            reports: BTreeMap::from([("example::example::lib".into(), report)]),
            files: BTreeMap::from([(PathBuf::from("/work/libexample-a.rmeta"), self.text.clone())]),
            complete: true,
            build_directory: "/work/target/build".into(),
            report_directory: "/work/target/reports".into(),
            cache_namespace: self.text.clone(),
            cargo_log: "/work/target/cargo.jsonl".into(),
            compilations: BTreeMap::from([(
                "example::example::lib".into(),
                Measurement {
                    crate_name: "example".into(),
                    kind: "lib".into(),
                    peak_resident_bytes: u64::from(self.seed),
                    started_unix_ns: i128::from(self.seed),
                    seconds: f64::from(self.seed % 1000) / 10.0,
                    admission: if self.optional {
                        Some(self.admission())
                    } else {
                        None
                    },
                },
            )]),
        }
    }

    fn matrix(&self) -> MatrixReport {
        let completion = self.completion();
        let reports: Vec<_> = completion.reports.values().cloned().collect();
        MatrixReport {
            identity: self.text.clone(),
            source_identity: self.text.clone(),
            compiler: self.text.clone(),
            root: "/work".into(),
            configurations: vec![completion.configuration.clone()],
            findings: aggregate(&reports).assured("the generated current report is complete"),
            evidence: vec![completion],
            complete: true,
            matrix_complete: self.optional,
            diagnostics_checked: self.optional,
        }
    }
}

fn round_trip<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: T) {
    let bytes = serde_json::to_vec(&value).assured("a bounded current representation serializes");
    let decoded: T = serde_json::from_slice(&bytes).assured("the current representation decodes");
    assert_eq!(decoded, value);
}

#[test]
fn bolero_lease_round_trip() {
    bolero::check!()
        .with_type::<[u8; 32]>()
        .for_each(|bytes| round_trip(Case::from_bytes(bytes).lease()));
}

#[test]
fn bolero_policy_round_trip() {
    bolero::check!()
        .with_type::<[u8; 32]>()
        .for_each(|bytes| round_trip(Case::from_bytes(bytes).policy()));
}

#[test]
fn bolero_compilation_round_trip() {
    bolero::check!()
        .with_type::<[u8; 32]>()
        .for_each(|bytes| round_trip(Case::from_bytes(bytes).compilation()));
}

#[test]
fn bolero_completion_round_trip() {
    bolero::check!()
        .with_type::<[u8; 32]>()
        .for_each(|bytes| round_trip(Case::from_bytes(bytes).completion()));
}

#[test]
fn bolero_identity_round_trip() {
    bolero::check!()
        .with_type::<[u8; 32]>()
        .for_each(|bytes| round_trip(Case::from_bytes(bytes).identity()));
}

#[test]
fn bolero_matrix_round_trip() {
    bolero::check!()
        .with_type::<[u8; 32]>()
        .for_each(|bytes| round_trip(Case::from_bytes(bytes).matrix()));
}

#[test]
fn every_nullable_lease_field_is_required_by_the_current_external_contract() {
    let value = serde_json::to_value(Case::from_bytes(&[0; 32]).lease())
        .assured("the current lease serializes");
    for field in ["cgroup", "admitted_ns", "driver_pid"] {
        let mut incomplete = value.clone();
        incomplete
            .as_object_mut()
            .assured("the lease is an object")
            .remove(field);
        assert!(
            serde_json::from_value::<Lease>(incomplete).is_err(),
            "a missing {field} is not an explicit null"
        );
    }
}
