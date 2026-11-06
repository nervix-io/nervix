//! Repository tooling, outside the product layer order.
//! Owns: compilation failure attribution and operational measurement reporting.
//! Depends on: typed Cargo/compilation evidence and the declared configuration.
//! Must not know: diagnostic policy or process scheduling.

use std::{collections::BTreeMap, fs, path::Path};

use error_stack::ResultExt as _;
use meticulous::ResultExt as _;
use nervix_approx_into::ApproxInto as _;

use crate::{
    admission::{AdmissionOutcome, Compiled, GIB, target_kind},
    configuration::Configuration,
    error::{Error, Result},
    evidence::{CargoMessage, Compilation, Ending, Measurement},
};

pub fn lines(
    configuration: &Configuration,
    compilations: &BTreeMap<String, Measurement>,
) -> Vec<String> {
    let Some(largest) = compilations
        .values()
        .max_by_key(|entry| entry.peak_resident_bytes)
    else {
        return Vec::new();
    };
    let peak: f64 = largest.peak_resident_bytes.approx_into();
    let gib: f64 = GIB.approx_into();
    let mut lines = vec![format!(
        "typed ratchet: {}: the largest analysis compilation, {} ({}), peaked at {:.1} GiB in \
         {:.0} s",
        configuration.name,
        largest.crate_name,
        largest.kind,
        peak / gib,
        largest.seconds
    )];
    let mut ordered: Vec<_> = compilations.values().collect();
    ordered.sort_by_key(|entry| entry.started_unix_ns);
    for measured in ordered {
        let Some(admission) = &measured.admission else {
            continue;
        };
        if admission.outcome == AdmissionOutcome::Fit && admission.waited_seconds < 1.0 {
            continue;
        }
        let reservation: f64 = admission.reservation_bytes.approx_into();
        let peak: f64 = measured.peak_resident_bytes.approx_into();
        let outcome = serde_json::to_value(admission.outcome)
            .assured("an admission outcome is a JSON string enum");
        let serde_json::Value::String(outcome) = outcome else {
            unreachable!("AdmissionOutcome is a string enum");
        };
        lines.push(format!(
            "typed ratchet: {}: {} ({}) waited {:.0} s for host memory admission ({outcome}) of \
             its {:.0} GiB reservation and peaked at {:.1} GiB",
            configuration.name,
            measured.crate_name,
            measured.kind,
            admission.waited_seconds,
            reservation / gib,
            peak / gib
        ));
    }
    lines
}

pub fn print_failures(messages: &[CargoMessage], reports: &Path) -> Result<()> {
    for line in failure_lines(messages, reports)? {
        eprintln!("  {line}");
    }
    Ok(())
}

pub fn failure_lines(messages: &[CargoMessage], reports: &Path) -> Result<Vec<String>> {
    let mut errors = BTreeMap::<Compiled, usize>::new();
    for message in messages {
        if let CargoMessage::CompilerMessage { target, message } = message
            && message.level == "error"
        {
            *errors
                .entry(Compiled::new(target.name.replace('-', "_"), &target.kind))
                .or_default() += 1;
        }
    }
    let records = reports.join("compilations");
    let mut paths = Vec::new();
    if records.is_dir() {
        for item in fs::read_dir(&records).change_context(Error::Io {
            operation: "read compilation failures",
            path: records.clone(),
        })? {
            let path = item
                .change_context(Error::Io {
                    operation: "read compilation failure",
                    path: records.clone(),
                })?
                .path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                paths.push(path);
            }
        }
    }
    paths.sort();
    let mut failures = Vec::new();
    for path in paths {
        let record: Compilation = crate::io::read(&path)?;
        if record.ending.succeeded() {
            continue;
        }
        let kind = target_kind(&record.crate_types);
        let name = record.crate_name.as_deref().unwrap_or("unknown");
        match record.ending {
            Ending::Signal {
                signal,
                name: signal_name,
            } => {
                let peak: f64 = record.peak_resident_bytes.approx_into();
                let gib: f64 = GIB.approx_into();
                let mut text = format!(
                    "{name} ({kind}): the analysis compiler was terminated by signal {signal} \
                     ({}) after {:.0}s at a peak resident size of {:.1} GiB",
                    signal_name.as_deref().unwrap_or("unnamed"),
                    record.seconds,
                    peak / gib
                );
                if let Some(kills) = record.oom_kills
                    && kills != 0
                {
                    text.push_str(&format!(
                        "; the kernel's out-of-memory killer recorded {kills} kill{} in its \
                         cgroup while it ran",
                        if kills == 1 { "" } else { "s" }
                    ));
                }
                text.push_str("; it wrote no compiler report");
                failures.push(text);
            }
            Ending::Exit { status } => {
                let compiled = Compiled {
                    crate_name: name.into(),
                    kind: kind.into(),
                };
                let count = errors.get(&compiled).copied().unwrap_or(0);
                if count > 0 {
                    failures.push(format!(
                        "{name} ({kind}): the analysis compiler failed with {count} compiler \
                         error{} (exit status {status})",
                        if count == 1 { "" } else { "s" }
                    ));
                } else {
                    failures.push(format!(
                        "{name} ({kind}): the analysis compiler exited with status {status} \
                         without a compiler error, so the analysis driver itself failed, for \
                         example while publishing its report; its message is in the compiler \
                         output above"
                    ));
                }
            }
        }
    }
    if failures.is_empty() {
        failures.push(
            "no analysis compilation ended unsuccessfully, so Cargo, a build script or the \
             compiler wrapper failed outside them; their output above names the failure"
                .into(),
        );
    }
    Ok(failures)
}

#[cfg(test)]
mod tests;
