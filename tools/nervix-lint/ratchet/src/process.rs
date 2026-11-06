//! Repository tooling, outside the product layer order.
//! Owns: checked subprocess invocation and Cargo's diagnostic log decoding.
//! Depends on: process I/O, current Cargo observations and typed errors.
//! Must not know: compilation admission or evidence reuse policy.

use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    process::{Command, Output},
};

use error_stack::{Report, ResultExt as _};

use crate::{
    error::{Error, Result},
    evidence::CargoMessage,
};

pub fn output(
    program: &str,
    arguments: &[String],
    root: &Path,
    environment: &BTreeMap<String, String>,
) -> Result<Output> {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .envs(environment)
        .output()
        .change_context(Error::Io {
            operation: "start command",
            path: program.into(),
        })?;
    if !output.status.success() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        eprint!("{}", String::from_utf8_lossy(&output.stdout));
        return Err(Report::new(Error::Process {
            program: program.into(),
            status: output.status.code(),
        }));
    }
    Ok(output)
}

pub fn text(
    program: &str,
    arguments: &[String],
    root: &Path,
    environment: &BTreeMap<String, String>,
) -> Result<String> {
    let output = output(program, arguments, root, environment)?;
    String::from_utf8(output.stdout).change_context(Error::Io {
        operation: "decode command output",
        path: program.into(),
    })
}

#[derive(serde::Deserialize)]
struct RecordClassification {
    reason: Option<serde_json::Value>,
}

pub fn messages(path: &Path) -> Result<Vec<CargoMessage>> {
    let text = fs::read_to_string(path).change_context(Error::Io {
        operation: "read Cargo log",
        path: path.into(),
    })?;
    let mut messages = Vec::new();
    for line in text.lines() {
        // Skip ordinary progress text, then strictly decode each Cargo JSON record.
        if !line.starts_with('{') {
            continue;
        }
        let message = match serde_json::from_str(line) {
            Ok(message) => message,
            Err(cause) => {
                // JSON progress objects keep their syntax check without requiring a Cargo tag.
                let classification: RecordClassification =
                    serde_json::from_str(line).change_context(Error::Json { path: path.into() })?;
                if !matches!(classification.reason, Some(serde_json::Value::String(_))) {
                    continue;
                }
                return Err(Report::new(cause).change_context(Error::Json { path: path.into() }));
            }
        };
        messages.push(message);
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    //! Repository test harness, outside the product layer order.
    //! Owns: Cargo record framing and malformed-record refusals.
    //! Depends on: the production reader and private observation files.
    //! Must not know: a compiler or product graph execution.

    use std::fs;

    use meticulous::ResultExt as _;

    use super::messages;
    use crate::{error::Error, evidence::CargoMessage};

    #[test]
    fn cargo_json_records_fail_closed_while_progress_text_is_ignored() {
        let directory =
            tempfile::tempdir().assured("the Cargo-log fixture has a private directory");
        let path = directory.path().join("cargo.jsonl");
        let valid = concat!(
            "    Checking example v0.1.0\n",
            "{\"progress\":\"building\"}\n",
            "{\"reason\":false,\"progress\":\"building\"}\n",
            "{\"reason\":\"build-finished\",\"success\":true}\n",
        );
        fs::write(&path, valid).assured("the valid interleaved observations can be written");
        let observed =
            messages(&path).assured("valid Cargo records and progress text are accepted");
        assert!(matches!(
            observed.as_slice(),
            [CargoMessage::BuildFinished { success: true }]
        ));
        for record in [
            r#"{"reason":"build-finished",broken}"#,
            r#"{"reason":"compiler-artifact"}"#,
        ] {
            fs::write(&path, format!("{valid}{record}\n"))
                .assured("the malformed Cargo observation can be written");
            let outcome = messages(&path);
            assert!(
                matches!(outcome, Err(error) if matches!(
                    error.current_context(),
                    Error::Json { path: observed } if observed == &path
                )),
                "malformed Cargo records retain their log path: {record}"
            );
        }
    }
}
