//! The application-owned input ledger: every reading before it is submitted, the START generation
//! each run plans in, and the outcome of every batch.
//!
//! - **Owns.** The ledger file's JSON lines, appending to it, and reading it back to find the
//!   readings a deliberate replay resubmits.
//! - **Depends on.** The readings and their timestamps.
//! - **Must not know.** Sessions or producers.
//!
//! The ledger is written ahead of each submission, so a reading whose outcome is lost, or whose
//! process ends before its outcome arrives, is still in the ledger to be replayed. A production
//! outbox would also make each append durable before submitting; this example only flushes it.

use std::{collections::BTreeMap, io, path::Path};

use error_stack::{Report, ResultExt as _};
use nervix_models::Timestamp;
use nervix_primitives::sync::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt as _,
};

use crate::{
    options::TimestampSource,
    readings::{Reading, Stamp},
};

/// Why the ledger cannot be read or written.
#[derive(Debug, Error)]
pub(crate) enum LedgerError {
    #[error("cannot open the ledger '{path}'")]
    Open { path: String },
    #[error("cannot append to the ledger")]
    Append,
    #[error("line {line} of the ledger '{path}' is not a ledger record")]
    Malformed { path: String, line: usize },
}

/// The terminal outcome of a batch, as the ledger records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutcomeKind {
    Completed,
    NotAdmitted,
    ProcessingFailed,
    OutcomeUnknown,
}

impl OutcomeKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::NotAdmitted => "not_admitted",
            Self::ProcessingFailed => "processing_failed",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }
}

/// One line of the ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum LedgerRecord {
    /// A run began planning readings in this START generation.
    Generation { generation: u64 },
    /// A reading, written before the batch that carries it is submitted.
    Reading(LedgerReading),
    /// The terminal outcome of the batch that carried these readings.
    Outcome {
        reading_ids: Vec<String>,
        outcome: OutcomeKind,
        cause: String,
    },
}

/// A reading as the ledger records it, with the ingestor and event-time source it was submitted
/// under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LedgerReading {
    pub(crate) reading_id: String,
    pub(crate) generation: u64,
    pub(crate) tick: u64,
    pub(crate) sensor: String,
    #[serde(with = "rfc3339")]
    pub(crate) occurred_at: Timestamp,
    pub(crate) value: i64,
    pub(crate) ingestor: String,
    pub(crate) timestamps: String,
    pub(crate) stamp: Stamp,
}

/// RFC 3339 text for timestamps in the ledger and the effect store.
pub(crate) mod rfc3339 {
    use std::str::FromStr as _;

    use nervix_models::Timestamp;
    use serde::{Deserialize as _, Deserializer, Serializer, de::Error as _};

    pub(crate) fn serialize<S: Serializer>(
        timestamp: &Timestamp,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&timestamp.to_rfc3339())
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Timestamp, D::Error> {
        let text = String::deserialize(deserializer)?;
        Timestamp::from_str(&text).map_err(D::Error::custom)
    }
}

/// The ledger file, appended to by the planner and by every outcome it awaits.
pub(crate) struct Ledger {
    file: Mutex<File>,
}

impl Ledger {
    /// Opens the ledger for appending, creating it when it does not exist.
    pub(crate) async fn open(path: &Path) -> Result<Self, Report<LedgerError>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|source| {
                Report::new(source).change_context(LedgerError::Open {
                    path: path.display().to_string(),
                })
            })?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    /// The readings of the ledger at `path` that never completed, in the order they were first
    /// planned. A reading's latest outcome decides: a completed batch resolved it for good, and
    /// any other outcome, or none, leaves it for the application to replay.
    pub(crate) async fn unresolved(path: &Path) -> Result<Vec<Reading>, Report<LedgerError>> {
        let text = match tokio::fs::read_to_string(path).await {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(Report::new(source).change_context(LedgerError::Open {
                    path: path.display().to_string(),
                }));
            }
        };
        let mut order = Vec::new();
        let mut readings: BTreeMap<String, LedgerReading> = BTreeMap::new();
        let mut outcomes: BTreeMap<String, OutcomeKind> = BTreeMap::new();
        for (index, line) in text.lines().enumerate() {
            let record: LedgerRecord = serde_json::from_str(line).map_err(|error| {
                Report::new(error).change_context(LedgerError::Malformed {
                    path: path.display().to_string(),
                    line: index + 1,
                })
            })?;
            match record {
                LedgerRecord::Generation { .. } => {}
                LedgerRecord::Reading(reading) => {
                    if !readings.contains_key(&reading.reading_id) {
                        order.push(reading.reading_id.clone());
                    }
                    readings.insert(reading.reading_id.clone(), reading);
                }
                LedgerRecord::Outcome {
                    reading_ids,
                    outcome,
                    ..
                } => {
                    for reading_id in reading_ids {
                        outcomes.insert(reading_id, outcome);
                    }
                }
            }
        }
        let mut unresolved = Vec::new();
        for reading_id in order {
            if outcomes.get(&reading_id) == Some(&OutcomeKind::Completed) {
                continue;
            }
            let Some(recorded) = readings.remove(&reading_id) else {
                continue;
            };
            unresolved.push(Reading {
                reading_id: recorded.reading_id,
                generation: recorded.generation,
                tick: recorded.tick,
                sensor: recorded.sensor,
                occurred_at: recorded.occurred_at,
                value: recorded.value,
                stamp: recorded.stamp,
            });
        }
        Ok(unresolved)
    }

    pub(crate) async fn generation(&self, generation: u64) -> Result<(), Report<LedgerError>> {
        self.append(&[LedgerRecord::Generation { generation }])
            .await
    }

    /// Records the readings of a batch before it is submitted.
    pub(crate) async fn readings(
        &self,
        readings: &[Reading],
        ingestor: &str,
        timestamps: TimestampSource,
    ) -> Result<(), Report<LedgerError>> {
        let mut records = Vec::with_capacity(readings.len());
        for reading in readings {
            records.push(LedgerRecord::Reading(LedgerReading {
                reading_id: reading.reading_id.clone(),
                generation: reading.generation,
                tick: reading.tick,
                sensor: reading.sensor.clone(),
                occurred_at: reading.occurred_at,
                value: reading.value,
                ingestor: ingestor.to_string(),
                timestamps: timestamps.as_str().to_string(),
                stamp: reading.stamp,
            }));
        }
        self.append(&records).await
    }

    /// Records the terminal outcome of the batch that carried `reading_ids`.
    pub(crate) async fn outcome(
        &self,
        reading_ids: Vec<String>,
        outcome: OutcomeKind,
        cause: &str,
    ) -> Result<(), Report<LedgerError>> {
        self.append(&[LedgerRecord::Outcome {
            reading_ids,
            outcome,
            cause: cause.to_string(),
        }])
        .await
    }

    async fn append(&self, records: &[LedgerRecord]) -> Result<(), Report<LedgerError>> {
        let mut text = String::new();
        for record in records {
            let line = serde_json::to_string(record).change_context(LedgerError::Append)?;
            text.push_str(&line);
            text.push('\n');
        }
        let mut file = self.file.lock().await;
        file.write_all(text.as_bytes())
            .await
            .change_context(LedgerError::Append)?;
        file.flush().await.change_context(LedgerError::Append)
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;
    use crate::readings::ReadingSlot;

    fn reading(tick: u64, sensor: u32) -> Reading {
        Reading::planned(
            ReadingSlot {
                generation: 1,
                tick,
                sensor,
                position: 0,
            },
            Timestamp::from_unix_nanos(1_800_000_000_100_000_000),
            Stamp::Center,
        )
    }

    #[nervix_primitives::test]
    async fn a_reading_stays_unresolved_until_a_batch_carrying_it_completes() {
        let directory = tempfile::tempdir().assured("a test can create a directory");
        let path = directory.path().join("ledger.jsonl");
        let ledger = Ledger::open(&path).await.assured("the ledger opens");
        let first = reading(1, 0);
        let second = reading(1, 1);
        let third = reading(2, 0);
        ledger.generation(1).await.assured("the ledger appends");
        ledger
            .readings(
                &[first.clone(), second.clone()],
                "simulated_readings",
                TimestampSource::At,
            )
            .await
            .assured("the ledger appends");
        ledger
            .outcome(
                vec![first.reading_id.clone(), second.reading_id.clone()],
                OutcomeKind::OutcomeUnknown,
                "session_lost",
            )
            .await
            .assured("the ledger appends");
        ledger
            .readings(
                std::slice::from_ref(&third),
                "simulated_readings",
                TimestampSource::At,
            )
            .await
            .assured("the ledger appends");
        ledger
            .readings(
                std::slice::from_ref(&first),
                "simulated_readings",
                TimestampSource::At,
            )
            .await
            .assured("the ledger appends");
        ledger
            .outcome(vec![first.reading_id.clone()], OutcomeKind::Completed, "")
            .await
            .assured("the ledger appends");

        let unresolved = Ledger::unresolved(&path)
            .await
            .assured("the ledger reads back");
        assert_eq!(unresolved, [second, third]);
    }

    #[nervix_primitives::test]
    async fn a_malformed_line_names_the_ledger_and_its_line() {
        let directory = tempfile::tempdir().assured("a test can create a directory");
        let path = directory.path().join("ledger.jsonl");
        tokio::fs::write(
            &path,
            "{\"record\":\"generation\",\"generation\":1}\nnot json\n",
        )
        .await
        .assured("a test can write a file");
        let error = Ledger::unresolved(&path).await.err();
        assert!(
            error
                .as_ref()
                .is_some_and(|report| report.contains::<serde_json::Error>())
        );
        assert!(matches!(
            error.as_ref().map(Report::current_context),
            Some(LedgerError::Malformed { line: 2, .. })
        ));
        let report = error.as_ref().assured("the malformed line is refused");
        let cause = report
            .downcast_ref::<serde_json::Error>()
            .assured("the report retains the decoder cause");
        assert_eq!(
            format!("{report:#}"),
            format!(
                "line 2 of the ledger '{}' is not a ledger record: {cause}",
                path.display()
            )
        );
    }

    #[nervix_primitives::test]
    async fn a_ledger_path_that_is_not_a_file_is_named_when_it_cannot_be_opened() {
        let directory = tempfile::tempdir().assured("a test can create a directory");
        let expected = directory.path().display().to_string();
        let appended = Ledger::open(directory.path()).await.err();
        assert!(matches!(
            appended.as_ref().map(Report::current_context),
            Some(LedgerError::Open { path, .. }) if path == &expected
        ));
        let replayed = Ledger::unresolved(directory.path()).await.err();
        assert!(matches!(
            replayed.as_ref().map(Report::current_context),
            Some(LedgerError::Open { path, .. }) if path == &expected
        ));
        for report in [appended, replayed] {
            let report = report.assured("a directory is refused as a ledger file");
            let cause = report
                .downcast_ref::<io::Error>()
                .assured("the report retains the I/O cause");
            assert_eq!(
                format!("{report:#}"),
                format!("cannot open the ledger '{expected}': {cause}")
            );
        }
    }
}
