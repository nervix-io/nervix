//! The application's idempotent effect store: the readings and rejection notices the consumers
//! applied, each exactly once.
//!
//! - **Owns.** The effect file's JSON lines, the identities already applied, and deciding whether a
//!   delivered record is new or a duplicate.
//! - **Depends on.** The delivered readings and notices.
//! - **Must not know.** Sessions, consumers or acknowledgements.
//!
//! A consumer records an effect before it acknowledges the delivery that carried it. A delivery
//! that comes again, because its acknowledgement was lost or the reading was replayed, finds its
//! reading already applied and is acknowledged without applying it twice. The store is keyed by
//! the application's reading identity, which survives replays; a delivery identity would not.

use std::{io, path::Path};

use ahash::HashSet;
use error_stack::{Report, ResultExt as _};
use nervix_models::Timestamp;
use nervix_primitives::sync::watch;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt as _,
};

use crate::{
    ledger::rfc3339,
    readings::{ObservedReading, RejectionNotice},
};

/// Why the effect store cannot be read or written.
#[derive(Debug, Error)]
pub(crate) enum EffectError {
    #[error("cannot open the effect store '{path}'")]
    Open { path: String },
    #[error("line {line} of the effect store '{path}' is not an effect")]
    Malformed { path: String, line: usize },
    #[error("cannot record an effect")]
    Record,
}

/// One line of the effect store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum EffectRecord {
    /// A constructed reading the output emitter delivered.
    Reading {
        reading_id: String,
        sensor: String,
        tick: u64,
        #[serde(with = "rfc3339")]
        occurred_at: Timestamp,
        #[serde(with = "rfc3339")]
        admitted_at: Timestamp,
        timestamp_source: String,
        value: i64,
        /// The START generation of the consumer that applied it.
        generation: u64,
    },
    /// A rejection notice the rejection emitter delivered.
    Rejection {
        reading_id: String,
        #[serde(with = "rfc3339")]
        occurred_at: Timestamp,
        error_code: String,
        error_message: String,
        /// The START generation of the consumer that recorded it.
        generation: u64,
    },
}

/// Whether a delivered record changed the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Applied {
    New,
    Duplicate,
}

/// The effect store. One consumer at a time applies a record, so the check for a duplicate and the
/// append it decides belong to one step.
pub(crate) struct EffectStore {
    file: File,
    readings: HashSet<String>,
    rejections: HashSet<String>,
    /// How many rejection notices the store holds, for a run that waits for some.
    notices: watch::Sender<usize>,
}

impl EffectStore {
    /// Opens the store, reading back every effect an earlier run applied.
    pub(crate) async fn open(path: &Path) -> Result<Self, Report<EffectError>> {
        let open_error = |source| {
            Report::new(source).change_context(EffectError::Open {
                path: path.display().to_string(),
            })
        };
        let text = match tokio::fs::read_to_string(path).await {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
            Err(source) => return Err(open_error(source)),
        };
        let mut readings = HashSet::default();
        let mut rejections = HashSet::default();
        for (index, line) in text.lines().enumerate() {
            let record: EffectRecord = serde_json::from_str(line).map_err(|error| {
                Report::new(error).change_context(EffectError::Malformed {
                    path: path.display().to_string(),
                    line: index + 1,
                })
            })?;
            match record {
                EffectRecord::Reading { reading_id, .. } => {
                    readings.insert(reading_id);
                }
                EffectRecord::Rejection { reading_id, .. } => {
                    rejections.insert(reading_id);
                }
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(open_error)?;
        let (notices, _) = watch::channel(rejections.len());
        Ok(Self {
            file,
            readings,
            rejections,
            notices,
        })
    }

    /// Changes whenever the store records a rejection notice.
    pub(crate) fn notices(&self) -> watch::Receiver<usize> {
        self.notices.subscribe()
    }

    /// Whether the store holds the rejection notice of `reading_id`.
    pub(crate) fn has_rejection(&self, reading_id: &str) -> bool {
        self.rejections.contains(reading_id)
    }

    /// Applies a delivered reading unless it was applied before.
    pub(crate) async fn reading(
        &mut self,
        reading: &ObservedReading,
        generation: u64,
    ) -> Result<Applied, Report<EffectError>> {
        if self.readings.contains(&reading.reading_id) {
            return Ok(Applied::Duplicate);
        }
        let record = EffectRecord::Reading {
            reading_id: reading.reading_id.clone(),
            sensor: reading.sensor.clone(),
            tick: reading.tick,
            occurred_at: reading.occurred_at,
            admitted_at: reading.admitted_at,
            timestamp_source: reading.timestamp_source.clone(),
            value: reading.value,
            generation,
        };
        self.append(&record).await?;
        self.readings.insert(reading.reading_id.clone());
        Ok(Applied::New)
    }

    /// Records a delivered rejection notice unless it was recorded before.
    pub(crate) async fn rejection(
        &mut self,
        notice: &RejectionNotice,
        generation: u64,
    ) -> Result<Applied, Report<EffectError>> {
        if self.rejections.contains(&notice.reading_id) {
            return Ok(Applied::Duplicate);
        }
        let record = EffectRecord::Rejection {
            reading_id: notice.reading_id.clone(),
            occurred_at: notice.occurred_at,
            error_code: notice.error_code.clone(),
            error_message: notice.error_message.clone(),
            generation,
        };
        self.append(&record).await?;
        self.rejections.insert(notice.reading_id.clone());
        self.notices.send_replace(self.rejections.len());
        Ok(Applied::New)
    }

    async fn append(&mut self, record: &EffectRecord) -> Result<(), Report<EffectError>> {
        let mut line = serde_json::to_string(record).change_context(EffectError::Record)?;
        line.push('\n');
        self.file
            .write_all(line.as_bytes())
            .await
            .change_context(EffectError::Record)?;
        self.file.flush().await.change_context(EffectError::Record)
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    fn observed(reading_id: &str) -> ObservedReading {
        ObservedReading {
            reading_id: reading_id.to_string(),
            sensor: "sensor-0".to_string(),
            tick: 4,
            occurred_at: Timestamp::from_unix_nanos(400),
            admitted_at: Timestamp::from_unix_nanos(410),
            timestamp_source: "at".to_string(),
            value: 124,
        }
    }

    #[nervix_primitives::test]
    async fn a_record_is_applied_once_across_runs() {
        let directory = tempfile::tempdir().assured("a test can create a directory");
        let path = directory.path().join("effects.jsonl");
        let mut store = EffectStore::open(&path).await.assured("the store opens");
        assert_eq!(
            store
                .reading(&observed("g1-t4-s0-0"), 1)
                .await
                .assured("the store appends"),
            Applied::New
        );
        assert_eq!(
            store
                .reading(&observed("g1-t4-s0-0"), 1)
                .await
                .assured("the store appends"),
            Applied::Duplicate
        );
        let notice = RejectionNotice {
            reading_id: "g1-t4-s0-0".to_string(),
            occurred_at: Timestamp::from_unix_nanos(-1),
            error_code: "validation".to_string(),
            error_message: "outside".to_string(),
        };
        assert_eq!(
            store
                .rejection(&notice, 1)
                .await
                .assured("the store appends"),
            Applied::New
        );
        drop(store);

        let mut reopened = EffectStore::open(&path)
            .await
            .assured("the store opens again");
        assert_eq!(
            reopened
                .reading(&observed("g1-t4-s0-0"), 1)
                .await
                .assured("the store appends"),
            Applied::Duplicate
        );
        assert_eq!(
            reopened
                .rejection(&notice, 1)
                .await
                .assured("the store appends"),
            Applied::Duplicate
        );
        assert_eq!(
            reopened
                .reading(&observed("g1-t5-s0-0"), 1)
                .await
                .assured("the store appends"),
            Applied::New
        );
        let text = tokio::fs::read_to_string(&path)
            .await
            .assured("the store is readable");
        assert_eq!(text.lines().count(), 3);
        assert!(text.contains("\"occurred_at\":\"1970-01-01T00:00:00.000000400Z\""));
    }

    #[nervix_primitives::test]
    async fn a_malformed_effect_retains_its_decoder_cause_in_one_chain() {
        let directory = tempfile::tempdir().assured("a test can create a directory");
        let path = directory.path().join("effects.jsonl");
        tokio::fs::write(&path, "not json\n")
            .await
            .assured("a test can write a file");
        let report = EffectStore::open(&path)
            .await
            .err()
            .assured("the malformed effect is refused");
        assert!(matches!(
            report.current_context(),
            EffectError::Malformed { line: 1, .. }
        ));
        let cause = report
            .downcast_ref::<serde_json::Error>()
            .assured("the report retains the decoder cause");
        assert_eq!(
            format!("{report:#}"),
            format!(
                "line 1 of the effect store '{}' is not an effect: {cause}",
                path.display()
            )
        );
    }

    #[nervix_primitives::test]
    async fn a_store_path_that_is_not_a_file_is_named_when_it_cannot_be_opened() {
        let directory = tempfile::tempdir().assured("a test can create a directory");
        let expected = directory.path().display().to_string();
        let opened = EffectStore::open(directory.path()).await.err();
        assert!(matches!(
            opened.as_ref().map(Report::current_context),
            Some(EffectError::Open { path, .. }) if path == &expected
        ));
        let report = opened.assured("a directory is refused as an effect file");
        let cause = report
            .downcast_ref::<io::Error>()
            .assured("the report retains the I/O cause");
        assert_eq!(
            format!("{report:#}"),
            format!("cannot open the effect store '{expected}': {cause}")
        );
    }
}
