//! Ingestion delivery timestamps and lifecycle-bound admission.
//!
//! Layer: data plane.
//! - **Owns.** Selecting external or logical delivery time and enforcing its admission window.
//! - **Depends on.** Installed domain clocks, vocabulary and Arrow row views.
//! - **Must not know.** Plexus parsing, transport intake or progress notification history.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use arrow_array::{Array, TimestampNanosecondArray};
use arrow_buffer::BooleanBuffer;
use error_stack::{Report, ResultExt as _};
use nervix_models::{DomainName, FieldName, IngestTimestampSource, IngestorName, Timestamp};
use thiserror::Error;

use super::{
    IngestFilterMapMetadata, RecordMetadataColumns, Runtime, RuntimeRecordBatch,
    domain_clock::{DomainClockLifecycle, DomainIngestionSnapshot},
};

#[derive(Debug, Error)]
pub(super) enum IngestionTimeError {
    #[error("domain '{domain}' clock is unavailable to ingestor '{ingestor}'")]
    Clock {
        domain: DomainName,
        ingestor: IngestorName,
    },
    #[error("domain '{domain}' is paused; ingestor '{ingestor}' cannot accept events")]
    Paused {
        domain: DomainName,
        ingestor: IngestorName,
    },
    #[error(
        "paced domain '{domain}' requires ingestor '{ingestor}' to declare TIMESTAMP NOW or \
         TIMESTAMP AT <field>"
    )]
    TimestampRequired {
        domain: DomainName,
        ingestor: IngestorName,
    },
    #[error(
        "TIMESTAMP field '{field}' for ingestor '{ingestor}' must contain a supported DATETIME"
    )]
    TimestampField {
        ingestor: IngestorName,
        field: FieldName,
    },
    #[error(
        "TIMESTAMP AT metadata.{field} for ingestor '{ingestor}' must contain a supported DATETIME"
    )]
    TimestampMetadataField {
        ingestor: IngestorName,
        field: FieldName,
    },
    #[error(
        "paced domain '{domain}' rejected ingestor '{ingestor}' event outside any reached logical \
         tick window"
    )]
    OutsideWindow {
        domain: DomainName,
        ingestor: IngestorName,
    },
}

pub(super) struct IngestionTime<'a> {
    domain: &'a DomainName,
    ingestor: &'a IngestorName,
    clock: DomainIngestionSnapshot,
}

impl IngestionTime<'_> {
    pub(super) fn now(&self) -> Timestamp {
        self.clock.snapshot.now()
    }

    /// Resolves one timestamp column for the decoded group. A declared payload or metadata field
    /// reuses its Arrow values buffer; connector-owned ingest instants reuse the metadata's low
    /// watermark buffer.
    pub(super) fn select_column(
        &self,
        source: Option<&IngestTimestampSource>,
        batch: &RuntimeRecordBatch,
        metadata: &RecordMetadataColumns,
        ingest_metadata: &IngestFilterMapMetadata,
    ) -> Result<TimestampNanosecondArray, Report<IngestionTimeError>> {
        match source {
            Some(IngestTimestampSource::Now) => {
                Ok(TimestampNanosecondArray::from(vec![
                    self.now().unix_nanos();
                    batch.batch().num_rows()
                ]))
            }
            Some(IngestTimestampSource::At(field)) => {
                let context = || IngestionTimeError::TimestampField {
                    ingestor: self.ingestor.clone(),
                    field: field.clone(),
                };
                let index = batch
                    .batch()
                    .schema_ref()
                    .index_of(field.as_str())
                    .map_err(|_| Report::new(context()))?;
                let column = batch.batch().column(index);
                let timestamps = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| Report::new(context()))?;
                Ok(timestamps.clone())
            }
            Some(IngestTimestampSource::AtMetadata(field)) => {
                let context = || IngestionTimeError::TimestampMetadataField {
                    ingestor: self.ingestor.clone(),
                    field: field.clone(),
                };
                let column = ingest_metadata
                    .field_column(field.as_str())
                    .change_context_lazy(context)?
                    .ok_or_else(|| Report::new(context()))?;
                let timestamps = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| Report::new(context()))?;
                Ok(timestamps.clone())
            }
            None => {
                if self.clock.window.is_some() {
                    return Err(Report::new(IngestionTimeError::TimestampRequired {
                        domain: self.domain.clone(),
                        ingestor: self.ingestor.clone(),
                    }));
                }
                Ok(metadata.low_timestamp_column())
            }
        }
    }

    /// One admission bitmap for the selected column. A null declared timestamp is invalid
    /// even in an unpaced domain.
    pub(super) fn admit_column(&self, events: &TimestampNanosecondArray) -> BooleanBuffer {
        if let Some(window) = &self.clock.window {
            return window.admit_column(events);
        }
        match events.nulls() {
            Some(nulls) => nulls.inner().clone(),
            None => BooleanBuffer::new_set(events.len()),
        }
    }

    pub(super) fn rejection(
        &self,
        source: Option<&IngestTimestampSource>,
        missing: bool,
    ) -> IngestionTimeError {
        if missing && let Some(IngestTimestampSource::At(field)) = source {
            return IngestionTimeError::TimestampField {
                ingestor: self.ingestor.clone(),
                field: field.clone(),
            };
        }
        if missing && let Some(IngestTimestampSource::AtMetadata(field)) = source {
            return IngestionTimeError::TimestampMetadataField {
                ingestor: self.ingestor.clone(),
                field: field.clone(),
            };
        }
        IngestionTimeError::OutsideWindow {
            domain: self.domain.clone(),
            ingestor: self.ingestor.clone(),
        }
    }
}

impl Runtime {
    #[cfg(test)]
    pub(super) fn ingestion_time<'a>(
        &self,
        domain: &'a DomainName,
        ingestor: &'a IngestorName,
    ) -> Result<IngestionTime<'a>, Report<IngestionTimeError>> {
        let clock =
            self.domain_clock_lifecycle(domain)
                .change_context(IngestionTimeError::Clock {
                    domain: domain.clone(),
                    ingestor: ingestor.clone(),
                })?;
        clock.ingestion_time(domain, ingestor)
    }
}

impl DomainClockLifecycle {
    pub(super) fn ingestion_time<'a>(
        &self,
        domain: &'a DomainName,
        ingestor: &'a IngestorName,
    ) -> Result<IngestionTime<'a>, Report<IngestionTimeError>> {
        let context = || IngestionTimeError::Clock {
            domain: domain.clone(),
            ingestor: ingestor.clone(),
        };
        let snapshot = match self.ingestion_read().change_context(context())? {
            super::domain_clock::DomainIngestionRead::Paused => {
                return Err(Report::new(IngestionTimeError::Paused {
                    domain: domain.clone(),
                    ingestor: ingestor.clone(),
                }));
            }
            super::domain_clock::DomainIngestionRead::Available(snapshot) => snapshot,
        };
        Ok(IngestionTime {
            domain,
            ingestor,
            clock: snapshot,
        })
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::collections::BTreeMap;

    use arrow_array::TimestampNanosecondArray;
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{DomainClockState, DomainStatus, DomainTimeRate};

    use super::*;
    use crate::{
        runtime::{
            DomainClockAccessError, domain, named, paced_domain_state, unpaced_domain_state,
        },
        runtime_schema::{RuntimeValue, test_runtime_row},
    };

    #[test]
    fn timestamp_now_uses_the_installed_logical_delivery_snapshot() {
        let runtime = Runtime::new();
        let domain = domain("paced");
        let ingestor = named("source");
        let mut state = paced_domain_state(domain.as_str());
        state.clock = Some(DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::try_from(1e-300).assured("fixture rate is positive and finite"),
        ));
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state)]));
        let time = runtime
            .ingestion_time(&domain, &ingestor)
            .assured("fixture clock is installed");
        let record = test_runtime_row([]).with_ingested_at_watermarks(Timestamp::now());
        let batch = record.one_row_batch();
        let metadata = RecordMetadataColumns::from_rows([record.metadata().clone()]);
        let no_metadata =
            IngestFilterMapMetadata::headerless(1).assured("one headerless row builds");
        let column = time
            .select_column(
                Some(&IngestTimestampSource::Now),
                &batch,
                &metadata,
                &no_metadata,
            )
            .assured("TIMESTAMP NOW has a logical execution snapshot");
        assert_eq!(column.value(0), 0);
        assert!(time.admit_column(&column).value(0));
        assert!(matches!(
            time.select_column(None, &batch, &metadata, &no_metadata)
                .err()
                .assured("paced intake requires a source")
                .current_context(),
            IngestionTimeError::TimestampRequired { .. }
        ));
    }

    #[test]
    fn unpaced_time_uses_utc_and_preserves_declared_external_values() {
        let runtime = Runtime::new();
        let domain = domain("unpaced");
        let ingestor = named("source");
        let state = unpaced_domain_state(domain.as_str());
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state)]));
        let before = Timestamp::now();
        let time = runtime
            .ingestion_time(&domain, &ingestor)
            .assured("fixture clock is installed");
        let external = Timestamp::from_unix_nanos(-123);
        let record = test_runtime_row([(
            "occurred_at".to_string(),
            RuntimeValue::Datetime(external.into_datetime().fixed_offset()),
        )])
        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(42));
        assert!(time.now() >= before && time.now() <= Timestamp::now());
        let batch = record.one_row_batch();
        let metadata = RecordMetadataColumns::from_rows([record.metadata().clone()]);
        let no_metadata =
            IngestFilterMapMetadata::headerless(1).assured("one headerless row builds");
        let logical = time
            .select_column(
                Some(&IngestTimestampSource::Now),
                &batch,
                &metadata,
                &no_metadata,
            )
            .assured("unpaced logical time is available");
        assert_eq!(logical.value(0), time.now().unix_nanos());
        let declared = time
            .select_column(
                Some(&IngestTimestampSource::At(named("occurred_at"))),
                &batch,
                &metadata,
                &no_metadata,
            )
            .assured("the declared DATETIME column exists");
        assert_eq!(declared.value(0), -123);
        let connector = time
            .select_column(None, &batch, &metadata, &no_metadata)
            .assured("the unpaced connector timestamp exists");
        assert_eq!(connector.value(0), 42);
        let nullable = TimestampNanosecondArray::from(vec![Some(-123), None, Some(42)]);
        let accepted = time.admit_column(&nullable);
        assert_eq!(
            (0..3).map(|row| accepted.value(row)).collect::<Vec<_>>(),
            vec![true, false, true]
        );
    }

    #[test]
    fn timestamp_at_a_metadata_field_reads_its_declared_column_with_payload_admission() {
        use nervix_connector::{
            IngestMetadataRow, IngestRowHeaders, RetainedIngestMetadata,
            RetainedIngestMetadataValue,
        };
        use nervix_models::{HttpIngestorEntity, IngestorEntityKind};

        use crate::runtime::{IngestMetadataProjection, test_fixtures::ingest_metadata_for_test};

        let runtime = Runtime::new();
        let domain = domain("unpaced");
        let ingestor = named("source");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            unpaced_domain_state(domain.as_str()),
        )]));
        let time = runtime
            .ingestion_time(&domain, &ingestor)
            .assured("fixture clock is installed");
        let declared = IngestorEntityKind::Http(HttpIngestorEntity::Request)
            .capabilities()
            .metadata;
        let polled = Timestamp::from_unix_nanos(1_234);
        let answered = RetainedIngestMetadata::from_values(vec![
            RetainedIngestMetadataValue::U16(200),
            RetainedIngestMetadataValue::Datetime(polled),
        ]);
        let unanswered = RetainedIngestMetadata::from_values(vec![
            RetainedIngestMetadataValue::U16(200),
            RetainedIngestMetadataValue::Absent,
        ]);
        let rows = [
            IngestMetadataRow {
                fields: &answered,
                headers: IngestRowHeaders::NONE,
                position: None,
            },
            IngestMetadataRow {
                fields: &unanswered,
                headers: IngestRowHeaders::NONE,
                position: None,
            },
        ];
        let reading = IngestMetadataProjection::reading(declared, ["polled_at"])
            .assured("HTTP REQUEST declares polled_at");
        let ingest_metadata = ingest_metadata_for_test(reading, &rows);
        let record =
            test_runtime_row([]).with_ingested_at_watermarks(Timestamp::from_unix_nanos(9));
        let batch = RuntimeRecordBatch::concat(&[&record.one_row_batch(), &record.one_row_batch()])
            .assured("two rows of one schema concatenate");
        let metadata = RecordMetadataColumns::from_rows([
            record.metadata().clone(),
            record.metadata().clone(),
        ]);
        let source = IngestTimestampSource::AtMetadata(named("polled_at"));

        let selected = time
            .select_column(Some(&source), &batch, &metadata, &ingest_metadata)
            .assured("the read DATETIME metadata column exists");
        assert_eq!(selected.value(0), 1_234);
        let admitted = time.admit_column(&selected);
        assert_eq!(
            (0..2).map(|row| admitted.value(row)).collect::<Vec<_>>(),
            vec![true, false],
            "a message that carried no value is rejected as a null payload timestamp is"
        );
        assert!(matches!(
            time.rejection(Some(&source), true),
            IngestionTimeError::TimestampMetadataField { .. }
        ));

        let unread = IngestMetadataProjection::reading(declared, ["status"])
            .assured("HTTP REQUEST declares status");
        let without_column = ingest_metadata_for_test(unread, &rows);
        assert!(matches!(
            time.select_column(Some(&source), &batch, &metadata, &without_column)
                .err()
                .assured("a column no program reads is not built")
                .current_context(),
            IngestionTimeError::TimestampMetadataField { .. }
        ));
    }

    #[test]
    fn ingestion_requires_an_installed_running_domain() {
        let runtime = Runtime::new();
        let domain = domain("paced");
        let ingestor = named("source");
        let error = runtime
            .ingestion_time(&domain, &ingestor)
            .err()
            .assured("domain has not been created");
        assert!(matches!(
            error.downcast_ref::<DomainClockAccessError>(),
            Some(DomainClockAccessError::Missing { .. })
        ));
        let mut state = paced_domain_state(domain.as_str());
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state.clone())]));
        let error = runtime
            .ingestion_time(&domain, &ingestor)
            .err()
            .assured("paced mapping is absent");
        assert!(matches!(
            error.downcast_ref::<DomainClockAccessError>(),
            Some(DomainClockAccessError::Uninstalled { .. })
        ));
        state.status = DomainStatus::Stopped;
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state.clone())]));
        let error = runtime
            .ingestion_time(&domain, &ingestor)
            .err()
            .assured("domain is stopped");
        assert!(matches!(
            error.downcast_ref::<DomainClockAccessError>(),
            Some(DomainClockAccessError::Stopped { .. })
        ));
        state.status = DomainStatus::Paused;
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state)]));
        let error = runtime
            .ingestion_time(&domain, &ingestor)
            .err()
            .assured("domain is paused");
        assert!(matches!(
            error.current_context(),
            IngestionTimeError::Paused { .. }
        ));
    }
}
