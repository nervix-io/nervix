//! Ingestion delivery timestamps and lifecycle-bound admission.
//!
//! Layer: data plane.
//! - **Owns.** Selecting external or logical delivery time and enforcing its admission window.
//! - **Depends on.** Installed domain clocks, vocabulary and Arrow row views.
//! - **Must not know.** NSPL parsing, transport intake or progress notification history.

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    DomainName, DomainStatus, FieldName, IngestTimestampSource, IngestorName, Timestamp,
};
use thiserror::Error;

use super::{
    Runtime, RuntimeRow, RuntimeValue,
    domain_clock::{DomainClockAccessError, DomainIngestionSnapshot},
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

    pub(super) fn select(
        &self,
        source: Option<&IngestTimestampSource>,
        record: &RuntimeRow,
    ) -> Result<Timestamp, Report<IngestionTimeError>> {
        let event = match source {
            Some(IngestTimestampSource::Now) => self.now(),
            Some(IngestTimestampSource::At(field)) => {
                let context = || IngestionTimeError::TimestampField {
                    ingestor: self.ingestor.clone(),
                    field: field.clone(),
                };
                let value = record
                    .value(field.as_str())
                    .map_err(|_| Report::new(context()))?;
                let Some(RuntimeValue::Datetime(value)) = value else {
                    return Err(Report::new(context()));
                };
                Timestamp::try_from(value.to_utc()).change_context(context())?
            }
            None => {
                if self.clock.window.is_some() {
                    return Err(Report::new(IngestionTimeError::TimestampRequired {
                        domain: self.domain.clone(),
                        ingestor: self.ingestor.clone(),
                    }));
                }
                // A connector's chosen source timestamp remains an external value.
                record.metadata().ingested_at_low_watermark()
            }
        };
        if let Some(window) = &self.clock.window
            && !window.contains(event)
        {
            return Err(Report::new(IngestionTimeError::OutsideWindow {
                domain: self.domain.clone(),
                ingestor: self.ingestor.clone(),
            }));
        }
        Ok(event)
    }
}

impl Runtime {
    pub(super) fn ingestion_time<'a>(
        &self,
        domain: &'a DomainName,
        ingestor: &'a IngestorName,
    ) -> Result<IngestionTime<'a>, Report<IngestionTimeError>> {
        let context = || IngestionTimeError::Clock {
            domain: domain.clone(),
            ingestor: ingestor.clone(),
        };
        let state = self.inner.domains.get(domain).ok_or_else(|| {
            Report::new(DomainClockAccessError::Missing {
                domain: domain.clone(),
            })
            .change_context(context())
        })?;
        if let DomainStatus::Paused = state.status {
            return Err(Report::new(IngestionTimeError::Paused {
                domain: domain.clone(),
                ingestor: ingestor.clone(),
            }));
        }
        let clock = state.clock.bind().change_context(context())?;
        let snapshot = clock
            .ingestion_snapshot(&state.config.period, &state.config.skew)
            .change_context(context())?;
        Ok(IngestionTime {
            domain,
            ingestor,
            clock: snapshot,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{DomainClockState, DomainTimeRate};

    use super::*;
    use crate::{
        runtime::{current_timestamp, domain, named, paced_domain_state, unpaced_domain_state},
        runtime_schema::test_runtime_row,
    };

    #[test]
    fn timestamp_now_uses_the_installed_logical_delivery_snapshot() {
        let runtime = Runtime::new();
        let domain = domain("paced");
        let ingestor = named("source");
        let mut state = paced_domain_state(domain.as_str());
        state.clock = Some(DomainClockState::new(
            current_timestamp(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::try_from(1e-300).assured("fixture rate is positive and finite"),
        ));
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state)]));
        let time = runtime
            .ingestion_time(&domain, &ingestor)
            .assured("fixture clock is installed");
        let record = test_runtime_row([]).with_ingested_at_watermarks(current_timestamp());
        assert_eq!(
            time.select(Some(&IngestTimestampSource::Now), &record)
                .assured("origin is eligible"),
            Timestamp::from_unix_nanos(0)
        );
        assert!(matches!(
            time.select(None, &record)
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
        let mut state = unpaced_domain_state(domain.as_str());
        state.config.period = "0ms".to_string();
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state)]));
        let before = current_timestamp();
        let time = runtime
            .ingestion_time(&domain, &ingestor)
            .assured("fixture clock is installed");
        let external = Timestamp::from_unix_nanos(-123);
        let record = test_runtime_row([(
            "occurred_at".to_string(),
            RuntimeValue::Datetime(external.into_datetime().fixed_offset()),
        )])
        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(42));
        assert!(time.now() >= before && time.now() <= current_timestamp());
        assert_eq!(
            time.select(Some(&IngestTimestampSource::Now), &record)
                .assured("unpaced time is eligible"),
            time.now()
        );
        assert_eq!(
            time.select(
                Some(&IngestTimestampSource::At(named("occurred_at"))),
                &record
            )
            .assured("external datetime is supported"),
            external
        );
        assert_eq!(
            time.select(None, &record)
                .assured("unpaced connector time is eligible"),
            Timestamp::from_unix_nanos(42)
        );
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
