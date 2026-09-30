//! Dependencies retained by an ingestor execution and every group it admits.
//!
//! Layer: data plane.
//! - **Owns.** The lifecycle, acknowledgement accounting and metric mark bound at source startup.
//! - **Depends on.** Domain clock publication, acknowledgement roots and metric replication.
//! - **Must not know.** Codecs, connector drivers, graph planning or payload construction.

use super::*;

#[derive(Clone)]
pub(super) struct IngestTaskHandles {
    inner: Arc<IngestTaskDependencies>,
}

struct IngestTaskDependencies {
    clock: domain_clock::DomainClockLifecycle,
    trackers: IngestorAckRootTrackers,
    metrics: BranchMetricsMark,
}

impl IngestTaskHandles {
    pub(super) fn tracked_root(&self) -> (AckSet, AckCompletion) {
        self.inner.trackers.tracked_root()
    }

    pub(super) fn ingestion_time<'a>(
        &self,
        domain: &'a DomainName,
        ingestor: &'a IngestorName,
    ) -> error_stack::Result<ingestion_time::IngestionTime<'a>, ingestion_time::IngestionTimeError>
    {
        self.inner.clock.ingestion_time(domain, ingestor)
    }

    pub(super) fn mark_metrics(&self) {
        self.inner.metrics.mark();
    }

    #[cfg(test)]
    pub(super) fn detached(domain: &DomainName) -> Self {
        let clock = domain_clock::DomainClockLifecycle::new(domain.clone());
        clock.synchronize(
            &unpaced_domain_state(domain.as_str()),
            &test_domain_clock_authority(),
        );
        Self {
            inner: Arc::new(IngestTaskDependencies {
                clock,
                trackers: IngestorAckRootTrackers::detached(),
                metrics: BranchMetricsMark::default(),
            }),
        }
    }
}

impl Runtime {
    pub(super) fn ingest_task_handles(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> domain_clock::DomainClockAccessResult<IngestTaskHandles> {
        let clock = self.domain_clock_lifecycle(domain)?;
        Ok(IngestTaskHandles {
            inner: Arc::new(IngestTaskDependencies {
                clock,
                trackers: self.ingestor_ack_root_trackers(domain, ingestor),
                metrics: self.branch_metrics_mark(domain, ModelKind::Ingestor, ingestor),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn retained_ingest_handles_share_drain_accounting_and_follow_domain_lifecycle() {
        let runtime = Runtime::new();
        let domain = domain("retained");
        let ingestor = named("source");
        let mut state = unpaced_domain_state(domain.as_str());
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state.clone())]));
        let handles = runtime
            .ingest_task_handles(&domain, &ingestor)
            .assured("the source binds an installed domain");
        let observer = runtime.ingestor_ack_root_trackers(&domain, &ingestor);
        let (acks, completion) = handles.tracked_root();
        assert_eq!(runtime.domain_outstanding_work(&domain), 1);
        assert_eq!(observer.ingestor_outstanding(), 1);
        acks.ack_success();
        drop(completion);
        assert_eq!(runtime.domain_outstanding_work(&domain), 0);
        assert_eq!(observer.ingestor_outstanding(), 0);

        handles
            .ingestion_time(&domain, &ingestor)
            .assured("the retained lifecycle admits running intake");
        state.status = nervix_models::DomainStatus::Paused;
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state.clone())]));
        let paused = handles
            .ingestion_time(&domain, &ingestor)
            .err()
            .assured("the same retained handle observes pause");
        assert!(matches!(
            paused.current_context(),
            ingestion_time::IngestionTimeError::Paused { .. }
        ));

        state.status = nervix_models::DomainStatus::Running;
        state.start_version = 1;
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state)]));
        handles
            .ingestion_time(&domain, &ingestor)
            .assured("the ingestor lifecycle follows the restarted generation");
        runtime.sync_domains(&BTreeMap::new());
        let missing = handles
            .ingestion_time(&domain, &ingestor)
            .err()
            .assured("domain removal fences the retained handle");
        assert!(matches!(
            missing.downcast_ref::<DomainClockAccessError>(),
            Some(DomainClockAccessError::Missing { .. })
        ));
    }
}
