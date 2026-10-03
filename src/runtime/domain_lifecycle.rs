//! Lifecycle observations retained with a task's domain clock.
//!
//! Layer: data plane.
//! - **Owns.** Pause, generation and start-point observations in the complete clock publication.
//! - **Depends on.** Clock capabilities, domain lifecycle vocabulary and publication snapshots.
//! - **Must not know.** Connector drivers, graph planning or task scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "domain start, pause and stop change the installed execution lifetime"
    )
)]

use error_stack::Report;
use nervix_models::{DomainName, DomainState};

use super::{
    DomainClock, DomainClockAccessError, DomainClockAccessResult, DomainClockLifecycle,
    DomainExecutionSnapshot, DomainIngestionSnapshot, Runtime,
};

/// Lifecycle information read by recurring task paths from the clock's coherent publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct DomainTaskState {
    pub(in crate::runtime) status: nervix_models::DomainStatus,
    pub(in crate::runtime) generation: u64,
    pub(in crate::runtime) last_start: nervix_models::DomainStartPoint,
}

impl From<&DomainState> for DomainTaskState {
    fn from(state: &DomainState) -> Self {
        Self {
            status: state.status.clone(),
            generation: state.start_version,
            last_start: state.last_start.clone(),
        }
    }
}

pub(in crate::runtime) enum DomainIngestionRead {
    Paused,
    Available(DomainIngestionSnapshot),
}

impl DomainClockLifecycle {
    /// Subscriptions outlive a START and read the generation currently installed in this owner.
    pub(crate) fn execution_snapshot(&self) -> DomainClockAccessResult<DomainExecutionSnapshot> {
        let published = self.inner.published.load();
        let generation = published.installation.generation().ok_or_else(|| {
            Report::new(DomainClockAccessError::Missing {
                domain: self.inner.domain.clone(),
            })
        })?;
        let clock = DomainClock {
            inner: self.inner.clone(),
            generation,
        };
        let source = clock.source(&published.installation)?;
        clock.observe(source, &published.watermark)
    }

    pub(in crate::runtime) fn ingestion_read(
        &self,
    ) -> DomainClockAccessResult<DomainIngestionRead> {
        let published = self.inner.published.load();
        let state = published.task_state.as_ref().ok_or_else(|| {
            Report::new(DomainClockAccessError::Missing {
                domain: self.inner.domain.clone(),
            })
        })?;
        if state.status == nervix_models::DomainStatus::Paused {
            return Ok(DomainIngestionRead::Paused);
        }
        let clock = DomainClock {
            inner: self.inner.clone(),
            generation: state.generation,
        };
        Ok(DomainIngestionRead::Available(
            clock.ingestion_snapshot_from(&published)?,
        ))
    }
}

impl DomainClockLifecycle {
    pub(in crate::runtime) fn task_state(&self) -> Option<DomainTaskState> {
        self.inner.published.load().task_state.clone()
    }
}

impl DomainClockLifecycle {
    pub(in crate::runtime) fn generation(&self) -> Option<u64> {
        self.inner.published.load().installation.generation()
    }
}

impl DomainClock {
    pub(in crate::runtime) fn is_paused(&self) -> bool {
        let publication = self.inner.published.load();
        matches!(
            publication.task_state.as_ref(),
            Some(DomainTaskState {
                status: nervix_models::DomainStatus::Paused,
                ..
            })
        )
    }
}

impl Runtime {
    pub(crate) fn domain_clock_lifecycle(
        &self,
        domain: &DomainName,
    ) -> DomainClockAccessResult<DomainClockLifecycle> {
        let entry = self.inner.domains.get(domain).ok_or_else(|| {
            Report::new(DomainClockAccessError::Missing {
                domain: domain.clone(),
            })
        })?;
        Ok(entry.clock.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{DomainStatus, Timestamp};

    use super::*;
    use crate::runtime::{domain, paced_domain_state};

    #[test]
    fn a_retained_subscription_clock_follows_start_pause_restart_and_stop() {
        let runtime = Runtime::new();
        let domain = domain("subscription_clock");
        let mut state = paced_domain_state(domain.as_str());
        state.status = DomainStatus::Stopped;
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state.clone())]));
        let lifecycle = runtime
            .domain_clock_lifecycle(&domain)
            .assured("a stopped domain still owns its lifecycle");
        assert!(matches!(
            lifecycle
                .execution_snapshot()
                .err()
                .assured("a stopped domain has no execution time")
                .current_context(),
            DomainClockAccessError::Stopped { .. }
        ));

        state.status = DomainStatus::Running;
        state.start_version = 1;
        state.clock = Some(nervix_models::DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(0),
            nervix_models::DomainTimeRate::ONE,
        ));
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state.clone())]));
        assert_eq!(
            lifecycle
                .execution_snapshot()
                .assured("the retained subscription follows START")
                .generation(),
            1
        );
        state.status = DomainStatus::Paused;
        state.clock = None;
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state.clone())]));
        assert_eq!(
            lifecycle
                .execution_snapshot()
                .assured("an alteration pause keeps execution time installed")
                .generation(),
            1
        );
        assert!(matches!(
            lifecycle
                .ingestion_read()
                .assured("the same publication suspends intake"),
            DomainIngestionRead::Paused
        ));

        state.status = DomainStatus::Running;
        state.start_version = 2;
        state.clock = Some(nervix_models::DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(100),
            nervix_models::DomainTimeRate::ONE,
        ));
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), state.clone())]));
        assert_eq!(
            lifecycle
                .execution_snapshot()
                .assured("a subscription follows the current installed generation")
                .generation(),
            2
        );
        state.status = DomainStatus::Stopped;
        state.clock = None;
        runtime.sync_domains(&BTreeMap::from([(domain, state)]));
        assert!(matches!(
            lifecycle
                .execution_snapshot()
                .err()
                .assured("STOP withdraws execution time from the retained owner")
                .current_context(),
            DomainClockAccessError::Stopped { generation: 2, .. }
        ));
    }
}
