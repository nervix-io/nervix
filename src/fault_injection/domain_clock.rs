//! Holding domain clock progress before delivery and starting a mapping with wall time elapsed.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Domain- and node-scoped holds of an authority's progress before its delivery, and a
//!   domain's initial elapsed wall time.
//! - **Depends on.** Typed domain and node names and the fault harness's existing pause primitive.
//! - **Must not know.** Authority selection, consensus, or how the runtime installs a clock.

use std::time::Duration;

use nervix_models::{ClusterNodeName, DomainName};
use nervix_primitives::sync::{Arc, CancellationToken};

use super::{FaultInjection, TestPause};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct DomainClockProgressPausePoint {
    domain: String,
    node: Option<ClusterNodeName>,
}

impl FaultInjection {
    pub fn pause_domain_clock_progress(&self, domain: impl Into<String>) {
        self.inner.domain_clock_progress_pauses.insert(
            DomainClockProgressPausePoint {
                domain: domain.into().to_ascii_lowercase(),
                node: None,
            },
            Arc::new(TestPause::default()),
        );
    }

    pub fn set_domain_clock_initial_elapsed(&self, domain: DomainName, elapsed: Duration) {
        self.inner
            .domain_clock_initial_elapsed
            .insert(domain, elapsed);
    }

    pub(crate) fn take_domain_clock_initial_elapsed(
        &self,
        domain: &DomainName,
    ) -> Option<Duration> {
        let (_, elapsed) = self.inner.domain_clock_initial_elapsed.remove(domain)?;
        Some(elapsed)
    }

    pub fn pause_domain_clock_progress_on(&self, domain: impl Into<String>, node: ClusterNodeName) {
        self.inner.domain_clock_progress_pauses.insert(
            DomainClockProgressPausePoint {
                domain: domain.into().to_ascii_lowercase(),
                node: Some(node),
            },
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_domain_clock_progress_pause(&self, domain: &str) {
        let point = DomainClockProgressPausePoint {
            domain: domain.to_ascii_lowercase(),
            node: None,
        };
        let pause = self.domain_clock_progress_pause(&point);
        pause.wait_until_reached().await;
    }

    pub async fn wait_for_domain_clock_progress_pause_on(
        &self,
        domain: &str,
        node: &ClusterNodeName,
    ) {
        let point = DomainClockProgressPausePoint {
            domain: domain.to_ascii_lowercase(),
            node: Some(node.clone()),
        };
        let pause = self.domain_clock_progress_pause(&point);
        pause.wait_until_reached().await;
    }

    pub async fn release_domain_clock_progress(&self, domain: &str) {
        let point = DomainClockProgressPausePoint {
            domain: domain.to_ascii_lowercase(),
            node: None,
        };
        let pause = self.domain_clock_progress_pause(&point);
        pause.release();
        pause.wait_until_delivered().await;
        self.inner.domain_clock_progress_pauses.remove(&point);
    }

    pub async fn release_domain_clock_progress_on(&self, domain: &str, node: &ClusterNodeName) {
        let point = DomainClockProgressPausePoint {
            domain: domain.to_ascii_lowercase(),
            node: Some(node.clone()),
        };
        let pause = self.domain_clock_progress_pause(&point);
        pause.release();
        pause.wait_until_delivered().await;
        self.inner.domain_clock_progress_pauses.remove(&point);
    }

    pub fn release_all_domain_clock_progress(&self) {
        for pause in &self.inner.domain_clock_progress_pauses {
            pause.release();
        }
        self.inner.domain_clock_progress_pauses.clear();
    }

    pub(crate) async fn pause_domain_clock_progress_if_armed(
        &self,
        domain: &DomainName,
        node: &ClusterNodeName,
        shutdown: &CancellationToken,
    ) -> bool {
        let exact = DomainClockProgressPausePoint {
            domain: domain.as_str().to_ascii_lowercase(),
            node: Some(node.clone()),
        };
        let any_node = DomainClockProgressPausePoint {
            domain: domain.as_str().to_ascii_lowercase(),
            node: None,
        };
        let pause = if let Some(pause) = self.inner.domain_clock_progress_pauses.get(&exact) {
            pause.value().clone()
        } else if let Some(pause) = self.inner.domain_clock_progress_pauses.get(&any_node) {
            pause.value().clone()
        } else {
            return false;
        };
        pause.reach();
        nervix_primitives::select! {
            _ = shutdown.cancelled() => false,
            _ = pause.wait_until_released() => true,
        }
    }

    pub(crate) fn mark_domain_clock_progress_delivered(
        &self,
        domain: &DomainName,
        node: &ClusterNodeName,
    ) {
        let exact = DomainClockProgressPausePoint {
            domain: domain.as_str().to_ascii_lowercase(),
            node: Some(node.clone()),
        };
        let any_node = DomainClockProgressPausePoint {
            domain: domain.as_str().to_ascii_lowercase(),
            node: None,
        };
        if let Some(pause) = self
            .inner
            .domain_clock_progress_pauses
            .get(&exact)
            .or_else(|| self.inner.domain_clock_progress_pauses.get(&any_node))
        {
            pause.mark_delivered();
        }
    }

    fn domain_clock_progress_pause(&self, point: &DomainClockProgressPausePoint) -> Arc<TestPause> {
        let Some(pause) = self.inner.domain_clock_progress_pauses.get(point) else {
            panic!("domain clock progress pause for '{point:?}' is not armed");
        };
        pause.value().clone()
    }
}
