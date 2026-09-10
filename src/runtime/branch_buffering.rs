//! Branch-local input collection and route flush timing for the data plane.
//!
//! Layer: data plane.
//!
//! - **Owns.** Branch-local collected batches and the typed deadline of a buffered branch.
//! - **Depends on.** Bound domain clocks, physical deadline capabilities and relay batches.
//! - **Must not know.** Graph planning, connector retries, source-group idle collection or sinks.

use std::{future::pending, time::Duration};

use error_stack::{Report, ResultExt as _};
use futures_util::{StreamExt as _, stream::FuturesUnordered};
use meticulous::OptionExt as _;
#[cfg(test)]
use meticulous::ResultExt as _;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    DomainClock, DomainExecutionSnapshot, LogicalDeadline, RelayRecordBatch,
    checked_add_duration_to_timestamp,
    physical_time::{PhysicalDeadline, PhysicalDeadlineCapability},
};
use crate::runtime_ack::AckSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RuntimeInputCollectPolicy {
    pub(super) interval: Duration,
    pub(super) max_batch_size: Option<u64>,
}

impl RuntimeInputCollectPolicy {
    pub(super) fn size_boundary_reached(self, pending_bytes: u64) -> bool {
        self.max_batch_size
            .is_some_and(|max_batch_size| pending_bytes >= max_batch_size)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RuntimeFlushPolicy {
    Each {
        interval: Duration,
        max_batch_size: u64,
    },
    Immediate,
}

impl RuntimeFlushPolicy {
    pub(super) const IMMEDIATE_MINIMUM_TIMEOUT: Duration = Duration::from_micros(100);

    pub(super) fn size_boundary_reached(self, pending_bytes: u64) -> bool {
        match self {
            Self::Each { max_batch_size, .. } => pending_bytes >= max_batch_size,
            Self::Immediate => false,
        }
    }
}

#[derive(Debug, Error)]
pub(super) enum BranchBufferTimingError {
    #[error("the physical FLUSH IMMEDIATE deadline is outside the monotonic clock range")]
    ImmediateDeadline,
    #[error("the domain clock could not reach a branch-buffer deadline")]
    LogicalDeadline,
}

pub(super) type BranchBufferTimingResult<T> = Result<T, Report<BranchBufferTimingError>>;

/// The deadline for one non-empty branch buffer.
///
/// The variants deliberately retain their clock coordinate. Code that waits for buffering may
/// branch on the variant, but cannot obtain an untyped timestamp from it.
#[derive(Debug, Clone)]
pub(super) enum BranchBufferDeadline {
    Logical(LogicalDeadline),
    Physical(PhysicalDeadline),
}

#[derive(Debug, Default, Clone)]
pub(super) struct BranchBufferTimer {
    deadline: Option<BranchBufferDeadline>,
}

impl BranchBufferTimer {
    pub(super) fn arm_logical(
        &mut self,
        clock: &DomainClock,
        snapshot: &DomainExecutionSnapshot,
        interval: Duration,
    ) {
        if self.deadline.is_none() {
            let due_at = checked_add_duration_to_timestamp(snapshot.now(), interval);
            self.deadline = Some(BranchBufferDeadline::Logical(clock.deadline_at(due_at)));
        }
    }

    pub(super) fn arm_flush(
        &mut self,
        policy: RuntimeFlushPolicy,
        clock: &DomainClock,
        snapshot: &DomainExecutionSnapshot,
    ) -> BranchBufferTimingResult<()> {
        if self.deadline.is_some() {
            return Ok(());
        }
        self.deadline = Some(match policy {
            RuntimeFlushPolicy::Each { interval, .. } => {
                let due_at = checked_add_duration_to_timestamp(snapshot.now(), interval);
                BranchBufferDeadline::Logical(clock.deadline_at(due_at))
            }
            RuntimeFlushPolicy::Immediate => {
                let physical = PhysicalDeadlineCapability::new()
                    .after(RuntimeFlushPolicy::IMMEDIATE_MINIMUM_TIMEOUT)
                    .change_context(BranchBufferTimingError::ImmediateDeadline)?;
                BranchBufferDeadline::Physical(physical)
            }
        });
        Ok(())
    }

    pub(super) fn is_due(
        &self,
        clock: &DomainClock,
        snapshot: &DomainExecutionSnapshot,
    ) -> BranchBufferTimingResult<bool> {
        let Some(deadline) = &self.deadline else {
            return Ok(false);
        };
        match deadline {
            BranchBufferDeadline::Logical(deadline) => clock
                .deadline_reached(deadline, snapshot)
                .change_context(BranchBufferTimingError::LogicalDeadline),
            BranchBufferDeadline::Physical(deadline) => {
                Ok(PhysicalDeadlineCapability::new().is_reached(*deadline))
            }
        }
    }

    pub(super) fn deadline(&self) -> Option<BranchBufferDeadline> {
        self.deadline.clone()
    }

    pub(super) fn clear(&mut self) {
        self.deadline = None;
    }
}

#[derive(Debug)]
pub(super) struct RuntimeInputCollector {
    policy: RuntimeInputCollectPolicy,
    pending: Vec<RelayRecordBatch>,
    pending_bytes: u64,
    timer: BranchBufferTimer,
}

impl RuntimeInputCollector {
    pub(super) fn new(policy: RuntimeInputCollectPolicy) -> Self {
        Self {
            policy,
            pending: Vec::new(),
            pending_bytes: 0,
            timer: BranchBufferTimer::default(),
        }
    }

    pub(super) fn reconfigure(&mut self, policy: RuntimeInputCollectPolicy) {
        self.policy = policy;
    }

    #[cfg(test)]
    pub(super) const fn policy(&self) -> RuntimeInputCollectPolicy {
        self.policy
    }

    pub(super) fn push(
        &mut self,
        batch: RelayRecordBatch,
        clock: &DomainClock,
        snapshot: &DomainExecutionSnapshot,
    ) -> bool {
        self.timer
            .arm_logical(clock, snapshot, self.policy.interval);
        self.pending_bytes = self
            .pending_bytes
            .checked_add(batch.estimated_bytes())
            .assured("both counts estimate bytes of batches this node already holds in memory");
        self.pending.push(batch);
        self.policy.size_boundary_reached(self.pending_bytes)
    }

    pub(super) fn is_due(
        &self,
        clock: &DomainClock,
        snapshot: &DomainExecutionSnapshot,
    ) -> BranchBufferTimingResult<bool> {
        if self.pending.is_empty() {
            return Ok(false);
        }
        self.timer.is_due(clock, snapshot)
    }

    pub(super) fn deadline(&self) -> Option<BranchBufferDeadline> {
        self.timer.deadline()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn merged_acks(&self) -> AckSet {
        AckSet::merged(self.pending.iter().map(RelayRecordBatch::merged_acks))
    }

    pub(super) fn no_ack_pending(&self, reason: &str) {
        for batch in &self.pending {
            batch.merged_acks().no_ack(reason.to_string());
        }
    }

    pub(super) fn take_pending(&mut self) -> Vec<RelayRecordBatch> {
        self.pending_bytes = 0;
        self.timer.clear();
        std::mem::take(&mut self.pending)
    }
}

pub(super) async fn wait_for_branch_buffer_deadlines(
    clock: &DomainClock,
    deadlines: Vec<BranchBufferDeadline>,
) -> BranchBufferTimingResult<()> {
    if deadlines.is_empty() {
        pending::<()>().await;
        return Ok(());
    }
    let mut waits = FuturesUnordered::new();
    for deadline in deadlines {
        waits.push(wait_for_branch_buffer_deadline(clock, deadline));
    }
    waits
        .next()
        .await
        .assured("a non-empty deadline set always contains one wait future")
}

pub(super) async fn wait_for_branch_buffer_deadline(
    clock: &DomainClock,
    deadline: BranchBufferDeadline,
) -> BranchBufferTimingResult<()> {
    match deadline {
        BranchBufferDeadline::Logical(deadline) => {
            let cancellation = CancellationToken::new();
            clock
                .wait_until(deadline, &cancellation)
                .await
                .change_context(BranchBufferTimingError::LogicalDeadline)?;
        }
        BranchBufferDeadline::Physical(deadline) => {
            PhysicalDeadlineCapability::new().wait_until(deadline).await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{domain, test_domain_clock};

    #[test]
    fn each_and_immediate_keep_distinct_deadline_coordinates() {
        let clock = test_domain_clock(&domain("buffer_timing"));
        let snapshot = clock
            .snapshot()
            .assured("the fixture installs a running unpaced clock");

        let mut each = BranchBufferTimer::default();
        each.arm_flush(
            RuntimeFlushPolicy::Each {
                interval: Duration::from_secs(1),
                max_batch_size: 10,
            },
            &clock,
            &snapshot,
        )
        .assured("logical deadline construction is infallible");
        assert!(matches!(
            each.deadline(),
            Some(BranchBufferDeadline::Logical(_))
        ));

        let mut immediate = BranchBufferTimer::default();
        immediate
            .arm_flush(RuntimeFlushPolicy::Immediate, &clock, &snapshot)
            .assured("the fixture timeout fits the monotonic clock range");
        assert!(matches!(
            immediate.deadline(),
            Some(BranchBufferDeadline::Physical(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn immediate_starts_once_and_reaches_the_physical_minimum() {
        let clock = test_domain_clock(&domain("immediate_timing"));
        let snapshot = clock
            .snapshot()
            .assured("the fixture installs a running unpaced clock");
        let mut timer = BranchBufferTimer::default();
        timer
            .arm_flush(RuntimeFlushPolicy::Immediate, &clock, &snapshot)
            .assured("the fixture timeout fits the monotonic clock range");

        tokio::time::advance(Duration::from_micros(50)).await;
        timer
            .arm_flush(RuntimeFlushPolicy::Immediate, &clock, &snapshot)
            .assured("an armed timer does not schedule a second deadline");
        assert!(
            !timer
                .is_due(&clock, &snapshot)
                .assured("the bound clock remains installed")
        );

        tokio::time::advance(Duration::from_micros(50)).await;
        assert!(
            timer
                .is_due(&clock, &snapshot)
                .assured("the bound clock remains installed")
        );
        timer.clear();
        assert!(
            !timer
                .is_due(&clock, &snapshot)
                .assured("a cleared timer has no deadline")
        );
    }

    #[test]
    fn only_each_has_a_size_boundary() {
        assert!(
            RuntimeFlushPolicy::Each {
                interval: Duration::from_secs(1),
                max_batch_size: 8,
            }
            .size_boundary_reached(8)
        );
        assert!(!RuntimeFlushPolicy::Immediate.size_boundary_reached(u64::MAX));
    }
}
