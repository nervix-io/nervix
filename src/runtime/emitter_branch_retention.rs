//! The lifetimes of source branches whose work an emitter still holds.
//!
//! Layer: data plane.
//! - **Owns.** Source- and partition-local activity, TTL and LRU retirement, and the physical
//!   maintenance wake while branch work is held, including cancellation of an expired input's
//!   processing and removal of retired buffer and retry acknowledgement ownership.
//! - **Depends on.** Typed emitter input plans, the branch instance registry, concrete branch
//!   keys, the domain clock consumer, physical deadline capabilities, and the emitter host
//!   buffer, retry and acknowledgement services.
//! - **Must not know.** Models, connector I/O, encoding, persistence or graph validation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::{collections::BTreeMap, convert::Infallible, num::NonZeroUsize, time::Duration};

use ahash::HashSet;
use error_stack::{Report, ResultExt as _};
use futures_util::pin_mut;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_branch_instances::BranchInstanceRegistry;
use nervix_connector::physical_time::{PhysicalDeadline, PhysicalDeadlineCapability};
use nervix_models::{RelayName, Timestamp, Unit};
use nervix_primitives::{sync::watch, time::Instant};
use thiserror::Error;

use super::{EmitterConnectorContext, EmitterTaskLoop, EmitterTaskState};
use crate::{
    emitter_execution_plan::EmitterInputPlan,
    runtime::{
        BranchKey, DomainClock, EmitterPublishBatch, NodeQuiesceWorkGuard, RelayPartitioning,
        RelayRecordBatch, branch_buffering::RuntimeWake,
    },
};

#[derive(Debug, Error)]
pub(super) enum EmitterBranchRetentionError {
    #[error("the emitter could not read domain time for branch retention")]
    Clock,
    #[error("the emitter branch maintenance deadline is outside the monotonic clock range")]
    ScanDeadline,
    #[error("branched emitter input '{relay}' has no concrete branch key")]
    MissingBranch { relay: RelayName },
}

type RetentionResult<T> = Result<T, Report<EmitterBranchRetentionError>>;

/// A retirement changes source branches together, without copying identities per buffered row.
#[derive(Default)]
pub(in crate::runtime) struct EmitterBranchRetirements {
    branches: BTreeMap<RelayName, HashSet<BranchKey>>,
}

impl EmitterBranchRetirements {
    pub(in crate::runtime) fn insert(
        &mut self,
        source: RelayName,
        keys: impl IntoIterator<Item = BranchKey>,
    ) {
        self.branches.entry(source).or_default().extend(keys);
    }
    pub(in crate::runtime) fn is_empty(&self) -> bool {
        self.branches.is_empty()
    }

    pub(in crate::runtime) fn contains(&self, source: &RelayName, key: &Option<BranchKey>) -> bool {
        key.as_ref().is_some_and(|key| {
            self.branches
                .get(source)
                .is_some_and(|keys| keys.contains(key))
        })
    }
}

#[derive(Default)]
pub(super) struct EmitterBranchAdmission {
    pub(super) created: bool,
    pub(super) retired: EmitterBranchRetirements,
}

struct EmitterSourceBranches {
    partitioning: RelayPartitioning,
    ttl: Duration,
    capacity: Option<NonZeroUsize>,
    units: BTreeMap<Unit, BranchInstanceRegistry<BranchKey, ()>>,
}

pub(super) struct EmitterBranchRetention {
    sources: BTreeMap<RelayName, EmitterSourceBranches>,
    scan_interval: Duration,
    next_scan: Option<PhysicalDeadline>,
}

impl EmitterBranchRetention {
    pub(super) fn new(inputs: &[EmitterInputPlan], scan_interval: Duration) -> Self {
        let mut sources = BTreeMap::new();
        for input in inputs {
            let Some(partitions) = input.partitions else {
                continue;
            };
            let ttl = input
                .retention
                .branch_ttl
                .verified("the activation plan resolves a TTL for every declared source branch");
            sources.insert(
                input.relay.clone(),
                EmitterSourceBranches {
                    partitioning: RelayPartitioning::of_partitions(partitions),
                    ttl,
                    capacity: input.retention.branch_capacity,
                    units: BTreeMap::new(),
                },
            );
        }
        Self {
            sources,
            scan_interval,
            next_scan: None,
        }
    }

    pub(super) fn tracks(&self, source: &RelayName) -> bool {
        self.sources.contains_key(source)
    }

    pub(super) fn wake(&self, wake: RuntimeWake) -> RuntimeWake {
        match self.next_scan {
            Some(deadline) => wake.with_physical(deadline),
            None => wake,
        }
    }

    pub(super) fn is_due(&self) -> bool {
        self.next_scan
            .is_some_and(|deadline| PhysicalDeadlineCapability::operational().is_reached(deadline))
    }

    fn has_work(&self) -> bool {
        self.sources.values().any(|source| !source.units.is_empty())
    }

    /// A deferred scan is physical maintenance, including while a stop owns the task's deadline.
    pub(super) fn defer_scan(&mut self) -> RetentionResult<()> {
        // A failed deadline construction must not leave an overdue wake spinning the task.
        self.next_scan = None;
        self.next_scan = if self.has_work() {
            Some(
                PhysicalDeadlineCapability::operational()
                    .after(self.scan_interval)
                    .change_context(EmitterBranchRetentionError::ScanDeadline)?,
            )
        } else {
            None
        };
        Ok(())
    }

    pub(super) fn admit(
        &mut self,
        source: &RelayName,
        key: &Option<BranchKey>,
        clock: &DomainClock,
    ) -> RetentionResult<EmitterBranchAdmission> {
        if !self.tracks(source) {
            return Ok(EmitterBranchAdmission::default());
        }
        let now = clock
            .snapshot()
            .change_context(EmitterBranchRetentionError::Clock)?
            .now();
        let deadline = match self.next_scan {
            Some(deadline) => deadline,
            None => PhysicalDeadlineCapability::operational()
                .after(self.scan_interval)
                .change_context(EmitterBranchRetentionError::ScanDeadline)?,
        };
        let admission = self.record_activity(source, key, now)?;
        self.next_scan = Some(deadline);
        Ok(admission)
    }

    fn record_activity(
        &mut self,
        source: &RelayName,
        key: &Option<BranchKey>,
        now: Timestamp,
    ) -> RetentionResult<EmitterBranchAdmission> {
        let Some(branches) = self.sources.get_mut(source) else {
            return Ok(EmitterBranchAdmission::default());
        };
        let concrete = key.as_ref().ok_or_else(|| {
            Report::new(EmitterBranchRetentionError::MissingBranch {
                relay: source.clone(),
            })
        })?;
        let unit = branches
            .partitioning
            .unit_of(key)
            .verified("a concrete source key was required immediately above");
        let instances = branches
            .units
            .entry(unit)
            .or_insert_with(BranchInstanceRegistry::new);
        let created = instances
            .get_or_try_create_with(concrete.clone(), now, |_, _| Ok::<(), Infallible>(()))
            .assured("branch activity tracking constructs no fallible state")
            .created;
        let mut retired = EmitterBranchRetirements::default();
        if let Some(capacity) = branches.capacity {
            let keys = instances
                .evict_lru_to_capacity(capacity)
                .into_iter()
                .map(|(key, _)| key)
                .collect::<HashSet<_>>();
            if !keys.is_empty() {
                retired.insert(source.clone(), keys);
            }
        }
        Ok(EmitterBranchAdmission { created, retired })
    }

    /// An unavailable clock keeps the work. Rescheduling first prevents an overdue scan spinning.
    pub(super) fn scan(
        &mut self,
        clock: &DomainClock,
    ) -> RetentionResult<EmitterBranchRetirements> {
        if !self.is_due() {
            return Ok(EmitterBranchRetirements::default());
        }
        self.defer_scan()?;
        let now = clock
            .snapshot()
            .change_context(EmitterBranchRetentionError::Clock)?
            .now();
        Ok(self.expire_at(now))
    }

    fn expire_at(&mut self, now: Timestamp) -> EmitterBranchRetirements {
        let mut retired = EmitterBranchRetirements::default();
        for (source, branches) in &mut self.sources {
            let mut keys = HashSet::default();
            let ttl = branches.ttl;
            branches.units.retain(|_, instances| {
                keys.extend(instances.expire(now, ttl).into_iter().map(|(key, _)| key));
                !instances.is_empty()
            });
            if !keys.is_empty() {
                retired.insert(source.clone(), keys);
            }
        }
        if !self.has_work() {
            self.next_scan = None;
        }
        retired
    }

    /// A newly admitted branch whose input produced no output holds no branch-local work.
    pub(super) fn forget(&mut self, source: &RelayName, key: &Option<BranchKey>) {
        let Some(branches) = self.sources.get_mut(source) else {
            return;
        };
        let Some(concrete) = key.as_ref() else {
            return;
        };
        let unit = branches
            .partitioning
            .unit_of(key)
            .assured("forget uses the same key the source plan admitted");
        if let Some(instances) = branches.units.get_mut(&unit) {
            instances.remove(concrete);
            if instances.is_empty() {
                branches.units.remove(&unit);
            }
        }
        if !self.has_work() {
            self.next_scan = None;
        }
    }

    pub(super) fn clear(&mut self) {
        for source in self.sources.values_mut() {
            source.units.clear();
        }
        self.next_scan = None;
    }
}

impl EmitterTaskLoop<'_> {
    /// Maintenance never owns retry, force-flush or stop progress. A stop keeps its physical
    /// deadline and buffered work, and an unreadable clock postpones expiry without rejecting it.
    pub(super) async fn maintain_branches(&mut self) {
        if !self.retention.is_due() {
            return;
        }
        let retired = self
            .context
            .scan_branch_retention(&mut self.retention, self.stop_signal);
        self.state
            .retire_source_branches(&retired, &mut |report| {
                self.batch_context.observe_sent(report);
            })
            .await;
    }

    pub(super) async fn process_input(
        &mut self,
        source: &RelayName,
        batch: RelayRecordBatch,
        wait_for_required_state: bool,
        work: Option<&mut NodeQuiesceWorkGuard>,
    ) -> Option<EmitterPublishBatch> {
        self.maintain_branches().await;
        let key = batch.key.clone();
        // Unbranched inputs add neither a retention clock read nor cloned ACK handles.
        let admission = if self.retention.tracks(source) {
            match self.retention.admit(source, &key, &self.context.clock) {
                Ok(admission) => Some(admission),
                Err(error) => {
                    self.batch_context
                        .report_general_error(batch.acks.iter(), &error);
                    return None;
                }
            }
        } else {
            None
        };
        if let Some(admission) = &admission {
            self.state
                .retire_source_branches(&admission.retired, &mut |report| {
                    self.batch_context.observe_sent(report);
                })
                .await;
        }

        let acks = admission.as_ref().map(|_| batch.acks.clone());
        let metrics = self.batch_context.output_metrics;
        let dirty = self.batch_context.metrics_dirty;
        let result = {
            let processing = self.batch_context.process(
                source,
                batch,
                &mut *self.work_cancel_rx,
                wait_for_required_state,
                work,
            );
            pin_mut!(processing);
            loop {
                nervix_primitives::task::consume_budget().await;
                let wake = self.state.keepalive.wake(
                    self.retention.wake(RuntimeWake::never()),
                    self.state.retry.is_active(),
                );
                nervix_primitives::select! {
                    result = &mut processing => break result,
                    result = wake.wait() => {
                        result.assured("a physical-only maintenance wake has no logical clock failure");
                        let retired = self.context.scan_branch_retention(
                            &mut self.retention, self.stop_signal,
                        );
                        self.state.retire_source_branches(&retired, &mut |report| {
                            metrics.observe(report);
                            dirty.mark();
                        }).await;
                        self.state.keep_buffered_work_alive();
                        if retired.contains(source, &key) {
                            // A retained ACK may also be held by the source/transport. Explicit
                            // NoACK settles it before dropping the parked processing future.
                            for ack in acks.iter().flatten() {
                                ack.no_ack("emitter source branch retired");
                            }
                            break None;
                        }
                    }
                }
            }
        };
        if result.is_none() && admission.is_some_and(|admission| admission.created) {
            self.retention.forget(source, &key);
        }
        result
    }
}

impl EmitterTaskState {
    async fn retire_source_branches(
        &mut self,
        retired: &EmitterBranchRetirements,
        observe_sent: &mut impl FnMut(&crate::runtime::PublishReport),
    ) {
        if retired.is_empty() {
            return;
        }
        self.buffer.retire_branches(retired, observe_sent).await;
        let acks = self.connector.pending_acks(&self.buffer);
        self.keepalive.refresh(&acks);
        self.retry.replace_acks(acks);
    }
}

impl EmitterConnectorContext {
    fn scan_branch_retention(
        &self,
        retention: &mut EmitterBranchRetention,
        stop_signal: &watch::Sender<Option<Instant>>,
    ) -> EmitterBranchRetirements {
        let result = if stop_signal.borrow().is_some() {
            retention
                .defer_scan()
                .map(|()| EmitterBranchRetirements::default())
        } else {
            retention.scan(&self.clock)
        };
        match result {
            Ok(retired) => retired,
            Err(error) => {
                self.record_failure(format!("{error:#}"));
                EmitterBranchRetirements::default()
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::num::NonZeroU16;

    use nervix_models::BranchPartitions;

    use super::*;
    use crate::{
        registry::PlannedRelayRetention,
        runtime::test_fixtures::{named, string_branch_key},
    };

    fn input(relay: &str, partitions: Option<BranchPartitions>) -> EmitterInputPlan {
        EmitterInputPlan {
            relay: named(relay),
            from_where: None,
            partitions,
            retention: PlannedRelayRetention {
                branch_ttl: partitions.map(|_| Duration::from_nanos(10)),
                branch_capacity: partitions.map(|_| NonZeroUsize::MIN),
            },
        }
    }

    #[test]
    fn capacity_is_local_to_each_source_and_partition_and_reuse_starts_fresh() {
        let partitions = BranchPartitions::Direct(NonZeroU16::new(2).assured("two is positive"));
        let first = named("first");
        let second = named("second");
        let mut retention = EmitterBranchRetention::new(
            &[
                input("plain", None),
                input("first", Some(partitions)),
                input("second", Some(partitions)),
            ],
            Duration::from_secs(30),
        );
        let acme = string_branch_key("tenant", "acme");
        let partitioning = RelayPartitioning::of_partitions(partitions);
        let acme_unit = partitioning
            .unit_of(&acme)
            .assured("a concrete key has a partition");
        let keys = (0..64)
            .map(|n| string_branch_key("tenant", &format!("tenant{n}")))
            .collect::<Vec<_>>();
        let same = keys
            .iter()
            .find(|key| {
                partitioning
                    .unit_of(key)
                    .assured("the fixture key is concrete")
                    == acme_unit
            })
            .assured("the fixed tenant keys exercise both partitions")
            .clone();
        let other = keys
            .iter()
            .find(|key| {
                partitioning
                    .unit_of(key)
                    .assured("the fixture key is concrete")
                    != acme_unit
            })
            .assured("the fixed tenant keys exercise both partitions")
            .clone();
        let now = Timestamp::from_unix_nanos(0);
        for (source, key) in [(&first, &acme), (&first, &other), (&second, &acme)] {
            let admission = retention
                .record_activity(source, key, now)
                .assured("the declared source receives a concrete key from the fixture");
            assert!(admission.created);
            assert!(admission.retired.is_empty());
        }
        let admission = retention
            .record_activity(&first, &same, now)
            .assured("the declared source receives a concrete key from the fixture");
        assert!(admission.retired.contains(&first, &acme));
        assert!(!admission.retired.contains(&first, &other));
        assert!(!admission.retired.contains(&second, &acme));
        let reused = retention
            .record_activity(&first, &acme, now)
            .assured("the declared source receives a concrete key from the fixture");
        assert!(reused.created);
        assert!(reused.retired.contains(&first, &same));
        assert!(!retention.tracks(&named("plain")));
    }

    #[test]
    fn expiry_uses_activity_time_and_releases_empty_units_without_completed_history() {
        let source = named("events");
        let key = string_branch_key("tenant", "acme");
        let mut retention = EmitterBranchRetention::new(
            &[input("events", Some(BranchPartitions::ONE))],
            Duration::from_secs(30),
        );
        assert!(!retention.has_work());
        retention
            .record_activity(&source, &key, Timestamp::from_unix_nanos(0))
            .assured("the fixture constructs a concrete key for its declared source");
        let touched = retention
            .record_activity(&source, &key, Timestamp::from_unix_nanos(5))
            .assured("the fixture constructs a concrete key for its declared source");
        assert!(!touched.created);
        assert!(
            retention
                .expire_at(Timestamp::from_unix_nanos(14))
                .is_empty()
        );
        assert!(
            retention
                .expire_at(Timestamp::from_unix_nanos(15))
                .contains(&source, &key)
        );
        assert!(!retention.has_work());
        assert!(retention.next_scan.is_none());
        assert!(
            retention
                .record_activity(&source, &key, Timestamp::from_unix_nanos(16))
                .assured("the fixture constructs a concrete key for its declared source")
                .created
        );
        retention.forget(&source, &key);
        assert!(!retention.has_work());
        retention
            .record_activity(&source, &key, Timestamp::from_unix_nanos(17))
            .assured("the fixture constructs a concrete key for its declared source");
        retention.clear();
        assert!(!retention.has_work());
        assert!(retention.next_scan.is_none());
    }

    #[test]
    fn an_unbranched_input_needs_neither_a_key_nor_retention_metadata() {
        let mut retention =
            EmitterBranchRetention::new(&[input("plain", None)], Duration::from_secs(30));
        let admission = retention
            .record_activity(&named("plain"), &None, Timestamp::from_unix_nanos(0))
            .assured("unbranched input");
        assert!(!admission.created);
        assert!(admission.retired.is_empty());
        assert!(!retention.has_work());
        assert!(retention.next_scan.is_none());

        let mut branched = EmitterBranchRetention::new(
            &[input("events", Some(BranchPartitions::ONE))],
            Duration::from_secs(30),
        );
        let error = branched
            .record_activity(&named("events"), &None, Timestamp::from_unix_nanos(0))
            .err()
            .assured("a branched source requires its concrete key");
        assert!(matches!(
            error.current_context(),
            EmitterBranchRetentionError::MissingBranch { .. }
        ));
        assert!(!branched.has_work());
    }

    #[nervix_primitives::test]
    async fn an_unreadable_clock_defers_maintenance_without_retiring_held_work() {
        use crate::runtime::{
            DomainClockLifecycle,
            test_fixtures::{test_domain_clock_authority, unpaced_domain_state},
        };

        let state = unpaced_domain_state("retention_clock");
        let lifecycle = DomainClockLifecycle::new(named("retention_clock"));
        lifecycle.synchronize(&state, &test_domain_clock_authority());
        let clock = lifecycle
            .bind()
            .assured("the fixture synchronizes a running unpaced clock before binding");
        let source = named("events");
        let key = string_branch_key("tenant", "acme");
        let mut retention = EmitterBranchRetention::new(
            &[input("events", Some(BranchPartitions::ONE))],
            Duration::from_secs(30),
        );
        retention
            .admit(&source, &key, &clock)
            .assured("the fixture clock remains running until admission completes");
        retention.next_scan = Some(
            PhysicalDeadlineCapability::operational()
                .after(Duration::ZERO)
                .assured("a zero duration is representable on the physical clock"),
        );
        lifecycle.stop(state.start_version);
        assert!(
            retention
                .wake(RuntimeWake::never())
                .is_reached()
                .assured("this maintenance wake contains no logical deadline")
        );
        let error = retention
            .scan(&clock)
            .err()
            .assured("stopping the bound generation makes its clock unreadable");
        assert!(matches!(
            error.current_context(),
            EmitterBranchRetentionError::Clock
        ));
        assert!(retention.has_work());
        assert!(
            !retention.is_due(),
            "an unavailable clock must not cause a busy maintenance loop"
        );
        retention.scan_interval = Duration::MAX;
        let error = retention
            .defer_scan()
            .err()
            .assured("the fixture exceeds the physical clock range");
        assert!(matches!(
            error.current_context(),
            EmitterBranchRetentionError::ScanDeadline
        ));
        assert!(
            retention.next_scan.is_none(),
            "an unrepresentable scan cannot leave an overdue wake armed"
        );
        assert!(retention.has_work());
        retention.clear();
        assert!(
            !retention
                .wake(RuntimeWake::never())
                .is_reached()
                .assured("an empty maintenance wake has no clock dependency")
        );
    }
}
