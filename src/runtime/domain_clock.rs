//! Runtime adapters for validated domain-clock mappings and progress delivery.
//!
//! Layer: data plane.
//!
//! - **Owns.** Installing committed mappings, observing progress and adapting clock arithmetic to
//!   runtime lifecycle decisions.
//! - **Depends on.** Vocabulary clock models and branch-local runtime state.
//! - **Must not know.** NSPL parsing, consensus decisions or clock-authority selection.

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
#[cfg(test)]
use nervix_models::DomainTick;
use nervix_models::{
    DomainAdmissionWindow, DomainClockAuthority, DomainClockPeriod, DomainClockProgress,
    DomainClockState, DomainName, DomainPace, DomainState, Timestamp,
};
#[cfg(test)]
use nervix_wasm::WasmExecutionContext;
use thiserror::Error;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use triomphe::Arc;

#[cfg(test)]
use super::VmExecutionContext;
use super::{ObservedDomainTick, Runtime};
use crate::runtime::physical_time::{PhysicalDeadlineCapability, actual_utc_now};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum DomainClockAccessError {
    #[error("domain '{domain}' is missing from this runtime")]
    Missing { domain: DomainName },
    #[error("domain '{domain}' clock generation {generation} is stopped")]
    Stopped { domain: DomainName, generation: u64 },
    #[error("domain '{domain}' clock generation {generation} is not installed")]
    Uninstalled { domain: DomainName, generation: u64 },
    #[error(
        "domain '{domain}' clock generation {bound_generation} is stale; current generation is \
         {current_generation}"
    )]
    StaleGeneration {
        domain: DomainName,
        bound_generation: u64,
        current_generation: u64,
    },
    #[error(
        "logical deadline for domain '{deadline_domain}' cannot be used by domain '{clock_domain}'"
    )]
    DeadlineDomainMismatch {
        clock_domain: DomainName,
        deadline_domain: DomainName,
    },
    #[error("domain '{domain}' clock arithmetic failed during {operation}")]
    Arithmetic {
        domain: DomainName,
        operation: DomainClockArithmetic,
    },
    #[error("domain '{domain}' has invalid ingestion timing configuration")]
    AdmissionConfiguration { domain: DomainName },
    #[error("domain '{domain}' has invalid cadence interval '{interval}'")]
    CadenceConfiguration {
        domain: DomainName,
        interval: String,
    },
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DomainClockArithmetic {
    #[error("logical-time projection")]
    Projection,
    #[error("logical-deadline conversion")]
    DeadlineConversion,
    #[error("cadence scheduling")]
    CadenceScheduling,
}

pub(in crate::runtime) type DomainClockAccessResult<T> = Result<T, Report<DomainClockAccessError>>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainClockWaitError {
    #[error("domain '{domain}' clock became unavailable while waiting for a logical deadline")]
    Clock { domain: DomainName },
    #[error("domain '{domain}' logical deadline wait was cancelled")]
    Cancelled { domain: DomainName },
    #[error("domain '{domain}' physical deadline could not be scheduled")]
    PhysicalDeadline { domain: DomainName },
}

pub type DomainClockWaitResult<T> = Result<T, Report<DomainClockWaitError>>;

#[derive(Debug, Clone, PartialEq, Eq)]
enum DomainClockSource {
    Unpaced,
    Paced(DomainClockState),
}

impl DomainClockSource {
    fn now(&self, domain: &DomainName, wall_now: Timestamp) -> DomainClockAccessResult<Timestamp> {
        match self {
            Self::Unpaced => Ok(wall_now),
            Self::Paced(mapping) => mapping.logical_time_at(wall_now).change_context(
                DomainClockAccessError::Arithmetic {
                    domain: domain.clone(),
                    operation: DomainClockArithmetic::Projection,
                },
            ),
        }
    }

    fn physical_duration_until(
        &self,
        domain: &DomainName,
        current: Timestamp,
        target: Timestamp,
    ) -> DomainClockAccessResult<Duration> {
        match self {
            Self::Unpaced => Ok(target.duration_since(current).unwrap_or(Duration::ZERO)),
            Self::Paced(mapping) => mapping.wall_duration_until(current, target).change_context(
                DomainClockAccessError::Arithmetic {
                    domain: domain.clone(),
                    operation: DomainClockArithmetic::DeadlineConversion,
                },
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DomainClockInstallation {
    Missing,
    Stopped {
        generation: u64,
    },
    Uninstalled {
        generation: u64,
    },
    Installed {
        generation: u64,
        source: DomainClockSource,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DomainClockBinding {
    Active,
    Passive,
}

impl DomainClockInstallation {
    const fn generation(&self) -> Option<u64> {
        match self {
            Self::Missing => None,
            Self::Stopped { generation }
            | Self::Uninstalled { generation }
            | Self::Installed { generation, .. } => Some(*generation),
        }
    }
}

#[derive(Debug)]
struct DomainClockSharedState {
    installation: DomainClockInstallation,
    last_read: Option<DomainExecutionSnapshot>,
    change_version: u64,
}

#[derive(Debug)]
struct DomainClockInner {
    domain: DomainName,
    state: parking_lot::Mutex<DomainClockSharedState>,
    changes: watch::Sender<u64>,
}

/// The lifecycle owner for one domain clock on one runtime node.
///
/// Bound execution capabilities and lifecycle updates share this one allocation. Updating the
/// installation therefore wakes every waiter without copying a mapping into task-local state.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct DomainClockLifecycle {
    inner: Arc<DomainClockInner>,
}

impl DomainClockLifecycle {
    pub(in crate::runtime) fn new(domain: DomainName) -> Self {
        let (changes, _) = watch::channel(0);
        Self {
            inner: Arc::new(DomainClockInner {
                domain,
                state: parking_lot::Mutex::new(DomainClockSharedState {
                    installation: DomainClockInstallation::Missing,
                    last_read: None,
                    change_version: 0,
                }),
                changes,
            }),
        }
    }

    pub(in crate::runtime) fn synchronize(
        &self,
        state: &DomainState,
        authority: &DomainClockAuthority,
    ) {
        if let nervix_models::DomainStatus::Paused = state.status
            && let DomainPace::Paced = state.config.pace
            && state.clock.is_none()
            && authority.owner().is_some()
        {
            let shared = self.inner.state.lock();
            if matches!(
                &shared.installation,
                DomainClockInstallation::Installed {
                    generation,
                    source: DomainClockSource::Paced(_),
                } if *generation == state.start_version
            ) {
                return;
            }
        }

        let installation = match state.status {
            nervix_models::DomainStatus::Stopped => DomainClockInstallation::Stopped {
                generation: state.start_version,
            },
            nervix_models::DomainStatus::Running | nervix_models::DomainStatus::Paused => {
                match state.config.pace {
                    DomainPace::Unpaced => DomainClockInstallation::Installed {
                        generation: state.start_version,
                        source: DomainClockSource::Unpaced,
                    },
                    DomainPace::Paced => match (&state.clock, authority.owner()) {
                        (Some(mapping), Some(_)) => DomainClockInstallation::Installed {
                            generation: state.start_version,
                            source: DomainClockSource::Paced(mapping.clone()),
                        },
                        (None, _) | (_, None) => DomainClockInstallation::Uninstalled {
                            generation: state.start_version,
                        },
                    },
                }
            }
        };
        self.replace(installation);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn install_paced(&self, generation: u64, mapping: DomainClockState) {
        self.replace(DomainClockInstallation::Installed {
            generation,
            source: DomainClockSource::Paced(mapping),
        });
    }

    #[cfg(test)]
    pub(in crate::runtime) fn stop(&self, generation: u64) {
        self.replace(DomainClockInstallation::Stopped { generation });
    }

    pub(in crate::runtime) fn mark_missing(&self) {
        self.replace(DomainClockInstallation::Missing);
    }

    pub(in crate::runtime) fn bind(&self) -> DomainClockAccessResult<DomainClock> {
        self.bind_for(DomainClockBinding::Active)
    }

    /// Binds the generation carried by a passive execution for a stopped domain.
    ///
    /// Passive executions own model and routing state but run no domain work. Their clock handle
    /// therefore preserves domain and generation identity while every attempted read continues to
    /// return the typed `Stopped` outcome.
    pub(super) fn bind_passive(&self) -> DomainClockAccessResult<DomainClock> {
        self.bind_for(DomainClockBinding::Passive)
    }

    fn bind_for(&self, binding: DomainClockBinding) -> DomainClockAccessResult<DomainClock> {
        let shared = self.inner.state.lock();
        let generation = match shared.installation {
            DomainClockInstallation::Missing => {
                return Err(Report::new(DomainClockAccessError::Missing {
                    domain: self.inner.domain.clone(),
                }));
            }
            DomainClockInstallation::Stopped { generation }
                if binding == DomainClockBinding::Active =>
            {
                return Err(Report::new(DomainClockAccessError::Stopped {
                    domain: self.inner.domain.clone(),
                    generation,
                }));
            }
            DomainClockInstallation::Stopped { generation } => generation,
            DomainClockInstallation::Uninstalled { generation } => {
                return Err(Report::new(DomainClockAccessError::Uninstalled {
                    domain: self.inner.domain.clone(),
                    generation,
                }));
            }
            DomainClockInstallation::Installed { generation, .. } => generation,
        };
        Ok(DomainClock {
            inner: self.inner.clone(),
            generation,
        })
    }

    fn replace(&self, installation: DomainClockInstallation) {
        let mut shared = self.inner.state.lock();
        if shared.installation == installation {
            return;
        }
        let generation_changed = shared.installation.generation() != installation.generation();
        shared.installation = installation;
        if generation_changed
            || !matches!(
                shared.installation,
                DomainClockInstallation::Installed { .. }
            )
        {
            shared.last_read = None;
        }
        shared.change_version = shared
            .change_version
            .checked_add(1)
            .assured("a runtime cannot install 2^64 domain clock lifecycle changes");
        self.inner.changes.send_replace(shared.change_version);
    }
}

/// A clock capability bound to one domain and one installed lifecycle generation.
#[derive(Debug, Clone)]
pub struct DomainClock {
    inner: Arc<DomainClockInner>,
    generation: u64,
}

impl DomainClock {
    pub(in crate::runtime) fn snapshot(&self) -> DomainClockAccessResult<DomainExecutionSnapshot> {
        let mut shared = self.inner.state.lock();
        self.read(&mut shared)
    }

    fn read(
        &self,
        shared: &mut DomainClockSharedState,
    ) -> DomainClockAccessResult<DomainExecutionSnapshot> {
        let wall_now = actual_utc_now();
        let source = self.source(&shared.installation)?;
        let projected = source.now(&self.inner.domain, wall_now)?;
        let now = match &shared.last_read {
            Some(previous)
                if previous.generation == self.generation && previous.now > projected =>
            {
                previous.now
            }
            _ => projected,
        };
        let snapshot = DomainExecutionSnapshot {
            generation: self.generation,
            now,
        };
        shared.last_read = Some(snapshot.clone());
        Ok(snapshot)
    }

    pub(super) fn ingestion_snapshot(
        &self,
        period: &str,
        skew: &str,
    ) -> DomainClockAccessResult<DomainIngestionSnapshot> {
        let mut shared = self.inner.state.lock();
        let snapshot = self.read(&mut shared)?;
        let window = match self.source(&shared.installation)? {
            DomainClockSource::Unpaced => None,
            DomainClockSource::Paced(mapping) => {
                let context = || DomainClockAccessError::AdmissionConfiguration {
                    domain: self.inner.domain.clone(),
                };
                let period = period
                    .parse::<DomainClockPeriod>()
                    .change_context(context())?;
                let skew = humantime::parse_duration(skew).change_context(context())?;
                Some(
                    DomainAdmissionWindow::reached(
                        mapping.logical_start(),
                        snapshot.now(),
                        period,
                        skew,
                    )
                    .assured(
                        "a projected, nondecreasing clock read never precedes its generation's \
                         origin",
                    ),
                )
            }
        };
        Ok(DomainIngestionSnapshot { snapshot, window })
    }

    pub(in crate::runtime) fn deadline_at(&self, due_at: Timestamp) -> LogicalDeadline {
        LogicalDeadline {
            domain: self.inner.domain.clone(),
            generation: self.generation,
            due_at,
        }
    }

    pub(super) fn deadline_reached(
        &self,
        deadline: &LogicalDeadline,
        snapshot: &DomainExecutionSnapshot,
    ) -> DomainClockAccessResult<bool> {
        self.revalidate()?;
        if deadline.domain != self.inner.domain {
            return Err(Report::new(
                DomainClockAccessError::DeadlineDomainMismatch {
                    clock_domain: self.inner.domain.clone(),
                    deadline_domain: deadline.domain.clone(),
                },
            ));
        }
        if deadline.generation != self.generation {
            return Err(Report::new(DomainClockAccessError::StaleGeneration {
                domain: self.inner.domain.clone(),
                bound_generation: deadline.generation,
                current_generation: self.generation,
            }));
        }
        if snapshot.generation != self.generation {
            return Err(Report::new(DomainClockAccessError::StaleGeneration {
                domain: self.inner.domain.clone(),
                bound_generation: snapshot.generation,
                current_generation: self.generation,
            }));
        }
        Ok(snapshot.now >= deadline.due_at)
    }

    pub(in crate::runtime) fn physical_duration_until(
        &self,
        current: Timestamp,
        target: Timestamp,
    ) -> DomainClockAccessResult<Duration> {
        let shared = self.inner.state.lock();
        self.source(&shared.installation)?.physical_duration_until(
            &self.inner.domain,
            current,
            target,
        )
    }

    fn revalidate(&self) -> DomainClockAccessResult<()> {
        let shared = self.inner.state.lock();
        self.source(&shared.installation)?;
        Ok(())
    }

    pub async fn wait_until(
        &self,
        deadline: LogicalDeadline,
        cancellation: &CancellationToken,
    ) -> DomainClockWaitResult<LogicalDeadlineReached> {
        if deadline.domain != self.inner.domain {
            return Err(Report::new(DomainClockAccessError::DeadlineDomainMismatch {
                clock_domain: self.inner.domain.clone(),
                deadline_domain: deadline.domain,
            })
            .change_context(DomainClockWaitError::Clock {
                domain: self.inner.domain.clone(),
            }));
        }
        if deadline.generation != self.generation {
            return Err(Report::new(DomainClockAccessError::StaleGeneration {
                domain: self.inner.domain.clone(),
                bound_generation: deadline.generation,
                current_generation: self.generation,
            })
            .change_context(DomainClockWaitError::Clock {
                domain: self.inner.domain.clone(),
            }));
        }

        let mut changes = self.inner.changes.subscribe();
        loop {
            tokio::task::consume_budget().await;
            let snapshot = self
                .snapshot()
                .change_context(DomainClockWaitError::Clock {
                    domain: self.inner.domain.clone(),
                })?;
            if snapshot.now >= deadline.due_at {
                return Ok(LogicalDeadlineReached {
                    due_at: deadline.due_at,
                    snapshot,
                });
            }
            let duration = self
                .physical_duration_until(snapshot.now, deadline.due_at)
                .change_context(DomainClockWaitError::Clock {
                    domain: self.inner.domain.clone(),
                })?;
            let physical_time = PhysicalDeadlineCapability::new();
            let physical = physical_time.after(duration).change_context(
                DomainClockWaitError::PhysicalDeadline {
                    domain: self.inner.domain.clone(),
                },
            )?;
            tokio::select! {
                _ = physical_time.wait_until(physical) => {}
                changed = changes.changed() => {
                    changed.assured(
                        "the bound clock holds its lifecycle notification sender for every waiter",
                    );
                }
                _ = cancellation.cancelled() => {
                    return Err(Report::new(DomainClockWaitError::Cancelled {
                        domain: self.inner.domain.clone(),
                    }));
                }
            }
        }
    }

    fn source(
        &self,
        installation: &DomainClockInstallation,
    ) -> DomainClockAccessResult<DomainClockSource> {
        if let Some(current_generation) = installation.generation()
            && current_generation != self.generation
        {
            return Err(Report::new(DomainClockAccessError::StaleGeneration {
                domain: self.inner.domain.clone(),
                bound_generation: self.generation,
                current_generation,
            }));
        }
        match installation {
            DomainClockInstallation::Missing => Err(Report::new(DomainClockAccessError::Missing {
                domain: self.inner.domain.clone(),
            })),
            DomainClockInstallation::Stopped { generation } => {
                Err(Report::new(DomainClockAccessError::Stopped {
                    domain: self.inner.domain.clone(),
                    generation: *generation,
                }))
            }
            DomainClockInstallation::Uninstalled { generation } => {
                Err(Report::new(DomainClockAccessError::Uninstalled {
                    domain: self.inner.domain.clone(),
                    generation: *generation,
                }))
            }
            DomainClockInstallation::Installed { source, .. } => Ok(source.clone()),
        }
    }
}

/// Delivery time and admission boundaries captured together from one installed generation.
pub(super) struct DomainIngestionSnapshot {
    pub(super) snapshot: DomainExecutionSnapshot,
    pub(super) window: Option<DomainAdmissionWindow>,
}

/// The time value handed to one VM or WASM invocation after its clock generation is validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DomainExecutionSnapshot {
    generation: u64,
    now: Timestamp,
}

impl DomainExecutionSnapshot {
    #[cfg(test)]
    pub(in crate::runtime) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) const fn now(&self) -> Timestamp {
        self.now
    }

    #[cfg(test)]
    pub(in crate::runtime) fn vm_context(&self) -> VmExecutionContext {
        VmExecutionContext {
            now: self.now,
            injector: None,
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) const fn wasm_context(&self) -> WasmExecutionContext {
        WasmExecutionContext::new(self.now)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalDeadline {
    domain: DomainName,
    generation: u64,
    due_at: Timestamp,
}

impl LogicalDeadline {
    pub(super) const fn due_at(&self) -> Timestamp {
        self.due_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalDeadlineReached {
    due_at: Timestamp,
    snapshot: DomainExecutionSnapshot,
}

#[cfg(test)]
impl LogicalDeadlineReached {
    pub(in crate::runtime) const fn due_at(&self) -> Timestamp {
        self.due_at
    }

    pub(in crate::runtime) const fn snapshot(&self) -> &DomainExecutionSnapshot {
        &self.snapshot
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DomainCadenceStart {
    Immediate,
    AfterInterval,
}

/// One occurrence on a domain-bound recurring schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DomainCadenceOccurrence {
    due_at: Timestamp,
}

impl DomainCadenceOccurrence {
    pub(super) const fn due_at(&self) -> Timestamp {
        self.due_at
    }
}

/// A recurring logical schedule bound to one installed domain-clock generation.
#[derive(Debug)]
pub(super) struct DomainCadence {
    clock: DomainClock,
    interval: DomainClockPeriod,
    next_due_at: Timestamp,
}

impl DomainCadence {
    fn new(
        clock: DomainClock,
        interval: DomainClockPeriod,
        start: DomainCadenceStart,
    ) -> DomainClockAccessResult<Self> {
        let snapshot = clock.snapshot()?;
        let next_due_at = match start {
            DomainCadenceStart::Immediate => snapshot.now(),
            DomainCadenceStart::AfterInterval => snapshot
                .now()
                .checked_add(interval.as_duration())
                .change_context(DomainClockAccessError::Arithmetic {
                    domain: clock.inner.domain.clone(),
                    operation: DomainClockArithmetic::CadenceScheduling,
                })?,
        };
        Ok(Self {
            clock,
            interval,
            next_due_at,
        })
    }

    pub(super) const fn clock(&self) -> &DomainClock {
        &self.clock
    }

    /// Returns the newest due occurrence and advances directly to the first future boundary.
    pub(super) async fn next(
        &mut self,
        cancellation: &CancellationToken,
    ) -> DomainClockWaitResult<DomainCadenceOccurrence> {
        let domain = self.clock.inner.domain.clone();
        let snapshot = self
            .clock
            .snapshot()
            .change_context(DomainClockWaitError::Clock {
                domain: domain.clone(),
            })?;
        if let Some(occurrence) =
            self.take_due(snapshot)
                .change_context(DomainClockWaitError::Clock {
                    domain: domain.clone(),
                })?
        {
            return Ok(occurrence);
        }

        let reached = self
            .clock
            .wait_until(self.clock.deadline_at(self.next_due_at), cancellation)
            .await?;
        let occurrence = self
            .take_due(reached.snapshot)
            .change_context(DomainClockWaitError::Clock { domain })?
            .assured("a completed cadence wait returns a snapshot at or after its due boundary");
        Ok(occurrence)
    }

    fn take_due(
        &mut self,
        snapshot: DomainExecutionSnapshot,
    ) -> DomainClockAccessResult<Option<DomainCadenceOccurrence>> {
        if snapshot.now() < self.next_due_at {
            return Ok(None);
        }
        let elapsed = snapshot
            .now()
            .duration_since(self.next_due_at)
            .verified("this branch requires the cadence boundary to be reached");
        let interval_nanos = u128::from(self.interval.as_nanos());
        let elapsed_intervals = elapsed.as_nanos() / interval_nanos;
        let due_offset_nanos = elapsed_intervals
            .checked_mul(interval_nanos)
            .assured("the whole-interval offset is no greater than the elapsed timestamp duration");
        let due_offset_nanos = u64::try_from(due_offset_nanos)
            .assured("the difference between two signed Unix-nanosecond timestamps fits in u64");
        let due_at = self
            .next_due_at
            .checked_add(Duration::from_nanos(due_offset_nanos))
            .assured("the coalesced due instant is no later than the observed timestamp");
        self.next_due_at = due_at
            .checked_add(self.interval.as_duration())
            .change_context(DomainClockAccessError::Arithmetic {
                domain: self.clock.inner.domain.clone(),
                operation: DomainClockArithmetic::CadenceScheduling,
            })?;
        Ok(Some(DomainCadenceOccurrence { due_at }))
    }
}

/// Waits for one branch's logical deadline on the branch's own bound clock.
///
/// An absent deadline never completes, so a supervisor may write this as one select branch and
/// let the deadline's presence gate it instead of converting logical instants to physical sleeps.
pub(super) async fn wait_for_branch_deadline(
    clock: &DomainClock,
    deadline: Option<LogicalDeadline>,
) -> DomainClockWaitResult<()> {
    let Some(deadline) = deadline else {
        std::future::pending::<()>().await;
        return Ok(());
    };
    let cancellation = CancellationToken::new();
    clock.wait_until(deadline, &cancellation).await?;
    Ok(())
}

pub(super) fn checked_add_duration_to_timestamp(base: Timestamp, duration: Duration) -> Timestamp {
    // Saturation is the meaning here: a schedule further out than the nanosecond range is already
    // further out than any timestamp this clock will reach.
    base.checked_add(duration)
        .unwrap_or_else(|_| Timestamp::from_unix_nanos(i64::MAX))
}

pub(super) fn current_timestamp() -> Timestamp {
    actual_utc_now()
}

impl Runtime {
    pub(super) fn bind_domain_cadence(
        &self,
        domain: &DomainName,
        interval: &str,
        start: DomainCadenceStart,
    ) -> DomainClockAccessResult<DomainCadence> {
        let interval = interval.parse::<DomainClockPeriod>().change_context(
            DomainClockAccessError::CadenceConfiguration {
                domain: domain.clone(),
                interval: interval.to_string(),
            },
        )?;
        DomainCadence::new(self.bind_domain_clock(domain)?, interval, start)
    }

    pub(crate) fn domain_execution_snapshot(
        &self,
        domain: &DomainName,
    ) -> DomainClockAccessResult<DomainExecutionSnapshot> {
        self.bind_domain_clock(domain)?.snapshot()
    }

    #[cfg(feature = "testing")]
    pub(crate) fn take_domain_clock_initial_elapsed(
        &self,
        domain: &DomainName,
    ) -> Option<Duration> {
        self.inner
            .fault_injection
            .take_domain_clock_initial_elapsed(domain)
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn pause_domain_clock_progress_if_armed(
        &self,
        domain: &DomainName,
        node: &nervix_models::ClusterNodeName,
    ) -> bool {
        self.inner
            .fault_injection
            .pause_domain_clock_progress_if_armed(domain, node)
            .await
    }

    #[cfg(feature = "testing")]
    pub(crate) fn mark_domain_clock_progress_delivered(
        &self,
        domain: &DomainName,
        node: &nervix_models::ClusterNodeName,
    ) {
        self.inner
            .fault_injection
            .mark_domain_clock_progress_delivered(domain, node);
    }

    pub(crate) fn handle_domain_clock_progress(
        &self,
        domain: &DomainName,
        authenticated_node: &nervix_models::ClusterNodeName,
        progress: &DomainClockProgress,
    ) -> DomainClockAccessResult<()> {
        let Some(entry) = self.inner.domains.get(domain) else {
            return Err(Report::new(DomainClockAccessError::Missing {
                domain: domain.clone(),
            }));
        };
        if matches!(&entry.status, nervix_models::DomainStatus::Stopped)
            || entry.start_version != progress.generation
            || entry.clock_authority.revision() != progress.authority_revision
            || entry.clock_authority.owner() != Some(&progress.authority)
            || progress.authority.node_id() != authenticated_node
            || progress.tick.tick_id == 0
        {
            return Ok(());
        }

        let mut observed = entry.progress.lock();
        if observed.as_ref().is_some_and(|observed| {
            observed.tick_id >= progress.tick.tick_id
                || observed.wall_clock > progress.tick.wall_clock
        }) {
            return Ok(());
        }
        *observed = Some(ObservedDomainTick {
            tick_id: progress.tick.tick_id,
            wall_clock: progress.tick.wall_clock,
        });
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn handle_domain_tick(
        &self,
        domain: &DomainName,
        tick: &DomainTick,
    ) -> DomainClockAccessResult<()> {
        let Some(entry) = self.inner.domains.get(domain) else {
            return Err(Report::new(DomainClockAccessError::Missing {
                domain: domain.clone(),
            }));
        };
        let authority = entry
            .clock_authority
            .owner()
            .cloned()
            .assured("test domain synchronization installs a clock authority");
        let progress = DomainClockProgress {
            generation: entry.start_version,
            authority_revision: entry.clock_authority.revision(),
            authority: authority.clone(),
            tick: tick.clone(),
        };
        drop(entry);
        self.handle_domain_clock_progress(domain, authority.node_id(), &progress)
    }

    pub(super) fn bind_domain_clock(
        &self,
        domain: &DomainName,
    ) -> DomainClockAccessResult<DomainClock> {
        if let Some(execution) = self.inner.executions.get(domain) {
            execution.domain_clock.revalidate()?;
            return Ok(execution.domain_clock.clone());
        }
        let Some(entry) = self.inner.domains.get(domain) else {
            return Err(Report::new(DomainClockAccessError::Missing {
                domain: domain.clone(),
            }));
        };
        entry.clock.bind()
    }

    pub(super) fn bind_passive_domain_clock(
        &self,
        domain: &DomainName,
    ) -> DomainClockAccessResult<DomainClock> {
        let Some(entry) = self.inner.domains.get(domain) else {
            return Err(Report::new(DomainClockAccessError::Missing {
                domain: domain.clone(),
            }));
        };
        entry.clock.bind_passive()
    }

    pub(crate) fn current_paced_domain_time(
        &self,
        domain: &DomainName,
    ) -> DomainClockAccessResult<Option<Timestamp>> {
        let Some(domain_state) = self.inner.domains.get(domain) else {
            return Err(Report::new(DomainClockAccessError::Missing {
                domain: domain.clone(),
            }));
        };
        if let DomainPace::Unpaced = domain_state.config.pace {
            return Ok(None);
        }
        let clock = domain_state.clock.bind()?;
        let snapshot = clock.snapshot()?;
        Ok(Some(snapshot.now()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_models::{
        ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, DomainClockAuthorityRevision,
        DomainClockProgress, DomainClockState, DomainConfig, DomainTick, DomainTimeRate,
        IngestTimestampSource, Timestamp,
    };

    use super::*;
    use crate::{
        runtime::{
            RuntimeValue, domain, named, paced_domain_state, test_domain_clock,
            test_domain_clock_authority,
        },
        runtime_schema::test_runtime_row,
    };

    #[test]
    fn progress_requires_the_committed_generation_revision_identity_and_peer() {
        let runtime = Runtime::new();
        let domain_id = domain("paced");
        let mut state = paced_domain_state("paced");
        state.start_version = 4;
        state.clock = Some(DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ));
        runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), state)]));
        let authority = test_domain_clock_authority();
        let owner = authority
            .owner()
            .cloned()
            .expect("the fixture authority is assigned");
        let tick = |tick_id, wall_clock| DomainTick {
            tick_id,
            logical_timestamp: Timestamp::from_unix_nanos(i64::try_from(tick_id).expect(
                "the fixture tick ids fit in the signed timestamp boundary representation",
            )),
            wall_clock: Timestamp::from_unix_nanos(wall_clock),
            period: "1s".parse().expect("fixture period is valid"),
        };
        let accepted = DomainClockProgress {
            generation: 4,
            authority_revision: authority.revision(),
            authority: owner.clone(),
            tick: tick(3, 30),
        };
        runtime
            .handle_domain_clock_progress(&domain_id, owner.node_id(), &accepted)
            .expect("the fixture domain exists");

        let rejected = [
            DomainClockProgress {
                generation: 3,
                tick: tick(4, 40),
                ..accepted.clone()
            },
            DomainClockProgress {
                authority_revision: DomainClockAuthorityRevision::INITIAL
                    .checked_next()
                    .expect("the initial revision has a successor"),
                tick: tick(5, 50),
                ..accepted.clone()
            },
            DomainClockProgress {
                authority: ClusterNodeIdentity::new(
                    owner.node_id().clone(),
                    ClusterNodeIncarnation::new(
                        owner
                            .incarnation()
                            .get()
                            .checked_add(1)
                            .expect("the fixture incarnation can advance"),
                    ),
                ),
                tick: tick(6, 60),
                ..accepted.clone()
            },
            DomainClockProgress {
                tick: tick(2, 20),
                ..accepted.clone()
            },
        ];
        for progress in rejected {
            runtime
                .handle_domain_clock_progress(&domain_id, owner.node_id(), &progress)
                .expect("a fenced progress message is safely ignored");
        }
        let wrong_peer = ClusterNodeName::parse("another-node").expect("fixture name is valid");
        runtime
            .handle_domain_clock_progress(&domain_id, &wrong_peer, &accepted)
            .expect("an unauthenticated authority claim is safely ignored");

        let observed = runtime
            .inner
            .domains
            .get(&domain_id)
            .expect("the fixture domain remains installed");
        let progress = observed.progress.lock();
        assert_eq!(progress.as_ref().map(|tick| tick.tick_id), Some(3));
    }

    #[test]
    fn progress_never_creates_a_missing_domain() {
        let runtime = Runtime::new();
        let domain_id = domain("missing");
        let authority = ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-1").expect("fixture name is valid"),
            ClusterNodeIncarnation::new(1),
        );
        let progress = DomainClockProgress {
            generation: 1,
            authority_revision: DomainClockAuthorityRevision::INITIAL,
            authority: authority.clone(),
            tick: DomainTick {
                tick_id: 1,
                logical_timestamp: Timestamp::from_unix_nanos(0),
                wall_clock: Timestamp::from_unix_nanos(0),
                period: "1s".parse().expect("fixture period is valid"),
            },
        };

        let error = runtime
            .handle_domain_clock_progress(&domain_id, authority.node_id(), &progress)
            .expect_err("progress for an unknown domain must be rejected");

        assert!(matches!(
            error.current_context(),
            DomainClockAccessError::Missing { domain } if domain == &domain_id
        ));
        assert!(runtime.inner.domains.is_empty());
    }

    #[test]
    fn stopped_and_restarted_generations_ignore_delayed_progress() {
        let runtime = Runtime::new();
        let domain_id = domain("paced");
        let first_mapping = DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        );
        let mut first = paced_domain_state("paced");
        first.start_version = 4;
        first.clock = Some(first_mapping);
        runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), first.clone())]));
        let authority = test_domain_clock_authority();
        let owner = authority
            .owner()
            .cloned()
            .expect("the fixture authority is assigned");
        let delayed = DomainClockProgress {
            generation: 4,
            authority_revision: authority.revision(),
            authority: owner.clone(),
            tick: DomainTick {
                tick_id: 1,
                logical_timestamp: Timestamp::from_unix_nanos(1),
                wall_clock: Timestamp::from_unix_nanos(1),
                period: "1s".parse().expect("fixture period is valid"),
            },
        };

        first.status = nervix_models::DomainStatus::Stopped;
        first.clock = None;
        runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), first)]));
        runtime
            .handle_domain_clock_progress(&domain_id, owner.node_id(), &delayed)
            .expect("stopped generations safely ignore progress");

        let next_mapping = DomainClockState::new(
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(1_000),
            DomainTimeRate::ONE,
        );
        let mut next = paced_domain_state("paced");
        next.start_version = 5;
        next.clock = Some(next_mapping.clone());
        runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), next)]));
        runtime
            .handle_domain_clock_progress(&domain_id, owner.node_id(), &delayed)
            .expect("earlier generations safely ignore delayed progress");

        let observed = runtime
            .inner
            .domains
            .get(&domain_id)
            .expect("the restarted domain remains installed");
        assert!(observed.progress.lock().is_none());
        let installed = observed.clock.inner.state.lock();
        assert!(matches!(
            &installed.installation,
            DomainClockInstallation::Installed {
                generation: 5,
                source: DomainClockSource::Paced(mapping),
            } if mapping == &next_mapping
        ));
    }

    #[test]
    fn coalesced_generation_transition_discards_prior_progress() {
        let runtime = Runtime::new();
        let domain_id = domain("paced");
        let mut first = paced_domain_state("paced");
        first.start_version = 4;
        first.clock = Some(DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ));
        runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), first)]));
        let authority = test_domain_clock_authority();
        let owner = authority
            .owner()
            .cloned()
            .expect("the fixture authority is assigned");
        let progress = |generation, tick_id| DomainClockProgress {
            generation,
            authority_revision: authority.revision(),
            authority: owner.clone(),
            tick: DomainTick {
                tick_id,
                logical_timestamp: Timestamp::from_unix_nanos(
                    i64::try_from(tick_id).expect("fixture tick ids fit in a timestamp"),
                ),
                wall_clock: Timestamp::from_unix_nanos(
                    i64::try_from(tick_id).expect("fixture tick ids fit in a timestamp"),
                ),
                period: "1s".parse().expect("fixture period is valid"),
            },
        };
        runtime
            .handle_domain_clock_progress(&domain_id, owner.node_id(), &progress(4, 50))
            .expect("the first generation accepts its progress");

        let mut next = paced_domain_state("paced");
        next.start_version = 5;
        next.clock = Some(DomainClockState::new(
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(1_000),
            DomainTimeRate::ONE,
        ));
        runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), next)]));

        let observed = runtime
            .inner
            .domains
            .get(&domain_id)
            .expect("the later generation remains installed");
        assert!(
            observed.progress.lock().is_none(),
            "progress retained from the skipped STOP belongs to the previous generation"
        );
        drop(observed);
        runtime
            .handle_domain_clock_progress(&domain_id, owner.node_id(), &progress(5, 1))
            .expect("the later generation accepts its first progress");
        let observed = runtime
            .inner
            .domains
            .get(&domain_id)
            .expect("the later generation remains installed");
        assert_eq!(
            observed.progress.lock().as_ref().map(|tick| tick.tick_id),
            Some(1)
        );
    }

    #[test]
    fn progress_retains_the_latest_accepted_report() {
        let runtime = Runtime::new();
        let domain_id = domain("paced");
        let mut state = paced_domain_state("paced");
        state.start_version = 4;
        state.clock = Some(DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ));
        runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), state)]));
        let authority = test_domain_clock_authority();
        let owner = authority
            .owner()
            .cloned()
            .expect("the fixture authority is assigned");
        let final_tick = 300_u64;
        for tick_id in 1..=final_tick {
            let timestamp = i64::try_from(tick_id).expect("fixture tick ids fit in a timestamp");
            runtime
                .handle_domain_clock_progress(
                    &domain_id,
                    owner.node_id(),
                    &DomainClockProgress {
                        generation: 4,
                        authority_revision: authority.revision(),
                        authority: owner.clone(),
                        tick: DomainTick {
                            tick_id,
                            logical_timestamp: Timestamp::from_unix_nanos(timestamp),
                            wall_clock: Timestamp::from_unix_nanos(timestamp),
                            period: "1s".parse().expect("fixture period is valid"),
                        },
                    },
                )
                .expect("the current authority progress is accepted");
        }

        let observed = runtime
            .inner
            .domains
            .get(&domain_id)
            .expect("the domain remains installed");
        let progress = observed.progress.lock();
        assert_eq!(progress.as_ref().map(|tick| tick.tick_id), Some(final_tick));
    }

    #[test]
    fn delayed_progress_delivery_does_not_move_logical_time_backwards() {
        let clock = DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        );
        let delayed_wall_time = Timestamp::from_unix_nanos(1_000_000_000);
        let before_delivery = clock
            .logical_time_at(delayed_wall_time)
            .assured("the fixture uses a finite positive rate");
        let after_delivery = clock
            .logical_time_at(delayed_wall_time)
            .assured("the fixture uses a finite positive rate");

        assert!(
            after_delivery >= before_delivery,
            "delivering progress moved logical time from {before_delivery} to {after_delivery}"
        );
    }

    #[test]
    fn paced_domains_admit_the_logical_origin() {
        let runtime = Runtime::new();
        let mut domains = BTreeMap::new();
        let mut state = paced_domain_state("paced");
        state.clock = Some(DomainClockState::new(
            current_timestamp(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ));
        domains.insert(domain("paced"), state);
        runtime.sync_domains(&domains);

        let domain = domain("paced");
        let ingestor = named("ing");
        let time = runtime
            .ingestion_time(&domain, &ingestor)
            .assured("the fixture installs a running clock");
        let record = test_runtime_row([(
            "occurred_at".to_string(),
            RuntimeValue::Datetime(Timestamp::from_unix_nanos(0).into_datetime().fixed_offset()),
        )]);
        let admission = time.select(
            Some(&IngestTimestampSource::At(named("occurred_at"))),
            &record,
        );

        assert!(
            admission.is_ok(),
            "logical origin was rejected: {admission:?}"
        );
    }

    #[test]
    fn scheduled_timestamp_addition_stays_in_the_serializable_range() {
        let timestamp = checked_add_duration_to_timestamp(
            Timestamp::from_unix_nanos(i64::MAX),
            Duration::from_nanos(1),
        );

        let serialized = serde_json::to_string(&timestamp);

        assert!(
            serialized.is_ok(),
            "schedule arithmetic constructed an unserializable timestamp: {serialized:?}"
        );
    }

    #[test]
    fn logical_time_projection_reports_range_overflow() {
        let clock = DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(i64::MAX),
            DomainTimeRate::ONE,
        );

        assert!(
            clock
                .logical_time_at(Timestamp::from_unix_nanos(1))
                .is_err()
        );
    }

    #[test]
    fn logical_rate_conversion_scales_physical_waits() {
        let clock = DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::try_from(4.0).expect("fixture rate is valid"),
        );

        let wait = clock
            .wall_duration_until(
                Timestamp::from_unix_nanos(0),
                Timestamp::from_unix_nanos(1_000_000_000),
            )
            .assured("the fixture uses a finite positive rate");

        assert_eq!(wait, Duration::from_millis(250));
    }

    #[test]
    fn lifecycle_access_reports_missing_stopped_and_uninstalled_clocks() {
        let clock_domain = domain("paced");
        let lifecycle = DomainClockLifecycle::new(clock_domain.clone());

        let Err(error) = lifecycle.bind() else {
            panic!("a missing clock must reject binding");
        };
        assert!(matches!(
            error.current_context(),
            DomainClockAccessError::Missing { domain } if domain == &clock_domain
        ));

        lifecycle.stop(7);
        let Err(error) = lifecycle.bind() else {
            panic!("a stopped clock must reject binding");
        };
        assert!(matches!(
            error.current_context(),
            DomainClockAccessError::Stopped {
                domain,
                generation: 7,
            } if domain == &clock_domain
        ));

        let mut uninstalled = paced_domain_state("paced");
        uninstalled.start_version = 8;
        lifecycle.synchronize(&uninstalled, &test_domain_clock_authority());
        let Err(error) = lifecycle.bind() else {
            panic!("an uninstalled clock must reject binding");
        };
        assert!(matches!(
            error.current_context(),
            DomainClockAccessError::Uninstalled {
                domain,
                generation: 8,
            } if domain == &clock_domain
        ));
    }

    #[test]
    fn bound_clock_rejects_a_later_generation() {
        let lifecycle = DomainClockLifecycle::new(domain("paced"));
        lifecycle.install_paced(
            1,
            DomainClockState::new(
                Timestamp::from_unix_nanos(0),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
        );
        let bound = lifecycle
            .bind()
            .assured("the fixture installed generation one");

        lifecycle.install_paced(
            2,
            DomainClockState::new(
                current_timestamp(),
                Timestamp::from_unix_nanos(1),
                DomainTimeRate::ONE,
            ),
        );

        let Err(error) = bound.snapshot() else {
            panic!("a superseded clock capability must be stale");
        };
        assert!(matches!(
            error.current_context(),
            DomainClockAccessError::StaleGeneration {
                bound_generation: 1,
                current_generation: 2,
                ..
            }
        ));
    }

    #[test]
    fn reads_do_not_decrease_within_one_generation() {
        let lifecycle = DomainClockLifecycle::new(domain("paced"));
        lifecycle.install_paced(
            1,
            DomainClockState::new(
                Timestamp::from_unix_nanos(0),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
        );
        let bound = lifecycle
            .bind()
            .assured("the fixture installed generation one");
        let before = bound
            .snapshot()
            .assured("the first mapping fits the timestamp range");

        lifecycle.install_paced(
            1,
            DomainClockState::new(
                current_timestamp(),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
        );
        let after = bound
            .snapshot()
            .assured("the replacement mapping fits the timestamp range");

        assert!(after.now() >= before.now());
    }

    #[test]
    fn automatic_pause_preserves_the_installed_mapping() {
        let lifecycle = DomainClockLifecycle::new(domain("paced"));
        let mapping = DomainClockState::new(
            current_timestamp(),
            Timestamp::from_unix_nanos(10),
            DomainTimeRate::ONE,
        );
        let mut running = paced_domain_state("paced");
        running.start_version = 3;
        running.clock = Some(mapping);
        lifecycle.synchronize(&running, &test_domain_clock_authority());
        let bound = lifecycle
            .bind()
            .assured("the running state installs its committed mapping");

        let mut paused = running;
        paused.status = nervix_models::DomainStatus::Paused;
        paused.clock = None;
        lifecycle.synchronize(&paused, &test_domain_clock_authority());

        assert!(bound.snapshot().is_ok());
    }

    #[tokio::test]
    async fn cancelled_logical_wait_returns_a_typed_outcome() {
        let lifecycle = DomainClockLifecycle::new(domain("paced"));
        lifecycle.install_paced(
            1,
            DomainClockState::new(
                current_timestamp(),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
        );
        let bound = lifecycle
            .bind()
            .assured("the fixture installed generation one");
        let snapshot = bound
            .snapshot()
            .assured("the fixture mapping fits the timestamp range");
        let due_at = snapshot
            .now()
            .checked_add(Duration::from_secs(60))
            .assured("the fixture deadline fits the timestamp range");
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let result = bound
            .wait_until(bound.deadline_at(due_at), &cancellation)
            .await;

        let Err(error) = result else {
            panic!("a cancelled deadline must return cancellation");
        };
        assert!(matches!(
            error.current_context(),
            DomainClockWaitError::Cancelled { .. }
        ));
    }

    #[tokio::test]
    async fn logical_deadline_cannot_cross_domain_capabilities() {
        let first = DomainClockLifecycle::new(domain("first"));
        first.synchronize(
            &DomainState {
                id: domain("first"),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    period: "1s".to_string(),
                    skew: "0ms".to_string(),
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: nervix_models::DomainStatus::Running,
                start_version: 1,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            },
            &test_domain_clock_authority(),
        );
        let second = DomainClockLifecycle::new(domain("second"));
        second.synchronize(
            &DomainState {
                id: domain("second"),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    period: "1s".to_string(),
                    skew: "0ms".to_string(),
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: nervix_models::DomainStatus::Running,
                start_version: 1,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            },
            &test_domain_clock_authority(),
        );
        let first = first.bind().assured("the first unpaced clock is installed");
        let second = second
            .bind()
            .assured("the second unpaced clock is installed");

        let result = first
            .wait_until(
                second.deadline_at(Timestamp::from_unix_nanos(0)),
                &CancellationToken::new(),
            )
            .await;

        let Err(error) = result else {
            panic!("a logical deadline from another domain must be rejected");
        };
        let access_error = error
            .downcast_ref::<DomainClockAccessError>()
            .assured("cross-domain waits retain their typed access-error frame");
        assert!(matches!(
            access_error,
            DomainClockAccessError::DeadlineDomainMismatch {
                clock_domain,
                deadline_domain,
            } if clock_domain == &domain("first") && deadline_domain == &domain("second")
        ));
    }

    #[tokio::test]
    async fn logical_wait_revalidates_generation_after_waking() {
        let lifecycle = DomainClockLifecycle::new(domain("paced"));
        lifecycle.install_paced(
            1,
            DomainClockState::new(
                current_timestamp(),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
        );
        let bound = lifecycle
            .bind()
            .assured("the fixture installed generation one");
        let due_at = bound
            .snapshot()
            .assured("the fixture mapping fits the timestamp range")
            .now()
            .checked_add(Duration::from_secs(60))
            .assured("the fixture deadline fits the timestamp range");
        let deadline = bound.deadline_at(due_at);
        let cancellation = CancellationToken::new();
        let task_clock = bound.clone();
        let task_cancellation = cancellation.clone();
        let waiter =
            tokio::spawn(async move { task_clock.wait_until(deadline, &task_cancellation).await });
        tokio::task::yield_now().await;

        lifecycle.install_paced(
            2,
            DomainClockState::new(
                current_timestamp(),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
        );
        let result = waiter.await.assured("the clock waiter task must join");

        let Err(error) = result else {
            panic!("a generation change must invalidate the waiter");
        };
        let access_error = error
            .downcast_ref::<DomainClockAccessError>()
            .assured("clock wait failures retain their typed access-error frame");
        assert!(matches!(
            access_error,
            DomainClockAccessError::StaleGeneration {
                bound_generation: 1,
                current_generation: 2,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn due_logical_wait_returns_due_time_and_a_fresh_snapshot() {
        let clock_domain = domain("unpaced");
        let lifecycle = DomainClockLifecycle::new(clock_domain.clone());
        lifecycle.synchronize(
            &DomainState {
                id: clock_domain,
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    period: "1s".to_string(),
                    skew: "0ms".to_string(),
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: nervix_models::DomainStatus::Running,
                start_version: 4,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            },
            &test_domain_clock_authority(),
        );
        let bound = lifecycle
            .bind()
            .assured("the fixture installs an unpaced clock");
        let due_at = Timestamp::from_unix_nanos(0);

        let reached = bound
            .wait_until(bound.deadline_at(due_at), &CancellationToken::new())
            .await
            .assured("the deadline is already due");

        assert_eq!(reached.due_at(), due_at);
        assert_eq!(reached.snapshot().generation(), 4);
        assert!(reached.snapshot().now() >= due_at);
        assert_eq!(
            reached.snapshot().vm_context().now,
            reached.snapshot().now()
        );
        assert_eq!(
            reached.snapshot().wasm_context().now(),
            reached.snapshot().now()
        );
    }

    #[test]
    fn cadence_coalesces_missed_occurrences_to_the_newest_due_boundary() {
        let clock = test_domain_clock(&domain("cadence"));
        let mut cadence = DomainCadence {
            clock: clock.clone(),
            interval: "10ns".parse().assured("fixture cadence is valid"),
            next_due_at: Timestamp::from_unix_nanos(100),
        };
        let snapshot = DomainExecutionSnapshot {
            generation: clock.generation,
            now: Timestamp::from_unix_nanos(145),
        };

        let occurrence = cadence
            .take_due(snapshot)
            .assured("fixture cadence arithmetic stays in range")
            .assured("the fixture snapshot reaches the cadence");

        assert_eq!(occurrence.due_at(), Timestamp::from_unix_nanos(140));
        assert_eq!(cadence.next_due_at, Timestamp::from_unix_nanos(150));
    }

    #[test]
    fn cadence_advances_directly_across_the_complete_timestamp_range() {
        let clock = test_domain_clock(&domain("fast_cadence"));
        let mut cadence = DomainCadence {
            clock: clock.clone(),
            interval: "1ns".parse().assured("fixture cadence is valid"),
            next_due_at: Timestamp::from_unix_nanos(i64::MIN),
        };
        let snapshot = DomainExecutionSnapshot {
            generation: clock.generation,
            now: Timestamp::from_unix_nanos(i64::MAX - 1),
        };

        let occurrence = cadence
            .take_due(snapshot)
            .assured("the final future boundary remains in range")
            .assured("the fixture snapshot reaches the cadence");

        assert_eq!(
            occurrence.due_at(),
            Timestamp::from_unix_nanos(i64::MAX - 1)
        );
        assert_eq!(cadence.next_due_at, Timestamp::from_unix_nanos(i64::MAX));
    }

    #[test]
    fn cadence_reports_a_schedule_without_a_representable_future_boundary() {
        let clock = test_domain_clock(&domain("bounded_cadence"));
        let mut cadence = DomainCadence {
            clock: clock.clone(),
            interval: "1ns".parse().assured("fixture cadence is valid"),
            next_due_at: Timestamp::from_unix_nanos(i64::MAX),
        };
        let snapshot = DomainExecutionSnapshot {
            generation: clock.generation,
            now: Timestamp::from_unix_nanos(i64::MAX),
        };

        let Err(error) = cadence.take_due(snapshot) else {
            panic!("a cadence at the timestamp limit must have no future boundary");
        };

        assert!(matches!(
            error.downcast_ref::<DomainClockAccessError>(),
            Some(DomainClockAccessError::Arithmetic {
                operation: DomainClockArithmetic::CadenceScheduling,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn cadence_wait_revalidates_its_bound_generation() {
        let clock_domain = domain("cadence_generation");
        let lifecycle = DomainClockLifecycle::new(clock_domain);
        lifecycle.install_paced(
            1,
            DomainClockState::new(
                current_timestamp(),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
        );
        let clock = lifecycle
            .bind()
            .assured("fixture clock generation is installed");
        let cadence = DomainCadence::new(
            clock,
            "1h".parse().assured("fixture cadence is valid"),
            DomainCadenceStart::AfterInterval,
        )
        .assured("fixture cadence starts inside the timestamp range");
        let wait = tokio::spawn(async move {
            let mut cadence = cadence;
            cadence.next(&CancellationToken::new()).await
        });
        tokio::task::yield_now().await;

        lifecycle.install_paced(
            2,
            DomainClockState::new(
                current_timestamp(),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
        );
        let result = wait.await.assured("fixture cadence task joins");
        let Err(error) = result else {
            panic!("the prior generation must not complete its cadence wait");
        };

        assert!(matches!(
            error.downcast_ref::<DomainClockAccessError>(),
            Some(DomainClockAccessError::StaleGeneration {
                bound_generation: 1,
                current_generation: 2,
                ..
            })
        ));
    }

    #[test]
    fn lifecycle_and_execution_handles_share_one_state_allocation() {
        let lifecycle = DomainClockLifecycle::new(domain("paced"));
        lifecycle.install_paced(
            1,
            DomainClockState::new(
                current_timestamp(),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
        );
        let bound = lifecycle
            .bind()
            .assured("the fixture installed generation one");

        assert!(Arc::ptr_eq(&lifecycle.inner, &bound.inner));
    }

    #[test]
    fn logical_clock_source_has_no_direct_wall_clock_imports() {
        let (product_source, _) = include_str!("domain_clock.rs")
            .split_once("#[cfg(test)]")
            .assured("the module has a test boundary");

        for forbidden in ["tokio::time", "Instant::", "Timestamp::now"] {
            assert!(
                !product_source.contains(forbidden),
                "logical clock source directly imports physical time through '{forbidden}'"
            );
        }
    }
}
