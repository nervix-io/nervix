//! Runtime adapters for validated domain-clock mappings and progress delivery.
//!
//! Layer: data plane.
//!
//! - **Owns.** Installing committed mappings, observing progress and adapting clock arithmetic to
//!   runtime lifecycle decisions.
//! - **Depends on.** Vocabulary clock models and branch-local runtime state.
//! - **Must not know.** NSPL parsing, consensus decisions or clock-authority selection.

#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicI64, Ordering};
use std::{sync::Arc as StdArc, time::Duration};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::physical_time::{PhysicalDeadlineCapability, actual_utc_now};
use nervix_execution::sync::ArcSwap;
#[cfg(test)]
use nervix_models::DomainTick;
use nervix_models::{
    DomainAdmissionWindow, DomainClockAuthority, DomainClockPeriod, DomainClockProgress,
    DomainClockSkew, DomainClockState, DomainName, DomainPace, DomainState, Timestamp,
};
#[cfg(test)]
use nervix_wasm::WasmExecutionContext;
// Shuttle schedules around the watermark's atomic maximum, so a read can be preempted between
// loading its publication and raising the watermark published with it.
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicI64, Ordering};
use thiserror::Error;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use triomphe::Arc;

#[cfg(test)]
use super::VmExecutionContext;
use super::{ObservedDomainTick, Runtime};

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
    Paced {
        mapping: DomainClockState,
        period: DomainClockPeriod,
        skew: DomainClockSkew,
    },
}

impl DomainClockSource {
    fn now(&self, domain: &DomainName, wall_now: Timestamp) -> DomainClockAccessResult<Timestamp> {
        match self {
            Self::Unpaced => Ok(wall_now),
            Self::Paced { mapping, .. } => mapping.logical_time_at(wall_now).change_context(
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
            Self::Paced { mapping, .. } => mapping
                .wall_duration_until(current, target)
                .change_context(DomainClockAccessError::Arithmetic {
                    domain: domain.clone(),
                    operation: DomainClockArithmetic::DeadlineConversion,
                }),
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

    /// The generation bound handles can read through this installation, if it is readable.
    const fn installed_generation(&self) -> Option<u64> {
        match self {
            Self::Installed { generation, .. } => Some(*generation),
            Self::Missing | Self::Stopped { .. } | Self::Uninstalled { .. } => None,
        }
    }
}

/// The latest time this node's reads have returned through the publications that share it.
///
/// Concurrent reads raise it with one atomic maximum instead of serializing on a lock. The cell
/// stores the signed Unix-nanosecond boundary form of a timestamp and accepts and returns only
/// typed timestamps.
#[derive(Debug)]
struct DomainClockReadWatermark {
    unix_nanos: AtomicI64,
}

impl DomainClockReadWatermark {
    /// Starts at the earliest representable timestamp, the identity of the maximum, so the first
    /// read returns its projection unchanged.
    const fn new() -> Self {
        Self {
            unix_nanos: AtomicI64::new(i64::MIN),
        }
    }

    /// Raises the watermark to `projected` and returns the raised watermark.
    ///
    /// Every read that returned through this watermark before this call had already raised it to
    /// its own result, so the returned time is never earlier than any of those results.
    fn raise(&self, projected: Timestamp) -> Timestamp {
        let projected_nanos = projected.unix_nanos();
        let previous_nanos = self
            .unix_nanos
            .fetch_max(projected_nanos, Ordering::Relaxed);
        Timestamp::from_unix_nanos(previous_nanos.max(projected_nanos))
    }
}

/// One domain-clock installation as bound handles read it.
///
/// Every lifecycle change publishes a replacement whole. A read therefore loads one consistent
/// installation without taking a lock, and a replacement never alters a publication that an
/// in-flight read already holds.
#[derive(Debug)]
struct DomainClockPublication {
    installation: DomainClockInstallation,
    /// Shared with a replacement only when that replacement keeps the same generation installed,
    /// so reads of that generation cannot decrease even when they race the replacement. Every
    /// other replacement starts a new watermark, which a read still holding an earlier publication
    /// cannot raise.
    watermark: Arc<DomainClockReadWatermark>,
}

impl DomainClockPublication {
    fn new(installation: DomainClockInstallation) -> Self {
        Self {
            installation,
            watermark: Arc::new(DomainClockReadWatermark::new()),
        }
    }

    /// The publication that installs `installation` in place of this one, or `None` when this
    /// publication already installs it.
    fn successor(&self, installation: &DomainClockInstallation) -> Option<Self> {
        if self.installation == *installation {
            return None;
        }
        if let Some(generation) = self.installation.installed_generation()
            && installation.installed_generation() == Some(generation)
        {
            return Some(Self {
                installation: installation.clone(),
                watermark: self.watermark.clone(),
            });
        }
        Some(Self::new(installation.clone()))
    }
}

#[derive(Debug)]
struct DomainClockInner {
    domain: DomainName,
    published: ArcSwap<DomainClockPublication>,
    /// Wakes logical waiters after every published replacement.
    changes: watch::Sender<()>,
}

/// The lifecycle owner for one domain clock on one runtime node.
///
/// Bound execution capabilities and lifecycle updates share this one allocation. Publishing an
/// installation therefore reaches every bound handle and wakes every waiter without copying a
/// mapping into task-local state.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct DomainClockLifecycle {
    inner: Arc<DomainClockInner>,
}

impl DomainClockLifecycle {
    pub(in crate::runtime) fn new(domain: DomainName) -> Self {
        let (changes, _) = watch::channel(());
        let published = ArcSwap::from_pointee(DomainClockPublication::new(
            DomainClockInstallation::Missing,
        ));
        Self {
            inner: Arc::new(DomainClockInner {
                domain,
                published,
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
            && let DomainPace::Paced { .. } = state.config.pace
            && state.clock.is_none()
            && authority.owner().is_some()
        {
            let published = self.inner.published.load();
            if matches!(
                &published.installation,
                DomainClockInstallation::Installed {
                    generation,
                    source: DomainClockSource::Paced { .. },
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
                    DomainPace::Paced { period, skew } => match (&state.clock, authority.owner()) {
                        (Some(mapping), Some(_)) => DomainClockInstallation::Installed {
                            generation: state.start_version,
                            source: DomainClockSource::Paced {
                                mapping: mapping.clone(),
                                period,
                                skew,
                            },
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
            source: DomainClockSource::Paced {
                mapping,
                period: "1s".parse().assured("one second is a valid fixture period"),
                skew: DomainClockSkew::ZERO,
            },
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
        let published = self.inner.published.load();
        let generation = match published.installation {
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

    /// Publishes `installation` unless it is already the published installation.
    ///
    /// The successor is derived from the publication it replaces and stored only while that
    /// publication is still current, so an unchanged installation wakes no waiter and a watermark
    /// is shared only across the replacement it was derived for.
    fn replace(&self, installation: DomainClockInstallation) {
        loop {
            let current = self.inner.published.load();
            let Some(successor) = current.successor(&installation) else {
                return;
            };
            let previous = self
                .inner
                .published
                .compare_and_swap(&*current, StdArc::new(successor));
            if StdArc::ptr_eq(&*previous, &*current) {
                break;
            }
        }
        self.inner.changes.send_replace(());
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
        let published = self.inner.published.load();
        let source = self.source(&published.installation)?;
        self.observe(source, &published.watermark)
    }

    /// Projects actual UTC through a source validated for this handle's generation and raises the
    /// watermark published with that source to the projection.
    fn observe(
        &self,
        source: &DomainClockSource,
        watermark: &DomainClockReadWatermark,
    ) -> DomainClockAccessResult<DomainExecutionSnapshot> {
        let wall_now = actual_utc_now();
        let projected = source.now(&self.inner.domain, wall_now)?;
        let now = watermark.raise(projected);
        Ok(DomainExecutionSnapshot {
            generation: self.generation,
            now,
        })
    }

    pub(super) fn ingestion_snapshot(&self) -> DomainClockAccessResult<DomainIngestionSnapshot> {
        let published = self.inner.published.load();
        let source = self.source(&published.installation)?;
        let snapshot = self.observe(source, &published.watermark)?;
        let window = match source {
            DomainClockSource::Unpaced => None,
            DomainClockSource::Paced {
                mapping,
                period,
                skew,
            } => Some(
                DomainAdmissionWindow::reached(
                    mapping.logical_start(),
                    snapshot.now(),
                    *period,
                    *skew,
                )
                .assured(
                    "a projected, nondecreasing clock read never precedes its generation's origin",
                ),
            ),
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
        let published = self.inner.published.load();
        let source = self.source(&published.installation)?;
        source.physical_duration_until(&self.inner.domain, current, target)
    }

    fn revalidate(&self) -> DomainClockAccessResult<()> {
        let published = self.inner.published.load();
        self.source(&published.installation)?;
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
            let physical_time = PhysicalDeadlineCapability::operational();
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

    /// Validates an installation for this handle's generation and borrows its source.
    fn source<'installation>(
        &self,
        installation: &'installation DomainClockInstallation,
    ) -> DomainClockAccessResult<&'installation DomainClockSource> {
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
            DomainClockInstallation::Installed { source, .. } => Ok(source),
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

impl Runtime {
    pub(super) fn bind_domain_cadence(
        &self,
        domain: &DomainName,
        interval: DomainClockPeriod,
        start: DomainCadenceStart,
    ) -> DomainClockAccessResult<DomainCadence> {
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
        shutdown: &CancellationToken,
    ) -> bool {
        self.inner
            .fault_injection
            .pause_domain_clock_progress_if_armed(domain, node, shutdown)
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
        let installed = observed.clock.inner.published.load();
        assert!(matches!(
            &installed.installation,
            DomainClockInstallation::Installed {
                generation: 5,
                source: DomainClockSource::Paced { mapping, .. },
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
            Timestamp::now(),
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
                Timestamp::now(),
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
                Timestamp::now(),
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
    fn a_read_racing_a_same_generation_replacement_bounds_later_reads() {
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
        let racing_publication = lifecycle.inner.published.load_full();

        lifecycle.install_paced(
            1,
            DomainClockState::new(
                Timestamp::now(),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
        );
        let racing_read = racing_publication.watermark.raise(
            "2200-01-01T00:00:00Z"
                .parse::<Timestamp>()
                .assured("the fixture timestamp is valid RFC 3339"),
        );
        let later = bound
            .snapshot()
            .assured("the replacement mapping fits the timestamp range");

        assert!(
            later.now() >= racing_read,
            "a read after the replacement returned {} before the racing read {racing_read}",
            later.now()
        );
    }

    #[test]
    fn a_read_racing_a_generation_change_cannot_clamp_the_next_generation() {
        let lifecycle = DomainClockLifecycle::new(domain("paced"));
        lifecycle.install_paced(
            1,
            DomainClockState::new(
                Timestamp::now(),
                "2010-01-01T00:00:00Z"
                    .parse()
                    .assured("the fixture timestamp is valid RFC 3339"),
                DomainTimeRate::ONE,
            ),
        );
        let racing_publication = lifecycle.inner.published.load_full();

        lifecycle.install_paced(
            2,
            DomainClockState::new(
                Timestamp::now(),
                "2000-01-01T00:00:00Z"
                    .parse()
                    .assured("the fixture timestamp is valid RFC 3339"),
                DomainTimeRate::ONE,
            ),
        );
        let bound = lifecycle
            .bind()
            .assured("the fixture installed generation two");
        racing_publication
            .watermark
            .raise(Timestamp::from_unix_nanos(i64::MAX));
        let snapshot = bound
            .snapshot()
            .assured("the second mapping fits the timestamp range");

        let generation_two_bound = "2001-01-01T00:00:00Z"
            .parse::<Timestamp>()
            .assured("the fixture timestamp is valid RFC 3339");
        assert!(
            snapshot.now() < generation_two_bound,
            "generation two read {} was clamped by a generation one read",
            snapshot.now()
        );
    }

    #[test]
    fn automatic_pause_preserves_the_installed_mapping() {
        let lifecycle = DomainClockLifecycle::new(domain("paced"));
        let mapping = DomainClockState::new(
            Timestamp::now(),
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
                Timestamp::now(),
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
                Timestamp::now(),
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
                Timestamp::now(),
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
                Timestamp::now(),
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
                Timestamp::now(),
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
                Timestamp::now(),
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

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_lifecycle_tests {
    use nervix_models::DomainTimeRate;
    use parking_lot::Mutex;
    use shuttle::thread;

    use super::*;
    use crate::{
        runtime::{domain, paced_domain_state, test_domain_clock_authority},
        shuttle_test::{check_pct, check_random},
    };

    // Shuttle does not model time, so every mapping in these models anchors its physical start at
    // the latest representable instant. Each projection is then exactly the mapping's logical
    // start: a read returns the same time in every run, and a persisted schedule replays the same
    // reads. A logical deadline years past a projection converts to a physical wait beyond the
    // one-year horizon within which Shuttle completes a sleep at once, so a waiter parked on that
    // deadline can return only through a lifecycle notification.

    /// The domain every model clock belongs to.
    const MODEL_DOMAIN: &str = "paced";
    /// The logical time the model mappings start from.
    const ORIGIN: &str = "2030-01-01T00:00:00Z";
    /// A logical time after the origin, for a generation-one mapping that projects later.
    const AFTER_ORIGIN: &str = "2031-01-01T00:00:00Z";
    /// A logical deadline whose physical wait from the origin is beyond Shuttle's sleep horizon.
    const BEYOND_SLEEP_HORIZON: &str = "2040-01-01T00:00:00Z";

    const RANDOM_ITERATIONS: usize = 200;
    const PCT_ITERATIONS: usize = 200;
    const PCT_DEPTH: usize = 3;

    /// The operations each concurrent reader performs, so that some can finish on either side of a
    /// concurrent publication.
    const READER_OPERATIONS: usize = 3;

    /// Why a join never returns a panic: the panic ends the schedule before the join resumes.
    const PANICS_END_THE_SCHEDULE: &str = "Shuttle ends the schedule when a model task panics";

    /// Explores `model` under the random scheduler and then under the PCT scheduler.
    fn check_random_and_pct(model: fn()) {
        check_random(model, RANDOM_ITERATIONS);
        check_pct(model, PCT_ITERATIONS, PCT_DEPTH);
    }

    fn model_time(rfc3339: &str) -> Timestamp {
        rfc3339
            .parse()
            .assured("every model time is a valid RFC 3339 literal")
    }

    /// A committed mapping that projects `logical_start` for every read a model makes.
    fn frozen_mapping(logical_start: &str) -> DomainClockState {
        DomainClockState::new(
            Timestamp::from_unix_nanos(i64::MAX),
            model_time(logical_start),
            DomainTimeRate::ONE,
        )
    }

    /// A running paced generation committed with a frozen mapping.
    fn running_generation(generation: u64, logical_start: &str) -> DomainState {
        let mut state = paced_domain_state(MODEL_DOMAIN);
        state.start_version = generation;
        state.clock = Some(frozen_mapping(logical_start));
        state
    }

    /// A lifecycle whose generation one is installed from `logical_start` under an assigned
    /// authority.
    fn installed_generation_one(logical_start: &str) -> DomainClockLifecycle {
        let lifecycle = DomainClockLifecycle::new(domain(MODEL_DOMAIN));
        lifecycle.synchronize(
            &running_generation(1, logical_start),
            &test_domain_clock_authority(),
        );
        lifecycle
    }

    /// The latest time returned by a read that has finished.
    #[derive(Default)]
    struct FinishedReads {
        latest: Mutex<Option<Timestamp>>,
    }

    impl FinishedReads {
        fn latest(&self) -> Option<Timestamp> {
            *self.latest.lock()
        }

        fn record(&self, now: Timestamp) {
            let mut latest = self.latest.lock();
            if *latest < Some(now) {
                *latest = Some(now);
            }
        }

        /// Reads `clock` repeatedly and requires every read to return no earlier than each read
        /// that had finished when it began.
        fn read_without_decreasing(&self, clock: &DomainClock) {
            for _ in 0..READER_OPERATIONS {
                let finished_before = self.latest();
                let snapshot = clock
                    .snapshot()
                    .assured("the model keeps generation one installed for every read");
                if let Some(finished_before) = finished_before {
                    assert!(
                        snapshot.now() >= finished_before,
                        "a read of generation one returned {} after another read had returned \
                         {finished_before}",
                        snapshot.now()
                    );
                }
                self.record(snapshot.now());
            }
        }
    }

    #[test]
    fn concurrent_reads_of_one_installed_generation_never_decrease() {
        check_random_and_pct(race_readers_against_an_earlier_mapping_of_their_generation);
    }

    /// Two readers race a replacement that keeps generation one installed but projects an earlier
    /// time, so only the read watermark the replacement shares keeps their reads from decreasing.
    fn race_readers_against_an_earlier_mapping_of_their_generation() {
        let lifecycle = installed_generation_one(AFTER_ORIGIN);
        let clock = lifecycle
            .bind()
            .assured("the model installs generation one before it binds");
        let finished = Arc::new(FinishedReads::default());

        let replacement = thread::spawn(move || lifecycle.install_paced(1, frozen_mapping(ORIGIN)));
        let first_clock = clock.clone();
        let first_finished = Arc::clone(&finished);
        let first_reader =
            thread::spawn(move || first_finished.read_without_decreasing(&first_clock));
        let second_clock = clock.clone();
        let second_finished = Arc::clone(&finished);
        let second_reader =
            thread::spawn(move || second_finished.read_without_decreasing(&second_clock));

        replacement.join().assured(PANICS_END_THE_SCHEDULE);
        first_reader.join().assured(PANICS_END_THE_SCHEDULE);
        second_reader.join().assured(PANICS_END_THE_SCHEDULE);
        finished.read_without_decreasing(&clock);
    }

    /// The refusal a generation-one handle receives once generation two is published.
    fn generation_one_replaced_by_two() -> DomainClockAccessError {
        DomainClockAccessError::StaleGeneration {
            domain: domain(MODEL_DOMAIN),
            bound_generation: 1,
            current_generation: 2,
        }
    }

    fn assert_refused_by_generation_two(error: &Report<DomainClockAccessError>) {
        assert_eq!(
            error.current_context(),
            &generation_one_replaced_by_two(),
            "generation one was refused for a reason other than its replacement"
        );
    }

    /// Revalidates a generation-one handle and tests a deadline due at its origin while generation
    /// two is published. A refusal must name the replacement, and nothing may accept the handle
    /// once anything has refused it.
    fn revalidate_across_the_generation_change(
        clock: &DomainClock,
        deadline: &LogicalDeadline,
        snapshot: &DomainExecutionSnapshot,
    ) {
        let mut refused = false;
        for _ in 0..READER_OPERATIONS {
            match clock.revalidate() {
                Ok(()) => assert!(
                    !refused,
                    "revalidation accepted generation one after its replacement was observed"
                ),
                Err(error) => {
                    assert_refused_by_generation_two(&error);
                    refused = true;
                }
            }
            match clock.deadline_reached(deadline, snapshot) {
                Ok(reached) => {
                    assert!(
                        !refused,
                        "a deadline check accepted generation one after its replacement was \
                         observed"
                    );
                    assert!(
                        reached,
                        "the model deadline is due at generation one's origin"
                    );
                }
                Err(error) => {
                    assert_refused_by_generation_two(&error);
                    refused = true;
                }
            }
        }
    }

    #[test]
    fn a_clock_bound_to_a_replaced_generation_is_refused_by_revalidation() {
        check_random_and_pct(check_a_bound_clock_across_a_generation_change);
    }

    fn check_a_bound_clock_across_a_generation_change() {
        let lifecycle = installed_generation_one(ORIGIN);
        let clock = lifecycle
            .bind()
            .assured("the model installs generation one before it binds");
        let snapshot = clock
            .snapshot()
            .assured("generation one stays installed until the replacement starts");
        let deadline = clock.deadline_at(model_time(ORIGIN));

        let replacing_lifecycle = lifecycle.clone();
        let replacement = thread::spawn(move || {
            replacing_lifecycle.synchronize(
                &running_generation(2, ORIGIN),
                &test_domain_clock_authority(),
            );
        });
        let checking_clock = clock.clone();
        let checking_deadline = deadline.clone();
        let checking_snapshot = snapshot.clone();
        let checker = thread::spawn(move || {
            revalidate_across_the_generation_change(
                &checking_clock,
                &checking_deadline,
                &checking_snapshot,
            );
        });
        replacement.join().assured(PANICS_END_THE_SCHEDULE);
        checker.join().assured(PANICS_END_THE_SCHEDULE);

        let Err(revalidation) = clock.revalidate() else {
            panic!("revalidation accepted generation one after generation two was installed");
        };
        assert_refused_by_generation_two(&revalidation);
        let Err(read) = clock.snapshot() else {
            panic!("a generation-one handle read the clock after generation two was installed");
        };
        assert_refused_by_generation_two(&read);
        let Err(deadline_check) = clock.deadline_reached(&deadline, &snapshot) else {
            panic!("a generation-one deadline was tested after generation two was installed");
        };
        assert_refused_by_generation_two(&deadline_check);
        let rebound = lifecycle
            .bind()
            .assured("generation two is installed once its synchronization returns");
        assert_eq!(rebound.generation, 2);
    }

    /// The stages of one generation, in the order the publication model publishes them.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum ObservedStage {
        Uninstalled,
        Installed,
        Stopped,
    }

    /// The installation one lifecycle read revealed, ordered as the publication model publishes
    /// installations.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum ObservedInstallation {
        Generation {
            generation: u64,
            stage: ObservedStage,
        },
        Missing,
    }

    impl ObservedInstallation {
        /// Binds a new handle, which reveals the published installation exactly.
        fn by_binding(lifecycle: &DomainClockLifecycle) -> Self {
            match lifecycle.bind() {
                Ok(clock) => Self::Generation {
                    generation: clock.generation,
                    stage: ObservedStage::Installed,
                },
                Err(error) => Self::refused(&error),
            }
        }

        /// Reads through a handle bound before the observation.
        fn by_reading(clock: &DomainClock) -> Self {
            match clock.snapshot() {
                Ok(snapshot) => Self::Generation {
                    generation: snapshot.generation(),
                    stage: ObservedStage::Installed,
                },
                Err(error) => Self::refused(&error),
            }
        }

        /// The installation a refused read revealed. A handle bound to an earlier generation sees
        /// a later one only as stale, which names the later generation but not its stage, so that
        /// refusal records the first stage the model publishes for it.
        fn refused(error: &Report<DomainClockAccessError>) -> Self {
            match error.current_context() {
                DomainClockAccessError::Missing { .. } => Self::Missing,
                DomainClockAccessError::Stopped { generation, .. } => Self::Generation {
                    generation: *generation,
                    stage: ObservedStage::Stopped,
                },
                DomainClockAccessError::Uninstalled { generation, .. } => Self::Generation {
                    generation: *generation,
                    stage: ObservedStage::Uninstalled,
                },
                DomainClockAccessError::StaleGeneration {
                    current_generation, ..
                } => Self::Generation {
                    generation: *current_generation,
                    stage: ObservedStage::Uninstalled,
                },
                DomainClockAccessError::DeadlineDomainMismatch { .. }
                | DomainClockAccessError::Arithmetic { .. } => {
                    panic!(
                        "a lifecycle read failed for a reason the model never publishes: {error:?}"
                    )
                }
            }
        }
    }

    /// The installations one reader has observed, which must follow publication order.
    #[derive(Default)]
    struct ReaderHistory {
        latest: Option<ObservedInstallation>,
    }

    impl ReaderHistory {
        fn observe(&mut self, observed: ObservedInstallation) {
            if let Some(latest) = self.latest {
                assert!(
                    observed >= latest,
                    "a reader observed {observed:?} after it had already observed {latest:?}"
                );
            }
            self.latest = Some(observed);
        }
    }

    fn bind_in_publication_order(lifecycle: &DomainClockLifecycle) {
        let mut history = ReaderHistory::default();
        for _ in 0..READER_OPERATIONS {
            history.observe(ObservedInstallation::by_binding(lifecycle));
        }
    }

    fn read_in_publication_order(clock: &DomainClock) {
        let mut history = ReaderHistory::default();
        for _ in 0..READER_OPERATIONS {
            history.observe(ObservedInstallation::by_reading(clock));
        }
    }

    /// Publishes generation one's stop, generation two without and then with an assigned
    /// authority, and the domain's removal.
    fn publish_the_lifecycle(lifecycle: &DomainClockLifecycle) {
        lifecycle.stop(1);
        lifecycle.synchronize(
            &running_generation(2, ORIGIN),
            &DomainClockAuthority::initial(),
        );
        lifecycle.synchronize(
            &running_generation(2, ORIGIN),
            &test_domain_clock_authority(),
        );
        lifecycle.mark_missing();
    }

    #[test]
    fn readers_never_observe_an_installation_older_than_one_they_observed() {
        check_random_and_pct(race_readers_against_the_published_lifecycle);
    }

    /// One reader binds new handles and another reads through a generation-one handle while the
    /// lifecycle is published.
    fn race_readers_against_the_published_lifecycle() {
        let lifecycle = installed_generation_one(ORIGIN);
        let clock = lifecycle
            .bind()
            .assured("the model installs generation one before it binds");

        let publishing_lifecycle = lifecycle.clone();
        let publisher = thread::spawn(move || publish_the_lifecycle(&publishing_lifecycle));
        let binding_lifecycle = lifecycle.clone();
        let binding_reader = thread::spawn(move || bind_in_publication_order(&binding_lifecycle));
        let reading_clock = clock.clone();
        let handle_reader = thread::spawn(move || read_in_publication_order(&reading_clock));
        publisher.join().assured(PANICS_END_THE_SCHEDULE);
        binding_reader.join().assured(PANICS_END_THE_SCHEDULE);
        handle_reader.join().assured(PANICS_END_THE_SCHEDULE);

        assert_eq!(
            ObservedInstallation::by_binding(&lifecycle),
            ObservedInstallation::Missing
        );
        assert_eq!(
            ObservedInstallation::by_reading(&clock),
            ObservedInstallation::Missing
        );
    }

    async fn wait_uncancelled(
        clock: DomainClock,
        deadline: LogicalDeadline,
    ) -> DomainClockWaitResult<LogicalDeadlineReached> {
        let cancellation = CancellationToken::new();
        clock.wait_until(deadline, &cancellation).await
    }

    /// Parks a generation-one waiter on a deadline Shuttle never sleeps through, applies `change`
    /// while the waiter can be anywhere in its wait loop, and returns the waiter's outcome.
    async fn wait_across(
        change: fn(&DomainClockLifecycle),
    ) -> DomainClockWaitResult<LogicalDeadlineReached> {
        let lifecycle = installed_generation_one(ORIGIN);
        let clock = lifecycle
            .bind()
            .assured("the model installs generation one before it binds");
        let deadline = clock.deadline_at(model_time(BEYOND_SLEEP_HORIZON));
        let waiter = tokio::spawn(wait_uncancelled(clock, deadline));
        change(&lifecycle);
        waiter.await.assured(PANICS_END_THE_SCHEDULE)
    }

    fn assert_waiter_refused(
        outcome: DomainClockWaitResult<LogicalDeadlineReached>,
        expected: &DomainClockAccessError,
    ) {
        let Err(error) = outcome else {
            panic!("a waiter reached a deadline that its generation's clock never reached");
        };
        assert_eq!(
            error.downcast_ref::<DomainClockAccessError>(),
            Some(expected),
            "a waiter returned a refusal other than the change it waited across: {error:?}"
        );
    }

    fn stop_generation_one(lifecycle: &DomainClockLifecycle) {
        lifecycle.stop(1);
    }

    #[test]
    fn a_logical_waiter_wakes_when_its_generation_stops() {
        check_random_and_pct(wait_across_a_stop);
    }

    fn wait_across_a_stop() {
        let outcome = shuttle::future::block_on(wait_across(stop_generation_one));
        assert_waiter_refused(
            outcome,
            &DomainClockAccessError::Stopped {
                domain: domain(MODEL_DOMAIN),
                generation: 1,
            },
        );
    }

    fn install_generation_two(lifecycle: &DomainClockLifecycle) {
        lifecycle.synchronize(
            &running_generation(2, ORIGIN),
            &test_domain_clock_authority(),
        );
    }

    #[test]
    fn a_logical_waiter_wakes_when_its_generation_is_replaced() {
        check_random_and_pct(wait_across_a_generation_change);
    }

    fn wait_across_a_generation_change() {
        let outcome = shuttle::future::block_on(wait_across(install_generation_two));
        assert_waiter_refused(outcome, &generation_one_replaced_by_two());
    }

    #[test]
    fn a_logical_waiter_wakes_when_its_domain_is_removed() {
        check_random_and_pct(wait_across_a_removal);
    }

    fn wait_across_a_removal() {
        let outcome = shuttle::future::block_on(wait_across(DomainClockLifecycle::mark_missing));
        assert_waiter_refused(
            outcome,
            &DomainClockAccessError::Missing {
                domain: domain(MODEL_DOMAIN),
            },
        );
    }

    fn map_generation_one_to_the_deadline(lifecycle: &DomainClockLifecycle) {
        lifecycle.install_paced(1, frozen_mapping(BEYOND_SLEEP_HORIZON));
    }

    #[test]
    fn a_logical_waiter_wakes_when_a_replacement_mapping_reaches_its_deadline() {
        check_random_and_pct(wait_across_a_mapping_that_reaches_the_deadline);
    }

    fn wait_across_a_mapping_that_reaches_the_deadline() {
        let outcome = shuttle::future::block_on(wait_across(map_generation_one_to_the_deadline));
        let reached = match outcome {
            Ok(reached) => reached,
            Err(error) => panic!(
                "a waiter did not reach the deadline its replacement mapping reached: {error:?}"
            ),
        };
        assert_eq!(reached.due_at(), model_time(BEYOND_SLEEP_HORIZON));
        assert_eq!(reached.snapshot().generation(), 1);
        assert!(
            reached.snapshot().now() >= reached.due_at(),
            "a waiter returned {} before its deadline {}",
            reached.snapshot().now(),
            reached.due_at()
        );
    }
}
