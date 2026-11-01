//! Administrative phase measurements for one domain cut or one owner's capture.
//!
//! Layer: control plane.
//! - **Owns.** The active capture phase, ordered completed durations and their timeout diagnostics.
//! - **Depends on.** Typed domain, execution and coordination identities, the monotonic clock and
//!   the lifecycle's typed drain failure.
//! - **Must not know.** Checkpoint contents, transport internals or archive encoding.
//!
//! Each capture task owns its measurements. Phase transitions add no shared state or synchronization.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns administrative capture-phase diagnostics"
    )
)]

use std::{fmt, time::Duration};

use error_stack::Report;
use nervix_models::{ClusterNodeName, CommandExecutionReference, CoordinationIdentity, DomainName};
use nervix_primitives::time::Instant;
use strum::Display;

use super::BackupError;
use crate::application::domain_lifecycle::DomainAlterError;

/// A coordinator phase, or a finer owner-local phase recorded in that owner's logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[strum(serialize_all = "snake_case")]
pub(in crate::application) enum CutPhase {
    MutationLease,
    ConfigurationRead,
    AlterationGuard,
    Reach,
    OpeningCounters,
    /// Includes the replicated pause, its application across the cluster and its confirming
    /// drain.
    Drain,
    Capture,
    ConfigurationCapture,
    OwnerCapture,
    OwnerInventory,
    ClosingCounters,
    RevisionWait,
    ClusterStateApply,
    LifecycleCheckpoints,
    StorageCapture,
    Staging,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PhaseTiming {
    phase: CutPhase,
    elapsed: Duration,
}

/// Completed measurements in execution order, without the still-active phase.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::application) struct CompletedPhases {
    phases: Vec<PhaseTiming>,
}

impl fmt::Display for CompletedPhases {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[")?;
        for (position, measurement) in self.phases.iter().enumerate() {
            if position != 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{}={:?}", measurement.phase, measurement.elapsed)?;
        }
        formatter.write_str("]")
    }
}

/// The correlation identity already owned by the task being measured.
pub(super) enum CaptureContext {
    Coordinator {
        execution: CommandExecutionReference,
    },
    Owner {
        node: ClusterNodeName,
        coordination: CoordinationIdentity,
    },
}

#[derive(Display)]
#[strum(serialize_all = "snake_case")]
enum PhaseEvent {
    Started,
    Completed,
    TimedOut,
    Failed,
}

pub(super) struct CutProgress {
    domain: DomainName,
    context: CaptureContext,
    started: Instant,
    entered: Instant,
    phase: CutPhase,
    completed: CompletedPhases,
}

impl CutProgress {
    pub(super) fn at(
        domain: DomainName,
        context: CaptureContext,
        phase: CutPhase,
        started: Instant,
    ) -> Self {
        let progress = Self {
            domain,
            context,
            started,
            entered: started,
            phase,
            completed: CompletedPhases::default(),
        };
        progress.log(PhaseEvent::Started, started);
        progress
    }

    pub(super) fn enter(&mut self, phase: CutPhase) {
        self.enter_at(phase, Instant::now());
    }

    fn enter_at(&mut self, phase: CutPhase, now: Instant) {
        self.log(PhaseEvent::Completed, now);
        self.completed.phases.push(PhaseTiming {
            phase: self.phase,
            elapsed: now.duration_since(self.entered),
        });
        self.phase = phase;
        self.entered = now;
        self.log(PhaseEvent::Started, now);
    }

    /// Attribute an exhausted budget to the phase that used it, before entering another one.
    pub(super) fn enter_within(
        &mut self,
        phase: CutPhase,
        deadline: Instant,
    ) -> Result<(), Report<BackupError>> {
        let now = Instant::now();
        self.ensure_at(now, deadline)?;
        self.enter_at(phase, now);
        Ok(())
    }

    pub(super) fn ensure_within(&self, deadline: Instant) -> Result<(), Report<BackupError>> {
        self.ensure_at(Instant::now(), deadline)
    }

    fn ensure_at(&self, now: Instant, deadline: Instant) -> Result<(), Report<BackupError>> {
        if now >= deadline {
            return Err(Report::new(self.timeout_at(now)));
        }
        Ok(())
    }

    pub(super) fn timeout(&self) -> BackupError {
        self.timeout_at(Instant::now())
    }

    fn timeout_at(&self, now: Instant) -> BackupError {
        self.log(PhaseEvent::TimedOut, now);
        BackupError::CaptureTimeout {
            domain: self.domain.clone(),
            phase: self.phase,
            elapsed: now.duration_since(self.started),
            phase_elapsed: now.duration_since(self.entered),
            completed: self.completed.clone(),
        }
    }

    /// Preserve the typed report of a pause application or drain the cut's deadline ended, such as
    /// the drain's outstanding work, beneath this cut's attribution.
    pub(super) fn drain_failure(&self, error: Report<DomainAlterError>) -> Report<BackupError> {
        match error.current_context() {
            DomainAlterError::BackupPauseTimeout { .. }
            | DomainAlterError::BackupQuiesceTimeout { .. }
            | DomainAlterError::QuiesceTimeout { .. } => error.change_context(self.timeout()),
            _ => error.change_context(BackupError::CaptureDomain {
                domain: self.domain.clone(),
                reason: "domain pause or drain failed".to_string(),
            }),
        }
    }

    pub(super) fn log_completed(&self) {
        self.log(PhaseEvent::Completed, Instant::now());
    }

    pub(super) fn log_failed(&self) {
        self.log(PhaseEvent::Failed, Instant::now());
    }

    fn log(&self, event: PhaseEvent, now: Instant) {
        let elapsed = now.duration_since(self.started);
        let phase_elapsed = now.duration_since(self.entered);
        match &self.context {
            CaptureContext::Coordinator { execution } => tracing::info!(
                domain = %self.domain,
                execution = %execution,
                phase = %self.phase,
                event = %event,
                ?elapsed,
                ?phase_elapsed,
                "backup cut phase"
            ),
            CaptureContext::Owner { node, coordination } => tracing::info!(
                domain = %self.domain,
                node = %node,
                coordination = ?coordination,
                phase = %self.phase,
                event = %event,
                ?elapsed,
                ?phase_elapsed,
                "backup owner phase"
            ),
        }
    }
}

#[cfg(test)]
#[path = "cut_progress_tests.rs"]
mod tests;
