//! Layer: engines and infrastructure.
//! - **Owns.** Retained emitter outcome, retry, acquisition, wait and finish metric children.
//! - **Depends on.** Bounded observation enums, Prometheus and the current task's failure source.
//! - **Must not know.** Drivers, record values, ACK decisions or runtime retry policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns bounded connector observation families")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(recurring, reason = "records through retained metric children")
)]

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_primitives::sync::Arc;
use prometheus::{Counter, CounterVec, IntCounter, IntCounterVec, Opts, Registry};
use strum::AsRefStr;

use super::{
    EMITTER_LABELS, EmitterInteractionMetrics,
    status::{self, EmitterFailureBinding, FailureProjection},
};

const REJECTED: &str = "nervix_emitter_records_rejected_total";
const FAILED: &str = "nervix_emitter_records_failed_total";
const RETRIED: &str = "nervix_emitter_records_retried_total";
const RETRIES: &str = "nervix_emitter_retries_total";
const RETRY_WAIT: &str = "nervix_emitter_retry_wait_seconds_total";
const OPENS: &str = "nervix_emitter_opens_total";
const POOL_WAITS: &str = "nervix_emitter_pool_waits_total";
const POOL_SECONDS: &str = "nervix_emitter_pool_wait_seconds_total";
const CONFIRMATION_TIMEOUTS: &str = "nervix_emitter_confirmation_timeouts_total";
const FINISHES: &str = "nervix_emitter_finishes_total";
const UNSENT: &str = "nervix_emitter_unsent_records_total";

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum EmitterRejectionCode {
    External,
    Oversize,
    Invalid,
    Encode,
}

impl EmitterRejectionCode {
    const ALL: [Self; 4] = [Self::External, Self::Oversize, Self::Invalid, Self::Encode];
    const fn index(self) -> usize {
        match self {
            Self::External => 0,
            Self::Oversize => 1,
            Self::Invalid => 2,
            Self::Encode => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum EmitterRetryCause {
    Publish,
    Connect,
    Commit,
}

impl EmitterRetryCause {
    const ALL: [Self; 3] = [Self::Publish, Self::Connect, Self::Commit];
    const fn index(self) -> usize {
        match self {
            Self::Publish => 0,
            Self::Connect => 1,
            Self::Commit => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum EmitterOpenOutcome {
    Succeeded,
    Failed,
}

impl EmitterOpenOutcome {
    const ALL: [Self; 2] = [Self::Succeeded, Self::Failed];
    const fn index(self) -> usize {
        match self {
            Self::Succeeded => 0,
            Self::Failed => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum EmitterFinishOutcome {
    NothingHeld,
    Flushed,
    Unsent,
    Failed,
}

impl EmitterFinishOutcome {
    const ALL: [Self; 4] = [Self::NothingHeld, Self::Flushed, Self::Unsent, Self::Failed];
    const fn index(self) -> usize {
        match self {
            Self::NothingHeld => 0,
            Self::Flushed => 1,
            Self::Unsent => 2,
            Self::Failed => 3,
        }
    }
}

#[derive(Debug)]
pub(super) struct OutcomeFamilies {
    registry: Registry,
    rejected: IntCounterVec,
    failed: IntCounterVec,
    retried: IntCounterVec,
    retries: IntCounterVec,
    retry_wait: CounterVec,
    opens: IntCounterVec,
    pool_waits: IntCounterVec,
    pool_seconds: CounterVec,
    confirmation_timeouts: IntCounterVec,
    finishes: IntCounterVec,
    unsent: IntCounterVec,
}

impl OutcomeFamilies {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers each fixed outcome family once")
    )]
    pub(super) fn register(registry: &Registry) -> Self {
        Self {
            registry: registry.clone(),
            rejected: Self::counter(
                registry,
                REJECTED,
                "Definitively rejected source records by typed refusal class.",
                Some("code"),
            ),
            failed: Self::counter(
                registry,
                FAILED,
                "Source records sent to message-error policy after a terminal publish failure.",
                None,
            ),
            retried: Self::counter(
                registry,
                RETRIED,
                "Unresolved source records selected by retried host publish attempts.",
                None,
            ),
            retries: Self::counter(
                registry,
                RETRIES,
                "Scheduled host retries by the operation being retried.",
                Some("cause"),
            ),
            retry_wait: Self::seconds(
                registry,
                RETRY_WAIT,
                "Wall seconds of retry delay actually scheduled.",
            ),
            opens: Self::counter(
                registry,
                OPENS,
                "Initial transport acquisitions and retained transport reacquisitions.",
                Some("outcome"),
            ),
            pool_waits: Self::counter(
                registry,
                POOL_WAITS,
                "Completed, failed or cancelled pending pool borrows.",
                None,
            ),
            pool_seconds: Self::seconds(
                registry,
                POOL_SECONDS,
                "Elapsed wall seconds of pending pool borrows.",
            ),
            confirmation_timeouts: Self::counter(
                registry,
                CONFIRMATION_TIMEOUTS,
                "Host attempts whose typed confirmation window timed out.",
                None,
            ),
            finishes: Self::counter(
                registry,
                FINISHES,
                "Transport finishes by their typed outcome, including cancelled finishes.",
                Some("outcome"),
            ),
            unsent: Self::counter(
                registry,
                UNSENT,
                "Records a transport finish reported still unsent at its deadline.",
                None,
            ),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers a bounded counter family")
    )]
    fn counter(registry: &Registry, name: &str, help: &str, extra: Option<&str>) -> IntCounterVec {
        let mut labels = EMITTER_LABELS.to_vec();
        if let Some(extra) = extra {
            labels.push(extra);
        }
        let counter = IntCounterVec::new(Opts::new(name, help), &labels)
            .assured("the fixed counter and label identifiers are valid");
        registry
            .register(Box::new(counter.clone()))
            .assured("each outcome family is distinct");
        counter
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "registers fractional elapsed or scheduled seconds"
        )
    )]
    fn seconds(registry: &Registry, name: &str, help: &str) -> CounterVec {
        let counter = CounterVec::new(Opts::new(name, help), &EMITTER_LABELS)
            .assured("the fixed duration counter and label identifiers are valid");
        registry
            .register(Box::new(counter.clone()))
            .assured("each duration family is distinct");
        counter
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "resolves every child before an emitter starts")
    )]
    pub(super) fn emitter(&self, values: &[&str; 6]) -> OutcomeMetrics {
        OutcomeMetrics {
            rejected: EmitterRejectionCode::ALL
                .map(|code| Self::child(&self.rejected, values, code.as_ref())),
            failed: self.failed.with_label_values(values),
            retried: self.retried.with_label_values(values),
            retries: EmitterRetryCause::ALL
                .map(|cause| Self::child(&self.retries, values, cause.as_ref())),
            retry_wait: self.retry_wait.with_label_values(values),
            opens: EmitterOpenOutcome::ALL
                .map(|outcome| Self::child(&self.opens, values, outcome.as_ref())),
            pool_waits: self.pool_waits.with_label_values(values),
            pool_seconds: self.pool_seconds.with_label_values(values),
            confirmation_timeouts: self.confirmation_timeouts.with_label_values(values),
            finishes: EmitterFinishOutcome::ALL
                .map(|outcome| Self::child(&self.finishes, values, outcome.as_ref())),
            unsent: self.unsent.with_label_values(values),
            failure: FailureProjection::register(&self.registry, values),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "resolves one bounded enum child")
    )]
    fn child(family: &IntCounterVec, values: &[&str; 6], extra: &str) -> IntCounter {
        let mut labels = values.to_vec();
        labels.push(extra);
        family.with_label_values(&labels)
    }
}

#[derive(Debug)]
pub(super) struct OutcomeMetrics {
    rejected: [IntCounter; 4],
    failed: IntCounter,
    retried: IntCounter,
    retries: [IntCounter; 3],
    retry_wait: Counter,
    opens: [IntCounter; 2],
    pool_waits: IntCounter,
    pool_seconds: Counter,
    confirmation_timeouts: IntCounter,
    finishes: [IntCounter; 4],
    unsent: IntCounter,
    // The registered collector retains this projection after emitter handles end.
    failure: Arc<FailureProjection>,
}

impl OutcomeMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "renders retained event counts and the current failure observation"
        )
    )]
    pub(super) fn describe_lines(&self, selector: &str, lines: &mut Vec<String>) {
        for code in EmitterRejectionCode::ALL {
            lines.push(format!(
                "{REJECTED}{{{selector},code=\"{}\"}} total={}",
                code.as_ref(),
                self.rejected[code.index()].get()
            ));
        }
        for cause in EmitterRetryCause::ALL {
            lines.push(format!(
                "{RETRIES}{{{selector},cause=\"{}\"}} total={}",
                cause.as_ref(),
                self.retries[cause.index()].get()
            ));
        }
        for outcome in EmitterOpenOutcome::ALL {
            lines.push(format!(
                "{OPENS}{{{selector},outcome=\"{}\"}} total={}",
                outcome.as_ref(),
                self.opens[outcome.index()].get()
            ));
        }
        for outcome in EmitterFinishOutcome::ALL {
            lines.push(format!(
                "{FINISHES}{{{selector},outcome=\"{}\"}} total={}",
                outcome.as_ref(),
                self.finishes[outcome.index()].get()
            ));
        }
        for (name, counter) in [
            (FAILED, &self.failed),
            (RETRIED, &self.retried),
            (POOL_WAITS, &self.pool_waits),
            (CONFIRMATION_TIMEOUTS, &self.confirmation_timeouts),
            (UNSENT, &self.unsent),
        ] {
            lines.push(format!("{name}{{{selector}}} total={}", counter.get()));
        }
        for (name, counter) in [
            (RETRY_WAIT, &self.retry_wait),
            (POOL_SECONDS, &self.pool_seconds),
        ] {
            lines.push(format!("{name}{{{selector}}} total={}", counter.get()));
        }
        lines.push(format!(
            "{}{{{selector}}} value={}",
            status::NAME,
            self.failure.value()
        ));
    }
}

impl EmitterInteractionMetrics {
    pub(crate) fn record_retry(&self, cause: EmitterRetryCause, delay: Duration) {
        self.series.outcomes.retries[cause.index()].inc();
        self.series.outcomes.retry_wait.inc_by(delay.as_secs_f64());
    }

    pub(crate) fn record_retried(&self, records: u64) {
        if records != 0 {
            self.series.outcomes.retried.inc_by(records);
        }
    }

    pub(crate) fn record_pool_wait(&self, elapsed: Duration) {
        self.series.outcomes.pool_waits.inc();
        self.series
            .outcomes
            .pool_seconds
            .inc_by(elapsed.as_secs_f64());
    }

    pub(crate) fn record_confirmation_timeout(&self) {
        self.series.outcomes.confirmation_timeouts.inc();
    }

    pub(crate) fn record_outcomes(&self) -> EmitterRecordOutcomes<'_> {
        EmitterRecordOutcomes {
            metrics: &self.series.outcomes,
            rejected: [0; 4],
            failed: 0,
        }
    }

    pub(crate) fn begin_open(&self) -> EmitterOpenAttempt<'_> {
        EmitterOpenAttempt {
            metrics: &self.series.outcomes,
            outcome: Some(EmitterOpenOutcome::Failed),
        }
    }

    pub(crate) fn begin_finish(&self) -> EmitterFinishing<'_> {
        EmitterFinishing {
            metrics: &self.series.outcomes,
            outcome: EmitterFinishOutcome::Failed,
            unsent: 0,
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "the task controls the publication lifetime of its failure observation"
        )
    )]
    pub(crate) fn bind_failure_source(
        &self,
        failed: impl Fn() -> bool + Send + Sync + 'static,
    ) -> EmitterFailureBinding {
        FailureProjection::bind(&self.series.outcomes.failure, failed)
    }
}

/// Accumulates locally, including partially completed routing, then increments once per class.
#[must_use]
pub(crate) struct EmitterRecordOutcomes<'a> {
    metrics: &'a OutcomeMetrics,
    rejected: [u64; 4],
    failed: u64,
}

impl EmitterRecordOutcomes<'_> {
    pub(crate) fn reject(&mut self, code: EmitterRejectionCode) {
        self.rejected[code.index()] += 1;
    }
    pub(crate) fn fail(&mut self) {
        self.failed += 1;
    }
}

impl Drop for EmitterRecordOutcomes<'_> {
    fn drop(&mut self) {
        for (counter, count) in self.metrics.rejected.iter().zip(self.rejected) {
            if count != 0 {
                counter.inc_by(count);
            }
        }
        if self.failed != 0 {
            self.metrics.failed.inc_by(self.failed);
        }
    }
}

#[must_use]
pub(crate) struct EmitterOpenAttempt<'a> {
    metrics: &'a OutcomeMetrics,
    outcome: Option<EmitterOpenOutcome>,
}

impl EmitterOpenAttempt<'_> {
    pub(crate) fn finish(mut self, outcome: EmitterOpenOutcome) {
        self.outcome = Some(outcome);
    }
    /// Local preparation succeeded; the resume attempt owns the acquisition outcome.
    pub(crate) fn prepared(mut self) {
        self.outcome = None;
    }
}

impl Drop for EmitterOpenAttempt<'_> {
    fn drop(&mut self) {
        if let Some(outcome) = self.outcome {
            self.metrics.opens[outcome.index()].inc();
        }
    }
}

#[must_use]
pub(crate) struct EmitterFinishing<'a> {
    metrics: &'a OutcomeMetrics,
    outcome: EmitterFinishOutcome,
    unsent: u64,
}

impl EmitterFinishing<'_> {
    pub(crate) fn finish(mut self, outcome: EmitterFinishOutcome, unsent: u64) {
        self.outcome = outcome;
        self.unsent = unsent;
    }
}

impl Drop for EmitterFinishing<'_> {
    fn drop(&mut self) {
        self.metrics.finishes[self.outcome.index()].inc();
        if self.unsent != 0 {
            self.metrics.unsent.inc_by(self.unsent);
        }
    }
}

#[cfg(test)]
mod tests;
