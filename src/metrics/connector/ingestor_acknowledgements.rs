//! Retained acknowledgement, transport settlement and connector lifecycle observations of one
//! ingestor unit.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The acknowledgement-outcome, transport-acknowledgement, resume, suspension and
//!   unconfirmed-stop families with their bounded label children, and the unit's transient-failure
//!   projection.
//! - **Depends on.** The connector contract's observation vocabulary, Prometheus and the shared
//!   task-bound failure projection.
//! - **Must not know.** Drivers, acknowledgement trees, settlement or retry policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns bounded ingestor observation families")
)]

use std::time::Duration;

use arch_into::ArchInto as _;
use meticulous::ResultExt as _;
use nervix_connector::{
    IngestorAcknowledgementOutcome, IngestorResumeOutcome, IngestorSettlementOperation,
    IngestorSettlementOutcome, IngestorSuspendCause,
};
use nervix_primitives::sync::Arc;
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry,
};
use strum::{AsRefStr, IntoEnumIterator as _};

use super::{
    DURATION_BUCKETS, format_number,
    status::{FailureFamily, FailureProjection, TaskFailureBinding},
};

const ACK_OUTCOMES: &str = "nervix_ingestor_ack_outcomes_total";
const TRANSPORT_ACKNOWLEDGEMENTS: &str = "nervix_ingestor_transport_acknowledgements_total";
const TRANSPORT_DURATION: &str = "nervix_ingestor_transport_acknowledgement_duration_seconds";
const RESUMES: &str = "nervix_ingestor_resumes_total";
const SUSPENDS: &str = "nervix_ingestor_suspends_total";
const UNCONFIRMED_STOPS: &str = "nervix_ingestor_unconfirmed_stop_positions_total";
const TRANSIENT_FAILURE: &str = "nervix_ingestor_transient_failure";

/// How an acknowledgement wait ended, as its counter's bounded `outcome` label names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, strum::EnumIter)]
#[strum(serialize_all = "snake_case")]
enum AcknowledgementOutcomeLabel {
    Acknowledged,
    Failed,
    TimedOut,
    Shutdown,
}

impl From<&IngestorAcknowledgementOutcome> for AcknowledgementOutcomeLabel {
    fn from(outcome: &IngestorAcknowledgementOutcome) -> Self {
        match outcome {
            IngestorAcknowledgementOutcome::Acknowledged => Self::Acknowledged,
            IngestorAcknowledgementOutcome::Failed { .. } => Self::Failed,
            IngestorAcknowledgementOutcome::TimedOut { .. } => Self::TimedOut,
            IngestorAcknowledgementOutcome::Shutdown => Self::Shutdown,
        }
    }
}

/// The families every ingestor unit's acknowledgement and lifecycle children belong to.
#[derive(Debug)]
pub(super) struct AcknowledgementFamilies {
    registry: Registry,
    outcomes: IntCounterVec,
    settlements: IntCounterVec,
    durations: HistogramVec,
    resumes: IntCounterVec,
    suspends: IntCounterVec,
    unconfirmed_stops: IntCounterVec,
    failure: FailureFamily,
}

impl AcknowledgementFamilies {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers each fixed ingestor family once")
    )]
    pub(super) fn register(registry: &Registry, labels: &'static [&'static str]) -> Self {
        let durations = HistogramVec::new(
            HistogramOpts::new(
                TRANSPORT_DURATION,
                "Elapsed wall seconds of transport acknowledgements and rejections of read \
                 positions.",
            )
            .buckets(DURATION_BUCKETS.to_vec()),
            &Self::extended(labels, &["operation"]),
        )
        .assured("fixed names and published duration buckets satisfy Prometheus");
        registry
            .register(Box::new(durations.clone()))
            .assured("the transport duration family is registered once");
        Self {
            registry: registry.clone(),
            outcomes: Self::counter(
                registry,
                ACK_OUTCOMES,
                "Acknowledgement waits an ingestor settled, by how they ended.",
                &Self::extended(labels, &["outcome"]),
            ),
            settlements: Self::counter(
                registry,
                TRANSPORT_ACKNOWLEDGEMENTS,
                "Transport acknowledgements and rejections of read positions, by outcome.",
                &Self::extended(labels, &["operation", "outcome"]),
            ),
            durations,
            resumes: Self::counter(
                registry,
                RESUMES,
                "Completed resumes of an ingestor's connector, by outcome.",
                &Self::extended(labels, &["outcome"]),
            ),
            suspends: Self::counter(
                registry,
                SUSPENDS,
                "Suspensions of a ready ingestor connector, by cause.",
                &Self::extended(labels, &["cause"]),
            ),
            unconfirmed_stops: Self::counter(
                registry,
                UNCONFIRMED_STOPS,
                "Read positions an ingestor stopped before its transport confirmed them.",
                labels,
            ),
            failure: FailureFamily::new(
                TRANSIENT_FAILURE,
                "Whether the current ingestor task has a published transient failure.",
                labels,
            ),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "composes one fixed label list at registration")
    )]
    fn extended<'a>(labels: &[&'a str], extra: &[&'a str]) -> Vec<&'a str> {
        let mut extended = labels.to_vec();
        extended.extend_from_slice(extra);
        extended
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers one distinct counter family")
    )]
    fn counter(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
        let counter = IntCounterVec::new(Opts::new(name, help), labels)
            .assured("fixed metric names and label names satisfy Prometheus");
        registry
            .register(Box::new(counter.clone()))
            .assured("each ingestor acknowledgement family is registered once");
        counter
    }

    /// The child of `family` for one unit's `identity` and the bounded label values `extra`.
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "resolves one bounded child at registration")
    )]
    fn child(family: &IntCounterVec, identity: &[&str], extra: &[&str]) -> IntCounter {
        family.with_label_values(&Self::extended(identity, extra))
    }

    /// Resolves every child of one unit at zero, with `identity` its identity label values in the
    /// order the families declare them.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "resolves one unit's complete child set when the unit is registered"
        )
    )]
    pub(super) fn unit(&self, identity: &[&str]) -> IngestorAcknowledgementMetrics {
        IngestorAcknowledgementMetrics {
            series: Arc::new(AcknowledgementSeries {
                outcomes: OutcomeChildren::register(&self.outcomes, identity),
                acknowledge: SettlementChildren::register(
                    &self.settlements,
                    &self.durations,
                    identity,
                    IngestorSettlementOperation::Acknowledge,
                ),
                reject: SettlementChildren::register(
                    &self.settlements,
                    &self.durations,
                    identity,
                    IngestorSettlementOperation::Reject,
                ),
                resumes: ResumeChildren::register(&self.resumes, identity),
                suspends: SuspendChildren::register(&self.suspends, identity),
                unconfirmed_stops: self.unconfirmed_stops.with_label_values(identity),
                failure: FailureProjection::register(&self.registry, &self.failure, identity),
            }),
        }
    }

    /// Withdraws every child of the unit `metrics` observes, with `identity` its identity label
    /// values, when the unit retires.
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "withdraws only a retired unit's children")
    )]
    pub(super) fn withdraw(&self, metrics: &IngestorAcknowledgementMetrics, identity: &[&str]) {
        for outcome in AcknowledgementOutcomeLabel::iter() {
            Self::remove(&self.outcomes, identity, &[outcome.as_ref()]);
        }
        for operation in IngestorSettlementOperation::iter() {
            for outcome in IngestorSettlementOutcome::iter() {
                Self::remove(
                    &self.settlements,
                    identity,
                    &[operation.as_ref(), outcome.as_ref()],
                );
            }
            let labels = Self::extended(identity, &[operation.as_ref()]);
            self.durations
                .remove_label_values(&labels)
                .assured("registration installs both transport duration children");
        }
        for outcome in IngestorResumeOutcome::iter() {
            Self::remove(&self.resumes, identity, &[outcome.as_ref()]);
        }
        for cause in IngestorSuspendCause::iter() {
            Self::remove(&self.suspends, identity, &[cause.as_ref()]);
        }
        self.unconfirmed_stops
            .remove_label_values(identity)
            .assured("registration installs the unconfirmed-stop counter");
        FailureProjection::withdraw(&metrics.series.failure, &self.registry);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "removes one bounded child of a retired unit")
    )]
    fn remove(family: &IntCounterVec, identity: &[&str], extra: &[&str]) {
        let labels = Self::extended(identity, extra);
        family
            .remove_label_values(&labels)
            .assured("registration installs every bounded child of the unit");
    }
}

/// The retained acknowledgement and lifecycle children of one ingestor unit, which every task
/// generation of the unit and its inspection share.
#[derive(Debug, Clone)]
pub(crate) struct IngestorAcknowledgementMetrics {
    series: Arc<AcknowledgementSeries>,
}

#[derive(Debug)]
struct AcknowledgementSeries {
    outcomes: OutcomeChildren,
    acknowledge: SettlementChildren,
    reject: SettlementChildren,
    resumes: ResumeChildren,
    suspends: SuspendChildren,
    unconfirmed_stops: IntCounter,
    // The registered collector retains this projection after ingestor hosts end.
    failure: Arc<FailureProjection>,
}

/// One acknowledgement-outcome child for each way an acknowledgement wait ends.
#[derive(Debug)]
struct OutcomeChildren {
    acknowledged: IntCounter,
    failed: IntCounter,
    timed_out: IntCounter,
    shutdown: IntCounter,
}

impl OutcomeChildren {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "resolves every bounded outcome child once")
    )]
    fn register(family: &IntCounterVec, identity: &[&str]) -> Self {
        Self {
            acknowledged: AcknowledgementFamilies::child(
                family,
                identity,
                &[AcknowledgementOutcomeLabel::Acknowledged.as_ref()],
            ),
            failed: AcknowledgementFamilies::child(
                family,
                identity,
                &[AcknowledgementOutcomeLabel::Failed.as_ref()],
            ),
            timed_out: AcknowledgementFamilies::child(
                family,
                identity,
                &[AcknowledgementOutcomeLabel::TimedOut.as_ref()],
            ),
            shutdown: AcknowledgementFamilies::child(
                family,
                identity,
                &[AcknowledgementOutcomeLabel::Shutdown.as_ref()],
            ),
        }
    }

    fn of(&self, outcome: AcknowledgementOutcomeLabel) -> &IntCounter {
        match outcome {
            AcknowledgementOutcomeLabel::Acknowledged => &self.acknowledged,
            AcknowledgementOutcomeLabel::Failed => &self.failed,
            AcknowledgementOutcomeLabel::TimedOut => &self.timed_out,
            AcknowledgementOutcomeLabel::Shutdown => &self.shutdown,
        }
    }
}

/// The outcome children and duration histogram of one transport settlement operation.
#[derive(Debug)]
struct SettlementChildren {
    succeeded: IntCounter,
    failed: IntCounter,
    duration: Histogram,
}

impl SettlementChildren {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "resolves one operation's children once")
    )]
    fn register(
        settlements: &IntCounterVec,
        durations: &HistogramVec,
        identity: &[&str],
        operation: IngestorSettlementOperation,
    ) -> Self {
        let labels = AcknowledgementFamilies::extended(identity, &[operation.as_ref()]);
        Self {
            succeeded: settlements.with_label_values(&AcknowledgementFamilies::extended(
                &labels,
                &[IngestorSettlementOutcome::Succeeded.as_ref()],
            )),
            failed: settlements.with_label_values(&AcknowledgementFamilies::extended(
                &labels,
                &[IngestorSettlementOutcome::Failed.as_ref()],
            )),
            duration: durations.with_label_values(&labels),
        }
    }

    fn of(&self, outcome: IngestorSettlementOutcome) -> &IntCounter {
        match outcome {
            IngestorSettlementOutcome::Succeeded => &self.succeeded,
            IngestorSettlementOutcome::Failed => &self.failed,
        }
    }
}

/// One resume child for each way a completed resume answers.
#[derive(Debug)]
struct ResumeChildren {
    ready: IntCounter,
    waiting: IntCounter,
    failed: IntCounter,
}

impl ResumeChildren {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "resolves every bounded resume child once")
    )]
    fn register(family: &IntCounterVec, identity: &[&str]) -> Self {
        Self {
            ready: AcknowledgementFamilies::child(
                family,
                identity,
                &[IngestorResumeOutcome::Ready.as_ref()],
            ),
            waiting: AcknowledgementFamilies::child(
                family,
                identity,
                &[IngestorResumeOutcome::Waiting.as_ref()],
            ),
            failed: AcknowledgementFamilies::child(
                family,
                identity,
                &[IngestorResumeOutcome::Failed.as_ref()],
            ),
        }
    }

    fn of(&self, outcome: IngestorResumeOutcome) -> &IntCounter {
        match outcome {
            IngestorResumeOutcome::Ready => &self.ready,
            IngestorResumeOutcome::Waiting => &self.waiting,
            IngestorResumeOutcome::Failed => &self.failed,
        }
    }
}

/// One suspension child for each cause the host suspends a ready connector for.
#[derive(Debug)]
struct SuspendChildren {
    quiesce: IntCounter,
    read_failure: IntCounter,
    rejection: IntCounter,
}

impl SuspendChildren {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "resolves every bounded suspension child once")
    )]
    fn register(family: &IntCounterVec, identity: &[&str]) -> Self {
        Self {
            quiesce: AcknowledgementFamilies::child(
                family,
                identity,
                &[IngestorSuspendCause::Quiesce.as_ref()],
            ),
            read_failure: AcknowledgementFamilies::child(
                family,
                identity,
                &[IngestorSuspendCause::ReadFailure.as_ref()],
            ),
            rejection: AcknowledgementFamilies::child(
                family,
                identity,
                &[IngestorSuspendCause::Rejection.as_ref()],
            ),
        }
    }

    fn of(&self, cause: IngestorSuspendCause) -> &IntCounter {
        match cause {
            IngestorSuspendCause::Quiesce => &self.quiesce,
            IngestorSuspendCause::ReadFailure => &self.read_failure,
            IngestorSuspendCause::Rejection => &self.rejection,
        }
    }
}

impl IngestorAcknowledgementMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "selects and increments one retained child")
    )]
    pub(crate) fn acknowledgement(&self, outcome: &IngestorAcknowledgementOutcome) {
        self.series
            .outcomes
            .of(AcknowledgementOutcomeLabel::from(outcome))
            .inc();
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "records one completed transport settlement through retained children"
        )
    )]
    pub(crate) fn transport_settlement(
        &self,
        operation: IngestorSettlementOperation,
        outcome: IngestorSettlementOutcome,
        elapsed: Duration,
    ) {
        let settlement = match operation {
            IngestorSettlementOperation::Acknowledge => &self.series.acknowledge,
            IngestorSettlementOperation::Reject => &self.series.reject,
        };
        settlement.of(outcome).inc();
        settlement.duration.observe(elapsed.as_secs_f64());
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "increments one retained resume child")
    )]
    pub(crate) fn resume(&self, outcome: IngestorResumeOutcome) {
        self.series.resumes.of(outcome).inc();
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "increments one retained suspension child")
    )]
    pub(crate) fn suspend(&self, cause: IngestorSuspendCause) {
        self.series.suspends.of(cause).inc();
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "increments one retained child at an ingestor stop"
        )
    )]
    pub(crate) fn unconfirmed_stop(&self, positions: usize) {
        self.series.unconfirmed_stops.inc_by(positions.arch_into());
    }

    /// Publishes `failed` as the unit's transient failure for as long as the returned binding
    /// lives, which the starting ingestor host holds until it ends.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "the host controls the publication lifetime of its failure observation"
        )
    )]
    pub(crate) fn bind_failure_source(
        &self,
        failed: impl Fn() -> bool + Send + Sync + 'static,
    ) -> TaskFailureBinding {
        FailureProjection::bind(&self.series.failure, failed)
    }

    /// Appends one line for every child under `selector`, the unit's identity label selector.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "renders retained counters, histogram samples and the current failure"
        )
    )]
    pub(super) fn describe_lines(&self, selector: &str, lines: &mut Vec<String>) {
        for outcome in AcknowledgementOutcomeLabel::iter() {
            lines.push(format!(
                "{ACK_OUTCOMES}{{{selector},outcome=\"{}\"}} total={}",
                outcome.as_ref(),
                self.series.outcomes.of(outcome).get()
            ));
        }
        for (operation, settlement) in [
            (
                IngestorSettlementOperation::Acknowledge,
                &self.series.acknowledge,
            ),
            (IngestorSettlementOperation::Reject, &self.series.reject),
        ] {
            for outcome in IngestorSettlementOutcome::iter() {
                lines.push(format!(
                    "{TRANSPORT_ACKNOWLEDGEMENTS}{{{selector},operation=\"{}\",outcome=\"{}\"}} \
                     total={}",
                    operation.as_ref(),
                    outcome.as_ref(),
                    settlement.of(outcome).get()
                ));
            }
            lines.push(format!(
                "{TRANSPORT_DURATION}{{{selector},operation=\"{}\"}} count={} sum_seconds={}",
                operation.as_ref(),
                settlement.duration.get_sample_count(),
                format_number(settlement.duration.get_sample_sum())
            ));
        }
        for outcome in IngestorResumeOutcome::iter() {
            lines.push(format!(
                "{RESUMES}{{{selector},outcome=\"{}\"}} total={}",
                outcome.as_ref(),
                self.series.resumes.of(outcome).get()
            ));
        }
        for cause in IngestorSuspendCause::iter() {
            lines.push(format!(
                "{SUSPENDS}{{{selector},cause=\"{}\"}} total={}",
                cause.as_ref(),
                self.series.suspends.of(cause).get()
            ));
        }
        lines.push(format!(
            "{UNCONFIRMED_STOPS}{{{selector}}} total={}",
            self.series.unconfirmed_stops.get()
        ));
        lines.push(format!(
            "{TRANSIENT_FAILURE}{{{selector}}} value={}",
            self.series.failure.value()
        ));
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "qualifies the production ingestor observation owner"
    )
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** Zero registration, exact child selection, the failure binding's lifetime and
    //!   exact-unit withdrawal of the acknowledgement and lifecycle families.
    //! - **Depends on.** The production metric registry and the retained ingestor handles.
    //! - **Must not know.** Runtime hosts, ACK trees or connector drivers.

    use std::collections::BTreeMap;

    use nervix_models::{
        ClusterNodeName, DomainName, IngestorEntityKind, IngestorName, KafkaIngestorEntity, Unit,
    };
    use nervix_primitives::sync::atomic::{AtomicBool, Ordering};
    use prometheus::proto::MetricType;

    use super::*;
    use crate::metrics::RuntimeMetrics;

    struct Fixture {
        metrics: RuntimeMetrics,
        domain: DomainName,
        ingestor: IngestorName,
        node: ClusterNodeName,
        kind: IngestorEntityKind,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                metrics: RuntimeMetrics::default(),
                domain: DomainName::parse("acknowledgements").assured("fixture domain"),
                ingestor: IngestorName::parse("input").assured("fixture ingestor"),
                node: ClusterNodeName::parse("node-1").assured("fixture node"),
                kind: IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic),
            }
        }

        fn unit(&self, unit: Unit) -> IngestorAcknowledgementMetrics {
            let (interactions, _decode_rejections) = self.metrics.register_ingestor_interactions(
                &self.domain,
                &self.ingestor,
                unit,
                Some(&self.node),
                self.kind,
            );
            interactions.acknowledgements
        }

        /// Every sample of `family` for `unit`, keyed by its bounded labels beyond the identity.
        fn samples(&self, family: &str, unit: Unit) -> BTreeMap<Vec<String>, f64> {
            let instance = crate::metrics::instance_label(unit);
            let mut samples = BTreeMap::new();
            for gathered in self.metrics.series.prometheus.registry.gather() {
                if gathered.name() != family {
                    continue;
                }
                for metric in gathered.get_metric() {
                    let labels = metric
                        .get_label()
                        .iter()
                        .map(|pair| (pair.name().to_owned(), pair.value().to_owned()))
                        .collect::<BTreeMap<_, _>>();
                    if labels["instance"] != instance.as_ref() {
                        continue;
                    }
                    assert_eq!(labels["domain"], "acknowledgements");
                    assert_eq!(labels["ingestor"], "input");
                    assert_eq!(labels["physical_node_id"], "node-1");
                    assert_eq!(labels["connector"], "KAFKA");
                    assert_eq!(labels["entity"], "TOPIC");
                    let mut bounded = Vec::new();
                    for name in ["operation", "outcome", "cause"] {
                        if let Some(value) = labels.get(name) {
                            bounded.push(format!("{name}={value}"));
                        }
                    }
                    let value = match gathered.get_field_type() {
                        MetricType::COUNTER => metric.get_counter().value(),
                        MetricType::GAUGE => metric.get_gauge().value(),
                        MetricType::HISTOGRAM => {
                            let count = metric.get_histogram().get_sample_count();
                            u32::try_from(count)
                                .assured("a check observes only a few samples")
                                .into()
                        }
                        other => panic!("unexpected ingestor family type {other:?}"),
                    };
                    samples.insert(bounded, value);
                }
            }
            samples
        }
    }

    fn key(labels: &[&str]) -> Vec<String> {
        labels.iter().map(|label| (*label).to_owned()).collect()
    }

    #[test]
    fn every_acknowledgement_and_lifecycle_child_starts_at_zero_with_bounded_labels() {
        let fixture = Fixture::new();
        let _unit = fixture.unit(Unit::SINGLE);

        let outcomes = fixture.samples(ACK_OUTCOMES, Unit::SINGLE);
        assert_eq!(
            outcomes.keys().cloned().collect::<Vec<_>>(),
            [
                key(&["outcome=acknowledged"]),
                key(&["outcome=failed"]),
                key(&["outcome=shutdown"]),
                key(&["outcome=timed_out"]),
            ]
        );
        assert!(outcomes.values().all(|value| *value == 0.0));
        let settlements = fixture.samples(TRANSPORT_ACKNOWLEDGEMENTS, Unit::SINGLE);
        assert_eq!(settlements.len(), 4);
        assert!(settlements.contains_key(&key(&["operation=reject", "outcome=failed"])));
        assert!(settlements.values().all(|value| *value == 0.0));
        let durations = fixture.samples(TRANSPORT_DURATION, Unit::SINGLE);
        assert_eq!(
            durations,
            BTreeMap::from([
                (key(&["operation=acknowledge"]), 0.0),
                (key(&["operation=reject"]), 0.0),
            ])
        );
        assert_eq!(fixture.samples(RESUMES, Unit::SINGLE).len(), 3);
        assert_eq!(
            fixture
                .samples(SUSPENDS, Unit::SINGLE)
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            [
                key(&["cause=quiesce"]),
                key(&["cause=read_failure"]),
                key(&["cause=rejection"]),
            ]
        );
        assert_eq!(
            fixture.samples(UNCONFIRMED_STOPS, Unit::SINGLE),
            BTreeMap::from([(Vec::new(), 0.0)])
        );
        assert_eq!(
            fixture.samples(TRANSIENT_FAILURE, Unit::SINGLE),
            BTreeMap::from([(Vec::new(), 0.0)])
        );
    }

    #[test]
    fn each_observation_updates_only_its_own_child() {
        let fixture = Fixture::new();
        let metrics = fixture.unit(Unit::SINGLE);

        metrics.acknowledgement(&IngestorAcknowledgementOutcome::Acknowledged);
        metrics.acknowledgement(&IngestorAcknowledgementOutcome::TimedOut {
            silence: Duration::from_secs(2),
        });
        metrics.acknowledgement(&IngestorAcknowledgementOutcome::TimedOut {
            silence: Duration::from_secs(2),
        });
        metrics.acknowledgement(&IngestorAcknowledgementOutcome::Failed {
            reason: "refused".to_owned(),
        });
        metrics.transport_settlement(
            IngestorSettlementOperation::Reject,
            IngestorSettlementOutcome::Failed,
            Duration::from_millis(250),
        );
        metrics.transport_settlement(
            IngestorSettlementOperation::Acknowledge,
            IngestorSettlementOutcome::Succeeded,
            Duration::from_millis(500),
        );
        metrics.resume(IngestorResumeOutcome::Waiting);
        metrics.suspend(IngestorSuspendCause::ReadFailure);
        metrics.unconfirmed_stop(3);

        let outcomes = fixture.samples(ACK_OUTCOMES, Unit::SINGLE);
        assert_eq!(outcomes[&key(&["outcome=acknowledged"])], 1.0);
        assert_eq!(outcomes[&key(&["outcome=timed_out"])], 2.0);
        assert_eq!(outcomes[&key(&["outcome=failed"])], 1.0);
        assert_eq!(outcomes[&key(&["outcome=shutdown"])], 0.0);
        let settlements = fixture.samples(TRANSPORT_ACKNOWLEDGEMENTS, Unit::SINGLE);
        assert_eq!(
            settlements[&key(&["operation=reject", "outcome=failed"])],
            1.0
        );
        assert_eq!(
            settlements[&key(&["operation=acknowledge", "outcome=succeeded"])],
            1.0
        );
        assert_eq!(
            settlements[&key(&["operation=reject", "outcome=succeeded"])],
            0.0
        );
        assert_eq!(
            settlements[&key(&["operation=acknowledge", "outcome=failed"])],
            0.0
        );
        assert_eq!(metrics.series.reject.duration.get_sample_sum(), 0.25);
        assert_eq!(metrics.series.acknowledge.duration.get_sample_sum(), 0.5);
        let resumes = fixture.samples(RESUMES, Unit::SINGLE);
        assert_eq!(resumes[&key(&["outcome=waiting"])], 1.0);
        assert_eq!(resumes[&key(&["outcome=ready"])], 0.0);
        let suspends = fixture.samples(SUSPENDS, Unit::SINGLE);
        assert_eq!(suspends[&key(&["cause=read_failure"])], 1.0);
        assert_eq!(suspends[&key(&["cause=quiesce"])], 0.0);
        assert_eq!(
            fixture.samples(UNCONFIRMED_STOPS, Unit::SINGLE)[&Vec::new()],
            3.0
        );

        let restarted = fixture.unit(Unit::SINGLE);
        restarted.acknowledgement(&IngestorAcknowledgementOutcome::Shutdown);
        assert_eq!(
            fixture.samples(ACK_OUTCOMES, Unit::SINGLE)[&key(&["outcome=shutdown"])],
            1.0,
            "a later task generation of the unit keeps counting the same children"
        );
        assert_eq!(
            fixture.samples(ACK_OUTCOMES, Unit::SINGLE)[&key(&["outcome=timed_out"])],
            2.0
        );
    }

    #[test]
    fn the_failure_gauge_follows_only_the_current_task_binding() {
        let fixture = Fixture::new();
        let metrics = fixture.unit(Unit::SINGLE);
        let failed = Arc::new(AtomicBool::new(true));
        let source = failed.clone();
        let first = metrics.bind_failure_source(move || source.load(Ordering::Acquire));
        assert_eq!(
            fixture.samples(TRANSIENT_FAILURE, Unit::SINGLE)[&Vec::new()],
            1.0
        );
        failed.store(false, Ordering::Release);
        assert_eq!(
            fixture.samples(TRANSIENT_FAILURE, Unit::SINGLE)[&Vec::new()],
            0.0
        );

        let replacement = fixture.unit(Unit::SINGLE).bind_failure_source(|| true);
        drop(first);
        assert_eq!(
            fixture.samples(TRANSIENT_FAILURE, Unit::SINGLE)[&Vec::new()],
            1.0,
            "an ending predecessor cannot clear its replacement"
        );
        drop(replacement);
        assert_eq!(
            fixture.samples(TRANSIENT_FAILURE, Unit::SINGLE)[&Vec::new()],
            0.0,
            "a unit without a running task has no transient failure"
        );
    }

    #[test]
    fn describe_lists_every_child_under_the_unit_selector() {
        let fixture = Fixture::new();
        let metrics = fixture.unit(Unit::SINGLE);
        metrics.acknowledgement(&IngestorAcknowledgementOutcome::TimedOut {
            silence: Duration::from_secs(5),
        });
        metrics.transport_settlement(
            IngestorSettlementOperation::Reject,
            IngestorSettlementOutcome::Succeeded,
            Duration::from_millis(125),
        );
        let _binding = metrics.bind_failure_source(|| true);
        let mut lines = Vec::new();
        metrics.describe_lines("ingestor=\"input\"", &mut lines);

        assert_eq!(lines.len(), 18);
        assert!(
            lines.contains(
                &"nervix_ingestor_ack_outcomes_total{ingestor=\"input\",outcome=\"timed_out\"} \
                  total=1"
                    .to_owned()
            )
        );
        assert!(
            lines.contains(
                &"nervix_ingestor_ack_outcomes_total{ingestor=\"input\",outcome=\"failed\"} \
                  total=0"
                    .to_owned()
            )
        );
        assert!(
            lines.contains(
                &"nervix_ingestor_transport_acknowledgements_total{ingestor=\"input\",operation=\"\
                  reject\",outcome=\"succeeded\"} total=1"
                    .to_owned()
            )
        );
        assert!(
            lines.contains(
                &"nervix_ingestor_transport_acknowledgement_duration_seconds{ingestor=\"input\",\
                  operation=\"reject\"} count=1 sum_seconds=0.125"
                    .to_owned()
            )
        );
        assert!(
            lines.contains(
                &"nervix_ingestor_suspends_total{ingestor=\"input\",cause=\"read_failure\"} \
                  total=0"
                    .to_owned()
            )
        );
        assert_eq!(
            lines.last(),
            Some(&"nervix_ingestor_transient_failure{ingestor=\"input\"} value=1".to_owned())
        );
    }

    #[test]
    fn retiring_a_unit_withdraws_its_children_and_gauge_and_keeps_its_sibling() {
        let fixture = Fixture::new();
        let retired = fixture.unit(Unit::SINGLE);
        let sibling = fixture.unit(Unit::FIRST_INSTANCE);
        retired.suspend(IngestorSuspendCause::Quiesce);
        let _sibling_binding = sibling.bind_failure_source(|| true);

        fixture.metrics.remove_ingestor_unit(
            &fixture.domain,
            &fixture.ingestor,
            Unit::SINGLE,
            Some(&fixture.node),
        );

        for family in [
            ACK_OUTCOMES,
            TRANSPORT_ACKNOWLEDGEMENTS,
            TRANSPORT_DURATION,
            RESUMES,
            SUSPENDS,
            UNCONFIRMED_STOPS,
            TRANSIENT_FAILURE,
        ] {
            assert!(
                fixture.samples(family, Unit::SINGLE).is_empty(),
                "{family} keeps no child of the retired unit"
            );
            assert!(
                !fixture.samples(family, Unit::FIRST_INSTANCE).is_empty(),
                "{family} keeps the sibling's children"
            );
        }
        assert_eq!(
            fixture.samples(TRANSIENT_FAILURE, Unit::FIRST_INSTANCE)[&Vec::new()],
            1.0
        );
        let registered_again = fixture.unit(Unit::SINGLE);
        assert_eq!(
            fixture.samples(SUSPENDS, Unit::SINGLE)[&key(&["cause=quiesce"])],
            0.0,
            "a unit registered again after retirement starts from zero"
        );
        drop(registered_again);
    }
}
