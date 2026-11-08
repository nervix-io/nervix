//! Layer: test harness.
//! - **Owns.** Zero children, process lifetimes, bounded outcomes and complete inspection checks.
//! - **Depends on.** The production interaction store and its Prometheus registry.
//! - **Must not know.** Runtime tasks, connector drivers or graph payloads.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests own metric witnesses")
)]

use super::*;

#[test]
fn ingestor_interaction_registration_preserves_every_execution_unit() {
    let metrics = RuntimeMetrics::default();
    let domain = DomainName::parse("metrics_test").assured("the fixture domain is valid");
    let source =
        nervix_models::IngestorName::parse("source").assured("the fixture ingestor is valid");
    let node = ClusterNodeName::parse("node-1").assured("the fixture node is valid");
    let children = [
        nervix_models::Unit::SINGLE,
        nervix_models::Unit::FIRST_INSTANCE,
        nervix_models::Unit::try_instance(u64::from(u32::MAX) + 1)
            .assured("the fixture instance fits the execution-unit range"),
    ]
    .map(|unit| {
        let (interactions, decode_rejections) = metrics.register_ingestor_interactions(
            &domain,
            &source,
            unit,
            Some(&node),
            nervix_models::IngestorEntityKind::Http(nervix_models::HttpIngestorEntity::Endpoint),
        );
        [
            interactions.unfolding_refused,
            interactions.transport_redeliveries,
            interactions.oversized_payloads,
            interactions.header_values_dropped,
            decode_rejections,
        ]
    });
    for (value, counters) in [1, 2, 3].into_iter().zip(&children) {
        for counter in counters {
            counter.inc_by(value);
        }
    }
    for (value, counters) in [1, 2, 3].into_iter().zip(&children) {
        for counter in counters {
            assert_eq!(counter.get(), value);
        }
    }
}

fn registered(
    metrics: &RuntimeMetrics,
    node: &str,
    kind: EmitterEntityKind,
) -> EmitterInteractionMetrics {
    metrics.register_emitter_interactions(
        &DomainName::parse("metrics_test").assured("the fixture domain is a valid literal"),
        &EmitterName::parse("output").assured("the fixture emitter is a valid literal"),
        Some(&ClusterNodeName::parse(node).assured("the fixture node is a valid name")),
        kind,
    )
}

fn redis() -> EmitterEntityKind {
    EmitterEntityKind::Redis(nervix_models::RedisEmitterEntity::PubSub)
}

#[test]
fn every_bounded_child_is_registered_at_zero_with_the_vocabulary_labels() {
    let metrics = RuntimeMetrics::default();
    let handle = registered(&metrics, "node-1", redis());
    let families = metrics.series.prometheus.registry.gather();
    let mut attempts = 0;
    let mut durations = 0;
    for family in families {
        if family.name() != PUBLISH_ATTEMPTS && family.name() != PUBLISH_DURATION {
            continue;
        }
        for sample in family.get_metric() {
            let labels = sample
                .get_label()
                .iter()
                .map(|label| (label.name(), label.value()))
                .collect::<std::collections::BTreeMap<_, _>>();
            assert_eq!(labels["domain"], "metrics_test");
            assert_eq!(labels["emitter"], "output");
            assert_eq!(labels["physical_node_id"], "node-1");
            assert_eq!(labels["connector"], redis().connector().to_string());
            assert_eq!(labels["entity"], redis().entity_keyword());
            assert!(["publish", "commit"].contains(&labels["operation"]));
            if family.name() == PUBLISH_ATTEMPTS {
                assert_eq!(labels.len(), 7);
                assert!(
                    ["succeeded", "transient", "misconfigured", "violation"]
                        .contains(&labels["outcome"])
                );
                assert_eq!(sample.get_counter().value(), 0.0);
                attempts += 1;
            } else {
                assert_eq!(labels.len(), 6);
                assert_eq!(sample.get_histogram().sample_count(), 0);
                assert_eq!(sample.get_histogram().sample_sum(), 0.0);
                durations += 1;
            }
        }
    }
    assert_eq!(attempts, 8);
    assert_eq!(durations, 2);
    assert_eq!(handle.describe_lines().len(), 35);
}

#[test]
fn outcomes_and_operations_increment_once_and_histograms_keep_the_exact_elapsed_measure() {
    let metrics = RuntimeMetrics::default();
    let handle = registered(&metrics, "node-1", redis());
    handle
        .begin(EmitterAttemptOperation::Publish)
        .finish(EmitterAttemptOutcome::Succeeded);
    handle
        .begin(EmitterAttemptOperation::Publish)
        .finish(EmitterAttemptOutcome::Transient);
    handle
        .begin(EmitterAttemptOperation::Commit)
        .finish(EmitterAttemptOutcome::Misconfigured);
    handle
        .begin(EmitterAttemptOperation::Commit)
        .finish(EmitterAttemptOutcome::Violation);
    assert_eq!(handle.series.publish.succeeded.prometheus.get(), 1);
    assert_eq!(handle.series.publish.succeeded.series.summary().value, 1);
    assert_eq!(handle.series.publish.transient.prometheus.get(), 1);
    assert_eq!(handle.series.publish.violation.prometheus.get(), 0);
    assert_eq!(handle.series.commit.misconfigured.prometheus.get(), 1);
    assert_eq!(handle.series.commit.violation.prometheus.get(), 1);
    assert_eq!(handle.series.publish.duration.get_sample_count(), 2);
    assert_eq!(handle.series.commit.duration.get_sample_count(), 2);

    let exact = registered(&metrics, "node-2", redis());
    exact
        .series
        .publish
        .record(EmitterAttemptOutcome::Succeeded, Duration::from_millis(250));
    assert_eq!(exact.series.publish.duration.get_sample_count(), 1);
    assert_eq!(exact.series.publish.duration.get_sample_sum(), 0.25);
}

#[test]
fn abandoning_a_started_attempt_records_one_transient_answer_and_one_duration() {
    let metrics = RuntimeMetrics::default();
    let handle = registered(&metrics, "node-1", redis());
    let attempt = handle.begin(EmitterAttemptOperation::Publish);
    drop(attempt);
    assert_eq!(handle.series.publish.transient.prometheus.get(), 1);
    assert_eq!(handle.series.publish.transient.series.summary().value, 1);
    assert_eq!(handle.series.publish.duration.get_sample_count(), 1);
    assert_eq!(handle.series.publish.succeeded.prometheus.get(), 0);
}

#[test]
fn registration_keeps_process_totals_and_separates_nodes_entities_and_processes() {
    let metrics = RuntimeMetrics::default();
    let handle = registered(&metrics, "node-1", redis());
    handle
        .begin(EmitterAttemptOperation::Publish)
        .finish(EmitterAttemptOutcome::Succeeded);
    drop(handle);
    let restarted = registered(&metrics, "node-1", redis());
    assert_eq!(restarted.series.publish.succeeded.prometheus.get(), 1);
    assert_eq!(restarted.series.publish.succeeded.series.summary().value, 1);
    let other_node = registered(&metrics, "node-2", redis());
    assert_eq!(other_node.series.publish.succeeded.prometheus.get(), 0);
    let other_entity = registered(
        &metrics,
        "node-1",
        EmitterEntityKind::Kafka(nervix_models::KafkaEmitterEntity::Topic),
    );
    assert_eq!(other_entity.series.publish.succeeded.prometheus.get(), 0);
    let other_process = registered(&RuntimeMetrics::default(), "node-1", redis());
    assert_eq!(other_process.series.publish.succeeded.prometheus.get(), 0);

    let entity = DomainNodeRef::node_in(
        DomainName::parse("metrics_test").assured("valid fixture literal"),
        ModelKind::Emitter,
        EmitterName::parse("output").assured("valid fixture literal"),
    );
    let lines = metrics.describe_emitter_interactions(&entity);
    assert_eq!(lines[0], "destination:");
    assert_eq!(lines.len(), 106);
    let missing = DomainNodeRef::node_in(
        entity.domain,
        ModelKind::Emitter,
        EmitterName::parse("missing").assured("valid fixture literal"),
    );
    assert!(metrics.describe_emitter_interactions(&missing).is_empty());
}

#[test]
fn inspection_names_the_family_selector_total_and_wall_rates_without_domain_time() {
    let metrics = RuntimeMetrics::default();
    let handle = registered(&metrics, "node-1", redis());
    handle
        .begin(EmitterAttemptOperation::Publish)
        .finish(EmitterAttemptOutcome::Succeeded);
    let lines = handle.describe_lines();
    assert!(lines[0].starts_with(
        "nervix_emitter_publish_attempts_total{domain=\"metrics_test\",emitter=\"output\",\
         physical_node_id=\"node-1\",connector=\"REDIS\",entity=\"PUBSUB\",operation=\"publish\",\
         outcome=\"succeeded\"} total=1 wall_rate_per_sec="
    ));
    for line in lines {
        assert!(!line.contains("NaN"));
        assert!(!line.contains("inf"));
        if line.starts_with(PUBLISH_ATTEMPTS) {
            assert!(line.contains(" domain_rate_per_sec=- "));
            assert!(line.contains(" wall_rate_ema_1m_per_sec="));
            assert!(line.contains(" wall_rate_ema_15m_per_sec="));
            assert!(line.ends_with("domain_rate_ema_1m_per_sec=- domain_rate_ema_15m_per_sec=-"));
        }
    }
}

struct PayloadProbe<'a>(&'a RuntimeMetrics);

impl PayloadProbe<'_> {
    fn gauge(&self, family: &str, state: Option<&str>) -> f64 {
        for metric_family in self.0.series.prometheus.registry.gather() {
            if metric_family.name() != family {
                continue;
            }
            for sample in metric_family.get_metric() {
                if let Some(state) = state
                    && !sample
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "state" && label.value() == state)
                {
                    continue;
                }
                return sample.get_gauge().value();
            }
        }
        panic!("required gauge {family} with state {state:?} was not registered");
    }
}

#[test]
fn retiring_one_work_owner_preserves_its_replacement_and_process_preparation_totals() {
    let metrics = RuntimeMetrics::default();
    let handle = registered(&metrics, "node-1", redis());
    handle.record_payload(3, 31, 38);
    handle.record_splits(2);
    let mut first = handle.work();
    first.set(EmitterWorkKind::Buffered, 3);
    first.set(EmitterWorkKind::Prepared, 3);
    first.set(EmitterWorkKind::Payloads, 1);
    first.set(EmitterWorkKind::Bytes, 38);
    let restarted = registered(&metrics, "node-1", redis());
    let mut second = restarted.work();
    second.set(EmitterWorkKind::Buffered, 5);
    second.set(EmitterWorkKind::InFlight, 2);
    second.set(EmitterWorkKind::Staged, 1);
    let probe = PayloadProbe(&metrics);
    assert_eq!(
        probe.gauge("nervix_emitter_work_messages", Some("buffered")),
        8.0
    );
    assert_eq!(probe.gauge("nervix_emitter_prepared_bytes", None), 38.0);
    first.set(EmitterWorkKind::Buffered, 1);
    assert_eq!(
        probe.gauge("nervix_emitter_work_messages", Some("buffered")),
        6.0
    );
    drop(first);
    assert_eq!(
        probe.gauge("nervix_emitter_work_messages", Some("buffered")),
        5.0
    );
    assert_eq!(
        probe.gauge("nervix_emitter_work_messages", Some("prepared")),
        0.0
    );
    assert_eq!(
        probe.gauge("nervix_emitter_work_messages", Some("in_flight")),
        2.0
    );
    assert_eq!(probe.gauge("nervix_emitter_prepared_bytes", None), 0.0);
    assert_eq!(probe.gauge("nervix_emitter_prepared_payloads", None), 0.0);
    drop(second);
    for state in ["buffered", "prepared", "in_flight", "staged"] {
        assert_eq!(
            probe.gauge("nervix_emitter_work_messages", Some(state)),
            0.0
        );
    }
    let lines = restarted.describe_lines().join("\n");
    assert!(lines.contains(
        "payloads: 1 source_messages=3 encoded_bytes=31 size_bound_bytes=31 prepared_bytes=38"
    ));
    assert!(lines.contains("payload_splits: 2"));
    assert!(lines.contains("messages_per_payload: count=1 source_messages=3"));
}

#[test]
fn staged_work_keeps_the_unsigned_connector_count_range() {
    let metrics = RuntimeMetrics::default();
    let handle = registered(&metrics, "node-1", redis());
    let mut first = handle.work();
    let mut second = handle.work();
    first.set(EmitterWorkKind::Staged, u64::MAX - 7);
    second.set(EmitterWorkKind::Staged, 5);
    let assert_count = |count: u64| {
        let expected = format!(" staged={count} prepared_payloads=");
        assert!(
            handle
                .describe_lines()
                .iter()
                .any(|line| line.contains(&expected))
        );
    };
    assert_count(u64::MAX - 2);
    drop(first);
    assert_count(5);
    second.set(EmitterWorkKind::Staged, 2);
    assert_count(2);
    drop(second);
    assert_count(0);
}
