//! Layer: test harness.
//! - **Owns.** Zero registration, bounded dimensions, event units and exact observation lifetimes.
//! - **Depends on.** The production metric registry and retained emitter handle.
//! - **Must not know.** Runtime policy, graph data or connector drivers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "qualifies the production observation owner")
)]

use nervix_models::{
    ClusterNodeName, DomainName, EmitterEntityKind, EmitterName, RedisEmitterEntity,
};
use nervix_primitives::sync::atomic::{AtomicBool, Ordering};
use prometheus::proto::MetricType;

use super::*;
use crate::metrics::RuntimeMetrics;

fn fixture() -> (RuntimeMetrics, EmitterInteractionMetrics) {
    let metrics = RuntimeMetrics::default();
    let handle = metrics.register_emitter_interactions(
        &DomainName::parse("outcomes").assured("fixture domain"),
        &EmitterName::parse("destination").assured("fixture emitter"),
        nervix_models::Unit::SINGLE,
        Some(&ClusterNodeName::parse("node-1").assured("fixture node")),
        EmitterEntityKind::Redis(RedisEmitterEntity::PubSub),
    );
    (metrics, handle)
}

fn sample(metrics: &RuntimeMetrics, name: &str, labels: &[(&str, &str)]) -> f64 {
    for family in metrics.series.prometheus.registry.gather() {
        if family.name() != name {
            continue;
        }
        for metric in family.get_metric() {
            let matches = labels.iter().all(|(key, value)| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == *key && label.value() == *value)
            });
            if !matches {
                continue;
            }
            return match family.get_field_type() {
                MetricType::COUNTER => metric.get_counter().value(),
                MetricType::GAUGE => metric.get_gauge().value(),
                kind => panic!("unexpected outcome observation type {kind:?}"),
            };
        }
    }
    panic!("missing outcome observation {name} with {labels:?}");
}

#[test]
fn emitter_outcomes_register_every_child_at_zero_with_only_bounded_dimensions() {
    let (metrics, handle) = fixture();
    let names = [
        REJECTED,
        FAILED,
        RETRIED,
        RETRIES,
        RETRY_WAIT,
        GIVE_UPS,
        OPENS,
        POOL_WAITS,
        POOL_SECONDS,
        CONFIRMATION_TIMEOUTS,
        FINISHES,
        UNSENT,
        status::EMITTER_FAILURE.name(),
    ];
    let mut samples = 0;
    let mut families = 0;
    for family in metrics.series.prometheus.registry.gather() {
        if !names.contains(&family.name()) {
            continue;
        }
        families += 1;
        for metric in family.get_metric() {
            samples += 1;
            let labels = metric
                .get_label()
                .iter()
                .map(|label| (label.name(), label.value()))
                .collect::<std::collections::BTreeMap<_, _>>();
            assert_eq!(labels["domain"], "outcomes");
            assert_eq!(labels["emitter"], "destination");
            assert_eq!(labels["physical_node_id"], "node-1");
            assert_eq!(labels["instance"], "-");
            assert_eq!(labels["connector"], "REDIS");
            assert_eq!(labels["entity"], "PUBSUB");
            for key in labels.keys() {
                assert!(EMITTER_LABELS.contains(key) || ["code", "cause", "outcome"].contains(key));
            }
            let value = match family.get_field_type() {
                MetricType::COUNTER => metric.get_counter().value(),
                MetricType::GAUGE => metric.get_gauge().value(),
                kind => panic!("unexpected observation {kind:?}"),
            };
            assert_eq!(value, 0.0);
        }
    }
    assert_eq!(families, 13);
    // The rejection family holds one child for each of the six classes of the REJECTED group,
    // `destination_limit` among them beside the `limit` it refines.
    assert_eq!(samples, 24);
    assert_eq!(handle.describe_lines().len(), 39);
}

/// Each unit of work the host gives up counts once, and an empty give-up counts nothing.
#[test]
fn emitter_give_ups_count_every_unit_given_up() {
    let (metrics, handle) = fixture();

    handle.record_give_ups(0);
    assert_eq!(sample(&metrics, GIVE_UPS, &[]), 0.0);
    handle.record_give_ups(2);
    handle.record_give_ups(1);

    assert_eq!(sample(&metrics, GIVE_UPS, &[]), 3.0);
    assert!(
        handle
            .describe_lines()
            .iter()
            .any(|line| line.starts_with(GIVE_UPS) && line.ends_with("total=3")),
        "DESCRIBE reports the give-up family with its total"
    );
}

#[test]
fn emitter_outcomes_keep_source_record_units_and_fractional_wait_seconds() {
    let (metrics, handle) = fixture();
    {
        let mut outcomes = handle.record_outcomes();
        outcomes.reject(RejectedErrorClass::Refused);
        outcomes.reject(RejectedErrorClass::Refused);
        outcomes.reject(RejectedErrorClass::Limit);
        outcomes.reject(RejectedErrorClass::DestinationLimit);
        outcomes.reject(RejectedErrorClass::DestinationLimit);
        outcomes.reject(RejectedErrorClass::DestinationLimit);
        outcomes.reject(RejectedErrorClass::Validation);
        outcomes.reject(RejectedErrorClass::Codec);
        outcomes.fail();
        outcomes.fail();
        outcomes.fail();
        assert_eq!(sample(&metrics, FAILED, &[]), 0.0);
    }
    // A destination's limit is counted under its own class, not under the `limit` it refines.
    for (code, expected) in [
        ("refused", 2.0),
        ("limit", 1.0),
        ("destination_limit", 3.0),
        ("validation", 1.0),
        ("codec", 1.0),
        ("evaluation", 0.0),
    ] {
        assert_eq!(sample(&metrics, REJECTED, &[("code", code)]), expected);
    }
    assert_eq!(sample(&metrics, FAILED, &[]), 3.0);
    handle.record_retried(7);
    assert_eq!(sample(&metrics, RETRIED, &[]), 7.0);
    for (cause, delay) in [
        (EmitterRetryCause::Publish, 125),
        (EmitterRetryCause::Connect, 250),
        (EmitterRetryCause::Commit, 500),
    ] {
        handle.record_retry(cause, Duration::from_millis(delay));
        assert_eq!(sample(&metrics, RETRIES, &[("cause", cause.as_ref())]), 1.0);
    }
    assert_eq!(sample(&metrics, RETRY_WAIT, &[]), 0.875);
    handle.record_pool_wait(Duration::from_millis(125));
    handle.record_pool_wait(Duration::from_millis(250));
    assert_eq!(sample(&metrics, POOL_WAITS, &[]), 2.0);
    assert_eq!(sample(&metrics, POOL_SECONDS, &[]), 0.375);
    handle.record_confirmation_timeout();
    assert_eq!(sample(&metrics, CONFIRMATION_TIMEOUTS, &[]), 1.0);
    let description = handle.describe_lines().join("\n");
    assert!(description.contains(
        "nervix_emitter_retry_wait_seconds_total{domain=\"outcomes\",emitter=\"destination\",\
         physical_node_id=\"node-1\",instance=\"-\",connector=\"REDIS\",entity=\"PUBSUB\"} \
         total=0.875"
    ));
    assert!(description.contains(
        "nervix_emitter_pool_wait_seconds_total{domain=\"outcomes\",emitter=\"destination\",\
         physical_node_id=\"node-1\",instance=\"-\",connector=\"REDIS\",entity=\"PUBSUB\"} \
         total=0.375"
    ));
}

#[test]
fn emitter_outcomes_observe_cancelled_attempts_and_exact_unsent_records() {
    let (metrics, handle) = fixture();
    handle.begin_open().prepared();
    assert_eq!(sample(&metrics, OPENS, &[("outcome", "succeeded")]), 0.0);
    assert_eq!(sample(&metrics, OPENS, &[("outcome", "failed")]), 0.0);
    handle.begin_open().finish(EmitterOpenOutcome::Succeeded);
    drop(handle.begin_open());
    assert_eq!(sample(&metrics, OPENS, &[("outcome", "succeeded")]), 1.0);
    assert_eq!(sample(&metrics, OPENS, &[("outcome", "failed")]), 1.0);
    for outcome in [
        EmitterFinishOutcome::NothingHeld,
        EmitterFinishOutcome::Flushed,
    ] {
        handle.begin_finish().finish(outcome, 0);
    }
    handle
        .begin_finish()
        .finish(EmitterFinishOutcome::Unsent, 3);
    drop(handle.begin_finish());
    for outcome in ["nothing_held", "flushed", "unsent", "failed"] {
        assert_eq!(sample(&metrics, FINISHES, &[("outcome", outcome)]), 1.0);
    }
    assert_eq!(sample(&metrics, UNSENT, &[]), 3.0);
}

#[test]
fn emitter_outcomes_expire_only_the_exact_task_observation_and_retain_history() {
    let (metrics, handle) = fixture();
    let failed = Arc::new(AtomicBool::new(true));
    let source = failed.clone();
    let first = handle.bind_failure_source(move || source.load(Ordering::Acquire));
    handle.record_retried(2);
    assert_eq!(sample(&metrics, status::EMITTER_FAILURE.name(), &[]), 1.0);
    failed.store(false, Ordering::Release);
    assert_eq!(sample(&metrics, status::EMITTER_FAILURE.name(), &[]), 0.0);
    failed.store(true, Ordering::Release);
    let replacement_failed = Arc::new(AtomicBool::new(false));
    let source = replacement_failed.clone();
    let replacement = handle.bind_failure_source(move || source.load(Ordering::Acquire));
    drop(first);
    assert_eq!(sample(&metrics, status::EMITTER_FAILURE.name(), &[]), 0.0);
    replacement_failed.store(true, Ordering::Release);
    assert_eq!(sample(&metrics, status::EMITTER_FAILURE.name(), &[]), 1.0);
    assert!(
        handle
            .describe_lines()
            .iter()
            .any(|line| line.starts_with(status::EMITTER_FAILURE.name())
                && line.ends_with("value=1"))
    );
    drop(replacement);
    assert_eq!(sample(&metrics, status::EMITTER_FAILURE.name(), &[]), 0.0);
    assert_eq!(sample(&metrics, RETRIED, &[]), 2.0);
    let ending_after_replacement = handle.bind_failure_source(|| true);
    let replacement_ending_first = handle.bind_failure_source(|| true);
    drop(replacement_ending_first);
    assert_eq!(sample(&metrics, status::EMITTER_FAILURE.name(), &[]), 0.0);
    drop(ending_after_replacement);
    assert_eq!(sample(&metrics, status::EMITTER_FAILURE.name(), &[]), 0.0);
}
