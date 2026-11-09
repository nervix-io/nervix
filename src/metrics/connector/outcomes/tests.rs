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
    assert_eq!(families, 12);
    assert_eq!(samples, 21);
    assert_eq!(handle.describe_lines().len(), 36);
}

#[test]
fn emitter_outcomes_keep_source_record_units_and_fractional_wait_seconds() {
    let (metrics, handle) = fixture();
    {
        let mut outcomes = handle.record_outcomes();
        outcomes.reject(EmitterRejectionCode::External);
        outcomes.reject(EmitterRejectionCode::External);
        outcomes.reject(EmitterRejectionCode::Oversize);
        outcomes.reject(EmitterRejectionCode::Invalid);
        outcomes.reject(EmitterRejectionCode::Encode);
        outcomes.fail();
        outcomes.fail();
        outcomes.fail();
        assert_eq!(sample(&metrics, FAILED, &[]), 0.0);
    }
    for (code, expected) in [
        ("external", 2.0),
        ("oversize", 1.0),
        ("invalid", 1.0),
        ("encode", 1.0),
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
         physical_node_id=\"node-1\",connector=\"REDIS\",entity=\"PUBSUB\"} total=0.875"
    ));
    assert!(description.contains(
        "nervix_emitter_pool_wait_seconds_total{domain=\"outcomes\",emitter=\"destination\",\
         physical_node_id=\"node-1\",connector=\"REDIS\",entity=\"PUBSUB\"} total=0.375"
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
