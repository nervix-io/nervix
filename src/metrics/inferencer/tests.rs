//! Layer: harness.
//! Owns: zero registration, branch accounting, warning cadence and exact series retirement evidence.
//! Depends on: the production inference metric owner.
//! Must not know: tensor execution, persistence or graph scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "checks inference metric lifetimes")
)]

use super::*;

fn recorder(metrics: &RuntimeMetrics) -> InferencerStateMetrics {
    let domain = "observations".parse().assured("the domain is valid");
    let name = "scoring".parse().assured("the inferencer is valid");
    let physical = "node-1".parse().assured("the node is valid");
    metrics.inferencer_state_metrics(&domain, &name, Unit::SINGLE, Some(&physical))
}

#[test]
fn units_register_all_families_at_zero_and_keep_state_bytes_per_session() {
    let metrics = RuntimeMetrics::default();
    let unit = recorder(&metrics);
    assert_eq!(unit.0.bytes.get(), 0.0);
    assert_eq!(unit.0.failures.get(), 0);
    assert_eq!(unit.0.restored.get(), 0);
    assert_eq!(unit.0.reset.get(), 0);
    let text = metrics.prometheus_text();
    for family in [
        STATE_BYTES,
        SNAPSHOT_FAILURES,
        RESTORED_BRANCHES,
        RESET_BRANCHES,
    ] {
        assert!(
            text.contains(&format!("nervix_{family}{{")),
            "{family}: {text}"
        );
    }
    assert!(text.contains("target_kind=\"INFERENCER\""));
    assert!(text.contains("direction=\"state\""));
    let first = unit.track_branch(8);
    unit.initialized(false);
    let second = unit.track_branch(8);
    unit.initialized(true);
    assert_eq!(unit.0.bytes.get(), 16.0);
    assert_eq!(unit.0.reset.get(), 1);
    assert_eq!(unit.0.restored.get(), 1);
    drop(first);
    assert_eq!(unit.0.bytes.get(), 8.0);
    drop(second);
    assert_eq!(unit.0.bytes.get(), 0.0);
}

#[test]
fn retained_reporters_share_the_unit_and_last_drop_withdraws_its_series() {
    let metrics = RuntimeMetrics::default();
    let unit = recorder(&metrics);
    let writer = recorder(&metrics);
    let branch = unit.track_branch(8);
    drop(unit);
    drop(writer);
    assert!(
        metrics
            .prometheus_text()
            .contains("nervix_inferencer_state_bytes{")
    );
    drop(branch);
    assert!(
        !metrics
            .prometheus_text()
            .contains("nervix_inferencer_state_bytes{")
    );
    let replacement = recorder(&metrics);
    assert_eq!(replacement.0.reset.get(), 0);
    assert_eq!(replacement.0.bytes.get(), 0.0);
}

#[test]
fn a_retired_identity_cannot_withdraw_its_replacement() {
    let metrics = RuntimeMetrics::default();
    let previous = recorder(&metrics);
    let identity = previous.0.identity.clone();
    let key = previous.0.key.clone();
    drop(previous);
    let replacement = recorder(&metrics);
    metrics.series.inferencer.retire(&key, &identity);
    assert!(
        metrics
            .prometheus_text()
            .contains("nervix_inferencer_state_bytes{")
    );
    let branch = replacement.track_branch(4);
    assert_eq!(replacement.0.bytes.get(), 4.0);
    drop(branch);
}

#[test]
fn every_failure_counts_while_warnings_share_one_unit_cadence() {
    let metrics = RuntimeMetrics::default();
    let unit = recorder(&metrics);
    let other_branch = recorder(&metrics);
    let start = Instant::now();
    let cadence = Duration::from_secs(1);
    assert!(unit.snapshot_failed(start, cadence));
    assert!(!other_branch.snapshot_failed(start + Duration::from_millis(500), cadence));
    assert!(unit.snapshot_failed(start + cadence, cadence));
    assert!(!other_branch.snapshot_failed(start, cadence));
    assert_eq!(unit.0.failures.get(), 4);
}
