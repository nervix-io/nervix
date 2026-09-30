//! How processor validation reports duration text that names no duration.
//!
//! Layer: test harness.
//! - **Owns.** Examples of every processor duration setting whose text is malformed or longer than
//!   a duration can be, and the diagnostic each owner keeps for it.
//! - **Depends on.** Processor decisions, registry storage and the registry test fixtures.
//! - **Must not know.** How the runtime plans or executes a processor.

use std::fs;

use nervix_models::{InputCollectPolicy, Model};
use nervix_recovery::Reported as _;

use super::*;
use crate::registry::{
    storage::Registry,
    test_fixtures::{
        TOO_LONG_DURATION_TEXT, example_graph_models, named, temp_db_path, unbranched_correlator,
    },
};

/// Asserts that `error` rejects an invalid Model for exactly `expected`.
fn assert_invalid_model_reason(error: &Report<RegistryError>, expected: &str) {
    assert!(
        matches!(
            error.current_context(),
            RegistryError::InvalidModel { reason, .. } if reason == expected
        ),
        "{error:?}"
    );
}

#[test]
fn apply_batch_says_why_a_retention_max_time_names_no_duration() {
    let deduplicator = r#"
        CREATE SCHEMA metric (value I64);
        CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
        CREATE RELAY deduped_metrics SCHEMA metric UNBRANCHED;
        CREATE DEDUPLICATOR dedup_metrics
          FROM raw_metrics
          DEDUPLICATE ON input.value
          MAX TIME 10m
          UNBRANCHED
          TO deduped_metrics INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        "#;
    let reorderer = r#"
        CREATE SCHEMA metric (value I64);
        CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
        CREATE RELAY ordered_metrics SCHEMA metric UNBRANCHED;
        CREATE REORDERER order_metrics
          FROM raw_metrics
          BY input.value
          MAX TIME 10s
          UNBRANCHED
          TO ordered_metrics INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        "#;
    for (value, why) in [
        ("oops", "expected number at 0"),
        (
            TOO_LONG_DURATION_TEXT,
            "it is longer than a duration can be",
        ),
    ] {
        for (source, kind) in [(deduplicator, "deduplicator"), (reorderer, "reorderer")] {
            let (domain, mut models) = example_graph_models("retention max time", source);
            for model in &mut models {
                match model {
                    Model::Deduplicator(deduplicator) => {
                        deduplicator.max_time = value.to_string();
                    }
                    Model::Reorderer(reorderer) => reorderer.max_time = value.to_string(),
                    _ => {}
                }
            }
            let path = temp_db_path();
            let registry = Registry::open(&path).expect("registry should open");
            let error = registry
                .apply_batch(&domain, models)
                .expect_err("the MAX TIME names no duration");
            assert_invalid_model_reason(
                &error,
                &format!("invalid {kind} MAX TIME '{value}': {why}"),
            );
            fs::remove_dir_all(path).reported("remove the registry test directory");
        }
    }
}

#[test]
fn processor_durations_say_why_their_text_names_no_duration() {
    let domain = named::<DomainName>("default");
    let identifier = named::<ModelName>("orders_processor");
    for (value, why) in [
        ("oops", "expected number at 0"),
        (
            TOO_LONG_DURATION_TEXT,
            "it is longer than a duration can be",
        ),
    ] {
        let policy = InputCollectPolicy {
            collect_for: value.to_string(),
            max_batch_size: None,
        };
        let error = ensure_input_collect_policy(&domain, &identifier, Some(&policy), "junction")
            .expect_err("the collection interval names no duration");
        assert_invalid_model_reason(
            &error,
            &format!("invalid junction COLLECT FOR duration '{value}': {why}"),
        );

        let error = parse_window_bound_duration(&domain, &identifier, "WIDTH", Some(value))
            .expect_err("the window width names no duration");
        assert_invalid_model_reason(
            &error,
            &format!("invalid window WIDTH duration '{value}': {why}"),
        );

        let outputs = ProcessorOutputs::single(named("projected_events")).with_flush_policy(
            FlushPolicy::Each {
                interval: value.to_string(),
                max_batch_size: "1MiB".to_string(),
            },
        );
        let error = ensure_processor_output_flush_policies(&domain, &identifier, &outputs)
            .expect_err("the flush interval names no duration");
        assert_invalid_model_reason(
            &error,
            &format!("invalid TO output 'projected_events' FLUSH EACH duration '{value}': {why}"),
        );

        let Model::Correlator(mut correlator) =
            unbranched_correlator("pairs", "left_events", "right_events", "paired_events")
        else {
            panic!("the fixture builds a correlator");
        };
        correlator.max_time = value.to_string();
        let error = validate_correlator(
            &domain,
            &identifier,
            &ModelIndex::new(),
            &correlator,
            &[],
            &[],
        )
        .expect_err("the correlator MAX TIME names no duration");
        assert_invalid_model_reason(
            &error,
            &format!("invalid correlator MAX TIME '{value}': {why}"),
        );
    }
}
