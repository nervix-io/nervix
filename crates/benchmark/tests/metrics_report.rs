use meticulous::ResultExt as _;
use nervix_benchmark::{
    HistogramError, MetricSeries, MetricsReportError, NervixMetricsReport, PercentileError,
    PrometheusSampleError, Quantile, ReportEntry,
};
use nervix_models::ClusterNodeName;

const PROMETHEUS_FIXTURE: &str = r#"
# HELP nervix_messages_total Total graph messages observed by Nervix runtime targets.
# TYPE nervix_messages_total counter
nervix_messages_total{direction="sent",domain="benchmark_run",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 6144
nervix_batches_total{direction="sent",domain="benchmark_run",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 6
nervix_messages_per_batch_bucket{direction="sent",domain="benchmark_run",le="500",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 0
nervix_messages_per_batch_bucket{direction="sent",domain="benchmark_run",le="1024",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 4
nervix_messages_per_batch_bucket{direction="sent",domain="benchmark_run",le="2048",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 6
nervix_messages_per_batch_bucket{direction="sent",domain="benchmark_run",le="+Inf",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 6
nervix_messages_per_batch_sum{direction="sent",domain="benchmark_run",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 6144
nervix_messages_per_batch_count{direction="sent",domain="benchmark_run",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 6
nervix_relay_buffer_len_bucket{direction="concrete",domain="benchmark_run",le="0",peer="",peer_kind="",physical_node_id="node-1",relay="ingested",target="ingested",target_kind="RELAY"} 4
nervix_relay_buffer_len_bucket{direction="concrete",domain="benchmark_run",le="1",peer="",peer_kind="",physical_node_id="node-1",relay="ingested",target="ingested",target_kind="RELAY"} 6
nervix_relay_buffer_len_bucket{direction="concrete",domain="benchmark_run",le="4",peer="",peer_kind="",physical_node_id="node-1",relay="ingested",target="ingested",target_kind="RELAY"} 9
nervix_relay_buffer_len_bucket{direction="concrete",domain="benchmark_run",le="8",peer="",peer_kind="",physical_node_id="node-1",relay="ingested",target="ingested",target_kind="RELAY"} 10
nervix_relay_buffer_len_bucket{direction="concrete",domain="benchmark_run",le="+Inf",peer="",peer_kind="",physical_node_id="node-1",relay="ingested",target="ingested",target_kind="RELAY"} 10
nervix_relay_buffer_len_count{direction="concrete",domain="benchmark_run",peer="",peer_kind="",physical_node_id="node-1",relay="ingested",target="ingested",target_kind="RELAY"} 10
nervix_messages_total{direction="sent",domain="another_domain",peer="ignored",peer_kind="RELAY",physical_node_id="node-1",relay="ignored",target="ignored",target_kind="INGESTOR"} 99
"#;

#[test]
fn derives_target_batch_sizes_and_relay_percentiles_from_prometheus_histograms() {
    let report = NervixMetricsReport::from_prometheus(PROMETHEUS_FIXTURE, "benchmark_run")
        .expect("Prometheus metrics should produce a benchmark report");

    assert_eq!(report.batch_targets.len(), 1);
    let target = &report.batch_targets[0];
    assert_eq!(target.target_kind, "INGESTOR");
    assert_eq!(
        target.physical_node_id,
        Some(ClusterNodeName::parse("node-1").expect("valid name"))
    );
    assert_eq!(target.target, "kafka_in");
    assert_eq!(target.direction, "sent");
    assert_eq!(target.relay, "ingested");
    assert_eq!(target.messages_total, 6_144);
    assert_eq!(target.batches_total, 6);
    assert_eq!(target.mean_messages_per_batch(), 1_024.0);
    assert_eq!(target.p50, 1_024.0);
    assert_eq!(target.p90, 2_048.0);
    assert_eq!(target.p99, 2_048.0);

    assert_eq!(report.relay_buffers.len(), 1);
    let relay = &report.relay_buffers[0];
    assert_eq!(relay.relay, "ingested");
    assert_eq!(relay.direction, "concrete");
    assert_eq!(relay.observations, 10);
    assert_eq!(relay.p50, 1.0);
    assert_eq!(relay.p90, 4.0);
    assert_eq!(relay.p99, 8.0);
}

#[test]
fn cluster_scrapes_do_not_count_the_same_node_label_twice() {
    let single = NervixMetricsReport::from_prometheus(PROMETHEUS_FIXTURE, "benchmark_run")
        .assured("the static fixture contains a complete metrics report");
    let cluster = NervixMetricsReport::from_prometheus_scrapes(
        [PROMETHEUS_FIXTURE, PROMETHEUS_FIXTURE],
        "benchmark_run",
    )
    .assured("two copies of the complete fixture describe the same cluster metrics");

    assert_eq!(cluster, single);
}

#[test]
fn rejects_a_batch_histogram_without_its_batch_counter() {
    let metrics = PROMETHEUS_FIXTURE.replace(
        "nervix_batches_total{direction=\"sent\",domain=\"benchmark_run\",peer=\"ingested\",\
         peer_kind=\"RELAY\",physical_node_id=\"node-1\",relay=\"ingested\",target=\"kafka_in\",\
         target_kind=\"INGESTOR\"} 6\n",
        "",
    );

    let error = NervixMetricsReport::from_prometheus(&metrics, "benchmark_run")
        .expect_err("a target without batches_total must be rejected");
    assert!(matches!(
        error.current_context(),
        MetricsReportError::MissingTargetMetric {
            metric: "nervix_batches_total",
            ..
        }
    ));
    assert_eq!(
        format!("{error:#}"),
        "target INGESTOR 'kafka_in' direction 'sent' relay 'ingested' on 'node-1' is missing \
         Prometheus metric 'nervix_batches_total'"
    );
}

#[test]
fn reads_the_absent_owner_label_as_no_cluster_node() {
    let metrics =
        PROMETHEUS_FIXTURE.replace("physical_node_id=\"node-1\"", "physical_node_id=\"-\"");

    let report = NervixMetricsReport::from_prometheus(&metrics, "benchmark_run")
        .expect("a series observed without a placed owner should still produce a report");

    assert_eq!(report.batch_targets[0].physical_node_id, None);
    assert_eq!(report.relay_buffers[0].physical_node_id, None);
}

#[test]
fn rejects_an_owner_label_that_is_not_a_cluster_node_name() {
    let metrics =
        PROMETHEUS_FIXTURE.replace("physical_node_id=\"node-1\"", "physical_node_id=\"node 1\"");

    let error = NervixMetricsReport::from_prometheus(&metrics, "benchmark_run")
        .expect_err("a physical_node_id that is not a cluster node name must be rejected");
    assert!(matches!(
        error.current_context(),
        MetricsReportError::InvalidPrometheusSample { line: 4 }
    ));
    assert!(matches!(
        error.downcast_ref::<PrometheusSampleError>(),
        Some(PrometheusSampleError::InvalidPhysicalNode { value }) if value == "node 1"
    ));
    let rendered = format!("{error:#}");
    assert!(
        rendered.starts_with(
            "invalid Prometheus sample on line 4: label 'physical_node_id' value 'node 1' is not \
             a cluster node name: "
        ),
        "{rendered}"
    );
}

#[test]
fn names_the_line_and_the_unparsable_value_of_a_sample_once() {
    let metrics = PROMETHEUS_FIXTURE.replace(
        "target_kind=\"INGESTOR\"} 6144",
        "target_kind=\"INGESTOR\"} six",
    );

    let error = NervixMetricsReport::from_prometheus(&metrics, "benchmark_run")
        .expect_err("a sample value that is not a number must be rejected");

    assert!(matches!(
        error.current_context(),
        MetricsReportError::InvalidPrometheusSample { line: 4 }
    ));
    assert!(matches!(
        error.downcast_ref::<PrometheusSampleError>(),
        Some(PrometheusSampleError::InvalidValue { value }) if value == "six"
    ));
    let cause = error
        .downcast_ref::<std::num::ParseFloatError>()
        .expect("the number parser's failure should stay beneath the sample");
    assert_eq!(
        format!("{error:#}"),
        format!("invalid Prometheus sample on line 4: invalid sample value 'six': {cause}")
    );
}

#[test]
fn names_the_histogram_and_the_property_that_does_not_hold() {
    let metrics = PROMETHEUS_FIXTURE.replace(
        "le=\"+Inf\",peer=\"ingested\",peer_kind=\"RELAY\",physical_node_id=\"node-1\",relay=\"\
         ingested\",target=\"kafka_in\",target_kind=\"INGESTOR\"} 6",
        "le=\"+Inf\",peer=\"ingested\",peer_kind=\"RELAY\",physical_node_id=\"node-1\",relay=\"\
         ingested\",target=\"kafka_in\",target_kind=\"INGESTOR\"} 7",
    );

    let error = NervixMetricsReport::from_prometheus(&metrics, "benchmark_run")
        .expect_err("a +Inf bucket that disagrees with its count must be rejected");

    let MetricsReportError::InvalidHistogram { metric, series } = error.current_context() else {
        panic!("the histogram should be named: {error:?}");
    };
    assert_eq!(*metric, "nervix_messages_per_batch_bucket");
    assert_eq!(
        *series,
        MetricSeries {
            target_kind: "INGESTOR".to_string(),
            target: "kafka_in".to_string(),
            direction: "sent".to_string(),
            relay: "ingested".to_string(),
            physical_node_id: Some(ClusterNodeName::parse("node-1").expect("valid name")),
        }
    );
    assert!(matches!(
        error.downcast_ref::<HistogramError>(),
        Some(HistogramError::InfiniteBucketMismatch {
            infinite_count: 7,
            count: 6
        })
    ));
}

#[test]
fn names_the_stored_report_whose_entries_are_invalid() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let path = directory.path().join("nervix-metrics.toml");
    let report = NervixMetricsReport::from_prometheus(PROMETHEUS_FIXTURE, "benchmark_run")
        .assured("the static fixture contains a complete metrics report");
    report
        .write(&path)
        .expect("a complete metrics report should be written");
    let stored = std::fs::read_to_string(&path).expect("the written report should be readable");
    std::fs::write(
        &path,
        stored.replace("batches_total = 6", "batches_total = 0"),
    )
    .expect("the stored report should be rewritten");

    let error = NervixMetricsReport::read(&path)
        .expect_err("a stored report with an unobserved target must be rejected");

    assert!(matches!(
        error.current_context(),
        MetricsReportError::Invalid { path: invalid } if *invalid == path
    ));
    assert_eq!(
        format!("{error:#}"),
        format!(
            "Nervix metrics report {} is invalid: INGESTOR 'kafka_in' has no observed batches",
            path.display()
        )
    );
}

/// The labels of the fixture's ingestor target, which every batch metric of these tests describes.
const TARGET_LABELS: &str = r#"direction="sent",domain="benchmark_run",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR""#;
/// The labels of the fixture's relay buffer.
const RELAY_LABELS: &str = r#"direction="concrete",domain="benchmark_run",peer="",peer_kind="",physical_node_id="node-1",relay="ingested",target="ingested",target_kind="RELAY""#;

fn sample(metric: &str, labels: &str, value: &str) -> String {
    format!("{metric}{{{labels}}} {value}")
}

fn bucket(metric: &str, labels: &str, upper_bound: &str, count: u64) -> String {
    format!("{metric}{{{labels},le=\"{upper_bound}\"}} {count}")
}

/// A scrape of one ingestor target whose `messages_per_batch` histogram has the given buckets and
/// count, together with a complete relay buffer histogram.
fn target_scrape(batches_total: u64, buckets: &[(&str, u64)], count: Option<u64>) -> String {
    let mut lines = vec![
        sample("nervix_messages_total", TARGET_LABELS, "60"),
        sample(
            "nervix_batches_total",
            TARGET_LABELS,
            &batches_total.to_string(),
        ),
    ];
    for (upper_bound, cumulative) in buckets {
        lines.push(bucket(
            "nervix_messages_per_batch_bucket",
            TARGET_LABELS,
            upper_bound,
            *cumulative,
        ));
    }
    if let Some(count) = count {
        lines.push(sample(
            "nervix_messages_per_batch_count",
            TARGET_LABELS,
            &count.to_string(),
        ));
    }
    lines.push(bucket(
        "nervix_relay_buffer_len_bucket",
        RELAY_LABELS,
        "1",
        2,
    ));
    lines.push(bucket(
        "nervix_relay_buffer_len_bucket",
        RELAY_LABELS,
        "+Inf",
        2,
    ));
    lines.push(sample("nervix_relay_buffer_len_count", RELAY_LABELS, "2"));
    lines.join("\n")
}

fn scrape_failure(scrape: &str) -> error_stack::Report<MetricsReportError> {
    NervixMetricsReport::from_prometheus(scrape, "benchmark_run")
        .expect_err("the scrape should be rejected")
}

fn histogram_failure(scrape: &str) -> String {
    let error = scrape_failure(scrape);
    assert!(
        matches!(
            error.current_context(),
            MetricsReportError::InvalidHistogram { metric: "nervix_messages_per_batch_bucket", series }
                if series.target == "kafka_in"
        ),
        "{error:?}"
    );
    error
        .downcast_ref::<HistogramError>()
        .expect("the histogram property should be beneath the histogram")
        .to_string()
}

#[test]
fn names_what_is_wrong_with_each_malformed_sample_and_its_line() {
    let cases = [
        ("nervix_messages_total".to_string(), "sample has no value"),
        (
            r#"nervix_messages_total{domain="benchmark_run"}x 1"#.to_string(),
            "metric label set is not closed",
        ),
        (
            "nervix_messages_total{=1} 1".to_string(),
            "invalid label name or missing '='",
        ),
        (
            "nervix_messages_total{domain=benchmark_run} 1".to_string(),
            "label 'domain' value is not quoted",
        ),
        (
            r#"nervix_messages_total{domain="\q"} 1"#.to_string(),
            "label 'domain' has invalid escaping",
        ),
        (
            r#"nervix_messages_total{domain="a",domain="b"} 1"#.to_string(),
            "label 'domain' is duplicated",
        ),
        (
            r#"nervix_messages_total{domain="a" target="b"} 1"#.to_string(),
            "label 'domain' is not followed by ','",
        ),
        (
            sample(
                "nervix_messages_total",
                &format!("{TARGET_LABELS},shard=\"7\""),
                "1",
            ),
            "unexpected labels: shard",
        ),
        (
            sample("nervix_messages_per_batch_bucket", TARGET_LABELS, "1"),
            "metric 'nervix_messages_per_batch_bucket' is missing label 'le'",
        ),
        (
            bucket("nervix_messages_per_batch_bucket", TARGET_LABELS, "-1", 1),
            "invalid histogram bucket bound '-1'",
        ),
        (
            sample("nervix_messages_total", TARGET_LABELS, "1.5"),
            "metric 'nervix_messages_total' value '1.5' is not a non-negative integer count",
        ),
        (
            sample("nervix_messages_total", TARGET_LABELS, "-1"),
            "metric 'nervix_messages_total' value '-1' is not a non-negative integer count",
        ),
        (
            r#"nervix_messages_total{domain="benchmark_run"} 1"#.to_string(),
            "missing required label 'target_kind'",
        ),
    ];

    for (line, expected) in cases {
        let error = scrape_failure(&format!("# a comment\n\n{line}\n"));

        assert!(
            matches!(
                error.current_context(),
                MetricsReportError::InvalidPrometheusSample { line: 3 }
            ),
            "{line}: {error:?}"
        );
        let issue = error
            .downcast_ref::<PrometheusSampleError>()
            .expect("the sample issue should be beneath the line");
        assert_eq!(issue.to_string(), expected, "{line}");
        assert_eq!(
            format!("{error:#}").matches(expected).count(),
            1,
            "{line}: {error:#}"
        );
    }
}

#[test]
fn refuses_a_series_reported_twice() {
    for (metric, duplicated) in [
        (
            "nervix_messages_total",
            sample("nervix_messages_total", TARGET_LABELS, "60"),
        ),
        (
            "nervix_messages_per_batch_bucket",
            bucket("nervix_messages_per_batch_bucket", TARGET_LABELS, "+Inf", 6),
        ),
        (
            "nervix_messages_per_batch_count",
            sample("nervix_messages_per_batch_count", TARGET_LABELS, "6"),
        ),
    ] {
        let scrape = format!(
            "{}\n{duplicated}",
            target_scrape(6, &[("10", 6), ("+Inf", 6)], Some(6))
        );

        let error = scrape_failure(&scrape);

        assert!(
            matches!(
                error.current_context(),
                MetricsReportError::DuplicateSeries { metric: reported, .. } if *reported == metric
            ),
            "{metric}: {error:?}"
        );
        assert_eq!(
            error.to_string(),
            format!(
                "Prometheus metric '{metric}' has a duplicate series for INGESTOR 'kafka_in' \
                 direction 'sent' relay 'ingested' on 'node-1'"
            )
        );
    }
}

#[test]
fn names_a_duplicate_series_without_a_placed_owner_by_the_absent_label() {
    let scrape = target_scrape(6, &[("10", 6), ("+Inf", 6)], Some(6))
        .replace("physical_node_id=\"node-1\"", "physical_node_id=\"-\"");
    let duplicated = sample(
        "nervix_messages_total",
        &TARGET_LABELS.replace("physical_node_id=\"node-1\"", "physical_node_id=\"-\""),
        "60",
    );

    let error = scrape_failure(&format!("{scrape}\n{duplicated}"));

    assert_eq!(
        error.to_string(),
        "Prometheus metric 'nervix_messages_total' has a duplicate series for INGESTOR 'kafka_in' \
         direction 'sent' relay 'ingested' on '-'"
    );
}

#[test]
fn names_each_histogram_property_that_does_not_hold() {
    assert_eq!(
        histogram_failure(&target_scrape(6, &[("10", 0), ("+Inf", 0)], Some(0))),
        "histogram count is zero"
    );
    assert_eq!(
        histogram_failure(&target_scrape(6, &[("10", 6)], Some(6))),
        "missing +Inf bucket"
    );
    assert_eq!(
        histogram_failure(&target_scrape(
            6,
            &[("1", 5), ("2", 3), ("+Inf", 6)],
            Some(6)
        )),
        "bucket 2 count 3 is below the preceding cumulative count 5"
    );
    assert_eq!(
        histogram_failure(&target_scrape(6, &[("+Inf", 6)], Some(6))),
        "histogram has no finite buckets"
    );
    assert_eq!(
        histogram_failure(&target_scrape(5, &[("10", 6), ("+Inf", 6)], Some(6))),
        "histogram count 6 does not equal batches_total 5"
    );
}

#[test]
fn refuses_a_percentile_beyond_the_largest_finite_bucket() {
    let error = scrape_failure(&target_scrape(6, &[("1", 0), ("+Inf", 6)], Some(6)));

    assert!(
        matches!(
            error.current_context(),
            MetricsReportError::HistogramQuantileOverflow {
                metric: "nervix_messages_per_batch_bucket",
                quantile: Quantile::P50,
                largest_finite,
                ..
            } if *largest_finite == 1.0
        ),
        "{error:?}"
    );
    assert_eq!(
        error.to_string(),
        "p50 for Prometheus histogram 'nervix_messages_per_batch_bucket' on INGESTOR 'kafka_in' \
         direction 'sent' relay 'ingested' on 'node-1' exceeds its largest finite bucket 1"
    );
}

#[test]
fn names_the_metric_a_target_is_missing() {
    let without_histogram = [
        sample("nervix_messages_total", TARGET_LABELS, "60"),
        sample("nervix_batches_total", TARGET_LABELS, "6"),
    ]
    .join("\n");
    let without_messages = target_scrape(6, &[("10", 6), ("+Inf", 6)], Some(6)).replace(
        &format!("{}\n", sample("nervix_messages_total", TARGET_LABELS, "60")),
        "",
    );
    let without_count = target_scrape(6, &[("10", 6), ("+Inf", 6)], None);

    for (scrape, missing) in [
        (without_histogram, "nervix_messages_per_batch_bucket"),
        (without_messages, "nervix_messages_total"),
        (without_count, "nervix_messages_per_batch_count"),
    ] {
        let error = scrape_failure(&scrape);

        assert!(
            matches!(
                error.current_context(),
                MetricsReportError::MissingTargetMetric { metric, .. } if *metric == missing
            ),
            "{missing}: {error:?}"
        );
    }
}

#[test]
fn needs_both_batch_targets_and_relay_buffers() {
    let relay_only = [
        bucket("nervix_relay_buffer_len_bucket", RELAY_LABELS, "1", 2),
        bucket("nervix_relay_buffer_len_bucket", RELAY_LABELS, "+Inf", 2),
        sample("nervix_relay_buffer_len_count", RELAY_LABELS, "2"),
    ]
    .join("\n");
    let error = scrape_failure(&relay_only);
    assert!(matches!(
        error.current_context(),
        MetricsReportError::NoBatchTargets
    ));

    let targets_only = [
        sample("nervix_messages_total", TARGET_LABELS, "60"),
        sample("nervix_batches_total", TARGET_LABELS, "6"),
        bucket("nervix_messages_per_batch_bucket", TARGET_LABELS, "10", 6),
        bucket("nervix_messages_per_batch_bucket", TARGET_LABELS, "+Inf", 6),
        sample("nervix_messages_per_batch_count", TARGET_LABELS, "6"),
    ]
    .join("\n");
    let error = scrape_failure(&targets_only);
    assert!(matches!(
        error.current_context(),
        MetricsReportError::NoRelayBuffers
    ));
}

fn fixture_report() -> NervixMetricsReport {
    NervixMetricsReport::from_prometheus(PROMETHEUS_FIXTURE, "benchmark_run")
        .assured("the static fixture contains a complete metrics report")
}

/// Stores `report` exactly as given, bypassing the validation `write` applies, and reads it back.
fn read_stored(report: &NervixMetricsReport) -> error_stack::Report<MetricsReportError> {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let path = directory.path().join("nervix-metrics.toml");
    let contents = toml::to_string_pretty(report).expect("the report should serialize");
    std::fs::write(&path, contents).expect("the stored report should be written");
    let error = NervixMetricsReport::read(&path).expect_err("the stored report should be refused");
    assert!(
        matches!(
            error.current_context(),
            MetricsReportError::Invalid { path: invalid } if *invalid == path
        ),
        "{error:?}"
    );
    error
}

#[test]
fn names_the_entry_of_a_stored_report_that_is_invalid() {
    let mut empty_targets = fixture_report();
    empty_targets.batch_targets.clear();
    let error = read_stored(&empty_targets);
    assert!(format!("{error:#}").ends_with(
        "is invalid: scraped metrics contain no batch observations for Nervix runtime targets"
    ));

    let mut empty_relays = fixture_report();
    empty_relays.relay_buffers.clear();
    let error = read_stored(&empty_relays);
    assert!(
        format!("{error:#}")
            .ends_with("is invalid: scraped metrics contain no relay buffer observations")
    );

    let mut unordered = fixture_report();
    unordered.batch_targets[0].p50 = 4_096.0;
    let error = read_stored(&unordered);
    assert!(matches!(
        error.downcast_ref::<PercentileError>(),
        Some(PercentileError::NotMonotonic)
    ));
    assert!(
        format!("{error:#}").ends_with(
            "is invalid: INGESTOR 'kafka_in' has invalid percentiles: p50, p90 and p99 are not \
             monotonic"
        ),
        "{error:#}"
    );

    let mut unobserved_relay = fixture_report();
    unobserved_relay.relay_buffers[0].observations = 0;
    let error = read_stored(&unobserved_relay);
    assert!(
        format!("{error:#}").ends_with("is invalid: relay 'ingested' has no buffer observations"),
        "{error:#}"
    );

    let mut negative_relay = fixture_report();
    negative_relay.relay_buffers[0].p99 = -1.0;
    let error = read_stored(&negative_relay);
    let mut named_entry = None;
    for frame in error.frames() {
        if let Some(MetricsReportError::InvalidPercentiles { entry }) =
            frame.downcast_ref::<MetricsReportError>()
        {
            named_entry = Some(entry.clone());
        }
    }
    assert_eq!(
        named_entry,
        Some(ReportEntry::RelayBuffer {
            relay: "ingested".to_string()
        })
    );
    assert!(
        format!("{error:#}").ends_with(
            "is invalid: relay 'ingested' has invalid percentiles: a percentile is non-finite or \
             negative"
        ),
        "{error:#}"
    );
}

#[test]
fn names_the_file_a_stored_report_cannot_be_read_from_or_written_to() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");

    let missing = directory.path().join("missing.toml");
    let error = NervixMetricsReport::read(&missing).expect_err("a missing report cannot be read");
    assert!(matches!(
        error.current_context(),
        MetricsReportError::Read { path } if *path == missing
    ));
    assert!(error.contains::<std::io::Error>());

    let malformed = directory.path().join("malformed.toml");
    std::fs::write(&malformed, "batch_targets = 7")
        .expect("the malformed report should be written");
    let error = NervixMetricsReport::read(&malformed).expect_err("malformed TOML cannot be read");
    assert!(matches!(
        error.current_context(),
        MetricsReportError::Parse { path } if *path == malformed
    ));
    assert!(error.contains::<toml::de::Error>());

    let unwritable = directory
        .path()
        .join("absent-directory/nervix-metrics.toml");
    let error = fixture_report()
        .write(&unwritable)
        .expect_err("a report cannot be written below a missing directory");
    assert!(matches!(
        error.current_context(),
        MetricsReportError::Write { path } if *path == unwritable
    ));

    let mut invalid = fixture_report();
    invalid.batch_targets[0].batches_total = 0;
    let refused = directory.path().join("refused.toml");
    let error = invalid
        .write(&refused)
        .expect_err("an invalid report is refused before it is written");
    assert!(matches!(
        error.current_context(),
        MetricsReportError::Invalid { path } if *path == refused
    ));
    assert!(!refused.exists());
}
