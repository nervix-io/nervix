use nervix_benchmark::{MetricsReportError, NervixMetricsReport};

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
        error,
        MetricsReportError::MissingTargetMetric {
            metric: "nervix_batches_total",
            ..
        }
    ));
}
