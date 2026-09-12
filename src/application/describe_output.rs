//! The text a describe or show command prints.
//!
//! Layer: control plane.
//!
//! - **Owns.** Rendering one model, one runtime report or one placement plan as the lines a client
//!   displays, and the envelopes that carry those reports between nodes.
//! - **Depends on.** The Models and the runtime reports it renders.
//! - **Must not know.** Where the values it renders were gathered.

use ahash::{HashMap, HashSet};
use arch_into::ArchInto;
use nervix_dataflow_graph::{DataflowNodeHealth, DataflowNodeStatus};
use nervix_interconnect::{
    DataflowNodeStatusEnvelope, IngestorDescribeEnvelope, LookupDescribeEnvelope,
};
use nervix_models::{
    BranchSelection, ClusterNodeName, CreateCorrelator, CreateDeduplicator, CreateEmitter,
    CreateEndpoint, CreateIngestor, CreateJunction, CreateReingestor, CreateReorderer,
    CreateWindowProcessor, DomainName, EmitSink, FieldName, IcebergCatalog, IngestSource,
    IngestTimestampSource, KafkaOffsetMode, Model, ModelName, MongoDbConflictAction,
    MySqlConflictAction, NodeRef, PlacementName, PlacementPolicy, PostgresConflictAction,
    ProcessorInputs, ProcessorOutputs, RelayName, ScheduledNode, expression_to_nspl,
    ingest_quiesce_to_nspl,
};
use nervix_vm::window::{WindowAggregateDemand, WindowAggregateProgram};
use tokio::time::Duration;

use crate::{
    registry::{
        PlacementEndpointPairPlan, PlacementPlan, PlacementRequireGroupPlan, PlacementRulePlan,
        Registry, RegistryMutation,
    },
    runtime::IngestorDescribe as RuntimeIngestorDescribe,
};
pub(in crate::application) fn runtime_ingestor_describe_to_envelope(
    summary: RuntimeIngestorDescribe,
    metrics: Vec<String>,
) -> IngestorDescribeEnvelope {
    IngestorDescribeEnvelope {
        running: summary.running,
        ready: summary.ready,
        quiesce_state: summary.quiesce_state,
        quiesce_buffered_records: summary.quiesce_counters.buffered_records.arch_into(),
        quiesce_buffered_bytes: summary.quiesce_counters.buffered_bytes.arch_into(),
        quiesce_dropped_total: summary.quiesce_counters.dropped_total,
        quiesce_rejected_total: summary.quiesce_counters.rejected_total,
        memory_backpressure_paused: summary.memory_backpressure_paused,
        transient_error: summary.transient_error,
        reconnect_backoff: summary.reconnect_backoff,
        reconnect_wait_millis: summary.reconnect_wait_millis,
        kafka_domain_offsets: summary.kafka_domain_offsets.map(|kafka| {
            nervix_interconnect::KafkaDomainOffsetDescribeEnvelope {
                topic: kafka.topic,
                instances: kafka.instances,
                observed_partitions: kafka.observed_partitions,
                rebalance_epoch: kafka.rebalance_epoch,
                instance_assignments: kafka.instance_assignments,
            }
        }),
        metrics,
    }
}

pub(in crate::application) fn runtime_ingestor_describe_from_envelope(
    summary: IngestorDescribeEnvelope,
) -> (RuntimeIngestorDescribe, Vec<String>) {
    (
        RuntimeIngestorDescribe {
            running: summary.running,
            ready: summary.ready,
            quiesce_state: summary.quiesce_state,
            quiesce_counters: crate::runtime::IngestorQuiesceCounters {
                buffered_records: summary.quiesce_buffered_records.arch_into(),
                buffered_bytes: summary.quiesce_buffered_bytes.arch_into(),
                dropped_total: summary.quiesce_dropped_total,
                rejected_total: summary.quiesce_rejected_total,
            },
            memory_backpressure_paused: summary.memory_backpressure_paused,
            transient_error: summary.transient_error,
            reconnect_backoff: summary.reconnect_backoff,
            reconnect_wait_millis: summary.reconnect_wait_millis,
            kafka_domain_offsets: summary.kafka_domain_offsets.map(|kafka| {
                crate::runtime::KafkaDomainOffsetDescribe {
                    topic: kafka.topic,
                    instances: kafka.instances,
                    observed_partitions: kafka.observed_partitions,
                    rebalance_epoch: kafka.rebalance_epoch,
                    instance_assignments: kafka.instance_assignments,
                }
            }),
        },
        summary.metrics,
    )
}

pub(in crate::application) fn dataflow_node_status_to_envelope(
    status: DataflowNodeStatus,
    detail: Option<String>,
    transient_error: Option<String>,
    reconnect_backoff: Option<String>,
    reconnect_wait_millis: Option<u64>,
) -> DataflowNodeStatusEnvelope {
    let status = match status {
        DataflowNodeStatus::Ok => "OK",
        DataflowNodeStatus::Waiting => "WAITING",
        DataflowNodeStatus::Error => "ERROR",
    };
    DataflowNodeStatusEnvelope {
        status: status.to_string(),
        detail,
        transient_error,
        reconnect_backoff,
        reconnect_wait_millis,
    }
}

pub(in crate::application) fn dataflow_node_status_from_envelope(
    envelope: DataflowNodeStatusEnvelope,
) -> DataflowNodeHealth {
    DataflowNodeHealth {
        status: if envelope.status.eq_ignore_ascii_case("ERROR") {
            DataflowNodeStatus::Error
        } else if envelope.status.eq_ignore_ascii_case("WAITING") {
            DataflowNodeStatus::Waiting
        } else {
            DataflowNodeStatus::Ok
        },
        detail: envelope.detail,
        reconnect_wait_millis: envelope.reconnect_wait_millis,
    }
}

fn format_timestamp_source(source: Option<&IngestTimestampSource>) -> &'static str {
    match source {
        Some(IngestTimestampSource::Now) => "NOW",
        Some(IngestTimestampSource::At(_)) => "AT",
        None => "-",
    }
}

pub(in crate::application) fn format_millis_duration(millis: u64) -> String {
    humantime::format_duration(Duration::from_millis(millis)).to_string()
}

fn format_ingestor_source(source: &IngestSource) -> &'static str {
    match source {
        IngestSource::Http { .. } => "HTTP",
        IngestSource::Kafka { .. } => "KAFKA",
        IngestSource::Pulsar { .. } => "PULSAR",
        IngestSource::Mqtt { .. } => "MQTT",
        IngestSource::Nats { .. } => "NATS",
        IngestSource::RabbitMq { .. } => "RABBITMQ",
        IngestSource::RedisPubSub { .. } => "REDIS",
        IngestSource::Prometheus { .. } => "PROMETHEUS",
        IngestSource::ZeroMq { .. } => "ZEROMQ",
        IngestSource::Sqs { .. } => "SQS",
        IngestSource::Endpoint { .. } => "ENDPOINT",
        IngestSource::Websockets { .. } => "WEBSOCKETS",
        IngestSource::Syslog { .. } => "SYSLOG",
    }
}

pub(in crate::application) fn format_endpoint_describe_output(
    name: &ModelName,
    endpoint: &CreateEndpoint,
) -> String {
    [
        format!("endpoint: {}", name.as_str()),
        "kind: ENDPOINT".to_string(),
        format!("vhost: {}", endpoint.on_vhost.as_str()),
        format!("path: {}", endpoint.path),
        format!("type: {}", endpoint.endpoint_type.as_ref()),
    ]
    .join("\n")
}

fn format_kafka_offset_mode(offset_mode: &KafkaOffsetMode) -> String {
    match offset_mode {
        KafkaOffsetMode::ConsumerGroup(group) => {
            format!("CONSUMER GROUP {}", group.as_str())
        }
        KafkaOffsetMode::Domain => "DOMAIN".to_string(),
    }
}

pub(in crate::application) fn format_ingestor_describe_output(
    name: impl Into<ModelName>,
    ingestor: &CreateIngestor,
    ingestor_node: &ScheduledNode,
    summary: &RuntimeIngestorDescribe,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("ingestor: {}", name.as_str()),
        "kind: INGESTOR".to_string(),
        format!("source: {}", format_ingestor_source(&ingestor.source)),
        format!(
            "streams: {}",
            ingestor
                .output_routes
                .relays()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        format!("codec: {}", ingestor.decode_using_codec.as_str()),
        format!(
            "owner: {}",
            match ingestor_node.execution_node() {
                Some(owner) => owner.as_str(),
                None => "-",
            }
        ),
        format!(
            "timestamp: {}",
            format_timestamp_source(ingestor.timestamp_source.as_ref())
        ),
        format!(
            "status: {}",
            if summary.quiesce_state.is_some() {
                "quiesced"
            } else if summary.running {
                "running"
            } else {
                "stopped"
            }
        ),
        format!("ready: {}", if summary.ready { "true" } else { "false" }),
        format!(
            "quiesce: {}",
            ingest_quiesce_to_nspl(ingestor.source.quiesce())
        ),
        format!(
            "quiesce state: {}",
            summary.quiesce_state.as_deref().unwrap_or("none")
        ),
        format!(
            "nervix_ingestor_quiesce_buffered_records: {}",
            summary.quiesce_counters.buffered_records
        ),
        format!(
            "nervix_ingestor_quiesce_buffered_bytes: {}",
            summary.quiesce_counters.buffered_bytes
        ),
        format!(
            "nervix_ingestor_quiesce_dropped_total: {}",
            summary.quiesce_counters.dropped_total
        ),
        format!(
            "nervix_ingestor_quiesce_rejected_total: {}",
            summary.quiesce_counters.rejected_total
        ),
    ];
    lines.extend(format_processor_output_lines(&ingestor.output_routes));
    let memory_backpressure_state = if summary.memory_backpressure_paused {
        "active"
    } else {
        "inactive"
    };
    lines.push(format!("memory-backpressure: {memory_backpressure_state}"));
    lines.push(format!(
        "transient error: {}",
        summary.transient_error.as_deref().unwrap_or("-")
    ));
    lines.push(format!(
        "reconnect backoff: {}",
        summary.reconnect_backoff.as_deref().unwrap_or("-")
    ));
    let reconnect_wait = match summary.reconnect_wait_millis {
        Some(millis) => format_millis_duration(millis),
        None => "-".to_string(),
    };
    lines.push(format!("reconnect wait: {reconnect_wait}"));

    if let IngestSource::Kafka {
        topic,
        offset_mode,
        instances,
        ..
    } = &ingestor.source
    {
        lines.push(format!("kafka topic: {}", topic.as_str()));
        lines.push(format!(
            "kafka offset mode: {}",
            format_kafka_offset_mode(offset_mode)
        ));
        lines.push(format!("kafka instances: {instances}"));
        if let Some(kafka) = summary.kafka_domain_offsets.as_ref() {
            lines.push(format!(
                "kafka observed partitions: {}",
                kafka
                    .observed_partitions
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ));
            lines.push(format!("kafka rebalance epoch: {}", kafka.rebalance_epoch));
            for (instance_idx, partitions) in kafka.instance_assignments.iter().enumerate() {
                let rendered = if partitions.is_empty() {
                    "-".to_string()
                } else {
                    partitions
                        .iter()
                        .map(i32::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                };
                lines.push(format!(
                    "kafka instance {instance_idx} partitions: {rendered}"
                ));
            }
        } else if let KafkaOffsetMode::Domain = offset_mode {
            lines.push("kafka observed partitions: -".to_string());
            lines.push("kafka rebalance epoch: 0".to_string());
            for instance_idx in 0..instances.get() {
                lines.push(format!("kafka instance {instance_idx} partitions: -"));
            }
        }
    } else if let IngestSource::Pulsar {
        topic,
        subscription,
        instances,
        ..
    } = &ingestor.source
    {
        lines.push(format!("pulsar topic: {}", topic.as_str()));
        lines.push(format!("pulsar subscription: {}", subscription.as_str()));
        lines.push(format!("pulsar instances: {instances}"));
    }

    lines.join("\n")
}

fn format_branch_selection(branched_by: &BranchSelection) -> &str {
    match branched_by.branch() {
        Some(name) => name.as_str(),
        None => "UNBRANCHED",
    }
}

fn format_output_branch(branch: Option<&nervix_models::OutputBranch>) -> &str {
    match branch {
        Some(nervix_models::OutputBranch::BranchedBy { branch, .. }) => branch.as_str(),
        Some(nervix_models::OutputBranch::Unbranched) => "UNBRANCHED",
        None => "NODE-WIDE",
    }
}

pub(in crate::application) fn append_metrics_lines(
    mut output: String,
    metrics: Vec<String>,
) -> String {
    if metrics.is_empty() {
        return output;
    }
    output.push('\n');
    output.push_str(&metrics.join("\n"));
    output
}

pub(in crate::application) fn format_relay_describe_output(
    relay: &nervix_models::CreateRelay,
    branching: &[FieldName],
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let mut lines = vec![
        format!("relay: {}", relay.name.as_str()),
        "kind: RELAY".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("schema: {}", relay.schema.as_str()),
        format!("branched by: {}", {
            if let Some(branch) = relay.branching.branch() {
                branch.as_str()
            } else if relay.branching.is_unbranched() {
                "UNBRANCHED"
            } else {
                "-"
            }
        }),
        format!(
            "branch fields: {}",
            if branching.is_empty() {
                "-".to_string()
            } else {
                branching
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        ),
        format!("capacity: {}", relay.buffer),
        format!(
            "materialized state: {}",
            if relay.materialized_state.is_some() {
                "present"
            } else {
                "none"
            }
        ),
    ]);
    if !branching.is_empty() {
        lines.push("branch-local describe: use WHERE bindings".to_string());
    }
    lines.join("\n")
}

fn format_schedule_placement_lines(scheduled_node: Option<&ScheduledNode>) -> Vec<String> {
    let owner = match scheduled_node.and_then(ScheduledNode::execution_node) {
        Some(owner) => owner.as_str(),
        None => "-",
    };
    let mut replicas = "-".to_string();
    if let Some(scheduled_node) = scheduled_node {
        let rendered = format_replica_nodes(scheduled_node);
        if !rendered.is_empty() {
            replicas = rendered;
        }
    }
    vec![format!("owner: {owner}"), format!("replicas: {replicas}")]
}

fn format_replica_nodes(scheduled_node: &ScheduledNode) -> String {
    scheduled_node
        .replica_nodes()
        .iter()
        .map(|node| node.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_processor_output_lines(outputs: &ProcessorOutputs) -> Vec<String> {
    let mut lines = Vec::new();
    let output_count = outputs.outputs().count();
    lines.push(format!("outputs: {output_count}"));

    for (index, output) in outputs.routes.iter().enumerate() {
        let flush = match &output.flush_policy {
            Some(nervix_models::FlushPolicy::Each {
                interval,
                max_batch_size,
            }) => format!("{interval} max-batch-size={max_batch_size}"),
            Some(nervix_models::FlushPolicy::Immediate) => "IMMEDIATE".to_string(),
            None => "none".to_string(),
        };
        lines.push(format!(
            "output {index}: into={} construction={} branch={} flush={flush}",
            output.relay.as_str(),
            if !output.construction.is_empty() {
                "present"
            } else {
                "none"
            },
            format_output_branch(output.branch.as_ref())
        ));
    }

    lines
}

fn processor_input_names(inputs: &ProcessorInputs) -> String {
    inputs
        .relays()
        .iter()
        .map(|name| name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

pub(in crate::application) fn format_lookup_describe_output(
    name: impl Into<ModelName>,
    scheduled_node: &ScheduledNode,
    summary: &LookupDescribeEnvelope,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("hash map: {}", name.as_str()),
        "kind: HASH MAP".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(Some(scheduled_node)));
    lines.extend([
        format!("key: {}", summary.key_field.as_str()),
        format!(
            "resource: {}@{}",
            summary.resource.as_str(),
            summary.resource_version
        ),
        format!("path: {}", summary.path),
        format!("codec: {}", summary.decode_using_codec.as_str()),
        format!("entries: {}", summary.entry_count),
    ]);
    lines.join("\n")
}

fn format_expression_list(expressions: &[nervix_models::Expression]) -> String {
    expressions
        .iter()
        .map(|expression| {
            expression_to_nspl(expression).unwrap_or_else(|error| format!("<invalid: {error}>"))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(in crate::application) fn format_deduplicator_describe_output(
    name: impl Into<ModelName>,
    deduplicator: &CreateDeduplicator,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("deduplicator: {}", name.as_str()),
        "kind: DEDUPLICATOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor_input_names(&deduplicator.from)),
        format!("mode: {}", deduplicator.mode.as_ref()),
        format!(
            "deduplicate on: {}",
            format_expression_list(&deduplicator.deduplicate_on)
        ),
        format!("max time: {}", deduplicator.max_time),
        format!(
            "filter-where: {}",
            if deduplicator.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        "branch-local: true".to_string(),
        "persistent state: true".to_string(),
        "replicated state: true".to_string(),
        "state structures: 1".to_string(),
        "structure 0:".to_string(),
        "  function: DEDUPLICATE_ON".to_string(),
        "  storage: recent_key_set".to_string(),
        format!(
            "  key expressions: {}",
            format_expression_list(&deduplicator.deduplicate_on)
        ),
        format!("  max time: {}", deduplicator.max_time),
    ]);
    lines.extend(format_processor_output_lines(&deduplicator.output_routes));
    lines.join("\n")
}

pub(in crate::application) fn format_junction_describe_output(
    name: impl Into<ModelName>,
    junction: &CreateJunction,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("junction: {}", name.as_str()),
        "kind: JUNCTION".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor_input_names(&junction.from)),
        format!("branch: {}", format_branch_selection(&junction.branched_by)),
        format!("mode: {}", junction.mode.as_ref()),
        format!(
            "filter-where: {}",
            if junction.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        "branch-local: true".to_string(),
    ]);
    lines.extend(format_processor_output_lines(&junction.output_routes));
    lines.join("\n")
}

pub(in crate::application) fn format_reingestor_describe_output(
    name: impl Into<ModelName>,
    reingestor: &CreateReingestor,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("reingestor: {}", name.as_str()),
        "kind: REINGESTOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor_input_names(&reingestor.from)),
        format!("mode: {}", reingestor.mode.as_ref()),
        format!(
            "filter-where: {}",
            if reingestor.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
    ]);
    lines.extend(format_processor_output_lines(&reingestor.output_routes));
    lines.join("\n")
}

pub(in crate::application) fn format_correlator_describe_output(
    name: impl Into<ModelName>,
    correlator: &CreateCorrelator,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("correlator: {}", name.as_str()),
        "kind: CORRELATOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("left: {}", processor_input_names(&correlator.left)),
        format!("right: {}", processor_input_names(&correlator.right)),
        format!(
            "branch: {}",
            format_branch_selection(&correlator.branched_by)
        ),
        format!("mode: {}", correlator.mode.as_ref()),
        format!("match: {}", correlator.match_policy.as_ref()),
        format!(
            "correlate where: {}",
            expression_to_nspl(&correlator.correlate_where)
                .unwrap_or_else(|error| format!("<invalid: {error}>"))
        ),
        format!("max time: {}", correlator.max_time),
        format!(
            "filter-where: {}",
            if correlator.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        format!(
            "timeout left: {}",
            format_correlation_timeout_action(&correlator.timeout_policy.left)
        ),
        format!(
            "timeout right: {}",
            format_correlation_timeout_action(&correlator.timeout_policy.right)
        ),
        "branch-local: true".to_string(),
        "persistent state: true".to_string(),
        "replicated state: true".to_string(),
    ]);
    lines.extend(format_processor_output_lines(&correlator.output_routes));
    lines.join("\n")
}

fn format_correlation_timeout_action(action: &nervix_models::CorrelationTimeoutAction) -> String {
    match action {
        nervix_models::CorrelationTimeoutAction::Drop => "DROP".to_string(),
        nervix_models::CorrelationTimeoutAction::SendTo { relay } => {
            format!("SEND TO {}", relay.as_str())
        }
    }
}

pub(in crate::application) fn format_reorderer_describe_output(
    name: impl Into<ModelName>,
    reorderer: &CreateReorderer,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("reorderer: {}", name.as_str()),
        "kind: REORDERER".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor_input_names(&reorderer.from)),
        format!("mode: {}", reorderer.mode.as_ref()),
        format!("order by: {}", format_expression_list(&reorderer.order_by)),
        format!("max time: {}", reorderer.max_time),
        format!(
            "filter-where: {}",
            if reorderer.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        "branch-local: true".to_string(),
        "persistent state: true".to_string(),
        "replicated state: true".to_string(),
    ]);
    lines.extend(format_processor_output_lines(&reorderer.output_routes));
    lines.join("\n")
}

pub(in crate::application) fn format_emitter_describe_output(
    name: impl Into<ModelName>,
    emitter: &CreateEmitter,
    scheduled_node: Option<&ScheduledNode>,
    status: Option<&DataflowNodeStatusEnvelope>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("emitter: {}", name.as_str()),
        "kind: EMITTER".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    if let Some(status) = status {
        lines.extend([
            format!("status: {}", status.status),
            // What the node is doing when it is neither working nor broken, such as waiting for a
            // connection from a shared client's pool.
            format!("detail: {}", status.detail.as_deref().unwrap_or("-")),
            format!(
                "transient error: {}",
                status.transient_error.as_deref().unwrap_or("-")
            ),
            format!(
                "reconnect backoff: {}",
                status.reconnect_backoff.as_deref().unwrap_or("-")
            ),
            format!(
                "reconnect wait: {}",
                match status.reconnect_wait_millis {
                    Some(millis) => format!("{millis}ms"),
                    None => "-".to_string(),
                }
            ),
        ]);
    }
    lines.extend([
        format!(
            "from: {}",
            emitter
                .from
                .relays()
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        format!(
            "codec: {}",
            match emitter.encode_using_codec.as_ref() {
                Some(name) => name.as_str(),
                None => "none",
            }
        ),
        format!("sink: {}", format_emit_sink(&emitter.sink)),
        format!("flush: {}", emitter.flush_policy.to_canonical_nspl()),
        format!(
            "publishing mode: {}",
            emitter.publishing_mode.to_canonical_nspl()
        ),
        format!(
            "filter-map: {}",
            if !emitter.construction.is_empty() {
                "present"
            } else {
                "none"
            }
        ),
    ]);
    lines.join("\n")
}

fn format_emit_sink(sink: &EmitSink) -> String {
    match sink {
        EmitSink::Kafka { client, topic } => {
            format!("KAFKA client={} topic={}", client.as_str(), topic.as_str())
        }
        EmitSink::Pulsar { client, topic } => {
            format!("PULSAR client={} topic={}", client.as_str(), topic.as_str())
        }
        EmitSink::RabbitMq { client, queue } => {
            format!(
                "RABBITMQ client={} queue={}",
                client.as_str(),
                queue.as_str()
            )
        }
        EmitSink::Redis { client, channel } => {
            format!(
                "REDIS client={} channel={}",
                client.as_str(),
                channel.as_str()
            )
        }
        EmitSink::Mqtt { client, topic } => {
            format!("MQTT client={} topic={}", client.as_str(), topic.as_str())
        }
        EmitSink::Nats { client, subject } => {
            format!(
                "NATS client={} subject={}",
                client.as_str(),
                subject.as_str()
            )
        }
        EmitSink::ZeroMq { client } => format!("ZEROMQ client={}", client.as_str()),
        EmitSink::Syslog { client } => format!("SYSLOG client={}", client.as_str()),
        EmitSink::Sqs {
            client,
            queue,
            fifo_group,
        } => {
            let fifo = match fifo_group.as_ref() {
                Some(group) => {
                    let value = match group {
                        nervix_models::SqsFifoGroup::FromBranch => "FROM BRANCH".to_string(),
                        nervix_models::SqsFifoGroup::Expression(expression) => {
                            nervix_models::expression_to_nspl(expression)
                                .unwrap_or_else(|_| "<unrenderable expression>".to_string())
                        }
                    };
                    format!(" fifo_group={value}")
                }
                None => String::new(),
            };
            format!(
                "SQS client={} queue={}{}",
                client.as_str(),
                queue.as_str(),
                fifo
            )
        }
        EmitSink::Sentry { client } => format!("SENTRY client={}", client.as_str()),
        EmitSink::Otel { client, signal, .. } => {
            let signal = match signal {
                nervix_models::OtelSignal::Logs => "logs".to_string(),
                nervix_models::OtelSignal::Traces => "traces".to_string(),
                nervix_models::OtelSignal::Metric(metric) => {
                    format!("metric {}", metric.name)
                }
            };
            format!("OTEL client={} signal={signal}", client.as_str())
        }
        EmitSink::ClickHouse {
            client,
            table,
            max_batch,
            ..
        } => format!(
            "CLICKHOUSE client={} table={} max_batch={}",
            client.as_str(),
            table.as_str(),
            max_batch
        ),
        EmitSink::Postgres {
            client,
            table,
            conflict_action,
            max_batch,
            ..
        } => {
            let conflict = match conflict_action {
                PostgresConflictAction::None => String::new(),
                PostgresConflictAction::DoNothing { target } => {
                    let target = if target.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", target.join(","))
                    };
                    format!(" conflict=ON CONFLICT{target} DO NOTHING")
                }
                PostgresConflictAction::DoUpdate { target } => {
                    let target = if target.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", target.join(","))
                    };
                    format!(" conflict=ON CONFLICT{target} DO UPDATE")
                }
            };
            format!(
                "POSTGRES client={} table={}{} max_batch={}",
                client.as_str(),
                table.as_str(),
                conflict,
                max_batch
            )
        }
        EmitSink::MySql {
            client,
            table,
            conflict_action,
            max_batch,
            ..
        } => {
            let conflict = match conflict_action {
                MySqlConflictAction::None => String::new(),
                MySqlConflictAction::DoNothing => " conflict=ON CONFLICT DO NOTHING".to_string(),
                MySqlConflictAction::DoUpdate => " conflict=ON CONFLICT DO UPDATE".to_string(),
            };
            format!(
                "MYSQL client={} table={}{} max_batch={}",
                client.as_str(),
                table.as_str(),
                conflict,
                max_batch
            )
        }
        EmitSink::MongoDb {
            client,
            collection,
            conflict_action,
            max_batch,
            ..
        } => {
            let conflict = match conflict_action {
                MongoDbConflictAction::None => String::new(),
                MongoDbConflictAction::DoNothing { target } => {
                    format!(" conflict=ON CONFLICT ({}) DO NOTHING", target.join(","))
                }
                MongoDbConflictAction::DoUpdate { target } => {
                    format!(" conflict=ON CONFLICT ({}) DO UPDATE", target.join(","))
                }
            };
            format!(
                "MONGODB client={} collection={}{} max_batch={}",
                client.as_str(),
                collection.as_str(),
                conflict,
                max_batch
            )
        }
        EmitSink::Iceberg {
            backend,
            client,
            table,
            values: _,
            location,
            catalog,
            commit_each,
            max_commit_size,
        } => {
            let catalog = match catalog {
                IcebergCatalog::Rest { client } => format!("rest client={}", client.as_str()),
            };
            format!(
                "ICEBERG backend={} client={} table={} location={} catalog={} commit_each={} \
                 max_commit_size={}",
                backend.as_ref(),
                client.as_str(),
                table.as_str(),
                location,
                catalog,
                commit_each,
                max_commit_size
            )
        }
    }
}

pub(in crate::application) fn format_window_processor_describe_output(
    name: impl Into<ModelName>,
    processor: &CreateWindowProcessor,
    aggregate: &WindowAggregateProgram,
    scheduled_node: Option<&ScheduledNode>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("window processor: {}", name.as_str()),
        "kind: WINDOW PROCESSOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor_input_names(&processor.from)),
        format!("mode: {:?}", processor.mode),
        format!("width: {}", processor.width.to_describe_string()),
        format!("step: {}", processor.step.to_describe_string()),
        format!(
            "filter-where: {}",
            if processor.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        "branch-local: true".to_string(),
        format!("aggregate structures: {}", aggregate.demands().len()),
    ]);
    lines.extend(format_processor_output_lines(&processor.output_routes));
    let references = aggregate.demand_reference_counts();
    for demand in aggregate.demands() {
        lines.extend(format_window_aggregate_demand(demand, &references));
    }
    lines.join("\n")
}

pub(in crate::application) fn format_wasm_processor_describe_output(
    name: impl Into<ModelName>,
    processor: &nervix_models::CreateWasmProcessor,
    scheduled_node: Option<&ScheduledNode>,
    state_lines: Vec<String>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("wasm processor: {}", name.as_str()),
        "kind: WASM PROCESSOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    let version = match processor.resource_version {
        Some(version) => version.to_string(),
        None => "latest".to_string(),
    };
    lines.extend([
        format!("from: {}", processor_input_names(&processor.from)),
        format!("mode: {}", processor.mode.as_ref()),
        format!("resource: {}", processor.resource.as_str()),
        format!("resource version: {version}"),
        format!("file: {}", processor.file),
        format!("max fuel: {}", processor.limits.max_fuel),
        format!("max memory: {} bytes", processor.limits.max_memory_bytes),
        format!("ABI serialization: {}", nervix_wasm::ABI_SERIALIZATION_NAME),
        format!(
            "filter-where: {}",
            if processor.filter_where.is_some() {
                "present"
            } else {
                "none"
            }
        ),
        "flush: guest-controlled".to_string(),
        "branch-local: true".to_string(),
        "persistent state: true".to_string(),
        "replicated state: true".to_string(),
    ]);
    lines.extend(format_processor_output_lines(&processor.output_routes));
    lines.extend(state_lines);
    lines.join("\n")
}

pub(in crate::application) fn format_materialized_stream_state_output(
    relay: &RelayName,
    scheduled_node: &ScheduledNode,
    entries: Vec<String>,
) -> String {
    let mut lines = vec![
        format!("materialized relay: {}", relay.as_str()),
        "kind: RELAY".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(Some(scheduled_node)));
    if entries.is_empty() {
        lines.push(format!(
            "relay '{}' materialized state is empty",
            relay.as_str()
        ));
    } else {
        lines.extend(entries);
    }
    lines.join("\n")
}

fn format_window_aggregate_demand(
    demand: &WindowAggregateDemand,
    references: &[usize],
) -> Vec<String> {
    let mut lines = vec![
        format!("structure {}:", demand.id),
        format!(
            "  functions: {}",
            demand
                .functions
                .iter()
                .map(|function| function.nspl_name())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        format!("  storage: {}", demand.storage.nspl_name()),
        format!(
            "  references: {}",
            references.get(demand.id).copied().unwrap_or(0)
        ),
    ];
    if let Some(input) = &demand.input {
        lines.push(format!("  input: {}", format_window_aggregate_input(input)));
    }
    if let Some(config) = &demand.linear_histogram {
        lines.push(format!("  buckets: {}", config.buckets));
        lines.push(format!("  min: {}", format_f64_for_describe(config.min)));
        lines.push(format!("  max: {}", format_f64_for_describe(config.max)));
        lines.push(format!(
            "  delay: {}",
            humantime::format_duration(config.delay)
        ));
    }
    lines
}

fn format_window_aggregate_input(expr: &nervix_vm::program::Expr) -> String {
    match expr {
        nervix_vm::program::Expr::FieldRef(field_ref) => {
            format!("{}.{}", field_ref.relay, field_ref.field)
        }
        _ => format!("{expr:?}"),
    }
}

fn format_f64_for_describe(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

pub(in crate::application) fn format_placement_runtime_nodes(nodes: &[NodeRef]) -> String {
    format_placement_runtime_nodes_in_context(nodes, nodes)
}

pub(in crate::application) fn format_placement_runtime_nodes_in_context(
    nodes: &[NodeRef],
    context: &[NodeRef],
) -> String {
    nodes
        .iter()
        .map(|node| format_placement_runtime_node(node, context))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(in crate::application) fn format_placement_runtime_node(
    node: &NodeRef,
    context: &[NodeRef],
) -> String {
    let kind_collision = context
        .iter()
        .any(|candidate| candidate.identifier == node.identifier && candidate.kind != node.kind);
    if kind_collision {
        format!("{}:{}", node.kind.as_ref(), node.identifier.as_str())
    } else {
        node.identifier.to_string()
    }
}

pub(in crate::application) fn placement_rule_runtime_nodes(
    rule: &PlacementRulePlan,
) -> Vec<NodeRef> {
    let mut nodes = Vec::new();
    for endpoint in &rule.endpoint_pairs {
        for node in std::iter::once(&endpoint.source)
            .chain(std::iter::once(&endpoint.destination))
            .chain(endpoint.corridor.iter())
        {
            if !nodes.contains(node) {
                nodes.push(node.clone());
            }
        }
    }
    nodes
}

pub(in crate::application) fn placement_rule_endpoint_nodes(
    rule: &PlacementRulePlan,
    sources: bool,
) -> Vec<NodeRef> {
    let mut nodes = Vec::new();
    for endpoint in &rule.endpoint_pairs {
        let node = if sources {
            &endpoint.source
        } else {
            &endpoint.destination
        };
        if !nodes.contains(node) {
            nodes.push(node.clone());
        }
    }
    nodes
}

pub(in crate::application) fn placement_rule_coverage_status(
    rule: &PlacementRulePlan,
) -> &'static str {
    let connected_pairs = rule
        .endpoint_pairs
        .iter()
        .filter(|pair| pair.connected)
        .count();
    if connected_pairs == 0 {
        return "empty";
    }

    let effective_claims = rule.claims.iter().filter(|claim| claim.effective).count();
    if !rule.claims.is_empty() && effective_claims == 0 {
        return "overridden";
    }
    if connected_pairs == rule.endpoint_pairs.len() && effective_claims == rule.claims.len() {
        "effective"
    } else {
        "partial"
    }
}

pub(in crate::application) fn placement_runtime_node_ref_suggestions(
    registry: &Registry,
    domain: &DomainName,
    prefix: &str,
    queued: &[RegistryMutation],
) -> Vec<String> {
    let Ok(models) = registry.resulting_models(domain, queued) else {
        return Vec::new();
    };

    let prefix = prefix.to_ascii_lowercase();
    let eligible = models
        .iter()
        .filter(|model| {
            placement_member_model_is_eligible(model) && model.name().as_str().starts_with(&prefix)
        })
        .map(|model| model.name())
        .collect::<Vec<_>>();

    let mut counts = HashMap::<ModelName, usize>::default();
    for identifier in &eligible {
        *counts.entry(identifier.clone()).or_default() += 1;
    }
    eligible
        .into_iter()
        .filter(|identifier| counts.get(identifier) == Some(&1))
        .map(|identifier| identifier.to_string())
        .collect()
}

fn placement_member_model_is_eligible(model: &Model) -> bool {
    match model {
        Model::Generator(_)
        | Model::Inferencer(_)
        | Model::WasmProcessor(_)
        | Model::Reingestor(_)
        | Model::Lookup(_)
        | Model::Junction(_)
        | Model::Deduplicator(_)
        | Model::Correlator(_)
        | Model::Reorderer(_)
        | Model::WindowProcessor(_)
        | Model::Emitter(_) => true,
        Model::Ingestor(ingestor) => !matches!(&ingestor.source, IngestSource::Endpoint { .. }),
        Model::Relay(_) => true,
        _ => false,
    }
}

pub(in crate::application) fn ordered_placement_corridor(
    endpoint: &PlacementEndpointPairPlan,
) -> Vec<NodeRef> {
    let longest_witness = endpoint
        .witnesses
        .iter()
        .max_by_key(|witness| witness.path.len());
    let mut ordered = if let Some(witness) = longest_witness {
        witness.path.clone()
    } else if endpoint.source == endpoint.destination {
        vec![endpoint.source.clone()]
    } else {
        vec![endpoint.source.clone(), endpoint.destination.clone()]
    };
    let mut seen = HashSet::default();
    let mut unique = Vec::with_capacity(endpoint.corridor.len());
    for node in ordered.drain(..).chain(endpoint.corridor.iter().cloned()) {
        if seen.insert(node.clone()) {
            unique.push(node);
        }
    }
    unique
}

pub(in crate::application) fn placement_claim_owner(rules: &[PlacementName]) -> String {
    if rules.is_empty() {
        "domain default".to_string()
    } else {
        rules
            .iter()
            .map(|rule| rule.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

pub(in crate::application) fn placement_group_members_equal(
    left: &[NodeRef],
    right: &[NodeRef],
) -> bool {
    let right = right.iter().collect::<HashSet<_>>();
    left.len() == right.len() && left.iter().all(|member| right.contains(member))
}

pub(in crate::application) fn placement_group_host<'a>(
    schedule: Option<&'a nervix_models::DomainSchedule>,
    members: &[NodeRef],
) -> Option<&'a ClusterNodeName> {
    let schedule = schedule?;
    let group = schedule
        .placement_groups
        .iter()
        .find(|group| placement_group_members_equal(&group.members, members))?;
    group.primary_node.as_ref()
}

pub(in crate::application) fn placement_groups_claimed_by_rule<'a>(
    plan: &'a PlacementPlan,
    rule: &PlacementRulePlan,
) -> Vec<&'a PlacementRequireGroupPlan> {
    let require_claims = rule
        .claims
        .iter()
        .filter(|claim| {
            claim.effective && claim.effective_policy == PlacementPolicy::RequireColocation
        })
        .collect::<Vec<_>>();
    plan.require_groups
        .iter()
        .filter(|group| {
            require_claims.iter().any(|claim| {
                group.members.contains(&claim.left) && group.members.contains(&claim.right)
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use nervix_models::ModelKind;

    use super::{super::test_fixtures::placement_member, *};

    #[test]
    fn placement_runtime_node_rendering_qualifies_only_kind_collisions() {
        let nodes = vec![
            placement_member("shared", ModelKind::Junction),
            placement_member("shared", ModelKind::Deduplicator),
            placement_member("sink", ModelKind::Emitter),
        ];

        assert_eq!(
            format_placement_runtime_nodes(&nodes),
            "junction:shared, deduplicator:shared, sink"
        );
        assert_eq!(
            format_placement_runtime_nodes(&[
                placement_member("source", ModelKind::Junction),
                placement_member("sink", ModelKind::Emitter),
            ]),
            "source, sink"
        );
    }
}
