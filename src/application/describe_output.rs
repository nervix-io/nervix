//! The text a describe or show command prints.
//!
//! Layer: control plane.
//!
//! - **Owns.** Rendering one model, one runtime report or one placement plan as the lines a client
//!   displays, and the envelopes that carry those reports between nodes.
//! - **Depends on.** The Models and the runtime reports it renders.
//! - **Must not know.** Where the values it renders were gathered.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::time::Duration;

use ahash::{HashMap, HashSet};
use arch_into::ArchInto;
use nervix_dataflow_graph::{DataflowNodeHealth, DataflowNodeStatus};
use nervix_interconnect::{
    DataflowNodeStatusEnvelope, DecodeRejectionEnvelope, IngestorDescribeEnvelope,
    LookupDescribeEnvelope, TransportPositionEnvelope,
};
use nervix_models::{
    BranchSelection, CanonicalPlexusError, ClusterNodeName, CreateCorrelator, CreateDeduplicator,
    CreateEmitter, CreateEndpoint, CreateIngestor, CreateJunction, CreateReingestor,
    CreateReorderer, CreateWindowProcessor, EmitterBody, FieldPath, HeaderModel,
    IcebergTableEmitter, IngestTimestampSource, IngestorEntity, IngestorInput, KafkaIngestor,
    KafkaOffsetMode, KafkaTopicIngestor, Model, ModelKind, ModelName, NodeRef, PlacementName,
    PlacementPolicy, ProcessorInputs, ProcessorOutputs, RelayName, RequestedResourceVersion,
    ResourceDescription, ResourceEntryContent, ResourceManifestEntry, ResourceUsage,
    ResourceVersionEntries, ScheduledNode, WasmStateInspection, expression_to_plexus,
};
use nervix_vm::window::{WindowAggregateDemand, WindowAggregateProgram, WindowArguments};
use sorted_vec::SortedSet;

use crate::{
    registry::{
        PlacementEndpointPairPlan, PlacementPlan, PlacementRequireGroupPlan, PlacementRulePlan,
    },
    runtime::{DecodeRejection, IngestorDescribe as RuntimeIngestorDescribe, TransportPosition},
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
        transport_redeliveries_total: summary.transport_redeliveries_total,
        decode_rejections_total: summary.decode_rejections_total,
        last_decode_rejection: summary
            .last_decode_rejection
            .map(decode_rejection_to_envelope),
        oversized_payloads_total: summary.oversized_payloads_total,
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
        client_producers: summary.client_producers.map(|gauges| {
            nervix_interconnect::ClientProducersDescribeEnvelope {
                producers: gauges.producers,
                forwarded_producers: gauges.forwarded_producers,
                outstanding_batches: gauges.outstanding_batches,
                outstanding_bytes: gauges.outstanding_bytes,
                admitted_batches: gauges.admitted_batches,
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
            transport_redeliveries_total: summary.transport_redeliveries_total,
            decode_rejections_total: summary.decode_rejections_total,
            last_decode_rejection: summary
                .last_decode_rejection
                .map(decode_rejection_from_envelope),
            oversized_payloads_total: summary.oversized_payloads_total,
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
            client_producers: summary.client_producers.map(|producers| {
                crate::runtime::ClientIngestorGauges {
                    producers: producers.producers,
                    forwarded_producers: producers.forwarded_producers,
                    outstanding_batches: producers.outstanding_batches,
                    outstanding_bytes: producers.outstanding_bytes,
                    admitted_batches: producers.admitted_batches,
                }
            }),
        },
        summary.metrics,
    )
}

/// The envelope a decode rejection crosses between nodes in.
fn decode_rejection_to_envelope(rejection: DecodeRejection) -> DecodeRejectionEnvelope {
    let mut fields = Vec::with_capacity(rejection.fields.len());
    for field in rejection.fields.iter() {
        fields.push(field.as_str().to_string());
    }
    let position = match rejection.position {
        Some(TransportPosition::Kafka {
            topic,
            partition,
            offset,
        }) => Some(TransportPositionEnvelope::Kafka {
            topic,
            partition,
            offset,
        }),
        Some(TransportPosition::Syslog { peer }) => {
            Some(TransportPositionEnvelope::Syslog { peer })
        }
        None => None,
    };
    DecodeRejectionEnvelope {
        reference: rejection.reference.as_u128(),
        codec: rejection.codec,
        position,
        fields,
        message: rejection.message,
        occurred_at: rejection.occurred_at,
    }
}

/// The decode rejection a node reported in `envelope`.
fn decode_rejection_from_envelope(envelope: DecodeRejectionEnvelope) -> DecodeRejection {
    let mut fields = Vec::with_capacity(envelope.fields.len());
    for field in envelope.fields {
        fields.push(FieldPath::new(field));
    }
    let position = match envelope.position {
        Some(TransportPositionEnvelope::Kafka {
            topic,
            partition,
            offset,
        }) => Some(TransportPosition::Kafka {
            topic,
            partition,
            offset,
        }),
        Some(TransportPositionEnvelope::Syslog { peer }) => {
            Some(TransportPosition::Syslog { peer })
        }
        None => None,
    };
    DecodeRejection {
        reference: uuid::Uuid::from_u128(envelope.reference),
        codec: envelope.codec,
        position,
        fields: SortedSet::from_unsorted(fields),
        message: envelope.message,
        occurred_at: envelope.occurred_at,
    }
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

pub(in crate::application) fn format_ingestor_describe_output(
    name: impl Into<ModelName>,
    ingestor: &CreateIngestor,
    ingestor_node: &ScheduledNode,
    summary: &RuntimeIngestorDescribe,
) -> error_stack::Result<String, CanonicalPlexusError> {
    let name = name.into();
    let mut lines = vec![
        format!("ingestor: {}", name.as_str()),
        "kind: INGESTOR".to_string(),
    ];
    lines.extend(ingestor.input.entity_presentation().describe_lines());
    lines.push("batch: none".to_owned());
    let mut flush = Vec::new();
    let mut errors = vec![
        ingestor
            .general_error_policy
            .to_canonical_plexus()
            .to_owned(),
    ];
    for output in ingestor.output_routes.outputs() {
        let policy = match &output.flush_policy {
            Some(policy) => policy.to_canonical_plexus(),
            None => "none".to_owned(),
        };
        flush.push(format!("{}={policy}", output.relay));
        errors.push(format!(
            "{}={}",
            output.relay,
            output.message_error_policy.to_canonical_plexus()?
        ));
    }
    lines.push(format!(
        "flush: {}",
        if flush.is_empty() {
            "none".to_owned()
        } else {
            flush.join("; ")
        }
    ));
    lines.push(format!("errors: {}", errors.join("; ")));
    lines.push(format!(
        "streams: {}",
        ingestor
            .output_routes
            .relays()
            .map(|name| name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    ));
    match &ingestor.input {
        IngestorInput::Transport(transport) => {
            lines.push(format!("codec: {}", transport.codec.as_str()));
        }
        IngestorInput::Client(client) => {
            lines.push(format!("schema: {}", client.schema.as_str()));
        }
    }
    lines.extend([
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
        format!(
            "nervix_ingestor_transport_redeliveries_total: {}",
            summary.transport_redeliveries_total
        ),
        format!(
            "nervix_ingestor_decode_rejections_total: {}",
            summary.decode_rejections_total
        ),
        format!(
            "nervix_ingestor_oversized_payloads_total: {}",
            summary.oversized_payloads_total
        ),
    ]);
    let last_decode_rejection = match summary.last_decode_rejection.as_ref() {
        Some(rejection) => rejection.to_string(),
        None => "-".to_string(),
    };
    lines.push(format!("last decode rejection: {last_decode_rejection}"));
    if let IngestorInput::Client(_) = &ingestor.input {
        lines.extend(format_client_producer_lines(summary));
    }
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

    if let Some(IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
        offset_mode,
        instances,
        ..
    }))) = ingestor.input.transport_entity()
    {
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
    }
    lines.push(format!(
        "headers: {}",
        format_header_model(ingestor.input.entity_kind().capabilities().headers)
    ));

    Ok(lines.join("\n"))
}

/// How `DESCRIBE` shows the header model of one side: the model, or `none` where that side's
/// messages carry no transport headers.
fn format_header_model(model: Option<HeaderModel>) -> String {
    match model {
        Some(model) => model.to_string(),
        None => "none".to_string(),
    }
}

/// One line of `SHOW INGESTORS`: the pair the ingestor reads, its schema or codec, the node that
/// executes it and its state, and for a client ingestor whether it admits batches and the
/// producers attached to it. `summary` is `None` when no node executes the ingestor, or when its
/// node could not be asked.
pub(in crate::application) fn format_ingestor_listing_line(
    ingestor: &CreateIngestor,
    owner: &str,
    summary: Option<&RuntimeIngestorDescribe>,
) -> String {
    let mut line = format!(
        "{} from={}",
        ingestor.name.as_str(),
        ingestor.input.source_phrase()
    );
    match &ingestor.input {
        IngestorInput::Transport(transport) => {
            line.push_str(" codec=");
            line.push_str(transport.codec.as_str());
        }
        IngestorInput::Client(client) => {
            line.push_str(" schema=");
            line.push_str(client.schema.as_str());
        }
    }
    let status = match summary {
        Some(summary) if summary.quiesce_state.is_some() => "quiesced",
        Some(summary) if summary.running => "running",
        Some(_) => "stopped",
        None if owner == "-" => "stopped",
        None => "unavailable",
    };
    line.push_str(&format!(" owner={owner} status={status}"));
    if let IngestorInput::Client(_) = &ingestor.input {
        let admission = match summary {
            Some(summary) if summary.running && summary.quiesce_state.is_none() => "open",
            Some(summary) if summary.running => "suspended",
            _ => "-",
        };
        let gauges = match summary {
            Some(summary) => summary.client_producers.unwrap_or_default(),
            None => crate::runtime::ClientIngestorGauges::default(),
        };
        line.push_str(&format!(
            " admission={admission} producers={} outstanding_batches={} outstanding_bytes={} \
             admitted_batches={}",
            gauges.producers,
            gauges.outstanding_batches,
            gauges.outstanding_bytes,
            gauges.admitted_batches
        ));
    }
    line
}

/// Whether a client ingestor admits batches now, and the producers attached to it on the node
/// that executes it.
fn format_client_producer_lines(summary: &RuntimeIngestorDescribe) -> Vec<String> {
    let admission = if !summary.running {
        "-"
    } else if summary.quiesce_state.is_some() {
        "suspended"
    } else {
        "open"
    };
    let gauges = summary.client_producers.unwrap_or_default();
    vec![
        format!("admission: {admission}"),
        format!("producers: {}", gauges.producers),
        format!("forwarded producers: {}", gauges.forwarded_producers),
        format!("outstanding batches: {}", gauges.outstanding_batches),
        format!("outstanding bytes: {}", gauges.outstanding_bytes),
        format!("admitted batches: {}", gauges.admitted_batches),
    ]
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
    branching: &nervix_models::ResolvedBranching,
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
            if branching.is_unbranched() {
                "-".to_string()
            } else {
                branching
                    .field_names()
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
    if !branching.is_unbranched() {
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
            expression_to_plexus(expression).unwrap_or_else(|error| format!("<invalid: {error}>"))
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
            expression_to_plexus(&correlator.correlate_where)
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
) -> error_stack::Result<String, CanonicalPlexusError> {
    let name = name.into();
    let mut lines = vec![
        format!("emitter: {}", name.as_str()),
        "kind: EMITTER".to_string(),
    ];
    lines.extend(
        emitter
            .connector
            .entity_presentation()?
            .with_envelope(&emitter.envelope)?
            .describe_lines(),
    );
    lines.push(match &emitter.batch {
        Some(batch) => format!("batch: {batch}"),
        None => "batch: none".to_owned(),
    });
    if matches!(
        emitter.connector.as_ref(),
        nervix_models::EmitterEntity::Iceberg(_)
    ) && emitter.batch.is_none()
    {
        lines.push(format!(
            "data file target: {}MiB",
            IcebergTableEmitter::DEFAULT_DATA_FILE_TARGET_BYTES / (1024 * 1024)
        ));
    }
    lines.push(format!(
        "flush: {}",
        emitter.flush_policy.to_canonical_plexus()
    ));
    lines.push(format!(
        "errors: {}",
        emitter.error_policies.to_canonical_plexus()?
    ));
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
            match emitter.body.codec() {
                Some(name) => name.as_str(),
                None => "none",
            }
        ),
        format!(
            "body: {}",
            match &emitter.body {
                EmitterBody::Codec { .. } => "codec",
                EmitterBody::WithoutBody => "without body",
                EmitterBody::Values => "values",
                EmitterBody::Client => "native Arrow",
            }
        ),
        format!(
            "filter-map: {}",
            if !emitter.construction.is_empty() {
                "present"
            } else {
                "none"
            }
        ),
        format!(
            "headers: {}",
            format_header_model(emitter.connector.capabilities().headers)
        ),
    ]);
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
    Ok(lines.join("\n"))
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
    inspection: Option<&WasmStateInspection>,
) -> String {
    let name = name.into();
    let mut lines = vec![
        format!("wasm processor: {}", name.as_str()),
        "kind: WASM PROCESSOR".to_string(),
    ];
    lines.extend(format_schedule_placement_lines(scheduled_node));
    lines.extend([
        format!("from: {}", processor_input_names(&processor.from)),
        format!("mode: {}", processor.mode.as_ref()),
        format!("resource: {}", processor.resource.as_str()),
        format!("resource version: {}", processor.resource_version),
        format!("file: {}", processor.file),
        format!("max fuel: {}", processor.limits.max_fuel),
        format!("max memory: {} bytes", processor.limits.max_memory_bytes),
        format!(
            "rejected state policy: {}",
            processor.rejected_state_policy.as_ref()
        ),
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
    lines.extend(format_wasm_state_reset_lines(inspection));
    lines.extend(format_wasm_state_recovery_lines(inspection));
    lines.extend(format_wasm_checkpoint_lines(inspection));
    lines.join("\n")
}

fn format_wasm_state_reset_lines(inspection: Option<&WasmStateInspection>) -> Vec<String> {
    let Some(inspection) = inspection else {
        return Vec::new();
    };
    let mut lines = vec![format!(
        "state default generation: {}",
        inspection.default_generation
    )];
    let Some(reset) = &inspection.reset else {
        return lines;
    };
    let scope = reset.reset.scope().kind();
    lines.push(format!(
        "state reset: {}, {scope}, generation {}",
        reset.reset.phase().as_ref(),
        reset.generation
    ));
    lines.push(format!(
        "state reset reason: {}",
        reset.reset.reason().as_ref()
    ));
    if let Some(readiness) = inspection.reset_readiness {
        lines.push(format!("state reset readiness: {}", readiness.as_ref()));
    }
    lines
}

/// What became of the one recovery attempt each refused guest-state lifetime was worth.
///
/// A refusal that is still being worked on, one that started a fresh lifetime, and one that spent
/// its attempt without producing a usable lifetime are the three states an operator acts on, so all
/// three are reported. The branch is named by the scope alone, because a branch key may carry
/// payload values.
fn format_wasm_state_recovery_lines(inspection: Option<&WasmStateInspection>) -> Vec<String> {
    let Some(inspection) = inspection else {
        return Vec::new();
    };
    let mut lines = Vec::new();
    for recovery in &inspection.recoveries {
        let selected = recovery.scope.kind();
        lines.push(format!(
            "rejected state recovery: {selected}, {}, {} (generation {})",
            recovery.rejection, recovery.outcome, recovery.generation
        ));
    }
    if inspection.omitted_recoveries > 0 {
        lines.push(format!(
            "rejected state recoveries omitted: {}",
            inspection.omitted_recoveries
        ));
    }
    lines
}

fn format_wasm_checkpoint_lines(inspection: Option<&WasmStateInspection>) -> Vec<String> {
    let Some(inspection) = inspection else {
        return Vec::new();
    };
    let mut lines = Vec::new();
    for checkpoint in &inspection.checkpoints {
        let branch = match checkpoint.branch {
            Some(fingerprint) => fingerprint.to_string(),
            None => "unbranched".to_string(),
        };
        let committed = match checkpoint.committed_revision {
            Some(revision) => revision.to_string(),
            None => "none".to_string(),
        };
        let latest = match checkpoint.latest_revision {
            Some(revision) => revision.to_string(),
            None => "none".to_string(),
        };
        let required = match checkpoint.required_replicas {
            Some(count) => count.to_string(),
            None => "unknown".to_string(),
        };
        let confirmed = match checkpoint.confirmed_replicas {
            Some(count) => count.to_string(),
            None => "unknown".to_string(),
        };
        lines.push(format!(
            "checkpoint branch={branch} generation={} committed_revision={committed} \
             latest_revision={latest} stage={} required_replicas={required} \
             confirmed_replicas={confirmed}",
            checkpoint.generation,
            checkpoint.stage.as_ref(),
        ));
    }
    let counts = &inspection.checkpoint_counts;
    lines.insert(0, format!("state structures: {}", counts.total));
    lines.insert(
        1,
        format!("checkpoints awaiting local storage: {}", counts.captured),
    );
    lines.insert(
        2,
        format!(
            "checkpoints awaiting replicas: {}",
            counts.awaiting_replicas
        ),
    );
    lines.insert(3, format!("failed checkpoints: {}", counts.failed));
    if inspection.omitted_checkpoints > 0 {
        lines.push(format!(
            "checkpoints omitted: {}",
            inspection.omitted_checkpoints
        ));
    }
    lines
}

/// The text `DESCRIBE RESOURCE <name>` prints for `description`.
pub(in crate::application) fn format_resource_description(
    description: &ResourceDescription,
) -> String {
    let latest = match description.latest_version {
        Some(version) => version.to_string(),
        None => "(none)".to_string(),
    };
    let version_numbers = if description.versions.is_empty() {
        "(none)".to_string()
    } else {
        description
            .versions
            .iter()
            .map(|version| version.version.to_string())
            .collect::<Vec<_>>()
            .join(",")
    };
    let mut lines = vec![
        format!("resource: {}", description.resource.as_str()),
        format!("latest: {latest}"),
        format!("versions: {version_numbers}"),
        "version_details:".to_string(),
    ];
    if description.versions.is_empty() {
        lines.push("- none".to_string());
    }
    for version in &description.versions {
        lines.push(format!(
            "- version={} root_checksum={} manifest_checksum={} file_count={} total_bytes={} \
             created_by_node={} created_at={}",
            version.version,
            version.root_checksum,
            version.manifest_checksum,
            version.file_count,
            version.total_bytes,
            version.created_by_node,
            version.created_at
        ));
        lines.push("  entries:".to_string());
        lines.extend(format_resource_entry_lines(&version.entries));
    }
    lines.extend(format_resource_usage_lines(&description.usages));
    lines.join("\n")
}

/// The lines listing the entries of one resource version.
pub(in crate::application) fn format_resource_entry_lines(
    entries: &ResourceVersionEntries,
) -> Vec<String> {
    match entries {
        ResourceVersionEntries::Listed(entries) if entries.is_empty() => {
            vec!["  - none".to_string()]
        }
        ResourceVersionEntries::Listed(entries) => {
            entries.iter().map(format_resource_manifest_entry).collect()
        }
        ResourceVersionEntries::Unavailable { reason } => {
            vec![format!("  - unavailable error={reason}")]
        }
    }
}

fn format_resource_manifest_entry(entry: &ResourceManifestEntry) -> String {
    let (entry_type, size, checksum) = match &entry.content {
        ResourceEntryContent::File { size, checksum } => ("file", *size, checksum.as_str()),
        ResourceEntryContent::Directory => ("directory", 0, "-"),
    };
    format!(
        "  - type={} path={} size={} checksum={}",
        entry_type, entry.path, size, checksum
    )
}

/// The `usages` lines: one per model bound to a version of a resource.
pub(in crate::application) fn format_resource_usage_lines(usages: &[ResourceUsage]) -> Vec<String> {
    let mut lines = vec!["usages:".to_string()];
    if usages.is_empty() {
        lines.push("- none".to_string());
    }
    for usage in usages {
        let kind = match usage.node.kind {
            ModelKind::Lookup => "hash_map",
            kind => kind.as_str(),
        };
        lines.push(format!(
            "- kind={kind} name={} version={}",
            usage.node.identifier.as_str(),
            usage.version
        ));
    }
    lines
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
                .map(|function| function.plexus_name())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        format!("  storage: {}", demand.storage.plexus_name()),
        format!(
            "  references: {}",
            references.get(demand.id).copied().unwrap_or(0)
        ),
    ];
    match &demand.arguments {
        WindowArguments::Single(input) => {
            lines.push(format!("  input: {}", format_window_aggregate_input(input)));
        }
        WindowArguments::Pair { first, second } => {
            lines.push(format!(
                "  inputs: {}, {}",
                format_window_aggregate_input(first),
                format_window_aggregate_input(second)
            ));
        }
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
    models: &[Model<RequestedResourceVersion>],
    prefix: &str,
) -> Vec<String> {
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

fn placement_member_model_is_eligible<Version>(model: &Model<Version>) -> bool {
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
        // Ingestors that execute on every node are outside placement rules.
        Model::Ingestor(ingestor) => !ingestor.input.executes_on_every_cluster_node(),
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

#[cfg(all(
    test,
    not(any(feature = "shuttle", feature = "loom", feature = "deloxide"))
))]
#[path = "describe_output_properties.rs"]
mod properties;

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::{super::test_fixtures::placement_member, *};

    #[test]
    fn a_resource_description_renders_every_version_entry_and_usage() {
        let version = |number: u64, entries: ResourceVersionEntries| {
            nervix_models::ResourceVersionDescription {
                version: std::num::NonZeroU64::new(number).assured("the test version is non-zero"),
                root_checksum: format!("root{number}"),
                manifest_checksum: format!("manifest{number}"),
                file_count: 1,
                total_bytes: 5,
                created_at: nervix_models::Timestamp::from_unix_nanos(0),
                created_by_node: ClusterNodeName::parse("node-1").assured("a literal node name"),
                entries,
            }
        };
        let usage = |kind: ModelKind, name: &str, number: u64| ResourceUsage {
            node: NodeRef::new(kind, ModelName::parse(name).assured("a literal model name")),
            version: std::num::NonZeroU64::new(number).assured("the test version is non-zero"),
        };
        let description = ResourceDescription {
            resource: nervix_models::ResourceName::parse("bundle").assured("a literal resource"),
            latest_version: std::num::NonZeroU64::new(1),
            versions: vec![
                version(
                    1,
                    ResourceVersionEntries::Listed(vec![
                        ResourceManifestEntry {
                            path: "release notes.txt".to_string(),
                            content: ResourceEntryContent::File {
                                size: 5,
                                checksum: "abc".to_string(),
                            },
                        },
                        ResourceManifestEntry {
                            path: "guides".to_string(),
                            content: ResourceEntryContent::Directory,
                        },
                    ]),
                ),
                version(2, ResourceVersionEntries::Listed(Vec::new())),
                version(
                    3,
                    ResourceVersionEntries::Unavailable {
                        reason: "manifest missing".to_string(),
                    },
                ),
            ],
            usages: vec![
                usage(ModelKind::Client, "store", 1),
                usage(ModelKind::Lookup, "by_id", 1),
            ],
        };

        assert_eq!(
            format_resource_description(&description),
            [
                "resource: bundle",
                "latest: 1",
                "versions: 1,2,3",
                "version_details:",
                "- version=1 root_checksum=root1 manifest_checksum=manifest1 file_count=1 \
                 total_bytes=5 created_by_node=node-1 created_at=1970-01-01 00:00:00 UTC",
                "  entries:",
                "  - type=file path=release notes.txt size=5 checksum=abc",
                "  - type=directory path=guides size=0 checksum=-",
                "- version=2 root_checksum=root2 manifest_checksum=manifest2 file_count=1 \
                 total_bytes=5 created_by_node=node-1 created_at=1970-01-01 00:00:00 UTC",
                "  entries:",
                "  - none",
                "- version=3 root_checksum=root3 manifest_checksum=manifest3 file_count=1 \
                 total_bytes=5 created_by_node=node-1 created_at=1970-01-01 00:00:00 UTC",
                "  entries:",
                "  - unavailable error=manifest missing",
                "usages:",
                "- kind=client name=store version=1",
                "- kind=hash_map name=by_id version=1",
            ]
            .join("\n")
        );

        let empty = ResourceDescription {
            latest_version: None,
            versions: Vec::new(),
            usages: Vec::new(),
            ..description
        };
        assert_eq!(
            format_resource_description(&empty),
            "resource: bundle\nlatest: (none)\nversions: (none)\nversion_details:\n- \
             none\nusages:\n- none"
        );
    }

    #[test]
    fn placement_runtime_node_rendering_qualifies_only_kind_collisions() {
        let nodes = vec![
            placement_member("shared", ModelKind::Junction),
            placement_member("shared", ModelKind::Deduplicator),
            placement_member("emitter", ModelKind::Emitter),
        ];

        assert_eq!(
            format_placement_runtime_nodes(&nodes),
            "junction:shared, deduplicator:shared, emitter"
        );
        assert_eq!(
            format_placement_runtime_nodes(&[
                placement_member("source", ModelKind::Junction),
                placement_member("emitter", ModelKind::Emitter),
            ]),
            "source, emitter"
        );
    }

    #[test]
    fn a_prometheus_ingestor_describes_the_response_bound_in_effect() {
        use nervix_models::{IngestQuiesceMode, PrometheusIngestMode, PrometheusQueryIngestor};

        use super::super::test_fixtures::{named, scheduled_node};

        let summary = RuntimeIngestorDescribe {
            running: true,
            ready: true,
            quiesce_state: None,
            quiesce_counters: Default::default(),
            transport_redeliveries_total: 0,
            decode_rejections_total: 0,
            last_decode_rejection: None,
            oversized_payloads_total: 0,
            memory_backpressure_paused: false,
            transient_error: None,
            reconnect_backoff: None,
            reconnect_wait_millis: None,
            kafka_domain_offsets: None,
            client_producers: None,
        };
        let written = "1KiB"
            .parse()
            .assured("the fixture size is a whole number of kibibytes");
        for (max_body_size, line) in [
            (None, "options: QUERY 'up' MAX BODY SIZE 16MiB EVERY 15s"),
            (
                Some(written),
                "options: QUERY 'up' MAX BODY SIZE 1KiB EVERY 15s",
            ),
        ] {
            let mut scheduled = scheduled_node("samples", ModelKind::Ingestor);
            let Model::Ingestor(ingestor) = scheduled.config.as_mut() else {
                panic!("the fixture constructs an ingestor");
            };
            let IngestorInput::Transport(input) = &mut ingestor.input else {
                panic!("the fixture constructs a transport ingestor");
            };
            input.source = PrometheusQueryIngestor {
                client: named("prom_main"),
                query: "up".to_owned(),
                max_body_size,
                every: "15s"
                    .parse()
                    .assured("the fixture cadence is a positive duration"),
                mode: PrometheusIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            }
            .into();
            let Model::Ingestor(ingestor) = scheduled.config.as_ref() else {
                panic!("the fixture constructs an ingestor");
            };

            let described =
                format_ingestor_describe_output(&ingestor.name, ingestor, &scheduled, &summary)
                    .assured("the current ingestor configuration renders");

            assert!(
                described.lines().any(|candidate| candidate == line),
                "expected `{line}` in {described}"
            );
        }
    }

    #[test]
    fn placement_completion_respects_every_node_ingestor_declarations() {
        use nervix_models::{
            EndpointIngestMode, HttpEndpointIngestor, IngestQuiesceMode, IngestorEntity,
            SyslogIngestMode, SyslogListenerIngestor, WebsocketsEndpointIngestor,
        };

        use super::super::test_fixtures::{named, scheduled_node};

        let scheduled = scheduled_node("intake", ModelKind::Ingestor);
        assert!(placement_member_model_is_eligible(
            scheduled.config.as_ref()
        ));

        let sources: [IngestorEntity; 3] = [
            HttpEndpointIngestor {
                endpoint: named("http_intake"),
                mode: EndpointIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Reject {
                    retry_after: "1s".to_owned(),
                },
            }
            .into(),
            WebsocketsEndpointIngestor {
                endpoint: named("websocket_intake"),
                mode: EndpointIngestMode::NoAck,
                quiesce: IngestQuiesceMode::EndpointBuffer {
                    max_size: "1MiB".to_owned(),
                },
            }
            .into(),
            SyslogListenerIngestor {
                client: named("syslog_listener"),
                mode: SyslogIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            }
            .into(),
        ];
        for source in sources {
            let mut model = scheduled.config.clone();
            let Model::Ingestor(ingestor) = model.as_mut() else {
                panic!("the fixture constructs an ingestor");
            };
            let IngestorInput::Transport(input) = &mut ingestor.input else {
                panic!("the fixture constructs a transport ingestor");
            };
            input.source = source;
            assert!(model.executes_on_every_cluster_node());
            assert!(!placement_member_model_is_eligible(model.as_ref()));
        }
    }
}
