//! Opaque drivers for measuring relay-interaction scheduling in isolation.
//!
//! This module only exists with the `benchmarks` feature. Its public surface deliberately exposes
//! benchmark operations and observations instead of Nervix runtime carriers or channels.

use std::{num::NonZeroUsize, sync::OnceLock};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{CreateSchema, Identifier, ParseAsType, Timestamp};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};

use super::{
    NodeQuiesceCounters, RelayBroadcast, RelayRecordBatch, RelayRuntimeFanIn,
    force_flush::DomainForceFlush,
    relay_interaction::{
        RelayInteraction, RelayInteractionCommand, RelayInteractionEvent, RelayInteractionInput,
        RuntimeInputCollectPolicy,
    },
};
use crate::{
    runtime_ack::AckSet,
    runtime_schema::{CompiledSchema, RuntimeRecordMetadata, RuntimeValue, compile_schema},
};

/// The externally observable result of one benchmark driver step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayInteractionBenchmarkEvent {
    Batch { rows: u64 },
    ForceFlush,
    Wake,
    Command,
    Stopped,
}

enum BenchmarkCommand {
    Drain,
}

impl RelayInteractionCommand for BenchmarkCommand {
    fn drain_inputs_before_handling(&self) -> bool {
        true
    }

    fn cancels_external_waits_while_draining(&self) -> bool {
        true
    }
}

/// An opaque, feature-gated relay interaction prepared for Criterion measurements.
pub struct RelayInteractionBenchmark {
    interaction: RelayInteraction<BenchmarkCommand>,
    sources: Vec<RelayBroadcast<RelayRecordBatch>>,
    shutdown_tx: watch::Sender<bool>,
    commands: mpsc::Sender<BenchmarkCommand>,
    force_flush: triomphe::Arc<DomainForceFlush>,
    quiesce_counters: triomphe::Arc<NodeQuiesceCounters>,
    batch: RelayRecordBatch,
}

impl RelayInteractionBenchmark {
    /// Creates pass-through sources whose batches are yielded without collection.
    pub fn pass_through(source_count: usize, capacity_per_source: usize) -> Self {
        Self::new(source_count, capacity_per_source, None)
    }

    /// Creates collecting sources with a deliberately distant deadline.
    ///
    /// Calling [`Self::force_flush`] drains a snapshot of all ready input, concatenates each
    /// source's unbranched batches, then yields the force-flush event.
    pub fn collecting(source_count: usize, capacity_per_source: usize) -> Self {
        Self::new(
            source_count,
            capacity_per_source,
            Some(RuntimeInputCollectPolicy {
                interval: tokio::time::Duration::from_secs(3_600),
                max_batch_size: None,
            }),
        )
    }

    fn new(
        source_count: usize,
        capacity_per_source: usize,
        collect_policy: Option<RuntimeInputCollectPolicy>,
    ) -> Self {
        assert!(source_count > 0, "benchmark requires at least one source");
        let capacity = NonZeroUsize::new(capacity_per_source).assured(
            "the benchmark entry points are only reached with a nonzero constant capacity",
        );
        let mut inputs = Vec::with_capacity(source_count);
        let mut sources = Vec::with_capacity(source_count);
        for source in 0..source_count {
            let relay = Identifier::parse(&format!("benchmark_source_{source}")).assured(
                "the name is built here from fixed text that satisfies the identifier grammar",
            );
            let broadcast = RelayBroadcast::with_capacity(capacity);
            let receiver = RelayRuntimeFanIn::new(broadcast.new_receiver());
            inputs.push(RelayInteractionInput::new(relay, receiver, collect_policy));
            sources.push(broadcast);
        }
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (commands, command_rx) = mpsc::channel(1);
        let force_flush = DomainForceFlush::new();
        let quiesce_counters = triomphe::Arc::new(NodeQuiesceCounters::default());
        let participant = DomainForceFlush::subscribe(&force_flush, Some(quiesce_counters.clone()));
        let interaction = RelayInteraction::with_commands(
            inputs,
            shutdown_rx,
            Some(participant),
            Some(quiesce_counters.clone()),
            command_rx,
        )
        .assured("the input list is built here with at least one source");
        Self {
            interaction,
            sources,
            shutdown_tx,
            commands,
            force_flush,
            quiesce_counters,
            batch: benchmark_batch(),
        }
    }

    /// Queues one batch on a source before the interaction is advanced.
    pub async fn enqueue(&self, source: usize) {
        self.sources[source]
            .broadcast(self.batch.clone())
            .await
            .verified("the benchmark owns both ends of this channel for its whole lifetime");
    }

    /// Latches a force-flush request. The following steps first drain already-ready input.
    pub fn force_flush(&self) {
        self.force_flush.request();
    }

    /// Requests a drain-first command through the same bounded command channel as runtime nodes.
    pub async fn graceful_command(&self) {
        self.commands
            .send(BenchmarkCommand::Drain)
            .await
            .verified("the benchmark owns both ends of this channel for its whole lifetime");
    }

    /// Requests receiver-local watch shutdown.
    pub fn shutdown(&self) {
        self.shutdown_tx.send_replace(true);
    }

    /// Clears the synthetic shutdown signal so lifecycle overhead can be sampled repeatedly.
    pub fn clear_shutdown(&self) {
        self.shutdown_tx.send_replace(false);
    }

    /// Advances directly to a due wake event.
    pub async fn wake_now(&mut self) -> RelayInteractionBenchmarkEvent {
        self.next_at(Some(Instant::now())).await
    }

    /// Returns all atomically tracked work owned by the isolated scheduler.
    pub fn quiesce_work(&self) -> usize {
        self.quiesce_counters.outstanding_work()
    }

    /// Advances the shared scheduler by one event without exposing runtime carrier types.
    pub async fn next(&mut self) -> RelayInteractionBenchmarkEvent {
        self.next_at(None).await
    }

    async fn next_at(&mut self, wake_at: Option<Instant>) -> RelayInteractionBenchmarkEvent {
        let (event, _work) = self
            .interaction
            .next(wake_at)
            .await
            .verified(
                "the benchmark keeps its sources and command channel alive, so the interaction \
                 never ends",
            )
            .into_parts();
        match event {
            RelayInteractionEvent::Batch { batch, .. } => RelayInteractionBenchmarkEvent::Batch {
                rows: batch.message_count(),
            },
            RelayInteractionEvent::ForceFlush(completion) => {
                assert!(completion.complete());
                RelayInteractionBenchmarkEvent::ForceFlush
            }
            RelayInteractionEvent::Wake => RelayInteractionBenchmarkEvent::Wake,
            RelayInteractionEvent::Stopped(_) => RelayInteractionBenchmarkEvent::Stopped,
            RelayInteractionEvent::Command(BenchmarkCommand::Drain) => {
                RelayInteractionBenchmarkEvent::Command
            }
        }
    }
}

fn benchmark_schema() -> triomphe::Arc<CompiledSchema> {
    static SCHEMA: OnceLock<triomphe::Arc<CompiledSchema>> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            triomphe::Arc::new(compile_schema(&CreateSchema {
                name: Identifier::parse("relay_interaction_benchmark").assured(
                    "the name is built here from fixed text that satisfies the identifier grammar",
                ),
                fields: vec![nervix_models::SchemaField {
                    name: Identifier::parse("value").assured(
                        "the name is built here from fixed text that satisfies the identifier \
                         grammar",
                    ),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            }))
        })
        .clone()
}

fn benchmark_batch() -> RelayRecordBatch {
    let watermark = Timestamp::from_unix_nanos(1);
    let schema = benchmark_schema();
    let mut builder = schema.batch_builder(1);
    builder
        .append(Some(&RuntimeValue::I64(1)))
        .assured("the single I64 value is built here against the schema declared beside it");
    builder
        .finish_row()
        .assured("the single I64 value is built here against the schema declared beside it");
    let batch = builder
        .finish()
        .assured("the single I64 value is built here against the schema declared beside it");
    let record = batch
        .runtime_row(
            0,
            RuntimeRecordMetadata::from_ingested_at_watermarks(watermark, watermark),
        )
        .assured("the single I64 value is built here against the schema declared beside it");
    RelayRecordBatch::single(schema, None, record, AckSet::empty())
        .assured("the single I64 value is built here against the schema declared beside it")
}
