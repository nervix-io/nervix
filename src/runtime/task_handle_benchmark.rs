//! Retained task dependencies measured through their production owners.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Warm task fixtures and individual recurring operations for measurement.
//! - **Depends on.** Runtime task owners, published lifecycle and resolved metric children.
//! - **Must not know.** Compiler decisions, network orchestration or external services.

use std::{
    collections::BTreeMap,
    future::Future,
    task::{Context, Waker},
};

use nervix_models::{DomainPace, DomainStartPoint, DomainState, DomainStatus, SchemaFingerprint};

use super::{
    shared_clients::{PoolWaitRegistration, PoolWaitSlot},
    *,
};
use crate::metrics::{ClientIngestorSeries, IngestorQuiesceMetrics};

#[derive(Clone, Copy, Debug)]
pub enum TaskHandlePath {
    HealthyStatus,
    ReadyPoolBorrow,
    Confirmation,
    IngestAck,
    GeneratorAck,
    MetricsDirty,
    Freeze,
    IngestClock,
    KafkaGeneration,
    ProcessorClock,
    WasmBoundary,
    IdleForceFlush,
    ClientOutcome,
    QuiescedPayload,
    SubscriptionDrop,
    ErrorRoutePublication,
}

pub const TASK_HANDLE_PATHS: [TaskHandlePath; 16] = [
    TaskHandlePath::HealthyStatus,
    TaskHandlePath::ReadyPoolBorrow,
    TaskHandlePath::Confirmation,
    TaskHandlePath::IngestAck,
    TaskHandlePath::GeneratorAck,
    TaskHandlePath::MetricsDirty,
    TaskHandlePath::Freeze,
    TaskHandlePath::IngestClock,
    TaskHandlePath::KafkaGeneration,
    TaskHandlePath::ProcessorClock,
    TaskHandlePath::WasmBoundary,
    TaskHandlePath::IdleForceFlush,
    TaskHandlePath::ClientOutcome,
    TaskHandlePath::QuiescedPayload,
    TaskHandlePath::SubscriptionDrop,
    TaskHandlePath::ErrorRoutePublication,
];

pub struct TaskHandleBenchmark {
    runtime: Runtime,
    domain: DomainName,
    ingestor: IngestorName,
    status: task_status::TaskStatus<u8>,
    pool: PoolWaitRegistration,
    confirmations: Arc<AtomicUsize>,
    ingest: IngestTaskHandles,
    generator: Arc<AckRootTracker>,
    metrics: BranchMetricsMark,
    freeze: OwnershipHandoffFreezeWatch,
    lifecycle: DomainClockLifecycle,
    clock: DomainClock,
    wasm: Arc<ReplicatedWasmProcessorState>,
    force_flush: DomainForceFlushParticipant,
    client_metrics: ClientIngestorSeries,
    quiesce_metrics: IngestorQuiesceMetrics,
    dropped: prometheus::IntCounter,
    routing: SharedDomainRouting,
}

impl Default for TaskHandleBenchmark {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskHandleBenchmark {
    pub fn new() -> Self {
        let runtime = Runtime::new();
        let domain = DomainName::parse("task_handles").assured("the fixture domain is valid");
        let ingestor = IngestorName::parse("source").assured("the fixture ingestor is valid");
        let domains = BTreeMap::from([(
            domain.clone(),
            DomainState {
                id: domain.clone(),
                config: nervix_models::DomainConfig {
                    pace: DomainPace::Unpaced,
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: DomainStatus::Running,
                start_version: 0,
                last_start: DomainStartPoint::Resume,
                clock: None,
            },
        )]);
        let authority = nervix_models::DomainClockAuthority::assigned(
            nervix_models::DomainClockAuthorityRevision::INITIAL,
            nervix_models::ClusterNodeIdentity::new(
                ClusterNodeName::parse("benchmark").assured("the node is valid"),
                nervix_models::ClusterNodeIncarnation::new(1),
            ),
        );
        runtime.sync_committed_domains(&domains, &BTreeMap::from([(domain.clone(), authority)]));
        let entity = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeState::BranchAggregated,
            kind: ModelKind::Ingestor,
            identifier: ModelName::from(&ingestor),
            branch_key: None,
        };
        runtime
            .replicated_branch_aggregated_state(
                placement,
                None,
                ClusterNodeName::parse("benchmark").assured("the node is valid"),
                Vec::new(),
                0,
            )
            .assured("empty metric state is valid");
        let ingest = runtime
            .ingest_task_handles(&domain, &ingestor)
            .assured("the fixture domain is installed");
        let lifecycle = runtime
            .domain_clock_lifecycle(&domain)
            .assured("the fixture lifecycle exists");
        let clock = runtime
            .bind_domain_clock(&domain)
            .assured("the unpaced fixture clock is installed");
        let wasm_node = ModelName::parse("guest").assured("the processor is valid");
        let schema = SchemaFingerprint::from_digest([1; 32]);
        runtime.publish_state_assignment(
            DomainNodeRef::node_in(domain.clone(), ModelKind::WasmProcessor, wasm_node.clone()),
            ScheduledStateAssignment {
                identity: ScheduledStateIdentity {
                    schema_fingerprint: schema,
                    wasm_state_generations: Some(nervix_models::WasmStateGenerations::first()),
                },
                checkpoint_owners: None,
            },
        );
        let wasm = runtime
            .replicated_wasm_processor_state(RuntimeStatePlacement {
                domain: domain.clone(),
                kind: ModelKind::WasmProcessor,
                identifier: wasm_node,
                branch_key: None,
                state: RuntimeState::WasmProcessor {
                    schema,
                    generation: nervix_models::WasmStateGeneration::FIRST,
                },
            })
            .assured("empty guest state is valid");
        let coordinator = DomainForceFlush::new();
        let force_flush = DomainForceFlush::subscribe(&coordinator, None);
        let pool = runtime.register_pool_wait(
            entity.clone(),
            ClientName::parse("pool").assured("the client is valid"),
        );
        let confirmations = Arc::new(AtomicUsize::new(0));
        let generator = runtime.domain_ack_root_tracker(&domain);
        let metrics = runtime.branch_metrics_mark(&domain, ModelKind::Ingestor, &ingestor);
        let freeze = OwnershipHandoffFreezeWatch::new(&runtime, entity);
        let client_metrics = runtime.metrics().client_ingestor_series(&domain, &ingestor);
        let quiesce_metrics = runtime
            .metrics()
            .register_ingestor_quiesce(&domain, &ingestor, None);
        let dropped = runtime.metrics().session_subscription_dropped_rows(
            &domain,
            &RelayName::parse("events").assured("the relay is valid"),
        );
        let routing = StdArc::new(ArcSwap::from_pointee(DomainRoutingSnapshot::default()));
        Self {
            runtime,
            domain,
            ingestor,
            status: task_status::TaskStatus::default(),
            pool,
            confirmations,
            ingest,
            generator,
            metrics,
            freeze,
            lifecycle,
            clock,
            wasm,
            force_flush,
            client_metrics,
            quiesce_metrics,
            dropped,
            routing,
        }
    }

    pub fn run(&mut self, path: TaskHandlePath) {
        match path {
            TaskHandlePath::HealthyStatus => self.status.clear(),
            TaskHandlePath::ReadyPoolBorrow => {
                let mut borrow = std::pin::pin!(PoolWaitSlot::borrow(
                    &self.pool.slot,
                    std::future::ready(())
                ));
                assert!(
                    borrow
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_ready()
                );
            }
            TaskHandlePath::Confirmation => drop(
                super::emitter_supervision::EmitterConfirmationWaitGuard::begin(
                    &self.confirmations,
                ),
            ),
            TaskHandlePath::IngestAck => {
                let (acks, completion) = self.ingest.tracked_root();
                acks.ack_success();
                drop(completion);
            }
            TaskHandlePath::GeneratorAck => {
                let (acks, completion) = AckSet::tracked_root(self.generator.clone());
                acks.ack_success();
                drop(completion);
            }
            TaskHandlePath::MetricsDirty => self.metrics.mark(),
            TaskHandlePath::Freeze => {
                std::hint::black_box(self.freeze.observe().is_frozen());
            }
            TaskHandlePath::IngestClock => {
                std::hint::black_box(
                    self.ingest
                        .ingestion_time(&self.domain, &self.ingestor)
                        .assured("the fixture clock is installed")
                        .now(),
                );
            }
            TaskHandlePath::KafkaGeneration => {
                std::hint::black_box(self.lifecycle.generation());
            }
            TaskHandlePath::ProcessorClock => {
                std::hint::black_box(
                    self.clock
                        .snapshot()
                        .assured("the fixture clock is installed"),
                );
            }
            TaskHandlePath::WasmBoundary => {
                std::hint::black_box(
                    self.runtime
                        .wasm_checkpoint_boundary(&self.wasm)
                        .assured("the guest is in its assigned generation"),
                );
            }
            TaskHandlePath::IdleForceFlush => assert!(
                self.force_flush
                    .pending_completion()
                    .assured("the coordinator remains open")
                    .is_none()
            ),
            TaskHandlePath::ClientOutcome => self
                .client_metrics
                .count(&nervix_models::ClientSubmissionOutcome::Completed),
            TaskHandlePath::QuiescedPayload => self
                .runtime
                .metrics()
                .increment_ingestor_quiesce_dropped(&self.quiesce_metrics, 1),
            TaskHandlePath::SubscriptionDrop => self.dropped.inc(),
            TaskHandlePath::ErrorRoutePublication => {
                std::hint::black_box(&self.routing.load().message_error_plans);
            }
        }
    }
}
