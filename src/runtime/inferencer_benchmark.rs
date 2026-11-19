//! One inferencer branch driven through its production processor node, for owner tests and for
//! measuring the admission of a batch from `INPUTS` through the model to its routes' dispatch.
//!
//! This module only exists in tests and with the `benchmarks` feature. Its public surface exposes
//! benchmark operations instead of Nervix runtime carriers.
//!
//! Layer: harness.
//! - **Owns.** A concrete inferencer branch planned from a Model, its resource store, its output
//!   relays and their downstream receivers.
//! - **Depends on.** The production planner, branch template, processor node and ONNX fixtures.
//! - **Must not know.** Schedules, interconnect or control-plane placement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "drives one inferencer branch through its production owner"
    )
)]

use std::{collections::BTreeMap, num::NonZeroUsize, path::PathBuf, time::Duration};

use ahash::HashMap;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AckMode, Assignment, AssignmentTarget, BranchSelection, ClusterNodeName, CreateInferencer,
    CreateSchema, DomainConfig, DomainName, DomainNodeRef, DomainPace, DomainStartPoint,
    DomainState, DomainStatus, FieldName, FlushPolicy, InferencerModelFingerprint,
    InferencerStateBinding, InferencerStateInitial, InferencerTensorDeclaration,
    InferencerTensorDimension, InferencerTensorElementType, InferencerTensorMapping,
    InferencerTensorRepresentation, InferencerTensorSchema, MessageErrorPolicy, Model, ModelKind,
    ModelName, ParseAsType, ProcessorInputs, ProcessorOutput, ProcessorOutputs, RelayName,
    ResourceId, ResourceName, RouteConstruction, SchemaField, SchemaFingerprint, Timestamp, Unit,
};
use nervix_primitives::sync::{Arc, StdArc};
use tempfile::TempDir;

use super::{
    BranchPresence, BranchRuntime, DomainRoutingSnapshot, RelayBoundaryFanout,
    RelayBoundaryServices, RelayBranchPresences, RelayOwnerTask, RelayRecordBatch, RelayRetention,
    RelayRuntimeFanIn, Runtime, planning,
};
use crate::{
    registry::{PlannedModel, branched_node_specs_from_models},
    runtime_ack::{AckRootTracker, AckSet},
    runtime_schema::{CompiledSchema, RuntimeRecordBatch, compile_schema},
};

/// What the model a fixture branch runs looks like, and how the branch's routes flush.
pub(in crate::runtime) struct InferencerFixtureModel {
    /// The file under the fixture resource's `models/` directory.
    pub(in crate::runtime) file: &'static str,
    /// The input relay's fields; every `INPUTS` mapping reads one of them by its tensor's name.
    pub(in crate::runtime) input_fields: Vec<(&'static str, ParseAsType)>,
    pub(in crate::runtime) inputs: Vec<(&'static str, Vec<InferencerTensorDimension>)>,
    pub(in crate::runtime) state: Vec<(&'static str, &'static str, Vec<InferencerTensorDimension>)>,
    pub(in crate::runtime) outputs: Vec<(&'static str, Vec<InferencerTensorDimension>)>,
    /// Each route writes every output tensor to its own relay, under the tensor's name.
    pub(in crate::runtime) routes: Vec<(&'static str, FlushPolicy)>,
}

/// One unbranched inferencer branch, its input relay schema, and a receiver downstream of each
/// route's relay.
pub(in crate::runtime) struct InferencerBranchFixture {
    pub(in crate::runtime) runtime: Runtime,
    pub(in crate::runtime) processor: ModelName,
    pub(in crate::runtime) input: RelayName,
    pub(in crate::runtime) input_schema: Arc<CompiledSchema>,
    pub(in crate::runtime) branch: BranchRuntime,
    pub(in crate::runtime) downstream: Vec<RelayRuntimeFanIn>,
    owners: Vec<RelayOwnerTask>,
    _store_root: TempDir,
}

fn name<T: std::str::FromStr>(text: &str) -> T
where
    T::Err: std::fmt::Debug,
{
    text.parse()
        .assured("fixture names are fixed valid identifiers")
}

fn dense(dimensions: Vec<InferencerTensorDimension>) -> InferencerTensorSchema {
    InferencerTensorSchema {
        representation: InferencerTensorRepresentation::Dense,
        element_type: InferencerTensorElementType::F32,
        dimensions,
    }
}

fn schema(schema_name: &str, fields: Vec<(String, ParseAsType)>) -> Arc<CompiledSchema> {
    Arc::new(compile_schema(&CreateSchema {
        labels: Default::default(),
        name: name(schema_name),
        fields: fields
            .into_iter()
            .map(|(field, ty)| SchemaField {
                name: name(&field),
                ty,
                optional: false,
                sensitive: false,
            })
            .collect(),
    }))
}

fn field(text: &str) -> nervix_models::Expression {
    nervix_models::Expression::Field(nervix_models::FieldReference {
        scope: nervix_models::FieldScope::Bare,
        field: name::<FieldName>(text),
    })
}

fn input_field(text: &str) -> nervix_models::Expression {
    nervix_models::Expression::Field(nervix_models::FieldReference {
        scope: nervix_models::FieldScope::Input,
        field: name::<FieldName>(text),
    })
}

impl InferencerBranchFixture {
    /// Plan, bind and instantiate the inferencer `model` describes, as a published plan revision
    /// does, against a resource store holding the generated ONNX fixtures.
    pub(in crate::runtime) async fn new(model: InferencerFixtureModel) -> Self {
        let runtime = Runtime::new();
        let domain = name::<DomainName>("inference");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            DomainState {
                id: domain.clone(),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                },
                status: DomainStatus::Running,
                start_version: 0,
                last_start: DomainStartPoint::Resume,
                clock: None,
                stop_cause: None,
            },
        )]));
        let resource = name::<ResourceName>("inference");
        let model_bytes = std::fs::read(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/onnx")
                .join(model.file),
        )
        .assured("the generated ONNX fixtures exist");
        let store_root = tempfile::tempdir().assured("the host can create a temporary directory");
        let store =
            crate::resource::ResourceStore::open(store_root.path(), runtime.executor().clone())
                .assured("a fresh resource store opens");
        // The model is uploaded as a session upload is: one bundle file, staged and installed.
        let mut bundle = store
            .create_bundle_stager()
            .await
            .assured("a fresh resource store opens a bundle");
        bundle
            .create_file(&PathBuf::from("models").join(model.file))
            .await
            .assured("the fixture model path is a valid bundle path");
        // Every generated ONNX fixture is a few hundred bytes, one staging chunk.
        let chunk = store
            .admit_staging_bytes(&model_bytes)
            .await
            .assured("the fixture store admits the model's bytes");
        bundle
            .write_chunk(chunk)
            .await
            .assured("the fixture bundle takes the model's bytes");
        let staged = bundle
            .finish()
            .await
            .assured("the fixture bundle builds its archive");
        store
            .install_from_archive_path(
                ResourceId::new(domain.clone(), resource.clone(), 1),
                staged.path(),
                staged.root_checksum().to_owned(),
                name::<ClusterNodeName>("node-1"),
                Timestamp::from_unix_nanos(0),
            )
            .await
            .assured("the fixture model installs");
        runtime.attach_resource_store(StdArc::new(store));

        let processor = name::<ModelName>("score");
        let input = name::<RelayName>("features");
        let input_schema = schema(
            "features",
            model
                .input_fields
                .iter()
                .map(|(field, ty)| ((*field).to_owned(), ty.clone()))
                .collect(),
        );
        let output_schema = schema(
            "scored",
            model
                .outputs
                .iter()
                .map(|(tensor, dimensions)| {
                    (
                        (*tensor).to_owned(),
                        dense(dimensions.clone()).message_type(),
                    )
                })
                .collect(),
        );
        let mut relay_schemas = HashMap::default();
        relay_schemas.insert(input.clone(), input_schema.clone());
        let mut relay_services = HashMap::default();
        let mut downstream = Vec::new();
        let mut owners = Vec::new();
        let mut routes = Vec::new();
        for (relay, flush) in &model.routes {
            let relay = name::<RelayName>(relay);
            let fanout = RelayBoundaryFanout::direct_with_capacity(
                NonZeroUsize::new(64).verified("a fixed nonzero capacity"),
            );
            downstream.push(RelayRuntimeFanIn::new(
                fanout.runtime_consumer_receiver_for_mode(AckMode::Attached),
            ));
            let services = Arc::new(RelayBoundaryServices::new(
                fanout,
                1,
                0,
                Vec::new(),
                None,
                RelayBranchPresences::single(Arc::new(BranchPresence::new())),
                Arc::new(AckRootTracker::default()),
            ));
            owners.push(
                runtime
                    .spawn_relay_owner_task(
                        &domain,
                        &relay,
                        Unit::SINGLE,
                        services.clone(),
                        RelayRetention::default(),
                    )
                    .assured("a whole relay schedules its single unit"),
            );
            relay_schemas.insert(relay.clone(), output_schema.clone());
            relay_services.insert(relay.clone(), services);
            routes.push(ProcessorOutput {
                relay,
                construction: RouteConstruction {
                    assignments: model
                        .outputs
                        .iter()
                        .map(|(tensor, _)| Assignment {
                            target: AssignmentTarget::bare(name(tensor)),
                            value: field(tensor),
                        })
                        .collect(),
                    ..RouteConstruction::default()
                },
                flush_policy: Some(flush.clone()),
                message_error_policy: MessageErrorPolicy::Log,
                branch: None,
            });
        }
        runtime.inner.domain_routings.insert(
            domain.clone(),
            StdArc::new(nervix_primitives::publication::ArcSwap::from_pointee(
                DomainRoutingSnapshot {
                    relay_schemas: relay_schemas.clone(),
                    relay_services: relay_services.clone(),
                    ..DomainRoutingSnapshot::default()
                },
            )),
        );
        let inferencer = Model::Inferencer(CreateInferencer {
            labels: Default::default(),
            name: name("score"),
            from: ProcessorInputs::single(input.clone()),
            output_routes: ProcessorOutputs { routes },
            branched_by: BranchSelection::unbranched(),
            resource,
            resource_version: 1,
            file: format!("models/{}", model.file),
            inputs: model
                .inputs
                .iter()
                .map(|(tensor, dimensions)| InferencerTensorMapping {
                    tensor: (*tensor).to_owned(),
                    schema: dense(dimensions.clone()),
                    expression: input_field(tensor),
                })
                .collect(),
            state: model
                .state
                .iter()
                .map(|(input, output, dimensions)| InferencerStateBinding {
                    input: (*input).to_owned(),
                    output: (*output).to_owned(),
                    schema: dense(dimensions.clone()),
                    initial: InferencerStateInitial::Zeros,
                })
                .collect(),
            output_schema: model
                .outputs
                .iter()
                .map(|(tensor, dimensions)| InferencerTensorDeclaration {
                    tensor: (*tensor).to_owned(),
                    schema: dense(dimensions.clone()),
                })
                .collect(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        });
        if !model.state.is_empty() {
            // Schedule application publishes the identity a stateful branch's replicated state is
            // keyed by before any branch binds it; a fixture branch needs the same.
            runtime.publish_state_assignment(
                DomainNodeRef::node_in(domain.clone(), ModelKind::Inferencer, processor.clone()),
                super::ScheduledStateAssignment {
                    identity: super::ScheduledStateIdentity {
                        schema_fingerprint: SchemaFingerprint::from_digest([1; 32]),
                        inferencer_model: Some(InferencerModelFingerprint::from_digest([2; 32])),
                        wasm_state_generations: None,
                    },
                    checkpoint_owners: None,
                },
            );
        }
        let specs = branched_node_specs_from_models(std::iter::once(PlannedModel {
            kind: ModelKind::Inferencer,
            identifier: processor.clone(),
            model: inferencer,
        }));
        let spec = specs
            .processors
            .first()
            .verified("one inferencer plans one processor");
        let template =
            planning::bound_test_template(&runtime, spec, &relay_schemas, &relay_services)
                .await
                .assured("the fixture inferencer's branch template binds");
        let branch = template
            .instantiate(&runtime, &domain, None, 1)
            .await
            .assured("the fixture inferencer branch instantiates")
            .into_inner();
        Self {
            runtime,
            processor,
            input,
            input_schema,
            branch,
            downstream,
            owners,
            _store_root: store_root,
        }
    }

    /// A batch of the input relay whose fields are all `F32` pairs: `columns` names each field
    /// with its pairs in row order, and every row carries the acknowledgement `acks` makes.
    pub(in crate::runtime) fn pairs_batch(
        &self,
        columns: Vec<(&'static str, Vec<[f32; 2]>)>,
        mut acks: impl FnMut() -> AckSet,
    ) -> RelayRecordBatch {
        let arrow_schema = self.input_schema.arrow_schema();
        let mut arrays = Vec::with_capacity(columns.len());
        let mut rows = 0;
        for (field, pairs) in columns {
            let index = arrow_schema
                .index_of(field)
                .assured("fixture columns name input relay fields");
            let arrow_schema::DataType::FixedSizeList(item, length) =
                arrow_schema.field(index).data_type()
            else {
                panic!("fixture input field '{field}' is not an F32 pair");
            };
            rows = pairs.len();
            let values = pairs.into_iter().flatten().collect::<Vec<_>>();
            let array: arrow_array::ArrayRef = StdArc::new(
                arrow_array::FixedSizeListArray::try_new(
                    item.clone(),
                    *length,
                    StdArc::new(arrow_array::Float32Array::from(values)),
                    None,
                )
                .assured("fixture pairs form a fixed-size list"),
            );
            arrays.push(array);
        }
        let record_batch = arrow_array::RecordBatch::try_new(arrow_schema.clone(), arrays)
            .assured("fixture columns follow the input relay schema");
        let batch = RuntimeRecordBatch::from_record_batch(arrow_schema, record_batch)
            .assured("fixture batches carry the exact input relay schema");
        RelayRecordBatch::from_runtime_batch(
            self.input_schema.clone(),
            None,
            batch,
            super::RecordMetadataColumns::from_ingestion_nanos(vec![0; rows]),
            (0..rows).map(|_| acks()).collect(),
        )
        .assured("fixture batches carry one acknowledgement per row")
    }

    /// Admit `batch` into the branch's inferencer, as its branch task does. Owner tests drain the
    /// routes themselves afterwards; the benchmark drains while it admits.
    #[cfg(test)]
    pub(in crate::runtime) async fn admit(&mut self, batch: RelayRecordBatch) {
        self.branch
            .execute_processor_input(&self.processor, &self.input, batch)
            .await;
    }

    pub(in crate::runtime) async fn stop(self) {
        let Self {
            runtime, owners, ..
        } = self;
        for owner in owners {
            owner
                .stop(Duration::from_secs(1))
                .await
                .assured("the fixture relay owner stops");
        }
        // The relay owners ran on this runtime's services, so it ends after them.
        drop(runtime);
    }
}

/// The model shape one measurement exercises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferencerBenchmarkCase {
    /// A per-message model without `STATE`: `simple_score`, two features to one score.
    OneByOne,
    /// A model whose tensors declare `BATCH`: `batch_score`, one call for the whole batch.
    Batched,
    /// A per-message model that carries `STATE`: `running_sum`.
    Stateful,
}

/// One inferencer branch whose single `FLUSH IMMEDIATE` route is drained after every admitted
/// batch, so each measurement covers `INPUTS`, the model, output construction and dispatch.
pub struct InferencerBenchmark {
    fixture: InferencerBranchFixture,
    batch: RelayRecordBatch,
    rows: u64,
}

impl InferencerBenchmark {
    pub async fn new(case: InferencerBenchmarkCase, rows: NonZeroUsize) -> Self {
        let pair = || ParseAsType::Array {
            element: Box::new(ParseAsType::F32),
            len: std::num::NonZeroU32::new(2).verified("a pair has two elements"),
        };
        let fixed = |size: u32| {
            InferencerTensorDimension::Fixed(
                std::num::NonZeroU32::new(size).verified("fixture axes are positive"),
            )
        };
        let routes = vec![("scored", FlushPolicy::Immediate)];
        let model = match case {
            InferencerBenchmarkCase::OneByOne => InferencerFixtureModel {
                file: "simple_score.onnx",
                input_fields: vec![("features", pair())],
                inputs: vec![("features", vec![fixed(2)])],
                state: Vec::new(),
                outputs: vec![("score", vec![fixed(1)])],
                routes,
            },
            InferencerBenchmarkCase::Batched => InferencerFixtureModel {
                file: "batch_score.onnx",
                input_fields: vec![("features", pair()), ("mask", pair())],
                inputs: vec![
                    ("features", vec![InferencerTensorDimension::Batch, fixed(2)]),
                    ("mask", vec![InferencerTensorDimension::Batch, fixed(2)]),
                ],
                state: Vec::new(),
                outputs: vec![("scores", vec![InferencerTensorDimension::Batch, fixed(2)])],
                routes,
            },
            InferencerBenchmarkCase::Stateful => InferencerFixtureModel {
                file: "running_sum.onnx",
                input_fields: vec![("features", pair())],
                inputs: vec![("features", vec![fixed(2)])],
                state: vec![("state_in", "state_out", vec![fixed(2)])],
                outputs: vec![("score", vec![fixed(1)])],
                routes,
            },
        };
        let fixture = InferencerBranchFixture::new(model).await;
        let mut pairs = Vec::with_capacity(rows.get());
        for row in 0..rows.get() {
            let value = f32::from(u16::try_from(row % 1024).verified("a bounded row index"));
            pairs.push([value, 1.0]);
        }
        let mut columns = vec![("features", pairs.clone())];
        if case == InferencerBenchmarkCase::Batched {
            columns.push(("mask", pairs));
        }
        let batch = fixture.pairs_batch(columns, AckSet::empty);
        Self {
            fixture,
            batch,
            rows: u64::try_from(rows.get()).assured("a benchmark batch size fits u64"),
        }
    }

    /// Admit one batch and wait until its route has dispatched every row; answers the rows
    /// delivered. The route's relay is drained while the batch is admitted, as its downstream
    /// would drain it, so a relay with fewer places than the batch has dispatches never stalls it.
    pub async fn admit(&mut self) -> u64 {
        let fixture = &mut self.fixture;
        let admission = fixture.branch.execute_processor_input(
            &fixture.processor,
            &fixture.input,
            self.batch.clone(),
        );
        let route = &mut fixture.downstream[0];
        let rows = self.rows;
        let drain = async move {
            let mut delivered = 0;
            while delivered < rows {
                nervix_primitives::task::consume_budget().await;
                let batch = route.recv().await.assured("the benchmark relay stays open");
                delivered += batch.message_count();
                batch.ack_success();
            }
            delivered
        };
        let ((), delivered) = futures_util::future::join(admission, drain).await;
        delivered
    }

    pub async fn stop(self) {
        self.fixture.stop().await;
    }
}
