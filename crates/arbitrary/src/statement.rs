//! Every statement form: Model creation, domain lifecycle, administration, every `ALTER`, and the
//! read-only queries.

use std::num::NonZeroUsize;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AlterBranch, AlterBranchOperation, AlterDeduplicator, AlterDeduplicatorOperation, AlterEmitter,
    AlterEmitterOperation, AlterGenerator, AlterGeneratorOperation, AlterIngestor,
    AlterIngestorOperation, AlterJunction, AlterPlacement, AlterPlacementOperation,
    AlterProcessorOperation, AlterReingestor, AlterRelay, AlterRelayOperation, AlterReorderer,
    AlterReordererOperation, AlterSchema, AlterSchemaOperation, AlterTransportIngestorOperation,
    AlterWireSchema, AlterWireSchemaOperation, Backup, BackupCapture, BackupResources, BackupScope,
    ClientIngestorEntity, CordonNode, CreateDomain, CreateResource, CreateStatement, CreateUser,
    DescribeBranch, DescribeClient, DescribeCluster, DescribeCorrelator, DescribeDeduplicator,
    DescribeDomain, DescribeEmitter, DescribeEndpoint, DescribeIngestor, DescribeJunction,
    DescribeLicense, DescribeLookup, DescribePlacement, DescribeReingestor, DescribeRelay,
    DescribeReorderer, DescribeResource, DescribeTransaction, DescribeUdf, DescribeWasmProcessor,
    DescribeWindowProcessor, DomainConfig, DomainPace, DomainTimeRate, DrainNode, DropModel,
    DropNode, EmitterBatchLegality, EmitterBodyForm, EmitterEntityKind, EmitterEnvelopeAlteration,
    EmitterReferenceKind, ExistingUserPolicy, FieldName, HttpIngestorEntity, IngestorAlteration,
    IngestorEntityKind, IngestorEntityModel, IngestorReferenceKind, InspectionFormat,
    KafkaIngestorEntity, LookupQuery, ModelKind, ModelName, MqttIngestorEntity, NatsIngestorEntity,
    NodeRef, PlacementPolicy, PrometheusIngestorEntity, PulsarIngestorEntity,
    RabbitMqIngestorEntity, RebindResource, RebindResourceMembers, RebindResourceSelection,
    RedisIngestorEntity, RelayBranching, RelayName, Relocation, RelocationMember,
    RelocationPreferenceOverride, RelocationPreferenceStrategy, RelocationSelection,
    ResetWasmState, Restore, RestoreLifecycle, RestoreMode, RestoreScope, RestoreState,
    ShowClusterStatus, ShowCreate, ShowEmitters, ShowIngestors, ShowPlacements, ShowProcessorState,
    ShowRelayMaterializedState, ShowTransactions, ShowUdfs, ShowWasmProcessorState,
    SqsIngestorEntity, StartDomain, StateBranchField, StateBranchSelection,
    StateInspectedProcessor, Statement, StopDomain, SubscriptionBinding, SubscriptionLiteral,
    SyslogIngestorEntity, TransactionInspectionRequest, TransactionInspectionTarget,
    TransactionOperationNumber, UncordonNode, UploadResource, WebsocketsIngestorEntity,
    ZeroMqIngestorEntity,
};
use strum::IntoEnumIterator as _;

use crate::{
    Arbitrary, Domain, GeneratedName,
    route::{RouteBranch, RouteFlush, RouteShape},
};

/// The most items a generated operation list, member list or binding list holds.
const ITEMS: usize = 3;

/// The kinds Plexus's `DROP` names. It has no `DROP` for a branch, generator, lookup, signaling
/// protocol, WASM processor or window processor, which the vocabulary's `DropModel` still holds.
const DROPPABLE_KINDS: [ModelKind; 19] = [
    ModelKind::Schema,
    ModelKind::WireJsonSchema,
    ModelKind::WireCborSchema,
    ModelKind::WireAvroSchema,
    ModelKind::Codec,
    ModelKind::Client,
    ModelKind::Vhost,
    ModelKind::Endpoint,
    ModelKind::Ingestor,
    ModelKind::Reingestor,
    ModelKind::Reorderer,
    ModelKind::Inferencer,
    ModelKind::Relay,
    ModelKind::Junction,
    ModelKind::Deduplicator,
    ModelKind::Correlator,
    ModelKind::Emitter,
    ModelKind::Placement,
    ModelKind::Udf,
];

/// The kinds a relocation moves: the scheduled runtime nodes.
const RELOCATABLE_KINDS: [ModelKind; 13] = [
    ModelKind::Ingestor,
    ModelKind::Reingestor,
    ModelKind::Generator,
    ModelKind::Junction,
    ModelKind::Deduplicator,
    ModelKind::Correlator,
    ModelKind::Reorderer,
    ModelKind::WindowProcessor,
    ModelKind::Inferencer,
    ModelKind::WasmProcessor,
    ModelKind::Emitter,
    ModelKind::Lookup,
    ModelKind::Relay,
];

/// The kinds that bind a resource version, which `REBIND RESOURCE ... FOR` names.
const RESOURCE_BINDING_KINDS: [ModelKind; 7] = [
    ModelKind::Vhost,
    ModelKind::Codec,
    ModelKind::SignalingProtocol,
    ModelKind::Inferencer,
    ModelKind::WasmProcessor,
    ModelKind::Lookup,
    ModelKind::Client,
];

/// The clauses a generated state inspection statement takes after its head.
struct StateInspectionClauses {
    selection: Option<StateBranchSelection>,
    page: Option<nervix_models::StateInspectionPageRequest>,
    format: InspectionFormat,
}

/// Every variant of [`Statement`], so a property reaches each form as often as any other and a
/// coverage check can ask for each one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum StatementVariant {
    CreateDomain,

    CreateUser,
    CreateResource,
    RebindResource,
    ResetWasmState,
    UploadResource,
    Backup,
    Restore,
    StartDomain,
    StopDomain,
    Create,
    AlterSchema,
    AlterWireJsonSchema,
    AlterWireCborSchema,
    AlterWireAvroSchema,
    AlterRelay,
    AlterJunction,
    AlterDeduplicator,
    AlterReorderer,
    AlterEmitter,
    AlterIngestor,
    AlterReingestor,
    AlterGenerator,
    AlterPlacement,
    Drop,
    DropNode,
    CordonNode,
    UncordonNode,
    DrainNode,
    Relocate,
    DescribeRelocation,
    DescribeRelay,
    DescribeDomain,
    DescribeIngestor,
    DescribeResource,
    DescribeLookup,
    DescribeEndpoint,
    DescribeClient,
    DescribeJunction,
    DescribeDeduplicator,
    DescribeReingestor,
    DescribeCorrelator,
    DescribeReorderer,
    DescribeEmitter,
    DescribeWindowProcessor,
    DescribeWasmProcessor,
    DescribeUdf,
    DescribePlacement,
    LookupQuery,
    ShowCreate,
    ShowUdfs,
    ShowIngestors,
    ShowEmitters,
    ShowPlacements,
    ShowRelayMaterializedState,
    ShowProcessorState,
    ShowWasmProcessorState,
    ShowClusterStatus,
    ShowTransactions,
    DescribeTransaction,
    DescribeCluster,
    DescribeLicense,
    ActivateLicense,
    RefreshLicense,
    AlterBranch,
    DescribeBranch,
    AlterCluster,
}

impl StatementVariant {
    /// The variant `statement` is. The match is exhaustive, so a new statement form does not
    /// compile until the generator is taught to build it.
    pub fn of(statement: &Statement) -> Self {
        match statement {
            Statement::AlterCluster(_) => Self::AlterCluster,
            Statement::CreateDomain(_) => Self::CreateDomain,
            Statement::CreateUser(_) => Self::CreateUser,
            Statement::CreateResource(_) => Self::CreateResource,
            Statement::RebindResource(_) => Self::RebindResource,
            Statement::ResetWasmState(_) => Self::ResetWasmState,
            Statement::UploadResource(_) => Self::UploadResource,
            Statement::Backup(_) => Self::Backup,
            Statement::Restore(_) => Self::Restore,
            Statement::StartDomain(_) => Self::StartDomain,
            Statement::StopDomain(_) => Self::StopDomain,
            Statement::Create(_) => Self::Create,
            Statement::AlterSchema(_) => Self::AlterSchema,
            Statement::AlterWireJsonSchema(_) => Self::AlterWireJsonSchema,
            Statement::AlterWireCborSchema(_) => Self::AlterWireCborSchema,
            Statement::AlterWireAvroSchema(_) => Self::AlterWireAvroSchema,
            Statement::AlterRelay(_) => Self::AlterRelay,
            Statement::AlterJunction(_) => Self::AlterJunction,
            Statement::AlterDeduplicator(_) => Self::AlterDeduplicator,
            Statement::AlterReorderer(_) => Self::AlterReorderer,
            Statement::AlterEmitter(_) => Self::AlterEmitter,
            Statement::AlterIngestor(_) => Self::AlterIngestor,
            Statement::AlterReingestor(_) => Self::AlterReingestor,
            Statement::AlterGenerator(_) => Self::AlterGenerator,
            Statement::AlterPlacement(_) => Self::AlterPlacement,
            Statement::AlterBranch(_) => Self::AlterBranch,
            Statement::DescribeBranch(_) => Self::DescribeBranch,
            Statement::Drop(_) => Self::Drop,
            Statement::DropNode(_) => Self::DropNode,
            Statement::CordonNode(_) => Self::CordonNode,
            Statement::UncordonNode(_) => Self::UncordonNode,
            Statement::DrainNode(_) => Self::DrainNode,
            Statement::Relocate(_) => Self::Relocate,
            Statement::DescribeRelocation(_) => Self::DescribeRelocation,
            Statement::DescribeRelay(_) => Self::DescribeRelay,
            Statement::DescribeDomain(_) => Self::DescribeDomain,
            Statement::DescribeIngestor(_) => Self::DescribeIngestor,
            Statement::DescribeResource(_) => Self::DescribeResource,
            Statement::DescribeLookup(_) => Self::DescribeLookup,
            Statement::DescribeEndpoint(_) => Self::DescribeEndpoint,
            Statement::DescribeClient(_) => Self::DescribeClient,
            Statement::DescribeJunction(_) => Self::DescribeJunction,
            Statement::DescribeDeduplicator(_) => Self::DescribeDeduplicator,
            Statement::DescribeReingestor(_) => Self::DescribeReingestor,
            Statement::DescribeCorrelator(_) => Self::DescribeCorrelator,
            Statement::DescribeReorderer(_) => Self::DescribeReorderer,
            Statement::DescribeEmitter(_) => Self::DescribeEmitter,
            Statement::DescribeWindowProcessor(_) => Self::DescribeWindowProcessor,
            Statement::DescribeWasmProcessor(_) => Self::DescribeWasmProcessor,
            Statement::DescribeUdf(_) => Self::DescribeUdf,
            Statement::DescribePlacement(_) => Self::DescribePlacement,
            Statement::LookupQuery(_) => Self::LookupQuery,
            Statement::ShowCreate(_) => Self::ShowCreate,
            Statement::ShowUdfs(_) => Self::ShowUdfs,
            Statement::ShowIngestors(_) => Self::ShowIngestors,
            Statement::ShowEmitters(_) => Self::ShowEmitters,
            Statement::ShowPlacements(_) => Self::ShowPlacements,
            Statement::ShowRelayMaterializedState(_) => Self::ShowRelayMaterializedState,
            Statement::ShowProcessorState(_) => Self::ShowProcessorState,
            Statement::ShowWasmProcessorState(_) => Self::ShowWasmProcessorState,
            Statement::ShowClusterStatus(_) => Self::ShowClusterStatus,
            Statement::ShowTransactions(_) => Self::ShowTransactions,
            Statement::DescribeTransaction(_) => Self::DescribeTransaction,
            Statement::DescribeCluster(_) => Self::DescribeCluster,
            Statement::DescribeLicense(_) => Self::DescribeLicense,
            Statement::ActivateLicense(_) => Self::ActivateLicense,
            Statement::RefreshLicense(_) => Self::RefreshLicense,
        }
    }
}

/// Why an offered envelope operation finds a clause or property to write.
const OFFERED_ENVELOPE_IS_DECLARED: &str =
    "emitter_operation_forms offers an envelope operation in Plexus only where the entity \
     declares a clause or property, and the vocabulary draws from every clause and any property";

#[derive(Clone, Copy)]
enum EmitterOperationForm {
    AddFrom,
    DropFrom,
    SetWhere,
    DropWhere,
    SetTo,
    Change,
    SetClient,
    SetEncode,
    DropEncode,
    Construction,
    SetCollect,
    DropCollect,
    SetAttachment,
    SetBatch,
    DropBatch,
    SetFlush,
    SetGeneralError,
    Envelope,
    SetInstances,
}

/// One targeted change an emitter's envelope takes.
#[derive(Clone, Copy)]
enum EnvelopeOperationForm {
    SetClause,
    DropClause,
    SetProperties,
    DropProperties,
}

impl Arbitrary<'_> {
    /// A bounded cluster transition naming each parameter once, with independently drawn values.
    fn alter_cluster(&mut self) -> nervix_models::AlterCluster {
        let assignments = vec![
            nervix_models::ClusterParameterChange::PasswordIterations(
                self.entropy
                    .boundary_biased(1..=nervix_models::PasswordIterations::MAXIMUM),
            ),
            nervix_models::ClusterParameterChange::NodeUnavailabilityTimeout(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::ForcedRecoveryBudget(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::OwnershipHandoffPollInterval(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::EntityGatePollInterval(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::DomainDrainPollInterval(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::RuntimeReadinessPropagationBound(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::DomainDrainTimeout(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::EntityGateDeadline(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::TransactionIdleTimeout(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::TransactionTombstoneRetention(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::CommandRetryValidity(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::StateSnapshotInterval(
                std::time::Duration::from_millis(self.entropy.between(1..=1000)),
            ),
            nervix_models::ClusterParameterChange::TransactionMaxStatements(
                self.entropy.between(1..=256),
            ),
            nervix_models::ClusterParameterChange::TransactionMaxSourceBytes(
                self.entropy.between(1..=256),
            ),
            nervix_models::ClusterParameterChange::TransactionMaxOpen(
                self.entropy.between(1..=256),
            ),
            nervix_models::ClusterParameterChange::CommandExecutionCapacity(
                self.entropy.between(1..=256),
            ),
            nervix_models::ClusterParameterChange::ReplicaCount(self.entropy.between(1..=256)),
            nervix_models::ClusterParameterChange::ResourceMaxArchiveBytes(
                self.entropy.between(1..=256),
            ),
            nervix_models::ClusterParameterChange::ResourceMaxExtractedBytes(
                self.entropy.between(1..=256),
            ),
            nervix_models::ClusterParameterChange::ResourceMaxFileCount(
                self.entropy.between(1..=256),
            ),
            nervix_models::ClusterParameterChange::MaxBatchMessages(self.entropy.between(1..=256)),
            nervix_models::ClusterParameterChange::ClientProducerSessionBytes(
                self.entropy.between(1..=256),
            ),
            nervix_models::ClusterParameterChange::ClientConsumerSessionBytes(
                self.entropy.between(1..=256),
            ),
            nervix_models::ClusterParameterChange::UdfCodeBytes(self.entropy.between(1..=256)),
            nervix_models::ClusterParameterChange::HttpHeaderBytes(self.entropy.between(1..=256)),
            nervix_models::ClusterParameterChange::HttpHeaderFields(self.entropy.between(1..=256)),
            nervix_models::ClusterParameterChange::ClientBatchRows(self.entropy.between(1..=256)),
        ];
        nervix_models::AlterCluster {
            parameters: nervix_models::ClusterParameterUpdates::try_from(assignments)
                .assured("the generated nonempty list names each declared parameter once"),
        }
    }

    /// A statement of any form, each form as likely as any other.
    pub fn statement(&mut self) -> Statement {
        let variants = StatementVariant::iter().collect::<Vec<_>>();
        let count = NonZeroUsize::new(variants.len()).assured("there are statement forms");
        let variant = variants[self.entropy.index(count)];
        self.statement_of(variant)
    }

    /// A statement of the requested form, with every clause it declares generated.
    pub fn statement_of(&mut self, variant: StatementVariant) -> Statement {
        match variant {
            StatementVariant::AlterCluster => Statement::AlterCluster(self.alter_cluster()),
            StatementVariant::CreateDomain => {
                let if_not_exists = self.entropy.flag();
                let pace = if self.entropy.flag() {
                    DomainPace::Paced {
                        period: self.clock_period(),
                        skew: self.clock_skew(),
                    }
                } else {
                    DomainPace::Unpaced
                };
                Statement::CreateDomain(CreateStatement::new(
                    CreateDomain {
                        id: self.name(),
                        config: DomainConfig { pace },
                    },
                    if_not_exists,
                ))
            }
            StatementVariant::CreateUser => {
                let if_not_exists = self.entropy.flag();
                Statement::CreateUser(CreateStatement::new(
                    CreateUser {
                        name: self.name(),
                        password: self.string(),
                    },
                    if_not_exists,
                ))
            }
            StatementVariant::CreateResource => {
                let if_not_exists = self.entropy.flag();
                Statement::CreateResource(CreateStatement::new(
                    CreateResource {
                        identifier: self.name(),
                    },
                    if_not_exists,
                ))
            }
            StatementVariant::RebindResource => {
                let selection = if self.entropy.flag() {
                    RebindResourceSelection::All
                } else {
                    let first = self.resource_binding_ref();
                    let count = self.entropy.count(ITEMS);
                    let mut remaining = Vec::with_capacity(count);
                    for _ in 0..count {
                        remaining.push(self.resource_binding_ref());
                    }
                    RebindResourceSelection::Members(RebindResourceMembers::new(first, remaining))
                };
                Statement::RebindResource(RebindResource {
                    resource: self.name(),
                    version: self.requested_version(),
                    selection,
                })
            }
            StatementVariant::ResetWasmState => {
                let scope = self.state_branch_selection();
                Statement::ResetWasmState(ResetWasmState {
                    processor: self.name(),
                    scope,
                })
            }
            StatementVariant::UploadResource => Statement::UploadResource(UploadResource {
                identifier: self.name(),
                source_path: self.non_empty_string(),
            }),
            StatementVariant::Backup => {
                let scope = match self.entropy.byte() % 3 {
                    0 => BackupScope::Cluster,
                    1 => BackupScope::Domain(None),
                    _ => BackupScope::Domain(Some(self.name())),
                };
                let capture = match self.entropy.byte() % 4 {
                    0 => BackupCapture::Quiesced { timeout: None },
                    1 => BackupCapture::Quiesced {
                        timeout: Some(std::time::Duration::from_nanos(self.entropy.any_u64())),
                    },
                    2 => BackupCapture::Live,
                    _ => BackupCapture::ConfigurationOnly,
                };
                Statement::Backup(Backup {
                    scope,
                    capture,
                    destination: self.non_empty_string(),
                    resources: self
                        .entropy
                        .pick([BackupResources::Included, BackupResources::Omitted]),
                })
            }
            StatementVariant::Restore => {
                let scope = if self.entropy.flag() {
                    RestoreScope::Cluster {
                        existing_users: self.entropy.pick([
                            ExistingUserPolicy::Fail,
                            ExistingUserPolicy::Skip,
                            ExistingUserPolicy::Replace,
                        ]),
                    }
                } else {
                    RestoreScope::Domain {
                        domain: self.name(),
                        target: if self.entropy.flag() {
                            Some(self.name())
                        } else {
                            None
                        },
                    }
                };
                Statement::Restore(Restore {
                    scope,
                    source: self.non_empty_string(),
                    mode: self.entropy.pick([RestoreMode::Apply, RestoreMode::DryRun]),
                    lifecycle: self
                        .entropy
                        .pick([RestoreLifecycle::Stopped, RestoreLifecycle::Resume]),
                    state: self.entropy.pick([
                        RestoreState::All,
                        RestoreState::WithoutSourceOffsets,
                        RestoreState::ConfigurationOnly,
                    ]),
                })
            }
            StatementVariant::StartDomain => {
                let start = self.start_point();
                Statement::StartDomain(StartDomain { start })
            }
            StatementVariant::StopDomain => Statement::StopDomain(StopDomain),
            StatementVariant::Create => {
                let if_not_exists = self.entropy.flag();
                Statement::Create(CreateStatement::new(Box::new(self.model()), if_not_exists))
            }
            StatementVariant::AlterSchema => {
                let operations = self.operations(Self::alter_schema_operation);
                Statement::AlterSchema(AlterSchema {
                    schema: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterWireJsonSchema => {
                let operations = self.operations(Self::alter_json_wire_schema_operation);
                Statement::AlterWireJsonSchema(AlterWireSchema {
                    schema: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterWireCborSchema => {
                let operations = self.operations(Self::alter_cbor_wire_schema_operation);
                Statement::AlterWireCborSchema(AlterWireSchema {
                    schema: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterWireAvroSchema => {
                let operations = self.operations(Self::alter_avro_wire_schema_operation);
                Statement::AlterWireAvroSchema(AlterWireSchema {
                    schema: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterRelay => {
                let operations = self.operations(Self::alter_relay_operation);
                Statement::AlterRelay(AlterRelay {
                    relay: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterBranch => {
                let operations = self.operations(|arbitrary| AlterBranchOperation::SetPartitions {
                    partitions: arbitrary.branch_partitions(),
                });
                Statement::AlterBranch(AlterBranch {
                    branch: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterJunction => {
                let operations = self.operations(|arbitrary| {
                    arbitrary.alter_processor_operation(RouteShape::Transforming, true)
                });
                Statement::AlterJunction(AlterJunction {
                    junction: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterDeduplicator => {
                let operations = self.operations(|arbitrary| match arbitrary.entropy.byte() % 3 {
                    0 => AlterDeduplicatorOperation::SetDeduplicateOn {
                        expressions: arbitrary.key_expression_list(),
                    },
                    1 => AlterDeduplicatorOperation::SetMaxTime {
                        max_time: arbitrary.duration(),
                    },
                    _ => AlterDeduplicatorOperation::Processor(Box::new(
                        arbitrary.alter_processor_operation(RouteShape::Transforming, true),
                    )),
                });
                Statement::AlterDeduplicator(AlterDeduplicator {
                    deduplicator: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterReorderer => {
                let operations = self.operations(|arbitrary| match arbitrary.entropy.byte() % 3 {
                    0 => AlterReordererOperation::SetOrderBy {
                        expressions: arbitrary.key_expression_list(),
                    },
                    1 => AlterReordererOperation::SetMaxTime {
                        max_time: arbitrary.duration(),
                    },
                    _ => AlterReordererOperation::Processor(Box::new(
                        arbitrary.alter_processor_operation(RouteShape::Transforming, true),
                    )),
                });
                Statement::AlterReorderer(AlterReorderer {
                    reorderer: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterEmitter => {
                let kinds = EmitterEntityKind::all().collect::<Vec<_>>();
                let count = NonZeroUsize::new(kinds.len())
                    .assured("the vocabulary declares emitter entities");
                let kind = kinds[self.entropy.index(count)];
                let operations =
                    self.operations(|arbitrary| arbitrary.alter_emitter_operation(kind));
                Statement::AlterEmitter(AlterEmitter {
                    emitter: self.name(),
                    kind,
                    operations,
                })
            }
            StatementVariant::AlterIngestor => {
                let alteration = self.ingestor_alteration();
                Statement::AlterIngestor(AlterIngestor {
                    ingestor: self.name(),
                    alteration,
                })
            }
            StatementVariant::AlterReingestor => {
                let operations = self.operations(|arbitrary| {
                    arbitrary.alter_processor_operation(RouteShape::Transforming, false)
                });
                Statement::AlterReingestor(AlterReingestor {
                    reingestor: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterGenerator => {
                let operations = self.operations(Self::alter_generator_operation);
                Statement::AlterGenerator(AlterGenerator {
                    generator: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterPlacement => {
                let operations = self.operations(Self::alter_placement_operation);
                Statement::AlterPlacement(AlterPlacement {
                    placement: self.name(),
                    operations,
                })
            }
            StatementVariant::Drop => {
                let kind = self.drop_kind();
                Statement::Drop(DropModel {
                    kind,
                    name: self.model_name_of(kind),
                })
            }
            StatementVariant::DropNode => Statement::DropNode(DropNode {
                node_id: self.cluster_node(),
            }),
            StatementVariant::CordonNode => Statement::CordonNode(CordonNode {
                node_id: self.cluster_node(),
            }),
            StatementVariant::UncordonNode => Statement::UncordonNode(UncordonNode {
                node_id: self.cluster_node(),
            }),
            StatementVariant::DrainNode => Statement::DrainNode(DrainNode {
                node_id: self.cluster_node(),
            }),
            StatementVariant::Relocate => Statement::Relocate(self.relocation()),
            StatementVariant::DescribeRelocation => {
                Statement::DescribeRelocation(self.relocation())
            }
            StatementVariant::DescribeRelay => {
                let names = self.distinct_names::<FieldName>(0, ITEMS);
                let mut bindings = Vec::with_capacity(names.len());
                for field in names {
                    bindings.push(SubscriptionBinding {
                        field,
                        value: self.subscription_literal(),
                    });
                }
                Statement::DescribeRelay(DescribeRelay {
                    relay: self.name(),
                    bindings,
                })
            }
            StatementVariant::DescribeDomain => Statement::DescribeDomain(DescribeDomain),
            StatementVariant::DescribeIngestor => Statement::DescribeIngestor(DescribeIngestor {
                ingestor: self.name(),
            }),
            StatementVariant::DescribeResource => Statement::DescribeResource(DescribeResource {
                identifier: self.name(),
                version: if self.entropy.flag() {
                    Some(self.entropy.any_u64())
                } else {
                    None
                },
            }),
            StatementVariant::DescribeLookup => {
                Statement::DescribeLookup(DescribeLookup { name: self.name() })
            }
            StatementVariant::DescribeBranch => Statement::DescribeBranch(DescribeBranch {
                branch: self.name(),
            }),
            StatementVariant::DescribeEndpoint => {
                Statement::DescribeEndpoint(DescribeEndpoint { name: self.name() })
            }
            StatementVariant::DescribeClient => {
                Statement::DescribeClient(DescribeClient { name: self.name() })
            }
            StatementVariant::DescribeJunction => {
                Statement::DescribeJunction(DescribeJunction { name: self.name() })
            }
            StatementVariant::DescribeDeduplicator => {
                Statement::DescribeDeduplicator(DescribeDeduplicator { name: self.name() })
            }
            StatementVariant::DescribeReingestor => {
                Statement::DescribeReingestor(DescribeReingestor { name: self.name() })
            }
            StatementVariant::DescribeCorrelator => {
                Statement::DescribeCorrelator(DescribeCorrelator { name: self.name() })
            }
            StatementVariant::DescribeReorderer => {
                Statement::DescribeReorderer(DescribeReorderer { name: self.name() })
            }
            StatementVariant::DescribeEmitter => {
                Statement::DescribeEmitter(DescribeEmitter { name: self.name() })
            }
            StatementVariant::DescribeWindowProcessor => {
                Statement::DescribeWindowProcessor(DescribeWindowProcessor { name: self.name() })
            }
            StatementVariant::DescribeWasmProcessor => {
                Statement::DescribeWasmProcessor(DescribeWasmProcessor { name: self.name() })
            }
            StatementVariant::DescribeUdf => {
                Statement::DescribeUdf(DescribeUdf { name: self.name() })
            }
            StatementVariant::DescribePlacement => {
                Statement::DescribePlacement(DescribePlacement { name: self.name() })
            }
            StatementVariant::LookupQuery => Statement::LookupQuery(LookupQuery {
                name: self.name(),
                key: self.subscription_literal(),
            }),
            StatementVariant::ShowCreate => {
                let kind = self.model_kind();
                Statement::ShowCreate(ShowCreate {
                    kind,
                    name: self.model_name_of(kind),
                })
            }
            StatementVariant::ShowUdfs => Statement::ShowUdfs(ShowUdfs),
            StatementVariant::ShowIngestors => Statement::ShowIngestors(ShowIngestors),
            StatementVariant::ShowEmitters => Statement::ShowEmitters(ShowEmitters),
            StatementVariant::ShowPlacements => Statement::ShowPlacements(ShowPlacements),
            StatementVariant::ShowRelayMaterializedState => {
                let relay = self.name();
                let clauses = self.state_inspection_clauses();
                Statement::ShowRelayMaterializedState(ShowRelayMaterializedState {
                    relay,
                    selection: clauses.selection,
                    page: clauses.page,
                    format: clauses.format,
                })
            }
            StatementVariant::ShowProcessorState => {
                let processor = match self.entropy.byte() % 4 {
                    0 => StateInspectedProcessor::Reorderer(self.name()),
                    1 => StateInspectedProcessor::Correlator(self.name()),
                    2 => StateInspectedProcessor::Deduplicator(self.name()),
                    _ => StateInspectedProcessor::WindowProcessor(self.name()),
                };
                let clauses = self.state_inspection_clauses();
                Statement::ShowProcessorState(ShowProcessorState {
                    processor,
                    selection: clauses.selection,
                    page: clauses.page,
                    format: clauses.format,
                })
            }
            StatementVariant::ShowWasmProcessorState => {
                let processor = self.name();
                let clauses = self.state_inspection_clauses();
                Statement::ShowWasmProcessorState(ShowWasmProcessorState {
                    processor,
                    selection: clauses.selection,
                    page: clauses.page,
                    format: clauses.format,
                })
            }
            StatementVariant::ShowClusterStatus => Statement::ShowClusterStatus(ShowClusterStatus),
            StatementVariant::ShowTransactions => Statement::ShowTransactions(ShowTransactions),
            StatementVariant::DescribeTransaction => {
                let target = if self.entropy.flag() {
                    TransactionInspectionTarget::Attached
                } else {
                    TransactionInspectionTarget::Transaction {
                        transaction_id: self.non_empty_string(),
                    }
                };
                let operation = if self.entropy.flag() {
                    let number = self.entropy.boundary_biased(1..=u64::MAX);
                    let number =
                        usize::try_from(number).assured("supported targets address 64 bits");
                    Some(TransactionOperationNumber::new(
                        NonZeroUsize::new(number).verified("the range above starts at one"),
                    ))
                } else {
                    None
                };
                Statement::DescribeTransaction(DescribeTransaction {
                    request: TransactionInspectionRequest { target, operation },
                    format: self.inspection_format(),
                })
            }
            StatementVariant::DescribeCluster => Statement::DescribeCluster(DescribeCluster),
            StatementVariant::DescribeLicense => Statement::DescribeLicense(DescribeLicense),
            StatementVariant::ActivateLicense => {
                Statement::ActivateLicense(nervix_models::ActivateLicense {
                    kind: if self.entropy().flag() {
                        nervix_models::ActivationKind::Persistent
                    } else {
                        nervix_models::ActivationKind::Ephemeral
                    },
                    license_key: self.non_empty_string(),
                })
            }
            StatementVariant::RefreshLicense => {
                Statement::RefreshLicense(nervix_models::RefreshLicense)
            }
        }
    }

    /// One or more operations built by `operation`, in the order an `ALTER` writes them.
    fn operations<T>(&mut self, mut operation: impl FnMut(&mut Self) -> T) -> Vec<T> {
        let count = self
            .entropy
            .positive_count(NonZeroUsize::new(ITEMS).assured("an ALTER applies an operation"));
        let mut operations = Vec::with_capacity(count);
        for _ in 0..count {
            operations.push(operation(self));
        }
        operations
    }

    /// The clauses every state inspection statement takes after its head, drawn in their order.
    fn state_inspection_clauses(&mut self) -> StateInspectionClauses {
        let selection = self.entropy.flag().then(|| self.state_branch_selection());
        let page = if self.entropy.flag() {
            Some(nervix_models::StateInspectionPageRequest {
                limit: self.positive_u64(),
                after: self.entropy.flag().then(|| self.non_empty_string()),
            })
        } else {
            None
        };
        let format = if self.entropy.flag() {
            InspectionFormat::Text
        } else {
            InspectionFormat::Json
        };
        StateInspectionClauses {
            selection,
            page,
            format,
        }
    }

    fn state_branch_selection(&mut self) -> StateBranchSelection {
        match self.entropy.byte() % 3 {
            0 => StateBranchSelection::Unbranched,
            1 => StateBranchSelection::AllBranches,
            _ => {
                let names = self.distinct_names::<FieldName>(1, ITEMS);
                let mut fields = Vec::with_capacity(names.len());
                for name in names {
                    fields.push(StateBranchField {
                        name,
                        value: self.branch_value(),
                    });
                }
                StateBranchSelection::Branch(fields)
            }
        }
    }

    pub(crate) fn placement_policy(&mut self) -> PlacementPolicy {
        self.entropy.pick([
            PlacementPolicy::RequireColocation,
            PlacementPolicy::PreferColocation,
            PlacementPolicy::Neutral,
            PlacementPolicy::SuggestSeparation,
        ])
    }

    /// A positive finite time rate: any bit pattern that is one, and the values around one that a
    /// simulation typically runs at.
    pub(crate) fn time_rate(&mut self) -> DomainTimeRate {
        let candidate = match self.entropy.byte() % 6 {
            0 => 1.0,
            1 => 0.5,
            2 => 1000.0,
            3 => f64::MIN_POSITIVE,
            4 => f64::MAX,
            _ => f64::from_bits(self.entropy.any_u64()),
        };
        let magnitude = candidate.abs();
        let rate = if magnitude.is_finite() && magnitude > 0.0 {
            magnitude
        } else {
            1.0
        };
        DomainTimeRate::try_from(rate).verified("the rate above is positive and finite")
    }

    /// A value a branch key field is selected by: a string, a boolean, or a signed number. Unlike
    /// an expression, a branch value spells a negative number, negative zero included, and never
    /// holds a null.
    fn branch_value(&mut self) -> nervix_models::Literal {
        match self.entropy.byte() % 4 {
            0 => nervix_models::Literal::String(self.string()),
            1 => nervix_models::Literal::Bool(self.entropy.flag()),
            2 => nervix_models::Literal::I64(self.entropy.any_i64()),
            _ => {
                let value = f64::from_bits(self.entropy.any_u64());
                let value = if value.is_finite() { value } else { -0.0 };
                nervix_models::Literal::F64(nervix_models::Float64Literal::new(value))
            }
        }
    }

    fn inspection_format(&mut self) -> InspectionFormat {
        self.entropy
            .pick([InspectionFormat::Text, InspectionFormat::Json])
    }

    /// Any kind of Model.
    pub fn model_kind(&mut self) -> ModelKind {
        let kinds = ModelKind::iter().collect::<Vec<_>>();
        let count = NonZeroUsize::new(kinds.len()).assured("there are Model kinds");
        kinds[self.entropy.index(count)]
    }

    /// A kind-qualified reference to a Model that binds a resource version.
    pub fn resource_binding_ref(&mut self) -> NodeRef {
        let kind = self.entropy.pick(RESOURCE_BINDING_KINDS);
        NodeRef::new(kind, self.model_name_of(kind))
    }

    /// The name of a Model of `kind`. Plexus reads a relay's name wherever it names a relay, so in
    /// the Plexus domain the name of a relay avoids the words Plexus refuses for one.
    fn model_name_of(&mut self, kind: ModelKind) -> ModelName {
        match kind {
            ModelKind::Relay => self.name_refusing::<ModelName>(RelayName::REFUSED_IN_PLEXUS),
            _ => self.name(),
        }
    }

    /// A cluster node's name, which a statement writes as a hostname. Its parts stay short enough
    /// that the whole hostname fits the name bound.
    pub fn cluster_node(&mut self) -> nervix_models::ClusterNodeName {
        let hostname = self.hostname_within(20);
        hostname
            .parse()
            .assured("a generated hostname is a valid cluster node name")
    }

    fn relocation_member(&mut self) -> RelocationMember {
        let kind = self.entropy.pick(RELOCATABLE_KINDS);
        let mut member = RelocationMember::new(kind, self.model_name_of(kind));
        member.selection = match self.entropy.byte() % 3 {
            0 => nervix_models::RelocationUnitSelection::All,
            1 => nervix_models::RelocationUnitSelection::Partition(u32::from_le_bytes(
                self.entropy.any_u64().to_le_bytes()[..4]
                    .try_into()
                    .verified("four bytes were selected"),
            )),
            _ => nervix_models::RelocationUnitSelection::Instance(self.entropy.any_u64()),
        };
        member
    }

    /// The kind a `DROP` names: one Plexus spells, or in the vocabulary domain any kind at all.
    fn drop_kind(&mut self) -> ModelKind {
        match self.domain {
            Domain::Plexus => self.entropy.pick(DROPPABLE_KINDS),
            Domain::Vocabulary => {
                let kinds = ModelKind::iter().collect::<Vec<_>>();
                let count = NonZeroUsize::new(kinds.len()).assured("ModelKind has variants");
                let chosen = self.entropy.index(count);
                kinds[chosen]
            }
        }
    }

    fn relocation_members(&mut self) -> Vec<RelocationMember> {
        let count = self
            .entropy
            .positive_count(NonZeroUsize::new(ITEMS).assured("a selection names a member"));
        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            members.push(self.relocation_member());
        }
        members
    }

    fn preference_strategy(&mut self) -> RelocationPreferenceStrategy {
        self.entropy.pick([
            RelocationPreferenceStrategy::Follow,
            RelocationPreferenceStrategy::Ignore,
        ])
    }

    fn relocation(&mut self) -> Relocation {
        let selection = if self.entropy.flag() {
            RelocationSelection::List(self.relocation_members())
        } else {
            RelocationSelection::Corridor {
                from: self
                    .relocation_members()
                    .into_iter()
                    .map(|member| member.node)
                    .collect(),
                to: self
                    .relocation_members()
                    .into_iter()
                    .map(|member| member.node)
                    .collect(),
            }
        };
        let count = self.entropy.count(ITEMS);
        let mut overrides = Vec::with_capacity(count);
        for _ in 0..count {
            overrides.push(RelocationPreferenceOverride {
                member: self.relocation_member(),
                strategy: self.preference_strategy(),
            });
        }
        Relocation {
            selection,
            destination: self.cluster_node(),
            strategy: self.preference_strategy(),
            overrides,
        }
    }

    /// A literal a subscription binding or a lookup key compares against.
    fn subscription_literal(&mut self) -> SubscriptionLiteral {
        match self.entropy.byte() % 3 {
            0 => SubscriptionLiteral::String(self.string()),
            1 => SubscriptionLiteral::Number(self.entropy.any_u64().to_string()),
            _ => SubscriptionLiteral::Bool(self.entropy.flag()),
        }
    }

    fn key_expression_list(&mut self) -> Vec<nervix_models::Expression> {
        let count = self
            .entropy
            .positive_count(NonZeroUsize::new(ITEMS).assured("a key holds an expression"));
        let mut expressions = Vec::with_capacity(count);
        for _ in 0..count {
            expressions.push(self.expression());
        }
        expressions
    }

    fn alter_schema_operation(&mut self) -> AlterSchemaOperation {
        match self.entropy.byte() % 6 {
            0 => AlterSchemaOperation::AddField {
                field: self
                    .schema_fields(1)
                    .into_iter()
                    .next()
                    .assured("one field was asked for"),
            },
            1 => AlterSchemaOperation::DropField { field: self.name() },
            2 => AlterSchemaOperation::RenameField {
                field: self.name(),
                to: self.name(),
            },
            3 => AlterSchemaOperation::SetFieldType {
                field: self.name(),
                ty: self.declared_type(),
            },
            4 => AlterSchemaOperation::SetFieldOptional {
                field: self.name(),
                optional: self.entropy.flag(),
            },
            _ => AlterSchemaOperation::SetFieldSensitive {
                field: self.name(),
                sensitive: self.entropy.flag(),
            },
        }
    }

    fn alter_json_wire_schema_operation(
        &mut self,
    ) -> AlterWireSchemaOperation<nervix_models::JsonType> {
        let schema = self.json_wire_schema();
        self.alter_wire_schema_operation(schema)
    }

    fn alter_cbor_wire_schema_operation(
        &mut self,
    ) -> AlterWireSchemaOperation<nervix_models::CborType> {
        let schema = self.cbor_wire_schema();
        self.alter_wire_schema_operation(schema)
    }

    fn alter_avro_wire_schema_operation(
        &mut self,
    ) -> AlterWireSchemaOperation<nervix_models::AvroType> {
        let schema = self.avro_wire_schema();
        self.alter_wire_schema_operation(schema)
    }

    fn alter_wire_schema_operation<T>(
        &mut self,
        schema: nervix_models::CreateWireSchema<T>,
    ) -> AlterWireSchemaOperation<T> {
        let field = schema
            .fields
            .into_iter()
            .next()
            .assured("a wire schema declares a field");
        match self.entropy.byte() % 6 {
            0 => AlterWireSchemaOperation::SetMode {
                mode: schema.strictness,
            },
            1 => AlterWireSchemaOperation::AddField { field },
            2 => AlterWireSchemaOperation::DropField { field: self.name() },
            3 => AlterWireSchemaOperation::RenameField {
                field: self.name(),
                to: self.name(),
            },
            4 => AlterWireSchemaOperation::SetFieldType {
                field: self.name(),
                ty: field.ty,
            },
            _ => AlterWireSchemaOperation::SetFieldOptional {
                field: self.name(),
                optional: self.entropy.flag(),
            },
        }
    }

    fn alter_relay_operation(&mut self) -> AlterRelayOperation {
        let relay = self.create_relay();
        match self.entropy.byte() % 5 {
            0 => AlterRelayOperation::SetCapacity {
                capacity: relay.buffer,
            },
            1 => AlterRelayOperation::SetSchema {
                schema: relay.schema,
            },
            2 => AlterRelayOperation::SetBranching {
                branching: if self.entropy.flag() {
                    RelayBranching::BranchedBy {
                        branch: self.name(),
                    }
                } else {
                    RelayBranching::Unbranched
                },
            },
            3 => AlterRelayOperation::SetMaterializedState,
            _ => AlterRelayOperation::DropMaterializedState,
        }
    }

    /// An operation every processor `ALTER` shares. `branching` says whether the processor
    /// declares its branch node-wide, which a reingestor, branching per route, does not.
    fn alter_processor_operation(
        &mut self,
        shape: RouteShape,
        branching: bool,
    ) -> AlterProcessorOperation {
        let route_branch = if branching {
            RouteBranch::NodeWide
        } else {
            RouteBranch::PerRoute
        };
        let choices = if branching { 16 } else { 15 };
        match self.entropy.byte() % choices {
            0 => AlterProcessorOperation::AddFrom {
                relay: self.name(),
                where_clause: self.optional_expression(),
            },
            1 => AlterProcessorOperation::DropFrom { relay: self.name() },
            2 => AlterProcessorOperation::AlterFromSetWhere {
                relay: self.name(),
                where_clause: self.expression(),
            },
            3 => AlterProcessorOperation::AlterFromDropWhere { relay: self.name() },
            4 => AlterProcessorOperation::SetCollect {
                policy: self.collect_policy(),
            },
            5 => AlterProcessorOperation::DropCollect,
            6 => AlterProcessorOperation::SetFilterWhere {
                where_clause: self.expression(),
            },
            7 => AlterProcessorOperation::DropFilterWhere,
            8 => AlterProcessorOperation::SetMode {
                mode: self.ack_mode(),
            },
            9 => {
                let relay = self.name();
                AlterProcessorOperation::AddMaterializedState {
                    dependency: self.materialized_dependency(relay),
                }
            }
            10 => AlterProcessorOperation::DropMaterializedState { relay: self.name() },
            11 => {
                let relay = self.name();
                let dependency = self.materialized_dependency(relay);
                AlterProcessorOperation::AlterMaterializedState {
                    relay: dependency.relay,
                    policy: dependency.policy,
                }
            }
            12 => AlterProcessorOperation::AddRoute {
                route: self.single_route(shape, RouteFlush::Required, route_branch),
            },
            13 => AlterProcessorOperation::DropRoute { relay: self.name() },
            14 => AlterProcessorOperation::ReplaceRoute {
                route: self.single_route(shape, RouteFlush::Required, route_branch),
            },
            _ => AlterProcessorOperation::SetBranching {
                branching: self.branch_selection(),
            },
        }
    }

    fn single_route(
        &mut self,
        shape: RouteShape,
        flush: RouteFlush,
        branch: RouteBranch,
    ) -> nervix_models::ProcessorOutput {
        self.processor_outputs(shape, flush, branch)
            .routes
            .into_iter()
            .next()
            .assured("a route list holds at least one route")
    }

    /// The operations an `ALTER` of an emitter of `kind` may write: every shared one, and those its
    /// entity's reference, body, batching and envelope admit.
    fn emitter_operation_forms(&self, kind: EmitterEntityKind) -> Vec<EmitterOperationForm> {
        let mut forms = vec![
            EmitterOperationForm::AddFrom,
            EmitterOperationForm::DropFrom,
            EmitterOperationForm::SetWhere,
            EmitterOperationForm::DropWhere,
            EmitterOperationForm::SetTo,
            EmitterOperationForm::Change,
            EmitterOperationForm::Construction,
            EmitterOperationForm::SetCollect,
            EmitterOperationForm::DropCollect,
            EmitterOperationForm::SetAttachment,
            EmitterOperationForm::SetFlush,
            EmitterOperationForm::SetGeneralError,
        ];
        let capabilities = kind.capabilities();
        if let EmitterReferenceKind::Client(_) | EmitterReferenceKind::StorageClient(_) =
            capabilities.reference
        {
            forms.push(EmitterOperationForm::SetClient);
        }
        if let EmitterBodyForm::Codec | EmitterBodyForm::CodecOrWithoutBody = capabilities.body {
            forms.extend([
                EmitterOperationForm::SetEncode,
                EmitterOperationForm::DropEncode,
            ]);
        }
        if !matches!(capabilities.batch, EmitterBatchLegality::Unavailable { .. }) {
            forms.extend([
                EmitterOperationForm::SetBatch,
                EmitterOperationForm::DropBatch,
            ]);
        }
        let plexus = self.domain == Domain::Plexus;
        if !plexus || !capabilities.envelope.is_empty() || !capabilities.properties.is_empty() {
            forms.push(EmitterOperationForm::Envelope);
        }
        if !plexus || capabilities.instancing.refusal(kind).is_none() {
            forms.push(EmitterOperationForm::SetInstances);
        }
        forms
    }

    /// The construction an emitter of `kind` writes, as a statement can change it in place: a
    /// filter alone where its entity writes direct values, and every part otherwise.
    fn emitter_construction_shape(&self, kind: EmitterEntityKind) -> RouteShape {
        if self.domain == Domain::Plexus && kind.capabilities().body == EmitterBodyForm::Values {
            RouteShape::FilterOnly
        } else {
            RouteShape::Transforming
        }
    }

    fn alter_emitter_operation(&mut self, kind: EmitterEntityKind) -> AlterEmitterOperation {
        let forms = self.emitter_operation_forms(kind);
        let count = NonZeroUsize::new(forms.len())
            .assured("every entity accepts the shared input and attachment operations");
        match forms[self.entropy.index(count)] {
            EmitterOperationForm::AddFrom => AlterEmitterOperation::AddFrom {
                relay: self.name(),
                where_clause: self.optional_expression(),
            },
            EmitterOperationForm::DropFrom => {
                AlterEmitterOperation::DropFrom { relay: self.name() }
            }
            EmitterOperationForm::SetWhere => AlterEmitterOperation::AlterFromSetWhere {
                relay: self.name(),
                where_clause: self.expression(),
            },
            EmitterOperationForm::DropWhere => {
                AlterEmitterOperation::AlterFromDropWhere { relay: self.name() }
            }
            EmitterOperationForm::SetTo => self.set_to(kind),
            EmitterOperationForm::Change => AlterEmitterOperation::Change {
                change: self.emitter_change(kind),
            },
            EmitterOperationForm::SetClient => AlterEmitterOperation::SetClient {
                client: self.name(),
            },
            EmitterOperationForm::SetEncode => {
                AlterEmitterOperation::SetEncodeUsing { codec: self.name() }
            }
            EmitterOperationForm::DropEncode => AlterEmitterOperation::DropEncode,
            EmitterOperationForm::Construction => {
                let shape = self.emitter_construction_shape(kind);
                AlterEmitterOperation::Construction {
                    alteration: self.construction_alteration(shape),
                }
            }
            EmitterOperationForm::SetCollect => AlterEmitterOperation::SetCollect {
                policy: self.collect_policy(),
            },
            EmitterOperationForm::DropCollect => AlterEmitterOperation::DropCollect,
            EmitterOperationForm::SetAttachment => AlterEmitterOperation::SetAttachment {
                mode: self.ack_mode(),
            },
            EmitterOperationForm::SetBatch => AlterEmitterOperation::SetBatch {
                policy: self.batch_policy(),
            },
            EmitterOperationForm::DropBatch => AlterEmitterOperation::DropBatch,
            EmitterOperationForm::SetFlush => AlterEmitterOperation::SetFlush {
                flush_policy: self.flush_policy(),
            },
            EmitterOperationForm::SetGeneralError => AlterEmitterOperation::SetGeneralError {
                policy: self.general_error_policy(),
            },
            EmitterOperationForm::Envelope => AlterEmitterOperation::AlterEnvelope {
                alteration: Box::new(self.envelope_alteration(kind)),
            },
            EmitterOperationForm::SetInstances => AlterEmitterOperation::SetInstances {
                instances: self.emitter_instances_of(kind),
            },
        }
    }

    /// One targeted change to the envelope of an emitter of `kind`: a clause or the properties its
    /// entity declares in Plexus, and any clause or property in the vocabulary.
    fn envelope_alteration(&mut self, kind: EmitterEntityKind) -> EmitterEnvelopeAlteration {
        let plexus = self.domain == Domain::Plexus;
        let capabilities = kind.capabilities();
        let mut forms = Vec::new();
        if !plexus || !capabilities.envelope.is_empty() {
            forms.extend([
                EnvelopeOperationForm::SetClause,
                EnvelopeOperationForm::DropClause,
            ]);
        }
        if !plexus || !capabilities.properties.is_empty() {
            forms.extend([
                EnvelopeOperationForm::SetProperties,
                EnvelopeOperationForm::DropProperties,
            ]);
        }
        let count = NonZeroUsize::new(forms.len()).verified(OFFERED_ENVELOPE_IS_DECLARED);
        match forms[self.entropy.index(count)] {
            EnvelopeOperationForm::SetClause => EmitterEnvelopeAlteration::SetClause {
                clause: self
                    .envelope_clause_value(kind)
                    .verified(OFFERED_ENVELOPE_IS_DECLARED),
            },
            EnvelopeOperationForm::DropClause => EmitterEnvelopeAlteration::DropClause {
                clause: self
                    .envelope_clause_kind(kind)
                    .verified(OFFERED_ENVELOPE_IS_DECLARED),
            },
            EnvelopeOperationForm::SetProperties => EmitterEnvelopeAlteration::SetProperties {
                properties: self
                    .envelope_properties(kind)
                    .verified(OFFERED_ENVELOPE_IS_DECLARED),
            },
            EnvelopeOperationForm::DropProperties => EmitterEnvelopeAlteration::DropProperties,
        }
    }

    /// `SET TO` with the complete body of an emitter of `kind`, its construction written where the
    /// body has one.
    fn set_to(&mut self, kind: EmitterEntityKind) -> AlterEmitterOperation {
        let emitter = self.create_emitter_to(kind);
        let construction = if emitter.construction == nervix_models::RouteConstruction::default() {
            None
        } else {
            Some(emitter.construction)
        };
        AlterEmitterOperation::SetTo {
            destination: emitter.connector,
            body: emitter.body,
            envelope: emitter.envelope,
            construction,
        }
    }

    /// The operations of an `ALTER … INGESTOR` statement under the head of any connector and
    /// entity, each `SET FROM` stating a source of that pair and each change one of its own.
    ///
    /// The match is exhaustive, so a new pair does not compile until the generator is taught to
    /// alter it.
    pub fn ingestor_alteration(&mut self) -> IngestorAlteration {
        match self.ingest_source_kind() {
            IngestorEntityKind::Client(ClientIngestorEntity::Producer) => self
                .operations(|arbitrary| {
                    arbitrary.alter_ingestor_operation(
                        Self::client_producer_source,
                        Self::client_producer_change,
                    )
                })
                .into(),
            IngestorEntityKind::Http(HttpIngestorEntity::Request) => {
                self.transport_operations(Self::http_request_source, Self::http_request_change)
            }
            IngestorEntityKind::Http(HttpIngestorEntity::Endpoint) => {
                self.transport_operations(Self::http_endpoint_source, Self::http_endpoint_change)
            }
            IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic) => {
                self.transport_operations(Self::kafka_topic_source, Self::kafka_topic_change)
            }
            IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic) => {
                self.transport_operations(Self::pulsar_topic_source, Self::pulsar_topic_change)
            }
            IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic) => {
                self.transport_operations(Self::mqtt_topic_source, Self::mqtt_topic_change)
            }
            IngestorEntityKind::Nats(NatsIngestorEntity::Subject) => {
                self.transport_operations(Self::nats_subject_source, Self::nats_subject_change)
            }
            IngestorEntityKind::Nats(NatsIngestorEntity::Stream) => {
                self.transport_operations(Self::nats_stream_source, Self::nats_stream_change)
            }
            IngestorEntityKind::RabbitMq(RabbitMqIngestorEntity::Queue) => {
                self.transport_operations(Self::rabbitmq_queue_source, Self::rabbitmq_queue_change)
            }
            IngestorEntityKind::Redis(RedisIngestorEntity::PubSub) => {
                self.transport_operations(Self::redis_pubsub_source, Self::redis_pubsub_change)
            }
            IngestorEntityKind::Prometheus(PrometheusIngestorEntity::Query) => self
                .transport_operations(Self::prometheus_query_source, Self::prometheus_query_change),
            IngestorEntityKind::ZeroMq(ZeroMqIngestorEntity::Pull) => {
                self.transport_operations(Self::zeromq_pull_source, Self::zeromq_pull_change)
            }
            IngestorEntityKind::Sqs(SqsIngestorEntity::Queue) => {
                self.transport_operations(Self::sqs_queue_source, Self::sqs_queue_change)
            }
            IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Connection) => self
                .transport_operations(
                    Self::websockets_connection_source,
                    Self::websockets_connection_change,
                ),
            IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint) => self
                .transport_operations(
                    Self::websockets_endpoint_source,
                    Self::websockets_endpoint_change,
                ),
            IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener) => self
                .transport_operations(Self::syslog_listener_source, Self::syslog_listener_change),
        }
    }

    /// The operations of an alteration of a transport's ingestor, whose `SET FROM` states a source
    /// `source` builds and whose changes `change` builds.
    fn transport_operations<Source: IngestorEntityModel>(
        &mut self,
        source: impl Fn(&mut Self) -> Source,
        change: impl Fn(&mut Self) -> Source::Change,
    ) -> IngestorAlteration
    where
        Vec<AlterTransportIngestorOperation<Source>>: Into<IngestorAlteration>,
    {
        self.operations(|arbitrary| arbitrary.transport_operation(&source, &change))
            .into()
    }

    /// One shared transport operation, or one change of the transport's own entity.
    fn transport_operation<Source: IngestorEntityModel>(
        &mut self,
        source: impl Fn(&mut Self) -> Source,
        change: impl Fn(&mut Self) -> Source::Change,
    ) -> AlterTransportIngestorOperation<Source> {
        if self.entropy.byte() % 14 == 13 {
            AlterTransportIngestorOperation::SetDecodeUsing { codec: self.name() }
        } else {
            AlterTransportIngestorOperation::from(self.alter_ingestor_operation(source, change))
        }
    }

    /// One operation every ingestor accepts, whose `SET FROM` states a source `source` builds and
    /// whose changes `change` builds. Plexus writes `SET CLIENT` only for an entity that reads
    /// through a client.
    fn alter_ingestor_operation<Source: IngestorEntityModel>(
        &mut self,
        source: impl Fn(&mut Self) -> Source,
        change: impl Fn(&mut Self) -> Source::Change,
    ) -> AlterIngestorOperation<Source> {
        let reads_through_client = matches!(
            Source::KIND.capabilities().reference,
            IngestorReferenceKind::Client(_)
        );
        match self.entropy.byte() % 13 {
            0 => AlterIngestorOperation::SetSource {
                source: source(self),
            },
            1 => AlterIngestorOperation::Change {
                change: change(self),
            },
            2 if reads_through_client || self.domain != Domain::Plexus => {
                AlterIngestorOperation::SetClient {
                    client: self.name(),
                }
            }
            3 => AlterIngestorOperation::SetQuiesce {
                quiesce: self.declared_quiesce_mode(Source::KIND),
            },
            4 => AlterIngestorOperation::SetTimestamp {
                source: match self.entropy.byte() % 3 {
                    0 => nervix_models::IngestTimestampSource::Now,
                    1 => nervix_models::IngestTimestampSource::At(self.name()),
                    _ => nervix_models::IngestTimestampSource::AtMetadata(self.name()),
                },
            },
            5 => AlterIngestorOperation::DropTimestamp,
            6 => AlterIngestorOperation::SetFilterWhere {
                where_clause: self.expression(),
            },
            7 => AlterIngestorOperation::DropFilterWhere,
            8 => AlterIngestorOperation::AddRoute {
                route: self.single_route(
                    RouteShape::Transforming,
                    RouteFlush::Required,
                    RouteBranch::PerRoute,
                ),
            },
            9 => AlterIngestorOperation::DropRoute { relay: self.name() },
            10 => AlterIngestorOperation::ReplaceRoute {
                route: self.single_route(
                    RouteShape::Transforming,
                    RouteFlush::Required,
                    RouteBranch::PerRoute,
                ),
            },
            11 => AlterIngestorOperation::AlterRoute {
                relay: self.name(),
                alteration: self.construction_alteration(RouteShape::Transforming),
            },
            _ => AlterIngestorOperation::SetGeneralError {
                policy: self.general_error_policy(),
            },
        }
    }

    fn alter_generator_operation(&mut self) -> AlterGeneratorOperation {
        match self.entropy.byte() % 6 {
            0 => AlterGeneratorOperation::SetMaterializedState { relay: self.name() },
            1 => AlterGeneratorOperation::SetEach {
                each: self.clock_period(),
            },
            2 => AlterGeneratorOperation::SetBranching {
                branching: self.branch_selection(),
            },
            3 => AlterGeneratorOperation::AddRoute {
                route: self.single_route(
                    RouteShape::SetOnly,
                    RouteFlush::Required,
                    RouteBranch::NodeWide,
                ),
            },
            4 => AlterGeneratorOperation::DropRoute { relay: self.name() },
            _ => AlterGeneratorOperation::ReplaceRoute {
                route: self.single_route(
                    RouteShape::SetOnly,
                    RouteFlush::Required,
                    RouteBranch::NodeWide,
                ),
            },
        }
    }

    fn alter_placement_operation(&mut self) -> AlterPlacementOperation {
        match self.entropy.byte() % 5 {
            0 => AlterPlacementOperation::SetPolicy {
                policy: self.placement_policy(),
            },
            1 => AlterPlacementOperation::SetRank {
                rank: self.positive_u64(),
            },
            2 => AlterPlacementOperation::DropRank,
            3 => {
                let placement = self.create_placement();
                match placement.scope {
                    nervix_models::PlacementScope::Corridor { from, to } => {
                        AlterPlacementOperation::SetMembers { from, to }
                    }
                    nervix_models::PlacementScope::DomainWide => {
                        AlterPlacementOperation::SetDomainWide
                    }
                }
            }
            _ => AlterPlacementOperation::RenameTo { name: self.name() },
        }
    }
}
