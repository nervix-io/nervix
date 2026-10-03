//! Every statement form: Model creation, domain lifecycle, administration, every `ALTER`, and the
//! read-only queries.

use std::num::NonZeroUsize;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AlterDeduplicator, AlterDeduplicatorOperation, AlterDomain, AlterEmitter,
    AlterEmitterOperation, AlterGenerator, AlterGeneratorOperation, AlterIngestor,
    AlterIngestorOperation, AlterJunction, AlterPlacement, AlterPlacementOperation,
    AlterProcessorOperation, AlterReingestor, AlterRelay, AlterRelayOperation, AlterReorderer,
    AlterReordererOperation, AlterSchema, AlterSchemaOperation, AlterWireSchema,
    AlterWireSchemaOperation, Backup, BackupResources, BackupScope, CordonNode, CreateDomain,
    CreateResource, CreateStatement, CreateUser, DescribeCorrelator, DescribeDeduplicator,
    DescribeDomain, DescribeEmitter, DescribeEndpoint, DescribeIngestor, DescribeJunction,
    DescribeLookup, DescribePlacement, DescribeReingestor, DescribeRelay, DescribeReorderer,
    DescribeResource, DescribeTransaction, DescribeUdf, DescribeWasmProcessor,
    DescribeWindowProcessor, DomainConfig, DomainPace, DomainStartPoint, DomainTimeRate, DrainNode,
    DropModel, DropNode, EmitterPublishingMode, ExistingUserPolicy, FieldName, InspectionFormat,
    LookupQuery, ModelKind, ModelName, NodeRef, PlacementPolicy, RebindResource,
    RebindResourceMembers, RebindResourceSelection, RelayBranching, Relocation, RelocationMember,
    RelocationPreferenceOverride, RelocationPreferenceStrategy, RelocationSelection,
    ResetWasmBranchField, ResetWasmState, ResetWasmStateScope, Restore, RestoreMode, RestoreScope,
    ShowClusterStatus, ShowCreate, ShowIngestors, ShowPlacements, ShowRelayMaterializedState,
    ShowTransactions, ShowUdfs, StartDomain, Statement, StopDomain, SubscriptionBinding,
    SubscriptionLiteral, Timestamp, TransactionInspectionRequest, TransactionInspectionTarget,
    TransactionOperationNumber, UncordonNode, UploadResource,
};
use strum::IntoEnumIterator as _;

use crate::{
    Arbitrary, Domain,
    route::{RouteBranch, RouteFlush, RouteShape},
};

/// The most items a generated operation list, member list or binding list holds.
const ITEMS: usize = 3;

/// The kinds NSPL's `DROP` names. It has no `DROP` for a branch, generator, lookup, signaling
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

/// Every variant of [`Statement`], so a property reaches each form as often as any other and a
/// coverage check can ask for each one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum StatementVariant {
    CreateDomain,
    AlterDomain,
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
    ShowPlacements,
    ShowRelayMaterializedState,
    ShowClusterStatus,
    ShowTransactions,
    DescribeTransaction,
}

impl StatementVariant {
    /// The variant `statement` is. The match is exhaustive, so a new statement form does not
    /// compile until the generator is taught to build it.
    pub fn of(statement: &Statement) -> Self {
        match statement {
            Statement::CreateDomain(_) => Self::CreateDomain,
            Statement::AlterDomain(_) => Self::AlterDomain,
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
            Statement::ShowPlacements(_) => Self::ShowPlacements,
            Statement::ShowRelayMaterializedState(_) => Self::ShowRelayMaterializedState,
            Statement::ShowClusterStatus(_) => Self::ShowClusterStatus,
            Statement::ShowTransactions(_) => Self::ShowTransactions,
            Statement::DescribeTransaction(_) => Self::DescribeTransaction,
        }
    }
}

impl Arbitrary<'_> {
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
                        config: DomainConfig {
                            pace,
                            placement: self.placement_policy(),
                        },
                    },
                    if_not_exists,
                ))
            }
            StatementVariant::AlterDomain => Statement::AlterDomain(AlterDomain {
                policy: self.placement_policy(),
            }),
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
                let scope = match self.entropy.byte() % 3 {
                    0 => ResetWasmStateScope::Unbranched,
                    1 => ResetWasmStateScope::AllBranches,
                    _ => {
                        let names = self.distinct_names::<FieldName>(1, ITEMS);
                        let mut fields = Vec::with_capacity(names.len());
                        for name in names {
                            fields.push(ResetWasmBranchField {
                                name,
                                value: self.branch_value(),
                            });
                        }
                        ResetWasmStateScope::Branch(fields)
                    }
                };
                Statement::ResetWasmState(ResetWasmState {
                    domain: self.name(),
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
                Statement::Backup(Backup {
                    scope,
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
                })
            }
            StatementVariant::StartDomain => {
                let start = match self.entropy.byte() % 3 {
                    0 => DomainStartPoint::Resume,
                    1 => DomainStartPoint::Now {
                        time_rate: self.time_rate(),
                    },
                    _ => DomainStartPoint::At {
                        timestamp: Timestamp::from_unix_nanos(self.entropy.any_i64()),
                        time_rate: self.time_rate(),
                    },
                };
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
                let operations = self.operations(Self::alter_json_wire_schema_operation);
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
                let operations = self.operations(Self::alter_emitter_operation);
                Statement::AlterEmitter(AlterEmitter {
                    emitter: self.name(),
                    operations,
                })
            }
            StatementVariant::AlterIngestor => {
                let operations = self.operations(Self::alter_ingestor_operation);
                Statement::AlterIngestor(AlterIngestor {
                    ingestor: self.name(),
                    operations,
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
            StatementVariant::Drop => Statement::Drop(DropModel {
                kind: self.drop_kind(),
                name: self.name(),
            }),
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
            StatementVariant::DescribeEndpoint => {
                Statement::DescribeEndpoint(DescribeEndpoint { name: self.name() })
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
                Statement::DescribeWasmProcessor(DescribeWasmProcessor {
                    name: self.name(),
                    format: self.inspection_format(),
                })
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
            StatementVariant::ShowCreate => Statement::ShowCreate(ShowCreate {
                kind: self.model_kind(),
                name: self.name(),
            }),
            StatementVariant::ShowUdfs => Statement::ShowUdfs(ShowUdfs),
            StatementVariant::ShowIngestors => Statement::ShowIngestors(ShowIngestors),
            StatementVariant::ShowPlacements => Statement::ShowPlacements(ShowPlacements),
            StatementVariant::ShowRelayMaterializedState => {
                Statement::ShowRelayMaterializedState(ShowRelayMaterializedState {
                    relay: self.name(),
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

    fn placement_policy(&mut self) -> PlacementPolicy {
        self.entropy.pick([
            PlacementPolicy::RequireColocation,
            PlacementPolicy::PreferColocation,
            PlacementPolicy::Neutral,
            PlacementPolicy::SuggestSeparation,
        ])
    }

    /// A positive finite time rate: any bit pattern that is one, and the values around one that a
    /// simulation typically runs at.
    fn time_rate(&mut self) -> DomainTimeRate {
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
        NodeRef::new(kind, self.name::<ModelName>())
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
        RelocationMember::new(self.entropy.pick(RELOCATABLE_KINDS), self.name())
    }

    /// A publishing mode written on its own, without the sink it applies to.
    ///
    /// NSPL spells a client acknowledgement exactly as a broker one, and an emitter applying a
    /// standalone mode to its client sink turns one into the other, so the NSPL domain writes a
    /// client acknowledgement as the broker acknowledgement it reads as.
    fn standalone_publishing_mode(&self, mode: EmitterPublishingMode) -> EmitterPublishingMode {
        match (self.domain, mode) {
            (
                Domain::Nspl,
                EmitterPublishingMode::ClientAck {
                    window,
                    ack_timeout,
                    retry_policy,
                },
            ) => EmitterPublishingMode::BrokerAck {
                window,
                ack_timeout,
                retry_policy,
            },
            (_, mode) => mode,
        }
    }

    /// The kind a `DROP` names: one NSPL spells, or in the vocabulary domain any kind at all.
    fn drop_kind(&mut self) -> ModelKind {
        match self.domain {
            Domain::Nspl => self.entropy.pick(DROPPABLE_KINDS),
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
                from: self.relocation_members(),
                to: self.relocation_members(),
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

    fn alter_avro_wire_schema_operation(
        &mut self,
    ) -> AlterWireSchemaOperation<nervix_models::AvroType> {
        let schema = self.avro_wire_schema();
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

    fn alter_emitter_operation(&mut self) -> AlterEmitterOperation {
        match self.entropy.byte() % 16 {
            0 => AlterEmitterOperation::AddFrom {
                relay: self.name(),
                where_clause: self.optional_expression(),
            },
            1 => AlterEmitterOperation::DropFrom { relay: self.name() },
            2 => AlterEmitterOperation::AlterFromSetWhere {
                relay: self.name(),
                where_clause: self.expression(),
            },
            3 => AlterEmitterOperation::AlterFromDropWhere { relay: self.name() },
            4 => {
                // Moving an emitter to an HTTP sink restates what the request carries, and moving
                // it to a client consumer selects native output; every other sink keeps the
                // emitter's own payload selection.
                let emitter = self.create_emitter();
                let body = match emitter.sink.as_ref() {
                    nervix_models::EmitSink::Http { .. }
                    | nervix_models::EmitSink::Client { .. } => Some(emitter.body),
                    _ => None,
                };
                AlterEmitterOperation::SetSink {
                    sink: emitter.sink,
                    publishing_mode: emitter.publishing_mode,
                    body,
                }
            }
            5 => AlterEmitterOperation::SetClient {
                client: self.name(),
            },
            6 => AlterEmitterOperation::SetEncodeUsing { codec: self.name() },
            7 => AlterEmitterOperation::DropEncode,
            8 => AlterEmitterOperation::SetCollect {
                policy: self.collect_policy(),
            },
            9 => AlterEmitterOperation::DropCollect,
            10 => AlterEmitterOperation::SetAttachment {
                mode: self.ack_mode(),
            },
            11 => {
                let emitter = self.create_emitter();
                AlterEmitterOperation::SetPublishingMode {
                    mode: self.standalone_publishing_mode(emitter.publishing_mode),
                }
            }
            12 => {
                let emitter = self.create_emitter();
                match emitter.batch {
                    Some(policy) => AlterEmitterOperation::SetBatch { policy },
                    None => AlterEmitterOperation::DropBatch,
                }
            }
            13 => AlterEmitterOperation::DropBatch,
            14 => AlterEmitterOperation::SetFlush {
                flush_policy: self.flush_policy(),
            },
            _ => AlterEmitterOperation::SetCommit {
                commit_each: self.duration(),
                max_commit_size: self.byte_size(),
            },
        }
    }

    fn alter_ingestor_operation(&mut self) -> AlterIngestorOperation {
        match self.entropy.byte() % 12 {
            0 => AlterIngestorOperation::SetSource {
                source: self.ingest_source(),
            },
            1 => {
                let ingestor = self.create_ingestor();
                match ingestor.input {
                    nervix_models::IngestorInput::Client(source) => {
                        AlterIngestorOperation::SetClientSource { source }
                    }
                    nervix_models::IngestorInput::Transport(input) => {
                        AlterIngestorOperation::SetSource {
                            source: input.source,
                        }
                    }
                }
            }
            2 => {
                let source = self.ingest_source();
                AlterIngestorOperation::SetQuiesce {
                    quiesce: source.quiesce().clone(),
                }
            }
            3 => AlterIngestorOperation::SetDecodeUsing { codec: self.name() },
            4 => AlterIngestorOperation::SetTimestamp {
                source: if self.entropy.flag() {
                    nervix_models::IngestTimestampSource::Now
                } else {
                    nervix_models::IngestTimestampSource::At(self.name())
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
                AlterPlacementOperation::SetMembers {
                    from: placement.from,
                    to: placement.to,
                }
            }
            _ => AlterPlacementOperation::RenameTo { name: self.name() },
        }
    }
}
