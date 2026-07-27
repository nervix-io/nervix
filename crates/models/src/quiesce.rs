use serde::{Deserialize, Serialize};
use strum::{AsRefStr, IntoStaticStr};

use crate::{
    CreateEmitter, CreateJunction, CreateRelay, CreateSchema, CreateWireSchemaStmt, EmitSink,
    Identifier, MessageErrorPolicy, Model, ProcessorInputs, ProcessorOutput, ProcessorOutputs,
};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    AsRefStr,
    IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum QuiesceLevel {
    Dynamic,
    EntityPause,
    DomainPause,
}

impl QuiesceLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dynamic => "DYNAMIC",
            Self::EntityPause => "ENTITY_PAUSE",
            Self::DomainPause => "DOMAIN_PAUSE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DynamicModelUpdate {
    RelayCapacity {
        relay: Identifier,
        capacity: usize,
    },
    Junction {
        junction: Identifier,
        config: CreateJunction,
    },
    Emitter {
        emitter: Identifier,
        config: CreateEmitter,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelChangeAspect {
    RelayCapacity,
    RelaySchema,
    RelayBranching,
    RelayMaterializedState,
    JunctionFilter,
    JunctionInputWhere,
    JunctionRouteConstruction,
    JunctionRouteFlushPolicy,
    JunctionCollectPolicy,
    JunctionMessageErrorPolicy,
    JunctionErrorRouteTargets,
    JunctionInputs,
    JunctionRoutes,
    JunctionMode,
    JunctionBranching,
    JunctionMaterializedState,
    EmitterInput,
    EmitterSink,
    EmitterClient,
    EmitterCodec,
    EmitterCollectPolicy,
    EmitterMode,
    EmitterFlushPolicy,
    EmitterConstruction,
    EmitterErrorPolicies,
    EmitterMaterializedState,
    SchemaDefinition,
    WireSchemaDefinition,
    EntityReplaced,
    EntityCreated,
    EntityDropped,
}

impl ModelChangeAspect {
    pub const fn quiesce_level(self) -> QuiesceLevel {
        match self {
            Self::RelayCapacity
            | Self::JunctionFilter
            | Self::JunctionInputWhere
            | Self::JunctionRouteConstruction
            | Self::JunctionRouteFlushPolicy
            | Self::JunctionCollectPolicy
            | Self::JunctionMessageErrorPolicy
            | Self::EmitterFlushPolicy
            | Self::EntityReplaced
            | Self::EntityCreated
            | Self::EntityDropped => QuiesceLevel::Dynamic,
            Self::RelayMaterializedState
            | Self::JunctionErrorRouteTargets
            | Self::JunctionInputs
            | Self::JunctionRoutes
            | Self::JunctionMode
            | Self::JunctionBranching
            | Self::JunctionMaterializedState
            | Self::EmitterSink
            | Self::EmitterClient
            | Self::EmitterCodec
            | Self::EmitterCollectPolicy
            | Self::EmitterMode
            | Self::EmitterConstruction
            | Self::EmitterErrorPolicies
            | Self::EmitterMaterializedState => QuiesceLevel::EntityPause,
            Self::RelaySchema
            | Self::RelayBranching
            | Self::EmitterInput
            | Self::SchemaDefinition
            | Self::WireSchemaDefinition => QuiesceLevel::DomainPause,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModelChangeAspects {
    aspects: Vec<ModelChangeAspect>,
    dynamic_updates: Vec<DynamicModelUpdate>,
}

impl ModelChangeAspects {
    pub fn aspects(&self) -> &[ModelChangeAspect] {
        &self.aspects
    }

    pub fn dynamic_updates(&self) -> &[DynamicModelUpdate] {
        &self.dynamic_updates
    }

    pub fn is_empty(&self) -> bool {
        self.aspects.is_empty()
    }

    pub fn quiesce_level(&self) -> QuiesceLevel {
        self.aspects
            .iter()
            .copied()
            .map(ModelChangeAspect::quiesce_level)
            .max()
            .unwrap_or(QuiesceLevel::Dynamic)
    }

    fn push(&mut self, aspect: ModelChangeAspect) {
        if !self.aspects.contains(&aspect) {
            self.aspects.push(aspect);
        }
    }

    fn push_dynamic(&mut self, aspect: ModelChangeAspect, update: DynamicModelUpdate) {
        self.push(aspect);
        if !self.dynamic_updates.contains(&update) {
            self.dynamic_updates.push(update);
        }
    }

    fn replaced() -> Self {
        Self {
            aspects: vec![ModelChangeAspect::EntityReplaced],
            dynamic_updates: Vec::new(),
        }
    }
}

impl Model {
    pub fn change_aspects_against(&self, candidate: &Self) -> ModelChangeAspects {
        if self == candidate {
            return ModelChangeAspects::default();
        }

        match (self, candidate) {
            (Self::Relay(base), Self::Relay(candidate)) => relay_change_aspects(base, candidate),
            (Self::Junction(base), Self::Junction(candidate)) => {
                junction_change_aspects(base, candidate)
            }
            (Self::Emitter(base), Self::Emitter(candidate)) => {
                emitter_change_aspects(base, candidate)
            }
            (Self::Schema(base), Self::Schema(candidate)) => {
                let CreateSchema {
                    name: base_name,
                    fields: base_fields,
                } = base;
                let CreateSchema {
                    name: candidate_name,
                    fields: candidate_fields,
                } = candidate;
                if base_name == candidate_name && base_fields != candidate_fields {
                    ModelChangeAspects {
                        aspects: vec![ModelChangeAspect::SchemaDefinition],
                        dynamic_updates: Vec::new(),
                    }
                } else {
                    ModelChangeAspects::replaced()
                }
            }
            (Self::WireSchema(base), Self::WireSchema(candidate)) => {
                wire_schema_change_aspects(base, candidate)
            }
            (Self::Codec(_), _)
            | (Self::ClientKafka(_), _)
            | (Self::ClientPulsar(_), _)
            | (Self::ClientHttp(_), _)
            | (Self::ClientSentry(_), _)
            | (Self::ClientPrometheus(_), _)
            | (Self::ClientMqtt(_), _)
            | (Self::ClientNats(_), _)
            | (Self::ClientRabbitMq(_), _)
            | (Self::ClientRedis(_), _)
            | (Self::ClientZeroMq(_), _)
            | (Self::ClientSqs(_), _)
            | (Self::ClientWebsockets(_), _)
            | (Self::ClientClickHouse(_), _)
            | (Self::ClientPostgres(_), _)
            | (Self::ClientMySql(_), _)
            | (Self::ClientMongoDb(_), _)
            | (Self::ClientS3(_), _)
            | (Self::ClientGcs(_), _)
            | (Self::ClientAzureBlob(_), _)
            | (Self::ClientIcebergRest(_), _)
            | (Self::Vhost(_), _)
            | (Self::Branch(_), _)
            | (Self::Endpoint(_), _)
            | (Self::SignalingProtocol(_), _)
            | (Self::Generator(_), _)
            | (Self::Inferencer(_), _)
            | (Self::WasmProcessor(_), _)
            | (Self::Ingestor(_), _)
            | (Self::Reingestor(_), _)
            | (Self::Materializer(_), _)
            | (Self::Lookup(_), _)
            | (Self::Deduplicator(_), _)
            | (Self::Correlator(_), _)
            | (Self::Reorderer(_), _)
            | (Self::WindowProcessor(_), _)
            | (Self::Emitter(_), _)
            | (Self::Udf(_), _)
            | (Self::Relay(_), _)
            | (Self::Junction(_), _)
            | (Self::Schema(_), _)
            | (Self::WireSchema(_), _) => ModelChangeAspects::replaced(),
        }
    }
}

fn relay_change_aspects(base: &CreateRelay, candidate: &CreateRelay) -> ModelChangeAspects {
    let CreateRelay {
        name: base_name,
        schema: base_schema,
        buffer: base_buffer,
        branching: base_branching,
        materialized_state: base_materialized_state,
    } = base;
    let CreateRelay {
        name: candidate_name,
        schema: candidate_schema,
        buffer: candidate_buffer,
        branching: candidate_branching,
        materialized_state: candidate_materialized_state,
    } = candidate;

    if base_name != candidate_name {
        return ModelChangeAspects::replaced();
    }

    let mut changes = ModelChangeAspects::default();
    if base_buffer != candidate_buffer {
        changes.push_dynamic(
            ModelChangeAspect::RelayCapacity,
            DynamicModelUpdate::RelayCapacity {
                relay: candidate_name.clone(),
                capacity: *candidate_buffer,
            },
        );
    }
    if base_schema != candidate_schema {
        changes.push(ModelChangeAspect::RelaySchema);
    }
    if base_branching != candidate_branching {
        changes.push(ModelChangeAspect::RelayBranching);
    }
    if base_materialized_state != candidate_materialized_state {
        changes.push(ModelChangeAspect::RelayMaterializedState);
    }
    changes
}

fn junction_change_aspects(
    base: &CreateJunction,
    candidate: &CreateJunction,
) -> ModelChangeAspects {
    let CreateJunction {
        name: base_name,
        from: base_from,
        output_routes: base_outputs,
        branched_by: base_branching,
        mode: base_mode,
        filter_where: base_filter,
        materialized_state: base_materialized_state,
    } = base;
    let CreateJunction {
        name: candidate_name,
        from: candidate_from,
        output_routes: candidate_outputs,
        branched_by: candidate_branching,
        mode: candidate_mode,
        filter_where: candidate_filter,
        materialized_state: candidate_materialized_state,
    } = candidate;

    if base_name != candidate_name {
        return ModelChangeAspects::replaced();
    }

    let mut changes = ModelChangeAspects::default();
    let mut has_dynamic_change = false;
    if base_filter != candidate_filter {
        changes.push(ModelChangeAspect::JunctionFilter);
        has_dynamic_change = true;
    }
    compare_junction_inputs(
        base_from,
        candidate_from,
        &mut changes,
        &mut has_dynamic_change,
    );
    compare_junction_outputs(
        base_outputs,
        candidate_outputs,
        &mut changes,
        &mut has_dynamic_change,
    );
    if base_branching != candidate_branching {
        changes.push(ModelChangeAspect::JunctionBranching);
    }
    if base_mode != candidate_mode {
        changes.push(ModelChangeAspect::JunctionMode);
    }
    if base_materialized_state != candidate_materialized_state {
        changes.push(ModelChangeAspect::JunctionMaterializedState);
    }
    if has_dynamic_change {
        changes.dynamic_updates.push(DynamicModelUpdate::Junction {
            junction: candidate_name.clone(),
            config: candidate.clone(),
        });
    }
    changes
}

fn compare_junction_inputs(
    base: &ProcessorInputs,
    candidate: &ProcessorInputs,
    changes: &mut ModelChangeAspects,
    has_dynamic_change: &mut bool,
) {
    let ProcessorInputs {
        from: base_relays,
        r#where: base_where,
        collect_policy: base_collect,
    } = base;
    let ProcessorInputs {
        from: candidate_relays,
        r#where: candidate_where,
        collect_policy: candidate_collect,
    } = candidate;

    if base_relays != candidate_relays {
        changes.push(ModelChangeAspect::JunctionInputs);
    }
    if base_where != candidate_where {
        changes.push(ModelChangeAspect::JunctionInputWhere);
        *has_dynamic_change = true;
    }
    if base_collect != candidate_collect {
        changes.push(ModelChangeAspect::JunctionCollectPolicy);
        *has_dynamic_change = true;
    }
}

fn compare_junction_outputs(
    base: &ProcessorOutputs,
    candidate: &ProcessorOutputs,
    changes: &mut ModelChangeAspects,
    has_dynamic_change: &mut bool,
) {
    let ProcessorOutputs {
        routes: base_routes,
    } = base;
    let ProcessorOutputs {
        routes: candidate_routes,
    } = candidate;
    let same_route_shape = base_routes.len() == candidate_routes.len()
        && base_routes
            .iter()
            .zip(candidate_routes)
            .all(|(base, candidate)| base.relay == candidate.relay);
    if !same_route_shape {
        changes.push(ModelChangeAspect::JunctionRoutes);
        return;
    }
    if base_routes != candidate_routes && routes_are_permutation(base_routes, candidate_routes) {
        changes.push(ModelChangeAspect::JunctionRoutes);
        return;
    }

    if message_error_targets(base_routes) != message_error_targets(candidate_routes) {
        changes.push(ModelChangeAspect::JunctionErrorRouteTargets);
    }

    for (base, candidate) in base_routes.iter().zip(candidate_routes) {
        let ProcessorOutput {
            relay: base_relay,
            construction: base_construction,
            flush_policy: base_flush,
            message_error_policy: base_message_error,
            branch: base_branch,
        } = base;
        let ProcessorOutput {
            relay: candidate_relay,
            construction: candidate_construction,
            flush_policy: candidate_flush,
            message_error_policy: candidate_message_error,
            branch: candidate_branch,
        } = candidate;
        debug_assert_eq!(base_relay, candidate_relay);
        if base_branch != candidate_branch {
            changes.push(ModelChangeAspect::JunctionRoutes);
        }
        if base_construction != candidate_construction {
            changes.push(ModelChangeAspect::JunctionRouteConstruction);
            *has_dynamic_change = true;
        }
        if base_flush != candidate_flush {
            changes.push(ModelChangeAspect::JunctionRouteFlushPolicy);
            *has_dynamic_change = true;
        }
        if base_message_error != candidate_message_error
            && !changes
                .aspects
                .contains(&ModelChangeAspect::JunctionErrorRouteTargets)
        {
            changes.push(ModelChangeAspect::JunctionMessageErrorPolicy);
            *has_dynamic_change = true;
        }
    }
}

fn routes_are_permutation(base: &[ProcessorOutput], candidate: &[ProcessorOutput]) -> bool {
    if base.len() != candidate.len() {
        return false;
    }
    let mut matched = vec![false; candidate.len()];
    for base_route in base {
        let Some((index, _)) = candidate
            .iter()
            .enumerate()
            .find(|(index, candidate_route)| !matched[*index] && *candidate_route == base_route)
        else {
            return false;
        };
        matched[index] = true;
    }
    true
}

fn message_error_targets(routes: &[ProcessorOutput]) -> Vec<&Identifier> {
    let mut targets = routes
        .iter()
        .filter_map(|route| match &route.message_error_policy {
            MessageErrorPolicy::Dlq { relay, .. } => Some(relay),
            MessageErrorPolicy::Ignore | MessageErrorPolicy::Log => None,
        })
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    targets.dedup();
    targets
}

fn emitter_change_aspects(base: &CreateEmitter, candidate: &CreateEmitter) -> ModelChangeAspects {
    let CreateEmitter {
        name: base_name,
        from_relay: base_from_relay,
        collect_policy: base_collect_policy,
        encode_using_codec: base_codec,
        sink: base_sink,
        flush_each: base_flush_each,
        max_batch_size: base_max_batch_size,
        error_policies: base_error_policies,
        mode: base_mode,
        construction: base_construction,
        materialized_state: base_materialized_state,
    } = base;
    let CreateEmitter {
        name: candidate_name,
        from_relay: candidate_from_relay,
        collect_policy: candidate_collect_policy,
        encode_using_codec: candidate_codec,
        sink: candidate_sink,
        flush_each: candidate_flush_each,
        max_batch_size: candidate_max_batch_size,
        error_policies: candidate_error_policies,
        mode: candidate_mode,
        construction: candidate_construction,
        materialized_state: candidate_materialized_state,
    } = candidate;

    if base_name != candidate_name {
        return ModelChangeAspects::replaced();
    }

    let mut changes = ModelChangeAspects::default();
    if base_from_relay != candidate_from_relay {
        changes.push(ModelChangeAspect::EmitterInput);
    }
    if base_sink.client() != candidate_sink.client() {
        changes.push(ModelChangeAspect::EmitterClient);
    }
    if !emitter_sink_definition_eq(base_sink, candidate_sink) {
        changes.push(ModelChangeAspect::EmitterSink);
    }
    if base_codec != candidate_codec {
        changes.push(ModelChangeAspect::EmitterCodec);
    }
    if base_collect_policy != candidate_collect_policy {
        changes.push(ModelChangeAspect::EmitterCollectPolicy);
    }
    if base_mode != candidate_mode {
        changes.push(ModelChangeAspect::EmitterMode);
    }
    if base_construction != candidate_construction {
        changes.push(ModelChangeAspect::EmitterConstruction);
    }
    if base_error_policies != candidate_error_policies {
        changes.push(ModelChangeAspect::EmitterErrorPolicies);
    }
    if base_materialized_state != candidate_materialized_state {
        changes.push(ModelChangeAspect::EmitterMaterializedState);
    }
    if base_flush_each != candidate_flush_each || base_max_batch_size != candidate_max_batch_size {
        changes.push_dynamic(
            ModelChangeAspect::EmitterFlushPolicy,
            DynamicModelUpdate::Emitter {
                emitter: candidate_name.clone(),
                config: candidate.clone(),
            },
        );
    }
    changes
}

fn emitter_sink_definition_eq(base: &EmitSink, candidate: &EmitSink) -> bool {
    match (base, candidate) {
        (
            EmitSink::Kafka {
                client: _,
                topic: base_topic,
            },
            EmitSink::Kafka {
                client: _,
                topic: candidate_topic,
            },
        )
        | (
            EmitSink::Pulsar {
                client: _,
                topic: base_topic,
            },
            EmitSink::Pulsar {
                client: _,
                topic: candidate_topic,
            },
        )
        | (
            EmitSink::Mqtt {
                client: _,
                topic: base_topic,
            },
            EmitSink::Mqtt {
                client: _,
                topic: candidate_topic,
            },
        ) => base_topic == candidate_topic,
        (
            EmitSink::RabbitMq {
                client: _,
                queue: base_queue,
            },
            EmitSink::RabbitMq {
                client: _,
                queue: candidate_queue,
            },
        )
        | (
            EmitSink::Sqs {
                client: _,
                queue: base_queue,
            },
            EmitSink::Sqs {
                client: _,
                queue: candidate_queue,
            },
        ) => base_queue == candidate_queue,
        (
            EmitSink::Redis {
                client: _,
                channel: base_channel,
            },
            EmitSink::Redis {
                client: _,
                channel: candidate_channel,
            },
        ) => base_channel == candidate_channel,
        (
            EmitSink::Nats {
                client: _,
                subject: base_subject,
            },
            EmitSink::Nats {
                client: _,
                subject: candidate_subject,
            },
        ) => base_subject == candidate_subject,
        (EmitSink::ZeroMq { client: _ }, EmitSink::ZeroMq { client: _ })
        | (EmitSink::Sentry { client: _ }, EmitSink::Sentry { client: _ }) => true,
        (
            EmitSink::ClickHouse {
                client: _,
                table: base_table,
                values: base_values,
                flush_each: _,
            },
            EmitSink::ClickHouse {
                client: _,
                table: candidate_table,
                values: candidate_values,
                flush_each: _,
            },
        ) => base_table == candidate_table && base_values == candidate_values,
        (
            EmitSink::Postgres {
                client: _,
                table: base_table,
                values: base_values,
                conflict_action: base_conflict,
                max_batch: base_max_batch,
                flush_each: _,
            },
            EmitSink::Postgres {
                client: _,
                table: candidate_table,
                values: candidate_values,
                conflict_action: candidate_conflict,
                max_batch: candidate_max_batch,
                flush_each: _,
            },
        ) => {
            base_table == candidate_table
                && base_values == candidate_values
                && base_conflict == candidate_conflict
                && base_max_batch == candidate_max_batch
        }
        (
            EmitSink::MySql {
                client: _,
                table: base_table,
                values: base_values,
                conflict_action: base_conflict,
                max_batch: base_max_batch,
                flush_each: _,
            },
            EmitSink::MySql {
                client: _,
                table: candidate_table,
                values: candidate_values,
                conflict_action: candidate_conflict,
                max_batch: candidate_max_batch,
                flush_each: _,
            },
        ) => {
            base_table == candidate_table
                && base_values == candidate_values
                && base_conflict == candidate_conflict
                && base_max_batch == candidate_max_batch
        }
        (
            EmitSink::MongoDb {
                client: _,
                collection: base_collection,
                values: base_values,
                conflict_action: base_conflict,
                max_batch: base_max_batch,
                flush_each: _,
            },
            EmitSink::MongoDb {
                client: _,
                collection: candidate_collection,
                values: candidate_values,
                conflict_action: candidate_conflict,
                max_batch: candidate_max_batch,
                flush_each: _,
            },
        ) => {
            base_collection == candidate_collection
                && base_values == candidate_values
                && base_conflict == candidate_conflict
                && base_max_batch == candidate_max_batch
        }
        (
            EmitSink::Iceberg {
                backend: base_backend,
                client: _,
                table: base_table,
                values: base_values,
                location: base_location,
                catalog: base_catalog,
                flush_each: _,
                max_batch_size: _,
                commit_each: base_commit_each,
                max_commit_size: base_max_commit_size,
            },
            EmitSink::Iceberg {
                backend: candidate_backend,
                client: _,
                table: candidate_table,
                values: candidate_values,
                location: candidate_location,
                catalog: candidate_catalog,
                flush_each: _,
                max_batch_size: _,
                commit_each: candidate_commit_each,
                max_commit_size: candidate_max_commit_size,
            },
        ) => {
            base_backend == candidate_backend
                && base_table == candidate_table
                && base_values == candidate_values
                && base_location == candidate_location
                && base_catalog == candidate_catalog
                && base_commit_each == candidate_commit_each
                && base_max_commit_size == candidate_max_commit_size
        }
        (
            EmitSink::Kafka { .. }
            | EmitSink::Pulsar { .. }
            | EmitSink::RabbitMq { .. }
            | EmitSink::Redis { .. }
            | EmitSink::Mqtt { .. }
            | EmitSink::Nats { .. }
            | EmitSink::ZeroMq { .. }
            | EmitSink::Sqs { .. }
            | EmitSink::Sentry { .. }
            | EmitSink::ClickHouse { .. }
            | EmitSink::Postgres { .. }
            | EmitSink::MySql { .. }
            | EmitSink::MongoDb { .. }
            | EmitSink::Iceberg { .. },
            _,
        ) => false,
    }
}

fn wire_schema_change_aspects(
    base: &CreateWireSchemaStmt,
    candidate: &CreateWireSchemaStmt,
) -> ModelChangeAspects {
    let base_name = match base {
        CreateWireSchemaStmt::Json(base) => &base.name,
        CreateWireSchemaStmt::Cbor(base) => &base.name,
        CreateWireSchemaStmt::Avro(base) => &base.name,
    };
    let candidate_name = match candidate {
        CreateWireSchemaStmt::Json(candidate) => &candidate.name,
        CreateWireSchemaStmt::Cbor(candidate) => &candidate.name,
        CreateWireSchemaStmt::Avro(candidate) => &candidate.name,
    };
    if base_name == candidate_name {
        ModelChangeAspects {
            aspects: vec![ModelChangeAspect::WireSchemaDefinition],
            dynamic_updates: Vec::new(),
        }
    } else {
        ModelChangeAspects::replaced()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        AckMode, BranchSelection, CreateEmitter, CreateJunction, CreateRelay, EmitSink,
        ErrorPolicies, Identifier, InputCollectPolicy, MaterializedRelayState,
        MaterializedStateDependency, MaterializedStatePolicy, MessageErrorPolicy, Model,
        ModelChangeAspect, OutputFlushPolicy, ProcessorInputWhere, ProcessorInputs,
        ProcessorOutput, ProcessorOutputs, QuiesceLevel, RelayBranching,
    };

    fn identifier(raw: &str) -> Identifier {
        Identifier::parse(raw).expect("valid identifier")
    }

    fn relay() -> CreateRelay {
        CreateRelay {
            name: identifier("events"),
            schema: identifier("event"),
            buffer: 1,
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        }
    }

    fn junction() -> CreateJunction {
        CreateJunction {
            name: identifier("merge"),
            from: ProcessorInputs::single(identifier("input")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                identifier("output"),
                "1s".to_string(),
                Some("1MiB".to_string()),
            )]),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }
    }

    fn emitter() -> CreateEmitter {
        CreateEmitter {
            name: identifier("emit"),
            from_relay: identifier("events"),
            collect_policy: None,
            encode_using_codec: Some(identifier("event_codec")),
            sink: EmitSink::ZeroMq {
                client: identifier("sink"),
            },
            flush_each: "1s".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            error_policies: ErrorPolicies::handled_by_log(),
            mode: AckMode::Attached,
            construction: crate::RouteConstruction::default(),
            materialized_state: Vec::new(),
        }
    }

    fn assert_single_aspect(
        base: Model,
        candidate: Model,
        aspect: ModelChangeAspect,
        level: QuiesceLevel,
    ) {
        let changes = base.change_aspects_against(&candidate);
        assert_eq!(changes.aspects(), &[aspect]);
        assert_eq!(changes.quiesce_level(), level);
    }

    #[test]
    fn no_op_has_no_aspects_and_is_dynamic() {
        let model = Model::Relay(relay());
        let changes = model.change_aspects_against(&model);
        assert!(changes.is_empty());
        assert_eq!(changes.quiesce_level(), QuiesceLevel::Dynamic);
    }

    #[test]
    fn relay_aspects_have_their_contract_levels() {
        let base = relay();

        let mut capacity = base.clone();
        capacity.buffer = 5;
        assert_single_aspect(
            Model::Relay(base.clone()),
            Model::Relay(capacity),
            ModelChangeAspect::RelayCapacity,
            QuiesceLevel::Dynamic,
        );

        let mut schema = base.clone();
        schema.schema = identifier("event_v2");
        assert_single_aspect(
            Model::Relay(base.clone()),
            Model::Relay(schema),
            ModelChangeAspect::RelaySchema,
            QuiesceLevel::DomainPause,
        );

        let mut branching = base.clone();
        branching.branching = RelayBranching::branched_by(identifier("tenant"));
        assert_single_aspect(
            Model::Relay(base.clone()),
            Model::Relay(branching),
            ModelChangeAspect::RelayBranching,
            QuiesceLevel::DomainPause,
        );

        let mut materialized = base.clone();
        materialized.materialized_state = Some(MaterializedRelayState::LastByTimestamp);
        assert_single_aspect(
            Model::Relay(base),
            Model::Relay(materialized),
            ModelChangeAspect::RelayMaterializedState,
            QuiesceLevel::EntityPause,
        );
    }

    #[test]
    fn junction_dynamic_aspects_are_classified_individually() {
        let base = junction();

        let mut filter = base.clone();
        filter.filter_where = Some(crate::Expression::Literal(crate::Literal::Bool(true)));
        assert_single_aspect(
            Model::Junction(base.clone()),
            Model::Junction(filter),
            ModelChangeAspect::JunctionFilter,
            QuiesceLevel::Dynamic,
        );

        let mut input_where = base.clone();
        input_where.from.r#where.push(ProcessorInputWhere {
            relay: identifier("input"),
            where_clause: crate::Expression::Literal(crate::Literal::Bool(true)),
        });
        assert_single_aspect(
            Model::Junction(base.clone()),
            Model::Junction(input_where),
            ModelChangeAspect::JunctionInputWhere,
            QuiesceLevel::Dynamic,
        );

        let mut collect = base.clone();
        collect.from.collect_policy = Some(InputCollectPolicy {
            collect_for: "10ms".to_string(),
            max_batch_size: None,
        });
        assert_single_aspect(
            Model::Junction(base.clone()),
            Model::Junction(collect),
            ModelChangeAspect::JunctionCollectPolicy,
            QuiesceLevel::Dynamic,
        );

        let mut construction = base.clone();
        construction.output_routes.routes[0]
            .construction
            .where_clause = Some(crate::Expression::Literal(crate::Literal::Bool(true)));
        assert_single_aspect(
            Model::Junction(base.clone()),
            Model::Junction(construction),
            ModelChangeAspect::JunctionRouteConstruction,
            QuiesceLevel::Dynamic,
        );

        let mut flush = base.clone();
        flush.output_routes.routes[0].flush_policy = Some(OutputFlushPolicy {
            flush_each: "2s".to_string(),
            max_batch_size: Some("2MiB".to_string()),
        });
        assert_single_aspect(
            Model::Junction(base.clone()),
            Model::Junction(flush),
            ModelChangeAspect::JunctionRouteFlushPolicy,
            QuiesceLevel::Dynamic,
        );

        let mut error_policy = base.clone();
        error_policy.output_routes.routes[0].message_error_policy = MessageErrorPolicy::Ignore;
        assert_single_aspect(
            Model::Junction(base),
            Model::Junction(error_policy),
            ModelChangeAspect::JunctionMessageErrorPolicy,
            QuiesceLevel::Dynamic,
        );
    }

    #[test]
    fn junction_entity_pause_aspects_are_classified_individually() {
        let base = junction();

        let mut inputs = base.clone();
        inputs.from.from.push(identifier("input_two"));
        assert_single_aspect(
            Model::Junction(base.clone()),
            Model::Junction(inputs),
            ModelChangeAspect::JunctionInputs,
            QuiesceLevel::EntityPause,
        );

        let mut routes = base.clone();
        routes
            .output_routes
            .routes
            .push(ProcessorOutput::new(identifier("other")));
        assert_single_aspect(
            Model::Junction(base.clone()),
            Model::Junction(routes),
            ModelChangeAspect::JunctionRoutes,
            QuiesceLevel::EntityPause,
        );

        let mut reordered = base.clone();
        reordered
            .output_routes
            .routes
            .push(ProcessorOutput::new(identifier("other")));
        let mut reordered_candidate = reordered.clone();
        reordered_candidate.output_routes.routes.swap(0, 1);
        assert_single_aspect(
            Model::Junction(reordered),
            Model::Junction(reordered_candidate),
            ModelChangeAspect::JunctionRoutes,
            QuiesceLevel::EntityPause,
        );

        let mut mode = base.clone();
        mode.mode = AckMode::Detached;
        assert_single_aspect(
            Model::Junction(base.clone()),
            Model::Junction(mode),
            ModelChangeAspect::JunctionMode,
            QuiesceLevel::EntityPause,
        );

        let mut branching = base.clone();
        branching.branched_by = BranchSelection::branched_by(identifier("tenant"));
        assert_single_aspect(
            Model::Junction(base.clone()),
            Model::Junction(branching),
            ModelChangeAspect::JunctionBranching,
            QuiesceLevel::EntityPause,
        );

        let mut materialized = base.clone();
        materialized
            .materialized_state
            .push(MaterializedStateDependency {
                relay: identifier("state"),
                policy: MaterializedStatePolicy::RequiredSkip,
            });
        assert_single_aspect(
            Model::Junction(base.clone()),
            Model::Junction(materialized),
            ModelChangeAspect::JunctionMaterializedState,
            QuiesceLevel::EntityPause,
        );

        let mut dlq = base;
        dlq.output_routes.routes[0].message_error_policy = MessageErrorPolicy::Dlq {
            relay: identifier("errors"),
            assignments: Vec::new(),
        };
        assert_single_aspect(
            Model::Junction(junction()),
            Model::Junction(dlq),
            ModelChangeAspect::JunctionErrorRouteTargets,
            QuiesceLevel::EntityPause,
        );
    }

    #[test]
    fn emitter_aspects_have_dynamic_entity_and_domain_levels() {
        let base = emitter();

        let mut flush = base.clone();
        flush.flush_each = "IMMEDIATE".to_string();
        flush.max_batch_size = None;
        assert_single_aspect(
            Model::Emitter(base.clone()),
            Model::Emitter(flush.clone()),
            ModelChangeAspect::EmitterFlushPolicy,
            QuiesceLevel::Dynamic,
        );
        let dynamic = Model::Emitter(base.clone()).change_aspects_against(&Model::Emitter(flush));
        assert_eq!(dynamic.dynamic_updates().len(), 1);

        let mut client = base.clone();
        client.sink = EmitSink::ZeroMq {
            client: identifier("sink_two"),
        };
        assert_single_aspect(
            Model::Emitter(base.clone()),
            Model::Emitter(client),
            ModelChangeAspect::EmitterClient,
            QuiesceLevel::EntityPause,
        );

        let mut sink = base.clone();
        sink.sink = EmitSink::Nats {
            client: identifier("sink"),
            subject: identifier("events"),
        };
        assert_single_aspect(
            Model::Emitter(base.clone()),
            Model::Emitter(sink),
            ModelChangeAspect::EmitterSink,
            QuiesceLevel::EntityPause,
        );

        let mut codec = base.clone();
        codec.encode_using_codec = Some(identifier("event_codec_v2"));
        assert_single_aspect(
            Model::Emitter(base.clone()),
            Model::Emitter(codec),
            ModelChangeAspect::EmitterCodec,
            QuiesceLevel::EntityPause,
        );

        let mut collect = base.clone();
        collect.collect_policy = Some(InputCollectPolicy {
            collect_for: "10ms".to_string(),
            max_batch_size: Some("1MiB".to_string()),
        });
        assert_single_aspect(
            Model::Emitter(base.clone()),
            Model::Emitter(collect),
            ModelChangeAspect::EmitterCollectPolicy,
            QuiesceLevel::EntityPause,
        );

        let mut mode = base.clone();
        mode.mode = AckMode::Detached;
        assert_single_aspect(
            Model::Emitter(base.clone()),
            Model::Emitter(mode),
            ModelChangeAspect::EmitterMode,
            QuiesceLevel::EntityPause,
        );

        let mut source = base;
        source.from_relay = identifier("other_events");
        assert_single_aspect(
            Model::Emitter(emitter()),
            Model::Emitter(source),
            ModelChangeAspect::EmitterInput,
            QuiesceLevel::DomainPause,
        );
    }
}
