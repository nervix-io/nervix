//! Message-error route decisions for one installed domain revision.
//!
//! Layer: decisions.
//! - **Owns.** Selecting error routes, resolving their source, partial-output, destination, branch
//!   and flush contracts, and lowering ordered SET assignments from validated scheduled Models.
//! - **Depends on.** Vocabulary Models, the domain activation plan and the VM frontend.
//! - **Must not know.** Runtime delivery tasks, node-local VM bindings or relay services.

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    CodecName, DomainName, FlushPolicy, IngestorInput, MessageErrorPolicy, Model, NodeRef,
    ProcessorOutputs, RelayName, ResolvedBranching, RouteConstruction, ScheduledNodes, SchemaName,
};
use nervix_vm::{
    SemanticScopePolicy, lower_route_construction,
    program::{Program, SpannedNode},
};
use thiserror::Error;
use triomphe::Arc;

use super::DomainActivationPlan;
use crate::runtime_schema::CompiledSchema;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct MessageErrorRouteKey {
    pub(crate) domain: DomainName,
    pub(crate) node: NodeRef,
    pub(crate) source_route: Option<RelayName>,
    pub(crate) error_relay: RelayName,
}

#[derive(Debug, Clone)]
pub(crate) struct MessageErrorCompileSchemas {
    pub(crate) input: Option<Arc<CompiledSchema>>,
    pub(crate) left: Option<Arc<CompiledSchema>>,
    pub(crate) right: Option<Arc<CompiledSchema>>,
    pub(crate) partial_output: Option<Arc<CompiledSchema>>,
    pub(crate) current_branching: ResolvedBranching,
    pub(crate) allow_header_reads: bool,
}

impl Default for MessageErrorCompileSchemas {
    fn default() -> Self {
        Self {
            input: None,
            left: None,
            right: None,
            partial_output: None,
            current_branching: ResolvedBranching::unbranched(),
            allow_header_reads: false,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MessageErrorRouteSpec {
    pub(crate) key: MessageErrorRouteKey,
    pub(crate) program: SpannedNode<Program>,
    pub(crate) output_schema: Arc<CompiledSchema>,
    pub(crate) target_branching: ResolvedBranching,
    pub(crate) compile_schemas: MessageErrorCompileSchemas,
    pub(crate) flush_policy: Option<FlushPolicy>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MessageErrorRouteSpecs {
    pub(crate) routes: Vec<MessageErrorRouteSpec>,
}

#[derive(Debug, Error)]
pub(crate) enum MessageErrorPlanError {
    #[error("{node:?} message-error route reads missing relay '{relay}'")]
    RelayNotFound { node: NodeRef, relay: RelayName },
    #[error("{node:?} message-error route reads missing codec '{codec}'")]
    CodecNotFound { node: NodeRef, codec: CodecName },
    #[error("{node:?} message-error route reads missing client input schema '{schema}'")]
    ClientSchemaNotFound { node: NodeRef, schema: SchemaName },
    #[error("{node:?} message-error route has no {scope} input relay")]
    InputNotDeclared { node: NodeRef, scope: &'static str },
    #[error("{node:?} message-error SET to relay '{relay}' is invalid")]
    InvalidSet { node: NodeRef, relay: RelayName },
}

struct MessageErrorPlanContext<'a> {
    domain: &'a DomainName,
    activation: &'a DomainActivationPlan,
    routes: Vec<MessageErrorRouteSpec>,
}

impl MessageErrorPlanContext<'_> {
    fn relay(
        &self,
        node: &NodeRef,
        relay: &RelayName,
    ) -> Result<&super::domain_activation_plan::PlannedRelay, Report<MessageErrorPlanError>> {
        self.activation.relays.get(relay).ok_or_else(|| {
            Report::new(MessageErrorPlanError::RelayNotFound {
                node: node.clone(),
                relay: relay.clone(),
            })
        })
    }

    fn codec_schema(
        &self,
        node: &NodeRef,
        codec: &CodecName,
    ) -> Result<Arc<CompiledSchema>, Report<MessageErrorPlanError>> {
        let Some(codec_plan) = self.activation.codecs.get(codec) else {
            return Err(Report::new(MessageErrorPlanError::CodecNotFound {
                node: node.clone(),
                codec: codec.clone(),
            }));
        };
        Ok(codec_plan.schema.clone())
    }

    /// The schema an ingestor's rows carry before route construction: the one its transport's
    /// codec decodes into, or the input schema a client source's batches carry.
    fn ingestor_input_schema(
        &self,
        node: &NodeRef,
        input: &IngestorInput,
    ) -> Result<Arc<CompiledSchema>, Report<MessageErrorPlanError>> {
        match input {
            IngestorInput::Transport(transport) => self.codec_schema(node, &transport.codec),
            IngestorInput::Client(client) => {
                let Some(schema) = self.activation.schemas.get(&client.schema) else {
                    return Err(Report::new(MessageErrorPlanError::ClientSchemaNotFound {
                        node: node.clone(),
                        schema: client.schema.clone(),
                    }));
                };
                Ok(schema.clone())
            }
        }
    }

    fn input(
        &self,
        node: &NodeRef,
        relay: Option<&RelayName>,
        scope: &'static str,
    ) -> Result<Arc<CompiledSchema>, Report<MessageErrorPlanError>> {
        let relay = relay.ok_or_else(|| {
            Report::new(MessageErrorPlanError::InputNotDeclared {
                node: node.clone(),
                scope,
            })
        })?;
        Ok(self.relay(node, relay)?.schema.clone())
    }

    fn branch_from(
        &self,
        node: &NodeRef,
        relay: Option<&RelayName>,
    ) -> Result<ResolvedBranching, Report<MessageErrorPlanError>> {
        match relay {
            Some(relay) => Ok(self.relay(node, relay)?.branching.clone()),
            None => Ok(ResolvedBranching::unbranched()),
        }
    }

    fn add_route(
        &mut self,
        node: &NodeRef,
        source_route: Option<&RelayName>,
        policy: &MessageErrorPolicy,
        schemas: MessageErrorCompileSchemas,
        flush_policy: Option<&FlushPolicy>,
    ) -> Result<(), Report<MessageErrorPlanError>> {
        let MessageErrorPolicy::Dlq { relay, assignments } = policy else {
            return Ok(());
        };
        let target = self.relay(node, relay)?;
        let program = lower_route_construction(
            &RouteConstruction {
                assignments: assignments.clone(),
                ..RouteConstruction::default()
            },
            SemanticScopePolicy::read_write("error_output", "error_output"),
        )
        .change_context(MessageErrorPlanError::InvalidSet {
            node: node.clone(),
            relay: relay.clone(),
        })?;
        self.routes.push(MessageErrorRouteSpec {
            key: MessageErrorRouteKey {
                domain: self.domain.clone(),
                node: node.clone(),
                source_route: source_route.cloned(),
                error_relay: relay.clone(),
            },
            program,
            output_schema: target.schema.clone(),
            target_branching: target.branching.clone(),
            compile_schemas: schemas,
            flush_policy: flush_policy.cloned(),
        });
        Ok(())
    }

    fn add_outputs(
        &mut self,
        node: &NodeRef,
        outputs: &ProcessorOutputs,
        schemas: MessageErrorCompileSchemas,
    ) -> Result<(), Report<MessageErrorPlanError>> {
        for output in outputs.outputs() {
            let mut route_schemas = schemas.clone();
            route_schemas.partial_output = Some(self.relay(node, &output.relay)?.schema.clone());
            self.add_route(
                node,
                Some(&output.relay),
                &output.message_error_policy,
                route_schemas,
                output.flush_policy.as_ref(),
            )?;
        }
        Ok(())
    }
}

impl MessageErrorRouteSpecs {
    pub(crate) fn from_scheduled_nodes(
        domain: &DomainName,
        nodes: &ScheduledNodes,
        activation: &DomainActivationPlan,
    ) -> Result<Self, Report<MessageErrorPlanError>> {
        let mut context = MessageErrorPlanContext {
            domain,
            activation,
            routes: Vec::new(),
        };
        for (node, scheduled) in nodes {
            let mut schemas = MessageErrorCompileSchemas::default();
            match scheduled.config.as_ref() {
                Model::Ingestor(model) => {
                    schemas.input = Some(context.ingestor_input_schema(node, &model.input)?);
                    schemas.allow_header_reads = model.input.reads_headers();
                    context.add_outputs(node, &model.output_routes, schemas)?;
                }
                Model::Reingestor(model) => {
                    schemas.input = Some(context.input(node, model.from.first(), "input")?);
                    schemas.current_branching = context.branch_from(node, model.from.first())?;
                    context.add_outputs(node, &model.output_routes, schemas)?;
                }
                Model::Junction(model) => {
                    schemas.input = Some(context.input(node, model.from.first(), "input")?);
                    schemas.current_branching = context.branch_from(node, model.from.first())?;
                    context.add_outputs(node, &model.output_routes, schemas)?;
                }
                Model::Deduplicator(model) => {
                    schemas.input = Some(context.input(node, model.from.first(), "input")?);
                    schemas.current_branching = context.branch_from(node, model.from.first())?;
                    context.add_outputs(node, &model.output_routes, schemas)?;
                }
                Model::Reorderer(model) => {
                    schemas.input = Some(context.input(node, model.from.first(), "input")?);
                    schemas.current_branching = context.branch_from(node, model.from.first())?;
                    context.add_outputs(node, &model.output_routes, schemas)?;
                }
                Model::WindowProcessor(model) => {
                    schemas.current_branching = context.branch_from(node, model.from.first())?;
                    context.add_outputs(node, &model.output_routes, schemas)?;
                }
                Model::Generator(model) => {
                    schemas.current_branching =
                        context.branch_from(node, Some(&model.materialized_relay))?;
                    context.add_outputs(node, &model.output_routes, schemas)?;
                }
                Model::Inferencer(model) => {
                    schemas.input = Some(context.input(node, model.from.first(), "input")?);
                    schemas.current_branching = context.branch_from(node, model.from.first())?;
                    context.add_outputs(node, &model.output_routes, schemas)?;
                }
                Model::WasmProcessor(model) => {
                    schemas.input = Some(context.input(node, model.from.first(), "input")?);
                    schemas.current_branching = context.branch_from(node, model.from.first())?;
                    context.add_outputs(node, &model.output_routes, schemas)?;
                }
                Model::Correlator(model) => {
                    schemas.left = Some(context.input(node, model.left.first(), "left")?);
                    schemas.right = Some(context.input(node, model.right.first(), "right")?);
                    schemas.current_branching = context.branch_from(node, model.left.first())?;
                    context.add_outputs(node, &model.output_routes, schemas)?;
                }
                Model::Emitter(model) => {
                    schemas.input = Some(context.input(node, model.from.first(), "input")?);
                    schemas.current_branching = context.branch_from(node, model.from.first())?;
                    if let Some(codec) = model.body.codec() {
                        schemas.partial_output = Some(context.codec_schema(node, codec)?);
                    }
                    context.add_route(
                        node,
                        None,
                        &model.error_policies.message,
                        schemas,
                        Some(&model.flush_policy),
                    )?;
                }
                _ => {}
            }
        }
        Ok(Self {
            routes: context.routes,
        })
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{DomainName, ModelKind, ModelName, ParseAsType, SchemaField, Statement};

    use super::*;
    use crate::registry::test_fixtures::{
        codec, deduplicator, emitter, ingestor, junction, reingestor, relay, schema,
        unbranched_correlator, unplaced_schedule, wasm_processor, window_processor, wire_schema,
    };

    fn named<N>(raw: &str) -> N
    where
        N: TryFrom<String>,
        <N as TryFrom<String>>::Error: std::fmt::Debug,
    {
        N::try_from(raw.to_string()).expect("valid name")
    }

    fn parsed_model(source: &str) -> Model {
        let statement = nervix_nspl::server_statement::parse_server_statement(source)
            .expect("test model should parse");
        let Statement::Create(create) = statement else {
            panic!("test source should create a model");
        };
        create
            .body
            .try_map_resource_versions(|resource, requested| match requested {
                nervix_models::RequestedResourceVersion::Number(version) => Ok(version),
                nervix_models::RequestedResourceVersion::Latest => Err(resource.clone()),
            })
            .expect("test sources name explicit resource versions")
    }

    fn with_dlq(mut model: Model) -> Model {
        let policy = MessageErrorPolicy::Dlq {
            relay: named("errors"),
            assignments: nervix_nspl::parse_route_construction(
                "SET value = error.code, operation = error.operation",
            )
            .expect("error assignments should parse")
            .assignments,
        };
        let outputs = match &mut model {
            Model::Ingestor(node) => &mut node.output_routes,
            Model::Reingestor(node) => &mut node.output_routes,
            Model::Junction(node) => &mut node.output_routes,
            Model::Deduplicator(node) => &mut node.output_routes,
            Model::Reorderer(node) => &mut node.output_routes,
            Model::WindowProcessor(node) => &mut node.output_routes,
            Model::Generator(node) => &mut node.output_routes,
            Model::Inferencer(node) => &mut node.output_routes,
            Model::WasmProcessor(node) => &mut node.output_routes,
            Model::Correlator(node) => &mut node.output_routes,
            Model::Emitter(node) => {
                node.error_policies.message = policy;
                return model;
            }
            _ => panic!("test model must own message errors"),
        };
        outputs.routes[0].message_error_policy = policy;
        model
    }

    fn fixture() -> (DomainName, ScheduledNodes, DomainActivationPlan) {
        let domain = named("sales");
        let mut error_schema = schema("error_payload");
        let Model::Schema(error_schema_fields) = &mut error_schema else {
            panic!("the fixture builds a schema");
        };
        error_schema_fields.fields.push(SchemaField {
            name: named("operation"),
            ty: ParseAsType::String,
            optional: false,
            sensitive: false,
        });
        let models = vec![
            schema("payload"),
            error_schema,
            wire_schema("event_wire"),
            codec("json", "payload"),
            relay("events", "payload"),
            relay("out", "payload"),
            relay("errors", "error_payload"),
            with_dlq(ingestor("source", "out", "json", "broker")),
            with_dlq(reingestor("repeat", "events", "out", &[])),
            with_dlq(junction("join", &["events"], "out")),
            with_dlq(deduplicator("dedup", "events", "out", "input.value", "10m")),
            with_dlq(window_processor(
                "window",
                "events",
                "out",
                "SET value = FIRST(input.value)",
            )),
            with_dlq(wasm_processor("wasm", "events", "out")),
            with_dlq(unbranched_correlator(
                "correlate",
                "events",
                "events",
                "out",
            )),
            with_dlq(emitter("sink", "events", "json", "broker")),
            with_dlq(parsed_model(
                "CREATE REORDERER order_events FROM events BY input.value MAX TIME 10s UNBRANCHED \
                 TO out INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
            )),
            with_dlq(parsed_model(
                "CREATE GENERATOR generate USING MATERIALIZED STATE events EACH 100ms UNBRANCHED \
                 TO out SET value = relay_state.events.value FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
            )),
            with_dlq(parsed_model(
                "CREATE INFERENCER infer FROM events USING RESOURCE model VERSION 1 FILE \
                 'model.onnx' INPUTS { \"features\" DENSE TENSOR<F32>[1] = input.value } OUTPUT \
                 SCHEMA { \"score\" DENSE TENSOR<F32>[1] } UNBRANCHED TO out SET value = score \
                 FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
            )),
        ];
        // The planner only needs the route declarations and activation surfaces of these current
        // Models; external clients and processor resources are bound by their own owners.
        let nodes = unplaced_schedule(models);
        let activation = DomainActivationPlan::from_scheduled_nodes(&domain, &nodes)
            .expect("the fixture surfaces resolve");
        (domain, nodes, activation)
    }

    #[test]
    fn plans_every_error_route_owner_with_ordered_assignments_and_scopes() {
        let (domain, nodes, activation) = fixture();
        let plans = MessageErrorRouteSpecs::from_scheduled_nodes(&domain, &nodes, &activation)
            .expect("every owner should have a route plan");
        assert_eq!(plans.routes.len(), 11);
        for route in &plans.routes {
            assert_eq!(route.program.inner.set.len(), 2);
            assert_eq!(route.key.error_relay, named::<RelayName>("errors"));
            assert!(route.compile_schemas.partial_output.is_some());
            match route.key.node.kind {
                ModelKind::Ingestor => {
                    assert!(route.compile_schemas.input.is_some());
                    assert!(route.key.source_route.is_some());
                }
                ModelKind::Correlator => {
                    assert!(route.compile_schemas.left.is_some());
                    assert!(route.compile_schemas.right.is_some());
                    assert!(route.compile_schemas.input.is_none());
                }
                ModelKind::Emitter => {
                    assert!(route.compile_schemas.input.is_some());
                    assert!(route.key.source_route.is_none());
                }
                ModelKind::WindowProcessor | ModelKind::Generator => {
                    assert!(route.compile_schemas.input.is_none());
                }
                _ => assert!(route.compile_schemas.input.is_some()),
            }
        }
        let source = plans
            .routes
            .iter()
            .find(|route| route.key.node.identifier == named::<ModelName>("source"))
            .expect("the ingestor route is planned");
        let targets = source
            .program
            .inner
            .set
            .iter()
            .map(|(target, _)| target.field.as_str())
            .collect::<Vec<_>>();
        assert_eq!(targets, ["value", "operation"]);
    }

    #[test]
    fn classifies_missing_input_and_codec_at_plan_creation() {
        let (domain, nodes, activation) = fixture();
        let mut missing_input = nodes.clone();
        let node = missing_input
            .get_mut(&NodeRef::new(
                ModelKind::Junction,
                named::<ModelName>("join"),
            ))
            .expect("the junction is scheduled");
        let Model::Junction(junction) = node.config.as_mut() else {
            panic!("the scheduled node is a junction");
        };
        junction.from.from.clear();
        let error =
            MessageErrorRouteSpecs::from_scheduled_nodes(&domain, &missing_input, &activation)
                .expect_err("a missing input must fail planning");
        assert!(matches!(
            error.current_context(),
            MessageErrorPlanError::InputNotDeclared { .. }
        ));

        let mut missing_codec = nodes;
        let node = missing_codec
            .get_mut(&NodeRef::new(
                ModelKind::Ingestor,
                named::<ModelName>("source"),
            ))
            .expect("the ingestor is scheduled");
        let Model::Ingestor(ingestor) = node.config.as_mut() else {
            panic!("the scheduled node is an ingestor");
        };
        let IngestorInput::Transport(transport) = &mut ingestor.input else {
            panic!("the fixture ingestor reads a transport");
        };
        transport.codec = named("unknown_codec");
        let error =
            MessageErrorRouteSpecs::from_scheduled_nodes(&domain, &missing_codec, &activation)
                .expect_err("a missing codec must fail planning");
        assert!(matches!(
            error.current_context(),
            MessageErrorPlanError::CodecNotFound { .. }
        ));
    }
}
