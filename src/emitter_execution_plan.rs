//! The typed execution decision for every emitter in one scheduled domain.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving emitter clients and source relays, preserving their delivery modes, and
//!   lowering emitter route, source, HTTP request, ordering and envelope expressions before runtime
//!   binding.
//! - **Depends on.** Validated schedule Models, the activation plan's schemas, the emitter start
//!   decision, and the expression VM frontend.
//! - **Must not know.** Tokio, locks, connector I/O, node-local resources, or task spawning.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::collections::BTreeMap;

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::RecordEnvelope;
use nervix_models::{
    AckMode, Assignment, AssignmentTarget, CodecName, CreateEmitter, CreateSchema, EmitterName,
    EnvelopeClauseForm, EnvelopeClauseKind, ErrorPolicies, Expression, FieldName, FlushPolicy,
    InputCollectPolicy, MaterializedStateDependency, Model, ModelKind, ModelName, NodeRef,
    PropertyDeclaration, RelayName, RouteConstruction, ScheduledNodes, SchemaField, SchemaName,
    SqsFifoGroup, parse_duration_text,
};
use nervix_primitives::sync::{Arc, StdArc};
use nervix_vm::{
    SemanticScopePolicy, lower_route_construction, lower_transforming_route,
    program::{FunctionName, Program, SpannedNode},
};
use thiserror::Error;

use crate::{
    emitter_start_plan::{DeclaredClientConfig, EmitterClientModels, EmitterStartPlan},
    registry::{DomainActivationPlan, LoweredFilter},
    runtime_schema::CompiledSchema,
};

const HTTP_REQUEST_NAMESPACE: &str = "http_request";
const ORDERING_GROUP_FIELD: &str = "ordering_group";
/// The internal namespace an envelope program writes its values into. No field reference a user
/// writes names it: the program assigns it only through the bare targets this plan synthesizes.
pub(crate) const ENVELOPE_NAMESPACE: &str = "envelope";

#[derive(Debug, Error)]
pub(crate) enum EmitterExecutionPlanError {
    #[error("scheduled emitter '{emitter}' is configured as another node")]
    Identity { emitter: EmitterName },
    #[error("emitter '{emitter}' has no input relay")]
    MissingInput { emitter: EmitterName },
    #[error("emitter '{emitter}' reads missing relay '{relay}'")]
    MissingRelay {
        emitter: EmitterName,
        relay: RelayName,
    },
    #[error("emitter '{emitter}' encodes with missing codec '{codec}'")]
    MissingCodec {
        emitter: EmitterName,
        codec: CodecName,
    },
    #[error("emitter '{emitter}' exports a missing CLIENT output schema '{schema}'")]
    MissingClientSchema {
        emitter: EmitterName,
        schema: nervix_models::SchemaName,
    },
    #[error("emitter '{emitter}' cannot plan its emitter")]
    Connector { emitter: EmitterName },
    #[error("emitter '{emitter}' has an invalid source predicate for '{relay}'")]
    SourceFilter {
        emitter: EmitterName,
        relay: RelayName,
    },
    #[error("emitter '{emitter}' has an invalid route construction")]
    Route { emitter: EmitterName },
    #[error("direct emitter '{emitter}' supports VALUES and WHERE only")]
    DirectConstruction { emitter: EmitterName },
    #[error("{connector} emitter '{emitter}' does not support FILTER-MAP headers")]
    Headers {
        emitter: EmitterName,
        connector: &'static str,
    },
    #[error("HTTP emitter '{emitter}' has invalid request fields")]
    HttpRequest { emitter: EmitterName },
    #[error("emitter '{emitter}' has an invalid ordering group expression")]
    OrderingGroup { emitter: EmitterName },
    #[error("emitter '{emitter}' has an invalid envelope")]
    Envelope { emitter: EmitterName },
}

/// One source edge, in declared input order, with its already lowered predicate.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EmitterInputPlan {
    pub(crate) relay: RelayName,
    pub(crate) from_where: Option<LoweredFilter>,
}

/// The route's frontend program and the boundary between inherited and declared SET operations.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EmitterRoutePlan {
    pub(crate) program: SpannedNode<Program>,
    pub(crate) inherited_count: usize,
    pub(crate) codec_route: bool,
}

/// The source-row group that the host evaluates before handing an SQS record to its emitter.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EmitterOrderingGroupPlan {
    FromBranch,
    Expression(SpannedNode<Program>),
}

/// What the value one field of an envelope program computes sets on a record: the routing key its
/// destination computes, a clause, or a property.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnvelopeTarget {
    RoutingKey,
    Clause(EnvelopeClauseKind),
    Property(PropertyDeclaration),
}

/// The values an emitter's envelope computes from each record, and the program that computes them.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ComputedEnvelopePlan {
    /// What the value of each field the program writes sets, in the order it writes them.
    pub(crate) targets: Vec<EnvelopeTarget>,
    /// Whether the program reads, as `output`, the record the emitter's route constructs. Where the
    /// emitter publishes its source records, `message` is the source record and nothing is
    /// `output`.
    pub(crate) reads_output: bool,
    /// The fields the program writes, one for each target: a non-null value of its exact type.
    pub(crate) schema: CreateSchema,
    pub(crate) program: SpannedNode<Program>,
}

/// What the host evaluates for the envelope of every record an emitter publishes.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EmitterEnvelopePlan {
    /// What every record carries whatever it holds: the flag and duration clauses.
    pub(crate) fixed: RecordEnvelope,
    /// The values each record computes, absent where the envelope computes none.
    pub(crate) computed: Option<ComputedEnvelopePlan>,
}

/// One emitter's complete in-memory decision. The host binds resource mounts and VM programs
/// against local resources, then executes these typed edges and policies.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EmitterExecutionPlan {
    pub(crate) name: EmitterName,
    pub(crate) mode: AckMode,
    pub(crate) inputs: Vec<EmitterInputPlan>,
    pub(crate) collect_policy: Option<InputCollectPolicy>,
    pub(crate) codec: Option<CodecName>,
    /// The validated schema of the rows handed to this emitter's emitter.
    pub(crate) output_schema: Arc<CompiledSchema>,
    pub(crate) connector: EmitterStartPlan<DeclaredClientConfig>,
    pub(crate) route: Option<EmitterRoutePlan>,
    pub(crate) http_request: Option<SpannedNode<Program>>,
    pub(crate) ordering_group: Option<EmitterOrderingGroupPlan>,
    /// The envelope every record carries, absent where the emitter writes none.
    pub(crate) envelope: Option<EmitterEnvelopePlan>,
    pub(crate) flush_policy: FlushPolicy,
    pub(crate) error_policies: ErrorPolicies,
    pub(crate) materialized_state: Vec<MaterializedStateDependency>,
}

impl EmitterExecutionPlan {
    fn decide(
        emitter: &CreateEmitter,
        nodes: &ScheduledNodes,
        activation: &DomainActivationPlan,
    ) -> error_stack::Result<Self, EmitterExecutionPlanError> {
        let client = match emitter.connector.client() {
            Some(name) => nodes
                .get(&NodeRef::new(ModelKind::Client, name))
                .map(|node| node.config.as_ref()),
            None => None,
        };
        let catalog_client = match emitter.connector.catalog_client() {
            Some(catalog) => nodes
                .get(&NodeRef::new(ModelKind::Client, catalog.client))
                .map(|node| node.config.as_ref()),
            None => None,
        };
        let connector = EmitterStartPlan::decide(
            emitter,
            EmitterClientModels {
                client,
                catalog_client,
            },
        )
        .change_context(EmitterExecutionPlanError::Connector {
            emitter: emitter.name.clone(),
        })?;

        let mut source_filters = BTreeMap::new();
        for source_filter in emitter.from.where_clauses() {
            source_filters.insert(&source_filter.relay, &source_filter.where_clause);
        }
        let mut inputs = Vec::with_capacity(emitter.from.relays().len());
        let mut input_schema = None;
        for relay in emitter.from.relays() {
            let planned = activation.relays.get(relay).ok_or_else(|| {
                Report::new(EmitterExecutionPlanError::MissingRelay {
                    emitter: emitter.name.clone(),
                    relay: relay.clone(),
                })
            })?;
            if input_schema.is_none() {
                input_schema = Some(planned.schema.arrow_schema());
            }
            let from_where = match source_filters.get(relay) {
                Some(expression) => Some(LoweredFilter::input(expression).change_context(
                    EmitterExecutionPlanError::SourceFilter {
                        emitter: emitter.name.clone(),
                        relay: relay.clone(),
                    },
                )?),
                None => None,
            };
            inputs.push(EmitterInputPlan {
                relay: relay.clone(),
                from_where,
            });
        }
        let input_schema = input_schema.ok_or_else(|| {
            Report::new(EmitterExecutionPlanError::MissingInput {
                emitter: emitter.name.clone(),
            })
        })?;
        let codec = emitter.body.codec().cloned();
        let output_schema = match emitter.connector.as_ref() {
            nervix_models::EmitterEntity::Client(nervix_models::ClientEmitter::Consumer(
                nervix_models::ClientConsumerEmitter { schema, .. },
            )) => activation
                .schemas
                .get(schema)
                .ok_or_else(|| {
                    Report::new(EmitterExecutionPlanError::MissingClientSchema {
                        emitter: emitter.name.clone(),
                        schema: schema.clone(),
                    })
                })?
                .clone(),
            _ => match &codec {
                Some(codec) => {
                    let planned = activation.codecs.get(codec).ok_or_else(|| {
                        Report::new(EmitterExecutionPlanError::MissingCodec {
                            emitter: emitter.name.clone(),
                            codec: codec.clone(),
                        })
                    })?;
                    planned.schema.clone()
                }
                None => activation
                    .relays
                    .get(
                        emitter
                            .from
                            .relays()
                            .first()
                            .verified("an emitter has an input relay"),
                    )
                    .verified("the input relay was resolved above")
                    .schema
                    .clone(),
            },
        };
        let route = Self::route(emitter, &input_schema, &output_schema.arrow_schema())?;
        let http_request = Self::http_request(emitter)?;
        let ordering_group = Self::ordering_group(emitter, &input_schema)?;
        let envelope = EmitterEnvelopePlan::decide(emitter, Self::constructs_output(emitter))?;
        Ok(Self {
            name: emitter.name.clone(),
            mode: emitter.mode,
            inputs,
            collect_policy: emitter.from.collect_policy.clone(),
            codec,
            output_schema,
            connector,
            route,
            http_request,
            ordering_group,
            envelope,
            flush_policy: emitter.flush_policy.clone(),
            error_policies: emitter.error_policies.clone(),
            materialized_state: emitter.materialized_state.clone(),
        })
    }

    /// Whether the emitter's route constructs the record it publishes from each source record: a
    /// codec or native body, built by the construction the emitter writes or, for an HTTP emitter,
    /// by the request route it always runs. Otherwise the emitter publishes its source records.
    pub(crate) fn constructs_output(emitter: &CreateEmitter) -> bool {
        let output_body = match emitter.body {
            nervix_models::EmitterBody::Codec { .. } | nervix_models::EmitterBody::Client => true,
            nervix_models::EmitterBody::WithoutBody | nervix_models::EmitterBody::Values => false,
        };
        let runs_route = !emitter.construction.is_empty()
            || matches!(
                emitter.connector.as_ref(),
                nervix_models::EmitterEntity::Http(_)
            );
        output_body && runs_route
    }

    pub(crate) fn route(
        emitter: &CreateEmitter,
        input: &arrow_schema::Schema,
        output: &arrow_schema::Schema,
    ) -> error_stack::Result<Option<EmitterRoutePlan>, EmitterExecutionPlanError> {
        let codec_route = emitter.body.codec().is_some()
            || matches!(emitter.body, nervix_models::EmitterBody::Client);
        if emitter.construction.is_empty()
            && !matches!(
                emitter.connector.as_ref(),
                nervix_models::EmitterEntity::Http(_)
            )
        {
            return Ok(None);
        }
        let construction = match emitter.connector.as_ref() {
            nervix_models::EmitterEntity::Http(_) => RouteConstruction {
                invocations: Vec::new(),
                ..emitter.construction.clone()
            },
            _ => emitter.construction.clone(),
        };
        if construction.is_empty() && !codec_route {
            return Ok(None);
        }
        if !codec_route
            && (construction.inherit.is_some()
                || !construction.assignments.is_empty()
                || !construction.invocations.is_empty())
        {
            return Err(Report::new(EmitterExecutionPlanError::DirectConstruction {
                emitter: emitter.name.clone(),
            }));
        }
        let program = if codec_route {
            lower_transforming_route(&construction, input, output)
        } else {
            lower_route_construction(&construction, SemanticScopePolicy::read_only("input"))
        }
        .change_context(EmitterExecutionPlanError::Route {
            emitter: emitter.name.clone(),
        })?;
        if program
            .inner
            .invoke
            .iter()
            .any(|invocation| invocation.inner.function == FunctionName::WriteHeader)
            && !emitter.connector.capabilities().writes_headers()
        {
            return Err(Report::new(EmitterExecutionPlanError::Headers {
                emitter: emitter.name.clone(),
                connector: emitter.connector.transport_label(),
            }));
        }
        let inherited_count = if codec_route {
            program
                .inner
                .set
                .len()
                .checked_sub(construction.assignments.len())
                .assured("the lowered route lists inherited SET steps before declared assignments")
        } else {
            0
        };
        Ok(Some(EmitterRoutePlan {
            program,
            inherited_count,
            codec_route,
        }))
    }

    fn http_request(
        emitter: &CreateEmitter,
    ) -> error_stack::Result<Option<SpannedNode<Program>>, EmitterExecutionPlanError> {
        let nervix_models::EmitterEntity::Http(nervix_models::HttpEmitter::Request(
            nervix_models::HttpRequestEmitter { method, path, .. },
        )) = emitter.connector.as_ref()
        else {
            return Ok(None);
        };
        let method_field = FieldName::parse("method").assured("method is a valid field literal");
        let path_field = FieldName::parse("path").assured("path is a valid field literal");
        let construction = RouteConstruction {
            assignments: vec![
                Assignment {
                    target: AssignmentTarget::bare(method_field),
                    value: method.clone(),
                },
                Assignment {
                    target: AssignmentTarget::bare(path_field),
                    value: path.clone(),
                },
            ],
            invocations: emitter.construction.invocations.clone(),
            ..RouteConstruction::default()
        };
        let program = lower_route_construction(
            &construction,
            SemanticScopePolicy::read_write("message", HTTP_REQUEST_NAMESPACE),
        )
        .change_context(EmitterExecutionPlanError::HttpRequest {
            emitter: emitter.name.clone(),
        })?;
        Ok(Some(program))
    }

    fn ordering_group(
        emitter: &CreateEmitter,
        input: &arrow_schema::Schema,
    ) -> error_stack::Result<Option<EmitterOrderingGroupPlan>, EmitterExecutionPlanError> {
        let nervix_models::EmitterEntity::Sqs(nervix_models::SqsEmitter::Queue(
            nervix_models::SqsQueueEmitter {
                fifo_group: Some(group),
                ..
            },
        )) = emitter.connector.as_ref()
        else {
            return Ok(None);
        };
        let expression = match group {
            SqsFifoGroup::FromBranch => return Ok(Some(EmitterOrderingGroupPlan::FromBranch)),
            SqsFifoGroup::Expression(expression) => expression,
        };
        let plan = EmitterOrderingGroupPlan::expression(&emitter.name, expression, input)?;
        Ok(Some(plan))
    }
}

impl EmitterOrderingGroupPlan {
    pub(crate) fn expression(
        emitter: &EmitterName,
        expression: &Expression,
        input: &arrow_schema::Schema,
    ) -> error_stack::Result<Self, EmitterExecutionPlanError> {
        let field = FieldName::parse(ORDERING_GROUP_FIELD)
            .assured("ordering_group is a valid field literal");
        let output = StdArc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            field.as_str(),
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let construction = RouteConstruction {
            assignments: vec![Assignment {
                target: AssignmentTarget::bare(field),
                value: expression.clone(),
            }],
            ..RouteConstruction::default()
        };
        let program = lower_transforming_route(&construction, input, output.as_ref())
            .change_context(EmitterExecutionPlanError::OrderingGroup {
                emitter: emitter.clone(),
            })?;
        Ok(Self::Expression(program))
    }
}

/// One value an envelope computes for every record: what it sets, the exact type it has and the
/// expression that computes it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EnvelopeValue {
    pub(crate) target: EnvelopeTarget,
    pub(crate) ty: nervix_models::ParseAsType,
    pub(crate) expression: Expression,
}

impl EmitterEnvelopePlan {
    /// The envelope plan of `emitter`, absent where it writes no envelope clause, sets no property
    /// and its destination computes no routing key.
    ///
    /// The registry has checked every clause and property against the emitter's entity and every
    /// duration and expression type, so a failure here is a Model that skipped that validation.
    fn decide(
        emitter: &CreateEmitter,
        reads_output: bool,
    ) -> error_stack::Result<Option<Self>, EmitterExecutionPlanError> {
        let envelope = &emitter.envelope;
        let routing_key = emitter.connector.routing_key();
        if envelope.is_empty() && routing_key.is_none() {
            return Ok(None);
        }
        let invalid = || EmitterExecutionPlanError::Envelope {
            emitter: emitter.name.clone(),
        };
        let deliver_after = match &envelope.deliver_after {
            Some(text) => Some(parse_duration_text(text).change_context_lazy(invalid)?),
            None => None,
        };
        let delay = match &envelope.delay {
            Some(text) => Some(parse_duration_text(text).change_context_lazy(invalid)?),
            None => None,
        };
        let fixed = RecordEnvelope {
            persistent: envelope.persistent,
            mandatory: envelope.mandatory,
            retain: envelope.retain,
            deliver_after,
            delay,
            ..RecordEnvelope::default()
        };

        let declared = emitter.connector.capabilities();
        let mut values = Vec::new();
        // The destination comes before the envelope in a statement, so the routing key is the first
        // value a record's evaluation can fail at.
        if let Some(expression) = routing_key {
            values.push(EnvelopeValue {
                target: EnvelopeTarget::RoutingKey,
                ty: nervix_models::ParseAsType::String,
                expression: expression.clone(),
            });
        }
        for clause in envelope.clauses() {
            let EnvelopeClauseForm::Expression(ty) = clause.kind().form() else {
                continue;
            };
            let expression = clause
                .expression()
                .assured("a clause whose form is an expression holds the expression it evaluates");
            values.push(EnvelopeValue {
                target: EnvelopeTarget::Clause(clause.kind()),
                ty,
                expression: expression.clone(),
            });
        }
        for property in &envelope.properties {
            let Some(declaration) = declared.property(&property.name) else {
                return Err(Report::new(invalid()).attach_printable(format!(
                    "{} emitters declare no PROPERTIES key '{}'",
                    emitter.connector.kind(),
                    property.name
                )));
            };
            values.push(EnvelopeValue {
                target: EnvelopeTarget::Property(*declaration),
                ty: declaration.ty.into(),
                expression: property.expression.clone(),
            });
        }
        let plan = Self::computing(fixed, values, reads_output).change_context_lazy(invalid)?;
        Ok(Some(plan))
    }

    /// The plan of an envelope whose every record carries `fixed` and computes `values`, in order:
    /// one program writing each value into a non-null field of its exact type, named by its
    /// position.
    pub(crate) fn computing(
        fixed: RecordEnvelope,
        values: Vec<EnvelopeValue>,
        reads_output: bool,
    ) -> nervix_vm::FrontendResult<Self> {
        if values.is_empty() {
            return Ok(Self {
                fixed,
                computed: None,
            });
        }
        let mut targets = Vec::with_capacity(values.len());
        let mut fields = Vec::with_capacity(values.len());
        let mut assignments = Vec::with_capacity(values.len());
        for value in values {
            let name = FieldName::parse(&format!("value_{}", fields.len()))
                .assured("value_ followed by a position is a valid field literal");
            fields.push(SchemaField {
                name: name.clone(),
                ty: value.ty,
                optional: false,
                sensitive: false,
            });
            assignments.push(Assignment {
                target: AssignmentTarget::bare(name),
                value: value.expression,
            });
            targets.push(value.target);
        }
        let program = lower_route_construction(
            &RouteConstruction {
                assignments,
                ..RouteConstruction::default()
            },
            SemanticScopePolicy::read_write("message", ENVELOPE_NAMESPACE),
        )?;
        let schema = CreateSchema {
            name: SchemaName::parse(ENVELOPE_NAMESPACE)
                .assured("the envelope namespace is a valid schema name"),
            fields,
        };
        Ok(Self {
            fixed,
            computed: Some(ComputedEnvelopePlan {
                targets,
                reads_output,
                schema,
                program,
            }),
        })
    }
}

/// All emitters of a schedule, including ones placed on other nodes, planned from the same
/// revision as the relay and codec surfaces they bind to.
#[derive(Debug, Clone, Default)]
pub(crate) struct EmitterExecutionPlans {
    emitters: BTreeMap<EmitterName, Arc<EmitterExecutionPlan>>,
}

impl EmitterExecutionPlans {
    pub(crate) fn from_scheduled_nodes(
        nodes: &ScheduledNodes,
        activation: &DomainActivationPlan,
    ) -> error_stack::Result<Self, EmitterExecutionPlanError> {
        let mut plans = Self::default();
        for node in nodes.values() {
            let Model::Emitter(emitter) = node.config.as_ref() else {
                continue;
            };
            if node.identifier != ModelName::from(&emitter.name) {
                return Err(Report::new(EmitterExecutionPlanError::Identity {
                    emitter: emitter.name.clone(),
                }));
            }
            let plan = EmitterExecutionPlan::decide(emitter, nodes, activation)?;
            plans.emitters.insert(emitter.name.clone(), Arc::new(plan));
        }
        Ok(plans)
    }

    pub(crate) fn emitter(&self, name: &EmitterName) -> Option<&Arc<EmitterExecutionPlan>> {
        self.emitters.get(name)
    }

    pub(crate) fn emitters(&self) -> impl Iterator<Item = &Arc<EmitterExecutionPlan>> {
        self.emitters.values()
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_models::{
        AckMode, ClientName, EmitterBody, EmitterEntity, ErrorPolicies, FlushPolicy, HttpEmitter,
        HttpRequestEmitter, ProcessorInputs, RetryPolicy, SqsEmitter, SqsQueueEmitter,
        ZeroMqEmitter, ZeroMqPushEmitter,
    };

    use super::*;

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("the fixture uses valid names")
    }

    fn expression(raw: &str) -> Expression {
        nervix_plexus::parse_expression(raw).expect("the fixture uses valid expressions")
    }

    fn emitter(connector: EmitterEntity, body: EmitterBody) -> CreateEmitter {
        CreateEmitter {
            name: named("output"),
            from: ProcessorInputs::single(named("input_relay")),
            body,
            envelope: Box::new(nervix_models::EmitterEnvelope::default()),
            connector: Box::new(connector),
            batch: None,
            flush_policy: FlushPolicy::Immediate,
            error_policies: ErrorPolicies::handled_by_log(),
            mode: AckMode::Attached,
            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        }
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy {
            backoff: "100ms".to_string(),
            max_backoff: "1s".to_string(),
        }
    }

    fn input_schema() -> arrow_schema::Schema {
        arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "seq",
            arrow_schema::DataType::Int64,
            false,
        )])
    }

    #[test]
    fn direct_emitter_lowers_only_its_filter_and_codec_emitter_preserves_empty_route() {
        let mut direct = emitter(
            EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter {
                mode: nervix_models::HttpRequestPublishingMode::Ack {
                    retry_policy: retry_policy(),
                },
                client: named::<ClientName>("api"),
                method: expression("'POST'"),
                path: expression("'/events'"),
            })),
            EmitterBody::WithoutBody,
        );
        direct.construction.where_clause = Some(expression("input.seq > 0"));
        let route = EmitterExecutionPlan::route(&direct, &input_schema(), &input_schema())
            .expect("the direct predicate should lower")
            .expect("the predicate produces a route program");
        assert!(!route.codec_route);
        assert!(route.program.inner.filter.is_some());
        assert_eq!(route.inherited_count, 0);

        *direct.connector = EmitterEntity::ZeroMq(ZeroMqEmitter::Push(ZeroMqPushEmitter {
            mode: nervix_models::ZeroMqPushPublishingMode::NoAck {
                retry_policy: retry_policy(),
            },
            client: named("emitter"),
        }));
        direct.body = EmitterBody::Codec {
            codec: named("codec"),
        };
        direct.construction = RouteConstruction::default();
        assert!(
            EmitterExecutionPlan::route(&direct, &input_schema(), &input_schema())
                .expect("an empty codec route is valid")
                .is_none()
        );
    }

    #[test]
    fn http_request_fields_are_lowered_beside_the_route() {
        let http = emitter(
            EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter {
                mode: nervix_models::HttpRequestPublishingMode::Ack {
                    retry_policy: retry_policy(),
                },
                client: named("api"),
                method: expression("'POST'"),
                path: expression("'/events'"),
            })),
            EmitterBody::WithoutBody,
        );
        let request = EmitterExecutionPlan::http_request(&http)
            .expect("HTTP fields should lower")
            .expect("HTTP has request fields");
        assert_eq!(request.inner.set.len(), 2);
        assert_eq!(request.inner.set[0].0.field, "method");
        assert_eq!(request.inner.set[1].0.field, "path");
        assert!(
            EmitterExecutionPlan::route(&http, &input_schema(), &input_schema())
                .expect("HTTP has a valid empty route")
                .is_none()
        );
    }

    #[test]
    fn sqs_ordering_group_is_lowered_or_reads_the_branch() {
        let mut sqs = emitter(
            EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
                mode: nervix_models::SqsQueuePublishingMode::Single {
                    retry_policy: retry_policy(),
                },
                client: named("queue_client"),
                queue: named("events.fifo"),
                fifo_group: Some(SqsFifoGroup::Expression(expression("'group'"))),
            })),
            EmitterBody::Codec {
                codec: named("codec"),
            },
        );
        let group = EmitterExecutionPlan::ordering_group(&sqs, &input_schema())
            .expect("the group expression should lower")
            .expect("SQS has a group");
        let EmitterOrderingGroupPlan::Expression(program) = group else {
            panic!("the group is an expression");
        };
        assert_eq!(program.inner.set[0].0.field, ORDERING_GROUP_FIELD);
        let EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter { fifo_group, .. })) =
            sqs.connector.as_mut()
        else {
            panic!("the fixture emitter is SQS");
        };
        *fifo_group = Some(SqsFifoGroup::FromBranch);
        assert_eq!(
            EmitterExecutionPlan::ordering_group(&sqs, &input_schema())
                .expect("the branch group should plan"),
            Some(EmitterOrderingGroupPlan::FromBranch),
        );
    }

    fn zero_mq(body: EmitterBody) -> CreateEmitter {
        emitter(
            EmitterEntity::ZeroMq(ZeroMqEmitter::Push(ZeroMqPushEmitter {
                mode: nervix_models::ZeroMqPushPublishingMode::NoAck {
                    retry_policy: retry_policy(),
                },
                client: named("emitter"),
            })),
            body,
        )
    }

    fn codec_body() -> EmitterBody {
        EmitterBody::Codec {
            codec: named("codec"),
        }
    }

    #[test]
    fn an_envelope_plans_its_fixed_clauses_and_one_program_computing_its_values_in_order() {
        let mut enveloped = zero_mq(codec_body());
        *enveloped.envelope = nervix_models::EmitterEnvelope {
            persistent: true,
            deliver_after: Some("5s".to_string()),
            timestamp: Some(expression("now()")),
            message_id: Some(expression("'event-1'")),
            ..nervix_models::EmitterEnvelope::default()
        };

        let plan = EmitterEnvelopePlan::decide(&enveloped, false)
            .expect("the envelope plans")
            .expect("the emitter writes an envelope");

        assert_eq!(
            plan.fixed,
            RecordEnvelope {
                persistent: true,
                deliver_after: Some(std::time::Duration::from_secs(5)),
                ..RecordEnvelope::default()
            }
        );
        let computed = plan.computed.expect("the envelope computes two values");
        assert_eq!(
            computed.targets,
            vec![
                EnvelopeTarget::Clause(EnvelopeClauseKind::Timestamp),
                EnvelopeTarget::Clause(EnvelopeClauseKind::MessageId),
            ]
        );
        let fields = computed
            .schema
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field.ty.clone(), field.optional))
            .collect::<Vec<_>>();
        assert_eq!(
            fields,
            vec![
                ("value_0", nervix_models::ParseAsType::Datetime, false),
                ("value_1", nervix_models::ParseAsType::String, false),
            ]
        );
        assert_eq!(computed.program.inner.set.len(), 2);
        assert!(!computed.reads_output);
    }

    /// A RabbitMQ exchange emitter's routing key is computed by the envelope program, first, so a
    /// record carries it with its envelope even where its statement writes no envelope clause.
    #[test]
    fn a_routing_key_is_the_first_value_the_envelope_program_computes() {
        let exchange = |envelope: nervix_models::EmitterEnvelope| {
            let mut emitter = emitter(
                EmitterEntity::RabbitMq(nervix_models::RabbitMqEmitter::Exchange(
                    nervix_models::RabbitMqExchangeEmitter {
                        mode: nervix_models::RabbitMqExchangePublishingMode::NoAck {
                            retry_policy: retry_policy(),
                        },
                        client: named("emitter"),
                        exchange: nervix_models::RabbitMqExchangeName::parse("events")
                            .expect("events is an exchange name"),
                        routing_key: expression("input.category"),
                    },
                )),
                codec_body(),
            );
            *emitter.envelope = envelope;
            emitter
        };

        let routed = EmitterEnvelopePlan::decide(
            &exchange(nervix_models::EmitterEnvelope::default()),
            false,
        )
        .expect("the routing key plans")
        .expect("the emitter computes a routing key for every record");
        assert_eq!(routed.fixed, RecordEnvelope::default());
        let computed = routed
            .computed
            .expect("the routing key is a computed value");
        assert_eq!(computed.targets, vec![EnvelopeTarget::RoutingKey]);
        assert_eq!(
            computed.schema.fields[0].ty,
            nervix_models::ParseAsType::String
        );

        let mandatory = EmitterEnvelopePlan::decide(
            &exchange(nervix_models::EmitterEnvelope {
                mandatory: true,
                ..nervix_models::EmitterEnvelope::default()
            }),
            true,
        )
        .expect("the routing key and MANDATORY plan")
        .expect("the emitter writes an envelope");
        assert!(mandatory.fixed.mandatory);
        let computed = mandatory
            .computed
            .expect("the routing key is a computed value");
        assert_eq!(computed.targets, vec![EnvelopeTarget::RoutingKey]);
        assert!(computed.reads_output);
    }

    #[test]
    fn an_emitter_without_an_envelope_plans_none_and_one_that_only_flags_computes_nothing() {
        let plain = zero_mq(codec_body());
        assert_eq!(
            EmitterEnvelopePlan::decide(&plain, false).expect("an empty envelope plans"),
            None
        );

        let mut flagged = zero_mq(codec_body());
        flagged.envelope.retain = true;
        let plan = EmitterEnvelopePlan::decide(&flagged, true)
            .expect("a flag plans")
            .expect("the emitter writes an envelope");
        assert!(plan.fixed.retain);
        assert_eq!(plan.computed, None);
    }

    #[test]
    fn a_property_its_entity_does_not_declare_is_not_planned() {
        let mut enveloped = zero_mq(codec_body());
        enveloped
            .envelope
            .properties
            .push(nervix_models::EnvelopeProperty {
                name: "priority".to_string(),
                expression: expression("1"),
            });

        let refused = EmitterEnvelopePlan::decide(&enveloped, false)
            .expect_err("ZEROMQ PUSH declares no property");

        assert!(matches!(
            refused.current_context(),
            EmitterExecutionPlanError::Envelope { .. }
        ));
    }

    #[test]
    fn an_envelope_reads_output_exactly_where_the_route_constructs_the_published_record() {
        let seq = FieldName::parse("seq").expect("seq is a valid field name");
        let constructed = RouteConstruction {
            assignments: vec![Assignment {
                target: AssignmentTarget::bare(seq),
                value: expression("input.seq"),
            }],
            ..RouteConstruction::default()
        };
        let http = |body| {
            emitter(
                EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter {
                    mode: nervix_models::HttpRequestPublishingMode::Ack {
                        retry_policy: retry_policy(),
                    },
                    client: named::<ClientName>("api"),
                    method: expression("'POST'"),
                    path: expression("'/events'"),
                })),
                body,
            )
        };
        let mut built = zero_mq(codec_body());
        built.construction = constructed;
        let mut filtered = http(EmitterBody::WithoutBody);
        filtered.construction.where_clause = Some(expression("input.seq > 0"));
        let cases = [
            (
                "a codec emitter without a construction",
                zero_mq(codec_body()),
                false,
            ),
            ("a codec emitter that constructs its output", built, true),
            (
                "an HTTP emitter without a body",
                http(EmitterBody::WithoutBody),
                false,
            ),
            (
                "an HTTP emitter that encodes its body",
                http(codec_body()),
                true,
            ),
            ("an HTTP emitter that only filters", filtered, false),
        ];
        // The record an HTTP emitter's empty construction builds leaves its optional field unset.
        let output = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "seq",
            arrow_schema::DataType::Int64,
            true,
        )]);
        for (case, emitter, constructs) in cases {
            let route = EmitterExecutionPlan::route(&emitter, &input_schema(), &output)
                .unwrap_or_else(|error| panic!("{case} plans its route: {error:?}"));
            let route_constructs = match route {
                Some(route) => route.codec_route,
                None => false,
            };
            assert_eq!(route_constructs, constructs, "{case}");
            assert_eq!(
                EmitterExecutionPlan::constructs_output(&emitter),
                constructs,
                "{case}"
            );
        }
    }
}
