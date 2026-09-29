//! The typed execution decision for every emitter in one scheduled domain.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving sink clients and source relays, preserving their delivery modes, and
//!   lowering emitter route, source, HTTP request, and ordering expressions before runtime binding.
//! - **Depends on.** Validated schedule Models, the activation plan's schemas, the sink start
//!   decision, and the expression VM frontend.
//! - **Must not know.** Tokio, locks, connector I/O, node-local resources, or task spawning.

use std::{collections::BTreeMap, sync::Arc as StdArc};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AckMode, Assignment, AssignmentTarget, CodecName, CreateEmitter, EmitterName, ErrorPolicies,
    Expression, FieldName, FlushPolicy, InputCollectPolicy, MaterializedStateDependency, Model,
    ModelKind, ModelName, NodeRef, RelayName, RouteConstruction, ScheduledNodes, SqsFifoGroup,
};
use nervix_vm::{
    SemanticScopePolicy, lower_route_construction, lower_transforming_route,
    program::{FunctionName, Program, SpannedNode},
};
use thiserror::Error;
use triomphe::Arc;

use crate::{
    emitter_start_plan::{DeclaredClientConfig, EmitterClientModels, EmitterStartPlan},
    registry::{DomainActivationPlan, LoweredFilter},
    runtime_schema::CompiledSchema,
};

const HTTP_REQUEST_NAMESPACE: &str = "http_request";
const ORDERING_GROUP_FIELD: &str = "ordering_group";

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
    #[error("emitter '{emitter}' cannot plan its sink")]
    Sink { emitter: EmitterName },
    #[error("emitter '{emitter}' has an invalid source predicate for '{relay}'")]
    SourceFilter {
        emitter: EmitterName,
        relay: RelayName,
    },
    #[error("emitter '{emitter}' has an invalid route construction")]
    Route { emitter: EmitterName },
    #[error("direct emitter '{emitter}' supports VALUES and WHERE only")]
    DirectConstruction { emitter: EmitterName },
    #[error("{sink} emitter '{emitter}' does not support FILTER-MAP headers")]
    Headers {
        emitter: EmitterName,
        sink: &'static str,
    },
    #[error("HTTP emitter '{emitter}' has invalid request fields")]
    HttpRequest { emitter: EmitterName },
    #[error("emitter '{emitter}' has an invalid ordering group expression")]
    OrderingGroup { emitter: EmitterName },
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

/// The source-row group that the host evaluates before handing an SQS record to its sink.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EmitterOrderingGroupPlan {
    FromBranch,
    Expression(SpannedNode<Program>),
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
    /// The validated schema of the rows handed to this emitter's sink.
    pub(crate) output_schema: Arc<CompiledSchema>,
    pub(crate) sink: EmitterStartPlan<DeclaredClientConfig>,
    pub(crate) route: Option<EmitterRoutePlan>,
    pub(crate) http_request: Option<SpannedNode<Program>>,
    pub(crate) ordering_group: Option<EmitterOrderingGroupPlan>,
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
        let client = match emitter.sink.client() {
            Some(name) => nodes
                .get(&NodeRef::new(ModelKind::Client, name))
                .map(|node| node.config.as_ref()),
            None => None,
        };
        let catalog_client = match emitter.sink.catalog_client() {
            Some(catalog) => nodes
                .get(&NodeRef::new(ModelKind::Client, catalog))
                .map(|node| node.config.as_ref()),
            None => None,
        };
        let sink = EmitterStartPlan::decide(
            emitter,
            EmitterClientModels {
                client,
                catalog_client,
            },
        )
        .change_context(EmitterExecutionPlanError::Sink {
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
        let output_schema = match emitter.sink.as_ref() {
            nervix_models::EmitSink::Client { schema } => activation
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
        Ok(Self {
            name: emitter.name.clone(),
            mode: emitter.mode,
            inputs,
            collect_policy: emitter.from.collect_policy.clone(),
            codec,
            output_schema,
            sink,
            route,
            http_request,
            ordering_group,
            flush_policy: emitter.flush_policy.clone(),
            error_policies: emitter.error_policies.clone(),
            materialized_state: emitter.materialized_state.clone(),
        })
    }

    pub(crate) fn route(
        emitter: &CreateEmitter,
        input: &arrow_schema::Schema,
        output: &arrow_schema::Schema,
    ) -> error_stack::Result<Option<EmitterRoutePlan>, EmitterExecutionPlanError> {
        let codec_route = emitter.body.codec().is_some()
            || matches!(emitter.body, nervix_models::EmitterBody::Client);
        if emitter.construction.is_empty()
            && !matches!(emitter.sink.as_ref(), nervix_models::EmitSink::Http { .. })
        {
            return Ok(None);
        }
        let construction = match emitter.sink.as_ref() {
            nervix_models::EmitSink::Http { .. } => RouteConstruction {
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
            && !emitter.sink.capabilities().writes_headers()
        {
            return Err(Report::new(EmitterExecutionPlanError::Headers {
                emitter: emitter.name.clone(),
                sink: emitter.sink.transport_label(),
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
        let nervix_models::EmitSink::Http { method, path, .. } = emitter.sink.as_ref() else {
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
        let nervix_models::EmitSink::Sqs {
            fifo_group: Some(group),
            ..
        } = emitter.sink.as_ref()
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
mod tests {
    use nervix_models::{
        AckMode, ClientName, EmitSink, EmitterBody, EmitterPublishingMode, ErrorPolicies,
        FlushPolicy, ProcessorInputs, RetryPolicy,
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
        nervix_nspl::parse_expression(raw).expect("the fixture uses valid expressions")
    }

    fn emitter(sink: EmitSink, body: EmitterBody) -> CreateEmitter {
        CreateEmitter {
            name: named("output"),
            from: ProcessorInputs::single(named("input_relay")),
            body,
            sink: Box::new(sink),
            batch: None,
            flush_policy: FlushPolicy::Immediate,
            error_policies: ErrorPolicies::handled_by_log(),
            publishing_mode: EmitterPublishingMode::NoAck {
                retry_policy: RetryPolicy {
                    backoff: "100ms".to_string(),
                    max_backoff: "1s".to_string(),
                },
            },
            mode: AckMode::Attached,
            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
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
            EmitSink::Http {
                client: named::<ClientName>("api"),
                method: expression("'POST'"),
                path: expression("'/events'"),
            },
            EmitterBody::WithoutBody,
        );
        direct.construction.where_clause = Some(expression("input.seq > 0"));
        let route = EmitterExecutionPlan::route(&direct, &input_schema(), &input_schema())
            .expect("the direct predicate should lower")
            .expect("the predicate produces a route program");
        assert!(!route.codec_route);
        assert!(route.program.inner.filter.is_some());
        assert_eq!(route.inherited_count, 0);

        *direct.sink = EmitSink::ZeroMq {
            client: named("sink"),
        };
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
            EmitSink::Http {
                client: named("api"),
                method: expression("'POST'"),
                path: expression("'/events'"),
            },
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
            EmitSink::Sqs {
                client: named("queue_client"),
                queue: "events.fifo".to_string(),
                fifo_group: Some(SqsFifoGroup::Expression(expression("'group'"))),
            },
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
        let EmitSink::Sqs { fifo_group, .. } = sqs.sink.as_mut() else {
            panic!("the fixture sink is SQS");
        };
        *fifo_group = Some(SqsFifoGroup::FromBranch);
        assert_eq!(
            EmitterExecutionPlan::ordering_group(&sqs, &input_schema())
                .expect("the branch group should plan"),
            Some(EmitterOrderingGroupPlan::FromBranch),
        );
    }
}
