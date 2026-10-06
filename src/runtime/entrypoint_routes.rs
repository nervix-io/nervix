//! Ingestor and reingestor programs bound to this node's runtime.
//!
//! Layer: data plane.
//!
//! - **Owns.** Binding the planned filters, route constructions and branch constructions of
//!   ingestors and reingestors to this node's relay schemas, lookups, materialized state and UDFs,
//!   and preparing the branched entrypoints their routes feed, before a task runs them, and how a
//!   bound route gives its records their branch key.
//! - **Depends on.** Decision-layer entrypoint plans, the installed domain surfaces, the VM
//!   bindings and the runtime planner that parses route cadences.
//! - **Must not know.** NSPL text, the schedule, registry validation, or placement. Flush and
//!   input-collection cadences and materialized-state dependencies still arrive as their declared
//!   Model values, which the decisions layer does not yet type.

use error_stack::ResultExt as _;

use super::*;

/// The program of an ingestor or reingestor that one binding failure concerns.
#[derive(Debug, Clone, PartialEq, Eq, strum::Display)]
pub(crate) enum EntrypointProgram {
    #[strum(to_string = "FILTER WHERE")]
    FilterWhere,
    #[strum(to_string = "FROM WHERE of input relay '{relay}'")]
    SourceWhere { relay: RelayName },
    #[strum(to_string = "route to '{relay}'")]
    Route { relay: RelayName },
    #[strum(to_string = "branch construction of the route to '{relay}'")]
    Branch { relay: RelayName },
}

/// Why an ingestor or reingestor could not be bound to this node's runtime.
#[derive(Debug, Error)]
pub(crate) enum EntrypointBindingError {
    #[error("{kind:?} '{node}' uses relay '{relay}', which this node has not instantiated")]
    MissingRelay {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error(
        "reingestor '{node}' reads relay '{relay}', whose branching this node has not resolved"
    )]
    MissingInputBranching { node: ModelName, relay: RelayName },
    #[error(
        "{kind:?} '{node}' constructs a branch key for relay '{relay}', which is unbranched on \
         this node"
    )]
    MissingBranchSchema {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("{kind:?} '{node}' {program} could not be compiled")]
    CompileProgram {
        kind: ModelKind,
        node: ModelName,
        program: EntrypointProgram,
    },
    #[error("{kind:?} '{node}' route to '{relay}' could not be prepared on this node")]
    PrepareRoute {
        kind: ModelKind,
        node: ModelName,
        relay: RelayName,
    },
    #[error("reingestor '{node}' input relay '{relay}' could not be prepared on this node")]
    PrepareInput { node: ModelName, relay: RelayName },
}

/// The ingestor or reingestor one binding compiles programs for.
#[derive(Clone, Copy)]
struct EntrypointTarget<'a> {
    kind: ModelKind,
    node: &'a ModelName,
}

impl EntrypointTarget<'_> {
    fn compile_failure(self, program: EntrypointProgram) -> EntrypointBindingError {
        EntrypointBindingError::CompileProgram {
            kind: self.kind,
            node: self.node.clone(),
            program,
        }
    }
}

/// How the records of one bound route receive their outgoing branch key.
#[derive(Debug, Clone)]
pub(super) enum BoundRouteBranch {
    /// The records leave unbranched.
    Unbranched,
    /// The records keep the branch key their input arrived with.
    Preserved,
    /// The records receive the branch key this program constructs.
    Constructed(CompiledBranchProgram),
}

/// One ingestor or reingestor output route, compiled for this node.
#[derive(Debug, Clone)]
pub(super) struct BoundEntryRoute {
    pub(super) relay: RelayName,
    pub(super) program: CompiledProgramWithMaterializedInterest,
    pub(super) branch: BoundRouteBranch,
    pub(super) message_error_policy: MessageErrorPolicy,
}

/// The output routes of one running ingestor, compiled once when it starts and shared by every
/// source instance, request and ingest group that dispatches through them.
#[derive(Debug)]
pub(super) struct BoundIngestorRoutes {
    /// Every route in declared order, which is never empty.
    pub(super) routes: Vec<BoundEntryRoute>,
}

/// An ingestor's node filter and routes, compiled for this node.
pub(super) struct BoundIngestorPrograms {
    pub(super) filter_where: Option<CompiledProgramWithMaterializedInterest>,
    pub(super) routes: Arc<BoundIngestorRoutes>,
}

/// One reingestor output route, compiled for this node, with the cadence its task buffers it on.
#[derive(Debug, Clone)]
pub(super) struct BoundReingestorRoute {
    pub(super) route: BoundEntryRoute,
    pub(super) flush_policy: RuntimeFlushPolicy,
}

/// One relay a reingestor reads on this node, with everything its task runs compiled.
pub(super) struct BoundReingestorInput {
    pub(super) reingestor: ReingestorName,
    pub(super) mode: AckMode,
    pub(super) relay: RelayName,
    pub(super) collect_policy: Option<RuntimeInputCollectPolicy>,
    pub(super) from_where: Option<CompiledProgramWithMaterializedInterest>,
    pub(super) filter_where: Option<CompiledProgramWithMaterializedInterest>,
    pub(super) materialized_state: Vec<nervix_models::MaterializedStateDependency>,
    /// Every route in declared order, which is never empty.
    pub(super) routes: Vec<BoundReingestorRoute>,
}

impl ExecutionBuildDeps<'_> {
    fn compile_context<'b>(
        &'b self,
        current_branching: &'b ResolvedBranching,
    ) -> RuntimeVmCompileContext<'b> {
        RuntimeVmCompileContext {
            available_materialized_streams: self.materialized_relay_specs,
            available_lookups: self.lookups,
            current_branching,
            udfs: self.udfs,
        }
    }

    fn relay_schema(
        &self,
        target: EntrypointTarget<'_>,
        relay: &RelayName,
    ) -> error_stack::Result<Arc<CompiledSchema>, EntrypointBindingError> {
        let Some(schema) = self.relay_schemas.get(relay) else {
            return Err(Report::new(EntrypointBindingError::MissingRelay {
                kind: target.kind,
                node: target.node.clone(),
                relay: relay.clone(),
            }));
        };
        Ok(schema.clone())
    }

    /// Compiles the branch construction of one route whose records leave for `relay`.
    fn bind_route_branch(
        &self,
        target: EntrypointTarget<'_>,
        branch: &PlannedRouteBranch,
        schemas: RouteBranchSchemas,
        context: RuntimeVmCompileContext<'_>,
    ) -> error_stack::Result<BoundRouteBranch, EntrypointBindingError> {
        let RouteBranchSchemas {
            relay,
            input,
            output,
        } = schemas;
        let lowered = match branch {
            PlannedRouteBranch::Unbranched => return Ok(BoundRouteBranch::Unbranched),
            PlannedRouteBranch::Preserved(_) => return Ok(BoundRouteBranch::Preserved),
            PlannedRouteBranch::Constructed { program, .. } => program,
        };
        let Some(branch_schema) = self
            .relay_branchings
            .get(&relay)
            .and_then(RuntimeVmSchema::from_branching)
        else {
            return Err(Report::new(EntrypointBindingError::MissingBranchSchema {
                kind: target.kind,
                node: target.node.clone(),
                relay,
            }));
        };
        let program = bind_output_branch_program(
            target.node,
            lowered.program(),
            input,
            output,
            branch_schema,
            context,
        )
        .map_err(|error| {
            error.change_context(target.compile_failure(EntrypointProgram::Branch {
                relay: relay.clone(),
            }))
        })?;
        Ok(BoundRouteBranch::Constructed(program))
    }

    /// Compiles an ingestor's node filter and routes against its input schema: what its codec
    /// decodes a transport's payloads into, or the schema a client source's batches carry.
    pub(super) fn bind_ingestor(
        &self,
        ingestor: &IngestorSpec,
        input_schema: &CompiledSchema,
    ) -> error_stack::Result<BoundIngestorPrograms, EntrypointBindingError> {
        let identifier = ModelName::from(&ingestor.name);
        let target = EntrypointTarget {
            kind: ModelKind::Ingestor,
            node: &identifier,
        };
        let input = RuntimeVmSchema {
            schema: input_schema.arrow_schema(),
            sensitivity: input_schema.vm_sensitivity(),
        };
        let unbranched = ResolvedBranching::unbranched();
        let filter_where = match ingestor.filter_where.as_ref() {
            Some(filter) => Some(
                bind_scoped_filter_program(
                    &identifier,
                    filter.program(),
                    input.clone(),
                    MessageErrorOperation::FilterWhere,
                    self.compile_context(&unbranched),
                    RuntimeFilterScope::Source {
                        namespace: "input",
                        allow_header_reads: ingestor.reads_headers(),
                        allow_metadata: ingestor.reads_headers(),
                    },
                )
                .map_err(|error| {
                    error.change_context(target.compile_failure(EntrypointProgram::FilterWhere))
                })?,
            ),
            None => None,
        };
        let mut routes = Vec::with_capacity(ingestor.routes.len());
        for route in &ingestor.routes {
            let relay = &route.relay;
            let output_schema = self.relay_schema(target, relay)?;
            let output = RuntimeVmSchema {
                schema: output_schema.arrow_schema(),
                sensitivity: output_schema.vm_sensitivity(),
            };
            let program = bind_ingestor_filter_map_program(
                &identifier,
                ingestor.metadata_kind(),
                ingestor.reads_headers(),
                &route.construction,
                RuntimeVmSchemaPair {
                    input: input.schema.clone(),
                    input_sensitivity: input.sensitivity.clone(),
                    output: output.schema.clone(),
                    output_sensitivity: output.sensitivity.clone(),
                },
                self.compile_context(&unbranched),
            )
            .map_err(|error| {
                error.change_context(target.compile_failure(EntrypointProgram::Route {
                    relay: relay.clone(),
                }))
            })?;
            let branch = self.bind_route_branch(
                target,
                &route.branch,
                RouteBranchSchemas {
                    relay: relay.clone(),
                    input: input.clone(),
                    output,
                },
                self.compile_context(&unbranched),
            )?;
            routes.push(BoundEntryRoute {
                relay: relay.clone(),
                program,
                branch,
                message_error_policy: route.message_error_policy().clone(),
            });
        }
        Ok(BoundIngestorPrograms {
            filter_where,
            routes: Arc::new(BoundIngestorRoutes { routes }),
        })
    }

    /// Compiles everything one reingestor runs for the messages of one input relay.
    pub(super) fn bind_reingestor_input(
        &self,
        reingestor: &ReingestorPlan,
        input: &ReingestorInputPlan,
    ) -> error_stack::Result<BoundReingestorInput, EntrypointBindingError> {
        let identifier = ModelName::from(&reingestor.name);
        let target = EntrypointTarget {
            kind: ModelKind::Reingestor,
            node: &identifier,
        };
        let input_schema = self.relay_schema(target, &input.relay)?;
        let input_vm = RuntimeVmSchema {
            schema: input_schema.arrow_schema(),
            sensitivity: input_schema.vm_sensitivity(),
        };
        let Some(current_branching) = self.relay_branchings.get(&input.relay) else {
            return Err(Report::new(EntrypointBindingError::MissingInputBranching {
                node: identifier.clone(),
                relay: input.relay.clone(),
            }));
        };
        let source_scope = RuntimeFilterScope::Source {
            namespace: "input",
            allow_header_reads: false,
            allow_metadata: false,
        };
        let from_where = match input.from_where.as_ref() {
            Some(filter) => Some(
                bind_scoped_filter_program(
                    &identifier,
                    filter.program(),
                    input_vm.clone(),
                    MessageErrorOperation::SourceWhere,
                    self.compile_context(current_branching),
                    source_scope,
                )
                .map_err(|error| {
                    error.change_context(target.compile_failure(EntrypointProgram::SourceWhere {
                        relay: input.relay.clone(),
                    }))
                })?,
            ),
            None => None,
        };
        let filter_where = match reingestor.filter_where.as_ref() {
            Some(filter) => Some(
                bind_scoped_filter_program(
                    &identifier,
                    filter.program(),
                    input_vm.clone(),
                    MessageErrorOperation::FilterWhere,
                    self.compile_context(current_branching),
                    source_scope,
                )
                .map_err(|error| {
                    error.change_context(target.compile_failure(EntrypointProgram::FilterWhere))
                })?,
            ),
            None => None,
        };
        let mut routes = Vec::with_capacity(reingestor.routes.len());
        for route in &reingestor.routes {
            let relay = &route.relay;
            let output_schema = self.relay_schema(target, relay)?;
            let output = RuntimeVmSchema {
                schema: output_schema.arrow_schema(),
                sensitivity: output_schema.vm_sensitivity(),
            };
            let set_operations = route.construction.set_operations();
            let program = bind_processor_output_filter_map_program(
                &identifier,
                std::slice::from_ref(&input.relay),
                relay,
                RouteProgram {
                    program: route.construction.program(),
                    set_operations: &set_operations,
                },
                RuntimeVmSchemaPair {
                    input: input_vm.schema.clone(),
                    input_sensitivity: input_vm.sensitivity.clone(),
                    output: output.schema.clone(),
                    output_sensitivity: output.sensitivity.clone(),
                },
                None,
                self.compile_context(current_branching),
            )
            .map_err(|error| {
                error.change_context(target.compile_failure(EntrypointProgram::Route {
                    relay: relay.clone(),
                }))
            })?;
            let branch = self.bind_route_branch(
                target,
                &route.branch,
                RouteBranchSchemas {
                    relay: relay.clone(),
                    input: input_vm.clone(),
                    output,
                },
                self.compile_context(current_branching),
            )?;
            let flush_policy = parse_branch_flush_policy(
                ModelKind::Reingestor,
                &identifier,
                relay,
                &route.flush_policy,
            )
            .change_context(EntrypointBindingError::PrepareRoute {
                kind: ModelKind::Reingestor,
                node: identifier.clone(),
                relay: relay.clone(),
            })?;
            routes.push(BoundReingestorRoute {
                route: BoundEntryRoute {
                    relay: relay.clone(),
                    program,
                    branch,
                    message_error_policy: route.message_error_policy().clone(),
                },
                flush_policy,
            });
        }
        let collect_policy = match reingestor.collect_policy.as_ref() {
            Some(policy) => Some(
                parse_input_collect_policy(
                    ModelKind::Reingestor,
                    &identifier,
                    &input.relay,
                    policy,
                )
                .change_context(EntrypointBindingError::PrepareInput {
                    node: identifier.clone(),
                    relay: input.relay.clone(),
                })?,
            ),
            None => None,
        };
        Ok(BoundReingestorInput {
            reingestor: reingestor.name.clone(),
            mode: reingestor.mode,
            relay: input.relay.clone(),
            collect_policy,
            from_where,
            filter_where,
            materialized_state: reingestor.materialized_state.clone(),
            routes,
        })
    }
}

/// The relay services of one domain execution, through which branched entrypoints publish.
#[derive(Clone, Copy)]
pub(super) struct RelayRuntimeHandles<'a> {
    pub(super) services: &'a HashMap<RelayName, Arc<RelayBoundaryServices>>,
}

impl RelayRuntimeHandles<'_> {
    /// The templates of the branched entrypoints the planned `routes` of the ingestor or
    /// reingestor `kind` `identifier` feed, keyed by the relay each route writes.
    pub(super) fn route_templates(
        &self,
        kind: ModelKind,
        identifier: &ModelName,
        routes: &[PlannedEntryRoute],
    ) -> error_stack::Result<HashMap<RelayName, IngestorRouteTemplate>, EntrypointBindingError>
    {
        let mut templates = HashMap::default();
        for route in routes {
            let template =
                materialize_ingestor_route_template(kind, identifier, route, self.services)
                    .change_context(EntrypointBindingError::PrepareRoute {
                        kind,
                        node: identifier.clone(),
                        relay: route.relay.clone(),
                    })?;
            templates.insert(route.relay.clone(), template);
        }
        Ok(templates)
    }
}

/// The schemas one route's branch construction reads and the relay whose branch it writes.
struct RouteBranchSchemas {
    relay: RelayName,
    input: RuntimeVmSchema,
    output: RuntimeVmSchema,
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        AckMode, CreateClientZeroMq, CreateIngestor, CreateReingestor, FlushPolicy,
        GeneralErrorPolicy, IngestQuiesceMode, IngestSource, Model, OutputBranch, ParseAsType,
        ProcessorInputWhere, ProcessorInputs, ProcessorOutputs, ZeroMqIngestMode,
    };

    use super::*;
    use crate::runtime::planning::PlanningError;

    const FIELDS: &[(&str, ParseAsType)] =
        &[("tenant", ParseAsType::String), ("value", ParseAsType::I64)];

    /// The node-local surfaces one binding test installs: every relay's schema and branching.
    struct NodeSurfaces {
        relay_schemas: HashMap<RelayName, Arc<CompiledSchema>>,
        relay_branchings: HashMap<RelayName, ResolvedBranching>,
        materialized: HashMap<RelayName, RuntimeMaterializedRelaySpec>,
        lookups: HashMap<LookupName, Arc<LookupRuntime>>,
    }

    impl NodeSurfaces {
        fn of(domain: &EntrypointTestDomain<'_>) -> Self {
            let mut relay_schemas = HashMap::default();
            let mut relay_branchings = HashMap::default();
            for relay in domain.relays {
                relay_schemas.insert(named(relay), domain.relay_schema());
                relay_branchings.insert(named(relay), domain.branching(relay));
            }
            Self {
                relay_schemas,
                relay_branchings,
                materialized: HashMap::default(),
                lookups: HashMap::default(),
            }
        }

        fn deps<'a>(&'a self, domain: &'a DomainName) -> ExecutionBuildDeps<'a> {
            ExecutionBuildDeps {
                domain,
                relay_schemas: &self.relay_schemas,
                relay_branchings: &self.relay_branchings,
                materialized_relay_specs: &self.materialized,
                lookups: &self.lookups,
                udfs: None,
            }
        }
    }

    fn keyed_domain() -> EntrypointTestDomain<'static> {
        EntrypointTestDomain {
            relays: &["keyed"],
            fields: FIELDS,
            branch_fields: &[("tenant", ParseAsType::String)],
        }
    }

    fn filtering_ingestor() -> Vec<Model> {
        vec![
            Model::ClientZeroMq(CreateClientZeroMq {
                name: named("zmq"),
                mount: None,
                config: Vec::new(),
            }),
            Model::Ingestor(CreateIngestor {
                name: named("source"),
                output_routes: with_inherit_all(ProcessorOutputs::single(named("keyed")))
                    .with_flush_policy(FlushPolicy::Immediate)
                    .with_branch(branched_by("keyed", &["tenant"])),
                input: nervix_models::IngestorInput::Transport(
                    nervix_models::TransportIngestorInput {
                        source: IngestSource::ZeroMq {
                            client: named("zmq"),
                            mode: ZeroMqIngestMode::NoAckSequential,
                            quiesce: IngestQuiesceMode::Suspend,
                        },
                        codec: named("payload_codec"),
                    },
                ),
                timestamp_source: None,
                general_error_policy: GeneralErrorPolicy::Log,
                filter_where: Some(expression("input.value > 1")),
            }),
        ]
    }

    fn planned_ingestor(fixture: &EntrypointTestDomain<'_>) -> Arc<IngestorStartPlan> {
        fixture
            .plan(&domain("default"), filtering_ingestor())
            .ingestor(&named("source"))
            .cloned()
            .assured("the fixture schedules the ingestor")
    }

    #[test]
    fn binds_an_ingestor_filter_and_the_branch_key_its_route_constructs() {
        let fixture = keyed_domain();
        let plan = planned_ingestor(&fixture);
        let surfaces = NodeSurfaces::of(&fixture);
        let domain = domain("default");

        let bound = surfaces
            .deps(&domain)
            .bind_ingestor(&plan.ingestor, &fixture.codec().schema())
            .assured("the planned ingestor binds");

        assert!(bound.filter_where.is_some());
        assert_eq!(bound.routes.routes.len(), 1);
        let route = &bound.routes.routes[0];
        assert_eq!(route.relay, named("keyed"));
        assert!(matches!(route.branch, BoundRouteBranch::Constructed(_)));
    }

    #[test]
    fn ingestor_binding_names_a_relay_this_node_has_not_instantiated() {
        let fixture = keyed_domain();
        let plan = planned_ingestor(&fixture);
        let mut surfaces = NodeSurfaces::of(&fixture);
        surfaces.relay_schemas.clear();
        let domain = domain("default");

        let error = surfaces
            .deps(&domain)
            .bind_ingestor(&plan.ingestor, &fixture.codec().schema())
            .err()
            .assured("a route relay the node lacks must not bind");

        assert!(matches!(
            error.current_context(),
            EntrypointBindingError::MissingRelay { kind: ModelKind::Ingestor, node, relay }
                if node == &named::<ModelName>("source") && relay == &named::<RelayName>("keyed")
        ));
    }

    #[test]
    fn ingestor_binding_names_a_branch_key_the_relay_does_not_have_here() {
        let fixture = keyed_domain();
        let plan = planned_ingestor(&fixture);
        let mut surfaces = NodeSurfaces::of(&fixture);
        surfaces
            .relay_branchings
            .insert(named("keyed"), ResolvedBranching::unbranched());
        let domain = domain("default");

        let error = surfaces
            .deps(&domain)
            .bind_ingestor(&plan.ingestor, &fixture.codec().schema())
            .err()
            .assured("a branch key the relay lacks must not bind");

        assert!(matches!(
            error.current_context(),
            EntrypointBindingError::MissingBranchSchema { relay, .. }
                if relay == &named::<RelayName>("keyed")
        ));
    }

    #[test]
    fn a_route_that_does_not_compile_on_this_node_names_its_program() {
        let fixture = keyed_domain();
        let plan = planned_ingestor(&fixture);
        let mut surfaces = NodeSurfaces::of(&fixture);
        // The node's relay no longer carries the field the planned route inherits into it.
        surfaces.relay_schemas.insert(
            named("keyed"),
            test_schema(&[("tenant", ParseAsType::String)]),
        );
        let domain = domain("default");

        let error = surfaces
            .deps(&domain)
            .bind_ingestor(&plan.ingestor, &fixture.codec().schema())
            .err()
            .assured("a route that does not compile must not bind");

        assert!(matches!(
            error.current_context(),
            EntrypointBindingError::CompileProgram {
                program: EntrypointProgram::Route { relay },
                ..
            } if relay == &named::<RelayName>("keyed")
        ));
        assert!(
            matches!(
                error.downcast_ref::<RuntimeVmCompileError>(),
                Some(RuntimeVmCompileError::CompileFilterMap { node }) if node.as_str() == "source"
            ),
            "the runtime binding must name the route's FILTER-MAP beneath the program: {error:#}"
        );
        assert!(
            error.contains::<nervix_vm::CompileError>(),
            "the VM's own compile failure must stay beneath the binding: {error:#}"
        );
    }

    fn repartitioning_domain() -> EntrypointTestDomain<'static> {
        EntrypointTestDomain {
            relays: &["incoming", "outgoing"],
            fields: FIELDS,
            branch_fields: &[],
        }
    }

    fn filtering_reingestor(flush_interval: &str, collect_for: &str) -> CreateReingestor {
        CreateReingestor {
            name: named("repartition"),
            from: ProcessorInputs::new(
                vec![named("incoming")],
                vec![ProcessorInputWhere {
                    relay: named("incoming"),
                    where_clause: expression("input.tenant != 'ignored'"),
                }],
            )
            .with_collect_policy(collect_for.to_string(), Some("1MiB".to_string())),
            output_routes: with_inherit_all(ProcessorOutputs::single(named("outgoing")))
                .with_flush_policy(FlushPolicy::Each {
                    interval: flush_interval.to_string(),
                    max_batch_size: "1MiB".to_string(),
                })
                .with_branch(OutputBranch::Unbranched),
            mode: AckMode::Detached,
            materialized_state: Vec::new(),
            filter_where: Some(expression("input.value > 10")),
        }
    }

    #[test]
    fn binds_a_reingestor_input_with_its_source_and_node_filters() {
        let fixture = repartitioning_domain();
        let domain = domain("default");
        let plan = fixture.plan_reingestor(&domain, filtering_reingestor("100ms", "25ms"));
        let surfaces = NodeSurfaces::of(&fixture);

        let bound = surfaces
            .deps(&domain)
            .bind_reingestor_input(&plan, &plan.inputs[0])
            .assured("the planned reingestor input binds");

        assert_eq!(bound.reingestor, named("repartition"));
        assert_eq!(bound.mode, AckMode::Detached);
        assert_eq!(bound.relay, named("incoming"));
        assert!(bound.from_where.is_some());
        assert!(bound.filter_where.is_some());
        assert!(bound.collect_policy.is_some());
        assert_eq!(bound.routes.len(), 1);
        let route = &bound.routes[0];
        assert_eq!(route.route.relay, named("outgoing"));
        assert!(matches!(route.route.branch, BoundRouteBranch::Unbranched));
        assert!(matches!(
            route.flush_policy,
            RuntimeFlushPolicy::Each { .. }
        ));
    }

    #[test]
    fn reingestor_binding_requires_the_branching_of_its_input() {
        let fixture = repartitioning_domain();
        let domain = domain("default");
        let plan = fixture.plan_reingestor(&domain, filtering_reingestor("100ms", "25ms"));
        let mut surfaces = NodeSurfaces::of(&fixture);
        surfaces.relay_branchings.clear();

        let error = surfaces
            .deps(&domain)
            .bind_reingestor_input(&plan, &plan.inputs[0])
            .err()
            .assured("an input without resolved branching must not bind");

        assert!(matches!(
            error.current_context(),
            EntrypointBindingError::MissingInputBranching { relay, .. }
                if relay == &named::<RelayName>("incoming")
        ));
    }

    #[test]
    fn reingestor_binding_names_the_route_or_input_whose_cadence_it_cannot_run() {
        let fixture = repartitioning_domain();
        let domain = domain("default");
        let surfaces = NodeSurfaces::of(&fixture);

        let flush = fixture.plan_reingestor(&domain, filtering_reingestor("soon", "25ms"));
        let error = surfaces
            .deps(&domain)
            .bind_reingestor_input(&flush, &flush.inputs[0])
            .err()
            .assured("an unparseable flush interval must not bind");
        assert!(matches!(
            error.current_context(),
            EntrypointBindingError::PrepareRoute { kind: ModelKind::Reingestor, relay, .. }
                if relay == &named::<RelayName>("outgoing")
        ));
        assert!(matches!(
            error.downcast_ref::<PlanningError>(),
            Some(PlanningError::InvalidFlushInterval { .. })
        ));

        let collect = fixture.plan_reingestor(&domain, filtering_reingestor("100ms", "later"));
        let error = surfaces
            .deps(&domain)
            .bind_reingestor_input(&collect, &collect.inputs[0])
            .err()
            .assured("an unparseable collection interval must not bind");
        assert!(matches!(
            error.current_context(),
            EntrypointBindingError::PrepareInput { node, relay }
                if node == &named::<ModelName>("repartition")
                    && relay == &named::<RelayName>("incoming")
        ));
        assert!(matches!(
            error.downcast_ref::<PlanningError>(),
            Some(PlanningError::InvalidCollectInterval { .. })
        ));
    }

    /// The relay services of the repartitioning domain's relays.
    struct RepartitioningRelays {
        services: HashMap<RelayName, Arc<RelayBoundaryServices>>,
    }

    impl RepartitioningRelays {
        fn new() -> Self {
            let mut services = HashMap::default();
            for relay in ["incoming", "outgoing"] {
                services.insert(named::<RelayName>(relay), test_relay_boundary_services());
            }
            Self { services }
        }

        fn handles(&self) -> RelayRuntimeHandles<'_> {
            RelayRuntimeHandles {
                services: &self.services,
            }
        }

        fn deferred_input(&self, plan: &Arc<ReingestorPlan>) -> PlannedReingestorInput {
            PlannedReingestorInput {
                plan: plan.clone(),
                input: plan.inputs[0].clone(),
                consumer: ReingestorInputConsumer::Deferred(
                    self.services[&named::<RelayName>("incoming")].clone(),
                ),
            }
        }

        fn incoming_consumers(&self) -> usize {
            self.services[&named::<RelayName>("incoming")]
                .detached_runtime_consumer_count
                .load(Ordering::Acquire)
        }
    }

    #[nervix_primitives::test]
    async fn reingestor_runtimes_start_one_entrypoint_set_per_reingestor() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let fixture = repartitioning_domain();
        let plan = fixture.plan_reingestor(&domain, filtering_reingestor("100ms", "25ms"));
        let surfaces = NodeSurfaces::of(&fixture);
        let relays = RepartitioningRelays::new();
        let (shutdown_tx, _) = watch::channel(false);

        let runtimes = runtime
            .start_reingestor_runtimes(
                surfaces.deps(&domain),
                &shutdown_tx,
                relays.handles(),
                vec![relays.deferred_input(&plan)],
            )
            .assured("the planned reingestor starts");

        assert_eq!(relays.incoming_consumers(), 1);
        assert_eq!(
            runtimes.branched_entrypoints[&named::<ModelName>("repartition")].len(),
            1
        );
        assert_eq!(
            runtimes.tasks[&NodeRef::new(ModelKind::Reingestor, named::<ModelName>("repartition"))]
                .len(),
            1
        );
        for runtimes in runtimes.branched_entrypoints.into_values() {
            for runtime in runtimes {
                runtime.shutdown().await;
            }
        }
        for task in runtimes.tasks.into_values().flatten() {
            task.abort();
        }
    }

    #[nervix_primitives::test]
    async fn a_reingestor_start_that_fails_registers_no_consumer_and_starts_nothing() {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let fixture = repartitioning_domain();
        let surfaces = NodeSurfaces::of(&fixture);
        let (shutdown_tx, _) = watch::channel(false);

        let mut relays = RepartitioningRelays::new();
        relays.services.remove(&named::<RelayName>("outgoing"));
        let plan = fixture.plan_reingestor(&domain, filtering_reingestor("100ms", "25ms"));
        let error = runtime
            .start_reingestor_runtimes(
                surfaces.deps(&domain),
                &shutdown_tx,
                relays.handles(),
                vec![relays.deferred_input(&plan)],
            )
            .err()
            .assured("an entrypoint without its relay services must not start");
        assert!(matches!(
            error.current_context(),
            EntrypointBindingError::PrepareRoute { relay, .. }
                if relay == &named::<RelayName>("outgoing")
        ));
        assert!(matches!(
            error.downcast_ref::<PlanningError>(),
            Some(PlanningError::MissingRelayServices { .. })
        ));
        assert_eq!(relays.incoming_consumers(), 0);

        let relays = RepartitioningRelays::new();
        let unbindable = fixture.plan_reingestor(&domain, filtering_reingestor("100ms", "later"));
        let error = runtime
            .start_reingestor_runtimes(
                surfaces.deps(&domain),
                &shutdown_tx,
                relays.handles(),
                vec![relays.deferred_input(&unbindable)],
            )
            .err()
            .assured("an input that does not bind must not start");
        assert!(matches!(
            error.current_context(),
            EntrypointBindingError::PrepareInput { .. }
        ));
        assert_eq!(relays.incoming_consumers(), 0);
        assert_eq!(shutdown_tx.receiver_count(), 0);
    }

    #[test]
    fn a_reingestor_program_that_does_not_compile_on_this_node_names_the_program() {
        let fixture = repartitioning_domain();
        let domain = domain("default");
        let plan = fixture.plan_reingestor(&domain, filtering_reingestor("100ms", "25ms"));
        // Each node surface no longer carries a field one of the planned programs reads.
        let cases = [
            (
                "incoming",
                test_schema(&[("value", ParseAsType::I64)]),
                EntrypointProgram::SourceWhere {
                    relay: named("incoming"),
                },
            ),
            (
                "incoming",
                test_schema(&[("tenant", ParseAsType::String)]),
                EntrypointProgram::FilterWhere,
            ),
            (
                "outgoing",
                test_schema(&[("tenant", ParseAsType::String)]),
                EntrypointProgram::Route {
                    relay: named("outgoing"),
                },
            ),
        ];
        for (relay, schema, expected) in cases {
            let mut surfaces = NodeSurfaces::of(&fixture);
            surfaces.relay_schemas.insert(named(relay), schema);
            let error = surfaces
                .deps(&domain)
                .bind_reingestor_input(&plan, &plan.inputs[0])
                .err()
                .assured("a program that does not compile must not bind");
            let EntrypointBindingError::CompileProgram { program, .. } = error.current_context()
            else {
                panic!("the binding must name the program that did not compile: {error:#}");
            };
            assert_eq!(program, &expected);
            assert!(
                error.contains::<nervix_vm::CompileError>(),
                "the VM's own compile failure must stay beneath the binding: {error:#}"
            );
        }
    }
}
