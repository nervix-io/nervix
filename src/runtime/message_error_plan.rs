//! Bound message-error routes for a domain routing revision.
//!
//! Layer: data plane.
//! - **Owns.** Binding the registry's typed error-route decisions to node-local relay services,
//!   lookup maps, UDFs and compiled VM programs before a revision is published.
//! - **Depends on.** Typed route specs, the VM and installed runtime capabilities.
//! - **Must not know.** Scheduled Models or how an error route was declared in NSPL.

use super::{message_error::MessageErrorHandlingError, vm_compile::RuntimeVmCompileContext, *};

#[derive(Clone, Default)]
pub(super) struct BoundMessageErrorRoutes {
    routes: HashMap<MessageErrorRouteKey, Arc<BoundMessageErrorRoute>>,
}

pub(super) struct BoundMessageErrorRoute {
    pub(super) key: MessageErrorRouteKey,
    pub(super) schema: Arc<CompiledSchema>,
    pub(super) target: MessageErrorRouteTarget,
    pub(super) branching: ResolvedBranching,
    pub(super) program: CompiledProgramWithMaterializedInterest,
    pub(super) flush_policy: Option<RuntimeFlushPolicy>,
    pub(super) delivery: Option<Arc<MessageErrorRouteRuntime>>,
}

pub(super) struct MessageErrorRouteBindingContext<'a> {
    pub(super) relay_services: &'a HashMap<RelayName, Arc<RelayBoundaryServices>>,
    pub(super) materialized_stream_specs: &'a HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    pub(super) lookups: &'a HashMap<LookupName, Arc<LookupRuntime>>,
    pub(super) udfs: &'a UdfExecutor,
}

impl BoundMessageErrorRoutes {
    pub(super) fn bind(
        specs: MessageErrorRouteSpecs,
        context: MessageErrorRouteBindingContext<'_>,
    ) -> error_stack::Result<Self, MessageErrorHandlingError> {
        let mut routes = HashMap::default();
        for spec in specs.routes {
            let key = spec.key;
            let relay = &key.error_relay;
            let services = context.relay_services.get(relay).cloned().ok_or_else(|| {
                error_stack::Report::new(MessageErrorHandlingError::DlqRelayNotInstantiated {
                    domain: key.domain.clone(),
                    relay: relay.clone(),
                })
            })?;
            let flush_policy = match spec.flush_policy.as_ref() {
                Some(policy) => Some(
                    Runtime::parse_runtime_node_flush_policy(
                        &key.domain,
                        key.node.kind.as_str(),
                        &key.node.identifier,
                        policy,
                    )
                    .map_err(|source| {
                        error_stack::Report::new(MessageErrorHandlingError::FlushPolicy {
                            node: key.node.clone(),
                            source,
                        })
                    })?,
                ),
                None => None,
            };
            let program = compile_message_error_set_program(
                &key.node.identifier,
                &spec.program,
                spec.output_schema.clone(),
                spec.compile_schemas,
                RuntimeVmCompileContext {
                    available_materialized_streams: context.materialized_stream_specs,
                    available_lookups: context.lookups,
                    current_branching: &spec.target_branching,
                    udfs: Some(context.udfs),
                },
            )
            .map_err(|error| {
                error_stack::Report::new(MessageErrorHandlingError::ProgramCompilation {
                    domain: key.domain.clone(),
                    node: key.node.clone(),
                    error_relay: relay.clone(),
                    error,
                })
            })?;
            let route = Arc::new(BoundMessageErrorRoute {
                key: key.clone(),
                schema: spec.output_schema,
                target: MessageErrorRouteTarget { services },
                branching: spec.target_branching,
                program,
                flush_policy,
                delivery: flush_policy.map(|_| MessageErrorRouteRuntime::prepare()),
            });
            if routes.insert(key.clone(), route).is_some() {
                return Err(error_stack::Report::new(
                    MessageErrorHandlingError::DuplicatePreparedRoute { route: key },
                ));
            }
        }
        Ok(Self { routes })
    }

    /// Only the successful running revision installs delivery workers. Binding is fallible and
    /// must not retire a worker belonging to the preceding published revision.
    pub(super) fn activate(&self, runtime: &Runtime, domain: &DomainName) {
        runtime.retire_unselected_message_error_routes(domain, &self.routes);
        for route in self.routes.values() {
            if route.flush_policy.is_some() {
                runtime.install_message_error_route(route);
            }
        }
    }

    pub(super) fn get(&self, key: &MessageErrorRouteKey) -> Option<Arc<BoundMessageErrorRoute>> {
        self.routes.get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use nervix_models::{CreateSchema, FlushPolicy, ModelKind, ModelName, SchemaName};

    use super::*;

    fn lowered_set(
        assignments: Vec<nervix_models::Assignment>,
    ) -> nervix_vm::program::SpannedNode<nervix_vm::program::Program> {
        lower_route_construction(
            &RouteConstruction {
                assignments,
                ..RouteConstruction::default()
            },
            SemanticScopePolicy::read_write("error_output", "error_output"),
        )
        .expect("the test assignments lower")
    }

    fn named<N>(raw: &str) -> N
    where
        N: TryFrom<String>,
        <N as TryFrom<String>>::Error: std::fmt::Debug,
    {
        N::try_from(raw.to_string()).expect("valid name")
    }

    fn spec(flush_policy: Option<FlushPolicy>) -> MessageErrorRouteSpec {
        MessageErrorRouteSpec {
            key: MessageErrorRouteKey {
                domain: named("sales"),
                node: NodeRef::new(ModelKind::Junction, named::<ModelName>("compute")),
                source_route: Some(named("out")),
                error_relay: named("errors"),
            },
            program: lowered_set(Vec::new()),
            output_schema: Arc::new(compile_schema(&CreateSchema {
                name: named::<SchemaName>("error_record"),
                fields: Vec::new(),
            })),
            target_branching: ResolvedBranching::unbranched(),
            compile_schemas: MessageErrorCompileSchemas::default(),
            flush_policy,
        }
    }

    fn services() -> Arc<RelayBoundaryServices> {
        Arc::new(RelayBoundaryServices::new(
            RelayBoundaryFanout::direct_with_capacity(
                NonZeroUsize::new(1).expect("nonzero test capacity"),
            ),
            0,
            0,
            Vec::new(),
            None,
            Arc::new(BranchPresence::new()),
        ))
    }

    #[test]
    fn binding_classifies_missing_relay_services_and_invalid_route_contracts() {
        let relay = named::<RelayName>("errors");
        let relay_services = HashMap::default();
        let materialized = HashMap::default();
        let lookups = HashMap::default();
        let udfs = UdfExecutor::default();
        let bind = |spec, relay_services: &HashMap<_, _>| {
            BoundMessageErrorRoutes::bind(
                MessageErrorRouteSpecs { routes: vec![spec] },
                MessageErrorRouteBindingContext {
                    relay_services,
                    materialized_stream_specs: &materialized,
                    lookups: &lookups,
                    udfs: &udfs,
                },
            )
        };

        let Err(error) = bind(spec(None), &relay_services) else {
            panic!("a relay without boundary services must fail binding");
        };
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::DlqRelayNotInstantiated { .. }
        ));

        let relay_services = HashMap::from_iter([(relay, services())]);
        let invalid_flush = FlushPolicy::Each {
            interval: "not-a-duration".to_string(),
            max_batch_size: "1MiB".to_string(),
        };
        let Err(error) = bind(spec(Some(invalid_flush)), &relay_services) else {
            panic!("invalid flush policy must fail binding");
        };
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::FlushPolicy { .. }
        ));

        let mut invalid_set = spec(Some(FlushPolicy::Immediate));
        invalid_set.program = lowered_set(
            nervix_nspl::parse_route_construction("SET missing = input.value")
                .expect("the test assignment parses")
                .assignments,
        );
        let Err(error) = bind(invalid_set, &relay_services) else {
            panic!("an invalid SET must fail VM compilation at binding");
        };
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::ProgramCompilation { .. }
        ));

        let plan = bind(spec(Some(FlushPolicy::Immediate)), &relay_services)
            .expect("a valid route is bound once");
        let key = spec(None).key;
        assert!(plan.get(&key).is_some());
        let Err(error) = BoundMessageErrorRoutes::bind(
            MessageErrorRouteSpecs {
                routes: vec![spec(None), spec(None)],
            },
            MessageErrorRouteBindingContext {
                relay_services: &relay_services,
                materialized_stream_specs: &materialized,
                lookups: &lookups,
                udfs: &udfs,
            },
        ) else {
            panic!("duplicate route identities must fail binding");
        };
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::DuplicatePreparedRoute { .. }
        ));
    }
}
