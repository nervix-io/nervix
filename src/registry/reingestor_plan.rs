//! Decides the complete specification for running one scheduled reingestor.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving a reingestor's input relays, their source predicates, its collection,
//!   node filter, materialized-state dependencies and routes into one typed plan.
//! - **Depends on.** Validated schedule Models and the entrypoint route planner.
//! - **Must not know.** Runtime tasks, relay consumers, node-local resources, or placement.

use error_stack::Report;
use nervix_models::{
    AckMode, CreateReingestor, InputCollectPolicy, MaterializedStateDependency,
    MessageErrorOperation, ModelKind, ModelName, ReingestorName, RelayName,
};

use super::{
    entrypoint_plan::{
        EntrypointOwner, EntrypointPlanError, EntrypointRouteContext, LoweredFilter,
        PlannedEntryRoute,
    },
    processor_plan::processor_input_where_by_relay,
};

/// One relay a reingestor reads, with the source predicate its messages pass first.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReingestorInputPlan {
    pub(crate) relay: RelayName,
    pub(crate) from_where: Option<LoweredFilter>,
}

/// Everything a node needs to run one reingestor.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReingestorPlan {
    pub(crate) name: ReingestorName,
    pub(crate) mode: AckMode,
    /// Every input relay in declared order, which is never empty. All of them share one schema.
    pub(crate) inputs: Vec<ReingestorInputPlan>,
    pub(crate) collect_policy: Option<InputCollectPolicy>,
    /// The node-wide predicate every input message passes after its source predicate.
    pub(crate) filter_where: Option<LoweredFilter>,
    pub(crate) materialized_state: Vec<MaterializedStateDependency>,
    /// Every output route in declared order, which is never empty.
    pub(crate) routes: Vec<PlannedEntryRoute>,
}

impl ReingestorPlan {
    pub(in crate::registry) fn decide(
        reingestor: &CreateReingestor,
        routes: &EntrypointRouteContext<'_>,
    ) -> Result<Self, Report<EntrypointPlanError>> {
        let identifier = ModelName::from(&reingestor.name);
        let owner = EntrypointOwner::reingestor(&identifier, reingestor);
        let from_where = processor_input_where_by_relay(reingestor.from.where_clauses());
        let mut input_schema = None;
        let mut inputs = Vec::with_capacity(reingestor.from.relays().len());
        for relay in reingestor.from.relays() {
            let Some(planned) = routes.activation().relays.get(relay) else {
                return Err(Report::new(EntrypointPlanError::MissingInputRelay {
                    kind: ModelKind::Reingestor,
                    node: identifier.clone(),
                    relay: relay.clone(),
                }));
            };
            let schema = planned.schema.arrow_schema();
            match input_schema.as_ref() {
                None => input_schema = Some(schema),
                Some(first) if first != &schema => {
                    return Err(Report::new(EntrypointPlanError::InputSchemaMismatch {
                        kind: ModelKind::Reingestor,
                        node: identifier.clone(),
                        relay: relay.clone(),
                    }));
                }
                Some(_) => {}
            }
            let source_predicate = match from_where.get(relay) {
                Some(filter) => Some(LoweredFilter::planned(
                    &owner,
                    filter,
                    MessageErrorOperation::SourceWhere,
                )?),
                None => None,
            };
            inputs.push(ReingestorInputPlan {
                relay: relay.clone(),
                from_where: source_predicate,
            });
        }
        let Some(input_schema) = input_schema else {
            return Err(Report::new(EntrypointPlanError::MissingInput {
                kind: ModelKind::Reingestor,
                node: identifier.clone(),
            }));
        };
        let filter_where = match reingestor.filter_where.as_ref() {
            Some(filter) => Some(LoweredFilter::planned(
                &owner,
                filter,
                MessageErrorOperation::FilterWhere,
            )?),
            None => None,
        };
        let planned_routes =
            routes.plan_routes(&owner, reingestor.output_routes.outputs(), &input_schema)?;
        Ok(Self {
            name: reingestor.name.clone(),
            mode: reingestor.mode,
            inputs,
            collect_policy: reingestor.from.collect_policy.clone(),
            filter_where,
            materialized_state: reingestor.materialized_state.clone(),
            routes: planned_routes,
        })
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{Model, ProcessorInputWhere, ProcessorInputs};

    use super::*;
    use crate::registry::{
        EntrypointPlans,
        test_fixtures::{
            branch_schema, client_model, codec, named, planned_entrypoints, reingestor,
            reingestor_statement, relay, schema, unlowerable_predicate, wire_schema,
        },
    };

    fn expression(source: &str) -> nervix_models::Expression {
        nervix_models::Expression::Binary {
            left: Box::new(nervix_models::Expression::Field(
                nervix_models::FieldReference::scoped(
                    nervix_models::FieldScope::Input,
                    named("value"),
                ),
            )),
            operator: nervix_models::BinaryOperator::NotEqual,
            right: Box::new(nervix_models::Expression::Literal(
                nervix_models::Literal::String(source.to_string()),
            )),
        }
    }

    /// Two payload relays and the relay a reingestor fixture writes, beside a schema of other
    /// fields.
    fn domain_models() -> Vec<Model> {
        vec![
            schema("payload"),
            branch_schema("other_payload", &["other"]),
            wire_schema("event_wire"),
            codec("json", "payload"),
            client_model("broker"),
            relay("events", "payload"),
            relay("more_events", "payload"),
            relay("outgoing", "payload"),
        ]
    }

    fn planned(plans: &EntrypointPlans) -> &ReingestorPlan {
        plans
            .reingestor(&named("repartition"))
            .assured("the fixture schedules the reingestor named repartition")
    }

    #[test]
    fn plans_every_input_with_its_source_predicate_and_the_node_filter() {
        let mut models = domain_models();
        let mut repartition = reingestor_statement("repartition", "events", "outgoing", &[]);
        repartition.from = ProcessorInputs::new(
            vec![named("events"), named("more_events")],
            vec![ProcessorInputWhere {
                relay: named("more_events"),
                where_clause: expression("skip"),
            }],
        )
        .with_collect_policy("25ms".to_string(), Some("1MiB".to_string()));
        repartition.filter_where = Some(expression("drop"));
        models.push(Model::Reingestor(repartition));

        let plans = planned_entrypoints(models).assured("the fixture schedule plans");
        let plan = planned(&plans);

        assert_eq!(plan.name, named("repartition"));
        assert_eq!(
            plan.inputs
                .iter()
                .map(|input| input.relay.clone())
                .collect::<Vec<_>>(),
            vec![named("events"), named("more_events")]
        );
        assert!(plan.inputs[0].from_where.is_none());
        assert!(plan.inputs[1].from_where.is_some());
        assert!(plan.filter_where.is_some());
        assert_eq!(
            plan.collect_policy
                .as_ref()
                .map(|policy| policy.collect_for.as_str()),
            Some("25ms")
        );
        assert_eq!(plan.routes.len(), 1);
        assert_eq!(plan.routes[0].relay, named("outgoing"));
    }

    #[rstest::rstest]
    #[case::source_predicate(MessageErrorOperation::SourceWhere)]
    #[case::node_filter(MessageErrorOperation::FilterWhere)]
    fn rejects_a_predicate_that_does_not_lower(#[case] operation: MessageErrorOperation) {
        let mut models = domain_models();
        let mut repartition = reingestor_statement("repartition", "events", "outgoing", &[]);
        match operation {
            MessageErrorOperation::SourceWhere => {
                repartition.from = ProcessorInputs::new(
                    vec![named("events")],
                    vec![ProcessorInputWhere {
                        relay: named("events"),
                        where_clause: unlowerable_predicate(),
                    }],
                );
            }
            _ => repartition.filter_where = Some(unlowerable_predicate()),
        }
        models.push(Model::Reingestor(repartition));

        let error = planned_entrypoints(models).expect_err("the predicate does not lower");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::InvalidFilter {
                kind: ModelKind::Reingestor,
                node: named("repartition"),
                operation,
            }
        );
    }

    #[test]
    fn rejects_inputs_whose_schemas_differ() {
        let mut models = domain_models();
        models.push(relay("other_events", "other_payload"));
        let mut repartition = reingestor_statement("repartition", "events", "outgoing", &[]);
        repartition.from =
            ProcessorInputs::new(vec![named("events"), named("other_events")], Vec::new());
        models.push(Model::Reingestor(repartition));

        let error = planned_entrypoints(models).expect_err("inputs share one schema");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::InputSchemaMismatch {
                kind: ModelKind::Reingestor,
                node: named("repartition"),
                relay: named("other_events"),
            }
        );
    }

    #[test]
    fn rejects_an_input_relay_that_is_not_scheduled() {
        let mut models = domain_models();
        models.push(reingestor("repartition", "missing", "outgoing", &[]));

        let error = planned_entrypoints(models).expect_err("the input relay is required");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::MissingInputRelay {
                kind: ModelKind::Reingestor,
                node: named("repartition"),
                relay: named("missing"),
            }
        );
    }

    #[test]
    fn rejects_a_reingestor_without_inputs() {
        let mut models = domain_models();
        let mut repartition = reingestor_statement("repartition", "events", "outgoing", &[]);
        repartition.from = ProcessorInputs::new(Vec::new(), Vec::new());
        models.push(Model::Reingestor(repartition));

        let error = planned_entrypoints(models).expect_err("an input is required");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::MissingInput {
                kind: ModelKind::Reingestor,
                node: named("repartition"),
            }
        );
    }
}
