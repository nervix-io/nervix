//! Complete processor draft and canonical representation checks.
//!
//! Layer: test harness, outside product layer order.
//! - **Owns.** Independent semantic-to-form fixtures and equality oracles for junction,
//!   deduplicator, reorderer and reingestor drafts.
//! - **Depends on.** Current vocabulary generators, visual drafts and canonical Plexus parsing.
//! - **Must not know.** Registry validation or runtime processing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns complete processor draft representation tests")
)]

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    Assignment, BranchSelection, CreateStatement, Expression, FlushPolicy, Inheritance,
    MaterializedStateDependency, MaterializedStatePolicy, MessageErrorPolicy, Model, OutputBranch,
    ProcessorInputs, ProcessorOutput, ProcessorOutputs, RelayName, Statement, expression_to_plexus,
};
use nervix_plexus::client_statement::{ClientStatement, parse_client_statement};

use super::{
    SelectedReference,
    ingestor_route_draft::{
        AssignmentDraft, FlushDraft, IngestRouteDraft, InheritDraft, InheritedFieldDraft,
        InvocationDraft, MessageErrorDraft, RouteBranchDraft,
    },
    processor_draft::{
        InputDraft, NodeBranchDraft, ProcessorDraft, ProcessorFamily, StateDependencyDraft,
        StatePolicyDraft,
    },
};

fn expression(value: &Expression) -> String {
    expression_to_plexus(value).assured("a current generated expression has canonical Plexus")
}

fn optional_expression(value: &Option<Expression>) -> String {
    match value {
        Some(value) => expression(value),
        None => String::new(),
    }
}

fn assignments(values: &[Assignment]) -> Vec<AssignmentDraft> {
    values
        .iter()
        .map(|value| AssignmentDraft {
            scope: value.target.scope,
            field: Some(SelectedReference::chosen(value.target.field.clone())),
            expression: expression(&value.value),
        })
        .collect()
}

fn inputs(value: &ProcessorInputs) -> Vec<InputDraft> {
    let mut predicates = BTreeMap::<RelayName, String>::new();
    for predicate in &value.r#where {
        predicates.insert(predicate.relay.clone(), expression(&predicate.where_clause));
    }
    let mut drafts = Vec::with_capacity(value.from.len());
    for relay in &value.from {
        let where_clause = match predicates.get(relay) {
            Some(predicate) => predicate.clone(),
            None => String::new(),
        };
        drafts.push(InputDraft {
            relay: Some(SelectedReference::chosen(relay.clone())),
            where_clause,
        });
    }
    drafts
}

fn branching(value: Option<&BranchSelection>) -> NodeBranchDraft {
    match value {
        None => NodeBranchDraft::Unselected,
        Some(BranchSelection::Unbranched) => NodeBranchDraft::Unbranched,
        Some(BranchSelection::BranchedBy { branch }) => {
            NodeBranchDraft::Branched(Some(SelectedReference::chosen(branch.clone())))
        }
    }
}

fn state(values: &[MaterializedStateDependency]) -> Vec<StateDependencyDraft> {
    let mut drafts = Vec::with_capacity(values.len());
    for dependency in values {
        let policy = match &dependency.policy {
            MaterializedStatePolicy::RequiredSkip => StatePolicyDraft::RequiredSkip,
            MaterializedStatePolicy::RequiredWait => StatePolicyDraft::RequiredWait,
            MaterializedStatePolicy::Default(values) => {
                StatePolicyDraft::Default(assignments(values))
            }
        };
        drafts.push(StateDependencyDraft {
            relay: Some(SelectedReference::chosen(dependency.relay.clone())),
            policy,
        });
    }
    drafts
}

fn inheritance(value: Option<&Inheritance>) -> InheritDraft {
    match value {
        None => InheritDraft::None,
        Some(Inheritance::All) => InheritDraft::All,
        Some(Inheritance::AllExcept(fields)) => InheritDraft::AllExcept(
            fields
                .iter()
                .cloned()
                .map(SelectedReference::chosen)
                .collect(),
        ),
        Some(Inheritance::Fields(fields)) => InheritDraft::Fields(
            fields
                .iter()
                .map(|field| InheritedFieldDraft {
                    field: SelectedReference::chosen(field.field.clone()),
                    leak_sensitive: field.leak_sensitive,
                })
                .collect(),
        ),
    }
}

fn route_branch(value: Option<&OutputBranch>) -> RouteBranchDraft {
    match value {
        None => RouteBranchDraft::Preserve,
        Some(OutputBranch::Unbranched) => RouteBranchDraft::Unbranched,
        Some(OutputBranch::BranchedBy {
            branch,
            assignments: values,
        }) => RouteBranchDraft::Branched {
            branch: Some(SelectedReference::chosen(branch.clone())),
            assignments: assignments(values),
        },
    }
}

fn flush(value: Option<&FlushPolicy>) -> FlushDraft {
    match value {
        None => FlushDraft::Unselected,
        Some(FlushPolicy::Immediate) => FlushDraft::Immediate,
        Some(FlushPolicy::Each {
            interval,
            max_batch_size,
        }) => FlushDraft::Each {
            interval: interval.clone(),
            max_batch_size: max_batch_size.clone(),
        },
    }
}

fn message_error(value: &MessageErrorPolicy) -> MessageErrorDraft {
    match value {
        MessageErrorPolicy::Ignore => MessageErrorDraft::Ignore,
        MessageErrorPolicy::Log => MessageErrorDraft::Log,
        MessageErrorPolicy::Dlq {
            relay,
            assignments: values,
        } => MessageErrorDraft::SendTo {
            relay: Some(SelectedReference::chosen(relay.clone())),
            assignments: assignments(values),
        },
    }
}

fn route(value: &ProcessorOutput) -> IngestRouteDraft {
    let construction = &value.construction;
    let invocations = construction
        .invocations
        .iter()
        .map(|call| InvocationDraft {
            function: call.function.to_string(),
            arguments: call.arguments.iter().map(expression).collect(),
        })
        .collect();
    IngestRouteDraft {
        relay: Some(SelectedReference::chosen(value.relay.clone())),
        inherit: inheritance(construction.inherit.as_ref()),
        assignments: assignments(&construction.assignments),
        where_clause: optional_expression(&construction.where_clause),
        invocations,
        branch: route_branch(value.branch.as_ref()),
        flush: flush(value.flush_policy.as_ref()),
        message_error: message_error(&value.message_error_policy),
    }
}

/// The parts every processor family shares, borrowed from one complete Model.
struct SharedParts<'a> {
    name: String,
    from: &'a ProcessorInputs,
    output_routes: &'a ProcessorOutputs,
    branched_by: Option<&'a BranchSelection>,
    mode: nervix_models::AckMode,
    filter_where: &'a Option<Expression>,
    materialized_state: &'a [MaterializedStateDependency],
}

/// The ordered key expressions and holding time of a keyed processor Model.
struct KeyedParts<'a> {
    expressions: &'a [Expression],
    max_time: &'a str,
}

fn shared_draft(family: ProcessorFamily, parts: SharedParts<'_>) -> ProcessorDraft {
    let mut draft = ProcessorDraft::new(family);
    draft.name = parts.name;
    draft.mode = parts.mode;
    draft.inputs = inputs(parts.from);
    draft.collect = parts.from.collect_policy.clone();
    draft.filter = optional_expression(parts.filter_where);
    draft.branching = branching(parts.branched_by);
    draft.state = state(parts.materialized_state);
    draft.routes = parts.output_routes.routes.iter().map(route).collect();
    draft
}

fn keyed_draft(
    family: ProcessorFamily,
    parts: SharedParts<'_>,
    keys: KeyedParts<'_>,
) -> ProcessorDraft {
    let mut draft = shared_draft(family, parts);
    let keyed = draft
        .keyed_mut()
        .assured("a deduplicator or reorderer draft holds its ordered keys");
    keyed.expressions = keys.expressions.iter().map(expression).collect();
    keyed.max_time = keys.max_time.to_string();
    draft
}

fn filled(value: &Model) -> ProcessorDraft {
    match value {
        Model::Junction(value) => shared_draft(
            ProcessorFamily::Junction,
            SharedParts {
                name: value.name.to_string(),
                from: &value.from,
                output_routes: &value.output_routes,
                branched_by: Some(&value.branched_by),
                mode: value.mode,
                filter_where: &value.filter_where,
                materialized_state: &value.materialized_state,
            },
        ),
        Model::Deduplicator(value) => keyed_draft(
            ProcessorFamily::Deduplicator,
            SharedParts {
                name: value.name.to_string(),
                from: &value.from,
                output_routes: &value.output_routes,
                branched_by: Some(&value.branched_by),
                mode: value.mode,
                filter_where: &value.filter_where,
                materialized_state: &value.materialized_state,
            },
            KeyedParts {
                expressions: &value.deduplicate_on,
                max_time: &value.max_time,
            },
        ),
        Model::Reorderer(value) => keyed_draft(
            ProcessorFamily::Reorderer,
            SharedParts {
                name: value.name.to_string(),
                from: &value.from,
                output_routes: &value.output_routes,
                branched_by: Some(&value.branched_by),
                mode: value.mode,
                filter_where: &value.filter_where,
                materialized_state: &value.materialized_state,
            },
            KeyedParts {
                expressions: &value.order_by,
                max_time: &value.max_time,
            },
        ),
        Model::Reingestor(value) => shared_draft(
            ProcessorFamily::Reingestor,
            SharedParts {
                name: value.name.to_string(),
                from: &value.from,
                output_routes: &value.output_routes,
                branched_by: None,
                mode: value.mode,
                filter_where: &value.filter_where,
                materialized_state: &value.materialized_state,
            },
        ),
        other => panic!("{:?} has no processor form", other.kind()),
    }
}

/// Fills a draft from `value`, builds it back, and requires the complete Model and its canonical
/// statement to survive unchanged.
fn round_trip(value: Model, conditional: bool) {
    let mut draft = filled(&value);
    draft.if_not_exists = conditional;
    let actual = draft
        .build()
        .assured("every complete current processor is representable by its visual form");
    assert_eq!(actual, value);
    let statement = Statement::Create(CreateStatement::new(Box::new(actual.into()), conditional));
    let source = statement
        .to_canonical_plexus()
        .assured("every visual processor renders canonical Plexus");
    assert_eq!(
        parse_client_statement(&source)
            .assured("canonical processors reparse without losing a clause"),
        ClientStatement::Server(statement),
        "{source}"
    );
}

#[test]
fn bolero_processor_drafts_preserve_every_complete_model() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary =
                nervix_arbitrary::Arbitrary::new(bytes, nervix_arbitrary::Domain::Plexus);
            let junction = Model::Junction(arbitrary.create_junction());
            round_trip(junction, arbitrary.entropy().flag());
            let deduplicator = Model::Deduplicator(arbitrary.create_deduplicator());
            round_trip(deduplicator, arbitrary.entropy().flag());
            let reorderer = Model::Reorderer(arbitrary.create_reorderer());
            round_trip(reorderer, arbitrary.entropy().flag());
            let reingestor = Model::Reingestor(arbitrary.create_reingestor());
            round_trip(reingestor, arbitrary.entropy().flag());
        });
}
