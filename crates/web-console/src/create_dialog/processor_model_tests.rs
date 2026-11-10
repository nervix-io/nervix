//! Complete processor draft and canonical representation checks.
//!
//! Layer: test harness, outside product layer order.
//! - **Owns.** Independent semantic-to-form fixtures and equality oracles for junction,
//!   deduplicator, reorderer, window processor, correlator, reingestor, generator, inferencer and
//!   WASM processor drafts.
//! - **Depends on.** Current vocabulary generators, visual drafts and canonical Plexus parsing.
//! - **Must not know.** Registry validation or runtime processing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns complete processor draft representation tests")
)]

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    Assignment, BranchSelection, CorrelationTimeoutAction, CreateCorrelator, CreateGenerator,
    CreateInferencer, CreateStatement, CreateWasmProcessor, CreateWindowProcessor, Expression,
    FlushPolicy, InferencerStateBinding, InferencerStateInitial, InferencerTensorDeclaration,
    InferencerTensorDimension, InferencerTensorMapping, InferencerTensorSchema, Inheritance,
    MaterializedStateDependency, MaterializedStatePolicy, MessageErrorPolicy, Model, OutputBranch,
    ProcessorInputs, ProcessorOutput, ProcessorOutputs, RelayName, RequestedResourceVersion,
    ResourceName, Statement, WindowBound, WindowStateLimit, expression_to_plexus,
};
use nervix_plexus::client_statement::{ClientStatement, parse_client_statement};

use super::{
    SelectedReference,
    ingestor_route_draft::{
        AssignmentDraft, FlushDraft, IngestRouteDraft, InheritDraft, InheritedFieldDraft,
        InvocationDraft, MessageErrorDraft, RouteBranchDraft,
    },
    processor_binding_draft::ProcessorBindingDraft,
    processor_correlator_draft::TimeoutDraft,
    processor_draft::{
        NodeBranchDraft, ProcessorDraft, ProcessorFamily, StateDependencyDraft, StatePolicyDraft,
    },
    processor_inferencer_draft::{
        DimensionDraft, OrderedDrafts, StateBindingDraft, StateInitialDraft,
        TensorDeclarationDraft, TensorMappingDraft, TensorShapeDraft,
    },
    processor_input_draft::{InputDraft, InputListDraft},
    processor_window_draft::WindowBoundDraft,
    resource_pin_draft::ResourcePinDraft,
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

/// The input list a draft shows for `value`, its collection included.
fn input_list(value: &ProcessorInputs) -> InputListDraft {
    InputListDraft {
        relays: inputs(value),
        active: 0,
        collect: value.collect_policy.clone(),
    }
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
    draft.from = input_list(parts.from);
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

/// A window bound as its form shows it: each kind it measures as text, and an empty text for a
/// kind it does not.
fn window_bound(value: &WindowBound) -> WindowBoundDraft {
    let messages = match value.messages {
        Some(messages) => messages.to_string(),
        None => String::new(),
    };
    let duration = match &value.duration {
        Some(duration) => duration.clone(),
        None => String::new(),
    };
    WindowBoundDraft { messages, duration }
}

fn window_draft(value: &CreateWindowProcessor) -> ProcessorDraft {
    let mut draft = shared_draft(
        ProcessorFamily::Window,
        SharedParts {
            name: value.name.to_string(),
            from: &value.from,
            output_routes: &value.output_routes,
            branched_by: Some(&value.branched_by),
            mode: value.mode,
            filter_where: &value.filter_where,
            materialized_state: &value.materialized_state,
        },
    );
    let window = draft
        .window_mut()
        .assured("a window processor draft holds its cadence");
    window.width = window_bound(&value.width);
    window.step = window_bound(&value.step);
    window.state_size = match value.state_limit {
        WindowStateLimit::Unbounded => None,
        WindowStateLimit::MaxBytes(bytes) => Some(format!("{bytes}B")),
    };
    draft
}

fn timeout(value: &CorrelationTimeoutAction) -> TimeoutDraft {
    match value {
        CorrelationTimeoutAction::Drop => TimeoutDraft::Drop,
        CorrelationTimeoutAction::SendTo { relay } => {
            TimeoutDraft::SendTo(Some(SelectedReference::chosen(relay.clone())))
        }
    }
}

fn correlator_draft(value: &CreateCorrelator) -> ProcessorDraft {
    let mut draft = shared_draft(
        ProcessorFamily::Correlator,
        SharedParts {
            name: value.name.to_string(),
            from: &value.left,
            output_routes: &value.output_routes,
            branched_by: Some(&value.branched_by),
            mode: value.mode,
            filter_where: &value.filter_where,
            materialized_state: &value.materialized_state,
        },
    );
    let correlator = draft
        .correlator_mut()
        .assured("a correlator draft holds its correlation");
    correlator.right = input_list(&value.right);
    correlator.correlate_where = expression(&value.correlate_where);
    correlator.match_policy = Some(value.match_policy);
    correlator.max_time = value.max_time.clone();
    correlator.left_timeout = timeout(&value.timeout_policy.left);
    correlator.right_timeout = timeout(&value.timeout_policy.right);
    draft
}

fn generator_draft(value: &CreateGenerator) -> ProcessorDraft {
    let mut draft = ProcessorDraft::new(ProcessorFamily::Generator);
    draft.name = value.name.to_string();
    draft.branching = branching(Some(&value.branched_by));
    draft.routes = value.output_routes.routes.iter().map(route).collect();
    let generator = draft
        .generator_mut()
        .assured("a generator draft holds its source");
    generator.relay = Some(SelectedReference::chosen(value.materialized_relay.clone()));
    generator.each = value.each.to_string();
    draft
}

/// A resource binding as its form shows it: the chosen resource and requested version, and the
/// file exactly as written.
fn binding(
    resource: &ResourceName,
    version: RequestedResourceVersion,
    file: &str,
) -> ProcessorBindingDraft {
    ProcessorBindingDraft {
        pin: ResourcePinDraft {
            resource: Some(SelectedReference::chosen(resource.clone())),
            version: Some(SelectedReference::chosen(version)),
        },
        file: file.to_string(),
    }
}

/// A tensor shape as its form shows it: each fixed size as its decimal text.
fn shape(value: &InferencerTensorSchema) -> TensorShapeDraft {
    let mut dimensions = Vec::with_capacity(value.dimensions.len());
    for dimension in &value.dimensions {
        dimensions.push(match dimension {
            InferencerTensorDimension::Fixed(size) => DimensionDraft::Size(size.to_string()),
            InferencerTensorDimension::Dynamic => DimensionDraft::Dynamic,
            InferencerTensorDimension::Batch => DimensionDraft::Batch,
        });
    }
    TensorShapeDraft {
        representation: value.representation,
        element_type: value.element_type,
        dimensions,
    }
}

fn ordered<T>(items: Vec<T>) -> OrderedDrafts<T> {
    OrderedDrafts { items, active: 0 }
}

fn tensor_mapping(value: &InferencerTensorMapping) -> TensorMappingDraft {
    TensorMappingDraft {
        tensor: value.tensor.clone(),
        shape: shape(&value.schema),
        expression: expression(&value.expression),
    }
}

fn state_binding(value: &InferencerStateBinding) -> StateBindingDraft {
    let initial = match &value.initial {
        InferencerStateInitial::Zeros => StateInitialDraft::Zeros,
        InferencerStateInitial::Constant(constant) => {
            StateInitialDraft::Constant(expression(constant))
        }
    };
    StateBindingDraft {
        input: value.input.clone(),
        output: value.output.clone(),
        shape: shape(&value.schema),
        initial,
    }
}

fn tensor_declaration(value: &InferencerTensorDeclaration) -> TensorDeclarationDraft {
    TensorDeclarationDraft {
        tensor: value.tensor.clone(),
        shape: shape(&value.schema),
    }
}

fn inferencer_draft(value: &CreateInferencer<RequestedResourceVersion>) -> ProcessorDraft {
    let mut draft = shared_draft(
        ProcessorFamily::Inferencer,
        SharedParts {
            name: value.name.to_string(),
            from: &value.from,
            output_routes: &value.output_routes,
            branched_by: Some(&value.branched_by),
            mode: value.mode,
            filter_where: &value.filter_where,
            materialized_state: &value.materialized_state,
        },
    );
    let inferencer = draft
        .inferencer_mut()
        .assured("an inferencer draft holds its model");
    inferencer.binding = binding(&value.resource, value.resource_version, &value.file);
    inferencer.inputs = ordered(value.inputs.iter().map(tensor_mapping).collect());
    inferencer.state = ordered(value.state.iter().map(state_binding).collect());
    inferencer.outputs = ordered(value.output_schema.iter().map(tensor_declaration).collect());
    draft
}

fn wasm_draft(value: &CreateWasmProcessor<RequestedResourceVersion>) -> ProcessorDraft {
    let mut draft = shared_draft(
        ProcessorFamily::Wasm,
        SharedParts {
            name: value.name.to_string(),
            from: &value.from,
            output_routes: &value.output_routes,
            branched_by: Some(&value.branched_by),
            mode: value.mode,
            filter_where: &value.filter_where,
            materialized_state: &value.materialized_state,
        },
    );
    let wasm = draft
        .wasm_mut()
        .assured("a WASM processor draft holds its guest");
    wasm.binding = binding(&value.resource, value.resource_version, &value.file);
    wasm.max_fuel = value.limits.max_fuel.to_string();
    wasm.max_memory = format!("{}B", value.limits.max_memory_bytes);
    wasm.rejected_state = value.rejected_state_policy;
    wasm.global_error = Some(value.global_error_policy.clone());
    draft
}

fn filled(value: &Model<RequestedResourceVersion>) -> ProcessorDraft {
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
        Model::WindowProcessor(value) => window_draft(value),
        Model::Correlator(value) => correlator_draft(value),
        Model::Generator(value) => generator_draft(value),
        Model::Inferencer(value) => inferencer_draft(value),
        Model::WasmProcessor(value) => wasm_draft(value),
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
fn round_trip(value: Model<RequestedResourceVersion>, conditional: bool) {
    let mut draft = filled(&value);
    draft.if_not_exists = conditional;
    let actual = draft
        .build()
        .assured("every complete current processor is representable by its visual form");
    assert_eq!(actual, value);
    let statement = Statement::Create(CreateStatement::new(Box::new(actual), conditional));
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
            let window = Model::WindowProcessor(arbitrary.create_window_processor());
            round_trip(window, arbitrary.entropy().flag());
            let correlator = Model::Correlator(arbitrary.create_correlator());
            round_trip(correlator, arbitrary.entropy().flag());
            let reingestor = Model::Reingestor(arbitrary.create_reingestor());
            round_trip(reingestor, arbitrary.entropy().flag());
            let generator = Model::Generator(arbitrary.create_generator());
            round_trip(generator, arbitrary.entropy().flag());
            let mut inferencer = arbitrary.create_inferencer();
            name_blank_texts_of_inferencer(&mut inferencer);
            round_trip(Model::Inferencer(inferencer), arbitrary.entropy().flag());
            let mut wasm = arbitrary.create_wasm_processor();
            name(&mut wasm.file);
            round_trip(Model::WasmProcessor(wasm), arbitrary.entropy().flag());
        });
}

/// Gives a blank text a name. The form reads a blank file, tensor or port name as the choice it
/// still lacks, so a generated blank one is named before the round trip; every other text keeps
/// its exact spelling, surrounding whitespace included.
fn name(text: &mut String) {
    if text.trim().is_empty() {
        text.push_str("named");
    }
}

fn name_blank_texts_of_inferencer(value: &mut CreateInferencer<RequestedResourceVersion>) {
    name(&mut value.file);
    for mapping in &mut value.inputs {
        name(&mut mapping.tensor);
    }
    for state in &mut value.state {
        name(&mut state.input);
        name(&mut state.output);
    }
    for declaration in &mut value.output_schema {
        name(&mut declaration.tensor);
    }
}
