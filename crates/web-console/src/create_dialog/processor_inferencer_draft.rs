//! The model binding and tensors of a visual inferencer until they become its Model.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of an inferencer's ordered `INPUTS` mappings, `STATE` bindings and
//!   `OUTPUT SCHEMA` declarations with their dense tensor shapes, the input fields its mapping
//!   picker inserts, the generated fields its routes may read, the execution mode its shapes
//!   imply, and their conversion to the inferencer Model's tensors.
//! - **Depends on.** Typed tensor vocabulary and its execution-mode rule, expression parsing, and
//!   the shared processor binding draft.
//! - **Must not know.** ONNX inspection, tensor conversion or model execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{fmt, num::NonZeroU32};

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    FieldName, FieldReference, FieldScope, InferencerExecutionMode, InferencerStateBinding,
    InferencerStateInitial, InferencerTensorDeclaration, InferencerTensorDimension,
    InferencerTensorElementType, InferencerTensorMapping, InferencerTensorRepresentation,
    InferencerTensorSchema, InferencerTensorSchemaError,
};
use nervix_plexus::parse_expression;
use strum::Display;

use super::{
    inserted_reference::InsertedReference,
    processor_binding_draft::{ProcessorBinding, ProcessorBindingDraft},
    processor_draft::ProcessorDraftError,
};

/// The tensor list a tensor belongs to, spelled as Plexus writes its clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(super) enum TensorList {
    #[strum(serialize = "INPUTS")]
    Inputs,
    #[strum(serialize = "STATE")]
    State,
    #[strum(serialize = "OUTPUT SCHEMA")]
    Outputs,
}

impl TensorList {
    /// Whether the language requires at least one tensor in the list. Only a stateful model
    /// declares `STATE`.
    pub(super) fn required(self) -> bool {
        match self {
            Self::Inputs | Self::Outputs => true,
            Self::State => false,
        }
    }
}

/// One change to a tensor list's order, or to which of its tensors is edited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TensorListEdit {
    Select(usize),
    Add,
    RemoveActive,
    MoveActiveUp,
    MoveActiveDown,
}

/// The kind of one axis, as its choice shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DimensionKind {
    Size,
    Dynamic,
    Batch,
}

/// One tensor of one list, as a refusal names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TensorPosition {
    pub(super) list: TensorList,
    /// The tensor's place in its list, counted from one as the form shows it.
    pub(super) number: usize,
}

impl fmt::Display for TensorPosition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} tensor {}", self.list, self.number)
    }
}

/// One axis of a tensor shape as edited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DimensionDraft {
    /// A fixed size, kept exactly as typed until the shape builds.
    Size(String),
    Dynamic,
    Batch,
}

impl DimensionDraft {
    pub(super) fn kind(&self) -> DimensionKind {
        match self {
            Self::Size(_) => DimensionKind::Size,
            Self::Dynamic => DimensionKind::Dynamic,
            Self::Batch => DimensionKind::Batch,
        }
    }

    /// The axis, while its size reads as the positive whole number the language requires.
    fn dimension(&self) -> Option<InferencerTensorDimension> {
        match self {
            Self::Size(text) => match text.trim().parse::<NonZeroU32>() {
                Ok(size) => Some(InferencerTensorDimension::Fixed(size)),
                Err(_) => None,
            },
            Self::Dynamic => Some(InferencerTensorDimension::Dynamic),
            Self::Batch => Some(InferencerTensorDimension::Batch),
        }
    }
}

/// A dense tensor's representation, element type and ordered axes as edited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TensorShapeDraft {
    pub(super) representation: InferencerTensorRepresentation,
    pub(super) element_type: InferencerTensorElementType,
    pub(super) dimensions: Vec<DimensionDraft>,
}

impl Default for TensorShapeDraft {
    /// One axis to size: most tensors have at least one, and an axis is removable for a scalar.
    fn default() -> Self {
        Self {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions: vec![DimensionDraft::Size(String::new())],
        }
    }
}

impl TensorShapeDraft {
    /// The shape, while every axis reads.
    fn schema(&self) -> Option<InferencerTensorSchema> {
        let mut dimensions = Vec::with_capacity(self.dimensions.len());
        for dimension in &self.dimensions {
            dimensions.push(dimension.dimension()?);
        }
        Some(InferencerTensorSchema {
            representation: self.representation,
            element_type: self.element_type,
            dimensions,
        })
    }

    /// The shape, or a refusal naming the first axis whose size does not read.
    fn build(
        &self,
        position: TensorPosition,
    ) -> error_stack::Result<InferencerTensorSchema, ProcessorDraftError> {
        let mut dimensions = Vec::with_capacity(self.dimensions.len());
        for (index, dimension) in self.dimensions.iter().enumerate() {
            let Some(dimension) = dimension.dimension() else {
                return Err(Report::new(ProcessorDraftError::TensorDimension {
                    tensor: position,
                    axis: index + 1,
                }));
            };
            dimensions.push(dimension);
        }
        Ok(InferencerTensorSchema {
            representation: self.representation,
            element_type: self.element_type,
            dimensions,
        })
    }

    pub(super) fn add_dimension(&mut self) {
        self.dimensions.push(DimensionDraft::Size(String::new()));
    }

    pub(super) fn remove_dimension(&mut self, axis: usize) {
        if axis < self.dimensions.len() {
            self.dimensions.remove(axis);
        }
    }

    /// Makes `axis` a dimension of `kind`. A size keeps the text it already holds.
    pub(super) fn set_kind(&mut self, axis: usize, kind: DimensionKind) {
        let Some(dimension) = self.dimensions.get_mut(axis) else {
            return;
        };
        if dimension.kind() == kind {
            return;
        }
        *dimension = match kind {
            DimensionKind::Size => DimensionDraft::Size(String::new()),
            DimensionKind::Dynamic => DimensionDraft::Dynamic,
            DimensionKind::Batch => DimensionDraft::Batch,
        };
    }

    pub(super) fn set_size(&mut self, axis: usize, text: String) {
        if let Some(DimensionDraft::Size(size)) = self.dimensions.get_mut(axis) {
            *size = text;
        }
    }

    /// The text of `axis` while it is a size.
    pub(super) fn size_text(&self, axis: usize) -> String {
        match self.dimensions.get(axis) {
            Some(DimensionDraft::Size(text)) => text.clone(),
            Some(DimensionDraft::Dynamic | DimensionDraft::Batch) | None => String::new(),
        }
    }

    pub(super) fn dimension_kind(&self, axis: usize) -> Option<DimensionKind> {
        self.dimensions.get(axis).map(DimensionDraft::kind)
    }
}

/// One `INPUTS` mapping: the model input it fills, its shape, and the expression over the live
/// input that fills it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct TensorMappingDraft {
    pub(super) tensor: String,
    pub(super) shape: TensorShapeDraft,
    pub(super) expression: String,
}

/// One `STATE` binding: the model input a branch's state enters through, the output its successor
/// leaves through, their shape, and the state's initial value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct StateBindingDraft {
    pub(super) input: String,
    pub(super) output: String,
    pub(super) shape: TensorShapeDraft,
    pub(super) initial: StateInitialDraft,
}

/// A state binding's `INITIAL` choice as edited.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum StateInitialDraft {
    #[default]
    Unselected,
    Zeros,
    /// A constant expression, kept exactly as typed until the draft builds.
    Constant(String),
}

/// One `OUTPUT SCHEMA` declaration: the model output whose values the routes read as a generated
/// field of the same name, and its shape.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct TensorDeclarationDraft {
    pub(super) tensor: String,
    pub(super) shape: TensorShapeDraft,
}

/// An ordered list of drafts and the one being edited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OrderedDrafts<T> {
    pub(super) items: Vec<T>,
    pub(super) active: usize,
}

impl<T: Default> OrderedDrafts<T> {
    fn empty() -> Self {
        Self {
            items: Vec::new(),
            active: 0,
        }
    }

    /// One empty entry to fill, for a list the language requires at least one of.
    fn one() -> Self {
        Self {
            items: vec![T::default()],
            active: 0,
        }
    }

    pub(super) fn active(&self) -> Option<&T> {
        self.items.get(self.active)
    }

    pub(super) fn active_mut(&mut self) -> Option<&mut T> {
        self.items.get_mut(self.active)
    }

    pub(super) fn add(&mut self) {
        self.items.push(T::default());
        self.active = self.items.len() - 1;
    }

    pub(super) fn remove_active(&mut self) {
        if self.active < self.items.len() {
            self.items.remove(self.active);
            self.active = match self.items.len().checked_sub(1) {
                Some(last) => self.active.min(last),
                None => 0,
            };
        }
    }

    pub(super) fn move_active(&mut self, up: bool) {
        let other = if up {
            self.active.checked_sub(1)
        } else if self.active + 1 < self.items.len() {
            Some(self.active + 1)
        } else {
            None
        };
        if let Some(other) = other {
            self.items.swap(self.active, other);
            self.active = other;
        }
    }

    fn apply(&mut self, edit: TensorListEdit) {
        match edit {
            TensorListEdit::Select(index) => {
                if index < self.items.len() {
                    self.active = index;
                }
            }
            TensorListEdit::Add => self.add(),
            TensorListEdit::RemoveActive => self.remove_active(),
            TensorListEdit::MoveActiveUp => self.move_active(true),
            TensorListEdit::MoveActiveDown => self.move_active(false),
        }
    }
}

/// What only an inferencer declares: the model it loads and the tensors it exchanges with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InferencerDraft {
    pub(super) binding: ProcessorBindingDraft,
    pub(super) inputs: OrderedDrafts<TensorMappingDraft>,
    pub(super) state: OrderedDrafts<StateBindingDraft>,
    pub(super) outputs: OrderedDrafts<TensorDeclarationDraft>,
}

impl Default for InferencerDraft {
    /// One input and one output tensor to fill, since the language requires both lists, and no
    /// state, which only a stateful model declares.
    fn default() -> Self {
        Self {
            binding: ProcessorBindingDraft::default(),
            inputs: OrderedDrafts::one(),
            state: OrderedDrafts::empty(),
            outputs: OrderedDrafts::one(),
        }
    }
}

/// The checked binding and tensors of an inferencer.
pub(super) struct InferencerParts {
    pub(super) binding: ProcessorBinding,
    pub(super) inputs: Vec<InferencerTensorMapping>,
    pub(super) state: Vec<InferencerStateBinding>,
    pub(super) output_schema: Vec<InferencerTensorDeclaration>,
}

/// A tensor or port name as a refusal requires it: present, and kept exactly as typed.
fn tensor_name(
    text: &str,
    refusal: ProcessorDraftError,
) -> error_stack::Result<String, ProcessorDraftError> {
    if text.trim().is_empty() {
        return Err(Report::new(refusal));
    }
    Ok(text.to_string())
}

impl InferencerDraft {
    /// The binding and tensors, or the first missing or unreadable one in form order. How the
    /// tensors' axes combine into an execution mode is the inferencer Model's rule, which the
    /// form shows through `execution_mode` and the server applies.
    pub(super) fn build(&self) -> error_stack::Result<InferencerParts, ProcessorDraftError> {
        let binding = self.binding.build()?;
        let inputs = self.build_inputs()?;
        let state = self.build_state()?;
        let output_schema = self.build_outputs()?;
        Ok(InferencerParts {
            binding,
            inputs,
            state,
            output_schema,
        })
    }

    fn build_inputs(
        &self,
    ) -> error_stack::Result<Vec<InferencerTensorMapping>, ProcessorDraftError> {
        if self.inputs.items.is_empty() {
            return Err(Report::new(ProcessorDraftError::Tensors {
                list: TensorList::Inputs,
            }));
        }
        let mut inputs = Vec::with_capacity(self.inputs.items.len());
        for (index, mapping) in self.inputs.items.iter().enumerate() {
            let position = TensorPosition {
                list: TensorList::Inputs,
                number: index + 1,
            };
            let tensor = tensor_name(
                &mapping.tensor,
                ProcessorDraftError::TensorName { tensor: position },
            )?;
            let schema = mapping.shape.build(position)?;
            let expression = parse_expression(mapping.expression.trim())
                .change_context(ProcessorDraftError::TensorExpression { tensor: position })?;
            inputs.push(InferencerTensorMapping {
                tensor,
                schema,
                expression,
            });
        }
        Ok(inputs)
    }

    fn build_state(&self) -> error_stack::Result<Vec<InferencerStateBinding>, ProcessorDraftError> {
        let mut state = Vec::with_capacity(self.state.items.len());
        for (index, binding) in self.state.items.iter().enumerate() {
            let position = TensorPosition {
                list: TensorList::State,
                number: index + 1,
            };
            let input = tensor_name(
                &binding.input,
                ProcessorDraftError::StateInput { tensor: position },
            )?;
            let output = tensor_name(
                &binding.output,
                ProcessorDraftError::StateOutput { tensor: position },
            )?;
            let schema = binding.shape.build(position)?;
            let initial = match &binding.initial {
                StateInitialDraft::Unselected => {
                    return Err(Report::new(ProcessorDraftError::StateInitial {
                        tensor: position,
                    }));
                }
                StateInitialDraft::Zeros => InferencerStateInitial::Zeros,
                StateInitialDraft::Constant(text) => InferencerStateInitial::Constant(
                    parse_expression(text.trim())
                        .change_context(ProcessorDraftError::StateConstant { tensor: position })?,
                ),
            };
            state.push(InferencerStateBinding {
                input,
                output,
                schema,
                initial,
            });
        }
        Ok(state)
    }

    fn build_outputs(
        &self,
    ) -> error_stack::Result<Vec<InferencerTensorDeclaration>, ProcessorDraftError> {
        if self.outputs.items.is_empty() {
            return Err(Report::new(ProcessorDraftError::Tensors {
                list: TensorList::Outputs,
            }));
        }
        let mut outputs = Vec::with_capacity(self.outputs.items.len());
        for (index, declaration) in self.outputs.items.iter().enumerate() {
            let position = TensorPosition {
                list: TensorList::Outputs,
                number: index + 1,
            };
            let tensor = tensor_name(
                &declaration.tensor,
                ProcessorDraftError::TensorName { tensor: position },
            )?;
            let schema = declaration.shape.build(position)?;
            outputs.push(InferencerTensorDeclaration { tensor, schema });
        }
        Ok(outputs)
    }

    /// The execution mode the tensor shapes imply, by the inferencer Model's rule, once every
    /// axis of every tensor reads: per message, batched, or the refusal of shapes that mix them.
    pub(super) fn execution_mode(
        &self,
    ) -> Option<error_stack::Result<InferencerExecutionMode, InferencerTensorSchemaError>> {
        let mut shapes = Vec::new();
        for mapping in &self.inputs.items {
            shapes.push((mapping.tensor.clone(), mapping.shape.schema()?));
        }
        for declaration in &self.outputs.items {
            shapes.push((declaration.tensor.clone(), declaration.shape.schema()?));
        }
        for binding in &self.state.items {
            shapes.push((binding.input.clone(), binding.shape.schema()?));
        }
        let tensors = shapes
            .iter()
            .map(|(tensor, schema)| (tensor.as_str(), schema));
        Some(InferencerExecutionMode::of_tensors(
            tensors,
            !self.state.items.is_empty(),
        ))
    }

    /// The generated fields route construction reads: each declared output tensor whose name is
    /// a field name, in declaration order.
    pub(super) fn generated_fields(&self) -> Vec<FieldName> {
        let mut fields = Vec::new();
        for declaration in &self.outputs.items {
            if let Ok(field) = FieldName::parse(&declaration.tensor) {
                fields.push(field);
            }
        }
        fields
    }

    /// Appends `field` of the live input to the active `INPUTS` mapping's expression, the one
    /// place an inferencer reads its input.
    pub(super) fn insert_input_field(&mut self, field: FieldName) {
        if let Some(mapping) = self.inputs.active_mut() {
            InsertedReference::Field(FieldReference::scoped(FieldScope::Input, field))
                .append_to(&mut mapping.expression);
        }
    }

    pub(super) fn tensor_count(&self, list: TensorList) -> usize {
        match list {
            TensorList::Inputs => self.inputs.items.len(),
            TensorList::State => self.state.items.len(),
            TensorList::Outputs => self.outputs.items.len(),
        }
    }

    /// The index of the tensor of `list` being edited.
    pub(super) fn active_tensor(&self, list: TensorList) -> usize {
        match list {
            TensorList::Inputs => self.inputs.active,
            TensorList::State => self.state.active,
            TensorList::Outputs => self.outputs.active,
        }
    }

    pub(super) fn edit_tensors(&mut self, list: TensorList, edit: TensorListEdit) {
        match list {
            TensorList::Inputs => self.inputs.apply(edit),
            TensorList::State => self.state.apply(edit),
            TensorList::Outputs => self.outputs.apply(edit),
        }
    }

    /// The shape of the tensor of `list` being edited.
    pub(super) fn active_shape(&self, list: TensorList) -> Option<&TensorShapeDraft> {
        match list {
            TensorList::Inputs => match self.inputs.active() {
                Some(mapping) => Some(&mapping.shape),
                None => None,
            },
            TensorList::State => match self.state.active() {
                Some(binding) => Some(&binding.shape),
                None => None,
            },
            TensorList::Outputs => match self.outputs.active() {
                Some(declaration) => Some(&declaration.shape),
                None => None,
            },
        }
    }

    pub(super) fn active_shape_mut(&mut self, list: TensorList) -> Option<&mut TensorShapeDraft> {
        match list {
            TensorList::Inputs => match self.inputs.active_mut() {
                Some(mapping) => Some(&mut mapping.shape),
                None => None,
            },
            TensorList::State => match self.state.active_mut() {
                Some(binding) => Some(&mut binding.shape),
                None => None,
            },
            TensorList::Outputs => match self.outputs.active_mut() {
                Some(declaration) => Some(&mut declaration.shape),
                None => None,
            },
        }
    }

    /// The number of axes of the tensor of `list` being edited.
    pub(super) fn active_axis_count(&self, list: TensorList) -> usize {
        match self.active_shape(list) {
            Some(shape) => shape.dimensions.len(),
            None => 0,
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{RequestedResourceVersion, ResourceName};

    use super::*;

    fn complete() -> InferencerDraft {
        let mut draft = InferencerDraft::default();
        draft
            .binding
            .pin
            .select_resource(ResourceName::parse("models").assured("valid resource"));
        draft
            .binding
            .pin
            .select_version(RequestedResourceVersion::Number(1));
        draft.binding.file = "models/score.onnx".into();
        let input = draft.inputs.active_mut().assured("one input exists");
        input.tensor = "features".into();
        input.shape.dimensions = vec![DimensionDraft::Size("2".into())];
        input.expression = "input.vector".into();
        let output = draft.outputs.active_mut().assured("one output exists");
        output.tensor = "score".into();
        output.shape.dimensions = vec![DimensionDraft::Size("1".into())];
        draft
    }

    fn refusal(draft: &InferencerDraft) -> ProcessorDraftError {
        draft
            .build()
            .err()
            .assured("the inferencer draft is incomplete")
            .current_context()
            .clone()
    }

    fn position(list: TensorList, number: usize) -> TensorPosition {
        TensorPosition { list, number }
    }

    /// The execution mode the form shows, with a refusal compared by its typed context.
    fn mode(
        draft: &InferencerDraft,
    ) -> Option<Result<InferencerExecutionMode, InferencerTensorSchemaError>> {
        match draft.execution_mode() {
            None => None,
            Some(Ok(mode)) => Some(Ok(mode)),
            Some(Err(refusal)) => Some(Err(refusal.current_context().clone())),
        }
    }

    #[test]
    fn tensors_need_names_readable_axes_and_expressions_in_form_order() {
        let mut draft = InferencerDraft::default();
        draft
            .binding
            .pin
            .select_resource(ResourceName::parse("models").assured("valid resource"));
        draft
            .binding
            .pin
            .select_version(RequestedResourceVersion::Latest);
        draft.binding.file = "score.onnx".into();
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::TensorName {
                tensor: position(TensorList::Inputs, 1)
            }
        );
        let input = draft.inputs.active_mut().assured("one input exists");
        input.tensor = "features".into();
        input.shape.dimensions = vec![DimensionDraft::Size("0".into())];
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::TensorDimension {
                tensor: position(TensorList::Inputs, 1),
                axis: 1
            }
        );
        let input = draft.inputs.active_mut().assured("one input exists");
        input.shape.dimensions = vec![DimensionDraft::Size("4294967296".into())];
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::TensorDimension {
                tensor: position(TensorList::Inputs, 1),
                axis: 1
            }
        );
        let input = draft.inputs.active_mut().assured("one input exists");
        input.shape.dimensions = vec![DimensionDraft::Size(" 2 ".into())];
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::TensorExpression {
                tensor: position(TensorList::Inputs, 1)
            }
        );
        draft.insert_input_field(FieldName::parse("vector").assured("valid field"));
        assert_eq!(
            draft.inputs.active().assured("one input exists").expression,
            "input.vector"
        );
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::TensorName {
                tensor: position(TensorList::Outputs, 1)
            }
        );
        draft.outputs.items.clear();
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::Tensors {
                list: TensorList::Outputs
            }
        );
        draft.inputs.items.clear();
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::Tensors {
                list: TensorList::Inputs
            }
        );
    }

    #[test]
    fn state_needs_both_ports_a_shape_and_an_initial_value() {
        let mut draft = complete();
        draft.state.add();
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::StateInput {
                tensor: position(TensorList::State, 1)
            }
        );
        let binding = draft.state.active_mut().assured("a state binding exists");
        binding.input = "state_in".into();
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::StateOutput {
                tensor: position(TensorList::State, 1)
            }
        );
        let binding = draft.state.active_mut().assured("a state binding exists");
        binding.output = "state_out".into();
        binding.shape.dimensions = vec![DimensionDraft::Size("2".into())];
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::StateInitial {
                tensor: position(TensorList::State, 1)
            }
        );
        let binding = draft.state.active_mut().assured("a state binding exists");
        binding.initial = StateInitialDraft::Constant("[1 AS".into());
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::StateConstant {
                tensor: position(TensorList::State, 1)
            }
        );
        let binding = draft.state.active_mut().assured("a state binding exists");
        binding.initial = StateInitialDraft::Constant("[1 AS F32, 10 AS F32]".into());
        let parts = draft.build().assured("the stateful draft is complete");
        assert_eq!(parts.state.len(), 1);
        assert!(matches!(
            parts.state[0].initial,
            InferencerStateInitial::Constant(_)
        ));
        let binding = draft.state.active_mut().assured("a state binding exists");
        binding.initial = StateInitialDraft::Zeros;
        let parts = draft.build().assured("the stateful draft is complete");
        assert_eq!(parts.state[0].initial, InferencerStateInitial::Zeros);
    }

    #[test]
    fn the_execution_mode_follows_the_batch_axes_by_the_models_rule() {
        let mut draft = complete();
        assert_eq!(mode(&draft), Some(Ok(InferencerExecutionMode::PerMessage)));
        let input = draft.inputs.active_mut().assured("one input exists");
        input.shape.add_dimension();
        assert_eq!(mode(&draft), None);
        let input = draft.inputs.active_mut().assured("one input exists");
        input.shape.dimensions[1] = DimensionDraft::Batch;
        assert_eq!(
            mode(&draft),
            Some(Err(InferencerTensorSchemaError::MixedExecutionModes))
        );
        // The form shows the refusal, and the server, whose rule it is, applies it.
        draft
            .build()
            .assured("mixed axes still form a complete statement");
        let output = draft.outputs.active_mut().assured("one output exists");
        output.shape.dimensions.insert(0, DimensionDraft::Batch);
        assert_eq!(mode(&draft), Some(Ok(InferencerExecutionMode::Batched)));
        draft.build().assured("a consistently batched draft builds");
        draft.state.add();
        let binding = draft.state.active_mut().assured("a state binding exists");
        binding.input = "state_in".into();
        binding.output = "state_out".into();
        binding.shape.dimensions = vec![DimensionDraft::Size("2".into())];
        binding.initial = StateInitialDraft::Zeros;
        assert_eq!(
            mode(&draft),
            Some(Err(InferencerTensorSchemaError::BatchWithState {
                tensor: "features".into()
            }))
        );
        let input = draft.inputs.active_mut().assured("one input exists");
        input.shape.remove_dimension(1);
        let output = draft.outputs.active_mut().assured("one output exists");
        output.shape.remove_dimension(0);
        output.shape.dimensions = vec![DimensionDraft::Dynamic];
        assert_eq!(mode(&draft), Some(Ok(InferencerExecutionMode::PerMessage)));
    }

    #[test]
    fn routes_read_the_output_tensors_that_name_fields() {
        let mut draft = complete();
        draft.outputs.add();
        let output = draft.outputs.active_mut().assured("a second output exists");
        output.tensor = "output:0".into();
        draft.outputs.add();
        let output = draft.outputs.active_mut().assured("a third output exists");
        output.tensor = "confidence".into();
        assert_eq!(
            draft
                .generated_fields()
                .iter()
                .map(FieldName::as_str)
                .collect::<Vec<_>>(),
            ["score", "confidence"]
        );
    }

    #[test]
    fn ordered_tensors_move_and_remove_around_the_one_being_edited() {
        let mut draft = complete();
        draft.inputs.add();
        draft
            .inputs
            .active_mut()
            .assured("a second input exists")
            .tensor = "mask".into();
        draft.inputs.move_active(true);
        assert_eq!(draft.inputs.active, 0);
        assert_eq!(
            draft
                .inputs
                .items
                .iter()
                .map(|mapping| mapping.tensor.as_str())
                .collect::<Vec<_>>(),
            ["mask", "features"]
        );
        draft.inputs.move_active(true);
        assert_eq!(draft.inputs.active, 0);
        draft.inputs.remove_active();
        assert_eq!(draft.inputs.items.len(), 1);
        assert_eq!(
            draft.inputs.active().assured("one input remains").tensor,
            "features"
        );
    }

    #[test]
    fn a_shape_edits_each_axis_in_place_and_keeps_its_typed_size() {
        let mut shape = TensorShapeDraft::default();
        assert_eq!(shape.dimension_kind(0), Some(DimensionKind::Size));
        shape.set_size(0, "3".into());
        // Choosing the kind an axis already has keeps its typed size.
        shape.set_kind(0, DimensionKind::Size);
        assert_eq!(shape.size_text(0), "3");
        shape.add_dimension();
        shape.set_kind(1, DimensionKind::Dynamic);
        // A DYNAMIC axis holds no size, so typing one into it changes nothing.
        shape.set_size(1, "7".into());
        assert_eq!(shape.size_text(1), "");
        assert_eq!(shape.dimension_kind(1), Some(DimensionKind::Dynamic));
        shape.add_dimension();
        shape.set_kind(2, DimensionKind::Batch);
        assert_eq!(shape.dimension_kind(2), Some(DimensionKind::Batch));
        // An axis that becomes a size again starts empty.
        shape.set_kind(1, DimensionKind::Size);
        assert_eq!(shape.size_text(1), "");
        // An axis past the end is neither read nor edited.
        assert_eq!(shape.dimension_kind(3), None);
        assert_eq!(shape.size_text(3), "");
        shape.set_kind(3, DimensionKind::Batch);
        shape.set_size(3, "1".into());
        shape.remove_dimension(3);
        assert_eq!(shape.dimensions.len(), 3);
        shape.remove_dimension(1);
        assert_eq!(
            shape.dimensions,
            vec![DimensionDraft::Size("3".into()), DimensionDraft::Batch]
        );
    }

    #[test]
    fn refusals_name_the_tensor_by_its_list_and_place_in_form_order() {
        assert_eq!(
            position(TensorList::Outputs, 2).to_string(),
            "OUTPUT SCHEMA tensor 2"
        );
        assert_eq!(
            ProcessorDraftError::TensorDimension {
                tensor: position(TensorList::State, 1),
                axis: 2,
            }
            .to_string(),
            "STATE tensor 1 axis 2: enter a positive whole size, or choose DYNAMIC or BATCH"
        );
    }

    #[test]
    fn each_tensor_list_is_edited_through_the_tensor_it_selects() {
        let mut draft = complete();
        assert!(TensorList::Inputs.required() && TensorList::Outputs.required());
        assert!(!TensorList::State.required());
        assert_eq!(draft.tensor_count(TensorList::State), 0);
        assert_eq!(draft.active_shape(TensorList::State), None);
        assert_eq!(draft.active_axis_count(TensorList::State), 0);
        draft.edit_tensors(TensorList::State, TensorListEdit::Add);
        assert_eq!(draft.tensor_count(TensorList::State), 1);
        assert_eq!(draft.active_axis_count(TensorList::State), 1);

        draft.edit_tensors(TensorList::Outputs, TensorListEdit::Add);
        assert_eq!(draft.active_tensor(TensorList::Outputs), 1);
        // A selection past the end keeps the tensor being edited.
        draft.edit_tensors(TensorList::Outputs, TensorListEdit::Select(5));
        assert_eq!(draft.active_tensor(TensorList::Outputs), 1);
        // The last tensor cannot move down.
        draft.edit_tensors(TensorList::Outputs, TensorListEdit::MoveActiveDown);
        assert_eq!(draft.active_tensor(TensorList::Outputs), 1);
        draft.edit_tensors(TensorList::Outputs, TensorListEdit::Select(0));
        draft.edit_tensors(TensorList::Outputs, TensorListEdit::MoveActiveDown);
        assert_eq!(draft.active_tensor(TensorList::Outputs), 1);
        assert_eq!(draft.outputs.items[1].tensor, "score");
        let shape = draft
            .active_shape_mut(TensorList::Outputs)
            .assured("the moved output is edited");
        shape.add_dimension();
        assert_eq!(draft.active_axis_count(TensorList::Outputs), 2);
        draft.edit_tensors(TensorList::Outputs, TensorListEdit::RemoveActive);
        assert_eq!(draft.tensor_count(TensorList::Outputs), 1);
        assert_eq!(draft.active_tensor(TensorList::Outputs), 0);

        draft.edit_tensors(TensorList::State, TensorListEdit::RemoveActive);
        draft.edit_tensors(TensorList::State, TensorListEdit::RemoveActive);
        assert_eq!(draft.tensor_count(TensorList::State), 0);
        assert_eq!(draft.active_tensor(TensorList::State), 0);
        // The first tensor cannot move up.
        draft.edit_tensors(TensorList::Inputs, TensorListEdit::MoveActiveUp);
        assert_eq!(draft.active_tensor(TensorList::Inputs), 0);
        assert_eq!(
            draft
                .active_shape(TensorList::Inputs)
                .assured("one input exists")
                .dimensions,
            vec![DimensionDraft::Size("2".into())]
        );
    }
}
