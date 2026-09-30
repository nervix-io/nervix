//! Processors and generators: the nodes that read relays and write routes.

use std::num::{NonZeroU32, NonZeroUsize};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy, CreateCorrelator,
    CreateDeduplicator, CreateGenerator, CreateInferencer, CreateJunction, CreateReingestor,
    CreateReorderer, CreateWasmProcessor, CreateWindowProcessor, Expression,
    InferencerTensorDeclaration, InferencerTensorDimension, InferencerTensorElementType,
    InferencerTensorMapping, InferencerTensorRepresentation, InferencerTensorSchema,
    RequestedResourceVersion, WasmProcessorLimits, WasmRejectedStatePolicy, WindowBound,
    WindowStateLimit,
};

use crate::{
    Arbitrary, Domain,
    route::{RouteBranch, RouteFlush, RouteShape},
};

/// The most items a generated key list, tensor list or tensor shape holds.
const ITEMS: usize = 3;

/// A window's width and the step it advances by.
#[derive(Debug, Clone)]
struct WindowBounds {
    width: WindowBound,
    step: WindowBound,
}

impl Arbitrary<'_> {
    /// A junction merging its inputs into transforming routes.
    pub fn create_junction(&mut self) -> CreateJunction {
        CreateJunction {
            name: self.name(),
            from: self.processor_inputs(true),
            output_routes: self.processor_outputs(
                RouteShape::Transforming,
                RouteFlush::Required,
                RouteBranch::NodeWide,
            ),
            branched_by: self.branch_selection(),
            mode: self.ack_mode(),
            filter_where: self.optional_expression(),
            materialized_state: self.materialized_state(),
        }
    }

    /// A deduplicator keyed by one or more expressions.
    pub fn create_deduplicator(&mut self) -> CreateDeduplicator {
        CreateDeduplicator {
            name: self.name(),
            from: self.processor_inputs(true),
            output_routes: self.processor_outputs(
                RouteShape::Transforming,
                RouteFlush::Required,
                RouteBranch::NodeWide,
            ),
            branched_by: self.branch_selection(),
            deduplicate_on: self.key_expressions(),
            max_time: self.duration(),
            mode: self.ack_mode(),
            filter_where: self.optional_expression(),
            materialized_state: self.materialized_state(),
        }
    }

    /// A reingestor, whose every route constructs its own outgoing branch.
    pub fn create_reingestor(&mut self) -> CreateReingestor {
        CreateReingestor {
            name: self.name(),
            from: self.processor_inputs(true),
            output_routes: self.processor_outputs(
                RouteShape::Transforming,
                RouteFlush::Required,
                RouteBranch::PerRoute,
            ),
            mode: self.ack_mode(),
            materialized_state: self.materialized_state(),
            filter_where: self.optional_expression(),
        }
    }

    /// A correlator matching its left and right inputs by a condition.
    pub fn create_correlator(&mut self) -> CreateCorrelator {
        CreateCorrelator {
            name: self.name(),
            left: self.processor_inputs(true),
            right: self.processor_inputs(true),
            output_routes: self.processor_outputs(
                RouteShape::SetOnly,
                RouteFlush::Required,
                RouteBranch::NodeWide,
            ),
            branched_by: self.branch_selection(),
            correlate_where: self.expression(),
            match_policy: self.entropy.pick([
                CorrelatorMatchPolicy::Earliest,
                CorrelatorMatchPolicy::Latest,
            ]),
            max_time: self.duration(),
            timeout_policy: CorrelationTimeoutPolicy {
                left: self.correlation_timeout_action(),
                right: self.correlation_timeout_action(),
            },
            mode: self.ack_mode(),
            filter_where: self.correlator_filter(),
            materialized_state: self.materialized_state(),
        }
    }

    /// A correlator's filter. NSPL declares none for a correlator, whose inputs filter each side
    /// on their own; only the vocabulary can hold one.
    fn correlator_filter(&mut self) -> Option<Expression> {
        match self.domain {
            Domain::Nspl => None,
            Domain::Vocabulary => self.optional_expression(),
        }
    }

    fn correlation_timeout_action(&mut self) -> CorrelationTimeoutAction {
        if self.entropy.flag() {
            CorrelationTimeoutAction::Drop
        } else {
            CorrelationTimeoutAction::SendTo { relay: self.name() }
        }
    }

    /// A reorderer ordering by one or more expressions.
    pub fn create_reorderer(&mut self) -> CreateReorderer {
        CreateReorderer {
            name: self.name(),
            from: self.processor_inputs(true),
            output_routes: self.processor_outputs(
                RouteShape::Transforming,
                RouteFlush::Required,
                RouteBranch::NodeWide,
            ),
            branched_by: self.branch_selection(),
            order_by: self.key_expressions(),
            max_time: self.duration(),
            mode: self.ack_mode(),
            filter_where: self.optional_expression(),
            materialized_state: self.materialized_state(),
        }
    }

    fn key_expressions(&mut self) -> Vec<Expression> {
        let count = self.entropy.positive_count(
            NonZeroUsize::new(ITEMS).assured("a key holds at least one expression"),
        );
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            keys.push(self.expression());
        }
        keys
    }

    /// A window processor, whose set-only routes emit on its width and step rather than a flush.
    pub fn create_window_processor(&mut self) -> CreateWindowProcessor {
        let bounds = self.window_bounds();
        CreateWindowProcessor {
            name: self.name(),
            from: self.processor_inputs(true),
            output_routes: self.processor_outputs(
                RouteShape::SetOnly,
                RouteFlush::Absent,
                RouteBranch::NodeWide,
            ),
            branched_by: self.branch_selection(),
            width: bounds.width,
            step: bounds.step,
            state_limit: if self.entropy.flag() {
                WindowStateLimit::MaxBytes(self.positive_u64())
            } else {
                WindowStateLimit::Unbounded
            },
            mode: self.ack_mode(),
            filter_where: self.optional_expression(),
            materialized_state: self.materialized_state(),
        }
    }

    /// A width and the step within it. Each bound counts messages, measures a duration, or does
    /// both; the step measures only what the width measures, and never more than the width.
    fn window_bounds(&mut self) -> WindowBounds {
        let messages = self.entropy.any_u64();
        let unit = self.entropy.pick(["ns", "us", "ms", "s", "m", "h", "d"]);
        let length = self.entropy.boundary_biased(0..=u64::from(u32::MAX));
        let step_messages = self.entropy.boundary_biased(0..=messages);
        let step_length = self.entropy.boundary_biased(0..=length);
        let duration = format!("{length}{unit}");
        let step_duration = format!("{step_length}{unit}");
        match self.entropy.byte() % 5 {
            0 => WindowBounds {
                width: WindowBound::of_messages(messages),
                step: WindowBound::of_messages(step_messages),
            },
            1 => WindowBounds {
                width: WindowBound::of_duration(duration),
                step: WindowBound::of_duration(step_duration),
            },
            width_and_step => {
                let width = WindowBound {
                    messages: Some(messages),
                    duration: Some(duration),
                };
                let step = match width_and_step {
                    2 => WindowBound::of_messages(step_messages),
                    3 => WindowBound::of_duration(step_duration),
                    _ => WindowBound {
                        messages: Some(step_messages),
                        duration: Some(step_duration),
                    },
                };
                WindowBounds { width, step }
            }
        }
    }

    /// An inferencer mapping expressions onto the input tensors of a model resource.
    pub fn create_inferencer(&mut self) -> CreateInferencer<RequestedResourceVersion> {
        let batched = self.entropy.flag();
        let inputs_count = self
            .entropy
            .positive_count(NonZeroUsize::new(ITEMS).assured("an inferencer reads a tensor"));
        let mut inputs = Vec::with_capacity(inputs_count);
        for _ in 0..inputs_count {
            inputs.push(InferencerTensorMapping {
                tensor: self.string(),
                schema: self.tensor_schema(batched),
                expression: self.expression(),
            });
        }
        let outputs_count = self
            .entropy
            .positive_count(NonZeroUsize::new(ITEMS).assured("an inferencer writes a tensor"));
        let mut output_schema = Vec::with_capacity(outputs_count);
        for _ in 0..outputs_count {
            output_schema.push(InferencerTensorDeclaration {
                tensor: self.string(),
                schema: self.tensor_schema(batched),
            });
        }
        CreateInferencer {
            name: self.name(),
            from: self.processor_inputs(true),
            output_routes: self.processor_outputs(
                RouteShape::SetOnly,
                RouteFlush::Required,
                RouteBranch::NodeWide,
            ),
            branched_by: self.branch_selection(),
            resource: self.name(),
            resource_version: self.requested_version(),
            file: self.string(),
            inputs,
            output_schema,
            mode: self.ack_mode(),
            filter_where: self.optional_expression(),
            materialized_state: self.materialized_state(),
        }
    }

    /// A dense `F32` tensor shape. A batched inferencer gives every tensor exactly one `BATCH`
    /// axis; a per-message one gives none.
    fn tensor_schema(&mut self, batched: bool) -> InferencerTensorSchema {
        let count = self.entropy.count(ITEMS);
        let mut dimensions = Vec::with_capacity(count);
        for _ in 0..count {
            let dimension = if self.entropy.flag() {
                InferencerTensorDimension::Dynamic
            } else {
                let size = self.entropy.boundary_biased(1..=u64::from(u32::MAX));
                let size = u32::try_from(size).verified("the range above ends at u32::MAX");
                InferencerTensorDimension::Fixed(
                    NonZeroU32::new(size).verified("the range above starts at one"),
                )
            };
            dimensions.push(dimension);
        }
        if batched {
            let position = self.entropy.count(dimensions.len());
            dimensions.insert(position, InferencerTensorDimension::Batch);
        }
        InferencerTensorSchema {
            representation: InferencerTensorRepresentation::Dense,
            element_type: InferencerTensorElementType::F32,
            dimensions,
        }
    }

    /// A WASM processor, whose guest owns its emission cadence, so no route flushes.
    pub fn create_wasm_processor(&mut self) -> CreateWasmProcessor<RequestedResourceVersion> {
        CreateWasmProcessor {
            name: self.name(),
            from: self.processor_inputs(true),
            output_routes: self.processor_outputs(
                RouteShape::SetOnly,
                RouteFlush::Absent,
                RouteBranch::NodeWide,
            ),
            branched_by: self.branch_selection(),
            resource: self.name(),
            resource_version: self.requested_version(),
            file: self.string(),
            limits: WasmProcessorLimits {
                max_fuel: self.positive_u64(),
                max_memory_bytes: self.positive_u64(),
            },
            global_error_policy: self.general_error_policy(),
            rejected_state_policy: self.entropy.pick([
                WasmRejectedStatePolicy::Preserve,
                WasmRejectedStatePolicy::Reset,
            ]),
            mode: self.ack_mode(),
            filter_where: self.optional_expression(),
            materialized_state: self.materialized_state(),
        }
    }

    /// A generator emitting set-only routes from one materialized relay on a clock period.
    pub fn create_generator(&mut self) -> CreateGenerator {
        CreateGenerator {
            name: self.name(),
            materialized_relay: self.name(),
            branched_by: self.branch_selection(),
            each: self.clock_period(),
            output_routes: self.processor_outputs(
                RouteShape::SetOnly,
                RouteFlush::Required,
                RouteBranch::NodeWide,
            ),
        }
    }
}
