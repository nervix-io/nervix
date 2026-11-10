//! Full collected-window emission for Criterion.
//!
//! Layer: harness.
//! - **Owns.** A compiled required-I64 collection route and its already admitted retained rows.
//! - **Depends on.** Language lowering, VM plans and the production window emission owner.
//! - **Must not know.** Placement, transport, replication or acknowledgements from real input.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "collection emission benchmark fixture")
)]

use std::num::NonZeroUsize;

use ahash::HashMap;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use meticulous::ResultExt as _;
use nervix_execution::Executor;
use nervix_models::{CreateSchema, ParseAsType, RelayName, SchemaField, Timestamp};
use nervix_primitives::sync::{Arc, StdArc};
use nervix_vm::window::lower_window_assignments;

use super::{WindowAccumulatorPlan, WindowArgumentColumns};
use crate::{
    runtime::{
        CompiledWindowAggregateProgram, RelayMessage, WindowProcessorState,
        window_processor::{WindowAdmission, evaluate_window_aggregate},
    },
    runtime_ack::AckSet,
    runtime_schema::{
        CompiledSchema, RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow, compile_schema,
    },
};

/// Repeated emissions preserve the retained window, just as a route reads it before stepping.
pub struct WindowCollectionBenchmark {
    executor: Executor,
    compiled: CompiledWindowAggregateProgram,
    output_schema: CompiledSchema,
    state: WindowProcessorState,
}

impl WindowCollectionBenchmark {
    /// Build outside the measured loop; only emission, including output IPC qualification, is timed.
    pub fn new(rows: NonZeroUsize) -> Self {
        let input = schema("reading", "value", ParseAsType::I64);
        let output_schema = schema(
            "history",
            "values",
            ParseAsType::Vec {
                element: Box::new(ParseAsType::I64),
            },
        );
        let from = RelayName::try_from("readings").assured("the relay name is valid");
        let to = RelayName::try_from("histories").assured("the relay name is valid");
        let construction =
            nervix_plexus::parse_route_construction("SET values = COLLECT_VEC(input.value)")
                .assured("the benchmark route is valid Plexus");
        let aggregate = lower_window_assignments(&construction)
            .assured("the collection route lowers")
            .inner;
        let schemas = HashMap::from_iter([
            (from.clone(), Arc::new(input.clone())),
            (to.clone(), Arc::new(output_schema.clone())),
        ]);
        let compiled = CompiledWindowAggregateProgram::compile(
            &aggregate,
            &[from],
            &to,
            &schemas,
            None,
            nervix_vm::VmSettings::default(),
        )
        .assured("the benchmark route compiles");
        let plan = WindowAccumulatorPlan::new([&compiled.route], None, None);
        let mut state = WindowProcessorState::new(&plan, 1);
        let count = i64::try_from(rows.get()).assured("a benchmark window is addressable");
        let values: ArrayRef = StdArc::new(Int64Array::from_iter_values(0..count));
        let arguments = Arc::new(WindowArgumentColumns::single(StdArc::clone(&values)));
        let input_schema = input.arrow_schema();
        let batch = RecordBatch::try_new(StdArc::clone(&input_schema), vec![values])
            .assured("the I64 column matches");
        let batch = Arc::new(
            RuntimeRecordBatch::from_record_batch(input_schema, batch)
                .assured("the input batch is valid"),
        );
        let mut admitted = Vec::with_capacity(rows.get());
        for row in 0..rows.get() {
            let at = Timestamp::from_unix_nanos(
                i64::try_from(row).assured("the benchmark row fits i64"),
            );
            let metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(at, at);
            let record = RuntimeRow::new(Arc::clone(&batch), row, metadata)
                .assured("the row indexes its batch");
            admitted.push(WindowAdmission {
                message: RelayMessage {
                    key: None,
                    record,
                    acks: AckSet::empty(),
                },
                row,
            });
        }
        state.admit(&arguments, admitted);
        Self {
            executor: Executor::default(),
            compiled,
            output_schema,
            state,
        }
    }

    /// Evaluate the real aggregate route and qualify its decoded and encoded output limits.
    pub async fn emit(&self) -> usize {
        let output = evaluate_window_aggregate(
            &self.executor,
            &self.compiled,
            &self.state,
            &self.output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .assured("a bounded benchmark collection emits");
        output.batch().num_rows()
    }
}

fn schema(name: &str, field: &str, ty: ParseAsType) -> CompiledSchema {
    compile_schema(&CreateSchema {
        name: name.try_into().assured("the schema name is valid"),
        fields: vec![SchemaField {
            name: field.try_into().assured("the field name is valid"),
            ty,
            optional: false,
            sensitive: false,
        }],
    })
}
