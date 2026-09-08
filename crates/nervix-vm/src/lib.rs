//! The Nervix expression VM: compile an expression program once, execute it over Arrow batches.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The instruction IR and its register layout, compilation into it, the semantics of
//!   every operator, cast and builtin, and execution over a typed batch with per-row error masks.
//! - **Depends on.** The vocabulary.
//! - **Must not know.** Relays, branches, connectors, schedules or the registry. It runs one
//!   program against the bindings it was handed.
//!
//! This crate breaks its own contract: it names `nervix_nspl::vm_program` for its own AST, spans
//! and function names, so an engine depends on the language layer above it.

/// Expands a consumer over the ordinary scalar types stored in typed VM registers.
///
/// Datetime and generic arrays have distinct Arrow type and ownership rules, so their handling
/// remains next to those rules in each consumer.
macro_rules! with_typed_registers {
    ($consumer:ident) => {
        $consumer! {
            UInt8 => uint8, set_uint8, as_uint8, UInt8Array, DataType::UInt8;
            Int8 => int8, set_int8, as_int8, Int8Array, DataType::Int8;
            UInt16 => uint16, set_uint16, as_uint16, UInt16Array, DataType::UInt16;
            Int16 => int16, set_int16, as_int16, Int16Array, DataType::Int16;
            UInt32 => uint32, set_uint32, as_uint32, UInt32Array, DataType::UInt32;
            Int32 => int32, set_int32, as_int32, Int32Array, DataType::Int32;
            UInt64 => uint64, set_uint64, as_uint64, UInt64Array, DataType::UInt64;
            Int64 => int64, set_int64, as_int64, Int64Array, DataType::Int64;
            Float32 => float32, set_float32, as_float32, Float32Array, DataType::Float32;
            Float64 => float64, set_float64, as_float64, Float64Array, DataType::Float64;
            Boolean => boolean, set_boolean, as_boolean, BooleanArray, DataType::Boolean;
            Utf8 => utf8, set_utf8, as_utf8, StringArray, DataType::Utf8;
        }
    };
}

mod batch;
mod compiler;
mod error;
mod ir;
mod runtime;
mod semantics;

pub use batch::{TypedArray, TypedBatch};
pub use compiler::{
    CompileBinding, CompileNamespace, CompileOptions, InferredSetField, OutputMode,
    SchemaSensitivity, UdfParameter, UdfSignature, UdfSignatures, compile_program,
    compile_program_for_bindings, compile_program_for_bindings_with_sensitivity,
    compile_program_for_relay, compile_program_for_relays, compile_program_with_options,
    compile_program_with_options_for_bindings,
    compile_program_with_options_for_bindings_with_sensitivity,
    compile_program_with_options_for_relay, compile_program_with_options_for_relays,
    infer_set_expr_types_for_bindings, infer_set_expr_types_for_bindings_with_udfs,
};
pub use error::{
    CompileError, ErrorCode, RowErrorLengths, RowErrorMask, RowErrors, RuntimeError, SideError,
};
pub use ir::{
    CompiledProgram, InputBinding, Instruction, InstructionKind, InvocationBinding, OutputBinding,
    RegisterLayout, RegisterLayouts, RegisterRef, RegisterSpace, RegisterType, ScalarValue,
};
pub use runtime::{
    ExecutionContext, ExecutionResult, FunctionExecutionPolicy, FunctionInjector,
    FunctionInvocation, InjectedResult, RowSelection, SPAWN_BLOCKING_ROW_THRESHOLD,
    execute_program, execute_program_in_context, execute_program_with_selection,
    execute_program_with_selection_in_context,
};
pub use semantics::{
    BinaryDescriptor, BuiltinDescriptor, BuiltinLowering, CastDescriptor, DependencyScope,
    ExpressionSemantics, NullPropagation, OperationSemantics, UnaryDescriptor, Volatility,
    binary_descriptor, binary_op_semantics, binary_output_type, builtin_descriptor,
    builtin_function_semantics, builtin_signature, cast_descriptor, cast_semantics, expr_semantics,
    unary_descriptor, unary_op_semantics,
};
