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
