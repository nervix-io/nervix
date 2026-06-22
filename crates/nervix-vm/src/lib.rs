mod batch;
mod compiler;
mod error;
mod ir;
mod runtime;
mod semantics;

pub use batch::{TypedArray, TypedBatch};
pub use compiler::{
    CompileBinding, CompileNamespace, CompileOptions, OutputMode, SchemaSensitivity,
    compile_program, compile_program_for_bindings, compile_program_for_bindings_with_sensitivity,
    compile_program_for_relay, compile_program_for_relays, compile_program_with_options,
    compile_program_with_options_for_bindings,
    compile_program_with_options_for_bindings_with_sensitivity,
    compile_program_with_options_for_relay, compile_program_with_options_for_relays,
    infer_set_expr_types_for_bindings,
};
pub use error::{CompileError, ErrorCode, RuntimeError, SideError};
pub use ir::{
    CompiledProgram, InputBinding, Instruction, InstructionKind, OutputBinding, RegisterLayout,
    RegisterLayouts, RegisterRef, RegisterSpace, RegisterType, ScalarValue,
};
pub use runtime::{
    ExecutionContext, ExecutionResult, SPAWN_BLOCKING_ROW_THRESHOLD, execute_program,
    execute_program_in_context, execute_program_with_selection,
    execute_program_with_selection_in_context,
};
pub use semantics::{
    BinaryDescriptor, BuiltinDescriptor, BuiltinLowering, CastDescriptor, DependencyScope,
    ExpressionSemantics, NullPropagation, OperationSemantics, UnaryDescriptor, Volatility,
    binary_descriptor, binary_op_semantics, binary_output_type, builtin_descriptor,
    builtin_function_semantics, builtin_signature, cast_descriptor, cast_semantics, expr_semantics,
    unary_descriptor, unary_op_semantics,
};
