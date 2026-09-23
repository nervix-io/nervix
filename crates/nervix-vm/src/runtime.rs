//! Columnar expression execution.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Executing compiled VM programs against typed Arrow batches and calling injected
//!   functions with one explicit execution context.
//! - **Depends on.** Compiled VM IR, Arrow kernels and vocabulary timestamps.
//! - **Must not know.** Domains, branches, runtime clock installation or physical deadlines.

use std::{
    cell::OnceCell,
    fmt::{self, Write as _},
    ops::Range,
    sync::Arc as StdArc,
};

use ahash::{HashMap, HashMapExt};
use arch_into::ArchInto as _;
use arrow_arith::{
    aggregate::sum_checked as arrow_sum_checked,
    boolean::{and_kleene, is_null, not, or_kleene},
};
use arrow_array::{
    Array, ArrayRef, ArrowNumericType, BooleanArray, Datum, FixedSizeListArray, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, PrimitiveArray,
    StringArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    builder::{BooleanBuilder, Int64Builder, PrimitiveBuilder, StringBuilder},
    new_null_array,
    types::{
        ArrowPrimitiveType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_cast::{
    cast::{CastOptions, cast_with_options},
    display::FormatOptions,
};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{ArrowError, DataType};
use arrow_select::{
    filter::FilterBuilder,
    nullif::nullif,
    take::{TakeOptions, take},
    zip::zip,
};
use arrow_string::like::{
    contains as string_contains, ends_with as string_ends_with, starts_with as string_starts_with,
};
use chrono::DateTime;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::Timestamp;
use tokio::task;
use uuid::{NoContext, Timestamp as UuidTimestamp, Uuid};

use crate::{
    batch::{TypedArray, TypedBatch},
    datetime::{self, FormattedColumn, TextFailure, UnitCounts, UnresolvedLocalTime},
    error::{
        DatetimeOperation, FloatOperation, IntegerOperation, RowErrorMask, RowErrors, RuntimeError,
        SideError, SideErrorReason,
    },
    ir::{
        AssignmentFallback, CompiledPredicate, CompiledProgram, InputBinding, Instruction,
        InstructionKind, RegisterLayout, RegisterLayouts, RegisterRef, RegisterSpace, RegisterType,
        ScalarValue, SelectArm,
    },
    numeric::{
        self, Arithmetic, BinaryMathFunction, Checked, CheckedFloat, CheckedInteger, Comparison,
        DecimalRounding, F64Operand, IntegerRounding, MathFunction, Rounding, RoundingDigits,
        Shift, ShiftCounts, ShiftedInteger, SignedInteger,
    },
    operand::{Broadcast, Operand},
    program::{BinaryOp, DatetimeFunction, FunctionName, Span, UnaryOp},
    regexp::{ActivePattern, BatchPatterns, PatternSource, RegexpCall, RegexpFunction},
    semantics::{
        BitwiseOperation, BuiltinLowering, CaseMapping, FloatClass, Volatility,
        builtin_semantics_for_lowering,
    },
};

pub const SPAWN_BLOCKING_ROW_THRESHOLD: usize = 1_024;

/// One register's value during an execution.
#[derive(Clone)]
enum Register<A> {
    /// One value per row of the batch.
    Column(A),
    /// One value every row of the batch shares, held as a one-row array.
    ///
    /// The column repeating it over the batch's rows is built at most once, the first time a
    /// kernel without a scalar form reads the register, and is dropped with the register.
    Scalar { value: A, column: OnceCell<A> },
}

/// Whether a value written to a register is a column of the batch or one scalar every row shares.
#[derive(Clone, Copy)]
enum Shape {
    Column,
    Scalar,
}

impl<A: Broadcast> Register<A> {
    fn with_shape(value: A, shape: Shape) -> Self {
        match shape {
            Shape::Column => Self::Column(value),
            Shape::Scalar => Self::Scalar {
                value,
                column: OnceCell::new(),
            },
        }
    }

    fn is_scalar(&self) -> bool {
        match self {
            Self::Column(_) => false,
            Self::Scalar { .. } => true,
        }
    }

    /// The register as a column of `rows` rows. A scalar read as one row is that row itself;
    /// otherwise it is expanded once and the expansion kept.
    fn column(&self, rows: usize) -> &A {
        match self {
            Self::Column(array) => array,
            Self::Scalar { value, .. } if rows == 1 => value,
            Self::Scalar { value, column } => column.get_or_init(|| value.broadcast(rows)),
        }
    }

    /// The register as a kernel operand: a column as it is, and a scalar as a scalar, except that
    /// a scalar read as one row is a one-row column.
    fn operand(&self, rows: usize) -> Operand<'_, A> {
        match self {
            Self::Column(array) => Operand::Column(array),
            Self::Scalar { value, .. } if rows == 1 => Operand::Column(value),
            Self::Scalar { value, .. } => Operand::Scalar(value),
        }
    }
}

/// An array type a register bank stores, which knows the bank slots of its type.
trait RegisterArray: Broadcast + Clone {
    const TYPE: RegisterType;
    const LABEL: &'static str;

    fn slot(bank: &TypedBank, index: usize) -> Option<&Register<Self>>;

    fn slot_mut(bank: &mut TypedBank, index: usize) -> Option<&mut Option<Register<Self>>>;
}

macro_rules! declare_typed_bank {
    ($($Variant:ident => $field:ident, $setter:ident, $accessor:ident, $Array:ty, $data_type:path;)+) => {
        struct TypedBank {
            $($field: Vec<Option<Register<$Array>>>,)+
            datetime: Vec<Option<Register<TimestampNanosecondArray>>>,
            generic: Vec<Option<Register<ArrayRef>>>,
        }

        impl TypedBank {
            fn new(layout: &RegisterLayout) -> Self {
                Self {
                    $($field: vec![None; layout.$field],)+
                    datetime: vec![None; layout.datetime],
                    generic: vec![None; layout.generic],
                }
            }
        }

        $(impl RegisterArray for $Array {
            const TYPE: RegisterType = RegisterType::$Variant;
            const LABEL: &'static str = stringify!($Array);

            fn slot(bank: &TypedBank, index: usize) -> Option<&Register<Self>> {
                bank.$field.get(index).and_then(Option::as_ref)
            }

            fn slot_mut(
                bank: &mut TypedBank,
                index: usize,
            ) -> Option<&mut Option<Register<Self>>> {
                bank.$field.get_mut(index)
            }
        })+
    };
}

with_typed_registers!(declare_typed_bank);

impl RegisterArray for TimestampNanosecondArray {
    const TYPE: RegisterType = RegisterType::Datetime;
    const LABEL: &'static str = "TimestampNanosecondArray";

    fn slot(bank: &TypedBank, index: usize) -> Option<&Register<Self>> {
        bank.datetime.get(index).and_then(Option::as_ref)
    }

    fn slot_mut(bank: &mut TypedBank, index: usize) -> Option<&mut Option<Register<Self>>> {
        bank.datetime.get_mut(index)
    }
}

impl RegisterArray for ArrayRef {
    const TYPE: RegisterType = RegisterType::Generic;
    const LABEL: &'static str = "ArrayRef";

    fn slot(bank: &TypedBank, index: usize) -> Option<&Register<Self>> {
        bank.generic.get(index).and_then(Option::as_ref)
    }

    fn slot_mut(bank: &mut TypedBank, index: usize) -> Option<&mut Option<Register<Self>>> {
        bank.generic.get_mut(index)
    }
}

struct RegisterBank {
    inputs: TypedBank,
    temps: TypedBank,
    condition: TypedBank,
    outputs: TypedBank,
    uninitialized: HashMap<RegisterRef, DataType>,
    /// How many rows a column read from the bank has: the batch's rows, or one while an
    /// instruction whose operands are all scalars computes its shared value over a single row.
    rows: usize,
}

macro_rules! impl_register_bank {
    ($($Variant:ident => $field:ident, $setter:ident, $accessor:ident, $Array:ty, $data_type:path;)+) => {
        impl RegisterBank {
            fn new(layouts: &RegisterLayouts, rows: usize) -> Self {
                Self {
                    inputs: TypedBank::new(&layouts.inputs),
                    temps: TypedBank::new(&layouts.temps),
                    condition: TypedBank::new(&layouts.condition),
                    outputs: TypedBank::new(&layouts.outputs),
                    uninitialized: HashMap::new(),
                    rows,
                }
            }

            fn load_input_batch(
                &mut self,
                inputs: &[InputBinding],
                batch: &TypedBatch,
            ) -> Result<(), RuntimeError> {
                for input in inputs {
                    self.set(input.reg, batch.column(input.column_index).clone(), Shape::Column)?;
                }
                Ok(())
            }

            fn bank(&self, space: RegisterSpace) -> &TypedBank {
                match space {
                    RegisterSpace::Input => &self.inputs,
                    RegisterSpace::Temp => &self.temps,
                    RegisterSpace::Condition => &self.condition,
                    RegisterSpace::Output => &self.outputs,
                }
            }

            fn bank_mut(&mut self, space: RegisterSpace) -> &mut TypedBank {
                match space {
                    RegisterSpace::Input => &mut self.inputs,
                    RegisterSpace::Temp => &mut self.temps,
                    RegisterSpace::Condition => &mut self.condition,
                    RegisterSpace::Output => &mut self.outputs,
                }
            }

            fn register<A: RegisterArray>(
                &self,
                reg: RegisterRef,
            ) -> Result<&Register<A>, RuntimeError> {
                self.ensure_type(reg, A::TYPE, A::LABEL)?;
                A::slot(self.bank(reg.space), reg.index)
                    .ok_or(RuntimeError::MissingRegister { reg })
            }

            /// The register as a column of the current row count.
            fn column<A: RegisterArray>(&self, reg: RegisterRef) -> Result<&A, RuntimeError> {
                Ok(self.register::<A>(reg)?.column(self.rows))
            }

            /// The register as a kernel operand, which keeps a scalar a scalar.
            fn operand<A: RegisterArray>(
                &self,
                reg: RegisterRef,
            ) -> Result<Operand<'_, A>, RuntimeError> {
                Ok(self.register::<A>(reg)?.operand(self.rows))
            }

            fn store<A: RegisterArray>(
                &mut self,
                reg: RegisterRef,
                value: Register<A>,
            ) -> Result<(), RuntimeError> {
                self.ensure_type(reg, A::TYPE, A::LABEL)?;
                let slot = A::slot_mut(self.bank_mut(reg.space), reg.index)
                    .ok_or(RuntimeError::MissingRegister { reg })?;
                *slot = Some(value);
                Ok(())
            }

            /// Whether the register holds one value every row shares. A register nothing has
            /// written yet holds no scalar; reading it reports the missing register.
            fn is_scalar(&self, reg: RegisterRef) -> bool {
                let register_is_scalar = match reg.ty {
                    $(RegisterType::$Variant => {
                        self.register::<$Array>(reg).map(Register::is_scalar)
                    })+
                    RegisterType::Datetime => self
                        .register::<TimestampNanosecondArray>(reg)
                        .map(Register::is_scalar),
                    RegisterType::Generic => self.register::<ArrayRef>(reg).map(Register::is_scalar),
                };
                register_is_scalar.unwrap_or(false)
            }

            /// Copies `src` into `dst`, shape included.
            fn copy(&mut self, dst: RegisterRef, src: RegisterRef) -> Result<(), RuntimeError> {
                self.uninitialized.remove(&dst);
                match src.ty {
                    $(RegisterType::$Variant => {
                        let value = self.register::<$Array>(src)?.clone();
                        self.store(dst, value)
                    })+
                    RegisterType::Datetime => {
                        let value = self.register::<TimestampNanosecondArray>(src)?.clone();
                        self.store(dst, value)
                    }
                    RegisterType::Generic => {
                        let value = self.register::<ArrayRef>(src)?.clone();
                        self.store(dst, value)
                    }
                }
            }

            /// The register as an operand of whatever array type it holds.
            fn any_operand(
                &self,
                reg: RegisterRef,
            ) -> Result<Operand<'_, dyn Array>, RuntimeError> {
                match reg.ty {
                    $(RegisterType::$Variant => Ok(self.operand::<$Array>(reg)?.erased()),)+
                    RegisterType::Datetime => {
                        Ok(self.operand::<TimestampNanosecondArray>(reg)?.erased())
                    }
                    RegisterType::Generic => Ok(self.operand::<ArrayRef>(reg)?.erased()),
                }
            }

            /// Writes `value` as a column of the batch, or as one scalar every row shares when
            /// it is a one-row array holding that value.
            fn set(
                &mut self,
                reg: RegisterRef,
                value: TypedArray,
                shape: Shape,
            ) -> Result<(), RuntimeError> {
                self.uninitialized.remove(&reg);
                match value {
                    $(TypedArray::$Variant(array) => {
                        self.store(reg, Register::with_shape(array, shape))
                    })+
                    TypedArray::Datetime(array) => {
                        self.store(reg, Register::with_shape(array, shape))
                    }
                    TypedArray::Generic(array) => {
                        self.store(reg, Register::with_shape(array, shape))
                    }
                    TypedArray::Uninitialized { data_type, len } => {
                        let materialized =
                            array_ref_to_typed_array(new_null_array(&data_type, len))?;
                        self.set(reg, materialized, shape)?;
                        self.uninitialized.insert(reg, data_type);
                        Ok(())
                    }
                }
            }

            fn output_array(&self, reg: RegisterRef) -> Result<TypedArray, RuntimeError> {
                if let Some(data_type) = self.uninitialized.get(&reg) {
                    return Ok(TypedArray::uninitialized(
                        data_type.clone(),
                        self.read_array(reg)?.len(),
                    ));
                }
                self.read_array(reg)
            }

            /// The register as a column of the current row count.
            fn read_array(&self, reg: RegisterRef) -> Result<TypedArray, RuntimeError> {
                match reg.ty {
                    $(RegisterType::$Variant => {
                        Ok(TypedArray::$Variant(self.column::<$Array>(reg)?.clone()))
                    },)+
                    RegisterType::Datetime => Ok(TypedArray::Datetime(
                        self.column::<TimestampNanosecondArray>(reg)?.clone(),
                    )),
                    RegisterType::Generic => {
                        Ok(TypedArray::Generic(self.column::<ArrayRef>(reg)?.clone()))
                    }
                }
            }

            fn ensure_type(
                &self,
                reg: RegisterRef,
                expected: RegisterType,
                label: &'static str,
            ) -> Result<(), RuntimeError> {
                if reg.ty == expected {
                    Ok(())
                } else {
                    Err(RuntimeError::InvalidRegisterType {
                        reg,
                        expected: label,
                    })
                }
            }
        }
    };
}

with_typed_registers!(impl_register_bank);

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionResult {
    pub batch: TypedBatch,
    pub selected_rows: RowSelection,
    pub invocations: Vec<FunctionInvocation>,
}

/// The only execution result exposed by a compiled predicate.
#[derive(Debug, Clone, PartialEq)]
pub struct PredicateExecutionResult {
    selected_rows: RowSelection,
    errors: RowErrors,
}

impl PredicateExecutionResult {
    pub fn selected_rows(&self) -> &RowSelection {
        &self.selected_rows
    }

    pub fn errors(&self) -> &RowErrors {
        &self.errors
    }
}

/// Maps output rows back to input rows without allocating for the identity case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowSelection {
    All(usize),
    Selected(Vec<usize>),
}

impl RowSelection {
    pub fn len(&self) -> usize {
        match self {
            Self::All(row_count) => *row_count,
            Self::Selected(rows) => rows.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_single(&self, row: usize) -> bool {
        match self {
            Self::All(1) => row == 0,
            Self::Selected(rows) => rows.as_slice() == [row],
            Self::All(_) => false,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        let (all, selected) = match self {
            Self::All(row_count) => (0..*row_count, &[][..]),
            Self::Selected(rows) => (0..0, rows.as_slice()),
        };
        all.chain(selected.iter().copied())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionInvocation {
    pub function: FunctionName,
    pub arguments: Vec<TypedArray>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionExecutionPolicy {
    Inline,
    SpawnBlocking,
}

pub trait FunctionInjector: Send + Sync + fmt::Debug {
    fn execution_policy(&self, _function: &FunctionName) -> FunctionExecutionPolicy {
        FunctionExecutionPolicy::Inline
    }

    fn inject_with_context(
        &self,
        function: &FunctionName,
        arguments: &[TypedArray],
        row_count: usize,
        span: Span,
        now: Timestamp,
        prior_error_rows: RowErrorMask<'_>,
    ) -> Result<InjectedResult, RuntimeError>;
}

#[derive(Debug, Clone)]
pub struct InjectedResult {
    pub output: TypedArray,
    pub side_errors: Vec<(usize, SideError)>,
}

impl InjectedResult {
    pub fn success(output: TypedArray) -> Self {
        Self {
            output,
            side_errors: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionContext {
    pub now: Timestamp,
    pub injector: Option<triomphe::Arc<Box<dyn FunctionInjector>>>,
}

impl ExecutionContext {
    pub const fn new(now: Timestamp) -> Self {
        Self {
            now,
            injector: None,
        }
    }
}

/// Executes a predicate without exposing constructed columns or invocations.
///
/// A construction-capable program cannot be passed through this entry point:
///
/// ```compile_fail
/// use nervix_vm::{CompiledProgram, ExecutionContext, TypedBatch, execute_predicate_in_context};
///
/// async fn execute(program: &CompiledProgram, batch: &TypedBatch, context: &ExecutionContext) {
///     let _ = execute_predicate_in_context(program, batch, context).await;
/// }
/// ```
pub async fn execute_predicate_in_context(
    predicate: &CompiledPredicate,
    batch: &TypedBatch,
    context: &ExecutionContext,
) -> Result<PredicateExecutionResult, Report<RuntimeError>> {
    let result =
        execute_program_with_selection_in_context(predicate.program(), batch, context).await?;
    Ok(PredicateExecutionResult {
        selected_rows: result.selected_rows,
        errors: result.batch.errors().clone(),
    })
}

pub async fn execute_program_in_context(
    program: &triomphe::Arc<CompiledProgram>,
    batch: &TypedBatch,
    context: &ExecutionContext,
) -> Result<ExecutionResult, RuntimeError> {
    execute_program_with_selection_in_context(program, batch, context).await
}

pub async fn execute_program_with_selection_in_context(
    program: &triomphe::Arc<CompiledProgram>,
    batch: &TypedBatch,
    context: &ExecutionContext,
) -> Result<ExecutionResult, RuntimeError> {
    if batch.row_count() <= SPAWN_BLOCKING_ROW_THRESHOLD
        && !program_requires_spawn_blocking(program, context)
    {
        return execute_program_with_selection_in_context_sync(program, batch, context);
    }

    let program = program.clone();
    let batch = batch.clone();
    let context = context.clone();
    task::spawn_blocking(move || {
        execute_program_with_selection_in_context_sync(&program, &batch, &context)
    })
    .await
    .map_err(|error| RuntimeError::BlockingExecutionFailed {
        message: error.to_string(),
    })?
}

fn program_requires_spawn_blocking(program: &CompiledProgram, context: &ExecutionContext) -> bool {
    program.instructions.iter().any(|instruction| {
        let InstructionKind::Inject { function, .. } = &instruction.kind else {
            return false;
        };
        context.injector.as_ref().is_some_and(|injector| {
            injector.execution_policy(function) == FunctionExecutionPolicy::SpawnBlocking
        }) || program.injector.as_ref().is_some_and(|injector| {
            injector.execution_policy(function) == FunctionExecutionPolicy::SpawnBlocking
        })
    })
}

#[cfg(test)]
fn execute_program_sync(
    program: &CompiledProgram,
    batch: &TypedBatch,
) -> Result<TypedBatch, RuntimeError> {
    let context = ExecutionContext::new(Timestamp::from_unix_nanos(0));
    execute_program_in_context_sync(program, batch, &context).map(|result| result.batch)
}

#[cfg(test)]
fn execute_program_in_context_sync(
    program: &CompiledProgram,
    batch: &TypedBatch,
    context: &ExecutionContext,
) -> Result<ExecutionResult, RuntimeError> {
    execute_program_with_selection_in_context_sync(program, batch, context)
}

#[cfg(test)]
fn execute_program_with_selection_sync(
    program: &CompiledProgram,
    batch: &TypedBatch,
) -> Result<ExecutionResult, RuntimeError> {
    let context = ExecutionContext::new(Timestamp::from_unix_nanos(0));
    execute_program_with_selection_in_context_sync(program, batch, &context)
}

fn execute_program_with_selection_in_context_sync(
    program: &CompiledProgram,
    batch: &TypedBatch,
    context: &ExecutionContext,
) -> Result<ExecutionResult, RuntimeError> {
    if batch.schema().as_ref() != program.input_schema.as_ref() {
        return Err(RuntimeError::SchemaMismatch);
    }

    let mut registers = RegisterBank::new(&program.layouts, batch.row_count());
    registers.load_input_batch(&program.inputs, batch)?;

    let mut row_errors = batch.errors().clone();

    for instruction in &program.instructions {
        let baseline = instruction.error_mask.map(|_| row_errors.row_lengths());
        instruction.execute(
            &mut registers,
            batch.row_count(),
            &mut row_errors,
            context,
            program.injector.as_ref(),
        )?;
        if let (Some(mask_reg), Some(baseline)) = (instruction.error_mask, baseline) {
            let mask = registers.column::<BooleanArray>(mask_reg)?;
            row_errors.restore_unselected(&baseline, |row| row_selected(mask, row));
        }
    }

    let mut columns = Vec::with_capacity(program.outputs.len());
    for output in &program.outputs {
        columns.push(registers.output_array(output.reg)?);
    }
    let mut invocations = Vec::with_capacity(program.invocations.len());
    for invocation in &program.invocations {
        // The function name is owned before any fallible argument lookup, which fixes the binding
        // materialization order for both successful and failed invocations.
        let function = invocation.function.clone();
        let span = invocation.span;
        let mut arguments = Vec::with_capacity(invocation.inputs.len());
        for input in &invocation.inputs {
            arguments.push(registers.output_array(*input)?);
        }
        invocations.push(FunctionInvocation {
            function,
            arguments,
            span,
        });
    }

    let global_predicate = if let Some(filter_reg) = program.filter {
        Some(registers.column::<BooleanArray>(filter_reg)?.clone())
    } else {
        None
    };

    /// The program output after the global filter has been applied, which narrows the columns and
    /// the per-row errors together with the selection that says which input rows survived.
    struct FilteredOutput {
        columns: Vec<TypedArray>,
        row_errors: RowErrors,
        selected_rows: RowSelection,
    }

    let filtered = if let Some(predicate) = global_predicate.as_ref() {
        let predicate_with_error_rows = if row_errors.is_error_free() {
            None
        } else {
            Some(BooleanArray::from_iter(predicate.iter().enumerate().map(
                |(row, selected)| {
                    if row_errors.row(row).is_empty() {
                        selected
                    } else {
                        Some(true)
                    }
                },
            )))
        };
        let selection_predicate = predicate_with_error_rows.as_ref().unwrap_or(predicate);
        let selected = selected_rows(selection_predicate);
        for invocation in &mut invocations {
            invocation.arguments =
                filter_columns(&invocation.arguments, selection_predicate, selected.len())?;
        }
        FilteredOutput {
            columns: filter_columns(&columns, selection_predicate, selected.len())?,
            row_errors: row_errors.select_rows(&selected),
            selected_rows: RowSelection::Selected(selected),
        }
    } else {
        FilteredOutput {
            columns,
            row_errors,
            selected_rows: RowSelection::All(batch.row_count()),
        }
    };

    Ok(ExecutionResult {
        batch: TypedBatch::with_errors(
            program.output_schema.clone(),
            filtered.columns,
            filtered.row_errors,
        )?,
        selected_rows: filtered.selected_rows,
        invocations,
    })
}

impl Instruction {
    fn execute(
        &self,
        registers: &mut RegisterBank,
        row_count: usize,
        row_errors: &mut RowErrors,
        context: &ExecutionContext,
        default_injector: Option<&triomphe::Arc<Box<dyn FunctionInjector>>>,
    ) -> Result<(), RuntimeError> {
        match &self.kind {
            InstructionKind::Move { dst, input } => registers.copy(*dst, *input),
            InstructionKind::Assign {
                dst,
                input,
                fallback,
            } => self.execute_assign(registers, *dst, *input, fallback, row_errors),
            InstructionKind::Literal { dst, value } => write_literal(registers, *dst, value),
            InstructionKind::NullLiteral { dst, data_type } => {
                write_null_literal(registers, *dst, data_type)
            }
            InstructionKind::Uninitialized { dst, data_type } => registers.set(
                *dst,
                TypedArray::uninitialized(data_type.clone(), row_count),
                Shape::Column,
            ),
            InstructionKind::Unary { dst, input, op } => self.write_computed(
                registers,
                *dst,
                false,
                row_count,
                row_errors,
                |registers, _, row_errors| self.execute_unary(registers, *input, *op, row_errors),
            ),
            InstructionKind::Binary {
                dst,
                left,
                right,
                op,
            } => self.write_computed(
                registers,
                *dst,
                false,
                row_count,
                row_errors,
                |registers, _, row_errors| {
                    self.execute_binary(registers, *left, *right, *op, row_errors)
                },
            ),
            InstructionKind::Cast { dst, input, target } => self.write_computed(
                registers,
                *dst,
                false,
                row_count,
                row_errors,
                |registers, _, row_errors| {
                    cast_typed_array(
                        registers.read_array(*input)?,
                        *target,
                        row_errors,
                        self.span,
                    )
                },
            ),
            InstructionKind::Builtin {
                dst,
                lowering,
                inputs,
            } => {
                let volatile =
                    builtin_semantics_for_lowering(lowering).volatility == Volatility::Volatile;
                self.write_computed(
                    registers,
                    *dst,
                    volatile,
                    row_count,
                    row_errors,
                    |registers, rows, row_errors| {
                        execute_builtin(
                            lowering, registers, inputs, rows, row_errors, self.span, context,
                        )
                    },
                )
            }
            InstructionKind::Inject {
                dst,
                function,
                inputs,
                output_type,
            } => {
                let arguments = inputs
                    .iter()
                    .map(|input| registers.read_array(*input))
                    .collect::<Result<Vec<_>, _>>()?;
                let prior_error_rows = row_errors.mask();
                let inject = |injector: &triomphe::Arc<Box<dyn FunctionInjector>>| {
                    injector.inject_with_context(
                        function,
                        &arguments,
                        row_count,
                        self.span,
                        context.now,
                        prior_error_rows,
                    )
                };
                let injected = if let Some(injector) = context.injector.as_ref() {
                    match inject(injector) {
                        Err(RuntimeError::MissingFunctionInjector { .. })
                            if default_injector.is_some() =>
                        {
                            inject(default_injector.verified(
                                "the match guard above requires the default injector to be present",
                            ))?
                        }
                        result => result?,
                    }
                } else if let Some(injector) = default_injector {
                    inject(injector)?
                } else {
                    return Err(RuntimeError::MissingFunctionInjector {
                        function: function.as_str().to_string(),
                    });
                };
                let output = injected.output;
                if output.data_type() != *output_type || output.len() != row_count {
                    return Err(RuntimeError::InvalidInjectedResult {
                        function: function.as_str().to_string(),
                        expected_type: output_type.clone(),
                        actual_type: output.data_type(),
                        expected_rows: row_count,
                        actual_rows: output.len(),
                    });
                }
                for (row, side_error) in injected.side_errors {
                    if row >= row_count {
                        return Err(RuntimeError::InvalidInjectedSideError {
                            function: function.as_str().to_string(),
                            row,
                            row_count,
                        });
                    }
                    row_errors.push(row, side_error);
                }
                registers.set(*dst, output, Shape::Column)
            }
            InstructionKind::Select {
                dst,
                arms,
                otherwise,
            } => self.write_computed(
                registers,
                *dst,
                false,
                row_count,
                row_errors,
                |registers, _, _| execute_select(registers, arms, *otherwise),
            ),
        }
    }

    /// Writes the value `compute` yields to `dst`.
    ///
    /// When every operand is a scalar and the computation is not volatile, computing it once is
    /// computing it for every row: it runs over a single row and `dst` becomes a scalar, and a
    /// failure of that row is a failure of every row of the batch. Otherwise it runs over the
    /// batch and `dst` becomes a column.
    fn write_computed(
        &self,
        registers: &mut RegisterBank,
        dst: RegisterRef,
        volatile: bool,
        row_count: usize,
        row_errors: &mut RowErrors,
        compute: impl FnOnce(&RegisterBank, usize, &mut RowErrors) -> Result<TypedArray, RuntimeError>,
    ) -> Result<(), RuntimeError> {
        let shared = !volatile
            && self
                .kind
                .operands()
                .iter()
                .all(|operand| registers.is_scalar(*operand));
        if !shared {
            let output = compute(registers, row_count, row_errors)?;
            return registers.set(dst, output, Shape::Column);
        }
        let mut shared_errors = RowErrors::new(1);
        registers.rows = 1;
        let computed = compute(registers, 1, &mut shared_errors);
        registers.rows = row_count;
        let output = computed?;
        for error in shared_errors.row(0) {
            for row in 0..row_count {
                row_errors.push(row, error.clone());
            }
        }
        registers.set(dst, output, Shape::Scalar)
    }

    fn execute_assign(
        &self,
        registers: &mut RegisterBank,
        dst: RegisterRef,
        input: RegisterRef,
        fallback: &AssignmentFallback,
        row_errors: &RowErrors,
    ) -> Result<(), RuntimeError> {
        let Some(failed) = row_errors.rows_failed_within(self.span) else {
            return registers.copy(dst, input);
        };
        match fallback {
            // The destination held no value before this assignment, so a failed row is null.
            // Marking those rows null reuses the input's values instead of copying them into a
            // new array.
            AssignmentFallback::Uninitialized(_) => {
                let failed = BooleanArray::new(failed, None);
                let input = registers.read_array(input)?;
                let output = nullif(input.as_array(), &failed)
                    .map_err(|error| arrow_kernel_error("assignment fallback failed", error))?;
                registers.set(dst, array_ref_to_typed_array(output)?, Shape::Column)
            }
            AssignmentFallback::Register(previous) => {
                let success = BooleanArray::new(!&failed, None);
                let input = registers.any_operand(input)?;
                let previous = registers.any_operand(*previous)?;
                let output = zip(&success, &input, &previous)
                    .map_err(|error| arrow_kernel_error("assignment fallback failed", error))?;
                registers.set(dst, array_ref_to_typed_array(output)?, Shape::Column)
            }
        }
    }

    fn execute_unary(
        &self,
        registers: &RegisterBank,
        input: RegisterRef,
        op: UnaryOp,
        row_errors: &mut RowErrors,
    ) -> Result<TypedArray, RuntimeError> {
        match op {
            UnaryOp::Neg => match input.ty {
                RegisterType::Int8 => Ok(TypedArray::Int8(execute_integer_negation(
                    registers.column::<Int8Array>(input)?,
                    row_errors,
                    self.span,
                ))),
                RegisterType::Int16 => Ok(TypedArray::Int16(execute_integer_negation(
                    registers.column::<Int16Array>(input)?,
                    row_errors,
                    self.span,
                ))),
                RegisterType::Int32 => Ok(TypedArray::Int32(execute_integer_negation(
                    registers.column::<Int32Array>(input)?,
                    row_errors,
                    self.span,
                ))),
                RegisterType::Int64 => Ok(TypedArray::Int64(execute_integer_negation(
                    registers.column::<Int64Array>(input)?,
                    row_errors,
                    self.span,
                ))),
                RegisterType::Float32 => Ok(TypedArray::Float32(numeric::float_negation(
                    registers.column::<Float32Array>(input)?,
                ))),
                RegisterType::Float64 => Ok(TypedArray::Float64(numeric::float_negation(
                    registers.column::<Float64Array>(input)?,
                ))),
                _ => Err(RuntimeError::InvalidRegisterType {
                    reg: input,
                    expected: "numeric array",
                }),
            },
            UnaryOp::Not => Ok(TypedArray::Boolean(execute_not(
                registers.column::<BooleanArray>(input)?,
            ))),
        }
    }

    fn execute_binary(
        &self,
        registers: &RegisterBank,
        left: RegisterRef,
        right: RegisterRef,
        op: BinaryOp,
        row_errors: &mut RowErrors,
    ) -> Result<TypedArray, RuntimeError> {
        let operator = NumericBinary::of(op);
        match (left.ty, operator) {
            (RegisterType::UInt8, Some(operator)) => Ok(execute_integer_binary(
                registers.operand::<UInt8Array>(left)?,
                registers.operand::<UInt8Array>(right)?,
                operator,
                row_errors,
                self.span,
            )),
            (RegisterType::Int8, Some(operator)) => Ok(execute_integer_binary(
                registers.operand::<Int8Array>(left)?,
                registers.operand::<Int8Array>(right)?,
                operator,
                row_errors,
                self.span,
            )),
            (RegisterType::UInt16, Some(operator)) => Ok(execute_integer_binary(
                registers.operand::<UInt16Array>(left)?,
                registers.operand::<UInt16Array>(right)?,
                operator,
                row_errors,
                self.span,
            )),
            (RegisterType::Int16, Some(operator)) => Ok(execute_integer_binary(
                registers.operand::<Int16Array>(left)?,
                registers.operand::<Int16Array>(right)?,
                operator,
                row_errors,
                self.span,
            )),
            (RegisterType::UInt32, Some(operator)) => Ok(execute_integer_binary(
                registers.operand::<UInt32Array>(left)?,
                registers.operand::<UInt32Array>(right)?,
                operator,
                row_errors,
                self.span,
            )),
            (RegisterType::Int32, Some(operator)) => Ok(execute_integer_binary(
                registers.operand::<Int32Array>(left)?,
                registers.operand::<Int32Array>(right)?,
                operator,
                row_errors,
                self.span,
            )),
            (RegisterType::UInt64, Some(operator)) => Ok(execute_integer_binary(
                registers.operand::<UInt64Array>(left)?,
                registers.operand::<UInt64Array>(right)?,
                operator,
                row_errors,
                self.span,
            )),
            (RegisterType::Int64, Some(operator)) => Ok(execute_integer_binary(
                registers.operand::<Int64Array>(left)?,
                registers.operand::<Int64Array>(right)?,
                operator,
                row_errors,
                self.span,
            )),
            (RegisterType::Float32, Some(operator)) => Ok(execute_float_binary(
                registers.operand::<Float32Array>(left)?,
                registers.operand::<Float32Array>(right)?,
                operator,
                row_errors,
                self.span,
            )),
            (RegisterType::Float64, Some(operator)) => Ok(execute_float_binary(
                registers.operand::<Float64Array>(left)?,
                registers.operand::<Float64Array>(right)?,
                operator,
                row_errors,
                self.span,
            )),
            (
                RegisterType::UInt8
                | RegisterType::Int8
                | RegisterType::UInt16
                | RegisterType::Int16
                | RegisterType::UInt32
                | RegisterType::Int32
                | RegisterType::UInt64
                | RegisterType::Int64
                | RegisterType::Float32
                | RegisterType::Float64,
                None,
            ) => Err(RuntimeError::InvalidRegisterType {
                reg: left,
                expected: "BooleanArray",
            }),
            (RegisterType::Boolean, _) => execute_binary_bool(registers, left, right, op),
            (RegisterType::Utf8, _) => Ok(TypedArray::Boolean(compare_with_arrow_ord(
                &registers.operand::<StringArray>(left)?,
                &registers.operand::<StringArray>(right)?,
                op,
                "utf8 comparison",
            )?)),
            (RegisterType::Datetime, _) => Ok(TypedArray::Boolean(compare_with_arrow_ord(
                &registers.operand::<TimestampNanosecondArray>(left)?,
                &registers.operand::<TimestampNanosecondArray>(right)?,
                op,
                "datetime comparison",
            )?)),
            (RegisterType::Generic, _) => Err(RuntimeError::InvalidRegisterType {
                reg: left,
                expected: "scalar array",
            }),
        }
    }
}

/// How a binary operator applies to numeric operands.
#[derive(Debug, Clone, Copy)]
enum NumericBinary {
    Arithmetic(Arithmetic),
    Comparison(Comparison),
}

impl NumericBinary {
    /// The numeric form of `op`, or `None` for a logical operator, which applies to booleans only.
    fn of(op: BinaryOp) -> Option<Self> {
        match op {
            BinaryOp::Add => Some(Self::Arithmetic(Arithmetic::Add)),
            BinaryOp::Sub => Some(Self::Arithmetic(Arithmetic::Sub)),
            BinaryOp::Mul => Some(Self::Arithmetic(Arithmetic::Mul)),
            BinaryOp::Div => Some(Self::Arithmetic(Arithmetic::Div)),
            BinaryOp::Rem => Some(Self::Arithmetic(Arithmetic::Rem)),
            BinaryOp::Eq => Some(Self::Comparison(Comparison::Eq)),
            BinaryOp::NotEq => Some(Self::Comparison(Comparison::NotEq)),
            BinaryOp::Lt => Some(Self::Comparison(Comparison::Lt)),
            BinaryOp::LtEq => Some(Self::Comparison(Comparison::LtEq)),
            BinaryOp::Gt => Some(Self::Comparison(Comparison::Gt)),
            BinaryOp::GtEq => Some(Self::Comparison(Comparison::GtEq)),
            BinaryOp::And | BinaryOp::Or => None,
        }
    }
}

fn execute_integer_binary<T>(
    left: Operand<'_, PrimitiveArray<T>>,
    right: Operand<'_, PrimitiveArray<T>>,
    operator: NumericBinary,
    row_errors: &mut RowErrors,
    span: Span,
) -> TypedArray
where
    T: ArrowPrimitiveType,
    T::Native: CheckedInteger,
    TypedArray: From<PrimitiveArray<T>>,
{
    match operator {
        NumericBinary::Arithmetic(arithmetic) => {
            let checked = arithmetic.evaluate_integers(left, right);
            row_errors.push_failures(checked.failed.lanes(), span, |row| {
                arithmetic.integer_failure(right.array().value(right.index(row)))
            });
            TypedArray::from(checked.column)
        }
        NumericBinary::Comparison(comparison) => {
            TypedArray::Boolean(comparison.evaluate(left, right))
        }
    }
}

fn execute_float_binary<T>(
    left: Operand<'_, PrimitiveArray<T>>,
    right: Operand<'_, PrimitiveArray<T>>,
    operator: NumericBinary,
    row_errors: &mut RowErrors,
    span: Span,
) -> TypedArray
where
    T: ArrowPrimitiveType,
    T::Native: CheckedFloat,
    TypedArray: From<PrimitiveArray<T>>,
{
    match operator {
        NumericBinary::Arithmetic(arithmetic) => {
            let checked = arithmetic.evaluate_floats(left, right);
            row_errors.push_failures(checked.failed.lanes(), span, |_| {
                SideErrorReason::NonFiniteResult(FloatOperation::Arithmetic)
            });
            TypedArray::from(checked.column)
        }
        NumericBinary::Comparison(comparison) => {
            TypedArray::Boolean(comparison.evaluate(left, right))
        }
    }
}

fn execute_integer_negation<T>(
    input: &PrimitiveArray<T>,
    row_errors: &mut RowErrors,
    span: Span,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: SignedInteger,
{
    let checked = numeric::integer_negation(input);
    row_errors.push_failures(checked.failed.lanes(), span, |_| {
        SideErrorReason::IntegerOverflow(IntegerOperation::Negation)
    });
    checked.column
}

fn arrow_kernel_error(context: &str, error: ArrowError) -> RuntimeError {
    RuntimeError::InvalidBatch {
        message: format!("{context}: {error}"),
    }
}

macro_rules! define_array_ref_to_typed_array {
    ($($Variant:ident => $field:ident, $setter:ident, $accessor:ident, $Array:ty, $data_type:path;)+) => {
        fn array_ref_to_typed_array(array: ArrayRef) -> Result<TypedArray, RuntimeError> {
            match array.data_type() {
                $($data_type => Ok(TypedArray::$Variant(
                    array
                        .as_any()
                        .downcast_ref::<$Array>()
                        .verified(
                            "the match arm above narrowed this array's data type, which fixes its \
                             concrete Arrow array type",
                        )
                        .clone(),
                )),)+
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz))
                    if tz.as_ref() == "+00:00" || tz.as_ref() == "UTC" =>
                {
                    Ok(TypedArray::Datetime(
                        array
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .verified(
                                "the match arm above narrowed this array's data type, which fixes its \
                                 concrete Arrow array type",
                            )
                            .clone(),
                    ))
                }
                _ => Ok(TypedArray::Generic(array)),
            }
        }
    };
}

with_typed_registers!(define_array_ref_to_typed_array);

/// `coalesce` keeps the first non-null value of each row. The first argument is read as a column
/// and each later one as an operand, so a literal fallback is zipped in as one scalar.
fn execute_coalesce(
    registers: &RegisterBank,
    inputs: &[RegisterRef],
) -> Result<TypedArray, RuntimeError> {
    let first = inputs
        .first()
        .verified("the compiler rejects a coalesce with fewer than one argument");
    let mut result = registers.read_array(*first)?.into_array_ref();
    for input in &inputs[1..] {
        let mask = is_null(result.as_ref())
            .map_err(|error| arrow_kernel_error("coalesce is_null kernel failed", error))?;
        let fallback = registers.any_operand(*input)?;
        result = zip(&mask, &fallback, &result)
            .map_err(|error| arrow_kernel_error("coalesce zip kernel failed", error))?;
    }
    array_ref_to_typed_array(result)
}

/// A conditional selects, for each row, the value of the first arm whose mask holds, and the
/// `otherwise` value when none does. Masks are columns; values are operands, so a literal arm is
/// zipped in as one scalar.
fn execute_select(
    registers: &RegisterBank,
    arms: &[SelectArm],
    otherwise: RegisterRef,
) -> Result<TypedArray, RuntimeError> {
    let mut selected: Option<ArrayRef> = None;
    for arm in arms.iter().rev() {
        let mask = registers.column::<BooleanArray>(arm.mask)?;
        let value = registers.any_operand(arm.value)?;
        let zipped = match &selected {
            Some(fallback) => zip(mask, &value, fallback),
            None => {
                let fallback = registers.any_operand(otherwise)?;
                zip(mask, &value, &fallback)
            }
        };
        let output =
            zipped.map_err(|error| arrow_kernel_error("conditional selection failed", error))?;
        selected = Some(output);
    }
    match selected {
        Some(output) => array_ref_to_typed_array(output),
        None => registers.read_array(otherwise),
    }
}

/// A literal is one value every row shares, so it is written as a scalar and never expanded into a
/// column unless a kernel without a scalar form, or an output field, reads it.
fn write_literal(
    registers: &mut RegisterBank,
    dst: RegisterRef,
    value: &ScalarValue,
) -> Result<(), RuntimeError> {
    let scalar = match value {
        ScalarValue::Int64(value) => TypedArray::Int64(Int64Array::from_value(*value, 1)),
        ScalarValue::Float64(value) => TypedArray::Float64(Float64Array::from_value(*value, 1)),
        ScalarValue::Boolean(value) => TypedArray::Boolean(BooleanArray::from(vec![*value])),
        ScalarValue::Utf8(value) => {
            TypedArray::Utf8(StringArray::from_iter_values([value.as_str()]))
        }
    };
    registers.set(dst, scalar, Shape::Scalar)
}

fn write_null_literal(
    registers: &mut RegisterBank,
    dst: RegisterRef,
    data_type: &DataType,
) -> Result<(), RuntimeError> {
    let scalar = array_ref_to_typed_array(new_null_array(data_type, 1))?;
    registers.set(dst, scalar, Shape::Scalar)
}

fn execute_not(input: &BooleanArray) -> BooleanArray {
    not(input).assured(
        "arrow's not kernel is defined for BooleanArray, and this signature accepts nothing else",
    )
}

fn execute_binary_bool(
    registers: &RegisterBank,
    left: RegisterRef,
    right: RegisterRef,
    op: BinaryOp,
) -> Result<TypedArray, RuntimeError> {
    let output = match op {
        // The Kleene kernels take two columns, so a scalar operand is read as a column here.
        BinaryOp::And => and_kleene(
            registers.column::<BooleanArray>(left)?,
            registers.column::<BooleanArray>(right)?,
        )
        .map_err(|error| arrow_kernel_error("boolean and kernel failed", error))?,
        BinaryOp::Or => or_kleene(
            registers.column::<BooleanArray>(left)?,
            registers.column::<BooleanArray>(right)?,
        )
        .map_err(|error| arrow_kernel_error("boolean or kernel failed", error))?,
        BinaryOp::Eq => eq(
            &registers.operand::<BooleanArray>(left)?,
            &registers.operand::<BooleanArray>(right)?,
        )
        .map_err(|error| arrow_kernel_error("boolean eq kernel failed", error))?,
        BinaryOp::NotEq => neq(
            &registers.operand::<BooleanArray>(left)?,
            &registers.operand::<BooleanArray>(right)?,
        )
        .map_err(|error| arrow_kernel_error("boolean neq kernel failed", error))?,
        _ => {
            return Err(RuntimeError::InvalidRegisterType {
                reg: RegisterRef::new(RegisterSpace::Temp, RegisterType::Boolean, 0),
                expected: "Boolean logical/comparison operator",
            });
        }
    };
    Ok(TypedArray::Boolean(output))
}

fn compare_with_arrow_ord(
    left: &dyn Datum,
    right: &dyn Datum,
    op: BinaryOp,
    context: &str,
) -> Result<BooleanArray, RuntimeError> {
    match op {
        BinaryOp::Eq => eq(left, right)
            .map_err(|error| arrow_kernel_error(&format!("{context} eq kernel failed"), error)),
        BinaryOp::NotEq => neq(left, right)
            .map_err(|error| arrow_kernel_error(&format!("{context} neq kernel failed"), error)),
        BinaryOp::Gt => gt(left, right)
            .map_err(|error| arrow_kernel_error(&format!("{context} gt kernel failed"), error)),
        BinaryOp::Lt => lt(left, right)
            .map_err(|error| arrow_kernel_error(&format!("{context} lt kernel failed"), error)),
        BinaryOp::GtEq => gt_eq(left, right)
            .map_err(|error| arrow_kernel_error(&format!("{context} gte kernel failed"), error)),
        BinaryOp::LtEq => lt_eq(left, right)
            .map_err(|error| arrow_kernel_error(&format!("{context} lte kernel failed"), error)),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Rem => {
            Err(RuntimeError::InvalidBatch {
                message: format!("{context} comparison helper received arithmetic operator {op:?}"),
            })
        }
        BinaryOp::And | BinaryOp::Or => Err(RuntimeError::InvalidBatch {
            message: format!("{context} comparison helper received boolean operator {op:?}"),
        }),
    }
}

/// `nullif` returns null exactly where `=` holds, so it decides equality the way `=` does. Float
/// operands use the IEEE 754 comparison `=` evaluates, and every other type uses the arrow equality
/// kernel, which agrees with `=` for them. The second argument is an operand, so a literal is
/// compared as one scalar.
fn execute_nullif(
    left: &TypedArray,
    right: Operand<'_, dyn Array>,
) -> Result<TypedArray, RuntimeError> {
    let float_predicate = match left {
        TypedArray::Float32(values) => right
            .downcast::<Float32Array>()
            .map(|right| Comparison::Eq.evaluate(Operand::Column(values), right)),
        TypedArray::Float64(values) => right
            .downcast::<Float64Array>()
            .map(|right| Comparison::Eq.evaluate(Operand::Column(values), right)),
        _ => None,
    };
    let predicate = match float_predicate {
        Some(predicate) => predicate,
        None => {
            let left = left.as_array();
            eq(&left, &right)
                .map_err(|error| arrow_kernel_error("nullif eq kernel failed", error))?
        }
    };
    let output = nullif(left.as_array(), &predicate)
        .map_err(|error| arrow_kernel_error("nullif kernel failed", error))?;
    array_ref_to_typed_array(output)
}

fn execute_builtin(
    lowering: &BuiltinLowering,
    registers: &RegisterBank,
    inputs: &[RegisterRef],
    row_count: usize,
    row_errors: &mut RowErrors,
    span: Span,
    context: &ExecutionContext,
) -> Result<TypedArray, RuntimeError> {
    // Each kernel reads its operands in the shape it can use. A kernel with a scalar form reads a
    // text operand as it is, so a literal argument stays one value; a kernel that walks the batch
    // reads a column, which expands a scalar once per batch.
    let column = |index: usize| registers.read_array(inputs[index]);
    let columns = || {
        inputs
            .iter()
            .map(|input| registers.read_array(*input))
            .collect::<Result<Vec<_>, _>>()
    };
    let text = |index: usize| registers.operand::<StringArray>(inputs[index]);

    match lowering {
        BuiltinLowering::Now => Ok(TypedArray::Datetime(execute_now(row_count, context.now))),
        BuiltinLowering::UuidV4 => Ok(TypedArray::Utf8(execute_uuid_v4(row_count))),
        BuiltinLowering::UuidV7 => Ok(TypedArray::Utf8(execute_uuid_v7(row_count, context.now))),
        BuiltinLowering::Lower => Ok(TypedArray::Utf8(
            CaseMapping::Lower.execute(as_utf8(&column(0)?)?),
        )),
        BuiltinLowering::Upper => Ok(TypedArray::Utf8(
            CaseMapping::Upper.execute(as_utf8(&column(0)?)?),
        )),
        BuiltinLowering::Trim | BuiltinLowering::Btrim => {
            Ok(TypedArray::Utf8(execute_trim(as_utf8(&column(0)?)?)))
        }
        BuiltinLowering::Ltrim => Ok(TypedArray::Utf8(execute_ltrim(as_utf8(&column(0)?)?))),
        BuiltinLowering::Rtrim => Ok(TypedArray::Utf8(execute_rtrim(as_utf8(&column(0)?)?))),
        BuiltinLowering::Length | BuiltinLowering::CharLength => {
            Ok(TypedArray::Int64(execute_length(as_utf8(&column(0)?)?)))
        }
        BuiltinLowering::BitLength => {
            Ok(TypedArray::Int64(execute_bit_length(as_utf8(&column(0)?)?)))
        }
        BuiltinLowering::Ascii => Ok(TypedArray::Int64(execute_ascii(as_utf8(&column(0)?)?))),
        BuiltinLowering::Coalesce => execute_coalesce(registers, inputs),
        BuiltinLowering::IsNull => Ok(TypedArray::Boolean(execute_is_null_typed(&column(0)?))),
        BuiltinLowering::NullIf => execute_nullif(&column(0)?, registers.any_operand(inputs[1])?),
        BuiltinLowering::Abs => execute_abs_typed(&column(0)?, row_errors, span),
        BuiltinLowering::Acos => execute_math(&column(0)?, MathFunction::Acos, row_errors, span),
        BuiltinLowering::Asin => execute_math(&column(0)?, MathFunction::Asin, row_errors, span),
        BuiltinLowering::Atan => execute_math(&column(0)?, MathFunction::Atan, row_errors, span),
        BuiltinLowering::Ceil => execute_rounding(&column(0)?, Rounding::Ceil, row_errors, span),
        BuiltinLowering::Concat => {
            let mut parts = Vec::with_capacity(inputs.len());
            for index in 0..inputs.len() {
                parts.push(text(index)?);
            }
            Ok(TypedArray::Utf8(execute_concat(row_count, &parts)))
        }
        BuiltinLowering::Sum => execute_list_sum(&column(0)?, row_errors, span),
        BuiltinLowering::First => execute_list_item(&column(0)?, ListItem::First, None),
        BuiltinLowering::Last => execute_list_item(&column(0)?, ListItem::Last, None),
        BuiltinLowering::Count => Ok(TypedArray::Int64(execute_list_count(&column(0)?)?)),
        BuiltinLowering::Nth => execute_list_item(&column(0)?, ListItem::Nth, Some(&column(1)?)),
        BuiltinLowering::Contains => Ok(TypedArray::Boolean(execute_contains(text(0)?, text(1)?))),
        BuiltinLowering::Cos => execute_math(&column(0)?, MathFunction::Cos, row_errors, span),
        BuiltinLowering::StartsWith => {
            Ok(TypedArray::Boolean(execute_starts_with(text(0)?, text(1)?)))
        }
        BuiltinLowering::EndsWith => Ok(TypedArray::Boolean(execute_ends_with(text(0)?, text(1)?))),
        BuiltinLowering::Exp => execute_math(&column(0)?, MathFunction::Exp, row_errors, span),
        BuiltinLowering::Floor => execute_rounding(&column(0)?, Rounding::Floor, row_errors, span),
        BuiltinLowering::Initcap => Ok(TypedArray::Utf8(execute_initcap(as_utf8(&column(0)?)?))),
        BuiltinLowering::Left => Ok(TypedArray::Utf8(execute_left(
            as_utf8(&column(0)?)?,
            &column(1)?,
        )?)),
        BuiltinLowering::Ln => execute_math(&column(0)?, MathFunction::Ln, row_errors, span),
        BuiltinLowering::Log => execute_log(&columns()?, row_errors, span),
        BuiltinLowering::Lpad => Ok(TypedArray::Utf8(execute_pad(
            as_utf8(&column(0)?)?,
            &column(1)?,
            text(2)?,
            PadSide::Left,
        )?)),
        BuiltinLowering::Md5 => Ok(TypedArray::Utf8(execute_md5(as_utf8(&column(0)?)?))),
        BuiltinLowering::Pow => execute_binary_math(
            &column(0)?,
            &column(1)?,
            BinaryMathFunction::Pow,
            row_errors,
            span,
        ),
        BuiltinLowering::Regexp(call) => execute_regexp(call, registers, inputs, row_errors, span),
        BuiltinLowering::Repeat => Ok(TypedArray::Utf8(execute_repeat(
            as_utf8(&column(0)?)?,
            &column(1)?,
        )?)),
        BuiltinLowering::Replace => Ok(TypedArray::Utf8(execute_replace(
            as_utf8(&column(0)?)?,
            text(1)?,
            text(2)?,
        ))),
        BuiltinLowering::Reverse => Ok(TypedArray::Utf8(execute_reverse(as_utf8(&column(0)?)?))),
        BuiltinLowering::Right => Ok(TypedArray::Utf8(execute_right(
            as_utf8(&column(0)?)?,
            &column(1)?,
        )?)),
        BuiltinLowering::Round => {
            let values = columns()?;
            match values.as_slice() {
                [value, digits] => execute_round_to_digits(value, digits, row_errors, span)
                    .ok_or_else(|| unsupported_builtin_inputs(lowering, &values)),
                _ => execute_rounding(&values[0], Rounding::Round, row_errors, span),
            }
        }
        BuiltinLowering::Rpad => Ok(TypedArray::Utf8(execute_pad(
            as_utf8(&column(0)?)?,
            &column(1)?,
            text(2)?,
            PadSide::Right,
        )?)),
        BuiltinLowering::SplitPart => Ok(TypedArray::Utf8(execute_split_part(
            as_utf8(&column(0)?)?,
            text(1)?,
            &column(2)?,
        )?)),
        BuiltinLowering::Sqrt => execute_math(&column(0)?, MathFunction::Sqrt, row_errors, span),
        BuiltinLowering::Strpos => Ok(TypedArray::Int64(execute_strpos(
            as_utf8(&column(0)?)?,
            text(1)?,
        ))),
        BuiltinLowering::Substr => {
            let length = match inputs.get(2) {
                Some(length) => Some(registers.read_array(*length)?),
                None => None,
            };
            Ok(TypedArray::Utf8(execute_substr(
                as_utf8(&column(0)?)?,
                &column(1)?,
                length.as_ref(),
            )?))
        }
        BuiltinLowering::Tan => execute_math(&column(0)?, MathFunction::Tan, row_errors, span),
        BuiltinLowering::ToHex => Ok(TypedArray::Utf8(execute_to_hex(&column(0)?)?)),
        BuiltinLowering::Translate => Ok(TypedArray::Utf8(execute_translate(
            as_utf8(&column(0)?)?,
            text(1)?,
            text(2)?,
        ))),
        BuiltinLowering::Sin => execute_math(&column(0)?, MathFunction::Sin, row_errors, span),
        BuiltinLowering::Atan2 => execute_binary_math(
            &column(0)?,
            &column(1)?,
            BinaryMathFunction::Atan2,
            row_errors,
            span,
        ),
        BuiltinLowering::Log2 => execute_math(&column(0)?, MathFunction::Log2, row_errors, span),
        BuiltinLowering::Radians => {
            execute_math(&column(0)?, MathFunction::Radians, row_errors, span)
        }
        BuiltinLowering::Degrees => {
            execute_math(&column(0)?, MathFunction::Degrees, row_errors, span)
        }
        BuiltinLowering::Sign => {
            let values = columns()?;
            execute_sign(&values[0], row_errors, span)
                .ok_or_else(|| unsupported_builtin_inputs(lowering, &values))
        }
        BuiltinLowering::Trunc => execute_rounding(&column(0)?, Rounding::Trunc, row_errors, span),
        BuiltinLowering::IsNan => {
            let values = columns()?;
            execute_float_classification(&values[0], FloatClass::Nan)
                .ok_or_else(|| unsupported_builtin_inputs(lowering, &values))
        }
        BuiltinLowering::IsFinite => {
            let values = columns()?;
            execute_float_classification(&values[0], FloatClass::Finite)
                .ok_or_else(|| unsupported_builtin_inputs(lowering, &values))
        }
        BuiltinLowering::IsInfinite => {
            let values = columns()?;
            execute_float_classification(&values[0], FloatClass::Infinite)
                .ok_or_else(|| unsupported_builtin_inputs(lowering, &values))
        }
        BuiltinLowering::BitwiseAnd => {
            let values = columns()?;
            execute_bitwise(&values[0], &values[1], BitwiseOperation::And)
                .ok_or_else(|| unsupported_builtin_inputs(lowering, &values))
        }
        BuiltinLowering::BitwiseOr => {
            let values = columns()?;
            execute_bitwise(&values[0], &values[1], BitwiseOperation::Or)
                .ok_or_else(|| unsupported_builtin_inputs(lowering, &values))
        }
        BuiltinLowering::BitwiseXor => {
            let values = columns()?;
            execute_bitwise(&values[0], &values[1], BitwiseOperation::Xor)
                .ok_or_else(|| unsupported_builtin_inputs(lowering, &values))
        }
        BuiltinLowering::BitwiseNot => {
            let values = columns()?;
            execute_bitwise_not(&values[0])
                .ok_or_else(|| unsupported_builtin_inputs(lowering, &values))
        }
        BuiltinLowering::BitCount => {
            let values = columns()?;
            execute_bit_count(&values[0])
                .ok_or_else(|| unsupported_builtin_inputs(lowering, &values))
        }
        BuiltinLowering::ShiftLeft => {
            let values = columns()?;
            execute_shift(&values[0], &values[1], Shift::Left, row_errors, span)
                .ok_or_else(|| unsupported_builtin_inputs(lowering, &values))
        }
        BuiltinLowering::ShiftRight => {
            let values = columns()?;
            execute_shift(&values[0], &values[1], Shift::Right, row_errors, span)
                .ok_or_else(|| unsupported_builtin_inputs(lowering, &values))
        }
        BuiltinLowering::Datetime(function) => {
            let values = columns()?;
            match execute_datetime(function, &values, row_errors, span) {
                DatetimeExecution::Computed(output) => Ok(output),
                DatetimeExecution::UnsupportedInputs => {
                    Err(unsupported_builtin_inputs(lowering, &values))
                }
                DatetimeExecution::FormattedTooLarge { longest } => {
                    Err(RuntimeError::FormattedDatetimesTooLarge {
                        rows: row_count,
                        longest,
                    })
                }
            }
        }
    }
}

/// The error for a builtin whose input columns have types its signature rejects. Compilation
/// checks every call against its signature, so this reports a program and a batch that disagree.
fn unsupported_builtin_inputs(lowering: &BuiltinLowering, inputs: &[TypedArray]) -> RuntimeError {
    let input_types = inputs.iter().map(TypedArray::data_type).collect::<Vec<_>>();
    RuntimeError::InvalidBatch {
        message: format!("builtin {lowering:?} does not accept inputs of types {input_types:?}"),
    }
}

/// What executing a datetime builtin over one batch produced.
enum DatetimeExecution {
    Computed(TypedArray),
    /// An operand has a type the builtin's signature rejects.
    UnsupportedInputs,
    /// The batch's formatted values, each up to `longest` bytes, could exceed the text one STRING
    /// column holds.
    FormattedTooLarge {
        longest: usize,
    },
}

/// Executes a datetime builtin over its row-valued operands, recording a row error for every lane
/// whose result its type cannot hold and for every text that names no instant.
fn execute_datetime(
    function: &DatetimeFunction,
    operands: &[TypedArray],
    row_errors: &mut RowErrors,
    span: Span,
) -> DatetimeExecution {
    let output = match (function, operands) {
        (DatetimeFunction::DatePart { part, zone }, [TypedArray::Datetime(values)]) => {
            TypedArray::Int64(datetime::date_part(values, *part, zone))
        }
        (DatetimeFunction::DateTrunc { unit, zone }, [TypedArray::Datetime(values)]) => {
            let truncated = datetime::truncate(values, *unit, zone);
            record_datetime_failures(truncated, DatetimeOperation::DateTrunc, row_errors, span)
        }
        (
            DatetimeFunction::DateBin(width),
            [TypedArray::Datetime(values), TypedArray::Datetime(origins)],
        ) => {
            let binned = datetime::bin(values, origins, *width);
            record_datetime_failures(binned, DatetimeOperation::DateBin, row_errors, span)
        }
        (DatetimeFunction::DateAdd { unit, zone }, [amounts, TypedArray::Datetime(values)]) => {
            let Some(amounts) = UnitCounts::from_typed(amounts) else {
                return DatetimeExecution::UnsupportedInputs;
            };
            let added = datetime::add(&amounts, values, *unit, zone);
            record_datetime_failures(added, DatetimeOperation::DateAdd, row_errors, span)
        }
        (
            DatetimeFunction::DateDiff { unit, zone },
            [TypedArray::Datetime(starts), TypedArray::Datetime(ends)],
        ) => {
            let counted = datetime::difference(starts, ends, *unit, zone);
            row_errors.push_failures(counted.failed.lanes(), span, |_| {
                SideErrorReason::DateDiffOverflow
            });
            TypedArray::Int64(counted.column)
        }
        (DatetimeFunction::ToUnix(unit), [TypedArray::Datetime(values)]) => {
            TypedArray::Int64(datetime::to_unix(values, *unit))
        }
        (DatetimeFunction::FromUnix(unit), [counts]) => {
            let Some(counts) = UnitCounts::from_typed(counts) else {
                return DatetimeExecution::UnsupportedInputs;
            };
            let converted = datetime::from_unix(&counts, *unit);
            record_datetime_failures(converted, DatetimeOperation::FromUnix, row_errors, span)
        }
        (DatetimeFunction::FormatDatetime { format, zone }, [TypedArray::Datetime(values)]) => {
            match datetime::format_datetimes(values, format, zone) {
                FormattedColumn::Formatted(column) => TypedArray::Utf8(column),
                FormattedColumn::TooLarge => {
                    return DatetimeExecution::FormattedTooLarge {
                        longest: format.longest_value(zone),
                    };
                }
            }
        }
        (DatetimeFunction::ParseDatetime(parser), [TypedArray::Utf8(texts)]) => {
            let parsed = datetime::parse_datetimes(texts, parser);
            for failed in parsed.failures {
                let error = SideError {
                    reason: failed.failure.into_reason(),
                    span,
                };
                row_errors.push(failed.lane, error);
            }
            TypedArray::Datetime(parsed.column)
        }
        _ => return DatetimeExecution::UnsupportedInputs,
    };
    DatetimeExecution::Computed(output)
}

impl TextFailure {
    /// The row error that reports this failure.
    fn into_reason(self) -> SideErrorReason {
        match self {
            Self::Unreadable(unreadable) => SideErrorReason::UnreadableDatetime(unreadable),
            Self::Unresolved {
                local_time: UnresolvedLocalTime::Skipped,
                zone,
            } => SideErrorReason::SkippedLocalTime { zone },
            Self::Unresolved {
                local_time: UnresolvedLocalTime::Repeated,
                zone,
            } => SideErrorReason::RepeatedLocalTime { zone },
            Self::OutOfRange => {
                SideErrorReason::DatetimeOutOfRange(DatetimeOperation::ParseDatetime)
            }
        }
    }
}

/// Records a row error for every lane of a datetime result that left the DATETIME range.
fn record_datetime_failures(
    checked: Checked<TimestampNanosecondType>,
    operation: DatetimeOperation,
    row_errors: &mut RowErrors,
    span: Span,
) -> TypedArray {
    row_errors.push_failures(checked.failed.lanes(), span, |_| {
        SideErrorReason::DatetimeOutOfRange(operation)
    });
    TypedArray::Datetime(checked.column)
}

#[derive(Clone, Copy)]
enum ListItem {
    First,
    Last,
    Nth,
}

#[derive(Clone, Copy)]
enum ListColumn<'a> {
    Variable(&'a ListArray),
    Fixed(&'a FixedSizeListArray),
}

impl<'a> ListColumn<'a> {
    fn from_typed(input: &'a TypedArray) -> Result<Self, RuntimeError> {
        let TypedArray::Generic(array) = input else {
            return Err(RuntimeError::InvalidBatch {
                message: format!(
                    "list builtin requires ARRAY or VEC input, found {:?}",
                    input.data_type()
                ),
            });
        };
        match array.data_type() {
            DataType::List(_) => match array.as_any().downcast_ref::<ListArray>() {
                Some(array) => Ok(Self::Variable(array)),
                None => Err(RuntimeError::InvalidBatch {
                    message: "list data type is not backed by ListArray".to_string(),
                }),
            },
            DataType::FixedSizeList(_, _) => {
                match array.as_any().downcast_ref::<FixedSizeListArray>() {
                    Some(array) => Ok(Self::Fixed(array)),
                    None => Err(RuntimeError::InvalidBatch {
                        message: "fixed-size list data type is not backed by FixedSizeListArray"
                            .to_string(),
                    }),
                }
            }
            other => Err(RuntimeError::InvalidBatch {
                message: format!("list builtin requires ARRAY or VEC input, found {other:?}"),
            }),
        }
    }

    fn len(self) -> usize {
        match self {
            Self::Variable(array) => array.len(),
            Self::Fixed(array) => array.len(),
        }
    }

    fn is_null(self, row: usize) -> bool {
        match self {
            Self::Variable(array) => array.is_null(row),
            Self::Fixed(array) => array.is_null(row),
        }
    }

    fn nulls(self) -> Option<&'a NullBuffer> {
        match self {
            Self::Variable(array) => array.nulls(),
            Self::Fixed(array) => array.nulls(),
        }
    }

    fn values(self) -> &'a ArrayRef {
        match self {
            Self::Variable(array) => array.values(),
            Self::Fixed(array) => array.values(),
        }
    }

    fn element_data_type(self) -> &'a DataType {
        self.values().data_type()
    }

    fn value_range(self, row: usize) -> Range<usize> {
        match self {
            Self::Variable(array) => {
                let offsets = array.value_offsets();
                let start = usize::try_from(offsets[row])
                    .assured("arrow offset and width buffers are non-negative by construction");
                let end = usize::try_from(offsets[row + 1])
                    .assured("arrow offset and width buffers are non-negative by construction");
                start..end
            }
            Self::Fixed(array) => {
                let width = usize::try_from(array.value_length())
                    .assured("arrow offset and width buffers are non-negative by construction");
                let start = row * width;
                start..start + width
            }
        }
    }
}

fn execute_list_count(input: &TypedArray) -> Result<Int64Array, RuntimeError> {
    let list = ListColumn::from_typed(input)?;
    let lengths = (0..list.len())
        .map(|row| {
            i64::try_from(list.value_range(row).len())
                .assured("an Arrow list range cannot exceed the allocator's isize limit")
        })
        .collect::<Vec<_>>();
    Ok(Int64Array::new(lengths.into(), list.nulls().cloned()))
}

/// Sums each row's list with the checks `+` applies to the same operands. An integer sum that
/// overflows reports an overflow and a float sum that is not finite reports an invalid argument,
/// and either failure nulls that row's sum. Null elements are skipped, and an empty list or one
/// whose elements are all null has no sum.
fn execute_list_sum_for_primitive<T>(
    list: ListColumn<'_>,
    is_finite: fn(T::Native) -> bool,
    row_errors: &mut RowErrors,
    span: Span,
) -> Result<PrimitiveArray<T>, RuntimeError>
where
    T: ArrowNumericType,
{
    let values = list
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| RuntimeError::InvalidBatch {
            message: format!("list values are not backed by {:?}", T::DATA_TYPE),
        })?;
    let mut builder = PrimitiveBuilder::<T>::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) {
            builder.append_null();
            continue;
        }
        let range = list.value_range(row);
        let elements = values.slice(range.start, range.len());
        match arrow_sum_checked(&elements) {
            Ok(Some(total)) => {
                if is_finite(total) {
                    builder.append_value(total);
                } else {
                    builder.append_null();
                    row_errors.push(
                        row,
                        SideError {
                            reason: SideErrorReason::NonFiniteResult(FloatOperation::Sum),
                            span,
                        },
                    );
                }
            }
            Ok(None) => builder.append_null(),
            Err(ArrowError::ArithmeticOverflow(_)) => {
                builder.append_null();
                row_errors.push(
                    row,
                    SideError {
                        reason: SideErrorReason::IntegerOverflow(IntegerOperation::Sum),
                        span,
                    },
                );
            }
            Err(error) => return Err(arrow_kernel_error("list sum kernel failed", error)),
        }
    }
    Ok(builder.finish())
}

fn execute_list_sum(
    input: &TypedArray,
    row_errors: &mut RowErrors,
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    let list = ListColumn::from_typed(input)?;
    // Every integer value is finite, so an integer sum can only fail by overflowing.
    match list.element_data_type() {
        DataType::UInt8 => {
            execute_list_sum_for_primitive::<UInt8Type>(list, |_| true, row_errors, span)
                .map(TypedArray::UInt8)
        }
        DataType::Int8 => {
            execute_list_sum_for_primitive::<Int8Type>(list, |_| true, row_errors, span)
                .map(TypedArray::Int8)
        }
        DataType::UInt16 => {
            execute_list_sum_for_primitive::<UInt16Type>(list, |_| true, row_errors, span)
                .map(TypedArray::UInt16)
        }
        DataType::Int16 => {
            execute_list_sum_for_primitive::<Int16Type>(list, |_| true, row_errors, span)
                .map(TypedArray::Int16)
        }
        DataType::UInt32 => {
            execute_list_sum_for_primitive::<UInt32Type>(list, |_| true, row_errors, span)
                .map(TypedArray::UInt32)
        }
        DataType::Int32 => {
            execute_list_sum_for_primitive::<Int32Type>(list, |_| true, row_errors, span)
                .map(TypedArray::Int32)
        }
        DataType::UInt64 => {
            execute_list_sum_for_primitive::<UInt64Type>(list, |_| true, row_errors, span)
                .map(TypedArray::UInt64)
        }
        DataType::Int64 => {
            execute_list_sum_for_primitive::<Int64Type>(list, |_| true, row_errors, span)
                .map(TypedArray::Int64)
        }
        DataType::Float32 => {
            execute_list_sum_for_primitive::<Float32Type>(list, f32::is_finite, row_errors, span)
                .map(TypedArray::Float32)
        }
        DataType::Float64 => {
            execute_list_sum_for_primitive::<Float64Type>(list, f64::is_finite, row_errors, span)
                .map(TypedArray::Float64)
        }
        other => Err(RuntimeError::InvalidBatch {
            message: format!("sum requires numeric ARRAY or VEC elements, found {other:?}"),
        }),
    }
}

fn list_nth_indices(index_input: Option<&TypedArray>) -> Result<Int64Array, RuntimeError> {
    let Some(index_input) = index_input else {
        return Err(RuntimeError::InvalidBatch {
            message: "nth requires an index input".to_string(),
        });
    };
    match index_input {
        TypedArray::UInt8(_)
        | TypedArray::Int8(_)
        | TypedArray::UInt16(_)
        | TypedArray::Int16(_)
        | TypedArray::UInt32(_)
        | TypedArray::Int32(_)
        | TypedArray::UInt64(_)
        | TypedArray::Int64(_) => {}
        other => {
            return Err(RuntimeError::InvalidBatch {
                message: format!(
                    "builtin requires integer input, found {:?}",
                    other.data_type()
                ),
            });
        }
    }
    let options = CastOptions {
        safe: true,
        ..CastOptions::default()
    };
    let indices = cast_with_options(index_input.as_array(), &DataType::Int64, &options)
        .map_err(|error| arrow_kernel_error("list index cast kernel failed", error))?;
    indices
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| RuntimeError::InvalidBatch {
            message: format!(
                "list index cast produced {:?} instead of Int64",
                indices.data_type()
            ),
        })
}

fn execute_list_item(
    input: &TypedArray,
    item: ListItem,
    index_input: Option<&TypedArray>,
) -> Result<TypedArray, RuntimeError> {
    let list = ListColumn::from_typed(input)?;
    match list.element_data_type() {
        DataType::UInt8
        | DataType::Int8
        | DataType::UInt16
        | DataType::Int16
        | DataType::UInt32
        | DataType::Int32
        | DataType::UInt64
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Boolean
        | DataType::Utf8 => {}
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz))
            if tz.as_ref() == "+00:00" || tz.as_ref() == "UTC" => {}
        other => {
            return Err(RuntimeError::InvalidBatch {
                message: format!("list item function does not support element type {other:?}"),
            });
        }
    }

    let nth_indices = match item {
        ListItem::Nth => Some(list_nth_indices(index_input)?),
        ListItem::First | ListItem::Last => None,
    };
    let indices = UInt64Array::from_iter((0..list.len()).map(|row| {
        if list.is_null(row) {
            return None;
        }
        let range = list.value_range(row);
        let relative = match item {
            ListItem::First => (!range.is_empty()).then_some(0),
            ListItem::Last => range.len().checked_sub(1),
            ListItem::Nth => {
                let indices = nth_indices.as_ref()?;
                if indices.is_null(row) {
                    return None;
                }
                let index = indices.value(row);
                if index < 0 {
                    return None;
                }
                let Ok(index) = usize::try_from(index) else {
                    return None;
                };
                (index < range.len()).then_some(index)
            }
        }?;
        Some((range.start + relative).arch_into())
    }));
    let output = take(
        list.values().as_ref(),
        &indices,
        Some(TakeOptions { check_bounds: true }),
    )
    .map_err(|error| arrow_kernel_error("list item take kernel failed", error))?;
    array_ref_to_typed_array(output)
}

/// A string builder sized for an output shaped like `input`, so appending rows does not
/// repeatedly grow and copy the offset and value buffers.
fn string_builder_like(input: &StringArray) -> StringBuilder {
    let offsets = input.value_offsets();
    let value_bytes = usize::try_from(offsets[input.len()] - offsets[0]).assured(
        "arrow offset buffers are non-decreasing, so the span between two of them is non-negative",
    );
    StringBuilder::with_capacity(input.len(), value_bytes)
}

/// Columnar execution of the case mapping `CaseMapping::apply` defines for one value.
impl CaseMapping {
    fn execute(self, input: &StringArray) -> StringArray {
        let offsets = input.value_offsets();
        let start = usize::try_from(offsets[0])
            .assured("arrow offset and width buffers are non-negative by construction");
        let end = usize::try_from(offsets[input.len()])
            .assured("arrow offset and width buffers are non-negative by construction");
        let visible = &input.values().as_slice()[start..end];
        if visible.is_ascii() {
            self.execute_ascii(input)
        } else {
            self.execute_unicode(input)
        }
    }

    /// Over ASCII text the Unicode mapping is the per-byte ASCII mapping, which never changes a
    /// value's byte length. Offsets and validity therefore carry over unchanged and only the value
    /// bytes are rewritten, which keeps the whole operation one pass over the buffer instead of an
    /// allocation per row. A sliced column shares its buffer with bytes outside the slice, and the
    /// ASCII mapping leaves any non-ASCII byte among them untouched.
    fn execute_ascii(self, input: &StringArray) -> StringArray {
        let bytes = input.values().as_slice();
        let values = match self {
            Self::Lower => bytes.to_ascii_lowercase(),
            Self::Upper => bytes.to_ascii_uppercase(),
        };
        StringArray::try_new(
            input.offsets().clone(),
            values.into(),
            input.nulls().cloned(),
        )
        .verified(
            "the output reuses the input's offsets and null buffer and maps bytes one to one, so \
             try_new's invariants still hold",
        )
    }

    /// Outside ASCII a mapping can change a value's length, as `ß` uppercases to `SS`, so every
    /// value is rebuilt through the whole-value mapping.
    fn execute_unicode(self, input: &StringArray) -> StringArray {
        let mut builder = string_builder_like(input);
        for value in input.iter() {
            let Some(value) = value else {
                builder.append_null();
                continue;
            };
            builder.append_value(self.apply(value));
        }
        builder.finish()
    }
}

/// Rebuilds one UTF-8 column from borrowed slices of its input while carrying the input validity
/// bitmap over unchanged. Trim operations only select substrings, so the visible input byte span
/// is an exact upper bound for the output buffer and no per-row `String` allocation is needed.
fn execute_string_slice_transform(
    input: &StringArray,
    transform: impl for<'a> Fn(&'a str) -> &'a str,
) -> StringArray {
    let input_offsets = input.value_offsets();
    let start = usize::try_from(input_offsets[0])
        .assured("arrow offset and width buffers are non-negative by construction");
    let end = usize::try_from(input_offsets[input.len()])
        .assured("arrow offset and width buffers are non-negative by construction");
    let mut values = Vec::with_capacity(end - start);
    let mut offsets = Vec::with_capacity(input.len() + 1);
    offsets.push(0_i32);
    for value in input.iter() {
        if let Some(value) = value {
            values.extend_from_slice(transform(value).as_bytes());
        }
        offsets.push(i32::try_from(values.len()).verified(
            "the transform returns slices of the input, so the output stays inside the input's \
             own i32 offset range",
        ));
    }
    StringArray::new(
        OffsetBuffer::new(offsets.into()),
        values.into(),
        input.nulls().cloned(),
    )
}

fn execute_trim(input: &StringArray) -> StringArray {
    execute_string_slice_transform(input, str::trim)
}

fn execute_length(input: &StringArray) -> Int64Array {
    let bytes = input.values().as_slice();
    let lengths = input
        .value_offsets()
        .windows(2)
        .map(|offsets| {
            let start = usize::try_from(offsets[0])
                .assured("arrow offset and width buffers are non-negative by construction");
            let end = usize::try_from(offsets[1])
                .assured("arrow offset and width buffers are non-negative by construction");
            bytes[start..end]
                .iter()
                .filter(|byte| **byte & 0b1100_0000 != 0b1000_0000)
                .count()
                .try_into()
                .verified(
                    "the count is bounded by the input's byte span, which already fits an i32 \
                     offset",
                )
        })
        .collect::<Vec<_>>();
    Int64Array::new(lengths.into(), input.nulls().cloned())
}

fn execute_contains(
    string: Operand<'_, StringArray>,
    substring: Operand<'_, StringArray>,
) -> BooleanArray {
    string_contains(&string, &substring)
        .assured("this kernel is defined for Utf8 arrays, and this signature accepts nothing else")
}

fn execute_starts_with(
    string: Operand<'_, StringArray>,
    prefix: Operand<'_, StringArray>,
) -> BooleanArray {
    string_starts_with(&string, &prefix)
        .assured("this kernel is defined for Utf8 arrays, and this signature accepts nothing else")
}

fn execute_ends_with(
    string: Operand<'_, StringArray>,
    suffix: Operand<'_, StringArray>,
) -> BooleanArray {
    string_ends_with(&string, &suffix)
        .assured("this kernel is defined for Utf8 arrays, and this signature accepts nothing else")
}

fn execute_now(row_count: usize, now: Timestamp) -> TimestampNanosecondArray {
    TimestampNanosecondArray::from(vec![Some(now.unix_nanos()); row_count]).with_timezone_utc()
}

fn execute_uuid_v4(row_count: usize) -> StringArray {
    StringArray::from_iter_values((0..row_count).map(|_| Uuid::new_v4().to_string()))
}

fn execute_uuid_v7(row_count: usize, now: Timestamp) -> StringArray {
    let datetime = now.into_datetime();
    let seconds = u64::try_from(datetime.timestamp()).unwrap_or(0);
    let nanos = datetime.timestamp_subsec_nanos();
    let ts = UuidTimestamp::from_unix(NoContext, seconds, nanos);
    StringArray::from_iter_values((0..row_count).map(|_| Uuid::new_v7(ts).to_string()))
}

fn as_utf8(value: &TypedArray) -> Result<&StringArray, RuntimeError> {
    value.as_utf8().ok_or(RuntimeError::InvalidBatch {
        message: format!("builtin expected Utf8 input, found {:?}", value.data_type()),
    })
}

fn execute_bit_length(input: &StringArray) -> Int64Array {
    let lengths = input
        .value_offsets()
        .windows(2)
        .map(|offsets| i64::from(offsets[1] - offsets[0]) * 8)
        .collect::<Vec<_>>();
    Int64Array::new(lengths.into(), input.nulls().cloned())
}

fn execute_ascii(input: &StringArray) -> Int64Array {
    let mut builder = Int64Builder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            let value = match input.value(row).chars().next() {
                Some(character) => i64::from(u32::from(character)),
                None => 0,
            };
            builder.append_value(value);
        }
    }
    builder.finish()
}

fn execute_ltrim(input: &StringArray) -> StringArray {
    execute_string_slice_transform(input, str::trim_start)
}

fn execute_rtrim(input: &StringArray) -> StringArray {
    execute_string_slice_transform(input, str::trim_end)
}

fn execute_initcap(input: &StringArray) -> StringArray {
    let mut builder = string_builder_like(input);
    let mut result = String::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
            continue;
        }
        result.clear();
        let mut start_word = true;
        for ch in input.value(row).chars() {
            if ch.is_alphanumeric() {
                if start_word {
                    for upper in ch.to_uppercase() {
                        result.push(upper);
                    }
                } else {
                    for lower in ch.to_lowercase() {
                        result.push(lower);
                    }
                }
                start_word = false;
            } else {
                result.push(ch);
                start_word = true;
            }
        }
        builder.append_value(&result);
    }
    builder.finish()
}

fn execute_is_null_typed(input: &TypedArray) -> BooleanArray {
    match input {
        TypedArray::Uninitialized { len, .. } => BooleanArray::from(vec![true; *len]),
        _ => is_null(input.as_array())
            .assured("arrow's is_null kernel is defined for every array type"),
    }
}

fn execute_abs_typed(
    input: &TypedArray,
    row_errors: &mut RowErrors,
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    match input {
        // An unsigned integer is its own absolute value.
        TypedArray::UInt8(_)
        | TypedArray::UInt16(_)
        | TypedArray::UInt32(_)
        | TypedArray::UInt64(_) => Ok(input.clone()),
        TypedArray::Int8(array) => Ok(TypedArray::Int8(execute_integer_absolute_value(
            array, row_errors, span,
        ))),
        TypedArray::Int16(array) => Ok(TypedArray::Int16(execute_integer_absolute_value(
            array, row_errors, span,
        ))),
        TypedArray::Int32(array) => Ok(TypedArray::Int32(execute_integer_absolute_value(
            array, row_errors, span,
        ))),
        TypedArray::Int64(array) => Ok(TypedArray::Int64(execute_integer_absolute_value(
            array, row_errors, span,
        ))),
        TypedArray::Float32(array) => Ok(TypedArray::Float32(execute_float_absolute_value(
            array, row_errors, span,
        ))),
        TypedArray::Float64(array) => Ok(TypedArray::Float64(execute_float_absolute_value(
            array, row_errors, span,
        ))),
        TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => Err(RuntimeError::InvalidBatch {
            message: format!("abs requires numeric input, found {:?}", input.data_type()),
        }),
    }
}

fn execute_integer_absolute_value<T>(
    input: &PrimitiveArray<T>,
    row_errors: &mut RowErrors,
    span: Span,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: SignedInteger,
{
    let checked = numeric::integer_absolute_value(input);
    row_errors.push_failures(checked.failed.lanes(), span, |_| {
        SideErrorReason::IntegerOverflow(IntegerOperation::AbsoluteValue)
    });
    checked.column
}

fn execute_float_absolute_value<T>(
    input: &PrimitiveArray<T>,
    row_errors: &mut RowErrors,
    span: Span,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: CheckedFloat,
{
    let checked = numeric::float_absolute_value(input);
    row_errors.push_failures(checked.failed.lanes(), span, |_| {
        SideErrorReason::NonFiniteResult(FloatOperation::AbsoluteValue)
    });
    checked.column
}

fn execute_math(
    input: &TypedArray,
    function: MathFunction,
    row_errors: &mut RowErrors,
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    let Some(operand) = F64Operand::from_typed(input) else {
        return Err(RuntimeError::InvalidBatch {
            message: format!(
                "numeric builtin requires numeric input, found {:?}",
                input.data_type()
            ),
        });
    };
    let checked = function.evaluate(&operand);
    row_errors.push_failures(checked.failed.lanes(), span, |_| function.failure());
    Ok(TypedArray::Float64(checked.column))
}

fn execute_binary_math(
    left: &TypedArray,
    right: &TypedArray,
    function: BinaryMathFunction,
    row_errors: &mut RowErrors,
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    let (Some(left_operand), Some(right_operand)) =
        (F64Operand::from_typed(left), F64Operand::from_typed(right))
    else {
        return Err(RuntimeError::InvalidBatch {
            message: format!(
                "numeric builtin requires numeric inputs, found {:?} and {:?}",
                left.data_type(),
                right.data_type()
            ),
        });
    };
    let checked = function.evaluate(&left_operand, &right_operand);
    row_errors.push_failures(checked.failed.lanes(), span, |_| function.failure());
    Ok(TypedArray::Float64(checked.column))
}

fn execute_rounding(
    input: &TypedArray,
    rounding: Rounding,
    row_errors: &mut RowErrors,
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    match input {
        // An integer is already integral, so every rounding returns it unchanged.
        TypedArray::UInt8(_)
        | TypedArray::Int8(_)
        | TypedArray::UInt16(_)
        | TypedArray::Int16(_)
        | TypedArray::UInt32(_)
        | TypedArray::Int32(_)
        | TypedArray::UInt64(_)
        | TypedArray::Int64(_) => Ok(input.clone()),
        TypedArray::Float32(array) => Ok(TypedArray::Float32(execute_float_rounding(
            array, rounding, row_errors, span,
        ))),
        TypedArray::Float64(array) => Ok(TypedArray::Float64(execute_float_rounding(
            array, rounding, row_errors, span,
        ))),
        TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => Err(RuntimeError::InvalidBatch {
            message: format!(
                "rounding builtin requires numeric input, found {:?}",
                input.data_type()
            ),
        }),
    }
}

fn execute_float_rounding<T>(
    input: &PrimitiveArray<T>,
    rounding: Rounding,
    row_errors: &mut RowErrors,
    span: Span,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: CheckedFloat,
{
    let checked = rounding.evaluate_floats(input);
    row_errors.push_failures(checked.failed.lanes(), span, |_| rounding.float_failure());
    checked.column
}

/// `round(value, digits)`, or `None` when the value is not numeric or the digits not integral.
fn execute_round_to_digits(
    value: &TypedArray,
    digits: &TypedArray,
    row_errors: &mut RowErrors,
    span: Span,
) -> Option<TypedArray> {
    let digits = RoundingDigits::from_typed(digits)?;
    let rounded = match value {
        TypedArray::UInt8(array) => TypedArray::UInt8(execute_integer_round_to_digits(
            array, &digits, row_errors, span,
        )),
        TypedArray::Int8(array) => TypedArray::Int8(execute_integer_round_to_digits(
            array, &digits, row_errors, span,
        )),
        TypedArray::UInt16(array) => TypedArray::UInt16(execute_integer_round_to_digits(
            array, &digits, row_errors, span,
        )),
        TypedArray::Int16(array) => TypedArray::Int16(execute_integer_round_to_digits(
            array, &digits, row_errors, span,
        )),
        TypedArray::UInt32(array) => TypedArray::UInt32(execute_integer_round_to_digits(
            array, &digits, row_errors, span,
        )),
        TypedArray::Int32(array) => TypedArray::Int32(execute_integer_round_to_digits(
            array, &digits, row_errors, span,
        )),
        TypedArray::UInt64(array) => TypedArray::UInt64(execute_integer_round_to_digits(
            array, &digits, row_errors, span,
        )),
        TypedArray::Int64(array) => TypedArray::Int64(execute_integer_round_to_digits(
            array, &digits, row_errors, span,
        )),
        TypedArray::Float32(array) => TypedArray::Float32(execute_float_round_to_digits(
            array, &digits, row_errors, span,
        )),
        TypedArray::Float64(array) => TypedArray::Float64(execute_float_round_to_digits(
            array, &digits, row_errors, span,
        )),
        TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => return None,
    };
    Some(rounded)
}

fn execute_integer_round_to_digits<T>(
    values: &PrimitiveArray<T>,
    digits: &RoundingDigits,
    row_errors: &mut RowErrors,
    span: Span,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: IntegerRounding,
{
    let checked = digits.round_integers(values);
    row_errors.push_failures(checked.failed.lanes(), span, |_| {
        SideErrorReason::IntegerOverflow(IntegerOperation::Rounding)
    });
    checked.column
}

fn execute_float_round_to_digits<T>(
    values: &PrimitiveArray<T>,
    digits: &RoundingDigits,
    row_errors: &mut RowErrors,
    span: Span,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: DecimalRounding,
{
    let checked = digits.round_floats(values);
    row_errors.push_failures(checked.failed.lanes(), span, |_| {
        Rounding::Round.float_failure()
    });
    checked.column
}

/// `sign(value)`, or `None` when the value is not numeric.
fn execute_sign(input: &TypedArray, row_errors: &mut RowErrors, span: Span) -> Option<TypedArray> {
    let signs = match input {
        TypedArray::UInt8(array) => TypedArray::UInt8(numeric::integer_sign(array)),
        TypedArray::Int8(array) => TypedArray::Int8(numeric::integer_sign(array)),
        TypedArray::UInt16(array) => TypedArray::UInt16(numeric::integer_sign(array)),
        TypedArray::Int16(array) => TypedArray::Int16(numeric::integer_sign(array)),
        TypedArray::UInt32(array) => TypedArray::UInt32(numeric::integer_sign(array)),
        TypedArray::Int32(array) => TypedArray::Int32(numeric::integer_sign(array)),
        TypedArray::UInt64(array) => TypedArray::UInt64(numeric::integer_sign(array)),
        TypedArray::Int64(array) => TypedArray::Int64(numeric::integer_sign(array)),
        TypedArray::Float32(array) => {
            TypedArray::Float32(execute_float_sign(array, row_errors, span))
        }
        TypedArray::Float64(array) => {
            TypedArray::Float64(execute_float_sign(array, row_errors, span))
        }
        TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => return None,
    };
    Some(signs)
}

fn execute_float_sign<T>(
    input: &PrimitiveArray<T>,
    row_errors: &mut RowErrors,
    span: Span,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: CheckedFloat,
{
    let checked = numeric::float_sign(input);
    row_errors.push_failures(checked.failed.lanes(), span, |_| {
        SideErrorReason::NonFiniteResult(FloatOperation::Sign)
    });
    checked.column
}

/// `is_nan`, `is_finite` or `is_infinite`, or `None` when the value is not a float.
fn execute_float_classification(input: &TypedArray, class: FloatClass) -> Option<TypedArray> {
    let classified = match input {
        TypedArray::Float32(array) => class.evaluate(array),
        TypedArray::Float64(array) => class.evaluate(array),
        TypedArray::UInt8(_)
        | TypedArray::Int8(_)
        | TypedArray::UInt16(_)
        | TypedArray::Int16(_)
        | TypedArray::UInt32(_)
        | TypedArray::Int32(_)
        | TypedArray::UInt64(_)
        | TypedArray::Int64(_)
        | TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => return None,
    };
    Some(TypedArray::Boolean(classified))
}

/// `bitwise_and`, `bitwise_or` or `bitwise_xor`, or `None` unless both operands are integers of
/// one type.
fn execute_bitwise(
    left: &TypedArray,
    right: &TypedArray,
    operation: BitwiseOperation,
) -> Option<TypedArray> {
    let combined = match (left, right) {
        (TypedArray::UInt8(left), TypedArray::UInt8(right)) => {
            TypedArray::UInt8(operation.evaluate(left, right))
        }
        (TypedArray::Int8(left), TypedArray::Int8(right)) => {
            TypedArray::Int8(operation.evaluate(left, right))
        }
        (TypedArray::UInt16(left), TypedArray::UInt16(right)) => {
            TypedArray::UInt16(operation.evaluate(left, right))
        }
        (TypedArray::Int16(left), TypedArray::Int16(right)) => {
            TypedArray::Int16(operation.evaluate(left, right))
        }
        (TypedArray::UInt32(left), TypedArray::UInt32(right)) => {
            TypedArray::UInt32(operation.evaluate(left, right))
        }
        (TypedArray::Int32(left), TypedArray::Int32(right)) => {
            TypedArray::Int32(operation.evaluate(left, right))
        }
        (TypedArray::UInt64(left), TypedArray::UInt64(right)) => {
            TypedArray::UInt64(operation.evaluate(left, right))
        }
        (TypedArray::Int64(left), TypedArray::Int64(right)) => {
            TypedArray::Int64(operation.evaluate(left, right))
        }
        _ => return None,
    };
    Some(combined)
}

/// `bitwise_not`, or `None` when the value is not an integer.
fn execute_bitwise_not(input: &TypedArray) -> Option<TypedArray> {
    let complement = match input {
        TypedArray::UInt8(array) => TypedArray::UInt8(numeric::bitwise_complement(array)),
        TypedArray::Int8(array) => TypedArray::Int8(numeric::bitwise_complement(array)),
        TypedArray::UInt16(array) => TypedArray::UInt16(numeric::bitwise_complement(array)),
        TypedArray::Int16(array) => TypedArray::Int16(numeric::bitwise_complement(array)),
        TypedArray::UInt32(array) => TypedArray::UInt32(numeric::bitwise_complement(array)),
        TypedArray::Int32(array) => TypedArray::Int32(numeric::bitwise_complement(array)),
        TypedArray::UInt64(array) => TypedArray::UInt64(numeric::bitwise_complement(array)),
        TypedArray::Int64(array) => TypedArray::Int64(numeric::bitwise_complement(array)),
        TypedArray::Float32(_)
        | TypedArray::Float64(_)
        | TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => return None,
    };
    Some(complement)
}

/// `bit_count`, or `None` when the value is not an integer.
fn execute_bit_count(input: &TypedArray) -> Option<TypedArray> {
    let counts = match input {
        TypedArray::UInt8(array) => numeric::bit_count(array),
        TypedArray::Int8(array) => numeric::bit_count(array),
        TypedArray::UInt16(array) => numeric::bit_count(array),
        TypedArray::Int16(array) => numeric::bit_count(array),
        TypedArray::UInt32(array) => numeric::bit_count(array),
        TypedArray::Int32(array) => numeric::bit_count(array),
        TypedArray::UInt64(array) => numeric::bit_count(array),
        TypedArray::Int64(array) => numeric::bit_count(array),
        TypedArray::Float32(_)
        | TypedArray::Float64(_)
        | TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => return None,
    };
    Some(TypedArray::Int64(counts))
}

/// `shift_left` or `shift_right`, or `None` when the value or the count is not an integer.
fn execute_shift(
    values: &TypedArray,
    counts: &TypedArray,
    shift: Shift,
    row_errors: &mut RowErrors,
    span: Span,
) -> Option<TypedArray> {
    let counts = ShiftCounts::from_typed(counts)?;
    let shifted = match values {
        TypedArray::UInt8(array) => TypedArray::UInt8(execute_integer_shift(
            array, &counts, shift, row_errors, span,
        )),
        TypedArray::Int8(array) => TypedArray::Int8(execute_integer_shift(
            array, &counts, shift, row_errors, span,
        )),
        TypedArray::UInt16(array) => TypedArray::UInt16(execute_integer_shift(
            array, &counts, shift, row_errors, span,
        )),
        TypedArray::Int16(array) => TypedArray::Int16(execute_integer_shift(
            array, &counts, shift, row_errors, span,
        )),
        TypedArray::UInt32(array) => TypedArray::UInt32(execute_integer_shift(
            array, &counts, shift, row_errors, span,
        )),
        TypedArray::Int32(array) => TypedArray::Int32(execute_integer_shift(
            array, &counts, shift, row_errors, span,
        )),
        TypedArray::UInt64(array) => TypedArray::UInt64(execute_integer_shift(
            array, &counts, shift, row_errors, span,
        )),
        TypedArray::Int64(array) => TypedArray::Int64(execute_integer_shift(
            array, &counts, shift, row_errors, span,
        )),
        TypedArray::Float32(_)
        | TypedArray::Float64(_)
        | TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => return None,
    };
    Some(shifted)
}

fn execute_integer_shift<T>(
    values: &PrimitiveArray<T>,
    counts: &ShiftCounts,
    shift: Shift,
    row_errors: &mut RowErrors,
    span: Span,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: ShiftedInteger,
{
    let checked = shift.evaluate(values, counts);
    row_errors.push_failures(checked.failed.lanes(), span, |row| {
        shift.failure(counts, row)
    });
    checked.column
}

fn execute_concat(row_count: usize, parts: &[Operand<'_, StringArray>]) -> StringArray {
    // The reservation is only a hint: a part whose bytes cannot be counted in `usize` leaves the
    // buffer to grow as rows are written.
    let mut value_capacity = 0_usize;
    for part in parts {
        let part_bytes = match part {
            Operand::Column(array) => Some(array.values().len()),
            Operand::Scalar(array) => array.values().len().checked_mul(row_count),
        };
        if let Some(part_bytes) = part_bytes
            && let Some(reserved) = value_capacity.checked_add(part_bytes)
        {
            value_capacity = reserved;
        }
    }
    let mut builder = StringBuilder::with_capacity(row_count, value_capacity);
    let mut result = String::new();
    for row in 0..row_count {
        result.clear();
        for part in parts {
            if !part.is_null(row) {
                result.push_str(part.array().value(part.index(row)));
            }
        }
        builder.append_value(&result);
    }
    builder.finish()
}

fn execute_left(input: &StringArray, count: &TypedArray) -> Result<StringArray, RuntimeError> {
    let mut builder = string_builder_like(input);
    for row in 0..input.len() {
        if input.is_null(row) || count.is_null(row) {
            builder.append_null();
            continue;
        }
        let count = integral_value_at(count, row)?.unwrap_or(0);
        builder.append_value(string_left(input.value(row), count));
    }
    Ok(builder.finish())
}

fn execute_right(input: &StringArray, count: &TypedArray) -> Result<StringArray, RuntimeError> {
    let mut builder = string_builder_like(input);
    for row in 0..input.len() {
        if input.is_null(row) || count.is_null(row) {
            builder.append_null();
            continue;
        }
        let count = integral_value_at(count, row)?.unwrap_or(0);
        builder.append_value(string_right(input.value(row), count));
    }
    Ok(builder.finish())
}

fn execute_repeat(input: &StringArray, count: &TypedArray) -> Result<StringArray, RuntimeError> {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) || count.is_null(row) {
            builder.append_null();
            continue;
        }
        let count = integral_value_at(count, row)?.unwrap_or(0);
        let repeat = usize::try_from(count.max(0))
            .assured("a non-negative i64 fits usize on every supported host architecture");
        builder.append_value(input.value(row).repeat(repeat));
    }
    Ok(builder.finish())
}

/// The side of a value `lpad` and `rpad` extend.
#[derive(Clone, Copy)]
enum PadSide {
    Left,
    Right,
}

fn execute_pad(
    input: &StringArray,
    length: &TypedArray,
    fill: Operand<'_, StringArray>,
    side: PadSide,
) -> Result<StringArray, RuntimeError> {
    let mut builder = string_builder_like(input);
    let mut result = String::new();
    for row in 0..input.len() {
        if input.is_null(row) || length.is_null(row) || fill.is_null(row) {
            builder.append_null();
            continue;
        }
        let target_len = usize::try_from(integral_value_at(length, row)?.unwrap_or(0).max(0))
            .assured("a non-negative i64 fits usize on every supported host architecture");
        let source = input.value(row);
        let fill = fill.array().value(fill.index(row));
        let source_len = source.chars().count();
        if target_len == 0 {
            builder.append_value("");
            continue;
        }
        if source_len >= target_len {
            builder.append_value(string_prefix(source, target_len));
            continue;
        }
        if fill.is_empty() {
            builder.append_value(source);
            continue;
        }
        let missing = target_len - source_len;
        result.clear();
        // The reservation is only a hint: a requested pad width that cannot be sized in
        // `usize` leaves the buffer to grow as the fill is written.
        let mut reservation = source.len();
        if let Some(padding) = missing.checked_mul(fill.len())
            && let Some(reserved) = source.len().checked_add(padding)
        {
            reservation = reserved;
        }
        result.reserve(reservation);
        match side {
            PadSide::Left => {
                result.extend(fill.chars().cycle().take(missing));
                result.push_str(source);
            }
            PadSide::Right => {
                result.push_str(source);
                result.extend(fill.chars().cycle().take(missing));
            }
        }
        builder.append_value(&result);
    }
    Ok(builder.finish())
}

fn execute_md5(input: &StringArray) -> StringArray {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut builder = StringBuilder::with_capacity(
        input.len(),
        input
            .len()
            .checked_mul(32)
            .assured("a fixed width per row of a batch this node already holds in memory"),
    );
    let mut digest_text = String::with_capacity(32);
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            digest_text.clear();
            for byte in md5::compute(input.value(row)).0 {
                digest_text.push(char::from(HEX[usize::from(byte >> 4)]));
                digest_text.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
            builder.append_value(&digest_text);
        }
    }
    builder.finish()
}

fn execute_log(
    values: &[TypedArray],
    row_errors: &mut RowErrors,
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    match values {
        [value] => execute_math(value, MathFunction::Log10, row_errors, span),
        [base, value] => {
            execute_binary_math(base, value, BinaryMathFunction::Log, row_errors, span)
        }
        _ => Err(RuntimeError::InvalidBatch {
            message: format!("log requires one or two arguments, found {}", values.len()),
        }),
    }
}

/// Executes a regular-expression builtin over one batch.
///
/// A constant pattern was compiled with the program and is shared by every row. A pattern read
/// from the pattern argument is resolved once per distinct text in the batch through the call's
/// bounded cache. Each distinct pattern then borrows one search cache for the whole batch, so no
/// row compiles a pattern or takes a lock. An invalid pattern reports its error on every row that
/// evaluates it with a value.
fn execute_regexp(
    call: &RegexpCall,
    registers: &RegisterBank,
    inputs: &[RegisterRef],
    row_errors: &mut RowErrors,
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    let text = registers.column::<StringArray>(inputs[0])?;
    let patterns = match &call.pattern {
        PatternSource::Constant(constant) => {
            BatchPatterns::shared(StdArc::clone(constant.outcome()))
        }
        PatternSource::Argument(cache) => {
            let pattern = registers.operand::<StringArray>(inputs[1])?;
            cache.resolve_rows((0..text.len()).map(|row| {
                if pattern.is_null(row) {
                    None
                } else {
                    Some(pattern.array().value(pattern.index(row)))
                }
            }))
        }
    };
    let mut active = patterns
        .outcomes()
        .iter()
        .map(|outcome| outcome.activate())
        .collect::<Vec<_>>();
    let output = match call.function {
        RegexpFunction::Like => TypedArray::Boolean(execute_regexp_like(
            text,
            &patterns,
            &mut active,
            row_errors,
            span,
        )),
        RegexpFunction::Substr => TypedArray::Utf8(execute_regexp_substr(
            text,
            &patterns,
            &mut active,
            row_errors,
            span,
        )),
        RegexpFunction::Replace => {
            // The replacement follows the text and, when the pattern is read from an argument,
            // the pattern.
            let replacement_input = match &call.pattern {
                PatternSource::Constant(_) => 1,
                PatternSource::Argument(_) => 2,
            };
            let replacement = registers.operand::<StringArray>(inputs[replacement_input])?;
            TypedArray::Utf8(execute_regexp_replace(
                text,
                replacement,
                &patterns,
                &mut active,
                row_errors,
                span,
            ))
        }
    };
    Ok(output)
}

fn invalid_pattern_error(row: usize, error: &regex::Error, row_errors: &mut RowErrors, span: Span) {
    row_errors.push(
        row,
        SideError {
            reason: SideErrorReason::InvalidRegularExpression(error.clone()),
            span,
        },
    );
}

fn execute_regexp_like(
    text: &StringArray,
    patterns: &BatchPatterns,
    active: &mut [ActivePattern<'_>],
    row_errors: &mut RowErrors,
    span: Span,
) -> BooleanArray {
    let mut builder = BooleanBuilder::with_capacity(text.len());
    for row in 0..text.len() {
        if text.is_null(row) {
            builder.append_null();
            continue;
        }
        let Some(slot) = patterns.slot(row) else {
            builder.append_null();
            continue;
        };
        match &mut active[slot] {
            ActivePattern::Regex(regex) => builder.append_value(regex.is_match(text.value(row))),
            ActivePattern::Invalid(error) => {
                builder.append_null();
                invalid_pattern_error(row, error, row_errors, span);
            }
        }
    }
    builder.finish()
}

fn execute_regexp_replace(
    text: &StringArray,
    replacement: Operand<'_, StringArray>,
    patterns: &BatchPatterns,
    active: &mut [ActivePattern<'_>],
    row_errors: &mut RowErrors,
    span: Span,
) -> StringArray {
    let mut builder = string_builder_like(text);
    let mut replaced = String::new();
    for row in 0..text.len() {
        if text.is_null(row) || replacement.is_null(row) {
            builder.append_null();
            continue;
        }
        let Some(slot) = patterns.slot(row) else {
            builder.append_null();
            continue;
        };
        match &mut active[slot] {
            ActivePattern::Regex(regex) => {
                replaced.clear();
                regex.replace_all_into(
                    text.value(row),
                    replacement.array().value(replacement.index(row)),
                    &mut replaced,
                );
                builder.append_value(&replaced);
            }
            ActivePattern::Invalid(error) => {
                builder.append_null();
                invalid_pattern_error(row, error, row_errors, span);
            }
        }
    }
    builder.finish()
}

fn execute_regexp_substr(
    text: &StringArray,
    patterns: &BatchPatterns,
    active: &mut [ActivePattern<'_>],
    row_errors: &mut RowErrors,
    span: Span,
) -> StringArray {
    let mut builder = string_builder_like(text);
    for row in 0..text.len() {
        if text.is_null(row) {
            builder.append_null();
            continue;
        }
        let Some(slot) = patterns.slot(row) else {
            builder.append_null();
            continue;
        };
        match &mut active[slot] {
            ActivePattern::Regex(regex) => match regex.find(text.value(row)) {
                Some(matched) => builder.append_value(matched),
                None => builder.append_null(),
            },
            ActivePattern::Invalid(error) => {
                builder.append_null();
                invalid_pattern_error(row, error, row_errors, span);
            }
        }
    }
    builder.finish()
}

fn execute_replace(
    input: &StringArray,
    from: Operand<'_, StringArray>,
    to: Operand<'_, StringArray>,
) -> StringArray {
    let mut builder = string_builder_like(input);
    for row in 0..input.len() {
        if input.is_null(row) || from.is_null(row) || to.is_null(row) {
            builder.append_null();
            continue;
        }
        let value = input.value(row);
        let from = from.array().value(from.index(row));
        let to = to.array().value(to.index(row));

        let mut copied_until = 0;
        for (start, matched) in value.match_indices(from) {
            builder
                .write_str(&value[copied_until..start])
                .assured("fmt::Write over an in-memory string buffer has no failure mode");
            builder
                .write_str(to)
                .assured("fmt::Write over an in-memory string buffer has no failure mode");
            copied_until = start + matched.len();
        }
        builder
            .write_str(&value[copied_until..])
            .assured("fmt::Write over an in-memory string buffer has no failure mode");
        builder.append_value("");
    }
    builder.finish()
}

fn execute_reverse(input: &StringArray) -> StringArray {
    let mut builder = string_builder_like(input);
    let mut reversed = String::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            reversed.clear();
            reversed.extend(input.value(row).chars().rev());
            builder.append_value(&reversed);
        }
    }
    builder.finish()
}

fn execute_split_part(
    input: &StringArray,
    delimiter: Operand<'_, StringArray>,
    index: &TypedArray,
) -> Result<StringArray, RuntimeError> {
    let mut builder = string_builder_like(input);
    for row in 0..input.len() {
        if input.is_null(row) || delimiter.is_null(row) || index.is_null(row) {
            builder.append_null();
            continue;
        }
        let index = integral_value_at(index, row)?.unwrap_or(0);
        if index <= 0 {
            builder.append_value("");
            continue;
        }
        let string = input.value(row);
        let delimiter = delimiter.array().value(delimiter.index(row));
        if delimiter.is_empty() {
            builder.append_value(if index == 1 { string } else { "" });
            continue;
        }
        let value = string
            .split(delimiter)
            .nth(
                usize::try_from(index - 1)
                    .assured("the index was checked to be a positive i64 above"),
            )
            .unwrap_or("");
        builder.append_value(value);
    }
    Ok(builder.finish())
}

fn execute_strpos(input: &StringArray, needle: Operand<'_, StringArray>) -> Int64Array {
    let mut builder = Int64Builder::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) || needle.is_null(row) {
            builder.append_null();
            continue;
        }
        let needle = needle.array().value(needle.index(row));
        let value = if let Some(byte_idx) = input.value(row).find(needle) {
            i64::try_from(input.value(row)[..byte_idx].chars().count())
                .assured("a string's character count cannot exceed its isize-bounded byte length")
                + 1
        } else {
            0
        };
        builder.append_value(value);
    }
    builder.finish()
}

fn execute_substr(
    input: &StringArray,
    start: &TypedArray,
    length: Option<&TypedArray>,
) -> Result<StringArray, RuntimeError> {
    let mut builder = string_builder_like(input);
    for row in 0..input.len() {
        if input.is_null(row)
            || start.is_null(row)
            || length.is_some_and(|value| value.is_null(row))
        {
            builder.append_null();
            continue;
        }
        let start = integral_value_at(start, row)?.unwrap_or(1);
        let length = match length {
            Some(value) => Some(integral_value_at(value, row)?.unwrap_or(0)),
            None => None,
        };
        // SQL positions count from one, so a start at or before the first position begins at
        // the start of the string.
        let mut begin = 0;
        if let Some(offset) = start.checked_sub(1)
            && let Ok(offset) = usize::try_from(offset)
        {
            begin = offset;
        }
        let length = length.map(|value| {
            usize::try_from(value.max(0))
                .assured("a non-negative i64 fits usize on every supported host architecture")
        });
        builder.append_value(string_substr(input.value(row), begin, length));
    }
    Ok(builder.finish())
}

fn execute_to_hex(input: &TypedArray) -> Result<StringArray, RuntimeError> {
    let output = match input {
        TypedArray::UInt8(array) => execute_to_hex_values(array.len(), |row| {
            (!array.is_null(row)).then(|| u64::from(array.value(row)))
        }),
        TypedArray::Int8(array) => execute_to_hex_values(array.len(), |row| {
            (!array.is_null(row)).then(|| u64::from(array.value(row).cast_unsigned()))
        }),
        TypedArray::UInt16(array) => execute_to_hex_values(array.len(), |row| {
            (!array.is_null(row)).then(|| u64::from(array.value(row)))
        }),
        TypedArray::Int16(array) => execute_to_hex_values(array.len(), |row| {
            (!array.is_null(row)).then(|| u64::from(array.value(row).cast_unsigned()))
        }),
        TypedArray::UInt32(array) => execute_to_hex_values(array.len(), |row| {
            (!array.is_null(row)).then(|| u64::from(array.value(row)))
        }),
        TypedArray::Int32(array) => execute_to_hex_values(array.len(), |row| {
            (!array.is_null(row)).then(|| u64::from(array.value(row).cast_unsigned()))
        }),
        TypedArray::UInt64(array) => execute_to_hex_values(array.len(), |row| {
            (!array.is_null(row)).then(|| array.value(row))
        }),
        TypedArray::Int64(array) => execute_to_hex_values(array.len(), |row| {
            (!array.is_null(row)).then(|| array.value(row).cast_unsigned())
        }),
        TypedArray::Float32(_)
        | TypedArray::Float64(_)
        | TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => {
            return Err(RuntimeError::InvalidBatch {
                message: format!(
                    "to_hex requires integer input, found {:?}",
                    input.data_type()
                ),
            });
        }
    };
    Ok(output)
}

fn execute_to_hex_values(
    row_count: usize,
    mut value_at: impl FnMut(usize) -> Option<u64>,
) -> StringArray {
    let mut builder = StringBuilder::with_capacity(
        row_count,
        row_count
            .checked_mul(16)
            .assured("a fixed width per row of a batch this node already holds in memory"),
    );
    let mut formatted = String::with_capacity(16);
    for row in 0..row_count {
        let Some(value) = value_at(row) else {
            builder.append_null();
            continue;
        };
        formatted.clear();
        fmt::write(&mut formatted, format_args!("{value:x}"))
            .assured("fmt::Write over an in-memory string buffer has no failure mode");
        builder.append_value(&formatted);
    }
    builder.finish()
}

fn execute_translate(
    input: &StringArray,
    from: Operand<'_, StringArray>,
    to: Operand<'_, StringArray>,
) -> StringArray {
    let mut builder = string_builder_like(input);
    let mut table = TranslateTable::default();
    let mut translated = String::new();
    for row in 0..input.len() {
        if input.is_null(row) || from.is_null(row) || to.is_null(row) {
            builder.append_null();
            continue;
        }
        let source = input.value(row);
        let replacements = table.replacements(
            from.array().value(from.index(row)),
            to.array().value(to.index(row)),
        );
        translated.clear();
        for ch in source.chars() {
            if let Some(replacement) = replacements.get(&ch) {
                if let Some(replacement) = replacement {
                    translated.push(*replacement);
                }
            } else {
                translated.push(ch);
            }
        }
        builder.append_value(&translated);
    }
    builder.finish()
}

#[derive(Default)]
struct TranslateTable {
    from: String,
    to: String,
    replacements: HashMap<char, Option<char>>,
}

impl TranslateTable {
    fn replacements(&mut self, from: &str, to: &str) -> &HashMap<char, Option<char>> {
        if self.from != from || self.to != to {
            self.from.clear();
            self.from.push_str(from);
            self.to.clear();
            self.to.push_str(to);
            self.replacements.clear();
            let mut to_chars = to.chars();
            for from_char in from.chars() {
                let replacement = to_chars.next();
                self.replacements.entry(from_char).or_insert(replacement);
            }
        }
        &self.replacements
    }
}

fn integral_value_at(input: &TypedArray, row: usize) -> Result<Option<i64>, RuntimeError> {
    match input {
        TypedArray::UInt8(array) => Ok((!array.is_null(row)).then(|| i64::from(array.value(row)))),
        TypedArray::Int8(array) => Ok((!array.is_null(row)).then(|| i64::from(array.value(row)))),
        TypedArray::UInt16(array) => Ok((!array.is_null(row)).then(|| i64::from(array.value(row)))),
        TypedArray::Int16(array) => Ok((!array.is_null(row)).then(|| i64::from(array.value(row)))),
        TypedArray::UInt32(array) => Ok((!array.is_null(row)).then(|| i64::from(array.value(row)))),
        TypedArray::Int32(array) => Ok((!array.is_null(row)).then(|| i64::from(array.value(row)))),
        TypedArray::UInt64(array) => {
            Ok((!array.is_null(row)).then(|| i64::try_from(array.value(row)).unwrap_or(i64::MAX)))
        }
        TypedArray::Int64(array) => Ok((!array.is_null(row)).then(|| array.value(row))),
        TypedArray::Float32(_)
        | TypedArray::Float64(_)
        | TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => Err(RuntimeError::InvalidBatch {
            message: format!(
                "builtin requires integer input, found {:?}",
                input.data_type()
            ),
        }),
    }
}

fn string_prefix(value: &str, count: usize) -> &str {
    let end = match value.char_indices().nth(count) {
        Some((index, _)) => index,
        None => value.len(),
    };
    &value[..end]
}

fn string_substr(value: &str, start: usize, length: Option<usize>) -> &str {
    let start = match value.char_indices().nth(start) {
        Some((index, _)) => index,
        None => value.len(),
    };
    let remaining = &value[start..];
    match length {
        Some(length) => string_prefix(remaining, length),
        None => remaining,
    }
}

fn string_left(value: &str, count: i64) -> &str {
    if count >= 0 {
        string_prefix(
            value,
            usize::try_from(count).assured("count is a non-negative i64 on this branch"),
        )
    } else {
        let remove = count.unsigned_abs().arch_into();
        if remove == 0 {
            return value;
        }
        let end = match value.char_indices().rev().nth(remove - 1) {
            Some((index, _)) => index,
            None => 0,
        };
        &value[..end]
    }
}

fn string_right(value: &str, count: i64) -> &str {
    if count >= 0 {
        let keep = usize::try_from(count).assured("count is a non-negative i64 on this branch");
        if keep == 0 {
            return &value[value.len()..];
        }
        let start = match value.char_indices().rev().nth(keep - 1) {
            Some((index, _)) => index,
            None => 0,
        };
        &value[start..]
    } else {
        let skip = count.unsigned_abs().arch_into();
        let start = match value.char_indices().nth(skip) {
            Some((index, _)) => index,
            None => value.len(),
        };
        &value[start..]
    }
}

fn cast_typed_array(
    input: TypedArray,
    target: RegisterType,
    row_errors: &mut RowErrors,
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    if input.data_type() == target.data_type() {
        return Ok(input);
    }

    let cast_options = CastOptions {
        safe: true,
        format_options: FormatOptions::new().with_timestamp_tz_format(Some("%+")),
    };
    let output: ArrayRef = match (&input, target) {
        (TypedArray::Float32(values), RegisterType::Utf8) => {
            StdArc::new(display_values_as_utf8(values.len(), values.iter()))
        }
        (TypedArray::Float64(values), RegisterType::Utf8) => {
            StdArc::new(display_values_as_utf8(values.len(), values.iter()))
        }
        (TypedArray::Utf8(values), RegisterType::Datetime) => {
            StdArc::new(parse_rfc3339_datetimes(values))
        }
        (TypedArray::Boolean(values), RegisterType::Datetime) => {
            new_null_array(&target.data_type(), values.len())
        }
        (TypedArray::Datetime(values), RegisterType::Boolean) => {
            new_null_array(&target.data_type(), values.len())
        }
        _ => cast_with_options(input.as_array(), &target.data_type(), &cast_options)
            .map_err(|error| arrow_kernel_error("cast kernel failed", error))?,
    };
    let output = array_ref_to_typed_array(output)?;
    annotate_cast_failures(&input, &output, target, row_errors, span);
    Ok(output)
}

fn display_values_as_utf8<T>(len: usize, values: impl Iterator<Item = Option<T>>) -> StringArray
where
    T: fmt::Display,
{
    let mut builder = StringBuilder::with_capacity(
        len,
        len.checked_mul(8)
            .assured("a fixed width per row of a batch this node already holds in memory"),
    );
    for value in values {
        let Some(value) = value else {
            builder.append_null();
            continue;
        };
        write!(&mut builder, "{value}")
            .assured("fmt::Write over an in-memory string buffer has no failure mode");
        builder.append_value("");
    }
    builder.finish()
}

fn parse_rfc3339_datetimes(input: &StringArray) -> TimestampNanosecondArray {
    TimestampNanosecondArray::from_iter(input.iter().map(|value| {
        let value = value?;
        let Ok(value) = DateTime::parse_from_rfc3339(value) else {
            return None;
        };
        value.timestamp_nanos_opt()
    }))
    .with_timezone_utc()
}

fn annotate_cast_failures(
    input: &TypedArray,
    output: &TypedArray,
    target: RegisterType,
    row_errors: &mut RowErrors,
    span: Span,
) {
    let input = input.as_array();
    let output = output.as_array();
    // Casts propagate every input null, so equal cached null counts prove that the kernel did not
    // introduce a failure without comparing the full validity buffers.
    if input.null_count() == output.null_count() {
        return;
    }

    let input_nulls = input.nulls();
    let output_nulls = output.nulls().verified(
        "a cast never removes nulls, so the unequal count checked above leaves the output with \
         nulls and therefore a null buffer",
    );
    let invalid_output = !output_nulls.inner();
    let failures = match input_nulls {
        Some(input_nulls) => input_nulls.inner() & &invalid_output,
        None => invalid_output,
    };
    row_errors.push_failures(failures.set_indices(), span, |_| {
        SideErrorReason::CastFailed { target }
    });
}

fn filter_columns(
    columns: &[TypedArray],
    predicate: &BooleanArray,
    selected_count: usize,
) -> Result<Vec<TypedArray>, RuntimeError> {
    let filter = FilterBuilder::new(predicate).optimize().build();
    columns
        .iter()
        .map(|column| {
            if let TypedArray::Uninitialized { data_type, .. } = column {
                return Ok(TypedArray::uninitialized(data_type.clone(), selected_count));
            }
            let filtered = filter
                .filter(column.as_array())
                .map_err(|error| arrow_kernel_error("column filter kernel failed", error))?;
            array_ref_to_typed_array(filtered)
        })
        .collect()
}

fn selected_rows(predicate: &BooleanArray) -> Vec<usize> {
    predicate
        .iter()
        .enumerate()
        .filter_map(|(index, value)| match value {
            Some(true) => Some(index),
            Some(false) | None => None,
        })
        .collect()
}

fn row_selected(predicate: &BooleanArray, row: usize) -> bool {
    !predicate.is_null(row) && predicate.value(row)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Mutex, mpsc},
        time::Duration,
    };

    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        StringArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        types::Int64Type,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use nervix_models::Timestamp;
    use uuid::{Uuid, Version};

    use super::*;
    use crate::{
        CompileBinding, CompileOptions, ErrorCode, OutputBinding, compile_program_for_bindings,
        compile_program_with_options_for_bindings,
        program::{Program, SpannedNode},
        test_support::parse_program,
    };

    #[derive(Debug)]
    struct TestHeaderInjector;

    impl FunctionInjector for TestHeaderInjector {
        fn inject_with_context(
            &self,
            function: &FunctionName,
            arguments: &[TypedArray],
            row_count: usize,
            _span: Span,
            _now: Timestamp,
            _prior_error_rows: RowErrorMask<'_>,
        ) -> Result<InjectedResult, RuntimeError> {
            assert_eq!(*function, FunctionName::ReadHeader);
            let [TypedArray::Utf8(names)] = arguments else {
                panic!("read_header must receive one Utf8 array");
            };
            assert_eq!(names.len(), row_count);
            Ok(InjectedResult::success(TypedArray::Utf8(
                StringArray::from_iter(names.iter().map(|name| match name {
                    Some("route") => Some("primary"),
                    Some(_) | None => None,
                })),
            )))
        }
    }

    #[derive(Debug)]
    struct BlockingPolicyInjector {
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl FunctionInjector for BlockingPolicyInjector {
        fn execution_policy(&self, function: &FunctionName) -> FunctionExecutionPolicy {
            assert_eq!(*function, FunctionName::ReadHeader);
            FunctionExecutionPolicy::SpawnBlocking
        }

        fn inject_with_context(
            &self,
            function: &FunctionName,
            arguments: &[TypedArray],
            row_count: usize,
            span: Span,
            now: Timestamp,
            prior_error_rows: RowErrorMask<'_>,
        ) -> Result<InjectedResult, RuntimeError> {
            self.release
                .lock()
                .expect("release receiver lock must be available")
                .recv_timeout(Duration::from_secs(1))
                .map_err(|error| RuntimeError::InjectedFunctionFailed {
                    function: function.as_str().to_string(),
                    message: format!("blocking injector was not released: {error}"),
                })?;
            TestHeaderInjector.inject_with_context(
                function,
                arguments,
                row_count,
                span,
                now,
                prior_error_rows,
            )
        }
    }

    fn instruction_span(
        compiled: &CompiledProgram,
        predicate: impl Fn(&InstructionKind) -> bool,
    ) -> Span {
        compiled
            .instructions
            .iter()
            .find(|instruction| predicate(&instruction.kind))
            .map(|instruction| instruction.span)
            .expect("matching instruction must exist")
    }

    fn output_column<'a>(batch: &'a TypedBatch, name: &str) -> &'a TypedArray {
        let index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == name)
            .expect("output column must exist");
        batch.column(index)
    }

    fn schema(fields: Vec<Field>) -> StdArc<Schema> {
        StdArc::new(Schema::new(fields))
    }

    fn with_output_fields(input_schema: &StdArc<Schema>, fields: Vec<Field>) -> StdArc<Schema> {
        let mut output_fields = input_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        output_fields.extend(fields);
        schema(output_fields)
    }

    fn compile_program_with_output_fields(
        program: &SpannedNode<Program>,
        input_schema: StdArc<Schema>,
        fields: Vec<Field>,
    ) -> CompiledProgram {
        let output_schema = with_output_fields(&input_schema, fields);
        compile_program_for_bindings(
            program,
            output_schema,
            [CompileBinding::writable("input", input_schema)],
        )
        .expect("program must compile")
    }

    #[test]
    fn executes_program_and_populates_error_side_channel() {
        let parsed =
            parse_program("SET div = input.left / input.right, parsed = input.raw AS INT64")
                .expect("must parse");
        let schema = schema(vec![
            Field::new("left", DataType::Int64, true),
            Field::new("right", DataType::Int64, true),
            Field::new("raw", DataType::Utf8, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("div", DataType::Int64, true),
                Field::new("parsed", DataType::Int64, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Int64(Int64Array::from(vec![Some(8), Some(9)])),
                TypedArray::Int64(Int64Array::from(vec![Some(2), Some(0)])),
                TypedArray::Utf8(StringArray::from(vec![Some("12"), Some("bad")])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Int64(div) = output_column(&output, "div") else {
            panic!("div must be Int64");
        };
        let TypedArray::Int64(parsed) = output_column(&output, "parsed") else {
            panic!("parsed must be Int64");
        };
        let div_span = instruction_span(&compiled, |kind| {
            matches!(
                kind,
                InstructionKind::Binary {
                    op: BinaryOp::Div,
                    ..
                }
            )
        });
        let cast_span = instruction_span(&compiled, |kind| {
            matches!(
                kind,
                InstructionKind::Cast {
                    target: RegisterType::Int64,
                    ..
                }
            )
        });

        assert_eq!(div.value(0), 4);
        assert!(div.is_null(1));
        assert_eq!(parsed.value(0), 12);
        assert!(parsed.is_null(1));
        assert!(output.errors().row(0).is_empty());
        assert_eq!(output.errors().row(1).len(), 2);
        assert_eq!(output.errors().row(1)[0].span, div_span);
        assert_eq!(output.errors().row(1)[1].span, cast_span);
    }

    #[test]
    fn conditional_results_observe_only_selected_row_errors() {
        let parsed =
            parse_program("SET result = CASE WHEN input.run THEN 10 / input.divisor ELSE 0 END")
                .expect("conditional expression must parse");
        let schema = schema(vec![
            Field::new("run", DataType::Boolean, true),
            Field::new("divisor", DataType::Int64, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![Field::new("result", DataType::Int64, true)],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Boolean(BooleanArray::from(vec![
                    Some(false),
                    Some(true),
                    Some(true),
                    None,
                ])),
                TypedArray::Int64(Int64Array::from(vec![Some(0), Some(0), Some(2), Some(0)])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Int64(result) = output_column(&output, "result") else {
            panic!("result must be Int64");
        };

        assert_eq!(result.value(0), 0);
        assert!(result.is_null(1));
        assert_eq!(result.value(2), 5);
        assert_eq!(result.value(3), 0);
        assert!(output.errors().row(0).is_empty());
        assert_eq!(output.errors().row(1).len(), 1);
        assert_eq!(output.errors().row(1)[0].code(), ErrorCode::DivisionByZero);
        assert!(output.errors().row(2).is_empty());
        assert!(output.errors().row(3).is_empty());
    }

    #[test]
    fn case_falls_back_to_the_first_statically_true_branch() {
        let parsed = parse_program(
            "SET result = CASE WHEN input.number = 0 THEN 1 WHEN TRUE THEN 2 ELSE 3 END",
        )
        .expect("conditional expression must parse");
        let schema = schema(vec![Field::new("number", DataType::Int64, true)]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![Field::new("result", DataType::Int64, true)],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![TypedArray::Int64(Int64Array::from(vec![Some(0), Some(7)]))],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Int64(result) = output_column(&output, "result") else {
            panic!("result must be Int64");
        };

        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 2);
    }

    #[test]
    fn executes_null_assignment_to_declared_optional_field() {
        let parsed = parse_program("SET maybe = NULL").expect("must parse");
        let schema = schema(vec![Field::new("value", DataType::Utf8, true)]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![Field::new("maybe", DataType::Utf8, true)],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![TypedArray::Utf8(StringArray::from(vec![
                Some("a"),
                Some("b"),
            ]))],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Utf8(maybe) = output_column(&output, "maybe") else {
            panic!("maybe must be Utf8");
        };

        assert_eq!(maybe.len(), 2);
        assert!(maybe.is_null(0));
        assert!(maybe.is_null(1));
    }

    #[test]
    fn reading_uninitialized_input_uses_typed_null_semantics() {
        let parsed = parse_program("SET value = coalesce(input.value, 1)").expect("must parse");
        let input_schema = schema(vec![Field::new("value", DataType::Int64, true)]);
        let output_schema = schema(vec![Field::new("value", DataType::Int64, false)]);
        let compiled = compile_program_for_bindings(
            &parsed,
            output_schema,
            [CompileBinding::writable("input", input_schema.clone())],
        )
        .expect("coalesce must initialize the destination");
        let batch = TypedBatch::try_new(
            input_schema,
            vec![TypedArray::uninitialized(DataType::Int64, 2)],
        )
        .expect("uninitialized input must enter VM execution");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Int64(values) = output_column(&output, "value") else {
            panic!("value must be Int64");
        };

        assert_eq!(values.values(), &[1, 1]);
        assert_eq!(values.null_count(), 0);
    }

    #[test]
    fn directly_reading_uninitialized_input_initializes_nulls() {
        let parsed = parse_program("SET value = input.value").expect("must parse");
        let schema = schema(vec![Field::new("value", DataType::Int64, true)]);
        let compiled = compile_program_for_bindings(
            &parsed,
            schema.clone(),
            [CompileBinding::writable("input", schema.clone())],
        )
        .expect("direct assignment must compile");
        let batch =
            TypedBatch::try_new(schema, vec![TypedArray::uninitialized(DataType::Int64, 2)])
                .expect("uninitialized input must enter VM execution");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Int64(values) = output_column(&output, "value") else {
            panic!("value must be initialized as Int64 NULLs");
        };

        assert_eq!(values.null_count(), 2);
    }

    #[test]
    fn filters_rows_after_projection() {
        let parsed = parse_program("SET total = input.left + input.right WHERE input.keep")
            .expect("must parse");
        let schema = schema(vec![
            Field::new("keep", DataType::Boolean, true),
            Field::new("left", DataType::Int64, true),
            Field::new("right", DataType::Int64, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![Field::new("total", DataType::Int64, true)],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Boolean(BooleanArray::from(vec![Some(true), Some(false), None])),
                TypedArray::Int64(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
                TypedArray::Int64(Int64Array::from(vec![Some(4), Some(5), Some(6)])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Int64(total) = output_column(&output, "total") else {
            panic!("total must be Int64");
        };

        assert_eq!(output.row_count(), 1);
        assert_eq!(total.value(0), 5);
    }

    #[test]
    fn executes_filter_against_projected_output_rows() {
        let parsed =
            parse_program("SET lowered = lower(input.level) WHERE lower(input.level) = \"error\"")
                .expect("must parse");
        let schema = schema(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("urgent", DataType::Boolean, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![Field::new("lowered", DataType::Utf8, true)],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Boolean(BooleanArray::from(vec![
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(true),
                ])),
                TypedArray::Utf8(StringArray::from(vec![
                    Some("ERROR"),
                    Some("warn"),
                    Some("error"),
                    Some("info"),
                ])),
                TypedArray::Boolean(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                    Some(true),
                ])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_with_selection_sync(&compiled, &batch).expect("must execute");
        let TypedArray::Utf8(lowered) = output_column(&output.batch, "lowered") else {
            panic!("lowered must be Utf8");
        };

        assert_eq!(output.batch.row_count(), 2);
        assert_eq!(output.selected_rows, RowSelection::Selected(vec![0, 2]));
        assert_eq!(lowered.value(0), "error");
        assert_eq!(lowered.value(1), "error");
    }

    #[test]
    fn filter_preserves_evaluation_error_rows_for_caller_handling() {
        let parsed = parse_program("WHERE input.left / input.right > 0").expect("must parse");
        let schema = schema(vec![
            Field::new("left", DataType::Int64, false),
            Field::new("right", DataType::Int64, false),
        ]);
        let compiled = compile_program_with_output_fields(&parsed, schema.clone(), Vec::new());
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Int64(Int64Array::from(vec![8, 9, -1])),
                TypedArray::Int64(Int64Array::from(vec![2, 0, 1])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_with_selection_sync(&compiled, &batch).expect("must execute");

        assert_eq!(output.selected_rows, RowSelection::Selected(vec![0, 1]));
        assert!(output.batch.errors().row(0).is_empty());
        assert_eq!(
            output.batch.errors().row(1)[0].code(),
            ErrorCode::DivisionByZero
        );
    }

    #[test]
    fn unfiltered_execution_uses_identity_row_selection() {
        let parsed = parse_program("SET copy = input.value").expect("must parse");
        let input_schema = schema(vec![Field::new("value", DataType::Int64, false)]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            input_schema.clone(),
            vec![Field::new("copy", DataType::Int64, false)],
        );
        let batch = TypedBatch::try_new(
            input_schema,
            vec![TypedArray::Int64(Int64Array::from(vec![10, 20, 30]))],
        )
        .expect("batch must build");

        let output = execute_program_with_selection_sync(&compiled, &batch).expect("must execute");

        assert_eq!(output.selected_rows, RowSelection::All(3));
        assert_eq!(output.selected_rows.iter().collect::<Vec<_>>(), [0, 1, 2]);
    }

    #[test]
    fn optimized_string_slices_and_translation_preserve_unicode_semantics() {
        let value = "aé🙂z";

        assert_eq!(string_left(value, 2), "aé");
        assert_eq!(string_left(value, -1), "aé🙂");
        assert_eq!(string_right(value, 2), "🙂z");
        assert_eq!(string_right(value, -1), "é🙂z");
        assert_eq!(string_substr(value, 1, Some(2)), "é🙂");

        let from = StringArray::from(vec!["é🙂", "aab", "xy"]);
        let to = StringArray::from(vec!["EO", "XYZ", "Q"]);
        let translated = execute_translate(
            &StringArray::from(vec!["aé🙂z", "aba", "xyz"]),
            Operand::Column(&from),
            Operand::Column(&to),
        );
        assert_eq!(translated.value(0), "aEOz");
        assert_eq!(translated.value(1), "XZX");
        assert_eq!(translated.value(2), "Qz");
    }

    #[test]
    fn executes_dedicated_builtin_instruction() {
        let parsed = parse_program("SET lowered = lower(input.name)").expect("must parse");
        let schema = schema(vec![Field::new("name", DataType::Utf8, true)]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![Field::new("lowered", DataType::Utf8, true)],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![TypedArray::Utf8(StringArray::from(vec![
                Some("HeLLo"),
                None,
            ]))],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Utf8(lowered) = output_column(&output, "lowered") else {
            panic!("lowered must be Utf8");
        };

        assert_eq!(lowered.value(0), "hello");
        assert!(lowered.is_null(1));
    }

    #[test]
    fn case_discards_float_function_errors_from_unselected_arms() {
        let parsed = parse_program(
            "SET grown = CASE WHEN input.exponent < 700.0 THEN exp(input.exponent) ELSE 0.0 END, \
             rounded = CASE WHEN input.value < 1000.0 THEN round(input.value) ELSE 0.0 END",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("exponent", DataType::Float64, true),
            Field::new("value", DataType::Float64, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("grown", DataType::Float64, true),
                Field::new("rounded", DataType::Float64, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Float64(Float64Array::from(vec![Some(0.0), Some(1000.0)])),
                TypedArray::Float64(Float64Array::from(vec![Some(2.5), Some(f64::INFINITY)])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::Float64(grown) = output_column(&output, "grown") else {
            panic!("grown must be Float64");
        };
        let TypedArray::Float64(rounded) = output_column(&output, "rounded") else {
            panic!("rounded must be Float64");
        };

        assert_eq!(grown.value(0), 1.0);
        assert_eq!(rounded.value(0), 3.0);
        // The second row selects both ELSE arms. `exp(1000.0)` and `round(inf)` are not finite and
        // would each report an error, but only a selected arm may report one.
        assert_eq!(grown.value(1), 0.0);
        assert_eq!(rounded.value(1), 0.0);
        assert!(output.errors().is_error_free());
    }

    #[test]
    fn list_item_functions_reject_nested_elements() {
        let detection =
            DataType::FixedSizeList(StdArc::new(Field::new("item", DataType::Float32, true)), 6);
        let detections = DataType::List(StdArc::new(Field::new("item", detection.clone(), true)));
        let input_schema = schema(vec![Field::new("detections", detections, true)]);
        let output_schema = with_output_fields(
            &input_schema,
            vec![Field::new("detection", detection, true)],
        );

        for program in [
            "SET detection = first(input.detections)",
            "SET detection = last(input.detections)",
            "SET detection = nth(input.detections, 0)",
        ] {
            let parsed = parse_program(program).expect("must parse");
            let compiled = compile_program_for_bindings(
                &parsed,
                output_schema.clone(),
                [CompileBinding::writable("input", input_schema.clone())],
            );
            let Err(error) = compiled else {
                panic!("`{program}` selects a nested element and must be rejected");
            };
            assert_eq!(error.code, "unsupported_function");
            assert!(
                error
                    .message
                    .contains("requires ARRAY or VEC elements of a scalar type"),
                "unexpected rejection for `{program}`: {}",
                error.message
            );
        }
    }

    #[test]
    fn list_sum_reports_the_errors_the_addition_operator_reports() {
        let integers: ArrayRef = StdArc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(i64::MAX), Some(1)]),
            Some(vec![]),
            None,
        ]));
        let floats: ArrayRef = StdArc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([
            Some(vec![Some(1.5), Some(2.5)]),
            Some(vec![Some(f64::MAX), Some(f64::MAX)]),
            Some(vec![]),
            None,
        ]));
        let parsed =
            parse_program("SET total = sum(input.integers), float_total = sum(input.floats)")
                .expect("must parse");
        let schema = schema(vec![
            Field::new("integers", integers.data_type().clone(), true),
            Field::new("floats", floats.data_type().clone(), true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("total", DataType::Int64, true),
                Field::new("float_total", DataType::Float64, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![TypedArray::Generic(integers), TypedArray::Generic(floats)],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::Int64(total) = output_column(&output, "total") else {
            panic!("total must be Int64");
        };
        let TypedArray::Float64(float_total) = output_column(&output, "float_total") else {
            panic!("float_total must be Float64");
        };

        assert_eq!(total.value(0), 6);
        assert_eq!(float_total.value(0), 4.0);
        assert!(output.errors().row(0).is_empty());
        // `i64::MAX + 1` overflows and `f64::MAX + f64::MAX` is not finite, so the row reports what
        // `+` reports for the same operands instead of wrapping or emitting a non-finite value.
        assert!(total.is_null(1));
        assert!(float_total.is_null(1));
        let row_codes = output
            .errors()
            .row(1)
            .iter()
            .map(|error| error.code())
            .collect::<Vec<_>>();
        assert_eq!(
            row_codes,
            vec![ErrorCode::Overflow, ErrorCode::InvalidArgument]
        );
        // An empty list has no sum and a null list stays null. Neither is an error.
        assert!(total.is_null(2));
        assert!(float_total.is_null(2));
        assert!(output.errors().row(2).is_empty());
        assert!(total.is_null(3));
        assert!(float_total.is_null(3));
        assert!(output.errors().row(3).is_empty());
    }

    #[test]
    fn executes_array_builtins() {
        let values: ArrayRef = StdArc::new(
            ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(99)]),
                Some(vec![Some(1), None, Some(3)]),
                Some(vec![]),
                None,
                Some(vec![Some(100)]),
            ])
            .slice(1, 3),
        );
        let fixed: ArrayRef = StdArc::new(
            FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
                [
                    Some(vec![Some(98), Some(99)]),
                    Some(vec![Some(10), Some(20)]),
                    Some(vec![Some(30), Some(40)]),
                    Some(vec![Some(50), Some(60)]),
                    Some(vec![Some(70), Some(80)]),
                ],
                2,
            )
            .slice(1, 3),
        );
        let parsed = parse_program(
            "SET total = sum(input.values), first_value = first(input.values), last_value = \
             last(input.values), second_value = nth(input.values, 1), value_count = \
             count(input.values), fixed_last = last(input.fixed)",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("values", values.data_type().clone(), true),
            Field::new("fixed", fixed.data_type().clone(), true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("total", DataType::Int64, true),
                Field::new("first_value", DataType::Int64, true),
                Field::new("last_value", DataType::Int64, true),
                Field::new("second_value", DataType::Int64, true),
                Field::new("value_count", DataType::Int64, true),
                Field::new("fixed_last", DataType::Int64, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![TypedArray::Generic(values), TypedArray::Generic(fixed)],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Int64(total) = output_column(&output, "total") else {
            panic!("total must be Int64");
        };
        let TypedArray::Int64(first_value) = output_column(&output, "first_value") else {
            panic!("first_value must be Int64");
        };
        let TypedArray::Int64(last_value) = output_column(&output, "last_value") else {
            panic!("last_value must be Int64");
        };
        let TypedArray::Int64(second_value) = output_column(&output, "second_value") else {
            panic!("second_value must be Int64");
        };
        let TypedArray::Int64(value_count) = output_column(&output, "value_count") else {
            panic!("value_count must be Int64");
        };
        let TypedArray::Int64(fixed_last) = output_column(&output, "fixed_last") else {
            panic!("fixed_last must be Int64");
        };

        assert_eq!(total.value(0), 4);
        assert!(total.is_null(1));
        assert!(total.is_null(2));
        assert_eq!(first_value.value(0), 1);
        assert!(first_value.is_null(1));
        assert!(first_value.is_null(2));
        assert_eq!(last_value.value(0), 3);
        assert!(last_value.is_null(1));
        assert!(last_value.is_null(2));
        assert!(second_value.is_null(0));
        assert!(second_value.is_null(1));
        assert!(second_value.is_null(2));
        assert_eq!(value_count.value(0), 3);
        assert_eq!(value_count.value(1), 0);
        assert!(value_count.is_null(2));
        assert_eq!(fixed_last.value(0), 20);
        assert_eq!(fixed_last.value(1), 40);
        assert_eq!(fixed_last.value(2), 60);
    }

    #[test]
    fn executes_int64_negation_and_comparison_paths() {
        let parsed = parse_program("SET neg = -input.value, lt = input.left < input.right")
            .expect("must parse");
        let schema = schema(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("left", DataType::Int64, true),
            Field::new("right", DataType::Int64, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("neg", DataType::Int64, true),
                Field::new("lt", DataType::Boolean, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Int64(Int64Array::from(vec![Some(5), Some(i64::MIN)])),
                TypedArray::Int64(Int64Array::from(vec![Some(1), Some(3)])),
                TypedArray::Int64(Int64Array::from(vec![Some(2), Some(2)])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Int64(neg) = output_column(&output, "neg") else {
            panic!("neg must be Int64");
        };
        let TypedArray::Boolean(lt) = output_column(&output, "lt") else {
            panic!("lt must be Boolean");
        };

        assert_eq!(neg.value(0), -5);
        assert!(neg.is_null(1));
        assert!(lt.value(0));
        assert!(!lt.value(1));
        assert_eq!(output.errors().row(1).len(), 1);
        assert_eq!(output.errors().row(1)[0].code(), ErrorCode::Overflow);
    }

    #[test]
    fn executes_literals_not_sub_mul_and_null_propagation() {
        let parsed = parse_program(
            "SET lit = 41, notted = NOT input.flag, diff = input.left - input.right, product = \
             input.left * input.right",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("flag", DataType::Boolean, true),
            Field::new("left", DataType::Int64, true),
            Field::new("right", DataType::Int64, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("lit", DataType::Int64, true),
                Field::new("notted", DataType::Boolean, true),
                Field::new("diff", DataType::Int64, true),
                Field::new("product", DataType::Int64, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Boolean(BooleanArray::from(vec![Some(true), None])),
                TypedArray::Int64(Int64Array::from(vec![Some(7), None])),
                TypedArray::Int64(Int64Array::from(vec![Some(3), Some(5)])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Int64(lit) = output_column(&output, "lit") else {
            panic!("lit must be Int64");
        };
        let TypedArray::Boolean(notted) = output_column(&output, "notted") else {
            panic!("notted must be Boolean");
        };
        let TypedArray::Int64(diff) = output_column(&output, "diff") else {
            panic!("diff must be Int64");
        };
        let TypedArray::Int64(product) = output_column(&output, "product") else {
            panic!("product must be Int64");
        };

        assert_eq!(lit.value(0), 41);
        assert_eq!(lit.value(1), 41);
        assert!(!notted.value(0));
        assert!(notted.is_null(1));
        assert_eq!(diff.value(0), 4);
        assert!(diff.is_null(1));
        assert_eq!(product.value(0), 21);
        assert!(product.is_null(1));
    }

    #[test]
    fn executes_float_boolean_and_utf8_projection_paths() {
        let parsed = parse_program(
            "SET neg = -input.amount, total = input.left + input.right, cmp = input.left < \
             input.right, both = input.on AND input.off, uppered = upper(input.name), trimmed = \
             trim(input.name), len = length(input.name), lexical = input.name > input.other",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("amount", DataType::Float64, true),
            Field::new("left", DataType::Float64, true),
            Field::new("right", DataType::Float64, true),
            Field::new("on", DataType::Boolean, true),
            Field::new("off", DataType::Boolean, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("other", DataType::Utf8, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("neg", DataType::Float64, true),
                Field::new("total", DataType::Float64, true),
                Field::new("cmp", DataType::Boolean, true),
                Field::new("both", DataType::Boolean, true),
                Field::new("uppered", DataType::Utf8, true),
                Field::new("trimmed", DataType::Utf8, true),
                Field::new("len", DataType::Int64, true),
                Field::new("lexical", DataType::Boolean, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Float64(Float64Array::from(vec![Some(2.5), None])),
                TypedArray::Float64(Float64Array::from(vec![Some(1.5), Some(4.0)])),
                TypedArray::Float64(Float64Array::from(vec![Some(2.0), Some(1.0)])),
                TypedArray::Boolean(BooleanArray::from(vec![Some(true), Some(true)])),
                TypedArray::Boolean(BooleanArray::from(vec![Some(false), Some(true)])),
                TypedArray::Utf8(StringArray::from(vec![Some("  AbC "), None])),
                TypedArray::Utf8(StringArray::from(vec![Some("aaa"), Some("zzz")])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::Float64(neg) = output_column(&output, "neg") else {
            panic!("neg must be Float64");
        };
        let TypedArray::Float64(total) = output_column(&output, "total") else {
            panic!("total must be Float64");
        };
        let TypedArray::Boolean(cmp) = output_column(&output, "cmp") else {
            panic!("cmp must be Boolean");
        };
        let TypedArray::Boolean(both) = output_column(&output, "both") else {
            panic!("both must be Boolean");
        };
        let TypedArray::Utf8(uppered) = output_column(&output, "uppered") else {
            panic!("uppered must be Utf8");
        };
        let TypedArray::Utf8(trimmed) = output_column(&output, "trimmed") else {
            panic!("trimmed must be Utf8");
        };
        let TypedArray::Int64(len) = output_column(&output, "len") else {
            panic!("len must be Int64");
        };
        let TypedArray::Boolean(lexical) = output_column(&output, "lexical") else {
            panic!("lexical must be Boolean");
        };

        assert_eq!(neg.value(0), -2.5);
        assert!(neg.is_null(1));
        assert_eq!(total.value(0), 3.5);
        assert!(cmp.value(0));
        assert!(!both.value(0));
        assert!(both.value(1));
        assert_eq!(uppered.value(0), "  ABC ");
        assert_eq!(trimmed.value(0), "AbC");
        assert_eq!(len.value(0), 6);
        assert!(!lexical.value(0));
        assert!(uppered.is_null(1));
    }

    #[test]
    fn reports_non_finite_float_arithmetic_per_row() {
        let parsed = parse_program(
            "SET f64_result = input.f64_left / input.f64_right, f32_result = input.f32_left * \
             input.f32_right",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("f64_left", DataType::Float64, true),
            Field::new("f64_right", DataType::Float64, true),
            Field::new("f32_left", DataType::Float32, true),
            Field::new("f32_right", DataType::Float32, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("f64_result", DataType::Float64, true),
                Field::new("f32_result", DataType::Float32, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Float64(
                    Float64Array::from(vec![Some(-99.0), Some(6.0), Some(1.0), None, Some(-99.0)])
                        .slice(1, 3),
                ),
                TypedArray::Float64(
                    Float64Array::from(vec![
                        Some(-99.0),
                        Some(3.0),
                        Some(0.0),
                        Some(0.0),
                        Some(-99.0),
                    ])
                    .slice(1, 3),
                ),
                TypedArray::Float32(
                    Float32Array::from(vec![
                        Some(-99.0),
                        Some(2.0),
                        Some(f32::MAX),
                        Some(3.0),
                        Some(-99.0),
                    ])
                    .slice(1, 3),
                ),
                TypedArray::Float32(
                    Float32Array::from(vec![
                        Some(-99.0),
                        Some(4.0),
                        Some(2.0),
                        Some(f32::NAN),
                        Some(-99.0),
                    ])
                    .slice(1, 3),
                ),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Float64(f64_result) = output_column(&output, "f64_result") else {
            panic!("f64_result must be Float64");
        };
        let TypedArray::Float32(f32_result) = output_column(&output, "f32_result") else {
            panic!("f32_result must be Float32");
        };

        assert_eq!(f64_result.value(0), 2.0);
        assert_eq!(f32_result.value(0), 8.0);
        assert!(f64_result.is_null(1));
        assert!(f32_result.is_null(1));
        assert!(f64_result.is_null(2));
        assert!(f32_result.is_null(2));
        assert!(output.errors().row(0).is_empty());
        assert_eq!(output.errors().row(1).len(), 2);
        assert_eq!(output.errors().row(2).len(), 1);
        assert!(
            output
                .errors()
                .row(1)
                .iter()
                .chain(output.errors().row(2))
                .all(|error| error.code() == ErrorCode::InvalidArgument)
        );
    }

    #[test]
    fn executes_cast_matrix_and_reports_failures() {
        let parsed = parse_program(
            "SET i_from_f = input.flt AS INT64, i_from_b = input.flag AS INT64, f_from_b = \
             input.flag AS FLOAT64, s_from_i = input.num AS STRING, s_from_f = input.flt AS \
             STRING, s_from_b = input.flag AS STRING, b_from_i = input.num AS BOOLEAN, b_from_f = \
             input.flt AS BOOLEAN, b_from_s = input.txt AS BOOLEAN, f_from_s = input.txt AS \
             FLOAT64",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("flt", DataType::Float64, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("num", DataType::Int64, true),
            Field::new("txt", DataType::Utf8, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("i_from_f", DataType::Int64, true),
                Field::new("i_from_b", DataType::Int64, true),
                Field::new("f_from_b", DataType::Float64, true),
                Field::new("s_from_i", DataType::Utf8, true),
                Field::new("s_from_f", DataType::Utf8, true),
                Field::new("s_from_b", DataType::Utf8, true),
                Field::new("b_from_i", DataType::Boolean, true),
                Field::new("b_from_f", DataType::Boolean, true),
                Field::new("b_from_s", DataType::Boolean, true),
                Field::new("f_from_s", DataType::Float64, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Float64(Float64Array::from(vec![Some(1.0), Some(2.5)])),
                TypedArray::Boolean(BooleanArray::from(vec![Some(true), Some(false)])),
                TypedArray::Int64(Int64Array::from(vec![Some(1), Some(2)])),
                TypedArray::Utf8(StringArray::from(vec![Some("true"), Some("nan")])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::Int64(i_from_f) = output_column(&output, "i_from_f") else {
            panic!("i_from_f must be Int64");
        };
        let TypedArray::Int64(i_from_b) = output_column(&output, "i_from_b") else {
            panic!("i_from_b must be Int64");
        };
        let TypedArray::Float64(f_from_b) = output_column(&output, "f_from_b") else {
            panic!("f_from_b must be Float64");
        };
        let TypedArray::Utf8(s_from_i) = output_column(&output, "s_from_i") else {
            panic!("s_from_i must be Utf8");
        };
        let TypedArray::Utf8(s_from_f) = output_column(&output, "s_from_f") else {
            panic!("s_from_f must be Utf8");
        };
        let TypedArray::Utf8(s_from_b) = output_column(&output, "s_from_b") else {
            panic!("s_from_b must be Utf8");
        };
        let TypedArray::Boolean(b_from_i) = output_column(&output, "b_from_i") else {
            panic!("b_from_i must be Boolean");
        };
        let TypedArray::Boolean(b_from_f) = output_column(&output, "b_from_f") else {
            panic!("b_from_f must be Boolean");
        };
        let TypedArray::Boolean(b_from_s) = output_column(&output, "b_from_s") else {
            panic!("b_from_s must be Boolean");
        };
        let TypedArray::Float64(f_from_s) = output_column(&output, "f_from_s") else {
            panic!("f_from_s must be Float64");
        };

        assert_eq!(i_from_f.value(0), 1);
        assert_eq!(i_from_f.value(1), 2);
        assert_eq!(i_from_b.value(0), 1);
        assert_eq!(i_from_b.value(1), 0);
        assert_eq!(f_from_b.value(0), 1.0);
        assert_eq!(f_from_b.value(1), 0.0);
        assert_eq!(s_from_i.value(0), "1");
        assert_eq!(s_from_f.value(0), "1");
        assert_eq!(s_from_b.value(0), "true");
        assert!(b_from_i.value(0));
        assert!(b_from_i.value(1));
        assert!(b_from_f.value(0));
        assert!(b_from_f.value(1));
        assert!(b_from_s.value(0));
        assert!(b_from_s.is_null(1));
        assert!(f_from_s.is_null(0));
        assert!(f_from_s.value(1).is_nan());
        assert_eq!(output.errors().row(0).len(), 1);
        assert_eq!(output.errors().row(1).len(), 1);
    }

    #[test]
    fn kernel_casts_only_report_new_nulls() {
        let parsed =
            parse_program("SET parsed = input.text AS INT64, narrowed = input.wide AS INT8")
                .expect("must parse");
        let schema = schema(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("wide", DataType::Int64, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("parsed", DataType::Int64, true),
                Field::new("narrowed", DataType::Int8, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Utf8(StringArray::from(vec![
                    Some("7"),
                    None,
                    Some("bad"),
                    Some("-2"),
                ])),
                TypedArray::Int64(Int64Array::from(vec![Some(1), None, Some(128), Some(-2)])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Int64(parsed) = output_column(&output, "parsed") else {
            panic!("parsed must be Int64");
        };
        let TypedArray::Int8(narrowed) = output_column(&output, "narrowed") else {
            panic!("narrowed must be Int8");
        };

        assert_eq!(parsed.value(0), 7);
        assert_eq!(narrowed.value(0), 1);
        assert!(parsed.is_null(1));
        assert!(narrowed.is_null(1));
        assert!(parsed.is_null(2));
        assert!(narrowed.is_null(2));
        assert_eq!(parsed.value(3), -2);
        assert_eq!(narrowed.value(3), -2);
        assert!(output.errors().row(0).is_empty());
        assert!(output.errors().row(1).is_empty());
        assert_eq!(output.errors().row(2).len(), 2);
        assert!(
            output
                .errors()
                .row(2)
                .iter()
                .all(|error| error.code() == ErrorCode::CastFailed)
        );
        assert!(output.errors().row(3).is_empty());
    }

    #[test]
    fn preserves_row_errors_for_nonconvertible_scalar_cast_pairs() {
        let parsed = parse_program(
            "SET datetime_from_bool = input.flag AS DATETIME, bool_from_datetime = \
             input.occurred_at AS BOOLEAN",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("flag", DataType::Boolean, true),
            Field::new(
                "occurred_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                true,
            ),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new(
                    "datetime_from_bool",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                    true,
                ),
                Field::new("bool_from_datetime", DataType::Boolean, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Boolean(BooleanArray::from(vec![Some(true), None])),
                TypedArray::Datetime(
                    TimestampNanosecondArray::from(vec![Some(1), None]).with_timezone_utc(),
                ),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
        let TypedArray::Datetime(datetime_from_bool) = output_column(&output, "datetime_from_bool")
        else {
            panic!("datetime_from_bool must be Datetime");
        };
        let TypedArray::Boolean(bool_from_datetime) = output_column(&output, "bool_from_datetime")
        else {
            panic!("bool_from_datetime must be Boolean");
        };

        assert!(datetime_from_bool.is_null(0));
        assert!(datetime_from_bool.is_null(1));
        assert!(bool_from_datetime.is_null(0));
        assert!(bool_from_datetime.is_null(1));
        assert_eq!(output.errors().row(0).len(), 2);
        assert!(
            output
                .errors()
                .row(0)
                .iter()
                .all(|error| error.code() == ErrorCode::CastFailed)
        );
        assert!(output.errors().row(1).is_empty());
    }

    #[test]
    fn executes_extended_builtin_instructions() {
        let parsed = parse_program(
            "SET chosen = coalesce(input.primary, input.fallback), was_null = \
             is_null(input.primary), maybe = nullif(input.primary, input.fallback), has = \
             contains(input.text, input.needle), starts = starts_with(input.text, input.prefix), \
             ends = ends_with(input.text, input.suffix)",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("primary", DataType::Utf8, true),
            Field::new("fallback", DataType::Utf8, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("needle", DataType::Utf8, true),
            Field::new("prefix", DataType::Utf8, true),
            Field::new("suffix", DataType::Utf8, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("chosen", DataType::Utf8, true),
                Field::new("was_null", DataType::Boolean, true),
                Field::new("maybe", DataType::Utf8, true),
                Field::new("has", DataType::Boolean, true),
                Field::new("starts", DataType::Boolean, true),
                Field::new("ends", DataType::Boolean, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Utf8(StringArray::from(vec![None, Some("same"), Some("keep")])),
                TypedArray::Utf8(StringArray::from(vec![
                    Some("backup"),
                    Some("same"),
                    Some("other"),
                ])),
                TypedArray::Utf8(StringArray::from(vec![
                    Some("hello.rs"),
                    Some("banana"),
                    None,
                ])),
                TypedArray::Utf8(StringArray::from(vec![Some(".rs"), Some("nan"), Some("x")])),
                TypedArray::Utf8(StringArray::from(vec![Some("he"), Some("ba"), Some("z")])),
                TypedArray::Utf8(StringArray::from(vec![Some(".rs"), Some("na"), Some("y")])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::Utf8(chosen) = output_column(&output, "chosen") else {
            panic!("chosen must be Utf8");
        };
        let TypedArray::Boolean(was_null) = output_column(&output, "was_null") else {
            panic!("was_null must be Boolean");
        };
        let TypedArray::Utf8(maybe) = output_column(&output, "maybe") else {
            panic!("maybe must be Utf8");
        };
        let TypedArray::Boolean(has) = output_column(&output, "has") else {
            panic!("has must be Boolean");
        };
        let TypedArray::Boolean(starts) = output_column(&output, "starts") else {
            panic!("starts must be Boolean");
        };
        let TypedArray::Boolean(ends) = output_column(&output, "ends") else {
            panic!("ends must be Boolean");
        };

        assert_eq!(chosen.value(0), "backup");
        assert_eq!(chosen.value(1), "same");
        assert_eq!(chosen.value(2), "keep");
        assert!(was_null.value(0));
        assert!(!was_null.value(1));
        assert!(!was_null.value(2));
        assert!(maybe.is_null(0));
        assert!(maybe.is_null(1));
        assert_eq!(maybe.value(2), "keep");
        assert!(has.value(0));
        assert!(has.value(1));
        assert!(has.is_null(2));
        assert!(starts.value(0));
        assert!(starts.value(1));
        assert!(starts.is_null(2));
        assert!(ends.value(0));
        assert!(ends.value(1));
        assert!(ends.is_null(2));
        assert!(output.errors().is_error_free());
    }

    #[test]
    fn executes_abs_and_reports_overflow() {
        let parsed = parse_program("SET int_abs = abs(input.ints), float_abs = abs(input.floats)")
            .expect("must parse");
        let schema = schema(vec![
            Field::new("ints", DataType::Int64, true),
            Field::new("floats", DataType::Float64, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("int_abs", DataType::Int64, true),
                Field::new("float_abs", DataType::Float64, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Int64(Int64Array::from(vec![Some(-7), Some(i64::MIN), None])),
                TypedArray::Float64(Float64Array::from(vec![Some(-1.5), Some(2.25), None])),
            ],
        )
        .expect("batch must build");
        let int_abs_span = instruction_span(&compiled, |kind| {
            matches!(
                kind,
                InstructionKind::Builtin {
                    lowering: BuiltinLowering::Abs,
                    inputs,
                    ..
                } if inputs.first().is_some_and(|input| input.ty == RegisterType::Int64)
            )
        });

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::Int64(int_abs) = output_column(&output, "int_abs") else {
            panic!("int_abs must be Int64");
        };
        let TypedArray::Float64(float_abs) = output_column(&output, "float_abs") else {
            panic!("float_abs must be Float64");
        };

        assert_eq!(int_abs.value(0), 7);
        assert!(int_abs.is_null(1));
        assert!(int_abs.is_null(2));
        assert_eq!(float_abs.value(0), 1.5);
        assert_eq!(float_abs.value(1), 2.25);
        assert!(float_abs.is_null(2));
        assert_eq!(output.errors().row(0).len(), 0);
        assert_eq!(output.errors().row(1).len(), 1);
        assert_eq!(output.errors().row(1)[0].code(), ErrorCode::Overflow);
        assert_eq!(output.errors().row(1)[0].span, int_abs_span);
    }

    #[test]
    fn executes_narrow_numeric_and_float32_paths() {
        let parsed = parse_program(
            "SET u8_sum = input.u8 + (1 AS U8), i8_abs = abs(input.i8), u16_keep = \
             coalesce(input.u16, 0 AS U16), u32_same = nullif(input.u32, 999 AS U32), u64_sum = \
             input.u64 + (2 AS U64), f32_sum = input.f32 + (1.5 AS F32), f32_text = input.f32 AS \
             STRING",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("u8", DataType::UInt8, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("u32", DataType::UInt32, true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("f32", DataType::Float32, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("u8_sum", DataType::UInt8, true),
                Field::new("i8_abs", DataType::Int8, true),
                Field::new("u16_keep", DataType::UInt16, true),
                Field::new("u32_same", DataType::UInt32, true),
                Field::new("u64_sum", DataType::UInt64, true),
                Field::new("f32_sum", DataType::Float32, true),
                Field::new("f32_text", DataType::Utf8, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::UInt8(UInt8Array::from(vec![Some(5u8)])),
                TypedArray::Int8(Int8Array::from(vec![Some(-7i8)])),
                TypedArray::UInt16(UInt16Array::from(vec![Some(9u16)])),
                TypedArray::UInt32(UInt32Array::from(vec![Some(42u32)])),
                TypedArray::UInt64(UInt64Array::from(vec![Some(100u64)])),
                TypedArray::Float32(Float32Array::from(vec![Some(2.5f32)])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::UInt8(u8_sum) = output_column(&output, "u8_sum") else {
            panic!("u8_sum must be UInt8");
        };
        let TypedArray::Int8(i8_abs) = output_column(&output, "i8_abs") else {
            panic!("i8_abs must be Int8");
        };
        let TypedArray::UInt16(u16_keep) = output_column(&output, "u16_keep") else {
            panic!("u16_keep must be UInt16");
        };
        let TypedArray::UInt32(u32_same) = output_column(&output, "u32_same") else {
            panic!("u32_same must be UInt32");
        };
        let TypedArray::UInt64(u64_sum) = output_column(&output, "u64_sum") else {
            panic!("u64_sum must be UInt64");
        };
        let TypedArray::Float32(f32_sum) = output_column(&output, "f32_sum") else {
            panic!("f32_sum must be Float32");
        };
        let TypedArray::Utf8(f32_text) = output_column(&output, "f32_text") else {
            panic!("f32_text must be Utf8");
        };

        assert_eq!(u8_sum.value(0), 6);
        assert_eq!(i8_abs.value(0), 7);
        assert_eq!(u16_keep.value(0), 9);
        assert_eq!(u32_same.value(0), 42);
        assert_eq!(u64_sum.value(0), 102);
        assert_eq!(f32_sum.value(0), 4.0);
        assert_eq!(f32_text.value(0), "2.5");
        assert!(output.errors().row(0).is_empty());
    }

    #[test]
    fn executes_numeric_binary_dispatch_for_all_scalar_widths() {
        let parsed = parse_program(
            "SET u8_eq = input.u8 = (5 AS U8), u16_sum = input.u16 + (2 AS U16), i16_rem = \
             input.i16 % (4 AS I16), i32_gte = input.i32 >= (9 AS I32), u32_product = input.u32 * \
             (3 AS U32), u64_lt = input.u64 < (20 AS U64), f32_lte = input.f32 <= (1.5 AS F32)",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("u8", DataType::UInt8, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("i16", DataType::Int16, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("u32", DataType::UInt32, true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("f32", DataType::Float32, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("u8_eq", DataType::Boolean, true),
                Field::new("u16_sum", DataType::UInt16, true),
                Field::new("i16_rem", DataType::Int16, true),
                Field::new("i32_gte", DataType::Boolean, true),
                Field::new("u32_product", DataType::UInt32, true),
                Field::new("u64_lt", DataType::Boolean, true),
                Field::new("f32_lte", DataType::Boolean, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::UInt8(UInt8Array::from(vec![Some(5u8), Some(4u8)])),
                TypedArray::UInt16(UInt16Array::from(vec![Some(8u16), Some(9u16)])),
                TypedArray::Int16(Int16Array::from(vec![Some(10i16), Some(-9i16)])),
                TypedArray::Int32(Int32Array::from(vec![Some(9i32), Some(8i32)])),
                TypedArray::UInt32(UInt32Array::from(vec![Some(7u32), Some(11u32)])),
                TypedArray::UInt64(UInt64Array::from(vec![Some(19u64), Some(20u64)])),
                TypedArray::Float32(Float32Array::from(vec![Some(1.5f32), Some(2.0f32)])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::Boolean(u8_eq) = output_column(&output, "u8_eq") else {
            panic!("u8_eq must be Boolean");
        };
        let TypedArray::UInt16(u16_sum) = output_column(&output, "u16_sum") else {
            panic!("u16_sum must be UInt16");
        };
        let TypedArray::Int16(i16_rem) = output_column(&output, "i16_rem") else {
            panic!("i16_rem must be Int16");
        };
        let TypedArray::Boolean(i32_gte) = output_column(&output, "i32_gte") else {
            panic!("i32_gte must be Boolean");
        };
        let TypedArray::UInt32(u32_product) = output_column(&output, "u32_product") else {
            panic!("u32_product must be UInt32");
        };
        let TypedArray::Boolean(u64_lt) = output_column(&output, "u64_lt") else {
            panic!("u64_lt must be Boolean");
        };
        let TypedArray::Boolean(f32_lte) = output_column(&output, "f32_lte") else {
            panic!("f32_lte must be Boolean");
        };

        assert!(u8_eq.value(0));
        assert!(!u8_eq.value(1));
        assert_eq!(u16_sum.value(0), 10);
        assert_eq!(u16_sum.value(1), 11);
        assert_eq!(i16_rem.value(0), 2);
        assert_eq!(i16_rem.value(1), -1);
        assert!(i32_gte.value(0));
        assert!(!i32_gte.value(1));
        assert_eq!(u32_product.value(0), 21);
        assert_eq!(u32_product.value(1), 33);
        assert!(u64_lt.value(0));
        assert!(!u64_lt.value(1));
        assert!(f32_lte.value(0));
        assert!(!f32_lte.value(1));
        assert!(output.errors().is_error_free());
    }

    #[test]
    fn converts_case_with_unicode_mappings_over_sliced_and_multibyte_values() {
        let input = StringArray::from(vec![
            Some("skipped-ü"),
            Some("MiXeD"),
            None,
            Some("grüßen-ok"),
            Some(""),
            Some("\u{39f}\u{394}\u{39f}\u{3a3}"),
        ]);
        // A sliced column still points at the full value buffer, so offsets and validity have
        // to stay aligned with the untouched bytes around the slice. This slice holds only ASCII
        // text between non-ASCII neighbours.
        let ascii = input.slice(1, 2);
        assert_eq!(
            CaseMapping::Upper.execute(&ascii),
            StringArray::from(vec![Some("MIXED"), None])
        );
        assert_eq!(
            CaseMapping::Lower.execute(&ascii),
            StringArray::from(vec![Some("mixed"), None])
        );

        let sliced = input.slice(1, 5);

        let upper = CaseMapping::Upper.execute(&sliced);
        let lower = CaseMapping::Lower.execute(&sliced);

        assert_eq!(upper.len(), 5);
        assert_eq!(upper.value(0), "MIXED");
        assert!(upper.is_null(1));
        // Case conversion uses Unicode's full mappings, so `ß` uppercases to `SS` and the output
        // value is longer than the input it was built from.
        assert_eq!(upper.value(2), "GRÜSSEN-OK");
        assert_eq!(upper.value(3), "");
        assert_eq!(upper.value(4), "\u{39f}\u{394}\u{39f}\u{3a3}");
        assert_eq!(lower.value(0), "mixed");
        assert!(lower.is_null(1));
        assert_eq!(lower.value(2), "grüßen-ok");
        assert_eq!(lower.value(3), "");
        // A trailing sigma lowercases to its final form. The condition is contextual but not
        // locale-dependent, so it belongs to the one mapping both folding and execution apply.
        assert_eq!(lower.value(4), "\u{3bf}\u{3b4}\u{3bf}\u{3c2}");
    }

    #[test]
    fn folded_and_executed_case_calls_produce_the_same_value() {
        let texts = [
            "",
            "plain ascii",
            "Grüßen",
            // A capital sigma lowercases to its final form only at the end of a word.
            "\u{39f}\u{394}\u{39f}\u{3a3} \u{3a3}\u{39f}\u{3a3}",
            // A dotted capital I lowercases to two scalar values.
            "\u{130}stanbul",
            // A titlecase digraph has distinct lowercase and uppercase forms.
            "\u{1c5}emal",
        ];
        for function in ["lower", "upper"] {
            for text in texts {
                let source =
                    format!("SET folded = {function}('{text}'), executed = {function}(input.raw)");
                let parsed = parse_program(&source).expect("must parse");
                let schema = schema(vec![Field::new("raw", DataType::Utf8, true)]);
                let compiled = compile_program_with_output_fields(
                    &parsed,
                    schema.clone(),
                    vec![
                        Field::new("folded", DataType::Utf8, true),
                        Field::new("executed", DataType::Utf8, true),
                    ],
                );
                // The literal call folds away, leaving the column call as the only case builtin.
                let case_builtins = compiled
                    .instructions
                    .iter()
                    .filter(|instruction| {
                        matches!(
                            instruction.kind,
                            InstructionKind::Builtin {
                                lowering: BuiltinLowering::Lower | BuiltinLowering::Upper,
                                ..
                            }
                        )
                    })
                    .count();
                assert_eq!(case_builtins, 1, "{source}");
                let batch = TypedBatch::try_new(
                    schema,
                    vec![TypedArray::Utf8(StringArray::from(vec![Some(text)]))],
                )
                .expect("batch must build");

                let output =
                    execute_program_sync(&compiled, &batch).expect("execution must succeed");

                let TypedArray::Utf8(folded) = output_column(&output, "folded") else {
                    panic!("folded must be Utf8");
                };
                let TypedArray::Utf8(executed) = output_column(&output, "executed") else {
                    panic!("executed must be Utf8");
                };
                assert_eq!(folded.value(0), executed.value(0), "{source}");
            }
        }
    }

    #[test]
    fn function_aliases_evaluate_like_their_canonical_names() {
        let parsed = parse_program(
            "SET ceil_value = ceil(input.amount), ceiling_value = ceiling(input.amount), \
             pow_value = pow(input.amount, 2.0), power_value = power(input.amount, 2.0), \
             substr_value = substr(input.text, 2, 3), substring_value = substring(input.text, 2, \
             3), trim_value = trim(input.text), btrim_value = btrim(input.text), length_value = \
             length(input.text), char_length_value = char_length(input.text)",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("amount", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("ceil_value", DataType::Float64, true),
                Field::new("ceiling_value", DataType::Float64, true),
                Field::new("pow_value", DataType::Float64, true),
                Field::new("power_value", DataType::Float64, true),
                Field::new("substr_value", DataType::Utf8, true),
                Field::new("substring_value", DataType::Utf8, true),
                Field::new("trim_value", DataType::Utf8, true),
                Field::new("btrim_value", DataType::Utf8, true),
                Field::new("length_value", DataType::Int64, true),
                Field::new("char_length_value", DataType::Int64, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Float64(Float64Array::from(vec![Some(1.25), Some(-2.5), None])),
                TypedArray::Utf8(StringArray::from(vec![Some(" grüßen "), Some(""), None])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        for (canonical, alias) in [
            ("ceil_value", "ceiling_value"),
            ("pow_value", "power_value"),
            ("substr_value", "substring_value"),
            ("trim_value", "btrim_value"),
            ("length_value", "char_length_value"),
        ] {
            assert_eq!(
                output_column(&output, canonical).as_array(),
                output_column(&output, alias).as_array(),
                "{alias} must evaluate like {canonical}"
            );
        }
        assert!(output.errors().is_error_free());
    }

    #[test]
    fn nullif_uses_the_same_equality_as_the_comparison_operator() {
        let parsed = parse_program(
            "SET equal = input.left = input.right, nullified = is_null(nullif(input.left, \
             input.right))",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("left", DataType::Float64, true),
            Field::new("right", DataType::Float64, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("equal", DataType::Boolean, true),
                Field::new("nullified", DataType::Boolean, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Float64(Float64Array::from(vec![
                    Some(f64::NAN),
                    Some(0.0),
                    Some(1.5),
                    None,
                ])),
                TypedArray::Float64(Float64Array::from(vec![
                    Some(f64::NAN),
                    Some(-0.0),
                    Some(1.5),
                    Some(1.5),
                ])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::Boolean(equal) = output_column(&output, "equal") else {
            panic!("equal must be Boolean");
        };
        let TypedArray::Boolean(nullified) = output_column(&output, "nullified") else {
            panic!("nullified must be Boolean");
        };

        // `nullif` nulls its first argument exactly where `=` holds. NaN equals nothing, including
        // itself, and `0.0` equals `-0.0`.
        assert!(!equal.value(0));
        assert!(!nullified.value(0));
        assert!(equal.value(1));
        assert!(nullified.value(1));
        assert!(equal.value(2));
        assert!(nullified.value(2));
        // A null operand makes the comparison null and leaves the already-null value in place.
        assert!(equal.is_null(3));
        assert!(nullified.value(3));
    }

    #[test]
    fn text_buffer_builtins_preserve_unicode_nulls_and_slices() {
        let input = StringArray::from(vec![
            Some("skipped"),
            Some("\u{2003}é界\u{2009}"),
            None,
            Some(" plain "),
            Some(""),
            Some("ignored"),
        ])
        .slice(1, 4);

        assert_eq!(
            execute_trim(&input),
            StringArray::from(vec![Some("é界"), None, Some("plain"), Some("")])
        );
        assert_eq!(
            execute_ltrim(&input),
            StringArray::from(vec![Some("é界\u{2009}"), None, Some("plain "), Some("")])
        );
        assert_eq!(
            execute_rtrim(&input),
            StringArray::from(vec![Some("\u{2003}é界"), None, Some(" plain"), Some("")])
        );
        assert_eq!(
            execute_length(&input),
            Int64Array::from(vec![Some(4), None, Some(7), Some(0)])
        );
        assert_eq!(
            execute_bit_length(&input),
            Int64Array::from(vec![Some(88), None, Some(56), Some(0)])
        );

        let replace_input = StringArray::from(vec![
            Some("skipped"),
            Some("é界é"),
            None,
            Some("banana"),
            Some(""),
            Some("ignored"),
        ])
        .slice(1, 4);
        let from = StringArray::from(vec![
            Some("skip"),
            Some("é"),
            Some("x"),
            Some("na"),
            Some(""),
            Some("ignore"),
        ])
        .slice(1, 4);
        let to = StringArray::from(vec![
            Some("skip"),
            Some("X"),
            Some("y"),
            Some("_"),
            Some("-"),
            Some("ignore"),
        ])
        .slice(1, 4);
        assert_eq!(
            execute_replace(&replace_input, Operand::Column(&from), Operand::Column(&to)),
            StringArray::from(vec![Some("X界X"), None, Some("ba__"), Some("-")])
        );
    }

    #[test]
    fn compares_nan_floats_with_ieee_semantics() {
        let parsed = parse_program(
            "SET eq = input.left = input.right, neq = input.left != input.right, gt = input.left \
             > input.right, lt = input.left < input.right",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("left", DataType::Float64, true),
            Field::new("right", DataType::Float64, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("eq", DataType::Boolean, true),
                Field::new("neq", DataType::Boolean, true),
                Field::new("gt", DataType::Boolean, true),
                Field::new("lt", DataType::Boolean, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Float64(Float64Array::from(vec![
                    Some(f64::NAN),
                    Some(f64::NAN),
                    Some(1.0),
                ])),
                TypedArray::Float64(Float64Array::from(vec![
                    Some(f64::NAN),
                    Some(1.0),
                    Some(1.0),
                ])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::Boolean(eq) = output_column(&output, "eq") else {
            panic!("eq must be Boolean");
        };
        let TypedArray::Boolean(neq) = output_column(&output, "neq") else {
            panic!("neq must be Boolean");
        };
        let TypedArray::Boolean(gt) = output_column(&output, "gt") else {
            panic!("gt must be Boolean");
        };
        let TypedArray::Boolean(lt) = output_column(&output, "lt") else {
            panic!("lt must be Boolean");
        };

        // NaN is unordered against every value including itself, so only `!=` holds.
        assert!(!eq.value(0));
        assert!(neq.value(0));
        assert!(!gt.value(0));
        assert!(!lt.value(0));
        assert!(!eq.value(1));
        assert!(neq.value(1));
        assert!(!gt.value(1));
        assert!(!lt.value(1));
        assert!(eq.value(2));
        assert!(!neq.value(2));
        assert!(output.errors().is_error_free());
    }

    #[test]
    fn executes_datetime_comparisons_and_casts() {
        let parsed = parse_program(
            "SET occurred_text = input.occurred_at AS STRING, occurred_roundtrip = \
             (input.occurred_at AS STRING) AS DATETIME, occurred_nanos = input.occurred_at AS \
             INT64 WHERE input.occurred_at > ('2026-04-07T00:00:00Z' AS DATETIME)",
        )
        .expect("must parse");
        let schema = schema(vec![Field::new(
            "occurred_at",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            true,
        )]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("occurred_text", DataType::Utf8, true),
                Field::new(
                    "occurred_roundtrip",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                    true,
                ),
                Field::new("occurred_nanos", DataType::Int64, true),
            ],
        );
        let expected_occured_nanos =
            chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56.123456789Z")
                .expect("valid timestamp")
                .timestamp_nanos_opt()
                .expect("timestamp must fit in nanoseconds");
        let batch = TypedBatch::try_new(
            schema,
            vec![TypedArray::Datetime(
                TimestampNanosecondArray::from(vec![
                    Some(expected_occured_nanos),
                    Some(
                        chrono::DateTime::parse_from_rfc3339("2026-04-06T23:59:59Z")
                            .expect("valid timestamp")
                            .timestamp_nanos_opt()
                            .expect("timestamp must fit in nanoseconds"),
                    ),
                ])
                .with_timezone_utc(),
            )],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::Utf8(occurred_text) = output_column(&output, "occurred_text") else {
            panic!("occurred_text must be Utf8");
        };
        let TypedArray::Datetime(occurred_roundtrip) = output_column(&output, "occurred_roundtrip")
        else {
            panic!("occurred_roundtrip must be Datetime");
        };
        let TypedArray::Int64(occurred_nanos) = output_column(&output, "occurred_nanos") else {
            panic!("occurred_nanos must be Int64");
        };

        assert_eq!(output.row_count(), 1);
        assert_eq!(
            occurred_text.value(0),
            "2026-04-07T12:34:56.123456789+00:00"
        );
        assert_eq!(occurred_roundtrip.value(0), expected_occured_nanos);
        assert_eq!(occurred_nanos.value(0), expected_occured_nanos);
        assert!(output.errors().row(0).is_empty());
    }

    #[test]
    fn executes_extended_text_regex_and_contextual_builtins() {
        let parsed = parse_program(
            "SET now_value = now(), uuid4 = uuid_v4(), uuid7 = uuid_v7(), bits = \
             bit_length(input.plain), ascii_value = ascii(input.plain), trimmed = \
             btrim(input.spaced), chars = char_length(input.spaced), joined = \
             concat(input.prefix, input.fill, input.prefix), titled = initcap(input.spaced), \
             lefted = left(input.plain, input.count), lowered = lower(input.plain), lpaded = \
             lpad(input.prefix, input.width, input.fill), ltrimmed = ltrim(input.spaced), digest \
             = md5(input.prefix), repeated = repeat(input.prefix, input.count), replaced = \
             replace(input.plain, input.prefix, input.replacement), reversed = \
             reverse(input.prefix), righted = right(input.plain, input.count), rpaded = \
             rpad(input.prefix, input.width, input.fill), rtrimmed = rtrim(input.spaced), part = \
             split_part(input.dotted, input.delim, input.count), starts = \
             starts_with(input.plain, input.prefix), pos = strpos(input.plain, input.prefix), \
             piece = substr(input.plain, input.start, input.length), hexed = \
             to_hex(input.hex_value), translated = translate(input.prefix, input.from_chars, \
             input.to_chars), trimmed2 = trim(input.spaced), uppered = upper(input.prefix), \
             regex_ok = regexp_like(input.plain, input.pattern), regex_replaced = \
             regexp_replace(input.plain, input.pattern, input.replacement), regex_piece = \
             regexp_substr(input.spaced, input.pattern)",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("spaced", DataType::Utf8, true),
            Field::new("plain", DataType::Utf8, true),
            Field::new("dotted", DataType::Utf8, true),
            Field::new("prefix", DataType::Utf8, true),
            Field::new("fill", DataType::Utf8, true),
            Field::new("replacement", DataType::Utf8, true),
            Field::new("pattern", DataType::Utf8, true),
            Field::new("from_chars", DataType::Utf8, true),
            Field::new("to_chars", DataType::Utf8, true),
            Field::new("delim", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
            Field::new("width", DataType::Int64, true),
            Field::new("start", DataType::Int64, true),
            Field::new("length", DataType::Int64, true),
            Field::new("hex_value", DataType::Int64, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new(
                    "now_value",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                    true,
                ),
                Field::new("uuid4", DataType::Utf8, true),
                Field::new("uuid7", DataType::Utf8, true),
                Field::new("bits", DataType::Int64, true),
                Field::new("ascii_value", DataType::Int64, true),
                Field::new("trimmed", DataType::Utf8, true),
                Field::new("chars", DataType::Int64, true),
                Field::new("joined", DataType::Utf8, true),
                Field::new("titled", DataType::Utf8, true),
                Field::new("lefted", DataType::Utf8, true),
                Field::new("lowered", DataType::Utf8, true),
                Field::new("lpaded", DataType::Utf8, true),
                Field::new("ltrimmed", DataType::Utf8, true),
                Field::new("digest", DataType::Utf8, true),
                Field::new("repeated", DataType::Utf8, true),
                Field::new("replaced", DataType::Utf8, true),
                Field::new("reversed", DataType::Utf8, true),
                Field::new("righted", DataType::Utf8, true),
                Field::new("rpaded", DataType::Utf8, true),
                Field::new("rtrimmed", DataType::Utf8, true),
                Field::new("part", DataType::Utf8, true),
                Field::new("starts", DataType::Boolean, true),
                Field::new("pos", DataType::Int64, true),
                Field::new("piece", DataType::Utf8, true),
                Field::new("hexed", DataType::Utf8, true),
                Field::new("translated", DataType::Utf8, true),
                Field::new("trimmed2", DataType::Utf8, true),
                Field::new("uppered", DataType::Utf8, true),
                Field::new("regex_ok", DataType::Boolean, true),
                Field::new("regex_replaced", DataType::Utf8, true),
                Field::new("regex_piece", DataType::Utf8, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Utf8(StringArray::from(vec![Some("  hello.world  ")])),
                TypedArray::Utf8(StringArray::from(vec![Some("hello")])),
                TypedArray::Utf8(StringArray::from(vec![Some("alpha.beta.gamma")])),
                TypedArray::Utf8(StringArray::from(vec![Some("he")])),
                TypedArray::Utf8(StringArray::from(vec![Some("xy")])),
                TypedArray::Utf8(StringArray::from(vec![Some("XX")])),
                TypedArray::Utf8(StringArray::from(vec![Some("h[a-z]+")])),
                TypedArray::Utf8(StringArray::from(vec![Some("he")])),
                TypedArray::Utf8(StringArray::from(vec![Some("HE")])),
                TypedArray::Utf8(StringArray::from(vec![Some(".")])),
                TypedArray::Int64(Int64Array::from(vec![Some(2)])),
                TypedArray::Int64(Int64Array::from(vec![Some(7)])),
                TypedArray::Int64(Int64Array::from(vec![Some(2)])),
                TypedArray::Int64(Int64Array::from(vec![Some(3)])),
                TypedArray::Int64(Int64Array::from(vec![Some(255)])),
            ],
        )
        .expect("batch must build");
        let context_now = Timestamp::from_unix_nanos(1_776_777_888_999_000_111);

        let output = execute_program_in_context_sync(
            &compiled,
            &batch,
            &ExecutionContext {
                now: context_now,
                injector: None,
            },
        )
        .expect("execution must succeed")
        .batch;

        let TypedArray::Datetime(now_value) = output_column(&output, "now_value") else {
            panic!("now_value must be Datetime");
        };
        let TypedArray::Utf8(uuid4) = output_column(&output, "uuid4") else {
            panic!("uuid4 must be Utf8");
        };
        let TypedArray::Utf8(uuid7) = output_column(&output, "uuid7") else {
            panic!("uuid7 must be Utf8");
        };
        let TypedArray::Int64(bits) = output_column(&output, "bits") else {
            panic!("bits must be Int64");
        };
        let TypedArray::Int64(ascii_value) = output_column(&output, "ascii_value") else {
            panic!("ascii_value must be Int64");
        };
        let TypedArray::Utf8(trimmed) = output_column(&output, "trimmed") else {
            panic!("trimmed must be Utf8");
        };
        let TypedArray::Int64(chars) = output_column(&output, "chars") else {
            panic!("chars must be Int64");
        };
        let TypedArray::Utf8(joined) = output_column(&output, "joined") else {
            panic!("joined must be Utf8");
        };
        let TypedArray::Utf8(titled) = output_column(&output, "titled") else {
            panic!("titled must be Utf8");
        };
        let TypedArray::Utf8(lefted) = output_column(&output, "lefted") else {
            panic!("lefted must be Utf8");
        };
        let TypedArray::Utf8(lowered) = output_column(&output, "lowered") else {
            panic!("lowered must be Utf8");
        };
        let TypedArray::Utf8(lpaded) = output_column(&output, "lpaded") else {
            panic!("lpaded must be Utf8");
        };
        let TypedArray::Utf8(ltrimmed) = output_column(&output, "ltrimmed") else {
            panic!("ltrimmed must be Utf8");
        };
        let TypedArray::Utf8(digest) = output_column(&output, "digest") else {
            panic!("digest must be Utf8");
        };
        let TypedArray::Utf8(repeated) = output_column(&output, "repeated") else {
            panic!("repeated must be Utf8");
        };
        let TypedArray::Utf8(replaced) = output_column(&output, "replaced") else {
            panic!("replaced must be Utf8");
        };
        let TypedArray::Utf8(reversed) = output_column(&output, "reversed") else {
            panic!("reversed must be Utf8");
        };
        let TypedArray::Utf8(righted) = output_column(&output, "righted") else {
            panic!("righted must be Utf8");
        };
        let TypedArray::Utf8(rpaded) = output_column(&output, "rpaded") else {
            panic!("rpaded must be Utf8");
        };
        let TypedArray::Utf8(rtrimmed) = output_column(&output, "rtrimmed") else {
            panic!("rtrimmed must be Utf8");
        };
        let TypedArray::Utf8(part) = output_column(&output, "part") else {
            panic!("part must be Utf8");
        };
        let TypedArray::Boolean(starts) = output_column(&output, "starts") else {
            panic!("starts must be Boolean");
        };
        let TypedArray::Int64(pos) = output_column(&output, "pos") else {
            panic!("pos must be Int64");
        };
        let TypedArray::Utf8(piece) = output_column(&output, "piece") else {
            panic!("piece must be Utf8");
        };
        let TypedArray::Utf8(hexed) = output_column(&output, "hexed") else {
            panic!("hexed must be Utf8");
        };
        let TypedArray::Utf8(translated) = output_column(&output, "translated") else {
            panic!("translated must be Utf8");
        };
        let TypedArray::Utf8(trimmed2) = output_column(&output, "trimmed2") else {
            panic!("trimmed2 must be Utf8");
        };
        let TypedArray::Utf8(uppered) = output_column(&output, "uppered") else {
            panic!("uppered must be Utf8");
        };
        let TypedArray::Boolean(regex_ok) = output_column(&output, "regex_ok") else {
            panic!("regex_ok must be Boolean");
        };
        let TypedArray::Utf8(regex_replaced) = output_column(&output, "regex_replaced") else {
            panic!("regex_replaced must be Utf8");
        };
        let TypedArray::Utf8(regex_piece) = output_column(&output, "regex_piece") else {
            panic!("regex_piece must be Utf8");
        };

        assert_eq!(now_value.value(0), context_now.unix_nanos());
        assert_eq!(
            Uuid::parse_str(uuid4.value(0))
                .expect("uuid4 must parse")
                .get_version(),
            Some(Version::Random)
        );
        assert_eq!(
            Uuid::parse_str(uuid7.value(0))
                .expect("uuid7 must parse")
                .get_version(),
            Some(Version::SortRand)
        );
        assert_eq!(bits.value(0), 40);
        assert_eq!(ascii_value.value(0), 104);
        assert_eq!(trimmed.value(0), "hello.world");
        assert_eq!(chars.value(0), 15);
        assert_eq!(joined.value(0), "hexyhe");
        assert_eq!(titled.value(0), "  Hello.World  ");
        assert_eq!(lefted.value(0), "he");
        assert_eq!(lowered.value(0), "hello");
        assert_eq!(lpaded.value(0), "xyxyxhe");
        assert_eq!(ltrimmed.value(0), "hello.world  ");
        assert_eq!(digest.value(0), "6f96cfdfe5ccc627cadf24b41725caa4");
        assert_eq!(repeated.value(0), "hehe");
        assert_eq!(replaced.value(0), "XXllo");
        assert_eq!(reversed.value(0), "eh");
        assert_eq!(righted.value(0), "lo");
        assert_eq!(rpaded.value(0), "hexyxyx");
        assert_eq!(rtrimmed.value(0), "  hello.world");
        assert_eq!(part.value(0), "beta");
        assert!(starts.value(0));
        assert_eq!(pos.value(0), 1);
        assert_eq!(piece.value(0), "ell");
        assert_eq!(hexed.value(0), "ff");
        assert_eq!(translated.value(0), "HE");
        assert_eq!(trimmed2.value(0), "hello.world");
        assert_eq!(uppered.value(0), "HE");
        assert!(regex_ok.value(0));
        assert_eq!(regex_replaced.value(0), "XX");
        assert_eq!(regex_piece.value(0), "hello");
        assert!(output.errors().row(0).is_empty());
    }

    #[test]
    fn executes_extended_math_builtins() {
        let parsed = parse_program(
            "SET absolute = abs(input.int_value), acos_value = acos(input.half), asin_value = \
             asin(input.half), atan_value = atan(input.two), ceil_value = ceil(input.neg_float), \
             cos_value = cos(input.half), exp_value = exp(input.one), floor_value = \
             floor(input.neg_float), ln_value = ln(input.two), log_value = log(input.hundred), \
             log_base_value = log(input.two, input.hundred), pow_value = pow(input.two, \
             input.three), round_value = round(input.round_me), sqrt_value = sqrt(input.nine), \
             tan_value = tan(input.half)",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("int_value", DataType::Int64, true),
            Field::new("half", DataType::Float64, true),
            Field::new("two", DataType::Float64, true),
            Field::new("neg_float", DataType::Float64, true),
            Field::new("one", DataType::Float64, true),
            Field::new("hundred", DataType::Float64, true),
            Field::new("three", DataType::Float64, true),
            Field::new("round_me", DataType::Float64, true),
            Field::new("nine", DataType::Float64, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("absolute", DataType::Int64, true),
                Field::new("acos_value", DataType::Float64, true),
                Field::new("asin_value", DataType::Float64, true),
                Field::new("atan_value", DataType::Float64, true),
                Field::new("ceil_value", DataType::Float64, true),
                Field::new("cos_value", DataType::Float64, true),
                Field::new("exp_value", DataType::Float64, true),
                Field::new("floor_value", DataType::Float64, true),
                Field::new("ln_value", DataType::Float64, true),
                Field::new("log_value", DataType::Float64, true),
                Field::new("log_base_value", DataType::Float64, true),
                Field::new("pow_value", DataType::Float64, true),
                Field::new("round_value", DataType::Float64, true),
                Field::new("sqrt_value", DataType::Float64, true),
                Field::new("tan_value", DataType::Float64, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Int64(Int64Array::from(vec![Some(-7)])),
                TypedArray::Float64(Float64Array::from(vec![Some(0.5)])),
                TypedArray::Float64(Float64Array::from(vec![Some(2.0)])),
                TypedArray::Float64(Float64Array::from(vec![Some(-1.75)])),
                TypedArray::Float64(Float64Array::from(vec![Some(1.0)])),
                TypedArray::Float64(Float64Array::from(vec![Some(100.0)])),
                TypedArray::Float64(Float64Array::from(vec![Some(3.0)])),
                TypedArray::Float64(Float64Array::from(vec![Some(1.6)])),
                TypedArray::Float64(Float64Array::from(vec![Some(9.0)])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        let TypedArray::Int64(absolute) = output_column(&output, "absolute") else {
            panic!("absolute must be Int64");
        };
        let TypedArray::Float64(acos_value) = output_column(&output, "acos_value") else {
            panic!("acos_value must be Float64");
        };
        let TypedArray::Float64(asin_value) = output_column(&output, "asin_value") else {
            panic!("asin_value must be Float64");
        };
        let TypedArray::Float64(atan_value) = output_column(&output, "atan_value") else {
            panic!("atan_value must be Float64");
        };
        let TypedArray::Float64(ceil_value) = output_column(&output, "ceil_value") else {
            panic!("ceil_value must be Float64");
        };
        let TypedArray::Float64(cos_value) = output_column(&output, "cos_value") else {
            panic!("cos_value must be Float64");
        };
        let TypedArray::Float64(exp_value) = output_column(&output, "exp_value") else {
            panic!("exp_value must be Float64");
        };
        let TypedArray::Float64(floor_value) = output_column(&output, "floor_value") else {
            panic!("floor_value must be Float64");
        };
        let TypedArray::Float64(ln_value) = output_column(&output, "ln_value") else {
            panic!("ln_value must be Float64");
        };
        let TypedArray::Float64(log_value) = output_column(&output, "log_value") else {
            panic!("log_value must be Float64");
        };
        let TypedArray::Float64(log_base_value) = output_column(&output, "log_base_value") else {
            panic!("log_base_value must be Float64");
        };
        let TypedArray::Float64(pow_value) = output_column(&output, "pow_value") else {
            panic!("pow_value must be Float64");
        };
        let TypedArray::Float64(round_value) = output_column(&output, "round_value") else {
            panic!("round_value must be Float64");
        };
        let TypedArray::Float64(sqrt_value) = output_column(&output, "sqrt_value") else {
            panic!("sqrt_value must be Float64");
        };
        let TypedArray::Float64(tan_value) = output_column(&output, "tan_value") else {
            panic!("tan_value must be Float64");
        };

        assert_eq!(absolute.value(0), 7);
        assert!((acos_value.value(0) - 0.5f64.acos()).abs() < 1e-12);
        assert!((asin_value.value(0) - 0.5f64.asin()).abs() < 1e-12);
        assert!((atan_value.value(0) - 2.0f64.atan()).abs() < 1e-12);
        assert_eq!(ceil_value.value(0), -1.0);
        assert!((cos_value.value(0) - 0.5f64.cos()).abs() < 1e-12);
        assert!((exp_value.value(0) - 1.0f64.exp()).abs() < 1e-12);
        assert_eq!(floor_value.value(0), -2.0);
        assert!((ln_value.value(0) - 2.0f64.ln()).abs() < 1e-12);
        assert!((log_value.value(0) - 100.0f64.log10()).abs() < 1e-12);
        assert!((log_base_value.value(0) - 100.0f64.log(2.0)).abs() < 1e-12);
        assert_eq!(pow_value.value(0), 8.0);
        assert_eq!(round_value.value(0), 2.0);
        assert_eq!(sqrt_value.value(0), 3.0);
        assert!((tan_value.value(0) - 0.5f64.tan()).abs() < 1e-12);
        assert!(output.errors().row(0).is_empty());
    }

    #[test]
    fn executes_injected_header_reads_and_returns_selected_invocations_in_order() {
        let parsed = parse_program(
            "SET header_name = lower(input.header_name), route = read_header(input.header_name) \
             WHERE input.keep INVOKE write_header(\"route\", input.header_name), \
             write_header(\"route\", \"second\")",
        )
        .expect("program must parse");
        let input_schema = schema(vec![
            Field::new("header_name", DataType::Utf8, false),
            Field::new("route", DataType::Utf8, true),
            Field::new("keep", DataType::Boolean, false),
        ]);
        let compiled = compile_program_with_options_for_bindings(
            &parsed,
            input_schema.clone(),
            [CompileBinding::writable("input", input_schema.clone())],
            CompileOptions {
                allow_header_reads: true,
                allow_header_writes: true,
                ..CompileOptions::default()
            },
        )
        .expect("header program must compile");
        let batch = TypedBatch::try_new(
            input_schema,
            vec![
                TypedArray::Utf8(StringArray::from(vec!["ROUTE", "DROP"])),
                TypedArray::Utf8(StringArray::from(vec![None::<&str>, None])),
                TypedArray::Boolean(BooleanArray::from(vec![true, false])),
            ],
        )
        .expect("batch must build");

        let result = execute_program_in_context_sync(
            &compiled,
            &batch,
            &ExecutionContext {
                now: Timestamp::from_unix_nanos(1),
                injector: Some(triomphe::Arc::new(Box::new(TestHeaderInjector))),
            },
        )
        .expect("program must execute");

        assert_eq!(result.selected_rows, RowSelection::Selected(vec![0]));
        let TypedArray::Utf8(route) = output_column(&result.batch, "route") else {
            panic!("route must be Utf8");
        };
        assert_eq!(route.value(0), "primary");
        assert_eq!(result.invocations.len(), 2);
        let [TypedArray::Utf8(first_name), TypedArray::Utf8(first_value)] =
            result.invocations[0].arguments.as_slice()
        else {
            panic!("write_header arguments must be Utf8");
        };
        assert_eq!(first_name.value(0), "route");
        assert_eq!(first_value.value(0), "route");
        let [_, TypedArray::Utf8(second_value)] = result.invocations[1].arguments.as_slice() else {
            panic!("write_header arguments must be Utf8");
        };
        assert_eq!(second_value.value(0), "second");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_injector_policy_offloads_small_batches() {
        let parsed = parse_program("SET route = read_header(input.header_name)")
            .expect("program must parse");
        let input_schema = schema(vec![
            Field::new("header_name", DataType::Utf8, false),
            Field::new("route", DataType::Utf8, true),
        ]);
        let compiled = compile_program_with_options_for_bindings(
            &parsed,
            input_schema.clone(),
            [CompileBinding::writable("input", input_schema.clone())],
            CompileOptions {
                allow_header_reads: true,
                ..CompileOptions::default()
            },
        )
        .expect("header program must compile");
        let batch = TypedBatch::try_new(
            input_schema,
            vec![
                TypedArray::Utf8(StringArray::from(vec!["route"])),
                TypedArray::Utf8(StringArray::from(vec![None::<&str>])),
            ],
        )
        .expect("batch must build");
        let compiled = triomphe::Arc::new(compiled);
        let (release_tx, release_rx) = mpsc::channel();
        let context = ExecutionContext {
            now: Timestamp::from_unix_nanos(1),
            injector: Some(triomphe::Arc::new(Box::new(BlockingPolicyInjector {
                release: Mutex::new(release_rx),
            }))),
        };

        let (result, ()) = tokio::join!(
            execute_program_in_context(&compiled, &batch, &context),
            async move {
                tokio::task::yield_now().await;
                release_tx
                    .send(())
                    .expect("blocking injector must still be waiting");
            }
        );
        let result = result.expect("blocking injector execution must succeed");
        let TypedArray::Utf8(route) = output_column(&result.batch, "route") else {
            panic!("route must be Utf8");
        };
        assert_eq!(route.value(0), "primary");
    }

    #[test]
    fn rejects_mismatched_runtime_register_type() {
        let schema = StdArc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )]));
        let float_input = RegisterRef::new(RegisterSpace::Input, RegisterType::Float64, 0);
        let utf8_output = RegisterRef::new(RegisterSpace::Output, RegisterType::Utf8, 0);
        let program = CompiledProgram {
            input_schema: schema.clone(),
            output_schema: StdArc::new(Schema::new(vec![Field::new(
                "lowered",
                DataType::Utf8,
                true,
            )])),
            inputs: vec![InputBinding {
                column_index: 0,
                reg: float_input,
            }],
            instructions: vec![Instruction {
                kind: InstructionKind::Builtin {
                    dst: utf8_output,
                    lowering: BuiltinLowering::Lower,
                    inputs: vec![float_input],
                },
                span: (0..1).into(),
                error_mask: None,
            }],
            filter: None,
            invocations: Vec::new(),
            outputs: vec![OutputBinding {
                output_index: 0,
                name: "lowered".to_string(),
                reg: utf8_output,
            }],
            layouts: RegisterLayouts {
                inputs: RegisterLayout {
                    float64: 1,
                    ..RegisterLayout::default()
                },
                temps: RegisterLayout::default(),
                condition: RegisterLayout::default(),
                outputs: RegisterLayout {
                    utf8: 1,
                    ..RegisterLayout::default()
                },
            },
            injector: None,
        };
        let batch = TypedBatch::try_new(
            schema,
            vec![TypedArray::Float64(Float64Array::from(vec![Some(1.0)]))],
        )
        .expect("batch must build");

        let error = execute_program_sync(&program, &batch).expect_err("execution must fail");

        match error {
            RuntimeError::InvalidBatch { message } => {
                assert!(message.contains("Utf8 input"));
                assert!(message.contains("Float64"));
            }
            other => panic!("expected invalid register type, got {other:?}"),
        }
    }

    #[test]
    fn rejects_binary_instruction_written_to_wrong_destination_type() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("left", DataType::Int64, true),
            Field::new("right", DataType::Int64, true),
        ]));
        let left = RegisterRef::new(RegisterSpace::Input, RegisterType::Int64, 0);
        let right = RegisterRef::new(RegisterSpace::Input, RegisterType::Int64, 1);
        let utf8_output = RegisterRef::new(RegisterSpace::Output, RegisterType::Utf8, 0);
        let program = CompiledProgram {
            input_schema: schema.clone(),
            output_schema: StdArc::new(Schema::new(vec![Field::new("bad", DataType::Utf8, true)])),
            inputs: vec![
                InputBinding {
                    column_index: 0,
                    reg: left,
                },
                InputBinding {
                    column_index: 1,
                    reg: right,
                },
            ],
            instructions: vec![Instruction {
                kind: InstructionKind::Binary {
                    dst: utf8_output,
                    left,
                    right,
                    op: BinaryOp::Add,
                },
                span: (0..1).into(),
                error_mask: None,
            }],
            filter: None,
            invocations: Vec::new(),
            outputs: vec![OutputBinding {
                output_index: 0,
                name: "bad".to_string(),
                reg: utf8_output,
            }],
            layouts: RegisterLayouts {
                inputs: RegisterLayout {
                    int64: 2,
                    ..RegisterLayout::default()
                },
                temps: RegisterLayout::default(),
                condition: RegisterLayout::default(),
                outputs: RegisterLayout {
                    utf8: 1,
                    ..RegisterLayout::default()
                },
            },
            injector: None,
        };
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Int64(Int64Array::from(vec![Some(1)])),
                TypedArray::Int64(Int64Array::from(vec![Some(2)])),
            ],
        )
        .expect("batch must build");

        let error = execute_program_sync(&program, &batch).expect_err("execution must fail");

        match error {
            RuntimeError::InvalidRegisterType { reg, expected } => {
                assert_eq!(reg, utf8_output);
                assert_eq!(expected, "Int64Array");
            }
            other => panic!("expected invalid register type, got {other:?}"),
        }
    }

    #[test]
    fn literals_are_scalars_that_expand_only_when_read_as_a_column() {
        let mut layouts = RegisterLayouts::default();
        let literal = layouts.alloc(RegisterSpace::Temp, RegisterType::Utf8);
        let copy = layouts.alloc(RegisterSpace::Temp, RegisterType::Utf8);
        let mut registers = RegisterBank::new(&layouts, 4);

        write_literal(
            &mut registers,
            literal,
            &ScalarValue::Utf8("shared".to_string()),
        )
        .expect("literal must write");

        assert!(registers.is_scalar(literal));
        let operand = registers
            .operand::<StringArray>(literal)
            .expect("register must read");
        assert!(matches!(operand, Operand::Scalar(array) if array.len() == 1));
        let register = registers
            .register::<StringArray>(literal)
            .expect("register must read");
        assert!(
            matches!(register, Register::Scalar { column, .. } if column.get().is_none()),
            "no column is built until one is read"
        );

        registers.copy(copy, literal).expect("copy must write");
        assert!(registers.is_scalar(copy), "a copy keeps the scalar");

        let column = registers
            .column::<StringArray>(literal)
            .expect("register must read");
        assert_eq!(column.len(), 4);
        assert!(column.iter().all(|value| value == Some("shared")));
        let register = registers
            .register::<StringArray>(literal)
            .expect("register must read");
        assert!(
            matches!(register, Register::Scalar { column, .. } if column.get().is_some()),
            "the column is kept once built"
        );
        let output = registers.output_array(copy).expect("output must read");
        assert_eq!(output.len(), 4);
    }

    fn utc_instants(seconds: &[Option<i64>]) -> TimestampNanosecondArray {
        TimestampNanosecondArray::from_iter(
            seconds
                .iter()
                .map(|value| value.map(|seconds| seconds * 1_000_000_000)),
        )
        .with_timezone_utc()
    }

    /// The outputs the scalar-operand and column-operand programs below both write.
    fn operand_agreement_outputs() -> Vec<Field> {
        vec![
            Field::new("i64_sum", DataType::Int64, true),
            Field::new("i64_greater", DataType::Boolean, true),
            Field::new("i32_sum", DataType::Int32, true),
            Field::new("i32_less", DataType::Boolean, true),
            Field::new("f64_product", DataType::Float64, true),
            Field::new("f64_equal", DataType::Boolean, true),
            Field::new("f32_sum", DataType::Float32, true),
            Field::new("text_equal", DataType::Boolean, true),
            Field::new("flag_equal", DataType::Boolean, true),
            Field::new("at_later", DataType::Boolean, true),
            Field::new("null_sum", DataType::Int64, true),
            Field::new("padded", DataType::Utf8, true),
            Field::new("tagged", DataType::Utf8, true),
            Field::new("has_b", DataType::Boolean, true),
            Field::new("fallback", DataType::Utf8, true),
            Field::new("split", DataType::Utf8, true),
            Field::new("replaced", DataType::Utf8, true),
            Field::new("position", DataType::Int64, true),
            Field::new("translated", DataType::Utf8, true),
            Field::new("chosen", DataType::Utf8, true),
            Field::new("nulled", DataType::Int64, true),
        ]
    }

    #[test]
    fn scalar_operands_agree_with_column_operands_across_types_and_nulls() {
        // The literal program pairs every operand with a literal, a null constant, a cast
        // literal or a constant call; the column program carries the same value in a column.
        let literal_program = parse_program(
            "SET i64_sum = input.i64 + 3, i64_greater = input.i64 > 3, i32_sum = input.i32 + (3 \
             AS I32), i32_less = input.i32 < (3 AS I32), f64_product = input.f64 * 1.5, f64_equal \
             = input.f64 = 1.5, f32_sum = input.f32 + (1.5 AS F32), text_equal = input.text = \
             'b', flag_equal = input.flag = true, at_later = input.at > from_unix('second', 100), \
             null_sum = input.i64 + nullif(3, 3), padded = lpad(input.text, 3, '*'), tagged = \
             concat(input.text, '-', 'x'), has_b = contains(input.text, 'b'), fallback = \
             coalesce(input.text, 'none'), split = split_part(input.text, 'b', 1), replaced = \
             replace(input.text, 'b', 'c'), position = strpos(input.text, 'b'), translated = \
             translate(input.text, 'ab', 'xy'), chosen = CASE WHEN input.flag THEN 'yes' ELSE \
             'no' END, nulled = nullif(input.i64, 3)",
        )
        .expect("must parse");
        let column_program = parse_program(
            "SET i64_sum = input.i64 + input.three, i64_greater = input.i64 > input.three, \
             i32_sum = input.i32 + input.three_i32, i32_less = input.i32 < input.three_i32, \
             f64_product = input.f64 * input.one_and_half, f64_equal = input.f64 = \
             input.one_and_half, f32_sum = input.f32 + input.one_and_half_f32, text_equal = \
             input.text = input.b, flag_equal = input.flag = input.yes_flag, at_later = input.at \
             > input.hundred_at, null_sum = input.i64 + input.null_i64, padded = lpad(input.text, \
             input.three, input.star), tagged = concat(input.text, input.dash, input.x), has_b = \
             contains(input.text, input.b), fallback = coalesce(input.text, input.none), split = \
             split_part(input.text, input.b, input.one), replaced = replace(input.text, input.b, \
             input.c), position = strpos(input.text, input.b), translated = translate(input.text, \
             input.ab, input.xy), chosen = CASE WHEN input.flag THEN input.yes ELSE input.no END, \
             nulled = nullif(input.i64, input.three)",
        )
        .expect("must parse");
        let base_fields = vec![
            Field::new("i64", DataType::Int64, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                true,
            ),
        ];
        let base_columns = vec![
            TypedArray::Int64(Int64Array::from(vec![Some(1), None, Some(3)])),
            TypedArray::Int32(Int32Array::from(vec![Some(1), None, Some(3)])),
            TypedArray::Float64(Float64Array::from(vec![Some(2.0), None, Some(1.5)])),
            TypedArray::Float32(Float32Array::from(vec![Some(2.0), None, Some(1.5)])),
            TypedArray::Utf8(StringArray::from(vec![Some("abc"), None, Some("b")])),
            TypedArray::Boolean(BooleanArray::from(vec![Some(true), None, Some(false)])),
            TypedArray::Datetime(utc_instants(&[Some(50), None, Some(150)])),
        ];
        let literal_schema = schema(base_fields.clone());
        let literal_compiled = compile_program_with_output_fields(
            &literal_program,
            literal_schema.clone(),
            operand_agreement_outputs(),
        );
        let literal_batch =
            TypedBatch::try_new(literal_schema, base_columns.clone()).expect("batch must build");

        let mut column_fields = base_fields;
        let mut column_columns = base_columns;
        let constant_text = |value: &str| {
            TypedArray::Utf8(StringArray::from(vec![
                Some(value),
                Some(value),
                Some(value),
            ]))
        };
        for (name, column) in [
            ("three", TypedArray::Int64(Int64Array::from_value(3, 3))),
            ("three_i32", TypedArray::Int32(Int32Array::from_value(3, 3))),
            ("one", TypedArray::Int64(Int64Array::from_value(1, 3))),
            (
                "one_and_half",
                TypedArray::Float64(Float64Array::from_value(1.5, 3)),
            ),
            (
                "one_and_half_f32",
                TypedArray::Float32(Float32Array::from_value(1.5, 3)),
            ),
            (
                "yes_flag",
                TypedArray::Boolean(BooleanArray::from(vec![true, true, true])),
            ),
            (
                "hundred_at",
                TypedArray::Datetime(utc_instants(&[Some(100), Some(100), Some(100)])),
            ),
            ("null_i64", TypedArray::Int64(Int64Array::new_null(3))),
            ("b", constant_text("b")),
            ("star", constant_text("*")),
            ("dash", constant_text("-")),
            ("x", constant_text("x")),
            ("none", constant_text("none")),
            ("c", constant_text("c")),
            ("ab", constant_text("ab")),
            ("xy", constant_text("xy")),
            ("yes", constant_text("yes")),
            ("no", constant_text("no")),
        ] {
            column_fields.push(Field::new(name, column.data_type(), true));
            column_columns.push(column);
        }
        let column_schema = schema(column_fields);
        let column_compiled = compile_program_with_output_fields(
            &column_program,
            column_schema.clone(),
            operand_agreement_outputs(),
        );
        let column_batch =
            TypedBatch::try_new(column_schema, column_columns).expect("batch must build");

        let literal_output = execute_program_sync(&literal_compiled, &literal_batch)
            .expect("literal program must execute");
        let column_output = execute_program_sync(&column_compiled, &column_batch)
            .expect("column program must execute");

        assert!(literal_output.errors().is_error_free());
        assert!(column_output.errors().is_error_free());
        for field in operand_agreement_outputs() {
            assert_eq!(
                output_column(&literal_output, field.name()),
                output_column(&column_output, field.name()),
                "output '{}' must not depend on whether its operand is a literal",
                field.name()
            );
        }
        assert_eq!(
            output_column(&literal_output, "i64_sum"),
            &TypedArray::Int64(Int64Array::from(vec![Some(4), None, Some(6)]))
        );
        assert_eq!(
            output_column(&literal_output, "null_sum"),
            &TypedArray::Int64(Int64Array::new_null(3))
        );
        assert_eq!(
            output_column(&literal_output, "at_later"),
            &TypedArray::Boolean(BooleanArray::from(vec![Some(false), None, Some(true)]))
        );
        assert_eq!(
            output_column(&literal_output, "tagged"),
            &TypedArray::Utf8(StringArray::from(vec![
                Some("abc-x"),
                Some("-x"),
                Some("b-x")
            ]))
        );
        assert_eq!(
            output_column(&literal_output, "fallback"),
            &TypedArray::Utf8(StringArray::from(vec![
                Some("abc"),
                Some("none"),
                Some("b")
            ]))
        );
        assert_eq!(
            output_column(&literal_output, "chosen"),
            &TypedArray::Utf8(StringArray::from(vec![Some("yes"), Some("no"), Some("no")]))
        );
        assert_eq!(
            output_column(&literal_output, "nulled"),
            &TypedArray::Int64(Int64Array::from(vec![Some(1), None, None]))
        );
    }

    #[test]
    fn now_is_one_value_per_execution_and_uuids_are_one_per_row() {
        let parsed =
            parse_program("SET at = now(), id4 = uuid_v4(), id7 = uuid_v7()").expect("must parse");
        let schema = schema(vec![Field::new("row", DataType::Int64, true)]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new(
                    "at",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                    true,
                ),
                Field::new("id4", DataType::Utf8, true),
                Field::new("id7", DataType::Utf8, true),
            ],
        );
        let rows = 6;
        let batch = TypedBatch::try_new(
            schema,
            vec![TypedArray::Int64(Int64Array::from_iter_values(0..rows))],
        )
        .expect("batch must build");
        let instant = Timestamp::from_unix_nanos(1_700_000_000_000_000_000);
        let context = ExecutionContext::new(instant);

        let output = execute_program_in_context_sync(&compiled, &batch, &context)
            .expect("execution must succeed")
            .batch;

        let TypedArray::Datetime(at) = output_column(&output, "at") else {
            panic!("at must be Datetime");
        };
        assert_eq!(at.len(), 6);
        assert!(
            at.values()
                .iter()
                .all(|value| *value == instant.unix_nanos())
        );
        for (name, version) in [("id4", Version::Random), ("id7", Version::SortRand)] {
            let TypedArray::Utf8(ids) = output_column(&output, name) else {
                panic!("{name} must be Utf8");
            };
            let mut distinct = std::collections::BTreeSet::new();
            for id in ids.iter() {
                let id = id.expect("every row receives an identifier");
                let parsed = Uuid::parse_str(id).expect("identifier must be a UUID");
                assert_eq!(parsed.get_version(), Some(version));
                distinct.insert(id.to_string());
            }
            assert_eq!(distinct.len(), 6, "{name} must differ on every row");
        }
    }

    #[test]
    fn a_shared_value_that_fails_reports_the_failure_on_every_row_its_arm_selects() {
        let parsed =
            parse_program("SET ratio = CASE WHEN input.flag THEN 1 / 0 ELSE 1 END, always = 1 / 0")
                .expect("must parse");
        let schema = schema(vec![Field::new("flag", DataType::Boolean, true)]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("ratio", DataType::Int64, true),
                Field::new("always", DataType::Int64, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![TypedArray::Boolean(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
            ]))],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        assert_eq!(
            output_column(&output, "ratio"),
            &TypedArray::Int64(Int64Array::from(vec![None, Some(1), None, Some(1)]))
        );
        assert_eq!(
            output_column(&output, "always"),
            &TypedArray::Int64(Int64Array::new_null(4))
        );
        let error_counts = output
            .errors()
            .iter()
            .map(<[SideError]>::len)
            .collect::<Vec<_>>();
        assert_eq!(error_counts, [2, 1, 2, 1]);
        for error in output.errors().iter().flatten() {
            assert_eq!(error.code(), ErrorCode::DivisionByZero);
        }
    }

    fn dynamic_pattern_caches(compiled: &CompiledProgram) -> Vec<&crate::regexp::DynamicPatterns> {
        let mut caches = Vec::new();
        for instruction in &compiled.instructions {
            if let InstructionKind::Builtin {
                lowering:
                    BuiltinLowering::Regexp(RegexpCall {
                        pattern: PatternSource::Argument(cache),
                        ..
                    }),
                ..
            } = &instruction.kind
            {
                caches.push(cache);
            }
        }
        caches
    }

    #[test]
    fn constant_patterns_are_shared_across_batches_and_argument_patterns_are_cached() {
        let parsed = parse_program(
            "SET matched = regexp_like(input.text, 'a+'), piece = regexp_substr(input.text, \
             input.pattern), rewritten = regexp_replace(input.text, input.pattern, '<$0>')",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("pattern", DataType::Utf8, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("matched", DataType::Boolean, true),
                Field::new("piece", DataType::Utf8, true),
                Field::new("rewritten", DataType::Utf8, true),
            ],
        );
        let caches = dynamic_pattern_caches(&compiled);
        assert_eq!(caches.len(), 2, "two calls read their pattern argument");

        let first = TypedBatch::try_new(
            schema.clone(),
            vec![
                TypedArray::Utf8(StringArray::from(vec![Some("aa"), Some("b"), None])),
                TypedArray::Utf8(StringArray::from(vec![Some("a+"), Some("b"), Some("a+")])),
            ],
        )
        .expect("batch must build");
        let second = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Utf8(StringArray::from(vec![Some("bb"), Some("a")])),
                TypedArray::Utf8(StringArray::from(vec![Some("b"), Some("a+")])),
            ],
        )
        .expect("batch must build");

        let first_output = execute_program_sync(&compiled, &first).expect("execution must succeed");
        let second_output =
            execute_program_sync(&compiled, &second).expect("execution must succeed");

        assert_eq!(
            output_column(&first_output, "matched"),
            &TypedArray::Boolean(BooleanArray::from(vec![Some(true), Some(false), None]))
        );
        assert_eq!(
            output_column(&first_output, "piece"),
            &TypedArray::Utf8(StringArray::from(vec![Some("aa"), Some("b"), None]))
        );
        assert_eq!(
            output_column(&first_output, "rewritten"),
            &TypedArray::Utf8(StringArray::from(vec![Some("<aa>"), Some("<b>"), None]))
        );
        assert_eq!(
            output_column(&second_output, "matched"),
            &TypedArray::Boolean(BooleanArray::from(vec![Some(false), Some(true)]))
        );
        assert_eq!(
            output_column(&second_output, "piece"),
            &TypedArray::Utf8(StringArray::from(vec![Some("b"), Some("a")]))
        );
        assert_eq!(
            output_column(&second_output, "rewritten"),
            &TypedArray::Utf8(StringArray::from(vec![Some("<b><b>"), Some("<a>")]))
        );
        for cache in caches {
            let statistics = cache.statistics();
            assert_eq!(
                statistics.compiled, 2,
                "each distinct pattern is compiled once for every batch that uses it"
            );
            assert_eq!(statistics.cached, 2);
            assert_eq!(statistics.evicted, 0);
        }
    }

    #[test]
    fn invalid_constant_patterns_report_per_row_only_where_the_arm_is_selected() {
        let parsed = parse_program(
            "SET guarded = CASE WHEN input.flag THEN regexp_like(input.text, '(') ELSE false END, \
             always = regexp_like(input.text, '(')",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
        ]);
        let compiled = compile_program_with_output_fields(
            &parsed,
            schema.clone(),
            vec![
                Field::new("guarded", DataType::Boolean, true),
                Field::new("always", DataType::Boolean, true),
            ],
        );
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Utf8(StringArray::from(vec![Some("a"), None, Some("b")])),
                TypedArray::Boolean(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                ])),
            ],
        )
        .expect("batch must build");

        let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

        assert_eq!(
            output_column(&output, "guarded"),
            &TypedArray::Boolean(BooleanArray::from(vec![None, Some(false), None]))
        );
        assert_eq!(
            output_column(&output, "always"),
            &TypedArray::Boolean(BooleanArray::new_null(3))
        );
        let error_counts = output
            .errors()
            .iter()
            .map(<[SideError]>::len)
            .collect::<Vec<_>>();
        assert_eq!(error_counts, [2, 0, 2], "a null text reports nothing");
        for error in output.errors().iter().flatten() {
            assert_eq!(error.code(), ErrorCode::InvalidArgument);
            assert!(
                error
                    .reason
                    .to_string()
                    .starts_with("invalid regular expression:"),
                "{}",
                error.reason
            );
        }
    }
}

#[cfg(test)]
#[path = "runtime_datetime_tests.rs"]
mod datetime_tests;
#[cfg(test)]
#[path = "runtime_numeric_tests.rs"]
mod numeric_tests;
