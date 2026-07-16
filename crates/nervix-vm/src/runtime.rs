use ahash::{HashMap, HashMapExt};
use arrow_arith::{
    aggregate::sum as arrow_sum,
    boolean::{and_kleene, is_null, not, or_kleene},
    numeric::{add, div, mul, neg, rem, sub},
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Datum, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    builder::{
        BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder,
        Int64Builder, StringBuilder, TimestampNanosecondBuilder, UInt8Builder, UInt16Builder,
        UInt32Builder, UInt64Builder,
    },
    new_null_array,
};
use arrow_cast::cast::{CastOptions, cast_with_options};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{ArrowError, DataType};
use arrow_select::{filter::FilterBuilder, nullif::nullif, zip::zip};
use arrow_string::like::{
    contains as string_contains, ends_with as string_ends_with, starts_with as string_starts_with,
};
use chrono::DateTime;
use nervix_models::Timestamp;
use nervix_nspl::vm_program::{BinaryOp, Span, UnaryOp};
use regex::Regex;
use tokio::task;
use uuid::{NoContext, Timestamp as UuidTimestamp, Uuid};

use crate::{
    batch::{TypedArray, TypedBatch},
    error::{ErrorCode, RuntimeError, SideError},
    ir::{
        CompiledProgram, InputBinding, Instruction, InstructionKind, RegisterLayout,
        RegisterLayouts, RegisterRef, RegisterSpace, RegisterType, ScalarValue,
    },
    semantics::BuiltinLowering,
};

pub const SPAWN_BLOCKING_ROW_THRESHOLD: usize = 1_024;

struct TypedBank {
    uint8: Vec<Option<UInt8Array>>,
    int8: Vec<Option<Int8Array>>,
    uint16: Vec<Option<UInt16Array>>,
    int16: Vec<Option<Int16Array>>,
    uint32: Vec<Option<UInt32Array>>,
    int32: Vec<Option<Int32Array>>,
    uint64: Vec<Option<UInt64Array>>,
    int64: Vec<Option<Int64Array>>,
    float32: Vec<Option<Float32Array>>,
    float64: Vec<Option<Float64Array>>,
    boolean: Vec<Option<BooleanArray>>,
    utf8: Vec<Option<StringArray>>,
    datetime: Vec<Option<TimestampNanosecondArray>>,
    generic: Vec<Option<ArrayRef>>,
}

impl TypedBank {
    fn new(layout: &RegisterLayout) -> Self {
        Self {
            uint8: vec![None; layout.uint8],
            int8: vec![None; layout.int8],
            uint16: vec![None; layout.uint16],
            int16: vec![None; layout.int16],
            uint32: vec![None; layout.uint32],
            int32: vec![None; layout.int32],
            uint64: vec![None; layout.uint64],
            int64: vec![None; layout.int64],
            float32: vec![None; layout.float32],
            float64: vec![None; layout.float64],
            boolean: vec![None; layout.boolean],
            utf8: vec![None; layout.utf8],
            datetime: vec![None; layout.datetime],
            generic: vec![None; layout.generic],
        }
    }

    fn set_uint8(&mut self, index: usize, value: UInt8Array) -> Result<(), ()> {
        let Some(slot) = self.uint8.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_int8(&mut self, index: usize, value: Int8Array) -> Result<(), ()> {
        let Some(slot) = self.int8.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_uint16(&mut self, index: usize, value: UInt16Array) -> Result<(), ()> {
        let Some(slot) = self.uint16.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_int16(&mut self, index: usize, value: Int16Array) -> Result<(), ()> {
        let Some(slot) = self.int16.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_uint32(&mut self, index: usize, value: UInt32Array) -> Result<(), ()> {
        let Some(slot) = self.uint32.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_int32(&mut self, index: usize, value: Int32Array) -> Result<(), ()> {
        let Some(slot) = self.int32.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_uint64(&mut self, index: usize, value: UInt64Array) -> Result<(), ()> {
        let Some(slot) = self.uint64.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_int64(&mut self, index: usize, value: Int64Array) -> Result<(), ()> {
        let Some(slot) = self.int64.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_float32(&mut self, index: usize, value: Float32Array) -> Result<(), ()> {
        let Some(slot) = self.float32.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_float64(&mut self, index: usize, value: Float64Array) -> Result<(), ()> {
        let Some(slot) = self.float64.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_boolean(&mut self, index: usize, value: BooleanArray) -> Result<(), ()> {
        let Some(slot) = self.boolean.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_utf8(&mut self, index: usize, value: StringArray) -> Result<(), ()> {
        let Some(slot) = self.utf8.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_datetime(&mut self, index: usize, value: TimestampNanosecondArray) -> Result<(), ()> {
        let Some(slot) = self.datetime.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn set_generic(&mut self, index: usize, value: ArrayRef) -> Result<(), ()> {
        let Some(slot) = self.generic.get_mut(index) else {
            return Err(());
        };
        *slot = Some(value);
        Ok(())
    }

    fn uint8(&self, index: usize) -> Option<&UInt8Array> {
        self.uint8.get(index).and_then(Option::as_ref)
    }

    fn int8(&self, index: usize) -> Option<&Int8Array> {
        self.int8.get(index).and_then(Option::as_ref)
    }

    fn uint16(&self, index: usize) -> Option<&UInt16Array> {
        self.uint16.get(index).and_then(Option::as_ref)
    }

    fn int16(&self, index: usize) -> Option<&Int16Array> {
        self.int16.get(index).and_then(Option::as_ref)
    }

    fn uint32(&self, index: usize) -> Option<&UInt32Array> {
        self.uint32.get(index).and_then(Option::as_ref)
    }

    fn int32(&self, index: usize) -> Option<&Int32Array> {
        self.int32.get(index).and_then(Option::as_ref)
    }

    fn uint64(&self, index: usize) -> Option<&UInt64Array> {
        self.uint64.get(index).and_then(Option::as_ref)
    }

    fn int64(&self, index: usize) -> Option<&Int64Array> {
        self.int64.get(index).and_then(Option::as_ref)
    }

    fn float32(&self, index: usize) -> Option<&Float32Array> {
        self.float32.get(index).and_then(Option::as_ref)
    }

    fn float64(&self, index: usize) -> Option<&Float64Array> {
        self.float64.get(index).and_then(Option::as_ref)
    }

    fn boolean(&self, index: usize) -> Option<&BooleanArray> {
        self.boolean.get(index).and_then(Option::as_ref)
    }

    fn utf8(&self, index: usize) -> Option<&StringArray> {
        self.utf8.get(index).and_then(Option::as_ref)
    }

    fn datetime(&self, index: usize) -> Option<&TimestampNanosecondArray> {
        self.datetime.get(index).and_then(Option::as_ref)
    }

    fn generic(&self, index: usize) -> Option<&ArrayRef> {
        self.generic.get(index).and_then(Option::as_ref)
    }
}

struct RegisterBank {
    inputs: TypedBank,
    temps: TypedBank,
    condition: TypedBank,
    outputs: TypedBank,
    uninitialized: HashMap<RegisterRef, DataType>,
}

impl RegisterBank {
    fn new(layouts: &RegisterLayouts) -> Self {
        Self {
            inputs: TypedBank::new(&layouts.inputs),
            temps: TypedBank::new(&layouts.temps),
            condition: TypedBank::new(&layouts.condition),
            outputs: TypedBank::new(&layouts.outputs),
            uninitialized: HashMap::new(),
        }
    }

    fn load_input_batch(
        &mut self,
        inputs: &[InputBinding],
        batch: &TypedBatch,
    ) -> Result<(), RuntimeError> {
        for input in inputs {
            self.set_array(input.reg, batch.column(input.column_index).clone())?;
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

    fn set_array(&mut self, reg: RegisterRef, value: TypedArray) -> Result<(), RuntimeError> {
        self.uninitialized.remove(&reg);
        match value {
            TypedArray::UInt8(array) => self.set_uint8(reg, array),
            TypedArray::Int8(array) => self.set_int8(reg, array),
            TypedArray::UInt16(array) => self.set_uint16(reg, array),
            TypedArray::Int16(array) => self.set_int16(reg, array),
            TypedArray::UInt32(array) => self.set_uint32(reg, array),
            TypedArray::Int32(array) => self.set_int32(reg, array),
            TypedArray::UInt64(array) => self.set_uint64(reg, array),
            TypedArray::Int64(array) => self.set_int64(reg, array),
            TypedArray::Float32(array) => self.set_float32(reg, array),
            TypedArray::Float64(array) => self.set_float64(reg, array),
            TypedArray::Boolean(array) => self.set_boolean(reg, array),
            TypedArray::Utf8(array) => self.set_utf8(reg, array),
            TypedArray::Datetime(array) => self.set_datetime(reg, array),
            TypedArray::Generic(array) => self.set_generic(reg, array),
            TypedArray::Uninitialized { data_type, len } => {
                let materialized = array_ref_to_typed_array(new_null_array(&data_type, len))?;
                self.set_array(reg, materialized)?;
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

    fn read_array(&self, reg: RegisterRef) -> Result<TypedArray, RuntimeError> {
        match reg.ty {
            RegisterType::UInt8 => Ok(TypedArray::UInt8(self.uint8(reg)?.clone())),
            RegisterType::Int8 => Ok(TypedArray::Int8(self.int8(reg)?.clone())),
            RegisterType::UInt16 => Ok(TypedArray::UInt16(self.uint16(reg)?.clone())),
            RegisterType::Int16 => Ok(TypedArray::Int16(self.int16(reg)?.clone())),
            RegisterType::UInt32 => Ok(TypedArray::UInt32(self.uint32(reg)?.clone())),
            RegisterType::Int32 => Ok(TypedArray::Int32(self.int32(reg)?.clone())),
            RegisterType::UInt64 => Ok(TypedArray::UInt64(self.uint64(reg)?.clone())),
            RegisterType::Int64 => Ok(TypedArray::Int64(self.int64(reg)?.clone())),
            RegisterType::Float32 => Ok(TypedArray::Float32(self.float32(reg)?.clone())),
            RegisterType::Float64 => Ok(TypedArray::Float64(self.float64(reg)?.clone())),
            RegisterType::Boolean => Ok(TypedArray::Boolean(self.boolean(reg)?.clone())),
            RegisterType::Utf8 => Ok(TypedArray::Utf8(self.utf8(reg)?.clone())),
            RegisterType::Datetime => Ok(TypedArray::Datetime(self.datetime(reg)?.clone())),
            RegisterType::Generic => Ok(TypedArray::Generic(self.generic(reg)?.clone())),
        }
    }

    fn set_uint8(&mut self, reg: RegisterRef, value: UInt8Array) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::UInt8, "UInt8Array")?;
        self.bank_mut(reg.space)
            .set_uint8(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_int8(&mut self, reg: RegisterRef, value: Int8Array) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::Int8, "Int8Array")?;
        self.bank_mut(reg.space)
            .set_int8(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_uint16(&mut self, reg: RegisterRef, value: UInt16Array) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::UInt16, "UInt16Array")?;
        self.bank_mut(reg.space)
            .set_uint16(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_int16(&mut self, reg: RegisterRef, value: Int16Array) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::Int16, "Int16Array")?;
        self.bank_mut(reg.space)
            .set_int16(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_uint32(&mut self, reg: RegisterRef, value: UInt32Array) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::UInt32, "UInt32Array")?;
        self.bank_mut(reg.space)
            .set_uint32(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_int32(&mut self, reg: RegisterRef, value: Int32Array) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::Int32, "Int32Array")?;
        self.bank_mut(reg.space)
            .set_int32(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_uint64(&mut self, reg: RegisterRef, value: UInt64Array) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::UInt64, "UInt64Array")?;
        self.bank_mut(reg.space)
            .set_uint64(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_int64(&mut self, reg: RegisterRef, value: Int64Array) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::Int64, "Int64Array")?;
        self.bank_mut(reg.space)
            .set_int64(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_float32(&mut self, reg: RegisterRef, value: Float32Array) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::Float32, "Float32Array")?;
        self.bank_mut(reg.space)
            .set_float32(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_float64(&mut self, reg: RegisterRef, value: Float64Array) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::Float64, "Float64Array")?;
        self.bank_mut(reg.space)
            .set_float64(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_boolean(&mut self, reg: RegisterRef, value: BooleanArray) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::Boolean, "BooleanArray")?;
        self.bank_mut(reg.space)
            .set_boolean(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_utf8(&mut self, reg: RegisterRef, value: StringArray) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::Utf8, "StringArray")?;
        self.bank_mut(reg.space)
            .set_utf8(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_datetime(
        &mut self,
        reg: RegisterRef,
        value: TimestampNanosecondArray,
    ) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::Datetime, "TimestampNanosecondArray")?;
        self.bank_mut(reg.space)
            .set_datetime(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn set_generic(&mut self, reg: RegisterRef, value: ArrayRef) -> Result<(), RuntimeError> {
        self.ensure_type(reg, RegisterType::Generic, "ArrayRef")?;
        self.bank_mut(reg.space)
            .set_generic(reg.index, value)
            .map_err(|()| RuntimeError::MissingRegister { reg })
    }

    fn uint8(&self, reg: RegisterRef) -> Result<&UInt8Array, RuntimeError> {
        self.ensure_type(reg, RegisterType::UInt8, "UInt8Array")?;
        self.bank(reg.space)
            .uint8(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn int8(&self, reg: RegisterRef) -> Result<&Int8Array, RuntimeError> {
        self.ensure_type(reg, RegisterType::Int8, "Int8Array")?;
        self.bank(reg.space)
            .int8(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn uint16(&self, reg: RegisterRef) -> Result<&UInt16Array, RuntimeError> {
        self.ensure_type(reg, RegisterType::UInt16, "UInt16Array")?;
        self.bank(reg.space)
            .uint16(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn int16(&self, reg: RegisterRef) -> Result<&Int16Array, RuntimeError> {
        self.ensure_type(reg, RegisterType::Int16, "Int16Array")?;
        self.bank(reg.space)
            .int16(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn uint32(&self, reg: RegisterRef) -> Result<&UInt32Array, RuntimeError> {
        self.ensure_type(reg, RegisterType::UInt32, "UInt32Array")?;
        self.bank(reg.space)
            .uint32(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn int32(&self, reg: RegisterRef) -> Result<&Int32Array, RuntimeError> {
        self.ensure_type(reg, RegisterType::Int32, "Int32Array")?;
        self.bank(reg.space)
            .int32(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn uint64(&self, reg: RegisterRef) -> Result<&UInt64Array, RuntimeError> {
        self.ensure_type(reg, RegisterType::UInt64, "UInt64Array")?;
        self.bank(reg.space)
            .uint64(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn int64(&self, reg: RegisterRef) -> Result<&Int64Array, RuntimeError> {
        self.ensure_type(reg, RegisterType::Int64, "Int64Array")?;
        self.bank(reg.space)
            .int64(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn float32(&self, reg: RegisterRef) -> Result<&Float32Array, RuntimeError> {
        self.ensure_type(reg, RegisterType::Float32, "Float32Array")?;
        self.bank(reg.space)
            .float32(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn float64(&self, reg: RegisterRef) -> Result<&Float64Array, RuntimeError> {
        self.ensure_type(reg, RegisterType::Float64, "Float64Array")?;
        self.bank(reg.space)
            .float64(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn boolean(&self, reg: RegisterRef) -> Result<&BooleanArray, RuntimeError> {
        self.ensure_type(reg, RegisterType::Boolean, "BooleanArray")?;
        self.bank(reg.space)
            .boolean(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn utf8(&self, reg: RegisterRef) -> Result<&StringArray, RuntimeError> {
        self.ensure_type(reg, RegisterType::Utf8, "StringArray")?;
        self.bank(reg.space)
            .utf8(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn datetime(&self, reg: RegisterRef) -> Result<&TimestampNanosecondArray, RuntimeError> {
        self.ensure_type(reg, RegisterType::Datetime, "TimestampNanosecondArray")?;
        self.bank(reg.space)
            .datetime(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
    }

    fn generic(&self, reg: RegisterRef) -> Result<&ArrayRef, RuntimeError> {
        self.ensure_type(reg, RegisterType::Generic, "ArrayRef")?;
        self.bank(reg.space)
            .generic(reg.index)
            .ok_or(RuntimeError::MissingRegister { reg })
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

pub async fn execute_program(
    program: &CompiledProgram,
    batch: &TypedBatch,
) -> Result<TypedBatch, RuntimeError> {
    execute_program_in_context(program, batch, &ExecutionContext::default())
        .await
        .map(|result| result.batch)
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionResult {
    pub batch: TypedBatch,
    pub selected_rows: Vec<usize>,
    pub branch_selected_rows: Vec<Vec<usize>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionContext {
    pub now: Timestamp,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self {
            now: Timestamp::now(),
        }
    }
}

pub async fn execute_program_in_context(
    program: &CompiledProgram,
    batch: &TypedBatch,
    context: &ExecutionContext,
) -> Result<ExecutionResult, RuntimeError> {
    execute_program_with_selection_in_context(program, batch, context).await
}

pub async fn execute_program_with_selection(
    program: &CompiledProgram,
    batch: &TypedBatch,
) -> Result<ExecutionResult, RuntimeError> {
    execute_program_with_selection_in_context(program, batch, &ExecutionContext::default()).await
}

pub async fn execute_program_with_selection_in_context(
    program: &CompiledProgram,
    batch: &TypedBatch,
    context: &ExecutionContext,
) -> Result<ExecutionResult, RuntimeError> {
    if batch.row_count() <= SPAWN_BLOCKING_ROW_THRESHOLD {
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

#[cfg(test)]
fn execute_program_sync(
    program: &CompiledProgram,
    batch: &TypedBatch,
) -> Result<TypedBatch, RuntimeError> {
    execute_program_in_context_sync(program, batch, &ExecutionContext::default())
        .map(|result| result.batch)
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
    execute_program_with_selection_in_context_sync(program, batch, &ExecutionContext::default())
}

fn execute_program_with_selection_in_context_sync(
    program: &CompiledProgram,
    batch: &TypedBatch,
    context: &ExecutionContext,
) -> Result<ExecutionResult, RuntimeError> {
    if batch.schema().as_ref() != program.input_schema.as_ref() {
        return Err(RuntimeError::SchemaMismatch);
    }

    let mut registers = RegisterBank::new(&program.layouts);
    registers.load_input_batch(&program.inputs, batch)?;

    let mut row_errors = batch.errors().to_vec();

    for instruction in &program.instructions {
        instruction.execute(&mut registers, batch.row_count(), &mut row_errors, context)?;
    }

    let mut columns = Vec::with_capacity(program.outputs.len());
    for output in &program.outputs {
        columns.push(registers.output_array(output.reg)?);
    }

    let global_predicate = if let Some(filter_reg) = program.filter {
        Some(registers.boolean(filter_reg)?.clone())
    } else {
        None
    };

    let branch_selected_rows = program
        .branch_filters
        .iter()
        .map(|filter_reg| {
            let predicate = registers.boolean(*filter_reg)?;
            let predicate = if let Some(global_predicate) = &global_predicate {
                filter_boolean(predicate, global_predicate)
            } else {
                predicate.clone()
            };
            Ok(selected_rows(&predicate))
        })
        .collect::<Result<Vec<_>, RuntimeError>>()?;

    let (columns, row_errors, selected_rows) = if let Some(predicate) = global_predicate.as_ref() {
        (
            filter_columns(&columns, predicate)?,
            filter_errors(&row_errors, predicate),
            selected_rows(predicate),
        )
    } else {
        (columns, row_errors, (0..batch.row_count()).collect())
    };

    Ok(ExecutionResult {
        batch: TypedBatch::with_errors(program.output_schema.clone(), columns, row_errors)?,
        selected_rows,
        branch_selected_rows,
    })
}

impl Instruction {
    fn execute(
        &self,
        registers: &mut RegisterBank,
        row_count: usize,
        row_errors: &mut [Vec<SideError>],
        context: &ExecutionContext,
    ) -> Result<(), RuntimeError> {
        match &self.kind {
            InstructionKind::Move { dst, input } => {
                let output = registers.read_array(*input)?;
                registers.set_array(*dst, output)
            }
            InstructionKind::Literal { dst, value } => {
                write_literal(registers, *dst, value, row_count)
            }
            InstructionKind::NullLiteral { dst, data_type } => {
                write_null_literal(registers, *dst, data_type, row_count)
            }
            InstructionKind::Uninitialized { dst, data_type } => registers.set_array(
                *dst,
                TypedArray::uninitialized(data_type.clone(), row_count),
            ),
            InstructionKind::Unary { dst, input, op } => {
                let output = self.execute_unary(registers, *input, *op, row_errors)?;
                registers.set_array(*dst, output)
            }
            InstructionKind::Binary {
                dst,
                left,
                right,
                op,
            } => {
                let output = self.execute_binary(registers, *left, *right, *op, row_errors)?;
                registers.set_array(*dst, output)
            }
            InstructionKind::Cast { dst, input, target } => {
                let output = execute_cast(registers, *input, *target, row_errors, self.span)?;
                registers.set_array(*dst, output)
            }
            InstructionKind::Builtin {
                dst,
                lowering,
                inputs,
            } => {
                let output = execute_builtin(
                    *lowering, registers, inputs, row_count, row_errors, self.span, context,
                )?;
                registers.set_array(*dst, output)
            }
        }
    }

    fn execute_unary(
        &self,
        registers: &RegisterBank,
        input: RegisterRef,
        op: UnaryOp,
        row_errors: &mut [Vec<SideError>],
    ) -> Result<TypedArray, RuntimeError> {
        match op {
            UnaryOp::Neg => match input.ty {
                RegisterType::Int8 => Ok(TypedArray::Int8(execute_neg_i8(
                    registers.int8(input)?,
                    row_errors,
                    self.span,
                ))),
                RegisterType::Int16 => Ok(TypedArray::Int16(execute_neg_i16(
                    registers.int16(input)?,
                    row_errors,
                    self.span,
                ))),
                RegisterType::Int32 => Ok(TypedArray::Int32(execute_neg_i32(
                    registers.int32(input)?,
                    row_errors,
                    self.span,
                ))),
                RegisterType::Int64 => Ok(TypedArray::Int64(execute_neg_i64(
                    registers.int64(input)?,
                    row_errors,
                    self.span,
                ))),
                RegisterType::Float32 => Ok(TypedArray::Float32(execute_neg_f32(
                    registers.float32(input)?,
                ))),
                RegisterType::Float64 => Ok(TypedArray::Float64(execute_neg_f64(
                    registers.float64(input)?,
                ))),
                _ => Err(RuntimeError::InvalidRegisterType {
                    reg: input,
                    expected: "numeric array",
                }),
            },
            UnaryOp::Not => Ok(TypedArray::Boolean(execute_not(registers.boolean(input)?))),
        }
    }

    fn execute_binary(
        &self,
        registers: &RegisterBank,
        left: RegisterRef,
        right: RegisterRef,
        op: BinaryOp,
        row_errors: &mut [Vec<SideError>],
    ) -> Result<TypedArray, RuntimeError> {
        match left.ty {
            RegisterType::UInt8 => execute_binary_u8(
                registers.uint8(left)?,
                registers.uint8(right)?,
                op,
                row_errors,
                self.span,
            ),
            RegisterType::Int8 => execute_binary_i8(
                registers.int8(left)?,
                registers.int8(right)?,
                op,
                row_errors,
                self.span,
            ),
            RegisterType::UInt16 => execute_binary_u16(
                registers.uint16(left)?,
                registers.uint16(right)?,
                op,
                row_errors,
                self.span,
            ),
            RegisterType::Int16 => execute_binary_i16(
                registers.int16(left)?,
                registers.int16(right)?,
                op,
                row_errors,
                self.span,
            ),
            RegisterType::UInt32 => execute_binary_u32(
                registers.uint32(left)?,
                registers.uint32(right)?,
                op,
                row_errors,
                self.span,
            ),
            RegisterType::Int32 => execute_binary_i32(
                registers.int32(left)?,
                registers.int32(right)?,
                op,
                row_errors,
                self.span,
            ),
            RegisterType::UInt64 => execute_binary_u64(
                registers.uint64(left)?,
                registers.uint64(right)?,
                op,
                row_errors,
                self.span,
            ),
            RegisterType::Int64 => execute_binary_i64(
                registers.int64(left)?,
                registers.int64(right)?,
                op,
                row_errors,
                self.span,
            ),
            RegisterType::Float32 => execute_binary_f32(
                registers.float32(left)?,
                registers.float32(right)?,
                op,
                row_errors,
                self.span,
            ),
            RegisterType::Float64 => execute_binary_f64(
                registers.float64(left)?,
                registers.float64(right)?,
                op,
                row_errors,
                self.span,
            ),
            RegisterType::Boolean => {
                execute_binary_bool(registers.boolean(left)?, registers.boolean(right)?, op)
            }
            RegisterType::Utf8 => {
                execute_compare_utf8(registers.utf8(left)?, registers.utf8(right)?, op)
            }
            RegisterType::Datetime => {
                execute_compare_datetime(registers.datetime(left)?, registers.datetime(right)?, op)
            }
            RegisterType::Generic => Err(RuntimeError::InvalidRegisterType {
                reg: left,
                expected: "scalar array",
            }),
        }
    }
}

fn arrow_kernel_error(context: &str, error: ArrowError) -> RuntimeError {
    RuntimeError::InvalidBatch {
        message: format!("{context}: {error}"),
    }
}

fn array_ref_to_typed_array(array: ArrayRef) -> Result<TypedArray, RuntimeError> {
    match array.data_type() {
        DataType::UInt8 => Ok(TypedArray::UInt8(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("arrow kernel returned UInt8 data type without UInt8Array backing")
                .clone(),
        )),
        DataType::Int8 => Ok(TypedArray::Int8(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("arrow kernel returned Int8 data type without Int8Array backing")
                .clone(),
        )),
        DataType::UInt16 => Ok(TypedArray::UInt16(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .expect("arrow kernel returned UInt16 data type without UInt16Array backing")
                .clone(),
        )),
        DataType::Int16 => Ok(TypedArray::Int16(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("arrow kernel returned Int16 data type without Int16Array backing")
                .clone(),
        )),
        DataType::UInt32 => Ok(TypedArray::UInt32(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("arrow kernel returned UInt32 data type without UInt32Array backing")
                .clone(),
        )),
        DataType::Int32 => Ok(TypedArray::Int32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("arrow kernel returned Int32 data type without Int32Array backing")
                .clone(),
        )),
        DataType::UInt64 => Ok(TypedArray::UInt64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("arrow kernel returned UInt64 data type without UInt64Array backing")
                .clone(),
        )),
        DataType::Int64 => Ok(TypedArray::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("arrow kernel returned Int64 data type without Int64Array backing")
                .clone(),
        )),
        DataType::Float32 => Ok(TypedArray::Float32(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("arrow kernel returned Float32 data type without Float32Array backing")
                .clone(),
        )),
        DataType::Float64 => Ok(TypedArray::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("arrow kernel returned Float64 data type without Float64Array backing")
                .clone(),
        )),
        DataType::Boolean => Ok(TypedArray::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("arrow kernel returned Boolean data type without BooleanArray backing")
                .clone(),
        )),
        DataType::Utf8 => Ok(TypedArray::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("arrow kernel returned Utf8 data type without StringArray backing")
                .clone(),
        )),
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz))
            if tz.as_ref() == "+00:00" || tz.as_ref() == "UTC" =>
        {
            Ok(TypedArray::Datetime(
                array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect(
                        "arrow kernel returned UTC nanosecond timestamp data type without \
                         TimestampNanosecondArray backing",
                    )
                    .clone(),
            ))
        }
        _ => Ok(TypedArray::Generic(array)),
    }
}

fn typed_array_as_array(column: &TypedArray) -> &dyn Array {
    match column {
        TypedArray::UInt8(array) => array,
        TypedArray::Int8(array) => array,
        TypedArray::UInt16(array) => array,
        TypedArray::Int16(array) => array,
        TypedArray::UInt32(array) => array,
        TypedArray::Int32(array) => array,
        TypedArray::UInt64(array) => array,
        TypedArray::Int64(array) => array,
        TypedArray::Float32(array) => array,
        TypedArray::Float64(array) => array,
        TypedArray::Boolean(array) => array,
        TypedArray::Utf8(array) => array,
        TypedArray::Datetime(array) => array,
        TypedArray::Generic(array) => array.as_ref(),
        TypedArray::Uninitialized { .. } => {
            unreachable!("uninitialized arrays must be materialized before Arrow kernel access")
        }
    }
}

fn typed_array_to_array_ref(column: TypedArray) -> ArrayRef {
    match column {
        TypedArray::UInt8(array) => std::sync::Arc::new(array),
        TypedArray::Int8(array) => std::sync::Arc::new(array),
        TypedArray::UInt16(array) => std::sync::Arc::new(array),
        TypedArray::Int16(array) => std::sync::Arc::new(array),
        TypedArray::UInt32(array) => std::sync::Arc::new(array),
        TypedArray::Int32(array) => std::sync::Arc::new(array),
        TypedArray::UInt64(array) => std::sync::Arc::new(array),
        TypedArray::Int64(array) => std::sync::Arc::new(array),
        TypedArray::Float32(array) => std::sync::Arc::new(array),
        TypedArray::Float64(array) => std::sync::Arc::new(array),
        TypedArray::Boolean(array) => std::sync::Arc::new(array),
        TypedArray::Utf8(array) => std::sync::Arc::new(array),
        TypedArray::Datetime(array) => std::sync::Arc::new(array),
        TypedArray::Generic(array) => array,
        TypedArray::Uninitialized { data_type, len } => new_null_array(&data_type, len),
    }
}

fn typed_array_is_null(column: &TypedArray, row: usize) -> bool {
    match column {
        TypedArray::UInt8(array) => array.is_null(row),
        TypedArray::Int8(array) => array.is_null(row),
        TypedArray::UInt16(array) => array.is_null(row),
        TypedArray::Int16(array) => array.is_null(row),
        TypedArray::UInt32(array) => array.is_null(row),
        TypedArray::Int32(array) => array.is_null(row),
        TypedArray::UInt64(array) => array.is_null(row),
        TypedArray::Int64(array) => array.is_null(row),
        TypedArray::Float32(array) => array.is_null(row),
        TypedArray::Float64(array) => array.is_null(row),
        TypedArray::Boolean(array) => array.is_null(row),
        TypedArray::Utf8(array) => array.is_null(row),
        TypedArray::Datetime(array) => array.is_null(row),
        TypedArray::Generic(array) => array.is_null(row),
        TypedArray::Uninitialized { .. } => true,
    }
}

fn try_execute_numeric_kernel(
    left: &dyn Array,
    right: &dyn Array,
    op: BinaryOp,
) -> Option<ArrayRef> {
    let left = &left as &dyn Datum;
    let right = &right as &dyn Datum;
    match op {
        BinaryOp::Add => add(left, right).ok(),
        BinaryOp::Sub => sub(left, right).ok(),
        BinaryOp::Mul => mul(left, right).ok(),
        BinaryOp::Div => div(left, right).ok(),
        BinaryOp::Rem => rem(left, right).ok(),
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Gt
        | BinaryOp::Lt
        | BinaryOp::GtEq
        | BinaryOp::LtEq
        | BinaryOp::And
        | BinaryOp::Or => None,
    }
}

fn try_execute_neg_kernel(input: &dyn Array) -> Option<ArrayRef> {
    neg(input).ok()
}

fn sanitize_float32_non_finite(
    input: &Float32Array,
    row_errors: &mut [Vec<SideError>],
    span: Span,
    message: &str,
) -> Float32Array {
    let mut builder = Float32Builder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
            continue;
        }
        let value = input.value(row);
        if value.is_finite() {
            builder.append_value(value);
        } else {
            builder.append_null();
            push_error(row_errors, row, ErrorCode::InvalidArgument, message, span);
        }
    }
    builder.finish()
}

fn sanitize_float64_non_finite(
    input: &Float64Array,
    row_errors: &mut [Vec<SideError>],
    span: Span,
    message: &str,
) -> Float64Array {
    let mut builder = Float64Builder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
            continue;
        }
        let value = input.value(row);
        if value.is_finite() {
            builder.append_value(value);
        } else {
            builder.append_null();
            push_error(row_errors, row, ErrorCode::InvalidArgument, message, span);
        }
    }
    builder.finish()
}

fn execute_coalesce_arrow(inputs: &[TypedArray]) -> Result<TypedArray, RuntimeError> {
    let mut result = typed_array_to_array_ref(
        inputs
            .first()
            .expect("coalesce requires at least one input")
            .clone(),
    );
    for input in &inputs[1..] {
        let mask = is_null(result.as_ref())
            .map_err(|error| arrow_kernel_error("coalesce is_null kernel failed", error))?;
        let truthy = typed_array_as_array(input);
        let falsy = result.as_ref();
        result = zip(&mask, &truthy as &dyn Datum, &falsy as &dyn Datum)
            .map_err(|error| arrow_kernel_error("coalesce zip kernel failed", error))?;
    }
    array_ref_to_typed_array(result)
}

fn write_literal(
    registers: &mut RegisterBank,
    dst: RegisterRef,
    value: &ScalarValue,
    len: usize,
) -> Result<(), RuntimeError> {
    match value {
        ScalarValue::Int64(value) if dst.ty != RegisterType::Int64 => {
            Err(RuntimeError::InvalidRegisterType {
                reg: dst,
                expected: "Int64Array",
            })
        }
        ScalarValue::Int64(value) => registers.set_int64(dst, Int64Array::from_value(*value, len)),
        ScalarValue::Float64(value) if dst.ty != RegisterType::Float64 => {
            Err(RuntimeError::InvalidRegisterType {
                reg: dst,
                expected: "Float64Array",
            })
        }
        ScalarValue::Float64(value) => {
            registers.set_float64(dst, Float64Array::from_value(*value, len))
        }
        ScalarValue::Boolean(value) if dst.ty != RegisterType::Boolean => {
            Err(RuntimeError::InvalidRegisterType {
                reg: dst,
                expected: "BooleanArray",
            })
        }
        ScalarValue::Boolean(value) => registers.set_boolean(
            dst,
            BooleanArray::from_iter(std::iter::repeat_n(Some(*value), len)),
        ),
        ScalarValue::Utf8(_) if dst.ty != RegisterType::Utf8 => {
            Err(RuntimeError::InvalidRegisterType {
                reg: dst,
                expected: "StringArray",
            })
        }
        ScalarValue::Utf8(value) => registers.set_utf8(
            dst,
            StringArray::from_iter_values(std::iter::repeat_n(value.as_str(), len)),
        ),
    }
}

fn write_null_literal(
    registers: &mut RegisterBank,
    dst: RegisterRef,
    data_type: &DataType,
    len: usize,
) -> Result<(), RuntimeError> {
    let array = new_null_array(data_type, len);
    let typed = array_ref_to_typed_array(array)?;
    registers.set_array(dst, typed)
}

fn execute_neg_i64(
    input: &Int64Array,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Int64Array {
    if let Some(output) = try_execute_neg_kernel(input) {
        let TypedArray::Int64(output) =
            array_ref_to_typed_array(output).expect("int64 neg kernel must produce Int64 output")
        else {
            unreachable!("int64 neg kernel must produce Int64Array");
        };
        return output;
    }
    let mut builder = Int64Builder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
            continue;
        }
        match input.value(row).checked_neg() {
            Some(value) => builder.append_value(value),
            None => {
                builder.append_null();
                push_error(
                    row_errors,
                    row,
                    ErrorCode::Overflow,
                    "integer negation overflowed",
                    span,
                );
            }
        }
    }
    builder.finish()
}

fn execute_neg_f64(input: &Float64Array) -> Float64Array {
    let output = try_execute_neg_kernel(input).expect("float64 neg kernel must succeed");
    let TypedArray::Float64(output) =
        array_ref_to_typed_array(output).expect("float64 neg kernel must produce Float64 output")
    else {
        unreachable!("float64 neg kernel must produce Float64Array");
    };
    output
}

macro_rules! define_checked_neg {
    ($(($fn_name:ident, $array:ty, $builder:ty, $prim:ty, $typed_variant:ident));+ $(;)?) => {
        $(
            fn $fn_name(
                input: &$array,
                row_errors: &mut [Vec<SideError>],
                span: Span,
            ) -> $array {
                if let Some(output) = try_execute_neg_kernel(input) {
                    let TypedArray::$typed_variant(output) = array_ref_to_typed_array(output)
                        .expect("integer neg kernel must produce matching integer output")
                    else {
                        unreachable!("integer neg kernel must produce matching integer array");
                    };
                    return output;
                }
                let mut builder = <$builder>::new();
                for row in 0..input.len() {
                    if input.is_null(row) {
                        builder.append_null();
                        continue;
                    }
                    match input.value(row).checked_neg() {
                        Some(value) => builder.append_value(value),
                        None => {
                            builder.append_null();
                            push_error(
                                row_errors,
                                row,
                                ErrorCode::Overflow,
                                "integer negation overflowed",
                                span,
                            );
                        }
                    }
                }
                builder.finish()
            }
        )+
    };
}

define_checked_neg!(
    (execute_neg_i8, Int8Array, Int8Builder, i8, Int8);
    (execute_neg_i16, Int16Array, Int16Builder, i16, Int16);
    (execute_neg_i32, Int32Array, Int32Builder, i32, Int32)
);

fn execute_neg_f32(input: &Float32Array) -> Float32Array {
    let output = try_execute_neg_kernel(input).expect("float32 neg kernel must succeed");
    let TypedArray::Float32(output) =
        array_ref_to_typed_array(output).expect("float32 neg kernel must produce Float32 output")
    else {
        unreachable!("float32 neg kernel must produce Float32Array");
    };
    output
}

fn execute_not(input: &BooleanArray) -> BooleanArray {
    not(input).expect("boolean not kernel must succeed for BooleanArray")
}

macro_rules! define_integer_binary {
    ($(($fn_name:ident, $array:ty, $builder:ty, $prim:ty, $typed_variant:ident, $reg_ty:ident));+ $(;)?) => {
        $(
            fn $fn_name(
                left: &$array,
                right: &$array,
                op: BinaryOp,
                row_errors: &mut [Vec<SideError>],
                span: Span,
            ) -> Result<TypedArray, RuntimeError> {
                match op {
                    BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Rem => {
                        if let Some(output) = try_execute_numeric_kernel(left, right, op) {
                            let typed = array_ref_to_typed_array(output)
                                .expect("integer arithmetic kernel must produce matching integer output");
                            return Ok(typed);
                        }
                        let mut builder = <$builder>::new();
                        for row in 0..left.len() {
                            if left.is_null(row) || right.is_null(row) {
                                builder.append_null();
                                continue;
                            }
                            let lhs = left.value(row);
                            let rhs = right.value(row);
                            let value = match op {
                                BinaryOp::Add => lhs
                                    .checked_add(rhs)
                                    .ok_or((ErrorCode::Overflow, "integer addition overflowed")),
                                BinaryOp::Sub => lhs
                                    .checked_sub(rhs)
                                    .ok_or((ErrorCode::Overflow, "integer subtraction overflowed")),
                                BinaryOp::Mul => lhs
                                    .checked_mul(rhs)
                                    .ok_or((ErrorCode::Overflow, "integer multiplication overflowed")),
                                BinaryOp::Div => {
                                    if rhs == 0 {
                                        Err((ErrorCode::DivisionByZero, "integer division by zero"))
                                    } else {
                                        lhs.checked_div(rhs)
                                            .ok_or((ErrorCode::Overflow, "integer division overflowed"))
                                    }
                                }
                                BinaryOp::Rem => {
                                    if rhs == 0 {
                                        Err((ErrorCode::DivisionByZero, "integer remainder by zero"))
                                    } else {
                                        lhs.checked_rem(rhs)
                                            .ok_or((ErrorCode::Overflow, "integer remainder overflowed"))
                                    }
                                }
                                _ => unreachable!(),
                            };
                            match value {
                                Ok(value) => builder.append_value(value),
                                Err((code, message)) => {
                                    builder.append_null();
                                    push_error(row_errors, row, code, message, span);
                                }
                            }
                        }
                        Ok(TypedArray::$typed_variant(builder.finish()))
                    }
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Gt
                    | BinaryOp::Lt
                    | BinaryOp::GtEq
                    | BinaryOp::LtEq => {
                        let mut builder = BooleanBuilder::new();
                        for row in 0..left.len() {
                            if left.is_null(row) || right.is_null(row) {
                                builder.append_null();
                            } else {
                                builder.append_value(compare_values(left.value(row), right.value(row), op));
                            }
                        }
                        Ok(TypedArray::Boolean(builder.finish()))
                    }
                    BinaryOp::And | BinaryOp::Or => Err(RuntimeError::InvalidRegisterType {
                        reg: RegisterRef::new(RegisterSpace::Temp, RegisterType::$reg_ty, 0),
                        expected: "BooleanArray",
                    }),
                }
            }
        )+
    };
}

define_integer_binary!(
    (execute_binary_u8, UInt8Array, UInt8Builder, u8, UInt8, UInt8);
    (execute_binary_i8, Int8Array, Int8Builder, i8, Int8, Int8);
    (execute_binary_u16, UInt16Array, UInt16Builder, u16, UInt16, UInt16);
    (execute_binary_i16, Int16Array, Int16Builder, i16, Int16, Int16);
    (execute_binary_u32, UInt32Array, UInt32Builder, u32, UInt32, UInt32);
    (execute_binary_i32, Int32Array, Int32Builder, i32, Int32, Int32);
    (execute_binary_u64, UInt64Array, UInt64Builder, u64, UInt64, UInt64);
    (execute_binary_i64, Int64Array, Int64Builder, i64, Int64, Int64)
);

macro_rules! define_float_binary {
    ($(($fn_name:ident, $array:ty, $builder:ty, $typed_variant:ident, $reg_ty:ident));+ $(;)?) => {
        $(
            fn $fn_name(
                left: &$array,
                right: &$array,
                op: BinaryOp,
                row_errors: &mut [Vec<SideError>],
                span: Span,
            ) -> Result<TypedArray, RuntimeError> {
                match op {
                    BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Rem => {
                        if let Some(output) = try_execute_numeric_kernel(left, right, op) {
                            let typed = array_ref_to_typed_array(output)
                                .expect("float arithmetic kernel must produce matching float output");
                            return Ok(match typed {
                                TypedArray::Float32(output) => TypedArray::Float32(
                                    sanitize_float32_non_finite(
                                        &output,
                                        row_errors,
                                        span,
                                        "floating-point operation produced a non-finite result",
                                    ),
                                ),
                                TypedArray::Float64(output) => TypedArray::Float64(
                                    sanitize_float64_non_finite(
                                        &output,
                                        row_errors,
                                        span,
                                        "floating-point operation produced a non-finite result",
                                    ),
                                ),
                                _ => unreachable!("float arithmetic kernel must produce float output"),
                            });
                        }
                        let mut builder = <$builder>::new();
                        for row in 0..left.len() {
                            if left.is_null(row) || right.is_null(row) {
                                builder.append_null();
                                continue;
                            }
                            let lhs = left.value(row);
                            let rhs = right.value(row);
                            let value = match op {
                                BinaryOp::Add => lhs + rhs,
                                BinaryOp::Sub => lhs - rhs,
                                BinaryOp::Mul => lhs * rhs,
                                BinaryOp::Div => lhs / rhs,
                                BinaryOp::Rem => lhs % rhs,
                                _ => unreachable!(),
                            };
                            if value.is_finite() {
                                builder.append_value(value);
                            } else {
                                builder.append_null();
                                push_error(
                                    row_errors,
                                    row,
                                    ErrorCode::InvalidArgument,
                                    "floating-point operation produced a non-finite result",
                                    span,
                                );
                            }
                        }
                        Ok(TypedArray::$typed_variant(builder.finish()))
                    }
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Gt
                    | BinaryOp::Lt
                    | BinaryOp::GtEq
                    | BinaryOp::LtEq => {
                        let mut builder = BooleanBuilder::new();
                        for row in 0..left.len() {
                            if left.is_null(row) || right.is_null(row) {
                                builder.append_null();
                            } else {
                                builder.append_value(compare_values(left.value(row), right.value(row), op));
                            }
                        }
                        Ok(TypedArray::Boolean(builder.finish()))
                    }
                    BinaryOp::And | BinaryOp::Or => Err(RuntimeError::InvalidRegisterType {
                        reg: RegisterRef::new(RegisterSpace::Temp, RegisterType::$reg_ty, 0),
                        expected: "BooleanArray",
                    }),
                }
            }
        )+
    };
}

define_float_binary!(
    (execute_binary_f32, Float32Array, Float32Builder, Float32, Float32);
    (execute_binary_f64, Float64Array, Float64Builder, Float64, Float64)
);

fn execute_binary_bool(
    left: &BooleanArray,
    right: &BooleanArray,
    op: BinaryOp,
) -> Result<TypedArray, RuntimeError> {
    let output = match op {
        BinaryOp::And => and_kleene(left, right)
            .map_err(|error| arrow_kernel_error("boolean and kernel failed", error))?,
        BinaryOp::Or => or_kleene(left, right)
            .map_err(|error| arrow_kernel_error("boolean or kernel failed", error))?,
        BinaryOp::Eq => eq(left, right)
            .map_err(|error| arrow_kernel_error("boolean eq kernel failed", error))?,
        BinaryOp::NotEq => neq(left, right)
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

fn execute_compare_utf8(
    left: &StringArray,
    right: &StringArray,
    op: BinaryOp,
) -> Result<TypedArray, RuntimeError> {
    Ok(TypedArray::Boolean(compare_with_arrow_ord(
        left,
        right,
        op,
        "utf8 comparison",
    )?))
}

fn execute_compare_datetime(
    left: &TimestampNanosecondArray,
    right: &TimestampNanosecondArray,
    op: BinaryOp,
) -> Result<TypedArray, RuntimeError> {
    Ok(TypedArray::Boolean(compare_with_arrow_ord(
        left,
        right,
        op,
        "datetime comparison",
    )?))
}

fn compare_with_arrow_ord(
    left: &dyn Array,
    right: &dyn Array,
    op: BinaryOp,
    context: &str,
) -> Result<BooleanArray, RuntimeError> {
    let left = &left as &dyn Datum;
    let right = &right as &dyn Datum;
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

fn execute_nullif_arrow(left: &dyn Array, right: &dyn Array) -> Result<TypedArray, RuntimeError> {
    let left_datum = &left as &dyn Datum;
    let right_datum = &right as &dyn Datum;
    let predicate = eq(left_datum, right_datum)
        .map_err(|error| arrow_kernel_error("nullif eq kernel failed", error))?;
    let output = nullif(left, &predicate)
        .map_err(|error| arrow_kernel_error("nullif kernel failed", error))?;
    array_ref_to_typed_array(output)
}

fn execute_cast(
    registers: &RegisterBank,
    input: RegisterRef,
    target: RegisterType,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    let input = registers.read_array(input)?;
    cast_typed_array(input, target, row_errors, span)
}

fn execute_builtin(
    lowering: BuiltinLowering,
    registers: &RegisterBank,
    inputs: &[RegisterRef],
    row_count: usize,
    row_errors: &mut [Vec<SideError>],
    span: Span,
    context: &ExecutionContext,
) -> Result<TypedArray, RuntimeError> {
    let values = inputs
        .iter()
        .map(|input| registers.read_array(*input))
        .collect::<Result<Vec<_>, _>>()?;

    match lowering {
        BuiltinLowering::Now => Ok(TypedArray::Datetime(execute_now(row_count, context.now))),
        BuiltinLowering::UuidV4 => Ok(TypedArray::Utf8(execute_uuid_v4(row_count))),
        BuiltinLowering::UuidV7 => Ok(TypedArray::Utf8(execute_uuid_v7(row_count, context.now))),
        BuiltinLowering::Lower => Ok(TypedArray::Utf8(execute_lower(as_utf8(&values[0])?))),
        BuiltinLowering::Upper => Ok(TypedArray::Utf8(execute_upper(as_utf8(&values[0])?))),
        BuiltinLowering::Trim | BuiltinLowering::Btrim => {
            Ok(TypedArray::Utf8(execute_trim(as_utf8(&values[0])?)))
        }
        BuiltinLowering::Ltrim => Ok(TypedArray::Utf8(execute_ltrim(as_utf8(&values[0])?))),
        BuiltinLowering::Rtrim => Ok(TypedArray::Utf8(execute_rtrim(as_utf8(&values[0])?))),
        BuiltinLowering::Length | BuiltinLowering::CharLength => {
            Ok(TypedArray::Int64(execute_length(as_utf8(&values[0])?)))
        }
        BuiltinLowering::BitLength => {
            Ok(TypedArray::Int64(execute_bit_length(as_utf8(&values[0])?)))
        }
        BuiltinLowering::Ascii => Ok(TypedArray::Int64(execute_ascii(as_utf8(&values[0])?))),
        BuiltinLowering::Coalesce => execute_coalesce_arrow(&values),
        BuiltinLowering::IsNull => Ok(TypedArray::Boolean(execute_is_null_typed(&values[0]))),
        BuiltinLowering::NullIf => execute_nullif_arrow(
            typed_array_as_array(&values[0]),
            typed_array_as_array(&values[1]),
        ),
        BuiltinLowering::Abs => execute_abs_typed(&values[0], row_errors, span),
        BuiltinLowering::Acos => {
            execute_unary_math_f64(&values[0], row_errors, span, "acos", |v| v.acos())
        }
        BuiltinLowering::Asin => {
            execute_unary_math_f64(&values[0], row_errors, span, "asin", |v| v.asin())
        }
        BuiltinLowering::Atan => {
            execute_unary_math_f64(&values[0], row_errors, span, "atan", |v| v.atan())
        }
        BuiltinLowering::Ceil => execute_ceil(&values[0], row_errors, span),
        BuiltinLowering::Concat => Ok(TypedArray::Utf8(execute_concat(&values)?)),
        BuiltinLowering::Sum => execute_list_sum(&values[0]),
        BuiltinLowering::First => execute_list_item(&values[0], ListItem::First, None),
        BuiltinLowering::Last => execute_list_item(&values[0], ListItem::Last, None),
        BuiltinLowering::Count => Ok(TypedArray::Int64(execute_list_count(&values[0])?)),
        BuiltinLowering::Nth => execute_list_item(&values[0], ListItem::Nth, Some(&values[1])),
        BuiltinLowering::Contains => Ok(TypedArray::Boolean(execute_contains(
            as_utf8(&values[0])?,
            as_utf8(&values[1])?,
        ))),
        BuiltinLowering::Cos => {
            execute_unary_math_f64(&values[0], row_errors, span, "cos", |v| v.cos())
        }
        BuiltinLowering::StartsWith => Ok(TypedArray::Boolean(execute_starts_with(
            as_utf8(&values[0])?,
            as_utf8(&values[1])?,
        ))),
        BuiltinLowering::EndsWith => Ok(TypedArray::Boolean(execute_ends_with(
            as_utf8(&values[0])?,
            as_utf8(&values[1])?,
        ))),
        BuiltinLowering::Exp => {
            execute_unary_math_f64(&values[0], row_errors, span, "exp", |v| v.exp())
        }
        BuiltinLowering::Floor => execute_floor(&values[0], row_errors, span),
        BuiltinLowering::Initcap => Ok(TypedArray::Utf8(execute_initcap(as_utf8(&values[0])?))),
        BuiltinLowering::Left => Ok(TypedArray::Utf8(execute_left(
            as_utf8(&values[0])?,
            &values[1],
        )?)),
        BuiltinLowering::Ln => {
            execute_unary_math_f64(&values[0], row_errors, span, "ln", |v| v.ln())
        }
        BuiltinLowering::Log => execute_log(&values, row_errors, span),
        BuiltinLowering::Lpad => Ok(TypedArray::Utf8(execute_lpad(
            as_utf8(&values[0])?,
            &values[1],
            as_utf8(&values[2])?,
        )?)),
        BuiltinLowering::Md5 => Ok(TypedArray::Utf8(execute_md5(as_utf8(&values[0])?))),
        BuiltinLowering::Pow => execute_pow(&values[0], &values[1], row_errors, span),
        BuiltinLowering::RegexpLike => Ok(TypedArray::Boolean(execute_regexp_like(
            as_utf8(&values[0])?,
            as_utf8(&values[1])?,
            row_errors,
            span,
        ))),
        BuiltinLowering::RegexpReplace => Ok(TypedArray::Utf8(execute_regexp_replace(
            as_utf8(&values[0])?,
            as_utf8(&values[1])?,
            as_utf8(&values[2])?,
            row_errors,
            span,
        ))),
        BuiltinLowering::RegexpSubstr => Ok(TypedArray::Utf8(execute_regexp_substr(
            as_utf8(&values[0])?,
            as_utf8(&values[1])?,
            row_errors,
            span,
        ))),
        BuiltinLowering::Repeat => Ok(TypedArray::Utf8(execute_repeat(
            as_utf8(&values[0])?,
            &values[1],
        )?)),
        BuiltinLowering::Replace => Ok(TypedArray::Utf8(execute_replace(
            as_utf8(&values[0])?,
            as_utf8(&values[1])?,
            as_utf8(&values[2])?,
        ))),
        BuiltinLowering::Reverse => Ok(TypedArray::Utf8(execute_reverse(as_utf8(&values[0])?))),
        BuiltinLowering::Right => Ok(TypedArray::Utf8(execute_right(
            as_utf8(&values[0])?,
            &values[1],
        )?)),
        BuiltinLowering::Round => execute_round(&values[0], row_errors, span),
        BuiltinLowering::Rpad => Ok(TypedArray::Utf8(execute_rpad(
            as_utf8(&values[0])?,
            &values[1],
            as_utf8(&values[2])?,
        )?)),
        BuiltinLowering::SplitPart => Ok(TypedArray::Utf8(execute_split_part(
            as_utf8(&values[0])?,
            as_utf8(&values[1])?,
            &values[2],
        )?)),
        BuiltinLowering::Sqrt => {
            execute_unary_math_f64(&values[0], row_errors, span, "sqrt", |v| v.sqrt())
        }
        BuiltinLowering::Strpos => Ok(TypedArray::Int64(execute_strpos(
            as_utf8(&values[0])?,
            as_utf8(&values[1])?,
        ))),
        BuiltinLowering::Substr => Ok(TypedArray::Utf8(execute_substr(
            as_utf8(&values[0])?,
            &values[1],
            values.get(2),
        )?)),
        BuiltinLowering::Tan => {
            execute_unary_math_f64(&values[0], row_errors, span, "tan", |v| v.tan())
        }
        BuiltinLowering::ToHex => Ok(TypedArray::Utf8(execute_to_hex(&values[0])?)),
        BuiltinLowering::Translate => Ok(TypedArray::Utf8(execute_translate(
            as_utf8(&values[0])?,
            as_utf8(&values[1])?,
            as_utf8(&values[2])?,
        ))),
    }
}

enum ListItem {
    First,
    Last,
    Nth,
}

fn generic_list_array(input: &TypedArray) -> Result<&dyn Array, RuntimeError> {
    let TypedArray::Generic(array) = input else {
        return Err(RuntimeError::InvalidBatch {
            message: format!(
                "list builtin requires ARRAY or VEC input, found {:?}",
                input.data_type()
            ),
        });
    };
    match array.data_type() {
        DataType::List(_) | DataType::FixedSizeList(_, _) => Ok(array.as_ref()),
        other => Err(RuntimeError::InvalidBatch {
            message: format!("list builtin requires ARRAY or VEC input, found {other:?}"),
        }),
    }
}

fn list_value(array: &dyn Array, row: usize) -> Result<Option<ArrayRef>, RuntimeError> {
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(array) = array.as_any().downcast_ref::<ListArray>() {
        return Ok(Some(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        return Ok(Some(array.value(row)));
    }
    Err(RuntimeError::InvalidBatch {
        message: format!("expected list array, found {:?}", array.data_type()),
    })
}

fn list_element_data_type(array: &dyn Array) -> Result<DataType, RuntimeError> {
    match array.data_type() {
        DataType::List(field) | DataType::FixedSizeList(field, _) => Ok(field.data_type().clone()),
        other => Err(RuntimeError::InvalidBatch {
            message: format!("expected list array, found {other:?}"),
        }),
    }
}

fn execute_list_count(input: &TypedArray) -> Result<Int64Array, RuntimeError> {
    let array = generic_list_array(input)?;
    let mut builder = Int64Builder::new();
    for row in 0..array.len() {
        let Some(values) = list_value(array, row)? else {
            builder.append_null();
            continue;
        };
        builder.append_value(i64::try_from(values.len()).unwrap_or(i64::MAX));
    }
    Ok(builder.finish())
}

macro_rules! execute_list_sum_for_primitive {
    ($array:expr, $array_ty:ty, $builder:ty) => {{
        let mut builder = <$builder>::new();
        for row in 0..$array.len() {
            let Some(values) = list_value($array, row)? else {
                builder.append_null();
                continue;
            };
            let values = values.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
                RuntimeError::InvalidBatch {
                    message: format!("list values are not {}", stringify!($array_ty)),
                }
            })?;
            if let Some(sum) = arrow_sum(values) {
                builder.append_value(sum);
            } else {
                builder.append_null();
            }
        }
        Ok(builder.finish())
    }};
}

fn execute_list_sum(input: &TypedArray) -> Result<TypedArray, RuntimeError> {
    let array = generic_list_array(input)?;
    match list_element_data_type(array)? {
        DataType::UInt8 => {
            execute_list_sum_for_primitive!(array, UInt8Array, UInt8Builder).map(TypedArray::UInt8)
        }
        DataType::Int8 => {
            execute_list_sum_for_primitive!(array, Int8Array, Int8Builder).map(TypedArray::Int8)
        }
        DataType::UInt16 => execute_list_sum_for_primitive!(array, UInt16Array, UInt16Builder)
            .map(TypedArray::UInt16),
        DataType::Int16 => {
            execute_list_sum_for_primitive!(array, Int16Array, Int16Builder).map(TypedArray::Int16)
        }
        DataType::UInt32 => execute_list_sum_for_primitive!(array, UInt32Array, UInt32Builder)
            .map(TypedArray::UInt32),
        DataType::Int32 => {
            execute_list_sum_for_primitive!(array, Int32Array, Int32Builder).map(TypedArray::Int32)
        }
        DataType::UInt64 => execute_list_sum_for_primitive!(array, UInt64Array, UInt64Builder)
            .map(TypedArray::UInt64),
        DataType::Int64 => {
            execute_list_sum_for_primitive!(array, Int64Array, Int64Builder).map(TypedArray::Int64)
        }
        DataType::Float32 => execute_list_sum_for_primitive!(array, Float32Array, Float32Builder)
            .map(TypedArray::Float32),
        DataType::Float64 => execute_list_sum_for_primitive!(array, Float64Array, Float64Builder)
            .map(TypedArray::Float64),
        other => Err(RuntimeError::InvalidBatch {
            message: format!("sum requires numeric ARRAY or VEC elements, found {other:?}"),
        }),
    }
}

fn list_item_index(
    item: &ListItem,
    values_len: usize,
    row: usize,
    index_input: Option<&TypedArray>,
) -> Result<Option<usize>, RuntimeError> {
    match item {
        ListItem::First => Ok((values_len > 0).then_some(0)),
        ListItem::Last => Ok(values_len.checked_sub(1)),
        ListItem::Nth => {
            let Some(index_input) = index_input else {
                return Err(RuntimeError::InvalidBatch {
                    message: "nth requires an index input".to_string(),
                });
            };
            let Some(index) = integral_value_at(index_input, row)? else {
                return Ok(None);
            };
            if index < 0 {
                return Ok(None);
            }
            let index = usize::try_from(index).unwrap_or(usize::MAX);
            Ok((index < values_len).then_some(index))
        }
    }
}

macro_rules! execute_list_item_for_primitive {
    ($array:expr, $item:expr, $index_input:expr, $array_ty:ty, $builder:ty) => {{
        let mut builder = <$builder>::new();
        for row in 0..$array.len() {
            let Some(values) = list_value($array, row)? else {
                builder.append_null();
                continue;
            };
            let Some(index) = list_item_index(&$item, values.len(), row, $index_input)? else {
                builder.append_null();
                continue;
            };
            let values = values.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
                RuntimeError::InvalidBatch {
                    message: format!("list values are not {}", stringify!($array_ty)),
                }
            })?;
            if values.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(values.value(index));
            }
        }
        Ok(builder.finish())
    }};
}

fn execute_list_item(
    input: &TypedArray,
    item: ListItem,
    index_input: Option<&TypedArray>,
) -> Result<TypedArray, RuntimeError> {
    let array = generic_list_array(input)?;
    match list_element_data_type(array)? {
        DataType::UInt8 => {
            execute_list_item_for_primitive!(array, item, index_input, UInt8Array, UInt8Builder)
                .map(TypedArray::UInt8)
        }
        DataType::Int8 => {
            execute_list_item_for_primitive!(array, item, index_input, Int8Array, Int8Builder)
                .map(TypedArray::Int8)
        }
        DataType::UInt16 => {
            execute_list_item_for_primitive!(array, item, index_input, UInt16Array, UInt16Builder)
                .map(TypedArray::UInt16)
        }
        DataType::Int16 => {
            execute_list_item_for_primitive!(array, item, index_input, Int16Array, Int16Builder)
                .map(TypedArray::Int16)
        }
        DataType::UInt32 => {
            execute_list_item_for_primitive!(array, item, index_input, UInt32Array, UInt32Builder)
                .map(TypedArray::UInt32)
        }
        DataType::Int32 => {
            execute_list_item_for_primitive!(array, item, index_input, Int32Array, Int32Builder)
                .map(TypedArray::Int32)
        }
        DataType::UInt64 => {
            execute_list_item_for_primitive!(array, item, index_input, UInt64Array, UInt64Builder)
                .map(TypedArray::UInt64)
        }
        DataType::Int64 => {
            execute_list_item_for_primitive!(array, item, index_input, Int64Array, Int64Builder)
                .map(TypedArray::Int64)
        }
        DataType::Float32 => {
            execute_list_item_for_primitive!(array, item, index_input, Float32Array, Float32Builder)
                .map(TypedArray::Float32)
        }
        DataType::Float64 => {
            execute_list_item_for_primitive!(array, item, index_input, Float64Array, Float64Builder)
                .map(TypedArray::Float64)
        }
        DataType::Boolean => {
            execute_list_item_for_primitive!(array, item, index_input, BooleanArray, BooleanBuilder)
                .map(TypedArray::Boolean)
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in 0..array.len() {
                let Some(values) = list_value(array, row)? else {
                    builder.append_null();
                    continue;
                };
                let Some(index) = list_item_index(&item, values.len(), row, index_input)? else {
                    builder.append_null();
                    continue;
                };
                let values = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| RuntimeError::InvalidBatch {
                        message: "list values are not StringArray".to_string(),
                    })?;
                if values.is_null(index) {
                    builder.append_null();
                } else {
                    builder.append_value(values.value(index));
                }
            }
            Ok(TypedArray::Utf8(builder.finish()))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz))
            if tz.as_ref() == "+00:00" || tz.as_ref() == "UTC" =>
        {
            let mut builder = TimestampNanosecondBuilder::new().with_data_type(
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("+00:00".into())),
            );
            for row in 0..array.len() {
                let Some(values) = list_value(array, row)? else {
                    builder.append_null();
                    continue;
                };
                let Some(index) = list_item_index(&item, values.len(), row, index_input)? else {
                    builder.append_null();
                    continue;
                };
                let values = values
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| RuntimeError::InvalidBatch {
                        message: "list values are not TimestampNanosecondArray".to_string(),
                    })?;
                if values.is_null(index) {
                    builder.append_null();
                } else {
                    builder.append_value(values.value(index));
                }
            }
            Ok(TypedArray::Datetime(builder.finish()))
        }
        other => Err(RuntimeError::InvalidBatch {
            message: format!("list item function does not support element type {other:?}"),
        }),
    }
}

fn execute_lower(input: &StringArray) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(input.value(row).to_ascii_lowercase());
        }
    }
    builder.finish()
}

fn execute_upper(input: &StringArray) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(input.value(row).to_ascii_uppercase());
        }
    }
    builder.finish()
}

fn execute_trim(input: &StringArray) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(input.value(row).trim());
        }
    }
    builder.finish()
}

fn execute_length(input: &StringArray) -> Int64Array {
    let mut builder = Int64Builder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(input.value(row).chars().count() as i64);
        }
    }
    builder.finish()
}

fn execute_is_null<A: Array + ?Sized>(input: &A) -> BooleanArray {
    BooleanArray::from_iter((0..input.len()).map(|row| Some(input.is_null(row))))
}

macro_rules! define_identity_abs {
    ($(($fn_name:ident, $array:ty, $builder:ty));+ $(;)?) => {
        $(
            fn $fn_name(input: &$array) -> $array {
                let mut builder = <$builder>::new();
                for row in 0..input.len() {
                    if input.is_null(row) {
                        builder.append_null();
                    } else {
                        builder.append_value(input.value(row));
                    }
                }
                builder.finish()
            }
        )+
    };
}

define_identity_abs!(
    (execute_abs_u8, UInt8Array, UInt8Builder);
    (execute_abs_u16, UInt16Array, UInt16Builder);
    (execute_abs_u32, UInt32Array, UInt32Builder);
    (execute_abs_u64, UInt64Array, UInt64Builder)
);

macro_rules! define_checked_abs {
    ($(($fn_name:ident, $array:ty, $builder:ty));+ $(;)?) => {
        $(
            fn $fn_name(input: &$array, row_errors: &mut [Vec<SideError>], span: Span) -> $array {
                let mut builder = <$builder>::new();
                for row in 0..input.len() {
                    if input.is_null(row) {
                        builder.append_null();
                        continue;
                    }
                    match input.value(row).checked_abs() {
                        Some(value) => builder.append_value(value),
                        None => {
                            builder.append_null();
                            push_error(
                                row_errors,
                                row,
                                ErrorCode::Overflow,
                                "integer absolute value overflowed",
                                span,
                            );
                        }
                    }
                }
                builder.finish()
            }
        )+
    };
}

define_checked_abs!(
    (execute_abs_i8, Int8Array, Int8Builder);
    (execute_abs_i16, Int16Array, Int16Builder);
    (execute_abs_i32, Int32Array, Int32Builder);
    (execute_abs_i64, Int64Array, Int64Builder)
);

fn execute_abs_f32(
    input: &Float32Array,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Float32Array {
    let zero = Float32Array::new_scalar(0.0);
    let input_datum = input as &dyn Datum;
    let zero_datum = &zero as &dyn Datum;
    let negative = lt(input_datum, zero_datum).expect("float32 abs comparison kernel must succeed");
    let negated = try_execute_neg_kernel(input).expect("float32 neg kernel must succeed");
    let negated = negated.as_ref();
    let zipped = zip(&negative, &negated as &dyn Datum, &input as &dyn Datum)
        .expect("float32 abs zip kernel must succeed");
    let TypedArray::Float32(output) =
        array_ref_to_typed_array(zipped).expect("float32 abs kernel must produce Float32 output")
    else {
        unreachable!("float32 abs kernel must produce Float32Array");
    };
    sanitize_float32_non_finite(
        &output,
        row_errors,
        span,
        "floating-point absolute value produced a non-finite result",
    )
}

fn execute_abs_f64(
    input: &Float64Array,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Float64Array {
    let zero = Float64Array::new_scalar(0.0);
    let input_datum = input as &dyn Datum;
    let zero_datum = &zero as &dyn Datum;
    let negative = lt(input_datum, zero_datum).expect("float64 abs comparison kernel must succeed");
    let negated = try_execute_neg_kernel(input).expect("float64 neg kernel must succeed");
    let negated = negated.as_ref();
    let zipped = zip(&negative, &negated as &dyn Datum, &input as &dyn Datum)
        .expect("float64 abs zip kernel must succeed");
    let TypedArray::Float64(output) =
        array_ref_to_typed_array(zipped).expect("float64 abs kernel must produce Float64 output")
    else {
        unreachable!("float64 abs kernel must produce Float64Array");
    };
    sanitize_float64_non_finite(
        &output,
        row_errors,
        span,
        "floating-point absolute value produced a non-finite result",
    )
}

fn execute_contains(string: &StringArray, substring: &StringArray) -> BooleanArray {
    string_contains(string, substring).expect("utf8 contains kernel must succeed for Utf8 arrays")
}

fn execute_starts_with(string: &StringArray, prefix: &StringArray) -> BooleanArray {
    string_starts_with(string, prefix)
        .expect("utf8 starts_with kernel must succeed for Utf8 arrays")
}

fn execute_ends_with(string: &StringArray, suffix: &StringArray) -> BooleanArray {
    string_ends_with(string, suffix).expect("utf8 ends_with kernel must succeed for Utf8 arrays")
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
    let mut builder = Int64Builder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value((input.value(row).len() * 8) as i64);
        }
    }
    builder.finish()
}

fn execute_ascii(input: &StringArray) -> Int64Array {
    let mut builder = Int64Builder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            let value = input
                .value(row)
                .chars()
                .next()
                .map(|ch| ch as i64)
                .unwrap_or(0);
            builder.append_value(value);
        }
    }
    builder.finish()
}

fn execute_ltrim(input: &StringArray) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(input.value(row).trim_start());
        }
    }
    builder.finish()
}

fn execute_rtrim(input: &StringArray) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(input.value(row).trim_end());
        }
    }
    builder.finish()
}

fn execute_initcap(input: &StringArray) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
            continue;
        }
        let mut result = String::new();
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
        builder.append_value(result);
    }
    builder.finish()
}

fn execute_is_null_typed(input: &TypedArray) -> BooleanArray {
    match input {
        TypedArray::UInt8(array) => execute_is_null(array),
        TypedArray::Int8(array) => execute_is_null(array),
        TypedArray::UInt16(array) => execute_is_null(array),
        TypedArray::Int16(array) => execute_is_null(array),
        TypedArray::UInt32(array) => execute_is_null(array),
        TypedArray::Int32(array) => execute_is_null(array),
        TypedArray::UInt64(array) => execute_is_null(array),
        TypedArray::Int64(array) => execute_is_null(array),
        TypedArray::Float32(array) => execute_is_null(array),
        TypedArray::Float64(array) => execute_is_null(array),
        TypedArray::Boolean(array) => execute_is_null(array),
        TypedArray::Utf8(array) => execute_is_null(array),
        TypedArray::Datetime(array) => execute_is_null(array),
        TypedArray::Generic(array) => execute_is_null(array.as_ref()),
        TypedArray::Uninitialized { len, .. } => BooleanArray::from(vec![true; *len]),
    }
}

fn execute_abs_typed(
    input: &TypedArray,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    match input {
        TypedArray::UInt8(array) => Ok(TypedArray::UInt8(execute_abs_u8(array))),
        TypedArray::Int8(array) => Ok(TypedArray::Int8(execute_abs_i8(array, row_errors, span))),
        TypedArray::UInt16(array) => Ok(TypedArray::UInt16(execute_abs_u16(array))),
        TypedArray::Int16(array) => Ok(TypedArray::Int16(execute_abs_i16(array, row_errors, span))),
        TypedArray::UInt32(array) => Ok(TypedArray::UInt32(execute_abs_u32(array))),
        TypedArray::Int32(array) => Ok(TypedArray::Int32(execute_abs_i32(array, row_errors, span))),
        TypedArray::UInt64(array) => Ok(TypedArray::UInt64(execute_abs_u64(array))),
        TypedArray::Int64(array) => Ok(TypedArray::Int64(execute_abs_i64(array, row_errors, span))),
        TypedArray::Float32(array) => Ok(TypedArray::Float32(execute_abs_f32(
            array, row_errors, span,
        ))),
        TypedArray::Float64(array) => Ok(TypedArray::Float64(execute_abs_f64(
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

fn execute_unary_math_f64(
    input: &TypedArray,
    row_errors: &mut [Vec<SideError>],
    span: Span,
    function: &str,
    op: impl Fn(f64) -> f64,
) -> Result<TypedArray, RuntimeError> {
    let mut builder = Float64Builder::new();
    for row in 0..input.len() {
        let Some(value) = numeric_value_as_f64(input, row)? else {
            builder.append_null();
            continue;
        };
        let output = op(value);
        if output.is_finite() {
            builder.append_value(output);
        } else {
            builder.append_null();
            push_error(
                row_errors,
                row,
                ErrorCode::InvalidArgument,
                &format!("{function} produced a non-finite result"),
                span,
            );
        }
    }
    Ok(TypedArray::Float64(builder.finish()))
}

fn execute_ceil(
    input: &TypedArray,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    match input {
        TypedArray::UInt8(_)
        | TypedArray::Int8(_)
        | TypedArray::UInt16(_)
        | TypedArray::Int16(_)
        | TypedArray::UInt32(_)
        | TypedArray::Int32(_)
        | TypedArray::UInt64(_)
        | TypedArray::Int64(_) => Ok(input.clone()),
        TypedArray::Float32(array) => {
            let mut builder = Float32Builder::new();
            for row in 0..array.len() {
                if array.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let value = array.value(row).ceil();
                if value.is_finite() {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                    push_error(
                        row_errors,
                        row,
                        ErrorCode::InvalidArgument,
                        "ceil produced a non-finite result",
                        span,
                    );
                }
            }
            Ok(TypedArray::Float32(builder.finish()))
        }
        TypedArray::Float64(array) => {
            let mut builder = Float64Builder::new();
            for row in 0..array.len() {
                if array.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let value = array.value(row).ceil();
                if value.is_finite() {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                    push_error(
                        row_errors,
                        row,
                        ErrorCode::InvalidArgument,
                        "ceil produced a non-finite result",
                        span,
                    );
                }
            }
            Ok(TypedArray::Float64(builder.finish()))
        }
        TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => Err(RuntimeError::InvalidBatch {
            message: format!("ceil requires numeric input, found {:?}", input.data_type()),
        }),
    }
}

fn execute_floor(
    input: &TypedArray,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    match input {
        TypedArray::UInt8(_)
        | TypedArray::Int8(_)
        | TypedArray::UInt16(_)
        | TypedArray::Int16(_)
        | TypedArray::UInt32(_)
        | TypedArray::Int32(_)
        | TypedArray::UInt64(_)
        | TypedArray::Int64(_) => Ok(input.clone()),
        TypedArray::Float32(array) => {
            let mut builder = Float32Builder::new();
            for row in 0..array.len() {
                if array.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let value = array.value(row).floor();
                if value.is_finite() {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                    push_error(
                        row_errors,
                        row,
                        ErrorCode::InvalidArgument,
                        "floor produced a non-finite result",
                        span,
                    );
                }
            }
            Ok(TypedArray::Float32(builder.finish()))
        }
        TypedArray::Float64(array) => {
            let mut builder = Float64Builder::new();
            for row in 0..array.len() {
                if array.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let value = array.value(row).floor();
                if value.is_finite() {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                    push_error(
                        row_errors,
                        row,
                        ErrorCode::InvalidArgument,
                        "floor produced a non-finite result",
                        span,
                    );
                }
            }
            Ok(TypedArray::Float64(builder.finish()))
        }
        TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => Err(RuntimeError::InvalidBatch {
            message: format!(
                "floor requires numeric input, found {:?}",
                input.data_type()
            ),
        }),
    }
}

fn execute_round(
    input: &TypedArray,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    match input {
        TypedArray::UInt8(_)
        | TypedArray::Int8(_)
        | TypedArray::UInt16(_)
        | TypedArray::Int16(_)
        | TypedArray::UInt32(_)
        | TypedArray::Int32(_)
        | TypedArray::UInt64(_)
        | TypedArray::Int64(_) => Ok(input.clone()),
        TypedArray::Float32(array) => {
            let mut builder = Float32Builder::new();
            for row in 0..array.len() {
                if array.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let value = array.value(row).round();
                if value.is_finite() {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                    push_error(
                        row_errors,
                        row,
                        ErrorCode::InvalidArgument,
                        "round produced a non-finite result",
                        span,
                    );
                }
            }
            Ok(TypedArray::Float32(builder.finish()))
        }
        TypedArray::Float64(array) => {
            let mut builder = Float64Builder::new();
            for row in 0..array.len() {
                if array.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let value = array.value(row).round();
                if value.is_finite() {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                    push_error(
                        row_errors,
                        row,
                        ErrorCode::InvalidArgument,
                        "round produced a non-finite result",
                        span,
                    );
                }
            }
            Ok(TypedArray::Float64(builder.finish()))
        }
        TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => Err(RuntimeError::InvalidBatch {
            message: format!(
                "round requires numeric input, found {:?}",
                input.data_type()
            ),
        }),
    }
}

fn execute_concat(values: &[TypedArray]) -> Result<StringArray, RuntimeError> {
    let Some(first) = values.first() else {
        return Ok(StringArray::from(Vec::<Option<String>>::new()));
    };
    let row_count = first.len();
    let mut builder = StringBuilder::new();
    for row in 0..row_count {
        let mut result = String::new();
        for value in values {
            let value = as_utf8(value)?;
            if !value.is_null(row) {
                result.push_str(value.value(row));
            }
        }
        builder.append_value(result);
    }
    Ok(builder.finish())
}

fn execute_left(input: &StringArray, count: &TypedArray) -> Result<StringArray, RuntimeError> {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) || typed_array_is_null(count, row) {
            builder.append_null();
            continue;
        }
        let count = integral_value_at(count, row)?.unwrap_or(0);
        builder.append_value(string_left(input.value(row), count));
    }
    Ok(builder.finish())
}

fn execute_right(input: &StringArray, count: &TypedArray) -> Result<StringArray, RuntimeError> {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) || typed_array_is_null(count, row) {
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
        if input.is_null(row) || typed_array_is_null(count, row) {
            builder.append_null();
            continue;
        }
        let count = integral_value_at(count, row)?.unwrap_or(0);
        let repeat = usize::try_from(count.max(0)).unwrap_or(usize::MAX);
        builder.append_value(input.value(row).repeat(repeat));
    }
    Ok(builder.finish())
}

fn execute_lpad(
    input: &StringArray,
    length: &TypedArray,
    fill: &StringArray,
) -> Result<StringArray, RuntimeError> {
    execute_pad(input, length, fill, true)
}

fn execute_rpad(
    input: &StringArray,
    length: &TypedArray,
    fill: &StringArray,
) -> Result<StringArray, RuntimeError> {
    execute_pad(input, length, fill, false)
}

fn execute_pad(
    input: &StringArray,
    length: &TypedArray,
    fill: &StringArray,
    pad_left: bool,
) -> Result<StringArray, RuntimeError> {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) || typed_array_is_null(length, row) || fill.is_null(row) {
            builder.append_null();
            continue;
        }
        let target_len = integral_value_at(length, row)?.unwrap_or(0).max(0) as usize;
        let source = input.value(row);
        let fill = fill.value(row);
        let source_len = source.chars().count();
        if target_len == 0 {
            builder.append_value("");
            continue;
        }
        if source_len >= target_len {
            builder.append_value(source.chars().take(target_len).collect::<String>());
            continue;
        }
        if fill.is_empty() {
            builder.append_value(source);
            continue;
        }
        let mut padding = String::new();
        while padding.chars().count() + source_len < target_len {
            padding.push_str(fill);
        }
        let missing = target_len - source_len;
        let padding = padding.chars().take(missing).collect::<String>();
        if pad_left {
            builder.append_value(format!("{padding}{source}"));
        } else {
            builder.append_value(format!("{source}{padding}"));
        }
    }
    Ok(builder.finish())
}

fn execute_md5(input: &StringArray) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(format!("{:x}", md5::compute(input.value(row))));
        }
    }
    builder.finish()
}

fn execute_log(
    values: &[TypedArray],
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    let mut builder = Float64Builder::new();
    for row in 0..values[0].len() {
        let value = numeric_value_as_f64(&values[values.len() - 1], row)?;
        let base = if values.len() == 2 {
            numeric_value_as_f64(&values[0], row)?
        } else {
            Some(10.0)
        };
        let (Some(value), Some(base)) = (value, base) else {
            builder.append_null();
            continue;
        };
        let output = if values.len() == 2 {
            value.log(base)
        } else {
            value.log10()
        };
        if output.is_finite() {
            builder.append_value(output);
        } else {
            builder.append_null();
            push_error(
                row_errors,
                row,
                ErrorCode::InvalidArgument,
                "log produced a non-finite result",
                span,
            );
        }
    }
    Ok(TypedArray::Float64(builder.finish()))
}

fn execute_pow(
    left: &TypedArray,
    right: &TypedArray,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    let mut builder = Float64Builder::new();
    for row in 0..left.len() {
        let Some(left) = numeric_value_as_f64(left, row)? else {
            builder.append_null();
            continue;
        };
        let Some(right) = numeric_value_as_f64(right, row)? else {
            builder.append_null();
            continue;
        };
        let output = left.powf(right);
        if output.is_finite() {
            builder.append_value(output);
        } else {
            builder.append_null();
            push_error(
                row_errors,
                row,
                ErrorCode::InvalidArgument,
                "pow produced a non-finite result",
                span,
            );
        }
    }
    Ok(TypedArray::Float64(builder.finish()))
}

fn execute_regexp_like(
    input: &StringArray,
    pattern: &StringArray,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> BooleanArray {
    let mut builder = BooleanBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) || pattern.is_null(row) {
            builder.append_null();
            continue;
        }
        match Regex::new(pattern.value(row)) {
            Ok(regex) => builder.append_value(regex.is_match(input.value(row))),
            Err(error) => {
                builder.append_null();
                push_error(
                    row_errors,
                    row,
                    ErrorCode::InvalidArgument,
                    &format!("invalid regular expression: {error}"),
                    span,
                );
            }
        }
    }
    builder.finish()
}

fn execute_regexp_replace(
    input: &StringArray,
    pattern: &StringArray,
    replacement: &StringArray,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) || pattern.is_null(row) || replacement.is_null(row) {
            builder.append_null();
            continue;
        }
        match Regex::new(pattern.value(row)) {
            Ok(regex) => {
                let value = regex
                    .replace_all(input.value(row), replacement.value(row))
                    .into_owned();
                builder.append_value(value);
            }
            Err(error) => {
                builder.append_null();
                push_error(
                    row_errors,
                    row,
                    ErrorCode::InvalidArgument,
                    &format!("invalid regular expression: {error}"),
                    span,
                );
            }
        }
    }
    builder.finish()
}

fn execute_regexp_substr(
    input: &StringArray,
    pattern: &StringArray,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) || pattern.is_null(row) {
            builder.append_null();
            continue;
        }
        match Regex::new(pattern.value(row)) {
            Ok(regex) => match regex.find(input.value(row)) {
                Some(matched) => builder.append_value(matched.as_str()),
                None => builder.append_null(),
            },
            Err(error) => {
                builder.append_null();
                push_error(
                    row_errors,
                    row,
                    ErrorCode::InvalidArgument,
                    &format!("invalid regular expression: {error}"),
                    span,
                );
            }
        }
    }
    builder.finish()
}

fn execute_replace(input: &StringArray, from: &StringArray, to: &StringArray) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) || from.is_null(row) || to.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(input.value(row).replace(from.value(row), to.value(row)));
        }
    }
    builder.finish()
}

fn execute_reverse(input: &StringArray) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(input.value(row).chars().rev().collect::<String>());
        }
    }
    builder.finish()
}

fn execute_split_part(
    input: &StringArray,
    delimiter: &StringArray,
    index: &TypedArray,
) -> Result<StringArray, RuntimeError> {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) || delimiter.is_null(row) || typed_array_is_null(index, row) {
            builder.append_null();
            continue;
        }
        let index = integral_value_at(index, row)?.unwrap_or(0);
        if index <= 0 {
            builder.append_value("");
            continue;
        }
        let string = input.value(row);
        let delimiter = delimiter.value(row);
        if delimiter.is_empty() {
            builder.append_value(if index == 1 { string } else { "" });
            continue;
        }
        let value = string
            .split(delimiter)
            .nth((index - 1) as usize)
            .unwrap_or("");
        builder.append_value(value);
    }
    Ok(builder.finish())
}

fn execute_strpos(input: &StringArray, needle: &StringArray) -> Int64Array {
    let mut builder = Int64Builder::new();
    for row in 0..input.len() {
        if input.is_null(row) || needle.is_null(row) {
            builder.append_null();
            continue;
        }
        let value = if let Some(byte_idx) = input.value(row).find(needle.value(row)) {
            (input.value(row)[..byte_idx].chars().count() as i64) + 1
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
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row)
            || typed_array_is_null(start, row)
            || length.is_some_and(|value| typed_array_is_null(value, row))
        {
            builder.append_null();
            continue;
        }
        let start = integral_value_at(start, row)?.unwrap_or(1);
        let length = match length {
            Some(value) => Some(integral_value_at(value, row)?.unwrap_or(0)),
            None => None,
        };
        let source = input.value(row).chars().collect::<Vec<_>>();
        let begin = usize::try_from(start.saturating_sub(1).max(0)).unwrap_or(usize::MAX);
        if begin >= source.len() {
            builder.append_value("");
            continue;
        }
        let end = match length {
            Some(value) if value <= 0 => begin,
            Some(value) => begin.saturating_add(value as usize).min(source.len()),
            None => source.len(),
        };
        builder.append_value(source[begin..end].iter().collect::<String>());
    }
    Ok(builder.finish())
}

fn execute_to_hex(input: &TypedArray) -> Result<StringArray, RuntimeError> {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        match input {
            TypedArray::UInt8(array) => {
                append_hex(&mut builder, array.is_null(row), array.value(row) as u64)
            }
            TypedArray::Int8(array) => append_hex(
                &mut builder,
                array.is_null(row),
                array.value(row) as u8 as u64,
            ),
            TypedArray::UInt16(array) => {
                append_hex(&mut builder, array.is_null(row), array.value(row) as u64)
            }
            TypedArray::Int16(array) => append_hex(
                &mut builder,
                array.is_null(row),
                array.value(row) as u16 as u64,
            ),
            TypedArray::UInt32(array) => {
                append_hex(&mut builder, array.is_null(row), array.value(row) as u64)
            }
            TypedArray::Int32(array) => append_hex(
                &mut builder,
                array.is_null(row),
                array.value(row) as u32 as u64,
            ),
            TypedArray::UInt64(array) => {
                append_hex(&mut builder, array.is_null(row), array.value(row))
            }
            TypedArray::Int64(array) => {
                append_hex(&mut builder, array.is_null(row), array.value(row) as u64)
            }
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
        }
    }
    Ok(builder.finish())
}

fn execute_translate(input: &StringArray, from: &StringArray, to: &StringArray) -> StringArray {
    let mut builder = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) || from.is_null(row) || to.is_null(row) {
            builder.append_null();
            continue;
        }
        let source = input.value(row);
        let from_chars = from.value(row).chars().collect::<Vec<_>>();
        let to_chars = to.value(row).chars().collect::<Vec<_>>();
        let mut translated = String::new();
        for ch in source.chars() {
            if let Some(index) = from_chars.iter().position(|candidate| *candidate == ch) {
                if let Some(replacement) = to_chars.get(index) {
                    translated.push(*replacement);
                }
            } else {
                translated.push(ch);
            }
        }
        builder.append_value(translated);
    }
    builder.finish()
}

fn append_hex(builder: &mut StringBuilder, is_null: bool, value: u64) {
    if is_null {
        builder.append_null();
    } else {
        builder.append_value(format!("{value:x}"));
    }
}

fn numeric_value_as_f64(input: &TypedArray, row: usize) -> Result<Option<f64>, RuntimeError> {
    match input {
        TypedArray::UInt8(array) => Ok((!array.is_null(row)).then(|| array.value(row) as f64)),
        TypedArray::Int8(array) => Ok((!array.is_null(row)).then(|| array.value(row) as f64)),
        TypedArray::UInt16(array) => Ok((!array.is_null(row)).then(|| array.value(row) as f64)),
        TypedArray::Int16(array) => Ok((!array.is_null(row)).then(|| array.value(row) as f64)),
        TypedArray::UInt32(array) => Ok((!array.is_null(row)).then(|| array.value(row) as f64)),
        TypedArray::Int32(array) => Ok((!array.is_null(row)).then(|| array.value(row) as f64)),
        TypedArray::UInt64(array) => Ok((!array.is_null(row)).then(|| array.value(row) as f64)),
        TypedArray::Int64(array) => Ok((!array.is_null(row)).then(|| array.value(row) as f64)),
        TypedArray::Float32(array) => Ok((!array.is_null(row)).then(|| array.value(row) as f64)),
        TypedArray::Float64(array) => Ok((!array.is_null(row)).then(|| array.value(row))),
        TypedArray::Boolean(_)
        | TypedArray::Utf8(_)
        | TypedArray::Datetime(_)
        | TypedArray::Generic(_)
        | TypedArray::Uninitialized { .. } => Err(RuntimeError::InvalidBatch {
            message: format!(
                "numeric builtin requires numeric input, found {:?}",
                input.data_type()
            ),
        }),
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

fn string_left(value: &str, count: i64) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if count >= 0 {
        chars.into_iter().take(count as usize).collect()
    } else {
        let keep = chars.len().saturating_sub(count.unsigned_abs() as usize);
        chars.into_iter().take(keep).collect()
    }
}

fn string_right(value: &str, count: i64) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if count >= 0 {
        let keep = count as usize;
        let start = chars.len().saturating_sub(keep);
        chars.into_iter().skip(start).collect()
    } else {
        chars
            .into_iter()
            .skip(count.unsigned_abs() as usize)
            .collect()
    }
}

#[derive(Clone, Debug)]
enum CastScalar {
    UInt(u64),
    Int(i64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    String(String),
    Datetime(i64),
}

fn cast_typed_array(
    input: TypedArray,
    target: RegisterType,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    if input.data_type() == target.data_type() {
        return Ok(input);
    }

    if cast_requires_custom_semantics(&input, target) {
        return cast_typed_array_fallback(input, target, row_errors, span);
    }

    let input_ref = typed_array_to_array_ref(input.clone());
    let cast_options = CastOptions {
        safe: true,
        ..CastOptions::default()
    };
    if let Ok(output) = cast_with_options(input_ref.as_ref(), &target.data_type(), &cast_options) {
        let output = array_ref_to_typed_array(output)?;
        annotate_cast_failures(&input, &output, row_errors, span);
        return Ok(output);
    }

    cast_typed_array_fallback(input, target, row_errors, span)
}

fn cast_requires_custom_semantics(input: &TypedArray, target: RegisterType) -> bool {
    if target == RegisterType::Utf8 {
        return true;
    }

    matches!(
        (input, target),
        (TypedArray::Utf8(_), RegisterType::Datetime)
    )
}

fn annotate_cast_failures(
    input: &TypedArray,
    output: &TypedArray,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) {
    for row in 0..row_errors.len() {
        if !typed_array_is_null(input, row) && typed_array_is_null(output, row) {
            push_error(
                row_errors,
                row,
                ErrorCode::CastFailed,
                &format!("cannot cast value to {}", output.data_type()),
                span,
            );
        }
    }
}

fn cast_typed_array_fallback(
    input: TypedArray,
    target: RegisterType,
    row_errors: &mut [Vec<SideError>],
    span: Span,
) -> Result<TypedArray, RuntimeError> {
    if input.data_type() == target.data_type() {
        return Ok(input);
    }

    let values = cast_scalars(input);

    macro_rules! build_cast_array {
        ($builder:ty, $variant:ident, $message:literal, $convert:expr) => {{
            let mut builder = <$builder>::new();
            for (row, value) in values.iter().enumerate() {
                match value {
                    None => builder.append_null(),
                    Some(value) => match $convert(value) {
                        Some(value) => builder.append_value(value),
                        None => {
                            builder.append_null();
                            push_error(row_errors, row, ErrorCode::CastFailed, $message, span);
                        }
                    },
                }
            }
            Ok(TypedArray::$variant(builder.finish()))
        }};
    }

    match target {
        RegisterType::UInt8 => build_cast_array!(
            UInt8Builder,
            UInt8,
            "cannot cast value to UInt8",
            cast_scalar_to_u8
        ),
        RegisterType::Int8 => build_cast_array!(
            Int8Builder,
            Int8,
            "cannot cast value to Int8",
            cast_scalar_to_i8
        ),
        RegisterType::UInt16 => build_cast_array!(
            UInt16Builder,
            UInt16,
            "cannot cast value to UInt16",
            cast_scalar_to_u16
        ),
        RegisterType::Int16 => build_cast_array!(
            Int16Builder,
            Int16,
            "cannot cast value to Int16",
            cast_scalar_to_i16
        ),
        RegisterType::UInt32 => build_cast_array!(
            UInt32Builder,
            UInt32,
            "cannot cast value to UInt32",
            cast_scalar_to_u32
        ),
        RegisterType::Int32 => build_cast_array!(
            Int32Builder,
            Int32,
            "cannot cast value to Int32",
            cast_scalar_to_i32
        ),
        RegisterType::UInt64 => build_cast_array!(
            UInt64Builder,
            UInt64,
            "cannot cast value to UInt64",
            cast_scalar_to_u64
        ),
        RegisterType::Int64 => build_cast_array!(
            Int64Builder,
            Int64,
            "cannot cast value to Int64",
            cast_scalar_to_i64
        ),
        RegisterType::Float32 => build_cast_array!(
            Float32Builder,
            Float32,
            "cannot cast value to Float32",
            cast_scalar_to_f32
        ),
        RegisterType::Float64 => build_cast_array!(
            Float64Builder,
            Float64,
            "cannot cast value to Float64",
            cast_scalar_to_f64
        ),
        RegisterType::Boolean => build_cast_array!(
            BooleanBuilder,
            Boolean,
            "cannot cast value to Boolean",
            cast_scalar_to_bool
        ),
        RegisterType::Utf8 => {
            let mut builder = StringBuilder::new();
            for (row, value) in values.iter().enumerate() {
                match value {
                    None => builder.append_null(),
                    Some(value) => match cast_scalar_to_utf8(value) {
                        Some(value) => builder.append_value(value),
                        None => {
                            builder.append_null();
                            push_error(
                                row_errors,
                                row,
                                ErrorCode::CastFailed,
                                "cannot cast value to Utf8",
                                span,
                            );
                        }
                    },
                }
            }
            Ok(TypedArray::Utf8(builder.finish()))
        }
        RegisterType::Datetime => {
            let mut builder = TimestampNanosecondBuilder::new();
            for (row, value) in values.iter().enumerate() {
                match value {
                    None => builder.append_null(),
                    Some(value) => match cast_scalar_to_datetime(value) {
                        Some(value) => builder.append_value(value),
                        None => {
                            builder.append_null();
                            push_error(
                                row_errors,
                                row,
                                ErrorCode::CastFailed,
                                "cannot cast value to Datetime",
                                span,
                            );
                        }
                    },
                }
            }
            Ok(TypedArray::Datetime(builder.finish().with_timezone_utc()))
        }
        RegisterType::Generic => Err(RuntimeError::InvalidBatch {
            message: "casts to generic Arrow arrays are not supported".to_string(),
        }),
    }
}

fn cast_scalars(input: TypedArray) -> Vec<Option<CastScalar>> {
    match input {
        TypedArray::UInt8(values) => values
            .iter()
            .map(|v| v.map(|v| CastScalar::UInt(v as u64)))
            .collect(),
        TypedArray::Int8(values) => values
            .iter()
            .map(|v| v.map(|v| CastScalar::Int(v as i64)))
            .collect(),
        TypedArray::UInt16(values) => values
            .iter()
            .map(|v| v.map(|v| CastScalar::UInt(v as u64)))
            .collect(),
        TypedArray::Int16(values) => values
            .iter()
            .map(|v| v.map(|v| CastScalar::Int(v as i64)))
            .collect(),
        TypedArray::UInt32(values) => values
            .iter()
            .map(|v| v.map(|v| CastScalar::UInt(v as u64)))
            .collect(),
        TypedArray::Int32(values) => values
            .iter()
            .map(|v| v.map(|v| CastScalar::Int(v as i64)))
            .collect(),
        TypedArray::UInt64(values) => values.iter().map(|v| v.map(CastScalar::UInt)).collect(),
        TypedArray::Int64(values) => values.iter().map(|v| v.map(CastScalar::Int)).collect(),
        TypedArray::Float32(values) => values.iter().map(|v| v.map(CastScalar::Float32)).collect(),
        TypedArray::Float64(values) => values.iter().map(|v| v.map(CastScalar::Float64)).collect(),
        TypedArray::Boolean(values) => values.iter().map(|v| v.map(CastScalar::Bool)).collect(),
        TypedArray::Utf8(values) => values
            .iter()
            .map(|v| v.map(|v| CastScalar::String(v.to_string())))
            .collect(),
        TypedArray::Datetime(values) => {
            values.iter().map(|v| v.map(CastScalar::Datetime)).collect()
        }
        TypedArray::Generic(_) => Vec::new(),
        TypedArray::Uninitialized { len, .. } => vec![None; len],
    }
}

fn cast_float_to_int<T>(value: f64) -> Option<T>
where
    T: TryFrom<i128>,
{
    if value.is_finite() && value >= i128::MIN as f64 && value <= i128::MAX as f64 {
        T::try_from(value.trunc() as i128).ok()
    } else {
        None
    }
}

macro_rules! define_scalar_int_casts {
    ($(($fn_name:ident, $target_ty:ty));+ $(;)?) => {
        $(
            fn $fn_name(value: &CastScalar) -> Option<$target_ty> {
                match value {
                    CastScalar::UInt(value) => <$target_ty>::try_from(*value).ok(),
                    CastScalar::Int(value) => <$target_ty>::try_from(*value).ok(),
                    CastScalar::Float32(value) => cast_float_to_int::<$target_ty>(*value as f64),
                    CastScalar::Float64(value) => cast_float_to_int::<$target_ty>(*value),
                    CastScalar::Bool(value) => <$target_ty>::try_from(u8::from(*value)).ok(),
                    CastScalar::String(value) => value.parse::<$target_ty>().ok(),
                    CastScalar::Datetime(value) => <$target_ty>::try_from(*value).ok(),
                }
            }
        )+
    };
}

define_scalar_int_casts!(
    (cast_scalar_to_u8, u8);
    (cast_scalar_to_i8, i8);
    (cast_scalar_to_u16, u16);
    (cast_scalar_to_i16, i16);
    (cast_scalar_to_u32, u32);
    (cast_scalar_to_i32, i32);
    (cast_scalar_to_u64, u64);
    (cast_scalar_to_i64, i64)
);

macro_rules! define_scalar_float_casts {
    ($(($fn_name:ident, $target_ty:ty));+ $(;)?) => {
        $(
            fn $fn_name(value: &CastScalar) -> Option<$target_ty> {
                let value = match value {
                    CastScalar::UInt(value) => *value as $target_ty,
                    CastScalar::Int(value) => *value as $target_ty,
                    CastScalar::Float32(value) => *value as $target_ty,
                    CastScalar::Float64(value) => *value as $target_ty,
                    CastScalar::Bool(value) => if *value { 1.0 } else { 0.0 },
                    CastScalar::String(value) => value.parse::<$target_ty>().ok()?,
                    CastScalar::Datetime(value) => *value as $target_ty,
                };
                value.is_finite().then_some(value)
            }
        )+
    };
}

define_scalar_float_casts!(
    (cast_scalar_to_f32, f32);
    (cast_scalar_to_f64, f64)
);

fn cast_scalar_to_bool(value: &CastScalar) -> Option<bool> {
    match value {
        CastScalar::UInt(0) | CastScalar::Int(0) => Some(false),
        CastScalar::UInt(1) | CastScalar::Int(1) => Some(true),
        CastScalar::Float32(value) if *value == 0.0 => Some(false),
        CastScalar::Float32(value) if *value == 1.0 => Some(true),
        CastScalar::Float64(value) if *value == 0.0 => Some(false),
        CastScalar::Float64(value) if *value == 1.0 => Some(true),
        CastScalar::Bool(value) => Some(*value),
        CastScalar::String(value) => match value.to_ascii_lowercase().as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        CastScalar::Datetime(_) => None,
        _ => None,
    }
}

fn cast_scalar_to_utf8(value: &CastScalar) -> Option<String> {
    match value {
        CastScalar::UInt(value) => Some(value.to_string()),
        CastScalar::Int(value) => Some(value.to_string()),
        CastScalar::Float32(value) => Some(value.to_string()),
        CastScalar::Float64(value) => Some(value.to_string()),
        CastScalar::Bool(value) => Some(value.to_string()),
        CastScalar::String(value) => Some(value.clone()),
        CastScalar::Datetime(value) => Some(DateTime::from_timestamp_nanos(*value).to_rfc3339()),
    }
}

fn cast_scalar_to_datetime(value: &CastScalar) -> Option<i64> {
    match value {
        CastScalar::UInt(value) => i64::try_from(*value).ok(),
        CastScalar::Int(value) => Some(*value),
        CastScalar::String(value) => DateTime::parse_from_rfc3339(value)
            .ok()
            .and_then(|value| value.timestamp_nanos_opt()),
        CastScalar::Datetime(value) => Some(*value),
        CastScalar::Float32(_) | CastScalar::Float64(_) | CastScalar::Bool(_) => None,
    }
}

fn filter_columns(
    columns: &[TypedArray],
    predicate: &BooleanArray,
) -> Result<Vec<TypedArray>, RuntimeError> {
    let filter = FilterBuilder::new(predicate).optimize().build();
    columns
        .iter()
        .map(|column| {
            if let TypedArray::Uninitialized { data_type, .. } = column {
                return Ok(TypedArray::uninitialized(
                    data_type.clone(),
                    selected_rows(predicate).len(),
                ));
            }
            let filtered = filter
                .filter(typed_array_as_array(column))
                .map_err(|error| arrow_kernel_error("column filter kernel failed", error))?;
            array_ref_to_typed_array(filtered)
        })
        .collect()
}

fn filter_errors(errors: &[Vec<SideError>], predicate: &BooleanArray) -> Vec<Vec<SideError>> {
    errors
        .iter()
        .enumerate()
        .filter(|(row, _)| row_selected(predicate, *row))
        .map(|(_, errors)| errors.clone())
        .collect()
}

fn filter_boolean(values: &BooleanArray, predicate: &BooleanArray) -> BooleanArray {
    let mut builder = BooleanBuilder::new();
    for row in 0..values.len() {
        if row_selected(predicate, row) {
            if values.is_null(row) {
                builder.append_null();
            } else {
                builder.append_value(values.value(row));
            }
        }
    }
    builder.finish()
}

fn selected_rows(predicate: &BooleanArray) -> Vec<usize> {
    predicate
        .iter()
        .enumerate()
        .filter_map(|(index, value)| value.and_then(|keep| keep.then_some(index)))
        .collect()
}

fn row_selected(predicate: &BooleanArray, row: usize) -> bool {
    !predicate.is_null(row) && predicate.value(row)
}

fn compare_values<T: PartialOrd + PartialEq>(left: T, right: T, op: BinaryOp) -> bool {
    match op {
        BinaryOp::Eq => left == right,
        BinaryOp::NotEq => left != right,
        BinaryOp::Gt => left > right,
        BinaryOp::Lt => left < right,
        BinaryOp::GtEq => left >= right,
        BinaryOp::LtEq => left <= right,
        _ => unreachable!("comparison helper only handles comparison operators"),
    }
}

fn push_error(
    row_errors: &mut [Vec<SideError>],
    row: usize,
    code: ErrorCode,
    message: &str,
    span: Span,
) {
    row_errors[row].push(SideError {
        code,
        message: message.to_string(),
        span,
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        StringArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        types::Int64Type,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use nervix_models::Timestamp;
    use nervix_nspl::vm_program::parse_program;
    use uuid::{Uuid, Version};

    use super::*;
    use crate::{CompileBinding, OutputBinding, compile_program_for_bindings};

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

    fn schema(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    fn with_output_fields(input_schema: &Arc<Schema>, fields: Vec<Field>) -> Arc<Schema> {
        let mut output_fields = input_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        output_fields.extend(fields);
        schema(output_fields)
    }

    fn compile_program_with_output_fields(
        program: &nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
        input_schema: Arc<Schema>,
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
        let parsed = parse_program(
            "SET input.div = input.left / input.right, input.parsed = input.raw AS INT64;",
        )
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
        assert!(output.errors()[0].is_empty());
        assert_eq!(output.errors()[1].len(), 2);
        assert_eq!(output.errors()[1][0].span, div_span);
        assert_eq!(output.errors()[1][1].span, cast_span);
    }

    #[test]
    fn executes_null_assignment_to_declared_optional_field() {
        let parsed = parse_program("SET input.maybe = NULL;").expect("must parse");
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
        let parsed =
            parse_program("SET input.value = coalesce(input.value, 1);").expect("must parse");
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
        let parsed = parse_program("SET input.value = input.value;").expect("must parse");
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
        let parsed = parse_program("SET input.total = input.left + input.right WHERE input.keep;")
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
        let parsed = parse_program(
            "SET input.lowered = lower(input.level) WHERE lower(input.level) = \"error\";",
        )
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
        assert_eq!(output.selected_rows, vec![0, 2]);
        assert!(output.branch_selected_rows.is_empty());
        assert_eq!(lowered.value(0), "error");
        assert_eq!(lowered.value(1), "error");
    }

    #[test]
    fn executes_dedicated_builtin_instruction() {
        let parsed = parse_program("SET input.lowered = lower(input.name);").expect("must parse");
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
    fn executes_array_builtins() {
        let values = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![]),
            None,
        ]));
        let fixed = Arc::new(FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
            [
                Some(vec![Some(10), Some(20)]),
                Some(vec![Some(30), Some(40)]),
                Some(vec![Some(50), Some(60)]),
            ],
            2,
        ));
        let parsed = parse_program(
            "SET input.total = sum(input.values), input.first_value = first(input.values), \
             input.last_value = last(input.values), input.second_value = nth(input.values, 1), \
             input.value_count = count(input.values), input.fixed_last = last(input.fixed);",
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
            vec![
                TypedArray::Generic(values as ArrayRef),
                TypedArray::Generic(fixed as ArrayRef),
            ],
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

        assert_eq!(total.value(0), 6);
        assert!(total.is_null(1));
        assert!(total.is_null(2));
        assert_eq!(first_value.value(0), 1);
        assert!(first_value.is_null(1));
        assert!(first_value.is_null(2));
        assert_eq!(last_value.value(0), 3);
        assert!(last_value.is_null(1));
        assert!(last_value.is_null(2));
        assert_eq!(second_value.value(0), 2);
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
        let parsed =
            parse_program("SET input.neg = -input.value, input.lt = input.left < input.right;")
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
        assert_eq!(lt.value(0), true);
        assert_eq!(lt.value(1), false);
        assert_eq!(output.errors()[1].len(), 1);
        assert_eq!(output.errors()[1][0].code, ErrorCode::Overflow);
    }

    #[test]
    fn executes_literals_not_sub_mul_and_null_propagation() {
        let parsed = parse_program(
            "SET input.lit = 41, input.notted = NOT input.flag, input.diff = input.left - \
             input.right, input.product = input.left * input.right;",
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
        assert_eq!(notted.value(0), false);
        assert!(notted.is_null(1));
        assert_eq!(diff.value(0), 4);
        assert!(diff.is_null(1));
        assert_eq!(product.value(0), 21);
        assert!(product.is_null(1));
    }

    #[test]
    fn executes_float_boolean_and_utf8_projection_paths() {
        let parsed = parse_program(
            "SET input.neg = -input.amount, input.total = input.left + input.right, input.cmp = \
             input.left < input.right, input.both = input.on AND input.off, input.uppered = \
             upper(input.name), input.trimmed = trim(input.name), input.len = length(input.name), \
             input.lexical = input.name > input.other;",
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
        assert_eq!(cmp.value(0), true);
        assert_eq!(both.value(0), false);
        assert_eq!(both.value(1), true);
        assert_eq!(uppered.value(0), "  ABC ");
        assert_eq!(trimmed.value(0), "AbC");
        assert_eq!(len.value(0), 6);
        assert_eq!(lexical.value(0), false);
        assert!(uppered.is_null(1));
    }

    #[test]
    fn executes_cast_matrix_and_reports_failures() {
        let parsed = parse_program(
            "SET input.i_from_f = input.flt AS INT64, input.i_from_b = input.flag AS INT64, \
             input.f_from_b = input.flag AS FLOAT64, input.s_from_i = input.num AS STRING, \
             input.s_from_f = input.flt AS STRING, input.s_from_b = input.flag AS STRING, \
             input.b_from_i = input.num AS BOOLEAN, input.b_from_f = input.flt AS BOOLEAN, \
             input.b_from_s = input.txt AS BOOLEAN, input.f_from_s = input.txt AS FLOAT64;",
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
        assert_eq!(b_from_i.value(0), true);
        assert_eq!(b_from_i.value(1), true);
        assert_eq!(b_from_f.value(0), true);
        assert_eq!(b_from_f.value(1), true);
        assert_eq!(b_from_s.value(0), true);
        assert!(b_from_s.is_null(1));
        assert!(f_from_s.is_null(0));
        assert!(f_from_s.value(1).is_nan());
        assert_eq!(output.errors()[0].len(), 1);
        assert_eq!(output.errors()[1].len(), 1);
    }

    #[test]
    fn executes_extended_builtin_instructions() {
        let parsed = parse_program(
            "SET input.chosen = coalesce(input.primary, input.fallback), input.was_null = \
             is_null(input.primary), input.maybe = nullif(input.primary, input.fallback), \
             input.has = contains(input.text, input.needle), input.starts = \
             starts_with(input.text, input.prefix), input.ends = ends_with(input.text, \
             input.suffix);",
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
        assert_eq!(was_null.value(0), true);
        assert_eq!(was_null.value(1), false);
        assert_eq!(was_null.value(2), false);
        assert!(maybe.is_null(0));
        assert!(maybe.is_null(1));
        assert_eq!(maybe.value(2), "keep");
        assert_eq!(has.value(0), true);
        assert_eq!(has.value(1), true);
        assert!(has.is_null(2));
        assert_eq!(starts.value(0), true);
        assert_eq!(starts.value(1), true);
        assert!(starts.is_null(2));
        assert_eq!(ends.value(0), true);
        assert_eq!(ends.value(1), true);
        assert!(ends.is_null(2));
        assert!(output.errors().iter().all(Vec::is_empty));
    }

    #[test]
    fn executes_abs_and_reports_overflow() {
        let parsed = parse_program(
            "SET input.int_abs = abs(input.ints), input.float_abs = abs(input.floats);",
        )
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
        assert_eq!(output.errors()[0].len(), 0);
        assert_eq!(output.errors()[1].len(), 1);
        assert_eq!(output.errors()[1][0].code, ErrorCode::Overflow);
        assert_eq!(output.errors()[1][0].span, int_abs_span);
    }

    #[test]
    fn executes_narrow_numeric_and_float32_paths() {
        let parsed = parse_program(
            "SET input.u8_sum = input.u8 + (1 AS U8), input.i8_abs = abs(input.i8), \
             input.u16_keep = coalesce(input.u16, 0 AS U16), input.u32_same = nullif(input.u32, \
             999 AS U32), input.u64_sum = input.u64 + (2 AS U64), input.f32_sum = input.f32 + \
             (1.5 AS F32), input.f32_text = input.f32 AS STRING;",
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
        assert!(output.errors()[0].is_empty());
    }

    #[test]
    fn executes_numeric_binary_dispatch_for_all_scalar_widths() {
        let parsed = parse_program(
            "SET input.u8_eq = input.u8 = (5 AS U8), input.u16_sum = input.u16 + (2 AS U16), \
             input.i16_rem = input.i16 % (4 AS I16), input.i32_gte = input.i32 >= (9 AS I32), \
             input.u32_product = input.u32 * (3 AS U32), input.u64_lt = input.u64 < (20 AS U64), \
             input.f32_lte = input.f32 <= (1.5 AS F32);",
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

        assert_eq!(u8_eq.value(0), true);
        assert_eq!(u8_eq.value(1), false);
        assert_eq!(u16_sum.value(0), 10);
        assert_eq!(u16_sum.value(1), 11);
        assert_eq!(i16_rem.value(0), 2);
        assert_eq!(i16_rem.value(1), -1);
        assert_eq!(i32_gte.value(0), true);
        assert_eq!(i32_gte.value(1), false);
        assert_eq!(u32_product.value(0), 21);
        assert_eq!(u32_product.value(1), 33);
        assert_eq!(u64_lt.value(0), true);
        assert_eq!(u64_lt.value(1), false);
        assert_eq!(f32_lte.value(0), true);
        assert_eq!(f32_lte.value(1), false);
        assert!(output.errors().iter().all(Vec::is_empty));
    }

    #[test]
    fn executes_datetime_comparisons_and_casts() {
        let parsed = parse_program(
            "SET input.occurred_text = input.occurred_at AS STRING, input.occurred_roundtrip = \
             (input.occurred_at AS STRING) AS DATETIME, input.occurred_nanos = input.occurred_at \
             AS INT64 WHERE input.occurred_at > ('2026-04-07T00:00:00Z' AS DATETIME);",
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
        let expected_occured_nanos = chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z")
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
        assert_eq!(occurred_text.value(0), "2026-04-07T12:34:56+00:00");
        assert_eq!(occurred_roundtrip.value(0), expected_occured_nanos);
        assert_eq!(occurred_nanos.value(0), expected_occured_nanos);
        assert!(output.errors()[0].is_empty());
    }

    #[test]
    fn executes_extended_text_regex_and_contextual_builtins() {
        let parsed = parse_program(
            "SET input.now_value = now(), input.uuid4 = uuid_v4(), input.uuid7 = uuid_v7(), \
             input.bits = bit_length(input.plain), input.ascii_value = ascii(input.plain), \
             input.trimmed = btrim(input.spaced), input.chars = char_length(input.spaced), \
             input.joined = concat(input.prefix, input.fill, input.prefix), input.titled = \
             initcap(input.spaced), input.lefted = left(input.plain, input.count), input.lowered \
             = lower(input.plain), input.lpaded = lpad(input.prefix, input.width, input.fill), \
             input.ltrimmed = ltrim(input.spaced), input.digest = md5(input.prefix), \
             input.repeated = repeat(input.prefix, input.count), input.replaced = \
             replace(input.plain, input.prefix, input.replacement), input.reversed = \
             reverse(input.prefix), input.righted = right(input.plain, input.count), input.rpaded \
             = rpad(input.prefix, input.width, input.fill), input.rtrimmed = rtrim(input.spaced), \
             input.part = split_part(input.dotted, input.delim, input.count), input.starts = \
             starts_with(input.plain, input.prefix), input.pos = strpos(input.plain, \
             input.prefix), input.piece = substr(input.plain, input.start, input.length), \
             input.hexed = to_hex(input.hex_value), input.translated = translate(input.prefix, \
             input.from_chars, input.to_chars), input.trimmed2 = trim(input.spaced), \
             input.uppered = upper(input.prefix), input.regex_ok = regexp_like(input.plain, \
             input.pattern), input.regex_replaced = regexp_replace(input.plain, input.pattern, \
             input.replacement), input.regex_piece = regexp_substr(input.spaced, input.pattern);",
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
            &ExecutionContext { now: context_now },
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
        assert_eq!(starts.value(0), true);
        assert_eq!(pos.value(0), 1);
        assert_eq!(piece.value(0), "ell");
        assert_eq!(hexed.value(0), "ff");
        assert_eq!(translated.value(0), "HE");
        assert_eq!(trimmed2.value(0), "hello.world");
        assert_eq!(uppered.value(0), "HE");
        assert_eq!(regex_ok.value(0), true);
        assert_eq!(regex_replaced.value(0), "XX");
        assert_eq!(regex_piece.value(0), "hello");
        assert!(output.errors()[0].is_empty());
    }

    #[test]
    fn executes_extended_math_builtins() {
        let parsed = parse_program(
            "SET input.absolute = abs(input.int_value), input.acos_value = acos(input.half), \
             input.asin_value = asin(input.half), input.atan_value = atan(input.two), \
             input.ceil_value = ceil(input.neg_float), input.cos_value = cos(input.half), \
             input.exp_value = exp(input.one), input.floor_value = floor(input.neg_float), \
             input.ln_value = ln(input.two), input.log_value = log(input.hundred), \
             input.log_base_value = log(input.two, input.hundred), input.pow_value = \
             pow(input.two, input.three), input.round_value = round(input.round_me), \
             input.sqrt_value = sqrt(input.nine), input.tan_value = tan(input.half);",
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
        assert!(output.errors()[0].is_empty());
    }

    #[test]
    fn rejects_mismatched_runtime_register_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )]));
        let float_input = RegisterRef::new(RegisterSpace::Input, RegisterType::Float64, 0);
        let utf8_output = RegisterRef::new(RegisterSpace::Output, RegisterType::Utf8, 0);
        let program = CompiledProgram {
            input_schema: schema.clone(),
            output_schema: Arc::new(Schema::new(vec![Field::new(
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
            }],
            filter: None,
            branch_filters: Vec::new(),
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("left", DataType::Int64, true),
            Field::new("right", DataType::Int64, true),
        ]));
        let left = RegisterRef::new(RegisterSpace::Input, RegisterType::Int64, 0);
        let right = RegisterRef::new(RegisterSpace::Input, RegisterType::Int64, 1);
        let utf8_output = RegisterRef::new(RegisterSpace::Output, RegisterType::Utf8, 0);
        let program = CompiledProgram {
            input_schema: schema.clone(),
            output_schema: Arc::new(Schema::new(vec![Field::new("bad", DataType::Utf8, true)])),
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
            }],
            filter: None,
            branch_filters: Vec::new(),
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
}
