use std::{
    cell::RefCell,
    fmt,
    panic::AssertUnwindSafe,
    sync::Arc as StdArc,
    time::{Duration, Instant},
};

use ahash::{HashMap, HashMapExt};
use arrow_arith::boolean;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Datum, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array, new_null_array,
    types::{
        ArrowPrimitiveType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::{DataType, Field, TimeUnit};
use arrow_select::{nullif::nullif, zip::zip};
use nervix_models::{CreateUdf, ParseAsType, Timestamp};
use nervix_vm::{
    ErrorCode, FunctionExecutionPolicy, FunctionInjector, InjectedResult, RuntimeError, SideError,
    TypedArray, UdfParameter, UdfSignature, UdfSignatures,
};
use parking_lot::Mutex;
use regex::Regex;
use roto::{FileTree, NoCtx, RotoString, Runtime, TypedFunc, Val, library};
use thiserror::Error;
use triomphe::Arc;

const DEFAULT_WATCHDOG: Duration = Duration::from_secs(5);
const COMPILE_TEST_BUDGET: Duration = Duration::from_secs(10);
const RESERVED_PREFIX: &str = "__nervix_";

#[derive(Debug, Error)]
pub enum UdfError {
    #[error("failed to initialize the ROTO_0_11 runtime: {0}")]
    RuntimeRegistration(String),
    #[error("UDF '{name}' uses reserved identifier prefix '__nervix_'")]
    ReservedIdentifier { name: String },
    #[error("{function}() requires VOLATILE")]
    VolatileRequired { function: &'static str },
    #[error("Roto compilation failed:\n{diagnostics}")]
    Compile { diagnostics: String },
    #[error("Roto test block failed")]
    TestsFailed,
    #[error("Roto compile and test budget of {limit:?} was exceeded")]
    CompileBudgetExceeded { limit: Duration },
    #[error("Roto compilation task failed")]
    CompileTask(#[source] tokio::task::JoinError),
    #[error("Roto entry signature is invalid: {0}")]
    Signature(String),
}

#[derive(Clone)]
struct Column(ArrayRef);

impl fmt::Debug for Column {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Column")
            .field("data_type", self.0.data_type())
            .field("len", &self.0.len())
            .finish()
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        StdArc::ptr_eq(&self.0, &other.0)
    }
}

macro_rules! column_type {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq)]
        struct $name(Column);
    };
}

column_type!(U8Column);
column_type!(I8Column);
column_type!(U16Column);
column_type!(I16Column);
column_type!(U32Column);
column_type!(I32Column);
column_type!(U64Column);
column_type!(I64Column);
column_type!(F32Column);
column_type!(F64Column);
column_type!(BoolColumn);
column_type!(StringColumn);
column_type!(DatetimeColumn);
column_type!(VecU8Column);
column_type!(VecI8Column);
column_type!(VecU16Column);
column_type!(VecI16Column);
column_type!(VecU32Column);
column_type!(VecI32Column);
column_type!(VecU64Column);
column_type!(VecI64Column);
column_type!(VecF32Column);
column_type!(VecF64Column);
column_type!(VecBoolColumn);
column_type!(VecStringColumn);
column_type!(VecDatetimeColumn);
column_type!(AnyColumn);

#[derive(Clone, PartialEq)]
struct StringWhen {
    arms: Vec<(BoolColumn, RotoString)>,
}

impl fmt::Debug for StringWhen {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StringWhen")
            .field("arms", &self.arms.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ColumnBuilderFactory;

#[derive(Clone)]
struct BoolColumnBuilder(Arc<Mutex<Vec<Option<bool>>>>);

impl fmt::Debug for BoolColumnBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("BoolColumnBuilder").finish()
    }
}

impl PartialEq for BoolColumnBuilder {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone, Debug)]
struct UdfArgs(Vec<ArrayRef>);

impl PartialEq for UdfArgs {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(&other.0)
                .all(|(left, right)| StdArc::ptr_eq(left, right))
    }
}

#[derive(Debug)]
struct CallState {
    udf_name: String,
    span: nervix_nspl::vm_program::Span,
    row_count: usize,
    now: Timestamp,
    side_errors: Vec<(usize, SideError)>,
    fatal: Option<String>,
}

thread_local! {
    static CALL_STATE: RefCell<Option<CallState>> = const { RefCell::new(None) };
}

fn with_state<R>(operation: impl FnOnce(&mut CallState) -> R) -> Option<R> {
    CALL_STATE.with(|state| state.borrow_mut().as_mut().map(operation))
}

fn fatal(message: impl Into<String>) {
    let message = message.into();
    with_state(|state| {
        if state.fatal.is_none() {
            state.fatal = Some(message);
        }
    });
}

fn side_error(row: usize, code: ErrorCode, operation: &str, detail: &str) {
    with_state(|state| {
        state.side_errors.push((
            row,
            SideError {
                code,
                message: format!("UDF '{}': {operation} failed: {detail}", state.udf_name),
                span: state.span,
            },
        ));
    });
}

fn row_count() -> usize {
    with_state(|state| state.row_count).unwrap_or(0)
}

fn call_timestamp() -> Timestamp {
    with_state(|state| state.now).unwrap_or_else(Timestamp::now)
}

fn column_arg(args: Val<UdfArgs>, index: u64, expected: &DataType) -> Column {
    let Some(column) = args.0.0.get(index as usize) else {
        fatal(format!(
            "generated UDF bridge requested missing argument {index}"
        ));
        return Column(new_null_array(expected, row_count()));
    };
    if column.data_type() != expected {
        fatal(format!(
            "generated UDF bridge expected argument {index} to be {expected:?}, found {:?}",
            column.data_type()
        ));
        return Column(new_null_array(expected, column.len()));
    }
    Column(column.clone())
}

fn untyped_column_arg(args: Val<UdfArgs>, index: u64) -> Column {
    let Some(column) = args.0.0.get(index as usize) else {
        fatal(format!(
            "generated UDF bridge requested missing argument {index}"
        ));
        return Column(new_null_array(&DataType::Null, row_count()));
    };
    Column(column.clone())
}

fn primitive<T: ArrowPrimitiveType>(column: &Column) -> &arrow_array::PrimitiveArray<T> {
    column
        .0
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
        .expect("column type is validated at the generated bridge")
}

fn numeric_binary<T>(
    left: &Column,
    right: &Column,
    operation: &str,
    calculate: impl Fn(T::Native, T::Native) -> Option<T::Native>,
) -> Column
where
    T: ArrowPrimitiveType,
    arrow_array::PrimitiveArray<T>: FromIterator<Option<T::Native>>,
{
    let left = primitive::<T>(left);
    let right = primitive::<T>(right);
    if left.len() != right.len() {
        fatal(format!(
            "{operation} received columns with {} and {} rows",
            left.len(),
            right.len()
        ));
        return Column(new_null_array(left.data_type(), left.len()));
    }
    let output = left
        .iter()
        .zip(right.iter())
        .enumerate()
        .map(|(row, values)| match values {
            (Some(left), Some(right)) => calculate(left, right).or_else(|| {
                let (code, detail) = if operation == "div" {
                    (
                        ErrorCode::DivisionByZero,
                        "division by zero or signed overflow",
                    )
                } else {
                    (ErrorCode::Overflow, "numeric overflow")
                };
                side_error(row, code, operation, detail);
                None
            }),
            _ => None,
        })
        .collect::<arrow_array::PrimitiveArray<T>>();
    Column(StdArc::new(output))
}

fn numeric_scalar<T>(
    left: &Column,
    right: T::Native,
    operation: &str,
    calculate: impl Fn(T::Native, T::Native) -> Option<T::Native>,
) -> Column
where
    T: ArrowPrimitiveType,
    arrow_array::PrimitiveArray<T>: FromIterator<Option<T::Native>>,
    T::Native: Copy,
{
    let left = primitive::<T>(left);
    let output = left
        .iter()
        .enumerate()
        .map(|(row, value)| {
            value.and_then(|left| {
                calculate(left, right).or_else(|| {
                    let (code, detail) = if operation == "div" {
                        (
                            ErrorCode::DivisionByZero,
                            "division by zero or signed overflow",
                        )
                    } else {
                        (ErrorCode::Overflow, "numeric overflow")
                    };
                    side_error(row, code, operation, detail);
                    None
                })
            })
        })
        .collect::<arrow_array::PrimitiveArray<T>>();
    Column(StdArc::new(output))
}

fn numeric_compare<T>(
    left: &Column,
    right: &Column,
    operation: &str,
    compare: impl Fn(T::Native, T::Native) -> bool,
) -> BoolColumn
where
    T: ArrowPrimitiveType,
{
    let left = primitive::<T>(left);
    let right = primitive::<T>(right);
    if left.len() != right.len() {
        fatal(format!(
            "{operation} received columns with {} and {} rows",
            left.len(),
            right.len()
        ));
        return BoolColumn(Column(StdArc::new(BooleanArray::new_null(left.len()))));
    }
    BoolColumn(Column(StdArc::new(BooleanArray::from_iter(
        left.iter().zip(right.iter()).map(|values| match values {
            (Some(left), Some(right)) => Some(compare(left, right)),
            _ => None,
        }),
    ))))
}

fn numeric_compare_scalar<T>(
    left: &Column,
    right: T::Native,
    compare: impl Fn(T::Native, T::Native) -> bool,
) -> BoolColumn
where
    T: ArrowPrimitiveType,
    T::Native: Copy,
{
    let left = primitive::<T>(left);
    BoolColumn(Column(StdArc::new(BooleanArray::from_iter(
        left.iter()
            .map(|left| left.map(|left| compare(left, right))),
    ))))
}

macro_rules! integer_column_library {
    ($wrapper:ident, $arrow:ty, $scalar:ty) => {
        library! {
            impl Val<$wrapper> {
                fn len(value: Val<$wrapper>) -> u64 { value.0.0.0.len() as u64 }
                fn add(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<$wrapper> {
                    Val($wrapper(numeric_binary::<$arrow>(&value.0.0, &other.0.0, "add", <$scalar>::checked_add)))
                }
                fn add_s(value: Val<$wrapper>, other: $scalar) -> Val<$wrapper> {
                    Val($wrapper(numeric_scalar::<$arrow>(&value.0.0, other, "add", <$scalar>::checked_add)))
                }
                fn sub(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<$wrapper> {
                    Val($wrapper(numeric_binary::<$arrow>(&value.0.0, &other.0.0, "sub", <$scalar>::checked_sub)))
                }
                fn sub_s(value: Val<$wrapper>, other: $scalar) -> Val<$wrapper> {
                    Val($wrapper(numeric_scalar::<$arrow>(&value.0.0, other, "sub", <$scalar>::checked_sub)))
                }
                fn mul(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<$wrapper> {
                    Val($wrapper(numeric_binary::<$arrow>(&value.0.0, &other.0.0, "mul", <$scalar>::checked_mul)))
                }
                fn mul_s(value: Val<$wrapper>, other: $scalar) -> Val<$wrapper> {
                    Val($wrapper(numeric_scalar::<$arrow>(&value.0.0, other, "mul", <$scalar>::checked_mul)))
                }
                fn div(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<$wrapper> {
                    Val($wrapper(numeric_binary::<$arrow>(&value.0.0, &other.0.0, "div", <$scalar>::checked_div)))
                }
                fn div_s(value: Val<$wrapper>, other: $scalar) -> Val<$wrapper> {
                    Val($wrapper(numeric_scalar::<$arrow>(&value.0.0, other, "div", <$scalar>::checked_div)))
                }
                fn eq(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<BoolColumn> {
                    Val(numeric_compare::<$arrow>(&value.0.0, &other.0.0, "eq", |a, b| a == b))
                }
                fn eq_s(value: Val<$wrapper>, other: $scalar) -> Val<BoolColumn> {
                    Val(numeric_compare_scalar::<$arrow>(&value.0.0, other, |a, b| a == b))
                }
                fn lt(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<BoolColumn> {
                    Val(numeric_compare::<$arrow>(&value.0.0, &other.0.0, "lt", |a, b| a < b))
                }
                fn lt_s(value: Val<$wrapper>, other: $scalar) -> Val<BoolColumn> {
                    Val(numeric_compare_scalar::<$arrow>(&value.0.0, other, |a, b| a < b))
                }
                fn gt(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<BoolColumn> {
                    Val(numeric_compare::<$arrow>(&value.0.0, &other.0.0, "gt", |a, b| a > b))
                }
                fn gt_s(value: Val<$wrapper>, other: $scalar) -> Val<BoolColumn> {
                    Val(numeric_compare_scalar::<$arrow>(&value.0.0, other, |a, b| a > b))
                }
                fn min(value: Val<$wrapper>) -> Option<$scalar> {
                    primitive::<$arrow>(&value.0.0).iter().flatten().reduce(<$scalar>::min)
                }
                fn max(value: Val<$wrapper>) -> Option<$scalar> {
                    primitive::<$arrow>(&value.0.0).iter().flatten().reduce(<$scalar>::max)
                }
            }
        }
    };
}

macro_rules! float_column_library {
    ($wrapper:ident, $arrow:ty, $scalar:ty) => {
        library! {
            impl Val<$wrapper> {
                fn len(value: Val<$wrapper>) -> u64 { value.0.0.0.len() as u64 }
                fn add(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<$wrapper> {
                    Val($wrapper(numeric_binary::<$arrow>(&value.0.0, &other.0.0, "add", |a, b| Some(a + b))))
                }
                fn add_s(value: Val<$wrapper>, other: $scalar) -> Val<$wrapper> {
                    Val($wrapper(numeric_scalar::<$arrow>(&value.0.0, other, "add", |a, b| Some(a + b))))
                }
                fn sub(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<$wrapper> {
                    Val($wrapper(numeric_binary::<$arrow>(&value.0.0, &other.0.0, "sub", |a, b| Some(a - b))))
                }
                fn sub_s(value: Val<$wrapper>, other: $scalar) -> Val<$wrapper> {
                    Val($wrapper(numeric_scalar::<$arrow>(&value.0.0, other, "sub", |a, b| Some(a - b))))
                }
                fn mul(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<$wrapper> {
                    Val($wrapper(numeric_binary::<$arrow>(&value.0.0, &other.0.0, "mul", |a, b| Some(a * b))))
                }
                fn mul_s(value: Val<$wrapper>, other: $scalar) -> Val<$wrapper> {
                    Val($wrapper(numeric_scalar::<$arrow>(&value.0.0, other, "mul", |a, b| Some(a * b))))
                }
                fn div(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<$wrapper> {
                    Val($wrapper(numeric_binary::<$arrow>(&value.0.0, &other.0.0, "div", |a, b| (b != 0.0).then_some(a / b))))
                }
                fn div_s(value: Val<$wrapper>, other: $scalar) -> Val<$wrapper> {
                    Val($wrapper(numeric_scalar::<$arrow>(&value.0.0, other, "div", |a, b| (b != 0.0).then_some(a / b))))
                }
                fn eq(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<BoolColumn> {
                    Val(numeric_compare::<$arrow>(&value.0.0, &other.0.0, "eq", |a, b| a == b))
                }
                fn eq_s(value: Val<$wrapper>, other: $scalar) -> Val<BoolColumn> {
                    Val(numeric_compare_scalar::<$arrow>(&value.0.0, other, |a, b| a == b))
                }
                fn lt(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<BoolColumn> {
                    Val(numeric_compare::<$arrow>(&value.0.0, &other.0.0, "lt", |a, b| a < b))
                }
                fn lt_s(value: Val<$wrapper>, other: $scalar) -> Val<BoolColumn> {
                    Val(numeric_compare_scalar::<$arrow>(&value.0.0, other, |a, b| a < b))
                }
                fn gt(value: Val<$wrapper>, other: Val<$wrapper>) -> Val<BoolColumn> {
                    Val(numeric_compare::<$arrow>(&value.0.0, &other.0.0, "gt", |a, b| a > b))
                }
                fn gt_s(value: Val<$wrapper>, other: $scalar) -> Val<BoolColumn> {
                    Val(numeric_compare_scalar::<$arrow>(&value.0.0, other, |a, b| a > b))
                }
                fn min(value: Val<$wrapper>) -> Option<$scalar> {
                    primitive::<$arrow>(&value.0.0).iter().flatten().reduce(<$scalar>::min)
                }
                fn max(value: Val<$wrapper>) -> Option<$scalar> {
                    primitive::<$arrow>(&value.0.0).iter().flatten().reduce(<$scalar>::max)
                }
            }
        }
    };
}

fn base_library() -> impl roto::Registerable {
    library! {
        #[clone] type NervixArgs = Val<UdfArgs>;
        #[copy] type Timestamp = Val<Timestamp>;
        #[copy] type ColumnBuilder = Val<ColumnBuilderFactory>;
        #[clone] type BoolColumnBuilder = Val<BoolColumnBuilder>;
        #[clone] type AnyColumn = Val<AnyColumn>;
        #[clone] type U8Column = Val<U8Column>;
        #[clone] type I8Column = Val<I8Column>;
        #[clone] type U16Column = Val<U16Column>;
        #[clone] type I16Column = Val<I16Column>;
        #[clone] type U32Column = Val<U32Column>;
        #[clone] type I32Column = Val<I32Column>;
        #[clone] type U64Column = Val<U64Column>;
        #[clone] type I64Column = Val<I64Column>;
        #[clone] type F32Column = Val<F32Column>;
        #[clone] type F64Column = Val<F64Column>;
        #[clone] type BoolColumn = Val<BoolColumn>;
        #[clone] type StringColumn = Val<StringColumn>;
        #[clone] type DatetimeColumn = Val<DatetimeColumn>;
        #[clone] type VecU8Column = Val<VecU8Column>;
        #[clone] type VecI8Column = Val<VecI8Column>;
        #[clone] type VecU16Column = Val<VecU16Column>;
        #[clone] type VecI16Column = Val<VecI16Column>;
        #[clone] type VecU32Column = Val<VecU32Column>;
        #[clone] type VecI32Column = Val<VecI32Column>;
        #[clone] type VecU64Column = Val<VecU64Column>;
        #[clone] type VecI64Column = Val<VecI64Column>;
        #[clone] type VecF32Column = Val<VecF32Column>;
        #[clone] type VecF64Column = Val<VecF64Column>;
        #[clone] type VecBoolColumn = Val<VecBoolColumn>;
        #[clone] type VecStringColumn = Val<VecStringColumn>;
        #[clone] type VecDatetimeColumn = Val<VecDatetimeColumn>;

        impl Val<UdfArgs> {
            fn u8(args: Val<UdfArgs>, index: u64) -> Val<U8Column> { Val(U8Column(column_arg(args, index, &DataType::UInt8))) }
            fn i8(args: Val<UdfArgs>, index: u64) -> Val<I8Column> { Val(I8Column(column_arg(args, index, &DataType::Int8))) }
            fn u16(args: Val<UdfArgs>, index: u64) -> Val<U16Column> { Val(U16Column(column_arg(args, index, &DataType::UInt16))) }
            fn i16(args: Val<UdfArgs>, index: u64) -> Val<I16Column> { Val(I16Column(column_arg(args, index, &DataType::Int16))) }
            fn u32(args: Val<UdfArgs>, index: u64) -> Val<U32Column> { Val(U32Column(column_arg(args, index, &DataType::UInt32))) }
            fn i32(args: Val<UdfArgs>, index: u64) -> Val<I32Column> { Val(I32Column(column_arg(args, index, &DataType::Int32))) }
            fn u64(args: Val<UdfArgs>, index: u64) -> Val<U64Column> { Val(U64Column(column_arg(args, index, &DataType::UInt64))) }
            fn i64(args: Val<UdfArgs>, index: u64) -> Val<I64Column> { Val(I64Column(column_arg(args, index, &DataType::Int64))) }
            fn f32(args: Val<UdfArgs>, index: u64) -> Val<F32Column> { Val(F32Column(column_arg(args, index, &DataType::Float32))) }
            fn f64(args: Val<UdfArgs>, index: u64) -> Val<F64Column> { Val(F64Column(column_arg(args, index, &DataType::Float64))) }
            fn bool(args: Val<UdfArgs>, index: u64) -> Val<BoolColumn> { Val(BoolColumn(column_arg(args, index, &DataType::Boolean))) }
            fn string(args: Val<UdfArgs>, index: u64) -> Val<StringColumn> { Val(StringColumn(column_arg(args, index, &DataType::Utf8))) }
            fn datetime(args: Val<UdfArgs>, index: u64) -> Val<DatetimeColumn> {
                Val(DatetimeColumn(column_arg(args, index, &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())))))
            }
            fn vec_u8(args: Val<UdfArgs>, index: u64) -> Val<VecU8Column> { Val(VecU8Column(untyped_column_arg(args, index))) }
            fn vec_i8(args: Val<UdfArgs>, index: u64) -> Val<VecI8Column> { Val(VecI8Column(untyped_column_arg(args, index))) }
            fn vec_u16(args: Val<UdfArgs>, index: u64) -> Val<VecU16Column> { Val(VecU16Column(untyped_column_arg(args, index))) }
            fn vec_i16(args: Val<UdfArgs>, index: u64) -> Val<VecI16Column> { Val(VecI16Column(untyped_column_arg(args, index))) }
            fn vec_u32(args: Val<UdfArgs>, index: u64) -> Val<VecU32Column> { Val(VecU32Column(untyped_column_arg(args, index))) }
            fn vec_i32(args: Val<UdfArgs>, index: u64) -> Val<VecI32Column> { Val(VecI32Column(untyped_column_arg(args, index))) }
            fn vec_u64(args: Val<UdfArgs>, index: u64) -> Val<VecU64Column> { Val(VecU64Column(untyped_column_arg(args, index))) }
            fn vec_i64(args: Val<UdfArgs>, index: u64) -> Val<VecI64Column> { Val(VecI64Column(untyped_column_arg(args, index))) }
            fn vec_f32(args: Val<UdfArgs>, index: u64) -> Val<VecF32Column> { Val(VecF32Column(untyped_column_arg(args, index))) }
            fn vec_f64(args: Val<UdfArgs>, index: u64) -> Val<VecF64Column> { Val(VecF64Column(untyped_column_arg(args, index))) }
            fn vec_bool(args: Val<UdfArgs>, index: u64) -> Val<VecBoolColumn> { Val(VecBoolColumn(untyped_column_arg(args, index))) }
            fn vec_string(args: Val<UdfArgs>, index: u64) -> Val<VecStringColumn> { Val(VecStringColumn(untyped_column_arg(args, index))) }
            fn vec_datetime(args: Val<UdfArgs>, index: u64) -> Val<VecDatetimeColumn> { Val(VecDatetimeColumn(untyped_column_arg(args, index))) }
            fn any(args: Val<UdfArgs>, index: u64) -> Val<AnyColumn> { Val(AnyColumn(untyped_column_arg(args, index))) }
        }

        impl Val<AnyColumn> {
            fn from_u8(value: Val<U8Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_i8(value: Val<I8Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_u16(value: Val<U16Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_i16(value: Val<I16Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_u32(value: Val<U32Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_i32(value: Val<I32Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_u64(value: Val<U64Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_i64(value: Val<I64Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_f32(value: Val<F32Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_f64(value: Val<F64Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_bool(value: Val<BoolColumn>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_string(value: Val<StringColumn>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_datetime(value: Val<DatetimeColumn>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_u8(value: Val<VecU8Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_i8(value: Val<VecI8Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_u16(value: Val<VecU16Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_i16(value: Val<VecI16Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_u32(value: Val<VecU32Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_i32(value: Val<VecI32Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_u64(value: Val<VecU64Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_i64(value: Val<VecI64Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_f32(value: Val<VecF32Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_f64(value: Val<VecF64Column>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_bool(value: Val<VecBoolColumn>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_string(value: Val<VecStringColumn>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_vec_datetime(value: Val<VecDatetimeColumn>) -> Self { Val(AnyColumn(value.0.0)) }
            fn from_any(value: Val<AnyColumn>) -> Self { value }
        }

        impl Val<ColumnBuilderFactory> {
            fn bool(capacity: u64) -> Val<BoolColumnBuilder> {
                Val(BoolColumnBuilder(Arc::new(Mutex::new(Vec::with_capacity(
                    usize::try_from(capacity).unwrap_or(row_count()).min(row_count())
                )))))
            }
        }

        impl Val<BoolColumnBuilder> {
            fn push(builder: Val<BoolColumnBuilder>, value: bool) {
                builder.0.0.lock().push(Some(value));
            }
            fn push_null(builder: Val<BoolColumnBuilder>) {
                builder.0.0.lock().push(None);
            }
            fn finish(builder: Val<BoolColumnBuilder>) -> Val<BoolColumn> {
                let values = std::mem::take(
                    &mut *builder.0.0.lock()
                );
                Val(BoolColumn(Column(StdArc::new(BooleanArray::from(values)))))
            }
        }
    }
}

fn bool_library() -> impl roto::Registerable {
    library! {
        impl Val<BoolColumn> {
            fn len(value: Val<BoolColumn>) -> u64 { value.0.0.0.len() as u64 }
            fn not(value: Val<BoolColumn>) -> Val<BoolColumn> {
                let input = value.0.0.0.as_any().downcast_ref::<BooleanArray>().expect("validated BoolColumn");
                match boolean::not(input) {
                    Ok(output) => Val(BoolColumn(Column(StdArc::new(output)))),
                    Err(error) => {
                        fatal(format!("not failed: {error}"));
                        Val(BoolColumn(Column(StdArc::new(BooleanArray::new_null(input.len())))))
                    }
                }
            }
            fn and(value: Val<BoolColumn>, other: Val<BoolColumn>) -> Val<BoolColumn> {
                let left = value.0.0.0.as_any().downcast_ref::<BooleanArray>().expect("validated BoolColumn");
                let right = other.0.0.0.as_any().downcast_ref::<BooleanArray>().expect("validated BoolColumn");
                match boolean::and(left, right) {
                    Ok(output) => Val(BoolColumn(Column(StdArc::new(output)))),
                    Err(error) => {
                        fatal(format!("and failed: {error}"));
                        Val(BoolColumn(Column(StdArc::new(BooleanArray::new_null(left.len())))))
                    }
                }
            }
            fn or(value: Val<BoolColumn>, other: Val<BoolColumn>) -> Val<BoolColumn> {
                let left = value.0.0.0.as_any().downcast_ref::<BooleanArray>().expect("validated BoolColumn");
                let right = other.0.0.0.as_any().downcast_ref::<BooleanArray>().expect("validated BoolColumn");
                match boolean::or(left, right) {
                    Ok(output) => Val(BoolColumn(Column(StdArc::new(output)))),
                    Err(error) => {
                        fatal(format!("or failed: {error}"));
                        Val(BoolColumn(Column(StdArc::new(BooleanArray::new_null(left.len())))))
                    }
                }
            }
            fn select(value: Val<BoolColumn>, truthy: Val<StringColumn>, falsy: Val<StringColumn>) -> Val<StringColumn> {
                let mask = value.0.0.0.as_any().downcast_ref::<BooleanArray>().expect("validated BoolColumn");
                let truthy = truthy.0.0.0.as_ref();
                let falsy = falsy.0.0.0.as_ref();
                match zip(mask, &truthy as &dyn Datum, &falsy as &dyn Datum) {
                    Ok(output) => Val(StringColumn(Column(output))),
                    Err(error) => {
                        fatal(format!("select failed: {error}"));
                        Val(StringColumn(Column(new_null_array(&DataType::Utf8, mask.len()))))
                    }
                }
            }
        }

        fn reject_where_s(condition: Val<BoolColumn>, message: RotoString) {
            let condition = condition.0.0.0.as_any().downcast_ref::<BooleanArray>()
                .expect("validated BoolColumn");
            for (row, reject) in condition.iter().enumerate() {
                if reject == Some(true) {
                    side_error(row, ErrorCode::InvalidArgument, "reject_where", message.as_ref());
                }
            }
        }
    }
}

fn string_library() -> impl roto::Registerable {
    library! {
        #[clone] type StringWhen = Val<StringWhen>;

        impl Val<StringColumn> {
            fn len(value: Val<StringColumn>) -> u64 { value.0.0.0.len() as u64 }
            fn trim(value: Val<StringColumn>) -> Val<StringColumn> {
                let input = value.0.0.0.as_any().downcast_ref::<StringArray>().expect("validated StringColumn");
                Val(StringColumn(Column(StdArc::new(StringArray::from_iter(
                    input.iter().map(|value| value.map(str::trim))
                )))))
            }
            fn contains_s(value: Val<StringColumn>, needle: RotoString) -> Val<BoolColumn> {
                let input = value.0.0.0.as_any().downcast_ref::<StringArray>().expect("validated StringColumn");
                Val(BoolColumn(Column(StdArc::new(BooleanArray::from_iter(
                    input.iter().map(|value| value.map(|value| value.contains(needle.as_ref())))
                )))))
            }
            fn regexp_replace(value: Val<StringColumn>, pattern: RotoString, replacement: RotoString) -> Val<StringColumn> {
                let input = value.0.0.0.as_any().downcast_ref::<StringArray>().expect("validated StringColumn");
                let regex = match Regex::new(pattern.as_ref()) {
                    Ok(regex) => regex,
                    Err(error) => {
                        fatal(format!("invalid regexp_replace pattern: {error}"));
                        return Val(StringColumn(Column(new_null_array(&DataType::Utf8, input.len()))));
                    }
                };
                Val(StringColumn(Column(StdArc::new(StringArray::from_iter(
                    input.iter().map(|value| value.map(|value| regex.replace_all(value, replacement.as_ref()).into_owned()))
                )))))
            }
            fn get(value: Val<StringColumn>, index: u64) -> Option<RotoString> {
                let input = value.0.0.0.as_any().downcast_ref::<StringArray>()
                    .expect("validated StringColumn");
                let Ok(index) = usize::try_from(index) else {
                    return None;
                };
                (index < input.len() && input.is_valid(index))
                    .then(|| RotoString::new(input.value(index)))
            }
        }

        fn coalesce(left: Val<StringColumn>, right: Val<StringColumn>) -> Val<StringColumn> {
            let left = left.0.0.0.as_any().downcast_ref::<StringArray>().expect("validated StringColumn");
            let right = right.0.0.0.as_any().downcast_ref::<StringArray>().expect("validated StringColumn");
            if left.len() != right.len() {
                fatal(format!("coalesce received columns with {} and {} rows", left.len(), right.len()));
                return Val(StringColumn(Column(new_null_array(&DataType::Utf8, left.len()))));
            }
            Val(StringColumn(Column(StdArc::new(StringArray::from_iter(
                left.iter().zip(right.iter()).map(|(left, right)| left.or(right))
            )))))
        }

        fn when_s(condition: Val<BoolColumn>, value: RotoString) -> Val<StringWhen> {
            Val(StringWhen { arms: vec![(condition.0, value)] })
        }

        impl Val<StringWhen> {
            fn when_s(mut chain: Val<StringWhen>, condition: Val<BoolColumn>, value: RotoString) -> Val<StringWhen> {
                chain.0.arms.push((condition.0, value));
                chain
            }

            fn otherwise_s(chain: Val<StringWhen>, fallback: RotoString) -> Val<StringColumn> {
                let expected_rows = chain.0.arms.first().map_or(row_count(), |(condition, _)| {
                    condition.0.0.len()
                });
                let conditions = chain.0.arms.iter().map(|(condition, value)| {
                    let condition = condition.0.0.as_any().downcast_ref::<BooleanArray>()
                        .expect("validated BoolColumn");
                    if condition.len() != expected_rows {
                        fatal(format!(
                            "when chain received columns with {expected_rows} and {} rows",
                            condition.len()
                        ));
                    }
                    (condition, value)
                }).collect::<Vec<_>>();
                Val(StringColumn(Column(StdArc::new(StringArray::from_iter(
                    (0..expected_rows).map(|row| {
                        conditions.iter().find_map(|(condition, value)| {
                            (condition.is_valid(row) && condition.value(row))
                                .then(|| value.as_ref().to_string())
                        }).or_else(|| Some(fallback.as_ref().to_string()))
                    })
                )))))
            }
        }
    }
}

fn cast_library() -> impl roto::Registerable {
    library! {
        impl Val<I64Column> {
            fn cast_f64(value: Val<I64Column>) -> Val<F64Column> {
                let input = value.0.0.0.as_any().downcast_ref::<Int64Array>().expect("validated I64Column");
                Val(F64Column(Column(StdArc::new(Float64Array::from_iter(
                    input.iter().map(|value| value.map(|value| value as f64))
                )))))
            }
        }
    }
}

fn list_library() -> impl roto::Registerable {
    library! {
        impl Val<VecStringColumn> {
            fn len(value: Val<VecStringColumn>) -> u64 { value.0.0.0.len() as u64 }
            fn contains_s(value: Val<VecStringColumn>, needle: RotoString) -> Val<BoolColumn> {
                let column = &value.0.0.0;
                let output = match column.data_type() {
                    DataType::List(_) => {
                        let lists = column.as_any().downcast_ref::<ListArray>()
                            .expect("validated VecStringColumn");
                        let values = lists.values().as_any().downcast_ref::<StringArray>()
                            .expect("validated VecStringColumn leaf");
                        BooleanArray::from_iter((0..lists.len()).map(|row| {
                            if lists.is_null(row) {
                                return None;
                            }
                            let offsets = lists.value_offsets();
                            let start = offsets[row] as usize;
                            let end = offsets[row + 1] as usize;
                            Some((start..end).any(|index| {
                                values.is_valid(index) && values.value(index) == needle.as_ref()
                            }))
                        }))
                    }
                    DataType::FixedSizeList(_, size) => {
                        let lists = column.as_any().downcast_ref::<FixedSizeListArray>()
                            .expect("validated VecStringColumn");
                        let values = lists.values().as_any().downcast_ref::<StringArray>()
                            .expect("validated VecStringColumn leaf");
                        let size = *size as usize;
                        BooleanArray::from_iter((0..lists.len()).map(|row| {
                            if lists.is_null(row) {
                                return None;
                            }
                            let start = row * size;
                            Some((start..start + size).any(|index| {
                                values.is_valid(index) && values.value(index) == needle.as_ref()
                            }))
                        }))
                    }
                    actual => {
                        fatal(format!("VecStringColumn contains_s received {actual:?}"));
                        BooleanArray::new_null(column.len())
                    }
                };
                Val(BoolColumn(Column(StdArc::new(output))))
            }
        }
    }
}

fn datetime_library() -> impl roto::Registerable {
    library! {
        impl Val<DatetimeColumn> {
            fn len(value: Val<DatetimeColumn>) -> u64 { value.0.0.0.len() as u64 }
            fn lt_s(value: Val<DatetimeColumn>, other: Val<Timestamp>) -> Val<BoolColumn> {
                let input = value.0.0.0.as_any().downcast_ref::<TimestampNanosecondArray>()
                    .expect("validated DatetimeColumn");
                let other = other.0.unix_nanos();
                Val(BoolColumn(Column(StdArc::new(BooleanArray::from_iter(
                    input.iter().map(|value| value.map(|value| value < other))
                )))))
            }
        }
    }
}

fn deterministic_runtime() -> Result<Runtime<NoCtx>, UdfError> {
    let mut runtime = Runtime::from_lib(base_library())
        .map_err(|error| UdfError::RuntimeRegistration(error.to_string()))?;
    for library in [
        integer_column_library!(U8Column, UInt8Type, u8),
        integer_column_library!(I8Column, Int8Type, i8),
        integer_column_library!(U16Column, UInt16Type, u16),
        integer_column_library!(I16Column, Int16Type, i16),
        integer_column_library!(U32Column, UInt32Type, u32),
        integer_column_library!(I32Column, Int32Type, i32),
        integer_column_library!(U64Column, UInt64Type, u64),
        integer_column_library!(I64Column, Int64Type, i64),
    ] {
        runtime
            .add(library)
            .map_err(|error| UdfError::RuntimeRegistration(error.to_string()))?;
    }
    runtime
        .add(float_column_library!(F32Column, Float32Type, f32))
        .and_then(|_| runtime.add(float_column_library!(F64Column, Float64Type, f64)))
        .and_then(|_| runtime.add(bool_library()))
        .and_then(|_| runtime.add(string_library()))
        .and_then(|_| runtime.add(cast_library()))
        .and_then(|_| runtime.add(list_library()))
        .and_then(|_| runtime.add(datetime_library()))
        .map_err(|error| UdfError::RuntimeRegistration(error.to_string()))?;
    Ok(runtime)
}

fn volatile_library() -> impl roto::Registerable {
    library! {
        fn now() -> Val<Timestamp> {
            Val(call_timestamp())
        }
        fn rand_f64() -> Val<F64Column> {
            Val(F64Column(Column(StdArc::new(Float64Array::from_iter(
                (0..row_count()).map(|_| Some(fastrand::f64()))
            )))))
        }
        fn uuid_v4() -> Val<StringColumn> {
            Val(StringColumn(Column(StdArc::new(StringArray::from_iter(
                (0..row_count()).map(|_| Some(uuid::Uuid::new_v4().to_string()))
            )))))
        }
    }
}

type EntryFunction = TypedFunc<NoCtx, fn(Val<UdfArgs>) -> Val<AnyColumn>>;

#[derive(Clone)]
struct CompiledUdf {
    model: CreateUdf,
    entry: EntryFunction,
    watchdog: Duration,
}

impl fmt::Debug for CompiledUdf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledUdf")
            .field("name", &self.model.name)
            .field("code_hash", &self.model.code_hash)
            .finish_non_exhaustive()
    }
}

impl CompiledUdf {
    fn execute(
        &self,
        arguments: &[TypedArray],
        row_count: usize,
        span: nervix_nspl::vm_program::Span,
        now: Timestamp,
        prior_error_rows: &[bool],
    ) -> Result<InjectedResult, RuntimeError> {
        if arguments.len() != self.model.arguments.len() {
            return Err(RuntimeError::InjectedFunctionFailed {
                function: self.model.name.to_string(),
                message: format!(
                    "expected {} arguments, found {}",
                    self.model.arguments.len(),
                    arguments.len()
                ),
            });
        }
        let mut argument_arrays = Vec::with_capacity(arguments.len());
        for (index, (argument, declaration)) in
            arguments.iter().zip(&self.model.arguments).enumerate()
        {
            let argument = argument.to_array_ref();
            let expected_type = arrow_data_type(&declaration.ty);
            if argument.data_type() != &expected_type || argument.len() != row_count {
                return Err(RuntimeError::InjectedFunctionFailed {
                    function: self.model.name.to_string(),
                    message: format!(
                        "argument {index} expected {expected_type:?} with {row_count} rows, found \
                         {:?} with {} rows",
                        argument.data_type(),
                        argument.len()
                    ),
                });
            }
            argument_arrays.push(argument);
        }
        let mut propagation = prior_error_rows.to_vec();
        if propagation.len() != row_count {
            return Err(RuntimeError::InjectedFunctionFailed {
                function: self.model.name.to_string(),
                message: format!(
                    "received {} prior-error rows for a {row_count}-row batch",
                    propagation.len()
                ),
            });
        }
        for (argument, declaration) in argument_arrays.iter().zip(&self.model.arguments) {
            if !declaration.optional {
                for (row, masked) in propagation.iter_mut().enumerate() {
                    *masked |= argument.is_null(row);
                }
            }
        }
        if propagation.iter().all(|masked| *masked) {
            return Ok(InjectedResult::success(typed_array_from_ref(
                new_null_array(&arrow_data_type(&self.model.returns.ty), row_count),
            )?));
        }
        if propagation.iter().any(|masked| *masked) {
            let mask = BooleanArray::from(propagation.clone());
            argument_arrays = argument_arrays
                .into_iter()
                .map(|argument| {
                    nullif(argument.as_ref(), &mask).map_err(|error| {
                        RuntimeError::InjectedFunctionFailed {
                            function: self.model.name.to_string(),
                            message: format!(
                                "failed to hide strict-propagation rows from the UDF: {error}"
                            ),
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
        }

        CALL_STATE.with(|state| {
            let mut state = state.borrow_mut();
            if state.is_some() {
                return Err(RuntimeError::InjectedFunctionFailed {
                    function: self.model.name.to_string(),
                    message: "nested Roto execution context is not supported".to_string(),
                });
            }
            *state = Some(CallState {
                udf_name: self.model.name.to_string(),
                span,
                row_count,
                now,
                side_errors: Vec::new(),
                fatal: None,
            });
            Ok(())
        })?;
        let started = Instant::now();
        let call = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.entry.call(Val(UdfArgs(argument_arrays))).0.0.0
        }));
        let state = CALL_STATE.with(|state| {
            state
                .borrow_mut()
                .take()
                .expect("call state was installed before Roto execution")
        });
        let output = call.map_err(|panic| RuntimeError::InjectedFunctionFailed {
            function: self.model.name.to_string(),
            message: panic.downcast_ref::<&str>().map_or_else(
                || {
                    panic
                        .downcast_ref::<String>()
                        .map(|message| format!("Roto execution trapped: {message}"))
                        .unwrap_or_else(|| "Roto execution trapped".to_string())
                },
                |message| format!("Roto execution trapped: {message}"),
            ),
        })?;
        if started.elapsed() > self.watchdog {
            return Err(RuntimeError::InjectedFunctionFailed {
                function: self.model.name.to_string(),
                message: format!("watchdog expired after {:?}", self.watchdog),
            });
        }
        if let Some(message) = state.fatal {
            return Err(RuntimeError::InjectedFunctionFailed {
                function: self.model.name.to_string(),
                message,
            });
        }
        let expected_type = arrow_data_type(&self.model.returns.ty);
        if output.data_type() != &expected_type || output.len() != row_count {
            return Err(RuntimeError::InvalidInjectedResult {
                function: self.model.name.to_string(),
                expected_type,
                actual_type: output.data_type().clone(),
                expected_rows: row_count,
                actual_rows: output.len(),
            });
        }
        let output = if propagation.iter().any(|masked| *masked) {
            let mask = BooleanArray::from(propagation.clone());
            nullif(output.as_ref(), &mask).map_err(|error| {
                RuntimeError::InjectedFunctionFailed {
                    function: self.model.name.to_string(),
                    message: format!("failed to apply required-argument null mask: {error}"),
                }
            })?
        } else {
            output
        };
        if !self.model.returns.optional {
            for row in 0..row_count {
                let error_row = state
                    .side_errors
                    .iter()
                    .any(|(error_row, _)| *error_row == row);
                if output.is_null(row) && !propagation[row] && !error_row {
                    return Err(RuntimeError::InjectedFunctionFailed {
                        function: self.model.name.to_string(),
                        message: format!(
                            "non-OPTIONAL return contains an unexplained null at row {row}"
                        ),
                    });
                }
            }
        }
        Ok(InjectedResult {
            output: typed_array_from_ref(output)?,
            side_errors: state.side_errors,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct UdfExecutor {
    functions: HashMap<String, Arc<CompiledUdf>>,
    signatures: UdfSignatures,
}

impl UdfExecutor {
    pub async fn compile(models: Vec<CreateUdf>) -> Result<Self, UdfError> {
        tokio::task::spawn_blocking(move || Self::compile_sync(models))
            .await
            .map_err(UdfError::CompileTask)?
    }

    fn compile_sync(models: impl IntoIterator<Item = CreateUdf>) -> Result<Self, UdfError> {
        let mut functions = HashMap::new();
        let mut signatures = UdfSignatures::default();
        for model in models {
            let started = Instant::now();
            let compiled = compile_udf(model.clone(), DEFAULT_WATCHDOG)?;
            if started.elapsed() > COMPILE_TEST_BUDGET {
                return Err(UdfError::CompileBudgetExceeded {
                    limit: COMPILE_TEST_BUDGET,
                });
            }
            signatures.insert(model.name.as_str(), signature_for(&model));
            functions.insert(model.name.as_str().to_ascii_lowercase(), Arc::new(compiled));
        }
        Ok(Self {
            functions,
            signatures,
        })
    }

    pub fn signatures(&self) -> &UdfSignatures {
        &self.signatures
    }

    pub fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }
}

impl FunctionInjector for UdfExecutor {
    fn execution_policy(
        &self,
        function: &nervix_nspl::vm_program::FunctionName,
    ) -> FunctionExecutionPolicy {
        if matches!(function, nervix_nspl::vm_program::FunctionName::Udf(_)) {
            FunctionExecutionPolicy::SpawnBlocking
        } else {
            FunctionExecutionPolicy::Inline
        }
    }

    fn inject(
        &self,
        function: &nervix_nspl::vm_program::FunctionName,
        arguments: &[TypedArray],
        row_count: usize,
        span: nervix_nspl::vm_program::Span,
    ) -> Result<TypedArray, RuntimeError> {
        self.inject_with_errors(function, arguments, row_count, span)
            .map(|result| result.output)
    }

    fn inject_with_errors(
        &self,
        function: &nervix_nspl::vm_program::FunctionName,
        arguments: &[TypedArray],
        row_count: usize,
        span: nervix_nspl::vm_program::Span,
    ) -> Result<InjectedResult, RuntimeError> {
        let nervix_nspl::vm_program::FunctionName::Udf(name) = function else {
            return Err(RuntimeError::MissingFunctionInjector {
                function: function.as_str().to_string(),
            });
        };
        let Some(compiled) = self.functions.get(&name.to_ascii_lowercase()) else {
            return Err(RuntimeError::MissingFunctionInjector {
                function: function.as_str().to_string(),
            });
        };
        compiled.execute(
            arguments,
            row_count,
            span,
            Timestamp::now(),
            &vec![false; row_count],
        )
    }

    fn inject_with_context(
        &self,
        function: &nervix_nspl::vm_program::FunctionName,
        arguments: &[TypedArray],
        row_count: usize,
        span: nervix_nspl::vm_program::Span,
        now: Timestamp,
        prior_error_rows: &[bool],
    ) -> Result<InjectedResult, RuntimeError> {
        let nervix_nspl::vm_program::FunctionName::Udf(name) = function else {
            return Err(RuntimeError::MissingFunctionInjector {
                function: function.as_str().to_string(),
            });
        };
        let Some(compiled) = self.functions.get(&name.to_ascii_lowercase()) else {
            return Err(RuntimeError::MissingFunctionInjector {
                function: function.as_str().to_string(),
            });
        };
        compiled.execute(arguments, row_count, span, now, prior_error_rows)
    }
}

pub fn signature_for(model: &CreateUdf) -> UdfSignature {
    UdfSignature {
        arguments: model
            .arguments
            .iter()
            .map(|argument| UdfParameter {
                data_type: arrow_data_type(&argument.ty),
                optional: argument.optional,
            })
            .collect(),
        return_type: arrow_data_type(&model.returns.ty),
        return_optional: model.returns.optional,
        volatile: model.volatile,
    }
}

pub fn signatures_for<'a>(models: impl IntoIterator<Item = &'a CreateUdf>) -> UdfSignatures {
    let mut signatures = UdfSignatures::default();
    for model in models {
        signatures.insert(model.name.as_str(), signature_for(model));
    }
    signatures
}

fn compile_udf(model: CreateUdf, watchdog: Duration) -> Result<CompiledUdf, UdfError> {
    if model.code.contains(RESERVED_PREFIX) {
        return Err(UdfError::ReservedIdentifier {
            name: model.name.to_string(),
        });
    }
    let mut runtime = deterministic_runtime()?;
    if model.volatile {
        runtime
            .add(volatile_library())
            .map_err(|error| UdfError::RuntimeRegistration(error.to_string()))?;
    }
    let wrapper = generated_wrapper(&model);
    let source = format!("{}\n{wrapper}", model.code);
    let mut package = match FileTree::test_file("udf.roto", &source, 0).compile(&runtime) {
        Ok(package) => package,
        Err(report) => {
            let mut diagnostics = String::new();
            let _ = report.write(&mut diagnostics, false);
            if !model.volatile {
                for function in ["now", "rand_f64", "uuid_v4"] {
                    if contains_call(&model.code, function) && diagnostics.contains(function) {
                        return Err(UdfError::VolatileRequired { function });
                    }
                }
            }
            return Err(UdfError::Compile { diagnostics });
        }
    };
    if package.run_tests().is_err() {
        return Err(UdfError::TestsFailed);
    }
    let entry = package
        .get_function::<fn(Val<UdfArgs>) -> Val<AnyColumn>>("__nervix_entry")
        .map_err(|error| UdfError::Signature(error.to_string()))?;
    Ok(CompiledUdf {
        model,
        entry,
        watchdog,
    })
}

fn contains_call(source: &str, function: &str) -> bool {
    let pattern = format!(r"\b{}\s*\(", regex::escape(function));
    Regex::new(&pattern)
        .expect("generated call pattern is valid")
        .is_match(source)
}

fn generated_wrapper(model: &CreateUdf) -> String {
    let arguments = model
        .arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            let accessor = bridge_accessor(&argument.ty);
            format!("__nervix_args.{accessor}({index})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let conversion = bridge_conversion(&model.returns.ty);
    format!(
        "fn __nervix_entry(__nervix_args: NervixArgs) -> AnyColumn {{\n    \
         AnyColumn.{conversion}({}({arguments}))\n}}\n",
        model.name.as_str()
    )
}

fn bridge_accessor(ty: &ParseAsType) -> &'static str {
    match ty {
        ParseAsType::U8 => "u8",
        ParseAsType::I8 => "i8",
        ParseAsType::U16 => "u16",
        ParseAsType::I16 => "i16",
        ParseAsType::U32 => "u32",
        ParseAsType::I32 => "i32",
        ParseAsType::U64 => "u64",
        ParseAsType::I64 => "i64",
        ParseAsType::F32 => "f32",
        ParseAsType::F64 => "f64",
        ParseAsType::Bool => "bool",
        ParseAsType::String => "string",
        ParseAsType::Datetime => "datetime",
        ParseAsType::Array { element, .. } | ParseAsType::Vec { element } => {
            list_bridge_accessor(element)
        }
    }
}

fn bridge_conversion(ty: &ParseAsType) -> &'static str {
    match ty {
        ParseAsType::U8 => "from_u8",
        ParseAsType::I8 => "from_i8",
        ParseAsType::U16 => "from_u16",
        ParseAsType::I16 => "from_i16",
        ParseAsType::U32 => "from_u32",
        ParseAsType::I32 => "from_i32",
        ParseAsType::U64 => "from_u64",
        ParseAsType::I64 => "from_i64",
        ParseAsType::F32 => "from_f32",
        ParseAsType::F64 => "from_f64",
        ParseAsType::Bool => "from_bool",
        ParseAsType::String => "from_string",
        ParseAsType::Datetime => "from_datetime",
        ParseAsType::Array { element, .. } | ParseAsType::Vec { element } => {
            list_bridge_conversion(element)
        }
    }
}

fn list_bridge_accessor(element: &ParseAsType) -> &'static str {
    match element {
        ParseAsType::U8 => "vec_u8",
        ParseAsType::I8 => "vec_i8",
        ParseAsType::U16 => "vec_u16",
        ParseAsType::I16 => "vec_i16",
        ParseAsType::U32 => "vec_u32",
        ParseAsType::I32 => "vec_i32",
        ParseAsType::U64 => "vec_u64",
        ParseAsType::I64 => "vec_i64",
        ParseAsType::F32 => "vec_f32",
        ParseAsType::F64 => "vec_f64",
        ParseAsType::Bool => "vec_bool",
        ParseAsType::String => "vec_string",
        ParseAsType::Datetime => "vec_datetime",
        ParseAsType::Array { .. } | ParseAsType::Vec { .. } => "any",
    }
}

fn list_bridge_conversion(element: &ParseAsType) -> &'static str {
    match element {
        ParseAsType::U8 => "from_vec_u8",
        ParseAsType::I8 => "from_vec_i8",
        ParseAsType::U16 => "from_vec_u16",
        ParseAsType::I16 => "from_vec_i16",
        ParseAsType::U32 => "from_vec_u32",
        ParseAsType::I32 => "from_vec_i32",
        ParseAsType::U64 => "from_vec_u64",
        ParseAsType::I64 => "from_vec_i64",
        ParseAsType::F32 => "from_vec_f32",
        ParseAsType::F64 => "from_vec_f64",
        ParseAsType::Bool => "from_vec_bool",
        ParseAsType::String => "from_vec_string",
        ParseAsType::Datetime => "from_vec_datetime",
        ParseAsType::Array { .. } | ParseAsType::Vec { .. } => "from_any",
    }
}

pub fn arrow_data_type(ty: &ParseAsType) -> DataType {
    match ty {
        ParseAsType::U8 => DataType::UInt8,
        ParseAsType::I8 => DataType::Int8,
        ParseAsType::U16 => DataType::UInt16,
        ParseAsType::I16 => DataType::Int16,
        ParseAsType::U32 => DataType::UInt32,
        ParseAsType::I32 => DataType::Int32,
        ParseAsType::U64 => DataType::UInt64,
        ParseAsType::I64 => DataType::Int64,
        ParseAsType::F32 => DataType::Float32,
        ParseAsType::F64 => DataType::Float64,
        ParseAsType::Bool => DataType::Boolean,
        ParseAsType::String => DataType::Utf8,
        ParseAsType::Datetime => DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        ParseAsType::Array { element, len } => DataType::FixedSizeList(
            StdArc::new(Field::new("item", arrow_data_type(element), false)),
            i32::try_from(*len).expect("validated ARRAY length fits Arrow"),
        ),
        ParseAsType::Vec { element } => DataType::List(StdArc::new(Field::new(
            "item",
            arrow_data_type(element),
            false,
        ))),
    }
}

fn typed_array_from_ref(array: ArrayRef) -> Result<TypedArray, RuntimeError> {
    macro_rules! downcast {
        ($array_ty:ty, $variant:ident) => {
            array
                .as_any()
                .downcast_ref::<$array_ty>()
                .cloned()
                .map(TypedArray::$variant)
                .ok_or_else(|| RuntimeError::InvalidBatch {
                    message: format!(
                        "Arrow array has invalid physical type for {:?}",
                        array.data_type()
                    ),
                })
        };
    }
    match array.data_type() {
        DataType::UInt8 => downcast!(UInt8Array, UInt8),
        DataType::Int8 => downcast!(Int8Array, Int8),
        DataType::UInt16 => downcast!(UInt16Array, UInt16),
        DataType::Int16 => downcast!(Int16Array, Int16),
        DataType::UInt32 => downcast!(UInt32Array, UInt32),
        DataType::Int32 => downcast!(Int32Array, Int32),
        DataType::UInt64 => downcast!(UInt64Array, UInt64),
        DataType::Int64 => downcast!(Int64Array, Int64),
        DataType::Float32 => downcast!(Float32Array, Float32),
        DataType::Float64 => downcast!(Float64Array, Float64),
        DataType::Boolean => downcast!(BooleanArray, Boolean),
        DataType::Utf8 => downcast!(StringArray, Utf8),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone))
            if timezone.as_ref() == "+00:00" || timezone.as_ref() == "UTC" =>
        {
            downcast!(TimestampNanosecondArray, Datetime)
        }
        DataType::List(_) | DataType::FixedSizeList(_, _) => Ok(TypedArray::Generic(array)),
        data_type => Err(RuntimeError::InvalidBatch {
            message: format!("unsupported UDF Arrow type {data_type:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{Identifier, UdfArgument, UdfLanguage, UdfReturn};

    use super::*;

    fn add_one_model() -> CreateUdf {
        CreateUdf::new(
            Identifier::parse("add_one").expect("valid identifier"),
            UdfLanguage::Roto0_11,
            vec![UdfArgument {
                name: Identifier::parse("value").expect("valid identifier"),
                ty: ParseAsType::I64,
                optional: false,
            }],
            UdfReturn {
                ty: ParseAsType::I64,
                optional: false,
            },
            false,
            "fn add_one(value: I64Column) -> I64Column { value.add_s(1) }".to_string(),
        )
    }

    fn model(
        name: &str,
        arguments: impl IntoIterator<Item = (&'static str, ParseAsType, bool)>,
        returns: ParseAsType,
        volatile: bool,
        code: &str,
    ) -> CreateUdf {
        CreateUdf::new(
            Identifier::parse(name).expect("valid identifier"),
            UdfLanguage::Roto0_11,
            arguments
                .into_iter()
                .map(|(name, ty, optional)| UdfArgument {
                    name: Identifier::parse(name).expect("valid identifier"),
                    ty,
                    optional,
                })
                .collect(),
            UdfReturn {
                ty: returns,
                optional: false,
            },
            volatile,
            code.to_string(),
        )
    }

    #[test]
    fn compiles_and_executes_i64_column_udf() {
        let executor = UdfExecutor::compile_sync([add_one_model()]).expect("UDF should compile");
        let function = nervix_nspl::vm_program::FunctionName::Udf("add_one".to_string());
        assert_eq!(
            executor.execution_policy(&function),
            FunctionExecutionPolicy::SpawnBlocking
        );
        let result = executor
            .inject_with_errors(
                &function,
                &[TypedArray::Int64(Int64Array::from(vec![
                    Some(1),
                    None,
                    Some(41),
                ]))],
                3,
                (0..7).into(),
            )
            .expect("UDF should execute");
        assert_eq!(
            result.output,
            TypedArray::Int64(Int64Array::from(vec![Some(2), None, Some(42)]))
        );
        assert!(result.side_errors.is_empty());
    }

    #[test]
    fn deterministic_udf_rejects_volatile_api_with_specific_error() {
        let mut model = add_one_model();
        model.code = "fn add_one(value: I64Column) -> I64Column { value.add_s(now()) }".to_string();
        assert!(matches!(
            UdfExecutor::compile_sync([model]),
            Err(UdfError::VolatileRequired { function: "now" })
        ));
    }

    #[test]
    fn failing_roto_test_rejects_udf_compilation() {
        let mut model = add_one_model();
        model.code = r#"
fn add_one(value: I64Column) -> I64Column {
    value
}

fn increment_scalar(value: i64) -> i64 {
    value
}

test increments_by_one {
    if increment_scalar(41) == 42 {
        accept
    } else {
        reject
    }
}
"#
        .to_string();
        assert!(matches!(
            UdfExecutor::compile_sync([model]),
            Err(UdfError::TestsFailed)
        ));
    }

    #[test]
    fn generated_wrapper_enforces_exact_signature() {
        let mut model = add_one_model();
        model.code = "fn add_one(value: StringColumn) -> StringColumn { value.trim() }".to_string();
        assert!(matches!(
            UdfExecutor::compile_sync([model]),
            Err(UdfError::Compile { .. })
        ));
    }

    #[test]
    fn documented_columnar_udf_shapes_compile() {
        let models = vec![
            model(
                "display_name",
                [
                    ("nick", ParseAsType::String, true),
                    ("email", ParseAsType::String, false),
                ],
                ParseAsType::String,
                false,
                r#"fn display_name(nick: StringColumn, email: StringColumn) -> StringColumn {
    coalesce(nick.trim(), email.regexp_replace("@.*$", ""))
}"#,
            ),
            model(
                "risk_band",
                [("score", ParseAsType::F64, false)],
                ParseAsType::String,
                false,
                r#"fn risk_band(score: F64Column) -> StringColumn {
    when_s(score.gt_s(0.9), "critical")
        .when_s(score.gt_s(0.7), "high")
        .when_s(score.gt_s(0.4), "medium")
        .otherwise_s("low")
}"#,
            ),
            model(
                "unit_price",
                [
                    ("total", ParseAsType::F64, false),
                    ("qty", ParseAsType::I64, false),
                ],
                ParseAsType::F64,
                false,
                r#"fn unit_price(total: F64Column, qty: I64Column) -> F64Column {
    reject_where_s(total.lt_s(0.0), "negative total");
    total.div(qty.cast_f64())
}"#,
            ),
            model(
                "minmax_norm",
                [("x", ParseAsType::F64, false)],
                ParseAsType::F64,
                false,
                r#"fn minmax_norm(x: F64Column) -> F64Column {
    match x.min() {
        None => x,
        Some(mn) => match x.max() {
            None => x,
            Some(mx) => {
                if mx == mn {
                    x.mul_s(0.0)
                } else {
                    x.sub_s(mn).div_s(mx - mn)
                }
            }
        }
    }
}"#,
            ),
            model(
                "has_pii_tag",
                [(
                    "tags",
                    ParseAsType::Vec {
                        element: Box::new(ParseAsType::String),
                    },
                    false,
                )],
                ParseAsType::Bool,
                false,
                r#"fn has_pii_tag(tags: VecStringColumn) -> BoolColumn {
    tags.contains_s("ssn").or(tags.contains_s("dob"))
}"#,
            ),
            model(
                "sample_flag",
                [("rate", ParseAsType::F64, false)],
                ParseAsType::Bool,
                true,
                r#"fn sample_flag(rate: F64Column) -> BoolColumn {
    rand_f64().lt(rate)
}"#,
            ),
            model(
                "is_expired",
                [("expires", ParseAsType::Datetime, false)],
                ParseAsType::Bool,
                true,
                r#"fn is_expired(expires: DatetimeColumn) -> BoolColumn {
    expires.lt_s(now())
}"#,
            ),
        ];

        UdfExecutor::compile_sync(models).expect("documented UDF shapes should compile");
    }
}
