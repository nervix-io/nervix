//! Tests of list conversions compiled into programs.
//!
//! Layer: test harness.
//!
//! - **Owns.** The type, nullability and sensitivity a list conversion compiles to, the
//!   conversions refused when a program compiles, and executing list conversions inside
//!   conditional arms with the row errors and spans they record.
//! - **Depends on.** The VM compiler, the runtime and the semantic program parser.
//! - **Must not know.** How the kernel converts a list column.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use arrow_array::{ArrayRef, BooleanArray, FixedSizeListArray, Int64Array, ListArray, StringArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use nervix_primitives::sync::StdArc;

use super::execute_program_sync;
use crate::{
    CastTarget, CompileBinding, CompileError, CompileOptions, CompiledProgram, InstructionKind,
    SchemaSensitivity, SideErrorReason, TypedArray, TypedBatch,
    compile_program_with_options_for_bindings,
    compile_program_with_options_for_bindings_with_sensitivity, program::CastFailure,
    test_support::parse_program,
};

fn item(data_type: DataType) -> FieldRef {
    StdArc::new(Field::new("item", data_type, false))
}

fn vec_type(element: DataType) -> DataType {
    DataType::List(item(element))
}

fn array_type(element: DataType, size: i32) -> DataType {
    DataType::FixedSizeList(item(element), size)
}

fn schema(fields: Vec<Field>) -> StdArc<Schema> {
    StdArc::new(Schema::new(fields))
}

fn compile(
    source: &str,
    input_schema: &StdArc<Schema>,
    outputs: Vec<Field>,
) -> error_stack::Result<CompiledProgram, CompileError> {
    let mut fields = Vec::new();
    for field in input_schema.fields() {
        fields.push(field.as_ref().clone());
    }
    fields.extend(outputs);
    let parsed = parse_program(source).expect("the conversion program must parse");
    compile_program_with_options_for_bindings(
        &parsed,
        schema(fields),
        [CompileBinding::writable("input", input_schema.clone())],
        CompileOptions::default(),
    )
}

/// A `VEC<I64>` column whose rows hold `lengths` elements each, from 1 upward.
fn integer_vecs(lengths: &[usize]) -> TypedArray {
    let mut elements = Vec::new();
    for length in lengths {
        for _ in 0..*length {
            let next = i64::try_from(elements.len()).expect("a test list is short") + 1;
            elements.push(next);
        }
    }
    let list = ListArray::try_new(
        item(DataType::Int64),
        OffsetBuffer::from_lengths(lengths.iter().copied()),
        StdArc::new(Int64Array::from(elements)),
        None,
    )
    .expect("the lengths cover exactly the elements");
    TypedArray::Generic(StdArc::new(list))
}

#[test]
fn a_list_conversion_takes_its_target_type_and_the_nullability_of_its_policy() {
    let input_schema = schema(vec![
        Field::new("required", vec_type(DataType::Int64), false),
        Field::new("optional", vec_type(DataType::Int64), true),
    ]);
    let pair = array_type(DataType::Float32, 2);
    compile(
        "SET result = input.required AS ARRAY<F32, 2>",
        &input_schema,
        vec![Field::new("result", pair.clone(), false)],
    )
    .expect("AS of a required list is required");
    for source in [
        "SET result = input.optional AS ARRAY<F32, 2>",
        // A conversion that can never fail is still optional under TRY_CAST.
        "SET result = TRY_CAST(input.required AS VEC<I64>)",
    ] {
        let target = if source.contains("VEC<I64>") {
            vec_type(DataType::Int64)
        } else {
            pair.clone()
        };
        let error = compile(
            source,
            &input_schema,
            vec![Field::new("result", target.clone(), false)],
        )
        .expect_err("the conversion may be null");
        assert_eq!(
            error.current_context().code(),
            "null_for_required_field",
            "{source}"
        );
        compile(
            source,
            &input_schema,
            vec![Field::new("result", target, true)],
        )
        .unwrap_or_else(|error| panic!("`{source}` initializes an optional field: {error:?}"));
    }
    let error = compile(
        "SET result = input.required AS VEC<F64>",
        &input_schema,
        vec![Field::new("result", vec_type(DataType::Float32), true)],
    )
    .expect_err("the result has exactly the target type");
    assert_eq!(error.current_context().code(), "type_mismatch");
}

#[test]
fn a_list_conversion_that_never_succeeds_is_refused_when_the_program_compiles() {
    let input_schema = schema(vec![
        Field::new("counts", vec_type(DataType::Int64), true),
        Field::new("pair", array_type(DataType::Int64, 2), true),
        Field::new("chunks", vec_type(DataType::Binary), true),
        Field::new("name", DataType::Utf8, true),
    ]);
    for (source, result, message) in [
        (
            "SET result = input.counts AS STRING",
            DataType::Utf8,
            "an ARRAY or VEC value converts only to an ARRAY or VEC type",
        ),
        (
            "SET result = TRY_CAST(input.pair AS I64)",
            DataType::Int64,
            "an ARRAY or VEC value converts only to an ARRAY or VEC type",
        ),
        (
            "SET result = input.name AS VEC<STRING>",
            vec_type(DataType::Utf8),
            "only an ARRAY or VEC value converts to an ARRAY or VEC type",
        ),
        (
            "SET result = TRY_CAST(input.counts AS VEC<VEC<I64>>)",
            vec_type(vec_type(DataType::Int64)),
            "an ARRAY or VEC value nested 1 deep converts only to a type nested 1 deep, found one \
             nested 2 deep",
        ),
        (
            "SET result = input.pair AS ARRAY<I64, 3>",
            array_type(DataType::Int64, 3),
            "an ARRAY of length 2 never converts to an ARRAY of length 3",
        ),
        (
            "SET result = TRY_CAST(input.chunks AS VEC<STRING>)",
            vec_type(DataType::Utf8),
            "BYTES conversions require bytes_from_utf8, bytes_to_utf8, base64 or hex functions",
        ),
    ] {
        let error = compile(
            source,
            &input_schema,
            vec![Field::new("result", result, true)],
        )
        .expect_err("the conversion can never succeed");
        assert_eq!(
            error.current_context().code(),
            "unsupported_cast",
            "{source}"
        );
        assert_eq!(error.current_context().message, message, "{source}");
    }
}

fn cast_instruction(compiled: &CompiledProgram, on_failure: CastFailure) -> &crate::Instruction {
    compiled
        .instructions
        .iter()
        .find(|instruction| {
            matches!(
                &instruction.kind,
                InstructionKind::Cast {
                    target: CastTarget::List(_),
                    on_failure: cast_failure,
                    ..
                } if *cast_failure == on_failure
            )
        })
        .expect("the program compiles the list conversion")
}

#[test]
fn a_conditional_arm_reports_the_failures_of_only_the_rows_it_selects() {
    let input_schema = schema(vec![
        Field::new("strict", DataType::Boolean, false),
        Field::new("counts", vec_type(DataType::Int64), false),
    ]);
    // A CASE selects only scalar results, so each arm reduces the list it converts.
    let compiled = compile(
        "SET total = CASE WHEN input.strict THEN sum(input.counts AS ARRAY<I64, 2>) ELSE \
         sum(TRY_CAST(input.counts AS ARRAY<I64, 2>)) END",
        &input_schema,
        vec![Field::new("total", DataType::Int64, true)],
    )
    .expect("both arms reduce a converted ARRAY");
    let batch = TypedBatch::try_new(
        input_schema,
        vec![
            TypedArray::Boolean(BooleanArray::from(vec![true, true, false, false])),
            integer_vecs(&[2, 1, 3, 2]),
        ],
    )
    .expect("the batch must build");
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert_eq!(
        output.column(2),
        &TypedArray::Int64(Int64Array::from(vec![Some(3), None, None, Some(15)]))
    );

    let strict_span = cast_instruction(&compiled, CastFailure::Error).span;
    let mut reported = Vec::new();
    for (row, errors) in output.errors().iter().enumerate() {
        for error in errors {
            assert_eq!(
                error.span, strict_span,
                "the strict conversion reports the failure"
            );
            reported.push((row, error.reason.clone()));
        }
    }
    assert_eq!(
        reported,
        [(
            1,
            SideErrorReason::ListCastLengthMismatch {
                expected: 2,
                found: 1,
            }
        )],
        "only the selected strict row fails; the tolerant list of three is null without an error"
    );
}

#[test]
fn a_tolerant_list_conversion_of_text_runs_only_over_the_rows_its_arm_selects() {
    let input_schema = schema(vec![
        Field::new("convert", DataType::Boolean, false),
        Field::new("texts", vec_type(DataType::Utf8), false),
        Field::new("counts", vec_type(DataType::Int64), false),
    ]);
    let compiled = compile(
        "SET parsed = CASE WHEN input.convert THEN sum(TRY_CAST(input.texts AS VEC<I64>)) END, \
         widened = CASE WHEN input.convert THEN sum(TRY_CAST(input.counts AS VEC<F64>)) END",
        &input_schema,
        vec![
            Field::new("parsed", DataType::Int64, true),
            Field::new("widened", DataType::Float64, true),
        ],
    )
    .expect("both conversions compile");
    let mut selections = Vec::new();
    for instruction in &compiled.instructions {
        if let InstructionKind::Cast {
            target: CastTarget::List(_),
            ..
        } = &instruction.kind
        {
            selections.push(instruction.selection.is_some());
        }
    }
    assert_eq!(
        selections,
        [true, false],
        "parsing text runs over the selected rows, and widening numbers over the whole batch"
    );

    let texts = ListArray::try_new(
        item(DataType::Utf8),
        OffsetBuffer::from_lengths([1, 1]),
        StdArc::new(StringArray::from(vec!["7", "x"])),
        None,
    )
    .expect("the lengths cover exactly the texts");
    let batch = TypedBatch::try_new(
        input_schema,
        vec![
            TypedArray::Boolean(BooleanArray::from(vec![true, false])),
            TypedArray::Generic(StdArc::new(texts)),
            integer_vecs(&[1, 1]),
        ],
    )
    .expect("the batch must build");
    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");
    assert_eq!(
        output.column(3),
        &TypedArray::Int64(Int64Array::from(vec![Some(7), None]))
    );
    assert_eq!(
        output.column(4),
        &TypedArray::Float64(arrow_array::Float64Array::from(vec![Some(1.0), None]))
    );
    assert!(output.errors().is_error_free());
}

#[test]
fn a_list_conversion_keeps_the_sensitivity_of_its_operand() {
    let input_schema = schema(vec![Field::new("secret", vec_type(DataType::Int64), true)]);
    let compile_sensitive = |source: &str| {
        let parsed = parse_program(source).expect("the conversion program must parse");
        compile_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            schema(vec![
                Field::new("secret", vec_type(DataType::Int64), true),
                Field::new("result", vec_type(DataType::Float64), true),
            ]),
            SchemaSensitivity::from_sensitive_fields(["secret"]),
            [CompileBinding::writable("input", input_schema.clone())
                .with_sensitivity(SchemaSensitivity::from_sensitive_fields(["secret"]))],
            CompileOptions::default(),
        )
    };
    let leak = compile_sensitive("SET result = TRY_CAST(input.secret AS VEC<F64>)")
        .expect_err("a converted sensitive list stays sensitive");
    assert_eq!(leak.current_context().code(), "sensitive_leak");
    compile_sensitive("SET result = leak_sensitive(input.secret AS VEC<F64>)")
        .expect("an explicit leak removes the sensitivity");
}

/// The span every conversion a property runs is recorded against.
const PROPERTY_SPAN: crate::program::Span = crate::program::Span { start: 2, end: 7 };

/// One element a property draws: a value, and a flag that writes a text element malformed or
/// chooses a `BOOL` element.
#[derive(Debug, Clone, Copy, bolero::TypeGenerator)]
struct RawElement {
    value: i64,
    flag: bool,
}

/// One row of a generated column: whether it holds a list, and the elements it holds, or hides
/// when it holds none.
#[derive(Debug, Clone, bolero::TypeGenerator)]
struct RawRow {
    valid: bool,
    elements: Vec<RawElement>,
}

/// A pair of element types whose scalar conversion a list conversion applies to every element.
#[derive(Debug, Clone, Copy, bolero::TypeGenerator)]
enum ElementPair {
    IntegerToNarrow,
    IntegerToUnsigned,
    IntegerToFloat,
    IntegerToText,
    IntegerToDatetime,
    IntegerToItself,
    TextToInteger,
    FloatToInteger,
    FloatToText,
    FlagToDatetime,
}

impl ElementPair {
    /// The element type converted from, and the one converted to.
    fn types(self) -> (DataType, DataType) {
        let datetime =
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("+00:00".into()));
        match self {
            Self::IntegerToNarrow => (DataType::Int64, DataType::Int8),
            Self::IntegerToUnsigned => (DataType::Int64, DataType::UInt8),
            Self::IntegerToFloat => (DataType::Int64, DataType::Float32),
            Self::IntegerToText => (DataType::Int64, DataType::Utf8),
            Self::IntegerToDatetime => (DataType::Int64, datetime),
            Self::IntegerToItself => (DataType::Int64, DataType::Int64),
            Self::TextToInteger => (DataType::Utf8, DataType::Int64),
            Self::FloatToInteger => (DataType::Float64, DataType::Int32),
            Self::FloatToText => (DataType::Float64, DataType::Utf8),
            Self::FlagToDatetime => (DataType::Boolean, datetime),
        }
    }

    /// The elements `raw` names, of the type converted from, with a null for `None`.
    fn elements(self, raw: &[Option<RawElement>]) -> ArrayRef {
        match self {
            Self::IntegerToNarrow
            | Self::IntegerToUnsigned
            | Self::IntegerToFloat
            | Self::IntegerToText
            | Self::IntegerToDatetime
            | Self::IntegerToItself => {
                let mut values = Vec::with_capacity(raw.len());
                for element in raw {
                    values.push(element.map(|element| element.value));
                }
                StdArc::new(Int64Array::from(values))
            }
            Self::TextToInteger => {
                let mut values = Vec::with_capacity(raw.len());
                for element in raw {
                    let text = match element {
                        Some(RawElement { value, flag: true }) => Some(format!("{value}x")),
                        Some(RawElement { value, flag: false }) => Some(value.to_string()),
                        None => None,
                    };
                    values.push(text);
                }
                StdArc::new(StringArray::from(values))
            }
            Self::FloatToInteger | Self::FloatToText => {
                let mut values = Vec::with_capacity(raw.len());
                for element in raw {
                    values
                        .push(element.map(|element| f64::from_bits(element.value.cast_unsigned())));
                }
                StdArc::new(arrow_array::Float64Array::from(values))
            }
            Self::FlagToDatetime => {
                let mut values = Vec::with_capacity(raw.len());
                for element in raw {
                    values.push(element.map(|element| element.flag));
                }
                StdArc::new(BooleanArray::from(values))
            }
        }
    }
}

/// The containers of a generated column and of the type it converts to.
#[derive(Debug, Clone, Copy, bolero::TypeGenerator)]
enum Containers {
    VecToVec,
    ArrayToArray { size: u8 },
    ArrayToVec { size: u8 },
}

/// The length of a generated `ARRAY`, from one to four.
fn array_length(size: u8) -> usize {
    usize::from(size % 4) + 1
}

/// A generated list column, the slice of it a conversion reads, and the element pair it converts.
#[derive(Debug, Clone, bolero::TypeGenerator)]
struct ConvertedColumn {
    pair: ElementPair,
    containers: Containers,
    rows: Vec<RawRow>,
    /// Whether a null `ARRAY` row hides null elements, which Arrow allows beneath a null value.
    hidden_nulls: bool,
    slice_start: u8,
    slice_length: u8,
}

/// The rows a generated slice reads: from `start` modulo the rows, at most `length` modulo what
/// remains.
fn sliced(rows: usize, start: u8, length: u8) -> std::ops::Range<usize> {
    let start = usize::from(start) % (rows + 1);
    let length = usize::from(length) % (rows - start + 1);
    start..start + length
}

/// Every element of every row, `None` for a null element, laid out as the column holds them.
fn column_of(
    pair: ElementPair,
    rows: &[Vec<Option<RawElement>>],
    valid: &[bool],
    array_size: Option<usize>,
) -> ArrayRef {
    let (element_type, _) = pair.types();
    let mut raw = Vec::new();
    let mut lengths = Vec::with_capacity(rows.len());
    for row in rows {
        raw.extend(row.iter().copied());
        lengths.push(row.len());
    }
    let elements = pair.elements(&raw);
    let nulls = if valid.contains(&false) {
        Some(NullBuffer::from(valid.to_vec()))
    } else {
        None
    };
    let column: ArrayRef = match array_size {
        Some(size) => StdArc::new(
            FixedSizeListArray::try_new(
                item(element_type),
                i32::try_from(size).expect("a generated ARRAY is at most four long"),
                elements,
                nulls,
            )
            .expect("every generated ARRAY row holds exactly its length"),
        ),
        None => StdArc::new(
            ListArray::try_new(
                item(element_type),
                OffsetBuffer::from_lengths(lengths),
                elements,
                nulls,
            )
            .expect("the generated lengths cover exactly the generated elements"),
        ),
    };
    column
}

/// One element of a list slot as an Arrow array, which equality compares bit for bit.
fn slot(column: &ArrayRef, row: usize) -> ArrayRef {
    match column.data_type() {
        DataType::List(_) => column
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("a VEC column is a ListArray")
            .value(row),
        _ => column
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("an ARRAY column is a FixedSizeListArray")
            .value(row),
    }
}

/// What converting a column under one failure policy yields.
struct PolicyConversion {
    on_failure: CastFailure,
    converted: ArrayRef,
    errors: crate::RowErrors,
}

/// Converts `column` under both policies with the plan for `value` AS `target`.
fn convert_both(value: &DataType, target: &DataType, column: &ArrayRef) -> Vec<PolicyConversion> {
    let CastTarget::List(plan) =
        CastTarget::between(value, target).expect("the generated pair converts")
    else {
        panic!("the generated pair is a list conversion");
    };
    let mut conversions = Vec::with_capacity(2);
    for on_failure in [CastFailure::Error, CastFailure::Null] {
        let mut errors = crate::RowErrors::new(column.len());
        let converted = plan
            .convert(
                TypedArray::Generic(StdArc::clone(column)),
                on_failure,
                &mut errors,
                PROPERTY_SPAN,
            )
            .expect("a column of the plan's type converts");
        let TypedArray::Generic(converted) = converted else {
            panic!("a list conversion yields a list column");
        };
        assert_eq!(converted.data_type(), target);
        converted
            .to_data()
            .validate_full()
            .expect("a converted column is a valid Arrow array");
        conversions.push(PolicyConversion {
            on_failure,
            converted,
            errors,
        });
    }
    conversions
}

/// The null elements the one level of a list column holds, whatever its validity.
fn element_nulls(column: &ArrayRef) -> usize {
    match column.data_type() {
        DataType::List(_) => column
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("a VEC column is a ListArray")
            .values()
            .null_count(),
        _ => column
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("an ARRAY column is a FixedSizeListArray")
            .values()
            .null_count(),
    }
}

/// The reasons `errors` records on `row`.
fn row_reasons(errors: &crate::RowErrors, row: usize) -> Vec<SideErrorReason> {
    let mut reasons = Vec::new();
    for error in errors.row(row) {
        assert_eq!(error.span, PROPERTY_SPAN);
        reasons.push(error.reason.clone());
    }
    reasons
}

/// Every list a conversion converts equals the scalar `AS` of each of its elements, a list with an
/// element that `AS` fails is null and fails under `AS` alone, and a null list stays null whatever
/// it hides.
#[test]
fn bolero_list_conversions_equal_the_scalar_conversion_of_each_element() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<ConvertedColumn>()
        .for_each(|case| {
            let (element_type, target_element) = case.pair.types();
            let target_register = crate::RegisterType::from_data_type(&target_element)
                .expect("every generated target element has a register type");
            let (array_size, value_type, target_type) = match case.containers {
                Containers::VecToVec => (
                    None,
                    vec_type(element_type.clone()),
                    vec_type(target_element.clone()),
                ),
                Containers::ArrayToArray { size } => {
                    let size = array_length(size);
                    let length = i32::try_from(size).expect("a generated ARRAY is short");
                    (
                        Some(size),
                        array_type(element_type.clone(), length),
                        array_type(target_element.clone(), length),
                    )
                }
                Containers::ArrayToVec { size } => {
                    let size = array_length(size);
                    let length = i32::try_from(size).expect("a generated ARRAY is short");
                    (
                        Some(size),
                        array_type(element_type.clone(), length),
                        vec_type(target_element.clone()),
                    )
                }
            };
            let mut rows = Vec::with_capacity(case.rows.len());
            let mut valid = Vec::with_capacity(case.rows.len());
            for row in &case.rows {
                let mut elements = Vec::new();
                for element in &row.elements {
                    elements.push(Some(*element));
                }
                if let Some(size) = array_size {
                    elements.resize(
                        size,
                        Some(RawElement {
                            value: 0,
                            flag: false,
                        }),
                    );
                    if !row.valid && case.hidden_nulls {
                        elements = vec![None; size];
                    }
                }
                rows.push(elements);
                valid.push(row.valid);
            }
            let full = column_of(case.pair, &rows, &valid, array_size);
            let range = sliced(rows.len(), case.slice_start, case.slice_length);
            let column = full.slice(range.start, range.len());
            for PolicyConversion {
                on_failure,
                converted,
                errors,
            } in convert_both(&value_type, &target_type, &column)
            {
                assert!(
                    element_nulls(&converted) <= element_nulls(&column),
                    "a conversion adds no null element, beneath a null list included"
                );
                for (index, row) in range.clone().enumerate() {
                    if !valid[row] {
                        assert!(converted.is_null(index), "a null list stays null");
                        assert!(row_reasons(&errors, index).is_empty());
                        continue;
                    }
                    let elements = TypedArray::try_from_array_ref(case.pair.elements(&rows[row]))
                        .expect("the generated elements have a register type");
                    let mut scalar_errors = crate::RowErrors::new(elements.len());
                    let scalar = super::cast_typed_array(
                        elements,
                        target_register,
                        CastFailure::Error,
                        &mut scalar_errors,
                        PROPERTY_SPAN,
                    )
                    .expect("every generated pair has a scalar conversion kernel");
                    let reasons = row_reasons(&errors, index);
                    if scalar_errors.is_error_free() {
                        assert!(converted.is_valid(index), "a converting list is not null");
                        assert_eq!(
                            slot(&converted, index).as_ref(),
                            scalar.into_array_ref().as_ref(),
                            "the list holds the scalar conversion of each element"
                        );
                        assert!(reasons.is_empty());
                    } else {
                        assert!(
                            converted.is_null(index),
                            "a list with a failed element is null"
                        );
                        let expected = match on_failure {
                            CastFailure::Error => vec![SideErrorReason::ListElementCastFailed {
                                target: target_register,
                            }],
                            CastFailure::Null => Vec::new(),
                        };
                        assert_eq!(reasons, expected);
                    }
                }
            }
        });
}

/// One row of a generated `VEC` column that becomes an `ARRAY`: whether it holds a list, whether
/// it holds exactly the `ARRAY`'s length, and its elements.
#[derive(Debug, Clone, bolero::TypeGenerator)]
struct ReshapedRow {
    valid: bool,
    fits: bool,
    elements: Vec<i64>,
}

/// A generated `VEC<I64>` column converted to an `ARRAY` of a generated length.
#[derive(Debug, Clone, bolero::TypeGenerator)]
struct ReshapedColumn {
    rows: Vec<ReshapedRow>,
    size: u8,
    /// Whether the elements convert to `F64` as well, or keep their type.
    widen: bool,
    slice_start: u8,
    slice_length: u8,
}

/// A `VEC` becomes an `ARRAY` exactly when it holds the `ARRAY`'s length: any other length fails
/// its row under `AS` with both lengths and nulls it under `TRY_CAST`, and a null row stays null.
#[test]
fn bolero_vecs_of_another_length_than_the_array_fail_their_rows() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<ReshapedColumn>()
        .for_each(|case| {
            let size = array_length(case.size);
            let length = i32::try_from(size).expect("a generated ARRAY is short");
            let target_element = if case.widen {
                DataType::Float64
            } else {
                DataType::Int64
            };
            let mut rows = Vec::with_capacity(case.rows.len());
            let mut valid = Vec::with_capacity(case.rows.len());
            for row in &case.rows {
                let mut elements = Vec::new();
                for element in &row.elements {
                    elements.push(Some(RawElement {
                        value: *element,
                        flag: false,
                    }));
                }
                if row.fits {
                    elements.resize(
                        size,
                        Some(RawElement {
                            value: 7,
                            flag: false,
                        }),
                    );
                }
                rows.push(elements);
                valid.push(row.valid);
            }
            let full = column_of(ElementPair::IntegerToItself, &rows, &valid, None);
            let range = sliced(rows.len(), case.slice_start, case.slice_length);
            let column = full.slice(range.start, range.len());
            let target_type = array_type(target_element.clone(), length);
            for PolicyConversion {
                on_failure,
                converted,
                errors,
            } in convert_both(&vec_type(DataType::Int64), &target_type, &column)
            {
                assert_eq!(
                    element_nulls(&converted),
                    0,
                    "an ARRAY made of VECs holds no null element, beneath a null list included"
                );
                for (index, row) in range.clone().enumerate() {
                    let reasons = row_reasons(&errors, index);
                    if !valid[row] {
                        assert!(converted.is_null(index), "a null list stays null");
                        assert!(reasons.is_empty());
                        continue;
                    }
                    let found = rows[row].len();
                    if found != size {
                        assert!(converted.is_null(index), "a list of another length is null");
                        let expected = match on_failure {
                            CastFailure::Error => vec![SideErrorReason::ListCastLengthMismatch {
                                expected: size,
                                found,
                            }],
                            CastFailure::Null => Vec::new(),
                        };
                        assert_eq!(reasons, expected);
                        continue;
                    }
                    let elements = ElementPair::IntegerToItself.elements(&rows[row]);
                    let expected = arrow_cast::cast(&elements, &target_element)
                        .expect("I64 converts to F64 and to itself");
                    assert!(converted.is_valid(index));
                    assert_eq!(slot(&converted, index).as_ref(), expected.as_ref());
                    assert!(reasons.is_empty());
                }
            }
        });
}
