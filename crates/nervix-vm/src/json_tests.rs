//! Tests for the scan that answers JSON extractions from one parse of each document.
//!
//! Layer: test harness.
//!
//! - **Owns.** Reading every declared target type, every defect the scan distinguishes, the
//!   document limits, and the rows a scan reads, straight from the scan over a column.
//! - **Depends on.** The JSON scan, its declared targets and its defects.
//! - **Must not know.** Registers, instructions, or how a program is compiled.

use std::num::NonZeroU32;

use arrow_array::{
    Array, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int32Array,
    Int64Array, ListArray, StringArray, StructArray, UInt8Array, UInt16Array, types::Int64Type,
};
use nervix_models::{JsonPath, ParseAsType};
use triomphe::Arc;

use super::{
    JsonDefect, JsonExtraction, JsonKind, JsonOperation, JsonOutput, JsonPlace, JsonScanOutput,
    JsonTarget, JsonTargetDefect, MAX_DOCUMENT_BYTES, MAX_DOCUMENT_DEPTH, ValueColumn, ValueDefect,
    scan,
};
use crate::{
    error::{ErrorCode, RowErrors, SideErrorReason},
    program::{CastFailure, Span},
};

fn span(operation: usize) -> Span {
    Span {
        start: operation,
        end: operation + 1,
    }
}

fn path(text: &str) -> Arc<JsonPath> {
    Arc::new(JsonPath::parse(text).expect("test paths are valid"))
}

fn target(declared: &ParseAsType) -> Arc<JsonTarget> {
    Arc::new(JsonTarget::try_from(declared).expect("test targets are readable"))
}

fn vec_of(element: ParseAsType) -> ParseAsType {
    ParseAsType::Vec {
        element: Box::new(element),
    }
}

fn array_of(element: ParseAsType, len: u32) -> ParseAsType {
    ParseAsType::Array {
        element: Box::new(element),
        len: NonZeroU32::new(len).expect("test array lengths are positive"),
    }
}

/// A strict read of `path` as `declared`, recorded against operation `operation`.
fn strict(path_text: &str, declared: &ParseAsType, operation: usize) -> JsonScanOutput {
    read(path_text, declared, CastFailure::Error, operation)
}

/// A tolerant read of `path` as `declared`, recorded against operation `operation`.
fn tolerant(path_text: &str, declared: &ParseAsType, operation: usize) -> JsonScanOutput {
    read(path_text, declared, CastFailure::Null, operation)
}

fn read(
    path_text: &str,
    declared: &ParseAsType,
    on_failure: CastFailure,
    operation: usize,
) -> JsonScanOutput {
    JsonScanOutput {
        extraction: JsonExtraction {
            path: path(path_text),
            output: JsonOutput::Value {
                target: target(declared),
                on_failure,
            },
        },
        span: span(operation),
    }
}

fn exists(path_text: &str, operation: usize) -> JsonScanOutput {
    JsonScanOutput {
        extraction: JsonExtraction {
            path: path(path_text),
            output: JsonOutput::Exists,
        },
        span: span(operation),
    }
}

/// The answers of one scan over every row of `documents`, and the errors it recorded.
struct Scanned {
    answers: StructArray,
    errors: RowErrors,
}

impl Scanned {
    fn of(documents: &[Option<&str>], outputs: &[JsonScanOutput]) -> Self {
        Self::selecting(documents, outputs, |_| true)
    }

    fn selecting(
        documents: &[Option<&str>],
        outputs: &[JsonScanOutput],
        selected: impl Fn(usize) -> bool,
    ) -> Self {
        let documents = StringArray::from(documents.to_vec());
        let mut errors = RowErrors::new(documents.len());
        let answers = scan(&documents, selected, outputs, &mut errors);
        Self { answers, errors }
    }

    fn column<A: Array + Clone + 'static>(&self, index: usize) -> A {
        self.answers
            .column(index)
            .as_any()
            .downcast_ref::<A>()
            .expect("the answer has its declared Arrow type")
            .clone()
    }

    /// The reasons recorded for `row`, with the operation each is recorded against.
    fn reasons(&self, row: usize) -> Vec<(SideErrorReason, Span)> {
        let mut reasons = Vec::new();
        for error in self.errors.row(row) {
            reasons.push((error.reason.clone(), error.span));
        }
        reasons
    }
}

fn json(defect: JsonDefect, operation: usize) -> (SideErrorReason, Span) {
    (SideErrorReason::Json(defect), span(operation))
}

#[test]
fn reads_every_scalar_target_from_one_parse_of_each_document() {
    let documents = [
        Some(
            r#"{"i":-9223372036854775808,"u":18446744073709551615,"b":true,"f":0.25,"s":"café \"q\" \\ 😀","n":null,"small":255,"whole":2.0}"#,
        ),
        Some(r#"{"i":7,"b":false,"f":3,"s":"","small":0,"whole":-1e2}"#),
        Some("{}"),
        None,
    ];
    let outputs = [
        strict("$.i", &ParseAsType::I64, 0),
        strict("$.u", &ParseAsType::U64, 1),
        strict("$.b", &ParseAsType::Bool, 2),
        strict("$.f", &ParseAsType::F64, 3),
        strict("$.s", &ParseAsType::String, 4),
        strict("$.n", &ParseAsType::String, 5),
        strict("$.small", &ParseAsType::U8, 6),
        strict("$.whole", &ParseAsType::I8, 7),
        strict("$.f", &ParseAsType::F32, 8),
    ];
    let scanned = Scanned::of(&documents, &outputs);

    assert!(scanned.errors.is_error_free());
    assert_eq!(
        scanned.column::<Int64Array>(0),
        Int64Array::from(vec![Some(i64::MIN), Some(7), None, None])
    );
    assert_eq!(
        scanned.column::<arrow_array::UInt64Array>(1).value(0),
        u64::MAX
    );
    assert_eq!(
        scanned.column::<BooleanArray>(2),
        BooleanArray::from(vec![Some(true), Some(false), None, None])
    );
    assert_eq!(
        scanned.column::<Float64Array>(3),
        Float64Array::from(vec![Some(0.25), Some(3.0), None, None])
    );
    assert_eq!(
        scanned.column::<StringArray>(4),
        StringArray::from(vec![Some("café \"q\" \\ 😀"), Some(""), None, None])
    );
    assert_eq!(
        scanned.column::<StringArray>(5),
        StringArray::from(vec![None::<&str>, None, None, None]),
        "JSON null and a missing member both read as a typed null"
    );
    assert_eq!(
        scanned.column::<UInt8Array>(6),
        UInt8Array::from(vec![Some(255), Some(0), None, None])
    );
    assert_eq!(
        scanned.column::<Int8Array>(7),
        Int8Array::from(vec![Some(2), Some(-100), None, None]),
        "a number with no fractional part reads as an integer however it is written"
    );
    assert_eq!(
        scanned.column::<Float32Array>(8),
        Float32Array::from(vec![Some(0.25), Some(3.0), None, None])
    );
}

#[test]
fn reads_declared_vectors_arrays_and_nested_collections() {
    let documents = [
        Some(
            r#"{"tags":["a","b"],"point":[1.5,-2],"matrix":[[1,2],[3]],"orders":[{"items":[1]},{"items":[2,3]}],"empty":[]}"#,
        ),
        Some(r#"{"tags":[],"point":[0,0],"matrix":[],"orders":[]}"#),
    ];
    let outputs = [
        strict("$.tags", &vec_of(ParseAsType::String), 0),
        strict("$.point", &array_of(ParseAsType::F64, 2), 1),
        strict("$.matrix", &vec_of(vec_of(ParseAsType::I32)), 2),
        strict("$.orders[1].items", &vec_of(ParseAsType::I64), 3),
        strict("$.empty", &vec_of(ParseAsType::I64), 4),
    ];
    let scanned = Scanned::of(&documents, &outputs);
    assert!(scanned.errors.is_error_free());

    let tags = scanned.column::<ListArray>(0);
    assert_eq!(tags.value_offsets(), &[0, 2, 2]);
    assert_eq!(
        tags.values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("VEC<STRING> holds strings"),
        &StringArray::from(vec!["a", "b"])
    );
    assert_eq!(
        tags.data_type(),
        &target(&vec_of(ParseAsType::String)).data_type()
    );

    let point = scanned.column::<FixedSizeListArray>(1);
    assert_eq!(
        point
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("ARRAY<F64, 2> holds F64"),
        &Float64Array::from(vec![1.5, -2.0, 0.0, 0.0])
    );

    let matrix = scanned.column::<ListArray>(2);
    let rows = matrix
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("VEC<VEC<I32>> holds vectors");
    assert_eq!(matrix.value_offsets(), &[0, 2, 2]);
    assert_eq!(rows.value_offsets(), &[0, 2, 3]);
    assert_eq!(
        rows.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("the innermost elements are I32"),
        &Int32Array::from(vec![1, 2, 3])
    );

    let items = scanned.column::<ListArray>(3);
    let expected_items =
        ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(2), Some(3)]), None]);
    assert_eq!(items.value_offsets(), expected_items.value_offsets());
    assert_eq!(items.values().as_ref(), expected_items.values().as_ref());
    assert!(
        items.is_null(1),
        "an index past the end of the array is missing"
    );

    let empty = scanned.column::<ListArray>(4);
    assert!(empty.is_valid(0) && empty.value_length(0) == 0);
    assert!(empty.is_null(1));
}

#[test]
fn a_failed_collection_leaves_nothing_of_itself_behind() {
    let documents = [
        Some(r#"{"v":[[1,2],[3,"x"]]}"#),
        Some(r#"{"v":[[4],[5,6]]}"#),
    ];
    let outputs = [strict("$.v", &vec_of(vec_of(ParseAsType::I64)), 0)];
    let scanned = Scanned::of(&documents, &outputs);

    let vectors = scanned.column::<ListArray>(0);
    assert!(vectors.is_null(0));
    let inner = vectors
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("VEC<VEC<I64>> holds vectors");
    assert_eq!(vectors.value_offsets(), &[0, 0, 2]);
    assert_eq!(inner.value_offsets(), &[0, 1, 3]);
    assert_eq!(
        inner
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("the innermost elements are I64"),
        &Int64Array::from(vec![4, 5, 6])
    );
    assert_eq!(
        scanned.reasons(0),
        [json(
            JsonDefect::TypeMismatch {
                path: path("$.v"),
                place: JsonPlace::Element,
                found: JsonKind::String,
                expected: target(&ParseAsType::I64),
            },
            0
        )]
    );
    let arrays = Scanned::of(
        &[Some(r#"{"p":[[1,2],[3]]}"#), Some(r#"{"p":[[4,5],[6,7]]}"#)],
        &[strict(
            "$.p",
            &array_of(array_of(ParseAsType::I64, 2), 2),
            0,
        )],
    );
    let fixed = arrays.column::<FixedSizeListArray>(0);
    assert!(fixed.is_null(0));
    assert_eq!(
        fixed.values().len(),
        4,
        "a null fixed array keeps its width of placeholders"
    );
    let innermost = fixed
        .values()
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("ARRAY<ARRAY<I64, 2>, 2> holds arrays")
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("the innermost elements are I64")
        .clone();
    assert_eq!(innermost.values().as_ref()[4..], [4, 5, 6, 7]);
    assert_eq!(
        arrays.reasons(0),
        [json(
            JsonDefect::ArrayLength {
                path: path("$.p"),
                place: JsonPlace::Element,
                found: 1,
                declared: NonZeroU32::new(2).expect("two is positive"),
            },
            0
        )]
    );
}

#[test]
fn distinguishes_each_defect_and_reports_it_against_its_own_operation() {
    let documents = [
        Some(r#"{"count":7,"#),
        Some(r#"{"count":"7"}"#),
        Some(r#"{"count":1.5}"#),
        Some(r#"{"level":256}"#),
        Some(r#"{"count":18446744073709551616}"#),
        Some(r#"{"tags":[1,null]}"#),
        Some(r#"{"point":[1,2,3]}"#),
        Some(r#"{"level":-1e3,"count":{"a":1},"tags":{}}"#),
    ];
    let outputs = [
        strict("$.count", &ParseAsType::I64, 0),
        strict("$.level", &ParseAsType::U8, 1),
        strict("$.tags", &vec_of(ParseAsType::I64), 2),
        strict("$.point", &array_of(ParseAsType::F64, 2), 3),
        exists("$.count", 4),
    ];
    let scanned = Scanned::of(&documents, &outputs);

    let malformed = |operation, index| {
        let output: &JsonScanOutput = &outputs[index];
        json(
            JsonDefect::Malformed(output.extraction.output.operation()),
            operation,
        )
    };
    assert_eq!(
        scanned.reasons(0),
        [
            malformed(0, 0),
            malformed(1, 1),
            malformed(2, 2),
            malformed(3, 3),
            malformed(4, 4)
        ]
    );
    let mismatch = |text, place, found, declared: &ParseAsType| JsonDefect::TypeMismatch {
        path: path(text),
        place,
        found,
        expected: target(declared),
    };
    assert_eq!(
        scanned.reasons(1),
        [json(
            mismatch(
                "$.count",
                JsonPlace::Value,
                JsonKind::String,
                &ParseAsType::I64
            ),
            0
        )]
    );
    assert_eq!(
        scanned.reasons(2),
        [json(
            mismatch(
                "$.count",
                JsonPlace::Value,
                JsonKind::FractionalNumber,
                &ParseAsType::I64
            ),
            0
        )]
    );
    assert_eq!(
        scanned.reasons(3),
        [json(
            JsonDefect::OutOfRange {
                path: path("$.level"),
                place: JsonPlace::Value,
                target: target(&ParseAsType::U8),
            },
            1
        )]
    );
    assert_eq!(
        scanned.reasons(4),
        [json(
            JsonDefect::OutOfRange {
                path: path("$.count"),
                place: JsonPlace::Value,
                target: target(&ParseAsType::I64),
            },
            0
        )],
        "an integer beyond 64 bits is a number out of range, not a malformed document"
    );
    assert_eq!(
        scanned.reasons(5),
        [json(
            mismatch(
                "$.tags",
                JsonPlace::Element,
                JsonKind::Null,
                &ParseAsType::I64
            ),
            2
        )]
    );
    assert_eq!(
        scanned.reasons(6),
        [json(
            JsonDefect::ArrayLength {
                path: path("$.point"),
                place: JsonPlace::Value,
                found: 3,
                declared: NonZeroU32::new(2).expect("two is positive"),
            },
            3
        )]
    );
    assert_eq!(
        scanned.reasons(7),
        [
            json(
                mismatch(
                    "$.count",
                    JsonPlace::Value,
                    JsonKind::Object,
                    &ParseAsType::I64
                ),
                0
            ),
            json(
                JsonDefect::OutOfRange {
                    path: path("$.level"),
                    place: JsonPlace::Value,
                    target: target(&ParseAsType::U8),
                },
                1
            ),
            json(
                mismatch(
                    "$.tags",
                    JsonPlace::Value,
                    JsonKind::Object,
                    &vec_of(ParseAsType::I64)
                ),
                2
            ),
        ]
    );
    let present = scanned.column::<BooleanArray>(4);
    assert!(
        present.is_null(0),
        "JSON_EXISTS answers null for a malformed document"
    );
    assert!(present.value(1) && present.value(7));
    assert!(!present.value(3));
}

#[test]
fn every_defect_names_its_failure_and_code() {
    let cases = [
        (
            JsonDefect::Malformed(JsonOperation::JsonValue),
            ErrorCode::CastFailed,
            "JSON_VALUE document is not valid JSON",
        ),
        (
            JsonDefect::DocumentTooLarge(JsonOperation::JsonExists),
            ErrorCode::InvalidArgument,
            "JSON_EXISTS document exceeds 16777216 bytes",
        ),
        (
            JsonDefect::DocumentTooDeep(JsonOperation::JsonValue),
            ErrorCode::InvalidArgument,
            "JSON_VALUE document nests deeper than 128 levels",
        ),
        (
            JsonDefect::TypeMismatch {
                path: path("$.count"),
                place: JsonPlace::Value,
                found: JsonKind::String,
                expected: target(&ParseAsType::I64),
            },
            ErrorCode::CastFailed,
            "JSON_VALUE found a JSON string at $.count where I64 is declared",
        ),
        (
            JsonDefect::TypeMismatch {
                path: path(r#"$["odd key"]"#),
                place: JsonPlace::Element,
                found: JsonKind::Boolean,
                expected: target(&array_of(ParseAsType::F32, 2)),
            },
            ErrorCode::CastFailed,
            r#"JSON_VALUE found a JSON boolean in $["odd key"] where ARRAY<F32, 2> elements are declared"#,
        ),
        (
            JsonDefect::OutOfRange {
                path: path("$.level"),
                place: JsonPlace::Value,
                target: target(&ParseAsType::U8),
            },
            ErrorCode::CastFailed,
            "JSON_VALUE number at $.level does not fit U8",
        ),
        (
            JsonDefect::ArrayLength {
                path: path("$.point"),
                place: JsonPlace::Value,
                found: 3,
                declared: NonZeroU32::new(2).expect("two is positive"),
            },
            ErrorCode::CastFailed,
            "JSON_VALUE array at $.point has 3 elements where 2 are declared",
        ),
        (
            JsonDefect::ResultTooLarge {
                operation: JsonOperation::TryJsonValue,
                target: target(&vec_of(ParseAsType::String)),
            },
            ErrorCode::Overflow,
            "TRY_JSON_VALUE result exceeds what one VEC<STRING> column holds",
        ),
    ];
    for (defect, code, message) in cases {
        assert_eq!(defect.code(), code, "{message}");
        assert_eq!(defect.to_string(), message);
        assert_eq!(SideErrorReason::Json(defect).code(), code, "{message}");
    }
    for (kind, text) in [
        (JsonKind::Null, "JSON null"),
        (JsonKind::Number, "a JSON number"),
        (JsonKind::Array, "a JSON array"),
    ] {
        assert_eq!(kind.to_string(), text);
    }
}

#[test]
fn a_tolerant_read_yields_a_typed_null_for_every_defect_of_its_document_or_value() {
    let documents = [
        Some(r#"{"count":7,"#),
        Some(r#"{"count":"7"}"#),
        Some(r#"{"count":300}"#),
        Some(r#"{"count":8}"#),
    ];
    let outputs = [
        tolerant("$.count", &ParseAsType::U8, 0),
        tolerant("$.count", &vec_of(ParseAsType::U8), 1),
    ];
    let scanned = Scanned::of(&documents, &outputs);

    assert!(scanned.errors.is_error_free());
    assert_eq!(
        scanned.column::<UInt8Array>(0),
        UInt8Array::from(vec![None, None, None, Some(8)])
    );
    assert_eq!(scanned.column::<ListArray>(1).null_count(), 4);
}

#[test]
fn reads_only_the_rows_it_selects() {
    let documents = [Some(r#"{"n":1}"#), Some("not json"), Some(r#"{"n":3}"#)];
    let outputs = [strict("$.n", &ParseAsType::I64, 0), exists("$.n", 1)];
    let scanned = Scanned::selecting(&documents, &outputs, |row| row != 1);

    assert!(
        scanned.errors.is_error_free(),
        "an unselected document is never parsed"
    );
    assert_eq!(
        scanned.column::<Int64Array>(0),
        Int64Array::from(vec![Some(1), None, Some(3)])
    );
    assert_eq!(
        scanned.column::<BooleanArray>(1),
        BooleanArray::from(vec![Some(true), None, Some(true)])
    );
}

#[test]
fn follows_paths_through_members_elements_and_duplicates() {
    let documents = [Some(
        r#"{"a":{"b":[10,{"c":"deep"}]},"dup":1,"dup":2,"odd key":{"":true},"list":[1,2,3],"scalar":4}"#,
    )];
    let outputs = [
        strict("$.a.b[1].c", &ParseAsType::String, 0),
        strict("$.dup", &ParseAsType::I64, 1),
        strict(r#"$["odd key"][""]"#, &ParseAsType::Bool, 2),
        exists("$.list[2]", 3),
        exists("$.list[3]", 4),
        exists("$.scalar.inner", 5),
        exists("$.list.inner", 6),
        exists("$[0]", 7),
        exists("$", 8),
        strict("$.a.b[0]", &ParseAsType::U16, 9),
    ];
    let scanned = Scanned::of(&documents, &outputs);

    assert!(scanned.errors.is_error_free());
    assert_eq!(scanned.column::<StringArray>(0).value(0), "deep");
    assert_eq!(
        scanned.column::<Int64Array>(1).value(0),
        2,
        "the last of several members with one name is the one read"
    );
    assert!(scanned.column::<BooleanArray>(2).value(0));
    let present = |index| scanned.column::<BooleanArray>(index).value(0);
    assert!(present(3));
    assert!(!present(4), "an index past the end is missing");
    assert!(!present(5), "a step into a number is missing");
    assert!(!present(6), "a member step into an array is missing");
    assert!(!present(7), "an element step into an object is missing");
    assert!(present(8));
    assert_eq!(
        scanned.column::<UInt16Array>(9),
        UInt16Array::from(vec![10])
    );
}

#[test]
fn enforces_the_document_size_and_depth_limits() {
    let too_large = format!("[{}]", "1,".repeat(MAX_DOCUMENT_BYTES / 2) + "1");
    let deepest = format!(
        "{}{}",
        "[".repeat(MAX_DOCUMENT_DEPTH),
        "]".repeat(MAX_DOCUMENT_DEPTH)
    );
    let too_deep = format!("[{deepest}]");
    let documents = [
        Some(too_large.as_str()),
        Some(deepest.as_str()),
        Some(too_deep.as_str()),
    ];
    let outputs = [
        strict("$[0]", &ParseAsType::I64, 0),
        tolerant("$[0]", &ParseAsType::I64, 1),
    ];
    let scanned = Scanned::of(&documents, &outputs);

    assert_eq!(
        scanned.reasons(0),
        [json(
            JsonDefect::DocumentTooLarge(JsonOperation::JsonValue),
            0
        )]
    );
    assert_eq!(
        scanned.reasons(1),
        [json(
            JsonDefect::TypeMismatch {
                path: path("$[0]"),
                place: JsonPlace::Value,
                found: JsonKind::Array,
                expected: target(&ParseAsType::I64),
            },
            0
        )],
        "a document at the depth limit is read"
    );
    assert_eq!(
        scanned.reasons(2),
        [json(
            JsonDefect::DocumentTooDeep(JsonOperation::JsonValue),
            0
        )]
    );
}

#[test]
fn a_result_beyond_its_column_offsets_is_an_overflow_whatever_the_read() {
    assert!(matches!(
        ValueColumn::next_offset(usize::try_from(i32::MAX).expect("i32::MAX fits usize") + 1),
        Err(ValueDefect::ResultTooLarge)
    ));
    let defect = ValueDefect::ResultTooLarge.into_defect(
        &tolerant("$.s", &ParseAsType::String, 0).extraction,
        &target(&ParseAsType::String),
    );
    assert!(!defect.concerns_the_document());
    assert_eq!(defect.code(), ErrorCode::Overflow);
}

#[test]
fn declares_only_types_a_json_value_can_hold() {
    for (declared, data_type) in [
        (ParseAsType::U8, arrow_schema::DataType::UInt8),
        (ParseAsType::I16, arrow_schema::DataType::Int16),
        (ParseAsType::U32, arrow_schema::DataType::UInt32),
    ] {
        assert_eq!(target(&declared).data_type(), data_type);
    }
    assert_eq!(
        JsonTarget::try_from(&ParseAsType::Datetime),
        Err(JsonTargetDefect::NoJsonValue(ParseAsType::Datetime))
    );
    assert_eq!(
        JsonTarget::try_from(&vec_of(ParseAsType::Bytes)),
        Err(JsonTargetDefect::NoJsonValue(ParseAsType::Bytes))
    );
    let too_wide = NonZeroU32::new(u32::MAX).expect("u32::MAX is positive");
    assert_eq!(
        JsonTarget::try_from(&ParseAsType::Array {
            element: Box::new(ParseAsType::I64),
            len: too_wide,
        }),
        Err(JsonTargetDefect::ArrayTooWide(too_wide))
    );
    assert_eq!(
        target(&array_of(vec_of(ParseAsType::F64), 3)).to_string(),
        "ARRAY<VEC<F64>, 3>"
    );
    assert_eq!(
        target(&vec_of(ParseAsType::F64)).data_type(),
        arrow_schema::DataType::List(std::sync::Arc::new(arrow_schema::Field::new(
            "item",
            arrow_schema::DataType::Float64,
            false
        ))),
        "collection elements are never null, as in a schema field of the same type"
    );
}
