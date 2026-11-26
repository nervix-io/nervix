//! The UDF host's `DURATION` column conversion.
//!
//! Layer: test harness.
//! - **Owns.** Exact equality of a `DURATION` column across the Roto column bridge into a UDF and
//!   back, and through its nanosecond view: every value, null, row and type.
//! - **Depends on.** The production UDF executor, its column bridges and its duration catalog.
//! - **Must not know.** Expression lowering, the VM's batches or the node's extension workers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_arbitrary::Entropy;
use nervix_models::FieldName;

use super::*;

/// The most rows one case reads: a bounded column, of every value and null.
const ROWS: usize = 64;

/// A UDF that returns the `DURATION` column it takes, or its nanoseconds.
fn duration_program(name: &str, returns: ParseAsType, code: &str) -> UdfProgram {
    UdfProgram::new(
        UdfName::parse(name).assured("the property's UDF names are valid"),
        UdfLanguage::Roto0_13,
        vec![UdfArgument {
            name: FieldName::parse("wait").assured("wait is a valid field name"),
            ty: ParseAsType::Duration,
            optional: true,
        }],
        UdfReturn {
            ty: returns,
            optional: true,
        },
        false,
        code.to_string(),
    )
}

/// The duration column one case reads: up to [`ROWS`] values, each a boundary of the signed range,
/// zero or an arbitrary length, or a null.
fn durations(entropy: &mut Entropy<'_>) -> Vec<Option<i64>> {
    let rows = entropy.count(ROWS);
    let mut values = Vec::with_capacity(rows);
    for _ in 0..rows {
        if entropy.flag() {
            values.push(None);
            continue;
        }
        let arbitrary = entropy.any_i64();
        values.push(Some(entropy.pick([
            i64::MIN,
            -1,
            0,
            1,
            i64::MAX,
            arbitrary,
        ])));
    }
    values
}

fn run(executor: &UdfExecutor, name: &str, input: &TypedArray, rows: usize) -> InjectedResult {
    executor
        .inject_with_context(
            &FunctionName::Udf(name.to_string()),
            std::slice::from_ref(input),
            &RowSelection::All(rows),
            (0..7).into(),
            Timestamp::from_unix_nanos(0),
            RowErrorMask::none(rows),
        )
        .assured("passing a column through and reading its nanoseconds never fail")
}

#[test]
fn bolero_udf_duration_columns_return_every_nanosecond_and_null() {
    let limits = UdfCompileLimits {
        settings: UdfSettings::default(),
        budget:
            <nervix_models::UdfCompileBudget as nervix_models::SettingDefinition>::default_value(&()),
    };
    let executor = UdfExecutor::compile_sync(
        [
            duration_program(
                "copy_wait",
                ParseAsType::Duration,
                "fn copy_wait(wait: DurationColumn) -> DurationColumn { wait }",
            ),
            duration_program(
                "wait_nanos",
                ParseAsType::I64,
                "fn wait_nanos(wait: DurationColumn) -> I64Column { wait.nanos() }",
            ),
        ],
        limits,
        None,
    )
    .assured("the DURATION bridge and catalog compile");
    // The executor holds the compiled UDFs; a failed case panics without reusing them.
    let executor = AssertUnwindSafe(executor);
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|bytes| {
            let mut entropy = Entropy::new(bytes);
            let values = durations(&mut entropy);
            let rows = values.len();
            let input = TypedArray::Duration(DurationNanosecondArray::from(values.clone()));

            let copied = run(&executor, "copy_wait", &input, rows);
            assert_eq!(copied.output, input);
            assert!(copied.side_errors.is_empty());

            let nanoseconds = run(&executor, "wait_nanos", &input, rows);
            assert_eq!(
                nanoseconds.output,
                TypedArray::Int64(Int64Array::from(values))
            );
            assert!(nanoseconds.side_errors.is_empty());
        });
}
