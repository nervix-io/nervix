use std::sync::Arc as StdArc;

use arch_into::ArchInto as _;
use arrow_array::{
    BooleanArray, Float64Array, Int64Array, ListArray, StringArray, types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use meticulous::ResultExt as _;
use nervix_approx_into::ApproxInto as _;
use nervix_models::Timestamp;
use nervix_vm::{
    CompileBinding, CompileOptions, CompiledProgram, ExecutionContext, OutputMode, RuntimeError,
    SemanticNamespaces, TypedArray, TypedBatch, compile_program_with_options_for_bindings,
    execute_program_in_context, lower_route_construction,
    program::{Program, SpannedNode},
};
use triomphe::Arc;

/// Row counts spanning `SPAWN_BLOCKING_ROW_THRESHOLD` so the sweep shows both the
/// amortization curve below it and the cost of the blocking hop above it.
const SWEEP_ROW_COUNTS: [usize; 6] = [64, 256, 1_024, 4_096, 16_384, 65_536];

async fn execute_benchmark_program(
    program: &Arc<CompiledProgram>,
    batch: &TypedBatch,
) -> Result<TypedBatch, RuntimeError> {
    let context = ExecutionContext::new(Timestamp::from_unix_nanos(0));
    execute_program_in_context(program, batch, &context)
        .await
        .map(|result| result.batch)
}

fn parse_program(source: &str) -> Result<SpannedNode<Program>, String> {
    parse_program_with_namespaces(source, SemanticNamespaces::new("input", "input"))
}

fn parse_program_with_namespaces(
    source: &str,
    namespaces: SemanticNamespaces<'_>,
) -> Result<SpannedNode<Program>, String> {
    let construction =
        nervix_nspl::parse_route_construction(source).map_err(|error| error.to_string())?;
    lower_route_construction(&construction, namespaces)
}

fn benchmark_row_i64(row: usize) -> i64 {
    i64::try_from(row).assured("benchmark row counts are fixed below i64::MAX")
}

fn arithmetic_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("left", DataType::Int64, true),
        Field::new("right", DataType::Int64, true),
        Field::new("divisor", DataType::Int64, true),
        Field::new("keep", DataType::Boolean, true),
    ]))
}

fn arithmetic_output_schema() -> StdArc<Schema> {
    let mut fields = arithmetic_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("total", DataType::Int64, true),
        Field::new("quotient", DataType::Int64, true),
        Field::new("magnitude", DataType::Int64, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn arithmetic_batch(row_count: usize) -> TypedBatch {
    let left =
        Int64Array::from_iter((0..row_count).map(|row| Some(benchmark_row_i64(row % 97) + 1)));
    let right =
        Int64Array::from_iter((0..row_count).map(|row| Some(benchmark_row_i64(row % 13) + 3)));
    let divisor =
        Int64Array::from_iter((0..row_count).map(|row| Some(benchmark_row_i64(row % 7) + 1)));
    let keep = BooleanArray::from_iter((0..row_count).map(|row| Some(row % 3 != 0)));

    TypedBatch::try_new(
        arithmetic_schema(),
        vec![
            TypedArray::Int64(left),
            TypedArray::Int64(right),
            TypedArray::Int64(divisor),
            TypedArray::Boolean(keep),
        ],
    )
    .expect("benchmark batch must build")
}

fn float_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("left", DataType::Float64, true),
        Field::new("right", DataType::Float64, true),
        Field::new("divisor", DataType::Float64, true),
    ]))
}

fn float_arithmetic_output_schema() -> StdArc<Schema> {
    let mut fields = float_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("total", DataType::Float64, true),
        Field::new("difference", DataType::Float64, true),
        Field::new("product", DataType::Float64, true),
        Field::new("quotient", DataType::Float64, true),
        Field::new("remainder", DataType::Float64, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn nullable_cast_output_schema() -> StdArc<Schema> {
    let mut fields = float_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("left_int", DataType::Int64, true),
        Field::new("right_f32", DataType::Float32, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn float_batch(row_count: usize) -> TypedBatch {
    let left = Float64Array::from_iter(
        (0..row_count).map(|row| (row % 17 != 0).then_some((row % 97).approx_into::<f64>() + 1.25)),
    );
    let right = Float64Array::from_iter(
        (0..row_count).map(|row| (row % 19 != 0).then_some((row % 13).approx_into::<f64>() + 0.5)),
    );
    let divisor = Float64Array::from_iter(
        (0..row_count).map(|row| (row % 23 != 0).then_some((row % 7).approx_into::<f64>() + 1.0)),
    );

    TypedBatch::try_new(
        float_schema(),
        vec![
            TypedArray::Float64(left),
            TypedArray::Float64(right),
            TypedArray::Float64(divisor),
        ],
    )
    .expect("benchmark batch must build")
}

fn string_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("primary", DataType::Utf8, true),
        Field::new("fallback", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("needle", DataType::Utf8, true),
        Field::new("prefix", DataType::Utf8, true),
        Field::new("suffix", DataType::Utf8, true),
        Field::new("number", DataType::Int64, true),
        Field::new("numeric_text", DataType::Utf8, true),
    ]))
}

fn string_output_schema() -> StdArc<Schema> {
    let mut fields = string_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("chosen", DataType::Utf8, true),
        Field::new("was_null", DataType::Boolean, true),
        Field::new("maybe", DataType::Utf8, true),
        Field::new("has", DataType::Boolean, true),
        Field::new("starts", DataType::Boolean, true),
        Field::new("ends", DataType::Boolean, true),
        Field::new("trimmed", DataType::Utf8, true),
        Field::new("characters", DataType::Int64, true),
        Field::new("replaced", DataType::Utf8, true),
        Field::new("number_text", DataType::Utf8, true),
        Field::new("parsed_number", DataType::Int64, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn string_batch(row_count: usize) -> TypedBatch {
    let primary = StringArray::from_iter((0..row_count).map(|row| {
        if row % 5 == 0 {
            None
        } else {
            Some(format!("value-{row}"))
        }
    }));
    let fallback =
        StringArray::from_iter((0..row_count).map(|row| Some(format!("fallback-{row}"))));
    let text =
        StringArray::from_iter((0..row_count).map(|row| Some(format!(" prefix-{row}-suffix "))));
    let needle = StringArray::from_iter((0..row_count).map(|_| Some("-")));
    let prefix = StringArray::from_iter((0..row_count).map(|_| Some("prefix-")));
    let suffix = StringArray::from_iter((0..row_count).map(|_| Some("-suffix")));
    let number = Int64Array::from_iter((0..row_count).map(|row| Some(benchmark_row_i64(row))));
    let numeric_text = StringArray::from_iter((0..row_count).map(|row| Some(row.to_string())));

    TypedBatch::try_new(
        string_schema(),
        vec![
            TypedArray::Utf8(primary),
            TypedArray::Utf8(fallback),
            TypedArray::Utf8(text),
            TypedArray::Utf8(needle),
            TypedArray::Utf8(prefix),
            TypedArray::Utf8(suffix),
            TypedArray::Int64(number),
            TypedArray::Utf8(numeric_text),
        ],
    )
    .expect("benchmark batch must build")
}

fn list_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new(
            "values",
            DataType::List(StdArc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        Field::new("index", DataType::Int64, true),
    ]))
}

fn list_output_schema() -> StdArc<Schema> {
    let mut fields = list_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("total", DataType::Int64, true),
        Field::new("first_value", DataType::Int64, true),
        Field::new("last_value", DataType::Int64, true),
        Field::new("nth_value", DataType::Int64, true),
        Field::new("value_count", DataType::Int64, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn list_batch(row_count: usize) -> TypedBatch {
    let values = ListArray::from_iter_primitive::<Int64Type, _, _>((0..row_count).map(|row| {
        (row % 7 != 0).then(|| {
            vec![
                Some(benchmark_row_i64(row)),
                Some(benchmark_row_i64(row) + 1),
                (row % 5 != 0).then_some(benchmark_row_i64(row) + 2),
                Some(benchmark_row_i64(row) + 3),
            ]
        })
    }));
    let index = Int64Array::from_iter((0..row_count).map(|row| Some(benchmark_row_i64(row % 5))));

    TypedBatch::try_new(
        list_schema(),
        vec![
            TypedArray::Generic(StdArc::new(values)),
            TypedArray::Int64(index),
        ],
    )
    .expect("benchmark batch must build")
}

fn long_tail_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("from_chars", DataType::Utf8, true),
        Field::new("to_chars", DataType::Utf8, true),
        Field::new("fill", DataType::Utf8, true),
        Field::new("delimiter", DataType::Utf8, true),
        Field::new("needle", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
        Field::new("width", DataType::Int64, true),
        Field::new("start", DataType::Int64, true),
        Field::new("length", DataType::Int64, true),
        Field::new("integer", DataType::Int64, true),
        Field::new("numeric", DataType::Float64, true),
    ]))
}

fn long_tail_output_schema() -> StdArc<Schema> {
    let mut fields = long_tail_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("translated", DataType::Utf8, true),
        Field::new("hexed", DataType::Utf8, true),
        Field::new("lefted", DataType::Utf8, true),
        Field::new("righted", DataType::Utf8, true),
        Field::new("padded", DataType::Utf8, true),
        Field::new("joined", DataType::Utf8, true),
        Field::new("piece", DataType::Utf8, true),
        Field::new("digest", DataType::Utf8, true),
        Field::new("titled", DataType::Utf8, true),
        Field::new("reversed", DataType::Utf8, true),
        Field::new("part", DataType::Utf8, true),
        Field::new("position", DataType::Int64, true),
        Field::new("cosine", DataType::Float64, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn long_tail_batch(row_count: usize) -> TypedBatch {
    let text =
        StringArray::from_iter((0..row_count).map(|row| Some(format!("alpha-{row}-beta-gamma"))));
    let from_chars = StringArray::from_iter((0..row_count).map(|_| Some("abg-")));
    let to_chars = StringArray::from_iter((0..row_count).map(|_| Some("ABG_")));
    let fill = StringArray::from_iter((0..row_count).map(|_| Some("xy")));
    let delimiter = StringArray::from_iter((0..row_count).map(|_| Some("-")));
    let needle = StringArray::from_iter((0..row_count).map(|_| Some("beta")));
    let count = Int64Array::from_iter((0..row_count).map(|_| Some(8)));
    let width = Int64Array::from_iter((0..row_count).map(|_| Some(32)));
    let start = Int64Array::from_iter((0..row_count).map(|_| Some(3)));
    let length = Int64Array::from_iter((0..row_count).map(|_| Some(12)));
    let integer = Int64Array::from_iter((0..row_count).map(|row| Some(benchmark_row_i64(row) + 1)));
    let numeric =
        Float64Array::from_iter((0..row_count).map(|row| Some((row % 100).approx_into::<f64>())));

    TypedBatch::try_new(
        long_tail_schema(),
        vec![
            TypedArray::Utf8(text),
            TypedArray::Utf8(from_chars),
            TypedArray::Utf8(to_chars),
            TypedArray::Utf8(fill),
            TypedArray::Utf8(delimiter),
            TypedArray::Utf8(needle),
            TypedArray::Int64(count),
            TypedArray::Int64(width),
            TypedArray::Int64(start),
            TypedArray::Int64(length),
            TypedArray::Int64(integer),
            TypedArray::Float64(numeric),
        ],
    )
    .expect("long-tail benchmark batch must build")
}

/// The record shape the stateful processors work over. `tenant` and `sequence` use coprime
/// moduli so a correlation predicate reading both keeps the same match rate at every row count
/// in the sweep, and `payload` is the column no stateful program reads, so an explicit-only
/// output stays visibly narrower than its input.
fn stateful_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("sequence", DataType::Int64, true),
        Field::new("observed_at", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

fn stateful_columns(row_count: usize) -> Vec<TypedArray> {
    let tenant =
        StringArray::from_iter((0..row_count).map(|row| Some(format!("tenant-{}", row % 4))));
    let region =
        StringArray::from_iter((0..row_count).map(|row| Some(format!("region-{}", row % 5))));
    let sequence =
        Int64Array::from_iter((0..row_count).map(|row| Some(benchmark_row_i64(row % 3))));
    let observed_at = Int64Array::from_iter((0..row_count).map(|row| Some(benchmark_row_i64(row))));
    let amount = Float64Array::from_iter(
        (0..row_count).map(|row| Some((row % 97).approx_into::<f64>() + 0.5)),
    );
    let payload = StringArray::from_iter((0..row_count).map(|row| Some(format!("payload-{row}"))));

    vec![
        TypedArray::Utf8(tenant),
        TypedArray::Utf8(region),
        TypedArray::Int64(sequence),
        TypedArray::Int64(observed_at),
        TypedArray::Float64(amount),
        TypedArray::Utf8(payload),
    ]
}

/// The incoming side of a correlation. The runtime repeats the one arriving record across the
/// whole candidate batch, so every column here holds the same value in all rows. Its `tenant`
/// and `sequence` select one candidate pair in twelve, independent of the row count.
fn correlation_probe_columns(row_count: usize) -> Vec<TypedArray> {
    vec![
        TypedArray::Utf8(StringArray::from_iter(
            (0..row_count).map(|_| Some("tenant-2")),
        )),
        TypedArray::Utf8(StringArray::from_iter(
            (0..row_count).map(|_| Some("region-1")),
        )),
        TypedArray::Int64(Int64Array::from_iter((0..row_count).map(|_| Some(1)))),
        TypedArray::Int64(Int64Array::from_iter((0..row_count).map(|_| Some(0)))),
        TypedArray::Float64(Float64Array::from_iter((0..row_count).map(|_| Some(1.5)))),
        TypedArray::Utf8(StringArray::from_iter(
            (0..row_count).map(|_| Some("probe")),
        )),
    ]
}

/// The key columns the deduplicator writes for `DEDUPLICATE ON` and the reorderer for its
/// ordering key. Both compile the same explicit-only projection, so the sweep benches it once.
fn key_projection_output_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("key_0", DataType::Utf8, true),
        Field::new("key_1", DataType::Int64, true),
        Field::new("key_2", DataType::Utf8, true),
    ]))
}

/// One arriving record per row. The schema comes from the compiled program because the VM names
/// its input fields for the namespace they were bound to, and only the program knows that.
fn stateful_batch(program: &CompiledProgram, row_count: usize) -> TypedBatch {
    TypedBatch::try_new(program.input_schema.clone(), stateful_columns(row_count))
        .expect("stateful benchmark batch must build")
}

/// One column per window aggregate argument, written into the `window_input` namespace by the
/// program the window processor runs over every arriving batch.
fn window_demand_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("demand_0", DataType::Float64, true),
        Field::new("demand_1", DataType::Int64, true),
        Field::new("demand_2", DataType::Float64, true),
    ]))
}

/// The output a correlator constructs from a matched pair. It reads both sides and initializes
/// every field itself, so nothing passes through from either input.
fn correlation_output_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("sequence", DataType::Int64, true),
        Field::new("total", DataType::Float64, true),
        Field::new("lag", DataType::Int64, true),
    ]))
}

/// One row per candidate pair, holding the arriving record beside the retained record it is
/// compared against. Both correlator programs read this same paired shape.
fn correlation_batch(program: &CompiledProgram, row_count: usize) -> TypedBatch {
    let mut columns = correlation_probe_columns(row_count);
    columns.extend(stateful_columns(row_count));

    TypedBatch::try_new(program.input_schema.clone(), columns)
        .expect("correlation benchmark batch must build")
}

fn compile_arithmetic(options: CompileOptions) -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET total = input.left + input.right, quotient = (input.left + input.right) / \
         input.divisor, magnitude = abs(input.left - input.right) WHERE input.keep",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        arithmetic_output_schema(),
        [CompileBinding::writable("input", arithmetic_schema())],
        options,
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn compile_float_arithmetic() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET total = input.left + input.right, difference = input.left - input.right, product = \
         input.left * input.right, quotient = input.left / input.divisor, remainder = input.left \
         % input.divisor",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        float_arithmetic_output_schema(),
        [CompileBinding::writable("input", float_schema())],
        CompileOptions::default(),
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn compile_nullable_casts() -> Arc<CompiledProgram> {
    let program =
        parse_program("SET left_int = input.left AS INT64, right_f32 = input.right AS FLOAT32")
            .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        nullable_cast_output_schema(),
        [CompileBinding::writable("input", float_schema())],
        CompileOptions::default(),
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn compile_string(options: CompileOptions) -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET chosen = coalesce(input.primary, input.fallback), was_null = is_null(input.primary), \
         maybe = nullif(input.primary, input.fallback), has = contains(input.text, input.needle), \
         starts = starts_with(input.text, input.prefix), ends = ends_with(input.text, \
         input.suffix)",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        string_output_schema(),
        [CompileBinding::writable("input", string_schema())],
        options,
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn compile_long_tail() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET translated = translate(input.text, input.from_chars, input.to_chars), hexed = \
         to_hex(input.integer), lefted = left(input.text, input.count), righted = \
         right(input.text, input.count), padded = lpad(input.text, input.width, input.fill), \
         joined = concat(input.text, input.fill, input.text), piece = substr(input.text, \
         input.start, input.length), digest = md5(input.text), titled = initcap(input.text), \
         reversed = reverse(input.text), part = split_part(input.text, input.delimiter, \
         input.count), position = strpos(input.text, input.needle), cosine = cos(input.numeric)",
    )
    .expect("long-tail benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        long_tail_output_schema(),
        [CompileBinding::writable("input", long_tail_schema())],
        CompileOptions::default(),
    )
    .map(Arc::new)
    .expect("long-tail benchmark program must compile")
}

/// Filters on a numeric comparison and writes text-case builtins, so the sweep also covers
/// the paths that evaluate row by row rather than through an Arrow kernel.
fn compile_numeric_compare() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET total = input.left + input.right, quotient = input.left / input.divisor, magnitude = \
         abs(input.left) WHERE input.left > input.divisor",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        arithmetic_output_schema(),
        [CompileBinding::writable("input", arithmetic_schema())],
        CompileOptions::default(),
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn compile_text_transform() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET trimmed = trim(input.text), characters = length(input.text), replaced = \
         replace(input.text, input.needle, input.suffix), number_text = input.number AS STRING, \
         parsed_number = input.numeric_text AS INT64",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        string_output_schema(),
        [CompileBinding::writable("input", string_schema())],
        CompileOptions::default(),
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn compile_list() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET total = sum(input.values), first_value = first(input.values), last_value = \
         last(input.values), nth_value = nth(input.values, input.index), value_count = \
         count(input.values)",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        list_output_schema(),
        [CompileBinding::writable("input", list_schema())],
        CompileOptions::default(),
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn unoptimized_options() -> CompileOptions {
    CompileOptions {
        optimize_temp_registers: false,
        ..CompileOptions::default()
    }
}

/// Every stateful processor but the correlator predicate builds its output field by field
/// instead of passing input columns through by name.
fn explicit_only_options() -> CompileOptions {
    CompileOptions {
        output_mode: OutputMode::ExplicitOnly,
        ..CompileOptions::default()
    }
}

/// The projection a deduplicator compiles for `DEDUPLICATE ON`, and a reorderer for its ordering
/// key: a whole record in, only the key columns out, so most input columns are read and dropped
/// rather than carried.
fn compile_key_projection() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET key_0 = input.tenant, key_1 = input.sequence, key_2 = concat(input.tenant, \
         input.region)",
    )
    .expect("key projection benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        key_projection_output_schema(),
        [CompileBinding::writable("input", stateful_schema())],
        explicit_only_options(),
    )
    .map(Arc::new)
    .expect("key projection benchmark program must compile")
}

/// The program a window processor runs over every arriving batch to evaluate its aggregate
/// arguments. Aggregation itself is folded per window outside the VM; this is the per-batch part.
fn compile_window_aggregate_input() -> Arc<CompiledProgram> {
    let program = parse_program_with_namespaces(
        "SET demand_0 = input.amount, demand_1 = input.sequence, demand_2 = input.amount * 2.0",
        SemanticNamespaces::new("input", "window_input"),
    )
    .expect("window aggregate input benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        window_demand_schema(),
        [
            CompileBinding::writeonly("window_input", window_demand_schema()),
            CompileBinding::readonly("input", stateful_schema()),
        ],
        explicit_only_options(),
    )
    .map(Arc::new)
    .expect("window aggregate input benchmark program must compile")
}

/// `CORRELATE WHERE` over one arriving record and the candidates retained for it. Its row count
/// is the retained candidate count, so this is the stateful shape whose batches grow with the
/// correlation window rather than with the arrival rate.
fn compile_correlate_where() -> Arc<CompiledProgram> {
    let program =
        parse_program("WHERE left.tenant = right.tenant AND left.sequence = right.sequence")
            .expect("correlate where benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        stateful_schema(),
        [
            CompileBinding::writable("left", stateful_schema()),
            CompileBinding::readonly("right", stateful_schema()),
        ],
        CompileOptions::default(),
    )
    .map(Arc::new)
    .expect("correlate where benchmark program must compile")
}

/// The set-only construction a correlator runs over the pairs `CORRELATE WHERE` matched, reading
/// both sides through explicit `left` and `right` scopes.
fn compile_correlate_output() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET output.tenant = left.tenant, output.region = right.region, output.sequence = \
         left.sequence, output.total = left.amount + right.amount, output.lag = right.observed_at \
         - left.observed_at",
    )
    .expect("correlate output benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        correlation_output_schema(),
        [
            CompileBinding::readonly("left", stateful_schema()),
            CompileBinding::readonly("right", stateful_schema()),
            CompileBinding::writeonly("output", correlation_output_schema()),
        ],
        explicit_only_options(),
    )
    .map(Arc::new)
    .expect("correlate output benchmark program must compile")
}

fn benchmark_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("benchmark runtime must build")
}

fn execute_benches(c: &mut Criterion) {
    let arithmetic_compiled = compile_arithmetic(CompileOptions::default());
    let arithmetic_unoptimized = compile_arithmetic(unoptimized_options());
    let arithmetic_batch = arithmetic_batch(8_192);

    let string_compiled = compile_string(CompileOptions::default());
    let string_unoptimized = compile_string(unoptimized_options());
    let string_batch = string_batch(8_192);
    let long_tail_compiled = compile_long_tail();
    let long_tail_batch = long_tail_batch(8_192);
    let runtime = benchmark_runtime();

    let mut group = c.benchmark_group("execute_program");
    group.bench_function("arithmetic_filter_optimized_8192", |b| {
        b.iter(|| {
            runtime.block_on(execute_benchmark_program(
                black_box(&arithmetic_compiled),
                black_box(&arithmetic_batch),
            ))
        })
    });
    group.bench_function("arithmetic_filter_unoptimized_8192", |b| {
        b.iter(|| {
            runtime.block_on(execute_benchmark_program(
                black_box(&arithmetic_unoptimized),
                black_box(&arithmetic_batch),
            ))
        })
    });
    group.bench_function("string_builtins_optimized_8192", |b| {
        b.iter(|| {
            runtime.block_on(execute_benchmark_program(
                black_box(&string_compiled),
                black_box(&string_batch),
            ))
        })
    });
    group.bench_function("string_builtins_unoptimized_8192", |b| {
        b.iter(|| {
            runtime.block_on(execute_benchmark_program(
                black_box(&string_unoptimized),
                black_box(&string_batch),
            ))
        })
    });
    group.bench_function("long_tail_builtins_8192", |b| {
        b.iter(|| {
            runtime.block_on(execute_benchmark_program(
                black_box(&long_tail_compiled),
                black_box(&long_tail_batch),
            ))
        })
    });
    group.finish();
}

/// Sweeps batch size for the same programs so throughput is reported per row instead of
/// per batch. This is what shows whether feeding the VM larger batches keeps paying.
///
/// The sweep covers the stateful processors through the program shapes they compile, because
/// what the VM sees from a deduplicator, reorderer, window processor or correlator is a program
/// over a batch like any other. Their batches are drawn differently: a key projection and a
/// window aggregate input run over each arriving batch, while both correlator programs run over
/// a batch of candidate pairs whose size follows the retained correlation window. Generator
/// routes and window closes are excluded because they execute one row per invocation whatever
/// the arrival rate, so no batch size applies to them.
fn batch_size_sweep_benches(c: &mut Criterion) {
    let arithmetic_compiled = compile_arithmetic(CompileOptions::default());
    let string_compiled = compile_string(CompileOptions::default());
    let numeric_compare_compiled = compile_numeric_compare();
    let float_arithmetic_compiled = compile_float_arithmetic();
    let nullable_casts_compiled = compile_nullable_casts();
    let text_transform_compiled = compile_text_transform();
    let list_compiled = compile_list();
    let key_projection_compiled = compile_key_projection();
    let window_aggregate_input_compiled = compile_window_aggregate_input();
    let correlate_where_compiled = compile_correlate_where();
    let correlate_output_compiled = compile_correlate_output();
    let runtime = benchmark_runtime();

    let mut group = c.benchmark_group("execute_program_batch_size");
    for rows in SWEEP_ROW_COUNTS {
        group.throughput(Throughput::Elements(rows.arch_into()));

        let batch = arithmetic_batch(rows);
        group.bench_with_input(
            BenchmarkId::new("arithmetic_filter", rows),
            &rows,
            |b, _| {
                b.iter(|| {
                    runtime.block_on(execute_benchmark_program(
                        black_box(&arithmetic_compiled),
                        black_box(&batch),
                    ))
                })
            },
        );
        group.bench_with_input(BenchmarkId::new("numeric_compare", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_benchmark_program(
                    black_box(&numeric_compare_compiled),
                    black_box(&batch),
                ))
            })
        });

        let batch = float_batch(rows);
        group.bench_with_input(BenchmarkId::new("float_arithmetic", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_benchmark_program(
                    black_box(&float_arithmetic_compiled),
                    black_box(&batch),
                ))
            })
        });
        group.bench_with_input(
            BenchmarkId::new("nullable_kernel_casts", rows),
            &rows,
            |b, _| {
                b.iter(|| {
                    runtime.block_on(execute_benchmark_program(
                        black_box(&nullable_casts_compiled),
                        black_box(&batch),
                    ))
                })
            },
        );

        let batch = string_batch(rows);
        group.bench_with_input(BenchmarkId::new("string_builtins", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_benchmark_program(
                    black_box(&string_compiled),
                    black_box(&batch),
                ))
            })
        });
        group.bench_with_input(BenchmarkId::new("text_transform", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_benchmark_program(
                    black_box(&text_transform_compiled),
                    black_box(&batch),
                ))
            })
        });

        let batch = list_batch(rows);
        group.bench_with_input(BenchmarkId::new("list_builtins", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_benchmark_program(
                    black_box(&list_compiled),
                    black_box(&batch),
                ))
            })
        });

        let batch = stateful_batch(&key_projection_compiled, rows);
        group.bench_with_input(BenchmarkId::new("key_projection", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_benchmark_program(
                    black_box(&key_projection_compiled),
                    black_box(&batch),
                ))
            })
        });

        let batch = stateful_batch(&window_aggregate_input_compiled, rows);
        group.bench_with_input(
            BenchmarkId::new("window_aggregate_input", rows),
            &rows,
            |b, _| {
                b.iter(|| {
                    runtime.block_on(execute_benchmark_program(
                        black_box(&window_aggregate_input_compiled),
                        black_box(&batch),
                    ))
                })
            },
        );

        let batch = correlation_batch(&correlate_where_compiled, rows);
        group.bench_with_input(BenchmarkId::new("correlate_where", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_benchmark_program(
                    black_box(&correlate_where_compiled),
                    black_box(&batch),
                ))
            })
        });

        let batch = correlation_batch(&correlate_output_compiled, rows);
        group.bench_with_input(BenchmarkId::new("correlate_output", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_benchmark_program(
                    black_box(&correlate_output_compiled),
                    black_box(&batch),
                ))
            })
        });
    }
    group.finish();
}

criterion_group!(benches, execute_benches, batch_size_sweep_benches);
criterion_main!(benches);
