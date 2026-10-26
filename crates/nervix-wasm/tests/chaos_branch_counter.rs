//! The branch-counting guest the external chaos runner uploads to a packaged node.
//!
//! `scripts/chaos/fixtures/wasm/processors/branch-counter.wat` is a prebuilt module checked in
//! beside the chaos scenarios: a chaos run uploads that file as a resource version and never
//! builds guest code. This test owns how the module is produced. It keeps the checked-in text equal
//! to what the current guest ABI produces, and it drives the checked-in module through the host to
//! prove the behavior the chaos verifier expects from it: one output row per input row, carrying
//! every input field and the number of rows the branch has counted so far, and a count that the
//! saved state carries into a restored instance.
//!
//! Regenerate the module after a guest ABI change with `just chaos-wasm-fixture`.

use std::{iter, path::PathBuf, time::Duration};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{FieldName, ParseAsType, SchemaField, Timestamp, WasmProcessorLimits};
use nervix_primitives::sync::StdArc;
use nervix_wasm::{
    SavedStateRejection, WasmAckSidecar, WasmAckToken, WasmBranchInit, WasmEnvelope,
    WasmExecutionContext, WasmOutputColumnRef, WasmOutputRow, WasmProcessorField,
    WasmProcessorSchema, WasmProcessorType, WasmRoutedOutput, WasmRuntime, WasmRuntimeConfig,
};
use nonzero_ext::nonzero;

/// The relay the chaos graph routes the guest's output to.
const OUTPUT_RELAY: &str = "chaos_counted";
/// The input fields of the chaos graph's stateful record, in declared order.
const INPUT_FIELDS: [(&str, WasmProcessorType); 6] = [
    ("event_id", WasmProcessorType::String),
    ("branch_name", WasmProcessorType::String),
    ("sequence", WasmProcessorType::I64),
    ("branch_index", WasmProcessorType::I64),
    ("dedup_key", WasmProcessorType::String),
    ("content", WasmProcessorType::String),
];
/// The output field the guest generates after every input field.
const COUNT_FIELD: &str = "branch_count";
/// Placeholder values the generator encodes into the output envelope and the guest overwrites
/// for every row it emits: the input row's acknowledgement token, which appears both as the row's
/// carried token and as its source token, and the generated count.
const TOKEN_PLACEHOLDER: u64 = 0x4a3b_2c1d_6e5f_8071;
const COUNT_PLACEHOLDER: i64 = 0x5c6d_7e8f_1a2b_3c4d;

/// Guest memory layout. The output envelope is copied once into its data segment; the saved
/// count, the acknowledgement tokens of the batch being emitted, and the host's input buffer each
/// own a disjoint range below and above the first page boundary.
const ENVELOPE_ADDRESS: usize = 1024;
const STATE_ADDRESS: usize = 4096;
const TOKENS_ADDRESS: usize = 8192;
const INPUT_ADDRESS: usize = 65536;
const MAX_BATCH_ROWS: usize = (INPUT_ADDRESS - TOKENS_ADDRESS) / 8;
const SAVED_STATE_BYTES: usize = 8;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/chaos/fixtures/wasm/processors/branch-counter.wat")
}

fn processor_field(name: &str, ty: WasmProcessorType) -> WasmProcessorField {
    WasmProcessorField {
        name: name.to_string(),
        ty,
        optional: false,
    }
}

fn input_schema() -> WasmProcessorSchema {
    WasmProcessorSchema {
        name: "chaos_state_record".to_string(),
        fields: INPUT_FIELDS
            .iter()
            .map(|(name, ty)| processor_field(name, ty.clone()))
            .collect(),
    }
}

fn output_schema() -> WasmProcessorSchema {
    let mut schema = input_schema();
    schema.name = "chaos_counted_record".to_string();
    schema
        .fields
        .push(processor_field(COUNT_FIELD, WasmProcessorType::I64));
    schema
}

/// The Arrow field a generated column must have to fill the count field: the destination field
/// without its name, exactly as the server compares them.
fn generated_count_field() -> Field {
    let count = SchemaField {
        name: FieldName::parse(COUNT_FIELD).assured("the count field name is a valid name"),
        ty: ParseAsType::I64,
        optional: false,
        sensitive: false,
    };
    count.arrow_field().with_name("")
}

/// One Arrow IPC stream holding one generated row with `count`.
fn generated_count_ipc(count: i64) -> Vec<u8> {
    let schema = StdArc::new(Schema::new(vec![generated_count_field()]));
    let column = StdArc::new(Int64Array::from(vec![count]));
    let batch = RecordBatch::try_new(schema.clone(), vec![column])
        .assured("one Int64 column matches its one-field schema");
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &schema)
            .assured("an in-memory IPC writer accepts a one-field schema");
        writer
            .write(&batch)
            .assured("an in-memory IPC writer accepts a batch of its schema");
        writer
            .finish()
            .assured("an in-memory IPC writer finishes its stream");
    }
    ipc
}

/// The output envelope the guest emits for one input row, with the placeholders still in place.
fn output_envelope() -> Vec<u8> {
    let input_columns = (0..INPUT_FIELDS.len()).map(|index| WasmOutputColumnRef::Input {
        column_index: u32::try_from(index).assured("the input schema has six fields"),
    });
    let columns = input_columns
        .chain(iter::once(WasmOutputColumnRef::Generated {
            column_index: 0,
        }))
        .collect();
    let acks = WasmAckSidecar {
        rows: vec![WasmOutputRow {
            tokens: vec![WasmAckToken(TOKEN_PLACEHOLDER)],
            source_token: Some(WasmAckToken(TOKEN_PLACEHOLDER)),
        }],
        ..WasmAckSidecar::default()
    };
    WasmEnvelope::output(
        generated_count_ipc(COUNT_PLACEHOLDER),
        vec![WasmRoutedOutput::new(OUTPUT_RELAY, columns, acks)],
    )
    .encode()
    .assured("an output envelope of one routed row encodes")
}

/// Every offset at which `needle` occurs in `bytes`.
fn offsets_of(bytes: &[u8], needle: [u8; 8]) -> Vec<usize> {
    bytes
        .windows(needle.len())
        .enumerate()
        .filter(|(_, window)| *window == needle)
        .map(|(offset, _)| offset)
        .collect()
}

/// The module text, generated from the current output envelope encoding.
fn branch_counter_wat() -> String {
    let envelope = output_envelope();
    assert!(
        ENVELOPE_ADDRESS + envelope.len() <= STATE_ADDRESS,
        "the output envelope must fit below the saved state"
    );
    let token_offsets = offsets_of(&envelope, TOKEN_PLACEHOLDER.to_le_bytes());
    assert_eq!(
        token_offsets.len(),
        2,
        "the token placeholder is the row's carried token and its source token"
    );
    let count_offsets = offsets_of(&envelope, COUNT_PLACEHOLDER.to_le_bytes());
    assert_eq!(
        count_offsets.len(),
        1,
        "the count placeholder is the generated row"
    );
    let envelope_text = envelope
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let token_address_carried = ENVELOPE_ADDRESS + token_offsets[0];
    let token_address_source = ENVELOPE_ADDRESS + token_offsets[1];
    let count_address = ENVELOPE_ADDRESS + count_offsets[0];
    let envelope_len = envelope.len();
    let rejected = SavedStateRejection::ApplicationState.code();
    format!(
        r#";; Branch-counting WASM processor guest for the external chaos runner.
;;
;; Generated by crates/nervix-wasm/tests/chaos_branch_counter.rs; regenerate it with
;; `just chaos-wasm-fixture` instead of editing it.
;;
;; Every concrete branch has its own instance, whose state is the number of input rows it has
;; counted. For each input row the guest emits one row to {OUTPUT_RELAY} that references all
;; six input columns through the row's source token and adds a generated {COUNT_FIELD} column
;; holding the count after that row. The saved state is that count as eight little-endian bytes.
(module
  (memory (export "memory") 2)
  (global $count (mut i64) (i64.const 0))
  (global $pending (mut i32) (i32.const 0))
  (global $next (mut i32) (i32.const 0))
  (global $base (mut i64) (i64.const 0))
  (global $read_ptr (mut i32) (i32.const {ENVELOPE_ADDRESS}))
  (global $read_len (mut i32) (i32.const 0))
  (data (i32.const {ENVELOPE_ADDRESS}) "{envelope_text}")
  ;; The absolute address of field `slot` of the FlatBuffers table at `table`.
  (func $field (param $table i32) (param $slot i32) (result i32)
    local.get $table
    local.get $table local.get $table i32.load i32.sub
    i32.const 4 i32.add local.get $slot i32.const 2 i32.mul i32.add
    i32.load16_u i32.add)
  ;; The absolute address an unsigned FlatBuffers offset at `address` points to.
  (func $follow (param $address i32) (result i32)
    local.get $address local.get $address i32.load i32.add)
  ;; The row vector of the input envelope's acknowledgement sidecar.
  (func $input_rows (param $ptr i32) (result i32)
    local.get $ptr i32.const 4 i32.add call $follow
    i32.const 1 call $field call $follow
    i32.const 1 call $field call $follow
    i32.const 0 call $field call $follow)
  (func (export "nervix_buffer_ptr") (result i32) global.get $read_ptr)
  (func (export "nervix_buffer_len") (result i32) global.get $read_len)
  (func (export "nervix_buffer_capacity") (result i32)
    memory.size i32.const 65536 i32.mul i32.const {INPUT_ADDRESS} i32.sub)
  (func (export "nervix_alloc") (param $len i32) (result i32)
    (local $pages i32)
    local.get $len i32.const {INPUT_ADDRESS} i32.add i32.const 65535 i32.add
    i32.const 16 i32.shr_u local.set $pages
    local.get $pages memory.size i32.gt_u
    if
      local.get $pages memory.size i32.sub memory.grow
      i32.const -1 i32.eq
      if unreachable end
    end
    i32.const {INPUT_ADDRESS})
  (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
  (func (export "nervix_current_domain_time_nanos") (result i64) (i64.const 0))
  (func (export "nervix_process_batch") (param $ptr i32) (param i32) (result i32)
    (local $rows i32) (local $index i32) (local $tokens i32)
    local.get $ptr call $input_rows local.set $rows
    local.get $rows i32.load global.set $pending
    global.get $pending i32.const {MAX_BATCH_ROWS} i32.gt_u
    if unreachable end
    i32.const 0 local.set $index
    block $copied
      loop $copy
        local.get $index global.get $pending i32.ge_u br_if $copied
        local.get $rows i32.const 4 i32.add local.get $index i32.const 4 i32.mul i32.add
        call $follow
        i32.const 0 call $field call $follow local.set $tokens
        i32.const {TOKENS_ADDRESS} local.get $index i32.const 8 i32.mul i32.add
        local.get $tokens i32.const 4 i32.add i64.load
        i64.store
        local.get $index i32.const 1 i32.add local.set $index
        br $copy
      end
    end
    i32.const 0 global.set $next
    global.get $count global.set $base
    global.get $count global.get $pending i64.extend_i32_u i64.add global.set $count
    i32.const 0)
  (func (export "nervix_on_timeout") (param i64) (result i32) (i32.const 0))
  (func (export "nervix_flush") (result i32) (i32.const 0))
  (func (export "nervix_read_emit") (result i32)
    (local $token i64)
    global.get $next global.get $pending i32.ge_u
    if (result i32)
      i32.const 0
    else
      i32.const {TOKENS_ADDRESS} global.get $next i32.const 8 i32.mul i32.add i64.load
      local.set $token
      i32.const {token_address_carried} local.get $token i64.store
      i32.const {token_address_source} local.get $token i64.store
      i32.const {count_address}
      global.get $base global.get $next i64.extend_i32_u i64.add i64.const 1 i64.add
      i64.store
      global.get $next i32.const 1 i32.add global.set $next
      i32.const {ENVELOPE_ADDRESS} global.set $read_ptr
      i32.const {envelope_len} global.set $read_len
      i32.const {envelope_len}
    end)
  (func (export "nervix_dump_state") (result i32)
    i32.const {STATE_ADDRESS} global.get $count i64.store
    i32.const {STATE_ADDRESS} global.set $read_ptr
    i32.const {SAVED_STATE_BYTES} global.set $read_len
    i32.const {SAVED_STATE_BYTES})
  (func (export "nervix_load_state") (param $ptr i32) (param $len i32) (result i32)
    local.get $len i32.const {SAVED_STATE_BYTES} i32.ne
    if (result i32)
      i32.const {rejected}
    else
      local.get $ptr i64.load global.set $count
      i32.const 0
    end)
  (func (export "nervix_reset_state") (result i32)
    i64.const 0 global.set $count
    i32.const 0 global.set $pending
    i32.const 0 global.set $next
    i32.const 0)
)
"#
    )
}

fn checked_in_module() -> String {
    std::fs::read_to_string(fixture_path())
        .assured("the chaos fixture directory holds the checked-in branch counter")
}

fn limits() -> WasmProcessorLimits {
    WasmProcessorLimits {
        max_fuel: nonzero!(1_000_000_000u64),
        max_memory_bytes: nonzero!(67_108_864u64),
    }
}

fn init(branch: &str) -> WasmBranchInit {
    WasmBranchInit {
        domain_name: "chaos_baseline".to_string(),
        domain_type: "UNPACED".to_string(),
        branch_key: Some(format!("branch_name={branch}").into_bytes()),
        input_schema: input_schema(),
        output_schemas: vec![output_schema()],
    }
}

fn context() -> WasmExecutionContext {
    WasmExecutionContext::new(Timestamp::from_unix_nanos(1_700_000_000_000_000_000))
}

/// An input envelope of `rows` stateful records of `branch`, each carrying the token
/// `first_token + row`.
fn input_envelope(branch: &str, first_index: i64, first_token: u64, rows: usize) -> WasmEnvelope {
    let schema = StdArc::new(Schema::new(
        INPUT_FIELDS
            .iter()
            .map(|(name, ty)| match ty {
                WasmProcessorType::I64 => Field::new(*name, DataType::Int64, false),
                _ => Field::new(*name, DataType::Utf8, false),
            })
            .collect::<Vec<_>>(),
    ));
    let indexes = (0..rows)
        .map(|row| first_index + i64::try_from(row).assured("a test batch has a few rows"))
        .collect::<Vec<_>>();
    let text = |prefix: &str| {
        let column: StdArc<dyn Array> = StdArc::new(StringArray::from(
            indexes
                .iter()
                .map(|index| format!("{prefix}-{index}"))
                .collect::<Vec<_>>(),
        ));
        column
    };
    let number = |values: Vec<i64>| {
        let column: StdArc<dyn Array> = StdArc::new(Int64Array::from(values));
        column
    };
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            text(branch),
            StdArc::new(StringArray::from(vec![branch.to_string(); rows])),
            number(indexes.iter().map(|index| index * 2).collect()),
            number(indexes.clone()),
            text("key"),
            text("content"),
        ],
    )
    .assured("six columns match the six-field input schema");
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &schema)
            .assured("an in-memory IPC writer accepts the input schema");
        writer
            .write(&batch)
            .assured("an in-memory IPC writer accepts a batch of its schema");
        writer
            .finish()
            .assured("an in-memory IPC writer finishes its stream");
    }
    let tokens = (0..rows)
        .map(|row| first_token + u64::try_from(row).assured("a test batch has a few rows"))
        .map(|token| WasmOutputRow {
            tokens: vec![WasmAckToken(token)],
            source_token: Some(WasmAckToken(token)),
        })
        .collect();
    WasmEnvelope::input(
        ipc,
        WasmAckSidecar {
            rows: tokens,
            ..WasmAckSidecar::default()
        },
    )
}

/// The row each emitted envelope routes: its carried and source token, and its generated count.
#[derive(Debug, PartialEq, Eq)]
struct CountedRow {
    token: u64,
    count: i64,
}

fn counted_rows(outputs: &[WasmEnvelope]) -> Vec<CountedRow> {
    let mut rows = Vec::new();
    for output in outputs {
        let WasmEnvelope::Output {
            generated_arrow_ipc_batch,
            outputs,
        } = output
        else {
            panic!("the guest emits output envelopes only");
        };
        assert_eq!(outputs.len(), 1, "every envelope routes one output");
        let routed = &outputs[0];
        assert_eq!(routed.output_relay, OUTPUT_RELAY);
        let expected_columns = (0..INPUT_FIELDS.len())
            .map(|index| WasmOutputColumnRef::Input {
                column_index: u32::try_from(index).assured("the input schema has six fields"),
            })
            .chain(iter::once(WasmOutputColumnRef::Generated {
                column_index: 0,
            }))
            .collect::<Vec<_>>();
        assert_eq!(routed.columns, expected_columns);
        assert_eq!(routed.acks.rows.len(), 1, "every envelope routes one row");
        let row = &routed.acks.rows[0];
        assert_eq!(row.tokens.len(), 1);
        assert_eq!(row.source_token, Some(row.tokens[0]));
        assert!(routed.acks.acked.is_empty() && routed.acks.nacked.is_empty());
        let reader = StreamReader::try_new(generated_arrow_ipc_batch.as_ref(), None)
            .assured("the guest emits the generator's IPC stream");
        assert_eq!(reader.schema().fields().len(), 1);
        assert_eq!(reader.schema().field(0), &generated_count_field());
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .assured("the guest emits the generator's IPC stream");
        assert_eq!(batches.len(), 1);
        let counts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .verified("the schema assertion above checked an Int64 column");
        assert_eq!(counts.len(), 1);
        rows.push(CountedRow {
            token: row.tokens[0].0,
            count: counts.value(0),
        });
    }
    rows
}

#[test]
fn the_checked_in_branch_counter_is_the_generated_module() {
    assert_eq!(
        checked_in_module(),
        branch_counter_wat(),
        "the checked-in chaos branch counter differs from the module the current guest ABI \
         produces; regenerate it with `just chaos-wasm-fixture`"
    );
}

/// Writes the generated module over the checked-in one. `just chaos-wasm-fixture` runs it.
#[test]
#[ignore = "writes scripts/chaos/fixtures/wasm/processors/branch-counter.wat"]
fn write_the_branch_counter_module() {
    std::fs::write(fixture_path(), branch_counter_wat())
        .assured("the chaos fixture directory is writable in a checkout");
}

#[nervix_primitives::test]
async fn the_branch_counter_counts_its_branch_rows_and_restores_the_count() {
    let runtime = WasmRuntime::new(WasmRuntimeConfig {
        optimize: false,
        epoch_tick_interval: Duration::from_millis(5),
        epoch_deadline_ticks: 2_000,
        max_guest_buffer_bytes: 64 * 1024 * 1024,
    })
    .assured("the host runtime starts with a valid configuration");
    let compiled = runtime
        .compile_processor(
            &nervix_execution::Executor::default(),
            checked_in_module().as_bytes(),
        )
        .await
        .assured("the checked-in branch counter compiles");

    let mut alpha = compiled
        .instantiate_branch(limits(), init("alpha"), context(), None)
        .await
        .assured("a new branch instance starts without saved state");
    let first = alpha
        .process_envelope_in_context(&input_envelope("alpha", 1, 11, 3), context())
        .await
        .assured("the guest processes a three-row batch");
    assert_eq!(
        counted_rows(&first),
        vec![
            CountedRow {
                token: 11,
                count: 1
            },
            CountedRow {
                token: 12,
                count: 2
            },
            CountedRow {
                token: 13,
                count: 3
            },
        ]
    );
    let second = alpha
        .process_envelope_in_context(&input_envelope("alpha", 4, 21, 1), context())
        .await
        .assured("the guest processes a one-row batch");
    assert_eq!(
        counted_rows(&second),
        vec![CountedRow {
            token: 21,
            count: 4
        }]
    );
    let saved = alpha
        .save_state_in_context(context())
        .await
        .assured("the guest saves its count");
    assert_eq!(saved, 4_i64.to_le_bytes());

    let mut restored = compiled
        .instantiate_branch(limits(), init("alpha"), context(), Some(&saved))
        .await
        .assured("a branch instance restores the saved count");
    let resumed = restored
        .process_envelope_in_context(&input_envelope("alpha", 5, 31, 2), context())
        .await
        .assured("the restored guest processes a two-row batch");
    assert_eq!(
        counted_rows(&resumed),
        vec![
            CountedRow {
                token: 31,
                count: 5
            },
            CountedRow {
                token: 32,
                count: 6
            },
        ]
    );

    let mut beta = compiled
        .instantiate_branch(limits(), init("beta"), context(), None)
        .await
        .assured("another branch starts its own instance");
    let other = beta
        .process_envelope_in_context(&input_envelope("beta", 1, 41, 1), context())
        .await
        .assured("the other branch's guest processes its row");
    assert_eq!(
        counted_rows(&other),
        vec![CountedRow {
            token: 41,
            count: 1
        }]
    );
    let flushed = beta
        .process_envelope_in_context(&input_envelope("beta", 2, 51, 0), context())
        .await
        .assured("an empty batch is processed");
    assert!(flushed.is_empty(), "an empty batch emits nothing");
}

#[nervix_primitives::test]
async fn the_branch_counter_refuses_saved_state_of_another_shape() {
    let runtime = WasmRuntime::new(WasmRuntimeConfig {
        optimize: false,
        epoch_tick_interval: Duration::from_millis(5),
        epoch_deadline_ticks: 2_000,
        max_guest_buffer_bytes: 64 * 1024 * 1024,
    })
    .assured("the host runtime starts with a valid configuration");
    let compiled = runtime
        .compile_processor(
            &nervix_execution::Executor::default(),
            checked_in_module().as_bytes(),
        )
        .await
        .assured("the checked-in branch counter compiles");
    let refused = compiled
        .instantiate_branch(limits(), init("alpha"), context(), Some(&[1, 2, 3, 4]))
        .await;
    let Err(error) = refused else {
        panic!("a four-byte saved state must be refused");
    };
    assert_eq!(
        error.current_context().saved_state_rejection(),
        Some(SavedStateRejection::ApplicationState),
        "the guest refuses the application state rather than failing the restore"
    );
}
