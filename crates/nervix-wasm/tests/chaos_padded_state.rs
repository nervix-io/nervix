//! The padded-state guest the external chaos runner's restore scenarios upload to a packaged node.
//!
//! `scripts/chaos/fixtures/wasm/padded-state-wat.sh` writes the guest in WebAssembly text for the
//! save size a run needs, and the run uploads exactly that text: a chaos run never builds guest
//! code. This test runs the generator and drives the module it writes through the host, to prove
//! what the restore verdicts rely on: a branch saves exactly the requested bytes and emits
//! nothing, a restored branch saves the very bytes it was handed, and a save of another size is
//! refused as application state rather than failing the restore.

use std::{path::PathBuf, process::Command, time::Duration};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{Timestamp, WasmProcessorLimits};
use nervix_primitives::sync::StdArc;
use nervix_wasm::{
    CompiledWasmProcessor, SavedStateRejection, WasmAckSidecar, WasmAckToken, WasmBranchInit,
    WasmEnvelope, WasmExecutionContext, WasmOutputRow, WasmProcessorField, WasmProcessorSchema,
    WasmProcessorType, WasmRuntime, WasmRuntimeConfig,
};
use nonzero_ext::nonzero;

/// The save size of the restore-fault scenario. A save this large makes the host build every later
/// input as a suffix of a buffer of at least 2 MiB.
const SAVE_BYTES: usize = 2 * 1024 * 1024;
/// The bytes of the batch count at the start of every save.
const COUNT_BYTES: usize = 8;
/// The xorshift state the guest starts its padding from.
const PADDING_SEED: u64 = 0x2545_f491_4f6c_dd1d;
/// The fields of the chaos graph's record, in declared order.
const RECORD_FIELDS: [(&str, WasmProcessorType); 4] = [
    ("event_id", WasmProcessorType::String),
    ("branch_name", WasmProcessorType::String),
    ("sequence", WasmProcessorType::I64),
    ("content", WasmProcessorType::String),
];

fn generator_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/chaos/fixtures/wasm/padded-state-wat.sh")
}

/// The module text the generator writes for a save of `bytes`.
fn padded_state_wat(bytes: usize) -> String {
    let output = Command::new("bash")
        .arg(generator_path())
        .arg(bytes.to_string())
        .output()
        .assured("bash runs the checked-in generator");
    assert!(
        output.status.success(),
        "the generator refused a {bytes}-byte save: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).assured("the generator writes WebAssembly text")
}

async fn compiled_guest() -> CompiledWasmProcessor {
    let runtime = WasmRuntime::new(WasmRuntimeConfig {
        optimize: false,
        epoch_tick_interval: Duration::from_millis(5),
        epoch_deadline_ticks: 2_000,
        max_guest_buffer_bytes: 64 * 1024 * 1024,
    })
    .assured("the host runtime starts with a valid configuration");
    runtime
        .compile_processor(
            &nervix_execution::Executor::default(),
            padded_state_wat(SAVE_BYTES).as_bytes(),
        )
        .await
        .assured("the generated padded guest compiles")
}

/// The limits `fixtures/restore-state.nspl` grants the guest.
fn limits() -> WasmProcessorLimits {
    WasmProcessorLimits {
        max_fuel: nonzero!(1_000_000_000u64),
        max_memory_bytes: nonzero!(100_663_296u64),
    }
}

fn record_schema() -> WasmProcessorSchema {
    WasmProcessorSchema {
        name: "chaos_record".to_string(),
        fields: RECORD_FIELDS
            .iter()
            .map(|(name, ty)| WasmProcessorField {
                name: (*name).to_string(),
                ty: ty.clone(),
                optional: false,
            })
            .collect(),
    }
}

fn init(branch: &str) -> WasmBranchInit {
    WasmBranchInit {
        domain_name: "chaos_baseline".to_string(),
        domain_type: "UNPACED".to_string(),
        branch_key: Some(format!("branch_name={branch}").into_bytes()),
        input_schema: record_schema(),
        output_schemas: vec![record_schema()],
    }
}

fn context() -> WasmExecutionContext {
    WasmExecutionContext::new(Timestamp::from_unix_nanos(1_700_000_000_000_000_000))
}

/// An input envelope of one chaos record of `branch` with `sequence`, carrying `token`.
fn input_envelope(branch: &str, sequence: i64, token: u64) -> WasmEnvelope {
    let schema = StdArc::new(Schema::new(
        RECORD_FIELDS
            .iter()
            .map(|(name, ty)| match ty {
                WasmProcessorType::I64 => Field::new(*name, DataType::Int64, false),
                _ => Field::new(*name, DataType::Utf8, false),
            })
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<StdArc<dyn Array>> = vec![
        StdArc::new(StringArray::from(vec![format!("{branch}-{sequence}")])),
        StdArc::new(StringArray::from(vec![branch.to_string()])),
        StdArc::new(Int64Array::from(vec![sequence])),
        StdArc::new(StringArray::from(vec!["chaos-padded-seed".to_string()])),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .assured("four columns match the four-field record schema");
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &schema)
            .assured("an in-memory IPC writer accepts the record schema");
        writer
            .write(&batch)
            .assured("an in-memory IPC writer accepts a batch of its schema");
        writer
            .finish()
            .assured("an in-memory IPC writer finishes its stream");
    }
    WasmEnvelope::input(
        ipc,
        WasmAckSidecar {
            rows: vec![WasmOutputRow {
                tokens: vec![WasmAckToken(token)],
                source_token: Some(WasmAckToken(token)),
            }],
            ..WasmAckSidecar::default()
        },
    )
}

/// The save the guest writes after `batches` input batches: the count, then the xorshift padding.
fn expected_save(batches: u64) -> Vec<u8> {
    let mut save = Vec::with_capacity(SAVE_BYTES);
    save.extend_from_slice(&batches.to_le_bytes());
    let mut state = PADDING_SEED;
    while save.len() < SAVE_BYTES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        save.extend_from_slice(&state.to_le_bytes());
    }
    save
}

/// The batch count at the start of a save.
fn saved_count(save: &[u8]) -> u64 {
    let count = save
        .first_chunk::<COUNT_BYTES>()
        .assured("every save of the padded guest starts with its count");
    u64::from_le_bytes(*count)
}

#[nervix_primitives::test]
async fn the_padded_guest_saves_exactly_its_size_and_emits_nothing() {
    let compiled = compiled_guest().await;
    let mut alpha = compiled
        .instantiate_branch(limits(), init("alpha"), context(), None)
        .await
        .assured("a new branch instance starts without saved state");
    let emitted = alpha
        .process_envelope_in_context(&input_envelope("alpha", 0, 11), context())
        .await
        .assured("the guest processes a one-row batch");
    assert!(emitted.is_empty(), "the padded guest emits nothing");

    let saved = alpha
        .save_state_in_context(context())
        .await
        .assured("the guest saves its padded state");
    assert_eq!(saved.len(), SAVE_BYTES);
    assert_eq!(saved_count(&saved), 1, "the save counts the one batch");
    assert!(
        saved == expected_save(1),
        "the save is the count followed by the guest's xorshift padding"
    );
}

#[nervix_primitives::test]
async fn a_restored_padded_guest_saves_the_bytes_it_was_handed() {
    let compiled = compiled_guest().await;
    let mut alpha = compiled
        .instantiate_branch(limits(), init("alpha"), context(), None)
        .await
        .assured("a new branch instance starts without saved state");
    alpha
        .process_envelope_in_context(&input_envelope("alpha", 0, 11), context())
        .await
        .assured("the guest processes a one-row batch");
    let saved = alpha
        .save_state_in_context(context())
        .await
        .assured("the guest saves its padded state");

    let mut restored = compiled
        .instantiate_branch(limits(), init("alpha"), context(), Some(&saved))
        .await
        .assured("a branch instance restores a save of its size");
    let saved_again = restored
        .save_state_in_context(context())
        .await
        .assured("the restored guest saves its state");
    assert!(
        saved_again == saved,
        "a restored guest saves exactly the bytes it was handed"
    );

    restored
        .process_envelope_in_context(&input_envelope("alpha", 1, 21), context())
        .await
        .assured("the restored guest processes a one-row batch");
    let resumed = restored
        .save_state_in_context(context())
        .await
        .assured("the restored guest saves its state");
    assert_eq!(saved_count(&resumed), 2, "the restored count carries on");
    assert!(
        resumed == expected_save(2),
        "the restored guest keeps the padding it was handed"
    );
}

#[nervix_primitives::test]
async fn the_padded_guest_refuses_a_save_of_another_size() {
    let compiled = compiled_guest().await;
    let save = expected_save(1);
    let short = save
        .get(..SAVE_BYTES - COUNT_BYTES)
        .assured("expected_save writes SAVE_BYTES bytes");
    let refused = compiled
        .instantiate_branch(limits(), init("alpha"), context(), Some(short))
        .await;
    let Err(error) = refused else {
        panic!("a save eight bytes short of the guest's size must be refused");
    };
    assert_eq!(
        error.current_context().saved_state_rejection(),
        Some(SavedStateRejection::ApplicationState),
        "the guest refuses the application state rather than failing the restore"
    );
}
