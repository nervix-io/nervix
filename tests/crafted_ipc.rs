//! Public fixtures carrying a valid envelope around a deliberately misframed Arrow stream.
//!
//! Layer: test harness.
//! - **Owns.** Current IPC streams with altered body declarations and the guest that emits them.
//! - **Depends on.** Arrow's message schema, the public WASM envelope and scenario setup.
//! - **Must not know.** The server's Arrow decoder or the runtime's validation implementation.

use arrow_array::Int32Array;
use meticulous::{OptionExt as _, ResultExt as _};

use super::*;

pub(crate) struct CraftedIpc {
    bytes: Vec<u8>,
}

impl CraftedIpc {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub(crate) fn declaring(mut self, declared: i64) -> Self {
        let mut offset = 0_usize;
        loop {
            let metadata = offset.checked_add(8).assured("the fixture stream is small");
            assert_eq!(&self.bytes[offset..offset + 4], &[0xff; 4]);
            let length = usize::try_from(i32::from_le_bytes(
                self.bytes[offset + 4..metadata].try_into().assured("a frame word has four bytes"),
            )).assured("a written message has a positive metadata length");
            assert_ne!(length, 0, "the nonempty fixture has a record batch");
            let end = metadata.checked_add(length).assured("the fixture message is small");
            let message = arrow_ipc::root_as_message(&self.bytes[metadata..end])
                .assured("the fixture's message verifies before its declaration is altered");
            if message.header_type() == arrow_ipc::MessageHeader::RecordBatch {
                let field = message._tab.vtable().get(arrow_ipc::Message::VT_BODYLENGTH);
                assert_ne!(field, 0, "the nonempty fixture has a message body");
                let table = metadata.checked_add(message._tab.loc()).assured("the table is within the fixture");
                let start = table.checked_add(usize::from(field)).assured("the field is within the fixture");
                let end = start.checked_add(8).assured("the field has eight bytes");
                self.bytes[start..end].copy_from_slice(&declared.to_le_bytes());
                return self;
            }
            let body = usize::try_from(message.bodyLength()).assured("the undamaged message length is positive");
            offset = end.checked_add(body).assured("the next message is within the fixture");
        }
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

#[given(expr = "node {string} has a WASM fixture declaring {int} Arrow body bytes to relay {string} in resource directory {string}")]
async fn given_guest_with_declared_body(
    world: &mut ScenarioWorld,
    node: String,
    declared: i64,
    relay: String,
    placeholder: String,
) {
    let schema = StdArc::new(ArrowSchema::new(vec![ArrowField::new("", ArrowDataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![StdArc::new(Int32Array::from(vec![42]))])
        .assured("the generated fixture column is well typed");
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).assured("the fixture schema writes");
        writer.write(&batch).assured("the fixture batch writes");
        writer.finish().assured("the fixture stream ends");
    }
    let bytes = CraftedIpc::new(bytes).declaring(declared).into_bytes();
    let envelope = WasmEnvelope::output(
        bytes,
        vec![WasmRoutedOutput::new(
            relay,
            vec![WasmOutputColumnRef::generated(0)],
            WasmAckSidecar { rows: vec![WasmOutputRow::default()], ..WasmAckSidecar::default() },
        )],
    ).encode().assured("the fixture envelope is current and valid");
    let encoded = envelope.iter().map(|byte| format!("\\{byte:02x}")).collect::<String>();
    let length = envelope.len();
    assert!(length < 32768, "the fixture fits its one-page guest memory");
    let wasm = format!(r#"(module
      (import "env" "nervix_domain_time_nanos" (func $domain_time (result i64)))
      (import "env" "nervix_timeout_after_nanos" (func $timeout (param i64) (result i64)))
      (memory (export "memory") 1)
      (global $emitted (mut i32) (i32.const 0))
      (data (i32.const 32768) "{encoded}")
      (func (export "nervix_buffer_ptr") (result i32) (i32.const 32768))
      (func (export "nervix_buffer_len") (result i32) (i32.const {length}))
      (func (export "nervix_buffer_capacity") (result i32) (i32.const 65536))
      (func (export "nervix_alloc") (param i32) (result i32) (i32.const 0))
      (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
      (func (export "nervix_current_domain_time_nanos") (result i64) call $domain_time)
      (func (export "nervix_process_batch") (param i32 i32) (result i32)
        i32.const 1 global.set $emitted i32.const 0)
      (func (export "nervix_on_timeout") (param i64) (result i32) (i32.const 0))
      (func (export "nervix_flush") (result i32) (i32.const 0))
      (func (export "nervix_read_emit") (result i32)
        global.get $emitted
        if (result i32)
          i32.const 0 global.set $emitted i32.const {length}
        else i32.const 0 end)
      (func (export "nervix_dump_state") (result i32) (i32.const 0))
      (func (export "nervix_load_state") (param i32 i32) (result i32) (i32.const 0))
      (func (export "nervix_reset_state") (result i32) (i32.const 0))
    )"#).into_bytes();
    place_generated_wasm_processor_fixture(world, &node, &placeholder, wasm).await;
}

#[then("every node still answers cluster status")]
async fn then_every_node_still_answers_cluster_status(world: &mut ScenarioWorld) {
    for node in world.cluster().node_ids() {
        let status = world
            .cluster()
            .status_text(&node, PhaseDeadline::after(STATUS_REQUEST_TIMEOUT))
            .await
            .assured("each node still answers its own public cluster status request");
        assert!(!status.is_empty(), "node '{node}' returned an empty cluster status");
    }
}
