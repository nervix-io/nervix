//! Complete snapshot bytes and branch validation before application restore.
//!
//! Layer: test harness.
//! - **Owns.** Bounded saves, restore acceptance and typed snapshot/application rejection cases.
//! - **Depends on.** Production branch contexts, guest snapshot codecs and bounded generators.
//! - **Must not know.** Host checkpoint generations, live ACKs or guest ABI global state.

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_arbitrary::WasmValues;
use nervix_wasm_protocol::{Envelope, GuestSnapshot, SavedStateRejection};

use crate::{
    BranchContext, GuestContext, GuestError, InputBatch, Processor, error::RejectedSnapshot,
};

#[derive(Debug, PartialEq, Eq)]
struct OpaqueState(Vec<u8>);

impl Processor for OpaqueState {
    fn create(_branch: &BranchContext) -> std::result::Result<Self, Report<GuestError>> {
        Ok(Self(vec![]))
    }
    fn process_batch(
        &mut self,
        _ctx: &mut GuestContext<'_>,
        _input: InputBatch,
    ) -> std::result::Result<(), Report<GuestError>> {
        Ok(())
    }
    fn save_state(&self) -> std::result::Result<Vec<u8>, Report<GuestError>> {
        Ok(self.0.clone())
    }
    fn restore(
        _branch: &BranchContext,
        state: &[u8],
    ) -> std::result::Result<Self, Report<GuestError>> {
        Ok(Self(state.to_vec()))
    }
}

#[derive(Debug)]
struct RestoreForbidden;

impl Processor for RestoreForbidden {
    fn create(_branch: &BranchContext) -> std::result::Result<Self, Report<GuestError>> {
        Ok(Self)
    }
    fn process_batch(
        &mut self,
        _ctx: &mut GuestContext<'_>,
        _input: InputBatch,
    ) -> std::result::Result<(), Report<GuestError>> {
        Ok(())
    }
    fn restore(
        _branch: &BranchContext,
        _state: &[u8],
    ) -> std::result::Result<Self, Report<GuestError>> {
        panic!("configuration must be validated before application restore")
    }
}

#[derive(Debug)]
struct Stateless;

impl Processor for Stateless {
    fn create(_branch: &BranchContext) -> std::result::Result<Self, Report<GuestError>> {
        Ok(Self)
    }
    fn process_batch(
        &mut self,
        _ctx: &mut GuestContext<'_>,
        _input: InputBatch,
    ) -> std::result::Result<(), Report<GuestError>> {
        Ok(())
    }
}

#[test]
fn bolero_sdk_snapshots_restore_every_byte_and_preserve_branch_isolation() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut values = WasmValues::new(bytes);
            let init = values.branch_init();
            let branch = BranchContext::from(init.clone());
            assert_eq!(branch.domain_name(), init.domain_name);
            assert_eq!(branch.domain_type(), init.domain_type);
            assert_eq!(branch.branch_key(), init.branch_key.as_deref());
            assert_eq!(branch.input_schema(), &init.input_schema);
            assert_eq!(branch.output_schemas(), init.output_schemas);
            for state in [values.bytes(), vec![], vec![0, 255, 0]] {
                let original = OpaqueState(state.clone());
                let saved = branch.encode_snapshot(
                    original
                        .save_state()
                        .assured("opaque computation state saves"),
                );
                assert_eq!(
                    GuestSnapshot::decode(&saved).assured("a saved SDK snapshot verifies"),
                    GuestSnapshot {
                        init_metadata: init.encode(),
                        application_state: state
                    }
                );
                assert_eq!(
                    branch
                        .restore_snapshot::<OpaqueState>(&saved)
                        .assured("matching state restores"),
                    original
                );
                let mut renamed = init.clone();
                renamed.domain_name.push_str("_restored");
                let renamed = BranchContext::from(renamed);
                assert_eq!(
                    renamed
                        .restore_snapshot::<OpaqueState>(&saved)
                        .assured("a domain rename preserves the state contract"),
                    original
                );
                for change in 0..5 {
                    let mut other = init.clone();
                    match change {
                        0 => {
                            other.branch_key = match other.branch_key {
                                None => Some(b"{\"tenant\":\"other\"}".to_vec()),
                                Some(_) => None,
                            }
                        }
                        1 => other.domain_type.push_str("_different"),
                        2 => other.input_schema.name.push_str("_different"),
                        3 => other
                            .input_schema
                            .fields
                            .push(nervix_wasm_protocol::ProcessorField {
                                name: "extra".into(),
                                ty: nervix_wasm_protocol::ProcessorType::Bool,
                                optional: true,
                            }),
                        _ => other.output_schemas.push(other.input_schema.clone()),
                    }
                    let error = BranchContext::from(other)
                        .restore_snapshot::<RestoreForbidden>(&saved)
                        .expect_err("another branch contract is rejected before application code");
                    assert!(matches!(
                        error.current_context(),
                        RejectedSnapshot::OtherBranchConfiguration
                    ));
                    assert_eq!(
                        error.current_context().verdict(),
                        SavedStateRejection::SnapshotEnvelope
                    );
                }
            }
        });
}

#[test]
fn bolero_sdk_snapshot_rejections_keep_envelope_and_application_verdicts() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut values = WasmValues::new(bytes);
            let init = values.branch_init();
            let branch = BranchContext::from(init.clone());
            let input = Envelope::Input {
                arrow_ipc_batch: values.bytes(),
                acks: values.sidecar(),
            }
            .encode();
            let error = branch
                .restore_snapshot::<RestoreForbidden>(&input)
                .expect_err("input is not a snapshot");
            assert!(matches!(
                error.current_context(),
                RejectedSnapshot::UndecodableEnvelope
            ));
            let snapshot = GuestSnapshot {
                init_metadata: input,
                application_state: values.bytes(),
            }
            .encode();
            let error = branch
                .restore_snapshot::<RestoreForbidden>(&snapshot)
                .expect_err("input is not branch metadata");
            assert!(matches!(
                error.current_context(),
                RejectedSnapshot::UndecodableInitMetadata
            ));
            for state in [vec![], vec![1], values.bytes()] {
                let saved = branch.encode_snapshot(state.clone());
                match branch.restore_snapshot::<Stateless>(&saved) {
                    Ok(_) => assert!(state.is_empty()),
                    Err(error) => {
                        assert!(!state.is_empty());
                        assert!(matches!(
                            error.current_context(),
                            RejectedSnapshot::ApplicationState
                        ));
                        assert_eq!(
                            error.current_context().verdict(),
                            SavedStateRejection::ApplicationState
                        );
                        assert!(error.downcast_ref::<GuestError>().is_some());
                    }
                }
            }
        });
}
