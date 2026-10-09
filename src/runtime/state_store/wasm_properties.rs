//! Current WASM placement, checkpoint and generation-fencing representations.
//!
//! Layer: test harness.
//! - **Owns.** Exact checkpoint bytes, placement-key projections and scoped generation cases.
//! - **Depends on.** Production storage codecs, typed placements and vocabulary generators.
//! - **Must not know.** Guest callbacks, live ACK guards or concurrent checkpoint publication.

use nervix_arbitrary::{Arbitrary, Domain, WasmValues};

use super::{
    generation::{CheckpointMetadata, RESTORE_STATE_CHUNK_BYTES, StateNamespace},
    *,
};

#[test]
fn bolero_stored_wasm_checkpoints_preserve_bytes_and_typed_placement() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let mut values = WasmValues::new(bytes);
            let schema =
                SchemaFingerprint::from_digest(std::array::from_fn(|_| arbitrary.entropy().byte()));
            let generation = WasmStateGeneration::try_from(arbitrary.positive_u64().get())
                .assured("the generation is positive");
            let payload = values.bytes();
            let revision = arbitrary.entropy().any_u64();
            for checkpoint in [
                StoredCheckpoint::Inline(PersistedRuntimeStateEntry {
                    lsm: revision,
                    payload: payload.clone(),
                }),
                StoredCheckpoint::Segmented(CheckpointMetadata {
                    lsm: revision,
                    length: arbitrary.entropy().any_u64(),
                    digest: std::array::from_fn(|_| arbitrary.entropy().byte()),
                }),
            ] {
                let encoded = checkpoint.encode().assured("a current checkpoint encodes");
                if matches!(checkpoint, StoredCheckpoint::Segmented(_)) {
                    assert_eq!(encoded.len(), StoredCheckpoint::SEGMENTED_BYTES);
                }
                assert_eq!(
                    StoredCheckpoint::decode(&encoded).assured("the complete checkpoint decodes"),
                    checkpoint
                );
                assert_eq!(checkpoint.lsm(), revision);
            }
            for branched in [false, true] {
                let branch_key = if branched {
                    Some(
                        BranchKey::from_fields([(
                            nervix_models::FieldName::parse("tenant")
                                .assured("the field name is valid"),
                            crate::runtime::RuntimeValue::String(arbitrary.string()),
                        )])
                        .assured("a concrete branch has one typed field"),
                    )
                } else {
                    None
                };
                let placement = RuntimeStatePlacement {
                    domain: arbitrary.name(),
                    state: RuntimeState::WasmProcessor { schema, generation },
                    kind: ModelKind::WasmProcessor,
                    identifier: arbitrary.name(),
                    branch_key,
                };
                for namespace in [
                    StateNamespace::Initial,
                    StateNamespace::Restored(arbitrary.entropy().any_u64()),
                ] {
                    let key = namespace
                        .key(&placement)
                        .assured("the placement is inside its byte bound");
                    let (decoded_namespace, tail) = generation::physical_namespace(&key)
                        .assured("a current physical namespace decodes");
                    assert_eq!(decoded_namespace, namespace);
                    assert_eq!(tail, placement.as_storage_key());
                    let (decoded_namespace, decoded) =
                        physical_placement(&key).assured("a current placement decodes");
                    assert_eq!(decoded_namespace, namespace);
                    assert_eq!(decoded.state, placement.state);
                    assert_eq!(decoded.kind, placement.kind);
                    assert_eq!(decoded.identifier, placement.identifier);
                    assert_eq!(
                        decoded.branch,
                        placement.branch_key.as_ref().map(BranchKey::fingerprint)
                    );
                    for offset in [
                        0,
                        u64::try_from(RESTORE_STATE_CHUNK_BYTES)
                            .assured("the chunk policy fits u64"),
                        u64::MAX,
                    ] {
                        let mut chunk_key = generation::chunk_prefix(&key, revision);
                        chunk_key.extend_from_slice(&offset.to_be_bytes());
                        let chunks = generation::CheckpointChunkSet::try_from(chunk_key.as_slice())
                            .assured(
                                "a current chunk key has a complete placement and coordinates",
                            );
                        assert_eq!(chunks.placement_key, key);
                        assert_eq!(chunks.lsm, revision);
                        assert!(chunk_key < chunks.exclusive_end());
                    }
                }
            }
            let alpha = BranchKeyFingerprint::new([1; 32]);
            let beta = BranchKeyFingerprint::new([2; 32]);
            let mut generations = WasmStateGenerations::first();
            let initial = generations.of_branch(None);
            let selected = generations.begin_branch(alpha);
            let identity = ScheduledStateIdentity {
                schema_fingerprint: schema,
                wasm_state_generations: Some(generations),
            };
            for (branch, expected) in [
                (None, initial),
                (Some(&alpha), selected),
                (Some(&beta), initial),
            ] {
                let state = RuntimeState::WasmProcessor {
                    schema,
                    generation: expected,
                };
                assert_eq!(
                    identity.state_of(RuntimeStateKind::WasmProcessor, branch),
                    Some(state)
                );
                assert!(identity.names(state, branch));
            }
            assert!(!identity.names(
                RuntimeState::WasmProcessor {
                    schema,
                    generation: initial
                },
                Some(&alpha)
            ));
            let absent = ScheduledStateIdentity {
                schema_fingerprint: schema,
                wasm_state_generations: None,
            };
            assert_eq!(absent.state_of(RuntimeStateKind::WasmProcessor, None), None);
        });
}

#[test]
fn bolero_malformed_stored_wasm_checkpoints_fail_within_bounds() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes| {
            crate::archive_allocation_tests::assert_decode_frees_allocations(|| {
                StoredCheckpoint::decode(bytes)
            });
            match StoredCheckpoint::decode(bytes) {
                Ok(value) => assert_eq!(
                    StoredCheckpoint::decode(
                        &value
                            .encode()
                            .assured("a bounded decoded checkpoint encodes")
                    )
                    .assured("an accepted checkpoint decodes"),
                    value
                ),
                Err(error) => assert!(matches!(
                    error.current_context(),
                    RuntimePersistenceError::DecodeState
                )),
            }
        });
}
