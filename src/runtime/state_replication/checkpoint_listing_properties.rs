//! The complete owner listing across its semantic conversion and production replication wire.
//!
//! Layer: test harness.
//! - **Owns.** Current listing generation, the complete owner oracle and invalid-key refusals.
//! - **Depends on.** Owner listings, typed branch keys and the bounded interconnect codec.
//! - **Must not know.** Catalog synchronization, runtime activation or checkpoint storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the owner checkpoint listing boundary")
)]

use nervix_arbitrary::{Arbitrary, Domain, assert_remote_keys_eq};
use nervix_execution::Executor;
use nervix_interconnect::{
    BranchCheckpointCursor, BranchCheckpointListingRequest, BranchCheckpointListingResponse,
    InterconnectRequest, RkyvMessage, RuntimeState,
};
use nervix_models::{
    RemoteRuntimeElementValue, RemoteRuntimeField, RemoteRuntimeValue, SchemaFingerprint,
    WasmStateGeneration,
};

use super::*;

const CASE_BYTES: usize = 2048;

struct Listings<'a> {
    arbitrary: Arbitrary<'a>,
}

impl<'a> Listings<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            arbitrary: Arbitrary::new(bytes, Domain::Vocabulary),
        }
    }

    fn number(&mut self) -> u64 {
        self.arbitrary.entropy().any_u64()
    }

    fn state(&mut self) -> RuntimeState {
        let schema = SchemaFingerprint::from_digest(std::array::from_fn(|_| {
            self.arbitrary.entropy().byte()
        }));
        match self.arbitrary.entropy().byte() % 8 {
            0 => RuntimeState::BranchAggregated,
            1 => RuntimeState::Correlator { schema },
            2 => RuntimeState::Deduplicator { schema },
            3 => RuntimeState::KafkaOffset,
            4 => RuntimeState::MaterializedRelay { schema },
            5 => RuntimeState::WasmProcessor {
                schema,
                generation: WasmStateGeneration::try_from(
                    self.arbitrary.entropy().boundary_biased(1..=u64::MAX),
                )
                .assured("a generated generation is nonzero"),
            },
            6 => RuntimeState::WindowProcessor { schema },
            _ => RuntimeState::BranchLru { schema },
        }
    }

    fn page(&mut self) -> BranchCheckpointPage {
        let cursor = BranchCheckpointCursor {
            epoch: self.number(),
            sequence: self.number(),
        };
        let count = self.arbitrary.entropy().count(4);
        let mut removed = Vec::with_capacity(count);
        for _ in 0..count {
            removed.push(self.arbitrary.remote_branch_key());
        }
        let count = self.arbitrary.entropy().count(4);
        let mut revised = Vec::with_capacity(count);
        for _ in 0..count {
            revised.push(BranchCheckpointRevision {
                branch_key: self.arbitrary.remote_branch_key(),
                state: self.state(),
                lsm: self.number(),
            });
        }
        BranchCheckpointPage {
            cursor,
            removed,
            revised,
            more: self.arbitrary.entropy().flag(),
        }
    }

    fn draw(&mut self) -> OwnerCheckpointListing {
        let variant = self.arbitrary.entropy().byte() % 3;
        let listing = match variant {
            0 => BranchCheckpointListing::Absent,
            1 => BranchCheckpointListing::Restarted(self.page()),
            _ => BranchCheckpointListing::Continued(self.page()),
        };
        OwnerCheckpointListing::from_remote(listing)
            .assured("the generator supplies valid, nonempty branch keys and finite current values")
    }

    fn malformed(&mut self) -> BranchCheckpointListing {
        let kind = self.arbitrary.entropy().byte() % 7;
        let in_removals = self.arbitrary.entropy().flag();
        let restart = self.arbitrary.entropy().flag();
        let mut page = self.page();
        let key = match kind {
            0 => Vec::new(),
            1 => vec![RemoteRuntimeField {
                name: "bad/name".to_string(),
                value: RemoteRuntimeValue::U64(self.number()),
            }],
            kind => {
                let value = match kind {
                    2 => RemoteRuntimeValue::Datetime("no-datetime".to_string()),
                    3 => RemoteRuntimeValue::F32(f32::from_bits(0x7fc0_0001)),
                    4 => RemoteRuntimeValue::F64(f64::NEG_INFINITY),
                    5 => RemoteRuntimeValue::Array(vec![RemoteRuntimeElementValue::Vec(vec![
                        RemoteRuntimeElementValue::F32(f32::INFINITY),
                    ])]),
                    _ => RemoteRuntimeValue::Vec(vec![RemoteRuntimeElementValue::Array(vec![
                        RemoteRuntimeElementValue::Datetime("no-datetime".to_string()),
                    ])]),
                };
                vec![RemoteRuntimeField {
                    name: "value".to_string(),
                    value,
                }]
            }
        };
        if in_removals {
            page.removed.push(Some(key));
        } else {
            page.revised.push(BranchCheckpointRevision {
                branch_key: Some(key),
                state: self.state(),
                lsm: self.number(),
            });
        }
        if restart {
            BranchCheckpointListing::Restarted(page)
        } else {
            BranchCheckpointListing::Continued(page)
        }
    }
}

async fn through_the_wire(
    executor: &Executor,
    listing: BranchCheckpointListing,
) -> BranchCheckpointListing {
    let response = BranchCheckpointListingResponse {
        result: Ok(listing),
    };
    let class = BranchCheckpointListingRequest::CLASS;
    let limit = executor.limits().replication_batch_bytes.as_u64();
    let (encoded, _encoded_charge) = response
        .clone()
        .encode_rkyv(executor.clone(), class, limit)
        .await
        .assured("a bounded listing fits the replication payload limit");
    let (decoded, _decoded_charge) =
        BranchCheckpointListingResponse::decode_rkyv(executor.clone(), class, encoded)
            .await
            .assured("a structurally valid current listing decodes");
    // The malformed target intentionally includes NaNs, for which numeric PartialEq is not an
    // oracle. Both targets compare every field below with the complete float-bit oracle instead.
    let received = match decoded.result {
        Ok(listing) => listing,
        Err(error) => panic!("a successful response changed to a failure: {error}"),
    };
    let expected = match response.result {
        Ok(listing) => listing,
        Err(_) => panic!("the generated response is successful"),
    };
    assert_listings_eq(&received, &expected);
    received
}

fn assert_listings_eq(actual: &BranchCheckpointListing, expected: &BranchCheckpointListing) {
    match (actual, expected) {
        (BranchCheckpointListing::Absent, BranchCheckpointListing::Absent) => {}
        (
            BranchCheckpointListing::Restarted(actual),
            BranchCheckpointListing::Restarted(expected),
        )
        | (
            BranchCheckpointListing::Continued(actual),
            BranchCheckpointListing::Continued(expected),
        ) => {
            assert_eq!(actual.cursor, expected.cursor);
            assert_eq!(actual.more, expected.more);
            assert_eq!(actual.removed.len(), expected.removed.len());
            assert_eq!(actual.revised.len(), expected.revised.len());
            for (actual, expected) in actual.removed.iter().zip(&expected.removed) {
                assert_remote_keys_eq(actual, expected);
            }
            for (actual, expected) in actual.revised.iter().zip(&expected.revised) {
                assert_eq!(actual.state, expected.state);
                assert_eq!(actual.lsm, expected.lsm);
                assert_remote_keys_eq(&actual.branch_key, &expected.branch_key);
            }
        }
        (actual, expected) => panic!("the listing variant changed: {actual:?} != {expected:?}"),
    }
}

fn runtime() -> nervix_primitives::runtime::Runtime {
    nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the ordinary property runtime builds")
}

#[test]
fn bolero_owner_checkpoint_listings_round_trip() {
    let runtime = runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|bytes: &[u8]| {
            let owner = Listings::new(bytes).draw();
            let executor = Executor::default();
            let received = runtime.block_on(through_the_wire(&executor, owner.to_remote()));
            let restored = OwnerCheckpointListing::from_remote(received)
                .assured("the current owner listing carries valid branch keys");
            assert_eq!(
                restored, owner,
                "every owner field, branch and ordered revision survives the semantic and wire \
                 boundaries"
            );
            assert_listings_eq(&restored.to_remote(), &owner.to_remote());
            assert_eq!(executor.snapshot().commands_memory.reserved_bytes, 0);
        });
}

#[test]
fn bolero_malformed_checkpoint_listing_keys_fail_typed() {
    let runtime = runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|bytes: &[u8]| {
            let listing = Listings::new(bytes).malformed();
            let executor = Executor::default();
            let received = runtime.block_on(through_the_wire(&executor, listing));
            match OwnerCheckpointListing::from_remote(received) {
                Err(error) => {
                    assert_eq!(error.current_context(), &CheckpointListingError::BranchKey)
                }
                Ok(listing) => {
                    panic!("a malformed branch key reached an owner listing: {listing:?}")
                }
            }
            assert_eq!(executor.snapshot().commands_memory.reserved_bytes, 0);
        });
}

#[test]
fn owner_listing_value_and_cursor_boundaries() {
    let runtime = runtime();
    let executor = Executor::default();
    for kind in 0..15 {
        let mut input = vec![1, 0, kind];
        input.extend_from_slice(&[u8::MAX; 128]);
        let key = Arbitrary::new(&input, Domain::Vocabulary).remote_branch_key();
        let page = BranchCheckpointPage {
            cursor: BranchCheckpointCursor {
                epoch: u64::MAX,
                sequence: u64::MAX,
            },
            removed: vec![None, key.clone()],
            revised: vec![
                BranchCheckpointRevision {
                    branch_key: key.clone(),
                    state: RuntimeState::WasmProcessor {
                        schema: SchemaFingerprint::from_digest([u8::MAX; 32]),
                        generation: WasmStateGeneration::try_from(u64::MAX)
                            .assured("the maximum generation is nonzero"),
                    },
                    lsm: u64::MAX,
                },
                BranchCheckpointRevision {
                    branch_key: None,
                    state: RuntimeState::WindowProcessor {
                        schema: SchemaFingerprint::from_digest([0; 32]),
                    },
                    lsm: 0,
                },
            ],
            more: true,
        };
        for listing in [
            BranchCheckpointListing::Restarted(page.clone()),
            BranchCheckpointListing::Continued(page),
        ] {
            let owner = OwnerCheckpointListing::from_remote(listing)
                .assured("every deliberate current branch value is valid");
            let received = runtime.block_on(through_the_wire(&executor, owner.to_remote()));
            let restored = OwnerCheckpointListing::from_remote(received)
                .assured("a current encoded owner listing restores");
            assert_eq!(restored, owner);
            assert_listings_eq(&restored.to_remote(), &owner.to_remote());
        }
    }
    assert_eq!(executor.snapshot().commands_memory.reserved_bytes, 0);
}
