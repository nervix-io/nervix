//! Current identity records through their production validation, signing and archive owners.
//!
//! Layer: test harness.
//! - **Owns.** Bounded complete-record generators, installation and private-key consistency checks.
//! - **Depends on.** Current consensus records, shared signed documents and the real record codec.
//! - **Must not know.** Bootstrap scheduling, network transport or licensing policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns cluster identity verification")
)]

use std::num::NonZeroUsize;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_licensing_protocol::{
    document::{DocumentLimits, verify},
    requests::{KeyRotationProof, Secret},
};

use super::*;
use crate::durable_batch::DurableBatch;

const KEY_MATERIAL: [&[u8]; 3] = [
    include_bytes!("../../../crypto/src/test_data/p256_test_key.pk8"),
    include_bytes!("test_data/retired-test-key.pk8"),
    include_bytes!("test_data/pending-test-key.pk8"),
];

pub(super) fn key(index: usize) -> ClusterKey {
    let private = KEY_MATERIAL[index];
    let signer =
        DocumentSigner::from_pkcs8(private).assured("the public test key has the P-256 profile");
    ClusterKey {
        kid: signer.key_id().clone(),
        public_jwk: signer.public_key().clone(),
        private_pkcs8: ClusterPrivateKey(private.to_vec()),
    }
}

pub(crate) fn candidate(byte: u8) -> ClusterIdentityRecord {
    ClusterIdentityRecord {
        cluster_uid: uuid::Builder::from_random_bytes([byte; 16]).into_uuid(),
        created_at: Timestamp::from_unix_nanos(i64::from(byte)),
        key_generation: NonZeroU64::MIN,
        ever_activated: false,
        current_key: key(usize::from(byte % 3)),
        retired_key: None,
        pending_key: None,
    }
}

pub(crate) fn inconsistent_candidate() -> ClusterIdentityRecord {
    let mut record = candidate(0x11);
    record.current_key.public_jwk = key(0).public_jwk;
    record
}

pub(crate) fn identity(arbitrary: &mut Arbitrary<'_>) -> ClusterIdentityRecord {
    let retired = arbitrary.entropy().flag();
    let pending = arbitrary.entropy().flag();
    let indices = match arbitrary.entropy().byte() % 3 {
        0 => [0, 1, 2],
        1 => [1, 2, 0],
        _ => [2, 0, 1],
    };
    let uid = std::array::from_fn(|_| arbitrary.entropy().byte());
    let created_at = arbitrary.timestamp();
    let first_generation = if retired { 2 } else { 1 };
    let last_generation = if pending { u64::MAX - 1 } else { u64::MAX };
    let generation = arbitrary
        .entropy()
        .between(first_generation..=last_generation);
    let retired_request_id = Uuid::from_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()));
    let pending_request_id = Uuid::from_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()));
    let mut record = ClusterIdentityRecord {
        cluster_uid: uuid::Builder::from_random_bytes(uid).into_uuid(),
        created_at,
        key_generation: NonZeroU64::new(generation).verified("the draw starts at one or two"),
        ever_activated: arbitrary.entropy().flag(),
        current_key: key(indices[0]),
        retired_key: if retired {
            Some(RetiredClusterKey {
                reference: reference(retired_request_id),
                key: key(indices[1]),
                retired_at: arbitrary.timestamp(),
                request_id: retired_request_id,
            })
        } else {
            None
        },
        pending_key: if pending {
            Some(PendingClusterKey {
                reference: reference(pending_request_id),
                key: key(indices[2]),
                requested_at: arbitrary.timestamp(),
                generation: NonZeroU64::new(
                    generation
                        .checked_add(1)
                        .verified("pending generations leave room for their checked increment"),
                )
                .verified("the increment is nonzero"),
                request_id: pending_request_id,
                prepared: None,
            })
        } else {
            None
        },
    };
    if pending && arbitrary.entropy().flag() {
        let rotation = record
            .pending_key
            .as_ref()
            .verified("the pending branch constructed a key");
        let prepared = rotation_request(
            &record,
            rotation,
            uuid::Uuid::from_u128(7),
            1,
            1,
            record.created_at,
            url::Url::parse("https://licensing.nervix.io").assured("fixed HTTPS fixture"),
        );
        record
            .pending_key
            .as_mut()
            .verified("pending key retained")
            .prepared = Some(prepared);
    }
    record
}

pub(crate) fn pending(arbitrary: &mut Arbitrary<'_>) -> PendingClusterKey {
    let reference = reference(Uuid::from_u128(2));
    PendingClusterKey {
        reference,
        key: key(2),
        requested_at: arbitrary.timestamp(),
        generation: NonZeroU64::new(arbitrary.entropy().between(2..=u64::MAX))
            .verified("draw begins at two"),
        request_id: Uuid::from_bytes(std::array::from_fn(|_| arbitrary.entropy().byte())),
        prepared: None,
    }
}

pub(crate) fn rotation_candidate(
    record: &ClusterIdentityRecord,
    request_id: Uuid,
) -> PendingClusterKey {
    let index = if record.current_key == key(0) { 1 } else { 0 };
    PendingClusterKey {
        reference: reference(request_id),
        key: key(index),
        requested_at: record.created_at,
        generation: NonZeroU64::new(record.key_generation.get() + 1)
            .verified("fixture leaves a generation"),
        request_id,
        prepared: None,
    }
}

pub(crate) fn rotation_request(
    record: &ClusterIdentityRecord,
    pending: &PendingClusterKey,
    activation_id: Uuid,
    decision_revision: u64,
    conditions_revision: u64,
    licensing_time: Timestamp,
    service: url::Url,
) -> crate::PreparedLicensingRequest {
    use nervix_licensing_protocol::requests::*;
    let limits = nervix_licensing_protocol::test_fixture::limits();
    let common = RequestCommon {
        protocol: 1,
        request_id: pending.request_id,
        challenge: Secret::new("retained-proof-challenge".to_owned()),
        sent_at: licensing_time
            .min(Timestamp::from_unix_nanos(i64::MAX - 1))
            .into_datetime(),
        aud: service,
        cluster_uid: record.cluster_uid,
        display_name: "cluster λ".to_owned(),
        client: ClientDescription {
            product: ClientProduct::NervixServer,
            version: "0.1.0-dev".to_owned(),
        },
        key_generation: record.key_generation.get(),
        trust_held: TrustHeld {
            working_keys: Vec::new(),
            revocation_serials: Vec::new(),
            succession_serials: Vec::new(),
        },
    };
    let signer = DocumentSigner::from_pkcs8(&pending.key.private_pkcs8.0)
        .assured("pending fixture key signs");
    let proof = signer
        .sign(
            &KeyRotationProof {
                challenge: common.challenge.clone(),
                cluster_uid: common.cluster_uid,
                activation_id,
                new_kid: pending.key.kid.clone(),
            },
            limits,
        )
        .assured("bounded fixture possession proof signs");
    let request = Request::RotateKey {
        common,
        activation_id,
        state: CommittedState {
            applied_decision_revision: decision_revision,
            applied_conditions_revision: conditions_revision,
            licensing_time: licensing_time.into_datetime(),
        },
        new_key: pending.key.public_jwk.clone(),
        proof,
    };
    let signer = DocumentSigner::from_pkcs8(&record.current_key.private_pkcs8.0)
        .assured("current fixture key signs");
    let body = signer
        .sign(&request, limits)
        .assured("bounded fixture rotation signs");
    let expires_at = licensing_time
        .min(Timestamp::from_unix_nanos(i64::MAX - 1))
        .checked_add(std::time::Duration::from_nanos(1))
        .unwrap_or(Timestamp::from_unix_nanos(i64::MAX));
    crate::PreparedLicensingRequest::new(body, expires_at, limits)
        .assured("bounded fixture request fits")
}

#[test]
fn first_committed_identity_wins_without_publishing_a_graph_revision() {
    for (first, second) in [
        (candidate(0x11), candidate(0x22)),
        (candidate(0x22), candidate(0x11)),
    ] {
        let mut state = StateMachineData {
            runtime_revision: 23,
            ..StateMachineData::default()
        };
        let installed = install(&mut state, &first);
        assert_eq!(installed.response, crate::ConsensusResponse::Applied);
        let winner = state
            .cluster_identity
            .clone()
            .verified("the first candidate was installed");
        let repeated = install(&mut state, &second);
        assert_eq!(repeated.response, crate::ConsensusResponse::Applied);
        assert!(Arc::ptr_eq(
            state
                .cluster_identity
                .as_ref()
                .verified("installation remains present"),
            &winner
        ));
        assert_eq!(winner.as_ref(), &first);
        assert_eq!(state.runtime_revision, 23);
        assert!(
            !installed.schedule_changed
                && !installed.domains_changed
                && !installed.resources_changed
                && !installed.transactions_changed
        );
        assert!(
            !repeated.schedule_changed
                && !repeated.domains_changed
                && !repeated.resources_changed
                && !repeated.transactions_changed
        );
    }
}

#[nervix_primitives::test]
async fn generation_carries_one_creation_sample_and_the_initial_key()
-> Result<(), Report<ClusterIdentityError>> {
    let created_at = Timestamp::from_unix_nanos(1_700_000_000_000_000_000);
    let record = ClusterIdentityRecord::generate(&Executor::default(), created_at).await?;
    record.validate()?;
    assert_eq!(record.created_at, created_at);
    assert_eq!(record.key_generation, NonZeroU64::MIN);
    assert!(record.retired_key.is_none() && record.pending_key.is_none());
    Ok(())
}

#[test]
fn invalid_cluster_uid_is_refused_by_both_record_decoders() {
    for bytes in [[0; 16], [u8::MAX; 16]] {
        let mut record = candidate(0x11);
        record.cluster_uid = Uuid::from_bytes(bytes);
        let Err(error) = record.validate() else {
            panic!("the cluster UID must be UUIDv4");
        };
        assert_eq!(error.current_context(), &ClusterIdentityError::Uid);
        let encoded =
            DurableBatch::encode(&record, 16 * 1024).assured("the bounded current record fits");
        assert!(crate::storage_decode::<ClusterIdentityRecord>(&encoded).is_err());
        let json = serde_json::to_vec(&record).assured("the current record serializes");
        assert!(serde_json::from_slice::<ClusterIdentityRecord>(&json).is_err());
    }
}

#[test]
fn private_public_and_key_id_validation_covers_every_key_slot() {
    for slot in 0..3 {
        for damage in 0..3 {
            let mut record = identity(&mut Arbitrary::new(&[u8::MAX; 128], Domain::Vocabulary));
            let affected = match slot {
                0 => &mut record.current_key,
                1 => {
                    &mut record
                        .retired_key
                        .as_mut()
                        .verified("the fixture has a retired key")
                        .key
                }
                _ => {
                    &mut record
                        .pending_key
                        .as_mut()
                        .verified("the fixture has a pending key")
                        .key
                }
            };
            let another = key(match slot {
                0 => 1,
                1 => 2,
                _ => 0,
            });
            let expected = match damage {
                0 => {
                    affected.private_pkcs8.0.zeroize();
                    ClusterIdentityError::PrivateKey
                }
                1 => {
                    affected.public_jwk = another.public_jwk;
                    ClusterIdentityError::PublicKeyMismatch
                }
                _ => {
                    affected.kid = another.kid;
                    ClusterIdentityError::KeyIdMismatch
                }
            };
            let Err(error) = record.validate() else {
                panic!("a damaged current key slot cannot validate");
            };
            assert_eq!(error.current_context(), &expected);
            let encoded =
                DurableBatch::encode(&record, 16 * 1024).assured("the bounded fixture fits");
            let Err(error) = crate::storage_decode::<ClusterIdentityRecord>(&encoded) else {
                panic!("the production decoder validates every key slot");
            };
            assert!(
                error
                    .to_string()
                    .contains("invalid consensus record storage")
            );
        }
    }
}

#[nervix_primitives::test]
async fn signing_only_handle_uses_the_committed_public_key_and_redacts_debug()
-> Result<(), Report<ClusterIdentityError>> {
    let record = candidate(0x11);
    let executor = Executor::default();
    let signer = ClusterSigner::from_record(&record, executor.clone())?;
    assert_eq!(
        format!("{signer:?}"),
        format!("ClusterSigner {{ kid: {:?} }}", record.current_key.kid)
    );
    let document = KeyRotationProof {
        challenge: Secret::new("a test-only challenge".to_string()),
        cluster_uid: record.cluster_uid,
        activation_id: candidate(0x22).cluster_uid,
        new_kid: candidate(0x22).current_key.kid,
    };
    let limits = DocumentLimits {
        bytes: NonZeroUsize::new(4096).assured("the document bound is positive"),
        signatures: NonZeroUsize::MIN,
        trust_documents: NonZeroUsize::MIN,
    };
    let reservation = executor
        .try_reserve(
            MemoryClass::Management,
            executor.limits().management_event_bytes.as_u64(),
        )
        .change_context(ClusterIdentityError::Admission)?;
    let signed = signer.sign(document.clone(), limits, reservation).await?;
    let verified: KeyRotationProof = verify(&signed, &record.current_key.public_jwk, limits)
        .change_context(ClusterIdentityError::Sign)?;
    assert_eq!(verified, document);
    Ok(())
}

#[test]
fn bolero_cluster_identity_record_preserves_all_keys_and_metadata() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(128)
        .for_each(|bytes: &[u8]| {
            let record = identity(&mut Arbitrary::new(bytes, Domain::Vocabulary));
            record
                .validate()
                .assured("the generator constructs valid current records");
            let encoded = DurableBatch::encode(&record, 16 * 1024)
                .assured("three fixed P-256 keys fit the record budget");
            let restored: ClusterIdentityRecord = crate::storage_decode(&encoded)
                .assured("a valid record restores through the production decoder");
            assert_eq!(restored, record);
            let json = serde_json::to_vec(&record).assured("the complete record serializes");
            let restored: ClusterIdentityRecord = serde_json::from_slice(&json)
                .assured("the complete current record deserializes and validates");
            assert_eq!(restored, record);
        });
}

pub(crate) fn reference(id: Uuid) -> nervix_models::CommandExecutionReference {
    nervix_models::CommandExecutionReference::parse(id.to_string())
        .assured("UUID text is a valid command reference")
}
