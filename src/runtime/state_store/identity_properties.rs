//! Ownership-handoff and forced-recovery records, and the keys a node stores them under.
//!
//! Layer: test harness.
//! - **Owns.** Generated transitions carrying checkpoints of every runtime state kind, their stored
//!   preparation and completion records, the keys they are stored under, and damaged records read
//!   through the same decoders.
//! - **Depends on.** The production record codecs and key layout, typed placements, and the
//!   vocabulary generators.
//! - **Must not know.** Coordination policy, replication, or schedule publication.

use nervix_arbitrary::{Arbitrary, Domain};

use super::{key_properties::generated_placement, *};

/// A placement's remote form as archived bytes. Typed branch values compare by these bytes, which
/// tell apart what value equality does not: NaN payloads, signed zeros and datetime offsets.
fn remote_bits(placement: &nervix_interconnect::StatePlacementEnvelope) -> Vec<u8> {
    rkyv::to_bytes::<rkyv::rancor::Error>(placement)
        .assured("a remote placement archives")
        .to_vec()
}

/// One checkpoint a transition carries: a placement of any kind and the entry stored for it.
fn checkpoint(
    arbitrary: &mut Arbitrary<'_>,
) -> (RuntimeStatePlacement, PersistedRuntimeStateEntry) {
    let placement = generated_placement(arbitrary);
    let lsm = arbitrary.entropy().any_u64();
    let payload = arbitrary.string().into_bytes();
    (placement, PersistedRuntimeStateEntry { lsm, payload })
}

/// Asserts that `stored` carries exactly the placements and entries of `checkpoints`, in order.
fn assert_same_checkpoints(
    stored: &[(
        nervix_interconnect::StatePlacementEnvelope,
        PersistedRuntimeStateEntry,
    )],
    checkpoints: &[(RuntimeStatePlacement, PersistedRuntimeStateEntry)],
) {
    assert_eq!(stored.len(), checkpoints.len());
    for ((envelope, entry), (placement, expected)) in stored.iter().zip(checkpoints) {
        assert_eq!(remote_bits(envelope), remote_bits(&placement.to_remote()));
        assert_eq!(entry, expected);
        let restored = RuntimeStatePlacement::from_remote(envelope.clone())
            .assured("a placement a node stored converts back");
        assert_eq!(
            remote_bits(&restored.to_remote()),
            remote_bits(&placement.to_remote())
        );
    }
}

/// Everything one generated ownership transition names.
struct Transition {
    coordination: CoordinationIdentity,
    operation_id: String,
    source: ClusterNodeName,
    destination: ClusterNodeName,
    source_incarnation: ClusterNodeIncarnation,
    destination_incarnation: ClusterNodeIncarnation,
    entity: DomainNodeRef,
    base_schedule_fingerprint: [u8; 32],
    target_schedule_fingerprint: [u8; 32],
    checkpoints: Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>,
}

impl Transition {
    fn generated(arbitrary: &mut Arbitrary<'_>) -> Self {
        let coordination = CoordinationIdentity::new(
            arbitrary.rule_name(),
            arbitrary.entropy().any_u64(),
            arbitrary.entropy().any_u64(),
        );
        let entity = DomainNodeRef::node_in(
            arbitrary.rule_name(),
            arbitrary.model_kind(),
            arbitrary.rule_name::<ModelName>(),
        );
        Self {
            coordination,
            operation_id: arbitrary.transaction_id(),
            source: arbitrary.rule_name(),
            destination: arbitrary.rule_name(),
            source_incarnation: arbitrary.incarnation(),
            destination_incarnation: arbitrary.incarnation(),
            entity,
            base_schedule_fingerprint: arbitrary.digest(),
            target_schedule_fingerprint: arbitrary.digest(),
            checkpoints: arbitrary.records(checkpoint),
        }
    }

    fn handoff(&self) -> RuntimeStateHandoffTransition<'_> {
        RuntimeStateHandoffTransition {
            coordination: &self.coordination,
            operation_id: &self.operation_id,
            source: &self.source,
            destination: &self.destination,
            source_incarnation: self.source_incarnation,
            destination_incarnation: self.destination_incarnation,
            entity: &self.entity,
            base_schedule_fingerprint: self.base_schedule_fingerprint,
            target_schedule_fingerprint: self.target_schedule_fingerprint,
        }
    }

    fn forced_recovery(&self) -> ForcedRuntimeStateRecoveryTransition<'_> {
        ForcedRuntimeStateRecoveryTransition {
            operation_id: &self.operation_id,
            source: &self.source,
            destination: &self.destination,
            destination_incarnation: self.destination_incarnation,
            entity: &self.entity,
            target_schedule_fingerprint: self.target_schedule_fingerprint,
        }
    }

    fn handoff_key(&self) -> Vec<u8> {
        RuntimeStateStore::handoff_preparation_key(
            &self.coordination,
            &self.operation_id,
            &self.entity.domain,
            self.entity.kind(),
            self.entity.identifier(),
        )
    }

    fn forced_recovery_key(&self) -> Vec<u8> {
        RuntimeStateStore::forced_recovery_key(
            &self.entity.domain,
            self.entity.kind(),
            self.entity.identifier(),
        )
    }
}

/// A handoff preparation, a forced-recovery preparation and its completion each restore every
/// identity, fingerprint and checkpoint of the transition they were stored for, and two transitions
/// share a stored key exactly when they name the same handoff or the same recovered entity.
#[test]
fn bolero_runtime_state_identity_records_round_trip() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let transition = Transition::generated(&mut arbitrary);

            let encoded = RuntimeStateStore::encode_handoff_preparation(
                &transition.handoff(),
                &transition.checkpoints,
            )
            .assured("a bounded handoff preparation encodes");
            let stored = RuntimeStateStore::decode_handoff_preparation(&encoded)
                .assured("a stored handoff preparation decodes from its own encoding");
            assert_eq!(stored.coordination, transition.coordination);
            assert_eq!(stored.operation_id, transition.operation_id);
            assert_eq!(stored.source, transition.source);
            assert_eq!(stored.destination, transition.destination);
            assert_eq!(stored.source_incarnation, transition.source_incarnation);
            assert_eq!(
                stored.destination_incarnation,
                transition.destination_incarnation
            );
            assert_eq!(stored.domain, transition.entity.domain);
            assert_eq!(stored.kind, transition.entity.kind());
            assert_eq!(&stored.identifier, transition.entity.identifier());
            assert_eq!(
                stored.base_schedule_fingerprint,
                transition.base_schedule_fingerprint
            );
            assert_eq!(
                stored.target_schedule_fingerprint,
                transition.target_schedule_fingerprint
            );
            assert_same_checkpoints(&stored.checkpoints, &transition.checkpoints);

            let forced = transition.forced_recovery();
            let encoded = RuntimeStateStore::encode_forced_recovery_preparation(
                &forced,
                &transition.checkpoints,
            )
            .assured("a bounded forced-recovery preparation encodes");
            let stored = RuntimeStateStore::decode_forced_recovery_preparation(&encoded)
                .assured("a stored forced-recovery preparation decodes from its own encoding");
            assert_eq!(stored.recovery, forced.identity());
            assert!(stored.accepts(&forced));
            assert_eq!(
                stored.destination_incarnation,
                transition.destination_incarnation
            );
            assert_eq!(
                stored.target_schedule_fingerprint,
                transition.target_schedule_fingerprint
            );
            assert_eq!(stored.checkpoints.len(), transition.checkpoints.len());
            for ((placement, entry), (expected_placement, expected_entry)) in
                stored.checkpoints.iter().zip(&transition.checkpoints)
            {
                assert_eq!(
                    remote_bits(&placement.to_remote()),
                    remote_bits(&expected_placement.to_remote())
                );
                assert_eq!(entry, expected_entry);
            }

            let encoded = RuntimeStateStore::encode_forced_recovery_completion(&forced)
                .assured("a forced-recovery completion encodes");
            let completed = RuntimeStateStore::decode_forced_recovery_completion(&encoded)
                .assured("a stored forced-recovery completion decodes from its own encoding");
            assert_eq!(completed, forced.identity());
            assert!(completed.matches(&forced));

            let other = Transition::generated(&mut arbitrary);
            let same_handoff = other.coordination == transition.coordination
                && other.operation_id == transition.operation_id
                && other.entity == transition.entity;
            assert_eq!(
                other.handoff_key() == transition.handoff_key(),
                same_handoff
            );
            assert_eq!(
                other.forced_recovery_key() == transition.forced_recovery_key(),
                other.entity == transition.entity
            );
        });
}

/// Arbitrary bytes read as a stored handoff preparation, forced-recovery preparation or
/// completion either fail with the store's typed decode failure, or restore a record whose
/// placements convert back to typed placements and which stores back unchanged.
#[test]
fn bolero_malformed_runtime_state_identity_records_fail_typed() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            crate::archive_allocation_tests::assert_decode_frees_allocations(|| {
                RuntimeStateStore::decode_handoff_preparation(bytes)
            });
            crate::archive_allocation_tests::assert_decode_frees_allocations(|| {
                RuntimeStateStore::decode_forced_recovery_preparation(bytes)
            });
            crate::archive_allocation_tests::assert_decode_frees_allocations(|| {
                RuntimeStateStore::decode_forced_recovery_completion(bytes)
            });
            let is_decode_failure = |report: &Report<RuntimePersistenceError>| {
                matches!(
                    report.current_context(),
                    RuntimePersistenceError::DecodeState
                )
            };
            match RuntimeStateStore::decode_handoff_preparation(bytes) {
                Ok(stored) => {
                    let entity = DomainNodeRef::node_in(
                        stored.domain.clone(),
                        stored.kind,
                        stored.identifier.clone(),
                    );
                    let handoff = RuntimeStateHandoffTransition {
                        coordination: &stored.coordination,
                        operation_id: &stored.operation_id,
                        source: &stored.source,
                        destination: &stored.destination,
                        source_incarnation: stored.source_incarnation,
                        destination_incarnation: stored.destination_incarnation,
                        entity: &entity,
                        base_schedule_fingerprint: stored.base_schedule_fingerprint,
                        target_schedule_fingerprint: stored.target_schedule_fingerprint,
                    };
                    let mut checkpoints = Vec::new();
                    for (envelope, entry) in &stored.checkpoints {
                        let Ok(placement) = RuntimeStatePlacement::from_remote(envelope.clone())
                        else {
                            // Startup refuses such a preparation with its typed placement error.
                            return;
                        };
                        checkpoints.push((placement, entry.clone()));
                    }
                    let encoded =
                        RuntimeStateStore::encode_handoff_preparation(&handoff, &checkpoints)
                            .assured("a decoded handoff preparation encodes again");
                    let again = RuntimeStateStore::decode_handoff_preparation(&encoded)
                        .assured("a re-encoded handoff preparation decodes");
                    assert_eq!(again.coordination, stored.coordination);
                    assert_eq!(again.operation_id, stored.operation_id);
                    assert_same_checkpoints(&again.checkpoints, &checkpoints);
                }
                Err(report) => assert!(is_decode_failure(&report), "{report:?}"),
            }
            if let Err(report) = RuntimeStateStore::decode_forced_recovery_preparation(bytes) {
                assert!(is_decode_failure(&report), "{report:?}");
            }
            if let Err(report) = RuntimeStateStore::decode_forced_recovery_completion(bytes) {
                assert!(is_decode_failure(&report), "{report:?}");
            }
        });
}

#[test]
fn a_handoff_refusing_its_second_checkpoint_frees_the_first() {
    let entropy = [0u8; 512];
    let mut arbitrary = Arbitrary::new(&entropy, Domain::Vocabulary);
    let mut transition = Transition::generated(&mut arbitrary);
    transition.checkpoints = vec![checkpoint(&mut arbitrary), checkpoint(&mut arbitrary)];
    transition.checkpoints[0].0.identifier =
        ModelName::parse("first_checkpoint").assured("the first checkpoint name is valid");
    transition.checkpoints[1].0.identifier =
        ModelName::parse("second_checkpoint").assured("the second checkpoint name is valid");

    let mut encoded = RuntimeStateStore::encode_handoff_preparation(
        &transition.handoff(),
        &transition.checkpoints,
    )
    .assured("the current handoff preparation archives");
    let target = b"second_checkpoint";
    assert_eq!(
        encoded
            .windows(target.len())
            .filter(|window| *window == target)
            .count(),
        1,
        "the second checkpoint name occurs once"
    );
    let start = encoded
        .windows(target.len())
        .position(|window| window == target)
        .assured("the second checkpoint name is archived");
    encoded[start + 7] = b'!';

    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(encoded.len());
    aligned.extend_from_slice(&encoded);
    rkyv::access::<rkyv::Archived<StoredHandoffPreparation>, rkyv::rancor::Error>(&aligned)
        .assured("the changed checkpoint leaves a valid archive shape");
    crate::archive_allocation_tests::assert_decode_frees_allocations(|| {
        let result = RuntimeStateStore::decode_handoff_preparation(&encoded);
        assert!(matches!(
            result.as_ref().map_err(|error| error.current_context()),
            Err(RuntimePersistenceError::DecodeState)
        ));
        result
    });
}
