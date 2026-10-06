//! Runtime-state placement keys, their namespaces, and the index and chunk keys stored under them.
//!
//! Layer: test harness.
//! - **Owns.** Generated placements of every runtime state, Model kind, namespace and branch scope;
//!   the projection the stored-key decoder recovers from their keys; the prefixes scans rely on;
//!   and damaged keys read through the same decoder.
//! - **Depends on.** The production key layout and decoders, typed branch keys, and the
//!   vocabulary generators.
//! - **Must not know.** Checkpoint payloads, replication, or guest callbacks.

use nervix_arbitrary::{Arbitrary, Domain};

use super::{
    generation::{StateNamespace, chunk_prefix, domain_prefix, index_key, physical_namespace},
    *,
};

/// One runtime state of any kind, with the schema fingerprint and guest-state generation its kind
/// names.
fn runtime_state(arbitrary: &mut Arbitrary<'_>) -> RuntimeState {
    let schema = arbitrary.schema_fingerprint();
    match arbitrary.entropy().byte() % 8 {
        0 => RuntimeState::BranchAggregated,
        1 => RuntimeState::Correlator { schema },
        2 => RuntimeState::Deduplicator { schema },
        3 => RuntimeState::KafkaOffset,
        4 => RuntimeState::MaterializedRelay { schema },
        5 => {
            let generation = WasmStateGeneration::try_from(arbitrary.positive_u64().get())
                .assured("a positive draw is a valid guest-state generation");
            RuntimeState::WasmProcessor { schema, generation }
        }
        6 => RuntimeState::WindowProcessor { schema },
        _ => RuntimeState::BranchLru { schema },
    }
}

/// Where one entity's runtime state of any kind lives: any domain, Model kind and name, and an
/// absent or concrete branch.
pub(super) fn generated_placement(arbitrary: &mut Arbitrary<'_>) -> RuntimeStatePlacement {
    RuntimeStatePlacement {
        domain: arbitrary.rule_name(),
        state: runtime_state(arbitrary),
        kind: arbitrary.model_kind(),
        identifier: arbitrary.rule_name(),
        branch_key: BranchKey::generated_scope(arbitrary),
    }
}

/// The initial namespace, or a restored one of any generation.
fn generated_namespace(arbitrary: &mut Arbitrary<'_>) -> StateNamespace {
    if arbitrary.entropy().flag() {
        StateNamespace::Restored(arbitrary.entropy().any_u64())
    } else {
        StateNamespace::Initial
    }
}

/// A placement's physical key decodes to its namespace and to the projection the stored-key
/// decoder documents: its state with schema and generation, its Model kind and name, and the
/// fingerprint of its branch's canonical text. The key sits under its domain's and namespace's
/// prefixes and no other's, and the index and chunk keys scans address it by never reach another
/// placement's.
#[test]
fn bolero_runtime_state_keys_decode_their_placement_and_stay_apart() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let placement = generated_placement(&mut arbitrary);
            let namespace = generated_namespace(&mut arbitrary);
            let key = namespace
                .key(&placement)
                .assured("a generated placement stays inside the key bound");

            let (decoded_namespace, tail) =
                physical_namespace(&key).assured("a current physical namespace decodes");
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
            assert!(key.starts_with(&namespace.prefix(&placement.domain)));
            assert!(key.starts_with(&domain_prefix(&placement.domain)));

            let lsm = arbitrary.entropy().any_u64();
            let index = index_key(&key, lsm);
            let mut expected_index = key.clone();
            expected_index.push(0);
            expected_index.extend_from_slice(&lsm.to_be_bytes());
            assert_eq!(index, expected_index);
            let mut expected_chunks = index.clone();
            expected_chunks.push(0);
            assert_eq!(chunk_prefix(&key, lsm), expected_chunks);

            let other = generated_placement(&mut arbitrary);
            let other_namespace = generated_namespace(&mut arbitrary);
            let other_key = other_namespace
                .key(&other)
                .assured("a generated placement stays inside the key bound");
            if other_key == key {
                return;
            }
            let mut scan = key.clone();
            scan.push(0);
            let other_lsm = arbitrary.entropy().any_u64();
            assert!(!index_key(&other_key, other_lsm).starts_with(&scan));
            assert!(!chunk_prefix(&other_key, other_lsm).starts_with(&scan));
            if other.domain != placement.domain || other_namespace != namespace {
                assert!(!other_key.starts_with(&namespace.prefix(&placement.domain)));
            }
            if other.domain != placement.domain {
                assert!(!other_key.starts_with(&domain_prefix(&placement.domain)));
            }
        });
}

/// Changes a stored key the way damaged storage can: a byte flipped, inserted, removed or
/// recased, or the key cut short.
fn damage(arbitrary: &mut Arbitrary<'_>, key: &mut Vec<u8>) {
    let Some(length) = std::num::NonZeroUsize::new(key.len()) else {
        key.push(arbitrary.entropy().byte());
        return;
    };
    let position = arbitrary.entropy().index(length);
    match arbitrary.entropy().byte() % 5 {
        0 => {
            let bit = arbitrary.entropy().byte() % 8;
            key[position] ^= 1 << bit;
        }
        1 => key.insert(position, arbitrary.entropy().byte()),
        2 => {
            key.remove(position);
        }
        3 => key[position] ^= 0x20,
        _ => key.truncate(position),
    }
}

/// Damaged or arbitrary stored keys either fail with the store's typed storage failure, or decode
/// to a placement whose own key is exactly the stored key: a canonical domain, separators where the
/// layout puts them, the canonical spelling of the kind and name, and a branch scope that ends the
/// key or holds the branch text the fingerprint names.
#[test]
fn bolero_malformed_runtime_state_keys_fail_typed_or_decode_canonically() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let mut key = if arbitrary.entropy().flag() {
                let placement = generated_placement(&mut arbitrary);
                generated_namespace(&mut arbitrary)
                    .key(&placement)
                    .assured("a generated placement stays inside the key bound")
            } else {
                let mut key = Vec::new();
                for _ in 0..arbitrary.entropy().count(64) {
                    key.push(arbitrary.entropy().byte());
                }
                key
            };
            let damages = arbitrary.entropy().between(0..=3);
            for _ in 0..damages {
                damage(&mut arbitrary, &mut key);
            }

            let (namespace, stored) = match physical_placement(&key) {
                Ok(decoded) => decoded,
                Err(report) => {
                    assert!(
                        matches!(
                            report.current_context(),
                            RuntimePersistenceError::InvalidStorageFormat
                                | RuntimePersistenceError::DecodeState(_)
                        ),
                        "{report:?}"
                    );
                    return;
                }
            };
            let domain_end = key
                .iter()
                .position(|byte| *byte == 0)
                .verified("a decoded key separates its domain");
            let domain = std::str::from_utf8(&key[..domain_end])
                .assured("an accepted key holds its domain as text");
            let domain =
                DomainName::decode(domain).assured("an accepted key holds a canonical domain");
            let unbranched = RuntimeStatePlacement {
                domain,
                state: stored.state,
                kind: stored.kind,
                identifier: stored.identifier.clone(),
                branch_key: None,
            };
            let unbranched = namespace
                .key(&unbranched)
                .assured("a decoded placement stays inside the key bound");
            let Some(fingerprint) = stored.branch else {
                assert_eq!(key, unbranched);
                return;
            };
            let scope = unbranched
                .len()
                .checked_sub(1)
                .verified("an unbranched key ends with its scope byte");
            assert_eq!(key[..scope], unbranched[..scope]);
            assert_eq!(key[scope], 1);
            let text = std::str::from_utf8(&key[scope + 1..])
                .assured("an accepted branched key holds its branch as text");
            assert_eq!(fingerprint, BranchKeyFingerprint::of_canonical_text(text));
        });
}

/// The physical key of the unbranched branch-aggregated state of relay `events` in domain
/// `readings`, in the initial namespace.
fn fixture_key() -> Vec<u8> {
    let placement = RuntimeStatePlacement {
        domain: DomainName::parse("readings").assured("the fixture domain follows the name rule"),
        state: RuntimeState::BranchAggregated,
        kind: ModelKind::Relay,
        identifier: ModelName::parse("events").assured("the fixture name follows the name rule"),
        branch_key: None,
    };
    StateNamespace::Initial
        .key(&placement)
        .assured("the fixture placement stays inside the key bound")
}

/// Replaces every occurrence of `from` in `key` with `to`, which has the same length.
fn replace_all_in_key(key: &mut [u8], from: &[u8], to: &[u8]) {
    let mut start = 0;
    while let Some(offset) = key[start..]
        .windows(from.len())
        .position(|window| window == from)
    {
        let at = start + offset;
        key[at..at + to.len()].copy_from_slice(to);
        start = at + to.len();
    }
}

#[test]
fn a_stored_key_spelling_its_name_in_upper_case_is_refused() {
    let mut key = fixture_key();
    replace_all_in_key(&mut key, b"events", b"EVENTS");

    let decoded = physical_placement(&key);

    assert!(
        decoded.is_err(),
        "no store writes a Model name in upper case"
    );
}

#[test]
fn a_stored_key_with_another_byte_after_its_state_kind_is_refused() {
    let mut key = fixture_key();
    let tail = StateNamespace::Initial
        .prefix(&DomainName::parse("readings").assured("the fixture domain follows the rule"))
        .len();
    // The logical key begins with the domain and its separator, then the state-kind tag and the
    // separator this damages.
    let separator = tail + b"readings\0".len() + 1;
    assert_eq!(key[separator], 0);
    key[separator] = 7;

    let decoded = physical_placement(&key);

    assert!(
        decoded.is_err(),
        "the state kind is followed by its separator"
    );
}

#[test]
fn a_stored_key_whose_domain_breaks_the_name_rule_is_refused() {
    let mut key = fixture_key();
    replace_all_in_key(&mut key, b"readings", b"read ngs");
    let decoded = physical_placement(&key);
    assert!(decoded.is_err(), "no domain name holds a space");

    let mut key = fixture_key();
    replace_all_in_key(&mut key, b"readings", b"READINGS");
    let decoded = physical_placement(&key);
    assert!(decoded.is_err(), "no store writes a domain in upper case");
}
