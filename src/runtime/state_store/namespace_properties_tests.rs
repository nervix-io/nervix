//! The namespaces restore maintenance walks: the domain a stored key names after its namespace,
//! and the cursor that visits each namespace of each domain once.
//!
//! Layer: test harness.
//! - **Owns.** Placement, index and chunk keys of several domains in the initial namespace and in
//!   restored generations, walks over them through the maintenance cursor, and damaged keys read
//!   through the same decoders.
//! - **Depends on.** The production key layout, the maintenance cursor and its domain decoder, a
//!   temporary database, and the key generators of the key properties.
//! - **Must not know.** Checkpoint payloads, restore authority, or which namespaces maintenance
//!   measures and reclaims.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::{BTreeMap, BTreeSet};

use nervix_arbitrary::{Arbitrary, Domain};

use super::{
    super::key_properties::{
        arbitrary_key, damage_repeatedly, generated_chunk_key, generated_namespace,
        generated_placement, generated_placement_key,
    },
    *,
};

/// One namespace of one domain a walk visits.
#[derive(Debug, PartialEq, Eq)]
struct Visit {
    namespace: StateNamespace,
    domain: DomainName,
}

/// A temporary database holding `keys` in one keyspace, the way a store holds its checkpoint
/// headers, index entries, chunks or restore receipts. Fields drop in order, so the database
/// closes before its directory is removed.
struct StoredKeys {
    keyspace: Keyspace,
    database: Database,
    _directory: tempfile::TempDir,
}

impl StoredKeys {
    fn new<'key>(keys: impl IntoIterator<Item = &'key Vec<u8>>) -> Self {
        let directory =
            tempfile::tempdir().assured("the test host provides a writable temporary directory");
        let database = Database::builder(directory.path())
            .open()
            .assured("a temporary test directory holds a database");
        let keyspace = database
            .keyspace("staged_keys", KeyspaceCreateOptions::default)
            .assured("a new database opens a keyspace");
        for key in keys {
            keyspace
                .insert(key.as_slice(), b"value")
                .assured("a bounded key is stored");
        }
        Self {
            keyspace,
            database,
            _directory: directory,
        }
    }
}

/// Where the walk resumes after `namespace` of `domain`: its prefix with the closing separator
/// raised by one, so no key of that namespace lies at or beyond it.
fn namespace_end(namespace: StateNamespace, domain: &DomainName) -> Vec<u8> {
    let mut end = namespace.prefix(domain);
    *end.last_mut()
        .assured("a namespace prefix ends with its closing separator") = 1;
    end
}

/// A key a store keeps for the placement `placement_key` names: its checkpoint header or restore
/// receipt, an index entry of one of its revisions, or a chunk of one of them.
fn stored_key(arbitrary: &mut Arbitrary<'_>, placement_key: Vec<u8>) -> Vec<u8> {
    match arbitrary.entropy().byte() % 3 {
        0 => placement_key,
        1 => generation::index_key(&placement_key, arbitrary.entropy().any_u64()),
        _ => generated_chunk_key(arbitrary, &placement_key),
    }
}

/// Keys of up to three domains, each in up to three namespaces holding up to three placements'
/// headers, index entries or chunks.
struct StagedKeys {
    /// Every stored key, with the prefix of the namespace it lies in.
    keys: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Every namespace a walk visits, keyed by its prefix.
    namespaces: BTreeMap<Vec<u8>, Visit>,
}

impl StagedKeys {
    fn generated(arbitrary: &mut Arbitrary<'_>) -> Self {
        let mut keys = BTreeMap::new();
        let mut namespaces = BTreeMap::new();
        let domains = arbitrary
            .entropy()
            .positive_count(nonzero_ext::nonzero!(3_usize));
        for _ in 0..domains {
            let domain: DomainName = arbitrary.rule_name();
            let domain_namespaces = arbitrary
                .entropy()
                .positive_count(nonzero_ext::nonzero!(3_usize));
            for _ in 0..domain_namespaces {
                let namespace = generated_namespace(arbitrary);
                let prefix = namespace.prefix(&domain);
                let placements = arbitrary
                    .entropy()
                    .positive_count(nonzero_ext::nonzero!(3_usize));
                for _ in 0..placements {
                    let mut placement = generated_placement(arbitrary);
                    placement.domain = domain.clone();
                    let placement_key = namespace
                        .key(&placement)
                        .assured("a generated placement stays inside the key bound");
                    keys.insert(stored_key(arbitrary, placement_key), prefix.clone());
                }
                let visit = Visit {
                    namespace,
                    domain: domain.clone(),
                };
                namespaces.insert(prefix, visit);
            }
        }
        Self { keys, namespaces }
    }
}

/// Keys of up to three domains, each in up to three namespaces holding up to three placements'
/// headers, index entries or chunks, name the domain of their namespace after it. A walk from the
/// start visits every namespace of every domain exactly once, in key order: every key between
/// where it resumed and where the namespace ends lies in that namespace, and no key lies past the
/// last.
#[test]
fn bolero_staged_namespaces_name_their_domain_and_are_each_visited_once_in_key_order() {
    bolero::check!()
        .with_iterations(64)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let StagedKeys { keys, namespaces } = StagedKeys::generated(&mut arbitrary);
            for (key, prefix) in &keys {
                let visit = namespaces
                    .get(prefix)
                    .assured("every generated key's namespace is recorded with it");
                let (decoded, tail) =
                    physical_namespace(key).assured("a current key names its namespace");
                assert_eq!(decoded, visit.namespace);
                let named = RuntimeStateStore::restore_key_domain(tail)
                    .assured("a current key names its domain after its namespace");
                assert_eq!(named, visit.domain);
            }

            let stored = StoredKeys::new(keys.keys());
            let view = stored.database.snapshot();
            let mut cursor = Vec::new();
            let mut visits = Vec::new();
            while let Some((namespace, domain, next)) =
                next_namespace(&view, &stored.keyspace, &cursor)
                    .assured("a walk over current keys decodes every namespace")
            {
                assert_eq!(next, namespace_end(namespace, &domain));
                let prefix = namespace.prefix(&domain);
                let mut held = 0_usize;
                for (key, key_prefix) in keys.range(cursor.clone()..next.clone()) {
                    assert_eq!(
                        key_prefix, &prefix,
                        "{key:?} lies before the end of a namespace that does not hold it"
                    );
                    held += 1;
                }
                assert!(held > 0, "a visited namespace holds a key");
                assert!(next > cursor);
                visits.push(Visit { namespace, domain });
                cursor = next;
            }
            assert!(keys.range(cursor..).next().is_none());
            let expected = namespaces.into_values().collect::<Vec<_>>();
            assert_eq!(visits, expected);
        });
}

/// Up to three keys a store keeps, or arbitrary bytes, each damaged up to three times.
fn damaged_keys(arbitrary: &mut Arbitrary<'_>) -> BTreeSet<Vec<u8>> {
    let mut keys = BTreeSet::new();
    let count = arbitrary
        .entropy()
        .positive_count(nonzero_ext::nonzero!(3_usize));
    for _ in 0..count {
        let mut key = if arbitrary.entropy().flag() {
            let placement_key = generated_placement_key(arbitrary);
            stored_key(arbitrary, placement_key)
        } else {
            arbitrary_key(arbitrary, 64)
        };
        damage_repeatedly(arbitrary, &mut key);
        // The storage engine admits no empty key, and no store writes one.
        if !key.is_empty() {
            keys.insert(key);
        }
    }
    keys
}

/// Damaged or arbitrary keys either fail the domain decoder with the typed storage-format failure
/// or name a domain whose canonical text and separator open what they hold. A walk over such keys
/// stops with the typed storage-format failure at the first key whose namespace does not decode,
/// and otherwise visits the namespace and domain that key opens with, spelled exactly as the key
/// spells them, resuming where that namespace ends.
#[test]
fn bolero_malformed_staged_keys_fail_typed_or_name_their_namespace_canonically() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let keys = damaged_keys(&mut arbitrary);

            for key in &keys {
                // What follows a namespace that decodes, or the whole key when none does.
                let tail = match physical_namespace(key) {
                    Ok((_, tail)) => tail,
                    Err(_) => key.as_slice(),
                };
                match RuntimeStateStore::restore_key_domain(tail) {
                    Ok(domain) => {
                        let mut spelled = domain.as_str().as_bytes().to_vec();
                        spelled.push(0);
                        assert!(
                            tail.starts_with(&spelled),
                            "{tail:?} does not open with {domain}"
                        );
                    }
                    Err(report) => assert!(
                        matches!(
                            report.current_context(),
                            RuntimePersistenceError::InvalidStorageFormat
                        ),
                        "{report:?}"
                    ),
                }
            }

            let stored = StoredKeys::new(&keys);
            let view = stored.database.snapshot();
            let mut cursor = Vec::new();
            loop {
                let walked = next_namespace(&view, &stored.keyspace, &cursor);
                let Some(first) = keys.range(cursor.clone()..).next() else {
                    let walked = walked.assured("a walk past every key reads nothing");
                    assert!(walked.is_none());
                    break;
                };
                let Ok((namespace, tail)) = physical_namespace(first) else {
                    let report = walked
                        .err()
                        .assured("a walk refuses a key whose namespace does not decode");
                    assert!(
                        matches!(
                            report.current_context(),
                            RuntimePersistenceError::InvalidStorageFormat
                        ),
                        "{report:?}"
                    );
                    break;
                };
                let domain = RuntimeStateStore::restore_key_domain(tail)
                    .assured("a decoded namespace is followed by the domain its key opens with");
                let (walked_namespace, walked_domain, next) = walked
                    .assured("a walk reads a key whose namespace decodes")
                    .assured("a walk finds the stored key at or after its cursor");
                assert_eq!(walked_namespace, namespace);
                assert_eq!(walked_domain, domain);
                assert!(first.starts_with(&namespace.prefix(&domain)));
                assert_eq!(next, namespace_end(namespace, &domain));
                assert!(first < &next);
                cursor = next;
            }
        });
}
