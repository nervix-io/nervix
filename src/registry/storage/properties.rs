//! Stored Models through the registry's keyed model storage.
//!
//! Layer: test harness.
//! - **Owns.** Generated domain-owned Models committed through the registry's storage batch and
//!   listed back from memory or from on-disk tables after the database reopens, and damaged
//!   stored records listed through the same decoder.
//! - **Depends on.** The registry's model storage, a temporary database, and the vocabulary
//!   generators.
//! - **Must not know.** Registry graph validation, scheduling, or the runtime.

use std::collections::BTreeMap;

use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};

use super::*;
use crate::registry::mutation::RegistryPersistMutation;

/// Up to four domain-owned Models, keyed by the stored key each is written under. Two draws that
/// name the same domain, kind and name are one record, as they are in storage.
fn stored_records(arbitrary: &mut Arbitrary<'_>) -> BTreeMap<Vec<u8>, StoredModelRecord> {
    let mut records = BTreeMap::new();
    for _ in 0..arbitrary.entropy().count(4) {
        let domain = arbitrary.rule_name::<DomainName>();
        let model = arbitrary.pinned_model();
        let key = encode_key(&domain, model.kind(), model.name())
            .assured("every generated domain and Model name encodes as a key");
        records.insert(key, StoredModelRecord { domain, model });
    }
    records
}

fn open_storage(path: &Path) -> ModelStorage {
    let database = Database::builder(path)
        .open()
        .assured("a temporary test directory holds a database");
    ModelStorage::from_database(database).assured("a test database opens the models keyspace")
}

/// Commits `records` through the registry's batch, one batch per domain as a registry commit
/// writes them.
fn commit(storage: &ModelStorage, records: &BTreeMap<Vec<u8>, StoredModelRecord>) {
    let mut domains: BTreeMap<DomainName, HashMap<NodeRef, RegistryPersistMutation>> =
        BTreeMap::new();
    for record in records.values() {
        let models = domains.entry(record.domain.clone()).or_default();
        models.insert(
            record.model.node_ref(),
            RegistryPersistMutation::Create(record.model.clone()),
        );
    }
    for (domain, models) in &domains {
        storage
            .commit_batch(domain, models, &HashSet::default())
            .assured("a bounded batch of generated Models commits");
    }
}

/// Every record the models keyspace holds, keyed by its raw key.
fn raw_records(storage: &ModelStorage) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut records = BTreeMap::new();
    for item in storage.index.iter() {
        let (key, value) = item
            .into_inner()
            .assured("a temporary test keyspace reads back what it holds");
        records.insert(key.to_vec(), value.to_vec());
    }
    records
}

/// Committed Models list back complete and in key order, and each reads back by its own key,
/// whether the reopened database serves them from its journal or from the on-disk table a flush
/// moved them into.
#[test]
fn bolero_registry_stored_models_list_back_from_memory_and_tables() {
    bolero::check!()
        .with_iterations(64)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let records = stored_records(&mut arbitrary);
            let flushed = arbitrary.entropy().flag();
            let directory = tempfile::tempdir()
                .assured("the test host provides a writable temporary directory");
            {
                let storage = open_storage(directory.path());
                commit(&storage, &records);
                if flushed {
                    storage
                        .index
                        .rotate_memtable_and_wait()
                        .assured("a test keyspace flushes its memtable into an on-disk table");
                }
            }

            let storage = open_storage(directory.path());
            let listed = storage
                .list_records()
                .assured("Models the registry committed list back");
            let expected = records.values().cloned().collect::<Vec<_>>();
            assert_eq!(listed, expected);
            for record in records.values() {
                let node = record.model.node_ref();
                let model = storage
                    .get(&record.domain, node.kind, node.identifier)
                    .assured("a committed Model reads back by its own key");
                assert_eq!(model.as_ref(), Some(&record.model));
            }
        });
}

/// Changes the raw records of a stored registry the way damaged storage can: a value's bits,
/// length or whole content; a key's case or trailing bytes; one record's value copied under
/// another's key; or an unknown key added.
fn damage(arbitrary: &mut Arbitrary<'_>, records: &mut BTreeMap<Vec<u8>, Vec<u8>>) {
    let stored = records.clone().into_iter().collect::<Vec<_>>();
    let Some(count) = std::num::NonZeroUsize::new(stored.len()) else {
        records.insert(unknown_key(arbitrary), Vec::new());
        return;
    };
    let (key, value) = stored[arbitrary.entropy().index(count)].clone();
    match arbitrary.entropy().byte() % 7 {
        0 => {
            let mut damaged = value;
            if let Some(count) = std::num::NonZeroUsize::new(damaged.len()) {
                let byte = arbitrary.entropy().index(count);
                let bit = arbitrary.entropy().byte() % 8;
                damaged[byte] ^= 1 << bit;
            }
            records.insert(key, damaged);
        }
        1 => {
            let mut truncated = value;
            let length = arbitrary.entropy().count(truncated.len());
            truncated.truncate(length);
            records.insert(key, truncated);
        }
        2 => {
            let mut extended = value;
            for _ in 0..arbitrary.entropy().count(16) {
                extended.push(arbitrary.entropy().byte());
            }
            records.insert(key, extended);
        }
        3 => {
            records.remove(&key);
            let mut recased = key;
            if let Some(count) = std::num::NonZeroUsize::new(recased.len()) {
                let byte = arbitrary.entropy().index(count);
                recased[byte] ^= 0x20;
            }
            records.insert(recased, value);
        }
        4 => {
            records.remove(&key);
            let mut extended = key;
            extended.push(arbitrary.entropy().byte());
            records.insert(extended, value);
        }
        5 => {
            let (_, copied) = stored[arbitrary.entropy().index(count)].clone();
            records.insert(key, copied);
        }
        _ => {
            records.insert(unknown_key(arbitrary), value);
        }
    }
}

/// A key of one to sixteen arbitrary bytes. The database stores no empty key.
fn unknown_key(arbitrary: &mut Arbitrary<'_>) -> Vec<u8> {
    let mut key = vec![arbitrary.entropy().byte()];
    for _ in 0..arbitrary.entropy().count(15) {
        key.push(arbitrary.entropy().byte());
    }
    key
}

/// Damaged stored records either fail the listing with the registry's typed storage failure, or
/// list Models whose own keys are exactly the stored keys, so no stored key is read as another.
#[test]
fn bolero_corrupt_registry_records_fail_typed_or_list_canonically() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let records = stored_records(&mut arbitrary);
            let directory = tempfile::tempdir()
                .assured("the test host provides a writable temporary directory");
            let storage = open_storage(directory.path());
            commit(&storage, &records);
            let mut raw = raw_records(&storage);
            let damages = arbitrary.entropy().between(1..=3);
            for _ in 0..damages {
                damage(&mut arbitrary, &mut raw);
            }
            for key in raw_records(&storage).into_keys() {
                storage
                    .index
                    .remove(key)
                    .assured("a temporary test keyspace removes a record it holds");
            }
            for (key, value) in &raw {
                storage
                    .index
                    .insert(key.clone(), value.clone())
                    .assured("a temporary test keyspace stores any key and value");
            }

            for value in raw.values() {
                crate::archive_allocation_tests::assert_decode_frees_allocations(|| {
                    deserialize_value(value)
                });
            }

            let listed = match storage.list_records() {
                Ok(listed) => listed,
                Err(report) => {
                    assert!(
                        matches!(
                            report.current_context(),
                            RegistryError::DecodeKey
                                | RegistryError::InvalidModelArchive
                                | RegistryError::DeserializeValue
                                | RegistryError::StoredModelKindMismatch { .. }
                        ),
                        "{report:?}"
                    );
                    return;
                }
            };
            let mut keys = Vec::with_capacity(listed.len());
            for record in &listed {
                let key = encode_key(&record.domain, record.model.kind(), record.model.name())
                    .assured("a listed record's domain and Model name encode as a key");
                keys.push(key);
                let stored = serialize_value(&record.model)
                    .assured("a listed Model encodes through the registry owner");
                let restored = deserialize_value(&stored)
                    .assured("a listed Model decodes from its own encoding");
                assert_eq!(restored, record.model);
            }
            assert_eq!(keys, raw.into_keys().collect::<Vec<_>>());
        });
}

#[test]
fn a_model_archive_refusing_its_second_schema_field_frees_the_first() {
    let name =
        |value| nervix_models::FieldName::parse(value).assured("a fixture field name is valid");
    let model = Model::Schema(nervix_models::CreateSchema {
        name: nervix_models::SchemaName::parse("events")
            .assured("the fixture schema name is valid"),
        fields: vec![
            nervix_models::SchemaField {
                name: name("first_field"),
                ty: nervix_models::ParseAsType::Bytes,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: name("second_field"),
                ty: nervix_models::ParseAsType::Bytes,
                optional: false,
                sensitive: false,
            },
        ],
    });
    let mut encoded = serialize_value(&model).assured("the current model archives");
    let target = b"second_field";
    let occurrences = encoded
        .windows(target.len())
        .filter(|window| *window == target)
        .count();
    assert_eq!(
        occurrences, 1,
        "the target field occurs once in the archive"
    );
    let start = encoded
        .windows(target.len())
        .position(|window| window == target)
        .assured("the target field is archived");
    encoded[start + 6] = b'!';

    let archive = encoded
        .strip_prefix(MODEL_ARCHIVE_HEADER)
        .assured("the archive keeps its header");
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(archive.len());
    aligned.extend_from_slice(archive);
    rkyv::access::<rkyv::Archived<Model>, rkyv::rancor::Error>(&aligned)
        .assured("the changed field leaves a valid archive shape");

    crate::archive_allocation_tests::assert_decode_frees_allocations(|| {
        let result = deserialize_value(&encoded);
        assert!(matches!(
            result.as_ref().map_err(|error| error.current_context()),
            Err(RegistryError::DeserializeValue)
        ));
        result
    });
}

/// Lists a store holding one schema record of domain `readings` named `events`, after `rewrite`
/// changed the record's key the way damaged storage can.
fn listed_after_rewriting_the_key(
    rewrite: impl FnOnce(&mut Vec<u8>),
) -> Result<Vec<StoredModelRecord>, Report<RegistryError>> {
    let directory =
        tempfile::tempdir().assured("the test host provides a writable temporary directory");
    let storage = open_storage(directory.path());
    let domain = DomainName::parse("readings").assured("the fixture domain follows the name rule");
    let model = Model::Schema(nervix_models::CreateSchema {
        name: nervix_models::SchemaName::parse("events")
            .assured("the fixture schema name follows the name rule"),
        fields: Vec::new(),
    });
    let mut key = encode_key(&domain, model.kind(), model.name())
        .assured("the fixture domain and name encode as a key");
    rewrite(&mut key);
    let value = serialize_value(&model).assured("the fixture schema encodes");
    storage
        .index
        .insert(key, value)
        .assured("a temporary test keyspace stores any key and value");
    storage.list_records()
}

/// Replaces the first occurrence of `from` in `key` with `to`, which has the same length.
fn replace_in_key(key: &mut [u8], from: &[u8], to: &[u8]) {
    let start = key
        .windows(from.len())
        .position(|window| window == from)
        .assured("the fixture key spells the replaced text");
    key[start..start + to.len()].copy_from_slice(to);
}

#[test]
fn a_stored_key_spelling_a_model_name_in_upper_case_is_refused() {
    let listed = listed_after_rewriting_the_key(|key| replace_in_key(key, b"events", b"EVENTS"));

    let report = listed.expect_err("no commit writes a Model name in upper case");
    assert!(matches!(report.current_context(), RegistryError::DecodeKey));
}

#[test]
fn a_stored_key_spelling_a_domain_in_upper_case_is_refused() {
    let listed =
        listed_after_rewriting_the_key(|key| replace_in_key(key, b"readings", b"READINGS"));

    let report = listed.expect_err("no commit writes a domain in upper case");
    assert!(matches!(report.current_context(), RegistryError::DecodeKey));
}

#[test]
fn a_stored_key_with_trailing_bytes_is_refused() {
    let listed = listed_after_rewriting_the_key(|key| key.push(1));

    let report = listed.expect_err("the key holds bytes after the Model it encodes");
    assert!(matches!(report.current_context(), RegistryError::DecodeKey));
}
