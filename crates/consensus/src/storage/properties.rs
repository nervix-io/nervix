//! Generated current consensus state through the keyed records a node stores and recovers.
//!
//! Layer: test harness.
//! - **Owns.** Bounded generators of current state-machine revisions, and the properties that store
//!   them through the production batch and read them back the way recovery does.
//! - **Depends on.** The production record codec and keyspace layout, a temporary database, and
//!   the consensus and vocabulary generators.
//! - **Must not know.** Raft scheduling, transport, or how commands are applied.

use std::collections::BTreeMap;

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{DomainName, ResourceName, ResourceUploadKey};
use tempfile::TempDir;

use super::{generators::state_machine, *};

/// What storing one generated revision may charge its batch. A generated state holds a few
/// bounded records of each family, far below the half of this the batch may use.
const BATCH_RESERVATION_BYTES: u64 = 8 * 1024 * 1024;

/// A consensus state-machine keyspace in a database of its own, removed with its directory.
///
/// Fields drop in the order they are declared, so the directory goes last: a database whose files
/// vanish under it fails its background workers, and closing it can then wait for them forever.
struct StateKeyspace {
    database: Database,
    sm: Keyspace,
    executor: Executor,
    _directory: TempDir,
}

impl StateKeyspace {
    fn open() -> Self {
        let directory =
            tempfile::tempdir().assured("the test host provides a writable temporary directory");
        let database = Database::builder(directory.path())
            .open()
            .assured("a new temporary directory holds a new database");
        let sm = database
            .keyspace(KEYSPACE_STATE_MACHINE, KeyspaceCreateOptions::default)
            .assured("a new database creates the state-machine keyspace");
        Self {
            database,
            sm,
            executor: Executor::default(),
            _directory: directory,
        }
    }

    /// Stores what changed from `preceding` to `state` in one batch, as applying a committed range
    /// does.
    fn store(&self, preceding: &StateMachineData, state: &StateMachineData) -> io::Result<()> {
        let reservation = self
            .executor
            .try_reserve(MemoryClass::Commands, BATCH_RESERVATION_BYTES)
            .map_err(|report| io::Error::other(report.to_string()))?;
        let mut batch = DurableBatch::new(&reservation)?;
        state.write_changes(preceding, &mut batch, &self.sm)?;
        batch.commit(&self.database)
    }

    /// The state the stored records describe, read the way recovery reads them.
    fn recover(&self) -> io::Result<StateMachineData> {
        let metadata = StateMetadata::read(&self.sm)?
            .ok_or_else(|| io::Error::other(StorageFailure::InvalidState))?;
        StateMachineData::load(&self.sm, metadata)
    }
}

/// A generated revision stored into an empty keyspace recovers equal to itself, and a second
/// revision stored as the changes from the first, replacing and removing records, recovers equal
/// to the second.
#[test]
fn bolero_consensus_state_records_recover_every_stored_value() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let first = state_machine(&mut arbitrary);
            let second = state_machine(&mut arbitrary);
            let keyspace = StateKeyspace::open();

            keyspace
                .store(&StateMachineData::default(), &first)
                .assured("a bounded generated revision fits its batch reservation");
            let recovered = keyspace
                .recover()
                .assured("a revision the production batch stored recovers");
            assert_eq!(recovered, first);

            keyspace
                .store(&first, &second)
                .assured("the changes between two bounded revisions fit their batch reservation");
            let recovered = keyspace
                .recover()
                .assured("a revision stored as changes from its predecessor recovers");
            assert_eq!(recovered, second);
        });
}

/// The tag every keyed state-machine record family's keys begin with, in the order recovery loads
/// them.
const RECORD_TAGS: [u8; 23] = *b"sdaucvronfxItPphijyklqe";

impl StateKeyspace {
    /// Every record the state-machine keyspace holds, keyed by its raw key.
    fn records(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut records = BTreeMap::new();
        for item in self.sm.iter() {
            let (key, value) = item
                .into_inner()
                .assured("a temporary test keyspace reads back what it holds");
            records.insert(key.to_vec(), value.to_vec());
        }
        records
    }

    /// Replaces what the keyspace holds with `records`, byte for byte, as damaged storage would
    /// present them to recovery.
    fn overwrite(&self, records: &BTreeMap<Vec<u8>, Vec<u8>>) {
        for key in self.records().into_keys() {
            self.sm
                .remove(key)
                .assured("a temporary test keyspace removes a record it holds");
        }
        for (key, value) in records {
            self.sm
                .insert(key.clone(), value.clone())
                .assured("a temporary test keyspace stores any key and value");
        }
    }
}

/// Up to sixteen arbitrary bytes.
fn arbitrary_bytes(arbitrary: &mut Arbitrary<'_>) -> Vec<u8> {
    let count = arbitrary.entropy().count(16);
    let mut bytes = Vec::with_capacity(count);
    for _ in 0..count {
        bytes.push(arbitrary.entropy().byte());
    }
    bytes
}

/// Changes the raw records of a stored revision the way damaged storage can: a value's bits,
/// length or whole content; a key's tag or trailing bytes; one record's value copied under
/// another's key; an unknown record added; or the revision's metadata lost.
fn corrupt(arbitrary: &mut Arbitrary<'_>, records: &mut BTreeMap<Vec<u8>, Vec<u8>>) {
    let stored = records.clone().into_iter().collect::<Vec<_>>();
    let Some(count) = std::num::NonZeroUsize::new(stored.len()) else {
        return;
    };
    let (key, value) = stored[arbitrary.entropy().index(count)].clone();
    match arbitrary.entropy().byte() % 9 {
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
            extended.extend(arbitrary_bytes(arbitrary));
            records.insert(key, extended);
        }
        3 => {
            records.insert(key, arbitrary_bytes(arbitrary));
        }
        4 => {
            records.remove(&key);
            let mut extended = key;
            extended.extend(arbitrary_bytes(arbitrary));
            records.insert(extended, value);
        }
        5 => {
            records.remove(&key);
            let mut retagged = key;
            if let Some(tag) = retagged.first_mut() {
                *tag = arbitrary.entropy().pick(RECORD_TAGS);
            }
            records.insert(retagged, value);
        }
        6 => {
            let (_, copied) = stored[arbitrary.entropy().index(count)].clone();
            records.insert(key, copied);
        }
        7 => {
            let mut unknown = vec![arbitrary.entropy().byte()];
            unknown.extend(arbitrary_bytes(arbitrary));
            records.insert(unknown, value);
        }
        _ => {
            records.remove(KEY_METADATA);
        }
    }
}

/// Whether `error` is the documented failure of a consensus store holding malformed records.
fn is_invalid_storage(error: &io::Error) -> bool {
    let Some(inner) = error.get_ref() else {
        return false;
    };
    matches!(
        inner.downcast_ref::<StorageFailure>(),
        Some(StorageFailure::InvalidState)
    )
}

/// The facts the readers of a recovered state rely on without checking them again: every record
/// that names its own key names the key it is stored under, a schedule keys each entry by the node
/// it places, and every upload holds a version of its own.
fn assert_readers_can_rely_on(state: &StateMachineData) {
    for (domain, schedule) in state.schedule.domains.iter() {
        assert_eq!(&schedule.domain, domain);
        for (node, scheduled) in &schedule.nodes {
            assert_eq!(node, &scheduled.identity());
        }
    }
    for (domain, record) in state.domains.iter() {
        assert_eq!(&record.id, domain);
    }
    for (name, user) in state.users.iter() {
        assert_eq!(&user.name, name);
    }
    for (id, version) in state.resources.versions.iter() {
        assert_eq!(&version.id, id);
    }
    for (key, replica) in state.resources.replicas.iter() {
        assert_eq!(&replica.key, key);
    }
    for (key, upload) in state.resources.uploads.iter() {
        assert_eq!(&upload.key, key);
    }
    for (id, transaction) in state.transactions.iter() {
        assert_eq!(&transaction.id, id);
    }
    for execution in state.command_executions.executions() {
        let stored = state.command_executions.get(&execution.reference);
        assert_eq!(stored, Some(execution));
    }
    let uploads =
        nervix_models::ResourceUploads::try_from_uploads(state.resources.uploads.values().cloned());
    assert!(uploads.is_ok(), "{uploads:?}");
}

/// Damaged records of a stored revision either fail recovery with the documented typed failure,
/// or recover a state whose every stored key is the one the state itself would be stored under,
/// whose readers' assumptions hold, and which stores and recovers again unchanged.
#[test]
fn bolero_corrupt_consensus_state_records_fail_typed_or_recover_canonically() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let state = state_machine(&mut arbitrary);
            let keyspace = StateKeyspace::open();
            keyspace
                .store(&StateMachineData::default(), &state)
                .assured("a bounded generated revision fits its batch reservation");
            let mut records = keyspace.records();
            let corruptions = arbitrary.entropy().between(1..=3);
            for _ in 0..corruptions {
                corrupt(&mut arbitrary, &mut records);
            }
            keyspace.overwrite(&records);

            let recovered = match keyspace.recover() {
                Ok(recovered) => recovered,
                Err(error) => {
                    assert!(is_invalid_storage(&error), "{error:?}");
                    return;
                }
            };
            assert_readers_can_rely_on(&recovered);
            let canonical = StateKeyspace::open();
            canonical
                .store(&StateMachineData::default(), &recovered)
                .assured("a recovered bounded revision fits its batch reservation");
            assert_eq!(
                canonical.records().into_keys().collect::<Vec<_>>(),
                records.into_keys().collect::<Vec<_>>()
            );
            let reloaded = canonical
                .recover()
                .assured("a revision the production batch stored recovers");
            assert_eq!(reloaded, recovered);
        });
}

/// Recovers the records `state` stores after `damage` changed them.
fn recover_damaged(
    state: &StateMachineData,
    damage: impl FnOnce(&mut BTreeMap<Vec<u8>, Vec<u8>>),
) -> io::Result<StateMachineData> {
    let keyspace = StateKeyspace::open();
    keyspace
        .store(&StateMachineData::default(), state)
        .assured("a small fixed revision fits its batch reservation");
    let mut records = keyspace.records();
    damage(&mut records);
    keyspace.overwrite(&records);
    keyspace.recover()
}

/// The stored key of the one record `state` stores under `tag`.
fn only_key_under(records: &BTreeMap<Vec<u8>, Vec<u8>>, tag: u8) -> Vec<u8> {
    let mut keys = records.keys().filter(|key| key.first() == Some(&tag));
    let key = keys
        .next()
        .verified("the fixture stores one record under this tag");
    assert!(keys.next().is_none());
    key.clone()
}

fn fixture_name<N: std::str::FromStr<Err: std::fmt::Debug>>(text: &str) -> N {
    N::from_str(text).assured("the fixture uses names that follow the name rule")
}

fn fixture_domain(id: DomainName) -> nervix_models::DomainState {
    nervix_models::DomainState {
        id,
        config: nervix_models::DomainConfig {
            pace: nervix_models::DomainPace::Unpaced,
            placement: nervix_models::PlacementPolicy::Neutral,
        },
        status: nervix_models::DomainStatus::Stopped,
        start_version: 0,
        last_start: nervix_models::DomainStartPoint::Resume,
        clock: None,
    }
}

#[test]
fn recovery_refuses_a_record_whose_value_names_another_key() {
    let mut state = StateMachineData::default();
    state.domains.insert(
        fixture_name("stored"),
        fixture_domain(fixture_name("renamed")),
    );

    let error = recover_damaged(&state, |_| {}).expect_err("the record names another domain");

    assert!(is_invalid_storage(&error), "{error:?}");
}

#[test]
fn recovery_refuses_a_record_key_with_trailing_bytes() {
    let mut state = StateMachineData::default();
    let domain: DomainName = fixture_name("stored");
    state.domains.insert(domain.clone(), fixture_domain(domain));

    let error = recover_damaged(&state, |records| {
        let key = only_key_under(records, b'd');
        let value = records
            .remove(&key)
            .verified("the key was read from these records");
        let mut extended = key;
        extended.push(7);
        records.insert(extended, value);
    })
    .expect_err("the key holds bytes after the domain it encodes");

    assert!(is_invalid_storage(&error), "{error:?}");
}

#[test]
fn recovery_refuses_a_record_no_family_owns() {
    let state = StateMachineData::default();

    let error = recover_damaged(&state, |records| {
        records.insert(b"zz".to_vec(), b"unread".to_vec());
    })
    .expect_err("no record family is stored under this key");

    assert!(is_invalid_storage(&error), "{error:?}");
}

#[test]
fn recovery_refuses_two_uploads_assigned_one_version() {
    let mut state = StateMachineData::default();
    let domain: DomainName = fixture_name("stored");
    let resource: ResourceName = fixture_name("zip_codes");
    for identity in ["first-upload", "second-upload"] {
        let key = ResourceUploadKey::new(
            fixture_name("operator"),
            domain.clone(),
            resource.clone(),
            nervix_models::ResourceUploadIdentity::parse(identity)
                .assured("the fixture identity follows the identity rule"),
        );
        let upload = nervix_models::ResourceUpload {
            key: key.clone(),
            version: 1,
            state: nervix_models::ResourceUploadState::Applying {
                root_checksum: "digest".to_string(),
            },
        };
        state.resources.uploads.insert(key, upload);
    }

    let error = recover_damaged(&state, |_| {}).expect_err("both uploads hold version 1");

    assert!(is_invalid_storage(&error), "{error:?}");
}

#[test]
fn recovery_refuses_a_schedule_entry_keyed_by_another_node() {
    let mut state = StateMachineData::default();
    let domain: DomainName = fixture_name("stored");
    let schema = nervix_models::Model::Schema(nervix_models::CreateSchema {
        name: fixture_name("readings"),
        fields: Vec::new(),
    });
    let node = nervix_models::ScheduledNode::new(
        schema,
        nervix_models::SchemaFingerprint::from_digest([1; 32]),
    );
    let mut schedule = nervix_models::DomainSchedule::new(domain.clone(), [], Vec::new());
    schedule.nodes.insert(
        nervix_models::NodeRef::new(
            nervix_models::ModelKind::Relay,
            fixture_name::<nervix_models::ModelName>("readings"),
        ),
        node,
    );
    state.schedule.domains.insert(domain, schedule);

    let error = recover_damaged(&state, |_| {}).expect_err("a relay key holds a schema entry");

    assert!(is_invalid_storage(&error), "{error:?}");
}

#[test]
fn recovery_refuses_an_expired_execution_without_an_issue_time() {
    let mut state = StateMachineData::default();
    let reference = nervix_models::CommandExecutionReference::parse("plain-reference")
        .assured("the fixture reference follows the reference rule");
    let applying = crate::CommandExecution::applying(
        reference.clone(),
        fixture_name("operator"),
        None,
        [1; 32],
        nervix_models::Timestamp::from_unix_nanos(1),
        crate::CommandExecutionEffect::CreateUser {
            if_not_exists: false,
            name: fixture_name("operator"),
            password_hash: "hash".to_string(),
        },
    );
    state.command_executions.insert_admitted(
        applying,
        &crate::CommandExecutionAdmissionPolicy::at(
            nervix_models::Timestamp::from_unix_nanos(1),
            std::time::Duration::from_secs(60),
            1,
        ),
    );
    let expired = crate::CommandExecution {
        reference,
        state: crate::CommandExecutionState::Expired,
    };
    let expired = DurableBatch::encode(&expired, 64 * 1024)
        .assured("a small fixed execution fits the storage codec budget");

    let error = recover_damaged(&state, |records| {
        let key = only_key_under(records, b'e');
        records.insert(key, expired);
    })
    .expect_err("only a retry identity whose issue time reads back is ever expired");

    assert!(is_invalid_storage(&error), "{error:?}");
}
