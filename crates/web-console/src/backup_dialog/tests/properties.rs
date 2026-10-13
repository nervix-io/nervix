//! Bolero properties of what the backup dialog keeps between the operator and the server: the
//! pending backup a reload reads back from the tab's session storage, and the restore form's draft
//! of the `RESTORE` Model it edits.
//!
//! A pending backup is recorded as one entry per value, each in its canonical text, and reads back
//! as exactly the backup recorded last, whatever its reference, query text and domain. Entries the
//! console did not write read back as no backup, and the record they belong to is forgotten while
//! the tab's other entries stay. A draft filled from a `RESTORE` lowers back to that `RESTORE`,
//! and every existing-user policy the form offers reads back from the text its option carries.

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain, Entropy};
use nervix_models::{
    CommandExecutionReference, DomainName, ExistingUserPolicy, Restore, RestoreMode, RestoreScope,
};

use crate::backup_dialog::{
    browser::{RecordStorage, in_memory::MemoryStorage},
    pending_backup::{PendingBackup, RecordedEntries},
    restore_draft::RestoreDraft,
};

const REFERENCE_KEY: &str = "nervix.console.backup.reference";
const QUERY_KEY: &str = "nervix.console.backup.query";
const DOMAIN_KEY: &str = "nervix.console.backup.domain";
/// An entry of the tab's session storage that belongs to no pending backup.
const UNRELATED_KEY: &str = "nervix.console.unrelated";

/// The longest text drawn for an entry: two characters past the longest reference or name.
const DRAWN_ENTRY_CHARACTERS: u64 = 130;

/// Characters an entry the console did not write is drawn from: those both a reference and a
/// domain hold, the `~` only a domain holds, the dot and the upper-case letters only a reference
/// holds, and characters neither holds.
const ENTRY_CHARACTERS: [char; 16] = [
    'a', 'z', '0', '9', '-', '_', '~', '.', 'A', 'Z', ' ', '/', ':', 'é', '🎉', '\n',
];

/// Every policy the restore form offers for an archived user the cluster already has.
const EXISTING_USER_POLICIES: [ExistingUserPolicy; 3] = [
    ExistingUserPolicy::Fail,
    ExistingUserPolicy::Skip,
    ExistingUserPolicy::Replace,
];

/// A backup a tab may have sent: under any execution reference, with any query text, and with a
/// domain of any spelling the name rule admits, or with none.
fn pending_backup(arbitrary: &mut Arbitrary<'_>) -> PendingBackup {
    let reference = arbitrary.execution_reference();
    let query = arbitrary.string();
    let domain = if arbitrary.entropy().flag() {
        Some(arbitrary.rule_name::<DomainName>())
    } else {
        None
    };
    PendingBackup {
        reference,
        query,
        domain,
    }
}

/// Every entry that records `pending`, by key. A backup sent without a domain has no domain entry.
fn recorded_entries(pending: &PendingBackup) -> BTreeMap<String, String> {
    let mut entries = BTreeMap::new();
    entries.insert(
        REFERENCE_KEY.to_string(),
        pending.reference.as_str().to_string(),
    );
    entries.insert(QUERY_KEY.to_string(), pending.query.clone());
    if let Some(domain) = &pending.domain {
        entries.insert(DOMAIN_KEY.to_string(), domain.as_str().to_string());
    }
    entries
}

#[test]
fn bolero_pending_backups_survive_a_reload() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let earlier = pending_backup(&mut arbitrary);
            let pending = pending_backup(&mut arbitrary);
            let overwrites_earlier = arbitrary.entropy().flag();

            assert_eq!(
                PendingBackup::of_entries(pending.entries()),
                Some(pending.clone())
            );

            let storage = MemoryStorage::default();
            if overwrites_earlier {
                earlier.record(&storage);
            }
            pending.record(&storage);
            // Each value has an entry of its own, and an earlier backup's domain entry is gone
            // when this backup has no domain.
            assert_eq!(storage.entries(), recorded_entries(&pending));
            // A reload reads the backup back, and the record stays until the backup ends.
            assert_eq!(PendingBackup::recorded(&storage), Some(pending.clone()));
            assert_eq!(storage.entries(), recorded_entries(&pending));

            // An earlier backup's end forgets the record only when it carries the same reference.
            PendingBackup::forget(&storage, &earlier.reference);
            if earlier.reference == pending.reference {
                assert!(storage.entries().is_empty());
            } else {
                assert_eq!(PendingBackup::recorded(&storage), Some(pending.clone()));
                PendingBackup::forget(&storage, &pending.reference);
                assert!(storage.entries().is_empty());
            }
            assert_eq!(PendingBackup::recorded(&storage), None);
        });
}

/// `text` with one character inserted, replaced or removed, the character drawn from
/// [`ENTRY_CHARACTERS`]. An edit at the end of the text inserts.
fn edited_text(entropy: &mut Entropy<'_>, text: &str) -> String {
    let mut characters: Vec<char> = text.chars().collect();
    let length = u64::try_from(characters.len()).assured("an entry's length fits in u64");
    let drawn = entropy.up_to(length);
    let position = usize::try_from(drawn).verified("the position is at most the entry's length");
    let character = entropy.pick(ENTRY_CHARACTERS);
    let edit = entropy.byte() % 3;
    if edit == 0 || position == characters.len() {
        characters.insert(position, character);
    } else if edit == 1 {
        let replaced = characters
            .get_mut(position)
            .verified("the position is below the entry's length");
        *replaced = character;
    } else {
        characters.remove(position);
    }
    characters.into_iter().collect()
}

/// Up to [`DRAWN_ENTRY_CHARACTERS`] characters drawn from [`ENTRY_CHARACTERS`].
fn drawn_text(entropy: &mut Entropy<'_>) -> String {
    let length = entropy.boundary_biased(0..=DRAWN_ENTRY_CHARACTERS);
    let mut text = String::new();
    for _ in 0..length {
        text.push(entropy.pick(ENTRY_CHARACTERS));
    }
    text
}

/// What the tab's session storage may hold under one key of the record whose canonical text is
/// `canonical`: no entry, that text, that text edited once or in upper case, or drawn text.
fn stored_entry(entropy: &mut Entropy<'_>, canonical: &str) -> Option<String> {
    match entropy.byte() % 6 {
        0 => None,
        1 | 2 => Some(canonical.to_string()),
        3 => Some(edited_text(entropy, canonical)),
        4 => Some(canonical.to_ascii_uppercase()),
        _ => Some(drawn_text(entropy)),
    }
}

/// The backup whose record is exactly `entries`, if there is one: its reference and its domain
/// read back from their entries as values whose own text the entries hold.
fn backup_recorded_as(entries: &RecordedEntries) -> Option<PendingBackup> {
    let Ok(reference) = CommandExecutionReference::parse(entries.reference.clone()) else {
        return None;
    };
    if reference.as_str() != entries.reference {
        return None;
    }
    let domain = match &entries.domain {
        Some(text) => {
            let Ok(domain) = DomainName::parse(text) else {
                return None;
            };
            if domain.as_str() != text {
                return None;
            }
            Some(domain)
        }
        None => None,
    };
    Some(PendingBackup {
        reference,
        query: entries.query.clone(),
        domain,
    })
}

/// What a tab's session storage may hold where the console records a pending backup: under each
/// of the record's keys an entry or none, and an entry under a key of no record.
#[derive(Debug)]
struct StoredRecord {
    reference: Option<String>,
    query: Option<String>,
    domain: Option<String>,
    unrelated: String,
}

impl StoredRecord {
    /// Entries around a reference and a domain the console could have written, each kept,
    /// altered, drawn anew or missing. Any query text is one the console could have written.
    fn generate(arbitrary: &mut Arbitrary<'_>) -> Self {
        let written_reference = arbitrary.execution_reference();
        let written_domain = arbitrary.rule_name::<DomainName>();
        let reference = stored_entry(arbitrary.entropy(), written_reference.as_str());
        let query = if arbitrary.entropy().byte() % 8 == 0 {
            None
        } else {
            Some(arbitrary.string())
        };
        let domain = stored_entry(arbitrary.entropy(), written_domain.as_str());
        let unrelated = arbitrary.string();
        Self {
            reference,
            query,
            domain,
            unrelated,
        }
    }

    /// A tab's session storage holding these entries.
    fn storage(&self) -> MemoryStorage {
        let storage = MemoryStorage::default();
        storage.set(UNRELATED_KEY, &self.unrelated);
        if let Some(text) = &self.reference {
            storage.set(REFERENCE_KEY, text);
        }
        if let Some(text) = &self.query {
            storage.set(QUERY_KEY, text);
        }
        if let Some(text) = &self.domain {
            storage.set(DOMAIN_KEY, text);
        }
        storage
    }
}

#[test]
fn bolero_recorded_entries_read_back_exactly_or_are_forgotten() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let record = StoredRecord::generate(&mut arbitrary);
            let storage = record.storage();
            let stored = storage.entries();
            let mut forgotten = BTreeMap::new();
            forgotten.insert(UNRELATED_KEY.to_string(), record.unrelated);

            let read = PendingBackup::recorded(&storage);

            // Without a reference there is no record, so a reload resumes and forgets nothing.
            let Some(reference) = record.reference else {
                assert_eq!(read, None);
                assert_eq!(storage.entries(), stored);
                return;
            };
            // A record without its query cannot be sent again, so a reload forgets it.
            let Some(query) = record.query else {
                assert_eq!(read, None);
                assert_eq!(storage.entries(), forgotten);
                return;
            };
            let entries = RecordedEntries {
                reference,
                query,
                domain: record.domain,
            };
            let expected = backup_recorded_as(&entries);
            assert_eq!(PendingBackup::of_entries(entries.clone()), expected);
            assert_eq!(read, expected);
            match expected {
                Some(pending) => {
                    // Nothing is read in another spelling than the one recorded.
                    assert_eq!(pending.entries(), entries);
                    assert_eq!(storage.entries(), stored);
                }
                None => assert_eq!(storage.entries(), forgotten),
            }
        });
}

/// A `RESTORE` the restore form may be filled from, in either mode. Half the time a domain
/// restore names its domains in any spelling the name rule admits, rather than as NSPL writes a
/// name bare.
fn filled_restore(arbitrary: &mut Arbitrary<'_>) -> Restore {
    let mode = arbitrary
        .entropy()
        .pick([RestoreMode::Apply, RestoreMode::DryRun]);
    let restore = arbitrary.restore_of(mode);
    let RestoreScope::Domain { target, .. } = &restore.scope else {
        return restore;
    };
    let renamed = target.is_some();
    let respelled = arbitrary.entropy().flag();
    if !respelled {
        return restore;
    }
    let domain = arbitrary.rule_name::<DomainName>();
    let target = if renamed {
        Some(arbitrary.rule_name::<DomainName>())
    } else {
        None
    };
    Restore {
        scope: RestoreScope::Domain { domain, target },
        ..restore
    }
}

#[test]
fn bolero_restore_drafts_lower_to_the_restore_they_were_filled_from() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let restore = filled_restore(&mut arbitrary);

            let draft = RestoreDraft::of_model(&restore);
            let lowered = draft.model(Some(&restore.source), restore.mode);
            match lowered {
                Ok(lowered) => assert_eq!(lowered, restore),
                Err(report) => panic!("the draft of {restore:?} does not lower: {report:?}"),
            }

            // The form shows a policy as the option that carries its text, and reads the text of
            // the option the operator chose back as that policy.
            for policy in EXISTING_USER_POLICIES {
                assert_eq!(
                    policy.as_ref().parse::<ExistingUserPolicy>().ok(),
                    Some(policy)
                );
            }
        });
}
