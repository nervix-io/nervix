//! The backup a page of this browser tab sent and has not downloaded yet.
//!
//! The record lives in the tab's session storage, which outlives a reload of the page but not the
//! tab. It holds exactly what sending the backup again needs: the execution reference, the exact
//! query text and the domain it was sent with, each in its own entry and in its own canonical
//! text. Sending them again recovers the backup's outcome, and with it the summary its download
//! checks, instead of running the backup twice.
//!
//! The reference is written last and removed first, so a reload between two writes finds no
//! reference and resumes nothing, and never a reference with another backup's query.

use nervix_models::{CommandExecutionReference, DomainName};

use super::browser::RecordStorage;

const REFERENCE_KEY: &str = "nervix.console.backup.reference";
const QUERY_KEY: &str = "nervix.console.backup.query";
const DOMAIN_KEY: &str = "nervix.console.backup.domain";

/// A backup this tab sent whose archive it has not downloaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingBackup {
    pub(crate) reference: CommandExecutionReference,
    /// The query text the backup was sent with, which a repetition sends unchanged.
    pub(crate) query: String,
    /// The domain the backup was sent with.
    pub(crate) domain: Option<DomainName>,
}

/// The session storage entries a pending backup is recorded as. A backup sent without a domain
/// has no domain entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordedEntries {
    pub(crate) reference: String,
    pub(crate) query: String,
    pub(crate) domain: Option<String>,
}

impl PendingBackup {
    /// The entries the backup is recorded as, each value in its canonical text.
    pub(crate) fn entries(&self) -> RecordedEntries {
        let domain = self
            .domain
            .as_ref()
            .map(|domain| domain.as_str().to_string());
        RecordedEntries {
            reference: self.reference.as_str().to_string(),
            query: self.query.clone(),
            domain,
        }
    }

    /// The backup recorded as `entries`, or `None` when an entry does not read back as the value
    /// it records.
    pub(crate) fn of_entries(entries: RecordedEntries) -> Option<Self> {
        let Ok(reference) = CommandExecutionReference::parse(entries.reference) else {
            return None;
        };
        let domain = match entries.domain {
            Some(text) => match DomainName::parse(&text) {
                Ok(domain) => Some(domain),
                Err(_) => return None,
            },
            None => None,
        };
        Some(Self {
            reference,
            query: entries.query,
            domain,
        })
    }

    /// Records the backup in `storage` before it is first sent.
    pub(crate) fn record(&self, storage: &dyn RecordStorage) {
        let entries = self.entries();
        storage.set(QUERY_KEY, &entries.query);
        match &entries.domain {
            Some(domain) => storage.set(DOMAIN_KEY, domain),
            None => storage.remove(DOMAIN_KEY),
        }
        storage.set(REFERENCE_KEY, &entries.reference);
    }

    /// The backup a previous page of this tab left pending in `storage`. A record that does not
    /// read back as one is forgotten.
    pub(crate) fn recorded(storage: &dyn RecordStorage) -> Option<Self> {
        let reference = storage.get(REFERENCE_KEY)?;
        let Some(query) = storage.get(QUERY_KEY) else {
            Self::forget_record(storage);
            return None;
        };
        let entries = RecordedEntries {
            reference,
            query,
            domain: storage.get(DOMAIN_KEY),
        };
        let Some(pending) = Self::of_entries(entries) else {
            Self::forget_record(storage);
            return None;
        };
        Some(pending)
    }

    /// Forgets the backup `reference` names, when it is still the one `storage` records: an
    /// earlier backup's end never forgets a later one.
    pub(crate) fn forget(storage: &dyn RecordStorage, reference: &CommandExecutionReference) {
        let recorded = storage.get(REFERENCE_KEY);
        if recorded.as_deref() != Some(reference.as_str()) {
            return;
        }
        Self::forget_record(storage);
    }

    /// Forgets whatever backup `storage` records, as when another identity takes over the console.
    pub(crate) fn forget_any(storage: &dyn RecordStorage) {
        Self::forget_record(storage);
    }

    /// Removes the record, its reference first.
    fn forget_record(storage: &dyn RecordStorage) {
        storage.remove(REFERENCE_KEY);
        storage.remove(QUERY_KEY);
        storage.remove(DOMAIN_KEY);
    }
}
