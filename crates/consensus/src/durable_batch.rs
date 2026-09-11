//! Bounded atomic consensus writes and their full synchronization barrier.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Encoding limits and the single data-and-metadata durable commit policy.
//! - **Depends on.** Fjall, serialization, and an admitted execution reservation.
//! - **Must not know.** Raft scheduling, observers, or the meaning of a stored record.

use std::io::{self, Write};

use fjall::{Database, Keyspace, PersistMode};
use nervix_execution::Reservation;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageFailure {
    #[error("consensus storage operation exceeds its admitted byte budget")]
    Capacity,
    #[error("consensus storage stopped after a failed write; restart the node to recover")]
    Stopped,
    #[error("invalid consensus record storage; recreate the node's stored state")]
    InvalidState,
    #[error("the snapshot generation this transfer was reading has been superseded")]
    SnapshotSuperseded,
    #[error("failed to encode consensus record: {0}")]
    Encode(#[source] ciborium::ser::Error<io::Error>),
    #[error("consensus database write failed: {0}")]
    Write(#[source] fjall::Error),
}

pub(crate) enum Mutation {
    Put {
        keyspace: Keyspace,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        keyspace: Keyspace,
        key: Vec<u8>,
    },
}

impl Mutation {
    fn write(self, batch: &mut fjall::OwnedWriteBatch) {
        match self {
            Self::Put {
                keyspace,
                key,
                value,
            } => batch.insert(&keyspace, key, value),
            Self::Delete { keyspace, key } => batch.remove(&keyspace, key),
        }
    }
}

/// The backend accepts the complete atomic write and its required barrier together. Only the
/// batch owner selects the mode; tests can discard a backend's unsynchronized writes.
pub(crate) trait CommitBackend {
    fn commit(&self, mutations: Vec<Mutation>, mode: PersistMode) -> io::Result<()>;
}

impl CommitBackend for Database {
    fn commit(&self, mutations: Vec<Mutation>, mode: PersistMode) -> io::Result<()> {
        let mut batch = self.batch().durability(Some(mode));
        for mutation in mutations {
            mutation.write(&mut batch);
        }
        batch
            .commit()
            .map_err(|error| io::Error::other(StorageFailure::Write(error)))
    }
}

pub(crate) struct DurableBatch<'a> {
    mutations: Vec<Mutation>,
    used: usize,
    limit: usize,
    // The job retains the charge across encoding, the database write and fsync.
    _reservation: &'a Reservation,
}

impl<'a> DurableBatch<'a> {
    pub(crate) fn new(reservation: &'a Reservation) -> io::Result<Self> {
        // Reserve the other half for Fjall's journal encoding and the moving batch descriptors.
        let limit = usize::try_from(reservation.bytes() / 2)
            .map_err(|_| io::Error::other(StorageFailure::Capacity))?;
        Ok(Self {
            mutations: Vec::new(),
            used: 0,
            limit,
            _reservation: reservation,
        })
    }

    pub(crate) fn insert<T: Serialize>(
        &mut self,
        keyspace: &Keyspace,
        key: &[u8],
        value: &T,
    ) -> io::Result<()> {
        self.insert_measured(keyspace, key, value)?;
        Ok(())
    }

    /// Insert one record and report how many bytes it encoded to.
    pub(crate) fn insert_measured<T: Serialize>(
        &mut self,
        keyspace: &Keyspace,
        key: &[u8],
        value: &T,
    ) -> io::Result<u64> {
        self.charge_key(key)?;
        let mut writer = RecordWriter {
            bytes: Vec::new(),
            limit: self.remaining(),
        };
        ciborium::ser::into_writer(value, &mut writer)
            .map_err(|error| io::Error::other(StorageFailure::Encode(error)))?;
        self.charge(writer.bytes.len())?;
        writer.bytes.shrink_to_fit();
        let encoded = u64::try_from(writer.bytes.len()).map_err(io::Error::other)?;
        self.mutations.push(Mutation::Put {
            keyspace: keyspace.clone(),
            key: key.to_vec(),
            value: writer.bytes,
        });
        Ok(encoded)
    }

    /// Write a value that is already encoded, such as one restored from a snapshot section.
    pub(crate) fn insert_encoded(
        &mut self,
        keyspace: &Keyspace,
        key: &[u8],
        value: Vec<u8>,
    ) -> io::Result<()> {
        self.charge_key(key)?;
        self.charge(value.len())?;
        self.mutations.push(Mutation::Put {
            keyspace: keyspace.clone(),
            key: key.to_vec(),
            value,
        });
        Ok(())
    }

    pub(crate) fn remove(&mut self, keyspace: &Keyspace, key: &[u8]) -> io::Result<()> {
        self.charge_key(key)?;
        self.mutations.push(Mutation::Delete {
            keyspace: keyspace.clone(),
            key: key.to_vec(),
        });
        Ok(())
    }

    pub(crate) fn encode<T: Serialize>(value: &T, limit: u64) -> io::Result<Vec<u8>> {
        let limit =
            usize::try_from(limit).map_err(|_| io::Error::other(StorageFailure::Capacity))?;
        let mut writer = RecordWriter {
            bytes: Vec::new(),
            limit,
        };
        ciborium::ser::into_writer(value, &mut writer)
            .map_err(|error| io::Error::other(StorageFailure::Encode(error)))?;
        writer.bytes.shrink_to_fit();
        Ok(writer.bytes)
    }

    pub(crate) fn commit(self, backend: &impl CommitBackend) -> io::Result<()> {
        backend.commit(self.mutations, PersistMode::SyncAll)
    }

    fn charge_key(&mut self, key: &[u8]) -> io::Result<()> {
        self.charge(std::mem::size_of::<Mutation>())?;
        self.charge(key.len())?;
        self.mutations
            .try_reserve_exact(1)
            .map_err(io::Error::other)
    }

    fn charge(&mut self, bytes: usize) -> io::Result<()> {
        let used = self
            .used
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other(StorageFailure::Capacity))?;
        if used > self.limit {
            return Err(io::Error::other(StorageFailure::Capacity));
        }
        self.used = used;
        Ok(())
    }

    fn remaining(&self) -> usize {
        use meticulous::OptionExt as _;
        self.limit
            .checked_sub(self.used)
            .verified("charge never admits bytes beyond the batch limit")
    }
}

struct RecordWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for RecordWriter {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let length = self
            .bytes
            .len()
            .checked_add(input.len())
            .ok_or_else(|| io::Error::other(StorageFailure::Capacity))?;
        if length > self.limit {
            return Err(io::Error::other(StorageFailure::Capacity));
        }
        if length > self.bytes.capacity() {
            let doubled = match self.bytes.capacity().checked_mul(2) {
                Some(capacity) => capacity,
                None => self.limit,
            };
            let capacity = doubled.max(64).max(length).min(self.limit);
            let additional = capacity
                .checked_sub(self.bytes.len())
                .ok_or_else(|| io::Error::other(StorageFailure::Capacity))?;
            self.bytes
                .try_reserve_exact(additional)
                .map_err(io::Error::other)?;
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
