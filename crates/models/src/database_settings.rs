//! Local database capacity declarations shared by their node and consensus owners.
//!
//! Layer: vocabulary.
//! - **Owns.** Cache, journal, worker and open-file setting classes, the fixed keyspace memtable
//!   size, names, defaults and validated resolved bounds.
//! - **Depends on.** Setting vocabulary and byte/count quantities.
//! - **Must not know.** Fjall, filesystems, threads or application startup.
#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns database capacity vocabulary")
)]
use std::{marker::PhantomData, num::NonZeroU64};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ubyte::ByteUnit;

use crate::{NodeCapabilities, Setting, SettingDefinition};

/// The memtable size every keyspace of the node and consensus databases opens with. A memtable is
/// the in-memory buffer that collects a keyspace's new writes: it starts empty and grows with each
/// write until it reaches this size, when the engine seals it and writes it to disk as a table.
/// Nervix caps neither one memtable's growth beyond it nor the memtables of a database together,
/// and the startup memory check does not count them.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the storage engine records a keyspace's memtable size when it creates the \
                  keyspace and reuses the recorded size on every later open, so the size belongs \
                  to the stored layout rather than to one run's capabilities"
    )
)]
pub const KEYSPACE_MEMTABLE_BYTES: u64 = 64 * 1024 * 1024;

/// The fewest table files a database may keep open.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the storage engine documents ten as the smallest open-file limit its database \
                  builder accepts"
    )
)]
const MIN_OPEN_FILES: u64 = 10;

pub trait DatabaseNamespace: std::fmt::Debug + Clone + Copy + PartialEq + Eq {
    const CACHE: &'static str;
    const JOURNAL: &'static str;
    const WORKERS: &'static str;
    const MAX_OPEN_FILES: &'static str;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeDatabase;
impl DatabaseNamespace for NodeDatabase {
    const CACHE: &'static str = "database-cache-bytes";
    const JOURNAL: &'static str = "database-journal-bytes";
    const WORKERS: &'static str = "database-workers";
    const MAX_OPEN_FILES: &'static str = "database-max-open-files";
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusDatabase;
impl DatabaseNamespace for ConsensusDatabase {
    const CACHE: &'static str = "raft-database-cache-bytes";
    const JOURNAL: &'static str = "raft-database-journal-bytes";
    const WORKERS: &'static str = "raft-database-workers";
    const MAX_OPEN_FILES: &'static str = "raft-database-max-open-files";
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("{setting} is outside the database engine's supported capacity")]
pub struct DatabaseSettingsError {
    pub setting: &'static str,
}

pub struct DatabaseCacheBytes<N>(PhantomData<N>);
impl<N: DatabaseNamespace> SettingDefinition for DatabaseCacheBytes<N> {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = DatabaseSettingsError;
    const NAME: &'static str = N::CACHE;
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Mebibyte(32)
    }
    fn validate(value: &ByteUnit, _: &()) -> Result<(), Report<Self::Error>> {
        if value.as_u64() == 0 || usize::try_from(value.as_u64()).is_err() {
            return Err(Report::new(DatabaseSettingsError {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
pub struct DatabaseJournalBytes<N>(PhantomData<N>);
impl<N: DatabaseNamespace> SettingDefinition for DatabaseJournalBytes<N> {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = DatabaseSettingsError;
    const NAME: &'static str = N::JOURNAL;
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Mebibyte(512)
    }
    fn validate(value: &ByteUnit, _: &()) -> Result<(), Report<Self::Error>> {
        // Fjall's builder requires a journal capacity of at least 64 MiB.
        if value.as_u64() < ByteUnit::Mebibyte(64).as_u64() {
            return Err(Report::new(DatabaseSettingsError {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
pub struct DatabaseWorkers<N>(PhantomData<N>);
impl<N: DatabaseNamespace> SettingDefinition for DatabaseWorkers<N> {
    type Class = NodeCapabilities;
    type Value = Option<usize>;
    type Context = ();
    type Error = DatabaseSettingsError;
    const NAME: &'static str = N::WORKERS;
    const DERIVATION: Option<&'static str> =
        Some("available CPU count capped at four; one when detection is unknown");
    fn default_value(_: &()) -> Option<usize> {
        None
    }
    fn validate(value: &Option<usize>, _: &()) -> Result<(), Report<Self::Error>> {
        if value == &Some(0) {
            return Err(Report::new(DatabaseSettingsError {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
/// How many table files a database keeps open at once. When it needs another, it closes the one it
/// used least recently, and a later read opens that file again.
pub struct DatabaseMaxOpenFiles<N>(PhantomData<N>);
impl<N: DatabaseNamespace> SettingDefinition for DatabaseMaxOpenFiles<N> {
    type Class = NodeCapabilities;
    type Value = NonZeroU64;
    type Context = ();
    type Error = DatabaseSettingsError;
    const NAME: &'static str = N::MAX_OPEN_FILES;
    const DERIVATION: Option<&'static str> =
        Some("by platform: 900, or 400 on Windows and 150 on macOS");
    fn default_value(_: &()) -> NonZeroU64 {
        // The count the storage engine picks by itself on each platform, so that no platform's
        // database keeps a different number of files open than before the count was declared.
        let files = if cfg!(target_os = "windows") {
            400
        } else if cfg!(target_os = "macos") {
            150
        } else {
            900
        };
        NonZeroU64::new(files).assured("every platform's default count is positive")
    }
    fn validate(value: &NonZeroU64, _: &()) -> Result<(), Report<Self::Error>> {
        if value.get() < MIN_OPEN_FILES || usize::try_from(value.get()).is_err() {
            return Err(Report::new(DatabaseSettingsError {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DatabaseSettingValues {
    pub cache_bytes: Option<ByteUnit>,
    pub journal_bytes: Option<ByteUnit>,
    pub workers: Option<usize>,
    pub max_open_files: Option<NonZeroU64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseSettings<N: DatabaseNamespace> {
    pub cache_bytes: Setting<DatabaseCacheBytes<N>>,
    pub journal_bytes: Setting<DatabaseJournalBytes<N>>,
    pub workers: Setting<DatabaseWorkers<N>>,
    pub max_open_files: Setting<DatabaseMaxOpenFiles<N>>,
}
impl<N: DatabaseNamespace> DatabaseSettings<N> {
    pub fn resolve(values: DatabaseSettingValues) -> Result<Self, Report<DatabaseSettingsError>> {
        Ok(Self {
            cache_bytes: Setting::resolve(values.cache_bytes, &())?,
            journal_bytes: Setting::resolve(values.journal_bytes, &())?,
            workers: Setting::new(values.workers, &())?,
            max_open_files: Setting::resolve(values.max_open_files, &())?,
        })
    }
    pub fn capabilities(&self, cpu: Option<NonZeroU64>) -> DatabaseCapabilities {
        let workers = match *self.workers.value() {
            Some(workers) => {
                u64::try_from(workers).assured("supported targets have at most 64-bit counts")
            }
            None => match cpu {
                Some(cpu) => cpu.get().min(4),
                None => 1,
            },
        };
        DatabaseCapabilities {
            cache_bytes: self.cache_bytes.value().as_u64(),
            journal_bytes: self.journal_bytes.value().as_u64(),
            workers: NonZeroU64::new(workers)
                .verified("validation and CPU detection both produce positive workers"),
            max_open_files: *self.max_open_files.value(),
        }
    }
}
impl<N: DatabaseNamespace> Default for DatabaseSettings<N> {
    fn default() -> Self {
        Self::resolve(DatabaseSettingValues::default())
            .assured("the database defaults satisfy engine bounds")
    }
}
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct DatabaseCapabilities {
    pub cache_bytes: u64,
    pub journal_bytes: u64,
    pub workers: NonZeroU64,
    /// Table files the database keeps open at once, at least ten and within the platform's counts.
    pub max_open_files: NonZeroU64,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn database_defaults_follow_cpu_detection_and_explicit_workers_keep_their_input() {
        let settings = DatabaseSettings::<NodeDatabase>::default();
        assert_eq!(settings.capabilities(None).workers.get(), 1);
        assert_eq!(settings.capabilities(NonZeroU64::new(2)).workers.get(), 2);
        assert_eq!(settings.capabilities(NonZeroU64::new(64)).workers.get(), 4);
        let explicit = DatabaseSettings::<ConsensusDatabase>::resolve(DatabaseSettingValues {
            workers: Some(2),
            ..Default::default()
        })
        .assured("two workers are positive");
        assert_eq!(explicit.capabilities(NonZeroU64::new(64)).workers.get(), 2);
        assert_eq!(*explicit.workers.value(), Some(2));
        assert_ne!(
            DatabaseWorkers::<NodeDatabase>::NAME,
            DatabaseWorkers::<ConsensusDatabase>::NAME
        );
    }
    #[test]
    fn the_engine_journal_floor_and_positive_workers_are_checked_before_opening() {
        for values in [
            DatabaseSettingValues {
                journal_bytes: Some(ByteUnit::Mebibyte(63)),
                ..Default::default()
            },
            DatabaseSettingValues {
                workers: Some(0),
                ..Default::default()
            },
            DatabaseSettingValues {
                cache_bytes: Some(ByteUnit::Byte(0)),
                ..Default::default()
            },
        ] {
            assert!(DatabaseSettings::<NodeDatabase>::resolve(values).is_err());
        }
    }
    #[test]
    fn the_open_file_limit_defaults_to_the_platform_count_and_carries_an_explicit_count() {
        let expected = if cfg!(target_os = "windows") {
            400
        } else if cfg!(target_os = "macos") {
            150
        } else {
            900
        };
        let node = DatabaseSettings::<NodeDatabase>::default();
        let consensus = DatabaseSettings::<ConsensusDatabase>::default();
        assert_eq!(node.capabilities(None).max_open_files.get(), expected);
        assert_eq!(consensus.capabilities(None).max_open_files.get(), expected);

        let explicit = DatabaseSettings::<ConsensusDatabase>::resolve(DatabaseSettingValues {
            max_open_files: NonZeroU64::new(10),
            ..Default::default()
        })
        .assured("ten open files is the engine's smallest accepted limit");
        assert_eq!(explicit.capabilities(None).max_open_files.get(), 10);
        assert_eq!(
            explicit.max_open_files.value().get(),
            10,
            "the explicit count stays the operator's input"
        );
        assert_ne!(
            DatabaseMaxOpenFiles::<NodeDatabase>::NAME,
            DatabaseMaxOpenFiles::<ConsensusDatabase>::NAME
        );
    }
    #[test]
    fn an_open_file_limit_below_the_engine_floor_is_refused_with_its_setting() {
        let error = DatabaseSettings::<NodeDatabase>::resolve(DatabaseSettingValues {
            max_open_files: NonZeroU64::new(9),
            ..Default::default()
        })
        .expect_err("nine open files is below the engine's smallest accepted limit");
        assert_eq!(
            *error.current_context(),
            DatabaseSettingsError {
                setting: "database-max-open-files"
            }
        );
        let error = DatabaseSettings::<ConsensusDatabase>::resolve(DatabaseSettingValues {
            max_open_files: NonZeroU64::new(9),
            ..Default::default()
        })
        .expect_err("the consensus database has the same floor");
        assert_eq!(
            error.current_context().to_string(),
            "raft-database-max-open-files is outside the database engine's supported capacity"
        );
    }
}
