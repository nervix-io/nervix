//! Local database capacity declarations shared by their node and consensus owners.
//!
//! Layer: vocabulary.
//! - **Owns.** Cache/journal/worker setting classes, names, defaults and validated resolved bounds.
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

pub trait DatabaseNamespace: std::fmt::Debug + Clone + Copy + PartialEq + Eq {
    const CACHE: &'static str;
    const JOURNAL: &'static str;
    const WORKERS: &'static str;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeDatabase;
impl DatabaseNamespace for NodeDatabase {
    const CACHE: &'static str = "database-cache-bytes";
    const JOURNAL: &'static str = "database-journal-bytes";
    const WORKERS: &'static str = "database-workers";
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusDatabase;
impl DatabaseNamespace for ConsensusDatabase {
    const CACHE: &'static str = "raft-database-cache-bytes";
    const JOURNAL: &'static str = "raft-database-journal-bytes";
    const WORKERS: &'static str = "raft-database-workers";
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DatabaseSettingValues {
    pub cache_bytes: Option<ByteUnit>,
    pub journal_bytes: Option<ByteUnit>,
    pub workers: Option<usize>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseSettings<N: DatabaseNamespace> {
    pub cache_bytes: Setting<DatabaseCacheBytes<N>>,
    pub journal_bytes: Setting<DatabaseJournalBytes<N>>,
    pub workers: Setting<DatabaseWorkers<N>>,
}
impl<N: DatabaseNamespace> DatabaseSettings<N> {
    pub fn resolve(values: DatabaseSettingValues) -> Result<Self, Report<DatabaseSettingsError>> {
        Ok(Self {
            cache_bytes: Setting::resolve(values.cache_bytes, &())?,
            journal_bytes: Setting::resolve(values.journal_bytes, &())?,
            workers: Setting::new(values.workers, &())?,
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
}
