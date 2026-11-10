//! Database capacity inputs at the command-line edge.
//!
//! Layer: edges.
//! - **Owns.** Parsing and rendering the node and consensus database options.
//! - **Depends on.** Shared database capacity declarations and edge byte quantities.
//! - **Must not know.** Fjall, filesystem I/O or worker lifetime.
#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns database capacity arguments")
)]
use error_stack::Report;
use nervix_models::{
    ConsensusDatabase, DatabaseCacheBytes, DatabaseJournalBytes, DatabaseSettingValues,
    DatabaseSettings, DatabaseSettingsError, DatabaseWorkers, NodeDatabase, SettingDefinition as _,
};

use super::settings::CommandLine;
use crate::application::cli_values::HumanBytes;
#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Databases")]
pub struct DatabaseArgs {
    #[arg(long = DatabaseCacheBytes::<NodeDatabase>::NAME, env = "NERVIX_DATABASE_CACHE_BYTES", help = super::settings::capability_help::<DatabaseCacheBytes::<NodeDatabase>>("Cache bytes", HumanBytes(DatabaseCacheBytes::<NodeDatabase>::default_value(&()))))]
    pub cache_bytes: Option<HumanBytes>,
    #[arg(long = DatabaseJournalBytes::<NodeDatabase>::NAME, env = "NERVIX_DATABASE_JOURNAL_BYTES", help = super::settings::capability_help::<DatabaseJournalBytes::<NodeDatabase>>("Journal bytes", HumanBytes(DatabaseJournalBytes::<NodeDatabase>::default_value(&()))))]
    pub journal_bytes: Option<HumanBytes>,
    #[arg(long = DatabaseWorkers::<NodeDatabase>::NAME, env = "NERVIX_DATABASE_WORKERS", help = super::settings::capability_help::<DatabaseWorkers::<NodeDatabase>>("Workers", "derived"))]
    pub workers: Option<usize>,
    #[arg(long = DatabaseCacheBytes::<ConsensusDatabase>::NAME, env = "NERVIX_RAFT_DATABASE_CACHE_BYTES", help = super::settings::capability_help::<DatabaseCacheBytes::<ConsensusDatabase>>("Raft cache bytes", HumanBytes(DatabaseCacheBytes::<ConsensusDatabase>::default_value(&()))))]
    pub raft_cache_bytes: Option<HumanBytes>,
    #[arg(long = DatabaseJournalBytes::<ConsensusDatabase>::NAME, env = "NERVIX_RAFT_DATABASE_JOURNAL_BYTES", help = super::settings::capability_help::<DatabaseJournalBytes::<ConsensusDatabase>>("Raft journal bytes", HumanBytes(DatabaseJournalBytes::<ConsensusDatabase>::default_value(&()))))]
    pub raft_journal_bytes: Option<HumanBytes>,
    #[arg(long = DatabaseWorkers::<ConsensusDatabase>::NAME, env = "NERVIX_RAFT_DATABASE_WORKERS", help = super::settings::capability_help::<DatabaseWorkers::<ConsensusDatabase>>("Raft workers", "derived"))]
    pub raft_workers: Option<usize>,
}
impl DatabaseArgs {
    pub(super) fn node(
        &self,
    ) -> Result<DatabaseSettings<NodeDatabase>, Report<DatabaseSettingsError>> {
        DatabaseSettings::resolve(DatabaseSettingValues {
            cache_bytes: self.cache_bytes.map(|v| v.0),
            journal_bytes: self.journal_bytes.map(|v| v.0),
            workers: self.workers,
        })
    }
    pub(super) fn consensus(
        &self,
    ) -> Result<DatabaseSettings<ConsensusDatabase>, Report<DatabaseSettingsError>> {
        DatabaseSettings::resolve(DatabaseSettingValues {
            cache_bytes: self.raft_cache_bytes.map(|v| v.0),
            journal_bytes: self.raft_journal_bytes.map(|v| v.0),
            workers: self.raft_workers,
        })
    }
    pub(super) fn from_settings(
        node: &DatabaseSettings<NodeDatabase>,
        consensus: &DatabaseSettings<ConsensusDatabase>,
    ) -> Self {
        Self {
            cache_bytes: Some(HumanBytes(*node.cache_bytes.value())),
            journal_bytes: Some(HumanBytes(*node.journal_bytes.value())),
            workers: *node.workers.value(),
            raft_cache_bytes: Some(HumanBytes(*consensus.cache_bytes.value())),
            raft_journal_bytes: Some(HumanBytes(*consensus.journal_bytes.value())),
            raft_workers: *consensus.workers.value(),
        }
    }
    pub(super) fn render(&self, line: &mut CommandLine) {
        line.optional(DatabaseCacheBytes::<NodeDatabase>::NAME, self.cache_bytes);
        line.optional(
            DatabaseJournalBytes::<NodeDatabase>::NAME,
            self.journal_bytes,
        );
        line.optional(DatabaseWorkers::<NodeDatabase>::NAME, self.workers);
        line.optional(
            DatabaseCacheBytes::<ConsensusDatabase>::NAME,
            self.raft_cache_bytes,
        );
        line.optional(
            DatabaseJournalBytes::<ConsensusDatabase>::NAME,
            self.raft_journal_bytes,
        );
        line.optional(
            DatabaseWorkers::<ConsensusDatabase>::NAME,
            self.raft_workers,
        );
    }
}
