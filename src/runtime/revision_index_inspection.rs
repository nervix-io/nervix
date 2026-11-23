//! Read-only inspection of the revision index a stopped node's runtime state store keeps.
//!
//! Layer: data plane.
//! - **Owns.** Opening a stopped node's runtime state store and counting the checkpoints and
//!   revision-index entries one domain's selected namespace holds.
//! - **Depends on.** The runtime state store and its namespace selection.
//! - **Must not know.** Stored key layouts, scenario steps, or how many entries a test expects.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "inspection opens a stopped runtime state store once"
    )
)]

use std::path::Path;

use error_stack::{Report, ResultExt as _};
use nervix_execution::Executor;
use nervix_models::{DomainName, SettingDefinition as _};
use thiserror::Error;

pub use super::state_store::StoredRevisionIndex;
use super::{RestoreStagingMaxBytes, state_store::RuntimeStateStore};

/// Why a stopped node's runtime state store could not be counted.
#[derive(Debug, Error)]
pub enum RevisionIndexInspectionError {
    #[error("the stopped node's database did not open")]
    OpenDatabase,
    #[error("the stopped node's runtime state store did not open")]
    OpenStore,
    #[error("the stopped node's runtime state store could not be read")]
    Read,
}

/// Counts what `domain`'s selected namespace holds in the database at `path`. Every node using that
/// database must have stopped before inspection.
pub fn read_stored_revision_index(
    path: &Path,
    domain: &DomainName,
) -> error_stack::Result<StoredRevisionIndex, RevisionIndexInspectionError> {
    let database = fjall::Database::builder(path)
        .open()
        .map_err(Report::new)
        .change_context(RevisionIndexInspectionError::OpenDatabase)?;
    let store = RuntimeStateStore::from_database(
        database,
        Executor::default(),
        RestoreStagingMaxBytes::default_value(&()).as_u64(),
    )
    .change_context(RevisionIndexInspectionError::OpenStore)?;
    store
        .stored_revision_index(domain)
        .change_context(RevisionIndexInspectionError::Read)
}
