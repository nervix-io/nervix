//! Admitted storage work for a fenced restore installation.
//!
//! Layer: control plane.
//! - **Owns.** The checkpoint byte source, bounded storage admission and the applied authority
//!   guard through staging, durable publication, cleanup and runtime handle clearing.
//! - **Depends on.** Consensus installation authority, the executor and runtime checkpoint APIs.
//! - **Must not know.** Database keys, checkpoint encoding or archive record encoding.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "stopped-domain restore storage is admitted lifecycle work"
    )
)]

use std::io::{self, Cursor, Read, Seek, SeekFrom};

use nervix_execution::{MemoryClass, StorageClass};
use nervix_interconnect::RemoteOperationFailure;
use nervix_models::{DomainName, RestoreStateAuthority};
use nervix_primitives::sync::Arc;

use super::interconnect::RestoreStateInventory;
use crate::{
    application::session_service::SessionServiceImpl,
    runtime::{RESTORE_STATE_WORKING_BYTES, RestoredRuntimeState, StagedArtifact},
};

/// Metadata records carry their own conversion reservation. Guest saves are read
/// directly from the verified archive or the receiver's quota-owned completed upload.
pub(in crate::application) enum RestoredStateSource {
    Encoded(Vec<u8>),
    Archive {
        artifact: Arc<StagedArtifact>,
        offset: u64,
    },
    Upload(StagedArtifact),
}

enum RestoredStateReader {
    Encoded(Cursor<Vec<u8>>),
    Archive {
        file: std::fs::File,
        _artifact: Arc<StagedArtifact>,
    },
    Upload {
        file: std::fs::File,
        _artifact: StagedArtifact,
    },
}

impl Read for RestoredStateReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Encoded(reader) => reader.read(buffer),
            Self::Archive { file, .. } | Self::Upload { file, .. } => file.read(buffer),
        }
    }
}

impl RestoredStateSource {
    fn open(self) -> io::Result<RestoredStateReader> {
        match self {
            Self::Encoded(bytes) => Ok(RestoredStateReader::Encoded(Cursor::new(bytes))),
            Self::Archive { artifact, offset } => {
                let mut file = std::fs::File::open(artifact.path())?;
                file.seek(SeekFrom::Start(offset))?;
                Ok(RestoredStateReader::Archive {
                    file,
                    _artifact: artifact,
                })
            }
            Self::Upload(artifact) => {
                let file = std::fs::File::open(artifact.path())?;
                Ok(RestoredStateReader::Upload {
                    file,
                    _artifact: artifact,
                })
            }
        }
    }

    fn working_bytes(&self) -> Option<u64> {
        match self {
            Self::Encoded(bytes) => u64::try_from(bytes.len())
                .ok()?
                .checked_mul(2)?
                .checked_add(RESTORE_STATE_WORKING_BYTES),
            Self::Archive { .. } | Self::Upload(_) => Some(RESTORE_STATE_WORKING_BYTES),
        }
    }
}

fn failed(domain: &DomainName, reason: &str) -> RemoteOperationFailure {
    RemoteOperationFailure::failed(
        nervix_interconnect::RemoteOperationSubject::domain(domain),
        reason.to_string(),
    )
}

impl SessionServiceImpl {
    pub(in crate::application) async fn stage_restored_state_checkpoint(
        &self,
        authority: &RestoreStateAuthority,
        checkpoint: RestoredRuntimeState,
        source: RestoredStateSource,
    ) -> Result<(), RemoteOperationFailure> {
        let domain = checkpoint.placement.domain.clone();
        let bytes = source
            .working_bytes()
            .ok_or_else(|| failed(&domain, "restore metadata exceeds address space"))?;
        let charge = self
            .inner
            .runtime
            .executor()
            .reserve(MemoryClass::Bulk, bytes)
            .await
            .map_err(|error| failed(&domain, &error.to_string()))?;
        let domain_owned = domain.clone();
        let authority = authority.clone();
        let service = self.clone();
        self.inner
            .runtime
            .executor()
            .run_storage(
                StorageClass::Filesystem,
                charge,
                move |_charge, cancellation| {
                    cancellation
                        .check()
                        .map_err(|error| failed(&domain_owned, &format!("{error:#}")))?;
                    let reader = source
                        .open()
                        .map_err(|error| failed(&domain_owned, &format!("{error:#}")))?;
                    service
                        .inner
                        .consensus
                        .with_restore_state_installation(&domain_owned, &authority, || {
                            service.inner.runtime.stage_restored_domain_state(
                                &authority,
                                checkpoint,
                                reader,
                                cancellation,
                            )
                        })
                        .map_err(|error| failed(&domain_owned, &format!("{error:#}")))?
                        .map_err(|error| failed(&domain_owned, &format!("{error:#}")))
                },
            )
            .await
            .map_err(|error| failed(&domain, &error.to_string()))?
    }

    pub(in crate::application) async fn publish_restored_state_generation(
        &self,
        domain: &DomainName,
        authority: &RestoreStateAuthority,
        inventory: RestoreStateInventory,
    ) -> Result<(), RemoteOperationFailure> {
        let charge = self
            .inner
            .runtime
            .executor()
            .reserve(MemoryClass::Bulk, RESTORE_STATE_WORKING_BYTES)
            .await
            .map_err(|error| failed(domain, &error.to_string()))?;
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_restore_state_publication_if_armed(domain, &authority.leader)
            .await;
        let domain_owned = domain.clone();
        let authority = authority.clone();
        let service = self.clone();
        self.inner
            .runtime
            .executor()
            .run_storage(
                StorageClass::Filesystem,
                charge,
                move |_charge, cancellation| {
                    cancellation
                        .check()
                        .map_err(|error| failed(&domain_owned, &format!("{error:#}")))?;
                    service
                        .inner
                        .consensus
                        .with_restore_state_installation(&domain_owned, &authority, || {
                            service.inner.runtime.publish_restored_domain_state(
                                &domain_owned,
                                &authority,
                                inventory,
                                cancellation,
                            )
                        })
                        .map_err(|error| {
                            #[cfg(feature = "testing")]
                            service
                                .inner
                                .runtime
                                .mark_restore_state_publication_refused(
                                    &domain_owned,
                                    &authority.leader,
                                );
                            failed(&domain_owned, &format!("{error:#}"))
                        })?
                        .map_err(|error| failed(&domain_owned, &format!("{error:#}")))
                },
            )
            .await
            .map_err(|error| failed(domain, &error.to_string()))?
    }
}
