//! Admitted storage work for a fenced restore installation.
//!
//! Layer: control plane.
//! - **Owns.** The checkpoint byte source, bounded storage admission and the applied authority
//!   guard through staging, durable publication, cleanup and runtime handle clearing.
//! - **Depends on.** Consensus installation authority, the executor and runtime checkpoint APIs.
//! - **Must not know.** Database keys, checkpoint encoding or archive record encoding.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "stopped-domain restore storage is admitted lifecycle work"
    )
)]

use std::io::{self, Read, Seek, SeekFrom};

use error_stack::{Report, ResultExt as _};
use nervix_execution::{MemoryClass, StorageClass};
use nervix_interconnect::RemoteOperationFailure;
use nervix_models::{DomainName, RestoreStateAuthority};
use nervix_primitives::sync::Arc;
use tracing::warn;

use super::interconnect::RestoreStateInventory;
use crate::{
    application::session_service::SessionServiceImpl,
    runtime::{RESTORE_STATE_WORKING_BYTES, RestoredRuntimeState, StagedArtifact},
};

/// Native checkpoints and guest saves are read from quota-owned files.
pub(in crate::application) enum RestoredStateSource {
    Archive {
        artifact: Arc<StagedArtifact>,
        offset: u64,
    },
    Upload(StagedArtifact),
}

enum RestoredStateReader {
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
            Self::Archive { file, .. } | Self::Upload { file, .. } => file.read(buffer),
        }
    }
}

impl RestoredStateSource {
    fn open(self) -> io::Result<RestoredStateReader> {
        match self {
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
}

use super::interconnect::failure_context;

impl SessionServiceImpl {
    pub(in crate::application) async fn sweep_restore_checkpoint_staging(&self) {
        let executor = self.inner.runtime.executor();
        let charge = match executor
            .reserve(MemoryClass::Bulk, RESTORE_STATE_WORKING_BYTES)
            .await
        {
            Ok(charge) => charge,
            Err(error) => {
                warn!(%error, "restore checkpoint maintenance admission failed");
                return;
            }
        };
        let service = self.clone();
        let result = executor
            .run_storage(
                StorageClass::Filesystem,
                charge,
                move |_charge, cancellation| {
                    service
                        .inner
                        .consensus
                        .with_restore_state_reclamation(|retention| {
                            service.inner.runtime.reclaim_restore_checkpoint_staging(
                                |domain, generation| retention.retains(domain, generation),
                                cancellation,
                            )
                        })
                },
            )
            .await;
        match result {
            Ok(Ok(observation)) => self
                .inner
                .runtime
                .metrics()
                .record_restore_staging(observation),
            Ok(Err(error)) => {
                warn!(error = %format!("{error:#}"), "restore checkpoint maintenance failed")
            }
            Err(error) => warn!(%error, "restore checkpoint maintenance executor failed"),
        }
    }

    pub(in crate::application) async fn stage_restored_state_checkpoint(
        &self,
        authority: &RestoreStateAuthority,
        checkpoint: RestoredRuntimeState,
        source: RestoredStateSource,
    ) -> Result<(), Report<RemoteOperationFailure>> {
        let domain = checkpoint.placement.domain.clone();
        let charge = self
            .inner
            .runtime
            .executor()
            .reserve(MemoryClass::Bulk, RESTORE_STATE_WORKING_BYTES)
            .await
            .change_context_lazy(|| {
                failure_context(&domain, "restore state installation failed")
            })?;
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
                    cancellation.check().change_context_lazy(|| {
                        failure_context(&domain_owned, "restore state installation failed")
                    })?;
                    let reader = source.open().map_err(|error| {
                        Report::new(error).change_context(failure_context(
                            &domain_owned,
                            "restore state installation failed",
                        ))
                    })?;
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
                        .change_context_lazy(|| {
                            failure_context(&domain_owned, "restore state installation failed")
                        })?
                        .change_context_lazy(|| {
                            failure_context(&domain_owned, "restore state installation failed")
                        })
                },
            )
            .await
            .change_context_lazy(|| failure_context(&domain, "restore state installation failed"))?
    }

    pub(in crate::application) async fn publish_restored_state_generation(
        &self,
        domain: &DomainName,
        authority: &RestoreStateAuthority,
        inventory: RestoreStateInventory,
    ) -> Result<(), Report<RemoteOperationFailure>> {
        let charge = self
            .inner
            .runtime
            .executor()
            .reserve(MemoryClass::Bulk, RESTORE_STATE_WORKING_BYTES)
            .await
            .change_context_lazy(|| failure_context(domain, "restore state installation failed"))?;
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
                    cancellation.check().change_context_lazy(|| {
                        failure_context(&domain_owned, "restore state installation failed")
                    })?;
                    let publication = service.inner.consensus.with_restore_state_installation(
                        &domain_owned,
                        &authority,
                        || {
                            service.inner.runtime.publish_restored_domain_state(
                                &domain_owned,
                                &authority,
                                inventory,
                                cancellation,
                            )
                        },
                    );
                    let state = match publication {
                        Ok(state) => state,
                        Err(error) => {
                            #[cfg(feature = "testing")]
                            service
                                .inner
                                .runtime
                                .mark_restore_state_publication_refused(
                                    &domain_owned,
                                    &authority.leader,
                                );
                            return Err(error.change_context(failure_context(
                                &domain_owned,
                                "restore state publication was refused",
                            )));
                        }
                    };
                    state.change_context_lazy(|| {
                        failure_context(&domain_owned, "restore state installation failed")
                    })
                },
            )
            .await
            .change_context_lazy(|| failure_context(domain, "restore state installation failed"))?
    }
}
