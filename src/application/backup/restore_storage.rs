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
use meticulous::OptionExt as _;
use nervix_execution::{AdmissionError, MemoryClass, Reservation, StorageClass};
use nervix_interconnect::RemoteOperationFailure;
use nervix_models::{DomainName, RestoreStateAuthority};
use nervix_primitives::sync::Arc;
use tracing::{debug, warn};

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
    /// Run one restore checkpoint maintenance sweep, when bulk memory has room for its working set.
    pub(in crate::application) async fn sweep_restore_checkpoint_staging(&self) {
        let charge = self.restore_maintenance_working_set();
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .fault_injection()
            .restore_maintenance_asked_for_bulk_memory(self.inner.consensus.local_node_id());
        let charge = match charge {
            Ok(Some(charge)) => charge,
            Ok(None) => return,
            Err(error) => {
                warn!(
                    error = %format_args!("{error:#}"),
                    "restore checkpoint maintenance admission failed"
                );
                return;
            }
        };
        let executor = self.inner.runtime.executor();
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
            Err(error) => warn!(
                error = %format_args!("{error:#}"),
                "restore checkpoint maintenance executor failed"
            ),
        }
    }

    /// One sweep's working set, charged only from room the bulk class has free now, or `None` when
    /// the class cannot hold it now and the sweep is deferred to the next round. A working set
    /// larger than the whole class is refused.
    ///
    /// The charge never waits in the class's queue. A place there takes every byte the class
    /// releases until it is satisfied, so it would hold every other bulk charge on the node behind
    /// it, including the next charge of an operation that holds part of the room it waits for. That
    /// operation could then never release its part, nor the sweep take its working set, while the
    /// rest of the class stays occupied.
    fn restore_maintenance_working_set(
        &self,
    ) -> Result<Option<Reservation>, Report<AdmissionError>> {
        let executor = self.inner.runtime.executor();
        let bulk = executor.memory(MemoryClass::Bulk);
        let room = bulk
            .capacity_bytes
            .checked_sub(bulk.reserved_bytes)
            .assured("a budget never reserves more than its capacity");
        // A working set the whole class could never hold is still asked for, so the class refuses
        // it at once instead of every round deferring a sweep that no room could ever admit.
        let class_holds_working_set = RESTORE_STATE_WORKING_BYTES <= bulk.capacity_bytes;
        if class_holds_working_set && room < RESTORE_STATE_WORKING_BYTES {
            debug!(
                room,
                working_set = RESTORE_STATE_WORKING_BYTES,
                "restore checkpoint maintenance deferred until bulk memory has room for its \
                 working set"
            );
            return Ok(None);
        }
        match executor.try_reserve(MemoryClass::Bulk, RESTORE_STATE_WORKING_BYTES) {
            Ok(charge) => Ok(Some(charge)),
            // Other bulk work took the room between the read and the charge.
            Err(refusal)
                if matches!(
                    refusal.current_context(),
                    AdmissionError::BudgetExhausted { .. }
                ) =>
            {
                debug!(
                    working_set = RESTORE_STATE_WORKING_BYTES,
                    "restore checkpoint maintenance deferred until bulk memory has room for its \
                     working set"
                );
                Ok(None)
            }
            Err(refusal) => Err(refusal),
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

#[cfg(all(
    test,
    feature = "testing",
    not(any(feature = "shuttle", feature = "loom"))
))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "restore_storage_tests.rs"]
mod tests;
