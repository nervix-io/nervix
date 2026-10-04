//! The interconnect requests through which a replica asks the owner of the runtime state it keeps.
//!
//! Layer: edges.
//!
//! - **Owns.** Decoding a replica's state synchronization and branch checkpoint listing requests,
//!   refusing a placement this node is not assigned, and encoding the answer.
//! - **Depends on.** The runtime's answers and the interconnect's typed requests.
//! - **Must not know.** How the runtime keeps, catalogues or encodes the state it answers from.

use error_stack::ResultExt as _;
use nervix_interconnect::{
    BranchCheckpointListingRequest, BranchCheckpointListingResponse, RemoteOperationFailure,
    RemoteOperationSubject, StateSnapshotEnvelope, StateSyncRequest, StateSyncResponse,
};

use super::{AppError, session_service::SessionServiceImpl};
use crate::runtime::RuntimeStatePlacement;

impl SessionServiceImpl {
    /// Answer the replicas that ask this node for the runtime state it owns.
    pub(super) fn register_state_replication_interconnect_handlers(
        &self,
    ) -> error_stack::Result<(), AppError> {
        let state_sync_service = self.clone();
        self.inner
            .interconnect
            .register_handler::<StateSyncRequest, _, _>(move |_context, request| {
                let service = state_sync_service.clone();
                async move {
                    let subject = RemoteOperationSubject::state(&request.placement);
                    let placement = match RuntimeStatePlacement::from_remote(request.placement) {
                        Ok(placement) => placement,
                        Err(reason) => {
                            return StateSyncResponse {
                                result: Err(RemoteOperationFailure::failed(
                                    subject,
                                    reason.to_string(),
                                )),
                            };
                        }
                    };
                    let Some(admitted) = service
                        .inner
                        .runtime
                        .resolve_state_replication_request(&placement)
                    else {
                        return StateSyncResponse {
                            result: Err(RemoteOperationFailure::rejected(subject)),
                        };
                    };
                    let snapshot = service
                        .inner
                        .runtime
                        .answer_state_sync_request(admitted, request.after_lsm)
                        .await
                        .map_err(|error| error.current_context().as_remote_failure(subject));
                    StateSyncResponse {
                        result: snapshot.map(|snapshot| {
                            snapshot.map(|snapshot| StateSnapshotEnvelope {
                                lsm: snapshot.lsm,
                                payload: snapshot.payload,
                            })
                        }),
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let checkpoint_listing_service = self.clone();
        self.inner
            .interconnect
            .register_handler::<BranchCheckpointListingRequest, _, _>(move |_context, request| {
                let service = checkpoint_listing_service.clone();
                async move {
                    let subject = RemoteOperationSubject::state(&request.lifecycle);
                    let placement = match RuntimeStatePlacement::from_remote(request.lifecycle) {
                        Ok(placement) => placement,
                        Err(reason) => {
                            return BranchCheckpointListingResponse {
                                result: Err(RemoteOperationFailure::failed(
                                    subject,
                                    reason.to_string(),
                                )),
                            };
                        }
                    };
                    let Some(admitted) = service
                        .inner
                        .runtime
                        .resolve_state_replication_request(&placement)
                    else {
                        return BranchCheckpointListingResponse {
                            result: Err(RemoteOperationFailure::rejected(subject)),
                        };
                    };
                    let listing = service
                        .inner
                        .runtime
                        .answer_branch_checkpoint_listing(admitted, request.after);
                    BranchCheckpointListingResponse {
                        result: Ok(listing),
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        Ok(())
    }
}
