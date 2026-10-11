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
    BranchCheckpointListingRequest, BranchCheckpointListingResponse, DescribeKafkaOffsets,
    FetchStateCheckpoint, RemoteOperationFailure, RemoteOperationSubject, StateSyncRequest,
    StateSyncResponse, SyncKafkaOffsets,
};

use super::{AppError, session_service::SessionServiceImpl};
use crate::runtime::RuntimeStatePlacement;

impl SessionServiceImpl {
    /// Answer the replicas that ask this node for the runtime state it owns.
    pub(super) fn register_state_replication_interconnect_handlers(
        &self,
    ) -> error_stack::Result<(), AppError> {
        let describe_kafka_offsets_service = self.clone();
        self.inner
            .interconnect
            .register_handler::<DescribeKafkaOffsets, _, _>(move |_context, request| {
                let service = describe_kafka_offsets_service.clone();
                async move { service.inner.runtime.describe_kafka_offsets(request) }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let kafka_offsets_service = self.clone();
        self.inner
            .interconnect
            .register_stream_handler::<SyncKafkaOffsets, _, _>(move |_context, request| {
                let service = kafka_offsets_service.clone();
                async move { service.inner.runtime.stream_kafka_offsets(request).await }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let checkpoint_service = self.clone();
        self.inner
            .interconnect
            .register_stream_handler::<FetchStateCheckpoint, _, _>(move |_context, request| {
                let service = checkpoint_service.clone();
                async move { service.inner.runtime.stream_state_checkpoint(request).await }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

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
                        .await;
                    let described = match snapshot {
                        Ok(Some(snapshot)) => service
                            .inner
                            .runtime
                            .describe_state_checkpoint(&placement, snapshot)
                            .await
                            .map(Some),
                        Ok(None) => Ok(None),
                        Err(error) => Err(error),
                    };
                    StateSyncResponse {
                        result: described
                            .map_err(|error| error.current_context().as_remote_failure(subject)),
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
