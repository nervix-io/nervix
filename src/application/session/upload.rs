//! Resource uploads streamed over a session transport.
//!
//! Layer: edges.
//!
//! - **Owns.** Reading one upload stream, its start and the archive chunks that follow, staging
//!   the archive within the resource store's size limit and staging budget, and the typed reply
//!   that answers the stream.
//! - **Depends on.** The resource store's staging, the control plane's installation, which keeps
//!   an upload's identity stable across retries and completes it on every live node, and the
//!   client wire contract.
//! - **Must not know.** How the transport carries the stream or authenticates it.
//!
//! An upload is answered with a typed failure for everything its own frames can get wrong. Only a
//! failure of the transport itself ends the stream with the transport's own error.

use std::num::NonZeroU64;

use arch_into::ArchInto as _;
use futures_util::{Stream, StreamExt as _};
use meticulous::OptionExt as _;
use nervix_client_wire::{
    OutcomeOrigin, RequestId, UploadDisposition, UploadFailure, UploadFrame, UploadMessage,
    UploadReply, UploadStart, VerifiedFrame,
};
use nervix_consensus::ConsensusError;
use nervix_models::{ResourceUploadIdentity, ResourceUploadKey, UserName};

use super::outcome::{leader_redirect, wire_diagnostics};
use crate::application::{
    command_result::{CommandDisposition, CommandOrigin},
    resource::ResourceUploadError,
    session_service::SessionServiceImpl,
};

/// What an upload's reply says about the stream it answers, once its start was read.
struct UploadIdentity {
    request_id: RequestId,
    upload_identity: ResourceUploadIdentity,
}

impl UploadIdentity {
    fn failed(
        &self,
        failure: UploadFailure,
        message: String,
        assigned_version: Option<NonZeroU64>,
    ) -> UploadReply {
        UploadReply {
            request_id: Some(self.request_id),
            disposition: UploadDisposition::Failed {
                upload_identity: Some(self.upload_identity.clone()),
                failure,
                assigned_version,
            },
            message,
            diagnostics: Vec::new(),
        }
    }
}

/// The reply to a stream that did not begin with a valid upload start.
fn invalid_stream(message: String) -> UploadReply {
    UploadReply {
        request_id: None,
        disposition: UploadDisposition::Failed {
            upload_identity: None,
            failure: UploadFailure::InvalidStream,
            assigned_version: None,
        },
        message,
        diagnostics: Vec::new(),
    }
}

/// A resource version as the wire carries it. The catalog assigns versions counting from 1.
fn wire_version(version: u64) -> NonZeroU64 {
    NonZeroU64::new(version).assured("a resource catalog assigns versions counting from 1")
}

impl SessionServiceImpl {
    /// Serves one upload stream for `user`. A transport failure while the stream is read is
    /// returned as the transport's own error; everything else is answered with a reply.
    pub(in crate::application) async fn serve_upload<S, E>(
        &self,
        user: UserName,
        mut frames: S,
    ) -> Result<UploadReply, E>
    where
        S: Stream<Item = Result<VerifiedFrame<UploadFrame>, E>> + Unpin,
    {
        let first = match frames.next().await {
            Some(Ok(frame)) => frame,
            Some(Err(error)) => return Err(error),
            None => return Ok(invalid_stream("the upload stream is empty".to_string())),
        };
        let start = match UploadMessage::decode(&first) {
            Ok(UploadMessage::Start(start)) => start,
            Ok(UploadMessage::Chunk(_)) => {
                let message = "an upload stream begins with its upload start".to_string();
                return Ok(invalid_stream(message));
            }
            Err(error) => {
                return Ok(invalid_stream(format!(
                    "the upload start is invalid: {error}"
                )));
            }
        };
        let UploadStart {
            request_id,
            domain,
            resource,
            upload_identity,
            total_bytes,
        } = start;
        let identity = UploadIdentity {
            request_id,
            upload_identity: upload_identity.clone(),
        };

        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            let redirect = self.redirect_to_leader(leader).await;
            return Ok(UploadReply {
                request_id: Some(request_id),
                disposition: UploadDisposition::NotLeader(leader_redirect(redirect)),
                message: "resource uploads must be sent to the cluster leader".to_string(),
                diagnostics: Vec::new(),
            });
        }
        let resources = self.inner.consensus.current_resources().await;
        if !resources.is_declared(&domain, &resource) {
            return Ok(identity.failed(
                UploadFailure::ResourceNotDeclared,
                format!("resource '{}' does not exist", resource.as_str()),
                None,
            ));
        }
        let store = &self.inner.resource_store;
        if let Err(error) = store.validate_archive_bytes(total_bytes.get()) {
            return Ok(identity.failed(UploadFailure::QuotaExceeded, error.to_string(), None));
        }
        let mut archive = match store.create_archive_stager().await {
            Ok(archive) => archive,
            Err(error) => {
                let message = format!("failed to create the upload archive: {error}");
                return Ok(identity.failed(UploadFailure::InstallationFailed, message, None));
            }
        };

        let mut received = 0_u64;
        while let Some(frame) = frames.next().await {
            tokio::task::consume_budget().await;
            let frame = frame?;
            let chunk = match UploadMessage::decode(&frame) {
                Ok(UploadMessage::Chunk(chunk)) => chunk,
                Ok(UploadMessage::Start(_)) => {
                    let message = "an upload stream carries one upload start".to_string();
                    return Ok(identity.failed(UploadFailure::InvalidStream, message, None));
                }
                Err(error) => {
                    let message = format!("an upload chunk is invalid: {error}");
                    return Ok(identity.failed(UploadFailure::InvalidStream, message, None));
                }
            };
            let bytes = chunk.bytes();
            let length: u64 = bytes.len().arch_into();
            let Some(next_received) = received.checked_add(length) else {
                let message = "the upload is larger than any archive can be".to_string();
                return Ok(identity.failed(UploadFailure::SizeMismatch, message, None));
            };
            if next_received > total_bytes.get() {
                let message = format!(
                    "the upload exceeds its declared size of {} bytes",
                    total_bytes.get()
                );
                return Ok(identity.failed(UploadFailure::SizeMismatch, message, None));
            }
            if let Err(error) = store.validate_archive_bytes(next_received) {
                return Ok(identity.failed(UploadFailure::QuotaExceeded, error.to_string(), None));
            }
            let charged = match store.admit_staging_bytes(bytes).await {
                Ok(charged) => charged,
                Err(error) => {
                    return Ok(identity.failed(
                        UploadFailure::QuotaExceeded,
                        error.to_string(),
                        None,
                    ));
                }
            };
            if let Err(error) = archive.write_chunk(charged).await {
                let message = format!("failed to write an upload chunk: {error}");
                return Ok(identity.failed(UploadFailure::InstallationFailed, message, None));
            }
            received = next_received;
        }
        let archive = match archive.finish().await {
            Ok(archive) => archive,
            Err(error) => {
                let message = format!("failed to finish the upload archive: {error}");
                return Ok(identity.failed(UploadFailure::InstallationFailed, message, None));
            }
        };
        if received != total_bytes.get() || archive.archive_bytes() != received {
            let message = format!(
                "upload size mismatch: expected {}, received {received}",
                total_bytes.get()
            );
            return Ok(identity.failed(UploadFailure::SizeMismatch, message, None));
        }

        let key = ResourceUploadKey::new(user, domain, resource, upload_identity);
        let root_checksum = archive.root_checksum().to_string();
        let installer = self.clone();
        // Installation runs as its own task, so a caller that stops waiting never interrupts a
        // version between its publication and its completion.
        let installation = self.inner.service_tasks.spawn(async move {
            installer
                .install_uploaded_resource_archive(key, archive.path(), root_checksum)
                .await
        });
        let installation = match installation.await {
            Ok(Some(installation)) => installation,
            Ok(None) => {
                let message =
                    "the node shut down before the uploaded resource was installed".to_string();
                return Ok(identity.failed(UploadFailure::InstallationFailed, message, None));
            }
            Err(error) => {
                let message = format!("the resource installation task failed: {error}");
                return Ok(identity.failed(UploadFailure::InstallationFailed, message, None));
            }
        };
        match installation {
            Ok(installation) => {
                let origin = match installation.origin {
                    CommandOrigin::Executed => OutcomeOrigin::Executed,
                    CommandOrigin::Recovered => OutcomeOrigin::Recovered,
                };
                let version = wire_version(installation.version);
                Ok(UploadReply {
                    request_id: Some(request_id),
                    disposition: UploadDisposition::Installed {
                        upload_identity: identity.upload_identity,
                        version,
                        origin,
                    },
                    message: format!("uploaded resource version {version}"),
                    diagnostics: Vec::new(),
                })
            }
            Err(error) => {
                let message = format!("{error:#}");
                if let Some(consensus) = error.downcast_ref::<ConsensusError>() {
                    let result = self
                        .consensus_error_response(consensus, message.clone())
                        .await;
                    if let CommandDisposition::NotLeader(redirect) = result.disposition {
                        return Ok(UploadReply {
                            request_id: Some(request_id),
                            disposition: UploadDisposition::NotLeader(leader_redirect(redirect)),
                            message,
                            diagnostics: wire_diagnostics(result.diagnostics),
                        });
                    }
                }
                let assigned_version = match error.downcast_ref::<ResourceUploadError>() {
                    Some(upload_error) => upload_error.assigned_version().map(wire_version),
                    None => None,
                };
                Ok(identity.failed(UploadFailure::InstallationFailed, message, assigned_version))
            }
        }
    }
}
