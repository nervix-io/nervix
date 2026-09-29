//! Restores streamed over a session transport.
//!
//! Layer: edges.
//!
//! - **Owns.** Reading one restore stream — its start, the `RESTORE` statement the start names, and
//!   the archive chunks that follow — and the typed reply that answers the stream.
//! - **Depends on.** The control plane's restore, which stages, verifies, plans and applies it, the
//!   language layer to read the statement, and the client wire contract.
//! - **Must not know.** How the transport carries the stream or authenticates it.
//!
//! A restore stream is answered with a typed failure for everything its own frames get wrong, and
//! otherwise with the outcome of the restore as the persistent command it is. Only a failure of
//! the transport itself ends the stream with the transport's own error.

use futures_util::{Stream, StreamExt as _};
use nervix_client_wire::{
    RequestId, RestoreDisposition, RestoreFrame, RestoreMessage, RestoreReply,
    RestoreUploadFailure, VerifiedFrame,
};
use nervix_models::{Statement, UserName};
use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};

use super::outcome::command_outcome;
use crate::application::{
    restore::{RestoreAnswer, RestoreRequest, RestoreStreamPart, StagingRefusal},
    session_service::SessionServiceImpl,
};

/// The reply to a stream refused before its restore ran.
fn refused(
    request_id: Option<RequestId>,
    failure: RestoreUploadFailure,
    message: String,
) -> RestoreReply {
    RestoreReply {
        request_id,
        disposition: RestoreDisposition::UploadFailed { failure, message },
    }
}

/// One frame after a restore start, as the control plane reads it.
fn restore_part(frame: VerifiedFrame<RestoreFrame>) -> RestoreStreamPart {
    match RestoreMessage::decode(&frame) {
        Ok(RestoreMessage::Chunk(chunk)) => RestoreStreamPart::Chunk(chunk.shared_bytes()),
        Ok(RestoreMessage::Start(_)) => {
            RestoreStreamPart::Invalid("a restore stream carries one restore start".to_string())
        }
        Err(error) => RestoreStreamPart::Invalid(format!("a restore chunk is invalid: {error}")),
    }
}

/// The typed failure a staging refusal is reported as.
fn upload_failure(refusal: &StagingRefusal) -> RestoreUploadFailure {
    match refusal {
        StagingRefusal::InvalidStream { .. } => RestoreUploadFailure::InvalidStream,
        StagingRefusal::Oversized { .. } | StagingRefusal::Truncated { .. } => {
            RestoreUploadFailure::SizeMismatch
        }
        StagingRefusal::DigestMismatch => RestoreUploadFailure::DigestMismatch,
        StagingRefusal::TooLarge { .. } | StagingRefusal::StagingFull { .. } => {
            RestoreUploadFailure::QuotaExceeded
        }
        StagingRefusal::StagingFailed => RestoreUploadFailure::StagingFailed,
    }
}

impl SessionServiceImpl {
    /// Serves one restore stream for `user`. A transport failure while the stream is read is
    /// returned as the transport's own error; everything else is answered with a reply.
    pub(in crate::application) async fn serve_restore<S, E>(
        &self,
        user: UserName,
        mut frames: S,
    ) -> Result<RestoreReply, E>
    where
        S: Stream<Item = Result<VerifiedFrame<RestoreFrame>, E>> + Unpin + Send,
    {
        let first = match frames.next().await {
            Some(Ok(frame)) => frame,
            Some(Err(error)) => return Err(error),
            None => {
                return Ok(refused(
                    None,
                    RestoreUploadFailure::InvalidStream,
                    "the restore stream is empty".to_string(),
                ));
            }
        };
        let start = match RestoreMessage::decode(&first) {
            Ok(RestoreMessage::Start(start)) => start,
            Ok(RestoreMessage::Chunk(_)) => {
                return Ok(refused(
                    None,
                    RestoreUploadFailure::InvalidStream,
                    "a restore stream begins with its restore start".to_string(),
                ));
            }
            Err(error) => {
                return Ok(refused(
                    None,
                    RestoreUploadFailure::InvalidStream,
                    format!("the restore start is invalid: {error}"),
                ));
            }
        };
        let request_id = Some(start.request_id);
        let restore = match parse_client_statement(&start.statement) {
            Ok(ClientStatement::Server(Statement::Restore(restore))) => restore,
            Ok(_) => {
                return Ok(refused(
                    request_id,
                    RestoreUploadFailure::InvalidStatement,
                    "a restore stream's start names one RESTORE statement".to_string(),
                ));
            }
            Err(error) => {
                return Ok(refused(
                    request_id,
                    RestoreUploadFailure::InvalidStatement,
                    format!("the RESTORE statement does not parse: {error:#}"),
                ));
            }
        };
        let reference = start.execution_reference.clone();
        let request = RestoreRequest {
            reference: reference.clone(),
            restore,
            archive: start.archive,
        };
        let parts = frames.map(|item| item.map(restore_part));
        let answer = self.serve_restore_stream(user, request, parts).await?;
        let disposition = match answer {
            RestoreAnswer::Outcome(response) => {
                RestoreDisposition::Outcome(Box::new(command_outcome(reference, *response)))
            }
            RestoreAnswer::Refused(refusal) => RestoreDisposition::UploadFailed {
                failure: upload_failure(&refusal),
                message: refusal.to_string(),
            },
        };
        Ok(RestoreReply {
            request_id,
            disposition,
        })
    }
}
