//! Edges layer: the archive a directory becomes, and the stream that carries it to a server.
//!
//! - **Owns.** Building the deterministic archive of a directory, streaming it as upload frames,
//!   checking the reply against the upload it answers, and the routing that reply calls for.
//! - **Depends on.** The wire contract's upload frames, tonic's gRPC client, and the client's
//!   session recovery and leader routing.
//! - **Must not know.** How the server installs a resource.

use std::{
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
};

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    RequestId, SessionLimits, UploadChunk, UploadDisposition, UploadFrame, UploadReply,
    UploadReplyFrame, UploadStart, VerifiedFrame, WireEncodeError, grpc::UPLOAD_RESOURCE_PATH,
};
use nervix_models::{DomainName, ResourceName, ResourceUploadIdentity};
use nervix_primitives::{stream::wrappers::ReceiverStream, sync::mpsc};
use nervix_recovery::Reported as _;
use tempfile::TempPath;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
};
use tokio_tar::{Builder as AsyncTarBuilder, EntryType, Header, HeaderMode};
use tonic::{Request, Response, Status, codegen::http::uri::PathAndQuery, transport::Channel};
use tracing::debug;

use crate::{
    client::{
        Client, LOCAL_STATEMENT_IN_TRANSACTION, RecoveryDeadline, RecoveryMode, SessionRecovery,
    },
    connection::GrpcConnector,
    error::{ClientError, RequestKind},
    outcome::{CommandOutcome, ResourceUploadOutcome, Routing},
};

/// Archive bytes one upload frame carries.
const UPLOAD_CHUNK_BYTES: usize = 64 * 1024;

/// Archive construction keeps one bounded read buffer and one bounded write buffer, avoiding
/// a filesystem worker handoff for every small tar copy and header write.
const ARCHIVE_BUFFER_BYTES: usize = 1024 * 1024;

/// The request identity of an upload's start frame. Every attempt is a stream of its own that
/// exactly one reply answers, so the identity only has to be non-zero.
const UPLOAD_REQUEST_ID: RequestId = RequestId::new(NonZeroU64::MIN);

// An upload opens at most one call per leader routing attempt, and its outcome counts the calls
// it opened in a `u32`: every attempt number below the bound fits one.
const _: () = assert!(
    Client::MAX_LEADER_ROUTING_ATTEMPTS.ilog2() < u32::BITS,
    "the calls an upload opens fit the u32 its outcome counts them in"
);

impl Client {
    pub async fn upload_resource_from_directory(
        &self,
        identifier: &str,
        directory: impl AsRef<Path>,
        on_progress: impl Fn(u64) + Send + Sync + Clone + 'static,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let Some(domain) = self.domain().await else {
            return Err(Report::new(ClientError::NoActiveDomain));
        };
        self.upload_resource_from_directory_with_identity(
            identifier,
            directory,
            domain,
            ResourceUploadIdentity::parse(uuid::Uuid::now_v7().to_string())
                .assured("a UUID string satisfies the upload identity grammar"),
            on_progress,
        )
        .await
    }

    /// Uploads `directory` as a version of the resource `identifier` names. Both `domain` and
    /// `upload_identity` stay the same across every retry and redirect, so an upload the server
    /// already installed is recovered rather than installed twice. A session that holds an active
    /// transaction refuses the upload before it sends anything: a version is never transaction
    /// content, so reverting the transaction could not take it back.
    pub async fn upload_resource_from_directory_with_identity(
        &self,
        identifier: &str,
        directory: impl AsRef<Path>,
        domain: DomainName,
        upload_identity: ResourceUploadIdentity,
        on_progress: impl Fn(u64) + Send + Sync + Clone + 'static,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        if self.active_transaction_status().await.is_some() {
            return Ok(CommandOutcome::failed_locally(
                LOCAL_STATEMENT_IN_TRANSACTION.to_string(),
            ));
        }
        let resource =
            ResourceName::parse(identifier).change_context(ClientError::InvalidResourceName {
                name: identifier.to_string(),
            })?;
        let archive = UploadArchive::build(directory.as_ref()).await?;
        let mut admission_possible = false;
        let result =
            nervix_primitives::time::timeout(self.inner.connector.retry_timeout(), async {
                // How many calls the upload opened, the one under way included.
                let mut calls = NonZeroU32::MIN;
                let mut next_limits = None;
                for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
                    nervix_primitives::task::consume_budget().await;
                    calls = Self::upload_calls_opened(attempt);
                    let start = UploadStart {
                        request_id: UPLOAD_REQUEST_ID,
                        domain: domain.clone(),
                        resource: resource.clone(),
                        upload_identity: upload_identity.clone(),
                        total_bytes: archive.total_bytes,
                    };
                    let reader = File::open(&archive.path)
                        .await
                        .change_context(ClientError::BuildUploadArchive)?;
                    let (channel, session_limits) = {
                        let exchange = self.inner.exchange.lock().await;
                        (exchange.channel.clone(), exchange.requests.limits)
                    };
                    let stream = UploadAttempt {
                        channel,
                        limits: next_limits.take().unwrap_or(session_limits),
                        start,
                        archive: reader,
                    };
                    let sent = nervix_primitives::time::timeout(
                        self.inner.connector.request_timeout(),
                        stream.send(&self.inner.connector, on_progress.clone()),
                    )
                    .await;
                    let sent = sent.unwrap_or_else(|_| {
                        Err(UploadSendFailure::Transport(Box::new(
                            Status::deadline_exceeded("upload request deadline exceeded"),
                        )))
                    });
                    if let Err(UploadSendFailure::Transport(status)) = &sent {
                        admission_possible |= upload_status_is_retryable(status);
                    }
                    let response = match sent {
                        Ok(response) => response,
                        Err(UploadSendFailure::BoundsChanged(limits)) => {
                            next_limits = Some(limits);
                            continue;
                        }
                        Err(UploadSendFailure::Encode(report)) => {
                            return Err(report.change_context(ClientError::EncodeRequest {
                                request: RequestKind::UploadResource,
                            }));
                        }
                        Err(UploadSendFailure::Transport(status))
                            if upload_status_is_retryable(&status)
                                && self.await_retry(attempt).await =>
                        {
                            debug!(
                                upload_identity = %upload_identity,
                                calls = calls.get(),
                                %status,
                                "a resource upload call ended before its reply arrived, so the \
                                 upload opens another with the same identity and archive"
                            );
                            match self
                                .recover_session(
                                    RecoveryMode::Replace,
                                    RecoveryDeadline::RetryBudget,
                                )
                                .await?
                            {
                                SessionRecovery::Ready => continue,
                                SessionRecovery::Unavailable => {
                                    return Err(Report::new(ClientError::UploadResource(status)));
                                }
                            }
                        }
                        Err(UploadSendFailure::Transport(status)) => {
                            return Err(Report::new(ClientError::UploadResource(status)));
                        }
                    };
                    let outcome =
                        Self::validate_upload_reply(response.get_ref(), &upload_identity, calls)?;
                    match outcome.routing() {
                        Routing::Redirect(leader) => {
                            debug!(
                                upload_identity = %upload_identity,
                                calls = calls.get(),
                                %leader,
                                "a resource upload call was redirected, so the upload opens \
                                 another at the leader with the same identity and archive"
                            );
                            self.follow_leader(leader, RecoveryDeadline::RetryBudget)
                                .await?;
                        }
                        Routing::AwaitElection if self.await_retry(attempt).await => {
                            debug!(
                                upload_identity = %upload_identity,
                                calls = calls.get(),
                                "a resource upload call found no leader, so the upload opens \
                                 another with the same identity and archive"
                            );
                        }
                        _ => return Ok(outcome),
                    }
                }

                if admission_possible {
                    return Err(Report::new(ClientError::RetryDeadline));
                }
                let mut exhausted =
                    CommandOutcome::failed_locally("upload redirect loop exceeded".to_string());
                exhausted.resource_upload = Some(ResourceUploadOutcome {
                    identity: upload_identity.clone(),
                    version: None,
                    origin: None,
                    failure: None,
                    calls,
                });
                Ok(exhausted)
            })
            .await;
        match result {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(report))
                if admission_possible || report.current_context().can_hide_installed_upload() =>
            {
                Err(report.change_context(ClientError::UncertainUpload {
                    identity: upload_identity,
                }))
            }
            Ok(Err(report)) => Err(report),
            Err(_) => {
                let expired = Report::new(ClientError::RetryDeadline);
                Err(expired.change_context(ClientError::UncertainUpload {
                    identity: upload_identity,
                }))
            }
        }
    }

    /// Decode and correlate a reply before its outcome can drive upload routing.
    fn validate_upload_reply(
        frame: &VerifiedFrame<UploadReplyFrame>,
        upload_identity: &ResourceUploadIdentity,
        calls: NonZeroU32,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let reply = UploadReply::decode(frame).change_context(ClientError::InvalidUploadReply)?;
        if let Some(request_id) = reply.request_id
            && request_id != UPLOAD_REQUEST_ID
        {
            return Err(Report::new(ClientError::UnexpectedReply {
                request: RequestKind::UploadResource,
            }));
        }
        let received = match &reply.disposition {
            UploadDisposition::Installed {
                upload_identity, ..
            } => Some(upload_identity),
            UploadDisposition::Failed {
                upload_identity, ..
            } => upload_identity.as_ref(),
            UploadDisposition::NotLeader(_) => None,
        };
        if let Some(received) = received
            && received != upload_identity
        {
            return Err(Report::new(ClientError::UploadIdentityMismatch {
                expected: upload_identity.clone(),
                received: received.clone(),
            }));
        }
        Ok(CommandOutcome::from_upload(
            reply,
            upload_identity.clone(),
            calls,
        ))
    }

    /// How many calls an upload has opened once it opened the call of its zero-based `attempt`.
    fn upload_calls_opened(attempt: usize) -> NonZeroU32 {
        let earlier = u32::try_from(attempt).assured(
            "an upload makes fewer attempts than MAX_LEADER_ROUTING_ATTEMPTS, which a const \
             assertion keeps within u32",
        );
        NonZeroU32::MIN.checked_add(earlier).assured(
            "the calls before this one number fewer than MAX_LEADER_ROUTING_ATTEMPTS, so counting \
             this one as well stays within u32",
        )
    }
}

/// A directory's deterministic archive, written once to a temporary file and streamed from it on
/// every attempt.
struct UploadArchive {
    path: TempPath,
    total_bytes: NonZeroU64,
}

impl UploadArchive {
    async fn build(directory: &Path) -> error_stack::Result<Self, ClientError> {
        let directory = expand_user_path(directory);
        if !directory.is_dir() {
            return Err(Report::new(ClientError::BuildUploadArchive));
        }
        let path = tempfile::NamedTempFile::new()
            .change_context(ClientError::BuildUploadArchive)?
            .into_temp_path();
        let file = File::create(&path)
            .await
            .change_context(ClientError::BuildUploadArchive)?;
        Self::write(
            &directory,
            BufWriter::with_capacity(ARCHIVE_BUFFER_BYTES, file),
        )
        .await
        .change_context(ClientError::BuildUploadArchive)?;
        let length = tokio::fs::metadata(&path)
            .await
            .change_context(ClientError::BuildUploadArchive)?
            .len();
        let Some(total_bytes) = NonZeroU64::new(length) else {
            return Err(Report::new(ClientError::BuildUploadArchive));
        };
        Ok(Self { path, total_bytes })
    }

    /// Writes `directory` as a tar archive whose bytes depend only on the directory's names and
    /// contents: entries in name order, with fixed times, owners and modes.
    async fn write<W>(directory: &Path, writer: W) -> std::io::Result<()>
    where
        W: AsyncWrite + Unpin + Send + Sync,
    {
        let entries = UploadArchiveEntry::collect(directory)?;
        // This constructor has no asynchronous drop finalizer: an error or cancellation
        // abandons the partial archive with its writer. Only the successful path writes the
        // terminator and flushes, before build exposes the temporary file to an upload.
        let mut builder = AsyncTarBuilder::new_non_terminated(writer);
        builder.mode(HeaderMode::Deterministic);

        for entry in entries {
            nervix_primitives::task::consume_budget().await;
            let mut header = Header::new_ustar();
            header.set_mtime(0);
            header.set_uid(0);
            header.set_gid(0);
            match entry {
                UploadArchiveEntry::Directory { relative } => {
                    header.set_size(0);
                    header.set_mode(0o755);
                    header.set_entry_type(EntryType::Directory);
                    header.set_cksum();
                    builder
                        .append_data(&mut header, &relative, tokio::io::empty())
                        .await?;
                }
                UploadArchiveEntry::File {
                    full_path,
                    relative,
                    size,
                } => {
                    header.set_size(size);
                    header.set_mode(0o644);
                    header.set_entry_type(EntryType::Regular);
                    header.set_cksum();
                    let file = File::open(&full_path).await?;
                    let reader = BufReader::with_capacity(ARCHIVE_BUFFER_BYTES, file);
                    builder.append_data(&mut header, &relative, reader).await?;
                }
            }
        }

        let mut writer = builder.into_inner().await?;
        writer.shutdown().await?;
        Ok(())
    }
}

enum UploadArchiveEntry {
    Directory {
        relative: PathBuf,
    },
    File {
        full_path: PathBuf,
        relative: PathBuf,
        size: u64,
    },
}

impl UploadArchiveEntry {
    /// Every directory and regular file under `directory`, depth first in name order.
    fn collect(directory: &Path) -> std::io::Result<Vec<Self>> {
        let mut entries = Vec::new();
        Self::collect_into(directory, directory, &mut entries)?;
        Ok(entries)
    }

    fn collect_into(root: &Path, current: &Path, entries: &mut Vec<Self>) -> std::io::Result<()> {
        let mut directory_entries = std::fs::read_dir(current)?.collect::<Result<Vec<_>, _>>()?;
        directory_entries.sort_by_key(|entry| entry.file_name());
        for entry in directory_entries {
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .map_err(std::io::Error::other)?
                .to_path_buf();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                entries.push(Self::Directory { relative });
                Self::collect_into(root, &path, entries)?;
            } else if file_type.is_file() {
                let size = std::fs::metadata(&path)?.len();
                entries.push(Self::File {
                    full_path: path,
                    relative,
                    size,
                });
            }
        }
        Ok(())
    }
}

/// One attempt to stream an archive: its start frame, then the archive in chunks.
struct UploadAttempt {
    channel: Channel,
    limits: SessionLimits,
    start: UploadStart,
    archive: File,
}

enum UploadSendFailure {
    BoundsChanged(SessionLimits),
    Encode(Report<WireEncodeError>),
    Transport(Box<Status>),
}

impl UploadAttempt {
    /// Streams the attempt and waits for the one reply that answers it.
    async fn send(
        self,
        connector: &GrpcConnector,
        on_progress: impl Fn(u64) + Send + Sync + 'static,
    ) -> Result<Response<VerifiedFrame<UploadReplyFrame>>, UploadSendFailure> {
        let Self {
            channel,
            limits,
            start,
            mut archive,
        } = self;
        let (frames, outbound) =
            mpsc::channel(connector.capabilities().upload_frame_capacity().get());
        let mut channel = channel;
        crate::session_open::ready(&mut channel)
            .await
            .map_err(|error| {
                UploadSendFailure::Transport(Box::new(Status::from_error(Box::new(error))))
            })?;
        let mut request = Request::new(ReceiverStream::new(outbound));
        connector.authorize(&mut request);
        let start = start.encode(&limits).map_err(UploadSendFailure::Encode)?;
        nervix_primitives::task::spawn(async move {
            if frames.send(start).await.is_err() {
                // The call ended before it took the start frame, and its status says why.
                return;
            }
            let mut buffer = vec![0_u8; UPLOAD_CHUNK_BYTES.min(limits.frame_bytes() / 2)];
            loop {
                nervix_primitives::task::consume_budget().await;
                let read = match archive.read(&mut buffer).await {
                    Ok(read) => read,
                    // Ending the stream early leaves the archive short of its declared size,
                    // which the server answers as a failed upload.
                    Err(error) => {
                        Err::<(), _>(error).reported("read resource upload archive");
                        return;
                    }
                };
                if read == 0 {
                    break;
                }
                on_progress(read.arch_into());
                let chunk = UploadChunk::encode(&buffer[..read], &limits).assured(
                    "a non-empty chunk of at most half the announced frame limit always encodes",
                );
                if frames.send(chunk).await.is_err() {
                    // The call ended, and its status says why.
                    return;
                }
            }
        });
        let opening = crate::session_open::open_offered::<UploadFrame, UploadReplyFrame>(
            &mut channel,
            request,
            PathAndQuery::from_static(UPLOAD_RESOURCE_PATH),
            limits,
        )
        .await
        .map_err(|status| UploadSendFailure::Transport(Box::new(status)))?;
        let opened = opening.map_err(UploadSendFailure::BoundsChanged)?;
        let (metadata, mut body, extensions) = opened.into_response().into_parts();
        let mut headers = metadata.into_headers();
        let received = body.message().await;
        let message = match received {
            Ok(message) => message,
            Err(mut status) => {
                let mut failure_headers = std::mem::take(status.metadata_mut()).into_headers();
                failure_headers.extend(headers.clone());
                *status.metadata_mut() =
                    tonic::metadata::MetadataMap::from_headers(failure_headers);
                return Err(UploadSendFailure::Transport(Box::new(status)));
            }
        };
        let message = message.ok_or_else(|| {
            UploadSendFailure::Transport(Box::new(Status::internal("Missing response message.")))
        })?;
        if let Some(trailers) = body
            .trailers()
            .await
            .map_err(|status| UploadSendFailure::Transport(Box::new(status)))?
        {
            headers.extend(trailers.into_headers());
        }
        Ok(Response::from_parts(
            tonic::metadata::MetadataMap::from_headers(headers),
            message,
            extensions,
        ))
    }
}

pub(crate) fn expand_user_path(path: &Path) -> PathBuf {
    let Some(raw) = path.to_str() else {
        return path.to_path_buf();
    };
    if raw == "~" {
        return match std::env::var_os("HOME") {
            Some(home) => PathBuf::from(home),
            None => path.to_path_buf(),
        };
    }
    if let Some(stripped) = raw.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(stripped);
    }
    path.to_path_buf()
}

/// Whether an upload that ended with `status` may have been installed without its reply
/// arriving, so the same upload identity is sent again to recover its outcome.
pub(crate) fn upload_status_is_retryable(status: &Status) -> bool {
    if let tonic::Code::Cancelled
    | tonic::Code::Unknown
    | tonic::Code::DeadlineExceeded
    | tonic::Code::Unavailable = status.code()
    {
        return true;
    }
    false
}

#[cfg(all(test, not(feature = "shuttle")))]
#[path = "upload_tests.rs"]
mod tests;
