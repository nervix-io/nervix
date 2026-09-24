//! Resource upload: the archive a directory becomes, and the stream that carries it to a server.
//!
//! - **Owns.** Building the deterministic archive of a directory, streaming it as upload frames,
//!   checking the reply against the upload it answers, and the routing that reply calls for.
//! - **Depends on.** The wire contract's upload frames, tonic's gRPC client, and the client's
//!   session recovery and leader routing.
//! - **Must not know.** How the server installs a resource.

use std::{
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use arch_into::ArchInto as _;
use async_tar::{Builder as AsyncTarBuilder, EntryType, Header, HeaderMode};
use meticulous::ResultExt as _;
use nervix_client_wire::{
    EncodedFrame, RequestId, UploadChunk, UploadDisposition, UploadFrame, UploadReply,
    UploadReplyFrame, UploadStart, VerifiedFrame,
    grpc::{ClientUploadCodec, UPLOAD_RESOURCE_PATH},
};
use nervix_models::{ResourceName, ResourceUploadIdentity};
use tempfile::TempPath;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, codegen::http::uri::PathAndQuery, transport::Channel};

use crate::{
    client::{Client, SessionRecovery},
    connection::GrpcConnector,
    error::{ClientError, RequestKind},
    exchange::SESSION_LIMITS,
    outcome::{CommandOutcome, ResourceUploadOutcome, Routing},
};

/// Archive bytes one upload frame carries.
const UPLOAD_CHUNK_BYTES: usize = 64 * 1024;

// A chunk fills at most half a frame, far more room than its frame's envelope needs, so a
// non-empty chunk always encodes.
const _: () = assert!(UPLOAD_CHUNK_BYTES <= SESSION_LIMITS.frame_bytes() / 2);

/// Upload frames queued for the stream before the archive reader waits for the transport.
const UPLOAD_FRAME_CAPACITY: usize = 8;

/// The request identity of an upload's start frame. Every attempt is a stream of its own that
/// exactly one reply answers, so the identity only has to be non-zero.
const UPLOAD_REQUEST_ID: RequestId = RequestId::new(NonZeroU64::MIN);

impl Client {
    pub async fn upload_resource_from_directory(
        &self,
        identifier: &str,
        directory: impl AsRef<Path>,
        on_progress: impl Fn(u64) + Send + Sync + Clone + 'static,
    ) -> Result<CommandOutcome, ClientError> {
        self.upload_resource_from_directory_with_identity(
            identifier,
            directory,
            ResourceUploadIdentity::parse(uuid::Uuid::now_v7().to_string())
                .assured("a UUID string satisfies the upload identity grammar"),
            on_progress,
        )
        .await
    }

    /// Uploads `directory` as a version of the resource `identifier` names. `upload_identity`
    /// stays the same across every retry and redirect, so an upload the server already installed
    /// is recovered rather than installed twice.
    pub async fn upload_resource_from_directory_with_identity(
        &self,
        identifier: &str,
        directory: impl AsRef<Path>,
        upload_identity: ResourceUploadIdentity,
        on_progress: impl Fn(u64) + Send + Sync + Clone + 'static,
    ) -> Result<CommandOutcome, ClientError> {
        let resource =
            ResourceName::parse(identifier).map_err(|report| ClientError::InvalidResourceName {
                name: identifier.to_string(),
                source: report.current_context().clone(),
            })?;
        let archive = UploadArchive::build(directory.as_ref()).await?;
        for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
            tokio::task::consume_budget().await;
            let Some(domain) = self.domain().await else {
                return Err(ClientError::NoActiveDomain);
            };
            let start = UploadStart {
                request_id: UPLOAD_REQUEST_ID,
                domain,
                resource: resource.clone(),
                upload_identity: upload_identity.clone(),
                total_bytes: archive.total_bytes,
            }
            .encode(&SESSION_LIMITS)
            .map_err(|report| ClientError::EncodeRequest {
                request: RequestKind::UploadResource,
                source: report.current_context().clone(),
            })?;
            let reader = File::open(&archive.path)
                .await
                .map_err(|_| ClientError::BuildUploadArchive)?;
            let stream = UploadAttempt {
                channel: self.current_channel().await,
                start,
                archive: reader,
            };
            let response = match stream
                .send(&self.inner.connector, on_progress.clone())
                .await
            {
                Ok(response) => response,
                Err(status)
                    if upload_status_is_retryable(&status) && Self::await_retry(attempt).await =>
                {
                    match self.recover_session().await? {
                        SessionRecovery::Ready => continue,
                        SessionRecovery::Unavailable => {
                            return Err(ClientError::UploadResource(status));
                        }
                    }
                }
                Err(status) => return Err(ClientError::UploadResource(status)),
            };
            let reply = UploadReply::decode(response.get_ref()).map_err(|report| {
                ClientError::InvalidUploadReply(report.current_context().clone())
            })?;
            if let Some(request_id) = reply.request_id
                && request_id != UPLOAD_REQUEST_ID
            {
                return Err(ClientError::UnexpectedReply {
                    request: RequestKind::UploadResource,
                });
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
                && received != &upload_identity
            {
                return Err(ClientError::UploadIdentityMismatch {
                    expected: upload_identity,
                    received: received.clone(),
                });
            }
            let outcome = CommandOutcome::from_upload(reply, upload_identity.clone());
            match outcome.routing() {
                Routing::Redirect(leader) => self.follow_leader(leader).await?,
                Routing::AwaitElection if Self::await_retry(attempt).await => {}
                _ => return Ok(outcome),
            }
        }

        let mut exhausted =
            CommandOutcome::failed_locally("upload redirect loop exceeded".to_string());
        exhausted.resource_upload = Some(ResourceUploadOutcome {
            identity: upload_identity,
            version: None,
            origin: None,
            failure: None,
        });
        Ok(exhausted)
    }
}

/// A directory's deterministic archive, written once to a temporary file and streamed from it on
/// every attempt.
struct UploadArchive {
    path: TempPath,
    total_bytes: NonZeroU64,
}

impl UploadArchive {
    async fn build(directory: &Path) -> Result<Self, ClientError> {
        let directory = expand_user_path(directory);
        if !directory.is_dir() {
            return Err(ClientError::BuildUploadArchive);
        }
        let path = tempfile::NamedTempFile::new()
            .map_err(|_| ClientError::BuildUploadArchive)?
            .into_temp_path();
        let file = File::create(&path)
            .await
            .map_err(|_| ClientError::BuildUploadArchive)?;
        Self::write(&directory, file)
            .await
            .map_err(|_| ClientError::BuildUploadArchive)?;
        let length = tokio::fs::metadata(&path)
            .await
            .map_err(|_| ClientError::BuildUploadArchive)?
            .len();
        let Some(total_bytes) = NonZeroU64::new(length) else {
            return Err(ClientError::BuildUploadArchive);
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
        let mut builder = AsyncTarBuilder::new(writer);
        builder.mode(HeaderMode::Deterministic);
        let mut result = Ok(());

        for entry in entries {
            tokio::task::consume_budget().await;
            let mut header = Header::new_ustar();
            header.set_mtime(0);
            header.set_uid(0);
            header.set_gid(0);
            let write_result = match entry {
                UploadArchiveEntry::Directory { relative } => {
                    header.set_size(0);
                    header.set_mode(0o755);
                    header.set_entry_type(EntryType::Directory);
                    header.set_cksum();
                    builder
                        .append_data(&mut header, &relative, tokio::io::empty())
                        .await
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
                    builder.append_data(&mut header, &relative, file).await
                }
            };

            if let Err(error) = write_result {
                result = Err(error);
                break;
            }
        }

        let mut writer = builder.into_inner().await?;
        let shutdown_result = writer.shutdown().await;
        result?;
        shutdown_result?;
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
    start: EncodedFrame<UploadFrame>,
    archive: File,
}

impl UploadAttempt {
    /// Streams the attempt and waits for the one reply that answers it.
    async fn send(
        self,
        connector: &GrpcConnector,
        on_progress: impl Fn(u64) + Send + Sync + 'static,
    ) -> Result<Response<VerifiedFrame<UploadReplyFrame>>, Box<Status>> {
        let Self {
            channel,
            start,
            mut archive,
        } = self;
        let (frames, outbound) = mpsc::channel(UPLOAD_FRAME_CAPACITY);
        tokio::spawn(async move {
            if frames.send(start).await.is_err() {
                // The call ended before it took the start frame, and its status says why.
                return;
            }
            let mut buffer = vec![0_u8; UPLOAD_CHUNK_BYTES];
            loop {
                tokio::task::consume_budget().await;
                let read = match archive.read(&mut buffer).await {
                    Ok(read) => read,
                    // Ending the stream early leaves the archive short of its declared size,
                    // which the server answers as a failed upload.
                    Err(_) => return,
                };
                if read == 0 {
                    break;
                }
                on_progress(read.arch_into());
                let chunk = UploadChunk::encode(&buffer[..read], &SESSION_LIMITS).assured(
                    "a non-empty chunk of at most half a frame, as a const assertion holds it, \
                     always encodes",
                );
                if frames.send(chunk).await.is_err() {
                    // The call ended, and its status says why.
                    return;
                }
            }
        });
        let mut client = tonic::client::Grpc::new(channel)
            .max_decoding_message_size(SESSION_LIMITS.frame_bytes())
            .max_encoding_message_size(SESSION_LIMITS.frame_bytes());
        client
            .ready()
            .await
            .map_err(|error| Box::new(Status::from_error(Box::new(error))))?;
        let mut request = Request::new(ReceiverStream::new(outbound));
        connector.authorize(&mut request);
        client
            .client_streaming(
                request,
                PathAndQuery::from_static(UPLOAD_RESOURCE_PATH),
                ClientUploadCodec::new(SESSION_LIMITS),
            )
            .await
            .map_err(Box::new)
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
