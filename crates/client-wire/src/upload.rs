//! Resource upload frames: the stream that carries an archive and the reply that answers it.

use std::num::NonZeroU64;

use bytes::Bytes;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_models::{DomainName, ResourceName, ResourceUploadIdentity};

use crate::{
    codec::{DecodeError, Decoder, EncodeError, EncodedUnion, Encoder, wire_enum},
    common::{Diagnostic, LeaderRedirect, OutcomeOrigin, RequestId},
    frame::{EncodedFrame, UploadFrame, UploadReplyFrame, VerifiedFrame},
    limits::SessionLimits,
    wire,
};

/// The first frame of an upload stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadStart {
    pub request_id: RequestId,
    pub domain: DomainName,
    pub resource: ResourceName,
    /// The stable identity of this upload across retries.
    pub upload_identity: ResourceUploadIdentity,
    /// The exact archive size the chunks add up to.
    pub total_bytes: NonZeroU64,
}

impl UploadStart {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<UploadFrame>, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let domain = encoder.text("UploadStart.domain", self.domain.as_str())?;
        let resource = encoder.text("UploadStart.resource", self.resource.as_str())?;
        let upload_identity =
            encoder.text("UploadStart.upload_identity", self.upload_identity.as_str())?;
        let start = wire::UploadStart::create(
            encoder.fbb(),
            &wire::UploadStartArgs {
                request_id: self.request_id.wire(),
                domain: Some(domain),
                resource: Some(resource),
                upload_identity: Some(upload_identity),
                total_bytes: self.total_bytes.get(),
            },
        );
        finish_upload_message(
            encoder,
            EncodedUnion::new(wire::UploadPart::UploadStart, start),
        )
    }

    fn decode(
        decoder: Decoder<'_>,
        start: wire::UploadStart<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let request_id = RequestId::decode(decoder, "UploadStart.request_id", start.request_id())?;
        let domain = decoder.name("UploadStart.domain", start.domain())?;
        let resource = decoder.name("UploadStart.resource", start.resource())?;
        let upload_identity = decode_upload_identity(
            decoder,
            "UploadStart.upload_identity",
            start.upload_identity(),
        )?;
        let total_bytes = decoder.non_zero("UploadStart.total_bytes", start.total_bytes())?;
        Ok(Self {
            request_id,
            domain,
            resource,
            upload_identity,
            total_bytes,
        })
    }
}

/// Archive bytes following the upload start, read in place from their frame.
#[derive(Debug, Clone)]
pub struct UploadChunk {
    frame: VerifiedFrame<UploadFrame>,
}

impl UploadChunk {
    pub fn encode(
        bytes: &[u8],
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<UploadFrame>, Report<EncodeError>> {
        if bytes.is_empty() {
            return Err(Report::new(EncodeError::EmptyCollection {
                field: "UploadChunk.bytes",
            }));
        }
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let bytes = encoder.bytes("UploadChunk.bytes", bytes)?;
        let chunk =
            wire::UploadChunk::create(encoder.fbb(), &wire::UploadChunkArgs { bytes: Some(bytes) });
        finish_upload_message(
            encoder,
            EncodedUnion::new(wire::UploadPart::UploadChunk, chunk),
        )
    }

    fn decode(
        frame: &VerifiedFrame<UploadFrame>,
        chunk: wire::UploadChunk<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        if chunk.bytes().is_empty() {
            return Err(Report::new(DecodeError::EmptyCollection {
                field: "UploadChunk.bytes",
            }));
        }
        Ok(Self {
            frame: frame.clone(),
        })
    }

    /// The chunk's bytes, borrowed from its frame.
    pub fn bytes(&self) -> &[u8] {
        let chunk = self
            .frame
            .root()
            .part_as_upload_chunk()
            .assured("this value is only decoded from a frame whose part is a chunk");
        chunk.bytes().bytes()
    }

    /// The chunk's bytes as shared bytes that keep the whole frame alive, without copying.
    pub fn shared_bytes(&self) -> Bytes {
        self.frame.bytes().slice_ref(self.bytes())
    }
}

/// Everything an upload stream frame can hold.
#[derive(Debug, Clone)]
pub enum UploadMessage {
    Start(UploadStart),
    Chunk(UploadChunk),
}

impl UploadMessage {
    pub fn decode(frame: &VerifiedFrame<UploadFrame>) -> Result<Self, Report<DecodeError>> {
        let decoder = Decoder::new(frame.limits());
        let message = frame.root();
        if let Some(start) = message.part_as_upload_start() {
            return Ok(Self::Start(UploadStart::decode(decoder, start)?));
        }
        if let Some(chunk) = message.part_as_upload_chunk() {
            return Ok(Self::Chunk(UploadChunk::decode(frame, chunk)?));
        }
        Err(decoder.unknown_union("UploadMessage.part", message.part_type().0))
    }
}

fn finish_upload_message(
    mut encoder: Encoder<'_>,
    part: EncodedUnion<wire::UploadPart>,
) -> Result<EncodedFrame<UploadFrame>, Report<EncodeError>> {
    let message = wire::UploadMessage::create(
        encoder.fbb(),
        &wire::UploadMessageArgs {
            part_type: part.discriminant,
            part: Some(part.value),
        },
    );
    encoder.finish::<UploadFrame>(message)
}

/// Why an upload failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UploadFailure {
    /// The stream did not begin with a valid upload start, or a later frame was not a chunk.
    InvalidStream,
    ResourceNotDeclared,
    /// The chunks did not add up to the declared archive size.
    SizeMismatch,
    QuotaExceeded,
    InstallationFailed,
}

wire_enum!(ALL_UPLOAD_FAILURES: UploadFailure => wire::UploadFailure {
    InvalidStream,
    ResourceNotDeclared,
    SizeMismatch,
    QuotaExceeded,
    InstallationFailed,
});

/// What became of an upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadDisposition {
    Installed {
        upload_identity: ResourceUploadIdentity,
        version: NonZeroU64,
        origin: OutcomeOrigin,
    },
    Failed {
        /// The upload's identity, when the stream carried a valid one.
        upload_identity: Option<ResourceUploadIdentity>,
        failure: UploadFailure,
        /// The version assigned before installation failed, when one was.
        assigned_version: Option<NonZeroU64>,
    },
    NotLeader(LeaderRedirect),
}

/// The frame that answers an upload stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadReply {
    /// The request identity of the stream's upload start, when the stream carried one.
    pub request_id: Option<RequestId>,
    pub disposition: UploadDisposition,
    pub message: String,
    pub diagnostics: Vec<Diagnostic>,
}

impl UploadReply {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<UploadReplyFrame>, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let disposition = match &self.disposition {
            UploadDisposition::Installed {
                upload_identity,
                version,
                origin,
            } => {
                let upload_identity = encoder.text(
                    "ResourceInstalled.upload_identity",
                    upload_identity.as_str(),
                )?;
                let installed = wire::ResourceInstalled::create(
                    encoder.fbb(),
                    &wire::ResourceInstalledArgs {
                        upload_identity: Some(upload_identity),
                        version: version.get(),
                        origin: Some((*origin).into()),
                    },
                );
                EncodedUnion::new(wire::UploadDisposition::ResourceInstalled, installed)
            }
            UploadDisposition::Failed {
                upload_identity,
                failure,
                assigned_version,
            } => {
                let upload_identity = match upload_identity {
                    Some(identity) => {
                        Some(encoder.text("UploadFailed.upload_identity", identity.as_str())?)
                    }
                    None => None,
                };
                let assigned_version = assigned_version.map(NonZeroU64::get);
                let failed = wire::UploadFailed::create(
                    encoder.fbb(),
                    &wire::UploadFailedArgs {
                        upload_identity,
                        failure: Some((*failure).into()),
                        assigned_version,
                    },
                );
                EncodedUnion::new(wire::UploadDisposition::UploadFailed, failed)
            }
            UploadDisposition::NotLeader(redirect) => EncodedUnion::new(
                wire::UploadDisposition::LeaderRedirect,
                redirect.encode(&mut encoder)?,
            ),
        };
        let message = encoder.text("UploadReply.message", &self.message)?;
        let diagnostics =
            Diagnostic::encode_all(&mut encoder, "UploadReply.diagnostics", &self.diagnostics)?;
        let request_id = self.request_id.map(RequestId::wire);
        let reply = wire::UploadReply::create(
            encoder.fbb(),
            &wire::UploadReplyArgs {
                request_id,
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
                diagnostics: Some(diagnostics),
            },
        );
        encoder.finish::<UploadReplyFrame>(reply)
    }

    pub fn decode(frame: &VerifiedFrame<UploadReplyFrame>) -> Result<Self, Report<DecodeError>> {
        let decoder = Decoder::new(frame.limits());
        let reply = frame.root();
        let request_id = match reply.request_id() {
            Some(request_id) => Some(RequestId::decode(
                decoder,
                "UploadReply.request_id",
                request_id,
            )?),
            None => None,
        };
        let disposition = if let Some(installed) = reply.disposition_as_resource_installed() {
            UploadDisposition::Installed {
                upload_identity: decode_upload_identity(
                    decoder,
                    "ResourceInstalled.upload_identity",
                    installed.upload_identity(),
                )?,
                version: decoder.non_zero("ResourceInstalled.version", installed.version())?,
                origin: decoder
                    .required_enumeration("ResourceInstalled.origin", installed.origin())?,
            }
        } else if let Some(failed) = reply.disposition_as_upload_failed() {
            let upload_identity = match failed.upload_identity() {
                Some(identity) => Some(decode_upload_identity(
                    decoder,
                    "UploadFailed.upload_identity",
                    identity,
                )?),
                None => None,
            };
            let assigned_version = match failed.assigned_version() {
                Some(version) => Some(decoder.non_zero("UploadFailed.assigned_version", version)?),
                None => None,
            };
            UploadDisposition::Failed {
                upload_identity,
                failure: decoder.required_enumeration("UploadFailed.failure", failed.failure())?,
                assigned_version,
            }
        } else if let Some(redirect) = reply.disposition_as_leader_redirect() {
            UploadDisposition::NotLeader(LeaderRedirect::decode(decoder, redirect)?)
        } else {
            return Err(
                decoder.unknown_union("UploadReply.disposition", reply.disposition_type().0)
            );
        };
        let message = decoder.text("UploadReply.message", reply.message())?;
        let diagnostics =
            Diagnostic::decode_all(decoder, "UploadReply.diagnostics", reply.diagnostics())?;
        Ok(Self {
            request_id,
            disposition,
            message,
            diagnostics,
        })
    }
}

fn decode_upload_identity(
    decoder: Decoder<'_>,
    field: &'static str,
    value: &str,
) -> Result<ResourceUploadIdentity, Report<DecodeError>> {
    let value = decoder.check_text(field, value)?;
    match ResourceUploadIdentity::parse(value) {
        Ok(identity) => Ok(identity),
        Err(error) => Err(
            Report::new(error).change_context(DecodeError::InvalidValue {
                field,
                kind: "upload identity",
            }),
        ),
    }
}
