//! Restores: the stream that carries an archive to the leader, the reply that answers it, and the
//! report a restore's outcome carries.

use bytes::Bytes;
use error_stack::Report;
use flatbuffers::WIPOffset;
use meticulous::OptionExt as _;
use nervix_models::{
    ArchiveDigest, CommandExecutionReference, RestoreArchive, RestoreMode, RestoreReport,
    RestoreStep, RestoreStepOutcome, RestoreStepReport, RestoredDomain, RestoredUsers, Timestamp,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    command::CommandOutcome,
    common::RequestId,
    frame::{EncodedFrame, RestoreFrame, RestoreReplyFrame, VerifiedFrame},
    impact::{decode_report, encode_report},
    limits::SessionLimits,
    wire,
};

wire_enum!(ALL_RESTORE_MODES: RestoreMode => wire::RestoreMode {
    Apply,
    DryRun,
});

/// What a step of a restore changes, as the wire names it beside the domain it changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum RestoreStepKind {
    Users,
    CreateDomain,
    ImportResources,
    ApplyModels,
}

wire_enum!(ALL_RESTORE_STEP_KINDS: RestoreStepKind => wire::RestoreStepKind {
    Users,
    CreateDomain,
    ImportResources,
    ApplyModels,
});

wire_enum!(ALL_RESTORE_STEP_OUTCOMES: RestoreStepOutcome => wire::RestoreStepOutcome {
    Applied,
    Planned,
    Failed,
    NotAttempted,
});

/// The first frame of a restore stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreStart {
    pub request_id: RequestId,
    /// The durable identity of the restore across retries, reconnects and leader changes.
    pub execution_reference: CommandExecutionReference,
    /// One `RESTORE` statement, as NSPL.
    pub statement: String,
    /// The archive's exact size and digest.
    pub archive: RestoreArchive,
}

impl RestoreStart {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<RestoreFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let execution_reference = encoder.text(
            "RestoreStart.execution_reference",
            self.execution_reference.as_str(),
        )?;
        let statement = encoder.text("RestoreStart.statement", &self.statement)?;
        let digest = encoder.fingerprint("RestoreStart.digest", self.archive.digest.as_bytes())?;
        let start = wire::RestoreStart::create(
            encoder.fbb(),
            &wire::RestoreStartArgs {
                request_id: self.request_id.wire(),
                execution_reference: Some(execution_reference),
                statement: Some(statement),
                total_bytes: self.archive.total_bytes.get(),
                digest: Some(digest),
            },
        );
        finish_restore_message(
            encoder,
            EncodedUnion::new(wire::RestorePart::RestoreStart, start),
        )
    }

    fn decode(
        decoder: Decoder<'_>,
        start: wire::RestoreStart<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let request_id = RequestId::decode(decoder, "RestoreStart.request_id", start.request_id())?;
        let execution_reference = decode_execution_reference(
            decoder,
            "RestoreStart.execution_reference",
            start.execution_reference(),
        )?;
        let statement = decoder.text("RestoreStart.statement", start.statement())?;
        let total_bytes = decoder.non_zero("RestoreStart.total_bytes", start.total_bytes())?;
        let digest = decoder.fingerprint("RestoreStart.digest", start.digest())?;
        Ok(Self {
            request_id,
            execution_reference,
            statement,
            archive: RestoreArchive {
                total_bytes,
                digest: ArchiveDigest::from_bytes(digest),
            },
        })
    }
}

/// Archive bytes following the restore start, read in place from their frame.
#[derive(Debug, Clone)]
pub struct RestoreChunk {
    frame: VerifiedFrame<RestoreFrame>,
}

impl RestoreChunk {
    pub fn encode(
        bytes: &[u8],
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<RestoreFrame>, Report<WireEncodeError>> {
        if bytes.is_empty() {
            return Err(Report::new(WireEncodeError::EmptyCollection {
                field: "RestoreChunk.bytes",
            }));
        }
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let bytes = encoder.bytes("RestoreChunk.bytes", bytes)?;
        let chunk = wire::RestoreChunk::create(
            encoder.fbb(),
            &wire::RestoreChunkArgs { bytes: Some(bytes) },
        );
        finish_restore_message(
            encoder,
            EncodedUnion::new(wire::RestorePart::RestoreChunk, chunk),
        )
    }

    fn decode(
        frame: &VerifiedFrame<RestoreFrame>,
        chunk: wire::RestoreChunk<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        if chunk.bytes().is_empty() {
            return Err(Report::new(WireDecodeError::EmptyCollection {
                field: "RestoreChunk.bytes",
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
            .part_as_restore_chunk()
            .assured("this value is only decoded from a frame whose part is a chunk");
        chunk.bytes().bytes()
    }

    /// The chunk's bytes as shared bytes that keep the whole frame alive, without copying.
    pub fn shared_bytes(&self) -> Bytes {
        self.frame.bytes().slice_ref(self.bytes())
    }
}

/// Everything a restore stream frame can hold.
#[derive(Debug, Clone)]
pub enum RestoreMessage {
    Start(RestoreStart),
    Chunk(RestoreChunk),
}

impl RestoreMessage {
    pub fn decode(frame: &VerifiedFrame<RestoreFrame>) -> Result<Self, Report<WireDecodeError>> {
        let decoder = Decoder::new(frame.limits());
        let message = frame.root();
        if let Some(start) = message.part_as_restore_start() {
            return Ok(Self::Start(RestoreStart::decode(decoder, start)?));
        }
        if let Some(chunk) = message.part_as_restore_chunk() {
            return Ok(Self::Chunk(RestoreChunk::decode(frame, chunk)?));
        }
        Err(decoder.unknown_union("RestoreMessage.part", message.part_type().0))
    }
}

fn finish_restore_message(
    mut encoder: Encoder<'_>,
    part: EncodedUnion<wire::RestorePart>,
) -> Result<EncodedFrame<RestoreFrame>, Report<WireEncodeError>> {
    let message = wire::RestoreMessage::create(
        encoder.fbb(),
        &wire::RestoreMessageArgs {
            part_type: part.discriminant,
            part: Some(part.value),
        },
    );
    encoder.finish::<RestoreFrame>(message)
}

/// Why a restore stream was refused before its restore ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestoreUploadFailure {
    /// The stream did not begin with a valid restore start, or a later frame was not a chunk.
    InvalidStream,
    /// The start's statement is not one `RESTORE` statement.
    InvalidStatement,
    /// The chunks did not add up to the declared archive size.
    SizeMismatch,
    /// The chunks do not have the declared digest.
    DigestMismatch,
    /// The declared size exceeds what the leader stages for one archive.
    QuotaExceeded,
    /// The leader could not stage the archive.
    StagingFailed,
}

wire_enum!(ALL_RESTORE_UPLOAD_FAILURES: RestoreUploadFailure => wire::RestoreUploadFailure {
    InvalidStream,
    InvalidStatement,
    SizeMismatch,
    DigestMismatch,
    QuotaExceeded,
    StagingFailed,
});

/// What answers a restore stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreDisposition {
    /// The outcome of the `RESTORE` the stream carried, as the command it is.
    Outcome(Box<CommandOutcome>),
    /// The stream itself was refused, and no restore ran.
    UploadFailed {
        failure: RestoreUploadFailure,
        message: String,
    },
}

/// The frame that answers a restore stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReply {
    /// The request identity of the stream's restore start, when the stream carried one.
    pub request_id: Option<RequestId>,
    pub disposition: RestoreDisposition,
}

impl RestoreReply {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<RestoreReplyFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let disposition = match &self.disposition {
            RestoreDisposition::Outcome(outcome) => EncodedUnion::new(
                wire::RestoreDisposition::CommandOutcome,
                outcome.encode_table(&mut encoder)?,
            ),
            RestoreDisposition::UploadFailed { failure, message } => {
                let message = encoder.text("RestoreUploadFailed.message", message)?;
                let failed = wire::RestoreUploadFailed::create(
                    encoder.fbb(),
                    &wire::RestoreUploadFailedArgs {
                        failure: Some((*failure).into()),
                        message: Some(message),
                    },
                );
                EncodedUnion::new(wire::RestoreDisposition::RestoreUploadFailed, failed)
            }
        };
        let request_id = self.request_id.map(RequestId::wire);
        let reply = wire::RestoreReply::create(
            encoder.fbb(),
            &wire::RestoreReplyArgs {
                request_id,
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
            },
        );
        encoder.finish::<RestoreReplyFrame>(reply)
    }

    pub fn decode(
        frame: &VerifiedFrame<RestoreReplyFrame>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let decoder = Decoder::new(frame.limits());
        let reply = frame.root();
        let request_id = match reply.request_id() {
            Some(request_id) => Some(RequestId::decode(
                decoder,
                "RestoreReply.request_id",
                request_id,
            )?),
            None => None,
        };
        let disposition = if let Some(outcome) = reply.disposition_as_command_outcome() {
            RestoreDisposition::Outcome(Box::new(CommandOutcome::decode(decoder, outcome)?))
        } else if let Some(failed) = reply.disposition_as_restore_upload_failed() {
            RestoreDisposition::UploadFailed {
                failure: decoder
                    .required_enumeration("RestoreUploadFailed.failure", failed.failure())?,
                message: decoder.text("RestoreUploadFailed.message", failed.message())?,
            }
        } else {
            return Err(
                decoder.unknown_union("RestoreReply.disposition", reply.disposition_type().0)
            );
        };
        Ok(Self {
            request_id,
            disposition,
        })
    }
}

pub(crate) fn encode_restore_report<'fbb>(
    encoder: &mut Encoder<'fbb>,
    report: &RestoreReport,
) -> Result<WIPOffset<wire::RestoreReport<'fbb>>, Report<WireEncodeError>> {
    let digest = encoder.fingerprint("RestoreReport.digest", report.archive.digest.as_bytes())?;
    let users = report.users.as_ref().map(|users| {
        wire::RestoredUsers::create(
            encoder.fbb(),
            &wire::RestoredUsersArgs {
                created: users.created,
                skipped: users.skipped,
                replaced: users.replaced,
            },
        )
    });
    let domains = encoder.table_vector(
        "RestoreReport.domains",
        &report.domains,
        encode_restored_domain,
    )?;
    let steps = encoder.table_vector("RestoreReport.steps", &report.steps, encode_step_report)?;
    Ok(wire::RestoreReport::create(
        encoder.fbb(),
        &wire::RestoreReportArgs {
            mode: Some(report.mode.into()),
            total_bytes: report.archive.total_bytes.get(),
            digest: Some(digest),
            captured_at: report.captured_at.unix_nanos(),
            users,
            domains: Some(domains),
            steps: Some(steps),
        },
    ))
}

fn encode_restored_domain<'fbb>(
    domain: &RestoredDomain,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::RestoredDomain<'fbb>>, Report<WireEncodeError>> {
    let source = encoder.text("RestoredDomain.source", domain.source.as_str())?;
    let name = encoder.text("RestoredDomain.domain", domain.domain.as_str())?;
    let planned_models = match &domain.planned_models {
        Some(report) => Some(encode_report(encoder, report)?),
        None => None,
    };
    Ok(wire::RestoredDomain::create(
        encoder.fbb(),
        &wire::RestoredDomainArgs {
            source: Some(source),
            domain: Some(name),
            resource_versions: domain.resource_versions,
            models: domain.models,
            status: Some(domain.status.clone().into()),
            start_version: Some(domain.start_version),
            planned_models,
        },
    ))
}

fn encode_step_report<'fbb>(
    report: &RestoreStepReport,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::RestoreStepReport<'fbb>>, Report<WireEncodeError>> {
    let (kind, domain) = match &report.step {
        RestoreStep::Users => (RestoreStepKind::Users, None),
        RestoreStep::CreateDomain(domain) => (RestoreStepKind::CreateDomain, Some(domain)),
        RestoreStep::ImportResources(domain) => (RestoreStepKind::ImportResources, Some(domain)),
        RestoreStep::ApplyModels(domain) => (RestoreStepKind::ApplyModels, Some(domain)),
    };
    let domain = match domain {
        Some(domain) => Some(encoder.text("RestoreStepReport.domain", domain.as_str())?),
        None => None,
    };
    Ok(wire::RestoreStepReport::create(
        encoder.fbb(),
        &wire::RestoreStepReportArgs {
            kind: Some(kind.into()),
            domain,
            outcome: Some(report.outcome.into()),
        },
    ))
}

pub(crate) fn decode_restore_report(
    decoder: Decoder<'_>,
    report: wire::RestoreReport<'_>,
) -> Result<RestoreReport, Report<WireDecodeError>> {
    let mode = decoder.required_enumeration("RestoreReport.mode", report.mode())?;
    let total_bytes = decoder.non_zero("RestoreReport.total_bytes", report.total_bytes())?;
    let digest = decoder.fingerprint("RestoreReport.digest", report.digest())?;
    let users = report.users().map(|users| RestoredUsers {
        created: users.created(),
        skipped: users.skipped(),
        replaced: users.replaced(),
    });
    let domains = decoder.table_vector("RestoreReport.domains", report.domains(), |domain| {
        decode_restored_domain(decoder, domain)
    })?;
    let steps = decoder.table_vector("RestoreReport.steps", report.steps(), |step| {
        decode_step_report(decoder, step)
    })?;
    Ok(RestoreReport {
        mode,
        archive: RestoreArchive {
            total_bytes,
            digest: ArchiveDigest::from_bytes(digest),
        },
        captured_at: Timestamp::from_unix_nanos(report.captured_at()),
        users,
        domains,
        steps,
    })
}

fn decode_restored_domain(
    decoder: Decoder<'_>,
    domain: wire::RestoredDomain<'_>,
) -> Result<RestoredDomain, Report<WireDecodeError>> {
    let planned_models = match domain.planned_models() {
        Some(report) => Some(decode_report(decoder, report)?),
        None => None,
    };
    Ok(RestoredDomain {
        source: decoder.name("RestoredDomain.source", domain.source())?,
        domain: decoder.name("RestoredDomain.domain", domain.domain())?,
        resource_versions: domain.resource_versions(),
        models: domain.models(),
        status: decoder.required_enumeration("RestoredDomain.status", domain.status())?,
        start_version: decoder.required("RestoredDomain.start_version", domain.start_version())?,
        planned_models,
    })
}

fn decode_step_report(
    decoder: Decoder<'_>,
    report: wire::RestoreStepReport<'_>,
) -> Result<RestoreStepReport, Report<WireDecodeError>> {
    let kind = decoder.required_enumeration("RestoreStepReport.kind", report.kind())?;
    let domain = decoder.optional_name("RestoreStepReport.domain", report.domain())?;
    let step = match (kind, domain) {
        (RestoreStepKind::Users, None) => RestoreStep::Users,
        (RestoreStepKind::CreateDomain, Some(domain)) => RestoreStep::CreateDomain(domain),
        (RestoreStepKind::ImportResources, Some(domain)) => RestoreStep::ImportResources(domain),
        (RestoreStepKind::ApplyModels, Some(domain)) => RestoreStep::ApplyModels(domain),
        (
            RestoreStepKind::Users
            | RestoreStepKind::CreateDomain
            | RestoreStepKind::ImportResources
            | RestoreStepKind::ApplyModels,
            _,
        ) => {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "RestoreStepReport.domain",
                kind: "a domain for every step but the users step",
            }));
        }
    };
    let outcome = decoder.required_enumeration("RestoreStepReport.outcome", report.outcome())?;
    Ok(RestoreStepReport { step, outcome })
}

fn decode_execution_reference(
    decoder: Decoder<'_>,
    field: &'static str,
    value: &str,
) -> Result<CommandExecutionReference, Report<WireDecodeError>> {
    let value = decoder.check_text(field, value)?;
    match CommandExecutionReference::parse(value) {
        Ok(reference) => Ok(reference),
        Err(error) => Err(error.change_context(WireDecodeError::InvalidValue {
            field,
            kind: "execution reference",
        })),
    }
}
