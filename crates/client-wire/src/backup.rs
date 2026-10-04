//! Backups: the summary a completed backup reports, and the stream that downloads its archive.

use std::num::NonZeroU64;

use bytes::Bytes;
use error_stack::Report;
use flatbuffers::WIPOffset;
use meticulous::OptionExt as _;
use nervix_models::{
    ArchiveDigest, BackupArchiveSummary, BackupCut, BackupCutKind, BackupDomainSummary,
    BackupQuiesceCounters, BackupResources, CommandExecutionReference, DomainName, Timestamp,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    common::LeaderRedirect,
    frame::{BackupDownloadFrame, BackupDownloadRequestFrame, EncodedFrame, VerifiedFrame},
    limits::SessionLimits,
    wire,
};

wire_enum!(ALL_BACKUP_RESOURCES: BackupResources => wire::BackupResources {
    Included,
    Omitted,
});

wire_enum!(ALL_BACKUP_CUT_KINDS: BackupCutKind => wire::BackupCutKind {
    Quiesced,
    Live,
    Stopped,
    ConfigurationOnly,
});

pub(crate) fn encode_backup_archive<'fbb>(
    encoder: &mut Encoder<'fbb>,
    archive: &BackupArchiveSummary,
) -> Result<WIPOffset<wire::BackupArchiveSummary<'fbb>>, Report<WireEncodeError>> {
    let digest = encoder.fingerprint("BackupArchiveSummary.digest", archive.digest.as_bytes())?;
    let domains = encoder.table_vector(
        "BackupArchiveSummary.domains",
        &archive.domains,
        encode_backup_domain,
    )?;
    Ok(wire::BackupArchiveSummary::create(
        encoder.fbb(),
        &wire::BackupArchiveSummaryArgs {
            total_bytes: archive.total_bytes.get(),
            digest: Some(digest),
            captured_at: archive.captured_at.unix_nanos(),
            retained_until: archive.retained_until.unix_nanos(),
            resources: Some(archive.resources.into()),
            users: archive.users,
            domains: Some(domains),
        },
    ))
}

fn encode_backup_domain<'fbb>(
    domain: &BackupDomainSummary,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::BackupDomainSummary<'fbb>>, Report<WireEncodeError>> {
    let name = encoder.text("BackupDomainSummary.domain", domain.domain.as_str())?;
    let (engaged_at, released_at, counters) = match domain.cut {
        BackupCut::Quiesced {
            engaged_at,
            released_at,
            quiesce,
        } => (
            Some(engaged_at.unix_nanos()),
            Some(released_at.unix_nanos()),
            quiesce,
        ),
        BackupCut::Live | BackupCut::Stopped | BackupCut::ConfigurationOnly => {
            (None, None, BackupQuiesceCounters::default())
        }
    };
    let cut = wire::BackupCut::create(
        encoder.fbb(),
        &wire::BackupCutArgs {
            kind: Some(domain.cut.kind().into()),
            engaged_at,
            released_at,
            buffered_records: counters.buffered_records,
            buffered_bytes: counters.buffered_bytes,
            dropped_records: counters.dropped_records,
            rejected_records: counters.rejected_records,
        },
    );
    Ok(wire::BackupDomainSummary::create(
        encoder.fbb(),
        &wire::BackupDomainSummaryArgs {
            domain: Some(name),
            revision: domain.revision,
            cut: Some(cut),
            sections: domain.sections,
            section_bytes: domain.section_bytes,
        },
    ))
}

/// Reads a backup summary. Its domains must be in strictly ascending name order, as the schema
/// requires, so no domain is reported twice.
pub(crate) fn decode_backup_archive(
    decoder: Decoder<'_>,
    archive: wire::BackupArchiveSummary<'_>,
) -> Result<BackupArchiveSummary, Report<WireDecodeError>> {
    let total_bytes =
        decoder.non_zero("BackupArchiveSummary.total_bytes", archive.total_bytes())?;
    let digest = decoder.fingerprint("BackupArchiveSummary.digest", archive.digest())?;
    let resources =
        decoder.required_enumeration("BackupArchiveSummary.resources", archive.resources())?;
    let domains = decoder.table_vector(
        "BackupArchiveSummary.domains",
        archive.domains(),
        |domain| decode_backup_domain(decoder, domain),
    )?;
    let mut previous: Option<&DomainName> = None;
    for domain in &domains {
        if let Some(previous) = previous
            && *previous >= domain.domain
        {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "BackupArchiveSummary.domains",
                kind: "strictly ascending domain names",
            }));
        }
        previous = Some(&domain.domain);
    }
    Ok(BackupArchiveSummary {
        total_bytes,
        digest: ArchiveDigest::from_bytes(digest),
        captured_at: Timestamp::from_unix_nanos(archive.captured_at()),
        retained_until: Timestamp::from_unix_nanos(archive.retained_until()),
        resources,
        users: archive.users(),
        domains,
    })
}

fn decode_backup_domain(
    decoder: Decoder<'_>,
    domain: wire::BackupDomainSummary<'_>,
) -> Result<BackupDomainSummary, Report<WireDecodeError>> {
    let cut = domain.cut();
    let kind: BackupCutKind = decoder.required_enumeration("BackupCut.kind", cut.kind())?;
    let counters = BackupQuiesceCounters {
        buffered_records: cut.buffered_records(),
        buffered_bytes: cut.buffered_bytes(),
        dropped_records: cut.dropped_records(),
        rejected_records: cut.rejected_records(),
    };
    let decoded_cut = match kind {
        BackupCutKind::Quiesced => {
            let (Some(engaged_at), Some(released_at)) = (cut.engaged_at(), cut.released_at())
            else {
                return Err(Report::new(WireDecodeError::InvalidValue {
                    field: "BackupCut",
                    kind: "quiesced cut interval",
                }));
            };
            if released_at < engaged_at {
                return Err(Report::new(WireDecodeError::InvalidValue {
                    field: "BackupCut",
                    kind: "ordered cut interval",
                }));
            }
            BackupCut::Quiesced {
                engaged_at: Timestamp::from_unix_nanos(engaged_at),
                released_at: Timestamp::from_unix_nanos(released_at),
                quiesce: counters,
            }
        }
        BackupCutKind::Live | BackupCutKind::Stopped | BackupCutKind::ConfigurationOnly => {
            if cut.engaged_at().is_some()
                || cut.released_at().is_some()
                || counters != BackupQuiesceCounters::default()
            {
                return Err(Report::new(WireDecodeError::InvalidValue {
                    field: "BackupCut",
                    kind: "non-quiesced cut fields",
                }));
            }
            match kind {
                BackupCutKind::Live => BackupCut::Live,
                BackupCutKind::Stopped => BackupCut::Stopped,
                BackupCutKind::ConfigurationOnly => BackupCut::ConfigurationOnly,
                BackupCutKind::Quiesced => unreachable!(),
            }
        }
    };
    Ok(BackupDomainSummary {
        domain: decoder.name("BackupDomainSummary.domain", domain.domain())?,
        revision: domain.revision(),
        cut: decoded_cut,
        sections: domain.sections(),
        section_bytes: domain.section_bytes(),
    })
}

/// The one frame a download sends: which retained archive it asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupDownloadRequest {
    /// The execution reference of the completed BACKUP whose archive is asked for.
    pub execution_reference: CommandExecutionReference,
}

impl BackupDownloadRequest {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<BackupDownloadRequestFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let reference = encoder.text(
            "BackupDownloadRequest.execution_reference",
            self.execution_reference.as_str(),
        )?;
        let request = wire::BackupDownloadRequest::create(
            encoder.fbb(),
            &wire::BackupDownloadRequestArgs {
                execution_reference: Some(reference),
            },
        );
        encoder.finish::<BackupDownloadRequestFrame>(request)
    }

    pub fn decode(
        frame: &VerifiedFrame<BackupDownloadRequestFrame>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let decoder = Decoder::new(frame.limits());
        let reference = decoder.check_text(
            "BackupDownloadRequest.execution_reference",
            frame.root().execution_reference(),
        )?;
        match CommandExecutionReference::parse(reference) {
            Ok(execution_reference) => Ok(Self {
                execution_reference,
            }),
            Err(error) => Err(error.change_context(WireDecodeError::InvalidValue {
                field: "BackupDownloadRequest.execution_reference",
                kind: "execution reference",
            })),
        }
    }
}

/// Why a download was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackupDownloadFailure {
    /// The request is malformed.
    InvalidRequest,
    /// The serving node retains no archive under the reference: it assembled none, a complete
    /// download already collected it, or the command is not a completed backup.
    NotRetained,
    /// The reference's retry validity ended, and the archive with it.
    Expired,
    /// Another user ran the backup.
    NotOwner,
    /// The serving node could not read its retained archive.
    ReadFailed,
}

wire_enum!(ALL_BACKUP_DOWNLOAD_FAILURES: BackupDownloadFailure => wire::BackupDownloadFailure {
    InvalidRequest,
    NotRetained,
    Expired,
    NotOwner,
    ReadFailed,
});

/// The archive's size and digest, sent before its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupArchiveStart {
    pub total_bytes: NonZeroU64,
    pub digest: ArchiveDigest,
}

/// Archive bytes, read in place from their frame.
#[derive(Debug, Clone)]
pub struct BackupArchiveChunk {
    frame: VerifiedFrame<BackupDownloadFrame>,
}

impl BackupArchiveChunk {
    /// The chunk's bytes, borrowed from its frame.
    pub fn bytes(&self) -> &[u8] {
        let chunk = self
            .frame
            .root()
            .part_as_backup_archive_chunk()
            .assured("this value is only decoded from a frame whose part is a chunk");
        chunk.bytes().bytes()
    }

    /// The chunk's bytes as shared bytes that keep the whole frame alive, without copying.
    pub fn shared_bytes(&self) -> Bytes {
        self.frame.bytes().slice_ref(self.bytes())
    }
}

/// Why a download was refused, and the words that say so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupDownloadFailed {
    pub failure: BackupDownloadFailure,
    pub message: String,
}

/// Everything a download stream frame can hold.
#[derive(Debug, Clone)]
pub enum BackupDownloadMessage {
    Start(BackupArchiveStart),
    Chunk(BackupArchiveChunk),
    /// Every byte was sent, and the server collected the archive.
    Complete,
    Failed(BackupDownloadFailed),
    NotLeader(LeaderRedirect),
}

impl BackupDownloadMessage {
    /// The frame that announces an archive's size and digest.
    pub fn encode_start(
        start: &BackupArchiveStart,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<BackupDownloadFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let digest = encoder.fingerprint("BackupArchiveStart.digest", start.digest.as_bytes())?;
        let part = wire::BackupArchiveStart::create(
            encoder.fbb(),
            &wire::BackupArchiveStartArgs {
                total_bytes: start.total_bytes.get(),
                digest: Some(digest),
            },
        );
        finish_download_message(
            encoder,
            EncodedUnion::new(wire::BackupDownloadPart::BackupArchiveStart, part),
        )
    }

    /// The frame that carries `bytes`, which must not be empty.
    pub fn encode_chunk(
        bytes: &[u8],
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<BackupDownloadFrame>, Report<WireEncodeError>> {
        if bytes.is_empty() {
            return Err(Report::new(WireEncodeError::EmptyCollection {
                field: "BackupArchiveChunk.bytes",
            }));
        }
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let bytes = encoder.bytes("BackupArchiveChunk.bytes", bytes)?;
        let part = wire::BackupArchiveChunk::create(
            encoder.fbb(),
            &wire::BackupArchiveChunkArgs { bytes: Some(bytes) },
        );
        finish_download_message(
            encoder,
            EncodedUnion::new(wire::BackupDownloadPart::BackupArchiveChunk, part),
        )
    }

    /// The frame that ends a complete download.
    pub fn encode_complete(
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<BackupDownloadFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let part =
            wire::BackupArchiveComplete::create(encoder.fbb(), &wire::BackupArchiveCompleteArgs {});
        finish_download_message(
            encoder,
            EncodedUnion::new(wire::BackupDownloadPart::BackupArchiveComplete, part),
        )
    }

    /// The frame that refuses a download.
    pub fn encode_failed(
        failed: &BackupDownloadFailed,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<BackupDownloadFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let message = encoder.text("BackupDownloadFailed.message", &failed.message)?;
        let part = wire::BackupDownloadFailed::create(
            encoder.fbb(),
            &wire::BackupDownloadFailedArgs {
                failure: Some(failed.failure.into()),
                message: Some(message),
            },
        );
        finish_download_message(
            encoder,
            EncodedUnion::new(wire::BackupDownloadPart::BackupDownloadFailed, part),
        )
    }

    /// The frame that sends a download to the leader.
    pub fn encode_redirect(
        redirect: &LeaderRedirect,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<BackupDownloadFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let part = redirect.encode(&mut encoder)?;
        finish_download_message(
            encoder,
            EncodedUnion::new(wire::BackupDownloadPart::LeaderRedirect, part),
        )
    }

    pub fn decode(
        frame: &VerifiedFrame<BackupDownloadFrame>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let decoder = Decoder::new(frame.limits());
        let message = frame.root();
        if let Some(start) = message.part_as_backup_archive_start() {
            let total_bytes =
                decoder.non_zero("BackupArchiveStart.total_bytes", start.total_bytes())?;
            let digest = decoder.fingerprint("BackupArchiveStart.digest", start.digest())?;
            return Ok(Self::Start(BackupArchiveStart {
                total_bytes,
                digest: ArchiveDigest::from_bytes(digest),
            }));
        }
        if let Some(chunk) = message.part_as_backup_archive_chunk() {
            if chunk.bytes().is_empty() {
                return Err(Report::new(WireDecodeError::EmptyCollection {
                    field: "BackupArchiveChunk.bytes",
                }));
            }
            return Ok(Self::Chunk(BackupArchiveChunk {
                frame: frame.clone(),
            }));
        }
        if message.part_as_backup_archive_complete().is_some() {
            return Ok(Self::Complete);
        }
        if let Some(failed) = message.part_as_backup_download_failed() {
            return Ok(Self::Failed(BackupDownloadFailed {
                failure: decoder
                    .required_enumeration("BackupDownloadFailed.failure", failed.failure())?,
                message: decoder.text("BackupDownloadFailed.message", failed.message())?,
            }));
        }
        if let Some(redirect) = message.part_as_leader_redirect() {
            return Ok(Self::NotLeader(LeaderRedirect::decode(decoder, redirect)?));
        }
        Err(decoder.unknown_union("BackupDownloadMessage.part", message.part_type().0))
    }
}

fn finish_download_message(
    mut encoder: Encoder<'_>,
    part: EncodedUnion<wire::BackupDownloadPart>,
) -> Result<EncodedFrame<BackupDownloadFrame>, Report<WireEncodeError>> {
    let message = wire::BackupDownloadMessage::create(
        encoder.fbb(),
        &wire::BackupDownloadMessageArgs {
            part_type: part.discriminant,
            part: Some(part.value),
        },
    );
    encoder.finish::<BackupDownloadFrame>(message)
}
