//! Receiving a restore's archive, verifying it, and planning the restore before anything changes.
//!
//! Layer: control plane.
//!
//! - **Owns.** Staging a restore stream's archive in this node's staging area, reading and
//!   verifying every section of it off the async workers, parsing each domain's models, planning
//!   the restore against the users and domains the cluster has, and planning each domain's model
//!   run with the transaction planner.
//! - **Depends on.** The staging area, the archive format's reader, the language layer, and the
//!   decision layer's restore and transaction planners.
//! - **Must not know.** How a plan is applied, or which transport carried the stream.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::Path,
};

use arch_into::ArchInto as _;
use bytes::Bytes;
use error_stack::{Report, ResultExt as _};
use futures_util::Stream;
use meticulous::OptionExt as _;
use nervix_backup::{
    ArchiveContents, ArchiveReadError, DescribedRuntimeState, DescribedSection, SectionDigester,
    read_archive_contents,
};
use nervix_execution::{Cancellation, ChargedBytes, Executor, MemoryClass, StorageClass};
use nervix_models::{
    CreateStatement, DomainName, Model, RequestedResourceVersion, ResourceUpload,
    ResourceUploadState, ResourceUploads, ResourceUploadsError, Restore, RestoreArchive,
    RestoreStep, Statement, Timestamp, TransactionImpactReport, UserName,
};
use nervix_nspl::client_statement::{ClientStatement, parse_client_statement_sources};
use thiserror::Error;

use super::{
    archives::{
        RestoreStaging, RestoreStagingWriter, RestoreStreamPart, StagingFailure, StagingOutcome,
        StagingRefusal, stage_restore_archive,
    },
    steps::restored_upload_key,
};
use crate::{
    application::{
        session_service::SessionServiceImpl, transaction::restored::RestoredDomainInputs,
    },
    registry::{ArchiveToRestore, ExistingState, PlannedDomain, RestorePlan},
    runtime::{Runtime, SnapshotStagingError, StagedArtifact, StagedSnapshotWriter},
};

/// Why a restore was refused before it changed anything. No variant carries archive contents.
#[derive(Debug, Error)]
pub(in crate::application) enum RestoreRefusal {
    #[error("the archive could not be read on this node")]
    Unreadable,
    #[error("the archive is not a valid backup archive")]
    InvalidArchive,
    #[error("the models of domain '{domain}' do not parse at line {line}")]
    ModelsDoNotParse { domain: DomainName, line: usize },
    #[error(
        "statement {statement} of the models of domain '{domain}', at line {line}, does not \
         create a model"
    )]
    NotAModel {
        domain: DomainName,
        statement: usize,
        line: usize,
    },
    #[error("the restore cannot be applied to this cluster")]
    Plan,
    #[error("the models of domain '{domain}' do not form a valid configuration")]
    ModelRun { domain: DomainName },
}

/// An archive a restore stream staged and verified, with each domain's models parsed.
pub(in crate::application) struct VerifiedArchive {
    artifact: StagedArtifact,
    contents: ArchiveContents,
    models: BTreeMap<DomainName, Vec<Model<RequestedResourceVersion>>>,
}

impl VerifiedArchive {
    pub(in crate::application) fn states_for(
        &self,
        domain: &DomainName,
    ) -> &[DescribedRuntimeState] {
        match self
            .contents
            .description
            .domains
            .iter()
            .find(|described| &described.capture.domain == domain)
        {
            Some(described) => described.state.as_slice(),
            None => &[],
        }
    }

    pub(in crate::application) fn skipped_state_for(
        &self,
        domain: &DomainName,
    ) -> &[nervix_backup::SkippedStateSection] {
        match self
            .contents
            .description
            .domains
            .iter()
            .find(|described| &described.capture.domain == domain)
        {
            Some(described) => described.skipped_state.as_slice(),
            None => &[],
        }
    }

    /// Re-reads a verified raw guest blob from the staged archive through the bulk storage
    /// executor. The digest is checked again before the bytes become an installed checkpoint.
    pub(in crate::application) async fn read_guest_blob(
        &self,
        runtime: &Runtime,
        section: &DescribedSection,
    ) -> Result<Vec<u8>, Report<RestoreRefusal>> {
        let executor = runtime.executor().clone();
        let path = self.artifact.path().to_path_buf();
        let offset = section.offset;
        let length =
            usize::try_from(section.length).map_err(|_| Report::new(RestoreRefusal::Unreadable))?;
        let digest = section.digest;
        let reservation = executor
            .reserve(MemoryClass::Bulk, section.length)
            .await
            .change_context(RestoreRefusal::Unreadable)?;
        let read = executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| -> io::Result<Vec<u8>> {
                    let mut file = std::fs::File::open(path)?;
                    file.seek(SeekFrom::Start(offset))?;
                    let mut bytes = vec![0; length];
                    if cancellation.is_cancelled() {
                        return Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "restore was cancelled",
                        ));
                    }
                    file.read_exact(&mut bytes)?;
                    if SectionDigester::digest_of(&bytes) != digest {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "guest blob digest changed",
                        ));
                    }
                    Ok(bytes)
                },
            )
            .await
            .change_context(RestoreRefusal::Unreadable)?;
        read.change_context(RestoreRefusal::Unreadable)
    }

    /// When the backup that wrote the archive read its contents.
    pub(in crate::application) fn captured_at(&self) -> Timestamp {
        self.contents.description.manifest.captured_at
    }

    /// The staged archive file, which holds every section at the offset its description names.
    pub(in crate::application) fn path(&self) -> &Path {
        self.artifact.path()
    }

    /// The NSPL of the archived domain `domain`, as the archive holds it.
    pub(in crate::application) fn models_text(&self, domain: &DomainName) -> &str {
        match self.contents.models.get(domain) {
            Some(text) => text.as_str(),
            None => "",
        }
    }
}

/// Stages restore archives in the runtime's staging area, charged to the bulk memory budget as
/// they are written.
struct ServerRestoreStaging<'a> {
    runtime: &'a Runtime,
    executor: Executor,
}

struct ServerRestoreWriter {
    writer: StagedSnapshotWriter,
    executor: Executor,
}

impl RestoreStaging for ServerRestoreStaging<'_> {
    type Writer = ServerRestoreWriter;

    async fn stage(&self, length: u64) -> Result<Self::Writer, Report<StagingRefusal>> {
        match self.runtime.try_stage_artifact(length).await {
            Ok(writer) => Ok(ServerRestoreWriter {
                writer,
                executor: self.executor.clone(),
            }),
            Err(error) => {
                let refusal = match error.current_context() {
                    SnapshotStagingError::QuotaExceeded { limit, .. } => StagingRefusal::TooLarge {
                        declared: length,
                        limit: *limit,
                    },
                    SnapshotStagingError::Full { .. } => {
                        StagingRefusal::StagingFull { declared: length }
                    }
                    _ => StagingRefusal::StagingFailed,
                };
                Err(error.change_context(refusal))
            }
        }
    }
}

impl RestoreStagingWriter for ServerRestoreWriter {
    type Artifact = StagedArtifact;

    async fn write(&mut self, bytes: Bytes) -> Result<(), Report<StagingFailure>> {
        let length: u64 = bytes.len().arch_into();
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, length)
            .await
            .change_context(StagingFailure)?;
        self.writer
            .write_chunk(ChargedBytes::from_owned(bytes.to_vec(), reservation))
            .await
            .change_context(StagingFailure)
    }

    async fn finish(self) -> Result<StagedArtifact, Report<StagingFailure>> {
        self.writer
            .finish_artifact()
            .await
            .change_context(StagingFailure)
    }
}

/// A reader that stops once the storage job reading it is cancelled.
struct CancellableRead<'job, R> {
    inner: R,
    cancellation: &'job Cancellation,
}

impl<R: Read> Read for CancellableRead<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "the archive read was cancelled",
            ));
        }
        self.inner.read(buffer)
    }
}

impl SessionServiceImpl {
    /// Stages the archive `parts` carry in this node's staging area.
    pub(super) async fn stage_restore_stream<P, E>(
        &self,
        declared: RestoreArchive,
        parts: P,
    ) -> StagingOutcome<StagedArtifact, E>
    where
        P: Stream<Item = Result<RestoreStreamPart, E>> + Unpin + Send,
    {
        let staging = ServerRestoreStaging {
            runtime: &self.inner.runtime,
            executor: self.inner.runtime.executor().clone(),
        };
        stage_restore_archive(&staging, declared, parts).await
    }

    /// Reads every section of `artifact` and verifies it against its manifest, off the async
    /// workers, and parses each domain's models. An archive that does not verify is released.
    pub(super) async fn verify_restore_archive(
        &self,
        artifact: StagedArtifact,
    ) -> Result<VerifiedArchive, Report<RestoreRefusal>> {
        let executor = self.inner.runtime.executor().clone();
        let path = artifact.path().to_path_buf();
        let reservation = executor
            .reserve(
                MemoryClass::Bulk,
                executor.limits().bulk_chunk_bytes.as_u64(),
            )
            .await
            .change_context(RestoreRefusal::Unreadable)?;
        let read = executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    let file = match std::fs::File::open(&path) {
                        Ok(file) => file,
                        Err(error) => {
                            return Err(Report::new(error).change_context(ArchiveReadError::Read));
                        }
                    };
                    read_archive_contents(CancellableRead {
                        inner: BufReader::new(file),
                        cancellation,
                    })
                },
            )
            .await
            .change_context(RestoreRefusal::Unreadable)?;
        let contents = read.change_context(RestoreRefusal::InvalidArchive)?;
        let mut models = BTreeMap::new();
        for (domain, text) in &contents.models {
            models.insert(domain.clone(), parse_models(domain, text)?);
        }
        Ok(VerifiedArchive {
            artifact,
            contents,
            models,
        })
    }

    /// Plans `restore` of `archive` against the users and domains the cluster has now. A step in
    /// `recorded` was applied by an earlier attempt of the same restore and is not checked again.
    pub(super) async fn plan_restore(
        &self,
        restore: &Restore,
        archive: &VerifiedArchive,
        recorded: &BTreeSet<RestoreStep>,
    ) -> Result<RestorePlan, Report<RestoreRefusal>> {
        let users = self
            .inner
            .consensus
            .current_users()
            .await
            .into_keys()
            .collect();
        let domains = self
            .inner
            .consensus
            .current_domains()
            .await
            .into_keys()
            .collect();
        RestorePlan::new(
            restore,
            ArchiveToRestore {
                description: &archive.contents.description,
                models: &archive.models,
            },
            &ExistingState {
                users: &users,
                domains: &domains,
                recorded,
            },
        )
        .change_context(RestoreRefusal::Plan)
    }

    /// Plans every domain's model run with the transaction planner, against the domain as the
    /// restore creates it, and returns each run's report, in plan order. A domain without models
    /// has no run to report.
    pub(super) async fn plan_model_runs(
        &self,
        plan: &RestorePlan,
        owner: &UserName,
    ) -> Result<Vec<Option<TransactionImpactReport>>, Report<RestoreRefusal>> {
        let mut reports = Vec::with_capacity(plan.domains.len());
        for domain in plan.domains.values() {
            nervix_primitives::task::consume_budget().await;
            if domain.models.is_empty() {
                reports.push(None);
                continue;
            }
            let refusal = || RestoreRefusal::ModelRun {
                domain: domain.target.clone(),
            };
            let statements = create_statements(domain);
            let resources = domain
                .resources
                .iter()
                .map(|declared| declared.resource.clone())
                .collect::<BTreeSet<_>>();
            let completed = completed_uploads(owner, domain).change_context_lazy(refusal)?;
            let planned = self
                .plan_restored_model_run(
                    RestoredDomainInputs {
                        state: &domain.state,
                        resources: &resources,
                        completed,
                    },
                    &statements,
                )
                .await
                .change_context_lazy(refusal)?;
            let report = planned.report().change_context_lazy(refusal)?;
            reports.push(Some(report));
        }
        Ok(reports)
    }
}

/// `domain`'s models as the statements that create them, in archive order.
pub(super) fn create_statements(domain: &PlannedDomain) -> Vec<Statement> {
    let mut statements = Vec::with_capacity(domain.models.len());
    for model in &domain.models {
        let requested = model.clone().try_map_resource_versions(|_, version| {
            Ok::<_, std::convert::Infallible>(RequestedResourceVersion::Number(version))
        });
        let body = match requested {
            Ok(body) => body,
            Err(never) => match never {},
        };
        statements.push(Statement::Create(CreateStatement::new(
            Box::new(body),
            false,
        )));
    }
    statements
}

/// The upload outcomes planning reads for a restored domain: exactly its imported versions, each
/// completed.
fn completed_uploads(
    owner: &UserName,
    domain: &PlannedDomain,
) -> Result<ResourceUploads, Report<ResourceUploadsError>> {
    let uploads = domain.versions.iter().map(|version| ResourceUpload {
        key: restored_upload_key(owner, &version.resource.id),
        version: version.resource.id.version,
        state: ResourceUploadState::Completed {
            root_checksum: version.resource.root_checksum.clone(),
            outcome_revision: 0,
        },
    });
    ResourceUploads::try_from_uploads(uploads)
}

/// Parses the models of `domain`, which must be one `CREATE` of a model per statement.
fn parse_models(
    domain: &DomainName,
    text: &str,
) -> Result<Vec<Model<RequestedResourceVersion>>, Report<RestoreRefusal>> {
    let statements = match parse_client_statement_sources(text) {
        Ok(statements) => statements,
        Err(error) => {
            let offset = match error.current_context().diagnostics().first() {
                Some(diagnostic) => diagnostic.span.start,
                None => text.len(),
            };
            let line = line_at(text, offset);
            return Err(error.change_context(RestoreRefusal::ModelsDoNotParse {
                domain: domain.clone(),
                line,
            }));
        }
    };
    let mut models = Vec::with_capacity(statements.len());
    for (index, parsed) in statements.into_iter().enumerate() {
        let statement = index
            .checked_add(1)
            .assured("a statement's index is below the length of the list that holds it");
        let not_a_model = || RestoreRefusal::NotAModel {
            domain: domain.clone(),
            statement,
            line: line_at(text, parsed.span.start),
        };
        let ClientStatement::Server(Statement::Create(create)) = parsed.statement else {
            return Err(Report::new(not_a_model()));
        };
        if create.if_not_exists {
            return Err(Report::new(not_a_model()));
        }
        models.push(*create.body);
    }
    Ok(models)
}

/// The one-based line the byte at `offset` falls on in `text`. A line break is one byte that no
/// multi-byte character contains, so counting the breaks before any byte offset is exact.
fn line_at(text: &str, offset: usize) -> usize {
    let mut breaks = 0_usize;
    for byte in text.as_bytes().iter().take(offset) {
        if *byte == b'\n' {
            breaks = breaks
                .checked_add(1)
                .assured("a text holds fewer line breaks than usize counts");
        }
    }
    breaks
        .checked_add(1)
        .assured("a text holds fewer line breaks than usize counts")
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    fn domain() -> DomainName {
        DomainName::parse("prod").assured("the test domain is a valid literal name")
    }

    fn refused(text: &str) -> Report<RestoreRefusal> {
        match parse_models(&domain(), text) {
            Ok(models) => panic!("{} models parsed", models.len()),
            Err(refusal) => refusal,
        }
    }

    #[test]
    fn every_create_statement_of_a_domain_is_one_model() {
        let text = "CREATE SCHEMA order_event ( id I64 );\nCREATE RELAY orders SCHEMA order_event \
                    UNBRANCHED;\n";
        let models = parse_models(&domain(), text).assured("the test models parse");
        assert_eq!(models.len(), 2);
    }

    #[test]
    fn text_that_does_not_parse_is_refused_naming_its_domain_and_line() {
        let text = "CREATE SCHEMA order_event ( id I64 );\nCREATE RELAY orders SCHEMA;\n";
        let refusal = refused(text);
        let RestoreRefusal::ModelsDoNotParse { domain, line } = refusal.current_context() else {
            panic!("the text does not parse: {refusal:?}");
        };
        assert_eq!(domain.as_str(), "prod");
        assert_eq!(*line, 2);
    }

    #[test]
    fn a_statement_that_creates_no_model_is_refused_naming_its_position() {
        let text = "CREATE SCHEMA order_event ( id I64 );\n\nSTART;\n";
        let refusal = refused(text);
        let RestoreRefusal::NotAModel {
            domain,
            statement,
            line,
        } = refusal.current_context()
        else {
            panic!("the second statement creates no model: {refusal:?}");
        };
        assert_eq!(domain.as_str(), "prod");
        assert_eq!(*statement, 2);
        assert_eq!(*line, 3);
    }

    #[test]
    fn a_create_that_tolerates_an_existing_model_is_refused() {
        let text = "CREATE IF NOT EXISTS SCHEMA order_event ( id I64 );\n";
        let refusal = refused(text);
        let RestoreRefusal::NotAModel { statement, .. } = refusal.current_context() else {
            panic!("an archive creates every model exactly: {refusal:?}");
        };
        assert_eq!(*statement, 1);
    }

    #[test]
    fn a_line_is_counted_in_bytes_across_multi_byte_characters() {
        let text = "é\nü\nx";
        assert_eq!(line_at(text, 0), 1);
        assert_eq!(line_at(text, 1), 1);
        assert_eq!(line_at(text, 3), 2);
        assert_eq!(line_at(text, 6), 3);
        assert_eq!(line_at(text, 600), 3);
    }
}
