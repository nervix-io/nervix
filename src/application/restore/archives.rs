//! The archives restores read on this node: staging one from its stream, and retaining it while its
//! restore applies.
//!
//! Layer: control plane.
//!
//! - **Owns.** Staging the archive one restore stream carries under the staging quota, checking it
//!   is exactly the archive its start declared, and retaining a verified archive under the
//!   restore's execution reference, for its owner, until the restore finishes or the reference's
//!   retry validity ends.
//! - **Depends on.** A staging area that reserves quota before it writes a byte, and the archive
//!   identity the vocabulary names.
//! - **Must not know.** What an archive holds, how a restore applies it, or which transport carried
//!   its bytes.
//!
//! A staged archive holds its quota for as long as it exists. The quota is released exactly once,
//! when the last holder lets go: the stream that staged it, if the stream ends before the archive
//! is complete, or the registry and every restore reading it otherwise. A stream a client abandons
//! midway therefore releases what it reserved and nothing else, and a retry stages its own copy.

use std::{collections::BTreeMap, future::Future};

use arch_into::ArchInto as _;
use bytes::Bytes;
use error_stack::Report;
use futures_util::{Stream, StreamExt as _};
use nervix_models::{CommandExecutionReference, RestoreArchive, Timestamp, UserName};
use nervix_primitives::sync::blocking::Mutex;
use thiserror::Error;
use triomphe::Arc;

use crate::application::backup::retained::RetainedArtifact;

/// Why staging could not go on.
#[derive(Debug, Error)]
#[error("the archive could not be staged")]
pub(in crate::application) struct StagingFailure;

/// Where the archive of a restore stream is staged.
pub(in crate::application) trait RestoreStaging: Send + Sync {
    type Writer: RestoreStagingWriter;

    /// Reserves staging quota for exactly `length` bytes and opens the file they land in. Refuses
    /// rather than waits when the quota cannot hold them now.
    fn stage(
        &self,
        length: u64,
    ) -> impl Future<Output = Result<Self::Writer, Report<StagingRefusal>>> + Send;
}

/// A staged archive being written, which holds its quota until it is dropped or finished.
pub(in crate::application) trait RestoreStagingWriter: Send {
    type Artifact: RetainedArtifact;

    /// Writes the next bytes of the archive.
    fn write(
        &mut self,
        bytes: Bytes,
    ) -> impl Future<Output = Result<(), Report<StagingFailure>>> + Send;

    /// Seals the archive once every declared byte was written.
    fn finish(self) -> impl Future<Output = Result<Self::Artifact, Report<StagingFailure>>> + Send;
}

/// Why a restore stream's archive was not staged. No variant carries archive bytes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(in crate::application) enum StagingRefusal {
    #[error(
        "a restore stream carries one restore start, followed only by archive chunks: {reason}"
    )]
    InvalidStream { reason: String },
    #[error("the archive's chunks add up to more than the {declared} bytes its start declares")]
    Oversized { declared: u64 },
    #[error(
        "the archive's chunks add up to {received} bytes, not the {declared} its start declares"
    )]
    Truncated { declared: u64, received: u64 },
    #[error("the archive does not have the digest its start declares")]
    DigestMismatch,
    #[error(
        "the archive's {declared} bytes exceed what the leader stages for one archive, {limit} \
         bytes"
    )]
    TooLarge { declared: u64, limit: u64 },
    #[error(
        "the leader's staging area cannot hold another {declared} bytes now; send the restore \
         again once running backups and transfers finish"
    )]
    StagingFull { declared: u64 },
    #[error("the leader could not stage the archive")]
    StagingFailed,
}

/// One part of a restore stream after its start.
pub(in crate::application) enum RestoreStreamPart {
    /// The next bytes of the archive.
    Chunk(Bytes),
    /// A frame that is not an archive chunk, and why.
    Invalid(String),
}

/// How staging one restore stream ended.
pub(in crate::application) enum StagingOutcome<A, E> {
    /// Every declared byte arrived and the archive has the declared digest.
    Staged(A),
    /// The stream did not carry the archive its start declared.
    Refused(StagingRefusal),
    /// The transport failed while the stream was read.
    Transport(E),
}

/// Stages the archive `parts` carry into `staging`. The archive must be exactly `declared`: every
/// byte of it, nothing after it, and the declared digest over all of it.
pub(in crate::application) async fn stage_restore_archive<S, P, E>(
    staging: &S,
    declared: RestoreArchive,
    mut parts: P,
) -> StagingOutcome<<S::Writer as RestoreStagingWriter>::Artifact, E>
where
    S: RestoreStaging,
    P: Stream<Item = Result<RestoreStreamPart, E>> + Unpin,
{
    let total = declared.total_bytes.get();
    let mut writer = match staging.stage(total).await {
        Ok(writer) => writer,
        Err(refusal) => return StagingOutcome::Refused(refusal.current_context().clone()),
    };
    let mut received = 0_u64;
    while let Some(part) = parts.next().await {
        nervix_primitives::task::consume_budget().await;
        let bytes = match part {
            Ok(RestoreStreamPart::Chunk(bytes)) => bytes,
            Ok(RestoreStreamPart::Invalid(reason)) => {
                return StagingOutcome::Refused(StagingRefusal::InvalidStream { reason });
            }
            Err(error) => return StagingOutcome::Transport(error),
        };
        let length: u64 = bytes.len().arch_into();
        let Some(next) = received.checked_add(length) else {
            return StagingOutcome::Refused(StagingRefusal::Oversized { declared: total });
        };
        if next > total {
            return StagingOutcome::Refused(StagingRefusal::Oversized { declared: total });
        }
        if writer.write(bytes).await.is_err() {
            return StagingOutcome::Refused(StagingRefusal::StagingFailed);
        }
        received = next;
    }
    if received != total {
        return StagingOutcome::Refused(StagingRefusal::Truncated {
            declared: total,
            received,
        });
    }
    let artifact = match writer.finish().await {
        Ok(artifact) => artifact,
        Err(_) => return StagingOutcome::Refused(StagingRefusal::StagingFailed),
    };
    if artifact.length() != total || artifact.digest() != *declared.digest.as_bytes() {
        return StagingOutcome::Refused(StagingRefusal::DigestMismatch);
    }
    StagingOutcome::Staged(artifact)
}

/// Why a verified archive was not retained.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(in crate::application) enum RetentionRefusal {
    #[error("execution reference '{reference}' belongs to another user's restore")]
    NotOwner {
        reference: CommandExecutionReference,
    },
}

/// The verified archives this node's restores read, by the execution reference of each restore.
pub(in crate::application) struct RestoreArchives<A> {
    inner: Arc<Mutex<BTreeMap<CommandExecutionReference, RetainedRestore<A>>>>,
}

struct RetainedRestore<A> {
    owner: UserName,
    identity: RestoreArchive,
    retained_until: Timestamp,
    archive: Arc<A>,
}

impl<A> Clone for RestoreArchives<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<A> Default for RestoreArchives<A> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl<A> RestoreArchives<A> {
    /// Retains `archive`, whose identity is `identity`, for `owner`'s restore under `reference`
    /// until `retained_until`. An archive an earlier attempt of the same restore retained is
    /// replaced; a restore still reading it keeps it until it ends.
    pub(in crate::application) fn retain(
        &self,
        reference: CommandExecutionReference,
        owner: UserName,
        identity: RestoreArchive,
        retained_until: Timestamp,
        archive: A,
    ) -> Result<(), RetentionRefusal> {
        let mut archives = self.inner.lock();
        if let Some(existing) = archives.get(&reference)
            && existing.owner != owner
        {
            return Err(RetentionRefusal::NotOwner { reference });
        }
        let entry = RetainedRestore {
            owner,
            identity,
            retained_until,
            archive: Arc::new(archive),
        };
        let replaced = archives.insert(reference, entry);
        drop(archives);
        // A replaced archive is released here, outside the lock, unless a restore still reads it.
        drop(replaced);
        Ok(())
    }

    /// The archive `owner`'s restore under `reference` retains, when it is `identity`.
    pub(in crate::application) fn get(
        &self,
        reference: &CommandExecutionReference,
        owner: &UserName,
        identity: &RestoreArchive,
    ) -> Option<Arc<A>> {
        let archives = self.inner.lock();
        let entry = archives.get(reference)?;
        if entry.owner != *owner || entry.identity != *identity {
            return None;
        }
        Some(entry.archive.clone())
    }

    /// Whether an archive is retained under `reference`, for any owner.
    pub(in crate::application) fn retains(&self, reference: &CommandExecutionReference) -> bool {
        self.inner.lock().contains_key(reference)
    }

    /// Releases the archive retained under `reference`, once its restore finished. A restore
    /// still reading it keeps it until it ends.
    pub(in crate::application) fn release(&self, reference: &CommandExecutionReference) {
        let released = self.inner.lock().remove(reference);
        drop(released);
    }

    /// Releases every archive whose restore's retry validity ended by `now`.
    pub(in crate::application) fn sweep(&self, now: Timestamp) {
        let mut expired = Vec::new();
        {
            let mut archives = self.inner.lock();
            // An archive is retained only while its restore's retry validity lasts, so the
            // registry holds the restores of that one window, which is what bounds this walk.
            let mut references = Vec::new();
            for (reference, entry) in archives.iter() {
                if now >= entry.retained_until {
                    references.push(reference.clone());
                }
            }
            for reference in references {
                if let Some(entry) = archives.remove(&reference) {
                    expired.push(entry);
                }
            }
        }
        // Releasing a staged archive removes a file, so it happens after the lock is released.
        drop(expired);
    }
}

#[cfg(test)]
pub(super) mod test_staging {
    //! A staging area in memory that counts every reservation it makes and every release of one.

    use std::{collections::BTreeMap, sync::Arc as StdArc};

    use arch_into::ArchInto as _;
    use bytes::Bytes;
    use error_stack::Report;
    use nervix_primitives::sync::{
        atomic::{AtomicUsize, Ordering},
        blocking::Mutex,
    };

    use super::{RestoreStaging, RestoreStagingWriter, StagingFailure, StagingRefusal};
    use crate::application::backup::retained::RetainedArtifact;

    /// Staging quota: how many reservations were made, and how often each one was released.
    #[derive(Default)]
    pub(in crate::application) struct Quota {
        pub(in crate::application) reserved: AtomicUsize,
        pub(in crate::application) released: Mutex<BTreeMap<usize, usize>>,
    }

    impl Quota {
        /// Whether reservation `hold` was released.
        pub(in crate::application) fn released(&self, hold: usize) -> bool {
            self.released.lock().contains_key(&hold)
        }

        /// How often each reservation was released, by reservation.
        pub(in crate::application) fn releases(&self) -> BTreeMap<usize, usize> {
            self.released.lock().clone()
        }
    }

    /// One reservation of staging quota, released when it is dropped.
    pub(in crate::application) struct Hold {
        pub(in crate::application) id: usize,
        quota: StdArc<Quota>,
    }

    impl Drop for Hold {
        fn drop(&mut self) {
            *self.quota.released.lock().entry(self.id).or_default() += 1;
        }
    }

    /// A staging area that holds archives in memory, or refuses every archive with `refusal`.
    pub(in crate::application) struct MemoryStaging {
        pub(in crate::application) quota: StdArc<Quota>,
        pub(in crate::application) refusal: Option<StagingRefusal>,
    }

    impl MemoryStaging {
        pub(in crate::application) fn new(quota: StdArc<Quota>) -> Self {
            Self {
                quota,
                refusal: None,
            }
        }
    }

    pub(in crate::application) struct MemoryWriter {
        hold: Hold,
        bytes: Vec<u8>,
    }

    /// An archive staged in memory, which holds its reservation for as long as it exists.
    pub(in crate::application) struct StagedMemory {
        pub(in crate::application) bytes: Vec<u8>,
        pub(in crate::application) hold: Hold,
    }

    impl RetainedArtifact for StagedMemory {
        fn length(&self) -> u64 {
            self.bytes.len().arch_into()
        }

        fn digest(&self) -> [u8; 32] {
            *blake3::hash(&self.bytes).as_bytes()
        }
    }

    impl RestoreStaging for MemoryStaging {
        type Writer = MemoryWriter;

        async fn stage(&self, _length: u64) -> Result<MemoryWriter, Report<StagingRefusal>> {
            if let Some(refusal) = &self.refusal {
                return Err(Report::new(refusal.clone()));
            }
            let id = self.quota.reserved.fetch_add(1, Ordering::SeqCst);
            Ok(MemoryWriter {
                hold: Hold {
                    id,
                    quota: self.quota.clone(),
                },
                bytes: Vec::new(),
            })
        }
    }

    impl RestoreStagingWriter for MemoryWriter {
        type Artifact = StagedMemory;

        async fn write(&mut self, bytes: Bytes) -> Result<(), Report<StagingFailure>> {
            // Writing a chunk is a storage job other tasks run beside.
            nervix_primitives::task::yield_now().await;
            self.bytes.extend_from_slice(&bytes);
            Ok(())
        }

        async fn finish(self) -> Result<StagedMemory, Report<StagingFailure>> {
            Ok(StagedMemory {
                bytes: self.bytes,
                hold: self.hold,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Arc as StdArc};

    use futures_util::stream;
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::ArchiveDigest;
    use nervix_primitives::sync::atomic::Ordering;

    use super::{test_staging::*, *};

    const ARCHIVE: &[u8] = b"a restore archive in three chunks";

    fn declared(bytes: &[u8]) -> RestoreArchive {
        let length: u64 = bytes.len().arch_into();
        RestoreArchive {
            total_bytes: NonZeroU64::new(length).assured("every test archive has bytes"),
            digest: ArchiveDigest::from_bytes(*blake3::hash(bytes).as_bytes()),
        }
    }

    fn chunks(bytes: &'static [u8]) -> Vec<Result<RestoreStreamPart, &'static str>> {
        let mut parts = Vec::new();
        for chunk in bytes.chunks(12) {
            parts.push(Ok(RestoreStreamPart::Chunk(Bytes::from_static(chunk))));
        }
        parts
    }

    fn reference(name: &str) -> CommandExecutionReference {
        CommandExecutionReference::parse(name).assured("the test reference is a valid literal")
    }

    fn user(name: &str) -> UserName {
        UserName::parse(name).assured("the test user is a valid literal name")
    }

    async fn stage(
        staging: &MemoryStaging,
        declared: RestoreArchive,
        parts: Vec<Result<RestoreStreamPart, &'static str>>,
    ) -> StagingOutcome<StagedMemory, &'static str> {
        stage_restore_archive(staging, declared, stream::iter(parts)).await
    }

    fn refusal(outcome: StagingOutcome<StagedMemory, &'static str>) -> StagingRefusal {
        match outcome {
            StagingOutcome::Refused(refusal) => refusal,
            StagingOutcome::Staged(staged) => {
                panic!("the stream was staged, {} bytes", staged.bytes.len())
            }
            StagingOutcome::Transport(error) => panic!("the transport failed: {error}"),
        }
    }

    #[nervix_primitives::test]
    async fn the_declared_archive_is_staged_and_holds_its_quota_until_it_is_dropped() {
        let quota = StdArc::new(Quota::default());
        let staging = MemoryStaging::new(quota.clone());
        let outcome = stage(&staging, declared(ARCHIVE), chunks(ARCHIVE)).await;
        let StagingOutcome::Staged(staged) = outcome else {
            panic!("the declared archive is staged");
        };
        assert_eq!(staged.bytes, ARCHIVE);
        assert!(!quota.released(staged.hold.id));
        drop(staged);
        assert_eq!(quota.releases(), BTreeMap::from([(0, 1)]));
    }

    #[nervix_primitives::test]
    async fn chunks_past_the_declared_size_are_refused_and_release_the_quota() {
        let quota = StdArc::new(Quota::default());
        let staging = MemoryStaging::new(quota.clone());
        let mut parts = chunks(ARCHIVE);
        parts.push(Ok(RestoreStreamPart::Chunk(Bytes::from_static(b"more"))));
        let refused = refusal(stage(&staging, declared(ARCHIVE), parts).await);
        assert_eq!(
            refused,
            StagingRefusal::Oversized {
                declared: ARCHIVE.len().arch_into()
            }
        );
        assert_eq!(quota.releases(), BTreeMap::from([(0, 1)]));
    }

    #[nervix_primitives::test]
    async fn a_stream_that_ends_early_is_refused_as_truncated() {
        let quota = StdArc::new(Quota::default());
        let staging = MemoryStaging::new(quota.clone());
        let mut parts = chunks(ARCHIVE);
        parts.pop();
        let refused = refusal(stage(&staging, declared(ARCHIVE), parts).await);
        assert_eq!(
            refused,
            StagingRefusal::Truncated {
                declared: ARCHIVE.len().arch_into(),
                received: 24,
            }
        );
        assert_eq!(quota.releases(), BTreeMap::from([(0, 1)]));
    }

    #[nervix_primitives::test]
    async fn bytes_without_the_declared_digest_are_refused() {
        let quota = StdArc::new(Quota::default());
        let staging = MemoryStaging::new(quota.clone());
        let other = b"another archive of equal length!!";
        assert_eq!(other.len(), ARCHIVE.len());
        let refused = refusal(stage(&staging, declared(other), chunks(ARCHIVE)).await);
        assert_eq!(refused, StagingRefusal::DigestMismatch);
        assert_eq!(quota.releases(), BTreeMap::from([(0, 1)]));
    }

    #[nervix_primitives::test]
    async fn a_frame_that_is_not_a_chunk_is_refused() {
        let staging = MemoryStaging::new(StdArc::new(Quota::default()));
        let parts = vec![Ok(RestoreStreamPart::Invalid(
            "a second restore start".to_string(),
        ))];
        let refused = refusal(stage(&staging, declared(ARCHIVE), parts).await);
        assert_eq!(
            refused,
            StagingRefusal::InvalidStream {
                reason: "a second restore start".to_string()
            }
        );
    }

    #[nervix_primitives::test]
    async fn a_transport_failure_ends_staging_with_the_transport_error() {
        let quota = StdArc::new(Quota::default());
        let staging = MemoryStaging::new(quota.clone());
        let mut parts = chunks(ARCHIVE);
        parts.truncate(1);
        parts.push(Err("the connection reset"));
        let outcome = stage(&staging, declared(ARCHIVE), parts).await;
        let StagingOutcome::Transport(error) = outcome else {
            panic!("the transport failure ends staging");
        };
        assert_eq!(error, "the connection reset");
        assert_eq!(quota.releases(), BTreeMap::from([(0, 1)]));
    }

    #[nervix_primitives::test]
    async fn a_staging_area_that_cannot_hold_the_archive_refuses_it_before_reading() {
        let quota = StdArc::new(Quota::default());
        let staging = MemoryStaging {
            quota: quota.clone(),
            refusal: Some(StagingRefusal::StagingFull { declared: 33 }),
        };
        let refused = refusal(stage(&staging, declared(ARCHIVE), chunks(ARCHIVE)).await);
        assert_eq!(refused, StagingRefusal::StagingFull { declared: 33 });
        assert_eq!(quota.reserved.load(Ordering::SeqCst), 0);
    }

    async fn staged(quota: &StdArc<Quota>) -> StagedMemory {
        let staging = MemoryStaging::new(quota.clone());
        match stage(&staging, declared(ARCHIVE), chunks(ARCHIVE)).await {
            StagingOutcome::Staged(staged) => staged,
            StagingOutcome::Refused(refusal) => panic!("the archive was refused: {refusal}"),
            StagingOutcome::Transport(error) => panic!("the transport failed: {error}"),
        }
    }

    #[nervix_primitives::test]
    async fn a_retained_archive_is_read_only_by_its_owner_under_its_identity() {
        let quota = StdArc::new(Quota::default());
        let archives = RestoreArchives::default();
        let identity = declared(ARCHIVE);
        archives
            .retain(
                reference("restore-1"),
                user("alice"),
                identity,
                Timestamp::from_unix_nanos(10),
                staged(&quota).await,
            )
            .assured("nothing else is retained under the reference");
        assert!(archives.retains(&reference("restore-1")));
        assert!(
            archives
                .get(&reference("restore-1"), &user("alice"), &identity)
                .is_some()
        );
        assert!(
            archives
                .get(&reference("restore-1"), &user("bob"), &identity)
                .is_none()
        );
        assert!(
            archives
                .get(&reference("restore-1"), &user("alice"), &declared(b"other"))
                .is_none()
        );
        assert!(
            archives
                .get(&reference("restore-2"), &user("alice"), &identity)
                .is_none()
        );
        let refused = archives.retain(
            reference("restore-1"),
            user("bob"),
            identity,
            Timestamp::from_unix_nanos(10),
            staged(&quota).await,
        );
        assert_eq!(
            refused,
            Err(RetentionRefusal::NotOwner {
                reference: reference("restore-1")
            })
        );
        // The refused archive was released; the retained one was not.
        assert_eq!(quota.releases(), BTreeMap::from([(1, 1)]));
    }

    #[nervix_primitives::test]
    async fn a_released_archive_stays_until_the_restore_reading_it_ends() {
        let quota = StdArc::new(Quota::default());
        let archives = RestoreArchives::default();
        let identity = declared(ARCHIVE);
        archives
            .retain(
                reference("restore-1"),
                user("alice"),
                identity,
                Timestamp::from_unix_nanos(10),
                staged(&quota).await,
            )
            .assured("nothing else is retained under the reference");
        let reading = archives
            .get(&reference("restore-1"), &user("alice"), &identity)
            .verified("the archive was retained above");
        archives.release(&reference("restore-1"));
        assert!(!archives.retains(&reference("restore-1")));
        assert!(!quota.released(reading.hold.id));
        drop(reading);
        assert_eq!(quota.releases(), BTreeMap::from([(0, 1)]));
    }

    #[nervix_primitives::test]
    async fn a_retry_replaces_the_archive_an_earlier_attempt_retained() {
        let quota = StdArc::new(Quota::default());
        let archives = RestoreArchives::default();
        let identity = declared(ARCHIVE);
        for _ in 0..2 {
            archives
                .retain(
                    reference("restore-1"),
                    user("alice"),
                    identity,
                    Timestamp::from_unix_nanos(10),
                    staged(&quota).await,
                )
                .assured("the same owner retains under its own reference");
        }
        assert_eq!(quota.releases(), BTreeMap::from([(0, 1)]));
    }

    #[nervix_primitives::test]
    async fn a_sweep_releases_only_the_archives_whose_retention_ended() {
        let quota = StdArc::new(Quota::default());
        let archives = RestoreArchives::default();
        let identity = declared(ARCHIVE);
        for (name, until) in [("restore-1", 10), ("restore-2", 20)] {
            archives
                .retain(
                    reference(name),
                    user("alice"),
                    identity,
                    Timestamp::from_unix_nanos(until),
                    staged(&quota).await,
                )
                .assured("each reference is retained once");
        }
        archives.sweep(Timestamp::from_unix_nanos(9));
        assert!(archives.retains(&reference("restore-1")));
        archives.sweep(Timestamp::from_unix_nanos(10));
        assert!(!archives.retains(&reference("restore-1")));
        assert!(archives.retains(&reference("restore-2")));
        assert_eq!(quota.releases(), BTreeMap::from([(0, 1)]));
    }
}
