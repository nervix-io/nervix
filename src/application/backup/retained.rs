//! The archives completed backups retain on this node, and the stream that downloads one.
//!
//! Layer: control plane.
//!
//! - **Owns.** Retaining an assembled archive under the execution reference of the backup that
//!   assembled it, until a complete download collects it or its retry validity ends; the leases
//!   downloads hold on a retained archive; and the bounded stream that sends one archive to one
//!   client.
//! - **Depends on.** The client wire contract's download frames, and Tokio's bounded channel for
//!   the frames a download has queued.
//! - **Must not know.** How an archive was assembled, where its bytes are staged, or which
//!   transport carries a download.
//!
//! A retained archive is shared by every download that opened it before it was collected. The
//! first download that queues its last frame collects it: the archive leaves the registry, and a
//! later download of the same reference is refused. The archive itself — its staged file and the
//! staging quota behind it — is released when the last holder lets go: the registry, once the
//! archive is collected or expires, and every download still streaming it. So a download that loses
//! its client mid-stream releases only its own hold, the archive stays for the client to download
//! again, and the staging is released exactly once however downloads and expiry interleave.

use std::{collections::BTreeMap, future::Future};

use arch_into::ArchInto as _;
use error_stack::Report;
use nervix_client_wire::{
    BackupArchiveStart, BackupDownloadFailed, BackupDownloadFailure, BackupDownloadFrame,
    BackupDownloadMessage, EncodedFrame, SessionLimits,
};
use nervix_models::{CommandExecutionReference, Timestamp, UserName};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::mpsc;
use triomphe::Arc;

/// Archive bytes one download frame carries at most.
const DOWNLOAD_CHUNK_BYTES: usize = 256 * 1024;

// Under the limits a session transport serves by default, a whole chunk fills at most half a
// frame, far more room than its frame's envelope needs.
const _: () = assert!(DOWNLOAD_CHUNK_BYTES <= SessionLimits::DEFAULT.frame_bytes() / 2);

/// Download frames queued for a client before the stream waits for it to read.
pub(in crate::application) const DOWNLOAD_FRAME_CAPACITY: usize = 4;

/// An archive a backup assembled, as a download needs it.
pub(in crate::application) trait RetainedArtifact:
    Send + Sync + 'static
{
    /// The archive's exact size.
    fn length(&self) -> u64;
    /// The BLAKE3 digest of the archive's bytes.
    fn digest(&self) -> [u8; 32];
}

/// Why a retained archive could not be read while it was downloaded.
#[derive(Debug, Error)]
#[error("the retained archive could not be read")]
pub(in crate::application) struct ArchiveReadFailure;

/// Where a download reads its archive's bytes from, in order.
pub(in crate::application) trait ArchiveBytes: Send {
    type Chunk: AsRef<[u8]> + Send;

    /// The next chunk of at most `limit` bytes, or `None` once every byte was read.
    fn next_chunk(
        &mut self,
        limit: u64,
    ) -> impl Future<Output = Result<Option<Self::Chunk>, Report<ArchiveReadFailure>>> + Send;
}

/// Why a retained archive was not opened for a download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum RetainedArchiveRefusal {
    /// No archive is retained under the reference on this node.
    NotRetained,
    /// The archive's retry validity ended.
    Expired,
    /// Another user ran the backup.
    NotOwner,
}

/// The archives this node retains, by the execution reference of the backup that assembled each.
pub(in crate::application) struct RetainedBackups<A> {
    inner: Arc<RetainedBackupsInner<A>>,
}

struct RetainedBackupsInner<A> {
    archives: Mutex<BTreeMap<CommandExecutionReference, RetainedEntry<A>>>,
}

struct RetainedEntry<A> {
    owner: UserName,
    retained_until: Timestamp,
    archive: Arc<A>,
}

impl<A> Clone for RetainedBackups<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<A> Default for RetainedBackups<A> {
    fn default() -> Self {
        Self {
            inner: Arc::new(RetainedBackupsInner {
                archives: Mutex::new(BTreeMap::new()),
            }),
        }
    }
}

impl<A> RetainedBackups<A> {
    /// Retains `archive` for `owner` under `reference` until `retained_until`. An archive the same
    /// backup assembled before, on an earlier attempt, is replaced; downloads already streaming it
    /// keep it until they end.
    pub(in crate::application) fn retain(
        &self,
        reference: CommandExecutionReference,
        owner: UserName,
        retained_until: Timestamp,
        archive: A,
    ) {
        let entry = RetainedEntry {
            owner,
            retained_until,
            archive: Arc::new(archive),
        };
        let replaced = self.inner.archives.lock().insert(reference, entry);
        // The replaced archive is released here, outside the lock, unless a download holds it.
        drop(replaced);
    }

    /// Opens the archive retained under `reference` for a download by `user` at `now`.
    pub(in crate::application) fn open(
        &self,
        reference: &CommandExecutionReference,
        user: &UserName,
        now: Timestamp,
    ) -> Result<ArchiveLease<A>, RetainedArchiveRefusal> {
        let mut archives = self.inner.archives.lock();
        let Some(entry) = archives.get(reference) else {
            return Err(RetainedArchiveRefusal::NotRetained);
        };
        if now >= entry.retained_until {
            let expired = archives.remove(reference);
            drop(archives);
            drop(expired);
            return Err(RetainedArchiveRefusal::Expired);
        }
        if entry.owner != *user {
            return Err(RetainedArchiveRefusal::NotOwner);
        }
        Ok(ArchiveLease {
            registry: self.clone(),
            reference: reference.clone(),
            archive: entry.archive.clone(),
        })
    }

    /// Releases every archive whose retry validity ended by `now`. A download still streaming one
    /// keeps it until it ends.
    pub(in crate::application) fn sweep(&self, now: Timestamp) {
        let mut expired = Vec::new();
        {
            let mut archives = self.inner.archives.lock();
            // Every entry is visited: an archive is retained only while its backup's retry
            // validity lasts, so the registry holds the backups of that one window, which is what
            // bounds this walk.
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
        // Dropping the entries after the lock is released keeps a staging release, which removes
        // a file, off the registry's critical section.
        drop(expired);
    }

    /// Whether an archive is retained under `reference`.
    #[cfg(test)]
    pub(in crate::application) fn retains(&self, reference: &CommandExecutionReference) -> bool {
        self.inner.archives.lock().contains_key(reference)
    }

    /// Removes the archive `lease` holds, if it is still the one retained under its reference.
    fn collect(&self, reference: &CommandExecutionReference, archive: &Arc<A>) -> bool {
        let mut archives = self.inner.archives.lock();
        let current = match archives.get(reference) {
            Some(entry) => Arc::ptr_eq(&entry.archive, archive),
            None => false,
        };
        if !current {
            return false;
        }
        let collected = archives.remove(reference);
        drop(archives);
        drop(collected);
        true
    }
}

/// A download's hold on one retained archive. The archive stays readable for as long as the lease
/// lives, whether or not the registry still retains it.
pub(in crate::application) struct ArchiveLease<A> {
    registry: RetainedBackups<A>,
    reference: CommandExecutionReference,
    archive: Arc<A>,
}

impl<A> ArchiveLease<A> {
    pub(in crate::application) fn archive(&self) -> &A {
        &self.archive
    }

    /// Collects the archive after a download queued its last frame. Exactly one lease of an
    /// archive collects it, whatever order concurrent downloads finish in; `false` means another
    /// download collected it first, or it was replaced or expired.
    pub(in crate::application) fn collect(self) -> bool {
        self.registry.collect(&self.reference, &self.archive)
    }
}

/// How one download ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum DownloadEnd {
    /// Every frame was queued for the client, and this download collected the archive.
    Collected,
    /// Every frame was queued for the client, and another download had already collected the
    /// archive, or it expired while this one streamed it.
    AlreadyCollected,
    /// The client stopped reading before the last frame. The archive stays retained.
    ClientGone,
    /// The retained archive could not be read. The download ended with a refusal, and the archive
    /// stays retained.
    ReadFailed,
}

/// Streams the archive `lease` holds to one client, at most [`DOWNLOAD_FRAME_CAPACITY`] frames
/// ahead of it.
///
/// Every send waits while the client's queue is full, so a slow client slows the reads instead of
/// growing what the node holds for it. A client that goes away ends the stream at its next send,
/// and the lease is released without collecting the archive.
pub(in crate::application) async fn stream_archive<A, S>(
    lease: ArchiveLease<A>,
    mut source: S,
    frames: mpsc::Sender<EncodedFrame<BackupDownloadFrame>>,
    limits: SessionLimits,
) -> DownloadEnd
where
    A: RetainedArtifact,
    S: ArchiveBytes,
{
    let start = match archive_start(lease.archive()) {
        Some(start) => start,
        None => return refuse_read(&frames, &limits).await,
    };
    let Ok(frame) = BackupDownloadMessage::encode_start(&start, &limits) else {
        return refuse_read(&frames, &limits).await;
    };
    if frames.send(frame).await.is_err() {
        return DownloadEnd::ClientGone;
    }
    // A chunk fills at most half a frame of the session's own limits, which leaves its frame's
    // envelope more room than it needs.
    let chunk_bytes = DOWNLOAD_CHUNK_BYTES.min(limits.frame_bytes() / 2);
    let chunk_bytes: u64 = chunk_bytes.arch_into();
    loop {
        tokio::task::consume_budget().await;
        let chunk = match source.next_chunk(chunk_bytes).await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(_) => return refuse_read(&frames, &limits).await,
        };
        let Ok(frame) = BackupDownloadMessage::encode_chunk(chunk.as_ref(), &limits) else {
            return refuse_read(&frames, &limits).await;
        };
        if frames.send(frame).await.is_err() {
            return DownloadEnd::ClientGone;
        }
    }
    let Ok(frame) = BackupDownloadMessage::encode_complete(&limits) else {
        return refuse_read(&frames, &limits).await;
    };
    if frames.send(frame).await.is_err() {
        return DownloadEnd::ClientGone;
    }
    if lease.collect() {
        return DownloadEnd::Collected;
    }
    DownloadEnd::AlreadyCollected
}

/// The start frame of an archive, which only a non-empty archive has.
fn archive_start(archive: &impl RetainedArtifact) -> Option<BackupArchiveStart> {
    let total_bytes = std::num::NonZeroU64::new(archive.length())?;
    Some(BackupArchiveStart {
        total_bytes,
        digest: nervix_models::ArchiveDigest::from_bytes(archive.digest()),
    })
}

/// Ends a download whose archive could not be read with a refusal, if the client still reads.
async fn refuse_read(
    frames: &mpsc::Sender<EncodedFrame<BackupDownloadFrame>>,
    limits: &SessionLimits,
) -> DownloadEnd {
    let failed = BackupDownloadFailed {
        failure: BackupDownloadFailure::ReadFailed,
        message: "the retained archive could not be read; download it again".to_string(),
    };
    if let Ok(frame) = BackupDownloadMessage::encode_failed(&failed, limits) {
        // The refusal is the stream's last frame; a client that already left has nothing to read.
        if frames.send(frame).await.is_err() {
            return DownloadEnd::ClientGone;
        }
    }
    DownloadEnd::ReadFailed
}

#[cfg(test)]
pub(in crate::application) mod test_archives {
    //! Retained archives held in memory, which count how often they are released.

    use std::sync::{
        Arc as StdArc,
        atomic::{AtomicUsize, Ordering},
    };

    use error_stack::Report;

    use super::{ArchiveBytes, ArchiveReadFailure, RetainedArtifact};

    /// An archive in memory whose release is counted.
    pub(in crate::application) struct MemoryArchive {
        pub(in crate::application) bytes: Vec<u8>,
        pub(in crate::application) releases: StdArc<AtomicUsize>,
    }

    impl RetainedArtifact for MemoryArchive {
        fn length(&self) -> u64 {
            u64::try_from(self.bytes.len()).unwrap_or(u64::MAX)
        }

        fn digest(&self) -> [u8; 32] {
            *blake3::hash(&self.bytes).as_bytes()
        }
    }

    impl Drop for MemoryArchive {
        fn drop(&mut self) {
            self.releases.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The bytes of an in-memory archive, read in chunks.
    pub(in crate::application) struct MemoryBytes {
        pub(in crate::application) bytes: Vec<u8>,
        pub(in crate::application) offset: usize,
        /// The chunk after which every read fails, when a test makes reads fail.
        pub(in crate::application) fail_after: Option<usize>,
        /// Counts every chunk read, when a test watches how far a stream reads ahead.
        pub(in crate::application) chunks_read: Option<StdArc<AtomicUsize>>,
    }

    impl ArchiveBytes for MemoryBytes {
        type Chunk = Vec<u8>;

        async fn next_chunk(
            &mut self,
            limit: u64,
        ) -> Result<Option<Vec<u8>>, Report<ArchiveReadFailure>> {
            if let Some(remaining) = self.fail_after.as_mut() {
                if *remaining == 0 {
                    return Err(Report::new(ArchiveReadFailure));
                }
                *remaining -= 1;
            }
            if self.offset >= self.bytes.len() {
                return Ok(None);
            }
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            let end = self.bytes.len().min(self.offset.saturating_add(limit));
            let chunk = self.bytes[self.offset..end].to_vec();
            self.offset = end;
            if let Some(chunks_read) = &self.chunks_read {
                chunks_read.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Some(chunk))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc as StdArc,
        atomic::{AtomicUsize, Ordering},
    };

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_client_wire::{BackupDownloadMessage, SessionLimits};

    use super::{test_archives::*, *};

    fn reference(raw: &str) -> CommandExecutionReference {
        CommandExecutionReference::parse(raw).assured("the test reference is valid")
    }

    fn user(raw: &str) -> UserName {
        UserName::parse(raw).assured("the test user is valid")
    }

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_unix_nanos(seconds * 1_000_000_000)
    }

    fn archive(length: usize, releases: &StdArc<AtomicUsize>) -> MemoryArchive {
        MemoryArchive {
            bytes: (0..length).map(|value| value.to_le_bytes()[0]).collect(),
            releases: releases.clone(),
        }
    }

    fn bytes_of(archive: &MemoryArchive) -> MemoryBytes {
        MemoryBytes {
            bytes: archive.bytes.clone(),
            offset: 0,
            fail_after: None,
            chunks_read: None,
        }
    }

    /// Reads every frame a download sends and reassembles its archive.
    async fn receive(mut frames: mpsc::Receiver<EncodedFrame<BackupDownloadFrame>>) -> Vec<u8> {
        let limits = SessionLimits::DEFAULT;
        let mut bytes = Vec::new();
        let mut started = false;
        let mut completed = false;
        while let Some(frame) = frames.recv().await {
            let frame = frame.verify(&limits).assured("a download frame verifies");
            match BackupDownloadMessage::decode(&frame).assured("a download frame decodes") {
                BackupDownloadMessage::Start(_) => started = true,
                BackupDownloadMessage::Chunk(chunk) => bytes.extend_from_slice(chunk.bytes()),
                BackupDownloadMessage::Complete => completed = true,
                other => panic!("an intact download sends no {other:?}"),
            }
        }
        assert!(started && completed, "a download starts and completes");
        bytes
    }

    #[tokio::test]
    async fn a_complete_download_collects_the_archive_and_releases_it_once() {
        let releases = StdArc::new(AtomicUsize::new(0));
        let registry = RetainedBackups::default();
        let archive = archive(3 * 256 * 1024 + 17, &releases);
        let expected = archive.bytes.clone();
        let source = bytes_of(&archive);
        registry.retain(reference("backup-1"), user("alice"), at(100), archive);

        let lease = registry
            .open(&reference("backup-1"), &user("alice"), at(10))
            .assured("the owner opens a retained archive");
        let (sender, receiver) = mpsc::channel(DOWNLOAD_FRAME_CAPACITY);
        let received = tokio::spawn(receive(receiver));
        let end = stream_archive(lease, source, sender, SessionLimits::DEFAULT).await;
        assert_eq!(end, DownloadEnd::Collected);
        assert_eq!(received.await.assured("the receiver finishes"), expected);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
        assert!(!registry.retains(&reference("backup-1")));
        assert_eq!(
            registry
                .open(&reference("backup-1"), &user("alice"), at(10))
                .err(),
            Some(RetainedArchiveRefusal::NotRetained)
        );
    }

    #[test]
    fn an_archive_opens_only_for_its_owner_and_before_it_expires() {
        let releases = StdArc::new(AtomicUsize::new(0));
        let registry = RetainedBackups::default();
        registry.retain(
            reference("backup-2"),
            user("alice"),
            at(100),
            archive(10, &releases),
        );
        assert_eq!(
            registry
                .open(&reference("backup-2"), &user("mallory"), at(10))
                .err(),
            Some(RetainedArchiveRefusal::NotOwner)
        );
        assert_eq!(
            registry
                .open(&reference("unknown"), &user("alice"), at(10))
                .err(),
            Some(RetainedArchiveRefusal::NotRetained)
        );
        assert_eq!(
            registry
                .open(&reference("backup-2"), &user("alice"), at(100))
                .err(),
            Some(RetainedArchiveRefusal::Expired)
        );
        assert_eq!(releases.load(Ordering::SeqCst), 1);
        assert!(!registry.retains(&reference("backup-2")));
    }

    #[test]
    fn a_sweep_releases_expired_archives_and_keeps_the_rest() {
        let releases = StdArc::new(AtomicUsize::new(0));
        let registry = RetainedBackups::default();
        registry.retain(
            reference("old"),
            user("alice"),
            at(50),
            archive(1, &releases),
        );
        registry.retain(
            reference("new"),
            user("alice"),
            at(150),
            archive(1, &releases),
        );
        let lease = registry
            .open(&reference("old"), &user("alice"), at(10))
            .assured("the archive is open before it expires");
        registry.sweep(at(100));
        assert!(!registry.retains(&reference("old")));
        assert!(registry.retains(&reference("new")));
        assert_eq!(
            releases.load(Ordering::SeqCst),
            0,
            "a download still holds the expired archive"
        );
        assert!(
            !lease.collect(),
            "an expired archive is no longer collected"
        );
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_retried_backup_replaces_its_earlier_archive() {
        let releases = StdArc::new(AtomicUsize::new(0));
        let registry = RetainedBackups::default();
        registry.retain(
            reference("again"),
            user("alice"),
            at(100),
            archive(1, &releases),
        );
        let earlier = registry
            .open(&reference("again"), &user("alice"), at(10))
            .assured("the first archive opens");
        registry.retain(
            reference("again"),
            user("alice"),
            at(100),
            archive(2, &releases),
        );
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        assert!(
            !earlier.collect(),
            "a replaced archive is not the one retained"
        );
        assert_eq!(releases.load(Ordering::SeqCst), 1);
        let current = registry
            .open(&reference("again"), &user("alice"), at(10))
            .assured("the replacement opens");
        assert_eq!(current.archive().bytes.len(), 2);
        assert!(current.collect());
        assert_eq!(releases.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_download_that_loses_its_client_keeps_the_archive_retained() {
        let releases = StdArc::new(AtomicUsize::new(0));
        let registry = RetainedBackups::default();
        let archive = archive(8 * 256 * 1024, &releases);
        let source = bytes_of(&archive);
        registry.retain(reference("lost"), user("alice"), at(100), archive);
        let lease = registry
            .open(&reference("lost"), &user("alice"), at(10))
            .assured("the owner opens a retained archive");
        let (sender, mut receiver) = mpsc::channel(DOWNLOAD_FRAME_CAPACITY);
        let reader = tokio::spawn(async move {
            receiver.recv().await.assured("the start frame arrives");
            drop(receiver);
        });
        let end = stream_archive(lease, source, sender, SessionLimits::DEFAULT).await;
        reader.await.assured("the reader finishes");
        assert_eq!(end, DownloadEnd::ClientGone);
        assert!(registry.retains(&reference("lost")));
        assert_eq!(releases.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_archive_that_cannot_be_read_ends_its_download_with_a_refusal() {
        let releases = StdArc::new(AtomicUsize::new(0));
        let registry = RetainedBackups::default();
        let archive = archive(4 * 256 * 1024, &releases);
        let mut source = bytes_of(&archive);
        source.fail_after = Some(2);
        registry.retain(reference("broken"), user("alice"), at(100), archive);
        let lease = registry
            .open(&reference("broken"), &user("alice"), at(10))
            .assured("the owner opens a retained archive");
        let (sender, mut receiver) = mpsc::channel(16);
        let end = stream_archive(lease, source, sender, SessionLimits::DEFAULT).await;
        assert_eq!(end, DownloadEnd::ReadFailed);
        let mut last = None;
        while let Some(frame) = receiver.recv().await {
            let frame = frame
                .verify(&SessionLimits::DEFAULT)
                .assured("a download frame verifies");
            last = Some(BackupDownloadMessage::decode(&frame).assured("a download frame decodes"));
        }
        let Some(BackupDownloadMessage::Failed(failed)) = last else {
            panic!("the download ends with a refusal: {last:?}");
        };
        assert_eq!(failed.failure, BackupDownloadFailure::ReadFailed);
        assert!(registry.retains(&reference("broken")));
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    //! The download stream's invariants under every interleaving Shuttle explores: a stream never
    //! runs more than its capacity ahead of its client, a lost client releases its hold without
    //! collecting, concurrent downloads collect an archive exactly once, and the archive is
    //! released exactly once however they finish.

    use std::sync::{
        Arc as StdArc,
        atomic::{AtomicUsize, Ordering},
    };

    use nervix_client_wire::{BackupDownloadMessage, SessionLimits};
    use shuttle::{future::block_on, thread};

    use super::{test_archives::*, *};
    use crate::shuttle_test::check_interleavings;

    /// Enough bytes for several chunks, so a stream has frames to run ahead with.
    const ARCHIVE_BYTES: usize = 3 * 256 * 1024 + 1;
    /// The frames an intact download of the archive sends: its start, one per chunk, completion.
    const DOWNLOAD_FRAMES: usize = 1 + 4 + 1;

    const MODEL_THREAD_JOINS: &str =
        "Shuttle fails the whole execution when a model thread panics, so no join observes one";

    fn reference() -> CommandExecutionReference {
        CommandExecutionReference::parse("backup-under-shuttle")
            .unwrap_or_else(|_| panic!("the model reference is valid"))
    }

    fn owner() -> UserName {
        UserName::parse("alice").unwrap_or_else(|_| panic!("the model user is valid"))
    }

    fn retained(releases: &StdArc<AtomicUsize>) -> (RetainedBackups<MemoryArchive>, Vec<u8>) {
        let registry = RetainedBackups::default();
        let bytes = (0..ARCHIVE_BYTES)
            .map(|value| value.to_le_bytes()[0])
            .collect::<Vec<_>>();
        registry.retain(
            reference(),
            owner(),
            Timestamp::from_unix_nanos(i64::MAX),
            MemoryArchive {
                bytes: bytes.clone(),
                releases: releases.clone(),
            },
        );
        (registry, bytes)
    }

    fn source(bytes: &[u8]) -> MemoryBytes {
        MemoryBytes {
            bytes: bytes.to_vec(),
            offset: 0,
            fail_after: None,
            chunks_read: None,
        }
    }

    fn now() -> Timestamp {
        Timestamp::from_unix_nanos(0)
    }

    /// A slow client: it yields after every read, while the source counts every chunk the stream
    /// took from it. Each chunk becomes one frame after the start frame, so what the stream has
    /// read and the client has not can never exceed the queue's capacity and the one chunk the
    /// stream holds while it waits to send it.
    fn bounded_stream_waits_for_its_client() {
        let releases = StdArc::new(AtomicUsize::new(0));
        let (registry, bytes) = retained(&releases);
        let lease = registry
            .open(&reference(), &owner(), now())
            .unwrap_or_else(|refusal| panic!("the archive opens: {refusal:?}"));
        let (sender, mut receiver) = mpsc::channel(DOWNLOAD_FRAME_CAPACITY);
        let chunks_read = StdArc::new(AtomicUsize::new(0));
        let mut counted = source(&bytes);
        counted.chunks_read = Some(chunks_read.clone());
        let limits = SessionLimits::DEFAULT;
        let stream =
            thread::spawn(move || block_on(stream_archive(lease, counted, sender, limits)));
        let mut frames_read = 0_usize;
        while block_on(receiver.recv()).is_some() {
            frames_read += 1;
            let produced = chunks_read.load(Ordering::SeqCst) + 1;
            let ahead = produced.saturating_sub(frames_read);
            assert!(
                ahead <= DOWNLOAD_FRAME_CAPACITY + 1,
                "the stream read {ahead} frames ahead of its client"
            );
            thread::yield_now();
        }
        assert_eq!(frames_read, DOWNLOAD_FRAMES);
        assert_eq!(
            stream
                .join()
                .unwrap_or_else(|_| panic!("{MODEL_THREAD_JOINS}")),
            DownloadEnd::Collected
        );
        assert!(!registry.retains(&reference()));
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    /// A client leaves after the start frame while its download streams. The download releases its
    /// hold without collecting, the archive stays retained, and it is released exactly once when
    /// it expires.
    fn a_lost_client_releases_its_hold_once() {
        let releases = StdArc::new(AtomicUsize::new(0));
        let (registry, bytes) = retained(&releases);
        let lease = registry
            .open(&reference(), &owner(), now())
            .unwrap_or_else(|refusal| panic!("the archive opens: {refusal:?}"));
        let (sender, mut receiver) = mpsc::channel(DOWNLOAD_FRAME_CAPACITY);
        let limits = SessionLimits::DEFAULT;
        let stream =
            thread::spawn(move || block_on(stream_archive(lease, source(&bytes), sender, limits)));
        let client = thread::spawn(move || {
            block_on(async move {
                let first = receiver.recv().await;
                assert!(first.is_some(), "the start frame arrives");
                drop(receiver);
            })
        });
        client
            .join()
            .unwrap_or_else(|_| panic!("{MODEL_THREAD_JOINS}"));
        let end = stream
            .join()
            .unwrap_or_else(|_| panic!("{MODEL_THREAD_JOINS}"));
        // The queue holds fewer frames than the download sends, and the client read only one, so
        // the stream always meets the closed queue before its last frame.
        assert_eq!(end, DownloadEnd::ClientGone);
        assert!(registry.retains(&reference()));
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        registry.sweep(Timestamp::from_unix_nanos(i64::MAX));
        assert!(!registry.retains(&reference()));
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    /// Two clients download one archive at once while the registry is swept. Exactly one download
    /// collects it, both receive every byte, and the archive is released exactly once.
    fn concurrent_downloads_collect_once() {
        let releases = StdArc::new(AtomicUsize::new(0));
        let (registry, bytes) = retained(&releases);
        let collected = StdArc::new(AtomicUsize::new(0));
        let mut downloads = Vec::new();
        for _ in 0..2 {
            let lease = registry
                .open(&reference(), &owner(), now())
                .unwrap_or_else(|refusal| panic!("the archive opens: {refusal:?}"));
            let bytes = bytes.clone();
            let collected = collected.clone();
            downloads.push(thread::spawn(move || {
                block_on(async move {
                    let (sender, mut receiver) =
                        mpsc::channel::<EncodedFrame<BackupDownloadFrame>>(DOWNLOAD_FRAME_CAPACITY);
                    let limits = SessionLimits::DEFAULT;
                    let expected = bytes.clone();
                    let reader = shuttle::future::spawn(async move {
                        let mut received = Vec::new();
                        while let Some(frame) = receiver.recv().await {
                            let frame = frame
                                .verify(&limits)
                                .unwrap_or_else(|_| panic!("a download frame verifies"));
                            if let Ok(BackupDownloadMessage::Chunk(chunk)) =
                                BackupDownloadMessage::decode(&frame)
                            {
                                received.extend_from_slice(chunk.bytes());
                            }
                        }
                        received
                    });
                    let end = stream_archive(lease, source(&bytes), sender, limits).await;
                    let received = reader
                        .await
                        .unwrap_or_else(|_| panic!("the reader finishes"));
                    assert_eq!(received, expected, "every download receives every byte");
                    match end {
                        DownloadEnd::Collected => {
                            collected.fetch_add(1, Ordering::SeqCst);
                        }
                        DownloadEnd::AlreadyCollected => {}
                        other => panic!("an intact download ends with {other:?}"),
                    }
                })
            }));
        }
        let sweeper = {
            let registry = registry.clone();
            thread::spawn(move || registry.sweep(now()))
        };
        sweeper
            .join()
            .unwrap_or_else(|_| panic!("{MODEL_THREAD_JOINS}"));
        for download in downloads {
            download
                .join()
                .unwrap_or_else(|_| panic!("{MODEL_THREAD_JOINS}"));
        }
        assert_eq!(collected.load(Ordering::SeqCst), 1);
        assert!(!registry.retains(&reference()));
        assert_eq!(
            registry.open(&reference(), &owner(), now()).err(),
            Some(RetainedArchiveRefusal::NotRetained)
        );
        drop(registry);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shuttle_a_download_stays_bounded_and_waits_for_its_client() {
        check_interleavings(bounded_stream_waits_for_its_client);
    }

    #[test]
    fn shuttle_a_lost_client_releases_its_hold_exactly_once() {
        check_interleavings(a_lost_client_releases_its_hold_once);
    }

    #[test]
    fn shuttle_concurrent_downloads_collect_an_archive_exactly_once() {
        check_interleavings(concurrent_downloads_collect_once);
    }
}
