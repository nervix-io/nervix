//! Recovery and publication checks through the Raft storage traits.

use std::{collections::BTreeMap, time::Duration};

use nervix_models::{
    ClusterNodeName, DomainConfig, DomainName, DomainPace, DomainSchedule, DomainStartPoint,
    DomainState, DomainStatus, ResourceName, ResourceNodeState, ResourceNodeStatus,
    ResourceReplicaKey,
};
use openraft::{entry::RaftEntry as _, storage::RaftLogStorageExt as _, vote::RaftLeaderId as _};
use tempfile::TempDir;

use super::*;
use crate::{
    ConsensusCommand, ConsensusResponse,
    durable_batch::{CommitBackend, Mutation},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Harness {
    directory: TempDir,
    store: FjallStore,
    executor: Executor,
}

impl Harness {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let executor = Executor::default();
        let store = FjallStore::from_database(
            Database::builder(directory.path()).open()?,
            executor.clone(),
        )
        .await?;
        Ok(Self {
            directory,
            store,
            executor,
        })
    }
    async fn reopen(self) -> Result<Self, Box<dyn std::error::Error>> {
        let Self {
            directory,
            store,
            executor,
        } = self;
        drop(store);
        let store = FjallStore::from_database(
            Database::builder(directory.path()).open()?,
            executor.clone(),
        )
        .await?;
        Ok(Self {
            directory,
            store,
            executor,
        })
    }
    fn node() -> ClusterNodeName {
        use meticulous::ResultExt as _;
        ClusterNodeName::parse("node-1").assured("the fixture uses a valid literal node name")
    }
    fn log_id(index: u64) -> LogIdOf {
        LogIdOf::new(
            openraft::type_config::alias::CommittedLeaderIdOf::<TypeConfig>::new(3, Self::node()),
            index,
        )
    }
    fn domain(name: &str) -> DomainState {
        use meticulous::ResultExt as _;
        DomainState {
            id: DomainName::try_from(name).assured("the fixture generates identifier-shaped names"),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "1s".into(),
                skew: "0s".into(),
                placement: Default::default(),
            },
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: DomainStartPoint::Resume,
            clock: None,
        }
    }
    /// Copy one generation's sections into this store the way an arriving transfer does.
    async fn receive(&self, source: &SealedSnapshot) -> Result<SealedSnapshot, io::Error> {
        let manifest = source.manifest().clone();
        let generation = self.store.claim_snapshot_generation();
        for index in 0..manifest.section_count {
            let bytes = source.section(index).await?;
            self.store
                .stage_snapshot_section(generation, index, bytes)
                .await?;
        }
        Ok(self.store.open_staged_snapshot(SnapshotManifest {
            generation,
            ..manifest
        }))
    }

    async fn read_sections(snapshot: &SealedSnapshot) -> Result<Vec<Vec<u8>>, io::Error> {
        let mut sections = Vec::new();
        for index in 0..snapshot.manifest().section_count {
            sections.push(snapshot.section(index).await?);
        }
        Ok(sections)
    }

    fn stored_generations(&self) -> Result<BTreeSet<u64>, io::Error> {
        let mut generations = BTreeSet::new();
        for item in self.store.inner.snapshot.iter() {
            let key = item.key().map_err(io::Error::other)?;
            if let Some(generation) = section_generation(&key) {
                generations.insert(generation);
            }
        }
        Ok(generations)
    }

    fn entry(index: u64, command: ConsensusCommand) -> EntryOf<TypeConfig> {
        EntryOf::<TypeConfig> {
            log_id: Self::log_id(index),
            payload: EntryPayload::Normal(command),
        }
    }
    async fn apply(&mut self, index: u64, command: ConsensusCommand) -> io::Result<()> {
        self.store
            .apply(futures_util::stream::iter([Ok((
                Self::entry(index, command),
                None,
            ))]))
            .await
    }
    async fn append(&mut self, end: u64) -> io::Result<()> {
        self.store
            .blocking_append(
                (1..=end).map(|index| EntryOf::<TypeConfig>::new_blank(Self::log_id(index))),
            )
            .await
            .map_err(io::Error::other)
    }
}

#[tokio::test]
async fn record_state_and_applied_position_recover_together_at_each_boundary() -> TestResult {
    for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
        tokio::task::consume_budget().await;
        let mut harness = Harness::new().await?;
        let domain = Harness::domain("tenant");
        let schedule = DomainSchedule::new(domain.id.clone(), [], vec![]);
        harness
            .apply(
                1,
                ConsensusCommand::PutDomainAndSchedule {
                    expected_domain: None,
                    expected_schedule: None,
                    domain: Box::new(domain.clone()),
                    schedule: Some(Box::new(schedule.clone())),
                },
            )
            .await?;
        let preceding = harness.store.inner.state();
        let domain_watch = harness.store.inner.domain_tx.subscribe();
        let schedule_watch = harness.store.inner.schedule_tx.subscribe();
        let mut changed_domain = domain.clone();
        changed_domain.status = DomainStatus::Running;
        let command = ConsensusCommand::PutDomainAndSchedule {
            expected_domain: Some(Box::new(domain)),
            expected_schedule: Some(Box::new(schedule)),
            domain: Box::new(changed_domain.clone()),
            schedule: None,
        };
        harness
            .store
            .inner
            .faults
            .fail_next(command.to_string(), boundary);
        assert!(harness.apply(2, command).await.is_err());
        assert_eq!(harness.store.inner.state(), preceding);
        assert!(!domain_watch.has_changed()?);
        assert!(!schedule_watch.has_changed()?);
        assert!(
            harness
                .store
                .save_vote(&VoteOf::new(9, Harness::node()))
                .await
                .is_err()
        );
        drop(domain_watch);
        drop(schedule_watch);
        let mut harness = harness.reopen().await?;
        let recovered = harness.store.inner.state();
        match boundary {
            StorageBoundary::BeforeCommit => assert_eq!(recovered, preceding),
            StorageBoundary::AfterSync => {
                assert_eq!(
                    recovered.domains.get(&changed_domain.id),
                    Some(&changed_domain)
                );
                assert!(recovered.schedule.domain(&changed_domain.id).is_none());
                assert_eq!(recovered.runtime_revision, 2);
                assert_eq!(
                    harness.store.applied_state().await?.0,
                    Some(Harness::log_id(2))
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn votes_appends_and_commit_positions_report_failures_and_recover_durable_values()
-> TestResult {
    for operation in ["vote", "append", "committed"] {
        tokio::task::consume_budget().await;
        for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
            tokio::task::consume_budget().await;
            let mut harness = Harness::new().await?;
            harness
                .store
                .inner
                .faults
                .fail_next(operation.to_owned(), boundary);
            let result = match operation {
                "vote" => {
                    harness
                        .store
                        .save_vote(&VoteOf::new(3, Harness::node()))
                        .await
                }
                "append" => harness.append(1).await,
                _ => harness.store.save_committed(Some(Harness::log_id(1))).await,
            };
            assert!(
                result.is_err(),
                "{operation} must report the failed boundary"
            );
            let mut harness = harness.reopen().await?;
            let expected = boundary == StorageBoundary::AfterSync;
            match operation {
                "vote" => assert_eq!(
                    harness
                        .store
                        .get_log_reader()
                        .await
                        .read_vote()
                        .await?
                        .is_some(),
                    expected
                ),
                "append" => assert_eq!(
                    harness.store.get_log_state().await?.last_log_id.is_some(),
                    expected
                ),
                _ => assert_eq!(harness.store.read_committed().await?.is_some(), expected),
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn responses_and_notifications_wait_for_storage_on_a_single_async_worker() -> TestResult {
    for operation in ["vote", "append", "committed", "put-domain:tenant"] {
        tokio::task::consume_budget().await;
        let harness = Harness::new().await?;
        let pause = harness
            .store
            .inner
            .faults
            .pause_next(operation.to_owned(), StorageBoundary::BeforeCommit);
        let notifications = harness.store.inner.domain_tx.subscribe();
        let mut store = harness.store.clone();
        let task = tokio::spawn(async move {
            match operation {
                "vote" => store.save_vote(&VoteOf::new(3, Harness::node())).await,
                "append" => store
                    .blocking_append([EntryOf::<TypeConfig>::new_blank(Harness::log_id(1))])
                    .await
                    .map_err(io::Error::other),
                "committed" => store.save_committed(Some(Harness::log_id(1))).await,
                _ => {
                    store
                        .apply(futures_util::stream::iter([Ok((
                            Harness::entry(
                                1,
                                ConsensusCommand::PutDomain {
                                    domain: Box::new(Harness::domain("tenant")),
                                },
                            ),
                            None,
                        ))]))
                        .await
                }
            }
        });
        tokio::time::timeout(Duration::from_secs(10), pause.entered()).await?;
        assert!(
            !task.is_finished(),
            "{operation} acknowledged before the durable write"
        );
        assert!(!notifications.has_changed()?);
        assert_eq!(harness.store.inner.state().last_applied_log_id, None);
        pause.release();
        task.await??;
    }
    Ok(())
}

#[tokio::test]
async fn a_queued_vote_runs_between_ready_append_entries() -> TestResult {
    let harness = Harness::new().await?;
    let append_pause = harness
        .store
        .inner
        .faults
        .pause_next("append".into(), StorageBoundary::BeforeCommit);
    let mut log = harness.store.clone();
    let append = tokio::spawn(async move {
        log.blocking_append(
            (1..=2).map(|index| EntryOf::<TypeConfig>::new_blank(Harness::log_id(index))),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), append_pause.entered()).await?;
    let vote_pause = harness
        .store
        .inner
        .faults
        .pause_next("vote".into(), StorageBoundary::BeforeCommit);
    let mut log = harness.store.clone();
    let vote = tokio::spawn(async move { log.save_vote(&VoteOf::new(4, Harness::node())).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while harness.executor.snapshot().consensus_storage.pending == 0 {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    })
    .await?;
    append_pause.release();
    tokio::time::timeout(Duration::from_secs(10), vote_pause.entered()).await?;
    assert!(!append.is_finished());
    vote_pause.release();
    vote.await??;
    append.await??;
    Ok(())
}

#[tokio::test]
async fn idle_wait_joins_a_blocking_job_abandoned_by_its_async_caller() -> TestResult {
    let harness = Harness::new().await?;
    let pause = harness
        .store
        .inner
        .faults
        .pause_next("append".into(), StorageBoundary::BeforeCommit);
    let mut log = harness.store.clone();
    let append = tokio::spawn(async move {
        log.blocking_append([EntryOf::<TypeConfig>::new_blank(Harness::log_id(1))])
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), pause.entered()).await?;

    append.abort();
    let Err(cancelled) = append.await else {
        panic!("the abandoned append future must be cancelled");
    };
    assert!(cancelled.is_cancelled());

    let store = harness.store.clone();
    let idle = tokio::spawn(async move { store.wait_for_idle().await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while harness.executor.snapshot().consensus_storage.pending == 0 {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(!idle.is_finished(), "the idle barrier passed a running job");

    pause.release();
    idle.await??;
    let Harness {
        directory,
        store,
        executor,
    } = harness;
    drop(store);
    drop(executor);
    let reopened = Database::builder(directory.path()).open()?;
    drop(reopened);
    Ok(())
}

#[tokio::test]
async fn purge_and_truncation_recover_with_their_position_metadata() -> TestResult {
    for operation in ["purge", "truncate"] {
        tokio::task::consume_budget().await;
        for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
            tokio::task::consume_budget().await;
            let mut harness = Harness::new().await?;
            harness.append(6).await?;
            harness
                .store
                .inner
                .faults
                .fail_next(operation.into(), boundary);
            let result = if operation == "purge" {
                harness.store.purge(Harness::log_id(3)).await
            } else {
                harness.store.truncate_after(Some(Harness::log_id(3))).await
            };
            assert!(result.is_err());
            let mut harness = harness.reopen().await?;
            let state = harness.store.get_log_state().await?;
            let entries = harness
                .store
                .get_log_reader()
                .await
                .try_get_log_entries(..)
                .await?;
            let indexes: Vec<_> = entries.iter().map(|entry| entry.log_id.index).collect();
            match (operation, boundary) {
                ("purge", StorageBoundary::AfterSync) => {
                    assert_eq!(indexes, vec![4, 5, 6]);
                    assert_eq!(state.last_purged_log_id, Some(Harness::log_id(3)));
                }
                ("truncate", StorageBoundary::AfterSync) => {
                    assert_eq!(indexes, vec![1, 2, 3]);
                    assert_eq!(state.last_log_id, Some(Harness::log_id(3)));
                }
                _ => {
                    assert_eq!(indexes, vec![1, 2, 3, 4, 5, 6]);
                    assert_eq!(state.last_purged_log_id, None);
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn snapshots_recover_state_and_snapshot_metadata_atomically() -> TestResult {
    let mut source = Harness::new().await?;
    source
        .apply(
            1,
            ConsensusCommand::PutDomain {
                domain: Box::new(Harness::domain("source")),
            },
        )
        .await?;
    let built = source.store.build_snapshot().await?;
    let expected = source.store.inner.state();
    for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
        tokio::task::consume_budget().await;
        let mut target = Harness::new().await?;
        target
            .apply(
                1,
                ConsensusCommand::PutDomain {
                    domain: Box::new(Harness::domain("target")),
                },
            )
            .await?;
        let preceding = target.store.inner.state();
        let staged = target.receive(&built.snapshot).await?;
        target
            .store
            .inner
            .faults
            .fail_next("snapshot_manifest".into(), boundary);
        assert!(
            target
                .store
                .install_snapshot(&built.meta, staged)
                .await
                .is_err()
        );
        assert_eq!(target.store.inner.state(), preceding);
        let mut target = target.reopen().await?;
        match boundary {
            StorageBoundary::BeforeCommit => {
                assert_eq!(target.store.inner.state(), preceding);
                assert!(target.store.get_current_snapshot().await?.is_none());
            }
            StorageBoundary::AfterSync => {
                // The manifest reached storage, so the interrupted replacement is finished on the
                // next start and the node ends on the generation that was published.
                assert_eq!(target.store.inner.state(), expected);
                let current = target
                    .store
                    .get_current_snapshot()
                    .await?
                    .ok_or("snapshot missing after its durable installation")?;
                assert_eq!(current.meta, built.meta);
                assert_eq!(
                    Harness::read_sections(&current.snapshot).await?,
                    Harness::read_sections(&built.snapshot).await?
                );
            }
        }
    }
    // The reader holds the source database open; a reopen only succeeds once it is released.
    let built_meta = built.meta.clone();
    drop(built);
    let mut source = source.reopen().await?;
    assert_eq!(
        source
            .store
            .get_current_snapshot()
            .await?
            .ok_or("built snapshot missing after reopen")?
            .meta,
        built_meta
    );
    Ok(())
}

#[tokio::test]
async fn a_snapshot_is_sealed_as_bounded_sections() -> TestResult {
    let mut source = Harness::new().await?;
    for index in 1..=8 {
        source
            .apply(
                index,
                ConsensusCommand::PutDomain {
                    domain: Box::new(Harness::domain(&format!("domain_{index}"))),
                },
            )
            .await?;
    }
    let built = source.store.build_snapshot().await?;
    let manifest_sections = built.snapshot.manifest().section_count;
    let sections = Harness::read_sections(&built.snapshot).await?;
    assert_eq!(
        u32::try_from(sections.len())?,
        manifest_sections,
        "the manifest names exactly the sections the generation holds"
    );
    let limit = source.executor.limits().snapshot_section_bytes.as_u64();
    for section in &sections {
        assert!(
            u64::try_from(section.len())? <= limit,
            "no section exceeds the configured section limit"
        );
    }
    let mut records = 0_usize;
    for bytes in &sections {
        let section: SnapshotSection = crate::storage_decode(bytes)?;
        records = records
            .checked_add(section.records.len())
            .ok_or("the sealed sections hold more records than a usize counts")?;
    }
    assert!(
        records >= 8,
        "every stored record belongs to one section, got {records}"
    );
    Ok(())
}

#[tokio::test]
async fn a_superseded_generation_is_deleted_once_nothing_reads_it() -> TestResult {
    let mut source = Harness::new().await?;
    source
        .apply(
            1,
            ConsensusCommand::PutDomain {
                domain: Box::new(Harness::domain("first")),
            },
        )
        .await?;
    let first = source.store.build_snapshot().await?;
    let first_generation = first.snapshot.manifest().generation;
    source
        .apply(
            2,
            ConsensusCommand::PutDomain {
                domain: Box::new(Harness::domain("second")),
            },
        )
        .await?;
    let second = source.store.build_snapshot().await?;
    assert!(
        source.stored_generations()?.contains(&first_generation),
        "a generation an open reader holds stays in storage"
    );
    drop(first);
    source.store.build_snapshot().await?;
    assert!(
        !source.stored_generations()?.contains(&first_generation),
        "a generation nothing reads is deleted by the next publication"
    );
    drop(second);
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DiskKey {
    keyspace: String,
    key: Vec<u8>,
}

#[derive(Default)]
struct DiskState {
    volatile: BTreeMap<DiskKey, Vec<u8>>,
    durable: BTreeMap<DiskKey, Vec<u8>>,
    fail_at: Option<StorageBoundary>,
    written_records: usize,
    written_bytes: usize,
}

#[derive(Default)]
struct CrashDisk {
    state: parking_lot::Mutex<DiskState>,
}

impl CrashDisk {
    fn power_loss(&self) {
        let mut state = self.state.lock();
        // Explicitly discard everything the required full sync did not reach.
        state.volatile = state.durable.clone();
    }
}

impl CommitBackend for CrashDisk {
    fn commit(&self, mutations: Vec<Mutation>, mode: fjall::PersistMode) -> io::Result<()> {
        let mut state = self.state.lock();
        state.written_records = mutations.len();
        state.written_bytes = 0;
        for mutation in mutations {
            match mutation {
                Mutation::Put {
                    keyspace,
                    key,
                    value,
                } => {
                    state.written_bytes = state
                        .written_bytes
                        .checked_add(value.len())
                        .ok_or_else(|| io::Error::other("test byte counter exceeded usize"))?;
                    state.volatile.insert(
                        DiskKey {
                            keyspace: keyspace.name().to_string(),
                            key,
                        },
                        value,
                    );
                }
                Mutation::Delete { keyspace, key } => {
                    state.volatile.remove(&DiskKey {
                        keyspace: keyspace.name().to_string(),
                        key,
                    });
                }
            }
        }
        if state.fail_at == Some(StorageBoundary::BeforeCommit) {
            return Err(io::Error::other("power loss before sync"));
        }
        assert_eq!(
            mode,
            fjall::PersistMode::SyncAll,
            "every consensus batch requires data and metadata synchronization"
        );
        state.durable = state.volatile.clone();
        if state.fail_at == Some(StorageBoundary::AfterSync) {
            return Err(io::Error::other("crash before publication"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn power_loss_discards_unsynced_records_and_keeps_whole_synced_revisions() -> TestResult {
    let harness = Harness::new().await?;
    let reservation = StoreInner::reserve(&harness.executor, MemoryClass::Commands).await?;
    for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
        tokio::task::consume_budget().await;
        let disk = CrashDisk::default();
        let mut preceding = StateMachineData::default();
        let domain = Harness::domain("tenant");
        preceding.domains.insert(domain.id.clone(), domain.clone());
        preceding.schedule.domains.insert(
            domain.id.clone(),
            DomainSchedule::new(domain.id.clone(), [], vec![]),
        );
        preceding.last_applied_log_id = Some(Harness::log_id(1));
        let mut batch = DurableBatch::new(&reservation)?;
        preceding.write_changes(
            &StateMachineData::default(),
            &mut batch,
            &harness.store.inner.sm,
        )?;
        batch.commit(&disk)?;
        let preceding_image = disk.state.lock().durable.clone();
        let mut succeeding = preceding.clone();
        succeeding.schedule.domains.remove(&domain.id);
        succeeding
            .domains
            .get_mut(&domain.id)
            .ok_or("fixture domain missing")?
            .status = DomainStatus::Running;
        succeeding.last_applied_log_id = Some(Harness::log_id(2));
        succeeding.runtime_revision = 2;
        disk.state.lock().fail_at = Some(boundary);
        let mut batch = DurableBatch::new(&reservation)?;
        succeeding.write_changes(&preceding, &mut batch, &harness.store.inner.sm)?;
        assert!(batch.commit(&disk).is_err());
        disk.power_loss();
        let recovered = disk.state.lock().volatile.clone();
        match boundary {
            StorageBoundary::BeforeCommit => assert_eq!(recovered, preceding_image),
            StorageBoundary::AfterSync => {
                let expected_disk = CrashDisk::default();
                let mut batch = DurableBatch::new(&reservation)?;
                succeeding.write_changes(
                    &StateMachineData::default(),
                    &mut batch,
                    &harness.store.inner.sm,
                )?;
                batch.commit(&expected_disk)?;
                assert_eq!(recovered, expected_disk.state.lock().durable);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn replica_update_writes_one_record_and_metadata_amid_unrelated_graphs() -> TestResult {
    let harness = Harness::new().await?;
    let mut preceding = StateMachineData::default();
    for index in 0..10_000 {
        let domain = Harness::domain(&format!("tenant_{index}"));
        preceding.domains.insert(domain.id.clone(), domain.clone());
        preceding.schedule.domains.insert(
            domain.id.clone(),
            DomainSchedule::new(domain.id, [], vec![]),
        );
    }
    let mut graph_nodes = Vec::new();
    for index in 0..10_000 {
        graph_nodes.push(nervix_models::ScheduledNode::new(
            nervix_models::Model::Schema(nervix_models::CreateSchema {
                name: nervix_models::SchemaName::parse(&format!("schema_{index}"))?,
                fields: vec![nervix_models::SchemaField {
                    name: nervix_models::FieldName::parse("value")?,
                    ty: nervix_models::ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            }),
        ));
    }
    let graph_domain = DomainName::try_from("tenant_9999")?;
    preceding.schedule.domains.insert(
        graph_domain.clone(),
        DomainSchedule::new(graph_domain, graph_nodes, vec![]),
    );
    let mut succeeding = preceding.clone();
    let replica = ResourceNodeStatus {
        key: ResourceReplicaKey::new(
            DomainName::try_from("tenant_0")?,
            ResourceName::parse("artifact")?,
            1,
            Harness::node(),
        ),
        state: ResourceNodeState::Ready,
        root_checksum: Some("digest".into()),
        last_verified_at: None,
        source_node_id: None,
        error: None,
    };
    let applied = apply_consensus_command(
        &mut succeeding,
        &ConsensusCommand::PutResourceReplica {
            replica: Box::new(replica),
        },
    );
    assert_eq!(applied.response, ConsensusResponse::Applied);
    succeeding.last_applied_log_id = Some(Harness::log_id(20_000));
    succeeding.record_runtime_revision(20_000, &applied);
    let reservation = StoreInner::reserve(&harness.executor, MemoryClass::Commands).await?;
    let mut batch = DurableBatch::new(&reservation)?;
    succeeding.write_changes(&preceding, &mut batch, &harness.store.inner.sm)?;
    let disk = CrashDisk::default();
    batch.commit(&disk)?;
    let state = disk.state.lock();
    assert_eq!(state.written_records, 2);
    assert!(
        state.written_bytes < 1024,
        "one replica update encoded {} bytes",
        state.written_bytes
    );
    assert_eq!(succeeding.runtime_revision, preceding.runtime_revision);
    Ok(())
}

#[tokio::test]
async fn transaction_effect_progress_and_cleanup_recover_with_the_applied_position() -> TestResult {
    use nervix_models::{StartDomain, Statement, Timestamp, UserName};

    use crate::{
        ReplicatedTransaction, TransactionCommandResult, TransactionOutcome,
        TransactionQueueLimits, TransactionStatement, TransactionStepEffect, TransactionStepResult,
    };
    for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
        tokio::task::consume_budget().await;
        let mut harness = Harness::new().await?;
        let domain = Harness::domain("tenant");
        let owner = UserName::parse("operator")?;
        let at = Timestamp::from_unix_nanos(1);
        harness
            .apply(
                1,
                ConsensusCommand::PutDomain {
                    domain: Box::new(domain.clone()),
                },
            )
            .await?;
        harness
            .apply(
                2,
                ConsensusCommand::OpenTransaction {
                    transaction: Box::new(ReplicatedTransaction::open(
                        "transaction".into(),
                        domain.id.clone(),
                        owner.clone(),
                        at,
                    )),
                    max_open_transactions: 10,
                },
            )
            .await?;
        harness
            .apply(
                3,
                ConsensusCommand::QueueTransactionStatement {
                    id: "transaction".into(),
                    owner: owner.clone(),
                    domain: domain.id.clone(),
                    at,
                    statement: Box::new(TransactionStatement {
                        source: "START;".into(),
                        statement: Statement::StartDomain(StartDomain {
                            start: DomainStartPoint::Resume,
                        }),
                    }),
                    limits: TransactionQueueLimits {
                        max_statements: 10,
                        max_source_bytes: 1024,
                    },
                },
            )
            .await?;
        harness
            .apply(
                4,
                ConsensusCommand::StartTransactionCommit {
                    id: "transaction".into(),
                    owner,
                    at,
                },
            )
            .await?;
        let preceding = harness.store.inner.state();
        let command = ConsensusCommand::AdvanceTransactionCommit {
            id: "transaction".into(),
            expected_next_statement: 0,
            next_statement: 1,
            at,
            result: Box::new(TransactionStepResult {
                first_statement: 0,
                statement_count: 1,
                quiesce_level: None,
                planned_relocations: None,
                result: TransactionCommandResult {
                    success: true,
                    message: "started".into(),
                    diagnostics: vec![],
                    already_existed: false,
                },
            }),
            effect: Some(Box::new(TransactionStepEffect::StartDomain {
                domain_id: domain.id.clone(),
                expected_start_version: 0,
                start: DomainStartPoint::Resume,
                clock: None,
                authority: None,
            })),
            completion: Some(TransactionOutcome::Committed),
        };
        harness
            .store
            .inner
            .faults
            .fail_next(command.to_string(), boundary);
        assert!(harness.apply(5, command).await.is_err());
        assert_eq!(harness.store.inner.state(), preceding);
        let mut harness = harness.reopen().await?;
        match boundary {
            StorageBoundary::BeforeCommit => assert_eq!(harness.store.inner.state(), preceding),
            StorageBoundary::AfterSync => {
                let state = harness.store.inner.state();
                let transaction = state
                    .transactions
                    .get("transaction")
                    .ok_or("committed transaction missing")?;
                assert_eq!(transaction.completed_statement_count(), 1);
                assert_eq!(
                    transaction.finished_outcome(),
                    Some(&TransactionOutcome::Committed)
                );
                assert_eq!(
                    state
                        .domains
                        .get(&domain.id)
                        .ok_or("domain missing")?
                        .start_version,
                    1
                );
                assert_eq!(state.runtime_revision, 5);
                assert_eq!(state.last_applied_log_id, Some(Harness::log_id(5)));
                harness
                    .apply(
                        6,
                        ConsensusCommand::RemoveFinishedTransactions {
                            finished_before: at,
                        },
                    )
                    .await?;
                let harness = harness.reopen().await?;
                let state = harness.store.inner.state();
                assert_eq!(state.transactions.len(), 0);
                assert_eq!(state.runtime_revision, 5);
                assert_eq!(state.last_applied_log_id, Some(Harness::log_id(6)));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn membership_recovery_uses_the_same_atomic_application_metadata() -> TestResult {
    use std::collections::BTreeSet;
    for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
        tokio::task::consume_budget().await;
        let mut harness = Harness::new().await?;
        let membership = openraft::Membership::new(
            vec![BTreeSet::from([Harness::node()])],
            BTreeMap::from([(Harness::node(), crate::Node::new("https://node-1.invalid"))]),
        )?;
        let entry = EntryOf::<TypeConfig> {
            log_id: Harness::log_id(1),
            payload: EntryPayload::Membership(membership.clone()),
        };
        harness
            .store
            .inner
            .faults
            .fail_next("apply".into(), boundary);
        assert!(
            harness
                .store
                .apply(futures_util::stream::iter([Ok((entry, None))]))
                .await
                .is_err()
        );
        assert_eq!(harness.store.applied_state().await?.0, None);
        let mut harness = harness.reopen().await?;
        let (applied, stored_membership) = harness.store.applied_state().await?;
        match boundary {
            StorageBoundary::BeforeCommit => {
                assert_eq!(applied, None);
                assert_eq!(stored_membership, StoredMembershipOf::default());
            }
            StorageBoundary::AfterSync => {
                assert_eq!(applied, Some(Harness::log_id(1)));
                assert_eq!(
                    stored_membership,
                    StoredMembership::new(applied, membership)
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn current_record_storage_requires_its_metadata() -> TestResult {
    let mut harness = Harness::new().await?;
    harness
        .apply(
            1,
            ConsensusCommand::PutDomain {
                domain: Box::new(Harness::domain("tenant")),
            },
        )
        .await?;
    harness.store.inner.sm.remove(KEY_METADATA)?;
    harness
        .store
        .inner
        .db
        .persist(fjall::PersistMode::SyncAll)?;
    let Harness {
        directory,
        store,
        executor,
    } = harness;
    drop(store);
    let result =
        FjallStore::from_database(Database::builder(directory.path()).open()?, executor).await;
    assert!(matches!(result, Err(error) if error.to_string().contains("recreate")));
    Ok(())
}
