//! Recovery and publication checks through the Raft storage traits.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    time::Duration,
};

use nervix_execution::{ExecutionConfig, MemoryBudgets, OperationLimits};
use nervix_models::{
    AckMode, BranchKeyFingerprint, BranchSelection, ClusterNodeIdentity, ClusterNodeIncarnation,
    ClusterNodeName, CreateWasmProcessor, DomainConfig, DomainName, DomainPace, DomainSchedule,
    DomainStartPoint, DomainState, DomainStatus, GeneralErrorPolicy, Model, ProcessorInputs,
    ProcessorOutputs, RelayName, ResourceName, ResourceNodeState, ResourceNodeStatus,
    ResourceReplicaKey, ScheduledNode, SchemaFingerprint, WasmProcessorLimits, WasmProcessorName,
    WasmStateGeneration,
};
use openraft::{
    entry::RaftEntry as _, storage::RaftLogStorageExt as _, type_config::TypeConfigExt as _,
    vote::RaftLeaderId as _,
};
use tempfile::TempDir;
use ubyte::ByteUnit;

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
        Self::with_executor(Executor::default()).await
    }
    async fn with_executor(executor: Executor) -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
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
    /// Admit one command execution whose stored record carries `bytes` of password hash, so the
    /// write that stores it charges a known share of its batch.
    fn admission(index: u64, bytes: usize) -> Result<ConsensusCommand, Box<dyn std::error::Error>> {
        let owner = nervix_models::UserName::parse("operator")?;
        Ok(ConsensusCommand::AdmitCommandExecution {
            execution: Box::new(crate::CommandExecution::applying(
                nervix_models::CommandExecutionReference::parse(format!("request-{index}"))?,
                owner.clone(),
                None,
                [0; 32],
                nervix_models::Timestamp::from_unix_nanos(1),
                crate::CommandExecutionEffect::CreateUser {
                    if_not_exists: false,
                    name: owner,
                    password_hash: "x".repeat(bytes),
                },
            )),
            mutation_domains: BTreeSet::new(),
        })
    }
    fn admissions(
        end: u64,
        bytes: usize,
    ) -> Result<Vec<EntryOf<TypeConfig>>, Box<dyn std::error::Error>> {
        let mut entries = Vec::new();
        for index in 1..=end {
            entries.push(Self::entry(index, Self::admission(index, bytes)?));
        }
        Ok(entries)
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
async fn node_admission_fence_recovers_after_reopen() -> TestResult {
    let mut harness = Harness::new().await?;
    let identity = ClusterNodeIdentity::new(
        ClusterNodeName::parse("node-2")?,
        ClusterNodeIncarnation::new(7),
    );
    harness
        .apply(
            1,
            ConsensusCommand::FenceNodeAdmission {
                identity: identity.clone(),
            },
        )
        .await?;

    let harness = harness.reopen().await?;
    assert_eq!(
        harness
            .store
            .inner
            .state()
            .node_admission_fences
            .get(identity.node_id()),
        Some(&identity.incarnation())
    );
    Ok(())
}

/// A WASM processor's guest-state generations are committed control-plane state. The transitions a
/// domain mutation publishes, for one concrete branch and then for every branch, are exactly what the
/// store recovers after a restart, and a publication that does not hold the domain mutation lease
/// changes neither the stored generations nor anything else.
#[tokio::test]
async fn wasm_state_generation_transitions_survive_restart_and_require_the_mutation_lease()
-> TestResult {
    let mut harness = Harness::new().await?;
    let domain = Harness::domain("tenant");
    let processor = ScheduledNode::new(
        Model::WasmProcessor(CreateWasmProcessor {
            name: WasmProcessorName::parse("guest")?,
            from: ProcessorInputs::single(RelayName::parse("input")?),
            output_routes: ProcessorOutputs::single(RelayName::parse("output")?),
            branched_by: BranchSelection::unbranched(),
            resource: ResourceName::parse("guest_bundle")?,
            resource_version: 1,
            file: "processors/guest.wasm".to_string(),
            limits: WasmProcessorLimits {
                max_fuel: NonZeroU64::MIN,
                max_memory_bytes: NonZeroU64::MIN,
            },
            global_error_policy: GeneralErrorPolicy::Log,
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }),
        SchemaFingerprint::from_digest([1; 32]),
    );
    let created = DomainSchedule::new(domain.id.clone(), [processor.clone()], vec![]);
    let create_inputs = harness
        .store
        .inner
        .state()
        .domain_planning_inputs(&domain.id);
    harness
        .apply(
            1,
            ConsensusCommand::PutDomainAndSchedule {
                inputs: Box::new(create_inputs),
                domain: Box::new(domain.clone()),
                schedule: Some(Box::new(created)),
                mutation: None,
            },
        )
        .await?;
    let owner = nervix_models::UserName::parse("operator")?;
    let reference = nervix_models::CommandExecutionReference::parse("reset-request")?;
    harness
        .apply(
            2,
            ConsensusCommand::AdmitCommandExecution {
                execution: Box::new(crate::CommandExecution::applying(
                    reference.clone(),
                    owner.clone(),
                    Some(domain.id.clone()),
                    [7; 32],
                    nervix_models::Timestamp::from_unix_nanos(1),
                    crate::CommandExecutionEffect::CreateUser {
                        if_not_exists: false,
                        name: owner,
                        password_hash: "argon2-hash".to_string(),
                    },
                )),
                mutation_domains: BTreeSet::from([domain.id.clone()]),
            },
        )
        .await?;
    let lease = {
        let state = harness.store.inner.state();
        let Some(execution) = state.command_executions.get(&reference) else {
            return Err("the admitted command must be recorded".into());
        };
        let Some(lease) = execution.domain_mutation(&domain.id) else {
            return Err("the admitted command must own the domain mutation".into());
        };
        lease.clone()
    };

    let branch = BranchKeyFingerprint::new([3; 32]);
    let mut transitioned = processor;
    transitioned.begin_wasm_branch_state_generation(branch);
    let branch_schedule = DomainSchedule::new(domain.id.clone(), [transitioned.clone()], vec![]);
    transitioned.begin_wasm_state_generation();
    let every_branch_schedule =
        DomainSchedule::new(domain.id.clone(), [transitioned.clone()], vec![]);
    let branch_transition_inputs = harness
        .store
        .inner
        .state()
        .domain_planning_inputs(&domain.id);
    harness
        .apply(
            3,
            ConsensusCommand::ReplaceDomainSchedule {
                inputs: Box::new(branch_transition_inputs),
                schedule: Some(Box::new(branch_schedule.clone())),
                mutation: Some(Box::new(lease.clone())),
            },
        )
        .await?;
    let rejected_transition_inputs = harness
        .store
        .inner
        .state()
        .domain_planning_inputs(&domain.id);
    harness
        .apply(
            4,
            ConsensusCommand::ReplaceDomainSchedule {
                inputs: Box::new(rejected_transition_inputs),
                schedule: Some(Box::new(every_branch_schedule.clone())),
                mutation: None,
            },
        )
        .await?;
    assert_eq!(
        harness.store.inner.state().schedule.domain(&domain.id),
        Some(&branch_schedule),
        "a generation transition published without the domain mutation lease must not apply"
    );
    let every_branch_transition_inputs = harness
        .store
        .inner
        .state()
        .domain_planning_inputs(&domain.id);
    harness
        .apply(
            5,
            ConsensusCommand::ReplaceDomainSchedule {
                inputs: Box::new(every_branch_transition_inputs),
                schedule: Some(Box::new(every_branch_schedule.clone())),
                mutation: Some(Box::new(lease)),
            },
        )
        .await?;

    let harness = harness.reopen().await?;
    let recovered = harness.store.inner.state();
    let recovered_schedule = recovered
        .schedule
        .domain(&domain.id)
        .ok_or("the committed schedule must recover")?;
    assert_eq!(recovered_schedule, &every_branch_schedule);
    let Some(recovered_processor) = recovered_schedule.nodes.values().next() else {
        return Err("the recovered schedule must carry the WASM processor".into());
    };
    let Some(generations) = recovered_processor.wasm_state_generations() else {
        return Err("the recovered WASM processor must carry its generations".into());
    };
    let third = WasmStateGeneration::try_from(3)?;
    assert_eq!(generations.of_branch(None), third);
    assert_eq!(generations.of_branch(Some(&branch)), third);
    Ok(())
}

#[tokio::test]
async fn record_state_and_applied_position_recover_together_at_each_boundary() -> TestResult {
    for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
        tokio::task::consume_budget().await;
        let mut harness = Harness::new().await?;
        let domain = Harness::domain("tenant");
        let schedule = DomainSchedule::new(domain.id.clone(), [], vec![]);
        let inputs = Box::new(
            harness
                .store
                .inner
                .state()
                .domain_planning_inputs(&domain.id),
        );
        harness
            .apply(
                1,
                ConsensusCommand::PutDomainAndSchedule {
                    inputs,
                    domain: Box::new(domain.clone()),
                    schedule: Some(Box::new(schedule.clone())),
                    mutation: None,
                },
            )
            .await?;
        let preceding = harness.store.inner.state();
        let domain_watch = harness.store.inner.domain_tx.subscribe();
        let schedule_watch = harness.store.inner.schedule_tx.subscribe();
        let mut changed_domain = domain.clone();
        changed_domain.status = DomainStatus::Running;
        let inputs = Box::new(
            harness
                .store
                .inner
                .state()
                .domain_planning_inputs(&domain.id),
        );
        let command = ConsensusCommand::PutDomainAndSchedule {
            inputs,
            domain: Box::new(changed_domain.clone()),
            schedule: None,
            mutation: None,
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
                "append" => {
                    let (tx, rx) = TypeConfig::oneshot();
                    harness
                        .store
                        .append(
                            (1..=2).map(|index| {
                                EntryOf::<TypeConfig>::new_blank(Harness::log_id(index))
                            }),
                            IOFlushed::signal(tx),
                        )
                        .await?;
                    rx.await.map_err(io::Error::other)?
                }
                _ => harness.store.save_committed(Some(Harness::log_id(1))).await,
            };
            assert!(
                result.is_err(),
                "{operation} must report the failed boundary"
            );
            harness.store.wait_for_idle().await?;
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
                "append" => {
                    let entries = harness
                        .store
                        .get_log_reader()
                        .await
                        .try_get_log_entries(..)
                        .await?;
                    let indexes = entries
                        .iter()
                        .map(|entry| entry.log_id.index)
                        .collect::<Vec<_>>();
                    if expected {
                        assert_eq!(indexes, vec![1, 2]);
                    } else {
                        assert!(indexes.is_empty());
                    }
                }
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
                                    mutation: None,
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
async fn a_committed_range_is_published_once_after_its_single_durable_write() -> TestResult {
    let harness = Harness::new().await?;
    let mut entries = Vec::new();
    for (index, name) in [(1, "first"), (2, "second"), (3, "third")] {
        entries.push(Ok((
            Harness::entry(
                index,
                ConsensusCommand::PutDomain {
                    domain: Box::new(Harness::domain(name)),
                    mutation: None,
                },
            ),
            None,
        )));
    }
    // Writing entry by entry would already have published the first two entries by the time the
    // last entry's commit is held.
    let pause = harness
        .store
        .inner
        .faults
        .pause_next("put-domain:third".to_owned(), StorageBoundary::BeforeCommit);
    let notifications = harness.store.inner.domain_tx.subscribe();
    let mut store = harness.store.clone();
    let task = tokio::spawn(async move { store.apply(futures_util::stream::iter(entries)).await });
    tokio::time::timeout(Duration::from_secs(10), pause.entered()).await?;
    assert!(
        !task.is_finished(),
        "the range was acknowledged before its durable write"
    );
    assert_eq!(
        harness.store.inner.state().last_applied_log_id,
        None,
        "an entry was published before the write that stores the whole range"
    );
    assert!(!notifications.has_changed()?);
    pause.release();
    task.await??;
    let state = harness.store.inner.state();
    assert_eq!(state.last_applied_log_id, Some(Harness::log_id(3)));
    for name in ["first", "second", "third"] {
        assert!(
            state.domains.contains_key(&DomainName::try_from(name)?),
            "domain {name} is missing from the published range"
        );
    }
    assert!(notifications.has_changed()?);
    Ok(())
}

#[tokio::test]
async fn a_range_is_split_where_the_next_entry_would_exceed_its_write() -> TestResult {
    for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
        tokio::task::consume_budget().await;
        let mut harness = Harness::new().await?;
        let reservation = StoreInner::reserve(&harness.executor, MemoryClass::Commands).await?;
        let limit = DurableBatch::byte_limit(&reservation)?;
        drop(reservation);
        // Two small entries share a write, a large one does not fit beside both of them but fits
        // beside one, so the range is written as entries 1 and 2, then 3 and 4, then 5.
        let small = limit / 5;
        let large = (limit / 10)
            .checked_mul(7)
            .ok_or("the large fixture entry size overflows usize")?;
        let mut entries = Vec::new();
        for (index, bytes) in [(1, small), (2, small), (3, large), (4, small), (5, small)] {
            entries.push(Ok((
                Harness::entry(index, Harness::admission(index, bytes)?),
                None,
            )));
        }
        // Entry 4 is the second entry of its write, so its failure fails the whole write.
        harness
            .store
            .inner
            .faults
            .fail_next("admit-command-execution:request-4".to_owned(), boundary);
        assert!(
            harness
                .store
                .apply(futures_util::stream::iter(entries))
                .await
                .is_err()
        );
        assert_eq!(
            harness.store.inner.state().last_applied_log_id,
            Some(Harness::log_id(2)),
            "the write holding entries 1 and 2 is published before the failed write"
        );
        let harness = harness.reopen().await?;
        let recovered = harness.store.inner.state();
        let durable = match boundary {
            StorageBoundary::BeforeCommit => 2,
            StorageBoundary::AfterSync => 4,
        };
        assert_eq!(
            recovered.last_applied_log_id,
            Some(Harness::log_id(durable))
        );
        assert_eq!(u64::try_from(recovered.command_executions.len())?, durable);
    }
    Ok(())
}

#[tokio::test]
async fn entries_before_one_that_cannot_be_stored_are_written_first() -> TestResult {
    let mut harness = Harness::new().await?;
    let reservation = StoreInner::reserve(&harness.executor, MemoryClass::Commands).await?;
    let limit = DurableBatch::byte_limit(&reservation)?;
    drop(reservation);
    let entries = vec![
        Ok((Harness::entry(1, Harness::admission(1, limit / 5)?), None)),
        Ok((Harness::entry(2, Harness::admission(2, limit)?), None)),
    ];
    assert!(
        harness
            .store
            .apply(futures_util::stream::iter(entries))
            .await
            .is_err(),
        "an entry larger than a whole write must fail"
    );
    assert_eq!(
        harness.store.inner.state().last_applied_log_id,
        Some(Harness::log_id(1)),
        "the entry before the one that cannot be stored is published"
    );
    let harness = harness.reopen().await?;
    assert_eq!(
        harness.store.inner.state().last_applied_log_id,
        Some(Harness::log_id(1))
    );
    Ok(())
}

#[tokio::test]
async fn a_queued_vote_runs_after_a_ready_append_batch() -> TestResult {
    let mut harness = Harness::new().await?;
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
    let append_result = tokio::time::timeout(Duration::from_secs(10), append).await?;
    let append_result = append_result?;
    append_result?;
    vote_pause.release();
    vote.await??;
    let entries = harness
        .store
        .get_log_reader()
        .await
        .try_get_log_entries(..)
        .await?;
    let indexes = entries
        .iter()
        .map(|entry| entry.log_id.index)
        .collect::<Vec<_>>();
    assert_eq!(indexes, vec![1, 2]);
    Ok(())
}

#[tokio::test]
async fn complete_log_reads_return_every_entry_in_a_large_range() -> TestResult {
    const ENTRY_BYTES: usize = 768 * 1024;
    const ENTRY_COUNT: u64 = 3;

    let mut harness = Harness::new().await?;
    harness
        .store
        .blocking_append(Harness::admissions(ENTRY_COUNT, ENTRY_BYTES)?)
        .await
        .map_err(io::Error::other)?;
    assert!(
        harness.store.retained_log_bytes()
            > append_batch_target_bytes(&harness.executor)
                .checked_mul(2)
                .assured("twice the configured command limit fits in u64"),
        "the fixture must span several byte-bounded read chunks"
    );

    let entries = harness
        .store
        .get_log_reader()
        .await
        .try_get_log_entries(..)
        .await?;
    let indexes = entries
        .iter()
        .map(|entry| entry.log_id.index)
        .collect::<Vec<_>>();
    assert_eq!(indexes, vec![1, 2, 3]);
    Ok(())
}

#[tokio::test]
async fn log_entry_stream_reads_a_large_range_in_separate_chunks() -> TestResult {
    const ENTRY_BYTES: usize = 768 * 1024;
    const ENTRY_COUNT: u64 = 3;

    let mut harness = Harness::new().await?;
    harness
        .store
        .blocking_append(Harness::admissions(ENTRY_COUNT, ENTRY_BYTES)?)
        .await
        .map_err(io::Error::other)?;
    let before = harness.executor.snapshot().commands_memory;
    let mut reader = harness.store.get_log_reader().await;
    let entries = reader.entries_stream(..).await;
    tokio::pin!(entries);

    let first = entries
        .next()
        .await
        .verified("the stored range contains its first entry")?;
    assert_eq!(first.log_id.index, 1);
    assert!(
        entries.next().now_or_never().is_none(),
        "the next byte-bounded chunk must cross an asynchronous read boundary"
    );

    let mut indexes = vec![1];
    while let Some(entry) = entries.next().await {
        tokio::task::consume_budget().await;
        indexes.push(entry?.log_id.index);
    }
    assert_eq!(indexes, vec![1, 2, 3]);
    let after = harness.executor.snapshot().commands_memory;
    assert!(
        after
            .granted
            .checked_sub(before.granted)
            .verified("the memory grant counter only increases")
            >= ENTRY_COUNT,
        "each oversized pair must be read under a separate memory grant"
    );
    assert_eq!(after.reserved_bytes, before.reserved_bytes);
    Ok(())
}

#[tokio::test]
async fn log_entry_stream_yields_at_the_entry_count_bound() -> TestResult {
    let entry_count = MAX_APPEND_BATCH_ENTRIES
        .checked_add(1)
        .assured("one more than the fixed append entry bound fits in u64");
    let mut harness = Harness::new().await?;
    harness.append(entry_count).await?;
    assert!(
        harness.store.retained_log_bytes() < append_batch_target_bytes(&harness.executor),
        "the fixture must reach the entry bound before the byte bound"
    );

    let mut reader = harness.store.get_log_reader().await;
    let entries = reader.entries_stream(..).await;
    tokio::pin!(entries);
    for expected in 1..=MAX_APPEND_BATCH_ENTRIES {
        tokio::task::consume_budget().await;
        let entry = entries
            .next()
            .await
            .verified("the first count-bounded chunk contains every expected entry")?;
        assert_eq!(entry.log_id.index, expected);
    }
    assert!(
        entries.next().now_or_never().is_none(),
        "the next count-bounded chunk must cross an asynchronous read boundary"
    );
    let final_entry = entries
        .next()
        .await
        .verified("the second chunk contains the final stored entry")?;
    assert_eq!(final_entry.log_id.index, entry_count);
    assert!(entries.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn leader_bounded_log_stream_revalidates_the_vote_between_chunks() -> TestResult {
    const ENTRY_BYTES: usize = 768 * 1024;

    let mut harness = Harness::new().await?;
    harness
        .store
        .blocking_append(Harness::admissions(2, ENTRY_BYTES)?)
        .await
        .map_err(io::Error::other)?;
    let vote = VoteOf::new_committed(3, Harness::node());
    harness.store.save_vote(&vote).await?;

    let mut vote_store = harness.store.clone();
    let mut reader = harness.store.get_log_reader().await;
    let entries = reader
        .leader_bounded_stream(vote.leader_id().clone(), ..)
        .await;
    tokio::pin!(entries);
    let first = entries
        .next()
        .await
        .verified("the stored range contains its first entry")?;
    assert_eq!(first.log_id.index, 1);

    vote_store
        .save_vote(&VoteOf::new(4, Harness::node()))
        .await?;
    let changed = entries
        .next()
        .await
        .verified("the leader change is reported as a stream item");
    assert!(matches!(
        changed,
        Err(LeaderBoundedStreamError::LeaderChanged(_))
    ));
    assert!(entries.next().await.is_none());
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
    let mut harness = harness.reopen().await?;
    assert_eq!(
        harness.store.get_log_state().await?.last_log_id,
        Some(Harness::log_id(1))
    );
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
async fn retained_log_bytes_follow_purge_truncation_and_reopen_without_flushing() -> TestResult {
    let mut harness = Harness::new().await?;
    harness.append(6).await?;
    let appended_bytes = harness.store.retained_log_bytes();
    assert!(appended_bytes > 0, "the appended log must occupy bytes");

    harness.store.purge(Harness::log_id(2)).await?;
    let purged_bytes = harness.store.retained_log_bytes();
    assert!(
        purged_bytes < appended_bytes,
        "purging entries must reduce retained bytes before a memtable flush"
    );

    harness
        .store
        .truncate_after(Some(Harness::log_id(4)))
        .await?;
    let truncated_bytes = harness.store.retained_log_bytes();
    assert!(
        truncated_bytes < purged_bytes,
        "truncating entries must reduce retained bytes before a memtable flush"
    );

    let harness = harness.reopen().await?;
    assert_eq!(harness.store.retained_log_bytes(), truncated_bytes);
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
                mutation: None,
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
                    mutation: None,
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
async fn an_interrupted_installation_finishes_on_the_next_start() -> TestResult {
    let mut source = Harness::new().await?;
    source
        .apply(
            1,
            ConsensusCommand::PutDomain {
                domain: Box::new(Harness::domain("source")),
                mutation: None,
            },
        )
        .await?;
    let built = source.store.build_snapshot().await?;
    let expected = source.store.inner.state();
    for operation in ["snapshot_clear", "snapshot_records", "snapshot_installed"] {
        for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
            tokio::task::consume_budget().await;
            let mut target = Harness::new().await?;
            target
                .apply(
                    1,
                    ConsensusCommand::PutDomain {
                        domain: Box::new(Harness::domain("target")),
                        mutation: None,
                    },
                )
                .await?;
            let staged = target.receive(&built.snapshot).await?;
            target
                .store
                .inner
                .faults
                .fail_next(operation.to_owned(), boundary);
            assert!(
                target
                    .store
                    .install_snapshot(&built.meta, staged)
                    .await
                    .is_err(),
                "the injected {operation} failure must stop the installation"
            );
            let mut target = target.reopen().await?;
            assert_eq!(
                target.store.inner.state(),
                expected,
                "a node interrupted at {operation} starts on the generation it published"
            );
            let current = target
                .store
                .get_current_snapshot()
                .await?
                .ok_or("the published generation is missing after the interrupted install")?;
            assert_eq!(current.meta, built.meta);
            assert_eq!(
                Harness::read_sections(&current.snapshot).await?,
                Harness::read_sections(&built.snapshot).await?
            );
        }
    }
    drop(built);
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
                    mutation: None,
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
async fn a_generation_larger_than_bulk_memory_seals_one_budgeted_section_at_a_time() -> TestResult {
    const RECORD_BYTES: usize = 40 * 1024;
    const RECORDS: u64 = 12;

    let limits = OperationLimits {
        snapshot_section_bytes: ByteUnit::Kibibyte(64),
        ..OperationLimits::default()
    };
    let executor = Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            bulk: ByteUnit::Kibibyte(320),
            ..MemoryBudgets::default()
        },
        limits,
        ..ExecutionConfig::default()
    })?;
    let section_working_bytes = limits
        .snapshot_section_working_bytes()
        .ok_or("the fixture's snapshot working set must fit in u64")?;
    let bulk_capacity = executor.snapshot().bulk_memory.capacity_bytes;
    let mut source = Harness::with_executor(executor.clone()).await?;
    for index in 1..=RECORDS {
        source
            .apply(index, Harness::admission(index, RECORD_BYTES)?)
            .await?;
    }
    source.append(RECORDS).await?;
    let log_bytes_at_open = source.store.log_bytes_since_snapshot();

    let pause = source
        .store
        .inner
        .faults
        .pause_next("snapshot_section".to_owned(), StorageBoundary::AfterSync);
    let mut builder = source.store.clone();
    let build = tokio::spawn(async move { builder.build_snapshot().await });
    tokio::time::timeout(Duration::from_secs(10), pause.entered()).await?;
    assert_eq!(
        executor.snapshot().bulk_memory.reserved_bytes,
        section_working_bytes,
        "one section write must hold exactly its configured working set"
    );

    let following_index = RECORDS
        .checked_add(1)
        .ok_or("the fixture's record count must leave room for one more revision")?;
    let following = Harness::entry(
        following_index,
        Harness::admission(following_index, RECORD_BYTES)?,
    );
    let mut log_store = source.store.clone();
    let append = tokio::spawn(async move {
        log_store
            .blocking_append([EntryOf::<TypeConfig>::new_blank(Harness::log_id(
                following_index,
            ))])
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while executor.snapshot().consensus_storage.pending < 1 {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let mut live_store = source.store.clone();
    let apply = tokio::spawn(async move {
        live_store
            .apply(futures_util::stream::iter([Ok((following, None))]))
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while executor.snapshot().consensus_storage.pending < 2 {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let next_pause = source
        .store
        .inner
        .faults
        .pause_next("snapshot_section".to_owned(), StorageBoundary::AfterSync);
    pause.release();
    tokio::time::timeout(Duration::from_secs(10), next_pause.entered()).await?;
    append.await??;
    apply.await??;
    let concurrent_log_bytes = source
        .store
        .log_bytes_since_snapshot()
        .checked_sub(log_bytes_at_open)
        .ok_or("the appended-byte counter must include its value at snapshot open")?;
    assert_eq!(
        source.store.inner.state().last_applied_log_id,
        Some(Harness::log_id(following_index)),
        "a state-machine write queued between sections must be allowed to proceed"
    );
    assert!(
        !build.is_finished(),
        "the generation must still be writing its later bounded sections"
    );
    next_pause.release();

    let built = build.await??;
    assert_eq!(
        built.meta.last_log_id,
        Some(Harness::log_id(RECORDS)),
        "every section must stay pinned to the view opened before the interleaved write"
    );
    assert!(
        built.snapshot.manifest().section_count > 1,
        "the fixture must span several independently written sections"
    );
    assert!(
        built.snapshot.manifest().total_bytes > bulk_capacity,
        "the complete generation must exceed the memory available to one node"
    );
    assert_eq!(
        source.store.log_bytes_since_snapshot(),
        concurrent_log_bytes,
        "entries appended while the generation seals must count toward the next snapshot"
    );
    assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
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
                mutation: None,
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
                mutation: None,
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
            SchemaFingerprint::from_digest([1; 32]),
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
            ClusterNodeIdentity::new(Harness::node(), ClusterNodeIncarnation::new(1)),
        ),
        state: ResourceNodeState::Ready,
        root_checksum: Some("digest".into()),
        last_verified_at: None,
        source_node: None,
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
async fn transaction_append_and_preview_recover_as_one_revision() -> TestResult {
    use nervix_models::{StartDomain, Statement, Timestamp, UserName};

    use crate::{
        ReplicatedTransaction, TransactionActivity, TransactionQueueLimits, TransactionStatement,
        TransactionStatementRequest,
    };

    for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
        tokio::task::consume_budget().await;
        let mut harness = Harness::new().await?;
        let domain = Harness::domain("tenant");
        let owner = UserName::parse("operator")?;
        let at = Timestamp::from_unix_nanos(1);
        let activity = TransactionActivity::from_timeout(at, Duration::from_secs(60));
        harness
            .apply(
                1,
                ConsensusCommand::PutDomain {
                    domain: Box::new(domain.clone()),
                    mutation: None,
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
                        activity,
                    )),
                    max_open_transactions: 10,
                },
            )
            .await?;
        let report = crate::transaction_report::test_report_archive("transaction", &domain.id, 1);
        let preview = report.identity().clone();
        let command = ConsensusCommand::QueueTransactionStatement {
            id: "transaction".into(),
            owner,
            domain: domain.id.clone(),
            activity,
            statement: Box::new(TransactionStatement::test_admitted(
                TransactionStatementRequest {
                    request_reference: nervix_models::CommandExecutionReference::parse(
                        "request-0",
                    )?,
                    expected_position: 0,
                    source: "START;".into(),
                    statement: Statement::StartDomain(StartDomain {
                        start: DomainStartPoint::Resume,
                    }),
                },
            )),
            report: Box::new(report),
            limits: TransactionQueueLimits {
                max_statements: 10,
                max_source_bytes: 1024,
            },
        };
        let preceding = harness.store.inner.state();
        harness
            .store
            .inner
            .faults
            .fail_next(command.to_string(), boundary);

        assert!(harness.apply(3, command).await.is_err());
        assert_eq!(harness.store.inner.state(), preceding);

        let harness = harness.reopen().await?;
        let recovered = harness.store.inner.state();
        let transaction = recovered
            .transactions
            .get("transaction")
            .ok_or("transaction missing after reopen")?;
        match boundary {
            StorageBoundary::BeforeCommit => {
                assert!(transaction.statements.is_empty());
                assert!(transaction.latest_preview().is_none());
                assert!(recovered.transaction_reports.report(&preview).is_err());
            }
            StorageBoundary::AfterSync => {
                assert_eq!(transaction.statements.len(), 1);
                assert_eq!(transaction.latest_preview(), Some(&preview));
                let retained = recovered.transaction_reports.report(&preview)?;
                assert_eq!(retained.position(), preview.position);
                assert_eq!(retained.planning_basis(), preview.planning_basis);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn transaction_report_and_frozen_plan_survive_snapshot_installation() -> TestResult {
    use nervix_models::{StartDomain, Statement, Timestamp, UserName};

    use crate::{
        ReplicatedTransaction, TransactionActivity, TransactionQueueLimits, TransactionState,
        TransactionStatement, TransactionStatementRequest,
    };

    let mut source = Harness::new().await?;
    let domain = Harness::domain("tenant");
    let owner = UserName::parse("operator")?;
    let at = Timestamp::from_unix_nanos(1);
    let activity = TransactionActivity::from_timeout(at, Duration::from_secs(60));
    source
        .apply(
            1,
            ConsensusCommand::PutDomain {
                domain: Box::new(domain.clone()),
                mutation: None,
            },
        )
        .await?;
    source
        .apply(
            2,
            ConsensusCommand::OpenTransaction {
                transaction: Box::new(ReplicatedTransaction::open(
                    "transaction".into(),
                    domain.id.clone(),
                    owner.clone(),
                    activity,
                )),
                max_open_transactions: 10,
            },
        )
        .await?;
    source
        .apply(
            3,
            ConsensusCommand::QueueTransactionStatement {
                id: "transaction".into(),
                owner: owner.clone(),
                domain: domain.id.clone(),
                activity,
                statement: Box::new(TransactionStatement::test_admitted(
                    TransactionStatementRequest {
                        request_reference: nervix_models::CommandExecutionReference::parse(
                            "request-0",
                        )?,
                        expected_position: 0,
                        source: "START;".into(),
                        statement: Statement::StartDomain(StartDomain {
                            start: DomainStartPoint::Resume,
                        }),
                    },
                )),
                report: Box::new(crate::transaction_report::test_report_archive(
                    "transaction",
                    &domain.id,
                    1,
                )),
                limits: TransactionQueueLimits {
                    max_statements: 10,
                    max_source_bytes: 1024,
                },
            },
        )
        .await?;
    let plan = crate::transaction::test_commit_plan("transaction", 1);
    let preview = plan.preview.clone();
    let expected_step = plan.steps[0].clone();
    let plan =
        crate::transaction_plan::test_admission_plan(&source.store.inner.state(), &domain.id, plan);
    source
        .apply(
            4,
            ConsensusCommand::StartTransactionCommit {
                id: "transaction".into(),
                owner,
                activity,
                expected_preview: preview.clone(),
                report: Box::new(crate::transaction_report::test_report_archive(
                    "transaction",
                    &domain.id,
                    1,
                )),
                plan: Box::new(plan),
            },
        )
        .await?;
    let expected_report = source
        .store
        .inner
        .state()
        .transaction_reports
        .report(&preview)?;

    let snapshot = source.store.build_snapshot().await?;
    let mut target = Harness::new().await?;
    let staged = target.receive(&snapshot.snapshot).await?;
    target
        .store
        .install_snapshot(&snapshot.meta, staged)
        .await?;
    let restored = target.store.inner.state();
    assert!(matches!(
        restored
            .transactions
            .get("transaction")
            .ok_or("transaction missing from installed snapshot")?
            .state,
        TransactionState::Committing(_)
    ));
    assert_eq!(
        restored.transaction_reports.report(&preview)?,
        expected_report
    );
    assert_eq!(
        restored
            .transaction_commit_plans
            .step("transaction", 0)?
            .decision,
        expected_step
    );
    drop(snapshot);
    Ok(())
}

#[tokio::test]
async fn transaction_effect_progress_and_cleanup_recover_with_the_applied_position() -> TestResult {
    use nervix_models::{StartDomain, Statement, Timestamp, UserName};

    use crate::{
        ReplicatedTransaction, TransactionActivity, TransactionCommandResult, TransactionOutcome,
        TransactionQueueLimits, TransactionState, TransactionStatement, TransactionStepEffect,
        TransactionStepResult,
    };
    for boundary in [StorageBoundary::BeforeCommit, StorageBoundary::AfterSync] {
        tokio::task::consume_budget().await;
        let mut harness = Harness::new().await?;
        let domain = Harness::domain("tenant");
        let owner = UserName::parse("operator")?;
        let at = Timestamp::from_unix_nanos(1);
        let activity = TransactionActivity::from_timeout(at, Duration::from_secs(60));
        harness
            .apply(
                1,
                ConsensusCommand::PutDomain {
                    domain: Box::new(domain.clone()),
                    mutation: None,
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
                        activity,
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
                    activity,
                    statement: Box::new(TransactionStatement::test_admitted(
                        crate::TransactionStatementRequest {
                            request_reference: nervix_models::CommandExecutionReference::parse(
                                "request-0",
                            )?,
                            expected_position: 0,
                            source: "START;".into(),
                            statement: Statement::StartDomain(StartDomain {
                                start: DomainStartPoint::Resume,
                            }),
                        },
                    )),
                    report: Box::new(crate::transaction_report::test_report_archive(
                        "transaction",
                        &domain.id,
                        1,
                    )),
                    limits: TransactionQueueLimits {
                        max_statements: 10,
                        max_source_bytes: 1024,
                    },
                },
            )
            .await?;
        let mut commit_plan = crate::transaction::test_commit_plan("transaction", 1);
        commit_plan.steps[0].kind = nervix_models::TransactionCommitStepKind::StartDomain {
            resolved: nervix_models::TransactionResolvedDomainStart {
                start: DomainStartPoint::Resume,
                clock: None,
                authority: None,
            },
        };
        let preview = commit_plan.preview.clone();
        let planned_step = commit_plan.steps[0].clone();
        let commit_plan = crate::transaction_plan::test_admission_plan(
            &harness.store.inner.state(),
            &domain.id,
            commit_plan,
        );
        harness
            .apply(
                4,
                ConsensusCommand::StartTransactionCommit {
                    id: "transaction".into(),
                    owner,
                    activity,
                    expected_preview: commit_plan.decision().preview.clone(),
                    report: Box::new(crate::transaction_report::test_report_archive(
                        "transaction",
                        &domain.id,
                        1,
                    )),
                    plan: Box::new(commit_plan),
                },
            )
            .await?;
        let preceding = harness.store.inner.state();
        let admitted_report = preceding.transaction_reports.report(&preview)?;
        assert_eq!(admitted_report.execution_steps().len(), 1);
        assert_eq!(
            preceding
                .transaction_commit_plans
                .step("transaction", 0)?
                .decision,
            planned_step
        );
        let mutation = preceding
            .transactions
            .get("transaction")
            .and_then(ReplicatedTransaction::domain_mutation)
            .cloned()
            .ok_or("committing transaction mutation lease missing")?;
        assert_eq!(mutation.recovery_fence().revision(), 4);
        assert_eq!(preceding.domain_mutations.get(&domain.id), Some(&mutation));
        let inputs = Box::new(
            harness
                .store
                .inner
                .state()
                .domain_planning_inputs(&domain.id),
        );
        let mut applying_impact = planned_step.impact.clone();
        *applying_impact.actual_mut() = nervix_models::ActualExecutionStepImpact::applying();
        let command = ConsensusCommand::AdvanceTransactionCommit {
            id: "transaction".into(),
            expected_next_statement: 0,
            next_statement: 1,
            at,
            result: Box::new(TransactionStepResult {
                impact: applying_impact,
                result: TransactionCommandResult {
                    success: true,
                    message: "started".into(),
                    diagnostics: vec![],
                    already_existed: false,
                    admission: None,
                },
            }),
            effect: Some(Box::new(TransactionStepEffect::StartDomain {
                inputs,
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
        let recovered = harness.store.inner.state();
        let recovered_report = recovered.transaction_reports.report(&preview)?;
        assert_eq!(recovered_report.domain(), admitted_report.domain());
        assert_eq!(recovered_report.position(), admitted_report.position());
        assert_eq!(recovered_report.operations(), admitted_report.operations());
        let expected_planned = match boundary {
            StorageBoundary::BeforeCommit => admitted_report.execution_steps()[0].planned(),
            StorageBoundary::AfterSync => planned_step.impact.planned(),
        };
        assert_eq!(
            recovered_report.execution_steps()[0].planned(),
            expected_planned
        );
        let expected_outcome = match boundary {
            StorageBoundary::BeforeCommit => nervix_models::ExecutionStepOutcome::Unattempted,
            StorageBoundary::AfterSync => nervix_models::ExecutionStepOutcome::Applying,
        };
        assert_eq!(
            recovered_report.execution_steps()[0].actual().outcome,
            expected_outcome
        );
        assert_eq!(
            recovered
                .transaction_commit_plans
                .step("transaction", 0)?
                .decision,
            planned_step
        );
        assert_eq!(
            recovered.domain_mutations.get(&domain.id),
            Some(&mutation),
            "reopening retains the committing transaction's domain mutation fence"
        );
        match boundary {
            StorageBoundary::BeforeCommit => assert_eq!(harness.store.inner.state(), preceding),
            StorageBoundary::AfterSync => {
                let state = harness.store.inner.state();
                let transaction = state
                    .transactions
                    .get("transaction")
                    .ok_or("committed transaction missing")?;
                assert_eq!(transaction.completed_statement_count(), 0);
                assert!(transaction.finished_outcome().is_none());
                let TransactionState::Committing(progress) = &transaction.state else {
                    return Err("transaction should be applying its committed effect".into());
                };
                assert_eq!(
                    progress
                        .applying
                        .as_ref()
                        .ok_or("applying transaction step missing")?
                        .effect_revision,
                    5
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
                        ConsensusCommand::CompleteTransactionApplication {
                            id: "transaction".into(),
                            expected_next_statement: 0,
                            at,
                            application_failure: None,
                        },
                    )
                    .await?;
                let completed = harness.store.inner.state();
                let transaction = completed
                    .transactions
                    .get("transaction")
                    .ok_or("completed transaction missing")?;
                assert_eq!(transaction.completed_statement_count(), 1);
                assert_eq!(
                    transaction.finished_outcome(),
                    Some(&TransactionOutcome::Committed)
                );
                let completed_report = completed.transaction_reports.report(&preview)?;
                assert!(matches!(
                    completed_report.execution_steps()[0].actual().outcome,
                    nervix_models::ExecutionStepOutcome::Applied
                ));
                assert!(
                    !completed.domain_mutations.contains_key(&domain.id),
                    "the terminal explicit transaction releases its domain mutation"
                );
                harness
                    .apply(
                        7,
                        ConsensusCommand::RemoveFinishedTransactions {
                            finished_before: at,
                        },
                    )
                    .await?;
                let harness = harness.reopen().await?;
                let state = harness.store.inner.state();
                assert_eq!(state.transactions.len(), 0);
                assert!(state.transaction_reports.report(&preview).is_err());
                assert!(
                    state
                        .transaction_commit_plans
                        .step("transaction", 0)
                        .is_err()
                );
                assert_eq!(state.runtime_revision, 5);
                assert_eq!(state.last_applied_log_id, Some(Harness::log_id(7)));
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
                mutation: None,
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
