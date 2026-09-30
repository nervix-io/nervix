//! A restore's concurrency owners under every interleaving Shuttle explores.
//!
//! - Retries of one restore that race each other and the leader's reconciliation, each under the
//!   execution reference's owner, join one execution: every step takes effect once, and every
//!   retry reads the one outcome.
//! - A new leader that resumes a restore from a stale view of its progress, while the old leader's
//!   proposals still commit until leadership moves, never repeats a recorded step's effect.
//! - An upload a client abandons midway releases the staging quota it reserved exactly once, and
//!   the archive a retry stages keeps its quota while a restore reads it, however its retention is
//!   swept, and then releases it exactly once.
//!
//! Consensus is modelled as one lock over the state a replicated command changes: applying a
//! command is one step under that lock, as the state machine applies one entry at a time.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

use arch_into::ArchInto as _;
use bytes::Bytes;
use futures_util::{StreamExt as _, stream};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ArchiveDigest, CommandExecutionReference, DomainName, RestoreArchive, RestoreStep, Timestamp,
    UserName,
};
use nervix_primitives::{
    sync::{StdArc, atomic::Ordering, blocking::Mutex},
    thread,
};
use shuttle::future::block_on;

use super::{
    archives::{
        RestoreArchives, RestoreStreamPart, StagingOutcome, stage_restore_archive,
        test_staging::{MemoryStaging, Quota, StagedMemory},
    },
    runner::{RestoreRunEnd, RestoreSteps, StepFailure, run_restore_steps},
};
use crate::{
    application::{command_execution::CommandExecutionOwners, model_mutation::command_error},
    shuttle_test::check_interleavings,
};

const MODEL_THREAD_JOINS: &str =
    "Shuttle fails the whole execution when a model thread panics, so no join observes one";

/// The outcome a finished model restore records.
const RESTORED: &str = "restored";

fn reference() -> CommandExecutionReference {
    CommandExecutionReference::parse("restore-under-shuttle")
        .assured("the model reference is an identifier-shaped literal")
}

fn owner() -> UserName {
    UserName::parse("alice").assured("the model user is a valid literal name")
}

fn steps() -> Vec<RestoreStep> {
    let domain = DomainName::parse("prod").assured("the model domain is a valid literal name");
    vec![
        RestoreStep::Users,
        RestoreStep::CreateDomain(domain.clone()),
        RestoreStep::ImportResources(domain.clone()),
        RestoreStep::ApplyModels(domain),
    ]
}

/// The replicated record of one restore: which node leads, the steps recorded, how often each
/// step took effect, how many executions ran steps, and the outcome once the restore finished.
#[derive(Default)]
struct Ledger {
    /// Only the leader's proposals commit.
    leader: usize,
    recorded: BTreeSet<RestoreStep>,
    effects: BTreeMap<RestoreStep, usize>,
    executions: usize,
    finished: Option<&'static str>,
}

impl Ledger {
    /// Applies `step` as `node` proposed it, as the state machine applies a restore step: nothing
    /// commits unless `node` leads, a step already recorded takes no effect, and a step whose
    /// prerequisite is not recorded is refused.
    fn apply(&mut self, node: usize, step: &RestoreStep) -> Result<(), StepFailure> {
        if self.leader != node {
            let redirect = command_error("leadership moved".to_string());
            return Err(StepFailure::LeadershipLost(Box::new(redirect)));
        }
        if self.recorded.contains(step) {
            return Ok(());
        }
        let prerequisite = match step {
            RestoreStep::Users => None,
            RestoreStep::CreateDomain(_) => Some(RestoreStep::Users),
            RestoreStep::ImportResources(domain) => Some(RestoreStep::CreateDomain(domain.clone())),
            RestoreStep::ApplyModels(domain) => Some(RestoreStep::ImportResources(domain.clone())),
        };
        if let Some(prerequisite) = prerequisite
            && !self.recorded.contains(&prerequisite)
        {
            return Err(StepFailure::Failed(format!(
                "the step to {step} came before the step to {prerequisite}"
            )));
        }
        self.recorded.insert(step.clone());
        *self.effects.entry(step.clone()).or_default() += 1;
        Ok(())
    }

    fn assert_each_step_took_effect_once(&self) {
        for step in steps() {
            assert_eq!(
                self.effects.get(&step),
                Some(&1),
                "the step to {step} took effect {:?} times",
                self.effects.get(&step)
            );
        }
    }
}

/// A restore's steps as one node proposes them.
struct NodeSteps {
    ledger: StdArc<Mutex<Ledger>>,
    node: usize,
}

impl RestoreSteps for NodeSteps {
    async fn apply(&self, step: &RestoreStep) -> Result<(), StepFailure> {
        // A proposal reaches the log only after the node prepared the step's effect.
        thread::yield_now();
        self.ledger.lock().apply(self.node, step)
    }
}

/// One execution of the restore on `node`: its recorded outcome once it finished, and otherwise
/// the steps not recorded when it began, followed by the outcome the first execution to finish
/// records. Lost leadership leaves no outcome.
async fn execute(ledger: &StdArc<Mutex<Ledger>>, node: usize) -> Option<&'static str> {
    let recorded = {
        let mut ledger = ledger.lock();
        if let Some(outcome) = ledger.finished {
            return Some(outcome);
        }
        ledger.executions += 1;
        ledger.recorded.clone()
    };
    let effects = NodeSteps {
        ledger: ledger.clone(),
        node,
    };
    let run = run_restore_steps(&steps(), &recorded, &effects).await;
    match run.end {
        RestoreRunEnd::Completed => {
            let mut ledger = ledger.lock();
            let outcome = ledger.finished.get_or_insert(RESTORED);
            Some(*outcome)
        }
        RestoreRunEnd::LeadershipLost(_) => None,
        RestoreRunEnd::Failed { step, reason } => {
            panic!("no step of the model fails, yet {step} did: {reason}")
        }
    }
}

/// Two retries of one restore and the leader's reconciliation race. Each retry waits for the
/// reference's owner, as a restore stream does; reconciliation takes only an owner no retry
/// holds.
fn concurrent_retries_join_one_execution() {
    let owners = StdArc::new(CommandExecutionOwners::default());
    let ledger = StdArc::new(Mutex::new(Ledger::default()));
    let mut retries = Vec::new();
    for _ in 0..2 {
        let owners = owners.clone();
        let ledger = ledger.clone();
        retries.push(thread::spawn(move || {
            block_on(async move {
                let _owner = owners.lock(reference()).await;
                execute(&ledger, 0).await
            })
        }));
    }
    let reconciliation = {
        let owners = owners.clone();
        let ledger = ledger.clone();
        thread::spawn(move || {
            let _owner = owners.try_lock(reference())?;
            block_on(execute(&ledger, 0))
        })
    };
    for retry in retries {
        let outcome = retry.join().assured(MODEL_THREAD_JOINS);
        assert_eq!(outcome, Some(RESTORED), "every retry reads the one outcome");
    }
    let reconciled = reconciliation.join().assured(MODEL_THREAD_JOINS);
    assert!(
        matches!(reconciled, None | Some(RESTORED)),
        "reconciliation reads the one outcome when it ran: {reconciled:?}"
    );
    let ledger = ledger.lock();
    ledger.assert_each_step_took_effect_once();
    assert_eq!(ledger.finished, Some(RESTORED));
    assert_eq!(
        ledger.executions, 1,
        "the retries and reconciliation joined one execution"
    );
}

/// The old leader applies steps until leadership moves. The new leader reads the restore's
/// progress before its election completes, so the view it resumes from can miss steps the old
/// leader recorded meanwhile; recording those again takes no effect.
fn a_resumed_restore_never_repeats_a_recorded_effect() {
    let ledger = StdArc::new(Mutex::new(Ledger::default()));
    let old_leader = {
        let ledger = ledger.clone();
        thread::spawn(move || block_on(execute(&ledger, 0)))
    };
    let election = {
        let ledger = ledger.clone();
        thread::spawn(move || {
            thread::yield_now();
            ledger.lock().leader = 1;
        })
    };
    let new_leader = {
        let ledger = ledger.clone();
        thread::spawn(move || {
            let recorded = ledger.lock().recorded.clone();
            election.join().assured(MODEL_THREAD_JOINS);
            let effects = NodeSteps {
                ledger: ledger.clone(),
                node: 1,
            };
            let run = block_on(run_restore_steps(&steps(), &recorded, &effects));
            assert!(
                matches!(run.end, RestoreRunEnd::Completed),
                "the new leader completes the restore"
            );
        })
    };
    let old_outcome = old_leader.join().assured(MODEL_THREAD_JOINS);
    assert!(
        matches!(old_outcome, None | Some(RESTORED)),
        "the old leader either completes the restore or loses leadership: {old_outcome:?}"
    );
    new_leader.join().assured(MODEL_THREAD_JOINS);
    let ledger = ledger.lock();
    ledger.assert_each_step_took_effect_once();
}

const CHUNKS: [&[u8]; 3] = [b"manifest", b"sections", b"resource"];

/// The archive every model upload declares: all of `CHUNKS`.
fn declared() -> RestoreArchive {
    let bytes = CHUNKS.concat();
    let length: u64 = bytes.len().arch_into();
    RestoreArchive {
        total_bytes: NonZeroU64::new(length).assured("the model archive has chunks"),
        digest: ArchiveDigest::from_bytes(*blake3::hash(&bytes).as_bytes()),
    }
}

/// The instant the model archive's retention ends.
fn retained_until() -> Timestamp {
    Timestamp::from_unix_nanos(1)
}

/// An upload that sends its first chunk and then stays silent until it is dropped.
async fn abandoned_upload(quota: StdArc<Quota>) {
    let staging = MemoryStaging::new(quota);
    let first = RestoreStreamPart::Chunk(Bytes::from_static(CHUNKS[0]));
    let parts = stream::iter([Ok::<_, ()>(first)]).chain(stream::pending());
    let outcome = stage_restore_archive(&staging, declared(), parts).await;
    match outcome {
        StagingOutcome::Staged(_) | StagingOutcome::Refused(_) | StagingOutcome::Transport(()) => {
            panic!("an upload that stays silent never ends")
        }
    }
}

/// A retry that stages the whole archive, retains it, and reads it while its restore applies.
async fn retried_upload(quota: StdArc<Quota>, archives: RestoreArchives<StagedMemory>) {
    let staging = MemoryStaging::new(quota.clone());
    let chunks =
        CHUNKS.map(|chunk| Ok::<_, ()>(RestoreStreamPart::Chunk(Bytes::from_static(chunk))));
    let outcome = stage_restore_archive(&staging, declared(), stream::iter(chunks)).await;
    let StagingOutcome::Staged(staged) = outcome else {
        panic!("the retry sends the whole declared archive");
    };
    archives
        .retain(reference(), owner(), declared(), retained_until(), staged)
        .assured("no other user retains an archive under the model reference");
    // The sweep may have released the retention already; a restore that got the archive first
    // keeps it for as long as it reads.
    let Some(reading) = archives.get(&reference(), &owner(), &declared()) else {
        return;
    };
    thread::yield_now();
    assert!(
        !quota.released(reading.hold.id),
        "an archive a restore reads keeps its quota"
    );
}

/// A client abandons its upload after the first chunk and disconnects, which cancels the upload's
/// task, while a retry stages the whole archive and reads it, and retention is swept.
fn an_abandoned_upload_releases_its_quota_exactly_once() {
    let quota = StdArc::new(Quota::default());
    let archives = RestoreArchives::<StagedMemory>::default();
    let abandoned = nervix_primitives::task::spawn(abandoned_upload(quota.clone()));
    let disconnect = thread::spawn(move || {
        abandoned.abort();
        block_on(abandoned)
    });
    let retry = {
        let quota = quota.clone();
        let archives = archives.clone();
        thread::spawn(move || block_on(retried_upload(quota, archives)))
    };
    let sweep = {
        let archives = archives.clone();
        thread::spawn(move || archives.sweep(retained_until()))
    };
    let cancelled = disconnect.join().assured(MODEL_THREAD_JOINS);
    assert!(
        cancelled.is_err(),
        "the abandoned upload ends only when cancelled"
    );
    retry.join().assured(MODEL_THREAD_JOINS);
    sweep.join().assured(MODEL_THREAD_JOINS);
    archives.sweep(retained_until());

    // The abandoned upload reserved quota unless it was cancelled before it began.
    let reserved = quota.reserved.load(Ordering::SeqCst);
    assert!(
        (1..=2).contains(&reserved),
        "each upload reserves at most once: {reserved}"
    );
    let released = quota.releases();
    for hold in 0..reserved {
        assert_eq!(
            released.get(&hold),
            Some(&1),
            "reservation {hold} is released exactly once: {released:?}"
        );
    }
    assert_eq!(
        released.len(),
        reserved,
        "only reservations are released: {released:?}"
    );
}

#[test]
fn shuttle_concurrent_restore_retries_join_one_execution() {
    check_interleavings(concurrent_retries_join_one_execution);
}

#[test]
fn shuttle_a_resumed_restore_never_repeats_a_recorded_effect() {
    check_interleavings(a_resumed_restore_never_repeats_a_recorded_effect);
}

#[test]
fn shuttle_an_abandoned_restore_upload_releases_its_quota_exactly_once() {
    check_interleavings(an_abandoned_upload_releases_its_quota_exactly_once);
}
