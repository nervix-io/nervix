//! The measurement of unpublished restore staging a store keeps between the changes to it.
//!
//! Layer: test harness.
//! - **Owns.** Histories of staging, staging that fails its digest, staging cut short, retried
//!   staging, sweeps, publication and reopening over a few domains and generations, and the
//!   comparison of the measurement the store kept with one taken afresh after every step.
//! - **Depends on.** The production store with its staging, publication and reclamation, and a
//!   temporary database.
//! - **Must not know.** What a checkpoint holds, how consensus retains a generation, or which
//!   transport delivered the bytes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeMap;

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_interconnect::backup::RestoreStateInventory;
use nonzero_ext::nonzero;

use super::{
    tests::{authority, store},
    *,
};

/// The domains a history stages into. Two are enough for one restore to stage beside the
/// generation another one left unpublished.
const DOMAINS: [&str; 2] = ["alpha", "beta"];

/// The entities a history stages checkpoints for within one domain.
const ENTITIES: [&str; 2] = ["source", "emitter"];

/// The newest generation a history stages or publishes.
const GENERATIONS: u64 = 3;

/// One staged checkpoint: the domain and entity that place it, and the generation it belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct StagedCheckpoint {
    domain: usize,
    entity: usize,
    generation: u64,
}

impl StagedCheckpoint {
    fn generated(arbitrary: &mut Arbitrary<'_>) -> Self {
        Self {
            domain: arbitrary.entropy().index(nonzero!(2_usize)),
            entity: arbitrary.entropy().index(nonzero!(2_usize)),
            generation: arbitrary.entropy().between(1..=GENERATIONS),
        }
    }

    fn placement(self) -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse(DOMAINS[self.domain]).assured("the test domain is valid"),
            kind: ModelKind::Ingestor,
            identifier: ModelName::parse(ENTITIES[self.entity]).assured("the test entity is valid"),
            state: RuntimeState::KafkaOffset,
            unit: nervix_models::Unit::Single,
            branch_key: None,
        }
    }
}

/// How one attempt at staging a checkpoint ends.
#[derive(Debug, Clone, Copy)]
enum StagingEnd {
    /// Every chunk arrives and their digest matches.
    Complete,
    /// Every chunk arrives, and the digest the checkpoint declared is another stream's.
    WrongDigest,
    /// The storage job is cancelled at this cancellation check.
    CutShortAt(u64),
}

/// A history's store and what it has staged completely and not yet published, by checkpoint.
struct History {
    store: Option<RuntimeStateStore>,
    directory: tempfile::TempDir,
    staged: BTreeMap<StagedCheckpoint, u64>,
}

impl History {
    fn new() -> Self {
        let directory =
            tempfile::tempdir().assured("the test host provides a writable temporary directory");
        let opened = store(directory.path(), DEFAULT_RESTORE_STAGING_MAX_BYTES);
        Self {
            store: Some(opened),
            directory,
            staged: BTreeMap::new(),
        }
    }

    fn store(&self) -> &RuntimeStateStore {
        self.store
            .as_ref()
            .assured("a history holds its store between its steps")
    }

    /// Stage one checkpoint of up to two whole chunks and a partial one.
    fn stage(&mut self, arbitrary: &mut Arbitrary<'_>) {
        let checkpoint = StagedCheckpoint::generated(arbitrary);
        let whole_chunks = usize::try_from(arbitrary.entropy().up_to(2))
            .assured("at most two chunks fit the address space");
        let tail =
            usize::try_from(arbitrary.entropy().up_to(64)).assured("a short tail fits usize");
        let payload =
            vec![arbitrary.entropy().byte(); whole_chunks * RESTORE_STATE_CHUNK_BYTES + tail];
        let end = match arbitrary.entropy().index(nonzero!(4_usize)) {
            0 => StagingEnd::WrongDigest,
            1 => StagingEnd::CutShortAt(arbitrary.entropy().up_to(4)),
            _ => StagingEnd::Complete,
        };
        let digest = match end {
            StagingEnd::WrongDigest => [0xA5; 32],
            StagingEnd::Complete | StagingEnd::CutShortAt(_) => *blake3::hash(&payload).as_bytes(),
        };
        let length = u64::try_from(payload.len()).assured("a bounded test payload fits u64");
        let mut checks = 0_u64;
        let staged = self.store().stage_restored_checkpoint(
            &authority(checkpoint.generation),
            &checkpoint.placement(),
            CheckpointMetadata {
                lsm: 1,
                length,
                digest,
            },
            payload.as_slice(),
            || {
                checks += 1;
                if let StagingEnd::CutShortAt(at) = end
                    && checks > at
                {
                    return Err(Report::new(RuntimePersistenceError::Cancelled));
                }
                Ok(())
            },
        );
        // An attempt removes what an earlier attempt at the same checkpoint left, whether or not
        // it then completes.
        self.staged.remove(&checkpoint);
        if staged.is_ok() {
            self.staged.insert(checkpoint, length);
        }
    }

    /// Run one sweep that retains every unpublished generation or none of them.
    fn sweep(&mut self, arbitrary: &mut Arbitrary<'_>) {
        let retain = arbitrary.entropy().flag();
        let swept = self
            .store()
            .reclaim_restore_staging(|_, _| retain, || Ok(()));
        if swept.is_ok() && !retain {
            self.staged.clear();
        }
    }

    /// Publish one generation of one domain with the inventory this history staged for it.
    fn publish(&mut self, arbitrary: &mut Arbitrary<'_>) {
        let domain = arbitrary.entropy().index(nonzero!(2_usize));
        let generation = arbitrary.entropy().between(1..=GENERATIONS);
        let mut inventory = RestoreStateInventory::default();
        for (checkpoint, length) in &self.staged {
            if checkpoint.domain == domain && checkpoint.generation == generation {
                inventory.checkpoints += 1;
                inventory.payload_bytes += length;
            }
        }
        let name = DomainName::parse(DOMAINS[domain]).assured("the test domain is valid");
        let published = self.store().publish_restored_state(
            &name,
            &authority(generation),
            inventory,
            || Ok(()),
        );
        if published.is_ok() {
            // Publication selects this generation and removes what it supersedes.
            self.staged.retain(|checkpoint, _| {
                checkpoint.domain != domain || checkpoint.generation > generation
            });
        }
    }

    /// Close the store and open its database again, as a restarted node does.
    fn reopen(&mut self) {
        drop(self.store.take());
        self.store = Some(store(
            self.directory.path(),
            DEFAULT_RESTORE_STAGING_MAX_BYTES,
        ));
    }

    /// The measurement the store answers admission and sweeps with is the one a fresh read of
    /// every staged entry gives.
    fn assert_kept_measurement_is_exact(&self, step: &str) {
        let store = self.store();
        let answered = store
            .staged_usage(&mut || Ok(()))
            .assured("the store answers with its staging usage");
        let measured = store
            .measure_staged_usage(&mut || Ok(()))
            .assured("the staged entries are measurable");
        assert_eq!(
            answered, measured,
            "after {step}, the store answers with a measurement that a fresh one contradicts"
        );
    }
}

/// After any history of staging, failed and cut-short staging, retries, sweeps, publication and
/// reopening, the measurement a store kept is the one a fresh read of every staged entry gives.
/// Admission and sweeps answer from the kept one, so a write or a removal that left it standing
/// without accounting for itself would admit against, and report, usage the store does not have.
#[test]
fn bolero_a_kept_staging_measurement_equals_a_fresh_one_after_any_history() {
    bolero::check!()
        .with_iterations(96)
        .with_max_len(96)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let mut history = History::new();
            let steps = arbitrary.entropy().count(10);
            for _ in 0..steps {
                let step = match arbitrary.entropy().index(nonzero!(8_usize)) {
                    0 => {
                        history.sweep(&mut arbitrary);
                        "a sweep"
                    }
                    1 => {
                        history.publish(&mut arbitrary);
                        "a publication"
                    }
                    2 => {
                        history.reopen();
                        "reopening"
                    }
                    _ => {
                        history.stage(&mut arbitrary);
                        "staging"
                    }
                };
                history.assert_kept_measurement_is_exact(step);
            }
        });
}
