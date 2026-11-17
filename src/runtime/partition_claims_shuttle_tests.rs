//! A claimant unit's parked input under concurrent arrivals and claim settlement, explored under
//! Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The ordering invariant of a claim: whatever order a unit's inputs, its catalog read
//!   and its claims' results reach it, each key's input runs exactly once and in arrival order, a
//!   claimed key's moved state is installed before any of the key's input runs, and a key no unit
//!   holds begins with its first input.
//! - **Depends on.** The production claimant and the server Shuttle runner.
//! - **Must not know.** How a holder releases state or how state travels between nodes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::{BTreeMap, BTreeSet};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::{BranchKeyFingerprint, FieldName, Unit};
use nervix_primitives::sync::mpsc;

use super::{CatalogResolution, ClaimAdmission, ClaimStart, PartitionClaimant};
use crate::{runtime::BranchKey, runtime_schema::RuntimeValue};

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";

/// How many inputs each source sends for its key.
const INPUTS_PER_KEY: u64 = 2;

/// How many inputs the two sources send together.
const INPUTS: usize = 4;

fn tenant(name: &str) -> BranchKey {
    BranchKey::from_fields([(
        FieldName::parse("tenant").assured("the model field name is valid"),
        RuntimeValue::String(name.to_string()),
    )])
    .assured("one field forms a concrete branch key")
}

/// What reaches the claimant unit: an input from a source, or a report from one of its claim tasks.
#[derive(Debug)]
enum Event {
    Input { key: BranchKey, sequence: u64 },
    CatalogRead(BTreeMap<BranchKeyFingerprint, Unit>),
    Claimed(BranchKey),
}

/// What the unit did for one key, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Applied {
    /// The key's moved state was installed.
    Installed,
    /// One of the key's inputs ran.
    Input(u64),
}

/// The claimant unit: the production claimant, the keys the unit holds, what it applied, and how
/// many reports of its claim tasks it still waits for.
struct ClaimantUnit {
    claimant: PartitionClaimant<u64>,
    held: BTreeSet<BranchKeyFingerprint>,
    applied: BTreeMap<BranchKeyFingerprint, Vec<Applied>>,
    reports_pending: usize,
    events: mpsc::Sender<Event>,
}

impl ClaimantUnit {
    fn apply(&mut self, key: &BranchKey, applied: Applied) {
        self.applied
            .entry(key.fingerprint())
            .or_default()
            .push(applied);
    }

    /// Starts a claim task that reports `event` at some point of the exploration.
    fn report(&mut self, event: Event) {
        self.reports_pending = self
            .reports_pending
            .checked_add(1)
            .assured("the model starts a handful of claim tasks");
        let events = self.events.clone();
        nervix_primitives::task::spawn(async move {
            nervix_primitives::task::yield_now().await;
            events
                .send(event)
                .await
                .assured("the unit receives until every report arrived");
        });
    }

    fn report_arrived(&mut self) {
        self.reports_pending = self
            .reports_pending
            .checked_sub(1)
            .assured("every report the unit receives was started by it");
    }

    /// One input: a key the unit holds runs at once; any other waits for its key to be resolved.
    fn input(
        &mut self,
        key: BranchKey,
        sequence: u64,
        holders: &BTreeMap<BranchKeyFingerprint, Unit>,
    ) {
        if self.held.contains(&key.fingerprint()) {
            self.apply(&key, Applied::Input(sequence));
            return;
        }
        match self.claimant.admit(key.clone(), sequence) {
            ClaimAdmission::Begin(sequence) => {
                self.held.insert(key.fingerprint());
                self.apply(&key, Applied::Input(sequence));
            }
            ClaimAdmission::Parked {
                start: ClaimStart::Nothing,
            } => {}
            ClaimAdmission::Parked {
                start: ClaimStart::CatalogRead,
            } => self.report(Event::CatalogRead(holders.clone())),
            ClaimAdmission::Parked {
                start: ClaimStart::Claim { .. },
            } => self.report(Event::Claimed(key)),
            ClaimAdmission::Parked {
                start: ClaimStart::Locate,
            } => panic!("the model's direct claimant resolves keys through its catalog"),
        }
    }

    fn catalog_read(&mut self, holders: BTreeMap<BranchKeyFingerprint, Unit>) {
        for resolution in self.claimant.catalog_read(holders) {
            match resolution {
                CatalogResolution::Begin { key, inputs } => {
                    self.held.insert(key.fingerprint());
                    for sequence in inputs {
                        self.apply(&key, Applied::Input(sequence));
                    }
                }
                CatalogResolution::Claim { key, .. } => self.report(Event::Claimed(key)),
            }
        }
    }

    fn claimed(&mut self, key: BranchKey) {
        self.apply(&key, Applied::Installed);
        self.held.insert(key.fingerprint());
        for sequence in self.claimant.claim_settled(&key) {
            self.apply(&key, Applied::Input(sequence));
        }
    }

    fn inputs_applied(&self) -> usize {
        let mut inputs = 0_usize;
        for applied in self.applied.values() {
            for step in applied {
                if let Applied::Input(_) = step {
                    inputs = inputs
                        .checked_add(1)
                        .assured("the model applies a few inputs");
                }
            }
        }
        inputs
    }
}

/// Two sources send two inputs each for their key while the unit reads its catalog and claims: `beta`
/// lies in unit 1 under the previous count, and no unit holds `eta`.
fn arrivals_racing_the_catalog_read_and_the_claim() {
    shuttle::future::block_on(async {
        let beta = tenant("beta");
        let eta = tenant("eta");
        let holders = BTreeMap::from([(beta.fingerprint(), Unit::partition(1))]);
        let (events, mut received) = mpsc::channel(1);
        let mut sources = Vec::new();
        for key in [beta.clone(), eta.clone()] {
            let events = events.clone();
            sources.push(nervix_primitives::task::spawn(async move {
                for sequence in 1..=INPUTS_PER_KEY {
                    events
                        .send(Event::Input {
                            key: key.clone(),
                            sequence,
                        })
                        .await
                        .assured("the unit receives until every input ran");
                }
            }));
        }
        let mut unit = ClaimantUnit {
            claimant: PartitionClaimant::new(8),
            held: BTreeSet::new(),
            applied: BTreeMap::new(),
            reports_pending: 0,
            events,
        };
        // A lost input or report leaves the unit waiting, which Shuttle reports as a deadlock.
        while unit.inputs_applied() < INPUTS || unit.reports_pending > 0 {
            let event = received
                .recv()
                .await
                .assured("the unit keeps a sender, so the channel stays open");
            match event {
                Event::Input { key, sequence } => unit.input(key, sequence, &holders),
                Event::CatalogRead(holders) => {
                    unit.report_arrived();
                    unit.catalog_read(holders);
                }
                Event::Claimed(key) => {
                    unit.report_arrived();
                    unit.claimed(key);
                }
            }
        }
        for source in sources {
            source.await.assured(MODEL_TASK_JOINS);
        }
        let Some(claimed) = unit.applied.get(&beta.fingerprint()) else {
            panic!("the claimed key's input never ran");
        };
        assert_eq!(
            claimed,
            &vec![Applied::Installed, Applied::Input(1), Applied::Input(2)],
            "the claimed key's state was not installed before its input, or its input ran out of \
             order"
        );
        let Some(begun) = unit.applied.get(&eta.fingerprint()) else {
            panic!("the input of the key no unit holds never ran");
        };
        assert_eq!(
            begun,
            &vec![Applied::Input(1), Applied::Input(2)],
            "the key no unit holds did not begin with its input in arrival order"
        );
    });
}

#[test]
fn shuttle_a_claimed_key_runs_its_input_once_in_arrival_order_after_its_state_installs() {
    check_interleavings(arrivals_racing_the_catalog_read_and_the_claim);
}
