use std::{collections::BTreeSet, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::ClusterNodeName;
use nervix_primitives::sync::Arc;

use super::{Announcer, AnnouncerStep, CheckpointReplication};

/// How long an async test waits for a wake-up it expects before failing instead of hanging.
const WAKE_BOUND: Duration = Duration::from_secs(10);

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("the test node names satisfy the cluster-node grammar")
}

fn nodes(names: &[&str]) -> BTreeSet<ClusterNodeName> {
    names.iter().map(|name| node(name)).collect()
}

fn offered(revision: u64, lagging: &[&str]) -> AnnouncerStep {
    AnnouncerStep::Offer {
        revision,
        lagging: nodes(lagging),
    }
}

#[test]
fn one_announcer_offers_the_newest_revision_to_the_replicas_that_lack_it() {
    let replication = CheckpointReplication::new();
    let replicas = nodes(&["node-2", "node-3"]);
    let mut announcer = replication
        .offer(3)
        .assured("an idle replication starts an announcer");
    assert!(
        replication.offer(2).is_none(),
        "a second offer joins the running announcer"
    );
    assert_eq!(announcer.next(&replicas), offered(3, &["node-2", "node-3"]));

    replication.record(&node("node-2"), 3);
    assert!(replication.offer(5).is_none());
    assert_eq!(
        announcer.next(&replicas),
        offered(5, &["node-2", "node-3"]),
        "a newer offer raises what the running announcer offers"
    );

    replication.record(&node("node-2"), 5);
    replication.record(&node("node-3"), 7);
    assert_eq!(announcer.next(&replicas), AnnouncerStep::Finished);
}

#[test]
fn an_offer_after_the_announcement_ended_starts_another_announcer() {
    let replication = CheckpointReplication::new();
    let replicas = nodes(&["node-2"]);
    let mut first = replication
        .offer(1)
        .assured("an idle replication starts an announcer");
    replication.record(&node("node-2"), 1);
    assert_eq!(first.next(&replicas), AnnouncerStep::Finished);

    let mut second = replication
        .offer(2)
        .assured("an offer after the announcement ended starts another announcer");
    assert_eq!(second.next(&replicas), offered(2, &["node-2"]));
    drop(first);
    assert!(
        replication.offer(3).is_none(),
        "dropping a finished announcer leaves its successor's announcement in place"
    );
    assert_eq!(second.next(&replicas), offered(3, &["node-2"]));
}

#[test]
fn a_placement_without_replicas_ends_its_announcement_at_once() {
    let replication = CheckpointReplication::new();
    let mut announcer = replication
        .offer(4)
        .assured("an idle replication starts an announcer");
    assert_eq!(announcer.next(&BTreeSet::new()), AnnouncerStep::Finished);
    assert!(replication.offer(4).is_some());
}

#[test]
fn a_cancelled_announcer_hands_its_announcement_back() {
    let replication = CheckpointReplication::new();
    let announcer = replication
        .offer(1)
        .assured("an idle replication starts an announcer");
    drop(announcer);
    let mut successor = replication
        .offer(2)
        .assured("an announcement handed back starts another announcer");
    assert_eq!(successor.next(&nodes(&["node-2"])), offered(2, &["node-2"]));
}

#[test]
fn a_retired_replication_offers_nothing_and_ends_its_announcer() {
    let replication = CheckpointReplication::new();
    let mut announcer = replication
        .offer(1)
        .assured("an idle replication starts an announcer");
    drop(replication);
    assert_eq!(
        announcer.next(&nodes(&["node-2"])),
        AnnouncerStep::Finished,
        "an announcer of a state that is gone must stop offering it"
    );
}

#[nervix_primitives::test]
async fn a_wait_completes_on_the_report_that_satisfies_it() {
    let replication = Arc::new(CheckpointReplication::new());
    let replica = node("node-2");
    let waiting = nervix_primitives::task::spawn({
        let replication = replication.clone();
        let replica = replica.clone();
        async move {
            replication
                .wait_until(|progress| progress.holds(&replica, 3))
                .await;
        }
    });
    replication.record(&replica, 2);
    nervix_primitives::task::yield_now().await;
    replication.record(&replica, 3);
    nervix_primitives::time::timeout(WAKE_BOUND, waiting)
        .await
        .assured("the report that satisfied the wait wakes it")
        .assured("the waiting task does not panic");
}

#[nervix_primitives::test]
async fn an_announcement_while_nothing_waits_wakes_the_next_wait() {
    let replication = CheckpointReplication::new();
    replication.announced();
    nervix_primitives::time::timeout(WAKE_BOUND, replication.next_announcement())
        .await
        .assured("an announcement that arrived while nothing waited is kept for the next wait");
}

/// The replicas the property draws from: few enough that sequences repeat reports and assign
/// every subset of them.
const PROPERTY_REPLICAS: usize = 4;
/// The revisions the property draws from.
const PROPERTY_REVISIONS: u8 = 16;

/// One step of the property's sequence.
#[derive(Debug, Clone, Copy)]
enum Operation {
    /// A replica reports holding a revision.
    Report { replica: usize, revision: u64 },
    /// The owner offers a revision.
    Offer { revision: u64 },
    /// The running announcer steps while the replicas `assigned` names are the placement's.
    Step { assigned: u8 },
    /// The task driving the running announcer is cancelled.
    Cancel,
    /// The replicated state goes away.
    Retire,
}

impl Operation {
    /// Decode the operations in `bytes`, three bytes each; a trailing partial operation is ignored.
    fn decode(bytes: &[u8]) -> Vec<Self> {
        let mut operations = Vec::new();
        let (whole_operations, _trailing_partial) = bytes.as_chunks::<3>();
        for chunk in whole_operations {
            let operation = match chunk[0] % 8 {
                0..=2 => Self::Report {
                    replica: usize::from(chunk[1]) % PROPERTY_REPLICAS,
                    revision: u64::from(chunk[2] % PROPERTY_REVISIONS),
                },
                3 | 4 => Self::Offer {
                    revision: u64::from(chunk[1] % PROPERTY_REVISIONS),
                },
                5 => Self::Step {
                    assigned: chunk[1] % 16,
                },
                6 => Self::Cancel,
                _ => Self::Retire,
            };
            operations.push(operation);
        }
        operations
    }
}

/// The announcement the property expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedAnnouncement {
    Idle,
    Offering(u64),
    Retired,
}

/// One placement's replication driven through a sequence, beside what the property expects of it.
struct ReplicationProperty {
    replicas: [ClusterNodeName; PROPERTY_REPLICAS],
    /// The replication under test, until the replicated state goes away.
    production: Option<CheckpointReplication>,
    /// The announcer the property drives, while one runs.
    announcer: Option<Announcer>,
    /// The highest revision each replica reported.
    held: [Option<u64>; PROPERTY_REPLICAS],
    announcement: ExpectedAnnouncement,
    /// How many replicas of each assignment held each revision after the previous step.
    holding: Vec<usize>,
}

impl ReplicationProperty {
    fn new() -> Self {
        let mut property = Self {
            replicas: [
                node("node-1"),
                node("node-2"),
                node("node-3"),
                node("node-4"),
            ],
            production: Some(CheckpointReplication::new()),
            announcer: None,
            held: [None; PROPERTY_REPLICAS],
            announcement: ExpectedAnnouncement::Idle,
            holding: Vec::new(),
        };
        property.holding = property.holding_counts();
        property
    }

    fn run(&mut self, operations: Vec<Operation>) {
        for operation in operations {
            match operation {
                Operation::Report { replica, revision } => self.report(replica, revision),
                Operation::Offer { revision } => self.offer(revision),
                Operation::Step { assigned } => self.step(assigned),
                Operation::Cancel => self.cancel(),
                Operation::Retire => self.retire(),
            }
            self.check();
        }
    }

    /// The replicas the bits of `assigned` name.
    fn assignment(&self, assigned: u8) -> BTreeSet<ClusterNodeName> {
        let mut replicas = BTreeSet::new();
        for (index, replica) in self.replicas.iter().enumerate() {
            if assigned & (1 << index) != 0 {
                replicas.insert(replica.clone());
            }
        }
        replicas
    }

    fn report(&mut self, replica: usize, revision: u64) {
        let Some(production) = self.production.as_ref() else {
            return;
        };
        production.record(&self.replicas[replica], revision);
        let raised = match self.held[replica] {
            Some(held) => held.max(revision),
            None => revision,
        };
        self.held[replica] = Some(raised);
    }

    fn offer(&mut self, revision: u64) {
        let Some(production) = self.production.as_ref() else {
            return;
        };
        let started = production.offer(revision);
        match self.announcement {
            ExpectedAnnouncement::Idle => {
                assert!(
                    self.announcer.is_none(),
                    "an idle announcement has no announcer"
                );
                assert!(
                    started.is_some(),
                    "an offer to an idle replication starts an announcer"
                );
                self.announcer = started;
                self.announcement = ExpectedAnnouncement::Offering(revision);
            }
            ExpectedAnnouncement::Offering(offered) => {
                assert!(
                    started.is_none(),
                    "an offer joins the running announcer instead of starting a second one"
                );
                self.announcement = ExpectedAnnouncement::Offering(offered.max(revision));
            }
            ExpectedAnnouncement::Retired => {
                panic!("the property only offers while the replicated state lives");
            }
        }
    }

    fn step(&mut self, assigned: u8) {
        let replicas = self.assignment(assigned);
        let Some(announcer) = self.announcer.as_mut() else {
            return;
        };
        let step = announcer.next(&replicas);
        let expected = match self.announcement {
            ExpectedAnnouncement::Offering(revision) => {
                let mut lagging = BTreeSet::new();
                for (index, replica) in self.replicas.iter().enumerate() {
                    let holds = match self.held[index] {
                        Some(held) => held >= revision,
                        None => false,
                    };
                    if replicas.contains(replica) && !holds {
                        lagging.insert(replica.clone());
                    }
                }
                if lagging.is_empty() {
                    AnnouncerStep::Finished
                } else {
                    AnnouncerStep::Offer { revision, lagging }
                }
            }
            ExpectedAnnouncement::Retired => AnnouncerStep::Finished,
            ExpectedAnnouncement::Idle => {
                panic!("an announcer only runs while its announcement is offered or retired")
            }
        };
        assert_eq!(step, expected);
        if step == AnnouncerStep::Finished {
            self.announcer = None;
            if self.announcement != ExpectedAnnouncement::Retired {
                self.announcement = ExpectedAnnouncement::Idle;
            }
        }
    }

    fn cancel(&mut self) {
        if self.announcer.take().is_none() {
            return;
        }
        if let ExpectedAnnouncement::Offering(_) = self.announcement {
            self.announcement = ExpectedAnnouncement::Idle;
        }
    }

    fn retire(&mut self) {
        if self.production.take().is_none() {
            return;
        }
        self.announcement = ExpectedAnnouncement::Retired;
    }

    /// How many replicas of every assignment hold every revision, in one fixed order.
    fn holding_counts(&self) -> Vec<usize> {
        let Some(production) = self.production.as_ref() else {
            return Vec::new();
        };
        let mut counts = Vec::new();
        for assigned in 0..16 {
            let replicas = self.assignment(assigned);
            for revision in 0..u64::from(PROPERTY_REVISIONS) {
                counts.push(
                    production.with_progress(|progress| progress.holding(&replicas, revision)),
                );
            }
        }
        counts
    }

    fn check(&mut self) {
        match self.announcement {
            ExpectedAnnouncement::Offering(_) => assert!(
                self.announcer.is_some(),
                "an offered revision always has an announcer driving it"
            ),
            ExpectedAnnouncement::Idle => assert!(self.announcer.is_none()),
            ExpectedAnnouncement::Retired => {}
        }
        let Some(production) = self.production.as_ref() else {
            return;
        };
        for (index, replica) in self.replicas.iter().enumerate() {
            let held = production.with_progress(|progress| progress.held(replica));
            assert_eq!(
                held, self.held[index],
                "a replica's progress is the highest revision it reported"
            );
        }
        let holding = self.holding_counts();
        for (now, before) in holding.iter().zip(&self.holding) {
            assert!(
                now >= before,
                "a quorum of replicas holding a revision never shrinks"
            );
        }
        self.holding = holding;
    }
}

#[test]
fn bolero_replica_progress_keeps_the_monotonic_quorum_contract() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .for_each(|bytes: &[u8]| {
            let mut property = ReplicationProperty::new();
            property.run(Operation::decode(bytes));
        });
}
