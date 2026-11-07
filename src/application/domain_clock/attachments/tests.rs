//! Clock attachment fencing and wakeups through the production attachment owner.
//!
//! Layer: test harness.
//! - **Owns.** Revision, incarnation, availability, replacement and stop invariants.
//! - **Depends on.** The production attachment owner and selected execution primitives.
//! - **Must not know.** Sockets, gossip keys, graph installation or elapsed-time scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the production clock attachment owner")
)]

use std::collections::{BTreeMap, BTreeSet};

use ahash::{HashMap, HashMapExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::{DomainClockAttachReceipt, DomainClockAttachRequest};
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, DomainClockAuthorityRevision,
    DomainClockPeriod, DomainClockState, DomainName, DomainTimeRate, Timestamp,
};

use super::{
    DomainClockAttachmentInstaller, DomainClockAttachmentObservation,
    DomainClockAttachmentObserver, DomainClockAttachmentTarget, DomainClockAttachments,
};
use crate::application::domain_clock::DomainClockTaskSpec;

struct Fixture {
    attachments: DomainClockAttachments,
    installation: DomainClockAttachmentInstaller,
    domain: DomainName,
    spec: DomainClockTaskSpec,
    request: DomainClockAttachRequest,
    live: BTreeSet<ClusterNodeIdentity>,
    members: BTreeSet<ClusterNodeName>,
}

impl Fixture {
    fn new() -> Self {
        let authority = Self::identity("authority", 1);
        let target = Self::identity("receiver", 2);
        let domain = DomainName::parse("clock").assured("the fixture domain is valid");
        let period = DomainClockPeriod::try_from(std::time::Duration::from_millis(100))
            .assured("the fixture period is positive");
        let spec = DomainClockTaskSpec {
            clock: DomainClockState::new(
                Timestamp::from_unix_nanos(0),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
            period,
            generation: 1,
            authority_revision: DomainClockAuthorityRevision::INITIAL,
            authority: authority.clone(),
        };
        let request = DomainClockAttachRequest {
            domain_id: domain.clone(),
            target: target.clone(),
            installed_revision: 7,
            generation: spec.generation,
            authority_revision: spec.authority_revision,
            authority,
        };
        let (attachments, mut installation) = DomainClockAttachments::new(spec.authority.clone());
        let mut desired = HashMap::new();
        desired.insert(domain.clone(), spec.clone());
        installation.install(&desired, 7);
        Self {
            attachments,
            installation,
            domain,
            spec,
            request,
            live: BTreeSet::from([target.clone()]),
            members: BTreeSet::from([target.node_id().clone()]),
        }
    }

    fn identity(name: &str, incarnation: u64) -> ClusterNodeIdentity {
        ClusterNodeIdentity::new(
            ClusterNodeName::parse(name).assured("fixture node names are valid"),
            ClusterNodeIncarnation::new(incarnation),
        )
    }

    fn observer(&self) -> DomainClockAttachmentObserver {
        self.attachments
            .observer(&self.domain, &self.spec)
            .assured("the fixture installed this exact authority before observing it")
    }

    fn attach(&self, request: &DomainClockAttachRequest) -> DomainClockAttachReceipt {
        self.attachments
            .attach(request.target.node_id(), 11, request, &self.live)
    }
}

impl DomainClockAttachmentObserver {
    /// These fixed fixture views are already known before any competing participant starts.
    fn observe_current(
        &mut self,
        live: &BTreeSet<ClusterNodeIdentity>,
        members: &BTreeSet<ClusterNodeName>,
    ) -> Option<BTreeMap<ClusterNodeIdentity, DomainClockAttachmentTarget>> {
        self.prepare_observation();
        match self.observe(live, members) {
            DomainClockAttachmentObservation::Current(targets) => Some(targets),
            DomainClockAttachmentObservation::Superseded => None,
            DomainClockAttachmentObservation::Refresh => {
                panic!("an uncontended fixture observation changed")
            }
        }
    }
}

#[cfg(not(feature = "shuttle"))]
mod ordinary {
    use super::*;

    #[test]
    fn a_duplicate_acceptance_invalidates_one_snapshot_without_replacing_its_lifetime() {
        let fixture = Fixture::new();
        let mut observer = fixture.observer();
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::Accepted
        );
        let initial = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the authority is installed");
        let lifetime = initial[&fixture.request.target].lifetime.clone();
        let mut notifications = observer.changes.clone();
        observer.prepare_observation();
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::Accepted
        );
        assert!(
            notifications
                .has_changed()
                .assured("the fixture retains its sender")
        );
        drop(notifications.borrow_and_update());
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::Accepted
        );
        assert!(
            !notifications
                .has_changed()
                .assured("one invalidation already wakes this read")
        );
        assert!(matches!(
            observer.observe(&BTreeSet::new(), &BTreeSet::new()),
            DomainClockAttachmentObservation::Refresh
        ));
        let current = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the accepted target survives the preceding empty view");
        assert_eq!(current.len(), 1);
        assert_eq!(
            current[&fixture.request.target].revision,
            fixture.request.installed_revision
        );
        assert_eq!(current[&fixture.request.target].peer_process_epoch, 11);
        assert!(!lifetime.is_cancelled());
        assert!(!current[&fixture.request.target].lifetime.is_cancelled());
        current[&fixture.request.target].lifetime.cancel();
        assert!(
            lifetime.is_cancelled(),
            "the duplicate retains the same cancellation lifetime"
        );
    }

    #[test]
    fn an_availability_snapshot_cannot_discard_an_attachment_accepted_after_it() {
        for receiver_in_snapshot in [true, false] {
            let fixture = Fixture::new();
            let mut observer = fixture.observer();
            observer.prepare_observation();
            let (preceding_view, preceding_members) = if receiver_in_snapshot {
                (fixture.live.clone(), fixture.members.clone())
            } else {
                (BTreeSet::new(), BTreeSet::new())
            };
            let mut request = fixture.request.clone();
            request.target = Fixture::identity("receiver", 3);
            let current_view = BTreeSet::from([request.target.clone()]);
            assert_eq!(
                fixture
                    .attachments
                    .attach(request.target.node_id(), 12, &request, &current_view,),
                DomainClockAttachReceipt::Accepted
            );
            assert!(matches!(
                observer.observe(&preceding_view, &preceding_members),
                DomainClockAttachmentObservation::Refresh
            ));
            let targets = observer
                .observe_current(&current_view, &fixture.members)
                .assured("the accepted receiver remains available");
            assert_eq!(
                targets.keys().cloned().collect::<Vec<_>>(),
                vec![request.target],
                "a stale availability snapshot must not remove an accepted attachment",
            );
        }
    }

    #[test]
    fn a_delayed_attachment_cannot_replace_a_newer_receiver_incarnation() {
        let fixture = Fixture::new();
        let mut observer = fixture.observer();
        let mut request = fixture.request.clone();
        request.target = Fixture::identity("receiver", 3);
        let live = BTreeSet::from([request.target.clone()]);
        assert_eq!(
            fixture
                .attachments
                .attach(request.target.node_id(), 12, &request, &live),
            DomainClockAttachReceipt::Accepted
        );
        let targets = observer
            .observe_current(&live, &fixture.members)
            .assured("the authority remains installed");
        let current = &targets[&request.target];
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::UnavailableTarget,
            "a handler holding a preceding availability view cannot replace a newer receiver",
        );
        assert!(!current.lifetime.is_cancelled());
        let after = observer
            .observe_current(&live, &fixture.members)
            .assured("the accepted incarnation remains installed");
        assert_eq!(
            after.keys().cloned().collect::<Vec<_>>(),
            vec![request.target]
        );
    }

    #[test]
    fn a_newer_attachment_ends_its_predecessor_and_a_late_one_cannot_replace_it() {
        let fixture = Fixture::new();
        let mut observer = fixture.observer();
        let mut request = fixture.request.clone();
        request.installed_revision = 6;
        assert_eq!(
            fixture.attach(&request),
            DomainClockAttachReceipt::StaleRevision
        );
        request.installed_revision = 7;
        assert_eq!(fixture.attach(&request), DomainClockAttachReceipt::Accepted);
        let targets = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the fixture authority remains installed");
        let first = targets[&request.target].clone();
        assert!(!first.lifetime.is_cancelled());

        request.installed_revision = 8;
        assert_eq!(fixture.attach(&request), DomainClockAttachReceipt::Accepted);
        assert!(first.lifetime.is_cancelled());
        let targets = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the fixture authority remains installed");
        let current = targets[&request.target].clone();
        assert_eq!(current.revision, 8);
        assert!(!current.lifetime.is_cancelled());

        request.installed_revision = 7;
        assert_eq!(
            fixture.attach(&request),
            DomainClockAttachReceipt::StaleRevision
        );
        assert!(!current.lifetime.is_cancelled());
        request.installed_revision = 8;
        assert_eq!(fixture.attach(&request), DomainClockAttachReceipt::Accepted);
        assert!(!current.lifetime.is_cancelled());
        assert!(
            !observer
                .changes
                .has_changed()
                .assured("the fixture retains its owner")
        );
    }

    #[test]
    fn changing_the_authenticated_transport_process_ends_the_preceding_delivery() {
        let fixture = Fixture::new();
        let mut observer = fixture.observer();
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::Accepted
        );
        let preceding = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the fixture authority remains installed");
        let lifetime = preceding[&fixture.request.target].lifetime.clone();
        assert_eq!(
            fixture.attachments.attach(
                fixture.request.target.node_id(),
                12,
                &fixture.request,
                &fixture.live,
            ),
            DomainClockAttachReceipt::Accepted
        );
        assert!(lifetime.is_cancelled());
        let current = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the fixture authority remains installed");
        assert_eq!(current[&fixture.request.target].peer_process_epoch, 12);
    }

    #[test]
    fn attaching_one_domain_does_not_wake_another_domains_producer() {
        let mut fixture = Fixture::new();
        let other = DomainName::parse("another_clock").assured("the fixture name is valid");
        let mut desired = HashMap::new();
        desired.insert(fixture.domain.clone(), fixture.spec.clone());
        desired.insert(other.clone(), fixture.spec.clone());
        fixture.installation.install(&desired, 7);
        let first = fixture.observer();
        let second = fixture
            .attachments
            .observer(&other, &fixture.spec)
            .assured("the fixture installed the second authority");
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::Accepted
        );
        assert!(
            first
                .changes
                .has_changed()
                .assured("the fixture retains the first slot")
        );
        assert!(
            !second
                .changes
                .has_changed()
                .assured("the fixture retains the second slot")
        );
    }

    #[test]
    fn an_unavailable_member_has_no_delivery_and_resumes_only_its_exact_incarnation() {
        let fixture = Fixture::new();
        let mut observer = fixture.observer();
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::Accepted
        );
        let targets = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the fixture authority remains installed");
        let lifetime = targets[&fixture.request.target].lifetime.clone();
        assert!(
            observer
                .observe_current(&BTreeSet::new(), &fixture.members)
                .assured("unavailability does not change authority")
                .is_empty()
        );
        assert_eq!(
            observer
                .observe_current(&fixture.live, &fixture.members)
                .assured("the same member can resume")
                .len(),
            1
        );
        assert!(!lifetime.is_cancelled());

        let replacement = Fixture::identity("receiver", 3);
        let live = BTreeSet::from([replacement.clone()]);
        assert!(
            observer
                .observe_current(&live, &fixture.members)
                .assured("the authority remains installed")
                .is_empty()
        );
        assert!(lifetime.is_cancelled());
        assert_eq!(
            fixture.attachments.attach(
                fixture.request.target.node_id(),
                11,
                &fixture.request,
                &live,
            ),
            DomainClockAttachReceipt::UnavailableTarget
        );
        let mut request = fixture.request.clone();
        request.target = replacement;
        assert_eq!(
            fixture
                .attachments
                .attach(request.target.node_id(), 12, &request, &live),
            DomainClockAttachReceipt::Accepted
        );
        let targets = observer
            .observe_current(&live, &fixture.members)
            .assured("the replacement attached to the current authority");
        assert_eq!(
            targets.keys().cloned().collect::<Vec<_>>(),
            vec![request.target]
        );
    }

    #[test]
    fn a_new_incarnation_does_not_inherit_its_predecessors_installed_revision() {
        let fixture = Fixture::new();
        let mut observer = fixture.observer();
        let mut preceding = fixture.request.clone();
        preceding.installed_revision = 8;
        assert_eq!(
            fixture.attach(&preceding),
            DomainClockAttachReceipt::Accepted
        );
        let targets = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the fixture authority remains installed");
        let lifetime = targets[&preceding.target].lifetime.clone();
        let mut request = fixture.request.clone();
        request.target = Fixture::identity("receiver", 3);
        let live = BTreeSet::from([request.target.clone()]);
        assert_eq!(
            fixture
                .attachments
                .attach(request.target.node_id(), 12, &request, &live),
            DomainClockAttachReceipt::Accepted
        );
        assert!(lifetime.is_cancelled());
        let current = observer
            .observe_current(&live, &fixture.members)
            .assured("the replacement attaches before a producer reconciles availability");
        assert_eq!(current[&request.target].revision, 7);
    }

    #[test]
    fn a_departed_nonmember_releases_its_attachment_metadata_and_lifetime() {
        let fixture = Fixture::new();
        let mut observer = fixture.observer();
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::Accepted
        );
        let targets = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the fixture authority remains installed");
        let lifetime = targets[&fixture.request.target].lifetime.clone();
        assert!(
            observer
                .observe_current(&BTreeSet::new(), &BTreeSet::new())
                .assured("membership removal does not change authority")
                .is_empty()
        );
        assert!(lifetime.is_cancelled());
        assert!(
            observer
                .observe_current(&fixture.live, &fixture.members)
                .assured("the returned peer must attach again")
                .is_empty()
        );
    }

    #[test]
    fn changing_the_authority_fence_ends_every_preceding_attachment() {
        let mut fixture = Fixture::new();
        let mut observer = fixture.observer();
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::Accepted
        );
        let targets = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the fixture authority remains installed");
        let lifetime = targets[&fixture.request.target].lifetime.clone();
        let mut spec = fixture.spec.clone();
        spec.authority_revision = spec
            .authority_revision
            .checked_next()
            .assured("the fixture starts at revision zero");
        let mut desired = HashMap::new();
        desired.insert(fixture.domain.clone(), spec.clone());
        fixture.installation.install(&desired, 8);
        assert!(lifetime.is_cancelled());
        assert!(
            observer
                .observe_current(&fixture.live, &fixture.members)
                .is_none()
        );
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::SupersededAuthority
        );
        let mut replacement = fixture
            .attachments
            .observer(&fixture.domain, &spec)
            .assured("the fixture installed the replacement authority");
        assert!(
            replacement
                .observe_current(&fixture.live, &fixture.members)
                .assured("the new authority is installed")
                .is_empty()
        );
    }

    #[test]
    fn a_request_can_only_attach_its_authenticated_node_to_an_installed_authority() {
        let fixture = Fixture::new();
        let foreign = Fixture::identity("other", 4);
        assert_eq!(
            fixture
                .attachments
                .attach(foreign.node_id(), 11, &fixture.request, &fixture.live,),
            DomainClockAttachReceipt::ForeignTarget
        );
        let mut request = fixture.request.clone();
        request.authority = Fixture::identity("authority", 2);
        request.installed_revision = u64::MAX;
        assert_eq!(
            fixture.attach(&request),
            DomainClockAttachReceipt::SupersededAuthority
        );

        request = fixture.request.clone();
        request.generation = request
            .generation
            .checked_add(1)
            .assured("the fixture starts at generation one");
        request.installed_revision = u64::MAX;
        assert_eq!(
            fixture.attach(&request),
            DomainClockAttachReceipt::SupersededAuthority
        );
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::Accepted
        );

        let (attachments, _installation) =
            DomainClockAttachments::new(fixture.spec.authority.clone());
        assert_eq!(
            attachments.attach(
                fixture.request.target.node_id(),
                11,
                &fixture.request,
                &fixture.live,
            ),
            DomainClockAttachReceipt::SupersededAuthority
        );
    }
}

#[cfg(feature = "shuttle")]
mod modeled {
    use nervix_model_harness::shuttle::check_dfs;
    use nervix_primitives::{sync::CancellationToken, thread};
    use shuttle::future::block_on;

    use super::*;

    fn attach_replace_and_stop() {
        let mut fixture = Fixture::new();
        let mut observer = fixture.observer();
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::Accepted
        );
        let first = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the initial authority is installed");
        let initial_lifetime = first[&fixture.request.target].lifetime.clone();
        let live = fixture.live.clone();
        let members = fixture.members.clone();
        let waiting = thread::spawn(move || {
            block_on(async move {
                let mut held: Vec<CancellationToken> = Vec::new();
                loop {
                    nervix_primitives::task::consume_budget().await;
                    observer.prepare_observation();
                    let targets = match observer.observe(&live, &members) {
                        DomainClockAttachmentObservation::Current(targets) => targets,
                        DomainClockAttachmentObservation::Refresh => continue,
                        DomainClockAttachmentObservation::Superseded => {
                            for lifetime in held {
                                assert!(lifetime.is_cancelled());
                            }
                            return;
                        }
                    };
                    for target in targets.into_values() {
                        held.push(target.lifetime);
                    }
                    observer.changed().await;
                }
            });
        });
        let mut request = fixture.request.clone();
        request.installed_revision = 8;
        let attachments = fixture.attachments.clone();
        let live = fixture.live.clone();
        let replacing = thread::spawn(move || {
            let receipt = attachments.attach(request.target.node_id(), 11, &request, &live);
            assert!(matches!(
                receipt,
                DomainClockAttachReceipt::Accepted | DomainClockAttachReceipt::SupersededAuthority
            ));
        });
        fixture.installation.clear();
        assert!(initial_lifetime.is_cancelled());
        replacing
            .join()
            .assured("Shuttle propagates a replacement participant's panic");
        waiting
            .join()
            .assured("Shuttle fails the execution if its observer panics");
    }

    #[test]
    fn shuttle_attachment_replacement_and_stop_wake_the_exact_delivery_lifetime() {
        // Replacement races authority stop and the producer's observe-before-wait protocol.
        // A lost wake leaves no runnable thread, which Shuttle reports as a deadlock.
        check_dfs(attach_replace_and_stop, Some(1_000));
    }

    fn attach_during_availability_observation(duplicate: bool) {
        let fixture = Fixture::new();
        let mut observer = fixture.observer();
        assert_eq!(
            fixture.attach(&fixture.request),
            DomainClockAttachReceipt::Accepted
        );
        let first = observer
            .observe_current(&fixture.live, &fixture.members)
            .assured("the initial authority is installed");
        let initial_lifetime = first[&fixture.request.target].lifetime.clone();
        observer.prepare_observation();
        let (preceding_view, preceding_members) = if duplicate {
            (BTreeSet::new(), BTreeSet::new())
        } else {
            (fixture.live.clone(), fixture.members.clone())
        };

        let mut request = fixture.request.clone();
        let process_epoch;
        if duplicate {
            process_epoch = 11;
        } else {
            request.target = Fixture::identity("receiver", 3);
            process_epoch = 12;
        }
        let target = request.target.clone();
        let current_view = BTreeSet::from([target.clone()]);
        let attachments = fixture.attachments.clone();
        let live = current_view.clone();
        let replacing = thread::spawn(move || {
            assert_eq!(
                attachments.attach(request.target.node_id(), process_epoch, &request, &live),
                DomainClockAttachReceipt::Accepted
            );
        });
        let attachments = fixture.attachments.clone();
        let request = fixture.request.clone();
        let live = fixture.live.clone();
        let delayed = thread::spawn(move || {
            let receipt = attachments.attach(request.target.node_id(), 11, &request, &live);
            assert!(matches!(
                receipt,
                DomainClockAttachReceipt::Accepted | DomainClockAttachReceipt::UnavailableTarget
            ));
        });
        assert!(!matches!(
            observer.observe(&preceding_view, &preceding_members),
            DomainClockAttachmentObservation::Superseded
        ));
        replacing
            .join()
            .assured("the replacement participant must complete");
        delayed
            .join()
            .assured("the delayed participant must complete");
        let targets = observer
            .observe_current(&current_view, &fixture.members)
            .assured("the authority remains installed after both handlers finish");
        assert_eq!(
            targets.keys().cloned().collect::<Vec<_>>(),
            vec![target.clone()]
        );
        assert!(!targets[&target].lifetime.is_cancelled());
        assert_eq!(targets[&target].peer_process_epoch, process_epoch);
        assert_eq!(
            targets[&target].revision,
            fixture.request.installed_revision
        );
        if !duplicate {
            assert!(initial_lifetime.is_cancelled());
        }
    }

    #[test]
    fn shuttle_an_attachment_survives_a_preceding_availability_snapshot() {
        check_dfs(
            || attach_during_availability_observation(false),
            Some(1_000),
        );
    }

    #[test]
    fn shuttle_a_duplicate_attachment_survives_a_preceding_availability_snapshot() {
        check_dfs(|| attach_during_availability_observation(true), Some(1_000));
    }
}
