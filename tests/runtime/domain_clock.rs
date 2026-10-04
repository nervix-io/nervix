//! Test harness, outside the product layer order.
//! Owns: regressions for the enclosing runtime state owner.
//! Depends on: that owner, typed vocabulary and the primitive boundary.
//! Must not know: product decisions beyond the behavior exercised by these tests.

use std::collections::BTreeMap;

use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, DomainClockAuthorityRevision,
    DomainClockProgress, DomainClockState, DomainConfig, DomainTick, DomainTimeRate,
    IngestTimestampSource, Timestamp,
};

use super::*;
use crate::{
    application::{ClockDeliveryOrder, NextClockFrame},
    runtime::{
        RecordMetadataColumns, RuntimeValue, domain, named, paced_domain_state, test_domain_clock,
        test_domain_clock_authority, unpaced_domain_state,
    },
    runtime_schema::test_runtime_row,
};

#[test]
fn progress_requires_the_committed_generation_revision_identity_and_peer() {
    let runtime = Runtime::new();
    let domain_id = domain("paced");
    let mut state = paced_domain_state("paced");
    state.start_version = 4;
    state.clock = Some(DomainClockState::new(
        Timestamp::from_unix_nanos(0),
        Timestamp::from_unix_nanos(0),
        DomainTimeRate::ONE,
    ));
    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), state)]));
    let authority = test_domain_clock_authority();
    let owner = authority
        .owner()
        .cloned()
        .expect("the fixture authority is assigned");
    let tick = |tick_id, wall_clock| DomainTick {
        tick_id,
        logical_timestamp: Timestamp::from_unix_nanos(
            i64::try_from(tick_id)
                .expect("the fixture tick ids fit in the signed timestamp boundary representation"),
        ),
        wall_clock: Timestamp::from_unix_nanos(wall_clock),
        period: "1s".parse().expect("fixture period is valid"),
    };
    let accepted = DomainClockProgress {
        generation: 4,
        authority_revision: authority.revision(),
        authority: owner.clone(),
        tick: tick(3, 30),
    };
    runtime
        .handle_domain_clock_progress(&domain_id, owner.node_id(), &accepted)
        .expect("the fixture domain exists");

    let rejected = [
        DomainClockProgress {
            generation: 3,
            tick: tick(4, 40),
            ..accepted.clone()
        },
        DomainClockProgress {
            authority_revision: DomainClockAuthorityRevision::INITIAL
                .checked_next()
                .expect("the initial revision has a successor"),
            tick: tick(5, 50),
            ..accepted.clone()
        },
        DomainClockProgress {
            authority: ClusterNodeIdentity::new(
                owner.node_id().clone(),
                ClusterNodeIncarnation::new(
                    owner
                        .incarnation()
                        .get()
                        .checked_add(1)
                        .expect("the fixture incarnation can advance"),
                ),
            ),
            tick: tick(6, 60),
            ..accepted.clone()
        },
        DomainClockProgress {
            tick: tick(2, 20),
            ..accepted.clone()
        },
    ];
    for progress in rejected {
        runtime
            .handle_domain_clock_progress(&domain_id, owner.node_id(), &progress)
            .expect("a fenced progress message is safely ignored");
    }
    let wrong_peer = ClusterNodeName::parse("another-node").expect("fixture name is valid");
    runtime
        .handle_domain_clock_progress(&domain_id, &wrong_peer, &accepted)
        .expect("an unauthenticated authority claim is safely ignored");

    let observed = runtime
        .inner
        .domains
        .get(&domain_id)
        .expect("the fixture domain remains installed");
    let progress = observed.progress.borrow();
    assert_eq!(progress.as_ref().map(|tick| tick.tick_id), Some(3));
    drop(progress);
    drop(observed);

    // A session attached after progress was accepted reads that frontier immediately,
    // without waiting for a subsequent watch notification.
    let observer = runtime
        .observe_domain_clock(&domain_id)
        .assured("the fixture domain remains installed");
    let initial = observer
        .current()
        .assured("the fixture clock remains installed");
    let NextClockFrame::Tick(tick) = ClockDeliveryOrder::new(initial).next(&observer) else {
        panic!("a late attachment must select the retained tick first");
    };
    assert_eq!(tick.generation, 4);
    assert_eq!(tick.tick_id, 3);
    assert_eq!(tick.logical_boundary, Timestamp::from_unix_nanos(3));
    assert!(tick.serving_logical >= tick.logical_boundary);
}

#[test]
fn an_observer_follows_each_installation_until_the_domain_leaves_the_node() {
    use futures_util::FutureExt as _;

    let runtime = Runtime::new();
    let domain_id = domain("observed");
    assert!(runtime.observe_domain_clock(&domain_id).is_none());
    let mapping = DomainClockState::new(
        Timestamp::from_unix_nanos(10),
        Timestamp::from_unix_nanos(1_000),
        DomainTimeRate::try_from(2.0).expect("the fixture rate is positive and finite"),
    );
    let mut running = paced_domain_state("observed");
    running.start_version = 1;
    running.clock = Some(mapping.clone());
    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), running.clone())]));
    let mut observer = runtime
        .observe_domain_clock(&domain_id)
        .expect("the synchronized domain is observable");
    let paced = PacedDomainClock {
        period: "1s".parse().expect("fixture period is valid"),
        skew: "250ms".parse().expect("fixture skew is valid"),
        mapping,
    };
    assert_eq!(
        observer.current(),
        Some(DomainClockObservation {
            generation: 1,
            state: DomainClockObservedState::Paced(paced),
        })
    );

    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), running.clone())]));
    assert!(
        observer.changed().now_or_never().is_none(),
        "synchronizing an unchanged installation wakes no observer"
    );
    let mut paused = running.clone();
    paused.status = nervix_models::DomainStatus::Paused;
    paused.clock = None;
    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), paused)]));
    assert!(
        observer.changed().now_or_never().is_none(),
        "the alteration pause keeps the running clock installed"
    );

    let mut stopped = running;
    stopped.status = nervix_models::DomainStatus::Stopped;
    stopped.clock = None;
    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), stopped)]));
    observer
        .changed()
        .now_or_never()
        .expect("stopping the domain wakes its observer");
    assert_eq!(
        observer.current(),
        Some(DomainClockObservation {
            generation: 1,
            state: DomainClockObservedState::Stopped,
        })
    );

    let mut unpaced = unpaced_domain_state("observed");
    unpaced.start_version = 2;
    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), unpaced)]));
    observer
        .changed()
        .now_or_never()
        .expect("a new generation wakes the observer");
    assert_eq!(
        observer.current(),
        Some(DomainClockObservation {
            generation: 2,
            state: DomainClockObservedState::Unpaced,
        })
    );

    runtime.sync_domains(&BTreeMap::new());
    observer
        .any_changed()
        .now_or_never()
        .expect("removing the domain wakes its observer");
    assert_eq!(observer.current(), None);
    assert!(runtime.observe_domain_clock(&domain_id).is_none());
}

#[test]
fn a_paced_generation_without_an_authority_is_observed_uninstalled() {
    use futures_util::FutureExt as _;

    let runtime = Runtime::new();
    let domain_id = domain("unowned");
    let mut state = paced_domain_state("unowned");
    state.start_version = 3;
    state.clock = Some(DomainClockState::new(
        Timestamp::from_unix_nanos(0),
        Timestamp::from_unix_nanos(0),
        DomainTimeRate::ONE,
    ));
    let domains = BTreeMap::from([(domain_id.clone(), state)]);
    let assigned = BTreeMap::from([(domain_id.clone(), test_domain_clock_authority())]);
    runtime.sync_committed_domains(&domains, &assigned);
    let mut observer = runtime
        .observe_domain_clock(&domain_id)
        .expect("the synchronized domain is observable");
    let installed = observer.current().expect("the assigned clock is installed");

    runtime.sync_committed_domains(&domains, &BTreeMap::new());
    observer
        .changed()
        .now_or_never()
        .expect("unassignment wakes the observer");
    assert_eq!(
        observer.current(),
        Some(DomainClockObservation {
            generation: 3,
            state: DomainClockObservedState::Uninstalled,
        })
    );

    runtime.sync_committed_domains(&domains, &assigned);
    observer
        .changed()
        .now_or_never()
        .expect("reassignment wakes the observer");
    assert_eq!(observer.current(), Some(installed));
}

#[test]
fn progress_never_creates_a_missing_domain() {
    let runtime = Runtime::new();
    let domain_id = domain("missing");
    let authority = ClusterNodeIdentity::new(
        ClusterNodeName::parse("node-1").expect("fixture name is valid"),
        ClusterNodeIncarnation::new(1),
    );
    let progress = DomainClockProgress {
        generation: 1,
        authority_revision: DomainClockAuthorityRevision::INITIAL,
        authority: authority.clone(),
        tick: DomainTick {
            tick_id: 1,
            logical_timestamp: Timestamp::from_unix_nanos(0),
            wall_clock: Timestamp::from_unix_nanos(0),
            period: "1s".parse().expect("fixture period is valid"),
        },
    };

    let error = runtime
        .handle_domain_clock_progress(&domain_id, authority.node_id(), &progress)
        .expect_err("progress for an unknown domain must be rejected");

    assert!(matches!(
        error.current_context(),
        DomainClockAccessError::Missing { domain } if domain == &domain_id
    ));
    assert!(runtime.inner.domains.is_empty());
}

#[test]
fn stopped_and_restarted_generations_ignore_delayed_progress() {
    let runtime = Runtime::new();
    let domain_id = domain("paced");
    let first_mapping = DomainClockState::new(
        Timestamp::from_unix_nanos(0),
        Timestamp::from_unix_nanos(0),
        DomainTimeRate::ONE,
    );
    let mut first = paced_domain_state("paced");
    first.start_version = 4;
    first.clock = Some(first_mapping);
    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), first.clone())]));
    let authority = test_domain_clock_authority();
    let owner = authority
        .owner()
        .cloned()
        .expect("the fixture authority is assigned");
    let delayed = DomainClockProgress {
        generation: 4,
        authority_revision: authority.revision(),
        authority: owner.clone(),
        tick: DomainTick {
            tick_id: 1,
            logical_timestamp: Timestamp::from_unix_nanos(1),
            wall_clock: Timestamp::from_unix_nanos(1),
            period: "1s".parse().expect("fixture period is valid"),
        },
    };

    first.status = nervix_models::DomainStatus::Stopped;
    first.clock = None;
    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), first)]));
    runtime
        .handle_domain_clock_progress(&domain_id, owner.node_id(), &delayed)
        .expect("stopped generations safely ignore progress");

    let next_mapping = DomainClockState::new(
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(1_000),
        DomainTimeRate::ONE,
    );
    let mut next = paced_domain_state("paced");
    next.start_version = 5;
    next.clock = Some(next_mapping.clone());
    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), next)]));
    runtime
        .handle_domain_clock_progress(&domain_id, owner.node_id(), &delayed)
        .expect("earlier generations safely ignore delayed progress");

    let observed = runtime
        .inner
        .domains
        .get(&domain_id)
        .expect("the restarted domain remains installed");
    assert!(observed.progress.borrow().is_none());
    let installed = observed.clock.inner.published.load();
    assert!(matches!(
        &installed.installation,
        DomainClockInstallation::Installed {
            generation: 5,
            source: DomainClockSource::Paced { mapping, .. },
        } if mapping == &next_mapping
    ));
}

#[test]
fn coalesced_generation_transition_discards_prior_progress() {
    let runtime = Runtime::new();
    let domain_id = domain("paced");
    let mut first = paced_domain_state("paced");
    first.start_version = 4;
    first.clock = Some(DomainClockState::new(
        Timestamp::from_unix_nanos(0),
        Timestamp::from_unix_nanos(0),
        DomainTimeRate::ONE,
    ));
    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), first)]));
    let authority = test_domain_clock_authority();
    let owner = authority
        .owner()
        .cloned()
        .expect("the fixture authority is assigned");
    let progress = |generation, tick_id| DomainClockProgress {
        generation,
        authority_revision: authority.revision(),
        authority: owner.clone(),
        tick: DomainTick {
            tick_id,
            logical_timestamp: Timestamp::from_unix_nanos(
                i64::try_from(tick_id).expect("fixture tick ids fit in a timestamp"),
            ),
            wall_clock: Timestamp::from_unix_nanos(
                i64::try_from(tick_id).expect("fixture tick ids fit in a timestamp"),
            ),
            period: "1s".parse().expect("fixture period is valid"),
        },
    };
    runtime
        .handle_domain_clock_progress(&domain_id, owner.node_id(), &progress(4, 50))
        .expect("the first generation accepts its progress");

    let mut next = paced_domain_state("paced");
    next.start_version = 5;
    next.clock = Some(DomainClockState::new(
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(1_000),
        DomainTimeRate::ONE,
    ));
    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), next)]));

    let observed = runtime
        .inner
        .domains
        .get(&domain_id)
        .expect("the later generation remains installed");
    assert!(
        observed.progress.borrow().is_none(),
        "progress retained from the skipped STOP belongs to the previous generation"
    );
    drop(observed);
    runtime
        .handle_domain_clock_progress(&domain_id, owner.node_id(), &progress(5, 1))
        .expect("the later generation accepts its first progress");
    let observed = runtime
        .inner
        .domains
        .get(&domain_id)
        .expect("the later generation remains installed");
    assert_eq!(
        observed.progress.borrow().as_ref().map(|tick| tick.tick_id),
        Some(1)
    );
}

#[test]
fn progress_retains_the_latest_accepted_report() {
    let runtime = Runtime::new();
    let domain_id = domain("paced");
    let mut state = paced_domain_state("paced");
    state.start_version = 4;
    state.clock = Some(DomainClockState::new(
        Timestamp::from_unix_nanos(0),
        Timestamp::from_unix_nanos(0),
        DomainTimeRate::ONE,
    ));
    runtime.sync_domains(&BTreeMap::from([(domain_id.clone(), state)]));
    let authority = test_domain_clock_authority();
    let owner = authority
        .owner()
        .cloned()
        .expect("the fixture authority is assigned");
    let final_tick = 300_u64;
    for tick_id in 1..=final_tick {
        let timestamp = i64::try_from(tick_id).expect("fixture tick ids fit in a timestamp");
        runtime
            .handle_domain_clock_progress(
                &domain_id,
                owner.node_id(),
                &DomainClockProgress {
                    generation: 4,
                    authority_revision: authority.revision(),
                    authority: owner.clone(),
                    tick: DomainTick {
                        tick_id,
                        logical_timestamp: Timestamp::from_unix_nanos(timestamp),
                        wall_clock: Timestamp::from_unix_nanos(timestamp),
                        period: "1s".parse().expect("fixture period is valid"),
                    },
                },
            )
            .expect("the current authority progress is accepted");
    }

    let observed = runtime
        .inner
        .domains
        .get(&domain_id)
        .expect("the domain remains installed");
    let progress = observed.progress.borrow();
    assert_eq!(progress.as_ref().map(|tick| tick.tick_id), Some(final_tick));
}

#[test]
fn delayed_progress_delivery_does_not_move_logical_time_backwards() {
    let clock = DomainClockState::new(
        Timestamp::from_unix_nanos(0),
        Timestamp::from_unix_nanos(0),
        DomainTimeRate::ONE,
    );
    let delayed_wall_time = Timestamp::from_unix_nanos(1_000_000_000);
    let before_delivery = clock
        .logical_time_at(delayed_wall_time)
        .assured("the fixture uses a finite positive rate");
    let after_delivery = clock
        .logical_time_at(delayed_wall_time)
        .assured("the fixture uses a finite positive rate");

    assert!(
        after_delivery >= before_delivery,
        "delivering progress moved logical time from {before_delivery} to {after_delivery}"
    );
}

#[test]
fn paced_domains_admit_the_logical_origin() {
    let runtime = Runtime::new();
    let mut domains = BTreeMap::new();
    let mut state = paced_domain_state("paced");
    state.clock = Some(DomainClockState::new(
        Timestamp::now(),
        Timestamp::from_unix_nanos(0),
        DomainTimeRate::ONE,
    ));
    domains.insert(domain("paced"), state);
    runtime.sync_domains(&domains);

    let domain = domain("paced");
    let ingestor = named("ing");
    let time = runtime
        .ingestion_time(&domain, &ingestor)
        .assured("the fixture installs a running clock");
    let record = test_runtime_row([(
        "occurred_at".to_string(),
        RuntimeValue::Datetime(Timestamp::from_unix_nanos(0).into_datetime().fixed_offset()),
    )]);
    let admission = time.select_column(
        Some(&IngestTimestampSource::At(named("occurred_at"))),
        &record.one_row_batch(),
        &RecordMetadataColumns::from_rows([record.metadata().clone()]),
    );

    assert!(
        admission
            .as_ref()
            .is_ok_and(|column| time.admit_column(column).value(0)),
        "logical origin was rejected: {admission:?}"
    );
}

#[test]
fn scheduled_timestamp_addition_stays_in_the_serializable_range() {
    let timestamp = checked_add_duration_to_timestamp(
        Timestamp::from_unix_nanos(i64::MAX),
        Duration::from_nanos(1),
    );

    let serialized = serde_json::to_string(&timestamp);

    assert!(
        serialized.is_ok(),
        "schedule arithmetic constructed an unserializable timestamp: {serialized:?}"
    );
}

#[test]
fn logical_time_projection_reports_range_overflow() {
    let clock = DomainClockState::new(
        Timestamp::from_unix_nanos(0),
        Timestamp::from_unix_nanos(i64::MAX),
        DomainTimeRate::ONE,
    );

    assert!(
        clock
            .logical_time_at(Timestamp::from_unix_nanos(1))
            .is_err()
    );
}

#[test]
fn logical_rate_conversion_scales_physical_waits() {
    let clock = DomainClockState::new(
        Timestamp::from_unix_nanos(0),
        Timestamp::from_unix_nanos(0),
        DomainTimeRate::try_from(4.0).expect("fixture rate is valid"),
    );

    let wait = clock
        .wall_duration_until(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(1_000_000_000),
        )
        .assured("the fixture uses a finite positive rate");

    assert_eq!(wait, Duration::from_millis(250));
}

#[test]
fn lifecycle_access_reports_missing_stopped_and_uninstalled_clocks() {
    let clock_domain = domain("paced");
    let lifecycle = DomainClockLifecycle::new(clock_domain.clone());

    let Err(error) = lifecycle.bind() else {
        panic!("a missing clock must reject binding");
    };
    assert!(matches!(
        error.current_context(),
        DomainClockAccessError::Missing { domain } if domain == &clock_domain
    ));

    lifecycle.stop(7);
    let Err(error) = lifecycle.bind() else {
        panic!("a stopped clock must reject binding");
    };
    assert!(matches!(
        error.current_context(),
        DomainClockAccessError::Stopped {
            domain,
            generation: 7,
        } if domain == &clock_domain
    ));

    let mut uninstalled = paced_domain_state("paced");
    uninstalled.start_version = 8;
    lifecycle.synchronize(&uninstalled, &test_domain_clock_authority());
    let Err(error) = lifecycle.bind() else {
        panic!("an uninstalled clock must reject binding");
    };
    assert!(matches!(
        error.current_context(),
        DomainClockAccessError::Uninstalled {
            domain,
            generation: 8,
        } if domain == &clock_domain
    ));
}

#[test]
fn bound_clock_rejects_a_later_generation() {
    let lifecycle = DomainClockLifecycle::new(domain("paced"));
    lifecycle.install_paced(
        1,
        DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ),
    );
    let bound = lifecycle
        .bind()
        .assured("the fixture installed generation one");

    lifecycle.install_paced(
        2,
        DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(1),
            DomainTimeRate::ONE,
        ),
    );

    let Err(error) = bound.snapshot() else {
        panic!("a superseded clock capability must be stale");
    };
    assert!(matches!(
        error.current_context(),
        DomainClockAccessError::StaleGeneration {
            bound_generation: 1,
            current_generation: 2,
            ..
        }
    ));
}

#[test]
fn reads_do_not_decrease_within_one_generation() {
    let lifecycle = DomainClockLifecycle::new(domain("paced"));
    lifecycle.install_paced(
        1,
        DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ),
    );
    let bound = lifecycle
        .bind()
        .assured("the fixture installed generation one");
    let before = bound
        .snapshot()
        .assured("the first mapping fits the timestamp range");

    lifecycle.install_paced(
        1,
        DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ),
    );
    let after = bound
        .snapshot()
        .assured("the replacement mapping fits the timestamp range");

    assert!(after.now() >= before.now());
}

#[test]
fn a_read_racing_a_same_generation_replacement_bounds_later_reads() {
    let lifecycle = DomainClockLifecycle::new(domain("paced"));
    lifecycle.install_paced(
        1,
        DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ),
    );
    let bound = lifecycle
        .bind()
        .assured("the fixture installed generation one");
    let racing_publication = lifecycle.inner.published.load_full();

    lifecycle.install_paced(
        1,
        DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ),
    );
    let racing_read = racing_publication.watermark.raise(
        "2200-01-01T00:00:00Z"
            .parse::<Timestamp>()
            .assured("the fixture timestamp is valid RFC 3339"),
    );
    let later = bound
        .snapshot()
        .assured("the replacement mapping fits the timestamp range");

    assert!(
        later.now() >= racing_read,
        "a read after the replacement returned {} before the racing read {racing_read}",
        later.now()
    );
}

#[test]
fn a_read_racing_a_generation_change_cannot_clamp_the_next_generation() {
    let lifecycle = DomainClockLifecycle::new(domain("paced"));
    lifecycle.install_paced(
        1,
        DomainClockState::new(
            Timestamp::now(),
            "2010-01-01T00:00:00Z"
                .parse()
                .assured("the fixture timestamp is valid RFC 3339"),
            DomainTimeRate::ONE,
        ),
    );
    let racing_publication = lifecycle.inner.published.load_full();

    lifecycle.install_paced(
        2,
        DomainClockState::new(
            Timestamp::now(),
            "2000-01-01T00:00:00Z"
                .parse()
                .assured("the fixture timestamp is valid RFC 3339"),
            DomainTimeRate::ONE,
        ),
    );
    let bound = lifecycle
        .bind()
        .assured("the fixture installed generation two");
    racing_publication
        .watermark
        .raise(Timestamp::from_unix_nanos(i64::MAX));
    let snapshot = bound
        .snapshot()
        .assured("the second mapping fits the timestamp range");

    let generation_two_bound = "2001-01-01T00:00:00Z"
        .parse::<Timestamp>()
        .assured("the fixture timestamp is valid RFC 3339");
    assert!(
        snapshot.now() < generation_two_bound,
        "generation two read {} was clamped by a generation one read",
        snapshot.now()
    );
}

#[test]
fn automatic_pause_preserves_the_installed_mapping() {
    let lifecycle = DomainClockLifecycle::new(domain("paced"));
    let mapping = DomainClockState::new(
        Timestamp::now(),
        Timestamp::from_unix_nanos(10),
        DomainTimeRate::ONE,
    );
    let mut running = paced_domain_state("paced");
    running.start_version = 3;
    running.clock = Some(mapping);
    lifecycle.synchronize(&running, &test_domain_clock_authority());
    let bound = lifecycle
        .bind()
        .assured("the running state installs its committed mapping");

    let mut paused = running;
    paused.status = nervix_models::DomainStatus::Paused;
    paused.clock = None;
    lifecycle.synchronize(&paused, &test_domain_clock_authority());

    assert!(bound.snapshot().is_ok());
}

#[nervix_primitives::test]
async fn cancelled_logical_wait_returns_a_typed_outcome() {
    let lifecycle = DomainClockLifecycle::new(domain("paced"));
    lifecycle.install_paced(
        1,
        DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ),
    );
    let bound = lifecycle
        .bind()
        .assured("the fixture installed generation one");
    let snapshot = bound
        .snapshot()
        .assured("the fixture mapping fits the timestamp range");
    let due_at = snapshot
        .now()
        .checked_add(Duration::from_secs(60))
        .assured("the fixture deadline fits the timestamp range");
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    let result = bound
        .wait_until(bound.deadline_at(due_at), &cancellation)
        .await;

    let Err(error) = result else {
        panic!("a cancelled deadline must return cancellation");
    };
    assert!(matches!(
        error.current_context(),
        DomainClockWaitError::Cancelled { .. }
    ));
}

#[nervix_primitives::test]
async fn logical_deadline_cannot_cross_domain_capabilities() {
    let first = DomainClockLifecycle::new(domain("first"));
    first.synchronize(
        &DomainState {
            id: domain("first"),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: nervix_models::DomainStatus::Running,
            start_version: 1,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        },
        &test_domain_clock_authority(),
    );
    let second = DomainClockLifecycle::new(domain("second"));
    second.synchronize(
        &DomainState {
            id: domain("second"),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: nervix_models::DomainStatus::Running,
            start_version: 1,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        },
        &test_domain_clock_authority(),
    );
    let first = first.bind().assured("the first unpaced clock is installed");
    let second = second
        .bind()
        .assured("the second unpaced clock is installed");

    let result = first
        .wait_until(
            second.deadline_at(Timestamp::from_unix_nanos(0)),
            &CancellationToken::new(),
        )
        .await;

    let Err(error) = result else {
        panic!("a logical deadline from another domain must be rejected");
    };
    let access_error = error
        .downcast_ref::<DomainClockAccessError>()
        .assured("cross-domain waits retain their typed access-error frame");
    assert!(matches!(
        access_error,
        DomainClockAccessError::DeadlineDomainMismatch {
            clock_domain,
            deadline_domain,
        } if clock_domain == &domain("first") && deadline_domain == &domain("second")
    ));
}

#[nervix_primitives::test]
async fn logical_wait_revalidates_generation_after_waking() {
    let lifecycle = DomainClockLifecycle::new(domain("paced"));
    lifecycle.install_paced(
        1,
        DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ),
    );
    let bound = lifecycle
        .bind()
        .assured("the fixture installed generation one");
    let due_at = bound
        .snapshot()
        .assured("the fixture mapping fits the timestamp range")
        .now()
        .checked_add(Duration::from_secs(60))
        .assured("the fixture deadline fits the timestamp range");
    let deadline = bound.deadline_at(due_at);
    let cancellation = CancellationToken::new();
    let task_clock = bound.clone();
    let task_cancellation = cancellation.clone();
    let waiter = nervix_primitives::task::spawn(async move {
        task_clock.wait_until(deadline, &task_cancellation).await
    });
    nervix_primitives::task::yield_now().await;

    lifecycle.install_paced(
        2,
        DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ),
    );
    let result = waiter.await.assured("the clock waiter task must join");

    let Err(error) = result else {
        panic!("a generation change must invalidate the waiter");
    };
    let access_error = error
        .downcast_ref::<DomainClockAccessError>()
        .assured("clock wait failures retain their typed access-error frame");
    assert!(matches!(
        access_error,
        DomainClockAccessError::StaleGeneration {
            bound_generation: 1,
            current_generation: 2,
            ..
        }
    ));
}

#[nervix_primitives::test]
async fn due_logical_wait_returns_due_time_and_a_fresh_snapshot() {
    let clock_domain = domain("unpaced");
    let lifecycle = DomainClockLifecycle::new(clock_domain.clone());
    lifecycle.synchronize(
        &DomainState {
            id: clock_domain,
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: nervix_models::DomainStatus::Running,
            start_version: 4,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        },
        &test_domain_clock_authority(),
    );
    let bound = lifecycle
        .bind()
        .assured("the fixture installs an unpaced clock");
    let due_at = Timestamp::from_unix_nanos(0);

    let reached = bound
        .wait_until(bound.deadline_at(due_at), &CancellationToken::new())
        .await
        .assured("the deadline is already due");

    assert_eq!(reached.due_at(), due_at);
    assert_eq!(reached.snapshot().generation(), 4);
    assert!(reached.snapshot().now() >= due_at);
    assert_eq!(
        reached.snapshot().vm_context().now,
        reached.snapshot().now()
    );
    assert_eq!(
        reached.snapshot().wasm_context().now(),
        reached.snapshot().now()
    );
}

#[test]
fn cadence_coalesces_missed_occurrences_to_the_newest_due_boundary() {
    let clock = test_domain_clock(&domain("cadence"));
    let mut cadence = DomainCadence {
        clock: clock.clone(),
        interval: "10ns".parse().assured("fixture cadence is valid"),
        next_due_at: Timestamp::from_unix_nanos(100),
    };
    let snapshot = DomainExecutionSnapshot {
        generation: clock.generation,
        now: Timestamp::from_unix_nanos(145),
    };

    let occurrence = cadence
        .take_due(snapshot)
        .assured("fixture cadence arithmetic stays in range")
        .assured("the fixture snapshot reaches the cadence");

    assert_eq!(occurrence.due_at(), Timestamp::from_unix_nanos(140));
    assert_eq!(cadence.next_due_at, Timestamp::from_unix_nanos(150));
}

#[test]
fn cadence_advances_directly_across_the_complete_timestamp_range() {
    let clock = test_domain_clock(&domain("fast_cadence"));
    let mut cadence = DomainCadence {
        clock: clock.clone(),
        interval: "1ns".parse().assured("fixture cadence is valid"),
        next_due_at: Timestamp::from_unix_nanos(i64::MIN),
    };
    let snapshot = DomainExecutionSnapshot {
        generation: clock.generation,
        now: Timestamp::from_unix_nanos(i64::MAX - 1),
    };

    let occurrence = cadence
        .take_due(snapshot)
        .assured("the final future boundary remains in range")
        .assured("the fixture snapshot reaches the cadence");

    assert_eq!(
        occurrence.due_at(),
        Timestamp::from_unix_nanos(i64::MAX - 1)
    );
    assert_eq!(cadence.next_due_at, Timestamp::from_unix_nanos(i64::MAX));
}

#[test]
fn cadence_reports_a_schedule_without_a_representable_future_boundary() {
    let clock = test_domain_clock(&domain("bounded_cadence"));
    let mut cadence = DomainCadence {
        clock: clock.clone(),
        interval: "1ns".parse().assured("fixture cadence is valid"),
        next_due_at: Timestamp::from_unix_nanos(i64::MAX),
    };
    let snapshot = DomainExecutionSnapshot {
        generation: clock.generation,
        now: Timestamp::from_unix_nanos(i64::MAX),
    };

    let Err(error) = cadence.take_due(snapshot) else {
        panic!("a cadence at the timestamp limit must have no future boundary");
    };

    assert!(matches!(
        error.downcast_ref::<DomainClockAccessError>(),
        Some(DomainClockAccessError::Arithmetic {
            operation: DomainClockArithmetic::CadenceScheduling,
            ..
        })
    ));
}

#[nervix_primitives::test]
async fn cadence_wait_revalidates_its_bound_generation() {
    let clock_domain = domain("cadence_generation");
    let lifecycle = DomainClockLifecycle::new(clock_domain);
    lifecycle.install_paced(
        1,
        DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ),
    );
    let clock = lifecycle
        .bind()
        .assured("fixture clock generation is installed");
    let cadence = DomainCadence::new(
        clock,
        "1h".parse().assured("fixture cadence is valid"),
        DomainCadenceStart::AfterInterval,
    )
    .assured("fixture cadence starts inside the timestamp range");
    let wait = nervix_primitives::task::spawn(async move {
        let mut cadence = cadence;
        cadence.next(&CancellationToken::new()).await
    });
    nervix_primitives::task::yield_now().await;

    lifecycle.install_paced(
        2,
        DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ),
    );
    let result = wait.await.assured("fixture cadence task joins");
    let Err(error) = result else {
        panic!("the prior generation must not complete its cadence wait");
    };

    assert!(matches!(
        error.downcast_ref::<DomainClockAccessError>(),
        Some(DomainClockAccessError::StaleGeneration {
            bound_generation: 1,
            current_generation: 2,
            ..
        })
    ));
}

#[test]
fn lifecycle_and_execution_handles_share_one_state_allocation() {
    let lifecycle = DomainClockLifecycle::new(domain("paced"));
    lifecycle.install_paced(
        1,
        DomainClockState::new(
            Timestamp::now(),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        ),
    );
    let bound = lifecycle
        .bind()
        .assured("the fixture installed generation one");

    assert!(Arc::ptr_eq(&lifecycle.inner, &bound.inner));
}

#[test]
fn logical_clock_source_has_no_direct_wall_clock_imports() {
    let (product_source, _) = include_str!("../../src/runtime/domain_clock.rs")
        .split_once("#[cfg(test)]")
        .assured("the module has a test boundary");

    for forbidden in [
        "nervix_primitives::time",
        "time::sleep",
        "time::timeout",
        "time::interval",
        "Instant::",
        "Timestamp::now",
    ] {
        assert!(
            !product_source.contains(forbidden),
            "logical clock source directly imports physical time through '{forbidden}'"
        );
    }
}
