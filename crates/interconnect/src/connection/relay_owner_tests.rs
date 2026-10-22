//! Relay owner transition, capacity and epoch-fencing evidence.
//!
//! Layer: test harness.
//! - **Owns.** Attempt lifetime assertions through production peer and admission owners.
//! - **Depends on.** Current relay metadata, bounded permits and the primitive boundary.
//! - **Must not know.** Runtime graphs, serialized live ACKs or external connectors.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation};

use super::*;
use crate::RelayPayloadKind;

struct PeerFixture {
    owner: StdArc<RelayPeerOwner>,
    items: StdArc<Semaphore>,
    terminals: StdArc<Semaphore>,
    executor: Executor,
    observations: Arc<TransportObservations>,
}

struct RetainedAttempt {
    record: StdArc<RelayAdmissionRecord>,
    expected: RelayAdmissionStatus,
}

struct ReservedAttempt {
    record: StdArc<RelayAdmissionRecord>,
    grant: RelayGrant,
}

impl PeerFixture {
    /// A peer that holds `capacity` attempts and twice as many channel watermarks.
    fn new(capacity: usize) -> Self {
        let watermarks = capacity
            .checked_mul(2)
            .assured("fixture capacities are a handful of positions");
        Self::with_capacity(RelayPeerCapacity {
            attempts: capacity,
            watermarks,
        })
    }

    fn with_capacity(capacity: RelayPeerCapacity) -> Self {
        let observations = Arc::new(TransportObservations::default());
        let owner = StdArc::new(RelayPeerOwner::new(capacity, observations.clone()));
        assert!(owner.bind_epoch(101, 1, true));
        Self {
            owner,
            items: StdArc::new(Semaphore::new(capacity.attempts)),
            terminals: StdArc::new(Semaphore::new(capacity.attempts)),
            executor: Executor::default(),
            observations,
        }
    }

    /// The position `sequence` of `channel`, as the fixture's peer in its first epoch names it.
    fn attempt(channel: u8, sequence: u64) -> RelayAttemptKey {
        RelayAttemptKey {
            channel: RelayChannelKey {
                peer_node_id: ClusterNodeName::parse("node-1").assured("the fixture peer is valid"),
                sender_epoch: 101,
                receiver_epoch: 201,
                channel_incarnation: [channel; 16],
            },
            sequence,
        }
    }

    fn reserve(&self, channel: u8, sequence: u64, grant_id: u64, epoch: u64) -> ReservedAttempt {
        let peer = ClusterNodeName::parse("node-1").assured("the fixture peer is valid");
        let registration = RemoteAckRegistration {
            ack_id: grant_id,
            registrar: ClusterNodeIdentity::new(peer.clone(), ClusterNodeIncarnation::new(1)),
        };
        let admission_key = RelayAdmissionKey {
            peer_node_id: peer.clone(),
            registration: registration.clone(),
        };
        let metadata = wire::RelayMetadata {
            kind: RelayPayloadKind::Routed,
            domain: nervix_models::DomainName::parse("test").assured("the fixture domain is valid"),
            relay: nervix_models::RelayName::parse("records").assured("the fixture relay is valid"),
            key: None,
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: Some(registration),
        };
        let record = StdArc::new(RelayAdmissionRecord {
            attempt: RelayAttemptKey {
                channel: RelayChannelKey {
                    peer_node_id: peer,
                    sender_epoch: epoch,
                    receiver_epoch: 201,
                    channel_incarnation: [channel; 16],
                },
                sequence,
            },
            admission_key,
            body_bytes: 1,
            metadata,
            _metadata_memory: self
                .executor
                .try_reserve(MemoryClass::Relay, 256)
                .assured("the fixture's retained metadata fits"),
            choice: AdmissionChoice::new(),
            state: Mutex::new(RelayAdmissionProtocol {
                phase: RelayBodyPhase::Reserved { grant_id },
                rejection: None,
                capacity: Some(RelayAdmissionCapacity {
                    _item: self
                        .items
                        .clone()
                        .try_acquire_owned()
                        .assured("the fixture has a free item"),
                    _terminal: self
                        .terminals
                        .clone()
                        .try_acquire_owned()
                        .assured("the fixture has a free terminal outcome"),
                }),
                last_progress: Instant::now(),
            }),
            cancellation: CancellationToken::new(),
            reserved_at: Instant::now(),
            observations: Arc::new(TransportObservations::default()),
            owner: StdArc::downgrade(&self.owner),
        });
        let grant = RelayGrant {
            expires_at: Instant::now()
                .checked_add(RELAY_GRANT_LIFETIME)
                .assured("the fixture grant fits the clock"),
            reservation: self
                .executor
                .try_reserve(MemoryClass::Relay, 1)
                .assured("one fixture byte fits"),
            admission: record.clone(),
            _expiry: CancelOnDrop::new(CancellationToken::new()),
        };
        ReservedAttempt { record, grant }
    }

    fn register(&self, channel: u8, grant_id: u64) -> StdArc<RelayAdmissionRecord> {
        let ReservedAttempt { record, grant } = self.reserve(channel, 0, grant_id, 101);
        assert!(matches!(
            self.owner.register(grant_id, grant),
            RelayGrantRegistration::Registered
        ));
        record
    }

    /// Admit `channel`'s batch at `sequence` and retire it, as a delivery that completed would.
    fn deliver(&self, channel: u8, sequence: u64, grant_id: u64) {
        let ReservedAttempt { record, grant } = self.reserve(channel, sequence, grant_id, 101);
        assert!(
            matches!(
                self.owner.register(grant_id, grant),
                RelayGrantRegistration::Registered
            ),
            "channel {channel} is admitted at {sequence}"
        );
        let claimed = self
            .owner
            .claim_grant(grant_id, 101, 201, Instant::now())
            .assured("the delivery's body claims its exact grant");
        assert!(record.mark_body_received());
        assert_eq!(
            RelayAdmission {
                record: record.clone()
            }
            .admit(),
            RelayAdmissionDecision::Admitted
        );
        self.owner.retire(&record, RelayAdmissionStatus::Admitted);
        drop(claimed);
    }

    /// How the owner answers a grant request repeated for `channel` at `sequence`.
    fn regrant(&self, channel: u8, sequence: u64, grant_id: u64) -> RelayGrantRegistration {
        let ReservedAttempt { record: _, grant } = self.reserve(channel, sequence, grant_id, 101);
        self.owner.register(grant_id, grant)
    }

    fn evicted(&self) -> u64 {
        self.observations
            .counters()
            .relay_channel_watermarks_evicted
    }
}

#[test]
fn ending_a_peer_releases_capacity_even_when_an_intake_keeps_the_record() {
    let fixture = PeerFixture::new(2);
    let admitted = fixture.register(1, 1);
    let pending = fixture.register(2, 2);
    admitted.mark_admitted();
    fixture.owner.end();
    assert_eq!(fixture.items.available_permits(), 2);
    assert_eq!(fixture.terminals.available_permits(), 2);
    assert_eq!(fixture.owner.snapshot().attempts, 0);
    assert_eq!(fixture.executor.snapshot().relay_memory.reserved_bytes, 512);
    assert_eq!(
        RelayAdmission { record: admitted }.admit(),
        RelayAdmissionDecision::Admitted
    );
    assert_eq!(
        RelayAdmission { record: pending }.admit(),
        RelayAdmissionDecision::Cancelled
    );
    assert!(!fixture.owner.bind_epoch(202, 2, true));
    assert_eq!(fixture.executor.snapshot().relay_memory.reserved_bytes, 0);
}

/// An intake that holds its received batch back before deciding admission learns when the attempt
/// can no longer be admitted: its sender cancels it, or its peer ends.
#[test]
fn a_held_intake_observes_the_cancellation_of_its_attempt() {
    use futures_util::FutureExt as _;

    let fixture = PeerFixture::new(2);
    let withdrawn = fixture.register(1, 1);
    let held = RelayAdmission {
        record: fixture.register(2, 2),
    };
    let withdrawn_intake = RelayAdmission {
        record: withdrawn.clone(),
    };
    assert!(withdrawn_intake.cancelled().now_or_never().is_none());
    assert!(held.cancelled().now_or_never().is_none());

    assert_eq!(withdrawn.cancel(), RelayAdmissionStatus::Cancelled);
    assert!(withdrawn_intake.cancelled().now_or_never().is_some());
    assert!(held.cancelled().now_or_never().is_none());

    fixture.owner.end();
    assert!(held.cancelled().now_or_never().is_some());
    assert_eq!(withdrawn_intake.admit(), RelayAdmissionDecision::Cancelled);
    assert_eq!(held.admit(), RelayAdmissionDecision::Cancelled);
}

#[test]
#[ignore = "native independent-peer cost probe run by bench-remote-owners"]
fn remote_relay_frame_cost_with_another_peers_guard_held() {
    use nervix_primitives::thread;
    use sorted_vec::SortedVec;

    let blocked = PeerFixture::new(1);
    let guard = blocked.owner.state.lock();
    let independent = thread::spawn(move || {
        let fixture = PeerFixture::new(1);
        let mut timings = Vec::with_capacity(5_000);
        let start = Instant::now();
        for sequence in 0..5_000 {
            let frame_start = Instant::now();
            let grant_id = sequence + 1;
            let ReservedAttempt { record, grant } = fixture.reserve(1, sequence, grant_id, 101);
            assert!(matches!(
                fixture.owner.register(grant_id, grant),
                RelayGrantRegistration::Registered
            ));
            let grant = fixture
                .owner
                .claim_grant(grant_id, 101, 201, Instant::now())
                .assured("the exact frame claims its grant");
            assert!(record.mark_body_received());
            assert_eq!(
                RelayAdmission {
                    record: record.clone()
                }
                .admit(),
                RelayAdmissionDecision::Admitted
            );
            fixture
                .owner
                .retire(&record, RelayAdmissionStatus::Admitted);
            drop(grant);
            drop(record);
            assert_eq!(fixture.items.available_permits(), 1);
            assert_eq!(fixture.terminals.available_permits(), 1);
            assert_eq!(fixture.executor.snapshot().relay_memory.reserved_bytes, 0);
            timings.push(frame_start.elapsed().as_nanos());
        }
        let elapsed = start.elapsed();
        let timings = SortedVec::from_unsorted(timings);
        println!(
            "remote-relay-independent-peer blocked_guard_held=true frames={} ns_per_frame={} \
             p50_ns={} p95_ns={} p99_ns={} retained_attempts=0 retained_relay_bytes=0",
            timings.len(),
            elapsed.as_nanos() / u128::try_from(timings.len()).assured("bounded frame count"),
            timings[timings.len() / 2],
            timings[timings.len() * 95 / 100],
            timings[timings.len() * 99 / 100],
        );
        fixture.owner.end();
    });
    independent
        .join()
        .assured("the independent peer completes while the other guard is held");
    drop(guard);
}

#[test]
fn a_late_connection_binding_cannot_restore_a_previous_peer_epoch() {
    let fixture = PeerFixture::new(1);
    let record = fixture.register(1, 1);
    assert!(fixture.owner.bind_epoch(102, 3, true));
    assert!(!fixture.owner.bind_epoch(101, 2, true));
    assert!(fixture.owner.accepts_epoch(102));
    assert!(!fixture.owner.accepts_epoch(101));
    assert_eq!(record.status(), RelayAdmissionStatus::Cancelled);
    assert_eq!(fixture.items.available_permits(), 1);
    assert!(fixture.owner.bind_epoch(102, 1, true));
}

#[test]
fn cancellation_before_a_grant_and_lost_terminal_replies_retain_channel_order() {
    let fixture = PeerFixture::new(2);
    let ReservedAttempt { record, grant } = fixture.reserve(1, 0, 1, 101);
    assert_eq!(
        fixture.owner.control(&record.attempt, true).0,
        RelayAdmissionStatus::Cancelled
    );
    assert!(matches!(
        fixture.owner.register(1, grant),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Cancelled)
    ));
    assert_eq!(fixture.items.available_permits(), 1); // the rejected fixture record still borrows its permit
    drop(record);
    assert_eq!(fixture.items.available_permits(), 2);
    let ReservedAttempt { record, grant } = fixture.reserve(1, 1, 2, 101);
    assert!(matches!(
        fixture.owner.register(2, grant),
        RelayGrantRegistration::Registered
    ));
    let claimed = fixture
        .owner
        .claim_grant(2, 101, 201, Instant::now())
        .assured("the current epoch claims its exact grant");
    drop(claimed);
    assert!(record.mark_body_received());
    assert_eq!(record.progress_registration(), record.metadata.admission);
    record.mark_admitted();
    assert_eq!(
        fixture.owner.control(&record.attempt, true).0,
        RelayAdmissionStatus::Admitted
    );
    fixture
        .owner
        .retire(&record, RelayAdmissionStatus::Admitted);
    assert_eq!(
        fixture.owner.control(&record.attempt, false).0,
        RelayAdmissionStatus::Admitted
    );
    assert_eq!(fixture.items.available_permits(), 2);
    let ReservedAttempt {
        record: duplicate,
        grant,
    } = fixture.reserve(1, 1, 3, 101);
    assert!(matches!(
        fixture.owner.register(3, grant),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Admitted)
    ));
    drop(duplicate);
    let mut superseded = record.attempt.clone();
    superseded.sequence = 0;
    assert_eq!(
        fixture.owner.control(&superseded, false).0,
        RelayAdmissionStatus::Retired
    );
}

#[test]
fn grant_expiry_and_idle_reconciliation_reclaim_every_protocol_collection() {
    let fixture = PeerFixture::new(2);
    let record = fixture.register(1, 1);
    assert!(
        fixture
            .owner
            .claim_grant(1, 102, 201, Instant::now())
            .is_none()
    );
    fixture.owner.expire_grant(1, Instant::now());
    assert_eq!(record.status(), RelayAdmissionStatus::Reserved);
    let expiry = Instant::now()
        .checked_add(RELAY_GRANT_LIFETIME)
        .assured("the fixture expiry fits");
    fixture.owner.expire_grant(1, expiry);
    assert_eq!(record.status(), RelayAdmissionStatus::Cancelled);
    assert_eq!(fixture.owner.snapshot().attempts, 0);
    let record = fixture.register(2, 2);
    let key = OutboundRelayKey {
        peer_node_id: record.admission_key.peer_node_id.clone(),
        delivery: record.attempt.delivery(),
    };
    fixture
        .owner
        .register_outbound(&key, 101, &record.admission_key)
        .assured("the fixture has outbound room");
    assert_eq!(fixture.owner.outbound_epoch(&key), Some(101));
    let expiry = Instant::now()
        .checked_add(RELAY_CHANNEL_RETENTION)
        .assured("the fixture retention fits");
    fixture.owner.sweep(expiry);
    assert_eq!(fixture.owner.snapshot().attempts, 0);
    assert_eq!(fixture.owner.outbound_epoch(&key), None);
    assert_eq!(fixture.items.available_permits(), 2);
}

#[test]
fn discovery_can_bind_before_membership_but_relay_work_waits_for_the_live_run() {
    let capacity = RelayPeerCapacity {
        attempts: 1,
        watermarks: 2,
    };
    let owner = RelayPeerOwner::new(capacity, Arc::new(TransportObservations::default()));
    assert!(owner.bind_epoch(101, 1, false));
    assert!(!owner.accepts_epoch(101));
    assert!(owner.awaiting_membership());
    owner.activate();
    assert!(owner.accepts_epoch(101));
    owner.end();
    owner.activate();
    assert!(!owner.accepts_epoch(101));

    let unobserved = RelayPeerOwner::new(capacity, Arc::new(TransportObservations::default()));
    assert!(unobserved.bind_epoch(102, 2, false));
    let expiry = unobserved
        .created_at
        .checked_add(RELAY_CHANNEL_RETENTION)
        .assured("the fixture membership grace fits");
    unobserved.sweep(expiry);
    assert!(unobserved.closed.is_cancelled());
    assert!(!unobserved.bind_epoch(102, 3, true));
}

#[test]
fn an_outbound_owner_bounds_correlations_and_fences_receiver_replacement() {
    let fixture = PeerFixture::new(1);
    let first = fixture.register(1, 1);
    let key = OutboundRelayKey {
        peer_node_id: first.admission_key.peer_node_id.clone(),
        delivery: first.attempt.delivery(),
    };
    fixture
        .owner
        .register_outbound(&key, 101, &first.admission_key)
        .assured("the first outbound delivery fits");
    let mut second = key.clone();
    second.delivery.channel_incarnation = [2; 16];
    let mut admission = first.admission_key.clone();
    admission.registration.ack_id = 2;
    fixture
        .owner
        .register_outbound(&second, 101, &admission)
        .assured("the next admission has its reserved correlation room");
    let mut third = second.clone();
    third.delivery.channel_incarnation = [3; 16];
    let mut third_admission = admission.clone();
    third_admission.registration.ack_id = 3;
    let error = fixture
        .owner
        .register_outbound(&third, 101, &third_admission)
        .expect_err("a peer's outbound correlation capacity is finite");
    assert!(matches!(
        error.current_context(),
        TransportError::PoolExhausted
    ));
    let error = fixture
        .owner
        .register_outbound(&second, 101, &first.admission_key)
        .expect_err("one admission cannot describe two deliveries");
    assert!(matches!(
        error.current_context(),
        TransportError::RelayGrant(_)
    ));
    assert!(fixture.owner.bind_epoch(102, 2, true));
    let error = fixture
        .owner
        .register_outbound(&key, 102, &first.admission_key)
        .expect_err("retained delivery state cannot cross the receiver's process epoch");
    assert!(matches!(
        error.current_context(),
        TransportError::RelayIndeterminate
    ));
    assert_eq!(fixture.owner.outbound_epoch(&key), None);
    fixture.owner.retire_outbound_admission(&admission);
    assert_eq!(fixture.owner.outbound_epoch(&second), None);
    fixture
        .owner
        .register_outbound(&third, 102, &third_admission)
        .assured("the reclaimed position admits fresh work in the current epoch");
    fixture.owner.end();
    assert_eq!(fixture.owner.outbound_epoch(&third), None);
}

#[test]
fn a_retained_record_cannot_retire_a_replacement_and_progress_renews_its_lifetime() {
    let fixture = PeerFixture::new(1);
    let first = fixture.register(1, 1);
    let terminal = fixture
        .owner
        .completed_admission(
            &first.admission_key,
            &RemoteAckOutcome::NoAck("rejected".to_string()),
        )
        .assured("the rejection names the exact retained admission");
    assert!(StdArc::ptr_eq(&first, &terminal));
    fixture.owner.retire(&first, terminal.status());
    assert_eq!(fixture.items.available_permits(), 1);
    assert!(fixture.owner.bind_epoch(102, 2, true));
    let ReservedAttempt {
        record: current,
        grant,
    } = fixture.reserve(1, 0, 2, 102);
    assert!(matches!(
        fixture.owner.register(2, grant),
        RelayGrantRegistration::Registered
    ));
    fixture.owner.retire(&first, RelayAdmissionStatus::Admitted);
    assert_eq!(fixture.owner.snapshot().attempts, 1);
    assert!(
        fixture
            .owner
            .completed_admission(&current.admission_key, &RemoteAckOutcome::Alive)
            .is_none()
    );
    current.mark_admitted();
    assert_eq!(fixture.owner.progress_registrations().len(), 0);
    let now = Instant::now();
    fixture.owner.sweep(now);
    assert_eq!(fixture.owner.snapshot().attempts, 1);
    let terminal = fixture
        .owner
        .completed_admission(&current.admission_key, &RemoteAckOutcome::Ack)
        .assured("the successful terminal outcome names the current admission");
    assert!(StdArc::ptr_eq(&current, &terminal));
    fixture.owner.retire(&current, terminal.status());
    assert_eq!(fixture.items.available_permits(), 1);
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_a_claimed_body_racing_cancellation_keeps_one_reconcilable_verdict() {
    use nervix_model_harness::shuttle::check_interleavings;
    use nervix_primitives::thread;
    check_interleavings(|| {
        let fixture = PeerFixture::new(1);
        let record = fixture.register(1, 1);
        let claiming = fixture.owner.clone();
        let intake = record.clone();
        let claim_at = record.reserved_at;
        let body = thread::spawn(move || {
            if let Some(grant) = claiming.claim_grant(1, 101, 201, claim_at)
                && intake.mark_body_received()
            {
                let decision = RelayAdmission { record: intake }.admit();
                drop(grant);
                return Some(decision);
            }
            None
        });
        let cancelling = fixture.owner.clone();
        let attempt = record.attempt.clone();
        let cancel = thread::spawn(move || {
            let (status, record) = cancelling.control(&attempt, true);
            if let Some(record) = record {
                cancelling.retire(&record, status.clone());
            }
            status
        });
        let received = body.join().assured("the body claimant joined");
        let cancelled = cancel.join().assured("the cancellation participant joined");
        assert_eq!(record.status(), cancelled);
        if let Some(RelayAdmissionDecision::Admitted) = received {
            assert_eq!(cancelled, RelayAdmissionStatus::Admitted);
        } else {
            assert_eq!(cancelled, RelayAdmissionStatus::Cancelled);
        }
        assert_eq!(fixture.owner.control(&record.attempt, false).0, cancelled);
        assert_eq!(fixture.items.available_permits(), 1);
        assert_eq!(fixture.terminals.available_permits(), 1);
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_another_peer_progresses_while_one_peer_holds_its_protocol_guard() {
    use nervix_model_harness::shuttle::check_interleavings;
    use nervix_primitives::thread;
    check_interleavings(|| {
        let blocked = PeerFixture::new(1);
        let independent = PeerFixture::new(2);
        let record = independent.register(1, 1);
        let other_record = independent.register(2, 2);
        let guard = blocked.owner.state.lock();
        let owner = independent.owner.clone();
        let progress = thread::spawn(move || {
            record.mark_admitted();
            owner.retire(&record, record.status());
        });
        let owner = independent.owner.clone();
        let other_progress = thread::spawn(move || {
            other_record.mark_admitted();
            owner.retire(&other_record, other_record.status());
        });
        progress
            .join()
            .assured("an independent peer needs no guard owned by the blocked peer");
        other_progress
            .join()
            .assured("the other channel progresses under the same blocked-peer guard");
        assert_eq!(independent.items.available_permits(), 2);
        assert_eq!(independent.terminals.available_permits(), 2);
        drop(guard);
    });
}

#[test]
fn bolero_relay_owner_sequences_preserve_verdicts_and_bounded_capacity() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let fixture = PeerFixture::new(2);
            let mut retained = Vec::<RetainedAttempt>::new();
            let mut epoch = 101;
            let mut binding = 1;
            let mut ended = false;
            for (position, byte) in bytes.iter().enumerate() {
                match byte % 7 {
                    0 if !ended && fixture.items.available_permits() > 0 => {
                        let channel = u8::try_from(position)
                            .assured("the operation sequence has at most 64 positions");
                        let grant_id =
                            u64::try_from(position).assured("the fixture position fits in u64") + 1;
                        let ReservedAttempt { record, grant } =
                            fixture.reserve(channel, 0, grant_id, epoch);
                        if matches!(
                            fixture.owner.register(grant_id, grant),
                            RelayGrantRegistration::Registered
                        ) {
                            retained.push(RetainedAttempt {
                                record,
                                expected: RelayAdmissionStatus::Reserved,
                            });
                        }
                    }
                    1..=4 if !retained.is_empty() => {
                        let index = usize::from(byte / 7) % retained.len();
                        let RetainedAttempt { record, expected } = &mut retained[index];
                        let pending = matches!(
                            expected,
                            RelayAdmissionStatus::Reserved | RelayAdmissionStatus::BodyReceived
                        );
                        match byte % 7 {
                            1 => {
                                assert_eq!(
                                    record.mark_body_received(),
                                    *expected == RelayAdmissionStatus::Reserved
                                );
                                if *expected == RelayAdmissionStatus::Reserved {
                                    *expected = RelayAdmissionStatus::BodyReceived;
                                }
                            }
                            2 => {
                                record.mark_admitted();
                                if pending {
                                    *expected = RelayAdmissionStatus::Admitted;
                                }
                            }
                            3 => {
                                record.reject("refused".to_string());
                                if pending {
                                    *expected =
                                        RelayAdmissionStatus::Rejected("refused".to_string());
                                }
                            }
                            _ => {
                                assert_eq!(
                                    record.cancel(),
                                    if pending {
                                        RelayAdmissionStatus::Cancelled
                                    } else {
                                        expected.clone()
                                    }
                                );
                                if pending {
                                    *expected = RelayAdmissionStatus::Cancelled;
                                }
                                fixture.owner.retire(record, expected.clone());
                            }
                        }
                    }
                    5 if !ended => {
                        epoch += 1;
                        binding += 1;
                        assert!(fixture.owner.bind_epoch(epoch, binding, true));
                        for RetainedAttempt { expected, .. } in &mut retained {
                            if matches!(
                                expected,
                                RelayAdmissionStatus::Reserved | RelayAdmissionStatus::BodyReceived
                            ) {
                                *expected = RelayAdmissionStatus::Cancelled;
                            }
                        }
                    }
                    6 => {
                        fixture.owner.end();
                        ended = true;
                        for RetainedAttempt { expected, .. } in &mut retained {
                            if matches!(
                                expected,
                                RelayAdmissionStatus::Reserved | RelayAdmissionStatus::BodyReceived
                            ) {
                                *expected = RelayAdmissionStatus::Cancelled;
                            }
                        }
                    }
                    _ => {}
                }
                for RetainedAttempt { record, expected } in &retained {
                    assert_eq!(&record.status(), expected);
                }
                assert!(fixture.owner.snapshot().attempts <= 2);
                assert!(fixture.owner.state.lock().watermarks.len() <= 4);
            }
            fixture.owner.end();
            assert_eq!(fixture.items.available_permits(), 2);
            assert_eq!(fixture.terminals.available_permits(), 2);
        });
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_peer_retirement_racing_runtime_admission_releases_the_exact_intake() {
    use nervix_model_harness::shuttle::check_interleavings;
    use nervix_primitives::thread;
    check_interleavings(|| {
        let fixture = PeerFixture::new(1);
        let record = fixture.register(1, 1);
        let intake = RelayAdmission {
            record: record.clone(),
        };
        let admit = thread::spawn(move || intake.admit());
        let owner = fixture.owner.clone();
        let end = thread::spawn(move || owner.end());
        let decision = admit.join().assured("the admission participant joined");
        end.join().assured("the peer retirement participant joined");
        assert_eq!(
            record.status(),
            match decision {
                RelayAdmissionDecision::Admitted => RelayAdmissionStatus::Admitted,
                RelayAdmissionDecision::Cancelled => RelayAdmissionStatus::Cancelled,
            }
        );
        assert_eq!(fixture.items.available_permits(), 1);
        assert_eq!(fixture.terminals.available_permits(), 1);
        assert_eq!(fixture.owner.snapshot().attempts, 0);
    });
}

/// A full watermark budget forgets the least recently used watermark, so the peer's next channel is
/// admitted however many of its channels have turned over.
#[test]
fn a_peer_keeps_admitting_new_channels_after_more_than_its_watermark_budget_turned_over() {
    let fixture = PeerFixture::with_capacity(RelayPeerCapacity {
        attempts: 1,
        watermarks: 2,
    });
    for channel in 1..=5 {
        fixture.deliver(channel, 0, u64::from(channel));
    }
    assert_eq!(fixture.owner.snapshot().watermarks, 2);
    assert_eq!(fixture.evicted(), 3);
    assert_eq!(fixture.items.available_permits(), 1);
    assert_eq!(fixture.terminals.available_permits(), 1);
}

/// A delayed or repeated request for a forgotten delivery identity is answered as retired by the
/// grant, status and cancellation paths and is never admitted again, while a retained channel keeps
/// its outcome and a channel opened afterwards is admitted.
#[test]
fn a_forgotten_channel_is_answered_retired_and_never_admitted_again() {
    let fixture = PeerFixture::with_capacity(RelayPeerCapacity {
        attempts: 1,
        watermarks: 2,
    });
    fixture.deliver(1, 0, 1);
    fixture.deliver(2, 0, 2);
    fixture.deliver(3, 0, 3);
    assert!(matches!(
        fixture.regrant(1, 0, 4),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Retired)
    ));
    assert!(matches!(
        fixture.regrant(1, 1, 5),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Retired)
    ));
    assert_eq!(
        fixture.owner.control(&PeerFixture::attempt(1, 0), false).0,
        RelayAdmissionStatus::Retired
    );
    assert_eq!(
        fixture.owner.control(&PeerFixture::attempt(1, 0), true).0,
        RelayAdmissionStatus::Retired
    );
    assert_eq!(
        fixture.owner.control(&PeerFixture::attempt(2, 0), false).0,
        RelayAdmissionStatus::Admitted
    );
    assert!(matches!(
        fixture.regrant(3, 0, 6),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Admitted)
    ));
    fixture.deliver(4, 0, 7);
    assert_eq!(fixture.owner.snapshot().attempts, 0);
}

/// A consultation keeps a watermark: the one recorded or consulted least recently goes first, and
/// a retained channel keeps answering even below the floor that forgetting another one raised.
#[test]
fn the_least_recently_consulted_watermark_is_forgotten_first() {
    let fixture = PeerFixture::with_capacity(RelayPeerCapacity {
        attempts: 1,
        watermarks: 2,
    });
    fixture.deliver(1, 0, 1);
    fixture.deliver(2, 0, 2);
    assert_eq!(
        fixture.owner.control(&PeerFixture::attempt(1, 0), false).0,
        RelayAdmissionStatus::Admitted
    );
    fixture.deliver(3, 0, 3);
    assert_eq!(
        fixture.owner.control(&PeerFixture::attempt(1, 0), false).0,
        RelayAdmissionStatus::Admitted
    );
    assert_eq!(
        fixture.owner.control(&PeerFixture::attempt(2, 0), false).0,
        RelayAdmissionStatus::Retired
    );
}

/// A channel its sender opened before a forgotten one, and that the owner never saw, is refused as
/// a forgotten one is; a channel opened after every forgotten one is admitted.
#[test]
fn the_retired_channel_floor_refuses_an_older_unseen_channel_and_admits_a_newer_one() {
    let fixture = PeerFixture::with_capacity(RelayPeerCapacity {
        attempts: 1,
        watermarks: 2,
    });
    fixture.deliver(5, 0, 1);
    fixture.deliver(6, 0, 2);
    fixture.deliver(7, 0, 3);
    assert!(matches!(
        fixture.regrant(4, 0, 4),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Retired)
    ));
    fixture.deliver(8, 0, 5);
}

/// An attempt still outstanding keeps its channel's watermark through a full budget, reconciles
/// exactly while its channel lies below the floor, and its channel continues once it resolves.
#[test]
fn an_outstanding_attempt_keeps_its_channel_known_below_the_floor() {
    let fixture = PeerFixture::with_capacity(RelayPeerCapacity {
        attempts: 2,
        watermarks: 3,
    });
    fixture.deliver(1, 0, 1);
    let ReservedAttempt { record, grant } = fixture.reserve(1, 1, 2, 101);
    assert!(matches!(
        fixture.owner.register(2, grant),
        RelayGrantRegistration::Registered
    ));
    fixture.deliver(2, 0, 3);
    fixture.deliver(3, 0, 4);
    fixture.deliver(4, 0, 5);
    assert_eq!(fixture.evicted(), 1);
    assert_eq!(
        fixture.owner.control(&PeerFixture::attempt(2, 0), false).0,
        RelayAdmissionStatus::Retired
    );
    assert_eq!(
        fixture.owner.control(&PeerFixture::attempt(1, 0), false).0,
        RelayAdmissionStatus::Admitted
    );
    assert!(matches!(
        fixture.regrant(1, 1, 6),
        RelayGrantRegistration::Existing(_)
    ));
    record.mark_admitted();
    fixture
        .owner
        .retire(&record, RelayAdmissionStatus::Admitted);
    fixture.deliver(1, 2, 7);
}

/// The idle sweep forgets a watermark past its retention under the same floor, and does not count
/// it as one forgotten for room.
#[test]
fn the_sweep_forgets_idle_watermarks_under_the_retired_channel_floor() {
    let fixture = PeerFixture::with_capacity(RelayPeerCapacity {
        attempts: 1,
        watermarks: 4,
    });
    fixture.deliver(1, 0, 1);
    let expiry = Instant::now()
        .checked_add(RELAY_CHANNEL_RETENTION)
        .assured("the fixture retention fits the monotonic clock");
    fixture.owner.sweep(expiry);
    assert_eq!(fixture.owner.snapshot().watermarks, 0);
    assert!(matches!(
        fixture.regrant(1, 0, 2),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Retired)
    ));
    assert_eq!(fixture.evicted(), 0);
    fixture.deliver(2, 0, 3);
}

/// Binding the peer's next process epoch forgets every watermark, and the floor with them.
#[test]
fn a_new_process_epoch_clears_the_retired_channel_floor() {
    let fixture = PeerFixture::with_capacity(RelayPeerCapacity {
        attempts: 1,
        watermarks: 2,
    });
    fixture.deliver(1, 0, 1);
    fixture.deliver(2, 0, 2);
    fixture.deliver(3, 0, 3);
    assert!(fixture.owner.bind_epoch(102, 2, true));
    assert_eq!(fixture.owner.snapshot().watermarks, 0);
    let ReservedAttempt { record: _, grant } = fixture.reserve(1, 0, 4, 102);
    assert!(matches!(
        fixture.owner.register(4, grant),
        RelayGrantRegistration::Registered
    ));
}

/// A cancellation that fences a channel before its grant takes a position from a full budget and
/// is answered as cancelled, and the fence refuses the grant that follows.
#[test]
fn a_cancellation_fence_takes_a_position_from_a_full_watermark_budget() {
    let fixture = PeerFixture::with_capacity(RelayPeerCapacity {
        attempts: 1,
        watermarks: 2,
    });
    fixture.deliver(1, 0, 1);
    fixture.deliver(2, 0, 2);
    assert_eq!(
        fixture.owner.control(&PeerFixture::attempt(3, 0), true).0,
        RelayAdmissionStatus::Cancelled
    );
    assert!(matches!(
        fixture.regrant(3, 0, 3),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Cancelled)
    ));
    assert_eq!(fixture.evicted(), 1);
}

/// One delivery identity of the turnover model: the channel, numbered in the order its sender
/// opened it, and the position on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TurnoverIdentity {
    channel: u8,
    sequence: u64,
}

/// What the turnover model knows of one channel.
struct TurnoverChannel {
    next_sequence: u64,
    outstanding: bool,
    /// The owner answered the channel as forgotten, so its sender opens a new one instead.
    abandoned: bool,
}

struct OutstandingTurnoverAttempt {
    channel: u8,
    record: StdArc<RelayAdmissionRecord>,
}

#[test]
fn bolero_relay_channel_turnover_never_admits_an_identity_twice() {
    const CAPACITY: RelayPeerCapacity = RelayPeerCapacity {
        attempts: 2,
        watermarks: 3,
    };
    bolero::check!()
        .with_iterations(256)
        .with_max_len(96)
        .for_each(|bytes: &[u8]| {
            let fixture = PeerFixture::with_capacity(CAPACITY);
            let mut registered = BTreeSet::<TurnoverIdentity>::new();
            let mut channels = Vec::<TurnoverChannel>::new();
            let mut outstanding = Vec::<OutstandingTurnoverAttempt>::new();
            let mut grant_id = 0_u64;
            for byte in bytes {
                grant_id = grant_id
                    .checked_add(1)
                    .assured("the operation sequence has at most 96 positions");
                let choice = usize::from(byte / 5);
                let room = fixture.items.available_permits() > 0;
                match byte % 5 {
                    // The sender opens a channel after every channel it opened before.
                    0 if room => {
                        let channel = u8::try_from(channels.len() + 1)
                            .assured("the operation sequence opens at most 96 channels");
                        let ReservedAttempt { record, grant } =
                            fixture.reserve(channel, 0, grant_id, 101);
                        assert!(
                            matches!(
                                fixture.owner.register(grant_id, grant),
                                RelayGrantRegistration::Registered
                            ),
                            "channel {channel} opened after every forgotten one is admitted"
                        );
                        assert!(registered.insert(TurnoverIdentity {
                            channel,
                            sequence: 0
                        }));
                        channels.push(TurnoverChannel {
                            next_sequence: 1,
                            outstanding: true,
                            abandoned: false,
                        });
                        outstanding.push(OutstandingTurnoverAttempt { channel, record });
                    }
                    // An outstanding attempt resolves.
                    1 if !outstanding.is_empty() => {
                        let attempt = outstanding.swap_remove(choice % outstanding.len());
                        let status = match choice % 3 {
                            0 => {
                                attempt.record.mark_admitted();
                                RelayAdmissionStatus::Admitted
                            }
                            1 => {
                                attempt.record.reject("refused".to_string());
                                RelayAdmissionStatus::Rejected("refused".to_string())
                            }
                            _ => attempt.record.cancel(),
                        };
                        fixture.owner.retire(&attempt.record, status);
                        let index = usize::from(attempt.channel)
                            .checked_sub(1)
                            .assured("the model numbers channels from one");
                        channels[index].outstanding = false;
                    }
                    // The sender continues a channel that has nothing outstanding.
                    2 if room && !channels.is_empty() => {
                        let index = choice % channels.len();
                        let model = &mut channels[index];
                        if model.outstanding || model.abandoned {
                            continue;
                        }
                        let channel = u8::try_from(index + 1)
                            .assured("the operation sequence opens at most 96 channels");
                        let identity = TurnoverIdentity {
                            channel,
                            sequence: model.next_sequence,
                        };
                        let ReservedAttempt { record, grant } =
                            fixture.reserve(channel, identity.sequence, grant_id, 101);
                        match fixture.owner.register(grant_id, grant) {
                            RelayGrantRegistration::Registered => {
                                assert!(registered.insert(identity), "{identity:?} admitted twice");
                                model.next_sequence = identity
                                    .sequence
                                    .checked_add(1)
                                    .assured("a channel holds at most 96 positions");
                                model.outstanding = true;
                                outstanding.push(OutstandingTurnoverAttempt { channel, record });
                            }
                            RelayGrantRegistration::Retired(RelayGrantDisposition::Retired) => {
                                model.abandoned = true;
                            }
                            _ => panic!(
                                "a continued channel is admitted or answered as forgotten, at \
                                 {identity:?}"
                            ),
                        }
                    }
                    // A delayed or repeated request for an identity the owner registered. The
                    // model holds at most 96 identities, which bounds this lookup.
                    3 if room && !registered.is_empty() => {
                        let identity = *registered
                            .iter()
                            .nth(choice % registered.len())
                            .verified("the index is below the set's length");
                        let ReservedAttempt { record: _, grant } =
                            fixture.reserve(identity.channel, identity.sequence, grant_id, 101);
                        assert!(
                            !matches!(
                                fixture.owner.register(grant_id, grant),
                                RelayGrantRegistration::Registered
                            ),
                            "{identity:?} is never admitted twice"
                        );
                    }
                    // The idle sweep passes the retention of every watermark.
                    4 if outstanding.is_empty() => {
                        let expiry = Instant::now()
                            .checked_add(RELAY_CHANNEL_RETENTION)
                            .assured("the fixture retention fits the monotonic clock");
                        fixture.owner.sweep(expiry);
                    }
                    _ => {}
                }
                assert!(fixture.owner.snapshot().watermarks <= CAPACITY.watermarks);
            }
            for attempt in outstanding {
                let status = attempt.record.cancel();
                fixture.owner.retire(&attempt.record, status);
            }
            fixture.owner.end();
            assert_eq!(fixture.items.available_permits(), CAPACITY.attempts);
            assert_eq!(fixture.terminals.available_permits(), CAPACITY.attempts);
        });
}
