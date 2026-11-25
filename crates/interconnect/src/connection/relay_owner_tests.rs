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
}

struct RetainedAttempt {
    record: StdArc<RelayAdmissionRecord>,
    expected: RelayAdmissionStatus,
}

struct ReservedAttempt {
    record: StdArc<RelayAdmissionRecord>,
    grant: RelayGrant,
}

#[test]
fn a_continued_channel_on_a_fresh_receiver_is_definitely_unopened() {
    let fixture = PeerFixture::new(2);
    assert_eq!(
        fixture
            .owner
            .grant_position(&PeerFixture::attempt(1, 2), default_retention()),
        RelayGrantPosition::Unopened,
        "a restarted receiver cannot wait for earlier positions its process never held"
    );
    assert_eq!(
        fixture
            .owner
            .grant_position(&PeerFixture::attempt(2, 0), default_retention()),
        RelayGrantPosition::Next,
        "the sender can open a new channel at zero"
    );
    fixture.deliver(2, 0, 1);
    assert_eq!(
        fixture
            .owner
            .grant_position(&PeerFixture::attempt(2, 2), default_retention()),
        RelayGrantPosition::OutOfOrder,
        "a retained channel still refuses a skipped position"
    );
    assert_eq!(
        fixture
            .owner
            .grant_position(&PeerFixture::attempt(2, 1), default_retention()),
        RelayGrantPosition::Next,
        "the retained channel takes only its next position"
    );
}

#[test]
fn an_admitted_attempt_keeps_its_channel_open_before_its_outcome_returns() {
    let fixture = PeerFixture::new(2);
    let record = fixture.register(1, 1);
    let _returning = fixture.admit_with_outcome_in_flight(&record, 1);
    assert_eq!(
        fixture
            .owner
            .grant_position(&PeerFixture::attempt(1, 2), default_retention()),
        RelayGrantPosition::OutOfOrder,
        "an admitted attempt keeps the channel known before its watermark is recorded"
    );
    assert_eq!(
        fixture
            .owner
            .grant_position(&PeerFixture::attempt(1, 1), default_retention()),
        RelayGrantPosition::Next,
        "only the immediately following position can retire the retained attempt"
    );
}

/// A validated timing whose total admission budget is 50 milliseconds, so what its deliveries
/// leave behind is retained for 100 milliseconds, far shorter than under the default timing.
fn short_timing() -> nervix_models::RemoteAckTiming {
    let timing = nervix_models::RemoteAckTiming {
        remote_ack_alive_interval: Duration::from_millis(1),
        remote_ack_sweep_interval: Duration::from_millis(1),
        remote_relay_total_timeout: Duration::from_millis(50),
    };
    timing.validate().assured("the short cohort is coherent");
    timing
}

/// The retention of the default timing's deliveries.
fn default_retention() -> RelayChannelRetention {
    RelayChannelRetention::captured_by(nervix_models::RemoteAckTiming::default())
}

/// The instant `retention` after now, when what was recorded before it is past its retention.
fn after(retention: RelayChannelRetention) -> Instant {
    Instant::now()
        .checked_add(retention.duration())
        .assured("a fixture retention fits the monotonic clock")
}

impl PeerFixture {
    /// A peer that holds `capacity` attempts at once.
    fn new(capacity: usize) -> Self {
        Self::with_capacity(RelayPeerCapacity { attempts: capacity })
    }

    fn with_capacity(capacity: RelayPeerCapacity) -> Self {
        let owner = StdArc::new(RelayPeerOwner::new(capacity));
        assert!(owner.bind_epoch(101, 1, true));
        Self {
            owner,
            items: StdArc::new(Semaphore::new(capacity.attempts)),
            terminals: StdArc::new(Semaphore::new(capacity.attempts)),
            executor: Executor::default(),
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
        self.reserve_with_timing(
            channel,
            sequence,
            grant_id,
            epoch,
            nervix_models::RemoteAckTiming::default(),
        )
    }

    fn reserve_with_timing(
        &self,
        channel: u8,
        sequence: u64,
        grant_id: u64,
        epoch: u64,
        timing: nervix_models::RemoteAckTiming,
    ) -> ReservedAttempt {
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
            ack_timing: timing,
            decoded_bytes: 0,
            kind: RelayPayloadKind::Routed,
            domain: nervix_models::DomainName::parse("test").assured("the fixture domain is valid"),
            relay: nervix_models::RelayName::parse("records").assured("the fixture relay is valid"),
            unit: nervix_models::Unit::SINGLE,
            key: nervix_models::RemoteBranchKey::Unbranched,
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
                next_progress_at: Instant::now() + timing.remote_ack_alive_interval,
                progress_reporting: false,
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
                note: Some(self.watermark_charge()),
                last_progress: Instant::now(),
            }),
            cancellation: CancellationToken::new(),
            reserved_at: Instant::now(),
            observations: Arc::new(TransportObservations::default()),
            owner: StdArc::downgrade(&self.owner),
        });
        let grant = RelayGrant {
            expires_at: Instant::now()
                .checked_add(nervix_models::RemoteAckTiming::default().remote_relay_total_timeout)
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
            RelayGrantRegistration::Registered { .. }
        ));
        record
    }

    /// One channel watermark's charge, as a grant reserves it from the fixture's relay memory.
    fn watermark_charge(&self) -> Reservation {
        self.executor
            .try_reserve(
                MemoryClass::Relay,
                RelayChannelWatermarks::watermark_charge(),
            )
            .assured("the fixture's relay memory holds a watermark's charge")
    }

    /// Admit `channel`'s batch at `sequence` and retire it, as a delivery that completed would.
    fn deliver(&self, channel: u8, sequence: u64, grant_id: u64) {
        self.deliver_with_timing(
            channel,
            sequence,
            grant_id,
            nervix_models::RemoteAckTiming::default(),
        );
    }

    /// Admit `channel`'s batch at `sequence`, delivered under `timing`, and retire it.
    fn deliver_with_timing(
        &self,
        channel: u8,
        sequence: u64,
        grant_id: u64,
        timing: nervix_models::RemoteAckTiming,
    ) {
        let ReservedAttempt { record, grant } =
            self.reserve_with_timing(channel, sequence, grant_id, 101, timing);
        assert!(
            matches!(
                self.owner.register(grant_id, grant),
                RelayGrantRegistration::Registered { .. }
            ),
            "channel {channel} is admitted at {sequence}"
        );
        let delivering = self.admit_with_outcome_in_flight(&record, grant_id);
        self.owner
            .retire(&delivering, RelayAdmissionStatus::Admitted);
    }

    /// Claim `record`'s grant, admit its batch and start delivering its terminal outcome, as a
    /// receiver does before that delivery returns. Returns the record the returned delivery
    /// retires.
    fn admit_with_outcome_in_flight(
        &self,
        record: &StdArc<RelayAdmissionRecord>,
        grant_id: u64,
    ) -> StdArc<RelayAdmissionRecord> {
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
        drop(claimed);
        self.owner
            .completed_admission(&record.admission_key, &RemoteAckOutcome::Ack)
            .assured("the terminal outcome names the delivery's exact admission")
    }

    /// How the owner answers a grant request repeated for `channel` at `sequence`, delivered under
    /// the default timing.
    fn regrant(&self, channel: u8, sequence: u64, grant_id: u64) -> RelayGrantRegistration {
        self.regrant_with_timing(
            channel,
            sequence,
            grant_id,
            nervix_models::RemoteAckTiming::default(),
        )
    }

    /// How the owner answers a grant request repeated for `channel` at `sequence`, delivered under
    /// `timing`.
    fn regrant_with_timing(
        &self,
        channel: u8,
        sequence: u64,
        grant_id: u64,
        timing: nervix_models::RemoteAckTiming,
    ) -> RelayGrantRegistration {
        let ReservedAttempt { record: _, grant } =
            self.reserve_with_timing(channel, sequence, grant_id, 101, timing);
        self.owner.register(grant_id, grant)
    }

    /// How the owner answers a status request for `attempt` delivered under the default timing.
    fn status(&self, attempt: &RelayAttemptKey) -> RelayAdmissionStatus {
        self.status_with_timing(attempt, nervix_models::RemoteAckTiming::default())
    }

    /// How the owner answers a status request for `attempt`, whose delivery captured `timing`.
    fn status_with_timing(
        &self,
        attempt: &RelayAttemptKey,
        timing: nervix_models::RemoteAckTiming,
    ) -> RelayAdmissionStatus {
        self.owner
            .control(
                attempt,
                RelayChannelRetention::captured_by(timing),
                RelayControl::Status,
            )
            .status
    }

    /// How the owner answers a cancellation of `attempt`, whose delivery captured `timing`, that
    /// reserved its fence's charge as the receiver's handler does.
    fn cancel_with_timing(
        &self,
        attempt: &RelayAttemptKey,
        timing: nervix_models::RemoteAckTiming,
    ) -> RelayControlAnswer {
        self.owner.control(
            attempt,
            RelayChannelRetention::captured_by(timing),
            RelayControl::Cancel {
                fence: Some(self.watermark_charge()),
            },
        )
    }

    /// How the owner answers a cancellation of `attempt` delivered under the default timing.
    fn cancel(&self, attempt: &RelayAttemptKey) -> RelayAdmissionStatus {
        self.cancel_with_timing(attempt, nervix_models::RemoteAckTiming::default())
            .status
    }

    /// Assert that the relay memory the owner's watermarks are charged for is exactly what their
    /// entries and the rejection reasons they keep take.
    fn assert_watermarks_charged(&self) {
        let state = self.owner.state.lock();
        assert_eq!(
            state.watermarks.charged_bytes(),
            state.watermarks.charge_owed(),
            "the watermarks are charged exactly what they hold"
        );
    }

    /// Sweep the owner at `now` under the default committed retention.
    fn sweep(&self, now: Instant) -> Option<RelayChannelRetention> {
        self.owner.sweep(now, default_retention())
    }

    /// The relay memory the owner's retired-channel floors are charged for, which they keep until
    /// the peer binds another process epoch or ends.
    fn floor_bytes(&self) -> u64 {
        let state = self.owner.state.lock();
        let floors =
            u64::try_from(state.watermarks.floors().len()).assured("a fixture keeps a few floors");
        floors * RelayChannelWatermarks::floor_charge()
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
                RelayGrantRegistration::Registered { .. }
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
            // Only the channel's one watermark stays charged.
            assert_eq!(
                fixture.executor.snapshot().relay_memory.reserved_bytes,
                RelayChannelWatermarks::watermark_charge()
            );
            timings.push(frame_start.elapsed().as_nanos());
        }
        let elapsed = start.elapsed();
        let timings = SortedVec::from_unsorted(timings);
        println!(
            "remote-relay-independent-peer blocked_guard_held=true frames={} ns_per_frame={} \
             p50_ns={} p95_ns={} p99_ns={} retained_attempts=0 retained_relay_bytes={}",
            timings.len(),
            elapsed.as_nanos() / u128::try_from(timings.len()).assured("bounded frame count"),
            timings[timings.len() / 2],
            timings[timings.len() * 95 / 100],
            timings[timings.len() * 99 / 100],
            fixture.executor.snapshot().relay_memory.reserved_bytes,
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
        fixture.cancel(&record.attempt),
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
        RelayGrantRegistration::Registered { .. }
    ));
    let claimed = fixture
        .owner
        .claim_grant(2, 101, 201, Instant::now())
        .assured("the current epoch claims its exact grant");
    drop(claimed);
    assert!(record.mark_body_received());
    let (reports, _) = fixture
        .owner
        .due_progress(Instant::now() + record.metadata.ack_timing.remote_ack_alive_interval);
    assert!(
        record
            .metadata
            .admission
            .as_ref()
            .is_some_and(|registration| reports.contains_key(registration))
    );
    record.mark_admitted();
    assert_eq!(
        fixture.cancel(&record.attempt),
        RelayAdmissionStatus::Admitted
    );
    fixture
        .owner
        .retire(&record, RelayAdmissionStatus::Admitted);
    assert_eq!(
        fixture.status(&record.attempt),
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
    assert_eq!(fixture.status(&superseded), RelayAdmissionStatus::Retired);
}

/// A receiver retires an attempt once the delivery of its terminal outcome returns, but its sender
/// continues the channel as soon as that outcome reaches it, so the grant for the channel's next
/// position can arrive first. That grant takes the position and retires the resolved attempt,
/// whose outcome the channel's watermark keeps, and the late return of the outcome's delivery
/// retires nothing more.
#[test]
fn the_next_grant_takes_its_position_before_the_outcome_of_the_one_before_returns() {
    let fixture = PeerFixture::new(2);
    let first = fixture.register(1, 1);
    let delivering = fixture.admit_with_outcome_in_flight(&first, 1);
    let ReservedAttempt {
        record: second,
        grant,
    } = fixture.reserve(1, 1, 2, 101);
    assert!(
        matches!(
            fixture.owner.register(2, grant),
            RelayGrantRegistration::Registered { .. }
        ),
        "the next grant takes its position while the outcome before it is in flight"
    );
    assert_eq!(fixture.owner.snapshot().attempts, 1);
    assert_eq!(fixture.items.available_permits(), 1);
    assert_eq!(fixture.terminals.available_permits(), 1);
    assert_eq!(
        fixture.status(&first.attempt),
        RelayAdmissionStatus::Admitted
    );

    fixture.owner.retire(&delivering, delivering.status());
    assert_eq!(fixture.owner.snapshot().attempts, 1);
    assert_eq!(fixture.items.available_permits(), 1);
    assert_eq!(
        fixture.status(&first.attempt),
        RelayAdmissionStatus::Admitted
    );

    second.mark_admitted();
    fixture
        .owner
        .retire(&second, RelayAdmissionStatus::Admitted);
    assert_eq!(
        fixture.status(&first.attempt),
        RelayAdmissionStatus::Retired
    );
    fixture.deliver(1, 2, 3);
    assert_eq!(fixture.items.available_permits(), 2);
    assert_eq!(fixture.terminals.available_permits(), 2);
}

/// A cancellation of a channel's next position that arrives before its grant fences that position
/// while the outcome of the attempt before it is still in flight, as it does once that attempt has
/// retired.
#[test]
fn a_cancellation_fences_the_next_position_before_the_outcome_of_the_one_before_returns() {
    let fixture = PeerFixture::new(2);
    let first = fixture.register(1, 1);
    let delivering = fixture.admit_with_outcome_in_flight(&first, 1);
    assert_eq!(
        fixture.cancel(&PeerFixture::attempt(1, 1)),
        RelayAdmissionStatus::Cancelled
    );
    assert_eq!(fixture.owner.snapshot().attempts, 0);
    assert_eq!(fixture.items.available_permits(), 2);

    fixture.owner.retire(&delivering, delivering.status());
    assert!(matches!(
        fixture.regrant(1, 1, 2),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Cancelled)
    ));
    assert_eq!(
        fixture.status(&first.attempt),
        RelayAdmissionStatus::Retired
    );
    fixture.deliver(1, 2, 3);
    assert_eq!(fixture.items.available_permits(), 2);
}

/// Retiring a resolved attempt for its channel's next grant or cancellation records the attempt's
/// outcome as any retirement does: its channel's watermark keeps it, charged to relay memory, on the
/// registration and the cancellation path alike, and no other channel's watermark gives way to it.
#[test]
fn retiring_a_resolved_attempt_for_the_next_position_records_its_outcome() {
    let fixture = PeerFixture::new(2);
    fixture.deliver(1, 0, 1);
    fixture.deliver(2, 0, 2);
    fixture.deliver(3, 0, 3);

    let granted = fixture.register(4, 4);
    let _granted_delivery = fixture.admit_with_outcome_in_flight(&granted, 4);
    let ReservedAttempt { record: _, grant } = fixture.reserve(4, 1, 5, 101);
    assert!(matches!(
        fixture.owner.register(5, grant),
        RelayGrantRegistration::Registered { .. }
    ));
    assert_eq!(
        fixture.status(&PeerFixture::attempt(4, 0)),
        RelayAdmissionStatus::Admitted
    );
    assert_eq!(
        fixture.status(&PeerFixture::attempt(1, 0)),
        RelayAdmissionStatus::Admitted
    );

    let cancelled = fixture.register(5, 6);
    let _cancelled_delivery = fixture.admit_with_outcome_in_flight(&cancelled, 6);
    assert_eq!(
        fixture.cancel(&PeerFixture::attempt(5, 1)),
        RelayAdmissionStatus::Cancelled
    );
    assert_eq!(
        fixture.status(&PeerFixture::attempt(5, 1)),
        RelayAdmissionStatus::Cancelled
    );
    assert_eq!(
        fixture.status(&PeerFixture::attempt(5, 2)),
        RelayAdmissionStatus::Unknown
    );
    assert_eq!(fixture.owner.snapshot().watermarks, 5);
    fixture.assert_watermarks_charged();
}

/// Only the next position retires a resolved attempt. A grant for it while the attempt before is
/// still unadmitted, and a grant that skips a position after a resolved one, are refused and leave
/// the attempt as it was.
#[test]
fn an_unresolved_or_skipped_predecessor_keeps_its_attempt() {
    let fixture = PeerFixture::new(3);
    let first = fixture.register(1, 1);
    assert!(matches!(
        fixture.regrant(1, 1, 2),
        RelayGrantRegistration::InvalidSequence
    ));
    assert_eq!(first.status(), RelayAdmissionStatus::Reserved);
    assert_eq!(fixture.owner.snapshot().attempts, 1);

    let _delivering = fixture.admit_with_outcome_in_flight(&first, 1);
    assert!(matches!(
        fixture.regrant(1, 2, 3),
        RelayGrantRegistration::InvalidSequence
    ));
    assert_eq!(
        fixture.cancel(&PeerFixture::attempt(1, 2)),
        RelayAdmissionStatus::Unknown
    );
    assert_eq!(fixture.owner.snapshot().attempts, 1);
    assert_eq!(
        fixture.status(&first.attempt),
        RelayAdmissionStatus::Admitted
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
    // A grant lives until its delivery's ceiling passes, unless its body arrives first.
    let expiry = Instant::now()
        .checked_add(nervix_models::RemoteAckTiming::default().remote_relay_total_timeout)
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
        .register_outbound(
            &key,
            101,
            &record.admission_key,
            nervix_models::RemoteAckTiming::default(),
        )
        .assured("the fixture has outbound room");
    assert_eq!(fixture.owner.outbound_epoch(&key), Some(101));
    fixture.sweep(after(default_retention()));
    assert_eq!(fixture.owner.snapshot().attempts, 0);
    assert_eq!(fixture.owner.outbound_epoch(&key), None);
    assert_eq!(fixture.items.available_permits(), 2);
}

#[test]
fn discovery_can_bind_before_membership_but_relay_work_waits_for_the_live_run() {
    let capacity = RelayPeerCapacity { attempts: 1 };
    let owner = RelayPeerOwner::new(capacity);
    assert!(owner.bind_epoch(101, 1, false));
    assert!(!owner.accepts_epoch(101));
    assert!(owner.awaiting_membership());
    owner.activate();
    assert!(owner.accepts_epoch(101));
    owner.end();
    owner.activate();
    assert!(!owner.accepts_epoch(101));

    // An owner still awaiting membership ends once the committed retention has passed since it
    // was created, the committed retention whatever it is.
    let committed = RelayChannelRetention::captured_by(short_timing());
    let unobserved = RelayPeerOwner::new(capacity);
    assert!(unobserved.bind_epoch(102, 2, false));
    let before = unobserved
        .created_at
        .checked_add(committed.duration() - Duration::from_nanos(1))
        .assured("the fixture membership grace fits");
    unobserved.sweep(before, committed);
    assert!(!unobserved.closed.is_cancelled());
    let expiry = unobserved
        .created_at
        .checked_add(committed.duration())
        .assured("the fixture membership grace fits");
    unobserved.sweep(expiry, committed);
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
        .register_outbound(
            &key,
            101,
            &first.admission_key,
            nervix_models::RemoteAckTiming::default(),
        )
        .assured("the first outbound delivery fits");
    let mut second = key.clone();
    second.delivery.channel_incarnation = [2; 16];
    let mut admission = first.admission_key.clone();
    admission.registration.ack_id = 2;
    fixture
        .owner
        .register_outbound(
            &second,
            101,
            &admission,
            nervix_models::RemoteAckTiming::default(),
        )
        .assured("the next admission has its reserved correlation room");
    let mut third = second.clone();
    third.delivery.channel_incarnation = [3; 16];
    let mut third_admission = admission.clone();
    third_admission.registration.ack_id = 3;
    let error = fixture
        .owner
        .register_outbound(
            &third,
            101,
            &third_admission,
            nervix_models::RemoteAckTiming::default(),
        )
        .expect_err("a peer's outbound correlation capacity is finite");
    assert!(matches!(
        error.current_context(),
        TransportError::PoolExhausted
    ));
    let error = fixture
        .owner
        .register_outbound(
            &second,
            101,
            &first.admission_key,
            nervix_models::RemoteAckTiming::default(),
        )
        .expect_err("one admission cannot describe two deliveries");
    assert!(matches!(
        error.current_context(),
        TransportError::RelayGrant(_)
    ));
    assert!(fixture.owner.bind_epoch(102, 2, true));
    let error = fixture
        .owner
        .register_outbound(
            &key,
            102,
            &first.admission_key,
            nervix_models::RemoteAckTiming::default(),
        )
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
        .register_outbound(
            &third,
            102,
            &third_admission,
            nervix_models::RemoteAckTiming::default(),
        )
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
        RelayGrantRegistration::Registered { .. }
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
    let (reports, next) = fixture.owner.due_progress(Instant::now());
    assert!(reports.is_empty());
    assert!(
        next.is_none(),
        "an admitted attempt schedules no progress report"
    );
    let now = Instant::now();
    fixture.sweep(now);
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
        let fence = fixture.watermark_charge();
        let cancel = thread::spawn(move || {
            let answer = cancelling.control(
                &attempt,
                default_retention(),
                RelayControl::Cancel { fence: Some(fence) },
            );
            if let Some(record) = answer.record {
                cancelling.retire(&record, answer.status.clone());
            }
            answer.status
        });
        let received = body.join().assured("the body claimant joined");
        let cancelled = cancel.join().assured("the cancellation participant joined");
        assert_eq!(record.status(), cancelled);
        if let Some(RelayAdmissionDecision::Admitted) = received {
            assert_eq!(cancelled, RelayAdmissionStatus::Admitted);
        } else {
            assert_eq!(cancelled, RelayAdmissionStatus::Cancelled);
        }
        assert_eq!(fixture.status(&record.attempt), cancelled);
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
                            RelayGrantRegistration::Registered { .. }
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
                fixture.assert_watermarks_charged();
            }
            fixture.owner.end();
            assert_eq!(fixture.items.available_permits(), 2);
            assert_eq!(fixture.terminals.available_permits(), 2);
            drop(retained);
            assert_eq!(
                fixture.executor.snapshot().relay_memory.reserved_bytes,
                0,
                "an ended owner and its released attempts return all relay memory"
            );
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

/// The return of an admitted attempt's terminal outcome delivery races the grant for its channel's
/// next position, which asks for its place, reserves the peer's only capacity and registers as the
/// receiver's grant path does. In every order the grant takes the next position with the capacity
/// the resolved attempt held, the resolved attempt's outcome stays reconcilable until the next one
/// retires, and every permit returns.
#[cfg(feature = "shuttle")]
#[test]
fn shuttle_the_next_grant_racing_the_outcome_return_before_it_takes_the_next_position() {
    use nervix_model_harness::shuttle::check_interleavings;
    use nervix_primitives::thread;

    /// How the grant for the channel's next position was registered.
    struct NextGrant {
        registration: RelayGrantRegistration,
        record: StdArc<RelayAdmissionRecord>,
    }

    check_interleavings(|| {
        let fixture = StdArc::new(PeerFixture::new(1));
        assert_eq!(
            fixture
                .owner
                .grant_position(&PeerFixture::attempt(1, 1), default_retention()),
            RelayGrantPosition::Unopened,
            "a new receiver asks for a channel opening before any continuation"
        );
        let first = fixture.register(1, 1);
        let delivering = fixture.admit_with_outcome_in_flight(&first, 1);
        let returning = fixture.owner.clone();
        let returned = thread::spawn(move || {
            returning.retire(&delivering, delivering.status());
        });
        let granting = StdArc::clone(&fixture);
        let granted = thread::spawn(move || {
            assert_eq!(
                granting
                    .owner
                    .grant_position(&PeerFixture::attempt(1, 2), default_retention()),
                RelayGrantPosition::OutOfOrder,
                "a skipped position stays out of order before or after the outcome returns"
            );
            assert_eq!(
                granting
                    .owner
                    .grant_position(&PeerFixture::attempt(1, 1), default_retention()),
                RelayGrantPosition::Next,
                "the next grant takes its position whether or not the outcome before it returned"
            );
            let ReservedAttempt { record, grant } = granting.reserve(1, 1, 2, 101);
            let registration = granting.owner.register(2, grant);
            NextGrant {
                registration,
                record,
            }
        });
        returned
            .join()
            .assured("the outcome delivery's return joined");
        let next = granted.join().assured("the next grant joined");
        assert!(matches!(
            next.registration,
            RelayGrantRegistration::Registered { .. }
        ));
        assert_eq!(fixture.owner.snapshot().attempts, 1);
        assert_eq!(
            fixture.status(&first.attempt),
            RelayAdmissionStatus::Admitted
        );
        next.record.mark_admitted();
        fixture
            .owner
            .retire(&next.record, RelayAdmissionStatus::Admitted);
        assert_eq!(
            fixture.status(&first.attempt),
            RelayAdmissionStatus::Retired
        );
        assert_eq!(fixture.items.available_permits(), 1);
        assert_eq!(fixture.terminals.available_permits(), 1);
        assert_eq!(fixture.owner.snapshot().attempts, 0);
    });
}

/// A peer's owner keeps the watermark of every channel it admitted however many channels have
/// turned over, each charged to relay memory, so no channel is forgotten to make room for another.
#[test]
fn a_peer_keeps_every_channel_watermark_however_many_channels_turned_over() {
    let fixture = PeerFixture::new(1);
    for channel in 1..=5 {
        fixture.deliver(channel, 0, u64::from(channel));
    }
    assert_eq!(fixture.owner.snapshot().watermarks, 5);
    for channel in 1..=5 {
        assert_eq!(
            fixture.status(&PeerFixture::attempt(channel, 0)),
            RelayAdmissionStatus::Admitted
        );
    }
    assert_eq!(
        fixture.executor.snapshot().relay_memory.reserved_bytes,
        5 * RelayChannelWatermarks::watermark_charge()
    );
    fixture.assert_watermarks_charged();
    assert_eq!(fixture.items.available_permits(), 1);
    assert_eq!(fixture.terminals.available_permits(), 1);
    fixture.owner.end();
    assert_eq!(fixture.executor.snapshot().relay_memory.reserved_bytes, 0);
}

/// A delivery on a channel whose watermark the owner already retains returns the charge its grant
/// reserved once its outcome is recorded, since the channel's watermark already holds one.
#[test]
fn a_continued_channel_keeps_one_watermark_charge() {
    let fixture = PeerFixture::new(1);
    for sequence in 0..4 {
        fixture.deliver(1, sequence, sequence + 1);
    }
    assert_eq!(fixture.owner.snapshot().watermarks, 1);
    assert_eq!(
        fixture.executor.snapshot().relay_memory.reserved_bytes,
        RelayChannelWatermarks::watermark_charge()
    );
    fixture.assert_watermarks_charged();
}

/// A rejection's reason is charged with the watermark that keeps it, and its charge returns when a
/// later outcome replaces it.
#[test]
fn a_rejection_reason_is_charged_with_its_watermark() {
    let reason = "refused by the receiving runtime";
    let fixture = PeerFixture::new(1);
    let record = fixture.register(1, 1);
    let claimed = fixture
        .owner
        .claim_grant(1, 101, 201, Instant::now())
        .assured("the delivery's body claims its exact grant");
    assert!(record.mark_body_received());
    drop(claimed);
    let rejected = fixture
        .owner
        .completed_admission(
            &record.admission_key,
            &RemoteAckOutcome::NoAck(reason.to_string()),
        )
        .assured("the terminal outcome names the delivery's exact admission");
    fixture.owner.retire(&rejected, rejected.status());
    drop(rejected);
    drop(record);
    let reason_bytes = u64::try_from(reason.len()).assured("a fixture reason length fits u64");
    assert_eq!(
        fixture.executor.snapshot().relay_memory.reserved_bytes,
        RelayChannelWatermarks::watermark_charge() + reason_bytes
    );
    assert_eq!(
        fixture.status(&PeerFixture::attempt(1, 0)),
        RelayAdmissionStatus::Rejected(reason.to_string())
    );
    fixture.assert_watermarks_charged();
    fixture.deliver(1, 1, 2);
    assert_eq!(
        fixture.executor.snapshot().relay_memory.reserved_bytes,
        RelayChannelWatermarks::watermark_charge()
    );
    fixture.assert_watermarks_charged();
}

/// A watermark is forgotten once its own retention has passed: a delayed or repeated request for a
/// forgotten delivery identity is answered as retired by the grant, status and cancellation paths
/// and is never admitted again, while a channel kept for a longer retention keeps its outcome and a
/// channel opened afterwards is admitted.
#[test]
fn a_forgotten_channel_is_answered_retired_and_never_admitted_again() {
    let fixture = PeerFixture::new(1);
    fixture.deliver_with_timing(1, 0, 1, short_timing());
    fixture.deliver(2, 0, 2);
    fixture.deliver(3, 0, 3);
    fixture.sweep(after(RelayChannelRetention::captured_by(short_timing())));
    assert_eq!(fixture.owner.snapshot().watermarks, 2);
    assert!(matches!(
        fixture.regrant_with_timing(1, 0, 4, short_timing()),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Retired)
    ));
    assert!(matches!(
        fixture.regrant_with_timing(1, 1, 5, short_timing()),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Retired)
    ));
    assert_eq!(
        fixture.status_with_timing(&PeerFixture::attempt(1, 0), short_timing()),
        RelayAdmissionStatus::Retired
    );
    assert_eq!(
        fixture
            .cancel_with_timing(&PeerFixture::attempt(1, 0), short_timing())
            .status,
        RelayAdmissionStatus::Retired
    );
    assert_eq!(
        fixture.status(&PeerFixture::attempt(2, 0)),
        RelayAdmissionStatus::Admitted
    );
    assert!(matches!(
        fixture.regrant(3, 0, 6),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Admitted)
    ));
    fixture.deliver(4, 0, 7);
    assert_eq!(fixture.owner.snapshot().attempts, 0);
    fixture.assert_watermarks_charged();
}

/// Each watermark keeps the longest retention among the deliveries it recorded. The sweep forgets
/// one recorded under a short total admission budget once that budget's retention has passed, keeps
/// one recorded under the default budget, and reports the shortest retention it still retains; a
/// later delivery under the short budget leaves a channel's watermark the default retention, and a
/// later delivery under the default budget gives a short channel's watermark the default one.
#[test]
fn each_watermark_keeps_the_longest_retention_its_deliveries_captured() {
    let fixture = PeerFixture::new(1);
    let short = RelayChannelRetention::captured_by(short_timing());
    fixture.deliver_with_timing(1, 0, 1, short_timing());
    fixture.deliver(2, 0, 2);
    assert_eq!(fixture.sweep(Instant::now()), Some(short));
    assert_eq!(fixture.sweep(after(short)), Some(default_retention()));
    assert_eq!(
        fixture.status_with_timing(&PeerFixture::attempt(1, 0), short_timing()),
        RelayAdmissionStatus::Retired
    );
    assert_eq!(
        fixture.status(&PeerFixture::attempt(2, 0)),
        RelayAdmissionStatus::Admitted
    );
    fixture.deliver_with_timing(2, 1, 3, short_timing());
    assert_eq!(fixture.sweep(after(short)), Some(default_retention()));
    assert_eq!(
        fixture.status_with_timing(&PeerFixture::attempt(2, 1), short_timing()),
        RelayAdmissionStatus::Admitted
    );
    fixture.deliver_with_timing(3, 0, 4, short_timing());
    fixture.deliver(3, 1, 5);
    assert_eq!(fixture.sweep(after(short)), Some(default_retention()));
    assert_eq!(
        fixture.status(&PeerFixture::attempt(3, 1)),
        RelayAdmissionStatus::Admitted
    );
    assert_eq!(fixture.sweep(after(default_retention())), None);
    assert_eq!(fixture.owner.snapshot().watermarks, 0);
    assert_eq!(
        fixture.executor.snapshot().relay_memory.reserved_bytes,
        fixture.floor_bytes()
    );
    fixture.assert_watermarks_charged();
}

/// A channel whose deliveries captured different total admission budgets keeps its watermark for
/// the longest of them, so once it is forgotten its floor still refuses a delayed repeat of every
/// delivery it recorded, the one under the longer budget included.
#[test]
fn a_forgotten_channel_refuses_a_repeat_of_any_delivery_it_recorded() {
    let fixture = PeerFixture::new(1);
    fixture.deliver(1, 0, 1);
    fixture.deliver_with_timing(1, 1, 2, short_timing());
    assert_eq!(
        fixture.sweep(after(RelayChannelRetention::captured_by(short_timing()))),
        Some(default_retention())
    );
    assert_eq!(fixture.owner.snapshot().watermarks, 1);
    assert_eq!(fixture.sweep(after(default_retention())), None);
    assert_eq!(fixture.owner.snapshot().watermarks, 0);
    assert!(matches!(
        fixture.regrant(1, 0, 3),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Retired)
    ));
    assert!(matches!(
        fixture.regrant_with_timing(1, 1, 4, short_timing()),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Retired)
    ));
    assert_eq!(
        fixture.status(&PeerFixture::attempt(1, 0)),
        RelayAdmissionStatus::Retired
    );
    fixture.assert_watermarks_charged();
}

/// A channel its sender opened before a forgotten one, and that the owner never saw, is refused as
/// the forgotten one is when its delivery captured no longer a total admission budget than the
/// forgotten channel's deliveries did: it ended long before. One whose delivery captured a longer
/// budget, as a delivery that started before the budget was lowered did, can still be held back by
/// backpressure and is admitted. A channel kept below the floor keeps answering, and a channel
/// opened after every forgotten one is admitted.
#[test]
fn the_retired_channel_floor_refuses_an_older_unseen_channel_only_within_its_retention() {
    let fixture = PeerFixture::new(1);
    fixture.deliver(5, 0, 1);
    fixture.deliver_with_timing(6, 0, 2, short_timing());
    fixture.deliver(7, 0, 3);
    fixture.sweep(after(RelayChannelRetention::captured_by(short_timing())));
    assert!(matches!(
        fixture.regrant_with_timing(4, 0, 4, short_timing()),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Retired)
    ));
    fixture.deliver(3, 0, 5);
    assert_eq!(
        fixture.status(&PeerFixture::attempt(5, 0)),
        RelayAdmissionStatus::Admitted
    );
    fixture.deliver(8, 0, 6);
    fixture.assert_watermarks_charged();
}

/// An attempt still outstanding keeps its channel's watermark past that watermark's retention,
/// reconciles exactly while its channel lies below the floor another channel's expiry raised, and
/// its channel continues once it resolves.
#[test]
fn an_outstanding_attempt_keeps_its_channel_known_below_the_floor() {
    let fixture = PeerFixture::new(2);
    fixture.deliver_with_timing(1, 0, 1, short_timing());
    let ReservedAttempt { record, grant } = fixture.reserve(1, 1, 2, 101);
    assert!(matches!(
        fixture.owner.register(2, grant),
        RelayGrantRegistration::Registered { .. }
    ));
    fixture.deliver_with_timing(2, 0, 3, short_timing());
    fixture.sweep(after(RelayChannelRetention::captured_by(short_timing())));
    assert_eq!(
        fixture.status_with_timing(&PeerFixture::attempt(2, 0), short_timing()),
        RelayAdmissionStatus::Retired
    );
    assert_eq!(
        fixture.status(&PeerFixture::attempt(1, 0)),
        RelayAdmissionStatus::Admitted
    );
    assert!(matches!(
        fixture.regrant(1, 1, 4),
        RelayGrantRegistration::Existing(_)
    ));
    record.mark_admitted();
    fixture
        .owner
        .retire(&record, RelayAdmissionStatus::Admitted);
    fixture.deliver(1, 2, 5);
}

/// The idle sweep forgets a watermark once its retention has passed, under the floor its retention
/// keeps, and returns its charge to relay memory, apart from that floor's.
#[test]
fn the_sweep_forgets_idle_watermarks_under_the_retired_channel_floor() {
    let fixture = PeerFixture::new(1);
    fixture.deliver(1, 0, 1);
    assert_eq!(fixture.sweep(Instant::now()), Some(default_retention()));
    assert_eq!(fixture.sweep(after(default_retention())), None);
    assert_eq!(fixture.owner.snapshot().watermarks, 0);
    assert_eq!(
        fixture.executor.snapshot().relay_memory.reserved_bytes,
        RelayChannelWatermarks::floor_charge()
    );
    assert!(matches!(
        fixture.regrant(1, 0, 2),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Retired)
    ));
    fixture.deliver(2, 0, 3);
}

/// Binding the peer's next process epoch forgets every watermark, the floor with them, and returns
/// their charge.
#[test]
fn a_new_process_epoch_clears_the_retired_channel_floor() {
    let fixture = PeerFixture::new(1);
    fixture.deliver_with_timing(1, 0, 1, short_timing());
    fixture.deliver(2, 0, 2);
    fixture.sweep(after(RelayChannelRetention::captured_by(short_timing())));
    assert!(fixture.owner.bind_epoch(102, 2, true));
    assert_eq!(fixture.owner.snapshot().watermarks, 0);
    assert_eq!(fixture.executor.snapshot().relay_memory.reserved_bytes, 0);
    let ReservedAttempt { record: _, grant } = fixture.reserve(1, 0, 4, 102);
    assert!(matches!(
        fixture.owner.register(4, grant),
        RelayGrantRegistration::Registered { .. }
    ));
}

/// A cancellation that fences a channel's position before its grant records that position for the
/// retention its delivery captured, charged to relay memory, and is answered as cancelled; the fence
/// refuses the grant that follows. Without room for its charge it records nothing and is answered
/// `Unknown`, so its sender asks again.
#[test]
fn a_cancellation_fence_records_its_position_for_the_retention_its_delivery_captured() {
    let fixture = PeerFixture::new(1);
    let unfenced = fixture.owner.control(
        &PeerFixture::attempt(3, 0),
        RelayChannelRetention::captured_by(short_timing()),
        RelayControl::Cancel { fence: None },
    );
    assert_eq!(unfenced.status, RelayAdmissionStatus::Unknown);
    assert_eq!(fixture.owner.snapshot().watermarks, 0);

    let fenced = fixture.cancel_with_timing(&PeerFixture::attempt(3, 0), short_timing());
    assert_eq!(fenced.status, RelayAdmissionStatus::Cancelled);
    assert!(fenced.shortens_sweep);
    assert!(matches!(
        fixture.regrant(3, 0, 1),
        RelayGrantRegistration::Retired(RelayGrantDisposition::Cancelled)
    ));
    fixture.assert_watermarks_charged();
    let short = RelayChannelRetention::captured_by(short_timing());
    assert_eq!(fixture.sweep(Instant::now()), Some(short));
    assert_eq!(fixture.sweep(after(short)), None);
    assert_eq!(
        fixture.executor.snapshot().relay_memory.reserved_bytes,
        RelayChannelWatermarks::floor_charge()
    );
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
    /// The channel's latest attempt has no decided outcome yet.
    undecided: bool,
    /// The owner answered the channel as forgotten, so its sender opens a new one instead.
    abandoned: bool,
}

/// An attempt the turnover model's peer owner retains.
struct RetainedTurnoverAttempt {
    channel: u8,
    record: StdArc<RelayAdmissionRecord>,
}

/// The turnover model: a peer owner that keeps every channel's watermark until that watermark's own
/// retention passes, deliveries that capture a short or the default total admission budget, and
/// what their sender knows of every channel it opened.
struct TurnoverModel {
    fixture: PeerFixture,
    /// Every identity the owner registered, with the timing its delivery captured.
    registered: BTreeMap<TurnoverIdentity, nervix_models::RemoteAckTiming>,
    channels: Vec<TurnoverChannel>,
    /// Attempts whose outcome is not decided yet.
    undecided: Vec<RetainedTurnoverAttempt>,
    /// Attempts whose outcome is decided while the delivery of that outcome has not returned.
    delivering: Vec<RetainedTurnoverAttempt>,
    grant_id: u64,
}

impl TurnoverModel {
    fn capacity() -> RelayPeerCapacity {
        RelayPeerCapacity { attempts: 2 }
    }

    /// The timing a delivery captures: the short cohort for an odd `choice`, the default otherwise.
    fn timing(choice: usize) -> nervix_models::RemoteAckTiming {
        if choice % 2 == 1 {
            short_timing()
        } else {
            nervix_models::RemoteAckTiming::default()
        }
    }

    fn new() -> Self {
        Self {
            fixture: PeerFixture::with_capacity(Self::capacity()),
            registered: BTreeMap::new(),
            channels: Vec::new(),
            undecided: Vec::new(),
            delivering: Vec::new(),
            grant_id: 0,
        }
    }

    fn apply(&mut self, byte: u8) {
        self.grant_id = self
            .grant_id
            .checked_add(1)
            .assured("the operation sequence has at most 96 positions");
        let choice = usize::from(byte / 6);
        match byte % 6 {
            0 => self.open(choice),
            1 => self.decide(choice),
            2 => self.continue_channel(choice),
            3 => self.repeat(choice),
            4 => self.sweep(choice),
            _ => self.return_outcome(choice),
        }
        self.fixture.assert_watermarks_charged();
    }

    fn room(&self) -> bool {
        self.fixture.items.available_permits() > 0
    }

    /// The sender opens a channel after every channel it opened before, for a delivery that
    /// captured the timing `choice` selects.
    fn open(&mut self, choice: usize) {
        if !self.room() {
            return;
        }
        let channel = u8::try_from(self.channels.len() + 1)
            .assured("the operation sequence opens at most 96 channels");
        let timing = Self::timing(choice);
        let ReservedAttempt { record, grant } =
            self.fixture
                .reserve_with_timing(channel, 0, self.grant_id, 101, timing);
        assert!(
            matches!(
                self.fixture.owner.register(self.grant_id, grant),
                RelayGrantRegistration::Registered { .. }
            ),
            "channel {channel} opened after every forgotten one is admitted"
        );
        let identity = TurnoverIdentity {
            channel,
            sequence: 0,
        };
        assert!(self.registered.insert(identity, timing).is_none());
        self.channels.push(TurnoverChannel {
            next_sequence: 1,
            undecided: true,
            abandoned: false,
        });
        self.undecided
            .push(RetainedTurnoverAttempt { channel, record });
    }

    /// An undecided attempt's outcome is decided, and the delivery of that outcome returns at once
    /// or stays in flight.
    fn decide(&mut self, choice: usize) {
        if self.undecided.is_empty() {
            return;
        }
        let attempt = self.undecided.swap_remove(choice % self.undecided.len());
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
        let index = usize::from(attempt.channel)
            .checked_sub(1)
            .assured("the model numbers channels from one");
        self.channels[index].undecided = false;
        if (choice / 3).is_multiple_of(2) {
            self.fixture.owner.retire(&attempt.record, status);
        } else {
            self.delivering.push(attempt);
        }
    }

    /// The sender continues a channel whose latest outcome is decided, in the receiver's grant
    /// order: a forgotten channel is answered first, then the position, the reservation and the
    /// registration. A continuation that captured a longer budget than every delivery a forgotten
    /// channel recorded is answered as never opened, so its sender opens another channel.
    fn continue_channel(&mut self, choice: usize) {
        if self.channels.is_empty() {
            return;
        }
        let index = choice % self.channels.len();
        if self.channels[index].undecided || self.channels[index].abandoned {
            return;
        }
        let channel =
            u8::try_from(index + 1).assured("the operation sequence opens at most 96 channels");
        let identity = TurnoverIdentity {
            channel,
            sequence: self.channels[index].next_sequence,
        };
        let attempt = PeerFixture::attempt(channel, identity.sequence);
        let timing = Self::timing(choice / 2);
        let retention = RelayChannelRetention::captured_by(timing);
        if let Some(status) = self.fixture.owner.retired_status(&attempt, retention) {
            assert_eq!(
                status,
                RelayAdmissionStatus::Retired,
                "a continued channel is answered only as forgotten, at {identity:?}"
            );
            self.channels[index].abandoned = true;
            return;
        }
        let position = self.fixture.owner.grant_position(&attempt, retention);
        if position == RelayGrantPosition::Unopened {
            let forgotten = self
                .fixture
                .owner
                .state
                .lock()
                .watermarks
                .get(&attempt.channel)
                .is_none();
            assert!(
                forgotten,
                "only a channel the owner forgot is answered as unopened, at {identity:?}"
            );
            self.channels[index].abandoned = true;
            return;
        }
        assert_eq!(
            position,
            RelayGrantPosition::Next,
            "a channel whose latest outcome is decided takes its next position, at {identity:?}"
        );
        if !self.room() {
            return;
        }
        let ReservedAttempt { record, grant } = self.fixture.reserve_with_timing(
            channel,
            identity.sequence,
            self.grant_id,
            101,
            timing,
        );
        assert!(
            matches!(
                self.fixture.owner.register(self.grant_id, grant),
                RelayGrantRegistration::Registered { .. }
            ),
            "{identity:?} is registered at the position it was given"
        );
        assert!(
            self.registered.insert(identity, timing).is_none(),
            "{identity:?} admitted twice"
        );
        let model = &mut self.channels[index];
        model.next_sequence = identity
            .sequence
            .checked_add(1)
            .assured("a channel holds at most 96 positions");
        model.undecided = true;
        self.undecided
            .push(RetainedTurnoverAttempt { channel, record });
    }

    /// A delayed or repeated request for an identity the owner registered, carrying the timing its
    /// delivery captured. The model holds at most 96 identities, which bounds this lookup.
    fn repeat(&mut self, choice: usize) {
        if !self.room() || self.registered.is_empty() {
            return;
        }
        let (identity, timing) = self
            .registered
            .iter()
            .nth(choice % self.registered.len())
            .verified("the index is below the map's length");
        let identity = *identity;
        let timing = *timing;
        let ReservedAttempt { record: _, grant } = self.fixture.reserve_with_timing(
            identity.channel,
            identity.sequence,
            self.grant_id,
            101,
            timing,
        );
        assert!(
            !matches!(
                self.fixture.owner.register(self.grant_id, grant),
                RelayGrantRegistration::Registered { .. }
            ),
            "{identity:?} is never admitted twice"
        );
    }

    /// The idle sweep passes the short retention, or the default one for an even `choice`, so it
    /// forgets every watermark and every retained attempt whose outcome is decided that its delivery
    /// keeps for no longer.
    fn sweep(&mut self, choice: usize) {
        if !self.undecided.is_empty() {
            return;
        }
        let retention = RelayChannelRetention::captured_by(Self::timing(choice));
        self.fixture.sweep(after(retention));
    }

    /// The delivery of a decided attempt's outcome returns.
    fn return_outcome(&mut self, choice: usize) {
        if self.delivering.is_empty() {
            return;
        }
        let attempt = self.delivering.swap_remove(choice % self.delivering.len());
        let status = attempt.record.status();
        self.fixture.owner.retire(&attempt.record, status);
    }

    /// Every undecided attempt is cancelled and every outcome delivery returns, and the ended peer
    /// gives back every permit.
    fn finish(self) {
        for attempt in self.undecided {
            let status = attempt.record.cancel();
            self.fixture.owner.retire(&attempt.record, status);
        }
        for attempt in self.delivering {
            let status = attempt.record.status();
            self.fixture.owner.retire(&attempt.record, status);
        }
        self.fixture.owner.end();
        assert_eq!(
            self.fixture.items.available_permits(),
            Self::capacity().attempts
        );
        assert_eq!(
            self.fixture.terminals.available_permits(),
            Self::capacity().attempts
        );
        assert_eq!(
            self.fixture.executor.snapshot().relay_memory.reserved_bytes,
            0,
            "an ended peer returns every watermark's charge"
        );
    }
}

#[test]
fn bolero_relay_channel_turnover_never_admits_an_identity_twice() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(96)
        .for_each(|bytes: &[u8]| {
            let mut model = TurnoverModel::new();
            for byte in bytes {
                model.apply(*byte);
            }
            model.finish();
        });
}

#[test]
fn independent_progress_cohorts_retain_their_send_budgets_and_channel_retention() {
    let fixture = PeerFixture::new(2);
    let timing = short_timing();
    let fast = fixture.reserve_with_timing(1, 0, 1, 101, timing);
    let fast_record = fast.record.clone();
    assert!(matches!(
        fixture.owner.register(1, fast.grant),
        RelayGrantRegistration::Registered {
            shortens_sweep: true
        }
    ));
    let slow = fixture.reserve(2, 0, 2, 101);
    assert!(matches!(
        fixture.owner.register(2, slow.grant),
        RelayGrantRegistration::Registered {
            shortens_sweep: false
        }
    ));
    // Each attempt keeps its own delivery's retention: the sweep honours the shortest of them.
    let short = RelayChannelRetention::captured_by(timing);
    assert_eq!(short.duration(), Duration::from_millis(100));
    assert_eq!(fixture.sweep(Instant::now()), Some(short));
    let now = Instant::now();
    let (fast_reports, _) = fixture.owner.due_progress(now + Duration::from_millis(2));
    assert_eq!(fast_reports.len(), 1);
    // The fast RPC stays in flight while the other cohort becomes due.
    let (slow_reports, _) = fixture.owner.due_progress(now + Duration::from_millis(101));
    assert_eq!(slow_reports.len(), 1);
    let _next = fast_record.finish_progress_report(now + Duration::from_millis(101));
    let (again, _) = fixture.owner.due_progress(now + Duration::from_millis(103));
    assert_eq!(again.len(), 1);
    assert_eq!(again.keys().next(), fast_reports.keys().next());
    fixture.owner.end();
}
