//! Channel watermark retention and relay memory charge evidence.
//!
//! Layer: test harness.
//! - **Owns.** Retention, expiry order, retired-channel floor and charge assertions through the
//!   production watermark set.
//! - **Depends on.** The connection's channel identities and the executor's relay memory class.
//! - **Must not know.** Peer owners, grants, runtime graphs or connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_execution::Executor;

use super::*;

/// The fixture's peer channel numbered `channel`, created in that order by its sender.
fn channel(channel: u8) -> RelayChannelKey {
    RelayChannelKey {
        peer_node_id: ClusterNodeName::parse("node-1").assured("the fixture peer is valid"),
        sender_epoch: 101,
        receiver_epoch: 201,
        channel_incarnation: [channel; 16],
    }
}

fn attempt(channel_number: u8, sequence: u64) -> RelayAttemptKey {
    RelayAttemptKey {
        channel: channel(channel_number),
        sequence,
    }
}

fn retention(millis: u64) -> RelayChannelRetention {
    RelayChannelRetention::captured_by(nervix_models::RemoteAckTiming {
        remote_relay_total_timeout: Duration::from_millis(millis) / 2,
        ..nervix_models::RemoteAckTiming::default()
    })
}

fn at(start: Instant, millis: u64) -> Instant {
    start
        .checked_add(Duration::from_millis(millis))
        .assured("a fixture instant fits the monotonic clock")
}

/// One watermark's charge, reserved as a grant reserves it.
fn note(executor: &Executor) -> Reservation {
    executor
        .try_reserve(
            MemoryClass::Relay,
            RelayChannelWatermarks::watermark_charge(),
        )
        .assured("the fixture's relay memory holds a watermark's charge")
}

fn reserved(executor: &Executor) -> u64 {
    executor.snapshot().relay_memory.reserved_bytes
}

/// What `floors` retired-channel floors are charged.
fn floors_charge(floors: u64) -> u64 {
    floors * RelayChannelWatermarks::floor_charge()
}

fn nothing_outstanding(_: &RelayChannelKey) -> bool {
    false
}

/// What a retained watermark is charged stays within the size the chapters document: its two
/// table entries, each with its own channel key, beside the text of a rejection reason it keeps.
#[test]
fn a_retained_watermark_is_charged_its_documented_size() {
    let charge = RelayChannelWatermarks::watermark_charge();
    assert!(
        charge <= 192,
        "a retained watermark is charged {charge} bytes before its reason"
    );
}

/// A watermark holds its charge for as long as it is retained, and returns it once its retention
/// has passed, apart from the charge of the floor that forgetting it raised.
#[test]
fn a_watermark_holds_its_charge_until_its_retention_passes() {
    let executor = Executor::default();
    let mut watermarks = RelayChannelWatermarks::new();
    let start = Instant::now();
    watermarks.record(
        &attempt(1, 0),
        RelayAdmissionStatus::Admitted,
        retention(100),
        Some(note(&executor)),
        start,
    );
    assert_eq!(
        reserved(&executor),
        RelayChannelWatermarks::watermark_charge()
    );
    assert_eq!(watermarks.charged_bytes(), watermarks.charge_owed());
    watermarks.expire(at(start, 99), nothing_outstanding);
    assert_eq!(watermarks.len(), 1);
    watermarks.expire(at(start, 100), nothing_outstanding);
    assert_eq!(watermarks.len(), 0);
    assert_eq!(reserved(&executor), floors_charge(1));
    assert_eq!(watermarks.charged_bytes(), watermarks.charge_owed());
    assert!(watermarks.is_forgotten(&channel(1), retention(100)));
}

/// Each watermark ends at its own retention: one recorded later but kept for less goes first, and
/// the set reports the shortest retention it still keeps.
#[test]
fn each_watermark_ends_at_its_own_retention() {
    let executor = Executor::default();
    let mut watermarks = RelayChannelWatermarks::new();
    let start = Instant::now();
    watermarks.record(
        &attempt(1, 0),
        RelayAdmissionStatus::Admitted,
        retention(1_000),
        Some(note(&executor)),
        start,
    );
    watermarks.record(
        &attempt(2, 0),
        RelayAdmissionStatus::Admitted,
        retention(100),
        Some(note(&executor)),
        at(start, 10),
    );
    assert_eq!(watermarks.shortest_retention(), Some(retention(100)));
    watermarks.expire(at(start, 110), nothing_outstanding);
    assert!(watermarks.get(&channel(1)).is_some());
    assert!(watermarks.get(&channel(2)).is_none());
    assert!(watermarks.is_forgotten(&channel(2), retention(100)));
    assert!(!watermarks.is_forgotten(&channel(1), retention(1_000)));
    assert_eq!(watermarks.shortest_retention(), Some(retention(1_000)));
    assert_eq!(
        reserved(&executor),
        RelayChannelWatermarks::watermark_charge() + floors_charge(1)
    );
    watermarks.expire(at(start, 1_000), nothing_outstanding);
    assert_eq!(watermarks.len(), 0);
    assert_eq!(watermarks.shortest_retention(), None);
    // Channel 2's floor lies above channel 1's, so each retention keeps its own.
    assert_eq!(reserved(&executor), floors_charge(2));
    assert_eq!(watermarks.charged_bytes(), watermarks.charge_owed());
}

/// A consultation starts the watermark's own retention again from that consultation.
#[test]
fn a_consultation_restarts_a_watermarks_own_retention() {
    let executor = Executor::default();
    let mut watermarks = RelayChannelWatermarks::new();
    let start = Instant::now();
    watermarks.record(
        &attempt(1, 0),
        RelayAdmissionStatus::Admitted,
        retention(100),
        Some(note(&executor)),
        start,
    );
    assert!(watermarks.consult(&channel(1), at(start, 50)).is_some());
    watermarks.expire(at(start, 100), nothing_outstanding);
    assert!(watermarks.get(&channel(1)).is_some());
    watermarks.expire(at(start, 150), nothing_outstanding);
    assert!(watermarks.get(&channel(1)).is_none());
    assert_eq!(reserved(&executor), floors_charge(1));
}

/// A later outcome on a channel replaces its watermark without a second charge, and the watermark
/// keeps the longest retention among the deliveries it recorded, from the latest recording; an
/// earlier position never replaces a later one, and a charge given with it returns.
#[test]
fn a_later_outcome_replaces_a_watermark_and_keeps_its_longest_retention() {
    let executor = Executor::default();
    let mut watermarks = RelayChannelWatermarks::new();
    let start = Instant::now();
    watermarks.record(
        &attempt(1, 0),
        RelayAdmissionStatus::Admitted,
        retention(1_000),
        Some(note(&executor)),
        start,
    );
    watermarks.record(
        &attempt(1, 1),
        RelayAdmissionStatus::Cancelled,
        retention(100),
        None,
        at(start, 10),
    );
    watermarks.record(
        &attempt(1, 0),
        RelayAdmissionStatus::Admitted,
        retention(1_000),
        Some(note(&executor)),
        at(start, 20),
    );
    let retained = watermarks
        .get(&channel(1))
        .assured("the channel's watermark is retained");
    assert_eq!(retained.sequence, 1);
    assert_eq!(retained.status, RelayAdmissionStatus::Cancelled);
    assert_eq!(watermarks.shortest_retention(), Some(retention(1_000)));
    assert_eq!(
        reserved(&executor),
        RelayChannelWatermarks::watermark_charge()
    );
    watermarks.expire(at(start, 1_009), nothing_outstanding);
    assert_eq!(watermarks.len(), 1);
    watermarks.expire(at(start, 1_010), nothing_outstanding);
    assert_eq!(watermarks.len(), 0);
    assert_eq!(reserved(&executor), floors_charge(1));

    // A later delivery that captured a longer retention extends a short channel's.
    watermarks.record(
        &attempt(2, 0),
        RelayAdmissionStatus::Admitted,
        retention(100),
        Some(note(&executor)),
        at(start, 1_010),
    );
    watermarks.record(
        &attempt(2, 1),
        RelayAdmissionStatus::Admitted,
        retention(1_000),
        None,
        at(start, 1_020),
    );
    assert_eq!(watermarks.shortest_retention(), Some(retention(1_000)));
    watermarks.expire(at(start, 2_019), nothing_outstanding);
    assert_eq!(watermarks.len(), 1);
    watermarks.clear();
    assert_eq!(reserved(&executor), 0);
}

/// A watermark whose channel has an attempt outstanding stays past its retention, and the expiry
/// order still reaches the watermarks behind it.
#[test]
fn an_outstanding_channel_keeps_its_watermark_past_its_retention() {
    let executor = Executor::default();
    let mut watermarks = RelayChannelWatermarks::new();
    let start = Instant::now();
    watermarks.record(
        &attempt(1, 0),
        RelayAdmissionStatus::Admitted,
        retention(100),
        Some(note(&executor)),
        start,
    );
    watermarks.record(
        &attempt(2, 0),
        RelayAdmissionStatus::Admitted,
        retention(100),
        Some(note(&executor)),
        at(start, 10),
    );
    watermarks.expire(at(start, 200), |key| *key == channel(1));
    assert!(watermarks.get(&channel(1)).is_some());
    assert!(watermarks.get(&channel(2)).is_none());
    assert!(!watermarks.is_forgotten(&channel(1), retention(100)));
    assert_eq!(
        reserved(&executor),
        RelayChannelWatermarks::watermark_charge() + floors_charge(1)
    );
}

/// Forgetting a channel raises the floor of its own retention. A request for a channel the set
/// holds nothing for is refused when it lies at or below that floor and its delivery captured no
/// longer a retention; an older channel whose delivery captured a longer one, and a newer channel,
/// are not.
#[test]
fn a_floor_refuses_only_what_its_retention_covers() {
    let executor = Executor::default();
    let mut watermarks = RelayChannelWatermarks::new();
    let start = Instant::now();
    watermarks.record(
        &attempt(6, 0),
        RelayAdmissionStatus::Admitted,
        retention(100),
        Some(note(&executor)),
        start,
    );
    watermarks.expire(at(start, 100), nothing_outstanding);
    assert!(watermarks.is_forgotten(&channel(6), retention(100)));
    assert!(watermarks.is_forgotten(&channel(5), retention(100)));
    assert!(watermarks.is_forgotten(&channel(5), retention(50)));
    assert!(!watermarks.is_forgotten(&channel(5), retention(1_000)));
    assert!(!watermarks.is_forgotten(&channel(7), retention(100)));
}

/// A floor stays only while no floor of a retention at least as long lies at or above it. Raising
/// a longer retention's floor over a shorter one's removes the shorter, whose charge returns; a
/// channel a longer floor already covers adds none; and a shorter retention's floor above a longer
/// one's stays beside it.
#[test]
fn the_floors_keep_only_what_a_longer_retention_does_not_cover() {
    let executor = Executor::default();
    let mut watermarks = RelayChannelWatermarks::new();
    let start = Instant::now();
    watermarks.record(
        &attempt(2, 0),
        RelayAdmissionStatus::Admitted,
        retention(100),
        Some(note(&executor)),
        start,
    );
    watermarks.record(
        &attempt(3, 0),
        RelayAdmissionStatus::Admitted,
        retention(1_000),
        Some(note(&executor)),
        start,
    );
    watermarks.expire(at(start, 100), nothing_outstanding);
    assert_eq!(watermarks.floors().get(&retention(100)), Some(&[2; 16]));
    watermarks.expire(at(start, 1_000), nothing_outstanding);
    assert_eq!(watermarks.floors().len(), 1);
    assert_eq!(watermarks.floors().get(&retention(1_000)), Some(&[3; 16]));
    assert_eq!(reserved(&executor), floors_charge(1));
    assert!(watermarks.is_forgotten(&channel(2), retention(100)));

    watermarks.record(
        &attempt(1, 0),
        RelayAdmissionStatus::Admitted,
        retention(100),
        Some(note(&executor)),
        at(start, 1_000),
    );
    watermarks.expire(at(start, 1_100), nothing_outstanding);
    assert_eq!(watermarks.floors().len(), 1);
    assert_eq!(reserved(&executor), floors_charge(1));

    watermarks.record(
        &attempt(4, 0),
        RelayAdmissionStatus::Admitted,
        retention(100),
        Some(note(&executor)),
        at(start, 1_100),
    );
    watermarks.expire(at(start, 1_200), nothing_outstanding);
    assert_eq!(watermarks.floors().len(), 2);
    assert!(watermarks.is_forgotten(&channel(4), retention(100)));
    assert!(!watermarks.is_forgotten(&channel(4), retention(1_000)));
    assert!(watermarks.is_forgotten(&channel(3), retention(1_000)));
    assert_eq!(reserved(&executor), floors_charge(2));
    assert_eq!(watermarks.charged_bytes(), watermarks.charge_owed());
    watermarks.clear();
    assert_eq!(reserved(&executor), 0);
}

/// A rejection's reason is charged with its watermark. When the relay memory class has no room
/// for the reason's text, the watermark keeps the rejection without it, and stays charged exactly
/// what it holds.
#[test]
fn a_rejection_reason_is_charged_or_left_out_for_want_of_room() {
    let reason = "refused by the receiving runtime";
    let reason_bytes = u64::try_from(reason.len()).assured("a fixture reason length fits u64");
    let executor = Executor::default();
    let mut watermarks = RelayChannelWatermarks::new();
    let start = Instant::now();
    watermarks.record(
        &attempt(1, 0),
        RelayAdmissionStatus::Rejected(reason.to_string()),
        retention(100),
        Some(note(&executor)),
        start,
    );
    assert_eq!(
        reserved(&executor),
        RelayChannelWatermarks::watermark_charge() + reason_bytes
    );
    assert_eq!(watermarks.charged_bytes(), watermarks.charge_owed());

    let memory = executor.snapshot().relay_memory;
    let charge = note(&executor);
    let filled = executor
        .try_reserve(
            MemoryClass::Relay,
            memory.capacity_bytes - memory.reserved_bytes - charge.bytes(),
        )
        .assured("the rest of the fixture's relay memory can be taken");
    watermarks.record(
        &attempt(2, 0),
        RelayAdmissionStatus::Rejected(reason.to_string()),
        retention(100),
        Some(charge),
        start,
    );
    assert_eq!(
        watermarks
            .get(&channel(2))
            .assured("the rejected channel's watermark is retained")
            .status,
        RelayAdmissionStatus::Rejected(String::new())
    );
    assert_eq!(watermarks.charged_bytes(), watermarks.charge_owed());
    drop(filled);
    watermarks.clear();
    assert_eq!(reserved(&executor), 0);
}
