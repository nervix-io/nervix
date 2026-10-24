use std::time::Duration;

use super::*;

fn record() -> NodePrerequisites {
    NodePrerequisites {
        cluster_identity: "production".to_string(),
        trust_root: TrustRootFingerprint::from_digest([0x11; 32]),
        wire_contract: WireContractFingerprint::from_digest([0x22; 32]),
        consensus_floors: ConsensusFloors {
            largest_command_bytes: 1 << 20,
            largest_management_event_bytes: 64 << 10,
        },
        durability: RaftLogDurability::FsyncEveryAppend,
        timings: PrerequisiteTimings {
            raft_heartbeat_interval: Duration::from_millis(250),
            gossip_interval: Duration::from_millis(500),
            clock_skew_tolerance: Duration::from_millis(500),
        },
    }
}

/// Judges `peer` from `local` and the other way round, and requires the two answers to be the
/// same disagreement seen from either side.
fn judged_both_ways(local: &NodePrerequisites, peer: &NodePrerequisites) -> PrerequisiteMismatch {
    let Err(mismatch) = local.verify_peer(peer) else {
        panic!("the records were expected to disagree");
    };
    let Err(other_side) = peer.verify_peer(local) else {
        panic!("compatibility must be symmetric");
    };
    assert_eq!(other_side, mismatch.clone().reversed());
    assert_eq!(other_side.prerequisite(), mismatch.prerequisite());
    mismatch
}

#[test]
fn equal_records_run_together() {
    assert_eq!(record().verify_peer(&record()), Ok(()));
}

#[test]
fn each_exact_prerequisite_refuses_a_different_value_naming_both() {
    let local = record();

    let mut other_cluster = record();
    other_cluster.cluster_identity = "staging".to_string();
    let mismatch = judged_both_ways(&local, &other_cluster);
    assert_eq!(
        mismatch,
        PrerequisiteMismatch::ClusterIdentity {
            local: "production".to_string(),
            peer: "staging".to_string(),
        }
    );
    assert_eq!(
        mismatch.to_string(),
        "cluster identity differs: this node is in cluster 'production', the peer in 'staging'"
    );

    let mut other_trust = record();
    other_trust.trust_root = TrustRootFingerprint::from_digest([0xab; 32]);
    let mismatch = judged_both_ways(&local, &other_trust);
    assert_eq!(mismatch.prerequisite(), Prerequisite::TrustRoot);
    let rendered = mismatch.to_string();
    assert!(rendered.starts_with("trust root differs: this node trusts 1111"));
    assert!(rendered.ends_with(&format!("the peer {}", "ab".repeat(32))));

    let mut other_contract = record();
    other_contract.wire_contract = WireContractFingerprint::from_digest([0x33; 32]);
    assert_eq!(
        judged_both_ways(&local, &other_contract).prerequisite(),
        Prerequisite::WireContract
    );

    let mut larger_command = record();
    larger_command.consensus_floors.largest_command_bytes = 2 << 20;
    let mismatch = judged_both_ways(&local, &larger_command);
    assert_eq!(
        mismatch,
        PrerequisiteMismatch::LargestCommand {
            local: 1 << 20,
            peer: 2 << 20,
        }
    );
    assert_eq!(
        mismatch.to_string(),
        "largest command differs: this node accepts 1MiB, the peer 2MiB"
    );

    let mut smaller_event = record();
    smaller_event
        .consensus_floors
        .largest_management_event_bytes = 1000;
    let mismatch = judged_both_ways(&local, &smaller_event);
    assert_eq!(
        mismatch.to_string(),
        "largest management event differs: this node accepts 64KiB, the peer 1000B"
    );
}

#[test]
fn the_first_disagreement_in_the_fixed_order_is_reported() {
    let mut peer = record();
    peer.cluster_identity = "staging".to_string();
    peer.timings.raft_heartbeat_interval = Duration::from_secs(10);
    let Err(mismatch) = record().verify_peer(&peer) else {
        panic!("a different cluster identity is refused");
    };
    assert_eq!(mismatch.prerequisite(), Prerequisite::ClusterIdentity);
}

#[test]
fn a_timing_coexists_below_the_factor_and_is_refused_at_it() {
    let local = record();

    let mut within = record();
    within.timings.raft_heartbeat_interval = Duration::from_nanos(499_999_999);
    assert_eq!(local.verify_peer(&within), Ok(()));
    assert_eq!(within.verify_peer(&local), Ok(()));

    let mut faster = record();
    faster.timings.raft_heartbeat_interval = Duration::from_nanos(125_000_001);
    assert_eq!(local.verify_peer(&faster), Ok(()));

    let mut at_factor = record();
    at_factor.timings.raft_heartbeat_interval = Duration::from_millis(500);
    let mismatch = judged_both_ways(&local, &at_factor);
    assert_eq!(
        mismatch,
        PrerequisiteMismatch::Timing {
            prerequisite: TimingPrerequisite::RaftHeartbeatInterval,
            local: Duration::from_millis(250),
            peer: Duration::from_millis(500),
        }
    );
    assert_eq!(
        mismatch.to_string(),
        "raft-heartbeat-interval is 250ms on this node and 500ms on the peer, not within a factor \
         of 2"
    );

    let mut slow_gossip = record();
    slow_gossip.timings.gossip_interval = Duration::from_millis(250);
    assert_eq!(
        judged_both_ways(&local, &slow_gossip).prerequisite(),
        Prerequisite::GossipInterval
    );

    let mut loose_skew = record();
    loose_skew.timings.clock_skew_tolerance = Duration::from_secs(5);
    let mismatch = judged_both_ways(&local, &loose_skew);
    assert_eq!(mismatch.prerequisite(), Prerequisite::ClockSkewTolerance);
    assert!(
        mismatch
            .to_string()
            .starts_with("clock-skew-tolerance is 500ms")
    );
}

#[test]
fn equal_timings_coexist_whatever_their_value() {
    assert!(timings_coexist(Duration::ZERO, Duration::ZERO));
    assert!(timings_coexist(Duration::MAX, Duration::MAX));
    assert!(!timings_coexist(Duration::ZERO, Duration::from_nanos(1)));
    assert!(timings_coexist(
        Duration::MAX,
        Duration::from_secs(u64::MAX / 2 + 1)
    ));
}

#[test]
fn reversing_twice_restores_the_disagreement() {
    let mismatch = PrerequisiteMismatch::Timing {
        prerequisite: TimingPrerequisite::GossipInterval,
        local: Duration::from_secs(1),
        peer: Duration::from_secs(3),
    };
    assert_eq!(mismatch.clone().reversed().reversed(), mismatch);
}

#[test]
fn differing_lists_every_timing_that_differs_compatible_or_not() {
    let local = record().timings;
    let mut other = local;
    assert!(local.differing(&other).is_empty());
    other.raft_heartbeat_interval = Duration::from_millis(300);
    other.clock_skew_tolerance = Duration::from_secs(10);
    assert_eq!(
        local.differing(&other),
        vec![
            TimingPrerequisite::RaftHeartbeatInterval,
            TimingPrerequisite::ClockSkewTolerance,
        ]
    );
}

#[test]
fn a_timing_prerequisite_is_named_by_its_setting() {
    assert_eq!(
        TimingPrerequisite::RaftHeartbeatInterval.setting_name(),
        "raft-heartbeat-interval"
    );
    assert_eq!(
        Prerequisite::from(TimingPrerequisite::GossipInterval).to_string(),
        "gossip-interval"
    );
    assert_eq!(
        Prerequisite::LargestManagementEvent.as_ref(),
        "largest_management_event"
    );
    assert_eq!(Prerequisite::Durability.to_string(), "durability promise");
}

#[test]
fn the_gossip_text_reads_back_as_the_same_record() {
    let original = record();
    let text = original.to_gossip_text();
    assert!(text.contains(&"11".repeat(32)));
    let read = NodePrerequisites::from_gossip_text(&text).assured("the record was just written");
    assert_eq!(read, original);
}

#[test]
fn gossip_text_that_holds_no_record_is_refused() {
    for text in [
        "",
        "{}",
        "not json",
        &record().to_gossip_text().replace(&"22".repeat(32), "22"),
    ] {
        assert!(
            NodePrerequisites::from_gossip_text(text).is_err(),
            "unexpectedly read a record from {text:?}"
        );
    }
}

#[test]
fn a_fingerprint_reads_back_from_its_text_and_refuses_other_text() {
    let fingerprint = TrustRootFingerprint::from_digest([0x5a; 32]);
    let text = fingerprint.to_string();
    assert_eq!(text, "5a".repeat(32));
    assert_eq!(text.parse::<TrustRootFingerprint>(), Ok(fingerprint));
    assert_eq!(
        "5A".repeat(32).parse::<TrustRootFingerprint>(),
        Ok(fingerprint)
    );
    assert_eq!(
        "5a".parse::<WireContractFingerprint>(),
        Err(FingerprintTextError::Length { length: 2 })
    );
    assert_eq!(
        "zz".repeat(32).parse::<WireContractFingerprint>(),
        Err(FingerprintTextError::NotHexadecimal)
    );
    assert_eq!(
        format!("{:?}", WireContractFingerprint::from_digest([0; 32])),
        format!("WireContractFingerprint({})", "00".repeat(32))
    );
}

#[test]
fn a_record_describes_every_prerequisite_in_the_order_it_is_compared() {
    let described = record().described();
    let names = described
        .iter()
        .map(|entry| entry.prerequisite.to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "cluster identity",
            "trust root",
            "wire contract",
            "largest command",
            "largest management event",
            "durability promise",
            "raft-heartbeat-interval",
            "gossip-interval",
            "clock-skew-tolerance",
        ]
    );
    let values = described
        .iter()
        .map(|entry| entry.value.as_str())
        .collect::<Vec<_>>();
    assert_eq!(values[0], "production");
    assert_eq!(values[3], "1MiB");
    assert_eq!(values[4], "64KiB");
    assert_eq!(values[5], "fsync every append");
    assert_eq!(values[6], "250ms");
    assert_eq!(timing_text(Duration::from_millis(1500)), "1s 500ms");
}
