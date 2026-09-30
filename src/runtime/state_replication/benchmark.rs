//! Opaque drivers for measuring runtime-state replication: a Kafka offset commit its replica
//! acknowledges, and the check a replica makes before it installs a branch checkpoint.
//!
//! This module only exists with the `benchmarks` feature. Its public surface deliberately exposes
//! benchmark operations instead of Nervix runtime carriers, placements or stores.

use nervix_models::{ClusterNodeName, DomainName, DomainNodeRef, ModelKind, ModelName, Timestamp};

use super::*;
use crate::runtime_schema::RuntimeValue;

const TOPIC: &str = "orders";

/// A runtime that originates Kafka offsets one replica must hold, and that replicates the branch
/// lifecycle of a deduplicator with a number of branches.
pub struct StateReplicationBenchmark {
    runtime: Runtime,
    offsets: KafkaOffsetStateOriginator,
    offsets_placement: RuntimeStatePlacement,
    replica: ClusterNodeName,
    next_offset: i64,
    branch_checkpoints: Vec<RuntimeStatePlacement>,
}

impl StateReplicationBenchmark {
    /// A runtime whose replica branch lifecycle names `branches` deduplicator branches.
    pub fn new(branches: usize) -> Self {
        let runtime = Runtime::new();
        let domain = DomainName::parse("benchmark").assured("the benchmark domain name is valid");
        let owner = ClusterNodeName::parse("node-1").assured("the benchmark node name is valid");
        let replica = ClusterNodeName::parse("node-2").assured("the benchmark node name is valid");
        let offsets_placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeState::KafkaOffset,
            kind: ModelKind::Ingestor,
            identifier: ModelName::parse("source").assured("the benchmark ingestor name is valid"),
            branch_key: None,
        };
        let mut assignment = runtime
            .replicated_kafka_offset_state(
                offsets_placement.clone(),
                Some(owner.clone()),
                vec![replica.clone()],
                1,
                Some(&owner),
            )
            .assured("an empty Kafka offset state initializes");
        let offsets = assignment
            .originator
            .take()
            .assured("the primary originates the offsets");

        let deduplicator =
            ModelName::parse("dedup").assured("the benchmark deduplicator name is valid");
        runtime.inner.state_identities.insert(
            DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Deduplicator,
                deduplicator.clone(),
            ),
            ScheduledStateIdentity {
                schema_fingerprint: SchemaFingerprint::from_digest([7; 32]),
                wasm_state_generations: None,
            },
        );
        let branch_lru = runtime
            .state_placement(
                &domain,
                RuntimeStateKind::BranchLru,
                ModelKind::Deduplicator,
                deduplicator.clone(),
                None,
            )
            .assured("the published identity places the branch lifecycle");
        let tenant = FieldName::parse("tenant").assured("the benchmark branch field is valid");
        let mut entries = Vec::with_capacity(branches);
        let mut branch_checkpoints = Vec::with_capacity(branches);
        for branch in 0..branches {
            let key = BranchKey::from_fields([(
                tenant.clone(),
                RuntimeValue::String(format!("tenant-{branch}")),
            )])
            .assured("a benchmark branch key names one field");
            branch_checkpoints.push(
                runtime
                    .state_placement(
                        &domain,
                        RuntimeStateKind::Deduplicator,
                        ModelKind::Deduplicator,
                        deduplicator.clone(),
                        Some(key.clone()),
                    )
                    .assured("the published identity places the branch state"),
            );
            entries.push(BranchInstanceSnapshotEntry {
                key: Some(key),
                last_ingestion: Timestamp::from_unix_nanos(1),
                incarnation: 1,
            });
        }
        let lifecycle = PersistedRuntimeStateEntry {
            lsm: 1,
            payload: encode_branch_lru_snapshot(&entries)
                .assured("the benchmark branch lifecycle encodes"),
        };
        runtime
            .install_replica_branch_lru_snapshot(&branch_lru, lifecycle)
            .assured("the benchmark branch lifecycle installs");
        Self {
            runtime,
            offsets,
            offsets_placement,
            replica,
            next_offset: 1,
            branch_checkpoints,
        }
    }

    /// Commit the next offset and deliver the replica's acknowledgement of it once the commit waits
    /// for its replica, as every acknowledged batch of a domain-offset Kafka source does.
    pub async fn commit_acknowledged_offset(&mut self) {
        let lsm = self
            .offsets
            .read()
            .current_lsm()
            .checked_add(1)
            .assured("a benchmark commits far fewer than 2^64 offsets");
        let position = KafkaOffsetPosition {
            topic: TOPIC.to_string(),
            partition: 0,
            offset: self.next_offset,
        };
        let committing = nervix_primitives::task::spawn({
            let runtime = self.runtime.clone();
            let offsets = self.offsets.clone();
            async move { runtime.commit_domain_kafka_offset(&offsets, position).await }
        });
        nervix_primitives::task::yield_now().await;
        self.runtime.handle_state_replication_ack(
            &self.replica,
            StateSyncAck {
                placement: self.offsets_placement.clone(),
                lsm,
            },
        );
        committing
            .await
            .assured("the benchmark commit task does not panic")
            .assured("the replica's acknowledgement completes the commit");
        self.next_offset = self
            .next_offset
            .checked_add(1)
            .assured("a benchmark commits far fewer than 2^63 offsets");
    }

    /// Decide, for each branch checkpoint of the deduplicator, whether this replica's branch
    /// lifecycle names its branch, as installing each of those checkpoints does. Returns how many
    /// it names.
    pub fn check_every_branch_checkpoint(&self) -> usize {
        let mut named = 0_usize;
        for placement in &self.branch_checkpoints {
            let current = self
                .runtime
                .replica_branch_is_current(placement)
                .assured("the benchmark branch lifecycle decodes");
            if current {
                named = named
                    .checked_add(1)
                    .assured("at most one count per benchmark branch");
            }
        }
        named
    }
}
