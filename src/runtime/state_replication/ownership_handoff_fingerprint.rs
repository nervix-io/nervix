//! Layer: data plane.
//! Owns: deterministic schedule fingerprints for ownership handoffs.
//! May depend on: validated execution plans and vocabulary models.
//! Must not know: raw configuration Models, state persistence, or edge protocols.
//!
//! This module breaks its contract: `DomainSchedule` still carries raw node Models. Runtime
//! planning must close that boundary before ownership handoffs consume the schedule.

use super::*;

impl Runtime {
    pub(crate) fn ownership_handoff_schedule_fingerprint(
        schedule: &DomainSchedule,
    ) -> OwnershipHandoffResult<[u8; 32]> {
        #[derive(serde::Serialize)]
        struct ScheduledNodeFingerprint<'a> {
            identifier: &'a ModelName,
            config: &'a Model,
            effective_branching: &'a Option<Vec<FieldName>>,
            effective_branching_schema: &'a Option<SchemaName>,
            schema_fingerprint: [u8; 32],
            kafka_partition_schedule: &'a Option<KafkaPartitionSchedule>,
            primary_node: &'a Option<ClusterNodeName>,
            assigned_nodes: &'a [ClusterNodeName],
        }

        #[derive(serde::Serialize)]
        struct DomainScheduleFingerprint<'a> {
            domain: &'a DomainName,
            nodes: Vec<ScheduledNodeFingerprint<'a>>,
            placement_groups: &'a [nervix_models::PlacementGroupSchedule],
        }

        let nodes = schedule
            .nodes
            .values()
            .map(|node| ScheduledNodeFingerprint {
                identifier: &node.identifier,
                config: node.config.as_ref(),
                effective_branching: &node.effective_branching,
                effective_branching_schema: &node.effective_branching_schema,
                schema_fingerprint: node.schema_fingerprint,
                kafka_partition_schedule: &node.kafka_partition_schedule,
                primary_node: &node.primary_node,
                assigned_nodes: &node.assigned_nodes,
            })
            .collect::<Vec<_>>();
        let fingerprint = DomainScheduleFingerprint {
            domain: &schedule.domain,
            nodes,
            placement_groups: &schedule.placement_groups,
        };
        let encoded = serde_json::to_vec(&fingerprint).map_err(|error| {
            OwnershipHandoffError::schedule(format!(
                "failed to encode ownership handoff schedule: {error}"
            ))
        })?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix/ownership-handoff/domain-schedule");
        hasher.update(&encoded);
        Ok(*hasher.finalize().as_bytes())
    }
}
