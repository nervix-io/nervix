//! Where a restore stages one domain's archived per-key state when a branch's partitions divide
//! its entities: in the unit the restored partition count places each key in.
//!
//! Layer: control plane.
//! - **Owns.** Recomputing, under the restored schedule, the unit of each archived per-key section
//!   and branch lifecycle entry of an entity a branch's partitions divide; choosing which copy of a
//!   key archived in two units the restore keeps; and the lifecycle each unit restores.
//! - **Depends on.** The verified archive description, the restored schedule's units and branch
//!   partitions, the runtime's typed branch key and the bounded executor.
//! - **Must not know.** Checkpoint encoding, staging, placement or publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::BTreeMap;

use ahash::{HashSet, HashSetExt as _};
use error_stack::{Report, ResultExt as _};
use indexmap::IndexMap;
use nervix_backup::{BranchLifecycleRecord, DescribedRuntimeState};
use nervix_execution::{CpuClass, Executor, MemoryClass};
use nervix_models::{
    BranchKeyFingerprint, DomainSchedule, ModelKind, NodeRef, ScheduledNode, SchemaFingerprint,
    Unit,
};
use nervix_primitives::sync::Arc;
use thiserror::Error;

use super::steps::remote_branch_key;
use crate::runtime::{
    BackupBranchLifecycleEntry, BranchKey, RESTORE_STATE_WORKING_BYTES, Runtime,
    SnapshotStagingError, StagedArtifact, write_restored_branch_lifecycle,
};

/// Why archived per-key state could not be placed under the restored partition count.
#[derive(Debug, Error)]
pub(in crate::application) enum RestoredPartitionError {
    #[error("regrouping the archived branch lifecycles could not be admitted")]
    Admission,
    #[error("an archived branch lifecycle entry of '{entity}' is not a valid branch key")]
    LifecycleKey { entity: nervix_models::ModelName },
    #[error("an archived branch lifecycle entry of '{entity}' names no unit the restore schedules")]
    LifecycleUnit { entity: nervix_models::ModelName },
    #[error("the regrouped branch lifecycle of '{entity}' could not be staged")]
    Staging { entity: nervix_models::ModelName },
}

/// One key of one entity: what two archived copies of the same key's state share.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EntityKey {
    entity: NodeRef,
    key: BranchKeyFingerprint,
}

/// The identity of one archived per-key section.
struct PerKeySection<'a> {
    entity: NodeRef,
    unit: Unit,
    key: Option<&'a BranchKeyFingerprint>,
}

impl<'a> PerKeySection<'a> {
    fn of(state: &'a DescribedRuntimeState) -> Option<Self> {
        match state {
            DescribedRuntimeState::Wasm { descriptor, .. } => Some(Self {
                entity: NodeRef::new(ModelKind::WasmProcessor, descriptor.entity.clone()),
                unit: descriptor.unit,
                key: descriptor.branch_fingerprint.as_ref(),
            }),
            DescribedRuntimeState::Deduplicator { descriptor, .. } => Some(Self {
                entity: NodeRef::new(ModelKind::Deduplicator, descriptor.entity.clone()),
                unit: descriptor.unit,
                key: descriptor.branch_fingerprint.as_ref(),
            }),
            DescribedRuntimeState::Window { descriptor, .. } => Some(Self {
                entity: NodeRef::new(ModelKind::WindowProcessor, descriptor.entity.clone()),
                unit: descriptor.unit,
                key: descriptor.branch_fingerprint.as_ref(),
            }),
            DescribedRuntimeState::KafkaOffsets { .. }
            | DescribedRuntimeState::BranchLifecycle { .. }
            | DescribedRuntimeState::Materialized { .. } => None,
        }
    }
}

/// How a restore places one domain's archived per-key state under its restored partition counts.
///
/// A backup cut while a branch's keys move between units may hold one key's state twice: in the
/// unit the count current at the cut places it in, where its claim installed it, and in the unit
/// that released it and had not forgotten it yet. The restored count is the count current at the
/// cut, and the copy in the key's own unit is the newer one, since the released copy stopped
/// changing when it was released: that copy is the one restored.
pub(in crate::application) struct RestoredPartitions<'a> {
    schedule: &'a DomainSchedule,
    /// Every key whose state the archive holds in the unit the restored count places it in.
    placed: HashSet<EntityKey>,
}

impl<'a> RestoredPartitions<'a> {
    /// The placement of `states`, one domain's archived state, under `schedule`.
    pub(in crate::application) fn of(
        schedule: &'a DomainSchedule,
        states: &[DescribedRuntimeState],
    ) -> Self {
        let mut placed = HashSet::new();
        for state in states {
            let Some(section) = PerKeySection::of(state) else {
                continue;
            };
            let Some(node) = schedule.nodes.get(&section.entity) else {
                continue;
            };
            let Some(key) = section.key else {
                continue;
            };
            if node.partition_branch().is_none() {
                continue;
            }
            if node.unit_of_branch(Some(key)) == Some(section.unit) {
                placed.insert(EntityKey {
                    entity: section.entity,
                    key: *key,
                });
            }
        }
        Self { schedule, placed }
    }

    /// The restored node of `entity` when a branch's partitions divide it, so the units its state
    /// was archived in give way to the units the restored count places its keys in.
    pub(in crate::application) fn divided(&self, entity: &NodeRef) -> Option<&'a ScheduledNode> {
        let node = self.schedule.nodes.get(entity)?;
        node.partition_branch()?;
        Some(node)
    }

    /// The unit a per-key section of `entity`, archived in `archived` for `key`, is restored in:
    /// none when the restored schedule places the key in no unit, or a copy of the key's state
    /// archived in the key's own unit supersedes this one.
    pub(in crate::application) fn section_unit(
        &self,
        entity: &NodeRef,
        archived: Unit,
        key: Option<&BranchKeyFingerprint>,
    ) -> Option<Unit> {
        let Some(node) = self.divided(entity) else {
            return Some(archived);
        };
        let unit = node.unit_of_branch(key)?;
        if unit == archived {
            return Some(unit);
        }
        let Some(key) = key else {
            return Some(unit);
        };
        let superseded = self.placed.contains(&EntityKey {
            entity: entity.clone(),
            key: *key,
        });
        if superseded { None } else { Some(unit) }
    }
}

/// The branch lifecycle a restore installs for one unit of an entity a branch's partitions divide.
#[derive(Debug, Clone)]
pub(in crate::application) struct RegroupedLifecycle {
    pub(in crate::application) unit: Unit,
    pub(in crate::application) schema: SchemaFingerprint,
    /// The newest revision any archived lifecycle of the entity had. Each archived lifecycle's
    /// revision is at least the incarnation of every entry it holds, so no restored unit issues an
    /// incarnation a restored branch already has.
    pub(in crate::application) revision: u64,
    pub(in crate::application) entries: Vec<BackupBranchLifecycleEntry>,
}

/// One archived lifecycle entry on its way to the unit that restores it.
struct PlacedEntry {
    entry: BackupBranchLifecycleEntry,
    /// Whether the entry was archived in the unit the restored count places its key in.
    in_own_unit: bool,
}

impl RegroupedLifecycle {
    /// Stages this lifecycle of `entity` as its unit's native checkpoint, within `maximum` bytes.
    pub(in crate::application) async fn stage(
        &self,
        runtime: &Runtime,
        entity: &nervix_models::ModelName,
        maximum: u64,
    ) -> Result<Arc<StagedArtifact>, Report<RestoredPartitionError>> {
        let staging = || RestoredPartitionError::Staging {
            entity: entity.clone(),
        };
        let charge = runtime
            .executor()
            .reserve(MemoryClass::Bulk, RESTORE_STATE_WORKING_BYTES)
            .await
            .change_context(RestoredPartitionError::Admission)?;
        let writer = runtime
            .stage_artifact(maximum)
            .await
            .change_context_lazy(staging)?;
        let entries = self.entries.clone();
        let owner = entity.clone();
        let artifact = writer
            .encode_artifact(charge, move |output, cancellation| {
                write_restored_branch_lifecycle(entries.into_iter(), &owner, output, cancellation)
                    .change_context(SnapshotStagingError::Encode)
            })
            .await
            .change_context_lazy(staging)?;
        Ok(Arc::new(artifact))
    }

    /// Regroups `lifecycles`, every archived lifecycle of `node`, a node a branch's partitions
    /// divide, by the unit the restored count places each entry's key in. A key archived in two
    /// units keeps the entry of its own unit, or else the entry it was last active in. The work
    /// runs as one admitted job off the async workers, bounded by the entries the archive holds.
    pub(in crate::application) async fn regroup(
        executor: &Executor,
        node: &ScheduledNode,
        lifecycles: Vec<BranchLifecycleRecord>,
    ) -> Result<Vec<Self>, Report<RestoredPartitionError>> {
        let Some(first) = lifecycles.first() else {
            return Ok(Vec::new());
        };
        let entity = first.entity.clone();
        let schema = first.schema;
        let node = node.clone();
        let charge = executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(RestoredPartitionError::Admission)?;
        executor
            .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                let mut revision = 0_u64;
                let mut units =
                    BTreeMap::<Unit, IndexMap<BranchKeyFingerprint, PlacedEntry>>::new();
                for lifecycle in &lifecycles {
                    revision = revision.max(lifecycle.revision);
                    for archived in &lifecycle.branches {
                        cancellation
                            .check()
                            .change_context(RestoredPartitionError::Admission)?;
                        let remote = remote_branch_key(archived.key.as_ref());
                        let key = BranchKey::from_remote_key(remote.clone()).change_context_lazy(
                            || RestoredPartitionError::LifecycleKey {
                                entity: entity.clone(),
                            },
                        )?;
                        // A branch's partitions divide only concrete keys.
                        let Some(key) = key else {
                            return Err(Report::new(RestoredPartitionError::LifecycleKey {
                                entity: entity.clone(),
                            }));
                        };
                        let fingerprint = key.fingerprint();
                        let Some(unit) = node.unit_of_branch(Some(&fingerprint)) else {
                            return Err(Report::new(RestoredPartitionError::LifecycleUnit {
                                entity: entity.clone(),
                            }));
                        };
                        let placed = PlacedEntry {
                            entry: BackupBranchLifecycleEntry {
                                key: remote,
                                last_ingestion: archived.last_ingestion,
                                incarnation: archived.incarnation,
                            },
                            in_own_unit: unit == lifecycle.unit,
                        };
                        let entries = units.entry(unit).or_default();
                        if let Some(present) = entries.get(&fingerprint) {
                            let present_wins = present.in_own_unit
                                || (!placed.in_own_unit
                                    && present.entry.last_ingestion >= placed.entry.last_ingestion);
                            if present_wins {
                                continue;
                            }
                        }
                        entries.insert(fingerprint, placed);
                    }
                }
                let mut regrouped = Vec::with_capacity(units.len());
                for (unit, entries) in units {
                    regrouped.push(RegroupedLifecycle {
                        unit,
                        schema,
                        revision,
                        entries: entries.into_values().map(|placed| placed.entry).collect(),
                    });
                }
                Ok::<_, Report<RestoredPartitionError>>(regrouped)
            })
            .await
            .change_context(RestoredPartitionError::Admission)?
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::num::{NonZeroU16, NonZeroUsize};

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_backup::{BranchLifecycleEntry, StateField, StateValue};
    use nervix_models::{
        BranchName, BranchPartitions, CreateRelay, CreateSchema, DomainName, Model, ModelName,
        RelayBranching, RelayName, ResolvedBranching, SchemaName, Timestamp,
    };

    use super::*;
    use crate::runtime::single_worker_executor;

    fn branch() -> BranchName {
        BranchName::parse("by_tenant").assured("the test branch name is valid")
    }

    fn relay_node(count: u16) -> ScheduledNode {
        let relay = CreateRelay {
            name: RelayName::parse("events").assured("the test relay name is valid"),
            schema: SchemaName::parse("event").assured("the test schema name is valid"),
            buffer: NonZeroUsize::MIN,
            branching: RelayBranching::branched_by(branch()),
            materialized_state: None,
        };
        let key_schema = CreateSchema {
            name: SchemaName::parse("tenant_key").assured("the test schema name is valid"),
            fields: Vec::new(),
        };
        let partitions =
            BranchPartitions::Direct(NonZeroU16::new(count).assured("test counts are positive"));
        ScheduledNode::new(Model::Relay(relay), SchemaFingerprint::from_digest([3; 32]))
            .with_resolved_branching(Some(ResolvedBranching::branched(
                branch(),
                key_schema,
                partitions,
            )))
    }

    fn schedule(count: u16) -> DomainSchedule {
        DomainSchedule::new(
            DomainName::parse("payments").assured("the test domain name is valid"),
            [relay_node(count)],
            Vec::new(),
        )
    }

    fn events() -> NodeRef {
        NodeRef::new(
            ModelKind::Relay,
            ModelName::from(&RelayName::parse("events").assured("the test relay name is valid")),
        )
    }

    /// A fingerprint whose partition seed is `seed`.
    fn key(seed: u8) -> BranchKeyFingerprint {
        let mut digest = [0; 32];
        digest[0] = seed;
        BranchKeyFingerprint::new(digest)
    }

    fn placement(schedule: &DomainSchedule) -> RestoredPartitions<'_> {
        RestoredPartitions {
            schedule,
            placed: HashSet::default(),
        }
    }

    #[test]
    fn a_key_archived_in_its_own_unit_is_restored_there() {
        let schedule = schedule(8);
        let partitions = placement(&schedule);
        assert_eq!(
            partitions.section_unit(&events(), Unit::partition(5), Some(&key(13))),
            Some(Unit::partition(5))
        );
    }

    #[test]
    fn a_key_archived_under_the_previous_count_is_restored_in_the_unit_the_restored_count_gives_it()
    {
        let schedule = schedule(8);
        let partitions = placement(&schedule);
        // Seed 13 lands in partition 1 of four and in partition 5 of eight.
        assert_eq!(
            partitions.section_unit(&events(), Unit::partition(1), Some(&key(13))),
            Some(Unit::partition(5))
        );
    }

    #[test]
    fn a_released_copy_gives_way_to_the_copy_its_claim_installed_in_the_key_s_own_unit() {
        let schedule = schedule(8);
        let mut partitions = placement(&schedule);
        partitions.placed.insert(EntityKey {
            entity: events(),
            key: key(13),
        });
        assert_eq!(
            partitions.section_unit(&events(), Unit::partition(1), Some(&key(13))),
            None
        );
        assert_eq!(
            partitions.section_unit(&events(), Unit::partition(5), Some(&key(13))),
            Some(Unit::partition(5))
        );
    }

    #[test]
    fn an_entity_the_restored_schedule_does_not_hold_keeps_its_archived_unit() {
        let schedule = schedule(8);
        let partitions = placement(&schedule);
        let absent = NodeRef::new(
            ModelKind::Deduplicator,
            ModelName::parse("absent").assured("the test name is valid"),
        );
        assert!(partitions.divided(&absent).is_none());
        assert_eq!(
            partitions.section_unit(&absent, Unit::partition(2), Some(&key(13))),
            Some(Unit::partition(2))
        );
    }

    #[test]
    fn a_key_archived_in_a_partition_is_restored_in_the_single_unit_of_one_partition() {
        let schedule = schedule(1);
        let partitions = placement(&schedule);
        assert_eq!(
            partitions.section_unit(&events(), Unit::partition(3), Some(&key(13))),
            Some(Unit::SINGLE)
        );
    }

    fn tenant(name: &str) -> Option<Vec<StateField>> {
        Some(vec![StateField {
            name: "tenant".to_string(),
            value: StateValue::String(name.to_string()),
        }])
    }

    fn lifecycle(
        unit: Unit,
        revision: u64,
        entries: Vec<BranchLifecycleEntry>,
    ) -> BranchLifecycleRecord {
        BranchLifecycleRecord {
            domain: DomainName::parse("payments").assured("the test domain name is valid"),
            owner_kind: ModelKind::Relay,
            entity: events().identifier,
            unit,
            schema: SchemaFingerprint::from_digest([3; 32]),
            revision,
            branches: entries,
        }
    }

    fn entry(name: &str, last_ingestion: i64, incarnation: u64) -> BranchLifecycleEntry {
        BranchLifecycleEntry {
            key: tenant(name),
            last_ingestion: Timestamp::from_unix_nanos(last_ingestion),
            incarnation,
        }
    }

    fn unit_of(node: &ScheduledNode, name: &str) -> Unit {
        let key = BranchKey::from_remote_key(remote_branch_key(tenant(name).as_ref()))
            .assured("the test key is a valid branch key")
            .assured("the test key is concrete");
        node.unit_of_branch(Some(&key.fingerprint()))
            .assured("every key lands in a unit of the restored count")
    }

    #[nervix_primitives::test]
    async fn regrouped_lifecycles_hold_each_key_in_its_unit_under_the_newest_archived_revision() {
        let executor = single_worker_executor();
        let node = relay_node(8);
        // Each key was archived in a unit of the previous count of four, each unit's lifecycle
        // at its own revision.
        let archived = vec![
            lifecycle(
                Unit::partition(0),
                11,
                vec![entry("eta", 100, 1), entry("omega", 100, 6)],
            ),
            lifecycle(Unit::partition(1), 12, vec![entry("beta", 100, 2)]),
            lifecycle(Unit::partition(2), 16, vec![entry("lambda", 100, 3)]),
            lifecycle(
                Unit::partition(3),
                14,
                vec![entry("xi", 100, 4), entry("kappa", 100, 5)],
            ),
        ];
        let tenants = ["eta", "omega", "beta", "lambda", "xi", "kappa"];
        let regrouped = RegroupedLifecycle::regroup(&executor, &node, archived)
            .await
            .assured("the archived lifecycles regroup");
        let mut restored = 0;
        for lifecycle in &regrouped {
            assert_eq!(
                lifecycle.revision, 16,
                "the newest archived revision covers every entry"
            );
            for restored_entry in &lifecycle.entries {
                let Some(fields) = restored_entry.key.as_ref() else {
                    panic!("every restored entry is keyed");
                };
                let Some(field) = fields.first() else {
                    panic!("every restored key has its tenant field");
                };
                let nervix_models::RemoteRuntimeValue::String(name) = &field.value else {
                    panic!("the tenant field is text");
                };
                assert_eq!(unit_of(&node, name), lifecycle.unit);
                restored += 1;
            }
        }
        assert_eq!(restored, tenants.len());
    }

    #[nervix_primitives::test]
    async fn a_key_in_two_archived_lifecycles_keeps_the_entry_of_its_own_unit() {
        let executor = single_worker_executor();
        let node = relay_node(8);
        let own = unit_of(&node, "beta");
        let other = if own == Unit::partition(0) {
            Unit::partition(1)
        } else {
            Unit::partition(0)
        };
        // The holder's released entry was last active later than the claimant's installed one,
        // and still gives way to it.
        let archived = vec![
            lifecycle(other, 7, vec![entry("beta", 900, 3)]),
            lifecycle(own, 9, vec![entry("beta", 500, 4)]),
        ];
        let regrouped = RegroupedLifecycle::regroup(&executor, &node, archived)
            .await
            .assured("the archived lifecycles regroup");
        assert_eq!(regrouped.len(), 1);
        let restored = regrouped.first().assured("one unit holds the key");
        assert_eq!(restored.unit, own);
        assert_eq!(restored.entries.len(), 1);
        let kept = restored.entries.first().assured("the unit holds one entry");
        assert_eq!(kept.incarnation, 4);
        assert_eq!(kept.last_ingestion, Timestamp::from_unix_nanos(500));
        assert_eq!(restored.revision, 9);
    }
}
