//! One inspection's authoritative branch copies while partition claims are in flight.
//!
//! Layer: control plane.
//! - **Owns.** A bounded census of selected branch fingerprints, choosing the current partition's
//!   installed copy or its holder, and fencing pages against changes to that census.
//! - **Depends on.** Committed partition counts, process-targeted owner pages and Bulk admission.
//! - **Must not know.** Checkpoint transfer, branch tasks or how publications are mutated.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns inspection copy selection")
)]

use std::collections::{BTreeMap, BTreeSet};

use nervix_execution::{Cancellation, CpuClass, MemoryClass, Reservation};
use nervix_models::{BranchPartitions, ClusterNodeIncarnation, ClusterNodeName};

use super::*;

pub(super) struct PartitionInspection {
    partitions: BranchPartitions,
    catalogs: BTreeMap<Unit, BTreeSet<BranchKeyFingerprint>>,
    owners: BTreeMap<BranchKeyFingerprint, Unit>,
    memory: Reservation,
    max_bytes: u64,
}

impl PartitionInspection {
    fn reserve(&mut self, keys: usize) -> Result<(), StateInspectionRejection> {
        // Charge catalog storage, the keyed owner index, the request's exclusion copy and tree
        // allocation overhead before retaining a catalog. Sparse trees can allocate a whole node
        // for one entry; overcharge those nodes as well as the four simultaneous representations.
        // The whole census uses one reply budget.
        let entry = std::mem::size_of::<BranchKeyFingerprint>()
            .checked_add(std::mem::size_of::<Unit>())
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        let entries = keys
            .checked_mul(entry)
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        let catalog_bytes = entries
            .checked_add(std::mem::size_of::<BTreeSet<BranchKeyFingerprint>>())
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        let bytes = catalog_bytes
            .checked_mul(64)
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        let bytes = u64::try_from(bytes)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::ReplyTooLarge))?;
        let total = self
            .memory
            .bytes()
            .checked_add(bytes)
            .ok_or(StateInspectionRejection::ReplyTooLarge)?;
        if total > self.max_bytes {
            return Err(StateInspectionRejection::ReplyTooLarge);
        }
        self.memory
            .grow_to(total)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))
    }

    fn record(
        &mut self,
        unit: Unit,
        catalog: BTreeSet<BranchKeyFingerprint>,
        cancellation: &Cancellation,
    ) -> Result<(), StateInspectionRejection> {
        self.reserve(catalog.len())?;
        for key in &catalog {
            cancellation
                .check()
                .map_err(|error| inspection_refusal(error, StateInspectionRejection::Deadline))?;
            let current = self.partitions.unit_of(key.partition_seed());
            match self.owners.entry(*key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(unit);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if unit == current {
                        entry.insert(unit);
                    } else if *entry.get() != current {
                        // Two holders without an installed claimant do not identify one source.
                        return Err(StateInspectionRejection::NotReady);
                    }
                }
            }
        }
        self.catalogs.insert(unit, catalog);
        Ok(())
    }

    pub(super) fn exclusions(
        &self,
        unit: Unit,
    ) -> Result<BTreeSet<BranchKeyFingerprint>, StateInspectionRejection> {
        let catalog = self
            .catalogs
            .get(&unit)
            .ok_or(StateInspectionRejection::NotReady)?;
        let mut excluded = BTreeSet::new();
        for key in catalog {
            if self.owners.get(key) != Some(&unit) {
                excluded.insert(*key);
            }
        }
        Ok(excluded)
    }

    pub(super) fn validate(
        &self,
        unit: Unit,
        catalog: Option<&BTreeSet<BranchKeyFingerprint>>,
    ) -> Result<(), StateInspectionRejection> {
        if catalog != self.catalogs.get(&unit) {
            return Err(StateInspectionRejection::NotReady);
        }
        Ok(())
    }
}

impl SessionServiceImpl {
    pub(super) async fn capture_partition_inspection(
        &self,
        plan: &ProcessorInspectionPlan,
        incarnations: &BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
    ) -> Result<Option<PartitionInspection>, StateInspectionRejection> {
        let Some(partitions) = plan.layout.moving_partitions else {
            return Ok(None);
        };
        let executor = self.inner.runtime.inspection_executor();
        let memory = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
        let mut inspection = PartitionInspection {
            partitions,
            catalogs: BTreeMap::new(),
            owners: BTreeMap::new(),
            memory,
            max_bytes: u64::try_from(plan.limits.transfer_bytes())
                .assured("session byte limits fit u64"),
        };
        for unit in plan.layout.node.units.keys() {
            nervix_primitives::task::consume_budget().await;
            let owner = inspection_owner(
                &plan.layout.node,
                *unit,
                self.inner.consensus.local_node_id(),
            )?;
            let incarnation = incarnations
                .get(&owner)
                .copied()
                .ok_or(StateInspectionRejection::OwnerUnavailable)?;
            let page = self
                .processor_unit_page(
                    ReadProcessorStatePage {
                        domain: plan.domain.clone(),
                        processor: plan.processor.clone(),
                        unit: *unit,
                        control_revision: plan.control_revision,
                        schema: plan.layout.node.schema_fingerprint,
                        selection: plan.selection,
                        partition_exclusions: Some(BTreeSet::new()),
                        after: None,
                        max_rows: 0,
                        max_bytes: 0,
                        target: ClusterNodeIdentity::new(owner, incarnation),
                    },
                    plan,
                )
                .await?;
            let catalog = page
                .descriptor
                .partition_catalog
                .ok_or(StateInspectionRejection::NotReady)?;
            let count = u64::try_from(catalog.len()).assured("an admitted catalog fits u64");
            if count != page.descriptor.counts.branches {
                return Err(StateInspectionRejection::NotReady);
            }
            let admission = executor
                .try_reserve(MemoryClass::Bulk, 1)
                .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?;
            let unit = *unit;
            inspection = executor
                .run_cpu(CpuClass::Bulk, admission, move |_charge, cancellation| {
                    inspection.record(unit, catalog, cancellation)?;
                    Ok::<_, StateInspectionRejection>(inspection)
                })
                .await
                .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))??;
        }
        Ok(Some(inspection))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use nervix_execution::Executor;

    use super::*;

    fn key(seed: u64) -> BranchKeyFingerprint {
        let mut bytes = [9; 32];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        BranchKeyFingerprint::new(bytes)
    }

    fn fixture(executor: &Executor, count: u16, max_bytes: u64) -> PartitionInspection {
        PartitionInspection {
            partitions: BranchPartitions::Direct(
                NonZeroU16::new(count).assured("fixture counts are positive"),
            ),
            catalogs: BTreeMap::new(),
            owners: BTreeMap::new(),
            memory: executor
                .try_reserve(MemoryClass::Bulk, 1)
                .assured("bounded fixture admission"),
            max_bytes,
        }
    }

    #[nervix_primitives::test]
    async fn the_installed_partition_wins_in_both_read_orders_and_both_count_directions() {
        struct Case {
            count: u16,
            holder: Unit,
            claimant: Unit,
        }
        let cases = [
            Case {
                count: 8,
                holder: Unit::partition(1),
                claimant: Unit::partition(5),
            },
            Case {
                count: 4,
                holder: Unit::partition(5),
                claimant: Unit::partition(1),
            },
            Case {
                count: 1,
                holder: Unit::partition(5),
                claimant: Unit::SINGLE,
            },
        ];
        let executor = Executor::default();
        for case in cases {
            for claimant_first in [false, true] {
                for installed in [false, true] {
                    nervix_primitives::task::consume_budget().await;
                    let moving = key(5);
                    let held = BTreeSet::from([moving]);
                    let claimed = if installed {
                        held.clone()
                    } else {
                        BTreeSet::new()
                    };
                    let mut inspection = fixture(&executor, case.count, 1_048_576);
                    let admission = executor
                        .try_reserve(MemoryClass::Bulk, 1)
                        .assured("bounded job admission");
                    let holder = case.holder;
                    let claimant = case.claimant;
                    inspection = executor
                        .run_cpu(CpuClass::Bulk, admission, move |_charge, cancellation| {
                            if claimant_first {
                                inspection.record(claimant, claimed, cancellation)?;
                                inspection.record(holder, held, cancellation)?;
                            } else {
                                inspection.record(holder, held, cancellation)?;
                                inspection.record(claimant, claimed, cancellation)?;
                            }
                            Ok::<_, StateInspectionRejection>(inspection)
                        })
                        .await
                        .assured("the bounded job completes")
                        .assured("one holder and its claimant are consistent");
                    let excluded = inspection
                        .exclusions(holder)
                        .assured("the holder was sampled");
                    assert_eq!(excluded.contains(&moving), installed);
                    assert!(
                        inspection
                            .exclusions(claimant)
                            .assured("the claimant was sampled")
                            .is_empty()
                    );
                    let selected = if installed { claimant } else { holder };
                    assert_eq!(inspection.owners.get(&moving), Some(&selected));
                    let catalog = inspection
                        .catalogs
                        .get(&claimant)
                        .assured("the claimant was sampled");
                    inspection
                        .validate(claimant, Some(catalog))
                        .assured("an unchanged catalog remains valid");
                    assert_eq!(
                        inspection.validate(claimant, None),
                        Err(StateInspectionRejection::NotReady)
                    );
                    if !installed {
                        assert_eq!(
                            inspection.validate(claimant, Some(&BTreeSet::from([moving]))),
                            Err(StateInspectionRejection::NotReady),
                            "installation racing the census refuses the page"
                        );
                    } else {
                        assert_eq!(
                            inspection.validate(holder, Some(&BTreeSet::new())),
                            Err(StateInspectionRejection::NotReady),
                            "forget racing the census refuses the page"
                        );
                    }
                }
            }
        }
    }

    #[nervix_primitives::test]
    async fn a_census_is_refused_before_retaining_a_catalog_beyond_its_byte_budget() {
        let executor = Executor::default();
        let mut inspection = fixture(&executor, 8, 1);
        let admission = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .assured("bounded job admission");
        executor
            .run_cpu(CpuClass::Bulk, admission, move |_charge, cancellation| {
                let result =
                    inspection.record(Unit::partition(1), BTreeSet::from([key(5)]), cancellation);
                assert_eq!(result, Err(StateInspectionRejection::ReplyTooLarge));
                assert!(inspection.catalogs.is_empty());
                assert!(inspection.owners.is_empty());
            })
            .await
            .assured("the bounded job completes");
    }
}
