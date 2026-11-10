//! The registry of each unit's published partition holdings, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The succession invariant of one unit's holdings: an owner that ends withdraws only its
//!   own registration, so a successor owner of the same unit that registers while it ends keeps its
//!   publication, and an observer reads one owner's complete holdings at a time.
//! - **Depends on.** The production holdings registry and the server Shuttle runner.
//! - **Must not know.** How a unit parks input, moves state or answers a claimant.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::{
    BranchName, DomainName, DomainNodeRef, DomainNodeUnitRef, ModelKind, ModelName,
    PartitionEpochNumber, Unit,
};
use nervix_primitives::sync::StdArc;

use super::{MisplacedKeys, PartitionHoldingsRegistry, UnitHoldings};

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";

fn unit() -> DomainNodeUnitRef {
    DomainNodeUnitRef::new(
        DomainNodeRef::node_in(
            DomainName::parse("payments").assured("the model domain name is valid"),
            ModelKind::Deduplicator,
            ModelName::parse("distinct_events").assured("the model entity name is valid"),
        ),
        Unit::partition(1),
    )
}

fn holdings(claimed: u64) -> UnitHoldings {
    UnitHoldings {
        branch: BranchName::parse("by_tenant").assured("the model branch name is valid"),
        epoch: PartitionEpochNumber::FIRST.next(),
        misplaced: MisplacedKeys::new(),
        claimed,
        claiming: 0,
    }
}

/// A unit's owner ends, withdrawing its holdings, while the unit's next owner registers its own and
/// republishes them, and an observer reads the unit. Whatever the order, the next owner's latest
/// holdings stay published, and the observer reads nothing or one owner's whole value.
fn a_successor_registering_while_its_predecessor_withdraws() {
    shuttle::future::block_on(async {
        let registry = StdArc::new(PartitionHoldingsRegistry::default());
        let predecessor = registry.register(unit(), holdings(1));
        let ending = nervix_primitives::task::spawn({
            let registry = registry.clone();
            async move {
                registry.withdraw(&unit(), &predecessor);
            }
        });
        let succeeding = nervix_primitives::task::spawn({
            let registry = registry.clone();
            async move {
                let successor = registry.register(unit(), holdings(2));
                successor.store(StdArc::new(holdings(3)));
            }
        });
        let observing = nervix_primitives::task::spawn({
            let registry = registry.clone();
            async move {
                let observed = registry.get(&unit())?;
                Some(observed.claimed)
            }
        });
        ending.await.assured(MODEL_TASK_JOINS);
        succeeding.await.assured(MODEL_TASK_JOINS);
        let observed = observing.await.assured(MODEL_TASK_JOINS);
        let current = registry
            .get(&unit())
            .assured("the successor's registration outlives its predecessor's withdrawal");
        assert_eq!(
            current.claimed, 3,
            "the unit publishes a value other than its current owner's latest"
        );
        if let Some(claimed) = observed {
            assert!(
                (1..=3).contains(&claimed),
                "the observer read holdings no owner published"
            );
        }
    });
}

#[test]
fn shuttle_a_successor_unit_owner_keeps_its_holdings_when_its_predecessor_withdraws() {
    check_interleavings(a_successor_registering_while_its_predecessor_withdraws);
}
