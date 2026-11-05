//! The production cell's scalar rollback invariant; no cross-location ordering claim.

use governor::{
    nanos::Nanos,
    state::{NotKeyed, StateStore as _},
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};
use nervix_primitives::sync::Arc;

use crate::cell::DirectCell;

#[test]
fn loom_a_refunded_charge_restores_the_exact_prior_horizon() {
    explore(InvariantId::new("rate-limit.direct.exact-refund"), || {
        let cell = Arc::new(DirectCell::new(7));
        let observed = cell.clone();
        let reader = spawn(move || {
            for _ in 0..2 {
                let horizon = observed.horizon();
                assert!(
                    horizon == 7 || horizon == 10,
                    "refund changed the pre-charge horizon"
                );
            }
        });
        let before = cell.horizon();
        cell.measure_and_replace(&NotKeyed::NonKey, |value| {
            let value = value.assured("the production store supplies its horizon");
            Ok::<_, ()>((
                (),
                Nanos::from(
                    value
                        .as_u64()
                        .checked_add(3)
                        .assured("the test horizon fits"),
                ),
            ))
        })
        .assured("the charge is conforming");
        cell.refund(before);
        assert_eq!(cell.horizon(), 7, "refund changed the pre-charge horizon");
        reader
            .join()
            .assured("the observer only reads the production cell");
    });
}
