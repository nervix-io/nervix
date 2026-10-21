//! The operation budget's arithmetic against the split its owner documents.
//!
//! Layer: test harness.
//! - **Owns.** Reference values for what is left of a budget, the lookup's share of it and each
//!   address attempt's share, the Bolero property that holds the budget to them, and focused tests.
//! - **Depends on.** The operation budget and the primitives boundary's monotonic clock.
//! - **Must not know.** Name resolution, sockets or any caller of the budget.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use meticulous::OptionExt as _;

use super::{LookupShare, OperationBudget};

/// What is left of `total` once `elapsed` has passed: never less than nothing.
fn left(total: Duration, elapsed: Duration) -> Duration {
    match total.checked_sub(elapsed) {
        Some(left) => left,
        None => Duration::ZERO,
    }
}

/// One budget, the instants at which a caller asks it, and how many answers it dials.
#[derive(Debug, bolero::TypeGenerator)]
struct BudgetCase {
    /// The operation's whole budget, in milliseconds.
    #[generator(0_u32..=600_000)]
    total_millis: u32,
    /// The lookup's share in milliseconds, where the case bounds it.
    share_millis: Option<u32>,
    /// The time that passes before each question, in milliseconds.
    #[generator(bolero::generator::produce_with::<Vec<u32>>().len(1_usize..=8))]
    pauses_millis: Vec<u32>,
    /// How many answers the lookup returns.
    #[generator(1_u32..=16)]
    answers: u32,
}

impl BudgetCase {
    fn total(&self) -> Duration {
        Duration::from_millis(u64::from(self.total_millis))
    }

    fn share(&self) -> LookupShare {
        match self.share_millis {
            Some(millis) => LookupShare::AtMost(Duration::from_millis(u64::from(millis))),
            None => LookupShare::Whole,
        }
    }
}

#[test]
fn bolero_operation_budget_shares_never_outgrow_what_is_left() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .with_type::<BudgetCase>()
        .for_each(|case| {
            let budget = OperationBudget::start(case.total(), case.share());
            let mut elapsed = Duration::ZERO;
            let mut previous_remaining = case.total();
            for pause in &case.pauses_millis {
                elapsed = elapsed
                    .checked_add(Duration::from_millis(u64::from(*pause)))
                    .assured("eight pauses of at most 2^32 milliseconds fit in a duration");
                let remaining = budget.remaining_after(elapsed);
                assert_eq!(remaining, left(case.total(), elapsed));
                assert!(remaining <= previous_remaining, "a budget never grows back");
                previous_remaining = remaining;

                let lookup = budget.lookup_after(elapsed);
                assert!(lookup <= remaining, "a lookup never outlives its operation");
                match case.share() {
                    LookupShare::Whole => assert_eq!(lookup, remaining),
                    LookupShare::AtMost(share) => {
                        assert_eq!(lookup, left(share, elapsed).min(remaining));
                    }
                }

                // Every attempt that runs out of its share leaves the next one an equal share of
                // what is left, so the attempts together never outlast the budget.
                let mut left_for_attempts = remaining;
                let mut spent = Duration::ZERO;
                for untried in (1..=case.answers).rev() {
                    let share = OperationBudget::attempt_share(left_for_attempts, untried);
                    let reserved = share
                        .checked_mul(untried)
                        .assured("an equal share times the answers it was divided among fits");
                    let unreserved = left_for_attempts
                        .checked_sub(reserved)
                        .assured("shares divided from what is left never exceed it");
                    assert!(
                        unreserved < Duration::from_nanos(u64::from(untried)),
                        "an equal share wastes less than one nanosecond per answer"
                    );
                    spent = spent
                        .checked_add(share)
                        .assured("the shares together never exceed the budget they divide");
                    left_for_attempts = left_for_attempts
                        .checked_sub(share)
                        .assured("one share never exceeds what it was divided from");
                }
                assert!(spent <= remaining, "the attempts never outlast the budget");
            }
        });
}

#[test]
fn a_whole_lookup_takes_everything_left_and_a_bounded_one_stops_at_its_share() {
    let whole = OperationBudget::start(Duration::from_secs(30), LookupShare::Whole);
    assert_eq!(
        whole.lookup_after(Duration::from_secs(10)),
        Duration::from_secs(20)
    );

    let bounded = OperationBudget::start(
        Duration::from_secs(30),
        LookupShare::AtMost(Duration::from_secs(5)),
    );
    assert_eq!(
        bounded.lookup_after(Duration::from_secs(2)),
        Duration::from_secs(3)
    );
    assert_eq!(bounded.lookup_after(Duration::from_secs(7)), Duration::ZERO);
    assert_eq!(
        bounded.remaining_after(Duration::from_secs(7)),
        Duration::from_secs(23),
        "the request keeps what the lookup did not take"
    );
}

#[test]
fn each_answer_gets_an_equal_share_of_what_is_left_when_its_attempt_starts() {
    let budget = OperationBudget::start(Duration::from_secs(30), LookupShare::Whole);
    let addresses = [
        SocketAddr::from((Ipv4Addr::LOCALHOST, 1)),
        SocketAddr::from((Ipv4Addr::LOCALHOST, 2)),
        SocketAddr::from((Ipv4Addr::LOCALHOST, 3)),
    ];
    let mut attempts = budget.attempts(&addresses);
    let first = attempts.next().assured("three answers give three attempts");
    assert_eq!(first.address, addresses[0]);
    assert!(first.budget <= Duration::from_secs(10));
    assert!(first.budget > Duration::from_secs(9));
    assert_eq!(attempts.count(), 2);
}

#[test]
fn a_spent_budget_leaves_nothing_for_the_lookup_or_the_attempts() {
    let budget = OperationBudget::start(Duration::ZERO, LookupShare::Whole);
    assert_eq!(budget.lookup(), Duration::ZERO);
    assert_eq!(budget.remaining(), Duration::ZERO);
    let addresses = [SocketAddr::from((Ipv4Addr::LOCALHOST, 1))];
    let shares: Vec<_> = budget
        .attempts(&addresses)
        .map(|attempt| attempt.budget)
        .collect();
    assert_eq!(shares, vec![Duration::ZERO]);
}
