use std::num::NonZeroU32;

use super::*;

fn count(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).expect("the test count is positive")
}

#[test]
fn a_pool_that_holds_its_minimum_needs_no_restoration() {
    for established in [
        EstablishedConnections { total: 2, idle: 2 },
        EstablishedConnections { total: 2, idle: 0 },
        EstablishedConnections { total: 3, idle: 1 },
    ] {
        assert_eq!(established.restoration(count(2)), None, "{established:?}");
    }
}

#[test]
fn an_idle_pool_below_its_minimum_checks_out_the_whole_minimum() {
    for total in 0..3 {
        let established = EstablishedConnections { total, idle: total };
        assert_eq!(
            established.restoration(count(3)),
            Some(Restoration {
                minimum: count(3),
                checkouts: count(3),
            }),
            "{established:?}"
        );
    }
}

/// One case of a pool below its minimum while publishers hold some of its connections.
struct HeldCase {
    established: EstablishedConnections,
    minimum: u32,
    checkouts: u32,
}

#[test]
fn connections_publishers_hold_count_toward_the_minimum() {
    let cases = [
        // A publisher holds the only connection left: one checkout opens the one missing.
        HeldCase {
            established: EstablishedConnections { total: 1, idle: 0 },
            minimum: 2,
            checkouts: 1,
        },
        // One of three connections is in use: holding both idle ones and one more opens exactly
        // the one missing.
        HeldCase {
            established: EstablishedConnections { total: 3, idle: 2 },
            minimum: 4,
            checkouts: 3,
        },
        // Publishers hold every connection left: each checkout opens one of the missing ones.
        HeldCase {
            established: EstablishedConnections { total: 2, idle: 0 },
            minimum: 4,
            checkouts: 2,
        },
    ];
    for case in cases {
        assert_eq!(
            case.established.restoration(count(case.minimum)),
            Some(Restoration {
                minimum: count(case.minimum),
                checkouts: count(case.checkouts),
            }),
            "{:?} below {}",
            case.established,
            case.minimum
        );
    }
}
