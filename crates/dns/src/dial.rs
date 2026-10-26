//! The physical budget of one outbound operation and the ordered address attempts inside it.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** One physical deadline for an outbound operation: how much of it the name lookup
//!   may take, the equal share of what is left that each resolved address receives when its
//!   attempt starts, and the time that remains for the handshake and the request after them.
//! - **Depends on.** Tokio's monotonic clock and typed socket addresses.
//! - **Must not know.** Connectors, interconnect peers, TLS, or protocol-specific retries.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{net::SocketAddr, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::Instant;

/// One physical deadline shared by an operation's name lookup, its address attempts and whatever
/// follows its connection.
pub struct OperationBudget {
    started: Instant,
    total: Duration,
    lookup: LookupShare,
}

/// How much of an operation's budget its name lookup may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupShare {
    /// Whatever the operation has left. An operation that is only a connection uses this, because
    /// nothing after its lookup can start without an answer, so holding time back for later steps
    /// would only fail it sooner.
    Whole,
    /// At most this much, counted from the start of the operation, so the address attempts and the
    /// request after them keep the rest whatever the name servers do.
    AtMost(Duration),
}

impl OperationBudget {
    pub fn start(total: Duration, lookup: LookupShare) -> Self {
        Self {
            started: Instant::now(),
            total,
            lookup,
        }
    }

    /// The time left before the deadline, or zero once it has passed.
    pub fn remaining(&self) -> Duration {
        self.remaining_after(self.started.elapsed())
    }

    /// The time the name lookup may take, started now: what is left of the operation, within the
    /// lookup's share of it.
    pub fn lookup(&self) -> Duration {
        self.lookup_after(self.started.elapsed())
    }

    /// Addresses in resolution order, each with its share of the remaining setup time.
    pub fn attempts<'a>(&'a self, addresses: &'a [SocketAddr]) -> AddressAttempts<'a> {
        AddressAttempts {
            budget: self,
            addresses: addresses.iter(),
        }
    }

    /// The time left once `elapsed` has passed since the operation started.
    fn remaining_after(&self, elapsed: Duration) -> Duration {
        match self.total.checked_sub(elapsed) {
            Some(remaining) => remaining,
            None => Duration::ZERO,
        }
    }

    /// The time a lookup started once `elapsed` has passed may take.
    fn lookup_after(&self, elapsed: Duration) -> Duration {
        let remaining = self.remaining_after(elapsed);
        let share = match self.lookup {
            LookupShare::Whole => return remaining,
            LookupShare::AtMost(share) => share,
        };
        let share_left = match share.checked_sub(elapsed) {
            Some(left) => left,
            None => Duration::ZERO,
        };
        share_left.min(remaining)
    }

    /// The share of `remaining` one attempt receives when `untried` addresses, itself included,
    /// are still to be tried.
    fn attempt_share(remaining: Duration, untried: u32) -> Duration {
        remaining
            .checked_div(untried)
            .verified("the address about to be dialled is itself untried")
    }
}

/// One address and the time allocated for connecting to it.
pub struct AddressAttempt {
    pub address: SocketAddr,
    pub budget: Duration,
}

/// The ordered address attempts of one connection.
pub struct AddressAttempts<'a> {
    budget: &'a OperationBudget,
    addresses: std::slice::Iter<'a, SocketAddr>,
}

impl Iterator for AddressAttempts<'_> {
    type Item = AddressAttempt;

    fn next(&mut self) -> Option<Self::Item> {
        let untried = self.addresses.len();
        if untried == 0 {
            return None;
        }
        let address = *self
            .addresses
            .next()
            .verified("the address iterator has a nonzero number of untried addresses");
        let untried = u32::try_from(untried)
            .assured("a DNS answer fits in one message and holds far fewer than 2^32 addresses");
        let share = OperationBudget::attempt_share(self.budget.remaining(), untried);
        Some(AddressAttempt {
            address,
            budget: share,
        })
    }
}

#[cfg(test)]
#[path = "dial_tests.rs"]
mod tests;
