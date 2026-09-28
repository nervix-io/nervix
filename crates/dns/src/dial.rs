//! The physical deadline and ordered address attempts of an outbound connection.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Sharing one setup deadline across DNS and successive resolved addresses, with each
//!   address receiving an equal share of the time still available when its attempt starts.
//! - **Depends on.** Tokio's monotonic clock and typed socket addresses.
//! - **Must not know.** Connectors, interconnect peers, TLS, or protocol-specific retries.

use std::{net::SocketAddr, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use tokio::time::Instant;

/// One physical connection deadline shared by lookup and every address attempt.
pub struct ConnectionBudget {
    started: Instant,
    total: Duration,
}

impl ConnectionBudget {
    pub fn start(total: Duration) -> Self {
        Self {
            started: Instant::now(),
            total,
        }
    }

    /// The time left before the deadline, or zero once it has passed.
    pub fn remaining(&self) -> Duration {
        match self.total.checked_sub(self.started.elapsed()) {
            Some(remaining) => remaining,
            None => Duration::ZERO,
        }
    }

    /// Addresses in resolution order, each with its share of the remaining setup time.
    pub fn attempts<'a>(&'a self, addresses: &'a [SocketAddr]) -> AddressAttempts<'a> {
        AddressAttempts {
            budget: self,
            addresses: addresses.iter(),
        }
    }
}

/// One address and the time allocated for connecting to it.
pub struct AddressAttempt {
    pub address: SocketAddr,
    pub budget: Duration,
}

/// The ordered address attempts of one connection.
pub struct AddressAttempts<'a> {
    budget: &'a ConnectionBudget,
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
        let share = self
            .budget
            .remaining()
            .checked_div(untried)
            .verified("the address about to be dialled is itself untried");
        Some(AddressAttempt {
            address,
            budget: share,
        })
    }
}
