//! Fault controls at the accepts of a node's public listeners.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Failing the next accept of the public listener bound to an address, the way an
//!   accept fails when the process has run out of file descriptors, and the observation that the
//!   failure happened.
//! - **Depends on.** The shared test injector and execution-sensitive watch channels and sockets.
//! - **Must not know.** What a listener serves, or what it does with an accept that failed.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns the test failure of a public listener's accept and its observation"
    )
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    net::SocketAddr,
};

use meticulous::ResultExt as _;
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    time::Instant,
};

use super::FaultInjection;

/// The bound addresses whose listener fails its next accept, and those whose armed failure
/// happened.
#[derive(Debug, Default)]
pub(super) struct ListenerAcceptFailures {
    armed: BTreeSet<SocketAddr>,
    failed: BTreeMap<SocketAddr, Instant>,
}

impl ListenerAcceptFailures {
    /// Takes the failure armed for the listener bound to `address` and records that it happened.
    /// `false` when no failure is armed there.
    fn fire(&mut self, address: SocketAddr) -> bool {
        if !self.armed.remove(&address) {
            return false;
        }
        self.failed.insert(address, Instant::now());
        true
    }
}

impl FaultInjection {
    /// Fails the next accept of the public listener bound to `address`, whether the listener is
    /// waiting for a connection or takes one later. The failure leaves the connections waiting to
    /// be accepted where they are, as a failure for want of descriptors does.
    pub fn fail_next_listener_accept(&self, address: SocketAddr) {
        self.inner.listener_accept_failures.send_modify(|failures| {
            failures.failed.remove(&address);
            failures.armed.insert(address);
        });
    }

    /// Resolves once the failure armed for the listener bound to `address` happened.
    pub async fn listener_accept_failure(&self, address: SocketAddr) -> Instant {
        let mut changes = self.inner.listener_accept_failures.subscribe();
        loop {
            nervix_primitives::task::consume_budget().await;
            if let Some(fired_at) = changes.borrow_and_update().failed.get(&address).copied() {
                return fired_at;
            }
            changes
                .changed()
                .await
                .assured("the injector this wait borrows holds the failures' sender");
        }
    }

    /// The accepts of a testing build's public listener, which fail where a scenario armed it.
    pub(crate) fn listener_accept_faults(&self) -> ListenerAcceptFaults {
        ListenerAcceptFaults {
            injection: self.clone(),
        }
    }
}

/// How a public listener of a testing build accepts: as its socket does, unless a scenario armed
/// the failure of its next accept.
pub(crate) struct ListenerAcceptFaults {
    injection: FaultInjection,
}

impl ListenerAcceptFaults {
    /// The connection `listener` accepts next, or the failure armed for it.
    pub(crate) async fn accept(
        &self,
        listener: &TcpListener,
    ) -> io::Result<(TcpStream, SocketAddr)> {
        let address = listener.local_addr()?;
        let failures = &self.injection.inner.listener_accept_failures;
        // Registered before the armed failures are read, so a failure armed after that read wakes
        // the wait below.
        let mut changes = failures.subscribe();
        loop {
            nervix_primitives::task::consume_budget().await;
            if failures.send_if_modified(|failures| failures.fire(address)) {
                return Err(io::Error::other("injected public listener accept failure"));
            }
            nervix_primitives::select! {
                accepted = listener.accept() => return accepted,
                changed = changes.changed() => {
                    changed.assured("this accept holds the injector, which holds the failures' sender");
                }
            }
        }
    }
}
