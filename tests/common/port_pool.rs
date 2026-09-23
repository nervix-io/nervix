//! The pool every port the integration-test harness binds is drawn from.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The set of ports scenarios in this process have claimed, drawing fresh ports from
//!   the operating system into that set, and giving ports back to it.
//! - **Depends on.** The operating system's ephemeral port allocation.
//! - **Must not know.** What a port is bound to, nodes, fixtures, or scenario state.

use std::{
    collections::BTreeSet,
    io,
    net::{IpAddr, Ipv4Addr, TcpListener},
    sync::LazyLock,
};

use parking_lot::Mutex;

const HOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Every port any scenario in this process has claimed. Scenarios run concurrently in one test
/// binary, and `next_ports` drops its probe listener as soon as it has read the port number, so the
/// operating system does not stop a second scenario from binding the same port. This set is the
/// only thing that does. Startup still retries on a fresh allocation, because the pool is shared
/// with sibling worktrees running the same suite, and their binds are invisible here.
///
/// A port leaves the set only once nothing can still dial it. Returning one while a peer holds it
/// in gossip lets an unrelated scenario's node answer that peer, and because every scenario names
/// its nodes `node-1`, `node-2` and `node-3`, the certificate identity alone cannot distinguish
/// those separate test clusters. Never releasing is not the alternative: seven ports per
/// node across the suite exceeds the ephemeral range, so teardown has to give them back.
static RESERVED_TEST_PORTS: LazyLock<Mutex<BTreeSet<u16>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));

pub(crate) fn next_port() -> io::Result<u16> {
    let mut ports = next_ports(1)?;
    Ok(ports.remove(0))
}

/// Returns ports a stopped fixture no longer binds to the reservation set every scenario shares.
pub(crate) fn release_test_ports(ports: &[u16]) {
    let mut reserved = RESERVED_TEST_PORTS.lock();
    for port in ports {
        reserved.remove(port);
    }
}

pub(crate) fn next_ports(count: usize) -> io::Result<Vec<u16>> {
    let mut listeners = Vec::with_capacity(count);
    let mut ports = Vec::with_capacity(count);
    while ports.len() < count {
        let listener = TcpListener::bind((HOST, 0))?;
        let port = listener.local_addr()?.port();
        let mut reserved = RESERVED_TEST_PORTS.lock();
        if !reserved.insert(port) {
            continue;
        }
        drop(reserved);
        ports.push(port);
        listeners.push(listener);
    }
    Ok(ports)
}
