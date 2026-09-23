//! The pool every port the integration-test harness binds is drawn from.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The set of ports scenarios in this process have claimed, drawing fresh ports from
//!   the operating system into that set, the bound on how long one draw may keep landing on ports
//!   already claimed, and giving ports back.
//! - **Depends on.** The operating system's ephemeral port allocation.
//! - **Must not know.** What a port is bound to, nodes, fixtures, or scenario state.
//!
//! # Why a draw is bounded
//!
//! Every scenario future is polled on the one runner task, so a draw that loops until the
//! operating system hands out a port the pool does not hold yet stalls every scenario and the
//! suite watchdog with it: nothing else on that task runs while the loop does. The pool is smaller
//! than the ephemeral range suggests, too. Linux hands `bind(0)` an odd port from the lower half
//! of that range, some seven thousand ports in all, and this process cannot see the ports its
//! sibling worktrees hold. A draw therefore gives up after [`PORT_DRAW_LIMIT`] misses in a row and
//! reports the pool exhausted, so the scenario that needed the port fails with that reason rather
//! than holding the suite until the workflow job is killed.

use std::{
    collections::BTreeSet,
    io,
    net::{IpAddr, Ipv4Addr, TcpListener},
    sync::LazyLock,
};

use meticulous::OptionExt as _;
use parking_lot::Mutex;
use thiserror::Error;

const HOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// How many draws in a row may land on a port the pool already holds before the pool is treated
/// as exhausted. The operating system picks at random from its ephemeral range, so a pool with a
/// single free port among seven thousand still hands it out inside this many draws almost every
/// time, while a pool with none left ends the draw in about a second. A policy input.
pub(crate) const PORT_DRAW_LIMIT: u32 = 65_536;

/// Every port any scenario in this process has claimed. Scenarios run concurrently in one test
/// binary, and a draw drops its probe listener as soon as it has read the port number, so the
/// operating system does not stop a second scenario from binding the same port. This set is the
/// only thing that does. Startup still retries on a fresh allocation, because the pool is shared
/// with sibling worktrees running the same suite, and their binds are invisible here.
///
/// A port leaves the set only once nothing can still dial it. Returning one while a peer holds it
/// in gossip lets an unrelated scenario's node answer that peer, and because every scenario names
/// its nodes `node-1`, `node-2` and `node-3`, the certificate identity alone cannot distinguish
/// those separate test clusters. Never releasing is not the alternative: the ports the suite
/// draws across a run exceed the range the operating system draws from, so every owner has to
/// give its ports back once what bound them is gone.
static RESERVED_TEST_PORTS: LazyLock<Mutex<BTreeSet<u16>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));

/// Why a draw produced no port.
#[derive(Debug, Error)]
pub(crate) enum PortPoolError {
    #[error("the operating system could not hand out a port")]
    Draw(#[source] io::Error),
    #[error(
        "the port pool is exhausted: {misses} draws in a row landed on ports this process had \
         already reserved, {reserved} in all"
    )]
    Exhausted { reserved: usize, misses: u32 },
}

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

/// Draws `count` fresh ports from the operating system and reserves them.
pub(crate) fn next_ports(count: usize) -> io::Result<Vec<u16>> {
    let ports = reserve(count, || {
        let listener = TcpListener::bind((HOST, 0))?;
        let address = listener.local_addr()?;
        Ok(address.port())
    })
    .map_err(io::Error::other)?;
    Ok(ports)
}

/// Reserves `count` ports drawn from `draw`.
///
/// A draw that lands on a port the pool already holds is discarded and drawn again, until
/// [`PORT_DRAW_LIMIT`] draws in a row have all been discarded. A draw that ends without its ports,
/// whether the pool is exhausted or the operating system refused, gives back the ports it had
/// already reserved.
pub(crate) fn reserve<Draw>(count: usize, mut draw: Draw) -> Result<Vec<u16>, PortPoolError>
where
    Draw: FnMut() -> io::Result<u16>,
{
    let mut ports = Vec::with_capacity(count);
    let mut misses = 0_u32;
    while ports.len() < count {
        let port = match draw() {
            Ok(port) => port,
            Err(error) => {
                release_test_ports(&ports);
                return Err(PortPoolError::Draw(error));
            }
        };
        let mut reserved = RESERVED_TEST_PORTS.lock();
        let fresh = reserved.insert(port);
        let held = reserved.len();
        drop(reserved);
        if fresh {
            ports.push(port);
            misses = 0;
            continue;
        }
        misses = misses
            .checked_add(1)
            .assured("the miss count is below the draw limit until the draw ends");
        if misses >= PORT_DRAW_LIMIT {
            release_test_ports(&ports);
            return Err(PortPoolError::Exhausted {
                reserved: held,
                misses,
            });
        }
    }
    Ok(ports)
}
