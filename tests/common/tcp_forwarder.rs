//! TCP forwarders a scenario stands at chosen loopback addresses in front of one dependency.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One listener per chosen address, all on one port drawn from the harness pool, the
//!   connections each listener forwards to the dependency, how many connections each accepted,
//!   stopping one listener together with its connections, and stalling them: the listener closes
//!   while its connections stay open and carry nothing more.
//! - **Depends on.** Tokio's TCP sockets and the harness port pool.
//! - **Must not know.** DNS, Nervix, or the protocol the connections carry.
//!
//! # Addresses that cannot connect
//!
//! Every forwarder listens on the same port, so a scenario can answer a DNS name with a loopback
//! address that has no forwarder, and a client dialling that answer is refused at once. A
//! dependency's own published port listens on every address, which is why a scenario that needs an
//! answer that cannot connect reaches the dependency through these forwarders instead.

use std::{
    collections::BTreeMap,
    io,
    net::{IpAddr, SocketAddr},
};

use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{
        CancellationToken, StdArc,
        atomic::{AtomicU64, Ordering},
    },
    task::JoinHandle,
};
use nervix_recovery::Discarded as _;
use tokio::io::copy_bidirectional;

use crate::common::port_pool::{next_port, release_test_ports};

/// The forwarders in front of one dependency, and the pool port they share.
#[derive(Debug)]
pub(crate) struct TcpForwarders {
    port: u16,
    target: SocketAddr,
    forwarders: BTreeMap<IpAddr, TcpForwarder>,
}

/// One listener and the connections it forwarded.
#[derive(Debug)]
struct TcpForwarder {
    accepted: StdArc<AtomicU64>,
    /// Cancelled to close every connection the listener forwarded, when the forwarder stops.
    connections: CancellationToken,
    /// Cancelled to stop carrying bytes on every connection the listener forwarded, while each
    /// stays open until the forwarder stops.
    stalled: CancellationToken,
    /// The accepting task, until `close_listener` has ended it.
    listener: Option<JoinHandle<()>>,
}

impl TcpForwarders {
    /// Listen on `addresses`, all on one port drawn from the pool, forwarding every accepted
    /// connection to `target`.
    pub(crate) async fn start(addresses: &[IpAddr], target: SocketAddr) -> io::Result<Self> {
        let port = next_port()?;
        // Constructed before anything binds, so a failed bind still gives the port back.
        let mut forwarders = Self {
            port,
            target,
            forwarders: BTreeMap::new(),
        };
        for address in addresses {
            let listener = TcpListener::bind(SocketAddr::new(*address, port)).await?;
            let forwarder = TcpForwarder::start(listener, target);
            forwarders.forwarders.insert(*address, forwarder);
        }
        Ok(forwarders)
    }

    /// The port every forwarder listens on.
    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    /// Connections the forwarder at `address` has accepted so far.
    pub(crate) fn accepted(&self, address: IpAddr) -> io::Result<u64> {
        Ok(self.forwarder(address)?.accepted.load(Ordering::Relaxed))
    }

    /// Stop the forwarder at `address`: its listener has closed when this returns, so the address
    /// refuses connections from then on, and the connections it accepted are closed.
    pub(crate) async fn stop(&mut self, address: IpAddr) -> io::Result<()> {
        let Some(mut forwarder) = self.forwarders.remove(&address) else {
            return Err(Self::unknown(address));
        };
        forwarder.close_listener().await;
        Ok(())
    }

    /// Stall the forwarder at `address`: its listener has closed when this returns, so the address
    /// refuses connections from then on, and every connection it accepted stays open but carries no
    /// further byte in either direction. Each peer then sees a silent connection rather than a
    /// closed one, until the forwarder stops or the scenario ends.
    pub(crate) async fn stall(&mut self, address: IpAddr) -> io::Result<()> {
        let Some(forwarder) = self.forwarders.get_mut(&address) else {
            return Err(Self::unknown(address));
        };
        forwarder.close_listener().await;
        forwarder.stalled.cancel();
        Ok(())
    }

    /// Listen again on the same reserved port after `stop` closed this address. New client
    /// connections reach the same target; the old connections stay closed.
    pub(crate) async fn restart(&mut self, address: IpAddr) -> io::Result<()> {
        if self.forwarders.contains_key(&address) {
            return Err(io::Error::other(format!(
                "a forwarder already listens at {address}"
            )));
        }
        let listener = TcpListener::bind(SocketAddr::new(address, self.port)).await?;
        self.forwarders
            .insert(address, TcpForwarder::start(listener, self.target))
            .discarded("the address was absent before its listener was rebound");
        Ok(())
    }

    fn forwarder(&self, address: IpAddr) -> io::Result<&TcpForwarder> {
        self.forwarders
            .get(&address)
            .ok_or_else(|| Self::unknown(address))
    }

    fn unknown(address: IpAddr) -> io::Error {
        io::Error::other(format!("no forwarder listens at {address}"))
    }
}

impl Drop for TcpForwarders {
    fn drop(&mut self) {
        // Every listener has to close before the port goes back to the pool.
        self.forwarders.clear();
        release_test_ports(&[self.port]);
    }
}

impl TcpForwarder {
    fn start(listener: TcpListener, target: SocketAddr) -> Self {
        let accepted = StdArc::new(AtomicU64::new(0));
        let connections = CancellationToken::new();
        let stalled = CancellationToken::new();
        let listener = nervix_primitives::task::spawn(Self::accept(
            listener,
            target,
            accepted.clone(),
            Lifetimes {
                stopped: connections.clone(),
                stalled: stalled.clone(),
            },
        ));
        Self {
            accepted,
            connections,
            stalled,
            listener: Some(listener),
        }
    }

    /// End the accepting task once; its socket has closed when this returns.
    async fn close_listener(&mut self) {
        let Some(listener) = self.listener.take() else {
            return;
        };
        listener.abort();
        listener.await.discarded(
            "an aborted listener ends with a cancellation, and its socket closes with it",
        );
    }

    async fn accept(
        listener: TcpListener,
        target: SocketAddr,
        accepted: StdArc<AtomicU64>,
        lifetimes: Lifetimes,
    ) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Ok((downstream, _)) = listener.accept().await else {
                return;
            };
            accepted.fetch_add(1, Ordering::Relaxed);
            nervix_primitives::task::spawn(Self::forward(downstream, target, lifetimes.clone()));
        }
    }

    /// Carry one connection to `target` until either side closes it or the forwarder stops. Once
    /// the forwarder stalls, the connection carries nothing more but both of its sockets stay open
    /// until the forwarder stops.
    async fn forward(mut downstream: TcpStream, target: SocketAddr, lifetimes: Lifetimes) {
        let Ok(mut upstream) = TcpStream::connect(target).await else {
            return;
        };
        nervix_primitives::select! {
            _ = lifetimes.stopped.cancelled() => return,
            _ = copy_bidirectional(&mut downstream, &mut upstream) => return,
            _ = lifetimes.stalled.cancelled() => {}
        }
        lifetimes.stopped.cancelled().await;
        drop(downstream);
        drop(upstream);
    }
}

/// What ends the forwarding of one connection: the forwarder stopping closes it, and the forwarder
/// stalling stops its bytes while leaving it open.
#[derive(Debug, Clone)]
struct Lifetimes {
    stopped: CancellationToken,
    stalled: CancellationToken,
}

impl Drop for TcpForwarder {
    fn drop(&mut self) {
        if let Some(listener) = &self.listener {
            listener.abort();
        }
        self.connections.cancel();
    }
}
