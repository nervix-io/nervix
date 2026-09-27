//! TCP forwarders a scenario stands at chosen loopback addresses in front of one dependency.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One listener per chosen address, all on one port drawn from the harness pool, the
//!   connections each listener forwards to the dependency, how many connections each accepted, and
//!   stopping one listener together with its connections.
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
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use nervix_recovery::Discarded as _;
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::common::port_pool::{next_port, release_test_ports};

/// The forwarders in front of one dependency, and the pool port they share.
#[derive(Debug)]
pub(crate) struct TcpForwarders {
    port: u16,
    forwarders: BTreeMap<IpAddr, TcpForwarder>,
}

/// One listener and the connections it forwarded.
#[derive(Debug)]
struct TcpForwarder {
    accepted: Arc<AtomicU64>,
    /// Cancelled to close every connection the listener forwarded, when the forwarder stops.
    connections: CancellationToken,
    listener: JoinHandle<()>,
}

impl TcpForwarders {
    /// Listen on `addresses`, all on one port drawn from the pool, forwarding every accepted
    /// connection to `target`.
    pub(crate) async fn start(addresses: &[IpAddr], target: SocketAddr) -> io::Result<Self> {
        let port = next_port()?;
        // Constructed before anything binds, so a failed bind still gives the port back.
        let mut forwarders = Self {
            port,
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
        forwarder.listener.abort();
        (&mut forwarder.listener).await.discarded(
            "an aborted listener ends with a cancellation, and its socket closes with it",
        );
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
        let accepted = Arc::new(AtomicU64::new(0));
        let connections = CancellationToken::new();
        let listener = tokio::spawn(Self::accept(
            listener,
            target,
            accepted.clone(),
            connections.clone(),
        ));
        Self {
            accepted,
            connections,
            listener,
        }
    }

    async fn accept(
        listener: TcpListener,
        target: SocketAddr,
        accepted: Arc<AtomicU64>,
        connections: CancellationToken,
    ) {
        loop {
            tokio::task::consume_budget().await;
            let Ok((downstream, _)) = listener.accept().await else {
                return;
            };
            accepted.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(Self::forward(downstream, target, connections.clone()));
        }
    }

    /// Carry one connection to `target` until either side closes it or the forwarder stops.
    async fn forward(mut downstream: TcpStream, target: SocketAddr, stopped: CancellationToken) {
        let Ok(mut upstream) = TcpStream::connect(target).await else {
            return;
        };
        tokio::select! {
            _ = stopped.cancelled() => {}
            _ = copy_bidirectional(&mut downstream, &mut upstream) => {}
        }
    }
}

impl Drop for TcpForwarder {
    fn drop(&mut self) {
        self.listener.abort();
        self.connections.cancel();
    }
}
