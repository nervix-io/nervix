//! A bounded TCP byte pacer for public follower catch-up checks.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** One scenario listener, its opaque inbound byte cadence and connection lifetimes.
//! - **Depends on.** Primitive sockets, tasks, cancellation and timers, and the cluster harness.
//! - **Must not know.** TLS records, HTTP/2 frames, Raft entries or product timeout policy.

use std::{io, net::SocketAddr};

use meticulous::OptionExt as _;
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{
        Arc, CancellationToken,
        atomic::{AtomicU64, Ordering},
    },
    task::{JoinHandle, JoinSet},
    time::{Instant, sleep_until},
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::{Cluster, byte_pace::BytePace, paced_connection::PacedConnection};

#[derive(Debug, Default)]
struct TransferCounts {
    accepted: AtomicU64,
    connected: AtomicU64,
    connect_failures: AtomicU64,
    transfer_failures: AtomicU64,
    cancelled: AtomicU64,
    completed: AtomicU64,
    total_forwarded: AtomicU64,
    maximum_forwarded: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PacedInterconnectObservation {
    pub(crate) accepted: u64,
    pub(crate) connected: u64,
    pub(crate) connect_failures: u64,
    pub(crate) transfer_failures: u64,
    pub(crate) cancelled: u64,
    pub(crate) completed: u64,
    pub(crate) total_forwarded: u64,
    pub(crate) maximum_forwarded: u64,
}

impl TransferCounts {
    fn observation(&self) -> PacedInterconnectObservation {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        PacedInterconnectObservation {
            accepted: load(&self.accepted),
            connected: load(&self.connected),
            connect_failures: load(&self.connect_failures),
            transfer_failures: load(&self.transfer_failures),
            cancelled: load(&self.cancelled),
            completed: load(&self.completed),
            total_forwarded: load(&self.total_forwarded),
            maximum_forwarded: load(&self.maximum_forwarded),
        }
    }
}

#[derive(Debug)]
pub(super) struct PacedInterconnect {
    port: u16,
    counts: Arc<TransferCounts>,
    release_pacing: CancellationToken,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl PacedInterconnect {
    async fn start(
        listen: SocketAddr,
        target: SocketAddr,
        bytes_per_second: u64,
    ) -> io::Result<Self> {
        if bytes_per_second == 0 {
            return Err(io::Error::other("a byte pacer needs a nonzero rate"));
        }
        let listener = TcpListener::bind(listen).await?;
        let port = listener.local_addr()?.port();
        let counts = Arc::new(TransferCounts::default());
        let observed = counts.clone();
        let release_pacing = CancellationToken::new();
        let released = release_pacing.clone();
        let cancellation = CancellationToken::new();
        let cancelled = cancellation.clone();
        let task = nervix_primitives::task::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    _ = cancelled.cancelled() => break,
                    _ = connections.join_next(), if !connections.is_empty() => {},
                    accepted = listener.accept() => {
                        let Ok((incoming, _)) = accepted else { break };
                        observed.accepted.fetch_add(1, Ordering::Relaxed);
                        let observed = observed.clone();
                        let cancelled = cancelled.child_token();
                        let released = released.clone();
                        connections.spawn(async move {
                            let transfer = forward(incoming, target, bytes_per_second, observed.clone(), released);
                            nervix_primitives::select! {
                                _ = cancelled.cancelled() => {
                                    observed.cancelled.fetch_add(1, Ordering::Relaxed);
                                },
                                result = transfer => match result {
                                    Ok(()) => { observed.completed.fetch_add(1, Ordering::Relaxed); },
                                    Err(error) => {
                                        observed.transfer_failures.fetch_add(1, Ordering::Relaxed);
                                        tracing::debug!(%target, %error, "paced interconnect transfer ended");
                                    }
                                },
                            }
                        });
                    }
                }
            }
            connections.shutdown().await;
        });
        Ok(Self {
            port,
            counts,
            release_pacing,
            cancellation,
            task,
        })
    }

    pub(super) fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for PacedInterconnect {
    fn drop(&mut self) {
        self.cancellation.cancel();
        // Dropping the listener's JoinSet also aborts every connection it owns.
        self.task.abort();
    }
}

async fn forward(
    incoming: TcpStream,
    target: SocketAddr,
    bytes_per_second: u64,
    observed: Arc<TransferCounts>,
    released: CancellationToken,
) -> io::Result<()> {
    let PacedConnection { incoming, outgoing } = PacedConnection::connect(incoming, target)
        .await
        .inspect_err(|_| {
        observed.connect_failures.fetch_add(1, Ordering::Relaxed);
    })?;
    observed.connected.fetch_add(1, Ordering::Relaxed);
    let (mut incoming_read, mut incoming_write) = incoming.into_split();
    let (mut outgoing_read, mut outgoing_write) = outgoing.into_split();
    let upstream = async {
        let mut buffer = [0_u8; 1024];
        let mut forwarded = 0_u64;
        let mut pace = BytePace::new(bytes_per_second);
        loop {
            nervix_primitives::task::consume_budget().await;
            let count = incoming_read.read(&mut buffer).await?;
            if count == 0 {
                return outgoing_write.shutdown().await;
            }
            // Delaying each bounded chunk before writing prevents an initial unpaced burst.
            if !released.is_cancelled() {
                nervix_primitives::select! {
                    _ = released.cancelled() => {},
                    _ = sleep_until(pace.deadline(count, Instant::now())) => {},
                }
            }
            outgoing_write.write_all(&buffer[..count]).await?;
            let count_bytes = u64::try_from(count).expect("the read fits its bounded buffer");
            forwarded = forwarded.saturating_add(count_bytes);
            observed
                .total_forwarded
                .fetch_add(count_bytes, Ordering::Relaxed);
            observed
                .maximum_forwarded
                .fetch_max(forwarded, Ordering::Relaxed);
        }
    };
    let downstream = tokio::io::copy(&mut outgoing_read, &mut incoming_write);
    tokio::try_join!(upstream, downstream)?;
    Ok(())
}

impl Cluster {
    /// Retains only the release signal; dropping the cluster still ends the pacer's sockets.
    pub(crate) fn interconnect_pacing_release(&self, node_id: &str) -> CancellationToken {
        self.nodes[node_id]
            .spec
            .paced_interconnect
            .as_ref()
            .assured("the scenario installed its byte pacer before observing fault release")
            .release_pacing
            .clone()
    }

    /// Releases this scenario's byte fault on existing and future connections, preserving their
    /// sockets and every byte already in flight.
    pub(crate) fn release_interconnect_pacing(&self, node_id: &str) {
        self.nodes[node_id]
            .spec
            .paced_interconnect
            .as_ref()
            .assured("the scenario installed its byte pacer before releasing the cadence")
            .release_pacing
            .cancel();
    }

    /// Pace a stopped node's existing advertised endpoint, forwarding to a private listener.
    pub(crate) async fn pace_stopped_node_interconnect(
        &mut self,
        node_id: &str,
        bytes_per_second: u64,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| io::Error::other("unknown node"))?;
        if handle.task.is_running() || handle.spec.paced_interconnect.is_some() {
            return Err(io::Error::other(
                "pace a stopped node once before it restarts",
            ));
        }
        let listen = handle.spec.interconnect_listen_addr();
        // Keep the advertised port reserved: existing peer routes must reach this pacer, and
        // another scenario must never claim an endpoint those peers may still dial.
        handle.spec.reallocate_interconnect_ports()?;
        let target = handle.spec.interconnect_listen_addr();
        handle.spec.paced_interconnect =
            Some(PacedInterconnect::start(listen, target, bytes_per_second).await?);
        Ok(())
    }

    pub(crate) fn paced_interconnect_observation(
        &self,
        node_id: &str,
    ) -> PacedInterconnectObservation {
        self.nodes[node_id]
            .spec
            .paced_interconnect
            .as_ref()
            .expect("the scenario installed a byte pacer")
            .counts
            .observation()
    }
}
