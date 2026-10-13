//! A bounded TCP byte pacer for public follower catch-up checks.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** One scenario listener, its opaque inbound byte cadence and connection lifetimes.
//! - **Depends on.** Primitive sockets, tasks, cancellation and timers, and the cluster harness.
//! - **Must not know.** TLS records, HTTP/2 frames, Raft entries or product timeout policy.

use std::{io, net::SocketAddr, time::Duration};

use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{
        Arc, CancellationToken,
        atomic::{AtomicU64, Ordering},
    },
    task::{JoinHandle, JoinSet},
    time::sleep,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::Cluster;

#[derive(Debug)]
pub(super) struct PacedInterconnect {
    port: u16,
    maximum_forwarded: Arc<AtomicU64>,
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
        let maximum_forwarded = Arc::new(AtomicU64::new(0));
        let observed = maximum_forwarded.clone();
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
                        let observed = observed.clone();
                        let cancelled = cancelled.child_token();
                        connections.spawn(async move {
                            let transfer = forward(incoming, target, bytes_per_second, observed);
                            nervix_primitives::select! {
                                _ = cancelled.cancelled() => {},
                                _ = transfer => {},
                            }
                        });
                    }
                }
            }
            connections.shutdown().await;
        });
        Ok(Self {
            port,
            maximum_forwarded,
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
    observed: Arc<AtomicU64>,
) -> io::Result<()> {
    let outgoing = TcpStream::connect(target).await?;
    let (mut incoming_read, mut incoming_write) = incoming.into_split();
    let (mut outgoing_read, mut outgoing_write) = outgoing.into_split();
    let upstream = async {
        let mut buffer = [0_u8; 1024];
        let mut forwarded = 0_u64;
        loop {
            let count = incoming_read.read(&mut buffer).await?;
            if count == 0 {
                return outgoing_write.shutdown().await;
            }
            let count_bytes = u64::try_from(count).expect("the read fits its bounded buffer");
            // Delaying each bounded chunk before writing prevents an initial unpaced burst.
            let nanos = count_bytes
                .checked_mul(1_000_000_000)
                .expect("one bounded chunk's nanoseconds fit in u64")
                .div_ceil(bytes_per_second);
            sleep(Duration::from_nanos(nanos)).await;
            outgoing_write.write_all(&buffer[..count]).await?;
            forwarded = forwarded.saturating_add(count_bytes);
            observed.fetch_max(forwarded, Ordering::Relaxed);
        }
    };
    let downstream = tokio::io::copy(&mut outgoing_read, &mut incoming_write);
    tokio::try_join!(upstream, downstream)?;
    Ok(())
}

impl Cluster {
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

    pub(crate) fn maximum_paced_interconnect_bytes(&self, node_id: &str) -> u64 {
        self.nodes[node_id]
            .spec
            .paced_interconnect
            .as_ref()
            .expect("the scenario installed a byte pacer")
            .maximum_forwarded
            .load(Ordering::Relaxed)
    }
}
