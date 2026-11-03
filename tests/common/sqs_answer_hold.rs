//! A forwarder a scenario stands in front of the SQS service to hold the answer that carries a
//! chosen message, and to count the SQS actions the requests it carries name.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One loopback listener, the connections it forwards to the service, the one answer it
//!   holds, and how many requests named each SQS action. It holds the first answer the service
//!   sends, once a scenario arms it, whose bytes carry the text it was told to watch for, from the
//!   read that brought the text until the scenario releases it.
//! - **Depends on.** Tokio's TCP sockets and I/O traits, and the boundary's tasks, cancellation and
//!   watch channel.
//! - **Must not know.** Nervix, or HTTP and the SQS protocol beyond the bytes an answer carries and
//!   the action a request's `x-amz-target` header line names.
//!
//! The service sends an answer once it has taken the messages in it off the queue, so a held answer
//! is a receive in flight: the messages it carries stay invisible to every other receive until the
//! client takes the answer or their visibility timeout passes. Requests, and every answer before or
//! after the held one, pass unchanged. A client that gives up on the held receive loses the answer
//! with its connection.

use std::{collections::BTreeMap, future::Future, io, net::SocketAddr};

use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{CancellationToken, watch},
    task::{AbortOnDropHandle, JoinHandle},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The most bytes one read of what the service sends takes.
const READ_BYTES: usize = 16 * 1024;
/// The most bytes of the watched text that can arrive in the read before the one that completes
/// it. A scenario watches for a short marker, so a longer tail only costs a few bytes of copying.
const CARRIED_TAIL_BYTES: usize = 256;
/// The most bytes of a request the forwarder keeps while it waits for the end of a line. A header
/// line is far shorter; a longer run without a line end is a body, which never names an action.
const REQUEST_LINE_BYTES: usize = 8 * 1024;
/// The header whose value names a request's SQS action, as the JSON protocol sends it.
const TARGET_HEADER: &str = "x-amz-target";
/// What every SQS action name in a target header begins with.
const TARGET_PREFIX: &str = "AmazonSQS.";

/// Which answer the forwarder holds.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Hold {
    /// Every answer passes.
    Passing,
    /// The next answer whose bytes carry `text` is held.
    Armed { text: Vec<u8> },
    /// An answer that carries the armed text is held until the scenario releases it.
    Holding,
}

/// A listener that forwards every client connection to the service, and holds the one answer a
/// scenario armed it for.
#[derive(Debug)]
pub(crate) struct SqsAnswerHold {
    local_addr: SocketAddr,
    hold: watch::Sender<Hold>,
    /// How many requests named each SQS action, by the action's name.
    actions: watch::Sender<BTreeMap<String, usize>>,
    /// Cancelled to close every connection the forwarder carried, when the forwarder stops.
    connections: CancellationToken,
    listener: JoinHandle<()>,
}

impl SqsAnswerHold {
    /// Listens on a loopback port and forwards every connection it accepts to `service`.
    pub(crate) async fn start(service: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let local_addr = listener.local_addr()?;
        let (hold, _) = watch::channel(Hold::Passing);
        let (actions, _) = watch::channel(BTreeMap::new());
        let connections = CancellationToken::new();
        let listener = nervix_primitives::task::spawn(Self::accept(
            listener,
            service,
            Carried {
                hold: hold.clone(),
                actions: actions.clone(),
            },
            connections.clone(),
        ));
        Ok(Self {
            local_addr,
            hold,
            actions,
            connections,
            listener,
        })
    }

    /// How many of the requests it carried named the SQS `action`, such as `DeleteMessageBatch`.
    pub(crate) fn requests_naming(&self, action: &str) -> usize {
        let actions = self.actions.borrow();
        match actions.get(action) {
            Some(requests) => *requests,
            None => 0,
        }
    }

    /// How many requests named each SQS action it carried, for a step's failure message.
    pub(crate) fn requests_by_action(&self) -> BTreeMap<String, usize> {
        self.actions.borrow().clone()
    }

    /// The loopback port clients connect to instead of the service's.
    pub(crate) fn local_port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Holds, from now on, the first answer the service sends whose bytes carry `text`.
    pub(crate) fn hold_the_answer_carrying(&self, text: Vec<u8>) {
        self.hold.send_replace(Hold::Armed { text });
    }

    /// Resolves once the forwarder holds the answer it was armed for.
    pub(crate) fn holding(&self) -> impl Future<Output = io::Result<()>> + use<> {
        let mut hold = self.hold.subscribe();
        async move {
            hold.wait_for(|hold| *hold == Hold::Holding)
                .await
                .map_err(io::Error::other)?;
            Ok(())
        }
    }

    /// Passes the held answer on once `ready` resolves, and every answer after it. The release
    /// waits on a task of its own, so a stalled runner task does not delay it; the returned future
    /// resolves once it released the answer, and dropping it abandons the release.
    pub(crate) fn release_once<R>(&self, ready: R) -> impl Future<Output = io::Result<()>> + use<R>
    where
        R: Future<Output = ()> + Send + 'static,
    {
        let hold = self.hold.clone();
        let release = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
            ready.await;
            hold.send_replace(Hold::Passing);
        }));
        async move { release.await.map_err(io::Error::other) }
    }

    async fn accept(
        listener: TcpListener,
        service: SocketAddr,
        carried: Carried,
        connections: CancellationToken,
    ) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Ok((client, _)) = listener.accept().await else {
                return;
            };
            nervix_primitives::task::spawn(Self::forward(
                client,
                service,
                carried.clone(),
                connections.clone(),
            ));
        }
    }

    /// Carries one connection to `service` until either side closes it or the forwarder stops.
    async fn forward(
        client: TcpStream,
        service: SocketAddr,
        carried: Carried,
        stopped: CancellationToken,
    ) {
        let Ok(service) = TcpStream::connect(service).await else {
            return;
        };
        let (mut client_read, mut client_write) = client.into_split();
        let (mut service_read, mut service_write) = service.into_split();
        nervix_primitives::select! {
            _ = stopped.cancelled() => {}
            _ = Self::forward_requests(&mut client_read, &mut service_write, &carried.actions) => {}
            _ = Self::forward_answers(&mut service_read, &mut client_write, &carried.hold) => {}
        }
    }

    /// Forwards what the client sends unchanged, counting the SQS action each request names in its
    /// `x-amz-target` header line.
    async fn forward_requests(
        client: &mut (impl AsyncRead + Unpin),
        service: &mut (impl AsyncWrite + Unpin),
        actions: &watch::Sender<BTreeMap<String, usize>>,
    ) -> io::Result<()> {
        let mut buffer = vec![0_u8; READ_BYTES];
        // The bytes since the last line end, so a header line the client's writes split is still
        // read whole.
        let mut line = Vec::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            let read = client.read(&mut buffer).await?;
            if read == 0 {
                return service.shutdown().await;
            }
            let bytes = &buffer[..read];
            for byte in bytes {
                line.push(*byte);
                if line.ends_with(b"\r\n") {
                    if let Some(action) = Self::named_action(&line) {
                        actions.send_modify(|actions| {
                            *actions.entry(action).or_default() += 1;
                        });
                    }
                    line.clear();
                } else if line.len() > REQUEST_LINE_BYTES {
                    line.clear();
                }
            }
            service.write_all(bytes).await?;
        }
    }

    /// The SQS action a request's header `line` names, where it is the `x-amz-target` header.
    fn named_action(line: &[u8]) -> Option<String> {
        let line = std::str::from_utf8(line).ok()?;
        let (name, value) = line.split_once(':')?;
        if !name.trim().eq_ignore_ascii_case(TARGET_HEADER) {
            return None;
        }
        let target = value.trim();
        let action = target.strip_prefix(TARGET_PREFIX).unwrap_or(target);
        Some(action.to_string())
    }

    /// Forwards what the service sends, holding the read that brings the armed text, and everything
    /// after it on this connection, until the scenario releases the hold.
    async fn forward_answers(
        service: &mut (impl AsyncRead + Unpin),
        client: &mut (impl AsyncWrite + Unpin),
        hold: &watch::Sender<Hold>,
    ) -> io::Result<()> {
        let mut buffer = vec![0_u8; READ_BYTES];
        // The last bytes of the previous read, so a text the service's writes split is still found.
        let mut carried = Vec::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            let read = service.read(&mut buffer).await?;
            if read == 0 {
                return client.shutdown().await;
            }
            let bytes = &buffer[..read];
            carried.extend_from_slice(bytes);
            if Self::takes_the_hold(hold, &carried) {
                let mut released = hold.subscribe();
                released
                    .wait_for(|hold| *hold != Hold::Holding)
                    .await
                    .map_err(io::Error::other)?;
            }
            client.write_all(bytes).await?;
            if let Some(passed) = carried.len().checked_sub(CARRIED_TAIL_BYTES) {
                carried.drain(..passed);
            }
        }
    }

    /// Whether `bytes` carry the armed text, in which case the forwarder now holds them. Only one
    /// answer, on one connection, takes an armed hold.
    fn takes_the_hold(hold: &watch::Sender<Hold>, bytes: &[u8]) -> bool {
        hold.send_if_modified(|hold| {
            let Hold::Armed { text } = hold else {
                return false;
            };
            // What one read and its carried tail hold is a few kilobytes at most, so a scan bounds
            // itself.
            let carried = bytes
                .windows(text.len())
                .any(|window| window == text.as_slice());
            if !carried {
                return false;
            }
            *hold = Hold::Holding;
            true
        })
    }
}

/// What every connection the forwarder carries shares with the scenario: the answer hold and the
/// count of the actions requests named.
#[derive(Debug, Clone)]
struct Carried {
    hold: watch::Sender<Hold>,
    actions: watch::Sender<BTreeMap<String, usize>>,
}

impl Drop for SqsAnswerHold {
    fn drop(&mut self) {
        self.listener.abort();
        self.connections.cancel();
    }
}
