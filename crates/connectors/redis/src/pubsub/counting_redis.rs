//! A loopback stand-in for Redis that answers command connections and counts them.
//!
//! Outside the layer order: a test harness.
//!
//! - **Owns.** One loopback listener, how many connections it accepted and how many of them are
//!   still open, and the answers it gives the commands a pooled command connection sends.
//! - **Depends on.** Redis's asynchronous RESP parser, and the primitives boundary's sockets, watch
//!   channel and tasks.
//! - **Must not know.** The command pool, the host, Pub/Sub, or a real Redis server.

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use combine::{
    parser::combinator::AnySendSyncPartialState,
    stream::{Decoder, PointerOffset},
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{AbortOnDropHandle, consume_budget, spawn},
    time::timeout,
};
use redis::Value;
use tokio::io::AsyncWriteExt as _;

/// How many connections the stand-in accepted, and how many of those are still open.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ConnectionCounts {
    pub(crate) accepted: usize,
    pub(crate) open: usize,
}

/// A loopback Redis stand-in that answers `CLIENT`, `PING` and `PUBLISH` and counts the connections
/// clients open to it.
pub(crate) struct CountingRedis {
    address: SocketAddr,
    counts: watch::Receiver<ConnectionCounts>,
    _listener: AbortOnDropHandle<()>,
}

impl CountingRedis {
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("a loopback port is available to tests");
        let address = listener
            .local_addr()
            .assured("a bound listener has an address");
        let (counter, counts) = watch::channel(ConnectionCounts::default());
        let accepting = spawn(async move {
            loop {
                consume_budget().await;
                let Ok((connection, _)) = listener.accept().await else {
                    return;
                };
                counter.send_modify(|counts| {
                    counts.accepted = counts
                        .accepted
                        .checked_add(1)
                        .assured("a test opens far fewer connections than usize counts");
                    counts.open = counts
                        .open
                        .checked_add(1)
                        .assured("no more connections are open than were accepted");
                });
                let closed = counter.clone();
                spawn(async move {
                    Self::answer(connection).await;
                    closed.send_modify(|counts| {
                        counts.open = counts
                            .open
                            .checked_sub(1)
                            .assured("a connection closes only after it was counted open");
                    });
                });
            }
        });
        Self {
            address,
            counts,
            _listener: AbortOnDropHandle::new(accepting),
        }
    }

    /// The address a Redis client connects to.
    pub(crate) fn url(&self) -> String {
        format!("redis://{}", self.address)
    }

    pub(crate) fn counts(&self) -> ConnectionCounts {
        *self.counts.borrow()
    }

    /// Wait up to `within` until the counts satisfy `reached`, and return them. `expected` says
    /// what the check waits for when the counts never get there.
    pub(crate) async fn reach(
        &self,
        within: Duration,
        expected: &str,
        reached: impl FnMut(&ConnectionCounts) -> bool,
    ) -> ConnectionCounts {
        let mut counts = self.counts.clone();
        let observed = timeout(within, counts.wait_for(reached)).await;
        let Ok(observed) = observed else {
            panic!(
                "the stand-in's connections did not reach {expected} within {within:?}: {:?}",
                self.counts()
            );
        };
        *observed.assured("the stand-in holds its counter for as long as it listens")
    }

    /// Answer every command `connection` sends until the client closes it.
    async fn answer(connection: TcpStream) {
        let (mut input, mut output) = tokio::io::split(connection);
        let mut decoder: Decoder<AnySendSyncPartialState, PointerOffset<[u8]>> = Decoder::new();
        loop {
            consume_budget().await;
            let Ok(request) = redis::parse_redis_value_async(&mut decoder, &mut input).await else {
                return;
            };
            let answer = Self::answer_to(&request);
            if output.write_all(answer).await.is_err() {
                return;
            }
        }
    }

    /// The answer to one command, by its name: the setup's `CLIENT SETINFO`, the checkout's
    /// `PING` and a publisher's `PUBLISH`.
    fn answer_to(request: &Value) -> &'static [u8] {
        let Value::Array(arguments) = request else {
            return b"-ERR the request is not a command\r\n";
        };
        let Some(Value::BulkString(name)) = arguments.first() else {
            return b"-ERR the command has no name\r\n";
        };
        if name.eq_ignore_ascii_case(b"PING") {
            b"+PONG\r\n"
        } else if name.eq_ignore_ascii_case(b"PUBLISH") {
            b":0\r\n"
        } else if name.eq_ignore_ascii_case(b"CLIENT") {
            b"+OK\r\n"
        } else {
            b"-ERR unknown command\r\n"
        }
    }
}
