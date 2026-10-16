//! Regressions for observing a core NATS subject.
//!
//! Layer: test harness.
//! - **Owns.** A stand-in NATS server that delivers one burst of numbered messages to the
//!   subscription its one client opens, and a stand-in step that drains that subscription on the
//!   runner task.
//! - **Depends on.** The NATS observer, the `async-nats` client, loopback sockets and a
//!   current-thread runtime.
//! - **Must not know.** What a scenario publishes, emits or asserts.

use std::{
    future::poll_fn,
    io,
    net::{Ipv4Addr, SocketAddr},
    pin::pin,
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    task::{AbortOnDropHandle, consume_budget, spawn, yield_now},
    time::timeout,
};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

use crate::nats_observer::NatsObserver;

/// The inventory the NATS emission scenarios publish and observe.
const BURST: u64 = 32_768;

/// The fewest messages a step must take on average in one poll of the runner task once the
/// observing client holds them. A loaded runner task comes back to a step only after it has polled
/// every other woken scenario, so a step that took one message per poll waited for that once for
/// every message.
const LEAST_MESSAGES_PER_POLL: u64 = 16;

/// The most polls a step may need to drain the whole burst.
const MOST_POLLS_FOR_THE_BURST: u64 = BURST / LEAST_MESSAGES_PER_POLL;

/// How long the stand-in server may take to hand the whole burst to the client.
const DELIVERY_BOUND: Duration = Duration::from_secs(60);

const SUBJECT: &str = "observed";

/// What the stand-in server announces when a client connects.
const SERVER_INFO: &[u8] = b"INFO {\"server_id\":\"stand-in\",\"server_name\":\"stand-in\",\
    \"version\":\"2.11.0\",\"proto\":1,\"headers\":true,\"max_payload\":1048576}\r\n";

/// The runner task is the only thread of a current-thread runtime here, as Cucumber's is the one
/// task that polls every scenario. The observing client's task and the stand-in server share that
/// thread, so a message the step waits for on another task costs the step a poll.
#[nervix_primitives::test(flavor = "current_thread")]
async fn a_step_takes_buffered_deliveries_without_a_poll_per_message() {
    let server = StandInNatsServer::start(BURST)
        .await
        .assured("a loopback listener binds an ephemeral port");
    let client = async_nats::connect(server.address.to_string())
        .await
        .assured("the stand-in server answers the client's handshake");
    let mut observer = NatsObserver::subscribe(&client, SUBJECT)
        .await
        .assured("the client is connected to the stand-in server");
    timeout(DELIVERY_BOUND, async {
        while observer.received() < BURST {
            consume_budget().await;
            yield_now().await;
        }
    })
    .await
    .assured("the stand-in server delivers the whole burst within its bound");

    let mut polls: u64 = 0;
    {
        let mut drain = pin!(async {
            for sequence in 1..=BURST {
                consume_budget().await;
                let message = observer
                    .next()
                    .await
                    .assured("the subscription holds the whole burst the client received");
                assert_eq!(
                    message.payload.as_ref(),
                    sequence.to_string().as_bytes(),
                    "the observer delivers the burst in the order the server sent it"
                );
            }
        });
        poll_fn(|context| {
            polls = polls
                .checked_add(1)
                .assured("a drain of the burst is polled far fewer than u64::MAX times");
            drain.as_mut().poll(context)
        })
        .await;
    }
    assert!(
        polls <= MOST_POLLS_FOR_THE_BURST,
        "draining {BURST} messages the client had already received took {polls} polls of the \
         runner task, more than {MOST_POLLS_FOR_THE_BURST}"
    );
}

/// A NATS server that answers its one client's handshake and pings, and delivers a burst of
/// numbered messages to the first subscription the client opens.
struct StandInNatsServer {
    address: SocketAddr,
    _connection: AbortOnDropHandle<io::Result<()>>,
}

impl StandInNatsServer {
    async fn start(burst: u64) -> io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let connection = spawn(async move {
            let (stream, _) = listener.accept().await?;
            Self::serve(stream, burst).await
        });
        Ok(Self {
            address,
            _connection: AbortOnDropHandle::new(connection),
        })
    }

    async fn serve(stream: TcpStream, burst: u64) -> io::Result<()> {
        let (reader, mut writer) = stream.into_split();
        writer.write_all(SERVER_INFO).await?;
        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines.next_line().await? {
            consume_budget().await;
            if line == "PING" {
                writer.write_all(b"PONG\r\n").await?;
                continue;
            }
            let Some(subscription) = line.strip_prefix("SUB ") else {
                continue;
            };
            let Some((subject, sid)) = subscription.split_once(' ') else {
                return Err(io::Error::other(format!(
                    "the client opened a subscription without a subject and id: {subscription:?}"
                )));
            };
            writer.write_all(&Self::burst(subject, sid, burst)).await?;
        }
        Ok(())
    }

    /// `burst` messages on `subject` for subscription `sid`, each carrying its sequence number.
    fn burst(subject: &str, sid: &str, burst: u64) -> Vec<u8> {
        let mut frames = Vec::new();
        for sequence in 1..=burst {
            let payload = sequence.to_string();
            let frame = format!("MSG {subject} {sid} {}\r\n{payload}\r\n", payload.len());
            frames.extend_from_slice(frame.as_bytes());
        }
        frames
    }
}
