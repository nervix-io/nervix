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
    let mut observer = NatsObserver::connect(&server.address.to_string(), SUBJECT)
        .await
        .assured("the real observing client opens its subscription");
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

/// Overflow is made deliberate on the actual driver's subscription, rather than inferred from a
/// missing sequence. The fixture still sends the full scenario burst before the observer drains.
#[nervix_primitives::test(flavor = "current_thread")]
async fn an_observer_records_the_drivers_slow_consumer_notifications() {
    let server = StandInNatsServer::start(BURST)
        .await
        .assured("the slow-consumer fixture binds a loopback port");
    let mut observer = NatsObserver::connect_with_options(
        &server.address.to_string(),
        SUBJECT,
        async_nats::ConnectOptions::new().subscription_capacity(1),
    )
    .await
    .assured("the observing client opens a deliberately small subscription");
    timeout(DELIVERY_BOUND, async {
        while observer.received() < BURST
            || event_count(&observer.describe_receipts(), "slow_consumer") == 0
        {
            consume_budget().await;
            yield_now().await;
        }
    })
    .await
    .assured("the driver receives the full burst and reports the controlled overflow");

    let receipts = observer.describe_receipts();
    assert_eq!(event_count(&receipts, "connected"), 1);
    assert!(event_count(&receipts, "slow_consumer") > 0);
    assert_eq!(event_count(&receipts, "disconnected"), 0);
    let first = observer
        .next()
        .await
        .assured("the bounded subscription retains the first delivery");
    assert_eq!(first.payload.as_ref(), b"1");
}

/// Ending the peer after every message reached the client must be observable without discarding
/// the buffered messages. This separates a real disconnection from an incomplete observer drain.
#[nervix_primitives::test(flavor = "current_thread")]
async fn a_disconnected_observer_keeps_its_buffered_burst_and_reports_the_connection_end() {
    let server = StandInNatsServer::start(BURST)
        .await
        .assured("the disconnect fixture binds a loopback port");
    let mut observer = NatsObserver::connect(&server.address.to_string(), SUBJECT)
        .await
        .assured("the observing client opens its subscription");
    timeout(DELIVERY_BOUND, async {
        while observer.received() < BURST {
            consume_budget().await;
            yield_now().await;
        }
    })
    .await
    .assured("the client buffers the full burst before its peer ends");
    drop(server);
    timeout(DELIVERY_BOUND, async {
        while event_count(&observer.describe_receipts(), "disconnected") == 0 {
            consume_budget().await;
            yield_now().await;
        }
    })
    .await
    .assured("ending the real TCP peer produces a driver disconnect notification");

    for sequence in 1..=BURST {
        consume_budget().await;
        let message = observer
            .next()
            .await
            .assured("the connection ending leaves every received message buffered");
        assert_eq!(message.payload.as_ref(), sequence.to_string().as_bytes());
    }
    let receipts = observer.describe_receipts();
    assert!(receipts.contains("32768 messages"));
    assert!(event_count(&receipts, "disconnected") > 0);
    assert_eq!(event_count(&receipts, "slow_consumer"), 0);
}

fn event_count(receipts: &str, event: &str) -> u64 {
    let prefix = format!("{event}=");
    let (_, remaining) = receipts
        .split_once(&prefix)
        .assured("the receipt diagnostic names this driver event");
    remaining
        .split(',')
        .next()
        .assured("the named event has a counter")
        .parse()
        .assured("the named event counter is an unsigned integer")
}

/// A NATS server that answers its one client's handshake and pings, and delivers a burst of
/// numbered messages to the first subscription the client opens.
pub(super) struct StandInNatsServer {
    pub(super) address: SocketAddr,
    _connection: AbortOnDropHandle<io::Result<()>>,
}

impl StandInNatsServer {
    async fn start(burst: u64) -> io::Result<Self> {
        Self::start_with_frames(burst, Self::burst).await
    }

    pub(super) async fn start_sequence(burst: usize) -> io::Result<Self> {
        let burst = u64::try_from(burst).assured("the fixture's bounded burst fits in u64");
        Self::start_with_frames(burst, Self::sequence_frames).await
    }

    async fn start_with_frames(
        burst: u64,
        frames: fn(&str, &str, u64) -> Vec<u8>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let connection = spawn(async move {
            let (stream, _) = listener.accept().await?;
            Self::serve(stream, burst, frames).await
        });
        Ok(Self {
            address,
            _connection: AbortOnDropHandle::new(connection),
        })
    }

    async fn serve(
        stream: TcpStream,
        burst: u64,
        frames: fn(&str, &str, u64) -> Vec<u8>,
    ) -> io::Result<()> {
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
            writer.write_all(&frames(subject, sid, burst)).await?;
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

    /// The complete JSON sequence and repeated headers checked by the scenario assertion.
    fn sequence_frames(subject: &str, sid: &str, burst: u64) -> Vec<u8> {
        let headers = "NATS/1.0\r\nroute: primary\r\nroute: columnar\r\n\r\n";
        let mut frames = Vec::new();
        for sequence in 1..=burst {
            let payload = format!("{{\"sequence\":{sequence}}}");
            let total = headers
                .len()
                .checked_add(payload.len())
                .assured("each bounded JSON message and header block fit in usize");
            let frame = format!(
                "HMSG {subject} {sid} {} {total}\r\n{headers}{payload}\r\n",
                headers.len(),
            );
            frames.extend_from_slice(frame.as_bytes());
        }
        frames
    }
}
