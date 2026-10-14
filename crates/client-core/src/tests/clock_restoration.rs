//! Following a domain clock while the client moves its attachment between exchanges: an attachment
//! a redirect moves, a detach that races the clock's restoration, and a restoration no session
//! answers.
//!
//! Each exchange is served by hand through the reader the client runs on it, so every reply and
//! frame the test answers reaches the client's clocks as a server's would. The test installs a
//! replacement exchange itself, the way a reconnect does, and holds the client's exchange for as
//! long as it wants the replaced exchange to stay open.

use std::{num::NonZeroU64, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    ClientFrame, ClientMessage, ClientRequest, DomainClockAttachDisposition,
    DomainClockAttachOutcome, DomainClockDetachDisposition, DomainClockDetachOutcome,
    DomainClockTicked, EncodedFrame, ReplyBody, RequestId, ServerFrame,
};
use nervix_models::{
    DomainClockObservation, DomainClockObservedState, DomainClockPeriod, DomainClockSkew,
    DomainClockState, DomainClockTickObservation, DomainTimeRate, PacedDomainClock, Timestamp,
};
use nervix_primitives::sync::{Arc, blocking::Mutex, mpsc};

use super::{DEADLINE, client_on, detached_exchange, domain, reply_frame, verified};
use crate::{
    Client, ClientError, DomainClockEvent, DomainClockInterruption, DomainClockRestorationFailure,
    exchange::{Exchange, ExchangeReader, PendingReplies, ReaderFlow, SESSION_LIMITS},
};

/// One exchange the test serves: the frames the client sends on it, and the reader the client
/// would run on it, which applies what the test answers before it completes the request.
struct ServedExchange {
    requests: mpsc::Receiver<EncodedFrame<ClientFrame>>,
    reader: ExchangeReader,
}

impl ServedExchange {
    /// A client whose first exchange the test serves, with the domain `tenant` selected. The
    /// client knows no server, so a session it loses is not opened again.
    fn connect() -> (Client, Self) {
        let (frames, requests) = mpsc::channel(8);
        let pending = Arc::new(Mutex::new(PendingReplies::new()));
        let exchange = detached_exchange(frames, pending.clone());
        let reader =
            ExchangeReader::new(pending, exchange.sinks.clone(), exchange.generation.clone());
        let client = client_on(exchange, Some(domain("tenant")));
        (client, Self { requests, reader })
    }

    /// A replacement exchange of `client`, opened the way a reconnect opens one, which the test
    /// installs.
    fn reopen(client: &Client) -> (Exchange, Self) {
        let (frames, requests) = mpsc::channel(8);
        let pending = Arc::new(Mutex::new(PendingReplies::new()));
        let mut exchange = detached_exchange(frames, pending.clone());
        let sinks = client.inner.events.sinks.clone();
        exchange.generation = sinks.begin_generation();
        exchange.sinks = sinks.clone();
        let reader = ExchangeReader::new(pending, sinks, exchange.generation.clone());
        (exchange, Self { requests, reader })
    }

    /// The next request the client sends on the exchange, as the server decodes it.
    async fn next_request(&mut self) -> ClientMessage {
        let frame = nervix_primitives::time::timeout(DEADLINE, self.requests.recv())
            .await
            .assured("the client sends its request within the deadline")
            .assured("the client keeps its exchange open");
        Self::decode(frame)
    }

    /// A request the client has already sent on the exchange and the test has not read yet.
    fn sent_request(&mut self) -> Option<ClientMessage> {
        let frame = self.requests.try_recv().ok()?;
        Some(Self::decode(frame))
    }

    fn decode(frame: EncodedFrame<ClientFrame>) -> ClientMessage {
        let frame = frame
            .verify(&SESSION_LIMITS)
            .assured("a request the client encoded verifies");
        ClientMessage::decode(&frame).assured("a request the client encoded decodes")
    }

    /// Answers a request the way the server's reply arrives on the exchange.
    async fn answer(&mut self, request_id: RequestId, body: ReplyBody) {
        let flow = self.reader.route(reply_frame(request_id, body)).await;
        assert_eq!(flow, ReaderFlow::Continue);
    }

    /// Sends an unsolicited frame on the exchange.
    async fn send(&mut self, frame: EncodedFrame<ServerFrame>) {
        let flow = self.reader.route(verified(frame)).await;
        assert_eq!(flow, ReaderFlow::Continue);
    }
}

fn paced() -> DomainClockObservation {
    DomainClockObservation {
        generation: 1,
        state: DomainClockObservedState::Paced(PacedDomainClock {
            period: DomainClockPeriod::from_nanos(
                NonZeroU64::new(100_000_000).assured("a positive period"),
            ),
            skew: DomainClockSkew::from_nanos(10_000_000),
            mapping: DomainClockState::new(
                Timestamp::from_unix_nanos(1_000),
                Timestamp::from_unix_nanos(2_000),
                DomainTimeRate::ONE,
            ),
        }),
    }
}

fn unpaced() -> DomainClockObservation {
    DomainClockObservation {
        generation: 1,
        state: DomainClockObservedState::Unpaced,
    }
}

/// The tick `tick_id` of the paced clock's generation.
fn tick(tick_id: u64) -> DomainClockTickObservation {
    let elapsed = i64::try_from(tick_id)
        .assured("a test tick id fits a timestamp")
        .checked_mul(100_000_000)
        .assured("a test tick boundary fits a timestamp");
    DomainClockTickObservation {
        generation: 1,
        tick_id,
        logical_boundary: Timestamp::from_unix_nanos(elapsed),
        authority_utc: Timestamp::from_unix_nanos(elapsed),
        serving_logical: Timestamp::from_unix_nanos(elapsed),
    }
}

fn ticked(tick_id: u64) -> DomainClockTicked {
    DomainClockTicked {
        domain: domain("tenant"),
        tick: tick(tick_id),
    }
}

fn tick_frame(tick_id: u64) -> EncodedFrame<ServerFrame> {
    ticked(tick_id)
        .encode(&SESSION_LIMITS)
        .assured("a tick frame fits the default limits")
}

fn attached(clock: DomainClockObservation) -> ReplyBody {
    ReplyBody::DomainClockAttach(DomainClockAttachOutcome {
        disposition: DomainClockAttachDisposition::Attached {
            domain: domain("tenant"),
            clock,
        },
        message: String::new(),
    })
}

fn refused_attach(message: &str) -> ReplyBody {
    ReplyBody::DomainClockAttach(DomainClockAttachOutcome {
        disposition: DomainClockAttachDisposition::Failed,
        message: message.to_string(),
    })
}

fn detached(disposition: DomainClockDetachDisposition) -> ReplyBody {
    ReplyBody::DomainClockDetach(DomainClockDetachOutcome {
        disposition,
        message: String::new(),
    })
}

/// Attaches `client` to the clock of `tenant` on `exchange`, which answers with `clock`.
async fn attach(client: &Client, exchange: &mut ServedExchange, clock: DomainClockObservation) {
    let attaching = client.clone();
    let attach = nervix_primitives::task::spawn(async move {
        attaching.attach_domain_clock(domain("tenant")).await
    });
    let request = exchange.next_request().await;
    assert!(matches!(
        request.request,
        ClientRequest::AttachDomainClock(_)
    ));
    exchange.answer(request.request_id, attached(clock)).await;
    let outcome = attach
        .await
        .assured("the attach task completes")
        .assured("the attach is answered");
    assert!(matches!(
        outcome.disposition,
        DomainClockAttachDisposition::Attached { .. }
    ));
}

/// The next clock event of `client`, which arrives within the deadline.
async fn next_event(client: &Client) -> DomainClockEvent {
    nervix_primitives::time::timeout(DEADLINE, client.next_domain_clock_event())
        .await
        .assured("a clock event arrives within the deadline")
        .assured("the client reads its clock events")
}

/// A redirect moves an attachment: the new session's attach reply arrives while the old session
/// still holds it. The tick the old session delivered stays the clock's tick, so the clock a
/// caller reads after taking that tick holds it, and a tick of the new session that does not
/// advance it reports nothing.
#[nervix_primitives::test]
async fn an_attachment_moved_before_its_old_session_ends_keeps_the_tick_it_reported() {
    let (client, mut old) = ServedExchange::connect();
    attach(&client, &mut old, paced()).await;
    old.send(tick_frame(5)).await;

    let (replacement, mut new) = ServedExchange::reopen(&client);
    // The old exchange stays the client's until the test lets the install replace it.
    let current = client.inner.exchange.lock().await;
    let installing = {
        let client = client.clone();
        nervix_primitives::task::spawn(async move { client.install(replacement).await })
    };
    let restore = new.next_request().await;
    assert!(
        matches!(restore.request, ClientRequest::AttachDomainClock(_)),
        "the new session attaches the followed clock first"
    );
    new.answer(restore.request_id, attached(paced())).await;

    assert_eq!(
        next_event(&client).await,
        DomainClockEvent::Ticked(ticked(5)),
        "the tick the old session delivered is still unread"
    );
    let read = client
        .domain_clock(&domain("tenant"))
        .assured("the moved clock is still followed");
    assert_eq!(
        read.latest_tick(),
        Some(&tick(5)),
        "a read is never older than an event the caller took"
    );

    drop(current);
    installing.await.assured("the install completes");
    old.send(tick_frame(9)).await;
    new.send(tick_frame(5)).await;
    new.send(tick_frame(6)).await;
    assert_eq!(
        next_event(&client).await,
        DomainClockEvent::Ticked(ticked(6)),
        "only a tick of the new session that advances the clock is reported"
    );
    assert!(client.inner.events.sinks.clocks.take_event().is_none());
    assert_eq!(
        client
            .domain_clock(&domain("tenant"))
            .assured("the moved clock is still followed")
            .latest_tick(),
        Some(&tick(6))
    );
}

/// A detach the caller sends while a refused restoration waits to be sent again is answered
/// first: the session does not hold the clock. The client neither follows the clock afterwards
/// nor attaches it again, so a session that answers whatever it receives holds no attachment of
/// it.
#[nervix_primitives::test(start_paused = true)]
async fn a_detach_sent_while_a_refused_restoration_waits_releases_the_clock_for_good() {
    let (client, mut old) = ServedExchange::connect();
    attach(&client, &mut old, unpaced()).await;
    let (replacement, mut new) = ServedExchange::reopen(&client);
    client.install(replacement).await;
    assert_eq!(
        next_event(&client).await,
        DomainClockEvent::Interrupted(DomainClockInterruption {
            domain: domain("tenant"),
        })
    );
    let restore = new.next_request().await;
    assert!(matches!(
        restore.request,
        ClientRequest::AttachDomainClock(_)
    ));
    new.answer(
        restore.request_id,
        refused_attach("the session holds a transaction"),
    )
    .await;
    assert_eq!(
        next_event(&client).await,
        DomainClockEvent::RestorationFailed(DomainClockRestorationFailure {
            domain: domain("tenant"),
            message: "the session holds a transaction".to_string(),
            retry_after: Duration::from_secs(1),
        })
    );

    let detaching = {
        let client = client.clone();
        nervix_primitives::task::spawn(
            async move { client.detach_domain_clock(domain("tenant")).await },
        )
    };
    let detach = new.next_request().await;
    assert!(matches!(
        detach.request,
        ClientRequest::DetachDomainClock(_)
    ));
    // The paused clock moves past the refused restoration's wait while the detach waits for its
    // answer, once every task has run as far as it can.
    nervix_primitives::time::sleep(Duration::from_secs(2)).await;
    new.answer(
        detach.request_id,
        detached(DomainClockDetachDisposition::NotAttached(domain("tenant"))),
    )
    .await;
    let outcome = detaching
        .await
        .assured("the detach task completes")
        .assured("the detach is answered");
    assert_eq!(
        outcome.disposition,
        DomainClockDetachDisposition::NotAttached(domain("tenant"))
    );

    // The session answers what the client sent after the detach the way a session does: it
    // attaches a clock it does not hold.
    let mut attached_by_the_session = false;
    while let Some(request) = new.sent_request() {
        nervix_primitives::task::consume_budget().await;
        assert!(
            matches!(request.request, ClientRequest::AttachDomainClock(_)),
            "the client sends nothing but a restoration here: {request:?}"
        );
        new.answer(request.request_id, attached(unpaced())).await;
        attached_by_the_session = true;
    }
    assert_eq!(
        client.domain_clock(&domain("tenant")),
        None,
        "the client follows no clock it was told to release"
    );
    assert!(
        !attached_by_the_session,
        "the session holds no attachment of a clock the client released"
    );
    nervix_primitives::time::sleep(Duration::from_secs(60)).await;
    assert!(
        new.sent_request().is_none(),
        "nothing attaches the released clock again"
    );
}

/// A reconnect installs a new exchange while a detach waits for the old session's answer, and the
/// old session answers before it ends. The new session does not attach the clock that detach
/// released.
#[nervix_primitives::test(start_paused = true)]
async fn a_detach_the_old_session_answers_during_a_reconnect_stays_released_on_the_new_session() {
    let (client, mut old) = ServedExchange::connect();
    attach(&client, &mut old, unpaced()).await;
    let detaching = {
        let client = client.clone();
        nervix_primitives::task::spawn(
            async move { client.detach_domain_clock(domain("tenant")).await },
        )
    };
    let detach = old.next_request().await;
    assert!(matches!(
        detach.request,
        ClientRequest::DetachDomainClock(_)
    ));

    let (replacement, mut new) = ServedExchange::reopen(&client);
    // The old exchange stays the client's until the test lets the install replace it.
    let current = client.inner.exchange.lock().await;
    let installing = {
        let client = client.clone();
        nervix_primitives::task::spawn(async move { client.install(replacement).await })
    };
    // The paused clock moves only once every task waits, so the install has gone as far as it can
    // before it replaces the old exchange.
    nervix_primitives::time::sleep(Duration::from_millis(1)).await;
    old.answer(
        detach.request_id,
        detached(DomainClockDetachDisposition::Detached(domain("tenant"))),
    )
    .await;
    let outcome = detaching
        .await
        .assured("the detach task completes")
        .assured("the detach is answered");
    assert_eq!(
        outcome.disposition,
        DomainClockDetachDisposition::Detached(domain("tenant"))
    );

    // The new session answers what the install sent the way a session does: it attaches a clock
    // it does not hold.
    let mut attached_by_the_session = false;
    while let Some(request) = new.sent_request() {
        nervix_primitives::task::consume_budget().await;
        assert!(
            matches!(request.request, ClientRequest::AttachDomainClock(_)),
            "the install sends nothing but a restoration here: {request:?}"
        );
        new.answer(request.request_id, attached(unpaced())).await;
        attached_by_the_session = true;
    }
    drop(current);
    installing.await.assured("the install completes");
    assert_eq!(
        client.domain_clock(&domain("tenant")),
        None,
        "the client follows no clock it was told to release"
    );
    assert!(
        !attached_by_the_session,
        "the new session holds no attachment of a clock the client released"
    );
    nervix_primitives::time::sleep(Duration::from_secs(60)).await;
    assert!(
        new.sent_request().is_none(),
        "nothing attaches the released clock again"
    );
}

/// A restoration no session answers outlives the request timeout, which ends its session. A
/// caller already waiting for the clock's next event is woken to open another session; with no
/// server left to open one on, it learns that the session closed.
#[nervix_primitives::test(start_paused = true)]
async fn a_restoration_left_unanswered_past_the_request_timeout_wakes_the_waiting_reader() {
    let (client, mut old) = ServedExchange::connect();
    attach(&client, &mut old, unpaced()).await;
    let (replacement, mut new) = ServedExchange::reopen(&client);
    client.install(replacement).await;
    assert_eq!(
        next_event(&client).await,
        DomainClockEvent::Interrupted(DomainClockInterruption {
            domain: domain("tenant"),
        })
    );
    let restore = new.next_request().await;
    assert!(matches!(
        restore.request,
        ClientRequest::AttachDomainClock(_)
    ));

    let reading = {
        let client = client.clone();
        nervix_primitives::task::spawn(async move { client.next_domain_clock_event().await })
    };
    let request_timeout = client.inner.connector.request_timeout();
    let read = nervix_primitives::time::timeout(
        request_timeout
            .checked_add(DEADLINE)
            .assured("two test deadlines add within Duration's range"),
        reading,
    )
    .await
    .assured("the waiting read is woken once the unanswered restoration ends its session")
    .assured("the read task completes");
    let failure = read.expect_err("no server is left to open another session on");
    assert!(
        matches!(failure.current_context(), ClientError::SessionClosed),
        "{failure:?}"
    );
    assert_eq!(
        client
            .domain_clock(&domain("tenant"))
            .assured("the interrupted clock is still followed")
            .clock(),
        &unpaced()
    );
}

/// A reconnect installs a new exchange while a detach waits for the old session's answer, and the
/// old session refuses the detach. The clock stays followed, and once the install has ended the old
/// session, the new session attaches it again: a restoration that waited for the detach is not
/// left undone.
#[nervix_primitives::test(start_paused = true)]
async fn a_detach_refused_during_a_reconnect_leaves_the_clock_restored_on_the_new_session() {
    let (client, mut old) = ServedExchange::connect();
    attach(&client, &mut old, unpaced()).await;
    let detaching = {
        let client = client.clone();
        nervix_primitives::task::spawn(
            async move { client.detach_domain_clock(domain("tenant")).await },
        )
    };
    let detach = old.next_request().await;
    assert!(matches!(
        detach.request,
        ClientRequest::DetachDomainClock(_)
    ));

    let (replacement, mut new) = ServedExchange::reopen(&client);
    // The old exchange stays the client's until the test lets the install replace it.
    let current = client.inner.exchange.lock().await;
    let installing = {
        let client = client.clone();
        nervix_primitives::task::spawn(async move { client.install(replacement).await })
    };
    // The paused clock moves only once every task waits, so the install has gone as far as it can
    // before it replaces the old exchange.
    nervix_primitives::time::sleep(Duration::from_millis(1)).await;
    assert!(
        new.sent_request().is_none(),
        "no restoration attaches a clock while its detach is in flight"
    );
    old.answer(
        detach.request_id,
        detached(DomainClockDetachDisposition::Failed),
    )
    .await;
    let outcome = detaching
        .await
        .assured("the detach task completes")
        .assured("the detach is answered");
    assert_eq!(outcome.disposition, DomainClockDetachDisposition::Failed);

    drop(current);
    installing.await.assured("the install completes");
    assert_eq!(
        next_event(&client).await,
        DomainClockEvent::Interrupted(DomainClockInterruption {
            domain: domain("tenant"),
        })
    );
    let restore = new.next_request().await;
    assert!(
        matches!(restore.request, ClientRequest::AttachDomainClock(_)),
        "the new session attaches the clock the refused detach left followed"
    );
    new.answer(restore.request_id, attached(paced())).await;
    assert_eq!(
        next_event(&client).await,
        DomainClockEvent::Observed(crate::DomainClockObserved {
            domain: domain("tenant"),
            clock: paced(),
        })
    );
}

/// The caller attaches a clock while a reconnect opens the next exchange, and the old session
/// answers before the install ends it. The install then attaches that clock on the new session as
/// well, after the end of the old one interrupted it.
#[nervix_primitives::test(start_paused = true)]
async fn a_clock_attached_while_a_reconnect_opens_its_exchange_is_attached_again_on_it() {
    let (client, mut old) = ServedExchange::connect();
    let attaching = {
        let client = client.clone();
        nervix_primitives::task::spawn(
            async move { client.attach_domain_clock(domain("tenant")).await },
        )
    };
    let request = old.next_request().await;
    assert!(matches!(
        request.request,
        ClientRequest::AttachDomainClock(_)
    ));

    let (replacement, mut new) = ServedExchange::reopen(&client);
    // The old exchange stays the client's until the test lets the install replace it.
    let current = client.inner.exchange.lock().await;
    let installing = {
        let client = client.clone();
        nervix_primitives::task::spawn(async move { client.install(replacement).await })
    };
    // The paused clock moves only once every task waits, so the install has attached the clocks
    // the client followed when it began before the old session answers.
    nervix_primitives::time::sleep(Duration::from_millis(1)).await;
    assert!(
        new.sent_request().is_none(),
        "the install began before the client followed the clock"
    );
    old.answer(request.request_id, attached(unpaced())).await;
    let outcome = attaching
        .await
        .assured("the attach task completes")
        .assured("the attach is answered");
    assert!(matches!(
        outcome.disposition,
        DomainClockAttachDisposition::Attached { .. }
    ));

    drop(current);
    installing.await.assured("the install completes");
    let restore = new.next_request().await;
    assert!(
        matches!(restore.request, ClientRequest::AttachDomainClock(_)),
        "the new session attaches the clock the old session attached"
    );
    new.answer(restore.request_id, attached(unpaced())).await;
    assert_eq!(
        next_event(&client).await,
        DomainClockEvent::Interrupted(DomainClockInterruption {
            domain: domain("tenant"),
        })
    );
    assert_eq!(
        next_event(&client).await,
        DomainClockEvent::Observed(crate::DomainClockObserved {
            domain: domain("tenant"),
            clock: unpaced(),
        })
    );
}
