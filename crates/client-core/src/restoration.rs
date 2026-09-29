//! Restoring on a new exchange what a client holds: the domain clocks it follows and the
//! subscriptions the server acknowledged.
//!
//! - **Owns.** Sending the restoration requests of a new exchange in the order the protocol
//!   requires, waiting for their replies, and sending a restoration the exchange refused again
//!   after a wait that doubles with each refusal.
//! - **Depends on.** The exchange's request registry, the followed domain clocks and desired
//!   subscriptions, and the wire contract.
//! - **Must not know.** How a lost session is detected, which server the new exchange opened on,
//!   or how the exchange routes its frames.
//!
//! Clocks are attached before the new exchange is published, so no other request of the client
//! precedes them on it. Subscriptions are opened again once the previous exchange has ended, which
//! is what interrupts the subscriptions it held. Both are sent before the reconnect returns, and so
//! before the transaction the reconnect attaches next: a session that holds a transaction refuses
//! them.
//!
//! The exchange reader applies every reply to the followed clocks and desired subscriptions as it
//! arrives. A restoration task only waits for the replies of its own requests, to report a refused
//! restoration and to send it again for as long as the exchange stays open.

use std::time::Duration;

use ahash::HashSet;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_client_wire::{
    AttachDomainClockRequest, ClientMessage, ClientRequest, DomainClockAttachDisposition,
    ReplyBody, SubscribeDisposition,
};
use nervix_models::DomainName;
use nervix_primitives::time::sleep;
use triomphe::Arc;

use crate::{
    domain_clock::DomainClockAttachments,
    error::{ClientError, RequestKind},
    exchange::{EventSinks, Exchange, ExchangeRequests, PendingRequest, SESSION_LIMITS},
    subscriptions::{DesiredSubscriptions, RestoreAttempt},
};

/// The wait before a restoration its exchange refused is sent again. It starts at one second and
/// doubles after each refusal on the same exchange up to thirty seconds, so a refusal that clears
/// soon, such as a relay that is still starting, is retried promptly, while one that lasts, such
/// as a removed relay, costs one request every thirty seconds.
struct RestorationDelay {
    next: Duration,
}

impl RestorationDelay {
    const FIRST: Duration = Duration::from_secs(1);
    const LIMIT: Duration = Duration::from_secs(30);

    /// The wait before the next attempt. Each call doubles the wait after it, up to the limit.
    fn advance(&mut self) -> Duration {
        let current = self.next;
        let doubled = current
            .checked_mul(2)
            .assured("a wait of at most thirty seconds doubles within Duration's range");
        self.next = doubled.min(Self::LIMIT);
        current
    }
}

impl Default for RestorationDelay {
    fn default() -> Self {
        Self { next: Self::FIRST }
    }
}

const _: () = assert!(
    RestorationDelay::FIRST.as_nanos() <= RestorationDelay::LIMIT.as_nanos(),
    "the first wait of a refused restoration is within the limit of every later wait",
);

/// The exchange a restoration is sent on.
#[derive(Clone)]
struct RestorationChannel {
    requests: Arc<ExchangeRequests>,
    request_timeout: Duration,
}

impl RestorationChannel {
    /// Registers the waiter of `request`, then sends its frame, so its reply can never arrive
    /// before its waiter.
    async fn send(
        &self,
        request: ClientRequest,
    ) -> error_stack::Result<PendingRequest, ClientError> {
        let kind = RequestKind::from(&request);
        let Some(registered) = self.requests.register() else {
            let failure = self.requests.pending.lock().failure();
            return Err(Report::new(failure));
        };
        let message = ClientMessage {
            request_id: registered.request_id,
            request,
        };
        let frame = match message.encode(&SESSION_LIMITS) {
            Ok(frame) => frame,
            Err(report) => {
                return Err(Report::new(ClientError::EncodeRequest {
                    request: kind,
                    source: report.current_context().clone(),
                }));
            }
        };
        if self.requests.frames.send(frame).await.is_err() {
            self.requests.pending.lock().close();
            let failure = self.requests.pending.lock().failure();
            return Err(Report::new(failure));
        }
        Ok(registered)
    }

    /// Waits for the reply of a request `send` sent.
    ///
    /// A request that outlives the request timeout is the only sign that the session is dead, so
    /// it ends every request still waiting on the exchange, exactly as any other request does.
    async fn reply(
        &self,
        sent: error_stack::Result<PendingRequest, ClientError>,
        kind: RequestKind,
    ) -> error_stack::Result<ReplyBody, ClientError> {
        let mut waiter = sent?;
        let Ok(received) =
            nervix_primitives::time::timeout(self.request_timeout, waiter.receive()).await
        else {
            self.requests.pending.lock().close();
            return Err(Report::new(ClientError::RequestDeadline { request: kind }));
        };
        if let Some(body) = received {
            return Ok(body);
        }
        let failure = self.requests.pending.lock().failure();
        match failure {
            ClientError::SessionClosed => Err(Report::new(ClientError::RequestInterrupted {
                request: kind,
            })),
            failure => Err(Report::new(failure)),
        }
    }

    fn is_open(&self) -> bool {
        self.requests.pending.lock().is_open()
    }
}

/// What a client restores on one new exchange, in the order it sends it.
pub(crate) struct Restoration {
    channel: RestorationChannel,
    generation: Arc<()>,
    sinks: EventSinks,
    /// The domains whose clocks an attach was sent for.
    attached: HashSet<DomainName>,
    clocks: Vec<ClockRestoration>,
    subscriptions: Vec<SubscriptionRestoration>,
}

/// The attach request restoring one followed clock, as it was sent.
struct ClockRestoration {
    domain: DomainName,
    sent: error_stack::Result<PendingRequest, ClientError>,
}

/// The subscribe request restoring one desired subscription, as it was sent.
struct SubscriptionRestoration {
    attempt: RestoreAttempt,
    sent: error_stack::Result<PendingRequest, ClientError>,
}

impl Restoration {
    pub(crate) fn new(exchange: &Exchange, request_timeout: Duration) -> Self {
        Self {
            channel: RestorationChannel {
                requests: exchange.requests(),
                request_timeout,
            },
            generation: exchange.generation.clone(),
            sinks: exchange.sinks.clone(),
            attached: HashSet::default(),
            clocks: Vec::new(),
            subscriptions: Vec::new(),
        }
    }

    /// Attaches every clock the client follows, before the exchange is published.
    pub(crate) async fn attach_followed_clocks(&mut self) {
        for domain in self.sinks.clocks.followed_domains() {
            nervix_primitives::task::consume_budget().await;
            self.attach_clock(domain).await;
        }
    }

    /// Attaches the clocks the end of the previous exchange interrupted that no attach was sent
    /// for yet: a clock the client began to follow there while this exchange was opening.
    pub(crate) async fn attach_interrupted_clocks(&mut self) {
        for domain in self.sinks.clocks.interrupted_domains() {
            nervix_primitives::task::consume_budget().await;
            if self.attached.contains(&domain) {
                continue;
            }
            self.attach_clock(domain).await;
        }
    }

    async fn attach_clock(&mut self, domain: DomainName) {
        let request = ClientRequest::AttachDomainClock(AttachDomainClockRequest {
            domain: domain.clone(),
        });
        let sent = self.channel.send(request).await;
        self.attached.insert(domain.clone());
        self.clocks.push(ClockRestoration { domain, sent });
    }

    /// Opens every acknowledged subscription the end of the previous exchange interrupted, each
    /// as a new generation.
    pub(crate) async fn open_subscriptions(&mut self) {
        for attempt in self.sinks.desired.restore(self.generation.clone()) {
            nervix_primitives::task::consume_budget().await;
            let request = ClientRequest::Subscribe(attempt.contract.request());
            let sent = self.channel.send(request).await;
            self.subscriptions
                .push(SubscriptionRestoration { attempt, sent });
        }
    }

    /// Waits for every reply on a task of its own, which sends a refused restoration again for
    /// as long as the exchange stays open.
    pub(crate) fn follow(self) {
        for clock in self.clocks {
            let follower = ClockFollower {
                channel: self.channel.clone(),
                clocks: self.sinks.clocks.clone(),
                domain: clock.domain,
            };
            nervix_primitives::task::spawn(follower.run(clock.sent));
        }
        for subscription in self.subscriptions {
            let follower = SubscriptionFollower {
                channel: self.channel.clone(),
                desired: self.sinks.desired.clone(),
                generation: self.generation.clone(),
            };
            nervix_primitives::task::spawn(follower.run(subscription.attempt, subscription.sent));
        }
    }
}

/// Follows the restoration of one clock on its exchange.
struct ClockFollower {
    channel: RestorationChannel,
    clocks: DomainClockAttachments,
    domain: DomainName,
}

impl ClockFollower {
    async fn run(self, sent: error_stack::Result<PendingRequest, ClientError>) {
        let mut sent = sent;
        let mut delay = RestorationDelay::default();
        loop {
            nervix_primitives::task::consume_budget().await;
            let reply = self
                .channel
                .reply(sent, RequestKind::AttachDomainClock)
                .await;
            let message = match reply {
                Ok(ReplyBody::DomainClockAttach(outcome)) => match outcome.disposition {
                    // The reader applied each of these as the reply arrived.
                    DomainClockAttachDisposition::Attached { .. }
                    | DomainClockAttachDisposition::AlreadyAttached(_)
                    | DomainClockAttachDisposition::DomainNotFound(_) => return,
                    DomainClockAttachDisposition::Failed => outcome.message,
                },
                Ok(other) => {
                    ClientError::unexpected_reply(RequestKind::AttachDomainClock, other).to_string()
                }
                // The exchange ended, and the next one attaches the clock again.
                Err(report) if report.current_context().retryable_session_failure() => return,
                Err(report) => report.current_context().to_string(),
            };
            if !self.channel.is_open() {
                return;
            }
            let retry_after = delay.advance();
            if !self
                .clocks
                .restoration_failed(&self.domain, message, retry_after)
            {
                return;
            }
            sleep(retry_after).await;
            if !self.clocks.awaits_restoration_of(&self.domain) {
                return;
            }
            let request = ClientRequest::AttachDomainClock(AttachDomainClockRequest {
                domain: self.domain.clone(),
            });
            sent = self.channel.send(request).await;
        }
    }
}

/// Follows the restoration of one subscription on its exchange.
struct SubscriptionFollower {
    channel: RestorationChannel,
    desired: DesiredSubscriptions,
    generation: Arc<()>,
}

impl SubscriptionFollower {
    async fn run(
        self,
        attempt: RestoreAttempt,
        sent: error_stack::Result<PendingRequest, ClientError>,
    ) {
        let name = attempt.contract.create.name.clone();
        let mut attempt = attempt;
        let mut sent = sent;
        let mut delay = RestorationDelay::default();
        loop {
            nervix_primitives::task::consume_budget().await;
            let reply = self.channel.reply(sent, RequestKind::Subscribe).await;
            let message = match reply {
                Ok(ReplyBody::Subscribe(outcome)) => match &outcome.disposition {
                    SubscribeDisposition::Opened(opened) => {
                        self.desired.created(&attempt, Some(&opened.subscription));
                        return;
                    }
                    SubscribeDisposition::Failed => {
                        self.desired.created(&attempt, None);
                        outcome.message
                    }
                },
                Ok(other) => {
                    self.desired.created(&attempt, None);
                    ClientError::unexpected_reply(RequestKind::Subscribe, other).to_string()
                }
                // The exchange ended, and the next one opens the subscription again.
                Err(report) if report.current_context().retryable_session_failure() => {
                    self.desired.created(&attempt, None);
                    return;
                }
                Err(report) => {
                    self.desired.created(&attempt, None);
                    report.current_context().to_string()
                }
            };
            if !self.channel.is_open() {
                return;
            }
            let retry_after = delay.advance();
            if !self
                .desired
                .restoration_failed(&attempt, message, retry_after)
            {
                return;
            }
            sleep(retry_after).await;
            let Some(next) = self.desired.retry(&name, &self.generation) else {
                return;
            };
            attempt = next;
            let request = ClientRequest::Subscribe(attempt.contract.request());
            sent = self.channel.send(request).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refused_restoration_waits_twice_as_long_each_time_up_to_the_limit() {
        let mut delay = RestorationDelay::default();
        let mut waits = Vec::new();
        for _ in 0..7 {
            waits.push(delay.advance().as_secs());
        }
        assert_eq!(waits, [1, 2, 4, 8, 16, 30, 30]);
    }
}
