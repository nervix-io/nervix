//! The consumers of the example's two attached client emitters: the competing consumers of the
//! constructed readings, and the consumer of rejection notices.
//!
//! - **Owns.** Opening a consumer, reading its deliveries, applying each to the effect store before
//!   acknowledging it, joining late and leaving early, opening a new consumer when the domain's
//!   START generation changes, and closing.
//! - **Depends on.** The Rust client's emitter consumers, decoding the delivered batches, and the
//!   effect store.
//! - **Must not know.** Producers or the clock.
//!
//! Receiving a delivery acknowledges nothing. The application's effect is recorded first and the
//! delivery acknowledged afterwards, so a delivery whose acknowledgement is lost comes again and
//! finds its effect already recorded. A gap in the session is reported once and the next read
//! restores the attachment; only a changed contract or generation needs an explicit new open.

use std::{
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use meticulous::OptionExt as _;
use nervix_client_core::{
    Client, ClientConsumerLimits, ClientError, ConsumerConnection, ConsumerReopenReason,
    DomainName, EmitterConsumer, EmitterDelivery, EmitterName, EmitterSettlement,
};
use nervix_primitives::{
    sync::{Arc, CancellationToken, Mutex, watch},
    time::Instant,
};
use thiserror::Error;

use crate::{
    effects::{Applied, EffectStore},
    readings::{self, observed_fields, rejected_fields},
    refusal::Refusal,
    report::{self, Report as Counters},
};

/// How long an open waits for an endpoint that is not running on its node yet.
pub(crate) const OPEN_RETRY_BUDGET: Duration = Duration::from_secs(30);

/// How long an open waits before asking again for an endpoint that is not running yet.
pub(crate) const OPEN_RETRY_DELAY: Duration = Duration::from_millis(200);

/// How long a consumer waits before reading again after its session could not be restored.
const UNAVAILABLE_RETRY: Duration = Duration::from_secs(1);

/// The credit every consumer asks for: enough for the largest batch the example's emitters
/// declare.
fn consumer_limits() -> ClientConsumerLimits {
    ClientConsumerLimits {
        batches: NonZeroU32::new(4).assured("four batches is not zero"),
        bytes: NonZeroU64::new(1024 * 1024).assured("a mebibyte is not zero"),
    }
}

/// The text of a consumer's reason to need a new open.
fn reopen_reason(reason: &ConsumerReopenReason) -> String {
    match reason {
        ConsumerReopenReason::DomainStopped => "domain_stopped".to_string(),
        ConsumerReopenReason::EndpointRemoved => "endpoint_removed".to_string(),
        ConsumerReopenReason::SchemaChanged => "schema_changed".to_string(),
        ConsumerReopenReason::ContractChanged => "contract_changed".to_string(),
        ConsumerReopenReason::GenerationChanged => "generation_changed".to_string(),
        ConsumerReopenReason::ProtocolViolated => "protocol_violated".to_string(),
        ConsumerReopenReason::Refused(refusal) => {
            format!("refused ({})", Refusal::from(*refusal).as_str())
        }
    }
}

/// The text of a settlement the server confirmed or refused.
const fn settlement(settlement: EmitterSettlement) -> &'static str {
    match settlement {
        EmitterSettlement::Confirmed => "confirmed",
        EmitterSettlement::StaleReference => "stale_reference",
        EmitterSettlement::WrongConsumer => "wrong_consumer",
        EmitterSettlement::InvalidReason => "invalid_reason",
        EmitterSettlement::ConsumerEnded => "consumer_ended",
    }
}

/// Why a consumer could not be opened.
#[derive(Debug, Error)]
pub(crate) enum ConsumerError {
    #[error("emitter '{emitter}' of domain '{domain}' refused the consumer: {}", .refusal.as_str())]
    Refused {
        emitter: EmitterName,
        domain: DomainName,
        refusal: Refusal,
    },
    #[error("cannot open a consumer on emitter '{emitter}' of domain '{domain}': {reason}")]
    Session {
        emitter: EmitterName,
        domain: DomainName,
        reason: String,
    },
}

/// Which of the example's emitters a consumer reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Output {
    /// The constructed readings, applied as reading effects.
    Readings,
    /// The rejection notices, recorded as rejections.
    Rejections,
}

impl Output {
    fn fields(self) -> Vec<nervix_client_core::SchemaField> {
        match self {
            Self::Readings => observed_fields(),
            Self::Rejections => rejected_fields(),
        }
    }
}

/// Opens a consumer of `emitter`, asking again while the emitter is not running on its node yet.
pub(crate) async fn open(
    client: &Client,
    domain: &DomainName,
    emitter: &EmitterName,
    output: Output,
) -> error_stack::Result<EmitterConsumer, ConsumerError> {
    let deadline = Instant::now() + OPEN_RETRY_BUDGET;
    loop {
        nervix_primitives::task::consume_budget().await;
        let opened = client
            .subscribe_emitter(
                domain.clone(),
                emitter.clone(),
                output.fields(),
                consumer_limits(),
            )
            .await;
        let report = match opened {
            Ok(consumer) => return Ok(consumer),
            Err(report) => report,
        };
        let refusal = match report.current_context() {
            ClientError::ConsumerRefused { refusal, .. } => Refusal::from(*refusal),
            other => {
                let reason = other.to_string();
                return Err(report.change_context(ConsumerError::Session {
                    emitter: emitter.clone(),
                    domain: domain.clone(),
                    reason,
                }));
            }
        };
        if refusal == Refusal::EndpointUnavailable && Instant::now() < deadline {
            nervix_primitives::time::sleep(OPEN_RETRY_DELAY).await;
            continue;
        }
        return Err(report.change_context(ConsumerError::Refused {
            emitter: emitter.clone(),
            domain: domain.clone(),
            refusal,
        }));
    }
}

/// What every consumer of a run shares.
#[derive(Clone)]
pub(crate) struct Shared {
    pub(crate) client: Client,
    pub(crate) domain: DomainName,
    pub(crate) effects: Arc<Mutex<EffectStore>>,
    pub(crate) counters: Arc<Counters>,
    /// The START generation the simulation runs in; a consumer whose generation ended opens a new
    /// consumer once the simulation moved on to a later one.
    pub(crate) generations: watch::Receiver<u64>,
    /// Ends every consumer once the simulation finished.
    pub(crate) stop: CancellationToken,
}

/// One consumer loop: its emitter, how long the application works on a delivery, and when it
/// leaves.
pub(crate) struct ConsumerLoop {
    pub(crate) name: String,
    pub(crate) emitter: EmitterName,
    pub(crate) output: Output,
    pub(crate) processing_time: Duration,
    pub(crate) leave_after: Option<u64>,
    pub(crate) shared: Shared,
}

impl ConsumerLoop {
    /// Reads, applies and acknowledges deliveries until the run stops or the consumer leaves.
    pub(crate) async fn run(mut self, consumer: EmitterConsumer) {
        let mut consumer = consumer;
        let mut generation = consumer.description().generation;
        let mut handled: u64 = 0;
        loop {
            nervix_primitives::task::consume_budget().await;
            let read = nervix_primitives::select! {
                read = consumer.next_batch() => read,
                () = self.shared.stop.cancelled() => break,
            };
            let failure = match read {
                Ok(Some(delivery)) => {
                    self.handle(&delivery, generation).await;
                    handled = handled
                        .checked_add(1)
                        .assured("a consumer handles far fewer than 2^64 deliveries");
                    if self.leave_after.is_some_and(|limit| handled >= limit) {
                        self.leave(&consumer, handled).await;
                        return;
                    }
                    continue;
                }
                Ok(None) => break,
                Err(report) => report,
            };
            match failure.current_context() {
                ClientError::ConsumerInterrupted => {
                    report::line(format!(
                        "INTERRUPTED consumer={} emitter={}",
                        self.name, self.emitter
                    ));
                }
                ClientError::ConsumerReopenRequired(reason) => {
                    report::line(format!(
                        "CONSUMER reopen_required consumer={} emitter={} reason={}",
                        self.name,
                        self.emitter,
                        reopen_reason(reason)
                    ));
                    let Some(reopened) = self.reopen(generation).await else {
                        break;
                    };
                    consumer = reopened;
                    generation = consumer.description().generation;
                }
                ClientError::ConsumerSessionUnavailable => {
                    report::line(format!(
                        "CONSUMER unavailable consumer={} emitter={}",
                        self.name, self.emitter
                    ));
                    nervix_primitives::select! {
                        () = nervix_primitives::time::sleep(UNAVAILABLE_RETRY) => {}
                        () = self.shared.stop.cancelled() => break,
                    }
                }
                ClientError::SessionClosed => break,
                other => {
                    report::line(format!(
                        "CONSUMER failed consumer={} emitter={} reason={other}",
                        self.name, self.emitter
                    ));
                    break;
                }
            }
            if consumer.connection() == ConsumerConnection::Closed {
                break;
            }
        }
        self.close(&consumer).await;
    }

    /// Opens a new consumer under the generation the simulation moved on to, once it has.
    async fn reopen(&mut self, generation: u64) -> Option<EmitterConsumer> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let current = *self.shared.generations.borrow_and_update();
            if current > generation {
                break;
            }
            nervix_primitives::select! {
                changed = self.shared.generations.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                }
                () = self.shared.stop.cancelled() => return None,
            }
        }
        match open(
            &self.shared.client,
            &self.shared.domain,
            &self.emitter,
            self.output,
        )
        .await
        {
            Ok(consumer) => {
                let description = consumer.description();
                report::line(format!(
                    "CONSUMER reopened consumer={} emitter={} generation={}",
                    self.name, self.emitter, description.generation
                ));
                Some(consumer)
            }
            Err(failure) => {
                report::error(failure.current_context().to_string());
                None
            }
        }
    }

    /// Applies one delivery to the effect store and acknowledges it.
    async fn handle(&self, delivery: &EmitterDelivery, generation: u64) {
        let batch = match delivery.record_batch() {
            Ok(batch) => batch,
            Err(failure) => {
                report::line(format!(
                    "DELIVERY undecodable consumer={} emitter={} reason={}",
                    self.name,
                    self.emitter,
                    failure.current_context()
                ));
                let rejected = delivery.reject("the delivery does not decode").await;
                self.settled("REJECT", rejected);
                return;
            }
        };
        report::line(format!(
            "PROCESSING consumer={} emitter={} rows={}",
            self.name,
            self.emitter,
            batch.num_rows()
        ));
        if !self.processing_time.is_zero() {
            nervix_primitives::time::sleep(self.processing_time).await;
        }
        let mut applied: u64 = 0;
        let mut duplicates: u64 = 0;
        let recorded = {
            let mut effects = self.shared.effects.lock().await;
            let mut recorded = Ok(());
            match self.output {
                Output::Readings => {
                    for reading in readings::observed_readings(&batch) {
                        match effects.reading(&reading, generation).await {
                            Ok(outcome) => {
                                self.shared.counters.reading_effect(outcome);
                                count(outcome, &mut applied, &mut duplicates);
                            }
                            Err(failure) => {
                                recorded = Err(failure);
                                break;
                            }
                        }
                    }
                }
                Output::Rejections => {
                    for notice in readings::rejection_notices(&batch) {
                        report::line(format!(
                            "REJECTED reading_id={} occurred_at={} error_code={} error_message={}",
                            notice.reading_id,
                            notice.occurred_at.to_rfc3339(),
                            notice.error_code,
                            notice.error_message
                        ));
                        match effects.rejection(&notice, generation).await {
                            Ok(outcome) => {
                                self.shared.counters.rejection_notice(outcome);
                                count(outcome, &mut applied, &mut duplicates);
                            }
                            Err(failure) => {
                                recorded = Err(failure);
                                break;
                            }
                        }
                    }
                }
            }
            recorded
        };
        if let Err(failure) = recorded {
            // The effect was not recorded, so the delivery must come again: ask for a retry.
            report::error(failure.to_string());
            let retried = delivery.retry().await;
            self.settled("RETRY", retried);
            return;
        }
        report::line(format!(
            "DELIVERY consumer={} emitter={} rows={} applied={applied} duplicates={duplicates}",
            self.name,
            self.emitter,
            batch.num_rows()
        ));
        let acknowledged = delivery.ack().await;
        self.settled("ACK", acknowledged);
    }

    /// Prints how a settlement of a delivery ended.
    fn settled(&self, action: &str, settled: error_stack::Result<EmitterSettlement, ClientError>) {
        let outcome = match settled {
            Ok(outcome) => settlement(outcome).to_string(),
            Err(failure) => match failure.current_context() {
                ClientError::SettlementUnknown { .. } => "unknown".to_string(),
                ClientError::DeliveryReferenceExpired { .. } => "expired".to_string(),
                other => format!("failed reason={other}"),
            },
        };
        report::line(format!(
            "{action} consumer={} emitter={} {outcome}",
            self.name, self.emitter
        ));
    }

    async fn leave(&self, consumer: &EmitterConsumer, handled: u64) {
        self.close(consumer).await;
        self.shared.counters.consumer_left();
        report::line(format!(
            "CONSUMER left consumer={} emitter={} deliveries={handled}",
            self.name, self.emitter
        ));
    }

    async fn close(&self, consumer: &EmitterConsumer) {
        if let Err(failure) = consumer.close().await {
            report::line(format!(
                "CONSUMER close_failed consumer={} emitter={} reason={}",
                self.name,
                self.emitter,
                failure.current_context()
            ));
        }
    }
}

/// Counts one applied record of a delivery as new or as a duplicate.
fn count(outcome: Applied, applied: &mut u64, duplicates: &mut u64) {
    let counter = match outcome {
        Applied::New => applied,
        Applied::Duplicate => duplicates,
    };
    *counter = counter
        .checked_add(1)
        .assured("a delivery carries at most 65,536 rows");
}

/// The report line of a consumer that opened.
pub(crate) fn opened_line(name: &str, emitter: &EmitterName, consumer: &EmitterConsumer) -> String {
    let description = consumer.description();
    format!(
        "CONSUMER opened consumer={name} emitter={emitter} generation={} batches={} bytes={}",
        description.generation, description.granted.batches, description.granted.bytes
    )
}
