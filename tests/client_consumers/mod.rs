//! Public scenarios for opening and settling native client emitter output.
//!
//! Layer: test harness.
//! - **Owns.** Named consumer attachments on gRPC or console WebSocket, their observed Arrow
//!   attempts and explicit application settlements.
//! - **Depends on.** The Rust client, raw console session and Arrow IPC reader.
//! - **Must not know.** The graph's assignment implementation or the owning node.

use std::{
    collections::BTreeMap,
    num::{NonZeroU32, NonZeroU64},
    sync::Arc as StdArc,
    time::Duration,
};

use arrow_array::{Int64Array, StringArray};
use arrow_ipc::reader::StreamReader;
use cucumber::{then, when};
use nervix_client_core::{ClientError, EmitterConsumer, EmitterDelivery};
use nervix_client_wire::{
    ClientRequest, CloseEmitterRequest, ConsumerId, EmitterBatchDecision, EmitterBatchReceived,
    EmitterOpenRefusal, EmitterSettlement, OpenEmitterDisposition, OpenEmitterRequest,
    ReadEmitterBatchRequest, ReadEmitterDisposition, ReplyBody, SettleEmitterBatchRequest,
};
use nervix_models::{ClientConsumerLimits, EmitterName};

use super::{
    client_producers::{expected_fields, scenario_domain},
    *,
};

const OPEN_WAIT: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(crate) struct ScenarioConsumers {
    open: BTreeMap<String, ScenarioConsumer>,
    received: BTreeMap<String, Received>,
    pending_reads: BTreeMap<String, AbortOnDropHandle<EmitterDelivery>>,
}

enum ScenarioConsumer {
    Native(StdArc<EmitterConsumer>),
    WebSocket { session: String, id: ConsumerId },
}

struct Received {
    consumer: String,
    attempt: Attempt,
}

enum Attempt {
    Native(EmitterDelivery),
    WebSocket(EmitterBatchReceived),
}

impl Attempt {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Native(attempt) => &attempt.batch,
            Self::WebSocket(attempt) => &attempt.batch,
        }
    }

    fn identity(&self) -> (uuid::Uuid, uuid::Uuid) {
        match self {
            Self::Native(attempt) => (attempt.identity, attempt.reference),
            Self::WebSocket(attempt) => (attempt.identity, attempt.reference),
        }
    }
}

fn limits() -> ClientConsumerLimits {
    ClientConsumerLimits {
        batches: NonZeroU32::new(8).expect("literal is nonzero"),
        bytes: NonZeroU64::new(8 * 1024 * 1024).expect("literal is nonzero"),
    }
}

#[then(expr = "within {string} the leader node describes emitter {string} with")]
async fn then_leader_describes_emitter_with(
    world: &mut ScenarioWorld,
    within: String,
    emitter: String,
    #[step] step: &Step,
) {
    let within = humantime::parse_duration(&within).expect("the step names a valid duration");
    let expected = expand_placeholders(world, docstring(step));
    let emitter = expand_placeholders(world, &emitter);
    let deadline = Instant::now() + within;
    loop {
        nervix_primitives::task::consume_budget().await;
        let leader = running_leader_node(world).await;
        let output = world
            .cluster()
            .run_command(
                &leader,
                &world.domain,
                &format!("DESCRIBE EMITTER {emitter};"),
            )
            .await
            .unwrap_or_else(|error| format!("DESCRIBE failed: {error}"));
        let missing = expected
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !output.contains(line))
            .collect::<Vec<_>>();
        if missing.is_empty() {
            world.last_command_output = Some(output);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "DESCRIBE EMITTER {emitter} never reported {missing:?}; last output:\n{output}"
        );
        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn open(
    world: &mut ScenarioWorld,
    session: &str,
    consumer: &str,
    emitter: &str,
    fields: &str,
    websocket: bool,
) {
    let deadline = Instant::now() + OPEN_WAIT;
    let emitter = EmitterName::parse(&expand_placeholders(world, emitter))
        .expect("scenario emitter is a name");
    let fields = expected_fields(&expand_placeholders(world, fields));
    let domain = scenario_domain(world);
    loop {
        let opened = if websocket {
            let raw = world
                .producers
                .sessions
                .get_mut(session)
                .unwrap_or_else(|| panic!("WebSocket session '{session}' is not connected"));
            let request = raw
                .send(ClientRequest::OpenEmitter(OpenEmitterRequest {
                    domain: domain.clone(),
                    emitter: emitter.clone(),
                    expected_fields: fields.clone(),
                    limits: limits(),
                }))
                .await
                .expect("console consumer open sent");
            let reply = raw
                .reply(request, OPEN_WAIT)
                .await
                .expect("console consumer open answered");
            let ReplyBody::OpenEmitter(outcome) = reply.body else {
                panic!("console consumer open got {:?}", reply.body);
            };
            match outcome.disposition {
                OpenEmitterDisposition::Opened(_) => Ok(ScenarioConsumer::WebSocket {
                    session: session.to_string(),
                    id: ConsumerId::opened_by(request),
                }),
                OpenEmitterDisposition::Refused(reason) => Err(reason),
            }
        } else {
            let client = world
                .transaction_clients
                .get(session)
                .unwrap_or_else(|| panic!("client '{session}' is not connected"));
            match client
                .subscribe_emitter(domain.clone(), emitter.clone(), fields.clone(), limits())
                .await
            {
                Ok(opened) => Ok(ScenarioConsumer::Native(StdArc::new(opened))),
                Err(report) => match report.current_context() {
                    ClientError::ConsumerRefused { refusal, .. } => Err(*refusal),
                    other => panic!("consumer open failed: {other}"),
                },
            }
        };
        match opened {
            Ok(opened) => {
                assert!(
                    world
                        .consumers
                        .open
                        .insert(consumer.to_string(), opened)
                        .is_none(),
                    "consumer '{consumer}' already exists"
                );
                return;
            }
            Err(EmitterOpenRefusal::EndpointUnavailable) if Instant::now() < deadline => {
                nervix_primitives::time::sleep(Duration::from_millis(50)).await;
            }
            Err(refusal) => panic!("emitter consumer open was refused: {refusal:?}"),
        }
    }
}

#[when(
    expr = "client {string} opens consumer {string} on emitter {string} expecting fields {string}"
)]
async fn when_client_opens_consumer(
    world: &mut ScenarioWorld,
    session: String,
    consumer: String,
    emitter: String,
    fields: String,
) {
    open(world, &session, &consumer, &emitter, &fields, false).await;
}

#[when(
    expr = "WebSocket session {string} opens consumer {string} on emitter {string} expecting \
            fields {string}"
)]
async fn when_console_opens_consumer(
    world: &mut ScenarioWorld,
    session: String,
    consumer: String,
    emitter: String,
    fields: String,
) {
    open(world, &session, &consumer, &emitter, &fields, true).await;
}

#[then(
    expr = "client {string} cannot open a consumer on emitter {string} expecting fields {string} \
            because {string}"
)]
async fn then_consumer_open_refused(
    world: &mut ScenarioWorld,
    session: String,
    emitter: String,
    fields: String,
    reason: String,
) {
    open_refused(world, &session, &emitter, &fields, &reason, false).await;
}

#[then(
    expr = "WebSocket session {string} cannot open a consumer on emitter {string} expecting \
            fields {string} because {string}"
)]
async fn then_console_consumer_open_refused(
    world: &mut ScenarioWorld,
    session: String,
    emitter: String,
    fields: String,
    reason: String,
) {
    open_refused(world, &session, &emitter, &fields, &reason, true).await;
}

async fn open_refused(
    world: &mut ScenarioWorld,
    session: &str,
    emitter: &str,
    fields: &str,
    reason: &str,
    websocket: bool,
) {
    let emitter = EmitterName::parse(&expand_placeholders(world, emitter))
        .expect("scenario emitter is a name");
    let fields = expected_fields(&expand_placeholders(world, fields));
    let domain = scenario_domain(world);
    let refusal = if websocket {
        let raw = world
            .producers
            .sessions
            .get_mut(session)
            .unwrap_or_else(|| panic!("WebSocket session '{session}' is not connected"));
        let request = raw
            .send(ClientRequest::OpenEmitter(OpenEmitterRequest {
                domain,
                emitter,
                expected_fields: fields,
                limits: limits(),
            }))
            .await
            .expect("console consumer open sent");
        let reply = raw
            .reply(request, OPEN_WAIT)
            .await
            .expect("console consumer open answered");
        let ReplyBody::OpenEmitter(outcome) = reply.body else {
            panic!("console consumer open got {:?}", reply.body);
        };
        match outcome.disposition {
            OpenEmitterDisposition::Opened(_) => panic!("consumer open unexpectedly succeeded"),
            OpenEmitterDisposition::Refused(refusal) => refusal,
        }
    } else {
        let client = world
            .transaction_clients
            .get(session)
            .unwrap_or_else(|| panic!("client '{session}' is not connected"));
        let report = match client
            .subscribe_emitter(domain, emitter, fields, limits())
            .await
        {
            Ok(_) => panic!("consumer open unexpectedly succeeded"),
            Err(report) => report,
        };
        let ClientError::ConsumerRefused { refusal, .. } = report.current_context() else {
            panic!("consumer open failed for another reason: {report:?}");
        };
        *refusal
    };
    let expected = match reason {
        "schema mismatch" => EmitterOpenRefusal::SchemaMismatch,
        "invalid limits" => EmitterOpenRefusal::InvalidLimits,
        "domain stopped" => EmitterOpenRefusal::DomainStopped,
        "session capacity exhausted" => EmitterOpenRefusal::SessionCapacityExhausted,
        other => panic!("undeclared consumer refusal expectation '{other}'"),
    };
    assert_eq!(refusal, expected);
}

#[when(expr = "consumer {string} reads output batch {string}")]
async fn when_consumer_reads(world: &mut ScenarioWorld, consumer: String, batch: String) {
    let attached = world
        .consumers
        .open
        .get(&consumer)
        .unwrap_or_else(|| panic!("consumer '{consumer}' is not open"));
    let attempt = match attached {
        ScenarioConsumer::Native(client) => {
            let delivery = nervix_primitives::time::timeout(OPEN_WAIT, client.next_batch())
                .await
                .expect("consumer read deadline")
                .expect("consumer read reply")
                .expect("consumer endpoint stayed open");
            Attempt::Native(delivery)
        }
        ScenarioConsumer::WebSocket { session, id } => {
            let raw = world
                .producers
                .sessions
                .get_mut(session)
                .expect("console session remains connected");
            let request = raw
                .send(ClientRequest::ReadEmitterBatch(ReadEmitterBatchRequest {
                    consumer: *id,
                }))
                .await
                .expect("console consumer read sent");
            let reply = raw
                .reply(request, OPEN_WAIT)
                .await
                .expect("console consumer read answered");
            let ReplyBody::ReadEmitterBatch(outcome) = reply.body else {
                panic!("console consumer read got {:?}", reply.body);
            };
            let ReadEmitterDisposition::Batch(batch) = outcome.disposition else {
                panic!("console consumer ended while awaiting output");
            };
            Attempt::WebSocket(batch)
        }
    };
    assert!(
        world
            .consumers
            .received
            .insert(batch, Received { consumer, attempt })
            .is_none()
    );
}

#[when(expr = "consumer {string} starts waiting for output batch {string}")]
async fn when_consumer_starts_read(world: &mut ScenarioWorld, consumer: String, batch: String) {
    let ScenarioConsumer::Native(client) = world
        .consumers
        .open
        .get(&consumer)
        .unwrap_or_else(|| panic!("consumer '{consumer}' is not open"))
    else {
        panic!("the concurrent read check uses a native client");
    };
    let client = client.clone();
    let wait = nervix_primitives::task::spawn(async move {
        nervix_primitives::time::timeout(OPEN_WAIT, client.next_batch())
            .await
            .expect("consumer read deadline")
            .expect("consumer read reply")
            .expect("consumer endpoint stayed open")
    });
    assert!(
        world
            .consumers
            .pending_reads
            .insert(batch, AbortOnDropHandle::new(wait))
            .is_none()
    );
}

#[when(expr = "consumer {string} finishes waiting for output batch {string}")]
async fn when_consumer_finishes_read(world: &mut ScenarioWorld, consumer: String, batch: String) {
    let wait = world
        .consumers
        .pending_reads
        .remove(&batch)
        .unwrap_or_else(|| panic!("no read was started for output batch '{batch}'"));
    let delivery = wait.await.expect("consumer read task completes");
    assert!(
        world
            .consumers
            .received
            .insert(
                batch,
                Received {
                    consumer,
                    attempt: Attempt::Native(delivery),
                }
            )
            .is_none()
    );
}

#[then(expr = "output batch {string} contains id {string} and cents {int}")]
async fn then_output_batch_contains(
    world: &mut ScenarioWorld,
    batch: String,
    id: String,
    cents: i64,
) {
    let received = world
        .consumers
        .received
        .get(&batch)
        .unwrap_or_else(|| panic!("output batch '{batch}' was not read"));
    let decoded = match &received.attempt {
        Attempt::Native(delivery) => delivery
            .record_batch()
            .expect("SDK verifies exact Arrow output"),
        Attempt::WebSocket(_) => {
            let mut reader =
                StreamReader::try_new(std::io::Cursor::new(received.attempt.bytes()), None)
                    .expect("client output is Arrow IPC");
            let decoded = reader
                .next()
                .expect("one Arrow batch")
                .expect("valid Arrow batch");
            assert!(
                reader.next().is_none(),
                "one delivery carries exactly one Arrow batch"
            );
            decoded
        }
    };
    assert_eq!(decoded.num_rows(), 1);
    assert_eq!(decoded.schema().fields().len(), 2);
    let ids = decoded
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("id is STRING");
    let values = decoded
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cents is I64");
    assert_eq!(ids.value(0), id);
    assert_eq!(values.value(0), cents);
}

#[then(expr = "output batches {string} cover both sources and concrete branches")]
async fn then_output_batches_cover_sources_and_branches(world: &mut ScenarioWorld, names: String) {
    let mut observed = BTreeMap::new();
    for name in names.split(',').map(str::trim) {
        let received = world
            .consumers
            .received
            .get(name)
            .unwrap_or_else(|| panic!("output batch '{name}' was not read"));
        let Attempt::Native(delivery) = &received.attempt else {
            panic!("this source and branch check uses the Rust consumer");
        };
        let decoded = delivery.record_batch().expect("SDK verifies Arrow output");
        assert_eq!(decoded.num_rows(), 1);
        let id = decoded
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id is STRING")
            .value(0)
            .to_string();
        let cents = decoded
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cents is I64")
            .value(0);
        let branch = delivery
            .branch_fingerprint
            .expect("a concrete branch fingerprint");
        assert!(
            observed
                .insert(
                    (delivery.source_relay.as_str().to_string(), id),
                    (branch, cents)
                )
                .is_none(),
            "one delivery per source and branch"
        );
    }
    assert_eq!(observed.len(), 4);
    for source in ["orders", "invoices"] {
        assert_eq!(observed[&(source.to_string(), "o-1".to_string())].1, 100);
        assert_eq!(observed[&(source.to_string(), "o-2".to_string())].1, 200);
    }
    assert_eq!(
        observed[&("orders".to_string(), "o-1".to_string())].0,
        observed[&("invoices".to_string(), "o-1".to_string())].0,
    );
    assert_eq!(
        observed[&("orders".to_string(), "o-2".to_string())].0,
        observed[&("invoices".to_string(), "o-2".to_string())].0,
    );
    assert_ne!(
        observed[&("orders".to_string(), "o-1".to_string())].0,
        observed[&("orders".to_string(), "o-2".to_string())].0,
    );
}

#[when(expr = "output batch {string} is acknowledged")]
async fn when_output_batch_acknowledged(world: &mut ScenarioWorld, batch: String) {
    assert_eq!(
        settle(world, &batch, EmitterBatchDecision::Ack).await,
        EmitterSettlement::Confirmed
    );
}

#[when(expr = "output batch {string} is retried")]
async fn when_output_batch_retried(world: &mut ScenarioWorld, batch: String) {
    assert_eq!(
        settle(world, &batch, EmitterBatchDecision::Retry).await,
        EmitterSettlement::Confirmed
    );
}

#[when(expr = "output batch {string} is rejected because {string}")]
async fn when_output_batch_rejected(world: &mut ScenarioWorld, batch: String, reason: String) {
    assert_eq!(
        settle(world, &batch, EmitterBatchDecision::Reject(reason)).await,
        EmitterSettlement::Confirmed
    );
}

#[then(expr = "output batch {string} has a stale acknowledgement reference")]
async fn then_stale_output_ack(world: &mut ScenarioWorld, batch: String) {
    assert_eq!(
        settle(world, &batch, EmitterBatchDecision::Ack).await,
        EmitterSettlement::StaleReference
    );
}

#[then(expr = "output batches {string} and {string} retain one identity with distinct attempts")]
async fn then_same_delivery_identity(world: &mut ScenarioWorld, first: String, second: String) {
    let first = world
        .consumers
        .received
        .get(&first)
        .expect("first output was read");
    let second = world
        .consumers
        .received
        .get(&second)
        .expect("second output was read");
    let (first_identity, first_reference) = first.attempt.identity();
    let (second_identity, second_reference) = second.attempt.identity();
    assert_eq!(first_identity, second_identity);
    assert_ne!(first_reference, second_reference);
    assert_eq!(first.attempt.bytes(), second.attempt.bytes());
}

async fn settle(
    world: &mut ScenarioWorld,
    batch: &str,
    decision: EmitterBatchDecision,
) -> EmitterSettlement {
    let received = world
        .consumers
        .received
        .get(batch)
        .unwrap_or_else(|| panic!("output batch '{batch}' was not read"));
    match &received.attempt {
        Attempt::Native(delivery) => match decision {
            EmitterBatchDecision::Ack => delivery.ack().await.expect("ACK reply"),
            EmitterBatchDecision::Retry => delivery.retry().await.expect("retry reply"),
            EmitterBatchDecision::Reject(reason) => {
                delivery.reject(reason).await.expect("reject reply")
            }
        },
        Attempt::WebSocket(delivery) => {
            let ScenarioConsumer::WebSocket { session, id } = world
                .consumers
                .open
                .get(&received.consumer)
                .expect("consumer remains open")
            else {
                panic!("consumer transport changed");
            };
            let raw = world
                .producers
                .sessions
                .get_mut(session)
                .expect("console session remains connected");
            let request = raw
                .send(ClientRequest::SettleEmitterBatch(
                    SettleEmitterBatchRequest {
                        consumer: *id,
                        reference: delivery.reference,
                        decision,
                    },
                ))
                .await
                .expect("console ACK sent");
            let reply = raw
                .reply(request, OPEN_WAIT)
                .await
                .expect("console ACK confirmed");
            let ReplyBody::SettleEmitterBatch(outcome) = reply.body else {
                panic!("console ACK got {:?}", reply.body);
            };
            outcome.disposition
        }
    }
}

#[when(expr = "consumer {string} closes")]
async fn when_consumer_closes(world: &mut ScenarioWorld, consumer: String) {
    let attached = world
        .consumers
        .open
        .remove(&consumer)
        .unwrap_or_else(|| panic!("consumer '{consumer}' is not open"));
    match attached {
        ScenarioConsumer::Native(native) => {
            let native = StdArc::try_unwrap(native)
                .unwrap_or_else(|_| panic!("consumer '{consumer}' still has handles"));
            native
                .close()
                .await
                .expect("native consumer close answered");
        }
        ScenarioConsumer::WebSocket { session, id } => {
            let raw = world
                .producers
                .sessions
                .get_mut(&session)
                .expect("console session remains connected");
            let request = raw
                .send(ClientRequest::CloseEmitter(CloseEmitterRequest {
                    consumer: id,
                }))
                .await
                .expect("console consumer close sent");
            let reply = raw
                .reply(request, OPEN_WAIT)
                .await
                .expect("console consumer close answered");
            assert!(matches!(reply.body, ReplyBody::CloseEmitter(_)));
        }
    }
}

#[then(expr = "consumer {string} eventually ends")]
async fn then_consumer_ends(world: &mut ScenarioWorld, consumer: String) {
    let attached = world
        .consumers
        .open
        .get(&consumer)
        .unwrap_or_else(|| panic!("consumer '{consumer}' is not open"));
    match attached {
        ScenarioConsumer::Native(client) => {
            assert!(
                nervix_primitives::time::timeout(OPEN_WAIT, client.next_batch())
                    .await
                    .expect("consumer end deadline")
                    .expect("consumer end reply")
                    .is_none()
            );
        }
        ScenarioConsumer::WebSocket { session, id } => {
            let raw = world
                .producers
                .sessions
                .get_mut(session)
                .expect("console session remains connected");
            let request = raw
                .send(ClientRequest::ReadEmitterBatch(ReadEmitterBatchRequest {
                    consumer: *id,
                }))
                .await
                .expect("console consumer end read sent");
            let reply = raw
                .reply(request, OPEN_WAIT)
                .await
                .expect("console consumer end read answered");
            let ReplyBody::ReadEmitterBatch(outcome) = reply.body else {
                panic!("console consumer end read got {:?}", reply.body);
            };
            assert!(matches!(outcome.disposition, ReadEmitterDisposition::Ended));
        }
    }
}
