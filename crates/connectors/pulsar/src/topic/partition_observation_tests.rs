//! The Pulsar partition inspector, opened through its host handle, against a scripted broker.
//!
//! Layer: test harness.
//! - **Owns.** The checks that an inspector opened through its handle counts a partitioned topic's
//!   partitions, reports a topic without partitions as unpartitioned, and reports a lookup the
//!   broker refuses and an address nothing listens at with their typed outcomes.
//! - **Depends on.** The production inspector, its host handle and the loopback broker the
//!   connection tests script.
//! - **Must not know.** Ingestors, emitters, schedules or a real broker.

use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{OutwardTlsPolicy, PartitionInspectorHost};
use nervix_execution::Executor;
use nervix_primitives::{net::TcpListener, task};
use nervix_recovery::Discarded as _;
use pulsar::message::proto::{
    BaseCommand, CommandPartitionedTopicMetadataResponse, ServerError as PulsarServerError,
    base_command::Type as CommandType,
    command_partitioned_topic_metadata_response::LookupType as PartitionLookupType,
};

use super::*;
use crate::topic::connection::tests::{ScriptedBroker, answer, connected, entry, read_connect};

/// The handle a host gives an inspector on a node with the default outward TLS policy.
fn host() -> PartitionInspectorHost {
    PartitionInspectorHost::new(Executor::default(), OutwardTlsPolicy::default())
}

fn topic() -> PulsarTopicName {
    PulsarTopicName::try_from("events").assured("the short topic is valid")
}

/// A broker that accepts every connection and answers each partition lookup with what `answering`
/// returns for the lookup's request.
async fn broker_answering<Answering>(answering: Answering) -> ScriptedBroker
where
    Answering: Fn(u64) -> CommandPartitionedTopicMetadataResponse + Clone + Send + Sync + 'static,
{
    ScriptedBroker::start(move |mut frames| {
        let answering = answering.clone();
        async move {
            let _accepted = read_connect(&mut frames).await;
            frames
                .send(connected())
                .await
                .discarded("a client that left stops reading the answer");
            while let Some(Ok(frame)) = frames.next().await {
                task::consume_budget().await;
                let Some(lookup) = frame.command.partition_metadata else {
                    continue;
                };
                let answered = answer(BaseCommand {
                    r#type: i32::from(CommandType::PartitionedMetadataResponse),
                    partition_metadata_response: Some(answering(lookup.request_id)),
                    ..Default::default()
                });
                frames
                    .send(answered)
                    .await
                    .discarded("a client that left stops reading the answer");
            }
        }
    })
    .await
}

/// The successful answer to the lookup of request `request_id`, naming `count` partitions.
fn partitions(request_id: u64, count: u32) -> CommandPartitionedTopicMetadataResponse {
    CommandPartitionedTopicMetadataResponse {
        request_id,
        response: Some(i32::from(PartitionLookupType::Success)),
        partitions: Some(count),
        ..Default::default()
    }
}

#[nervix_primitives::test]
async fn an_inspector_opened_through_its_handle_counts_a_partitioned_topics_partitions() {
    let broker = broker_answering(|request_id| partitions(request_id, 4)).await;
    let config = [entry("addr", &broker.service_url)];

    let inspector = PulsarTopicPartitionInspector::connect(&config, &topic(), &host())
        .await
        .assured("the scripted broker accepts the inspector's client");

    assert_eq!(
        inspector
            .partitions()
            .await
            .assured("the broker answers the lookup"),
        ExternalPartitionSet::Partitioned(NonZeroU32::new(4).assured("four is not zero"))
    );
}

#[nervix_primitives::test]
async fn an_inspector_reports_a_topic_without_partitions_as_unpartitioned() {
    let broker = broker_answering(|request_id| partitions(request_id, 0)).await;
    let config = [entry("addr", &broker.service_url)];

    let inspector = PulsarTopicPartitionInspector::connect(&config, &topic(), &host())
        .await
        .assured("the scripted broker accepts the inspector's client");

    assert_eq!(
        inspector
            .partitions()
            .await
            .assured("the broker answers the lookup"),
        ExternalPartitionSet::Unpartitioned
    );
}

#[nervix_primitives::test]
async fn a_lookup_the_broker_refuses_is_the_inspectors_lookup_failure() {
    let broker = broker_answering(|request_id| CommandPartitionedTopicMetadataResponse {
        request_id,
        response: Some(i32::from(PartitionLookupType::Failed)),
        error: Some(i32::from(PulsarServerError::ServiceNotReady)),
        message: Some("the namespace is loading".to_string()),
        ..Default::default()
    })
    .await;
    let config = [entry("addr", &broker.service_url)];
    let inspector = PulsarTopicPartitionInspector::connect(&config, &topic(), &host())
        .await
        .assured("the scripted broker accepts the inspector's client");

    let Err(failure) = inspector.partitions().await else {
        panic!("the broker refuses every lookup");
    };

    assert!(matches!(
        failure.current_context(),
        PulsarPartitionObservationError::Lookup
    ));
}

#[nervix_primitives::test]
async fn an_address_nothing_listens_at_is_the_inspectors_connection_failure() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .assured("a loopback port is available");
    let address = listener
        .local_addr()
        .assured("a bound listener has an address");
    drop(listener);
    let config = [entry("addr", &format!("pulsar://{address}"))];

    let Err(failure) = PulsarTopicPartitionInspector::connect(&config, &topic(), &host()).await
    else {
        panic!("nothing listens at the address");
    };

    assert!(matches!(
        failure.current_context(),
        PulsarPartitionObservationError::Connect
    ));
}
