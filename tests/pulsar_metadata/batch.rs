//! A broker-stored batch with independently specified member metadata.
//!
//! Layer: test harness.
//! - **Owns.** One bounded protocol publication and the broker receipt identifying its batch.
//! - **Depends on.** The scenario world, Pulsar wire codec and primitive TCP transport.
//! - **Must not know.** The Nervix ingestor or the driver's consumer metadata decoder.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns an independent broker publication fixture")
)]

use std::{net::SocketAddr, time::Duration};

use cucumber::when;
use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{net::TcpStream, task::consume_budget, time::timeout};
use prost::Message as _;
use pulsar::message::{
    Codec, Message, Payload,
    proto::{
        BaseCommand, CommandConnect, CommandPong, CommandProducer, CommandSend, CommandSendReceipt,
        KeyValue, MessageMetadata, SingleMessageMetadata, base_command::Type as CommandType,
    },
};
use tokio_util::codec::Framed;

use super::{ScenarioWorld, expand_placeholders};

#[when(expr = "a Pulsar batch with distinct member metadata is published to topic {string}")]
async fn publish_batch(world: &mut ScenarioWorld, topic: String) {
    let topic = format!(
        "persistent://public/default/{}",
        expand_placeholders(world, &topic)
    );
    let endpoint = url::Url::parse(&expand_placeholders(world, "{{pulsar_addr}}"))
        .assured("the fixture broker declares its Pulsar endpoint");
    let address = SocketAddr::new(
        endpoint
            .host_str()
            .verified("the fixture broker endpoint has a host")
            .parse()
            .assured("the test environment publishes the broker on a literal loopback address"),
        endpoint
            .port()
            .verified("the fixture broker publishes its port"),
    );
    let producer = format!("metadata_batch_{}", world.test_id);
    let receipt = timeout(Duration::from_secs(30), publish(address, &topic, &producer))
        .await
        .assured("the complete batch publication is bounded");
    assert_eq!(receipt.sequence_id, 41);
    let id = receipt
        .message_id
        .verified("the broker identifies the stored batch");
    for index in 0..3 {
        world.placeholders.insert(
            format!("pulsar_batch_id_{}", index + 1),
            format!(
                "{}:{}:{}:{index}",
                id.ledger_id,
                id.entry_id,
                id.partition.unwrap_or(-1)
            ),
        );
    }
    world
        .placeholders
        .insert("pulsar_batch_producer".into(), producer);
}

async fn publish(address: SocketAddr, topic: &str, producer: &str) -> CommandSendReceipt {
    let socket = TcpStream::connect(address)
        .await
        .assured("the real fixture broker accepts its batch producer");
    let mut connection = Framed::new(socket, Codec);
    send(
        &mut connection,
        BaseCommand {
            r#type: CommandType::Connect as i32,
            connect: Some(CommandConnect {
                client_version: "Nervix metadata fixture".into(),
                protocol_version: Some(19),
                ..Default::default()
            }),
            ..Default::default()
        },
        None,
    )
    .await;
    receive(&mut connection, CommandType::Connected).await;
    send(
        &mut connection,
        BaseCommand {
            r#type: CommandType::Producer as i32,
            producer: Some(CommandProducer {
                topic: topic.into(),
                producer_id: 1,
                request_id: 1,
                producer_name: Some(producer.into()),
                ..Default::default()
            }),
            ..Default::default()
        },
        None,
    )
    .await;
    receive(&mut connection, CommandType::ProducerSuccess).await;
    eprintln!(
        "Pulsar batch fixture publishes envelope event_time=1767225600456 and member sequences \
         41/42/99"
    );
    send(
        &mut connection,
        BaseCommand {
            r#type: CommandType::Send as i32,
            send: Some(CommandSend {
                producer_id: 1,
                sequence_id: 41,
                highest_sequence_id: Some(99),
                num_messages: Some(3),
                ..Default::default()
            }),
            ..Default::default()
        },
        Some(Payload {
            metadata: MessageMetadata {
                producer_name: producer.into(),
                sequence_id: 41,
                highest_sequence_id: Some(99),
                publish_time: 1_767_225_600_123,
                event_time: Some(1_767_225_600_456),
                num_messages_in_batch: Some(3),
                ..Default::default()
            },
            data: encoded_members(),
        }),
    )
    .await;
    receive(&mut connection, CommandType::SendReceipt)
        .await
        .command
        .send_receipt
        .verified("the broker confirms the stored batch")
}

fn encoded_members() -> Vec<u8> {
    let mut bytes = Vec::new();
    for (index, mut metadata) in [
        SingleMessageMetadata {
            partition_key: Some("batch-key".into()),
            ordering_key: Some(vec![0, 255]),
            event_time: Some(1_767_225_600_000),
            ..Default::default()
        },
        SingleMessageMetadata {
            sequence_id: Some(42),
            partition_key: Some(String::new()),
            ordering_key: Some(Vec::new()),
            event_time: Some(0),
            ..Default::default()
        },
        SingleMessageMetadata {
            sequence_id: Some(99),
            ..Default::default()
        },
    ]
    .into_iter()
    .enumerate()
    {
        let payload = format!("{{\"seq\":{}}}", index + 1).into_bytes();
        metadata.payload_size =
            i32::try_from(payload.len()).assured("the fixed fixture payload is bounded");
        metadata.properties = vec![KeyValue {
            key: "Case".into(),
            value: "kept".into(),
        }];
        let metadata = metadata.encode_to_vec();
        bytes.extend_from_slice(
            &u32::try_from(metadata.len())
                .assured("the fixed member metadata is bounded")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&metadata);
        bytes.extend_from_slice(&payload);
    }
    bytes
}

async fn send(
    connection: &mut Framed<TcpStream, Codec>,
    command: BaseCommand,
    payload: Option<Payload>,
) {
    connection
        .send(Message { command, payload })
        .await
        .assured("the fixture command reaches the broker");
}

async fn receive(connection: &mut Framed<TcpStream, Codec>, expected: CommandType) -> Message {
    loop {
        consume_budget().await;
        let frame = connection
            .next()
            .await
            .verified("the broker keeps the publication connection open")
            .assured("the broker sends a valid Pulsar frame");
        if frame.command.r#type == CommandType::Ping as i32 {
            send(
                connection,
                BaseCommand {
                    r#type: CommandType::Pong as i32,
                    pong: Some(CommandPong {}),
                    ..Default::default()
                },
                None,
            )
            .await;
            continue;
        }
        assert_eq!(frame.command.r#type, expected as i32, "{:?}", frame.command);
        return frame;
    }
}
