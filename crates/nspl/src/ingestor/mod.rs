use chumsky::prelude::*;
use nervix_models::{
    CreateIngestor, CreateStatement, EndpointIngestMode, IngestSource, IngestTimestampSource,
    KafkaIngestMode, KafkaOffsetMode, KinesisIngestMode, MqttIngestMode, MqttQos, MqttSession,
    NatsIngestMode, PulsarIngestMode, RabbitMqIngestMode, RedisPubSubIngestMode, RetryPolicy,
    SqsIngestMode, WebsocketsIngestMode, ZeroMqIngestMode,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, branch_initiator_selection, channel_ref, client_ref,
        codec_ref, consumer_group_ref, current_word_prefix, duration_lit, endpoint_ref,
        error_policies, filter_where_clause, flush_each, if_not_exists_clause, ingestor_name,
        ingestor_outputs, into_parse_error, kw, kw_phrase2, lex_input, mqtt_topic_filter,
        nats_queue_group_ref, queue_ref, relay_ref, string_lit, subscription_ref,
        suggestions_from_errors, tok, topic_ref, word_raw,
    },
};

fn u64_word<'src>() -> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone {
    choice((select! { Token::NumberLiteral(v) => v }, word_raw())).try_map(|raw, span| {
        raw.parse::<u64>()
            .map_err(|_| Rich::custom(span, format!("invalid integer '{raw}'")))
    })
}

fn positive_u64_word<'src>()
-> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone {
    u64_word().try_map(|value, span| {
        if value == 0 {
            Err(Rich::custom(span, "instances must be greater than 0"))
        } else {
            Ok(value)
        }
    })
}

fn ack_timeout_parser<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(kw(Identifier::Timeout))
        .ignore_then(duration_lit())
}

fn batch_timeout_parser<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Batch)
        .ignore_then(kw(Identifier::Timeout))
        .ignore_then(duration_lit())
}

fn retry_policy_parser<'src>()
-> impl Parser<'src, &'src [Token], RetryPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Retry)
        .ignore_then(kw(Identifier::Policy))
        .ignore_then(kw(Identifier::Backoff))
        .ignore_then(duration_lit())
        .then_ignore(kw(Identifier::Max))
        .then(duration_lit())
        .map(|(backoff, max_backoff)| RetryPolicy {
            backoff,
            max_backoff,
        })
}

fn timestamp_source<'src>()
-> impl Parser<'src, &'src [Token], IngestTimestampSource, extra::Err<ParseError<'src>>> + Clone {
    let at_field = kw(Identifier::At).ignore_then(word_raw().try_map(|raw, span| {
        nervix_models::Identifier::try_from(raw.as_str())
            .map(IngestTimestampSource::At)
            .map_err(|err| Rich::custom(span, format!("invalid identifier: {err}")))
    }));

    kw(Identifier::Timestamp).ignore_then(choice((
        kw(Identifier::Now).to(IngestTimestampSource::Now),
        at_field,
    )))
}

fn mode_parser<'src>()
-> impl Parser<'src, &'src [Token], KafkaIngestMode, extra::Err<ParseError<'src>>> + Clone {
    let ack_timeout = ack_timeout_parser();
    let batch_timeout = batch_timeout_parser();
    let retry_policy = retry_policy_parser();
    choice((
        kw(Identifier::Ack)
            .ignore_then(kw(Identifier::Parallel))
            .ignore_then(kw(Identifier::Max))
            .ignore_then(positive_u64_word())
            .then(batch_timeout)
            .then(ack_timeout.clone())
            .then(retry_policy.clone())
            .map(
                |(((max, batch_timeout), timeout), retry_policy)| KafkaIngestMode::AckParallel {
                    max,
                    batch_timeout,
                    timeout,
                    retry_policy,
                },
            ),
        kw(Identifier::Ack)
            .ignore_then(kw(Identifier::Sequential))
            .ignore_then(ack_timeout)
            .then(retry_policy)
            .map(|(timeout, retry_policy)| KafkaIngestMode::AckSequential {
                timeout,
                retry_policy,
            }),
        kw(Identifier::NoAck)
            .ignore_then(kw(Identifier::Parallel))
            .ignore_then(kw(Identifier::Max))
            .ignore_then(positive_u64_word())
            .map(|max| KafkaIngestMode::NoAckParallel { max }),
    ))
}

fn pulsar_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], PulsarIngestMode, extra::Err<ParseError<'src>>> + Clone {
    mode_parser()
}

fn rabbitmq_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], RabbitMqIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(kw(Identifier::Sequential))
        .ignore_then(kw(Identifier::Ack))
        .ignore_then(kw(Identifier::Timeout))
        .ignore_then(duration_lit())
        .then_ignore(kw(Identifier::Retry))
        .then_ignore(kw(Identifier::Policy))
        .then_ignore(kw(Identifier::Backoff))
        .then(duration_lit())
        .then_ignore(kw(Identifier::Max))
        .then(duration_lit())
        .map(
            |((timeout, backoff), max_backoff)| RabbitMqIngestMode::AckSequential {
                timeout,
                retry_policy: RetryPolicy {
                    backoff,
                    max_backoff,
                },
            },
        )
}

fn kinesis_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], KinesisIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(kw(Identifier::Sequential))
        .ignore_then(kw(Identifier::Ack))
        .ignore_then(kw(Identifier::Timeout))
        .ignore_then(duration_lit())
        .then_ignore(kw(Identifier::Retry))
        .then_ignore(kw(Identifier::Policy))
        .then_ignore(kw(Identifier::Backoff))
        .then(duration_lit())
        .then_ignore(kw(Identifier::Max))
        .then(duration_lit())
        .map(
            |((timeout, backoff), max_backoff)| KinesisIngestMode::AckSequential {
                timeout,
                retry_policy: RetryPolicy {
                    backoff,
                    max_backoff,
                },
            },
        )
}

fn redis_pubsub_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], RedisPubSubIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::NoAck)
        .ignore_then(kw(Identifier::Sequential))
        .to(RedisPubSubIngestMode::NoAckSequential)
}

#[derive(Clone)]
enum ParsedMqttIngestMode {
    NoAckSequential,
    NoAckParallel {
        max: u64,
    },
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
    AckParallel {
        max: u64,
        batch_timeout: String,
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

fn mqtt_session_clause<'src>()
-> impl Parser<'src, &'src [Token], MqttSession, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Session).ignore_then(choice((
        kw(Identifier::Clean).to(MqttSession::Clean),
        kw(Identifier::Persistent).to(MqttSession::Persistent),
    )))
}

fn mqtt_qos_clause<'src>()
-> impl Parser<'src, &'src [Token], MqttQos, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Qos).ignore_then(select! { Token::NumberLiteral(raw) => raw }.try_map(
        |raw, span| match raw.as_str() {
            "0" => Ok(MqttQos::AtMostOnce),
            "1" => Ok(MqttQos::AtLeastOnce),
            _ => Err(Rich::custom(span, "MQTT QOS must be 0 or 1")),
        },
    ))
}

fn mqtt_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], MqttIngestMode, extra::Err<ParseError<'src>>> + Clone {
    let ack_timeout = ack_timeout_parser();
    let batch_timeout = batch_timeout_parser();
    let retry_policy = retry_policy_parser();

    let raw_mode = choice((
        kw(Identifier::Ack)
            .ignore_then(kw(Identifier::Parallel))
            .ignore_then(kw(Identifier::Max))
            .ignore_then(positive_u64_word())
            .then(batch_timeout)
            .then(ack_timeout.clone())
            .then(retry_policy.clone())
            .map(|(((max, batch_timeout), timeout), retry_policy)| {
                ParsedMqttIngestMode::AckParallel {
                    max,
                    batch_timeout,
                    timeout,
                    retry_policy,
                }
            }),
        kw(Identifier::Ack)
            .ignore_then(kw(Identifier::Sequential))
            .ignore_then(ack_timeout)
            .then(retry_policy)
            .map(
                |(timeout, retry_policy)| ParsedMqttIngestMode::AckSequential {
                    timeout,
                    retry_policy,
                },
            ),
        kw(Identifier::NoAck)
            .ignore_then(kw(Identifier::Parallel))
            .ignore_then(kw(Identifier::Max))
            .ignore_then(positive_u64_word())
            .map(|max| ParsedMqttIngestMode::NoAckParallel { max }),
        kw(Identifier::NoAck)
            .ignore_then(kw(Identifier::Sequential))
            .to(ParsedMqttIngestMode::NoAckSequential),
    ));

    mqtt_session_clause()
        .or_not()
        .map(|session| session.unwrap_or(MqttSession::Clean))
        .then(
            mqtt_qos_clause()
                .or_not()
                .map(|qos| qos.unwrap_or(MqttQos::AtMostOnce)),
        )
        .then_ignore(kw(Identifier::Mode))
        .then(raw_mode)
        .try_map(|((session, qos), mode), span| match mode {
            ParsedMqttIngestMode::NoAckSequential => {
                Ok(MqttIngestMode::NoAckSequential { session, qos })
            }
            ParsedMqttIngestMode::NoAckParallel { max } => {
                Ok(MqttIngestMode::NoAckParallel { max, session, qos })
            }
            ParsedMqttIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => {
                if session != MqttSession::Persistent {
                    return Err(Rich::custom(
                        span,
                        "MQTT ACK modes require SESSION PERSISTENT",
                    ));
                }
                if qos != MqttQos::AtLeastOnce {
                    return Err(Rich::custom(span, "MQTT ACK modes require QOS 1"));
                }
                Ok(MqttIngestMode::AckSequential {
                    timeout,
                    retry_policy,
                })
            }
            ParsedMqttIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            } => {
                if session != MqttSession::Persistent {
                    return Err(Rich::custom(
                        span,
                        "MQTT ACK modes require SESSION PERSISTENT",
                    ));
                }
                if qos != MqttQos::AtLeastOnce {
                    return Err(Rich::custom(span, "MQTT ACK modes require QOS 1"));
                }
                Ok(MqttIngestMode::AckParallel {
                    max,
                    batch_timeout,
                    timeout,
                    retry_policy,
                })
            }
        })
}

fn nats_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], NatsIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::NoAck)
        .ignore_then(kw(Identifier::Sequential))
        .to(NatsIngestMode::NoAckSequential)
}

fn endpoint_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], EndpointIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::NoAck)
        .ignore_then(kw(Identifier::Sequential))
        .to(EndpointIngestMode::NoAckSequential)
}

fn websockets_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], WebsocketsIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::NoAck)
        .ignore_then(kw(Identifier::Sequential))
        .to(WebsocketsIngestMode::NoAckSequential)
}

fn zeromq_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], ZeroMqIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::NoAck)
        .ignore_then(kw(Identifier::Sequential))
        .to(ZeroMqIngestMode::NoAckSequential)
}

fn sqs_mode_parser<'src>()
-> impl Parser<'src, &'src [Token], SqsIngestMode, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(kw(Identifier::Sequential))
        .ignore_then(kw(Identifier::Ack))
        .ignore_then(kw(Identifier::Timeout))
        .ignore_then(duration_lit())
        .then_ignore(kw(Identifier::Retry))
        .then_ignore(kw(Identifier::Policy))
        .then_ignore(kw(Identifier::Backoff))
        .then(duration_lit())
        .then_ignore(kw(Identifier::Max))
        .then(duration_lit())
        .map(
            |((timeout, backoff), max_backoff)| SqsIngestMode::AckSequential {
                timeout,
                retry_policy: RetryPolicy {
                    backoff,
                    max_backoff,
                },
            },
        )
}

fn kafka_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    let offset_mode = kw(Identifier::Offset)
        .ignore_then(kw(Identifier::By))
        .ignore_then(choice((
            kw_phrase2(Identifier::Consumer, Identifier::Group)
                .ignore_then(consumer_group_ref())
                .map(KafkaOffsetMode::ConsumerGroup),
            kw(Identifier::Domain).to(KafkaOffsetMode::Domain),
        )));
    kw(Identifier::Kafka)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Topic))
        .then(topic_ref())
        .then(offset_mode)
        .then(
            kw(Identifier::Instances)
                .ignore_then(positive_u64_word())
                .or_not()
                .map(|instances| instances.unwrap_or(1)),
        )
        .then_ignore(kw(Identifier::Mode))
        .then(mode_parser())
        .map(
            |((((client, topic), offset_mode), instances), mode)| IngestSource::Kafka {
                client,
                topic,
                offset_mode,
                instances,
                mode,
            },
        )
}

fn pulsar_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Pulsar)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Topic))
        .then(topic_ref())
        .then_ignore(kw(Identifier::Subscription))
        .then(subscription_ref())
        .then(
            kw(Identifier::Instances)
                .ignore_then(positive_u64_word())
                .or_not()
                .map(|instances| instances.unwrap_or(1)),
        )
        .then_ignore(kw(Identifier::Mode))
        .then(pulsar_mode_parser())
        .map(
            |((((client, topic), subscription), instances), mode)| IngestSource::Pulsar {
                client,
                topic,
                subscription,
                instances,
                mode,
            },
        )
}

fn rabbitmq_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Rabbitmq)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Queue))
        .then(queue_ref())
        .then(
            kw(Identifier::Instances)
                .ignore_then(positive_u64_word())
                .or_not()
                .map(|instances| instances.unwrap_or(1)),
        )
        .then_ignore(kw(Identifier::Mode))
        .then(rabbitmq_mode_parser())
        .map(
            |(((client, queue), instances), mode)| IngestSource::RabbitMq {
                client,
                queue,
                instances,
                mode,
            },
        )
}

fn kinesis_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Kinesis)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Relay))
        .then(relay_ref())
        .then(
            kw(Identifier::Instances)
                .ignore_then(positive_u64_word())
                .or_not()
                .map(|instances| instances.unwrap_or(1)),
        )
        .then_ignore(kw(Identifier::Mode))
        .then(kinesis_mode_parser())
        .map(
            |(((client, relay), instances), mode)| IngestSource::Kinesis {
                client,
                relay,
                instances,
                mode,
            },
        )
}

fn redis_pubsub_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Redis)
        .ignore_then(kw(Identifier::Pubsub))
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Channel))
        .then(channel_ref())
        .then_ignore(kw(Identifier::Mode))
        .then(redis_pubsub_mode_parser())
        .map(|((client, channel), mode)| IngestSource::RedisPubSub {
            client,
            channel,
            mode,
        })
}

fn mqtt_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Mqtt)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Topic))
        .then(mqtt_topic_filter())
        .then(
            kw(Identifier::Instances)
                .ignore_then(positive_u64_word())
                .or_not()
                .map(|instances| instances.unwrap_or(1)),
        )
        .then(mqtt_mode_parser())
        .map(|(((client, topic), instances), mode)| IngestSource::Mqtt {
            client,
            topic,
            instances,
            mode,
        })
}

fn prometheus_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Prometheus)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Query))
        .then(string_lit())
        .then_ignore(kw(Identifier::Every))
        .then(duration_lit().try_map(|every, span| {
            humantime::parse_duration(&every)
                .map(|_| every.clone())
                .map_err(|err| Rich::custom(span, format!("invalid duration '{every}': {err}")))
        }))
        .map(|((client, query), every)| IngestSource::Prometheus {
            client,
            query,
            every,
        })
}

fn http_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Http)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Every))
        .then(duration_lit().try_map(|every, span| {
            humantime::parse_duration(&every)
                .map(|_| every.clone())
                .map_err(|err| Rich::custom(span, format!("invalid duration '{every}': {err}")))
        }))
        .map(|(client, every)| IngestSource::Http { client, every })
}

fn nats_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Nats)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Subject))
        .then(topic_ref())
        .then_ignore(kw_phrase2(Identifier::Queue, Identifier::Group))
        .then(nats_queue_group_ref())
        .then_ignore(kw(Identifier::Instances))
        .then(positive_u64_word())
        .then_ignore(kw(Identifier::Mode))
        .then(nats_mode_parser())
        .map(
            |((((client, subject), queue_group), instances), mode)| IngestSource::Nats {
                client,
                subject,
                queue_group,
                instances,
                mode,
            },
        )
}

fn zeromq_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Zeromq)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Mode))
        .then(zeromq_mode_parser())
        .map(|(client, mode)| IngestSource::ZeroMq { client, mode })
}

fn sqs_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Sqs)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Queue))
        .then(queue_ref())
        .then(
            kw(Identifier::Instances)
                .ignore_then(positive_u64_word())
                .or_not()
                .map(|instances| instances.unwrap_or(1)),
        )
        .then_ignore(kw(Identifier::Mode))
        .then(sqs_mode_parser())
        .map(|(((client, queue), instances), mode)| IngestSource::Sqs {
            client,
            queue,
            instances,
            mode,
        })
}

fn endpoint_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Endpoint)
        .ignore_then(endpoint_ref())
        .then_ignore(kw(Identifier::Mode))
        .then(endpoint_mode_parser())
        .map(|(endpoint, mode)| IngestSource::Endpoint { endpoint, mode })
}

fn websockets_ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Websockets)
        .ignore_then(client_ref())
        .then_ignore(kw(Identifier::Mode))
        .then(websockets_mode_parser())
        .map(|(client, mode)| IngestSource::Websockets { client, mode })
}

fn ingest_source_parser<'src>()
-> impl Parser<'src, &'src [Token], IngestSource, extra::Err<ParseError<'src>>> + Clone {
    choice((
        http_ingest_source_parser(),
        kinesis_ingest_source_parser(),
        kafka_ingest_source_parser(),
        pulsar_ingest_source_parser(),
        rabbitmq_ingest_source_parser(),
        redis_pubsub_ingest_source_parser(),
        mqtt_ingest_source_parser(),
        nats_ingest_source_parser(),
        prometheus_ingest_source_parser(),
        zeromq_ingest_source_parser(),
        sqs_ingest_source_parser(),
        endpoint_ingest_source_parser(),
        websockets_ingest_source_parser(),
    ))
}

pub fn create_ingestor_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateIngestor>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Ingestor))
        .then(ingestor_name())
        .then(filter_where_clause().or_not())
        .then(ingestor_outputs())
        .then_ignore(kw_phrase2(Identifier::Decode, Identifier::Using))
        .then(codec_ref())
        .then(branch_initiator_selection())
        .then(flush_each())
        .then(timestamp_source().or_not())
        .then_ignore(kw(Identifier::From))
        .then(ingest_source_parser())
        .then(error_policies())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(
                (
                    (
                        (
                            (
                                (
                                    (((if_not_exists, name), filter_where), output_routes),
                                    decode_using_codec,
                                ),
                                branched_by,
                            ),
                            flush_each,
                        ),
                        timestamp_source,
                    ),
                    source,
                ),
                error_policies,
            )| {
                let (flush_each, max_batch_size) = flush_each;
                CreateStatement::new(
                    CreateIngestor {
                        name,
                        output_routes,
                        decode_using_codec,
                        branched_by,
                        flush_each,
                        max_batch_size,
                        timestamp_source,
                        source,
                        error_policies,
                        filter_where,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn parse_create_ingestor_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateIngestor>, Vec<ParseError<'_>>> {
    let out = create_ingestor_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_ingestor(
    input: &str,
) -> Result<CreateStatement<CreateIngestor>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_ingestor_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_ingestor(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_ingestor_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex;

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn parses_create_ingestor_ack_parallel() {
        let input = r#"
            CREATE INGESTOR kafka_notifications
                TO notifications
                DECODE USING notification_kafka_message
                BRANCHED BY notification_params VALUES { user_id = notifications.user_id }
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                FROM
                    KAFKA kafka_main
                    TOPIC notifications
                    OFFSET BY CONSUMER GROUP nervix_consumer
                    MODE ACK PARALLEL MAX 10 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "kafka_notifications");
        assert_eq!(
            parsed
                .output_routes
                .outputs()
                .map(|output| output.relay.as_str())
                .collect::<Vec<_>>(),
            vec!["notifications"]
        );
        assert_eq!(
            parsed.decode_using_codec.as_str(),
            "notification_kafka_message"
        );
        assert_eq!(
            parsed.branched_by.branch().map(|branch| branch.as_str()),
            Some("notification_params")
        );
        assert_eq!(parsed.timestamp_source, None);
        assert_eq!(
            parsed.source,
            IngestSource::Kafka {
                client: nervix_models::Identifier::try_from("kafka_main")
                    .expect("valid client identifier"),
                topic: nervix_models::Identifier::try_from("notifications")
                    .expect("valid topic identifier"),
                offset_mode: KafkaOffsetMode::ConsumerGroup(
                    nervix_models::Identifier::try_from("nervix_consumer")
                        .expect("valid consumer group identifier"),
                ),
                instances: 1,
                mode: KafkaIngestMode::AckParallel {
                    max: 10,
                    batch_timeout: "500ms".to_string(),
                    timeout: "30s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "200ms".to_string(),
                        max_backoff: "5s".to_string(),
                    },
                },
            }
        );
        assert_eq!(parsed.flush_each, "100ms");
    }

    #[test]
    fn parses_create_ingestor_unbranched() {
        let input = r#"
            CREATE INGESTOR http_notifications
                TO notifications
                DECODE USING notification_codec
                UNBRANCHED
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                FROM ENDPOINT http_notifications_endpoint
                MODE NO_ACK SEQUENTIAL
                ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.branched_by,
            nervix_models::BranchInitiatorSelection::unbranched()
        );
        assert!(parsed.branched_by.is_unbranched());
    }

    #[test]
    fn parses_branched_by_with_values_block() {
        let input = r#"
            CREATE INGESTOR i
              TO s
              DECODE USING sch
              BRANCHED BY u_branch VALUES { u = s.u }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM ENDPOINT ep MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(
            parsed.branched_by.branch().map(|branch| branch.as_str()),
            Some("u_branch")
        );
        assert_eq!(parsed.branched_by.values().len(), 1);
        assert_eq!(parsed.branched_by.values()[0].field.as_str(), "u");
        assert_eq!(parsed.branched_by.values()[0].relay.as_str(), "s");
        assert_eq!(parsed.branched_by.values()[0].relay_field.as_str(), "u");
    }

    #[test]
    fn rejects_bare_by() {
        let input = r#"
            CREATE INGESTOR i
              TO s
              DECODE USING sch
              BY u_branch
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM ENDPOINT ep MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("BY is only valid in CREATE BRANCH");
    }

    #[test]
    fn rejects_unbranched_with_ttl() {
        let input = r#"
            CREATE INGESTOR i
              TO s
              DECODE USING sch
              UNBRANCHED TTL 5m
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM ENDPOINT ep MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("TTL must not follow UNBRANCHED");
    }

    #[test]
    fn parses_create_ingestor_ack_sequential() {
        let input = r#"
            CREATE INGESTOR i
              TO s
              DECODE USING sch
              BRANCHED BY u_branch VALUES { u = s.u }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM KAFKA t TOPIC top OFFSET BY CONSUMER GROUP g MODE ACK SEQUENTIAL ACK TIMEOUT 15s RETRY POLICY BACKOFF 250ms MAX 8s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        let IngestSource::Kafka {
            instances, mode, ..
        } = &parsed.source
        else {
            panic!("expected kafka ingestor source");
        };
        assert_eq!(*instances, 1);
        assert_eq!(
            mode,
            &KafkaIngestMode::AckSequential {
                timeout: "15s".to_string(),
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "8s".to_string(),
                }
            }
        );
    }

    #[test]
    fn parses_create_ingestor_kinesis_ack_sequential() {
        let input = r#"
            CREATE INGESTOR kinesis_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM KINESIS kinesis_main RELAY notifications INSTANCES 2 MODE ACK SEQUENTIAL ACK TIMEOUT 15s RETRY POLICY BACKOFF 250ms MAX 8s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.source,
            IngestSource::Kinesis {
                client: nervix_models::Identifier::try_from("kinesis_main")
                    .expect("valid client identifier"),
                relay: nervix_models::Identifier::try_from("notifications")
                    .expect("valid relay identifier"),
                instances: 2,
                mode: KinesisIngestMode::AckSequential {
                    timeout: "15s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "8s".to_string(),
                    },
                },
            }
        );
    }

    #[test]
    fn parses_create_ingestor_no_ack_parallel() {
        let input = r#"
            CREATE INGESTOR i
              TO s
              DECODE USING sch
              BRANCHED BY u_branch VALUES { u = s.u }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM KAFKA t TOPIC top OFFSET BY CONSUMER GROUP g MODE NO_ACK PARALLEL MAX 20 ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        let IngestSource::Kafka { mode, .. } = &parsed.source else {
            panic!("expected kafka ingestor source");
        };
        assert_eq!(mode, &KafkaIngestMode::NoAckParallel { max: 20 });
    }

    #[test]
    fn parses_create_ingestor_pulsar_ack_sequential() {
        let input = r#"
            CREATE INGESTOR pulsar_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM PULSAR pulsar_main TOPIC notifications SUBSCRIPTION nervix_subscription MODE ACK SEQUENTIAL ACK TIMEOUT 15s RETRY POLICY BACKOFF 250ms MAX 8s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.source,
            IngestSource::Pulsar {
                client: nervix_models::Identifier::try_from("pulsar_main")
                    .expect("valid client identifier"),
                topic: nervix_models::Identifier::try_from("notifications")
                    .expect("valid topic identifier"),
                subscription: nervix_models::Identifier::try_from("nervix_subscription")
                    .expect("valid subscription identifier"),
                instances: 1,
                mode: PulsarIngestMode::AckSequential {
                    timeout: "15s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "8s".to_string(),
                    },
                },
            }
        );
    }

    #[test]
    fn rejects_create_ingestor_pulsar_without_subscription() {
        let input = r#"
            CREATE INGESTOR pulsar_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM PULSAR pulsar_main TOPIC notifications MODE ACK SEQUENTIAL ACK TIMEOUT 15s RETRY POLICY BACKOFF 250ms MAX 8s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let error = parse_create_ingestor_tokens(&tokens).expect_err("parse should fail");
        assert!(!error.is_empty());
    }

    #[test]
    fn parses_create_ingestor_rabbitmq_ack_sequential() {
        let input = r#"
            CREATE INGESTOR rabbit_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM RABBITMQ rabbit_main QUEUE notifications MODE ACK SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "rabbit_notifications");
        assert_eq!(
            parsed
                .output_routes
                .outputs()
                .map(|output| output.relay.as_str())
                .collect::<Vec<_>>(),
            vec!["notifications"]
        );
        assert_eq!(
            parsed.source,
            IngestSource::RabbitMq {
                client: nervix_models::Identifier::try_from("rabbit_main")
                    .expect("valid client identifier"),
                queue: nervix_models::Identifier::try_from("notifications")
                    .expect("valid queue identifier"),
                instances: 1,
                mode: RabbitMqIngestMode::AckSequential {
                    timeout: "20s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "1s".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
            }
        );
    }

    #[test]
    fn parses_create_ingestor_redis_pubsub_no_ack_sequential() {
        let input = r#"
            CREATE INGESTOR redis_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM REDIS PUBSUB redis_main CHANNEL notifications MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "redis_notifications");
        assert_eq!(
            parsed.source,
            IngestSource::RedisPubSub {
                client: nervix_models::Identifier::try_from("redis_main")
                    .expect("valid client identifier"),
                channel: nervix_models::Identifier::try_from("notifications")
                    .expect("valid channel identifier"),
                mode: RedisPubSubIngestMode::NoAckSequential,
            }
        );
    }

    #[test]
    fn rejects_redis_pubsub_ingestor_without_pubsub_action() {
        let input = r#"
            CREATE INGESTOR redis_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM REDIS redis_main CHANNEL notifications MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let errs = parse_create_ingestor_tokens(&tokens).expect_err("old syntax must fail");
        assert!(!errs.is_empty());
    }

    #[test]
    fn suggests_pubsub_action_after_redis_source() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = \
                     s.u } FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM REDIS ";
        let suggestions = suggest_create_ingestor(input, input.len());

        assert!(suggestions.contains(&"PUBSUB".to_string()));
        assert!(!suggestions.contains(&"CHANNEL".to_string()));
    }

    #[test]
    fn suggests_mode_options() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = \
                     s.u } FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM KAFKA t TOPIC top OFFSET BY \
                     CONSUMER GROUP g MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"ACK".to_string()));
        assert!(suggestions.contains(&"NO_ACK".to_string()));
    }

    #[test]
    fn suggests_source_transport_kinds_after_from() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = \
                     s.u } FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"HTTP".to_string()));
        assert!(suggestions.contains(&"KINESIS".to_string()));
        assert!(suggestions.contains(&"KAFKA".to_string()));
        assert!(suggestions.contains(&"PULSAR".to_string()));
        assert!(suggestions.contains(&"PROMETHEUS".to_string()));
        assert!(suggestions.contains(&"RABBITMQ".to_string()));
        assert!(suggestions.contains(&"REDIS".to_string()));
        assert!(suggestions.contains(&"MQTT".to_string()));
        assert!(suggestions.contains(&"NATS".to_string()));
        assert!(suggestions.contains(&"ZEROMQ".to_string()));
        assert!(suggestions.contains(&"SQS".to_string()));
        assert!(suggestions.contains(&"WEBSOCKETS".to_string()));
    }

    #[test]
    fn suggests_values_after_branched_by() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"VALUES".to_string()));
        assert!(!suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!suggestions.contains(&"TTL".to_string()));
        assert!(!suggestions.contains(&"TIMESTAMP".to_string()));
    }

    #[test]
    fn suggests_flush_after_branch_values_without_transport_leakage() {
        let input =
            "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = s.u } ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(!suggestions.contains(&"TIMESTAMP".to_string()));
        assert!(!suggestions.contains(&"FROM".to_string()));
    }

    #[test]
    fn pulsar_mode_context_does_not_offer_kafka_offset_keywords() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = \
                     s.u } FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM PULSAR p TOPIC top \
                     SUBSCRIPTION sub MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"ACK".to_string()));
        assert!(suggestions.contains(&"NO_ACK".to_string()));
        assert!(!suggestions.contains(&"OFFSET".to_string()));
        assert!(!suggestions.contains(&"CONSUMER GROUP".to_string()));
    }

    #[test]
    fn pulsar_subscription_context_expects_subscription_name() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = \
                     s.u } FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM PULSAR p TOPIC top \
                     SUBSCRIPTION ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"subscription_name".to_string()));
        assert!(!suggestions.contains(&"SHARED".to_string()));
        assert!(!suggestions.contains(&"MODE".to_string()));
    }

    #[test]
    fn rejects_create_ingestor_pulsar_shared_subscription_keyword() {
        let input = r#"
            CREATE INGESTOR pulsar_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM PULSAR pulsar_main TOPIC notifications SUBSCRIPTION SHARED nervix_subscription MODE ACK SEQUENTIAL ACK TIMEOUT 15s RETRY POLICY BACKOFF 250ms MAX 8s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("Pulsar subscription type is not part of the public grammar");
    }

    #[test]
    fn parses_create_ingestor_flush_each() {
        let input = r#"
            CREATE INGESTOR kafka_notifications
                TO notifications
                DECODE USING notification_kafka_message
                BRANCHED BY user_id_kind_branch VALUES { user_id = notifications.user_id }
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                FROM
                    KAFKA kafka_main
                    TOPIC notifications
                    OFFSET BY CONSUMER GROUP nervix_consumer
                    MODE ACK PARALLEL MAX 10 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.flush_each, "100ms");
    }

    #[test]
    fn parses_create_ingestor_flush_immediate() {
        let input = r#"
            CREATE INGESTOR i
              TO s
              DECODE USING sch
              BRANCHED BY u_branch VALUES { u = s.u }
              FLUSH IMMEDIATE
              FROM ENDPOINT ep MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor_tokens(&to_tokens(input)).expect("parse should succeed");
        assert_eq!(parsed.flush_each, "IMMEDIATE");
    }

    #[test]
    fn suggests_branched_by_as_compound_keyword() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"BRANCHED BY".to_string()));
        assert!(suggestions.contains(&"UNBRANCHED".to_string()));
        assert!(!suggestions.contains(&"BRANCHED_BY".to_string()));
    }

    #[test]
    fn rejects_unbranched_with_values_block() {
        let input = r#"
            CREATE INGESTOR i
              TO s
              DECODE USING sch
              UNBRANCHED VALUES {}
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM ENDPOINT ep MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("VALUES block must not follow UNBRANCHED");
    }

    #[test]
    fn unbranched_context_suggests_flush_not_values() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch UNBRANCHED ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"FLUSH EACH".to_string()));
        assert!(suggestions.contains(&"FLUSH IMMEDIATE".to_string()));
        assert!(!suggestions.contains(&"VALUES".to_string()));
    }

    #[test]
    fn suggests_to_keyword() {
        let input = "CREATE INGESTOR i ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"TO".to_string()));
        assert!(!suggestions.contains(&"RELAY".to_string()));
    }

    #[test]
    fn suggests_decode_using_as_compound_keyword() {
        let input = "CREATE INGESTOR i TO s ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"DECODE USING".to_string()));
    }

    #[test]
    fn rabbitmq_mode_context_does_not_offer_kafka_only_modes() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = \
                     s.u } FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM RABBITMQ t QUEUE q MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"ACK".to_string()));
        assert!(!suggestions.contains(&"NO_ACK".to_string()));
        assert!(!suggestions.contains(&"PARALLEL".to_string()));
    }

    #[test]
    fn redis_pubsub_mode_context_does_not_offer_ack_or_parallel() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = \
                     s.u } FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM REDIS PUBSUB t CHANNEL c \
                     MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"NO_ACK".to_string()));
        assert!(!suggestions.contains(&"ACK".to_string()));
        assert!(!suggestions.contains(&"PARALLEL".to_string()));
    }

    #[test]
    fn parses_create_ingestor_mqtt_no_ack_sequential() {
        let input = r#"
            CREATE INGESTOR mqtt_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM MQTT mqtt_main TOPIC notifications MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "mqtt_notifications");
        assert_eq!(
            parsed.source,
            IngestSource::Mqtt {
                client: nervix_models::Identifier::try_from("mqtt_main")
                    .expect("valid client identifier"),
                topic: "notifications".to_string(),
                instances: 1,
                mode: MqttIngestMode::NoAckSequential {
                    session: MqttSession::Clean,
                    qos: MqttQos::AtMostOnce,
                },
            }
        );
    }

    #[test]
    fn parses_create_ingestor_mqtt_no_ack_parallel_with_qos1() {
        let input = r#"
            CREATE INGESTOR mqtt_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM MQTT mqtt_main TOPIC notifications QOS 1 MODE NO_ACK PARALLEL MAX 4 ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(
            parsed.source,
            IngestSource::Mqtt {
                client: nervix_models::Identifier::try_from("mqtt_main")
                    .expect("valid client identifier"),
                topic: "notifications".to_string(),
                instances: 1,
                mode: MqttIngestMode::NoAckParallel {
                    max: 4,
                    session: MqttSession::Clean,
                    qos: MqttQos::AtLeastOnce,
                },
            }
        );
    }

    #[test]
    fn parses_create_ingestor_mqtt_ack_parallel_instances() {
        let input = r#"
            CREATE INGESTOR mqtt_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM MQTT mqtt_main
              TOPIC 'devices/+/notifications'
              INSTANCES 3
              SESSION PERSISTENT QOS 1
              MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 250ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s
              ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor_tokens(&to_tokens(input)).expect("parse should succeed");

        assert_eq!(
            parsed.source,
            IngestSource::Mqtt {
                client: nervix_models::Identifier::try_from("mqtt_main")
                    .expect("valid client identifier"),
                topic: "devices/+/notifications".to_string(),
                instances: 3,
                mode: MqttIngestMode::AckParallel {
                    max: 8,
                    batch_timeout: "250ms".to_string(),
                    timeout: "5s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "100ms".to_string(),
                        max_backoff: "2s".to_string(),
                    },
                },
            }
        );
    }

    #[test]
    fn rejects_create_ingestor_mqtt_subscription_clause() {
        let input = r#"
            CREATE INGESTOR mqtt_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM MQTT mqtt_main
              TOPIC notifications
              SUBSCRIPTION SHARED nervix_group
              MODE NO_ACK SEQUENTIAL
              ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("MQTT subscription mode is not part of the public grammar");
    }

    #[test]
    fn parses_create_ingestor_mqtt_ack_sequential() {
        let input = r#"
            CREATE INGESTOR mqtt_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM MQTT mqtt_main TOPIC notifications SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor_tokens(&to_tokens(input)).expect("parse should succeed");
        let IngestSource::Mqtt { mode, .. } = &parsed.source else {
            panic!("expected mqtt ingestor source");
        };

        assert_eq!(
            mode,
            &MqttIngestMode::AckSequential {
                timeout: "5s".to_string(),
                retry_policy: RetryPolicy {
                    backoff: "100ms".to_string(),
                    max_backoff: "2s".to_string(),
                },
            }
        );
    }

    #[test]
    fn rejects_create_ingestor_mqtt_ack_without_persistent_session() {
        let input = r#"
            CREATE INGESTOR mqtt_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM MQTT mqtt_main TOPIC notifications QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("ACK mode must require SESSION PERSISTENT");
    }

    #[test]
    fn rejects_create_ingestor_mqtt_ack_without_qos1() {
        let input = r#"
            CREATE INGESTOR mqtt_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM MQTT mqtt_main TOPIC notifications SESSION PERSISTENT MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input)).expect_err("ACK mode must require QOS 1");
    }

    #[test]
    fn rejects_create_ingestor_mqtt_parallel_max_zero() {
        let input = r#"
            CREATE INGESTOR mqtt_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM MQTT mqtt_main TOPIC notifications MODE NO_ACK PARALLEL MAX 0 ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input)).expect_err("MAX must be greater than zero");
    }

    #[test]
    fn parses_create_ingestor_nats_no_ack_sequential() {
        let input = r#"
            CREATE INGESTOR nats_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM NATS nats_main SUBJECT notifications QUEUE GROUP nats_notifications_group INSTANCES 3 MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.source,
            IngestSource::Nats {
                client: nervix_models::Identifier::try_from("nats_main")
                    .expect("valid client identifier"),
                subject: nervix_models::Identifier::try_from("notifications")
                    .expect("valid topic identifier"),
                queue_group: nervix_models::Identifier::try_from("nats_notifications_group")
                    .expect("valid queue group identifier"),
                instances: 3,
                mode: NatsIngestMode::NoAckSequential,
            }
        );
    }

    #[test]
    fn rejects_create_ingestor_nats_without_queue_group() {
        let input = r#"
            CREATE INGESTOR nats_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM NATS nats_main SUBJECT notifications MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        parse_create_ingestor_tokens(&to_tokens(input))
            .expect_err("NATS ingestors must require QUEUE GROUP before MODE");
    }

    #[test]
    fn rejects_create_ingestor_nats_zero_instances() {
        let input = r#"
            CREATE INGESTOR nats_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM NATS nats_main SUBJECT notifications QUEUE GROUP nats_notifications_group INSTANCES 0 MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let error = parse_create_ingestor_tokens(&tokens)
            .expect_err("NATS INSTANCES must be greater than zero");
        assert!(
            format!("{error:?}").contains("instances must be greater than 0"),
            "expected instances validation error, got {error:?}"
        );
    }

    #[test]
    fn nats_subject_context_requires_queue_group_before_mode() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = \
                     s.u } FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM NATS t SUBJECT top ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"QUEUE GROUP".to_string()));
        assert!(!suggestions.contains(&"INSTANCES".to_string()));
        assert!(!suggestions.contains(&"MODE".to_string()));
    }

    #[test]
    fn mqtt_mode_context_offers_ack_and_no_ack() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = \
                     s.u } FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM MQTT t TOPIC top MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"ACK".to_string()));
        assert!(suggestions.contains(&"NO_ACK".to_string()));
        assert!(!suggestions.contains(&"PARALLEL".to_string()));
    }

    #[test]
    fn mqtt_session_context_offers_session_values() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = \
                     s.u } FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM MQTT t TOPIC top SESSION ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"CLEAN".to_string()));
        assert!(suggestions.contains(&"PERSISTENT".to_string()));
        assert!(!suggestions.contains(&"MODE".to_string()));
    }

    #[test]
    fn parses_create_ingestor_prometheus_query() {
        let input = r#"
            CREATE INGESTOR prom_samples
              TO samples
              DECODE USING sample_codec
              BRANCHED BY source_branch VALUES { source = samples.source }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM PROMETHEUS prom_main
              QUERY 'label_replace(vector(42.5), "source", "local", "", "")'
              EVERY 15s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.source,
            IngestSource::Prometheus {
                client: nervix_models::Identifier::try_from("prom_main")
                    .expect("valid client identifier"),
                query: r#"label_replace(vector(42.5), "source", "local", "", "")"#.to_string(),
                every: "15s".to_string(),
            }
        );
    }

    #[test]
    fn parses_create_ingestor_http_poll() {
        let input = r#"
            CREATE INGESTOR http_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM HTTP http_main EVERY 1s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.source,
            IngestSource::Http {
                client: nervix_models::Identifier::try_from("http_main")
                    .expect("valid client identifier"),
                every: "1s".to_string(),
            }
        );
    }

    #[test]
    fn kinesis_mode_context_does_not_offer_no_ack_or_parallel() {
        let input = "CREATE INGESTOR i TO s DECODE USING sch BRANCHED BY u_branch VALUES { u = \
                     s.u } FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM KINESIS c RELAY events MODE ";
        let suggestions = suggest_create_ingestor(input, input.len());
        assert!(suggestions.contains(&"ACK".to_string()));
        assert!(!suggestions.contains(&"NO_ACK".to_string()));
        assert!(!suggestions.contains(&"PARALLEL".to_string()));
    }

    #[test]
    fn parses_create_ingestor_with_timestamp_at_field() {
        let input = r#"
            CREATE INGESTOR http_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              TIMESTAMP AT occurred_at
              FROM HTTP http_main EVERY 1s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.timestamp_source,
            Some(IngestTimestampSource::At(
                nervix_models::Identifier::try_from("occurred_at").expect("valid field identifier")
            ))
        );
    }

    #[test]
    fn parses_create_ingestor_with_timestamp_now() {
        let input = r#"
            CREATE INGESTOR http_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              TIMESTAMP NOW
              FROM HTTP http_main EVERY 1s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.timestamp_source, Some(IngestTimestampSource::Now));
    }

    #[test]
    fn parses_create_ingestor_endpoint_no_ack_sequential() {
        let input = r#"
            CREATE INGESTOR ws_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.source,
            IngestSource::Endpoint {
                endpoint: nervix_models::Identifier::try_from("ws_notifications_endpoint")
                    .expect("valid endpoint identifier"),
                mode: EndpointIngestMode::NoAckSequential,
            }
        );
    }

    #[test]
    fn parses_create_ingestor_websockets_no_ack_sequential() {
        let input = r#"
            CREATE INGESTOR ws_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM WEBSOCKETS ws_main MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.source,
            IngestSource::Websockets {
                client: nervix_models::Identifier::try_from("ws_main")
                    .expect("valid client identifier"),
                mode: WebsocketsIngestMode::NoAckSequential,
            }
        );
    }

    #[test]
    fn parses_create_ingestor_zeromq_no_ack_sequential() {
        let input = r#"
            CREATE INGESTOR zmq_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM ZEROMQ zmq_main MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.source,
            IngestSource::ZeroMq {
                client: nervix_models::Identifier::try_from("zmq_main")
                    .expect("valid client identifier"),
                mode: ZeroMqIngestMode::NoAckSequential,
            }
        );
    }

    #[test]
    fn parses_create_ingestor_sqs_ack_sequential() {
        let input = r#"
            CREATE INGESTOR sqs_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM SQS sqs_main QUEUE notifications MODE ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 2s MAX 1m ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.source,
            IngestSource::Sqs {
                client: nervix_models::Identifier::try_from("sqs_main")
                    .expect("valid client identifier"),
                queue: nervix_models::Identifier::try_from("notifications")
                    .expect("valid queue identifier"),
                instances: 1,
                mode: SqsIngestMode::AckSequential {
                    timeout: "45s".to_string(),
                    retry_policy: RetryPolicy {
                        backoff: "2s".to_string(),
                        max_backoff: "1m".to_string(),
                    },
                },
            }
        );
    }

    #[test]
    fn parses_create_ingestor_kafka_instances() {
        let input = r#"
            CREATE INGESTOR i
              TO s
              DECODE USING sch
              BRANCHED BY u_branch VALUES { u = s.u }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM KAFKA t TOPIC top OFFSET BY CONSUMER GROUP g INSTANCES 3 MODE NO_ACK PARALLEL MAX 20 ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        let IngestSource::Kafka { instances, .. } = parsed.source else {
            panic!("expected kafka ingestor source");
        };
        assert_eq!(instances, 3);
    }

    #[test]
    fn parses_create_ingestor_rabbitmq_instances() {
        let input = r#"
            CREATE INGESTOR rabbit_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM RABBITMQ rabbit_main QUEUE notifications INSTANCES 2 MODE ACK SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        let IngestSource::RabbitMq { instances, .. } = parsed.source else {
            panic!("expected rabbitmq ingestor source");
        };
        assert_eq!(instances, 2);
    }

    #[test]
    fn parses_create_ingestor_sqs_instances() {
        let input = r#"
            CREATE INGESTOR sqs_notifications
              TO notifications
              DECODE USING notification_codec
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM SQS sqs_main QUEUE notifications INSTANCES 4 MODE ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 2s MAX 1m ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_ingestor_tokens(&tokens).expect("parse should succeed");
        let IngestSource::Sqs { instances, .. } = parsed.source else {
            panic!("expected sqs ingestor source");
        };
        assert_eq!(instances, 4);
    }

    #[test]
    fn rejects_zero_instances() {
        let input = r#"
            CREATE INGESTOR i
              TO s
              DECODE USING sch
              BRANCHED BY u_branch VALUES { u = s.u }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM KAFKA t TOPIC top OFFSET BY CONSUMER GROUP g INSTANCES 0 MODE NO_ACK PARALLEL MAX 20 ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let tokens = to_tokens(input);
        let error = parse_create_ingestor_tokens(&tokens).expect_err("parse should fail");
        assert!(
            format!("{error:?}").contains("instances must be greater than 0"),
            "expected instances validation error, got {error:?}"
        );
    }

    #[test]
    fn parses_ingestor_with_filter_where_and_output_routes() {
        let input = r#"
            CREATE INGESTOR kafka_notifications
              FILTER WHERE message.active
              TO notifications
                SET notifications.normalized = lower(message.name), notifications.total = message.amount AS INT64
                UNSET notifications.raw
                WHERE message.kind = "audit"
              TO audit_notifications
              DECODE USING notification_kafka_message
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM KAFKA kafka_main TOPIC notifications OFFSET BY CONSUMER GROUP nervix_consumer MODE NO_ACK PARALLEL MAX 10 ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let parsed = parse_create_ingestor(input).expect("parse should succeed");

        assert_eq!(parsed.filter_where.as_deref(), Some("WHERE message.active"));
        assert_eq!(parsed.output_routes.routes.len(), 2);
        assert_eq!(
            parsed.output_routes.routes[0].relay.as_str(),
            "notifications"
        );
        assert_eq!(
            parsed.output_routes.routes[0].filter_map.as_deref(),
            Some(
                "SET notifications.normalized = lower ( message.name ) , notifications.total = \
                 message.amount AS INT64 UNSET notifications.raw WHERE message.kind = \"audit\""
            )
        );
        assert_eq!(
            parsed.output_routes.routes[1].relay.as_str(),
            "audit_notifications"
        );
        assert_eq!(parsed.output_routes.routes[1].filter_map, None);
    }

    #[test]
    fn rejects_invalid_ingestor_output_route_program() {
        let input = r#"
            CREATE INGESTOR kafka_notifications
              TO notifications
                SET notifications.normalized =
              DECODE USING notification_kafka_message
              BRANCHED BY user_id_branch VALUES { user_id = notifications.user_id }
              FLUSH EACH 100ms MAX BATCH SIZE 1MiB
              FROM KAFKA kafka_main TOPIC notifications OFFSET BY CONSUMER GROUP nervix_consumer MODE NO_ACK PARALLEL MAX 10 ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#;

        let error = parse_create_ingestor(input).expect_err("parse should fail");
        match error {
            ParseFromSourceError::Parse { diagnostics, .. } => {
                assert!(!diagnostics.is_empty());
            }
            other => panic!("expected parse error, got {other:?}"),
        }
    }
}
