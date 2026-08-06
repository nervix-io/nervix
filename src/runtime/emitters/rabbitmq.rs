use futures_util::FutureExt;
use lapin::{
    Confirmation, Connection, ConnectionProperties, PublisherConfirm,
    message::BasicReturnMessage,
    options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions},
    tcp::OwnedTLSConfig,
    types::{AMQPValue, FieldTable},
};

use super::*;

pub(in crate::runtime) struct RabbitMqEmitter {
    channel: Option<lapin::Channel>,
    mode: BrokerPublishingMode,
}

struct PendingRabbitMqConfirmation {
    position: BrokerRecordPosition,
    acks: AckSet,
    deadline: Instant,
    confirmation: PublisherConfirm,
}

impl RabbitMqEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientRabbitMq,
        resolved: Option<&ResolvedClientConfig>,
        queue: &Identifier,
        mode: BrokerPublishingMode,
    ) -> EmitterRuntimeResult<Self> {
        let channel = Self::channel_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        )
        .await?;
        channel
            .queue_declare(
                queue.as_str().into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(emitter_init_error)?;
        if let BrokerPublishingMode::Ack { .. } = mode {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(emitter_init_error)?;
        }
        Ok(Self {
            channel: Some(channel),
            mode,
        })
    }

    async fn channel_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<lapin::Channel> {
        let connection = Self::connection_from_config(config).await?;
        connection
            .create_channel()
            .await
            .map_err(emitter_init_error)
    }

    async fn connection_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<Connection> {
        let addr = emitter_config_value(config, "addr", || {
            "missing RabbitMQ client config key 'addr'".to_string()
        })?;
        if emitter_service_url_has_scheme(&addr, "RabbitMQ addr", "amqps")? {
            let tls = client_tls_paths(config);
            let cert_chain = if let Some(ca_file) = tls.ca_file.as_ref() {
                Some(
                    String::from_utf8(emitter_read_tls_file(ca_file, "TLS CA certificate")?)
                        .map_err(|source| {
                            emitter_config_error(format!(
                                "failed to parse RabbitMQ CA PEM: {source}"
                            ))
                        })?,
                )
            } else {
                None
            };
            Connection::connect_with_config(
                &addr,
                ConnectionProperties::default(),
                OwnedTLSConfig {
                    identity: None,
                    cert_chain,
                },
                lapin::runtime::default_runtime().map_err(emitter_init_error)?,
            )
            .await
            .map_err(emitter_init_error)
        } else {
            Connection::connect(&addr, ConnectionProperties::default())
                .await
                .map_err(emitter_init_error)
        }
    }

    async fn publish_message(
        channel: &lapin::Channel,
        queue: &str,
        payload: &[u8],
        headers: &EmitterHeaders,
    ) -> EmitterRuntimeResult<PublisherConfirm> {
        let properties = if headers.is_empty() {
            lapin::BasicProperties::default()
        } else {
            let mut table = FieldTable::default();
            for (name, value) in headers {
                table.insert(
                    name.as_str().into(),
                    AMQPValue::LongString(value.as_str().into()),
                );
            }
            lapin::BasicProperties::default().with_headers(table)
        };
        channel
            .basic_publish(
                "".into(),
                queue.into(),
                BasicPublishOptions {
                    mandatory: true,
                    ..Default::default()
                },
                payload,
                properties,
            )
            .await
            .map_err(emitter_publish_error)
    }

    pub(in crate::runtime) async fn publish(
        &self,
        queue: &Identifier,
        payload: &[u8],
        headers: &EmitterHeaders,
    ) -> EmitterRuntimeResult<()> {
        let Some(channel) = self.channel.as_ref() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized rabbitmq sink client"));
        };
        let confirmation = Self::publish_message(channel, queue.as_str(), payload, headers).await?;
        match self.mode {
            BrokerPublishingMode::NoAck => match confirmation.await {
                Ok(Confirmation::NotRequested | Confirmation::Ack(None)) => Ok(()),
                Ok(Confirmation::Ack(Some(returned))) => {
                    Err(Self::returned_message_error(&returned))
                }
                Ok(Confirmation::Nack(_)) => Err(emitter_publish_error(
                    "rabbitmq channel acceptance returned nack",
                )),
                Err(source) => Err(emitter_publish_error(source)),
            },
            BrokerPublishingMode::Ack { timeout, .. } => {
                match tokio::time::timeout(timeout, confirmation).await {
                    Ok(Ok(Confirmation::Ack(None))) => Ok(()),
                    Ok(Ok(Confirmation::Ack(Some(returned)))) => {
                        Err(Self::returned_message_error(&returned))
                    }
                    Ok(Ok(Confirmation::Nack(_))) => Err(emitter_publish_error(
                        "rabbitmq publisher confirm returned nack",
                    )),
                    Ok(Ok(Confirmation::NotRequested)) => Err(emitter_publish_error(
                        "rabbitmq publisher confirms were not enabled",
                    )),
                    Ok(Err(source)) => Err(emitter_publish_error(source)),
                    Err(_) => Err(emitter_publish_error(format!(
                        "rabbitmq publisher confirm exceeded ACK TIMEOUT {}",
                        humantime::format_duration(timeout)
                    ))),
                }
            }
        }
    }

    pub(super) async fn publish_records(
        &self,
        queue: &Identifier,
        records: Vec<EncodedBrokerRecord>,
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        let Some(channel) = self.channel.as_ref() else {
            outcome.fail(
                Report::new(EmitterRuntimeError::SinkNotInitialized)
                    .attach_printable("no initialized rabbitmq sink client"),
            );
            return outcome;
        };
        outcome.delivered.reserve(records.len());
        let mut pending: VecDeque<PendingRabbitMqConfirmation> = VecDeque::new();
        for record in records {
            tokio::task::consume_budget().await;
            let enqueue_acks = AckSet::merged(
                pending
                    .iter()
                    .map(|confirmation| confirmation.acks.clone())
                    .chain(std::iter::once(record.acks.clone())),
            );
            let position = (record.batch_index, record.row_index);
            let confirmation = match await_emitter_confirmation(
                &enqueue_acks,
                Self::publish_message(channel, queue.as_str(), &record.payload, &record.headers),
            )
            .await
            {
                Ok(confirmation) => confirmation,
                Err(error) => {
                    outcome.fail(error);
                    return outcome;
                }
            };
            match self.mode {
                BrokerPublishingMode::NoAck => {
                    match await_emitter_confirmation(&record.acks, confirmation).await {
                        Ok(Confirmation::NotRequested | Confirmation::Ack(None)) => {
                            outcome.deliver(position);
                        }
                        Ok(Confirmation::Ack(Some(returned))) => {
                            if Self::is_returned_record_rejection(&returned) {
                                outcome.reject(position, Self::returned_message_reason(&returned));
                            } else {
                                outcome.fail(Self::returned_message_error(&returned));
                                return outcome;
                            }
                        }
                        Ok(Confirmation::Nack(_)) => {
                            outcome.fail(emitter_publish_error(
                                "rabbitmq channel acceptance returned nack",
                            ));
                            return outcome;
                        }
                        Err(error) => {
                            outcome.fail(emitter_publish_error(error));
                            return outcome;
                        }
                    }
                }
                BrokerPublishingMode::Ack {
                    max_in_flight,
                    timeout,
                } => {
                    pending.push_back(PendingRabbitMqConfirmation {
                        position,
                        acks: record.acks,
                        deadline: Instant::now() + timeout,
                        confirmation,
                    });
                    if pending.len() >= max_in_flight
                        && let Err(error) =
                            Self::confirm_oldest(&mut pending, timeout, &mut outcome).await
                    {
                        outcome.fail(error);
                        return outcome;
                    }
                }
            }
        }
        while !pending.is_empty() {
            tokio::task::consume_budget().await;
            let timeout = match self.mode {
                BrokerPublishingMode::Ack { timeout, .. } => timeout,
                BrokerPublishingMode::NoAck => unreachable!("NO_ACK has no confirmations"),
            };
            if let Err(error) = Self::confirm_oldest(&mut pending, timeout, &mut outcome).await {
                outcome.fail(error);
                return outcome;
            }
        }
        outcome
    }

    async fn confirm_oldest(
        pending: &mut VecDeque<PendingRabbitMqConfirmation>,
        timeout: Duration,
        outcome: &mut PerRecordPublishOutcome,
    ) -> EmitterRuntimeResult<()> {
        loop {
            tokio::task::consume_budget().await;
            for confirmation in pending.iter() {
                confirmation.acks.ack_alive();
            }
            let Some(oldest) = pending.front_mut() else {
                return Err(emitter_publish_error(
                    "rabbitmq acknowledgment window unexpectedly became empty",
                ));
            };
            let remaining = oldest
                .deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                return Err(emitter_publish_error(format!(
                    "rabbitmq publisher confirm exceeded ACK TIMEOUT {}",
                    humantime::format_duration(timeout)
                )));
            }
            let wait = remaining.min(REMOTE_ACK_ALIVE_INTERVAL);
            let result = tokio::select! {
                biased;
                result = &mut oldest.confirmation => Some(result),
                _ = sleep(wait) => None,
            };
            let Some(result) = result else {
                continue;
            };
            let position = oldest.position;
            match result {
                Ok(Confirmation::Ack(None)) => {
                    pending.pop_front();
                    outcome.deliver(position);
                    return Ok(());
                }
                Ok(Confirmation::Ack(Some(returned))) => {
                    if Self::is_returned_record_rejection(&returned) {
                        pending.pop_front();
                        outcome.reject(position, Self::returned_message_reason(&returned));
                        return Ok(());
                    }
                    Self::harvest_ready_after_oldest_failure(pending, outcome);
                    return Err(Self::returned_message_error(&returned));
                }
                Ok(Confirmation::Nack(Some(returned)))
                    if Self::is_returned_record_rejection(&returned) =>
                {
                    pending.pop_front();
                    outcome.reject(position, Self::returned_message_reason(&returned));
                    return Ok(());
                }
                Ok(Confirmation::Nack(_)) => {
                    Self::harvest_ready_after_oldest_failure(pending, outcome);
                    return Err(emitter_publish_error(
                        "rabbitmq publisher confirm returned nack",
                    ));
                }
                Ok(Confirmation::NotRequested) => {
                    Self::harvest_ready_after_oldest_failure(pending, outcome);
                    return Err(emitter_publish_error(
                        "rabbitmq publisher confirms were not enabled",
                    ));
                }
                Err(source) => {
                    Self::harvest_ready_after_oldest_failure(pending, outcome);
                    return Err(emitter_publish_error(source));
                }
            }
        }
    }

    fn harvest_ready_after_oldest_failure(
        pending: &mut VecDeque<PendingRabbitMqConfirmation>,
        outcome: &mut PerRecordPublishOutcome,
    ) {
        let mut index = 1;
        while index < pending.len() {
            let ready = pending
                .get_mut(index)
                .and_then(|confirmation| (&mut confirmation.confirmation).now_or_never());
            let Some(result) = ready else {
                index += 1;
                continue;
            };
            let confirmation = pending
                .remove(index)
                .expect("ready RabbitMQ confirmation must remain in the window");
            match result {
                Ok(Confirmation::Ack(None)) => outcome.deliver(confirmation.position),
                Ok(Confirmation::Ack(Some(returned)) | Confirmation::Nack(Some(returned)))
                    if Self::is_returned_record_rejection(&returned) =>
                {
                    outcome.reject(
                        confirmation.position,
                        Self::returned_message_reason(&returned),
                    );
                }
                Ok(
                    Confirmation::Ack(Some(_)) | Confirmation::Nack(_) | Confirmation::NotRequested,
                )
                | Err(_) => {}
            }
        }
    }

    fn is_returned_record_rejection(returned: &BasicReturnMessage) -> bool {
        returned.reply_code == 311
    }

    fn returned_message_reason(returned: &BasicReturnMessage) -> String {
        format!(
            "rabbitmq returned message with reply code {}: {}",
            returned.reply_code, returned.reply_text
        )
    }

    fn returned_message_error(returned: &BasicReturnMessage) -> Report<EmitterRuntimeError> {
        emitter_publish_error(Self::returned_message_reason(returned))
    }
}

#[cfg(test)]
mod tests {
    use lapin::message::Delivery;

    use super::*;

    fn returned_message(reply_code: u16, reply_text: &str) -> BasicReturnMessage {
        BasicReturnMessage {
            delivery: Delivery::mock(0, "".into(), "notifications".into(), false, Vec::new()),
            reply_code,
            reply_text: reply_text.into(),
        }
    }

    #[test]
    fn content_too_large_return_is_a_record_rejection() {
        assert!(RabbitMqEmitter::is_returned_record_rejection(
            &returned_message(311, "CONTENT_TOO_LARGE")
        ));
    }

    #[test]
    fn no_route_return_remains_an_infrastructure_failure() {
        assert!(!RabbitMqEmitter::is_returned_record_rejection(
            &returned_message(312, "NO_ROUTE")
        ));
    }
}
