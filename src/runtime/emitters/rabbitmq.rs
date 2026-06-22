use lapin::{
    Connection, ConnectionProperties,
    options::BasicPublishOptions,
    tcp::OwnedTLSConfig,
    types::{AMQPValue, FieldTable},
};

use super::*;

pub(in crate::runtime) struct RabbitMqEmitter {
    channel: Option<lapin::Channel>,
}

impl RabbitMqEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientRabbitMq,
        resolved: Option<&ResolvedClientConfig>,
    ) -> EmitterRuntimeResult<Self> {
        let channel = Self::channel_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        )
        .await?;
        Ok(Self {
            channel: Some(channel),
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
    ) -> EmitterRuntimeResult<()> {
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
                BasicPublishOptions::default(),
                payload,
                properties,
            )
            .await
            .map_err(emitter_publish_error)?
            .await
            .map_err(emitter_publish_error)?;
        Ok(())
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
        Self::publish_message(channel, queue.as_str(), payload, headers).await
    }
}
