use rumqttc::{AsyncClient, Event, MqttOptions, QoS, TlsConfiguration, Transport as MqttTransport};
use url::{Host, Url};

use super::*;

pub(in crate::runtime) struct MqttEmitter {
    client: Option<AsyncClient>,
}

impl MqttEmitter {
    pub(in crate::runtime) fn new(
        client: &CreateClientMqtt,
        resolved: Option<&ResolvedClientConfig>,
        context: &EmitterSinkContext,
    ) -> EmitterRuntimeResult<Self> {
        let (client, mut eventloop) = Self::client_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
            context.emitter.as_str(),
        )?;
        let domain = context.domain.clone();
        let emitter = context.emitter.clone();
        let events = context.events.clone();
        tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                match eventloop.poll().await {
                    Ok(Event::Incoming(_)) | Ok(Event::Outgoing(_)) => {}
                    Err(error) => {
                        let _ = events.send(RuntimeEvent::Error(format!(
                            "mqtt emitter event loop failed for '{}' in domain '{}': {}",
                            emitter.as_str(),
                            domain.as_str(),
                            error
                        )));
                        warn!(
                            domain = domain.as_str(),
                            emitter = emitter.as_str(),
                            error = %error,
                            "mqtt emitter event loop failed"
                        );
                        break;
                    }
                }
            }
        });
        Ok(Self {
            client: Some(client),
        })
    }

    fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
        default_client_id: &str,
    ) -> EmitterRuntimeResult<(AsyncClient, rumqttc::EventLoop)> {
        let addr = emitter_config_value(config, "addr", || {
            "missing MQTT client config key 'addr'".to_string()
        })?;
        let client_id = optional_client_config_value(config, "client_id")
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| default_client_id.to_string());

        let mqtt_addr = Self::parse_addr(&addr)?;
        let mut options = MqttOptions::new(client_id, mqtt_addr.host, mqtt_addr.port);
        options.set_clean_session(true);
        if mqtt_addr.tls {
            let tls = client_tls_paths(config);
            let ca = if let Some(ca_file) = tls.ca_file.as_ref() {
                emitter_read_tls_file(ca_file, "TLS CA certificate")?
            } else {
                return Err(emitter_config_error(
                    "MQTT TLS requires client config key 'tls_ca_file'",
                ));
            };
            let client_auth = match (&tls.cert_file, &tls.key_file) {
                (Some(cert_file), Some(key_file)) => Some((
                    emitter_read_tls_file(cert_file, "TLS certificate")?,
                    emitter_read_tls_file(key_file, "TLS private key")?,
                )),
                (None, None) => None,
                _ => {
                    return Err(emitter_config_error(
                        "MQTT TLS client authentication requires both 'tls_cert_file' and \
                         'tls_key_file'",
                    ));
                }
            };
            options.set_transport(MqttTransport::Tls(TlsConfiguration::Simple {
                ca,
                alpn: None,
                client_auth,
            }));
        }
        Ok(AsyncClient::new(options, 1024))
    }

    fn parse_addr(addr: &str) -> EmitterRuntimeResult<MqttEmitterAddr> {
        let url = Url::parse(addr).map_err(|source| {
            emitter_config_error(format!("invalid MQTT addr '{addr}': {source}"))
        })?;
        let tls = if url.scheme() == "mqtt" {
            false
        } else if url.scheme() == "mqtts" {
            true
        } else {
            return Err(emitter_config_error(format!(
                "unsupported MQTT addr scheme '{}', expected mqtt:// or mqtts://",
                url.scheme()
            )));
        };
        let host = url
            .host()
            .map(|host| match host {
                Host::Domain(domain) => domain.to_string(),
                Host::Ipv4(addr) => addr.to_string(),
                Host::Ipv6(addr) => addr.to_string(),
            })
            .filter(|host| !host.is_empty())
            .ok_or_else(|| emitter_config_error(format!("missing host in MQTT addr '{addr}'")))?;
        let port = url
            .port()
            .ok_or_else(|| emitter_config_error(format!("missing port in MQTT addr '{addr}'")))?;
        Ok(MqttEmitterAddr { host, port, tls })
    }

    pub(in crate::runtime) async fn publish(
        &mut self,
        topic: &Identifier,
        payload: &[u8],
    ) -> EmitterRuntimeResult<()> {
        let Some(client) = self.client.as_mut() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized mqtt sink client"));
        };
        client
            .publish(topic.as_str(), QoS::AtMostOnce, false, payload.to_vec())
            .await
            .map_err(emitter_publish_error)
    }
}

struct MqttEmitterAddr {
    host: String,
    port: u16,
    tls: bool,
}
