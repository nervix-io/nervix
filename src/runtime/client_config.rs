use std::{fs, path::PathBuf};

use upon::Engine as TemplateEngine;

use super::*;

#[derive(Debug, Clone)]
pub(super) struct ClientTlsPaths {
    pub(super) ca_file: Option<PathBuf>,
    pub(super) cert_file: Option<PathBuf>,
    pub(super) key_file: Option<PathBuf>,
}

impl ClientTlsPaths {
    pub(super) fn is_empty(&self) -> bool {
        self.ca_file.is_none() && self.cert_file.is_none() && self.key_file.is_none()
    }
}

pub(super) fn render_client_config_template<T: serde::Serialize>(
    template_engine: &TemplateEngine<'_>,
    key: &str,
    value: &str,
    context: &T,
) -> Result<String, String> {
    let template = template_engine.compile(value).map_err(|source| {
        format!(
            "failed to compile client config template for '{}={value}': {source:#}",
            key
        )
    })?;
    template
        .render(template_engine, context)
        .to_string()
        .map_err(|source| {
            format!(
                "failed to render client config template for '{}={value}': {source:#}",
                key
            )
        })
}

pub(super) fn client_tls_paths(config: &[nervix_models::ClientConfigEntry]) -> ClientTlsPaths {
    ClientTlsPaths {
        ca_file: super::optional_client_config_value(config, "tls_ca_file").map(PathBuf::from),
        cert_file: super::optional_client_config_value(config, "tls_cert_file").map(PathBuf::from),
        key_file: super::optional_client_config_value(config, "tls_key_file").map(PathBuf::from),
    }
}

pub(super) fn client_identity_pem(tls: &ClientTlsPaths) -> Result<Option<Vec<u8>>, String> {
    match (&tls.cert_file, &tls.key_file) {
        (Some(cert_file), Some(key_file)) => {
            let mut pem = read_tls_file(cert_file, "TLS certificate")?;
            pem.extend(read_tls_file(key_file, "TLS private key")?);
            Ok(Some(pem))
        }
        (None, None) => Ok(None),
        _ => Err(
            "TLS client authentication requires both 'tls_cert_file' and 'tls_key_file'"
                .to_string(),
        ),
    }
}

pub(super) fn read_tls_file(path: &PathBuf, label: &str) -> Result<Vec<u8>, String> {
    fs::read(path)
        .map_err(|source| format!("failed to read {label} '{}': {source}", path.display()))
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ParsedRetryPolicy {
    pub(super) backoff: Duration,
    pub(super) max_backoff: Duration,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ResolvedClientConfig {
    pub(crate) entries: Vec<nervix_models::ClientConfigEntry>,
    pub(crate) mounts: Option<Arc<ClientResourceMounts>>,
}

#[derive(Debug)]
pub(crate) struct ClientResourceMounts {
    pub(super) _root: TempDir,
    pub(super) _aliases: BTreeMap<String, PathBuf>,
}

/// The entries a client connects with: the resolved ones once the control plane has rendered
/// them, and the entries written on the model until then.
pub(super) fn client_config_entries<'a>(
    resolved: Option<&'a ResolvedClientConfig>,
    declared: &'a [nervix_models::ClientConfigEntry],
) -> &'a [nervix_models::ClientConfigEntry] {
    match resolved {
        Some(resolved) => resolved.entries.as_slice(),
        None => declared,
    }
}

pub(super) fn client_config_value(
    config: &[nervix_models::ClientConfigEntry],
    key: &str,
    missing_message: impl FnOnce() -> String,
) -> Result<String, String> {
    let entry = config
        .iter()
        .find(|entry| entry.key.eq_ignore_ascii_case(key));
    match entry {
        Some(entry) => Ok(entry.value.clone()),
        None => Err(missing_message()),
    }
}

pub(super) fn optional_client_config_value<'a>(
    config: &'a [nervix_models::ClientConfigEntry],
    key: &str,
) -> Option<&'a str> {
    config
        .iter()
        .find(|entry| entry.key.eq_ignore_ascii_case(key))
        .map(|entry| entry.value.as_str())
}

pub(super) fn optional_bool_client_config_value(
    config: &[nervix_models::ClientConfigEntry],
    key: &str,
) -> Result<Option<bool>, String> {
    let Some(value) = optional_client_config_value(config, key) else {
        return Ok(None);
    };

    if value.eq_ignore_ascii_case("true") {
        Ok(Some(true))
    } else if value.eq_ignore_ascii_case("false") {
        Ok(Some(false))
    } else {
        Err(format!(
            "invalid boolean client config key '{key}' value '{value}'"
        ))
    }
}

pub(super) fn next_retry_delay(current: Duration, policy: ParsedRetryPolicy) -> Duration {
    current
        .checked_mul(2)
        .unwrap_or(policy.max_backoff)
        .min(policy.max_backoff)
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        ClientConfigEntry, CreateClientHttp, CreateClientPrometheus, CreateClientWebsockets,
        CreateClientZeroMq,
    };
    use tokio::time::Duration;

    use super::*;

    #[test]
    fn next_retry_delay_doubles_and_caps() {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        };

        assert_eq!(
            next_retry_delay(policy.backoff, policy),
            Duration::from_millis(200)
        );
        assert_eq!(
            next_retry_delay(Duration::from_millis(700), policy),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn client_config_extractors_handle_defaults_and_missing_keys() {
        let zeromq = CreateClientZeroMq {
            name: named("zmq"),
            mount: None,
            config: vec![
                ClientConfigEntry {
                    key: "addr".to_string(),
                    value: "tcp://127.0.0.1:5555".to_string(),
                },
                ClientConfigEntry {
                    key: "bind".to_string(),
                    value: "TRUE".to_string(),
                },
            ],
        };
        assert_eq!(
            ingestors::zeromq::ZeroMqIngestor::addr_from_config(&zeromq.config).expect("addr"),
            "tcp://127.0.0.1:5555"
        );
        assert!(ingestors::zeromq::ZeroMqIngestor::bind_from_config(
            &zeromq.config
        ));

        let http = CreateClientHttp {
            name: named("http"),
            mount: None,
            config: vec![ClientConfigEntry {
                key: "endpoint".to_string(),
                value: "https://example.com/api".to_string(),
            }],
        };
        assert_eq!(
            ingestors::http::HttpIngestor::endpoint_from_config(&http.config).expect("endpoint"),
            "https://example.com/api"
        );
        assert_eq!(
            ingestors::http::HttpIngestor::method_from_config(&http.config)
                .expect("default method"),
            reqwest::Method::GET
        );

        let http_post = CreateClientHttp {
            name: named("http"),
            mount: None,
            config: vec![
                ClientConfigEntry {
                    key: "endpoint".to_string(),
                    value: "https://example.com/api".to_string(),
                },
                ClientConfigEntry {
                    key: "method".to_string(),
                    value: "POST".to_string(),
                },
            ],
        };
        assert_eq!(
            ingestors::http::HttpIngestor::method_from_config(&http_post.config)
                .expect("post method"),
            reqwest::Method::POST
        );
        assert!(
            ingestors::http::HttpIngestor::method_from_config(
                &CreateClientHttp {
                    name: named("http"),
                    mount: None,
                    config: vec![ClientConfigEntry {
                        key: "method".to_string(),
                        value: "NOT A METHOD".to_string(),
                    }],
                }
                .config
            )
            .is_err()
        );

        let websocket = CreateClientWebsockets {
            name: named("ws"),
            mount: None,
            signaling_protocol: None,
            config: vec![ClientConfigEntry {
                key: "endpoint".to_string(),
                value: "wss://example.com/socket".to_string(),
            }],
        };
        assert_eq!(
            ingestors::websockets::WebsocketsIngestor::endpoint_from_config(&websocket.config)
                .expect("endpoint"),
            "wss://example.com/socket"
        );

        let prometheus = CreateClientPrometheus {
            name: named("prom"),
            mount: None,
            config: vec![ClientConfigEntry {
                key: "addr".to_string(),
                value: "http://prometheus:9090".to_string(),
            }],
        };
        assert_eq!(
            ingestors::prometheus::PrometheusIngestor::addr_from_config(&prometheus.config)
                .expect("addr"),
            "http://prometheus:9090"
        );

        let zeromq_default = CreateClientZeroMq {
            name: named("zmq"),
            mount: None,
            config: vec![ClientConfigEntry {
                key: "addr".to_string(),
                value: "tcp://127.0.0.1:5555".to_string(),
            }],
        };
        assert!(!ingestors::zeromq::ZeroMqIngestor::bind_from_config(
            &zeromq_default.config
        ));

        assert!(
            ingestors::zeromq::ZeroMqIngestor::addr_from_config(
                &CreateClientZeroMq {
                    name: named("zmq"),
                    mount: None,
                    config: vec![],
                }
                .config
            )
            .expect_err("missing zeromq addr")
            .contains("missing ZeroMQ client config key 'addr'")
        );
        assert!(
            ingestors::http::HttpIngestor::endpoint_from_config(
                &CreateClientHttp {
                    name: named("http"),
                    mount: None,
                    config: vec![],
                }
                .config
            )
            .expect_err("missing http endpoint")
            .contains("missing HTTP client config key 'endpoint'")
        );
        assert!(
            ingestors::websockets::WebsocketsIngestor::endpoint_from_config(
                &CreateClientWebsockets {
                    name: named("ws"),
                    mount: None,
                    signaling_protocol: None,
                    config: vec![],
                }
                .config,
            )
            .expect_err("missing websocket endpoint")
            .contains("missing WebSockets client config key 'endpoint'")
        );
        assert!(
            ingestors::prometheus::PrometheusIngestor::addr_from_config(
                &CreateClientPrometheus {
                    name: named("prom"),
                    mount: None,
                    config: vec![],
                }
                .config
            )
            .expect_err("missing prometheus addr")
            .contains("missing Prometheus client config key 'addr'")
        );
    }
}
