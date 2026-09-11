use std::time::Duration;

use reqwest::{Certificate as HttpCertificate, Client as HttpClient, Identity as HttpIdentity};

use super::{
    client_config::{client_identity_pem, client_tls_paths, read_tls_file},
    optional_client_config_value,
};

pub(in crate::runtime) struct HttpClientConfig<'a> {
    entries: &'a [nervix_models::ClientConfigEntry],
    label: &'static str,
}

impl<'a> HttpClientConfig<'a> {
    pub(in crate::runtime) fn new(
        entries: &'a [nervix_models::ClientConfigEntry],
        label: &'static str,
    ) -> Self {
        Self { entries, label }
    }

    pub(in crate::runtime) fn build(&self) -> Result<HttpClient, String> {
        self.builder()?.build().map_err(|source| source.to_string())
    }

    fn builder(&self) -> Result<reqwest::ClientBuilder, String> {
        let mut builder = HttpClient::builder();
        if let Some(timeout_ms) = optional_client_config_value(self.entries, "timeout_ms") {
            let timeout_ms = timeout_ms
                .parse::<u64>()
                .map_err(|_| format!("invalid {} timeout_ms '{timeout_ms}'", self.label))?;
            builder = builder.timeout(Duration::from_millis(timeout_ms));
        }

        let tls = client_tls_paths(self.entries);
        if let Some(ca_file) = tls.ca_file.as_ref() {
            let ca_pem = read_tls_file(ca_file, "TLS CA certificate")?;
            builder = builder.add_root_certificate(
                HttpCertificate::from_pem(&ca_pem)
                    .map_err(|source| format!("failed to parse TLS CA certificate: {source}"))?,
            );
        }
        if let Some(identity_pem) = client_identity_pem(&tls)? {
            builder = builder.identity(
                HttpIdentity::from_pem(&identity_pem)
                    .map_err(|source| format!("failed to parse TLS client identity: {source}"))?,
            );
        }
        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{ClientConfigEntry, CreateClientPrometheus};

    use super::*;
    use crate::runtime::{ingestors, named};

    #[test]
    fn http_and_prometheus_clients_validate_timeout_configuration() {
        let client = HttpClientConfig::new(
            &[ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "250".to_string(),
            }],
            "HTTP",
        )
        .build();
        assert!(client.is_ok());

        let err = HttpClientConfig::new(
            &[ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "oops".to_string(),
            }],
            "HTTP",
        )
        .build()
        .expect_err("invalid timeout");
        assert!(err.contains("invalid HTTP timeout_ms 'oops'"));

        let err = ingestors::prometheus::PrometheusIngestor::client_from_config_for_test(
            &CreateClientPrometheus {
                name: named("prom"),
                mount: None,
                config: vec![ClientConfigEntry {
                    key: "timeout_ms".to_string(),
                    value: "oops".to_string(),
                }],
            }
            .config,
        )
        .expect_err("invalid prometheus timeout");
        assert!(err.contains("Prometheus timeout_ms"));
    }
}
