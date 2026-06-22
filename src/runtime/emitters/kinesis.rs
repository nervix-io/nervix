use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_kinesis::{Client as KinesisClient, primitives::Blob as KinesisBlob};

use super::*;

pub(in crate::runtime) struct KinesisEmitter {
    client: Option<KinesisClient>,
}

impl KinesisEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientKinesis,
        resolved: Option<&ResolvedClientConfig>,
    ) -> EmitterRuntimeResult<Self> {
        let client = Self::client_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        )
        .await?;
        Ok(Self {
            client: Some(client),
        })
    }

    async fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<KinesisClient> {
        let region = optional_client_config_value(config, "region")
            .unwrap_or("us-east-1")
            .to_string();
        let access_key_id = optional_client_config_value(config, "access_key_id")
            .unwrap_or("x")
            .to_string();
        let secret_access_key = optional_client_config_value(config, "secret_access_key")
            .unwrap_or("x")
            .to_string();

        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_kinesis::config::Region::new(region))
            .credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                "nervix-kinesis",
            ));
        if let Some(endpoint) = optional_client_config_value(config, "endpoint") {
            loader = loader.endpoint_url(endpoint.to_string());
        }
        if let Some(ca_file) = client_tls_paths(config).ca_file.as_ref() {
            let ca_pem = emitter_read_tls_file(ca_file, "TLS CA certificate")?;
            let tls_context = aws_smithy_http_client::tls::TlsContext::builder()
                .with_trust_store(
                    aws_smithy_http_client::tls::TrustStore::empty().with_pem_certificate(ca_pem),
                )
                .build()
                .map_err(emitter_init_error)?;
            let http_client = aws_smithy_http_client::Builder::new()
                .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                    aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
                ))
                .tls_context(tls_context)
                .build_https();
            loader = loader.http_client(http_client);
        }
        let sdk_config = loader.load().await;
        Ok(KinesisClient::new(&sdk_config))
    }

    pub(in crate::runtime) async fn publish(
        &mut self,
        relay: &Identifier,
        message: &RelayMessage,
        payload: &[u8],
    ) -> EmitterRuntimeResult<()> {
        let Some(client) = self.client.as_mut() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized kinesis sink client"));
        };
        let partition_key = message
            .key
            .as_ref()
            .map(|key| key.as_str().to_string())
            .unwrap_or_else(|| "nervix".to_string());
        client
            .put_record()
            .stream_name(relay.as_str())
            .partition_key(partition_key)
            .data(KinesisBlob::new(payload.to_vec()))
            .send()
            .await
            .map(|_| ())
            .map_err(emitter_publish_error)
    }
}
