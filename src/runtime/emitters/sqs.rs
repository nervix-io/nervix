use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_sqs::{Client as SqsClient, types::MessageAttributeValue};

use super::*;

pub(in crate::runtime) struct SqsEmitter {
    client: Option<SqsClient>,
}

impl SqsEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientSqs,
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
    ) -> EmitterRuntimeResult<SqsClient> {
        let endpoint = emitter_config_value(config, "endpoint", || {
            "missing SQS client config key 'endpoint'".to_string()
        })?;
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
            .region(aws_sdk_sqs::config::Region::new(region))
            .endpoint_url(endpoint)
            .credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                "nervix-sqs",
            ));
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
        Ok(SqsClient::new(&sdk_config))
    }

    async fn queue_url(client: &SqsClient, queue: &str) -> EmitterRuntimeResult<String> {
        client
            .get_queue_url()
            .queue_name(queue)
            .send()
            .await
            .map_err(emitter_publish_error)?
            .queue_url()
            .map(ToOwned::to_owned)
            .ok_or_else(|| emitter_publish_error(format!("SQS queue '{queue}' has no URL")))
    }

    pub(in crate::runtime) async fn publish(
        &mut self,
        queue: &Identifier,
        payload: &[u8],
        headers: &EmitterHeaders,
    ) -> EmitterRuntimeResult<()> {
        let Some(client) = self.client.as_mut() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized sqs sink client"));
        };
        let queue_url = Self::queue_url(client, queue.as_str()).await?;
        let mut request = client
            .send_message()
            .queue_url(queue_url)
            .message_body(String::from_utf8_lossy(payload).to_string());
        for (name, value) in headers {
            let attribute = MessageAttributeValue::builder()
                .data_type("String")
                .string_value(value)
                .build()
                .map_err(emitter_publish_error)?;
            request = request.message_attributes(name, attribute);
        }
        request
            .send()
            .await
            .map(|_| ())
            .map_err(emitter_publish_error)
    }
}
