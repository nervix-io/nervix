//! Pre-commit client validation at the server composition root.
//!
//! Layer: control plane.
//! - **Owns.** Bounded offline inspection and the confidential command-refusal projection.
//! - **Depends on.** Client declarations, resource rendering, the executor and connector parsers.
//! - **Must not know.** Connections, graph execution, delivery or connector task lifetimes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns pre-commit client inspection")
)]

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_connector::OfflineClientConfigError;
use nervix_execution::{MemoryClass, QueueAdmission, StorageClass};
use nervix_models::{
    ClientConfigEntry, ClientConfigOwner, ClientConfigSchemaError, ClientConfigValue,
    ConfiguredClientRef, ConnectorKind, DomainName, MqttClientConfig, MqttClientConfigError,
    PrometheusApiRoot, PrometheusClientConfigError, WebsocketsClientConfig,
    WebsocketsClientConfigError, validate_clickhouse_client_config,
};

use super::{ModelBindingValidationError, SessionServiceImpl};

#[derive(Debug, thiserror::Error)]
enum ClientValidationError {
    #[error("client configuration inspection size exceeds the addressable byte count")]
    SizeOverflow,
    #[error("client configuration inspection could not be admitted")]
    Admission,
    #[error("client configuration inspection was cancelled")]
    Cancelled,
    #[error("client configuration templates or mounts could not be resolved")]
    Resolve,
    #[error("rendered client configuration is invalid")]
    Schema,
    #[error("client config file key '{key}' is not a readable file")]
    File { key: &'static str },
    #[error("client driver configuration is invalid")]
    Driver,
}

impl SessionServiceImpl {
    pub(super) async fn validate_client_configuration(
        &self,
        domain: &DomainName,
        client: ConfiguredClientRef<'_, u64>,
        admission: QueueAdmission,
    ) -> error_stack::Result<(), ModelBindingValidationError> {
        let context = || ModelBindingValidationError::Client {
            domain: domain.clone(),
            node: client.name.clone(),
        };
        // The command already bounds its entries. Admit the worker's raw and rendered copies
        // before cloning; native parsers own their internal allocations.
        let overflow = || Report::new(ClientValidationError::SizeOverflow);
        let entry_bytes: u64 = std::mem::size_of::<ClientConfigEntry>().arch_into();
        let mut bytes = 0_u64;
        for entry in client.config {
            let key_bytes: u64 = entry.key.len().arch_into();
            let value_bytes: u64 = entry.value.len().arch_into();
            bytes = bytes
                .checked_add(entry_bytes)
                .ok_or_else(overflow)
                .change_context_lazy(context)?;
            bytes = bytes
                .checked_add(key_bytes)
                .ok_or_else(overflow)
                .change_context_lazy(context)?;
            bytes = bytes
                .checked_add(value_bytes)
                .ok_or_else(overflow)
                .change_context_lazy(context)?;
        }
        let bytes = bytes
            .checked_mul(2)
            .ok_or_else(overflow)
            .change_context_lazy(context)?;
        let runtime = self.inner.runtime.clone();
        let reservation = match admission {
            QueueAdmission::RefuseWhenFull => {
                runtime.executor().try_reserve(MemoryClass::Commands, bytes)
            }
            QueueAdmission::WaitForPlace => {
                runtime
                    .executor()
                    .reserve(MemoryClass::Commands, bytes)
                    .await
            }
        }
        .change_context(ClientValidationError::Admission)
        .change_context_lazy(context)?;
        let connector = client.connector;
        let worker_domain = domain.clone();
        let mount = client.mount.cloned();
        let config = client.config.to_vec();
        let execution_runtime = runtime.clone();
        let inspection = runtime.executor().run_storage_with(
            StorageClass::Filesystem,
            admission,
            reservation,
            move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(ClientValidationError::Cancelled)?;
                // A client whose templates name the instance that connects, MQTT's identity
                // template, is validated as its first instance, which an emitter also connects
                // as, using the same renderer; each ingestor instance renders its own on its host.
                let resolved = if connector.renders_client_instance() {
                    execution_runtime.resolve_client_config_with_instance(
                        &worker_domain,
                        mount.as_ref(),
                        &config,
                        0,
                    )
                } else {
                    execution_runtime.resolve_client_config(&worker_domain, mount.as_ref(), &config)
                }
                .change_context(ClientValidationError::Resolve)?;
                let declaration = connector
                    .client()
                    .assured("a configured client has a client declaration");
                let validation = if connector == ConnectorKind::ClickHouse {
                    validate_clickhouse_client_config(&resolved.entries)
                } else {
                    declaration.config.validate_rendered(&resolved.entries)
                };
                validation.change_context(ClientValidationError::Schema)?;
                Self::validate_rendered_client_values(connector, &resolved.entries)?;
                for entry in &resolved.entries {
                    cancellation
                        .check()
                        .change_context(ClientValidationError::Cancelled)?;
                    if let Some(key) = declaration.config.key(&entry.key)
                        && key.owner == ClientConfigOwner::Nervix
                        && matches!(key.value, ClientConfigValue::File(_))
                    {
                        let context = || ClientValidationError::File { key: key.name };
                        let metadata =
                            std::fs::metadata(&entry.value).change_context_lazy(context)?;
                        if !metadata.is_file() {
                            return Err(Report::new(context()));
                        }
                        let file =
                            std::fs::File::open(&entry.value).change_context_lazy(context)?;
                        if !file.metadata().change_context_lazy(context)?.is_file() {
                            return Err(Report::new(context()));
                        }
                    }
                }
                validate_driver(connector, &resolved.entries)
                    .change_context(ClientValidationError::Driver)
            },
        );
        #[cfg(feature = "testing")]
        let inspection = runtime.observe_client_inspection_admission(domain, inspection);
        let result = inspection
            .await
            .change_context(ClientValidationError::Admission)
            .change_context_lazy(context)?;
        result.change_context_lazy(context)
    }

    fn validate_rendered_client_values(
        connector: ConnectorKind,
        config: &[ClientConfigEntry],
    ) -> error_stack::Result<(), ClientValidationError> {
        match connector {
            ConnectorKind::Websockets => WebsocketsClientConfig::parse(config)
                .map(|_| ())
                .change_context(ClientValidationError::Schema),
            ConnectorKind::Prometheus => PrometheusApiRoot::of_client_config(config)
                .map(|_| ())
                .change_context(ClientValidationError::Schema),
            ConnectorKind::Mqtt => MqttClientConfig::parse(config)
                .map(|_| ())
                .change_context(ClientValidationError::Schema),
            _ => Ok(()),
        }
    }
}

fn validate_driver(
    connector: ConnectorKind,
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    match connector {
        ConnectorKind::S3 => nervix_connector_iceberg::validate_client_configuration(
            nervix_models::IcebergStorageBackend::S3,
            config,
        ),
        ConnectorKind::Gcs => nervix_connector_iceberg::validate_client_configuration(
            nervix_models::IcebergStorageBackend::Gcs,
            config,
        ),
        ConnectorKind::AzureBlob => nervix_connector_iceberg::validate_client_configuration(
            nervix_models::IcebergStorageBackend::AzureBlob,
            config,
        ),
        ConnectorKind::IcebergRest => {
            nervix_connector_iceberg::validate_catalog_configuration(config)
        }
        ConnectorKind::Kafka => nervix_connector_kafka::validate_client_configuration(config),
        ConnectorKind::Postgres => nervix_connector_postgres::validate_client_configuration(config),
        ConnectorKind::MySql => nervix_connector_mysql::validate_client_configuration(config),
        ConnectorKind::MongoDb => nervix_connector_mongodb::validate_client_configuration(config),
        ConnectorKind::Redis => nervix_connector_redis::validate_client_configuration(config),
        ConnectorKind::RabbitMq => nervix_connector_rabbitmq::validate_client_configuration(config),
        ConnectorKind::Nats => nervix_connector_nats::validate_client_configuration(config),
        ConnectorKind::ZeroMq => nervix_connector_zeromq::validate_client_configuration(config),
        ConnectorKind::Sentry => nervix_connector_sentry::validate_client_configuration(config),
        ConnectorKind::Sqs => nervix_connector_sqs::validate_client_configuration(config),
        _ => Ok(()),
    }
}

/// The fixed command outcome carries only reviewed, value-free contexts. The complete typed
/// report remains available internally; driver diagnostics can contain a URL or secret value.
pub(in crate::application) fn client_validation_message<C: error_stack::Context>(
    error: &Report<C>,
) -> Option<String> {
    if let Some(schema) = error.downcast_ref::<ClientConfigSchemaError>()
        && let Some(client) = error.downcast_ref::<crate::registry::RegistryError>()
    {
        return Some(format!("{client}: {schema}"));
    }
    let client = error.downcast_ref::<ModelBindingValidationError>()?;
    if !matches!(client, ModelBindingValidationError::Client { .. }) {
        return None;
    }
    let reason = if let Some(driver) = error.downcast_ref::<OfflineClientConfigError>() {
        driver.to_string()
    } else if let Some(schema) = error.downcast_ref::<ClientConfigSchemaError>() {
        schema.to_string()
    } else if let Some(config) = error.downcast_ref::<WebsocketsClientConfigError>() {
        config.to_string()
    } else if let Some(config) = error.downcast_ref::<PrometheusClientConfigError>() {
        config.to_string()
    } else if let Some(config) = error.downcast_ref::<MqttClientConfigError>() {
        config.to_string()
    } else if let Some(validation) = error.downcast_ref::<ClientValidationError>() {
        match validation {
            ClientValidationError::Resolve => {
                match error.downcast_ref::<crate::runtime::RuntimeResourceError>() {
                    Some(resource) => resource.to_string(),
                    None => validation.to_string(),
                }
            }
            _ => validation.to_string(),
        }
    } else {
        client.to_string()
    };
    Some(format!("{client}: {reason}"))
}

#[cfg(all(test, feature = "testing"))]
mod admission_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zeromq_driver_validation_refuses_an_endpoint_without_its_transport() {
        let config = [ClientConfigEntry {
            key: "addr".to_string(),
            value: "127.0.0.1:5555".to_string(),
        }];
        let refusal = validate_driver(ConnectorKind::ZeroMq, &config)
            .expect_err("a ZeroMQ endpoint requires its transport scheme");
        assert!(matches!(
            refusal.current_context(),
            OfflineClientConfigError::InvalidKey { key } if key == "addr"
        ));
    }

    #[test]
    fn rendered_api_root_refusal_retains_its_cause_and_hides_its_query() {
        let config = [
            ClientConfigEntry {
                key: "addr".to_string(),
                value: "http://prometheus.example.com/?query=private-api-value".to_string(),
            },
            ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "100".to_string(),
            },
        ];
        let report =
            SessionServiceImpl::validate_rendered_client_values(ConnectorKind::Prometheus, &config)
                .expect_err("the API root cannot contain query options")
                .change_context(ModelBindingValidationError::Client {
                    domain: DomainName::parse("sales").expect("valid domain name"),
                    node: nervix_models::ClientName::parse("metrics").expect("valid client name"),
                });
        assert_eq!(
            report.downcast_ref::<nervix_models::PrometheusApiRootError>(),
            Some(&nervix_models::PrometheusApiRootError::Query)
        );
        assert_eq!(
            client_validation_message(&report).as_deref(),
            Some(
                "invalid client 'metrics' in domain 'sales': invalid Prometheus client config key \
                 'addr'"
            )
        );
    }

    #[test]
    fn rendered_mqtt_refusal_retains_its_cause_and_names_its_key_without_a_value() {
        let entry = |key: &str, value: &str| ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        };
        let config = [
            entry("addr", "mqtt://broker.example.com:1883"),
            entry("keep_alive_ms", "1500"),
            entry("password", "private-mqtt-password"),
        ];
        let report =
            SessionServiceImpl::validate_rendered_client_values(ConnectorKind::Mqtt, &config)
                .expect_err("a keep-alive of a second and a half is not whole seconds")
                .change_context(ModelBindingValidationError::Client {
                    domain: DomainName::parse("sales").expect("valid domain name"),
                    node: nervix_models::ClientName::parse("sensors").expect("valid client name"),
                });
        assert_eq!(
            report.downcast_ref::<MqttClientConfigError>(),
            Some(&MqttClientConfigError::KeepAlive)
        );
        let message = client_validation_message(&report);
        assert_eq!(
            message.as_deref(),
            Some(
                "invalid client 'sensors' in domain 'sales': 'keep_alive_ms' must be whole seconds \
                 from 1 s through 65535 s"
            )
        );
        assert!(!format!("{report:?}").contains("private-mqtt-password"));
    }

    #[test]
    fn command_refusal_keeps_driver_values_private() {
        let report = Report::new(std::io::Error::other("credential-in-driver-diagnostic"))
            .change_context(OfflineClientConfigError::key("addr"))
            .change_context(ModelBindingValidationError::Client {
                domain: DomainName::parse("sales").expect("valid domain name"),
                node: nervix_models::ClientName::parse("database").expect("valid client name"),
            });
        assert_eq!(
            client_validation_message(&report).as_deref(),
            Some("invalid client 'database' in domain 'sales': invalid client config key 'addr'")
        );
        assert!(report.downcast_ref::<std::io::Error>().is_some());
    }
}
