//! Network-free inspection of Iceberg storage properties before a client is committed.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Refusals from the same storage-configuration readers that initialize file I/O.
//! - **Depends on.** Resolved configuration entries, the connector contract and Iceberg storage.
//! - **Must not know.** Models, resource resolution, transaction admission or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns network-free storage configuration inspection")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies resolved client entries")
)]

use ahash::HashMap;
use error_stack::Report;
use iceberg::io::{ADLS_CONNECTION_STRING, S3_SSE_TYPE};
use nervix_connector::OfflineClientConfigError;
use nervix_models::{ClientConfigEntry, IcebergStorageBackend};

use super::storage;

/// Interpret the properties that do not require a table location or an initialized operator.
/// This opens no transport, credential provider or OpenDAL operator.
pub fn validate_client_configuration(
    backend: IcebergStorageBackend,
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    let mut properties = HashMap::default();
    for entry in backend.file_io_entries(config) {
        properties.insert(entry.property.to_string(), entry.value.to_string());
    }
    validate_storage_properties(backend, &properties)
}

/// A catalog can supply storage properties for any supported backend to its table's file I/O:
/// the keys of each backend's delegated namespaces, as written.
pub fn validate_catalog_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    for backend in [
        IcebergStorageBackend::S3,
        IcebergStorageBackend::Gcs,
        IcebergStorageBackend::AzureBlob,
    ] {
        let mut properties = HashMap::default();
        for entry in config {
            if backend.delegates(&entry.key) {
                properties.insert(entry.key.clone(), entry.value.clone());
            }
        }
        validate_storage_properties(backend, &properties)?;
    }
    Ok(())
}

/// Runs the storage reader of `backend` over file I/O properties without building an operator.
fn validate_storage_properties(
    backend: IcebergStorageBackend,
    properties: &HashMap<String, String>,
) -> error_stack::Result<(), OfflineClientConfigError> {
    match backend {
        IcebergStorageBackend::S3 => {
            storage::s3_config(properties).map_err(|source| {
                Report::new(source).change_context(OfflineClientConfigError::key(S3_SSE_TYPE))
            })?;
        }
        IcebergStorageBackend::Gcs => {}
        IcebergStorageBackend::AzureBlob => {
            storage::azure_config(properties).map_err(|source| {
                Report::new(source)
                    .change_context(OfflineClientConfigError::key(ADLS_CONNECTION_STRING))
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "checks the production offline reader without transport"
    )
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** Offline refusal and typed-cause evidence.
    //! - **Depends on.** The connector's production configuration readers.
    //! - **Must not know.** Nodes, transactions or transport fixtures.

    use super::*;

    #[test]
    fn storage_and_catalog_checks_preserve_their_typed_driver_refusal() {
        for (backend, key, value) in [
            (
                IcebergStorageBackend::S3,
                S3_SSE_TYPE,
                "not-an-encryption-kind",
            ),
            (
                IcebergStorageBackend::AzureBlob,
                ADLS_CONNECTION_STRING,
                "opaque",
            ),
        ] {
            let entries = [ClientConfigEntry {
                key: key.to_string(),
                value: value.to_string(),
            }];
            let refusal = validate_client_configuration(backend, &entries)
                .expect_err("the production storage reader refuses this property");
            assert!(refusal.contains::<iceberg::Error>());
            assert!(matches!(
                refusal.current_context(),
                OfflineClientConfigError::InvalidKey { key: found } if found == key
            ));
            assert!(!refusal.to_string().contains(value));
            let refusal = validate_catalog_configuration(&entries)
                .expect_err("catalog file-I/O properties use the same reader");
            assert!(refusal.contains::<iceberg::Error>());
            assert_eq!(
                refusal.to_string(),
                format!("invalid client config key '{key}'")
            );
        }
    }

    #[test]
    fn an_offline_check_needs_no_bucket_or_credentials() {
        for backend in [
            IcebergStorageBackend::S3,
            IcebergStorageBackend::Gcs,
            IcebergStorageBackend::AzureBlob,
        ] {
            validate_client_configuration(backend, &[])
                .expect("the table and credential provider are initialization concerns");
        }
        validate_catalog_configuration(&[ClientConfigEntry {
            key: S3_SSE_TYPE.to_string(),
            value: "s3".to_string(),
        }])
        .expect("the storage reader accepts its declared encryption selection");
    }
}
