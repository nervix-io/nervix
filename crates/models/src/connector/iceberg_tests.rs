//! Iceberg declaration checks.
//!
//! Layer: test harness.
//! - **Owns.** Current object-store selection and location validation evidence.
//! - **Depends on.** The public Iceberg vocabulary and its declared capabilities.
//! - **Must not know.** Drivers, registry state or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "checks the Iceberg vocabulary through its production owners"
    )
)]

use strum::IntoEnumIterator as _;

use super::{
    IcebergCatalogProperties, IcebergLocationError, IcebergNamespace, IcebergPropertyCollision,
    IcebergStorageBackend, IcebergTableEmitter,
};
use crate::{ConnectorKind, EmitterDeclaration};

fn config_entry(key: &str, value: &str) -> crate::ClientConfigEntry {
    crate::ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn client_schema(kind: ConnectorKind) -> crate::ClientConfigSchema {
    kind.client()
        .expect("an Iceberg client is configured")
        .config
}

const STORAGE_BACKENDS: [IcebergStorageBackend; 3] = [
    IcebergStorageBackend::S3,
    IcebergStorageBackend::Gcs,
    IcebergStorageBackend::AzureBlob,
];

#[test]
fn iceberg_client_booleans_are_nervix_owned_and_checked_before_opening() {
    for (kind, key) in [
        (ConnectorKind::S3, "path_style_access"),
        (ConnectorKind::S3, "allow_anonymous"),
        (ConnectorKind::Gcs, "no_auth"),
    ] {
        let schema = client_schema(kind);
        assert_eq!(
            schema.key(key).expect("the key is declared").owner,
            crate::ClientConfigOwner::Nervix,
        );
        for value in ["true", "false", "TRUE", "FALSE"] {
            schema
                .validate(&[config_entry(key, value)])
                .expect("the declared boolean has a value");
        }
        let refusal = schema
            .validate(&[config_entry(key, "private-invalid-value")])
            .expect_err("a nonboolean cannot be committed");
        assert!(matches!(
            refusal.current_context(),
            crate::ClientConfigSchemaError::InvalidValue { key: found, .. } if found == key
        ));
        assert!(!refusal.to_string().contains("private-invalid-value"));
    }
}

#[test]
fn iceberg_client_namespaces_delegate_properties_and_refuse_unprefixed_typos() {
    for (kind, property, base) in [
        (ConnectorKind::S3, "s3.driver_extension", vec![]),
        (ConnectorKind::Gcs, "gcs.driver_extension", vec![]),
        (ConnectorKind::AzureBlob, "adls.driver_extension", vec![]),
        (
            ConnectorKind::IcebergRest,
            "header.x-fixture",
            vec![config_entry("uri", "http://catalog.example.com")],
        ),
    ] {
        let schema = client_schema(kind);
        let mut delegated = base.clone();
        delegated.push(config_entry(property, "driver-value"));
        schema
            .validate(&delegated)
            .expect("the driver owns this namespace");
        assert_eq!(
            schema
                .inspection_entries(&delegated)
                .iter()
                .find(|entry| entry.key == property)
                .expect("the delegated value is rendered")
                .value,
            crate::CLIENT_CONFIG_REDACTION,
        );
        let mut typo = base;
        typo.push(config_entry("nervix_option_typo", "private-value"));
        let refusal = schema
            .validate(&typo)
            .expect_err("unprefixed options are strict");
        assert!(matches!(
            refusal.current_context(),
            crate::ClientConfigSchemaError::UnknownKey { key } if key == "nervix_option_typo"
        ));
    }
}

#[test]
fn iceberg_rest_requires_its_http_locator_and_pairs_its_declared_identity_files() {
    let schema = client_schema(ConnectorKind::IcebergRest);
    let missing = schema
        .validate(&[])
        .expect_err("the catalog requires its locator");
    assert!(matches!(
        missing.current_context(),
        crate::ClientConfigSchemaError::MissingRequired { key: "uri" }
    ));
    let malformed = schema
        .validate(&[config_entry("uri", "file:///catalog")])
        .expect_err("a REST catalog uses HTTP or HTTPS");
    assert!(matches!(
        malformed.current_context(),
        crate::ClientConfigSchemaError::InvalidValue { key, .. } if key == "uri"
    ));
    let unpaired = schema
        .validate(&[
            config_entry("uri", "https://catalog.example.com"),
            config_entry("tls_cert_file", "/mount/certificate.pem"),
        ])
        .expect_err("a declared client identity has both halves");
    assert!(matches!(
        unpaired.current_context(),
        crate::ClientConfigSchemaError::RequiresKey {
            key: "tls_cert_file",
            required: "tls_key_file"
        }
    ));
}

#[test]
fn namespace_levels_preserve_the_catalog_identity() {
    let namespace = IcebergNamespace::try_from("Analytics.Events").expect("valid namespace");
    assert_eq!(
        namespace.levels().collect::<Vec<_>>(),
        ["Analytics", "Events"]
    );
    for text in [
        "",
        ".Events",
        "Analytics.",
        "Analytics..Events",
        "a/b",
        "a b",
    ] {
        assert!(IcebergNamespace::try_from(text).is_err(), "{text:?}");
    }
    assert!(IcebergNamespace::try_from("N".repeat(128)).is_ok());
    assert!(IcebergNamespace::try_from("N".repeat(129)).is_err());
}

#[test]
fn bolero_iceberg_namespaces_and_backend_choices_round_trip() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(128)
        .with_type::<[u8; 32]>()
        .for_each(|bytes| {
            let alphabet = b"aAzZ09_-~";
            let len = 1 + usize::from(bytes[0]) % 128;
            let text: String = (0..len)
                .map(|index| {
                    if index > 0 && index + 1 < len && index % 7 == 0 {
                        '.'
                    } else {
                        char::from(
                            alphabet[usize::from(bytes[index % bytes.len()]) % alphabet.len()],
                        )
                    }
                })
                .collect();
            let namespace =
                IcebergNamespace::try_from(text.clone()).expect("generated valid namespace");
            let json = serde_json::to_vec(&namespace).expect("serialize namespace");
            let decoded: IcebergNamespace =
                serde_json::from_slice(&json).expect("deserialize namespace");
            assert_eq!(decoded, namespace);
            let archived =
                rkyv::to_bytes::<rkyv::rancor::Error>(&namespace).expect("archive namespace");
            let decoded = rkyv::from_bytes::<IcebergNamespace, rkyv::rancor::Error>(&archived)
                .expect("read archived namespace");
            assert_eq!(decoded, namespace);
            assert_eq!(decoded.levels().collect::<Vec<_>>().join("."), text);
            for backend in [
                IcebergStorageBackend::S3,
                IcebergStorageBackend::Gcs,
                IcebergStorageBackend::AzureBlob,
            ] {
                assert_eq!(
                    IcebergStorageBackend::try_from(backend.connector()),
                    Ok(backend)
                );
            }
        });
}

#[test]
fn storage_backend_selection_agrees_with_the_declared_client_reference() {
    for kind in ConnectorKind::iter() {
        let backend = IcebergStorageBackend::try_from(kind);
        assert_eq!(
            backend.is_ok(),
            IcebergTableEmitter::CAPABILITIES
                .reference
                .admits_client(kind),
            "{kind}"
        );
        if let Ok(backend) = backend {
            assert_eq!(backend.connector(), kind);
        }
    }
}

#[test]
fn each_backend_accepts_its_object_location_schemes() {
    for (backend, location) in [
        (IcebergStorageBackend::S3, "s3://warehouse/Events"),
        (IcebergStorageBackend::Gcs, "gs://warehouse/Events"),
        (
            IcebergStorageBackend::AzureBlob,
            "wasb://warehouse@account.blob.core.windows.net/Events",
        ),
        (
            IcebergStorageBackend::AzureBlob,
            "wasbs://warehouse@account.blob.core.windows.net/Events",
        ),
        (
            IcebergStorageBackend::AzureBlob,
            "abfs://warehouse@account.dfs.core.windows.net/Events",
        ),
        (
            IcebergStorageBackend::AzureBlob,
            "abfss://warehouse@account.dfs.core.windows.net/Events",
        ),
    ] {
        assert!(backend.validate_location(location).is_ok(), "{location}");
    }
}

#[test]
fn a_location_refusal_names_its_scheme_and_storage_kind() {
    for (backend, location, scheme) in [
        (IcebergStorageBackend::S3, "gs://warehouse/events", "gs"),
        (IcebergStorageBackend::Gcs, "s3://warehouse/events", "s3"),
        (
            IcebergStorageBackend::AzureBlob,
            "s3://warehouse/events",
            "s3",
        ),
    ] {
        let Err(error) = backend.validate_location(location) else {
            panic!("{backend:?} must refuse {location}");
        };
        assert_eq!(
            error.current_context(),
            &IcebergLocationError::Scheme {
                backend,
                found: scheme.to_string(),
            }
        );
    }
}

#[test]
fn a_location_refusal_keeps_its_url_cause_and_requires_an_authority() {
    let Err(error) = IcebergStorageBackend::S3.validate_location("relative/objects") else {
        panic!("a relative path is not an absolute object-store URL");
    };
    assert_eq!(error.current_context(), &IcebergLocationError::Malformed);
    assert!(error.contains::<url::ParseError>());

    let Err(error) = IcebergStorageBackend::Gcs.validate_location("gs:///objects") else {
        panic!("a GCS object location must name its bucket");
    };
    assert_eq!(
        error.current_context(),
        &IcebergLocationError::MissingAuthority
    );
}

#[test]
fn every_storage_client_key_supplies_a_file_io_property_or_configures_its_transport() {
    let transport = ["timeout_ms", "tls_ca_file", "tls_cert_file", "tls_key_file"];
    for backend in STORAGE_BACKENDS {
        let schema = client_schema(backend.connector());
        for key in schema.keys() {
            let property = backend.file_io_property(key.name);
            if transport.contains(&key.name) {
                assert_eq!(property, None, "{} supplies no file I/O property", key.name);
                continue;
            }
            let property = property.unwrap_or_else(|| {
                panic!("{backend:?} key {} supplies a file I/O property", key.name)
            });
            assert!(
                backend.delegates(property),
                "{} supplies {property}, a property of the backend's delegated namespace",
                key.name
            );
        }
    }
}

#[test]
fn every_storage_client_declares_its_tls_files() {
    for backend in STORAGE_BACKENDS {
        let schema = client_schema(backend.connector());
        for (key, file) in [
            ("tls_ca_file", crate::ClientConfigFile::CertificateAuthority),
            ("tls_cert_file", crate::ClientConfigFile::Certificate),
            ("tls_key_file", crate::ClientConfigFile::PrivateKey),
        ] {
            let declared = schema
                .key(key)
                .expect("the storage client declares its TLS files");
            assert_eq!(declared.value, crate::ClientConfigValue::File(file));
            assert_eq!(declared.presence, crate::ClientConfigPresence::Optional);
            assert_eq!(declared.owner, crate::ClientConfigOwner::Nervix);
        }
        let refusal = schema
            .validate(&[config_entry("tls_cert_file", "client.pem")])
            .expect_err("a client certificate needs its private key");
        assert!(matches!(
            refusal.current_context(),
            crate::ClientConfigSchemaError::RequiresKey {
                key: "tls_cert_file",
                required: "tls_key_file",
            }
        ));
    }
}

#[test]
fn a_storage_client_supplies_owned_keys_under_their_properties_and_delegated_keys_as_written() {
    let storage = [
        config_entry("endpoint", "http://objects.example.com"),
        config_entry("timeout_ms", "1000"),
        config_entry("tls_ca_file", "ca.pem"),
        config_entry("s3.driver_extension", "Case-Preserved"),
    ];
    let supplied: Vec<_> = IcebergStorageBackend::S3
        .file_io_entries(&storage)
        .map(|entry| (entry.property, entry.key, entry.value))
        .collect();
    assert_eq!(
        supplied,
        [
            ("s3.endpoint", "endpoint", "http://objects.example.com"),
            (
                "s3.driver_extension",
                "s3.driver_extension",
                "Case-Preserved"
            ),
        ]
    );
}

#[test]
fn a_catalog_loads_with_both_clients_properties_without_their_tls_files() {
    let storage = [
        config_entry("endpoint", "http://first.example.com"),
        config_entry("s3.endpoint", "http://last.example.com"),
        config_entry("region", "storage-region"),
        config_entry("tls_ca_file", "storage-ca.pem"),
        config_entry("timeout_ms", "1000"),
    ];
    let catalog = [
        config_entry("uri", "http://catalog.example.com"),
        config_entry("tls_ca_file", "catalog-ca.pem"),
        config_entry("gcs.no-auth", "true"),
        config_entry("token", "catalog-token"),
    ];
    let properties = IcebergCatalogProperties::pair(IcebergStorageBackend::S3, &storage, &catalog)
        .expect("the two clients set disjoint properties");
    let loaded: Vec<_> = properties.iter().collect();
    assert_eq!(
        loaded,
        [
            ("gcs.no-auth", "true"),
            ("s3.endpoint", "http://last.example.com"),
            ("s3.region", "storage-region"),
            ("token", "catalog-token"),
            ("uri", "http://catalog.example.com"),
        ]
    );
}

#[test]
fn a_catalog_property_its_storage_client_supplies_is_refused_naming_both_keys() {
    for (backend, storage, catalog, property) in [
        (
            IcebergStorageBackend::S3,
            config_entry("endpoint", "http://objects.example.com"),
            config_entry("s3.endpoint", "http://other.example.com"),
            "s3.endpoint",
        ),
        (
            IcebergStorageBackend::S3,
            config_entry("client_region", "storage-region"),
            config_entry("client.region", "catalog-region"),
            "client.region",
        ),
        (
            IcebergStorageBackend::S3,
            config_entry("s3.driver_extension", "private-storage-value"),
            config_entry("s3.driver_extension", "private-catalog-value"),
            "s3.driver_extension",
        ),
        (
            IcebergStorageBackend::Gcs,
            config_entry("token", "storage-token"),
            config_entry("gcs.oauth2.token", "catalog-token"),
            "gcs.oauth2.token",
        ),
        (
            IcebergStorageBackend::AzureBlob,
            config_entry("account_name", "storage-account"),
            config_entry("adls.account-name", "catalog-account"),
            "adls.account-name",
        ),
    ] {
        let storage = [storage];
        let catalog = [config_entry("uri", "http://catalog.example.com"), catalog];
        let refusal = IcebergCatalogProperties::pair(backend, &storage, &catalog)
            .expect_err("both clients set the same property");
        assert_eq!(
            refusal.current_context(),
            &IcebergPropertyCollision {
                property: property.to_string(),
                storage_key: storage[0].key.clone(),
            }
        );
        let rendered = refusal.to_string();
        assert!(!rendered.contains(&storage[0].value), "{rendered}");
        assert!(!rendered.contains(&catalog[1].value), "{rendered}");
    }
}
