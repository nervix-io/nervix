//! Unit evidence for the Iceberg table engine.
//!
//! Layer: test harness.
//! - **Owns.** Table configuration, projection, staging and commit-owner tests.
//! - **Depends on.** Parent table helpers, connector host fixtures, selected primitives and local transports.
//! - **Must not know.** Server graph execution, registry state or another connector implementation.

use ::iceberg::{
    TableIdent,
    arrow::arrow_schema_to_schema_auto_assign_ids,
    io::{
        ADLS_ACCOUNT_KEY, ADLS_ACCOUNT_NAME, ADLS_AUTHORITY_HOST, ADLS_CLIENT_ID,
        ADLS_CLIENT_SECRET, ADLS_SAS_TOKEN, ADLS_TENANT_ID, CLIENT_REGION, FileIO,
        GCS_ALLOW_ANONYMOUS, GCS_CREDENTIALS_JSON, GCS_DISABLE_CONFIG_LOAD,
        GCS_DISABLE_VM_METADATA, GCS_NO_AUTH, GCS_SERVICE_HOST, GCS_TOKEN, S3_ACCESS_KEY_ID,
        S3_ALLOW_ANONYMOUS, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_ENDPOINT,
        S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
    },
    spec::{
        DataContentType, DataFileBuilder, FormatVersion, Operation, PartitionSpec, Schema,
        SortOrder, Struct, Summary, TableMetadataBuilder,
    },
};
use arrow_array::builder::{
    FixedSizeListBuilder, LargeListBuilder, ListBuilder, MapBuilder, StringBuilder,
    TimestampNanosecondBuilder,
};
use arrow_schema::Field;
use nervix_dns::{DnsConfiguration, NameServers};
use nervix_primitives::sync::atomic::{AtomicUsize, Ordering};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;

#[test]
fn the_vocabulary_names_the_storage_properties_the_iceberg_crate_reads() {
    for (backend, key, property) in [
        (IcebergStorageBackend::S3, "access_key_id", S3_ACCESS_KEY_ID),
        (
            IcebergStorageBackend::S3,
            "allow_anonymous",
            S3_ALLOW_ANONYMOUS,
        ),
        (IcebergStorageBackend::S3, "client_region", CLIENT_REGION),
        (
            IcebergStorageBackend::S3,
            "disable_config_load",
            S3_DISABLE_CONFIG_LOAD,
        ),
        (
            IcebergStorageBackend::S3,
            "disable_ec2_metadata",
            S3_DISABLE_EC2_METADATA,
        ),
        (IcebergStorageBackend::S3, "endpoint", S3_ENDPOINT),
        (
            IcebergStorageBackend::S3,
            "path_style_access",
            S3_PATH_STYLE_ACCESS,
        ),
        (IcebergStorageBackend::S3, "region", S3_REGION),
        (
            IcebergStorageBackend::S3,
            "secret_access_key",
            S3_SECRET_ACCESS_KEY,
        ),
        (IcebergStorageBackend::S3, "session_token", S3_SESSION_TOKEN),
        (
            IcebergStorageBackend::Gcs,
            "allow_anonymous",
            GCS_ALLOW_ANONYMOUS,
        ),
        (
            IcebergStorageBackend::Gcs,
            "credentials_json",
            GCS_CREDENTIALS_JSON,
        ),
        (
            IcebergStorageBackend::Gcs,
            "disable_config_load",
            GCS_DISABLE_CONFIG_LOAD,
        ),
        (
            IcebergStorageBackend::Gcs,
            "disable_vm_metadata",
            GCS_DISABLE_VM_METADATA,
        ),
        (IcebergStorageBackend::Gcs, "no_auth", GCS_NO_AUTH),
        (IcebergStorageBackend::Gcs, "service_path", GCS_SERVICE_HOST),
        (IcebergStorageBackend::Gcs, "token", GCS_TOKEN),
        (
            IcebergStorageBackend::AzureBlob,
            "account_key",
            ADLS_ACCOUNT_KEY,
        ),
        (
            IcebergStorageBackend::AzureBlob,
            "account_name",
            ADLS_ACCOUNT_NAME,
        ),
        (
            IcebergStorageBackend::AzureBlob,
            "authority_host",
            ADLS_AUTHORITY_HOST,
        ),
        (
            IcebergStorageBackend::AzureBlob,
            "client_id",
            ADLS_CLIENT_ID,
        ),
        (
            IcebergStorageBackend::AzureBlob,
            "client_secret",
            ADLS_CLIENT_SECRET,
        ),
        (
            IcebergStorageBackend::AzureBlob,
            "sas_token",
            ADLS_SAS_TOKEN,
        ),
        (
            IcebergStorageBackend::AzureBlob,
            "tenant_id",
            ADLS_TENANT_ID,
        ),
    ] {
        assert_eq!(backend.file_io_property(key), Some(property), "{key}");
    }
}

#[test]
fn a_client_tls_file_that_cannot_be_read_stops_the_emitter_naming_its_client() {
    let missing = tempfile::tempdir()
        .assured("the fixture can create a directory")
        .path()
        .join("missing-ca.pem");
    let config = [ClientConfigEntry {
        key: "tls_ca_file".to_string(),
        value: missing.display().to_string(),
    }];
    for client in [
        IcebergClientTlsError::Catalog,
        IcebergClientTlsError::ObjectStore,
    ] {
        let refusal = client
            .client_config(&config)
            .expect_err("an unreadable CA file cannot configure a client");
        assert!(matches!(
            refusal.current_context(),
            EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
        ));
        assert_eq!(
            refusal.downcast_ref::<IcebergClientTlsError>(),
            Some(&client)
        );
        assert!(matches!(
            refusal.downcast_ref::<nervix_connector::TlsClientConfigError>(),
            Some(nervix_connector::TlsClientConfigError::ReadCaCertificate { path })
                if *path == missing
        ));
    }
    IcebergClientTlsError::Catalog
        .client_config(&[])
        .assured("a client without TLS files trusts the bundled and platform roots");
}

#[test]
fn azure_locations_use_the_storage_service_named_by_their_scheme() {
    for (scheme, service) in [
        ("wasb", "blob"),
        ("wasbs", "blob"),
        ("abfs", "dfs"),
        ("abfss", "dfs"),
    ] {
        let location = format!("{scheme}://container@account.{service}.core.windows.net/events");
        let url = IcebergTableEmitter::validate_blob_location(
            IcebergStorageBackend::AzureBlob,
            "table",
            &location,
        )
        .assured("the scheme and host identify the same Azure storage service");
        assert_eq!(url.as_str(), location);
    }
}

#[nervix_primitives::test]
async fn iceberg_catalog_authentication_uses_the_node_dns_client() {
    const CATALOG: &str = "catalog.nervix.test";
    const AUTH: &str = "auth.nervix.test";
    let authority = DnsAuthority::start_on_loopback()
        .await
        .assured("the fixture can bind a loopback DNS port");
    for name in [CATALOG, AUTH] {
        authority.set(
            name,
            DnsAnswer::Addresses {
                addresses: vec![std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
                ttl: Duration::from_secs(1),
            },
        );
    }
    let files = tempfile::tempdir().assured("the fixture can create a directory");
    let resolver_configuration = files.path().join("resolv.conf");
    let hosts_file = files.path().join("hosts");
    std::fs::write(
        &resolver_configuration,
        "search nervix.test\noptions ndots:1 timeout:1 attempts:1\n",
    )
    .assured("the fixture resolver configuration can be written");
    std::fs::write(&hosts_file, "").assured("the fixture hosts file can be written");
    let dns = DnsResolver::load(DnsConfiguration {
        resolver_configuration,
        hosts_file,
        name_servers: NameServers::Explicit(vec![authority.address()]),
    })
    .await
    .assured("the fixture resolver configuration is valid");

    let auth_listener =
        nervix_primitives::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("the OAuth endpoint can bind");
    let auth_port = auth_listener
        .local_addr()
        .assured("the OAuth endpoint has an address")
        .port();
    let auth_server = nervix_primitives::task::spawn(async move {
        let (mut stream, _) = auth_listener
            .accept()
            .await
            .assured("the OAuth request connects");
        let mut bytes = [0_u8; 4096];
        let length = stream
            .read(&mut bytes)
            .await
            .assured("the OAuth request is readable");
        let request = String::from_utf8_lossy(&bytes[..length]).to_ascii_lowercase();
        assert!(request.starts_with("post /token http/1.1"), "{request}");
        assert!(
            request.contains(&format!("host: {AUTH}:{auth_port}")),
            "{request}"
        );
        let body = r#"{"access_token":"fixture-token","token_type":"Bearer"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .await
            .assured("the OAuth response can be written");
    });

    let catalog_listener =
        nervix_primitives::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("the catalog endpoint can bind");
    let catalog_port = catalog_listener
        .local_addr()
        .assured("the catalog endpoint has an address")
        .port();
    /// One request the catalog fixture answers: the request line it expects and the response
    /// it writes.
    struct CatalogExchange {
        request_line: &'static str,
        status: &'static str,
        body: &'static str,
    }
    // The catalog loads its configuration before the first operation, and the table load
    // carries the token the configuration request already used.
    let exchanges = [
        CatalogExchange {
            request_line: "get /v1/config http/1.1",
            status: "200 OK",
            body: r#"{"overrides":{},"defaults":{}}"#,
        },
        CatalogExchange {
            request_line: "get /v1/namespaces/events/tables/orders http/1.1",
            status: "404 Not Found",
            body: "",
        },
    ];
    let catalog_server = nervix_primitives::task::spawn(async move {
        for exchange in exchanges {
            nervix_primitives::task::consume_budget().await;
            let (mut stream, _) = catalog_listener
                .accept()
                .await
                .assured("the catalog request connects");
            let mut bytes = [0_u8; 4096];
            let length = stream
                .read(&mut bytes)
                .await
                .assured("the catalog request is readable");
            let request = String::from_utf8_lossy(&bytes[..length]).to_ascii_lowercase();
            assert!(request.starts_with(exchange.request_line), "{request}");
            assert!(
                request.contains(&format!("host: {CATALOG}:{catalog_port}")),
                "{request}"
            );
            assert!(
                request.contains("authorization: bearer fixture-token"),
                "{request}"
            );
            let response = format!(
                "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                exchange.status,
                exchange.body.len(),
                exchange.body
            );
            stream
                .write_all(response.as_bytes())
                .await
                .assured("the catalog response can be written");
        }
    });

    let catalog_config = [
        ClientConfigEntry {
            key: "uri".to_string(),
            value: format!("http://{CATALOG}:{catalog_port}"),
        },
        ClientConfigEntry {
            key: "credential".to_string(),
            value: "client:secret".to_string(),
        },
        ClientConfigEntry {
            key: "oauth2-server-uri".to_string(),
            value: format!("http://{AUTH}:{auth_port}/token"),
        },
    ];
    let tls = IcebergClientTlsError::Catalog
        .client_config(&catalog_config)
        .assured("a catalog client without TLS files trusts the default roots");
    let storage_root = Url::parse("s3://bucket/table").assured("the fixture storage root is a URL");
    let storage =
        DnsStorageFactory::new(IcebergStorageBackend::S3, &dns, &storage_root, None, &tls)
            .assured("the object-store client can be built without a request");
    let catalog = IcebergCatalogConnection {
        name: "fixture",
        properties: IcebergCatalogProperties::pair(IcebergStorageBackend::S3, &[], &catalog_config)
            .assured("a catalog paired with an empty storage client sets no shared property"),
        tls: &tls,
        storage,
    }
    .open(&dns)
    .await
    .assured("the REST catalog can be configured");
    let table = TableIdent::from_strs(["events", "orders"]).assured("the fixture names a table");
    let error =
        nervix_primitives::time::timeout(Duration::from_secs(10), catalog.load_table(&table))
            .await
            .assured("the catalog answers the table load")
            .expect_err("the fixture catalog holds no table");
    assert_eq!(error.kind(), IcebergErrorKind::TableNotFound, "{error}");
    assert!(authority.questions_for(CATALOG) > 0);
    assert!(authority.questions_for(AUTH) > 0);
    auth_server.await.assured("the OAuth server task completes");
    catalog_server
        .await
        .assured("the catalog server task completes");
}

fn commit_policy(interval_millis: u64, max_size: u64) -> IcebergCommitPolicy {
    IcebergCommitPolicy {
        interval: Duration::from_millis(interval_millis),
        max_size,
    }
}

/// What one acknowledgement handover was resolved as, counted by the test that shares it.
#[derive(Default)]
struct RecordedAcknowledgements {
    acknowledged: AtomicUsize,
    kept_alive: AtomicUsize,
    rejected: AtomicUsize,
}

/// One acknowledgement handover the emitter holds, recording into the shared counters.
struct RecordedHandover(StdArc<RecordedAcknowledgements>);

impl EmitterRecordAcknowledgementServices for RecordedHandover {
    fn acknowledge(&self) {
        self.0.acknowledged.fetch_add(1, Ordering::Release);
    }

    fn keep_alive(&self) {
        self.0.kept_alive.fetch_add(1, Ordering::Release);
    }

    fn reject(&self, _reason: String) {
        self.0.rejected.fetch_add(1, Ordering::Release);
    }

    fn is_empty(&self) -> bool {
        false
    }
}

#[nervix_primitives::test]
async fn iceberg_catalog_transaction_disables_library_internal_retries() {
    let metadata = TableMetadataBuilder::new(
        Schema::builder().build().expect("valid empty schema"),
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "memory://warehouse/table".to_string(),
        FormatVersion::V2,
        [(
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
            "7".to_string(),
        )]
        .into_iter()
        .collect(),
    )
    .expect("valid table metadata builder")
    .build()
    .expect("valid table metadata")
    .metadata;
    let table = Table::builder()
        .metadata(metadata)
        .metadata_location("memory://warehouse/table/metadata/v1.json")
        .identifier(TableIdent::from_strs(["test", "table"]).expect("valid table ident"))
        .file_io(FileIO::new_with_memory())
        .runtime(::iceberg::Runtime::try_current().expect("test runs in Tokio runtime"))
        .build()
        .expect("valid in-memory table");

    let single_attempt = IcebergTableEmitterClient::single_attempt_table(&table)
        .expect("single-attempt table must build");

    assert_eq!(
        single_attempt
            .metadata()
            .table_properties()
            .commit_num_retries()
            .expect("valid transient commit retries"),
        0
    );
    assert_eq!(
        table
            .metadata()
            .table_properties()
            .commit_num_retries()
            .expect("valid source commit retries"),
        7,
        "the catalog table metadata must not be mutated"
    );
}

#[nervix_primitives::test]
async fn installing_catalog_metadata_retains_the_initialized_storage() {
    const PATH: &str = "memory://warehouse/table/data.parquet";
    #[expect(
        clippy::disallowed_types,
        reason = "Iceberg TableMetadataBuilder requires a standard-library property map at its \
                  external API boundary"
    )]
    let metadata = TableMetadataBuilder::new(
        Schema::builder()
            .build()
            .assured("an empty table schema is valid"),
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "memory://warehouse/table".to_string(),
        FormatVersion::V2,
        std::collections::HashMap::default(),
    )
    .assured("the table metadata has every required field")
    .build()
    .assured("the table metadata is valid")
    .metadata;
    let table = Table::builder()
        .metadata(metadata)
        .identifier(
            TableIdent::from_strs(["test", "table"]).assured("the table has a namespace and name"),
        )
        .metadata_location("memory://warehouse/table/metadata/initial.json")
        .file_io(FileIO::new_with_memory())
        .runtime(::iceberg::Runtime::try_current().assured("the async test runs inside Tokio"))
        .build()
        .assured("the in-memory table has every required field");
    table
        .file_io()
        .new_output(PATH)
        .assured("the object URL uses memory storage")
        .write(bytes::Bytes::from_static(b"retained object"))
        .await
        .assured("the fixture can write its object");
    let refreshed = Table::builder()
        .metadata(table.metadata_ref())
        .identifier(table.identifier().clone())
        .metadata_location("memory://warehouse/table/metadata/refreshed.json")
        .readonly(true)
        .file_io(FileIO::new_with_memory())
        .runtime(::iceberg::Runtime::try_current().assured("the async test runs inside Tokio"))
        .build()
        .assured("the refreshed table has every required field");
    #[expect(
        clippy::disallowed_types,
        reason = "Iceberg CatalogBuilder requires a standard-library property map at its external \
                  API boundary"
    )]
    let catalog = ::iceberg::memory::MemoryCatalogBuilder::default()
        .load(
            "fixture",
            std::collections::HashMap::from_iter([(
                "warehouse".to_string(),
                "memory://warehouse".to_string(),
            )]),
        )
        .await
        .assured("the fixture catalog has a warehouse");
    let mut client = IcebergTableEmitterClient {
        catalog: StdArc::new(catalog),
        table,
        file_name_prefix: "fixture".to_string(),
        data_file_sequence: 0,
    };
    client
        .install_table(refreshed)
        .assured("catalog metadata can be installed");
    let content = client
        .table
        .file_io()
        .new_input(PATH)
        .assured("the retained object URL uses memory storage")
        .read()
        .await
        .assured("the installed table retains its initialized object storage");
    assert_eq!(content.as_ref(), b"retained object");
    assert_eq!(
        client.table.metadata_location(),
        Some("memory://warehouse/table/metadata/refreshed.json")
    );
    assert!(client.table.readonly());
}

#[test]
fn a_preparation_keeps_its_data_files_and_append_identity_until_every_covered_file_is_written() {
    let append_id = uuid::Uuid::now_v7();
    let data_file = |path: &str| {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(128)
            .record_count(2)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .expect("valid prepared Iceberg data file")
    };
    let mut preparation = IcebergCommitPreparation::with_append_id(3, append_id);
    assert_eq!(preparation.unwritten(), 0..3);
    assert!(!preparation.is_written());

    preparation.record_written(2, vec![data_file("s3://bucket/table/data/first.parquet")]);
    assert_eq!(preparation.unwritten(), 2..3);
    assert!(!preparation.is_written());
    preparation.record_written(1, vec![data_file("s3://bucket/table/data/second.parquet")]);

    assert!(preparation.is_written());
    assert_eq!(preparation.append_id(), append_id);
    let paths = preparation
        .data_files()
        .iter()
        .map(DataFile::file_path)
        .collect::<Vec<_>>();
    assert_eq!(
        paths,
        [
            "s3://bucket/table/data/first.parquet",
            "s3://bucket/table/data/second.parquet"
        ]
    );
}

#[test]
fn what_commits_published_before_a_failed_attempt_is_reported_with_the_next_success() {
    let mut state = IcebergCommitState::default();
    state.record_published(EmitterCommitReport {
        messages: 2,
        bytes: 100,
        domain_timestamp: Timestamp::from_unix_nanos(7),
    });
    state.record_published(EmitterCommitReport {
        messages: 1,
        bytes: 40,
        domain_timestamp: Timestamp::from_unix_nanos(5),
    });

    assert_eq!(
        state.unreported,
        Some(EmitterCommitReport {
            messages: 3,
            bytes: 140,
            domain_timestamp: Timestamp::from_unix_nanos(7),
        })
    );
}

#[test]
fn load_groups_take_the_leading_staged_batches_the_class_holds_together() {
    let mut start = 0_u64;
    let mut batches = Vec::new();
    for bytes in [400_u64, 500, 300, 900] {
        let end = start + bytes;
        batches.push(IcebergStagedBatch {
            file: 1,
            range: start..end,
            rows: 1,
            acknowledgements: None,
            domain_timestamp: Timestamp::from_unix_nanos(1),
        });
        start = end;
    }

    assert_eq!(
        IcebergLoadGroup::leading(&batches, 2_100),
        IcebergLoadGroup {
            batches: 4,
            charge: IcebergLoadCharge::Staged { bytes: 2_100 },
        }
    );
    assert_eq!(
        IcebergLoadGroup::leading(&batches, 1_000),
        IcebergLoadGroup {
            batches: 2,
            charge: IcebergLoadCharge::Staged { bytes: 900 },
        }
    );
    assert_eq!(
        IcebergLoadGroup::leading(&batches[2..], 1_000),
        IcebergLoadGroup {
            batches: 1,
            charge: IcebergLoadCharge::Staged { bytes: 300 },
        }
    );
    // A first batch larger than the whole class is a group of its own, charged the whole class.
    let oversized = IcebergLoadGroup::leading(&batches[3..], 800);
    assert_eq!(
        oversized,
        IcebergLoadGroup {
            batches: 1,
            charge: IcebergLoadCharge::WholeClass {
                batch_bytes: 900,
                capacity: 800,
            },
        }
    );
    assert_eq!(oversized.charge.reserved_bytes(), 800);
    // A batch larger than the whole class after a smaller one is left for a group of its own.
    assert_eq!(
        IcebergLoadGroup::leading(&batches[2..], 800),
        IcebergLoadGroup {
            batches: 1,
            charge: IcebergLoadCharge::Staged { bytes: 300 },
        }
    );
}

#[test]
fn iceberg_append_marker_identifies_an_ambiguously_successful_commit() {
    let append_id = uuid::Uuid::now_v7();
    let prepared = IcebergCommitPreparation::with_append_id(0, append_id);
    let snapshot = Snapshot::builder()
        .with_snapshot_id(1)
        .with_sequence_number(1)
        .with_timestamp_ms(1)
        .with_manifest_list("s3://bucket/table/metadata/manifest-list.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: [(
                ICEBERG_APPEND_ID_PROPERTY.to_string(),
                append_id.to_string(),
            )]
            .into_iter()
            .collect(),
        })
        .build();

    assert!(prepared.matches_snapshot(&snapshot));
    assert!(
        !IcebergCommitPreparation::with_append_id(0, uuid::Uuid::now_v7())
            .matches_snapshot(&snapshot)
    );
}

#[test]
fn iceberg_staged_schema_uses_microsecond_utc_timestamps() {
    let mapped = arrow_schema::Schema::new(vec![Field::new(
        "observed_at",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        true,
    )]);

    let staged = IcebergTableEmitter::staged_arrow_schema(&mapped)
        .expect("a mapped datetime column must narrow to microseconds");

    assert_eq!(
        staged.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
    );
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&staged)
        .expect("microsecond timestamp schema must convert to Iceberg");
    let serialized = serde_json::to_string(&iceberg_schema).expect("Iceberg schema must serialize");
    assert!(serialized.contains("timestamptz"));
    assert!(!serialized.contains("timestamptz_ns"));
}

#[test]
fn iceberg_staged_schema_rejects_a_column_mapped_twice() {
    let mapped = arrow_schema::Schema::new(vec![
        Field::new("user_id", DataType::Int64, true),
        Field::new("user_id", DataType::Int64, true),
    ]);

    let error = IcebergTableEmitter::staged_arrow_schema(&mapped)
        .expect_err("a column written twice has no single staged value");

    assert_eq!(
        *error.current_context(),
        EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
    );
}

#[test]
fn iceberg_open_checks_mapped_column_names_and_types() {
    let table = arrow_schema_to_schema_auto_assign_ids(&arrow_schema::Schema::new(vec![
        Field::new(
            "observed",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        ),
        Field::new("user_id", DataType::Int64, true),
    ]))
    .assured("table schema converts to Iceberg");
    let compatible = arrow_schema::Schema::new(vec![
        Field::new("user_id", DataType::Int64, true),
        Field::new(
            "observed",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        ),
    ]);
    IcebergTableEmitter::validate_table_schema(&compatible, &table)
        .assured("column order and field IDs do not change the schema contract");

    let wrong_type = arrow_schema::Schema::new(vec![Field::new("user_id", DataType::Utf8, true)]);
    let error = IcebergTableEmitter::validate_table_schema(&wrong_type, &table)
        .expect_err("a mapped type the table cannot write fails at open");
    assert_eq!(
        *error.current_context(),
        EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
    );
    assert!(
        format!("{error:?}").contains(
            "mapped column 'user_id' has Iceberg type string, but the table requires long"
        )
    );

    let missing = arrow_schema::Schema::new(vec![Field::new("unknown", DataType::Int64, true)]);
    let error = IcebergTableEmitter::validate_table_schema(&missing, &table)
        .expect_err("a column absent from the table fails at open");
    assert_eq!(
        *error.current_context(),
        EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
    );
}

#[test]
fn iceberg_open_checks_nested_column_names_and_types() {
    let datetime = DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()));
    let list = |element| DataType::List(StdArc::new(Field::new("item", element, true)));
    let structure = |fields: Vec<Field>| DataType::Struct(fields.into());
    let map = |key, value| {
        let entries = structure(vec![
            Field::new("key", key, false),
            Field::new("value", value, true),
        ]);
        DataType::Map(StdArc::new(Field::new("entries", entries, false)), false)
    };
    let observed = Field::new("observed", list(datetime.clone()), true);
    let dimension = Field::new(
        "dimension",
        structure(vec![
            Field::new("tag", DataType::Utf8, true),
            Field::new("score", DataType::Int64, true),
        ]),
        true,
    );
    let labels = Field::new("labels", map(DataType::Utf8, datetime.clone()), true);
    let table = arrow_schema_to_schema_auto_assign_ids(&arrow_schema::Schema::new(vec![
        observed.clone(),
        dimension,
        labels.clone(),
    ]))
    .assured("the fixture uses Iceberg-supported list, struct, map and UTC timestamp types");
    let reordered = arrow_schema::Schema::new(vec![
        labels,
        Field::new(
            "dimension",
            structure(vec![
                Field::new("score", DataType::Int64, true),
                Field::new("tag", DataType::Utf8, true),
            ]),
            true,
        ),
        observed,
    ]);
    IcebergTableEmitter::validate_table_schema(&reordered, &table)
        .assured("matching nested names and types are independent of generated field IDs");

    let mismatches = [
        Field::new("observed", list(DataType::Int64), true),
        Field::new("observed", DataType::Utf8, true),
        Field::new(
            "dimension",
            structure(vec![Field::new("tag", DataType::Utf8, true)]),
            true,
        ),
        Field::new(
            "dimension",
            structure(vec![
                Field::new("category", DataType::Utf8, true),
                Field::new("score", DataType::Int64, true),
            ]),
            true,
        ),
        Field::new(
            "dimension",
            structure(vec![
                Field::new("tag", DataType::Utf8, true),
                Field::new("score", DataType::Utf8, true),
            ]),
            true,
        ),
        Field::new("labels", map(DataType::Int64, datetime), true),
        Field::new("labels", map(DataType::Utf8, DataType::Int64), true),
    ];
    for field in mismatches {
        let name = field.name().clone();
        let mapped = arrow_schema::Schema::new(vec![field]);
        let error = IcebergTableEmitter::validate_table_schema(&mapped, &table)
            .expect_err("an incompatible nested column must fail before staging");
        assert_eq!(
            *error.current_context(),
            EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
        );
        assert!(format!("{error:?}").contains(&format!("mapped column '{name}'")));
    }
}

#[test]
fn iceberg_open_retains_the_schema_conversion_cause() {
    let table =
        arrow_schema_to_schema_auto_assign_ids(&arrow_schema::Schema::new(vec![Field::new(
            "user_id",
            DataType::Int64,
            true,
        )]))
        .assured("table schema converts to Iceberg");
    let unsupported = arrow_schema::Schema::new(vec![Field::new("user_id", DataType::Null, true)]);

    let error = IcebergTableEmitter::validate_table_schema(&unsupported, &table)
        .expect_err("an unsupported mapped type fails at open");

    assert_eq!(
        *error.current_context(),
        EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
    );
    assert!(error.contains::<::iceberg::Error>());
}

#[test]
fn iceberg_datetime_columns_are_staged_as_microseconds() {
    let values: ArrayRef = StdArc::new(
        TimestampNanosecondArray::from(vec![Some(1_234_567), None, Some(-1)]).with_timezone_utc(),
    );

    let staged = IcebergTableEmitter::staged_column(
        &values,
        &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
    )
    .expect("a nanosecond datetime column must stage as microseconds");

    assert_eq!(
        staged.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
    );
    let staged = staged
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("Iceberg datetime column must be a microsecond timestamp array");
    assert_eq!(staged.value(0), 1_234);
    assert!(staged.is_null(1));
    assert_eq!(staged.value(2), -1);
}

#[test]
fn iceberg_struct_datetime_is_staged_as_microseconds() {
    let field = StdArc::new(Field::new(
        "observed",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        true,
    ));
    let values: ArrayRef = StdArc::new(
        TimestampNanosecondArray::from(vec![Some(-1), None, Some(1_234_567)]).with_timezone_utc(),
    );
    let source: ArrayRef = StdArc::new(
        StructArray::try_new(vec![field].into(), vec![values], None)
            .assured("source struct is valid"),
    );
    let mapped =
        arrow_schema::Schema::new(vec![Field::new("event", source.data_type().clone(), true)]);
    let staged_schema = IcebergTableEmitter::staged_arrow_schema(&mapped)
        .assured("datetime in a struct narrows in staged schema");
    let staged = IcebergTableEmitter::staged_column(&source, staged_schema.field(0).data_type())
        .assured("datetime in a struct narrows in staged values");
    assert_eq!(staged.data_type(), staged_schema.field(0).data_type());
    let observed = staged
        .as_any()
        .downcast_ref::<StructArray>()
        .assured("struct is retained")
        .column(0);
    let observed = observed
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .assured("struct member is a microsecond datetime");
    assert_eq!(observed.value(0), -1);
    assert!(observed.is_null(1));
    assert_eq!(observed.value(2), 1_234);
}

#[test]
fn iceberg_nested_container_shapes_keep_values_offsets_and_nulls() {
    let timestamp_field = StdArc::new(Field::new(
        "item",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        true,
    ));
    let stage = |source: &ArrayRef| {
        let mapped = arrow_schema::Schema::new(vec![Field::new(
            "observed",
            source.data_type().clone(),
            true,
        )]);
        let schema = IcebergTableEmitter::staged_arrow_schema(&mapped)
            .assured("nested datetime schema stages as microseconds");
        let staged = IcebergTableEmitter::staged_column(source, schema.field(0).data_type())
            .assured("nested datetime values stage as microseconds");
        assert_eq!(staged.data_type(), schema.field(0).data_type());
        staged
    };

    let mut large =
        LargeListBuilder::new(TimestampNanosecondBuilder::new().with_timezone("+00:00"))
            .with_field(timestamp_field.clone());
    large.values().append_value(-1);
    large.values().append_value(1_999);
    large.append(true);
    large.append(false);
    let source: ArrayRef = StdArc::new(large.finish());
    let staged = stage(&source);
    let large = staged
        .as_any()
        .downcast_ref::<LargeListArray>()
        .assured("large list remains a large list");
    assert_eq!(large.value_offsets(), &[0, 2, 2]);
    assert!(large.is_null(1));
    let values = large
        .values()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .assured("large list members are microsecond datetimes");
    assert_eq!(values.values().as_ref(), &[-1, 1]);

    let mut fixed =
        FixedSizeListBuilder::new(TimestampNanosecondBuilder::new().with_timezone("+00:00"), 2)
            .with_field(timestamp_field);
    fixed.values().append_value(1_999);
    fixed.values().append_null();
    fixed.append(true);
    fixed.values().append_value(0);
    fixed.values().append_value(0);
    fixed.append(false);
    let source: ArrayRef = StdArc::new(fixed.finish());
    let staged = stage(&source);
    let fixed = staged
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .assured("fixed list remains a fixed list");
    assert_eq!(fixed.value_length(), 2);
    assert!(fixed.is_null(1));
    let values = fixed
        .values()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .assured("fixed list members are microsecond datetimes");
    assert_eq!(values.value(0), 1);
    assert!(values.is_null(1));

    let mut map = MapBuilder::new(
        None,
        StringBuilder::new(),
        TimestampNanosecondBuilder::new().with_timezone("+00:00"),
    );
    map.keys().append_value("first");
    map.values().append_value(-1);
    map.append(true).assured("map entry is valid");
    let source: ArrayRef = StdArc::new(map.finish());
    let staged = stage(&source);
    let map = staged
        .as_any()
        .downcast_ref::<MapArray>()
        .assured("map remains a map");
    assert_eq!(map.value_offsets(), &[0, 1]);
    let values = map
        .entries()
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .assured("map values are microsecond datetimes");
    assert_eq!(values.value(0), -1);

    let error = IcebergTableEmitter::staged_column(&source, &DataType::Utf8)
        .expect_err("an unplanned staged type must fail publication");
    assert_eq!(
        *error.current_context(),
        EmitterWriteError::Publish { connector: ICEBERG }
    );
}

/// The staged schema and the staged values follow every list level, retaining each list
/// boundary and rounding negative nanoseconds down to the containing microsecond.
#[test]
fn bolero_nested_iceberg_datetimes_are_narrowed_at_every_depth() {
    let values = bolero::generator::produce_with::<Vec<i64>>().len(0_usize..=16);
    bolero::check!()
        .with_iterations(256)
        .with_max_len(160)
        .with_generator(values)
        .for_each(|nanoseconds| {
            let timestamp_field = StdArc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                false,
            ));
            let inner = ListBuilder::new(TimestampNanosecondBuilder::new().with_timezone("+00:00"))
                .with_field(timestamp_field.clone());
            let inner_field =
                StdArc::new(Field::new("item", DataType::List(timestamp_field), false));
            let mut outer = ListBuilder::new(inner).with_field(inner_field);
            for group in nanoseconds.chunks(3) {
                for &nanosecond in group {
                    outer.values().values().append_value(nanosecond);
                }
                outer.values().append(true);
            }
            outer.append(true);
            let source: ArrayRef = StdArc::new(outer.finish());
            let mapped = arrow_schema::Schema::new(vec![Field::new(
                "observed",
                source.data_type().clone(),
                true,
            )]);

            let staged_schema = IcebergTableEmitter::staged_arrow_schema(&mapped)
                .assured("nested datetime schema stages as microseconds");
            let staged =
                IcebergTableEmitter::staged_column(&source, staged_schema.field(0).data_type())
                    .assured("nested datetime values stage as microseconds");
            assert_eq!(staged.data_type(), staged_schema.field(0).data_type());
            let staged_outer = staged
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .assured("outer list is retained");
            let staged_inner = staged_outer
                .values()
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .assured("inner lists are retained");
            let staged_values = staged_inner
                .values()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .assured("the nested datetime is narrowed");
            let expected = nanoseconds
                .iter()
                .map(|value| value.div_euclid(1_000))
                .collect::<Vec<_>>();
            assert_eq!(staged_values.values().as_ref(), expected.as_slice());
            assert_eq!(staged_inner.len(), nanoseconds.chunks(3).len());
            assert_eq!(staged_outer.len(), 1);
            for (index, group) in nanoseconds.chunks(3).enumerate() {
                assert_eq!(staged_inner.value(index).len(), group.len());
            }
        });
}

#[test]
fn iceberg_staged_files_wait_for_the_commit_cadence_until_the_maximum_size() {
    let policy = commit_policy(100, 1_024);
    let cadence = EmitterStagedCommit::Cadence(Duration::from_millis(100));

    assert_eq!(policy.staged_commit(16), cadence);
    assert_eq!(policy.staged_commit(1_023), cadence);
    // Reaching the declared maximum makes the commit due without waiting for the cadence.
    assert_eq!(
        policy.staged_commit(1_024),
        EmitterStagedCommit::SizeReached
    );
    assert_eq!(
        policy.staged_commit(4_096),
        EmitterStagedCommit::SizeReached
    );
}

#[test]
fn iceberg_retained_acknowledgements_resolve_every_staged_batch_at_once() {
    let first = StdArc::new(RecordedAcknowledgements::default());
    let second = StdArc::new(RecordedAcknowledgements::default());
    let retained = RetainedAcknowledgements(vec![
        EmitterRecordAcknowledgements::new(RecordedHandover(first.clone())),
        EmitterRecordAcknowledgements::new(RecordedHandover(second.clone())),
    ]);

    retained.keep_alive();
    retained.acknowledge();
    retained.reject("Iceberg emitter dropped staged batch".to_string());

    assert!(!retained.is_empty());
    for recorded in [&first, &second] {
        assert_eq!(recorded.kept_alive.load(Ordering::Acquire), 1);
        assert_eq!(recorded.acknowledged.load(Ordering::Acquire), 1);
        assert_eq!(recorded.rejected.load(Ordering::Acquire), 1);
    }
}

#[nervix_primitives::test]
async fn staged_batches_are_appended_and_loaded_on_the_filesystem_workers_under_their_charge() {
    let executor = Executor::default();
    let directory = tempfile::tempdir().assured("the staging directory is created");
    let path = directory.path().join("staging-1.arrow");
    let schema = StdArc::new(arrow_schema::Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        StdArc::clone(&schema),
        vec![StdArc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
    )
    .assured("the column matches the schema");

    let staged = IcebergTableEmitter::append_staged(
        &executor,
        &staging::AppendTarget::New { path: path.clone() },
        StdArc::clone(&schema),
        vec![batch.clone()],
    )
    .await
    .assured("the filesystem workers append the staged batch to a new staging file");
    let range = staged.appended.ranges[0].clone();
    let written = range.end - range.start;
    let IcebergLoadedGroup { batches, charge } = IcebergTableEmitter::load_group(
        &executor,
        schema,
        vec![staging::Span {
            path,
            schema_end: staged.schema_end,
            bytes: range,
            batches: 1,
        }],
        IcebergLoadCharge::Staged { bytes: written },
    )
    .await
    .assured("the filesystem workers read the staged batch back");

    assert!(written > 0);
    assert_eq!(batches, vec![batch]);
    assert_eq!(charge.bytes(), written);
    assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, written);
    drop(charge);
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.filesystem_storage.admitted, 2);
    assert_eq!(snapshot.filesystem_storage.completed, 2);
    assert_eq!(snapshot.bulk_memory.reserved_bytes, 0);
}
