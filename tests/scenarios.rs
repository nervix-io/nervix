use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{OpenOptions, create_dir_all},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use arrow_array::{
    Array, BooleanArray, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    TimestampMicrosecondArray, UInt64Array,
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use cucumber::{
    World as _, WriterExt,
    gherkin::Step,
    given, then, when,
    writer::{self},
};
use futures_util::{TryStreamExt, future::try_join_all};
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
    arrow::arrow_schema_to_schema_auto_assign_ids,
    io::{
        FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA,
        S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
    },
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use mongodb::{
    Client as MongoDbClient,
    bson::{Bson as MongoDbBson, Document as MongoDbDocument, doc as mongodb_doc},
    options::{
        ClientOptions as MongoDbClientOptions, Tls as MongoDbTls, TlsOptions as MongoDbTlsOptions,
    },
};
use mysql_async::{
    Opts as MySqlOpts, OptsBuilder as MySqlOptsBuilder, Pool as MySqlPool, SslOpts as MySqlSslOpts,
    prelude::Queryable as MySqlQueryable,
};
use nervix::{
    application::InternalTransportMode, memory_pressure::MemoryPressureConfig,
    runtime::RuntimeTestHooks,
};
use nervix_client_core::Client;
use nervix_wasm::{
    WasmAckSidecar, WasmEnvelope, WasmOutputColumnRef, WasmOutputRow, WasmRoutedOutput,
};
use playwright_rs::{
    FilePayload, LaunchOptions, Playwright, Viewport, WaitForOptions, WaitForState,
};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use rustls_pki_types::{CertificateDer, pem::PemObject};
use tempfile::TempDir;
use tokio_postgres::{Client as PostgresClient, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;
use uuid::Uuid;

use crate::common::cluster::{
    BrokerObserver, Cluster, TEST_AUTH_USERNAME, TestClusterConfig, TestSession,
    WebsocketExchangeAction, client_connect_options,
};

mod common;

const SCENARIOS_PATH: &str = "tests/features";
const TEST_LOG_DIR: &str = "tests/logs";
const CUCUMBER_LOG_FILE: &str = "tests/logs/cucumber.log";
static ONNX_RUNTIME_INIT: OnceLock<Result<(), String>> = OnceLock::new();
static ICEBERG_TABLE_PROVISION_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

#[derive(cucumber::World, Debug, Default)]
struct ScenarioWorld {
    cluster: Option<Cluster>,
    active_session: Option<TestSession>,
    active_session_node: Option<String>,
    active_session_has_subscription: bool,
    last_subscription_payload: Option<String>,
    last_command_error: Option<String>,
    last_command_output: Option<String>,
    last_server_error: Option<String>,
    last_auth_attempts_elapsed: Option<Duration>,
    broker_observer: Option<BrokerObserver>,
    last_broker_payload: Option<String>,
    last_broker_headers: Vec<(String, String)>,
    clickhouse_table: Option<String>,
    clickhouse_tls: bool,
    postgres_table: Option<String>,
    postgres_tls: bool,
    mysql_table: Option<String>,
    mysql_tls: bool,
    mongodb_collection: Option<String>,
    mongodb_tls: bool,
    domain: String,
    test_id: String,
    zeromq_ingest_addr: String,
    zeromq_emit_addr: String,
    placeholders: BTreeMap<String, String>,
    mqtt_ingestors_by_domain: BTreeMap<String, BTreeSet<String>>,
    avro_http_field_order: Vec<String>,
    avro_http_optional_fields: BTreeSet<String>,
    runtime_test_hooks: RuntimeTestHooks,
    cluster_config: TestClusterConfig,
    temp_root: Option<TempDir>,
    last_cluster_operation_elapsed: Option<Duration>,
    browser_page: Option<playwright_rs::Page>,
    browser_context: Option<playwright_rs::BrowserContext>,
    browser: Option<playwright_rs::Browser>,
    playwright: Option<Playwright>,
}

impl ScenarioWorld {
    fn cluster_mut(&mut self) -> &mut Cluster {
        self.cluster
            .as_mut()
            .expect("cluster must be created before using it")
    }

    fn cluster(&self) -> &Cluster {
        self.cluster
            .as_ref()
            .expect("cluster must be created before using it")
    }
}

#[derive(Clone, Copy, Debug)]
enum IngestorLogicTransportFixture {
    HttpEndpoint,
    Kafka,
    Mqtt,
    Nats,
    WebsocketEndpoint,
    ZeroMq,
}

impl IngestorLogicTransportFixture {
    fn parse(value: &str) -> Self {
        match value {
            "http_endpoint" => Self::HttpEndpoint,
            "kafka" => Self::Kafka,
            "mqtt" => Self::Mqtt,
            "nats" => Self::Nats,
            "websocket_endpoint" => Self::WebsocketEndpoint,
            "zeromq" => Self::ZeroMq,
            other => panic!("unsupported ingestor logic transport fixture '{other}'"),
        }
    }

    async fn prepare(self, world: &mut ScenarioWorld) {
        if let Self::Kafka = self {
            let topic = expand_placeholders(world, "logic_notifications_{{test_id}}");
            world
                .cluster()
                .ensure_kafka_topic_partitions(&topic, 1)
                .await
                .expect("failed to prepare ingestor logic kafka topic");
        }
    }

    async fn await_ready(self, world: &mut ScenarioWorld) {
        let _ = world;
    }

    fn setup_fragment(self) -> &'static str {
        match self {
            Self::HttpEndpoint => {
                r#"
      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT logic_endpoint
        ON edge
        PATH '/logic'
        TYPE HTTP;
"#
            }
            Self::Kafka => {
                r#"
      CREATE CLIENT logic_kafka
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
"#
            }
            Self::Mqtt => {
                r#"
      CREATE CLIENT logic_mqtt
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-logic-{{test_id}}'
        };
"#
            }
            Self::Nats => {
                r#"
      CREATE CLIENT logic_nats
        TYPE NATS
        CONFIG {
          'addr' = 'nats://127.0.0.1:4222'
        };
"#
            }
            Self::WebsocketEndpoint => {
                r#"
      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE ENDPOINT logic_endpoint
        ON edge
        PATH '/logic'
        TYPE WEBSOCKETS;
"#
            }
            Self::ZeroMq => {
                r#"
      CREATE CLIENT logic_zeromq
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_ingest_addr}}',
          'bind' = 'true'
        };
"#
            }
        }
    }

    fn source_fragment(self) -> &'static str {
        match self {
            Self::HttpEndpoint | Self::WebsocketEndpoint => {
                "FROM ENDPOINT logic_endpoint MODE NO_ACK SEQUENTIAL"
            }
            Self::Kafka => {
                r#"TIMESTAMP NOW
        FROM KAFKA logic_kafka
        TOPIC logic_notifications_{{test_id}}
        OFFSET BY DOMAIN
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms"#
            }
            Self::Mqtt => {
                r#"FROM MQTT logic_mqtt
        TOPIC logic_notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL"#
            }
            Self::Nats => {
                r#"FROM NATS logic_nats
        SUBJECT logic_notifications_{{test_id}}
        QUEUE GROUP logic_notifications_group_{{test_id}}
        INSTANCES 1
        MODE NO_ACK SEQUENTIAL"#
            }
            Self::ZeroMq => "FROM ZEROMQ logic_zeromq MODE NO_ACK SEQUENTIAL",
        }
    }

    async fn deliver(self, world: &mut ScenarioWorld, payload: &str) {
        match self {
            Self::HttpEndpoint => {
                let host = expand_placeholders(world, "http-{{test_id}}.example.com");
                world
                    .cluster()
                    .publish_http("node-1", &host, "/logic", payload)
                    .await
                    .expect("failed to post ingestor logic http payload");
            }
            Self::Kafka => {
                let topic = expand_placeholders(world, "logic_notifications_{{test_id}}");
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    tokio::task::consume_budget().await;
                    world
                        .cluster()
                        .publish_kafka(&topic, payload)
                        .await
                        .expect("failed to publish ingestor logic kafka payload");
                    if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await
                    {
                        return;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for ingestor logic kafka payload to reach the relay \
                         subscription"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            Self::Mqtt => {
                let topic = expand_placeholders(world, "logic_notifications_{{test_id}}");
                world
                    .cluster()
                    .publish_mqtt(&topic, payload)
                    .await
                    .expect("failed to publish ingestor logic mqtt payload");
            }
            Self::Nats => {
                let subject = expand_placeholders(world, "logic_notifications_{{test_id}}");
                world
                    .cluster()
                    .publish_nats(&subject, payload)
                    .await
                    .expect("failed to publish ingestor logic nats payload");
            }
            Self::WebsocketEndpoint => {
                let host = expand_placeholders(world, "ws-{{test_id}}.example.com");
                world
                    .cluster()
                    .publish_websocket("node-1", &host, "/logic", payload)
                    .await
                    .expect("failed to publish ingestor logic websocket payload");
            }
            Self::ZeroMq => {
                world
                    .cluster()
                    .publish_zeromq(&world.zeromq_ingest_addr, payload)
                    .await
                    .expect("failed to publish ingestor logic zeromq payload");
            }
        }
    }

    async fn deliver_with_headers(self, world: &mut ScenarioWorld, payload: &str) {
        let headers = [("tenant", "acme"), ("route", "header-route")];
        match self {
            Self::HttpEndpoint => {
                let host = expand_placeholders(world, "http-{{test_id}}.example.com");
                world
                    .cluster()
                    .publish_http_with_headers("node-1", &host, "/logic", payload, &headers)
                    .await
                    .expect("failed to post ingestor logic http payload with headers");
            }
            Self::Kafka => {
                let topic = expand_placeholders(world, "logic_notifications_{{test_id}}");
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    tokio::task::consume_budget().await;
                    world
                        .cluster()
                        .publish_kafka_with_headers(&topic, payload, &headers)
                        .await
                        .expect("failed to publish ingestor logic kafka payload with headers");
                    if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await
                    {
                        return;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for ingestor logic kafka payload with headers to reach \
                         the relay subscription"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            Self::Nats => {
                let subject = expand_placeholders(world, "logic_notifications_{{test_id}}");
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    tokio::task::consume_budget().await;
                    world
                        .cluster()
                        .publish_nats_with_headers(&subject, payload, &headers)
                        .await
                        .expect("failed to publish ingestor logic nats payload with headers");
                    if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await
                    {
                        return;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for ingestor logic nats payload with headers to reach \
                         the relay subscription"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            Self::Mqtt | Self::WebsocketEndpoint | Self::ZeroMq => {
                panic!("ingestor logic transport fixture '{self:?}' does not support headers")
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum IngestorLogicOutputSchemaFixture {
    Input,
    Rewritten,
    HeaderRouted,
    Parsed,
    FunctionMatrix,
    ExtendedBuiltinMatrix,
    CastMatrix,
    ArithmeticMatrix,
    MathBuiltinMatrix,
    InternalTypes,
    ListOperations,
}

impl IngestorLogicOutputSchemaFixture {
    fn parse(value: &str) -> Self {
        match value {
            "input" => Self::Input,
            "rewritten" => Self::Rewritten,
            "header_routed" => Self::HeaderRouted,
            "parsed" => Self::Parsed,
            "function_matrix" => Self::FunctionMatrix,
            "extended_builtin_matrix" => Self::ExtendedBuiltinMatrix,
            "cast_matrix" => Self::CastMatrix,
            "arithmetic_matrix" => Self::ArithmeticMatrix,
            "math_builtin_matrix" => Self::MathBuiltinMatrix,
            "internal_types" => Self::InternalTypes,
            "list_operations" => Self::ListOperations,
            other => panic!("unsupported ingestor logic output schema fixture '{other}'"),
        }
    }

    fn schema_name(self) -> &'static str {
        match self {
            Self::Input => "logic_notification_ingest",
            Self::Rewritten => "logic_notification_rewritten",
            Self::HeaderRouted => "logic_notification_header_routed",
            Self::Parsed => "logic_notification_parsed",
            Self::FunctionMatrix => "logic_notification_function_matrix",
            Self::ExtendedBuiltinMatrix => "logic_notification_extended_builtin_matrix",
            Self::CastMatrix => "logic_notification_cast_matrix",
            Self::ArithmeticMatrix => "logic_notification_arithmetic_matrix",
            Self::MathBuiltinMatrix => "logic_notification_math_builtin_matrix",
            Self::InternalTypes => "logic_notification_internal_types",
            Self::ListOperations => "logic_notification_list_operations",
        }
    }

    fn input_codec_name(self) -> &'static str {
        match self {
            Self::InternalTypes => "logic_notification_internal_types_ingest_codec",
            Self::ListOperations => "logic_notification_list_operations_ingest_codec",
            Self::Input
            | Self::Rewritten
            | Self::HeaderRouted
            | Self::Parsed
            | Self::FunctionMatrix
            | Self::ExtendedBuiltinMatrix
            | Self::CastMatrix
            | Self::ArithmeticMatrix
            | Self::MathBuiltinMatrix => "logic_notification_ingest_codec",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum IngestorLogicPayloadFixture {
    MixedFilterMessages,
    HeaderMessage,
    RuntimeFailureMessage,
    FunctionMatrixMessage,
    ExtendedBuiltinMessage,
    CastMatrixMessage,
    ArithmeticMessage,
    MathBuiltinMessage,
    InternalTypesMessage,
    ListOperationsMessage,
}

impl IngestorLogicPayloadFixture {
    fn parse(value: &str) -> Self {
        match value {
            "mixed_filter_messages" => Self::MixedFilterMessages,
            "header_message" => Self::HeaderMessage,
            "runtime_failure_message" => Self::RuntimeFailureMessage,
            "function_matrix_message" => Self::FunctionMatrixMessage,
            "extended_builtin_message" => Self::ExtendedBuiltinMessage,
            "cast_matrix_message" => Self::CastMatrixMessage,
            "arithmetic_message" => Self::ArithmeticMessage,
            "math_builtin_message" => Self::MathBuiltinMessage,
            "internal_types_message" => Self::InternalTypesMessage,
            "list_operations_message" => Self::ListOperationsMessage,
            other => panic!("unsupported ingestor logic payload fixture '{other}'"),
        }
    }

    fn payloads(self) -> &'static [&'static str] {
        match self {
            Self::MixedFilterMessages => &[
                r#"{"tenant":"acme","active":true,"amount":7,"raw":"URGENT"}"#,
                r#"{"tenant":"acme","active":false,"amount":8,"raw":"DROP"}"#,
            ],
            Self::HeaderMessage => {
                &[r#"{"tenant":"acme","active":true,"amount":7,"raw":"ignored"}"#]
            }
            Self::RuntimeFailureMessage => {
                &[r#"{"tenant":"acme","active":true,"amount":7,"raw":"not-a-number"}"#]
            }
            Self::FunctionMatrixMessage => {
                &[r#"{"tenant":"acme","active":true,"amount":7,"raw":"  KeepMe  "}"#]
            }
            Self::ExtendedBuiltinMessage => {
                &[r#"{"tenant":"acme","active":true,"amount":7,"raw":"  hello.world  "}"#]
            }
            Self::CastMatrixMessage => {
                &[r#"{"tenant":"acme","active":false,"amount":42,"raw":"42"}"#]
            }
            Self::ArithmeticMessage => {
                &[r#"{"tenant":"acme","active":false,"amount":20,"raw":"6"}"#]
            }
            Self::MathBuiltinMessage => {
                &[r#"{"tenant":"acme","active":true,"amount":7,"raw":"ignored"}"#]
            }
            Self::InternalTypesMessage => &[
                r#"{"tenant":"acme","active":true,"u8":5,"i8":-7,"u16":9,"i16":12,"u32":42,"i32":-11,"u64":100,"i64":-64,"f32":2.5,"f64":7.25,"occurred_at":"2026-04-07T12:34:56Z","raw":"ignored"}"#,
            ],
            Self::ListOperationsMessage => &[
                r#"{"tenant":"acme","values":[1,2,3],"fixed":[10,20],"labels":["prod","api","edge"]}"#,
            ],
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum IngestorLogicExpectationFixture {
    CompileError,
    RewrittenFilteredOnce,
    HeaderRoutedOnce,
    RuntimeErrorDrop,
}

impl IngestorLogicExpectationFixture {
    fn parse(value: &str) -> Self {
        match value {
            "compile_error" => Self::CompileError,
            "rewritten_filtered_once" => Self::RewrittenFilteredOnce,
            "header_routed_once" => Self::HeaderRoutedOnce,
            "runtime_error_drop" => Self::RuntimeErrorDrop,
            other => panic!("unsupported ingestor logic expectation fixture '{other}'"),
        }
    }

    async fn assert_observed(self, world: &mut ScenarioWorld) {
        match self {
            Self::CompileError => {
                let error = world
                    .last_command_error
                    .as_deref()
                    .expect("logic compile failure should populate last_command_error");
                append_cucumber_log_line(&format!("logic compile error observed: {error}"));
            }
            Self::RewrittenFilteredOnce => {
                capture_and_assert_subscription_payload(
                    world,
                    "\"normalized\":\"urgent\"",
                    false,
                    Duration::from_secs(10),
                )
                .await;
                let payload = world
                    .last_subscription_payload
                    .as_deref()
                    .expect("logic rewrite payload must be captured");
                assert!(
                    payload.contains("\"amount\":8"),
                    "expected rewritten payload to contain incremented amount, got: {payload}"
                );
                assert!(
                    payload.contains(r#"key={"tenant":"acme"}"#),
                    "expected rewritten payload to preserve tenant key, got: {payload}"
                );
                assert!(
                    !payload.contains("\"raw\""),
                    "expected rewritten payload to omit raw field, got: {payload}"
                );
                assert_no_subscription_payload_within(world, Duration::from_secs(1)).await;
            }
            Self::HeaderRoutedOnce => {
                capture_and_assert_subscription_payload(
                    world,
                    "\"normalized\":\"header-route\"",
                    false,
                    Duration::from_secs(10),
                )
                .await;
                let payload = world
                    .last_subscription_payload
                    .as_deref()
                    .expect("header rewrite payload must be captured");
                assert!(
                    payload.contains("\"amount\":8"),
                    "expected header-routed payload to contain incremented amount, got: {payload}"
                );
                assert!(
                    payload.contains(r#"key={"tenant":"acme"}"#),
                    "expected header-routed payload to preserve tenant key, got: {payload}"
                );
                assert!(
                    !payload.contains("\"raw\""),
                    "expected header-routed payload to omit raw field, got: {payload}"
                );
                assert_no_subscription_payload_within(world, Duration::from_secs(1)).await;
            }
            Self::RuntimeErrorDrop => {
                then_within_duration_the_active_session_observes_a_server_error(
                    world,
                    "2s".to_string(),
                )
                .await;
                assert_no_subscription_payload_within(world, Duration::from_secs(1)).await;
            }
        }
    }
}

fn build_ingestor_logic_commands(
    transport: IngestorLogicTransportFixture,
    output_schema: IngestorLogicOutputSchemaFixture,
    logic_program: &str,
    include_subscription: bool,
) -> String {
    let subscription_commands = if include_subscription {
        let start_command = if let IngestorLogicTransportFixture::Kafka = transport {
            "START AT NOW;"
        } else {
            "START;"
        };
        format!(
            r#"
      CREATE SUBSCRIPTION logic_notifications_subscription TO logic_notifications;
      {start_command}
"#
        )
    } else {
        String::new()
    };
    let logic_program = logic_program.trim().trim_end_matches(';');
    format!(
        r#"
      CREATE SCHEMA logic_notification_ingest (
        tenant STRING,
        active BOOL,
        amount I64,
        raw STRING
      );

      CREATE SCHEMA logic_notification_internal_types_ingest (
        tenant STRING,
        active BOOL,
        u8 U8,
        i8 I8,
        u16 U16,
        i16 I16,
        u32 U32,
        i32 I32,
        u64 U64,
        i64 I64,
        f32 F32,
        f64 F64,
        occurred_at DATETIME,
        raw STRING
      );

      CREATE SCHEMA logic_notification_list_operations_ingest (
        tenant STRING,
        values VEC<I64>,
        fixed ARRAY<I64, 2>,
        labels VEC<STRING>
      );

      CREATE SCHEMA logic_notification_rewritten (
        tenant STRING,
        active BOOL,
        amount I64,
        normalized STRING
      );

      CREATE SCHEMA logic_notification_header_routed (
        tenant STRING,
        active BOOL,
        amount I64,
        normalized STRING OPTIONAL
      );

      CREATE SCHEMA logic_notification_parsed (
        tenant STRING,
        parsed I64
      );

      CREATE SCHEMA logic_notification_function_matrix (
        tenant STRING,
        amount_abs I64,
        trimmed STRING,
        lowered STRING,
        uppered STRING,
        raw_len I64,
        contains_keep BOOL,
        starts_keep BOOL,
        ends_me BOOL,
        fallback STRING,
        was_keep BOOL
      );

      CREATE SCHEMA logic_notification_extended_builtin_matrix (
        tenant STRING,
        now_text STRING,
        uuid4 STRING,
        uuid7 STRING,
        bit_len I64,
        ascii_value I64,
        btrimmed STRING,
        char_len I64,
        joined STRING,
        titled STRING,
        lefted STRING,
        lowered STRING,
        lpaded STRING,
        ltrimmed STRING,
        digest STRING,
        repeated STRING,
        replaced STRING,
        reversed STRING,
        righted STRING,
        rpaded STRING,
        rtrimmed STRING,
        part STRING,
        starts BOOL,
        pos I64,
        piece STRING,
        hexed STRING,
        translated STRING,
        trimmed2 STRING,
        uppered STRING,
        regex_ok BOOL,
        regex_replaced STRING,
        regex_piece STRING
      );

      CREATE SCHEMA logic_notification_cast_matrix (
        tenant STRING,
        parsed I64,
        amount_text STRING,
        amount_float F64,
        truthy BOOL,
        not_active BOOL,
        literal_bool BOOL,
        literal_float F64,
        literal_int I64,
        label STRING,
        is_exact BOOL,
        negated I64
      );

      CREATE SCHEMA logic_notification_arithmetic_matrix (
        tenant STRING,
        parsed I64,
        sum I64,
        difference I64,
        product I64,
        quotient I64,
        remainder I64,
        complex I64,
        comparison BOOL,
        chained STRING
      );

      CREATE SCHEMA logic_notification_math_builtin_matrix (
        tenant STRING,
        absolute I64,
        acos_value F64,
        asin_value F64,
        atan_value F64,
        ceil_value F64,
        cos_value F64,
        exp_value F64,
        floor_value F64,
        ln_value F64,
        log_value F64,
        log_base_value F64,
        pow_value F64,
        round_value F64,
        sqrt_value F64,
        tan_value F64
      );

      CREATE SCHEMA logic_notification_internal_types (
        tenant STRING,
        u8_next U8,
        i8_abs I8,
        u16_keep U16,
        i16_prev I16,
        u32_same U32,
        i32_neg I32,
        u64_next U64,
        i64_keep I64,
        f32_next F32,
        f64_keep F64,
        bool_copy BOOL,
        occurred_text STRING,
        occurred_copy DATETIME
      );

      CREATE SCHEMA logic_notification_list_operations (
        tenant STRING,
        total I64,
        first_value I64,
        last_value I64,
        second_value I64,
        value_count I64,
        fixed_first I64,
        fixed_last I64,
        first_label STRING,
        last_label STRING
      );

      CREATE STRICT WIRE JSON SCHEMA logic_notification_ingest_wire (
        tenant string,
        active boolean,
        amount integer,
        raw string
      );

      CREATE STRICT WIRE JSON SCHEMA logic_notification_internal_types_ingest_wire (
        tenant string,
        active boolean,
        u8 integer,
        i8 integer,
        u16 integer,
        i16 integer,
        u32 integer,
        i32 integer,
        u64 integer,
        i64 integer,
        f32 number,
        f64 number,
        occurred_at string,
        raw string
      );

      CREATE STRICT WIRE JSON SCHEMA logic_notification_list_operations_ingest_wire (
        tenant string,
        values array,
        fixed array,
        labels array
      );

      CREATE CODEC logic_notification_ingest_codec
        FROM WIRE JSON SCHEMA logic_notification_ingest_wire
        TO SCHEMA logic_notification_ingest;

      CREATE CODEC logic_notification_internal_types_ingest_codec
        FROM WIRE JSON SCHEMA logic_notification_internal_types_ingest_wire
        TO SCHEMA logic_notification_internal_types_ingest
        ENCODE occurred_at AS RFC3339;

      CREATE CODEC logic_notification_list_operations_ingest_codec
        FROM WIRE JSON SCHEMA logic_notification_list_operations_ingest_wire
        TO SCHEMA logic_notification_list_operations_ingest;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_logic_ingestor
        SCHEMA tenant_branch
        TTL 5m;
      CREATE RELAY logic_notifications SCHEMA {} BRANCHED BY by_logic_ingestor;
      {}
      CREATE INGESTOR logic_ingestor
        TO logic_notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        {} ON MESSAGE ERROR LOG
        DECODE USING {}
        BRANCHED BY by_logic_ingestor
        VALUES {{ tenant = logic_notifications.tenant }}

        {} ON GENERAL ERROR LOG;
      {}
"#,
        output_schema.schema_name(),
        transport.setup_fragment(),
        logic_program,
        output_schema.input_codec_name(),
        transport.source_fragment(),
        subscription_commands
    )
}

fn append_cucumber_log_line(line: &str) {
    let _ = create_dir_all(TEST_LOG_DIR);
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(CUCUMBER_LOG_FILE)
    {
        let _ = writeln!(file, "{line}");
    }
}

fn truncate_cucumber_log() {
    let _ = create_dir_all(TEST_LOG_DIR);
    let _ = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(CUCUMBER_LOG_FILE);
}

async fn clickhouse_post_to(
    url: &str,
    ca_file: Option<PathBuf>,
    query: &str,
) -> Result<String, String> {
    let mut builder = reqwest::Client::builder();
    if let Some(ca_file) = ca_file {
        let ca_pem = std::fs::read(&ca_file)
            .map_err(|source| format!("failed to read ClickHouse TLS CA: {source}"))?;
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_pem(&ca_pem)
                .map_err(|source| format!("failed to parse ClickHouse TLS CA: {source}"))?,
        );
    }
    let response = builder
        .build()
        .map_err(|source| source.to_string())?
        .post(url)
        .basic_auth("default", Some("nervix"))
        .body(query.to_string())
        .send()
        .await
        .map_err(|source| source.to_string())?;
    let status = response.status();
    let body = response.text().await.map_err(|source| source.to_string())?;
    if status.is_success() {
        Ok(body)
    } else {
        Err(format!("ClickHouse query failed with {status}: {body}"))
    }
}

async fn clickhouse_post(query: &str) -> Result<String, String> {
    clickhouse_post_to("http://127.0.0.1:8123/", None, query).await
}

async fn clickhouse_tls_post(query: &str) -> Result<String, String> {
    let ca_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tls/dev")
        .join("ca.pem");
    clickhouse_post_to("https://127.0.0.1:8124/", Some(ca_file), query).await
}

async fn clickhouse_post_for_world(world: &ScenarioWorld, query: &str) -> Result<String, String> {
    if world.clickhouse_tls {
        clickhouse_tls_post(query).await
    } else {
        clickhouse_post(query).await
    }
}

async fn postgres_client(tls: bool) -> Result<PostgresClient, String> {
    let addr = if tls {
        "host=127.0.0.1 port=5433 user=postgres password=nervix dbname=postgres sslmode=require"
    } else {
        "host=127.0.0.1 port=5432 user=postgres password=nervix dbname=postgres"
    };
    if tls {
        let ca_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tls/dev")
            .join("ca.pem");
        let ca_pem = std::fs::read(&ca_file)
            .map_err(|source| format!("failed to read Postgres TLS CA: {source}"))?;
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(&ca_pem) {
            let cert =
                cert.map_err(|source| format!("failed to parse Postgres TLS CA: {source}"))?;
            roots
                .add(cert)
                .map_err(|source| format!("failed to add Postgres TLS CA: {source}"))?;
        }
        let tls_config = RustlsClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let (client, connection) =
            tokio_postgres::connect(addr, MakeRustlsConnect::new(tls_config))
                .await
                .map_err(|source| source.to_string())?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    } else {
        let (client, connection) = tokio_postgres::connect(addr, NoTls)
            .await
            .map_err(|source| source.to_string())?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }
}

fn mysql_pool(tls: bool) -> Result<MySqlPool, String> {
    let addr = if tls {
        "mysql://nervix:nervix@127.0.0.1:3307/nervix?require_ssl=true"
    } else {
        "mysql://nervix:nervix@127.0.0.1:3306/nervix"
    };
    let opts = MySqlOpts::from_url(addr).map_err(|source| source.to_string())?;
    let opts = if tls {
        let ca_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tls/dev")
            .join("ca.pem");
        let ssl_opts = MySqlSslOpts::default()
            .with_root_certs(vec![ca_file.into()])
            .with_disable_built_in_roots(true);
        MySqlOptsBuilder::from_opts(opts).ssl_opts(Some(ssl_opts))
    } else {
        MySqlOptsBuilder::from_opts(opts)
    };
    Ok(MySqlPool::new(opts))
}

async fn mongodb_client(tls: bool) -> Result<MongoDbClient, String> {
    let addr = if tls {
        "mongodb://root:nervix@127.0.0.1:27018/nervix?authSource=admin&tls=true"
    } else {
        "mongodb://root:nervix@127.0.0.1:27017/nervix?authSource=admin"
    };
    let mut options = MongoDbClientOptions::parse(addr)
        .await
        .map_err(|source| source.to_string())?;
    if tls {
        let ca_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tls/dev")
            .join("ca.pem");
        options.tls = Some(MongoDbTls::Enabled(
            MongoDbTlsOptions::builder().ca_file_path(ca_file).build(),
        ));
    }
    MongoDbClient::with_options(options).map_err(|source| source.to_string())
}

async fn append_cluster_statuses(world: &ScenarioWorld, prefix: &str) {
    let Some(cluster) = world.cluster.as_ref() else {
        append_cucumber_log_line(&format!("{prefix}: cluster not initialized"));
        return;
    };
    let snapshots = cluster.collect_status_snapshots().await;
    if snapshots.is_empty() {
        append_cucumber_log_line(&format!("{prefix}: no cluster nodes"));
        return;
    }
    for (node_id, snapshot) in snapshots {
        match snapshot {
            Ok(status) => append_cucumber_log_line(&format!(
                "{prefix}: node={node_id} status={}",
                status.replace('\n', "\\n")
            )),
            Err(error) => {
                append_cucumber_log_line(&format!("{prefix}: node={node_id} status_error={error}"))
            }
        }
    }
}

#[given(expr = "a {int} node nervix cluster is started")]
async fn given_cluster_is_started(world: &mut ScenarioWorld, node_count: usize) {
    assert!(world.cluster.is_none(), "cluster is already started");
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    world.last_subscription_payload = None;
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.broker_observer = None;
    world.last_broker_payload = None;
    world.last_broker_headers.clear();
    world.domain = format!("d{}", Uuid::now_v7().as_simple());
    world.test_id = format!("t{}", Uuid::now_v7().as_simple());
    world.zeromq_ingest_addr = format!(
        "tcp://127.0.0.1:{}",
        crate::common::cluster::next_port().expect("failed to allocate ZeroMQ ingest port")
    );
    world.zeromq_emit_addr = format!(
        "tcp://127.0.0.1:{}",
        crate::common::cluster::next_port().expect("failed to allocate ZeroMQ emit port")
    );
    world.placeholders.clear();
    append_cucumber_log_line(&format!(
        "cluster start requested: nodes={node_count} domain={} test_id={}",
        world.domain, world.test_id
    ));
    match Cluster::start_with_config(
        node_count,
        world.runtime_test_hooks.clone(),
        world.cluster_config.clone(),
    )
    .await
    {
        Ok(cluster) => {
            world.cluster = Some(cluster);
        }
        Err(error) => {
            append_cucumber_log_line(&format!("cluster start failed: {error}"));
            panic!("failed to start cluster: {error}");
        }
    }
}

#[given(
    expr = "runtime replication is configured with replica count {int} and snapshot interval \
            {string}"
)]
async fn given_runtime_replication_is_configured(
    world: &mut ScenarioWorld,
    replica_count: usize,
    snapshot_interval: String,
) {
    assert!(
        world.cluster.is_none(),
        "replication must be configured before cluster startup"
    );
    world.cluster_config.replica_count = replica_count;
    world.cluster_config.state_snapshot_interval = humantime::parse_duration(&snapshot_interval)
        .expect("snapshot interval must be a valid duration");
}

#[given("temporary files use a custom temp directory")]
async fn given_temporary_files_use_custom_temp_directory(world: &mut ScenarioWorld) {
    assert!(
        world.cluster.is_none(),
        "temporary file directory must be configured before cluster startup"
    );
    let temp_root = tempfile::Builder::new()
        .prefix("nervix-temp-")
        .tempdir()
        .expect("failed to create temp root");
    world.cluster_config.temp_dir = Some(temp_root.path().to_path_buf());
    world.temp_root = Some(temp_root);
}

#[given(
    expr = "memory pressure is configured with high watermark {string} and low watermark {string}"
)]
async fn given_memory_pressure_is_configured(
    world: &mut ScenarioWorld,
    high_watermark: String,
    low_watermark: String,
) {
    assert!(
        world.cluster.is_none(),
        "memory pressure must be configured before cluster startup"
    );
    let config = MemoryPressureConfig::builder()
        .high_watermark(
            high_watermark
                .parse::<ubyte::ByteUnit>()
                .expect("high watermark must be valid bytes"),
        )
        .low_watermark(
            low_watermark
                .parse::<ubyte::ByteUnit>()
                .expect("low watermark must be valid bytes"),
        )
        .check_interval(Duration::from_millis(50))
        .resume_jitter(Duration::from_millis(10))
        .build();
    config
        .validate()
        .expect("memory pressure watermarks must be valid");
    world.cluster_config.memory_pressure = Some(config);
}

#[given(expr = "drain timeout is configured as {string}")]
async fn given_drain_timeout_is_configured(world: &mut ScenarioWorld, timeout: String) {
    assert!(
        world.cluster.is_none(),
        "drain timeout must be configured before cluster startup"
    );
    world.cluster_config.drain_timeout =
        humantime::parse_duration(&timeout).expect("shutdown drain timeout must be valid");
}

#[given("graceful shutdown drain is enabled")]
async fn given_graceful_shutdown_drain_is_enabled(world: &mut ScenarioWorld) {
    assert!(
        world.cluster.is_none(),
        "graceful shutdown drain must be configured before cluster startup"
    );
    world.cluster_config.graceful_shutdown_drain = true;
}

#[given(
    expr = "cluster internal transports are configured with cluster api mode {string} and \
            interconnect mode {string}"
)]
async fn given_cluster_internal_transports_are_configured(
    world: &mut ScenarioWorld,
    cluster_api_mode: String,
    interconnect_mode: String,
) {
    assert!(
        world.cluster.is_none(),
        "internal transport modes must be configured before cluster startup"
    );
    world.cluster_config.cluster_api_mode = parse_internal_transport_mode(&cluster_api_mode);
    world.cluster_config.interconnect_mode = parse_internal_transport_mode(&interconnect_mode);
}

#[given(expr = "client grpc transport is configured with mode {string}")]
async fn given_client_grpc_transport_is_configured(world: &mut ScenarioWorld, grpc_mode: String) {
    assert!(
        world.cluster.is_none(),
        "grpc transport mode must be configured before cluster startup"
    );
    world.cluster_config.grpc_mode = parse_internal_transport_mode(&grpc_mode);
}

fn parse_internal_transport_mode(value: &str) -> InternalTransportMode {
    match value {
        "http" => InternalTransportMode::Http,
        "https" => InternalTransportMode::Https,
        other => panic!("unsupported internal transport mode '{other}'"),
    }
}

#[given(expr = "branched relay expiration scan interval is configured as {string}")]
async fn given_branched_relay_expiration_scan_interval_is_configured(
    world: &mut ScenarioWorld,
    scan_interval: String,
) {
    assert!(
        world.cluster.is_none(),
        "expiration must be configured before cluster startup"
    );
    world
        .runtime_test_hooks
        .branch_instance_expiration_scan_interval = Some(
        humantime::parse_duration(&scan_interval).expect("scan interval must be a valid duration"),
    );
}

#[given(expr = "node {string} has resource directory {string} containing")]
async fn given_node_has_resource_directory_containing(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
    #[step] step: &Step,
) {
    let base_dir = world
        .cluster()
        .node_base_dir(&node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(&placeholder);
    if resource_dir.exists() {
        std::fs::remove_dir_all(&resource_dir).expect("old fixture directory should be removed");
    }
    std::fs::create_dir_all(&resource_dir).expect("fixture directory should be created");

    let files: BTreeMap<String, String> =
        serde_json::from_str(docstring(step)).expect("fixture docstring must be valid JSON");
    for (relative_path, contents) in files {
        let destination = resource_dir.join(PathBuf::from(relative_path));
        let parent = destination
            .parent()
            .expect("fixture file must have a parent directory");
        std::fs::create_dir_all(parent).expect("fixture parent directory should be created");
        std::fs::write(destination, contents).expect("fixture file should be written");
    }

    world
        .placeholders
        .insert(placeholder, resource_dir.display().to_string());
}

#[given(expr = "node {string} has ONNX fixture resource directory {string}")]
async fn given_node_has_onnx_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    ensure_onnx_runtime_loaded();

    let base_dir = world
        .cluster()
        .node_base_dir(&node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(&placeholder);
    if resource_dir.exists() {
        std::fs::remove_dir_all(&resource_dir).expect("old fixture directory should be removed");
    }
    std::fs::create_dir_all(resource_dir.join("models"))
        .expect("fixture model directory should be created");
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let source_path = repo_root
        .join("tests")
        .join("fixtures")
        .join("onnx")
        .join("simple_score.onnx");
    for fixture in [
        "simple_score.onnx",
        "batch_score.onnx",
        "dynamic_batch_score.onnx",
        "matrix_identity.onnx",
        "f64_score.onnx",
    ] {
        let source_path = source_path.with_file_name(fixture);
        let destination_path = resource_dir.join("models").join(fixture);
        std::fs::copy(&source_path, &destination_path).unwrap_or_else(|error| {
            panic!(
                "failed to copy ONNX fixture '{}' to '{}': {error}",
                source_path.display(),
                destination_path.display()
            )
        });
    }

    world
        .placeholders
        .insert(placeholder, resource_dir.display().to_string());
}

#[given(expr = "node {string} has WASM processor fixture resource directory {string}")]
async fn given_node_has_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_wasm_processor_fixture(world, &node_id, &placeholder, "rust").await;
}

#[given(expr = "node {string} has {string} WASM processor fixture resource directory {string}")]
async fn given_node_has_named_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    guest: String,
    placeholder: String,
) {
    place_wasm_processor_fixture(world, &node_id, &placeholder, &guest).await;
}

#[given(expr = "node {string} has {string} example WASM processor resource directory {string}")]
async fn given_node_has_example_wasm_processor_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    guest: String,
    placeholder: String,
) {
    place_wasm_processor_fixture_with_layout(
        world,
        &node_id,
        &placeholder,
        &guest,
        WasmProcessorFixtureLayout::ExampleRoot,
    )
    .await;
}

enum WasmProcessorFixtureLayout {
    FixtureProcessorFile,
    ExampleRoot,
}

async fn place_wasm_processor_fixture(
    world: &mut ScenarioWorld,
    node_id: &str,
    placeholder: &str,
    guest: &str,
) {
    place_wasm_processor_fixture_with_layout(
        world,
        node_id,
        placeholder,
        guest,
        WasmProcessorFixtureLayout::FixtureProcessorFile,
    )
    .await;
}

async fn place_wasm_processor_fixture_with_layout(
    world: &mut ScenarioWorld,
    node_id: &str,
    placeholder: &str,
    guest: &str,
    layout: WasmProcessorFixtureLayout,
) {
    let base_dir = world
        .cluster()
        .node_base_dir(node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(placeholder);
    if tokio::fs::try_exists(&resource_dir)
        .await
        .expect("fixture directory existence check should succeed")
    {
        tokio::fs::remove_dir_all(&resource_dir)
            .await
            .expect("old fixture directory should be removed");
    }
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let (source_path, artifact_file) = match guest {
        "rust" => (
            repo_root
                .join("examples")
                .join("wasm-processors")
                .join("rust-guest")
                .join("target")
                .join("wasm32-unknown-unknown")
                .join("release")
                .join("nervix_wasm_processor_rust_guest.wasm"),
            "nervix_wasm_processor_rust_guest.wasm",
        ),
        "go" => (
            repo_root
                .join("examples")
                .join("wasm-processors")
                .join("go-guest")
                .join("nervix_wasm_processor_go_guest.wasm"),
            "nervix_wasm_processor_go_guest.wasm",
        ),
        other => panic!("unsupported WASM processor fixture guest '{other}'"),
    };
    let destination_path = match layout {
        WasmProcessorFixtureLayout::FixtureProcessorFile => {
            tokio::fs::create_dir_all(resource_dir.join("processors"))
                .await
                .expect("fixture processor directory should be created");
            resource_dir.join("processors").join("filter_even.wasm")
        }
        WasmProcessorFixtureLayout::ExampleRoot => {
            tokio::fs::create_dir_all(&resource_dir)
                .await
                .expect("fixture resource directory should be created");
            resource_dir.join(artifact_file)
        }
    };
    tokio::fs::copy(&source_path, &destination_path)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to copy WASM processor fixture '{}' to '{}': {error}",
                source_path.display(),
                destination_path.display()
            )
        });

    world
        .placeholders
        .insert(placeholder.to_string(), resource_dir.display().to_string());
}

#[given(expr = "node {string} has invalid WASM processor fixture resource directory {string}")]
async fn given_node_has_invalid_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        b"not a wasm module".to_vec(),
    )
    .await;
}

#[given(
    expr = "node {string} has malformed-output WASM processor fixture resource directory {string}"
)]
async fn given_node_has_malformed_output_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        malformed_output_wasm_fixture().to_vec(),
    )
    .await;
}

#[given(
    expr = "node {string} has a WASM fixture returning an uninitialized column to relay {string} \
            in resource directory {string}"
)]
async fn given_node_has_uninitialized_output_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    output_relay: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        uninitialized_output_wasm_fixture(&output_relay),
    )
    .await;
}

#[given(expr = "node {string} has trapping WASM processor fixture resource directory {string}")]
async fn given_node_has_trapping_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        trapping_wasm_fixture().to_vec(),
    )
    .await;
}

async fn place_generated_wasm_processor_fixture(
    world: &mut ScenarioWorld,
    node_id: &str,
    placeholder: &str,
    wasm: Vec<u8>,
) {
    let base_dir = world
        .cluster()
        .node_base_dir(node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(placeholder);
    if tokio::fs::try_exists(&resource_dir)
        .await
        .expect("fixture directory existence check should succeed")
    {
        tokio::fs::remove_dir_all(&resource_dir)
            .await
            .expect("old fixture directory should be removed");
    }
    tokio::fs::create_dir_all(resource_dir.join("processors"))
        .await
        .expect("fixture processor directory should be created");
    tokio::fs::write(
        resource_dir.join("processors").join("filter_even.wasm"),
        wasm,
    )
    .await
    .expect("generated WASM fixture should be written");
    world
        .placeholders
        .insert(placeholder.to_string(), resource_dir.display().to_string());
}

fn malformed_output_wasm_fixture() -> &'static [u8] {
    br#"(module
      (import "env" "nervix_domain_time_nanos" (func $domain_time (result i64)))
      (import "env" "nervix_timeout_after_nanos" (func $timeout (param i64) (result i64)))
      (memory (export "memory") 1)
      (global $emitted (mut i32) (i32.const 0))
      (data (i32.const 0) "\01")
      (func (export "nervix_buffer_ptr") (result i32) (i32.const 0))
      (func (export "nervix_buffer_len") (result i32) (i32.const 1))
      (func (export "nervix_buffer_capacity") (result i32) (i32.const 65536))
      (func (export "nervix_alloc") (param i32) (result i32) (i32.const 0))
      (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
      (func (export "nervix_current_domain_time_nanos") (result i64) call $domain_time)
      (func (export "nervix_process_batch") (param i32) (result i32)
        i32.const 1
        global.set $emitted
        i32.const 0)
      (func (export "nervix_on_timeout") (param i64) (result i32) (i32.const 0))
      (func (export "nervix_read_emit") (result i32)
        global.get $emitted
        if (result i32)
          i32.const 0
          global.set $emitted
          i32.const 1
        else
          i32.const 0
        end)
      (func (export "nervix_dump_state") (result i32) (i32.const 0))
      (func (export "nervix_load_state") (param i32 i32) (result i32) (i32.const 0))
      (func (export "nervix_reset_state") (result i32) (i32.const 0))
    )"#
}

fn uninitialized_output_wasm_fixture(output_relay: &str) -> Vec<u8> {
    let encoded = WasmEnvelope::output(
        Vec::new(),
        vec![WasmRoutedOutput::new(
            output_relay,
            vec![WasmOutputColumnRef::uninitialized()],
            WasmAckSidecar {
                rows: vec![WasmOutputRow::default()],
                ..WasmAckSidecar::default()
            },
        )],
    )
    .encode()
    .expect("uninitialized WASM output fixture must encode");
    let encoded_wat = encoded
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let encoded_len = encoded.len();

    format!(
        r#"(module
          (memory (export "memory") 2)
          (global $emitted (mut i32) (i32.const 0))
          (data (i32.const 32768) "{encoded_wat}")
          (func (export "nervix_buffer_ptr") (result i32) (i32.const 32768))
          (func (export "nervix_buffer_len") (result i32) (i32.const {encoded_len}))
          (func (export "nervix_buffer_capacity") (result i32) (i32.const 131072))
          (func (export "nervix_alloc") (param i32) (result i32) (i32.const 0))
          (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
          (func (export "nervix_current_domain_time_nanos") (result i64) (i64.const 0))
          (func (export "nervix_process_batch") (param i32) (result i32)
            i32.const 1
            global.set $emitted
            i32.const 0)
          (func (export "nervix_on_timeout") (param i64) (result i32) (i32.const 0))
          (func (export "nervix_read_emit") (result i32)
            global.get $emitted
            if (result i32)
              i32.const 0
              global.set $emitted
              i32.const {encoded_len}
            else
              i32.const 0
            end)
          (func (export "nervix_dump_state") (result i32) (i32.const 0))
          (func (export "nervix_load_state") (param i32 i32) (result i32) (i32.const 0))
          (func (export "nervix_reset_state") (result i32)
            i32.const 0
            global.set $emitted
            i32.const 0)
        )"#
    )
    .into_bytes()
}

fn trapping_wasm_fixture() -> &'static [u8] {
    br#"(module
      (import "env" "nervix_domain_time_nanos" (func $domain_time (result i64)))
      (import "env" "nervix_timeout_after_nanos" (func $timeout (param i64) (result i64)))
      (memory (export "memory") 1)
      (func (export "nervix_buffer_ptr") (result i32) (i32.const 0))
      (func (export "nervix_buffer_len") (result i32) (i32.const 0))
      (func (export "nervix_buffer_capacity") (result i32) (i32.const 65536))
      (func (export "nervix_alloc") (param i32) (result i32) (i32.const 0))
      (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
      (func (export "nervix_current_domain_time_nanos") (result i64) call $domain_time)
      (func (export "nervix_process_batch") (param i32) (result i32)
        unreachable)
      (func (export "nervix_on_timeout") (param i64) (result i32) (i32.const 0))
      (func (export "nervix_read_emit") (result i32) (i32.const 0))
      (func (export "nervix_dump_state") (result i32) (i32.const 0))
      (func (export "nervix_load_state") (param i32 i32) (result i32) (i32.const 0))
      (func (export "nervix_reset_state") (result i32) (i32.const 0))
    )"#
}

fn ensure_onnx_runtime_loaded() {
    let result = ONNX_RUNTIME_INIT.get_or_init(|| {
        let dylib_path = resolve_onnxruntime_dylib()?;
        ort::init_from(&dylib_path)
            .map_err(|error| {
                format!(
                    "failed to initialize ONNX Runtime from '{}': {error}",
                    dylib_path.display()
                )
            })?
            .commit();
        Ok(())
    });

    if let Err(error) = result {
        panic!("{error}");
    }
}

fn resolve_onnxruntime_dylib() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("ORT_DYLIB_PATH").map(PathBuf::from) {
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!(
            "ORT_DYLIB_PATH points to missing ONNX Runtime library '{}'",
            path.display()
        ));
    }

    Err(
        "ORT_DYLIB_PATH must be set before running ONNX inferencer scenarios; use `just test` or \
         `just test-scenarios --input tests/features/runtime/inferencer.feature --concurrency 1`"
            .to_string(),
    )
}

#[given(expr = "node {string} has TLS resource directory {string} for hosts {string}")]
async fn given_node_has_tls_resource_directory_for_hosts(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
    hosts: String,
) {
    let hosts = expand_placeholders(world, &hosts)
        .split(',')
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    assert!(
        !hosts.is_empty(),
        "TLS resource directory requires at least one hostname"
    );

    let base_dir = world
        .cluster()
        .node_base_dir(&node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(&placeholder);
    if resource_dir.exists() {
        std::fs::remove_dir_all(&resource_dir).expect("old fixture directory should be removed");
    }
    std::fs::create_dir_all(&resource_dir).expect("fixture directory should be created");

    let ca_key = KeyPair::generate().expect("ca key should generate");
    let mut ca_params = CertificateParams::new(Vec::new()).expect("empty CA SAN must be valid");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "nervix test ca");
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .expect("ca certificate should generate");

    let leaf_key = KeyPair::generate().expect("leaf key should generate");
    let mut leaf_params =
        CertificateParams::new(hosts.clone()).expect("leaf SAN names should be valid");
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, hosts[0].clone());
    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &ca_cert, &ca_key)
        .expect("leaf certificate should generate");

    std::fs::write(resource_dir.join("ca.crt"), ca_cert.pem()).expect("ca cert should be written");
    std::fs::write(resource_dir.join("tls.crt"), leaf_cert.pem())
        .expect("leaf cert should be written");
    std::fs::write(resource_dir.join("tls.key"), leaf_key.serialize_pem())
        .expect("leaf key should be written");

    world
        .placeholders
        .insert(placeholder, resource_dir.display().to_string());
}

#[given(expr = "node {string} has dev TLS resource directory {string}")]
async fn given_node_has_dev_tls_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    let status = std::process::Command::new("bash")
        .arg("scripts/generate_dev_tls.sh")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("dev TLS asset generation command should start");
    assert!(
        status.success(),
        "dev TLS asset generation command failed with status {status}"
    );

    let base_dir = world
        .cluster()
        .node_base_dir(&node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(&placeholder);
    if resource_dir.exists() {
        std::fs::remove_dir_all(&resource_dir).expect("old fixture directory should be removed");
    }
    std::fs::create_dir_all(&resource_dir).expect("fixture directory should be created");

    for filename in ["ca.pem", "node.pem", "node-key.pem"] {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tls/dev")
            .join(filename);
        let destination = resource_dir.join(filename);
        std::fs::copy(&source, &destination)
            .unwrap_or_else(|error| panic!("failed to copy dev TLS asset '{filename}': {error}"));
    }

    world
        .placeholders
        .insert(placeholder, resource_dir.display().to_string());
}

#[given(expr = "node {string} is stopped")]
#[when(expr = "node {string} is stopped")]
async fn when_node_is_stopped(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster_mut()
        .stop_node(&node_id)
        .await
        .expect("failed to stop node");
}

#[when(expr = "node {string} is gracefully stopped")]
async fn when_node_is_gracefully_stopped(world: &mut ScenarioWorld, node_id: String) {
    assert!(
        world.cluster_config.graceful_shutdown_drain,
        "graceful shutdown drain must be configured before cluster startup"
    );
    let started = Instant::now();
    world
        .cluster_mut()
        .stop_node(&node_id)
        .await
        .expect("failed to gracefully stop node");
    world.last_cluster_operation_elapsed = Some(started.elapsed());
}

#[when("all nodes are stopped")]
async fn when_all_nodes_are_stopped(world: &mut ScenarioWorld) {
    world
        .cluster_mut()
        .shutdown()
        .await
        .expect("failed to stop all nodes");
}

#[when("all nodes are gracefully stopped")]
async fn when_all_nodes_are_gracefully_stopped(world: &mut ScenarioWorld) {
    assert!(
        world.cluster_config.graceful_shutdown_drain,
        "graceful shutdown drain must be configured before cluster startup"
    );
    let started = Instant::now();
    world
        .cluster_mut()
        .shutdown()
        .await
        .expect("failed to gracefully stop all nodes");
    world.last_cluster_operation_elapsed = Some(started.elapsed());
}

#[given(expr = "node {string} is started")]
#[when(expr = "node {string} is started")]
async fn when_node_is_started(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster_mut()
        .start_node(&node_id)
        .await
        .expect("failed to start node");
}

#[when(expr = "leadership is transferred from node {string} to node {string}")]
async fn when_leadership_is_transferred_from_node_to_node(
    world: &mut ScenarioWorld,
    from_node_id: String,
    to_node_id: String,
) {
    let from_node_id = expand_placeholders(world, &from_node_id);
    let to_node_id = expand_placeholders(world, &to_node_id);
    world
        .cluster()
        .transfer_leadership(&from_node_id, &to_node_id);
}

#[when("the cluster is restarted")]
async fn when_the_cluster_is_restarted(world: &mut ScenarioWorld) {
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    world.last_subscription_payload = None;
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world
        .cluster_mut()
        .restart()
        .await
        .expect("failed to restart cluster");
}

#[then(expr = "the last cluster operation completes within {string}")]
async fn then_last_cluster_operation_completes_within(world: &mut ScenarioWorld, duration: String) {
    let max_duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let elapsed = world
        .last_cluster_operation_elapsed
        .expect("a timed cluster operation must run before assertion");
    assert!(
        elapsed <= max_duration,
        "expected cluster operation to complete within {:?}, took {:?}",
        max_duration,
        elapsed
    );
}

#[then(expr = "the last authentication attempts take at least {string}")]
async fn then_last_authentication_attempts_take_at_least(
    world: &mut ScenarioWorld,
    duration: String,
) {
    let min_duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let elapsed = world
        .last_auth_attempts_elapsed
        .expect("an authentication attempt step must run first");
    assert!(
        elapsed >= min_duration,
        "expected authentication attempts to take at least {:?}, took {:?}",
        min_duration,
        elapsed
    );
}

#[when(expr = "emitter {string} enters fault mode")]
async fn when_emitter_enters_fault_mode(world: &mut ScenarioWorld, emitter: String) {
    let emitter = expand_placeholders(world, &emitter);
    world.cluster().fail_emitter_on_all_nodes(&emitter);
}

#[when(expr = "emitter {string} enters stall mode")]
async fn when_emitter_enters_stall_mode(world: &mut ScenarioWorld, emitter: String) {
    let emitter = expand_placeholders(world, &emitter);
    world.cluster().stall_emitter_on_all_nodes(&emitter);
}

#[when(expr = "emitter {string} leaves fault mode")]
#[then(expr = "emitter {string} leaves fault mode")]
#[when(expr = "emitter {string} leaves stall mode")]
#[then(expr = "emitter {string} leaves stall mode")]
async fn when_emitter_leaves_fault_mode(world: &mut ScenarioWorld, emitter: String) {
    let emitter = expand_placeholders(world, &emitter);
    world.cluster().clear_emitter_fault_on_all_nodes(&emitter);
}

#[when(expr = "ingestor {string} enters fault mode")]
async fn when_ingestor_enters_fault_mode(world: &mut ScenarioWorld, ingestor: String) {
    let ingestor = expand_placeholders(world, &ingestor);
    world.cluster().fail_ingestor_on_all_nodes(&ingestor);
}

#[when(expr = "ingestor {string} leaves fault mode")]
async fn when_ingestor_leaves_fault_mode(world: &mut ScenarioWorld, ingestor: String) {
    let ingestor = expand_placeholders(world, &ingestor);
    world.cluster().clear_ingestor_fault_on_all_nodes(&ingestor);
}

#[given(expr = "node {string} eventually reports leader {string}")]
#[then(expr = "node {string} eventually reports leader {string}")]
async fn then_node_eventually_reports_leader(
    world: &mut ScenarioWorld,
    node_id: String,
    leader_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let leader_id = expand_placeholders(world, &leader_id);
    world
        .cluster()
        .wait_for_leader(&node_id, Some(&leader_id))
        .await
        .expect("leader did not converge");
}

#[then(expr = "node {string} eventually reports a leader other than {string}")]
async fn then_node_eventually_reports_other_leader(
    world: &mut ScenarioWorld,
    node_id: String,
    old_leader_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let old_leader_id = expand_placeholders(world, &old_leader_id);
    world
        .cluster()
        .wait_for_leader_not(&node_id, &old_leader_id)
        .await
        .expect("new leader was not elected");
}

#[then(expr = "node {string} eventually observes a stable leader")]
async fn then_node_eventually_observes_stable_leader(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster()
        .wait_for_any_leader(&node_id)
        .await
        .expect("leader did not appear");
}

#[then(expr = "node {string} eventually reports raft state {string}")]
async fn then_node_eventually_reports_raft_state(
    world: &mut ScenarioWorld,
    node_id: String,
    expected_state: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster()
        .wait_for_raft_state(&node_id, &expected_state)
        .await
        .expect("raft state did not converge");
}

#[then(expr = "node {string} eventually reports raft voters {string}")]
async fn then_node_eventually_reports_voters(
    world: &mut ScenarioWorld,
    node_id: String,
    expected: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let voters = expected
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    world
        .cluster()
        .wait_for_voters(&node_id, &voters)
        .await
        .expect("voter set did not converge");
}

#[then(expr = "Kafka consumer group {string} eventually has {int} consumers")]
async fn then_kafka_consumer_group_eventually_has_consumers(
    world: &mut ScenarioWorld,
    group: String,
    expected: usize,
) {
    let group = expand_placeholders(world, &group);
    world
        .cluster()
        .wait_for_kafka_consumer_group_members(&group, expected)
        .await
        .expect("kafka consumer group did not reach expected member count");
}

#[then(expr = "RabbitMQ queue {string} eventually has {int} consumers")]
async fn then_rabbitmq_queue_eventually_has_consumers(
    world: &mut ScenarioWorld,
    queue: String,
    expected: usize,
) {
    let queue = expand_placeholders(world, &queue);
    world
        .cluster()
        .wait_for_rabbitmq_queue_consumers(&queue, expected)
        .await
        .expect("rabbitmq queue did not reach expected consumer count");
}

#[then(expr = "Redis channel {string} eventually has {int} subscribers")]
async fn then_redis_channel_eventually_has_subscribers(
    world: &mut ScenarioWorld,
    channel: String,
    expected: usize,
) {
    let channel = expand_placeholders(world, &channel);
    world
        .cluster()
        .wait_for_redis_channel_subscribers(&channel, expected)
        .await
        .expect("redis channel did not reach expected subscriber count");
}

fn docstring(step: &Step) -> &str {
    step.docstring
        .as_deref()
        .expect("step docstring is required")
}

fn expand_placeholders(world: &ScenarioWorld, input: &str) -> String {
    let mut output = input
        .replace("{{test_id}}", &world.test_id)
        .replace("{{domain}}", &world.domain)
        .replace("{{zeromq_ingest_addr}}", &world.zeromq_ingest_addr)
        .replace("{{zeromq_emit_addr}}", &world.zeromq_emit_addr);
    for (key, value) in &world.placeholders {
        output = output.replace(&format!("{{{{{key}}}}}"), value);
    }
    output
}

fn encode_http_payload_for_codec(
    wire_format: &str,
    payload: &str,
    avro_field_order: &[String],
    avro_optional_fields: &BTreeSet<String>,
) -> Vec<u8> {
    let json_value = serde_json::from_str::<serde_json::Value>(payload).unwrap_or_else(|error| {
        panic!("http payload must be valid JSON for {wire_format}: {error}")
    });

    match wire_format.to_ascii_uppercase().as_str() {
        "JSON" => serde_json::to_vec(&json_value)
            .unwrap_or_else(|error| panic!("failed to encode JSON payload: {error}")),
        "AVRO" => encode_avro_http_payload(&json_value, avro_field_order, avro_optional_fields),
        "CBOR" => {
            let mut encoded = Vec::new();
            ciborium::into_writer(&json_value, &mut encoded)
                .unwrap_or_else(|error| panic!("failed to encode CBOR payload: {error}"));
            encoded
        }
        other => panic!("unsupported codec wire format '{other}'"),
    }
}

fn encode_avro_http_payload(
    json_value: &serde_json::Value,
    field_order: &[String],
    optional_fields: &BTreeSet<String>,
) -> Vec<u8> {
    use apache_avro::{Schema as AvroSchema, to_avro_datum, types::Value as AvroValue};

    let serde_json::Value::Object(object) = json_value else {
        panic!("avro http payload must be a JSON object");
    };

    let mut schema_fields = Vec::with_capacity(object.len());
    let mut value_fields = Vec::with_capacity(object.len());
    let mut ordered_fields = Vec::new();
    for name in field_order {
        if let Some(value) = object.get(name) {
            ordered_fields.push((name.as_str(), value));
        }
    }
    for (name, value) in object {
        if !field_order.iter().any(|field| field == name) {
            ordered_fields.push((name.as_str(), value));
        }
    }

    for (name, value) in ordered_fields {
        let (schema_ty, avro_value) = avro_field_from_json(value);
        let schema_ty = if schema_ty.starts_with('{') {
            schema_ty
        } else {
            format!(r#""{schema_ty}""#)
        };
        let (schema_ty, avro_value) = if optional_fields.contains(name) {
            (
                format!(r#"["null",{schema_ty}]"#),
                AvroValue::Union(1, Box::new(avro_value)),
            )
        } else {
            (schema_ty, avro_value)
        };
        schema_fields.push(format!(r#"{{"name":"{name}","type":{schema_ty}}}"#));
        value_fields.push((name.to_string(), avro_value));
    }

    let schema_json = format!(
        r#"{{"type":"record","name":"HttpPayload","fields":[{}]}}"#,
        schema_fields.join(",")
    );
    let schema = AvroSchema::parse_str(&schema_json)
        .unwrap_or_else(|error| panic!("failed to build avro payload schema: {error}"));
    to_avro_datum(&schema, AvroValue::Record(value_fields))
        .unwrap_or_else(|error| panic!("failed to encode avro payload: {error}"))
}

fn avro_field_from_json(value: &serde_json::Value) -> (String, apache_avro::types::Value) {
    use apache_avro::types::Value as AvroValue;

    match value {
        serde_json::Value::Bool(v) => ("boolean".to_string(), AvroValue::Boolean(*v)),
        serde_json::Value::Number(v) => {
            if let Some(integer) = v.as_i64() {
                ("long".to_string(), AvroValue::Long(integer))
            } else if let Some(float) = v.as_f64() {
                ("double".to_string(), AvroValue::Double(float))
            } else {
                panic!("unsupported avro numeric value {v}");
            }
        }
        serde_json::Value::String(v) => ("string".to_string(), AvroValue::String(v.clone())),
        serde_json::Value::Null => ("null".to_string(), AvroValue::Null),
        serde_json::Value::Array(values) => {
            let Some(first) = values.first() else {
                panic!("avro http payload arrays must not be empty");
            };
            let (item_schema, _) = avro_array_item_from_json(first);
            let avro_values = values
                .iter()
                .map(|value| {
                    let (schema, avro_value) = avro_array_item_from_json(value);
                    assert_eq!(
                        schema, item_schema,
                        "avro http payload arrays must have homogeneous item types"
                    );
                    avro_value
                })
                .collect::<Vec<_>>();
            (
                format!(r#"{{"type":"array","items":"{item_schema}"}}"#),
                AvroValue::Array(avro_values),
            )
        }
        serde_json::Value::Object(_) => {
            panic!("avro http payload only supports flat scalar or array fields")
        }
    }
}

fn avro_array_item_from_json(value: &serde_json::Value) -> (String, apache_avro::types::Value) {
    use apache_avro::types::Value as AvroValue;

    match value {
        serde_json::Value::Number(v) if v.as_f64().is_some() => (
            "float".to_string(),
            AvroValue::Float(v.as_f64().expect("checked above") as f32),
        ),
        other => avro_field_from_json(other),
    }
}

fn http_content_type_for_codec(wire_format: &str) -> &'static str {
    match wire_format.to_ascii_uppercase().as_str() {
        "JSON" => "application/json",
        "AVRO" => "application/avro",
        "CBOR" => "application/cbor",
        other => panic!("unsupported codec wire format '{other}'"),
    }
}

fn jaq_native_payload_fixture(fixture: &str) -> (Vec<u8>, &'static str) {
    match fixture {
        "json_wrapped_notification" => (
            br#"{"payload":{"user_id":42,"payload":"aligned"}}"#.to_vec(),
            "application/json",
        ),
        "yaml_wrapped_notification" => (
            b"payload:\n  user_id: 42\n  payload: aligned\n".to_vec(),
            "application/yaml",
        ),
        "toml_wrapped_notification" => (
            b"[payload]\nuser_id = 42\npayload = \"aligned\"\n".to_vec(),
            "application/toml",
        ),
        "xml_wrapped_notification" => (
            b"<notification><user_id>42</user_id><payload>aligned</payload></notification>"
                .to_vec(),
            "application/xml",
        ),
        "cbor_wrapped_notification" => {
            let value = serde_json::json!({
                "payload": {
                    "user_id": 42,
                    "payload": "aligned"
                }
            });
            let mut encoded = Vec::new();
            ciborium::into_writer(&value, &mut encoded)
                .unwrap_or_else(|error| panic!("failed to encode CBOR fixture: {error}"));
            (encoded, "application/cbor")
        }
        other => panic!("unknown JAQ native payload fixture '{other}'"),
    }
}

fn protobuf_payload_fixture(fixture: &str) -> (Vec<u8>, &'static str) {
    match fixture {
        "notification" => (
            vec![
                0x08, 42, 0x12, 4, b'a', b'c', b'm', b'e', 0x1a, 7, b'a', b'l', b'i', b'g', b'n',
                b'e', b'd',
            ],
            "application/x-protobuf",
        ),
        other => panic!("unknown protobuf payload fixture '{other}'"),
    }
}

fn resource_directory_path(world: &ScenarioWorld, placeholder: &str) -> PathBuf {
    PathBuf::from(
        world
            .placeholders
            .get(placeholder)
            .unwrap_or_else(|| panic!("unknown resource directory placeholder '{placeholder}'")),
    )
}

fn resource_directory_ca_pem(world: &ScenarioWorld, placeholder: &str) -> String {
    std::fs::read_to_string(resource_directory_path(world, placeholder).join("ca.crt"))
        .unwrap_or_else(|error| {
            panic!("failed to read ca.crt from resource directory '{placeholder}': {error}")
        })
}

fn nspl_statements(input: &str) -> Vec<String> {
    input
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(|statement| format!("{statement};"))
        .collect()
}

fn payload_matches_expected(actual: &str, expected: &str) -> bool {
    let expected = expected.trim();
    if let Ok(expected_json) = serde_json::from_str::<serde_json::Value>(expected)
        && let Ok(actual_json) = serde_json::from_str::<serde_json::Value>(actual.trim())
    {
        return actual_json == expected_json;
    }
    actual.contains(expected)
}

fn requires_persistent_session(commands: &str) -> bool {
    nspl_statements(commands).iter().any(|statement| {
        let normalized = statement.trim().to_ascii_uppercase();
        normalized.starts_with("CREATE SUBSCRIPTION ")
            || normalized == "BEGIN;"
            || normalized == "COMMIT;"
            || normalized == "REVERT;"
    })
}

fn command_updates_subscription_state(current: bool, command: &str) -> bool {
    let normalized = command.trim_start().to_ascii_uppercase();
    if normalized.starts_with("CREATE SUBSCRIPTION ") {
        return true;
    }
    if normalized.starts_with("DELETE SUBSCRIPTION ") {
        return false;
    }
    current
}

fn commands_update_subscription_state(current: bool, commands: &str) -> bool {
    nspl_statements(commands)
        .into_iter()
        .fold(current, |state, command| {
            command_updates_subscription_state(state, &command)
        })
}

fn record_avro_wire_optional_fields(world: &mut ScenarioWorld, commands: &str) {
    for statement in nspl_statements(commands) {
        let normalized = statement.trim_start().to_ascii_uppercase();
        if !normalized.starts_with("CREATE STRICT WIRE AVRO SCHEMA ") {
            continue;
        }
        let Some((_, fields)) = statement.split_once('(') else {
            continue;
        };
        let Some((fields, _)) = fields.rsplit_once(')') else {
            continue;
        };
        for field in fields.split(',') {
            let field = field.trim();
            if let Some(name) = field.split_whitespace().next() {
                world.avro_http_field_order.push(name.to_string());
                if field.to_ascii_uppercase().contains(" OPTIONAL") {
                    world.avro_http_optional_fields.insert(name.to_string());
                }
            }
        }
    }
}

fn record_mqtt_ingestors(world: &mut ScenarioWorld, commands: &str) {
    let Ok(parsed) = nervix_nspl::client_statement::parse_client_statement_sources(commands) else {
        return;
    };
    for statement in parsed {
        let nervix_nspl::client_statement::ClientStatement::Server(
            nervix_models::Statement::Create(create),
        ) = statement.statement
        else {
            continue;
        };
        let nervix_models::Model::Ingestor(ingestor) = *create.body else {
            continue;
        };
        let nervix_models::IngestSource::Mqtt { .. } = ingestor.source else {
            continue;
        };
        world
            .mqtt_ingestors_by_domain
            .entry(world.domain.clone())
            .or_default()
            .insert(ingestor.name.as_str().to_string());
    }
}

async fn execute_nspl_commands_on_node(
    world: &mut ScenarioWorld,
    node_id: &str,
    commands: &str,
) -> Result<TestSession, String> {
    record_avro_wire_optional_fields(world, commands);
    append_cucumber_log_line(&format!(
        "nspl commands on node {node_id}: {}",
        commands.replace('\n', "\\n")
    ));
    if !requires_persistent_session(commands) {
        for command in nspl_statements(commands) {
            append_cucumber_log_line(&format!("nspl command on node {node_id}: {command}"));
            let output = world
                .cluster()
                .run_command(node_id, &world.domain, &command)
                .await
                .map_err(|error| error.to_string())?;
            world.last_command_output = Some(output);
        }
        record_mqtt_ingestors(world, commands);
        return world
            .cluster()
            .open_session(node_id, &world.domain)
            .await
            .map_err(|error| error.to_string());
    }

    let mut session = world
        .cluster()
        .open_session(node_id, &world.domain)
        .await
        .map_err(|error| error.to_string())?;

    for command in nspl_statements(commands) {
        append_cucumber_log_line(&format!("nspl command on session {node_id}: {command}"));
        match session.run_command(&command).await {
            Ok(output) => world.last_command_output = Some(output),
            Err(error) => return Err(error.to_string()),
        }
    }

    record_mqtt_ingestors(world, commands);
    Ok(session)
}

#[given(expr = "the active domain is {string}")]
async fn given_the_active_domain_is(world: &mut ScenarioWorld, raw_domain: String) {
    world.domain = expand_placeholders(world, &raw_domain);
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    world.last_command_error = None;
    world.last_command_output = None;
}

async fn run_nspl_commands_on_node(
    world: &ScenarioWorld,
    node_id: &str,
    commands: &str,
) -> Result<String, String> {
    let mut last_output = String::new();
    for command in nspl_statements(commands) {
        last_output = world
            .cluster()
            .run_command(node_id, &world.domain, &command)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(last_output)
}

fn commands_are_retry_safe_session_ops(commands: &str) -> bool {
    nspl_statements(commands).into_iter().all(|command| {
        let normalized = command.trim().to_ascii_uppercase();
        normalized.starts_with("CREATE SUBSCRIPTION ")
            || normalized.starts_with("DELETE SUBSCRIPTION ")
            || normalized == "DESCRIBE DOMAIN;"
            || normalized.starts_with("DESCRIBE ENDPOINT ")
            || normalized.starts_with("DESCRIBE RESOURCE ")
            || normalized.starts_with("DESCRIBE RELAY ")
            || normalized.starts_with("DESCRIBE DEDUPLICATOR ")
            || normalized.starts_with("DESCRIBE REINGESTOR ")
            || normalized.starts_with("DESCRIBE EMITTER ")
            || normalized.starts_with("DESCRIBE WASM PROCESSOR ")
            || normalized.starts_with("DESCRIBE WINDOW PROCESSOR ")
    })
}

async fn run_nspl_commands_on_active_session(
    world: &mut ScenarioWorld,
    commands: &str,
) -> Result<(), String> {
    record_avro_wire_optional_fields(world, commands);
    append_cucumber_log_line(&format!(
        "nspl commands on active session: {}",
        commands.replace('\n', "\\n")
    ));
    let session = world
        .active_session
        .as_mut()
        .expect("an active session must exist");
    for command in nspl_statements(commands) {
        append_cucumber_log_line(&format!("nspl command on active session: {command}"));
        match session.run_command(&command).await {
            Ok(output) => world.last_command_output = Some(output),
            Err(error) => return Err(error.to_string()),
        }
        world.active_session_has_subscription =
            command_updates_subscription_state(world.active_session_has_subscription, &command);
    }
    record_mqtt_ingestors(world, commands);
    Ok(())
}

#[when(expr = "the active session targets domain {string}")]
async fn when_the_active_session_targets_domain(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world
        .active_session
        .as_mut()
        .expect("an active session must exist")
        .set_domain(domain);
}

#[when("these NSPL commands are executed on the active session")]
async fn when_these_nspl_commands_are_executed_on_the_active_session(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let commands = expand_placeholders(world, docstring(step));
    run_nspl_commands_on_active_session(world, &commands)
        .await
        .expect("failed to execute NSPL commands on active session");
}

#[when("these NSPL commands fail on the active session")]
async fn when_these_nspl_commands_fail_on_the_active_session(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let commands = expand_placeholders(world, docstring(step));
    match run_nspl_commands_on_active_session(world, &commands).await {
        Ok(()) => panic!("expected NSPL commands to fail on active session"),
        Err(error) => world.last_command_error = Some(error),
    }
}

#[when("a new session executes these NSPL commands")]
async fn when_a_new_session_executes_these_nspl_commands(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    let session = execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .expect("failed to execute NSPL commands on a new session");
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

async fn current_leader_node(world: &ScenarioWorld) -> String {
    world
        .cluster()
        .wait_for_consistent_leader_on_all_nodes()
        .await
        .expect("cluster leader did not appear")
}

async fn wait_for_mqtt_ingestors_ready(world: &mut ScenarioWorld) {
    let ingestors = world
        .mqtt_ingestors_by_domain
        .get(&world.domain)
        .cloned()
        .unwrap_or_default();
    if ingestors.is_empty() {
        return;
    }
    let leader = current_leader_node(world).await;
    for ingestor in ingestors {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            tokio::task::consume_budget().await;
            let output = run_nspl_commands_on_node(
                world,
                &leader,
                &format!("DESCRIBE INGESTOR {ingestor};"),
            )
            .await
            .unwrap_or_else(|error| error);
            if output.contains("ready: true") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for MQTT ingestor '{ingestor}' to become ready. last output: {}",
                output
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn open_web_console_page(world: &mut ScenarioWorld, url: &str) -> Result<(), String> {
    let playwright = Playwright::launch()
        .await
        .map_err(|error| error.to_string())?;
    let browser = playwright
        .chromium()
        .launch_with_options(chromium_launch_options())
        .await
        .map_err(|error| error.to_string())?;
    let context = browser
        .new_context()
        .await
        .map_err(|error| error.to_string())?;
    let page = context
        .new_page()
        .await
        .map_err(|error| error.to_string())?;
    page.set_default_timeout(10_000.0).await;
    page.goto(url, None)
        .await
        .map_err(|error| error.to_string())?;
    world.browser_page = Some(page);
    world.browser_context = Some(context);
    world.browser = Some(browser);
    world.playwright = Some(playwright);
    Ok(())
}

async fn close_browser(world: &mut ScenarioWorld) {
    world.browser_page = None;
    if let Some(context) = world.browser_context.take() {
        let _ = context.close().await;
    }
    if let Some(browser) = world.browser.take() {
        let _ = browser.close().await;
    }
    world.playwright = None;
}

fn chromium_launch_options() -> LaunchOptions {
    let mut options = LaunchOptions::default().headless(true).args(vec![
        "--no-sandbox".to_string(),
        "--disable-dev-shm-usage".to_string(),
    ]);
    if let Some(path) = chrome_executable_path() {
        options = options.executable_path(path.to_string_lossy().into_owned());
    }
    options
}

fn chrome_executable_path() -> Option<&'static Path> {
    [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ]
    .into_iter()
    .map(Path::new)
    .find(|path| path.exists())
}

#[when("these NSPL commands are executed")]
async fn when_these_nspl_commands_are_executed(world: &mut ScenarioWorld, #[step] step: &Step) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    if world.active_session_has_subscription
        && world.active_session.is_some()
        && world.active_session_node.as_deref() == Some(leader.as_str())
    {
        run_nspl_commands_on_active_session(world, &commands)
            .await
            .expect("failed to execute NSPL command on active session");
        return;
    }
    let session = execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .expect("failed to execute NSPL setup command");
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[when("these NSPL commands are executed through the client on a follower node")]
async fn when_these_nspl_commands_are_executed_through_the_client_on_a_follower_node(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    let follower = world
        .cluster()
        .any_follower_node("node-1")
        .await
        .expect("failed to resolve follower node");
    let grpc_uri = world
        .cluster()
        .grpc_uri(&follower)
        .expect("failed to resolve follower gRPC URI");
    let client = Client::connect_with_options(
        &grpc_uri,
        world.domain.clone(),
        client_connect_options(&grpc_uri).expect("failed to build client tls options"),
    )
    .await
    .expect("failed to connect follower client");
    let commands = expand_placeholders(world, docstring(step));
    for command in nspl_statements(&commands) {
        let outcome = client
            .execute(command.clone())
            .await
            .expect("client command should complete");
        assert!(
            outcome.success,
            "client command must succeed: {command}: {}",
            outcome.message
        );
        world.last_command_output = Some(outcome.message);
    }
}

#[when("these NSPL commands are executed through the client on the leader node")]
async fn when_these_nspl_commands_are_executed_through_the_client_on_the_leader_node(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let leader = current_leader_node(world).await;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&leader)
        .expect("failed to resolve leader gRPC URI");
    let client = Client::connect_with_options(
        &grpc_uri,
        world.domain.clone(),
        client_connect_options(&grpc_uri).expect("failed to build client tls options"),
    )
    .await
    .expect("failed to connect leader client");
    let commands = expand_placeholders(world, docstring(step));
    for command in nspl_statements(&commands) {
        let outcome = client
            .execute(command.clone())
            .await
            .expect("client command should complete");
        assert!(
            outcome.success,
            "client command must succeed: {command}: {}",
            outcome.message
        );
        world.last_command_output = Some(outcome.message);
    }
}

#[when(expr = "the client connects to the leader node with password {string}")]
async fn when_the_client_connects_to_the_leader_node_with_password(
    world: &mut ScenarioWorld,
    password: String,
) {
    connect_to_leader_with_credentials(world, TEST_AUTH_USERNAME.to_string(), password).await;
}

#[when(expr = "the client connects to the leader node as user {string} with password {string}")]
async fn when_the_client_connects_to_the_leader_node_as_user_with_password(
    world: &mut ScenarioWorld,
    username: String,
    password: String,
) {
    connect_to_leader_with_credentials(world, username, password).await;
}

#[when(
    expr = "the client attempts to connect to the leader node as user {string} with password \
            {string} {int} times"
)]
async fn when_the_client_attempts_to_connect_to_the_leader_node_as_user_with_password_times(
    world: &mut ScenarioWorld,
    username: String,
    password: String,
    attempts: usize,
) {
    assert!(attempts > 0, "auth attempt count must be positive");
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.last_auth_attempts_elapsed = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    let username = expand_placeholders(world, &username);
    let password = expand_placeholders(world, &password);
    let leader = current_leader_node(world).await;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&leader)
        .expect("failed to resolve leader gRPC URI");
    let started = Instant::now();
    for attempt in 1..=attempts {
        let mut options =
            client_connect_options(&grpc_uri).expect("failed to build client tls options");
        options.username = Some(username.clone());
        options.password = Some(password.clone());
        match Client::connect_with_options(&grpc_uri, world.domain.clone(), options).await {
            Ok(client) => match client.execute("SHOW CLUSTER STATUS;".to_string()).await {
                Ok(outcome) if outcome.success => {
                    panic!("auth attempt {attempt} unexpectedly succeeded");
                }
                Ok(outcome) => {
                    world.last_command_error = Some(outcome.message);
                }
                Err(error) => {
                    world.last_command_error = Some(error.to_string());
                }
            },
            Err(error) => {
                world.last_command_error = Some(error.to_string());
            }
        }
    }
    world.last_auth_attempts_elapsed = Some(started.elapsed());
}

async fn connect_to_leader_with_credentials(
    world: &mut ScenarioWorld,
    username: String,
    password: String,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    let username = expand_placeholders(world, &username);
    let password = expand_placeholders(world, &password);
    let leader = current_leader_node(world).await;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&leader)
        .expect("failed to resolve leader gRPC URI");
    let mut options =
        client_connect_options(&grpc_uri).expect("failed to build client tls options");
    options.username = Some(username);
    options.password = Some(password);
    match Client::connect_with_options(&grpc_uri, world.domain.clone(), options).await {
        Ok(client) => match client.execute("SHOW CLUSTER STATUS;".to_string()).await {
            Ok(outcome) if outcome.success => {
                world.last_command_output = Some(outcome.message);
            }
            Ok(outcome) => {
                world.last_command_error = Some(outcome.message);
            }
            Err(error) => {
                world.last_command_error = Some(error.to_string());
            }
        },
        Err(error) => {
            world.last_command_error = Some(error.to_string());
        }
    }
}

#[when(expr = "these NSPL commands are executed through the client on node {string}")]
async fn when_these_nspl_commands_are_executed_through_the_client_on_node(
    world: &mut ScenarioWorld,
    node_id: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    let node_id = expand_placeholders(world, &node_id);
    let grpc_uri = world
        .cluster()
        .grpc_uri(&node_id)
        .expect("failed to resolve node gRPC URI");
    let client = Client::connect_with_options(
        &grpc_uri,
        world.domain.clone(),
        client_connect_options(&grpc_uri).expect("failed to build client tls options"),
    )
    .await
    .expect("failed to connect node client");
    let commands = expand_placeholders(world, docstring(step));
    for command in nspl_statements(&commands) {
        let outcome = client
            .execute(command.clone())
            .await
            .expect("client command should complete");
        assert!(
            outcome.success,
            "client command must succeed: {command}: {}",
            outcome.message
        );
        world.last_command_output = Some(outcome.message);
    }
}

#[when(expr = "these NSPL commands fail through the client on node {string} with {string}")]
async fn when_these_nspl_commands_fail_through_the_client_on_node_with(
    world: &mut ScenarioWorld,
    node_id: String,
    expected_error: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&node_id)
        .expect("failed to resolve node gRPC URI");
    let client = Client::connect_with_options(
        &grpc_uri,
        world.domain.clone(),
        client_connect_options(&grpc_uri).expect("failed to build client tls options"),
    )
    .await
    .expect("failed to connect node client");
    let commands = expand_placeholders(world, docstring(step));
    for command in nspl_statements(&commands) {
        let outcome = client
            .execute(command.clone())
            .await
            .expect("client command should complete");
        assert!(
            !outcome.success,
            "client command must fail: {command}: {}",
            outcome.message
        );
        assert!(
            outcome.message.contains(&expected_error),
            "expected error containing {:?}, got: {}",
            expected_error,
            outcome.message
        );
        world.last_command_error = Some(outcome.message);
    }
}

#[when("these NSPL commands are executed on the leader node")]
async fn when_these_nspl_commands_are_executed_on_leader_node(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    let retry_safe = commands_are_retry_safe_session_ops(&commands);
    if world.active_session_has_subscription
        && world.active_session.is_some()
        && world.active_session_node.as_deref() == Some(leader.as_str())
    {
        if retry_safe {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                tokio::task::consume_budget().await;
                match run_nspl_commands_on_active_session(world, &commands).await {
                    Ok(()) => break,
                    Err(error) => {
                        assert!(
                            Instant::now() < deadline,
                            "failed to execute NSPL setup command on active session: {error:?}"
                        );
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        } else {
            run_nspl_commands_on_active_session(world, &commands)
                .await
                .expect("failed to execute NSPL setup command on active session");
        }
        return;
    }
    let session = if retry_safe {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::task::consume_budget().await;
            match execute_nspl_commands_on_node(world, &leader, &commands).await {
                Ok(session) => break session,
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "failed to execute NSPL setup command on leader: {error:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    } else {
        execute_nspl_commands_on_node(world, &leader, &commands)
            .await
            .expect("failed to execute NSPL setup command on leader")
    };
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[when("this NSPL command request is executed on the leader node")]
async fn when_this_nspl_command_request_is_executed_on_leader_node(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    match world
        .cluster()
        .run_command(&leader, &world.domain, &commands)
        .await
    {
        Ok(output) => world.last_command_output = Some(output),
        Err(error) => world.last_command_error = Some(error.to_string()),
    }
}

#[then(expr = "the current leader node is saved as placeholder {string}")]
async fn then_current_leader_node_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let leader = current_leader_node(world).await;
    world.placeholders.insert(placeholder, leader);
}

#[then(expr = "the leader reported by node {string} is saved as placeholder {string}")]
async fn then_leader_reported_by_node_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let leader = world
        .cluster()
        .current_leader(&node_id)
        .await
        .expect("failed to read node leader")
        .unwrap_or_else(|| panic!("node '{node_id}' does not report a leader"));
    world.placeholders.insert(placeholder, leader);
}

#[when(expr = "the web console is opened on node {string}")]
async fn when_web_console_is_opened_on_node(world: &mut ScenarioWorld, node_id: String) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    close_browser(world).await;
    let node_id = expand_placeholders(world, &node_id);
    let url = world
        .cluster()
        .web_console_url(&node_id)
        .expect("failed to resolve web console URL");
    open_web_console_page(world, &url)
        .await
        .expect("failed to open web console");
}

#[when("the web console is opened on the leader node")]
async fn when_web_console_is_opened_on_leader_node(world: &mut ScenarioWorld) {
    let leader = current_leader_node(world).await;
    when_web_console_is_opened_on_node(world, leader).await;
}

#[when(expr = "the web console is opened on the leader node with password {string}")]
async fn when_web_console_is_opened_on_leader_node_with_password(
    world: &mut ScenarioWorld,
    password: String,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    close_browser(world).await;
    let password = expand_placeholders(world, &password);
    let leader = current_leader_node(world).await;
    let url = world
        .cluster()
        .web_console_url_with_password(&leader, &password)
        .expect("failed to resolve web console URL");
    open_web_console_page(world, &url)
        .await
        .expect("failed to open web console");
}

#[when(expr = "the browser viewport is resized to {int} by {int}")]
async fn when_browser_viewport_is_resized(world: &mut ScenarioWorld, width: usize, height: usize) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before viewport changes");
    page.set_viewport_size(Viewport {
        width: width as u32,
        height: height as u32,
    })
    .await
    .expect("browser viewport must be resizable");
}

#[when(expr = "selector {string} is filled with {string}")]
async fn when_selector_is_filled_with(world: &mut ScenarioWorld, selector: String, value: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    let value = expand_placeholders(world, &value);
    let locator = page.locator(&selector).await;
    locator
        .fill(&value, None)
        .await
        .expect("selector must be fillable");
}

#[when(expr = "selector {string} is pressed with {string}")]
async fn when_selector_is_pressed_with(world: &mut ScenarioWorld, selector: String, key: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    let key = expand_placeholders(world, &key);
    let locator = page.locator(&selector).await;
    locator
        .press(&key, None)
        .await
        .expect("selector must accept key press");
}

#[when(expr = "selector {string} is typed with {string}")]
async fn when_selector_is_typed_with(world: &mut ScenarioWorld, selector: String, value: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    let value = expand_placeholders(world, &value);
    let locator = page.locator(&selector).await;
    locator
        .press_sequentially(&value, None)
        .await
        .expect("selector must accept typed text");
}

#[when(expr = "selector {string} is clicked")]
async fn when_selector_is_clicked(world: &mut ScenarioWorld, selector: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    page.locator(&selector)
        .await
        .click(None)
        .await
        .expect("selector must be clickable");
}

#[when(expr = "selector {string} is clicked by script")]
async fn when_selector_is_clicked_by_script(world: &mut ScenarioWorld, selector: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    page.locator(&selector)
        .await
        .evaluate::<(), ()>("element => element.click()", None::<()>)
        .await
        .expect("selector must be script-clickable");
}

#[when(expr = "selector {string} uploads resource directory {string}")]
async fn when_selector_uploads_resource_directory(
    world: &mut ScenarioWorld,
    selector: String,
    placeholder: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    let resource_dir = resource_directory_path(world, &placeholder);
    let mut files = Vec::new();
    collect_regular_files(&resource_dir, &mut files);
    assert!(
        !files.is_empty(),
        "resource directory '{}' should contain files",
        resource_dir.display()
    );
    let payloads = files
        .iter()
        .map(|file| {
            let name = file
                .strip_prefix(&resource_dir)
                .expect("uploaded file must be under resource directory")
                .to_string_lossy()
                .replace('\\', "/");
            FilePayload::new(
                name,
                "application/octet-stream",
                std::fs::read(file).expect("uploaded file should be readable"),
            )
        })
        .collect::<Vec<_>>();
    page.locator(&selector)
        .await
        .set_input_files_payload_multiple(&payloads, None)
        .await
        .expect("selector must accept uploaded files");
}

fn collect_regular_files(directory: &Path, files: &mut Vec<PathBuf>) {
    let mut entries = std::fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read '{}': {error}", directory.display()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|error| panic!("failed to collect '{}': {error}", directory.display()));
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .unwrap_or_else(|error| panic!("failed to inspect '{}': {error}", path.display()));
        if file_type.is_dir() {
            collect_regular_files(&path, files);
        } else if file_type.is_file() {
            files.push(path);
        }
    }
}

#[when(expr = "these NSPL commands are executed on node {string}")]
async fn when_these_nspl_commands_are_executed_on_node(
    world: &mut ScenarioWorld,
    node_id: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let commands = expand_placeholders(world, docstring(step));
    let session = if commands_are_retry_safe_session_ops(&commands) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match execute_nspl_commands_on_node(world, &node_id, &commands).await {
                Ok(session) => break session,
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "failed to execute NSPL setup command on requested node: {error:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    } else {
        execute_nspl_commands_on_node(world, &node_id, &commands)
            .await
            .expect("failed to execute NSPL setup command on requested node")
    };
    world.active_session = Some(session);
    world.active_session_node = Some(node_id);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[given("the leader node is configured with these NSPL commands")]
async fn given_the_leader_node_is_configured_with_these_nspl_commands(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let leader = current_leader_node(world).await;
    let commands = expand_placeholders(world, docstring(step));
    let session = execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .expect("failed to execute NSPL setup command on leader");
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[when(expr = "these NSPL commands fail with {string}")]
async fn when_these_nspl_commands_fail_with(
    world: &mut ScenarioWorld,
    expected_error: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;

    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    match execute_nspl_commands_on_node(world, &leader, &commands).await {
        Ok(_) => panic!("expected commands to fail with {:?}", expected_error),
        Err(error) => {
            assert!(
                error.contains(&expected_error),
                "expected error containing {:?}, got: {error}",
                expected_error
            );
            world.last_command_error = Some(error);
        }
    }
}

#[when("these NSPL commands fail")]
async fn when_these_nspl_commands_fail(world: &mut ScenarioWorld, #[step] step: &Step) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;

    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    match execute_nspl_commands_on_node(world, &leader, &commands).await {
        Ok(_) => panic!("expected commands to fail"),
        Err(error) => {
            append_cucumber_log_line(&format!("expected command failure observed: {error}"));
            world.last_command_error = Some(error);
        }
    }
}

#[when(expr = "the ingestor logic fixture {string} starts with output schema {string} and program")]
async fn when_the_ingestor_logic_fixture_starts_with_output_schema_and_program(
    world: &mut ScenarioWorld,
    transport_fixture: String,
    output_schema_fixture: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.last_subscription_payload = None;

    let transport = IngestorLogicTransportFixture::parse(&transport_fixture);
    let output_schema = IngestorLogicOutputSchemaFixture::parse(&output_schema_fixture);
    transport.prepare(world).await;
    let commands = expand_placeholders(
        world,
        &build_ingestor_logic_commands(transport, output_schema, docstring(step), true),
    );
    let leader = current_leader_node(world).await;
    let session = execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .expect("failed to start ingestor logic fixture on leader");
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = true;
    transport.await_ready(world).await;
}

#[when(
    expr = "the ingestor logic fixture {string} fails to start with output schema {string} and \
            program"
)]
async fn when_the_ingestor_logic_fixture_fails_to_start_with_output_schema_and_program(
    world: &mut ScenarioWorld,
    transport_fixture: String,
    output_schema_fixture: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.last_subscription_payload = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;

    let transport = IngestorLogicTransportFixture::parse(&transport_fixture);
    let output_schema = IngestorLogicOutputSchemaFixture::parse(&output_schema_fixture);
    transport.prepare(world).await;
    let commands = expand_placeholders(
        world,
        &build_ingestor_logic_commands(transport, output_schema, docstring(step), false),
    );
    let leader = current_leader_node(world).await;
    match execute_nspl_commands_on_node(world, &leader, &commands).await {
        Ok(_) => panic!("expected ingestor logic fixture to fail during leader validation"),
        Err(error) => {
            append_cucumber_log_line(&format!("logic fixture start failure observed: {error}"));
            world.last_command_error = Some(error);
        }
    }
}

#[when(expr = "the ingestor logic transport {string} delivers payload fixture {string}")]
async fn when_the_ingestor_logic_transport_delivers_payload_fixture(
    world: &mut ScenarioWorld,
    transport_fixture: String,
    payload_fixture: String,
) {
    let transport = IngestorLogicTransportFixture::parse(&transport_fixture);
    let payload_fixture = IngestorLogicPayloadFixture::parse(&payload_fixture);
    for payload in payload_fixture.payloads() {
        transport.deliver(world, payload).await;
    }
}

#[when(
    expr = "the ingestor logic transport {string} delivers payload fixture {string} with headers"
)]
async fn when_the_ingestor_logic_transport_delivers_payload_fixture_with_headers(
    world: &mut ScenarioWorld,
    transport_fixture: String,
    payload_fixture: String,
) {
    let transport = IngestorLogicTransportFixture::parse(&transport_fixture);
    let payload_fixture = IngestorLogicPayloadFixture::parse(&payload_fixture);
    for payload in payload_fixture.payloads() {
        transport.deliver_with_headers(world, payload).await;
    }
}

#[when(expr = "these NSPL commands fail on a follower node with {string}")]
async fn when_these_nspl_commands_fail_on_a_follower_node_with(
    world: &mut ScenarioWorld,
    expected_error: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    world.last_server_error = None;
    let follower = world
        .cluster()
        .any_follower_node("node-1")
        .await
        .expect("failed to resolve follower node");
    let commands = expand_placeholders(world, docstring(step));
    let mut session = world
        .cluster()
        .open_session(&follower, &world.domain)
        .await
        .expect("failed to open raw follower session");
    for command in nspl_statements(&commands) {
        match session.run_command(&command).await {
            Ok(_) => panic!(
                "expected follower commands to fail with {:?}",
                expected_error
            ),
            Err(error) => {
                let error = error.to_string();
                assert!(
                    error.contains(&expected_error),
                    "expected error containing {:?}, got: {error}",
                    expected_error
                );
                world.last_command_error = Some(error);
            }
        }
    }
}

#[then(expr = "the ingestor logic expectation {string} is observed")]
async fn then_the_ingestor_logic_expectation_is_observed(
    world: &mut ScenarioWorld,
    expectation_fixture: String,
) {
    let expectation = IngestorLogicExpectationFixture::parse(&expectation_fixture);
    expectation.assert_observed(world).await;
}

#[then("the last command output contains")]
async fn then_last_command_output_contains(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected = expand_placeholders(world, docstring(step));
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    assert!(
        output.contains(expected.trim()),
        "expected command output fragment {} in output, got: {output}",
        expected.trim()
    );
}

#[then("the last command error contains")]
async fn then_last_command_error_contains(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected = expand_placeholders(world, docstring(step));
    let error = world
        .last_command_error
        .as_deref()
        .expect("a command error must exist before assertion");
    assert!(
        error.contains(expected.trim()),
        "expected command error fragment {} in error, got: {error}",
        expected.trim()
    );
}

#[then(expr = "selector {string} contains {string} exactly {int} times")]
async fn then_selector_contains_text_exactly_times(
    world: &mut ScenarioWorld,
    selector: String,
    expected: String,
    expected_count: usize,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let expected = expand_placeholders(world, &expected);
    let locator = page.locator(&selector).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let texts = locator
            .all_inner_texts()
            .await
            .expect("selector text must be readable");
        let text = texts.join("\n");
        let count = text.matches(&expected).count();
        if count == expected_count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to contain '{expected}' {expected_count} times, got \
             {count}: {text}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} contains {string}")]
async fn then_selector_contains_text(
    world: &mut ScenarioWorld,
    selector: String,
    expected: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let expected = expand_placeholders(world, &expected);
    let locator = page.locator(&selector).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let texts = locator
            .all_inner_texts()
            .await
            .expect("selector text must be readable");
        let text = texts.join("\n");
        if text.contains(&expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to contain '{expected}', got '{text}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} contains {string} for {int} milliseconds")]
async fn then_selector_contains_text_for_milliseconds(
    world: &mut ScenarioWorld,
    selector: String,
    expected: String,
    duration_milliseconds: usize,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let expected = expand_placeholders(world, &expected);
    let locator = page.locator(&selector).await;
    let deadline = Instant::now() + Duration::from_millis(duration_milliseconds as u64);
    loop {
        tokio::task::consume_budget().await;
        let texts = locator
            .all_inner_texts()
            .await
            .expect("selector text must be readable");
        let text = texts.join("\n");
        assert!(
            text.contains(&expected),
            "expected selector '{selector}' to keep containing '{expected}', got '{text}'"
        );
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} does not contain {string}")]
async fn then_selector_does_not_contain_text(
    world: &mut ScenarioWorld,
    selector: String,
    unexpected: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let unexpected = expand_placeholders(world, &unexpected);
    let locator = page.locator(&selector).await;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        tokio::task::consume_budget().await;
        let texts = locator
            .all_inner_texts()
            .await
            .expect("selector text must be readable");
        let text = texts.join("\n");
        assert!(
            !text.contains(&unexpected),
            "expected selector '{selector}' not to contain '{unexpected}', got '{text}'"
        );
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} does not exist")]
async fn then_selector_does_not_exist(world: &mut ScenarioWorld, selector: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let script = format!(
        r#"
        () => document.querySelectorAll({selector:?}).length === 0
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        tokio::task::consume_budget().await;
        let missing = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("selector existence must be readable");
        assert!(missing, "expected selector '{selector}' not to exist");
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} has value {string}")]
async fn then_selector_has_value(world: &mut ScenarioWorld, selector: String, expected: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let expected = expand_placeholders(world, &expected);
    let locator = page.locator(&selector).await;
    locator
        .wait_for(Some(
            WaitForOptions::builder()
                .state(WaitForState::Visible)
                .timeout(10_000.0)
                .build(),
        ))
        .await
        .expect("selector must become visible");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let value = locator
            .input_value(None)
            .await
            .expect("selector value must be readable");
        if value == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to have value '{expected}', got '{value}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} is scrolled to bottom")]
async fn then_selector_is_scrolled_to_bottom(world: &mut ScenarioWorld, selector: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let script = format!(
        r#"
        async () => {{
            const el = document.querySelector({selector:?});
            if (!el) {{
                return false;
            }}
            return Math.abs(el.scrollHeight - el.clientHeight - el.scrollTop) <= 2;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let is_scrolled_to_bottom = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("selector scroll position must be readable");
        if is_scrolled_to_bottom {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to be scrolled to bottom"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} is pinned to viewport bottom")]
async fn then_selector_is_pinned_to_viewport_bottom(world: &mut ScenarioWorld, selector: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let script = format!(
        r#"
        () => {{
            const el = document.querySelector({selector:?});
            if (!el) {{
                return false;
            }}
            const style = window.getComputedStyle(el);
            const rect = el.getBoundingClientRect();
            return style.display !== 'none'
                && style.visibility !== 'hidden'
                && rect.width > 0
                && rect.height > 0
                && rect.top >= 0
                && rect.left >= 0
                && rect.right <= window.innerWidth + 2
                && Math.abs(rect.bottom - window.innerHeight) <= 2;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let is_pinned = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("selector viewport position must be readable");
        if is_pinned {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to be pinned to viewport bottom"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} does not overlap selector {string}")]
async fn then_selector_does_not_overlap_selector(
    world: &mut ScenarioWorld,
    first: String,
    second: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let first = expand_placeholders(world, &first);
    let second = expand_placeholders(world, &second);
    let script = format!(
        r#"
        () => {{
            const first = document.querySelector({first:?});
            const second = document.querySelector({second:?});
            if (!first || !second) {{
                return false;
            }}
            const a = first.getBoundingClientRect();
            const b = second.getBoundingClientRect();
            return a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let does_not_overlap = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("selector positions must be readable");
        if does_not_overlap {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{first}' not to overlap selector '{second}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph item {string} does not overlap graph item {string}")]
async fn then_graph_item_does_not_overlap_graph_item(
    world: &mut ScenarioWorld,
    first: String,
    second: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let first = expand_placeholders(world, &first);
    let second = expand_placeholders(world, &second);
    let script = format!(
        r#"
        () => {{
            const itemByLabel = (label) => Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === label);
            const first = itemByLabel({first:?});
            const second = itemByLabel({second:?});
            if (!first || !second) {{
                return false;
            }}
            const a = first.getBoundingClientRect();
            const b = second.getBoundingClientRect();
            return a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let does_not_overlap = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("graph item positions must be readable");
        if does_not_overlap {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph item '{first}' not to overlap graph item '{second}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph item {string} has graph width at least {int} pixels")]
async fn then_graph_item_has_graph_width_at_least(
    world: &mut ScenarioWorld,
    item: String,
    expected_width: i32,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            if (!item) {{
                return null;
            }}
            return Number.parseFloat(item.style.width || getComputedStyle(item).width);
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let actual_width = page
            .evaluate::<(), Option<f64>>(&script, None::<&()>)
            .await
            .expect("graph item width must be readable");
        if actual_width.is_some_and(|width| width >= f64::from(expected_width)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph item '{item}' to have graph width at least {expected_width}px, got \
             {actual_width:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph item {string} has status {string}")]
async fn then_graph_item_has_status(world: &mut ScenarioWorld, item: String, expected: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let item = expand_placeholders(world, &item);
    let expected = expand_placeholders(world, &expected);
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            return item ? item.dataset.status : null;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let status = page
            .evaluate::<(), Option<String>>(&script, None::<&()>)
            .await
            .expect("graph item status must be readable");
        if status.as_deref() == Some(expected.as_str()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph item '{item}' to have status '{expected}', got '{status:?}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph item {string} is highlighted by graph search")]
async fn then_graph_item_is_highlighted_by_graph_search(world: &mut ScenarioWorld, item: String) {
    then_graph_item_search_highlight_matches(world, item, true).await;
}

#[then(expr = "graph item {string} is not highlighted by graph search")]
async fn then_graph_item_is_not_highlighted_by_graph_search(
    world: &mut ScenarioWorld,
    item: String,
) {
    then_graph_item_search_highlight_matches(world, item, false).await;
}

async fn then_graph_item_search_highlight_matches(
    world: &mut ScenarioWorld,
    item: String,
    expected: bool,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            return item ? item.dataset.searchHighlight === "true" : null;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let highlighted = page
            .evaluate::<(), Option<bool>>(&script, None::<&()>)
            .await
            .expect("graph search highlight state must be readable");
        if highlighted == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph item '{item}' search highlight to be {expected}, got {highlighted:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph search highlights exactly {int} graph items")]
async fn then_graph_search_highlights_exactly_graph_items(
    world: &mut ScenarioWorld,
    expected_count: i32,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let script = r#"
        () => Array
            .from(document.querySelectorAll(".graph-hit-layer button"))
            .filter((element) => element.dataset.searchHighlight === "true")
            .length
    "#;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let count = page
            .evaluate::<(), i32>(script, None::<&()>)
            .await
            .expect("graph search highlight count must be readable");
        if count == expected_count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph search to highlight {expected_count} graph items, got {count}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph search result {string} is visible in the graph viewport")]
async fn then_graph_search_result_is_visible_in_the_graph_viewport(
    world: &mut ScenarioWorld,
    item: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const stage = document.querySelector(".graph-stage");
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            if (!stage || !item) {{
                return `missing stage=${{Boolean(stage)}} item=${{Boolean(item)}}`;
            }}
            const stageBox = stage.getBoundingClientRect();
            const itemBox = item.getBoundingClientRect();
            if (item.dataset.searchHighlight !== "true") {{
                return "item is not highlighted";
            }}
            if (boxContained(itemBox, stageBox)) {{
                return "OK";
            }}
            return `outside viewport item=${{JSON.stringify(boxSummary(itemBox))}} stage=${{JSON.stringify(boxSummary(stageBox))}}`;

            function boxContained(box, stageBox) {{
                const margin = 8;
                return box.left >= stageBox.left + margin
                    && box.right <= stageBox.right - margin
                    && box.top >= stageBox.top + margin
                    && box.bottom <= stageBox.bottom - margin;
            }}

            function boxSummary(box) {{
                return {{
                    left: Math.round(box.left),
                    right: Math.round(box.right),
                    top: Math.round(box.top),
                    bottom: Math.round(box.bottom),
                }};
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph search result visibility must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph search result '{item}' to be visible in the graph viewport: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph relay item {string} has buffer statistics")]
async fn then_graph_relay_item_has_buffer_statistics(
    world: &mut ScenarioWorld,
    relay: String,
    #[step] step: &Step,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let relay = expand_placeholders(world, &relay);
    let assertions = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(parse_numeric_metric_assertion)
        .collect::<Vec<_>>();
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".relay-hit"))
                .find((element) => element.dataset.label === {relay:?});
            if (!item) {{
                return null;
            }}
            return {{
                capacity: item.dataset.bufferCapacity || "",
                p50: item.dataset.bufferP50 || "",
                p90: item.dataset.bufferP90 || "",
                p99: item.dataset.bufferP99 || ""
            }};
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let statistics = page
            .evaluate::<(), Option<BTreeMap<String, String>>>(&script, None::<&()>)
            .await
            .expect("graph relay buffer statistics must be readable");
        if let Some(statistics) = &statistics {
            let matches = assertions.iter().all(|assertion| {
                statistics
                    .get(&assertion.field)
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_some_and(|actual| assertion.op.matches(actual, assertion.expected))
            });
            if matches {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "expected relay '{relay}' buffer statistics to satisfy {:?}, got {:?}",
            assertions,
            statistics
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} is visible")]
async fn then_graph_edge_from_to_is_visible(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    then_graph_edge_with_kind_from_to_is_visible(world, "DATA".to_string(), source, target).await;
}

#[when(
    expr = "graph edge from {string} to {string} is clicked with viewport focused on its middle"
)]
async fn when_graph_edge_from_to_is_clicked_with_viewport_focused_on_its_middle(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph interactions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            return (async () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = findEdge(".graph-edge", source, target);
            const hit = findEdge(".graph-edge-hit", source, target);
            const stage = document.querySelector(".graph-stage");
            if (!edge || !hit || !stage) {{
                return failure(`missing edge=${{Boolean(edge)}} hit=${{Boolean(hit)}} stage=${{Boolean(stage)}}`);
            }}
            const reset = Array
                .from(document.querySelectorAll(".zoom-group button"))
                .find((button) => button.getAttribute("title") === "Reset zoom");
            const zoomIn = Array
                .from(document.querySelectorAll(".zoom-group button"))
                .find((button) => button.getAttribute("title") === "Zoom in");
            if (!reset || !zoomIn) {{
                return failure(`missing zoom controls reset=${{Boolean(reset)}} zoomIn=${{Boolean(zoomIn)}}`);
            }}
            reset.click();
            for (let index = 0; index < 6; index += 1) {{
                zoomIn.click();
            }}
            await waitForStableTransform();
            const rect = stage.getBoundingClientRect();
            const middle = edgeScreenPoint(edge, 0.5);
            if (!middle) {{
                return failure("edge middle is unreadable");
            }}
            const centerX = Math.round(rect.left + rect.width / 2);
            const centerY = Math.round(rect.top + rect.height / 2);
            const deltaX = Math.round(centerX - middle.x);
            const deltaY = Math.round(centerY - middle.y);
            stage.dispatchEvent(new MouseEvent("mousedown", {{
                bubbles: true,
                cancelable: true,
                button: 0,
                clientX: centerX,
                clientY: centerY
            }}));
            stage.dispatchEvent(new MouseEvent("mousemove", {{
                bubbles: true,
                cancelable: true,
                button: 0,
                clientX: centerX + deltaX,
                clientY: centerY + deltaY
            }}));
            stage.dispatchEvent(new MouseEvent("mouseup", {{
                bubbles: true,
                cancelable: true,
                button: 0,
                clientX: centerX + deltaX,
                clientY: centerY + deltaY
            }}));
            await waitForStableTransform();
            if (endpointsVisibleInStage(source, target)) {{
                return failure("setup did not isolate the edge middle from both endpoints");
            }}
            const clickPoint = edgeScreenPoint(findEdge(".graph-edge", source, target), 0.5);
            if (!clickPoint) {{
                return failure("edge click point is unreadable");
            }}
            const element = document.elementFromPoint(clickPoint.x, clickPoint.y);
            const clickHit = findEdge(".graph-edge-hit", source, target);
            if (
                element !== clickHit
                && element?.closest?.(".graph-edge-group") !== clickHit?.closest?.(".graph-edge-group")
            ) {{
                return failure(`edge click point is not owned by target edge: tag=${{element?.tagName ?? ""}} class=${{element?.getAttribute?.("class") ?? ""}}`);
            }}
            return {{
                status: "OK",
                x: String(Math.round(clickPoint.x)),
                y: String(Math.round(clickPoint.y)),
            }};

            function failure(message) {{
                return {{
                    status: message,
                    x: "0",
                    y: "0",
                }};
            }}

            function nextFrame() {{
                return new Promise((resolve) => requestAnimationFrame(() => resolve()));
            }}

            async function waitForStableTransform() {{
                const layer = document.querySelector(".graph-zoom-layer");
                if (!layer) {{
                    await nextFrame();
                    return;
                }}
                let stableFrames = 0;
                let previous = "";
                for (let index = 0; index < 30; index += 1) {{
                    await nextFrame();
                    const current = getComputedStyle(layer).transform;
                    if (current === previous) {{
                        stableFrames += 1;
                        if (stableFrames >= 3) {{
                            return;
                        }}
                    }} else {{
                        stableFrames = 0;
                        previous = current;
                    }}
                }}
            }}

            function findEdge(selector, source, target) {{
                return Array
                    .from(document.querySelectorAll(selector))
                    .find((path) =>
                        path.dataset.kind === "DATA"
                        && path.dataset.source.endsWith(`:${{source}}`)
                        && path.dataset.target.endsWith(`:${{target}}`)
                    );
            }}

            function edgeScreenPoint(path, ratio) {{
                if (!path) {{
                    return null;
                }}
                const length = path.getTotalLength();
                const matrix = path.getScreenCTM();
                if (length <= 0 || !matrix) {{
                    return null;
                }}
                const local = path.getPointAtLength(length * ratio);
                return new DOMPoint(local.x, local.y).matrixTransform(matrix);
            }}

            function endpointsVisibleInStage(source, target) {{
                const sourceItem = graphItem(source);
                const targetItem = graphItem(target);
                if (!sourceItem || !targetItem) {{
                    return false;
                }}
                const stageBox = stage.getBoundingClientRect();
                return boxContained(sourceItem.getBoundingClientRect(), stageBox)
                    && boxContained(targetItem.getBoundingClientRect(), stageBox);
            }}

            function graphItem(label) {{
                return Array
                    .from(document.querySelectorAll(".graph-hit-layer button"))
                    .find((element) => element.dataset.label === label);
            }}

            function boxContained(box, stageBox) {{
                const margin = 8;
                return box.left >= stageBox.left + margin
                    && box.right <= stageBox.right - margin
                    && box.top >= stageBox.top + margin
                    && box.bottom <= stageBox.bottom - margin;
            }}
            }})();
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), BTreeMap<String, String>>(&script, None::<&()>)
            .await
            .expect("graph edge click setup must be executable");
        let status = result
            .get("status")
            .expect("graph edge click setup must return status");
        if status == "OK" {
            let x = result
                .get("x")
                .and_then(|value| value.parse::<i32>().ok())
                .expect("graph edge click setup must return x coordinate");
            let y = result
                .get("y")
                .and_then(|value| value.parse::<i32>().ok())
                .expect("graph edge click setup must return y coordinate");
            page.mouse()
                .click(x, y, None)
                .await
                .expect("graph edge click must be executable");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to be clickable with a focused \
             middle viewport: {status}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(
    expr = "graph edge from {string} to {string} has both endpoints visible in the graph viewport"
)]
async fn then_graph_edge_from_to_has_both_endpoints_visible_in_the_graph_viewport(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const stage = document.querySelector(".graph-stage");
            const sourceItem = graphItem(source);
            const targetItem = graphItem(target);
            if (!stage || !sourceItem || !targetItem) {{
                return `missing stage=${{Boolean(stage)}} source=${{Boolean(sourceItem)}} target=${{Boolean(targetItem)}}`;
            }}
            const stageBox = stage.getBoundingClientRect();
            const sourceBox = sourceItem.getBoundingClientRect();
            const targetBox = targetItem.getBoundingClientRect();
            if (boxContained(sourceBox, stageBox) && boxContained(targetBox, stageBox)) {{
                return "OK";
            }}
            return `outside viewport source=${{JSON.stringify(boxSummary(sourceBox))}} target=${{JSON.stringify(boxSummary(targetBox))}} stage=${{JSON.stringify(boxSummary(stageBox))}}`;

            function graphItem(label) {{
                return Array
                    .from(document.querySelectorAll(".graph-hit-layer button"))
                    .find((element) => element.dataset.label === label);
            }}

            function boxContained(box, stageBox) {{
                const margin = 8;
                return box.left >= stageBox.left + margin
                    && box.right <= stageBox.right - margin
                    && box.top >= stageBox.top + margin
                    && box.bottom <= stageBox.bottom - margin;
            }}

            function boxSummary(box) {{
                return {{
                    left: Math.round(box.left),
                    right: Math.round(box.right),
                    top: Math.round(box.top),
                    bottom: Math.round(box.bottom),
                }};
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge endpoint visibility must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to focus both endpoints in the \
             graph viewport: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} does not intersect graph item {string}")]
async fn then_graph_edge_from_to_does_not_intersect_graph_item(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    item: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            if (!edge || !item) {{
                return false;
            }}
            const itemBox = item.getBoundingClientRect();
            return !pathIntersectsScreenBox(edge, itemBox, 2);

            function pathIntersectsScreenBox(path, box, inset) {{
                const matrix = path.getScreenCTM();
                const svg = path.ownerSVGElement;
                if (!matrix || !svg) {{
                    return true;
                }}
                const point = svg.createSVGPoint();
                const length = path.getTotalLength();
                const samples = Math.max(2, Math.ceil(length / 4));
                for (let index = 0; index <= samples; index += 1) {{
                    const local = path.getPointAtLength(length * index / samples);
                    point.x = local.x;
                    point.y = local.y;
                    const screen = point.matrixTransform(matrix);
                    if (
                        screen.x > box.left + inset
                        && screen.x < box.right - inset
                        && screen.y > box.top + inset
                        && screen.y < box.bottom - inset
                    ) {{
                        return true;
                    }}
                }}
                return false;
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let does_not_intersect = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("graph edge and item positions must be readable");
        if does_not_intersect {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' not to intersect graph item \
             '{item}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} does not intersect branch group {string} body")]
async fn then_graph_edge_from_to_does_not_intersect_branch_group_body(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    schema: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let schema = expand_placeholders(world, &schema);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            const bodies = Array
                .from(document.querySelectorAll(".graph-branch-body"))
                .filter((path) => path.dataset.schema === {schema:?});
            if (!edge || bodies.length === 0) {{
                return `missing edge=${{Boolean(edge)}} bodies=${{bodies.length}}`;
            }}
            const path = edge.getAttribute("d") ?? "";
            for (const body of bodies) {{
                const box = bodyScreenBox(body);
                if (pathIntersectsScreenBox(edge, box, 2)) {{
                    return `intersects path=${{path}} body=${{JSON.stringify({{
                        x: body.dataset.x,
                        y: body.dataset.y,
                        width: body.dataset.width,
                        height: body.dataset.height,
                        box,
                    }})}}`;
                }}
            }}
            return "OK";

            function bodyScreenBox(body) {{
                const matrix = body.getScreenCTM();
                const svg = body.ownerSVGElement;
                if (!matrix || !svg) {{
                    return null;
                }}
                const point = svg.createSVGPoint();
                const x = Number(body.dataset.x);
                const y = Number(body.dataset.y);
                const width = Number(body.dataset.width);
                const height = Number(body.dataset.height);
                point.x = x;
                point.y = y;
                const topLeft = point.matrixTransform(matrix);
                point.x = x + width;
                point.y = y + height;
                const bottomRight = point.matrixTransform(matrix);
                return {{
                    left: Math.min(topLeft.x, bottomRight.x),
                    right: Math.max(topLeft.x, bottomRight.x),
                    top: Math.min(topLeft.y, bottomRight.y),
                    bottom: Math.max(topLeft.y, bottomRight.y),
                }};
            }}

            function pathIntersectsScreenBox(path, box, inset) {{
                if (!box) {{
                    return true;
                }}
                const matrix = path.getScreenCTM();
                const svg = path.ownerSVGElement;
                if (!matrix || !svg) {{
                    return true;
                }}
                const point = svg.createSVGPoint();
                const length = path.getTotalLength();
                const samples = Math.max(2, Math.ceil(length / 4));
                for (let index = 0; index <= samples; index += 1) {{
                    const local = path.getPointAtLength(length * index / samples);
                    point.x = local.x;
                    point.y = local.y;
                    const screen = point.matrixTransform(matrix);
                    if (
                        screen.x > box.left + inset
                        && screen.x < box.right - inset
                        && screen.y > box.top + inset
                        && screen.y < box.bottom - inset
                    ) {{
                        return true;
                    }}
                }}
                return false;
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge and branch group positions must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' not to intersect branch group \
             '{schema}' body: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(
    expr = "graph edge from {string} to {string} does not intersect graph edge from {string} to \
            {string}"
)]
async fn then_graph_edge_from_to_does_not_intersect_graph_edge_from_to(
    world: &mut ScenarioWorld,
    first_source: String,
    first_target: String,
    second_source: String,
    second_target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let first_source = expand_placeholders(world, &first_source);
    let first_target = expand_placeholders(world, &first_target);
    let second_source = expand_placeholders(world, &second_source);
    let second_target = expand_placeholders(world, &second_target);
    let script = format!(
        r#"
        () => {{
            const firstSource = {first_source:?};
            const firstTarget = {first_target:?};
            const secondSource = {second_source:?};
            const secondTarget = {second_target:?};
            const first = findEdge(firstSource, firstTarget);
            const second = findEdge(secondSource, secondTarget);
            if (!first || !second) {{
                return `missing first=${{Boolean(first)}} second=${{Boolean(second)}}`;
            }}
            const firstPath = first.getAttribute("d") ?? "";
            const secondPath = second.getAttribute("d") ?? "";
            const firstPoints = pathScreenPoints(first);
            const secondPoints = pathScreenPoints(second);
            if (!firstPoints || !secondPoints) {{
                return `unreadable first=${{firstPath}} second=${{secondPath}}`;
            }}
            for (let firstIndex = 0; firstIndex < firstPoints.length - 1; firstIndex += 1) {{
                const a = firstPoints[firstIndex];
                const b = firstPoints[firstIndex + 1];
                for (let secondIndex = 0; secondIndex < secondPoints.length - 1; secondIndex += 1) {{
                    const c = secondPoints[secondIndex];
                    const d = secondPoints[secondIndex + 1];
                    if (shareEndpoint(a, b, c, d)) {{
                        continue;
                    }}
                    if (segmentsIntersect(a, b, c, d)) {{
                        return `intersects first=${{firstPath}} second=${{secondPath}} firstSegment=${{firstIndex}} secondSegment=${{secondIndex}}`;
                    }}
                }}
            }}
            return "OK";

            function findEdge(source, target) {{
                return Array
                    .from(document.querySelectorAll(".graph-edge"))
                    .find((path) =>
                        path.dataset.kind === "DATA"
                        && path.dataset.source.endsWith(`:${{source}}`)
                        && path.dataset.target.endsWith(`:${{target}}`)
                    );
            }}

            function pathScreenPoints(path) {{
                const matrix = path.getScreenCTM();
                const svg = path.ownerSVGElement;
                if (!matrix || !svg) {{
                    return null;
                }}
                const point = svg.createSVGPoint();
                const length = path.getTotalLength();
                const samples = Math.max(2, Math.ceil(length / 6));
                const points = [];
                for (let index = 0; index <= samples; index += 1) {{
                    const local = path.getPointAtLength(length * index / samples);
                    point.x = local.x;
                    point.y = local.y;
                    const screen = point.matrixTransform(matrix);
                    points.push({{ x: screen.x, y: screen.y }});
                }}
                return points;
            }}

            function shareEndpoint(a, b, c, d) {{
                return pointDistance(a, c) < 6
                    || pointDistance(a, d) < 6
                    || pointDistance(b, c) < 6
                    || pointDistance(b, d) < 6;
            }}

            function pointDistance(a, b) {{
                return Math.hypot(a.x - b.x, a.y - b.y);
            }}

            function segmentsIntersect(a, b, c, d) {{
                const epsilon = 0.1;
                if (
                    Math.max(a.x, b.x) + epsilon < Math.min(c.x, d.x)
                    || Math.max(c.x, d.x) + epsilon < Math.min(a.x, b.x)
                    || Math.max(a.y, b.y) + epsilon < Math.min(c.y, d.y)
                    || Math.max(c.y, d.y) + epsilon < Math.min(a.y, b.y)
                ) {{
                    return false;
                }}
                const abC = cross(a, b, c);
                const abD = cross(a, b, d);
                const cdA = cross(c, d, a);
                const cdB = cross(c, d, b);
                if (Math.abs(abC) <= epsilon && onSegment(a, b, c, epsilon)) {{
                    return true;
                }}
                if (Math.abs(abD) <= epsilon && onSegment(a, b, d, epsilon)) {{
                    return true;
                }}
                if (Math.abs(cdA) <= epsilon && onSegment(c, d, a, epsilon)) {{
                    return true;
                }}
                if (Math.abs(cdB) <= epsilon && onSegment(c, d, b, epsilon)) {{
                    return true;
                }}
                return (
                    (abC > epsilon && abD < -epsilon || abC < -epsilon && abD > epsilon)
                    && (cdA > epsilon && cdB < -epsilon || cdA < -epsilon && cdB > epsilon)
                );
            }}

            function cross(a, b, c) {{
                return (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x);
            }}

            function onSegment(a, b, c, epsilon) {{
                return c.x >= Math.min(a.x, b.x) - epsilon
                    && c.x <= Math.max(a.x, b.x) + epsilon
                    && c.y >= Math.min(a.y, b.y) - epsilon
                    && c.y <= Math.max(a.y, b.y) + epsilon;
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge paths must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{first_source}' to '{first_target}' not to intersect graph \
             edge from '{second_source}' to '{second_target}': {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(
    expr = "graph edge from {string} to {string} does not share horizontal lane with graph edge \
            from {string} to {string}"
)]
async fn then_graph_edge_from_to_does_not_share_horizontal_lane_with_graph_edge_from_to(
    world: &mut ScenarioWorld,
    first_source: String,
    first_target: String,
    second_source: String,
    second_target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let first_source = expand_placeholders(world, &first_source);
    let first_target = expand_placeholders(world, &first_target);
    let second_source = expand_placeholders(world, &second_source);
    let second_target = expand_placeholders(world, &second_target);
    let script = format!(
        r#"
        () => {{
            const first = findEdge({first_source:?}, {first_target:?});
            const second = findEdge({second_source:?}, {second_target:?});
            if (!first || !second) {{
                return `missing first=${{Boolean(first)}} second=${{Boolean(second)}}`;
            }}
            const firstLane = dominantHorizontalLane(first);
            const secondLane = dominantHorizontalLane(second);
            if (!firstLane || !secondLane) {{
                return `missing lane first=${{JSON.stringify(firstLane)}} second=${{JSON.stringify(secondLane)}}`;
            }}
            if (Math.abs(firstLane.y - secondLane.y) >= 8) {{
                return "OK";
            }}
            return `shared lane first=${{JSON.stringify(firstLane)}} second=${{JSON.stringify(secondLane)}} firstPath=${{first.getAttribute("d")}} secondPath=${{second.getAttribute("d")}}`;

            function findEdge(source, target) {{
                return Array
                    .from(document.querySelectorAll(".graph-edge"))
                    .find((path) =>
                        path.dataset.kind === "DATA"
                        && path.dataset.source.endsWith(`:${{source}}`)
                        && path.dataset.target.endsWith(`:${{target}}`)
                    );
            }}

            function dominantHorizontalLane(path) {{
                const length = path.getTotalLength();
                if (length <= 0) {{
                    return null;
                }}
                const samples = Math.max(4, Math.ceil(length / 4));
                let best = null;
                let active = null;
                let previous = path.getPointAtLength(0);
                for (let index = 1; index <= samples; index += 1) {{
                    const current = path.getPointAtLength(length * index / samples);
                    const dx = current.x - previous.x;
                    const dy = current.y - previous.y;
                    const segment = Math.hypot(dx, dy);
                    if (segment > 0 && Math.abs(dy) <= 1 && Math.abs(dx) >= Math.abs(dy) * 4) {{
                        const y = (previous.y + current.y) / 2;
                        if (active && Math.abs(active.y - y) <= 2) {{
                            active.length += segment;
                            active.y = (active.y + y) / 2;
                        }} else {{
                            if (!best || active && active.length > best.length) {{
                                best = active;
                            }}
                            active = {{ y, length: segment }};
                        }}
                    }} else {{
                        if (!best || active && active.length > best.length) {{
                            best = active;
                        }}
                        active = null;
                    }}
                    previous = current;
                }}
                if (!best || active && active.length > best.length) {{
                    best = active;
                }}
                return best;
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge horizontal lanes must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{first_source}' to '{first_target}' not to share a \
             horizontal lane with graph edge from '{second_source}' to '{second_target}': {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} starts horizontally")]
async fn then_graph_edge_from_to_starts_horizontally(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge) {{
                return "missing edge";
            }}
            const path = edge.getAttribute("d") ?? "";
            const length = edge.getTotalLength();
            if (length <= 0) {{
                return `empty path=${{path}}`;
            }}
            const start = edge.getPointAtLength(0);
            const end = edge.getPointAtLength(length);
            const sample = edge.getPointAtLength(Math.min(16, length));
            const expectedDirection = Math.sign(end.x - start.x);
            const dx = sample.x - start.x;
            const dy = sample.y - start.y;
            if (
                Math.abs(dx) > 1
                && Math.abs(dx) >= Math.abs(dy) * 2
                && (expectedDirection === 0 || Math.sign(dx) === expectedDirection)
            ) {{
                return "OK";
            }}
            return `non-horizontal start path=${{path}} dx=${{dx}} dy=${{dy}} expected=${{expectedDirection}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge path must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to start horizontally: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} ends horizontally")]
async fn then_graph_edge_from_to_ends_horizontally(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge) {{
                return "missing edge";
            }}
            const path = edge.getAttribute("d") ?? "";
            const length = edge.getTotalLength();
            if (length <= 0) {{
                return `empty path=${{path}}`;
            }}
            const sample = edge.getPointAtLength(Math.max(0, length - 16));
            const start = edge.getPointAtLength(0);
            const end = edge.getPointAtLength(length);
            const expectedDirection = Math.sign(end.x - start.x);
            const dx = end.x - sample.x;
            const dy = end.y - sample.y;
            if (
                Math.abs(dx) > 1
                && Math.abs(dx) >= Math.abs(dy) * 2
                && (expectedDirection === 0 || Math.sign(dx) === expectedDirection)
            ) {{
                return "OK";
            }}
            return `non-horizontal end path=${{path}} dx=${{dx}} dy=${{dy}} expected=${{expectedDirection}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge path must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to end horizontally: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} has target plug at least {int} pixels")]
async fn then_graph_edge_from_to_has_target_plug_at_least(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    expected_pixels: i32,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const expectedPixels = {expected_pixels};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge) {{
                return "missing edge";
            }}
            const path = edge.getAttribute("d") ?? "";
            const length = edge.getTotalLength();
            if (length <= 0) {{
                return `empty path=${{path}}`;
            }}
            const start = edge.getPointAtLength(0);
            const end = edge.getPointAtLength(length);
            const expectedDirection = Math.sign(end.x - start.x);
            let plug = 0;
            const limit = Math.min(length, expectedPixels + 24);
            for (let offset = 1; offset <= limit; offset += 1) {{
                const current = edge.getPointAtLength(length - offset);
                const dx = end.x - current.x;
                const dy = end.y - current.y;
                if (Math.abs(dy) > 1.5) {{
                    break;
                }}
                if (expectedDirection !== 0 && Math.sign(dx) !== expectedDirection) {{
                    break;
                }}
                plug = offset;
            }}
            if (plug >= expectedPixels) {{
                return "OK";
            }}
            return `short target plug length=${{plug}} expected=${{expectedPixels}} path=${{path}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge target plug must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to have a target plug at least \
             {expected_pixels}px: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} has at most {int} rounded turns")]
async fn then_graph_edge_from_to_has_at_most_rounded_turns(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    expected_turns: usize,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge) {{
                return "missing edge";
            }}
            const path = edge.getAttribute("d") ?? "";
            const turns = (path.match(/ Q/g) ?? []).length;
            if (turns <= {expected_turns}) {{
                return "OK";
            }}
            return `too many rounded turns=${{turns}} path=${{path}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge path must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to have at most {expected_turns} \
             rounded turns: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} has source plug at least {int} pixels")]
async fn then_graph_edge_from_to_has_source_plug_at_least(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    expected_pixels: i32,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const expectedPixels = {expected_pixels};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge) {{
                return "missing edge";
            }}
            const path = edge.getAttribute("d") ?? "";
            const length = edge.getTotalLength();
            if (length <= 0) {{
                return `empty path=${{path}}`;
            }}
            const start = edge.getPointAtLength(0);
            const end = edge.getPointAtLength(length);
            const expectedDirection = Math.sign(end.x - start.x);
            let plug = 0;
            const limit = Math.min(length, expectedPixels + 24);
            for (let offset = 1; offset <= limit; offset += 1) {{
                const current = edge.getPointAtLength(offset);
                const dx = current.x - start.x;
                const dy = current.y - start.y;
                if (Math.abs(dy) > 1.5) {{
                    break;
                }}
                if (expectedDirection !== 0 && Math.sign(dx) !== expectedDirection) {{
                    break;
                }}
                plug = offset;
            }}
            if (plug >= expectedPixels) {{
                return "OK";
            }}
            return `short source plug length=${{plug}} expected=${{expectedPixels}} path=${{path}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge source plug must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to have a source plug at least \
             {expected_pixels}px: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} uses a direct curve")]
async fn then_graph_edge_from_to_uses_direct_curve(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge) {{
                return "missing edge";
            }}
            const path = edge.getAttribute("d") ?? "";
            if (path.includes(" C")) {{
                return "OK";
            }}
            return `not a direct curve path=${{path}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge path must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to use a direct curve: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph action edge {string} from {string} to {string} is visible")]
async fn then_graph_action_edge_from_to_is_visible(
    world: &mut ScenarioWorld,
    kind: String,
    source: String,
    target: String,
) {
    let kind = kind.replace(' ', "_").to_ascii_uppercase();
    then_graph_edge_with_kind_from_to_is_visible(world, kind, source, target).await;
}

#[then(expr = "graph edge from {string} to {string} has traffic statistics")]
async fn then_graph_edge_from_to_has_traffic_statistics(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    #[step] step: &Step,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let assertions = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(parse_numeric_metric_assertion)
        .collect::<Vec<_>>();
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const item = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((element) =>
                    element.dataset.kind === "DATA"
                    &&
                    element.dataset.source.endsWith(`:${{source}}`)
                    && element.dataset.target.endsWith(`:${{target}}`)
                );
            if (!item) {{
                return null;
            }}
            return {{
                messages_total: item.dataset.messagesTotal || "",
                bytes_total: item.dataset.bytesTotal || "",
                batches_total: item.dataset.batchesTotal || "",
                messages_per_second: item.dataset.messagesPerSecond || "",
                bytes_per_second: item.dataset.bytesPerSecond || "",
                batches_per_second: item.dataset.batchesPerSecond || ""
            }};
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let statistics = page
            .evaluate::<(), Option<BTreeMap<String, String>>>(&script, None::<&()>)
            .await
            .expect("graph edge traffic statistics must be readable");
        if let Some(statistics) = &statistics {
            let matches = assertions.iter().all(|assertion| {
                statistics
                    .get(&assertion.field)
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_some_and(|actual| assertion.op.matches(actual, assertion.expected))
            });
            if matches {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to satisfy traffic assertions \
             {assertions:?}, got {statistics:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[when("graph topology render count observation starts")]
async fn when_graph_topology_render_count_observation_starts(world: &mut ScenarioWorld) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let script = r##"
        () => {
            const chart = document.querySelector("#execution-graph-chart");
            const renderCount = Number(chart?.dataset?.renderCount ?? NaN);
            if (!chart || !Number.isFinite(renderCount) || renderCount <= 0) {
                return false;
            }
            window.__nervixGraphTopologyObservedRenderCount = renderCount;
            return true;
        }
    "##;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let observing = page
            .evaluate::<(), bool>(script, None::<&()>)
            .await
            .expect("graph topology mutation observer must be installable");
        if observing {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected execution graph chart render count to be available for topology observation"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("graph topology render count does not change during observed traffic")]
async fn then_graph_topology_render_count_does_not_change_during_observed_traffic(
    world: &mut ScenarioWorld,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let script = r##"
        () => {
            const chart = document.querySelector("#execution-graph-chart");
            const observed = Number(window.__nervixGraphTopologyObservedRenderCount ?? NaN);
            const current = Number(chart?.dataset?.renderCount ?? NaN);
            if (!Number.isFinite(observed) || !Number.isFinite(current)) {
                return `missing render count observed=${observed} current=${current}`;
            }
            if (current === observed) {
                return "OK";
            }
            return `render count changed from ${observed} to ${current}`;
        }
    "##;
    let deadline = Instant::now() + Duration::from_millis(600);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(script, None::<&()>)
            .await
            .expect("graph topology render count must be readable");
        assert!(
            result == "OK",
            "expected no execution graph chart renders during traffic-only updates: {result}"
        );
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn then_graph_edge_with_kind_from_to_is_visible(
    world: &mut ScenarioWorld,
    kind: String,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const kind = {kind:?};
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === kind
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            const svg = document.querySelector(".graph-pulse-layer");
            if (!edge || !svg) {{
                return false;
            }}
            const box = edge.getBBox();
            const viewBox = svg.viewBox.baseVal;
            return box.width > 0
                && box.height >= 0
                && box.x >= viewBox.x
                && box.y >= viewBox.y
                && box.x + box.width <= viewBox.x + viewBox.width
                && box.y + box.height <= viewBox.y + viewBox.height;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let is_visible = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("graph edge position must be readable");
        if is_visible {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge kind '{kind}' from '{source}' to '{target}' to be visible"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} has exact hover target")]
async fn then_graph_edge_from_to_has_exact_hover_target(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            const hit = Array
                .from(document.querySelectorAll(".graph-edge-hit"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge || !hit) {{
                return `missing edge=${{Boolean(edge)}} hit=${{Boolean(hit)}}`;
            }}
            const length = edge.getTotalLength();
            const matrix = edge.getScreenCTM();
            if (length <= 0 || !matrix) {{
                return `invalid length=${{length}} matrix=${{Boolean(matrix)}}`;
            }}
            const samples = [0.35, 0.5, 0.65].map((ratio) => {{
                const point = edge.getPointAtLength(length * ratio);
                const screen = new DOMPoint(point.x, point.y).matrixTransform(matrix);
                return {{ ratio, screen }};
            }});
            const viewportWidth = window.innerWidth;
            const viewportHeight = window.innerHeight;
            const onScreen = samples.some((sample) =>
                sample.screen.x >= 0
                && sample.screen.y >= 0
                && sample.screen.x < viewportWidth
                && sample.screen.y < viewportHeight
            );
            if (!onScreen) {{
                const stage = document.querySelector(".graph-stage");
                const rect = stage?.getBoundingClientRect();
                if (!stage || !rect) {{
                    return "missing graph stage";
                }}
                const sample = samples[Math.floor(samples.length / 2)].screen;
                const startX = Math.round(rect.left + rect.width / 2);
                const startY = Math.round(rect.top + rect.height / 2);
                const deltaX = Math.round(startX - sample.x);
                const deltaY = Math.round(startY - sample.y);
                stage.dispatchEvent(new MouseEvent("mousedown", {{
                    bubbles: true,
                    cancelable: true,
                    button: 0,
                    clientX: startX,
                    clientY: startY
                }}));
                stage.dispatchEvent(new MouseEvent("mousemove", {{
                    bubbles: true,
                    cancelable: true,
                    button: 0,
                    clientX: startX + deltaX,
                    clientY: startY + deltaY
                }}));
                stage.dispatchEvent(new MouseEvent("mouseup", {{
                    bubbles: true,
                    cancelable: true,
                    button: 0,
                    clientX: startX + deltaX,
                    clientY: startY + deltaY
                }}));
                return `panned by ${{deltaX}},${{deltaY}}`;
            }}
            for (const {{screen}} of samples) {{
                const element = document.elementFromPoint(screen.x, screen.y);
                if (element === hit || element?.closest?.(".graph-edge-hit") === hit) {{
                    return "OK";
                }}
            }}
            const details = samples.map(({{ratio, screen}}) => {{
                const element = document.elementFromPoint(screen.x, screen.y);
                return {{
                    ratio,
                    x: Math.round(screen.x),
                    y: Math.round(screen.y),
                    tag: element?.tagName ?? "",
                    className: element?.getAttribute?.("class") ?? "",
                    source: element?.dataset?.source ?? "",
                    target: element?.dataset?.target ?? ""
                }};
            }});
            return `hover target mismatch samples=${{JSON.stringify(details)}} path=${{edge.getAttribute("d")}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge hover target must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to own its hover target: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "branch group {string} has {int} initiator callout and {int} finalizer callout")]
async fn then_branch_group_has_callouts(
    world: &mut ScenarioWorld,
    schema: String,
    initiator_count: usize,
    finalizer_count: usize,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let schema = expand_placeholders(world, &schema);
    let script = format!(
        r#"
        () => {{
            const groups = Array
                .from(document.querySelectorAll(".graph-branch-body"))
                .filter((path) => path.dataset.schema === {schema:?});
            return groups.some((path) =>
                Number(path.dataset.leftCallouts) === {initiator_count}
                    && Number(path.dataset.rightCallouts) === {finalizer_count}
            );
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let has_callouts = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("branch group callouts must be readable");
        if has_callouts {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected branch group '{schema}' to have {initiator_count} initiator callout(s) and \
             {finalizer_count} finalizer callout(s)"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "branch group {string} body does not overlap graph item {string}")]
async fn then_branch_group_body_does_not_overlap_graph_item(
    world: &mut ScenarioWorld,
    schema: String,
    item: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let schema = expand_placeholders(world, &schema);
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            const paths = Array
                .from(document.querySelectorAll(".graph-branch-body"))
                .filter((path) => path.dataset.schema === {schema:?});
            if (!item || paths.length === 0) {{
                return false;
            }}
            const itemBox = item.getBoundingClientRect();
            const bodyBox = (path) => {{
                const matrix = path.getScreenCTM();
                const svg = path.ownerSVGElement;
                if (!matrix || !svg) {{
                    return null;
                }}
                const point = svg.createSVGPoint();
                const x = Number(path.dataset.x);
                const y = Number(path.dataset.y);
                const width = Number(path.dataset.width);
                const height = Number(path.dataset.height);
                point.x = x;
                point.y = y;
                const topLeft = point.matrixTransform(matrix);
                point.x = x + width;
                point.y = y + height;
                const bottomRight = point.matrixTransform(matrix);
                return {{
                    left: Math.min(topLeft.x, bottomRight.x),
                    right: Math.max(topLeft.x, bottomRight.x),
                    top: Math.min(topLeft.y, bottomRight.y),
                    bottom: Math.max(topLeft.y, bottomRight.y),
                }};
            }};
            return paths.some((path) => {{
                const body = bodyBox(path);
                return body
                    && (
                        body.right <= itemBox.left
                        || itemBox.right <= body.left
                        || body.bottom <= itemBox.top
                        || itemBox.bottom <= body.top
                    );
            }});
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let does_not_overlap = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("branch group body position must be readable");
        if does_not_overlap {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected branch group '{schema}' body not to overlap graph item '{item}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "branch group {string} body overlaps graph item {string}")]
async fn then_branch_group_body_overlaps_graph_item(
    world: &mut ScenarioWorld,
    schema: String,
    item: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let schema = expand_placeholders(world, &schema);
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            const bodies = Array
                .from(document.querySelectorAll(".graph-branch-body"))
                .filter((path) => path.dataset.schema === {schema:?});
            if (!item || bodies.length === 0) {{
                return false;
            }}
            const itemBox = item.getBoundingClientRect();
            const bodyBox = (path) => {{
                const matrix = path.getScreenCTM();
                const svg = path.ownerSVGElement;
                if (!matrix || !svg) {{
                    return null;
                }}
                const point = svg.createSVGPoint();
                const x = Number(path.dataset.x);
                const y = Number(path.dataset.y);
                const width = Number(path.dataset.width);
                const height = Number(path.dataset.height);
                point.x = x;
                point.y = y;
                const topLeft = point.matrixTransform(matrix);
                point.x = x + width;
                point.y = y + height;
                const bottomRight = point.matrixTransform(matrix);
                return {{
                    left: Math.min(topLeft.x, bottomRight.x),
                    right: Math.max(topLeft.x, bottomRight.x),
                    top: Math.min(topLeft.y, bottomRight.y),
                    bottom: Math.max(topLeft.y, bottomRight.y),
                }};
            }};
            return bodies.some((path) => {{
                const body = bodyBox(path);
                return body
                    && body.right > itemBox.left
                    && itemBox.right > body.left
                    && body.bottom > itemBox.top
                    && itemBox.bottom > body.top;
            }});
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let overlaps = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("branch group body position must be readable");
        if overlaps {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected branch group '{schema}' body to overlap graph item '{item}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "branch group {string} left callout points to graph item {string}")]
async fn then_branch_group_left_callout_points_to_graph_item(
    world: &mut ScenarioWorld,
    schema: String,
    item: String,
) {
    then_branch_group_callout_points_to_graph_item(world, schema, item, "left").await;
}

#[then(expr = "branch group {string} right callout points to graph item {string}")]
async fn then_branch_group_right_callout_points_to_graph_item(
    world: &mut ScenarioWorld,
    schema: String,
    item: String,
) {
    then_branch_group_callout_points_to_graph_item(world, schema, item, "right").await;
}

async fn then_branch_group_callout_points_to_graph_item(
    world: &mut ScenarioWorld,
    schema: String,
    item: String,
    side: &str,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let schema = expand_placeholders(world, &schema);
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            const body = Array
                .from(document.querySelectorAll(".graph-branch-body"))
                .find((path) => path.dataset.schema === {schema:?});
            if (!item || !body) {{
                return false;
            }}
            const itemBox = item.getBoundingClientRect();
            const bodyMatrix = body.getScreenCTM();
            const svg = body.ownerSVGElement;
            if (!bodyMatrix || !svg) {{
                return false;
            }}
            const point = svg.createSVGPoint();
            const x = Number(body.dataset.x);
            const y = Number(body.dataset.y);
            const width = Number(body.dataset.width);
            const height = Number(body.dataset.height);
            const side = {side:?};
            point.x = side === "left" ? x : x + width;
            point.y = y + height / 2;
            const bodySide = point.matrixTransform(bodyMatrix).x;
            const targetX = side === "left" ? itemBox.right : itemBox.left;
            const targetY = itemBox.top + itemBox.height / 2;
            const callout = Array
                .from(document.querySelectorAll(".graph-branch-callout"))
                .some((path) => {{
                    const segments = path.getAttribute("d").match(/-?\d+(?:\.\d+)?/g);
                    if (!segments || segments.length < 6) {{
                        return false;
                    }}
                    const tip = svg.createSVGPoint();
                    tip.x = Number(segments[2]);
                    tip.y = Number(segments[3]);
                    const tipOnScreen = tip.matrixTransform(path.getScreenCTM());
                    return Math.abs(tipOnScreen.x - targetX) <= 3
                        && Math.abs(tipOnScreen.y - targetY) <= itemBox.height / 2 + 3;
                }});
            const calloutLength = Math.abs(bodySide - targetX);
            return callout && calloutLength >= 2 && calloutLength <= 64;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let points_to_item = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("branch group callout position must be readable");
        if points_to_item {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected branch group '{schema}' {side} callout to point to graph item '{item}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(
    expr = "the last command output metric {string} {string} relay {string} physical node \
            {string} has values"
)]
async fn then_last_command_output_metric_has_values(
    world: &mut ScenarioWorld,
    metric: String,
    direction: String,
    relay: String,
    physical_node: String,
    #[step] step: &Step,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    let prefix = format!("{metric} {direction} relay={relay} physical_node={physical_node}");
    let line = output
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("expected metric line starting with '{prefix}', got: {output}"));
    for assertion in expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let Some((field, expected)) = assertion.split_once('=') else {
            panic!("unsupported metric value assertion '{assertion}'");
        };
        let Some(actual) = metric_line_value(line, field.trim()) else {
            panic!(
                "expected field '{}' in metric line '{}'",
                field.trim(),
                line
            );
        };
        assert_eq!(
            actual,
            expected.trim(),
            "expected field '{}' to equal '{}' in metric line '{}'",
            field.trim(),
            expected.trim(),
            line
        );
    }
}

#[then(
    expr = "the last command output metric {string} {string} relay {string} on any physical node \
            has values"
)]
async fn then_last_command_output_metric_on_any_physical_node_has_values(
    world: &mut ScenarioWorld,
    metric: String,
    direction: String,
    relay: String,
    #[step] step: &Step,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    let prefix = format!("{metric} {direction} relay={relay} physical_node=");
    let line = output
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("expected metric line starting with '{prefix}', got: {output}"));
    for assertion in expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let Some((field, expected)) = assertion.split_once('=') else {
            panic!("unsupported metric value assertion '{assertion}'");
        };
        let Some(actual) = metric_line_value(line, field.trim()) else {
            panic!(
                "expected field '{}' in metric line '{}'",
                field.trim(),
                line
            );
        };
        assert_eq!(
            actual,
            expected.trim(),
            "expected field '{}' to equal '{}' in metric line '{}'",
            field.trim(),
            expected.trim(),
            line
        );
    }
}

#[then(
    expr = "the last command output metric {string} {string} relay {string} on any physical node \
            has numeric values"
)]
async fn then_last_command_output_metric_on_any_physical_node_has_numeric_values(
    world: &mut ScenarioWorld,
    metric: String,
    direction: String,
    relay: String,
    #[step] step: &Step,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    let prefix = format!("{metric} {direction} relay={relay} physical_node=");
    let assertions = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(parse_numeric_metric_assertion)
        .collect::<Vec<_>>();
    let lines = output
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with(&prefix))
        .collect::<Vec<_>>();

    assert!(
        !lines.is_empty(),
        "expected metric line starting with '{prefix}', got: {output}"
    );

    for line in lines {
        if assertions
            .iter()
            .all(|assertion| assertion.matches_metric_line(line))
        {
            return;
        }
    }

    panic!(
        "no metric line starting with '{prefix}' satisfied numeric assertions {:?}. output: \
         {output}",
        assertions
    );
}

fn metric_line_value<'a>(line: &'a str, field: &str) -> Option<&'a str> {
    line.split_whitespace()
        .filter_map(|part| part.split_once('='))
        .find_map(|(name, value)| (name == field).then_some(value))
}

#[derive(Debug)]
struct NumericMetricAssertion {
    field: String,
    op: NumericMetricOperator,
    expected: f64,
}

impl NumericMetricAssertion {
    fn matches_metric_line(&self, line: &str) -> bool {
        let Some(actual) = metric_line_value(line, &self.field).and_then(|value| {
            value
                .parse::<f64>()
                .ok()
                .or_else(|| (value == "-").then_some(f64::NAN))
        }) else {
            return false;
        };
        self.op.matches(actual, self.expected)
    }
}

#[derive(Debug)]
enum NumericMetricOperator {
    Equal,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

impl NumericMetricOperator {
    fn matches(&self, actual: f64, expected: f64) -> bool {
        match self {
            Self::Equal => (actual - expected).abs() < f64::EPSILON,
            Self::GreaterThan => actual > expected,
            Self::LessThan => actual < expected,
            Self::GreaterThanOrEqual => actual >= expected,
            Self::LessThanOrEqual => actual <= expected,
        }
    }
}

fn parse_numeric_metric_assertion(assertion: &str) -> NumericMetricAssertion {
    let operators = [
        (">=", NumericMetricOperator::GreaterThanOrEqual),
        ("<=", NumericMetricOperator::LessThanOrEqual),
        (">", NumericMetricOperator::GreaterThan),
        ("<", NumericMetricOperator::LessThan),
        ("=", NumericMetricOperator::Equal),
    ];
    for (symbol, op) in operators {
        if let Some((field, expected)) = assertion.split_once(symbol) {
            return NumericMetricAssertion {
                field: field.trim().to_string(),
                op,
                expected: expected.trim().parse().unwrap_or_else(|error| {
                    panic!("invalid numeric assertion '{assertion}': {error}")
                }),
            };
        }
    }
    panic!("unsupported numeric metric assertion '{assertion}'");
}

#[then("the last command output does not contain")]
async fn then_last_command_output_does_not_contain(world: &mut ScenarioWorld, #[step] step: &Step) {
    let unexpected = expand_placeholders(world, docstring(step));
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    assert!(
        !output.contains(unexpected.trim()),
        "did not expect command output fragment {} in output, got: {output}",
        unexpected.trim()
    );
}

fn owner_from_describe_output(output: &str) -> Option<&str> {
    output
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix("owner: ").map(str::trim))
}

fn replica_nodes_from_describe_output(output: &str) -> Vec<&str> {
    output
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix("replicas: ").map(str::trim))
        .map(|replicas| {
            replicas
                .split(',')
                .map(str::trim)
                .filter(|replica| !replica.is_empty() && *replica != "-")
                .collect()
        })
        .unwrap_or_default()
}

fn scheduled_node_placement_from_status<'a>(
    status: &'a str,
    domain: &str,
    kind: &str,
    name: &str,
) -> Option<(&'a str, Vec<&'a str>)> {
    status.lines().map(str::trim).find_map(|line| {
        let line = line.strip_prefix("- ")?;
        let mut line_domain = None;
        let mut line_kind = None;
        let mut line_name = None;
        let mut owner = None;
        let mut replicas = None;

        for field in line.split_whitespace() {
            if let Some(value) = field.strip_prefix("domain=") {
                line_domain = Some(value);
            } else if let Some(value) = field.strip_prefix("kind=") {
                line_kind = Some(value);
            } else if let Some(value) = field.strip_prefix("name=") {
                line_name = Some(value);
            } else if let Some(value) = field.strip_prefix("owner=") {
                owner = Some(value);
            } else if let Some(value) = field.strip_prefix("replicas=") {
                replicas = Some(value);
            }
        }

        if line_domain == Some(domain) && line_kind == Some(kind) && line_name == Some(name) {
            let replica_nodes = replicas
                .filter(|value| *value != "-")
                .map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|replica| !replica.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            owner.map(|owner| (owner, replica_nodes))
        } else {
            None
        }
    })
}

#[then(expr = "the last command output owner is saved as placeholder {string}")]
async fn then_last_command_output_owner_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before saving its owner");
    let owner = owner_from_describe_output(output)
        .unwrap_or_else(|| panic!("last command output must contain an owner line, got: {output}"))
        .to_string();
    world.placeholders.insert(placeholder, owner);
}

#[then(expr = "the first replica in the last command output is saved as placeholder {string}")]
async fn then_first_replica_in_last_command_output_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before saving its replica");
    let replica = replica_nodes_from_describe_output(output)
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            panic!("last command output must contain at least one replica, got: {output}")
        })
        .to_string();
    world.placeholders.insert(placeholder, replica);
}

#[then(
    expr = "the last cluster status owner for scheduled {string} {string} is saved as placeholder \
            {string}"
)]
async fn then_last_cluster_status_scheduled_owner_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    kind: String,
    name: String,
    placeholder: String,
) {
    let kind = expand_placeholders(world, &kind);
    let name = expand_placeholders(world, &name);
    let output = world
        .last_command_output
        .as_deref()
        .expect("a cluster status output must exist before saving its scheduled owner");
    let (owner, _) = scheduled_node_placement_from_status(output, &world.domain, &kind, &name)
        .unwrap_or_else(|| {
            panic!(
                "last command output must contain scheduled {kind} {name} placement for domain \
                 '{}', got: {output}",
                world.domain
            )
        });
    world.placeholders.insert(placeholder, owner.to_string());
}

#[then(
    expr = "the first replica for scheduled {string} {string} in the last cluster status is saved \
            as placeholder {string}"
)]
async fn then_last_cluster_status_scheduled_first_replica_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    kind: String,
    name: String,
    placeholder: String,
) {
    let kind = expand_placeholders(world, &kind);
    let name = expand_placeholders(world, &name);
    let output = world
        .last_command_output
        .as_deref()
        .expect("a cluster status output must exist before saving its scheduled replica");
    let (_, replicas) = scheduled_node_placement_from_status(output, &world.domain, &kind, &name)
        .unwrap_or_else(|| {
            panic!(
                "last command output must contain scheduled {kind} {name} placement for domain \
                 '{}', got: {output}",
                world.domain
            )
        });
    let replica = replicas
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("scheduled {kind} {name} must contain at least one replica"));
    world.placeholders.insert(placeholder, replica.to_string());
}

#[then(expr = "a node other than placeholder {string} is saved as placeholder {string}")]
async fn then_a_node_other_than_placeholder_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    excluded_placeholder: String,
    placeholder: String,
) {
    let excluded = world
        .placeholders
        .get(&excluded_placeholder)
        .unwrap_or_else(|| {
            panic!("placeholder '{excluded_placeholder}' must be saved before assertion")
        });
    let node_id = world
        .cluster()
        .node_ids()
        .into_iter()
        .find(|node_id| node_id != excluded)
        .unwrap_or_else(|| {
            panic!("no node exists other than placeholder '{excluded_placeholder}'")
        });
    world.placeholders.insert(placeholder, node_id);
}

#[then(expr = "the last command output owner equals placeholder {string}")]
async fn then_last_command_output_owner_equals_placeholder(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let expected = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before assertion"));
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before owner assertion");
    let actual = owner_from_describe_output(output)
        .unwrap_or_else(|| panic!("last command output must contain an owner line, got: {output}"));
    assert_eq!(
        actual, expected,
        "expected owner placeholder '{placeholder}' to match last command output owner"
    );
}

#[when("these NSPL commands are executed on a node that is not the last described hash map owner")]
async fn when_these_nspl_commands_are_executed_on_non_hash_map_owner(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_server_error = None;
    let owner = world
        .last_command_output
        .as_deref()
        .and_then(owner_from_describe_output)
        .expect("last command output must be DESCRIBE HASH MAP output with an owner")
        .to_string();
    let node_id = world
        .cluster()
        .node_other_than(&owner)
        .expect("cluster must contain a node other than the hash map owner");
    let commands = expand_placeholders(world, docstring(step));
    let session = execute_nspl_commands_on_node(world, &node_id, &commands)
        .await
        .expect("failed to execute NSPL command on non-owner node");
    world.active_session = Some(session);
    world.active_session_node = Some(node_id);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[when(
    "these NSPL commands are executed on a node that is not a holder of the last described hash \
     map"
)]
async fn when_these_nspl_commands_are_executed_on_non_hash_map_holder(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_server_error = None;
    let output = world
        .last_command_output
        .as_deref()
        .expect("last command output must be DESCRIBE HASH MAP output");
    let owner = owner_from_describe_output(output)
        .expect("last command output must be DESCRIBE HASH MAP output with an owner")
        .to_string();
    let replicas = replica_nodes_from_describe_output(output)
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let node_id = world
        .cluster()
        .node_ids()
        .into_iter()
        .find(|node_id| node_id != &owner && !replicas.contains(node_id))
        .unwrap_or_else(|| {
            panic!(
                "cluster must contain a node that is neither hash map owner '{owner}' nor \
                 replicas {:?}",
                replicas
            )
        });
    let commands = expand_placeholders(world, docstring(step));
    let session = execute_nspl_commands_on_node(world, &node_id, &commands)
        .await
        .expect("failed to execute NSPL command on non-holder node");
    world.active_session = Some(session);
    world.active_session_node = Some(node_id);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[then(expr = "within {string} DESCRIBE INGESTOR {string} on the leader node contains")]
async fn then_within_duration_describe_ingestor_on_leader_contains(
    world: &mut ScenarioWorld,
    duration: String,
    ingestor: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let ingestor = expand_placeholders(world, &ingestor);
    let expected = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let leader = current_leader_node(world).await;
        let output =
            run_nspl_commands_on_node(world, &leader, &format!("DESCRIBE INGESTOR {ingestor};"))
                .await
                .expect("describe ingestor command must succeed");
        world.last_command_output = Some(output.clone());
        if output.contains(expected.trim()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for DESCRIBE INGESTOR {ingestor} to contain {}. last output: \
             {output}",
            expected.trim()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "within {string} DESCRIBE EMITTER {string} on the leader node contains")]
async fn then_within_duration_describe_emitter_on_leader_contains(
    world: &mut ScenarioWorld,
    duration: String,
    emitter: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let emitter = expand_placeholders(world, &emitter);
    let expected = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let leader = current_leader_node(world).await;
        let output =
            run_nspl_commands_on_node(world, &leader, &format!("DESCRIBE EMITTER {emitter};"))
                .await
                .expect("describe emitter command must succeed");
        world.last_command_output = Some(output.clone());
        if output.contains(expected.trim()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for DESCRIBE EMITTER {emitter} to contain {}. last output: {output}",
            expected.trim()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "node {string} eventually reports status containing {string}")]
async fn then_node_eventually_reports_status_containing(
    world: &mut ScenarioWorld,
    node_id: String,
    fragment: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster()
        .wait_for_status_contains(&node_id, &expand_placeholders(world, &fragment))
        .await
        .expect("cluster status fragment did not appear");
}

#[then(
    expr = "within {string} node {string} eventually reports deduplicator {string} owner equals \
            placeholder {string}"
)]
async fn then_within_duration_node_eventually_reports_deduplicator_owner_equals_placeholder(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    deduplicator: String,
    placeholder: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let deduplicator = expand_placeholders(world, &deduplicator);
    let expected = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before assertion"))
        .clone();
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(
            world,
            &node_id,
            &format!("DESCRIBE DEDUPLICATOR {deduplicator};"),
        )
        .await
        {
            Ok(output) => {
                world.last_command_output = Some(output.clone());
                if owner_from_describe_output(&output) == Some(expected.as_str()) {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(error),
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for deduplicator '{deduplicator}' owner to equal '{expected}'. \
             last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} node {string} eventually reports deduplicator {string} owner \
            different from placeholder {string}"
)]
async fn then_within_duration_node_eventually_reports_deduplicator_owner_different_from_placeholder(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    deduplicator: String,
    placeholder: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let deduplicator = expand_placeholders(world, &deduplicator);
    let unexpected = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before assertion"))
        .clone();
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(
            world,
            &node_id,
            &format!("DESCRIBE DEDUPLICATOR {deduplicator};"),
        )
        .await
        {
            Ok(output) => {
                world.last_command_output = Some(output.clone());
                if owner_from_describe_output(&output)
                    .is_some_and(|owner| owner != unexpected.as_str())
                {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(error),
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for deduplicator '{deduplicator}' owner to differ from \
             '{unexpected}'. last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} node {string} eventually reports scheduled {string} {string} owner \
            equals placeholder {string}"
)]
async fn then_within_duration_node_eventually_reports_scheduled_owner_equals_placeholder(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    kind: String,
    name: String,
    placeholder: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let kind = expand_placeholders(world, &kind);
    let name = expand_placeholders(world, &name);
    let expected = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before assertion"))
        .clone();
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(world, &node_id, "SHOW CLUSTER STATUS;").await {
            Ok(output) => {
                world.last_command_output = Some(output.clone());
                if scheduled_node_placement_from_status(&output, &world.domain, &kind, &name)
                    .is_some_and(|(owner, _)| owner == expected)
                {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(error),
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for scheduled {kind} {name} owner to equal '{expected}'. last \
             output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} node {string} eventually reports scheduled {string} {string} owner \
            different from placeholder {string}"
)]
async fn then_within_duration_node_eventually_reports_scheduled_owner_different_from_placeholder(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    kind: String,
    name: String,
    placeholder: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let kind = expand_placeholders(world, &kind);
    let name = expand_placeholders(world, &name);
    let unexpected = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before assertion"))
        .clone();
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(world, &node_id, "SHOW CLUSTER STATUS;").await {
            Ok(output) => {
                world.last_command_output = Some(output.clone());
                if scheduled_node_placement_from_status(&output, &world.domain, &kind, &name)
                    .is_some_and(|(owner, _)| owner != unexpected)
                {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(error),
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for scheduled {kind} {name} owner to differ from '{unexpected}'. \
             last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "node {string} observability path {string} eventually responds with {int} and {string}"
)]
async fn then_node_observability_path_eventually_responds(
    world: &mut ScenarioWorld,
    node_id: String,
    path: String,
    expected_status: u16,
    expected_body: String,
) {
    world
        .cluster()
        .wait_for_observability_response(&node_id, &path, expected_status, &expected_body)
        .await
        .expect("observability endpoint did not return the expected response");
}

#[then(
    expr = "node {string} observability path {string} eventually responds with {int} and contains \
            {string}"
)]
async fn then_node_observability_path_eventually_responds_containing(
    world: &mut ScenarioWorld,
    node_id: String,
    path: String,
    expected_status: u16,
    expected_body_fragment: String,
) {
    world
        .cluster()
        .wait_for_observability_response_containing(
            &node_id,
            &path,
            expected_status,
            &expected_body_fragment,
        )
        .await
        .expect("observability endpoint did not return the expected response");
}

#[then(expr = "node {string} observability metric {string} with labels eventually equals {int}")]
async fn then_node_observability_metric_with_labels_eventually_equals(
    world: &mut ScenarioWorld,
    node_id: String,
    metric_name: String,
    expected_value: i64,
    #[step] step: &Step,
) {
    let node_id = expand_placeholders(world, &node_id);
    let metric_name = expand_placeholders(world, &metric_name);
    let label_fragments = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| expand_placeholders(world, line))
        .collect::<Vec<_>>();
    world
        .cluster()
        .wait_for_observability_metric_value(
            &node_id,
            &metric_name,
            &label_fragments,
            expected_value,
        )
        .await
        .expect("observability endpoint did not report the expected metric value");
}

#[then(expr = "within {string} node {string} eventually reports describe relay as {string}")]
async fn then_within_duration_node_eventually_reports_describe_stream_as(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    expected: String,
    #[step] step: &Step,
) {
    let timeout =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let commands = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + timeout;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(world, &node_id, &commands).await {
            Ok(output) if output.contains(expected.as_str()) => {
                world.last_command_output = Some(output);
                return;
            }
            Ok(output) => {
                world.last_command_output = Some(output);
            }
            Err(error) => {
                world.last_command_error = Some(error.clone());
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for node '{node_id}' to report describe relay as '{expected}'. \
             last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error,
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} node {string} eventually reports describe ingestor {string} as \
            {string}"
)]
async fn then_within_duration_node_eventually_reports_describe_ingestor_as(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    ingestor: String,
    expected: String,
) {
    let timeout =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let ingestor = expand_placeholders(world, &ingestor);
    let expected = expand_placeholders(world, &expected);
    let commands = format!("DESCRIBE INGESTOR {ingestor};");
    let deadline = Instant::now() + timeout;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(world, &node_id, &commands).await {
            Ok(output) if output.contains(expected.as_str()) => {
                world.last_command_output = Some(output);
                return;
            }
            Ok(output) => {
                world.last_command_output = Some(output);
            }
            Err(error) => {
                world.last_command_error = Some(error.clone());
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for node '{node_id}' to report describe ingestor '{ingestor}' as \
             '{expected}'. last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error,
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "within {string} node {string} eventually reports describe resource as {string}")]
async fn then_within_duration_node_eventually_reports_describe_resource_as(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    expected: String,
    #[step] step: &Step,
) {
    let timeout =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let commands = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + timeout;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(world, &node_id, &commands).await {
            Ok(output) if output.contains(expected.as_str()) => {
                world.last_command_output = Some(output);
                return;
            }
            Ok(output) => {
                world.last_command_output = Some(output);
            }
            Err(error) => {
                world.last_command_error = Some(error.clone());
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for node '{node_id}' to report describe resource as '{expected}'. \
             last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error,
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} node {string} eventually reports materialized state for relay \
            {string} containing"
)]
async fn then_within_duration_node_eventually_reports_materialized_state_containing(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    relay: String,
    #[step] step: &Step,
) {
    let timeout =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let expected = expand_placeholders(world, docstring(step));
    let command = format!(
        "SHOW RELAY {} MATERIALIZED STATE;",
        expand_placeholders(world, &relay)
    );
    let deadline = Instant::now() + timeout;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(world, &node_id, &command).await {
            Ok(output) if output.contains(expected.trim()) => {
                world.last_command_output = Some(output);
                return;
            }
            Ok(output) => {
                world.last_command_output = Some(output);
            }
            Err(error) => {
                world.last_command_error = Some(error.clone());
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for node '{node_id}' to report materialized state containing {}. \
             last output: {:?}, last error: {:?}",
            expected.trim(),
            world.last_command_output,
            world.last_command_error,
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[given(expr = "RabbitMQ queue {string} exists")]
async fn given_rabbitmq_queue_exists(world: &mut ScenarioWorld, queue: String) {
    let queue = expand_placeholders(world, &queue);
    world
        .cluster()
        .ensure_rabbitmq_queue(&queue)
        .await
        .expect("failed to declare rabbitmq queue");
}

#[given(expr = "Kafka topic {string} exists with {int} partitions")]
async fn given_kafka_topic_exists_with_partitions(
    world: &mut ScenarioWorld,
    topic: String,
    partitions: usize,
) {
    let topic = expand_placeholders(world, &topic);
    let partitions = i32::try_from(partitions).expect("partition count must fit i32");
    world
        .cluster()
        .ensure_kafka_topic_partitions(&topic, partitions)
        .await
        .expect("failed to create kafka topic");
}

#[given(expr = "SQS queue {string} exists")]
async fn given_sqs_queue_exists(world: &mut ScenarioWorld, queue: String) {
    let queue = expand_placeholders(world, &queue);
    world
        .cluster()
        .ensure_sqs_queue(&queue)
        .await
        .expect("failed to declare sqs queue");
}

#[given(expr = "TLS SQS queue {string} exists")]
async fn given_tls_sqs_queue_exists(world: &mut ScenarioWorld, queue: String) {
    let queue = expand_placeholders(world, &queue);
    world
        .cluster()
        .ensure_sqs_queue_tls(&queue)
        .await
        .expect("failed to declare tls sqs queue");
}

#[given(expr = "Iceberg table {string} exists at {string} with columns")]
async fn given_iceberg_table_exists_at_with_columns(
    world: &mut ScenarioWorld,
    table: String,
    location: String,
    #[step] step: &Step,
) {
    let fixture = IcebergTableFixture::from_step(world, table, location, docstring(step));
    fixture
        .ensure()
        .await
        .expect("failed to create Iceberg table");
}

#[given(expr = "Kafka topic {string} is observed")]
async fn given_kafka_topic_is_observed(world: &mut ScenarioWorld, topic: String) {
    let topic = expand_placeholders(world, &topic);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_kafka(&topic)
            .await
            .expect("failed to observe kafka topic"),
    );
}

#[given(expr = "Pulsar topic {string} is observed")]
async fn given_pulsar_topic_is_observed(world: &mut ScenarioWorld, topic: String) {
    let topic = expand_placeholders(world, &topic);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_pulsar(&topic)
            .await
            .expect("failed to observe pulsar topic"),
    );
}

#[given(expr = "Pulsar TLS topic {string} is observed")]
async fn given_pulsar_tls_topic_is_observed(world: &mut ScenarioWorld, topic: String) {
    let topic = expand_placeholders(world, &topic);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_pulsar_tls(&topic)
            .await
            .expect("failed to observe pulsar tls topic"),
    );
}

#[given(expr = "RabbitMQ queue {string} is observed")]
async fn given_rabbitmq_queue_is_observed(world: &mut ScenarioWorld, queue: String) {
    let queue = expand_placeholders(world, &queue);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_rabbitmq(&queue)
            .await
            .expect("failed to observe rabbitmq queue"),
    );
}

#[given(expr = "Redis channel {string} is observed")]
async fn given_redis_channel_is_observed(world: &mut ScenarioWorld, channel: String) {
    let channel = expand_placeholders(world, &channel);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_redis(&channel)
            .await
            .expect("failed to observe redis channel"),
    );
}

#[given(expr = "MQTT topic {string} is observed")]
async fn given_mqtt_topic_is_observed(world: &mut ScenarioWorld, topic: String) {
    let topic = expand_placeholders(world, &topic);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_mqtt(&topic)
            .await
            .expect("failed to observe mqtt topic"),
    );
}

#[given(expr = "SQS queue {string} is observed")]
async fn given_sqs_queue_is_observed(world: &mut ScenarioWorld, queue: String) {
    let queue = expand_placeholders(world, &queue);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_sqs(&queue)
            .await
            .expect("failed to observe sqs queue"),
    );
}

#[given(expr = "NATS subject {string} is observed")]
async fn given_nats_subject_is_observed(world: &mut ScenarioWorld, subject: String) {
    let subject = expand_placeholders(world, &subject);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_nats(&subject)
            .await
            .expect("failed to observe nats subject"),
    );
}

#[given(expr = "ZeroMQ emission endpoint {string} is observed")]
async fn given_zeromq_emission_endpoint_is_observed(world: &mut ScenarioWorld, addr: String) {
    let addr = expand_placeholders(world, &addr);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_zeromq(&addr)
            .await
            .expect("failed to observe zeromq endpoint"),
    );
}

#[given(expr = "ClickHouse table {string} exists")]
async fn given_clickhouse_table_exists(world: &mut ScenarioWorld, table: String) {
    let table = expand_placeholders(world, &table);
    clickhouse_post(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .expect("failed to drop ClickHouse table");
    clickhouse_post(&format!(
        "CREATE TABLE {table} (clickhouse_user_id UInt32, clickhouse_now String, \
         clickhouse_action String) ENGINE = Memory"
    ))
    .await
    .expect("failed to create ClickHouse table");
    world.clickhouse_table = Some(table);
    world.clickhouse_tls = false;
}

#[given(expr = "ClickHouse TLS table {string} exists")]
async fn given_clickhouse_tls_table_exists(world: &mut ScenarioWorld, table: String) {
    let table = expand_placeholders(world, &table);
    clickhouse_tls_post(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .expect("failed to drop ClickHouse TLS table");
    clickhouse_tls_post(&format!(
        "CREATE TABLE {table} (clickhouse_user_id UInt32, clickhouse_now String, \
         clickhouse_action String) ENGINE = Memory"
    ))
    .await
    .expect("failed to create ClickHouse TLS table");
    world.clickhouse_table = Some(table);
    world.clickhouse_tls = true;
}

#[given(expr = "Postgres table {string} exists")]
async fn given_postgres_table_exists(world: &mut ScenarioWorld, table: String) {
    prepare_postgres_table(world, table, false).await;
}

#[given(expr = "Postgres table {string} with primary key exists")]
async fn given_postgres_table_with_primary_key_exists(world: &mut ScenarioWorld, table: String) {
    prepare_postgres_table_with_primary_key(world, table, false).await;
}

#[given(expr = "Postgres TLS table {string} exists")]
async fn given_postgres_tls_table_exists(world: &mut ScenarioWorld, table: String) {
    prepare_postgres_table(world, table, true).await;
}

async fn prepare_postgres_table(world: &mut ScenarioWorld, table: String, tls: bool) {
    prepare_postgres_table_schema(world, table, tls, false).await;
}

async fn prepare_postgres_table_with_primary_key(
    world: &mut ScenarioWorld,
    table: String,
    tls: bool,
) {
    prepare_postgres_table_schema(world, table, tls, true).await;
}

async fn prepare_postgres_table_schema(
    world: &mut ScenarioWorld,
    table: String,
    tls: bool,
    primary_key: bool,
) {
    let table = expand_placeholders(world, &table);
    let client = postgres_client(tls)
        .await
        .expect("failed to connect to Postgres");
    client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .expect("failed to drop Postgres table");
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (postgres_user_id integer, postgres_now text, postgres_action \
             text{})",
            if primary_key {
                ", PRIMARY KEY (postgres_user_id)"
            } else {
                ""
            }
        ))
        .await
        .expect("failed to create Postgres table");
    world.postgres_table = Some(table);
    world.postgres_tls = tls;
}

#[given(expr = "MySQL table {string} exists")]
async fn given_mysql_table_exists(world: &mut ScenarioWorld, table: String) {
    prepare_mysql_table(world, table, false).await;
}

#[given(expr = "MySQL TLS table {string} exists")]
async fn given_mysql_tls_table_exists(world: &mut ScenarioWorld, table: String) {
    prepare_mysql_table(world, table, true).await;
}

#[given(expr = "MySQL table {string} with primary key exists")]
async fn given_mysql_table_with_primary_key_exists(world: &mut ScenarioWorld, table: String) {
    prepare_mysql_table_schema(world, table, false, true).await;
}

async fn prepare_mysql_table(world: &mut ScenarioWorld, table: String, tls: bool) {
    prepare_mysql_table_schema(world, table, tls, false).await;
}

async fn prepare_mysql_table_schema(
    world: &mut ScenarioWorld,
    table: String,
    tls: bool,
    primary_key: bool,
) {
    let table = expand_placeholders(world, &table);
    let pool = mysql_pool(tls).expect("failed to build MySQL pool");
    let mut conn = pool.get_conn().await.expect("failed to connect to MySQL");
    conn.query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .expect("failed to drop MySQL table");
    conn.query_drop(format!(
        "CREATE TABLE `{table}` (mysql_user_id integer{}, mysql_now text, mysql_action text)",
        if primary_key { " PRIMARY KEY" } else { "" }
    ))
    .await
    .expect("failed to create MySQL table");
    drop(conn);
    pool.disconnect()
        .await
        .expect("failed to disconnect MySQL pool");
    world.mysql_table = Some(table);
    world.mysql_tls = tls;
}

#[given(expr = "MongoDB collection {string} exists")]
async fn given_mongodb_collection_exists(world: &mut ScenarioWorld, collection: String) {
    prepare_mongodb_collection(world, collection, false).await;
}

#[given(expr = "MongoDB collection {string} with unique user id exists")]
async fn given_mongodb_collection_with_unique_user_id_exists(
    world: &mut ScenarioWorld,
    collection: String,
) {
    prepare_mongodb_collection_schema(world, collection, false, true).await;
}

#[given(expr = "MongoDB TLS collection {string} exists")]
async fn given_mongodb_tls_collection_exists(world: &mut ScenarioWorld, collection: String) {
    prepare_mongodb_collection(world, collection, true).await;
}

async fn prepare_mongodb_collection(world: &mut ScenarioWorld, collection: String, tls: bool) {
    prepare_mongodb_collection_schema(world, collection, tls, false).await;
}

async fn prepare_mongodb_collection_schema(
    world: &mut ScenarioWorld,
    collection: String,
    tls: bool,
    unique_user_id: bool,
) {
    let collection = expand_placeholders(world, &collection);
    let client = mongodb_client(tls)
        .await
        .expect("failed to connect to MongoDB");
    client
        .database("nervix")
        .collection::<MongoDbDocument>(&collection)
        .drop()
        .await
        .expect("failed to drop MongoDB collection");
    client
        .database("nervix")
        .create_collection(&collection)
        .await
        .expect("failed to create MongoDB collection");
    if unique_user_id {
        client
            .database("nervix")
            .run_command(mongodb_doc! {
                "createIndexes": &collection,
                "indexes": [{
                    "key": { "mongodb_user_id": 1 },
                    "name": "mongodb_user_id_unique",
                    "unique": true,
                }],
            })
            .await
            .expect("failed to create MongoDB unique user id index");
    }
    world.mongodb_collection = Some(collection);
    world.mongodb_tls = tls;
}

#[when(expr = "MQTT message is published to topic {string}")]
async fn when_mqtt_message_is_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    wait_for_mqtt_ingestors_ready(world).await;
    world
        .cluster()
        .publish_mqtt(&topic, &payload)
        .await
        .expect("failed to publish mqtt message");
}

#[when(expr = "MQTT QoS 1 message is published to topic {string}")]
async fn when_mqtt_qos1_message_is_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    wait_for_mqtt_ingestors_ready(world).await;
    world
        .cluster()
        .publish_mqtt_qos1(&topic, &payload)
        .await
        .expect("failed to publish mqtt QoS 1 message");
}

#[when(expr = "RabbitMQ message is published to queue {string}")]
async fn when_rabbitmq_message_is_published(
    world: &mut ScenarioWorld,
    queue: String,
    #[step] step: &Step,
) {
    let queue = expand_placeholders(world, &queue);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_rabbitmq(&queue, &payload)
        .await
        .expect("failed to publish rabbitmq message");
}

#[when(expr = "Redis message is published to channel {string}")]
async fn when_redis_message_is_published(
    world: &mut ScenarioWorld,
    channel: String,
    #[step] step: &Step,
) {
    let channel = expand_placeholders(world, &channel);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_redis(&channel, &payload)
        .await
        .expect("failed to publish redis message");
}

#[when(expr = "Kafka message is published to topic {string}")]
async fn when_kafka_message_is_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_kafka(&topic, &payload)
        .await
        .expect("failed to publish kafka message");
}

#[when(
    expr = "{int} JSON messages with user id {int} are rapidly published to {string} input \
            {string}"
)]
async fn when_json_messages_with_user_id_are_rapidly_published_to_input(
    world: &mut ScenarioWorld,
    count: u64,
    user_id: u32,
    source_kind: String,
    input: String,
) {
    let input = expand_placeholders(world, &input);
    let source_kind = source_kind.to_ascii_uppercase();
    let payload = format!(r#"{{"user_id":{user_id}}}"#);

    let count = count
        .try_into()
        .expect("rapid publish count must fit into usize");
    if source_kind == "MQTT" || source_kind == "MQTT_QOS1" {
        wait_for_mqtt_ingestors_ready(world).await;
    }
    match source_kind.as_str() {
        "KAFKA" => world
            .cluster()
            .publish_kafka_burst(&input, &payload, count)
            .await
            .expect("failed to publish kafka message burst"),
        "MQTT" => world
            .cluster()
            .publish_mqtt_burst(&input, &payload, count)
            .await
            .expect("failed to publish mqtt message burst"),
        "MQTT_QOS1" => world
            .cluster()
            .publish_mqtt_qos1_burst(&input, &payload, count)
            .await
            .expect("failed to publish mqtt QoS 1 message burst"),
        "REDIS" => world
            .cluster()
            .publish_redis_burst(&input, &payload, count)
            .await
            .expect("failed to publish redis message burst"),
        unsupported => panic!("unsupported rapid ingestor input source kind '{unsupported}'"),
    }
}

#[when(expr = "Pulsar message is published to topic {string}")]
async fn when_pulsar_message_is_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_pulsar(&topic, &payload)
        .await
        .expect("failed to publish pulsar message");
}

#[when(expr = "Pulsar TLS message is published to topic {string}")]
async fn when_pulsar_tls_message_is_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_pulsar_tls(&topic, &payload)
        .await
        .expect("failed to publish pulsar tls message");
}

#[when(expr = "Kafka message is published to topic {string} partition {int}")]
async fn when_kafka_message_is_published_to_partition(
    world: &mut ScenarioWorld,
    topic: String,
    partition: usize,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let partition = i32::try_from(partition).expect("partition id must fit i32");
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_kafka_partition(&topic, partition, &payload)
        .await
        .expect("failed to publish kafka message");
}

#[when(expr = "Kafka topic {string} partition count is changed to {int}")]
async fn when_kafka_topic_partition_count_is_changed_to(
    world: &mut ScenarioWorld,
    topic: String,
    partitions: usize,
) {
    let topic = expand_placeholders(world, &topic);
    let partitions = i32::try_from(partitions).expect("partition count must fit i32");
    world
        .cluster()
        .ensure_kafka_topic_partitions(&topic, partitions)
        .await
        .expect("failed to change kafka topic partition count");
}

#[when(expr = "Kafka topic {string} is reset to {int} partitions")]
async fn when_kafka_topic_is_reset_to_partitions(
    world: &mut ScenarioWorld,
    topic: String,
    partitions: usize,
) {
    let topic = expand_placeholders(world, &topic);
    let partitions = i32::try_from(partitions).expect("partition count must fit i32");
    world
        .cluster()
        .reset_kafka_topic_partitions(&topic, partitions)
        .await
        .expect("failed to reset kafka topic partition count");
}

#[when(expr = "SQS message is published to queue {string}")]
async fn when_sqs_message_is_published(
    world: &mut ScenarioWorld,
    queue: String,
    #[step] step: &Step,
) {
    let queue = expand_placeholders(world, &queue);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_sqs(&queue, &payload)
        .await
        .expect("failed to publish sqs message");
}

#[when(expr = "TLS SQS message is published to queue {string}")]
async fn when_tls_sqs_message_is_published(
    world: &mut ScenarioWorld,
    queue: String,
    #[step] step: &Step,
) {
    let queue = expand_placeholders(world, &queue);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_sqs_tls(&queue, &payload)
        .await
        .expect("failed to publish tls sqs message");
}

#[when(expr = "NATS message is published to subject {string}")]
async fn when_nats_message_is_published(
    world: &mut ScenarioWorld,
    subject: String,
    #[step] step: &Step,
) {
    let subject = expand_placeholders(world, &subject);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_nats(&subject, &payload)
        .await
        .expect("failed to publish nats message");
}

#[when("ZeroMQ message is published")]
async fn when_zeromq_message_is_published(world: &mut ScenarioWorld, #[step] step: &Step) {
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_zeromq(&world.zeromq_ingest_addr, &payload)
        .await
        .expect("failed to publish zeromq message");
}

#[when(expr = "websocket message is published to host {string} path {string}")]
async fn when_websocket_message_is_published(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_websocket("node-1", &host, &path, &payload)
        .await
        .expect("failed to publish websocket message");
}

#[when(expr = "websocket text frames are exchanged with host {string} path {string}")]
async fn when_websocket_text_frames_are_exchanged(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let actions = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            if let Some(payload) = line.strip_prefix("EXPECT ") {
                WebsocketExchangeAction::ExpectText(expand_placeholders(world, payload))
            } else if let Some(payload) = line.strip_prefix("SEND ") {
                WebsocketExchangeAction::SendText(expand_placeholders(world, payload))
            } else {
                panic!("websocket exchange lines must start with EXPECT or SEND: {line}");
            }
        })
        .collect::<Vec<_>>();
    world
        .cluster()
        .exchange_websocket_text("node-1", &host, &path, &actions)
        .await
        .expect("failed to exchange websocket text frames");
}

#[when(expr = "websocket message is published to host {string} path {string} and fails")]
async fn when_websocket_message_is_published_and_fails(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let result = world
        .cluster()
        .publish_websocket("node-1", &host, &path, &payload)
        .await;
    assert!(result.is_err(), "expected websocket publish to fail");
}

#[when(
    expr = "secure websocket message is published to host {string} path {string} using CA from \
            resource directory {string}"
)]
async fn when_secure_websocket_message_is_published(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    ca_resource_directory: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let ca_pem = resource_directory_ca_pem(world, &ca_resource_directory);
    world
        .cluster()
        .publish_secure_websocket("node-1", &host, &path, &payload, &ca_pem)
        .await
        .expect("failed to publish secure websocket message");
}

#[when(expr = "websocket message is published to node {string} host {string} path {string}")]
async fn when_websocket_message_is_published_to_node(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_websocket(&node_id, &host, &path, &payload)
        .await
        .expect("failed to publish websocket message");
}

#[when(
    expr = "websocket message is published to node {string} host {string} path {string} and fails"
)]
async fn when_websocket_message_is_published_to_node_and_fails(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let result = world
        .cluster()
        .publish_websocket(&node_id, &host, &path, &payload)
        .await;
    assert!(
        result.is_err(),
        "expected websocket publish to node '{node_id}' to fail"
    );
}

#[when(expr = "JAQ native payload fixture {string} is posted to host {string} path {string}")]
async fn when_jaq_native_payload_fixture_is_posted(
    world: &mut ScenarioWorld,
    fixture: String,
    host: String,
    path: String,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let (payload, content_type) = jaq_native_payload_fixture(&fixture);
    append_cucumber_log_line(&format!(
        "http publish: node=node-1 host={host} path={path} fixture={fixture} \
         content_type={content_type}"
    ));
    world
        .cluster()
        .publish_http_bytes("node-1", &host, &path, &payload, content_type)
        .await
        .expect("failed to post JAQ native payload fixture");
}

#[when(expr = "protobuf payload fixture {string} is posted to host {string} path {string}")]
async fn when_protobuf_payload_fixture_is_posted(
    world: &mut ScenarioWorld,
    fixture: String,
    host: String,
    path: String,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let (payload, content_type) = protobuf_payload_fixture(&fixture);
    append_cucumber_log_line(&format!(
        "http publish: node=node-1 host={host} path={path} fixture={fixture} \
         content_type={content_type}"
    ));
    world
        .cluster()
        .publish_http_bytes("node-1", &host, &path, &payload, content_type)
        .await
        .expect("failed to post protobuf payload fixture");
}

#[when(expr = "http payload is posted to host {string} path {string}")]
async fn when_http_payload_is_posted(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    append_cucumber_log_line(&format!(
        "http publish: node=node-1 host={host} path={path} payload={payload}"
    ));
    world
        .cluster()
        .publish_http("node-1", &host, &path, &payload)
        .await
        .expect("failed to post http payload");
}

#[when(expr = "http payloads are posted concurrently to host {string} path {string}")]
async fn when_http_payloads_are_posted_concurrently(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payloads = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert!(
        !payloads.is_empty(),
        "concurrent http publish step must include at least one payload line"
    );
    append_cucumber_log_line(&format!(
        "http publish concurrent: node=node-1 host={host} path={path} payloads={payloads:?}"
    ));
    let cluster = world.cluster();
    try_join_all(
        payloads
            .iter()
            .map(|payload| cluster.publish_http("node-1", &host, &path, payload)),
    )
    .await
    .expect("failed to post concurrent http payloads");
}

#[when(expr = "{int} sequential metric http payloads are posted to host {string} path {string}")]
async fn when_sequential_metric_http_payloads_are_posted(
    world: &mut ScenarioWorld,
    count: u64,
    host: String,
    path: String,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    append_cucumber_log_line(&format!(
        "http publish sequential metrics: node=node-1 host={host} path={path} count={count}"
    ));
    for value in 1..=count {
        tokio::task::consume_budget().await;
        let payload = format!(r#"{{"value":{value}}}"#);
        world
            .cluster()
            .publish_http("node-1", &host, &path, &payload)
            .await
            .unwrap_or_else(|error| panic!("failed to post http payload {value}: {error}"));
    }
}

#[when(expr = "http payload encoded as {string} is posted to host {string} path {string}")]
async fn when_encoded_http_payload_is_posted(
    world: &mut ScenarioWorld,
    wire_format: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let encoded_payload = encode_http_payload_for_codec(
        &wire_format,
        &payload,
        &world.avro_http_field_order,
        &world.avro_http_optional_fields,
    );
    append_cucumber_log_line(&format!(
        "http publish: node=node-1 host={host} path={path} wire_format={wire_format} \
         payload={payload}"
    ));
    world
        .cluster()
        .publish_http_bytes(
            "node-1",
            &host,
            &path,
            &encoded_payload,
            http_content_type_for_codec(&wire_format),
        )
        .await
        .expect("failed to post encoded http payload");
}

#[when(expr = "http payload is posted to host {string} path {string} and fails")]
async fn when_http_payload_is_posted_and_fails(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    append_cucumber_log_line(&format!(
        "http publish expect-fail: node=node-1 host={host} path={path} payload={payload}"
    ));
    let result = world
        .cluster()
        .publish_http("node-1", &host, &path, &payload)
        .await;
    assert!(result.is_err(), "expected http post to fail");
}

#[when(
    expr = "https payload is posted to host {string} path {string} using CA from resource \
            directory {string}"
)]
async fn when_https_payload_is_posted(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    ca_resource_directory: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let ca_pem = resource_directory_ca_pem(world, &ca_resource_directory);
    append_cucumber_log_line(&format!(
        "https publish: node=node-1 host={host} path={path} payload={payload}"
    ));
    world
        .cluster()
        .publish_https("node-1", &host, &path, &payload, &ca_pem)
        .await
        .expect("failed to post https payload");
}

#[when(expr = "http payload is posted to node {string} with host {string} path {string}")]
async fn when_http_payload_is_posted_to_node(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    append_cucumber_log_line(&format!(
        "http publish: node={node_id} host={host} path={path} payload={payload}"
    ));
    world
        .cluster()
        .publish_http(&node_id, &host, &path, &payload)
        .await
        .expect("failed to post http payload");
}

#[when(expr = "http payload is posted to node {string} with host {string} path {string} and fails")]
async fn when_http_payload_is_posted_to_node_and_fails(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        tokio::task::consume_budget().await;
        let result = world
            .cluster()
            .publish_http(&node_id, &host, &path, &payload)
            .await;
        if result.is_err() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected http post to node '{node_id}' to fail"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then("the relay subscription receives a payload")]
async fn then_stream_subscription_receives_payload(world: &mut ScenarioWorld, #[step] step: &Step) {
    append_cucumber_log_line(&format!(
        "awaiting subscription payload containing {}",
        docstring(step).replace('\n', "\\n")
    ));
    capture_and_assert_subscription_payload(world, docstring(step), false, Duration::from_secs(10))
        .await;
}

#[then(expr = "within {string} the relay subscription receives a payload")]
async fn then_within_stream_subscription_receives_payload(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    append_cucumber_log_line(&format!(
        "awaiting subscription payload within {:?} containing {}",
        duration,
        docstring(step).replace('\n', "\\n")
    ));
    capture_and_assert_subscription_payload(world, docstring(step), false, duration).await;
}

#[then(expr = "node {string} eventually accepts websocket traffic for host {string} path {string}")]
async fn then_node_eventually_accepts_websocket_traffic(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        tokio::task::consume_budget().await;
        match world
            .cluster()
            .publish_websocket(&node_id, &host, &path, &payload)
            .await
        {
            Ok(()) => return,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for websocket endpoint on node '{node_id}': {error}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[then(expr = "node {string} eventually accepts http traffic for host {string} path {string}")]
async fn then_node_eventually_accepts_http_traffic(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        tokio::task::consume_budget().await;
        match world
            .cluster()
            .publish_http(&node_id, &host, &path, &payload)
            .await
        {
            Ok(()) => return,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for http endpoint on node '{node_id}': {error}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[then(
    expr = "within {string} repeatedly posting http payload to host {string} path {string} yields \
            a relay subscription payload"
)]
async fn then_within_duration_repeatedly_posting_http_payload_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let _ = world
            .cluster()
            .publish_http("node-1", &host, &path, &payload)
            .await;

        if try_capture_any_subscription_payload(world, Duration::from_millis(350)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for http payload posted to host '{host}' path '{path}' to reach \
             the relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly posting http payload encoded as {string} to host {string} \
            path {string} yields a relay subscription payload"
)]
async fn then_within_duration_repeatedly_posting_encoded_http_payload_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    wire_format: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let encoded_payload = encode_http_payload_for_codec(
        &wire_format,
        &payload,
        &world.avro_http_field_order,
        &world.avro_http_optional_fields,
    );
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let _ = world
            .cluster()
            .publish_http_bytes(
                "node-1",
                &host,
                &path,
                &encoded_payload,
                http_content_type_for_codec(&wire_format),
            )
            .await;

        if try_capture_any_subscription_payload(world, Duration::from_millis(350)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for {wire_format} payload posted to host '{host}' path '{path}' to \
             reach the relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing Kafka message to topic {string} yields a relay \
            subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_kafka_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    topic: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_kafka(&topic, &payload)
            .await
            .expect("failed to publish kafka message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Kafka message published to topic '{topic}' to reach the relay \
             subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing Kafka message to topic {string} partition {int} \
            yields a relay subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_kafka_message_to_partition_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    topic: String,
    partition: usize,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let topic = expand_placeholders(world, &topic);
    let partition = i32::try_from(partition).expect("partition id must fit i32");
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_kafka_partition(&topic, partition, &payload)
            .await
            .expect("failed to publish kafka message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Kafka message published to topic '{topic}' partition \
             '{partition}' to reach the relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing MQTT message to topic {string} yields a relay \
            subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_mqtt_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    topic: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_mqtt(&topic, &payload)
            .await
            .expect("failed to publish mqtt message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for MQTT message published to topic '{topic}' to reach the relay \
             subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing Pulsar TLS message to topic {string} yields a \
            relay subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_pulsar_tls_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    topic: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_pulsar_tls(&topic, &payload)
            .await
            .expect("failed to publish pulsar tls message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Pulsar TLS message published to topic '{topic}' to reach the \
             relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing Redis message to channel {string} yields a \
            relay subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_redis_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    channel: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let channel = expand_placeholders(world, &channel);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_redis(&channel, &payload)
            .await
            .expect("failed to publish redis message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Redis message published to channel '{channel}' to reach the \
             relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing NATS message to subject {string} yields a relay \
            subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_nats_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    subject: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let subject = expand_placeholders(world, &subject);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_nats(&subject, &payload)
            .await
            .expect("failed to publish nats message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for NATS message published to subject '{subject}' to reach the \
             relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing NATS TLS message to subject {string} yields a \
            relay subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_nats_tls_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    subject: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let subject = expand_placeholders(world, &subject);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_nats_tls(&subject, &payload)
            .await
            .expect("failed to publish nats tls message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for NATS TLS message published to subject '{subject}' to reach the \
             relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing SQS message to queue {string} yields a relay \
            subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_sqs_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    queue: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let queue = expand_placeholders(world, &queue);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_sqs(&queue, &payload)
            .await
            .expect("failed to publish sqs message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for SQS message published to queue '{queue}' to reach the relay \
             subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing TLS SQS message to queue {string} yields a \
            relay subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_tls_sqs_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    queue: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let queue = expand_placeholders(world, &queue);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_sqs_tls(&queue, &payload)
            .await
            .expect("failed to publish tls sqs message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for TLS SQS message published to queue '{queue}' to reach the \
             relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "node {string} eventually forwards websocket traffic for host {string} path {string} \
            to the observed broker"
)]
async fn then_node_eventually_forwards_websocket_traffic_to_observed_broker(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + Duration::from_secs(15);

    loop {
        let _ = world
            .cluster()
            .publish_websocket(&node_id, &host, &path, &payload)
            .await;
        let next_payload = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_payload(Duration::from_millis(250))
            .await;
        match next_payload {
            Ok(Some(observed)) if observed.contains(payload.trim()) => {
                world.last_broker_payload = Some(observed);
                return;
            }
            Ok(_) | Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for node '{node_id}' websocket traffic to reach observed \
                     broker"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[then(
    expr = "node {string} eventually forwards http traffic for host {string} path {string} to the \
            observed broker"
)]
async fn then_node_eventually_forwards_http_traffic_to_observed_broker(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + Duration::from_secs(15);

    loop {
        let _ = world
            .cluster()
            .publish_http(&node_id, &host, &path, &payload)
            .await;
        let next_payload = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_payload(Duration::from_millis(250))
            .await;
        match next_payload {
            Ok(Some(observed)) if observed.contains(payload.trim()) => {
                world.last_broker_payload = Some(observed);
                return;
            }
            Ok(_) | Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for node '{node_id}' http traffic to reach observed broker"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[then(
    expr = "within {string} repeatedly posting http payload to node {string} with host {string} \
            path {string} yields an observed broker payload"
)]
async fn then_within_duration_repeatedly_posting_http_payload_yields_observed_broker_payload(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let _ = world
            .cluster()
            .publish_http(&node_id, &host, &path, &payload)
            .await;
        let next_payload = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_payload(Duration::from_millis(250))
            .await;
        if let Ok(Some(observed)) = next_payload
            && payload_matches_expected(&observed, &payload)
        {
            world.last_broker_payload = Some(observed);
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for http payload posted to node '{node_id}' host '{host}' path \
             '{path}' to reach the observed broker"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "within {string} the relay subscription receives payloads")]
async fn then_within_duration_the_stream_subscription_receives_payloads(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let expected_fragments = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| expand_placeholders(world, line))
        .collect::<Vec<_>>();

    assert!(
        !expected_fragments.is_empty(),
        "step docstring must contain at least one expected payload fragment"
    );

    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let deadline = Instant::now() + duration;
    let mut remaining = expected_fragments
        .iter()
        .fold(BTreeMap::new(), |mut counts, fragment| {
            *counts.entry(fragment.clone()).or_insert(0usize) += 1;
            counts
        });
    let mut observed = Vec::with_capacity(expected_fragments.len());

    while !remaining.is_empty() {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for subscription payloads. expected remaining {:?}, observed {:?}",
            remaining,
            observed
        );
        let wait = deadline.saturating_duration_since(now);
        let event = session
            .try_next_subscription(wait)
            .await
            .expect("failed while waiting for subscription payloads")
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for subscription payloads. expected remaining {:?}, \
                     observed {:?}",
                    remaining, observed
                )
            });
        let payload = event.payload;
        observed.push(payload.clone());
        world.last_subscription_payload = Some(payload.clone());

        if let Some(fragment) = remaining
            .keys()
            .find(|fragment| payload.contains(fragment.as_str()))
            .cloned()
        {
            let count = remaining
                .get_mut(&fragment)
                .expect("matched fragment must be present in remaining set");
            *count -= 1;
            if *count == 0 {
                remaining.remove(&fragment);
            }
        }
    }
}

#[then(expr = "within {string} the relay subscription receives payloads in order")]
async fn then_within_duration_the_stream_subscription_receives_payloads_in_order(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let expected_fragments = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| expand_placeholders(world, line))
        .collect::<Vec<_>>();

    assert!(
        !expected_fragments.is_empty(),
        "step docstring must contain at least one expected payload fragment"
    );

    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let deadline = Instant::now() + duration;
    let mut observed = Vec::with_capacity(expected_fragments.len());

    for expected_fragment in &expected_fragments {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for subscription payload fragment {:?}. observed {:?}",
            expected_fragment,
            observed
        );
        let wait = deadline.saturating_duration_since(now);
        let event = session
            .try_next_subscription(wait)
            .await
            .expect("failed while waiting for subscription payloads")
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for subscription payload fragment {:?}. observed {:?}",
                    expected_fragment, observed
                )
            });
        let payload = event.payload;
        observed.push(payload.clone());
        world.last_subscription_payload = Some(payload.clone());
        assert!(
            payload.contains(expected_fragment),
            "expected next subscription payload to contain {:?}, got {:?}; observed {:?}",
            expected_fragment,
            payload,
            observed
        );
    }
}

#[then(expr = "within {string} the relay subscription receives payloads containing all fragments")]
async fn then_within_duration_the_stream_subscription_receives_payloads_containing_all_fragments(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let expected_fragment_sets = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            line.split('|')
                .map(str::trim)
                .filter(|fragment| !fragment.is_empty())
                .map(|fragment| expand_placeholders(world, fragment))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert!(
        !expected_fragment_sets.is_empty(),
        "step docstring must contain at least one expected payload fragment set"
    );

    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let deadline = Instant::now() + duration;
    let mut remaining = expected_fragment_sets;
    let mut observed = Vec::new();

    while !remaining.is_empty() {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for subscription payload fragment sets. expected remaining {:?}, \
             observed {:?}",
            remaining,
            observed
        );
        let wait = deadline.saturating_duration_since(now);
        let event = session
            .try_next_subscription(wait)
            .await
            .expect("failed while waiting for subscription payloads")
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for subscription payload fragment sets. expected remaining \
                     {:?}, observed {:?}",
                    remaining, observed
                )
            });
        let payload = event.payload;
        observed.push(payload.clone());
        world.last_subscription_payload = Some(payload.clone());

        if let Some(index) = remaining
            .iter()
            .position(|fragments| fragments.iter().all(|fragment| payload.contains(fragment)))
        {
            remaining.remove(index);
        }
    }
}

#[then("the relay subscription does not receive a payload")]
async fn then_stream_subscription_does_not_receive_a_payload(world: &mut ScenarioWorld) {
    assert_no_subscription_payload_within(world, Duration::from_secs(3)).await;
}

#[then(expr = "the relay subscription does not receive a payload within {string}")]
async fn then_stream_subscription_does_not_receive_a_payload_within(
    world: &mut ScenarioWorld,
    duration: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    assert_no_subscription_payload_within(world, duration).await;
}

async fn assert_no_subscription_payload_within(world: &mut ScenarioWorld, duration: Duration) {
    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let event = session
        .try_next_subscription(duration)
        .await
        .expect("failed while waiting for absence of subscription payload");

    assert!(
        event.is_none(),
        "expected no subscription payload, got: {:?}",
        event.map(|value| value.payload)
    );
}

#[then("the relay subscription receives a payload with topic key")]
async fn then_stream_subscription_receives_payload_with_topic_key(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    capture_and_assert_subscription_payload(world, docstring(step), true, Duration::from_secs(10))
        .await;
}

#[then(expr = "the last relay subscription payload contains key fragment {string}")]
async fn then_last_stream_subscription_payload_contains_key_fragment(
    world: &mut ScenarioWorld,
    expected_key_fragment: String,
) {
    let payload = world
        .last_subscription_payload
        .as_deref()
        .expect("subscription payload must be captured before assertion");

    assert!(
        payload.contains(&format!("key={expected_key_fragment}")),
        "expected key fragment {:?} in payload, got: {payload}",
        expected_key_fragment
    );
}

#[then(expr = "the last relay subscription payload contains {string}")]
async fn then_last_stream_subscription_payload_contains(
    world: &mut ScenarioWorld,
    expected_fragment: String,
) {
    let payload = world
        .last_subscription_payload
        .as_deref()
        .expect("subscription payload must be captured before assertion");

    assert!(
        payload.contains(&expected_fragment),
        "expected fragment {:?} in payload, got: {payload}",
        expected_fragment
    );
}

#[then(expr = "the last relay subscription payload masks field {string}")]
async fn then_last_stream_subscription_payload_masks_field(
    world: &mut ScenarioWorld,
    field: String,
) {
    let payload = world
        .last_subscription_payload
        .as_deref()
        .expect("subscription payload must be captured before assertion");
    let expected_fragment = format!("\"{field}\":\"<masked>\"");

    assert!(
        payload.contains(&expected_fragment),
        "expected masked field fragment {:?} in payload, got: {payload}",
        expected_fragment
    );
}

#[then("the last relay subscription payload contains")]
async fn then_last_stream_subscription_payload_contains_docstring(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let payload = world
        .last_subscription_payload
        .as_deref()
        .expect("subscription payload must be captured before assertion");

    for expected_fragment in docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        assert!(
            payload.contains(expected_fragment),
            "expected fragment {:?} in payload, got: {payload}",
            expected_fragment
        );
    }
}

#[then(expr = "the last relay subscription payload does not contain {string}")]
async fn then_last_stream_subscription_payload_does_not_contain(
    world: &mut ScenarioWorld,
    unexpected_fragment: String,
) {
    let payload = world
        .last_subscription_payload
        .as_deref()
        .expect("subscription payload must be captured before assertion");

    assert!(
        !payload.contains(&unexpected_fragment),
        "did not expect fragment {:?} in payload, got: {payload}",
        unexpected_fragment
    );
}

#[then(expr = "within {string} the active session observes a server error")]
async fn then_within_duration_the_active_session_observes_a_server_error(
    world: &mut ScenarioWorld,
    duration: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let session = world
        .active_session
        .as_mut()
        .expect("an active session must exist");
    let event = session
        .try_next_server_error(duration)
        .await
        .expect("failed while waiting for server error")
        .unwrap_or_else(|| panic!("timed out waiting for a server error within {:?}", duration));
    append_cucumber_log_line(&format!(
        "observed runtime server error level={} message={}",
        event.level, event.message
    ));
    world.last_server_error = Some(event.message);
}

#[then("the last server error contains")]
async fn then_last_server_error_contains(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected = expand_placeholders(world, docstring(step).trim());
    let error = world
        .last_server_error
        .as_deref()
        .expect("server error must be captured before assertion");
    assert!(
        error.contains(&expected),
        "expected server error to contain {expected:?}, got: {error}"
    );
}

#[then("the observed broker receives a payload")]
async fn then_observed_broker_receives_payload(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected_payload = expand_placeholders(world, docstring(step));
    let observer = world
        .broker_observer
        .as_mut()
        .expect("a broker observer must exist before assertion");
    let message = observer
        .next_message()
        .await
        .expect("failed to receive broker payload");
    world.last_broker_payload = Some(message.payload);
    world.last_broker_headers = message.headers;

    let actual = world
        .last_broker_payload
        .as_deref()
        .expect("broker payload must be captured before assertion");
    assert!(
        payload_matches_expected(actual, &expected_payload),
        "expected payload fragment {} in broker payload, got: {actual}",
        expected_payload.trim()
    );
}

#[then("the last observed broker message has headers")]
async fn then_last_observed_broker_message_has_headers(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected_headers = expand_placeholders(world, docstring(step));
    for line in expected_headers
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let (name, value) = line.split_once('=').unwrap_or_else(|| {
            panic!("expected broker header assertion '{line}' to use name=value")
        });
        assert!(
            world
                .last_broker_headers
                .iter()
                .any(|(actual_name, actual_value)| actual_name == name && actual_value == value),
            "expected broker header {name}={value}, got {:?}",
            world.last_broker_headers
        );
    }
}

#[then("the last observed broker payload contains")]
async fn then_last_observed_broker_payload_contains(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected_payload = expand_placeholders(world, docstring(step));
    let payload = world
        .last_broker_payload
        .as_deref()
        .expect("broker payload must be captured before assertion");

    for expected_fragment in expected_payload
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        assert!(
            payload.contains(expected_fragment),
            "expected fragment {expected_fragment:?} in broker payload, got: {payload}"
        );
    }
}

#[then("the ClickHouse table eventually contains a row")]
async fn then_clickhouse_table_eventually_contains_row(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected = expand_placeholders(world, docstring(step));
    let table = world
        .clickhouse_table
        .as_ref()
        .expect("a ClickHouse table must be prepared before assertion")
        .clone();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let query =
            format!("SELECT clickhouse_user_id, clickhouse_action FROM {table} FORMAT JSONEachRow");
        let payload = clickhouse_post_for_world(world, &query)
            .await
            .expect("failed to query ClickHouse table");
        let observed = payload.lines().map(str::to_string).collect::<Vec<_>>();
        if observed
            .iter()
            .any(|row| payload_matches_expected(row, &expected))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for ClickHouse row. expected {expected}, observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("the Postgres table eventually contains a row")]
async fn then_postgres_table_eventually_contains_row(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected = expand_placeholders(world, docstring(step));
    let table = world
        .postgres_table
        .as_ref()
        .expect("a Postgres table must be prepared before assertion")
        .clone();
    let client = postgres_client(world.postgres_tls)
        .await
        .expect("failed to connect to Postgres");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rows = client
            .query(
                &format!("SELECT postgres_user_id, postgres_action FROM {table}"),
                &[],
            )
            .await
            .expect("failed to query Postgres table");
        let observed = rows
            .iter()
            .map(|row| {
                let user_id: i32 = row.get(0);
                let action: String = row.get(1);
                serde_json::json!({
                    "postgres_user_id": user_id,
                    "postgres_action": action,
                })
                .to_string()
            })
            .collect::<Vec<_>>();
        if observed
            .iter()
            .any(|row| payload_matches_expected(row, &expected))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for Postgres row. expected {expected}, observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("the MySQL table eventually contains a row")]
async fn then_mysql_table_eventually_contains_row(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected = expand_placeholders(world, docstring(step));
    let table = world
        .mysql_table
        .as_ref()
        .expect("a MySQL table must be prepared before assertion")
        .clone();
    let pool = mysql_pool(world.mysql_tls).expect("failed to build MySQL pool");
    let mut conn = pool.get_conn().await.expect("failed to connect to MySQL");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observed = conn
            .query_map(
                format!("SELECT mysql_user_id, mysql_action FROM `{table}`"),
                |(user_id, action): (i32, String)| {
                    serde_json::json!({
                        "mysql_user_id": user_id,
                        "mysql_action": action,
                    })
                    .to_string()
                },
            )
            .await
            .expect("failed to query MySQL table");
        if observed
            .iter()
            .any(|row| payload_matches_expected(row, &expected))
        {
            drop(conn);
            pool.disconnect()
                .await
                .expect("failed to disconnect MySQL pool");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for MySQL row. expected {expected}, observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("the MongoDB collection eventually contains a document")]
async fn then_mongodb_collection_eventually_contains_document(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected = expand_placeholders(world, docstring(step));
    let collection = world
        .mongodb_collection
        .as_ref()
        .expect("a MongoDB collection must be prepared before assertion")
        .clone();
    let client = mongodb_client(world.mongodb_tls)
        .await
        .expect("failed to connect to MongoDB");
    let collection = client
        .database("nervix")
        .collection::<MongoDbDocument>(&collection);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let cursor = collection
            .find(mongodb_doc! {})
            .await
            .expect("failed to query MongoDB collection");
        let documents = cursor
            .try_collect::<Vec<_>>()
            .await
            .expect("failed to read MongoDB documents");
        let observed = documents
            .into_iter()
            .map(|document| {
                let user_id = match document.get("mongodb_user_id") {
                    Some(MongoDbBson::Int32(value)) => i64::from(*value),
                    Some(MongoDbBson::Int64(value)) => *value,
                    Some(MongoDbBson::Double(value)) => *value as i64,
                    _ => 0,
                };
                let action = document.get_str("mongodb_action").unwrap_or_default();
                serde_json::json!({
                    "mongodb_user_id": user_id,
                    "mongodb_action": action,
                })
                .to_string()
            })
            .collect::<Vec<_>>();
        if observed
            .iter()
            .any(|row| payload_matches_expected(row, &expected))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for MongoDB document. expected {expected}, observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "the Iceberg table {string} eventually contains a row")]
async fn then_iceberg_table_eventually_contains_row(
    world: &mut ScenarioWorld,
    table: String,
    #[step] step: &Step,
) {
    let table = expand_placeholders(world, &table);
    let domain = world.domain.clone();
    let expected = expand_placeholders(world, docstring(step));
    let expected = serde_json::from_str::<serde_json::Value>(&expected)
        .expect("Iceberg expected row must be valid JSON");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut observed = Vec::new();

    loop {
        tokio::task::consume_budget().await;
        match iceberg_table_rows(&domain, &table).await {
            Ok(rows) => {
                if rows
                    .iter()
                    .any(|row| iceberg_row_matches_expected(row, &expected))
                {
                    append_cucumber_log_line(&format!(
                        "observed searchable Iceberg row in table {table}: {expected}"
                    ));
                    return;
                }
                observed.push(format!("{rows:?}"));
            }
            Err(error) => observed.push(error),
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Iceberg table {table} to contain {expected}. observed \
             {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "the Iceberg table {string} does not contain a row within {string}")]
async fn then_iceberg_table_does_not_contain_row_within(
    world: &mut ScenarioWorld,
    table: String,
    duration: String,
    #[step] step: &Step,
) {
    let table = expand_placeholders(world, &table);
    let domain = world.domain.clone();
    let expected = expand_placeholders(world, docstring(step));
    let expected = serde_json::from_str::<serde_json::Value>(&expected)
        .expect("Iceberg expected row must be valid JSON");
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        match iceberg_table_rows(&domain, &table).await {
            Ok(rows) => {
                assert!(
                    !rows
                        .iter()
                        .any(|row| iceberg_row_matches_expected(row, &expected)),
                    "expected Iceberg table {table} not to contain {expected}, observed {rows:?}"
                );
            }
            Err(error) => append_cucumber_log_line(&format!(
                "Iceberg absence check for table {table} could not scan yet: {error}"
            )),
        }
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "the Iceberg table {string} metadata does not contain {string}")]
async fn then_iceberg_table_metadata_does_not_contain(
    world: &mut ScenarioWorld,
    table: String,
    fragment: String,
) {
    let table = expand_placeholders(world, &table);
    let fragment = expand_placeholders(world, &fragment);
    let domain = world.domain.clone();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut observed = Vec::new();

    loop {
        tokio::task::consume_budget().await;
        match iceberg_table_metadata(&domain, &table).await {
            Ok(metadata) => {
                assert!(
                    !metadata.contains(&fragment),
                    "expected Iceberg table {table} metadata not to contain {fragment:?}, got \
                     {metadata}"
                );
                return;
            }
            Err(error) => observed.push(error),
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Iceberg table {table} metadata. observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("the temp directory eventually contains an Iceberg Arrow IPC staged batch")]
async fn then_temp_directory_contains_iceberg_arrow_ipc_staged_batch(world: &mut ScenarioWorld) {
    let temp_root = world
        .temp_root
        .as_ref()
        .expect("temp root must be configured by the scenario");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        tokio::task::consume_budget().await;
        if path_contains_staged_iceberg_arrow_ipc_batch(temp_root.path()) {
            append_cucumber_log_line(&format!(
                "observed Iceberg Arrow IPC staged batch under {}",
                temp_root.path().display()
            ));
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for Iceberg Arrow IPC staged batch under {}",
            temp_root.path().display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("the temp directory does not contain an Iceberg Parquet staged batch")]
async fn then_temp_directory_does_not_contain_iceberg_parquet_staged_batch(
    world: &mut ScenarioWorld,
) {
    let temp_root = world
        .temp_root
        .as_ref()
        .expect("temp root must be configured by the scenario");
    assert!(
        !path_contains_staged_iceberg_parquet_batch(temp_root.path()),
        "observed local Iceberg Parquet staged batch under {}",
        temp_root.path().display()
    );
}

#[then(expr = "the object storage path {string} does not exist")]
async fn then_object_storage_path_does_not_exist(world: &mut ScenarioWorld, path: String) {
    let path = expand_placeholders(world, &path);
    let exists = rustfs_iceberg_file_io()
        .exists(&path)
        .await
        .unwrap_or_else(|source| panic!("failed to check object storage path {path}: {source}"));
    assert!(!exists, "object storage path {path} exists");
}

fn path_contains_staged_iceberg_arrow_ipc_batch(root: &Path) -> bool {
    path_contains_staged_iceberg_batch(root, |path, name| {
        name.starts_with("batch-")
            && name.ends_with(".arrow")
            && std::fs::File::open(path)
                .ok()
                .and_then(|file| StreamReader::try_new(file, None).ok())
                .and_then(|reader| reader.collect::<Result<Vec<_>, _>>().ok())
                .is_some_and(|batches| !batches.is_empty())
    })
}

fn path_contains_staged_iceberg_parquet_batch(root: &Path) -> bool {
    path_contains_staged_iceberg_batch(root, |_path, name| {
        name.starts_with("batch-") && name.ends_with(".parquet")
    })
}

fn path_contains_staged_iceberg_batch(
    root: &Path,
    predicate: impl Copy + Fn(&Path, &str) -> bool,
) -> bool {
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path_contains_staged_iceberg_batch(&path, predicate) {
                return true;
            }
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| predicate(&path, name))
        {
            return true;
        }
    }
    false
}

async fn iceberg_table_rows(domain: &str, table: &str) -> Result<Vec<serde_json::Value>, String> {
    let table = rustfs_iceberg_table(domain, table).await?;
    let stream = table
        .scan()
        .build()
        .map_err(|source| format!("failed to build Iceberg table scan: {source}"))?
        .to_arrow()
        .await
        .map_err(|source| format!("failed to open Iceberg Arrow scan: {source}"))?;
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|source| format!("failed to read Iceberg Arrow batches: {source}"))?;
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(iceberg_record_batch_rows(&batch)?);
    }
    Ok(rows)
}

async fn iceberg_table_metadata(domain: &str, table: &str) -> Result<String, String> {
    let metadata_location = iceberg_table_metadata_location(domain, table).await?;
    let bytes = rustfs_iceberg_file_io()
        .new_input(&metadata_location)
        .map_err(|source| format!("failed to open Iceberg table metadata: {source}"))?
        .read()
        .await
        .map_err(|source| format!("failed to read Iceberg table metadata: {source}"))?;
    String::from_utf8(bytes.to_vec())
        .map_err(|source| format!("Iceberg table metadata is not UTF-8 JSON: {source}"))
}

async fn iceberg_table_metadata_location(domain: &str, table: &str) -> Result<String, String> {
    let table = rustfs_iceberg_table(domain, table).await?;
    table
        .metadata_location_result()
        .map(|location| location.to_string())
        .map_err(|source| format!("Iceberg table metadata location is unavailable: {source}"))
}

fn rustfs_iceberg_file_io() -> FileIO {
    FileIOBuilder::new(rustfs_iceberg_storage_factory())
        .with_props(rustfs_iceberg_props())
        .build()
}

async fn rustfs_iceberg_table(domain: &str, table: &str) -> Result<iceberg::table::Table, String> {
    let catalog = rustfs_rest_catalog().await?;
    let table_ident = TableIdent::new(NamespaceIdent::new(domain.to_string()), table.to_string());
    catalog
        .load_table(&table_ident)
        .await
        .map_err(|source| format!("failed to load Iceberg table {domain}.{table}: {source}"))
}

async fn rustfs_rest_catalog() -> Result<RestCatalog, String> {
    let props = rustfs_iceberg_props()
        .into_iter()
        .chain([
            (
                REST_CATALOG_PROP_URI.to_string(),
                "http://127.0.0.1:8181".to_string(),
            ),
            (
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                "s3://nervix-iceberg/warehouse".to_string(),
            ),
        ])
        .collect();
    RestCatalogBuilder::default()
        .with_storage_factory(rustfs_iceberg_storage_factory())
        .load("iceberg_catalog", props)
        .await
        .map_err(|source| format!("{source}"))
}

fn rustfs_iceberg_props() -> [(String, String); 7] {
    [
        (S3_ENDPOINT.to_string(), "http://127.0.0.1:9900".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
        (S3_ACCESS_KEY_ID.to_string(), "rustfsadmin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "rustfsadmin".to_string()),
        (S3_PATH_STYLE_ACCESS.to_string(), "true".to_string()),
        (S3_DISABLE_EC2_METADATA.to_string(), "true".to_string()),
        (S3_DISABLE_CONFIG_LOAD.to_string(), "true".to_string()),
    ]
}

fn rustfs_iceberg_storage_factory() -> Arc<dyn iceberg::io::StorageFactory> {
    Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load: None,
    })
}

struct IcebergTableFixture {
    domain: String,
    table: String,
    location: String,
    fields: Vec<arrow_schema::Field>,
}

const ICEBERG_TABLE_PROVISION_TIMEOUT: Duration = Duration::from_secs(15);
const ICEBERG_TABLE_PROVISION_RETRY_INTERVAL: Duration = Duration::from_millis(100);

impl IcebergTableFixture {
    fn from_step(world: &ScenarioWorld, table: String, location: String, columns: &str) -> Self {
        Self {
            domain: world.domain.clone(),
            table: expand_placeholders(world, &table),
            location: expand_placeholders(world, &location),
            fields: Self::parse_fields(columns),
        }
    }

    async fn ensure(&self) -> Result<(), String> {
        let _guard = ICEBERG_TABLE_PROVISION_LOCK
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await;
        let deadline = Instant::now() + ICEBERG_TABLE_PROVISION_TIMEOUT;
        loop {
            tokio::task::consume_budget().await;
            match self.ensure_once().await {
                Ok(()) => return Ok(()),
                Err(error) if Self::is_transient_catalog_lock(&error) => {
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "timed out retrying Iceberg table setup after transient catalog lock: \
                             {error}"
                        ));
                    }
                    tokio::time::sleep(ICEBERG_TABLE_PROVISION_RETRY_INTERVAL).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn ensure_once(&self) -> Result<(), String> {
        let catalog = rustfs_rest_catalog().await?;
        let namespace = NamespaceIdent::new(self.domain.clone());
        if !catalog
            .namespace_exists(&namespace)
            .await
            .map_err(|source| format!("failed to check Iceberg namespace: {source}"))?
        {
            catalog
                .create_namespace(&namespace, Default::default())
                .await
                .map_err(|source| format!("failed to create Iceberg namespace: {source}"))?;
        }

        let table_ident = TableIdent::new(namespace, self.table.clone());
        if catalog
            .table_exists(&table_ident)
            .await
            .map_err(|source| format!("failed to check Iceberg table: {source}"))?
        {
            let table = catalog
                .load_table(&table_ident)
                .await
                .map_err(|source| format!("failed to load Iceberg table: {source}"))?;
            if table.metadata().location() != self.location {
                return Err(format!(
                    "Iceberg table {table_ident} exists at '{}' instead of '{}'",
                    table.metadata().location(),
                    self.location
                ));
            }
            return Ok(());
        }

        let schema =
            arrow_schema_to_schema_auto_assign_ids(&arrow_schema::Schema::new(self.fields.clone()))
                .map_err(|source| format!("failed to build Iceberg table schema: {source}"))?;
        let creation = TableCreation::builder()
            .name(self.table.clone())
            .location(self.location.clone())
            .schema(schema)
            .build();
        catalog
            .create_table(table_ident.namespace(), creation)
            .await
            .map_err(|source| format!("failed to create Iceberg table {table_ident}: {source}"))?;
        Ok(())
    }

    fn is_transient_catalog_lock(error: &str) -> bool {
        error.contains("SQLITE_BUSY") || error.contains("database is locked")
    }

    fn parse_fields(columns: &str) -> Vec<arrow_schema::Field> {
        columns
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(Self::parse_field)
            .collect()
    }

    fn parse_field(line: &str) -> arrow_schema::Field {
        let normalized = line.trim_end_matches(',');
        let mut parts = normalized.split_whitespace();
        let name = parts
            .next()
            .unwrap_or_else(|| panic!("Iceberg column line is missing a name: {line}"));
        let column_type = parts
            .next()
            .unwrap_or_else(|| panic!("Iceberg column line is missing a type: {line}"));
        assert!(
            parts.next().is_none(),
            "Iceberg column line must be '<name> <type>': {line}"
        );
        arrow_schema::Field::new(name, Self::parse_data_type(column_type), true)
    }

    fn parse_data_type(column_type: &str) -> ArrowDataType {
        match column_type {
            "STRING" => ArrowDataType::Utf8,
            "I64" => ArrowDataType::Int64,
            "F64" => ArrowDataType::Float64,
            "BOOLEAN" => ArrowDataType::Boolean,
            "DATETIME" => {
                ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, Some("+00:00".into()))
            }
            other => panic!("unsupported Iceberg fixture column type '{other}'"),
        }
    }
}

fn iceberg_record_batch_rows(batch: &RecordBatch) -> Result<Vec<serde_json::Value>, String> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let mut row = serde_json::Map::new();
        for column_index in 0..batch.num_columns() {
            let field = schema.field(column_index);
            row.insert(
                field.name().to_string(),
                iceberg_cell_value(batch, column_index, row_index)?,
            );
        }
        rows.push(serde_json::Value::Object(row));
    }
    Ok(rows)
}

fn iceberg_cell_value(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
) -> Result<serde_json::Value, String> {
    let schema = batch.schema();
    let field = schema.field(column_index);
    let array = batch.column(column_index);
    if array.is_null(row_index) {
        return Ok(serde_json::Value::Null);
    }
    match field.data_type() {
        ArrowDataType::Int64 => Ok(serde_json::Value::from(
            iceberg_column::<Int64Array>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::UInt64 => Ok(serde_json::Value::from(
            iceberg_column::<UInt64Array>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::Boolean => Ok(serde_json::Value::from(
            iceberg_column::<BooleanArray>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::Utf8 => Ok(serde_json::Value::from(
            iceberg_column::<StringArray>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::LargeUtf8 => Ok(serde_json::Value::from(
            iceberg_column::<LargeStringArray>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::Utf8View => Ok(serde_json::Value::from(
            iceberg_column::<StringViewArray>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, _) => Ok(serde_json::Value::from(
            iceberg_column::<TimestampMicrosecondArray>(batch, column_index)?.value(row_index),
        )),
        unsupported => Err(format!(
            "unsupported Iceberg assertion field '{}' Arrow type {unsupported:?}",
            field.name()
        )),
    }
}

fn iceberg_column<T: 'static>(batch: &RecordBatch, column_index: usize) -> Result<&T, String> {
    let schema = batch.schema();
    batch
        .column(column_index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            format!(
                "Iceberg assertion column '{}' did not contain expected Arrow array {}",
                schema.field(column_index).name(),
                std::any::type_name::<T>()
            )
        })
}

fn iceberg_row_matches_expected(row: &serde_json::Value, expected: &serde_json::Value) -> bool {
    let (Some(row), Some(expected)) = (row.as_object(), expected.as_object()) else {
        return row == expected;
    };
    expected
        .iter()
        .all(|(key, value)| row.get(key).is_some_and(|row_value| row_value == value))
}

#[then(expr = "within {string} the observed broker receives payloads")]
async fn then_within_duration_the_observed_broker_receives_payloads(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let expected_fragments = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| expand_placeholders(world, line))
        .collect::<Vec<_>>();

    assert!(
        !expected_fragments.is_empty(),
        "step docstring must contain at least one expected payload fragment"
    );

    let observer = world
        .broker_observer
        .as_mut()
        .expect("a broker observer must exist before assertion");
    let deadline = Instant::now() + duration;
    let mut remaining = expected_fragments
        .iter()
        .fold(BTreeMap::new(), |mut counts, fragment| {
            *counts.entry(fragment.clone()).or_insert(0usize) += 1;
            counts
        });
    let mut observed = Vec::with_capacity(expected_fragments.len());

    while !remaining.is_empty() {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for broker payloads. expected remaining {:?}, observed {:?}",
            remaining,
            observed
        );
        let wait = deadline.saturating_duration_since(now);
        let payload = observer
            .try_next_payload(wait)
            .await
            .expect("failed while waiting for broker payloads")
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for broker payloads. expected remaining {:?}, observed {:?}",
                    remaining, observed
                )
            });
        observed.push(payload.clone());
        world.last_broker_payload = Some(payload.clone());

        if let Some(fragment) = remaining
            .keys()
            .find(|fragment| payload.contains(fragment.as_str()))
            .cloned()
        {
            let count = remaining
                .get_mut(&fragment)
                .expect("matched fragment must be present in remaining set");
            *count -= 1;
            if *count == 0 {
                remaining.remove(&fragment);
            }
        }
    }
}

#[then(expr = "the observed broker does not receive a payload within {string}")]
async fn then_the_observed_broker_does_not_receive_a_payload_within(
    world: &mut ScenarioWorld,
    duration: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let observer = world
        .broker_observer
        .as_mut()
        .expect("a broker observer must exist before assertion");
    let payload = observer
        .try_next_payload(duration)
        .await
        .expect("failed while waiting for absence of broker payload");

    assert!(
        payload.is_none(),
        "expected no broker payload, got: {:?}",
        payload
    );
}

async fn capture_and_assert_subscription_payload(
    world: &mut ScenarioWorld,
    expected_payload: &str,
    expect_topic_key: bool,
    timeout: Duration,
) {
    let expected_payload = expected_payload.trim().to_string();
    if let Some(payload) = world.last_subscription_payload.as_deref()
        && (!expect_topic_key || payload.contains("key=notifications"))
        && payload.contains(&expected_payload)
    {
        return;
    }
    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let deadline = Instant::now() + timeout;
    let mut observed = Vec::new();

    loop {
        tokio::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for subscription payload containing {}. observed {:?}",
            expected_payload,
            observed
        );
        let wait = deadline.saturating_duration_since(now);
        let event = session
            .try_next_subscription(wait)
            .await
            .expect("failed to receive subscription event")
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for subscription payload containing {}. observed {:?}",
                    expected_payload, observed
                )
            });
        let payload = event.payload;
        observed.push(payload.clone());
        world.last_subscription_payload = Some(payload.clone());

        if expect_topic_key && !payload.contains("key=notifications") {
            continue;
        }
        if payload.contains(&expected_payload) {
            break;
        }
    }
}

async fn try_capture_any_subscription_payload(
    world: &mut ScenarioWorld,
    duration: Duration,
) -> bool {
    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    tokio::task::consume_budget().await;
    let Some(event) = session
        .try_next_subscription(duration)
        .await
        .expect("failed to receive subscription event")
    else {
        return false;
    };
    world.last_subscription_payload = Some(event.payload);
    true
}
#[then(expr = "node {string} eventually reports interconnect to {string} as {string}")]
async fn then_node_eventually_reports_interconnect_status(
    world: &mut ScenarioWorld,
    node_id: String,
    peer_node_id: String,
    expected_status: String,
) {
    world
        .cluster()
        .wait_for_interconnect_status(&node_id, &peer_node_id, &expected_status)
        .await
        .expect("interconnect status did not converge");
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("scenario runtime should build")
        .block_on(run_scenarios());
}

async fn run_scenarios() {
    truncate_cucumber_log();
    let writer = writer::Basic::raw(
        std::io::stdout(), // Output to stdout
        writer::Coloring::Auto,
        writer::Verbosity::ShowWorldAndDocString,
    )
    .summarized()
    .normalized()
    .repeat_failed();
    ScenarioWorld::cucumber()
        .max_concurrent_scenarios(8)
        .retries(1)
        .before(|feature, _rule, scenario, _world| {
            let feature_name = feature.name.clone();
            let scenario_name = scenario.name.clone();
            Box::pin(async move {
                append_cucumber_log_line(&format!(
                    "scenario started: feature={feature_name:?} scenario={scenario_name:?}"
                ));
            })
        })
        .after(|_feature, _rule, _scenario, _ev, world| {
            Box::pin(async move {
                append_cucumber_log_line("scenario finished");
                if let Some(world) = world {
                    append_cluster_statuses(world, "scenario teardown").await;
                    append_cucumber_log_line(&format!(
                        "scenario context: domain={} test_id={} last_command_error={:?} \
                         last_command_output={:?} last_server_error={:?} \
                         last_subscription_payload={:?} last_broker_payload={:?}",
                        world.domain,
                        world.test_id,
                        world.last_command_error,
                        world.last_command_output,
                        world.last_server_error,
                        world.last_subscription_payload,
                        world.last_broker_payload
                    ));
                    world.broker_observer = None;
                    close_browser(world).await;
                    world.active_session = None;
                    world.active_session_node = None;
                    world.active_session_has_subscription = false;
                    world.last_server_error = None;
                    if let Some(mut cluster) = world.cluster.take() {
                        let errors = cluster.shutdown_for_teardown().await;
                        for error in errors {
                            append_cucumber_log_line(&format!(
                                "cluster teardown forced node shutdown after error: {error}"
                            ));
                        }
                    }
                }
            })
        })
        .fail_on_skipped()
        .with_writer(writer)
        .run_and_exit(SCENARIOS_PATH)
        .await;
}
