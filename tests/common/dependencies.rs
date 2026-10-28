//! Outside the layer order: the scenario suite's shared external dependencies.
//!
//! - **Owns.** Suite startup and teardown, publishing endpoint snapshots, and sharing ready Postgres
//!   observation clients with scenarios.
//! - **Depends on.** The test environment's container, client, TLS, and observation contracts.
//! - **Must not know.** Product graph construction, connector plans, or node lifecycle policy.

use std::{
    fmt, io,
    path::{Path, PathBuf},
};

use nervix_primitives::sync::{Mutex, blocking::OnceLock};
pub(crate) use nervix_test_environment::{
    CLICKHOUSE_ADDR, CLICKHOUSE_TLS_ADDR, DependencyEndpoints, ICEBERG_REST_ADDR, KAFKA_ADDR,
    KAFKA_DOCKER_ADDR, KAFKA_DOCKER_NETWORK, MOCK_HTTP_ADDR, MOCK_WS_ADDR, MOCK_WSS_ADDR,
    MONGODB_ADDR, MONGODB_MTLS_ADDR, MONGODB_REPLICA_SET_ADDR, MONGODB_TLS_ADDR, MQTT_ADDR,
    MQTT_TLS_ADDR, MYSQL_ADDR, MYSQL_TLS_ADDR, NATS_ADDR, NATS_TLS_ADDR, PULSAR_ADDR,
    PULSAR_ADMIN_ADDR, PULSAR_TLS_ADDR, QUICKWIT_ADDR, RABBITMQ_ADDR, RABBITMQ_MANAGEMENT_ADDR,
    RABBITMQ_TLS_ADDR, REDIS_ADDR, REDIS_TLS_ADDR, RUSTFS_ADDR, SQS_ENDPOINT, SQS_TLS_ENDPOINT,
    collector_exports::CollectorSignal,
    collector_output::{CollectorOutput, CollectorWait},
};
use nervix_test_environment::{
    ContainerMode, DependencyEnvironment, PostgresClients, TeardownFailures,
    configure_process_lifecycle,
};

static SUITE_DEPENDENCIES: OnceLock<Mutex<DependencyEnvironment>> = OnceLock::new();

#[derive(Clone, Default)]
pub(crate) struct TestDependencies {
    endpoints: DependencyEndpoints,
    tls_dir: Option<PathBuf>,
    /// The suite's view of the OpenTelemetry Collector's output, once the Collector started.
    otel_collector_output: Option<CollectorOutput>,
    postgres_clients: Option<PostgresClients>,
}

impl fmt::Debug for TestDependencies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestDependencies")
            .field("endpoints", &self.endpoints)
            .field("tls_materials_generated", &self.tls_dir.is_some())
            .field(
                "otel_collector_followed",
                &self.otel_collector_output.is_some(),
            )
            .finish()
    }
}

macro_rules! dependency_starters {
    ($($method:ident => $dependency:literal),+ $(,)?) => {
        $(
            pub(crate) async fn $method(&mut self, scope: &str) -> io::Result<()> {
                let mut environment = suite_dependencies(scope).lock().await;
                let result = environment.$method().await;
                if result.is_err() {
                    environment.reset_dependency($dependency);
                } else {
                    self.refresh_from(&environment);
                }
                result
            }
        )+
    };
}

impl TestDependencies {
    fn running_postgres_clients(&self) -> io::Result<&PostgresClients> {
        self.postgres_clients.as_ref().ok_or_else(|| {
            io::Error::other("Postgres clients are unavailable; add 'Given Postgres is running'")
        })
    }

    pub(crate) fn postgres_client(&self, tls: bool) -> io::Result<sqlx::PgPool> {
        Ok(self.running_postgres_clients()?.client(tls))
    }

    /// A lock controller connects on its own, so a held lock takes no observation connection.
    pub(crate) async fn postgres_lock_connection(
        &self,
        tls: bool,
    ) -> io::Result<sqlx::PgConnection> {
        self.running_postgres_clients()?.lock_connection(tls).await
    }

    pub(crate) fn configure_process_lifecycle() {
        configure_process_lifecycle(ContainerMode::from_environment());
    }

    pub(crate) fn endpoints(&self) -> &DependencyEndpoints {
        &self.endpoints
    }

    pub(crate) fn tls_dir(&self) -> io::Result<&Path> {
        self.tls_dir.as_deref().ok_or_else(|| {
            io::Error::other("scenario TLS materials are unavailable; start a TLS dependency first")
        })
    }

    fn clock_source_observations_url(&self, name: &str) -> io::Result<url::Url> {
        let base = self.endpoints.get(MOCK_HTTP_ADDR)?;
        let mut url = url::Url::parse(base)
            .map_err(|error| io::Error::other(format!("invalid HTTP mock endpoint: {error}")))?;
        let mut segments = url
            .path_segments_mut()
            .map_err(|()| io::Error::other("HTTP mock endpoint cannot hold path segments"))?;
        segments
            .clear()
            .push("clock-source-observations")
            .push(name);
        drop(segments);
        Ok(url)
    }

    pub(crate) async fn reset_clock_source(&self, name: &str) -> io::Result<()> {
        let url = self.clock_source_observations_url(name)?;
        reqwest::Client::new()
            .delete(url)
            .send()
            .await
            .map_err(|error| io::Error::other(format!("clock source reset failed: {error}")))?
            .error_for_status()
            .map_err(|error| io::Error::other(format!("clock source reset failed: {error}")))?;
        Ok(())
    }

    pub(crate) async fn clock_source_observations(
        &self,
        name: &str,
    ) -> io::Result<serde_json::Value> {
        let url = self.clock_source_observations_url(name)?;
        reqwest::get(url)
            .await
            .map_err(|error| io::Error::other(format!("clock source query failed: {error}")))?
            .error_for_status()
            .map_err(|error| io::Error::other(format!("clock source query failed: {error}")))?
            .json()
            .await
            .map_err(|error| {
                io::Error::other(format!("clock source response was not valid JSON: {error}"))
            })
    }

    /// Whether the HTTP mock has answered `name`'s one polled payload. It answers the first poll of
    /// a name with the payload two seconds after the request, and every later poll with no
    /// content.
    pub(crate) async fn http_poll_answered(&self, name: &str) -> io::Result<bool> {
        let base = self.endpoints.get(MOCK_HTTP_ADDR)?;
        let mut url = url::Url::parse(base)
            .map_err(|error| io::Error::other(format!("invalid HTTP mock endpoint: {error}")))?;
        let mut segments = url
            .path_segments_mut()
            .map_err(|()| io::Error::other("HTTP mock endpoint cannot hold path segments"))?;
        segments.clear().push("http-observations").push(name);
        drop(segments);
        let observations: serde_json::Value = reqwest::get(url)
            .await
            .map_err(|error| io::Error::other(format!("HTTP poll query failed: {error}")))?
            .error_for_status()
            .map_err(|error| io::Error::other(format!("HTTP poll query failed: {error}")))?
            .json()
            .await
            .map_err(|error| {
                io::Error::other(format!("HTTP poll response was not valid JSON: {error}"))
            })?;
        match observations.get("answered") {
            Some(serde_json::Value::Bool(answered)) => Ok(*answered),
            _ => Err(io::Error::other(format!(
                "HTTP poll observations carry no answered flag: {observations}"
            ))),
        }
    }

    dependency_starters! {
        start_kafka => "kafka",
        start_pulsar => "pulsar",
        start_rabbitmq => "rabbitmq",
        start_redis => "redis",
        start_mqtt => "mqtt",
        start_clickhouse => "clickhouse",
        start_postgres => "postgres",
        start_mysql => "mysql",
        start_mongodb => "mongodb",
        start_mongodb_mutual_tls => "mongodb-mutual-tls",
        start_mongodb_replica_set => "mongodb-replica-set",
        start_nats => "nats",
        start_nats_tls => "nats-tls",
        start_prometheus => "prometheus",
        start_prometheus_tls => "prometheus-tls",
        start_sqs => "sqs",
        start_mock_server => "mock-server",
        start_iceberg => "iceberg",
        start_gcs => "gcs",
        start_azurite => "azurite",
        start_quickwit => "quickwit",
        start_otel_collector => "otel-collector",
        start_jaeger => "jaeger",
        start_sentry => "sentry",
    }

    pub(crate) async fn sentry_event(
        &self,
        environment: &str,
    ) -> io::Result<Option<serde_json::Value>> {
        let suite = SUITE_DEPENDENCIES.get().ok_or_else(|| {
            io::Error::other("Sentry test container is unavailable; add 'Given Sentry is running'")
        })?;
        suite.lock().await.sentry_event(environment).await
    }

    pub(crate) async fn quickwit_index_contains(
        &self,
        index: &str,
        needle: &str,
    ) -> io::Result<bool> {
        let base = self.endpoints.get(QUICKWIT_ADDR)?;
        let mut url = url::Url::parse(base)
            .map_err(|error| io::Error::other(format!("invalid Quickwit endpoint: {error}")))?;
        url.set_path(&format!("/api/v1/{index}/search"));
        url.query_pairs_mut()
            .append_pair("query", "*")
            .append_pair("max_hits", "100");
        let response = reqwest::get(url)
            .await
            .map_err(|error| io::Error::other(format!("Quickwit search failed: {error}")))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        let response = response
            .error_for_status()
            .map_err(|error| io::Error::other(format!("Quickwit search failed: {error}")))?;
        let body = response
            .text()
            .await
            .map_err(|error| io::Error::other(format!("Quickwit search body failed: {error}")))?;
        Ok(body.contains(needle))
    }

    /// The OpenTelemetry Collector's followed output, once this scenario started the Collector.
    pub(crate) fn otel_collector_output(&self) -> io::Result<&CollectorOutput> {
        self.otel_collector_output.as_ref().ok_or_else(|| {
            io::Error::other(
                "OpenTelemetry Collector is unavailable; add 'Given OpenTelemetry Collector is \
                 running'",
            )
        })
    }

    pub(crate) async fn shutdown_suite() -> Result<(), TeardownFailures> {
        let Some(suite) = SUITE_DEPENDENCIES.get() else {
            return Ok(());
        };
        suite.lock().await.shutdown().await
    }

    pub(crate) async fn suite_container_ids() -> Vec<String> {
        let Some(suite) = SUITE_DEPENDENCIES.get() else {
            return Vec::new();
        };
        suite.lock().await.container_ids()
    }

    pub(crate) async fn container_exists(id: &str) -> io::Result<bool> {
        DependencyEnvironment::container_exists(id).await
    }

    pub(crate) async fn force_remove_container(id: &str) -> io::Result<()> {
        DependencyEnvironment::force_remove_container(id).await
    }

    fn refresh_from(&mut self, environment: &DependencyEnvironment) {
        self.endpoints = environment.endpoints().clone();
        self.tls_dir = environment.tls_dir().ok().map(Path::to_path_buf);
        self.otel_collector_output = environment.otel_collector_output();
        self.postgres_clients = environment.postgres_clients();
    }
}

fn suite_dependencies(scope: &str) -> &'static Mutex<DependencyEnvironment> {
    SUITE_DEPENDENCIES
        .get_or_init(|| Mutex::new(DependencyEnvironment::from_environment(scope.to_string())))
}
