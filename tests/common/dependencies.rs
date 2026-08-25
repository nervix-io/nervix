use std::{
    fmt, io,
    path::{Path, PathBuf},
    sync::OnceLock,
};

pub(crate) use nervix_test_environment::{
    CLICKHOUSE_ADDR, CLICKHOUSE_TLS_ADDR, DependencyEndpoints, ICEBERG_REST_ADDR, KAFKA_ADDR,
    KAFKA_DOCKER_ADDR, KAFKA_DOCKER_NETWORK, MONGODB_ADDR, MONGODB_TLS_ADDR, MQTT_ADDR, MYSQL_ADDR,
    MYSQL_TLS_ADDR, NATS_ADDR, NATS_TLS_ADDR, POSTGRES_ADDR, POSTGRES_TLS_ADDR, PULSAR_ADDR,
    PULSAR_TLS_ADDR, QUICKWIT_ADDR, RABBITMQ_ADDR, REDIS_ADDR, RUSTFS_ADDR, SQS_ENDPOINT,
    SQS_TLS_ENDPOINT,
};
use nervix_test_environment::{ContainerMode, DependencyEnvironment, configure_process_lifecycle};
use tokio::sync::Mutex;

static SUITE_DEPENDENCIES: OnceLock<Mutex<DependencyEnvironment>> = OnceLock::new();

#[derive(Clone, Default)]
pub(crate) struct TestDependencies {
    endpoints: DependencyEndpoints,
    tls_dir: Option<PathBuf>,
}

impl fmt::Debug for TestDependencies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestDependencies")
            .field("endpoints", &self.endpoints)
            .field("tls_materials_generated", &self.tls_dir.is_some())
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

    pub(crate) async fn otel_collector_contains(&self, needle: &str) -> io::Result<bool> {
        let suite = SUITE_DEPENDENCIES.get().ok_or_else(|| {
            io::Error::other(
                "OpenTelemetry Collector is unavailable; add 'Given OpenTelemetry Collector is \
                 running'",
            )
        })?;
        suite.lock().await.otel_collector_contains(needle).await
    }

    pub(crate) async fn shutdown_suite() -> Vec<String> {
        let Some(suite) = SUITE_DEPENDENCIES.get() else {
            return Vec::new();
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
    }
}

fn suite_dependencies(scope: &str) -> &'static Mutex<DependencyEnvironment> {
    SUITE_DEPENDENCIES
        .get_or_init(|| Mutex::new(DependencyEnvironment::from_environment(scope.to_string())))
}
