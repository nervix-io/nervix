//! Iceberg object storage with the node's HTTP resolver.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The OpenDAL operators used by Iceberg data and credential requests.
//! - **Depends on.** Iceberg's storage contract, OpenDAL, and the node DNS resolver.
//! - **Must not know.** Runtime tasks, branches, schedules, or registry state.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use ahash::HashMap;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt as _, stream::BoxStream};
use iceberg::{
    Error, ErrorKind, Result,
    io::{
        ADLS_ACCOUNT_KEY, ADLS_ACCOUNT_NAME, ADLS_AUTHORITY_HOST, ADLS_CLIENT_ID,
        ADLS_CLIENT_SECRET, ADLS_CONNECTION_STRING, ADLS_SAS_TOKEN, ADLS_TENANT_ID, CLIENT_REGION,
        FileMetadata, FileRead, FileWrite, GCS_ALLOW_ANONYMOUS, GCS_CREDENTIALS_JSON,
        GCS_DISABLE_CONFIG_LOAD, GCS_DISABLE_VM_METADATA, GCS_NO_AUTH, GCS_SERVICE_PATH, GCS_TOKEN,
        InputFile, OutputFile, S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ASSUME_ROLE_ARN,
        S3_ASSUME_ROLE_EXTERNAL_ID, S3_ASSUME_ROLE_SESSION_NAME, S3_DISABLE_CONFIG_LOAD,
        S3_DISABLE_EC2_METADATA, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
        S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN, S3_SSE_KEY, S3_SSE_MD5, S3_SSE_TYPE, Storage,
        StorageConfig, StorageFactory,
    },
};
use nervix_connector::ConfiguredDeadline;
use nervix_dns::DnsResolver;
use nervix_models::IcebergStorageBackend;
use nervix_primitives::sync::StdArc;
use opendal::{
    Operator,
    layers::{HttpClientLayer, TimeoutLayer},
    raw::HttpClient,
    services::{AzdlsConfig, GcsConfig, S3Config},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::Url;

/// The pinned Iceberg OpenDAL factory does not expose its operator's HTTP client. This factory
/// constructs the same three backends with OpenDAL's client layer, so data requests and the
/// credential provider's `AccessorInfoHttpSend` use the resolver installed by the node and the
/// TLS configuration of the emitter's storage client.
#[derive(Clone, Debug)]
pub(super) struct DnsStorageFactory {
    backend: IcebergStorageBackend,
    client: HttpClient,
    root: Url,
    deadline: StorageDeadline,
}

impl DnsStorageFactory {
    pub(super) fn new(
        backend: IcebergStorageBackend,
        dns: &DnsResolver,
        root: &Url,
        deadline: Option<ConfiguredDeadline>,
        tls: &rustls::ClientConfig,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .dns_resolver(StdArc::new(dns.clone()))
            .retry(reqwest::retry::never())
            .tls_backend_preconfigured(tls.clone())
            .build()
            .map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    "failed to build Iceberg object-store client",
                )
                .with_source(error)
            })?;
        Ok(Self {
            backend,
            client: HttpClient::with(client),
            root: root.clone(),
            deadline: StorageDeadline {
                configured: deadline,
            },
        })
    }
}

// FileIO and its factory are runtime-only. Serializing either would discard the configured
// resolver; fail explicitly if a caller tries to turn one into persisted state.
impl Serialize for DnsStorageFactory {
    fn serialize<S: Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "Iceberg DNS storage factories cannot be serialized",
        ))
    }
}

impl<'de> Deserialize<'de> for DnsStorageFactory {
    fn deserialize<D: Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "Iceberg DNS storage factories cannot be deserialized",
        ))
    }
}

#[typetag::serde]
impl StorageFactory for DnsStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<StdArc<dyn Storage>> {
        let props: HashMap<_, _> = config
            .props()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let backend = match self.backend {
            IcebergStorageBackend::S3 => Backend::S3(Box::new(s3_config(&props)?)),
            IcebergStorageBackend::Gcs => Backend::Gcs(gcs_config(&props)),
            IcebergStorageBackend::AzureBlob => Backend::Azure(azure_config(&props)?),
        };
        Ok(StdArc::new(DnsStorage::new(
            backend,
            self.client.clone(),
            self.root.clone(),
            self.deadline,
        )?))
    }
}

#[derive(Clone, Debug)]
enum Backend {
    S3(Box<S3Config>),
    Gcs(GcsConfig),
    Azure(AzdlsConfig),
}

#[derive(Clone, Debug)]
struct DnsStorage {
    root: Url,
    #[cfg_attr(
        nervix_lint,
        nervix::state(
            installed,
            reason = "one operator is initialized with this emitter's storage and retained by its \
                      FileIO"
        )
    )]
    operator: Operator,
    deadline: StorageDeadline,
}

/// One configured deadline applies equally to an operation and an I/O call. Its absence keeps
/// OpenDAL's documented operation and I/O bounds.
#[derive(Debug, Clone, Copy, Default)]
struct StorageDeadline {
    configured: Option<ConfiguredDeadline>,
}

impl StorageDeadline {
    fn layer(self) -> TimeoutLayer {
        let layer = TimeoutLayer::new();
        match self.configured {
            Some(deadline) => layer
                .with_timeout(deadline.bound())
                .with_io_timeout(deadline.bound()),
            None => layer,
        }
    }

    fn failure(self, error: opendal::Error) -> Error {
        let error = from_opendal_error(error);
        match self.configured {
            Some(deadline) => error.with_context(
                deadline.key().key(),
                deadline.bound().as_millis().to_string(),
            ),
            None => error,
        }
    }
}

impl Serialize for DnsStorage {
    fn serialize<S: Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "Iceberg DNS storage cannot be serialized",
        ))
    }
}

impl<'de> Deserialize<'de> for DnsStorage {
    fn deserialize<D: Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "Iceberg DNS storage cannot be deserialized",
        ))
    }
}

impl DnsStorage {
    fn new(
        backend: Backend,
        client: HttpClient,
        root: Url,
        deadline: StorageDeadline,
    ) -> Result<Self> {
        let operator = match backend {
            Backend::S3(config) => {
                let bucket = root.host_str().ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "S3 object URL requires a bucket")
                })?;
                let mut config = *config;
                config.bucket = bucket.to_string();
                Operator::from_config(config)
                    .map_err(from_opendal_error)?
                    .layer(HttpClientLayer::new(client))
                    .finish()
            }
            Backend::Gcs(mut config) => {
                let bucket = root.host_str().ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "GCS object URL requires a bucket")
                })?;
                config.bucket = bucket.to_string();
                Operator::from_config(config)
                    .map_err(from_opendal_error)?
                    .layer(HttpClientLayer::new(client))
                    .finish()
            }
            Backend::Azure(config) => {
                let (config, _) = azure_path(&config, root.as_str(), &root)?;
                Operator::from_config(config)
                    .map_err(from_opendal_error)?
                    .layer(HttpClientLayer::new(client))
                    .finish()
            }
        };
        Ok(Self {
            root,
            operator: operator.layer(deadline.layer()),
            deadline,
        })
    }

    fn operator<'storage, 'path>(
        &'storage self,
        path: &'path str,
    ) -> Result<(&'storage Operator, &'path str)> {
        let url = Url::parse(path).map_err(|error| {
            Error::new(ErrorKind::DataInvalid, "invalid Iceberg object URL").with_source(error)
        })?;
        Ok((&self.operator, self.relative_path(path, &url)?))
    }

    fn relative_path<'a>(&self, path: &'a str, url: &Url) -> Result<&'a str> {
        if url.scheme() != self.root.scheme()
            || url.host_str() != self.root.host_str()
            || url.port() != self.root.port()
            || url.username() != self.root.username()
            || url.password() != self.root.password()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Iceberg object URL does not match the emitter storage root",
            ));
        }
        let host = url.host_str().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Iceberg object URL requires a host")
        })?;
        let prefix = match self.root.scheme() {
            "wasb" | "wasbs" | "abfs" | "abfss" => return azure_relative_path(path, url),
            scheme => format!("{scheme}://{host}/"),
        };
        path.strip_prefix(&prefix).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Iceberg object URL does not match its backend",
            )
        })
    }
}

#[typetag::serde]
#[async_trait]
impl Storage for DnsStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (operator, relative_path) = self.operator(path)?;
        operator
            .exists(relative_path)
            .await
            .map_err(|error| self.deadline.failure(error))
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (operator, relative_path) = self.operator(path)?;
        let metadata = operator
            .stat(relative_path)
            .await
            .map_err(|error| self.deadline.failure(error))?;
        Ok(FileMetadata {
            size: metadata.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (operator, relative_path) = self.operator(path)?;
        Ok(opendal::Operator::read(operator, relative_path)
            .await
            .map_err(|error| self.deadline.failure(error))?
            .to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (operator, relative_path) = self.operator(path)?;
        Ok(Box::new(DnsStorageReader {
            reader: operator
                .reader(relative_path)
                .await
                .map_err(|error| self.deadline.failure(error))?,
            deadline: self.deadline,
        }))
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        let (operator, relative_path) = self.operator(path)?;
        opendal::Operator::write(operator, relative_path, bytes)
            .await
            .map_err(|error| self.deadline.failure(error))?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (operator, relative_path) = self.operator(path)?;
        Ok(Box::new(DnsStorageWriter {
            writer: operator
                .writer(relative_path)
                .await
                .map_err(|error| self.deadline.failure(error))?,
            deadline: self.deadline,
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (operator, relative_path) = self.operator(path)?;
        operator
            .delete(relative_path)
            .await
            .map_err(|error| self.deadline.failure(error))
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (operator, relative_path) = self.operator(path)?;
        let prefix = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        operator
            .delete_with(&prefix)
            .recursive(true)
            .await
            .map_err(|error| self.deadline.failure(error))
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        let mut deleter = self
            .operator
            .deleter()
            .await
            .map_err(|error| self.deadline.failure(error))?;
        while let Some(path) = paths.next().await {
            nervix_primitives::task::consume_budget().await;
            let (_, relative_path) = self.operator(&path)?;
            deleter
                .delete(relative_path)
                .await
                .map_err(|error| self.deadline.failure(error))?;
        }
        deleter
            .close()
            .await
            .map_err(|error| self.deadline.failure(error))?;
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(StdArc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(StdArc::new(self.clone()), path.to_string()))
    }
}

struct DnsStorageReader {
    reader: opendal::Reader,
    deadline: StorageDeadline,
}

#[async_trait]
impl FileRead for DnsStorageReader {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(&self.reader, range)
            .await
            .map_err(|error| self.deadline.failure(error))?
            .to_bytes())
    }
}

struct DnsStorageWriter {
    writer: opendal::Writer,
    deadline: StorageDeadline,
}

#[async_trait]
impl FileWrite for DnsStorageWriter {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        opendal::Writer::write(&mut self.writer, bytes)
            .await
            .map_err(|error| self.deadline.failure(error))
    }

    async fn close(&mut self) -> Result<()> {
        self.writer
            .close()
            .await
            .map_err(|error| self.deadline.failure(error))?;
        Ok(())
    }
}

fn from_opendal_error(error: opendal::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Iceberg object-store operation failed",
    )
    .with_source(error)
}

fn truthy(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "true" | "t" | "1" | "on"
    )
}

pub(super) fn s3_config(props: &HashMap<String, String>) -> Result<S3Config> {
    let mut config = S3Config::default();
    config.enable_virtual_host_style = true;
    config.endpoint = props.get(S3_ENDPOINT).cloned();
    config.access_key_id = props.get(S3_ACCESS_KEY_ID).cloned();
    config.secret_access_key = props.get(S3_SECRET_ACCESS_KEY).cloned();
    config.session_token = props.get(S3_SESSION_TOKEN).cloned();
    config.region = props
        .get(CLIENT_REGION)
        .or_else(|| props.get(S3_REGION))
        .cloned();
    config.role_arn = props.get(S3_ASSUME_ROLE_ARN).cloned();
    config.external_id = props.get(S3_ASSUME_ROLE_EXTERNAL_ID).cloned();
    config.role_session_name = props.get(S3_ASSUME_ROLE_SESSION_NAME).cloned();
    match props
        .get(S3_SSE_TYPE)
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        None | Some("none") => {}
        Some("s3") => config.server_side_encryption = Some("AES256".to_string()),
        Some("kms") => {
            config.server_side_encryption = Some("aws:kms".to_string());
            config.server_side_encryption_aws_kms_key_id = props.get(S3_SSE_KEY).cloned();
        }
        Some("custom") => {
            config.server_side_encryption_customer_algorithm = Some("AES256".to_string());
            config.server_side_encryption_customer_key = props.get(S3_SSE_KEY).cloned();
            config.server_side_encryption_customer_key_md5 = props.get(S3_SSE_MD5).cloned();
        }
        Some(_) => {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "invalid S3 encryption type",
            ));
        }
    }
    if let Some(value) = props.get(S3_PATH_STYLE_ACCESS) {
        config.enable_virtual_host_style = !truthy(value);
    }
    if let Some(value) = props.get(S3_ALLOW_ANONYMOUS) {
        config.skip_signature = truthy(value);
    }
    if let Some(value) = props.get(S3_DISABLE_EC2_METADATA) {
        config.disable_ec2_metadata = truthy(value);
    }
    if let Some(value) = props.get(S3_DISABLE_CONFIG_LOAD) {
        config.disable_config_load = truthy(value);
    }
    Ok(config)
}

fn gcs_config(props: &HashMap<String, String>) -> GcsConfig {
    let mut config = GcsConfig::default();
    config.credential = props.get(GCS_CREDENTIALS_JSON).cloned();
    config.token = props.get(GCS_TOKEN).cloned();
    config.endpoint = props.get(GCS_SERVICE_PATH).cloned();
    if let Some(value) = props.get(GCS_NO_AUTH)
        && truthy(value)
    {
        config.skip_signature = true;
        config.disable_vm_metadata = true;
        config.disable_config_load = true;
    }
    if let Some(value) = props.get(GCS_ALLOW_ANONYMOUS) {
        config.skip_signature = config.skip_signature || truthy(value);
    }
    if let Some(value) = props.get(GCS_DISABLE_VM_METADATA) {
        config.disable_vm_metadata = config.disable_vm_metadata || truthy(value);
    }
    if let Some(value) = props.get(GCS_DISABLE_CONFIG_LOAD) {
        config.disable_config_load = config.disable_config_load || truthy(value);
    }
    config
}

pub(super) fn azure_config(props: &HashMap<String, String>) -> Result<AzdlsConfig> {
    if props.contains_key(ADLS_CONNECTION_STRING) {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Azure connection strings are not supported",
        ));
    }
    let config = AzdlsConfig {
        account_name: props.get(ADLS_ACCOUNT_NAME).cloned(),
        account_key: props.get(ADLS_ACCOUNT_KEY).cloned(),
        sas_token: props.get(ADLS_SAS_TOKEN).cloned(),
        tenant_id: props.get(ADLS_TENANT_ID).cloned(),
        client_id: props.get(ADLS_CLIENT_ID).cloned(),
        client_secret: props.get(ADLS_CLIENT_SECRET).cloned(),
        authority_host: props.get(ADLS_AUTHORITY_HOST).cloned(),
        ..AzdlsConfig::default()
    };
    Ok(config)
}

fn azure_path<'a>(
    config: &AzdlsConfig,
    path: &'a str,
    url: &Url,
) -> Result<(AzdlsConfig, &'a str)> {
    let filesystem = url.username();
    if filesystem.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Azure object URL requires a container",
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Azure object URL requires a host"))?;
    let (account, service_suffix) = host.split_once('.').ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "Azure object URL requires an account",
        )
    })?;
    let (service, suffix) = service_suffix.split_once('.').ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "Azure object URL requires a storage service",
        )
    })?;
    let expected_service = match url.scheme() {
        "wasb" | "wasbs" => "blob",
        "abfs" | "abfss" => "dfs",
        _ => {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "unsupported Azure object URL scheme",
            ));
        }
    };
    if service != expected_service || account.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Azure object URL has an invalid account or storage service",
        ));
    }
    if config
        .account_name
        .as_deref()
        .is_some_and(|configured| configured != account)
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Azure object URL account differs from configuration",
        ));
    }
    let scheme = if url.scheme().ends_with('s') {
        "https"
    } else {
        "http"
    };
    if let Some(endpoint) = &config.endpoint
        && (!endpoint.starts_with(scheme) || !endpoint.trim_end_matches('/').ends_with(suffix))
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Azure object URL endpoint differs from configuration",
        ));
    }
    let mut config = config.clone();
    if config.endpoint.is_none() {
        config.endpoint = Some(format!("{scheme}://{account}.dfs.{suffix}"));
    }
    config.filesystem = filesystem.to_string();
    Ok((config, azure_relative_path(path, url)?))
}

fn azure_relative_path<'a>(path: &'a str, url: &Url) -> Result<&'a str> {
    let prefix = path.strip_suffix(url.path()).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "Azure object URL path differs from the parsed URL",
        )
    })?;
    Ok(&path[prefix.len()..])
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::time::Duration;

    use meticulous::ResultExt as _;
    use nervix_dns::{DnsConfiguration, NameServers};
    use nervix_primitives::sync::{
        Arc, CancellationToken,
        atomic::{AtomicUsize, Ordering},
    };
    use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
    use opendal::raw::Access as _;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    fn client() -> HttpClient {
        HttpClient::with(reqwest::Client::new())
    }

    fn test_storage(backend: Backend, root: &str) -> DnsStorage {
        DnsStorage::new(
            backend,
            client(),
            Url::parse(root).assured("the fixture root is a URL"),
            StorageDeadline::default(),
        )
        .assured("the configured storage can initialize its operator")
    }

    struct UnansweredObjectStore;

    impl opendal::raw::HttpFetch for UnansweredObjectStore {
        async fn fetch(
            &self,
            _: http::Request<opendal::Buffer>,
        ) -> opendal::Result<http::Response<opendal::raw::HttpBody>> {
            std::future::pending().await
        }
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_storage_write_spends_the_configured_timeout_once() {
        let deadline = ConfiguredDeadline::timeout(&[nervix_models::ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "1000".to_string(),
        }])
        .assured("one second is a valid client deadline");
        let factory = DnsStorageFactory {
            backend: IcebergStorageBackend::S3,
            client: HttpClient::with(UnansweredObjectStore),
            root: Url::parse("s3://bucket/").assured("the fixture root is a URL"),
            deadline: StorageDeadline {
                configured: deadline,
            },
        };
        #[expect(
            clippy::disallowed_types,
            reason = "Iceberg StorageConfig requires a standard-library map at its external API \
                      boundary"
        )]
        let config = StorageConfig::from_props(std::collections::HashMap::from_iter([
            (S3_REGION.to_string(), "us-east-1".to_string()),
            (S3_ALLOW_ANONYMOUS.to_string(), "true".to_string()),
            ("timeout_ms".to_string(), "1000".to_string()),
        ]));
        let storage = factory
            .build(&config)
            .assured("the configured storage client can be built without a request");
        let started = nervix_primitives::time::Instant::now();
        let error = storage
            .write(
                "s3://bucket/table/data.parquet",
                Bytes::from_static(b"data"),
            )
            .await
            .expect_err("the store never answers the write");
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        assert!(error.to_string().contains("timeout_ms: 1000"), "{error}");
    }

    #[test]
    fn one_storage_owner_retains_its_operator_between_objects() {
        let mut config = S3Config::default();
        config.region = Some("us-east-1".to_string());
        config.skip_signature = true;
        let storage = test_storage(Backend::S3(Box::new(config)), "s3://bucket/");
        let (first, first_path) = storage
            .operator("s3://bucket/tables/first.parquet")
            .assured("the first object names the configured backend");
        let (second, second_path) = storage
            .operator("s3://bucket/tables/second.parquet")
            .assured("the second object names the configured backend");
        assert_eq!(first_path, "tables/first.parquet");
        assert_eq!(second_path, "tables/second.parquet");
        assert!(
            std::ptr::eq(first.inner().as_ref(), second.inner().as_ref()),
            "objects of one storage owner must use its retained operator"
        );
        for path in [
            "s3://another-bucket/tables/part.parquet",
            "gs://bucket/tables/part.parquet",
        ] {
            let error = storage
                .operator(path)
                .expect_err("an object outside the initialized root cannot reach its operator");
            assert_eq!(error.kind(), ErrorKind::DataInvalid);
        }
    }

    #[nervix_primitives::test]
    async fn a_transient_storage_failure_returns_to_the_host_after_one_request() {
        let listener = nervix_primitives::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .assured("the scripted object store can listen on loopback");
        let endpoint = listener
            .local_addr()
            .assured("the object store listener has a bound address");
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let server = nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                let (mut stream, _) = nervix_primitives::select! {
                    _ = shutdown.cancelled() => return,
                    connection = listener.accept() => connection
                        .assured("the scripted object store accepts its client"),
                };
                let mut request = [0_u8; 4096];
                let received = stream
                    .read(&mut request)
                    .await
                    .assured("the object request carries a header");
                assert!(received > 0);
                observed.fetch_add(1, Ordering::Relaxed);
                stream
                    .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await
                    .assured("the transient response can be written");
            }
        });
        let mut config = S3Config::default();
        config.endpoint = Some(format!("http://{endpoint}"));
        config.region = Some("us-east-1".to_string());
        config.skip_signature = true;
        let storage = test_storage(Backend::S3(Box::new(config)), "s3://bucket/");
        let outcome = storage
            .write(
                "s3://bucket/table/data.parquet",
                Bytes::from_static(b"data"),
            )
            .await;
        cancellation.cancel();
        server
            .await
            .assured("the object store task stops on cancellation");
        assert!(outcome.is_err(), "the host must receive the failed attempt");
        assert_eq!(
            requests.load(Ordering::Relaxed),
            1,
            "the emitter host owns the next retry"
        );
    }

    #[test]
    fn s3_properties_preserve_addressing_authentication_and_encryption() {
        let props = HashMap::from_iter([
            (
                S3_ENDPOINT.to_string(),
                "http://objects.nervix.test:9000".to_string(),
            ),
            (S3_REGION.to_string(), "us-east-1".to_string()),
            (S3_ACCESS_KEY_ID.to_string(), "key".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "secret".to_string()),
            (S3_PATH_STYLE_ACCESS.to_string(), "true".to_string()),
            (S3_DISABLE_CONFIG_LOAD.to_string(), "true".to_string()),
            (S3_DISABLE_EC2_METADATA.to_string(), "true".to_string()),
            (S3_SSE_TYPE.to_string(), "kms".to_string()),
            (S3_SSE_KEY.to_string(), "key-id".to_string()),
        ]);
        let config = s3_config(&props).expect("the S3 configuration is valid");
        assert_eq!(
            config.endpoint.as_deref(),
            Some("http://objects.nervix.test:9000")
        );
        assert_eq!(config.region.as_deref(), Some("us-east-1"));
        assert_eq!(config.access_key_id.as_deref(), Some("key"));
        assert_eq!(config.secret_access_key.as_deref(), Some("secret"));
        assert!(!config.enable_virtual_host_style);
        assert!(config.disable_config_load);
        assert!(config.disable_ec2_metadata);
        assert_eq!(config.server_side_encryption.as_deref(), Some("aws:kms"));
        assert_eq!(
            config.server_side_encryption_aws_kms_key_id.as_deref(),
            Some("key-id")
        );
        let storage = test_storage(Backend::S3(Box::new(config)), "s3://bucket/");
        let (operator, relative) = storage
            .operator("s3://bucket/tables/part.parquet")
            .expect("the S3 object path is valid");
        assert_eq!(operator.info().name(), "bucket");
        assert_eq!(relative, "tables/part.parquet");
        assert!(
            s3_config(&HashMap::from_iter([(
                S3_SSE_TYPE.to_string(),
                "invalid".to_string()
            )]))
            .is_err()
        );
    }

    #[test]
    fn gcs_no_auth_disables_authentication_only_when_it_is_true() {
        let props = |no_auth: &str| {
            HashMap::from_iter([
                (
                    GCS_SERVICE_PATH.to_string(),
                    "http://gcs.nervix.test".to_string(),
                ),
                (GCS_TOKEN.to_string(), "token".to_string()),
                (GCS_NO_AUTH.to_string(), no_auth.to_string()),
            ])
        };
        for authenticated in ["false", "FALSE"] {
            let config = gcs_config(&props(authenticated));
            assert_eq!(config.token.as_deref(), Some("token"));
            assert!(!config.skip_signature, "{authenticated} keeps signing");
            assert!(
                !config.disable_vm_metadata,
                "{authenticated} keeps VM metadata"
            );
            assert!(
                !config.disable_config_load,
                "{authenticated} keeps configuration"
            );
        }
        let config = gcs_config(&props("true"));
        assert_eq!(config.endpoint.as_deref(), Some("http://gcs.nervix.test"));
        assert_eq!(config.token.as_deref(), Some("token"));
        assert!(config.skip_signature);
        assert!(config.disable_vm_metadata);
        assert!(config.disable_config_load);
        let storage = test_storage(Backend::Gcs(config), "gs://bucket/");
        let (operator, relative) = storage
            .operator("gs://bucket/tables/part.parquet")
            .expect("the GCS object path is valid");
        assert_eq!(operator.info().name(), "bucket");
        assert_eq!(relative, "tables/part.parquet");
    }

    #[test]
    fn azure_blob_path_preserves_container_and_account() {
        let props = HashMap::from_iter([
            (ADLS_ACCOUNT_NAME.to_string(), "account".to_string()),
            (ADLS_ACCOUNT_KEY.to_string(), "key".to_string()),
        ]);
        let config = azure_config(&props).expect("the Azure configuration is valid");
        let storage = test_storage(
            Backend::Azure(config),
            "wasbs://container@account.blob.core.windows.net/",
        );
        let (operator, relative) = storage
            .operator("wasbs://container@account.blob.core.windows.net/tables/part.parquet")
            .expect("the Azure object path is valid");
        assert_eq!(operator.info().name(), "container");
        assert_eq!(relative, "/tables/part.parquet");
        assert!(
            storage
                .operator("wasbs://container@other.blob.core.windows.net/file")
                .is_err()
        );
        assert!(
            storage
                .operator("wasbs://container@account.dfs.core.windows.net/file")
                .is_err()
        );
        assert!(
            azure_config(&HashMap::from_iter([(
                ADLS_CONNECTION_STRING.to_string(),
                "secret".to_string()
            )]))
            .is_err()
        );
    }

    #[test]
    fn runtime_storage_does_not_serialize_without_its_resolver() {
        let factory = DnsStorageFactory {
            backend: IcebergStorageBackend::S3,
            client: client(),
            root: Url::parse("s3://bucket/").assured("the fixture root is a URL"),
            deadline: StorageDeadline::default(),
        };
        let mut config = S3Config::default();
        config.region = Some("us-east-1".to_string());
        let storage = test_storage(Backend::S3(Box::new(config)), "s3://bucket/");
        assert!(serde_json::to_string(&factory).is_err());
        assert!(serde_json::to_string(&storage).is_err());
    }

    #[nervix_primitives::test]
    async fn opendal_credential_sender_uses_the_node_dns_client() {
        const NAME: &str = "credential.nervix.test";
        let authority = DnsAuthority::start_on_loopback()
            .await
            .expect("the DNS fixture starts");
        authority.set(
            NAME,
            DnsAnswer::Addresses {
                addresses: vec![std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
                ttl: Duration::from_secs(1),
            },
        );
        let files = tempfile::tempdir().expect("a fixture directory can be created");
        let resolver_configuration = files.path().join("resolv.conf");
        let hosts_file = files.path().join("hosts");
        std::fs::write(
            &resolver_configuration,
            "search nervix.test\noptions ndots:1 timeout:1 attempts:1\n",
        )
        .expect("the fixture resolver configuration can be written");
        std::fs::write(&hosts_file, "").expect("the fixture hosts file can be written");
        let dns = DnsResolver::load(DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: NameServers::Explicit(vec![authority.address()]),
        })
        .await
        .expect("the fixture DNS configuration is valid");
        let listener =
            nervix_primitives::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .expect("the token endpoint can bind");
        let port = listener
            .local_addr()
            .expect("the token endpoint has an address")
            .port();
        let server = nervix_primitives::task::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("the token request connects");
            let mut request = [0_u8; 2048];
            let bytes_read = stream
                .read(&mut request)
                .await
                .expect("the token request is readable");
            assert!(bytes_read > 0, "the token request carries HTTP headers");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\ntoken",
                )
                .await
                .expect("the token response is writable");
        });
        let root = Url::parse("s3://bucket/").assured("the fixture root is a URL");
        let tls = nervix_connector::RustlsClientConfigProvider::new(&[])
            .build_with_default_roots()
            .assured("a client without TLS files trusts the default roots");
        let factory = DnsStorageFactory::new(IcebergStorageBackend::S3, &dns, &root, None, &tls)
            .expect("the object client uses the fixture resolver");
        let mut config = S3Config::default();
        config.region = Some("us-east-1".to_string());
        config.skip_signature = true;
        let storage = DnsStorage::new(
            Backend::S3(Box::new(config)),
            factory.client,
            root,
            StorageDeadline::default(),
        )
        .assured("the storage uses the fixture resolver");
        let (operator, _) = storage
            .operator("s3://bucket/file")
            .expect("the S3 operator can be built without a network request");
        let sender = opendal::raw::AccessorInfoHttpSend::new(operator.inner().info());
        let request = http::Request::get(format!("http://{NAME}:{port}/token"))
            .body(Bytes::new())
            .expect("the token request is valid");
        let response = reqsign_core::HttpSend::http_send(&sender, request)
            .await
            .expect("the credential request uses the configured DNS answer");
        assert_eq!(response.body().as_ref(), b"token");
        assert!(authority.questions_for(NAME) > 0);
        server.await.expect("the token server task finishes");
    }
}
