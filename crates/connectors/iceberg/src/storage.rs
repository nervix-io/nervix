//! Iceberg object storage with the node's HTTP resolver.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The OpenDAL operators used by Iceberg data and credential requests.
//! - **Depends on.** Iceberg's storage contract, OpenDAL, and the node DNS resolver.
//! - **Must not know.** Runtime tasks, branches, schedules, or registry state.

use std::sync::Arc;

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
use nervix_dns::DnsResolver;
use nervix_models::IcebergStorageBackend;
use opendal::{
    Operator,
    layers::{HttpClientLayer, RetryLayer, TimeoutLayer},
    raw::HttpClient,
    services::{AzdlsConfig, GcsConfig, S3Config},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::Url;

/// The pinned Iceberg OpenDAL factory does not expose its operator's HTTP client. This factory
/// constructs the same three backends with OpenDAL's client layer, so data requests and the
/// credential provider's `AccessorInfoHttpSend` use the resolver installed by the node.
#[derive(Clone, Debug)]
pub(super) struct DnsStorageFactory {
    backend: IcebergStorageBackend,
    client: HttpClient,
}

impl DnsStorageFactory {
    pub(super) fn new(backend: IcebergStorageBackend, dns: &DnsResolver) -> Result<Self> {
        let client = reqwest::Client::builder()
            .dns_resolver(Arc::new(dns.clone()))
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
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
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
        Ok(Arc::new(DnsStorage {
            backend,
            client: self.client.clone(),
        }))
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
    backend: Backend,
    client: HttpClient,
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
    fn operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
        let url = Url::parse(path).map_err(|error| {
            Error::new(ErrorKind::DataInvalid, "invalid Iceberg object URL").with_source(error)
        })?;
        let (operator, relative_path) = match &self.backend {
            Backend::S3(config) => {
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "S3 object URL requires a bucket")
                })?;
                let mut config = config.as_ref().clone();
                config.bucket = bucket.to_string();
                let operator = Operator::from_config(config)
                    .map_err(from_opendal_error)?
                    .layer(HttpClientLayer::new(self.client.clone()))
                    .finish();
                (operator, self.relative_path(path, &url)?)
            }
            Backend::Gcs(config) => {
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "GCS object URL requires a bucket")
                })?;
                let mut config = config.clone();
                config.bucket = bucket.to_string();
                let operator = Operator::from_config(config)
                    .map_err(from_opendal_error)?
                    .layer(HttpClientLayer::new(self.client.clone()))
                    .finish();
                (operator, self.relative_path(path, &url)?)
            }
            Backend::Azure(config) => {
                let (config, relative_path) = azure_path(config, path, &url)?;
                let operator = Operator::from_config(config)
                    .map_err(from_opendal_error)?
                    .layer(HttpClientLayer::new(self.client.clone()))
                    .finish();
                (operator, relative_path)
            }
        };
        // Keep the pinned Iceberg storage layer order: each retry gets its own timeout.
        let operator = operator.layer(TimeoutLayer::new()).layer(RetryLayer::new());
        Ok((operator, relative_path))
    }

    fn relative_path<'a>(&self, path: &'a str, url: &Url) -> Result<&'a str> {
        let host = url.host_str().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Iceberg object URL requires a host")
        })?;
        let prefix = match &self.backend {
            Backend::S3(_) => format!("{}://{host}/", url.scheme()),
            Backend::Gcs(_) => format!("gs://{host}/"),
            Backend::Azure(_) => return azure_relative_path(path, url),
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
            .map_err(from_opendal_error)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (operator, relative_path) = self.operator(path)?;
        let metadata = operator
            .stat(relative_path)
            .await
            .map_err(from_opendal_error)?;
        Ok(FileMetadata {
            size: metadata.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (operator, relative_path) = self.operator(path)?;
        Ok(opendal::Operator::read(&operator, relative_path)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (operator, relative_path) = self.operator(path)?;
        Ok(Box::new(DnsStorageReader(
            operator
                .reader(relative_path)
                .await
                .map_err(from_opendal_error)?,
        )))
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        let (operator, relative_path) = self.operator(path)?;
        opendal::Operator::write(&operator, relative_path, bytes)
            .await
            .map_err(from_opendal_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (operator, relative_path) = self.operator(path)?;
        Ok(Box::new(DnsStorageWriter(
            operator
                .writer(relative_path)
                .await
                .map_err(from_opendal_error)?,
        )))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (operator, relative_path) = self.operator(path)?;
        operator
            .delete(relative_path)
            .await
            .map_err(from_opendal_error)
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
            .map_err(from_opendal_error)
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        let mut deleters = HashMap::<String, opendal::Deleter>::default();
        while let Some(path) = paths.next().await {
            nervix_primitives::task::consume_budget().await;
            let url = Url::parse(&path).map_err(|error| {
                Error::new(ErrorKind::DataInvalid, "invalid Iceberg object URL").with_source(error)
            })?;
            let key = format!(
                "{}://{}@{}",
                url.scheme(),
                url.username(),
                url.host_str().unwrap_or_default()
            );
            if let Some(deleter) = deleters.get_mut(&key) {
                let relative_path = self.relative_path(&path, &url)?.to_string();
                deleter
                    .delete(relative_path)
                    .await
                    .map_err(from_opendal_error)?;
            } else {
                let (operator, relative_path) = self.operator(&path)?;
                let mut deleter = operator.deleter().await.map_err(from_opendal_error)?;
                deleter
                    .delete(relative_path)
                    .await
                    .map_err(from_opendal_error)?;
                deleters.insert(key, deleter);
            }
        }
        for (_, mut deleter) in deleters {
            nervix_primitives::task::consume_budget().await;
            deleter.close().await.map_err(from_opendal_error)?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

struct DnsStorageReader(opendal::Reader);

#[async_trait]
impl FileRead for DnsStorageReader {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(&self.0, range)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }
}

struct DnsStorageWriter(opendal::Writer);

#[async_trait]
impl FileWrite for DnsStorageWriter {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        opendal::Writer::write(&mut self.0, bytes)
            .await
            .map_err(from_opendal_error)
    }

    async fn close(&mut self) -> Result<()> {
        self.0.close().await.map_err(from_opendal_error)?;
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

fn s3_config(props: &HashMap<String, String>) -> Result<S3Config> {
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
    if props.contains_key(GCS_NO_AUTH) {
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

fn azure_config(props: &HashMap<String, String>) -> Result<AzdlsConfig> {
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
mod tests {
    use std::time::Duration;

    use nervix_dns::{DnsConfiguration, NameServers};
    use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
    use opendal::raw::Access as _;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    fn client() -> HttpClient {
        HttpClient::with(reqwest::Client::new())
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
        let storage = DnsStorage {
            backend: Backend::S3(Box::new(config)),
            client: client(),
        };
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
    fn gcs_properties_preserve_token_and_anonymous_policy() {
        let props = HashMap::from_iter([
            (
                GCS_SERVICE_PATH.to_string(),
                "http://gcs.nervix.test".to_string(),
            ),
            (GCS_TOKEN.to_string(), "token".to_string()),
            (GCS_NO_AUTH.to_string(), "false".to_string()),
        ]);
        let config = gcs_config(&props);
        assert_eq!(config.endpoint.as_deref(), Some("http://gcs.nervix.test"));
        assert_eq!(config.token.as_deref(), Some("token"));
        assert!(config.skip_signature);
        assert!(config.disable_vm_metadata);
        assert!(config.disable_config_load);
        let storage = DnsStorage {
            backend: Backend::Gcs(config),
            client: client(),
        };
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
        let storage = DnsStorage {
            backend: Backend::Azure(config),
            client: client(),
        };
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
        };
        let storage = DnsStorage {
            backend: Backend::S3(Box::default()),
            client: client(),
        };
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
        let factory = DnsStorageFactory::new(IcebergStorageBackend::S3, &dns)
            .expect("the object client uses the fixture resolver");
        let mut config = S3Config::default();
        config.region = Some("us-east-1".to_string());
        config.skip_signature = true;
        let storage = DnsStorage {
            backend: Backend::S3(Box::new(config)),
            client: factory.client,
        };
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
