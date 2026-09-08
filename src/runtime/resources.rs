use super::*;

#[derive(Debug)]
pub(super) struct ProtobufDescriptorCompileConfig {
    pub(super) files: Vec<String>,
    pub(super) includes: Vec<String>,
}

impl ProtobufDescriptorCompileConfig {
    pub(super) fn from_entries(entries: &[ClientConfigEntry]) -> Result<Self, String> {
        let mut files = Vec::new();
        let mut includes = Vec::new();
        for entry in entries {
            match entry.key.to_ascii_lowercase().as_str() {
                "file" | "files" => Self::append_paths(&mut files, &entry.value),
                "include" | "includes" => Self::append_paths(&mut includes, &entry.value),
                other => {
                    return Err(format!(
                        "unsupported protobuf config key '{other}'; expected 'file', 'files', \
                         'include', or 'includes'"
                    ));
                }
            }
        }
        Ok(Self { files, includes })
    }

    pub(super) fn append_paths(paths: &mut Vec<String>, value: &str) {
        paths.extend(
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_string),
        );
    }

    pub(super) fn compile_descriptor_set(
        self,
        store: &ResourceStore,
        id: &ResourceId,
    ) -> Result<prost_types::FileDescriptorSet, String> {
        let files = if self.files.is_empty() {
            Self::collect_resource_proto_files(store, id)?
        } else {
            self.files
                .iter()
                .map(|path| {
                    store
                        .resolve_content_path(id, path)
                        .map_err(|error| format!("invalid protobuf source path '{path}': {error}"))
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        if files.is_empty() {
            return Err(format!(
                "protobuf resource '{}' version {} contains no .proto files",
                id.identifier.as_str(),
                id.version
            ));
        }
        let includes = if self.includes.is_empty() {
            vec![store.content_root(id)]
        } else {
            self.includes
                .iter()
                .map(|path| {
                    store
                        .resolve_content_path(id, path)
                        .map_err(|error| format!("invalid protobuf include path '{path}': {error}"))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        protox::compile(files, includes)
            .map_err(|error| format!("failed to compile protobuf descriptors: {error}"))
    }

    pub(super) fn collect_resource_proto_files(
        store: &ResourceStore,
        id: &ResourceId,
    ) -> Result<Vec<PathBuf>, String> {
        let root = store.content_root(id);
        let mut files = BTreeSet::new();
        Self::collect_proto_files_recursive(&root, &mut files)?;
        Ok(files.into_iter().collect())
    }

    pub(super) fn collect_proto_files_recursive(
        directory: &PathBuf,
        files: &mut BTreeSet<PathBuf>,
    ) -> Result<(), String> {
        let entries = std::fs::read_dir(directory).map_err(|error| {
            format!(
                "failed to read protobuf resource directory '{}': {error}",
                directory.display()
            )
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                format!(
                    "failed to read protobuf resource directory entry '{}': {error}",
                    directory.display()
                )
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|error| {
                format!(
                    "failed to inspect protobuf resource path '{}': {error}",
                    path.display()
                )
            })?;
            if file_type.is_dir() {
                Self::collect_proto_files_recursive(&path, files)?;
            } else if file_type.is_file()
                && path
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .is_some_and(|extension| extension == "proto")
            {
                files.insert(path);
            }
        }
        Ok(())
    }
}

impl Runtime {
    pub(super) async fn compile_domain_codec(
        &self,
        domain: &DomainName,
        codec: &CreateCodec,
        schema: Arc<CompiledSchema>,
        wire_format: ResolvedCodecWireFormat<'_>,
    ) -> Result<Arc<CompiledCodec>, RuntimeError> {
        let protobuf_descriptor = if let CodecWireFormat::Protobuf(config) = &codec.wire_format {
            let build_error = |reason: String| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason,
            };
            let pool = self
                .compile_protobuf_descriptor_pool(
                    domain,
                    &config.resource,
                    config.resource_version,
                    &config.config,
                )
                .await
                .map_err(build_error)?;
            Some(pool.message(&config.message).map_err(build_error)?)
        } else {
            None
        };

        compile_codec_with_protobuf(codec, schema, wire_format, protobuf_descriptor).map_err(
            |err| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: err.to_string(),
            },
        )
    }

    pub(super) async fn compile_signaling_protocol(
        &self,
        domain: &DomainName,
        protocol: &CreateSignalingProtocol,
    ) -> Result<Arc<CompiledSignalingProtocol>, RuntimeError> {
        let build_error = |reason: String| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason,
        };
        let descriptors = if let SignalingWireFormat::Protobuf(config) = &protocol.format {
            let pool = self
                .compile_protobuf_descriptor_pool(
                    domain,
                    &config.resource,
                    config.resource_version,
                    &config.config,
                )
                .await
                .map_err(build_error)?;
            Some(SignalingProtobufDescriptors {
                send: pool.message(&config.send_message).map_err(build_error)?,
                wait: pool.message(&config.wait_message).map_err(build_error)?,
            })
        } else {
            None
        };

        CompiledSignalingProtocol::compile(protocol, descriptors)
            .map(Arc::new)
            .map_err(|error| build_error(error.to_string()))
    }

    pub(super) async fn compile_protobuf_descriptor_pool(
        &self,
        domain: &DomainName,
        resource: &ResourceName,
        resource_version: Option<u64>,
        config: &[ClientConfigEntry],
    ) -> Result<ProtobufDescriptorPool, String> {
        let store =
            self.inner.resource_store.read().clone().ok_or_else(|| {
                "protobuf descriptors require an attached resource store".to_string()
            })?;
        let id = self.resolve_resource_id(domain, resource, resource_version, resource.as_str())?;
        let compile_config = ProtobufDescriptorCompileConfig::from_entries(config)?;
        let file_descriptor_set =
            tokio::task::spawn_blocking(move || compile_config.compile_descriptor_set(&store, &id))
                .await
                .map_err(|error| {
                    format!("failed to join protobuf descriptor compilation task: {error}")
                })??;

        ProtobufDescriptorPool::from_file_descriptor_set(file_descriptor_set)
    }

    pub fn attach_resource_store(&self, resource_store: Arc<ResourceStore>) {
        *self.inner.resource_store.write() = Some(resource_store);
    }

    pub fn sync_resource_versions(&self, resources: &nervix_models::ResourceVersionStatus) {
        self.inner.latest_resource_versions.clear();
        for resource in &resources.versions {
            let key = DomainResourceKey {
                domain: resource.id.domain.clone(),
                resource: resource.id.identifier.clone(),
            };
            if let Some(mut existing) = self.inner.latest_resource_versions.get_mut(&key) {
                if resource.id.version > *existing {
                    *existing = resource.id.version;
                }
            } else {
                self.inner
                    .latest_resource_versions
                    .insert(key, resource.id.version);
            }
        }
    }

    pub fn attach_resources(
        &self,
        resource_store: Arc<ResourceStore>,
        resource_versions: ResourceVersionStatus,
    ) {
        *self.inner.resource_store.write() = Some(resource_store);
        self.sync_resource_versions(&resource_versions);
        *self.inner.resource_versions.write() = resource_versions;
    }

    pub fn update_resource_versions(&self, resource_versions: ResourceVersionStatus) {
        self.sync_resource_versions(&resource_versions);
        *self.inner.resource_versions.write() = resource_versions;
    }

    /// Resolves a resource reference to the concrete version installed in `domain`. Resources are
    /// domain-owned, so the same name in another domain is a different resource with its own
    /// version sequence. `spec` may pin a version as `<name>@<version>`.
    pub(in crate::runtime) fn resolve_resource_id(
        &self,
        domain: &DomainName,
        identifier: &ResourceName,
        requested_version: Option<u64>,
        spec: &str,
    ) -> Result<ResourceId, String> {
        if let Some(version) = requested_version {
            return Ok(ResourceId::new(domain.clone(), identifier.clone(), version));
        }
        if let Some((name, version)) = spec.rsplit_once('@') {
            let parsed = ResourceName::parse(name)
                .map_err(|_| format!("invalid client resource identifier '{name}'"))?;
            if &parsed != identifier {
                return Err(format!(
                    "client resource mount '{spec}' resolved to unexpected identifier '{}'",
                    parsed.as_str()
                ));
            }
            let version = version
                .parse::<u64>()
                .map_err(|_| format!("invalid client resource version '{version}'"))?;
            return Ok(ResourceId::new(domain.clone(), identifier.clone(), version));
        }

        let resources = self.inner.resource_versions.read();
        let Some(version) = resources.latest_version(domain, identifier) else {
            return Err(format!(
                "resource '{}' has no installed versions in domain '{}'",
                identifier.as_str(),
                domain.as_str()
            ));
        };
        Ok(ResourceId::new(domain.clone(), identifier.clone(), version))
    }

    pub(crate) fn resolve_client_config(
        &self,
        domain: &DomainName,
        mount: Option<&ResourceName>,
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<ResolvedClientConfig, String> {
        self.resolve_client_config_with_template_vars(domain, mount, config, BTreeMap::default())
    }

    pub(in crate::runtime) fn resolve_client_config_with_instance(
        &self,
        domain: &DomainName,
        mount: Option<&ResourceName>,
        config: &[nervix_models::ClientConfigEntry],
        instance: u64,
    ) -> Result<ResolvedClientConfig, String> {
        self.resolve_client_config_with_template_vars(
            domain,
            mount,
            config,
            BTreeMap::from([("instance".to_string(), instance.to_string())]),
        )
    }

    pub(super) fn resolve_client_config_with_template_vars(
        &self,
        domain: &DomainName,
        mount: Option<&ResourceName>,
        config: &[nervix_models::ClientConfigEntry],
        mut context: BTreeMap<String, String>,
    ) -> Result<ResolvedClientConfig, String> {
        let template_engine = TemplateEngine::new();
        let mut entries = Vec::with_capacity(config.len());
        for entry in config {
            entries.push(entry.clone());
        }

        let Some(mount) = mount else {
            for entry in &mut entries {
                entry.value = render_client_config_template(
                    &template_engine,
                    &entry.key,
                    &entry.value,
                    &context,
                )?;
            }
            return Ok(ResolvedClientConfig {
                entries,
                mounts: None,
            });
        };

        let resource_store = self
            .inner
            .resource_store
            .read()
            .clone()
            .ok_or_else(|| "runtime resource store is not available".to_string())?;
        let mount_root = tempfile::tempdir()
            .map_err(|source| format!("failed to create client resource mount root: {source}"))?;
        let mut aliases = BTreeMap::new();
        let id = self.resolve_resource_id(domain, mount, None, mount.as_str())?;
        let source_root = resource_store.content_root(&id);
        if !source_root.exists() {
            return Err(format!(
                "client resource mount '{}' points to missing content root '{}'",
                mount.as_str(),
                source_root.display()
            ));
        }
        let mount_path = mount_root.path().join(mount.as_str());
        #[cfg(unix)]
        std::os::unix::fs::symlink(&source_root, &mount_path).map_err(|source| {
            format!(
                "failed to mount client resource '{}' at '{}': {source}",
                mount.as_str(),
                mount_path.display()
            )
        })?;
        #[cfg(not(unix))]
        {
            return Err("client resource mounts are only supported on unix targets".to_string());
        }
        aliases.insert(mount.as_str().to_string(), mount_path);

        for (resource_name, mount_path) in &aliases {
            context.insert(
                resource_name.clone(),
                mount_path.to_string_lossy().into_owned(),
            );
        }
        for entry in &mut entries {
            entry.value = render_client_config_template(
                &template_engine,
                &entry.key,
                &entry.value,
                &context,
            )?;
        }

        Ok(ResolvedClientConfig {
            entries,
            mounts: Some(Arc::new(ClientResourceMounts {
                _root: mount_root,
                _aliases: aliases,
            })),
        })
    }

    pub(crate) fn udf_executor(&self, domain: &DomainName) -> Option<UdfExecutor> {
        self.inner
            .executions
            .get(domain)
            .map(|execution| execution.udfs.clone())
    }

    pub(crate) async fn prepare_domain_udfs(
        &self,
        mut models: Vec<CreateUdf>,
    ) -> Result<CompiledDomainUdfs, nervix_roto::UdfError> {
        models.sort_by(|left, right| left.name.cmp(&right.name));
        let executor = UdfExecutor::compile(models.clone()).await?;
        Ok(CompiledDomainUdfs { models, executor })
    }

    pub(crate) fn install_prepared_domain_udfs(
        &self,
        domain: &DomainName,
        prepared: CompiledDomainUdfs,
    ) {
        self.inner
            .compiled_domain_udfs
            .insert(domain.clone(), prepared);
    }

    pub(super) async fn compile_domain_udfs(
        &self,
        domain: &DomainName,
        models: Vec<CreateUdf>,
    ) -> Result<UdfExecutor, nervix_roto::UdfError> {
        let mut sorted_models = models;
        sorted_models.sort_by(|left, right| left.name.cmp(&right.name));
        if let Some(cached) = self.inner.compiled_domain_udfs.get(domain)
            && cached.models == sorted_models
        {
            return Ok(cached.executor.clone());
        }
        let prepared = self.prepare_domain_udfs(sorted_models).await?;
        let executor = prepared.executor.clone();
        self.install_prepared_domain_udfs(domain, prepared);
        Ok(executor)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use nervix_models::{
        ClientConfigEntry, ClusterNodeName, DomainName, ResourceId, ResourceVersion,
        ResourceVersionCounter, ResourceVersionStatus, Timestamp,
    };
    use sorted_vec::SortedVec;
    use tempfile::tempdir;
    use triomphe::Arc;

    use super::*;
    use crate::resource::ResourceStore;
    #[tokio::test]
    async fn client_resource_mounts_expand_into_runtime_paths() {
        let store_root = tempdir().expect("resource store tempdir");
        let source_root = tempdir().expect("resource source tempdir");
        let ca_path = source_root.path().join("ca.pem");
        std::fs::write(&ca_path, "test-ca").expect("ca file should be written");

        let mount_domain = DomainName::parse("tenant").expect("valid domain");
        let store = ResourceStore::open(store_root.path()).expect("resource store should open");
        store
            .install_from_directory(
                ResourceId::new(mount_domain.clone(), named("dev_tls"), 1),
                source_root.path(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(0),
            )
            .await
            .expect("resource version should install");

        let runtime = Runtime::new();
        runtime.attach_resources(
            Arc::new(store),
            ResourceVersionStatus {
                next_version_by_resource: SortedVec::from_unsorted(vec![ResourceVersionCounter {
                    domain: mount_domain.clone(),
                    identifier: named("dev_tls"),
                    next_version: 2,
                }]),
                versions: SortedVec::from_unsorted(vec![ResourceVersion {
                    id: ResourceId::new(mount_domain.clone(), named("dev_tls"), 1),
                    root_checksum: "root".to_string(),
                    manifest_checksum: "manifest".to_string(),
                    file_count: 1,
                    total_bytes: 7,
                    created_at: Timestamp::from_unix_nanos(0),
                    created_by_node: ClusterNodeName::parse("node-1").expect("valid name"),
                }]),
                replicas: SortedVec::new(),
            },
        );

        let resolved = runtime
            .resolve_client_config(
                &mount_domain,
                Some(&named("dev_tls")),
                &[ClientConfigEntry {
                    key: "tls_ca_file".to_string(),
                    value: "{{ dev_tls }}/ca.pem".to_string(),
                }],
            )
            .expect("client config should resolve");

        assert!(resolved.mounts.is_some());
        assert_eq!(resolved.entries.len(), 1);
        let mounted_ca = PathBuf::from(&resolved.entries[0].value);
        assert!(mounted_ca.ends_with("ca.pem"));
        assert_eq!(
            std::fs::read_to_string(&mounted_ca).expect("mounted ca should be readable"),
            "test-ca"
        );

        let other_domain = DomainName::parse("other").expect("valid domain");
        let error = runtime
            .resolve_client_config(
                &other_domain,
                Some(&named("dev_tls")),
                &[ClientConfigEntry {
                    key: "tls_ca_file".to_string(),
                    value: "{{ dev_tls }}/ca.pem".to_string(),
                }],
            )
            .expect_err("another domain must not see this domain's resource");
        assert!(
            error.contains("has no installed versions in domain 'other'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn client_resource_mounts_reject_unknown_placeholders() {
        let runtime = Runtime::new();
        let error = runtime
            .resolve_client_config(
                &DomainName::parse("tenant").expect("valid domain"),
                None,
                &[ClientConfigEntry {
                    key: "tls_ca_file".to_string(),
                    value: "{{dev_tls}}/ca.pem".to_string(),
                }],
            )
            .expect_err("unknown placeholder should fail");
        assert!(error.contains("failed to render client config template"));
    }

    #[test]
    fn client_config_instance_placeholder_renders_for_concrete_instance() {
        let runtime = Runtime::new();
        let resolved = runtime
            .resolve_client_config_with_instance(
                &DomainName::parse("tenant").expect("valid domain"),
                None,
                &[ClientConfigEntry {
                    key: "client_id".to_string(),
                    value: "mqtt-client-{{instance}}".to_string(),
                }],
                7,
            )
            .expect("instance placeholder should resolve");

        assert_eq!(resolved.entries[0].value, "mqtt-client-7");
    }
}
