use nervix_connector::{ClientResourceMounts, ResolvedClientConfig, render_client_config_template};
use nervix_connector_websockets::{CompiledSignalingProtocol, SignalingProtobufDescriptors};

use super::*;

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum RuntimeResourceError {
    #[error("protobuf descriptor configuration key '{key}' is unsupported")]
    UnsupportedProtobufConfigKey { key: String },
    #[error("protobuf resource '{resource}' version {version} has an invalid source path '{path}'")]
    InvalidProtobufSourcePath {
        resource: ResourceName,
        version: u64,
        path: String,
    },
    #[error("protobuf resource '{resource}' version {version} contains no source files")]
    MissingProtobufSources {
        resource: ResourceName,
        version: u64,
    },
    #[error(
        "protobuf resource '{resource}' version {version} has an invalid include path '{path}'"
    )]
    InvalidProtobufIncludePath {
        resource: ResourceName,
        version: u64,
        path: String,
    },
    #[error("failed to compile protobuf resource '{resource}' version {version}")]
    ProtobufCompilation {
        resource: ResourceName,
        version: u64,
    },
    #[error("failed to read protobuf resource directory '{directory}'")]
    ReadProtobufDirectory { directory: PathBuf },
    #[error("failed to read an entry in protobuf resource directory '{directory}'")]
    ReadProtobufDirectoryEntry { directory: PathBuf },
    #[error("failed to inspect protobuf resource path '{path}'")]
    InspectProtobufPath { path: PathBuf },
    #[error(
        "protobuf descriptors for resource '{resource}' in domain '{domain}' require a resource \
         store"
    )]
    ProtobufStoreUnavailable {
        domain: DomainName,
        resource: ResourceName,
    },
    #[error(
        "protobuf descriptor compilation for resource '{resource}' version {version} did not \
         complete"
    )]
    ProtobufTask {
        resource: ResourceName,
        version: u64,
    },
    #[error("protobuf resource '{resource}' version {version} produced an invalid descriptor set")]
    InvalidProtobufDescriptorSet {
        resource: ResourceName,
        version: u64,
    },
    #[error("client resource '{resource}' in domain '{domain}' requires a resource store")]
    ClientResourceStoreUnavailable {
        domain: DomainName,
        resource: ResourceName,
    },
    #[error(
        "failed to create the mount root for client resource '{resource}' in domain '{domain}'"
    )]
    ClientMountRoot {
        domain: DomainName,
        resource: ResourceName,
    },
    #[error(
        "client resource '{resource}' version {version} in domain '{domain}' has no content root"
    )]
    ClientMountContentMissing {
        domain: DomainName,
        resource: ResourceName,
        version: u64,
    },
    #[error("failed to mount client resource '{resource}' version {version} in domain '{domain}'")]
    ClientMount {
        domain: DomainName,
        resource: ResourceName,
        version: u64,
    },
    #[error("client resource mounts are unsupported on this platform")]
    #[cfg(not(unix))]
    ClientMountUnsupported,
    #[error("client configuration entry '{key}' has an invalid template")]
    ClientConfigTemplate { key: String },
}

#[derive(Debug)]
pub(super) struct ProtobufDescriptorCompileConfig {
    pub(super) files: Vec<String>,
    pub(super) includes: Vec<String>,
}

impl ProtobufDescriptorCompileConfig {
    pub(super) fn from_entries(
        entries: &[ClientConfigEntry],
    ) -> error_stack::Result<Self, RuntimeResourceError> {
        let mut files = Vec::new();
        let mut includes = Vec::new();
        for entry in entries {
            match entry.key.to_ascii_lowercase().as_str() {
                "file" | "files" => Self::append_paths(&mut files, &entry.value),
                "include" | "includes" => Self::append_paths(&mut includes, &entry.value),
                other => {
                    return Err(Report::new(
                        RuntimeResourceError::UnsupportedProtobufConfigKey {
                            key: other.to_string(),
                        },
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
    ) -> error_stack::Result<prost_types::FileDescriptorSet, RuntimeResourceError> {
        let files = if self.files.is_empty() {
            Self::collect_resource_proto_files(store, id)?
        } else {
            self.files
                .iter()
                .map(|path| {
                    store.resolve_content_path(id, path).map_err(|error| {
                        Report::new(RuntimeResourceError::InvalidProtobufSourcePath {
                            resource: id.identifier.clone(),
                            version: id.version,
                            path: path.clone(),
                        })
                        .attach_printable(error)
                    })
                })
                .collect::<error_stack::Result<Vec<_>, RuntimeResourceError>>()?
        };
        if files.is_empty() {
            return Err(Report::new(RuntimeResourceError::MissingProtobufSources {
                resource: id.identifier.clone(),
                version: id.version,
            }));
        }
        let includes = if self.includes.is_empty() {
            vec![store.content_root(id)]
        } else {
            self.includes
                .iter()
                .map(|path| {
                    store.resolve_content_path(id, path).map_err(|error| {
                        Report::new(RuntimeResourceError::InvalidProtobufIncludePath {
                            resource: id.identifier.clone(),
                            version: id.version,
                            path: path.clone(),
                        })
                        .attach_printable(error)
                    })
                })
                .collect::<error_stack::Result<Vec<_>, RuntimeResourceError>>()?
        };

        protox::compile(files, includes).map_err(|error| {
            Report::new(RuntimeResourceError::ProtobufCompilation {
                resource: id.identifier.clone(),
                version: id.version,
            })
            .attach_printable(error)
        })
    }

    pub(super) fn collect_resource_proto_files(
        store: &ResourceStore,
        id: &ResourceId,
    ) -> error_stack::Result<Vec<PathBuf>, RuntimeResourceError> {
        let root = store.content_root(id);
        let mut files = BTreeSet::new();
        Self::collect_proto_files_recursive(&root, &mut files)?;
        Ok(files.into_iter().collect())
    }

    pub(super) fn collect_proto_files_recursive(
        directory: &PathBuf,
        files: &mut BTreeSet<PathBuf>,
    ) -> error_stack::Result<(), RuntimeResourceError> {
        let entries = std::fs::read_dir(directory).map_err(|error| {
            Report::new(RuntimeResourceError::ReadProtobufDirectory {
                directory: directory.clone(),
            })
            .attach_printable(error)
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                Report::new(RuntimeResourceError::ReadProtobufDirectoryEntry {
                    directory: directory.clone(),
                })
                .attach_printable(error)
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|error| {
                Report::new(RuntimeResourceError::InspectProtobufPath { path: path.clone() })
                    .attach_printable(error)
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
        let protobuf_descriptors = if let CodecWireFormat::Protobuf(config) = &codec.wire_format {
            let build_error = |reason: String| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason,
            };
            let resource = ResourceId::new(
                domain.clone(),
                config.resource.clone(),
                config.resource_version,
            );
            let pool = self
                .compile_protobuf_descriptor_pool(resource, &config.config)
                .await
                .map_err(|error| build_error(error.to_string()))?;
            let message = pool
                .message(&config.message)
                .map_err(|error| build_error(error.to_string()))?;
            let batch_message = match &config.batch_message {
                Some(batch_message) => Some(
                    pool.message(batch_message)
                        .map_err(|error| build_error(error.to_string()))?,
                ),
                None => None,
            };
            Some(ProtobufCodecDescriptors {
                message,
                batch_message,
            })
        } else {
            None
        };

        compile_codec_with_protobuf(codec, schema, wire_format, protobuf_descriptors).map_err(
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
            let resource = ResourceId::new(
                domain.clone(),
                config.resource.clone(),
                config.resource_version,
            );
            let pool = self
                .compile_protobuf_descriptor_pool(resource, &config.config)
                .await
                .map_err(|error| build_error(error.to_string()))?;
            Some(SignalingProtobufDescriptors {
                send: pool
                    .message(&config.send_message)
                    .map_err(|error| build_error(error.to_string()))?,
                wait: pool
                    .message(&config.wait_message)
                    .map_err(|error| build_error(error.to_string()))?,
            })
        } else {
            None
        };

        CompiledSignalingProtocol::compile(protocol, descriptors)
            .map(Arc::new)
            .map_err(|error| build_error(error.to_string()))
    }

    /// Compiles the descriptors of the one resource version a codec or signaling protocol pins.
    pub(super) async fn compile_protobuf_descriptor_pool(
        &self,
        id: ResourceId,
        config: &[ClientConfigEntry],
    ) -> error_stack::Result<ProtobufDescriptorPool, RuntimeResourceError> {
        let Some(store) = self.inner.resource_store.load_full() else {
            return Err(Report::new(
                RuntimeResourceError::ProtobufStoreUnavailable {
                    domain: id.domain,
                    resource: id.identifier,
                },
            ));
        };
        let compile_config = ProtobufDescriptorCompileConfig::from_entries(config)?;
        let task_resource = id.identifier.clone();
        let task_version = id.version;
        let descriptor_id = id.clone();
        let file_descriptor_set = tokio::task::spawn_blocking(move || {
            compile_config.compile_descriptor_set(&store, &descriptor_id)
        })
        .await
        .map_err(|error| {
            Report::new(RuntimeResourceError::ProtobufTask {
                resource: task_resource,
                version: task_version,
            })
            .attach_printable(error)
        })??;

        ProtobufDescriptorPool::from_file_descriptor_set(file_descriptor_set).map_err(|error| {
            Report::new(RuntimeResourceError::InvalidProtobufDescriptorSet {
                resource: id.identifier,
                version: id.version,
            })
            .attach_printable(error)
        })
    }

    pub(crate) fn attach_resource_store(&self, resource_store: StdArc<ResourceStore>) {
        self.inner.resource_store.store(Some(resource_store));
    }

    pub(crate) fn resolve_client_config(
        &self,
        domain: &DomainName,
        mount: Option<&ClientResourceMount>,
        config: &[nervix_models::ClientConfigEntry],
    ) -> error_stack::Result<ResolvedClientConfig, RuntimeResourceError> {
        self.resolve_client_config_with_template_vars(domain, mount, config, BTreeMap::default())
    }

    pub(in crate::runtime) fn resolve_client_config_with_instance(
        &self,
        domain: &DomainName,
        mount: Option<&ClientResourceMount>,
        config: &[nervix_models::ClientConfigEntry],
        instance: u64,
    ) -> error_stack::Result<ResolvedClientConfig, RuntimeResourceError> {
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
        mount: Option<&ClientResourceMount>,
        config: &[nervix_models::ClientConfigEntry],
        mut context: BTreeMap<String, String>,
    ) -> error_stack::Result<ResolvedClientConfig, RuntimeResourceError> {
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
                )
                .map_err(|_| {
                    Report::new(RuntimeResourceError::ClientConfigTemplate {
                        key: entry.key.clone(),
                    })
                })?;
            }
            return Ok(ResolvedClientConfig {
                entries,
                mounts: None,
            });
        };

        let Some(resource_store) = self.inner.resource_store.load_full() else {
            return Err(Report::new(
                RuntimeResourceError::ClientResourceStoreUnavailable {
                    domain: domain.clone(),
                    resource: mount.resource.clone(),
                },
            ));
        };
        let mount_root = tempfile::tempdir().map_err(|source| {
            Report::new(RuntimeResourceError::ClientMountRoot {
                domain: domain.clone(),
                resource: mount.resource.clone(),
            })
            .attach_printable(source)
        })?;
        let mut aliases = BTreeMap::new();
        let id = ResourceId::new(domain.clone(), mount.resource.clone(), mount.version);
        let source_root = resource_store.content_root(&id);
        if !source_root.exists() {
            return Err(Report::new(
                RuntimeResourceError::ClientMountContentMissing {
                    domain: domain.clone(),
                    resource: mount.resource.clone(),
                    version: id.version,
                },
            ));
        }
        let mount_path = mount_root.path().join(mount.resource.as_str());
        #[cfg(unix)]
        std::os::unix::fs::symlink(&source_root, &mount_path).map_err(|source| {
            Report::new(RuntimeResourceError::ClientMount {
                domain: domain.clone(),
                resource: mount.resource.clone(),
                version: id.version,
            })
            .attach_printable(source)
        })?;
        #[cfg(not(unix))]
        {
            return Err(Report::new(RuntimeResourceError::ClientMountUnsupported));
        }
        aliases.insert(mount.resource.as_str().to_string(), mount_path);

        for (resource_name, mount_path) in &aliases {
            context.insert(
                resource_name.clone(),
                mount_path.to_string_lossy().into_owned(),
            );
        }
        for entry in &mut entries {
            entry.value =
                render_client_config_template(&template_engine, &entry.key, &entry.value, &context)
                    .map_err(|_| {
                        Report::new(RuntimeResourceError::ClientConfigTemplate {
                            key: entry.key.clone(),
                        })
                    })?;
        }

        Ok(ResolvedClientConfig {
            entries,
            mounts: Some(Arc::new(ClientResourceMounts::new(mount_root, aliases))),
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
        ClientConfigEntry, ClientResourceMount, ClusterNodeName, DomainName, ResourceId, Timestamp,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::resource::ResourceStore;

    fn mount(resource: &str, version: u64) -> ClientResourceMount {
        ClientResourceMount {
            resource: named(resource),
            version,
        }
    }

    #[test]
    fn protobuf_descriptor_configuration_reports_typed_failures() {
        let unsupported = ProtobufDescriptorCompileConfig::from_entries(&[ClientConfigEntry {
            key: "package".to_string(),
            value: "events".to_string(),
        }])
        .expect_err("unsupported protobuf configuration must fail");
        assert!(matches!(
            unsupported.current_context(),
            RuntimeResourceError::UnsupportedProtobufConfigKey { key } if key == "package"
        ));

        let store_root = tempdir().expect("resource store tempdir");
        let store = ResourceStore::open(store_root.path(), Executor::default())
            .expect("resource store should open");
        let id = ResourceId::new(
            DomainName::parse("tenant").expect("valid domain"),
            named("events_proto"),
            1,
        );

        let invalid_source = ProtobufDescriptorCompileConfig {
            files: vec!["../schema.proto".to_string()],
            includes: Vec::new(),
        }
        .compile_descriptor_set(&store, &id)
        .expect_err("a parent source path must fail");
        assert!(matches!(
            invalid_source.current_context(),
            RuntimeResourceError::InvalidProtobufSourcePath {
                resource,
                version: 1,
                path,
            } if resource.as_str() == "events_proto" && path == "../schema.proto"
        ));

        std::fs::create_dir_all(store.content_root(&id))
            .expect("empty protobuf content root should be created");
        let missing_sources = ProtobufDescriptorCompileConfig {
            files: Vec::new(),
            includes: Vec::new(),
        }
        .compile_descriptor_set(&store, &id)
        .expect_err("an empty protobuf resource must fail");
        assert!(matches!(
            missing_sources.current_context(),
            RuntimeResourceError::MissingProtobufSources {
                resource,
                version: 1,
            } if resource.as_str() == "events_proto"
        ));

        let invalid_include = ProtobufDescriptorCompileConfig {
            files: vec!["schema.proto".to_string()],
            includes: vec!["../includes".to_string()],
        }
        .compile_descriptor_set(&store, &id)
        .expect_err("a parent include path must fail");
        assert!(matches!(
            invalid_include.current_context(),
            RuntimeResourceError::InvalidProtobufIncludePath {
                resource,
                version: 1,
                path,
            } if resource.as_str() == "events_proto" && path == "../includes"
        ));

        let compilation = ProtobufDescriptorCompileConfig {
            files: vec!["missing.proto".to_string()],
            includes: Vec::new(),
        }
        .compile_descriptor_set(&store, &id)
        .expect_err("a missing protobuf source must fail compilation");
        assert!(matches!(
            compilation.current_context(),
            RuntimeResourceError::ProtobufCompilation {
                resource,
                version: 1,
            } if resource.as_str() == "events_proto"
        ));

        let missing_directory = store_root.path().join("missing-directory");
        let directory = ProtobufDescriptorCompileConfig::collect_proto_files_recursive(
            &missing_directory,
            &mut BTreeSet::new(),
        )
        .expect_err("an unreadable protobuf directory must fail");
        assert!(matches!(
            directory.current_context(),
            RuntimeResourceError::ReadProtobufDirectory { directory }
                if directory == &missing_directory
        ));
    }

    #[tokio::test]
    async fn protobuf_descriptor_pool_requires_a_resource_store() {
        let domain = DomainName::parse("tenant").expect("valid domain");
        let resource = named::<ResourceName>("events_proto");
        let error = Runtime::new()
            .compile_protobuf_descriptor_pool(
                ResourceId::new(domain.clone(), resource.clone(), 1),
                &[],
            )
            .await
            .expect_err("protobuf compilation without a resource store must fail");

        assert!(matches!(
            error.current_context(),
            RuntimeResourceError::ProtobufStoreUnavailable {
                domain: error_domain,
                resource: error_resource,
            } if error_domain == &domain && error_resource == &resource
        ));
    }

    #[test]
    fn client_resource_mount_failures_preserve_domain_and_resource() {
        let runtime = Runtime::new();
        let domain = DomainName::parse("tenant").expect("valid domain");
        let resource = named::<ResourceName>("dev_tls");
        let resource_mount = ClientResourceMount {
            resource: resource.clone(),
            version: 1,
        };
        let unavailable = runtime
            .resolve_client_config(&domain, Some(&resource_mount), &[])
            .expect_err("a client mount without a resource store must fail");
        assert!(matches!(
            unavailable.current_context(),
            RuntimeResourceError::ClientResourceStoreUnavailable {
                domain: error_domain,
                resource: error_resource,
            } if error_domain == &domain && error_resource == &resource
        ));

        let store_root = tempdir().expect("resource store tempdir");
        let store = ResourceStore::open(store_root.path(), Executor::default())
            .expect("resource store should open");
        runtime.attach_resource_store(StdArc::new(store));
        let missing_content = runtime
            .resolve_client_config(&domain, Some(&resource_mount), &[])
            .expect_err("a client mount without installed content must fail");
        assert!(matches!(
            missing_content.current_context(),
            RuntimeResourceError::ClientMountContentMissing {
                domain: error_domain,
                resource: error_resource,
                version: 1,
            } if error_domain == &domain && error_resource == &resource
        ));
    }

    #[tokio::test]
    async fn client_resource_mounts_expand_into_runtime_paths() {
        let store_root = tempdir().expect("resource store tempdir");
        let source_root = tempdir().expect("resource source tempdir");
        let ca_path = source_root.path().join("ca.pem");
        std::fs::write(&ca_path, "test-ca").expect("ca file should be written");

        let mount_domain = DomainName::parse("tenant").expect("valid domain");
        let store = ResourceStore::open(store_root.path(), Executor::default())
            .expect("resource store should open");
        store
            .install_from_directory(
                ResourceId::new(mount_domain.clone(), named("dev_tls"), 1),
                source_root.path(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(0),
            )
            .await
            .expect("resource version should install");

        let later_source_root = tempdir().expect("later resource source tempdir");
        std::fs::write(later_source_root.path().join("ca.pem"), "later-ca")
            .expect("later ca file should be written");
        store
            .install_from_directory(
                ResourceId::new(mount_domain.clone(), named("dev_tls"), 2),
                later_source_root.path(),
                ClusterNodeName::parse("node-1").expect("valid name"),
                Timestamp::from_unix_nanos(1),
            )
            .await
            .expect("later resource version should install");

        let runtime = Runtime::new();
        runtime.attach_resource_store(StdArc::new(store));
        let pinned_mount = mount("dev_tls", 1);

        let resolved = runtime
            .resolve_client_config(
                &mount_domain,
                Some(&pinned_mount),
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

        let template_error = runtime
            .resolve_client_config(
                &mount_domain,
                Some(&pinned_mount),
                &[ClientConfigEntry {
                    key: "tls_ca_file".to_string(),
                    value: "{{missing}}/ca.pem".to_string(),
                }],
            )
            .expect_err("an unknown mounted-resource placeholder must fail");
        assert!(matches!(
            template_error.current_context(),
            RuntimeResourceError::ClientConfigTemplate { key } if key == "tls_ca_file"
        ));

        let other_domain = DomainName::parse("other").expect("valid domain");
        let error = runtime
            .resolve_client_config(
                &other_domain,
                Some(&pinned_mount),
                &[ClientConfigEntry {
                    key: "tls_ca_file".to_string(),
                    value: "{{ dev_tls }}/ca.pem".to_string(),
                }],
            )
            .expect_err("another domain must not see this domain's resource");
        assert!(matches!(
            error.current_context(),
            RuntimeResourceError::ClientMountContentMissing {
                domain,
                resource,
                version: 1,
            } if domain == &other_domain && resource.as_str() == "dev_tls"
        ));
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
        assert!(matches!(
            error.current_context(),
            RuntimeResourceError::ClientConfigTemplate { key }
                if key == "tls_ca_file"
        ));
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
