use std::{
    collections::BTreeMap,
    fs, io,
    path::{Component, Path, PathBuf},
};

use error_stack::Report;
use meticulous::OptionExt as _;
use serde::Serialize;
use thiserror::Error;

use crate::definition::{BenchmarkDefinition, DefinitionError, is_slug};

const BENCHMARKS_DIRECTORY: &str = "benches/benchmarks";
const BENCHMARK_MANIFEST: &str = "benchmark.toml";

#[derive(Debug, Clone)]
pub struct BenchmarkCatalog {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LoadedBenchmark {
    slug: String,
    directory: PathBuf,
    definition: BenchmarkDefinition,
    templates: BTreeMap<String, String>,
    after_start_templates: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy)]
pub struct KafkaRenderInputs<'a> {
    pub kafka_bootstrap_servers: &'a str,
    pub input_topic: &'a str,
    pub output_topic: &'a str,
    pub consumer_group: &'a str,
    pub lane_count: u32,
    pub dependency_endpoints: &'a BTreeMap<String, String>,
}

#[derive(Serialize)]
struct BenchmarkRenderContext<'a> {
    kafka_bootstrap_servers: &'a str,
    input_topic: &'a str,
    output_topic: &'a str,
    consumer_group: &'a str,
    lanes: Vec<u32>,
    parameters: &'a toml::Table,
    dependencies: &'a BTreeMap<String, String>,
}

#[derive(Debug, Error)]
pub enum BenchmarkError {
    #[error(
        "invalid benchmark slug '{slug}': expected lowercase letters and digits separated by \
         single hyphens"
    )]
    InvalidSlug { slug: String },

    #[error("failed to {operation} '{}': {source}", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to parse benchmark manifest '{}': {source}", path.display())]
    ParseManifest {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("benchmark '{slug}' directory escapes the benchmark catalog")]
    EscapingDirectory { slug: String },

    #[error("benchmark '{slug}' is invalid: {error:#}")]
    InvalidDefinition {
        slug: String,
        error: Report<DefinitionError>,
    },

    #[error(
        "benchmark '{slug}' implementation '{implementation}' has invalid template path '{}': {reason}",
        path.display()
    )]
    InvalidTemplatePath {
        slug: String,
        implementation: String,
        path: PathBuf,
        reason: String,
    },

    #[error(
        "benchmark '{slug}' implementation '{implementation}' template '{}' is invalid: {source:#}",
        path.display()
    )]
    CompileTemplate {
        slug: String,
        implementation: String,
        path: PathBuf,
        #[source]
        source: Box<upon::Error>,
    },

    #[error("benchmark '{slug}' has no implementation named '{implementation}'")]
    UnknownImplementation {
        slug: String,
        implementation: String,
    },

    #[error("failed to render benchmark '{slug}' implementation '{implementation}': {source:#}")]
    RenderTemplate {
        slug: String,
        implementation: String,
        #[source]
        source: Box<upon::Error>,
    },
}

impl BenchmarkCatalog {
    pub fn from_repository_root(repository_root: impl AsRef<Path>) -> Self {
        Self {
            root: repository_root.as_ref().join(BENCHMARKS_DIRECTORY),
        }
    }

    pub fn from_benchmarks_root(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn discover(&self) -> Result<Vec<LoadedBenchmark>, BenchmarkError> {
        let root = canonicalize(&self.root, "open benchmark catalog")?;
        let entries = fs::read_dir(&root).map_err(|source| BenchmarkError::Io {
            operation: "read benchmark catalog",
            path: root.clone(),
            source,
        })?;
        let mut slugs = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| BenchmarkError::Io {
                operation: "read benchmark catalog entry",
                path: root.clone(),
                source,
            })?;
            let file_type = entry.file_type().map_err(|source| BenchmarkError::Io {
                operation: "inspect benchmark catalog entry",
                path: entry.path(),
                source,
            })?;
            if !file_type.is_dir() {
                continue;
            }
            let slug =
                entry
                    .file_name()
                    .into_string()
                    .map_err(|value| BenchmarkError::InvalidSlug {
                        slug: value.to_string_lossy().into_owned(),
                    })?;
            validate_slug(&slug)?;
            slugs.push(slug);
        }
        slugs.sort_unstable();
        slugs
            .into_iter()
            .map(|slug| self.load_from_root(&root, &slug))
            .collect()
    }

    pub fn load(&self, slug: &str) -> Result<LoadedBenchmark, BenchmarkError> {
        validate_slug(slug)?;
        let root = canonicalize(&self.root, "open benchmark catalog")?;
        self.load_from_root(&root, slug)
    }

    fn load_from_root(
        &self,
        canonical_root: &Path,
        slug: &str,
    ) -> Result<LoadedBenchmark, BenchmarkError> {
        let directory_path = canonical_root.join(slug);
        let directory = canonicalize(&directory_path, "open benchmark directory")?;
        if !directory.starts_with(canonical_root) {
            return Err(BenchmarkError::EscapingDirectory {
                slug: slug.to_string(),
            });
        }

        let manifest_path = directory.join(BENCHMARK_MANIFEST);
        let manifest = read_contained_utf8(
            slug,
            "manifest",
            &directory,
            Path::new(BENCHMARK_MANIFEST),
            &manifest_path,
        )?;
        let definition = toml::from_str::<BenchmarkDefinition>(&manifest).map_err(|source| {
            BenchmarkError::ParseManifest {
                path: manifest_path.clone(),
                source,
            }
        })?;
        definition
            .validate(slug)
            .map_err(|error| BenchmarkError::InvalidDefinition {
                slug: slug.to_string(),
                error,
            })?;

        let engine = upon::Engine::new();
        let mut templates = BTreeMap::new();
        let mut after_start_templates = BTreeMap::new();
        for (implementation, configuration) in &definition.implementations {
            let relative_path = configuration.template();
            validate_relative_path(slug, implementation, relative_path)?;
            let source_path = directory.join(relative_path);
            let source = read_contained_utf8(
                slug,
                implementation,
                &directory,
                relative_path,
                &source_path,
            )?;
            engine
                .compile(source.as_str())
                .map_err(|source| BenchmarkError::CompileTemplate {
                    slug: slug.to_string(),
                    implementation: implementation.clone(),
                    path: relative_path.to_path_buf(),
                    source: Box::new(source),
                })?;
            templates.insert(implementation.clone(), source);

            if let Some(relative_path) = configuration.after_start_template() {
                validate_relative_path(slug, implementation, relative_path)?;
                let source_path = directory.join(relative_path);
                let source = read_contained_utf8(
                    slug,
                    implementation,
                    &directory,
                    relative_path,
                    &source_path,
                )?;
                engine.compile(source.as_str()).map_err(|source| {
                    BenchmarkError::CompileTemplate {
                        slug: slug.to_string(),
                        implementation: implementation.clone(),
                        path: relative_path.to_path_buf(),
                        source: Box::new(source),
                    }
                })?;
                after_start_templates.insert(implementation.clone(), source);
            }
        }

        Ok(LoadedBenchmark {
            slug: slug.to_string(),
            directory,
            definition,
            templates,
            after_start_templates,
        })
    }
}

impl LoadedBenchmark {
    pub fn slug(&self) -> &str {
        &self.slug
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn definition(&self) -> &BenchmarkDefinition {
        &self.definition
    }

    pub fn render_implementation(
        &self,
        implementation: &str,
        inputs: KafkaRenderInputs<'_>,
    ) -> error_stack::Result<String, BenchmarkError> {
        self.render_implementation_with_parameters(
            implementation,
            inputs,
            &self.definition.parameters,
        )
    }

    pub fn render_implementation_with_parameters(
        &self,
        implementation: &str,
        inputs: KafkaRenderInputs<'_>,
        parameters: &toml::Table,
    ) -> error_stack::Result<String, BenchmarkError> {
        let source = self.templates.get(implementation).ok_or_else(|| {
            BenchmarkError::UnknownImplementation {
                slug: self.slug.clone(),
                implementation: implementation.to_string(),
            }
        })?;
        self.render_template(
            implementation,
            source,
            self.definition.implementations[implementation].template(),
            inputs,
            parameters,
        )
    }

    pub fn render_after_start_with_parameters(
        &self,
        implementation: &str,
        inputs: KafkaRenderInputs<'_>,
        parameters: &toml::Table,
    ) -> error_stack::Result<Option<String>, BenchmarkError> {
        let Some(source) = self.after_start_templates.get(implementation) else {
            return Ok(None);
        };
        let path = self.definition.implementations[implementation]
            .after_start_template()
            .verified("the source map is populated only from an implementation template");
        self.render_template(implementation, source, path, inputs, parameters)
            .map(Some)
    }

    fn render_template(
        &self,
        implementation: &str,
        source: &str,
        path: &Path,
        inputs: KafkaRenderInputs<'_>,
        parameters: &toml::Table,
    ) -> error_stack::Result<String, BenchmarkError> {
        let context = BenchmarkRenderContext {
            kafka_bootstrap_servers: inputs.kafka_bootstrap_servers,
            input_topic: inputs.input_topic,
            output_topic: inputs.output_topic,
            consumer_group: inputs.consumer_group,
            lanes: (0..inputs.lane_count).collect(),
            parameters,
            dependencies: inputs.dependency_endpoints,
        };
        let engine = upon::Engine::new();
        let template =
            engine
                .compile(source)
                .map_err(|source| BenchmarkError::CompileTemplate {
                    slug: self.slug.clone(),
                    implementation: implementation.to_string(),
                    path: path.to_path_buf(),
                    source: Box::new(source),
                })?;
        let rendered = template
            .render(&engine, &context)
            .to_string()
            .map_err(|source| BenchmarkError::RenderTemplate {
                slug: self.slug.clone(),
                implementation: implementation.to_string(),
                source: Box::new(source),
            })?;
        Ok(rendered)
    }
}

fn validate_slug(slug: &str) -> Result<(), BenchmarkError> {
    if is_slug(slug) {
        Ok(())
    } else {
        Err(BenchmarkError::InvalidSlug {
            slug: slug.to_string(),
        })
    }
}

fn validate_relative_path(
    slug: &str,
    implementation: &str,
    path: &Path,
) -> Result<(), BenchmarkError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(BenchmarkError::InvalidTemplatePath {
            slug: slug.to_string(),
            implementation: implementation.to_string(),
            path: path.to_path_buf(),
            reason: "template must be a non-empty contained relative path".to_string(),
        });
    }
    Ok(())
}

fn read_contained_utf8(
    slug: &str,
    implementation: &str,
    directory: &Path,
    relative_path: &Path,
    source_path: &Path,
) -> Result<String, BenchmarkError> {
    let canonical_path = canonicalize(source_path, "open benchmark template")?;
    if !canonical_path.starts_with(directory) {
        return Err(BenchmarkError::InvalidTemplatePath {
            slug: slug.to_string(),
            implementation: implementation.to_string(),
            path: relative_path.to_path_buf(),
            reason: "resolved path escapes the benchmark directory".to_string(),
        });
    }
    let metadata = fs::metadata(&canonical_path).map_err(|source| BenchmarkError::Io {
        operation: "inspect benchmark template",
        path: canonical_path.clone(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(BenchmarkError::InvalidTemplatePath {
            slug: slug.to_string(),
            implementation: implementation.to_string(),
            path: relative_path.to_path_buf(),
            reason: "resolved path is not a regular file".to_string(),
        });
    }
    fs::read_to_string(&canonical_path).map_err(|source| BenchmarkError::Io {
        operation: "read benchmark template",
        path: canonical_path,
        source,
    })
}

fn canonicalize(path: &Path, operation: &'static str) -> Result<PathBuf, BenchmarkError> {
    path.canonicalize().map_err(|source| BenchmarkError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    })
}
