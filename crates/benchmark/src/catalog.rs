use std::{
    collections::BTreeMap,
    fmt, fs,
    path::{Component, Path, PathBuf},
};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use serde::Serialize;
use thiserror::Error;

use crate::definition::{BenchmarkDefinition, is_slug};

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
    lane_count: u32,
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

    /// The I/O failure is beneath.
    #[error("failed to open benchmark catalog '{}'", path.display())]
    OpenCatalog { path: PathBuf },

    /// The I/O failure is beneath.
    #[error("failed to read benchmark catalog '{}'", path.display())]
    ReadCatalog { path: PathBuf },

    /// The I/O failure is beneath.
    #[error("failed to inspect benchmark catalog entry '{}'", path.display())]
    InspectCatalogEntry { path: PathBuf },

    /// The I/O failure is beneath.
    #[error("failed to open benchmark directory '{}'", path.display())]
    OpenBenchmark { path: PathBuf },

    #[error("benchmark '{slug}' directory escapes the benchmark catalog")]
    EscapingDirectory { slug: String },

    /// The [`BenchmarkFileError`] beneath says why the manifest could not be read.
    #[error("failed to read benchmark '{slug}' manifest '{}'", path.display())]
    ReadManifest { slug: String, path: PathBuf },

    /// The TOML decoder's failure is beneath.
    #[error("failed to parse benchmark manifest '{}'", path.display())]
    ParseManifest { path: PathBuf },

    /// The [`crate::DefinitionError`] beneath names the declaration that is invalid.
    #[error("benchmark '{slug}' is invalid")]
    InvalidDefinition { slug: String },

    #[error(
        "benchmark '{slug}' implementation '{implementation}' has invalid template path '{}': a \
         template must be a non-empty contained relative path",
        path.display()
    )]
    InvalidTemplatePath {
        slug: String,
        implementation: String,
        path: PathBuf,
    },

    /// The [`BenchmarkFileError`] beneath says why the template could not be read.
    #[error(
        "failed to read benchmark '{slug}' implementation '{implementation}' template '{}'",
        path.display()
    )]
    ReadTemplate {
        slug: String,
        implementation: String,
        path: PathBuf,
    },

    /// The [`TemplateDiagnostic`] beneath locates the failure in the template.
    #[error(
        "benchmark '{slug}' implementation '{implementation}' template '{}' is invalid",
        path.display()
    )]
    CompileTemplate {
        slug: String,
        implementation: String,
        path: PathBuf,
    },

    #[error("benchmark '{slug}' has no implementation named '{implementation}'")]
    UnknownImplementation {
        slug: String,
        implementation: String,
    },

    /// The [`TemplateDiagnostic`] beneath locates the failure in the template.
    #[error(
        "failed to render benchmark '{slug}' implementation '{implementation}' template '{}'",
        path.display()
    )]
    RenderTemplate {
        slug: String,
        implementation: String,
        path: PathBuf,
    },
}

/// Why a file a benchmark declares could not be read from inside the benchmark's directory,
/// beneath the [`BenchmarkError`] that names the file.
#[derive(Debug, Error)]
pub enum BenchmarkFileError {
    /// The I/O failure is beneath.
    #[error("the path does not resolve")]
    Resolve,

    #[error("the path resolves outside the benchmark directory")]
    OutsideDirectory,

    /// The I/O failure is beneath.
    #[error("the file's metadata cannot be read")]
    Inspect,

    #[error("the path is not a regular file")]
    NotAFile,

    /// The I/O failure is beneath.
    #[error("the file cannot be read as UTF-8 text")]
    Read,
}

/// A template engine failure, shown with the line of the template it points at.
#[derive(Debug)]
pub struct TemplateDiagnostic(upon::Error);

impl fmt::Display for TemplateDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The engine's alternate form adds the failing template line and marks the span in it,
        // which its plain form leaves out.
        write!(formatter, "{:#}", self.0)
    }
}

impl std::error::Error for TemplateDiagnostic {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

/// One benchmark's canonical directory, which every file the benchmark declares must stay
/// inside.
struct BenchmarkDirectory<'a> {
    slug: &'a str,
    path: PathBuf,
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

    pub fn discover(&self) -> error_stack::Result<Vec<LoadedBenchmark>, BenchmarkError> {
        let root = self.canonical_root()?;
        let entries = fs::read_dir(&root)
            .change_context_lazy(|| BenchmarkError::ReadCatalog { path: root.clone() })?;
        let mut slugs = Vec::new();
        for entry in entries {
            let entry =
                entry.change_context_lazy(|| BenchmarkError::ReadCatalog { path: root.clone() })?;
            let file_type =
                entry
                    .file_type()
                    .change_context_lazy(|| BenchmarkError::InspectCatalogEntry {
                        path: entry.path(),
                    })?;
            if !file_type.is_dir() {
                continue;
            }
            let slug = match entry.file_name().into_string() {
                Ok(slug) => slug,
                Err(name) => {
                    return Err(Report::new(BenchmarkError::InvalidSlug {
                        slug: name.to_string_lossy().into_owned(),
                    }));
                }
            };
            validate_slug(&slug)?;
            slugs.push(slug);
        }
        slugs.sort_unstable();
        slugs
            .into_iter()
            .map(|slug| self.load_from_root(&root, &slug))
            .collect()
    }

    pub fn load(&self, slug: &str) -> error_stack::Result<LoadedBenchmark, BenchmarkError> {
        validate_slug(slug)?;
        let root = self.canonical_root()?;
        self.load_from_root(&root, slug)
    }

    fn canonical_root(&self) -> error_stack::Result<PathBuf, BenchmarkError> {
        self.root
            .canonicalize()
            .change_context_lazy(|| BenchmarkError::OpenCatalog {
                path: self.root.clone(),
            })
    }

    fn load_from_root(
        &self,
        canonical_root: &Path,
        slug: &str,
    ) -> error_stack::Result<LoadedBenchmark, BenchmarkError> {
        let directory_path = canonical_root.join(slug);
        let canonical_directory = directory_path.canonicalize().change_context_lazy(|| {
            BenchmarkError::OpenBenchmark {
                path: directory_path.clone(),
            }
        })?;
        if !canonical_directory.starts_with(canonical_root) {
            return Err(Report::new(BenchmarkError::EscapingDirectory {
                slug: slug.to_string(),
            }));
        }
        let directory = BenchmarkDirectory {
            slug,
            path: canonical_directory,
        };

        let manifest_path = directory.path.join(BENCHMARK_MANIFEST);
        let manifest = directory
            .read_contained(Path::new(BENCHMARK_MANIFEST))
            .change_context_lazy(|| BenchmarkError::ReadManifest {
                slug: slug.to_string(),
                path: manifest_path.clone(),
            })?;
        let definition = toml::from_str::<BenchmarkDefinition>(&manifest).change_context(
            BenchmarkError::ParseManifest {
                path: manifest_path,
            },
        )?;
        definition
            .validate(slug)
            .change_context_lazy(|| BenchmarkError::InvalidDefinition {
                slug: slug.to_string(),
            })?;

        let engine = upon::Engine::new();
        let mut templates = BTreeMap::new();
        let mut after_start_templates = BTreeMap::new();
        for (implementation, configuration) in &definition.implementations {
            let source =
                directory.load_template(&engine, implementation, configuration.template())?;
            templates.insert(implementation.clone(), source);

            if let Some(relative_path) = configuration.after_start_template() {
                let source = directory.load_template(&engine, implementation, relative_path)?;
                after_start_templates.insert(implementation.clone(), source);
            }
        }

        Ok(LoadedBenchmark {
            slug: slug.to_string(),
            directory: directory.path,
            definition,
            templates,
            after_start_templates,
        })
    }
}

impl BenchmarkDirectory<'_> {
    /// Reads and compiles one implementation template, so a template that cannot render is
    /// refused when the benchmark loads rather than when a run starts.
    fn load_template(
        &self,
        engine: &upon::Engine<'_>,
        implementation: &str,
        relative_path: &Path,
    ) -> error_stack::Result<String, BenchmarkError> {
        if !is_contained_relative_path(relative_path) {
            return Err(Report::new(BenchmarkError::InvalidTemplatePath {
                slug: self.slug.to_string(),
                implementation: implementation.to_string(),
                path: relative_path.to_path_buf(),
            }));
        }
        let source = self.read_contained(relative_path).change_context_lazy(|| {
            BenchmarkError::ReadTemplate {
                slug: self.slug.to_string(),
                implementation: implementation.to_string(),
                path: relative_path.to_path_buf(),
            }
        })?;
        engine
            .compile(source.as_str())
            .map_err(TemplateDiagnostic)
            .change_context_lazy(|| BenchmarkError::CompileTemplate {
                slug: self.slug.to_string(),
                implementation: implementation.to_string(),
                path: relative_path.to_path_buf(),
            })?;
        Ok(source)
    }

    fn read_contained(
        &self,
        relative_path: &Path,
    ) -> error_stack::Result<String, BenchmarkFileError> {
        let canonical_path = self
            .path
            .join(relative_path)
            .canonicalize()
            .change_context(BenchmarkFileError::Resolve)?;
        if !canonical_path.starts_with(&self.path) {
            return Err(Report::new(BenchmarkFileError::OutsideDirectory));
        }
        let metadata = fs::metadata(&canonical_path).change_context(BenchmarkFileError::Inspect)?;
        if !metadata.is_file() {
            return Err(Report::new(BenchmarkFileError::NotAFile));
        }
        fs::read_to_string(&canonical_path).change_context(BenchmarkFileError::Read)
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
        let Some(source) = self.templates.get(implementation) else {
            return Err(Report::new(BenchmarkError::UnknownImplementation {
                slug: self.slug.clone(),
                implementation: implementation.to_string(),
            }));
        };
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
        let rendered = self.render_template(implementation, source, path, inputs, parameters)?;
        Ok(Some(rendered))
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
            lane_count: inputs.lane_count,
            parameters,
            dependencies: inputs.dependency_endpoints,
        };
        let engine = upon::Engine::new();
        let template = engine
            .compile(source)
            .map_err(TemplateDiagnostic)
            .change_context_lazy(|| BenchmarkError::CompileTemplate {
                slug: self.slug.clone(),
                implementation: implementation.to_string(),
                path: path.to_path_buf(),
            })?;
        let rendered = template
            .render(&engine, &context)
            .to_string()
            .map_err(TemplateDiagnostic)
            .change_context_lazy(|| BenchmarkError::RenderTemplate {
                slug: self.slug.clone(),
                implementation: implementation.to_string(),
                path: path.to_path_buf(),
            })?;
        Ok(rendered)
    }
}

fn validate_slug(slug: &str) -> error_stack::Result<(), BenchmarkError> {
    if is_slug(slug) {
        Ok(())
    } else {
        Err(Report::new(BenchmarkError::InvalidSlug {
            slug: slug.to_string(),
        }))
    }
}

/// Whether `path` names a file below a directory without leaving it: non-empty, relative, and
/// made of plain components only.
fn is_contained_relative_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}
