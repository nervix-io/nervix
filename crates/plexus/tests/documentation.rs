use std::{
    fs,
    path::{Path, PathBuf},
};

use nervix_plexus::{
    client_statement::parse_client_statement_sources, schema::ParseFromSourceError,
};

#[derive(Debug, PartialEq, Eq)]
struct PlexusBlock {
    source_line: usize,
    source: String,
    validation: PlexusBlockValidation,
}

impl PlexusBlock {
    fn parse(&self) -> error_stack::Result<(), ParseFromSourceError> {
        parse_client_statement_sources(&self.source)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlexusBlockValidation {
    Parse,
    Ignore,
}

#[derive(Debug, PartialEq, Eq)]
enum ExtractPlexusBlocksError {
    Unclosed { opening_line: usize },
    UnsupportedAnnotation { line: usize, fence: String },
}

fn extract_plexus_blocks(markdown: &str) -> Result<Vec<PlexusBlock>, ExtractPlexusBlocksError> {
    let mut blocks = Vec::new();
    let mut current = None;

    for (line_index, line) in markdown.lines().enumerate() {
        let line_number = line_index + 1;
        if let Some((opening_line, validation, source)) = &mut current {
            if line.trim() == "```" {
                blocks.push(PlexusBlock {
                    source_line: *opening_line + 1,
                    source: std::mem::take(source),
                    validation: *validation,
                });
                current = None;
            } else {
                source.push_str(line);
                source.push('\n');
            }
        } else {
            let validation = match line.trim() {
                "```plexus" => Some(PlexusBlockValidation::Parse),
                "```plexus,ignore" => Some(PlexusBlockValidation::Ignore),
                fence if fence.starts_with("```plexus") => {
                    return Err(ExtractPlexusBlocksError::UnsupportedAnnotation {
                        line: line_number,
                        fence: fence.to_string(),
                    });
                }
                _ => None,
            };
            if let Some(validation) = validation {
                current = Some((line_number, validation, String::new()));
            }
        }
    }

    if let Some((opening_line, _, _)) = current {
        return Err(ExtractPlexusBlocksError::Unclosed { opening_line });
    }

    Ok(blocks)
}

struct Documentation {
    repository_root: PathBuf,
    root: PathBuf,
}

impl Documentation {
    fn new(repository_root: PathBuf) -> Self {
        let root = repository_root.join("docs/src");
        Self {
            repository_root,
            root,
        }
    }

    fn markdown_paths(&self) -> Vec<PathBuf> {
        let mut directories = vec![self.root.clone()];
        let mut paths = Vec::new();

        while let Some(directory) = directories.pop() {
            let entries = fs::read_dir(&directory)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()));
            for entry in entries {
                let entry = entry.unwrap_or_else(|error| {
                    panic!(
                        "failed to read an entry in {}: {error}",
                        directory.display()
                    )
                });
                let path = entry.path();
                if path.is_dir() {
                    directories.push(path);
                } else if path.extension().is_some_and(|extension| extension == "md") {
                    paths.push(path);
                }
            }
        }

        paths.sort();
        paths
    }

    fn parse_failures(&self) -> DocumentationParseOutcome {
        let mut parsed_block_count = 0;
        let mut ignored_block_count = 0;
        let mut failures = Vec::new();

        for path in self.markdown_paths() {
            let display_path = path.strip_prefix(&self.repository_root).unwrap_or(&path);
            let markdown = fs::read_to_string(&path).unwrap_or_else(|error| {
                panic!("failed to read {}: {error}", display_path.display())
            });
            let blocks = extract_plexus_blocks(&markdown).unwrap_or_else(|error| match error {
                ExtractPlexusBlocksError::Unclosed { opening_line } => panic!(
                    "{}:{opening_line}: unclosed Plexus code block",
                    display_path.display()
                ),
                ExtractPlexusBlocksError::UnsupportedAnnotation { line, fence } => panic!(
                    "{}:{line}: unsupported Plexus code block annotation {fence:?}; use `plexus` \
                     or `plexus,ignore`",
                    display_path.display()
                ),
            });

            for block in blocks {
                match block.validation {
                    PlexusBlockValidation::Parse => {
                        parsed_block_count += 1;
                        if let Err(error) = block.parse() {
                            failures
                                .extend(block.format_error(display_path, error.current_context()));
                        }
                    }
                    PlexusBlockValidation::Ignore => ignored_block_count += 1,
                }
            }
        }

        DocumentationParseOutcome {
            parsed_block_count,
            ignored_block_count,
            failures,
        }
    }
}

/// What one sweep over the documentation found: how many Plexus blocks were parser-checked, how many
/// were explicitly ignored, and the rendered diagnostics for the blocks that failed.
struct DocumentationParseOutcome {
    parsed_block_count: usize,
    ignored_block_count: usize,
    failures: Vec<String>,
}

impl PlexusBlock {
    fn format_error(&self, path: &Path, error: &ParseFromSourceError) -> Vec<String> {
        let (kind, diagnostics) = match error {
            ParseFromSourceError::Lex { diagnostics, .. } => ("lex", diagnostics),
            ParseFromSourceError::Parse { diagnostics, .. } => ("parse", diagnostics),
        };

        let failures = diagnostics
            .iter()
            .map(|diagnostic| {
                let (relative_line, column) = line_and_column(&self.source, diagnostic.span.start);
                format!(
                    "{}:{}:{}: {kind} error: {}",
                    path.display(),
                    self.source_line + relative_line - 1,
                    column,
                    diagnostic.message
                )
            })
            .collect::<Vec<_>>();
        if failures.is_empty() {
            vec![format!(
                "{}:{}:1: {kind} error without diagnostics",
                path.display(),
                self.source_line
            )]
        } else {
            failures
        }
    }
}

fn line_and_column(source: &str, byte_offset: usize) -> (usize, usize) {
    let prefix = &source[..byte_offset.min(source.len())];
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = prefix
        .rsplit_once('\n')
        .map_or(prefix, |(_, current_line)| current_line)
        .chars()
        .count()
        + 1;
    (line, column)
}

#[test]
fn extracts_runnable_and_ignored_plexus_fences_with_source_locations() {
    let markdown = r#"# Example

```text
not Plexus
```

```plexus
CREATE DOMAIN one;
```

```plexus,ignore
CREATE <model>;
```
"#;

    assert_eq!(
        extract_plexus_blocks(markdown).expect("fences should be balanced"),
        vec![
            PlexusBlock {
                source_line: 8,
                source: "CREATE DOMAIN one;\n".to_string(),
                validation: PlexusBlockValidation::Parse,
            },
            PlexusBlock {
                source_line: 12,
                source: "CREATE <model>;\n".to_string(),
                validation: PlexusBlockValidation::Ignore,
            },
        ]
    );
}

#[test]
fn reports_unclosed_plexus_fence() {
    assert_eq!(
        extract_plexus_blocks("before\n```plexus\nCREATE DOMAIN one;\n"),
        Err(ExtractPlexusBlocksError::Unclosed { opening_line: 2 })
    );
}

#[test]
fn rejects_unsupported_plexus_fence_annotations() {
    assert_eq!(
        extract_plexus_blocks("```plexus,skip\nCREATE DOMAIN one;\n```\n"),
        Err(ExtractPlexusBlocksError::UnsupportedAnnotation {
            line: 1,
            fence: "```plexus,skip".to_string(),
        })
    );
}

#[test]
fn rejects_a_documented_plexus_block_with_a_parse_error() {
    let block = extract_plexus_blocks("```plexus\nCREATE DOMAIN;\n```\n")
        .expect("fences should be balanced")
        .pop()
        .expect("a Plexus block should be extracted");

    assert!(block.parse().is_err());
}

#[test]
fn all_documented_plexus_blocks_parse() {
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root should exist");
    let documentation = Documentation::new(repository_root);
    let DocumentationParseOutcome {
        parsed_block_count,
        ignored_block_count,
        failures,
    } = documentation.parse_failures();

    assert!(
        parsed_block_count > 0,
        "documentation contains no parser-checked Plexus blocks"
    );
    assert!(
        failures.is_empty(),
        "{parsed_block_count} documented Plexus blocks were checked and {ignored_block_count} \
         loose Plexus blocks were ignored:\n{}",
        failures.join("\n")
    );
    eprintln!(
        "parsed {parsed_block_count} documented Plexus blocks; skipped {ignored_block_count} \
         explicit plexus,ignore blocks"
    );
}
