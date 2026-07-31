use std::{
    fs,
    path::{Path, PathBuf},
};

use nervix_nspl::{client_statement::parse_client_statement_sources, schema::ParseFromSourceError};

#[derive(Debug, PartialEq, Eq)]
struct NsplBlock {
    source_line: usize,
    source: String,
    validation: NsplBlockValidation,
}

impl NsplBlock {
    fn parse(&self) -> Result<(), ParseFromSourceError> {
        parse_client_statement_sources(&self.source).map(|_| ())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NsplBlockValidation {
    Parse,
    Ignore,
}

#[derive(Debug, PartialEq, Eq)]
enum ExtractNsplBlocksError {
    Unclosed { opening_line: usize },
    UnsupportedAnnotation { line: usize, fence: String },
}

fn extract_nspl_blocks(markdown: &str) -> Result<Vec<NsplBlock>, ExtractNsplBlocksError> {
    let mut blocks = Vec::new();
    let mut current = None;

    for (line_index, line) in markdown.lines().enumerate() {
        let line_number = line_index + 1;
        if let Some((opening_line, validation, source)) = &mut current {
            if line.trim() == "```" {
                blocks.push(NsplBlock {
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
                "```nspl" => Some(NsplBlockValidation::Parse),
                "```nspl,ignore" => Some(NsplBlockValidation::Ignore),
                fence if fence.starts_with("```nspl") => {
                    return Err(ExtractNsplBlocksError::UnsupportedAnnotation {
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
        return Err(ExtractNsplBlocksError::Unclosed { opening_line });
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

    fn parse_failures(&self) -> (usize, usize, Vec<String>) {
        let mut parsed_block_count = 0;
        let mut ignored_block_count = 0;
        let mut failures = Vec::new();

        for path in self.markdown_paths() {
            let display_path = path.strip_prefix(&self.repository_root).unwrap_or(&path);
            let markdown = fs::read_to_string(&path).unwrap_or_else(|error| {
                panic!("failed to read {}: {error}", display_path.display())
            });
            let blocks = extract_nspl_blocks(&markdown).unwrap_or_else(|error| match error {
                ExtractNsplBlocksError::Unclosed { opening_line } => panic!(
                    "{}:{opening_line}: unclosed NSPL code block",
                    display_path.display()
                ),
                ExtractNsplBlocksError::UnsupportedAnnotation { line, fence } => panic!(
                    "{}:{line}: unsupported NSPL code block annotation {fence:?}; use `nspl` or \
                     `nspl,ignore`",
                    display_path.display()
                ),
            });

            for block in blocks {
                match block.validation {
                    NsplBlockValidation::Parse => {
                        parsed_block_count += 1;
                        if let Err(error) = block.parse() {
                            failures.extend(block.format_error(display_path, &error));
                        }
                    }
                    NsplBlockValidation::Ignore => ignored_block_count += 1,
                }
            }
        }

        (parsed_block_count, ignored_block_count, failures)
    }
}

impl NsplBlock {
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
fn extracts_runnable_and_ignored_nspl_fences_with_source_locations() {
    let markdown = r#"# Example

```text
not NSPL
```

```nspl
CREATE DOMAIN one;
```

```nspl,ignore
CREATE <model>;
```
"#;

    assert_eq!(
        extract_nspl_blocks(markdown).expect("fences should be balanced"),
        vec![
            NsplBlock {
                source_line: 8,
                source: "CREATE DOMAIN one;\n".to_string(),
                validation: NsplBlockValidation::Parse,
            },
            NsplBlock {
                source_line: 12,
                source: "CREATE <model>;\n".to_string(),
                validation: NsplBlockValidation::Ignore,
            },
        ]
    );
}

#[test]
fn reports_unclosed_nspl_fence() {
    assert_eq!(
        extract_nspl_blocks("before\n```nspl\nCREATE DOMAIN one;\n"),
        Err(ExtractNsplBlocksError::Unclosed { opening_line: 2 })
    );
}

#[test]
fn rejects_unsupported_nspl_fence_annotations() {
    assert_eq!(
        extract_nspl_blocks("```nspl,skip\nCREATE DOMAIN one;\n```\n"),
        Err(ExtractNsplBlocksError::UnsupportedAnnotation {
            line: 1,
            fence: "```nspl,skip".to_string(),
        })
    );
}

#[test]
fn rejects_a_documented_nspl_block_with_a_parse_error() {
    let block = extract_nspl_blocks("```nspl\nCREATE DOMAIN;\n```\n")
        .expect("fences should be balanced")
        .pop()
        .expect("an NSPL block should be extracted");

    assert!(block.parse().is_err());
}

#[test]
fn all_documented_nspl_blocks_parse() {
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root should exist");
    let documentation = Documentation::new(repository_root);
    let (parsed_block_count, ignored_block_count, failures) = documentation.parse_failures();

    assert!(
        parsed_block_count > 0,
        "documentation contains no parser-checked NSPL blocks"
    );
    assert!(
        failures.is_empty(),
        "{parsed_block_count} documented NSPL blocks were checked and {ignored_block_count} loose \
         NSPL blocks were ignored:\n{}",
        failures.join("\n")
    );
    eprintln!(
        "parsed {parsed_block_count} documented NSPL blocks; skipped {ignored_block_count} \
         explicit nspl,ignore blocks"
    );
}
