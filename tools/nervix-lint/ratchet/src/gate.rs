//! Repository tooling, outside the product layer order.
//! Owns: the six structural debt counts and their downward-only baseline gate.
//! Depends on: source and compiler reports, the pinned analyzer, Cargo's release graph and Git.
//! Must not know: product runtime execution or manually curated source locations.

mod graph;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context as _, Result, ensure};
use nervix_lint_report::source::{self, SourceInput};

use crate::{crap, matrix};

const BASELINE: &str = "debt-baseline.json";
const LARGE_FILE_LINES: usize = 3000;
const COMPLEXITY_LIMIT: f64 = 30.0;
const COUNTS: [(&str, &str); 6] = [
    ("files_over_3000_lines", "Rust files longer than 3000 lines"),
    ("as_casts", "Rust cast expressions"),
    (
        "hidden_tunables",
        "numeric policy outside class-typed settings or fixed contracts",
    ),
    (
        "testing_feature_struct_fields",
        "struct fields gated on `feature = \"testing\"`",
    ),
    (
        "functions_over_complexity_30",
        "functions whose cyclomatic complexity exceeds 30",
    ),
    (
        "non_aws_lc_crypto",
        "authored cryptography and product dependencies outside AWS-LC",
    ),
];

#[derive(Clone, Debug, clap::Args)]
pub struct Options {
    #[arg(long)]
    pub update: bool,
    #[arg(long)]
    pub show: Option<String>,
    #[arg(long)]
    pub root: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Site {
    pub path: String,
    pub line: usize,
    pub detail: String,
}

impl Site {
    fn render(&self) -> String {
        format!("{}:{}: {}", self.path, self.line, self.detail)
    }
}

#[derive(Clone, Debug)]
pub struct RustFile {
    path: String,
    source: String,
    lines: usize,
}

pub fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("ratchet crate is under tools/nervix-lint")
        .to_owned()
}

fn discovered_paths(root: &Path) -> Result<Vec<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            "*.rs",
        ])
        .output()
        .context("cannot discover authored Rust sources")?;
    ensure!(
        output.status.success(),
        "cannot discover authored Rust sources: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let paths: BTreeSet<String> = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(|entry| String::from_utf8_lossy(entry).into_owned())
        .filter(|path: &String| root.join(path).is_file())
        .filter(|path: &String| {
            !Path::new(path).components().any(|component| {
                ["tests", "benches", "examples"]
                    .iter()
                    .any(|excluded| component.as_os_str() == *excluded)
            })
        })
        .collect();
    Ok(paths.into_iter().collect())
}

pub fn load_files(root: &Path) -> Result<Vec<RustFile>> {
    discovered_paths(root)?
        .into_iter()
        .map(|path| {
            let source = fs::read_to_string(root.join(&path))
                .with_context(|| format!("cannot read {path}"))?;
            let lines = source.bytes().filter(|byte| *byte == b'\n').count()
                + usize::from(!source.is_empty() && !source.ends_with('\n'));
            Ok(RustFile {
                path,
                source,
                lines,
            })
        })
        .collect()
}

pub fn source_sites(root: &Path, target: &Path) -> Result<BTreeMap<String, Vec<Site>>> {
    let files = load_files(root)?;
    measure_sources(root, target, &files, &graph::product_sites(root)?)
}

pub fn measure_sources(
    root: &Path,
    target: &Path,
    files: &[RustFile],
    graph_sites: &[Site],
) -> Result<BTreeMap<String, Vec<Site>>> {
    let mut sites: BTreeMap<String, Vec<Site>> = COUNTS
        .iter()
        .map(|(name, _)| ((*name).into(), Vec::new()))
        .collect();
    for file in files {
        if file.lines > LARGE_FILE_LINES {
            sites
                .get_mut("files_over_3000_lines")
                .expect("declared count")
                .push(Site {
                    path: file.path.clone(),
                    line: 1,
                    detail: format!("{} lines", file.lines),
                });
        }
    }
    let product: Vec<SourceInput> = files
        .iter()
        .filter(|file| {
            !["/tests.rs", "_tests.rs", "/test_fixtures.rs"]
                .iter()
                .any(|suffix| file.path.ends_with(suffix))
        })
        .map(|file| SourceInput {
            path: file.path.clone(),
            source: file.source.clone(),
        })
        .collect();
    let by_path: BTreeMap<_, _> = files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect();
    for site in source::scan(product)
        .map_err(|error| anyhow::anyhow!("Rust source analysis failed: {error:?}"))?
    {
        let detail = if site.rule == "non_aws_lc_crypto" {
            let source = by_path
                .get(site.path.as_str())
                .context("source site has no discovered file")?;
            let line = source
                .source
                .lines()
                .nth(site.line.saturating_sub(1))
                .context("source site has no line")?;
            format!(
                "non_aws_lc_crypto (authored): {}",
                line.split_whitespace().collect::<Vec<_>>().join(" ")
            )
        } else {
            site.rule.clone()
        };
        sites
            .get_mut(&site.rule)
            .with_context(|| format!("unrecognized source rule {}", site.rule))?
            .push(Site {
                path: site.path,
                line: site.line,
                detail,
            });
    }
    let paths: Vec<_> = files.iter().map(|file| file.path.clone()).collect();
    let unreachable = crap::unreachable_sources(root, &paths)?;
    ensure!(
        unreachable.is_empty(),
        "the complexity analyzer cannot read these discovered Rust files, so the count would miss \
         their functions:\n{}",
        unreachable.join("\n")
    );
    let excludes = ["benches", "examples", "tests"].map(|directory| format!("**/{directory}/**"));
    let functions = crap::analyze(root, target, &excludes).context("complexity analysis failed")?;
    let discovered: BTreeSet<_> = paths.iter().collect();
    let outside: BTreeSet<_> = functions
        .iter()
        .filter(|function| !discovered.contains(&function.path))
        .map(|function| function.path.as_str())
        .collect();
    ensure!(
        outside.is_empty(),
        "the complexity analysis measured Rust files that source discovery does not govern:\n{}",
        outside.into_iter().collect::<Vec<_>>().join("\n")
    );
    let complexity = sites
        .get_mut("functions_over_complexity_30")
        .expect("declared count");
    for function in functions {
        if function.complexity > COMPLEXITY_LIMIT {
            complexity.push(Site {
                path: function.path,
                line: function.line,
                detail: format!(
                    "{} has cyclomatic complexity {}",
                    function.name, function.complexity
                ),
            });
        }
    }
    complexity.sort_by(|left, right| (&left.path, left.line).cmp(&(&right.path, right.line)));
    sites
        .get_mut("non_aws_lc_crypto")
        .expect("declared count")
        .extend(graph_sites.iter().cloned());
    Ok(sites)
}

fn read_baseline(path: &Path) -> Result<BTreeMap<String, usize>> {
    ensure!(
        path.exists(),
        "no baseline at {}. Run `just ratchet --update` to write one.",
        path.display()
    );
    let baseline: BTreeMap<String, usize> = serde_json::from_slice(&fs::read(path)?)?;
    let expected: BTreeSet<_> = COUNTS.iter().map(|(name, _)| *name).collect();
    let actual: BTreeSet<_> = baseline.keys().map(String::as_str).collect();
    let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
    let unknown = actual.difference(&expected).copied().collect::<Vec<_>>();
    ensure!(
        missing.is_empty() && unknown.is_empty(),
        "{} does not match the counts this script measures{}{}. Run `just ratchet --update` to \
         rewrite it.",
        path.file_name().unwrap_or_default().to_string_lossy(),
        if missing.is_empty() {
            String::new()
        } else {
            format!("; missing {}", missing.join(", "))
        },
        if unknown.is_empty() {
            String::new()
        } else {
            format!("; unknown {}", unknown.join(", "))
        }
    );
    Ok(baseline)
}

fn report(
    sites: &BTreeMap<String, Vec<Site>>,
    baseline: &BTreeMap<String, usize>,
    update: bool,
    path: &Path,
) -> Result<i32> {
    let width = COUNTS.iter().map(|(name, _)| name.len()).max().unwrap_or(0);
    println!("Debt ratchet — every count may fall and none may rise.\n");
    for (name, description) in COUNTS {
        let current = sites[name].len();
        let before = baseline[name];
        let change = isize::try_from(current)? - isize::try_from(before)?;
        let marker = if change > 0 {
            "^^"
        } else if change < 0 {
            "vv"
        } else {
            "  "
        };
        println!(
            "  {marker} {name:<width$}  {current:>6}  baseline {before:>6}  {change:+6}   \
             {description}"
        );
    }
    println!();
    let risen: Vec<_> = COUNTS
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| sites[*name].len() > baseline[*name])
        .collect();
    let fallen: Vec<_> = COUNTS
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| sites[*name].len() < baseline[*name])
        .collect();
    if !fallen.is_empty() {
        println!(
            "These counts fell. Run `just ratchet --update` and commit the baseline with them:"
        );
        for name in &fallen {
            println!("  {name}: {} -> {}", baseline[*name], sites[*name].len());
        }
        println!();
    }
    if !risen.is_empty() {
        println!("error: debt rose. These counts are above their baseline:");
        for name in &risen {
            println!(
                "  {name}: {} -> {} (+{}); `just ratchet --show {name}` lists the sites",
                baseline[*name],
                sites[*name].len(),
                sites[*name].len() - baseline[*name]
            );
        }
        for name in risen {
            println!("\n{name} sites:");
            for site in &sites[name] {
                println!("  {}", site.render());
            }
        }
        return Ok(1);
    }
    if update {
        let counts: BTreeMap<_, _> = sites
            .iter()
            .map(|(name, values)| (name, values.len()))
            .collect();
        fs::write(
            path,
            format!("{}\n", serde_json::to_string_pretty(&counts)?),
        )?;
        println!(
            "Wrote {} from the current counts.",
            path.file_name().unwrap_or_default().to_string_lossy()
        );
    }
    Ok(0)
}

impl Options {
    pub fn run(self) -> Result<i32> {
        let root = self.root.unwrap_or_else(repository_root);
        let target = crap::target(&root, None);
        execute(
            &root,
            self.update,
            self.show,
            || {
                let problems = source::provenance::check(&root)?;
                ensure!(
                    problems.is_empty(),
                    "language import provenance failed\n{}",
                    problems.join("\n")
                );
                Ok(())
            },
            || {
                let compiler = matrix::Analyze {
                    root: Some(root.clone()),
                    target_dir: Some(target.clone()),
                    configuration: Vec::new(),
                    turmoil_child: false,
                    fixture_mode: None,
                    fresh: false,
                    recompile: false,
                    inventory: false,
                    output: Some(target.join("typed-ratchet/gate.json")),
                    show: false,
                    jobs: None,
                };
                compiler.run().map_err(|error| {
                    anyhow::anyhow!("compiler synchronization diagnostics failed: {error:?}")
                })?;
                Ok(())
            },
            || source_sites(&root, &target),
        )
    }
}

fn execute(
    root: &Path,
    update: bool,
    show: Option<String>,
    provenance: impl FnOnce() -> Result<()>,
    compiler: impl FnOnce() -> Result<()>,
    measure: impl FnOnce() -> Result<BTreeMap<String, Vec<Site>>>,
) -> Result<i32> {
    provenance()?;
    compiler()?;
    let sites = measure()?;
    if let Some(name) = show {
        if let Some(found) = sites.get(&name) {
            for site in found {
                println!("{}", site.render());
            }
            return Ok(0);
        }
        eprintln!(
            "error: unknown count '{}'. Known counts: {}",
            name.replace('\\', "\\\\").replace('\'', "\\'"),
            COUNTS
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>()
                .join(", ")
        );
        return Ok(2);
    }
    let path = root.join(BASELINE);
    report(&sites, &read_baseline(&path)?, update, &path)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    fn counts(count: usize) -> BTreeMap<String, Vec<Site>> {
        COUNTS
            .iter()
            .map(|(name, _)| {
                (
                    (*name).into(),
                    (0..count)
                        .map(|index| Site {
                            path: "src/lib.rs".into(),
                            line: index + 1,
                            detail: (*name).into(),
                        })
                        .collect(),
                )
            })
            .collect()
    }

    fn baseline(count: usize) -> BTreeMap<String, usize> {
        COUNTS
            .iter()
            .map(|(name, _)| ((*name).into(), count))
            .collect()
    }

    #[test]
    fn baseline_requires_exactly_the_measured_count_names() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(BASELINE);
        fs::write(&path, r#"{"as_casts":0}"#).unwrap();
        assert!(
            read_baseline(&path)
                .unwrap_err()
                .to_string()
                .contains("missing")
        );
        let mut complete = baseline(0);
        complete.insert("other".into(), 1);
        fs::write(&path, serde_json::to_string(&complete).unwrap()).unwrap();
        assert!(
            read_baseline(&path)
                .unwrap_err()
                .to_string()
                .contains("unknown")
        );
    }

    #[test]
    fn increase_refuses_update_and_a_decrease_updates_only_downward() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(BASELINE);
        let original = serde_json::to_string(&baseline(1)).unwrap();
        fs::write(&path, &original).unwrap();
        assert_eq!(
            report(&counts(2), &read_baseline(&path).unwrap(), true, &path).unwrap(),
            1
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), original);
        assert_eq!(
            report(&counts(1), &read_baseline(&path).unwrap(), false, &path).unwrap(),
            0
        );
        assert_eq!(
            report(&counts(0), &read_baseline(&path).unwrap(), true, &path).unwrap(),
            0
        );
        assert_eq!(read_baseline(&path).unwrap(), baseline(0));
    }

    #[test]
    fn site_listing_names_the_source_line_and_detail() {
        assert_eq!(counts(1)["as_casts"][0].render(), "src/lib.rs:1: as_casts");
        assert_eq!(
            Site {
                path: "Cargo.lock".into(),
                line: 42,
                detail: "non_aws_lc_crypto (graph): owner 1 -> sha2 1".into()
            }
            .render(),
            "Cargo.lock:42: non_aws_lc_crypto (graph): owner 1 -> sha2 1"
        );
    }

    #[test]
    fn failed_compiler_analysis_cannot_rewrite_the_baseline() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(BASELINE);
        let original = serde_json::to_string(&baseline(0)).unwrap();
        fs::write(&path, &original).unwrap();
        let result = execute(
            directory.path(),
            true,
            None,
            || Ok(()),
            || anyhow::bail!("compiler diagnostics failed"),
            || Ok(counts(0)),
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("compiler diagnostics failed")
        );
        assert_eq!(fs::read_to_string(path).unwrap(), original);
    }

    #[test]
    fn in_crate_test_file_counts_its_final_line_but_test_tree_is_excluded() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(root)
            .status()
            .unwrap();
        for path in ["src/tests.rs", "tests/suite.rs"] {
            fs::create_dir_all(root.join(path).parent().unwrap()).unwrap();
            fs::write(root.join(path), "x\n".repeat(3000) + "x").unwrap();
        }
        let files = load_files(root).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].lines, 3001);
        assert_eq!(files[0].path, "src/tests.rs");
    }

    #[test]
    fn structural_scanner_and_complexity_threshold_share_discovered_files() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(root)
            .status()
            .unwrap();
        fs::create_dir(root.join("src")).unwrap();
        fs::write(
            root.join("src/lib.rs"),
            "fn lower(x: u64) -> u32 { x as u32 }\n",
        )
        .unwrap();
        fs::write(
            root.join("src/tests.rs"),
            "fn test(x: u64) -> u32 { x as u32 }\n",
        )
        .unwrap();
        let target = root.join("target");
        let executable = crap::executable(&target);
        fs::create_dir_all(executable.parent().unwrap()).unwrap();
        let script = r#"#!/bin/sh
if [ "$1" = --version ]; then echo 'cargo-crap 0.6.1'; exit 0; fi
printf '%s' '{"version":"0.6.1","entries":[{"file":"./src/lib.rs","function":"at_limit","line":1,"cyclomatic":30.0},{"file":"./src/lib.rs","function":"over_limit","line":1,"cyclomatic":31.0}]}'
"#;
        fs::write(&executable, script).unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let files = load_files(root).unwrap();
        let sites = measure_sources(root, &target, &files, &[]).unwrap();
        assert_eq!(
            sites["as_casts"].len(),
            1,
            "in-crate test syntax is excluded"
        );
        assert_eq!(sites["functions_over_complexity_30"].len(), 1);
        assert!(
            sites["functions_over_complexity_30"][0]
                .render()
                .contains("over_limit has cyclomatic complexity 31")
        );
    }

    #[test]
    fn bolero_baseline_file_round_trip_preserves_every_declared_count() {
        bolero::check!().with_type::<[u8; 32]>().for_each(|seed| {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join(BASELINE);
            let values: BTreeMap<String, usize> = COUNTS
                .iter()
                .enumerate()
                .map(|(index, (name, _))| {
                    (
                        (*name).into(),
                        usize::from(seed[index]) * 256 + usize::from(seed[index + 6]),
                    )
                })
                .collect();
            fs::write(
                &path,
                format!("{}\n", serde_json::to_string_pretty(&values).unwrap()),
            )
            .unwrap();
            assert_eq!(read_baseline(&path).unwrap(), values);
        });
    }
}
