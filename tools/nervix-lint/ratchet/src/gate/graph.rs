//! Repository tooling, outside the product layer order.
//! Owns: the two release-target product dependency trees and cryptography graph sites.
//! Depends on: Cargo's depth-prefixed tree and the repository lockfile.
//! Must not know: authored source syntax or synchronization analysis.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    process::Command,
};

use anyhow::{Context as _, Result, bail, ensure};

use super::Site;

const PACKAGE: &str = "nervix-server";
const TARGETS: [&str; 2] = ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"];
const CRYPTO: &[&str] = &[
    "blake2",
    "blake3",
    "md-5",
    "md4",
    "md5",
    "ripemd",
    "sha1",
    "sha1_smol",
    "sha2",
    "sha3",
    "sm3",
    "argon2",
    "bcrypt",
    "cmac",
    "hkdf",
    "hmac",
    "pbkdf2",
    "scrypt",
    "aes",
    "aes-gcm",
    "aes-gcm-siv",
    "aes-siv",
    "blowfish",
    "cbc",
    "ccm",
    "cfb-mode",
    "chacha20",
    "chacha20poly1305",
    "ctr",
    "des",
    "eax",
    "ghash",
    "ocb3",
    "ofb",
    "poly1305",
    "polyval",
    "rc2",
    "rc4",
    "salsa20",
    "xsalsa20poly1305",
    "curve25519-dalek",
    "dsa",
    "ecdsa",
    "ed25519-dalek",
    "k256",
    "p256",
    "p384",
    "p521",
    "rsa",
    "x25519-dalek",
    "rand_chacha",
    "boring",
    "boring-sys",
    "libsodium-sys",
    "mbedtls",
    "openssl",
    "openssl-sys",
    "ring",
    "wolfssl",
];
const MODULES: [&str; 2] = ["aws-lc-sys", "aws-lc-fips-sys"];

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Package {
    name: String,
    version: String,
}

impl Package {
    fn render(&self) -> String {
        format!("{} {}", self.name, self.version)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Edge {
    dependent: Package,
    dependency: Package,
}

pub fn read_tree(root: &Path) -> Result<String> {
    let output = tree_command(root)
        .output()
        .context("cannot read the product dependency graph")?;
    ensure!(
        output.status.success(),
        "cannot read the product dependency graph\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).context("product dependency graph is not UTF-8")
}

fn tree_command(root: &Path) -> Command {
    let mut command = Command::new("cargo");
    command
        .current_dir(root)
        .args(["tree", "--locked", "--manifest-path"])
        .arg(root.join("Cargo.toml"))
        .args([
            "--package",
            PACKAGE,
            "--edges",
            "normal,build",
            "--prefix",
            "depth",
            "--format",
            "{p}",
        ]);
    for target in TARGETS {
        command.args(["--target", target]);
    }
    command
}

fn edges(tree: &str) -> Result<BTreeSet<Edge>> {
    let mut found = BTreeSet::new();
    let mut ancestors: Vec<Package> = Vec::new();
    let mut trees = 0;
    for line in tree.lines().filter(|line| !line.trim().is_empty()) {
        let (depth, package) = parse_line(line)?;
        if depth == 0 {
            ensure!(
                package.name == PACKAGE,
                "a product dependency tree starts at {}, not {PACKAGE}",
                package.render()
            );
            trees += 1;
        } else {
            ensure!(
                depth <= ancestors.len(),
                "product dependency graph line skips a level: {line:?}"
            );
        }
        ancestors.truncate(depth);
        if depth > 0 {
            found.insert(Edge {
                dependent: ancestors[depth - 1].clone(),
                dependency: package.clone(),
            });
        }
        ancestors.push(package);
    }
    ensure!(
        trees == TARGETS.len(),
        "the product dependency graph has {trees} trees, one for each of the {} release targets \
         was expected",
        TARGETS.len()
    );
    Ok(found)
}

fn parse_line(line: &str) -> Result<(usize, Package)> {
    let end = line
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(line.len());
    ensure!(
        end > 0 && end < line.len(),
        "unrecognized product dependency graph line: {line:?}"
    );
    let depth = line[..end].parse::<usize>()?;
    let rest = &line[end..];
    ensure!(
        !rest.starts_with(char::is_whitespace),
        "unrecognized product dependency graph line: {line:?}"
    );
    let words: Vec<_> = rest.split_whitespace().collect();
    ensure!(
        words.len() >= 2 && words[1].starts_with('v') && words[1].len() > 1,
        "unrecognized product dependency graph line: {line:?}"
    );
    Ok((
        depth,
        Package {
            name: words[0].into(),
            version: words[1][1..].into(),
        },
    ))
}

fn lock_entries(lockfile: &str) -> BTreeMap<Package, usize> {
    let mut entries = BTreeMap::new();
    let mut start = None;
    let mut name: Option<String> = None;
    for (offset, line) in lockfile.lines().enumerate() {
        if line == "[[package]]" {
            start = Some(offset + 1);
            name = None;
            continue;
        }
        if start.is_none() {
            continue;
        }
        if let Some(value) = line
            .strip_prefix("name = \"")
            .and_then(|line| line.strip_suffix('"'))
        {
            name = Some(value.into());
        }
        if let Some(version) = line
            .strip_prefix("version = \"")
            .and_then(|line| line.strip_suffix('"'))
            && let (Some(name), Some(start)) = (name.as_ref(), start)
        {
            entries
                .entry(Package {
                    name: name.clone(),
                    version: version.into(),
                })
                .or_insert(start);
        }
    }
    entries
}

fn check_module(edges: &BTreeSet<Edge>) -> Result<()> {
    let mut modules: BTreeMap<Package, BTreeSet<Package>> = BTreeMap::new();
    for edge in edges {
        if MODULES.contains(&edge.dependency.name.as_str()) {
            modules
                .entry(edge.dependency.clone())
                .or_default()
                .insert(edge.dependent.clone());
        }
    }
    if modules.len() == 1 {
        return Ok(());
    }
    if modules.is_empty() {
        bail!(
            "the product dependency graph links no AWS-LC module, but the product's cryptography \
             comes from AWS-LC; docs/src/cryptography.md owns the rule"
        );
    }
    let details = modules
        .iter()
        .map(|(module, dependents)| {
            format!(
                "  {}, linked by {}",
                module.render(),
                dependents
                    .iter()
                    .map(Package::render)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    bail!(
        "the product links {} AWS-LC modules and must link one, so that the FIPS migration brings \
         one module; docs/src/cryptography.md owns the rule:\n{details}",
        modules.len()
    );
}

pub fn sites(tree: &str, lockfile: &str) -> Result<Vec<Site>> {
    let edges = edges(tree)?;
    check_module(&edges)?;
    let entries = lock_entries(lockfile);
    let mut sites = Vec::new();
    let mut ordered: Vec<_> = edges.into_iter().collect();
    ordered.sort_by(|left, right| {
        (&left.dependency, &left.dependent).cmp(&(&right.dependency, &right.dependent))
    });
    for edge in ordered {
        if !CRYPTO.contains(&edge.dependency.name.as_str()) {
            continue;
        }
        let line = entries.get(&edge.dependent).with_context(|| {
            format!(
                "Cargo.lock has no entry for {}, which the product dependency graph names",
                edge.dependent.render()
            )
        })?;
        sites.push(Site {
            path: "Cargo.lock".into(),
            line: *line,
            detail: format!(
                "non_aws_lc_crypto (graph): {} -> {}",
                edge.dependent.render(),
                edge.dependency.render()
            ),
        });
    }
    Ok(sites)
}

pub fn product_sites(root: &Path) -> Result<Vec<Site>> {
    sites(
        &read_tree(root)?,
        &fs::read_to_string(root.join("Cargo.lock"))?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn two_trees(lines: &str) -> String {
        let one = format!("0nervix-server v1\n1aws-lc-rs v1\n2aws-lc-sys v1\n{lines}");
        format!("{one}\n{one}")
    }

    fn lockfile(names: &[&str]) -> String {
        names
            .iter()
            .map(|name| format!("[[package]]\nname = \"{name}\"\nversion = \"1\"\n"))
            .collect()
    }

    #[test]
    fn duplicate_release_edges_count_once_and_module_is_required() {
        let tree = "0nervix-server v1\n1aws-lc-sys v1\n1dep v1\n2sha2 v1\n0nervix-server \
                    v1\n1aws-lc-sys v1 (*)\n1dep v1 (*)\n";
        let lock = "[[package]]\nname = \"nervix-server\"\nversion = \"1\"\n[[package]]\nname = \
                    \"dep\"\nversion = \"1\"\n";
        let found = sites(tree, lock).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].line, 4);
    }

    #[test]
    fn each_crypto_edge_points_at_its_dependents_lock_entry_and_extra_dependents_add_sites() {
        let lock = lockfile(&["nervix-server", "argon2", "nervix-models", "parquet"]);
        let base = two_trees("1argon2 v1\n2blake2 v1\n1nervix-models v1\n2sha2 v1\n");
        let before = sites(&base, &lock).unwrap();
        assert_eq!(before.len(), 3);
        assert_eq!(
            before[0].render(),
            "Cargo.lock:1: non_aws_lc_crypto (graph): nervix-server 1 -> argon2 1"
        );
        assert_eq!(
            before[1].render(),
            "Cargo.lock:4: non_aws_lc_crypto (graph): argon2 1 -> blake2 1"
        );
        assert_eq!(
            before[2].render(),
            "Cargo.lock:7: non_aws_lc_crypto (graph): nervix-models 1 -> sha2 1"
        );
        let extra = two_trees(
            "1argon2 v1\n2blake2 v1\n1nervix-models v1\n2sha2 v1\n2argon2 v1 (*)\n1parquet \
             v1\n2ring v1\n",
        );
        let after = sites(&extra, &lock).unwrap();
        assert_eq!(after.len(), before.len() + 2);
        assert!(
            after
                .iter()
                .any(|site| site.detail.contains("nervix-models 1 -> argon2 1"))
        );
        assert!(
            after
                .iter()
                .any(|site| site.detail.contains("parquet 1 -> ring 1"))
        );
    }

    #[test]
    fn wrong_release_roots_depths_and_missing_lock_entries_are_refused() {
        let base = two_trees("1sha2 v1\n");
        for (tree, phrase) in [
            (
                base.replace("1sha2 v1", "1sha2"),
                "unrecognized product dependency graph line",
            ),
            (base.replace("1sha2 v1", "9sha2 v1"), "skips a level"),
            (
                base.replace("0nervix-server", "0nervix-models"),
                "starts at nervix-models",
            ),
            ("0nervix-server v1\n1aws-lc-sys v1\n".into(), "has 1 trees"),
            (String::new(), "has 0 trees"),
        ] {
            assert!(
                sites(&tree, "").unwrap_err().to_string().contains(phrase),
                "{phrase}"
            );
        }
        let missing = two_trees("1parquet v1\n2ring v1\n");
        assert!(
            sites(&missing, "")
                .unwrap_err()
                .to_string()
                .contains("no entry for parquet 1")
        );
    }

    #[test]
    fn release_graph_command_keeps_package_edges_and_both_targets() {
        let command = tree_command(Path::new("/workspace"));
        let args: Vec<_> = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        for expected in [
            "tree",
            "--locked",
            "--package",
            "nervix-server",
            "--edges",
            "normal,build",
            "--prefix",
            "depth",
            "x86_64-unknown-linux-gnu",
            "aarch64-unknown-linux-gnu",
        ] {
            assert!(args.iter().any(|arg| arg == expected), "missing {expected}");
        }
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--manifest-path", "/workspace/Cargo.toml"])
        );
        assert_eq!(
            args.iter().filter(|arg| arg.as_str() == "--target").count(),
            2
        );
    }

    #[test]
    fn distinct_versions_fips_and_per_target_modules_name_their_dependents() {
        let two_versions = two_trees("1rdkafka-sys v1\n2aws-lc-sys v2\n");
        let error = sites(&two_versions, "").unwrap_err().to_string();
        assert!(error.contains("2 AWS-LC modules"));
        assert!(error.contains("aws-lc-sys 1, linked by aws-lc-rs 1"));
        assert!(error.contains("aws-lc-sys 2, linked by rdkafka-sys 1"));
        let fips = two_trees("1rdkafka-sys v1\n2aws-lc-fips-sys v1\n");
        assert!(
            sites(&fips, "")
                .unwrap_err()
                .to_string()
                .contains("aws-lc-fips-sys 1, linked by rdkafka-sys 1")
        );
        let targets = "0nervix-server v1\n1aws-lc-sys v1\n0nervix-server v1\n1aws-lc-sys v2\n";
        assert!(
            sites(targets, "")
                .unwrap_err()
                .to_string()
                .contains("2 AWS-LC modules")
        );
    }

    #[test]
    fn second_module_and_missing_module_refuse() {
        let two = "0nervix-server v1\n1aws-lc-sys v1\n1aws-lc-fips-sys v1\n0nervix-server \
                   v1\n1aws-lc-sys v1\n";
        assert!(
            sites(two, "")
                .unwrap_err()
                .to_string()
                .contains("2 AWS-LC modules")
        );
        assert!(
            sites("0nervix-server v1\n0nervix-server v1\n", "")
                .unwrap_err()
                .to_string()
                .contains("no AWS-LC module")
        );
    }

    #[test]
    fn bolero_generated_depth_trees_recover_every_edge_and_refuse_skipped_depths() {
        bolero::check!().with_type::<[u8; 32]>().for_each(|seed| {
            let mut tree = String::new();
            let mut expected = BTreeSet::new();
            for _ in 0..2 {
                tree.push_str("0nervix-server v1\n1aws-lc-sys v1\n");
                expected.insert(Edge {
                    dependent: Package {
                        name: "nervix-server".into(),
                        version: "1".into(),
                    },
                    dependency: Package {
                        name: "aws-lc-sys".into(),
                        version: "1".into(),
                    },
                });
                let mut ancestors = vec![
                    Package {
                        name: "nervix-server".into(),
                        version: "1".into(),
                    },
                    Package {
                        name: "aws-lc-sys".into(),
                        version: "1".into(),
                    },
                ];
                for (index, byte) in seed.iter().take(12).enumerate() {
                    let depth = (1 + usize::from(*byte % 3)).min(ancestors.len());
                    let package = Package {
                        name: format!("p{index}"),
                        version: "1".into(),
                    };
                    tree.push_str(&format!("{depth}{} v1\n", package.name));
                    expected.insert(Edge {
                        dependent: ancestors[depth - 1].clone(),
                        dependency: package.clone(),
                    });
                    ancestors.truncate(depth);
                    ancestors.push(package);
                }
            }
            assert_eq!(edges(&tree).unwrap(), expected);
            tree.push_str("9skipped v1\n");
            assert!(edges(&tree).is_err());
        });
    }
}
