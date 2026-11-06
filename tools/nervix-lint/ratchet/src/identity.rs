//! Repository tooling, outside the product layer order.
//! Owns: analysis identity and the complete source/configuration/environment input set.
//! Depends on: Git discovery, file digests and compiler identity.
//! Must not know: compiler findings, configuration scheduling or product graph policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{error::Result, io, process};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub mode: String,
    pub root: PathBuf,
    pub compiler: String,
    pub driver: String,
    pub workspace_wrapper: String,
    pub inputs: BTreeMap<String, String>,
    pub cargo_configuration: BTreeMap<PathBuf, String>,
    pub environment: BTreeMap<String, String>,
}

pub fn source_inputs(root: &Path) -> Result<BTreeMap<String, String>> {
    let tracked = process::text(
        "git",
        &[
            "ls-files".into(),
            "-z".into(),
            "--cached".into(),
            "--others".into(),
            "--exclude-standard".into(),
        ],
        root,
        &BTreeMap::new(),
    )?;
    let names: BTreeSet<_> = tracked.split('\0').collect();
    let mut inputs = BTreeMap::new();
    for name in names {
        let path = root.join(name);
        let supported = path.extension().is_some_and(|extension| {
            matches!(
                extension.to_str(),
                Some("rs" | "toml" | "lock" | "fbs" | "proto" | "json")
            )
        });
        if !name.is_empty() && path.is_file() && supported {
            inputs.insert(name.to_owned(), io::digest(&path)?);
        }
    }
    Ok(inputs)
}

pub fn cargo_configuration(
    root: &Path,
    environment: &BTreeMap<String, String>,
) -> Result<BTreeMap<PathBuf, String>> {
    let cargo_home = match environment.get("CARGO_HOME") {
        Some(home) => PathBuf::from(home),
        None => match environment.get("HOME") {
            Some(home) => PathBuf::from(home).join(".cargo"),
            None => PathBuf::from(".cargo"),
        },
    };
    let mut paths = vec![cargo_home.join("config"), cargo_home.join("config.toml")];
    for ancestor in root.ancestors() {
        paths.extend([
            ancestor.join(".cargo/config"),
            ancestor.join(".cargo/config.toml"),
        ]);
    }
    let mut digests = BTreeMap::new();
    for path in paths {
        if path.is_file() {
            digests.insert(path.clone(), io::digest(&path)?);
        }
    }
    Ok(digests)
}

pub fn compiler_environment(environment: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut selected = BTreeMap::new();
    for (key, value) in environment {
        if matches!(
            key.as_str(),
            "RUSTFLAGS" | "CARGO_ENCODED_RUSTFLAGS" | "RUSTC" | "RUSTC_WRAPPER" | "KACHE_KEY_SALT"
        ) || key.starts_with("CARGO_BUILD_")
            || key.starts_with("CARGO_TARGET_")
            || key.starts_with("CARGO_PROFILE_")
        {
            selected.insert(key.clone(), value.clone());
        }
    }
    selected
}
