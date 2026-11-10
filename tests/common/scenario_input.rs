//! Feature selection for the public scenario runner.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** File, directory and glob inputs rooted at their literal directory prefix.
//! - **Depends on.** Filesystem paths, globwalk matching, Gherkin parsing and Cucumber's example
//!   expansion.
//! - **Must not know.** Scenario state, scheduling, retries, deadlines or product behavior.

use std::{
    io,
    path::{Path, PathBuf},
};

use cucumber::feature::Ext as _;

#[derive(Clone, Debug, Default, clap::Args)]
pub(crate) struct ScenarioInput {
    /// Feature file, directory or glob, relative to the workspace unless absolute.
    #[arg(
        id = "input",
        long = "input",
        short = 'i',
        value_name = "PATH_OR_GLOB",
        global = true
    )]
    features: Option<String>,
}

/// Select before parsing, so a focused input never walks the checkout's generated trees.
pub(crate) fn parse_features(
    workspace: &Path,
    default_input: &Path,
    cli: ScenarioInput,
) -> Vec<cucumber::parser::Result<cucumber::gherkin::Feature>> {
    let input = cli
        .features
        .as_deref()
        .map(Path::new)
        .unwrap_or(default_input);
    let input = workspace.join(input);
    match feature_paths(&input) {
        Ok(paths) => paths
            .into_iter()
            .map(|path| {
                let feature = cucumber::gherkin::Feature::parse_path(path, Default::default())
                    .map_err(cucumber::parser::Error::from)?;
                feature
                    .expand_examples()
                    .map_err(cucumber::parser::Error::from)
            })
            .collect(),
        Err(source) => vec![Err(cucumber::gherkin::ParseFileError::Reading {
            path: input,
            source,
        }
        .into())],
    }
}

fn feature_paths(input: &Path) -> io::Result<Vec<PathBuf>> {
    if input.is_file() {
        return Ok(vec![input.canonicalize()?]);
    }
    let (root, pattern) = if input.is_dir() {
        (input.to_path_buf(), "*.feature".to_owned())
    } else {
        let mut root = PathBuf::new();
        let mut pattern = PathBuf::new();
        let mut patterned = false;
        for component in input.components() {
            patterned |= component
                .as_os_str()
                .to_string_lossy()
                .contains(['*', '?', '{', '[']);
            if patterned {
                pattern.push(component);
            } else {
                root.push(component);
            }
        }
        if !patterned {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "feature input does not exist",
            ));
        }
        let pattern = pattern
            .to_str()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "feature pattern must be valid UTF-8",
                )
            })?
            .to_owned();
        (root, pattern)
    };
    // A nonexistent literal prefix is an input error, not a reason to walk its ancestors.
    if !root.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "feature input directory does not exist",
        ));
    }
    let pattern = if cfg!(windows) {
        pattern.replace('\\', "/")
    } else {
        pattern
    };
    let walker = globwalk::GlobWalkerBuilder::from_patterns(root.canonicalize()?, &[pattern])
        .build()
        .map_err(io::Error::other)?;
    let mut paths = walker
        .map(|entry| {
            entry
                .map(|entry| entry.into_path())
                .map_err(io::Error::other)
        })
        .collect::<io::Result<Vec<_>>>()?;
    paths.sort();
    Ok(paths)
}
