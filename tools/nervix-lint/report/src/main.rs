//! Repository tooling, outside the product layer order.
//! Owns: the JSON boundary of pure acquisition aggregation and policy validation.
//! Depends on: the report vocabulary and filesystem I/O.
//! Must not know: compiler internals or graph execution.

use std::io::{Read as _, Write as _};

use nervix_lint_report::{PolicyInput, aggregate};

fn main() -> anyhow::Result<()> {
    let mut bytes = Vec::new();
    std::io::stdin().read_to_end(&mut bytes)?;
    let input: PolicyInput = serde_json::from_slice(&bytes)?;
    let output = if std::env::args().any(|argument| argument == "--inventory") {
        let sites = aggregate(&input.reports).map_err(|report| anyhow::anyhow!("{report:?}"))?;
        serde_json::to_vec(&sites)?
    } else {
        let sites = input
            .classify()
            .map_err(|report| anyhow::anyhow!("{report:?}"))?;
        serde_json::to_vec(&sites)?
    };
    std::io::stdout().write_all(&output)?;
    Ok(())
}
