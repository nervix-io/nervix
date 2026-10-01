//! Repository tooling, outside the product layer order.
//! Owns: the JSON boundary of generated acquisition aggregation.
//! Depends on: report vocabulary and filesystem I/O.
//! Must not know: compiler internals, reviewed source locations or graph execution.

use std::io::{Read as _, Write as _};

use nervix_lint_report::{CompilerReport, aggregate};

fn main() -> anyhow::Result<()> {
    let mut bytes = Vec::new();
    std::io::stdin().read_to_end(&mut bytes)?;
    let reports: Vec<CompilerReport> = serde_json::from_slice(&bytes)?;
    let sites = aggregate(&reports).map_err(|report| anyhow::anyhow!("{report:?}"))?;
    std::io::stdout().write_all(&serde_json::to_vec(&sites)?)?;
    Ok(())
}
