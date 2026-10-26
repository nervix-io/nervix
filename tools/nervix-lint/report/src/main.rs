//! Repository tooling, outside the product layer order.
//! Owns: the JSON boundary of acquisition aggregation and structural source findings.
//! Depends on: report vocabulary, stable source parsing and filesystem I/O.
//! Must not know: compiler internals, reviewed source locations or graph execution.

use std::io::{Read as _, Write as _};

use nervix_lint_report::{CompilerReport, aggregate};

fn main() -> anyhow::Result<()> {
    let mut bytes = Vec::new();
    std::io::stdin().read_to_end(&mut bytes)?;
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments == ["--source"] {
        let files = serde_json::from_slice(&bytes)?;
        let sites = nervix_lint_report::source::scan(files)
            .map_err(|report| anyhow::anyhow!("{report:#}"))?;
        std::io::stdout().write_all(&serde_json::to_vec(&sites)?)?;
        return Ok(());
    }
    anyhow::ensure!(arguments.is_empty(), "expected --source or no arguments");
    let reports: Vec<CompilerReport> = serde_json::from_slice(&bytes)?;
    let sites = aggregate(&reports).map_err(|report| anyhow::anyhow!("{report:?}"))?;
    std::io::stdout().write_all(&serde_json::to_vec(&sites)?)?;
    Ok(())
}
