//! Local diagnostic evidence inspection and review.
//!
//! Layer: edges.
//! - **Owns.** CLI selection, export, explicit review and qualification exit status.
//! - **Depends on.** The diagnostic evidence owner and Clap.
//! - **Must not know.** Application state, detector internals or a running node.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use error_stack::Report;
use nervix_deadlock::{
    DeadlockEvidence, EvidenceError, FindingSelection, ProofBasis, TriageProof, render_finding,
};

nervix_primitives::product_binary!("nervix-deadlock-report");

#[derive(Parser)]
#[command(about = "Inspect and triage bounded diagnostic evidence locally")]
struct Arguments {
    #[command(subcommand)]
    command: Action,
}

#[derive(Subcommand)]
enum Action {
    Inspect {
        evidence: PathBuf,
        #[arg(long, value_enum, default_value = "all")]
        source: FindingSelection,
        #[arg(long)]
        export: Option<PathBuf>,
    },
    Triage {
        evidence: PathBuf,
        #[arg(long)]
        finding: usize,
        #[arg(long, value_enum)]
        basis: ProofBasis,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        regression: String,
        #[arg(long)]
        output: PathBuf,
    },
    Qualify {
        evidence: PathBuf,
    },
}

impl Action {
    fn execute(self) -> Result<i32, Report<EvidenceError>> {
        match self {
            Self::Inspect {
                evidence,
                source,
                export,
            } => {
                let original = DeadlockEvidence::read_file(&evidence)?;
                println!(
                    "process {}, selection {:?}, scope {:?}; evidence qualifies: {}",
                    original.process().id,
                    original.process().selection,
                    original.scope(),
                    original.qualifies()
                );
                for (index, finding) in original.findings().iter().enumerate() {
                    if source.includes(finding) {
                        print!("finding {index}: {}", render_finding(finding));
                    }
                }
                if let Some(export) = export {
                    original.selected(source).write_file(&export)?;
                }
                Ok(0)
            }
            Self::Triage {
                evidence,
                finding,
                basis,
                reason,
                regression,
                output,
            } => {
                let mut evidence = DeadlockEvidence::read_file(&evidence)?;
                let proof = TriageProof::new(basis, &reason, &regression)?;
                evidence.triage(finding, proof)?;
                evidence.write_file(&output)?;
                println!(
                    "reviewed finding {finding}; complete evidence retained at {}",
                    output.display()
                );
                Ok(0)
            }
            Self::Qualify { evidence } => {
                let evidence = DeadlockEvidence::read_file(&evidence)?;
                if evidence.qualifies() {
                    println!("diagnostic evidence qualifies");
                    Ok(0)
                } else {
                    println!(
                        "diagnostic evidence cannot qualify: selected scope or active, lost, \
                         incomplete or unreviewed findings"
                    );
                    Ok(5)
                }
            }
        }
    }
}

fn main() {
    let status = match Arguments::parse().command.execute() {
        Ok(status) => status,
        Err(error) => {
            eprintln!("diagnostic evidence operation failed: {error:?}");
            4
        }
    };
    std::process::exit(status);
}
