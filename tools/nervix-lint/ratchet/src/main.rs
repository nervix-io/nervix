//! Repository tooling, outside the product layer order.
//! Owns: the analysis-runner and Cargo-workspace-wrapper command boundary.
//! Depends on: the ratchet library and command-line parsing.
//! Must not know: compiler internals or product graph execution.

use std::ffi::OsString;

use clap::{Parser as _, Subcommand};
use nervix_ratchet::wrapper::Entry;

#[derive(clap::Parser)]
#[command(name = "nervix-ratchet")]
struct Cli {
    #[command(subcommand)]
    command: Operation,
}

#[derive(Subcommand)]
enum Operation {
    Analyze(nervix_ratchet::matrix::Analyze),
    PrepareContracts(nervix_ratchet::contracts::PrepareContracts),
    QualifyCache(nervix_ratchet::cache::QualifyCache),
}

fn main() -> anyhow::Result<()> {
    let mut arguments: Vec<OsString> = std::env::args_os().skip(1).collect();
    let wrapper = match arguments.first().and_then(|argument| argument.to_str()) {
        Some("wrap") => {
            arguments.remove(0);
            Some(Entry::Explicit)
        }
        Some(
            "analyze" | "prepare-contracts" | "qualify-cache" | "--help" | "-h" | "help"
            | "--version" | "-V",
        )
        | None => None,
        // Cargo supplies the actual rustc path as the first wrapper argument. A wrapper path
        // cannot carry a subcommand, so that invocation enters the same explicit wrap owner.
        Some(_) => Some(Entry::Cargo),
    };
    let result = if let Some(entry) = wrapper {
        match nervix_ratchet::wrapper::execute(&arguments, entry) {
            Ok(never) => match never {},
            Err(error) => Err(error),
        }
    } else {
        match Cli::parse().command {
            Operation::Analyze(options) => options.run().map(|_| 0),
            Operation::PrepareContracts(options) => options.run().map(|()| 0),
            Operation::QualifyCache(options) => options.run().map(|()| 0),
        }
    };
    let status = match result {
        Ok(status) => status,
        Err(report) => {
            eprintln!("typed ratchet: {report:?}");
            1
        }
    };
    std::process::exit(status)
}
