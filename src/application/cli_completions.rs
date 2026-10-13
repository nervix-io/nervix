//! Shell completion generation at the server edge.
//!
//! Layer: edges.
//!
//! - **Owns.** Rendering shell completions from the server's command-line definition.
//! - **Depends on.** The command-line arguments and clap's completion generator.
//! - **Must not know.** Runtime state, sessions, graph planning or cluster coordination.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns shell completion output at the server edge")
)]

use clap::CommandFactory;
use clap_complete::{Shell, generate};

use super::Args;

pub(in crate::application) fn print_completions(shell: Shell) {
    let mut command = Args::command();
    let bin_name = command.get_name().to_string();
    generate(shell, &mut command, bin_name, &mut std::io::stdout());
}
