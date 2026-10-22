//! Layer: edges.
//! Owns: node CLI arguments and command variants, composed of one options group per area.
//! Depends on: control-plane configuration values, Clap and the shared vocabulary.
//! Must not know: graph execution or connector internals.
#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns node command-line configuration")
)]

use clap::Subcommand;

use super::*;

mod consensus;
#[cfg(feature = "deloxide")]
mod diagnostics;
mod membership;
mod memory_pressure;
mod node;
mod resources;
mod runtime;
mod scheduling;
mod settings;
mod shutdown;
#[cfg(test)]
mod tests;
mod trace_export;
mod transactions;

pub use consensus::ConsensusArgs;
#[cfg(feature = "deloxide")]
pub use diagnostics::DiagnosticArgs;
pub use membership::MembershipArgs;
pub use memory_pressure::MemoryPressureArgs;
pub use node::NodeArgs;
pub use resources::ResourceArgs;
pub use runtime::RuntimeArgs;
pub use scheduling::SchedulingArgs;
pub use settings::{SettingOverride, SettingsArgs};
pub use shutdown::ShutdownArgs;
pub use trace_export::TracingArgs;
pub use transactions::TransactionArgs;

#[derive(Parser, Debug, Clone)]
#[command(name = "nervix-server")]
#[command(about = "NSPL gRPC server")]
pub struct Args {
    /// Who the node is, where it listens and is reached, and the files and directories it uses.
    #[command(flatten)]
    pub node: NodeArgs,
    /// Every setting, one group per area.
    #[command(flatten)]
    pub settings: SettingsArgs,
    #[command(flatten)]
    pub tracing: TracingArgs,
    #[cfg(feature = "deloxide")]
    #[command(flatten)]
    pub diagnostics: DiagnosticArgs,
    #[command(subcommand)]
    pub subcommand: Option<Command>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Generate shell completion scripts
    Completions {
        /// Target shell
        shell: Shell,
    },
}
