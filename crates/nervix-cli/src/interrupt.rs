//! The operator's Ctrl-C while the CLI waits for a statement.
//!
//! Layer: edges.
//!
//! - **Owns.** Waiting for the interrupt signal, and stopping a wait of the CLI's own when it
//!   arrives.
//! - **Depends on.** Tokio's signal handling.
//! - **Must not know.** What the interrupted statement was, or what its interruption means for
//!   the server.
//!
//! The REPL reads its input in raw mode, where Ctrl-C is a key its line editor reads. Only while a
//! statement runs does the terminal turn Ctrl-C into the interrupt signal this module waits for.
//! The first wait installs the process's handler for that signal, and from then on the signal
//! interrupts the statement under way rather than ending the process.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::{Report as StackReport, ResultExt as _};
use tokio::signal;

use super::ClientError;

/// Waits for the operator's next Ctrl-C.
pub(super) async fn interrupted() -> Result<(), StackReport<ClientError>> {
    signal::ctrl_c()
        .await
        .change_context(ClientError::InterruptSignal)
}

/// Runs `work` until it ends, or until Ctrl-C stops it where it waits, which is `None`.
pub(super) async fn unless_interrupted<F: Future>(
    work: F,
) -> Result<Option<F::Output>, StackReport<ClientError>> {
    nervix_primitives::select! {
        output = work => Ok(Some(output)),
        interrupt = interrupted() => {
            interrupt?;
            Ok(None)
        }
    }
}
