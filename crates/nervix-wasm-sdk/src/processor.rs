use crate::{
    context::{BranchContext, GuestContext, TimeoutHandle},
    envelope::InputBatch,
    error::GuestError,
};

/// Branch-local guest processor logic.
///
/// The host creates one instance per concrete branch, so implementations own
/// only branch-local state and must never aggregate across branch keys.
/// Register the implementation with [`crate::export_processor!`].
pub trait Processor: Sized {
    /// Creates the branch instance from the host `BranchInit` configuration.
    fn create(branch: &BranchContext) -> Result<Self, GuestError>;

    /// Handles one host input envelope. Queue output envelopes with
    /// [`GuestContext::emit`]; the host collects them after this call
    /// returns.
    fn process_batch(
        &mut self,
        ctx: &mut GuestContext<'_>,
        input: InputBatch,
    ) -> Result<(), GuestError>;

    /// Handles a domain-clock timeout previously requested through
    /// [`GuestContext::request_timeout`].
    fn on_timeout(
        &mut self,
        ctx: &mut GuestContext<'_>,
        handle: TimeoutHandle,
    ) -> Result<(), GuestError> {
        let _ = (ctx, handle);
        Ok(())
    }

    /// Serializes processor-owned state. The bytes are persisted opaquely
    /// inside the guest snapshot and handed back to [`Processor::restore`].
    fn save_state(&self) -> Vec<u8> {
        Vec::new()
    }

    /// Restores the branch instance from bytes previously produced by
    /// [`Processor::save_state`]. The default restores only stateless
    /// processors and rejects non-empty state instead of silently dropping
    /// it.
    fn restore(branch: &BranchContext, state: &[u8]) -> Result<Self, GuestError> {
        if state.is_empty() {
            Self::create(branch)
        } else {
            Err(GuestError::failed("processor does not restore saved state"))
        }
    }
}
