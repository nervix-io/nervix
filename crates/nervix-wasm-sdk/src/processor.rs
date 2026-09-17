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

    /// Releases everything the processor is still holding because the host is quiescing this
    /// branch, either to hand it to a replacement node or to shut it down.
    ///
    /// Emit every buffered output envelope through [`GuestContext::emit`]. Any input the
    /// processor keeps beyond this call stays unacknowledged until the branch resumes and never
    /// reaches a replacement, so a processor that buffers input has to emit it here rather than
    /// wait for more. State that survives the handoff belongs in [`Processor::save_state`], which
    /// the host calls next.
    ///
    /// The default is correct only for processors that never buffer between calls.
    fn flush(&mut self, ctx: &mut GuestContext<'_>) -> Result<(), GuestError> {
        let _ = ctx;
        Ok(())
    }

    /// Serializes the processor's durable computation state: what an instance recreated from it
    /// needs to continue, such as counters, aggregates, or open windows. The SDK stores the bytes
    /// in the guest snapshot and hands them back to [`Processor::restore`].
    ///
    /// Never serialize execution state. Buffered [`InputBatch`]es with their ACK sidecars, output
    /// envelopes the processor has not emitted, and [`TimeoutHandle`]s belong to the live instance
    /// and mean nothing to one created later.
    ///
    /// Return an error when the state cannot be serialized: the host reports the failed save and
    /// keeps the state saved last. As from every callback, [`GuestError::failed`] also latches the
    /// instance into error state. The default saves the empty state of a stateless processor.
    fn save_state(&self) -> Result<Vec<u8>, GuestError> {
        Ok(Vec::new())
    }

    /// Restores the branch instance from the application state [`Processor::save_state`] returned
    /// when the branch was saved last. Whenever the branch has saved state, including empty state,
    /// the instance this returns replaces the one [`Processor::create`] built for the branch. The
    /// default restores only stateless processors and rejects non-empty state instead of silently
    /// dropping it.
    ///
    /// The restored instance starts without error state and without pending timeouts. Request any
    /// timeout the restored state needs from the next callback that receives a [`GuestContext`].
    ///
    /// Returning an error is the processor's verdict that the saved application
    /// state cannot be restored: the SDK reports it to the host as a rejected
    /// application state, with the error as the reason. Whether that state is
    /// ever discarded is the host's decision, never this method's.
    fn restore(branch: &BranchContext, state: &[u8]) -> Result<Self, GuestError> {
        if state.is_empty() {
            Self::create(branch)
        } else {
            Err(GuestError::failed("processor does not restore saved state"))
        }
    }
}
