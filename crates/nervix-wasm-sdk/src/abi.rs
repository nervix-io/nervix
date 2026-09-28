//! Guest-side implementation of the Nervix WASM C ABI.
//!
//! [`crate::export_processor!`] expands to thin `nervix_*` exports that
//! delegate here. Guests execute single-threaded and the host never re-enters
//! an export while another is on the stack; that execution contract is what
//! makes the interior-mutable statics below sound.

use std::{cell::UnsafeCell, ops::Range, panic::AssertUnwindSafe};

use error_stack::{FrameKind, Report, Result, ResultExt as _};
use nervix_wasm_protocol::BranchInit;

use crate::{
    context::{BranchContext, GuestContext, TimeoutHandle},
    envelope::InputBatch,
    error::{
        ERR_ERROR_STATE, ERR_INVALID_SIZE, ERR_OUT_OF_BOUNDS, GuestError, RejectedSnapshot, SUCCESS,
    },
    processor::Processor,
};

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "env")]
unsafe extern "C" {
    fn nervix_domain_time_nanos() -> i64;
    fn nervix_timeout_after_nanos(delay_nanos: i64) -> i64;
    fn nervix_request_state_reset() -> i32;
}

pub(crate) fn host_domain_time_nanos() -> i64 {
    #[cfg(target_arch = "wasm32")]
    {
        unsafe { nervix_domain_time_nanos() }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        panic!("nervix_domain_time_nanos is only callable inside a Nervix WASM guest")
    }
}

pub(crate) fn host_timeout_after_nanos(delay_nanos: i64) -> i64 {
    #[cfg(target_arch = "wasm32")]
    {
        unsafe { nervix_timeout_after_nanos(delay_nanos) }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = delay_nanos;
        panic!("nervix_timeout_after_nanos is only callable inside a Nervix WASM guest")
    }
}

pub(crate) fn host_request_state_reset() -> i32 {
    #[cfg(target_arch = "wasm32")]
    {
        unsafe { nervix_request_state_reset() }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        panic!("nervix_request_state_reset is only callable inside a Nervix WASM guest")
    }
}

/// SDK-owned guest runtime state shared by every export.
///
/// All of it is execution state of this instance. A snapshot carries only the branch
/// configuration and the processor's application state, so an instance restored from one starts
/// with an empty emit queue and without error state.
struct RuntimeCore {
    buffer: Vec<u8>,
    pending_emit: Vec<Vec<u8>>,
    global_error: Vec<u8>,
    error_state: Option<String>,
    branch: Option<BranchContext>,
}

impl RuntimeCore {
    const fn new() -> Self {
        Self {
            buffer: Vec::new(),
            pending_emit: Vec::new(),
            global_error: Vec::new(),
            error_state: None,
            branch: None,
        }
    }

    fn alloc(&mut self, size: usize) -> i32 {
        if self.buffer.capacity() < size {
            self.buffer.reserve_exact(size - self.buffer.capacity());
        }
        self.buffer.resize(size, 0);
        abi_pointer(self.buffer.as_mut_ptr())
    }

    fn buffer_range(&self, ptr: i32, size: i32) -> Result<Range<usize>, GuestError> {
        let ptr = usize::try_from(ptr).map_err(|_| Report::new(GuestError::OutOfBounds))?;
        let size = usize::try_from(size).map_err(|_| Report::new(GuestError::InvalidSize))?;
        let end = ptr
            .checked_add(size)
            .ok_or_else(|| Report::new(GuestError::OutOfBounds))?;
        let base = self.buffer.as_ptr().addr();
        if ptr < base || end > base + self.buffer.len() {
            return Err(Report::new(GuestError::OutOfBounds));
        }
        Ok(ptr - base..end - base)
    }

    fn read_buffer(&self, ptr: i32, size: i32) -> Result<Vec<u8>, GuestError> {
        Ok(self.buffer[self.buffer_range(ptr, size)?].to_vec())
    }

    fn guest_context(&mut self) -> Result<GuestContext<'_>, GuestError> {
        let Some(branch) = &self.branch else {
            return Err(Report::new(GuestError::NotInitialized));
        };
        Ok(GuestContext {
            branch,
            pending_emit: &mut self.pending_emit,
            global_error: &mut self.global_error,
            error_state: &mut self.error_state,
        })
    }

    fn enter_error_state(&mut self, reason: &str) {
        self.error_state = Some(reason.to_string());
        self.global_error.clear();
        self.global_error.extend_from_slice(reason.as_bytes());
    }

    /// Reports why the saved state handed to `nervix_load_state` cannot be restored and returns the
    /// code that classifies the verdict.
    ///
    /// The reason goes on the global-error channel, but nothing is latched: the host discards an
    /// instance whose restore failed, so there is no later callback for a latch to refuse.
    fn reject_saved_state(&mut self, rejected: &Report<RejectedSnapshot>) -> i32 {
        let skip_application_context = matches!(
            rejected.current_context(),
            RejectedSnapshot::ApplicationState
        );
        let mut reason = String::new();
        let mut first_context = true;
        for frame in rejected.frames() {
            let FrameKind::Context(context) = frame.kind() else {
                continue;
            };
            if first_context && skip_application_context {
                first_context = false;
                continue;
            }
            if !reason.is_empty() {
                reason.push_str(": ");
            }
            reason.push_str(&context.to_string());
            first_context = false;
        }
        self.global_error.clear();
        self.global_error.extend_from_slice(reason.as_bytes());
        rejected.current_context().verdict().code()
    }

    /// Clears guest-owned state while keeping the reusable buffer allocation.
    fn reset(&mut self) {
        self.pending_emit.clear();
        self.global_error.clear();
        self.error_state = None;
        self.branch = None;
    }
}

struct GlobalCell<T>(UnsafeCell<T>);

// SAFETY: guests execute single-threaded and the host never re-enters an
// export while another is running.
unsafe impl<T> Sync for GlobalCell<T> {}

impl<T> GlobalCell<T> {
    const fn new(value: T) -> Self {
        Self(UnsafeCell::new(value))
    }

    fn with<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let value = unsafe { &mut *self.0.get() };
        f(value)
    }
}

static CORE: GlobalCell<RuntimeCore> = GlobalCell::new(RuntimeCore::new());

/// Storage for the single processor instance, declared by
/// [`crate::export_processor!`] because statics cannot be generic.
pub struct InstanceSlot<P>(GlobalCell<Option<P>>);

impl<P> InstanceSlot<P> {
    pub const fn new() -> Self {
        Self(GlobalCell::new(None))
    }

    fn with<R>(&self, f: impl FnOnce(&mut Option<P>) -> R) -> R {
        self.0.with(f)
    }
}

impl<P> Default for InstanceSlot<P> {
    fn default() -> Self {
        Self::new()
    }
}

fn panic_reason(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(reason) = payload.downcast_ref::<&str>() {
        format!("guest panic: {reason}")
    } else if let Some(reason) = payload.downcast_ref::<String>() {
        format!("guest panic: {reason}")
    } else {
        "guest panic".to_string()
    }
}

fn guarded(
    check_error_state: bool,
    f: impl FnOnce(&mut RuntimeCore) -> Result<i32, GuestError>,
) -> i32 {
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        CORE.with(|core| {
            if check_error_state && let Some(error_state) = &core.error_state {
                if core.global_error.is_empty() {
                    let reason = error_state.clone();
                    core.global_error.extend_from_slice(reason.as_bytes());
                }
                return ERR_ERROR_STATE;
            }
            match f(core) {
                Ok(code) => code,
                Err(error) if matches!(error.current_context(), GuestError::Failed { .. }) => {
                    core.enter_error_state(&format!("{error:#}"));
                    ERR_ERROR_STATE
                }
                Err(error) => error.current_context().abi_code(),
            }
        })
    }));
    match result {
        Ok(code) => code,
        Err(payload) => CORE.with(|core| {
            core.enter_error_state(&panic_reason(payload));
            ERR_ERROR_STATE
        }),
    }
}

pub fn buffer_ptr() -> i32 {
    CORE.with(|core| abi_pointer(core.buffer.as_mut_ptr()))
}

fn abi_size(size: usize) -> i32 {
    match i32::try_from(size) {
        Ok(size) => size,
        Err(_) => ERR_INVALID_SIZE,
    }
}

/// Renders a guest address as the `i32` the C ABI passes addresses in.
///
/// A wasm32 guest addresses at most four gigabytes, so an address the ABI cannot express is a
/// buffer the host could never read back.
fn abi_pointer<T>(ptr: *const T) -> i32 {
    match i32::try_from(ptr.addr()) {
        Ok(address) => address,
        Err(_) => ERR_OUT_OF_BOUNDS,
    }
}

pub fn buffer_len() -> i32 {
    CORE.with(|core| abi_size(core.buffer.len()))
}

pub fn buffer_capacity() -> i32 {
    CORE.with(|core| abi_size(core.buffer.capacity()))
}

pub fn alloc(size: i32) -> i32 {
    let Ok(size) = usize::try_from(size) else {
        return ERR_INVALID_SIZE;
    };
    CORE.with(|core| core.alloc(size))
}

pub fn global_error_ptr() -> i32 {
    CORE.with(|core| {
        if core.global_error.is_empty() {
            0
        } else {
            abi_pointer(core.global_error.as_mut_ptr())
        }
    })
}

pub fn global_error_len() -> i32 {
    CORE.with(|core| abi_size(core.global_error.len()))
}

pub fn clear_global_error() -> i32 {
    CORE.with(|core| {
        core.global_error.clear();
        SUCCESS
    })
}

pub fn init<P: Processor>(slot: &InstanceSlot<P>, ptr: i32, size: i32) -> i32 {
    guarded(true, |core| {
        let metadata = core.read_buffer(ptr, size)?;
        let branch = BranchContext::from(
            BranchInit::decode(&metadata).change_context(GuestError::Protocol)?,
        );
        let instance = P::create(&branch)?;
        core.branch = Some(branch);
        slot.with(|slot| *slot = Some(instance));
        Ok(SUCCESS)
    })
}

pub fn current_domain_time_nanos() -> i64 {
    host_domain_time_nanos()
}

pub fn process_batch<P: Processor>(slot: &InstanceSlot<P>, ptr: i32, size: i32) -> i32 {
    guarded(true, |core| {
        if core.branch.is_none() {
            return Err(Report::new(GuestError::NotInitialized));
        }
        let input = InputBatch::from_envelope_bytes(core.read_buffer(ptr, size)?)?;
        let mut ctx = core.guest_context()?;
        slot.with(|instance| {
            let Some(instance) = instance.as_mut() else {
                return Err(Report::new(GuestError::NotInitialized));
            };
            instance.process_batch(&mut ctx, input)
        })?;
        Ok(SUCCESS)
    })
}

pub fn on_timeout<P: Processor>(slot: &InstanceSlot<P>, handle: i64) -> i32 {
    guarded(true, |core| {
        let mut ctx = core.guest_context()?;
        slot.with(|instance| {
            let Some(instance) = instance.as_mut() else {
                return Err(Report::new(GuestError::NotInitialized));
            };
            instance.on_timeout(&mut ctx, TimeoutHandle::new(handle))
        })?;
        Ok(SUCCESS)
    })
}

pub fn flush<P: Processor>(slot: &InstanceSlot<P>) -> i32 {
    guarded(true, |core| {
        if core.branch.is_none() {
            return Err(Report::new(GuestError::NotInitialized));
        }
        let mut ctx = core.guest_context()?;
        slot.with(|instance| {
            let Some(instance) = instance.as_mut() else {
                return Err(Report::new(GuestError::NotInitialized));
            };
            instance.flush(&mut ctx)
        })?;
        Ok(SUCCESS)
    })
}

pub fn read_emit() -> i32 {
    guarded(true, |core| {
        if core.pending_emit.is_empty() {
            return Ok(0);
        }
        let envelope = core.pending_emit.remove(0);
        core.buffer.clear();
        core.buffer.extend_from_slice(&envelope);
        i32::try_from(core.buffer.len()).map_err(|_| Report::new(GuestError::InvalidSize))
    })
}

/// Encodes the snapshot of the processor into the reusable buffer and returns its size.
///
/// A processor that latched into error state still saves its application state, because error
/// state belongs to this instance and never enters the snapshot. An error from
/// [`Processor::save_state`] fails the call like an error from any other callback, and the host
/// keeps the state saved last.
pub fn dump_state<P: Processor>(slot: &InstanceSlot<P>) -> i32 {
    guarded(false, |core| {
        let Some(branch) = &core.branch else {
            return Err(Report::new(GuestError::NotInitialized));
        };
        let application_state = slot.with(|instance| {
            let Some(instance) = instance.as_ref() else {
                return Err(Report::new(GuestError::NotInitialized));
            };
            instance.save_state()
        })?;
        core.buffer = branch.encode_snapshot(application_state);
        i32::try_from(core.buffer.len()).map_err(|_| Report::new(GuestError::InvalidSize))
    })
}

/// Restores the processor from the saved state the host hands over, into the branch
/// configuration `nervix_init` initialized this instance with.
///
/// A range the host did not allocate fails the call with its own code, because it says nothing
/// about the saved state, and so does a call before `nervix_init`. Everything after that is a
/// verdict on the state: a snapshot the SDK cannot decode, including its init metadata, or one
/// taken under a different branch configuration rejects the snapshot envelope, and an error from
/// [`Processor::restore`] rejects the application state it carries.
pub fn load_state<P: Processor>(slot: &InstanceSlot<P>, ptr: i32, size: i32) -> i32 {
    guarded(false, |core| {
        let saved = core.read_buffer(ptr, size)?;
        let Some(branch) = &core.branch else {
            return Err(Report::new(GuestError::NotInitialized));
        };
        match branch.restore_snapshot::<P>(&saved) {
            Ok(restored) => {
                slot.with(|slot| *slot = Some(restored));
                Ok(SUCCESS)
            }
            Err(rejected) => Ok(core.reject_saved_state(&rejected)),
        }
    })
}

pub fn reset_state<P: Processor>(slot: &InstanceSlot<P>) -> i32 {
    CORE.with(RuntimeCore::reset);
    slot.with(|slot| *slot = None);
    SUCCESS
}

#[cfg(test)]
mod tests {
    use nervix_wasm_protocol::{ProtocolError, SavedStateRejection};

    use super::*;

    #[test]
    fn rejected_snapshot_reports_its_protocol_cause_without_latching() {
        let rejected = Report::new(ProtocolError::InvalidIdentifier)
            .change_context(RejectedSnapshot::UndecodableEnvelope);
        let mut core = RuntimeCore::new();

        let code = core.reject_saved_state(&rejected);

        assert_eq!(code, SavedStateRejection::SnapshotEnvelope.code());
        assert_eq!(
            String::from_utf8(core.global_error).expect("the rejection reason is text"),
            format!(
                "saved state is not a guest snapshot envelope: {}",
                ProtocolError::InvalidIdentifier
            )
        );
        assert!(core.error_state.is_none());
    }

    #[test]
    fn application_rejection_keeps_the_processor_reason_as_its_abi_text() {
        let rejected = GuestError::failed("saved count must be exactly 8 bytes")
            .change_context(RejectedSnapshot::ApplicationState);
        let mut core = RuntimeCore::new();

        let code = core.reject_saved_state(&rejected);

        assert_eq!(code, SavedStateRejection::ApplicationState.code());
        assert_eq!(
            String::from_utf8(core.global_error).expect("the rejection reason is text"),
            "saved count must be exactly 8 bytes"
        );
        assert!(core.error_state.is_none());
    }

    #[test]
    fn callback_report_is_rendered_and_latched_only_at_the_abi_boundary() {
        CORE.with(RuntimeCore::reset);
        let code = guarded(true, |_| {
            Err(
                Report::new(ProtocolError::InvalidIdentifier).change_context(GuestError::Failed {
                    reason: "callback rejected its input".to_string(),
                }),
            )
        });

        assert_eq!(code, ERR_ERROR_STATE);
        let reason = CORE.with(|core| {
            String::from_utf8(core.global_error.clone()).expect("the global error is text")
        });
        assert_eq!(
            reason,
            format!(
                "callback rejected its input: {}",
                ProtocolError::InvalidIdentifier
            )
        );

        CORE.with(|core| core.global_error.clear());
        assert_eq!(guarded(true, |_| Ok(SUCCESS)), ERR_ERROR_STATE);
        CORE.with(|core| {
            assert_eq!(core.global_error, reason.as_bytes());
            core.reset();
        });
    }
}
