//! Guest-side implementation of the Nervix WASM C ABI.
//!
//! [`crate::export_processor!`] expands to thin `nervix_*` exports that
//! delegate here. Guests execute single-threaded and the host never re-enters
//! an export while another is on the stack; that execution contract is what
//! makes the interior-mutable statics below sound.

use std::{cell::UnsafeCell, ops::Range, panic::AssertUnwindSafe};

use nervix_wasm_protocol::GuestSnapshot;

use crate::{
    context::{BranchContext, GuestContext, TimeoutHandle},
    envelope::InputBatch,
    error::{ERR_ERROR_STATE, ERR_INVALID_SIZE, GuestError, SUCCESS},
    processor::Processor,
};

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "env")]
unsafe extern "C" {
    fn nervix_domain_time_nanos() -> i64;
    fn nervix_timeout_after_nanos(delay_nanos: i64) -> i64;
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

/// SDK-owned guest runtime state shared by every export.
struct RuntimeCore {
    buffer: Vec<u8>,
    pending_emit: Vec<Vec<u8>>,
    global_error: Vec<u8>,
    error_state: Option<String>,
    init_metadata: Vec<u8>,
    branch: Option<BranchContext>,
    processed_batches: u64,
    processed_rows: u64,
    last_domain_time_nanos: i64,
    last_timeout_handle: i64,
}

impl RuntimeCore {
    const fn new() -> Self {
        Self {
            buffer: Vec::new(),
            pending_emit: Vec::new(),
            global_error: Vec::new(),
            error_state: None,
            init_metadata: Vec::new(),
            branch: None,
            processed_batches: 0,
            processed_rows: 0,
            last_domain_time_nanos: 0,
            last_timeout_handle: 0,
        }
    }

    fn alloc(&mut self, size: usize) -> i32 {
        if self.buffer.capacity() < size {
            self.buffer.reserve_exact(size - self.buffer.capacity());
        }
        self.buffer.resize(size, 0);
        self.buffer.as_mut_ptr() as i32
    }

    fn buffer_range(&self, ptr: i32, size: i32) -> Result<Range<usize>, GuestError> {
        let ptr = usize::try_from(ptr).map_err(|_| GuestError::OutOfBounds)?;
        let size = usize::try_from(size).map_err(|_| GuestError::InvalidSize)?;
        let end = ptr.checked_add(size).ok_or(GuestError::OutOfBounds)?;
        let base = self.buffer.as_ptr() as usize;
        if ptr < base || end > base + self.buffer.len() {
            return Err(GuestError::OutOfBounds);
        }
        Ok(ptr - base..end - base)
    }

    fn read_buffer(&self, ptr: i32, size: i32) -> Result<Vec<u8>, GuestError> {
        Ok(self.buffer[self.buffer_range(ptr, size)?].to_vec())
    }

    fn guest_context(&mut self) -> Result<GuestContext<'_>, GuestError> {
        let Some(branch) = &self.branch else {
            return Err(GuestError::NotInitialized);
        };
        Ok(GuestContext {
            branch,
            pending_emit: &mut self.pending_emit,
            global_error: &mut self.global_error,
            error_state: &mut self.error_state,
            last_domain_time_nanos: &mut self.last_domain_time_nanos,
            last_timeout_handle: &mut self.last_timeout_handle,
        })
    }

    fn enter_error_state(&mut self, reason: &str) {
        self.error_state = Some(reason.to_string());
        self.global_error.clear();
        self.global_error.extend_from_slice(reason.as_bytes());
    }

    /// Clears guest-owned state while keeping the reusable buffer allocation.
    fn reset(&mut self) {
        self.pending_emit.clear();
        self.global_error.clear();
        self.error_state = None;
        self.init_metadata.clear();
        self.branch = None;
        self.processed_batches = 0;
        self.processed_rows = 0;
        self.last_domain_time_nanos = 0;
        self.last_timeout_handle = 0;
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
                Err(GuestError::Failed { reason }) => {
                    core.enter_error_state(&reason);
                    ERR_ERROR_STATE
                }
                Err(error) => error.abi_code(),
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
    CORE.with(|core| core.buffer.as_mut_ptr() as i32)
}

pub fn buffer_len() -> i32 {
    CORE.with(|core| core.buffer.len() as i32)
}

pub fn buffer_capacity() -> i32 {
    CORE.with(|core| core.buffer.capacity() as i32)
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
            core.global_error.as_mut_ptr() as i32
        }
    })
}

pub fn global_error_len() -> i32 {
    CORE.with(|core| core.global_error.len() as i32)
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
        let branch = BranchContext::from_init_metadata(&metadata)?;
        let instance = P::create(&branch)?;
        core.init_metadata = metadata;
        core.branch = Some(branch);
        slot.with(|slot| *slot = Some(instance));
        Ok(SUCCESS)
    })
}

pub fn current_domain_time_nanos() -> i64 {
    let now = host_domain_time_nanos();
    CORE.with(|core| core.last_domain_time_nanos = now);
    now
}

pub fn process_batch<P: Processor>(slot: &InstanceSlot<P>, ptr: i32, size: i32) -> i32 {
    guarded(true, |core| {
        if core.branch.is_none() {
            return Err(GuestError::NotInitialized);
        }
        let input = InputBatch::from_envelope_bytes(core.read_buffer(ptr, size)?)?;
        core.processed_batches = core.processed_batches.saturating_add(1);
        core.processed_rows = core.processed_rows.saturating_add(input.row_count());
        let mut ctx = core.guest_context()?;
        slot.with(|instance| {
            let Some(instance) = instance.as_mut() else {
                return Err(GuestError::NotInitialized);
            };
            instance.process_batch(&mut ctx, input)
        })?;
        Ok(SUCCESS)
    })
}

pub fn on_timeout<P: Processor>(slot: &InstanceSlot<P>, handle: i64) -> i32 {
    guarded(true, |core| {
        core.last_timeout_handle = handle;
        let mut ctx = core.guest_context()?;
        slot.with(|instance| {
            let Some(instance) = instance.as_mut() else {
                return Err(GuestError::NotInitialized);
            };
            instance.on_timeout(&mut ctx, TimeoutHandle::new(handle))
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
        Ok(core.buffer.len() as i32)
    })
}

pub fn dump_state<P: Processor>(slot: &InstanceSlot<P>) -> i32 {
    guarded(false, |core| {
        let saved_state =
            slot.with(|instance| instance.as_ref().map(P::save_state).unwrap_or_default());
        let snapshot = GuestSnapshot {
            processed_batches: core.processed_batches,
            processed_rows: core.processed_rows,
            pending_start_row: 0,
            last_domain_time_nanos: core.last_domain_time_nanos,
            last_timeout_handle: core.last_timeout_handle,
            pending_batch: Vec::new(),
            init_metadata: core.init_metadata.clone(),
            saved_state,
            error_state: core.error_state.clone(),
        };
        core.buffer = snapshot.encode();
        Ok(core.buffer.len() as i32)
    })
}

pub fn load_state<P: Processor>(slot: &InstanceSlot<P>, ptr: i32, size: i32) -> i32 {
    guarded(false, |core| {
        let bytes = core.read_buffer(ptr, size)?;
        let snapshot = GuestSnapshot::decode(&bytes)?;
        if !snapshot.pending_batch.is_empty() {
            return Err(GuestError::UnsupportedSnapshot);
        }
        let branch = BranchContext::from_init_metadata(&snapshot.init_metadata)?;
        let instance = if snapshot.error_state.is_none() {
            Some(P::restore(&branch, &snapshot.saved_state)?)
        } else {
            None
        };
        core.processed_batches = snapshot.processed_batches;
        core.processed_rows = snapshot.processed_rows;
        core.last_domain_time_nanos = snapshot.last_domain_time_nanos;
        core.last_timeout_handle = snapshot.last_timeout_handle;
        core.init_metadata = snapshot.init_metadata;
        core.error_state = snapshot.error_state;
        core.branch = Some(branch);
        slot.with(|slot| *slot = instance);
        Ok(SUCCESS)
    })
}

pub fn reset_state<P: Processor>(slot: &InstanceSlot<P>) -> i32 {
    CORE.with(RuntimeCore::reset);
    slot.with(|slot| *slot = None);
    SUCCESS
}
