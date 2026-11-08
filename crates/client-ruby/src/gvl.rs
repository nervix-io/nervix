//! The protected boundary for a blocking native call on its calling Ruby thread.
//!
//! Layer: edges.
//! - **Owns.** GVL release, call-local interruption and containing both Ruby and Rust unwinding.
//! - **Depends on.** Magnus protection, rb-sys's GVL API and host cancellation.
//! - **Must not know.** Sessions, commands, endpoint state or any Ruby callback from a worker.

use std::{
    any::Any,
    ffi::c_void,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
};

use magnus::{
    Error, Ruby,
    rb_sys::{AsRawValue, protect},
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_host::{Cancel, Failure};

enum Completed<T> {
    Outcome(Result<T, Failure>),
    Panicked(Box<dyn Any + Send>),
}

struct Call<'a, F, T> {
    work: Option<F>,
    result: Option<Completed<T>>,
    cancel: &'a Cancel,
}

unsafe extern "C" fn run<F, T>(data: *mut c_void) -> *mut c_void
where
    F: FnOnce(&Cancel) -> Result<T, Failure> + Send,
    T: Send,
{
    // SAFETY: without_gvl keeps this exact Call alive, exclusively on the calling thread,
    // until rb_thread_call_without_gvl returns. The callback runs exactly once, synchronously.
    let call = unsafe { &mut *data.cast::<Call<'_, F, T>>() };
    let result = catch_unwind(AssertUnwindSafe(|| {
        let work = call
            .work
            .take()
            .assured("Ruby invokes the native callback exactly once");
        work(call.cancel)
    }));
    let completed = match result {
        Ok(outcome) => Completed::Outcome(outcome),
        Err(panic) => Completed::Panicked(panic),
    };
    call.result = Some(completed);
    std::ptr::null_mut()
}

unsafe extern "C" fn interrupt(data: *mut c_void) {
    // SAFETY: The distinct immutable cancellation field outlives the callback. The host token
    // permits trigger from any thread and this function never accesses Ruby or mutable Call data.
    let cancel = unsafe { &*data.cast::<Cancel>() };
    cancel.trigger();
}

/// Work contains only Send native values. Ruby values cannot cross the released-GVL boundary.
pub(crate) fn without_gvl<F, T>(cancel: Option<Cancel>, work: F) -> Result<T, Error>
where
    F: FnOnce(&Cancel) -> Result<T, Failure> + Send,
    T: Send,
{
    let cancel = cancel.unwrap_or_default();
    let mut call = Call {
        work: Some(work),
        result: None,
        cancel: &cancel,
    };
    let interrupt_data = std::ptr::from_ref(&cancel).cast_mut().cast();
    let work_data = std::ptr::from_mut(&mut call).cast();
    let ruby = Ruby::get().assured("a native Ruby callback holds the GVL");
    let protected = protect(|| {
        // SAFETY: Both data pointers stay valid until this synchronous call returns; the work
        // callback catches Rust panics and calls no Ruby, the unblock callback only triggers
        // cancellation, and magnus catches Ruby's interrupt before Rust owners are dropped.
        unsafe {
            rb_sys::rb_thread_call_without_gvl(
                Some(run::<F, T>),
                work_data,
                Some(interrupt),
                interrupt_data,
            );
        }
        ruby.qnil().as_raw()
    });
    let result = match call.result.take() {
        Some(Completed::Panicked(panic)) => resume_unwind(panic),
        result => result,
    };
    protected?;
    let result = result.assured("a successful Ruby GVL call invoked its native callback");
    match result {
        Completed::Outcome(result) => result.map_err(crate::errors::raise),
        Completed::Panicked(panic) => resume_unwind(panic),
    }
}
