//! High-level SDK for building Rust Nervix WASM processor guests.
//!
//! Implement [`Processor`] for a branch-local type and register it with
//! [`export_processor!`]. The SDK owns the complete guest side of the Nervix
//! WASM C ABI: the reusable linear-memory buffer, FlatBuffers envelope
//! encoding and decoding, the emit queue, the global-error channel, panic
//! conversion, error-state latching, and guest snapshot plumbing. Processor
//! code only sees typed inputs ([`InputBatch`]), typed outputs
//! ([`OutputEnvelope`]), and the branch configuration ([`BranchContext`]).
//!
//! ```ignore
//! use nervix_wasm_sdk::{BranchContext, GuestContext, GuestError, InputBatch, Processor};
//!
//! struct Passthrough;
//!
//! impl Processor for Passthrough {
//!     fn create(_branch: &BranchContext) -> Result<Self, GuestError> {
//!         Ok(Self)
//!     }
//!
//!     fn process_batch(
//!         &mut self,
//!         ctx: &mut GuestContext<'_>,
//!         input: InputBatch,
//!     ) -> Result<(), GuestError> {
//!         // Build an OutputEnvelope from `input` and queue it with ctx.emit(...).
//!         Ok(())
//!     }
//! }
//!
//! nervix_wasm_sdk::export_processor!(Passthrough);
//! ```

pub mod abi;
mod context;
mod envelope;
mod error;
mod processor;

pub use nervix_wasm_protocol::{
    AckSidecar, AckToken, AckTokenSet, MessageErrorSet, NackSet, OutputColumnRef, OutputRow,
    ProcessorField, ProcessorSchema, ProcessorType,
};

pub use crate::{
    context::{BranchContext, DomainTime, GuestContext, TimeoutHandle},
    envelope::{InputBatch, OutputEnvelope, ProcessorTypeArrow},
    error::GuestError,
    processor::Processor,
};

/// Exports the complete Nervix WASM guest C ABI for one [`Processor`]
/// implementation. Invoke exactly once at the crate root of a `cdylib`
/// targeting `wasm32-unknown-unknown`.
#[macro_export]
macro_rules! export_processor {
    ($processor:ty) => {
        const _: () = {
            static INSTANCE: $crate::abi::InstanceSlot<$processor> =
                $crate::abi::InstanceSlot::new();

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_buffer_ptr() -> i32 {
                $crate::abi::buffer_ptr()
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_buffer_len() -> i32 {
                $crate::abi::buffer_len()
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_buffer_capacity() -> i32 {
                $crate::abi::buffer_capacity()
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_alloc(size: i32) -> i32 {
                $crate::abi::alloc(size)
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_global_error_ptr() -> i32 {
                $crate::abi::global_error_ptr()
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_global_error_len() -> i32 {
                $crate::abi::global_error_len()
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_clear_global_error() -> i32 {
                $crate::abi::clear_global_error()
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_init(ptr: i32, size: i32) -> i32 {
                $crate::abi::init(&INSTANCE, ptr, size)
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_current_domain_time_nanos() -> i64 {
                $crate::abi::current_domain_time_nanos()
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_process_batch(ptr: i32, size: i32) -> i32 {
                $crate::abi::process_batch(&INSTANCE, ptr, size)
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_on_timeout(handle: i64) -> i32 {
                $crate::abi::on_timeout(&INSTANCE, handle)
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_read_emit() -> i32 {
                $crate::abi::read_emit()
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_dump_state() -> i32 {
                $crate::abi::dump_state(&INSTANCE)
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_load_state(ptr: i32, size: i32) -> i32 {
                $crate::abi::load_state(&INSTANCE, ptr, size)
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn nervix_reset_state() -> i32 {
                $crate::abi::reset_state(&INSTANCE)
            }
        };
    };
}
