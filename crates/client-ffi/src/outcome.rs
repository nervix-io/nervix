//! `nx_outcome`: what became of a command, as a host reads it.
//!
//! - **Owns.** Reading an outcome's disposition, message, diagnostics, execution reference,
//!   opened subscription, backup archive and restore report, and freeing it.
//! - **Depends on.** The shared outcome, the Rust client's command outcome, the schema handle and
//!   the C ABI boundary.
//! - **Must not know.** How the command was sent, retried or redirected.

use nervix_client_host::{Disposition, Failure, FailureKind, Outcome, Schema};

use crate::abi;

/// # Safety
///
/// `outcome` is a live outcome this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_disposition(outcome: *const Outcome) -> Disposition {
    // SAFETY: the header requires a live outcome.
    unsafe { abi::accessor(outcome) }.disposition()
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_message(
    outcome: *const Outcome,
    message: *mut *const u8,
    message_len: *mut usize,
) {
    // SAFETY: the header requires a live outcome and writable out-parameters.
    unsafe {
        let outcome = abi::accessor(outcome);
        abi::write_bytes(message, message_len, outcome.command().message.as_bytes());
    }
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_execution_reference(
    outcome: *const Outcome,
    reference: *mut *const u8,
    reference_len: *mut usize,
) -> bool {
    // SAFETY: the header requires a live outcome.
    let outcome = unsafe { abi::accessor(outcome) };
    let Some(execution_reference) = &outcome.command().execution_reference else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters.
    unsafe {
        abi::write_bytes(
            reference,
            reference_len,
            execution_reference.as_str().as_bytes(),
        )
    };
    true
}

/// # Safety
///
/// `outcome` is a live outcome this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_diagnostic_count(outcome: *const Outcome) -> usize {
    // SAFETY: the header requires a live outcome.
    unsafe { abi::accessor(outcome) }
        .command()
        .diagnostics
        .len()
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_diagnostic(
    outcome: *const Outcome,
    index: usize,
    message: *mut *const u8,
    message_len: *mut usize,
    has_span: *mut bool,
    start: *mut u32,
    end: *mut u32,
) -> *mut Failure {
    // SAFETY: the header requires a live outcome and writable out-parameters.
    abi::outcome(unsafe {
        write_diagnostic(outcome, index, message, message_len, has_span, start, end)
    })
}

/// # Safety
///
/// As [`nx_outcome_diagnostic`].
unsafe fn write_diagnostic(
    outcome: *const Outcome,
    index: usize,
    message: *mut *const u8,
    message_len: *mut usize,
    has_span: *mut bool,
    start: *mut u32,
    end: *mut u32,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live outcome.
    let outcome = unsafe { abi::handle(outcome, "outcome") }?;
    let diagnostics = &outcome.command().diagnostics;
    let Some(diagnostic) = diagnostics.get(index) else {
        return Err(Failure::new(
            FailureKind::InvalidArgument,
            format!(
                "diagnostic {index} is past the {} diagnostics",
                diagnostics.len()
            ),
        ));
    };
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe {
        abi::write_bytes(message, message_len, diagnostic.message.as_bytes());
        abi::write(has_span, diagnostic.span.is_some());
        if let Some(span) = diagnostic.span {
            abi::write(start, span.start());
            abi::write(end, span.end());
        }
    }
    Ok(())
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_subscription(
    outcome: *const Outcome,
    name: *mut *const u8,
    name_len: *mut usize,
    generation: *mut u64,
) -> bool {
    // SAFETY: the header requires a live outcome.
    let outcome = unsafe { abi::accessor(outcome) };
    let Some(opened) = &outcome.command().subscription else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters.
    unsafe {
        abi::write_bytes(name, name_len, opened.subscription.name.as_str().as_bytes());
        abi::write(generation, opened.subscription.generation.get());
    }
    true
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_backup(
    outcome: *const Outcome,
    total_bytes: *mut u64,
    digest: *mut *const u8,
    digest_len: *mut usize,
) -> bool {
    // SAFETY: the header requires a live outcome.
    let outcome = unsafe { abi::accessor(outcome) };
    let Some(summary) = &outcome.command().backup else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters.
    unsafe {
        abi::write(total_bytes, summary.total_bytes.get());
        abi::write_bytes(digest, digest_len, summary.digest.as_bytes());
    }
    true
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_restore(
    outcome: *const Outcome,
    dry_run: *mut bool,
    domains: *mut u64,
    resource_versions: *mut u64,
    models: *mut u64,
    failed: *mut bool,
) -> bool {
    // SAFETY: the header requires a live outcome.
    let outcome = unsafe { abi::accessor(outcome) };
    let Some(summary) = outcome.restore_summary() else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters.
    unsafe {
        abi::write(dry_run, summary.dry_run);
        abi::write(domains, summary.domains);
        abi::write(resource_versions, summary.resource_versions);
        abi::write(models, summary.models);
        abi::write(failed, summary.failed);
    }
    true
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_schema(
    outcome: *const Outcome,
    out: *mut *mut Schema,
) -> *mut Failure {
    // SAFETY: the header requires a live outcome and a writable `out`.
    abi::outcome(unsafe { write_schema(outcome, out) })
}

/// # Safety
///
/// As [`nx_outcome_schema`].
unsafe fn write_schema(outcome: *const Outcome, out: *mut *mut Schema) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live outcome.
    let outcome = unsafe { abi::handle(outcome, "outcome") }?;
    abi::require_out(out, "out")?;
    let schema = outcome.schema()?.clone();
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(schema)) };
    Ok(())
}

/// # Safety
///
/// A non-null `outcome` is an outcome this library returned that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_free(outcome: *mut Outcome) {
    // SAFETY: the header requires an unreleased outcome or null.
    unsafe { abi::release(outcome) };
}
