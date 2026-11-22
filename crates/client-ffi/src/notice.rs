//! `nx_notice`: a server notice a host holds, and the level, kind, text and reported error it reads.
//!
//! Layer: edges.
//!
//! - **Owns.** The shared ownership of a notice a host retains and releases, and borrowing its text
//!   and the class, node kind and node name of the error it reports for as long as a reference to
//!   it is held.
//! - **Depends on.** The shared notice and the C ABI boundary.
//! - **Must not know.** What a notice reports or how notices are queued; the shared notice and the
//!   session decide that.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::mem::ManuallyDrop;

use nervix_client_host::{Notice, NoticeKind, NoticeLevel};
use nervix_primitives::sync::Arc;

use crate::abi;

/// # Safety
///
/// `notice` is a live notice this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_notice_level_of(notice: *const Notice) -> NoticeLevel {
    // SAFETY: the header requires a live notice.
    unsafe { abi::accessor(notice) }.level()
}

/// # Safety
///
/// `notice` is a live notice this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_notice_kind_of(notice: *const Notice) -> NoticeKind {
    // SAFETY: the header requires a live notice.
    unsafe { abi::accessor(notice) }.kind()
}

/// # Safety
///
/// `notice` is a live notice this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_notice_message(
    notice: *const Notice,
    message: *mut *const u8,
    message_len: *mut usize,
) {
    // SAFETY: the header requires a live notice and writable out-parameters.
    unsafe {
        let notice = abi::accessor(notice);
        abi::write_bytes(message, message_len, notice.message().as_bytes());
    }
}

/// # Safety
///
/// `notice` is a live notice this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_notice_error(
    notice: *const Notice,
    error_class: *mut *const u8,
    error_class_len: *mut usize,
    node_kind: *mut *const u8,
    node_kind_len: *mut usize,
    node: *mut *const u8,
    node_len: *mut usize,
) -> bool {
    // SAFETY: the header requires a live notice.
    let notice = unsafe { abi::accessor(notice) };
    // The shared notice reads the three together: a notice that reports a classified error has
    // all of them, and any other notice has none, which writes nothing.
    let Some(class) = notice.error_class() else {
        return false;
    };
    let Some(kind) = notice.error_node_kind() else {
        return false;
    };
    let Some(name) = notice.error_node() else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters, and the names live as long as the
    // notice the host holds.
    unsafe {
        abi::write_bytes(error_class, error_class_len, class.as_bytes());
        abi::write_bytes(node_kind, node_kind_len, kind.as_bytes());
        abi::write_bytes(node, node_len, name.as_bytes());
    }
    true
}

/// # Safety
///
/// `notice` is a live reference to a notice this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_notice_retain(notice: *mut Notice) -> *mut Notice {
    // SAFETY: the header requires a live reference, which `into_shared` or this function made
    // from an `Arc`. Wrapping it in `ManuallyDrop` leaves the caller's reference in place.
    let shared = ManuallyDrop::new(unsafe { Arc::from_raw(notice.cast_const()) });
    Arc::into_raw(Arc::clone(&shared)).cast_mut()
}

/// # Safety
///
/// A non-null `notice` is a reference this library returned that has not been released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_notice_release(notice: *mut Notice) {
    if notice.is_null() {
        return;
    }
    // SAFETY: the header requires an unreleased reference, which `into_shared` or
    // `nx_notice_retain` made from an `Arc`.
    drop(unsafe { Arc::from_raw(notice.cast_const()) });
}
