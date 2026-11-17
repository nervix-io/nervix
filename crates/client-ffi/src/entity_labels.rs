//! C access to an independently retained entity metadata view.
//!
//! Layer: edges.
//! - **Owns.** Metadata handles, exact borrowed bytes and explicit optional view fields.
//! - **Depends on.** The shared host snapshot and checked C ABI helpers.
//! - **Must not know.** Display parsing, registry state or execution behavior.

#![cfg_attr(nervix_lint, nervix::layer(edge, reason = "owns C metadata handles"))]

use nervix_client_host::{
    EntityLabelViewKind, EntityLabelsInspection, Failure, FailureKind, Outcome,
};

use crate::abi;

/// # Safety
/// `outcome` is live and `out` is writable. The result is independently owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_entity_labels(
    outcome: *const Outcome,
    out: *mut *mut EntityLabelsInspection,
) -> *mut Failure {
    abi::outcome((|| unsafe {
        abi::require_out(out, "out")?;
        let labels = abi::handle(outcome, "outcome")?
            .entity_labels()
            .ok_or_else(|| {
                Failure::new(
                    FailureKind::InvalidArgument,
                    "the command did not inspect entity labels",
                )
            })?;
        abi::write(out, abi::into_handle(labels));
        Ok(())
    })())
}

/// # Safety
/// `labels` transfers its independently owned live handle, or is null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_entity_labels_free(labels: *mut EntityLabelsInspection) {
    // SAFETY: the caller transfers this handle's ownership.
    unsafe {
        abi::release(labels);
    }
}

macro_rules! text_getter {
    ($name:ident, $getter:ident) => {
        /// # Safety
        /// The handle is live and non-null output pointers are writable. Bytes borrow the handle.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $name(
            labels: *const EntityLabelsInspection,
            data: *mut *const u8,
            len: *mut usize,
        ) {
            // SAFETY: the caller guarantees a live handle and writable output pointers.
            unsafe {
                abi::write_bytes(data, len, abi::accessor(labels).$getter().as_bytes());
            }
        }
    };
}
text_getter!(nx_entity_labels_domain, domain);
text_getter!(nx_entity_labels_entity_kind, entity_kind);
text_getter!(nx_entity_labels_entity_name, entity_name);

/// # Safety
/// `labels` is a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_entity_labels_view(
    labels: *const EntityLabelsInspection,
) -> EntityLabelViewKind {
    // SAFETY: the caller guarantees the handle's lifetime.
    unsafe { abi::accessor(labels).view() }
}

macro_rules! optional_count {
    ($name:ident, $getter:ident) => {
        /// # Safety
        /// The handle is live and non-null output pointers are writable.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $name(
            labels: *const EntityLabelsInspection,
            out: *mut u64,
        ) -> bool {
            // SAFETY: the caller guarantees the handle's lifetime.
            let Some(value) = (unsafe { abi::accessor(labels) }).$getter() else {
                return false;
            };
            // SAFETY: the caller guarantees a writable output pointer when non-null.
            unsafe {
                abi::write(out, value);
            }
            true
        }
    };
}
optional_count!(nx_entity_labels_revision, revision);
optional_count!(nx_entity_labels_position, position);

/// # Safety
/// The handle is live and non-null output pointers are writable. Bytes borrow the handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_entity_labels_transaction_id(
    labels: *const EntityLabelsInspection,
    data: *mut *const u8,
    len: *mut usize,
) -> bool {
    // SAFETY: the caller guarantees the handle's lifetime.
    let Some(value) = (unsafe { abi::accessor(labels) }).transaction_id() else {
        return false;
    };
    // SAFETY: the caller guarantees writable non-null output pointers.
    unsafe {
        abi::write_bytes(data, len, value.as_bytes());
    }
    true
}

/// # Safety
/// The handle is live and non-null output pointers are writable. Bytes borrow the handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_entity_labels_planning_basis(
    labels: *const EntityLabelsInspection,
    data: *mut *const u8,
    len: *mut usize,
) -> bool {
    // SAFETY: the caller guarantees the handle's lifetime.
    let Some(value) = (unsafe { abi::accessor(labels) }).planning_basis() else {
        return false;
    };
    // SAFETY: the caller guarantees writable non-null output pointers.
    unsafe {
        abi::write_bytes(data, len, value);
    }
    true
}

/// # Safety
/// `labels` is a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_entity_labels_policy_revision(
    labels: *const EntityLabelsInspection,
) -> u64 {
    // SAFETY: the caller guarantees the handle's lifetime.
    unsafe { abi::accessor(labels).policy_revision() }
}

/// # Safety
/// The handle is live and non-null output pointers are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_entity_labels_limits(
    labels: *const EntityLabelsInspection,
    count: *mut u64,
    key_bytes: *mut u64,
    value_bytes: *mut u64,
    bytes: *mut u64,
) {
    // SAFETY: the caller guarantees the handle and output pointers' lifetimes.
    unsafe {
        let labels = abi::accessor(labels);
        abi::write(count, labels.max_count());
        abi::write(key_bytes, labels.max_key_bytes());
        abi::write(value_bytes, labels.max_value_bytes());
        abi::write(bytes, labels.max_bytes());
    }
}

/// # Safety
/// `labels` is a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_entity_labels_count(labels: *const EntityLabelsInspection) -> usize {
    // SAFETY: the caller guarantees the handle's lifetime.
    unsafe { abi::accessor(labels).count() }
}

/// # Safety
/// The handle is live and output pointers are writable. Bytes borrow the handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_entity_labels_entry(
    labels: *const EntityLabelsInspection,
    index: usize,
    key: *mut *const u8,
    key_len: *mut usize,
    value: *mut *const u8,
    value_len: *mut usize,
) -> *mut Failure {
    abi::outcome((|| unsafe {
        abi::require_out(key, "key")?;
        abi::require_out(key_len, "key_len")?;
        abi::require_out(value, "value")?;
        abi::require_out(value_len, "value_len")?;
        let (entry_key, entry_value) = abi::handle(labels, "labels")?.entry(index)?;
        abi::write_bytes(key, key_len, entry_key.as_bytes());
        abi::write_bytes(value, value_len, entry_value.as_bytes());
        Ok(())
    })())
}
