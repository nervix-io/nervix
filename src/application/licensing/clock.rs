//! The licensing coordinator's sole observation of physical UTC.
//!
//! Layer: control plane.
//! - **Owns.** Physical observations proposed as monotone licensing time and test clock offsets.
//! - **Depends on.** UTC, timestamps and the configured test fault handle.
//! - **Must not know.** Policy derivation, signed document validation or graph execution.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::Timestamp;

pub(super) fn observe(fault: &crate::ConfiguredFaultInjection) -> Timestamp {
    let physical = chrono::Utc::now();
    let offset = chrono::TimeDelta::from_std(fault.licensing_clock_offset())
        .assured("the configured test offset fits UTC delta");
    Timestamp::try_from(
        physical
            .checked_add_signed(offset)
            .verified("the configured licensing clock offset fits UTC"),
    )
    .assured("UTC observation fits the vocabulary's current timestamp coordinate")
}

pub(super) fn request_id(fault: &crate::ConfiguredFaultInjection) -> uuid::Uuid {
    let now = observe(fault);
    let millis = u64::try_from(now.unix_millis())
        .assured("current licensing observation follows Unix epoch");
    uuid::Builder::from_unix_timestamp_millis(millis, &nervix_crypto::random::bytes::<10>())
        .into_uuid()
}
