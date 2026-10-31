//! The vocabulary's own representations keep every value they carry.
//!
//! Layer: test harness.
//!
//! - **Owns.** The Bolero properties over the vocabulary's text, serde and archived forms: names,
//!   external names, timestamps, domain-clock values, JSON paths, batch limits, SQS receive
//!   windows, identities, node prerequisite records, emitter envelopes, and whole Models and
//!   statements as they are archived.
//!   Each form has a round-trip property over its valid domain and, where its input can be
//!   malformed, a separate property over arbitrary input.
//! - **Depends on.** The vocabulary and the generators of `nervix-arbitrary`.
//! - **Must not know.** The language, persistence owners above the vocabulary, or runtime state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

mod batch_limits;
mod domain_clock;
mod envelopes;
mod external_names;
mod identities;
mod json_paths;
mod models;
mod names;
mod prerequisites;
mod sqs_receive_windows;
mod timestamps;

/// Archives `value` and reads it back, as every archived form in the vocabulary is read.
#[macro_export]
macro_rules! archive_round_trip {
    ($value:expr, $Type:ty) => {{
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>($value)
            .unwrap_or_else(|error| panic!("{:?} must archive: {error}", $value));
        rkyv::from_bytes::<$Type, rkyv::rancor::Error>(&bytes)
            .unwrap_or_else(|error| panic!("{:?} must read back: {error}", $value))
    }};
}
