//! The vocabulary's own representations keep every value they carry.
//!
//! Layer: test harness.
//!
//! - **Owns.** The Bolero properties over the vocabulary's text, serde and archived forms: names,
//!   timestamps, domain-clock values, JSON paths, batch limits, identities, and whole Models and
//!   statements as they are archived. Each form has a round-trip property over its valid domain
//!   and, where its input can be malformed, a separate property over arbitrary input.
//! - **Depends on.** The vocabulary and the generators of `nervix-arbitrary`.
//! - **Must not know.** The language, persistence owners above the vocabulary, or runtime state.

mod batch_limits;
mod domain_clock;
mod identities;
mod json_paths;
mod models;
mod names;
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
