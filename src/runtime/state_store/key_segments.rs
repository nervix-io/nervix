//! The segment vocabulary of a stored runtime-state key: the byte that opens each of its
//! generation, model, schema and unit segments, and the bytes of its unit segment.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The markers that open a stored state key's segments and the unit segment's encoding.
//! - **Depends on.** Vocabulary units.
//! - **Must not know.** Keyspaces, generations, checkpoints or how a placement is resolved.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use nervix_models::Unit;

/// The byte that opens the generation segment of a WASM guest state key. A key without it was not
/// written in the current shape and fails to decode instead of addressing the current lifetime.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the stored WASM guest state key format opens its generation segment with g"
    )
)]
pub(super) const STATE_GENERATION_KEY_MARKER: u8 = b'g';

/// The byte that opens the pinned model fingerprint segment of an inferencer state key.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the stored inferencer key format prefixes its model with m"
    )
)]
pub(super) const STATE_MODEL_KEY_MARKER: u8 = b'm';

/// The byte that opens the schema fingerprint segment of every schema-bound state key.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the stored state key format opens a schema-bound key's fingerprint segment with \
                  s"
    )
)]
pub(super) const STATE_SCHEMA_KEY_MARKER: u8 = b's';
/// The current key's unit segment. A missing marker is an invalid stored shape.
#[cfg_attr(
    nervix_lint,
    nervix::constant(fixed, reason = "unit identity is part of the stored key format")
)]
pub(super) const STATE_UNIT_KEY_MARKER: u8 = b'U';

pub(super) fn append_unit_key(key: &mut Vec<u8>, unit: Unit) {
    key.push(0);
    key.push(STATE_UNIT_KEY_MARKER);
    match unit.kind() {
        nervix_models::UnitKind::Single => {
            key.push(0);
            key.extend_from_slice(&0_u32.to_be_bytes());
        }
        nervix_models::UnitKind::Partition(partition) => {
            key.push(1);
            key.extend_from_slice(&partition.to_be_bytes());
        }
        nervix_models::UnitKind::Instance(instance) => {
            key.push(2);
            key.extend_from_slice(&instance.to_be_bytes());
        }
    }
}
