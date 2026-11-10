//! Committed acceptance limits for entity metadata.
//!
//! Layer: vocabulary.
//! - **Owns.** The four label policy declarations and their single defaults.
//! - **Depends on.** Setting definitions and typed parameter validation.
//! - **Must not know.** Label storage, parsing, consensus or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns metadata policy declarations")
)]

use super::{ClusterParameterError, ClusterParameterKey};
use crate::{ClusterParameters, SettingDefinition};

macro_rules! label_limit {
    ($definition:ident, $name:literal, $default:literal) => {
        pub struct $definition;
        impl SettingDefinition for $definition {
            type Class = ClusterParameters;
            type Value = u64;
            type Context = ();
            type Error = ClusterParameterError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;
            fn default_value(_: &()) -> u64 {
                $default
            }
            fn validate(value: &u64, _: &()) -> error_stack::Result<(), ClusterParameterError> {
                ClusterParameterKey::$definition.validate_count(*value, 0, u64::MAX)
            }
        }
    };
}

label_limit!(EntityLabelsMaxCount, "entity-labels-max-count", 64);
label_limit!(EntityLabelsMaxKeyBytes, "entity-labels-max-key-bytes", 128);
label_limit!(
    EntityLabelsMaxValueBytes,
    "entity-labels-max-value-bytes",
    1024
);
label_limit!(EntityLabelsMaxBytes, "entity-labels-max-bytes", 16384);
