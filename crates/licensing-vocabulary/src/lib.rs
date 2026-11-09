//! Shared licensing quota values.
//!
//! Layer: vocabulary.
//! - **Owns.** The bounded and unlimited quota value and its arithmetic.
//! - **Depends on.** Serialization and primitive value types.
//! - **Must not know.** Policies, cryptography, Models, consensus or network I/O.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns shared licensing words")
)]

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Quota {
    Bounded { value: u64 },
    Unlimited,
}

impl Quota {
    pub const fn admits(self, usage: u64) -> bool {
        match self {
            Self::Unlimited => true,
            Self::Bounded { value } => usage <= value,
        }
    }

    pub const fn is_reduction_from(self, previous: Self) -> bool {
        match (previous, self) {
            (Self::Unlimited, Self::Bounded { .. }) => true,
            (Self::Bounded { value: previous }, Self::Bounded { value }) => value < previous,
            _ => false,
        }
    }
}
