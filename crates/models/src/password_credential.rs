//! Bounded, secret-bearing password credentials.
//!
//! Layer: vocabulary.
//! - **Owns.** The current password profile and validated stored derivation cost.
//! - **Depends on.** Setting validation, fixed byte arrays and serialization.
//! - **Must not know.** Cryptographic operations, authentication, executors or storage engines.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns bounded credential values")
)]

use std::{fmt, num::NonZeroU32};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

use crate::{
    ClusterParameterError, PasswordIterations, Setting,
    settings::archive::{self, ValidatedSetting},
};

/// A positive iteration count within the cluster's declared verification bound.
///
/// ```
/// use nervix_models::PasswordCost;
/// let cost = PasswordCost::try_from(210_000_u64)?;
/// assert_eq!(cost.get().get(), 210_000);
/// # Ok::<(), error_stack::Report<nervix_models::ClusterParameterError>>(())
/// ```
///
/// A caller cannot bypass validation by constructing the stored count directly:
///
/// ```compile_fail,E0423
/// use nervix_models::PasswordCost;
/// let cost = PasswordCost(0);
/// ```
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct PasswordCost(
    #[serde(with = "archive")]
    #[rkyv(with = ValidatedSetting)]
    Setting<PasswordIterations>,
);

impl TryFrom<u64> for PasswordCost {
    type Error = Report<ClusterParameterError>;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Ok(Self(Setting::new(value, &())?))
    }
}

impl PasswordCost {
    pub fn get(self) -> NonZeroU32 {
        let count =
            u32::try_from(*self.0.value()).assured("the declared password cost bound fits u32");
        NonZeroU32::new(count).assured("the password cost definition refuses zero")
    }
}

/// A complete current credential. Debug deliberately excludes salt and digest.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
#[serde(deny_unknown_fields)]
pub enum PasswordCredential {
    Pbkdf2HmacSha512 {
        cost: PasswordCost,
        salt: [u8; 16],
        #[serde(with = "BigArray")]
        digest: [u8; 64],
    },
}

impl PasswordCredential {
    pub fn from_parts(
        iterations: u64,
        salt: [u8; 16],
        digest: [u8; 64],
    ) -> Result<Self, Report<ClusterParameterError>> {
        Ok(Self::Pbkdf2HmacSha512 {
            cost: PasswordCost::try_from(iterations)?,
            salt,
            digest,
        })
    }
    pub fn cost(&self) -> PasswordCost {
        match self {
            Self::Pbkdf2HmacSha512 { cost, .. } => *cost,
        }
    }
    pub fn salt(&self) -> &[u8; 16] {
        match self {
            Self::Pbkdf2HmacSha512 { salt, .. } => salt,
        }
    }
    pub fn digest(&self) -> &[u8; 64] {
        match self {
            Self::Pbkdf2HmacSha512 { digest, .. } => digest,
        }
    }
}

impl fmt::Debug for PasswordCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Pbkdf2HmacSha512")
            .field("iterations", &self.cost())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
