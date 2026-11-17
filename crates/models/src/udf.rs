//! Semantic domain entity configuration and alterations.
//!
//! Layer: vocabulary.
//! - **Owns.** Typed entity declarations and their network-free local invariants.
//! - **Depends on.** Vocabulary names, metadata and serialization.
//! - **Must not know.** Parsing, registry state or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns entity configuration vocabulary")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumString, IntoStaticStr};

use crate::{FieldName, ParseAsType, UdfName};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    AsRefStr,
    EnumString,
    IntoStaticStr,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE", ascii_case_insensitive)]
pub enum UdfLanguage {
    #[strum(serialize = "ROTO_0_13")]
    Roto0_13,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct UdfArgument {
    pub name: FieldName,
    pub ty: ParseAsType,
    #[serde(default)]
    pub optional: bool,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct UdfReturn {
    pub ty: ParseAsType,
    #[serde(default)]
    pub optional: bool,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateUdf {
    pub labels: crate::EntityLabels,
    pub name: UdfName,
    pub language: UdfLanguage,
    pub arguments: Vec<UdfArgument>,
    pub returns: UdfReturn,
    #[serde(default)]
    pub volatile: bool,
    pub code: String,
    pub code_hash: String,
}

impl CreateUdf {
    /// Non-security BLAKE3: identifies UDF source; declaration/compilation policy owns execution admission.
    pub fn new(
        name: UdfName,
        language: UdfLanguage,
        arguments: Vec<UdfArgument>,
        returns: UdfReturn,
        volatile: bool,
        code: String,
    ) -> Self {
        let code_hash = blake3::hash(code.as_bytes()).to_hex().to_string();
        Self {
            labels: crate::EntityLabels::default(),
            name,
            language,
            arguments,
            returns,
            volatile,
            code,
            code_hash,
        }
    }

    /// Non-security BLAKE3: checks stored source/hash consistency, not a code signature.
    pub fn has_valid_code_hash(&self) -> bool {
        self.code_hash == blake3::hash(self.code.as_bytes()).to_hex().as_str()
    }
}
