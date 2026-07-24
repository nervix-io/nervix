use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumString, IntoStaticStr};

use crate::{Identifier, ParseAsType};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsRefStr, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE", ascii_case_insensitive)]
pub enum UdfLanguage {
    #[strum(serialize = "ROTO_0_11")]
    Roto0_11,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UdfArgument {
    pub name: Identifier,
    pub ty: ParseAsType,
    #[serde(default)]
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UdfReturn {
    pub ty: ParseAsType,
    #[serde(default)]
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateUdf {
    pub name: Identifier,
    pub language: UdfLanguage,
    pub arguments: Vec<UdfArgument>,
    pub returns: UdfReturn,
    #[serde(default)]
    pub volatile: bool,
    pub code: String,
    pub code_hash: String,
}

impl CreateUdf {
    pub fn new(
        name: Identifier,
        language: UdfLanguage,
        arguments: Vec<UdfArgument>,
        returns: UdfReturn,
        volatile: bool,
        code: String,
    ) -> Self {
        let code_hash = blake3::hash(code.as_bytes()).to_hex().to_string();
        Self {
            name,
            language,
            arguments,
            returns,
            volatile,
            code,
            code_hash,
        }
    }

    pub fn has_valid_code_hash(&self) -> bool {
        self.code_hash == blake3::hash(self.code.as_bytes()).to_hex().as_str()
    }
}
