use serde::{Deserialize, Serialize};
use sorted_vec::SortedSet;
use strum::{AsRefStr, EnumString};
use uuid::Uuid;

use crate::Timestamp;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FieldPath(String);

impl FieldPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AsRefStr, EnumString)]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
#[serde(rename_all = "snake_case")]
pub enum MessageErrorCode {
    Evaluation,
    Validation,
    External,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AsRefStr, EnumString)]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
#[serde(rename_all = "snake_case")]
pub enum MessageErrorOperation {
    SourceWhere,
    FilterWhere,
    Inferencer,
    Wasm,
    Correlate,
    Deduplicate,
    Reorder,
    Window,
    Inherit,
    Set,
    Finalize,
    RouteWhere,
    BranchSet,
    Values,
    Invoke,
    Encode,
    Flush,
    Commit,
    Publish,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredMessageError {
    pub reference: Uuid,
    pub code: MessageErrorCode,
    pub message: String,
    pub operation: MessageErrorOperation,
    pub operation_index: Option<u32>,
    pub fields: SortedSet<FieldPath>,
    pub occurred_at: Timestamp,
}
