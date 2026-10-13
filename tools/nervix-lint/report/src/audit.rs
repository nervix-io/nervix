//! Generated review candidates for code shaped around an inaccurate lint.
//!
//! A candidate names a resolved pattern and the evidence that located it. It is neither a
//! diagnostic nor an approval: review decides whether the pattern is harmful, intentional or
//! unrelated, and records that decision with the task that owns the audit.

use serde::{Deserialize, Serialize};

use crate::{Context, Expansion, SourceSpan};

/// One resolved pattern inside one authored body.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuditCandidate {
    /// The definition owning the body, closures included.
    pub owner: String,
    /// The owner's declared execution contract, or recurring reachability from one.
    #[serde(deserialize_with = "Option::deserialize")]
    pub context: Option<Context>,
    pub span: SourceSpan,
    pub expansion: Vec<Expansion>,
    pub pattern: AuditPattern,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuditPattern {
    /// One collection place is reached more than once with one key value, and a later operation
    /// can run after an earlier one in the same execution.
    RepeatedLookup {
        collection: String,
        receiver: String,
        key: String,
        /// A later operation runs in a branch selected by an earlier operation's result.
        guarded: bool,
        operations: Vec<KeyedOperation>,
    },
    /// A lookup builds an owned key for a call that also accepts the borrowed form, or an entry
    /// builds one that a hit discards.
    KeyAllocation {
        collection: String,
        operation: String,
        allocation: String,
        key_type: String,
    },
    /// An owned copy exists only to be borrowed.
    BorrowedCopy {
        operation: String,
        value_type: String,
    },
    /// Every branch of one exhaustive conditional repeats the same statement.
    DuplicatedBranch {
        construct: Construct,
        statement: String,
        occurrences: Vec<SourceSpan>,
    },
    /// A function's whole body passes its parameters unchanged to one local call.
    ForwardingWrapper {
        callee: String,
        callers: u32,
        /// The wrapper or its callee carries a Nervix source contract.
        contract: bool,
        /// The wrapper implements a trait method.
        implementation: bool,
        /// The callee is defined in another file than the wrapper.
        relocated: bool,
    },
    /// A failure is replaced, defaulted or tested without propagating its value.
    DiscardedError {
        operation: String,
        error_type: String,
        information: ErrorInformation,
    },
    /// Checked arithmetic or a checked conversion falls back to a value instead of deciding
    /// what overflow means.
    DisguisedClamp { operation: String, fallback: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KeyedOperation {
    pub operation: String,
    pub span: SourceSpan,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Construct {
    If,
    Match,
}

/// What the dropped failure carried. Zero-sized and uninhabited failures carry nothing and are
/// not candidates.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorInformation {
    /// An `error-stack` report with its frames and attachments.
    Report,
    /// A sized failure value.
    Value,
    /// A generic failure whose layout this configuration does not determine.
    Generic,
}

/// How a resolved keyed-collection operation reaches its key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyedAccess {
    /// Reads or tests a key through a borrowed form.
    Lookup,
    /// Removes a key through a borrowed form.
    Removal,
    /// Stores an owned key.
    Insertion,
    /// Positions on an owned key; a hit discards it.
    Entry,
}

impl KeyedAccess {
    /// The operation accepts the collection's borrowed key form.
    pub fn borrows_key(self) -> bool {
        matches!(self, Self::Lookup | Self::Removal)
    }
}

/// Finite keyed-collection APIs, identified by the resolved owner of the called method.
pub fn keyed_access(collection: &str, operation: &str) -> Option<KeyedAccess> {
    if !matches!(
        collection,
        "std::collections::hash::map::HashMap"
            | "std::collections::hash::set::HashSet"
            | "alloc::collections::btree::map::BTreeMap"
            | "alloc::collections::btree::set::BTreeSet"
            | "hashbrown::map::HashMap"
            | "hashbrown::set::HashSet"
            | "indexmap::map::IndexMap"
            | "indexmap::set::IndexSet"
            | "dashmap::DashMap"
            | "dashmap::set::DashSet"
            | "shuttle_dashmap_impl::DashMap"
            | "serde_json::map::Map"
    ) {
        return None;
    }
    match operation {
        "get" | "get_mut" | "get_key_value" | "contains_key" | "contains" | "get_full"
        | "get_full_mut" | "get_index_of" | "index" => Some(KeyedAccess::Lookup),
        "remove" | "remove_entry" | "swap_remove" | "swap_remove_entry" | "swap_remove_full"
        | "shift_remove" | "shift_remove_entry" | "shift_remove_full" | "take" => {
            Some(KeyedAccess::Removal)
        }
        "insert" | "try_insert" | "replace" => Some(KeyedAccess::Insertion),
        "entry" => Some(KeyedAccess::Entry),
        _ => None,
    }
}

/// Conversions that preserve a key's value while possibly changing its ownership or view.
pub fn preserves_value(operation: &str) -> bool {
    matches!(
        operation,
        "clone"
            | "to_owned"
            | "to_string"
            | "to_vec"
            | "to_path_buf"
            | "into_owned"
            | "into"
            | "as_str"
            | "as_slice"
            | "as_bytes"
            | "as_path"
            | "as_os_str"
            | "as_ref"
            | "as_deref"
            | "borrow"
            | "deref"
            | "copied"
            | "cloned"
    )
}

/// Conversions that build an owned value from a borrowed one.
pub fn copies_value(operation: &str) -> bool {
    matches!(
        operation,
        "clone" | "to_owned" | "to_string" | "to_vec" | "to_path_buf" | "into_owned" | "into"
    )
}

/// Primitive integer and Duration operations that report overflow as absence.
pub fn checked_operation(operation: &str) -> bool {
    operation.starts_with("checked_")
}

/// Option and Result fallbacks that replace absence or failure with a value.
pub fn fallback(operation: &str) -> bool {
    matches!(
        operation,
        "unwrap_or" | "unwrap_or_default" | "unwrap_or_else"
    )
}
