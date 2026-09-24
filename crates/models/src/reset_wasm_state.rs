//! A requested replacement of a WASM processor's durable guest state.
//!
//! Layer: vocabulary.
//! - **Owns.** The explicit domain, processor, and branch selection of one reset command.
//! - **Depends on.** Domain and processor names, schema field names, and scalar literals.
//! - **Must not know.** Parsing, branch instances, coordination, or checkpoint storage.

use std::collections::{BTreeMap, BTreeSet};

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use serde_json::{Number as JsonNumber, Value as JsonValue};
use thiserror::Error;

use crate::{
    BranchKeyFingerprint, DomainName, FieldName, Literal, ParseAsType, RemoteRuntimeField,
    RemoteRuntimeValue, ResolvedBranching, WasmProcessorName, WasmStateResetScope,
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ResetWasmState {
    pub domain: DomainName,
    pub processor: WasmProcessorName,
    pub scope: ResetWasmStateScope,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ResetWasmStateScope {
    Unbranched,
    AllBranches,
    Branch(Vec<ResetWasmBranchField>),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ResetWasmBranchField {
    pub name: FieldName,
    pub value: Literal,
}

/// A branch selection resolved against the exact scheduled schema.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedResetWasmStateScope {
    Unbranched,
    AllBranches,
    Branch {
        fingerprint: BranchKeyFingerprint,
        fields: Vec<RemoteRuntimeField>,
    },
}

impl ResolvedResetWasmStateScope {
    pub fn scope(&self) -> WasmStateResetScope {
        match self {
            Self::Unbranched => WasmStateResetScope::Unbranched,
            Self::AllBranches => WasmStateResetScope::AllBranches,
            Self::Branch { fingerprint, .. } => WasmStateResetScope::Branch(*fingerprint),
        }
    }
}

#[derive(Debug, Error)]
pub enum ResetWasmStateSelectionError {
    #[error("reset scope does not match the WASM processor's branch declaration")]
    InvalidScope,
    #[error("reset branch fields do not match the declared branch schema")]
    InvalidFields,
    #[error("reset branch field '{field}' requires an exact {expected} literal")]
    InvalidFieldType {
        field: FieldName,
        expected: ParseAsType,
    },
    #[error("reset branch field '{field}' has a non-finite floating-point value")]
    NonFiniteFloat { field: FieldName },
}

impl ResetWasmStateScope {
    /// Resolve a command's written branch values before any durable or runtime reset effect.
    /// The selected fingerprint follows the same canonical JSON key encoding as runtime branch
    /// keys, while the field values themselves stay out of impact reports.
    pub fn resolve(
        &self,
        branching: &ResolvedBranching,
    ) -> error_stack::Result<ResolvedResetWasmStateScope, ResetWasmStateSelectionError> {
        match (self, branching) {
            (Self::Unbranched, ResolvedBranching::Unbranched) => {
                Ok(ResolvedResetWasmStateScope::Unbranched)
            }
            (Self::AllBranches, ResolvedBranching::Branched { schema, .. })
                if !schema.fields.is_empty() =>
            {
                Ok(ResolvedResetWasmStateScope::AllBranches)
            }
            (Self::Branch(fields), ResolvedBranching::Branched { schema, .. })
                if !schema.fields.is_empty() =>
            {
                let declared_fields = schema
                    .fields
                    .iter()
                    .map(|field| (&field.name, &field.ty))
                    .collect::<BTreeMap<_, _>>();
                let supplied = fields
                    .iter()
                    .map(|field| &field.name)
                    .collect::<BTreeSet<_>>();
                if fields.len() != supplied.len()
                    || supplied != declared_fields.keys().copied().collect()
                {
                    return Err(Report::new(ResetWasmStateSelectionError::InvalidFields));
                }
                let mut canonical = BTreeMap::new();
                let mut branch_key = Vec::with_capacity(fields.len());
                for field in fields {
                    let declared_type = declared_fields
                        .get(&field.name)
                        .ok_or_else(|| Report::new(ResetWasmStateSelectionError::InvalidFields))?;
                    let (value, json) = field.resolve_value(declared_type)?;
                    canonical.insert(field.name.as_str().to_string(), json);
                    branch_key.push(RemoteRuntimeField {
                        name: field.name.as_str().to_string(),
                        value,
                    });
                }
                let canonical = serde_json::to_string(&canonical)
                    .map_err(|_| Report::new(ResetWasmStateSelectionError::InvalidFields))?;
                Ok(ResolvedResetWasmStateScope::Branch {
                    fingerprint: BranchKeyFingerprint::of_canonical_text(&canonical),
                    fields: branch_key,
                })
            }
            _ => Err(Report::new(ResetWasmStateSelectionError::InvalidScope)),
        }
    }
}

impl ResetWasmBranchField {
    fn resolve_value(
        &self,
        expected: &ParseAsType,
    ) -> error_stack::Result<(RemoteRuntimeValue, JsonValue), ResetWasmStateSelectionError> {
        let mismatch = || ResetWasmStateSelectionError::InvalidFieldType {
            field: self.name.clone(),
            expected: expected.clone(),
        };
        match (&self.value, expected) {
            (Literal::String(value), ParseAsType::String) => Ok((
                RemoteRuntimeValue::String(value.clone()),
                JsonValue::String(value.clone()),
            )),
            (Literal::Bool(value), ParseAsType::Bool) => {
                Ok((RemoteRuntimeValue::Bool(*value), JsonValue::Bool(*value)))
            }
            (Literal::I64(value), ParseAsType::I64) => Ok((
                RemoteRuntimeValue::I64(*value),
                JsonValue::Number(JsonNumber::from(*value)),
            )),
            (Literal::F64(value), ParseAsType::F64) => {
                let number = JsonNumber::from_f64(value.value()).ok_or_else(|| {
                    Report::new(ResetWasmStateSelectionError::NonFiniteFloat {
                        field: self.name.clone(),
                    })
                })?;
                Ok((
                    RemoteRuntimeValue::F64(value.value()),
                    JsonValue::Number(number),
                ))
            }
            _ => Err(Report::new(mismatch())),
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;
    use crate::{BranchName, CreateSchema, Float64Literal, SchemaField, SchemaName};

    fn tenant_branch() -> ResolvedBranching {
        ResolvedBranching::branched(
            BranchName::parse("by_tenant")
                .assured("the test name satisfies the branch name grammar"),
            CreateSchema {
                name: SchemaName::parse("tenant_key")
                    .assured("the test name satisfies the schema name grammar"),
                fields: vec![SchemaField {
                    name: FieldName::parse("tenant")
                        .assured("the test name satisfies the field name grammar"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                }],
            },
        )
    }

    #[test]
    fn selected_branch_resolves_to_the_runtime_key_fingerprint() {
        let selection = ResetWasmStateScope::Branch(vec![ResetWasmBranchField {
            name: FieldName::parse("tenant")
                .assured("the test name satisfies the field name grammar"),
            value: Literal::String("alpha".to_string()),
        }]);
        let resolved = selection
            .resolve(&tenant_branch())
            .assured("the supplied string matches the sole declared branch key field");
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix/branch-key");
        hasher.update(br#"{"tenant":"alpha"}"#);
        assert_eq!(
            resolved.scope(),
            WasmStateResetScope::Branch(BranchKeyFingerprint::new(*hasher.finalize().as_bytes()))
        );
        assert!(matches!(
            resolved,
            ResolvedResetWasmStateScope::Branch { fields, .. } if fields == vec![RemoteRuntimeField {
                name: "tenant".to_string(),
                value: RemoteRuntimeValue::String("alpha".to_string()),
            }]
        ));
    }

    #[test]
    fn selection_requires_exact_scope_fields_and_types() {
        let tenant =
            FieldName::parse("tenant").assured("the test name satisfies the field name grammar");
        assert!(
            ResetWasmStateScope::Unbranched
                .resolve(&tenant_branch())
                .is_err()
        );
        assert!(
            ResetWasmStateScope::AllBranches
                .resolve(&ResolvedBranching::Unbranched)
                .is_err()
        );
        assert!(
            ResetWasmStateScope::Branch(vec![ResetWasmBranchField {
                name: tenant.clone(),
                value: Literal::I64(7),
            }])
            .resolve(&tenant_branch())
            .is_err()
        );
        assert!(
            ResetWasmStateScope::Branch(vec![
                ResetWasmBranchField {
                    name: tenant.clone(),
                    value: Literal::String("alpha".to_string()),
                },
                ResetWasmBranchField {
                    name: tenant,
                    value: Literal::String("beta".to_string()),
                },
            ])
            .resolve(&tenant_branch())
            .is_err()
        );
    }

    #[test]
    fn selected_scalar_values_resolve_to_their_exact_branch_types() {
        let cases = [
            (
                Literal::Bool(true),
                ParseAsType::Bool,
                RemoteRuntimeValue::Bool(true),
            ),
            (
                Literal::I64(-7),
                ParseAsType::I64,
                RemoteRuntimeValue::I64(-7),
            ),
            (
                Literal::F64(Float64Literal::new(1.25)),
                ParseAsType::F64,
                RemoteRuntimeValue::F64(1.25),
            ),
        ];
        for (literal, ty, expected) in cases {
            let mut branching = tenant_branch();
            let ResolvedBranching::Branched { schema, .. } = &mut branching else {
                panic!("the fixture declares a concrete branch schema");
            };
            schema.fields[0].ty = ty;
            let selected = ResetWasmStateScope::Branch(vec![ResetWasmBranchField {
                name: FieldName::parse("tenant")
                    .assured("the test name satisfies the field name grammar"),
                value: literal,
            }])
            .resolve(&branching)
            .assured("the literal exactly matches the declared field type");
            let ResolvedResetWasmStateScope::Branch { fields, .. } = selected else {
                panic!("the selected scope remains a concrete branch");
            };
            assert_eq!(fields[0].value, expected);
        }
    }

    #[test]
    fn non_finite_floating_branch_values_are_invalid() {
        let mut branching = tenant_branch();
        let ResolvedBranching::Branched { schema, .. } = &mut branching else {
            panic!("the fixture declares a concrete branch schema");
        };
        schema.fields[0].ty = ParseAsType::F64;
        let selection = ResetWasmStateScope::Branch(vec![ResetWasmBranchField {
            name: FieldName::parse("tenant")
                .assured("the test name satisfies the field name grammar"),
            value: Literal::F64(Float64Literal::new(f64::INFINITY)),
        }]);
        let error = selection
            .resolve(&branching)
            .expect_err("a non-finite value has no canonical JSON branch key");
        assert!(matches!(
            error.current_context(),
            ResetWasmStateSelectionError::NonFiniteFloat { .. }
        ));
    }
}
