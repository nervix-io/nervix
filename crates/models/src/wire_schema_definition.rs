//! Metadata-free wire schema inputs to codec engines.
//!
//! Layer: vocabulary.
//! - **Owns.** Borrowed and owned schema definitions after entity metadata is discarded.
//! - **Depends on.** Native field types, wire schema strictness and existing schema names.
//! - **Must not know.** Parsing, registry validation, labels, placement or execution tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns typed codec schema inputs")
)]

use crate::{CreateWireSchema, WireSchemaField, WireSchemaName, WireSchemaStrictness};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireSchemaDefinition<T> {
    pub name: WireSchemaName,
    pub strictness: WireSchemaStrictness,
    pub fields: Vec<WireSchemaField<T>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct WireSchemaRef<'a, T> {
    pub name: &'a WireSchemaName,
    pub strictness: WireSchemaStrictness,
    pub fields: &'a [WireSchemaField<T>],
}
impl<T> Copy for WireSchemaRef<'_, T> {}
impl<T> Clone for WireSchemaRef<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> CreateWireSchema<T> {
    pub fn definition(&self) -> WireSchemaRef<'_, T> {
        self.into()
    }
}
impl<T> WireSchemaDefinition<T> {
    pub fn as_ref(&self) -> WireSchemaRef<'_, T> {
        self.into()
    }
}
impl<T: Clone> WireSchemaRef<'_, T> {
    pub fn to_owned(self) -> WireSchemaDefinition<T> {
        WireSchemaDefinition {
            name: self.name.clone(),
            strictness: self.strictness,
            fields: self.fields.to_vec(),
        }
    }
}
impl<'a, T> From<&'a CreateWireSchema<T>> for WireSchemaRef<'a, T> {
    fn from(schema: &'a CreateWireSchema<T>) -> Self {
        Self {
            name: &schema.name,
            strictness: schema.strictness,
            fields: &schema.fields,
        }
    }
}
impl<'a, T> From<&'a WireSchemaDefinition<T>> for WireSchemaRef<'a, T> {
    fn from(schema: &'a WireSchemaDefinition<T>) -> Self {
        Self {
            name: &schema.name,
            strictness: schema.strictness,
            fields: &schema.fields,
        }
    }
}
