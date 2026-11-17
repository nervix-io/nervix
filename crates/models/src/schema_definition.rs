//! Metadata-free internal schema inputs to engines.
//!
//! Layer: vocabulary.
//! - **Owns.** Borrowed and owned schema definitions at the engine boundary.
//! - **Depends on.** Typed schema names and complete ordered fields.
//! - **Must not know.** Metadata policy, graph placement, persistence or running execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns typed internal schema inputs")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{CreateSchema, SchemaField, SchemaName};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SchemaDefinition {
    pub name: SchemaName,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaRef<'a> {
    pub name: &'a SchemaName,
    pub fields: &'a [SchemaField],
}

impl CreateSchema {
    pub fn definition(&self) -> SchemaRef<'_> {
        self.into()
    }
}
impl SchemaDefinition {
    pub fn as_ref(&self) -> SchemaRef<'_> {
        self.into()
    }
}
impl SchemaRef<'_> {
    pub fn to_owned(self) -> SchemaDefinition {
        SchemaDefinition {
            name: self.name.clone(),
            fields: self.fields.to_vec(),
        }
    }
}
impl From<CreateSchema> for SchemaDefinition {
    fn from(schema: CreateSchema) -> Self {
        Self {
            name: schema.name,
            fields: schema.fields,
        }
    }
}
impl<'a> From<&'a CreateSchema> for SchemaRef<'a> {
    fn from(schema: &'a CreateSchema) -> Self {
        Self {
            name: &schema.name,
            fields: &schema.fields,
        }
    }
}
impl<'a> From<&'a SchemaDefinition> for SchemaRef<'a> {
    fn from(schema: &'a SchemaDefinition) -> Self {
        Self {
            name: &schema.name,
            fields: &schema.fields,
        }
    }
}
