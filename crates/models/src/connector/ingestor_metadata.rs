//! The typed `metadata` namespace a source exposes to its ingestor's programs.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The shape of an entity's metadata declaration: the fields, in order and with their
//!   exact types, that the entity's messages expose as `metadata.<field>`, and how a declared field
//!   reads as a schema field.
//! - **Depends on.** The schema field Model.
//! - **Must not know.** Which source exposes a field, how a message hands its value over, which
//!   fields a host builds columns for, or how a program reads them.
//!
//! Every ingestor entity declares its fields once, beside its other capabilities. The registry
//! validates an ingestor's programs and its `TIMESTAMP AT metadata.<field>` against that
//! declaration, the host builds the columns its programs read from it, and the entity's connector
//! hands a message's values over in its order, so the three cannot disagree about what
//! `metadata.partition` is.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::fmt;

use meticulous::ResultExt as _;

use crate::{FieldName, ParseAsType, SchemaField};

/// The exact type of one metadata field: a scalar the connector hands the host straight from its
/// message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IngestorMetadataType {
    String,
    Bytes,
    Bool,
    U16,
    I32,
    I64,
    Datetime,
}

impl IngestorMetadataType {
    /// The schema type a program reads a field of this type as.
    pub fn parse_as_type(self) -> ParseAsType {
        match self {
            Self::String => ParseAsType::String,
            Self::Bytes => ParseAsType::Bytes,
            Self::Bool => ParseAsType::Bool,
            Self::U16 => ParseAsType::U16,
            Self::I32 => ParseAsType::I32,
            Self::I64 => ParseAsType::I64,
            Self::Datetime => ParseAsType::Datetime,
        }
    }
}

impl fmt::Display for IngestorMetadataType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.parse_as_type().fmt(formatter)
    }
}

/// One typed field an entity's messages expose to its ingestor's programs as `metadata.<name>`.
///
/// Every field is optional, because a message may carry no value for it, and none is sensitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestorMetadataField {
    name: &'static str,
    ty: IngestorMetadataType,
}

impl IngestorMetadataField {
    pub const fn new(name: &'static str, ty: IngestorMetadataType) -> Self {
        Self { name, ty }
    }

    pub const fn name(self) -> &'static str {
        self.name
    }

    pub const fn ty(self) -> IngestorMetadataType {
        self.ty
    }

    /// The field as a program reads it: optional and not sensitive.
    pub fn schema_field(self) -> SchemaField {
        SchemaField {
            name: FieldName::parse(self.name).assured(
                "every declared metadata field name is a literal the declaration tests hold to \
                 the field-name grammar",
            ),
            ty: self.ty.parse_as_type(),
            optional: true,
            sensitive: false,
        }
    }
}

/// The metadata fields one entity's messages expose, in the order a message hands their values
/// over.
///
/// A declaration is a static list written beside its entity's other capabilities, and its names are
/// distinct, which the declaration tests check for every entity. It grows only when the vocabulary
/// declares a field, never with domains, graphs or clusters, and it is looked up by name only when
/// an ingestor's programs are validated or bound, never per message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestorMetadataDeclaration(&'static [IngestorMetadataField]);

impl IngestorMetadataDeclaration {
    /// The declaration of an entity whose messages expose no metadata field.
    pub const NONE: Self = Self(&[]);

    /// The declaration of `fields`, in the order a message hands their values over.
    pub const fn new(fields: &'static [IngestorMetadataField]) -> Self {
        Self(fields)
    }

    /// The declared fields in declared order.
    pub const fn fields(self) -> &'static [IngestorMetadataField] {
        self.0
    }

    pub const fn is_empty(self) -> bool {
        self.0.is_empty()
    }

    /// The declared position of the field named `name`, where the entity declares one.
    pub fn position(self, name: &str) -> Option<usize> {
        // The scan is bounded by the fields one entity declares in the vocabulary, and it runs
        // only when programs are validated or bound.
        self.0.iter().position(|field| field.name == name)
    }

    /// The declared field named `name`, where the entity declares one.
    pub fn field(self, name: &str) -> Option<IngestorMetadataField> {
        let position = self.position(name)?;
        self.0.get(position).copied()
    }

    /// The declared fields as a program reads them, in declared order.
    pub fn schema_fields(self) -> Vec<SchemaField> {
        self.0.iter().map(|field| field.schema_field()).collect()
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::IngestorEntityKind;

    #[test]
    fn every_entity_declares_distinct_field_names_of_the_field_name_grammar() {
        for kind in IngestorEntityKind::all() {
            let declared = kind.capabilities().metadata;
            let mut names = BTreeSet::new();
            for field in declared.fields() {
                assert!(
                    FieldName::parse(field.name()).is_ok(),
                    "{} declares metadata field '{}', which is not a field name",
                    kind.phrase(),
                    field.name()
                );
                assert!(
                    names.insert(field.name()),
                    "{} declares metadata field '{}' twice",
                    kind.phrase(),
                    field.name()
                );
            }
        }
    }

    #[test]
    fn a_declared_field_reads_as_an_optional_schema_field_of_its_type() {
        const FIELDS: &[IngestorMetadataField] = &[
            IngestorMetadataField::new("topic", IngestorMetadataType::String),
            IngestorMetadataField::new("observed_at", IngestorMetadataType::Datetime),
        ];
        let declared = IngestorMetadataDeclaration::new(FIELDS);

        assert_eq!(declared.position("observed_at"), Some(1));
        assert_eq!(declared.position("missing"), None);
        assert_eq!(
            declared.field("topic"),
            Some(IngestorMetadataField::new(
                "topic",
                IngestorMetadataType::String
            ))
        );
        let schema_fields = declared.schema_fields();
        assert_eq!(schema_fields.len(), 2);
        assert_eq!(schema_fields[1].name.as_str(), "observed_at");
        assert_eq!(schema_fields[1].ty, ParseAsType::Datetime);
        assert!(
            schema_fields
                .iter()
                .all(|field| field.optional && !field.sensitive)
        );
        assert!(IngestorMetadataDeclaration::NONE.is_empty());
    }

    #[test]
    fn every_type_names_its_schema_type() {
        for (ty, expected) in [
            (IngestorMetadataType::String, "STRING"),
            (IngestorMetadataType::Bytes, "BYTES"),
            (IngestorMetadataType::Bool, "BOOL"),
            (IngestorMetadataType::U16, "U16"),
            (IngestorMetadataType::I32, "I32"),
            (IngestorMetadataType::I64, "I64"),
            (IngestorMetadataType::Datetime, "DATETIME"),
        ] {
            assert_eq!(ty.to_string(), expected);
        }
    }
}
