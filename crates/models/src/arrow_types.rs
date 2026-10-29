//! The Arrow shape every schema type and field takes, in each batch Nervix builds or accepts.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The one mapping from a schema type to its Arrow data type, from declared fields
//!   to the canonical Arrow schema a batch of them carries, and from an entity's declared metadata
//!   fields to the Arrow fields and schema they are carried as.
//! - **Depends on.** The schema Models, the metadata declarations, and Arrow's schema types.
//! - **Must not know.** Arrow arrays, how a batch is built, decoded or validated, or who reads it.
//!
//! The mapping is part of the data model rather than of any one reader of it: the server compiles
//! schemas with it and the client library builds the batches it submits with it, so the two can
//! never disagree about the representation of a type.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use meticulous::ResultExt as _;
use nervix_primitives::sync::StdArc;

use crate::{IngestorMetadataDeclaration, IngestorMetadataField, ParseAsType, SchemaField};

impl ParseAsType {
    /// The Arrow data type a value of this type is represented as. A datetime is nanoseconds in
    /// UTC, and a list element is never null.
    pub fn arrow_data_type(&self) -> DataType {
        match self {
            Self::U8 => DataType::UInt8,
            Self::I8 => DataType::Int8,
            Self::U16 => DataType::UInt16,
            Self::I16 => DataType::Int16,
            Self::U32 => DataType::UInt32,
            Self::I32 => DataType::Int32,
            Self::U64 => DataType::UInt64,
            Self::I64 => DataType::Int64,
            Self::Bool => DataType::Boolean,
            Self::String => DataType::Utf8,
            Self::Bytes => DataType::Binary,
            Self::Datetime => DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            Self::F32 => DataType::Float32,
            Self::F64 => DataType::Float64,
            Self::Array { element, len } => DataType::FixedSizeList(
                StdArc::new(Field::new("item", element.arrow_data_type(), false)),
                i32::try_from(len.get()).verified(
                    "the schema parser rejects an array length that does not fit an Arrow \
                     fixed-size list",
                ),
            ),
            Self::Vec { element } => DataType::List(StdArc::new(Field::new(
                "item",
                element.arrow_data_type(),
                false,
            ))),
        }
    }
}

impl SchemaField {
    /// The Arrow field this schema field is carried as: its name, its exact type, and nullable
    /// exactly when it is optional. Sensitivity is not an Arrow property and is not carried.
    pub fn arrow_field(&self) -> Field {
        Field::new(self.name.as_str(), self.ty.arrow_data_type(), self.optional)
    }

    /// The canonical Arrow schema of `fields`, in their declared order and without metadata.
    pub fn arrow_schema(fields: &[Self]) -> Schema {
        Schema::new(fields.iter().map(Self::arrow_field).collect::<Vec<_>>())
    }
}

impl IngestorMetadataField {
    /// The Arrow field this metadata field is carried as: its name, its exact type and nullable,
    /// because a message may carry no value for it.
    ///
    /// A host builds the fields its programs read from this, straight from the declaration, without
    /// the schema fields a validation reads them as.
    pub fn arrow_field(self) -> Field {
        Field::new(
            self.name(),
            self.ty().parse_as_type().arrow_data_type(),
            true,
        )
    }
}

impl IngestorMetadataDeclaration {
    /// The Arrow schema of every declared field, in declared order. An entity that declares no
    /// field has no typed namespace, so it has no schema.
    pub fn arrow_schema(self) -> Option<Schema> {
        if self.is_empty() {
            return None;
        }
        let fields = self
            .fields()
            .iter()
            .map(|field| field.arrow_field())
            .collect::<Vec<_>>();
        Some(Schema::new(fields))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use meticulous::OptionExt as _;

    use super::*;
    use crate::FieldName;

    fn field(name: &str, ty: ParseAsType, optional: bool) -> SchemaField {
        SchemaField {
            name: FieldName::parse(name).assured("the fixture field name is valid"),
            ty,
            optional,
            sensitive: false,
        }
    }

    #[test]
    fn nested_lists_never_carry_null_elements() {
        let length = NonZeroU32::new(3).assured("three is not zero");
        let ty = ParseAsType::Vec {
            element: Box::new(ParseAsType::Array {
                element: Box::new(ParseAsType::Datetime),
                len: length,
            }),
        };
        let DataType::List(outer) = ty.arrow_data_type() else {
            panic!("a VEC is an Arrow list");
        };
        assert!(!outer.is_nullable());
        let DataType::FixedSizeList(inner, 3) = outer.data_type() else {
            panic!("an ARRAY of three is a fixed-size list of three");
        };
        assert!(!inner.is_nullable());
        assert_eq!(
            inner.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
        );
    }

    fn declared_metadata(kind: crate::IngestorEntityKind) -> IngestorMetadataDeclaration {
        kind.capabilities().metadata
    }

    #[test]
    fn a_metadata_declaration_is_carried_as_its_declared_fields_and_none_as_no_schema() {
        use crate::{
            HttpIngestorEntity, IngestorEntityKind, KafkaIngestorEntity, MqttIngestorEntity,
            SyslogIngestorEntity, WebsocketsIngestorEntity,
        };

        let described = |kind: IngestorEntityKind| {
            declared_metadata(kind).arrow_schema().map(|schema| {
                schema
                    .fields()
                    .iter()
                    .map(|field| {
                        (
                            field.name().clone(),
                            field.data_type().clone(),
                            field.is_nullable(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(
            described(IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic)),
            None
        );
        assert_eq!(
            described(IngestorEntityKind::Http(HttpIngestorEntity::Request)),
            Some(vec![
                ("status".to_owned(), DataType::UInt16, true),
                (
                    "polled_at".to_owned(),
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                    true
                ),
            ])
        );
        assert_eq!(
            described(IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic)),
            Some(vec![
                ("topic".to_owned(), DataType::Utf8, true),
                ("partition".to_owned(), DataType::Int32, true),
                ("offset".to_owned(), DataType::Int64, true),
            ])
        );
        assert_eq!(
            described(IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener)),
            Some(vec![("peer_addr".to_owned(), DataType::Utf8, true)])
        );
        assert_eq!(
            described(IngestorEntityKind::Websockets(
                WebsocketsIngestorEntity::Connection
            )),
            Some(vec![("frame_type".to_owned(), DataType::Utf8, true)])
        );
    }

    #[test]
    fn declared_metadata_columns_are_the_schema_fields_a_program_is_validated_against() {
        for kind in crate::IngestorEntityKind::all() {
            let declared = declared_metadata(kind);
            if declared.is_empty() {
                assert_eq!(declared.arrow_schema(), None, "{kind:?}");
                assert!(declared.schema_fields().is_empty(), "{kind:?}");
                continue;
            }
            assert_eq!(
                declared.arrow_schema(),
                Some(SchemaField::arrow_schema(&declared.schema_fields())),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn a_schema_keeps_field_order_and_optionality_without_metadata() {
        let schema = SchemaField::arrow_schema(&[
            field("seq", ParseAsType::I64, false),
            field("payload", ParseAsType::Bytes, true),
        ]);
        let names = schema
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>();
        assert_eq!(names, [("seq", false), ("payload", true)]);
        assert!(schema.metadata().is_empty());
        assert!(
            schema
                .fields()
                .iter()
                .all(|field| field.metadata().is_empty())
        );
    }
}
