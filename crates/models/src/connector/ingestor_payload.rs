//! The immutable payload facts an ingestor entity exposes.
//!
//! Layer: vocabulary.
//! - **Owns.** Declared payload fields and their required, non-sensitive `U64` schema.
//! - **Depends on.** Typed field names and internal schema fields.
//! - **Must not know.** Connector messages, decoding, expression execution or column projection.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the payload declaration boundary above")
)]

use meticulous::ResultExt as _;

use crate::{FieldName, IngestorMetadataType, SchemaField};

/// One immutable measure of the original payload, shared by all its decoded rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestorPayloadField {
    Bytes,
    Rows,
}

impl IngestorPayloadField {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Bytes => "bytes",
            Self::Rows => "rows",
        }
    }

    pub const fn ty(self) -> IngestorMetadataType {
        IngestorMetadataType::U64
    }

    pub fn schema_field(self) -> SchemaField {
        SchemaField {
            name: FieldName::parse(self.name())
                .assured("payload field names are fixed valid field-name literals"),
            ty: self.ty().parse_as_type(),
            optional: false,
            sensitive: false,
        }
    }
}

/// The payload facts an entity exposes, in the order its owner hands over their values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestorPayloadDeclaration(&'static [IngestorPayloadField]);

impl IngestorPayloadDeclaration {
    pub const BYTES: Self = Self(&[IngestorPayloadField::Bytes]);
    pub const BATCH: Self = Self(&[IngestorPayloadField::Bytes, IngestorPayloadField::Rows]);

    pub const fn fields(self) -> &'static [IngestorPayloadField] {
        self.0
    }

    pub fn position(self, name: &str) -> Option<usize> {
        self.0.iter().position(|field| field.name() == name)
    }

    pub fn field(self, name: &str) -> Option<IngestorPayloadField> {
        self.position(name)
            .and_then(|position| self.0.get(position))
            .copied()
    }

    pub fn schema_fields(self) -> Vec<SchemaField> {
        self.0.iter().map(|field| field.schema_field()).collect()
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "declaration conformance checks")
)]
mod tests {
    //! Payload declaration conformance.
    //!
    //! Layer: test harness.
    //! - **Owns.** The universal byte count and the client-only batch-row declaration.
    //! - **Depends on.** Vocabulary declarations and their schema projection.
    //! - **Must not know.** Message drivers, host projection or registry state.

    use super::*;
    use crate::{ClientProducerIngestor, IngestorDeclaration, IngestorEntityKind, ParseAsType};

    #[test]
    fn every_entity_declares_required_unsigned_bytes() {
        for kind in IngestorEntityKind::all() {
            let declaration = kind.capabilities().payload;
            assert_eq!(
                declaration.field("bytes"),
                Some(IngestorPayloadField::Bytes)
            );
            let fields = declaration.schema_fields();
            assert!(
                fields.iter().all(|field| field.ty == ParseAsType::U64
                    && !field.optional
                    && !field.sensitive)
            );
            let names = fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(names.len(), fields.len());
        }
    }

    #[test]
    fn a_client_batch_declares_its_original_rows() {
        assert_eq!(
            ClientProducerIngestor::CAPABILITIES.payload,
            IngestorPayloadDeclaration::BATCH
        );
        assert_eq!(IngestorPayloadDeclaration::BYTES.field("rows"), None);
        assert_eq!(
            IngestorPayloadDeclaration::BATCH.field("rows"),
            Some(IngestorPayloadField::Rows)
        );
    }
}
