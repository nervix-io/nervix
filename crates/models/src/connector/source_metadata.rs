//! The typed `metadata` namespace a source exposes to its ingestor's programs.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The one declaration of each metadata namespace: which namespaces exist and the
//!   fields, in order and with their exact types, that each one carries.
//! - **Depends on.** The schema field Model.
//! - **Must not know.** Which source exposes a namespace, how a row of it is built from a message,
//!   or how a program reads it.
//!
//! The registry validates an ingestor's programs against these fields, the runtime builds the
//! columns its programs read from them, and the connector contract names the same scope, so the
//! three cannot disagree about what `metadata.partition` is.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use meticulous::ResultExt as _;

use crate::{FieldName, ParseAsType, SchemaField};

/// The typed `metadata` namespace the messages of one source expose to its ingestor's programs,
/// beside the transport headers `read_header` and `read_headers` read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceMetadataScope {
    /// No typed field: transport headers are all a message carries beside its payload.
    Headers,
    /// Where a Kafka message sits in its topic: `topic`, `partition` and `offset`.
    Kafka,
    /// The peer a syslog message arrived from: `peer_addr`.
    Syslog,
}

impl SourceMetadataScope {
    /// The fields of this namespace in declared order.
    ///
    /// Every field is optional and none is sensitive. A scope without a typed namespace has no
    /// field.
    pub fn fields(self) -> Vec<SchemaField> {
        match self {
            Self::Headers => Vec::new(),
            Self::Kafka => vec![
                Self::field("topic", ParseAsType::String),
                Self::field("partition", ParseAsType::I32),
                Self::field("offset", ParseAsType::I64),
            ],
            Self::Syslog => vec![Self::field("peer_addr", ParseAsType::String)],
        }
    }

    fn field(name: &'static str, ty: ParseAsType) -> SchemaField {
        SchemaField {
            name: FieldName::parse(name)
                .assured("every metadata field name is a literal that satisfies the grammar"),
            ty,
            optional: true,
            sensitive: false,
        }
    }
}
