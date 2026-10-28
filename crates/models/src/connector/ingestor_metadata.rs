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
pub enum IngestorMetadataScope {
    /// No typed field: transport headers are all a message carries beside its payload.
    Headers,
    /// The response status and scheduled poll instant of an HTTP request.
    Http,
    /// Where a Kafka message sits in its topic: `topic`, `partition` and `offset`.
    Kafka,
    /// The peer a syslog message arrived from: `peer_addr`.
    Syslog,
    /// The text or binary kind of one WebSockets connection data frame.
    Websockets,
}

/// One typed field a metadata namespace declares: its name and its exact type. Every field is
/// optional and none is sensitive.
pub(crate) struct IngestorMetadataField {
    pub(crate) name: &'static str,
    pub(crate) ty: ParseAsType,
}

/// Where a Kafka message sits in its topic.
const KAFKA_FIELDS: &[IngestorMetadataField] = &[
    IngestorMetadataField {
        name: "topic",
        ty: ParseAsType::String,
    },
    IngestorMetadataField {
        name: "partition",
        ty: ParseAsType::I32,
    },
    IngestorMetadataField {
        name: "offset",
        ty: ParseAsType::I64,
    },
];

/// The peer a syslog message arrived from.
const SYSLOG_FIELDS: &[IngestorMetadataField] = &[IngestorMetadataField {
    name: "peer_addr",
    ty: ParseAsType::String,
}];

const WEBSOCKETS_FIELDS: &[IngestorMetadataField] = &[IngestorMetadataField {
    name: "frame_type",
    ty: ParseAsType::String,
}];

const HTTP_FIELDS: &[IngestorMetadataField] = &[
    IngestorMetadataField {
        name: "status",
        ty: ParseAsType::U16,
    },
    IngestorMetadataField {
        name: "polled_at",
        ty: ParseAsType::Datetime,
    },
];

impl IngestorMetadataScope {
    /// The one declaration of this namespace's fields, in order. The schema fields a program is
    /// validated against and the Arrow columns a host builds are both read from it, so neither
    /// states a field a second time.
    pub(crate) const fn declared_fields(self) -> &'static [IngestorMetadataField] {
        match self {
            Self::Headers => &[],
            Self::Http => HTTP_FIELDS,
            Self::Kafka => KAFKA_FIELDS,
            Self::Syslog => SYSLOG_FIELDS,
            Self::Websockets => WEBSOCKETS_FIELDS,
        }
    }

    /// The fields of this namespace in declared order.
    ///
    /// Every field is optional and none is sensitive. A scope without a typed namespace has no
    /// field.
    pub fn fields(self) -> Vec<SchemaField> {
        self.declared_fields()
            .iter()
            .map(|field| SchemaField {
                name: FieldName::parse(field.name)
                    .assured("every metadata field name is a literal that satisfies the grammar"),
                ty: field.ty.clone(),
                optional: true,
                sensitive: false,
            })
            .collect()
    }
}
