//! The transport headers, typed metadata and transport position an ingestor message carries into
//! the host.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The header trait an ingestor message implements over its own borrowed headers, the
//!   retained copy of headers that must outlive their message, the visit through which a message
//!   hands the host the value of every metadata field its entity declares, the retained copy of
//!   those values, and the transport position a decode rejection names.
//! - **Depends on.** The vocabulary's metadata types and timestamps, and the standard library.
//! - **Must not know.** The Arrow columns the host projects these values into, which fields a
//!   program reads, or the VM namespaces programs read them through.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{fmt, net::SocketAddr};

use nervix_models::{IngestorMetadataType, Timestamp};
use nervix_primitives::sync::Arc;

/// Transport headers of one ingestor message, visited in the order its entity's declared header model
/// keeps them: as the message carries them for `ordered_repeats`, each name's values together for
/// `repeats_by_name`, one value per name for `last_wins`, and names in ascending byte order for
/// `sorted_by_name`.
///
/// Connectors implement this over their own borrowed message so a group's header builders
/// are appended from the ingestor directly, without an owned header vector per message.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the ingestor host visits transport headers for each admitted record"
    )
)]
pub trait IngestMessageHeaders: Send + Sync {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies a synchronous header visitor; local \
                                   visitor bodies remain analyzed")
    )]
    fn visit(&self, visit: &mut dyn FnMut(&str, &str));
}

/// A ingestor message that carries no transport headers.
pub struct NoIngestHeaders;

impl IngestMessageHeaders for NoIngestHeaders {
    fn visit(&self, _visit: &mut dyn FnMut(&str, &str)) {}
}

/// Transport headers copied out of an ingestor message so they outlive it.
///
/// Only the paths that append after their ingestor message is gone retain headers: the
/// quiesce buffer replays payloads long after the message was dropped, and a WebSocket
/// session carries its handshake headers into every later frame. Every other connector
/// appends from the borrowed message instead.
#[derive(Clone, Debug)]
pub struct RetainedIngestHeaders(Vec<(String, String)>);

impl RetainedIngestHeaders {
    /// Takes already validated transport header pairs in their declared order.
    pub fn from_pairs(pairs: Vec<(String, String)>) -> Self {
        Self(pairs)
    }

    /// Copies the headers an ingestor message carries right now.
    pub fn capture(headers: &dyn IngestMessageHeaders) -> Self {
        let mut retained = Vec::new();
        headers.visit(&mut |name, value| retained.push((name.to_string(), value.to_string())));
        Self(retained)
    }

    /// The headers of an ingestor that carries none.
    pub fn none() -> Self {
        Self(Vec::new())
    }
}

impl IngestMessageHeaders for RetainedIngestHeaders {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in &self.0 {
            visit(name, value);
        }
    }
}

/// How an ingestor message lends the host its transport headers.
#[derive(Clone, Copy)]
pub enum IngestRowHeaders<'a> {
    /// Headers the message lends for as long as it lives, which the host copies when it must keep
    /// them after the message is gone.
    Borrowed(&'a dyn IngestMessageHeaders),
    /// A snapshot the message shares with the other messages of its connection, which the host
    /// keeps by reference rather than copying it per message.
    Shared(&'a Arc<RetainedIngestHeaders>),
}

impl<'a> IngestRowHeaders<'a> {
    /// The headers of a message that carries none.
    pub const NONE: IngestRowHeaders<'static> = IngestRowHeaders::Borrowed(&NoIngestHeaders);

    /// The headers themselves, however the message lends them.
    pub fn headers(self) -> &'a dyn IngestMessageHeaders {
        match self {
            Self::Borrowed(headers) => headers,
            Self::Shared(headers) => headers.as_ref(),
        }
    }
}

/// Where a message's metadata values go: exactly one call for each field its entity declares, in
/// the order the declaration lists them, the call of the field's declared type or `absent` where the
/// message carries no value for that field.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the ingestor host appends the metadata a program reads for each admitted record"
    )
)]
pub trait IngestMetadataValues {
    /// The value of a `STRING` field.
    fn string(&mut self, value: &str);
    /// The value of a `STRING` field the message writes in place rather than holding as text, such
    /// as a socket address, so that no text is allocated for it per message.
    fn rendered(&mut self, value: &dyn fmt::Display);
    /// The value of a `BYTES` field.
    fn bytes(&mut self, value: &[u8]);
    /// The value of a `BOOL` field.
    fn bool(&mut self, value: bool);
    /// The value of a `U16` field.
    fn u16(&mut self, value: u16);
    /// The value of a `U32` field.
    fn u32(&mut self, value: u32);
    /// The value of an `I32` field.
    fn i32(&mut self, value: i32);
    /// The value of an `I64` field.
    fn i64(&mut self, value: i64);
    /// The value of a `DATETIME` field.
    fn datetime(&mut self, value: Timestamp);
    /// The message carries no value for the field, which reads as a typed null.
    fn absent(&mut self);
}

/// The typed metadata of one ingestor message, which hands the host the value of every field the
/// message's entity declares.
///
/// Connectors implement this over their own borrowed message, as they implement
/// [`IngestMessageHeaders`], so the host appends the values a program reads straight into its
/// columns, without an owned row per message.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the ingestor host visits the metadata a program reads for each admitted record"
    )
)]
pub trait IngestMessageMetadata: Send + Sync {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies the synchronous value sink; local sink \
                                   bodies remain analyzed")
    )]
    fn visit_metadata(&self, values: &mut dyn IngestMetadataValues);
}

/// The metadata of a message whose entity declares no metadata field.
pub struct NoIngestMetadata;

impl IngestMessageMetadata for NoIngestMetadata {
    fn visit_metadata(&self, _values: &mut dyn IngestMetadataValues) {}
}

/// One metadata value of a message, owned so it outlives the message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetainedIngestMetadataValue {
    /// The message carried no value for the field.
    Absent,
    String(String),
    Bytes(Vec<u8>),
    Bool(bool),
    U16(u16),
    U32(u32),
    I32(i32),
    I64(i64),
    Datetime(Timestamp),
}

impl RetainedIngestMetadataValue {
    /// The declared type this value is of, or `None` for a field the message carried no value
    /// for, which fits a field of any type.
    pub fn ty(&self) -> Option<IngestorMetadataType> {
        match self {
            Self::Absent => None,
            Self::String(_) => Some(IngestorMetadataType::String),
            Self::Bytes(_) => Some(IngestorMetadataType::Bytes),
            Self::Bool(_) => Some(IngestorMetadataType::Bool),
            Self::U16(_) => Some(IngestorMetadataType::U16),
            Self::U32(_) => Some(IngestorMetadataType::U32),
            Self::I32(_) => Some(IngestorMetadataType::I32),
            Self::I64(_) => Some(IngestorMetadataType::I64),
            Self::Datetime(_) => Some(IngestorMetadataType::Datetime),
        }
    }
}

/// The metadata values of a message copied out of it, in its entity's declared order, so they
/// outlive it: the quiesce buffer replays them long after the message was dropped, and a paced
/// source returns its poll's messages owned.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RetainedIngestMetadata(Vec<RetainedIngestMetadataValue>);

impl RetainedIngestMetadata {
    /// Copies the values `metadata` hands over right now.
    pub fn capture(metadata: &dyn IngestMessageMetadata) -> Self {
        let mut values = Vec::new();
        metadata.visit_metadata(&mut RetainedIngestMetadataCapture(&mut values));
        Self(values)
    }

    /// The metadata of a message whose entity declares no metadata field.
    pub fn none() -> Self {
        Self(Vec::new())
    }

    /// The values, in declared order, of a message a connector already holds owned.
    pub fn from_values(values: Vec<RetainedIngestMetadataValue>) -> Self {
        Self(values)
    }

    /// The retained values in declared order.
    pub fn values(&self) -> &[RetainedIngestMetadataValue] {
        &self.0
    }
}

impl IngestMessageMetadata for RetainedIngestMetadata {
    fn visit_metadata(&self, values: &mut dyn IngestMetadataValues) {
        for value in &self.0 {
            match value {
                RetainedIngestMetadataValue::Absent => values.absent(),
                RetainedIngestMetadataValue::String(value) => values.string(value),
                RetainedIngestMetadataValue::Bytes(value) => values.bytes(value),
                RetainedIngestMetadataValue::Bool(value) => values.bool(*value),
                RetainedIngestMetadataValue::U16(value) => values.u16(*value),
                RetainedIngestMetadataValue::U32(value) => values.u32(*value),
                RetainedIngestMetadataValue::I32(value) => values.i32(*value),
                RetainedIngestMetadataValue::I64(value) => values.i64(*value),
                RetainedIngestMetadataValue::Datetime(value) => values.datetime(*value),
            }
        }
    }
}

/// Appends every value a message hands over to the owned copy being taken of it.
struct RetainedIngestMetadataCapture<'a>(&'a mut Vec<RetainedIngestMetadataValue>);

impl IngestMetadataValues for RetainedIngestMetadataCapture<'_> {
    fn string(&mut self, value: &str) {
        self.0
            .push(RetainedIngestMetadataValue::String(value.to_string()));
    }

    fn rendered(&mut self, value: &dyn fmt::Display) {
        self.0
            .push(RetainedIngestMetadataValue::String(value.to_string()));
    }

    fn bytes(&mut self, value: &[u8]) {
        self.0
            .push(RetainedIngestMetadataValue::Bytes(value.to_vec()));
    }

    fn bool(&mut self, value: bool) {
        self.0.push(RetainedIngestMetadataValue::Bool(value));
    }

    fn u16(&mut self, value: u16) {
        self.0.push(RetainedIngestMetadataValue::U16(value));
    }

    fn u32(&mut self, value: u32) {
        self.0.push(RetainedIngestMetadataValue::U32(value));
    }

    fn i32(&mut self, value: i32) {
        self.0.push(RetainedIngestMetadataValue::I32(value));
    }

    fn i64(&mut self, value: i64) {
        self.0.push(RetainedIngestMetadataValue::I64(value));
    }

    fn datetime(&mut self, value: Timestamp) {
        self.0.push(RetainedIngestMetadataValue::Datetime(value));
    }

    fn absent(&mut self) {
        self.0.push(RetainedIngestMetadataValue::Absent);
    }
}

/// Where on its transport a message was read, which a decode rejection of its payload names.
///
/// A position is the transport's own coordinate, never a header value its sender wrote, and only
/// the transports that give one name it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestTransportPosition<'a> {
    /// The topic, partition and offset of a Kafka record.
    Kafka {
        topic: &'a str,
        partition: i32,
        offset: i64,
    },
    /// The peer a syslog frame came from.
    Syslog { peer: SocketAddr },
}

/// One message's ingest metadata, read from its ingestor message: the values of its entity's
/// declared metadata fields, its transport headers and, where its transport names one, its
/// position.
///
/// The parts borrow the ingestor message, so copying a row copies only those borrows.
#[derive(Clone, Copy)]
pub struct IngestMetadataRow<'a> {
    pub fields: &'a dyn IngestMessageMetadata,
    pub headers: IngestRowHeaders<'a>,
    pub position: Option<IngestTransportPosition<'a>>,
}

impl<'a> IngestMetadataRow<'a> {
    /// The row of a message that lends its headers alone: its entity declares no metadata field and
    /// its transport names no position.
    pub fn headers(headers: &'a dyn IngestMessageHeaders) -> Self {
        Self {
            fields: &NoIngestMetadata,
            headers: IngestRowHeaders::Borrowed(headers),
            position: None,
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::net::SocketAddr;

    use nervix_models::Timestamp;

    use super::{
        IngestMessageMetadata, IngestMetadataRow, IngestMetadataValues, IngestRowHeaders,
        NoIngestHeaders, NoIngestMetadata, RetainedIngestMetadata, RetainedIngestMetadataValue,
    };

    /// A message whose entity declares one field of every type, in a fixed order.
    struct EveryType {
        peer: SocketAddr,
        observed_at: Timestamp,
    }

    impl IngestMessageMetadata for EveryType {
        fn visit_metadata(&self, values: &mut dyn IngestMetadataValues) {
            values.string("orders");
            values.rendered(&self.peer);
            values.bytes(b"key");
            values.bool(true);
            values.u16(206);
            values.u32(u32::MAX);
            values.i32(-3);
            values.i64(42);
            values.datetime(self.observed_at);
            values.absent();
        }
    }

    #[test]
    fn a_retained_copy_visits_every_value_in_the_order_its_message_handed_them_over() {
        let message = EveryType {
            peer: SocketAddr::from(([127, 0, 0, 1], 514)),
            observed_at: Timestamp::from_unix_nanos(1_234),
        };

        let retained = RetainedIngestMetadata::capture(&message);

        assert_eq!(
            retained.values(),
            [
                RetainedIngestMetadataValue::String("orders".to_string()),
                RetainedIngestMetadataValue::String("127.0.0.1:514".to_string()),
                RetainedIngestMetadataValue::Bytes(b"key".to_vec()),
                RetainedIngestMetadataValue::Bool(true),
                RetainedIngestMetadataValue::U16(206),
                RetainedIngestMetadataValue::U32(u32::MAX),
                RetainedIngestMetadataValue::I32(-3),
                RetainedIngestMetadataValue::I64(42),
                RetainedIngestMetadataValue::Datetime(Timestamp::from_unix_nanos(1_234)),
                RetainedIngestMetadataValue::Absent,
            ]
        );
        assert_eq!(RetainedIngestMetadata::capture(&retained), retained);
    }

    #[test]
    fn a_message_without_declared_fields_or_headers_hands_over_nothing() {
        assert!(
            RetainedIngestMetadata::capture(&NoIngestMetadata)
                .values()
                .is_empty()
        );
        assert_eq!(
            RetainedIngestMetadata::none(),
            RetainedIngestMetadata::from_values(Vec::new())
        );

        let row = IngestMetadataRow::headers(&NoIngestHeaders);
        assert!(
            RetainedIngestMetadata::capture(row.fields)
                .values()
                .is_empty()
        );
        assert!(row.position.is_none());
        let mut visited = 0;
        row.headers.headers().visit(&mut |_, _| visited += 1);
        IngestRowHeaders::NONE
            .headers()
            .visit(&mut |_, _| visited += 1);
        assert_eq!(visited, 0);
    }
}
