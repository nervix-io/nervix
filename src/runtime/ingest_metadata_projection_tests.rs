//! Declared metadata values across a connector's retained copy, the quiesce buffer and the host's
//! lazy projection.
//!
//! Layer: test harness.
//! - **Owns.** A declaration of every metadata type, generated read sets and messages with their
//!   values, unrepresentable values among them, headers and positions, and the oracle their
//!   retained copies, columns and refusals are held to.
//! - **Depends on.** The production metadata projection and column builders, the connector
//!   contract's retained metadata, headers and value sink, and the quiesce buffer's retained row.
//! - **Must not know.** Connectors, transports, programs or the registry.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the declared metadata projection boundary")
)]

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    num::NonZeroUsize,
};

use arrow_array::{
    Array,
    cast::AsArray as _,
    types::{Int32Type, Int64Type, TimestampNanosecondType, UInt8Type, UInt16Type, UInt64Type},
};
use nervix_arbitrary::Entropy;
use nervix_connector::{
    IngestMessageHeaders, IngestMessageMetadata, IngestMetadataValues, IngestRowHeaders,
    IngestTransportPosition, RetainedIngestHeaders, RetainedIngestMetadata,
    RetainedIngestMetadataValue as Value,
};
use nervix_models::Timestamp;
use nervix_primitives::sync::Arc;
use nonzero_ext::nonzero;

use super::*;
use crate::runtime::BufferedIngestMetadata;

const CASE_BYTES: usize = 1024;

/// Every metadata type twice and interleaved, so a read set can read one field of a type and skip
/// the other, and every column sits between columns of other types.
const DECLARED_FIELDS: &[IngestorMetadataField] = &[
    IngestorMetadataField::new("topic", IngestorMetadataType::String),
    IngestorMetadataField::new("key", IngestorMetadataType::Bytes),
    IngestorMetadataField::new("redelivered", IngestorMetadataType::Bool),
    IngestorMetadataField::new("priority", IngestorMetadataType::U8),
    IngestorMetadataField::new("status", IngestorMetadataType::U16),
    IngestorMetadataField::new("partition", IngestorMetadataType::I32),
    IngestorMetadataField::new("offset", IngestorMetadataType::I64),
    IngestorMetadataField::new("delivery_tag", IngestorMetadataType::U64),
    IngestorMetadataField::new("polled_at", IngestorMetadataType::Datetime),
    IngestorMetadataField::new("peer_addr", IngestorMetadataType::String),
    IngestorMetadataField::new("token", IngestorMetadataType::Bytes),
    IngestorMetadataField::new("retained", IngestorMetadataType::Bool),
    IngestorMetadataField::new("level", IngestorMetadataType::U8),
    IngestorMetadataField::new("code", IngestorMetadataType::U16),
    IngestorMetadataField::new("sequence", IngestorMetadataType::I32),
    IngestorMetadataField::new("lag", IngestorMetadataType::I64),
    IngestorMetadataField::new("count", IngestorMetadataType::U64),
    IngestorMetadataField::new("published_at", IngestorMetadataType::Datetime),
];

const DECLARED: IngestorMetadataDeclaration = IngestorMetadataDeclaration::new(DECLARED_FIELDS);

const DECLARED_COUNT: NonZeroUsize = match NonZeroUsize::new(DECLARED_FIELDS.len()) {
    Some(count) => count,
    None => panic!("the generated declaration declares at least one field"),
};

/// A read set names up to twice as many fields as are declared, so it repeats and reorders them.
const MAX_READ_NAMES: usize = DECLARED_FIELDS.len() * 2;

const MAX_MESSAGES: NonZeroUsize = nonzero!(8_usize);

const MAX_TEXT_CHARS: usize = 6;

const MAX_BYTES: usize = 6;

const MAX_HEADERS: usize = 3;

/// Characters of one, two, three and four UTF-8 bytes.
const TEXT: [char; 8] = ['a', 'Z', '0', ' ', '-', 'é', '中', '🦀'];

/// Two names, so a message's headers often repeat one.
const HEADER_NAMES: [&str; 2] = ["x-trace", "x-tenant"];

#[derive(Debug, Clone)]
struct GeneratedHeader {
    name: &'static str,
    value: String,
}

struct GeneratedHeaders(Vec<GeneratedHeader>);

impl IngestMessageHeaders for GeneratedHeaders {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for header in &self.0 {
            visit(header.name, &header.value);
        }
    }
}

/// The transport position a generated message names, owned so the message can lend it.
enum GeneratedPosition {
    None,
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    Syslog {
        peer: SocketAddr,
    },
}

impl GeneratedPosition {
    fn borrowed(&self) -> Option<IngestTransportPosition<'_>> {
        match self {
            Self::None => None,
            Self::Kafka {
                topic,
                partition,
                offset,
            } => Some(IngestTransportPosition::Kafka {
                topic,
                partition: *partition,
                offset: *offset,
            }),
            Self::Syslog { peer } => Some(IngestTransportPosition::Syslog { peer: *peer }),
        }
    }
}

/// A message of the generated entity: the value it hands over for every declared field in
/// declared order, its headers and its position.
struct GeneratedMessage {
    values: Vec<Value>,
    /// Whether the message hands its text over rendered, as a connector that formats a transport
    /// value does, rather than borrowed.
    renders_text: bool,
    headers: Vec<GeneratedHeader>,
    snapshot: Arc<RetainedIngestHeaders>,
    /// Whether the message shares its header snapshot with its connection rather than lending it.
    shares_headers: bool,
    position: GeneratedPosition,
}

impl IngestMessageMetadata for GeneratedMessage {
    fn visit_metadata(&self, values: &mut dyn IngestMetadataValues) {
        for value in &self.values {
            match value {
                Value::Absent => values.absent(),
                Value::String(text) => {
                    if self.renders_text {
                        values.rendered(text);
                    } else {
                        values.string(text);
                    }
                }
                Value::Bytes(bytes) => values.bytes(bytes),
                Value::Bool(value) => values.bool(*value),
                Value::U8(value) => values.u8(*value),
                Value::U16(value) => values.u16(*value),
                Value::I32(value) => values.i32(*value),
                Value::I64(value) => values.i64(*value),
                Value::U64(value) => values.u64(*value),
                Value::Datetime(value) => values.datetime(*value),
                Value::Unrepresentable { ty, value } => values.unrepresentable(*ty, value),
            }
        }
    }
}

impl GeneratedMessage {
    fn row(&self) -> IngestMetadataRow<'_> {
        let headers = if self.shares_headers {
            IngestRowHeaders::Shared(&self.snapshot)
        } else {
            let lent: &RetainedIngestHeaders = &self.snapshot;
            IngestRowHeaders::Borrowed(lent)
        };
        IngestMetadataRow {
            fields: self,
            headers,
            position: self.position.borrowed(),
        }
    }

    fn header_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::with_capacity(self.headers.len());
        for header in &self.headers {
            pairs.push((header.name.to_string(), header.value.clone()));
        }
        pairs
    }

    /// The values of the headers named `name`, in the order the message carries them.
    fn header_values(&self, name: &str) -> Vec<String> {
        let mut values = Vec::new();
        for header in &self.headers {
            if header.name == name {
                values.push(header.value.clone());
            }
        }
        values
    }
}

/// Metadata a message must never be asked for: a group whose programs read no metadata field
/// visits no metadata value.
struct Unvisited;

impl IngestMessageMetadata for Unvisited {
    fn visit_metadata(&self, _values: &mut dyn IngestMetadataValues) {
        panic!("a group whose programs read no metadata field visited a message's metadata values");
    }
}

fn visited_headers(headers: &dyn IngestMessageHeaders) -> Vec<(String, String)> {
    let mut visited = Vec::new();
    headers.visit(&mut |name, value| visited.push((name.to_string(), value.to_string())));
    visited
}

/// Reads bounded generated cases from a property's bytes.
struct Cases<'bytes> {
    entropy: Entropy<'bytes>,
}

impl Cases<'_> {
    fn text(&mut self) -> String {
        let length = self.entropy.count(MAX_TEXT_CHARS);
        let mut text = String::new();
        for _ in 0..length {
            text.push(self.entropy.pick(TEXT));
        }
        text
    }

    fn bytes(&mut self) -> Vec<u8> {
        let length = self.entropy.count(MAX_BYTES);
        let mut bytes = Vec::with_capacity(length);
        for _ in 0..length {
            bytes.push(self.entropy.byte());
        }
        bytes
    }

    fn u8(&mut self) -> u8 {
        let value = self.entropy.boundary_biased(0..=u64::from(u8::MAX));
        u8::try_from(value).verified("the value was drawn from the u8 range")
    }

    fn u16(&mut self) -> u16 {
        let value = self.entropy.boundary_biased(0..=u64::from(u16::MAX));
        u16::try_from(value).verified("the value was drawn from the u16 range")
    }

    /// Any `u64`, landing on the ends of its range and next to them as often as elsewhere.
    fn u64(&mut self) -> u64 {
        self.entropy.boundary_biased(0..=u64::MAX)
    }

    /// Any `i32`, landing on zero, the extremes and their neighbours as often as elsewhere.
    fn i32(&mut self) -> i32 {
        match self.entropy.byte() % 6 {
            0 => 0,
            1 => i32::MIN,
            2 => i32::MAX,
            3 => -1,
            4 => 1,
            _ => {
                let bits = self.entropy.up_to(u64::from(u32::MAX));
                let bits = u32::try_from(bits).verified("the bits were drawn from the u32 range");
                bits.cast_signed()
            }
        }
    }

    /// A value of `ty` half the time, otherwise an absence or, less often, a value the transport
    /// carried that `ty` cannot hold, described by generated text.
    fn value(&mut self, ty: IngestorMetadataType) -> Value {
        match self.entropy.byte() % 8 {
            0..=3 => self.present(ty),
            4..=6 => Value::Absent,
            _ => Value::Unrepresentable {
                ty,
                value: self.text(),
            },
        }
    }

    fn present(&mut self, ty: IngestorMetadataType) -> Value {
        match ty {
            IngestorMetadataType::String => Value::String(self.text()),
            IngestorMetadataType::Bytes => Value::Bytes(self.bytes()),
            IngestorMetadataType::Bool => Value::Bool(self.entropy.flag()),
            IngestorMetadataType::U8 => Value::U8(self.u8()),
            IngestorMetadataType::U16 => Value::U16(self.u16()),
            IngestorMetadataType::I32 => Value::I32(self.i32()),
            IngestorMetadataType::I64 => Value::I64(self.entropy.any_i64()),
            IngestorMetadataType::U64 => Value::U64(self.u64()),
            IngestorMetadataType::Datetime => {
                Value::Datetime(Timestamp::from_unix_nanos(self.entropy.any_i64()))
            }
        }
    }

    fn headers(&mut self) -> Vec<GeneratedHeader> {
        let count = self.entropy.count(MAX_HEADERS);
        let mut headers = Vec::with_capacity(count);
        for _ in 0..count {
            let name = self.entropy.pick(HEADER_NAMES);
            let value = self.text();
            headers.push(GeneratedHeader { name, value });
        }
        headers
    }

    fn peer(&mut self) -> SocketAddr {
        let address = if self.entropy.flag() {
            let bits = self.entropy.up_to(u64::from(u32::MAX));
            let bits = u32::try_from(bits).verified("the bits were drawn from the u32 range");
            IpAddr::V4(Ipv4Addr::from(bits))
        } else {
            let high = u128::from(self.entropy.any_u64());
            let low = u128::from(self.entropy.any_u64());
            IpAddr::V6(Ipv6Addr::from((high << 64) | low))
        };
        SocketAddr::new(address, self.u16())
    }

    fn position(&mut self) -> GeneratedPosition {
        match self.entropy.byte() % 3 {
            0 => GeneratedPosition::None,
            1 => GeneratedPosition::Kafka {
                topic: self.text(),
                partition: self.i32(),
                offset: self.entropy.any_i64(),
            },
            _ => GeneratedPosition::Syslog { peer: self.peer() },
        }
    }

    fn message(&mut self) -> GeneratedMessage {
        let mut values = Vec::with_capacity(DECLARED_FIELDS.len());
        for field in DECLARED_FIELDS {
            values.push(self.value(field.ty()));
        }
        let renders_text = self.entropy.flag();
        let headers = self.headers();
        let snapshot = Arc::new(RetainedIngestHeaders::capture(&GeneratedHeaders(
            headers.clone(),
        )));
        let shares_headers = self.entropy.flag();
        let position = self.position();
        GeneratedMessage {
            values,
            renders_text,
            headers,
            snapshot,
            shares_headers,
            position,
        }
    }

    fn case(mut self) -> MetadataCase {
        let names = self.entropy.count(MAX_READ_NAMES);
        let mut read = Vec::with_capacity(names);
        for _ in 0..names {
            read.push(self.entropy.index(DECLARED_COUNT));
        }
        let count = self.entropy.positive_count(MAX_MESSAGES);
        let mut messages = Vec::with_capacity(count);
        for _ in 0..count {
            messages.push(self.message());
        }
        MetadataCase { read, messages }
    }
}

struct MetadataCase {
    /// The declared positions of the fields the programs read, in the order and with the
    /// repetition they name them.
    read: Vec<usize>,
    messages: Vec<GeneratedMessage>,
}

impl MetadataCase {
    /// Every message's values come back exactly from the connector's owned copy, and its values,
    /// headers and position from the quiesce buffer's retained row.
    fn assert_retained_exactly(&self) {
        for message in &self.messages {
            let captured = RetainedIngestMetadata::capture(message);
            assert_eq!(captured.values(), message.values.as_slice());

            let row = message.row();
            let buffered = BufferedIngestMetadata::retain(&row);
            let replayed = buffered.row();
            assert_eq!(RetainedIngestMetadata::capture(replayed.fields), captured);
            assert_eq!(replayed.position, row.position);
            assert_eq!(
                visited_headers(replayed.headers.headers()),
                message.header_pairs()
            );
            match replayed.headers {
                IngestRowHeaders::Shared(snapshot) => {
                    assert!(
                        message.shares_headers,
                        "the buffer shares only the snapshot a message shared"
                    );
                    assert!(
                        Arc::ptr_eq(snapshot, &message.snapshot),
                        "the buffer keeps the message's shared snapshot rather than a copy"
                    );
                }
                IngestRowHeaders::Borrowed(_) => {
                    assert!(
                        !message.shares_headers,
                        "the buffer keeps a shared snapshot by reference"
                    );
                }
            }
        }
    }

    /// The names the programs read, in the order and with the repetition they name them.
    fn read_names(&self) -> Vec<&'static str> {
        let mut names = Vec::with_capacity(self.read.len());
        for position in &self.read {
            let field = DECLARED_FIELDS
                .get(*position)
                .verified("every read position was drawn below the declared field count");
            names.push(field.name());
        }
        names
    }

    /// Whether each declared field is read, in declared order.
    fn read_fields(&self) -> Vec<bool> {
        let mut read = vec![false; DECLARED_FIELDS.len()];
        for position in &self.read {
            let slot = read
                .get_mut(*position)
                .verified("every read position was drawn below the declared field count");
            *slot = true;
        }
        read
    }

    /// One group of the messages builds exactly the columns of the fields that are read, each once
    /// and in declared order, holding every message's value with its validity, and every message's
    /// headers in order.
    fn assert_projected_exactly(&self) {
        let read = self.read_fields();
        let reads_any = read.contains(&true);
        let projection = IngestMetadataProjection::reading(DECLARED, self.read_names())
            .verified("every read name was drawn from the declaration");
        assert_eq!(projection.reads_any(), reads_any);

        let mut builders = IngestMetadataBuilders::new(projection, self.messages.len());
        for message in &self.messages {
            let mut row = message.row();
            if !reads_any {
                row.fields = &Unvisited;
            }
            builders
                .append(&row)
                .verified("every generated message hands over one value of each declared type");
        }
        let metadata = builders
            .finish()
            .verified("every message was appended as one complete row");
        assert_eq!(metadata.len(), self.messages.len());

        let built = &metadata.columns.integration_fields;
        assert_eq!(built.num_rows(), self.messages.len());
        let mut expected_names = Vec::new();
        for (field, is_read) in DECLARED_FIELDS.iter().zip(&read) {
            if *is_read {
                expected_names.push(field.name());
            }
        }
        let mut built_names = Vec::new();
        for field in built.schema().fields() {
            built_names.push(field.name().clone());
        }
        assert_eq!(built_names, expected_names);

        for (position, (field, is_read)) in DECLARED_FIELDS.iter().zip(&read).enumerate() {
            let column = metadata
                .field_column(field.name())
                .verified("the group's own rows select every column it built");
            let Some(column) = column else {
                assert!(!is_read, "the group builds a column for '{}'", field.name());
                continue;
            };
            assert!(
                is_read,
                "the group builds no column for unread '{}'",
                field.name()
            );
            let mut expected = Vec::with_capacity(self.messages.len());
            for message in &self.messages {
                let value = message
                    .values
                    .get(position)
                    .verified("every message hands over a value for every declared field");
                expected.push(projected(value));
            }
            assert_eq!(column_values(column.as_ref(), *field), expected);
        }

        let mut expected_refusals = Vec::with_capacity(self.messages.len());
        for (row, message) in self.messages.iter().enumerate() {
            let expected = message.refusal(&read);
            assert_eq!(refusal_of(&metadata, row), expected, "row {row}");
            expected_refusals.push(expected);
        }
        self.assert_refusals_follow_a_selection(&metadata, &expected_refusals);

        for (row, message) in self.messages.iter().enumerate() {
            let header_row = metadata
                .header_row(row)
                .verified("the group built one header row for every message");
            let carried = header_row
                .end
                .checked_sub(header_row.start)
                .verified("a header row ends at or after its start");
            assert_eq!(carried, message.headers.len());
            for name in HEADER_NAMES {
                let mut values = Vec::new();
                metadata.visit_header_values(row, name, |value| values.push(value.to_string()));
                assert_eq!(values, message.header_values(name));
            }
        }
    }
}

impl MetadataCase {
    /// Keeping every other row keeps exactly those rows' refusals, each still with its own row, so
    /// a refusal belongs to the message it was handed over for however the rows are selected.
    fn assert_refusals_follow_a_selection(
        &self,
        metadata: &IngestFilterMapMetadata,
        expected: &[Option<ExpectedRefusal>],
    ) {
        let mut keep = arrow_array::builder::BooleanBufferBuilder::new(self.messages.len());
        let mut kept = Vec::new();
        for (row, refusal) in expected.iter().enumerate() {
            let is_kept = row % 2 == 0;
            keep.append(is_kept);
            if is_kept {
                kept.push(refusal.clone());
            }
        }
        let selected = metadata
            .select(&keep.finish())
            .verified("the selection has one bit for every row of the group");
        assert_eq!(selected.len(), kept.len());
        for (row, refusal) in kept.into_iter().enumerate() {
            assert_eq!(refusal_of(&selected, row), refusal, "selected row {row}");
        }
    }
}

/// What a refused message names: the read field whose value its type cannot hold, and the
/// description the message gave of that value.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedRefusal {
    field: &'static str,
    value: String,
}

impl GeneratedMessage {
    /// The refusal of a group whose programs read the fields `read` marks: the first read field,
    /// in declared order, whose value is unrepresentable, or none.
    fn refusal(&self, read: &[bool]) -> Option<ExpectedRefusal> {
        for ((field, is_read), value) in DECLARED_FIELDS.iter().zip(read).zip(&self.values) {
            if !is_read {
                continue;
            }
            if let Value::Unrepresentable { value, .. } = value {
                return Some(ExpectedRefusal {
                    field: field.name(),
                    value: value.clone(),
                });
            }
        }
        None
    }
}

fn refusal_of(metadata: &IngestFilterMapMetadata, row: usize) -> Option<ExpectedRefusal> {
    let refusal = metadata.refusal(row)?;
    Some(ExpectedRefusal {
        field: refusal.field().name(),
        value: refusal.value().to_string(),
    })
}

/// The value a read field's column holds for a message's value: a value its type cannot hold
/// reads as null there, because the message is refused rather than read.
fn projected(value: &Value) -> Value {
    match value {
        Value::Unrepresentable { .. } => Value::Absent,
        other => other.clone(),
    }
}

/// The values a built column holds, read back row by row, `Absent` where it holds a null.
fn column_values(column: &dyn Array, field: IngestorMetadataField) -> Vec<Value> {
    assert_eq!(
        column.data_type(),
        field.arrow_field().data_type(),
        "the column of '{}' has its declared type",
        field.name()
    );
    let mut values = Vec::with_capacity(column.len());
    for row in 0..column.len() {
        values.push(column_value(column, field.ty(), row));
    }
    values
}

fn column_value(column: &dyn Array, ty: IngestorMetadataType, row: usize) -> Value {
    const TYPED: &str = "the column's data type was asserted to be the declared type's";
    if column.is_null(row) {
        return Value::Absent;
    }
    match ty {
        IngestorMetadataType::String => {
            let column = column.as_string_opt::<i32>().verified(TYPED);
            Value::String(column.value(row).to_string())
        }
        IngestorMetadataType::Bytes => {
            let column = column.as_binary_opt::<i32>().verified(TYPED);
            Value::Bytes(column.value(row).to_vec())
        }
        IngestorMetadataType::Bool => {
            let column = column.as_boolean_opt().verified(TYPED);
            Value::Bool(column.value(row))
        }
        IngestorMetadataType::U8 => {
            let column = column.as_primitive_opt::<UInt8Type>().verified(TYPED);
            Value::U8(column.value(row))
        }
        IngestorMetadataType::U16 => {
            let column = column.as_primitive_opt::<UInt16Type>().verified(TYPED);
            Value::U16(column.value(row))
        }
        IngestorMetadataType::I32 => {
            let column = column.as_primitive_opt::<Int32Type>().verified(TYPED);
            Value::I32(column.value(row))
        }
        IngestorMetadataType::I64 => {
            let column = column.as_primitive_opt::<Int64Type>().verified(TYPED);
            Value::I64(column.value(row))
        }
        IngestorMetadataType::U64 => {
            let column = column.as_primitive_opt::<UInt64Type>().verified(TYPED);
            Value::U64(column.value(row))
        }
        IngestorMetadataType::Datetime => {
            let column = column
                .as_primitive_opt::<TimestampNanosecondType>()
                .verified(TYPED);
            Value::Datetime(Timestamp::from_unix_nanos(column.value(row)))
        }
    }
}

#[test]
fn bolero_declared_metadata_rows_project_and_retain_exactly() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|bytes: &[u8]| {
            let case = Cases {
                entropy: Entropy::new(bytes),
            }
            .case();
            case.assert_retained_exactly();
            case.assert_projected_exactly();
        });
}
