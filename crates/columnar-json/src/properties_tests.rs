//! Generated rows written from typed columns, compared with an independent JSON reference.
//!
//! Layer: test harness.
//!
//! - **Owns.** Generated batches of every supported column type, sliced and nested, with every
//!   null policy and number and bytes encoding, and the reference rendering of their logical values.
//! - **Depends on.** The writer under test, Arrow arrays, `serde_json` for strings and numbers, and
//!   `chrono` for RFC 3339 text.
//! - **Must not know.** Codecs, connector plans or emitters.

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{Field, Schema};
use chrono::{DateTime, SecondsFormat};
use meticulous::ResultExt as _;
use nervix_primitives::sync::StdArc;

use crate::{
    BytesEncoding, FieldNulls, Float32Encoding, JsonColumnSpec, JsonColumns, JsonWriteError,
    NestedNulls,
};

/// The characters generated text is drawn from: JSON's own syntax and escapes, control characters,
/// multi-byte characters and plain letters, so most strings mix clean runs and escapes.
const ALPHABET: [&str; 16] = [
    "a", "Z", " ", "\"", "\\", "\n", "\t", "\u{1}", "\u{1f}", "\u{7f}", "é", "ключ", "😀", "/",
    "0", "}",
];

/// A scalar Arrow type the writer reads.
#[derive(Debug, Clone, Copy, bolero::TypeGenerator)]
enum ScalarKind {
    Bool,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    Text,
    Bytes,
    Timestamp,
}

/// The shape of a generated column.
#[derive(Debug, Clone, Copy, bolero::TypeGenerator)]
enum ColumnKind {
    Scalar(ScalarKind),
    List(ScalarKind),
    FixedList { kind: ScalarKind, size: u8 },
    NestedList(ScalarKind),
}

/// One generated value: numbers, booleans and instants read `raw`, and text and bytes read `text`.
#[derive(Debug, Clone, bolero::TypeGenerator)]
struct Scalar {
    raw: u64,
    #[generator(bolero::generator::produce_with::<Vec<u8>>().len(0_usize..=8))]
    text: Vec<u8>,
}

/// A generated value that is null one time in eight, so most rows are written whole and a null
/// still reaches every null policy.
#[derive(Debug, Clone)]
struct Mostly<T>(Option<T>);

impl<T: bolero::generator::TypeGenerator> bolero::generator::TypeGenerator for Mostly<T> {
    fn generate<D: bolero::generator::bolero_generator::Driver>(driver: &mut D) -> Option<Self> {
        let draw = u8::generate(driver)?;
        let value = T::generate(driver)?;
        if draw < 32 {
            return Some(Self(None));
        }
        Some(Self(Some(value)))
    }
}

impl<T> Mostly<T> {
    fn get(&self) -> Option<&T> {
        self.0.as_ref()
    }
}

type Elements = Vec<Mostly<Scalar>>;

/// One generated cell, holding a value of every shape; the column's kind decides which it reads.
#[derive(Debug, Clone, bolero::TypeGenerator)]
struct Cell {
    scalar: Scalar,
    #[generator(bolero::generator::produce_with::<Elements>().len(0_usize..=3))]
    elements: Elements,
    #[generator(bolero::generator::produce_with::<Vec<Mostly<Elements>>>().len(0_usize..=2))]
    nested: Vec<Mostly<Elements>>,
}

#[derive(Debug, bolero::TypeGenerator)]
struct FieldCase {
    #[generator(bolero::generator::produce_with::<Vec<u8>>().len(0_usize..=6))]
    name: Vec<u8>,
    kind: ColumnKind,
    nulls: u8,
    widened: bool,
    octets: bool,
    /// Cells before the selected rows, which the column is sliced past.
    #[generator(bolero::generator::produce_with::<Vec<Mostly<Cell>>>().len(0_usize..=2))]
    before: Vec<Mostly<Cell>>,
    /// The cells of the selected rows, repeated as often as the batch has rows.
    #[generator(bolero::generator::produce_with::<Vec<Mostly<Cell>>>().len(1_usize..=4))]
    cells: Vec<Mostly<Cell>>,
}

/// A generated batch: up to four rows of every field, each field sliced past its own leading
/// cells, and the nested null policy of the whole row.
#[derive(Debug, bolero::TypeGenerator)]
struct BatchCase {
    rows: u8,
    write_nested_nulls: bool,
    #[generator(bolero::generator::produce_with::<Vec<FieldCase>>().len(1_usize..=5))]
    fields: Vec<FieldCase>,
}

fn text(bytes: &[u8]) -> String {
    let mut text = String::new();
    for byte in bytes {
        text.push_str(ALPHABET[usize::from(byte % 16)]);
    }
    text
}

impl Scalar {
    fn narrow_bits(&self) -> u32 {
        u32::try_from(self.raw & u64::from(u32::MAX)).assured("the low half of a u64 fits u32")
    }

    fn bytes(&self) -> Vec<u8> {
        self.text.clone()
    }

    fn text(&self) -> String {
        text(&self.text)
    }
}

/// The text a row is expected to be: exact bytes, and float numbers whose exact digits are the
/// writer's choice. A float must be a JSON number of at most the significant digits its type needs
/// that reads back as exactly its bits.
#[derive(Debug, Default)]
struct Rendering {
    segments: Vec<Segment>,
}

#[derive(Debug)]
enum Segment {
    Bytes(Vec<u8>),
    Wide(f64),
    Narrow(f32),
}

impl Rendering {
    fn bytes(&mut self, bytes: &[u8]) {
        match self.segments.last_mut() {
            Some(Segment::Bytes(last)) => last.extend_from_slice(bytes),
            _ => self.segments.push(Segment::Bytes(bytes.to_vec())),
        }
    }

    fn wide(&mut self, value: f64) {
        if value.is_finite() {
            self.segments.push(Segment::Wide(value));
        } else {
            self.bytes(b"null");
        }
    }

    fn narrow(&mut self, value: f32) {
        if value.is_finite() {
            self.segments.push(Segment::Narrow(value));
        } else {
            self.bytes(b"null");
        }
    }

    fn append(&mut self, other: Self) {
        for segment in other.segments {
            match segment {
                Segment::Bytes(bytes) => self.bytes(&bytes),
                Segment::Wide(value) => self.segments.push(Segment::Wide(value)),
                Segment::Narrow(value) => self.segments.push(Segment::Narrow(value)),
            }
        }
    }

    /// Checks `written` against the rendering, segment by segment.
    fn assert_matches(&self, written: &[u8], row: usize) {
        let shown = String::from_utf8_lossy(written);
        let mut rest = written;
        for segment in &self.segments {
            match segment {
                Segment::Bytes(bytes) => {
                    assert!(
                        rest.starts_with(bytes),
                        "row {row} wrote {shown}, expected {} next",
                        String::from_utf8_lossy(bytes)
                    );
                    rest = &rest[bytes.len()..];
                }
                Segment::Wide(value) => {
                    let token = number_token(rest);
                    let read: f64 = parse_number(token, 17, row);
                    assert_eq!(read.to_bits(), value.to_bits(), "row {row} wrote {shown}");
                    rest = &rest[token.len()..];
                }
                Segment::Narrow(value) => {
                    let token = number_token(rest);
                    let read: f32 = parse_number(token, 9, row);
                    assert_eq!(read.to_bits(), value.to_bits(), "row {row} wrote {shown}");
                    rest = &rest[token.len()..];
                }
            }
        }
        assert!(
            rest.is_empty(),
            "row {row} wrote {shown} with trailing bytes"
        );
    }
}

/// Whether `text` is one or more ASCII digits.
fn digits_only(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

/// The longest prefix of `text` made of the characters a JSON number holds.
fn number_token(text: &[u8]) -> &[u8] {
    let mut length = 0;
    for byte in text {
        if !matches!(byte, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E') {
            break;
        }
        length += 1;
    }
    &text[..length]
}

/// Reads a JSON number of at most `digits` significant digits, checking its grammar:
/// `-? (0 | [1-9][0-9]*) (. [0-9]+)? ([eE] [+-]? [0-9]+)?`.
fn parse_number<F: std::str::FromStr>(token: &[u8], digits: usize, row: usize) -> F {
    let text = std::str::from_utf8(token).assured("a number token is ASCII");
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, Some(exponent)),
        None => (unsigned, None),
    };
    let (integer, fraction) = match mantissa.split_once('.') {
        Some((integer, fraction)) => (integer, Some(fraction)),
        None => (mantissa, None),
    };
    let integer_valid = integer == "0" || (digits_only(integer) && !integer.starts_with('0'));
    let fraction_valid = match fraction {
        Some(fraction) => digits_only(fraction),
        None => true,
    };
    let exponent_valid = match exponent {
        Some(exponent) => digits_only(exponent.strip_prefix(['+', '-']).unwrap_or(exponent)),
        None => true,
    };
    assert!(
        integer_valid && fraction_valid && exponent_valid,
        "row {row} wrote the number {text}, which JSON does not read"
    );
    // The significant digits run from the first nonzero digit of the mantissa to its last; a
    // zero counts only once a nonzero digit follows it.
    let mut significant = 0;
    let mut zeros_since_nonzero = 0;
    let mut started = false;
    for byte in mantissa.bytes() {
        if byte == b'.' {
            continue;
        }
        if byte == b'0' {
            if started {
                zeros_since_nonzero += 1;
            }
            continue;
        }
        started = true;
        significant += zeros_since_nonzero + 1;
        zeros_since_nonzero = 0;
    }
    assert!(
        significant <= digits.max(1),
        "row {row} wrote {text} with more digits than its type needs"
    );
    let Ok(number) = text.parse() else {
        panic!("row {row} wrote the number {text}, which does not read back");
    };
    number
}

/// A column of `values`, each present one read by `read`.
fn typed_column<T, A>(values: &[Option<&Scalar>], read: impl Fn(&Scalar) -> T) -> A
where
    A: FromIterator<Option<T>>,
{
    let mut cells = Vec::with_capacity(values.len());
    for value in values {
        match value {
            Some(value) => cells.push(Some(read(value))),
            None => cells.push(None),
        }
    }
    cells.into_iter().collect()
}

/// The low bytes of a generated value, read as an integer of type `N`.
macro_rules! low_bytes {
    ($native:ty, $raw:expr) => {{
        let bytes = $raw.to_le_bytes();
        let (low, _) = bytes.split_at(size_of::<$native>());
        <$native>::from_le_bytes(
            low.try_into()
                .assured("the slice has the type's byte width"),
        )
    }};
}

impl ScalarKind {
    /// A column of `values`.
    fn array(self, values: &[Option<&Scalar>]) -> ArrayRef {
        match self {
            Self::Bool => StdArc::new(typed_column::<_, BooleanArray>(values, |value| {
                value.raw & 1 == 1
            })),
            Self::U8 => StdArc::new(typed_column::<_, UInt8Array>(values, |value| {
                low_bytes!(u8, value.raw)
            })),
            Self::I8 => StdArc::new(typed_column::<_, Int8Array>(values, |value| {
                low_bytes!(i8, value.raw)
            })),
            Self::U16 => StdArc::new(typed_column::<_, UInt16Array>(values, |value| {
                low_bytes!(u16, value.raw)
            })),
            Self::I16 => StdArc::new(typed_column::<_, Int16Array>(values, |value| {
                low_bytes!(i16, value.raw)
            })),
            Self::U32 => StdArc::new(typed_column::<_, UInt32Array>(values, |value| {
                low_bytes!(u32, value.raw)
            })),
            Self::I32 => StdArc::new(typed_column::<_, Int32Array>(values, |value| {
                low_bytes!(i32, value.raw)
            })),
            Self::U64 => StdArc::new(typed_column::<_, UInt64Array>(values, |value| value.raw)),
            Self::I64 => StdArc::new(typed_column::<_, Int64Array>(values, |value| {
                value.raw.cast_signed()
            })),
            Self::F32 => StdArc::new(typed_column::<_, Float32Array>(values, |value| {
                f32::from_bits(value.narrow_bits())
            })),
            Self::F64 => StdArc::new(typed_column::<_, Float64Array>(values, |value| {
                f64::from_bits(value.raw)
            })),
            Self::Text => StdArc::new(typed_column::<_, StringArray>(values, Scalar::text)),
            Self::Bytes => StdArc::new(typed_column::<_, BinaryArray>(values, Scalar::bytes)),
            Self::Timestamp => StdArc::new(typed_column::<_, TimestampNanosecondArray>(
                values,
                |value| value.raw.cast_signed(),
            )),
        }
    }

    /// The JSON text of a present value, written without the writer: numbers as JSON numbers
    /// that read back as their exact bits, non-finite floats as null, text escaped by
    /// `serde_json`, bytes as padded base64 or as escaped octets, and instants as RFC 3339 in UTC.
    fn reference(self, value: &Scalar, spec: &FieldCase, rendering: &mut Rendering) {
        macro_rules! integer {
            ($native:ty) => {{
                let integer = low_bytes!($native, value.raw);
                rendering.bytes(integer.to_string().as_bytes());
            }};
        }
        match self {
            Self::Bool => {
                let text: &[u8] = if value.raw & 1 == 1 {
                    b"true"
                } else {
                    b"false"
                };
                rendering.bytes(text);
            }
            Self::U8 => integer!(u8),
            Self::I8 => integer!(i8),
            Self::U16 => integer!(u16),
            Self::I16 => integer!(i16),
            Self::U32 => integer!(u32),
            Self::I32 => integer!(i32),
            Self::U64 => integer!(u64),
            Self::I64 => integer!(i64),
            Self::F32 => {
                let number = f32::from_bits(value.narrow_bits());
                if spec.widened {
                    rendering.wide(f64::from(number));
                } else {
                    rendering.narrow(number);
                }
            }
            Self::F64 => rendering.wide(f64::from_bits(value.raw)),
            Self::Text => {
                let text = serde_json::to_vec(&value.text()).assured("a string serializes");
                rendering.bytes(&text);
            }
            Self::Bytes => {
                let mut written = vec![b'"'];
                if spec.octets {
                    escaped_octets(&value.bytes(), &mut written);
                } else {
                    reference_base64(&value.bytes(), &mut written);
                }
                written.push(b'"');
                rendering.bytes(&written);
            }
            Self::Timestamp => {
                let instant = DateTime::from_timestamp_nanos(value.raw.cast_signed());
                let text = instant.to_rfc3339_opts(SecondsFormat::AutoSi, false);
                let text = serde_json::to_vec(&text).assured("a string serializes");
                rendering.bytes(&text);
            }
        }
    }
}

/// Padded standard base64, one group of three octets at a time.
fn reference_base64(octets: &[u8], written: &mut Vec<u8>) {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for group in octets.chunks(3) {
        let mut word = 0_u32;
        for (index, octet) in group.iter().enumerate() {
            word |= u32::from(*octet) << (16 - 8 * index);
        }
        for sextet in 0..4 {
            if sextet <= group.len() {
                let index = (word >> (18 - 6 * sextet)) & 0x3f;
                let index = usize::try_from(index).assured("a sextet fits usize");
                written.push(ALPHABET[index]);
            } else {
                written.push(b'=');
            }
        }
    }
}

/// Each octet as itself, except the quote, the backslash and the control octets, which are
/// escaped so a JSON reader yields exactly that octet.
fn escaped_octets(octets: &[u8], written: &mut Vec<u8>) {
    for octet in octets {
        match *octet {
            b'"' => written.extend_from_slice(b"\\\""),
            b'\\' => written.extend_from_slice(b"\\\\"),
            0x00..=0x1f => written.extend_from_slice(format!("\\u{octet:04x}").as_bytes()),
            other => written.push(other),
        }
    }
}

/// A null the writer must refuse: which field held it, and for a top-level null the row.
#[derive(Debug, PartialEq)]
struct RequiredNull {
    field: String,
    row: Option<usize>,
}

impl FieldCase {
    fn name(&self) -> String {
        text(&self.name)
    }

    fn nulls(&self) -> FieldNulls {
        match self.nulls % 3 {
            0 => FieldNulls::Omit,
            1 => FieldNulls::Reject,
            _ => FieldNulls::Write,
        }
    }

    fn spec(&self) -> JsonColumnSpec {
        let float32 = if self.widened {
            Float32Encoding::WidenedF64
        } else {
            Float32Encoding::Native
        };
        let bytes = if self.octets {
            BytesEncoding::Octets
        } else {
            BytesEncoding::Base64
        };
        JsonColumnSpec::new(&self.name(), self.nulls())
            .with_float32_encoding(float32)
            .with_bytes_encoding(bytes)
    }

    fn fixed_size(size: u8) -> usize {
        usize::from(size % 4)
    }

    /// The column of `cells`, sliced past this field's leading cells.
    fn column(&self, cells: &[Option<&Cell>]) -> ArrayRef {
        let mut all = Vec::with_capacity(self.before.len() + cells.len());
        for cell in &self.before {
            all.push(cell.get());
        }
        all.extend_from_slice(cells);
        let array = match self.kind {
            ColumnKind::Scalar(kind) => {
                let mut values = Vec::with_capacity(all.len());
                for cell in &all {
                    match cell {
                        Some(cell) => values.push(Some(&cell.scalar)),
                        None => values.push(None),
                    }
                }
                kind.array(&values)
            }
            ColumnKind::List(kind) => list_array(kind, &all),
            ColumnKind::FixedList { kind, size } => {
                fixed_list_array(kind, Self::fixed_size(size), &all)
            }
            ColumnKind::NestedList(kind) => nested_list_array(kind, &all),
        };
        array.slice(self.before.len(), cells.len())
    }

    /// The JSON of a present cell, or the null the writer refuses inside it.
    fn reference_value(
        &self,
        cell: &Cell,
        nested_nulls: NestedNulls,
    ) -> Result<Rendering, RequiredNull> {
        let mut rendering = Rendering::default();
        match self.kind {
            ColumnKind::Scalar(kind) => kind.reference(&cell.scalar, self, &mut rendering),
            ColumnKind::List(kind) => {
                let elements = cell.elements.iter().map(Mostly::get);
                self.reference_list(kind, elements, nested_nulls, &mut rendering)?;
            }
            ColumnKind::FixedList { kind, size } => {
                let elements = fixed_elements(cell, Self::fixed_size(size));
                self.reference_list(kind, elements, nested_nulls, &mut rendering)?;
            }
            ColumnKind::NestedList(kind) => {
                rendering.bytes(b"[");
                for (index, inner) in cell.nested.iter().enumerate() {
                    if index != 0 {
                        rendering.bytes(b",");
                    }
                    match inner.get() {
                        Some(inner) => {
                            let elements = inner.iter().map(Mostly::get);
                            self.reference_list(kind, elements, nested_nulls, &mut rendering)?;
                        }
                        None => self.nested_null(nested_nulls, &mut rendering)?,
                    }
                }
                rendering.bytes(b"]");
            }
        }
        Ok(rendering)
    }

    fn reference_list<'a>(
        &self,
        kind: ScalarKind,
        elements: impl IntoIterator<Item = Option<&'a Scalar>>,
        nested_nulls: NestedNulls,
        rendering: &mut Rendering,
    ) -> Result<(), RequiredNull> {
        rendering.bytes(b"[");
        for (index, element) in elements.into_iter().enumerate() {
            if index != 0 {
                rendering.bytes(b",");
            }
            match element {
                Some(element) => kind.reference(element, self, rendering),
                None => self.nested_null(nested_nulls, rendering)?,
            }
        }
        rendering.bytes(b"]");
        Ok(())
    }

    fn nested_null(
        &self,
        nested_nulls: NestedNulls,
        rendering: &mut Rendering,
    ) -> Result<(), RequiredNull> {
        match nested_nulls {
            NestedNulls::Write => {
                rendering.bytes(b"null");
                Ok(())
            }
            NestedNulls::Reject => Err(RequiredNull {
                field: self.name(),
                row: None,
            }),
        }
    }
}

/// The elements a fixed-size list cell holds: its first `size` elements, padded with nulls.
fn fixed_elements(cell: &Cell, size: usize) -> Vec<Option<&Scalar>> {
    let mut elements = Vec::with_capacity(size);
    for index in 0..size {
        match cell.elements.get(index) {
            Some(element) => elements.push(element.get()),
            None => elements.push(None),
        }
    }
    elements
}

fn element_field(kind: ScalarKind) -> StdArc<Field> {
    let data_type = kind.array(&[]).data_type().clone();
    StdArc::new(Field::new("item", data_type, true))
}

/// A list column whose rows are the elements of each present cell.
fn list_array(kind: ScalarKind, cells: &[Option<&Cell>]) -> ArrayRef {
    let mut values = Vec::new();
    let mut lengths = Vec::with_capacity(cells.len());
    for cell in cells {
        match cell {
            Some(cell) => {
                values.extend(cell.elements.iter().map(Mostly::get));
                lengths.push(cell.elements.len());
            }
            None => lengths.push(0),
        }
    }
    let child = kind.array(&values);
    let nulls = NullBuffer::from_iter(cells.iter().map(Option::is_some));
    let list = ListArray::try_new(
        element_field(kind),
        OffsetBuffer::from_lengths(lengths),
        child,
        Some(nulls),
    )
    .assured("the offsets were built from the child's own lengths");
    StdArc::new(list)
}

fn fixed_list_array(kind: ScalarKind, size: usize, cells: &[Option<&Cell>]) -> ArrayRef {
    let mut rows = Vec::with_capacity(cells.len());
    for cell in cells {
        match cell {
            Some(cell) => rows.push(fixed_elements(cell, size)),
            None => rows.push(vec![None; size]),
        }
    }
    let mut values = Vec::with_capacity(rows.len() * size);
    for row in &rows {
        values.extend_from_slice(row);
    }
    let child = kind.array(&values);
    let nulls = NullBuffer::from_iter(cells.iter().map(Option::is_some));
    let size = i32::try_from(size).assured("a fixed size is below four");
    let list = FixedSizeListArray::try_new(element_field(kind), size, child, Some(nulls))
        .assured("the child holds exactly size values for every row");
    StdArc::new(list)
}

fn nested_list_array(kind: ScalarKind, cells: &[Option<&Cell>]) -> ArrayRef {
    let mut inner_lists = Vec::new();
    let mut lengths = Vec::with_capacity(cells.len());
    for cell in cells {
        match cell {
            Some(cell) => {
                inner_lists.extend(cell.nested.iter().map(Mostly::get));
                lengths.push(cell.nested.len());
            }
            None => lengths.push(0),
        }
    }
    let mut inner_values = Vec::new();
    let mut inner_lengths = Vec::with_capacity(inner_lists.len());
    for inner in &inner_lists {
        match inner {
            Some(inner) => {
                inner_values.extend(inner.iter().map(Mostly::get));
                inner_lengths.push(inner.len());
            }
            None => inner_lengths.push(0),
        }
    }
    let inner_child = kind.array(&inner_values);
    let inner_nulls = NullBuffer::from_iter(inner_lists.iter().map(Option::is_some));
    let inner = ListArray::try_new(
        element_field(kind),
        OffsetBuffer::from_lengths(inner_lengths),
        inner_child,
        Some(inner_nulls),
    )
    .assured("the offsets were built from the child's own lengths");
    let inner_field = StdArc::new(Field::new("item", inner.data_type().clone(), true));
    let nulls = NullBuffer::from_iter(cells.iter().map(Option::is_some));
    let outer = ListArray::try_new(
        inner_field,
        OffsetBuffer::from_lengths(lengths),
        StdArc::new(inner),
        Some(nulls),
    )
    .assured("the offsets were built from the child's own lengths");
    StdArc::new(outer)
}

impl BatchCase {
    fn rows(&self) -> usize {
        usize::from(self.rows % 5)
    }

    /// The cell of `field` in `row`: the field's generated cells, repeated.
    fn cell(&self, row: usize, field: usize) -> Option<&Cell> {
        let cells = &self.fields[field].cells;
        cells[row % cells.len()].get()
    }

    fn nested_nulls(&self) -> NestedNulls {
        if self.write_nested_nulls {
            NestedNulls::Write
        } else {
            NestedNulls::Reject
        }
    }

    fn batch(&self) -> RecordBatch {
        let mut fields = Vec::with_capacity(self.fields.len());
        let mut columns = Vec::with_capacity(self.fields.len());
        for (index, field) in self.fields.iter().enumerate() {
            let cells = (0..self.rows())
                .map(|row| self.cell(row, index))
                .collect::<Vec<_>>();
            let column = field.column(&cells);
            fields.push(Field::new(
                format!("column_{index}"),
                column.data_type().clone(),
                true,
            ));
            columns.push(column);
        }
        RecordBatch::try_new(StdArc::new(Schema::new(fields)), columns)
            .assured("every generated column has one cell per row")
    }

    /// The object one row writes, or the first null the writer refuses in it.
    fn reference_row(&self, row: usize) -> Result<Rendering, RequiredNull> {
        let mut rendering = Rendering::default();
        rendering.bytes(b"{");
        let mut first = true;
        for (index, field) in self.fields.iter().enumerate() {
            let value = match self.cell(row, index) {
                Some(cell) => field.reference_value(cell, self.nested_nulls())?,
                None => match field.nulls() {
                    FieldNulls::Omit => continue,
                    FieldNulls::Reject => {
                        return Err(RequiredNull {
                            field: field.name(),
                            row: Some(row),
                        });
                    }
                    FieldNulls::Write => {
                        let mut null = Rendering::default();
                        null.bytes(b"null");
                        null
                    }
                },
            };
            if !first {
                rendering.bytes(b",");
            }
            first = false;
            let key = serde_json::to_vec(&field.name()).assured("a string serializes");
            rendering.bytes(&key);
            rendering.bytes(b":");
            rendering.append(value);
        }
        rendering.bytes(b"}");
        Ok(rendering)
    }
}

#[test]
fn bolero_rows_match_an_independent_json_rendering_of_their_values() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(8192)
        .with_type::<BatchCase>()
        .for_each(|case| {
            let batch = case.batch();
            let specs = case.fields.iter().map(FieldCase::spec).collect::<Vec<_>>();
            let columns = JsonColumns::new(&batch, &specs, case.nested_nulls())
                .assured("every generated column type is supported");
            for row in 0..case.rows() {
                let mut written = Vec::new();
                let outcome = columns.write_row(row, &mut written);
                match (outcome, case.reference_row(row)) {
                    (Ok(()), Ok(expected)) => expected.assert_matches(&written, row),
                    (Err(report), Err(expected)) => {
                        let JsonWriteError::RequiredNull { field, row: at } =
                            report.current_context()
                        else {
                            panic!("row {row} failed with {report:?}, expected {expected:?}");
                        };
                        assert_eq!(field, &expected.field, "row {row}");
                        if let Some(expected_row) = expected.row {
                            assert_eq!(*at, expected_row);
                        }
                    }
                    (outcome, expected) => {
                        panic!("row {row} wrote {outcome:?}, expected {expected:?}")
                    }
                }
            }
            let rows = case.rows();
            let Err(outside) = columns.write_row(rows, &mut Vec::new()) else {
                panic!("the row past the last of {rows} was written");
            };
            let JsonWriteError::RowOutOfBounds {
                row,
                rows: batch_rows,
            } = outside.current_context()
            else {
                panic!("the row past the last failed with {outside:?}");
            };
            assert_eq!(*row, rows);
            assert_eq!(*batch_rows, rows);
        });
}
