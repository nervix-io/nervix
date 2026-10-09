//! Bounded Arrow batches of every current schema type, built from the bytes a property receives,
//! and the logical oracle the Arrow representation properties compare batches with.
//!
//! Layer: test harness.
//!
//! - **Owns.** Generated schemas over every current field type, valid Arrow columns for them with
//!   nulls, nesting, empty, single and multiple rows, views cut from a larger batch and boundary
//!   values, the cell-by-cell logical oracle that compares two batches with floats by their bits,
//!   and the damage properties apply to a valid encoding.
//! - **Depends on.** Arrow, the vocabulary's schema types and their Arrow mapping, the compiled
//!   schema and the vocabulary generators.
//! - **Must not know.** Codecs, relays, the interconnect or any other carrier a property sends a
//!   batch through.
//!
//! A batch is built column by column from typed values, never from rows. Every column a property
//! sends is the Arrow array the generator built, shared by reference: a view keeps the offsets of
//! the larger batch it was cut from, so a carrier meets columns whose data does not start at zero.

use std::num::NonZeroU32;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Entropy};
use nervix_models::{CreateSchema, FieldName, ParseAsType, SchemaField, SchemaName};
use nervix_primitives::sync::{Arc, StdArc};

use super::{CompiledSchema, RuntimeRecordBatch, compile_schema};

/// The most fields a generated schema declares. A schema declares at least one.
const FIELDS: usize = 4;

/// The most rows a generated view holds.
const ROWS: usize = 5;

/// The most rows the batch a view is cut from holds before it and after it.
const MARGIN: usize = 2;

/// The most elements one generated `VEC` value holds.
const ELEMENTS: usize = 3;

/// The longest generated `ARRAY`.
const ARRAY_LENGTH: u32 = 3;

/// How many collection levels a generated type nests.
const DEPTH: u8 = 2;

/// The most octets one generated `BYTES` value holds.
const OCTETS: usize = 24;

/// The values one carrier promises to preserve, which bounds what a property generates for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GeneratedDomain {
    /// Every value an Arrow batch of the current schema types holds: Arrow IPC bodies, producer
    /// batches and remote relay payloads.
    Arrow,
    /// The values the schemaful `JSON` and `CBOR` codecs promise to preserve. A non-finite float
    /// has no JSON spelling, and the CBOR reader reads values through the same JSON model.
    Json,
    /// The values the schemaful `AVRO` codec promises to preserve. A top-level field holds only a
    /// type an Avro wire type binds, a field name is an Avro name, and an unsigned element is
    /// written as an Avro `long`, which holds at most `i64::MAX`.
    Avro,
}

impl GeneratedDomain {
    /// Whether a schema field in this domain may declare `ty` at its top level.
    fn admits_field_type(self, ty: &ParseAsType) -> bool {
        match self {
            Self::Arrow | Self::Json => true,
            Self::Avro => matches!(
                ty,
                ParseAsType::Bool
                    | ParseAsType::I32
                    | ParseAsType::I64
                    | ParseAsType::F32
                    | ParseAsType::F64
                    | ParseAsType::String
                    | ParseAsType::Datetime
                    | ParseAsType::Bytes
                    | ParseAsType::Array { .. }
                    | ParseAsType::Vec { .. }
            ),
        }
    }

    /// Whether a float in this domain may hold a non-finite bit pattern.
    fn admits_non_finite_floats(self) -> bool {
        match self {
            Self::Arrow | Self::Avro => true,
            Self::Json => false,
        }
    }

    /// The largest `U64` value this domain holds.
    fn largest_u64(self) -> u64 {
        match self {
            Self::Arrow | Self::Json => u64::MAX,
            Self::Avro => u64::try_from(i64::MAX).assured("i64::MAX is positive"),
        }
    }

    /// A schema of one to [`FIELDS`] distinctly named fields of this domain's types.
    pub(crate) fn schema(self, arbitrary: &mut Arbitrary<'_>) -> GeneratedSchema {
        let wanted = arbitrary
            .entropy()
            .count(FIELDS.checked_sub(1).assured("a schema declares fields"))
            .checked_add(1)
            .verified("the count is below the field bound");
        let mut fields: Vec<SchemaField> = Vec::with_capacity(wanted);
        while fields.len() < wanted {
            let name = self.field_name(arbitrary, &fields);
            let ty = self.field_type(arbitrary);
            fields.push(SchemaField {
                name,
                ty,
                optional: arbitrary.entropy().flag(),
                sensitive: arbitrary.entropy().flag(),
            });
        }
        GeneratedSchema::new(CreateSchema {
            name: SchemaName::parse("generated").assured("the schema name is a literal name"),
            fields,
        })
    }

    /// A field name no field in `fields` has. JSON and CBOR keys hold a name of the whole name
    /// rule; an Avro field holds an Avro name.
    fn field_name(self, arbitrary: &mut Arbitrary<'_>, fields: &[SchemaField]) -> FieldName {
        let drawn = match self {
            Self::Arrow | Self::Json => arbitrary.rule_name::<FieldName>(),
            Self::Avro => arbitrary.name::<FieldName>(),
        };
        let taken = fields.iter().any(|field| field.name == drawn);
        if !taken {
            return drawn;
        }
        // A repeat is rare; the position of the new field names it uniquely, because every
        // earlier fallback was taken at a smaller position.
        let mut text = format!("field_{}", fields.len());
        while fields.iter().any(|field| field.name.as_str() == text) {
            text.push('_');
        }
        FieldName::parse(&text).assured("a field name of letters, digits and underscores is valid")
    }

    /// A top-level field type of this domain.
    fn field_type(self, arbitrary: &mut Arbitrary<'_>) -> ParseAsType {
        let ty = generated_type(arbitrary.entropy(), DEPTH);
        if self.admits_field_type(&ty) {
            return ty;
        }
        // An Avro schema declares no narrow or unsigned top-level integer: the draw becomes the
        // signed integer an Avro `int` or `long` binds.
        match ty {
            ParseAsType::U8 | ParseAsType::I8 | ParseAsType::U16 | ParseAsType::I16 => {
                ParseAsType::I32
            }
            _ => ParseAsType::I64,
        }
    }

    /// A batch of `schema` holding zero to [`ROWS`] rows, cut from a batch with up to [`MARGIN`]
    /// rows before and after the rows a carrier receives.
    pub(crate) fn batch(
        self,
        arbitrary: &mut Arbitrary<'_>,
        schema: &GeneratedSchema,
    ) -> RecordBatch {
        let rows = BatchRows::draw(arbitrary.entropy());
        self.batch_of(arbitrary, schema, rows)
    }

    /// A batch of `schema` holding the rows `shape` counts, their values drawn from `arbitrary`.
    pub(crate) fn batch_of(
        self,
        arbitrary: &mut Arbitrary<'_>,
        schema: &GeneratedSchema,
        shape: BatchRows,
    ) -> RecordBatch {
        let BatchRows {
            before,
            rows,
            after,
        } = shape;
        let through_view = before.checked_add(rows).verified("two small counts add up");
        let total = through_view
            .checked_add(after)
            .verified("three small counts add up");
        let mut columns = Vec::with_capacity(schema.model.fields.len());
        for field in &schema.model.fields {
            let mut slots = Vec::with_capacity(total);
            for _ in 0..total {
                let present = !field.optional || arbitrary.entropy().byte() % 4 != 0;
                slots.push(if present { Slot::Valid } else { Slot::Null });
            }
            columns.push(self.column(arbitrary, &field.ty, &slots));
        }
        let whole = RecordBatch::try_new(schema.compiled.arrow_schema(), columns)
            .assured("generated columns hold one value of their declared type per row");
        whole.slice(before, rows)
    }

    /// One column of `ty` with one value for every slot.
    fn column(self, arbitrary: &mut Arbitrary<'_>, ty: &ParseAsType, slots: &[Slot]) -> ArrayRef {
        macro_rules! primitive {
            ($array:ty, $value:expr) => {{
                let mut values = Vec::with_capacity(slots.len());
                for slot in slots {
                    let present = slot.holds_value(arbitrary.entropy());
                    values.push(if present { Some($value) } else { None });
                }
                StdArc::new(<$array>::from(values))
            }};
        }
        match ty {
            ParseAsType::U8 => primitive!(UInt8Array, unsigned::<u8>(arbitrary.entropy())),
            ParseAsType::U16 => primitive!(UInt16Array, unsigned::<u16>(arbitrary.entropy())),
            ParseAsType::U32 => primitive!(UInt32Array, unsigned::<u32>(arbitrary.entropy())),
            ParseAsType::U64 => primitive!(
                UInt64Array,
                arbitrary.entropy().boundary_biased(0..=self.largest_u64())
            ),
            ParseAsType::I8 => primitive!(Int8Array, signed::<i8>(arbitrary.entropy())),
            ParseAsType::I16 => primitive!(Int16Array, signed::<i16>(arbitrary.entropy())),
            ParseAsType::I32 => primitive!(Int32Array, signed::<i32>(arbitrary.entropy())),
            ParseAsType::I64 => primitive!(Int64Array, signed::<i64>(arbitrary.entropy())),
            ParseAsType::F32 => primitive!(Float32Array, self.float32(arbitrary.entropy())),
            ParseAsType::F64 => primitive!(Float64Array, self.float64(arbitrary.entropy())),
            ParseAsType::Bool => primitive!(BooleanArray, arbitrary.entropy().flag()),
            ParseAsType::String => {
                let mut values = Vec::with_capacity(slots.len());
                for slot in slots {
                    let present = slot.holds_value(arbitrary.entropy());
                    values.push(if present {
                        Some(arbitrary.string())
                    } else {
                        None
                    });
                }
                StdArc::new(StringArray::from(values))
            }
            ParseAsType::Bytes => {
                let mut values = Vec::with_capacity(slots.len());
                for slot in slots {
                    let present = slot.holds_value(arbitrary.entropy());
                    values.push(if present {
                        Some(octets(arbitrary.entropy()))
                    } else {
                        None
                    });
                }
                StdArc::new(BinaryArray::from_iter(values))
            }
            ParseAsType::Datetime => {
                let mut values = Vec::with_capacity(slots.len());
                for slot in slots {
                    let present = slot.holds_value(arbitrary.entropy());
                    values.push(if present {
                        Some(signed::<i64>(arbitrary.entropy()))
                    } else {
                        None
                    });
                }
                StdArc::new(TimestampNanosecondArray::from(values).with_timezone("+00:00"))
            }
            ParseAsType::Array { element, len } => {
                self.fixed_list(arbitrary, ty, element, *len, slots)
            }
            ParseAsType::Vec { element } => self.list(arbitrary, ty, element, slots),
        }
    }

    /// A fixed-size list column. A null value still owns its `len` elements, which Arrow allows to
    /// be null because the null above them masks them.
    fn fixed_list(
        self,
        arbitrary: &mut Arbitrary<'_>,
        ty: &ParseAsType,
        element: &ParseAsType,
        len: NonZeroU32,
        slots: &[Slot],
    ) -> ArrayRef {
        let length = usize::try_from(len.get()).assured("a generated array length fits in usize");
        let mut validity = Vec::with_capacity(slots.len());
        let mut element_slots = Vec::new();
        for slot in slots {
            let present = slot.holds_value(arbitrary.entropy());
            validity.push(present);
            let element_slot = if present { Slot::Valid } else { Slot::Masked };
            element_slots.extend(std::iter::repeat_n(element_slot, length));
        }
        let elements = self.column(arbitrary, element, &element_slots);
        let DataType::FixedSizeList(field, size) = ty.arrow_data_type() else {
            unreachable!("an ARRAY is carried as an Arrow fixed-size list")
        };
        StdArc::new(
            FixedSizeListArray::try_new(field, size, elements, null_buffer(validity))
                .assured("every value owns exactly `len` elements of the element type"),
        )
    }

    /// A variable-length list column. A null value may still own elements, which Arrow allows and
    /// no reader of the value sees.
    fn list(
        self,
        arbitrary: &mut Arbitrary<'_>,
        ty: &ParseAsType,
        element: &ParseAsType,
        slots: &[Slot],
    ) -> ArrayRef {
        let mut validity = Vec::with_capacity(slots.len());
        let mut offsets = Vec::with_capacity(slots.len().checked_add(1).assured("a small count"));
        offsets.push(0_i32);
        let mut elements = 0_usize;
        for slot in slots {
            let present = slot.holds_value(arbitrary.entropy());
            validity.push(present);
            let count = if present {
                arbitrary.entropy().count(ELEMENTS)
            } else {
                arbitrary.entropy().count(1)
            };
            elements = elements
                .checked_add(count)
                .verified("a bounded number of bounded lists fits in usize");
            offsets.push(i32::try_from(elements).verified("a bounded element count fits in i32"));
        }
        // A list element is never null, under a null list value as anywhere else.
        let element_slots = vec![Slot::Valid; elements];
        let values = self.column(arbitrary, element, &element_slots);
        let DataType::List(field) = ty.arrow_data_type() else {
            unreachable!("a VEC is carried as an Arrow list")
        };
        StdArc::new(
            ListArray::try_new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                values,
                null_buffer(validity),
            )
            .assured("generated offsets address the generated elements in order"),
        )
    }

    /// A 32-bit float of any bit pattern this domain holds, landing on the extremes, signed zeros,
    /// subnormals and NaN payloads as often as anywhere else.
    fn float32(self, entropy: &mut Entropy<'_>) -> f32 {
        let random = u32::from_le_bytes(std::array::from_fn(|_| entropy.byte()));
        let bits = entropy.pick([
            random,
            random,
            0x0000_0000,
            0x8000_0000,
            0x0000_0001,
            0x8000_0001,
            0x0080_0000,
            0x7f7f_ffff,
            0xff7f_ffff,
            0x3f80_0000,
            0x7f80_0000,
            0xff80_0000,
            0x7fc0_0000,
            0x7fc0_0001,
            0x7fa0_0001,
            0xffc0_0000,
        ]);
        let value = f32::from_bits(bits);
        if value.is_finite() || self.admits_non_finite_floats() {
            return value;
        }
        // Clearing the exponent's top bit keeps the sign and mantissa of a non-finite pattern and
        // lands on a finite value.
        f32::from_bits(bits & !0x4000_0000)
    }

    /// A 64-bit float of any bit pattern this domain holds, landing on the extremes, signed zeros,
    /// subnormals and NaN payloads as often as anywhere else.
    fn float64(self, entropy: &mut Entropy<'_>) -> f64 {
        let random = u64::from_le_bytes(std::array::from_fn(|_| entropy.byte()));
        let bits = entropy.pick([
            random,
            random,
            0x0000_0000_0000_0000,
            0x8000_0000_0000_0000,
            0x0000_0000_0000_0001,
            0x8000_0000_0000_0001,
            0x0010_0000_0000_0000,
            0x7fef_ffff_ffff_ffff,
            0xffef_ffff_ffff_ffff,
            0x3ff0_0000_0000_0000,
            0x7ff0_0000_0000_0000,
            0xfff0_0000_0000_0000,
            0x7ff8_0000_0000_0000,
            0x7ff8_0000_0000_0001,
            0x7ff4_0000_0000_0001,
            0xfff8_0000_0000_0000,
        ]);
        let value = f64::from_bits(bits);
        if value.is_finite() || self.admits_non_finite_floats() {
            return value;
        }
        f64::from_bits(bits & !0x4000_0000_0000_0000)
    }
}

/// A generated schema Model and the schema compiled from it.
#[derive(Debug, Clone)]
pub(crate) struct GeneratedSchema {
    pub(crate) model: CreateSchema,
    pub(crate) compiled: Arc<CompiledSchema>,
}

impl GeneratedSchema {
    /// `model` and the schema compiled from it.
    pub(crate) fn new(model: CreateSchema) -> Self {
        let compiled = Arc::new(compile_schema(&model));
        Self { model, compiled }
    }

    /// `batch` as the runtime carries it, checked against this schema.
    pub(crate) fn runtime_batch(&self, batch: RecordBatch) -> RuntimeRecordBatch {
        RuntimeRecordBatch::from_record_batch(self.compiled.arrow_schema(), batch)
            .assured("a generated batch carries its compiled schema")
    }
}

/// How many rows a generated batch holds, and how many rows of the larger batch it is cut from lie
/// before and after them. A property whose subject needs rows reads this before the schema, so
/// that an ordinary run's few bytes are not gone before the rows are counted.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchRows {
    before: usize,
    rows: usize,
    after: usize,
}

impl BatchRows {
    pub(crate) fn draw(entropy: &mut Entropy<'_>) -> Self {
        let before = entropy.count(MARGIN);
        let rows = entropy.count(ROWS);
        let after = entropy.count(MARGIN);
        Self {
            before,
            rows,
            after,
        }
    }
}

/// Whether one value of a column is present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    /// The value is present.
    Valid,
    /// The value is a null of an optional field.
    Null,
    /// The value sits under a null fixed-size list value, which masks it, so it may be null even
    /// where its field is not nullable.
    Masked,
}

impl Slot {
    /// Whether this slot holds a value, drawing the choice a masked slot leaves open.
    fn holds_value(self, entropy: &mut Entropy<'_>) -> bool {
        match self {
            Self::Valid => true,
            Self::Null => false,
            Self::Masked => entropy.flag(),
        }
    }
}

/// A schema type of at most `depth` collection levels whose fixed-size lists hold one to
/// [`ARRAY_LENGTH`] elements, so its values stay small. Above depth zero half the draws are
/// collections, so lists nested in lists are common.
fn generated_type(entropy: &mut Entropy<'_>, depth: u8) -> ParseAsType {
    let scalars = ParseAsType::scalar_variants();
    let kind = if depth == 0 { 2 } else { entropy.byte() % 4 };
    if kind >= 2 {
        let count = std::num::NonZeroUsize::new(scalars.len()).assured("scalar types exist");
        return scalars[entropy.index(count)].clone();
    }
    let below = depth
        .checked_sub(1)
        .verified("a collection is offered only above depth zero");
    if kind == 0 {
        return ParseAsType::Vec {
            element: Box::new(generated_type(entropy, below)),
        };
    }
    let length = entropy.between(1..=u64::from(ARRAY_LENGTH));
    let length = u32::try_from(length).verified("the length is at most the array bound");
    ParseAsType::Array {
        element: Box::new(generated_type(entropy, below)),
        len: NonZeroU32::new(length).verified("the length is at least one"),
    }
}

/// The null buffer of `validity`, or none when every value is present.
fn null_buffer(validity: Vec<bool>) -> Option<NullBuffer> {
    if validity.iter().all(|present| *present) {
        return None;
    }
    Some(NullBuffer::from(validity))
}

/// Up to [`OCTETS`] octets of any value.
fn octets(entropy: &mut Entropy<'_>) -> Vec<u8> {
    let count = entropy.count(OCTETS);
    let mut bytes = Vec::with_capacity(count);
    for _ in 0..count {
        bytes.push(entropy.byte());
    }
    bytes
}

/// An unsigned integer of type `T`, landing on its extremes as often as anywhere else.
fn unsigned<T: TryFrom<u64> + Bounded>(entropy: &mut Entropy<'_>) -> T {
    let largest = T::LARGEST_UNSIGNED;
    let value = entropy.boundary_biased(0..=largest);
    T::try_from(value)
        .ok()
        .verified("the value is at most the largest value of its type")
}

/// A signed integer of type `T`, landing on zero, its extremes and their neighbours as often as
/// anywhere else.
fn signed<T: TryFrom<i64> + Bounded>(entropy: &mut Entropy<'_>) -> T {
    let smallest = T::SMALLEST_SIGNED;
    let largest = T::LARGEST_SIGNED;
    let value = match entropy.byte() % 8 {
        0 => 0,
        1 => smallest,
        2 => largest,
        3 => -1,
        4 => 1,
        5 => smallest
            .checked_add(1)
            .assured("the type holds more than one value"),
        6 => largest
            .checked_sub(1)
            .assured("the type holds more than one value"),
        _ => {
            let span = largest.abs_diff(smallest);
            let offset = entropy.between(0..=span);
            smallest
                .checked_add_unsigned(offset)
                .verified("the offset is at most the span of the type")
        }
    };
    T::try_from(value)
        .ok()
        .verified("the value lies between the extremes of its type")
}

/// The extremes of an integer type, as the generators above read them.
trait Bounded {
    const LARGEST_UNSIGNED: u64 = 0;
    const SMALLEST_SIGNED: i64 = 0;
    const LARGEST_SIGNED: i64 = 0;
}

impl Bounded for u8 {
    const LARGEST_UNSIGNED: u64 = 0xff;
}

impl Bounded for u16 {
    const LARGEST_UNSIGNED: u64 = 0xffff;
}

impl Bounded for u32 {
    const LARGEST_UNSIGNED: u64 = 0xffff_ffff;
}

impl Bounded for i8 {
    const SMALLEST_SIGNED: i64 = -0x80;
    const LARGEST_SIGNED: i64 = 0x7f;
}

impl Bounded for i16 {
    const SMALLEST_SIGNED: i64 = -0x8000;
    const LARGEST_SIGNED: i64 = 0x7fff;
}

impl Bounded for i32 {
    const SMALLEST_SIGNED: i64 = -0x8000_0000;
    const LARGEST_SIGNED: i64 = 0x7fff_ffff;
}

impl Bounded for i64 {
    const SMALLEST_SIGNED: i64 = i64::MIN;
    const LARGEST_SIGNED: i64 = i64::MAX;
}

/// A place among however many positions a case turns out to have, drawn before the case exists:
/// a share of them out of 65536.
///
/// An ordinary run hands a property at most 64 bytes, and a choice read after they run out takes
/// its first option. What selects a case's shape is therefore read before the case, and a place
/// inside it is drawn as a share and resolved once the case is known.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Place(u16);

impl Place {
    pub(crate) fn draw(entropy: &mut Entropy<'_>) -> Self {
        Self(u16::from_le_bytes([entropy.byte(), entropy.byte()]))
    }

    /// The one of `positions` places this is, which there must be at least one of.
    pub(crate) fn among(self, positions: usize) -> usize {
        let positions = u64::try_from(positions).assured("supported targets address 64 bits");
        let scaled = u64::from(self.0)
            .checked_mul(positions)
            .assured("a share of a count held in memory fits in 64 bits")
            / 65_536;
        usize::try_from(scaled).verified("a share of the positions is below their count")
    }
}

/// How a property damages a valid encoding before it reaches a decoder. A damage is drawn whole
/// before the case it damages; read after its case, it would be a truncation at offset zero of
/// nearly every case an ordinary run generates.
#[derive(Debug, Clone)]
pub(crate) struct Damage {
    kind: DamageKind,
    /// Where in the body the damage lands.
    place: Place,
    /// Which bit of the byte at that place a flip changes.
    bit: u8,
    /// What an overwritten word holds.
    word: DamageWord,
    /// The bytes a trail appends, or the bytes arbitrary damage replaces the body with.
    bytes: Vec<u8>,
}

/// What a damage does to a body.
#[derive(Debug, Clone, Copy)]
enum DamageKind {
    /// The body ends early.
    Truncate,
    /// One bit is flipped.
    FlipBit,
    /// One aligned little-endian word, such as a length, a count or an offset, holds an extreme.
    OverwriteWord,
    /// A byte is removed, shifting everything after it.
    RemoveByte,
    /// Bytes follow the end of the stream.
    Trail,
    /// The body is arbitrary bytes.
    Arbitrary,
}

impl DamageKind {
    const ALL: [Self; 6] = [
        Self::Truncate,
        Self::FlipBit,
        Self::OverwriteWord,
        Self::RemoveByte,
        Self::Trail,
        Self::Arbitrary,
    ];
}

/// What an overwritten word holds: a fixed value, or the length of the body it is written into.
#[derive(Debug, Clone, Copy)]
enum DamageWord {
    Value(u32),
    BodyLength,
}

impl Damage {
    /// The most bytes a trail appends.
    const TRAILING_BYTES: usize = 16;

    /// The most bytes arbitrary damage replaces a body with.
    const ARBITRARY_BYTES: usize = 256;

    /// One damage, read from the front of `entropy`.
    pub(crate) fn draw(entropy: &mut Entropy<'_>) -> Self {
        let kind = entropy.pick(DamageKind::ALL);
        let place = Place::draw(entropy);
        let bit = entropy.byte() % 8;
        let small = u32::from(entropy.byte());
        let word = entropy.pick([
            DamageWord::Value(0),
            DamageWord::Value(1),
            DamageWord::Value(u32::MAX),
            DamageWord::Value(0x7fff_ffff),
            DamageWord::Value(0x8000_0000),
            DamageWord::Value(small),
            DamageWord::BodyLength,
        ]);
        let length = match kind {
            DamageKind::Trail => entropy.count(Self::TRAILING_BYTES),
            DamageKind::Arbitrary => entropy.count(Self::ARBITRARY_BYTES),
            DamageKind::Truncate
            | DamageKind::FlipBit
            | DamageKind::OverwriteWord
            | DamageKind::RemoveByte => 0,
        };
        let bytes = (0..length).map(|_| entropy.byte()).collect();
        Self {
            kind,
            place,
            bit,
            word,
            bytes,
        }
    }

    /// `body` with this damage applied.
    pub(crate) fn apply(&self, mut body: Vec<u8>) -> Vec<u8> {
        match self.kind {
            DamageKind::Truncate => {
                // A body of `n` bytes may keep any of its `n + 1` prefixes.
                let prefixes = body
                    .len()
                    .checked_add(1)
                    .assured("a body in memory is shorter than the address space");
                let keep = self.place.among(prefixes);
                body.truncate(keep);
            }
            DamageKind::FlipBit => {
                if !body.is_empty() {
                    let position = self.place.among(body.len());
                    body[position] ^= 1 << self.bit;
                }
            }
            DamageKind::OverwriteWord => {
                let words = body.len() / 4;
                if words > 0 {
                    let start = self
                        .place
                        .among(words)
                        .checked_mul(4)
                        .verified("a word index inside the body");
                    let end = start.checked_add(4).verified("a word inside the body");
                    let word = match self.word {
                        DamageWord::Value(value) => value,
                        DamageWord::BodyLength => {
                            u32::try_from(body.len()).assured("a bounded body length")
                        }
                    };
                    body[start..end].copy_from_slice(&word.to_le_bytes());
                }
            }
            DamageKind::RemoveByte => {
                if !body.is_empty() {
                    let position = self.place.among(body.len());
                    body.remove(position);
                }
            }
            DamageKind::Trail => body.extend_from_slice(&self.bytes),
            DamageKind::Arbitrary => body = self.bytes.clone(),
        }
        body
    }
}

/// One value as a reader of the batch sees it. Floats keep their bits, so every NaN payload and
/// signed zero is compared; a datetime keeps its Unix nanoseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogicalValue {
    U8(u8),
    I8(i8),
    U16(u16),
    I16(i16),
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    F32(u32),
    F64(u64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Datetime(i64),
    Array(Vec<Option<LogicalValue>>),
    Vec(Vec<Option<LogicalValue>>),
}

impl LogicalValue {
    /// Row `row` of `array`, or `None` for a null. A value under a null is never read.
    pub(crate) fn of(array: &dyn Array, row: usize) -> Option<Self> {
        if array.is_null(row) {
            return None;
        }
        macro_rules! scalar {
            ($array:ty, $read:expr) => {{
                let typed = array
                    .as_any()
                    .downcast_ref::<$array>()
                    .assured("the data type above names the concrete array");
                let read: fn(&$array, usize) -> Self = $read;
                read(typed, row)
            }};
        }
        let value = match array.data_type() {
            DataType::UInt8 => scalar!(UInt8Array, |a, r| Self::U8(a.value(r))),
            DataType::Int8 => scalar!(Int8Array, |a, r| Self::I8(a.value(r))),
            DataType::UInt16 => scalar!(UInt16Array, |a, r| Self::U16(a.value(r))),
            DataType::Int16 => scalar!(Int16Array, |a, r| Self::I16(a.value(r))),
            DataType::UInt32 => scalar!(UInt32Array, |a, r| Self::U32(a.value(r))),
            DataType::Int32 => scalar!(Int32Array, |a, r| Self::I32(a.value(r))),
            DataType::UInt64 => scalar!(UInt64Array, |a, r| Self::U64(a.value(r))),
            DataType::Int64 => scalar!(Int64Array, |a, r| Self::I64(a.value(r))),
            DataType::Float32 => scalar!(Float32Array, |a, r| Self::F32(a.value(r).to_bits())),
            DataType::Float64 => scalar!(Float64Array, |a, r| Self::F64(a.value(r).to_bits())),
            DataType::Boolean => scalar!(BooleanArray, |a, r| Self::Bool(a.value(r))),
            DataType::Utf8 => scalar!(StringArray, |a, r| Self::String(a.value(r).to_string())),
            DataType::Binary => scalar!(BinaryArray, |a, r| Self::Bytes(a.value(r).to_vec())),
            DataType::Timestamp(_, _) => {
                scalar!(TimestampNanosecondArray, |a, r| Self::Datetime(a.value(r)))
            }
            DataType::FixedSizeList(_, _) => {
                let list = array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .assured("the data type above names the concrete array");
                let elements = list.value(row);
                Self::Array(Self::all(elements.as_ref()))
            }
            DataType::List(_) => {
                let list = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .assured("the data type above names the concrete array");
                let elements = list.value(row);
                Self::Vec(Self::all(elements.as_ref()))
            }
            other => panic!("a batch of a current schema holds no {other} column"),
        };
        Some(value)
    }

    /// Every value of `array`, in order.
    fn all(array: &dyn Array) -> Vec<Option<Self>> {
        (0..array.len()).map(|row| Self::of(array, row)).collect()
    }

    /// Every row of `batch`, each as the values of its columns in order.
    pub(crate) fn rows(batch: &RecordBatch) -> Vec<Vec<Option<Self>>> {
        (0..batch.num_rows())
            .map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| Self::of(column.as_ref(), row))
                    .collect()
            })
            .collect()
    }
}

/// Asserts that `actual` is `expected`: the same schema, field metadata and schema metadata
/// included, the same number of rows, and the same logical value or null in every cell.
pub(crate) fn assert_same_batch(actual: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(
        actual.schema(),
        expected.schema(),
        "the schema is preserved"
    );
    assert_eq!(
        actual.num_rows(),
        expected.num_rows(),
        "every row is preserved"
    );
    assert_eq!(
        LogicalValue::rows(actual),
        LogicalValue::rows(expected),
        "every value and null is preserved"
    );
}

/// Asserts that `rewritten`, read back from Arrow's writer, is `original` as the writer writes it:
/// the same schema but for a timestamp's empty zone name, which the writer writes as no zone, the
/// same number of rows, and the same logical value or null in every cell. A decoder that takes any
/// schema accepts such a zone from a damaged stream; no schema of the node's own declares one.
pub(crate) fn assert_rewritten_batch(rewritten: &RecordBatch, original: &RecordBatch) {
    assert_eq!(
        schema_as_written(&rewritten.schema()),
        schema_as_written(&original.schema()),
        "the schema is preserved as the writer writes it"
    );
    assert_eq!(
        rewritten.num_rows(),
        original.num_rows(),
        "every row is preserved"
    );
    assert_eq!(
        LogicalValue::rows(rewritten),
        LogicalValue::rows(original),
        "every value and null is preserved"
    );
}

/// `schema` as Arrow's writer writes it.
fn schema_as_written(schema: &ArrowSchema) -> ArrowSchema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field_as_written(field))
        .collect();
    ArrowSchema::new_with_metadata(fields, schema.metadata().clone())
}

/// `field` as Arrow's writer writes it: the empty zone name of a timestamp, the field's own or
/// that of a field it nests, becomes no zone.
fn field_as_written(field: &Field) -> Field {
    let data_type = match field.data_type() {
        DataType::Timestamp(unit, Some(zone)) if zone.is_empty() => {
            DataType::Timestamp(*unit, None)
        }
        DataType::List(element) => DataType::List(StdArc::new(field_as_written(element))),
        DataType::FixedSizeList(element, size) => {
            DataType::FixedSizeList(StdArc::new(field_as_written(element)), *size)
        }
        other => other.clone(),
    };
    field.clone().with_data_type(data_type)
}

/// Arrow's writer writes a timestamp's empty zone name as no zone, at the top level and inside a
/// list: the one difference [`assert_rewritten_batch`] sets aside, and nothing else.
#[test]
fn a_rewritten_batch_differs_only_in_an_empty_timestamp_zone() {
    use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
    use arrow_schema::TimeUnit;

    let zoned = DataType::Timestamp(TimeUnit::Nanosecond, Some("".into()));
    let element = StdArc::new(Field::new("item", zoned.clone(), false));
    let schema = StdArc::new(ArrowSchema::new(vec![
        Field::new("at", zoned, true),
        Field::new("times", DataType::List(StdArc::clone(&element)), false),
    ]));
    let at = TimestampNanosecondArray::from(vec![Some(1), None]).with_timezone("");
    let times = ListArray::try_new(
        element,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3])),
        StdArc::new(TimestampNanosecondArray::from(vec![5, -6, i64::MAX]).with_timezone("")),
        None,
    )
    .assured("three elements fill two lists");
    let columns: Vec<ArrayRef> = vec![StdArc::new(at), StdArc::new(times)];
    let original =
        RecordBatch::try_new(StdArc::clone(&schema), columns).assured("matching columns");
    let mut stream = Vec::new();
    let mut writer = StreamWriter::try_new(&mut stream, &schema).assured("the schema writes");
    writer.write(&original).assured("the batch writes");
    writer.finish().assured("the stream ends");
    drop(writer);
    let mut reader = StreamReader::try_new(std::io::Cursor::new(stream), None)
        .assured("the writer's stream opens");
    let rewritten = reader
        .next()
        .assured("the stream holds its batch")
        .assured("the writer's batch decodes");
    assert_ne!(
        rewritten.schema(),
        original.schema(),
        "the writer changes how the schema spells the zone"
    );
    assert_rewritten_batch(&rewritten, &original);
}
