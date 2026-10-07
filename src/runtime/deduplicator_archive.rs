//! One deduplicator branch's keyspace as typed archive columns, and the keyspace rebuilt from them.
//!
//! Layer: data plane.
//! - **Owns.** Typed key columns and the `seen_at` column built from the keys a branch published
//!   or persisted, bounded key groups, and the native keyspace checkpoint rebuilt from archived
//!   groups with the same key normalization the branch task applies.
//! - **Depends on.** The deduplicator keyspace and its checkpoint codec, the VM's typed arrays, and
//!   Arrow.
//! - **Must not know.** Archive records or paths, capture fencing, or restore placement.

use std::{io::Write, ops::Range};

use arrow_array::{
    Array, ArrayRef, RecordBatch, TimestampNanosecondArray,
    builder::{
        BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
        Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder, UInt8Builder,
        UInt16Builder, UInt32Builder, UInt64Builder,
    },
    new_null_array,
};
use arrow_schema::{DataType, Schema as ArrowSchema, TimeUnit};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto as _;
use nervix_execution::{Cancellation, ChargedBytes, Executor, MemoryClass, Reservation};
use nervix_expiry_map::ExpiryMap;
use nervix_models::Timestamp;
use nervix_primitives::sync::StdArc;
use ordered_float::OrderedFloat;

use super::{
    DeduplicatorKey, PublishedDeduplicatorKey, ReorderKeyPart, ReplicatedDeduplicatorState,
    VmTypedArray,
    deduplicator::{decode_deduplicator_snapshot, write_deduplicator_snapshot},
    published_generation::Generation,
    reorder_key_part,
};
use crate::runtime_schema::RuntimeRecordBatch;

/// Why a keyspace could not become archive columns, or archive columns a keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum DeduplicatorArchiveError {
    #[error("the archived key schema does not end in a nanosecond seen_at column")]
    SeenAtColumn,
    #[error("key {column} of a key does not have the type its DEDUPLICATE ON expression produces")]
    KeyPart { column: usize },
    #[error("a key has {found} parts where its keyspace has {expected} key columns")]
    KeyArity { expected: usize, found: usize },
    #[error("an archived key column {column} has a type no key part can hold")]
    KeyColumn { column: usize },
    #[error("the archived keyspace holds one key more than once")]
    DuplicateKey,
    #[error("an archived key carries no seen_at time")]
    MissingSeenAt,
    #[error("the keyspace could not be assembled into Arrow columns")]
    Columns,
    #[error("the keyspace's Arrow columns could not be admitted")]
    Admission,
    #[error("the restored keyspace checkpoint could not be encoded")]
    Encode,
    #[error("the persisted keyspace checkpoint could not be decoded")]
    Decode,
    #[error("rebuilding the keyspace was cancelled")]
    Cancelled,
}

/// The keys one deduplicator branch held at a backup cut, oldest first, and their revision.
///
/// The keys are shared with the generation the branch task published, or with a checkpoint read
/// from the state store; nothing is copied until a group of them becomes Arrow columns.
#[derive(Debug, Clone)]
pub(crate) struct CapturedDeduplicatorKeyspace {
    generation: StdArc<Generation<Vec<PublishedDeduplicatorKey>>>,
}

impl CapturedDeduplicatorKeyspace {
    /// The keys the branch task published last.
    pub(super) fn published(state: &ReplicatedDeduplicatorState) -> Self {
        Self {
            generation: state.generations.load(),
        }
    }

    /// The keys a persisted checkpoint at `revision` holds.
    pub(super) fn stored(
        revision: u64,
        payload: &[u8],
    ) -> error_stack::Result<Self, DeduplicatorArchiveError> {
        let recent_keys = decode_deduplicator_snapshot(payload)
            .change_context(DeduplicatorArchiveError::Decode)?;
        Ok(Self {
            generation: StdArc::new(Generation {
                revision,
                value: ReplicatedDeduplicatorState::published_keys(&recent_keys),
            }),
        })
    }

    pub(crate) fn revision(&self) -> u64 {
        self.generation.revision
    }

    pub(crate) fn key_count(&self) -> usize {
        self.generation.value.len()
    }

    /// Consecutive keys whose estimated Arrow payload stays within `limit`, oldest first. A key
    /// larger than the limit occupies a group of its own.
    pub(crate) fn groups(&self, limit: u64) -> Vec<Range<usize>> {
        let mut groups = Vec::new();
        let mut start = 0;
        let mut bytes = 0_u64;
        for (index, published) in self.generation.value.iter().enumerate() {
            let key_bytes = published.key.estimated_bytes();
            let next = bytes.checked_add(key_bytes);
            let overruns = match next {
                Some(next) => next > limit,
                None => true,
            };
            if index > start && overruns {
                groups.push(start..index);
                start = index;
                bytes = key_bytes;
                continue;
            }
            bytes = next.unwrap_or(limit);
        }
        if start < self.generation.value.len() {
            groups.push(start..self.generation.value.len());
        }
        groups
    }

    /// The keys in `group` as one Arrow IPC stream of `schema`. Building the columns and writing
    /// the stream share one bulk admission and one CPU job.
    pub(crate) async fn encode_group(
        &self,
        executor: &Executor,
        schema: &StdArc<ArrowSchema>,
        group: Range<usize>,
    ) -> error_stack::Result<ChargedBytes, DeduplicatorArchiveError> {
        let Some(keys) = self.generation.value.get(group.clone()) else {
            return Err(Report::new(DeduplicatorArchiveError::Columns));
        };
        let mut payload = 0_u64;
        for published in keys {
            payload = payload
                .checked_add(published.key.estimated_bytes())
                .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
        }
        // Builders may double a buffer while appending, and every row carries offsets and
        // validity beside its values.
        let rows = u64::try_from(keys.len())
            .map_err(|_| Report::new(DeduplicatorArchiveError::Admission))?;
        let row_bytes = rows
            .checked_mul(64)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
        let doubled = payload
            .checked_mul(2)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
        let projection_bytes = doubled
            .checked_add(row_bytes)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
        let encoded_estimate = payload
            .checked_add(64 * 1024)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
        let keyspace = self.clone();
        let schema = StdArc::clone(schema);
        RuntimeRecordBatch::encode_arrow_snapshot_projection(
            executor,
            projection_bytes,
            encoded_estimate,
            move || {
                let batch = keyspace.key_batch(&schema, group)?;
                RuntimeRecordBatch::from_record_batch(schema, batch)
                    .change_context(DeduplicatorArchiveError::Columns)
            },
        )
        .await
        .change_context(DeduplicatorArchiveError::Columns)
    }

    /// The keys in `range` as one batch of `schema`: a column per key part, typed as `schema`
    /// declares, and the `seen_at` column last.
    fn key_batch(
        &self,
        schema: &StdArc<ArrowSchema>,
        range: Range<usize>,
    ) -> error_stack::Result<RecordBatch, DeduplicatorArchiveError> {
        let keys = self
            .generation
            .value
            .get(range)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Columns))?;
        let Some((_seen_at, key_fields)) = schema.fields().split_last() else {
            return Err(Report::new(DeduplicatorArchiveError::SeenAtColumn));
        };
        for published in keys {
            let found = published.key.parts().len();
            if found != key_fields.len() {
                return Err(Report::new(DeduplicatorArchiveError::KeyArity {
                    expected: key_fields.len(),
                    found,
                }));
            }
        }
        let mut columns = Vec::with_capacity(schema.fields().len());
        for (column, field) in key_fields.iter().enumerate() {
            let parts = keys.iter().map(|published| &published.key.parts()[column]);
            columns.push(key_column(column, field.data_type(), parts, keys.len())?);
        }
        let mut seen_at = TimestampNanosecondBuilder::with_capacity(keys.len());
        for published in keys {
            seen_at.append_value(published.seen_at.unix_nanos());
        }
        let seen_at: ArrayRef = StdArc::new(seen_at.finish().with_timezone("+00:00"));
        columns.push(seen_at);
        RecordBatch::try_new(StdArc::clone(schema), columns)
            .map_err(|error| Report::new(DeduplicatorArchiveError::Columns).attach_printable(error))
    }
}

/// One typed key column of `data_type` holding `parts`, every part the type's own normalization.
fn key_column<'a>(
    column: usize,
    data_type: &DataType,
    parts: impl Iterator<Item = &'a ReorderKeyPart>,
    rows: usize,
) -> error_stack::Result<ArrayRef, DeduplicatorArchiveError> {
    let mismatch = || Report::new(DeduplicatorArchiveError::KeyPart { column });
    macro_rules! integers {
        ($builder:ty, $variant:ident, $integer:ty) => {{
            let mut builder = <$builder>::with_capacity(rows);
            for part in parts {
                match part {
                    ReorderKeyPart::Null => builder.append_null(),
                    ReorderKeyPart::$variant(value) => {
                        let narrowed = <$integer>::try_from(*value).map_err(|_| mismatch())?;
                        builder.append_value(narrowed);
                    }
                    _ => return Err(mismatch()),
                }
            }
            let array: ArrayRef = StdArc::new(builder.finish());
            Ok(array)
        }};
    }
    match data_type {
        DataType::UInt8 => integers!(UInt8Builder, UInt64, u8),
        DataType::UInt16 => integers!(UInt16Builder, UInt64, u16),
        DataType::UInt32 => integers!(UInt32Builder, UInt64, u32),
        DataType::UInt64 => integers!(UInt64Builder, UInt64, u64),
        DataType::Int8 => integers!(Int8Builder, Int64, i8),
        DataType::Int16 => integers!(Int16Builder, Int64, i16),
        DataType::Int32 => integers!(Int32Builder, Int64, i32),
        DataType::Int64 => integers!(Int64Builder, Int64, i64),
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(rows);
            for part in parts {
                match part {
                    ReorderKeyPart::Null => builder.append_null(),
                    ReorderKeyPart::Float64(OrderedFloat(value)) => {
                        // A single-precision key part was widened exactly, so narrowing it back
                        // recovers the same value; anything else was never this column's part.
                        let value = *value;
                        let narrowed: f32 = value.approx_into();
                        let widened = f64::from(narrowed);
                        if widened.to_bits() != value.to_bits()
                            && !(widened.is_nan() && value.is_nan())
                        {
                            return Err(mismatch());
                        }
                        builder.append_value(narrowed);
                    }
                    _ => return Err(mismatch()),
                }
            }
            let array: ArrayRef = StdArc::new(builder.finish());
            Ok(array)
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(rows);
            for part in parts {
                match part {
                    ReorderKeyPart::Null => builder.append_null(),
                    ReorderKeyPart::Float64(OrderedFloat(value)) => builder.append_value(*value),
                    _ => return Err(mismatch()),
                }
            }
            let array: ArrayRef = StdArc::new(builder.finish());
            Ok(array)
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows);
            for part in parts {
                match part {
                    ReorderKeyPart::Null => builder.append_null(),
                    ReorderKeyPart::Boolean(value) => builder.append_value(*value),
                    _ => return Err(mismatch()),
                }
            }
            let array: ArrayRef = StdArc::new(builder.finish());
            Ok(array)
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(rows, 0);
            for part in parts {
                match part {
                    ReorderKeyPart::Null => builder.append_null(),
                    ReorderKeyPart::Utf8(value) => builder.append_value(value),
                    _ => return Err(mismatch()),
                }
            }
            let array: ArrayRef = StdArc::new(builder.finish());
            Ok(array)
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(rows, 0);
            for part in parts {
                match part {
                    ReorderKeyPart::Null => builder.append_null(),
                    ReorderKeyPart::Bytes(value) => builder.append_value(value),
                    _ => return Err(mismatch()),
                }
            }
            let array: ArrayRef = StdArc::new(builder.finish());
            Ok(array)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(zone)) => {
            let mut builder = TimestampNanosecondBuilder::with_capacity(rows);
            for part in parts {
                match part {
                    ReorderKeyPart::Null => builder.append_null(),
                    ReorderKeyPart::Datetime(value) => builder.append_value(*value),
                    _ => return Err(mismatch()),
                }
            }
            let array: ArrayRef = StdArc::new(builder.finish().with_timezone(zone.clone()));
            Ok(array)
        }
        // Every other type reaches a key as a null part, so only nulls fit its column.
        other => {
            for part in parts {
                if *part != ReorderKeyPart::Null {
                    return Err(mismatch());
                }
            }
            Ok(new_null_array(other, rows))
        }
    }
}

/// The share of its restore conversion each archived key takes whatever its values: its entry in
/// the resident keyspace and its resolver in the streamed checkpoint's serializer scratch. The
/// archive description admits this share for every key its deduplicator descriptors count.
pub(crate) fn restored_key_fixed_bytes() -> u64 {
    let bytes = ExpiryMap::<DeduplicatorKey, Timestamp>::ENTRY_BYTES
        .checked_add(super::deduplicator::STREAMED_KEY_RESOLVER_BYTES)
        .assured("an entry and a resolver are a few hundred bytes");
    u64::try_from(bytes).assured("a few hundred bytes fit 64 bits")
}

/// How far the keyspace's restore metadata charge grows at a time, so that admitting keys asks the
/// budget once per mebibyte rather than once per key.
const RESIDENT_GROWTH_BYTES: u64 = 1024 * 1024;

/// A keyspace rebuilt from archived key groups, oldest key first.
///
/// Every key is held as the runtime holds a restored keyspace, normalized in an expiry map, so a
/// key the archive holds twice is refused. Each key's parts and values, which follow the archived
/// Arrow columns rather than the descriptor, are charged to the restore metadata budget as its
/// group is admitted; the fixed share of every key was admitted with the archive description.
#[derive(Debug)]
pub(crate) struct ArchivedDeduplicatorKeys {
    recent_keys: ExpiryMap<DeduplicatorKey, Timestamp>,
    /// The restore metadata charge, at least `charged_bytes`.
    charge: Reservation,
    /// What the admitted keys' parts and values occupy.
    charged_bytes: u64,
    /// The most parts and the most part and value bytes of one key, which the streamed encoding
    /// converts one entry at a time.
    largest_parts: usize,
    largest_key_bytes: u64,
    /// The most the native checkpoint of the admitted keys can occupy.
    encoded_bound: u64,
}

impl ArchivedDeduplicatorKeys {
    pub(crate) fn new(executor: &Executor) -> error_stack::Result<Self, DeduplicatorArchiveError> {
        let charge = executor
            .try_reserve(MemoryClass::RestoreMetadata, RESIDENT_GROWTH_BYTES)
            .change_context(DeduplicatorArchiveError::Admission)?;
        Ok(Self {
            recent_keys: ExpiryMap::new(),
            charge,
            charged_bytes: 0,
            largest_parts: 0,
            largest_key_bytes: 0,
            encoded_bound: super::deduplicator::ENCODED_SNAPSHOT_BYTES,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.recent_keys.len()
    }

    /// The most the native checkpoint of the admitted keys can occupy.
    pub(crate) fn encoded_bound(&self) -> u64 {
        self.encoded_bound
    }

    /// Charge `bytes` more to the restore metadata budget before they are allocated. Growth the
    /// budget cannot back now is refused rather than awaited, because the keys already held cannot
    /// free it.
    fn charge_more(&mut self, bytes: u64) -> error_stack::Result<(), DeduplicatorArchiveError> {
        let target = self
            .charged_bytes
            .checked_add(bytes)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
        if target > self.charge.bytes() {
            let grown = target
                .checked_add(RESIDENT_GROWTH_BYTES)
                .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
            self.charge
                .grow_to(grown)
                .change_context(DeduplicatorArchiveError::Admission)?;
        }
        self.charged_bytes = target;
        Ok(())
    }

    /// Admits one group's keys in row order, normalizing each column exactly as a branch task
    /// normalizes the values its `DEDUPLICATE ON` expressions produce.
    pub(crate) fn admit_group(
        &mut self,
        batch: &RecordBatch,
        cancellation: &Cancellation,
    ) -> error_stack::Result<(), DeduplicatorArchiveError> {
        let Some((seen_at, key_columns)) = batch.columns().split_last() else {
            return Err(Report::new(DeduplicatorArchiveError::SeenAtColumn));
        };
        let Some(seen_at) = seen_at.as_any().downcast_ref::<TimestampNanosecondArray>() else {
            return Err(Report::new(DeduplicatorArchiveError::SeenAtColumn));
        };
        let mut typed = Vec::with_capacity(key_columns.len());
        for (column, array) in key_columns.iter().enumerate() {
            let array = VmTypedArray::try_from_array_ref(StdArc::clone(array))
                .change_context(DeduplicatorArchiveError::KeyColumn { column })?;
            typed.push(array);
        }
        for row in 0..batch.num_rows() {
            cancellation
                .check()
                .change_context(DeduplicatorArchiveError::Cancelled)?;
            if !seen_at.is_valid(row) {
                return Err(Report::new(DeduplicatorArchiveError::MissingSeenAt));
            }
            let mut parts = Vec::with_capacity(typed.len());
            for array in &typed {
                parts.push(reorder_key_part(array, row));
            }
            let key = DeduplicatorKey::new(parts);
            let key_bytes = key.resident_bytes();
            self.charge_more(key_bytes)?;
            self.largest_parts = self.largest_parts.max(key.parts().len());
            self.largest_key_bytes = self.largest_key_bytes.max(key_bytes);
            self.encoded_bound = self
                .encoded_bound
                .checked_add(key.encoded_bound())
                .ok_or_else(|| Report::new(DeduplicatorArchiveError::Encode))?;
            let seen = Timestamp::from_unix_nanos(seen_at.value(row));
            if !self.recent_keys.insert(key, seen) {
                return Err(Report::new(DeduplicatorArchiveError::DuplicateKey));
            }
        }
        Ok(())
    }

    /// Charge what streaming the checkpoint adds beside the resident keys: the parts and values of
    /// the one entry it converts at a time, and that entry's part resolvers.
    pub(crate) fn admit_encoding(&mut self) -> error_stack::Result<(), DeduplicatorArchiveError> {
        let nested = super::deduplicator::streamed_snapshot_scratch_bytes(0, self.largest_parts)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
        let nested =
            u64::try_from(nested).map_err(|_| Report::new(DeduplicatorArchiveError::Admission))?;
        let conversion = self
            .largest_key_bytes
            .checked_add(nested)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
        self.charge_more(conversion)
    }

    /// Stream the native keyspace checkpoint a deduplicator branch restores from into `output`,
    /// converting one entry at a time.
    pub(crate) fn write_checkpoint(
        &self,
        output: &mut dyn Write,
        cancellation: &Cancellation,
    ) -> error_stack::Result<(), DeduplicatorArchiveError> {
        write_deduplicator_snapshot(
            self.recent_keys.iter(),
            self.largest_parts,
            output,
            cancellation,
        )
        .change_context(DeduplicatorArchiveError::Encode)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, ListArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        types::Int64Type,
    };
    use arrow_schema::Field;
    use nervix_arbitrary::Entropy;
    use nervix_execution::ExecutionConfig;
    use nonzero_ext::nonzero;

    use super::*;

    /// One of the key column types a `DEDUPLICATE ON` expression can produce.
    fn generated_key_type(entropy: &mut Entropy<'_>) -> DataType {
        match entropy.index(nonzero!(15_usize)) {
            0 => DataType::Int8,
            1 => DataType::Int16,
            2 => DataType::Int32,
            3 => DataType::Int64,
            4 => DataType::UInt8,
            5 => DataType::UInt16,
            6 => DataType::UInt32,
            7 => DataType::UInt64,
            8 => DataType::Float32,
            9 => DataType::Float64,
            10 => DataType::Boolean,
            11 => DataType::Utf8,
            12 => DataType::Binary,
            13 => DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            _ => DataType::List(StdArc::new(Field::new_list_field(DataType::Int64, true))),
        }
    }

    /// One key part as its exact representation, so a float compares by its bits.
    #[derive(Debug, PartialEq, Eq)]
    enum ExactPart {
        Null,
        Boolean(bool),
        Int64(i64),
        UInt64(u64),
        Float64(u64),
        Utf8(String),
        Bytes(Vec<u8>),
        Datetime(i64),
    }

    impl From<&ReorderKeyPart> for ExactPart {
        fn from(part: &ReorderKeyPart) -> Self {
            match part {
                ReorderKeyPart::Null => Self::Null,
                ReorderKeyPart::Boolean(value) => Self::Boolean(*value),
                ReorderKeyPart::Int64(value) => Self::Int64(*value),
                ReorderKeyPart::UInt64(value) => Self::UInt64(*value),
                ReorderKeyPart::Float64(OrderedFloat(value)) => Self::Float64(value.to_bits()),
                ReorderKeyPart::Utf8(value) => Self::Utf8(value.clone()),
                ReorderKeyPart::Bytes(value) => Self::Bytes(value.clone()),
                ReorderKeyPart::Datetime(value) => Self::Datetime(*value),
            }
        }
    }

    /// One published key as its exact parts and its `seen_at` time.
    #[derive(Debug, PartialEq, Eq)]
    struct ExactKey {
        parts: Vec<ExactPart>,
        seen_at: Timestamp,
    }

    fn exact_keys(keys: &[PublishedDeduplicatorKey]) -> Vec<ExactKey> {
        let mut exact = Vec::with_capacity(keys.len());
        for published in keys {
            let mut parts = Vec::with_capacity(published.key.parts().len());
            for part in published.key.parts() {
                parts.push(ExactPart::from(part));
            }
            exact.push(ExactKey {
                parts,
                seen_at: published.seen_at,
            });
        }
        exact
    }

    fn executor() -> Executor {
        Executor::new(ExecutionConfig::default()).assured("default bounds are valid")
    }

    fn key_schema(key_types: &[DataType]) -> StdArc<ArrowSchema> {
        let mut fields = Vec::with_capacity(key_types.len() + 1);
        for (index, data_type) in key_types.iter().enumerate() {
            fields.push(Field::new(
                nervix_backup::deduplicator_key_column(index),
                data_type.clone(),
                true,
            ));
        }
        fields.push(Field::new(
            nervix_backup::DEDUPLICATOR_SEEN_AT_COLUMN,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            false,
        ));
        StdArc::new(ArrowSchema::new(fields))
    }

    fn four_bytes(entropy: &mut Entropy<'_>) -> [u8; 4] {
        [
            entropy.byte(),
            entropy.byte(),
            entropy.byte(),
            entropy.byte(),
        ]
    }

    /// A column of `rows` generated values of `data_type`, each valid or null as `entropy` picks.
    fn generated_column(entropy: &mut Entropy<'_>, data_type: &DataType, rows: usize) -> ArrayRef {
        macro_rules! values {
            ($array:ty, $value:expr) => {{
                let mut values = Vec::with_capacity(rows);
                for _ in 0..rows {
                    if entropy.flag() {
                        values.push(None);
                    } else {
                        values.push(Some($value));
                    }
                }
                let array: ArrayRef = StdArc::new(<$array>::from(values));
                array
            }};
        }
        match data_type {
            DataType::Int8 => values!(Int8Array, i8::from_le_bytes([entropy.byte()])),
            DataType::Int16 => {
                values!(
                    Int16Array,
                    i16::from_le_bytes([entropy.byte(), entropy.byte()])
                )
            }
            DataType::Int32 => values!(Int32Array, i32::from_le_bytes(four_bytes(entropy))),
            DataType::Int64 => values!(Int64Array, entropy.any_i64()),
            DataType::UInt8 => values!(UInt8Array, entropy.byte()),
            DataType::UInt16 => {
                values!(
                    UInt16Array,
                    u16::from_le_bytes([entropy.byte(), entropy.byte()])
                )
            }
            DataType::UInt32 => values!(UInt32Array, u32::from_le_bytes(four_bytes(entropy))),
            DataType::UInt64 => values!(UInt64Array, entropy.any_u64()),
            DataType::Float32 => {
                values!(
                    Float32Array,
                    f32::from_bits(u32::from_le_bytes(four_bytes(entropy)))
                )
            }
            DataType::Float64 => values!(Float64Array, f64::from_bits(entropy.any_u64())),
            DataType::Boolean => values!(BooleanArray, entropy.flag()),
            DataType::Utf8 => values!(StringArray, {
                let length = entropy.count(4);
                let mut text = String::with_capacity(length);
                for _ in 0..length {
                    text.push(entropy.pick(['a', 'é', '7', '\u{1F600}']));
                }
                text
            }),
            DataType::Binary => {
                let mut values = Vec::with_capacity(rows);
                for _ in 0..rows {
                    if entropy.flag() {
                        values.push(None);
                        continue;
                    }
                    let length = entropy.count(4);
                    let mut bytes = Vec::with_capacity(length);
                    for _ in 0..length {
                        bytes.push(entropy.byte());
                    }
                    values.push(Some(bytes));
                }
                let array: ArrayRef = StdArc::new(BinaryArray::from_iter(values));
                array
            }
            DataType::Timestamp(_, zone) => {
                let mut values = Vec::with_capacity(rows);
                for _ in 0..rows {
                    if entropy.flag() {
                        values.push(None);
                    } else {
                        values.push(Some(entropy.any_i64()));
                    }
                }
                let array = TimestampNanosecondArray::from(values).with_timezone_opt(zone.clone());
                let array: ArrayRef = StdArc::new(array);
                array
            }
            DataType::List(_) => {
                let mut lists = Vec::with_capacity(rows);
                for _ in 0..rows {
                    if entropy.flag() {
                        lists.push(None);
                    } else {
                        lists.push(Some(vec![Some(entropy.any_i64())]));
                    }
                }
                let array: ArrayRef =
                    StdArc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(lists));
                array
            }
            other => unreachable!("the generator only picks key types, not {other}"),
        }
    }

    /// A keyspace as a branch task builds it: every row of the generated key columns normalized
    /// by the task's own key normalization, a repeated key keeping its first sighting.
    fn generated_keyspace(
        entropy: &mut Entropy<'_>,
        key_types: &[DataType],
    ) -> ExpiryMap<DeduplicatorKey, Timestamp> {
        let rows = entropy.count(8);
        let mut columns = Vec::with_capacity(key_types.len());
        for data_type in key_types {
            let column = generated_column(entropy, data_type, rows);
            columns.push(
                VmTypedArray::try_from_array_ref(column)
                    .assured("every generated key type is a VM typed array"),
            );
        }
        let mut keys = ExpiryMap::new();
        for row in 0..rows {
            let mut parts = Vec::with_capacity(columns.len());
            for column in &columns {
                parts.push(reorder_key_part(column, row));
            }
            let seen_at = Timestamp::from_unix_nanos(entropy.any_i64());
            keys.insert(DeduplicatorKey::new(parts), seen_at);
        }
        keys
    }

    /// `keys` with `batch` admitted in one bulk job, as restore conversion admits a key group,
    /// and what admitting it returned.
    async fn admit_into(
        executor: &Executor,
        mut keys: ArchivedDeduplicatorKeys,
        batch: RecordBatch,
    ) -> (
        ArchivedDeduplicatorKeys,
        error_stack::Result<(), DeduplicatorArchiveError>,
    ) {
        let charge = executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .assured("a one-byte test charge is admitted");
        executor
            .run_cpu(
                nervix_execution::CpuClass::Bulk,
                charge,
                move |_charge, cancellation| {
                    let admitted = keys.admit_group(&batch, cancellation);
                    (keys, admitted)
                },
            )
            .await
            .assured("the admission job runs")
    }

    /// The checkpoint `keys` streams in one bulk job.
    async fn streamed(executor: &Executor, mut keys: ArchivedDeduplicatorKeys) -> Vec<u8> {
        keys.admit_encoding()
            .assured("a bounded keyspace's conversion is admitted");
        let charge = executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .assured("a one-byte test charge is admitted");
        executor
            .run_cpu(
                nervix_execution::CpuClass::Bulk,
                charge,
                move |_charge, cancellation| {
                    let mut checkpoint = Vec::new();
                    keys.write_checkpoint(&mut checkpoint, cancellation)
                        .assured("a restored keyspace streams");
                    assert!(
                        u64::try_from(checkpoint.len()).assured("a test checkpoint fits 64 bits")
                            <= keys.encoded_bound(),
                        "the checkpoint stays within the disk bound reserved for it"
                    );
                    checkpoint
                },
            )
            .await
            .assured("the encoding job runs")
    }

    async fn archived_and_restored(
        executor: &Executor,
        keyspace: &CapturedDeduplicatorKeyspace,
        schema: &StdArc<ArrowSchema>,
        limit: u64,
    ) -> ArchivedDeduplicatorKeys {
        let mut restored =
            ArchivedDeduplicatorKeys::new(executor).assured("the restore metadata budget has room");
        for group in keyspace.groups(limit) {
            let bytes = keyspace
                .encode_group(executor, schema, group)
                .await
                .assured("a bounded key group encodes");
            let decoded = RuntimeRecordBatch::decode_arrow_snapshot_section(
                executor,
                StdArc::clone(schema),
                bytes,
            )
            .await
            .assured("an encoded key group decodes under its exact schema");
            let (keys, admitted) = admit_into(executor, restored, decoded.batch().clone()).await;
            admitted.assured("archived keys of the keyspace's own shape are admitted");
            restored = keys;
        }
        restored
    }

    #[test]
    fn bolero_deduplicator_archive_keys_preserve_every_key_and_seen_at() {
        bolero::check!()
            .with_iterations(128)
            .with_max_len(2048)
            .for_each(|input| {
                let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .assured("property runtime opens");
                let executor = executor();
                let mut entropy = Entropy::new(input);
                let column_count = entropy.positive_count(nonzero!(4_usize));
                let mut key_types = Vec::with_capacity(column_count);
                for _ in 0..column_count {
                    key_types.push(generated_key_type(&mut entropy));
                }
                let keys = generated_keyspace(&mut entropy, &key_types);
                let revision = entropy.any_u64();
                let limit = entropy.between(1..=96);
                let keyspace = CapturedDeduplicatorKeyspace {
                    generation: StdArc::new(Generation {
                        revision,
                        value: ReplicatedDeduplicatorState::published_keys(&keys),
                    }),
                };
                assert_eq!(keyspace.revision(), revision);
                assert_eq!(keyspace.key_count(), keys.len());

                let groups = keyspace.groups(limit);
                let mut next = 0;
                for group in &groups {
                    assert_eq!(group.start, next, "groups cover the keys in order");
                    assert!(group.end > group.start, "no group is empty");
                    if group.len() > 1 {
                        let mut bytes = 0_u64;
                        for published in &keyspace.generation.value[group.clone()] {
                            bytes = bytes
                                .checked_add(published.key.estimated_bytes())
                                .assured("a few bounded keys stay far below u64::MAX bytes");
                        }
                        assert!(
                            bytes <= limit,
                            "a group of several keys stays within its bound"
                        );
                    }
                    next = group.end;
                }
                assert_eq!(next, keys.len(), "groups cover every key");

                let schema = key_schema(&key_types);
                let checkpoint = runtime.block_on(async {
                    let restored =
                        archived_and_restored(&executor, &keyspace, &schema, limit).await;
                    assert_eq!(restored.len(), keys.len());
                    streamed(&executor, restored).await
                });
                assert_eq!(
                    executor.snapshot().restore_metadata_memory.reserved_bytes,
                    0,
                    "the resident keyspace's charge ends with its conversion"
                );
                let decoded = decode_deduplicator_snapshot(&checkpoint)
                    .assured("a restored checkpoint decodes as a keyspace");
                assert_eq!(
                    exact_keys(&ReplicatedDeduplicatorState::published_keys(&decoded)),
                    exact_keys(&keyspace.generation.value),
                    "every key, its order, its exact parts and its seen_at survive the archive"
                );
                assert_eq!(
                    checkpoint,
                    super::super::deduplicator::encode_deduplicator_snapshot(
                        &keyspace.generation.value
                    )
                    .assured("the published keys encode"),
                    "streaming writes exactly the ordinary checkpoint of the same keys"
                );
            });
    }

    fn one_key_keyspace(parts: Vec<ReorderKeyPart>) -> CapturedDeduplicatorKeyspace {
        let mut keys = ExpiryMap::new();
        keys.insert(DeduplicatorKey::new(parts), Timestamp::from_unix_nanos(7));
        CapturedDeduplicatorKeyspace {
            generation: StdArc::new(Generation {
                revision: 1,
                value: ReplicatedDeduplicatorState::published_keys(&keys),
            }),
        }
    }

    fn key_batch_error(
        parts: Vec<ReorderKeyPart>,
        key_types: &[DataType],
    ) -> DeduplicatorArchiveError {
        let keyspace = one_key_keyspace(parts);
        let error = keyspace
            .key_batch(&key_schema(key_types), 0..1)
            .expect_err("a key that does not fit its columns is refused");
        *error.current_context()
    }

    #[test]
    fn key_columns_refuse_parts_their_type_does_not_produce() {
        assert_eq!(
            key_batch_error(vec![ReorderKeyPart::Int64(300)], &[DataType::Int8]),
            DeduplicatorArchiveError::KeyPart { column: 0 },
            "an integer outside the column's type was never this column's part"
        );
        assert_eq!(
            key_batch_error(
                vec![ReorderKeyPart::Float64(OrderedFloat(0.1))],
                &[DataType::Float32]
            ),
            DeduplicatorArchiveError::KeyPart { column: 0 },
            "a double that no single-precision value widens to was never this column's part"
        );
        assert_eq!(
            key_batch_error(
                vec![ReorderKeyPart::Utf8("a".to_string())],
                &[DataType::Binary]
            ),
            DeduplicatorArchiveError::KeyPart { column: 0 }
        );
        assert_eq!(
            key_batch_error(
                vec![ReorderKeyPart::Boolean(true)],
                &[DataType::List(StdArc::new(Field::new_list_field(
                    DataType::Int64,
                    true
                )))]
            ),
            DeduplicatorArchiveError::KeyPart { column: 0 },
            "a list column only ever holds null parts"
        );
        assert_eq!(
            key_batch_error(
                vec![ReorderKeyPart::Int64(1)],
                &[DataType::Int64, DataType::Int64]
            ),
            DeduplicatorArchiveError::KeyArity {
                expected: 2,
                found: 1
            }
        );
        let keyspace = one_key_keyspace(vec![ReorderKeyPart::Null]);
        let no_seen_at = StdArc::new(ArrowSchema::empty());
        let error = keyspace
            .key_batch(&no_seen_at, 0..1)
            .expect_err("a schema without its seen_at column is refused");
        assert_eq!(
            error.current_context(),
            &DeduplicatorArchiveError::SeenAtColumn
        );
    }

    fn archived_batch(keys: ArrayRef, seen_at: ArrayRef) -> RecordBatch {
        let schema = StdArc::new(ArrowSchema::new(vec![
            Field::new("key_0", keys.data_type().clone(), true),
            Field::new("seen_at", seen_at.data_type().clone(), true),
        ]));
        RecordBatch::try_new(schema, vec![keys, seen_at]).assured("the test columns align")
    }

    #[nervix_primitives::test]
    async fn restoring_keys_refuses_repeated_keys_and_missing_seen_at_times() {
        let executor = executor();
        let fresh = || ArchivedDeduplicatorKeys::new(&executor).assured("the budget has room");
        let seen_at: ArrayRef = StdArc::new(
            TimestampNanosecondArray::from(vec![Some(1), Some(2)]).with_timezone("+00:00"),
        );
        let repeated: ArrayRef = StdArc::new(Int32Array::from(vec![Some(4), Some(4)]));
        let (_, admitted) = admit_into(
            &executor,
            fresh(),
            archived_batch(repeated, StdArc::clone(&seen_at)),
        )
        .await;
        let error = admitted.expect_err("a keyspace holds every key once");
        assert_eq!(
            error.current_context(),
            &DeduplicatorArchiveError::DuplicateKey
        );

        let keys: ArrayRef = StdArc::new(Int32Array::from(vec![Some(4), Some(5)]));
        let missing: ArrayRef = StdArc::new(
            TimestampNanosecondArray::from(vec![Some(1), None]).with_timezone("+00:00"),
        );
        let (_, admitted) = admit_into(
            &executor,
            fresh(),
            archived_batch(StdArc::clone(&keys), missing),
        )
        .await;
        let error = admitted.expect_err("every archived key was seen at some time");
        assert_eq!(
            error.current_context(),
            &DeduplicatorArchiveError::MissingSeenAt
        );

        let not_a_time: ArrayRef = StdArc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let (_, admitted) = admit_into(&executor, fresh(), archived_batch(keys, not_a_time)).await;
        let error = admitted.expect_err("seen_at is a nanosecond timestamp");
        assert_eq!(
            error.current_context(),
            &DeduplicatorArchiveError::SeenAtColumn
        );

        let keys: ArrayRef = StdArc::new(Int32Array::from(vec![Some(4), None]));
        let (restored, admitted) =
            admit_into(&executor, fresh(), archived_batch(keys, seen_at)).await;
        admitted.assured("distinct keys with their times are admitted");
        let decoded = decode_deduplicator_snapshot(&streamed(&executor, restored).await)
            .assured("a restored checkpoint decodes");
        let published = ReplicatedDeduplicatorState::published_keys(&decoded);
        assert_eq!(
            exact_keys(&published),
            vec![
                ExactKey {
                    parts: vec![ExactPart::Int64(4)],
                    seen_at: Timestamp::from_unix_nanos(1),
                },
                ExactKey {
                    parts: vec![ExactPart::Null],
                    seen_at: Timestamp::from_unix_nanos(2),
                },
            ],
            "a 32-bit key widens as a branch task widens it, and a null stays a null part"
        );
    }

    /// The resident keyspace is charged to the restore metadata budget as its keys arrive, and a
    /// keyspace that budget cannot hold is refused instead of growing uncharged.
    #[nervix_primitives::test]
    async fn the_resident_keyspace_is_charged_to_restore_metadata_and_refused_when_full() {
        let config = ExecutionConfig::default();
        let budget = config.budgets.restore_metadata.as_u64();
        let executor = Executor::new(config).assured("default bounds are valid");
        let keys: ArrayRef = StdArc::new(StringArray::from(vec![
            Some("a".repeat(3 * 1024 * 1024)),
            Some("b".repeat(1024)),
        ]));
        let seen_at: ArrayRef = StdArc::new(
            TimestampNanosecondArray::from(vec![Some(1), Some(2)]).with_timezone("+00:00"),
        );
        let batch = archived_batch(keys, seen_at);
        let restored = ArchivedDeduplicatorKeys::new(&executor).assured("the budget has room");
        let (restored, admitted) = admit_into(&executor, restored, batch.clone()).await;
        admitted.assured("two keys fit the restore metadata budget");
        let charged = executor.snapshot().restore_metadata_memory.reserved_bytes;
        assert!(
            charged > 3 * 1024 * 1024,
            "the resident key values are charged: {charged}"
        );
        drop(restored);
        assert_eq!(
            executor.snapshot().restore_metadata_memory.reserved_bytes,
            0
        );

        let occupied = executor
            .try_reserve(MemoryClass::RestoreMetadata, budget - 2 * 1024 * 1024)
            .assured("the test occupies most of the restore metadata budget");
        let restored = ArchivedDeduplicatorKeys::new(&executor).assured("one growth step fits");
        let (_, admitted) = admit_into(&executor, restored, batch).await;
        let error = admitted.expect_err("a keyspace the budget cannot hold is refused");
        assert_eq!(
            error.current_context(),
            &DeduplicatorArchiveError::Admission
        );
        drop(occupied);
        assert_eq!(
            executor.snapshot().restore_metadata_memory.reserved_bytes,
            0
        );
    }
}
