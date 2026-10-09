//! Kafka offsets and branch lifecycle records written while their entries are produced.
//!
//! Either record can be larger than the memory a node admits for one section. These writers
//! produce the complete section — the record header, then the rkyv payload — while they hold one
//! entry at a time and one serializer resolver per entry, never the entries or the encoded bytes
//! whole. [`ArchiveRecord::encode`] of both kinds writes through them, so a streamed section and an
//! encoded one are the same bytes.
//!
//! rkyv writes a vector in two passes: every entry's out-of-line data first, then the entries
//! themselves. A writer therefore walks its entries twice, through clones of one iterator, and the
//! iterator must produce the same values both times.

use std::{
    cell::Cell,
    io::{self, Write},
    marker::PhantomData,
    mem::MaybeUninit,
};

use error_stack::Report;
use nervix_models::{DomainName, ModelKind, ModelName, SchemaFingerprint};
use rkyv::{
    Archive, Place, Serialize,
    rancor::{self, Fallible, Source},
    ser::{Allocator, Writer, allocator::SubAllocator, writer::IoWriter},
    vec::{ArchivedVec, VecResolver},
    with::{ArchiveWith, SerializeWith},
};
use thiserror::Error;

use crate::{
    error::ArchiveWriteError,
    section::{ArchiveRecord, MAX_RECORD_BYTES, RECORD_MAGIC, RecordKind},
    state::{
        BranchLifecycleEntry, BranchLifecycleRecord, KafkaOffsetsRecord, KafkaPartitionOffset,
    },
    wire::{
        ArchivedBranchLifecycleWire, ArchivedKafkaOffsetsWire, BranchLifecycleEntryWire,
        KafkaPartitionWire, StateField, StateValue,
    },
};

/// Scratch beyond the resolvers themselves, for the alignment of the buffer they are carved from.
const SCRATCH_SLACK_BYTES: usize = 256;

/// A Kafka offsets record whose partition offsets are produced while it is written.
pub struct StreamedKafkaOffsets<I> {
    pub domain: DomainName,
    pub entity: ModelName,
    pub schema: SchemaFingerprint,
    pub revision: u64,
    /// Every partition in topic and partition order. It is walked once for each serializer pass.
    pub offsets: I,
}

/// A branch lifecycle record whose branches are produced while it is written.
pub struct StreamedBranchLifecycle<I> {
    pub domain: DomainName,
    pub owner_kind: ModelKind,
    pub entity: ModelName,
    pub schema: SchemaFingerprint,
    pub revision: u64,
    /// Every branch in LRU order. It is walked once for its scratch and once for each serializer
    /// pass.
    pub branches: I,
}

impl<I> StreamedKafkaOffsets<I>
where
    I: ExactSizeIterator<Item = KafkaPartitionOffset> + Clone,
{
    /// The serializer scratch writing the record takes: one resolver for every partition.
    pub fn scratch_bytes(&self) -> Result<usize, Report<ArchiveWriteError>> {
        record_scratch::<KafkaPartitionWire>(self.offsets.len(), 0, KafkaOffsetsRecord::KIND)
    }

    /// Writes the complete section into `output` and returns its length. `scratch` holds at least
    /// [`Self::scratch_bytes`]. `stop` is asked before each partition, and the write ends with
    /// [`ArchiveWriteError::Interrupted`] once it answers true.
    pub fn write(
        &self,
        scratch: &mut [MaybeUninit<u8>],
        output: &mut dyn Write,
        stop: &dyn Fn() -> bool,
    ) -> Result<u64, Report<ArchiveWriteError>> {
        let stop = Stop::new(stop);
        let root = KafkaOffsetsStream {
            domain: self.domain.as_str().to_string(),
            entity: self.entity.as_str().to_string(),
            schema: *self.schema.as_digest(),
            revision: self.revision,
            offsets: CheckedEntries {
                entries: self.offsets.clone().map(KafkaPartitionWire::from),
                stop: &stop,
            },
        };
        let kind = KafkaOffsetsRecord::KIND;
        write_record(
            kind,
            KafkaOffsetsRecord::VERSION,
            output,
            &stop,
            |section| {
                rkyv::api::low::to_bytes_in_with_alloc::<_, _, rancor::Error>(
                    &root,
                    IoWriter::new(section),
                    SubAllocator::new(scratch),
                )
                .map(|_| ())
            },
        )
    }

    /// The complete section, held whole: what [`ArchiveRecord::encode`] returns.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        let mut scratch = vec![MaybeUninit::uninit(); self.scratch_bytes()?];
        let mut section = Vec::new();
        self.write(&mut scratch, &mut section, &|| false)?;
        Ok(section)
    }
}

impl<I> StreamedBranchLifecycle<I>
where
    I: ExactSizeIterator<Item = BranchLifecycleEntry> + Clone,
{
    /// The serializer scratch writing the record takes: one resolver for every branch beside the
    /// nested resolvers of the branch whose key takes the most.
    pub fn scratch_bytes(&self) -> Result<usize, Report<ArchiveWriteError>> {
        let kind = BranchLifecycleRecord::KIND;
        let mut nested = 0;
        for branch in self.branches.clone() {
            let Some(fields) = &branch.key else {
                continue;
            };
            let Some(key) = fields_scratch(fields) else {
                return Err(Report::new(ArchiveWriteError::Encode { kind }));
            };
            nested = nested.max(key);
        }
        record_scratch::<BranchLifecycleEntryWire>(self.branches.len(), nested, kind)
    }

    /// Writes the complete section into `output` and returns its length. `scratch` holds at least
    /// [`Self::scratch_bytes`]. `stop` is asked before each branch, and the write ends with
    /// [`ArchiveWriteError::Interrupted`] once it answers true.
    pub fn write(
        &self,
        scratch: &mut [MaybeUninit<u8>],
        output: &mut dyn Write,
        stop: &dyn Fn() -> bool,
    ) -> Result<u64, Report<ArchiveWriteError>> {
        let stop = Stop::new(stop);
        let root = BranchLifecycleStream {
            domain: self.domain.as_str().to_string(),
            owner_kind: self.owner_kind.as_str().to_string(),
            entity: self.entity.as_str().to_string(),
            schema: *self.schema.as_digest(),
            revision: self.revision,
            branches: CheckedEntries {
                entries: self.branches.clone().map(BranchLifecycleEntryWire::from),
                stop: &stop,
            },
        };
        let kind = BranchLifecycleRecord::KIND;
        write_record(
            kind,
            BranchLifecycleRecord::VERSION,
            output,
            &stop,
            |section| {
                rkyv::api::low::to_bytes_in_with_alloc::<_, _, rancor::Error>(
                    &root,
                    IoWriter::new(section),
                    SubAllocator::new(scratch),
                )
                .map(|_| ())
            },
        )
    }

    /// The complete section, held whole: what [`ArchiveRecord::encode`] returns.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        let mut scratch = vec![MaybeUninit::uninit(); self.scratch_bytes()?];
        let mut section = Vec::new();
        self.write(&mut scratch, &mut section, &|| false)?;
        Ok(section)
    }
}

/// The current Kafka offsets wire root, with its partitions taken from an iterator. Its fields are
/// declared in the wire shape's order, which is the order rkyv writes them in.
#[derive(Archive, Serialize)]
#[rkyv(as = ArchivedKafkaOffsetsWire)]
#[rkyv(serialize_bounds(__S: Writer + Allocator, __S::Error: Source))]
struct KafkaOffsetsStream<'a, I: ExactSizeIterator<Item = KafkaPartitionWire> + Clone> {
    domain: String,
    entity: String,
    schema: [u8; 32],
    revision: u64,
    #[rkyv(with = IteratorAsVec<CheckedEntry<'a, KafkaPartitionWire>>, omit_bounds)]
    offsets: CheckedEntries<'a, I>,
}

/// The current branch lifecycle wire root, with its branches taken from an iterator. Its fields
/// are declared in the wire shape's order, which is the order rkyv writes them in.
#[derive(Archive, Serialize)]
#[rkyv(as = ArchivedBranchLifecycleWire)]
#[rkyv(serialize_bounds(__S: Writer + Allocator, __S::Error: Source))]
struct BranchLifecycleStream<'a, I: ExactSizeIterator<Item = BranchLifecycleEntryWire> + Clone> {
    domain: String,
    owner_kind: String,
    entity: String,
    schema: [u8; 32],
    revision: u64,
    #[rkyv(with = IteratorAsVec<CheckedEntry<'a, BranchLifecycleEntryWire>>, omit_bounds)]
    branches: CheckedEntries<'a, I>,
}

/// The caller's answer to whether a write should end, and whether it ended one.
struct Stop<'a> {
    asked: &'a dyn Fn() -> bool,
    stopped: Cell<bool>,
}

impl<'a> Stop<'a> {
    fn new(asked: &'a dyn Fn() -> bool) -> Self {
        Self {
            asked,
            stopped: Cell::new(false),
        }
    }

    /// Asks the caller, and remembers a request to stop so the failure it causes is reported as
    /// the interruption it is.
    fn requested(&self) -> bool {
        if (self.asked)() {
            self.stopped.set(true);
        }
        self.stopped.get()
    }
}

/// Why the serializer ended a write early: its caller asked it to stop.
#[derive(Debug, Error)]
#[error("the record's writer was asked to stop")]
struct Stopped;

/// One entry that asks its writer's [`Stop`] before it is serialized, including entries whose
/// data is entirely inline and so never reach the output on the first pass.
struct CheckedEntry<'a, T> {
    value: T,
    stop: &'a Stop<'a>,
}

impl<T: Archive> Archive for CheckedEntry<'_, T> {
    type Archived = T::Archived;
    type Resolver = T::Resolver;

    fn resolve(&self, resolver: Self::Resolver, out: Place<Self::Archived>) {
        self.value.resolve(resolver, out);
    }
}

impl<T, S> Serialize<S> for CheckedEntry<'_, T>
where
    T: Serialize<S>,
    S: Fallible + ?Sized,
    S::Error: Source,
{
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        if self.stop.requested() {
            return Err(S::Error::new(Stopped));
        }
        self.value.serialize(serializer)
    }
}

#[derive(Clone)]
struct CheckedEntries<'a, I> {
    entries: I,
    stop: &'a Stop<'a>,
}

impl<'a, I: Iterator> Iterator for CheckedEntries<'a, I> {
    type Item = CheckedEntry<'a, I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.entries.next()?;
        Some(CheckedEntry {
            value,
            stop: self.stop,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries.size_hint()
    }
}

impl<I: ExactSizeIterator> ExactSizeIterator for CheckedEntries<'_, I> {
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Serializes an iterator into the archived vector a collected `Vec` of its items archives as.
/// Only the items' resolvers occupy serializer scratch.
struct IteratorAsVec<T>(PhantomData<T>);

impl<I, T> ArchiveWith<I> for IteratorAsVec<T>
where
    I: ExactSizeIterator<Item = T>,
    T: Archive,
{
    type Archived = ArchivedVec<T::Archived>;
    type Resolver = VecResolver;

    fn resolve_with(field: &I, resolver: VecResolver, out: Place<Self::Archived>) {
        ArchivedVec::resolve_from_len(field.len(), resolver, out);
    }
}

impl<I, T, S> SerializeWith<I, S> for IteratorAsVec<T>
where
    I: ExactSizeIterator<Item = T> + Clone,
    T: Serialize<S>,
    S: Fallible + Allocator + Writer + ?Sized,
{
    fn serialize_with(field: &I, serializer: &mut S) -> Result<VecResolver, S::Error> {
        ArchivedVec::serialize_from_iter(field.clone(), serializer)
    }
}

/// The section a record is written into. It counts every byte, forwards only those within the
/// record limit, and remembers whether its destination failed, so a record that exceeds the limit
/// still learns its complete length.
struct RecordOutput<'a> {
    output: &'a mut dyn Write,
    written: u64,
    failed: bool,
}

impl Write for RecordOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = u64::try_from(bytes.len()).map_err(io::Error::other)?;
        let Some(written) = self.written.checked_add(length) else {
            return Err(io::Error::other("a record's length exceeds 64 bits"));
        };
        if written <= MAX_RECORD_BYTES
            && let Err(error) = self.output.write_all(bytes)
        {
            self.failed = true;
            return Err(error);
        }
        self.written = written;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Err(error) = self.output.flush() {
            self.failed = true;
            return Err(error);
        }
        Ok(())
    }
}

/// Writes the header of a `kind` record at `version`, then the payload `serialize` writes, and
/// classifies a failure by what caused it.
fn write_record(
    kind: RecordKind,
    version: u16,
    output: &mut dyn Write,
    stop: &Stop<'_>,
    serialize: impl FnOnce(&mut RecordOutput<'_>) -> Result<(), rancor::Error>,
) -> Result<u64, Report<ArchiveWriteError>> {
    let mut section = RecordOutput {
        output,
        written: 0,
        failed: false,
    };
    let mut header = Vec::with_capacity(RECORD_MAGIC.len() + 4);
    header.extend_from_slice(&RECORD_MAGIC);
    header.extend_from_slice(&kind.tag().to_le_bytes());
    header.extend_from_slice(&version.to_le_bytes());
    if let Err(error) = section.write_all(&header) {
        return Err(Report::new(error).change_context(ArchiveWriteError::Write));
    }
    let serialized = serialize(&mut section);
    if let Err(error) = serialized {
        let context = if stop.stopped.get() {
            ArchiveWriteError::Interrupted { kind }
        } else if section.failed {
            ArchiveWriteError::Write
        } else {
            ArchiveWriteError::Encode { kind }
        };
        return Err(Report::new(error).change_context(context));
    }
    if section.written > MAX_RECORD_BYTES {
        return Err(Report::new(ArchiveWriteError::RecordTooLarge {
            kind,
            length: section.written,
            limit: MAX_RECORD_BYTES,
        }));
    }
    Ok(section.written)
}

/// The scratch of a record vector of `entries` entries of `T`: their resolvers and the alignment
/// their allocation may need, beside `nested` bytes for the entry whose own vectors take the most.
fn record_scratch<T: Archive>(
    entries: usize,
    nested: usize,
    kind: RecordKind,
) -> Result<usize, Report<ArchiveWriteError>> {
    let overflow = || Report::new(ArchiveWriteError::Encode { kind });
    let Some(resolvers) = vector_scratch::<T>(entries) else {
        return Err(overflow());
    };
    let Some(scratch) = resolvers.checked_add(nested) else {
        return Err(overflow());
    };
    scratch
        .checked_add(SCRATCH_SLACK_BYTES)
        .ok_or_else(overflow)
}

/// The scratch one vector of `entries` entries of `T` allocates while it is serialized: one
/// resolver per entry, aligned for the resolver.
fn vector_scratch<T: Archive>(entries: usize) -> Option<usize> {
    let resolvers = entries.checked_mul(std::mem::size_of::<T::Resolver>())?;
    resolvers.checked_add(std::mem::align_of::<T::Resolver>())
}

/// The scratch a branch key's field vector takes at its deepest: the fields' resolvers, and the
/// nested values of the field that takes the most. Sibling vectors are serialized one after
/// another and release their scratch in turn.
fn fields_scratch(fields: &[StateField]) -> Option<usize> {
    let mut deepest = 0;
    for field in fields {
        deepest = deepest.max(value_scratch(&field.value)?);
    }
    vector_scratch::<StateField>(fields.len())?.checked_add(deepest)
}

fn value_scratch(value: &StateValue) -> Option<usize> {
    let (StateValue::Array(values) | StateValue::Vec(values)) = value else {
        return Some(0);
    };
    let mut deepest = 0;
    for value in values {
        deepest = deepest.max(value_scratch(value)?);
    }
    vector_scratch::<StateValue>(values.len())?.checked_add(deepest)
}

impl From<KafkaPartitionOffset> for KafkaPartitionWire {
    fn from(offset: KafkaPartitionOffset) -> Self {
        Self {
            topic: offset.topic,
            partition: offset.partition,
            next_offset: offset.next_offset,
        }
    }
}

impl From<BranchLifecycleEntry> for BranchLifecycleEntryWire {
    fn from(branch: BranchLifecycleEntry) -> Self {
        Self {
            key: branch.key,
            last_ingestion_unix_nanos: branch.last_ingestion.unix_nanos(),
            incarnation: branch.incarnation,
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;
    use crate::{
        archive_values::Values,
        section::encode_record,
        wire::{BranchLifecycleWire, KafkaOffsetsWire},
    };

    /// The section the record's complete wire shape encodes to through rkyv's own collected vector.
    fn wire_offsets(record: &KafkaOffsetsRecord) -> Vec<u8> {
        let wire = KafkaOffsetsWire {
            domain: record.domain.as_str().to_string(),
            entity: record.entity.as_str().to_string(),
            schema: *record.schema.as_digest(),
            revision: record.revision,
            offsets: record
                .offsets
                .iter()
                .cloned()
                .map(KafkaPartitionWire::from)
                .collect(),
        };
        encode_record(KafkaOffsetsRecord::KIND, KafkaOffsetsRecord::VERSION, &wire)
            .assured("a bounded generated offsets wire encodes")
    }

    /// The section the record's complete wire shape encodes to through rkyv's own collected vector.
    fn wire_lifecycle(record: &BranchLifecycleRecord) -> Vec<u8> {
        let wire = BranchLifecycleWire {
            domain: record.domain.as_str().to_string(),
            owner_kind: record.owner_kind.as_str().to_string(),
            entity: record.entity.as_str().to_string(),
            schema: *record.schema.as_digest(),
            revision: record.revision,
            branches: record
                .branches
                .iter()
                .cloned()
                .map(BranchLifecycleEntryWire::from)
                .collect(),
        };
        encode_record(
            BranchLifecycleRecord::KIND,
            BranchLifecycleRecord::VERSION,
            &wire,
        )
        .assured("a bounded generated lifecycle wire encodes")
    }

    fn streamed_offsets(
        record: &KafkaOffsetsRecord,
    ) -> StreamedKafkaOffsets<std::vec::IntoIter<KafkaPartitionOffset>> {
        StreamedKafkaOffsets {
            domain: record.domain.clone(),
            entity: record.entity.clone(),
            schema: record.schema,
            revision: record.revision,
            offsets: record.offsets.clone().into_iter(),
        }
    }

    fn streamed_lifecycle(
        record: &BranchLifecycleRecord,
    ) -> StreamedBranchLifecycle<std::vec::IntoIter<BranchLifecycleEntry>> {
        StreamedBranchLifecycle {
            domain: record.domain.clone(),
            owner_kind: record.owner_kind,
            entity: record.entity.clone(),
            schema: record.schema,
            revision: record.revision,
            branches: record.branches.clone().into_iter(),
        }
    }

    /// Writes a streamed section into memory with exactly the scratch the writer declares.
    fn write_section(
        scratch_bytes: usize,
        write: impl FnOnce(
            &mut [MaybeUninit<u8>],
            &mut dyn Write,
        ) -> Result<u64, Report<ArchiveWriteError>>,
    ) -> Vec<u8> {
        let mut scratch = vec![MaybeUninit::uninit(); scratch_bytes];
        let mut section = Vec::new();
        let length = write(&mut scratch, &mut section).assured("a bounded current record streams");
        assert_eq!(
            length,
            u64::try_from(section.len()).assured("a bounded section length fits 64 bits"),
            "the writer reports the length it wrote"
        );
        section
    }

    #[test]
    fn bolero_streamed_records_match_their_wire_encoding() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(4096)
            .for_each(|bytes| {
                let mut values = Values::new(bytes);
                let domain = values.0.name();
                let offsets = values.offsets(domain);
                let stream = streamed_offsets(&offsets);
                let scratch = stream
                    .scratch_bytes()
                    .assured("bounded offsets have addressable scratch");
                let section = write_section(scratch, |scratch, output| {
                    stream.write(scratch, output, &|| false)
                });
                assert_eq!(section, wire_offsets(&offsets));
                assert_eq!(
                    KafkaOffsetsRecord::decode("synthetic.rkyv", &section)
                        .assured("the owning validator accepts the streamed offsets"),
                    offsets
                );

                let domain = values.0.name();
                let lifecycle = values.lifecycle(domain);
                let stream = streamed_lifecycle(&lifecycle);
                let scratch = stream
                    .scratch_bytes()
                    .assured("bounded branches have addressable scratch");
                let section = write_section(scratch, |scratch, output| {
                    stream.write(scratch, output, &|| false)
                });
                assert_eq!(section, wire_lifecycle(&lifecycle));
                assert_eq!(
                    BranchLifecycleRecord::decode("synthetic.rkyv", &section)
                        .assured("the owning validator accepts the streamed lifecycle"),
                    lifecycle
                );
            });
    }

    fn lifecycle_with_keys(keys: Vec<Vec<StateField>>) -> BranchLifecycleRecord {
        let mut branches = Vec::new();
        for (incarnation, key) in (1..).zip(keys) {
            branches.push(BranchLifecycleEntry {
                key: Some(key),
                last_ingestion: nervix_models::Timestamp::from_unix_nanos(incarnation),
                incarnation: incarnation.cast_unsigned(),
            });
        }
        BranchLifecycleRecord {
            domain: DomainName::parse("payments").assured("the domain is an accepted literal"),
            owner_kind: ModelKind::Ingestor,
            entity: ModelName::parse("source").assured("the entity is an accepted literal"),
            schema: SchemaFingerprint::from_digest([3; 32]),
            revision: 9,
            branches,
        }
    }

    /// Nested arrays beside wide sibling arrays, so the deepest key is not the widest one.
    fn nested(depth: usize) -> StateValue {
        let mut value = StateValue::U64(7);
        for level in 0..depth {
            let mut siblings = vec![value];
            for sibling in 0..level {
                siblings.push(StateValue::String(format!("sibling-{sibling}")));
            }
            value = if level % 2 == 0 {
                StateValue::Array(siblings)
            } else {
                StateValue::Vec(siblings)
            };
        }
        value
    }

    #[test]
    fn the_declared_scratch_holds_the_deepest_and_the_widest_keys() {
        let deep = vec![StateField {
            name: "deep".to_string(),
            value: nested(48),
        }];
        let wide = (0..512)
            .map(|index| StateField {
                name: format!("field_{index:04}"),
                value: StateValue::Vec(vec![StateValue::I32(index); 4]),
            })
            .collect();
        let record = lifecycle_with_keys(vec![wide, deep, Vec::new()]);
        let stream = streamed_lifecycle(&record);
        let scratch = stream.scratch_bytes().assured("bounded keys have scratch");
        let section = write_section(scratch, |scratch, output| {
            stream.write(scratch, output, &|| false)
        });
        assert_eq!(section, wire_lifecycle(&record));
    }

    #[test]
    fn scratch_smaller_than_declared_fails_to_encode() {
        let record = lifecycle_with_keys(vec![vec![StateField {
            name: "tenant".to_string(),
            value: StateValue::Array(vec![StateValue::U8(1), StateValue::U8(2)]),
        }]]);
        let stream = streamed_lifecycle(&record);
        let mut scratch = Vec::new();
        let failure = stream
            .write(&mut scratch, &mut Vec::new(), &|| false)
            .err()
            .assured("resolvers need scratch");
        assert_eq!(
            failure.current_context(),
            &ArchiveWriteError::Encode {
                kind: RecordKind::BranchLifecycle
            }
        );
    }

    #[test]
    fn a_stop_request_ends_the_write_as_an_interruption() {
        let record = lifecycle_with_keys(vec![Vec::new(), Vec::new(), Vec::new()]);
        let stream = streamed_lifecycle(&record);
        let mut scratch = vec![
            MaybeUninit::uninit();
            stream.scratch_bytes().assured("bounded keys have scratch")
        ];
        let asked = Cell::new(0_u32);
        let stop = || {
            asked.set(asked.get() + 1);
            asked.get() > 1
        };
        let mut section = Vec::new();
        let failure = stream
            .write(&mut scratch, &mut section, &stop)
            .err()
            .assured("the second branch is never written");
        assert_eq!(
            failure.current_context(),
            &ArchiveWriteError::Interrupted {
                kind: RecordKind::BranchLifecycle
            }
        );
        assert_eq!(asked.get(), 2, "the writer stops at the first request");
        assert!(
            section.starts_with(&RECORD_MAGIC),
            "the interrupted section began with its header"
        );
    }

    /// A destination that accepts `remaining` bytes and then fails.
    struct FailingOutput {
        remaining: usize,
    }

    impl Write for FailingOutput {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.remaining {
                return Err(io::Error::other("the destination is full"));
            }
            self.remaining -= bytes.len();
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_failed_destination_is_a_write_failure() {
        let record = lifecycle_with_keys(vec![vec![StateField {
            name: "tenant".to_string(),
            value: StateValue::String("a tenant name longer than inline text".to_string()),
        }]]);
        let stream = streamed_lifecycle(&record);
        let mut scratch = vec![
            MaybeUninit::uninit();
            stream.scratch_bytes().assured("bounded keys have scratch")
        ];
        for remaining in [0, RECORD_MAGIC.len(), 40] {
            let failure = stream
                .write(&mut scratch, &mut FailingOutput { remaining }, &|| false)
                .err()
                .assured("the destination refuses the record");
            assert_eq!(failure.current_context(), &ArchiveWriteError::Write);
        }
    }

    /// Counts what reaches the destination without keeping it.
    #[derive(Default)]
    struct CountingOutput {
        received: u64,
    }

    impl Write for CountingOutput {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.received += u64::try_from(bytes.len()).assured("a write length fits 64 bits");
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_record_above_the_limit_reports_its_whole_length_and_forwards_no_more_than_the_limit() {
        let topic = "t".repeat(200);
        let stream = StreamedKafkaOffsets {
            domain: DomainName::parse("payments").assured("the domain is an accepted literal"),
            entity: ModelName::parse("source").assured("the entity is an accepted literal"),
            schema: SchemaFingerprint::from_digest([5; 32]),
            revision: 3,
            offsets: (0..310_000).map(move |partition| KafkaPartitionOffset {
                topic: topic.clone(),
                partition,
                next_offset: i64::from(partition),
            }),
        };
        let mut scratch = vec![
            MaybeUninit::uninit();
            stream
                .scratch_bytes()
                .assured("the offsets have addressable scratch")
        ];
        let mut output = CountingOutput::default();
        let failure = stream
            .write(&mut scratch, &mut output, &|| false)
            .err()
            .assured("the record exceeds the record limit");
        let ArchiveWriteError::RecordTooLarge {
            kind,
            length,
            limit,
        } = failure.current_context()
        else {
            panic!("an oversized record is refused for its size: {failure:?}");
        };
        assert_eq!(*kind, RecordKind::KafkaOffsets);
        assert_eq!(*limit, MAX_RECORD_BYTES);
        assert!(
            *length > 310_000 * 224,
            "the whole record was measured: {length}"
        );
        assert!(output.received <= MAX_RECORD_BYTES);
    }
}
