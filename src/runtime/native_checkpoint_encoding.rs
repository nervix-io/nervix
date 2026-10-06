//! Streaming serialization of the current native checkpoint shape.
//!
//! Layer: data plane.
//! - **Owns.** An iterator's archived-vector representation and fixed serializer scratch.
//! - **Depends on.** The state codec and the caller's admitted writer and memory reservation.
//! - **Must not know.** Archive records, placement, restoration authority, or database keys.

use std::marker::PhantomData;

use nervix_execution::Cancellation;
use rkyv::{
    Archive, Place, Serialize,
    rancor::Fallible,
    ser::{Allocator, Writer},
    vec::{ArchivedVec, VecResolver},
    with::{ArchiveWith, SerializeWith},
};

/// Cancellation is checked during each entry's serialization, including entries whose children
/// are entirely inline and therefore do not yet call the file writer.
pub(super) struct CancellableEntry<'a, T> {
    value: T,
    cancellation: &'a Cancellation,
}

impl<T: Archive> Archive for CancellableEntry<'_, T> {
    type Archived = T::Archived;
    type Resolver = T::Resolver;

    fn resolve(&self, resolver: Self::Resolver, out: Place<Self::Archived>) {
        self.value.resolve(resolver, out);
    }
}

impl<T, S> Serialize<S> for CancellableEntry<'_, T>
where
    T: Serialize<S>,
    S: Fallible + ?Sized,
    S::Error: rkyv::rancor::Source,
{
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        use rkyv::rancor::Source as _;

        self.cancellation.check().map_err(S::Error::new)?;
        self.value.serialize(serializer)
    }
}

#[derive(Clone)]
pub(super) struct CancellableIterator<'a, I> {
    pub(super) entries: I,
    pub(super) cancellation: &'a Cancellation,
}

impl<'a, I: Iterator> Iterator for CancellableIterator<'a, I> {
    type Item = CancellableEntry<'a, I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next().map(|value| CancellableEntry {
            value,
            cancellation: self.cancellation,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries.size_hint()
    }
}

impl<I: ExactSizeIterator> ExactSizeIterator for CancellableIterator<'_, I> {
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Serialize one entry at a time into the same archived vector used by ordinary checkpoints.
/// Only resolvers occupy serializer scratch; neither converted entries nor encoded bytes collect
/// into a second whole-record vector.
pub(super) struct IteratorAsVec<T>(PhantomData<T>);

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

/// Outer entry resolvers overlap the largest individual entry's nested resolvers. The remaining
/// space covers allocator alignment. This is charged with retained restore metadata, separately
/// from the fixed bulk I/O working set.
pub(super) fn scratch_bytes<T: Archive>(entries: usize, nested: usize) -> Option<usize> {
    entries
        .checked_mul(std::mem::size_of::<T::Resolver>())?
        .checked_add(nested)?
        .checked_add(256)
}
