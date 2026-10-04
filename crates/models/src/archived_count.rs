//! Lossless archived representations of pointer-sized vocabulary counts.
//!
//! Layer: vocabulary.
//! - **Owns.** The 64-bit archive encoding and checked native decoding of counts.
//! - **Depends on.** Integer conversions and serialization primitives.
//! - **Must not know.** Parsing, storage keys, consensus, or runtime execution.

use std::num::{NonZeroU64, NonZeroUsize};

use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{
    Archive, Archived, Place,
    rancor::{Fallible, Source},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};

/// Archives a native count as a fixed-width `u64`, preserving nonzero validation when required.
/// Decoding refuses a value the receiving target's pointer width cannot represent.
pub struct CountAsU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("archived count {count} does not fit the target's usize")]
pub struct ArchivedCountError {
    pub count: u64,
}

impl ArchiveWith<usize> for CountAsU64 {
    type Archived = Archived<u64>;
    type Resolver = ();

    fn resolve_with(field: &usize, (): Self::Resolver, out: Place<Self::Archived>) {
        let count = u64::try_from(*field).assured("supported targets address at most 64 bits");
        count.resolve((), out);
    }
}

impl<S: Fallible + ?Sized> SerializeWith<usize, S> for CountAsU64 {
    fn serialize_with(_: &usize, _: &mut S) -> Result<Self::Resolver, S::Error> {
        Ok(())
    }
}

impl<D: Fallible + ?Sized> DeserializeWith<Archived<u64>, usize, D> for CountAsU64
where
    D::Error: Source,
{
    fn deserialize_with(field: &Archived<u64>, _: &mut D) -> Result<usize, D::Error> {
        let count = field.to_native();
        usize::try_from(count).map_err(|_| D::Error::new(ArchivedCountError { count }))
    }
}

impl ArchiveWith<NonZeroUsize> for CountAsU64 {
    type Archived = Archived<NonZeroU64>;
    type Resolver = ();

    fn resolve_with(field: &NonZeroUsize, (): Self::Resolver, out: Place<Self::Archived>) {
        let count = u64::try_from(field.get()).assured("supported targets address at most 64 bits");
        let count = NonZeroU64::new(count).assured("widening a positive count preserves nonzero");
        count.resolve((), out);
    }
}

impl<S: Fallible + ?Sized> SerializeWith<NonZeroUsize, S> for CountAsU64 {
    fn serialize_with(_: &NonZeroUsize, _: &mut S) -> Result<Self::Resolver, S::Error> {
        Ok(())
    }
}

impl<D: Fallible + ?Sized> DeserializeWith<Archived<NonZeroU64>, NonZeroUsize, D> for CountAsU64
where
    D::Error: Source,
{
    fn deserialize_with(field: &Archived<NonZeroU64>, _: &mut D) -> Result<NonZeroUsize, D::Error> {
        let count = field.to_native().get();
        let count =
            usize::try_from(count).map_err(|_| D::Error::new(ArchivedCountError { count }))?;
        Ok(NonZeroUsize::new(count).assured("checked decoding preserves a nonzero count"))
    }
}
