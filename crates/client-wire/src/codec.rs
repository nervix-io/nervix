//! The encoding and decoding primitives every message family shares.
//!
//! [`Encoder`] is the only way this crate builds a frame. It checks every string and vector
//! against the session limits before the FlatBuffers builder grows, so an encoder can never
//! produce a frame a receiver rejects for size, and never reaches the builder's own hard limit.
//! [`Decoder`] is the only way a verified table becomes an owned value: it applies the limits and
//! the value rules the FlatBuffers verifier does not know.

use std::num::NonZeroU64;

use bytes::Bytes;
use error_stack::Report;
use flatbuffers::{
    FlatBufferBuilder, Follow, ForwardsUOffset, Push, UnionWIPOffset, Vector, WIPOffset,
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::NameError;
use thiserror::Error;

use crate::{
    frame::{EncodedFrame, FrameRoot},
    limits::SessionLimits,
};

/// Bytes a string or vector may add beyond its payload: a length prefix, a null terminator and
/// alignment padding.
const LENGTH_PREFIX_SLACK_BYTES: usize = 16;

/// Why a verified frame does not decode into the value its schema describes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WireDecodeError {
    #[error("`{field}` holds union discriminant {discriminant}, which the schema does not declare")]
    UnknownUnionVariant {
        field: &'static str,
        discriminant: u8,
    },
    #[error("`{field}` holds enum value {value}, which the schema does not declare")]
    UnknownEnumValue { field: &'static str, value: u8 },
    #[error("`{field}` is required")]
    MissingField { field: &'static str },
    #[error("`{field}` must not be zero")]
    ZeroValue { field: &'static str },
    #[error("`{field}` must not be empty")]
    EmptyCollection { field: &'static str },
    #[error("`{field}` holds {actual} entries, above the limit of {limit}")]
    TooManyEntries {
        field: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("`{field}` holds {actual} bytes, above the limit of {limit}")]
    StringTooLong {
        field: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("`{field}` value {value} does not fit this platform")]
    OutOfRange { field: &'static str, value: u64 },
    #[error("`{field}` is not a valid {kind}")]
    InvalidValue {
        field: &'static str,
        kind: &'static str,
    },
    #[error("`{field}` is not a set in canonical order")]
    NonCanonicalSet { field: &'static str },
    #[error("a {actual} frame was decoded where a {expected} was expected")]
    UnexpectedMessage {
        expected: &'static str,
        actual: &'static str,
    },
}

/// Why a value could not be encoded as a frame.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WireEncodeError {
    #[error("`{field}` holds {actual} bytes, above the limit of {limit}")]
    StringTooLong {
        field: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("`{field}` holds {actual} entries, above the limit of {limit}")]
    TooManyEntries {
        field: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("the frame would grow past the limit of {limit} bytes while encoding `{field}`")]
    FrameTooLarge { field: &'static str, limit: usize },
    #[error("`{field}` must not be empty")]
    EmptyCollection { field: &'static str },
    #[error("`{field}` nests tables deeper than the limit of {limit}")]
    NestingTooDeep { field: &'static str, limit: usize },
}

/// An enum value the schema does not declare.
///
/// Nominally public because the schema enum conversions name it, but unreachable outside the
/// crate: callers see [`WireDecodeError::UnknownEnumValue`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("enum value {value} is not declared by the schema")]
pub struct UndeclaredEnumValue {
    pub value: u8,
}

/// Declares the conversions between an enum and the schema enum with the same variant names.
///
/// The encoding direction is an exhaustive match over the Rust enum, so a variant missing here or
/// in the schema is a compile error rather than a value that silently fails to encode.
macro_rules! wire_enum {
    ($(#[$doc:meta])* $all:ident: $value:ty => $wire:ty { $($variant:ident),+ $(,)? }) => {
        impl From<$value> for $wire {
            fn from(value: $value) -> Self {
                match value {
                    $(<$value>::$variant => <$wire>::$variant,)+
                }
            }
        }

        impl TryFrom<$wire> for $value {
            type Error = error_stack::Report<$crate::codec::UndeclaredEnumValue>;

            fn try_from(
                wire: $wire,
            ) -> Result<Self, error_stack::Report<$crate::codec::UndeclaredEnumValue>> {
                match wire {
                    $(<$wire>::$variant => Ok(<$value>::$variant),)+
                    undeclared => Err(error_stack::Report::new(
                        $crate::codec::UndeclaredEnumValue { value: undeclared.0 },
                    )),
                }
            }
        }

        $(#[$doc])*
        #[cfg(test)]
        pub(crate) const $all: &[$value] = &[$(<$value>::$variant),+];
    };
}

pub(crate) use wire_enum;

/// A union member ready to be stored in its owning table.
pub(crate) struct EncodedUnion<D> {
    pub(crate) discriminant: D,
    pub(crate) value: WIPOffset<UnionWIPOffset>,
}

impl<D> EncodedUnion<D> {
    pub(crate) fn new<T>(discriminant: D, value: WIPOffset<T>) -> Self {
        Self {
            discriminant,
            value: value.as_union_value(),
        }
    }
}

/// Builds one frame within a byte limit.
pub(crate) struct Encoder<'fbb> {
    builder: FlatBufferBuilder<'fbb>,
    byte_limit: usize,
    /// Bytes of the limit held back for parts written later, which growth must leave free.
    free_bytes: usize,
    limits: SessionLimits,
}

impl<'fbb> Encoder<'fbb> {
    pub(crate) fn new(byte_limit: usize, limits: &SessionLimits) -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            byte_limit,
            free_bytes: 0,
            limits: *limits,
        }
    }

    /// The builder, for creating tables whose variable-size parts this encoder already created.
    pub(crate) fn fbb(&mut self) -> &mut FlatBufferBuilder<'fbb> {
        &mut self.builder
    }

    /// The bytes encoded so far.
    pub(crate) fn encoded_bytes(&self) -> usize {
        self.builder.unfinished_data().len()
    }

    pub(crate) fn text(
        &mut self,
        field: &'static str,
        value: &str,
    ) -> Result<WIPOffset<&'fbb str>, Report<WireEncodeError>> {
        if value.len() > self.limits.string_bytes() {
            return Err(Report::new(WireEncodeError::StringTooLong {
                field,
                actual: value.len(),
                limit: self.limits.string_bytes(),
            }));
        }
        self.reserve(field, value.len())?;
        Ok(self.builder.create_string(value))
    }

    pub(crate) fn optional_text(
        &mut self,
        field: &'static str,
        value: Option<&str>,
    ) -> Result<Option<WIPOffset<&'fbb str>>, Report<WireEncodeError>> {
        match value {
            Some(value) => Ok(Some(self.text(field, value)?)),
            None => Ok(None),
        }
    }

    pub(crate) fn bytes(
        &mut self,
        field: &'static str,
        value: &[u8],
    ) -> Result<WIPOffset<Vector<'fbb, u8>>, Report<WireEncodeError>> {
        self.reserve(field, value.len())?;
        Ok(self.builder.create_vector(value))
    }

    pub(crate) fn scalars<T>(
        &mut self,
        field: &'static str,
        values: &[T],
    ) -> Result<WIPOffset<Vector<'fbb, T::Output>>, Report<WireEncodeError>>
    where
        T: Push + Copy,
    {
        self.entries(field, values.len())?;
        let payload = values.len().checked_mul(T::size()).assured(
            "a slice of scalars occupies its length times the scalar width in memory, which is at \
             most isize::MAX bytes",
        );
        self.reserve(field, payload)?;
        Ok(self.builder.create_vector(values))
    }

    pub(crate) fn tables<T>(
        &mut self,
        field: &'static str,
        offsets: &[WIPOffset<T>],
    ) -> Result<WIPOffset<Vector<'fbb, ForwardsUOffset<T>>>, Report<WireEncodeError>> {
        self.entries(field, offsets.len())?;
        let payload = offsets.len().checked_mul(size_of::<u32>()).assured(
            "a slice of 32-bit offsets occupies its length times four bytes in memory, which is \
             at most isize::MAX bytes",
        );
        self.reserve(field, payload)?;
        Ok(self.builder.create_vector(offsets))
    }

    /// Encodes every item as a table and collects the tables into a vector, in order.
    ///
    /// The frame is checked after every item, so tables that carry no string or vector of their
    /// own grow it at most one item past the byte limit.
    pub(crate) fn table_vector<T, W>(
        &mut self,
        field: &'static str,
        items: &[T],
        mut encode: impl FnMut(&T, &mut Self) -> Result<WIPOffset<W>, Report<WireEncodeError>>,
    ) -> Result<WIPOffset<Vector<'fbb, ForwardsUOffset<W>>>, Report<WireEncodeError>> {
        self.entries(field, items.len())?;
        let mut offsets = Vec::with_capacity(items.len());
        for item in items {
            let offset = encode(item, self)?;
            self.within_limit(field)?;
            offsets.push(offset);
        }
        self.tables(field, &offsets)
    }

    pub(crate) fn entries(
        &self,
        field: &'static str,
        len: usize,
    ) -> Result<(), Report<WireEncodeError>> {
        if len > self.limits.collection_entries() {
            return Err(Report::new(WireEncodeError::TooManyEntries {
                field,
                actual: len,
                limit: self.limits.collection_entries(),
            }));
        }
        Ok(())
    }

    /// Refuses a table deeper than a receiver verifies. `depth` counts the tables from the frame's
    /// root, which is at depth 1, to this one.
    pub(crate) fn nesting(
        &self,
        field: &'static str,
        depth: usize,
    ) -> Result<(), Report<WireEncodeError>> {
        if depth > self.limits.nesting_depth() {
            return Err(Report::new(WireEncodeError::NestingTooDeep {
                field,
                limit: self.limits.nesting_depth(),
            }));
        }
        Ok(())
    }

    /// Keeps `bytes` of the byte limit free for parts the frame writes last. Growth is checked
    /// against the rest of the limit until another call replaces the amount.
    pub(crate) fn keep_free(&mut self, bytes: usize) {
        self.free_bytes = bytes;
    }

    /// Refuses a frame that has already grown into the bytes kept free or past the byte limit.
    pub(crate) fn within_limit(&self, field: &'static str) -> Result<(), Report<WireEncodeError>> {
        self.check_growth(field, 0)
    }

    /// Refuses to grow the frame by `payload` bytes when that would pass the byte limit.
    pub(crate) fn reserve(
        &self,
        field: &'static str,
        payload: usize,
    ) -> Result<(), Report<WireEncodeError>> {
        let payload = payload.checked_add(LENGTH_PREFIX_SLACK_BYTES).assured(
            "a payload is the length of an in-memory slice or a small table bound, which is at \
             most isize::MAX bytes",
        );
        self.check_growth(field, payload)
    }

    /// Refuses growth by `payload` bytes that would reach into the bytes kept free or pass the
    /// byte limit.
    fn check_growth(
        &self,
        field: &'static str,
        payload: usize,
    ) -> Result<(), Report<WireEncodeError>> {
        let grown = self.encoded_bytes().checked_add(payload).assured(
            "every check holds the builder to at most one item's tables past a byte limit of at \
             most MAX_FRAME_BYTES, and adding at most isize::MAX bytes to that cannot overflow \
             usize",
        );
        let needed = grown.checked_add(self.free_bytes).assured(
            "the bytes kept free are a small envelope plus four bytes per row of an in-memory \
             vector, so the sum stays far below usize::MAX",
        );
        if needed > self.byte_limit {
            return Err(Report::new(WireEncodeError::FrameTooLarge {
                field,
                limit: self.byte_limit,
            }));
        }
        Ok(())
    }

    /// Finishes the frame with root `R`'s identifier.
    pub(crate) fn finish<R: FrameRoot>(
        mut self,
        root: WIPOffset<R::Table<'fbb>>,
    ) -> Result<EncodedFrame<R>, Report<WireEncodeError>> {
        self.builder.finish(root, Some(R::IDENTIFIER));
        let (buffer, head) = self.builder.collapse();
        let bytes = Bytes::from(buffer).slice(head..);
        if bytes.len() > self.byte_limit {
            return Err(Report::new(WireEncodeError::FrameTooLarge {
                field: R::NAME,
                limit: self.byte_limit,
            }));
        }
        Ok(EncodedFrame::new(bytes))
    }
}

/// Reads verified tables into owned values under the session limits.
#[derive(Clone, Copy)]
pub(crate) struct Decoder<'l> {
    limits: &'l SessionLimits,
}

impl<'l> Decoder<'l> {
    pub(crate) fn new(limits: &'l SessionLimits) -> Self {
        Self { limits }
    }

    /// Checks a borrowed string against the string limit.
    pub(crate) fn check_text<'a>(
        &self,
        field: &'static str,
        value: &'a str,
    ) -> Result<&'a str, Report<WireDecodeError>> {
        if value.len() > self.limits.string_bytes() {
            return Err(Report::new(WireDecodeError::StringTooLong {
                field,
                actual: value.len(),
                limit: self.limits.string_bytes(),
            }));
        }
        Ok(value)
    }

    pub(crate) fn text(
        &self,
        field: &'static str,
        value: &str,
    ) -> Result<String, Report<WireDecodeError>> {
        let value = self.check_text(field, value)?;
        Ok(value.to_owned())
    }

    pub(crate) fn optional_text(
        &self,
        field: &'static str,
        value: Option<&str>,
    ) -> Result<Option<String>, Report<WireDecodeError>> {
        match value {
            Some(value) => Ok(Some(self.text(field, value)?)),
            None => Ok(None),
        }
    }

    /// Checks a collection's length against the collection limit.
    pub(crate) fn entries(
        &self,
        field: &'static str,
        len: usize,
    ) -> Result<(), Report<WireDecodeError>> {
        if len > self.limits.collection_entries() {
            return Err(Report::new(WireDecodeError::TooManyEntries {
                field,
                actual: len,
                limit: self.limits.collection_entries(),
            }));
        }
        Ok(())
    }

    /// Decodes every table of a vector, in order.
    pub(crate) fn table_vector<'a, W, T>(
        &self,
        field: &'static str,
        tables: Vector<'a, ForwardsUOffset<W>>,
        mut decode: impl FnMut(W) -> Result<T, Report<WireDecodeError>>,
    ) -> Result<Vec<T>, Report<WireDecodeError>>
    where
        W: Follow<'a, Inner = W> + 'a,
    {
        self.entries(field, tables.len())?;
        let mut values = Vec::with_capacity(tables.len());
        for table in tables.iter() {
            values.push(decode(table)?);
        }
        Ok(values)
    }

    pub(crate) fn name<N>(
        &self,
        field: &'static str,
        value: &str,
    ) -> Result<N, Report<WireDecodeError>>
    where
        N: for<'a> TryFrom<&'a str, Error = NameError>,
    {
        match N::try_from(value) {
            Ok(name) => Ok(name),
            Err(error) => Err(
                Report::new(error).change_context(WireDecodeError::InvalidValue {
                    field,
                    kind: "name",
                }),
            ),
        }
    }

    pub(crate) fn optional_name<N>(
        &self,
        field: &'static str,
        value: Option<&str>,
    ) -> Result<Option<N>, Report<WireDecodeError>>
    where
        N: for<'a> TryFrom<&'a str, Error = NameError>,
    {
        match value {
            Some(value) => Ok(Some(self.name(field, value)?)),
            None => Ok(None),
        }
    }

    /// Requires a `= null` field that the schema says must be present.
    pub(crate) fn required<T>(
        &self,
        field: &'static str,
        value: Option<T>,
    ) -> Result<T, Report<WireDecodeError>> {
        match value {
            Some(value) => Ok(value),
            None => Err(Report::new(WireDecodeError::MissingField { field })),
        }
    }

    pub(crate) fn non_zero(
        &self,
        field: &'static str,
        value: u64,
    ) -> Result<NonZeroU64, Report<WireDecodeError>> {
        match NonZeroU64::new(value) {
            Some(value) => Ok(value),
            None => Err(Report::new(WireDecodeError::ZeroValue { field })),
        }
    }

    /// Converts a wire count or position into this platform's `usize`.
    pub(crate) fn size(
        &self,
        field: &'static str,
        value: u64,
    ) -> Result<usize, Report<WireDecodeError>> {
        match usize::try_from(value) {
            Ok(size) => Ok(size),
            Err(_) => Err(Report::new(WireDecodeError::OutOfRange { field, value })),
        }
    }

    pub(crate) fn enumeration<E, W>(
        &self,
        field: &'static str,
        value: W,
    ) -> Result<E, Report<WireDecodeError>>
    where
        E: TryFrom<W, Error = Report<UndeclaredEnumValue>>,
    {
        match E::try_from(value) {
            Ok(value) => Ok(value),
            Err(undeclared) => {
                let value = undeclared.current_context().value;
                Err(undeclared.change_context(WireDecodeError::UnknownEnumValue { field, value }))
            }
        }
    }

    /// Decodes a `= null` enum field the schema says must be present.
    pub(crate) fn required_enumeration<E, W>(
        &self,
        field: &'static str,
        value: Option<W>,
    ) -> Result<E, Report<WireDecodeError>>
    where
        E: TryFrom<W, Error = Report<UndeclaredEnumValue>>,
    {
        let value = self.required(field, value)?;
        self.enumeration(field, value)
    }

    /// The error for a union discriminant the schema does not declare.
    pub(crate) fn unknown_union(
        &self,
        field: &'static str,
        discriminant: u8,
    ) -> Report<WireDecodeError> {
        Report::new(WireDecodeError::UnknownUnionVariant {
            field,
            discriminant,
        })
    }
}

/// Converts a `usize` count or position into its wire width.
pub(crate) fn wire_size(value: usize) -> u64 {
    u64::try_from(value).assured("usize is at most 64 bits on every target Rust supports")
}
