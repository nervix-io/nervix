//! Fixed window collection dimensions at the Arrow type boundary.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Turning a positive count width into an addressable fixed collection type.
//! - **Depends on.** Arrow types and the caller's declared width.
//! - **Must not know.** Models, output targets, graph validation or retained runtime rows.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns fixed collection type representation")
)]

use std::num::NonZeroU64;

use arrow_schema::{DataType, Field};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::StdArc;
use thiserror::Error;

/// The declared number of argument values in one fixed collection.
pub(crate) struct FixedWindowCollection {
    width: NonZeroU64,
}

#[derive(Debug, Error)]
pub(crate) enum WindowCollectionTypeError {
    #[error("COLLECT_ARRAY cannot represent the declared width {width} in an Arrow fixed array")]
    Width { width: u64 },
    #[error(
        "COLLECT_ARRAY with width {width} exceeds Arrow field lengths at argument depth {depth}"
    )]
    FieldLength { width: u64, depth: usize },
}

impl From<NonZeroU64> for FixedWindowCollection {
    fn from(width: NonZeroU64) -> Self {
        Self { width }
    }
}

impl FixedWindowCollection {
    pub(crate) fn output_type(
        self,
        element: DataType,
    ) -> error_stack::Result<DataType, WindowCollectionTypeError> {
        let width = self.width.get();
        let length =
            i32::try_from(width).change_context(WindowCollectionTypeError::Width { width })?;
        let mut count = width;
        let mut depth = 0_usize;
        let mut nested = &element;
        // Each consecutive fixed level necessarily contributes this many child values. A
        // variable level may be empty, so deeper lengths depend on data and stay emission checks.
        while let DataType::FixedSizeList(child, child_width) = nested {
            depth = depth
                .checked_add(1)
                .assured("an addressable Arrow type has an addressable depth");
            let child_width = u64::try_from(*child_width)
                .change_context(WindowCollectionTypeError::FieldLength { width, depth })?;
            count = count.checked_mul(child_width).ok_or_else(|| {
                Report::new(WindowCollectionTypeError::FieldLength { width, depth })
            })?;
            if count > u64::try_from(i32::MAX).assured("positive Arrow field lengths fit u64") {
                return Err(Report::new(WindowCollectionTypeError::FieldLength {
                    width,
                    depth,
                }));
            }
            nested = child.data_type();
        }
        Ok(DataType::FixedSizeList(
            StdArc::new(Field::new("item", element, false)),
            length,
        ))
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "fixed collection representation boundaries")
)]
mod tests {
    //! Layer: harness.
    //! - **Owns.** Intrinsic Arrow dimension bounds without allocating payloads.
    //! - **Depends on.** The production fixed collection type projection.
    //! - **Must not know.** Graph acceptance or node memory policy.

    use super::*;

    #[test]
    fn fixed_collection_checks_the_width_before_creating_an_arrow_type() {
        let limit = u64::try_from(i32::MAX).assured("the positive Arrow bound fits u64");
        let width = NonZeroU64::new(limit).assured("the Arrow bound is positive");
        let output = FixedWindowCollection::from(width)
            .output_type(DataType::Int64)
            .assured("the last representable fixed width is valid");
        assert!(matches!(output, DataType::FixedSizeList(_, length) if length == i32::MAX));
        let beyond = limit
            .checked_add(1)
            .assured("i32 maximum plus one fits u64");
        let width = NonZeroU64::new(beyond).assured("the width remains positive");
        let error = FixedWindowCollection::from(width)
            .output_type(DataType::Int64)
            .expect_err("a fixed width cannot exceed i32");
        assert!(
            matches!(error.current_context(), WindowCollectionTypeError::Width { width } if *width == beyond)
        );
        assert!(error.contains::<std::num::TryFromIntError>());
    }

    #[test]
    fn fixed_collection_checks_necessarily_present_nested_field_lengths() {
        let pair =
            DataType::FixedSizeList(StdArc::new(Field::new("item", DataType::Float32, false)), 2);
        let limit = u64::try_from(i32::MAX).assured("the positive Arrow bound fits u64");
        let safe = limit / 2;
        FixedWindowCollection::from(
            NonZeroU64::new(safe).assured("the safe pair count is positive"),
        )
        .output_type(pair.clone())
        .assured("the last fitting pair count has addressable children");
        let beyond = safe.checked_add(1).assured("the boundary count fits u64");
        let error = FixedWindowCollection::from(
            NonZeroU64::new(beyond).assured("the boundary count is positive"),
        )
        .output_type(pair)
        .expect_err("the outer width fits but its children do not");
        assert!(
            matches!(error.current_context(), WindowCollectionTypeError::FieldLength { width, depth: 1 } if *width == beyond)
        );
    }
}
