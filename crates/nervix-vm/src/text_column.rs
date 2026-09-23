//! A STRING column built from values whose length an argument chooses.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The text one STRING column holds, and refusing a value the column under
//!   construction has no room left for, so a builtin can size a value before it allocates it.
//! - **Depends on.** Arrow's string builder.
//! - **Must not know.** Registers, programs, spans, or how a refused value is reported.

use std::num::NonZeroUsize;

use arrow_array::{StringArray, builder::StringBuilder};

/// A STRING column under construction that never holds more text than an Arrow string array
/// addresses.
///
/// An Arrow string array records where each value ends as an `i32` offset into its text, so one
/// column holds at most `i32::MAX` bytes over all of its rows. A value that every row of a batch
/// shares is computed once, as a one-row column, and expanded into one row per message wherever a
/// message-by-message operation or an output field needs one, so it is charged once for every row
/// it stands for.
pub(crate) struct TextColumnBuilder {
    builder: StringBuilder,
    /// How many rows of the finished column each value appended here stands for.
    rows_per_value: NonZeroUsize,
}

impl TextColumnBuilder {
    /// A column appending to `builder` that charges each value once for every one of
    /// `rows_per_value` rows, counting the text `builder` already holds.
    pub(crate) fn new(builder: StringBuilder, rows_per_value: NonZeroUsize) -> Self {
        Self {
            builder,
            rows_per_value,
        }
    }

    /// Whether a value of `bytes` bytes fits in the text the column has left.
    pub(crate) fn fits(&self, bytes: usize) -> bool {
        let Some(charged) = bytes.checked_mul(self.rows_per_value.get()) else {
            return false;
        };
        let Some(total) = self.builder.values_slice().len().checked_add(charged) else {
            return false;
        };
        // The offset after the column's last value is the length of all of its text.
        i32::try_from(total).is_ok()
    }

    /// Appends `value`, or answers false without appending anything when it does not fit in the
    /// text the column has left.
    #[must_use]
    pub(crate) fn append_value(&mut self, value: &str) -> bool {
        if !self.fits(value.len()) {
            return false;
        }
        self.builder.append_value(value);
        true
    }

    pub(crate) fn append_null(&mut self) {
        self.builder.append_null();
    }

    pub(crate) fn finish(mut self) -> StringArray {
        self.builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use arrow_array::{Array, builder::StringBuilder};

    use super::TextColumnBuilder;

    /// The most text one Arrow string array addresses.
    fn column_bytes() -> usize {
        usize::try_from(i32::MAX).expect("i32::MAX is positive")
    }

    #[test]
    fn a_value_fits_up_to_the_text_a_string_array_addresses() {
        let column = TextColumnBuilder::new(StringBuilder::new(), NonZeroUsize::MIN);

        assert!(column.fits(column_bytes()));
        assert!(!column.fits(column_bytes() + 1));
        assert!(!column.fits(usize::MAX));
    }

    #[test]
    fn values_share_the_text_of_their_column() {
        let mut column = TextColumnBuilder::new(StringBuilder::new(), NonZeroUsize::MIN);
        assert!(column.append_value("abc"));

        assert!(column.fits(column_bytes() - 3));
        assert!(!column.fits(column_bytes() - 2));
    }

    #[test]
    fn a_shared_value_is_charged_for_every_row_it_stands_for() {
        let rows = NonZeroUsize::new(3).expect("three is nonzero");
        let column = TextColumnBuilder::new(StringBuilder::new(), rows);

        // 715,827,882 bytes three times is 2,147,483,646 bytes, one byte short of the limit.
        assert!(column.fits(715_827_882));
        assert!(!column.fits(715_827_883));
        assert!(!column.fits(usize::MAX / 2));
    }

    #[test]
    fn a_refused_value_appends_nothing() {
        // A value standing for every row a `usize` counts cannot fit unless it is empty.
        let mut column = TextColumnBuilder::new(StringBuilder::new(), NonZeroUsize::MAX);

        assert!(!column.append_value("refused"));
        column.append_null();
        assert!(column.append_value(""));

        let finished = column.finish();
        assert_eq!(finished.len(), 2);
        assert!(finished.is_null(0));
        assert_eq!(finished.value(1), "");
    }
}
