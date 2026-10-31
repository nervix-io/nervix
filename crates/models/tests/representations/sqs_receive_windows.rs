//! An SQS ingestor's parallel receive window keeps its count through every representation, and
//! window input reads as a window in range or fails with a typed error.
//!
//! A window is 1 through `SqsReceiveMax::MAX`, the most messages one SQS receive returns.

use std::num::{NonZeroU8, NonZeroU64, NonZeroUsize};

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{SqsReceiveMax, SqsReceiveMaxError};

#[test]
fn bolero_sqs_receive_windows_round_trip_through_every_representation() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(16)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let entropy = arbitrary.entropy();

            let count = entropy.boundary_biased(1..=u64::from(SqsReceiveMax::MAX));
            let window = SqsReceiveMax::try_from(count).expect("the count is in range");
            assert_eq!(u64::from(window), count);
            assert_eq!(window.get(), NonZeroU64::new(count).expect("positive"));
            assert_eq!(
                NonZeroUsize::from(window).get(),
                usize::try_from(count).expect("a window fits a count of messages")
            );
            assert_eq!(window.to_string(), count.to_string());
            let json = serde_json::to_string(&window).expect("a window has a JSON form");
            assert_eq!(json, count.to_string());
            assert_eq!(
                serde_json::from_str::<SqsReceiveMax>(&json).ok(),
                Some(window)
            );
            assert_eq!(crate::archive_round_trip!(&window, SqsReceiveMax), window);
        });
}

#[test]
fn bolero_sqs_receive_window_input_is_validated_or_rejected() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(16)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let entropy = arbitrary.entropy();

            let count = entropy.boundary_biased(0..=u64::from(u8::MAX) + 1);
            let in_range = (1..=u64::from(SqsReceiveMax::MAX)).contains(&count);
            let window = SqsReceiveMax::try_from(count);
            assert_eq!(window.is_ok(), in_range, "{count}");
            if let Err(error) = &window {
                assert_eq!(error, &SqsReceiveMaxError::OutOfRange { value: count });
            }
            assert_eq!(
                serde_json::from_str::<SqsReceiveMax>(&count.to_string()).is_ok(),
                in_range,
                "JSON decoding of {count}"
            );
            let Ok(narrow) = u8::try_from(count) else {
                return;
            };
            let Some(nonzero) = NonZeroU8::new(narrow) else {
                return;
            };
            // The archived window is its count, so an archived count past the maximum reaches the
            // decoder's range check as an out-of-range window would.
            let archived =
                rkyv::to_bytes::<rkyv::rancor::Error>(&nonzero).expect("a count archives");
            assert_eq!(
                rkyv::from_bytes::<SqsReceiveMax, rkyv::rancor::Error>(&archived).is_ok(),
                in_range,
                "archive decoding of {count}"
            );
        });
}
