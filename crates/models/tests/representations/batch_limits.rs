//! An emitter's batch limits keep their values through every representation, and limit input
//! reads as a limit in range or fails with a typed error.
//!
//! A message limit is 1 through `BatchMessageLimit::MAX`. A size limit is a positive whole number
//! of a byte unit whose product fits in a `u64`; it keeps the unit it was written in. Size text
//! also accepts leading zeros and a unit in any case and writes neither, which is a canonicalizing
//! contract tested on arbitrary text.

use std::num::{NonZeroU32, NonZeroU64};

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{
    BatchMessageLimit, ByteSizeUnit, EmitterBatchLimitError, EmitterBatchPolicy, PayloadSizeLimit,
};

const UNITS: [ByteSizeUnit; 9] = [
    ByteSizeUnit::B,
    ByteSizeUnit::KB,
    ByteSizeUnit::KiB,
    ByteSizeUnit::MB,
    ByteSizeUnit::MiB,
    ByteSizeUnit::GB,
    ByteSizeUnit::GiB,
    ByteSizeUnit::TB,
    ByteSizeUnit::TiB,
];

/// Unit spellings arbitrary size text is written with, beside units no size takes.
const UNIT_SPELLINGS: [&str; 12] = [
    "B", "kb", "KiB", "MB", "mib", "GB", "GiB", "tb", "TiB", "PiB", "", "bytes",
];

#[test]
fn bolero_batch_limits_round_trip_through_every_representation() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let entropy = arbitrary.entropy();

            let count = entropy.boundary_biased(1..=u64::from(BatchMessageLimit::MAX));
            let messages = BatchMessageLimit::try_from(count).expect("the count is in range");
            assert_eq!(u64::from(u32::from(messages)), count);
            assert_eq!(messages.to_string(), count.to_string());
            let json = serde_json::to_string(&messages).expect("a limit has a JSON form");
            assert_eq!(
                serde_json::from_str::<BatchMessageLimit>(&json).ok(),
                Some(messages)
            );
            assert_eq!(
                crate::archive_round_trip!(&messages, BatchMessageLimit),
                messages
            );

            let unit = entropy.pick(UNITS);
            assert_eq!(unit.to_string().parse::<ByteSizeUnit>().ok(), Some(unit));
            let most = u64::MAX / unit.bytes();
            let count = NonZeroU64::new(entropy.boundary_biased(1..=most)).expect("positive");
            let size = PayloadSizeLimit::new(count, unit).expect("the product fits in a u64");
            let text = size.to_string();
            assert_eq!(text, format!("{count}{unit}"));
            assert_eq!(text.parse::<PayloadSizeLimit>().ok(), Some(size), "{text}");
            let json = serde_json::to_string(&size).expect("a size has a JSON form");
            assert_eq!(
                serde_json::from_str::<PayloadSizeLimit>(&json).ok(),
                Some(size)
            );
            assert_eq!(crate::archive_round_trip!(&size, PayloadSizeLimit), size);

            let policy = EmitterBatchPolicy {
                max_messages: messages,
                max_size: size,
            };
            assert_eq!(
                crate::archive_round_trip!(&policy, EmitterBatchPolicy),
                policy
            );
        });
}

/// The archived layout of a size limit, written field for field so a test can archive a byte
/// count its unit does not divide and hand it to the decoder.
#[derive(rkyv::Archive, rkyv::Serialize)]
struct ArchivedSizeFields {
    bytes: NonZeroU64,
    unit: ByteSizeUnit,
}

#[test]
fn bolero_batch_limit_input_is_validated_or_rejected() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let entropy = arbitrary.entropy();

            let count = entropy.boundary_biased(0..=u64::from(u32::MAX) + 1);
            let in_range = (1..=u64::from(BatchMessageLimit::MAX)).contains(&count);
            let limit = BatchMessageLimit::try_from(count);
            assert_eq!(limit.is_ok(), in_range, "{count}");
            if let Err(error) = &limit {
                assert_eq!(
                    error,
                    &EmitterBatchLimitError::MessageLimitOutOfRange { value: count }
                );
            }
            if let Ok(narrow) = u32::try_from(count) {
                let json = narrow.to_string();
                assert_eq!(
                    serde_json::from_str::<BatchMessageLimit>(&json).is_ok(),
                    in_range,
                    "JSON decoding of {count}"
                );
                if let Some(nonzero) = NonZeroU32::new(narrow) {
                    let archived =
                        rkyv::to_bytes::<rkyv::rancor::Error>(&nonzero).expect("a count archives");
                    assert_eq!(
                        rkyv::from_bytes::<BatchMessageLimit, rkyv::rancor::Error>(&archived)
                            .is_ok(),
                        in_range,
                        "archive decoding of {count}"
                    );
                }
            }

            let zeros = "0".repeat(usize::try_from(entropy.up_to(2)).expect("2 fits in usize"));
            let digits = entropy.any_u64();
            let spelling = entropy.pick(UNIT_SPELLINGS);
            let text = format!("{zeros}{digits}{spelling}");
            match text.parse::<PayloadSizeLimit>() {
                Ok(size) => {
                    let canonical = size.to_string();
                    assert_eq!(
                        canonical.parse::<PayloadSizeLimit>().ok(),
                        Some(size),
                        "{text}"
                    );
                }
                Err(error) => assert!(matches!(
                    error,
                    EmitterBatchLimitError::MalformedSize { .. }
                        | EmitterBatchLimitError::ZeroSize
                        | EmitterBatchLimitError::SizeOverflow { .. }
                )),
            }

            // A decoder admits exactly the byte counts a whole number of the unit makes.
            let unit = entropy.pick(UNITS);
            let bytes = NonZeroU64::new(entropy.boundary_biased(1..=u64::MAX)).expect("positive");
            let whole = bytes.get().is_multiple_of(unit.bytes());
            let archived =
                rkyv::to_bytes::<rkyv::rancor::Error>(&ArchivedSizeFields { bytes, unit })
                    .expect("the fields archive");
            assert_eq!(
                rkyv::from_bytes::<PayloadSizeLimit, rkyv::rancor::Error>(&archived).is_ok(),
                whole,
                "archive decoding of {bytes} bytes in {unit}"
            );
            let json = serde_json::to_string(&serde_json::json!({ "bytes": bytes, "unit": unit }))
                .expect("the fields have a JSON form");
            assert_eq!(
                serde_json::from_str::<PayloadSizeLimit>(&json).is_ok(),
                whole,
                "JSON decoding of {bytes} bytes in {unit}"
            );
        });
}
