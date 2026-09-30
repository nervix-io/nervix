//! Domain-clock periods, skews and time rates keep their values through every representation, and
//! clock input reads as a value in range or fails with a typed error.
//!
//! A period is any positive number of nanoseconds a `u64` holds and a skew any number at all; both
//! read and write humantime text. A time rate is any positive finite `f64`, and every
//! representation keeps its bit pattern.

use std::time::Duration;

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{DomainClockError, DomainClockPeriod, DomainClockSkew, DomainTimeRate};

/// A positive finite `f64` from arbitrary bits: its magnitude, with non-finite and zero values
/// replaced by values at the ends of the range.
fn positive_finite(bits: u64) -> f64 {
    let magnitude = f64::from_bits(bits).abs();
    if magnitude.is_nan() || magnitude == 0.0 {
        f64::from_bits(1)
    } else if magnitude.is_infinite() {
        f64::MAX
    } else {
        magnitude
    }
}

#[test]
fn bolero_domain_clock_values_round_trip_through_every_representation() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);

            let period = arbitrary.clock_period();
            let nanos = period.as_nanos();
            assert_eq!(
                DomainClockPeriod::try_from(period.as_duration()).ok(),
                Some(period)
            );
            let text = period.to_string();
            assert_eq!(
                text.parse::<DomainClockPeriod>().ok(),
                Some(period),
                "{text}"
            );
            let json = serde_json::to_string(&period).expect("a period has a JSON form");
            assert_eq!(json, nanos.to_string());
            assert_eq!(
                serde_json::from_str::<DomainClockPeriod>(&json).ok(),
                Some(period)
            );
            assert_eq!(
                crate::archive_round_trip!(&period, DomainClockPeriod),
                period
            );

            let skew = arbitrary.clock_skew();
            assert_eq!(
                DomainClockSkew::try_from(skew.as_duration()).ok(),
                Some(skew)
            );
            let text = skew.to_string();
            assert_eq!(text.parse::<DomainClockSkew>().ok(), Some(skew), "{text}");
            let json = serde_json::to_string(&skew).expect("a skew has a JSON form");
            assert_eq!(
                serde_json::from_str::<DomainClockSkew>(&json).ok(),
                Some(skew)
            );
            assert_eq!(crate::archive_round_trip!(&skew, DomainClockSkew), skew);

            let value = positive_finite(arbitrary.entropy().any_u64());
            let rate = DomainTimeRate::try_from(value).expect("the value is positive and finite");
            assert_eq!(rate.get().to_bits(), value.to_bits());
            assert_eq!(f64::from(rate).to_bits(), value.to_bits());
            let text = rate.to_string();
            let reread = text
                .parse::<DomainTimeRate>()
                .expect("a rate's text reads back");
            assert_eq!(reread.get().to_bits(), value.to_bits(), "{text}");
            let restored = crate::archive_round_trip!(&rate, DomainTimeRate);
            assert_eq!(restored.get().to_bits(), value.to_bits());
        });
}

/// Pieces arbitrary duration text is assembled from.
const DURATION_PIECES: [&str; 16] = [
    "0",
    "1",
    "500",
    "18446744073709551615",
    "18446744073709551616",
    "ns",
    "us",
    "ms",
    "s",
    "m",
    "h",
    "d",
    "y",
    " ",
    ".5",
    "x",
];

#[test]
fn bolero_domain_clock_input_is_validated_or_rejected() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let entropy = arbitrary.entropy();

            let pieces = entropy.boundary_biased(0..=8);
            let mut text = String::new();
            for _ in 0..pieces {
                text.push_str(entropy.pick(DURATION_PIECES));
            }
            match text.parse::<DomainClockPeriod>() {
                Ok(period) => {
                    assert!(period.as_nanos() > 0);
                    let canonical = period.to_string();
                    assert_eq!(canonical.parse::<DomainClockPeriod>().ok(), Some(period));
                }
                Err(error) => assert!(
                    matches!(error, DomainClockError::InvalidPeriod { .. }),
                    "{text:?}: {error}"
                ),
            }
            match text.parse::<DomainClockSkew>() {
                Ok(skew) => {
                    let canonical = skew.to_string();
                    assert_eq!(canonical.parse::<DomainClockSkew>().ok(), Some(skew));
                }
                Err(error) => assert!(
                    matches!(error, DomainClockError::InvalidSkew { .. }),
                    "{text:?}: {error}"
                ),
            }

            let nanos = entropy.boundary_biased(0..=u64::MAX);
            assert_eq!(
                DomainClockPeriod::try_from(Duration::from_nanos(nanos)).is_ok(),
                nanos > 0
            );
            assert_eq!(
                serde_json::from_str::<DomainClockPeriod>(&nanos.to_string()).is_ok(),
                nanos > 0
            );
            let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&nanos).expect("a u64 archives");
            assert_eq!(
                rkyv::from_bytes::<DomainClockPeriod, rkyv::rancor::Error>(&archived).is_ok(),
                nanos > 0
            );

            let value = f64::from_bits(entropy.any_u64());
            let valid = value.is_finite() && value > 0.0;
            let rate = DomainTimeRate::try_from(value);
            assert_eq!(rate.is_ok(), valid, "{value:e}");
            if let Err(error) = rate {
                assert!(matches!(error, DomainClockError::InvalidTimeRate { .. }));
            }
            let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&value).expect("an f64 archives");
            assert_eq!(
                rkyv::from_bytes::<DomainTimeRate, rkyv::rancor::Error>(&archived).is_ok(),
                valid,
                "archive decoding of {value:e}"
            );
            if value.is_finite() {
                let json = serde_json::to_string(&value).expect("a finite f64 has a JSON form");
                assert_eq!(
                    serde_json::from_str::<DomainTimeRate>(&json).is_ok(),
                    valid,
                    "JSON decoding of {json}"
                );
            }
        });
}
