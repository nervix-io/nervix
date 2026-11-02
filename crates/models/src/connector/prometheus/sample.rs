//! The sample a Prometheus query source delivers.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The JSON object a query source delivers for one sample of an instant query, how
//!   that object writes the sample's value, and what a query can answer that a query source does
//!   not read.
//! - **Depends on.** The vocabulary's timestamps, JSON numbers and declarative string rendering.
//! - **Must not know.** How a query is sent, how an answer is read or decoded, or which codec
//!   judges the object a source delivers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{collections::BTreeMap, str::FromStr};

use error_stack::{Report, ResultExt as _};
use serde::{Serialize, Serializer};
use thiserror::Error;

use crate::Timestamp;

/// The one `resultType` a query source reads: a `vector` of float samples, each delivered as a
/// [`PrometheusSample`]. Everything else a query can answer is a [`PrometheusUnsupportedResult`].
pub const PROMETHEUS_VECTOR_RESULT: &str = "vector";

/// The object a query source delivers for one sample: what the API's `vector` result holds for it,
/// with the sample's instant written out as text.
///
/// This type is the payload contract, and the connector encodes nothing but it. Labels stay under
/// `metric`, so a label named `value` or `timestamp` cannot collide with the sample's own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrometheusSample {
    /// The labels of the sample's series in ascending name order, its `__name__` among them where
    /// the series has one.
    pub metric: BTreeMap<String, String>,
    pub value: PrometheusSampleValue,
    /// The sample's own instant, as [`Timestamp::to_rfc3339`] writes it.
    #[serde(serialize_with = "PrometheusSample::serialize_timestamp")]
    pub timestamp: Timestamp,
}

impl PrometheusSample {
    fn serialize_timestamp<S>(timestamp: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&timestamp.to_rfc3339())
    }
}

/// The value of one sample: what the object writes under `value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum PrometheusSampleValue {
    /// A finite value, written as a JSON number. A [`serde_json::Number`] holds no NaN and no
    /// infinity, and it keeps an integer the API writes as that integer.
    Number(serde_json::Number),
    /// A value JSON has no number for, written as the string the API writes it as, for the codec
    /// to judge per record.
    NonFinite(PrometheusNonFiniteValue),
}

/// Why a sample's value is neither a number nor one of the API's non-finite strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("a Prometheus sample value is a decimal number, NaN, +Inf or -Inf")]
pub struct PrometheusSampleValueError;

/// Reads the text the API holds a sample's value as.
impl FromStr for PrometheusSampleValue {
    type Err = Report<PrometheusSampleValueError>;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if let Ok(non_finite) = text.parse::<PrometheusNonFiniteValue>() {
            return Ok(Self::NonFinite(non_finite));
        }
        let number = serde_json::from_str::<serde_json::Number>(text)
            .change_context(PrometheusSampleValueError)?;
        Ok(Self::Number(number))
    }
}

/// The values JSON has no number for, each spelled as the API spells it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::IntoStaticStr)]
pub enum PrometheusNonFiniteValue {
    #[strum(serialize = "NaN")]
    NotANumber,
    #[strum(serialize = "+Inf")]
    PositiveInfinity,
    #[strum(serialize = "-Inf")]
    NegativeInfinity,
}

impl Serialize for PrometheusNonFiniteValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.into())
    }
}

/// What an instant query can answer that a query source does not read. Each fails its poll with a
/// failure that names it, the next poll queries again, and no sample of that answer is delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::EnumIter, strum::EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum PrometheusUnsupportedResult {
    Matrix,
    Scalar,
    String,
    /// A native histogram among the samples of a `vector`.
    #[strum(serialize = "native histogram")]
    NativeHistogram,
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    fn sample(value: PrometheusSampleValue) -> PrometheusSample {
        PrometheusSample {
            metric: BTreeMap::from([
                ("__name__".to_string(), "up".to_string()),
                ("timestamp".to_string(), "a label".to_string()),
                ("value".to_string(), "another label".to_string()),
            ]),
            value,
            timestamp: Timestamp::from_unix_nanos(1_775_520_000_123_000_000),
        }
    }

    fn value(text: &str) -> PrometheusSampleValue {
        text.parse()
            .assured("every table entry is a value the API writes")
    }

    #[test]
    fn the_object_keeps_labels_under_metric_beside_the_samples_own_value_and_instant() {
        let encoded = serde_json::to_string(&sample(value("42.5")))
            .assured("a sample holds only strings and a number");
        assert_eq!(
            encoded,
            r#"{"metric":{"__name__":"up","timestamp":"a label","value":"another label"},"value":42.5,"timestamp":"2026-04-07T00:00:00.123Z"}"#
        );
    }

    #[test]
    fn a_value_is_a_json_number_or_the_string_the_api_writes() {
        for (text, written) in [
            ("3", "3"),
            ("-3", "-3"),
            ("42.5", "42.5"),
            ("0.1", "0.1"),
            ("1e+21", "1e+21"),
            ("1.5e-07", "1.5e-7"),
            ("-0", "-0.0"),
            ("18446744073709551615", "18446744073709551615"),
            ("NaN", r#""NaN""#),
            ("+Inf", r#""+Inf""#),
            ("-Inf", r#""-Inf""#),
        ] {
            let encoded = serde_json::to_string(&value(text))
                .assured("a value is a string or a finite number");
            assert_eq!(encoded, written, "{text}");
        }
    }

    #[test]
    fn a_value_the_api_cannot_write_is_refused() {
        for text in [
            "", "nan", "inf", "+inf", "Infinity", "+1", ".5", "1e999", "up",
        ] {
            assert!(text.parse::<PrometheusSampleValue>().is_err(), "'{text}'");
        }
    }

    #[test]
    fn an_unsupported_result_is_named_as_the_api_or_the_manual_names_it() {
        for (result, name) in [
            (PrometheusUnsupportedResult::Matrix, "matrix"),
            (PrometheusUnsupportedResult::Scalar, "scalar"),
            (PrometheusUnsupportedResult::String, "string"),
            (
                PrometheusUnsupportedResult::NativeHistogram,
                "native histogram",
            ),
        ] {
            assert_eq!(result.to_string(), name);
            assert_eq!(name.parse(), Ok(result));
        }
        assert!(
            PROMETHEUS_VECTOR_RESULT
                .parse::<PrometheusUnsupportedResult>()
                .is_err()
        );
    }
}
