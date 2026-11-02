//! What the Prometheus query API answers, read into samples.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading one answer: whether it holds samples at all, what the API refused or what it
//!   held instead of a vector, and each float sample as the object the vocabulary declares,
//!   including the exact instant the API's decimal seconds name.
//! - **Depends on.** The vocabulary's sample contract and the API's JSON.
//! - **Must not know.** How a query is sent or a response is read, the domain cadence, or the codec
//!   that judges a delivered sample.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::collections::BTreeMap;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_models::{
    PROMETHEUS_VECTOR_RESULT, PrometheusSample, PrometheusSampleValue, PrometheusUnsupportedResult,
    Timestamp,
};
use reqwest::StatusCode;
use serde::{Deserialize, de::IgnoredAny};
use serde_json::value::RawValue;

use super::PrometheusQueryIngestorError;

#[cfg_attr(
    nervix_lint,
    nervix::constant(fixed, reason = "a nanosecond is the ninth decimal place of a second")
)]
const NANOSECOND_DIGITS: i64 = 9;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PrometheusAnswerStatus {
    Success,
    Error,
}

/// The envelope of an answer. Only a successful one holds `data`, and only a refused one
/// `errorType` and `error`.
#[derive(Debug, Deserialize)]
struct PrometheusQueryAnswer<'a> {
    status: PrometheusAnswerStatus,
    #[serde(borrow)]
    data: Option<PrometheusQueryData<'a>>,
    #[serde(rename = "errorType")]
    error_type: Option<String>,
    error: Option<String>,
}

/// What a successful answer holds. `result` stays unread until `resultType` says what it is, in
/// whichever order the answer writes the two: a `scalar` or a `string` is not a list of samples.
#[derive(Debug, Deserialize)]
struct PrometheusQueryData<'a> {
    #[serde(rename = "resultType")]
    result_type: String,
    #[serde(borrow)]
    result: &'a RawValue,
}

/// One element of a `vector`: a float sample holds `value`, a native histogram `histogram`.
#[derive(Debug, Deserialize)]
struct PrometheusVectorEntry<'a> {
    metric: BTreeMap<String, String>,
    #[serde(borrow)]
    value: Option<(&'a RawValue, String)>,
    histogram: Option<IgnoredAny>,
}

/// One float sample of a `vector`, as the API wrote it.
#[derive(Debug)]
pub(super) struct PrometheusFloatSample<'a> {
    metric: BTreeMap<String, String>,
    instant: &'a RawValue,
    value: String,
}

/// A response to one query: its status and the bounded body read from it.
#[derive(Debug)]
pub(super) struct PrometheusQueryResponse {
    pub(super) status: StatusCode,
    pub(super) body: Vec<u8>,
}

impl PrometheusQueryResponse {
    /// The float samples this response holds, or why it holds none.
    pub(super) fn samples(
        &self,
    ) -> Result<Vec<PrometheusFloatSample<'_>>, Report<PrometheusQueryIngestorError>> {
        let decoded = serde_json::from_slice::<PrometheusQueryAnswer<'_>>(&self.body);
        if !self.status.is_success() {
            return Err(self.refusal(decoded.ok()));
        }
        // The decoder's own message can quote the payload, so it stays an attachment and out of
        // the transient error the ingestor reports.
        let answer = decoded.map_err(|source| {
            Report::new(PrometheusQueryIngestorError::DecodeResponse)
                .attach_printable(source.to_string())
        })?;
        if let PrometheusAnswerStatus::Error = answer.status {
            return Err(self.refusal(Some(answer)));
        }
        let Some(data) = answer.data else {
            return Err(Report::new(PrometheusQueryIngestorError::DecodeResponse));
        };
        if data.result_type != PROMETHEUS_VECTOR_RESULT {
            return Err(Self::not_a_vector(&data.result_type));
        }
        PrometheusFloatSample::of_vector(data.result)
    }

    /// What this response says instead of samples: the API's own `errorType` and `error` where it
    /// holds them. A proxy's page in front of the API holds neither, and has only its status.
    fn refusal(
        &self,
        answer: Option<PrometheusQueryAnswer<'_>>,
    ) -> Report<PrometheusQueryIngestorError> {
        if let Some(PrometheusQueryAnswer {
            status: PrometheusAnswerStatus::Error,
            error_type: Some(error_type),
            error: Some(error),
            ..
        }) = answer
        {
            return Report::new(PrometheusQueryIngestorError::QueryRejected {
                status: self.status,
                error_type,
                error,
            });
        }
        if self.status.is_success() {
            return Report::new(PrometheusQueryIngestorError::DecodeResponse);
        }
        Report::new(PrometheusQueryIngestorError::QueryStatus {
            status: self.status,
        })
    }

    /// A result of `result_type` is either one the vocabulary declares unsupported or none the API
    /// answers.
    fn not_a_vector(result_type: &str) -> Report<PrometheusQueryIngestorError> {
        match result_type.parse::<PrometheusUnsupportedResult>() {
            Ok(result_type) => {
                Report::new(PrometheusQueryIngestorError::UnsupportedResultType { result_type })
            }
            Err(source) => {
                Report::new(source).change_context(PrometheusQueryIngestorError::DecodeResponse)
            }
        }
    }
}

impl<'a> PrometheusFloatSample<'a> {
    /// The samples of a `vector`. One native histogram among them fails the whole poll.
    fn of_vector(result: &'a RawValue) -> Result<Vec<Self>, Report<PrometheusQueryIngestorError>> {
        let entries = serde_json::from_str::<Vec<PrometheusVectorEntry<'a>>>(result.get())
            .map_err(|source| {
                Report::new(PrometheusQueryIngestorError::DecodeResponse)
                    .attach_printable(source.to_string())
            })?;
        let mut samples = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry.histogram.is_some() {
                return Err(Report::new(
                    PrometheusQueryIngestorError::UnsupportedResultType {
                        result_type: PrometheusUnsupportedResult::NativeHistogram,
                    },
                ));
            }
            let Some((instant, value)) = entry.value else {
                return Err(Report::new(PrometheusQueryIngestorError::DecodeResponse));
            };
            samples.push(Self {
                metric: entry.metric,
                instant,
                value,
            });
        }
        Ok(samples)
    }

    /// The payload of this sample: exactly the object the vocabulary declares.
    pub(super) fn payload(self) -> Result<Vec<u8>, Report<PrometheusQueryIngestorError>> {
        let value = self
            .value
            .parse::<PrometheusSampleValue>()
            .change_context(PrometheusQueryIngestorError::InvalidSampleValue)?;
        let Some(unix_nanos) = self.unix_nanos() else {
            return Err(Report::new(PrometheusQueryIngestorError::InvalidTimestamp));
        };
        let sample = PrometheusSample {
            metric: self.metric,
            value,
            timestamp: Timestamp::from_unix_nanos(unix_nanos),
        };
        serde_json::to_vec(&sample).change_context(PrometheusQueryIngestorError::EncodeSample)
    }

    /// The whole nanoseconds of this sample's instant, read from the digits of the JSON number the API
    /// wrote it as, so that no float rounds them, and `None` where they do not fit a timestamp. Digits
    /// past the ninth fractional place are dropped.
    ///
    /// The API writes plain decimals of at most three fractional digits. Other servers that answer
    /// the same API can write any JSON number, so an exponent shifts the decimal point the same way.
    fn unix_nanos(&self) -> Option<i64> {
        let seconds = self.instant.get();
        let (negative, unsigned) = match seconds.strip_prefix('-') {
            Some(unsigned) => (true, unsigned),
            None => (false, seconds),
        };
        let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
            Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>().ok()?),
            None => (unsigned, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));

        let digits = whole.bytes().chain(fraction.bytes());
        let leading_zeros = digits.clone().take_while(|digit| *digit == b'0').count();
        let mut significant = digits.skip(leading_zeros).peekable();
        if significant.peek().is_none() {
            return Some(0);
        }

        // How many digits, counted from the first significant one, lie above the nanosecond. Past the
        // digits the mantissa holds they are zeros; the first significant digit is not, so an
        // exponent of any size overflows within the width of an i64.
        let whole_digits = i64::try_from(whole.len())
            .assured("a length held in memory fits i64 on every supported target");
        let zeros = i64::try_from(leading_zeros)
            .assured("a length held in memory fits i64 on every supported target");
        let kept = whole_digits
            .checked_add(exponent)?
            .checked_add(NANOSECOND_DIGITS)?
            .checked_sub(zeros)?;

        let mut nanoseconds: i64 = 0;
        for _ in 0..kept {
            let digit = significant.next().unwrap_or(b'0');
            let digit = char::from(digit).to_digit(10)?;
            nanoseconds = nanoseconds.checked_mul(10)?.checked_add(i64::from(digit))?;
        }
        if negative {
            return nanoseconds.checked_neg();
        }
        Some(nanoseconds)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::OptionExt as _;
    use nervix_connector::{RetainedIngestMetadata, RetainedIngestMetadataValue};
    use nervix_models::{IngestorEntityKind, PrometheusIngestorEntity};

    use super::*;
    use crate::query::PrometheusQueryIngestor;

    fn answered(body: &str) -> PrometheusQueryResponse {
        PrometheusQueryResponse {
            status: StatusCode::OK,
            body: body.as_bytes().to_vec(),
        }
    }

    fn vector(elements: &str) -> String {
        format!(r#"{{"status":"success","data":{{"resultType":"vector","result":[{elements}]}}}}"#)
    }

    fn payload(element: &str) -> String {
        let response = answered(&vector(element));
        let mut samples = response
            .samples()
            .assured("the test element is a float sample");
        let sample = samples
            .pop()
            .assured("the test vector holds exactly one sample");
        let payload = sample
            .payload()
            .assured("the test element's value and instant are valid");
        String::from_utf8(payload).assured("a JSON payload is UTF-8")
    }

    fn failure_of(response: &PrometheusQueryResponse) -> Report<PrometheusQueryIngestorError> {
        match response.samples() {
            Ok(samples) => panic!("the response must fail, but it read {samples:?}"),
            Err(failure) => failure,
        }
    }

    fn nanoseconds(seconds: &str) -> Option<i64> {
        let instant =
            serde_json::from_str::<&RawValue>(seconds).assured("the test instant is JSON");
        PrometheusFloatSample {
            metric: BTreeMap::new(),
            instant,
            value: "1".to_string(),
        }
        .unix_nanos()
    }

    /// A label named after the sample's own members stays under `metric`, and every member is
    /// what the vocabulary declares, in its order.
    #[test]
    fn a_sample_is_delivered_as_the_declared_object() {
        assert_eq!(
            payload(
                r#"{"metric":{"timestamp":"lt","__name__":"up","value":"lv"},"value":[1775520000.123,"42.5"]}"#
            ),
            r#"{"metric":{"__name__":"up","timestamp":"lt","value":"lv"},"value":42.5,"timestamp":"2026-04-07T00:00:00.123Z"}"#
        );
        assert_eq!(
            payload(r#"{"metric":{},"value":[1775520000,"3"]}"#),
            r#"{"metric":{},"value":3,"timestamp":"2026-04-07T00:00:00Z"}"#
        );
    }

    #[test]
    fn a_non_finite_sample_is_delivered_as_the_string_the_api_wrote() {
        for text in ["NaN", "+Inf", "-Inf"] {
            assert_eq!(
                payload(&format!(
                    r#"{{"metric":{{}},"value":[1775520000,"{text}"]}}"#
                )),
                format!(r#"{{"metric":{{}},"value":"{text}","timestamp":"2026-04-07T00:00:00Z"}}"#)
            );
        }
    }

    /// JSON objects have no order, so `resultType` decides however an answer lays its members out.
    #[test]
    fn an_answer_reads_the_same_in_any_member_order() {
        let body = r#"{"data":{"result":[{"value":[1775520000,"1"],"metric":{"a":"b"}}],"resultType":"vector"},"status":"success"}"#;
        let response = answered(body);
        let samples = response.samples().assured("the test answer is a vector");
        assert_eq!(samples.len(), 1);
    }

    /// Each answer is what the pinned Prometheus image answers for that kind of query.
    #[test]
    fn every_unsupported_result_fails_the_poll_and_names_itself() {
        for (body, expected) in [
            (
                r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1775519955,"1"],[1775519970,"1"]]}]}}"#,
                PrometheusUnsupportedResult::Matrix,
            ),
            (
                r#"{"status":"success","data":{"resultType":"scalar","result":[1775520000.123,"2"]}}"#,
                PrometheusUnsupportedResult::Scalar,
            ),
            (
                r#"{"status":"success","data":{"resultType":"string","result":[1775520000.123,"text"]}}"#,
                PrometheusUnsupportedResult::String,
            ),
            (
                &vector(
                    r#"{"metric":{"source":"float"},"value":[1775520000,"1"]},{"metric":{"source":"histogram"},"histogram":[1775520000,{"count":"1","sum":"0.5","buckets":[[0,"0.25","0.5","1"]]}]}"#,
                ),
                PrometheusUnsupportedResult::NativeHistogram,
            ),
        ] {
            let failure = failure_of(&answered(body));
            assert!(
                matches!(
                    failure.current_context(),
                    PrometheusQueryIngestorError::UnsupportedResultType { result_type }
                        if *result_type == expected
                ),
                "{failure:?}"
            );
            assert_eq!(
                format!("{failure:#}"),
                format!("Prometheus query returned unsupported result type '{expected}'")
            );
        }
    }

    #[test]
    fn an_answer_that_is_not_the_apis_fails_its_decoding() {
        for body in [
            "not-json",
            "{}",
            r#"{"status":"partial"}"#,
            r#"{"status":"success"}"#,
            r#"{"status":"error"}"#,
            r#"{"status":"success","data":{"resultType":"histogram","result":[]}}"#,
            r#"{"status":"success","data":{"resultType":"vector","result":{}}}"#,
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{}}]}}"#,
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"a":1},"value":[1,"1"]}]}}"#,
        ] {
            let failure = failure_of(&answered(body));
            assert!(
                matches!(
                    failure.current_context(),
                    PrometheusQueryIngestorError::DecodeResponse
                ),
                "{body}: {failure:?}"
            );
        }
    }

    /// The answer is what the pinned Prometheus image answers `sum(` with.
    #[test]
    fn a_refused_query_reports_the_apis_own_words() {
        let refused = PrometheusQueryResponse {
            status: StatusCode::BAD_REQUEST,
            body: br#"{"status":"error","errorType":"bad_data","error":"invalid parameter \"query\": 1:5: parse error: unclosed left parenthesis"}"#.to_vec(),
        };
        let failure = failure_of(&refused);
        assert!(
            matches!(
                failure.current_context(),
                PrometheusQueryIngestorError::QueryRejected { status, error_type, error }
                    if *status == StatusCode::BAD_REQUEST
                        && error_type == "bad_data"
                        && error.ends_with("unclosed left parenthesis")
            ),
            "{failure:?}"
        );
        assert_eq!(
            format!("{failure:#}"),
            "Prometheus rejected the query with HTTP status 400 Bad Request: bad_data: invalid \
             parameter \"query\": 1:5: parse error: unclosed left parenthesis"
        );

        let in_a_success = failure_of(&answered(
            r#"{"status":"error","errorType":"timeout","error":"too slow"}"#,
        ));
        assert!(
            matches!(
                in_a_success.current_context(),
                PrometheusQueryIngestorError::QueryRejected { error_type, .. }
                    if error_type == "timeout"
            ),
            "{in_a_success:?}"
        );
    }

    /// A status with nothing of the API's in its body, such as a proxy's page, reports that status.
    #[test]
    fn a_status_without_the_apis_error_reports_the_status() {
        for body in [
            "<html><body>upstream unavailable</body></html>",
            "",
            "{}",
            r#"{"status":"error"}"#,
            r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#,
        ] {
            let page = PrometheusQueryResponse {
                status: StatusCode::SERVICE_UNAVAILABLE,
                body: body.as_bytes().to_vec(),
            };
            let failure = failure_of(&page);
            assert!(
                matches!(
                    failure.current_context(),
                    PrometheusQueryIngestorError::QueryStatus { status }
                        if *status == StatusCode::SERVICE_UNAVAILABLE
                ),
                "{body}: {failure:?}"
            );
        }
    }

    /// Nanosecond Unix time passed the `f64` mantissa in 1970, so these rows hold only if the
    /// digits are read as digits.
    #[test]
    fn an_instant_is_read_exactly_from_its_digits() {
        for (seconds, expected) in [
            ("0", Some(0)),
            ("0.0", Some(0)),
            ("-0", Some(0)),
            ("0e999999999", Some(0)),
            ("1775520000", Some(1_775_520_000_000_000_000)),
            ("1775520000.123", Some(1_775_520_000_123_000_000)),
            ("1775520000.123456789", Some(1_775_520_000_123_456_789)),
            ("1775520000.123456790", Some(1_775_520_000_123_456_790)),
            ("1.775520000123e9", Some(1_775_520_000_123_000_000)),
            ("1.775520000123E+9", Some(1_775_520_000_123_000_000)),
            ("1775520000123456789e-9", Some(1_775_520_000_123_456_789)),
            ("0.000000001", Some(1)),
            ("-0.5", Some(-500_000_000)),
            ("-1.5", Some(-1_500_000_000)),
            ("9223372036.854775807", Some(i64::MAX)),
            ("-9223372036.854775807", Some(-i64::MAX)),
            // Past the ninth fractional digit there is nothing to keep.
            ("1775520000.1234567899", Some(1_775_520_000_123_456_789)),
            ("0.0000000009", Some(0)),
            ("1e-10", Some(0)),
            ("9223372036.854775808", None),
            ("1e10", None),
            ("1e999999999", None),
            ("1e99999999999999999999", None),
        ] {
            assert_eq!(nanoseconds(seconds), expected, "{seconds}");
        }
    }

    #[test]
    fn a_sample_that_cannot_be_read_names_what_is_wrong_with_it() {
        let response = answered(&vector(r#"{"metric":{},"value":[1,"not-a-number"]}"#));
        let mut samples = response
            .samples()
            .assured("the test element is a float sample");
        let sample = samples.pop().assured("the test vector holds one sample");
        let failure = sample.payload().expect_err("the value is not a number");
        assert!(matches!(
            failure.current_context(),
            PrometheusQueryIngestorError::InvalidSampleValue
        ));

        let response = answered(&vector(r#"{"metric":{},"value":[1e30,"1"]}"#));
        let mut samples = response
            .samples()
            .assured("the test element is a float sample");
        let sample = samples.pop().assured("the test vector holds one sample");
        let failure = sample
            .payload()
            .expect_err("the instant does not fit a timestamp");
        assert!(matches!(
            failure.current_context(),
            PrometheusQueryIngestorError::InvalidTimestamp
        ));
    }

    /// A value as the API can hold it.
    #[derive(Debug, bolero::TypeGenerator)]
    enum GeneratedValue {
        Float(f64),
        Integer(i64),
        NotANumber,
        PositiveInfinity,
        NegativeInfinity,
    }

    impl GeneratedValue {
        /// The text the API holds this value as, and the JSON the object writes for it, each
        /// worked out without the code under test. A non-finite float stands for its own string.
        fn texts(&self) -> (String, String) {
            match self {
                Self::Float(value) if value.is_nan() => Self::NotANumber.texts(),
                Self::Float(value) if value.is_infinite() && value.is_sign_positive() => {
                    Self::PositiveInfinity.texts()
                }
                Self::Float(value) if value.is_infinite() => Self::NegativeInfinity.texts(),
                // Exponent notation keeps every float a float, whatever its value: only text
                // without one stays an integer.
                Self::Float(value) => (
                    format!("{value:e}"),
                    serde_json::to_string(value).assured("a finite float has JSON text"),
                ),
                Self::Integer(value) => (value.to_string(), value.to_string()),
                Self::NotANumber => ("NaN".to_string(), r#""NaN""#.to_string()),
                Self::PositiveInfinity => ("+Inf".to_string(), r#""+Inf""#.to_string()),
                Self::NegativeInfinity => ("-Inf".to_string(), r#""-Inf""#.to_string()),
            }
        }
    }

    #[derive(Debug, bolero::TypeGenerator)]
    struct GeneratedSample {
        labels: BTreeMap<String, String>,
        value: GeneratedValue,
        unix_nanos: i64,
        as_exponent: bool,
    }

    impl GeneratedSample {
        /// The instant as a decimal, or as the same digits behind an exponent.
        fn seconds(&self) -> String {
            let sign = if self.unix_nanos < 0 { "-" } else { "" };
            let nanos = self.unix_nanos.unsigned_abs();
            if self.as_exponent {
                return format!("{sign}{nanos}e-9");
            }
            let whole = nanos / 1_000_000_000;
            let fraction = nanos % 1_000_000_000;
            format!("{sign}{whole}.{fraction:09}")
        }

        fn element(&self) -> String {
            let (text, _) = self.value.texts();
            format!(
                r#"{{"metric":{},"value":[{},{}]}}"#,
                serde_json::to_string(&self.labels).assured("labels are strings"),
                self.seconds(),
                serde_json::to_string(&text).assured("a value's text is a string"),
            )
        }

        fn expected_payload(&self) -> String {
            let (_, written) = self.value.texts();
            format!(
                r#"{{"metric":{},"value":{written},"timestamp":"{}"}}"#,
                serde_json::to_string(&self.labels).assured("labels are strings"),
                Timestamp::from_unix_nanos(self.unix_nanos).to_rfc3339(),
            )
        }
    }

    /// One poll: the samples of its answer, the query that asked for them and its instant.
    #[derive(Debug, bolero::TypeGenerator)]
    struct GeneratedPoll {
        samples: Vec<GeneratedSample>,
        query: String,
        evaluated_at: i64,
    }

    /// Every sample of a generated vector is exactly the declared object: its labels, its value and
    /// its instant survive to the byte, and a poll's metadata is the entity's declaration.
    #[test]
    fn bolero_every_sample_is_exactly_the_declared_object() {
        bolero::check!()
            .with_type::<GeneratedPoll>()
            .for_each(|poll| {
                let samples: Vec<&GeneratedSample> = poll
                    .samples
                    .iter()
                    .filter(|sample| sample.unix_nanos != i64::MIN)
                    .collect();
                let elements: Vec<String> = samples.iter().map(|sample| sample.element()).collect();
                let response = answered(&vector(&elements.join(",")));
                let decoded = response
                    .samples()
                    .assured("every generated element is a sample");
                assert_eq!(decoded.len(), samples.len());
                for (read, sample) in decoded.into_iter().zip(&samples) {
                    let payload = read.payload().assured("every generated sample is valid");
                    let payload = String::from_utf8(payload).assured("a JSON payload is UTF-8");
                    assert_eq!(payload, sample.expected_payload());

                    let object: serde_json::Value =
                        serde_json::from_str(&payload).assured("a payload is a JSON object");
                    let labels: BTreeMap<String, String> =
                        serde_json::from_value(object["metric"].clone())
                            .assured("metric holds strings");
                    assert_eq!(labels, sample.labels);
                    if let GeneratedValue::Float(value) = sample.value
                        && value.is_finite()
                    {
                        let written = object["value"]
                            .as_f64()
                            .assured("a finite value is a number");
                        assert_eq!(written.to_bits(), value.to_bits());
                    }
                }

                let evaluated_at = Timestamp::from_unix_nanos(poll.evaluated_at);
                let metadata = PrometheusQueryIngestor::poll_metadata(&poll.query, evaluated_at);
                assert_eq!(
                    metadata,
                    RetainedIngestMetadata::from_values(vec![
                        RetainedIngestMetadataValue::String(poll.query.clone()),
                        RetainedIngestMetadataValue::Datetime(evaluated_at),
                    ])
                );
                let declared = IngestorEntityKind::Prometheus(PrometheusIngestorEntity::Query)
                    .capabilities()
                    .metadata;
                let types: Vec<_> = metadata
                    .values()
                    .iter()
                    .map(RetainedIngestMetadataValue::ty)
                    .collect();
                let declared_types: Vec<_> = declared
                    .fields()
                    .iter()
                    .map(|field| Some(field.ty()))
                    .collect();
                assert_eq!(types, declared_types);
            });
    }
}
