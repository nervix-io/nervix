//! The final response that ends one request of the outbound HTTP emitter, read from the head hyper
//! received.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Classifying a complete final status, refusing a status outside the range HTTP
//!   defines, reading the one `Retry-After` value a final head states and the delay it asks for,
//!   and reading the one `Location` a redirect states.
//! - **Depends on.** The `http` types hyper hands back, the HTTP-date grammar, the vocabulary
//!   timestamp and typed connector errors.
//! - **Must not know.** How the response was received, emitter buffers, acknowledgements or retry
//!   cadence.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::time::{Duration, UNIX_EPOCH};

use error_stack::Report;
use http::{
    HeaderMap, Response,
    header::{LOCATION, RETRY_AFTER},
};
use nervix_models::Timestamp;
use thiserror::Error;

#[derive(Debug, Error)]
pub(super) enum ResponseHeadError {
    /// A status HTTP's grammar admits but its semantics leave undefined, so it has no class.
    #[error("HTTP response status {status} is outside the range HTTP defines")]
    StatusOutOfRange { status: u16 },
}

type ResponseHeadResult<T> = Result<T, Report<ResponseHeadError>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FinalStatus(u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Disposition {
    Delivered,
    Rejected,
    AuthenticationFailure,
    RetryableFailure,
}

/// A complete final response head: its status, the one `Retry-After` value it states, and the one
/// `Location` a redirect states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FinalResponse {
    status: FinalStatus,
    retry_after: Option<RetryAfter>,
    location: Option<String>,
}

/// The one `Retry-After` value a final head states, in either form RFC 9110 defines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryAfter {
    /// `delay-seconds`: whole seconds, measured physically from the response's arrival.
    Seconds(u64),
    /// `HTTP-date`: an instant, compared with actual UTC when the response arrives.
    Date(Timestamp),
}

impl FinalResponse {
    /// The final response `response` is, which hyper delivers once its head is complete; its body
    /// is neither awaited nor read.
    pub(super) fn read<B>(response: &Response<B>) -> ResponseHeadResult<Self> {
        let status = response.status().as_u16();
        if !(100..=599).contains(&status) {
            return Err(Report::new(ResponseHeadError::StatusOutOfRange { status }));
        }
        let headers = response.headers();
        Ok(Self {
            status: FinalStatus(status),
            retry_after: RetryAfter::stated(headers),
            location: Self::location_stated(status, headers),
        })
    }

    pub(super) fn status(&self) -> FinalStatus {
        self.status
    }

    /// The `Location` of a `3xx` head that states exactly one, as text. Its value is never
    /// described in a failure: a server can put anything in it.
    pub(super) fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    /// The delay this response asks for before the next attempt, measured from `received_at`, the
    /// actual UTC when it arrived.
    ///
    /// Seconds ask for exactly that many, and a date for the time until it, which is none for a
    /// date already past. A head stating no single valid `Retry-After` asks for no delay, and so
    /// does one whose delay would end outside the instants a timestamp represents.
    pub(super) fn retry_delay(&self, received_at: Timestamp) -> Option<Duration> {
        match self.retry_after? {
            RetryAfter::Seconds(seconds) => {
                let delay = Duration::from_secs(seconds);
                if received_at.checked_add(delay).is_err() {
                    return None;
                }
                Some(delay)
            }
            // A date already past asks for no wait beyond the host's own backoff.
            RetryAfter::Date(at) => Some(at.duration_since(received_at).unwrap_or(Duration::ZERO)),
        }
    }

    /// The one `Location` value `headers` state for a `3xx` `status`. Two such fields state none,
    /// and so does any other status, whose `Location` no client follows.
    fn location_stated(status: u16, headers: &HeaderMap) -> Option<String> {
        if !(300..=399).contains(&status) {
            return None;
        }
        let mut values = headers.get_all(LOCATION).iter();
        let value = values.next()?;
        if values.next().is_some() {
            return None;
        }
        let text = std::str::from_utf8(value.as_bytes()).ok()?;
        Some(text.to_string())
    }
}

impl RetryAfter {
    /// The one `Retry-After` value `headers` state. Two such fields state none, however valid either
    /// one is.
    fn stated(headers: &HeaderMap) -> Option<Self> {
        let mut values = headers.get_all(RETRY_AFTER).iter();
        let value = values.next()?;
        if values.next().is_some() {
            return None;
        }
        Self::parse(value.as_bytes())
    }

    /// Reads the value of one `Retry-After` field: `1*DIGIT` seconds, or an IMF-fixdate, RFC 850
    /// or asctime date. Anything else, and a number or date no timestamp represents, is `None`.
    fn parse(value: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(value).ok()?;
        let text = text.trim_matches([' ', '\t']);
        if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) {
            let seconds = text.parse::<u64>().ok()?;
            return Some(Self::Seconds(seconds));
        }
        let date = httpdate::parse_http_date(text).ok()?;
        // The grammar admits dates from 1970 through 9999; a timestamp ends in 2262.
        let since_epoch = date.duration_since(UNIX_EPOCH).ok()?;
        let unix_nanos = i64::try_from(since_epoch.as_nanos()).ok()?;
        Some(Self::Date(Timestamp::from_unix_nanos(unix_nanos)))
    }
}

impl FinalStatus {
    pub(super) fn code(self) -> u16 {
        self.0
    }

    pub(super) fn disposition(self) -> Disposition {
        match self.0 {
            200..=299 => Disposition::Delivered,
            401 | 403 | 407 => Disposition::AuthenticationFailure,
            408 | 425 | 429 | 500..=599 => Disposition::RetryableFailure,
            _ => Disposition::Rejected,
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use http::{HeaderValue, StatusCode};

    use super::*;

    fn response(status: u16, fields: &[(&'static str, &[u8])]) -> Response<()> {
        let mut response = Response::new(());
        *response.status_mut() = StatusCode::from_u16(status).expect("the test status is valid");
        for (name, value) in fields {
            response.headers_mut().append(
                *name,
                HeaderValue::from_bytes(value).expect("the test field value is valid"),
            );
        }
        response
    }

    fn final_response(status: u16, fields: &[(&'static str, &[u8])]) -> FinalResponse {
        FinalResponse::read(&response(status, fields)).expect("the test status is in range")
    }

    #[test]
    fn valid_final_statuses_have_one_disposition() {
        for code in [200, 202, 204, 299] {
            assert_eq!(FinalStatus(code).disposition(), Disposition::Delivered);
        }
        for code in [101, 300, 304, 400, 404, 409, 413, 499] {
            assert_eq!(FinalStatus(code).disposition(), Disposition::Rejected);
        }
        for code in [401, 403, 407] {
            assert_eq!(
                FinalStatus(code).disposition(),
                Disposition::AuthenticationFailure
            );
        }
        for code in [408, 425, 429, 500, 503, 599] {
            assert_eq!(
                FinalStatus(code).disposition(),
                Disposition::RetryableFailure
            );
        }
    }

    #[test]
    fn a_status_outside_the_range_http_defines_has_no_class() {
        for status in [600, 999] {
            let error = FinalResponse::read(&response(status, &[]))
                .expect_err("a status above 599 has no class");
            assert!(matches!(
                error.current_context(),
                ResponseHeadError::StatusOutOfRange { status: found } if *found == status
            ));
        }
    }

    /// The RFC 9110 example instant, `Sun, 06 Nov 1994 08:49:37 GMT`.
    const EXAMPLE_DATE_SECONDS: i64 = 784_111_777;

    fn unix_seconds(seconds: i64) -> Timestamp {
        Timestamp::from_unix_nanos(seconds * 1_000_000_000)
    }

    fn retry_delay(retry_after: &str, received_at: Timestamp) -> Option<Duration> {
        final_response(503, &[("retry-after", retry_after.as_bytes())]).retry_delay(received_at)
    }

    #[test]
    fn retry_after_states_whole_seconds_or_one_http_date_in_any_of_its_forms() {
        let received_at = unix_seconds(EXAMPLE_DATE_SECONDS - 30);
        assert_eq!(
            retry_delay("120", received_at),
            Some(Duration::from_secs(120))
        );
        assert_eq!(retry_delay("0", received_at), Some(Duration::ZERO));
        assert_eq!(
            retry_delay("007", received_at),
            Some(Duration::from_secs(7))
        );
        for date in [
            "Sun, 06 Nov 1994 08:49:37 GMT",
            "Sunday, 06-Nov-94 08:49:37 GMT",
            "Sun Nov  6 08:49:37 1994",
        ] {
            assert_eq!(
                retry_delay(date, received_at),
                Some(Duration::from_secs(30)),
                "{date}"
            );
        }
        let later = unix_seconds(EXAMPLE_DATE_SECONDS + 5);
        assert_eq!(
            retry_delay("Sun, 06 Nov 1994 08:49:37 GMT", later),
            Some(Duration::ZERO),
            "a date already past asks for no wait"
        );
    }

    #[test]
    fn a_repeated_malformed_or_unrepresentable_retry_after_states_no_delay() {
        let received_at = unix_seconds(EXAMPLE_DATE_SECONDS);
        for malformed in [
            "",
            "3600.5",
            "-1",
            "+5",
            "5 s",
            "1e3",
            "Sun, 06 Nov 1994 08:49:37 UTC",
            "Mon, 06 Nov 1994 08:49:37 GMT",
            "Sun, 06 Nov 1994 08:49:37 GMT, 5",
        ] {
            assert_eq!(retry_delay(malformed, received_at), None, "{malformed:?}");
        }
        for unrepresentable in [
            "99999999999999999999999",
            "18446744073709551615",
            "Fri, 31 Dec 9999 23:59:59 GMT",
        ] {
            assert_eq!(
                retry_delay(unrepresentable, received_at),
                None,
                "{unrepresentable}"
            );
        }
        let repeated = final_response(429, &[("retry-after", b"5"), ("retry-after", b"5")]);
        assert_eq!(repeated.retry_delay(received_at), None);
        let absent = final_response(503, &[]);
        assert_eq!(absent.retry_delay(received_at), None);
    }

    #[test]
    fn a_redirect_states_its_one_location_and_no_other_status_states_any() {
        let moved = final_response(307, &[("location", b"/moved?a=1")]);
        assert_eq!(moved.location(), Some("/moved?a=1"));
        let absolute = final_response(301, &[("location", b"https://api.example.com/v2")]);
        assert_eq!(absolute.location(), Some("https://api.example.com/v2"));
        let repeated = final_response(302, &[("location", b"/first"), ("location", b"/second")]);
        assert_eq!(repeated.location(), None);
        assert_eq!(final_response(308, &[]).location(), None);
        assert_eq!(
            final_response(302, &[("location", b"/caf\xe9")]).location(),
            None
        );
        assert_eq!(
            final_response(201, &[("location", b"/events/1")]).location(),
            None
        );
        assert_eq!(
            final_response(404, &[("location", b"/elsewhere")]).location(),
            None
        );
    }
}
