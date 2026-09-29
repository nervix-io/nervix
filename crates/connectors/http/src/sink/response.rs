//! Bounded HTTP/1.1 response heads for the outbound HTTP sink.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Reading and validating each interim and final header block, classifying a
//!   complete final status, and reading the one `Retry-After` value a final head states.
//! - **Depends on.** Tokio streams, the HTTP/1.1 parser, the HTTP-date grammar, the vocabulary
//!   timestamp and typed connector errors.
//! - **Must not know.** Emitter buffers, acknowledgements or retry cadence.

use std::time::{Duration, UNIX_EPOCH};

use error_stack::Report;
use nervix_models::Timestamp;
use thiserror::Error;
use tokio::io::AsyncRead;

const MAX_HEADER_FIELDS: usize = 128;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_HEAD_BUFFER_BYTES: usize = MAX_HEADER_BYTES + MAX_HEADER_FIELDS * 4 + 8 * 1024;
const READ_BYTES: usize = 4096;

#[derive(Debug, Error)]
pub(super) enum ResponseHeadError {
    #[error("HTTP response headers are malformed")]
    Malformed,
    #[error("HTTP response headers exceed 128 fields or 64 KiB")]
    Excessive,
    #[error("HTTP connection closed before complete final response headers")]
    Lost,
    #[error("HTTP response header read failed")]
    Read,
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

/// A complete, valid final response head: its status and the one `Retry-After` value it states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FinalResponse {
    status: FinalStatus,
    retry_after: Option<RetryAfter>,
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
    pub(super) fn status(self) -> FinalStatus {
        self.status
    }

    /// The delay this response asks for before the next attempt, measured from `received_at`, the
    /// actual UTC when it arrived.
    ///
    /// Seconds ask for exactly that many, and a date for the time until it, which is none for a
    /// date already past. A head stating no single valid `Retry-After` asks for no delay, and so
    /// does one whose delay would end outside the instants a timestamp represents.
    pub(super) fn retry_delay(self, received_at: Timestamp) -> Option<Duration> {
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
}

impl RetryAfter {
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

struct ParsedBlock {
    length: usize,
    status: u16,
    retry_after: Option<RetryAfter>,
}

/// Reads through complete final headers. Any body bytes received with their terminating CRLF are
/// discarded with the stream; the body is neither parsed nor awaited. Only the final head's
/// `Retry-After` counts; an interim head's is read with its block and dropped with it.
pub(super) async fn read_final_headers<S>(stream: &mut S) -> ResponseHeadResult<FinalResponse>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut pending = Vec::with_capacity(READ_BYTES);
    loop {
        tokio::task::consume_budget().await;
        if let Some(block) = ParsedBlock::parse(&pending)? {
            pending.drain(..block.length);
            if block.status == 101 || block.status >= 200 {
                return Ok(FinalResponse {
                    status: FinalStatus(block.status),
                    retry_after: block.retry_after,
                });
            }
            continue;
        }
        if pending.len() >= MAX_HEAD_BUFFER_BYTES {
            return Err(Report::new(ResponseHeadError::Excessive));
        }
        let mut chunk = [0_u8; READ_BYTES];
        let read = tokio::io::AsyncReadExt::read(stream, &mut chunk)
            .await
            .map_err(|_| Report::new(ResponseHeadError::Read))?;
        if read == 0 {
            return Err(Report::new(ResponseHeadError::Lost));
        }
        pending.extend_from_slice(&chunk[..read]);
    }
}

impl ParsedBlock {
    fn parse(bytes: &[u8]) -> ResponseHeadResult<Option<Self>> {
        let mut fields = [httparse::EMPTY_HEADER; MAX_HEADER_FIELDS];
        let mut response = httparse::Response::new(&mut fields);
        let parsed = response.parse(bytes).map_err(|error| match error {
            httparse::Error::TooManyHeaders => Report::new(ResponseHeadError::Excessive),
            _ => Report::new(ResponseHeadError::Malformed),
        })?;
        let httparse::Status::Complete(length) = parsed else {
            return Ok(None);
        };
        let Some(status) = response.code else {
            return Err(Report::new(ResponseHeadError::Malformed));
        };
        if !(100..=599).contains(&status) {
            return Err(Report::new(ResponseHeadError::Malformed));
        }
        Self::validate_line_endings(&bytes[..length])?;
        Self::validate_fields(status, response.headers)?;
        let retry_after = Self::retry_after(response.headers);
        Ok(Some(Self {
            length,
            status,
            retry_after,
        }))
    }

    /// The one `Retry-After` value `fields` state. Two such fields state none, however valid
    /// either one is. The scan is bounded by the 128 fields one parsed block can hold.
    fn retry_after(fields: &[httparse::Header<'_>]) -> Option<RetryAfter> {
        let mut values = fields
            .iter()
            .filter(|field| field.name.eq_ignore_ascii_case("retry-after"));
        let value = values.next()?;
        if values.next().is_some() {
            return None;
        }
        RetryAfter::parse(value.value)
    }

    fn validate_line_endings(bytes: &[u8]) -> ResponseHeadResult<()> {
        for (index, byte) in bytes.iter().enumerate() {
            if *byte == b'\n' && (index == 0 || bytes[index - 1] != b'\r') {
                return Err(Report::new(ResponseHeadError::Malformed));
            }
            if *byte == b'\r' && bytes.get(index + 1) != Some(&b'\n') {
                return Err(Report::new(ResponseHeadError::Malformed));
            }
        }
        Ok(())
    }

    fn validate_fields(status: u16, fields: &[httparse::Header<'_>]) -> ResponseHeadResult<()> {
        let mut bytes = 0_usize;
        let mut content_length = None;
        let mut transfer_encoding = None;
        for field in fields {
            let Some(next) = bytes
                .checked_add(field.name.len())
                .and_then(|size| size.checked_add(field.value.len()))
            else {
                return Err(Report::new(ResponseHeadError::Excessive));
            };
            bytes = next;
            if bytes > MAX_HEADER_BYTES {
                return Err(Report::new(ResponseHeadError::Excessive));
            }
            if field.name.eq_ignore_ascii_case("content-length") {
                Self::read_content_length(field.value, &mut content_length)?;
            }
            if field.name.eq_ignore_ascii_case("transfer-encoding") {
                Self::read_transfer_encoding(field.value, &mut transfer_encoding)?;
            }
        }
        if content_length.is_some() && transfer_encoding.is_some() {
            return Err(Report::new(ResponseHeadError::Malformed));
        }
        if let Some(last_encoding) = transfer_encoding
            && !last_encoding.eq_ignore_ascii_case("chunked")
        {
            return Err(Report::new(ResponseHeadError::Malformed));
        }
        if (status < 200 || status == 204)
            && (content_length.is_some() || transfer_encoding.is_some())
        {
            return Err(Report::new(ResponseHeadError::Malformed));
        }
        Ok(())
    }

    fn read_content_length(value: &[u8], length: &mut Option<u64>) -> ResponseHeadResult<()> {
        let text =
            std::str::from_utf8(value).map_err(|_| Report::new(ResponseHeadError::Malformed))?;
        for item in text.split(',') {
            let item = item.trim_matches([' ', '\t']);
            if item.is_empty() || !item.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(Report::new(ResponseHeadError::Malformed));
            }
            let parsed = item
                .parse::<u64>()
                .map_err(|_| Report::new(ResponseHeadError::Malformed))?;
            if let Some(existing) = length
                && *existing != parsed
            {
                return Err(Report::new(ResponseHeadError::Malformed));
            }
            *length = Some(parsed);
        }
        Ok(())
    }

    fn read_transfer_encoding<'a>(
        value: &'a [u8],
        last: &mut Option<&'a str>,
    ) -> ResponseHeadResult<()> {
        let text =
            std::str::from_utf8(value).map_err(|_| Report::new(ResponseHeadError::Malformed))?;
        for coding in text.split(',') {
            let coding = coding.trim_matches([' ', '\t']);
            if coding.is_empty()
                || !coding
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
            {
                return Err(Report::new(ResponseHeadError::Malformed));
            }
            *last = Some(coding);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(bytes: &[u8]) -> ParsedBlock {
        match ParsedBlock::parse(bytes) {
            Ok(Some(block)) => block,
            Ok(None) => panic!("the fixture contains a complete response head"),
            Err(error) => panic!("the fixture should have valid response headers: {error:#}"),
        }
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

    /// The RFC 9110 example instant, `Sun, 06 Nov 1994 08:49:37 GMT`.
    const EXAMPLE_DATE_SECONDS: i64 = 784_111_777;

    fn unix_seconds(seconds: i64) -> Timestamp {
        Timestamp::from_unix_nanos(seconds * 1_000_000_000)
    }

    fn final_response(head: &[u8]) -> FinalResponse {
        let block = parsed(head);
        FinalResponse {
            status: FinalStatus(block.status),
            retry_after: block.retry_after,
        }
    }

    fn retry_delay(retry_after: &str, received_at: Timestamp) -> Option<Duration> {
        let head =
            format!("HTTP/1.1 503 Service Unavailable\r\nRetry-After: {retry_after}\r\n\r\n");
        final_response(head.as_bytes()).retry_delay(received_at)
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
        let repeated = final_response(
            b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 5\r\nretry-after: 5\r\n\r\n",
        );
        assert_eq!(repeated.retry_delay(received_at), None);
        let absent = final_response(b"HTTP/1.1 503 Service Unavailable\r\n\r\n");
        assert_eq!(absent.retry_delay(received_at), None);
    }

    #[tokio::test]
    async fn only_the_final_head_states_its_retry_after() {
        let (mut client, mut server) = tokio::io::duplex(256);
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt as _;
            let head = b"HTTP/1.1 103 Early Hints\r\nRetry-After: 60\r\n\r\nHTTP/1.1 503 Service \
                         Unavailable\r\nContent-Length: 0\r\n\r\n";
            if let Err(error) = server.write_all(head).await {
                panic!("the in-memory server must write its response: {error}");
            }
        });
        let Ok(response) = read_final_headers(&mut client).await else {
            panic!("a valid interim block precedes a valid final head");
        };
        assert_eq!(response.status(), FinalStatus(503));
        assert_eq!(
            response.retry_delay(unix_seconds(EXAMPLE_DATE_SECONDS)),
            None
        );
    }

    #[test]
    fn every_header_block_accepts_the_exact_field_limits() {
        let mut fields = Vec::new();
        for index in 0..127 {
            fields.extend_from_slice(format!("x-{index}: a\r\n").as_bytes());
        }
        let used: usize = (0..127).map(|index| format!("x-{index}").len() + 1).sum();
        let fill = MAX_HEADER_BYTES - used - "x-fill".len();
        fields.extend_from_slice(b"x-fill: ");
        fields.extend(std::iter::repeat_n(b'x', fill));
        fields.extend_from_slice(b"\r\n");
        let mut head = b"HTTP/1.1 200 OK\r\n".to_vec();
        head.extend_from_slice(&fields);
        head.extend_from_slice(b"\r\n");
        assert_eq!(parsed(&head).status, 200);

        let mut oversized = head;
        let position = oversized.len() - 4;
        oversized.insert(position, b'x');
        let error = ParsedBlock::parse(&oversized)
            .err()
            .unwrap_or_else(|| panic!("one byte beyond the bound must fail"));
        assert!(matches!(
            error.current_context(),
            ResponseHeadError::Excessive
        ));
    }

    #[test]
    fn too_many_fields_and_invalid_framing_fail_before_status_classification() {
        let mut fields = Vec::new();
        for index in 0..129 {
            fields.extend_from_slice(format!("x-{index}: a\r\n").as_bytes());
        }
        let mut head = b"HTTP/1.1 200 OK\r\n".to_vec();
        head.extend_from_slice(&fields);
        head.extend_from_slice(b"\r\n");
        let error = ParsedBlock::parse(&head)
            .err()
            .unwrap_or_else(|| panic!("129 fields must fail"));
        assert!(matches!(
            error.current_context(),
            ResponseHeadError::Excessive
        ));

        for malformed in [
            b"HTTP/1.1 200 OK\r\nContent-Length: nope\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n",
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.1 200 OK\nx: y\n\n",
        ] {
            let error = ParsedBlock::parse(malformed)
                .err()
                .unwrap_or_else(|| panic!("malformed framing must fail"));
            assert!(
                matches!(error.current_context(), ResponseHeadError::Malformed),
                "{error:#}"
            );
        }
    }

    #[tokio::test]
    async fn interim_headers_are_checked_separately_from_final_headers() {
        let (mut client, mut server) = tokio::io::duplex(128);
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt as _;
            let head =
                b"HTTP/1.1 103 Early Hints\r\nx-hint: ready\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n";
            if let Err(error) = server.write_all(head).await {
                panic!("the in-memory server must write its response: {error}");
            }
        });
        let result = read_final_headers(&mut client).await;
        let Ok(response) = result else {
            panic!("an interim block within the bounds precedes a valid final head");
        };
        assert_eq!(response.status(), FinalStatus(204));

        let mut oversized = b"HTTP/1.1 103 Early Hints\r\nx-fill: ".to_vec();
        oversized.extend(std::iter::repeat_n(b'x', MAX_HEADER_BYTES));
        oversized.extend_from_slice(b"\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n");
        let (mut client, mut server) = tokio::io::duplex(8192);
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt as _;
            if let Err(error) = server.write_all(&oversized).await {
                panic!("the in-memory server must write its response: {error}");
            }
        });
        let error = read_final_headers(&mut client)
            .await
            .err()
            .unwrap_or_else(|| panic!("oversized interim headers must fail the attempt"));
        assert!(matches!(
            error.current_context(),
            ResponseHeadError::Excessive
        ));
    }
}
