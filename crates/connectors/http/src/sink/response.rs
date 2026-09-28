//! Bounded HTTP/1.1 response heads for the outbound HTTP sink.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Reading and validating each interim and final header block and classifying a
//!   complete final status.
//! - **Depends on.** Tokio streams, the HTTP/1.1 parser and typed connector errors.
//! - **Must not know.** Emitter buffers, acknowledgements or retry cadence.

use error_stack::Report;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt as _};

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
}

/// Reads through complete final headers. Any body bytes received with their terminating CRLF are
/// discarded with the stream; the body is neither parsed nor awaited.
pub(super) async fn read_final_headers<S>(stream: &mut S) -> ResponseHeadResult<FinalStatus>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut pending = Vec::with_capacity(READ_BYTES);
    loop {
        tokio::task::consume_budget().await;
        if let Some(block) = ParsedBlock::parse(&pending)? {
            pending.drain(..block.length);
            if block.status == 101 || block.status >= 200 {
                return Ok(FinalStatus(block.status));
            }
            continue;
        }
        if pending.len() >= MAX_HEAD_BUFFER_BYTES {
            return Err(Report::new(ResponseHeadError::Excessive));
        }
        let mut chunk = [0_u8; READ_BYTES];
        let read = stream
            .read(&mut chunk)
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
        Ok(Some(Self { length, status }))
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
        assert_eq!(result.ok(), Some(FinalStatus(204)));

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
