//! The HTTP request fields that cross from a validated emitter plan to its connector.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The valid origin, method, target and application-header values of an outbound HTTP
//!   request, including the bounds that are checked both for literals and evaluated records.
//! - **Depends on.** The URL Standard parser and semantic error vocabulary.
//! - **Must not know.** NSPL parsing, registry state, Arrow batches or connector I/O.

use std::collections::BTreeMap;

use error_stack::Report;
use thiserror::Error;
use url::{Position, Url};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum HttpRequestFieldError {
    #[error("endpoint must be an http or https origin")]
    Origin,
    #[error("method is not a supported HTTP token")]
    Method,
    #[error("GET and HEAD require WITHOUT BODY")]
    MethodRequiresNoBody,
    #[error("path is not a valid origin-relative request target")]
    Target,
    #[error("header name is invalid or reserved")]
    HeaderName,
    #[error("header value is invalid")]
    HeaderValue,
    #[error("application headers exceed the 128-header or 32 KiB bound")]
    HeaderLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpBodyMode {
    Codec,
    WithoutBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpOrigin(Url);

impl HttpOrigin {
    pub fn parse(value: &str) -> Result<Self, Report<HttpRequestFieldError>> {
        if value.chars().any(|character| {
            character == '\\'
                || character == '@'
                || character.is_whitespace()
                || character.is_control()
        }) {
            return Err(Report::new(HttpRequestFieldError::Origin));
        }
        let url = Url::parse(value).map_err(|_| Report::new(HttpRequestFieldError::Origin))?;
        let lower = value.to_ascii_lowercase();
        let explicit_scheme = lower.starts_with("http://") || lower.starts_with("https://");
        if !explicit_scheme
            || lower.contains("/.")
            || lower.contains("/%2e")
            || !matches!(url.scheme(), "http" | "https")
            || url.host().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Report::new(HttpRequestFieldError::Origin));
        }
        Ok(Self(url))
    }

    pub fn target(&self, value: &str) -> Result<HttpTarget, Report<HttpRequestFieldError>> {
        if !value.starts_with('/') || value.starts_with("//") {
            return Err(Report::new(HttpRequestFieldError::Target));
        }
        if value.chars().any(|character| {
            character == '\\'
                || character == '#'
                || character.is_ascii_whitespace()
                || character.is_ascii_control()
        }) {
            return Err(Report::new(HttpRequestFieldError::Target));
        }
        let bytes = value.as_bytes();
        for (index, byte) in bytes.iter().enumerate() {
            if *byte == b'%' {
                let suffix = &bytes[index..];
                let [_, first, second, ..] = suffix else {
                    return Err(Report::new(HttpRequestFieldError::Target));
                };
                if !first.is_ascii_hexdigit() || !second.is_ascii_hexdigit() {
                    return Err(Report::new(HttpRequestFieldError::Target));
                }
            }
        }
        let parsed = self
            .0
            .join(value)
            .map_err(|_| Report::new(HttpRequestFieldError::Target))?;
        if parsed.origin() != self.0.origin() || parsed.path().starts_with("//") {
            return Err(Report::new(HttpRequestFieldError::Target));
        }
        let target = &parsed[Position::BeforePath..Position::AfterQuery];
        if target.len() > 8 * 1024 {
            return Err(Report::new(HttpRequestFieldError::Target));
        }
        Ok(HttpTarget(target.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpMethod(String);

impl HttpMethod {
    pub fn parse(value: &str, body: HttpBodyMode) -> Result<Self, Report<HttpRequestFieldError>> {
        let valid = !value.is_empty()
            && value.len() <= 64
            && value.bytes().all(http_token_byte)
            && !value.eq_ignore_ascii_case("CONNECT")
            && !value.eq_ignore_ascii_case("TRACE");
        if !valid {
            return Err(Report::new(HttpRequestFieldError::Method));
        }
        if body == HttpBodyMode::Codec
            && (value.eq_ignore_ascii_case("GET") || value.eq_ignore_ascii_case("HEAD"))
        {
            return Err(Report::new(HttpRequestFieldError::MethodRequiresNoBody));
        }
        Ok(Self(value.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpTarget(String);

impl HttpTarget {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HttpHeaderName(String);

impl HttpHeaderName {
    pub fn parse(value: &str) -> Result<Self, Report<HttpRequestFieldError>> {
        const RESERVED: &[&str] = &[
            "host",
            "content-length",
            "transfer-encoding",
            "connection",
            "keep-alive",
            "proxy-connection",
            "te",
            "trailer",
            "upgrade",
            "expect",
        ];
        let reserved = RESERVED
            .iter()
            .any(|candidate| value.eq_ignore_ascii_case(candidate));
        if value.is_empty() || !value.bytes().all(http_token_byte) || reserved {
            return Err(Report::new(HttpRequestFieldError::HeaderName));
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpHeaderValue(String);

impl HttpHeaderValue {
    pub fn parse(value: &str) -> Result<Self, Report<HttpRequestFieldError>> {
        let valid_bytes = value
            .bytes()
            .all(|byte| byte == b'\t' || (b' '..=b'~').contains(&byte) || byte >= 0x80);
        let bytes = value.as_bytes();
        let valid_edges = value.is_empty()
            || (!matches!(bytes.first(), Some(b' ' | b'\t'))
                && !matches!(bytes.last(), Some(b' ' | b'\t')));
        if !valid_bytes || !valid_edges {
            return Err(Report::new(HttpRequestFieldError::HeaderValue));
        }
        Ok(Self(value.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpApplicationHeaders {
    values: BTreeMap<HttpHeaderName, HttpHeaderValue>,
}

impl HttpApplicationHeaders {
    pub fn write(
        &mut self,
        name: HttpHeaderName,
        value: HttpHeaderValue,
    ) -> Result<(), Report<HttpRequestFieldError>> {
        let size = name
            .0
            .len()
            .checked_add(value.0.len())
            .ok_or_else(|| Report::new(HttpRequestFieldError::HeaderLimit))?;
        if size > 32 * 1024 {
            return Err(Report::new(HttpRequestFieldError::HeaderLimit));
        }
        if !self.values.contains_key(&name) && self.values.len() >= 128 {
            return Err(Report::new(HttpRequestFieldError::HeaderLimit));
        }
        self.values.insert(name, value);
        Ok(())
    }

    pub fn validate_total(&self) -> Result<(), Report<HttpRequestFieldError>> {
        // The map is bounded at 128 entries. The total is checked after every invocation has
        // replaced any earlier value with the same name.
        let mut bytes = 0usize;
        for (existing_name, existing_value) in &self.values {
            let retained_size = existing_name
                .0
                .len()
                .checked_add(existing_value.0.len())
                .ok_or_else(|| Report::new(HttpRequestFieldError::HeaderLimit))?;
            bytes = bytes
                .checked_add(retained_size)
                .ok_or_else(|| Report::new(HttpRequestFieldError::HeaderLimit))?;
        }
        if bytes > 32 * 1024 {
            return Err(Report::new(HttpRequestFieldError::HeaderLimit));
        }
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&HttpHeaderName, &HttpHeaderValue)> {
        self.values.iter()
    }
}

fn http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    #[test]
    fn request_targets_normalize_without_changing_the_origin() {
        let origin = HttpOrigin::parse("https://api.example.com")
            .assured("the test origin has an HTTPS scheme, a host, and no request path");
        for (source, target) in [
            (
                "/v1/a/../events?tag=a&tag=b&q=a+b",
                "/v1/events?tag=a&tag=b&q=a+b",
            ),
            ("/objects/a%2Fb", "/objects/a%2Fb"),
            ("/café", "/caf%C3%A9"),
            ("/events?", "/events?"),
        ] {
            assert_eq!(
                origin
                    .target(source)
                    .assured("each table entry has a valid origin-relative target")
                    .as_str(),
                target
            );
        }
        for source in [
            "//other.example/events",
            "/a/..//events",
            "/events?value=%ZZ",
            "/a#b",
            "/a\\b",
        ] {
            assert!(origin.target(source).is_err());
        }
        assert!(
            origin
                .target(&format!("/{}", "a".repeat(8 * 1024)))
                .is_err()
        );
    }

    #[test]
    fn request_fields_enforce_method_and_header_contracts() {
        assert!(HttpMethod::parse("PATCH", HttpBodyMode::Codec).is_ok());
        assert!(HttpMethod::parse("get", HttpBodyMode::Codec).is_err());
        assert!(HttpMethod::parse("get", HttpBodyMode::WithoutBody).is_ok());
        assert!(HttpMethod::parse("TRACE", HttpBodyMode::WithoutBody).is_err());
        assert!(HttpMethod::parse("CONNECT", HttpBodyMode::WithoutBody).is_err());
        assert!(HttpMethod::parse(&"X".repeat(65), HttpBodyMode::WithoutBody).is_err());
        assert!(HttpHeaderName::parse("Host").is_err());
        assert!(HttpHeaderName::parse("X\nBad").is_err());
        assert!(HttpHeaderValue::parse("a b").is_ok());
        assert!(HttpHeaderValue::parse("").is_ok());
        assert!(HttpHeaderValue::parse(" a").is_err());
        assert!(HttpHeaderValue::parse("a\r\nb").is_err());

        let mut headers = HttpApplicationHeaders::default();
        headers
            .write(
                HttpHeaderName::parse("X-Key").assured("X-Key is a valid application field name"),
                HttpHeaderValue::parse("first").assured("first is a valid field value"),
            )
            .assured("one short application header is within both envelope limits");
        headers
            .write(
                HttpHeaderName::parse("x-key").assured("x-key is a valid application field name"),
                HttpHeaderValue::parse("second").assured("second is a valid field value"),
            )
            .assured("replacing a short header with another short value is within the limit");
        assert_eq!(headers.iter().count(), 1);
        assert_eq!(
            headers
                .iter()
                .next()
                .assured("the two writes leave one header")
                .1
                .as_str(),
            "second"
        );
        let oversized = HttpHeaderValue::parse(&"a".repeat(32 * 1024))
            .assured("ASCII letter bytes are permitted in a header value");
        assert!(
            headers
                .write(
                    HttpHeaderName::parse("X-Other").assured("valid name"),
                    oversized
                )
                .is_err()
        );
    }

    #[test]
    fn endpoint_accepts_only_an_explicit_origin() {
        assert!(HttpOrigin::parse("http://localhost:8080/").is_ok());
        assert!(HttpOrigin::parse("https://api.example.com").is_ok());
        for value in [
            "https://key@api.example.com",
            "https://api.example.com/a/..",
            "https://api.example.com/a",
            "https://api.example.com?x=1",
            "https://api.example.com#part",
            "https://api.example.com\\a",
            " https://api.example.com",
            "http:api.example.com",
            "ftp://api.example.com",
        ] {
            assert!(HttpOrigin::parse(value).is_err());
        }
    }

    #[test]
    fn application_header_limits_apply_after_case_insensitive_replacement() {
        let mut headers = HttpApplicationHeaders::default();
        let value = HttpHeaderValue::parse("").assured("an empty header value is permitted");
        // The loop has exactly 128 iterations, the declared application-header count bound.
        for index in 0..128 {
            let name = HttpHeaderName::parse(&format!("X-{index}"))
                .assured("an ASCII alphanumeric application header name is valid");
            headers
                .write(name, value.clone())
                .assured("up to 128 short headers fit the count and byte bounds");
        }
        let next =
            HttpHeaderName::parse("X-next").assured("X-next is a valid application header name");
        assert!(headers.write(next, value).is_err());

        let replacement =
            HttpHeaderName::parse("x-0").assured("x-0 is a valid application header name");
        let replacement_value =
            HttpHeaderValue::parse("replacement").assured("replacement is a valid header value");
        headers
            .write(replacement, replacement_value)
            .assured("replacing one existing header does not increase the count past 128");
        assert_eq!(headers.iter().count(), 128);

        let mut sized = HttpApplicationHeaders::default();
        let large = HttpHeaderValue::parse(&"a".repeat(20 * 1024))
            .assured("ASCII letters are valid header-value bytes");
        sized
            .write(
                HttpHeaderName::parse("X-One").assured("valid application header name"),
                large.clone(),
            )
            .assured("an individual header below 32 KiB is valid");
        sized
            .write(
                HttpHeaderName::parse("X-Two").assured("valid application header name"),
                large,
            )
            .assured("each individual header is below 32 KiB");
        assert!(sized.validate_total().is_err());
        sized
            .write(
                HttpHeaderName::parse("x-one").assured("valid application header name"),
                HttpHeaderValue::parse("short").assured("valid header value"),
            )
            .assured("a shorter replacement is an individually valid write");
        sized
            .validate_total()
            .assured("the replacement brings the final envelope below 32 KiB");
    }
}
