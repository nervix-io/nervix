//! The HTTP request fields that cross from a validated emitter plan to its connector.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The valid origin, method, target and application-header values of an outbound HTTP
//!   request, including the bounds that are checked both for literals and evaluated records.
//! - **Depends on.** The URL Standard parser and semantic error vocabulary.
//! - **Must not know.** Plexus parsing, registry state, Arrow batches or connector I/O.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::collections::BTreeMap;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
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
    #[error("application headers exceed the configured field-count or byte bound")]
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
        let url = Url::parse(value)
            .map_err(|cause| Report::new(cause).change_context(HttpRequestFieldError::Origin))?;
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
            .map_err(|cause| Report::new(cause).change_context(HttpRequestFieldError::Target))?;
        if parsed.origin() != self.0.origin() || parsed.path().starts_with("//") {
            return Err(Report::new(HttpRequestFieldError::Target));
        }
        let target = HttpTarget { url: parsed };
        if target.as_str().len() > 8 * 1024 {
            return Err(Report::new(HttpRequestFieldError::Target));
        }
        Ok(target)
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

    /// Whether the method defines a meaning for content a request encloses: `POST`, `PUT` and
    /// `PATCH`, in any ASCII case, as their case variants count for the body rules too. A request
    /// with such a method declares its length even when it encloses nothing, as RFC 9110
    /// section 8.6 asks of a client.
    pub fn defines_content(&self) -> bool {
        self.0.eq_ignore_ascii_case("POST")
            || self.0.eq_ignore_ascii_case("PUT")
            || self.0.eq_ignore_ascii_case("PATCH")
    }
}

/// A request target normalized on the origin it was parsed against, which it cannot leave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpTarget {
    /// The origin followed by the normalized path and query.
    url: Url,
}

impl HttpTarget {
    /// The normalized path and query, exactly as the request line carries them.
    pub fn as_str(&self) -> &str {
        &self.url[Position::BeforePath..Position::AfterQuery]
    }

    /// The absolute URL a request is sent to: the origin followed by this target.
    pub fn url(&self) -> &Url {
        &self.url
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

/// The class-typed header bounds captured for one request publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpHeaderLimits {
    maximum_bytes: crate::Setting<crate::HttpHeaderBytes>,
    maximum_fields: crate::Setting<crate::HttpHeaderFields>,
}

impl HttpHeaderLimits {
    pub fn new(
        maximum_bytes: crate::Setting<crate::HttpHeaderBytes>,
        maximum_fields: crate::Setting<crate::HttpHeaderFields>,
    ) -> Self {
        Self {
            maximum_bytes,
            maximum_fields,
        }
    }
}

impl Default for HttpHeaderLimits {
    fn default() -> Self {
        Self::new(
            crate::Setting::resolve(None, &()).assured("the default header byte bound is valid"),
            crate::Setting::resolve(None, &()).assured("the default header count bound is valid"),
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpApplicationHeaders {
    limits: HttpHeaderLimits,
    values: BTreeMap<HttpHeaderName, HttpHeaderValue>,
}

impl HttpApplicationHeaders {
    pub fn with_limits(limits: HttpHeaderLimits) -> Self {
        Self {
            limits,
            values: BTreeMap::new(),
        }
    }

    /// Writes `value` under `name`, replacing the value an earlier write gave the same name. A
    /// write that would take the headers past the configured field count, or whose name and value together exceed
    /// the configured byte bound, is refused and changes nothing.
    pub fn insert(
        &mut self,
        name: HttpHeaderName,
        value: HttpHeaderValue,
    ) -> Result<(), Report<HttpRequestFieldError>> {
        let size = name
            .0
            .len()
            .checked_add(value.0.len())
            .ok_or_else(|| Report::new(HttpRequestFieldError::HeaderLimit))?;
        if u64::try_from(size).assured("header sizes fit the process address space")
            > *self.limits.maximum_bytes.value()
        {
            return Err(Report::new(HttpRequestFieldError::HeaderLimit));
        }
        if !self.values.contains_key(&name)
            && u64::try_from(self.values.len())
                .assured("header counts fit the process address space")
                >= *self.limits.maximum_fields.value()
        {
            return Err(Report::new(HttpRequestFieldError::HeaderLimit));
        }
        self.values.insert(name, value);
        Ok(())
    }

    /// Checks the configured byte bound on every retained name and value together, once every invocation
    /// has replaced any earlier value written under the same name.
    pub fn validate_total(&self) -> Result<(), Report<HttpRequestFieldError>> {
        if u64::try_from(self.field_bytes()).assured("header sizes fit the process address space")
            > *self.limits.maximum_bytes.value()
        {
            return Err(Report::new(HttpRequestFieldError::HeaderLimit));
        }
        Ok(())
    }

    /// The UTF-8 bytes of every retained name and value.
    pub fn field_bytes(&self) -> usize {
        // Retained strings together fit the process address space.
        let mut bytes = 0usize;
        for (name, value) in &self.values {
            let field = name
                .0
                .len()
                .checked_add(value.0.len())
                .assured("the retained header strings fit the process address space");
            bytes = bytes
                .checked_add(field)
                .assured("the retained header strings fit the process address space");
        }
        bytes
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
        let target = origin
            .target("/v1/a/../events?")
            .assured("a dot segment and an empty query form a valid origin-relative target");
        assert_eq!(target.as_str(), "/v1/events?");
        assert_eq!(target.url().as_str(), "https://api.example.com/v1/events?");
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
    fn methods_that_define_request_content_are_recognized_in_every_ascii_case() {
        for method in ["POST", "PUT", "PATCH", "post", "pUt", "Patch"] {
            let parsed = HttpMethod::parse(method, HttpBodyMode::WithoutBody)
                .assured("the method is a valid HTTP token");
            assert!(parsed.defines_content(), "{method} defines request content");
        }
        for method in ["DELETE", "GET", "HEAD", "OPTIONS", "purge", "POSTS", "PUTS"] {
            let parsed = HttpMethod::parse(method, HttpBodyMode::WithoutBody)
                .assured("the method is a valid HTTP token");
            assert!(
                !parsed.defines_content(),
                "{method} defines no request content"
            );
        }
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
            .insert(
                HttpHeaderName::parse("X-Key").assured("X-Key is a valid application field name"),
                HttpHeaderValue::parse("first").assured("first is a valid field value"),
            )
            .assured("one short application header is within both envelope limits");
        headers
            .insert(
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
                .insert(
                    HttpHeaderName::parse("X-Other").assured("valid name"),
                    oversized
                )
                .is_err()
        );
    }

    /// HTTP REQUEST emitters declare sorted by name with lowercase names: the application headers
    /// keep one value per name as ASCII lowercase records it, the last one written, and a request
    /// carries them in ascending order of name.
    #[test]
    fn application_headers_keep_the_last_value_of_each_lowercase_name_in_name_order() {
        let mut headers = HttpApplicationHeaders::default();
        for (name, value) in [
            ("X-Trace", "a"),
            ("Tenant", "x"),
            ("x-trace", "b"),
            ("Accept", "y"),
        ] {
            let name = HttpHeaderName::parse(name).assured("each name is an application field");
            let value = HttpHeaderValue::parse(value).assured("each value is a valid field value");
            headers
                .insert(name, value)
                .assured("four short headers fit the application envelope");
        }
        let mut carried = Vec::new();
        for (name, value) in headers.iter() {
            carried.push(format!("{}={}", name.as_str(), value.as_str()));
        }
        assert_eq!(carried, ["accept=y", "tenant=x", "x-trace=b"]);
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
                .insert(name, value.clone())
                .assured("up to 128 short headers fit the count and byte bounds");
        }
        let next =
            HttpHeaderName::parse("X-next").assured("X-next is a valid application header name");
        assert!(headers.insert(next, value).is_err());

        let replacement =
            HttpHeaderName::parse("x-0").assured("x-0 is a valid application header name");
        let replacement_value =
            HttpHeaderValue::parse("replacement").assured("replacement is a valid header value");
        headers
            .insert(replacement, replacement_value)
            .assured("replacing one existing header does not increase the count past 128");
        assert_eq!(headers.iter().count(), 128);

        let mut sized = HttpApplicationHeaders::default();
        let large = HttpHeaderValue::parse(&"a".repeat(20 * 1024))
            .assured("ASCII letters are valid header-value bytes");
        sized
            .insert(
                HttpHeaderName::parse("X-One").assured("valid application header name"),
                large.clone(),
            )
            .assured("an individual header below 32 KiB is valid");
        sized
            .insert(
                HttpHeaderName::parse("X-Two").assured("valid application header name"),
                large,
            )
            .assured("each individual header is below 32 KiB");
        assert!(sized.validate_total().is_err());
        sized
            .insert(
                HttpHeaderName::parse("x-one").assured("valid application header name"),
                HttpHeaderValue::parse("short").assured("valid header value"),
            )
            .assured("a shorter replacement is an individually valid write");
        sized
            .validate_total()
            .assured("the replacement brings the final envelope below 32 KiB");
        assert_eq!(
            sized.field_bytes(),
            "x-one".len() + "short".len() + "x-two".len() + 20 * 1024
        );
    }
}

#[cfg(test)]
mod parameter_tests {
    use super::*;
    use crate::{ClusterParameterChange, ClusterParameterUpdates, ClusterParameterValues};

    #[test]
    fn a_request_captures_both_header_bounds_and_allows_replacing_a_name_at_capacity() {
        let changes = ClusterParameterUpdates::try_from(vec![
            ClusterParameterChange::HttpHeaderFields(2),
            ClusterParameterChange::HttpHeaderBytes(64),
        ])
        .assured("the candidate assigns two different bounds");
        let policy = ClusterParameterValues::default()
            .changed(&changes)
            .assured("the candidate has valid header bounds");
        let mut headers = HttpApplicationHeaders::with_limits(policy.http_header_limits());
        let name = |value| HttpHeaderName::parse(value).assured("the fixture has a valid name");
        let value = || HttpHeaderValue::parse("v").assured("the fixture value has no whitespace");
        headers
            .insert(name("a"), value())
            .assured("one header fits");
        headers
            .insert(name("b"), value())
            .assured("two distinct headers fit");
        assert!(headers.insert(name("c"), value()).is_err());
        headers
            .insert(name("A"), value())
            .assured("case-folded replacement uses the same field");
        assert_eq!(headers.iter().count(), 2);
        let changes =
            ClusterParameterUpdates::try_from(vec![ClusterParameterChange::HttpHeaderFields(3)])
                .assured("the raised bound has one assignment");
        let raised = policy
            .changed(&changes)
            .assured("three fields are representable");
        let mut next = HttpApplicationHeaders::with_limits(raised.http_header_limits());
        for field in ["a", "b", "c"] {
            next.insert(name(field), value())
                .assured("the next publication admits three fields");
        }
        assert!(headers.insert(name("c"), value()).is_err());
        let changes =
            ClusterParameterUpdates::try_from(vec![ClusterParameterChange::HttpHeaderBytes(1)])
                .assured("the lower byte bound has one assignment");
        let lowered = raised
            .changed(&changes)
            .assured("one byte is a valid bound");
        assert!(
            HttpApplicationHeaders::with_limits(lowered.http_header_limits())
                .insert(name("a"), value())
                .is_err()
        );
    }
}
