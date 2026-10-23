//! The request head the emitter writes, read back by an HTTP/1.1 parser.
//!
//! Layer: test harness.
//! - **Owns.** A bounded domain of valid prepared requests, built only from valid methods, origins,
//!   targets, application headers and bodies, and the complete oracle of the head each one is
//!   written as: request line, generated transport fields, every application field in order, and
//!   the declared framing.
//! - **Depends on.** The emitter's request-head encoding, the vocabulary's request fields and the
//!   `httparse` request grammar.
//! - **Must not know.** Connections, DNS, TLS, responses or the host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use meticulous::ResultExt as _;
use nervix_connector::{EmitterHttpRequest, EmitterRecordId};
use nervix_models::{
    HttpApplicationHeaders, HttpBodyMode, HttpHeaderName, HttpHeaderValue, HttpMethod, HttpOrigin,
    Timestamp,
};
use url::Position;

use super::HttpRequestEmitter;

/// Spellings whose framing the property must cover deliberately: the methods that define request
/// content, those that define none, their ASCII case variants, and extension methods.
const METHODS: [&str; 12] = [
    "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "GET", "HEAD", "post", "Put", "pAtCh", "purge",
    "PROPFIND",
];
/// The bytes of an HTTP token, which a generated extension method is drawn from.
const TOKEN_BYTES: &[u8] =
    b"!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
/// Origins with and without a default port, by name and by IPv4 and IPv6 address.
const ORIGINS: [&str; 6] = [
    "http://api.example.com",
    "https://api.example.com:8443",
    "http://127.0.0.1:8080",
    "https://[::1]:9443",
    "http://example.test:80",
    "https://events.example.org",
];
/// The bytes of a generated path segment or query, none of which a target normalizes away.
const PATH_BYTES: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-_~";
/// The bytes of a generated application header name, after its `x-` prefix, which no reserved
/// transport field can take.
const NAME_BYTES: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-";
/// The bytes of a generated application header value: visible ASCII, so a value never begins or
/// ends with whitespace.
const VALUE_BYTES: &[u8] = b"!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

/// One generated prepared request.
#[derive(Debug, bolero::TypeGenerator)]
struct HeadCase {
    /// An index into [`METHODS`], or past it for an extension method of [`HeadCase::extension`].
    method: u8,
    #[generator(bolero::generator::produce_with::<Vec<u8>>().len(1_usize..=8))]
    extension: Vec<u8>,
    origin: u8,
    #[generator(bolero::generator::produce_with::<Vec<Vec<u8>>>().len(0_usize..=4))]
    segments: Vec<Vec<u8>>,
    query: Option<Vec<u8>>,
    /// Whether the application writes its own `Accept`.
    accept: bool,
    #[generator(bolero::generator::produce_with::<Vec<(Vec<u8>, Vec<u8>)>>().len(0_usize..=6))]
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: Option<Vec<u8>>,
}

/// `bytes` mapped into `alphabet`, at least one byte long and at most `limit` bytes long.
fn spelled(bytes: &[u8], alphabet: &[u8], limit: usize) -> String {
    let mut spelled = String::new();
    for byte in bytes.iter().take(limit) {
        let index = usize::from(*byte) % alphabet.len();
        spelled.push(char::from(alphabet[index]));
    }
    if spelled.is_empty() {
        spelled.push(char::from(alphabet[0]));
    }
    spelled
}

impl HeadCase {
    fn method_spelling(&self) -> String {
        if let Some(spelling) = METHODS.get(usize::from(self.method)) {
            return (*spelling).to_string();
        }
        let extension = spelled(&self.extension, TOKEN_BYTES, 8);
        // The two methods outside the publishing contract stay reachable as longer extensions.
        if extension.eq_ignore_ascii_case("CONNECT") || extension.eq_ignore_ascii_case("TRACE") {
            return format!("X{extension}");
        }
        extension
    }

    fn target_text(&self) -> String {
        let mut target = String::new();
        for segment in &self.segments {
            target.push('/');
            target.push_str(&spelled(segment, PATH_BYTES, 12));
        }
        if target.is_empty() {
            target.push('/');
        }
        if let Some(query) = &self.query {
            target.push_str("?q=");
            target.push_str(&spelled(query, PATH_BYTES, 12));
        }
        target
    }

    /// The prepared request this case describes. A body is dropped for `GET` and `HEAD`, which
    /// require `WITHOUT BODY`, so every case is a request the host can prepare.
    fn request(&self) -> EmitterHttpRequest {
        let spelling = self.method_spelling();
        let requires_no_body =
            spelling.eq_ignore_ascii_case("GET") || spelling.eq_ignore_ascii_case("HEAD");
        let body = match &self.body {
            Some(_) if requires_no_body => None,
            Some(body) => Some(body.iter().copied().take(64).collect::<Vec<u8>>()),
            None => None,
        };
        let mode = match body {
            Some(_) => HttpBodyMode::Codec,
            None => HttpBodyMode::WithoutBody,
        };
        let method =
            HttpMethod::parse(&spelling, mode).assured("the generator spells only valid methods");
        let origin_text = ORIGINS[usize::from(self.origin) % ORIGINS.len()];
        let origin =
            HttpOrigin::parse(origin_text).assured("the generator names only valid origins");
        let target = origin
            .target(&self.target_text())
            .assured("the generator builds only origin-relative targets with plain segments");
        let mut headers = HttpApplicationHeaders::default();
        if self.accept {
            headers
                .insert(
                    HttpHeaderName::parse("Accept").assured("Accept is an application header"),
                    HttpHeaderValue::parse("application/json")
                        .assured("a media type is a valid header value"),
                )
                .assured("one short header fits the application envelope");
        }
        for (name, value) in &self.headers {
            let name = format!("x-{}", spelled(name, NAME_BYTES, 12));
            let value = spelled(value, VALUE_BYTES, 24);
            headers
                .insert(
                    HttpHeaderName::parse(&name).assured("an x- token is an application header"),
                    HttpHeaderValue::parse(&value).assured("visible ASCII is a valid header value"),
                )
                .assured("at most seven short headers fit the application envelope");
        }
        EmitterHttpRequest {
            id: EmitterRecordId::new(0),
            method,
            target,
            headers,
            body,
            occurred_at: Timestamp::from_unix_nanos(0),
        }
    }
}

/// Every field the head of `request` carries, in the order the transport writes them.
fn expected_fields(request: &EmitterHttpRequest) -> Vec<(String, Vec<u8>)> {
    let url = request.target.url();
    let authority = url[Position::BeforeHost..Position::AfterPort].to_string();
    let mut fields = vec![
        ("host".to_string(), authority.into_bytes()),
        ("connection".to_string(), b"close".to_vec()),
    ];
    let mut writes_accept = false;
    for (name, _) in request.headers.iter() {
        if name.as_str().eq_ignore_ascii_case("accept") {
            writes_accept = true;
        }
    }
    if !writes_accept {
        fields.push(("accept".to_string(), b"*/*".to_vec()));
    }
    for (name, value) in request.headers.iter() {
        fields.push((
            name.as_str().to_string(),
            value.as_str().as_bytes().to_vec(),
        ));
    }
    match &request.body {
        Some(body) => fields.push((
            "content-length".to_string(),
            body.len().to_string().into_bytes(),
        )),
        None if request.method.defines_content() => {
            fields.push(("content-length".to_string(), b"0".to_vec()));
        }
        None => {}
    }
    fields
}

#[test]
fn bolero_request_heads_carry_every_prepared_field_and_their_declared_framing() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .with_type::<HeadCase>()
        .for_each(|case| {
            let request = case.request();
            let head = HttpRequestEmitter::request_head(&request);

            let mut parsed_fields = [httparse::EMPTY_HEADER; 16];
            let mut parsed = httparse::Request::new(&mut parsed_fields);
            let status = parsed
                .parse(&head)
                .assured("the transport writes a valid HTTP/1.1 request head");
            assert_eq!(
                status,
                httparse::Status::Complete(head.len()),
                "the head is exactly one complete request head and carries no body"
            );
            assert_eq!(parsed.method, Some(request.method.as_str()));
            assert_eq!(parsed.path, Some(request.target.as_str()));
            assert_eq!(parsed.version, Some(1));
            let mut fields = Vec::new();
            for field in parsed.headers.iter() {
                fields.push((field.name.to_string(), field.value.to_vec()));
            }
            assert_eq!(fields, expected_fields(&request));
        });
}
