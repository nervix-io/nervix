//! The requests the emitter hands hyper, against every prepared field and route.
//!
//! Layer: test harness.
//! - **Owns.** A bounded domain of valid prepared requests, built only from valid methods, origins,
//!   targets, application headers, bodies and routes, and the complete oracle of the request each
//!   one becomes: its method, its target in origin or absolute form, the transport's fields, the
//!   proxy's credentials, every application field in order, the declared empty length and the exact
//!   body; and of the tunnel request that opens a proxy's way to its target.
//! - **Depends on.** The emitter's conversion into hyper's request type and the vocabulary's
//!   request fields.
//! - **Must not know.** Connections, DNS, TLS, responses or the host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt as _, Full};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{EmitterHttpRequest, EmitterRecordId};
use nervix_models::{
    HttpApplicationHeaders, HttpBodyMode, HttpHeaderName, HttpHeaderValue, HttpMethod, HttpOrigin,
    Timestamp,
};
use url::Position;

use super::{HttpRequestEmitter, HttpVersion, RequestForm};

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

/// The bytes of generated proxy credentials, after their `Basic ` scheme: the Base64 alphabet.
const CREDENTIAL_BYTES: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";

/// How a generated request travels, which decides how it names its target and what it carries.
#[derive(Debug, Clone, Copy, bolero::TypeGenerator)]
enum RequestRoute {
    /// Straight to its endpoint, or inside a proxy's tunnel to it, over HTTP/1.1.
    Http1,
    /// Straight to its endpoint, or inside a proxy's tunnel to it, over HTTP/2.
    Http2,
    /// To a proxy that forwards it over HTTP/1.1, with or without the proxy's credentials.
    Forwarded { authenticated: bool },
}

/// One generated prepared request, with the route it travels. The route's fields come last, so a
/// corpus input written before routes existed still spells the same request.
#[derive(Debug, bolero::TypeGenerator)]
struct RequestCase {
    /// An index into [`METHODS`], or past it for an extension method of [`RequestCase::extension`].
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
    route: RequestRoute,
    /// The proxy's credentials, after their `Basic ` scheme, for a route that carries them and for
    /// the tunnel request.
    #[generator(bolero::generator::produce_with::<Vec<u8>>().len(1_usize..=24))]
    credentials: Vec<u8>,
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

impl RequestCase {
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

    /// The proxy's `Proxy-Authorization` value.
    fn authorization(&self) -> String {
        format!("Basic {}", spelled(&self.credentials, CREDENTIAL_BYTES, 24))
    }

    /// The version and form the route sends the request in.
    fn sent_as<'a>(&self, authorization: &'a str) -> (HttpVersion, RequestForm<'a>) {
        match self.route {
            RequestRoute::Http1 => (HttpVersion::Http1, RequestForm::Direct),
            RequestRoute::Http2 => (HttpVersion::Http2, RequestForm::Direct),
            RequestRoute::Forwarded {
                authenticated: true,
            } => (
                HttpVersion::Http1,
                RequestForm::Forwarded {
                    authorization: Some(authorization),
                },
            ),
            RequestRoute::Forwarded {
                authenticated: false,
            } => (
                HttpVersion::Http1,
                RequestForm::Forwarded {
                    authorization: None,
                },
            ),
        }
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

/// Every field the request carries in `version` along `form`, in the order the transport adds
/// them.
fn expected_fields(
    request: &EmitterHttpRequest,
    version: HttpVersion,
    form: RequestForm<'_>,
) -> Vec<(String, Vec<u8>)> {
    let url = request.target.url();
    let mut fields = Vec::new();
    if version == HttpVersion::Http1 {
        let authority = url[Position::BeforeHost..Position::AfterPort].to_string();
        fields.push(("host".to_string(), authority.into_bytes()));
        fields.push(("connection".to_string(), b"close".to_vec()));
    }
    if let RequestForm::Forwarded {
        authorization: Some(authorization),
    } = form
    {
        fields.push((
            "proxy-authorization".to_string(),
            authorization.as_bytes().to_vec(),
        ));
    }
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
    if request.body.is_none() && request.method.defines_content() {
        fields.push(("content-length".to_string(), b"0".to_vec()));
    }
    fields
}

/// Every field `request` carries, by name and exact value bytes, in order.
fn carried_fields(request: &Request<Full<Bytes>>) -> Vec<(String, Vec<u8>)> {
    let mut fields = Vec::new();
    for (name, value) in request.headers() {
        fields.push((name.as_str().to_string(), value.as_bytes().to_vec()));
    }
    fields
}

/// The bytes of `request`'s body, which a full body yields at once.
fn carried_body(request: Request<Full<Bytes>>) -> Vec<u8> {
    let collected = request.into_body().collect();
    let polled = std::pin::pin!(collected);
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    match std::future::Future::poll(polled, &mut context) {
        std::task::Poll::Ready(collected) => collected
            .assured("a full body cannot fail")
            .to_bytes()
            .to_vec(),
        std::task::Poll::Pending => panic!("a full body is ready at once"),
    }
}

/// The tunnel request a proxy receives for `request`: `CONNECT` to its host and port, which it
/// also names as its `Host`, with the proxy's credentials.
fn assert_tunnel_request(request: &EmitterHttpRequest, authorization: &str) {
    let url = request.target.url();
    let tunnel = HttpRequestEmitter::tunnel_request(url, Some(authorization))
        .assured("every generated target has a host and a known port");
    let host = url.host_str().assured("every generated target has a host");
    let port = url
        .port_or_known_default()
        .assured("every generated target has a known port");
    let authority = format!("{host}:{port}");
    assert_eq!(tunnel.method(), http::Method::CONNECT);
    assert_eq!(tunnel.uri().to_string(), authority);
    assert_eq!(
        carried_fields(&tunnel),
        vec![
            ("host".to_string(), authority.into_bytes()),
            (
                "proxy-authorization".to_string(),
                authorization.as_bytes().to_vec()
            ),
        ]
    );
}

#[test]
fn bolero_requests_carry_every_prepared_field_on_every_route() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .with_type::<RequestCase>()
        .for_each(|case| {
            let request = case.request();
            let authorization = case.authorization();
            let (version, form) = case.sent_as(&authorization);
            let url = request.target.url();
            let sent = HttpRequestEmitter::http_request(&request, url, version, form)
                .assured("every prepared request converts");

            let expected_target = if version == HttpVersion::Http2 || form != RequestForm::Direct {
                &url[..Position::AfterQuery]
            } else {
                request.target.as_str()
            };
            assert_eq!(sent.method().as_str(), request.method.as_str());
            assert_eq!(sent.uri().to_string(), expected_target);
            assert_eq!(
                carried_fields(&sent),
                expected_fields(&request, version, form)
            );
            let expected_body = request.body.clone().unwrap_or_default();
            assert_eq!(carried_body(sent), expected_body);
            assert_tunnel_request(&request, &authorization);
        });
}
