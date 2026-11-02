//! Endpoint request and WebSocket frame metadata across the quiesce buffer and the host's
//! projection.
//!
//! Layer: test harness.
//! - **Owns.** Generated endpoint requests and WebSocket frames with their headers, and the oracle
//!   their declared metadata values, retained copies and columns are held to.
//! - **Depends on.** The production endpoint metadata, the endpoint entities' metadata
//!   declarations, the quiesce buffer's retained row and the host's metadata projection.
//! - **Must not know.** Sockets, HTTP parsing, route tables or the registry.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "tests the endpoint metadata boundary declared above"
    )
)]

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use arrow_array::{Array, cast::AsArray as _};
use nervix_arbitrary::Entropy;
use nervix_connector::{
    IngestMessageHeaders, IngestRowHeaders, RetainedIngestHeaders, RetainedIngestMetadata,
    RetainedIngestMetadataValue as Value,
};
use nervix_models::{
    HttpIngestorEntity, IngestorEntityKind, IngestorMetadataDeclaration, IngestorMetadataType,
    WebsocketsIngestorEntity,
};

use super::*;

const CASE_BYTES: usize = 256;
const MAX_TEXT_CHARS: usize = 12;
const MAX_HOST_CHARS: usize = 12;
const MAX_HEADERS: usize = 4;
/// Characters of a request target or a header value, within ASCII and beyond it.
const TEXT: [char; 12] = ['a', 'Z', '0', '/', '?', '=', '&', '%', '.', '-', 'ü', '✓'];
/// Characters of a host name in both letter cases, so routing's lowercase reading is exercised.
const HOST_CHARS: [char; 8] = ['a', 'b', 'Z', 'Q', '0', '9', '.', '-'];
const METHODS: [&str; 3] = ["POST", "PUT", "PATCH"];
const HEADER_NAMES: [&str; 3] = ["x-tenant", "x-trace", "content-type"];

/// Headers a generated request carries, in the order it carries them.
struct GeneratedHeaders(Vec<(String, String)>);

impl IngestMessageHeaders for GeneratedHeaders {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in &self.0 {
            visit(name, value);
        }
    }
}

fn visited_headers(headers: &dyn IngestMessageHeaders) -> Vec<(String, String)> {
    let mut visited = Vec::new();
    headers.visit(&mut |name, value| visited.push((name.to_string(), value.to_string())));
    visited
}

/// A generated `Host` value: a name in either letter case and, optionally, a port.
struct GeneratedHost {
    name: String,
    port: Option<u16>,
}

impl GeneratedHost {
    /// The value a client writes in its `Host` header.
    fn written(&self) -> String {
        match self.port {
            Some(port) => format!("{}:{port}", self.name),
            None => self.name.clone(),
        }
    }

    /// The host routing reads: the name in ASCII lowercase, without the port.
    fn routed(&self) -> String {
        self.name.to_ascii_lowercase()
    }
}

struct GeneratedRequest {
    method: &'static str,
    path: String,
    query: Option<String>,
    remote_addr: SocketAddr,
    host: GeneratedHost,
    headers: Vec<(String, String)>,
}

impl GeneratedRequest {
    /// The values `HTTP ENDPOINT` declares, in declared order, as this request carries them.
    fn expected(&self) -> Vec<Value> {
        let query = match &self.query {
            Some(query) => Value::String(query.clone()),
            None => Value::Absent,
        };
        vec![
            Value::String(self.method.to_string()),
            Value::String(self.path.clone()),
            query,
            Value::String(self.remote_addr.to_string()),
            Value::String(self.host.routed()),
        ]
    }
}

struct GeneratedFrame {
    frame_type: WebsocketsFrameType,
    remote_addr: SocketAddr,
    path: String,
    headers: Arc<RetainedIngestHeaders>,
    header_pairs: Vec<(String, String)>,
}

impl GeneratedFrame {
    /// The values `WEBSOCKETS ENDPOINT` declares, in declared order, as this frame carries them.
    fn expected(&self) -> Vec<Value> {
        vec![
            Value::String(self.frame_type.as_ref().to_string()),
            Value::String(self.remote_addr.to_string()),
            Value::String(self.path.clone()),
        ]
    }
}

/// Reads bounded generated cases from a property's bytes.
struct Cases<'bytes> {
    entropy: Entropy<'bytes>,
}

impl Cases<'_> {
    fn text(&mut self) -> String {
        let length = self.entropy.count(MAX_TEXT_CHARS);
        let mut text = String::new();
        for _ in 0..length {
            text.push(self.entropy.pick(TEXT));
        }
        text
    }

    fn port(&mut self) -> u16 {
        let port = self.entropy.boundary_biased(0..=u64::from(u16::MAX));
        u16::try_from(port).verified("the port was drawn from the u16 range")
    }

    fn peer(&mut self) -> SocketAddr {
        let address = if self.entropy.flag() {
            let bits = self.entropy.up_to(u64::from(u32::MAX));
            let bits = u32::try_from(bits).verified("the bits were drawn from the u32 range");
            IpAddr::V4(Ipv4Addr::from(bits))
        } else {
            let high = u128::from(self.entropy.any_u64());
            let low = u128::from(self.entropy.any_u64());
            IpAddr::V6(Ipv6Addr::from((high << 64) | low))
        };
        SocketAddr::new(address, self.port())
    }

    fn path(&mut self) -> String {
        let mut path = String::from("/");
        path.push_str(&self.text());
        path
    }

    /// No query, or a query of any text, an empty one included.
    fn query(&mut self) -> Option<String> {
        if self.entropy.flag() {
            return None;
        }
        Some(self.text())
    }

    fn host(&mut self) -> GeneratedHost {
        let length = self.entropy.count(MAX_HOST_CHARS);
        let mut name = String::from("h");
        for _ in 0..length {
            name.push(self.entropy.pick(HOST_CHARS));
        }
        let port = if self.entropy.flag() {
            Some(self.port())
        } else {
            None
        };
        GeneratedHost { name, port }
    }

    fn headers(&mut self) -> Vec<(String, String)> {
        let count = self.entropy.count(MAX_HEADERS);
        let mut headers = Vec::with_capacity(count);
        for _ in 0..count {
            let name = self.entropy.pick(HEADER_NAMES);
            headers.push((name.to_string(), self.text()));
        }
        headers
    }

    fn request(&mut self) -> GeneratedRequest {
        GeneratedRequest {
            method: self.entropy.pick(METHODS),
            path: self.path(),
            query: self.query(),
            remote_addr: self.peer(),
            host: self.host(),
            headers: self.headers(),
        }
    }

    fn frame(&mut self) -> GeneratedFrame {
        let frame_type = if self.entropy.flag() {
            WebsocketsFrameType::Text
        } else {
            WebsocketsFrameType::Binary
        };
        let header_pairs = self.headers();
        let headers = Arc::new(RetainedIngestHeaders::capture(&GeneratedHeaders(
            header_pairs.clone(),
        )));
        GeneratedFrame {
            frame_type,
            remote_addr: self.peer(),
            path: self.path(),
            headers,
            header_pairs,
        }
    }
}

/// The value one built `STRING` column holds in its only row, `Absent` where it holds a null.
fn string_value(column: &dyn Array) -> Value {
    if column.is_null(0) {
        return Value::Absent;
    }
    let column = column
        .as_string_opt::<i32>()
        .verified("every endpoint metadata field is declared STRING, which builds a string column");
    Value::String(column.value(0).to_string())
}

/// `row` hands over exactly `expected`, one value of each field `declared` lists and of its type;
/// the quiesce buffer's retained copy replays those values with `headers`; and a group whose
/// programs read every declared field builds each column with the row's value.
fn assert_retained_and_projected(
    declared: IngestorMetadataDeclaration,
    row: IngestMetadataRow<'_>,
    expected: &[Value],
    headers: &[(String, String)],
) {
    let captured = RetainedIngestMetadata::capture(row.fields);
    assert_eq!(captured.values(), expected);
    assert_eq!(captured.values().len(), declared.fields().len());
    for (field, value) in declared.fields().iter().zip(captured.values()) {
        assert_eq!(field.ty(), IngestorMetadataType::String, "{}", field.name());
        if let Some(ty) = value.ty() {
            assert_eq!(ty, field.ty(), "the value of '{}'", field.name());
        }
    }

    let buffered = BufferedIngestMetadata::retain(&row);
    let replayed = buffered.row();
    assert_eq!(RetainedIngestMetadata::capture(replayed.fields), captured);
    assert!(replayed.position.is_none());
    assert_eq!(visited_headers(replayed.headers.headers()), headers);

    let mut names = Vec::with_capacity(declared.fields().len());
    for field in declared.fields() {
        names.push(field.name());
    }
    let projection = IngestMetadataProjection::reading(declared, names)
        .verified("every name read is one of the declaration's own fields");
    let mut builders = IngestMetadataBuilders::new(projection, 1);
    builders
        .append(&replayed)
        .verified("the replayed row hands over one value of each declared field's type");
    let metadata = builders
        .finish()
        .verified("the group's one row was appended whole");
    for (field, value) in declared.fields().iter().zip(expected) {
        let column = metadata
            .field_column(field.name())
            .verified("the group's own row selects every column it built")
            .verified("a group that reads every declared field builds a column for each");
        assert_eq!(&string_value(column.as_ref()), value, "{}", field.name());
    }
}

#[test]
fn bolero_endpoint_request_and_frame_metadata_retain_and_project_exactly() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|bytes: &[u8]| {
            let mut cases = Cases {
                entropy: Entropy::new(bytes),
            };

            let request = cases.request();
            let written_host = request.host.written();
            let metadata = HttpEndpointRequestMetadata::new(
                request.method,
                &request.path,
                request.query.as_deref(),
                request.remote_addr,
                &written_host,
            );
            let headers = GeneratedHeaders(request.headers.clone());
            let row = IngestMetadataRow {
                fields: &metadata,
                headers: IngestRowHeaders::Borrowed(&headers),
                position: None,
            };
            assert_retained_and_projected(
                IngestorEntityKind::Http(HttpIngestorEntity::Endpoint)
                    .capabilities()
                    .metadata,
                row,
                &request.expected(),
                &request.headers,
            );

            let frame = cases.frame();
            let metadata = WebsocketsEndpointFrameMetadata {
                frame_type: frame.frame_type,
                remote_addr: frame.remote_addr,
                path: &frame.path,
            };
            let row = IngestMetadataRow {
                fields: &metadata,
                headers: IngestRowHeaders::Shared(&frame.headers),
                position: None,
            };
            assert_retained_and_projected(
                IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint)
                    .capabilities()
                    .metadata,
                row,
                &frame.expected(),
                &frame.header_pairs,
            );
            let replayed = BufferedIngestMetadata::retain(&row);
            let IngestRowHeaders::Shared(snapshot) = replayed.row().headers else {
                panic!("the buffer keeps the connection's shared header snapshot by reference");
            };
            assert!(Arc::ptr_eq(snapshot, &frame.headers));
        });
}
