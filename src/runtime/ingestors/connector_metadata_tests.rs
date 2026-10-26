//! Complete preservation of connection metadata through the production quiesce buffer boundary.
//!
//! Layer: test harness.
//! - **Owns.** The bounded current metadata oracle and its retain/replay property.
//! - **Depends on.** The production connector host and shared typed ingest metadata.
//! - **Must not know.** Transport drivers, sockets, schedules or a copied buffer algorithm.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the metadata property boundary declared above")
)]

use nervix_connector::IngestMessageHeaders;
use nervix_models::WebsocketsFrameType;

use super::*;

struct Headers(Vec<(String, String)>);

impl IngestMessageHeaders for Headers {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in &self.0 {
            visit(name, value);
        }
    }
}

#[test]
fn bolero_websockets_retained_metadata_preserves_every_field() {
    bolero::check!().with_type::<[u8; 8]>().for_each(|input| {
        let expected = vec![
            ("x-event".to_string(), format!("value-{:02x}", input[0])),
            ("x-event".to_string(), format!("value-{:02x}", input[1])),
            ("x-other".to_string(), format!("value-{:02x}", input[2])),
        ];
        let headers = Arc::new(RetainedIngestHeaders::capture(&Headers(expected.clone())));
        for kind in [WebsocketsFrameType::Text, WebsocketsFrameType::Binary] {
            let original = IngestMetadataRow::Websockets {
                frame_type: kind,
                headers: &headers,
            };
            let retained = RuntimeIngestorHost::retained_metadata(&original)
                .assured("a WebSockets connection declares BUFFER and its metadata is retainable");
            let decoded = retained.row();
            assert_eq!(
                decoded.scope(),
                nervix_models::IngestorMetadataScope::Websockets
            );
            let IngestMetadataRow::Websockets {
                frame_type,
                headers: decoded_headers,
            } = decoded
            else {
                panic!("the retained metadata keeps its scope");
            };
            assert_eq!(frame_type, kind);
            assert!(Arc::ptr_eq(&headers, decoded_headers));
            let mut actual = Vec::new();
            decoded_headers
                .visit(&mut |name, value| actual.push((name.to_string(), value.to_string())));
            assert_eq!(actual, expected);
        }
    });
}
