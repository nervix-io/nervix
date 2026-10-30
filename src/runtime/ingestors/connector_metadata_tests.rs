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

use nervix_connector::{
    IngestMessageHeaders, IngestRowHeaders, RetainedIngestHeaders, RetainedIngestMetadata,
    RetainedIngestMetadataValue,
};
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
            let frame =
                RetainedIngestMetadata::from_values(vec![RetainedIngestMetadataValue::String(
                    kind.as_ref().to_string(),
                )]);
            let original = IngestMetadataRow {
                fields: &frame,
                headers: IngestRowHeaders::Shared(&headers),
                position: None,
            };
            let retained = BufferedIngestMetadata::retain(&original);
            let decoded = retained.row();
            assert_eq!(RetainedIngestMetadata::capture(decoded.fields), frame);
            assert!(decoded.position.is_none());
            let IngestRowHeaders::Shared(decoded_headers) = decoded.headers else {
                panic!("the retained metadata keeps the connection's shared header snapshot");
            };
            assert!(Arc::ptr_eq(&headers, decoded_headers));
            let mut actual = Vec::new();
            decoded_headers
                .visit(&mut |name, value| actual.push((name.to_string(), value.to_string())));
            assert_eq!(actual, expected);
        }
    });
}
