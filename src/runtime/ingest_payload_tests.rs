//! Focused checks of the original payload facts through the production projection.
//!
//! Layer: test harness.
//! - **Owns.** Required count types, lazy visitors and original batch facts after row selection.
//! - **Depends on.** The connector value boundary and production group column builders.
//! - **Must not know.** Plexus parsing, transport drivers, ACK protocols or cluster placement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "payload projection owner checks")
)]

use arrow_array::{Array as _, UInt64Array};
use arrow_buffer::BooleanBuffer;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    IngestMessageMetadata, IngestMetadataRow, IngestMetadataValues, NoIngestHeaders,
};
use nervix_models::{IngestorMetadataDeclaration, IngestorPayloadDeclaration};
use nervix_primitives::sync::Arc;

use super::{IngestFilterMapMetadata, IngestMetadataBuilders, IngestMetadataProjection};

struct Body(usize);

impl IngestMessageMetadata for Body {
    fn payload_bytes(&self) -> usize {
        self.0
    }
    fn visit_metadata(&self, _values: &mut dyn IngestMetadataValues) {
        panic!("a payload-only program must not visit transport metadata");
    }
}

fn projection(
    declaration: IngestorPayloadDeclaration,
    fields: &[&str],
) -> Arc<IngestMetadataProjection> {
    IngestMetadataProjection::reading_input(
        IngestorMetadataDeclaration::NONE,
        std::iter::empty(),
        declaration,
        fields.iter().copied(),
    )
    .assured("the fixture requests only declared payload fields")
}

fn counts(metadata: &IngestFilterMapMetadata, field: &str) -> Vec<u64> {
    let column = metadata
        .field_column(field)
        .assured("the selected rows are in bounds")
        .assured("the fixture projects the requested field");
    let values = column
        .as_any()
        .downcast_ref::<UInt64Array>()
        .assured("payload counts have their declared U64 type");
    assert_eq!(values.null_count(), 0);
    values.values().to_vec()
}

#[test]
fn ingest_payload_bytes_use_the_existing_column_builder_without_visiting_metadata() {
    let projection = projection(IngestorPayloadDeclaration::BYTES, &["bytes"]);
    let mut builders = IngestMetadataBuilders::new(projection, 2);
    for bytes in [0, 1_337] {
        let body = Body(bytes);
        builders
            .append(&IngestMetadataRow::headers(&NoIngestHeaders, &body), None)
            .assured("a declared byte count is representable");
    }
    let metadata = builders.finish().assured("the two payload rows finish");
    assert_eq!(counts(&metadata, "payload.bytes"), [0, 1_337]);
    let schema = metadata.columns.integration_fields.schema();
    assert_eq!(schema.fields().len(), 1);
    assert!(!schema.field(0).is_nullable());
    assert!(
        metadata
            .field_column("payload.rows")
            .assured("missing fields are ordinary absence")
            .is_none()
    );
}

#[test]
fn ingest_payload_batch_facts_keep_the_original_counts_through_selection() {
    let metadata = IngestFilterMapMetadata::client_batch(
        projection(IngestorPayloadDeclaration::BATCH, &["bytes", "rows"]),
        &nervix_connector::NoIngestMetadata(4_096),
        3,
    )
    .assured("a three-row client batch projects its original facts");
    let selected = metadata
        .select(&BooleanBuffer::from(vec![false, true, true]))
        .assured("the selection has the batch's row count");
    assert!(Arc::ptr_eq(&metadata.columns, &selected.columns));
    assert_eq!(counts(&selected, "payload.bytes"), [4_096, 4_096]);
    assert_eq!(counts(&selected, "payload.rows"), [3, 3]);
    assert_eq!(
        counts(
            &selected.row(1).assured("the second selected row exists"),
            "payload.rows"
        ),
        [3]
    );
}

#[test]
fn ingest_payload_unread_counts_are_never_queried() {
    struct Unread;
    impl IngestMessageMetadata for Unread {
        fn payload_bytes(&self) -> usize {
            panic!("an unread count must not be queried")
        }
        fn visit_metadata(&self, _values: &mut dyn IngestMetadataValues) {
            panic!("metadata is unread too")
        }
    }
    let mut builders =
        IngestMetadataBuilders::new(projection(IngestorPayloadDeclaration::BYTES, &[]), 1);
    builders
        .append(&IngestMetadataRow::headers(&NoIngestHeaders, &Unread), None)
        .assured("an unprojected row visits neither value namespace");
    let metadata = builders
        .finish()
        .assured("a row without requested fields finishes");
    assert!(
        metadata
            .columns
            .integration_fields
            .schema()
            .fields()
            .is_empty()
    );
}
