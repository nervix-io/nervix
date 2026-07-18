include!(concat!(env!("OUT_DIR"), "/io.nervix.api.v1.rs"));

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{DomainSnapshot, UploadResourceRequest, upload_resource_request};

    fn assert_bytes(_: &prost::bytes::Bytes) {}

    #[test]
    fn protobuf_bytes_fields_use_shared_bytes() {
        let snapshot = DomainSnapshot::default();
        assert_bytes(&snapshot.dataflow_graph);

        let upload = UploadResourceRequest {
            event: Some(upload_resource_request::Event::Chunk(Default::default())),
        };
        let Some(upload_resource_request::Event::Chunk(chunk)) = upload.event else {
            panic!("expected upload chunk");
        };
        assert_bytes(&chunk);
    }

    #[test]
    fn protobuf_bytes_fields_alias_the_decode_buffer() {
        let snapshot = DomainSnapshot {
            dataflow_graph: prost::bytes::Bytes::from_static(b"arrow ipc payload"),
            ..Default::default()
        };
        let encoded = prost::bytes::Bytes::from(snapshot.encode_to_vec());
        let encoded_start = encoded.as_ptr() as usize;
        let encoded_end = encoded_start + encoded.len();

        let decoded = DomainSnapshot::decode(encoded).expect("snapshot should decode");
        let field_start = decoded.dataflow_graph.as_ptr() as usize;
        let field_end = field_start + decoded.dataflow_graph.len();

        assert!(field_start >= encoded_start);
        assert!(field_end <= encoded_end);
    }
}
