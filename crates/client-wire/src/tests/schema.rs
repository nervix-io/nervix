//! The schema files other languages generate code from agree with the Rust frame roots.

use crate::{ClientFrame, FrameRoot, ServerFrame, UploadFrame, UploadReplyFrame};

/// The root table and file identifier a root schema file declares.
struct DeclaredRoot {
    table: String,
    identifier: String,
}

fn declared_root(schema: &str) -> DeclaredRoot {
    let mut table = None;
    let mut identifier = None;
    for line in schema.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("root_type ") {
            table = Some(rest.trim_end_matches(';').to_string());
        }
        if let Some(rest) = line.strip_prefix("file_identifier ") {
            identifier = Some(rest.trim_end_matches(';').trim_matches('"').to_string());
        }
    }
    match (table, identifier) {
        (Some(table), Some(identifier)) => DeclaredRoot { table, identifier },
        _ => panic!("a root schema file declares its root type and file identifier"),
    }
}

fn assert_root<R: FrameRoot>(schema: &str) {
    let declared = declared_root(schema);
    assert_eq!(declared.table, R::NAME);
    assert_eq!(declared.identifier, R::IDENTIFIER);
    assert!(
        schema.contains("include \"session.fbs\";"),
        "a root file declares no shapes of its own"
    );
}

#[test]
fn root_schema_files_declare_the_frame_roots() {
    assert_root::<ClientFrame>(include_str!("../../schema/client_message.fbs"));
    assert_root::<ServerFrame>(include_str!("../../schema/server_message.fbs"));
    assert_root::<UploadFrame>(include_str!("../../schema/upload_message.fbs"));
    assert_root::<UploadReplyFrame>(include_str!("../../schema/upload_reply.fbs"));
}

#[test]
fn every_frame_root_has_a_distinct_identifier() {
    let identifiers = [
        ClientFrame::IDENTIFIER,
        ServerFrame::IDENTIFIER,
        UploadFrame::IDENTIFIER,
        UploadReplyFrame::IDENTIFIER,
    ];
    for (index, identifier) in identifiers.iter().enumerate() {
        assert_eq!(identifier.len(), 4);
        assert!(!identifiers[index + 1..].contains(identifier));
    }
}
