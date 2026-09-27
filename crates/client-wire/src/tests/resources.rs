//! Resource descriptions: every shape round trips, including a missing latest version beside
//! published versions, and a description that names no valid version, repeats a version or
//! carries an invalid name is refused.

use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use nervix_models::{
    ModelKind, ModelName, NodeRef, ResourceDescription, ResourceEntryContent,
    ResourceManifestEntry, ResourceUsage, ResourceVersionDescription, ResourceVersionEntries,
    Timestamp,
};

use super::{
    fixtures::{decode_error, finish_reply, name, non_zero, raw_server, request, round_trip_reply},
    samples::command_outcome,
};
use crate::{CommandDisposition, Reply, ReplyBody, ServerMessage, WireDecodeError, wire};

fn version(number: u64, entries: ResourceVersionEntries) -> ResourceVersionDescription {
    ResourceVersionDescription {
        version: non_zero(number),
        root_checksum: format!("root-{number}"),
        manifest_checksum: format!("manifest-{number}"),
        file_count: 2,
        total_bytes: 5,
        created_at: Timestamp::from_unix_nanos(1_789_000_000_123_456_789),
        created_by_node: name("node-1"),
        entries,
    }
}

fn described(description: ResourceDescription) -> Reply {
    let mut outcome = command_outcome(CommandDisposition::Completed {
        already_existed: false,
    });
    outcome.resource = Some(Box::new(description));
    Reply {
        request_id: request(1),
        body: ReplyBody::Command(Box::new(outcome)),
    }
}

#[test]
fn a_resource_description_round_trips_its_versions_entries_and_usages() {
    let entries = ResourceVersionEntries::Listed(vec![
        ResourceManifestEntry {
            path: "release notes.txt".to_string(),
            content: ResourceEntryContent::File {
                size: 5,
                checksum: "a1".to_string(),
            },
        },
        ResourceManifestEntry {
            path: "user guides".to_string(),
            content: ResourceEntryContent::Directory,
        },
        ResourceManifestEntry {
            path: "user guides/empty.md".to_string(),
            content: ResourceEntryContent::File {
                size: 0,
                checksum: "b2".to_string(),
            },
        },
    ]);
    let descriptions = [
        ResourceDescription {
            resource: name("bundle"),
            latest_version: None,
            versions: Vec::new(),
            usages: Vec::new(),
        },
        ResourceDescription {
            resource: name("bundle"),
            latest_version: None,
            versions: vec![version(1, ResourceVersionEntries::Listed(Vec::new()))],
            usages: Vec::new(),
        },
        ResourceDescription {
            resource: name("bundle"),
            latest_version: Some(non_zero(2)),
            versions: vec![
                version(1, entries),
                version(2, ResourceVersionEntries::Listed(Vec::new())),
                version(
                    u64::MAX,
                    ResourceVersionEntries::Unavailable {
                        reason: "the manifest could not be read".to_string(),
                    },
                ),
            ],
            usages: vec![
                ResourceUsage {
                    node: NodeRef::new(ModelKind::Client, name::<ModelName>("store")),
                    version: non_zero(1),
                },
                ResourceUsage {
                    node: NodeRef::new(ModelKind::Lookup, name::<ModelName>("by_id")),
                    version: non_zero(2),
                },
            ],
        },
    ];
    for description in descriptions {
        let original = described(description);
        assert_eq!(round_trip_reply(&original), original);
    }
}

/// The values of a hand-built description a test varies.
#[derive(Clone, Copy)]
struct RawDescription<'a> {
    resource: &'a str,
    latest_version: Option<u64>,
    versions: &'a [u64],
    created_by_node: &'a str,
    usage_version: u64,
}

const VALID: RawDescription<'static> = RawDescription {
    resource: "bundle",
    latest_version: Some(2),
    versions: &[1, 2],
    created_by_node: "node-1",
    usage_version: 1,
};

/// A completed command reply carrying a description built from `raw`, for descriptions the typed
/// encoder cannot produce.
fn raw_description(raw: RawDescription<'_>) -> Bytes {
    let mut builder = FlatBufferBuilder::new();
    let mut versions = Vec::new();
    for number in raw.versions {
        let root_checksum = builder.create_string("root");
        let manifest_checksum = builder.create_string("manifest");
        let created_by_node = builder.create_string(raw.created_by_node);
        let entries = builder.create_vector::<WIPOffset<wire::ResourceEntry>>(&[]);
        let listed = wire::ResourceEntriesListed::create(
            &mut builder,
            &wire::ResourceEntriesListedArgs {
                entries: Some(entries),
            },
        );
        versions.push(wire::ResourceVersionDescription::create(
            &mut builder,
            &wire::ResourceVersionDescriptionArgs {
                version: *number,
                root_checksum: Some(root_checksum),
                manifest_checksum: Some(manifest_checksum),
                file_count: 0,
                total_bytes: 0,
                created_at: 0,
                created_by_node: Some(created_by_node),
                entries_type: wire::ResourceVersionEntries::ResourceEntriesListed,
                entries: Some(listed.as_union_value()),
            },
        ));
    }
    let versions = builder.create_vector(&versions);
    let usage_name = builder.create_string("store");
    let node = wire::NodeRef::create(
        &mut builder,
        &wire::NodeRefArgs {
            kind: Some(wire::ModelKind::Client),
            name: Some(usage_name),
        },
    );
    let usage = wire::ResourceUsage::create(
        &mut builder,
        &wire::ResourceUsageArgs {
            node: Some(node),
            version: raw.usage_version,
        },
    );
    let usages = builder.create_vector(&[usage]);
    let resource = builder.create_string(raw.resource);
    let description = wire::ResourceDescription::create(
        &mut builder,
        &wire::ResourceDescriptionArgs {
            resource: Some(resource),
            latest_version: raw.latest_version,
            versions: Some(versions),
            usages: Some(usages),
        },
    );
    let execution_reference = builder.create_string("0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44");
    let completed = wire::CommandCompleted::create(
        &mut builder,
        &wire::CommandCompletedArgs {
            already_existed: false,
        },
    );
    let message = builder.create_string("");
    let diagnostics = builder.create_vector::<WIPOffset<wire::Diagnostic>>(&[]);
    let statements = builder.create_vector::<WIPOffset<wire::StatementOutcome>>(&[]);
    let outcome = wire::CommandOutcome::create(
        &mut builder,
        &wire::CommandOutcomeArgs {
            execution_reference: Some(execution_reference),
            origin: Some(wire::OutcomeOrigin::Executed),
            disposition_type: wire::CommandDisposition::CommandCompleted,
            disposition: Some(completed.as_union_value()),
            message: Some(message),
            diagnostics: Some(diagnostics),
            statements: Some(statements),
            transaction: None,
            transaction_admission: None,
            inspection: None,
            wasm_state: None,
            resource: Some(description),
        },
    );
    finish_reply(
        builder,
        wire::ReplyBody::CommandOutcome,
        outcome.as_union_value(),
    )
}

#[test]
fn a_resource_description_without_valid_versions_or_names_is_refused() {
    let frame = raw_server(raw_description(VALID));
    let Ok(ServerMessage::Reply(reply)) = ServerMessage::decode(&frame) else {
        panic!("the valid hand-built description must decode as a reply");
    };
    let ReplyBody::Command(outcome) = reply.body else {
        panic!("the valid hand-built description must decode as a command outcome");
    };
    let Some(description) = outcome.resource else {
        panic!("the valid hand-built command outcome must carry its description");
    };
    assert_eq!(description.latest_version, Some(non_zero(2)));

    let ascending = WireDecodeError::InvalidValue {
        field: "ResourceDescription.versions",
        kind: "strictly ascending versions",
    };
    let refusals = [
        (
            RawDescription {
                versions: &[0, 2],
                ..VALID
            },
            WireDecodeError::ZeroValue {
                field: "ResourceVersionDescription.version",
            },
        ),
        (
            RawDescription {
                latest_version: Some(0),
                ..VALID
            },
            WireDecodeError::ZeroValue {
                field: "ResourceDescription.latest_version",
            },
        ),
        (
            RawDescription {
                usage_version: 0,
                ..VALID
            },
            WireDecodeError::ZeroValue {
                field: "ResourceUsage.version",
            },
        ),
        (
            RawDescription {
                versions: &[2, 1],
                ..VALID
            },
            ascending.clone(),
        ),
        (
            RawDescription {
                versions: &[1, 1],
                ..VALID
            },
            ascending,
        ),
        (
            RawDescription {
                resource: "not a name",
                ..VALID
            },
            WireDecodeError::InvalidValue {
                field: "ResourceDescription.resource",
                kind: "name",
            },
        ),
        (
            RawDescription {
                created_by_node: "",
                ..VALID
            },
            WireDecodeError::InvalidValue {
                field: "ResourceVersionDescription.created_by_node",
                kind: "name",
            },
        ),
    ];
    for (raw, expected) in refusals {
        let frame = raw_server(raw_description(raw));
        assert_eq!(decode_error(ServerMessage::decode(&frame)), expected);
    }
}
