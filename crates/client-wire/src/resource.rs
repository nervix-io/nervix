//! The typed description of a resource: its published versions, their entries, and the models
//! bound to each version.

use std::num::NonZeroU64;

use error_stack::Report;
use flatbuffers::WIPOffset;
use nervix_models::{
    ClusterNodeName, ResourceDescription, ResourceEntryContent, ResourceManifestEntry,
    ResourceName, ResourceUsage, ResourceVersionDescription, ResourceVersionEntries, Timestamp,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError},
    common::{decode_node_ref, encode_node_ref},
    wire,
};

pub(crate) fn encode_resource_description<'fbb>(
    encoder: &mut Encoder<'fbb>,
    description: &ResourceDescription,
) -> Result<WIPOffset<wire::ResourceDescription<'fbb>>, Report<WireEncodeError>> {
    let resource = encoder.text(
        "ResourceDescription.resource",
        description.resource.as_str(),
    )?;
    let versions = encoder.table_vector(
        "ResourceDescription.versions",
        &description.versions,
        encode_version,
    )?;
    let usages = encoder.table_vector(
        "ResourceDescription.usages",
        &description.usages,
        encode_usage,
    )?;
    Ok(wire::ResourceDescription::create(
        encoder.fbb(),
        &wire::ResourceDescriptionArgs {
            resource: Some(resource),
            latest_version: description.latest_version.map(NonZeroU64::get),
            versions: Some(versions),
            usages: Some(usages),
        },
    ))
}

/// Reads a resource description. Its versions must be in strictly ascending order, as the
/// schema requires, so no version is described twice.
pub(crate) fn decode_resource_description(
    decoder: Decoder<'_>,
    description: wire::ResourceDescription<'_>,
) -> Result<ResourceDescription, Report<WireDecodeError>> {
    let resource: ResourceName =
        decoder.name("ResourceDescription.resource", description.resource())?;
    let latest_version = match description.latest_version() {
        Some(version) => Some(decoder.non_zero("ResourceDescription.latest_version", version)?),
        None => None,
    };
    let versions = decoder.table_vector(
        "ResourceDescription.versions",
        description.versions(),
        |version| decode_version(decoder, version),
    )?;
    let mut previous = None;
    for version in &versions {
        if let Some(previous) = previous
            && previous >= version.version
        {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "ResourceDescription.versions",
                kind: "strictly ascending versions",
            }));
        }
        previous = Some(version.version);
    }
    let usages = decoder.table_vector(
        "ResourceDescription.usages",
        description.usages(),
        |usage| decode_usage(decoder, usage),
    )?;
    Ok(ResourceDescription {
        resource,
        latest_version,
        versions,
        usages,
    })
}

fn encode_version<'fbb>(
    version: &ResourceVersionDescription,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::ResourceVersionDescription<'fbb>>, Report<WireEncodeError>> {
    let root_checksum = encoder.text(
        "ResourceVersionDescription.root_checksum",
        &version.root_checksum,
    )?;
    let manifest_checksum = encoder.text(
        "ResourceVersionDescription.manifest_checksum",
        &version.manifest_checksum,
    )?;
    let created_by_node = encoder.text(
        "ResourceVersionDescription.created_by_node",
        version.created_by_node.as_str(),
    )?;
    let entries = encode_entries(encoder, &version.entries)?;
    Ok(wire::ResourceVersionDescription::create(
        encoder.fbb(),
        &wire::ResourceVersionDescriptionArgs {
            version: version.version.get(),
            root_checksum: Some(root_checksum),
            manifest_checksum: Some(manifest_checksum),
            file_count: version.file_count,
            total_bytes: version.total_bytes,
            created_at: version.created_at.unix_nanos(),
            created_by_node: Some(created_by_node),
            entries_type: entries.discriminant,
            entries: Some(entries.value),
        },
    ))
}

fn decode_version(
    decoder: Decoder<'_>,
    version: wire::ResourceVersionDescription<'_>,
) -> Result<ResourceVersionDescription, Report<WireDecodeError>> {
    let number = decoder.non_zero("ResourceVersionDescription.version", version.version())?;
    let root_checksum = decoder.text(
        "ResourceVersionDescription.root_checksum",
        version.root_checksum(),
    )?;
    let manifest_checksum = decoder.text(
        "ResourceVersionDescription.manifest_checksum",
        version.manifest_checksum(),
    )?;
    let created_by_node: ClusterNodeName = decoder.name(
        "ResourceVersionDescription.created_by_node",
        version.created_by_node(),
    )?;
    let entries = decode_entries(decoder, version)?;
    Ok(ResourceVersionDescription {
        version: number,
        root_checksum,
        manifest_checksum,
        file_count: version.file_count(),
        total_bytes: version.total_bytes(),
        created_at: Timestamp::from_unix_nanos(version.created_at()),
        created_by_node,
        entries,
    })
}

fn encode_entries(
    encoder: &mut Encoder<'_>,
    entries: &ResourceVersionEntries,
) -> Result<EncodedUnion<wire::ResourceVersionEntries>, Report<WireEncodeError>> {
    match entries {
        ResourceVersionEntries::Listed(entries) => {
            let entries =
                encoder.table_vector("ResourceEntriesListed.entries", entries, encode_entry)?;
            let listed = wire::ResourceEntriesListed::create(
                encoder.fbb(),
                &wire::ResourceEntriesListedArgs {
                    entries: Some(entries),
                },
            );
            Ok(EncodedUnion::new(
                wire::ResourceVersionEntries::ResourceEntriesListed,
                listed,
            ))
        }
        ResourceVersionEntries::Unavailable { reason } => {
            let reason = encoder.text("ResourceEntriesUnavailable.reason", reason)?;
            let unavailable = wire::ResourceEntriesUnavailable::create(
                encoder.fbb(),
                &wire::ResourceEntriesUnavailableArgs {
                    reason: Some(reason),
                },
            );
            Ok(EncodedUnion::new(
                wire::ResourceVersionEntries::ResourceEntriesUnavailable,
                unavailable,
            ))
        }
    }
}

fn decode_entries(
    decoder: Decoder<'_>,
    version: wire::ResourceVersionDescription<'_>,
) -> Result<ResourceVersionEntries, Report<WireDecodeError>> {
    if let Some(listed) = version.entries_as_resource_entries_listed() {
        let entries =
            decoder.table_vector("ResourceEntriesListed.entries", listed.entries(), |entry| {
                decode_entry(decoder, entry)
            })?;
        return Ok(ResourceVersionEntries::Listed(entries));
    }
    if let Some(unavailable) = version.entries_as_resource_entries_unavailable() {
        let reason = decoder.text("ResourceEntriesUnavailable.reason", unavailable.reason())?;
        return Ok(ResourceVersionEntries::Unavailable { reason });
    }
    Err(decoder.unknown_union(
        "ResourceVersionDescription.entries",
        version.entries_type().0,
    ))
}

fn encode_entry<'fbb>(
    entry: &ResourceManifestEntry,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::ResourceEntry<'fbb>>, Report<WireEncodeError>> {
    let path = encoder.text("ResourceEntry.path", &entry.path)?;
    let content = match &entry.content {
        ResourceEntryContent::Directory => EncodedUnion::new(
            wire::ResourceEntryContent::ResourceDirectory,
            wire::ResourceDirectory::create(encoder.fbb(), &wire::ResourceDirectoryArgs {}),
        ),
        ResourceEntryContent::File { size, checksum } => {
            let checksum = encoder.text("ResourceFile.checksum", checksum)?;
            let file = wire::ResourceFile::create(
                encoder.fbb(),
                &wire::ResourceFileArgs {
                    size: *size,
                    checksum: Some(checksum),
                },
            );
            EncodedUnion::new(wire::ResourceEntryContent::ResourceFile, file)
        }
    };
    Ok(wire::ResourceEntry::create(
        encoder.fbb(),
        &wire::ResourceEntryArgs {
            path: Some(path),
            content_type: content.discriminant,
            content: Some(content.value),
        },
    ))
}

fn decode_entry(
    decoder: Decoder<'_>,
    entry: wire::ResourceEntry<'_>,
) -> Result<ResourceManifestEntry, Report<WireDecodeError>> {
    let path = decoder.text("ResourceEntry.path", entry.path())?;
    let content = if entry.content_as_resource_directory().is_some() {
        ResourceEntryContent::Directory
    } else if let Some(file) = entry.content_as_resource_file() {
        let checksum = decoder.text("ResourceFile.checksum", file.checksum())?;
        ResourceEntryContent::File {
            size: file.size(),
            checksum,
        }
    } else {
        return Err(decoder.unknown_union("ResourceEntry.content", entry.content_type().0));
    };
    Ok(ResourceManifestEntry { path, content })
}

fn encode_usage<'fbb>(
    usage: &ResourceUsage,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::ResourceUsage<'fbb>>, Report<WireEncodeError>> {
    let node = encode_node_ref(encoder, &usage.node)?;
    Ok(wire::ResourceUsage::create(
        encoder.fbb(),
        &wire::ResourceUsageArgs {
            node: Some(node),
            version: usage.version.get(),
        },
    ))
}

fn decode_usage(
    decoder: Decoder<'_>,
    usage: wire::ResourceUsage<'_>,
) -> Result<ResourceUsage, Report<WireDecodeError>> {
    let node = decode_node_ref(decoder, usage.node())?;
    let version = decoder.non_zero("ResourceUsage.version", usage.version())?;
    Ok(ResourceUsage { node, version })
}
