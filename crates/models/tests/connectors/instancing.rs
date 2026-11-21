//! Current ingestor instancing declarations and their serialization boundaries.
//!
//! Layer: test harness.
//! - **Owns.** Count bounds, the transport-partition choice and complete JSON/archive preservation.
//! - **Depends on.** The vocabulary's instancing declaration and its production encodings.
//! - **Must not know.** Placement allocation, connector drivers or runtime state.

use std::num::NonZeroU64;

use meticulous::ResultExt as _;
use nervix_models::{
    IngestorEntityKind, IngestorExternalPartitions, IngestorInstances,
    IngestorInstancingCapability, OptionDeclaration,
};

#[test]
fn instancing_declaration_keeps_eight_byte_native_and_archive_layout() {
    assert_eq!(std::mem::size_of::<IngestorInstances>(), 8);
    assert_eq!(std::mem::size_of::<rkyv::Archived<IngestorInstances>>(), 8);
    assert_eq!(std::mem::align_of::<rkyv::Archived<IngestorInstances>>(), 8);
    for declaration in [
        IngestorInstances::ONE,
        IngestorInstances::Count(NonZeroU64::MAX),
        IngestorInstances::PerTransportPartition,
    ] {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&declaration)
            .assured("every current instancing declaration has an archive encoding");
        assert_eq!(bytes.len(), 8);
        assert_eq!(
            rkyv::from_bytes::<IngestorInstances, rkyv::rancor::Error>(&bytes)
                .assured("a current instancing declaration decodes"),
            declaration,
        );
    }
}

#[test]
fn bolero_instancing_preserves_its_count_or_partition_form() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(8)
        .for_each(|bytes: &[u8]| {
            let count = u64::from_le_bytes(std::array::from_fn(|index| {
                bytes.get(index).copied().unwrap_or(0)
            }));
            let count = NonZeroU64::new(count).unwrap_or(NonZeroU64::MIN);
            for declaration in [
                IngestorInstances::Count(count),
                IngestorInstances::Count(NonZeroU64::MIN),
                IngestorInstances::Count(NonZeroU64::MAX),
                IngestorInstances::PerTransportPartition,
            ] {
                let json =
                    serde_json::to_vec(&declaration).expect("the current declaration serializes");
                assert_eq!(
                    serde_json::from_slice::<IngestorInstances>(&json)
                        .expect("the current JSON decodes"),
                    declaration
                );
                let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&declaration)
                    .expect("the current declaration archives");
                assert_eq!(
                    rkyv::from_bytes::<IngestorInstances, rkyv::rancor::Error>(&archive)
                        .expect("the current archive decodes"),
                    declaration
                );
            }
        });
}

#[test]
fn instancing_count_preserves_the_complete_positive_u64_range() {
    for count in [1_u64, 65_536, u64::from(u32::MAX) + 1, u64::MAX] {
        let json = format!("{{\"Count\":{count}}}");
        let declaration = serde_json::from_str::<IngestorInstances>(&json)
            .expect("every positive full-width instance count is a current declaration");
        assert_eq!(declaration.to_string(), count.to_string());
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&declaration)
            .expect("the current declaration archives");
        assert_eq!(
            rkyv::from_bytes::<IngestorInstances, rkyv::rancor::Error>(&archive)
                .expect("the current declaration decodes"),
            declaration,
        );
    }
}

/// Only a transport that keeps partitions, which Nervix observes, has partitions to follow: an
/// entity offers `INSTANCES PER TRANSPORT PARTITION` exactly when it declares an enumerable external
/// partition set, and its `INSTANCES` option offers exactly the forms its capability permits.
#[test]
fn only_entities_with_transport_partitions_offer_the_transport_partition_form() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        let enumerated = matches!(
            declared.external_partitions,
            IngestorExternalPartitions::Enumerated(_)
        );
        let option = declared.option(OptionDeclaration::INSTANCES.keywords);
        match declared.instancing {
            IngestorInstancingCapability::Single => {
                assert!(
                    option.is_none(),
                    "{kind} runs one instance yet offers INSTANCES"
                );
            }
            IngestorInstancingCapability::Instances => {
                assert!(
                    !enumerated,
                    "{kind} has transport partitions it cannot follow"
                );
                assert_eq!(
                    option.map(|option| option.form),
                    Some(OptionDeclaration::INSTANCES.form),
                    "{kind} offers a count only"
                );
            }
            IngestorInstancingCapability::PerTransportPartition => {
                assert!(
                    enumerated,
                    "{kind} follows partitions its transport does not keep"
                );
                assert_eq!(
                    option.map(|option| option.form),
                    Some(OptionDeclaration::TRANSPORT_PARTITION_INSTANCES.form),
                    "{kind} offers a count and the transport-partition form"
                );
            }
        }
        assert!(declared.instancing.permits(IngestorInstances::ONE) || option.is_none());
        assert_eq!(
            declared
                .instancing
                .permits(IngestorInstances::PerTransportPartition),
            declared.instancing == IngestorInstancingCapability::PerTransportPartition,
            "{kind}"
        );
    }
}
