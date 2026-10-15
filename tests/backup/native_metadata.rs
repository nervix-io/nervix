//! Steps that back up native branch lifecycle and Kafka offset metadata above the bulk budget,
//! interrupt that capture, measure it, and compare the native values two archives hold.
//!
//! Layer: test harness.
//! - **Owns.** The native metadata capture interruption a scenario arms, measured CLI backups, and
//!   the comparison of every archived lifecycle branch and Kafka partition offset.
//! - **Depends on.** The public CLI, the archive format's records and reader, the shared memory
//!   sampler and the fault injection a scenario arms.
//! - **Must not know.** How an owner reads, converts or stages a native checkpoint.

use nervix_backup::{
    ArchiveRecord as _, BranchLifecycleEntry, BranchLifecycleRecord, KafkaOffsetsRecord,
    KafkaPartitionOffset,
};
use nervix_models::{ModelName, SchemaFingerprint};

use super::{memory::MemorySampler, restore::copy_of_archive, *};

#[given(
    expr = "the next native metadata capture of {string} in domain {string} is interrupted after \
            {int} entries"
)]
fn given_native_metadata_capture_is_interrupted(
    world: &mut ScenarioWorld,
    entity: String,
    domain: String,
    entries: u64,
) {
    let domain = scenario_domain(world, &domain);
    let entity = ModelName::parse(&entity).assured("the scenario names a valid entity");
    world
        .fault_injection
        .interrupt_native_metadata_capture(domain, entity, entries);
}

/// Samples every node's public executor gauges only while the public CLI backs up.
#[when(
    expr = "the CLI backs up {string} from node {string} into {string} reporting JSON with memory \
            measurements"
)]
async fn when_cli_backs_up_with_memory_measurements(
    world: &mut ScenarioWorld,
    scope: String,
    node: String,
    file: String,
) {
    let sampler = MemorySampler::start(world).await;
    let began = Instant::now();
    when_cli_backs_up(world, scope, node, file.clone()).await;
    let elapsed = began.elapsed();
    let sampled = sampler.finish().await;
    let archive_bytes = match std::fs::metadata(archive_path(world, &file)) {
        Ok(metadata) => metadata.len(),
        Err(_) => 0,
    };
    eprintln!(
        "backup capture measurement: {}",
        sampled.evidence(world, archive_bytes, elapsed)
    );
    world.backup_memory = Some(sampled);
}

#[then("the measured backup kept every node within its bulk budget without a bulk refusal")]
fn then_measured_backup_kept_the_bulk_budget(world: &mut ScenarioWorld) {
    let sampled = world
        .backup_memory
        .take()
        .assured("a preceding step measured a CLI backup");
    sampled.assert_bulk_within_budget();
    sampled.assert_no_bulk_refusal("backup capture is admitted within the bulk budget");
}

/// The branch lifecycle of one entity as an archive records it, apart from the domain and the
/// revision of the cut that took it.
#[derive(Debug, PartialEq, Eq)]
struct ArchivedLifecycle {
    schema: SchemaFingerprint,
    branches: Vec<BranchLifecycleEntry>,
}

/// The Kafka offsets of one ingestor as an archive records them, apart from the domain and the
/// revision of the cut that took them.
#[derive(Debug, PartialEq, Eq)]
struct ArchivedOffsets {
    schema: SchemaFingerprint,
    offsets: Vec<KafkaPartitionOffset>,
}

/// One entity's lifecycle, named by its owner kind and entity.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct LifecycleOwner {
    kind: String,
    entity: ModelName,
}

/// Every native lifecycle and Kafka offset value an archive holds.
struct NativeMetadata {
    lifecycles: BTreeMap<LifecycleOwner, ArchivedLifecycle>,
    offsets: BTreeMap<ModelName, ArchivedOffsets>,
}

impl NativeMetadata {
    /// Asserts that this archive holds every lifecycle and offset value `expected` holds, in order.
    fn assert_holds_every_value_of(&self, expected: &Self) {
        let branches: usize = expected
            .lifecycles
            .values()
            .map(|lifecycle| lifecycle.branches.len())
            .sum();
        let partitions: usize = expected
            .offsets
            .values()
            .map(|offsets| offsets.offsets.len())
            .sum();
        assert!(
            branches > 1024 && partitions > 350_000,
            "the expected archive holds the enlarged native metadata: {branches} branches and \
             {partitions} partitions"
        );
        assert_eq!(
            self.lifecycles.keys().collect::<Vec<_>>(),
            expected.lifecycles.keys().collect::<Vec<_>>(),
            "both archives hold the lifecycles of the same entities"
        );
        for (owner, lifecycle) in &expected.lifecycles {
            let archived = &self.lifecycles[owner];
            assert_eq!(archived.schema, lifecycle.schema, "{owner:?}");
            assert_eq!(
                archived.branches.len(),
                lifecycle.branches.len(),
                "{owner:?}"
            );
            for (index, (archived, branch)) in archived
                .branches
                .iter()
                .zip(&lifecycle.branches)
                .enumerate()
            {
                assert_eq!(
                    archived, branch,
                    "branch {index} of {owner:?}, in LRU order"
                );
            }
        }
        assert_eq!(
            self.offsets.keys().collect::<Vec<_>>(),
            expected.offsets.keys().collect::<Vec<_>>(),
            "both archives hold the offsets of the same ingestors"
        );
        for (entity, offsets) in &expected.offsets {
            let archived = &self.offsets[entity];
            assert_eq!(archived.schema, offsets.schema, "{entity}");
            assert_eq!(archived.offsets.len(), offsets.offsets.len(), "{entity}");
            for (archived, offset) in archived.offsets.iter().zip(&offsets.offsets) {
                assert_eq!(archived, offset, "the offsets of ingestor {entity}");
            }
        }
    }

    fn read(path: &Path) -> Self {
        let archive = copy_of_archive(path);
        let mut metadata = Self {
            lifecycles: BTreeMap::new(),
            offsets: BTreeMap::new(),
        };
        for (path, bytes) in &archive.sections {
            if path.contains("/state/branch_lifecycle/") {
                let record = BranchLifecycleRecord::decode(path, bytes)
                    .assured("an archived lifecycle verifies");
                metadata.insert_lifecycle(record);
            } else if path.contains("/state/kafka_offset/") {
                let record = KafkaOffsetsRecord::decode(path, bytes)
                    .assured("archived Kafka offsets verify");
                let previous = metadata.offsets.insert(
                    record.entity,
                    ArchivedOffsets {
                        schema: record.schema,
                        offsets: record.offsets,
                    },
                );
                assert!(previous.is_none(), "an ingestor has one offsets section");
            }
        }
        metadata
    }

    fn insert_lifecycle(&mut self, record: BranchLifecycleRecord) {
        let owner = LifecycleOwner {
            kind: record.owner_kind.as_str().to_string(),
            entity: record.entity,
        };
        let previous = self.lifecycles.insert(
            owner,
            ArchivedLifecycle {
                schema: record.schema,
                branches: record.branches,
            },
        );
        assert!(previous.is_none(), "an entity has one lifecycle section");
    }
}

#[then(
    expr = "backup archive {string} holds every branch lifecycle and Kafka offset of backup \
            archive {string}"
)]
async fn then_archive_holds_the_native_metadata(
    world: &mut ScenarioWorld,
    actual: String,
    expected: String,
) {
    let actual = archive_path(world, &actual);
    let expected = archive_path(world, &expected);
    // Both archives hold records above 32 MiB, so they are read and compared on a blocking task.
    off_the_runner(move || {
        let actual = NativeMetadata::read(&actual);
        let expected = NativeMetadata::read(&expected);
        actual.assert_holds_every_value_of(&expected);
    })
    .await;
}
