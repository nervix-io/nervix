//! The vocabulary records the control plane stores beside its Models: domains with their clock
//! mappings and authority, domain schedules, the resource catalog, and the identities of cluster
//! nodes, commands and uploads.
//!
//! Each value is built through the constructors and transitions production uses, so a storage
//! property compares what a node actually writes. A name that keys a stored record is drawn from
//! the whole name rule, delimiter-only spellings included, because a stored key holds it as text.

use std::convert::Infallible;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    BranchKeyFingerprint, ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName,
    CommandExecutionReference, DomainClockAuthority, DomainClockAuthorityRevision,
    DomainClockState, DomainConfig, DomainName, DomainPace, DomainSchedule, DomainStartPoint,
    DomainState, DomainStatus, KafkaPartitionSchedule, Model, ModelName, NodeRef,
    OwnershipStateComponent, OwnershipStateRecoveryOutcome, OwnershipStateReset,
    OwnershipStateResetCause, OwnershipTransition, PlacementGroupSchedule,
    RequestedResourceVersion, ResolvedBranching, ResourceId, ResourceNodeState, ResourceNodeStatus,
    ResourceReplicaKey, ResourceUpload, ResourceUploadIdentity, ResourceUploadKey,
    ResourceUploadState, ResourceVersion, RestoreStateAuthority, ScheduledNode, SchemaFingerprint,
    Timestamp, WasmSavedStateRejection, WasmStateRecoveryAdmission, WasmStateRecoveryOutcome,
    WasmStateResetReason, WasmStateResetScope,
};

use crate::Arbitrary;

/// How many entries a generated collection of stored records holds at most.
const RECORDS: usize = 3;

/// The characters a command execution reference or a resource upload identity holds, case kept.
const IDENTITY_CHARACTERS: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-";

/// The longest command execution reference or resource upload identity, in bytes.
const IDENTITY_BYTES: u64 = 128;

/// Nanoseconds in one millisecond, the unit a UUIDv7 retry identity records its issue time in.
const NANOS_PER_MILLISECOND: u64 = 1_000_000;

/// The latest millisecond a UUIDv7 retry identity may carry while its issue time still fits the
/// signed Unix-nanosecond timestamp it is read back as.
const LATEST_RETRY_MILLISECOND: u64 = i64::MAX.unsigned_abs() / NANOS_PER_MILLISECOND;

/// The random bits below a UUIDv7's version nibble.
const UUID_RANDOM_A: u64 = 0x0fff;

/// The random bits below a UUIDv7's variant bits.
const UUID_RANDOM_B: u64 = 0x3fff_ffff_ffff_ffff;

impl Arbitrary<'_> {
    /// Up to three values, each built by `record`, in the order they were built.
    pub fn records<T>(&mut self, mut record: impl FnMut(&mut Self) -> T) -> Vec<T> {
        let count = self.entropy.count(RECORDS);
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            records.push(record(self));
        }
        records
    }

    /// Any instant a signed Unix-nanosecond timestamp holds, landing on the extremes as often as
    /// elsewhere.
    pub fn timestamp(&mut self) -> Timestamp {
        Timestamp::from_unix_nanos(self.entropy.any_i64())
    }

    /// A 32-byte digest: all zeros, all ones, one repeated byte, or 32 arbitrary bytes. A digest is
    /// accepted for its identity whatever its bytes, so the constant ones are as valid as any.
    pub fn digest(&mut self) -> [u8; 32] {
        match self.entropy.byte() % 4 {
            0 => [0; 32],
            1 => [u8::MAX; 32],
            2 => [self.entropy.byte(); 32],
            _ => {
                let mut digest = [0; 32];
                for byte in &mut digest {
                    *byte = self.entropy.byte();
                }
                digest
            }
        }
    }

    /// The fingerprint of the schemas a scheduled node's records are laid out by.
    pub fn schema_fingerprint(&mut self) -> SchemaFingerprint {
        SchemaFingerprint::from_digest(self.digest())
    }

    /// The fingerprint naming one concrete branch.
    pub fn branch_fingerprint(&mut self) -> BranchKeyFingerprint {
        BranchKeyFingerprint::new(self.digest())
    }

    /// One process incarnation of a cluster node.
    pub fn incarnation(&mut self) -> ClusterNodeIncarnation {
        ClusterNodeIncarnation::new(self.entropy.any_u64())
    }

    /// One process incarnation of a named cluster node.
    pub fn node_identity(&mut self) -> ClusterNodeIdentity {
        let node = self.rule_name::<ClusterNodeName>();
        let incarnation = self.incarnation();
        ClusterNodeIdentity::new(node, incarnation)
    }

    /// A cluster node's name, or none.
    pub fn optional_cluster_node(&mut self) -> Option<ClusterNodeName> {
        if self.entropy.flag() {
            Some(self.rule_name())
        } else {
            None
        }
    }

    /// Text of 1 through 128 bytes of ASCII letters, digits, `.`, `_` and `-`: the rule command
    /// execution references and resource upload identities share.
    pub fn identity_text(&mut self) -> String {
        let characters =
            u64::try_from(IDENTITY_CHARACTERS.len()).assured("a small table fits in u64");
        let last = characters
            .checked_sub(1)
            .assured("the identity characters are not empty");
        let length = self.entropy.boundary_biased(1..=IDENTITY_BYTES);
        let mut text = String::new();
        for _ in 0..length {
            let chosen = self.entropy.up_to(last);
            let chosen = usize::try_from(chosen).verified("an index below the table length");
            text.push(char::from(IDENTITY_CHARACTERS[chosen]));
        }
        text
    }

    /// A command execution reference: any text the reference rule admits, or a UUIDv7 retry
    /// identity as a client mints one.
    pub fn execution_reference(&mut self) -> CommandExecutionReference {
        if self.entropy.flag() {
            return self.retry_reference();
        }
        let text = self.identity_text();
        CommandExecutionReference::parse(text).assured("the text follows the reference rule")
    }

    /// A UUIDv7 retry identity whose issue time lies anywhere a timestamp holds, with arbitrary
    /// random bits. Durable command admission reads that issue time back, so only an identity of
    /// this shape enters the retry ledger.
    pub fn retry_reference(&mut self) -> CommandExecutionReference {
        let text = self.uuid_v7_text(LATEST_RETRY_MILLISECOND);
        CommandExecutionReference::parse(text)
            .assured("a UUID's text holds only hexadecimal digits and hyphens")
    }

    /// The lowercase text of a UUIDv7 whose millisecond is at most `latest_millisecond`, with any
    /// random bits below its version and variant bits.
    pub(crate) fn uuid_v7_text(&mut self, latest_millisecond: u64) -> String {
        let millisecond = self.entropy.boundary_biased(0..=latest_millisecond);
        let random_a = self.entropy.up_to(UUID_RANDOM_A);
        let random_b = self.entropy.up_to(UUID_RANDOM_B);
        format!(
            "{:08x}-{:04x}-7{:03x}-{:04x}-{:012x}",
            millisecond >> 16,
            millisecond & 0xffff,
            random_a,
            0x8000 | (random_b >> 48),
            random_b & 0xffff_ffff_ffff,
        )
    }

    /// The identity a client chose for one administrative upload attempt.
    pub fn upload_identity(&mut self) -> ResourceUploadIdentity {
        let text = self.identity_text();
        ResourceUploadIdentity::parse(text).assured("the text follows the upload identity rule")
    }

    /// A domain's pace: paced with any period and skew, or unpaced.
    pub fn domain_pace(&mut self) -> DomainPace {
        if !self.entropy.flag() {
            return DomainPace::Unpaced;
        }
        let period = self.clock_period();
        let skew = self.clock_skew();
        DomainPace::Paced { period, skew }
    }

    /// Where a domain starts: where it stopped, now, or at a requested logical instant, with any
    /// positive finite time rate.
    pub fn start_point(&mut self) -> DomainStartPoint {
        match self.entropy.byte() % 3 {
            0 => DomainStartPoint::Resume,
            1 => DomainStartPoint::Now {
                time_rate: self.time_rate(),
            },
            _ => DomainStartPoint::At {
                timestamp: Timestamp::from_unix_nanos(self.entropy.any_i64()),
                time_rate: self.time_rate(),
            },
        }
    }

    /// The mapping a running paced generation projects logical time through.
    pub fn domain_clock_state(&mut self) -> DomainClockState {
        let wall_started_at = self.timestamp();
        let logical_start = self.timestamp();
        let time_rate = self.time_rate();
        DomainClockState::new(wall_started_at, logical_start, time_rate)
    }

    /// The revision of a domain clock's authority fence.
    ///
    /// The vocabulary has no constructor for an arbitrary revision on purpose: a cluster only ever
    /// advances one from the initial revision. A stored revision is the archived `u64` it holds, so
    /// one is read back from that archive here.
    pub fn authority_revision(&mut self) -> DomainClockAuthorityRevision {
        let value = self.entropy.any_u64();
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&value)
            .assured("a u64 archives into eight bytes");
        rkyv::from_bytes::<DomainClockAuthorityRevision, rkyv::rancor::Error>(&archived)
            .assured("a revision archives as the u64 it holds")
    }

    /// A domain clock's authority fence, unassigned or held by one node process.
    pub fn domain_clock_authority(&mut self) -> DomainClockAuthority {
        let revision = self.authority_revision();
        if !self.entropy.flag() {
            return DomainClockAuthority::unassigned(revision);
        }
        let owner = self.node_identity();
        DomainClockAuthority::assigned(revision, owner)
    }

    /// The replicated state of the domain named `id`: its configuration, lifecycle and generation,
    /// the start it last committed, and the clock mapping of a running paced generation.
    pub fn domain_state_of(&mut self, id: DomainName) -> DomainState {
        let pace = self.domain_pace();
        let placement = self.placement_policy();
        let status = self.entropy.pick([
            DomainStatus::Stopped,
            DomainStatus::Running,
            DomainStatus::Paused,
        ]);
        let start_version = self.entropy.any_u64();
        let last_start = self.start_point();
        let clock = if self.entropy.flag() {
            Some(self.domain_clock_state())
        } else {
            None
        };
        DomainState {
            id,
            config: DomainConfig { pace, placement },
            status,
            start_version,
            last_start,
            clock,
        }
    }

    /// Any Model with every resource version it binds pinned to a number, as a stored Model holds
    /// it.
    pub fn pinned_model(&mut self) -> Model {
        let requested = self.model();
        self.pinned(requested)
    }

    /// `requested` with every resource version it binds pinned to a number: the number it asked
    /// for, or for `LATEST` any number.
    pub(crate) fn pinned(&mut self, requested: Model<RequestedResourceVersion>) -> Model {
        let pinned: Result<Model, Infallible> =
            requested.try_map_resource_versions(|_, version| match version {
                RequestedResourceVersion::Number(number) => Ok(number),
                RequestedResourceVersion::Latest => Ok(self.entropy.any_u64()),
            });
        let Ok(model) = pinned;
        model
    }

    /// A Kafka partition number, landing on zero, the extremes and their neighbours as often as
    /// elsewhere.
    fn partition(&mut self) -> i32 {
        match self.entropy.byte() % 6 {
            0 => 0,
            1 => i32::MIN,
            2 => i32::MAX,
            3 => -1,
            4 => 1,
            _ => {
                let bits = self.entropy.up_to(u64::from(u32::MAX));
                let bits = u32::try_from(bits).verified("the draw ends at u32::MAX");
                bits.cast_signed()
            }
        }
    }

    /// The Kafka partitions a leader observed for one ingestor and how its instances share them.
    pub fn kafka_partition_schedule(&mut self) -> KafkaPartitionSchedule {
        let observed_partitions = self.records(Self::partition);
        let rebalance_epoch = self.entropy.any_u64();
        let instance_assignments = self.records(|arbitrary| arbitrary.records(Self::partition));
        KafkaPartitionSchedule {
            observed_partitions,
            rebalance_epoch,
            instance_assignments,
        }
    }

    /// The branch declaration a scheduled node carries: none resolved yet, unbranched, or branched
    /// by a named branch with its complete key schema.
    fn resolved_branching(&mut self) -> Option<ResolvedBranching> {
        match self.entropy.byte() % 3 {
            0 => None,
            1 => Some(ResolvedBranching::unbranched()),
            _ => {
                let branch = self.name();
                let schema = self.create_schema();
                Some(ResolvedBranching::branched(branch, schema))
            }
        }
    }

    /// One component an ownership move reset, and why.
    fn ownership_state_reset(&mut self) -> OwnershipStateReset {
        let component = self.entropy.pick([
            OwnershipStateComponent::BranchAggregated,
            OwnershipStateComponent::BranchLifecycle,
            OwnershipStateComponent::Deduplicator,
            OwnershipStateComponent::KafkaOffsets,
            OwnershipStateComponent::MaterializedRelay,
            OwnershipStateComponent::WasmProcessor,
            OwnershipStateComponent::WindowProcessor,
        ]);
        let cause = self.entropy.pick([
            OwnershipStateResetCause::MissingCheckpoint,
            OwnershipStateResetCause::InvalidCheckpoint,
            OwnershipStateResetCause::ConflictingCheckpoint,
            OwnershipStateResetCause::ExpiredBranchMetadata,
        ]);
        OwnershipStateReset { component, cause }
    }

    /// An ownership move in progress for one scheduled node.
    fn ownership_transition(&mut self) -> OwnershipTransition {
        let id = self.string();
        let source = self.rule_name();
        let destination = self.rule_name();
        let state_recovery = self.entropy.pick([
            OwnershipStateRecoveryOutcome::Complete,
            OwnershipStateRecoveryOutcome::Unverified,
            OwnershipStateRecoveryOutcome::Reset,
        ]);
        let resets = self.records(Self::ownership_state_reset);
        OwnershipTransition {
            id,
            source,
            destination,
            state_recovery,
            resets,
        }
    }

    /// The guest-state scope a coordinated reset or a recovery attempt selects.
    fn reset_scope(&mut self) -> WasmStateResetScope {
        match self.entropy.byte() % 3 {
            0 => WasmStateResetScope::Unbranched,
            1 => WasmStateResetScope::Branch(self.branch_fingerprint()),
            _ => WasmStateResetScope::AllBranches,
        }
    }

    /// Records what a scheduled WASM processor published about its guest state, through the same
    /// transitions the control plane applies: new lifetimes for every branch or one branch,
    /// coordinated resets begun and completed, and recovery attempts admitted and settled. Every
    /// other kind of node holds no guest state and is left as it is.
    fn advance_wasm_state(&mut self, node: &mut ScheduledNode) {
        if node.wasm_processor().is_none() {
            return;
        }
        let steps = self.entropy.count(RECORDS);
        for _ in 0..steps {
            match self.entropy.byte() % 5 {
                0 => node.begin_wasm_state_generation(),
                1 => {
                    let branch = self.branch_fingerprint();
                    node.begin_wasm_branch_state_generation(branch);
                }
                2 => {
                    let request = self.execution_reference();
                    let scope = self.reset_scope();
                    let reason = self.entropy.pick([
                        WasmStateResetReason::Operator,
                        WasmStateResetReason::Transaction,
                        WasmStateResetReason::Guest,
                        WasmStateResetReason::RejectedSnapshot,
                    ]);
                    node.begin_wasm_state_reset(request, scope, reason);
                }
                3 => {
                    let Some(reset) = node.wasm_state_reset() else {
                        continue;
                    };
                    let request = reset.request().clone();
                    node.complete_wasm_state_reset(&request);
                }
                _ => self.attempt_wasm_state_recovery(node),
            }
        }
    }

    /// Admits a recovery attempt for a refused lifetime of one scope of `node`, and settles it with
    /// an outcome when the admission recorded or resumed one.
    fn attempt_wasm_state_recovery(&mut self, node: &mut ScheduledNode) {
        let scope = self.reset_scope();
        let Some(generations) = node.wasm_state_generations() else {
            return;
        };
        let generation = generations.of_reset_scope(&scope);
        let rejection = self.entropy.pick([
            WasmSavedStateRejection::SnapshotEnvelope,
            WasmSavedStateRejection::ApplicationState,
        ]);
        let admission = node.admit_wasm_state_recovery(scope, generation, rejection);
        let request = match admission {
            Some(
                WasmStateRecoveryAdmission::Admitted(request)
                | WasmStateRecoveryAdmission::Resumed(request),
            ) => request,
            Some(
                WasmStateRecoveryAdmission::AlreadyRecovered
                | WasmStateRecoveryAdmission::Exhausted,
            )
            | None => return,
        };
        if !self.entropy.flag() {
            return;
        }
        let outcome = self.entropy.pick([
            WasmStateRecoveryOutcome::Attempted,
            WasmStateRecoveryOutcome::Recovered,
            WasmStateRecoveryOutcome::Failed,
        ]);
        node.settle_wasm_state_recovery(&scope, &request, outcome);
    }

    /// One entry of a domain schedule, built the way the scheduler builds one: any Model with its
    /// resource versions pinned, its schema fingerprint and resolved branching, the Kafka
    /// partitions it shares out, where it is placed, an ownership move in progress, and for a WASM
    /// processor the guest-state lifetimes, resets and recovery attempts it has published.
    pub fn scheduled_node(&mut self) -> ScheduledNode {
        let config = self.pinned_model();
        let fingerprint = self.schema_fingerprint();
        let branching = self.resolved_branching();
        let mut node = ScheduledNode::new(config, fingerprint).with_resolved_branching(branching);
        if self.entropy.flag() {
            let partitions = self.kafka_partition_schedule();
            node = node.with_kafka_partitions(partitions);
        }
        let primary = self.optional_cluster_node();
        let assigned = self.records(Self::rule_name::<ClusterNodeName>);
        node = node.placed_on(primary, assigned);
        if self.entropy.flag() {
            node.ownership_transition = Some(self.ownership_transition());
        }
        self.advance_wasm_state(&mut node);
        node
    }

    /// One placement group of a domain schedule: its member nodes and the cluster node they share.
    fn placement_group(&mut self) -> PlacementGroupSchedule {
        let members = self.records(|arbitrary| {
            let kind = arbitrary.model_kind();
            NodeRef::new(kind, arbitrary.name::<ModelName>())
        });
        let primary_node = self.optional_cluster_node();
        PlacementGroupSchedule {
            members,
            primary_node,
        }
    }

    /// The committed schedule of the domain named `domain`: its scheduled nodes in the order the
    /// registry emitted them, and its placement groups.
    pub fn domain_schedule_of(&mut self, domain: DomainName) -> DomainSchedule {
        let nodes = self.records(Self::scheduled_node);
        let placement_groups = self.records(Self::placement_group);
        DomainSchedule::new(domain, nodes, placement_groups)
    }

    /// The authority a leader grants one restore to install a domain's state under: its tenure, the
    /// restore's execution, the mutation revision that admitted it, and the state generation.
    pub fn restore_state_authority(&mut self) -> RestoreStateAuthority {
        let leader = self.rule_name();
        let term = self.entropy.any_u64();
        let execution = self.execution_reference();
        let mutation_revision = self.entropy.any_u64();
        let generation = self.entropy.any_u64();
        RestoreStateAuthority {
            leader,
            term,
            execution,
            mutation_revision,
            generation,
        }
    }

    /// The text a checksum, a reason or an error message holds, or none.
    fn optional_string(&mut self) -> Option<String> {
        if self.entropy.flag() {
            Some(self.string())
        } else {
            None
        }
    }

    /// The published metadata of the resource version `id`.
    pub fn resource_version_of(&mut self, id: ResourceId) -> ResourceVersion {
        let root_checksum = self.string();
        let manifest_checksum = self.string();
        let file_count = self.entropy.any_u64();
        let total_bytes = self.entropy.any_u64();
        let archive_bytes = self.entropy.any_u64();
        let created_at = self.timestamp();
        let created_by_node = self.rule_name();
        ResourceVersion {
            id,
            root_checksum,
            manifest_checksum,
            file_count,
            total_bytes,
            archive_bytes,
            created_at,
            created_by_node,
        }
    }

    /// What one node process recorded about its copy of a resource version.
    pub fn resource_node_status_of(&mut self, key: ResourceReplicaKey) -> ResourceNodeStatus {
        let state = self.entropy.pick([
            ResourceNodeState::Pending,
            ResourceNodeState::Ready,
            ResourceNodeState::Failed,
        ]);
        let root_checksum = self.optional_string();
        let last_verified_at = if self.entropy.flag() {
            Some(self.timestamp())
        } else {
            None
        };
        let source_node = if self.entropy.flag() {
            Some(self.node_identity())
        } else {
            None
        };
        let error = self.optional_string();
        ResourceNodeStatus {
            key,
            state,
            root_checksum,
            last_verified_at,
            source_node,
            error,
        }
    }

    /// The installation outcome of one upload: applying, or terminal with the revision that
    /// recorded it.
    pub fn resource_upload_state(&mut self) -> ResourceUploadState {
        let root_checksum = self.string();
        match self.entropy.byte() % 3 {
            0 => ResourceUploadState::Applying { root_checksum },
            1 => ResourceUploadState::Completed {
                root_checksum,
                outcome_revision: self.entropy.any_u64(),
            },
            _ => ResourceUploadState::Failed {
                root_checksum,
                outcome_revision: self.entropy.any_u64(),
                reason: self.string(),
            },
        }
    }

    /// The durable assignment of the upload `key` to `version`, and its installation outcome.
    pub fn resource_upload_of(&mut self, key: ResourceUploadKey, version: u64) -> ResourceUpload {
        let state = self.resource_upload_state();
        ResourceUpload {
            key,
            version,
            state,
        }
    }
}
