//! WASM guest-state facts as typed tables of the inspection envelope, and their text and JSON
//! projections.

use std::num::NonZeroU64;

use error_stack::{Report, ResultExt as _};
use flatbuffers::WIPOffset;
use nervix_models::{
    BranchKeyFingerprint, CommandExecutionReference, WasmBranchStateInspection,
    WasmCheckpointCounts, WasmCheckpointStage, WasmCheckpointState, WasmRecoveryCounts,
    WasmRecoveryInspection, WasmSavedStateRejection, WasmStateBinding, WasmStateGeneration,
    WasmStateRecoveryOutcome, WasmStateReset, WasmStateResetInspection, WasmStateResetPhase,
    WasmStateResetReadiness, WasmStateResetReason, WasmStateResetScope, WasmUnitStateInspection,
};
use serde::{Serialize, Serializer, ser::SerializeMap as _};

use crate::{
    codec::{
        Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum, wire_size,
    },
    wire,
};

wire_enum!(ALL_WASM_CHECKPOINT_STAGES: WasmCheckpointStage => wire::WasmCheckpointStage {
    Empty, Captured, LocallyDurable, ReplicaConfirmed, Failed,
});
wire_enum!(ALL_WASM_RESET_PHASES: WasmStateResetPhase => wire::WasmResetPhase {
    Publishing, Ready,
});
wire_enum!(ALL_WASM_RESET_REASONS: WasmStateResetReason => wire::WasmResetReason {
    Operator, Transaction, Guest, RejectedSnapshot,
});
wire_enum!(ALL_WASM_RESET_READINESS: WasmStateResetReadiness => wire::WasmResetReadiness {
    Resetting, AwaitingUsableExecution, Ready,
});
wire_enum!(ALL_WASM_SAVED_STATE_REJECTIONS: WasmSavedStateRejection => wire::WasmSavedStateRejection {
    SnapshotEnvelope, ApplicationState,
});
wire_enum!(ALL_WASM_RECOVERY_OUTCOMES: WasmStateRecoveryOutcome => wire::WasmRecoveryOutcome {
    Attempted, Recovered, Failed,
});

/// The encoded size one branch entry cannot exceed in the session schema: its table and vtable,
/// a fingerprint table of 32 bytes, a checkpoint table, a recovery table with the longest command
/// reference the vocabulary admits, and alignment padding for each.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the session schema's WasmBranchStateFacts layout with a 32-byte fingerprint and \
                  a 128-byte command reference encodes within 512 bytes; the bound test pins it"
    )
)]
pub const MAX_ENCODED_WASM_BRANCH_ENTRY_BYTES: usize = 512;

fn ordered_entries(entries: &[WasmBranchStateInspection], total: usize) -> bool {
    if entries.len() > total || !entries.iter().all(WasmBranchStateInspection::has_state) {
        return false;
    }
    entries.windows(2).all(|pair| pair[0].key() < pair[1].key())
}

fn invalid_reference(field: &'static str) -> WireDecodeError {
    WireDecodeError::InvalidValue {
        field,
        kind: "command execution reference",
    }
}

fn invalid_generation(field: &'static str) -> WireDecodeError {
    WireDecodeError::InvalidValue {
        field,
        kind: "nonzero WASM state generation",
    }
}

pub(super) fn encode_binding<'fbb>(
    binding: &WasmStateBinding,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::WasmStateBinding<'fbb>>, Report<WireEncodeError>> {
    let resource = encoder.text("WasmStateBinding.resource", binding.resource.as_str())?;
    let file = encoder.text("WasmStateBinding.file", &binding.file)?;
    Ok(wire::WasmStateBinding::create(
        encoder.fbb(),
        &wire::WasmStateBindingArgs {
            resource: Some(resource),
            version: binding.version,
            file: Some(file),
        },
    ))
}

pub(super) fn decode_binding(
    decoder: Decoder<'_>,
    binding: wire::WasmStateBinding<'_>,
) -> Result<WasmStateBinding, Report<WireDecodeError>> {
    Ok(WasmStateBinding {
        resource: decoder.name("WasmStateBinding.resource", binding.resource())?,
        version: binding.version(),
        file: decoder.text("WasmStateBinding.file", binding.file())?,
    })
}

fn encode_scope(
    scope: &WasmStateResetScope,
    encoder: &mut Encoder<'_>,
) -> Result<EncodedUnion<wire::WasmStateScope>, Report<WireEncodeError>> {
    match scope {
        WasmStateResetScope::Unbranched => {
            let table =
                wire::WasmUnbranchedScope::create(encoder.fbb(), &wire::WasmUnbranchedScopeArgs {});
            Ok(EncodedUnion::new(
                wire::WasmStateScope::WasmUnbranchedScope,
                table,
            ))
        }
        WasmStateResetScope::Branch(branch) => {
            let branch = encoder.fingerprint("WasmBranchScope.branch", branch.fingerprint())?;
            let table = wire::WasmBranchScope::create(
                encoder.fbb(),
                &wire::WasmBranchScopeArgs {
                    branch: Some(branch),
                },
            );
            Ok(EncodedUnion::new(
                wire::WasmStateScope::WasmBranchScope,
                table,
            ))
        }
        WasmStateResetScope::AllBranches => {
            let table = wire::WasmAllBranchesScope::create(
                encoder.fbb(),
                &wire::WasmAllBranchesScopeArgs {},
            );
            Ok(EncodedUnion::new(
                wire::WasmStateScope::WasmAllBranchesScope,
                table,
            ))
        }
    }
}

fn decode_scope(
    decoder: Decoder<'_>,
    reset: wire::WasmResetFacts<'_>,
) -> Result<WasmStateResetScope, Report<WireDecodeError>> {
    match reset.scope_type() {
        wire::WasmStateScope::WasmUnbranchedScope => Ok(WasmStateResetScope::Unbranched),
        wire::WasmStateScope::WasmBranchScope => {
            let scope = reset.scope_as_wasm_branch_scope().ok_or_else(|| {
                Report::new(WireDecodeError::MissingField {
                    field: "WasmResetFacts.scope",
                })
            })?;
            let branch = decoder.fingerprint("WasmBranchScope.branch", scope.branch())?;
            Ok(WasmStateResetScope::Branch(BranchKeyFingerprint::new(
                branch,
            )))
        }
        wire::WasmStateScope::WasmAllBranchesScope => Ok(WasmStateResetScope::AllBranches),
        unknown => Err(decoder.unknown_union("WasmResetFacts.scope", unknown.0)),
    }
}

fn encode_reset<'fbb>(
    reset: &WasmStateResetInspection,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::WasmResetFacts<'fbb>>, Report<WireEncodeError>> {
    let request = encoder.text("WasmResetFacts.request", reset.reset.request().as_str())?;
    let scope = encode_scope(reset.reset.scope(), encoder)?;
    Ok(wire::WasmResetFacts::create(
        encoder.fbb(),
        &wire::WasmResetFactsArgs {
            request: Some(request),
            scope_type: scope.discriminant,
            scope: Some(scope.value),
            phase: Some(reset.reset.phase().into()),
            reason: Some(reset.reset.reason().into()),
            generation: u64::from(reset.generation),
            readiness: Some(reset.readiness.into()),
        },
    ))
}

fn decode_reset(
    decoder: Decoder<'_>,
    reset: wire::WasmResetFacts<'_>,
) -> Result<WasmStateResetInspection, Report<WireDecodeError>> {
    let request = decoder.text("WasmResetFacts.request", reset.request())?;
    let request = CommandExecutionReference::parse(request)
        .change_context(invalid_reference("WasmResetFacts.request"))?;
    let scope = decode_scope(decoder, reset)?;
    let phase: WasmStateResetPhase =
        decoder.required_enumeration("WasmResetFacts.phase", reset.phase())?;
    let reason = decoder.required_enumeration("WasmResetFacts.reason", reset.reason())?;
    let generation = WasmStateGeneration::try_from(reset.generation()).map_err(|error| {
        Report::new(error).change_context(invalid_generation("WasmResetFacts.generation"))
    })?;
    let readiness = decoder.required_enumeration("WasmResetFacts.readiness", reset.readiness())?;
    let mut decoded = WasmStateReset::publishing(request, scope, reason);
    match phase {
        WasmStateResetPhase::Publishing => {}
        WasmStateResetPhase::Ready => decoded.mark_ready(),
    }
    Ok(WasmStateResetInspection {
        reset: decoded,
        generation,
        readiness,
    })
}

fn encode_checkpoint_counts<'fbb>(
    counts: &WasmCheckpointCounts,
    encoder: &mut Encoder<'fbb>,
) -> WIPOffset<wire::WasmCheckpointCounts<'fbb>> {
    wire::WasmCheckpointCounts::create(
        encoder.fbb(),
        &wire::WasmCheckpointCountsArgs {
            total: wire_size(counts.total),
            empty: wire_size(counts.empty),
            captured: wire_size(counts.captured),
            locally_durable: wire_size(counts.locally_durable),
            awaiting_replicas: wire_size(counts.awaiting_replicas),
            replica_confirmed: wire_size(counts.replica_confirmed),
            failed: wire_size(counts.failed),
        },
    )
}

fn decode_checkpoint_counts(
    decoder: Decoder<'_>,
    counts: wire::WasmCheckpointCounts<'_>,
) -> Result<WasmCheckpointCounts, Report<WireDecodeError>> {
    Ok(WasmCheckpointCounts {
        total: decoder.size("WasmCheckpointCounts.total", counts.total())?,
        empty: decoder.size("WasmCheckpointCounts.empty", counts.empty())?,
        captured: decoder.size("WasmCheckpointCounts.captured", counts.captured())?,
        locally_durable: decoder.size(
            "WasmCheckpointCounts.locally_durable",
            counts.locally_durable(),
        )?,
        awaiting_replicas: decoder.size(
            "WasmCheckpointCounts.awaiting_replicas",
            counts.awaiting_replicas(),
        )?,
        replica_confirmed: decoder.size(
            "WasmCheckpointCounts.replica_confirmed",
            counts.replica_confirmed(),
        )?,
        failed: decoder.size("WasmCheckpointCounts.failed", counts.failed())?,
    })
}

fn encode_recovery_counts<'fbb>(
    counts: &WasmRecoveryCounts,
    encoder: &mut Encoder<'fbb>,
) -> WIPOffset<wire::WasmRecoveryCounts<'fbb>> {
    wire::WasmRecoveryCounts::create(
        encoder.fbb(),
        &wire::WasmRecoveryCountsArgs {
            attempted: wire_size(counts.attempted),
            recovered: wire_size(counts.recovered),
            failed: wire_size(counts.failed),
        },
    )
}

fn decode_recovery_counts(
    decoder: Decoder<'_>,
    counts: wire::WasmRecoveryCounts<'_>,
) -> Result<WasmRecoveryCounts, Report<WireDecodeError>> {
    Ok(WasmRecoveryCounts {
        attempted: decoder.size("WasmRecoveryCounts.attempted", counts.attempted())?,
        recovered: decoder.size("WasmRecoveryCounts.recovered", counts.recovered())?,
        failed: decoder.size("WasmRecoveryCounts.failed", counts.failed())?,
    })
}

fn encode_checkpoint<'fbb>(
    checkpoint: &WasmCheckpointState,
    encoder: &mut Encoder<'fbb>,
) -> WIPOffset<wire::WasmCheckpointFacts<'fbb>> {
    wire::WasmCheckpointFacts::create(
        encoder.fbb(),
        &wire::WasmCheckpointFactsArgs {
            generation: u64::from(checkpoint.generation),
            committed_revision: checkpoint.committed_revision.map(NonZeroU64::get),
            latest_revision: checkpoint.latest_revision.map(NonZeroU64::get),
            stage: Some(checkpoint.stage.into()),
            required_replicas: checkpoint.required_replicas,
            confirmed_replicas: checkpoint.confirmed_replicas,
        },
    )
}

fn decode_revision(
    decoder: Decoder<'_>,
    field: &'static str,
    revision: Option<u64>,
) -> Result<Option<NonZeroU64>, Report<WireDecodeError>> {
    match revision {
        Some(revision) => Ok(Some(decoder.non_zero(field, revision)?)),
        None => Ok(None),
    }
}

fn decode_checkpoint(
    decoder: Decoder<'_>,
    checkpoint: wire::WasmCheckpointFacts<'_>,
) -> Result<WasmCheckpointState, Report<WireDecodeError>> {
    let generation = WasmStateGeneration::try_from(checkpoint.generation()).map_err(|error| {
        Report::new(error).change_context(invalid_generation("WasmCheckpointFacts.generation"))
    })?;
    Ok(WasmCheckpointState {
        generation,
        committed_revision: decode_revision(
            decoder,
            "WasmCheckpointFacts.committed_revision",
            checkpoint.committed_revision(),
        )?,
        latest_revision: decode_revision(
            decoder,
            "WasmCheckpointFacts.latest_revision",
            checkpoint.latest_revision(),
        )?,
        stage: decoder.required_enumeration("WasmCheckpointFacts.stage", checkpoint.stage())?,
        required_replicas: checkpoint.required_replicas(),
        confirmed_replicas: checkpoint.confirmed_replicas(),
    })
}

fn encode_recovery<'fbb>(
    recovery: &WasmRecoveryInspection,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::WasmRecoveryFacts<'fbb>>, Report<WireEncodeError>> {
    let request = encoder.text("WasmRecoveryFacts.request", recovery.request.as_str())?;
    Ok(wire::WasmRecoveryFacts::create(
        encoder.fbb(),
        &wire::WasmRecoveryFactsArgs {
            generation: u64::from(recovery.generation),
            rejection: Some(recovery.rejection.into()),
            request: Some(request),
            outcome: Some(recovery.outcome.into()),
        },
    ))
}

fn decode_recovery(
    decoder: Decoder<'_>,
    recovery: wire::WasmRecoveryFacts<'_>,
) -> Result<WasmRecoveryInspection, Report<WireDecodeError>> {
    let generation = WasmStateGeneration::try_from(recovery.generation()).map_err(|error| {
        Report::new(error).change_context(invalid_generation("WasmRecoveryFacts.generation"))
    })?;
    let request = decoder.text("WasmRecoveryFacts.request", recovery.request())?;
    let request = CommandExecutionReference::parse(request)
        .change_context(invalid_reference("WasmRecoveryFacts.request"))?;
    Ok(WasmRecoveryInspection {
        generation,
        rejection: decoder
            .required_enumeration("WasmRecoveryFacts.rejection", recovery.rejection())?,
        request,
        outcome: decoder.required_enumeration("WasmRecoveryFacts.outcome", recovery.outcome())?,
    })
}

fn encode_entry<'fbb>(
    entry: &WasmBranchStateInspection,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::WasmBranchStateFacts<'fbb>>, Report<WireEncodeError>> {
    let branch = match &entry.branch {
        Some(branch) => {
            Some(encoder.fingerprint("WasmBranchStateFacts.branch", branch.fingerprint())?)
        }
        None => None,
    };
    let checkpoint = entry
        .checkpoint
        .as_ref()
        .map(|checkpoint| encode_checkpoint(checkpoint, encoder));
    let recovery = match &entry.recovery {
        Some(recovery) => Some(encode_recovery(recovery, encoder)?),
        None => None,
    };
    Ok(wire::WasmBranchStateFacts::create(
        encoder.fbb(),
        &wire::WasmBranchStateFactsArgs {
            branch,
            checkpoint,
            recovery,
        },
    ))
}

fn decode_entry(
    decoder: Decoder<'_>,
    entry: wire::WasmBranchStateFacts<'_>,
) -> Result<WasmBranchStateInspection, Report<WireDecodeError>> {
    let branch = match entry.branch() {
        Some(branch) => Some(BranchKeyFingerprint::new(
            decoder.fingerprint("WasmBranchStateFacts.branch", branch)?,
        )),
        None => None,
    };
    let checkpoint = match entry.checkpoint() {
        Some(checkpoint) => Some(decode_checkpoint(decoder, checkpoint)?),
        None => None,
    };
    let recovery = match entry.recovery() {
        Some(recovery) => Some(decode_recovery(decoder, recovery)?),
        None => None,
    };
    Ok(WasmBranchStateInspection {
        branch,
        checkpoint,
        recovery,
    })
}

const ENTRIES_KIND: &str = "branch entries in strictly increasing branch order, each with a \
                            checkpoint or a recovery, within the complete entry count";

pub(super) fn encode_unit<'fbb>(
    state: &WasmUnitStateInspection,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::WasmUnitStateFacts<'fbb>>, Report<WireEncodeError>> {
    if !ordered_entries(&state.branches, state.entries) {
        return Err(Report::new(WireEncodeError::InvalidValue {
            field: "WasmUnitStateFacts.branches",
            kind: ENTRIES_KIND,
        }));
    }
    let reset = match &state.reset {
        Some(reset) => Some(encode_reset(reset, encoder)?),
        None => None,
    };
    let checkpoints = encode_checkpoint_counts(&state.checkpoints, encoder);
    let recoveries = encode_recovery_counts(&state.recoveries, encoder);
    let branches = encoder.table_vector(
        "WasmUnitStateFacts.branches",
        &state.branches,
        |entry, encoder| encode_entry(entry, encoder),
    )?;
    Ok(wire::WasmUnitStateFacts::create(
        encoder.fbb(),
        &wire::WasmUnitStateFactsArgs {
            default_generation: u64::from(state.default_generation),
            reset,
            entries: wire_size(state.entries),
            checkpoints: Some(checkpoints),
            recoveries: Some(recoveries),
            branches: Some(branches),
        },
    ))
}

pub(super) fn decode_unit(
    decoder: Decoder<'_>,
    state: wire::WasmUnitStateFacts<'_>,
) -> Result<WasmUnitStateInspection, Report<WireDecodeError>> {
    let default_generation =
        WasmStateGeneration::try_from(state.default_generation()).map_err(|error| {
            Report::new(error)
                .change_context(invalid_generation("WasmUnitStateFacts.default_generation"))
        })?;
    let reset = match state.reset() {
        Some(reset) => Some(decode_reset(decoder, reset)?),
        None => None,
    };
    let entries = decoder.size("WasmUnitStateFacts.entries", state.entries())?;
    let branches =
        decoder.table_vector("WasmUnitStateFacts.branches", state.branches(), |entry| {
            decode_entry(decoder, entry)
        })?;
    if !ordered_entries(&branches, entries) {
        return Err(Report::new(WireDecodeError::InvalidValue {
            field: "WasmUnitStateFacts.branches",
            kind: ENTRIES_KIND,
        }));
    }
    Ok(WasmUnitStateInspection {
        default_generation,
        reset,
        entries,
        checkpoints: decode_checkpoint_counts(decoder, state.checkpoints())?,
        recoveries: decode_recovery_counts(decoder, state.recoveries())?,
        branches,
    })
}

/// The binding line a WASM state inspection prints after its complete counts.
pub(super) fn binding_line(binding: &WasmStateBinding) -> String {
    format!(
        "binding: resource={} version={} file={}",
        binding.resource.as_str(),
        binding.version,
        binding.file
    )
}

/// The lifetime, reset, recovery and checkpoint-count lines of one unit's WASM guest state, which
/// `DESCRIBE WASM PROCESSOR` and the state inspection both print.
pub fn wasm_unit_summary_lines(state: &WasmUnitStateInspection) -> Vec<String> {
    let mut lines = vec![format!(
        "state default generation: {}",
        state.default_generation
    )];
    if let Some(reset) = &state.reset {
        lines.push(format!(
            "state reset: {}, {}, generation {}",
            reset.reset.phase().as_ref(),
            reset.reset.scope().kind(),
            reset.generation
        ));
        lines.push(format!(
            "state reset reason: {}",
            reset.reset.reason().as_ref()
        ));
        lines.push(format!(
            "state reset readiness: {}",
            reset.readiness.as_ref()
        ));
        lines.push(format!(
            "state reset request: {}",
            reset.reset.request().as_str()
        ));
    }
    if !state.recoveries.is_empty() {
        lines.push(format!(
            "rejected state recoveries: attempted={} recovered={} failed={}",
            state.recoveries.attempted, state.recoveries.recovered, state.recoveries.failed
        ));
    }
    let counts = &state.checkpoints;
    lines.push(format!("state structures: {}", counts.total));
    lines.push(format!(
        "checkpoints awaiting local storage: {}",
        counts.captured
    ));
    lines.push(format!(
        "checkpoints awaiting replicas: {}",
        counts.awaiting_replicas
    ));
    lines.push(format!("failed checkpoints: {}", counts.failed));
    lines
}

fn optional_count(value: Option<u32>) -> String {
    match value {
        Some(count) => count.to_string(),
        None => "unknown".to_string(),
    }
}

fn optional_revision(value: Option<NonZeroU64>) -> String {
    match value {
        Some(revision) => revision.to_string(),
        None => "none".to_string(),
    }
}

/// One line per checkpoint and per recovery of the unit's returned branch entries.
pub(super) fn entry_lines(state: &WasmUnitStateInspection) -> Vec<String> {
    let mut lines = Vec::new();
    for entry in &state.branches {
        let branch = match &entry.branch {
            Some(fingerprint) => fingerprint.to_string(),
            None => "unbranched".to_string(),
        };
        if let Some(checkpoint) = &entry.checkpoint {
            lines.push(format!(
                "checkpoint branch={branch} generation={} committed_revision={} \
                 latest_revision={} stage={} required_replicas={} confirmed_replicas={}",
                checkpoint.generation,
                optional_revision(checkpoint.committed_revision),
                optional_revision(checkpoint.latest_revision),
                checkpoint.stage.as_ref(),
                optional_count(checkpoint.required_replicas),
                optional_count(checkpoint.confirmed_replicas),
            ));
        }
        if let Some(recovery) = &entry.recovery {
            lines.push(format!(
                "rejected state recovery: branch={branch} generation={}, {}, {}",
                recovery.generation, recovery.rejection, recovery.outcome
            ));
        }
    }
    lines
}

fn checkpoint_stage_name(stage: WasmCheckpointStage) -> &'static str {
    match stage {
        WasmCheckpointStage::Empty => "empty",
        WasmCheckpointStage::Captured => "captured",
        WasmCheckpointStage::LocallyDurable => "locally_durable",
        WasmCheckpointStage::ReplicaConfirmed => "replica_confirmed",
        WasmCheckpointStage::Failed => "failed",
    }
}

fn reset_phase_name(phase: WasmStateResetPhase) -> &'static str {
    match phase {
        WasmStateResetPhase::Publishing => "publishing",
        WasmStateResetPhase::Ready => "ready",
    }
}

fn reset_reason_name(reason: WasmStateResetReason) -> &'static str {
    match reason {
        WasmStateResetReason::Operator => "operator",
        WasmStateResetReason::Transaction => "transaction",
        WasmStateResetReason::Guest => "guest",
        WasmStateResetReason::RejectedSnapshot => "rejected_snapshot",
    }
}

fn reset_readiness_name(readiness: WasmStateResetReadiness) -> &'static str {
    match readiness {
        WasmStateResetReadiness::Resetting => "resetting",
        WasmStateResetReadiness::AwaitingUsableExecution => "awaiting_usable_execution",
        WasmStateResetReadiness::Ready => "ready",
    }
}

fn rejection_name(rejection: WasmSavedStateRejection) -> &'static str {
    match rejection {
        WasmSavedStateRejection::SnapshotEnvelope => "snapshot_envelope",
        WasmSavedStateRejection::ApplicationState => "application_state",
    }
}

fn recovery_outcome_name(outcome: WasmStateRecoveryOutcome) -> &'static str {
    match outcome {
        WasmStateRecoveryOutcome::Attempted => "attempted",
        WasmStateRecoveryOutcome::Recovered => "recovered",
        WasmStateRecoveryOutcome::Failed => "failed",
    }
}

/// The JSON object of a pinned binding.
pub(super) struct BindingJson<'a>(pub(super) &'a WasmStateBinding);

impl Serialize for BindingJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut output = serializer.serialize_map(Some(3))?;
        output.serialize_entry("resource", self.0.resource.as_str())?;
        output.serialize_entry("version", &self.0.version)?;
        output.serialize_entry("file", &self.0.file)?;
        output.end()
    }
}

struct ScopeJson<'a>(&'a WasmStateResetScope);

impl Serialize for ScopeJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut output = serializer.serialize_map(None)?;
        match self.0 {
            WasmStateResetScope::Unbranched => output.serialize_entry("kind", "unbranched")?,
            WasmStateResetScope::Branch(branch) => {
                output.serialize_entry("kind", "branch")?;
                output.serialize_entry("branch", branch)?;
            }
            WasmStateResetScope::AllBranches => output.serialize_entry("kind", "all_branches")?,
        }
        output.end()
    }
}

struct ResetJson<'a>(&'a WasmStateResetInspection);

impl Serialize for ResetJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let reset = self.0;
        let mut output = serializer.serialize_map(Some(6))?;
        output.serialize_entry("request", reset.reset.request().as_str())?;
        output.serialize_entry("scope", &ScopeJson(reset.reset.scope()))?;
        output.serialize_entry("phase", reset_phase_name(reset.reset.phase()))?;
        output.serialize_entry("reason", reset_reason_name(reset.reset.reason()))?;
        output.serialize_entry("generation", &reset.generation)?;
        output.serialize_entry("readiness", reset_readiness_name(reset.readiness))?;
        output.end()
    }
}

struct CheckpointJson<'a>(&'a WasmCheckpointState);

impl Serialize for CheckpointJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let checkpoint = self.0;
        let mut output = serializer.serialize_map(Some(6))?;
        output.serialize_entry("generation", &checkpoint.generation)?;
        output.serialize_entry("committed_revision", &checkpoint.committed_revision)?;
        output.serialize_entry("latest_revision", &checkpoint.latest_revision)?;
        output.serialize_entry("stage", checkpoint_stage_name(checkpoint.stage))?;
        output.serialize_entry("required_replicas", &checkpoint.required_replicas)?;
        output.serialize_entry("confirmed_replicas", &checkpoint.confirmed_replicas)?;
        output.end()
    }
}

struct RecoveryJson<'a>(&'a WasmRecoveryInspection);

impl Serialize for RecoveryJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let recovery = self.0;
        let mut output = serializer.serialize_map(Some(4))?;
        output.serialize_entry("generation", &recovery.generation)?;
        output.serialize_entry("rejection", rejection_name(recovery.rejection))?;
        output.serialize_entry("request", recovery.request.as_str())?;
        output.serialize_entry("outcome", recovery_outcome_name(recovery.outcome))?;
        output.end()
    }
}

struct EntryJson<'a>(&'a WasmBranchStateInspection);

impl Serialize for EntryJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let entry = self.0;
        let mut output = serializer.serialize_map(Some(3))?;
        output.serialize_entry("branch", &entry.branch)?;
        output.serialize_entry("checkpoint", &entry.checkpoint.as_ref().map(CheckpointJson))?;
        output.serialize_entry("recovery", &entry.recovery.as_ref().map(RecoveryJson))?;
        output.end()
    }
}

struct CheckpointCountsJson<'a>(&'a WasmCheckpointCounts);

impl Serialize for CheckpointCountsJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let counts = self.0;
        let mut output = serializer.serialize_map(Some(7))?;
        output.serialize_entry("total", &counts.total)?;
        output.serialize_entry("empty", &counts.empty)?;
        output.serialize_entry("captured", &counts.captured)?;
        output.serialize_entry("locally_durable", &counts.locally_durable)?;
        output.serialize_entry("awaiting_replicas", &counts.awaiting_replicas)?;
        output.serialize_entry("replica_confirmed", &counts.replica_confirmed)?;
        output.serialize_entry("failed", &counts.failed)?;
        output.end()
    }
}

struct RecoveryCountsJson<'a>(&'a WasmRecoveryCounts);

impl Serialize for RecoveryCountsJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let counts = self.0;
        let mut output = serializer.serialize_map(Some(3))?;
        output.serialize_entry("attempted", &counts.attempted)?;
        output.serialize_entry("recovered", &counts.recovered)?;
        output.serialize_entry("failed", &counts.failed)?;
        output.end()
    }
}

struct EntriesJson<'a>(&'a [WasmBranchStateInspection]);

impl Serialize for EntriesJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(EntryJson))
    }
}

/// The JSON object of one unit's WASM guest-state facts.
pub(super) struct UnitJson<'a>(pub(super) &'a WasmUnitStateInspection);

impl Serialize for UnitJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let state = self.0;
        let mut output = serializer.serialize_map(Some(6))?;
        output.serialize_entry("default_generation", &state.default_generation)?;
        output.serialize_entry("reset", &state.reset.as_ref().map(ResetJson))?;
        output.serialize_entry("entries", &state.entries)?;
        output.serialize_entry("checkpoints", &CheckpointCountsJson(&state.checkpoints))?;
        output.serialize_entry("recoveries", &RecoveryCountsJson(&state.recoveries))?;
        output.serialize_entry("branches", &EntriesJson(&state.branches))?;
        output.end()
    }
}

/// The `wasm` member of a serialized inspection unit; units without WASM facts omit it.
pub(super) fn serialize_unit<S: Serializer>(
    state: &Option<WasmUnitStateInspection>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match state {
        Some(state) => UnitJson(state).serialize(serializer),
        None => serializer.serialize_none(),
    }
}
