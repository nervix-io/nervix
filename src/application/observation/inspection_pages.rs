//! Page coordination every state inspection shares, whichever kind of state it reads.
//!
//! Layer: control plane.
//! - **Owns.** Projecting an observed failure to its fixed refusal, growing an inspection's charge
//!   without waiting while it holds part of its class, the unit owner an inspection asks, the
//!   resume position of a continuation within one unit, complete count accumulation, the distinct
//!   names of the columns a section appends to its payload, and the command result that carries a
//!   read's envelope beside its requested projection.
//! - **Depends on.** Committed schedules, typed inspection identities and bounded execution.
//! - **Must not know.** How any kind of state is held, captured, carried or encoded.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns the boundary declared above")
)]

use nervix_client_wire::{StateInspection, StateInspectionResult};
use nervix_models::{
    BranchKeyFingerprint, ClusterNodeName, FieldName, InspectionFormat, ParseAsType, ScheduledNode,
    SchemaField, StateInspectionCounts, StateInspectionPosition, StateInspectionRejection, Unit,
};

use crate::{
    application::{
        command_result::{CommandResult, RenderedStateInspection},
        model_mutation::{command_error, command_ok},
    },
    state_inspection_cursor::StateInspectionCursor,
};

/// The command result of one state read: its requested projection as the message, a refusal as
/// a failed command, and the typed envelope beside either.
pub(super) fn rendered_inspection(
    state: StateInspection,
    format: InspectionFormat,
) -> CommandResult {
    let message = match format {
        InspectionFormat::Json => state.to_json(),
        InspectionFormat::Text => match state.display_text() {
            Ok(text) => text,
            Err(error) => return command_error(format!("{error:#}")),
        },
    };
    let mut result = if matches!(state.result, StateInspectionResult::Rejected(_)) {
        command_error(message)
    } else {
        command_ok(message)
    };
    result.state = Some(Box::new(RenderedStateInspection {
        inspection: state,
        format,
    }));
    result
}

/// How growing an inspection's charge went: it grew, or its class had no room for it now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InspectionGrowth {
    Grown,
    /// The inspection gives back everything it holds and runs again under a charge of `needed`
    /// bytes once its class has room for them, so it never waits while holding part of the class.
    Short {
        needed: u64,
    },
}

/// Grow an inspection's charge by `bytes`. A class without room now is backpressure, never a
/// refusal: the inspection reports the charge it needs. Only a charge larger than the whole class
/// refuses, since no wait could grant it.
pub(super) fn grow_inspection(
    memory: &mut nervix_execution::GrowingCharge,
    bytes: u64,
) -> Result<InspectionGrowth, StateInspectionRejection> {
    let Err(refusal) = memory.grow_by(bytes) else {
        return Ok(InspectionGrowth::Grown);
    };
    if let Some(needed) = nervix_execution::Shortfall::of(&refusal) {
        return Ok(InspectionGrowth::Short { needed });
    }
    Err(inspection_refusal(
        refusal,
        StateInspectionRejection::NotReady,
    ))
}

/// Runs one owner page `read` within allowances of the state-read, restore metadata and bulk
/// classes, passing it the bulk allowance's executor, through which the page and its reply take
/// every charge. The bulk share starts at `bulk` bytes. A read whose share has no room for a
/// charge it requires gives everything back and runs again within the share it needed, which it
/// waits for while it holds nothing, so a full class holds the read back and never refuses it. A
/// row after a page's first joins only while the share has room for it now, so it never makes the
/// read run again. It returns the read's own outcome, or the refusal of a share larger than the
/// whole class.
pub(super) async fn within_owner_page_shares<T, F, Fut>(
    executor: &nervix_execution::Executor,
    bulk: u64,
    read: F,
) -> Result<T, error_stack::Report<nervix_execution::AdmissionError>>
where
    F: Fn(nervix_execution::Executor) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    use nervix_execution::{AdmissionError, MemoryClass};

    let read = &read;
    executor
        .within_allowance(MemoryClass::StateRead, 0, |state_read| async move {
            state_read
                .within_allowance(MemoryClass::RestoreMetadata, 0, |metadata| async move {
                    metadata
                        .within_allowance(MemoryClass::Bulk, bulk, |share| async move {
                            Ok::<_, error_stack::Report<AdmissionError>>(read(share).await)
                        })
                        .await?
                })
                .await?
        })
        .await?
}

/// The refusal of an owner page whose allowance could not be had: a share larger than the whole
/// class is a page this node could never hold, and any other refusal means the node admits no
/// more work.
pub(super) fn owner_page_refusal(
    refusal: error_stack::Report<nervix_execution::AdmissionError>,
) -> StateInspectionRejection {
    let reason = match refusal.current_context() {
        nervix_execution::AdmissionError::ExceedsBudget { .. } => {
            StateInspectionRejection::ReplyTooLarge
        }
        _ => StateInspectionRejection::NotReady,
    };
    inspection_refusal(refusal, reason)
}

/// Project an observed internal failure to the fixed inspection protocol refusal.
/// The diagnostic records the complete report before its wire projection leaves this boundary.
pub(super) fn inspection_refusal(
    error: impl std::fmt::Display,
    reason: StateInspectionRejection,
) -> StateInspectionRejection {
    tracing::debug!(error = %format_args!("{error:#}"), ?reason, "state inspection refused");
    reason
}

/// The node that answers an inspection of `unit`: its execution node, or this node when the unit
/// executes on every node, this one included.
pub(super) fn inspection_owner(
    node: &ScheduledNode,
    unit: Unit,
    local: &ClusterNodeName,
) -> Result<ClusterNodeName, StateInspectionRejection> {
    if let Some(owner) = node.unit_execution_node(unit) {
        return Ok(owner.clone());
    }
    if node.unit_executes_on(unit, local) {
        return Ok(local.clone());
    }
    Err(StateInspectionRejection::OwnerUnavailable)
}

/// Whether a continuation already read all of `unit`, and the position it resumes inside it.
pub(super) fn resume_position(
    cursor: Option<&StateInspectionCursor>,
    unit: Unit,
) -> (bool, Option<(Option<BranchKeyFingerprint>, u64)>) {
    match cursor.map(|cursor| &cursor.position) {
        Some(StateInspectionPosition::AfterUnit(previous)) => (unit <= *previous, None),
        Some(StateInspectionPosition::Rows {
            unit: previous,
            branch,
            offset,
        }) => (
            unit < *previous,
            (unit == *previous).then_some((*branch, *offset)),
        ),
        None => (false, None),
    }
}

pub(super) fn add_counts(
    total: &mut StateInspectionCounts,
    unit: StateInspectionCounts,
) -> Result<(), StateInspectionRejection> {
    total.branches = total
        .branches
        .checked_add(unit.branches)
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    total.rows = total
        .rows
        .checked_add(unit.rows)
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    total.bytes = total
        .bytes
        .checked_add(unit.bytes)
        .ok_or(StateInspectionRejection::ReplyTooLarge)?;
    Ok(())
}

/// A column a section appends after its payload, named `base` unless a field already holds that
/// name, and otherwise `base_<n>` for the first `n` no field holds.
pub(super) fn distinct_field(
    fields: &[SchemaField],
    base: &str,
    ty: ParseAsType,
    optional: bool,
    sensitive: bool,
    cancellation: &nervix_execution::Cancellation,
) -> Result<SchemaField, StateInspectionRejection> {
    // n field names cannot occupy n+1 distinct candidates. Numeric suffixes keep every candidate
    // addressable even when the fields occupy all underscore suffixes to the name limit. Each scan
    // is bounded by the session's schema collection bound.
    for suffix in 0..=fields.len() {
        cancellation
            .check()
            .map_err(|error| inspection_refusal(error, StateInspectionRejection::Deadline))?;
        let name = if suffix == 0 {
            base.to_string()
        } else {
            format!("{base}_{suffix}")
        };
        if fields.iter().any(|field| field.name.as_str() == name) {
            continue;
        }
        return Ok(SchemaField {
            name: FieldName::parse(&name)
                .map_err(|error| inspection_refusal(error, StateInspectionRejection::NotReady))?,
            ty,
            optional,
            sensitive,
        });
    }
    unreachable!("n schema fields cannot occupy n+1 distinct column names")
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    #[nervix_primitives::test]
    async fn appended_columns_remain_addressable_after_dense_name_collisions() {
        let base = "ingested_at_low_watermark";
        let mut fields = Vec::new();
        for suffix in 0..128 {
            let candidate = format!("{base}{}", "_".repeat(suffix));
            let Ok(name) = FieldName::parse(&candidate) else {
                break;
            };
            fields.push(SchemaField {
                name,
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            });
        }
        assert!(fields.len() > 1);
        let executor = nervix_execution::Executor::default();
        let charge = executor
            .try_reserve(nervix_execution::MemoryClass::Bulk, 4096)
            .assured("fixture admission");
        executor
            .run_cpu(
                nervix_execution::CpuClass::Bulk,
                charge,
                move |_charge, cancellation| {
                    let appended = distinct_field(
                        &fields,
                        base,
                        ParseAsType::Datetime,
                        false,
                        false,
                        cancellation,
                    )
                    .assured("a distinct column name fits");
                    assert!(fields.iter().all(|field| field.name != appended.name));
                    assert_eq!(appended.ty, ParseAsType::Datetime);
                    assert!(!appended.optional && !appended.sensitive);
                },
            )
            .await
            .assured("bounded naming job");
    }
}
