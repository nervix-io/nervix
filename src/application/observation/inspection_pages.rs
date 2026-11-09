//! Page coordination every state inspection shares, whichever kind of state it reads.
//!
//! Layer: control plane.
//! - **Owns.** Projecting an observed failure to its fixed refusal, the unit owner an inspection
//!   asks, the resume position of a continuation within one unit, complete count accumulation and
//!   the distinct names of the columns a section appends to its payload.
//! - **Depends on.** Committed schedules, typed inspection identities and bounded execution.
//! - **Must not know.** How any kind of state is held, captured, carried or encoded.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns the boundary declared above")
)]

use nervix_models::{
    BranchKeyFingerprint, ClusterNodeName, FieldName, ParseAsType, ScheduledNode, SchemaField,
    StateInspectionCounts, StateInspectionPosition, StateInspectionRejection, Unit,
};

use crate::state_inspection_cursor::StateInspectionCursor;

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
