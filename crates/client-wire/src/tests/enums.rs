//! Every enum maps to the schema enum of the same name and back, and nothing else maps back.

use std::{collections::BTreeSet, fmt::Debug};

use error_stack::Report;

use crate::{
    codec::UndeclaredEnumValue,
    command::{ALL_EXECUTION_REFERENCE_CONFLICTS, ALL_UNKNOWN_OUTCOME_CAUSES},
    common::{ALL_MODEL_KINDS, ALL_OUTCOME_ORIGINS},
    domain::ALL_DOMAIN_STATUSES,
    event::ALL_NOTICE_LEVELS,
    impact::{
        ALL_ACTIVATION_ACTIONS, ALL_DOMAIN_LIFECYCLE_ACTIONS, ALL_IMPACT_DIAGNOSTIC_KINDS,
        ALL_IMPACT_EDGE_KINDS, ALL_MODEL_CHANGE_ASPECTS, ALL_REBUILD_REASONS,
        ALL_RESOURCE_CATALOG_ACTIONS, ALL_STATE_PURGES,
    },
    reply::{
        ALL_CANCEL_STATES, ALL_CANCELLATION_STAGES, ALL_INSPECTION_REJECTIONS,
        ALL_REQUEST_REJECTIONS, ALL_SUGGESTION_KINDS,
    },
    row::ALL_SCALAR_TYPES,
    subscription::{ALL_ROWS_SKIPPED_CAUSES, ALL_SUBSCRIPTION_END_REASONS, ALL_SUBSCRIPTION_TYPES},
    upload::ALL_UPLOAD_FAILURES,
    wire,
};

/// Checks one enum against its schema enum: every value maps to a distinct schema value and back,
/// and the schema declares no value the enum lacks.
fn assert_exact_mapping<V, W>(values: &[V], declared: &[W])
where
    V: Clone + PartialEq + Debug + Into<W> + TryFrom<W, Error = Report<UndeclaredEnumValue>>,
    W: Copy + Ord + Debug,
{
    let declared = declared.iter().copied().collect::<BTreeSet<W>>();
    let mut mapped = BTreeSet::new();
    for value in values {
        let wire_value: W = value.clone().into();
        assert!(
            declared.contains(&wire_value),
            "{value:?} maps to a declared value"
        );
        assert!(
            mapped.insert(wire_value),
            "{value:?} maps to a distinct value"
        );
        match V::try_from(wire_value) {
            Ok(decoded) => assert_eq!(&decoded, value),
            Err(error) => panic!("{value:?} does not map back: {error:?}"),
        }
    }
    assert_eq!(
        mapped, declared,
        "the schema declares exactly one value per variant"
    );
}

/// Refuses every byte past the schema's last declared value.
fn assert_undeclared_refused<V, W>(maximum: u8, construct: fn(u8) -> W)
where
    V: Debug + TryFrom<W, Error = Report<UndeclaredEnumValue>>,
{
    let Some(first_undeclared) = maximum.checked_add(1) else {
        panic!("every schema enum leaves bytes undeclared");
    };
    for byte in first_undeclared..=u8::MAX {
        let error = match V::try_from(construct(byte)) {
            Ok(value) => panic!("undeclared byte {byte} maps to {value:?}"),
            Err(error) => error,
        };
        assert_eq!(
            error.current_context(),
            &UndeclaredEnumValue { value: byte }
        );
    }
}

#[test]
fn every_enum_maps_exactly_to_its_schema_enum() {
    assert_exact_mapping(ALL_OUTCOME_ORIGINS, wire::OutcomeOrigin::ENUM_VALUES);
    assert_exact_mapping(ALL_MODEL_KINDS, wire::ModelKind::ENUM_VALUES);
    assert_exact_mapping(
        ALL_UNKNOWN_OUTCOME_CAUSES,
        wire::UnknownOutcomeCause::ENUM_VALUES,
    );
    assert_exact_mapping(
        ALL_EXECUTION_REFERENCE_CONFLICTS,
        wire::ExecutionReferenceConflictKind::ENUM_VALUES,
    );
    assert_exact_mapping(ALL_DOMAIN_STATUSES, wire::DomainStatus::ENUM_VALUES);
    assert_exact_mapping(ALL_NOTICE_LEVELS, wire::NoticeLevel::ENUM_VALUES);
    assert_exact_mapping(
        ALL_IMPACT_DIAGNOSTIC_KINDS,
        wire::ImpactDiagnosticKind::ENUM_VALUES,
    );
    assert_exact_mapping(ALL_IMPACT_EDGE_KINDS, wire::ImpactEdgeKind::ENUM_VALUES);
    assert_exact_mapping(
        ALL_RESOURCE_CATALOG_ACTIONS,
        wire::ResourceCatalogAction::ENUM_VALUES,
    );
    assert_exact_mapping(
        ALL_DOMAIN_LIFECYCLE_ACTIONS,
        wire::DomainLifecycleAction::ENUM_VALUES,
    );
    assert_exact_mapping(ALL_ACTIVATION_ACTIONS, wire::ActivationAction::ENUM_VALUES);
    assert_exact_mapping(ALL_REBUILD_REASONS, wire::RebuildReason::ENUM_VALUES);
    assert_exact_mapping(ALL_STATE_PURGES, wire::StatePurge::ENUM_VALUES);
    assert_exact_mapping(
        ALL_MODEL_CHANGE_ASPECTS,
        wire::ModelChangeAspect::ENUM_VALUES,
    );
    assert_exact_mapping(ALL_SUGGESTION_KINDS, wire::SuggestionKind::ENUM_VALUES);
    assert_exact_mapping(
        ALL_INSPECTION_REJECTIONS,
        wire::InspectionRejection::ENUM_VALUES,
    );
    assert_exact_mapping(ALL_CANCEL_STATES, wire::CancelState::ENUM_VALUES);
    assert_exact_mapping(
        ALL_CANCELLATION_STAGES,
        wire::CancellationStage::ENUM_VALUES,
    );
    assert_exact_mapping(ALL_REQUEST_REJECTIONS, wire::RequestRejection::ENUM_VALUES);
    assert_exact_mapping(ALL_SCALAR_TYPES, wire::ScalarType::ENUM_VALUES);
    assert_exact_mapping(ALL_SUBSCRIPTION_TYPES, wire::SubscriptionType::ENUM_VALUES);
    assert_exact_mapping(ALL_ROWS_SKIPPED_CAUSES, wire::RowsSkippedCause::ENUM_VALUES);
    assert_exact_mapping(
        ALL_SUBSCRIPTION_END_REASONS,
        wire::SubscriptionEndReason::ENUM_VALUES,
    );
    assert_exact_mapping(ALL_UPLOAD_FAILURES, wire::UploadFailure::ENUM_VALUES);
}

#[test]
fn every_undeclared_enum_byte_is_refused() {
    assert_undeclared_refused::<crate::OutcomeOrigin, _>(
        wire::OutcomeOrigin::ENUM_MAX,
        wire::OutcomeOrigin,
    );
    assert_undeclared_refused::<nervix_models::ModelKind, _>(
        wire::ModelKind::ENUM_MAX,
        wire::ModelKind,
    );
    assert_undeclared_refused::<crate::UnknownOutcomeCause, _>(
        wire::UnknownOutcomeCause::ENUM_MAX,
        wire::UnknownOutcomeCause,
    );
    assert_undeclared_refused::<crate::ExecutionReferenceConflict, _>(
        wire::ExecutionReferenceConflictKind::ENUM_MAX,
        wire::ExecutionReferenceConflictKind,
    );
    assert_undeclared_refused::<nervix_models::DomainStatus, _>(
        wire::DomainStatus::ENUM_MAX,
        wire::DomainStatus,
    );
    assert_undeclared_refused::<crate::NoticeLevel, _>(
        wire::NoticeLevel::ENUM_MAX,
        wire::NoticeLevel,
    );
    assert_undeclared_refused::<nervix_models::ImpactDiagnosticKind, _>(
        wire::ImpactDiagnosticKind::ENUM_MAX,
        wire::ImpactDiagnosticKind,
    );
    assert_undeclared_refused::<nervix_models::ImpactEdgeKind, _>(
        wire::ImpactEdgeKind::ENUM_MAX,
        wire::ImpactEdgeKind,
    );
    assert_undeclared_refused::<nervix_models::ResourceCatalogAction, _>(
        wire::ResourceCatalogAction::ENUM_MAX,
        wire::ResourceCatalogAction,
    );
    assert_undeclared_refused::<nervix_models::DomainLifecycleAction, _>(
        wire::DomainLifecycleAction::ENUM_MAX,
        wire::DomainLifecycleAction,
    );
    assert_undeclared_refused::<nervix_models::ActivationAction, _>(
        wire::ActivationAction::ENUM_MAX,
        wire::ActivationAction,
    );
    assert_undeclared_refused::<nervix_models::RebuildReason, _>(
        wire::RebuildReason::ENUM_MAX,
        wire::RebuildReason,
    );
    assert_undeclared_refused::<nervix_models::StatePurge, _>(
        wire::StatePurge::ENUM_MAX,
        wire::StatePurge,
    );
    assert_undeclared_refused::<nervix_models::ModelChangeAspect, _>(
        wire::ModelChangeAspect::ENUM_MAX,
        wire::ModelChangeAspect,
    );
    assert_undeclared_refused::<crate::SuggestionKind, _>(
        wire::SuggestionKind::ENUM_MAX,
        wire::SuggestionKind,
    );
    assert_undeclared_refused::<crate::InspectionRejection, _>(
        wire::InspectionRejection::ENUM_MAX,
        wire::InspectionRejection,
    );
    assert_undeclared_refused::<crate::CancelState, _>(
        wire::CancelState::ENUM_MAX,
        wire::CancelState,
    );
    assert_undeclared_refused::<crate::CancellationStage, _>(
        wire::CancellationStage::ENUM_MAX,
        wire::CancellationStage,
    );
    assert_undeclared_refused::<crate::RequestRejection, _>(
        wire::RequestRejection::ENUM_MAX,
        wire::RequestRejection,
    );
    assert_undeclared_refused::<crate::row::ScalarType, _>(
        wire::ScalarType::ENUM_MAX,
        wire::ScalarType,
    );
    assert_undeclared_refused::<crate::SubscriptionType, _>(
        wire::SubscriptionType::ENUM_MAX,
        wire::SubscriptionType,
    );
    assert_undeclared_refused::<crate::RowsSkippedCause, _>(
        wire::RowsSkippedCause::ENUM_MAX,
        wire::RowsSkippedCause,
    );
    assert_undeclared_refused::<crate::SubscriptionEndReason, _>(
        wire::SubscriptionEndReason::ENUM_MAX,
        wire::SubscriptionEndReason,
    );
    assert_undeclared_refused::<crate::UploadFailure, _>(
        wire::UploadFailure::ENUM_MAX,
        wire::UploadFailure,
    );
}
