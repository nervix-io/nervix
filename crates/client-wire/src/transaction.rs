//! Encoding the transaction status, preview identity, inspection target and inspection read of the
//! vocabulary.

use std::num::NonZeroUsize;

use error_stack::Report;
use flatbuffers::WIPOffset;
use meticulous::OptionExt as _;
use nervix_models::{
    ImpactPlanningBasis, TransactionInspection, TransactionInspectionTarget, TransactionLifecycle,
    TransactionOperationNumber, TransactionPosition, TransactionPreviewIdentity, TransactionStatus,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_size},
    impact::{decode_report, encode_report},
    wire,
};

/// Encodes the lifecycle of a transaction status.
fn encode_lifecycle(
    lifecycle: &TransactionLifecycle,
    encoder: &mut Encoder<'_>,
) -> Result<EncodedUnion<wire::TransactionState>, Report<WireEncodeError>> {
    let union = match lifecycle {
        TransactionLifecycle::Open => EncodedUnion::new(
            wire::TransactionState::TransactionOpen,
            wire::TransactionOpen::create(encoder.fbb(), &wire::TransactionOpenArgs {}),
        ),
        TransactionLifecycle::Committing => EncodedUnion::new(
            wire::TransactionState::TransactionCommitting,
            wire::TransactionCommitting::create(encoder.fbb(), &wire::TransactionCommittingArgs {}),
        ),
        TransactionLifecycle::Committed => EncodedUnion::new(
            wire::TransactionState::TransactionCommitted,
            wire::TransactionCommitted::create(encoder.fbb(), &wire::TransactionCommittedArgs {}),
        ),
        TransactionLifecycle::Failed {
            failing_operation,
            error,
        } => {
            let error = encoder.text("TransactionFailed.error", error)?;
            let failed = wire::TransactionFailed::create(
                encoder.fbb(),
                &wire::TransactionFailedArgs {
                    failing_operation: wire_size(failing_operation.get()),
                    error: Some(error),
                },
            );
            EncodedUnion::new(wire::TransactionState::TransactionFailed, failed)
        }
        TransactionLifecycle::Reverted => EncodedUnion::new(
            wire::TransactionState::TransactionReverted,
            wire::TransactionReverted::create(encoder.fbb(), &wire::TransactionRevertedArgs {}),
        ),
        TransactionLifecycle::Expired => EncodedUnion::new(
            wire::TransactionState::TransactionExpired,
            wire::TransactionExpired::create(encoder.fbb(), &wire::TransactionExpiredArgs {}),
        ),
    };
    Ok(union)
}

/// Decodes the lifecycle of a transaction status.
fn decode_lifecycle(
    decoder: Decoder<'_>,
    status: wire::TransactionStatus<'_>,
) -> Result<TransactionLifecycle, Report<WireDecodeError>> {
    if let Some(failed) = status.state_as_transaction_failed() {
        let failing_operation = decode_operation_number(
            decoder,
            "TransactionFailed.failing_operation",
            failed.failing_operation(),
        )?;
        let error = decoder.text("TransactionFailed.error", failed.error())?;
        return Ok(TransactionLifecycle::Failed {
            failing_operation,
            error,
        });
    }
    match status.state_type() {
        wire::TransactionState::TransactionOpen => Ok(TransactionLifecycle::Open),
        wire::TransactionState::TransactionCommitting => Ok(TransactionLifecycle::Committing),
        wire::TransactionState::TransactionCommitted => Ok(TransactionLifecycle::Committed),
        wire::TransactionState::TransactionReverted => Ok(TransactionLifecycle::Reverted),
        wire::TransactionState::TransactionExpired => Ok(TransactionLifecycle::Expired),
        undeclared => Err(decoder.unknown_union("TransactionStatus.state", undeclared.0)),
    }
}

pub(crate) fn encode_transaction_status<'fbb>(
    encoder: &mut Encoder<'fbb>,
    status: &TransactionStatus,
) -> Result<WIPOffset<wire::TransactionStatus<'fbb>>, Report<WireEncodeError>> {
    let transaction_id =
        encoder.text("TransactionStatus.transaction_id", status.transaction_id())?;
    let domain = encoder.text("TransactionStatus.domain", status.domain().as_str())?;
    let lifecycle = encode_lifecycle(status.lifecycle(), encoder)?;
    Ok(wire::TransactionStatus::create(
        encoder.fbb(),
        &wire::TransactionStatusArgs {
            transaction_id: Some(transaction_id),
            domain: Some(domain),
            state_type: lifecycle.discriminant,
            state: Some(lifecycle.value),
            accepted_operations: wire_size(status.accepted_operations().accepted_operations()),
            applied_operations: wire_size(status.applied_operations()),
        },
    ))
}

pub(crate) fn decode_transaction_status(
    decoder: Decoder<'_>,
    status: wire::TransactionStatus<'_>,
) -> Result<TransactionStatus, Report<WireDecodeError>> {
    let transaction_id =
        decoder.text("TransactionStatus.transaction_id", status.transaction_id())?;
    let domain = decoder.name("TransactionStatus.domain", status.domain())?;
    let lifecycle = decode_lifecycle(decoder, status)?;
    let accepted_operations = decoder.size(
        "TransactionStatus.accepted_operations",
        status.accepted_operations(),
    )?;
    let applied_operations = decoder.size(
        "TransactionStatus.applied_operations",
        status.applied_operations(),
    )?;
    match TransactionStatus::new(
        transaction_id,
        domain,
        lifecycle,
        TransactionPosition::new(accepted_operations),
        applied_operations,
    ) {
        Ok(status) => Ok(status),
        Err(error) => Err(error.change_context(WireDecodeError::InvalidValue {
            field: "TransactionStatus.applied_operations",
            kind: "operation count",
        })),
    }
}

/// Decodes a one-based operation number.
pub(crate) fn decode_operation_number(
    decoder: Decoder<'_>,
    field: &'static str,
    value: u64,
) -> Result<TransactionOperationNumber, Report<WireDecodeError>> {
    let number = decoder.non_zero(field, value)?;
    let number = decoder.size(field, number.get())?;
    let number = NonZeroUsize::new(number).verified("the value was checked to be non-zero above");
    Ok(TransactionOperationNumber::new(number))
}

/// Encodes a one-based operation number.
pub(crate) fn encode_operation_number(number: TransactionOperationNumber) -> u64 {
    wire_size(number.get())
}

pub(crate) fn encode_preview_identity<'fbb>(
    encoder: &mut Encoder<'fbb>,
    preview: &TransactionPreviewIdentity,
) -> Result<WIPOffset<wire::TransactionPreviewIdentity<'fbb>>, Report<WireEncodeError>> {
    let transaction_id = encoder.text(
        "TransactionPreviewIdentity.transaction_id",
        &preview.transaction_id,
    )?;
    let planning_basis = encoder.fingerprint(
        "TransactionPreviewIdentity.planning_basis",
        preview.planning_basis.fingerprint(),
    )?;
    Ok(wire::TransactionPreviewIdentity::create(
        encoder.fbb(),
        &wire::TransactionPreviewIdentityArgs {
            transaction_id: Some(transaction_id),
            position: wire_size(preview.position.accepted_operations()),
            planning_basis: Some(planning_basis),
        },
    ))
}

pub(crate) fn decode_preview_identity(
    decoder: Decoder<'_>,
    preview: wire::TransactionPreviewIdentity<'_>,
) -> Result<TransactionPreviewIdentity, Report<WireDecodeError>> {
    let transaction_id = decoder.text(
        "TransactionPreviewIdentity.transaction_id",
        preview.transaction_id(),
    )?;
    let position = decoder.size("TransactionPreviewIdentity.position", preview.position())?;
    let planning_basis = decoder.fingerprint(
        "TransactionPreviewIdentity.planning_basis",
        preview.planning_basis(),
    )?;
    let planning_basis = ImpactPlanningBasis::new(planning_basis);
    Ok(TransactionPreviewIdentity {
        transaction_id,
        position: TransactionPosition::new(position),
        planning_basis,
    })
}

/// Encodes one inspection read: the inspected status, the selected operation and the report.
pub(crate) fn encode_inspection<'fbb>(
    encoder: &mut Encoder<'fbb>,
    inspection: &TransactionInspection,
) -> Result<WIPOffset<wire::TransactionInspected<'fbb>>, Report<WireEncodeError>> {
    let transaction = encode_transaction_status(encoder, &inspection.transaction)?;
    let report = encode_report(encoder, &inspection.report)?;
    let operation = inspection.operation.map(encode_operation_number);
    Ok(wire::TransactionInspected::create(
        encoder.fbb(),
        &wire::TransactionInspectedArgs {
            transaction: Some(transaction),
            operation,
            report: Some(report),
        },
    ))
}

/// Decodes one inspection read.
pub(crate) fn decode_inspection(
    decoder: Decoder<'_>,
    inspected: wire::TransactionInspected<'_>,
) -> Result<TransactionInspection, Report<WireDecodeError>> {
    let transaction = decode_transaction_status(decoder, inspected.transaction())?;
    let operation = match inspected.operation() {
        Some(operation) => Some(decode_operation_number(
            decoder,
            "TransactionInspected.operation",
            operation,
        )?),
        None => None,
    };
    let report = decode_report(decoder, inspected.report())?;
    Ok(TransactionInspection {
        transaction,
        operation,
        report,
    })
}

pub(crate) fn encode_inspection_target(
    encoder: &mut Encoder<'_>,
    target: &TransactionInspectionTarget,
) -> Result<EncodedUnion<wire::InspectionTarget>, Report<WireEncodeError>> {
    match target {
        TransactionInspectionTarget::Attached => Ok(EncodedUnion::new(
            wire::InspectionTarget::AttachedTransaction,
            wire::AttachedTransaction::create(encoder.fbb(), &wire::AttachedTransactionArgs {}),
        )),
        TransactionInspectionTarget::Transaction { transaction_id } => {
            let transaction_id = encoder.text("TransactionById.transaction_id", transaction_id)?;
            let by_id = wire::TransactionById::create(
                encoder.fbb(),
                &wire::TransactionByIdArgs {
                    transaction_id: Some(transaction_id),
                },
            );
            Ok(EncodedUnion::new(
                wire::InspectionTarget::TransactionById,
                by_id,
            ))
        }
    }
}

pub(crate) fn decode_inspection_target(
    decoder: Decoder<'_>,
    request: wire::InspectTransactionRequest<'_>,
) -> Result<TransactionInspectionTarget, Report<WireDecodeError>> {
    if let Some(by_id) = request.target_as_transaction_by_id() {
        let transaction_id =
            decoder.text("TransactionById.transaction_id", by_id.transaction_id())?;
        return Ok(TransactionInspectionTarget::Transaction { transaction_id });
    }
    match request.target_type() {
        wire::InspectionTarget::AttachedTransaction => Ok(TransactionInspectionTarget::Attached),
        undeclared => Err(decoder.unknown_union("InspectTransactionRequest.target", undeclared.0)),
    }
}
