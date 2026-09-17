//! Transaction status, the preview identity a commit expects, and the target an inspection reads.

use std::num::NonZeroUsize;

use error_stack::Report;
use flatbuffers::WIPOffset;
use meticulous::OptionExt as _;
use nervix_models::{
    DomainName, ImpactPlanningBasis, TransactionInspectionTarget, TransactionOperationNumber,
    TransactionPosition, TransactionPreviewIdentity,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_size},
    common::WireValueError,
    wire,
};

/// Where a replicated transaction is in its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    Open,
    Committing,
    Committed,
    Failed {
        /// The operation whose execution step failed.
        failing_operation: TransactionOperationNumber,
        error: String,
    },
    Reverted,
    Expired,
}

impl TransactionState {
    /// Whether the transaction can still change: it is open or committing.
    pub const fn is_active(&self) -> bool {
        matches!(self, Self::Open | Self::Committing)
    }

    fn encode(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::TransactionState>, Report<WireEncodeError>> {
        let union = match self {
            Self::Open => EncodedUnion::new(
                wire::TransactionState::TransactionOpen,
                wire::TransactionOpen::create(encoder.fbb(), &wire::TransactionOpenArgs {}),
            ),
            Self::Committing => EncodedUnion::new(
                wire::TransactionState::TransactionCommitting,
                wire::TransactionCommitting::create(
                    encoder.fbb(),
                    &wire::TransactionCommittingArgs {},
                ),
            ),
            Self::Committed => EncodedUnion::new(
                wire::TransactionState::TransactionCommitted,
                wire::TransactionCommitted::create(
                    encoder.fbb(),
                    &wire::TransactionCommittedArgs {},
                ),
            ),
            Self::Failed {
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
            Self::Reverted => EncodedUnion::new(
                wire::TransactionState::TransactionReverted,
                wire::TransactionReverted::create(encoder.fbb(), &wire::TransactionRevertedArgs {}),
            ),
            Self::Expired => EncodedUnion::new(
                wire::TransactionState::TransactionExpired,
                wire::TransactionExpired::create(encoder.fbb(), &wire::TransactionExpiredArgs {}),
            ),
        };
        Ok(union)
    }

    fn decode(
        decoder: Decoder<'_>,
        status: wire::TransactionStatus<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let state = status.state_type();
        if let Some(failed) = status.state_as_transaction_failed() {
            let failing_operation = decode_operation_number(
                decoder,
                "TransactionFailed.failing_operation",
                failed.failing_operation(),
            )?;
            let error = decoder.text("TransactionFailed.error", failed.error())?;
            return Ok(Self::Failed {
                failing_operation,
                error,
            });
        }
        match state {
            wire::TransactionState::TransactionOpen => Ok(Self::Open),
            wire::TransactionState::TransactionCommitting => Ok(Self::Committing),
            wire::TransactionState::TransactionCommitted => Ok(Self::Committed),
            wire::TransactionState::TransactionReverted => Ok(Self::Reverted),
            wire::TransactionState::TransactionExpired => Ok(Self::Expired),
            undeclared => Err(decoder.unknown_union("TransactionStatus.state", undeclared.0)),
        }
    }
}

/// A replicated transaction as the serving leader records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionStatus {
    transaction_id: String,
    domain: DomainName,
    state: TransactionState,
    accepted_operations: TransactionPosition,
    applied_operations: usize,
}

impl TransactionStatus {
    pub fn new(
        transaction_id: String,
        domain: DomainName,
        state: TransactionState,
        accepted_operations: TransactionPosition,
        applied_operations: usize,
    ) -> Result<Self, Report<WireValueError>> {
        if applied_operations > accepted_operations.accepted_operations() {
            return Err(Report::new(
                WireValueError::AppliedOperationsExceedAccepted {
                    applied: applied_operations,
                    accepted: accepted_operations.accepted_operations(),
                },
            ));
        }
        Ok(Self {
            transaction_id,
            domain,
            state,
            accepted_operations,
            applied_operations,
        })
    }

    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    pub fn domain(&self) -> &DomainName {
        &self.domain
    }

    pub fn state(&self) -> &TransactionState {
        &self.state
    }

    /// Operations accepted into the transaction, which is also the position the next append
    /// expects.
    pub const fn accepted_operations(&self) -> TransactionPosition {
        self.accepted_operations
    }

    /// Accepted operations whose execution steps have applied.
    pub const fn applied_operations(&self) -> usize {
        self.applied_operations
    }

    /// Accepted operations still waiting to apply. A finished transaction has none.
    pub fn pending_operations(&self) -> usize {
        if !self.state.is_active() {
            return 0;
        }
        self.accepted_operations
            .accepted_operations()
            .checked_sub(self.applied_operations)
            .assured("construction rejects applied operations above accepted operations")
    }

    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::TransactionStatus<'fbb>>, Report<WireEncodeError>> {
        let transaction_id =
            encoder.text("TransactionStatus.transaction_id", &self.transaction_id)?;
        let domain = encoder.text("TransactionStatus.domain", self.domain.as_str())?;
        let state = self.state.encode(encoder)?;
        Ok(wire::TransactionStatus::create(
            encoder.fbb(),
            &wire::TransactionStatusArgs {
                transaction_id: Some(transaction_id),
                domain: Some(domain),
                state_type: state.discriminant,
                state: Some(state.value),
                accepted_operations: wire_size(self.accepted_operations.accepted_operations()),
                applied_operations: wire_size(self.applied_operations),
            },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        status: wire::TransactionStatus<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let transaction_id =
            decoder.text("TransactionStatus.transaction_id", status.transaction_id())?;
        let domain = decoder.name("TransactionStatus.domain", status.domain())?;
        let state = TransactionState::decode(decoder, status)?;
        let accepted_operations = decoder.size(
            "TransactionStatus.accepted_operations",
            status.accepted_operations(),
        )?;
        let applied_operations = decoder.size(
            "TransactionStatus.applied_operations",
            status.applied_operations(),
        )?;
        match Self::new(
            transaction_id,
            domain,
            state,
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
    let planning_basis = wire::Fingerprint::new(preview.planning_basis.fingerprint());
    Ok(wire::TransactionPreviewIdentity::create(
        encoder.fbb(),
        &wire::TransactionPreviewIdentityArgs {
            transaction_id: Some(transaction_id),
            position: wire_size(preview.position.accepted_operations()),
            planning_basis: Some(&planning_basis),
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
    let planning_basis = <[u8; 32]>::from(preview.planning_basis().bytes());
    let planning_basis = ImpactPlanningBasis::new(planning_basis);
    Ok(TransactionPreviewIdentity {
        transaction_id,
        position: TransactionPosition::new(position),
        planning_basis,
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
