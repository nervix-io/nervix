//! The outcomes of commands and transaction attachment.

use error_stack::Report;
use flatbuffers::WIPOffset;
use nervix_models::{
    CommandExecutionReference, TransactionOperationAdmission, TransactionPreviewIdentity,
    TransactionStatus,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    common::{Diagnostic, LeaderRedirect, OutcomeOrigin},
    transaction::{
        decode_operation_number, decode_preview_identity, decode_transaction_status,
        encode_operation_number, encode_preview_identity, encode_transaction_status,
    },
    wire,
};

/// Why an admitted command's outcome is not known yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnknownOutcomeCause {
    /// Leadership moved after the command was durably admitted.
    LeadershipLost,
    /// The command is durably admitted and still applying.
    StillApplying,
    /// The command finished, but its outcome is not yet authoritative on every live node.
    NotYetAuthoritative,
}

wire_enum!(ALL_UNKNOWN_OUTCOME_CAUSES: UnknownOutcomeCause => wire::UnknownOutcomeCause {
    LeadershipLost,
    StillApplying,
    NotYetAuthoritative,
});

/// How a reused execution reference differs from the command it already identifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionReferenceConflict {
    Content,
    Domain,
    Owner,
    Position,
}

wire_enum!(ALL_EXECUTION_REFERENCE_CONFLICTS: ExecutionReferenceConflict => wire::ExecutionReferenceConflictKind {
    Content,
    Domain,
    Owner,
    Position,
});

/// What became of a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandDisposition {
    Completed {
        /// The statement found the entity it creates already present and changed nothing.
        already_existed: bool,
    },
    /// The command failed definitively; the outcome's message and diagnostics say why.
    Failed,
    NotLeader(LeaderRedirect),
    /// The serving leader holds no binding for the session's transaction. Attach it again and
    /// retry.
    TransactionDetached {
        transaction_id: String,
    },
    /// Another session attached the transaction this session was bound to.
    TransactionTakenOver {
        transaction_id: String,
    },
    /// The command was durably admitted and its outcome is not known yet. Retry with the same
    /// execution reference to recover it.
    OutcomeUnknown(UnknownOutcomeCause),
    /// The execution reference already identifies a different command.
    ExecutionReferenceConflict(ExecutionReferenceConflict),
    /// The execution reference aged out of execution history; its outcome can no longer be
    /// recovered and the command was not executed again.
    ExecutionReferenceExpired,
    /// The COMMIT expected a preview that no longer describes the transaction. Nothing applied and
    /// the transaction stays open.
    PreviewStale {
        expected: TransactionPreviewIdentity,
        current: TransactionPreviewIdentity,
    },
}

impl CommandDisposition {
    fn encode(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::CommandDisposition>, Report<WireEncodeError>> {
        let union = match self {
            Self::Completed { already_existed } => EncodedUnion::new(
                wire::CommandDisposition::CommandCompleted,
                encode_completed(encoder, *already_existed),
            ),
            Self::Failed => EncodedUnion::new(
                wire::CommandDisposition::RequestFailed,
                wire::RequestFailed::create(encoder.fbb(), &wire::RequestFailedArgs {}),
            ),
            Self::NotLeader(redirect) => EncodedUnion::new(
                wire::CommandDisposition::LeaderRedirect,
                redirect.encode(encoder)?,
            ),
            Self::TransactionDetached { transaction_id } => {
                let transaction_id =
                    encoder.text("TransactionDetached.transaction_id", transaction_id)?;
                let detached = wire::TransactionDetached::create(
                    encoder.fbb(),
                    &wire::TransactionDetachedArgs {
                        transaction_id: Some(transaction_id),
                    },
                );
                EncodedUnion::new(wire::CommandDisposition::TransactionDetached, detached)
            }
            Self::TransactionTakenOver { transaction_id } => {
                let transaction_id =
                    encoder.text("TransactionTakenOver.transaction_id", transaction_id)?;
                let taken_over = wire::TransactionTakenOver::create(
                    encoder.fbb(),
                    &wire::TransactionTakenOverArgs {
                        transaction_id: Some(transaction_id),
                    },
                );
                EncodedUnion::new(wire::CommandDisposition::TransactionTakenOver, taken_over)
            }
            Self::OutcomeUnknown(cause) => {
                let unknown = wire::OutcomeUnknown::create(
                    encoder.fbb(),
                    &wire::OutcomeUnknownArgs {
                        cause: Some((*cause).into()),
                    },
                );
                EncodedUnion::new(wire::CommandDisposition::OutcomeUnknown, unknown)
            }
            Self::ExecutionReferenceConflict(conflict) => {
                let conflict = wire::ExecutionReferenceConflict::create(
                    encoder.fbb(),
                    &wire::ExecutionReferenceConflictArgs {
                        conflict: Some((*conflict).into()),
                    },
                );
                EncodedUnion::new(
                    wire::CommandDisposition::ExecutionReferenceConflict,
                    conflict,
                )
            }
            Self::ExecutionReferenceExpired => EncodedUnion::new(
                wire::CommandDisposition::ExecutionReferenceExpired,
                wire::ExecutionReferenceExpired::create(
                    encoder.fbb(),
                    &wire::ExecutionReferenceExpiredArgs {},
                ),
            ),
            Self::PreviewStale { expected, current } => {
                let expected = encode_preview_identity(encoder, expected)?;
                let current = encode_preview_identity(encoder, current)?;
                let stale = wire::PreviewStale::create(
                    encoder.fbb(),
                    &wire::PreviewStaleArgs {
                        expected: Some(expected),
                        current: Some(current),
                    },
                );
                EncodedUnion::new(wire::CommandDisposition::PreviewStale, stale)
            }
        };
        Ok(union)
    }

    fn decode(
        decoder: Decoder<'_>,
        outcome: wire::CommandOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        if let Some(completed) = outcome.disposition_as_command_completed() {
            return Ok(Self::Completed {
                already_existed: completed.already_existed(),
            });
        }
        if let Some(redirect) = outcome.disposition_as_leader_redirect() {
            return Ok(Self::NotLeader(LeaderRedirect::decode(decoder, redirect)?));
        }
        if let Some(detached) = outcome.disposition_as_transaction_detached() {
            let transaction_id = decoder.text(
                "TransactionDetached.transaction_id",
                detached.transaction_id(),
            )?;
            return Ok(Self::TransactionDetached { transaction_id });
        }
        if let Some(taken_over) = outcome.disposition_as_transaction_taken_over() {
            let transaction_id = decoder.text(
                "TransactionTakenOver.transaction_id",
                taken_over.transaction_id(),
            )?;
            return Ok(Self::TransactionTakenOver { transaction_id });
        }
        if let Some(unknown) = outcome.disposition_as_outcome_unknown() {
            let cause = decoder.required_enumeration("OutcomeUnknown.cause", unknown.cause())?;
            return Ok(Self::OutcomeUnknown(cause));
        }
        if let Some(conflict) = outcome.disposition_as_execution_reference_conflict() {
            let conflict = decoder
                .required_enumeration("ExecutionReferenceConflict.conflict", conflict.conflict())?;
            return Ok(Self::ExecutionReferenceConflict(conflict));
        }
        if let Some(stale) = outcome.disposition_as_preview_stale() {
            return Ok(Self::PreviewStale {
                expected: decode_preview_identity(decoder, stale.expected())?,
                current: decode_preview_identity(decoder, stale.current())?,
            });
        }
        match outcome.disposition_type() {
            wire::CommandDisposition::RequestFailed => Ok(Self::Failed),
            wire::CommandDisposition::ExecutionReferenceExpired => {
                Ok(Self::ExecutionReferenceExpired)
            }
            undeclared => Err(decoder.unknown_union("CommandOutcome.disposition", undeclared.0)),
        }
    }
}

fn encode_completed<'fbb>(
    encoder: &mut Encoder<'fbb>,
    already_existed: bool,
) -> WIPOffset<wire::CommandCompleted<'fbb>> {
    wire::CommandCompleted::create(
        encoder.fbb(),
        &wire::CommandCompletedArgs { already_existed },
    )
}

/// What became of one statement of a multi-statement command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementDisposition {
    Completed { already_existed: bool },
    Failed,
    NotLeader(LeaderRedirect),
}

/// The outcome of one statement of a multi-statement command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementOutcome {
    pub disposition: StatementDisposition,
    pub message: String,
    pub diagnostics: Vec<Diagnostic>,
}

impl StatementOutcome {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::StatementOutcome<'fbb>>, Report<WireEncodeError>> {
        let disposition = match &self.disposition {
            StatementDisposition::Completed { already_existed } => EncodedUnion::new(
                wire::StatementDisposition::CommandCompleted,
                encode_completed(encoder, *already_existed),
            ),
            StatementDisposition::Failed => EncodedUnion::new(
                wire::StatementDisposition::RequestFailed,
                wire::RequestFailed::create(encoder.fbb(), &wire::RequestFailedArgs {}),
            ),
            StatementDisposition::NotLeader(redirect) => EncodedUnion::new(
                wire::StatementDisposition::LeaderRedirect,
                redirect.encode(encoder)?,
            ),
        };
        let message = encoder.text("StatementOutcome.message", &self.message)?;
        let diagnostics =
            Diagnostic::encode_all(encoder, "StatementOutcome.diagnostics", &self.diagnostics)?;
        Ok(wire::StatementOutcome::create(
            encoder.fbb(),
            &wire::StatementOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
                diagnostics: Some(diagnostics),
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        outcome: wire::StatementOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let disposition = if let Some(completed) = outcome.disposition_as_command_completed() {
            StatementDisposition::Completed {
                already_existed: completed.already_existed(),
            }
        } else if let Some(redirect) = outcome.disposition_as_leader_redirect() {
            StatementDisposition::NotLeader(LeaderRedirect::decode(decoder, redirect)?)
        } else if let wire::StatementDisposition::RequestFailed = outcome.disposition_type() {
            StatementDisposition::Failed
        } else {
            return Err(
                decoder.unknown_union("StatementOutcome.disposition", outcome.disposition_type().0)
            );
        };
        let message = decoder.text("StatementOutcome.message", outcome.message())?;
        let diagnostics = Diagnostic::decode_all(
            decoder,
            "StatementOutcome.diagnostics",
            outcome.diagnostics(),
        )?;
        Ok(Self {
            disposition,
            message,
            diagnostics,
        })
    }
}

/// The outcome of a command request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutcome {
    /// The execution reference of the request this outcome answers.
    pub execution_reference: CommandExecutionReference,
    pub origin: OutcomeOrigin,
    pub disposition: CommandDisposition,
    pub message: String,
    pub diagnostics: Vec<Diagnostic>,
    /// The outcome of each statement of a multi-statement command, in written order. Empty for a
    /// single statement.
    pub statements: Vec<StatementOutcome>,
    /// The transaction this session was bound to while serving the command.
    pub transaction: Option<TransactionStatus>,
    /// Stable operation metadata when the command accepted one transaction append.
    pub transaction_admission: Option<TransactionOperationAdmission>,
}

impl CommandOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let execution_reference = encoder.text(
            "CommandOutcome.execution_reference",
            self.execution_reference.as_str(),
        )?;
        let disposition = self.disposition.encode(encoder)?;
        let message = encoder.text("CommandOutcome.message", &self.message)?;
        let diagnostics =
            Diagnostic::encode_all(encoder, "CommandOutcome.diagnostics", &self.diagnostics)?;
        let statements = encoder.table_vector(
            "CommandOutcome.statements",
            &self.statements,
            StatementOutcome::encode,
        )?;
        let transaction = match &self.transaction {
            Some(transaction) => Some(encode_transaction_status(encoder, transaction)?),
            None => None,
        };
        let transaction_admission = match &self.transaction_admission {
            Some(admission) => {
                let preview = encode_preview_identity(encoder, &admission.preview)?;
                Some(wire::TransactionOperationAdmission::create(
                    encoder.fbb(),
                    &wire::TransactionOperationAdmissionArgs {
                        operation: encode_operation_number(admission.operation),
                        preview: Some(preview),
                    },
                ))
            }
            None => None,
        };
        let outcome = wire::CommandOutcome::create(
            encoder.fbb(),
            &wire::CommandOutcomeArgs {
                execution_reference: Some(execution_reference),
                origin: Some(self.origin.into()),
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
                diagnostics: Some(diagnostics),
                statements: Some(statements),
                transaction,
                transaction_admission,
            },
        );
        Ok(EncodedUnion::new(wire::ReplyBody::CommandOutcome, outcome))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::CommandOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let execution_reference = decoder.check_text(
            "CommandOutcome.execution_reference",
            outcome.execution_reference(),
        )?;
        let execution_reference = match CommandExecutionReference::parse(execution_reference) {
            Ok(reference) => reference,
            Err(error) => {
                return Err(error.change_context(WireDecodeError::InvalidValue {
                    field: "CommandOutcome.execution_reference",
                    kind: "execution reference",
                }));
            }
        };
        let origin = decoder.required_enumeration("CommandOutcome.origin", outcome.origin())?;
        let disposition = CommandDisposition::decode(decoder, outcome)?;
        let message = decoder.text("CommandOutcome.message", outcome.message())?;
        let diagnostics =
            Diagnostic::decode_all(decoder, "CommandOutcome.diagnostics", outcome.diagnostics())?;
        let statements = decoder.table_vector(
            "CommandOutcome.statements",
            outcome.statements(),
            |statement| StatementOutcome::decode(decoder, statement),
        )?;
        let transaction = match outcome.transaction() {
            Some(transaction) => Some(decode_transaction_status(decoder, transaction)?),
            None => None,
        };
        let transaction_admission = match outcome.transaction_admission() {
            Some(admission) => Some(TransactionOperationAdmission {
                operation: decode_operation_number(
                    decoder,
                    "TransactionOperationAdmission.operation",
                    admission.operation(),
                )?,
                preview: decode_preview_identity(decoder, admission.preview())?,
            }),
            None => None,
        };
        Ok(Self {
            execution_reference,
            origin,
            disposition,
            message,
            diagnostics,
            statements,
            transaction,
            transaction_admission,
        })
    }
}

/// What became of an attach request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachDisposition {
    /// The transaction is now bound to this session.
    Attached(TransactionStatus),
    /// The transaction already finished; it stays unattached and its final status is reported.
    AlreadyFinished(TransactionStatus),
    /// The attach failed definitively; the outcome's message and diagnostics say why.
    Failed,
    NotLeader(LeaderRedirect),
}

/// The outcome of an attach request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachOutcome {
    pub disposition: AttachDisposition,
    pub message: String,
    pub diagnostics: Vec<Diagnostic>,
}

impl AttachOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let disposition = match &self.disposition {
            AttachDisposition::Attached(transaction) => {
                let transaction = encode_transaction_status(encoder, transaction)?;
                let attached = wire::TransactionAttached::create(
                    encoder.fbb(),
                    &wire::TransactionAttachedArgs {
                        transaction: Some(transaction),
                    },
                );
                EncodedUnion::new(wire::AttachDisposition::TransactionAttached, attached)
            }
            AttachDisposition::AlreadyFinished(transaction) => {
                let transaction = encode_transaction_status(encoder, transaction)?;
                let finished = wire::TransactionAlreadyFinished::create(
                    encoder.fbb(),
                    &wire::TransactionAlreadyFinishedArgs {
                        transaction: Some(transaction),
                    },
                );
                EncodedUnion::new(
                    wire::AttachDisposition::TransactionAlreadyFinished,
                    finished,
                )
            }
            AttachDisposition::Failed => EncodedUnion::new(
                wire::AttachDisposition::RequestFailed,
                wire::RequestFailed::create(encoder.fbb(), &wire::RequestFailedArgs {}),
            ),
            AttachDisposition::NotLeader(redirect) => EncodedUnion::new(
                wire::AttachDisposition::LeaderRedirect,
                redirect.encode(encoder)?,
            ),
        };
        let message = encoder.text("AttachOutcome.message", &self.message)?;
        let diagnostics =
            Diagnostic::encode_all(encoder, "AttachOutcome.diagnostics", &self.diagnostics)?;
        let outcome = wire::AttachOutcome::create(
            encoder.fbb(),
            &wire::AttachOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
                diagnostics: Some(diagnostics),
            },
        );
        Ok(EncodedUnion::new(wire::ReplyBody::AttachOutcome, outcome))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::AttachOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let disposition = if let Some(attached) = outcome.disposition_as_transaction_attached() {
            AttachDisposition::Attached(decode_transaction_status(decoder, attached.transaction())?)
        } else if let Some(finished) = outcome.disposition_as_transaction_already_finished() {
            AttachDisposition::AlreadyFinished(decode_transaction_status(
                decoder,
                finished.transaction(),
            )?)
        } else if let Some(redirect) = outcome.disposition_as_leader_redirect() {
            AttachDisposition::NotLeader(LeaderRedirect::decode(decoder, redirect)?)
        } else if let wire::AttachDisposition::RequestFailed = outcome.disposition_type() {
            AttachDisposition::Failed
        } else {
            return Err(
                decoder.unknown_union("AttachOutcome.disposition", outcome.disposition_type().0)
            );
        };
        let message = decoder.text("AttachOutcome.message", outcome.message())?;
        let diagnostics =
            Diagnostic::decode_all(decoder, "AttachOutcome.diagnostics", outcome.diagnostics())?;
        Ok(Self {
            disposition,
            message,
            diagnostics,
        })
    }
}
