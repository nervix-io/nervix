//! Layer: engines and infrastructure.
//!
//! - **Owns.** Converting semantic Models once into programs accepted by the expression VM.
//! - **Depends on.** The vocabulary, Arrow schemas, and the VM's program types.
//! - **Must not know.** NSPL tokens or diagnostics, registry state, or runtime tasks.

use ahash::{HashSet, HashSetExt};
use arrow_schema::{DataType, Schema, TimeUnit};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    Assignment, AssignmentTarget, AssignmentTargetScope, BinaryOperator as ModelBinaryOperator,
    CaseBranch as ModelCaseBranch, Expression as ModelExpression, FieldName, FieldReference,
    FieldScope, Inheritance, Literal as ModelLiteral, ParseAsType, RouteConstruction,
    UnaryOperator as ModelUnaryOperator,
};
use thiserror::Error;

use crate::program::{
    BinaryOp, CaseArm, Expr, FieldRef, FunctionName, Invocation, Literal, Program, Span,
    SpannedExpr, SpannedInvocation, SpannedNode, UnaryOp, spanned,
};

/// Why a semantic expression or route construction cannot be represented as a VM program.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum FrontendError {
    #[error("schema field '{field}' is not a valid inherited field name")]
    InvalidInheritedFieldName { field: String },
    #[error("branch targets are valid only in branch construction")]
    BranchTargetOutsideBranchConstruction,
    #[error("SET targets unknown output field '{field}'")]
    UnknownOutputSetTarget { field: FieldName },
    #[error("branch SET targets must be bare fields or branch fields")]
    InvalidBranchSetTarget { scope: AssignmentTargetScope },
    #[error("SET targets unknown branch field '{field}'")]
    UnknownBranchSetTarget { field: FieldName },
    #[error("unknown branch field '{field}'")]
    UnknownBranchField { field: FieldName },
    #[error("branch field '{field}' is not initialized")]
    UninitializedBranchField { field: FieldName },
    #[error("unknown finalized output field '{field}'")]
    UnknownFinalizedOutputField { field: FieldName },
    #[error("unknown input field '{field}'")]
    UnknownInputField { field: FieldName },
    #[error("{scope:?} is unavailable during branch construction")]
    ScopeUnavailableDuringBranchConstruction { scope: FieldScope },
    #[error("INHERIT is not valid for set-only routes")]
    InheritInSetOnlyRoute,
    #[error("INVOKE is not valid for internal set-only routes")]
    InvokeInSetOnlyRoute,
    #[error("set-only SET targets must be bare fields or output fields")]
    InvalidSetOnlySetTarget { scope: AssignmentTargetScope },
    #[error("INHERIT is not valid for generated routes")]
    InheritInGeneratedRoute,
    #[error("INVOKE is not valid for internal generated routes")]
    InvokeInGeneratedRoute,
    #[error("generated-route SET targets must be bare fields or output fields")]
    InvalidGeneratedSetTarget { scope: AssignmentTargetScope },
    #[error("unknown output field '{field}'")]
    UnknownOutputField { field: FieldName },
    #[error("unknown generated field '{field}'")]
    UnknownGeneratedField { field: FieldName },
    #[error("output field '{field}' is not initialized")]
    UninitializedOutputField { field: FieldName },
    #[error("message is unavailable in generated route construction")]
    MessageUnavailableInGeneratedRoute,
    #[error("input is unavailable in generated route construction")]
    InputUnavailableInGeneratedRoute,
    #[error("message is unavailable in set-only route construction")]
    MessageUnavailableInSetOnlyRoute,
    #[error("input is unavailable in set-only route construction")]
    InputUnavailableInSetOnlyRoute,
    #[error("message is unavailable after set-only output finalization")]
    MessageUnavailableAfterSetOnlyFinalization,
    #[error("input is unavailable after set-only output finalization")]
    InputUnavailableAfterSetOnlyFinalization,
    #[error("required output field '{field}' remains uninitialized")]
    RequiredOutputFieldUninitialized { field: String },
    #[error("required branch field '{field}' remains uninitialized")]
    RequiredBranchFieldUninitialized { field: String },
    #[error("INHERIT ALL EXCEPT names unknown input field '{field}'")]
    UnknownInheritanceExclusion { field: FieldName },
    #[error("INHERIT names unknown input field '{field}'")]
    UnknownInheritedInputField { field: String },
    #[error("INHERIT has no same-named output field '{field}'")]
    MissingInheritedOutputField { field: String },
    #[error("INHERIT field '{field}' requires identical input and output type and nullability")]
    IncompatibleInheritedField {
        field: String,
        input_type: DataType,
        input_nullable: bool,
        output_type: DataType,
        output_nullable: bool,
    },
    #[error("working message field '{field}' is uninitialized")]
    UninitializedWorkingMessageField { field: FieldName },
    #[error("working message field '{field}' is not an output field")]
    WorkingMessageFieldNotOutput { field: FieldName },
    #[error(
        "working message field '{field}' cannot fall back to input with a different type or \
         nullability"
    )]
    IncompatibleWorkingMessageFallback {
        field: FieldName,
        input_type: DataType,
        input_nullable: bool,
        output_type: DataType,
        output_nullable: bool,
    },
    #[error("INHERIT must be expanded against the input and output schemas")]
    UnexpandedInheritance,
    #[error("array expressions are valid only in window SET values")]
    ArrayExpressionOutsideWindow,
    #[error("casts to collection types are not supported")]
    UnsupportedCollectionCast { target: ParseAsType },
}

pub type FrontendResult<T> = error_stack::Result<T, FrontendError>;

#[derive(Debug, Clone, Copy)]
enum RequiredFieldTarget {
    Output,
    Branch,
}

#[derive(Debug, Clone, Copy)]
pub struct SemanticNamespaces<'a> {
    pub bare_read: &'a str,
    pub bare_write: &'a str,
}

impl<'a> SemanticNamespaces<'a> {
    pub const fn new(bare_read: &'a str, bare_write: &'a str) -> Self {
        Self {
            bare_read,
            bare_write,
        }
    }
}

/// Lowers a transforming route after resolving ordered working-message reads and inheritance.
pub fn lower_transforming_route(
    construction: &RouteConstruction,
    input_schema: &Schema,
    output_schema: &Schema,
) -> FrontendResult<SpannedNode<Program>> {
    let mut initialized = HashSet::new();
    let mut normalized = RouteConstruction::default();
    if let Some(inherit) = &construction.inherit {
        for inherited in inherited_fields(inherit, input_schema, output_schema)? {
            initialized.insert(inherited.field.as_str().to_string());
            let input = ModelExpression::Field(FieldReference::scoped(
                FieldScope::Input,
                inherited.field.clone(),
            ));
            let value = if inherited.leak_sensitive {
                ModelExpression::Call {
                    function: nervix_models::BuiltinFunctionName::parse("leak_sensitive")
                        .assured("leak_sensitive is a language-defined built-in function"),
                    arguments: vec![input],
                }
            } else {
                input
            };
            normalized.assignments.push(Assignment {
                target: AssignmentTarget {
                    scope: AssignmentTargetScope::Output,
                    field: inherited.field,
                },
                value,
            });
        }
    }
    for assignment in &construction.assignments {
        if assignment.target.scope == AssignmentTargetScope::Branch {
            return Err(Report::new(
                FrontendError::BranchTargetOutsideBranchConstruction,
            ));
        }
        let field = assignment.target.field.as_str();
        output_schema.field_with_name(field).map_err(|_| {
            Report::new(FrontendError::UnknownOutputSetTarget {
                field: assignment.target.field.clone(),
            })
        })?;
        let value = resolve_transforming_expression(
            &assignment.value,
            &initialized,
            input_schema,
            output_schema,
            false,
        )?;
        normalized.assignments.push(Assignment {
            target: AssignmentTarget {
                scope: AssignmentTargetScope::Output,
                field: assignment.target.field.clone(),
            },
            value,
        });
        initialized.insert(field.to_string());
    }
    ensure_required_fields_initialized(&initialized, output_schema, RequiredFieldTarget::Output)?;
    normalized.where_clause = construction
        .where_clause
        .as_ref()
        .map(|expression| {
            resolve_transforming_expression(
                expression,
                &initialized,
                input_schema,
                output_schema,
                true,
            )
        })
        .transpose()?;
    normalized.invocations = construction
        .invocations
        .iter()
        .map(|invocation| {
            Ok(nervix_models::Invocation {
                function: invocation.function.clone(),
                arguments: invocation
                    .arguments
                    .iter()
                    .map(|expression| {
                        resolve_transforming_expression(
                            expression,
                            &initialized,
                            input_schema,
                            output_schema,
                            true,
                        )
                    })
                    .collect::<FrontendResult<Vec<_>>>()?,
            })
        })
        .collect::<FrontendResult<Vec<_>>>()?;
    lower_route_construction(&normalized, SemanticNamespaces::new("output", "output"))
}

/// Lowers the ordered construction of a new branch key.
///
/// Bare and `branch` reads refer only to fields initialized by an earlier assignment. `message`
/// and `output` both refer to the finalized route output, while `input` remains the original row.
pub fn lower_branch_construction(
    assignments: &[Assignment],
    branch_schema: &Schema,
    output_schema: &Schema,
    input_schema: &Schema,
) -> FrontendResult<SpannedNode<Program>> {
    let mut initialized = HashSet::new();
    let mut normalized = RouteConstruction::default();
    for assignment in assignments {
        if !matches!(
            assignment.target.scope,
            AssignmentTargetScope::Bare | AssignmentTargetScope::Branch
        ) {
            return Err(Report::new(FrontendError::InvalidBranchSetTarget {
                scope: assignment.target.scope,
            }));
        }
        let field = assignment.target.field.as_str();
        branch_schema.field_with_name(field).map_err(|_| {
            Report::new(FrontendError::UnknownBranchSetTarget {
                field: assignment.target.field.clone(),
            })
        })?;
        let value = resolve_expression(&assignment.value, &mut |reference| {
            let name = reference.field.as_str();
            match reference.scope {
                FieldScope::Bare | FieldScope::Branch => {
                    branch_schema.field_with_name(name).map_err(|_| {
                        Report::new(FrontendError::UnknownBranchField {
                            field: reference.field.clone(),
                        })
                    })?;
                    if !initialized.contains(name) {
                        return Err(Report::new(FrontendError::UninitializedBranchField {
                            field: reference.field.clone(),
                        }));
                    }
                    Ok(FieldReference::scoped(
                        FieldScope::Branch,
                        reference.field.clone(),
                    ))
                }
                FieldScope::Message | FieldScope::Output => {
                    output_schema.field_with_name(name).map_err(|_| {
                        Report::new(FrontendError::UnknownFinalizedOutputField {
                            field: reference.field.clone(),
                        })
                    })?;
                    Ok(FieldReference::scoped(
                        FieldScope::Output,
                        reference.field.clone(),
                    ))
                }
                FieldScope::Input => {
                    input_schema.field_with_name(name).map_err(|_| {
                        Report::new(FrontendError::UnknownInputField {
                            field: reference.field.clone(),
                        })
                    })?;
                    Ok(reference.clone())
                }
                FieldScope::Left
                | FieldScope::Right
                | FieldScope::Metadata
                | FieldScope::PartialOutput
                | FieldScope::Error => Err(Report::new(
                    FrontendError::ScopeUnavailableDuringBranchConstruction {
                        scope: reference.scope.clone(),
                    },
                )),
                FieldScope::RelayState { .. } => Ok(reference.clone()),
            }
        })?;
        normalized.assignments.push(Assignment {
            target: AssignmentTarget {
                scope: AssignmentTargetScope::Branch,
                field: assignment.target.field.clone(),
            },
            value,
        });
        initialized.insert(field.to_string());
    }
    ensure_required_fields_initialized(&initialized, branch_schema, RequiredFieldTarget::Branch)?;
    lower_route_construction(&normalized, SemanticNamespaces::new("branch", "branch"))
}

/// Lowers a route that starts with an empty output and has no implicit input or generated base.
///
/// This is the construction model used by generators. A bare RHS field is an ordered read of an
/// output field initialized by an earlier assignment; `message` and `input` are deliberately not
/// available.
pub fn lower_set_only_route(
    construction: &RouteConstruction,
    output_schema: &Schema,
) -> FrontendResult<SpannedNode<Program>> {
    if construction.inherit.is_some() {
        return Err(Report::new(FrontendError::InheritInSetOnlyRoute));
    }
    if !construction.invocations.is_empty() {
        return Err(Report::new(FrontendError::InvokeInSetOnlyRoute));
    }

    let mut initialized = HashSet::new();
    let mut normalized = RouteConstruction::default();
    for assignment in &construction.assignments {
        if !matches!(
            assignment.target.scope,
            AssignmentTargetScope::Bare | AssignmentTargetScope::Output
        ) {
            return Err(Report::new(FrontendError::InvalidSetOnlySetTarget {
                scope: assignment.target.scope,
            }));
        }
        let field = assignment.target.field.as_str();
        output_schema.field_with_name(field).map_err(|_| {
            Report::new(FrontendError::UnknownOutputSetTarget {
                field: assignment.target.field.clone(),
            })
        })?;
        let value =
            resolve_set_only_expression(&assignment.value, &initialized, output_schema, false)?;
        normalized.assignments.push(Assignment {
            target: AssignmentTarget {
                scope: AssignmentTargetScope::Output,
                field: assignment.target.field.clone(),
            },
            value,
        });
        initialized.insert(field.to_string());
    }
    ensure_required_fields_initialized(&initialized, output_schema, RequiredFieldTarget::Output)?;
    normalized.where_clause = construction
        .where_clause
        .as_ref()
        .map(|expression| {
            resolve_set_only_expression(expression, &initialized, output_schema, true)
        })
        .transpose()?;

    lower_route_construction(
        &normalized,
        SemanticNamespaces::new("__invalid_bare_read", "output"),
    )
}

/// Lowers a route predicate that runs after a set-only output has been finalized.
///
/// Bare and `output` reads address the finalized output. Construction-only `message` and live
/// `input` scopes remain unavailable.
pub fn lower_finalized_output_filter(
    filter: &ModelExpression,
    output_schema: &Schema,
) -> FrontendResult<SpannedNode<Program>> {
    let resolved = resolve_set_only_expression(filter, &HashSet::new(), output_schema, true)?;
    lower_route_construction(
        &RouteConstruction {
            where_clause: Some(resolved),
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("output", "__invalid_finalized_output_target"),
    )
}

/// Lowers a set-only route backed by immutable generated fields (inferencer or WASM output).
/// Bare reads prefer an earlier route-local assignment and otherwise read the generated base.
pub fn lower_generated_route(
    construction: &RouteConstruction,
    output_schema: &Schema,
    generated_schema: &Schema,
) -> FrontendResult<SpannedNode<Program>> {
    if construction.inherit.is_some() {
        return Err(Report::new(FrontendError::InheritInGeneratedRoute));
    }
    if !construction.invocations.is_empty() {
        return Err(Report::new(FrontendError::InvokeInGeneratedRoute));
    }

    let mut initialized = HashSet::new();
    let mut normalized = RouteConstruction::default();
    for assignment in &construction.assignments {
        if !matches!(
            assignment.target.scope,
            AssignmentTargetScope::Bare | AssignmentTargetScope::Output
        ) {
            return Err(Report::new(FrontendError::InvalidGeneratedSetTarget {
                scope: assignment.target.scope,
            }));
        }
        let field = assignment.target.field.as_str();
        output_schema.field_with_name(field).map_err(|_| {
            Report::new(FrontendError::UnknownOutputSetTarget {
                field: assignment.target.field.clone(),
            })
        })?;
        let value = resolve_generated_expression(
            &assignment.value,
            &initialized,
            output_schema,
            generated_schema,
            false,
        )?;
        normalized.assignments.push(Assignment {
            target: AssignmentTarget {
                scope: AssignmentTargetScope::Output,
                field: assignment.target.field.clone(),
            },
            value,
        });
        initialized.insert(field.to_string());
    }
    ensure_required_fields_initialized(&initialized, output_schema, RequiredFieldTarget::Output)?;
    normalized.where_clause = construction
        .where_clause
        .as_ref()
        .map(|expression| {
            resolve_generated_expression(
                expression,
                &initialized,
                output_schema,
                generated_schema,
                true,
            )
        })
        .transpose()?;

    lower_route_construction(&normalized, SemanticNamespaces::new("generated", "output"))
}

fn resolve_generated_expression(
    expression: &ModelExpression,
    initialized: &HashSet<String>,
    output_schema: &Schema,
    generated_schema: &Schema,
    finalized: bool,
) -> FrontendResult<ModelExpression> {
    resolve_expression(expression, &mut |reference| match reference.scope {
        FieldScope::Bare => {
            let name = reference.field.as_str();
            if finalized || initialized.contains(name) {
                output_schema.field_with_name(name).map_err(|_| {
                    Report::new(FrontendError::UnknownOutputField {
                        field: reference.field.clone(),
                    })
                })?;
                Ok(FieldReference::scoped(
                    FieldScope::Output,
                    reference.field.clone(),
                ))
            } else {
                generated_schema.field_with_name(name).map_err(|_| {
                    Report::new(FrontendError::UnknownGeneratedField {
                        field: reference.field.clone(),
                    })
                })?;
                Ok(reference.clone())
            }
        }
        FieldScope::Output => {
            let name = reference.field.as_str();
            output_schema.field_with_name(name).map_err(|_| {
                Report::new(FrontendError::UnknownOutputField {
                    field: reference.field.clone(),
                })
            })?;
            if !finalized && !initialized.contains(name) {
                return Err(Report::new(FrontendError::UninitializedOutputField {
                    field: reference.field.clone(),
                }));
            }
            Ok(reference.clone())
        }
        FieldScope::Message => Err(Report::new(
            FrontendError::MessageUnavailableInGeneratedRoute,
        )),
        FieldScope::Input => Err(Report::new(FrontendError::InputUnavailableInGeneratedRoute)),
        _ => Ok(reference.clone()),
    })
}

fn resolve_set_only_expression(
    expression: &ModelExpression,
    initialized: &HashSet<String>,
    output_schema: &Schema,
    finalized: bool,
) -> FrontendResult<ModelExpression> {
    resolve_expression(expression, &mut |reference| match reference.scope {
        FieldScope::Bare | FieldScope::Output => {
            let name = reference.field.as_str();
            output_schema.field_with_name(name).map_err(|_| {
                Report::new(FrontendError::UnknownOutputField {
                    field: reference.field.clone(),
                })
            })?;
            if !finalized && !initialized.contains(name) {
                return Err(Report::new(FrontendError::UninitializedOutputField {
                    field: reference.field.clone(),
                }));
            }
            Ok(FieldReference::scoped(
                FieldScope::Output,
                reference.field.clone(),
            ))
        }
        FieldScope::Message if finalized => Err(Report::new(
            FrontendError::MessageUnavailableAfterSetOnlyFinalization,
        )),
        FieldScope::Input if finalized => Err(Report::new(
            FrontendError::InputUnavailableAfterSetOnlyFinalization,
        )),
        FieldScope::Message => Err(Report::new(FrontendError::MessageUnavailableInSetOnlyRoute)),
        FieldScope::Input => Err(Report::new(FrontendError::InputUnavailableInSetOnlyRoute)),
        _ => Ok(reference.clone()),
    })
}

fn ensure_required_fields_initialized(
    initialized: &HashSet<String>,
    schema: &Schema,
    target: RequiredFieldTarget,
) -> FrontendResult<()> {
    for field in schema.fields() {
        if !field.is_nullable() && !initialized.contains(field.name()) {
            let error = match target {
                RequiredFieldTarget::Output => FrontendError::RequiredOutputFieldUninitialized {
                    field: field.name().clone(),
                },
                RequiredFieldTarget::Branch => FrontendError::RequiredBranchFieldUninitialized {
                    field: field.name().clone(),
                },
            };
            return Err(Report::new(error));
        }
    }
    Ok(())
}

struct InheritedName {
    field: FieldName,
    leak_sensitive: bool,
}

fn inherited_fields(
    inheritance: &Inheritance,
    input_schema: &Schema,
    output_schema: &Schema,
) -> FrontendResult<Vec<InheritedName>> {
    let selected = match inheritance {
        Inheritance::All => input_schema
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), false))
            .collect::<Vec<_>>(),
        Inheritance::AllExcept(excluded) => {
            for field in excluded {
                input_schema.field_with_name(field.as_str()).map_err(|_| {
                    Report::new(FrontendError::UnknownInheritanceExclusion {
                        field: field.clone(),
                    })
                })?;
            }
            input_schema
                .fields()
                .iter()
                .filter(|field| {
                    !excluded
                        .iter()
                        .any(|excluded| excluded.as_str() == field.name())
                })
                .map(|field| (field.name().as_str(), false))
                .collect::<Vec<_>>()
        }
        Inheritance::Fields(fields) => fields
            .iter()
            .map(|field| (field.field.as_str(), field.leak_sensitive))
            .collect::<Vec<_>>(),
    };
    selected
        .into_iter()
        .map(|(name, leak_sensitive)| {
            let input = input_schema.field_with_name(name).map_err(|_| {
                Report::new(FrontendError::UnknownInheritedInputField {
                    field: name.to_string(),
                })
            })?;
            let output = output_schema.field_with_name(name).map_err(|_| {
                Report::new(FrontendError::MissingInheritedOutputField {
                    field: name.to_string(),
                })
            })?;
            if input.data_type() != output.data_type()
                || input.is_nullable() != output.is_nullable()
            {
                return Err(Report::new(FrontendError::IncompatibleInheritedField {
                    field: name.to_string(),
                    input_type: input.data_type().clone(),
                    input_nullable: input.is_nullable(),
                    output_type: output.data_type().clone(),
                    output_nullable: output.is_nullable(),
                }));
            }
            let field = FieldName::parse(name).change_context(
                FrontendError::InvalidInheritedFieldName {
                    field: name.to_string(),
                },
            )?;
            Ok(InheritedName {
                field,
                leak_sensitive,
            })
        })
        .collect()
}

fn resolve_transforming_expression(
    expression: &ModelExpression,
    initialized: &HashSet<String>,
    input_schema: &Schema,
    output_schema: &Schema,
    finalized: bool,
) -> FrontendResult<ModelExpression> {
    resolve_expression(expression, &mut |reference| match reference.scope {
        FieldScope::Bare | FieldScope::Message => {
            let name = reference.field.as_str();
            if finalized || initialized.contains(name) {
                output_schema.field_with_name(name).map_err(|_| {
                    Report::new(FrontendError::UnknownOutputField {
                        field: reference.field.clone(),
                    })
                })?;
                Ok(FieldReference::scoped(
                    FieldScope::Output,
                    reference.field.clone(),
                ))
            } else {
                let input = input_schema.field_with_name(name).map_err(|_| {
                    Report::new(FrontendError::UninitializedWorkingMessageField {
                        field: reference.field.clone(),
                    })
                })?;
                let output = output_schema.field_with_name(name).map_err(|_| {
                    Report::new(FrontendError::WorkingMessageFieldNotOutput {
                        field: reference.field.clone(),
                    })
                })?;
                if input.data_type() != output.data_type()
                    || input.is_nullable() != output.is_nullable()
                {
                    return Err(Report::new(
                        FrontendError::IncompatibleWorkingMessageFallback {
                            field: reference.field.clone(),
                            input_type: input.data_type().clone(),
                            input_nullable: input.is_nullable(),
                            output_type: output.data_type().clone(),
                            output_nullable: output.is_nullable(),
                        },
                    ));
                }
                Ok(FieldReference::scoped(
                    FieldScope::Input,
                    reference.field.clone(),
                ))
            }
        }
        FieldScope::Output => {
            let name = reference.field.as_str();
            if !finalized && !initialized.contains(name) {
                return Err(Report::new(FrontendError::UninitializedOutputField {
                    field: reference.field.clone(),
                }));
            }
            output_schema.field_with_name(name).map_err(|_| {
                Report::new(FrontendError::UnknownOutputField {
                    field: reference.field.clone(),
                })
            })?;
            Ok(reference.clone())
        }
        FieldScope::Input => {
            input_schema
                .field_with_name(reference.field.as_str())
                .map_err(|_| {
                    Report::new(FrontendError::UnknownInputField {
                        field: reference.field.clone(),
                    })
                })?;
            Ok(reference.clone())
        }
        _ => Ok(reference.clone()),
    })
}

fn resolve_expression(
    expression: &ModelExpression,
    resolve_field: &mut impl FnMut(&FieldReference) -> FrontendResult<FieldReference>,
) -> FrontendResult<ModelExpression> {
    Ok(match expression {
        ModelExpression::Literal(_) => expression.clone(),
        ModelExpression::Field(reference) => ModelExpression::Field(resolve_field(reference)?),
        ModelExpression::Unary {
            operator,
            expression,
        } => ModelExpression::Unary {
            operator: *operator,
            expression: Box::new(resolve_expression(expression, resolve_field)?),
        },
        ModelExpression::Binary {
            operator,
            left,
            right,
        } => ModelExpression::Binary {
            operator: *operator,
            left: Box::new(resolve_expression(left, resolve_field)?),
            right: Box::new(resolve_expression(right, resolve_field)?),
        },
        ModelExpression::Cast { expression, target } => ModelExpression::Cast {
            expression: Box::new(resolve_expression(expression, resolve_field)?),
            target: target.clone(),
        },
        ModelExpression::Call {
            function,
            arguments,
        } => ModelExpression::Call {
            function: function.clone(),
            arguments: arguments
                .iter()
                .map(|argument| resolve_expression(argument, resolve_field))
                .collect::<FrontendResult<Vec<_>>>()?,
        },
        ModelExpression::UdfCall {
            function,
            arguments,
        } => ModelExpression::UdfCall {
            function: function.clone(),
            arguments: arguments
                .iter()
                .map(|argument| resolve_expression(argument, resolve_field))
                .collect::<FrontendResult<Vec<_>>>()?,
        },
        ModelExpression::Array(items) => ModelExpression::Array(
            items
                .iter()
                .map(|item| resolve_expression(item, resolve_field))
                .collect::<FrontendResult<Vec<_>>>()?,
        ),
        ModelExpression::If {
            condition,
            then_result,
            else_result,
        } => ModelExpression::If {
            condition: Box::new(resolve_expression(condition, resolve_field)?),
            then_result: Box::new(resolve_expression(then_result, resolve_field)?),
            else_result: Box::new(resolve_expression(else_result, resolve_field)?),
        },
        ModelExpression::Case {
            operand,
            branches,
            else_result,
        } => ModelExpression::Case {
            operand: operand
                .as_ref()
                .map(|operand| resolve_expression(operand, resolve_field).map(Box::new))
                .transpose()?,
            branches: branches
                .iter()
                .map(|branch| {
                    Ok(ModelCaseBranch {
                        when: resolve_expression(&branch.when, resolve_field)?,
                        result: resolve_expression(&branch.result, resolve_field)?,
                    })
                })
                .collect::<FrontendResult<Vec<_>>>()?,
            else_result: else_result
                .as_ref()
                .map(|result| resolve_expression(result, resolve_field).map(Box::new))
                .transpose()?,
        },
    })
}

pub fn lower_route_construction(
    construction: &RouteConstruction,
    namespaces: SemanticNamespaces<'_>,
) -> FrontendResult<SpannedNode<Program>> {
    if construction.inherit.is_some() {
        return Err(Report::new(FrontendError::UnexpandedInheritance));
    }
    let operation_count = construction.assignments.len()
        + usize::from(construction.where_clause.is_some())
        + construction.invocations.len();
    let span: Span = (0..operation_count
        .checked_add(1)
        .assured("the operations counted here belong to one construction held in memory"))
        .into();
    let set = construction
        .assignments
        .iter()
        .enumerate()
        .map(|(index, assignment)| {
            let relay = match assignment.target.scope {
                AssignmentTargetScope::Bare => namespaces.bare_write,
                AssignmentTargetScope::Message => "message",
                AssignmentTargetScope::Output => "output",
                AssignmentTargetScope::Branch => "branch",
            };
            Ok((
                FieldRef {
                    relay: relay.to_string(),
                    field: assignment.target.field.as_str().to_string(),
                },
                lower_expression_with_span(
                    &assignment.value,
                    namespaces.bare_read,
                    (index + 1..index + 2).into(),
                )?,
            ))
        })
        .collect::<FrontendResult<Vec<_>>>()?;
    let filter = construction
        .where_clause
        .as_ref()
        .map(|expression| {
            let index = construction.assignments.len() + 1;
            lower_expression_with_span(expression, namespaces.bare_read, (index..index + 1).into())
        })
        .transpose()?;
    let invocation_offset =
        construction.assignments.len() + usize::from(construction.where_clause.is_some()) + 1;
    let invoke = construction
        .invocations
        .iter()
        .enumerate()
        .map(|(index, invocation)| {
            let invocation_span: Span =
                (invocation_offset + index..invocation_offset + index + 1).into();
            Ok(spanned(
                Invocation {
                    function: FunctionName::parse(invocation.function.as_str()),
                    args: invocation
                        .arguments
                        .iter()
                        .map(|argument| {
                            lower_expression_with_span(
                                argument,
                                namespaces.bare_read,
                                invocation_span,
                            )
                        })
                        .collect::<FrontendResult<Vec<_>>>()?,
                },
                invocation_span,
            ))
        })
        .collect::<FrontendResult<Vec<SpannedInvocation>>>()?;
    Ok(spanned(
        Program {
            filter,
            set,
            invoke,
        },
        span,
    ))
}

pub fn lower_expression(
    expression: &ModelExpression,
    bare_read_namespace: &str,
) -> FrontendResult<SpannedExpr> {
    let span: Span = (0..0).into();
    lower_expression_with_span(expression, bare_read_namespace, span)
}

fn lower_expression_with_span(
    expression: &ModelExpression,
    bare_read_namespace: &str,
    span: Span,
) -> FrontendResult<SpannedExpr> {
    let expression = match expression {
        ModelExpression::Literal(literal) => Expr::Literal(match literal {
            ModelLiteral::I64(value) => Literal::Int64(*value),
            ModelLiteral::F64(value) => Literal::Float64(value.value()),
            ModelLiteral::Bool(value) => Literal::Bool(*value),
            ModelLiteral::String(value) => Literal::String(value.clone()),
            ModelLiteral::Null => Literal::Null,
        }),
        ModelExpression::Field(reference) => {
            let relay = match &reference.scope {
                FieldScope::Bare => bare_read_namespace.to_string(),
                FieldScope::Message => "message".to_string(),
                FieldScope::Input => "input".to_string(),
                FieldScope::Output => "output".to_string(),
                FieldScope::Branch => "branch".to_string(),
                FieldScope::Left => "left".to_string(),
                FieldScope::Right => "right".to_string(),
                FieldScope::RelayState { relay } => {
                    format!("relay_state.{}", relay.as_str())
                }
                FieldScope::Metadata => "metadata".to_string(),
                FieldScope::PartialOutput => "partial_output".to_string(),
                FieldScope::Error => "error".to_string(),
            };
            Expr::FieldRef(FieldRef {
                relay,
                field: reference.field.as_str().to_string(),
            })
        }
        ModelExpression::Unary {
            operator,
            expression,
        } => Expr::Unary {
            op: match operator {
                ModelUnaryOperator::Negate => UnaryOp::Neg,
                ModelUnaryOperator::Not => UnaryOp::Not,
            },
            expr: Box::new(lower_expression_with_span(
                expression,
                bare_read_namespace,
                span,
            )?),
        },
        ModelExpression::Binary {
            operator,
            left,
            right,
        } => Expr::Binary {
            op: match operator {
                ModelBinaryOperator::Add => BinaryOp::Add,
                ModelBinaryOperator::Subtract => BinaryOp::Sub,
                ModelBinaryOperator::Multiply => BinaryOp::Mul,
                ModelBinaryOperator::Divide => BinaryOp::Div,
                ModelBinaryOperator::Remainder => BinaryOp::Rem,
                ModelBinaryOperator::Equal => BinaryOp::Eq,
                ModelBinaryOperator::NotEqual => BinaryOp::NotEq,
                ModelBinaryOperator::GreaterThan => BinaryOp::Gt,
                ModelBinaryOperator::LessThan => BinaryOp::Lt,
                ModelBinaryOperator::GreaterThanOrEqual => BinaryOp::GtEq,
                ModelBinaryOperator::LessThanOrEqual => BinaryOp::LtEq,
                ModelBinaryOperator::And => BinaryOp::And,
                ModelBinaryOperator::Or => BinaryOp::Or,
            },
            left: Box::new(lower_expression_with_span(left, bare_read_namespace, span)?),
            right: Box::new(lower_expression_with_span(
                right,
                bare_read_namespace,
                span,
            )?),
        },
        ModelExpression::Cast { expression, target } => Expr::Cast {
            expr: Box::new(lower_expression_with_span(
                expression,
                bare_read_namespace,
                span,
            )?),
            data_type: scalar_data_type(target)?,
        },
        ModelExpression::Call {
            function,
            arguments,
        } => Expr::Call {
            function: FunctionName::parse(function.as_str()),
            args: arguments
                .iter()
                .map(|argument| lower_expression_with_span(argument, bare_read_namespace, span))
                .collect::<FrontendResult<Vec<_>>>()?,
        },
        ModelExpression::UdfCall {
            function,
            arguments,
        } => Expr::Call {
            function: FunctionName::Udf(function.as_str().to_string()),
            args: arguments
                .iter()
                .map(|argument| lower_expression_with_span(argument, bare_read_namespace, span))
                .collect::<FrontendResult<Vec<_>>>()?,
        },
        ModelExpression::Array(_) => {
            return Err(Report::new(FrontendError::ArrayExpressionOutsideWindow));
        }
        ModelExpression::If {
            condition,
            then_result,
            else_result,
        } => Expr::Case {
            operand: None,
            branches: vec![CaseArm {
                when: lower_expression_with_span(condition, bare_read_namespace, span)?,
                result: lower_expression_with_span(then_result, bare_read_namespace, span)?,
            }],
            else_result: Some(Box::new(lower_expression_with_span(
                else_result,
                bare_read_namespace,
                span,
            )?)),
        },
        ModelExpression::Case {
            operand,
            branches,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|operand| {
                    lower_expression_with_span(operand, bare_read_namespace, span).map(Box::new)
                })
                .transpose()?,
            branches: branches
                .iter()
                .map(|branch| {
                    Ok(CaseArm {
                        when: lower_expression_with_span(&branch.when, bare_read_namespace, span)?,
                        result: lower_expression_with_span(
                            &branch.result,
                            bare_read_namespace,
                            span,
                        )?,
                    })
                })
                .collect::<FrontendResult<Vec<_>>>()?,
            else_result: else_result
                .as_ref()
                .map(|result| {
                    lower_expression_with_span(result, bare_read_namespace, span).map(Box::new)
                })
                .transpose()?,
        },
    };
    Ok(spanned(expression, span))
}

fn scalar_data_type(target: &ParseAsType) -> FrontendResult<DataType> {
    match target {
        ParseAsType::U8 => Ok(DataType::UInt8),
        ParseAsType::I8 => Ok(DataType::Int8),
        ParseAsType::U16 => Ok(DataType::UInt16),
        ParseAsType::I16 => Ok(DataType::Int16),
        ParseAsType::U32 => Ok(DataType::UInt32),
        ParseAsType::I32 => Ok(DataType::Int32),
        ParseAsType::U64 => Ok(DataType::UInt64),
        ParseAsType::I64 => Ok(DataType::Int64),
        ParseAsType::Bool => Ok(DataType::Boolean),
        ParseAsType::String => Ok(DataType::Utf8),
        ParseAsType::Datetime => Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("+00:00".into()),
        )),
        ParseAsType::F32 => Ok(DataType::Float32),
        ParseAsType::F64 => Ok(DataType::Float64),
        ParseAsType::Array { .. } | ParseAsType::Vec { .. } => {
            Err(Report::new(FrontendError::UnsupportedCollectionCast {
                target: target.clone(),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;

    use super::*;

    fn field(name: &str) -> FieldName {
        FieldName::parse(name).assured("test field names use the language name alphabet")
    }

    #[test]
    fn unknown_transforming_set_target_carries_the_typed_field() {
        let missing = field("missing");
        let construction = RouteConstruction {
            assignments: vec![Assignment {
                target: AssignmentTarget::bare(missing.clone()),
                value: ModelExpression::Literal(ModelLiteral::I64(1)),
            }],
            ..RouteConstruction::default()
        };

        let error = lower_transforming_route(&construction, &Schema::empty(), &Schema::empty())
            .expect_err("an unknown output field must be rejected");

        assert_eq!(
            error.current_context(),
            &FrontendError::UnknownOutputSetTarget { field: missing }
        );
    }

    #[test]
    fn inheritance_type_mismatch_carries_both_arrow_contracts() {
        let input = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let output = Schema::new(vec![Field::new("value", DataType::Utf8, true)]);
        let construction = RouteConstruction {
            inherit: Some(Inheritance::All),
            ..RouteConstruction::default()
        };

        let error = lower_transforming_route(&construction, &input, &output)
            .expect_err("different field contracts must not be inherited");

        assert_eq!(
            error.current_context(),
            &FrontendError::IncompatibleInheritedField {
                field: "value".to_string(),
                input_type: DataType::Int64,
                input_nullable: false,
                output_type: DataType::Utf8,
                output_nullable: true,
            }
        );
    }
}
