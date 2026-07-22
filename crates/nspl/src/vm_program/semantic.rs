use ahash_compile_time::{HashSet, HashSetExt};
use arrow_schema::{DataType, Schema, TimeUnit};
use nervix_models::{
    Assignment, AssignmentTarget, AssignmentTargetScope, BinaryOperator as ModelBinaryOperator,
    Expression as ModelExpression, FieldReference, FieldScope, Inheritance,
    Literal as ModelLiteral, ParseAsType, RouteConstruction, UnaryOperator as ModelUnaryOperator,
};

use super::{
    BinaryOp, Expr, FieldRef, FunctionName, Invocation, Literal, Program, Span, SpannedExpr,
    SpannedInvocation, SpannedNode, UnaryOp, ast::spanned,
};

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
) -> Result<SpannedNode<Program>, String> {
    let mut initialized = HashSet::new();
    let mut normalized = RouteConstruction::default();
    if let Some(inherit) = &construction.inherit {
        for inherited in inherited_fields(inherit, input_schema, output_schema)? {
            initialized.insert(inherited.field.clone());
            let input = ModelExpression::Field(FieldReference::scoped(
                FieldScope::Input,
                nervix_models::Identifier::parse(&inherited.field)
                    .map_err(|error| error.to_string())?,
            ));
            let value = if inherited.leak_sensitive {
                ModelExpression::Call {
                    function: nervix_models::Identifier::parse("leak_sensitive")
                        .map_err(|error| error.to_string())?,
                    arguments: vec![input],
                }
            } else {
                input
            };
            normalized.assignments.push(Assignment {
                target: AssignmentTarget {
                    scope: AssignmentTargetScope::Output,
                    field: nervix_models::Identifier::parse(&inherited.field)
                        .map_err(|error| error.to_string())?,
                },
                value,
            });
        }
    }
    for assignment in &construction.assignments {
        if assignment.target.scope == AssignmentTargetScope::Branch {
            return Err("branch targets are valid only in branch construction".to_string());
        }
        let field = assignment.target.field.as_str();
        output_schema
            .field_with_name(field)
            .map_err(|_| format!("SET targets unknown output field '{field}'"))?;
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
    ensure_required_fields_initialized(&initialized, output_schema, "output")?;
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
                    .collect::<Result<Vec<_>, String>>()?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
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
) -> Result<SpannedNode<Program>, String> {
    let mut initialized = HashSet::new();
    let mut normalized = RouteConstruction::default();
    for assignment in assignments {
        if !matches!(
            assignment.target.scope,
            AssignmentTargetScope::Bare | AssignmentTargetScope::Branch
        ) {
            return Err("branch SET targets must be bare fields or branch fields".to_string());
        }
        let field = assignment.target.field.as_str();
        branch_schema
            .field_with_name(field)
            .map_err(|_| format!("SET targets unknown branch field '{field}'"))?;
        let value = resolve_expression(&assignment.value, &mut |reference| {
            let name = reference.field.as_str();
            match reference.scope {
                FieldScope::Bare | FieldScope::Branch => {
                    branch_schema
                        .field_with_name(name)
                        .map_err(|_| format!("unknown branch field '{name}'"))?;
                    if !initialized.contains(name) {
                        return Err(format!("branch field '{name}' is not initialized"));
                    }
                    Ok(FieldReference::scoped(
                        FieldScope::Branch,
                        reference.field.clone(),
                    ))
                }
                FieldScope::Message | FieldScope::Output => {
                    output_schema
                        .field_with_name(name)
                        .map_err(|_| format!("unknown finalized output field '{name}'"))?;
                    Ok(FieldReference::scoped(
                        FieldScope::Output,
                        reference.field.clone(),
                    ))
                }
                FieldScope::Input => {
                    input_schema
                        .field_with_name(name)
                        .map_err(|_| format!("unknown input field '{name}'"))?;
                    Ok(reference.clone())
                }
                FieldScope::Left
                | FieldScope::Right
                | FieldScope::Metadata
                | FieldScope::PartialOutput
                | FieldScope::Error => Err(format!(
                    "{:?} is unavailable during branch construction",
                    reference.scope
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
    ensure_required_fields_initialized(&initialized, branch_schema, "branch")?;
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
) -> Result<SpannedNode<Program>, String> {
    if construction.inherit.is_some() {
        return Err("INHERIT is not valid for set-only routes".to_string());
    }
    if !construction.invocations.is_empty() {
        return Err("INVOKE is not valid for internal set-only routes".to_string());
    }

    let mut initialized = HashSet::new();
    let mut normalized = RouteConstruction::default();
    for assignment in &construction.assignments {
        if !matches!(
            assignment.target.scope,
            AssignmentTargetScope::Bare | AssignmentTargetScope::Output
        ) {
            return Err("set-only SET targets must be bare fields or output fields".to_string());
        }
        let field = assignment.target.field.as_str();
        output_schema
            .field_with_name(field)
            .map_err(|_| format!("SET targets unknown output field '{field}'"))?;
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
    ensure_required_fields_initialized(&initialized, output_schema, "output")?;
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
) -> Result<SpannedNode<Program>, String> {
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
) -> Result<SpannedNode<Program>, String> {
    if construction.inherit.is_some() {
        return Err("INHERIT is not valid for generated routes".to_string());
    }
    if !construction.invocations.is_empty() {
        return Err("INVOKE is not valid for internal generated routes".to_string());
    }

    let mut initialized = HashSet::new();
    let mut normalized = RouteConstruction::default();
    for assignment in &construction.assignments {
        if !matches!(
            assignment.target.scope,
            AssignmentTargetScope::Bare | AssignmentTargetScope::Output
        ) {
            return Err(
                "generated-route SET targets must be bare fields or output fields".to_string(),
            );
        }
        let field = assignment.target.field.as_str();
        output_schema
            .field_with_name(field)
            .map_err(|_| format!("SET targets unknown output field '{field}'"))?;
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
    ensure_required_fields_initialized(&initialized, output_schema, "output")?;
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
) -> Result<ModelExpression, String> {
    resolve_expression(expression, &mut |reference| match reference.scope {
        FieldScope::Bare => {
            let name = reference.field.as_str();
            if finalized || initialized.contains(name) {
                output_schema
                    .field_with_name(name)
                    .map_err(|_| format!("unknown output field '{name}'"))?;
                Ok(FieldReference::scoped(
                    FieldScope::Output,
                    reference.field.clone(),
                ))
            } else {
                generated_schema
                    .field_with_name(name)
                    .map_err(|_| format!("unknown generated field '{name}'"))?;
                Ok(reference.clone())
            }
        }
        FieldScope::Output => {
            let name = reference.field.as_str();
            output_schema
                .field_with_name(name)
                .map_err(|_| format!("unknown output field '{name}'"))?;
            if !finalized && !initialized.contains(name) {
                return Err(format!("output field '{name}' is not initialized"));
            }
            Ok(reference.clone())
        }
        FieldScope::Message | FieldScope::Input => Err(format!(
            "{} is unavailable in generated route construction",
            match reference.scope {
                FieldScope::Message => "message",
                FieldScope::Input => "input",
                _ => unreachable!(),
            }
        )),
        _ => Ok(reference.clone()),
    })
}

fn resolve_set_only_expression(
    expression: &ModelExpression,
    initialized: &HashSet<String>,
    output_schema: &Schema,
    finalized: bool,
) -> Result<ModelExpression, String> {
    resolve_expression(expression, &mut |reference| match reference.scope {
        FieldScope::Bare | FieldScope::Output => {
            let name = reference.field.as_str();
            output_schema
                .field_with_name(name)
                .map_err(|_| format!("unknown output field '{name}'"))?;
            if !finalized && !initialized.contains(name) {
                return Err(format!("output field '{name}' is not initialized"));
            }
            Ok(FieldReference::scoped(
                FieldScope::Output,
                reference.field.clone(),
            ))
        }
        FieldScope::Message | FieldScope::Input => {
            let scope = match reference.scope {
                FieldScope::Message => "message",
                FieldScope::Input => "input",
                _ => unreachable!(),
            };
            if finalized {
                Err(format!(
                    "{scope} is unavailable after set-only output finalization"
                ))
            } else {
                Err(format!(
                    "{scope} is unavailable in set-only route construction"
                ))
            }
        }
        _ => Ok(reference.clone()),
    })
}

fn ensure_required_fields_initialized(
    initialized: &HashSet<String>,
    schema: &Schema,
    target: &str,
) -> Result<(), String> {
    for field in schema.fields() {
        if !field.is_nullable() && !initialized.contains(field.name()) {
            return Err(format!(
                "required {target} field '{}' remains uninitialized",
                field.name()
            ));
        }
    }
    Ok(())
}

struct InheritedName {
    field: String,
    leak_sensitive: bool,
}

fn inherited_fields(
    inheritance: &Inheritance,
    input_schema: &Schema,
    output_schema: &Schema,
) -> Result<Vec<InheritedName>, String> {
    let selected = match inheritance {
        Inheritance::All => input_schema
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), false))
            .collect::<Vec<_>>(),
        Inheritance::AllExcept(excluded) => {
            for field in excluded {
                input_schema.field_with_name(field.as_str()).map_err(|_| {
                    format!("INHERIT ALL EXCEPT names unknown input field '{field}'")
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
            let input = input_schema
                .field_with_name(name)
                .map_err(|_| format!("INHERIT names unknown input field '{name}'"))?;
            let output = output_schema
                .field_with_name(name)
                .map_err(|_| format!("INHERIT has no same-named output field '{name}'"))?;
            if input.data_type() != output.data_type()
                || input.is_nullable() != output.is_nullable()
            {
                return Err(format!(
                    "INHERIT field '{name}' requires identical input and output type and \
                     nullability"
                ));
            }
            Ok(InheritedName {
                field: name.to_string(),
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
) -> Result<ModelExpression, String> {
    resolve_expression(expression, &mut |reference| match reference.scope {
        FieldScope::Bare | FieldScope::Message => {
            let name = reference.field.as_str();
            if finalized || initialized.contains(name) {
                output_schema
                    .field_with_name(name)
                    .map_err(|_| format!("unknown output field '{name}'"))?;
                Ok(FieldReference::scoped(
                    FieldScope::Output,
                    reference.field.clone(),
                ))
            } else {
                let input = input_schema
                    .field_with_name(name)
                    .map_err(|_| format!("working message field '{name}' is uninitialized"))?;
                let output = output_schema.field_with_name(name).map_err(|_| {
                    format!("working message field '{name}' is not an output field")
                })?;
                if input.data_type() != output.data_type()
                    || input.is_nullable() != output.is_nullable()
                {
                    return Err(format!(
                        "working message field '{name}' cannot fall back to input with a \
                         different type or nullability"
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
                return Err(format!("output field '{name}' is not initialized"));
            }
            output_schema
                .field_with_name(name)
                .map_err(|_| format!("unknown output field '{name}'"))?;
            Ok(reference.clone())
        }
        FieldScope::Input => {
            input_schema
                .field_with_name(reference.field.as_str())
                .map_err(|_| format!("unknown input field '{}'", reference.field))?;
            Ok(reference.clone())
        }
        _ => Ok(reference.clone()),
    })
}

fn resolve_expression(
    expression: &ModelExpression,
    resolve_field: &mut impl FnMut(&FieldReference) -> Result<FieldReference, String>,
) -> Result<ModelExpression, String> {
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
                .collect::<Result<Vec<_>, String>>()?,
        },
        ModelExpression::Array(items) => ModelExpression::Array(
            items
                .iter()
                .map(|item| resolve_expression(item, resolve_field))
                .collect::<Result<Vec<_>, String>>()?,
        ),
    })
}

pub fn lower_route_construction(
    construction: &RouteConstruction,
    namespaces: SemanticNamespaces<'_>,
) -> Result<SpannedNode<Program>, String> {
    if construction.inherit.is_some() {
        return Err("INHERIT must be expanded against the input and output schemas".to_string());
    }
    let operation_count = construction.assignments.len()
        + usize::from(construction.where_clause.is_some())
        + construction.invocations.len();
    let span: Span = (0..operation_count.saturating_add(1)).into();
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
        .collect::<Result<Vec<_>, String>>()?;
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
                        .collect::<Result<Vec<_>, String>>()?,
                },
                invocation_span,
            ))
        })
        .collect::<Result<Vec<SpannedInvocation>, String>>()?;
    Ok(spanned(
        Program {
            filter,
            branch_filters: Vec::new(),
            set,
            invoke,
        },
        span,
    ))
}

pub fn lower_expression(
    expression: &ModelExpression,
    bare_read_namespace: &str,
) -> Result<SpannedExpr, String> {
    let span: Span = (0..0).into();
    lower_expression_with_span(expression, bare_read_namespace, span)
}

fn lower_expression_with_span(
    expression: &ModelExpression,
    bare_read_namespace: &str,
    span: Span,
) -> Result<SpannedExpr, String> {
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
                .collect::<Result<Vec<_>, String>>()?,
        },
        ModelExpression::Array(_) => {
            return Err("array expressions are valid only in window SET values".to_string());
        }
    };
    Ok(spanned(expression, span))
}

fn scalar_data_type(target: &ParseAsType) -> Result<DataType, String> {
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
            Err("casts to collection types are not supported".to_string())
        }
    }
}
