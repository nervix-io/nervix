//! Walking the expressions a model carries.
//!
//! Layer: decisions.
//!
//! - **Owns.** Visiting every expression a Model declares, the field references one reads, the
//!   header reads it performs, and the lookup rewriting a compiled program needs.
//! - **Depends on.** The Models and the VM's program representation.
//! - **Must not know.** What any caller concludes from what it finds.

use std::sync::Arc as StdArc;

use ahash::{HashMap, HashSet};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use error_stack::{Report, ResultExt};
use nervix_models::{
    DomainName, EmitSink, Expression, FieldName, LookupName, MaterializedStatePolicy,
    MessageErrorPolicy, Model, ModelIndex, ModelKind, ModelName, NodeRef, ProcessorOutputs,
};
use nervix_vm::{
    CompileBinding,
    program::{
        CaseArm, Expr, FunctionName, InternalFieldNamespace, InternalFieldRef, Literal, Program,
        SpannedExpr,
    },
};
use petgraph::{graph::DiGraph, prelude::NodeIndex};

use crate::registry::{
    error::RegistryError,
    graph::{ActiveNode, EdgeKind},
    validation::{
        materialized_state::model_materialized_state_dependencies,
        schema::arrow_data_type_for_parse_as, wire::schema_for_lookup_model,
    },
};

pub(in crate::registry) fn visit_model_expressions(
    model: &Model,
    visitor: &mut impl FnMut(&Expression),
) {
    fn visit_inputs(
        inputs: &nervix_models::ProcessorInputs,
        visitor: &mut impl FnMut(&Expression),
    ) {
        for source_filter in inputs.where_clauses() {
            visitor(&source_filter.where_clause);
        }
    }
    fn visit_error_policy(policy: &MessageErrorPolicy, visitor: &mut impl FnMut(&Expression)) {
        if let MessageErrorPolicy::Dlq { assignments, .. } = policy {
            for assignment in assignments {
                visitor(&assignment.value);
            }
        }
    }
    fn visit_outputs(outputs: &ProcessorOutputs, visitor: &mut impl FnMut(&Expression)) {
        for output in outputs.outputs() {
            if let Some(branch) = &output.branch {
                for assignment in branch.assignments() {
                    visitor(&assignment.value);
                }
            }
            for assignment in &output.construction.assignments {
                visitor(&assignment.value);
            }
            if let Some(where_clause) = &output.construction.where_clause {
                visitor(where_clause);
            }
            for invocation in &output.construction.invocations {
                for argument in &invocation.arguments {
                    visitor(argument);
                }
            }
            visit_error_policy(&output.message_error_policy, visitor);
        }
    }
    fn visit_filter(filter: &Option<Expression>, visitor: &mut impl FnMut(&Expression)) {
        if let Some(filter) = filter {
            visitor(filter);
        }
    }

    match model {
        Model::Ingestor(model) => {
            visit_filter(&model.filter_where, visitor);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Reingestor(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Generator(model) => visit_outputs(&model.output_routes, visitor),
        Model::Inferencer(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            for mapping in &model.inputs {
                visitor(&mapping.expression);
            }
            visit_outputs(&model.output_routes, visitor);
        }
        Model::WasmProcessor(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Junction(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Deduplicator(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            for expression in &model.deduplicate_on {
                visitor(expression);
            }
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Correlator(model) => {
            visit_inputs(&model.left, visitor);
            visit_inputs(&model.right, visitor);
            visitor(&model.correlate_where);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Reorderer(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            for expression in &model.order_by {
                visitor(expression);
            }
            visit_outputs(&model.output_routes, visitor);
        }
        Model::WindowProcessor(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Emitter(model) => {
            visit_inputs(&model.from, visitor);
            for assignment in &model.construction.assignments {
                visitor(&assignment.value);
            }
            if let Some(where_clause) = &model.construction.where_clause {
                visitor(where_clause);
            }
            for invocation in &model.construction.invocations {
                for argument in &invocation.arguments {
                    visitor(argument);
                }
            }
            if let EmitSink::Http { method, path, .. } = model.sink.as_ref() {
                visitor(method);
                visitor(path);
            }
            if let EmitSink::Otel {
                values,
                attributes,
                resource,
                ..
            } = model.sink.as_ref()
            {
                for value in values.iter().chain(attributes).chain(resource) {
                    visitor(&value.expression);
                }
            }
            let values = match model.sink.as_ref() {
                EmitSink::ClickHouse { values, .. }
                | EmitSink::Postgres { values, .. }
                | EmitSink::MySql { values, .. }
                | EmitSink::MongoDb { values, .. }
                | EmitSink::Iceberg { values, .. } => Some(values.as_slice()),
                _ => None,
            };
            if let Some(values) = values {
                for value in values {
                    visitor(&value.expression);
                }
            }
            visit_error_policy(&model.error_policies.message, visitor);
        }
        _ => {}
    }
    for dependency in model_materialized_state_dependencies(model) {
        if let MaterializedStatePolicy::Default(assignments) = &dependency.policy {
            for assignment in assignments {
                visitor(&assignment.value);
            }
        }
    }
}

pub(in crate::registry) fn lookup_hash_map_bindings(
    mut fields: Vec<(String, ArrowDataType)>,
) -> Vec<CompileBinding> {
    if fields.is_empty() {
        return Vec::new();
    }
    fields.sort_by(|left, right| left.0.cmp(&right.0));
    fields.dedup_by(|left, right| left.0 == right.0);
    vec![CompileBinding::internal_readonly(
        InternalFieldNamespace::LookupHashMap,
        StdArc::new(ArrowSchema::new(
            fields
                .into_iter()
                .map(|(name, data_type)| ArrowField::new(name, data_type, true))
                .collect::<Vec<_>>(),
        )),
    )]
}

/// A rewritten program together with the internal fields its `LOOKUP_HASH_MAP` calls now read
/// from, which the compiler binds as an extra input namespace.
pub(in crate::registry) struct LookupHashMapRewriteResult {
    pub(in crate::registry) program: nervix_vm::program::SpannedNode<Program>,
    pub(in crate::registry) fields: Vec<(String, ArrowDataType)>,
}

/// One `LOOKUP_HASH_MAP` call lifted out of a program: which hash map and field it reads, the key
/// expression it looks up, and the internal field the rewritten program reads the result from.
struct LookupHashMapCallSite {
    lookup: LookupName,
    lookup_field: FieldName,
    key: Expr,
    generated_field: String,
    data_type: ArrowDataType,
}

pub(in crate::registry) fn rewrite_lookup_hash_map_program(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    parsed: &nervix_vm::program::SpannedNode<Program>,
) -> Result<LookupHashMapRewriteResult, Report<RegistryError>> {
    let mut next_field = 0usize;
    let mut calls = Vec::<LookupHashMapCallSite>::new();
    let mut rewrite = |expr: &SpannedExpr| {
        rewrite_lookup_hash_map_expr(
            domain,
            identifier,
            models,
            expr,
            &mut calls,
            &mut next_field,
        )
    };
    let program = nervix_vm::program::SpannedNode {
        inner: Program {
            filter: parsed.inner.filter.as_ref().map(&mut rewrite).transpose()?,
            set: parsed
                .inner
                .set
                .iter()
                .map(|(field, expr)| rewrite(expr).map(|expr| (field.clone(), expr)))
                .collect::<Result<Vec<_>, _>>()?,
            invoke: parsed
                .inner
                .invoke
                .iter()
                .map(|invocation| {
                    Ok(nervix_vm::program::SpannedNode {
                        inner: nervix_vm::program::Invocation {
                            function: invocation.inner.function.clone(),
                            args: invocation
                                .inner
                                .args
                                .iter()
                                .map(&mut rewrite)
                                .collect::<Result<Vec<_>, Report<RegistryError>>>()?,
                        },
                        span: invocation.span,
                    })
                })
                .collect::<Result<Vec<_>, Report<RegistryError>>>()?,
        },
        span: parsed.span,
    };
    let fields = calls
        .into_iter()
        .map(|call| (call.generated_field, call.data_type))
        .collect();
    Ok(LookupHashMapRewriteResult { program, fields })
}

fn rewrite_lookup_hash_map_expr(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    expr: &SpannedExpr,
    calls: &mut Vec<LookupHashMapCallSite>,
    next_field: &mut usize,
) -> Result<SpannedExpr, Report<RegistryError>> {
    let inner = match &expr.inner {
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => expr.inner.clone(),
        Expr::Membership { operand, set } => Expr::Membership {
            operand: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, operand, calls, next_field,
            )?),
            set: set
                .iter()
                .map(|element| {
                    rewrite_lookup_hash_map_expr(
                        domain, identifier, models, element, calls, next_field,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        },
        Expr::Between { operand, low, high } => Expr::Between {
            operand: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, operand, calls, next_field,
            )?),
            low: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, low, calls, next_field,
            )?),
            high: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, high, calls, next_field,
            )?),
        },
        Expr::Unary { op, expr: inner } => Expr::Unary {
            op: *op,
            expr: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, inner, calls, next_field,
            )?),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, left, calls, next_field,
            )?),
            right: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, right, calls, next_field,
            )?),
        },
        Expr::Cast {
            expr: inner,
            data_type,
            on_failure,
        } => Expr::Cast {
            expr: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, inner, calls, next_field,
            )?),
            data_type: data_type.clone(),
            on_failure: *on_failure,
        },
        Expr::Json {
            document,
            extraction,
        } => Expr::Json {
            document: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, document, calls, next_field,
            )?),
            extraction: extraction.clone(),
        },
        Expr::Call { function, args } => {
            if let FunctionName::LookupHashMap = function {
                let [lookup_arg, key_arg, field_arg] = args.as_slice() else {
                    return Err(Report::new(RegistryError::InvalidModel {
                        domain: domain.as_str().to_string(),
                        identifier: identifier.as_str().to_string(),
                        reason: format!(
                            "LOOKUP_HASH_MAP expects 3 arguments, found {}",
                            args.len()
                        ),
                    }));
                };
                let Expr::Literal(Literal::String(lookup_name)) = &lookup_arg.inner else {
                    return Err(Report::new(RegistryError::LookupHashMapLiteralArgument {
                        domain: domain.clone(),
                        identifier: identifier.clone(),
                        argument: 1,
                    }));
                };
                let lookup = LookupName::parse(lookup_name).map_err(|error| {
                    Report::new(RegistryError::InvalidModel {
                        domain: domain.as_str().to_string(),
                        identifier: identifier.as_str().to_string(),
                        reason: format!(
                            "LOOKUP_HASH_MAP hash map name '{lookup_name}' is invalid: {error}"
                        ),
                    })
                })?;
                let Expr::Literal(Literal::String(raw_lookup_field)) = &field_arg.inner else {
                    return Err(Report::new(RegistryError::LookupHashMapLiteralArgument {
                        domain: domain.clone(),
                        identifier: identifier.clone(),
                        argument: 3,
                    }));
                };
                let lookup_field = FieldName::parse(raw_lookup_field).change_context(
                    RegistryError::InvalidModel {
                        domain: domain.as_str().to_string(),
                        identifier: identifier.as_str().to_string(),
                        reason: format!("LOOKUP_HASH_MAP field '{raw_lookup_field}' is invalid"),
                    },
                )?;
                let lookup_schema = schema_for_lookup_model(domain, identifier, models, &lookup)?;
                let Some(schema_field) = lookup_schema
                    .fields
                    .iter()
                    .find(|field| field.name == lookup_field)
                else {
                    return Err(Report::new(RegistryError::IncompatibleSchema {
                        domain: domain.as_str().to_string(),
                        identifier: identifier.as_str().to_string(),
                        reason: format!(
                            "LOOKUP_HASH_MAP field '{}' is missing from hash map '{}' schema",
                            lookup_field,
                            lookup.as_str()
                        ),
                    }));
                };
                // Matches the runtime's identity for the same call: the key expression itself,
                // compared without its source spans.
                let key = key_arg.inner.clone();
                let data_type = arrow_data_type_for_parse_as(&schema_field.ty);
                let existing = calls.iter().find(|call| {
                    call.lookup == lookup && call.lookup_field == lookup_field && call.key == key
                });
                let generated_field = if let Some(existing) = existing {
                    existing.generated_field.clone()
                } else {
                    let generated_field = format!("value_{}", *next_field);
                    *next_field += 1;
                    calls.push(LookupHashMapCallSite {
                        lookup: lookup.clone(),
                        lookup_field,
                        key,
                        generated_field: generated_field.clone(),
                        data_type,
                    });
                    generated_field
                };
                Expr::InternalFieldRef(InternalFieldRef {
                    namespace: InternalFieldNamespace::LookupHashMap,
                    field: generated_field,
                })
            } else {
                Expr::Call {
                    function: function.clone(),
                    args: args
                        .iter()
                        .map(|arg| {
                            rewrite_lookup_hash_map_expr(
                                domain, identifier, models, arg, calls, next_field,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                }
            }
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|operand| {
                    rewrite_lookup_hash_map_expr(
                        domain, identifier, models, operand, calls, next_field,
                    )
                    .map(Box::new)
                })
                .transpose()?,
            branches: branches
                .iter()
                .map(|branch| {
                    Ok(CaseArm {
                        when: rewrite_lookup_hash_map_expr(
                            domain,
                            identifier,
                            models,
                            &branch.when,
                            calls,
                            next_field,
                        )?,
                        result: rewrite_lookup_hash_map_expr(
                            domain,
                            identifier,
                            models,
                            &branch.result,
                            calls,
                            next_field,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, Report<RegistryError>>>()?,
            else_result: else_result
                .as_ref()
                .map(|result| {
                    rewrite_lookup_hash_map_expr(
                        domain, identifier, models, result, calls, next_field,
                    )
                    .map(Box::new)
                })
                .transpose()?,
        },
    };
    Ok(nervix_vm::program::SpannedNode {
        inner,
        span: expr.span,
    })
}

fn collect_expr_field_refs(expr: &SpannedExpr, refs: &mut Vec<(String, String)>) {
    match &expr.inner {
        Expr::Literal(_) | Expr::InternalFieldRef(_) => {}
        Expr::Membership { operand, set } => {
            collect_expr_field_refs(operand, refs);
            for element in set {
                collect_expr_field_refs(element, refs);
            }
        }
        Expr::Between { operand, low, high } => {
            collect_expr_field_refs(operand, refs);
            collect_expr_field_refs(low, refs);
            collect_expr_field_refs(high, refs);
        }
        Expr::FieldRef(field_ref) => {
            refs.push((field_ref.relay.clone(), field_ref.field.clone()));
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::Json { document: expr, .. } => {
            collect_expr_field_refs(expr, refs);
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_field_refs(left, refs);
            collect_expr_field_refs(right, refs);
        }
        Expr::Call { args, .. } => {
            for arg in args {
                collect_expr_field_refs(arg, refs);
            }
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_expr_field_refs(operand, refs);
            }
            for branch in branches {
                collect_expr_field_refs(&branch.when, refs);
                collect_expr_field_refs(&branch.result, refs);
            }
            if let Some(else_result) = else_result {
                collect_expr_field_refs(else_result, refs);
            }
        }
    }
}

fn expr_uses_header_read(expr: &SpannedExpr) -> bool {
    match &expr.inner {
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => false,
        Expr::Membership { operand, set } => {
            expr_uses_header_read(operand) || set.iter().any(expr_uses_header_read)
        }
        Expr::Between { operand, low, high } => {
            expr_uses_header_read(operand)
                || expr_uses_header_read(low)
                || expr_uses_header_read(high)
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::Json { document: expr, .. } => {
            expr_uses_header_read(expr)
        }
        Expr::Binary { left, right, .. } => {
            expr_uses_header_read(left) || expr_uses_header_read(right)
        }
        Expr::Call { function, args } => {
            if let FunctionName::ReadHeader | FunctionName::ReadHeaders = function {
                true
            } else {
                args.iter().any(expr_uses_header_read)
            }
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_uses_header_read(expr))
                || branches.iter().any(|branch| {
                    expr_uses_header_read(&branch.when) || expr_uses_header_read(&branch.result)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_uses_header_read(expr))
        }
    }
}

pub(in crate::registry) fn program_uses_header_reads(program: &Program) -> bool {
    program.filter.as_ref().is_some_and(expr_uses_header_read)
        || program
            .set
            .iter()
            .any(|(_field, expr)| expr_uses_header_read(expr))
        || program
            .invoke
            .iter()
            .flat_map(|invocation| &invocation.inner.args)
            .any(expr_uses_header_read)
}

pub(in crate::registry) fn collect_program_field_refs(
    program: &nervix_vm::program::Program,
) -> Vec<(String, String)> {
    let mut refs = Vec::new();
    if let Some(filter) = &program.filter {
        collect_expr_field_refs(filter, &mut refs);
    }
    for (_field_ref, expr) in &program.set {
        collect_expr_field_refs(expr, &mut refs);
    }
    for invocation in &program.invoke {
        for arg in &invocation.inner.args {
            collect_expr_field_refs(arg, &mut refs);
        }
    }
    refs
}

pub(in crate::registry) fn add_udf_dependency_edges(
    domain: &DomainName,
    identifier: &ModelName,
    model: &Model,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    consumer: NodeIndex,
) -> Result<(), Report<RegistryError>> {
    let mut dependencies = HashSet::default();
    visit_model_expressions(model, &mut |expression| {
        expression.visit_udf_calls(&mut |function, _| {
            dependencies.insert(function.clone());
        });
    });
    for function in dependencies {
        let key = NodeRef::new(ModelKind::Udf, function.clone());
        let udf = indices.get(&key).copied().ok_or_else(|| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("referenced UDF 'udf::{}' does not exist", function.as_str()),
            })
        })?;
        graph.add_edge(udf, consumer, EdgeKind::RequiredBy);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use meticulous::ResultExt as _;
    use nervix_models::{JsonPath, ModelName};
    use nervix_vm::{
        JsonExtraction, JsonOutput,
        program::{Expr, FieldRef, FunctionName, SpannedExpr, SpannedNode},
    };

    use super::{collect_expr_field_refs, expr_uses_header_read};
    use crate::registry::{
        error::RegistryError,
        storage::Registry,
        test_fixtures::{example_graph_models, temp_db_path},
    };

    fn spanned(inner: Expr) -> SpannedExpr {
        SpannedNode {
            inner,
            span: (0..1).into(),
        }
    }

    fn json_exists(document: Expr) -> SpannedExpr {
        spanned(Expr::Json {
            document: Box::new(spanned(document)),
            extraction: JsonExtraction {
                path: triomphe::Arc::new(JsonPath::parse("$.a").assured("the path is valid")),
                output: JsonOutput::Exists,
            },
        })
    }

    #[test]
    fn walkers_reach_the_document_of_a_json_extraction() {
        let field = json_exists(Expr::FieldRef(FieldRef {
            relay: "input".to_string(),
            field: "doc".to_string(),
        }));
        let mut refs = Vec::new();
        collect_expr_field_refs(&field, &mut refs);
        assert_eq!(refs, [("input".to_string(), "doc".to_string())]);
        assert!(!expr_uses_header_read(&field));

        let header = json_exists(Expr::Call {
            function: FunctionName::ReadHeader,
            args: Vec::new(),
        });
        assert!(expr_uses_header_read(&header));
    }

    #[test]
    fn apply_batch_rejects_lookup_hash_map_arguments_that_are_not_string_literals() {
        for (call, argument) in [
            ("LOOKUP_HASH_MAP(input.source, input.source, \"city\")", 1),
            ("LOOKUP_HASH_MAP(\"cities\", input.source, input.source)", 3),
        ] {
            let (domain, models) = example_graph_models(
                "lookup hash map argument literals",
                &format!(
                    r#"
                    CREATE SCHEMA metric (
                      value I64,
                      source STRING
                    );

                    CREATE SCHEMA located_metric (
                      value I64,
                      source STRING,
                      city STRING OPTIONAL
                    );

                    CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
                    CREATE RELAY located_metrics SCHEMA located_metric UNBRANCHED;

                    CREATE DEDUPLICATOR locate_metrics
                      FROM raw_metrics
                      DEDUPLICATE ON input.source
                      MAX TIME 10m
                      UNBRANCHED
                      TO located_metrics
                        INHERIT ALL
                        SET city = {call}
                        FLUSH IMMEDIATE
                        ON MESSAGE ERROR LOG;
                    "#
                ),
            );
            let path = temp_db_path();
            let registry = Registry::open(&path).expect("registry should open");

            let err = registry
                .apply_batch(&domain, models)
                .expect_err("a LOOKUP_HASH_MAP name argument must be a string literal");

            assert_eq!(
                err.current_context(),
                &RegistryError::LookupHashMapLiteralArgument {
                    domain: domain.clone(),
                    identifier: ModelName::parse("locate_metrics").expect("valid identifier"),
                    argument,
                }
            );

            let _ = fs::remove_dir_all(path);
        }
    }
}
