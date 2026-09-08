use super::*;

#[derive(Debug, Clone)]
pub(super) struct LookupHashMapCall {
    pub(super) lookup: LookupName,
    pub(super) lookup_runtime: Arc<LookupRuntime>,
    pub(super) lookup_field: String,
    pub(super) generated_field: String,
    pub(super) key_program: Arc<VmCompiledProgram>,
    /// Identifies the call across output routes. Two routes of one node compile separate programs,
    /// so the compiled key program cannot be compared; the source expression can.
    pub(super) key_expr: Expr,
}

/// Identity of one `LOOKUP_HASH_MAP` call, shared by every output route that spells it the same
/// way over the same batch.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct LookupHashMapCallKey {
    pub(super) lookup: LookupName,
    pub(super) lookup_field: String,
    pub(super) key_expr: Expr,
}

#[derive(Debug, Clone)]
pub(super) struct PendingLookupHashMapCall {
    pub(super) lookup: LookupName,
    pub(super) lookup_runtime: Arc<LookupRuntime>,
    pub(super) lookup_field: String,
    pub(super) lookup_field_type: ArrowDataType,
    pub(super) generated_field: String,
    pub(super) key_expr: SpannedExpr,
}

pub(super) fn collect_expr_field_refs(expr: &SpannedExpr, refs: &mut Vec<(String, String)>) {
    match &expr.inner {
        Expr::Literal(_) | Expr::InternalFieldRef(_) => {}
        Expr::FieldRef(field_ref) => {
            refs.push((field_ref.relay.clone(), field_ref.field.clone()));
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
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

pub(super) fn collect_program_field_refs(
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

pub(super) fn lookup_hash_map_literal_arg(
    args: &[SpannedExpr],
    index: usize,
    function_span: nervix_vm::program::Span,
) -> Result<&str, String> {
    let Some(arg) = args.get(index) else {
        return Err(format!(
            "LOOKUP_HASH_MAP expects 3 arguments, found {}",
            args.len()
        ));
    };
    match &arg.inner {
        Expr::Literal(Literal::String(value)) => Ok(value.as_str()),
        _ => Err(format!(
            "LOOKUP_HASH_MAP argument {} must be a string literal at {}..{}",
            index + 1,
            function_span.start,
            function_span.end
        )),
    }
}

pub(super) fn expr_contains_lookup_hash_map(expr: &SpannedExpr) -> bool {
    match &expr.inner {
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => false,
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => expr_contains_lookup_hash_map(expr),
        Expr::Binary { left, right, .. } => {
            expr_contains_lookup_hash_map(left) || expr_contains_lookup_hash_map(right)
        }
        Expr::Call { function, args } => {
            if let FunctionName::LookupHashMap = function {
                return true;
            }
            args.iter().any(expr_contains_lookup_hash_map)
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            operand
                .as_deref()
                .is_some_and(expr_contains_lookup_hash_map)
                || branches.iter().any(|branch| {
                    expr_contains_lookup_hash_map(&branch.when)
                        || expr_contains_lookup_hash_map(&branch.result)
                })
                || else_result
                    .as_deref()
                    .is_some_and(expr_contains_lookup_hash_map)
        }
    }
}

pub(super) fn rewrite_lookup_hash_map_expr(
    expr: &SpannedExpr,
    available_lookups: &HashMap<LookupName, Arc<LookupRuntime>>,
    pending_calls: &mut Vec<PendingLookupHashMapCall>,
) -> Result<SpannedExpr, String> {
    let rewritten = match &expr.inner {
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => expr.clone(),
        Expr::Unary { op, expr: inner } => nervix_vm::program::SpannedNode {
            inner: Expr::Unary {
                op: *op,
                expr: Box::new(rewrite_lookup_hash_map_expr(
                    inner,
                    available_lookups,
                    pending_calls,
                )?),
            },
            span: expr.span,
        },
        Expr::Binary { op, left, right } => nervix_vm::program::SpannedNode {
            inner: Expr::Binary {
                op: *op,
                left: Box::new(rewrite_lookup_hash_map_expr(
                    left,
                    available_lookups,
                    pending_calls,
                )?),
                right: Box::new(rewrite_lookup_hash_map_expr(
                    right,
                    available_lookups,
                    pending_calls,
                )?),
            },
            span: expr.span,
        },
        Expr::Cast {
            expr: inner,
            data_type,
        } => nervix_vm::program::SpannedNode {
            inner: Expr::Cast {
                expr: Box::new(rewrite_lookup_hash_map_expr(
                    inner,
                    available_lookups,
                    pending_calls,
                )?),
                data_type: data_type.clone(),
            },
            span: expr.span,
        },
        Expr::Case {
            operand,
            branches,
            else_result,
        } => nervix_vm::program::SpannedNode {
            inner: Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|operand| {
                        rewrite_lookup_hash_map_expr(operand, available_lookups, pending_calls)
                            .map(Box::new)
                    })
                    .transpose()?,
                branches: branches
                    .iter()
                    .map(|branch| {
                        Ok(CaseArm {
                            when: rewrite_lookup_hash_map_expr(
                                &branch.when,
                                available_lookups,
                                pending_calls,
                            )?,
                            result: rewrite_lookup_hash_map_expr(
                                &branch.result,
                                available_lookups,
                                pending_calls,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                else_result: else_result
                    .as_ref()
                    .map(|else_result| {
                        rewrite_lookup_hash_map_expr(else_result, available_lookups, pending_calls)
                            .map(Box::new)
                    })
                    .transpose()?,
            },
            span: expr.span,
        },
        Expr::Call { function, args } => {
            if let FunctionName::LookupHashMap = function {
                if args.len() != 3 {
                    return Err(format!(
                        "LOOKUP_HASH_MAP expects 3 arguments, found {}",
                        args.len()
                    ));
                }
                let lookup_name = lookup_hash_map_literal_arg(args, 0, expr.span)?;
                let lookup = LookupName::parse(lookup_name).map_err(|error| {
                    format!("LOOKUP_HASH_MAP hash map name '{lookup_name}' is invalid: {error}")
                })?;
                let lookup_field = lookup_hash_map_literal_arg(args, 2, expr.span)?.to_string();
                if expr_contains_lookup_hash_map(&args[1]) {
                    return Err("LOOKUP_HASH_MAP key expression cannot contain another \
                                LOOKUP_HASH_MAP"
                        .to_string());
                }
                let Some(lookup_runtime) = available_lookups.get(&lookup).cloned() else {
                    return Err(format!(
                        "LOOKUP_HASH_MAP hash map '{}' is not instantiated",
                        lookup.as_str()
                    ));
                };
                let lookup_field_type = lookup_runtime
                    .schema
                    .arrow_schema()
                    .field_with_name(&lookup_field)
                    .map(|field| field.data_type().clone())
                    .map_err(|_| {
                        format!(
                            "LOOKUP_HASH_MAP field '{}' is missing from hash map '{}' schema",
                            lookup_field,
                            lookup.as_str()
                        )
                    })?;
                // Deduplication over the calls lowered so far in this one program. The
                // comparison includes the key expression, which has no hash or ordering, so the
                // bound is the LOOKUP_HASH_MAP calls written in the program being compiled.
                let existing = pending_calls.iter().find(|call| {
                    call.lookup == lookup
                        && call.lookup_field == lookup_field
                        && call.key_expr.inner == args[1].inner
                });
                let generated_field = if let Some(existing) = existing {
                    existing.generated_field.clone()
                } else {
                    let generated_field = format!("value_{}", pending_calls.len());
                    pending_calls.push(PendingLookupHashMapCall {
                        lookup,
                        lookup_runtime,
                        lookup_field,
                        lookup_field_type,
                        generated_field: generated_field.clone(),
                        key_expr: args[1].clone(),
                    });
                    generated_field
                };
                nervix_vm::program::SpannedNode {
                    inner: Expr::InternalFieldRef(InternalFieldRef {
                        namespace: InternalFieldNamespace::LookupHashMap,
                        field: generated_field,
                    }),
                    span: expr.span,
                }
            } else {
                nervix_vm::program::SpannedNode {
                    inner: Expr::Call {
                        function: function.clone(),
                        args: args
                            .iter()
                            .map(|arg| {
                                rewrite_lookup_hash_map_expr(arg, available_lookups, pending_calls)
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    },
                    span: expr.span,
                }
            }
        }
    };
    Ok(rewritten)
}

pub(super) fn rewrite_lookup_hash_map_program(
    parsed: &nervix_vm::program::SpannedNode<nervix_vm::program::Program>,
    available_lookups: &HashMap<LookupName, Arc<LookupRuntime>>,
) -> Result<
    (
        nervix_vm::program::SpannedNode<nervix_vm::program::Program>,
        Vec<PendingLookupHashMapCall>,
    ),
    String,
> {
    let mut pending_calls = Vec::new();
    let program = nervix_vm::program::Program {
        filter: parsed
            .inner
            .filter
            .as_ref()
            .map(|expr| rewrite_lookup_hash_map_expr(expr, available_lookups, &mut pending_calls))
            .transpose()?,
        set: parsed
            .inner
            .set
            .iter()
            .map(|(field, expr)| {
                rewrite_lookup_hash_map_expr(expr, available_lookups, &mut pending_calls)
                    .map(|expr| (field.clone(), expr))
            })
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
                            .map(|arg| {
                                rewrite_lookup_hash_map_expr(
                                    arg,
                                    available_lookups,
                                    &mut pending_calls,
                                )
                            })
                            .collect::<Result<Vec<_>, String>>()?,
                    },
                    span: invocation.span,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
    };
    Ok((
        nervix_vm::program::SpannedNode {
            inner: program,
            span: parsed.span,
        },
        pending_calls,
    ))
}

pub(super) fn compile_lookup_hash_map_calls(
    pending_calls: Vec<PendingLookupHashMapCall>,
    writable_namespace: &str,
    bindings: &[VmCompileBinding],
    udfs: Option<&UdfExecutor>,
) -> Result<(Vec<LookupHashMapCall>, Option<VmCompileBinding>), String> {
    if pending_calls.is_empty() {
        return Ok((Vec::new(), None));
    }

    let lookup_fields = pending_calls
        .iter()
        .map(|call| {
            arrow_schema::Field::new(&call.generated_field, call.lookup_field_type.clone(), true)
        })
        .collect::<Vec<_>>();
    let lookup_binding = VmCompileBinding::internal_readonly(
        InternalFieldNamespace::LookupHashMap,
        StdArc::new(arrow_schema::Schema::new(lookup_fields)),
    );
    let mut compiled_calls = Vec::with_capacity(pending_calls.len());
    for call in pending_calls {
        let key_program = nervix_vm::program::SpannedNode {
            inner: nervix_vm::program::Program {
                filter: None,
                set: vec![(
                    nervix_vm::program::FieldRef {
                        relay: writable_namespace.to_string(),
                        field: call.generated_field.clone(),
                    },
                    call.key_expr.clone(),
                )],
                invoke: Vec::new(),
            },
            span: (0..0).into(),
        };
        let signatures = udfs
            .map(|executor| executor.signatures().clone())
            .unwrap_or_default();
        let key_types = infer_vm_set_expr_types_for_bindings_with_udfs(
            &key_program,
            bindings.iter().cloned(),
            signatures,
        )
        .map_err(|error| {
            format!(
                "LOOKUP_HASH_MAP key compile failed for hash map '{}' field '{}': {}",
                call.lookup.as_str(),
                call.lookup_field,
                error.message
            )
        })?;
        let key_output_schema = StdArc::new(arrow_schema::Schema::new(
            key_types
                .into_iter()
                .map(|inferred| {
                    arrow_schema::Field::new(inferred.field, inferred.data_type, inferred.nullable)
                })
                .collect::<Vec<_>>(),
        ));
        let compiled_key = compile_vm_program_with_options_for_bindings_with_sensitivity(
            &key_program,
            key_output_schema,
            VmSchemaSensitivity::default(),
            bindings.iter().cloned(),
            runtime_udf_compile_options(
                udfs,
                VmCompileOptions {
                    output_mode: VmOutputMode::ExplicitOnly,
                    ..VmCompileOptions::default()
                },
            ),
        )
        .map_err(|error| {
            format!(
                "LOOKUP_HASH_MAP key compile failed for hash map '{}' field '{}': {}",
                call.lookup.as_str(),
                call.lookup_field,
                error.message
            )
        })?;
        compiled_calls.push(LookupHashMapCall {
            lookup: call.lookup,
            lookup_runtime: call.lookup_runtime,
            lookup_field: call.lookup_field,
            generated_field: call.generated_field,
            key_program: Arc::new(compiled_key),
            key_expr: call.key_expr.inner,
        });
    }
    Ok((compiled_calls, Some(lookup_binding)))
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use nervix_models::{CreateLookup, CreateSchema, ModelName, ParseAsType};
    use triomphe::Arc;

    use super::*;
    use crate::{
        runtime_ack::AckSet,
        runtime_schema::{RuntimeValue, compile_schema, test_runtime_row},
    };
    #[tokio::test]
    async fn filter_map_lookup_hash_map_enriches_rows_and_filters_misses() {
        let input_schema = test_schema(&[
            ("id", ParseAsType::String),
            ("active", ParseAsType::Bool),
            ("title", ParseAsType::String),
        ]);
        let lookup_schema = test_schema(&[
            ("normalized_title", ParseAsType::String),
            ("city_name", ParseAsType::String),
            ("region_name", ParseAsType::String),
        ]);
        let lookup_batch = lookup_schema
            .batch_from_test_rows([[
                (
                    "normalized_title".to_string(),
                    RuntimeValue::String("mr".to_string()),
                ),
                (
                    "city_name".to_string(),
                    RuntimeValue::String("Chicago".to_string()),
                ),
                (
                    "region_name".to_string(),
                    RuntimeValue::String("IL".to_string()),
                ),
            ]])
            .expect("lookup fixture should build as Arrow");
        let lookup = Arc::new(LookupRuntime {
            model: CreateLookup {
                name: named("titles_by_normalized"),
                key_field: named("normalized_title"),
                resource: named("titles_data"),
                path: "lookup.jsonl".to_string(),
                decode_using_codec: named("title_lookup_codec"),
            },
            resource_version: 1,
            schema: lookup_schema,
            batch: Arc::new(lookup_batch),
            entries: Arc::new(HashMap::from_iter([("mr".to_string(), 0)])),
        });
        let lookups = HashMap::from_iter([(named("titles_by_normalized"), lookup)]);
        let output_schema = Arc::new(compile_schema(&CreateSchema {
            name: named("lookup_output"),
            fields: vec![
                nervix_models::SchemaField {
                    name: named("id"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("active"),
                    ty: ParseAsType::Bool,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("title_key"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("city"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("region"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
            ],
        }));
        let program = compile_processor_output_filter_map_program(
            RuntimeCompileTarget {
                domain: &domain("default"),
                identifier: &named("project_titles"),
            },
            &[named("incoming_logs")],
            &named("projected_titles"),
            &construction(
                "INHERIT ALL EXCEPT title SET title_key = lower(input.title), city = \
                 LOOKUP_HASH_MAP(\"titles_by_normalized\", lower(input.title), \"city_name\"), \
                 region = LOOKUP_HASH_MAP(\"titles_by_normalized\", lower(input.title), \
                 \"region_name\") WHERE NOT is_null(LOOKUP_HASH_MAP(\"titles_by_normalized\", \
                 lower(input.title), \"city_name\"))",
            ),
            RuntimeVmSchemaPair {
                input: input_schema.arrow_schema(),
                input_sensitivity: VmSchemaSensitivity::default(),
                output: output_schema.arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            None,
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &lookups,
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("filter-map should compile")
        .expect("program should exist");
        assert_eq!(program.lookup_hash_maps.len(), 2);

        let (hit_acks, _hit_completion) = AckSet::root();
        let (miss_acks, _miss_completion) = AckSet::root();
        let batch = RelayRecordBatch::from_messages(
            input_schema,
            vec![
                RelayMessage {
                    key: string_branch_key("tenant", "acme"),
                    record: test_runtime_row([
                        ("id".to_string(), RuntimeValue::String("hit-1".to_string())),
                        ("active".to_string(), RuntimeValue::Bool(true)),
                        ("title".to_string(), RuntimeValue::String("MR".to_string())),
                    ]),
                    acks: hit_acks,
                },
                RelayMessage {
                    key: string_branch_key("tenant", "acme"),
                    record: test_runtime_row([
                        ("id".to_string(), RuntimeValue::String("miss-1".to_string())),
                        ("active".to_string(), RuntimeValue::Bool(true)),
                        (
                            "title".to_string(),
                            RuntimeValue::String("Unknown".to_string()),
                        ),
                    ]),
                    acks: miss_acks,
                },
            ],
        )
        .expect("batch should build");

        let plan = plan_filter_map_messages(
            "deduplicator",
            &named::<ModelName>("project_titles"),
            "FILTER-MAP",
            &program,
            batch,
            current_timestamp(),
            &HashMap::default(),
        )
        .await
        .expect("filter-map planning should succeed");
        let messages = plan
            .batch
            .expect("filter-map should produce a batch")
            .try_into_messages()
            .expect("filter-map batch should convert to messages");

        assert_eq!(messages.len(), 1);
        assert_eq!(
            row_value(&messages[0].record, "city"),
            Some(RuntimeValue::String("Chicago".to_string()))
        );
        assert_eq!(
            row_value(&messages[0].record, "region"),
            Some(RuntimeValue::String("IL".to_string()))
        );
    }
}
