//! Predicate capability used by session subscriptions.
//!
//! Layer: data plane.
//!
//! - **Owns.** Compiling and executing the predicate-only capability that selects subscription
//!   records.
//! - **Depends on.** Structured expressions, relay row views, schema metadata, UDF execution, and
//!   the expression VM.
//! - **Must not know.** Subscription lifecycle, sampling, delivery, branch values, materialized
//!   state, output construction, or side effects.

use super::*;

/// A compiled subscription predicate whose general VM program cannot be observed or replaced.
#[derive(Debug, Clone)]
pub struct CompiledSubscriptionPredicate {
    predicate: VmCompiledPredicate,
}

/// The complete compilation context available at the subscription boundary.
#[derive(Debug, Clone)]
pub(crate) struct SubscriptionPredicateCompileContext<'a> {
    input_schema: StdArc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    udfs: Option<&'a UdfExecutor>,
}

impl<'a> SubscriptionPredicateCompileContext<'a> {
    pub(crate) fn new(
        input_schema: StdArc<arrow_schema::Schema>,
        input_sensitivity: VmSchemaSensitivity,
        udfs: Option<&'a UdfExecutor>,
    ) -> Self {
        Self {
            input_schema,
            input_sensitivity,
            udfs,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum SubscriptionPredicateExecutionError {
    #[error("failed to project the subscribed record into predicate inputs: {reason}")]
    InputProjection { reason: String },
    #[error("predicate VM execution failed: {reason}")]
    VmExecution { reason: String },
    #[error("predicate evaluation error {}: {reason} at {span}", reason.code().as_str())]
    Evaluation {
        reason: nervix_vm::SideErrorReason,
        span: nervix_vm::program::Span,
    },
}

pub(crate) fn compile_subscription_predicate(
    domain: &DomainName,
    subscription: &SubscriptionName,
    expression: &nervix_models::Expression,
    context: SubscriptionPredicateCompileContext<'_>,
) -> Result<CompiledSubscriptionPredicate, Report<RuntimeError>> {
    let expression = nervix_vm::lower_expression(
        expression,
        nervix_vm::SemanticScopePolicy::read_only("input"),
    )
    .map_err(|error| {
        let reason = format!(
            "subscription predicate for '{}' is invalid: {error}",
            subscription.as_str()
        );
        error.change_context(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason,
        })
    })?;
    let bindings = [
        VmCompileBinding::readonly("input", context.input_schema.clone())
            .with_sensitivity(context.input_sensitivity.clone()),
        VmCompileBinding::readonly("message", context.input_schema)
            .with_sensitivity(context.input_sensitivity),
    ];
    let mut options = VmPredicateCompileOptions::default();
    if let Some(udfs) = context.udfs {
        options.udf_signatures = udfs.signatures().clone();
        options.injector = Some(Arc::new(Box::new(udfs.clone())));
    }
    let predicate = compile_vm_predicate_with_options_for_bindings(&expression, bindings, options)
        .map_err(|error| {
            let reason = format!(
                "subscription predicate compile failed for '{}': {}",
                subscription.as_str(),
                error.current_context().message
            );
            error.change_context(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason,
            })
        })?;
    Ok(CompiledSubscriptionPredicate { predicate })
}

pub(crate) async fn execute_subscription_predicate_on_record(
    predicate: &CompiledSubscriptionPredicate,
    record: &RuntimeRow,
    execution_now: Timestamp,
) -> Result<bool, Report<SubscriptionPredicateExecutionError>> {
    let carrier = record.one_row_batch();
    let namespace_batches = [("input", &carrier), ("message", &carrier)];
    let strict_namespaces = ["input", "message"];
    let keys = [None];
    let side_inputs = HashMap::default();
    let lookup_columns = HashMap::default();
    let input = project_vm_input_batch(
        predicate.predicate.input_schema(),
        &VmInputProjectionSources {
            carrier: &carrier,
            namespace_batches: &namespace_batches,
            strict_namespaces: &strict_namespaces,
            keys: &keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: None,
        },
        None,
    )
    .map_err(|error| {
        let reason = error.current_context().to_string();
        error.change_context(SubscriptionPredicateExecutionError::InputProjection { reason })
    })?;
    let execution_context = VmExecutionContext::new(execution_now);
    let result = execute_vm_predicate_in_context(&predicate.predicate, &input, &execution_context)
        .await
        .map_err(|error| {
            let reason = error.current_context().to_string();
            error.change_context(SubscriptionPredicateExecutionError::VmExecution { reason })
        })?;
    if let Some(error) = result.errors().first() {
        return Err(Report::new(
            SubscriptionPredicateExecutionError::Evaluation {
                reason: error.reason.clone(),
                span: error.span,
            },
        ));
    }
    Ok(result.selected_rows().is_single(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::test_fixtures::{expression, named, test_schema};

    #[test]
    fn invalid_predicate_keeps_subscription_and_vm_compile_context() {
        let domain: DomainName = named("test_domain");
        let subscription: SubscriptionName = named("filtered_view");
        let schema = test_schema(&[("value", ParseAsType::I64)]);
        let report = compile_subscription_predicate(
            &domain,
            &subscription,
            &expression("input.missing > 0"),
            SubscriptionPredicateCompileContext::new(
                schema.arrow_schema(),
                VmSchemaSensitivity::default(),
                None,
            ),
        )
        .expect_err("a predicate cannot read an undeclared input field");
        assert!(matches!(
            report.current_context(),
            RuntimeError::BuildDomainExecution { domain, reason }
                if domain == "test_domain"
                    && reason.contains("filtered_view")
                    && reason.contains("missing")
        ));
        assert!(report.contains::<nervix_vm::CompileError>());
    }

    #[tokio::test]
    async fn missing_record_field_keeps_projection_report() {
        let domain: DomainName = named("test_domain");
        let subscription: SubscriptionName = named("filtered_view");
        let schema = test_schema(&[("value", ParseAsType::I64)]);
        let predicate = compile_subscription_predicate(
            &domain,
            &subscription,
            &expression("input.value > 0"),
            SubscriptionPredicateCompileContext::new(
                schema.arrow_schema(),
                VmSchemaSensitivity::default(),
                None,
            ),
        )
        .expect("a comparison of a declared I64 field is a valid predicate");
        let record =
            crate::runtime_schema::test_runtime_row([("other".to_string(), RuntimeValue::I64(1))]);
        let report = execute_subscription_predicate_on_record(
            &predicate,
            &record,
            Timestamp::from_unix_nanos(7),
        )
        .await
        .expect_err("the record lacks the predicate's required input field");
        assert!(matches!(
            report.current_context(),
            SubscriptionPredicateExecutionError::InputProjection { reason }
                if reason.contains("value")
        ));
        assert!(report.contains::<RuntimeSchemaError>());
    }
}
