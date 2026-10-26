//! Pure model-size checks against a captured committed cluster policy.
//!
//! Layer: decisions.
//! - **Owns.** Checking model requirements against the policy used to plan their publication.
//! - **Depends on.** Typed Models, parameter values and registry refusals.
//! - **Must not know.** Consensus, locks, runtime tasks or sessions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns parameter-dependent model validation")
)]

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_models::{ClusterParameterKey, ClusterParameterValues, DomainName, Model};

use super::RegistryError;

pub(crate) fn validate_resource_parameters(
    resources: &[nervix_models::ResourceVersion],
    parameters: &ClusterParameterValues,
) -> error_stack::Result<(), RegistryError> {
    for resource in resources {
        for (parameter, actual, maximum) in [
            (
                ClusterParameterKey::ResourceMaxArchiveBytes,
                resource.archive_bytes,
                parameters.resource_max_archive_bytes(),
            ),
            (
                ClusterParameterKey::ResourceMaxExtractedBytes,
                resource.total_bytes,
                parameters.resource_max_extracted_bytes(),
            ),
            (
                ClusterParameterKey::ResourceMaxFileCount,
                resource.file_count,
                parameters.resource_max_file_count(),
            ),
        ] {
            if actual > maximum {
                return Err(Report::new(RegistryError::ResourceParameterLimit {
                    resource: resource.id.clone(),
                    parameter,
                    actual,
                    maximum,
                }));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_model_parameters<'a>(
    domain: &DomainName,
    models: impl IntoIterator<Item = &'a Model>,
    parameters: &ClusterParameterValues,
) -> error_stack::Result<(), RegistryError> {
    for model in models {
        let check = |parameter: ClusterParameterKey, actual: u64, maximum: u64| {
            if actual > maximum {
                Err(Report::new(RegistryError::ClusterParameterLimit {
                    domain: domain.clone(),
                    node: model.node_ref(),
                    parameter,
                    actual,
                    maximum,
                }))
            } else {
                Ok(())
            }
        };
        match model {
            Model::Udf(udf) => check(
                ClusterParameterKey::UdfCodeBytes,
                udf.code.len().arch_into(),
                parameters.udf_code_bytes(),
            )?,
            Model::Emitter(emitter) => {
                if let Some(batch) = emitter.batch {
                    check(
                        ClusterParameterKey::MaxBatchMessages,
                        u64::from(batch.max_messages.get().get()),
                        parameters.max_batch_messages(),
                    )?;
                    if matches!(
                        emitter.connector.as_ref(),
                        nervix_models::EmitterEntity::Client(_)
                    ) {
                        check(
                            ClusterParameterKey::ClientConsumerSessionBytes,
                            batch.max_size.bytes().get(),
                            parameters.client_consumer_session_bytes(),
                        )?;
                    }
                }
                if matches!(
                    emitter.connector.as_ref(),
                    nervix_models::EmitterEntity::Http(_)
                ) {
                    let mut headers = std::collections::BTreeMap::<String, u64>::new();
                    for invocation in &emitter.construction.invocations {
                        let [name, value] = invocation.arguments.as_slice() else {
                            continue;
                        };
                        let minimum_value_bytes = match value {
                            nervix_models::Expression::Literal(nervix_models::Literal::String(
                                value,
                            )) => value.len().arch_into(),
                            _ => 0,
                        };
                        let minimum_name_bytes;
                        if let nervix_models::Expression::Literal(nervix_models::Literal::String(
                            name,
                        )) = name
                        {
                            minimum_name_bytes = name.len().arch_into();
                            headers.insert(name.to_ascii_lowercase(), minimum_value_bytes);
                        } else {
                            minimum_name_bytes = 0;
                            // A dynamic name can replace any earlier value. Its bytes do not
                            // remove a known name, but no earlier value remains a guaranteed
                            // contribution to the final envelope's byte lower bound.
                            for minimum in headers.values_mut() {
                                *minimum = 0;
                            }
                        }
                        check(
                            ClusterParameterKey::HttpHeaderBytes,
                            u64::checked_add(minimum_name_bytes, minimum_value_bytes)
                                .assured("known header bytes fit the process address space"),
                            parameters.http_header_bytes(),
                        )?;
                    }
                    check(
                        ClusterParameterKey::HttpHeaderFields,
                        headers.len().arch_into(),
                        parameters.http_header_fields(),
                    )?;
                    let total = headers
                        .iter()
                        .fold(0_u64, |total, (name, minimum_value_bytes)| {
                            total
                                .checked_add(name.len().arch_into())
                                .assured("known headers fit the process address space")
                                .checked_add(*minimum_value_bytes)
                                .assured("known headers fit the process address space")
                        });
                    check(
                        ClusterParameterKey::HttpHeaderBytes,
                        total,
                        parameters.http_header_bytes(),
                    )?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        AckMode, BuiltinFunctionName, ClientName, ClusterParameterChange, ClusterParameterUpdates,
        CreateEmitter, EmitterBody, EmitterEntity, EmitterName, ErrorPolicies, Expression,
        FieldName, FieldReference, FieldScope, FlushPolicy, HttpEmitter, HttpRequestEmitter,
        HttpRequestPublishingMode, Invocation, Literal, ProcessorInputs, RelayName, RetryPolicy,
        RouteConstruction,
    };

    use super::*;

    fn literal(value: &str) -> Expression {
        Expression::Literal(Literal::String(value.to_string()))
    }

    fn input(field: &str) -> Expression {
        Expression::Field(FieldReference::scoped(
            FieldScope::Input,
            FieldName::parse(field).assured("the fixture field name is valid"),
        ))
    }

    fn header(name: Expression, value: Expression) -> Invocation {
        Invocation {
            function: BuiltinFunctionName::parse("write_header")
                .assured("the fixture names the declared header function"),
            arguments: vec![name, value],
        }
    }

    fn emitter(invocations: Vec<Invocation>) -> Model {
        Model::Emitter(CreateEmitter {
            name: EmitterName::parse("headers").assured("the fixture emitter name is valid"),
            from: ProcessorInputs::single(
                RelayName::parse("events").assured("the fixture relay name is valid"),
            ),
            body: EmitterBody::WithoutBody,
            connector: Box::new(EmitterEntity::Http(HttpEmitter::Request(
                HttpRequestEmitter {
                    mode: HttpRequestPublishingMode::Ack {
                        retry_policy: RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    client: ClientName::parse("api").assured("the fixture client name is valid"),
                    method: literal("POST"),
                    path: literal("/events"),
                },
            ))),
            batch: None,
            flush_policy: FlushPolicy::Immediate,
            error_policies: ErrorPolicies::handled_by_log(),
            mode: AckMode::Attached,
            construction: RouteConstruction {
                invocations,
                ..Default::default()
            },
            materialized_state: Vec::new(),
        })
    }

    fn parameters(fields: u64, bytes: u64) -> ClusterParameterValues {
        let updates = ClusterParameterUpdates::try_from(vec![
            ClusterParameterChange::HttpHeaderFields(fields),
            ClusterParameterChange::HttpHeaderBytes(bytes),
        ])
        .assured("the fixture assigns each header bound once");
        ClusterParameterValues::default()
            .changed(&updates)
            .assured("the fixture header bounds are positive")
    }

    #[test]
    fn dynamic_values_keep_every_known_header_name_in_the_parameter_requirements() {
        let domain = DomainName::parse("scope").assured("the fixture domain name is valid");
        let model = emitter(vec![
            header(literal("x-first"), input("value")),
            header(literal("x-second"), input("value")),
        ]);
        for (limits, parameter, actual, maximum) in [
            (
                parameters(1, 15),
                ClusterParameterKey::HttpHeaderFields,
                2,
                1,
            ),
            (
                parameters(2, 14),
                ClusterParameterKey::HttpHeaderBytes,
                15,
                14,
            ),
        ] {
            let error = validate_model_parameters(&domain, [&model], &limits)
                .expect_err("the known header names exceed the proposed bound");
            assert_eq!(
                error.current_context(),
                &RegistryError::ClusterParameterLimit {
                    domain: domain.clone(),
                    node: model.node_ref(),
                    parameter,
                    actual,
                    maximum,
                }
            );
        }
        validate_model_parameters(&domain, [&model], &parameters(2, 15))
            .assured("two names totaling fifteen bytes fit when their dynamic values are empty");
    }

    #[test]
    fn a_dynamic_name_can_replace_an_earlier_literal_value_before_the_total_is_checked() {
        let domain = DomainName::parse("scope").assured("the fixture domain name is valid");
        let model = emitter(vec![
            header(literal("a"), literal("1234")),
            header(input("name"), literal("")),
            header(literal("b"), literal("1234")),
        ]);
        validate_model_parameters(&domain, [&model], &parameters(2, 7))
            .assured("a dynamic write of 'a' leaves six bytes in the complete envelope");

        let literal_model = emitter(vec![
            header(literal("a"), literal("1234")),
            header(literal("b"), literal("1234")),
        ]);
        let error = validate_model_parameters(&domain, [&literal_model], &parameters(2, 7))
            .expect_err("the complete literal envelope requires ten bytes");
        assert!(matches!(
            error.current_context(),
            RegistryError::ClusterParameterLimit {
                parameter: ClusterParameterKey::HttpHeaderBytes,
                actual: 10,
                maximum: 7,
                ..
            }
        ));
    }
}
