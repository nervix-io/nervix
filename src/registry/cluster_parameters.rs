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
                    let mut headers = std::collections::BTreeMap::new();
                    for invocation in &emitter.construction.invocations {
                        if let [
                            nervix_models::Expression::Literal(nervix_models::Literal::String(
                                name,
                            )),
                            nervix_models::Expression::Literal(nervix_models::Literal::String(
                                value,
                            )),
                        ] = invocation.arguments.as_slice()
                        {
                            check(
                                ClusterParameterKey::HttpHeaderBytes,
                                u64::checked_add(name.len().arch_into(), value.len().arch_into())
                                    .assured("literal header bytes fit the process address space"),
                                parameters.http_header_bytes(),
                            )?;
                            headers.insert(name.to_ascii_lowercase(), value);
                        }
                    }
                    check(
                        ClusterParameterKey::HttpHeaderFields,
                        headers.len().arch_into(),
                        parameters.http_header_fields(),
                    )?;
                    let total = headers.iter().fold(0_u64, |total, (name, value)| {
                        total
                            .checked_add(name.len().arch_into())
                            .assured("literal headers fit the process address space")
                            .checked_add(value.len().arch_into())
                            .assured("literal headers fit the process address space")
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
