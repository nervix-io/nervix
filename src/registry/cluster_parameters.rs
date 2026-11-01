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
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::{ClusterParameterKey, ClusterParameterValues, DomainName, Model};

use super::RegistryError;

pub(crate) fn validate_resource_parameters(
    resources: &[nervix_models::ResourceVersion],
    parameters: &ClusterParameterValues,
) -> error_stack::Result<(), RegistryError> {
    struct Requirement {
        parameter: ClusterParameterKey,
        actual: u64,
        maximum: u64,
    }

    for resource in resources {
        for Requirement {
            parameter,
            actual,
            maximum,
        } in [
            Requirement {
                parameter: ClusterParameterKey::ResourceMaxArchiveBytes,
                actual: resource.archive_bytes,
                maximum: parameters.resource_max_archive_bytes(),
            },
            Requirement {
                parameter: ClusterParameterKey::ResourceMaxExtractedBytes,
                actual: resource.total_bytes,
                maximum: parameters.resource_max_extracted_bytes(),
            },
            Requirement {
                parameter: ClusterParameterKey::ResourceMaxFileCount,
                actual: resource.file_count,
                maximum: parameters.resource_max_file_count(),
            },
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

fn validate_ingestor_header_parameters(
    domain: &DomainName,
    ingestor: &nervix_models::CreateIngestor,
    parameters: &ClusterParameterValues,
    check: impl Fn(ClusterParameterKey, u64, u64) -> error_stack::Result<(), RegistryError>,
) -> error_stack::Result<(), RegistryError> {
    if let Some(nervix_models::IngestorEntity::Websockets(
        nervix_models::WebsocketsIngestor::Connection(source),
    )) = ingestor.input.transport_entity()
    {
        let headers = nervix_models::WebsocketsUpgradeHeaders::parse(&source.headers)
            .change_context(RegistryError::InvalidWebsocketsUpgradeHeaders {
                domain: domain.clone(),
                ingestor: ingestor.name.clone().into(),
            })?;
        check(
            ClusterParameterKey::HttpHeaderFields,
            headers.iter().count().arch_into(),
            parameters.http_header_fields(),
        )?;
        let mut total = 0_u64;
        for (name, value) in headers.iter() {
            let bytes = name
                .as_str()
                .len()
                .checked_add(value.as_str().len())
                .assured("literal header bytes fit the process address space");
            let bytes: u64 = bytes.arch_into();
            check(
                ClusterParameterKey::HttpHeaderBytes,
                bytes,
                parameters.http_header_bytes(),
            )?;
            total = total
                .checked_add(bytes)
                .assured("literal headers fit the process address space");
        }
        check(
            ClusterParameterKey::HttpHeaderBytes,
            total,
            parameters.http_header_bytes(),
        )?;
    }
    if let Some(nervix_models::IngestorEntity::Http(nervix_models::HttpIngestor::Request(source))) =
        ingestor.input.transport_entity()
    {
        let mut headers = std::collections::BTreeMap::<String, u64>::new();
        for header in &source.headers {
            let bytes = header
                .name
                .len()
                .checked_add(header.value.len())
                .assured("literal header bytes fit the process address space");
            check(
                ClusterParameterKey::HttpHeaderBytes,
                bytes.arch_into(),
                parameters.http_header_bytes(),
            )?;
            headers.insert(
                header.name.to_ascii_lowercase(),
                header.value.len().arch_into(),
            );
        }
        check(
            ClusterParameterKey::HttpHeaderFields,
            headers.len().arch_into(),
            parameters.http_header_fields(),
        )?;
        let total = headers.iter().fold(0_u64, |total, (name, value_bytes)| {
            total
                .checked_add(name.len().arch_into())
                .assured("literal headers fit the process address space")
                .checked_add(*value_bytes)
                .assured("literal headers fit the process address space")
        });
        check(
            ClusterParameterKey::HttpHeaderBytes,
            total,
            parameters.http_header_bytes(),
        )?;
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
            Model::Ingestor(ingestor) => {
                validate_ingestor_header_parameters(domain, ingestor, parameters, check)?;
            }
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
            envelope: Default::default(),
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

    fn polling_headers(headers: Vec<nervix_models::HttpRequestHeader>) -> Model {
        use nervix_models::{
            CodecName, CreateIngestor, GeneralErrorPolicy, HttpIngestor, HttpRequestIngestMode,
            HttpRequestIngestor, IngestQuiesceMode, IngestorEntity, IngestorInput, IngestorName,
            TransportIngestorInput,
        };

        Model::Ingestor(CreateIngestor {
            name: IngestorName::parse("polling").assured("the fixture name is valid"),
            output_routes: crate::registry::test_fixtures::unbranched_transforming_outputs(
                "events",
            ),
            input: IngestorInput::Transport(TransportIngestorInput {
                source: IngestorEntity::Http(HttpIngestor::Request(HttpRequestIngestor {
                    client: ClientName::parse("api").assured("the fixture client is valid"),
                    method: "GET".to_string(),
                    headers,
                    max_body_size: None,
                    every: "1s".parse().assured("the fixture cadence is positive"),
                    mode: HttpRequestIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Suspend,
                })),
                codec: CodecName::parse("events").assured("the fixture codec is valid"),
            }),
            timestamp_source: None,
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        })
    }

    fn websocket_headers(headers: Vec<nervix_models::WebsocketsUpgradeHeader>) -> Model {
        let Model::Ingestor(mut ingestor) = polling_headers(Vec::new()) else {
            unreachable!("the fixture builds an ingestor");
        };
        ingestor.input =
            nervix_models::IngestorInput::Transport(nervix_models::TransportIngestorInput {
                source: nervix_models::IngestorEntity::Websockets(
                    nervix_models::WebsocketsIngestor::Connection(
                        nervix_models::WebsocketsConnectionIngestor {
                            client: ClientName::parse("api").assured("the fixture client is valid"),
                            headers,
                            mode: nervix_models::WebsocketsIngestMode::NoAck,
                            quiesce: nervix_models::IngestQuiesceMode::Drop,
                        },
                    ),
                ),
                codec: nervix_models::CodecName::parse("events")
                    .assured("the fixture codec is valid"),
            });
        Model::Ingestor(ingestor)
    }

    #[test]
    fn websocket_header_requirements_use_committed_caps_and_the_canonical_offer() {
        let domain = DomainName::parse("scope").assured("the fixture domain is valid");
        let header = |name: &str, value: &str| nervix_models::WebsocketsUpgradeHeader {
            name: name.to_string(),
            value: value.to_string(),
        };
        let model = websocket_headers(vec![
            header("x-key", "v"),
            header("Sec-WebSocket-Protocol", "chat, events"),
        ]);
        validate_model_parameters(&domain, [&model], &parameters(2, 39))
            .assured("canonical fields need exactly39bytes and two fields");
        let fields = validate_model_parameters(&domain, [&model], &parameters(1, 39))
            .expect_err("the offer is one of the two application fields");
        assert!(matches!(
            fields.current_context(),
            RegistryError::ClusterParameterLimit {
                parameter: ClusterParameterKey::HttpHeaderFields,
                actual: 2,
                maximum: 1,
                ..
            }
        ));
        let bytes = validate_model_parameters(&domain, [&model], &parameters(2, 38))
            .expect_err("the complete canonical envelope needs39bytes");
        assert!(matches!(
            bytes.current_context(),
            RegistryError::ClusterParameterLimit {
                parameter: ClusterParameterKey::HttpHeaderBytes,
                actual: 39,
                maximum: 38,
                ..
            }
        ));
        let model = websocket_headers(
            (0..129)
                .map(|index| header(&format!("x-field-{index}"), "v"))
                .collect(),
        );
        validate_model_parameters(&domain, [&model], &parameters(129, 4096))
            .assured("a raised committed field cap admits the complete upgrade");
        let value = "v".repeat(32 * 1024);
        let model = websocket_headers(vec![header("x-key", &value)]);
        validate_model_parameters(&domain, [&model], &parameters(1, 32773))
            .assured("a raised committed byte cap admits the complete upgrade");
        let bytes = validate_model_parameters(&domain, [&model], &parameters(1, 32772))
            .expect_err("the individual field also keeps its captured byte cap");
        assert!(matches!(
            bytes.current_context(),
            RegistryError::ClusterParameterLimit {
                parameter: ClusterParameterKey::HttpHeaderBytes,
                actual: 32773,
                maximum: 32772,
                ..
            }
        ));
    }

    #[test]
    fn polling_header_requirements_use_the_committed_caps_and_final_literal_values() {
        let domain = DomainName::parse("scope").assured("the fixture domain is valid");
        let model = polling_headers(vec![
            nervix_models::HttpRequestHeader {
                name: "x-key".to_string(),
                value: "a".to_string(),
            },
            nervix_models::HttpRequestHeader {
                name: "X-KEY".to_string(),
                value: "bb".to_string(),
            },
        ]);
        validate_model_parameters(&domain, [&model], &parameters(1, 7))
            .assured("case-insensitive replacement keeps one field and the final value");
        let error = validate_model_parameters(&domain, [&model], &parameters(1, 6))
            .expect_err("the complete retained header needs seven bytes");
        assert!(matches!(
            error.current_context(),
            RegistryError::ClusterParameterLimit {
                parameter: ClusterParameterKey::HttpHeaderBytes,
                actual: 7,
                maximum: 6,
                ..
            }
        ));
        let model = polling_headers(
            (0..129)
                .map(|index| nervix_models::HttpRequestHeader {
                    name: format!("x-key-{index}"),
                    value: "a".to_string(),
                })
                .collect(),
        );
        validate_model_parameters(&domain, [&model], &parameters(129, 4096))
            .assured("a raised committed cap admits every literal header");
        let error = validate_model_parameters(&domain, [&model], &parameters(128, 4096))
            .expect_err("the model needs its complete distinct-field count");
        assert!(matches!(
            error.current_context(),
            RegistryError::ClusterParameterLimit {
                parameter: ClusterParameterKey::HttpHeaderFields,
                actual: 129,
                maximum: 128,
                ..
            }
        ));
    }

    #[test]
    fn the_maximum_application_header_count_leaves_transport_capacity() {
        let maximum = usize::try_from(nervix_models::HttpHeaderFields::MAXIMUM)
            .assured("the native header maximum fits this target");
        let mut headers = http::HeaderMap::with_capacity(maximum);
        for index in 0..maximum {
            let name = http::header::HeaderName::from_bytes(format!("x-limit-{index}").as_bytes())
                .assured("the generated header name is valid");
            headers
                .try_insert(name, http::header::HeaderValue::from_static("v"))
                .assured("each field up to the declared maximum fits the native map");
        }
        assert_eq!(headers.len(), maximum);
        let transport_names = [
            http::header::ACCEPT,
            http::header::ACCEPT_ENCODING,
            http::header::AUTHORIZATION,
            http::header::CONTENT_LENGTH,
            http::header::CONTENT_TYPE,
            http::header::HOST,
            http::header::PROXY_AUTHORIZATION,
            http::header::REFERER,
            http::header::TRANSFER_ENCODING,
            http::header::USER_AGENT,
        ];
        let transport_fields = transport_names.len();
        for name in transport_names {
            headers
                .try_insert(name, http::header::HeaderValue::from_static("v"))
                .assured("the application maximum leaves room for transport-owned fields");
        }
        assert_eq!(headers.len(), maximum + transport_fields);
    }

    #[test]
    fn resource_parameters_accept_exact_bounds_and_identify_each_exceeded_requirement() {
        use nervix_models::{
            ClusterNodeName, ResourceId, ResourceName, ResourceVersion, Timestamp,
        };

        struct RefusedResourceBound {
            change: ClusterParameterChange,
            parameter: ClusterParameterKey,
            actual: u64,
            maximum: u64,
        }

        let resource = ResourceVersion {
            id: ResourceId::new(
                DomainName::parse("scope").assured("the fixture domain name is valid"),
                ResourceName::parse("bundle").assured("the fixture resource name is valid"),
                1,
            ),
            root_checksum: "0".repeat(64),
            manifest_checksum: "0".repeat(64),
            file_count: 3,
            total_bytes: 20,
            archive_bytes: 10,
            created_at: Timestamp::from_unix_nanos(0),
            created_by_node: ClusterNodeName::parse("node-1")
                .assured("the fixture node name is valid"),
        };
        let updates = ClusterParameterUpdates::try_from(vec![
            ClusterParameterChange::ResourceMaxArchiveBytes(10),
            ClusterParameterChange::ResourceMaxExtractedBytes(20),
            ClusterParameterChange::ResourceMaxFileCount(3),
        ])
        .assured("the fixture assigns each resource bound once");
        let exact = ClusterParameterValues::default()
            .changed(&updates)
            .assured("the fixture resource bounds are positive");
        validate_resource_parameters(std::slice::from_ref(&resource), &exact)
            .assured("an existing resource fits all three exact bounds");

        for refused in [
            RefusedResourceBound {
                change: ClusterParameterChange::ResourceMaxArchiveBytes(9),
                parameter: ClusterParameterKey::ResourceMaxArchiveBytes,
                actual: 10,
                maximum: 9,
            },
            RefusedResourceBound {
                change: ClusterParameterChange::ResourceMaxExtractedBytes(19),
                parameter: ClusterParameterKey::ResourceMaxExtractedBytes,
                actual: 20,
                maximum: 19,
            },
            RefusedResourceBound {
                change: ClusterParameterChange::ResourceMaxFileCount(2),
                parameter: ClusterParameterKey::ResourceMaxFileCount,
                actual: 3,
                maximum: 2,
            },
        ] {
            let updates = ClusterParameterUpdates::try_from(vec![refused.change])
                .assured("the fixture changes one bound");
            let limits = exact
                .changed(&updates)
                .assured("each lower fixture bound is positive");
            let error = validate_resource_parameters(std::slice::from_ref(&resource), &limits)
                .expect_err("the resource exceeds the lower bound");
            assert_eq!(
                error.current_context(),
                &RegistryError::ResourceParameterLimit {
                    resource: resource.id.clone(),
                    parameter: refused.parameter,
                    actual: refused.actual,
                    maximum: refused.maximum,
                }
            );
        }
    }

    #[test]
    fn dynamic_values_keep_every_known_header_name_in_the_parameter_requirements() {
        struct RefusedTransition {
            limits: ClusterParameterValues,
            parameter: ClusterParameterKey,
            actual: u64,
            maximum: u64,
        }

        let domain = DomainName::parse("scope").assured("the fixture domain name is valid");
        let model = emitter(vec![
            header(literal("x-first"), input("value")),
            header(literal("x-second"), input("value")),
        ]);
        for refused in [
            RefusedTransition {
                limits: parameters(1, 15),
                parameter: ClusterParameterKey::HttpHeaderFields,
                actual: 2,
                maximum: 1,
            },
            RefusedTransition {
                limits: parameters(2, 14),
                parameter: ClusterParameterKey::HttpHeaderBytes,
                actual: 15,
                maximum: 14,
            },
        ] {
            let error = validate_model_parameters(&domain, [&model], &refused.limits)
                .expect_err("the known header names exceed the proposed bound");
            assert_eq!(
                error.current_context(),
                &RegistryError::ClusterParameterLimit {
                    domain: domain.clone(),
                    node: model.node_ref(),
                    parameter: refused.parameter,
                    actual: refused.actual,
                    maximum: refused.maximum,
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
