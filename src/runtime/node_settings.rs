use super::*;

impl Runtime {
    pub(in crate::runtime) fn parse_ack_timeout(
        domain: &DomainName,
        ingestor: &IngestorName,
        timeout: &str,
    ) -> Result<Duration, RuntimeError> {
        humantime::parse_duration(timeout).map_err(|source| RuntimeError::StartIngestor {
            domain: domain.as_str().to_string(),
            ingestor: ingestor.as_str().to_string(),
            reason: format!("invalid ack timeout '{timeout}': {source}"),
        })
    }

    pub(super) fn validate_ingestor_start_settings(
        domain: &DomainName,
        ingestor: &CreateIngestor,
    ) -> Result<(), RuntimeError> {
        match &ingestor.source {
            IngestSource::Kafka { mode, .. } | IngestSource::Pulsar { mode, .. } => match mode {
                KafkaIngestMode::AckParallel {
                    batch_timeout,
                    timeout,
                    retry_policy,
                    ..
                } => {
                    Self::parse_duration_setting(
                        domain,
                        &ingestor.name,
                        "batch timeout",
                        batch_timeout,
                    )?;
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                    Self::parse_retry_policy(domain, &ingestor.name, retry_policy)?;
                }
                KafkaIngestMode::AckSequential {
                    timeout,
                    retry_policy,
                } => {
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                    Self::parse_retry_policy(domain, &ingestor.name, retry_policy)?;
                }
                KafkaIngestMode::NoAckParallel => {}
            },
            IngestSource::Mqtt { mode, .. } => match mode {
                MqttIngestMode::AckParallel {
                    batch_timeout,
                    timeout,
                    retry_policy,
                    ..
                } => {
                    Self::parse_duration_setting(
                        domain,
                        &ingestor.name,
                        "batch timeout",
                        batch_timeout,
                    )?;
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                    Self::parse_retry_policy(domain, &ingestor.name, retry_policy)?;
                }
                MqttIngestMode::AckSequential {
                    timeout,
                    retry_policy,
                } => {
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                    Self::parse_retry_policy(domain, &ingestor.name, retry_policy)?;
                }
                MqttIngestMode::NoAckParallel { .. } | MqttIngestMode::NoAckSequential { .. } => {}
            },
            IngestSource::RabbitMq { mode, .. } => match mode {
                RabbitMqIngestMode::AckSequential { timeout, .. } => {
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                }
            },
            IngestSource::Sqs { mode, .. } => match mode {
                SqsIngestMode::AckSequential { timeout, .. } => {
                    Self::parse_ack_timeout(domain, &ingestor.name, timeout)?;
                }
            },
            IngestSource::Http { .. }
            | IngestSource::Prometheus { .. }
            | IngestSource::RedisPubSub { .. }
            | IngestSource::Nats { .. }
            | IngestSource::ZeroMq { .. }
            | IngestSource::Websockets { .. }
            | IngestSource::Syslog { .. }
            | IngestSource::Endpoint { .. } => {}
        }
        Ok(())
    }

    pub(in crate::runtime) fn parse_duration_setting(
        domain: &DomainName,
        ingestor: &IngestorName,
        field: &str,
        value: &str,
    ) -> Result<Duration, RuntimeError> {
        humantime::parse_duration(value).map_err(|source| RuntimeError::StartIngestor {
            domain: domain.as_str().to_string(),
            ingestor: ingestor.as_str().to_string(),
            reason: format!("invalid {field} '{value}': {source}"),
        })
    }

    pub(in crate::runtime) fn parse_runtime_node_duration_setting(
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
        field: &str,
        value: &str,
    ) -> Result<Duration, RuntimeError> {
        let identifier = identifier.into();
        humantime::parse_duration(value).map_err(|source| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "invalid {field} '{value}' for {kind} '{}': {source}",
                identifier.as_str()
            ),
        })
    }

    pub(in crate::runtime) fn parse_runtime_node_flush_policy(
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
        value: &str,
        max_batch_size: Option<&str>,
    ) -> Result<RuntimeFlushPolicy, RuntimeError> {
        let identifier = identifier.into();
        if value.eq_ignore_ascii_case("IMMEDIATE") {
            Ok(RuntimeFlushPolicy::Immediate)
        } else {
            let interval = Self::parse_runtime_node_duration_setting(
                domain,
                kind,
                identifier.clone(),
                "flush_each",
                value,
            )?;
            let max_batch_size =
                max_batch_size.ok_or_else(|| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "{} '{}' FLUSH EACH requires MAX BATCH SIZE",
                        kind,
                        identifier.as_str()
                    ),
                })?;
            let max_batch_size = max_batch_size
                .parse::<ubyte::ByteUnit>()
                .map_err(|source| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "invalid max_batch_size '{}' for {} '{}': {}",
                        max_batch_size,
                        kind,
                        identifier.as_str(),
                        source
                    ),
                })?;
            Ok(RuntimeFlushPolicy::Each {
                interval,
                max_batch_size: max_batch_size.as_u64(),
            })
        }
    }

    pub(in crate::runtime) fn parse_runtime_node_input_collect_policy(
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
        policy: Option<&nervix_models::InputCollectPolicy>,
    ) -> Result<Option<RuntimeInputCollectPolicy>, RuntimeError> {
        let identifier = identifier.into();
        policy
            .map(|policy| {
                let interval = Self::parse_runtime_node_duration_setting(
                    domain,
                    kind,
                    identifier.clone(),
                    "collect_for",
                    &policy.collect_for,
                )?;
                let max_batch_size = policy
                    .max_batch_size
                    .as_deref()
                    .map(|max_batch_size| {
                        max_batch_size
                            .parse::<ubyte::ByteUnit>()
                            .map(|size| size.as_u64())
                            .map_err(|source| RuntimeError::BuildDomainExecution {
                                domain: domain.as_str().to_string(),
                                reason: format!(
                                    "invalid input collection max_batch_size '{}' for {} '{}': {}",
                                    max_batch_size,
                                    kind,
                                    identifier.as_str(),
                                    source
                                ),
                            })
                    })
                    .transpose()?;
                Ok(RuntimeInputCollectPolicy {
                    interval,
                    max_batch_size,
                })
            })
            .transpose()
    }

    pub(in crate::runtime) fn parse_retry_policy(
        domain: &DomainName,
        ingestor: &IngestorName,
        policy: &RetryPolicy,
    ) -> Result<ParsedRetryPolicy, RuntimeError> {
        Ok(ParsedRetryPolicy {
            backoff: Self::parse_duration_setting(
                domain,
                ingestor,
                "retry backoff",
                &policy.backoff,
            )?,
            max_backoff: Self::parse_duration_setting(
                domain,
                ingestor,
                "retry max backoff",
                &policy.max_backoff,
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::RetryPolicy;
    use tokio::time::Duration;

    use super::*;

    #[test]
    fn runtime_duration_parsers_validate_and_report_context() {
        let domain = domain("default");
        let ingestor = named("orders_ingestor");

        assert_eq!(
            Runtime::parse_ack_timeout(&domain, &ingestor, "2s").expect("valid timeout"),
            Duration::from_secs(2)
        );
        assert_eq!(
            Runtime::parse_duration_setting(&domain, &ingestor, "batch timeout", "250ms")
                .expect("valid duration"),
            Duration::from_millis(250)
        );

        let err = Runtime::parse_ack_timeout(&domain, &ingestor, "oops")
            .expect_err("invalid ack timeout");
        assert!(
            matches!(err, RuntimeError::StartIngestor { reason, .. } if reason.contains("invalid ack timeout 'oops'"))
        );

        let err = Runtime::parse_duration_setting(&domain, &ingestor, "batch timeout", "oops")
            .expect_err("invalid duration");
        assert!(
            matches!(err, RuntimeError::StartIngestor { reason, .. } if reason.contains("invalid batch timeout 'oops'"))
        );

        let retry = RetryPolicy {
            backoff: "100ms".to_string(),
            max_backoff: "1s".to_string(),
        };
        let parsed =
            Runtime::parse_retry_policy(&domain, &ingestor, &retry).expect("valid retry policy");
        assert_eq!(parsed.backoff, Duration::from_millis(100));
        assert_eq!(parsed.max_backoff, Duration::from_secs(1));

        let bad_retry = RetryPolicy {
            backoff: "oops".to_string(),
            max_backoff: "1s".to_string(),
        };
        let err = Runtime::parse_retry_policy(&domain, &ingestor, &bad_retry)
            .expect_err("invalid retry backoff");
        assert!(
            matches!(err, RuntimeError::StartIngestor { reason, .. } if reason.contains("invalid retry backoff 'oops'"))
        );

        let bad_max_retry = RetryPolicy {
            backoff: "100ms".to_string(),
            max_backoff: "oops".to_string(),
        };
        let err = Runtime::parse_retry_policy(&domain, &ingestor, &bad_max_retry)
            .expect_err("invalid retry max_backoff");
        assert!(
            matches!(err, RuntimeError::StartIngestor { reason, .. } if reason.contains("retry max backoff") && reason.contains("oops"))
        );
    }
}
