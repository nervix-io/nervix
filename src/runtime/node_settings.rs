use nervix_connector::{ParsedRetryPolicy, SourceAckPolicy};
use nervix_models::IngestAcknowledgement;

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

    /// Parses the acknowledgement a delivery mode declares into the policy its source loop runs.
    ///
    /// A domain build parses every ingestor's declaration before it builds anything, so a mode
    /// whose durations do not parse fails the build instead of a later start.
    pub(in crate::runtime) fn parse_ingest_acknowledgement(
        domain: &DomainName,
        ingestor: &IngestorName,
        acknowledgement: IngestAcknowledgement<'_>,
    ) -> Result<SourceAckPolicy, RuntimeError> {
        match acknowledgement {
            IngestAcknowledgement::Unacknowledged => Ok(SourceAckPolicy::None),
            IngestAcknowledgement::Sequential { timeout, retry } => {
                let timeout = Self::parse_ack_timeout(domain, ingestor, timeout)?;
                let retry = Self::parse_retry_policy(domain, ingestor, retry)?;
                Ok(SourceAckPolicy::Sequential { timeout, retry })
            }
            IngestAcknowledgement::Parallel {
                max,
                batch_timeout,
                timeout,
                retry,
            } => {
                let batch_timeout =
                    Self::parse_duration_setting(domain, ingestor, "batch timeout", batch_timeout)?;
                let timeout = Self::parse_ack_timeout(domain, ingestor, timeout)?;
                let retry = Self::parse_retry_policy(domain, ingestor, retry)?;
                Ok(SourceAckPolicy::Parallel {
                    max_in_flight: addressable_count(max),
                    batch_timeout,
                    timeout,
                    retry,
                })
            }
        }
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
        policy: &FlushPolicy,
    ) -> Result<RuntimeFlushPolicy, RuntimeError> {
        let identifier = identifier.into();
        let FlushPolicy::Each {
            interval,
            max_batch_size,
        } = policy
        else {
            return Ok(RuntimeFlushPolicy::Immediate);
        };
        let interval = Self::parse_runtime_node_duration_setting(
            domain,
            kind,
            identifier.clone(),
            "flush_each",
            interval,
        )?;
        let parsed_max_batch_size =
            max_batch_size
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
            max_batch_size: parsed_max_batch_size.as_u64(),
        })
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

    #[test]
    fn declared_acknowledgements_parse_into_the_policy_a_source_loop_runs() {
        let domain = domain("default");
        let ingestor = named("orders_ingestor");
        let retry = RetryPolicy {
            backoff: "100ms".to_string(),
            max_backoff: "1s".to_string(),
        };
        let parsed_retry = ParsedRetryPolicy {
            backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        };

        assert_eq!(
            Runtime::parse_ingest_acknowledgement(
                &domain,
                &ingestor,
                IngestAcknowledgement::Unacknowledged,
            )
            .expect("an unacknowledged mode has nothing to parse"),
            SourceAckPolicy::None
        );
        assert_eq!(
            Runtime::parse_ingest_acknowledgement(
                &domain,
                &ingestor,
                IngestAcknowledgement::Sequential {
                    timeout: "2s",
                    retry: &retry,
                },
            )
            .expect("valid sequential mode"),
            SourceAckPolicy::Sequential {
                timeout: Duration::from_secs(2),
                retry: parsed_retry,
            }
        );
        assert_eq!(
            Runtime::parse_ingest_acknowledgement(
                &domain,
                &ingestor,
                IngestAcknowledgement::Parallel {
                    max: NonZeroU64::new(4).assured("four is non-zero"),
                    batch_timeout: "250ms",
                    timeout: "2s",
                    retry: &retry,
                },
            )
            .expect("valid parallel mode"),
            SourceAckPolicy::Parallel {
                max_in_flight: NonZeroUsize::new(4).assured("four is non-zero"),
                batch_timeout: Duration::from_millis(250),
                timeout: Duration::from_secs(2),
                retry: parsed_retry,
            }
        );

        // A retry policy the mode declares is checked along with its timeout, whichever delivery
        // mode declares it.
        let bad_retry = RetryPolicy {
            backoff: "oops".to_string(),
            max_backoff: "1s".to_string(),
        };
        let err = Runtime::parse_ingest_acknowledgement(
            &domain,
            &ingestor,
            IngestAcknowledgement::Sequential {
                timeout: "2s",
                retry: &bad_retry,
            },
        )
        .expect_err("invalid retry backoff");
        assert!(
            matches!(err, RuntimeError::StartIngestor { reason, .. } if reason.contains("invalid retry backoff 'oops'"))
        );
    }
}
