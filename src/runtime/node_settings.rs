use nervix_connector::{ParsedRetryPolicy, SourceAckPolicy};
use nervix_models::{IngestAcknowledgement, parse_duration_text};

use super::{
    ingestors::{DeliverySetting, IngestorStartError, SourceStartError},
    *,
};

impl Runtime {
    pub(in crate::runtime) fn parse_ack_timeout(
        domain: &DomainName,
        ingestor: &IngestorName,
        timeout: &str,
    ) -> error_stack::Result<Duration, IngestorStartError> {
        Self::parse_duration_setting(domain, ingestor, DeliverySetting::AckTimeout, timeout)
    }

    /// Parses the acknowledgement a delivery mode declares into the policy its source loop runs.
    ///
    /// A domain build parses every ingestor's declaration before it builds anything, so a mode
    /// whose durations do not parse fails the build instead of a later start.
    pub(in crate::runtime) fn parse_ingest_acknowledgement(
        domain: &DomainName,
        ingestor: &IngestorName,
        acknowledgement: IngestAcknowledgement<'_>,
    ) -> error_stack::Result<SourceAckPolicy, IngestorStartError> {
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
                let batch_timeout = Self::parse_duration_setting(
                    domain,
                    ingestor,
                    DeliverySetting::BatchTimeout,
                    batch_timeout,
                )?;
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

    /// Parses one duration `setting` of an ingestor's delivery mode. A value that does not parse
    /// fails the ingestor's initialization, naming the setting beneath it.
    pub(in crate::runtime) fn parse_duration_setting(
        domain: &DomainName,
        ingestor: &IngestorName,
        setting: DeliverySetting,
        value: &str,
    ) -> error_stack::Result<Duration, IngestorStartError> {
        parse_duration_text(value).map_err(|report| {
            Report::new(SourceStartError::InvalidDuration {
                setting,
                value: value.to_string(),
                source: report.current_context().clone(),
            })
            .change_context(IngestorStartError::Initialize {
                domain: domain.clone(),
                ingestor: ingestor.clone(),
            })
        })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the typed node identifier converts through the caller-supplied Into contract"
        )
    )]
    pub(in crate::runtime) fn parse_runtime_node_duration_setting(
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
        field: &str,
        value: &str,
    ) -> Result<Duration, RuntimeError> {
        let identifier = identifier.into();
        parse_duration_text(value).map_err(|source| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "invalid {field} '{value}' for {kind} '{}': {source}",
                identifier.as_str()
            ),
        })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the typed node identifier converts through the caller-supplied Into contract"
        )
    )]
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
    ) -> error_stack::Result<ParsedRetryPolicy, IngestorStartError> {
        Ok(ParsedRetryPolicy {
            backoff: Self::parse_duration_setting(
                domain,
                ingestor,
                DeliverySetting::RetryBackoff,
                &policy.backoff,
            )?,
            max_backoff: Self::parse_duration_setting(
                domain,
                ingestor,
                DeliverySetting::RetryMaxBackoff,
                &policy.max_backoff,
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nervix_models::RetryPolicy;

    use super::*;

    /// Whether `report` is `ingestor`'s failure to initialize, with the delivery `setting` whose
    /// value did not parse as its typed cause.
    fn names_invalid_setting(
        report: &Report<IngestorStartError>,
        ingestor: &IngestorName,
        setting: DeliverySetting,
    ) -> bool {
        let initialize = matches!(
            report.current_context(),
            IngestorStartError::Initialize { ingestor: failed, .. } if failed == ingestor
        );
        let cause = matches!(
            report.downcast_ref::<SourceStartError>(),
            Some(SourceStartError::InvalidDuration { setting: invalid, .. }) if *invalid == setting
        );
        initialize && cause
    }

    #[test]
    fn runtime_duration_parsers_validate_and_report_context() {
        let domain = domain("default");
        let ingestor = named("orders_ingestor");

        assert_eq!(
            Runtime::parse_ack_timeout(&domain, &ingestor, "2s").expect("valid timeout"),
            Duration::from_secs(2)
        );
        assert_eq!(
            Runtime::parse_duration_setting(
                &domain,
                &ingestor,
                DeliverySetting::BatchTimeout,
                "250ms"
            )
            .expect("valid duration"),
            Duration::from_millis(250)
        );

        let err = Runtime::parse_ack_timeout(&domain, &ingestor, "oops")
            .expect_err("invalid ack timeout");
        assert!(
            names_invalid_setting(&err, &ingestor, DeliverySetting::AckTimeout),
            "{err:?}"
        );
        assert!(
            format!("{err:#}").starts_with(
                "failed to initialize ingestor 'orders_ingestor' in domain 'default': invalid ack \
                 timeout 'oops': "
            ),
            "{err:#}"
        );

        let err = Runtime::parse_duration_setting(
            &domain,
            &ingestor,
            DeliverySetting::BatchTimeout,
            "oops",
        )
        .expect_err("invalid duration");
        assert!(
            names_invalid_setting(&err, &ingestor, DeliverySetting::BatchTimeout),
            "{err:?}"
        );
        assert!(
            format!("{err:#}").contains("invalid batch timeout 'oops'"),
            "{err:#}"
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
            names_invalid_setting(&err, &ingestor, DeliverySetting::RetryBackoff),
            "{err:?}"
        );
        assert!(
            format!("{err:#}").contains("invalid retry backoff 'oops'"),
            "{err:#}"
        );

        let bad_max_retry = RetryPolicy {
            backoff: "100ms".to_string(),
            max_backoff: "oops".to_string(),
        };
        let err = Runtime::parse_retry_policy(&domain, &ingestor, &bad_max_retry)
            .expect_err("invalid retry max_backoff");
        assert!(
            names_invalid_setting(&err, &ingestor, DeliverySetting::RetryMaxBackoff),
            "{err:?}"
        );
        assert!(
            format!("{err:#}").contains("invalid retry max backoff 'oops'"),
            "{err:#}"
        );
    }

    #[test]
    fn runtime_duration_parsers_say_why_text_names_no_duration() {
        let domain = domain("default");
        let ingestor = named("orders_ingestor");
        for (value, why) in [
            ("oops", "expected number at 0"),
            (
                TOO_LONG_DURATION_TEXT,
                "it is longer than a duration can be",
            ),
        ] {
            let error = Runtime::parse_ack_timeout(&domain, &ingestor, value)
                .expect_err("the ack timeout names no duration");
            assert!(
                names_invalid_setting(&error, &ingestor, DeliverySetting::AckTimeout),
                "{error:?}"
            );
            assert!(
                matches!(
                    error.downcast_ref::<SourceStartError>(),
                    Some(SourceStartError::InvalidDuration { source, .. })
                        if source.to_string() == why
                ),
                "{error:?}"
            );
            assert_eq!(
                format!("{error:#}"),
                format!(
                    "failed to initialize ingestor 'orders_ingestor' in domain 'default': invalid \
                     ack timeout '{value}': {why}"
                )
            );

            let error = Runtime::parse_duration_setting(
                &domain,
                &ingestor,
                DeliverySetting::BatchTimeout,
                value,
            )
            .expect_err("the batch timeout names no duration");
            assert!(
                names_invalid_setting(&error, &ingestor, DeliverySetting::BatchTimeout),
                "{error:?}"
            );
            assert!(
                format!("{error:#}").ends_with(&format!("invalid batch timeout '{value}': {why}")),
                "{error:#}"
            );

            let error = Runtime::parse_runtime_node_duration_setting(
                &domain,
                "emitter",
                named::<ModelName>("orders_emitter"),
                "flush_each",
                value,
            )
            .expect_err("the flush interval names no duration");
            let expected =
                format!("invalid flush_each '{value}' for emitter 'orders_emitter': {why}");
            assert!(
                matches!(
                    &error,
                    RuntimeError::BuildDomainExecution { reason, .. } if reason == &expected
                ),
                "{error:?}"
            );
        }
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
            names_invalid_setting(&err, &ingestor, DeliverySetting::RetryBackoff),
            "{err:?}"
        );
    }
}
