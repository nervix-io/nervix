use error_stack::ResultExt as _;
use nervix_connector::{ParsedRetryPolicy, SourceAckPolicy};
use nervix_models::{IngestAcknowledgement, parse_duration_text};

use super::{
    ingestors::{DeliverySetting, IngestorStartError, SourceStartError},
    *,
};

/// A relay, processor, generator or emitter setting whose value does not parse. The node that
/// declares the setting names itself in the context its caller adds above this one.
#[derive(Debug, Error)]
pub(crate) enum NodeSettingError {
    /// The duration parser's own error is beneath.
    #[error("invalid {setting} '{value}'")]
    Duration {
        setting: NodeDurationSetting,
        value: String,
    },
    #[error("invalid {setting} '{value}': {cause}")]
    ByteSize {
        setting: NodeByteSizeSetting,
        value: String,
        cause: ubyte::Error,
    },
}

/// A duration a relay, processor, generator or emitter declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(crate) enum NodeDurationSetting {
    #[strum(serialize = "flush_each")]
    FlushEach,
    #[strum(serialize = "collect_for")]
    CollectFor,
}

/// A byte size a relay, processor, generator or emitter declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(crate) enum NodeByteSizeSetting {
    #[strum(serialize = "max_batch_size")]
    MaxBatchSize,
    #[strum(serialize = "input collection max_batch_size")]
    CollectMaxBatchSize,
}

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

    /// Parses one duration setting of a relay, processor, generator or emitter.
    pub(in crate::runtime) fn parse_runtime_node_duration_setting(
        setting: NodeDurationSetting,
        value: &str,
    ) -> error_stack::Result<Duration, NodeSettingError> {
        parse_duration_text(value).change_context_lazy(|| NodeSettingError::Duration {
            setting,
            value: value.to_string(),
        })
    }

    /// Parses one byte size setting of a relay, processor, generator or emitter.
    fn parse_runtime_node_byte_size(
        setting: NodeByteSizeSetting,
        value: &str,
    ) -> error_stack::Result<u64, NodeSettingError> {
        match value.parse::<ubyte::ByteUnit>() {
            Ok(size) => Ok(size.as_u64()),
            Err(cause) => Err(Report::new(NodeSettingError::ByteSize {
                setting,
                value: value.to_string(),
                cause,
            })),
        }
    }

    /// Parses the flush policy a route or emitter declares. The node that declares it names
    /// itself in the context it adds above a failure.
    pub(in crate::runtime) fn parse_runtime_node_flush_policy(
        policy: &FlushPolicy,
    ) -> error_stack::Result<RuntimeFlushPolicy, NodeSettingError> {
        let FlushPolicy::Each {
            interval,
            max_batch_size,
        } = policy
        else {
            return Ok(RuntimeFlushPolicy::Immediate);
        };
        let interval =
            Self::parse_runtime_node_duration_setting(NodeDurationSetting::FlushEach, interval)?;
        let max_batch_size =
            Self::parse_runtime_node_byte_size(NodeByteSizeSetting::MaxBatchSize, max_batch_size)?;
        Ok(RuntimeFlushPolicy::Each {
            interval,
            max_batch_size,
        })
    }

    /// Parses the input collection policy a node declares, when it declares one.
    pub(in crate::runtime) fn parse_runtime_node_input_collect_policy(
        policy: Option<&nervix_models::InputCollectPolicy>,
    ) -> error_stack::Result<Option<RuntimeInputCollectPolicy>, NodeSettingError> {
        let Some(policy) = policy else {
            return Ok(None);
        };
        let interval = Self::parse_runtime_node_duration_setting(
            NodeDurationSetting::CollectFor,
            &policy.collect_for,
        )?;
        let max_batch_size = match policy.max_batch_size.as_deref() {
            Some(max_batch_size) => Some(Self::parse_runtime_node_byte_size(
                NodeByteSizeSetting::CollectMaxBatchSize,
                max_batch_size,
            )?),
            None => None,
        };
        Ok(Some(RuntimeInputCollectPolicy {
            interval,
            max_batch_size,
        }))
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

            let error =
                Runtime::parse_runtime_node_duration_setting(NodeDurationSetting::FlushEach, value)
                    .expect_err("the flush interval names no duration");
            assert!(
                matches!(
                    error.current_context(),
                    NodeSettingError::Duration {
                        setting: NodeDurationSetting::FlushEach,
                        value: invalid,
                    } if invalid == value
                ),
                "{error:?}"
            );
            assert_eq!(
                format!("{error:#}"),
                format!("invalid flush_each '{value}': {why}")
            );
        }
    }

    #[test]
    fn node_flush_and_collection_policies_name_the_setting_that_does_not_parse() {
        assert_eq!(
            Runtime::parse_runtime_node_flush_policy(&FlushPolicy::Immediate)
                .expect("an immediate flush has nothing to parse"),
            RuntimeFlushPolicy::Immediate
        );
        assert_eq!(
            Runtime::parse_runtime_node_flush_policy(&FlushPolicy::Each {
                interval: "250ms".to_string(),
                max_batch_size: "1KiB".to_string(),
            })
            .expect("a valid cadence parses"),
            RuntimeFlushPolicy::Each {
                interval: Duration::from_millis(250),
                max_batch_size: 1024,
            }
        );
        let error = Runtime::parse_runtime_node_flush_policy(&FlushPolicy::Each {
            interval: "250ms".to_string(),
            max_batch_size: "lots".to_string(),
        })
        .expect_err("the batch size names no size");
        assert!(
            matches!(
                error.current_context(),
                NodeSettingError::ByteSize {
                    setting: NodeByteSizeSetting::MaxBatchSize,
                    value,
                    ..
                } if value == "lots"
            ),
            "{error:?}"
        );
        assert!(
            format!("{error:#}").starts_with("invalid max_batch_size 'lots': "),
            "{error:#}"
        );

        assert_eq!(
            Runtime::parse_runtime_node_input_collect_policy(None)
                .expect("an absent collection policy has nothing to parse"),
            None
        );
        let collect =
            |collect_for: &str, max_batch_size: Option<&str>| nervix_models::InputCollectPolicy {
                collect_for: collect_for.to_string(),
                max_batch_size: max_batch_size.map(str::to_string),
            };
        assert_eq!(
            Runtime::parse_runtime_node_input_collect_policy(Some(&collect("1s", Some("2KiB"))))
                .expect("a valid collection policy parses"),
            Some(RuntimeInputCollectPolicy {
                interval: Duration::from_secs(1),
                max_batch_size: Some(2048),
            })
        );
        let error = Runtime::parse_runtime_node_input_collect_policy(Some(&collect("oops", None)))
            .expect_err("the collection interval names no duration");
        assert!(
            matches!(
                error.current_context(),
                NodeSettingError::Duration {
                    setting: NodeDurationSetting::CollectFor,
                    ..
                }
            ),
            "{error:?}"
        );
        let error =
            Runtime::parse_runtime_node_input_collect_policy(Some(&collect("1s", Some("lots"))))
                .expect_err("the collection batch size names no size");
        assert!(
            format!("{error:#}").starts_with("invalid input collection max_batch_size 'lots': "),
            "{error:#}"
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
            names_invalid_setting(&err, &ingestor, DeliverySetting::RetryBackoff),
            "{err:?}"
        );
    }
}
