//! The source composition root, and the one start path every ingestor takes.
//!
//! Layer: data plane.
//!
//! - **Owns.** Starting an ingestor: preparing its quiescence, refusing a second start, compiling
//!   its dependencies, and mapping its source plan to the connector that runs it, which is the only
//!   place a source plan's kind selects anything.
//! - **Depends on.** Ingestor start plans, the host source launcher, every source connector, and
//!   the endpoint source the server keeps.
//! - **Must not know.** NSPL parsing, registry validation, or placement computation.

use super::*;

pub(in crate::runtime) mod endpoint;
pub(in crate::runtime) mod http;
pub(in crate::runtime) mod kafka;
pub(in crate::runtime) mod mqtt;
pub(in crate::runtime) mod nats;
pub(in crate::runtime) mod prometheus;
pub(in crate::runtime) mod pulsar;
pub(in crate::runtime) mod rabbitmq;
pub(in crate::runtime) mod redis_pubsub;
mod source;
pub(in crate::runtime) mod sqs;
pub(in crate::runtime) mod syslog;
pub(in crate::runtime) mod websockets;
pub(in crate::runtime) mod zeromq;

impl Runtime {
    /// Starts one ingestor. Every source takes this path.
    ///
    /// Everything that can fail comes first: the ingestor's dependencies compile and its source
    /// plan composes into opened connector instances. Only then does the host register the source
    /// and start its tasks, so an ingestor that cannot start leaves nothing running behind it.
    pub(in crate::runtime) async fn start_ingestor(
        &self,
        plan: IngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let IngestorStartPlan { ingestor, source } = plan;
        let quiesce = self.prepare_ingestor_quiescence(&ingestor.domain, &ingestor);
        if self.inner.ingestors.contains_key(&ingestor.runtime_key()) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: ingestor.domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let dependencies = self
            .ingestor_dependencies(&ingestor.domain, &ingestor)
            .await?;
        let source = match source {
            SourceStartPlan::Http(plan) => plan.compose(self, &ingestor).await?,
            SourceStartPlan::Kafka(plan) => plan.compose(self, &ingestor).await?,
            SourceStartPlan::Pulsar(plan) => plan.compose(self, &ingestor).await?,
            SourceStartPlan::Mqtt(plan) => plan.compose(self, &ingestor).await?,
            SourceStartPlan::Nats(plan) => plan.compose(self, &ingestor).await?,
            SourceStartPlan::RabbitMq(plan) => plan.compose(self, &ingestor).await?,
            SourceStartPlan::RedisPubSub(plan) => plan.compose(self, &ingestor).await?,
            SourceStartPlan::Prometheus(plan) => plan.compose(self, &ingestor).await?,
            SourceStartPlan::ZeroMq(plan) => plan.compose(&ingestor).await?,
            SourceStartPlan::Sqs(plan) => plan.compose(self, &ingestor).await?,
            SourceStartPlan::Endpoint(plan) => plan.compose(self, &ingestor).await?,
            SourceStartPlan::Websockets(plan) => plan.compose(self, &ingestor).await?,
            SourceStartPlan::Syslog(plan) => plan.compose(self, &ingestor).await?,
        };
        self.host_source(&ingestor, quiesce, dependencies, source);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: std::fmt::Debug,
    {
        T::try_from(value.to_string()).assured("the fixture name is valid")
    }

    #[tokio::test]
    async fn http_sources_report_missing_node_dns_as_start_failure() {
        let runtime = Runtime::default();
        let domain: DomainName = named("sales");
        let client_name: ClientName = named("upstream");
        let cadence: nervix_models::DomainClockPeriod = "1s"
            .parse()
            .assured("the fixture cadence is a positive duration");
        for (source, client_model) in [
            (
                IngestSource::Http {
                    client: client_name.clone(),
                    every: cadence,
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientHttp(CreateClientHttp {
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestSource::Prometheus {
                    client: client_name.clone(),
                    query: "up".to_string(),
                    every: cadence,
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientPrometheus(CreateClientPrometheus {
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
        ] {
            let ingestor = CreateIngestor {
                name: named("source"),
                output_routes: nervix_models::ProcessorOutputs::single(named("events")),
                decode_using_codec: named("json"),
                timestamp_source: None,
                source,
                general_error_policy: GeneralErrorPolicy::Log,
                filter_where: None,
            };
            let plan = IngestorStartPlan::decide_unscheduled(&domain, &ingestor, &client_model)
                .assured("the fixture source and client are paired");
            let IngestorStartPlan { ingestor, source } = plan;
            let result = match source {
                SourceStartPlan::Http(source) => source.compose(&runtime, &ingestor).await,
                SourceStartPlan::Prometheus(source) => source.compose(&runtime, &ingestor).await,
                _ => panic!("the fixture only includes HTTP sources"),
            };
            let Err(RuntimeError::StartIngestor {
                domain,
                ingestor,
                reason,
            }) = result
            else {
                panic!("an HTTP source without node DNS must fail before it opens");
            };
            assert_eq!(domain, "sales");
            assert_eq!(ingestor, "source");
            assert_eq!(reason, "the node DNS resolver is not installed");
        }
    }
}
