//! A scenario-owned RabbitMQ broker that can restart without disturbing other scenarios.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** One ephemeral broker, its current address, restart and explicit teardown.
//! - **Depends on.** The test environment's container provisioning and cleanup.
//! - **Must not know.** Nervix graphs, plans or acknowledgement state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the isolated broker fixture")
)]

use super::*;

#[derive(Debug)]
pub struct RestartableRabbitMq {
    environment: DependencyEnvironment,
}

impl RestartableRabbitMq {
    pub async fn start(scope: impl Into<String>) -> io::Result<Self> {
        let mut environment = DependencyEnvironment::new(scope, ContainerMode::Ephemeral);
        environment.start_rabbitmq().await?;
        Ok(Self { environment })
    }

    pub fn address(&self) -> io::Result<&str> {
        self.environment.endpoints().get(RABBITMQ_ADDR)
    }

    /// The isolated broker whose diagnostics a failed test must retain before teardown.
    pub fn container_id(&self) -> &str {
        let [RunningContainer::Generic(container)] = self.environment.containers.as_slice() else {
            unreachable!("this private environment starts exactly one RabbitMQ container");
        };
        container.id()
    }

    pub async fn restart(&mut self) -> io::Result<()> {
        let [RunningContainer::Generic(container)] = self.environment.containers.as_slice() else {
            unreachable!("this private environment starts exactly one RabbitMQ container");
        };
        container
            .stop_with_timeout(Some(10))
            .await
            .map_err(testcontainers_error("isolated RabbitMQ stop"))?;
        container
            .start()
            .await
            .map_err(testcontainers_error("isolated RabbitMQ restart"))?;
        // Docker can assign another published port when the same container starts again.
        let port = mapped_port(container, 5672, "restarted RabbitMQ").await?;
        self.environment.endpoints.insert(
            RABBITMQ_ADDR,
            format!("amqp://guest:guest@127.0.0.1:{port}/%2f"),
        );
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), TeardownFailures> {
        self.environment.shutdown().await
    }
}
