//! The MySQL client's declared connection-pool policy.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Reading the vocabulary's effective pool keys and applying their validated values
//!   to driver options, the checkout deadline and the minimum keeper's interval.
//! - **Depends on.** Vocabulary declarations, the connector's deadline reader and driver options.
//! - **Must not know.** Models, graph placement, emitter retries or pool synchronization.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns MySQL pool configuration interpretation")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "receives the client's resolved configuration")
)]

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use mysql_async::{PoolConstraints, PoolOpts};
use nervix_connector::{ConfiguredDeadline, optional_client_config_value};
use nervix_models::{ClientConfigEntry, ClientConfigSchema, ConnectorKind};

#[derive(Debug, thiserror::Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "refuses a client pool value that violates its declared configuration rule"
    )
)]
pub(super) enum MySqlPoolPolicyError {
    #[error("invalid pool configuration key '{key}'")]
    InvalidKey { key: &'static str },
}

/// One immutable policy, interpreted once when the node opens its shared pool.
pub(super) struct MySqlPoolPolicy {
    acquire: ConfiguredDeadline,
    idle: Duration,
    maximum: Option<Duration>,
    maintenance: Duration,
}

impl MySqlPoolPolicy {
    pub(super) fn read(config: &[ClientConfigEntry]) -> Result<Self, Report<MySqlPoolPolicyError>> {
        let schema = ConnectorKind::MySql
            .client()
            .assured("the MySQL connector declares its configured client")
            .config;
        let effective = schema.effective_entries(config);
        let acquire = ConfiguredDeadline::pool_acquire(&effective)
            .change_context(MySqlPoolPolicyError::InvalidKey {
                key: "pool_acquire_timeout_ms",
            })?
            .assured("the schema supplies the defaulted checkout deadline");
        let idle = Self::duration(schema, &effective, "pool_idle_timeout_ms")?;
        let maintenance = Self::duration(schema, &effective, "pool_maintenance_interval_ms")?;
        let maximum = match optional_client_config_value(&effective, "pool_max_lifetime_ms") {
            Some(_) => Some(Self::duration(schema, &effective, "pool_max_lifetime_ms")?),
            None => None,
        };
        Ok(Self {
            acquire,
            idle,
            maximum,
            maintenance,
        })
    }

    fn duration(
        schema: ClientConfigSchema,
        entries: &[ClientConfigEntry],
        name: &'static str,
    ) -> Result<Duration, Report<MySqlPoolPolicyError>> {
        let key = schema
            .key(name)
            .assured("the MySQL schema declares its pool duration keys");
        let value = optional_client_config_value(entries, name)
            .assured("effective configuration supplies the requested pool key");
        key.validate(value)
            .change_context(MySqlPoolPolicyError::InvalidKey { key: name })?;
        let milliseconds = value
            .parse::<u64>()
            .verified("the declared millisecond value rule parsed these same decimal digits");
        Ok(Duration::from_millis(milliseconds))
    }

    /// Both current bounds apply; retain the key of the one that ends the checkout first.
    pub(super) fn checkout(&self, operation: Option<ConfiguredDeadline>) -> ConfiguredDeadline {
        if let Some(operation) = operation
            && operation.bound() <= self.acquire.bound()
        {
            return operation;
        }
        self.acquire
    }

    pub(super) fn pool_options(&self, constraints: PoolConstraints) -> PoolOpts {
        PoolOpts::default()
            .with_constraints(constraints)
            .with_inactive_connection_ttl(self.idle)
            .with_abs_conn_ttl(self.maximum)
    }

    pub(super) fn maintenance_interval(&self) -> Duration {
        self.maintenance
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the production pool policy owner")
)]
mod tests {
    use nervix_models::DeadlineKey;

    use super::*;

    fn entries(pairs: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
        pairs
            .iter()
            .map(|(key, value)| ClientConfigEntry {
                key: (*key).to_string(),
                value: (*value).to_string(),
            })
            .collect()
    }

    #[test]
    fn defaults_preserve_driver_lifetimes_and_bound_checkout() {
        let policy = MySqlPoolPolicy::read(&[]).expect("declared defaults are valid");
        let constraints = PoolConstraints::new(1, 3).expect("the bounds are ordered");
        assert_eq!(
            policy.pool_options(constraints),
            PoolOpts::default().with_constraints(constraints)
        );
        assert_eq!(policy.acquire.bound(), Duration::from_secs(30));
        assert_eq!(policy.acquire.key(), DeadlineKey::PoolAcquireTimeout);
        assert_eq!(policy.maintenance, Duration::from_secs(1));
    }

    #[test]
    fn explicit_keys_reach_the_driver_and_keeper_without_truncation() {
        let policy = MySqlPoolPolicy::read(&entries(&[
            ("pool_acquire_timeout_ms", "250"),
            ("pool_idle_timeout_ms", "1250"),
            ("pool_max_lifetime_ms", "1000"),
            ("pool_maintenance_interval_ms", "100"),
        ]))
        .expect("the declared values are valid");
        let options = policy.pool_options(PoolConstraints::new(0, 2).expect("ordered bounds"));
        assert_eq!(
            options.inactive_connection_ttl(),
            Duration::from_millis(1250)
        );
        assert_eq!(options.abs_conn_ttl(), Some(Duration::from_secs(1)));
        assert_eq!(policy.acquire.bound(), Duration::from_millis(250));
        assert_eq!(policy.maintenance, Duration::from_millis(100));
    }

    #[test]
    fn checkout_keeps_the_shorter_bounds_typed_key() {
        let policy = MySqlPoolPolicy::read(&entries(&[("pool_acquire_timeout_ms", "250")]))
            .expect("the pool bound is valid");
        struct Case {
            milliseconds: &'static str,
            key: DeadlineKey,
            bound: Duration,
        }
        for case in [
            Case {
                milliseconds: "100",
                key: DeadlineKey::Timeout,
                bound: Duration::from_millis(100),
            },
            Case {
                milliseconds: "500",
                key: DeadlineKey::PoolAcquireTimeout,
                bound: Duration::from_millis(250),
            },
        ] {
            let operation =
                ConfiguredDeadline::timeout(&entries(&[("timeout_ms", case.milliseconds)]))
                    .expect("the operation bound is valid");
            let checkout = policy.checkout(operation);
            assert_eq!(checkout.key(), case.key);
            assert_eq!(checkout.bound(), case.bound);
        }
    }

    #[test]
    fn invalid_pool_values_keep_the_declared_key_and_typed_schema_cause() {
        for (key, value) in [
            ("pool_idle_timeout_ms", "18446744073709551615"),
            ("pool_max_lifetime_ms", "1001"),
            ("pool_maintenance_interval_ms", "0"),
        ] {
            let report = match MySqlPoolPolicy::read(&entries(&[(key, value)])) {
                Ok(_) => panic!("the declared value rule must refuse {key}"),
                Err(report) => report,
            };
            assert!(
                matches!(report.current_context(), MySqlPoolPolicyError::InvalidKey { key: found } if *found == key)
            );
            assert!(report.contains::<nervix_models::ClientConfigSchemaError>());
        }
    }
}
