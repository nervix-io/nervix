//! Network-free validation of a ZeroMQ client's socket configuration.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Endpoint and Boolean refusals with their complete typed causes.
//! - **Depends on.** The connector contract, configuration entries and the ZeroMQ driver.
//! - **Must not know.** Models, transaction admission, resource resolution or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns ZeroMQ offline client configuration validation")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies the client's resolved entries")
)]

use error_stack::ResultExt as _;
use nervix_connector::{
    OfflineClientConfigError, client_config_value, optional_bool_client_config_value,
};
use nervix_models::{ClientConfigEntry, ConnectorKind};

/// Check the endpoint with the same parser a socket uses, without opening or resolving it.
pub fn validate_client_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    let address = client_config_value(config, "addr", ConnectorKind::ZeroMq.keyword())
        .change_context(OfflineClientConfigError::key("addr"))?;
    address
        .parse::<zeromq::Endpoint>()
        .change_context(OfflineClientConfigError::key("addr"))?;
    optional_bool_client_config_value(config, "bind")
        .map(|_| ())
        .change_context(OfflineClientConfigError::key("bind"))
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns offline ZeroMQ configuration evidence")
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** Supported endpoints, strict Boolean values and cause-preserving refusals.
    //! - **Depends on.** The production configuration validator and its public error contract.
    //! - **Must not know.** Running sockets, hosts or graph execution.

    use meticulous::ResultExt as _;

    use super::*;

    fn entries(address: &str, bind: Option<&str>) -> Vec<ClientConfigEntry> {
        let mut config = vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: address.to_string(),
        }];
        if let Some(bind) = bind {
            config.push(ClientConfigEntry {
                key: "bind".to_string(),
                value: bind.to_string(),
            });
        }
        config
    }

    #[test]
    fn tcp_and_ipc_endpoints_need_no_running_peer() {
        for address in [
            "tcp://127.0.0.1:5555",
            "tcp://[::1]:5555",
            "tcp://broker.example.invalid:65535",
            "ipc:///tmp/nervix.sock",
            "ipc://relative.sock",
        ] {
            for bind in [
                None,
                Some("true"),
                Some("false"),
                Some("TRUE"),
                Some("False"),
            ] {
                validate_client_configuration(&entries(address, bind))
                    .assured("a supported endpoint and Boolean need no peer or name lookup");
            }
        }
    }

    #[test]
    fn endpoint_refusals_keep_the_driver_cause_and_name_the_key() {
        for address in [
            "127.0.0.1:5555",
            "http://127.0.0.1:5555",
            "tcp://127.0.0.1",
            "tcp://127.0.0.1:65536",
            "tcp://127.0.0.1:not-a-port",
            "ipc://",
        ] {
            let refusal = validate_client_configuration(&entries(address, None))
                .expect_err("the socket driver refuses this endpoint");
            assert!(matches!(
                refusal.current_context(),
                OfflineClientConfigError::InvalidKey { key } if key == "addr"
            ));
            assert!(refusal.contains::<<zeromq::Endpoint as std::str::FromStr>::Err>());
            assert_eq!(refusal.to_string(), "invalid client config key 'addr'");
        }
    }

    #[test]
    fn a_boolean_refusal_keeps_the_shared_parser_cause() {
        let refusal = validate_client_configuration(&entries("tcp://127.0.0.1:5555", Some("yes")))
            .expect_err("a Boolean accepts only true or false");
        assert!(matches!(
            refusal.current_context(),
            OfflineClientConfigError::InvalidKey { key } if key == "bind"
        ));
        assert!(refusal.contains::<nervix_connector::ClientConfigError>());
        assert_eq!(refusal.to_string(), "invalid client config key 'bind'");
    }
}
