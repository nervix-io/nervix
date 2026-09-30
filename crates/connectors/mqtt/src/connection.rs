//! How an MQTT client reaches its broker.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The socket connector every MQTT client's event loop opens a broker connection with:
//!   resolving the broker host through the node resolver for every connection, the budget and
//!   order of the address attempts, and what a failed or lost connection reports.
//! - **Depends on.** `nervix-dns`, `error-stack`, Tokio's timer, and the socket hook, network
//!   options and per-address TCP dialer of `rumqttc`.
//! - **Must not know.** Topics, sessions, acknowledgements, the host's retry policy, or Models.
//!
//! # One connection
//!
//! The driver asks its socket connector for a stream every time its event loop connects: first,
//! and again after every lost connection. It layers TLS and the MQTT handshake on the stream it
//! gets, all inside its connect timeout. The connector installed here resolves the host again
//! through the node resolver, so an expired answer is replaced before anything is dialled, and
//! tries the addresses in resolution order. Each attempt gets an equal share of what remains of
//! that same connect timeout, so an address that refuses or never answers leaves time for the
//! next, and each applies the driver's network options to its socket as the driver's own dialer
//! does. The driver then completes TLS against the configured host, so an `mqtts` broker's
//! certificate must name that host whichever of its addresses was dialled.
//!
//! A failed lookup reaches the driver as the resolver's [`DnsLookupError`], which the driver keeps
//! as the cause of the connection error it reports; [`MqttConnectionError::report`] finds it
//! there again. A connection is never closed because its host's answer changed or expired; the
//! next connection uses the new answer.

use std::{io, time::Duration};

use error_stack::Report;
use nervix_dns::{ConnectionBudget, DnsLookupError, DnsLookupFailure, DnsResolver};
use rumqttc::{ConnectionError, MqttOptions, NetworkOptions};
use thiserror::Error;
use tokio::{net::TcpStream, time::timeout};

/// Why an MQTT client's event loop could not connect to its broker, or lost the connection.
#[derive(Debug, Error)]
pub enum MqttConnectionError {
    /// The broker host did not resolve through the node resolver.
    #[error("resolving MQTT host '{host}' failed: {failure}")]
    Resolve {
        host: String,
        failure: DnsLookupFailure,
    },
    /// Any other failure, as the driver describes it: no address that accepted a connection, a
    /// failed TLS handshake, including a broker certificate that does not name the host, a refused
    /// MQTT handshake, or a connection that broke.
    #[error(transparent)]
    Driver(ConnectionError),
}

impl MqttConnectionError {
    /// The failure the driver reported as `error`. A failed lookup of the broker host is found
    /// among its causes, where the socket connector left it, and keeps the resolver's own error
    /// beneath it.
    pub(crate) fn report(error: ConnectionError) -> Report<Self> {
        let Some(lookup) = DnsLookupError::find_in(&error) else {
            return Report::new(Self::Driver(error));
        };
        let context = Self::Resolve {
            host: lookup.name().to_string(),
            failure: lookup.failure(),
        };
        Report::new(lookup.clone()).change_context(context)
    }
}

/// The socket connector an MQTT client's event loop opens every broker connection with.
#[derive(Clone)]
pub(crate) struct MqttDialer {
    dns: DnsResolver,
    /// The driver's connect timeout, the one deadline of DNS, the address attempts, TLS and the
    /// MQTT handshake of a connection.
    budget: Duration,
}

impl MqttDialer {
    /// Replace the driver's default socket connector, which resolves the broker host with Tokio's
    /// system lookup, with one that resolves it through `dns`. Installed once `options` are
    /// otherwise complete, so the budget is the connect timeout the driver enforces.
    pub(crate) fn install(options: &mut MqttOptions, dns: DnsResolver) {
        let dialer = Self {
            dns,
            budget: options.connect_timeout(),
        };
        options.set_socket_connector(move |authority, network| {
            let dialer = dialer.clone();
            async move { dialer.dial(&authority, network).await }
        });
    }

    /// A TCP stream to the first address of `authority` that accepts one.
    ///
    /// The driver names what it dials as `host:port` and leaves an IPv6 literal unbracketed, a
    /// form the URL grammar cannot read, so the port is what follows the last colon.
    async fn dial(&self, authority: &str, network: NetworkOptions) -> io::Result<TcpStream> {
        let budget = ConnectionBudget::start(self.budget);
        let Some((host, port)) = authority.rsplit_once(':') else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("the MQTT driver asked to dial '{authority}', which names no port"),
            ));
        };
        let Ok(port) = port.parse::<u16>() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("the MQTT driver asked to dial '{authority}', whose port is not a number"),
            ));
        };
        let resolved = self.dns.resolve(host, port, budget.remaining()).await;
        let addresses = match resolved {
            Ok(addresses) => addresses,
            Err(report) => return Err(io::Error::other(report.current_context().clone())),
        };
        let mut failure = io::Error::new(
            io::ErrorKind::NotFound,
            format!("MQTT host '{host}' resolved to no address"),
        );
        for attempt in budget.attempts(&addresses) {
            nervix_primitives::task::consume_budget().await;
            let address = attempt.address;
            let share = attempt.budget;
            let connecting = rumqttc_core::connect_socket_addr(address, network.clone());
            match timeout(share, connecting).await {
                Ok(Ok(stream)) => return Ok(stream),
                Ok(Err(error)) => {
                    failure = io::Error::new(error.kind(), format!("{address}: {error}"));
                }
                Err(_) => {
                    failure = io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("{address}: no connection within {share:?}"),
                    );
                }
            }
        }
        Err(failure)
    }
}

#[cfg(test)]
mod tests;
