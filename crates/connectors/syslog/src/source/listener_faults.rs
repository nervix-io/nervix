//! Syslog listener fault capability used by the test harness.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Host-supplied accept-fault subscriptions and the address of the selected listener.
//! - **Depends on.** Execution-sensitive TCP listeners, broadcast channels and watch channels.
//! - **Must not know.** Product configuration, Plexus, graph ownership, or host runtime state.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "the test harness owns deterministic listener faults"
    )
)]

use std::net::SocketAddr;

use error_stack::Report;
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, watch},
};

use super::SyslogListenerError;

#[derive(Clone, Default)]
pub(super) struct ConfiguredListenerFaults {
    faults: Option<broadcast::Sender<(String, watch::Sender<bool>)>>,
}

impl ConfiguredListenerFaults {
    pub(super) fn new(faults: broadcast::Sender<(String, watch::Sender<bool>)>) -> Self {
        Self {
            faults: Some(faults),
        }
    }

    pub(super) fn stream_accept(&self, bind_addr: &str) -> ConfiguredStreamAccept {
        ConfiguredStreamAccept {
            faults: self.faults.as_ref().map(|faults| faults.subscribe()),
            bind_addr: bind_addr.to_owned(),
        }
    }
}

pub(super) struct ConfiguredStreamAccept {
    faults: Option<broadcast::Receiver<(String, watch::Sender<bool>)>>,
    bind_addr: String,
}

impl ConfiguredStreamAccept {
    pub(super) async fn accept(
        &mut self,
        listener: &TcpListener,
    ) -> error_stack::Result<(TcpStream, SocketAddr), SyslogListenerError> {
        self.accept_socket(listener)
            .await
            .map_err(|source| Report::new(SyslogListenerError::StreamAccept { source }))
    }

    async fn accept_socket(
        &mut self,
        listener: &TcpListener,
    ) -> std::io::Result<(TcpStream, SocketAddr)> {
        let Some(faults) = self.faults.as_mut() else {
            return listener.accept().await;
        };
        loop {
            nervix_primitives::select! {
                accepted = listener.accept() => return accepted,
                fault = faults.recv() => match fault {
                    Ok((address, fired)) if address == self.bind_addr => {
                        fired.send_replace(true);
                        return Err(std::io::Error::other("injected Syslog stream accept failure"));
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return listener.accept().await,
                }
            }
        }
    }
}
