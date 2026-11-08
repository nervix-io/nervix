//! Syslog listener fault capability used by the test harness.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Retaining one armed accept failure until its selected listener acknowledges it.
//! - **Depends on.** Execution-sensitive TCP listeners and watch channels.
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
    sync::watch,
};

use super::SyslogListenerError;

/// One harness request to fail an accept on a selected listener and acknowledge that failure.
#[derive(Clone, Debug)]
pub struct SyslogListenerFault {
    /// The exact bound address whose next accept fails.
    pub bind_addr: String,
    /// Shared acknowledgement; only the listener that changes it to true fires the fault.
    pub fired: watch::Sender<bool>,
}

#[derive(Clone, Default)]
pub(super) struct ConfiguredListenerFaults {
    faults: Option<watch::Sender<Option<SyslogListenerFault>>>,
}

impl ConfiguredListenerFaults {
    pub(super) fn new(faults: watch::Sender<Option<SyslogListenerFault>>) -> Self {
        Self {
            faults: Some(faults),
        }
    }

    pub(super) fn stream_accept(&self, bind_addr: SocketAddr) -> ConfiguredStreamAccept {
        ConfiguredStreamAccept {
            faults: self.faults.as_ref().map(|faults| faults.subscribe()),
            bind_addr: bind_addr.to_string(),
        }
    }
}

pub(super) struct ConfiguredStreamAccept {
    faults: Option<watch::Receiver<Option<SyslogListenerFault>>>,
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
            {
                let pending = faults.borrow_and_update();
                if let Some(fault) = pending.as_ref()
                    && fault.bind_addr == self.bind_addr
                    && fault.fired.send_if_modified(|seen| {
                        if *seen {
                            false
                        } else {
                            *seen = true;
                            true
                        }
                    })
                {
                    return Err(std::io::Error::other(
                        "injected Syslog stream accept failure",
                    ));
                }
            }
            nervix_primitives::select! {
                accepted = listener.accept() => return accepted,
                changed = faults.changed() => {
                    if changed.is_err() {
                        return listener.accept().await;
                    }
                }
            }
        }
    }
}
