//! Connector clients: their names, resource mounts, connection pools and configuration.

use std::num::NonZeroU32;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClientConfigEntry, ClientName, ClientPoolBounds, ClientResourceMount, CreateClientWebsockets,
    RequestedResourceVersion,
};

use crate::Arbitrary;

/// The most configuration entries a generated client declares.
const CONFIG_ENTRIES: usize = 4;

/// What every client declares, whatever system it connects to.
#[derive(Debug, Clone)]
pub struct ClientParts {
    pub name: ClientName,
    pub mount: Option<ClientResourceMount<RequestedResourceVersion>>,
    pub config: Vec<ClientConfigEntry>,
}

impl Arbitrary<'_> {
    /// The name, optional resource mount and configuration of a client.
    pub fn client_parts(&mut self) -> ClientParts {
        let name = self.name();
        let mount = if self.entropy.flag() {
            Some(ClientResourceMount {
                resource: self.name(),
                version: self.requested_version(),
            })
        } else {
            None
        };
        let config = self.config_entries();
        ClientParts {
            name,
            mount,
            config,
        }
    }

    /// Configuration entries, keyed and valued by any strings, in declaration order.
    pub fn config_entries(&mut self) -> Vec<ClientConfigEntry> {
        let count = self.entropy.count(CONFIG_ENTRIES);
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(ClientConfigEntry {
                key: self.string(),
                value: self.string(),
            });
        }
        entries
    }

    /// Connection-pool bounds whose minimum is reachable under their maximum.
    pub fn pool_bounds(&mut self) -> ClientPoolBounds {
        let maximum = self.entropy.boundary_biased(1..=u64::from(u32::MAX));
        let maximum = u32::try_from(maximum).verified("the range above ends at u32::MAX");
        let minimum = self.entropy.boundary_biased(0..=u64::from(maximum));
        let minimum = u32::try_from(minimum).verified("the minimum is at most the u32 maximum");
        ClientPoolBounds::new(
            minimum,
            NonZeroU32::new(maximum).verified("the range above starts at one"),
        )
        .verified("the minimum was drawn from zero through the maximum")
    }

    /// A WebSockets client, which may name the signaling protocol it connects with.
    pub fn websockets_client(&mut self) -> CreateClientWebsockets<RequestedResourceVersion> {
        let parts = self.client_parts();
        let signaling_protocol = if self.entropy.flag() {
            Some(self.name())
        } else {
            None
        };
        CreateClientWebsockets {
            name: parts.name,
            mount: parts.mount,
            signaling_protocol,
            config: parts.config,
        }
    }
}
