//! The hosts file a resolver consults before DNS.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** One snapshot of the hosts file, and the addresses it lists for a name.
//! - **Depends on.** Hickory's hosts-file grammar.
//! - **Must not know.** DNS, the search list, or who resolves the name.

use std::{fs::File, net::IpAddr, path::Path};

use error_stack::Report;
use hickory_resolver::{
    Hosts,
    proto::{
        op::Query,
        rr::{Name, RecordType},
    },
};
use indexmap::IndexSet;

use crate::DnsConfigurationError;

/// The hosts file as it was when the resolver was loaded.
///
/// An entry answers for exactly the name it lists, in either address family, so a name the file
/// lists never reaches DNS: an IPv4-only entry does not wait for an IPv6 query, and search domains do
/// not apply to it.
pub(crate) struct HostsTable {
    hosts: Hosts,
}

impl HostsTable {
    /// Read the hosts file at `path`. Blocking file I/O.
    pub(crate) fn read(path: &Path) -> Result<Self, Report<DnsConfigurationError>> {
        let unreadable = || {
            Report::new(DnsConfigurationError::ReadHostsFile {
                path: path.to_path_buf(),
            })
        };
        let file = File::open(path).map_err(|error| unreadable().attach_printable(error))?;
        let mut hosts = Hosts::default();
        hosts
            .read_hosts_conf(file)
            .map_err(|error| unreadable().attach_printable(error))?;
        Ok(Self { hosts })
    }

    /// Every address the file lists for `name`, IPv4 first, or `None` when it lists none.
    pub(crate) fn addresses(&self, name: &Name) -> Option<IndexSet<IpAddr>> {
        let name = name.to_lowercase();
        let mut addresses = IndexSet::new();
        for record_type in [RecordType::A, RecordType::AAAA] {
            let query = Query::query(name.clone(), record_type);
            let Some(listed) = self.hosts.lookup_static_host(&query) else {
                continue;
            };
            for record in listed.answers() {
                if let Some(ip) = record.data.ip_addr() {
                    addresses.insert(ip);
                }
            }
        }
        if addresses.is_empty() {
            return None;
        }
        Some(addresses)
    }
}
