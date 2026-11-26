//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::host_services(dns, reason = "dials through the node resolver"))]
pub fn open(host: &callee::Host) -> u32 { use callee::Host as Renamed; Renamed::dns(host) }
