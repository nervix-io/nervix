//! A connector cannot reach a host service its owner did not declare.

#[cfg_attr(nervix_lint, nervix::value(host_handle, reason = "the handle a host gives its connector"))]
pub struct Host;

impl Host {
    #[cfg_attr(nervix_lint, nervix::host_service(dns, reason = "hands over the node resolver"))]
    pub fn dns(&self) -> u32 { 1 }

    #[cfg_attr(nervix_lint, nervix::host_service(executor, reason = "hands over the bounded executor"))]
    pub fn executor(&self) -> u32 { 2 }
}

pub type Handle = Host;

/// No contract covers this owner.
pub fn uncontracted(host: &Host) -> u32 { host.dns() }

#[cfg_attr(nervix_lint, nervix::host_services(executor, reason = "runs driver work on the executor"))]
pub mod connector {
    use super::{Handle, Host};

    /// The declared service is reached, and the undeclared one is refused.
    pub fn method(host: &Host) -> u32 {
        assert_eq!(host.executor(), 2);
        host.dns()
    }

    /// An alias and a qualified call keep the definition.
    pub fn qualified(host: &Handle) -> u32 { Handle::dns(host) }

    /// A function reference is refused before a pointer type can erase its definition.
    pub fn reference() -> fn(&Host) -> u32 { Host::dns }

    /// A narrower declaration replaces its parent's and does not extend it.
    #[cfg_attr(nervix_lint, nervix::host_services(dns, reason = "this function dials"))]
    pub fn narrower(host: &Host) -> u32 { host.executor() }

    macro_rules! reach { ($host:expr) => { $host.dns() }; }

    /// A call written in an authored macro is refused where it was written.
    pub fn authored_macro(host: &Host) -> u32 { reach!(host) }
}

/// Only a harness is outside the rule: a product layer without a contract is refused like any
/// other owner.
#[cfg_attr(nervix_lint, nervix::layer(engine, reason = "a product engine, not a harness"))]
pub mod engine {
    pub fn uncontracted(host: &super::Host) -> u32 { host.dns() }
}
