//! A connector reaches exactly the host services its owner declares.

#[cfg_attr(nervix_lint, nervix::value(host_handle, reason = "the handle a host gives its connector"))]
pub struct Host;

impl Host {
    /// A constructor has no receiver, so it hands no service over.
    pub fn new() -> Self { Self }

    #[cfg_attr(nervix_lint, nervix::host_service(dns, reason = "hands over the node resolver"))]
    pub fn dns(&self) -> u32 { self.provider() }

    #[cfg_attr(nervix_lint, nervix::host_service(executor, reason = "hands over the bounded executor"))]
    pub fn executor(&self) -> u32 { 2 }

    #[cfg_attr(nervix_lint, nervix::host_service(status, reason = "records a transient failure"))]
    pub fn record_transient_error(&self) {}

    /// A private helper hands nothing to a connector.
    fn provider(&self) -> u32 { 1 }
}

/// A type that is no host handle names its methods as it likes.
pub struct Resolver;
impl Resolver { pub fn dns(&self) -> u32 { 3 } }

#[cfg_attr(nervix_lint, nervix::host_services(dns, executor, reason = "dials through the node resolver and runs driver work on the executor"))]
pub mod connector {
    use super::{Host, Resolver};

    pub fn open(host: &Host) -> u32 {
        assert_eq!(Host::executor(host), 2);
        host.dns()
    }

    /// A nested module inherits its parent's declaration.
    pub mod driver {
        pub fn dial(host: &super::Host) -> u32 { host.dns() }
    }

    /// The narrowest declaration is the one that applies.
    #[cfg_attr(nervix_lint, nervix::host_services(status, reason = "this task alone reports the connection's status"))]
    pub fn report(host: &Host) { host.record_transient_error(); }

    /// A same-named method of an unrelated type reaches no host service.
    pub fn unrelated(resolver: &Resolver) -> u32 { resolver.dns() }
}

/// An owner that declares nothing still calls an unrelated type's method, and builds a handle.
pub fn undeclared_owner(resolver: &Resolver) -> u32 {
    let _host = Host::new();
    resolver.dns()
}

/// A harness is outside the product layer order: it asserts what a handle hands over, and a
/// scripted connector in it stands in for any real one, so neither declares services.
#[cfg_attr(nervix_lint, nervix::layer(harness, reason = "asserts what the handle hands over"))]
pub mod harness {
    use super::Host;

    pub fn asserts_every_service(host: &Host) -> u32 {
        host.record_transient_error();
        Host::executor(host) + host.dns()
    }

    /// A scripted connector nested in the harness is still harness code.
    pub mod scripted_connector {
        pub fn open(host: &super::Host) -> u32 { host.dns() }
    }
}
