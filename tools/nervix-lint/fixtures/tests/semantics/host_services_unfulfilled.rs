//! An expectation over a declared reach covers nothing.

#[cfg_attr(nervix_lint, nervix::value(host_handle, reason = "the handle a host gives its connector"))]
pub struct Host;

impl Host {
    #[cfg_attr(nervix_lint, nervix::host_service(dns, reason = "hands over the node resolver"))]
    pub fn dns(&self) -> u32 { 1 }
}

#[cfg_attr(nervix_lint, nervix::host_services(dns, reason = "dials through the node resolver"))]
pub fn open(host: &Host) -> u32 {
    #[cfg_attr(nervix_lint, expect(nervix::undeclared_host_service, reason = "the reach below is declared"))]
    let answer = host.dns();
    answer
}
