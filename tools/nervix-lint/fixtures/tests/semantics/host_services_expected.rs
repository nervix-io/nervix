//! One reviewed reach carries its own reason-bearing expectation.

#[cfg_attr(nervix_lint, nervix::value(host_handle, reason = "the handle a host gives its connector"))]
pub struct Host;

impl Host {
    #[cfg_attr(nervix_lint, nervix::host_service(dns, reason = "hands over the node resolver"))]
    pub fn dns(&self) -> u32 { 1 }
}

pub fn probe(host: &Host) -> u32 {
    #[cfg_attr(nervix_lint, expect(nervix::undeclared_host_service, reason = "fixture owner documents this one reviewed reach"))]
    let answer = host.dns();
    answer
}
