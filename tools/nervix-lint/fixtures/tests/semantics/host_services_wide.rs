//! One expectation cannot hide two undeclared reaches.

#[cfg_attr(nervix_lint, nervix::value(host_handle, reason = "the handle a host gives its connector"))]
pub struct Host;

impl Host {
    #[cfg_attr(nervix_lint, nervix::host_service(dns, reason = "hands over the node resolver"))]
    pub fn dns(&self) -> u32 { 1 }

    #[cfg_attr(nervix_lint, nervix::host_service(executor, reason = "hands over the bounded executor"))]
    pub fn executor(&self) -> u32 { 2 }
}

pub fn probe(host: &Host) -> u32 {
    #[cfg_attr(nervix_lint, expect(nervix::undeclared_host_service, reason = "one expectation over two reaches"))]
    let answer = host.dns().max(host.executor());
    answer
}
