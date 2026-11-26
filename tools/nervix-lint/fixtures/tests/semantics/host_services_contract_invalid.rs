//! A host-service contract names known services, with a reason, on its exact owner.
#[cfg_attr(nervix_lint, nervix::value(host_handle, reason = "the handle a host gives its connector"))]
pub struct Host;
impl Host {
    #[cfg_attr(nervix_lint, nervix::host_service(telepathy, reason = "no host hands this over"))]
    pub fn telepathy(&self) {}
    #[cfg_attr(nervix_lint, nervix::host_service(dns, reason = ""))]
    pub fn dns(&self) {}
    pub fn unnamed(&self) {}
}
#[cfg_attr(nervix_lint, nervix::host_service(dns, reason = "a module hands nothing over"))]
pub mod misplaced {}
#[cfg_attr(nervix_lint, nervix::host_services(reason = "declares no service"))]
pub mod empty {}
#[cfg_attr(nervix_lint, nervix::host_services(dns, dns, reason = "names one service twice"))]
pub mod repeated {}
#[cfg_attr(nervix_lint, nervix::host_services(dns, telepathy, reason = "names an unknown service"))]
pub mod unknown {}
pub struct Connector {
    #[cfg_attr(nervix_lint, nervix::host_services(dns, reason = "a field reaches nothing"))]
    pub field: u32,
}
#[cfg_attr(nervix_lint, nervix::host_services(dns, reason = "the first declaration"))]
#[cfg_attr(nervix_lint, nervix::host_services(executor, reason = "a second declaration"))]
pub mod duplicated {}
