//! Source-contract compiler fixture; no runtime behavior is asserted.

#[cfg_attr(nervix_lint, nervix::host_services(executor, reason = "runs driver work on the executor"))]
pub fn open(host: &callee::Host) -> u32 {
    assert_eq!(host.executor(), 2);
    host.dns()
}
