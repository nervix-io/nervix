pub fn harmless() {
    #[cfg_attr(nervix_lint, expect(nervix::cluster_await, reason="reviewed one awaited operation"))]
    let result = 42;
    assert_eq!(result, 42);
}
