#[cfg_attr(nervix_lint, nervix::value(cluster_access, reason="membership service"))]
pub struct View;
impl View { pub async fn inspect(&self) {} }
#[cfg_attr(nervix_lint, nervix::layer(data_plane, reason="record execution"))]
#[cfg_attr(nervix_lint, nervix::context(recurring, reason="per batch"))]
pub async fn execute(view: &View) {
    #[cfg_attr(nervix_lint, expect(nervix::cluster_await, reason="single bounded fixture calibration query"))]
    let result = view.inspect().await;
    assert_eq!(result, ());
}
