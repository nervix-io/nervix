#[cfg_attr(nervix_lint, nervix::value(cluster_access, reason="membership service"))]
pub struct View;
impl View { pub async fn inspect(&self) {} }
#[cfg_attr(nervix_lint, nervix::layer(data_plane, reason="record execution"))]
#[cfg_attr(nervix_lint, nervix::context(recurring, reason="per batch"))]
pub async fn execute(view: &View) {
    let query = View::inspect(view);
    let retained = query;
    retained.await;
}
