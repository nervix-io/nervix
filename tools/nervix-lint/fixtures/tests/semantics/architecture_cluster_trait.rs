#[cfg_attr(nervix_lint, nervix::value(cluster_access, reason="membership interface"))]
#[cfg_attr(nervix_lint, nervix::context(recurring, reason="typed observation access"))]
pub trait Observation { async fn inspect(&self); }
#[cfg_attr(nervix_lint, nervix::layer(data_plane, reason="record execution"))]
#[cfg_attr(nervix_lint, nervix::context(recurring, reason="per batch"))]
pub async fn execute<T: Observation>(view: &T) {
    let inspect = async || { Observation::inspect(view).await; };
    inspect().await;
}
