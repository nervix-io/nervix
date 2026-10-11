//! Awaited cluster reads through a helper retain their resolved effect.

#[cfg_attr(nervix_lint, nervix::value(cluster_access, reason = "owns current cluster observations"))]
pub struct Membership;

impl Membership {
    pub async fn read(&self) {}
}

async fn helper(view: &Membership) {
    Membership::read(view).await;
}

#[cfg_attr(nervix_lint, nervix::layer(data_plane, reason = "executes prepared plans"))]
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "handles each batch"))]
pub async fn process(view: &Membership) {
    helper(view).await;
}
