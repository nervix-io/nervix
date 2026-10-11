#[cfg_attr(nervix_lint, nervix::layer(unknown, reason="no owning layer"))]
mod owner {}
pub struct State {
    #[cfg_attr(nervix_lint, nervix::state(installed, reason=""))]
    pub state: u32,
}
