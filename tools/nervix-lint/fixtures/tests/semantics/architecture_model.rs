//! A renamed configuration model remains forbidden in a typed-plan executor.

#[cfg_attr(nervix_lint, nervix::value(model, reason = "configuration union in compiler calibration"))]
pub enum Configuration {
    Ready,
}

#[cfg_attr(nervix_lint, nervix::layer(data_plane, reason = "executes prepared plans"))]
pub mod execution {
    use super::Configuration as Renamed;
    pub fn consume(_: Option<Renamed>) {}
}
