#![cfg_attr(nervix_lint, nervix::layer(control_plane, reason="configuration owner"))]
#[cfg_attr(nervix_lint, nervix::value(model, reason="configuration union"))]
pub enum Configuration { Ready }
pub trait Run { fn run(&self); }
#[cfg_attr(nervix_lint, nervix::layer(data_plane, reason="execution owner"))]
mod execution {
    pub struct Task;
    impl super::Run for Task {
        fn run(&self) { assert!(matches!(super::Configuration::Ready, super::Configuration::Ready)); }
    }
}
