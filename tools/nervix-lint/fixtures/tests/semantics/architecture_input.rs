#![cfg_attr(nervix_lint, nervix::layer(engine, reason="infrastructure engine"))]
#[cfg_attr(nervix_lint, nervix::value(model, reason="configuration union"))]
pub enum Configuration { Ready }
#[cfg_attr(nervix_lint, nervix::input(plans, reason="host supplies validated plans"))]
pub trait Driver { fn run(&self); }
pub struct Connector;
impl Driver for Connector {
    fn run(&self) { assert!(matches!(Configuration::Ready, Configuration::Ready)); }
}
