#![cfg_attr(nervix_lint, nervix::layer(engine, reason="infrastructure owns stored configuration"))]
use nervix_lint_fixtures::{SharedValue, StateLock};
#[cfg_attr(nervix_lint, nervix::value(model, reason="configuration union"))]
pub enum Configuration { Ready }
pub fn persist(_: Configuration) {}
#[cfg_attr(nervix_lint, nervix::identity(node, reason="shared typed identity"))]
#[cfg_attr(nervix_lint, nervix::layer(vocabulary, reason="shared identity owner"))]
pub struct Node(u32);
pub type NodeAlias = Node;
pub struct State {
    #[cfg_attr(nervix_lint, nervix::identity(node, reason="typed destinations"))]
    pub destinations: Vec<Option<NodeAlias>>,
    #[cfg_attr(nervix_lint, nervix::state(mutable, reason="ownership replaces this observation"))]
    pub observation: StateLock<Option<SharedValue<u32>>>,
    #[cfg_attr(nervix_lint, nervix::state(installed, reason="constructed before execution"))]
    pub installed: SharedValue<u32>,
    pub node_label: String,
}
#[cfg_attr(nervix_lint, nervix::layer(data_plane, reason="typed plan execution"))]
mod execution {
    pub struct Model;
    pub struct Plan;
    pub fn execute(_: Plan) -> Model { Model }
    pub async fn local_io() {}
    #[cfg_attr(nervix_lint, nervix::context(recurring, reason="record loop"))]
    pub async fn process() { local_io().await; }
}
#[cfg_attr(nervix_lint, nervix::layer(language, reason="parser implementation"))]
mod language { pub fn parse() -> u32 { 1 } }
#[cfg_attr(nervix_lint, nervix::layer(edge, reason="client adapter"))]
pub fn parse_at_edge() -> u32 { language::parse() }
