#[cfg_attr(nervix_lint, nervix::identity(node, reason="shared node vocabulary"))]
#[cfg_attr(nervix_lint, nervix::layer(vocabulary, reason="shared identity owner"))]
pub struct Node(u32);
pub struct Destination {
    #[cfg_attr(nervix_lint, nervix::identity(node, reason="node set"))]
    pub destinations: Vec<(Node, String)>,
}
