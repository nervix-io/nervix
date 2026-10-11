//! Identity ownership is declared rather than inferred from a field's spelling.

pub struct Destination {
    #[cfg_attr(nervix_lint, nervix::identity(node, reason = "addresses the selected cluster node"))]
    pub destination: String,
}
