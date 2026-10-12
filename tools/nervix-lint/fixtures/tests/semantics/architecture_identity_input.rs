pub fn send(#[cfg_attr(nervix_lint, nervix::identity(node, reason="destination address"))] destination: String) { drop(destination); }
