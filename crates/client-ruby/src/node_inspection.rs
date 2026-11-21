//! Ruby access to a typed cluster node inspection.
//!
//! Layer: edges.
//! - **Owns.** The independently retained Ruby inspection object.
//! - **Depends on.** The shared host inspection and Magnus.
//! - **Must not know.** Command display text or record storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "projects node inspections to Ruby")
)]

use magnus::{DataTypeFunctions, Error, RModule, Ruby, TypedData, method, prelude::*};

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::NodeInspection", free_immediately)]
pub(crate) struct NodeInspection {
    owner: nervix_client_host::NodeInspection,
}

impl NodeInspection {
    pub(crate) fn new(owner: nervix_client_host::NodeInspection) -> Self {
        Self { owner }
    }

    fn names(&self) -> Vec<String> {
        self.owner
            .nodes()
            .iter()
            .map(|node| node.name.to_string())
            .collect()
    }

    fn json(&self) -> &str {
        self.owner.json()
    }
}

pub(crate) fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let class = crate::class(ruby, module, "NodeInspection")?;
    class.define_method("names", method!(NodeInspection::names, 0))?;
    class.define_method("json", method!(NodeInspection::json, 0))?;
    Ok(())
}
