//! Native access to an independently retained entity metadata view.
//!
//! Layer: edges.
//! - **Owns.** Typed host metadata getters and independent value ownership.
//! - **Depends on.** The shared host snapshot and the host binding library.
//! - **Must not know.** Display parsing, registry storage or execution behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns native metadata views")
)]

use magnus::{
    DataTypeFunctions, Error, RHash, RModule, RString, Ruby, TypedData, method, prelude::*,
};
use meticulous::ResultExt as _;
use nervix_client_host as host;

use crate::enums::EntityLabelViewKind;

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::EntityLabelsInspection", free_immediately)]
pub(crate) struct EntityLabelsInspection {
    owner: host::EntityLabelsInspection,
}
impl EntityLabelsInspection {
    pub(crate) fn new(owner: host::EntityLabelsInspection) -> Self {
        Self { owner }
    }
    fn domain(&self) -> &str {
        self.owner.domain()
    }
    fn entity_kind(&self) -> &str {
        self.owner.entity_kind()
    }
    fn entity_name(&self) -> &str {
        self.owner.entity_name()
    }
    fn revision(&self) -> Option<u64> {
        self.owner.revision()
    }
    fn transaction_id(&self) -> Option<&str> {
        self.owner.transaction_id()
    }
    fn position(&self) -> Option<u64> {
        self.owner.position()
    }
    fn policy_revision(&self) -> u64 {
        self.owner.policy_revision()
    }
    fn max_count(&self) -> u64 {
        self.owner.max_count()
    }
    fn max_key_bytes(&self) -> u64 {
        self.owner.max_key_bytes()
    }
    fn max_value_bytes(&self) -> u64 {
        self.owner.max_value_bytes()
    }
    fn max_bytes(&self) -> u64 {
        self.owner.max_bytes()
    }
    fn labels(&self) -> Result<RHash, Error> {
        let ruby = Ruby::get().assured("a native Ruby getter holds the GVL");
        let labels = ruby.hash_new();
        for (key, value) in self.owner.labels().iter() {
            labels.aset(key.as_str(), value)?;
        }
        Ok(labels)
    }
    fn view(&self) -> i32 {
        EntityLabelViewKind::code(self.owner.view())
    }
    fn planning_basis(&self) -> Option<RString> {
        let ruby = Ruby::get().assured("a native Ruby getter holds the GVL");
        self.owner
            .planning_basis()
            .map(|bytes| ruby.str_from_slice(bytes))
    }
}

pub(crate) fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let class = crate::class(ruby, module, "EntityLabelsInspection")?;
    class.define_method("domain", method!(EntityLabelsInspection::domain, 0))?;
    class.define_method(
        "entity_kind",
        method!(EntityLabelsInspection::entity_kind, 0),
    )?;
    class.define_method(
        "entity_name",
        method!(EntityLabelsInspection::entity_name, 0),
    )?;
    class.define_method("revision", method!(EntityLabelsInspection::revision, 0))?;
    class.define_method(
        "transaction_id",
        method!(EntityLabelsInspection::transaction_id, 0),
    )?;
    class.define_method("position", method!(EntityLabelsInspection::position, 0))?;
    class.define_method(
        "policy_revision",
        method!(EntityLabelsInspection::policy_revision, 0),
    )?;
    class.define_method("max_count", method!(EntityLabelsInspection::max_count, 0))?;
    class.define_method(
        "max_key_bytes",
        method!(EntityLabelsInspection::max_key_bytes, 0),
    )?;
    class.define_method(
        "max_value_bytes",
        method!(EntityLabelsInspection::max_value_bytes, 0),
    )?;
    class.define_method("max_bytes", method!(EntityLabelsInspection::max_bytes, 0))?;
    class.define_method("labels", method!(EntityLabelsInspection::labels, 0))?;
    class.define_method("view", method!(EntityLabelsInspection::view, 0))?;
    class.define_method(
        "planning_basis",
        method!(EntityLabelsInspection::planning_basis, 0),
    )?;
    Ok(())
}
