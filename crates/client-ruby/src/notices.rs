//! Ruby server notices: what a notice reports, its display text and the error it names.
//!
//! Layer: edges.
//! - **Owns.** The Ruby notice object and its readers over one host notice.
//! - **Depends on.** The shared host notice, the binding's notice enumerations and magnus.
//! - **Must not know.** How notices are published, queued, dropped or routed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "projects server notices to Ruby")
)]

use magnus::{DataTypeFunctions, Error, RModule, Ruby, TypedData, method, prelude::*};
use nervix_client_host as host;

use crate::enums;

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Notice", free_immediately)]
pub(crate) struct Notice {
    notice: host::Notice,
}

impl Notice {
    pub(crate) fn new(notice: host::Notice) -> Self {
        Self { notice }
    }
    fn level(&self) -> i32 {
        enums::NoticeLevel::code(self.notice.level())
    }
    fn kind(&self) -> i32 {
        enums::NoticeKind::code(self.notice.kind())
    }
    fn message(&self) -> &str {
        self.notice.message()
    }
    fn error_class(&self) -> Option<&'static str> {
        self.notice.error_class()
    }
    fn error_node_kind(&self) -> Option<&'static str> {
        self.notice.error_node_kind()
    }
    fn error_node(&self) -> Option<&str> {
        self.notice.error_node()
    }
}

pub(crate) fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let notice = crate::class(ruby, module, "Notice")?;
    notice.define_method("level", method!(Notice::level, 0))?;
    notice.define_method("kind", method!(Notice::kind, 0))?;
    notice.define_method("message", method!(Notice::message, 0))?;
    notice.define_method("error_class", method!(Notice::error_class, 0))?;
    notice.define_method("error_node_kind", method!(Notice::error_node_kind, 0))?;
    notice.define_method("error_node", method!(Notice::error_node, 0))?;
    Ok(())
}
