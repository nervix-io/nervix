//! The WASM state page replies a scenario made too large for their request class, so a public read
//! meets the owner's fitting of its page to one reply.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The WASM processors whose owner page replies a scenario made overflow, and which of
//!   their replies overflow.
//! - **Depends on.** Vocabulary identities and the entries of a WASM state page.
//! - **Must not know.** The reply's archived layout, the request class's payload limit or how an
//!   owner fits its page to a reply.

#![cfg_attr(
    nervix_lint,
    nervix::context(outside, reason = "owns test WASM state page reply overflows")
)]

use nervix_models::{
    DomainName, DomainNodeRef, ModelKind, ModelName, WasmGuestStateRender, WasmUnitStatePage,
};

use super::FaultInjection;

/// Which owner page replies of a WASM processor a scenario made too large for their request class,
/// as replies whose entries pad beyond the payload limit inside one reply would be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasmStatePageOverflow {
    /// Every reply that holds more than one branch entry, so an owner keeps only the first entry of
    /// each page and reports that more remain.
    BeyondOneEntry,
    /// Every reply that holds an entry its guest described, so a page reduced to its first entry
    /// carries that entry's description failure instead.
    DescribedEntry,
    /// Every reply, so the owner refuses the read as too large.
    EveryReply,
}

impl WasmStatePageOverflow {
    /// Whether this overflow makes the reply of `page` too large for its request class.
    fn overflows(self, page: &WasmUnitStatePage) -> bool {
        let entries = &page.state.branches;
        match self {
            Self::BeyondOneEntry => entries.len() > 1,
            Self::DescribedEntry => {
                // A page holds at most the entries one reply has room for.
                for entry in entries {
                    if let Some(description) = &entry.description
                        && let WasmGuestStateRender::Described { .. } = description.render
                    {
                        return true;
                    }
                }
                false
            }
            Self::EveryReply => true,
        }
    }
}

impl FaultInjection {
    /// Makes every owner page reply of `processor`, a WASM processor, that `overflow` names too
    /// large for its request class, on every node that owns one of the processor's units.
    pub fn overflow_wasm_state_page_replies(
        &self,
        processor: DomainNodeRef,
        overflow: WasmStatePageOverflow,
    ) {
        self.inner
            .wasm_state_page_overflows
            .insert(processor, overflow);
    }

    /// Whether a scenario made the reply of `page`, an owner page of WASM processor `processor` of
    /// `domain`, too large for its request class.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "a scenario-owned overflow is read each time an owner checks whether its \
                      WASM state page reply fits",
            key = "one test processor",
            bound = "one keyed read; its guard ends before the page is judged"
        )
    )]
    pub(crate) fn overflows_wasm_state_page_reply(
        &self,
        domain: &DomainName,
        processor: &ModelName,
        page: &WasmUnitStatePage,
    ) -> bool {
        let processor =
            DomainNodeRef::node_in(domain.clone(), ModelKind::WasmProcessor, processor.clone());
        let overflow = self
            .inner
            .wasm_state_page_overflows
            .get(&processor)
            .map(|overflow| *overflow.value());
        let Some(overflow) = overflow else {
            return false;
        };
        overflow.overflows(page)
    }
}
