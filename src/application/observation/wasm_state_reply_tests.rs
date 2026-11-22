//! The owner's fitting of a WASM state page to one reply of the page operation: a page whose reply
//! fits, the trailing entries an over-limit reply drops, the processor a lone entry's description
//! failure needs, and the refusal of a page with nothing left to give way.
//!
//! Layer: test harness.
//! - **Owns.** Owner tests of `SessionServiceImpl::fit_wasm_page_to_reply` through a test service:
//!   with the node's own payload limit, and with the testing build's page reply overflow where
//!   only a limit below one entry's own facts reaches a branch.
//! - **Depends on.** The test service, the WASM state page vocabulary and the page operation's
//!   payload measures.
//! - **Must not know.** Guest rendering, checkpoints or the coordinator's paging.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use std::num::NonZeroU64;

use nervix_models::{
    BranchKeyFingerprint, ClusterNodeIncarnation, ClusterNodeName, WasmCheckpointCounts,
    WasmCheckpointStage, WasmCheckpointState, WasmGuestStateDescription, WasmGuestStateRender,
    WasmGuestStateRenderStage, WasmRecoveryCounts, WasmStateGeneration,
};

use super::*;
use crate::{
    WasmStatePageOverflow,
    application::test_fixtures::{TestService, build_test_service},
};

fn domain() -> DomainName {
    DomainName::parse("inspected").assured("a literal domain name follows the name rules")
}

fn processor() -> ModelName {
    ModelName::from(&wasm_processor())
}

fn wasm_processor() -> WasmProcessorName {
    WasmProcessorName::parse("counting_guest")
        .assured("a literal processor name follows the name rules")
}

fn owner() -> ClusterNodeIdentity {
    ClusterNodeIdentity::new(
        ClusterNodeName::parse("node-1").assured("a literal node name follows the node-name rules"),
        ClusterNodeIncarnation::new(1),
    )
}

/// The owner page read of the processor's only unit, as a coordinator sends it.
fn request() -> ReadWasmStatePage {
    ReadWasmStatePage {
        domain: domain(),
        processor: wasm_processor(),
        unit: Unit::SINGLE,
        control_revision: 1,
        selection: StateInspectionBranchSelection::All,
        after: None,
        max_entries: 3,
        max_sections: 128,
        max_bytes: u64::MAX,
        render_budget: Duration::from_secs(1),
        target: owner(),
    }
}

fn origin() -> StateInspectionOrigin {
    StateInspectionOrigin {
        node: owner(),
        fingerprint: [7; 32],
        generation: 1,
        revision: 1,
        stage: StateInspectionStage::Live,
    }
}

/// The entry of branch `branch` with a committed checkpoint, and with a failed render whose
/// diagnostic is `diagnostic_bytes` bytes when `diagnostic_bytes` is `Some`, so that the entry takes
/// about that much of a reply.
fn entry(branch: u8, diagnostic_bytes: Option<usize>) -> WasmBranchStateInspection {
    let revision = NonZeroU64::new(1);
    let mut description = None;
    if let Some(bytes) = diagnostic_bytes {
        description = Some(WasmGuestStateDescription {
            generation: WasmStateGeneration::FIRST,
            revision,
            render: WasmGuestStateRender::Failed {
                stage: WasmGuestStateRenderStage::StateDescription,
                diagnostic: "d".repeat(bytes),
            },
        });
    }
    WasmBranchStateInspection {
        branch: Some(BranchKeyFingerprint::new([branch; 32])),
        checkpoint: Some(WasmCheckpointState {
            generation: WasmStateGeneration::FIRST,
            committed_revision: revision,
            latest_revision: revision,
            stage: WasmCheckpointStage::LocallyDurable,
            required_replicas: None,
            confirmed_replicas: None,
        }),
        recovery: None,
        description,
    }
}

/// A page of `branches`, the unit's every selected entry, with nothing reported beyond it.
fn page(branches: Vec<WasmBranchStateInspection>) -> WasmUnitStatePage {
    WasmUnitStatePage {
        state: WasmUnitStateInspection {
            default_generation: WasmStateGeneration::FIRST,
            reset: None,
            entries: branches.len(),
            checkpoints: WasmCheckpointCounts::default(),
            recoveries: WasmRecoveryCounts::default(),
            branches,
        },
        more: false,
    }
}

/// The archived bytes one reply of `service`'s node leaves the entries of a page.
fn entry_room(service: &SessionServiceImpl) -> usize {
    let room = ReadWasmStatePage::entry_room(service.inner.runtime.inspection_executor());
    usize::try_from(room).assured("a reply's room is one in-memory payload, which fits usize")
}

/// `page` fitted to the reply of `service`'s node.
fn fit(
    service: &SessionServiceImpl,
    page: &mut WasmUnitStatePage,
) -> Result<(), StateInspectionRejection> {
    service.fit_wasm_page_to_reply(&request(), &processor(), &[], &origin(), page)
}

#[nervix_primitives::test]
async fn a_page_whose_reply_fits_is_returned_unchanged() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    let room = entry_room(&service);
    // Together the two entries take half the room.
    let mut fitted = page(vec![entry(1, Some(room / 4)), entry(2, Some(room / 4))]);
    let expected = fitted.clone();

    assert_eq!(fit(&service, &mut fitted), Ok(()));
    assert_eq!(
        fitted, expected,
        "a page whose reply fits keeps every entry, every description and its more flag"
    );
    std::fs::remove_dir_all(path).expect("remove the throwaway database");
}

#[nervix_primitives::test]
async fn an_over_limit_page_loses_only_its_trailing_entries_and_reports_more() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    let room = entry_room(&service);
    // Each entry takes two fifths of the room: no reply holds the three, and the first two take
    // four fifths of it.
    let share = room / 5 * 2;
    let first = entry(1, Some(share));
    let second = entry(2, Some(share));
    let mut fitted = page(vec![first.clone(), second.clone(), entry(3, Some(share))]);

    assert_eq!(fit(&service, &mut fitted), Ok(()));
    assert_eq!(
        fitted.state.branches,
        vec![first, second],
        "only the trailing entry is dropped, and the leading ones keep their descriptions"
    );
    assert!(
        fitted.more,
        "a page that dropped an entry reports that more remain"
    );
    assert_eq!(
        fitted.state.entries, 3,
        "the unit still reports every selected entry, beyond the ones the page returns"
    );
    std::fs::remove_dir_all(path).expect("remove the throwaway database");
}

/// A page reduced to one entry whose reply still does not fit carries that entry's description
/// failure instead, which the processor's render plan on this node produces. A node that does not
/// run the processor any more cannot produce it, and refuses the read as `owner_unavailable`.
#[nervix_primitives::test]
async fn a_lone_entry_over_the_limit_needs_the_running_processor_for_its_failure() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    let room = entry_room(&service);
    // The description alone takes twice the room, so no reply holds the entry.
    let mut fitted = page(vec![entry(1, Some(room * 2))]);

    assert_eq!(
        fit(&service, &mut fitted),
        Err(StateInspectionRejection::OwnerUnavailable)
    );
    std::fs::remove_dir_all(path).expect("remove the throwaway database");
}

/// A page whose reply does not fit with one entry and no description left, or with no entry at
/// all, has nothing left to give way and is refused as `reply_too_large`. Only a payload limit
/// below one entry's own facts reaches this, which the testing build's overflow stands in for.
#[nervix_primitives::test]
async fn a_page_with_nothing_left_to_give_way_is_refused_as_too_large() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    service
        .inner
        .runtime
        .fault_injection()
        .overflow_wasm_state_page_replies(
            DomainNodeRef::node_in(domain(), ModelKind::WasmProcessor, processor()),
            WasmStatePageOverflow::EveryReply,
        );

    let mut undescribed = page(vec![entry(1, None), entry(2, None)]);
    assert_eq!(
        fit(&service, &mut undescribed),
        Err(StateInspectionRejection::ReplyTooLarge),
        "a lone entry without a description has nothing to give way"
    );
    let mut empty = page(Vec::new());
    assert_eq!(
        fit(&service, &mut empty),
        Err(StateInspectionRejection::ReplyTooLarge),
        "a page without entries has nothing to drop"
    );
    std::fs::remove_dir_all(path).expect("remove the throwaway database");
}
