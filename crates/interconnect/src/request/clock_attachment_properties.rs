//! Clock attachments through the production typed-request payload codec.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current attachment requests and all receipt variants, with complete equality.
//! - **Depends on.** The request codec, executor and vocabulary generators.
//! - **Must not know.** Clock installation, delivery tasks or authority selection.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests current clock attachment wire messages")
)]

use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_execution::Executor;
use nervix_models::DomainName;

use super::{
    DomainClockAttachReceipt, DomainClockAttachRequest, progress_properties::assert_round_trips,
};

#[test]
fn bolero_domain_clock_attachments_round_trip() {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the ordinary property runtime builds");
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let request = DomainClockAttachRequest {
                domain_id: arbitrary.rule_name::<DomainName>(),
                target: arbitrary.node_identity(),
                installed_revision: arbitrary.entropy().boundary_biased(0..=u64::MAX),
                generation: arbitrary.entropy().boundary_biased(1..=u64::MAX),
                authority_revision: arbitrary.authority_revision(),
                authority: arbitrary.node_identity(),
            };
            runtime.block_on(async {
                let executor = Executor::default();
                assert_round_trips::<DomainClockAttachRequest, _>(&executor, request).await;
                for receipt in [
                    DomainClockAttachReceipt::Accepted,
                    DomainClockAttachReceipt::ForeignTarget,
                    DomainClockAttachReceipt::UnavailableTarget,
                    DomainClockAttachReceipt::SupersededAuthority,
                    DomainClockAttachReceipt::StaleRevision,
                ] {
                    nervix_primitives::task::consume_budget().await;
                    assert_round_trips::<DomainClockAttachRequest, _>(&executor, receipt).await;
                }
            });
        });
}
