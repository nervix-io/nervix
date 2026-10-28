//! Refusal and complete-value oracles for damaged backup capture messages.
//!
//! Layer: test harness.
//! - **Owns.** Bounded byte damage and the typed refusal or stable re-encoding assertion.
//! - **Depends on.** Current capture generators and the production bounded payload codec.
//! - **Must not know.** Transport admission, capture execution or archive installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;

#[derive(Debug, bolero::TypeGenerator)]
struct MalformedCase {
    capture: CaptureWireCase,
    position: u16,
    byte: u8,
    arbitrary: [u8; 64],
    arbitrary_length: u8,
}

impl MalformedCase {
    fn damaged(&self, encoded: &[u8]) -> [Vec<u8>; 4] {
        assert!(
            !encoded.is_empty(),
            "every current backup wire message has archived fields"
        );
        let position = usize::from(self.position) % encoded.len();
        let truncated = encoded[..position].to_vec();
        let mut flipped = encoded.to_vec();
        flipped[position] ^= 1 << (self.byte % 8);
        let mut replaced = encoded.to_vec();
        replaced[position] = self.byte;
        let length = usize::from(self.arbitrary_length % 65);
        let arbitrary = self.arbitrary[..length].to_vec();
        [truncated, flipped, replaced, arbitrary]
    }

    async fn check<M>(&self, executor: &Executor, class: PoolClass, message: M)
    where
        M: RkyvMessage + Clone + Debug + PartialEq,
    {
        let (bytes, charge) = message
            .encode_rkyv(executor.clone(), class, class.payload_limit(executor))
            .await
            .assured("the bounded current message fits its declared class limit");
        for damaged in self.damaged(&bytes) {
            nervix_primitives::task::consume_budget().await;
            match M::decode_rkyv(executor.clone(), class, damaged).await {
                Err(error) => {
                    assert!(
                        matches!(error.current_context(), crate::TransportError::Decode(_)),
                        "a damaged message is refused as a typed decode failure: {error:#}"
                    );
                }
                Ok((decoded, decoded_charge)) => {
                    let (encoded, encoded_charge) = decoded
                        .clone()
                        .encode_rkyv(executor.clone(), class, class.payload_limit(executor))
                        .await
                        .assured("an accepted bounded message can be re-encoded");
                    let (again, again_charge) = M::decode_rkyv(executor.clone(), class, encoded)
                        .await
                        .assured("an accepted message's encoding decodes");
                    assert_eq!(again, decoded);
                    drop(again);
                    drop(decoded);
                    drop(again_charge);
                    drop(encoded_charge);
                    drop(decoded_charge);
                }
            }
        }
        drop(bytes);
        drop(charge);
        assert_charges_released(executor);
    }
}

#[test]
fn bolero_backup_capture_messages_refuse_damage_or_round_trip() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(128)
        .with_max_len(1024)
        .with_type::<MalformedCase>()
        .for_each(|case| {
            runtime.block_on(async {
                let executor = Executor::default();
                let capture = &case.capture;
                let class = CaptureDomainStateRequest::CLASS;
                case.check(&executor, class, capture.capture()).await;
                case.check(&executor, class, Ok::<(), RemoteOperationFailure>(()))
                    .await;
                for action in [
                    BackupDrainAction::FlushHeldWork,
                    BackupDrainAction::Confirm,
                    BackupDrainAction::Observe,
                ] {
                    nervix_primitives::task::consume_budget().await;
                    case.check(
                        &executor,
                        BackupDrainStatusRequest::CLASS,
                        capture.drain(action),
                    )
                    .await;
                }
                for counters in [[0; 10], [u64::MAX; 10], capture.counters] {
                    nervix_primitives::task::consume_budget().await;
                    case.check(
                        &executor,
                        BackupDrainStatusRequest::CLASS,
                        Ok::<_, RemoteOperationFailure>(CaptureWireCase::drain_status(counters)),
                    )
                    .await;
                }
                case.check(
                    &executor,
                    CaptureInventoryRequest::CLASS,
                    capture.inventory_request(),
                )
                .await;
                for inventory in [Vec::new(), capture.inventory()] {
                    nervix_primitives::task::consume_budget().await;
                    case.check(
                        &executor,
                        CaptureInventoryRequest::CLASS,
                        Ok::<_, RemoteOperationFailure>(inventory),
                    )
                    .await;
                }
                for failure in capture.failures() {
                    nervix_primitives::task::consume_budget().await;
                    case.check(&executor, class, Err::<(), _>(failure.clone()))
                        .await;
                    case.check(
                        &executor,
                        BackupDrainStatusRequest::CLASS,
                        Err::<BackupDrainStatus, _>(failure.clone()),
                    )
                    .await;
                    case.check(
                        &executor,
                        CaptureInventoryRequest::CLASS,
                        Err::<Vec<CapturedSectionInventory>, _>(failure),
                    )
                    .await;
                }
                case.check(&executor, FetchCapturedSection::CLASS, capture.fetch())
                    .await;
            });
        });
}
