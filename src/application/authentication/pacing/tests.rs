//! Tests of the production authentication pacing owner and its elapsed-time contract.
//!
//! Outside the layer order: tests may name any layer and product code may not name them.
//!
//! - **Owns.** Quota, concurrent refusal and bounded-state assertions.
//! - **Depends on.** The production pacing owner and the primitive boundary's paused clock.
//! - **Must not know.** Password hashing or a client transport.

use futures_util::{future::join_all, poll};
use nervix_primitives::time::advance;

use super::*;

fn pacing() -> AuthenticationPacing {
    AuthenticationPacing::new(NonZeroU32::new(2).assured("two is nonzero"))
}

#[nervix_primitives::test(start_paused = true)]
async fn only_judged_refusals_establish_pacing() {
    let pacing = pacing();
    let name = "unknown".to_string();
    for _ in 0..32 {
        nervix_primitives::task::consume_budget().await;
        assert!(matches!(
            pacing.before_verification(&name).await,
            CredentialPacing::Unpaced
        ));
    }
    assert!(pacing.names.lock().await.is_empty());
    pacing
        .refused(name.clone(), CredentialPacing::Unpaced)
        .await;
    assert!(matches!(
        pacing.before_verification(&name).await,
        CredentialPacing::Paced(_)
    ));
}

#[nervix_primitives::test(start_paused = true)]
async fn a_refusal_consumes_one_slot_and_a_paced_refusal_is_not_charged_twice() {
    let pacing = pacing();
    let name = "known".to_string();
    let started = Instant::now();
    pacing
        .refused(name.clone(), CredentialPacing::Unpaced)
        .await;
    let charged = pacing.before_verification(&name).await;
    assert!(matches!(charged, CredentialPacing::Paced(_)));
    pacing.refused(name.clone(), charged).await;
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert!(matches!(
        pacing.before_verification(&name).await,
        CredentialPacing::Paced(_)
    ));
    assert_eq!(started.elapsed(), Duration::from_millis(500));
}

#[nervix_primitives::test(start_paused = true)]
async fn recovered_quota_is_fresh_whether_or_not_another_call_collected_it() {
    for collected in [false, true] {
        let pacing = pacing();
        let target = "target".to_string();
        let blocker = "blocker".to_string();
        pacing
            .refused(target.clone(), CredentialPacing::Unpaced)
            .await;
        let first_admission = pacing.before_verification(&target).await;
        assert!(matches!(first_admission, CredentialPacing::Paced(_)));
        advance(Duration::from_millis(750)).await;
        pacing
            .refused(blocker.clone(), CredentialPacing::Unpaced)
            .await;
        pacing.before_verification(&blocker).await;
        let admission = pacing.before_verification(&target).await;
        assert!(matches!(admission, CredentialPacing::Paced(_)));
        advance(Duration::from_millis(750)).await;

        // The older blocker still has debt when the target's quota fully recovers. A different
        // call can move that blocker behind the target, making the target eligible for cleanup.
        if collected {
            pacing.before_verification(&blocker).await;
            assert!(matches!(
                pacing.before_verification(&"probe".to_string()).await,
                CredentialPacing::Unpaced
            ));
        }
        pacing.refused(target.clone(), first_admission).await;
        pacing.refused(target.clone(), admission).await;
        let started = Instant::now();
        assert!(
            matches!(
                pacing.before_verification(&target).await,
                CredentialPacing::Paced(_)
            ),
            "the judged refusal starts a fresh quota whether collected={collected}"
        );
        assert_eq!(started.elapsed(), Duration::from_millis(500));
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn a_paced_refusal_is_charged_to_a_quota_recreated_after_success() {
    let pacing = pacing();
    let name = "known".to_string();
    pacing
        .refused(name.clone(), CredentialPacing::Unpaced)
        .await;
    let admission = pacing.before_verification(&name).await;
    assert!(matches!(admission, CredentialPacing::Paced(_)));
    pacing.authenticated(&name).await;
    pacing
        .refused(name.clone(), CredentialPacing::Unpaced)
        .await;
    pacing.refused(name.clone(), admission).await;
    let started = Instant::now();
    assert!(matches!(
        pacing.before_verification(&name).await,
        CredentialPacing::Paced(_)
    ));
    assert_eq!(started.elapsed(), Duration::from_millis(500));
}

#[nervix_primitives::test(start_paused = true)]
async fn concurrent_first_refusals_all_pay_the_same_names_quota() {
    let pacing = pacing();
    let name = "unknown".to_string();
    let mut admissions = Vec::new();
    for _ in 0..5 {
        nervix_primitives::task::consume_budget().await;
        admissions.push(pacing.before_verification(&name).await);
    }
    assert!(
        admissions
            .iter()
            .all(|a| matches!(a, CredentialPacing::Unpaced))
    );
    let started = Instant::now();
    join_all(
        admissions
            .into_iter()
            .map(|admission| pacing.refused(name.clone(), admission)),
    )
    .await;
    assert_eq!(started.elapsed(), Duration::from_millis(1500));
    assert_eq!(pacing.names.lock().await.len(), 1);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_wait_releases_the_owner_and_unknown_spray_does_not_erase_another_names_debt() {
    let pacing = pacing();
    let known = "known".to_string();
    pacing
        .refused(known.clone(), CredentialPacing::Unpaced)
        .await;
    pacing.before_verification(&known).await;
    let debt = pacing
        .names
        .lock()
        .await
        .get(&known)
        .assured("the known name has two charged slots")
        .theoretical_arrival;
    let started = Instant::now();
    let waiting = pacing.before_verification(&known);
    futures_util::pin_mut!(waiting);
    assert!(poll!(&mut waiting).is_pending());
    for index in 0..128 {
        nervix_primitives::task::consume_budget().await;
        let name = format!("unknown_{index}");
        assert!(matches!(
            pacing.before_verification(&name).await,
            CredentialPacing::Unpaced
        ));
        pacing.refused(name, CredentialPacing::Unpaced).await;
    }
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(
        pacing
            .names
            .lock()
            .await
            .get(&known)
            .map(|b| b.theoretical_arrival),
        Some(debt)
    );
    assert!(matches!(waiting.await, CredentialPacing::Paced(_)));
    assert_eq!(started.elapsed(), Duration::from_millis(500));
}

#[nervix_primitives::test(start_paused = true)]
async fn successful_authentication_clears_only_its_names_quota() {
    let pacing = pacing();
    let authenticated = "authenticated".to_string();
    let refused = "refused".to_string();
    pacing
        .refused(authenticated.clone(), CredentialPacing::Unpaced)
        .await;
    pacing
        .refused(refused.clone(), CredentialPacing::Unpaced)
        .await;
    pacing.authenticated(&authenticated).await;
    assert!(matches!(
        pacing.before_verification(&authenticated).await,
        CredentialPacing::Unpaced
    ));
    assert!(matches!(
        pacing.before_verification(&refused).await,
        CredentialPacing::Paced(_)
    ));
}

#[nervix_primitives::test(start_paused = true)]
async fn a_refusal_after_concurrent_success_establishes_its_own_bucket() {
    let pacing = pacing();
    let name = "known".to_string();
    pacing
        .refused(name.clone(), CredentialPacing::Unpaced)
        .await;
    let admission = pacing.before_verification(&name).await;
    pacing.authenticated(&name).await;
    pacing.refused(name.clone(), admission).await;
    assert!(matches!(
        pacing.before_verification(&name).await,
        CredentialPacing::Paced(_)
    ));
    assert_eq!(pacing.names.lock().await.len(), 1);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_long_spray_retains_a_recovery_window_instead_of_every_name_ever_refused() {
    let pacing = pacing();
    for wave in 0..64 {
        for index in 0..64 {
            nervix_primitives::task::consume_budget().await;
            let name = format!("unknown_{wave}_{index}");
            let admission = pacing.before_verification(&name).await;
            assert!(matches!(admission, CredentialPacing::Unpaced));
            pacing.refused(name, admission).await;
            assert!(pacing.names.lock().await.len() <= 64);
        }
        advance(Duration::from_secs(1)).await;
    }
    let name = "unknown_63_63".to_string();
    let started = Instant::now();
    assert!(matches!(
        pacing.before_verification(&name).await,
        CredentialPacing::Unpaced
    ));
    assert_eq!(started.elapsed(), Duration::ZERO);
    pacing
        .refused(name.clone(), CredentialPacing::Unpaced)
        .await;
    assert!(matches!(
        pacing.before_verification(&name).await,
        CredentialPacing::Paced(_)
    ));
}

struct AdvancingClock<'a>(&'a Cell<Nanos>);

impl Clock for AdvancingClock<'_> {
    type Instant = Nanos;

    fn now(&self) -> Nanos {
        self.0.get()
    }
}

/// Reanchoring keeps the same answer, exact wait and complete theoretical arrival state as a
/// governor limiter whose clock and state live through every operation. Large idle advances
/// exercise pruning boundaries without making a node's uptime part of a bucket's representation.
#[test]
fn bolero_quota_reanchoring_preserves_governors_complete_state() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let rate = u32::from(bytes.first().copied().unwrap_or_default()) + 1;
            let quota =
                Quota::per_second(NonZeroU32::new(rate).assured("the rate is at least one"));
            let base = Instant::now();
            let mut bucket = AuthenticationBucket {
                theoretical_arrival: base,
            };
            let elapsed = Cell::new(Nanos::new(0));
            let reference_state = Cell::new(None);
            let reference = RateLimiter::<NotKeyed, _, _, NoOpMiddleware<Nanos>>::new(
                quota,
                SnapshotState(&reference_state),
                AdvancingClock(&elapsed),
            );
            for byte in bytes.iter().skip(1) {
                let advance = match byte % 16 {
                    0 | 1 => 0,
                    2 => 1,
                    3 => 999_999,
                    4 => 100_000_000,
                    5 => 999_999_999,
                    6 => 1_000_000_000,
                    7 => 604_800_000_000_000,
                    _ => u64::from(*byte) * 1_000_000,
                };
                elapsed.set(elapsed.get() + Nanos::new(advance));
                let now = base + Duration::from(elapsed.get());
                let expected = reference
                    .check()
                    .err()
                    .map(|limited| limited.wait_time_from(elapsed.get()));
                assert_eq!(bucket.take_slot(quota, now), expected);
                let expected_arrival = base
                    + Duration::from(
                        reference_state
                            .get()
                            .assured("the first check always establishes state"),
                    );
                assert_eq!(bucket.theoretical_arrival, expected_arrival);
            }
        });
}
