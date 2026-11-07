//! Application progress messages through the production typed-request codec.
//!
//! Layer: test harness.
//! - **Owns.** Bounded generation of every current progress query answer, progress report, report
//!   receipt and completion-peers answer, and their exact round trips.
//! - **Depends on.** The current message types, the request codec, the executor and the
//!   vocabulary generators.
//! - **Must not know.** What a barrier waits for or which node leads.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests current application progress wire messages")
)]

use std::fmt::Debug;

use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_execution::Executor;
use nervix_models::ClusterNodeName;

use super::{
    ApplicationCompletionPeersRequest, ApplicationCompletionPeersResponse, ApplicationProgress,
    ApplicationProgressReceipt, ApplicationProgressReport, ApplicationRevisionRequest,
    InterconnectRequest, RkyvMessage,
};

/// The current progress messages one generated input describes.
struct ProgressMessages {
    answer: ApplicationProgress,
    report: ApplicationProgressReport,
    receipt: ApplicationProgressReceipt,
    peers: Option<ApplicationCompletionPeersResponse>,
}

struct Generator<'a> {
    arbitrary: Arbitrary<'a>,
}

impl<'a> Generator<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            arbitrary: Arbitrary::new(bytes, Domain::Vocabulary),
        }
    }

    /// A revision, or none, with the boundaries 0 and `u64::MAX` reachable.
    fn phase(&mut self) -> Option<u64> {
        if !self.arbitrary.entropy().flag() {
            return None;
        }
        Some(self.arbitrary.entropy().boundary_biased(0..=u64::MAX))
    }

    fn progress(&mut self) -> ApplicationProgress {
        let identity = self.arbitrary.node_identity();
        ApplicationProgress {
            identity,
            authoritative: self.phase(),
            runtime_prepared: self.phase(),
            runtime_ready: self.phase(),
            domain_clock_installed: self.phase(),
        }
    }

    fn receipt(&mut self) -> ApplicationProgressReceipt {
        match self.arbitrary.entropy().byte() % 3 {
            0 => ApplicationProgressReceipt::Accepted,
            1 => ApplicationProgressReceipt::NotLeader,
            _ => ApplicationProgressReceipt::ForeignReporter,
        }
    }

    fn completion_peers(&mut self) -> Option<ApplicationCompletionPeersResponse> {
        if !self.arbitrary.entropy().flag() {
            return None;
        }
        let leader = self.arbitrary.node_identity();
        let term = self.arbitrary.entropy().any_u64();
        // At most eight peers, beyond what any test cluster runs.
        let peer_count = self.arbitrary.entropy().byte() % 9;
        let mut peers = Vec::new();
        let mut progress = Vec::new();
        for _ in 0..peer_count {
            let peer = self.progress();
            peers.push(peer.identity.clone());
            if self.arbitrary.entropy().flag() {
                progress.push(peer);
            }
        }
        Some(ApplicationCompletionPeersResponse {
            leader,
            term,
            peers,
            progress,
        })
    }

    fn messages(&mut self) -> ProgressMessages {
        let answer = self.progress();
        let report = ApplicationProgressReport {
            progress: self.progress(),
            leader: self.arbitrary.rule_name::<ClusterNodeName>(),
            term: self.arbitrary.entropy().any_u64(),
        };
        let receipt = self.receipt();
        let peers = self.completion_peers();
        ProgressMessages {
            answer,
            report,
            receipt,
            peers,
        }
    }
}

/// Encodes `message` as the transport encodes the payload or the answer of request `R`, and
/// decodes it as the receiving node does.
async fn round_trip<R, M>(executor: &Executor, message: M) -> M
where
    R: InterconnectRequest,
    M: RkyvMessage,
{
    let class = R::CLASS;
    let (encoded, _reservation) = message
        .encode_rkyv(executor.clone(), class, class.payload_limit(executor))
        .await
        .assured("a bounded current progress message encodes within the management payload limit");
    let (decoded, _reservation) = M::decode_rkyv(executor.clone(), class, encoded)
        .await
        .assured("a progress message the transport encoded decodes");
    decoded
}

pub(super) async fn assert_round_trips<R, M>(executor: &Executor, message: M)
where
    R: InterconnectRequest,
    M: RkyvMessage + Clone + Debug + PartialEq,
{
    let decoded = round_trip::<R, M>(executor, message.clone()).await;
    assert_eq!(decoded, message);
}

#[test]
fn bolero_application_progress_messages_round_trip() {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the ordinary test runtime builds");
    bolero::check!()
        .with_iterations(256)
        // The register's input bound for this target.
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let ProgressMessages {
                answer,
                report,
                receipt,
                peers,
            } = Generator::new(bytes).messages();
            runtime.block_on(async {
                let executor = Executor::default();
                assert_round_trips::<ApplicationRevisionRequest, _>(
                    &executor,
                    ApplicationRevisionRequest,
                )
                .await;
                assert_round_trips::<ApplicationRevisionRequest, _>(&executor, answer).await;
                assert_round_trips::<ApplicationProgressReport, _>(&executor, report).await;
                assert_round_trips::<ApplicationProgressReport, _>(&executor, receipt).await;
                assert_round_trips::<ApplicationCompletionPeersRequest, _>(
                    &executor,
                    ApplicationCompletionPeersRequest,
                )
                .await;
                assert_round_trips::<ApplicationCompletionPeersRequest, _>(&executor, peers).await;
            });
        });
}
