//! A runtime ingestor report across its describe envelope and the production command wire.
//!
//! Layer: test harness.
//! - **Owns.** Current ingestor report generation and the complete round-trip oracle of the
//!   conversion into and out of the describe envelope.
//! - **Depends on.** The runtime ingestor report, the describe envelope conversions and the bounded
//!   interconnect codec.
//! - **Must not know.** How a node gathers the values it reports or renders them.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the ingestor describe envelope boundary")
)]

use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_execution::Executor;
use nervix_interconnect::{
    DescribeIngestorRequest, IngestorDescribeEnvelope, InterconnectRequest, RemoteOperationFailure,
    RkyvMessage,
};

use super::*;
use crate::runtime::{ClientIngestorGauges, IngestorQuiesceCounters, KafkaDomainOffsetDescribe};

const CASE_BYTES: usize = 2048;

/// Builds current runtime ingestor reports, with the metric lines a node sends beside one.
struct Reports<'a> {
    arbitrary: Arbitrary<'a>,
}

impl<'a> Reports<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            arbitrary: Arbitrary::new(bytes, Domain::Vocabulary),
        }
    }

    fn number(&mut self) -> u64 {
        self.arbitrary.entropy().any_u64()
    }

    fn size(&mut self) -> usize {
        let size = self.arbitrary.entropy().any_u64();
        usize::try_from(size).assured("the server builds only for 64-bit targets")
    }

    fn partition(&mut self) -> i32 {
        let bits = self.arbitrary.entropy().between(0..=u64::from(u32::MAX));
        let bits = u32::try_from(bits).verified("the range above ends at u32::MAX");
        bits.cast_signed()
    }

    fn text(&mut self) -> Option<String> {
        if self.arbitrary.entropy().flag() {
            return Some(self.arbitrary.string());
        }
        None
    }

    fn partitions(&mut self) -> Vec<i32> {
        let count = self.arbitrary.entropy().count(4);
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            partitions.push(self.partition());
        }
        partitions
    }

    fn kafka_domain_offsets(&mut self) -> Option<KafkaDomainOffsetDescribe> {
        if !self.arbitrary.entropy().flag() {
            return None;
        }
        let topic = self.arbitrary.string();
        let instances = self.number();
        let observed_partitions = self.partitions();
        let rebalance_epoch = self.number();
        let count = self.arbitrary.entropy().count(3);
        let mut instance_assignments = Vec::with_capacity(count);
        for _ in 0..count {
            instance_assignments.push(self.partitions());
        }
        Some(KafkaDomainOffsetDescribe {
            topic,
            instances,
            observed_partitions,
            rebalance_epoch,
            instance_assignments,
        })
    }

    fn client_producers(&mut self) -> Option<ClientIngestorGauges> {
        if !self.arbitrary.entropy().flag() {
            return None;
        }
        Some(ClientIngestorGauges {
            producers: self.number(),
            forwarded_producers: self.number(),
            outstanding_batches: self.number(),
            outstanding_bytes: self.number(),
            admitted_batches: self.number(),
        })
    }

    fn report(&mut self) -> RuntimeIngestorDescribe {
        let running = self.arbitrary.entropy().flag();
        let ready = self.arbitrary.entropy().flag();
        let quiesce_state = self.text();
        let quiesce_counters = IngestorQuiesceCounters {
            buffered_records: self.size(),
            buffered_bytes: self.size(),
            dropped_total: self.number(),
            rejected_total: self.number(),
        };
        let transport_redeliveries_total = self.number();
        let memory_backpressure_paused = self.arbitrary.entropy().flag();
        let transient_error = self.text();
        let reconnect_backoff = self.text();
        let reconnect_wait_millis = if self.arbitrary.entropy().flag() {
            Some(self.number())
        } else {
            None
        };
        let kafka_domain_offsets = self.kafka_domain_offsets();
        let client_producers = self.client_producers();
        RuntimeIngestorDescribe {
            running,
            ready,
            quiesce_state,
            quiesce_counters,
            transport_redeliveries_total,
            memory_backpressure_paused,
            transient_error,
            reconnect_backoff,
            reconnect_wait_millis,
            kafka_domain_offsets,
            client_producers,
        }
    }

    fn metrics(&mut self) -> Vec<String> {
        let count = self.arbitrary.entropy().count(4);
        let mut metrics = Vec::with_capacity(count);
        for _ in 0..count {
            metrics.push(self.arbitrary.string());
        }
        metrics
    }
}

/// Encodes `envelope` as the owner answers a describe request, and decodes it the way the node
/// that asked does.
async fn through_the_wire(
    executor: &Executor,
    envelope: IngestorDescribeEnvelope,
) -> IngestorDescribeEnvelope {
    let response: Result<IngestorDescribeEnvelope, RemoteOperationFailure> = Ok(envelope);
    let class = DescribeIngestorRequest::CLASS;
    let limit = executor.limits().command_bytes.as_u64();
    let (encoded, _encoded_charge) = response
        .encode_rkyv(executor.clone(), class, limit)
        .await
        .assured("a bounded report fits the command payload limit");
    let (decoded, _decoded_charge) =
        <Result<IngestorDescribeEnvelope, RemoteOperationFailure>>::decode_rkyv(
            executor.clone(),
            class,
            encoded,
        )
        .await
        .assured("a report the transport encoded decodes");
    match decoded {
        Ok(envelope) => envelope,
        Err(failure) => panic!("a successful describe answer changed to a failure: {failure:?}"),
    }
}

#[test]
fn bolero_ingestor_describe_reports_round_trip() {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the ordinary property runtime builds");
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|bytes: &[u8]| {
            let mut reports = Reports::new(bytes);
            let report = reports.report();
            let metrics = reports.metrics();
            let executor = Executor::default();
            let envelope = runtime_ingestor_describe_to_envelope(report.clone(), metrics.clone());
            let received = runtime.block_on(through_the_wire(&executor, envelope));
            let (restored, restored_metrics) = runtime_ingestor_describe_from_envelope(received);
            assert_eq!(
                restored, report,
                "every field of the report survives the envelope and the command wire"
            );
            assert_eq!(
                restored_metrics, metrics,
                "every metric line survives in order"
            );
            assert_eq!(executor.snapshot().commands_memory.reserved_bytes, 0);
        });
}
