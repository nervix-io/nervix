//! Test harness for the current request-admission wire outcomes.
//! Owns: complete remote refusal round trips through the production wire codec.
//! Depends on: current response envelopes, bounded generators and execution admission.
//! Must not know: request retry policy or runtime state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::Entropy;
use nervix_execution::{AdmissionError, ExecutionError, Executor};

use crate::{
    PoolClass, TransportError,
    request::{RemoteRequestFailure, ResponseEnvelope},
};

#[test]
fn bolero_receive_admission_outcomes_preserve_every_field() {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the property runtime opens");
    bolero::check!()
        .with_iterations(64)
        .with_max_len(64)
        .for_each(|input| {
            let mut entropy = Entropy::new(input);
            let class = entropy.pick(PoolClass::ALL);
            let capacity = entropy.boundary_biased(1..=u64::from(u32::MAX));
            let requested = capacity
                .checked_add(1)
                .assured("one byte above a u32 budget fits u64");
            let pending = entropy.boundary_biased(1..=65536);
            let memory = class.memory_class().as_str();
            // Worker labels are diagnostic; dispatch supplies the traffic class separately.
            let cpu = "bulk_cpu";
            let failures = [
                (
                    TransportError::with_cause(
                        error_stack::Report::new(AdmissionError::BudgetExhausted {
                            class: memory,
                            requested: capacity,
                        }),
                        TransportError::Decode,
                    ),
                    RemoteRequestFailure::MemoryBudgetFull {
                        class,
                        requested: capacity,
                    },
                    http::StatusCode::SERVICE_UNAVAILABLE,
                ),
                (
                    TransportError::with_cause(
                        error_stack::Report::new(AdmissionError::ExceedsBudget {
                            class: memory,
                            requested,
                            capacity,
                        }),
                        TransportError::Decode,
                    ),
                    RemoteRequestFailure::MemoryBudgetExceeded {
                        class,
                        requested,
                        capacity,
                    },
                    http::StatusCode::PAYLOAD_TOO_LARGE,
                ),
                (
                    TransportError::with_cause(
                        error_stack::Report::new(AdmissionError::BudgetClosed { class: memory }),
                        TransportError::Decode,
                    ),
                    RemoteRequestFailure::MemoryBudgetClosed { class },
                    http::StatusCode::SERVICE_UNAVAILABLE,
                ),
                (
                    TransportError::with_cause(
                        error_stack::Report::new(ExecutionError::QueueFull {
                            class: cpu,
                            pending: usize::try_from(pending)
                                .assured("the bounded queue count fits supported targets"),
                        }),
                        TransportError::Decode,
                    ),
                    RemoteRequestFailure::ExecutionQueueFull { class, pending },
                    http::StatusCode::SERVICE_UNAVAILABLE,
                ),
                (
                    TransportError::with_cause(
                        error_stack::Report::new(ExecutionError::PoolClosed { class: cpu }),
                        TransportError::Decode,
                    ),
                    RemoteRequestFailure::ExecutionPoolClosed { class },
                    http::StatusCode::SERVICE_UNAVAILABLE,
                ),
            ];
            let executor = Executor::default();
            runtime.block_on(async {
                for (report, expected_failure, status) in failures {
                    nervix_primitives::task::consume_budget().await;
                    let failure = RemoteRequestFailure::decode(report, class);
                    assert_eq!(failure, expected_failure);
                    assert_eq!(failure.status(), status);
                    let expected = ResponseEnvelope {
                        class,
                        request: "fetch_state_checkpoint".to_owned(),
                        result: Err(failure),
                    };
                    let encoded = super::encode_rkyv(
                        &executor,
                        class.memory_class(),
                        class.cpu_class(),
                        class.control_body_limit(&executor),
                        expected.clone(),
                    )
                    .await
                    .assured("the bounded current refusal encodes");
                    let (actual, charge) = super::decode_rkyv::<ResponseEnvelope>(
                        &executor,
                        class.memory_class(),
                        class.cpu_class(),
                        encoded,
                    )
                    .await
                    .assured("the current refusal decodes")
                    .into_parts();
                    assert_eq!(actual, expected);
                    drop(actual);
                    drop(charge);
                    assert_eq!(executor.memory(class.memory_class()).reserved_bytes, 0);
                }
            });
        });
}
