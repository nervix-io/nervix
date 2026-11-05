//! The two drivers print one report. The Python driver's tables use the native package's typed
//! enum members, and these tests pair each member with the Rust client's corresponding outcome
//! to preserve every refusal, cause, reopen reason and settlement label.

use std::collections::BTreeMap;

use nervix_client_core::{
    ClientBatchDefect, ClientProcessingFailure, ClientProducerRefusal, ClientSubmissionRefusal,
    ConsumerReopenReason, EmitterSettlement, ProducerOutcome, ProducerReopenReason,
    SubmissionUncertainty, wire::EmitterOpenRefusal,
};
use strum::IntoEnumIterator as _;

use crate::{
    consumers::settlement, ledger::OutcomeKind, refusal::Refusal, reopen::Reopen,
    simulation::outcome_of,
};

/// The Python driver, whose tables hold the words both drivers print.
const PYTHON_DRIVER: &str = include_str!("../../python/paced_simulation.py");

/// The words of the Python driver's table `name`, keyed by the native package's enum members.
fn python_table(name: &str) -> BTreeMap<&'static str, &'static str> {
    let opening = format!("\n{name} = {{");
    let Some(start) = PYTHON_DRIVER.find(&opening) else {
        panic!("the Python driver declares no table {name}");
    };
    let (_, declaration) = PYTHON_DRIVER.split_at(start);
    let Some((_, rest)) = declaration.split_once('{') else {
        panic!("the Python driver's table {name} has no opening brace");
    };
    let Some((entries, _)) = rest.split_once('}') else {
        panic!("the Python driver's table {name} has no closing brace");
    };
    let mut words = BTreeMap::new();
    for entry in entries.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((member, word)) = entry.split_once(':') else {
            panic!("the Python driver's table {name} holds an entry without a member: {entry}");
        };
        let member = member.trim();
        let word = word.trim().trim_matches('"');
        let previous = words.insert(member, word);
        assert_eq!(
            previous, None,
            "the Python driver's table {name} repeats {member}"
        );
    }
    words
}

/// The word for the package enum `member` in a table the Python driver declares.
fn word(table: &BTreeMap<&'static str, &'static str>, member: &str) -> &'static str {
    let Some(&word) = table.get(member) else {
        panic!("the Python driver has no word for the package's enum member {member}");
    };
    word
}

/// A refusal to open, paired with its Python package member.
struct OpenRefusalCase {
    member: &'static str,
    producer: ClientProducerRefusal,
    consumer: EmitterOpenRefusal,
}

const OPEN_REFUSALS: [OpenRefusalCase; 11] = [
    OpenRefusalCase {
        member: "nx.OpenRefusal.DOMAIN_NOT_FOUND",
        producer: ClientProducerRefusal::DomainNotFound,
        consumer: EmitterOpenRefusal::DomainNotFound,
    },
    OpenRefusalCase {
        member: "nx.OpenRefusal.DOMAIN_STOPPED",
        producer: ClientProducerRefusal::DomainStopped,
        consumer: EmitterOpenRefusal::DomainStopped,
    },
    OpenRefusalCase {
        member: "nx.OpenRefusal.ENDPOINT_NOT_FOUND",
        producer: ClientProducerRefusal::IngestorNotFound,
        consumer: EmitterOpenRefusal::EmitterNotFound,
    },
    OpenRefusalCase {
        member: "nx.OpenRefusal.NOT_CLIENT_ENDPOINT",
        producer: ClientProducerRefusal::NotClientIngestor,
        consumer: EmitterOpenRefusal::NotClientEmitter,
    },
    OpenRefusalCase {
        member: "nx.OpenRefusal.ENDPOINT_UNAVAILABLE",
        producer: ClientProducerRefusal::EndpointUnavailable,
        consumer: EmitterOpenRefusal::EndpointUnavailable,
    },
    OpenRefusalCase {
        member: "nx.OpenRefusal.SCHEMA_MISMATCH",
        producer: ClientProducerRefusal::SchemaMismatch,
        consumer: EmitterOpenRefusal::SchemaMismatch,
    },
    OpenRefusalCase {
        member: "nx.OpenRefusal.TOO_MANY_ENDPOINTS",
        producer: ClientProducerRefusal::TooManyProducers,
        consumer: EmitterOpenRefusal::TooManyConsumers,
    },
    OpenRefusalCase {
        member: "nx.OpenRefusal.SESSION_CAPACITY_EXHAUSTED",
        producer: ClientProducerRefusal::SessionCapacityExhausted,
        consumer: EmitterOpenRefusal::SessionCapacityExhausted,
    },
    OpenRefusalCase {
        member: "nx.OpenRefusal.NODE_CAPACITY_EXHAUSTED",
        producer: ClientProducerRefusal::NodeCapacityExhausted,
        consumer: EmitterOpenRefusal::NodeCapacityExhausted,
    },
    OpenRefusalCase {
        member: "nx.OpenRefusal.INVALID_LIMITS",
        producer: ClientProducerRefusal::InvalidLimits,
        consumer: EmitterOpenRefusal::InvalidLimits,
    },
    OpenRefusalCase {
        member: "nx.OpenRefusal.IN_TRANSACTION",
        producer: ClientProducerRefusal::InTransaction,
        consumer: EmitterOpenRefusal::InTransaction,
    },
];

#[test]
fn a_refused_open_prints_the_python_drivers_words() {
    let words = python_table("REFUSALS");
    assert_eq!(words.len(), OPEN_REFUSALS.len());
    assert_eq!(ClientProducerRefusal::iter().count(), OPEN_REFUSALS.len());
    for case in OPEN_REFUSALS {
        let expected = word(&words, case.member);
        assert_eq!(Refusal::from(case.producer).as_str(), expected);
        assert_eq!(Refusal::from(case.consumer).as_str(), expected);
    }
}

/// A reason to open a consumer again, paired with its Python package member.
struct ReopenCase {
    member: &'static str,
    reason: ConsumerReopenReason,
    producer: ProducerReopenReason,
}

#[test]
fn reopen_reasons_and_generation_waits_match_the_python_drivers_vocabulary() {
    let words = python_table("REOPEN_REASONS");
    let cases = [
        ReopenCase {
            member: "nx.ReopenReason.DOMAIN_STOPPED",
            reason: ConsumerReopenReason::DomainStopped,
            producer: ProducerReopenReason::DomainStopped,
        },
        ReopenCase {
            member: "nx.ReopenReason.ENDPOINT_REMOVED",
            reason: ConsumerReopenReason::EndpointRemoved,
            producer: ProducerReopenReason::EndpointRemoved,
        },
        ReopenCase {
            member: "nx.ReopenReason.SCHEMA_CHANGED",
            reason: ConsumerReopenReason::SchemaChanged,
            producer: ProducerReopenReason::SchemaChanged,
        },
        ReopenCase {
            member: "nx.ReopenReason.CONTRACT_CHANGED",
            reason: ConsumerReopenReason::ContractChanged,
            producer: ProducerReopenReason::ContractChanged,
        },
        ReopenCase {
            member: "nx.ReopenReason.GENERATION_CHANGED",
            reason: ConsumerReopenReason::GenerationChanged,
            producer: ProducerReopenReason::GenerationChanged,
        },
        ReopenCase {
            member: "nx.ReopenReason.PROTOCOL_VIOLATED",
            reason: ConsumerReopenReason::ProtocolViolated,
            producer: ProducerReopenReason::ProtocolViolated,
        },
    ];
    for case in cases {
        let consumer = Reopen::from(&case.reason);
        let producer = Reopen::from(&case.producer);
        assert_eq!(consumer, producer);
        assert_eq!(consumer.text(), word(&words, case.member));
        assert_eq!(
            consumer.waits_for_generation(),
            matches!(
                case.reason,
                ConsumerReopenReason::DomainStopped | ConsumerReopenReason::GenerationChanged
            )
        );
    }

    // A refused restoration names its refusal the way a refused open does.
    let refusals = python_table("REFUSALS");
    for case in OPEN_REFUSALS {
        let reason = ConsumerReopenReason::Refused(case.consumer);
        let producer_reason = ProducerReopenReason::Refused(case.producer);
        let consumer = Reopen::from(&reason);
        let producer = Reopen::from(&producer_reason);
        let expected = format!(
            "{} ({})",
            word(&words, "nx.ReopenReason.REFUSED"),
            word(&refusals, case.member)
        );
        assert_eq!(consumer, producer);
        assert_eq!(consumer.text(), expected);
        assert!(!consumer.waits_for_generation());
    }
}

/// A settlement the server answered, paired with its Python package member.
struct SettlementCase {
    member: &'static str,
    settlement: EmitterSettlement,
}

#[test]
fn a_settlement_prints_the_python_drivers_words() {
    let words = python_table("SETTLEMENTS");
    let cases = [
        SettlementCase {
            member: "nx.Settlement.CONFIRMED",
            settlement: EmitterSettlement::Confirmed,
        },
        SettlementCase {
            member: "nx.Settlement.STALE_REFERENCE",
            settlement: EmitterSettlement::StaleReference,
        },
        SettlementCase {
            member: "nx.Settlement.WRONG_CONSUMER",
            settlement: EmitterSettlement::WrongConsumer,
        },
        SettlementCase {
            member: "nx.Settlement.INVALID_REASON",
            settlement: EmitterSettlement::InvalidReason,
        },
        SettlementCase {
            member: "nx.Settlement.CONSUMER_ENDED",
            settlement: EmitterSettlement::ConsumerEnded,
        },
    ];
    assert_eq!(words.len(), cases.len());
    for case in cases {
        assert_eq!(settlement(case.settlement), word(&words, case.member));
    }
}

/// A batch's outcome, paired with the Python package member of its cause.
struct OutcomeCase {
    member: &'static str,
    outcome: ProducerOutcome,
}

/// The kind and cause the Rust driver records and prints for each case, against the Python
/// driver's word for the case's member in `table`.
fn assert_outcomes(table: &str, kind: OutcomeKind, cases: Vec<OutcomeCase>) {
    let words = python_table(table);
    assert_eq!(words.len(), cases.len());
    for case in cases {
        let (printed_kind, cause) = outcome_of(&case.outcome);
        assert_eq!(printed_kind, kind);
        assert_eq!(cause, word(&words, case.member));
    }
}

#[test]
fn a_not_admitted_batch_prints_the_python_drivers_words() {
    let not_admitted = |refusal| ProducerOutcome::NotAdmitted {
        refusal,
        message: String::new(),
    };
    let cases = vec![
        OutcomeCase {
            member: "nx.SubmissionRefusal.INVALID_BATCH",
            outcome: not_admitted(ClientSubmissionRefusal::InvalidBatch(
                ClientBatchDefect::Malformed,
            )),
        },
        OutcomeCase {
            member: "nx.SubmissionRefusal.SUSPENDED",
            outcome: not_admitted(ClientSubmissionRefusal::Suspended),
        },
        OutcomeCase {
            member: "nx.SubmissionRefusal.BUSY",
            outcome: not_admitted(ClientSubmissionRefusal::Busy),
        },
        OutcomeCase {
            member: "nx.SubmissionRefusal.DRAINING",
            outcome: not_admitted(ClientSubmissionRefusal::Draining),
        },
        OutcomeCase {
            member: "nx.SubmissionRefusal.PRODUCER_ENDED",
            outcome: not_admitted(ClientSubmissionRefusal::ProducerEnded),
        },
        OutcomeCase {
            member: "nx.SubmissionRefusal.CREDIT_EXCEEDED",
            outcome: not_admitted(ClientSubmissionRefusal::CreditExceeded),
        },
    ];
    assert_outcomes("SUBMISSION_REFUSALS", OutcomeKind::NotAdmitted, cases);
}

#[test]
fn a_failed_batch_prints_the_python_drivers_words() {
    let failed = |failure| ProducerOutcome::ProcessingFailed {
        failure,
        message: String::new(),
    };
    let cases = vec![
        OutcomeCase {
            member: "nx.ProcessingFailure.ACK_TIMED_OUT",
            outcome: failed(ClientProcessingFailure::AckTimedOut),
        },
        OutcomeCase {
            member: "nx.ProcessingFailure.REJECTED",
            outcome: failed(ClientProcessingFailure::Rejected),
        },
    ];
    assert_eq!(ClientProcessingFailure::iter().count(), cases.len());
    assert_outcomes("PROCESSING_FAILURES", OutcomeKind::ProcessingFailed, cases);
}

#[test]
fn an_uncertain_batch_prints_the_python_drivers_words() {
    let unknown = |cause| ProducerOutcome::OutcomeUnknown {
        cause,
        message: String::new(),
    };
    let cases = vec![
        OutcomeCase {
            member: "nx.Uncertainty.INTERRUPTED",
            outcome: unknown(SubmissionUncertainty::Interrupted),
        },
        OutcomeCase {
            member: "nx.Uncertainty.OWNER_LOST",
            outcome: unknown(SubmissionUncertainty::OwnerLost),
        },
        OutcomeCase {
            member: "nx.Uncertainty.SESSION_LOST",
            outcome: unknown(SubmissionUncertainty::SessionLost),
        },
    ];
    assert_outcomes("UNCERTAINTIES", OutcomeKind::OutcomeUnknown, cases);
}

#[test]
fn a_completed_batch_prints_no_cause() {
    let (kind, cause) = outcome_of(&ProducerOutcome::Completed);
    assert_eq!(kind, OutcomeKind::Completed);
    assert_eq!(cause, "");
}
