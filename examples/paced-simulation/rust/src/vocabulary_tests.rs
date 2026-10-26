//! The two drivers print one report, so for every refusal, outcome, reopen reason and settlement
//! the Rust driver prints the word the Python driver prints for the same case. The Python driver
//! keeps its words in tables keyed by the numbers `crates/client-ffi/include/nervix_client.h`
//! gives each case; these tests read those tables from its source and pair each number with the
//! client value the shared binding reports under it.

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

/// The words of the Python driver's table `name`, keyed by the binding's numbers.
fn python_table(name: &str) -> BTreeMap<u8, &'static str> {
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
        let Some((number, word)) = entry.split_once(':') else {
            panic!("the Python driver's table {name} holds an entry without a number: {entry}");
        };
        let number = match number.trim().parse::<u8>() {
            Ok(number) => number,
            Err(error) => panic!("the Python driver's table {name} numbers {entry}: {error}"),
        };
        let word = word.trim().trim_matches('"');
        let previous = words.insert(number, word);
        assert_eq!(
            previous, None,
            "the Python driver's table {name} repeats {number}"
        );
    }
    words
}

/// The word of `number` in a table the Python driver declares.
fn word(table: &BTreeMap<u8, &'static str>, number: u8) -> &'static str {
    let Some(&word) = table.get(&number) else {
        panic!("the Python driver has no word for the binding's number {number}");
    };
    word
}

/// A refusal to open, under the number the binding reports it with.
struct OpenRefusalCase {
    number: u8,
    producer: ClientProducerRefusal,
    consumer: EmitterOpenRefusal,
}

const OPEN_REFUSALS: [OpenRefusalCase; 11] = [
    OpenRefusalCase {
        number: 1,
        producer: ClientProducerRefusal::DomainNotFound,
        consumer: EmitterOpenRefusal::DomainNotFound,
    },
    OpenRefusalCase {
        number: 2,
        producer: ClientProducerRefusal::DomainStopped,
        consumer: EmitterOpenRefusal::DomainStopped,
    },
    OpenRefusalCase {
        number: 3,
        producer: ClientProducerRefusal::IngestorNotFound,
        consumer: EmitterOpenRefusal::EmitterNotFound,
    },
    OpenRefusalCase {
        number: 4,
        producer: ClientProducerRefusal::NotClientIngestor,
        consumer: EmitterOpenRefusal::NotClientEmitter,
    },
    OpenRefusalCase {
        number: 5,
        producer: ClientProducerRefusal::EndpointUnavailable,
        consumer: EmitterOpenRefusal::EndpointUnavailable,
    },
    OpenRefusalCase {
        number: 6,
        producer: ClientProducerRefusal::SchemaMismatch,
        consumer: EmitterOpenRefusal::SchemaMismatch,
    },
    OpenRefusalCase {
        number: 7,
        producer: ClientProducerRefusal::TooManyProducers,
        consumer: EmitterOpenRefusal::TooManyConsumers,
    },
    OpenRefusalCase {
        number: 8,
        producer: ClientProducerRefusal::SessionCapacityExhausted,
        consumer: EmitterOpenRefusal::SessionCapacityExhausted,
    },
    OpenRefusalCase {
        number: 9,
        producer: ClientProducerRefusal::NodeCapacityExhausted,
        consumer: EmitterOpenRefusal::NodeCapacityExhausted,
    },
    OpenRefusalCase {
        number: 10,
        producer: ClientProducerRefusal::InvalidLimits,
        consumer: EmitterOpenRefusal::InvalidLimits,
    },
    OpenRefusalCase {
        number: 11,
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
        let expected = word(&words, case.number);
        assert_eq!(Refusal::from(case.producer).as_str(), expected);
        assert_eq!(Refusal::from(case.consumer).as_str(), expected);
    }
}

/// A reason to open a consumer again, under the number the binding reports it with.
struct ReopenCase {
    number: u8,
    reason: ConsumerReopenReason,
    producer: ProducerReopenReason,
}

#[test]
fn reopen_reasons_and_generation_waits_match_the_python_drivers_vocabulary() {
    let words = python_table("REOPEN_REASONS");
    let cases = [
        ReopenCase {
            number: 1,
            reason: ConsumerReopenReason::DomainStopped,
            producer: ProducerReopenReason::DomainStopped,
        },
        ReopenCase {
            number: 2,
            reason: ConsumerReopenReason::EndpointRemoved,
            producer: ProducerReopenReason::EndpointRemoved,
        },
        ReopenCase {
            number: 3,
            reason: ConsumerReopenReason::SchemaChanged,
            producer: ProducerReopenReason::SchemaChanged,
        },
        ReopenCase {
            number: 4,
            reason: ConsumerReopenReason::ContractChanged,
            producer: ProducerReopenReason::ContractChanged,
        },
        ReopenCase {
            number: 5,
            reason: ConsumerReopenReason::GenerationChanged,
            producer: ProducerReopenReason::GenerationChanged,
        },
        ReopenCase {
            number: 6,
            reason: ConsumerReopenReason::ProtocolViolated,
            producer: ProducerReopenReason::ProtocolViolated,
        },
    ];
    for case in cases {
        let consumer = Reopen::from(&case.reason);
        let producer = Reopen::from(&case.producer);
        assert_eq!(consumer, producer);
        assert_eq!(consumer.text(), word(&words, case.number));
        assert_eq!(
            consumer.waits_for_generation(),
            matches!(case.number, 1 | 5)
        );
    }

    // A refused restoration names its refusal the way a refused open does.
    let refusals = python_table("REFUSALS");
    for case in OPEN_REFUSALS {
        let reason = ConsumerReopenReason::Refused(case.consumer);
        let producer_reason = ProducerReopenReason::Refused(case.producer);
        let consumer = Reopen::from(&reason);
        let producer = Reopen::from(&producer_reason);
        let expected = format!("{} ({})", word(&words, 7), word(&refusals, case.number));
        assert_eq!(consumer, producer);
        assert_eq!(consumer.text(), expected);
        assert!(!consumer.waits_for_generation());
    }
}

/// A settlement the server answered, under the number the binding reports it with.
struct SettlementCase {
    number: u8,
    settlement: EmitterSettlement,
}

#[test]
fn a_settlement_prints_the_python_drivers_words() {
    let words = python_table("SETTLEMENTS");
    let cases = [
        SettlementCase {
            number: 1,
            settlement: EmitterSettlement::Confirmed,
        },
        SettlementCase {
            number: 2,
            settlement: EmitterSettlement::StaleReference,
        },
        SettlementCase {
            number: 3,
            settlement: EmitterSettlement::WrongConsumer,
        },
        SettlementCase {
            number: 4,
            settlement: EmitterSettlement::InvalidReason,
        },
        SettlementCase {
            number: 5,
            settlement: EmitterSettlement::ConsumerEnded,
        },
    ];
    assert_eq!(words.len(), cases.len());
    for case in cases {
        assert_eq!(settlement(case.settlement), word(&words, case.number));
    }
}

/// A batch's outcome, under the number the binding reports its cause with.
struct OutcomeCase {
    number: u8,
    outcome: ProducerOutcome,
}

/// The kind and cause the Rust driver records and prints for each case, against the Python
/// driver's word for the case's number in `table`.
fn assert_outcomes(table: &str, kind: OutcomeKind, cases: Vec<OutcomeCase>) {
    let words = python_table(table);
    assert_eq!(words.len(), cases.len());
    for case in cases {
        let (printed_kind, cause) = outcome_of(&case.outcome);
        assert_eq!(printed_kind, kind);
        assert_eq!(cause, word(&words, case.number));
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
            number: 1,
            outcome: not_admitted(ClientSubmissionRefusal::InvalidBatch(
                ClientBatchDefect::Malformed,
            )),
        },
        OutcomeCase {
            number: 2,
            outcome: not_admitted(ClientSubmissionRefusal::Suspended),
        },
        OutcomeCase {
            number: 3,
            outcome: not_admitted(ClientSubmissionRefusal::Busy),
        },
        OutcomeCase {
            number: 4,
            outcome: not_admitted(ClientSubmissionRefusal::Draining),
        },
        OutcomeCase {
            number: 5,
            outcome: not_admitted(ClientSubmissionRefusal::ProducerEnded),
        },
        OutcomeCase {
            number: 6,
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
            number: 1,
            outcome: failed(ClientProcessingFailure::AckTimedOut),
        },
        OutcomeCase {
            number: 2,
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
            number: 1,
            outcome: unknown(SubmissionUncertainty::Interrupted),
        },
        OutcomeCase {
            number: 2,
            outcome: unknown(SubmissionUncertainty::OwnerLost),
        },
        OutcomeCase {
            number: 3,
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
