//! The codec and relay rules that text completion and the typed choices both apply.

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};

use super::{CodecDirection, RelayQuestion, RelayRule};
use crate::{
    BranchName, CodecJaqFormat, CodecJaqTransformations, CodecWireFormat, CreateRelay,
    MaterializedRelayState, RelayBranching, RelayName, SchemaName, WireSchemaName,
};

fn relay_name(name: &str) -> RelayName {
    RelayName::parse(name).assured("the test names a valid relay")
}

fn branch_name(name: &str) -> BranchName {
    BranchName::parse(name).assured("the test names a valid branch")
}

fn relay(name: &str, schema: &str, branch: Option<&str>, materialized: bool) -> CreateRelay {
    let branching = match branch {
        Some(branch) => RelayBranching::branched_by(branch_name(branch)),
        None => RelayBranching::unbranched(),
    };
    let materialized_state = if materialized {
        Some(MaterializedRelayState::LastByTimestamp)
    } else {
        None
    };
    CreateRelay {
        name: relay_name(name),
        schema: SchemaName::parse(schema).assured("the test names a valid schema"),
        buffer: nonzero_ext::nonzero!(4usize),
        branching,
        materialized_state,
    }
}

/// The identity program where a direction is present.
fn program(present: bool) -> Option<String> {
    if present { Some(".".to_string()) } else { None }
}

fn jaq(on_ingestion: bool, on_emitting: bool) -> CodecWireFormat {
    CodecWireFormat::JaqNative {
        format: CodecJaqFormat::Json,
        transformations: CodecJaqTransformations {
            on_ingestion: program(on_ingestion),
            on_emitting: program(on_emitting),
            on_emitting_batch: None,
        },
    }
}

/// One domain's relays: two unbranched `reading` relays, an unbranched `label` relay, a `reading`
/// relay branched by `by_id`, and an unbranched and a branched `reading` relay with materialized
/// state.
struct Relays {
    by_name: BTreeMap<String, CreateRelay>,
}

impl Relays {
    fn new() -> Self {
        let mut by_name = BTreeMap::new();
        for relay in [
            relay("first", "reading", None, false),
            relay("same", "reading", None, false),
            relay("labels", "label", None, false),
            relay("branched", "reading", Some("by_id"), false),
            relay("state", "reading", None, true),
            relay("branched_state", "reading", Some("by_id"), true),
        ] {
            by_name.insert(relay.name.to_string(), relay);
        }
        Self { by_name }
    }

    fn configured(&self, name: &RelayName) -> Option<&CreateRelay> {
        self.by_name.get(name.as_str())
    }

    fn rule<'a>(&'a self, question: &'a RelayQuestion) -> Option<RelayRule<'a>> {
        question.rule(|name| self.configured(name))
    }

    /// The names of the relays `question` accepts, in name order.
    fn admitted(&self, question: &RelayQuestion) -> Vec<&str> {
        let rule = self
            .rule(question)
            .assured("the question names configured relays");
        let mut admitted = Vec::new();
        for (name, relay) in &self.by_name {
            if rule.admits(relay) {
                admitted.push(name.as_str());
            }
        }
        admitted
    }
}

#[test]
fn a_codec_direction_admits_exactly_the_codecs_its_format_supports() {
    let wire: CodecWireFormat = CodecWireFormat::Json {
        wire_schema: WireSchemaName::parse("reading_wire")
            .assured("the test names a valid wire schema"),
    };
    assert!(CodecDirection::Decoding.admits(&wire));
    assert!(CodecDirection::Encoding.admits(&wire));

    let decodes = jaq(true, false);
    assert!(CodecDirection::Decoding.admits(&decodes));
    assert!(!CodecDirection::Encoding.admits(&decodes));

    let encodes = jaq(false, true);
    assert!(!CodecDirection::Decoding.admits(&encodes));
    assert!(CodecDirection::Encoding.admits(&encodes));
}

#[test]
fn processor_questions_compare_a_candidate_with_the_first_input() {
    let relays = Relays::new();
    assert_eq!(
        relays.admitted(&RelayQuestion::ProcessorInput {
            first: relay_name("first"),
        }),
        ["first", "same", "state"],
        "a further input keeps the first input's schema and exact branching"
    );
    assert_eq!(
        relays.admitted(&RelayQuestion::ProcessorBranch {
            source: relay_name("first"),
        }),
        ["first", "labels", "same", "state"],
        "an output or error relay keeps only the source's branching"
    );
    assert_eq!(
        relays.admitted(&RelayQuestion::ProcessorBranch {
            source: relay_name("branched_state"),
        }),
        ["branched", "branched_state"]
    );
    assert_eq!(
        relays.admitted(&RelayQuestion::ProcessorState {
            first: relay_name("first"),
        }),
        ["state"],
        "materialized state sits on the first input's branching"
    );
}

#[test]
fn emitter_questions_compare_a_candidate_with_the_schema_or_the_shared_branching() {
    let relays = Relays::new();
    assert_eq!(
        relays.admitted(&RelayQuestion::EmitterInput {
            first: relay_name("first"),
        }),
        ["branched", "branched_state", "first", "same", "state"],
        "an emitter's further input keeps the schema on any branching"
    );
    let unbranched = vec![relay_name("first"), relay_name("same")];
    assert_eq!(
        relays.admitted(&RelayQuestion::EmitterState {
            inputs: unbranched.clone(),
        }),
        ["state"]
    );
    assert_eq!(
        relays.admitted(&RelayQuestion::EmitterError { inputs: unbranched }),
        ["first", "labels", "same", "state"]
    );
    assert_eq!(
        relays.admitted(&RelayQuestion::EmitterState {
            inputs: vec![relay_name("branched")],
        }),
        ["branched_state"]
    );

    let mixed = vec![relay_name("first"), relay_name("branched")];
    assert!(
        relays
            .admitted(&RelayQuestion::EmitterError {
                inputs: mixed.clone(),
            })
            .is_empty(),
        "inputs on different branchings share no branching to agree with"
    );
    assert!(
        relays
            .admitted(&RelayQuestion::EmitterState { inputs: mixed })
            .is_empty()
    );
}

#[test]
fn branch_questions_compare_a_candidate_with_the_branch_alone() {
    let relays = Relays::new();
    assert_eq!(
        relays.admitted(&RelayQuestion::OnBranch(None)),
        ["first", "labels", "same", "state"]
    );
    assert_eq!(
        relays.admitted(&RelayQuestion::OnBranch(Some(branch_name("by_id")))),
        ["branched", "branched_state"]
    );
    assert_eq!(
        relays.admitted(&RelayQuestion::MaterializedOnBranch(None)),
        ["state"]
    );
    assert_eq!(
        relays.admitted(&RelayQuestion::MaterializedOnBranch(Some(branch_name(
            "by_id"
        )))),
        ["branched_state"]
    );
}

#[test]
fn a_question_naming_a_relay_that_is_not_configured_has_no_rule() {
    let relays = Relays::new();
    for question in [
        RelayQuestion::ProcessorInput {
            first: relay_name("missing"),
        },
        RelayQuestion::ProcessorBranch {
            source: relay_name("missing"),
        },
        RelayQuestion::ProcessorState {
            first: relay_name("missing"),
        },
        RelayQuestion::EmitterInput {
            first: relay_name("missing"),
        },
        RelayQuestion::EmitterState {
            inputs: vec![relay_name("first"), relay_name("missing")],
        },
        RelayQuestion::EmitterError {
            inputs: vec![relay_name("missing"), relay_name("first")],
        },
    ] {
        assert!(relays.rule(&question).is_none(), "{question:?}");
    }
}

#[test]
fn a_rule_names_the_configured_relays_of_its_question_in_order() {
    let relays = Relays::new();
    let named = |question: &RelayQuestion| {
        let rule = relays
            .rule(question)
            .assured("the question names configured relays");
        let mut names = Vec::new();
        for relay in rule.named() {
            names.push(relay.name.to_string());
        }
        names
    };
    assert_eq!(
        named(&RelayQuestion::EmitterState {
            inputs: vec![relay_name("same"), relay_name("first")],
        }),
        ["same", "first"]
    );
    assert_eq!(
        named(&RelayQuestion::ProcessorBranch {
            source: relay_name("first"),
        }),
        ["first"]
    );
    assert_eq!(
        named(&RelayQuestion::ProcessorInput {
            first: relay_name("same"),
        }),
        ["same"]
    );
    assert_eq!(
        named(&RelayQuestion::ProcessorState {
            first: relay_name("first"),
        }),
        ["first"]
    );
    assert_eq!(
        named(&RelayQuestion::EmitterInput {
            first: relay_name("same"),
        }),
        ["same"]
    );
    assert!(named(&RelayQuestion::OnBranch(None)).is_empty());
}
