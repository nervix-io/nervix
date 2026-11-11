//! Semantic offers for the codec and relay references whose position names their role.
//!
//! Layer: language.
//! - **Owns.** Turning a codec direction or relay role label into the semantic question its
//!   position asks, with the relays the statement being written names before the cursor, read with
//!   the grammar's own clause parsers.
//! - **Depends on.** Shared clause grammar, statement heads composed from vocabulary declarations,
//!   the reference role labels and the vocabulary's completion questions.
//! - **Must not know.** Registries, sessions or the Models a domain configures.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        language,
        reason = "owns the semantic completion boundary declared above"
    )
)]

use chumsky::prelude::*;
use meticulous::ResultExt as _;
use nervix_models::{ModelKind, RelayName, RelayQuestion, SemanticReference};

use super::{CompletionExpectation, Keyword, Token, ingestor_source_kind};
use crate::parser_support::{
    ParseError, RelayRole, codec_direction, collect_for, emitter_name, from_relay_clause,
    if_not_exists_clause, kw, kw_phrase3, relay_ref, tok, word_raw,
};

/// The processors whose statement names its inputs in one `FROM` list right after its name.
const LISTED_INPUT_PROCESSORS: [ModelKind; 7] = [
    ModelKind::Junction,
    ModelKind::Deduplicator,
    ModelKind::Reorderer,
    ModelKind::WindowProcessor,
    ModelKind::Inferencer,
    ModelKind::WasmProcessor,
    ModelKind::Reingestor,
];

/// One immutable statement prefix, with the relays its codec and relay role positions agree with.
pub(super) struct ReferenceCompletion {
    named: Option<NamedRelays>,
}

impl ReferenceCompletion {
    /// Reads the relays `tokens` name only when one of `labels` is a relay role, since no other
    /// position asks for them.
    pub(super) fn new(tokens: &[Token], labels: &[String]) -> Self {
        let mut asks_for_relays = false;
        for label in labels {
            if RelayRole::of_label(label).is_some() {
                asks_for_relays = true;
                break;
            }
        }
        let named = if asks_for_relays {
            NamedRelays::of(tokens)
        } else {
            None
        };
        Self { named }
    }

    /// The expectation a codec direction or relay role label states. `None` means the label
    /// belongs to another grammar concept.
    pub(super) fn expectation(&self, label: &str) -> Option<CompletionExpectation> {
        if let Some(direction) = codec_direction(label) {
            return Some(CompletionExpectation::Semantic(SemanticReference::Codec(
                direction,
            )));
        }
        let role = RelayRole::of_label(label)?;
        let question = match &self.named {
            Some(named) => named.question(role),
            None => None,
        };
        // A relay that agrees with no relay the statement names can be any relay of the domain.
        let reference = match question {
            Some(question) => SemanticReference::Relay(question),
            None => SemanticReference::Model(ModelKind::Relay),
        };
        Some(CompletionExpectation::Semantic(reference))
    }
}

/// The relays a statement being written names before the cursor, in the clauses that its later
/// relay references must agree with.
#[derive(Debug, Clone, PartialEq, Eq)]
enum NamedRelays {
    /// A processor that reads one `FROM` list: the inputs written so far.
    Processor { inputs: Vec<RelayName> },
    /// A correlator: its left inputs, and its right inputs once `RIGHT FROM` is written.
    Correlator {
        left: Vec<RelayName>,
        right: Option<Vec<RelayName>>,
    },
    /// A generator, which reads the materialized state of `state`.
    Generator { state: RelayName },
    /// An emitter: the inputs written so far.
    Emitter { inputs: Vec<RelayName> },
    /// An ingestor, whose message errors are never branched.
    Ingestor,
}

impl NamedRelays {
    fn of(tokens: &[Token]) -> Option<Self> {
        if ingestor_source_kind(tokens).is_some() {
            return Some(Self::Ingestor);
        }
        Self::prefix().parse(tokens).into_output()
    }

    /// A `CREATE` statement of a processor, correlator, generator or emitter up to the relays it
    /// names, followed by whatever has been written since.
    fn prefix<'src>() -> impl Parser<'src, &'src [Token], Self, extra::Err<ParseError<'src>>> {
        let mut heads = Vec::with_capacity(LISTED_INPUT_PROCESSORS.len());
        for kind in LISTED_INPUT_PROCESSORS {
            heads.push(kind_head(kind));
        }
        let processor = choice(heads)
            .ignore_then(word_raw())
            .ignore_then(kw(Keyword::From))
            .ignore_then(input_list())
            .map(|inputs| Self::Processor { inputs });
        let right = collect_for()
            .or_not()
            .ignore_then(kw(Keyword::Right))
            .ignore_then(kw(Keyword::From))
            .ignore_then(input_list());
        let correlator = kind_head(ModelKind::Correlator)
            .ignore_then(word_raw())
            .ignore_then(kw(Keyword::Left))
            .ignore_then(kw(Keyword::From))
            .ignore_then(input_list())
            .then(right.or_not())
            .map(|(left, right)| Self::Correlator { left, right });
        let generator = kind_head(ModelKind::Generator)
            .ignore_then(word_raw())
            .ignore_then(kw_phrase3(
                Keyword::Using,
                Keyword::Materialized,
                Keyword::State,
            ))
            .ignore_then(relay_ref())
            .map(|state| Self::Generator { state });
        let emitter = crate::emitter::head_kind()
            .ignore_then(emitter_name())
            .ignore_then(kw(Keyword::From))
            .ignore_then(input_list())
            .map(|inputs| Self::Emitter { inputs });
        kw(Keyword::Create)
            .ignore_then(if_not_exists_clause())
            .ignore_then(choice((processor, correlator, generator, emitter)))
            .then_ignore(any().repeated())
            .then_ignore(end())
    }

    /// The question a relay reference in `role` asks. `None` means the statement names no relay
    /// that the reference must agree with.
    fn question(&self, role: RelayRole) -> Option<RelayQuestion> {
        match (role, self) {
            (RelayRole::Input, Self::Processor { inputs }) => Some(RelayQuestion::ProcessorInput {
                first: inputs.first()?.clone(),
            }),
            (RelayRole::Input, Self::Correlator { left, right }) => {
                // The side being written is the right one once `RIGHT FROM` is.
                let side = match right {
                    Some(right) => right,
                    None => left,
                };
                Some(RelayQuestion::ProcessorInput {
                    first: side.first()?.clone(),
                })
            }
            (RelayRole::Input, Self::Emitter { inputs }) => Some(RelayQuestion::EmitterInput {
                first: inputs.first()?.clone(),
            }),
            (
                RelayRole::State,
                Self::Processor { inputs } | Self::Correlator { left: inputs, .. },
            ) => Some(RelayQuestion::ProcessorState {
                first: inputs.first()?.clone(),
            }),
            (RelayRole::State, Self::Emitter { inputs }) if !inputs.is_empty() => {
                Some(RelayQuestion::EmitterState {
                    inputs: inputs.clone(),
                })
            }
            (
                RelayRole::Output | RelayRole::Error,
                Self::Processor { inputs } | Self::Correlator { left: inputs, .. },
            ) => Some(RelayQuestion::ProcessorBranch {
                source: inputs.first()?.clone(),
            }),
            (RelayRole::Output | RelayRole::Error, Self::Generator { state }) => {
                Some(RelayQuestion::ProcessorBranch {
                    source: state.clone(),
                })
            }
            (RelayRole::Error, Self::Emitter { inputs }) if !inputs.is_empty() => {
                Some(RelayQuestion::EmitterError {
                    inputs: inputs.clone(),
                })
            }
            (RelayRole::Error, Self::Ingestor) => Some(RelayQuestion::OnBranch(None)),
            (RelayRole::LeftTimeout, Self::Correlator { left, .. }) => {
                Some(RelayQuestion::ProcessorInput {
                    first: left.first()?.clone(),
                })
            }
            (
                RelayRole::RightTimeout,
                Self::Correlator {
                    right: Some(right), ..
                },
            ) => Some(RelayQuestion::ProcessorInput {
                first: right.first()?.clone(),
            }),
            _ => None,
        }
    }
}

/// The head words of `kind`'s statements, composed from the kind's vocabulary declaration.
fn kind_head<'src>(
    kind: ModelKind,
) -> Boxed<'src, 'src, &'src [Token], (), extra::Err<ParseError<'src>>> {
    let mut head = empty().boxed();
    for word in kind.keyword_phrase().split(' ') {
        let keyword = word
            .parse::<Keyword>()
            .assured("every Model kind's keyword phrase belongs to the shared token definition");
        head = head.ignore_then(kw(keyword)).boxed();
    }
    head
}

/// The relays a `FROM` list names before the cursor, whether the list is complete or still open
/// after a comma.
fn input_list<'src>()
-> impl Parser<'src, &'src [Token], Vec<RelayName>, extra::Err<ParseError<'src>>> + Clone {
    from_relay_clause()
        .map(|(relay, _)| relay)
        .separated_by(tok(Token::Comma))
        .allow_trailing()
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{BranchName, CodecDirection, ModelKind, RelayName, RelayQuestion};

    use super::{
        super::{CompletionExpectation, SemanticReference, suggest_client_expectations},
        NamedRelays,
    };
    use crate::parser_support::RelayRole;

    const INGESTOR: &str = "CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK ON \
                            QUIESCE BUFFER MAX SIZE 1MiB";
    const EMITTER_DELIVERY: &str =
        "TO broker TOPIC 'events' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s";
    const CORRELATOR: &str = "CREATE CORRELATOR pairs LEFT FROM readings RIGHT FROM labels \
                              CORRELATE WHERE left.id = right.id MATCH EARLIEST MAX TIME 5s ON \
                              CORRELATION TIMEOUT";

    fn relay(name: &str) -> RelayName {
        RelayName::parse(name).assured("the test names a valid relay")
    }

    /// The semantic references completion expects at the end of `input`.
    fn references(input: &str) -> Vec<SemanticReference> {
        let mut references = Vec::new();
        for expectation in suggest_client_expectations(input, input.len()) {
            if let CompletionExpectation::Semantic(reference) = expectation {
                references.push(reference);
            }
        }
        references
    }

    fn assert_expects(input: &str, expected: SemanticReference) {
        assert_eq!(references(input), [expected], "{input}");
    }

    fn assert_relay_question(input: &str, question: RelayQuestion) {
        assert_expects(input, SemanticReference::Relay(question));
    }

    #[test]
    fn a_codec_position_asks_for_the_direction_its_clause_uses() {
        let decoding = SemanticReference::Codec(CodecDirection::Decoding);
        let encoding = SemanticReference::Codec(CodecDirection::Encoding);
        assert_expects(&format!("{INGESTOR} DECODE USING "), decoding.clone());
        assert_expects(
            "ALTER HTTP ENDPOINT INGESTOR source SET DECODE USING ",
            decoding.clone(),
        );
        assert_expects(
            "CREATE HASH MAP lookup_by_id KEY id FROM RESOURCE bundle VERSION 1 PATH \
             'lookup.jsonl' DECODE USING ",
            decoding,
        );
        assert_expects(
            &format!(
                "CREATE KAFKA TOPIC EMITTER sink FROM readings {EMITTER_DELIVERY} ENCODE USING "
            ),
            encoding.clone(),
        );
        assert_expects("ALTER KAFKA TOPIC EMITTER sink SET ENCODE USING ", encoding);
        assert_expects("DROP CODEC ", SemanticReference::Model(ModelKind::Codec));
    }

    #[test]
    fn a_processor_relay_agrees_with_the_first_input_of_its_statement() {
        let first = || RelayQuestion::ProcessorInput {
            first: relay("readings"),
        };
        assert_expects(
            "CREATE JUNCTION copy FROM ",
            SemanticReference::Model(ModelKind::Relay),
        );
        assert_relay_question("CREATE JUNCTION copy FROM readings, ", first());
        assert_relay_question(
            "CREATE JUNCTION copy FROM readings WHERE input.value > 1, more, ",
            first(),
        );
        assert_relay_question(
            "CREATE IF NOT EXISTS DEDUPLICATOR dedup FROM readings, ",
            first(),
        );
        assert_relay_question("CREATE WINDOW PROCESSOR window FROM readings, ", first());
        assert_relay_question("CREATE REINGESTOR again FROM readings, ", first());
        assert_relay_question(
            "CREATE JUNCTION copy FROM readings UNBRANCHED USING MATERIALIZED STATE ",
            RelayQuestion::ProcessorState {
                first: relay("readings"),
            },
        );
        let branch = || RelayQuestion::ProcessorBranch {
            source: relay("readings"),
        };
        assert_relay_question(
            "CREATE JUNCTION copy FROM readings UNBRANCHED TO ",
            branch(),
        );
        assert_relay_question(
            "CREATE JUNCTION copy FROM readings UNBRANCHED TO more INHERIT ALL FLUSH IMMEDIATE ON \
             MESSAGE ERROR SEND TO ",
            branch(),
        );
        assert_relay_question(
            "CREATE WASM PROCESSOR guest FROM readings USING RESOURCE guest VERSION 1 FILE \
             'guest.wasm' MAX FUEL 1000 MAX MEMORY 64MiB UNBRANCHED TO ",
            branch(),
        );
        assert_relay_question(
            "CREATE REINGESTOR again FROM readings TO more INHERIT ALL UNBRANCHED FLUSH IMMEDIATE \
             ON MESSAGE ERROR SEND TO ",
            branch(),
        );
    }

    #[test]
    fn a_relay_whose_narrowing_clause_follows_it_is_any_relay() {
        let any = SemanticReference::Model(ModelKind::Relay);
        assert_expects("CREATE REINGESTOR again FROM readings TO ", any.clone());
        assert_expects(
            "CREATE GENERATOR synth USING MATERIALIZED STATE ",
            any.clone(),
        );
        assert_expects(
            "ALTER JUNCTION copy ADD ROUTE TO more INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
             SEND TO ",
            any,
        );
    }

    #[test]
    fn a_correlator_relay_agrees_with_the_first_relay_of_its_side() {
        let side = |name: &str| RelayQuestion::ProcessorInput { first: relay(name) };
        assert_relay_question(
            "CREATE CORRELATOR pairs LEFT FROM readings, ",
            side("readings"),
        );
        assert_expects(
            "CREATE CORRELATOR pairs LEFT FROM readings RIGHT FROM ",
            SemanticReference::Model(ModelKind::Relay),
        );
        assert_relay_question(
            "CREATE CORRELATOR pairs LEFT FROM readings RIGHT FROM labels, ",
            side("labels"),
        );
        assert_relay_question(&format!("{CORRELATOR} SEND TO "), side("readings"));
        assert_relay_question(&format!("{CORRELATOR} DROP, SEND TO "), side("labels"));
        assert_relay_question(
            &format!("{CORRELATOR} DROP, DROP UNBRANCHED TO "),
            RelayQuestion::ProcessorBranch {
                source: relay("readings"),
            },
        );
    }

    #[test]
    fn a_generator_route_keeps_the_branch_of_its_materialized_relay() {
        assert_relay_question(
            "CREATE GENERATOR synth USING MATERIALIZED STATE state EACH 100ms UNBRANCHED TO ",
            RelayQuestion::ProcessorBranch {
                source: relay("state"),
            },
        );
    }

    #[test]
    fn an_emitter_relay_agrees_with_the_inputs_of_its_statement() {
        assert_relay_question(
            "CREATE KAFKA TOPIC EMITTER sink FROM readings, ",
            RelayQuestion::EmitterInput {
                first: relay("readings"),
            },
        );
        assert_relay_question(
            "CREATE KAFKA TOPIC EMITTER sink FROM readings, more USING MATERIALIZED STATE ",
            RelayQuestion::EmitterState {
                inputs: vec![relay("readings"), relay("more")],
            },
        );
        assert_relay_question(
            &format!(
                "CREATE KAFKA TOPIC EMITTER sink FROM readings, more {EMITTER_DELIVERY} ENCODE \
                 USING codec INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR SEND TO "
            ),
            RelayQuestion::EmitterError {
                inputs: vec![relay("readings"), relay("more")],
            },
        );
    }

    #[test]
    fn a_role_its_statement_names_no_relay_for_asks_no_question() {
        let no_inputs = NamedRelays::Emitter { inputs: Vec::new() };
        assert_eq!(no_inputs.question(RelayRole::State), None);
        assert_eq!(no_inputs.question(RelayRole::Error), None);
        assert_eq!(no_inputs.question(RelayRole::Input), None);
        let left_only = NamedRelays::Correlator {
            left: vec![relay("readings")],
            right: None,
        };
        assert_eq!(left_only.question(RelayRole::RightTimeout), None);
        assert_eq!(NamedRelays::Ingestor.question(RelayRole::Input), None);
        let generator = NamedRelays::Generator {
            state: relay("state"),
        };
        assert_eq!(generator.question(RelayRole::State), None);
    }

    #[test]
    fn an_ingestor_error_relay_has_no_branch() {
        let unbranched = RelayQuestion::OnBranch(None::<BranchName>);
        assert_relay_question(
            &format!(
                "{INGESTOR} DECODE USING codec TO readings INHERIT ALL UNBRANCHED FLUSH IMMEDIATE \
                 ON MESSAGE ERROR SEND TO "
            ),
            unbranched.clone(),
        );
        assert_relay_question(
            "ALTER HTTP ENDPOINT INGESTOR source ADD ROUTE TO readings INHERIT ALL UNBRANCHED \
             FLUSH IMMEDIATE ON MESSAGE ERROR SEND TO ",
            unbranched,
        );
    }
}
