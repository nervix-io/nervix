//! Connector completion through the composed client grammar.
//!
//! Layer: test harness.
//! - **Owns.** Declaration-wide semantic completion and expression-position regression checks.
//! - **Depends on.** The composed client grammar, connector declarations and bounded generators.
//! - **Must not know.** Registry state, session transports or connector drivers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "owns the declaration-wide completion checks declared above"
    )
)]

use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{HttpIngestorEntity, KafkaEmitterEntity};

use super::*;

#[test]
fn every_declared_ingestor_completes_its_filter_functions_and_metadata() {
    for kind in IngestorEntityKind::all() {
        let bytes = vec![7; 2048];
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Plexus);
        let mut ingestor = arbitrary.create_ingestor_from(kind);
        ingestor.filter_where = None;
        ingestor.timestamp_source = None;
        let statement = Statement::Create(nervix_models::CreateStatement::new(
            Box::new(nervix_models::Model::Ingestor(ingestor)),
            false,
        ));
        let canonical = statement
            .to_canonical_plexus()
            .assured("the bounded generator constructs a renderable current ingestor Model");
        let lexed =
            lex_input(&canonical).assured("canonical Plexus is generated from structured Models");
        let output = lexed
            .tokens
            .iter()
            .position(|token| {
                matches!(
                    token,
                    Token::Word(Word::KnownWord {
                        iden: Keyword::To,
                        ..
                    })
                )
            })
            .assured("the generator constructs an ingestor with at least one output route");
        let end = lexed.spanned_tokens[output].span.start;
        let prefix = &canonical[..end];
        let input = format!("{prefix}FILTER WHERE co");
        assert!(
            suggest_client_expectations(&input, input.len()).contains(
                &CompletionExpectation::Semantic(SemanticReference::BuiltinFunction(
                    BuiltinFunctionScope::IngestorEntity(kind)
                ))
            ),
            "{kind}: {input}"
        );
        let input = format!("{prefix}FILTER WHERE metadata.");
        let expected: Vec<_> = kind
            .capabilities()
            .metadata
            .fields()
            .iter()
            .map(|field| CompletionExpectation::Literal(field.name().to_string()))
            .collect();
        assert_eq!(
            suggest_client_expectations(&input, input.len()),
            expected,
            "{kind}: {input}"
        );
        let input = format!("{prefix}TIMESTAMP AT metadata.");
        let mut expected = Vec::new();
        for field in kind.capabilities().metadata.fields() {
            if field.ty() == nervix_models::IngestorMetadataType::Datetime {
                expected.push(CompletionExpectation::Literal(field.name().to_string()));
            }
        }
        assert_eq!(
            suggest_client_expectations(&input, input.len()),
            expected,
            "{kind}: {input}"
        );
    }
}

#[test]
fn every_declared_emitter_completes_ordinary_functions_in_its_input_filter() {
    for kind in EmitterEntityKind::all() {
        let input = format!("CREATE {} EMITTER e FROM incoming WHERE co", kind.phrase());
        assert!(
            suggest_client_expectations(&input, input.len()).contains(
                &CompletionExpectation::Semantic(SemanticReference::BuiltinFunction(
                    BuiltinFunctionScope::Ordinary
                ))
            ),
            "{kind}: {input}"
        );
    }
}

#[test]
fn connector_expression_positions_keep_their_declared_function_scope() {
    let endpoint = BuiltinFunctionScope::IngestorEntity(IngestorEntityKind::Http(
        HttpIngestorEntity::Endpoint,
    ));
    let kafka = BuiltinFunctionScope::EmitterInvocation(EmitterEntityKind::Kafka(
        KafkaEmitterEntity::Topic,
    ));
    for (input, scope) in [
        (
            "CREATE JUNCTION j FROM incoming UNBRANCHED TO outgoing INVOKE co",
            BuiltinFunctionScope::Ordinary,
        ),
        (
            "CREATE HTTP ENDPOINT INGESTOR i FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE \
             1MiB DECODE USING codec FILTER WHERE read_",
            endpoint,
        ),
        (
            "CREATE HTTP ENDPOINT INGESTOR i FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE \
             1MiB DECODE USING codec FILTER WHERE coalesce(read_",
            endpoint,
        ),
        (
            "ALTER HTTP ENDPOINT INGESTOR i ADD ROUTE TO outgoing SET value = read_",
            endpoint,
        ),
        (
            "CREATE KAFKA TOPIC EMITTER e FROM incoming WHERE co",
            BuiltinFunctionScope::Ordinary,
        ),
        (
            "CREATE HTTP REQUEST EMITTER e FROM incoming TO client METHOD co",
            BuiltinFunctionScope::Ordinary,
        ),
        (
            "CREATE HTTP REQUEST EMITTER e FROM incoming TO client METHOD 'POST' PATH co",
            BuiltinFunctionScope::Ordinary,
        ),
        (
            "ALTER KAFKA TOPIC EMITTER e SET TO client TOPIC 'events' MODE NO ACK RETRY POLICY \
             BACKOFF 1s MAX 2s ENCODE USING codec INVOKE write_",
            kafka,
        ),
    ] {
        let expectations = suggest_client_expectations(input, input.len());
        assert!(
            expectations.contains(&CompletionExpectation::Semantic(
                SemanticReference::BuiltinFunction(scope)
            )),
            "{input}: {expectations:?}"
        );
    }
}

#[test]
fn connector_completion_does_not_escape_the_composed_grammar() {
    for input in [
        "CREATE HTTP ENDPOINT INGESTOR i FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB \
         DECODE USING codec TO outgoing FLUSH EACH co",
        "CREATE HTTP REQUEST EMITTER e FROM incoming TO client METHOD 'GET' PATH '/' MODE co",
        "CREATE HTTP REQUEST EMITTER e FROM incoming WHERE input.value IS co",
        "ALTER HTTP ENDPOINT INGESTOR i SET FROM read_",
    ] {
        let expectations = suggest_client_expectations(input, input.len());
        assert!(
            !expectations.iter().any(|e| matches!(
                e,
                CompletionExpectation::Semantic(SemanticReference::BuiltinFunction(_))
            )),
            "{input}: {expectations:?}"
        );
    }
}

#[test]
fn metadata_completion_reads_the_entity_declaration() {
    let input = "CREATE KAFKA TOPIC INGESTOR i FROM broker TOPIC 'events' OFFSET BY CONSUMER \
                 GROUP 'g' MODE NO ACK ON QUIESCE SUSPEND DECODE USING codec FILTER WHERE \
                 metadata.";
    let expectations = suggest_client_expectations(input, input.len());
    let kind = IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic);
    for field in kind.capabilities().metadata.fields() {
        assert!(
            expectations.contains(&CompletionExpectation::Literal(field.name().to_string())),
            "{}: {expectations:?}",
            field.name()
        );
    }
    assert!(!kind.capabilities().metadata.fields().is_empty());
}
