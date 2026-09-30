//! Canonical NSPL renders every generated current value to text that reads back as that value.
//!
//! Layer: test harness.
//!
//! - **Owns.** The Bolero properties over canonical rendering and reparsing: expressions as a
//!   statement embeds them, every Model family, every statement form including the session-only
//!   ones, and text that is not canonical at all. Deterministic sweeps reach every family and form
//!   on every run.
//! - **Depends on.** The composed statement grammar, the vocabulary's canonical renderer and the
//!   generators of `nervix-arbitrary`.
//! - **Must not know.** Validation, persistence or runtime execution.

use nervix_arbitrary::{Arbitrary, Domain, ModelVariant, SinkVariant, StatementVariant};
use nervix_models::{
    CreateStatement, CreateSubscription, DeleteSubscription, DescribeBackup, Expression,
    InspectionFormat, Model, Statement, SubscriptionDeliveryBehavior,
};
use strum::IntoEnumIterator as _;

use crate::{
    client_statement::{ClientStatement, parse_client_statement, parse_client_statements},
    parse_expression,
    parser_support::render_expression_tokens,
    schema::ParseFromSourceError,
    statement::parse_statement,
};

/// Reads expression text the way a statement reads an expression it embeds: the statement lexer
/// tokenizes it, taking every string literal verbatim, and the expression grammar parses those
/// tokens as the statement hands them over.
fn read_embedded_expression(text: &str) -> error_stack::Result<Expression, ParseFromSourceError> {
    let spanned = crate::lex(text).unwrap_or_else(|errors| panic!("{text} must lex: {errors:?}"));
    let tokens = spanned
        .into_iter()
        .map(|spanned| spanned.token)
        .collect::<Vec<_>>();
    parse_expression(&render_expression_tokens(&tokens))
}

/// The session-only statement forms, which the client grammar reads beside every server statement.
const SESSION_FORMS: u64 = 10;

/// A statement of the client grammar: a server statement of any form, or a session-only one.
fn client_statement(arbitrary: &mut Arbitrary<'_>) -> ClientStatement {
    let choice = arbitrary.entropy().up_to(SESSION_FORMS * 3);
    match choice {
        0 => ClientStatement::UseDomain(arbitrary.name()),
        1 => ClientStatement::ListDomains,
        2 => ClientStatement::AttachDomainClock,
        3 => ClientStatement::DetachDomainClock,
        4 => ClientStatement::BeginTransaction,
        5 => ClientStatement::CommitTransaction,
        6 => ClientStatement::RevertTransaction,
        7 => ClientStatement::DescribeBackup(DescribeBackup {
            source: arbitrary.non_empty_string(),
            format: arbitrary
                .entropy()
                .pick([InspectionFormat::Text, InspectionFormat::Json]),
        }),
        8 => {
            let name = arbitrary.name();
            let relay = arbitrary.name();
            let delivery_behavior = arbitrary.entropy().pick([
                SubscriptionDeliveryBehavior::Blocking,
                SubscriptionDeliveryBehavior::Dropping,
            ]);
            let batch_sample_rate = if arbitrary.entropy().flag() {
                Some(
                    arbitrary
                        .entropy()
                        .pick(["0", "1", "0.5", "0.125", "1.0", "0.0", "1e-3", "0.9999"])
                        .to_string(),
                )
            } else {
                None
            };
            ClientStatement::CreateSubscription(CreateSubscription {
                name,
                relay,
                delivery_behavior,
                batch_sample_rate,
                where_clause: arbitrary.optional_expression(),
            })
        }
        9 => ClientStatement::DeleteSubscription(DeleteSubscription {
            name: arbitrary.name(),
        }),
        _ => match arbitrary.statement() {
            Statement::UploadResource(upload) => ClientStatement::UploadResource(upload),
            server => ClientStatement::Server(server),
        },
    }
}

/// Renders `statement` canonically and reads it back through both grammars that read it.
fn assert_statement_round_trips(statement: &ClientStatement) {
    let canonical = statement
        .to_canonical_nspl()
        .unwrap_or_else(|error| panic!("{statement:?} must render: {error:?}"));
    let reparsed = parse_client_statement(&canonical)
        .unwrap_or_else(|error| panic!("{canonical}\nmust reparse: {error:?}"));
    assert_eq!(
        &reparsed, statement,
        "{canonical}\nchanged when it was reparsed"
    );
    // The server reads its own statements with the server grammar, which has no session-only
    // forms and leaves a backup, a restore and an upload to the client.
    if let ClientStatement::Server(server) = statement
        && !matches!(server, Statement::Backup(_) | Statement::Restore(_))
    {
        let served = parse_statement(&canonical)
            .unwrap_or_else(|error| panic!("{canonical}\nmust reparse on a server: {error:?}"));
        assert_eq!(
            &served, server,
            "{canonical}\nchanged when a server reparsed it"
        );
    }
}

/// Deterministic bytes for a sweep, so every family is reached on every run whatever the random
/// cases happen to draw.
fn sweep_bytes(seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_add(1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut bytes = Vec::with_capacity(1024);
    for _ in 0..1024 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.push(state.to_le_bytes()[3]);
    }
    bytes
}

#[test]
fn bolero_expression_roundtrip_minimal_parentheses() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Nspl);
            let expression = arbitrary.expression();
            let rendered = nervix_models::expression_to_nspl(&expression)
                .unwrap_or_else(|error| panic!("{expression:?} must render: {error:?}"));
            let reparsed = read_embedded_expression(&rendered)
                .unwrap_or_else(|error| panic!("{rendered} must reparse: {error:?}"));
            assert_eq!(
                expression, reparsed,
                "{rendered} changed when it was reparsed"
            );
        });
}

#[test]
fn bolero_model_roundtrip_canonical_nspl() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Nspl);
            let model = arbitrary.model();
            let if_not_exists = arbitrary.entropy().flag();
            let statement = Statement::Create(CreateStatement::new(Box::new(model), if_not_exists));
            assert_statement_round_trips(&ClientStatement::Server(statement));
        });
}

#[test]
fn bolero_statement_roundtrip_canonical_nspl() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Nspl);
            let statement = client_statement(&mut arbitrary);
            assert_statement_round_trips(&statement);
        });
}

#[test]
fn every_model_family_round_trips_canonically() {
    for variant in ModelVariant::ALL {
        for seed in 0..16 {
            let bytes = sweep_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Nspl);
            let model = arbitrary.model_of(variant);
            assert_eq!(ModelVariant::of(&model), variant);
            let statement = Statement::Create(CreateStatement::new(Box::new(model), false));
            assert_statement_round_trips(&ClientStatement::Server(statement));
        }
    }
}

#[test]
fn every_emitter_sink_round_trips_canonically() {
    for variant in SinkVariant::ALL {
        for seed in 0..16 {
            let bytes = sweep_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Nspl);
            let emitter = arbitrary.create_emitter_to(variant);
            assert_eq!(SinkVariant::of(&emitter.sink), variant);
            let model = Model::Emitter(emitter);
            let statement = Statement::Create(CreateStatement::new(Box::new(model), false));
            assert_statement_round_trips(&ClientStatement::Server(statement));
        }
    }
}

#[test]
fn every_statement_form_round_trips_canonically() {
    for variant in StatementVariant::iter() {
        for seed in 0..16 {
            let bytes = sweep_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Nspl);
            let statement = arbitrary.statement_of(variant);
            assert_eq!(StatementVariant::of(&statement), variant);
            let statement = match statement {
                Statement::UploadResource(upload) => ClientStatement::UploadResource(upload),
                server => ClientStatement::Server(server),
            };
            assert_statement_round_trips(&statement);
        }
    }
}

/// Characters an edit inserts or substitutes: the delimiters, quotes and operators the grammar
/// reads, digits, and characters outside ASCII.
const EDIT_CHARACTERS: [char; 20] = [
    ' ', '\n', ';', ',', '(', ')', '[', ']', '{', '}', '\'', '"', '$', '.', '=', '-', '0', '9',
    'a', 'é',
];

/// Canonical text with a few characters deleted, inserted or replaced, so it is mostly no longer
/// NSPL and sometimes still is.
fn edited_text(arbitrary: &mut Arbitrary<'_>, text: &str) -> String {
    let mut characters = text.chars().collect::<Vec<_>>();
    let edits = arbitrary.entropy().boundary_biased(1..=3);
    for _ in 0..edits {
        let length = u64::try_from(characters.len()).expect("a length fits in u64");
        let at =
            usize::try_from(arbitrary.entropy().up_to(length)).expect("the draw ends at a length");
        match arbitrary.entropy().byte() % 3 {
            0 if at < characters.len() => {
                characters.remove(at);
            }
            1 => characters.insert(at, arbitrary.entropy().pick(EDIT_CHARACTERS)),
            _ if at < characters.len() => {
                characters[at] = arbitrary.entropy().pick(EDIT_CHARACTERS);
            }
            _ => characters.push(arbitrary.entropy().pick(EDIT_CHARACTERS)),
        }
    }
    characters.into_iter().collect()
}

#[test]
fn bolero_statement_text_is_canonical_or_rejected() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Nspl);
            let statement = client_statement(&mut arbitrary);
            let canonical = statement
                .to_canonical_nspl()
                .unwrap_or_else(|error| panic!("{statement:?} must render: {error:?}"));
            let text = edited_text(&mut arbitrary, &canonical);
            match parse_client_statements(&text) {
                Ok(statements) => {
                    for accepted in statements {
                        assert_statement_round_trips(&accepted);
                    }
                }
                Err(report) => {
                    let rejection = report.current_context();
                    assert!(
                        !rejection.diagnostics().is_empty(),
                        "{text:?} was rejected without a diagnostic"
                    );
                    for diagnostic in rejection.diagnostics() {
                        assert!(
                            diagnostic.span.start <= diagnostic.span.end
                                && diagnostic.span.end <= text.len(),
                            "{text:?} was rejected at {:?}, outside the text",
                            diagnostic.span
                        );
                    }
                }
            }
        });
}
