//! Canonical Plexus renders every generated current value to text that reads back as that value.
//!
//! Layer: test harness.
//!
//! - **Owns.** The Bolero properties over canonical rendering and reparsing: expressions as a
//!   statement embeds them, every Model family, every statement form including the session-only
//!   ones, text that is not canonical at all, and edited expression text that every entry point
//!   must read alike. Deterministic sweeps reach every family and form on every run.
//! - **Depends on.** The composed statement grammar, the vocabulary's canonical renderer and the
//!   generators of `nervix-arbitrary`.
//! - **Must not know.** Validation, persistence or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use chumsky::prelude::*;
use nervix_arbitrary::{Arbitrary, Domain, ModelVariant, StatementVariant};
use nervix_models::{
    CreateStatement, CreateSubscription, DeleteSubscription, DescribeBackup, EmitterEntityKind,
    Expression, IngestorEntityKind, InspectionFormat, Model, Statement,
    SubscriptionDeliveryBehavior,
};
use strum::IntoEnumIterator as _;

use crate::{
    client_statement::{ClientStatement, parse_client_statement, parse_client_statements},
    parse_expression, parse_expression_list, parse_route_construction,
    statement::parse_statement,
};

/// Asserts that `text` reads as `expression` through every entry point that builds an expression
/// from Plexus: a statement that embeds it, and the standalone readers of an expression, an
/// expression list and a route construction, which the web console's forms, `nervix-cli subscribe
/// --where` and the client library call.
fn assert_every_entry_point_reads(text: &str, expression: &Expression) {
    let statement = format!("CREATE SUBSCRIPTION literal TO events WHERE {text};");
    let embedded = match parse_client_statement(&statement) {
        Ok(ClientStatement::CreateSubscription(subscription)) => subscription.where_clause,
        Ok(other) => panic!("{statement}\nread as another statement: {other:?}"),
        Err(error) => panic!("{statement}\nmust reparse: {error:?}"),
    };
    assert_eq!(
        embedded.as_ref(),
        Some(expression),
        "{text} changed when a statement read it"
    );

    let standalone =
        parse_expression(text).unwrap_or_else(|error| panic!("{text} must reparse: {error:?}"));
    assert_eq!(
        &standalone, expression,
        "{text} changed when it was read alone"
    );

    let listed = parse_expression_list(text)
        .unwrap_or_else(|error| panic!("{text} must reparse as a list: {error:?}"));
    assert_eq!(
        listed,
        std::slice::from_ref(expression),
        "{text} changed when it was read as a list"
    );

    let route = format!("WHERE {text}");
    let construction = parse_route_construction(&route)
        .unwrap_or_else(|error| panic!("{route} must reparse: {error:?}"));
    assert_eq!(
        construction.where_clause.as_ref(),
        Some(expression),
        "{text} changed when a route construction read it"
    );
}

/// Asserts that every entry point that builds an expression reads `text` alike: a statement that
/// embeds it and the standalone readers of an expression, an expression list and a route
/// construction all read it as the same expression, or none of them reads it as one.
fn assert_every_entry_point_reads_alike(text: &str) {
    let alone = parse_expression(text).ok();

    // The line break ends a comment the text closes with, so the `;` still ends the statement.
    let statement = format!("CREATE SUBSCRIPTION literal TO events WHERE {text}\n;");
    let embedded = match parse_client_statement(&statement) {
        Ok(ClientStatement::CreateSubscription(subscription)) => subscription.where_clause,
        Ok(other) => panic!("{statement:?}\nread as another statement: {other:?}"),
        Err(_) => None,
    };
    assert_eq!(
        embedded, alone,
        "{text:?} read differently in a statement and alone"
    );

    // A list of one expression is that expression; a list of more is no single expression.
    let listed = match parse_expression_list(text) {
        Ok(mut list) if list.len() == 1 => list.pop(),
        Ok(_) | Err(_) => None,
    };
    assert_eq!(
        listed, alone,
        "{text:?} read differently as a list and alone"
    );

    let route = format!("WHERE {text}");
    let constructed = match parse_route_construction(&route) {
        Ok(construction)
            if construction.inherit.is_none()
                && construction.assignments.is_empty()
                && construction.invocations.is_empty() =>
        {
            construction.where_clause
        }
        Ok(_) | Err(_) => None,
    };
    assert_eq!(
        constructed, alone,
        "{text:?} read differently in a route construction and alone"
    );
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
        .to_canonical_plexus()
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
            let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
            let expression = arbitrary.expression();
            let rendered = nervix_models::expression_to_plexus(&expression)
                .unwrap_or_else(|error| panic!("{expression:?} must render: {error:?}"));
            assert_every_entry_point_reads(&rendered, &expression);
        });
}

#[test]
fn bolero_entity_presentations_keep_declared_rows_and_complete_configuration() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut statements = Vec::new();
            for kind in IngestorEntityKind::all() {
                let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
                let ingestor = arbitrary.create_ingestor_from(kind);
                let presentation = ingestor.input.entity_presentation();
                assert_eq!(presentation.phrase(), kind.phrase());
                assert_presentation_rows(&presentation, "from");
                statements.push(ClientStatement::Server(Statement::Create(
                    CreateStatement::new(Box::new(Model::Ingestor(ingestor)), false),
                )));
            }
            for kind in EmitterEntityKind::all() {
                let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
                let emitter = arbitrary.create_emitter_to(kind);
                let presentation = emitter
                    .connector
                    .entity_presentation()
                    .unwrap_or_else(|error| panic!("the generated sink renders: {error:#}"));
                assert_eq!(presentation.phrase(), kind.phrase());
                assert_presentation_rows(&presentation, "to");
                statements.push(ClientStatement::Server(Statement::Create(
                    CreateStatement::new(Box::new(Model::Emitter(emitter)), false),
                )));
            }
            assert_entity_round_trips(&statements);
        });
}

/// Reuses each production grammar across this input's entities. Building the complete grammars
/// for every entity dominated the bounded campaign without exercising another representation.
fn assert_entity_round_trips(statements: &[ClientStatement]) {
    let sources = statements
        .iter()
        .map(|statement| {
            let canonical = statement
                .to_canonical_plexus()
                .unwrap_or_else(|error| panic!("{statement:?} must render: {error:?}"));
            let lexed = crate::parser_support::lex_input(&canonical)
                .unwrap_or_else(|error| panic!("{canonical}\nmust lex: {error:?}"));
            (canonical, lexed)
        })
        .collect::<Vec<_>>();
    let client = crate::client_statement::client_statement_parser().then_ignore(end());
    let server = crate::statement::statement_parser().then_ignore(end());
    for (statement, (canonical, lexed)) in statements.iter().zip(&sources) {
        let reparsed = client
            .parse(lexed.tokens.as_slice())
            .into_result()
            .unwrap_or_else(|error| panic!("{canonical}\nmust reparse: {error:?}"));
        assert_eq!(
            &reparsed, statement,
            "{canonical}\nchanged when it was reparsed"
        );
        let ClientStatement::Server(expected) = statement else {
            panic!("entity creation must be a server statement: {statement:?}");
        };
        let served = server
            .parse(lexed.tokens.as_slice())
            .into_result()
            .unwrap_or_else(|error| panic!("{canonical}\nmust reparse on a server: {error:?}"));
        assert_eq!(
            &served, expected,
            "{canonical}\nchanged when a server reparsed it"
        );
    }
}

fn assert_presentation_rows(presentation: &nervix_models::EntityPresentation, reference: &str) {
    let rows = presentation.describe_lines();
    let keys = rows
        .iter()
        .map(|row| {
            row.split_once(':')
                .unwrap_or_else(|| panic!("inspection row needs a label: {row}"))
                .0
        })
        .collect::<Vec<_>>();
    assert_eq!(keys, [reference, "options", "mode", "quiesce", "envelope"]);
    assert!(rows[0].starts_with(&format!("{reference}: {} ", presentation.phrase())));
}

#[test]
fn bolero_model_roundtrip_canonical_plexus() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
            let model = arbitrary.model();
            let if_not_exists = arbitrary.entropy().flag();
            let statement = Statement::Create(CreateStatement::new(Box::new(model), if_not_exists));
            assert_statement_round_trips(&ClientStatement::Server(statement));
        });
}

#[test]
fn postgres_update_generation_keeps_a_mapped_column_to_update() {
    let mut checked = 0;
    for seed in 0..=u8::MAX {
        let bytes = (0..512_u16)
            .map(|index| {
                index.to_le_bytes()[0]
                    .wrapping_mul(31)
                    .wrapping_add(seed.wrapping_mul(17))
            })
            .collect::<Vec<_>>();
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Plexus);
        let emitter = arbitrary.create_emitter_to(nervix_models::EmitterEntityKind::Postgres(
            nervix_models::PostgresEmitterEntity::Table,
        ));
        let nervix_models::EmitterEntity::Postgres(nervix_models::PostgresEmitter::Table(
            destination,
        )) = emitter.connector.as_ref()
        else {
            panic!("current Postgres destination");
        };
        let nervix_models::PostgresConflictAction::DoUpdate { target } =
            &destination.conflict_action
        else {
            continue;
        };
        checked += 1;
        assert!(!target.is_empty());
        assert!(
            destination
                .values
                .iter()
                .any(|mapping| !target.contains(&mapping.column))
        );
        assert_statement_round_trips(&ClientStatement::Server(Statement::Create(
            nervix_models::CreateStatement::new(Box::new(Model::Emitter(emitter)), false),
        )));
    }
    assert!(
        checked >= 2,
        "the current generator reaches Postgres updates"
    );
}

#[test]
fn statement_corpus_reaches_resumed_restore() {
    let bytes = include_bytes!(
        "__fuzz__/canonical_round_trip_tests__bolero_statement_roundtrip_canonical_plexus/corpus/\
         resume"
    );
    let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
    let ClientStatement::Server(Statement::Restore(restore)) = client_statement(&mut arbitrary)
    else {
        panic!("the resume corpus entry generates a current restore statement");
    };
    assert_eq!(restore.lifecycle, nervix_models::RestoreLifecycle::Resume);
}

#[test]
fn statement_corpus_reaches_client_inspection() {
    let bytes = include_bytes!(
        "__fuzz__/canonical_round_trip_tests__bolero_statement_roundtrip_canonical_plexus/corpus/\
         describe-client"
    );
    let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
    let statement = client_statement(&mut arbitrary);
    assert!(matches!(
        statement,
        ClientStatement::Server(Statement::DescribeClient(_))
    ));
    assert_statement_round_trips(&statement);
}

#[test]
fn bolero_statement_roundtrip_canonical_plexus() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
            let statement = client_statement(&mut arbitrary);
            assert_statement_round_trips(&statement);
        });
}

#[test]
fn every_model_family_round_trips_canonically() {
    for variant in ModelVariant::ALL {
        for seed in 0..16 {
            let bytes = sweep_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Plexus);
            let model = arbitrary.model_of(variant);
            assert_eq!(ModelVariant::of(&model), variant);
            let statement = Statement::Create(CreateStatement::new(Box::new(model), false));
            assert_statement_round_trips(&ClientStatement::Server(statement));
        }
    }
}

#[test]
fn every_emitter_connector_round_trips_canonically() {
    for kind in EmitterEntityKind::all() {
        for seed in 0..16 {
            let bytes = sweep_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Plexus);
            let emitter = arbitrary.create_emitter_to(kind);
            assert_eq!(emitter.connector.kind(), kind);
            let model = Model::Emitter(emitter);
            let statement = Statement::Create(CreateStatement::new(Box::new(model), false));
            assert_statement_round_trips(&ClientStatement::Server(statement));
        }
    }
}

#[test]
fn every_ingestor_source_round_trips_canonically() {
    for kind in IngestorEntityKind::all() {
        for seed in 0..16 {
            let bytes = sweep_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Plexus);
            let ingestor = arbitrary.create_ingestor_from(kind);
            assert_eq!(ingestor.input.entity_kind(), kind);
            let model = Model::Ingestor(ingestor);
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
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Plexus);
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

/// Every region that ends at a keyword a builtin or a field scope also spells reads the call or the
/// field back as itself, whether it was written in parentheses or not: canonical Plexus writes it
/// without them. A clause that goes straight on with an expression still begins at its keyword
/// when that expression opens with a parenthesis.
#[test]
fn calls_and_scopes_named_like_clause_keywords_round_trip_in_every_region() {
    for source in [
        "CREATE REORDERER peaks_in_order FROM sensors WHERE max(input.readings) > 0 BY \
         (input.first + input.second) * 2 MAX TIME 10s UNBRANCHED TO ordered_readings INHERIT ALL \
         FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        "CREATE HTTP REQUEST EMITTER send FROM source WHERE output.total > 0 TO api METHOD \
         input.method PATH (input.prefix + input.tenant) * 2 MODE ACK RETRY POLICY BACKOFF 250ms \
         MAX 30s WITHOUT BODY FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
        "CREATE JUNCTION peaks FROM sensors WHERE (max(input.readings) > 10) FILTER WHERE \
         (output.total > 0) UNBRANCHED TO alerts INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        "CREATE DEDUPLICATOR distinct_peaks FROM sensors FILTER WHERE max(input.readings) > 0 \
         DEDUPLICATE ON max(input.readings), input.id MAX TIME 10m UNBRANCHED TO \
         distinct_readings INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        "CREATE REORDERER peaks_in_order FROM sensors BY (max(input.readings)) MAX TIME 10s \
         UNBRANCHED TO ordered_readings INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        "CREATE CORRELATOR suffix_matches LEFT FROM sensors WHERE (right(left.name, 2) = \
         right.suffix) RIGHT FROM labels WHERE max(right.readings) > output.id CORRELATE WHERE \
         left.id = right.id MATCH EARLIEST MAX TIME 5s ON CORRELATION TIMEOUT DROP, DROP \
         UNBRANCHED TO matched SET id = left.id FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        "ALTER JUNCTION peaks SET FILTER WHERE concat(input.name, replace(input.name, 'a', 'b')) \
         != '', ADD FROM labels WHERE max(input.readings) > output.total, SET DETACHED;",
        "ALTER DEDUPLICATOR distinct_peaks SET DEDUPLICATE ON input.id, replace(input.name, 'a', \
         'b'), SET MAX TIME 20m;",
    ] {
        let statement = parse_client_statement(source)
            .unwrap_or_else(|error| panic!("{source}\nmust parse: {error:?}"));
        assert_statement_round_trips(&statement);
    }
}

/// Pieces an edit of expression text inserts or substitutes: the whitespace and comments between
/// tokens, the `.` of a float with and without spaces beside it, exponent letters, digits and a
/// minus, the punctuation of statements that an expression never reads, and keywords only an
/// expression reserves beside ones it reads as names.
const EXPRESSION_EDIT_PIECES: [&str; 36] = [
    " ",
    "\n",
    "\t",
    "// note\n",
    "//",
    ".",
    " .",
    ". ",
    " . ",
    "e",
    "E",
    "e5",
    "E-3",
    "0",
    "9",
    "-",
    "+",
    ":",
    "::",
    "{",
    "}",
    "(",
    ")",
    "[",
    "]",
    ",",
    ";",
    "'",
    "\"",
    "$",
    " IS ",
    " end ",
    " between ",
    " TRY_CAST ",
    " max ",
    " first ",
];

/// Canonical expression text with a few pieces inserted, characters deleted or characters replaced
/// by pieces, so it is sometimes still an expression and often reads differently or not at all.
fn edited_expression_text(arbitrary: &mut Arbitrary<'_>, text: &str) -> String {
    let mut characters = text.chars().collect::<Vec<_>>();
    let edits = arbitrary.entropy().boundary_biased(1..=4);
    for _ in 0..edits {
        let length = u64::try_from(characters.len()).expect("a length fits in u64");
        let at =
            usize::try_from(arbitrary.entropy().up_to(length)).expect("the draw ends at a length");
        let piece = arbitrary.entropy().pick(EXPRESSION_EDIT_PIECES);
        match arbitrary.entropy().byte() % 3 {
            0 if at < characters.len() => {
                characters.remove(at);
            }
            1 => {
                characters.splice(at..at, piece.chars());
            }
            _ if at < characters.len() => {
                characters.splice(at..=at, piece.chars());
            }
            _ => characters.extend(piece.chars()),
        }
    }
    characters.into_iter().collect()
}

/// Characters an edit inserts or substitutes: the delimiters, quotes and operators the grammar
/// reads, digits, and characters outside ASCII.
const EDIT_CHARACTERS: [char; 20] = [
    ' ', '\n', ';', ',', '(', ')', '[', ']', '{', '}', '\'', '"', '$', '.', '=', '-', '0', '9',
    'a', 'é',
];

/// Canonical text with a few characters deleted, inserted or replaced, so it is mostly no longer
/// Plexus and sometimes still is.
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
fn bolero_expression_text_reads_alike_at_every_entry_point() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
            let expression = arbitrary.expression();
            let canonical = nervix_models::expression_to_plexus(&expression)
                .unwrap_or_else(|error| panic!("{expression:?} must render: {error:?}"));
            let text = edited_expression_text(&mut arbitrary, &canonical);
            assert_every_entry_point_reads_alike(&text);
        });
}

#[test]
fn bolero_statement_text_is_canonical_or_rejected() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
            let statement = client_statement(&mut arbitrary);
            let canonical = statement
                .to_canonical_plexus()
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
