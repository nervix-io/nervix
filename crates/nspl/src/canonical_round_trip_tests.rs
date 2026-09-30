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
    parse_expression, parse_expression_list, parse_route_construction,
    statement::parse_statement,
};

/// Asserts that `text` reads as `expression` through every entry point that builds an expression
/// from NSPL: a statement that embeds it, and the standalone readers of an expression, an
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
            assert_every_entry_point_reads(&rendered, &expression);
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

/// Every region that ends at a keyword a builtin or a field scope also spells reads the call or the
/// field back as itself, whether it was written in parentheses or not: canonical NSPL writes it
/// without them. A clause that goes straight on with an expression still begins at its keyword
/// when that expression opens with a parenthesis.
#[test]
fn calls_and_scopes_named_like_clause_keywords_round_trip_in_every_region() {
    for source in [
        "CREATE REORDERER peaks_in_order FROM sensors WHERE max(input.readings) > 0 BY \
         (input.first + input.second) * 2 MAX TIME 10s UNBRANCHED TO ordered_readings INHERIT ALL \
         FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        "CREATE EMITTER send FROM source WHERE output.total > 0 TO HTTP api METHOD input.method \
         PATH (input.prefix + input.tenant) * 2 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s \
         WITHOUT BODY FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
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
