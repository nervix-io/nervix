//! `BACKUP`, `RESTORE` and `DESCRIBE BACKUP` grammar.
//!
//! Layer: language.
//! - **Owns.** Parsing a backup of the cluster or of one domain into a local archive file, a
//!   restore of the cluster or of one domain from a local archive file, and a description of a
//!   local archive file, each with its optional clauses in their fixed order.
//! - **Depends on.** Shared NSPL tokens, the shared keyword, domain name and reference, local path
//!   and inspection format primitives, and vocabulary Models.
//! - **Must not know.** Sessions, how an archive is assembled, transferred, read or restored, or
//!   where a client writes or reads it.

use chumsky::prelude::*;
use nervix_models::{
    Backup, BackupCapture, BackupResources, BackupScope, DescribeBackup, ExistingUserPolicy,
    InspectionFormat, Restore, RestoreMode, RestoreScope, RestoreState,
};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        ParseError, domain_name, domain_ref, duration_lit, inspection_format, kw, kw_phrase2,
        kw_phrase3, local_path, tok,
    },
};

/// `BACKUP CLUSTER TO '<file>' [WITHOUT RESOURCES]` or
/// `BACKUP DOMAIN [<domain>] TO '<file>' [WITHOUT RESOURCES]`.
///
/// Omitting the domain backs up the domain the session has selected. A domain is only read where
/// `TO` follows it, so `BACKUP DOMAIN TO '<file>'` names no domain rather than one called `to`.
pub fn backup_parser<'src>()
-> impl Parser<'src, &'src [Token], Backup, extra::Err<ParseError<'src>>> + Clone {
    let named_domain = domain_ref().then_ignore(kw(Identifier::To).rewind());
    let scope = choice((
        kw(Identifier::Cluster).to(BackupScope::Cluster),
        kw(Identifier::Domain)
            .ignore_then(named_domain.or_not())
            .map(BackupScope::Domain),
    ));
    let resources = kw_phrase2(Identifier::Without, Identifier::Resources)
        .or_not()
        .map(|without| match without {
            Some(()) => BackupResources::Omitted,
            None => BackupResources::Included,
        });
    let timeout = kw(Identifier::Timeout)
        .ignore_then(duration_lit())
        .try_map(|raw, span| {
            nervix_models::parse_duration_text(&raw)
                .map(|timeout| BackupCapture::Quiesced {
                    timeout: Some(timeout),
                })
                .map_err(|report| {
                    Rich::custom(
                        span,
                        format!("invalid backup timeout: {}", report.current_context()),
                    )
                })
        });
    let capture = choice((
        kw_phrase2(Identifier::Without, Identifier::State).to(BackupCapture::ConfigurationOnly),
        kw_phrase2(Identifier::Without, Identifier::Pause).to(BackupCapture::Live),
        timeout,
    ))
    .or_not()
    .map(Option::unwrap_or_default);
    kw(Identifier::Backup)
        .ignore_then(scope)
        .then_ignore(kw(Identifier::To))
        .then(local_path())
        .then(resources)
        .then(capture)
        .map(|(((scope, destination), resources), capture)| Backup {
            scope,
            destination,
            resources,
            capture,
        })
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

/// `RESTORE CLUSTER FROM '<file>' [ON EXISTING USER FAIL | SKIP | REPLACE] [DRY RUN]` or
/// `RESTORE DOMAIN <name> [AS <new_name>] FROM '<file>' [DRY RUN]`.
///
/// Omitting the user policy refuses a restore that meets an existing user. The domain names a
/// domain of the archive, which need not exist in the cluster, so it is a name rather than a
/// reference to an existing domain.
pub fn restore_parser<'src>()
-> impl Parser<'src, &'src [Token], Restore, extra::Err<ParseError<'src>>> + Clone {
    let dry_run =
        kw_phrase2(Identifier::Dry, Identifier::Run)
            .or_not()
            .map(|dry_run| match dry_run {
                Some(()) => RestoreMode::DryRun,
                None => RestoreMode::Apply,
            });
    let state = choice((
        kw_phrase2(Identifier::Without, Identifier::State).to(RestoreState::ConfigurationOnly),
        kw_phrase3(Identifier::Without, Identifier::Source, Identifier::Offsets)
            .to(RestoreState::WithoutSourceOffsets),
    ))
    .or_not()
    .map(Option::unwrap_or_default);
    let existing_users = kw_phrase3(Identifier::On, Identifier::Existing, Identifier::User)
        .ignore_then(choice((
            kw(Identifier::Fail).to(ExistingUserPolicy::Fail),
            kw(Identifier::Skip).to(ExistingUserPolicy::Skip),
            kw(Identifier::Replace).to(ExistingUserPolicy::Replace),
        )))
        .or_not()
        .map(Option::unwrap_or_default);
    let cluster = kw(Identifier::Cluster)
        .ignore_then(kw(Identifier::From))
        .ignore_then(local_path())
        .then(existing_users)
        .then(dry_run.clone())
        .then(state.clone())
        .map(|(((source, existing_users), mode), state)| Restore {
            scope: RestoreScope::Cluster { existing_users },
            source,
            mode,
            state,
        });
    let domain = kw(Identifier::Domain)
        .ignore_then(domain_name())
        .then(kw(Identifier::As).ignore_then(domain_name()).or_not())
        .then_ignore(kw(Identifier::From))
        .then(local_path())
        .then(dry_run)
        .then(state)
        .map(|((((domain, target), source), mode), state)| Restore {
            scope: RestoreScope::Domain { domain, target },
            source,
            mode,
            state,
        });
    kw(Identifier::Restore)
        .ignore_then(choice((cluster, domain)))
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

/// `DESCRIBE BACKUP '<file>' [FORMAT TEXT | JSON]`. Omitting the format renders `TEXT`.
pub fn describe_backup_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeBackup, extra::Err<ParseError<'src>>> + Clone {
    let format = kw(Identifier::Format)
        .ignore_then(inspection_format())
        .or_not();
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Backup))
        .ignore_then(local_path())
        .then(format)
        .map(|(source, format)| DescribeBackup {
            source,
            format: format.unwrap_or_default(),
        })
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

/// Whether `tokens` wrote the keyword `keyword`.
///
/// A default written out parses to the same Model as one left out, so whether an optional clause
/// was written is read from the tokens its statement was parsed from.
fn writes_keyword(tokens: &[Token], keyword: Identifier) -> bool {
    tokens.iter().any(|token| {
        matches!(
            token,
            Token::Word(Word::KnownWord { iden, .. }) if *iden == keyword
        )
    })
}

/// The clauses that may still follow a complete `BACKUP`, as completion offers them.
pub(crate) fn backup_tail(tokens: &[Token]) -> Vec<String> {
    if writes_keyword(tokens, Identifier::State)
        || writes_keyword(tokens, Identifier::Pause)
        || writes_keyword(tokens, Identifier::Timeout)
    {
        return vec![";".to_string()];
    }
    let mut tail = vec![
        ";".to_string(),
        "WITHOUT STATE".to_string(),
        "WITHOUT PAUSE".to_string(),
        "TIMEOUT".to_string(),
    ];
    if !writes_keyword(tokens, Identifier::Resources) {
        tail.push("WITHOUT RESOURCES".to_string());
    }
    tail.sort();
    tail
}

/// The clauses that may still follow a complete `RESTORE`, as completion offers them.
pub(crate) fn restore_tail(restore: &Restore, tokens: &[Token]) -> Vec<String> {
    if writes_keyword(tokens, Identifier::State) || writes_keyword(tokens, Identifier::Offsets) {
        return vec![";".to_string()];
    }
    let mut tail = vec![
        ";".to_string(),
        "WITHOUT STATE".to_string(),
        "WITHOUT SOURCE OFFSETS".to_string(),
    ];
    if !writes_keyword(tokens, Identifier::Dry) {
        match &restore.scope {
            RestoreScope::Cluster { .. } if !writes_keyword(tokens, Identifier::Existing) => {
                tail.push("ON EXISTING USER".to_string());
            }
            RestoreScope::Cluster { .. } | RestoreScope::Domain { .. } => {}
        }
        tail.push("DRY RUN".to_string());
    }
    tail.sort();
    tail
}

/// The clauses that may still follow a complete `DESCRIBE BACKUP`, as completion offers them.
pub(crate) fn describe_backup_tail(describe: &DescribeBackup, tokens: &[Token]) -> Vec<String> {
    if writes_keyword(tokens, Identifier::Format) || describe.format != InspectionFormat::Text {
        return vec![";".to_string()];
    }
    vec![";".to_string(), "FORMAT".to_string()]
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_arbitrary::{Arbitrary, Domain};
    use nervix_models::{DomainName, Statement};
    use rstest::rstest;

    use super::*;
    use crate::{
        client_statement::{ClientStatement, parse_client_statement, suggest_client_statement},
        statement::{parse_statement, suggest_statement},
    };

    fn domain(name: &str) -> DomainName {
        DomainName::parse(name).assured("the test domain is an identifier-shaped literal")
    }

    fn backup(scope: BackupScope, destination: &str, resources: BackupResources) -> Statement {
        Statement::Backup(Backup {
            scope,
            destination: destination.to_string(),
            resources,
            capture: BackupCapture::default(),
        })
    }

    #[rstest]
    #[case::cluster(
        "BACKUP CLUSTER TO '/tmp/cluster.nvxb';",
        backup(BackupScope::Cluster, "/tmp/cluster.nvxb", BackupResources::Included)
    )]
    #[case::cluster_without_terminator(
        "BACKUP CLUSTER TO 'cluster.nvxb'",
        backup(BackupScope::Cluster, "cluster.nvxb", BackupResources::Included)
    )]
    #[case::cluster_without_resources(
        "BACKUP CLUSTER TO '/tmp/cluster.nvxb' WITHOUT RESOURCES;",
        backup(BackupScope::Cluster, "/tmp/cluster.nvxb", BackupResources::Omitted)
    )]
    #[case::named_domain(
        "BACKUP DOMAIN prod TO \"/var/backups/prod's.nvxb\";",
        backup(
            BackupScope::Domain(Some(domain("prod"))),
            "/var/backups/prod's.nvxb",
            BackupResources::Included
        )
    )]
    #[case::session_domain(
        "BACKUP DOMAIN TO 'domain.nvxb' WITHOUT RESOURCES;",
        backup(BackupScope::Domain(None), "domain.nvxb", BackupResources::Omitted)
    )]
    #[case::domain_named_like_a_keyword(
        "BACKUP DOMAIN to TO 'to.nvxb';",
        backup(
            BackupScope::Domain(Some(domain("to"))),
            "to.nvxb",
            BackupResources::Included
        )
    )]
    #[case::lowercase_keywords(
        "backup domain Prod to '~/prod.nvxb' without resources",
        backup(
            BackupScope::Domain(Some(domain("prod"))),
            "~/prod.nvxb",
            BackupResources::Omitted
        )
    )]
    #[case::spread_over_lines(
        "BACKUP\n  CLUSTER\n  TO '/tmp/c.nvxb'\n  WITHOUT\n  RESOURCES;",
        backup(BackupScope::Cluster, "/tmp/c.nvxb", BackupResources::Omitted)
    )]
    fn parses_every_backup_form(#[case] source: &str, #[case] expected: Statement) {
        let client = parse_client_statement(source).expect("the client grammar reads BACKUP");
        assert_eq!(client, ClientStatement::Server(expected));
        assert!(client.requires_local_handling());
    }

    #[rstest]
    #[case::configuration_only(
        "BACKUP DOMAIN prod TO 'a.nvxb' WITHOUT STATE;",
        BackupCapture::ConfigurationOnly
    )]
    #[case::live("BACKUP DOMAIN prod TO 'a.nvxb' WITHOUT PAUSE;", BackupCapture::Live)]
    #[case::quiesced_timeout("BACKUP DOMAIN prod TO 'a.nvxb' TIMEOUT 5s;", BackupCapture::Quiesced { timeout: Some(std::time::Duration::from_secs(5)) })]
    fn backup_capture_options_are_semantic(#[case] source: &str, #[case] capture: BackupCapture) {
        let parsed = parse_client_statement(source).expect("the capture option parses");
        let ClientStatement::Server(Statement::Backup(backup)) = parsed else {
            panic!("the statement is a backup");
        };
        assert_eq!(backup.capture, capture);
    }

    /// A backup writes a file on the client's machine, so only the client grammar reads it; the
    /// server receives it as the command a client sends.
    #[test]
    fn the_server_grammar_does_not_read_a_backup() {
        assert!(parse_statement("BACKUP CLUSTER TO '/tmp/c.nvxb';").is_err());
    }

    #[rstest]
    #[case::no_scope("BACKUP TO '/tmp/c.nvxb';")]
    #[case::no_destination("BACKUP CLUSTER;")]
    #[case::no_to("BACKUP CLUSTER '/tmp/c.nvxb';")]
    #[case::unquoted_destination("BACKUP CLUSTER TO /tmp/c.nvxb;")]
    #[case::empty_destination("BACKUP CLUSTER TO '';")]
    #[case::cluster_with_domain("BACKUP CLUSTER prod TO '/tmp/c.nvxb';")]
    #[case::two_domains("BACKUP DOMAIN a b TO '/tmp/c.nvxb';")]
    #[case::without_alone("BACKUP CLUSTER TO '/tmp/c.nvxb' WITHOUT;")]
    #[case::repeated_option("BACKUP CLUSTER TO 'c' WITHOUT RESOURCES WITHOUT RESOURCES;")]
    #[case::conflicting_capture("BACKUP CLUSTER TO 'c' WITHOUT PAUSE WITHOUT STATE;")]
    #[case::timeout_without_value("BACKUP CLUSTER TO 'c' TIMEOUT;")]
    #[case::trailing_word("BACKUP CLUSTER TO '/tmp/c.nvxb' NOW;")]
    fn rejects_malformed_backups(#[case] source: &str) {
        assert!(
            parse_client_statement(source).is_err(),
            "{source:?} must be rejected by the client grammar"
        );
    }

    #[test]
    fn an_empty_destination_says_why() {
        let error =
            parse_client_statement("BACKUP CLUSTER TO '';").expect_err("an empty path is no file");
        assert!(
            error
                .current_context()
                .diagnostics()
                .iter()
                .any(|diagnostic| diagnostic.message.contains("local_path must not be empty")),
            "{error:?}"
        );
    }

    #[rstest]
    #[case::cluster("BACKUP CLUSTER TO '/tmp/c.nvxb';")]
    #[case::without_resources("BACKUP CLUSTER TO '/tmp/c.nvxb' WITHOUT RESOURCES;")]
    #[case::without_state("BACKUP CLUSTER TO '/tmp/c.nvxb' WITHOUT STATE;")]
    #[case::without_pause("BACKUP CLUSTER TO '/tmp/c.nvxb' WITHOUT PAUSE;")]
    #[case::timeout("BACKUP CLUSTER TO '/tmp/c.nvxb' TIMEOUT 5s;")]
    #[case::fractional_second_timeout("BACKUP CLUSTER TO '/tmp/c.nvxb' TIMEOUT 1500ms;")]
    #[case::mixed_hour_timeout("BACKUP CLUSTER TO '/tmp/c.nvxb' TIMEOUT 90m;")]
    #[case::nanosecond_timeout_boundary(
        "BACKUP CLUSTER TO '/tmp/c.nvxb' TIMEOUT 18446744073709551615ns;"
    )]
    #[case::named_domain("BACKUP DOMAIN prod TO '/tmp/p.nvxb';")]
    #[case::session_domain("BACKUP DOMAIN TO '/tmp/p.nvxb' WITHOUT RESOURCES;")]
    #[case::quote_in_path("BACKUP DOMAIN prod TO \"it's.nvxb\";")]
    fn renders_a_canonical_backup_that_parses_back(#[case] source: &str) {
        let parsed = parse_client_statement(source).expect("BACKUP must parse");
        let canonical = parsed.to_canonical_nspl().expect("BACKUP always renders");
        assert_eq!(canonical, source);
        let reparsed = parse_client_statement(&canonical).expect("the canonical form must parse");
        assert_eq!(reparsed, parsed);
    }

    fn describe(source: &str, format: InspectionFormat) -> ClientStatement {
        ClientStatement::DescribeBackup(DescribeBackup {
            source: source.to_string(),
            format,
        })
    }

    #[rstest]
    #[case::text_by_default(
        "DESCRIBE BACKUP '/tmp/c.nvxb';",
        describe("/tmp/c.nvxb", InspectionFormat::Text)
    )]
    #[case::explicit_text(
        "DESCRIBE BACKUP '/tmp/c.nvxb' FORMAT TEXT",
        describe("/tmp/c.nvxb", InspectionFormat::Text)
    )]
    #[case::json(
        "describe backup \"c.nvxb\" format json;",
        describe("c.nvxb", InspectionFormat::Json)
    )]
    fn parses_describe_backup_as_a_client_statement(
        #[case] source: &str,
        #[case] expected: ClientStatement,
    ) {
        let parsed = parse_client_statement(source).expect("DESCRIBE BACKUP must parse");
        assert_eq!(parsed, expected);
        assert!(parsed.requires_local_handling());
        let canonical = parsed
            .to_canonical_nspl()
            .expect("DESCRIBE BACKUP always renders");
        assert_eq!(
            parse_client_statement(&canonical).expect("the canonical form must parse"),
            parsed
        );
    }

    #[rstest]
    #[case::no_path("DESCRIBE BACKUP;")]
    #[case::unquoted("DESCRIBE BACKUP c.nvxb;")]
    #[case::empty("DESCRIBE BACKUP '';")]
    #[case::unknown_format("DESCRIBE BACKUP 'c' FORMAT YAML;")]
    #[case::format_without_value("DESCRIBE BACKUP 'c' FORMAT;")]
    fn rejects_malformed_backup_descriptions(#[case] source: &str) {
        assert!(
            parse_client_statement(source).is_err(),
            "{source:?} must be rejected"
        );
    }

    #[test]
    fn the_server_grammar_does_not_read_a_backup_description() {
        assert!(parse_statement("DESCRIBE BACKUP '/tmp/c.nvxb';").is_err());
    }

    #[rstest]
    #[case::statement_start("BA", "BACKUP")]
    #[case::scope("BACKUP ", "CLUSTER")]
    #[case::scope_domain("BACKUP ", "DOMAIN")]
    #[case::cluster_target("BACKUP CLUSTER ", "TO")]
    #[case::domain_target("BACKUP DOMAIN ", "TO")]
    #[case::named_domain_target("BACKUP DOMAIN prod ", "TO")]
    #[case::destination("BACKUP CLUSTER TO ", "local_path")]
    #[case::optional_tail("BACKUP CLUSTER TO '/tmp/c.nvxb' ", "WITHOUT RESOURCES")]
    #[case::without_state("BACKUP CLUSTER TO '/tmp/c.nvxb' ", "WITHOUT STATE")]
    #[case::without_pause("BACKUP CLUSTER TO '/tmp/c.nvxb' ", "WITHOUT PAUSE")]
    #[case::timeout("BACKUP CLUSTER TO '/tmp/c.nvxb' ", "TIMEOUT")]
    #[case::optional_tail_prefix("BACKUP DOMAIN TO '/tmp/c.nvxb' WI", "WITHOUT RESOURCES")]
    #[case::terminator("BACKUP CLUSTER TO '/tmp/c.nvxb' WITHOUT RESOURCES ", ";")]
    #[case::describe_after_describe("DESCRIBE ", "BACKUP")]
    #[case::describe_source("DESCRIBE BACKUP ", "local_path")]
    #[case::describe_format("DESCRIBE BACKUP '/tmp/c.nvxb' ", "FORMAT")]
    #[case::describe_format_values("DESCRIBE BACKUP '/tmp/c.nvxb' FORMAT ", "JSON")]
    fn completion_offers_each_backup_clause(#[case] source: &str, #[case] expected: &str) {
        let suggestions = suggest_client_statement(source, source.len());
        assert!(
            suggestions.windows(2).all(|pair| pair[0] < pair[1]),
            "{source:?} must offer strictly sorted suggestions: {suggestions:?}"
        );
        assert!(
            suggestions.contains(&expected.to_string()),
            "{source:?} must offer {expected:?}: {suggestions:?}"
        );
    }

    #[test]
    fn completion_offers_domains_where_a_domain_may_be_named() {
        let suggestions = suggest_client_statement("BACKUP DOMAIN ", "BACKUP DOMAIN ".len());
        assert!(
            suggestions.contains(&"ref:domain".to_string()),
            "{suggestions:?}"
        );
        let suggestions = suggest_client_statement("BACKUP CLUSTER ", "BACKUP CLUSTER ".len());
        assert!(
            !suggestions.contains(&"ref:domain".to_string()),
            "{suggestions:?}"
        );
    }

    #[test]
    fn a_written_clause_is_not_offered_again() {
        let source = "BACKUP CLUSTER TO '/tmp/c.nvxb' WITHOUT RESOURCES ";
        let suggestions = suggest_client_statement(source, source.len());
        assert!(
            !suggestions.contains(&"WITHOUT RESOURCES".to_string()),
            "{suggestions:?}"
        );
        let source = "DESCRIBE BACKUP '/tmp/c.nvxb' FORMAT JSON ";
        let suggestions = suggest_client_statement(source, source.len());
        assert_eq!(suggestions, [";"]);
    }

    #[test]
    fn backup_phrases_stay_out_of_other_statement_contexts() {
        for source in ["CREATE ", "SHOW ", "DROP ", "START ", "USE ", "LIST "] {
            let suggestions = suggest_client_statement(source, source.len());
            for phrase in ["BACKUP", "WITHOUT RESOURCES", "CLUSTER TO"] {
                assert!(
                    !suggestions.contains(&phrase.to_string()),
                    "{source:?} leaks {phrase:?}: {suggestions:?}"
                );
            }
        }
        let suggestions = suggest_statement("DESCRIBE ", "DESCRIBE ".len());
        assert!(
            !suggestions.contains(&"BACKUP".to_string()),
            "the server grammar offers no client-local description: {suggestions:?}"
        );
    }

    fn restore(scope: RestoreScope, source: &str, mode: RestoreMode) -> Statement {
        Statement::Restore(Restore {
            scope,
            source: source.to_string(),
            mode,
            state: RestoreState::default(),
        })
    }

    fn cluster_scope(existing_users: ExistingUserPolicy) -> RestoreScope {
        RestoreScope::Cluster { existing_users }
    }

    fn domain_scope(name: &str, target: Option<&str>) -> RestoreScope {
        RestoreScope::Domain {
            domain: domain(name),
            target: target.map(domain),
        }
    }

    #[rstest]
    #[case::cluster(
        "RESTORE CLUSTER FROM '/tmp/cluster.nvxb';",
        restore(
            cluster_scope(ExistingUserPolicy::Fail),
            "/tmp/cluster.nvxb",
            RestoreMode::Apply
        )
    )]
    #[case::cluster_without_terminator(
        "RESTORE CLUSTER FROM 'c.nvxb'",
        restore(cluster_scope(ExistingUserPolicy::Fail), "c.nvxb", RestoreMode::Apply)
    )]
    #[case::cluster_fail_written_out(
        "RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER FAIL;",
        restore(cluster_scope(ExistingUserPolicy::Fail), "c.nvxb", RestoreMode::Apply)
    )]
    #[case::cluster_skip(
        "RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER SKIP;",
        restore(cluster_scope(ExistingUserPolicy::Skip), "c.nvxb", RestoreMode::Apply)
    )]
    #[case::cluster_replace_dry_run(
        "RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER REPLACE DRY RUN;",
        restore(
            cluster_scope(ExistingUserPolicy::Replace),
            "c.nvxb",
            RestoreMode::DryRun
        )
    )]
    #[case::cluster_dry_run(
        "RESTORE CLUSTER FROM 'c.nvxb' DRY RUN;",
        restore(cluster_scope(ExistingUserPolicy::Fail), "c.nvxb", RestoreMode::DryRun)
    )]
    #[case::domain(
        "RESTORE DOMAIN prod FROM '/var/backups/prod.nvxb';",
        restore(
            domain_scope("prod", None),
            "/var/backups/prod.nvxb",
            RestoreMode::Apply
        )
    )]
    #[case::domain_as(
        "RESTORE DOMAIN prod AS prod_copy FROM \"it's.nvxb\";",
        restore(
            domain_scope("prod", Some("prod_copy")),
            "it's.nvxb",
            RestoreMode::Apply
        )
    )]
    #[case::domain_as_dry_run(
        "RESTORE DOMAIN prod AS prod_copy FROM 'p.nvxb' DRY RUN;",
        restore(domain_scope("prod", Some("prod_copy")), "p.nvxb", RestoreMode::DryRun)
    )]
    #[case::domain_named_like_a_keyword(
        "RESTORE DOMAIN from AS dry FROM 'f.nvxb';",
        restore(domain_scope("from", Some("dry")), "f.nvxb", RestoreMode::Apply)
    )]
    #[case::lowercase_keywords(
        "restore cluster from '~/c.nvxb' on existing user skip dry run",
        restore(
            cluster_scope(ExistingUserPolicy::Skip),
            "~/c.nvxb",
            RestoreMode::DryRun
        )
    )]
    #[case::spread_over_lines(
        "RESTORE\n  DOMAIN prod\n  AS copy\n  FROM 'p.nvxb'\n  DRY\n  RUN;",
        restore(domain_scope("prod", Some("copy")), "p.nvxb", RestoreMode::DryRun)
    )]
    fn parses_every_restore_form(#[case] source: &str, #[case] expected: Statement) {
        let client = parse_client_statement(source).expect("the client grammar reads RESTORE");
        assert_eq!(client, ClientStatement::Server(expected));
        assert!(client.requires_local_handling());
    }

    #[rstest]
    #[case::configuration_only(
        "RESTORE DOMAIN prod FROM 'a.nvxb' WITHOUT STATE;",
        RestoreState::ConfigurationOnly
    )]
    #[case::without_offsets(
        "RESTORE DOMAIN prod FROM 'a.nvxb' WITHOUT SOURCE OFFSETS;",
        RestoreState::WithoutSourceOffsets
    )]
    fn restore_state_options_are_semantic(#[case] source: &str, #[case] state: RestoreState) {
        let parsed = parse_client_statement(source).expect("the restore state option parses");
        let ClientStatement::Server(Statement::Restore(restore)) = parsed else {
            panic!("the statement is a restore");
        };
        assert_eq!(restore.state, state);
    }

    /// A restore reads a file on the client's machine, so only the client grammar reads it; the
    /// server receives it on the stream that carries the archive.
    #[test]
    fn the_server_grammar_does_not_read_a_restore() {
        assert!(parse_statement("RESTORE CLUSTER FROM '/tmp/c.nvxb';").is_err());
        assert!(parse_statement("RESTORE DOMAIN prod FROM '/tmp/c.nvxb';").is_err());
    }

    #[rstest]
    #[case::no_scope("RESTORE FROM 'c.nvxb';")]
    #[case::no_source("RESTORE CLUSTER;")]
    #[case::no_from("RESTORE CLUSTER 'c.nvxb';")]
    #[case::to_instead_of_from("RESTORE CLUSTER TO 'c.nvxb';")]
    #[case::unquoted_source("RESTORE CLUSTER FROM c.nvxb;")]
    #[case::empty_source("RESTORE CLUSTER FROM '';")]
    #[case::cluster_with_domain("RESTORE CLUSTER prod FROM 'c.nvxb';")]
    #[case::cluster_with_as("RESTORE CLUSTER AS copy FROM 'c.nvxb';")]
    #[case::domain_without_name("RESTORE DOMAIN FROM 'c.nvxb';")]
    #[case::domain_with_user_policy("RESTORE DOMAIN prod FROM 'c.nvxb' ON EXISTING USER SKIP;")]
    #[case::as_after_from("RESTORE DOMAIN prod FROM 'c.nvxb' AS copy;")]
    #[case::as_without_name("RESTORE DOMAIN prod AS FROM 'c.nvxb';")]
    #[case::policy_without_value("RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER;")]
    #[case::unknown_policy("RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER MERGE;")]
    #[case::partial_policy_phrase("RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING SKIP;")]
    #[case::dry_alone("RESTORE CLUSTER FROM 'c.nvxb' DRY;")]
    #[case::dry_run_before_policy("RESTORE CLUSTER FROM 'c.nvxb' DRY RUN ON EXISTING USER SKIP;")]
    #[case::repeated_dry_run("RESTORE CLUSTER FROM 'c.nvxb' DRY RUN DRY RUN;")]
    #[case::conflicting_state(
        "RESTORE CLUSTER FROM 'c.nvxb' WITHOUT STATE WITHOUT SOURCE OFFSETS;"
    )]
    #[case::trailing_word("RESTORE CLUSTER FROM 'c.nvxb' NOW;")]
    #[case::two_domains("RESTORE DOMAIN a b FROM 'c.nvxb';")]
    fn rejects_malformed_restores(#[case] source: &str) {
        assert!(
            parse_client_statement(source).is_err(),
            "{source:?} must be rejected by the client grammar"
        );
    }

    #[rstest]
    #[case::cluster("RESTORE CLUSTER FROM '/tmp/c.nvxb';")]
    #[case::cluster_skip("RESTORE CLUSTER FROM '/tmp/c.nvxb' ON EXISTING USER SKIP;")]
    #[case::cluster_replace_dry_run(
        "RESTORE CLUSTER FROM '/tmp/c.nvxb' ON EXISTING USER REPLACE DRY RUN;"
    )]
    #[case::domain("RESTORE DOMAIN prod FROM '/tmp/p.nvxb';")]
    #[case::without_state("RESTORE DOMAIN prod FROM '/tmp/p.nvxb' WITHOUT STATE;")]
    #[case::without_source_offsets(
        "RESTORE DOMAIN prod FROM '/tmp/p.nvxb' WITHOUT SOURCE OFFSETS;"
    )]
    #[case::domain_as_dry_run("RESTORE DOMAIN prod AS prod_copy FROM '/tmp/p.nvxb' DRY RUN;")]
    #[case::quote_in_path("RESTORE DOMAIN prod FROM \"it's.nvxb\";")]
    fn renders_a_canonical_restore_that_parses_back(#[case] source: &str) {
        let parsed = parse_client_statement(source).expect("RESTORE must parse");
        let canonical = parsed.to_canonical_nspl().expect("RESTORE always renders");
        assert_eq!(canonical, source);
        let reparsed = parse_client_statement(&canonical).expect("the canonical form must parse");
        assert_eq!(reparsed, parsed);
    }

    #[test]
    fn the_default_user_policy_is_left_out_of_the_canonical_form() {
        let parsed = parse_client_statement("RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER FAIL;")
            .expect("RESTORE must parse");
        assert_eq!(
            parsed.to_canonical_nspl().expect("RESTORE always renders"),
            "RESTORE CLUSTER FROM 'c.nvxb';"
        );
    }

    #[rstest]
    #[case::statement_start("RES", "RESTORE")]
    #[case::scope_cluster("RESTORE ", "CLUSTER")]
    #[case::scope_domain("RESTORE ", "DOMAIN")]
    #[case::cluster_source("RESTORE CLUSTER ", "FROM")]
    #[case::domain_name("RESTORE DOMAIN ", "domain_name")]
    #[case::domain_target("RESTORE DOMAIN prod ", "AS")]
    #[case::domain_source("RESTORE DOMAIN prod ", "FROM")]
    #[case::renamed_domain_source("RESTORE DOMAIN prod AS copy ", "FROM")]
    #[case::source_path("RESTORE CLUSTER FROM ", "local_path")]
    #[case::cluster_policy("RESTORE CLUSTER FROM 'c.nvxb' ", "ON EXISTING USER")]
    #[case::cluster_dry_run("RESTORE CLUSTER FROM 'c.nvxb' ", "DRY RUN")]
    #[case::without_state("RESTORE CLUSTER FROM 'c.nvxb' ", "WITHOUT STATE")]
    #[case::without_source_offsets("RESTORE CLUSTER FROM 'c.nvxb' ", "WITHOUT SOURCE OFFSETS")]
    #[case::cluster_terminator("RESTORE CLUSTER FROM 'c.nvxb' ", ";")]
    #[case::policy_prefix("RESTORE CLUSTER FROM 'c.nvxb' ON", "ON EXISTING USER")]
    #[case::policy_fail("RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER ", "FAIL")]
    #[case::policy_skip("RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER ", "SKIP")]
    #[case::policy_replace("RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER ", "REPLACE")]
    #[case::dry_run_after_policy("RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER SKIP ", "DRY RUN")]
    #[case::domain_dry_run("RESTORE DOMAIN prod FROM 'p.nvxb' ", "DRY RUN")]
    #[case::dry_run_prefix("RESTORE DOMAIN prod FROM 'p.nvxb' DR", "DRY RUN")]
    #[case::terminator_after_dry_run("RESTORE DOMAIN prod FROM 'p.nvxb' DRY RUN ", ";")]
    fn completion_offers_each_restore_clause(#[case] source: &str, #[case] expected: &str) {
        let suggestions = suggest_client_statement(source, source.len());
        assert!(
            suggestions.windows(2).all(|pair| pair[0] < pair[1]),
            "{source:?} must offer strictly sorted suggestions: {suggestions:?}"
        );
        assert!(
            suggestions.contains(&expected.to_string()),
            "{source:?} must offer {expected:?}: {suggestions:?}"
        );
    }

    #[test]
    fn a_written_restore_clause_is_not_offered_again() {
        let source = "RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER SKIP ";
        let suggestions = suggest_client_statement(source, source.len());
        assert!(
            !suggestions.contains(&"ON EXISTING USER".to_string()),
            "{suggestions:?}"
        );
        let source = "RESTORE CLUSTER FROM 'c.nvxb' DRY RUN ";
        let suggestions = suggest_client_statement(source, source.len());
        assert!(suggestions.contains(&"WITHOUT STATE".to_string()));
        assert!(suggestions.contains(&"WITHOUT SOURCE OFFSETS".to_string()));
        let source = "RESTORE DOMAIN prod FROM 'p.nvxb' ";
        let suggestions = suggest_client_statement(source, source.len());
        assert!(
            !suggestions.contains(&"ON EXISTING USER".to_string()),
            "a domain restore imports no users: {suggestions:?}"
        );
    }

    #[test]
    fn a_restore_names_archived_domains_rather_than_existing_ones() {
        let suggestions = suggest_client_statement("RESTORE DOMAIN ", "RESTORE DOMAIN ".len());
        assert!(
            !suggestions.contains(&"ref:domain".to_string()),
            "{suggestions:?}"
        );
    }

    #[test]
    fn restore_phrases_stay_out_of_other_statement_contexts() {
        for source in [
            "CREATE ", "SHOW ", "DROP ", "START ", "USE ", "LIST ", "BACKUP ",
        ] {
            let suggestions = suggest_client_statement(source, source.len());
            for phrase in ["RESTORE", "ON EXISTING USER", "DRY RUN"] {
                assert!(
                    !suggestions.contains(&phrase.to_string()),
                    "{source:?} leaks {phrase:?}: {suggestions:?}"
                );
            }
        }
        let source = "BACKUP CLUSTER TO 'c.nvxb' ";
        let suggestions = suggest_client_statement(source, source.len());
        for phrase in ["ON EXISTING USER", "DRY RUN"] {
            assert!(
                !suggestions.contains(&phrase.to_string()),
                "a backup leaks {phrase:?}: {suggestions:?}"
            );
        }
        let suggestions = suggest_statement("RES", "RES".len());
        assert!(
            !suggestions.contains(&"RESTORE".to_string()),
            "the server grammar offers no restore: {suggestions:?}"
        );
    }

    /// Every generated Model exported the way a backup exports a domain — one canonical document,
    /// held as a section of an archive and read back from the archive stream — reparses to itself,
    /// statement for statement and in order.
    #[test]
    fn bolero_models_exported_through_an_archive_reparse_to_themselves() {
        bolero::check!()
            .with_iterations(64)
            .with_max_len(4096)
            .for_each(|bytes: &[u8]| {
                let mut arbitrary = Arbitrary::new(bytes, Domain::Nspl);
                let count = arbitrary.entropy().count(4);
                let mut models = Vec::with_capacity(count);
                for _ in 0..count {
                    models.push(arbitrary.model());
                }
                let document = nervix_models::canonical_nspl_document(&models)
                    .expect("generator output must be renderable");
                let exported = archived_models_document(&document);
                assert_eq!(
                    exported, document,
                    "the archive returns the document it holds"
                );
                let statements = crate::client_statement::parse_client_statements(&exported)
                    .unwrap_or_else(|error| panic!("{exported} must reparse: {error:?}"));
                let expected = models
                    .into_iter()
                    .map(|model| {
                        crate::client_statement::ClientStatement::Server(Statement::Create(
                            nervix_models::CreateStatement::new(Box::new(model), false),
                        ))
                    })
                    .collect::<Vec<_>>();
                assert_eq!(statements, expected, "{exported} changed meaning");
            });
    }

    /// Writes `document` as the `models.nspl` section of an archive and reads it back from the
    /// archive's bytes.
    fn archived_models_document(document: &str) -> String {
        use std::io::Read as _;

        use nervix_backup::{
            ArchiveLayout, ArchiveReadError, ArchiveScope, BackupManifest, SectionContent,
            SectionDigester, SectionEntry, SectionPath, SectionReader, SectionVisitor,
            read_archive,
        };

        struct Collector {
            text: String,
        }

        impl SectionVisitor for Collector {
            fn manifest(
                &mut self,
                _manifest: &BackupManifest,
            ) -> Result<(), error_stack::Report<ArchiveReadError>> {
                Ok(())
            }

            fn section(
                &mut self,
                _entry: &SectionEntry,
                content: &mut SectionReader<'_>,
            ) -> Result<(), error_stack::Report<ArchiveReadError>> {
                content
                    .read_to_string(&mut self.text)
                    .expect("the exported document is UTF-8");
                Ok(())
            }
        }

        let domain = nervix_models::DomainName::parse("exported").expect("a valid domain");
        let entry = SectionEntry {
            path: SectionPath::domain_models(&domain),
            content: SectionContent::Nspl,
            length: u64::try_from(document.len()).expect("the document fits 64 bits"),
            digest: SectionDigester::digest_of(document.as_bytes()),
        };
        let manifest = BackupManifest {
            producer_version: nervix_models::NSPL_LANGUAGE_VERSION.to_string(),
            language_version: nervix_models::NSPL_LANGUAGE_VERSION.to_string(),
            cluster_id: "property".to_string(),
            captured_at: nervix_models::Timestamp::from_unix_nanos(0),
            scope: ArchiveScope::Domain(domain),
            resources: nervix_models::BackupResources::Included,
            domains: Vec::new(),
            sections: vec![entry],
        };
        let layout = ArchiveLayout::new(manifest).expect("the archive lays out");
        let mut archive = Vec::new();
        layout
            .write_to(&mut archive, |_, sink| {
                std::io::Write::write_all(sink, document.as_bytes())
            })
            .expect("the archive writes");
        let mut collector = Collector {
            text: String::new(),
        };
        read_archive(archive.as_slice(), &mut collector).expect("the archive reads back");
        collector.text
    }
}
