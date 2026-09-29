//! `BACKUP` and `DESCRIBE BACKUP` grammar.
//!
//! Layer: language.
//! - **Owns.** Parsing a backup of the cluster or of one domain into a local archive file, and a
//!   description of a local archive file, each with its optional clauses in their fixed order.
//! - **Depends on.** Shared NSPL tokens, the shared keyword, domain reference, local path and
//!   inspection format primitives, and vocabulary Models.
//! - **Must not know.** Sessions, how an archive is assembled, transferred or read, or where a
//!   client writes it.

use chumsky::prelude::*;
use nervix_models::{Backup, BackupResources, BackupScope, DescribeBackup, InspectionFormat};

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{ParseError, domain_ref, inspection_format, kw, kw_phrase2, local_path, tok},
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
    kw(Identifier::Backup)
        .ignore_then(scope)
        .then_ignore(kw(Identifier::To))
        .then(local_path())
        .then(resources)
        .map(|((scope, destination), resources)| Backup {
            scope,
            destination,
            resources,
        })
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
    if writes_keyword(tokens, Identifier::Without) {
        return vec![";".to_string()];
    }
    vec![";".to_string(), "WITHOUT RESOURCES".to_string()]
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
    use nervix_models::{DomainName, Statement};
    use rstest::rstest;

    use super::*;
    use crate::{
        client_statement::{ClientStatement, parse_client_statement, suggest_client_statement},
        statement::{parse_statement, suggest_statement, tests::gen_model},
    };

    fn domain(name: &str) -> DomainName {
        DomainName::parse(name).assured("the test domain is an identifier-shaped literal")
    }

    fn backup(scope: BackupScope, destination: &str, resources: BackupResources) -> Statement {
        Statement::Backup(Backup {
            scope,
            destination: destination.to_string(),
            resources,
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
    #[case::without_state("BACKUP CLUSTER TO '/tmp/c.nvxb' WITHOUT STATE;")]
    #[case::repeated_option("BACKUP CLUSTER TO 'c' WITHOUT RESOURCES WITHOUT RESOURCES;")]
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
    #[case::optional_tail_prefix("BACKUP DOMAIN TO '/tmp/c.nvxb' WI", "WITHOUT RESOURCES")]
    #[case::terminator("BACKUP CLUSTER TO '/tmp/c.nvxb' WITHOUT RESOURCES ", ";")]
    #[case::describe_after_describe("DESCRIBE ", "BACKUP")]
    #[case::describe_source("DESCRIBE BACKUP ", "local_path")]
    #[case::describe_format("DESCRIBE BACKUP '/tmp/c.nvxb' ", "FORMAT")]
    #[case::describe_format_values("DESCRIBE BACKUP '/tmp/c.nvxb' FORMAT ", "JSON")]
    fn completion_offers_each_backup_clause(#[case] source: &str, #[case] expected: &str) {
        let suggestions = suggest_client_statement(source, source.len());
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

    /// Every generated Model exported the way a backup exports a domain — one canonical document,
    /// held as a section of an archive and read back from the archive stream — reparses to itself,
    /// statement for statement and in order.
    #[test]
    fn bolero_models_exported_through_an_archive_reparse_to_themselves() {
        bolero::check!()
            .with_test_time(std::time::Duration::from_millis(150))
            .for_each(|bytes: &[u8]| {
                let models = bytes.chunks(32).map(gen_model).collect::<Vec<_>>();
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
