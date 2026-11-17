//! Administrative statements for the cluster licensing authority.
//!
//! Layer: language.
//! - **Owns.** Composed licensing command heads, credential syntax and completion expectations.
//! - **Depends on.** Shared tokens, literal grammar and semantic licensing Models.
//! - **Must not know.** Service addresses, cryptography, consensus or policy execution.

use chumsky::prelude::*;
use nervix_models::{
    ActivateLicense, ActivationKind, RefreshLicense, ReleaseLicense, RotateLicenseClusterKey,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, kw, kw_phrase, kw_phrase2, string_lit, tok},
};

pub fn activate_license_parser<'src>()
-> impl Parser<'src, &'src [Token], ActivateLicense, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Activate, Identifier::License)
        .ignore_then(choice((
            kw(Identifier::Persistent).to(ActivationKind::Persistent),
            kw(Identifier::Ephemeral).to(ActivationKind::Ephemeral),
        )))
        .then_ignore(kw_phrase2(Identifier::With, Identifier::Key))
        .then(string_lit())
        .map(|(kind, license_key)| ActivateLicense { kind, license_key })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn refresh_license_parser<'src>()
-> impl Parser<'src, &'src [Token], RefreshLicense, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Refresh, Identifier::License)
        .to(RefreshLicense)
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn release_license_parser<'src>()
-> impl Parser<'src, &'src [Token], ReleaseLicense, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Release, Identifier::License)
        .to(ReleaseLicense)
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn rotate_license_cluster_key_parser<'src>()
-> impl Parser<'src, &'src [Token], RotateLicenseClusterKey, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase([
        Identifier::Rotate,
        Identifier::License,
        Identifier::Cluster,
        Identifier::Key,
    ])
    .to(RotateLicenseClusterKey)
    .then_ignore(tok(Token::Semicolon).or_not())
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::Statement;

    #[test]
    fn activation_and_refresh_use_the_composed_grammar_and_canonical_literals() {
        for source in [
            "activate license persistent with key 'test-credential';",
            "ACTIVATE LICENSE EPHEMERAL WITH KEY 'test-credential';",
            "refresh license;",
            "release license;",
            "rotate license cluster key;",
        ] {
            let parsed =
                crate::statement::parse_statement(source).assured("current command parses");
            let formatted = parsed
                .to_canonical_plexus()
                .assured("semantic command formats");
            assert_eq!(
                crate::statement::parse_statement(&formatted).assured("canonical command parses"),
                parsed
            );
            if let Statement::ActivateLicense(activate) = parsed {
                assert!(!format!("{activate:?}").contains("test-credential"));
            }
        }
    }
    #[test]
    fn incomplete_heads_and_nonliteral_credentials_fail_in_the_composed_grammar() {
        for source in [
            "ACTIVATE LICENSE WITH KEY 'credential';",
            "ACTIVATE LICENSE PERSISTENT WITH KEY 123;",
            "REFRESH LICENSE WITH KEY 'credential';",
            "RELEASE LICENSE WITH KEY 'credential';",
            "ROTATE LICENSE KEY;",
            "ROTATE CLUSTER KEY;",
        ] {
            assert!(
                crate::statement::parse_statement(source).is_err(),
                "{source}"
            );
        }
    }
    #[cfg(feature = "client")]
    #[test]
    fn activation_completion_comes_from_the_composed_head_and_kind() {
        use crate::client_statement::{CompletionExpectation, suggest_client_expectations};
        for (prefix, labels) in [
            ("ACTIVATE LICENSE ", &["PERSISTENT", "EPHEMERAL"][..]),
            ("ACTIVATE LICENSE PERSISTENT ", &["WITH KEY"][..]),
            ("ACTIVATE LICENSE EPHEMERAL ", &["WITH KEY"][..]),
            ("RELEASE ", &["LICENSE"][..]),
            ("R", &["ROTATE LICENSE CLUSTER KEY", "RELEASE LICENSE"][..]),
            ("ROTATE ", &["LICENSE"][..]),
            ("ROTATE LICENSE ", &["CLUSTER"][..]),
            ("ROTATE LICENSE CLUSTER ", &["KEY"][..]),
        ] {
            let suggestions = suggest_client_expectations(prefix, prefix.len());
            for label in labels {
                assert!(
                    suggestions.contains(&CompletionExpectation::Literal((*label).to_owned())),
                    "{prefix}: {suggestions:?}"
                );
            }
        }
    }
}
