//! `REBIND RESOURCE` grammar.
//!
//! Layer: language.
//! - **Owns.** Parsing one resource rebinding and its optional kind-qualified selection.
//! - **Depends on.** Shared NSPL tokens and vocabulary Models.
//! - **Must not know.** Resource catalogs, stored Models, transactions, or runtime activation.

use std::collections::BTreeSet;

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{
    ModelKind, NodeRef, RebindResource, RebindResourceMembers, RebindResourceSelection,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, boxed_choice, client_ref, codec_ref, inferencer_ref, kw, kw_phrase2,
        lookup_ref, resource_ref, resource_version_clause, signaling_protocol_ref, tok, vhost_ref,
        wasm_processor_ref,
    },
};

fn rebind_member<'src>()
-> impl Parser<'src, &'src [Token], NodeRef, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        kw(Identifier::Vhost)
            .ignore_then(vhost_ref())
            .map(|name| NodeRef::new(ModelKind::Vhost, name)),
        kw(Identifier::Codec)
            .ignore_then(codec_ref())
            .map(|name| NodeRef::new(ModelKind::Codec, name)),
        kw_phrase2(Identifier::Signaling, Identifier::Protocol)
            .ignore_then(signaling_protocol_ref())
            .map(|name| NodeRef::new(ModelKind::SignalingProtocol, name)),
        kw(Identifier::Inferencer)
            .ignore_then(inferencer_ref())
            .map(|name| NodeRef::new(ModelKind::Inferencer, name)),
        kw_phrase2(Identifier::Wasm, Identifier::Processor)
            .ignore_then(wasm_processor_ref())
            .map(|name| NodeRef::new(ModelKind::WasmProcessor, name)),
        kw_phrase2(Identifier::Hash, Identifier::Map)
            .ignore_then(lookup_ref())
            .map(|name| NodeRef::new(ModelKind::Lookup, name)),
        kw(Identifier::Client)
            .ignore_then(client_ref())
            .map(|name| NodeRef::new(ModelKind::Client, name)),
    )
}

fn rebind_members<'src>()
-> impl Parser<'src, &'src [Token], RebindResourceMembers, extra::Err<ParseError<'src>>> + Clone {
    rebind_member()
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>()
        .try_map(|members, span| {
            let mut unique = BTreeSet::new();
            for member in members {
                if !unique.insert(member.clone()) {
                    return Err(Rich::custom(
                        span,
                        format!(
                            "duplicate FOR member {} '{}'",
                            member.kind.keyword_phrase(),
                            member.identifier.as_str()
                        ),
                    ));
                }
            }
            let mut unique = unique.into_iter();
            let first = unique
                .next()
                .verified("at_least(1) above guarantees one parsed FOR member");
            Ok(RebindResourceMembers::new(first, unique))
        })
        .boxed()
}

pub fn rebind_resource_parser<'src>()
-> impl Parser<'src, &'src [Token], RebindResource, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Rebind)
        .ignore_then(kw(Identifier::Resource))
        .ignore_then(resource_ref())
        .then_ignore(kw(Identifier::To))
        .then(resource_version_clause())
        .then(kw(Identifier::For).ignore_then(rebind_members()).or_not())
        .map(|((resource, version), members)| {
            let selection = match members {
                Some(members) => RebindResourceSelection::Members(members),
                None => RebindResourceSelection::All,
            };
            RebindResource {
                resource,
                version,
                selection,
            }
        })
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

#[cfg(test)]
mod tests {
    use nervix_models::{RebindResourceSelection, RequestedResourceVersion, Statement};

    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn parses_every_resource_binding_kind() {
        let statement = parse_statement(
            "REBIND RESOURCE bundle TO VERSION LATEST FOR VHOST api, CODEC proto, SIGNALING \
             PROTOCOL ws, INFERENCER score, WASM PROCESSOR enrich, HASH MAP countries, CLIENT \
             mounted;",
        )
        .expect("resource rebind must parse");
        let Statement::RebindResource(rebind) = statement else {
            panic!("expected a resource rebind");
        };
        assert_eq!(rebind.resource.as_str(), "bundle");
        assert_eq!(rebind.version, RequestedResourceVersion::Latest);
        let RebindResourceSelection::Members(members) = rebind.selection else {
            panic!("FOR must produce an exact member selection");
        };
        assert_eq!(members.iter().count(), 7);
    }

    #[test]
    fn parses_an_explicit_version_without_a_selection() {
        let statement = parse_statement("REBIND RESOURCE bundle TO VERSION 7;")
            .expect("resource rebind must parse");
        let Statement::RebindResource(rebind) = statement else {
            panic!("expected a resource rebind");
        };
        assert_eq!(rebind.version, RequestedResourceVersion::Number(7));
        assert_eq!(rebind.selection, RebindResourceSelection::All);
    }

    #[test]
    fn rejects_duplicate_members() {
        let error = parse_statement(
            "REBIND RESOURCE bundle TO VERSION 2 FOR HASH MAP countries, HASH MAP countries;",
        )
        .expect_err("duplicate FOR members must be rejected");
        assert!(error.to_string().contains("duplicate FOR member"));
    }

    #[test]
    fn rejects_an_unsupported_kind_and_an_unqualified_member() {
        assert!(parse_statement("REBIND RESOURCE bundle TO VERSION 2 FOR RELAY events;").is_err());
        assert!(
            parse_statement(
                "REBIND RESOURCE bundle TO VERSION 2 FOR CLIENT mounted, another_client;"
            )
            .is_err()
        );
    }

    #[test]
    fn offers_the_composed_grammar_at_each_context() {
        for (source, expected) in [
            ("REBIND ", "RESOURCE"),
            ("REBIND RESOURCE bundle ", "TO"),
            ("REBIND RESOURCE bundle TO ", "VERSION"),
            ("REBIND RESOURCE bundle TO VERSION ", "LATEST"),
            ("REBIND RESOURCE bundle TO VERSION 2 ", "FOR"),
            ("REBIND RESOURCE bundle TO VERSION 2 FOR ", "HASH MAP"),
            (
                "REBIND RESOURCE bundle TO VERSION 2 FOR CLIENT mounted, ",
                "CLIENT",
            ),
        ] {
            let suggestions = suggest_statement(source, source.len());
            assert!(
                suggestions.contains(&expected.to_string()),
                "{source:?} offered {suggestions:?}"
            );
        }

        let completed_member = "REBIND RESOURCE bundle TO VERSION 2 FOR CLIENT mounted ";
        let suggestions = suggest_statement(completed_member, completed_member.len());
        assert!(suggestions.contains(&",".to_string()), "{suggestions:?}");
        assert!(suggestions.contains(&";".to_string()), "{suggestions:?}");

        let multiline = "REBIND\nRESOURCE bundle\nTO VERSION 2 ";
        let suggestions = suggest_statement(multiline, multiline.len());
        assert!(suggestions.contains(&"FOR".to_string()), "{suggestions:?}");
    }
}
