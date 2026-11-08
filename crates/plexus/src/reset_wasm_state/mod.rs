//! `RESET WASM PROCESSOR ... STATE` grammar.
//!
//! Layer: language.
//! - **Owns.** Parsing an explicit WASM guest-state reset selection.
//! - **Depends on.** Shared Plexus tokens and vocabulary Models.
//! - **Must not know.** Processor schedules, branch instances, or checkpoint storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use nervix_models::ResetWasmState;

use crate::{
    lexer::{Identifier, Token},
    parser_support::{ParseError, domain_ref, kw, kw_phrase2, tok, wasm_processor_ref},
};

pub fn reset_wasm_state_parser<'src>()
-> impl Parser<'src, &'src [Token], ResetWasmState, extra::Err<ParseError<'src>>> + Clone {
    let scope = crate::state_branch_selection::branch_selection();
    kw_phrase2(Identifier::Reset, Identifier::Wasm)
        .ignore_then(kw(Identifier::Processor))
        .ignore_then(wasm_processor_ref())
        .then_ignore(kw(Identifier::State))
        .then_ignore(kw(Identifier::In))
        .then_ignore(kw(Identifier::Domain))
        .then(domain_ref())
        .then(scope)
        .map(|((processor, domain), scope)| ResetWasmState {
            domain,
            processor,
            scope,
        })
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{Literal, StateBranchSelection, Statement};

    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn parses_explicit_branch_values() {
        let statement = parse_statement(
            "RESET WASM PROCESSOR counter STATE IN DOMAIN sales FOR BRANCH VALUES { tenant = \
             'alpha' };",
        )
        .assured("the test command has an explicit domain and branch");
        let Statement::ResetWasmState(reset) = statement else {
            panic!("expected a WASM state reset");
        };
        assert_eq!(reset.domain.as_str(), "sales");
        assert_eq!(reset.processor.as_str(), "counter");
        let StateBranchSelection::Branch(fields) = reset.scope else {
            panic!("expected one branch selection");
        };
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name.as_str(), "tenant");
        assert_eq!(fields[0].value, Literal::String("alpha".to_string()));
    }

    #[test]
    fn parses_every_explicit_scope() {
        let unbranched =
            parse_statement("RESET WASM PROCESSOR counter STATE IN DOMAIN sales FOR UNBRANCHED;")
                .assured("the test command has an explicit unbranched scope");
        let all =
            parse_statement("RESET WASM PROCESSOR counter STATE IN DOMAIN sales FOR ALL BRANCHES;")
                .assured("the test command has an explicit all-branches scope");
        assert!(matches!(
            unbranched,
            Statement::ResetWasmState(reset) if reset.scope == StateBranchSelection::Unbranched
        ));
        assert!(matches!(
            all,
            Statement::ResetWasmState(reset) if reset.scope == StateBranchSelection::AllBranches
        ));
        let numeric = parse_statement(
            "RESET WASM PROCESSOR counter STATE IN DOMAIN sales FOR BRANCH VALUES { shard = -7 };",
        )
        .assured("the test command contains a valid signed numeric literal");
        assert!(matches!(
            numeric,
            Statement::ResetWasmState(reset)
                if matches!(&reset.scope, StateBranchSelection::Branch(fields)
                    if fields[0].value == Literal::I64(-7))
        ));
        let fractional = parse_statement(
            "RESET WASM PROCESSOR counter STATE IN DOMAIN sales FOR BRANCH VALUES { shard = -1.25 \
             };",
        )
        .assured("the test command contains a valid floating-point literal");
        assert!(matches!(
            fractional,
            Statement::ResetWasmState(reset)
                if matches!(&reset.scope, StateBranchSelection::Branch(fields)
                    if fields[0].value == Literal::F64(nervix_models::Float64Literal::new(-1.25)))
        ));
    }

    #[test]
    fn rejects_implicit_scope_and_duplicate_fields() {
        assert!(parse_statement("RESET WASM PROCESSOR counter STATE IN DOMAIN sales;").is_err());
        assert!(
            parse_statement(
                "RESET WASM PROCESSOR counter STATE IN DOMAIN sales FOR BRANCH VALUES { tenant = \
                 'alpha', tenant = 'beta' };"
            )
            .is_err()
        );
    }

    #[test]
    fn completion_follows_the_reset_phrase() {
        let cases = [
            ("RESET ", "WASM"),
            ("RESET WASM ", "PROCESSOR"),
            ("RESET WASM PROCESSOR counter ", "STATE"),
            ("RESET WASM PROCESSOR counter STATE ", "IN"),
            ("RESET WASM PROCESSOR counter STATE IN ", "DOMAIN"),
            ("RESET WASM PROCESSOR counter STATE IN DOMAIN sales ", "FOR"),
            (
                "RESET WASM PROCESSOR counter STATE IN DOMAIN sales FOR ",
                "ALL BRANCHES",
            ),
            (
                "RESET WASM PROCESSOR counter STATE IN DOMAIN sales FOR ",
                "BRANCH VALUES",
            ),
            (
                "RESET WASM PROCESSOR counter STATE IN DOMAIN sales FOR BRANCH ",
                "VALUES",
            ),
        ];
        for (input, suggestion) in cases {
            let actual = suggest_statement(input, input.len());
            assert!(
                actual.contains(&suggestion.to_string()),
                "{input:?}: {actual:?}"
            );
        }
    }

    #[test]
    fn selected_reset_roundtrips_through_canonical_plexus() {
        let source = "reset wasm processor counter state in domain sales for branch values { \
                      tenant = 'alpha', shard = -7 };";
        let parsed = parse_statement(source)
            .assured("the test command contains an explicit and valid branch selection");
        let canonical = parsed
            .to_canonical_plexus()
            .assured("every reset literal has a canonical Plexus rendering");
        let reparsed = parse_statement(&canonical)
            .assured("the canonical reset uses the same declared grammar");
        assert_eq!(reparsed, parsed);
        for scope in ["UNBRANCHED", "ALL BRANCHES"] {
            let source = format!("RESET WASM PROCESSOR counter STATE IN DOMAIN sales FOR {scope};");
            let parsed = parse_statement(&source)
                .assured("the test command contains an explicit reset scope");
            let canonical = parsed
                .to_canonical_plexus()
                .assured("each explicit reset scope has a canonical rendering");
            assert_eq!(
                parse_statement(&canonical)
                    .assured("the canonical reset scope follows the public grammar"),
                parsed
            );
        }
    }
}
