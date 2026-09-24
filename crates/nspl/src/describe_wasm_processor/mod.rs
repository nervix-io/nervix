//! `DESCRIBE WASM PROCESSOR` grammar and its optional inspection format.
//!
//! Layer: language.
//! - **Owns.** Parsing a WASM processor reference and its text or JSON inspection rendering.
//! - **Depends on.** Shared NSPL tokens, inspection format grammar and vocabulary Models.
//! - **Must not know.** Runtime checkpoint state, sessions or output rendering.

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::DescribeWasmProcessor;

use crate::{
    lexer::{Identifier, Token, Word},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, inspection_format, into_parse_error, kw,
        lex_input, suggest_from, tok, wasm_processor_ref,
    },
};

pub fn describe_wasm_processor_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeWasmProcessor, extra::Err<ParseError<'src>>> + Clone {
    let format = kw(Identifier::Format)
        .ignore_then(inspection_format())
        .or_not();
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Wasm))
        .ignore_then(kw(Identifier::Processor))
        .ignore_then(wasm_processor_ref())
        .then(format)
        .map(|(name, format)| DescribeWasmProcessor {
            name,
            format: format.unwrap_or_default(),
        })
        .then_ignore(tok(Token::Semicolon).or_not())
}

/// The optional format clause is still available after a complete name.
pub(crate) fn describe_wasm_processor_tail(tokens: &[Token]) -> Vec<String> {
    let format_written = tokens.iter().any(|token| {
        matches!(
            token,
            Token::Word(Word::KnownWord {
                iden: Identifier::Format,
                ..
            })
        )
    });
    if format_written {
        vec![";".to_string()]
    } else {
        vec![";".to_string(), "FORMAT".to_string()]
    }
}

pub fn parse_describe_wasm_processor_tokens(
    tokens: &[Token],
) -> Result<DescribeWasmProcessor, Vec<ParseError<'_>>> {
    let out = describe_wasm_processor_parser()
        .then_ignore(end())
        .parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_describe_wasm_processor(
    input: &str,
) -> Result<DescribeWasmProcessor, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_describe_wasm_processor_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_describe_wasm_processor(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, describe_wasm_processor_parser())
}

#[cfg(test)]
mod tests {
    use nervix_models::InspectionFormat;

    use super::*;
    use crate::lexer::lex;

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn parses_describe_wasm_processor() {
        let tokens = to_tokens("DESCRIBE WASM PROCESSOR filter_even;");
        let parsed = parse_describe_wasm_processor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "filter_even");
        assert_eq!(parsed.format, InspectionFormat::Text);
        let tokens = to_tokens("DESCRIBE WASM PROCESSOR filter_even FORMAT JSON;");
        let parsed = parse_describe_wasm_processor_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.format, InspectionFormat::Json);
    }

    #[test]
    fn suggests_wasm_processor_reference_after_describe_wasm_processor() {
        let input = "DESCRIBE WASM PROCESSOR ";
        let suggestions = suggest_describe_wasm_processor(input, input.len());
        assert!(suggestions.contains(&"ref:wasm_processor".to_string()));
    }
}
