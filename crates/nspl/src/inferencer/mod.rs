use chumsky::prelude::*;
use nervix_models::{AckMode, CreateInferencer, CreateStatement, InferencerTensorMapping};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_parameterization, current_word_prefix,
        filter_where_clause, flush_each, if_not_exists_clause, inferencer_name, into_parse_error,
        kw, lex_input, message_error_policy, processor_outputs, relay_ref, resource_ref,
        string_lit, suggestions_from_errors, tok, word_raw,
    },
};

fn u64_value<'src>() -> impl Parser<'src, &'src [Token], u64, extra::Err<ParseError<'src>>> + Clone
{
    choice((select! { Token::NumberLiteral(v) => v }, word_raw())).try_map(|raw, span| {
        raw.parse::<u64>()
            .map_err(|_| Rich::custom(span, format!("invalid integer '{raw}'")))
    })
}

fn field_mapping<'src>()
-> impl Parser<'src, &'src [Token], InferencerTensorMapping, extra::Err<ParseError<'src>>> + Clone {
    string_lit()
        .then_ignore(tok(Token::Eq))
        .then(relay_ref())
        .then_ignore(tok(Token::Dot))
        .then(crate::parser_support::field_ref())
        .map(|((tensor, relay), field)| InferencerTensorMapping {
            tensor,
            relay,
            field,
        })
}

fn field_mappings<'src>(
    keyword: Identifier,
) -> impl Parser<'src, &'src [Token], Vec<InferencerTensorMapping>, extra::Err<ParseError<'src>>> + Clone
{
    kw(keyword)
        .ignore_then(tok(Token::LBrace))
        .ignore_then(
            field_mapping()
                .separated_by(tok(Token::Comma))
                .allow_trailing()
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .then_ignore(tok(Token::RBrace))
}

pub fn create_inferencer_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateInferencer>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(ack_mode().or_not())
        .then_ignore(kw(Identifier::Inferencer))
        .then(inferencer_name())
        .then_ignore(kw(Identifier::From))
        .then(relay_ref())
        .then(filter_where_clause().or_not())
        .then(processor_outputs())
        .then(branch_parameterization())
        .then_ignore(kw(Identifier::Using))
        .then_ignore(kw(Identifier::Resource))
        .then(resource_ref())
        .then(kw(Identifier::Version).ignore_then(u64_value()).or_not())
        .then_ignore(kw(Identifier::File))
        .then(string_lit())
        .then(field_mappings(Identifier::Inputs))
        .then(field_mappings(Identifier::Outputs))
        .then(flush_each())
        .then(message_error_policy())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|value| {
            let (
                (
                    (((((base, resource), resource_version), file), inputs), tensor_outputs),
                    flush_each,
                ),
                message_error_policy,
            ) = value;
            let (
                (((((if_not_exists, mode), name), from_relay), filter_where), processor_outputs),
                parameterized_by,
            ) = base;
            let (flush_each, max_batch_size) = flush_each;
            CreateStatement::new(
                CreateInferencer {
                    name,
                    from_relay,
                    output_routes: processor_outputs,
                    parameterized_by,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    outputs: tensor_outputs,
                    flush_each,
                    max_batch_size,
                    message_error_policy,
                    mode: mode.unwrap_or(AckMode::Attached),
                    filter_where,
                },
                if_not_exists,
            )
        })
}

pub fn parse_create_inferencer_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateInferencer>, Vec<ParseError<'_>>> {
    let out = create_inferencer_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_inferencer(
    input: &str,
) -> Result<CreateStatement<CreateInferencer>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_inferencer_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_inferencer(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_inferencer_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

#[cfg(test)]
mod tests {
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
    fn parses_create_inferencer() {
        let input = r#"
            CREATE DETACHED INFERENCER score_model
            FROM features FILTER WHERE features.present
            TO scored SET scored.ready = true
            PARAMETERIZED BY tenant
            USING RESOURCE fraud_model VERSION 3
            FILE 'models/fraud.onnx'
            INPUTS { "features" = features.vector }
            OUTPUTS { "score" = scored.score }
            FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        "#;

        let parsed = parse_create_inferencer_tokens(&to_tokens(input)).expect("parse should work");

        assert_eq!(parsed.name.as_str(), "score_model");
        assert_eq!(parsed.from_relay.as_str(), "features");
        assert_eq!(
            parsed
                .output_routes
                .routes
                .first()
                .expect("output route should parse")
                .relay
                .as_str(),
            "scored"
        );
        assert_eq!(parsed.resource.as_str(), "fraud_model");
        assert_eq!(parsed.resource_version, Some(3));
        assert_eq!(parsed.file, "models/fraud.onnx");
        assert_eq!(parsed.mode, AckMode::Detached);
        assert_eq!(parsed.flush_each, "100ms");
        assert_eq!(parsed.inputs[0].tensor, "features");
        assert_eq!(parsed.inputs[0].relay.as_str(), "features");
        assert_eq!(parsed.inputs[0].field.as_str(), "vector");
        assert_eq!(parsed.outputs[0].tensor, "score");
        assert_eq!(parsed.outputs[0].relay.as_str(), "scored");
        assert_eq!(parsed.outputs[0].field.as_str(), "score");
    }

    #[test]
    fn rejects_inferencer_without_flush_policy() {
        let input = r#"
            CREATE INFERENCER p FROM a TO b UNPARAMETERIZED USING RESOURCE r FILE 'm.onnx'
            INPUTS { "x" = a.x } OUTPUTS { "y" = b.y } ON MESSAGE ERROR LOG;
        "#;
        assert!(parse_create_inferencer_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn rejects_legacy_parenthesized_tensor_mappings() {
        let input = r#"
            CREATE INFERENCER p FROM a TO b UNPARAMETERIZED USING RESOURCE r FILE 'm.onnx'
            INPUTS ("x" = a.x) OUTPUTS ("y" = b.y)
            FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        "#;
        assert!(parse_create_inferencer_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn suggests_inputs_after_filter_map_without_schema_leakage() {
        let input = "CREATE INFERENCER p FROM a TO b PARAMETERIZED BY tenant USING RESOURCE r \
                     FILE 'm.onnx' ";
        let suggestions = suggest_create_inferencer(input, input.len());
        assert!(suggestions.contains(&"INPUTS".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_braced_tensor_mapping_without_branch_value_leakage() {
        let input = "CREATE INFERENCER p FROM a TO b UNPARAMETERIZED USING RESOURCE r FILE \
                     'm.onnx' INPUTS ";
        let suggestions = suggest_create_inferencer(input, input.len());
        assert!(suggestions.contains(&"{".to_string()));
        assert!(!suggestions.contains(&"VALUES".to_string()));
        assert!(!suggestions.contains(&"PARAMETERIZED BY".to_string()));
    }
}
