use chumsky::prelude::*;
use nervix_models::{
    AckMode, CreateInferencer, CreateStatement, InferencerTensorDeclaration,
    InferencerTensorDimension, InferencerTensorElementType, InferencerTensorMapping,
    InferencerTensorRepresentation, InferencerTensorSchema,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, ack_mode, branch_selection, current_word_prefix,
        filter_where_clause, flushed_processor_outputs, from_relay_clauses, if_not_exists_clause,
        inferencer_name, into_parse_error, kw, kw_phrase2, lex_input,
        materialized_state_dependencies, render_vm_program_tokens, resource_ref, string_lit,
        suggestions_from_errors, tok, vm_program_error_message, word_raw,
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
        .then(tensor_schema())
        .then_ignore(tok(Token::Eq))
        .then(expression_tokens())
        .try_map(|((tensor, schema), tokens), span| {
            crate::parse_expression(&render_vm_program_tokens(&tokens))
                .map(|expression| InferencerTensorMapping {
                    tensor,
                    schema,
                    expression,
                })
                .map_err(|error| Rich::custom(span, vm_program_error_message(error)))
        })
}

fn balanced_expression_group<'src>()
-> impl Parser<'src, &'src [Token], Vec<Token>, extra::Err<ParseError<'src>>> + Clone {
    recursive(|element| {
        let contents = element
            .repeated()
            .collect::<Vec<_>>()
            .map(|parts| parts.into_iter().flatten().collect::<Vec<_>>());
        let parens = contents
            .clone()
            .delimited_by(tok(Token::LParen), tok(Token::RParen))
            .map(|mut tokens| {
                tokens.insert(0, Token::LParen);
                tokens.push(Token::RParen);
                tokens
            });
        let brackets = contents
            .clone()
            .delimited_by(tok(Token::LBracket), tok(Token::RBracket))
            .map(|mut tokens| {
                tokens.insert(0, Token::LBracket);
                tokens.push(Token::RBracket);
                tokens
            });
        let braces = contents
            .delimited_by(tok(Token::LBrace), tok(Token::RBrace))
            .map(|mut tokens| {
                tokens.insert(0, Token::LBrace);
                tokens.push(Token::RBrace);
                tokens
            });
        let leaf = any()
            .filter(|token: &Token| {
                !matches!(
                    token,
                    Token::LParen
                        | Token::RParen
                        | Token::LBracket
                        | Token::RBracket
                        | Token::LBrace
                        | Token::RBrace
                )
            })
            .map(|token| vec![token]);
        choice((parens, brackets, braces, leaf))
    })
}

fn expression_tokens<'src>()
-> impl Parser<'src, &'src [Token], Vec<Token>, extra::Err<ParseError<'src>>> + Clone {
    let balanced =
        balanced_expression_group().filter(|tokens| !matches!(tokens.as_slice(), [Token::Comma]));
    balanced
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .map(|parts| parts.into_iter().flatten().collect())
}

fn tensor_schema<'src>()
-> impl Parser<'src, &'src [Token], InferencerTensorSchema, extra::Err<ParseError<'src>>> + Clone {
    let dimension = choice((
        kw(Identifier::Batch).to(InferencerTensorDimension::Batch),
        kw(Identifier::Dynamic).to(InferencerTensorDimension::Dynamic),
        select! { Token::NumberLiteral(raw) => raw }.try_map(|raw, span| {
            let size = raw.parse::<u32>().map_err(|_| {
                Rich::custom(
                    span,
                    "tensor dimension must be a positive integer, DYNAMIC, or BATCH",
                )
            })?;
            if size == 0 {
                return Err(Rich::custom(
                    span,
                    "tensor dimension must be greater than zero",
                ));
            }
            Ok(InferencerTensorDimension::Fixed(size))
        }),
    ));

    kw(Identifier::Dense)
        .to(InferencerTensorRepresentation::Dense)
        .then_ignore(kw(Identifier::Tensor))
        .then_ignore(tok(Token::Lt))
        .then(kw(Identifier::F32).to(InferencerTensorElementType::F32))
        .then_ignore(tok(Token::Gt))
        .then(
            dimension
                .separated_by(tok(Token::Comma))
                .collect::<Vec<_>>()
                .delimited_by(tok(Token::LBracket), tok(Token::RBracket)),
        )
        .map(
            |((representation, element_type), dimensions)| InferencerTensorSchema {
                representation,
                element_type,
                dimensions,
            },
        )
}

fn input_mappings<'src>()
-> impl Parser<'src, &'src [Token], Vec<InferencerTensorMapping>, extra::Err<ParseError<'src>>> + Clone
{
    kw(Identifier::Inputs)
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

fn output_declaration<'src>()
-> impl Parser<'src, &'src [Token], InferencerTensorDeclaration, extra::Err<ParseError<'src>>> + Clone
{
    string_lit()
        .then(tensor_schema())
        .map(|(tensor, schema)| InferencerTensorDeclaration { tensor, schema })
}

fn output_schema<'src>()
-> impl Parser<'src, &'src [Token], Vec<InferencerTensorDeclaration>, extra::Err<ParseError<'src>>>
+ Clone {
    kw_phrase2(Identifier::Output, Identifier::Schema)
        .ignore_then(tok(Token::LBrace))
        .ignore_then(
            output_declaration()
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
        .then(from_relay_clauses())
        .then(filter_where_clause().or_not())
        .boxed()
        .then_ignore(kw(Identifier::Using))
        .then_ignore(kw(Identifier::Resource))
        .then(resource_ref())
        .then(kw(Identifier::Version).ignore_then(u64_value()).or_not())
        .then_ignore(kw(Identifier::File))
        .then(string_lit())
        .boxed()
        .then(input_mappings())
        .then(output_schema())
        .then(branch_selection())
        .then(materialized_state_dependencies())
        .boxed()
        .then(flushed_processor_outputs())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|value| {
            let (
                (
                    (
                        (((((base, resource), resource_version), file), inputs), output_schema),
                        branched_by,
                    ),
                    materialized_state,
                ),
                processor_outputs,
            ) = value;
            let ((((if_not_exists, mode), name), from_input), filter_where) = base;
            CreateStatement::new(
                CreateInferencer {
                    name,
                    from: from_input,
                    output_routes: processor_outputs,
                    branched_by,
                    resource,
                    resource_version,
                    file,
                    inputs,
                    output_schema,
                    mode: mode.unwrap_or(AckMode::Attached),
                    filter_where,
                    materialized_state,
                },
                if_not_exists,
            )
        })
        .boxed()
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
            FROM features FILTER WHERE input.present
            USING RESOURCE fraud_model VERSION 3
            FILE 'models/fraud.onnx'
            INPUTS { "features" DENSE TENSOR<F32>[BATCH, 2] = input.vector }
            OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[BATCH, 1] }
            BRANCHED BY tenant
            TO scored SET ready = true, score = score FLUSH EACH 100ms MAX BATCH SIZE 1MiB
            ON MESSAGE ERROR LOG
            TO audited SET model_input = score
            FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        "#;

        let parsed = parse_create_inferencer_tokens(&to_tokens(input)).expect("parse should work");

        assert_eq!(parsed.name.as_str(), "score_model");
        assert_eq!(parsed.from.from[0].as_str(), "features");
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
        assert_eq!(
            parsed.output_routes.routes[0]
                .flush_policy
                .as_ref()
                .expect("output flush policy should parse")
                .flush_each,
            "100ms"
        );
        assert_eq!(parsed.inputs[0].tensor, "features");
        assert_eq!(
            parsed.inputs[0].expression,
            crate::parse_expression("input.vector").expect("valid expression")
        );
        assert_eq!(parsed.output_schema[0].tensor, "score");
        assert_eq!(parsed.output_routes.routes.len(), 2);
        assert_eq!(
            parsed.output_routes.routes[0]
                .construction
                .assignments
                .len(),
            2
        );
        assert_eq!(
            parsed.output_routes.routes[1]
                .construction
                .assignments
                .len(),
            1
        );
    }

    #[test]
    fn rejects_legacy_outputs_mapping() {
        let input = r#"
            CREATE INFERENCER p FROM a TO b FLUSH IMMEDIATE SET b.y = inner_output.y ON MESSAGE ERROR LOG UNBRANCHED
            USING RESOURCE r FILE 'm.onnx'
            INPUTS { "x" DENSE TENSOR<F32>[1] = a.x }
            OUTPUTS { "y" DENSE TENSOR<F32>[1] = b.y };
        "#;
        assert!(parse_create_inferencer_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn parses_scalar_fixed_dynamic_and_non_leading_batch_tensor_dimensions() {
        let input = r#"
            CREATE INFERENCER p FROM a USING RESOURCE r FILE 'm.onnx'
            INPUTS {
                "scalar" DENSE TENSOR<F32>[] = input.scalar,
                "image" DENSE TENSOR<F32>[3, 224, 224] = input.image,
                "sequence" DENSE TENSOR<F32>[DYNAMIC, 64] = input.sequence,
                "tokens" DENSE TENSOR<F32>[128, BATCH] = input.tokens
            }
            OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[10, BATCH] }
            UNBRANCHED
            TO b SET score = score FLUSH EACH 10ms MAX BATCH SIZE 16mb
            ON MESSAGE ERROR LOG;
        "#;

        let parsed = parse_create_inferencer_tokens(&to_tokens(input)).expect("parse should work");

        assert!(parsed.inputs[0].schema.dimensions.is_empty());
        assert_eq!(
            parsed.inputs[1].schema.dimensions,
            vec![
                nervix_models::InferencerTensorDimension::Fixed(3),
                nervix_models::InferencerTensorDimension::Fixed(224),
                nervix_models::InferencerTensorDimension::Fixed(224),
            ]
        );
        assert_eq!(
            parsed.inputs[2].schema.dimensions,
            vec![
                nervix_models::InferencerTensorDimension::Dynamic,
                nervix_models::InferencerTensorDimension::Fixed(64),
            ]
        );
        assert_eq!(parsed.inputs[3].schema.batch_axis(), Some(1));
        assert_eq!(parsed.output_schema[0].schema.batch_axis(), Some(1));
    }

    #[test]
    fn rejects_zero_sized_tensor_dimension() {
        let input = r#"
            CREATE INFERENCER p FROM a TO b FLUSH IMMEDIATE ON MESSAGE ERROR LOG UNBRANCHED USING RESOURCE r FILE 'm.onnx'
            INPUTS { "x" DENSE TENSOR<F32>[0] = a.x }
            OUTPUT SCHEMA { "y" DENSE TENSOR<F32>[1] };
        "#;
        assert!(parse_create_inferencer_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn rejects_unsupported_tensor_representation_and_element_type() {
        let sparse = r#"
            CREATE INFERENCER p FROM a TO b FLUSH IMMEDIATE ON MESSAGE ERROR LOG UNBRANCHED USING RESOURCE r FILE 'm.onnx'
            INPUTS { "x" SPARSE TENSOR<F32>[1] = a.x }
            OUTPUT SCHEMA { "y" DENSE TENSOR<F32>[1] };
        "#;
        let f64 = r#"
            CREATE INFERENCER p FROM a TO b FLUSH IMMEDIATE ON MESSAGE ERROR LOG UNBRANCHED USING RESOURCE r FILE 'm.onnx'
            INPUTS { "x" DENSE TENSOR<F64>[1] = a.x }
            OUTPUT SCHEMA { "y" DENSE TENSOR<F32>[1] };
        "#;
        assert!(parse_create_inferencer_tokens(&to_tokens(sparse)).is_err());
        assert!(parse_create_inferencer_tokens(&to_tokens(f64)).is_err());
    }

    #[test]
    fn rejects_binding_without_complete_tensor_schema() {
        let input = r#"
            CREATE INFERENCER p FROM a TO b FLUSH IMMEDIATE ON MESSAGE ERROR LOG UNBRANCHED USING RESOURCE r FILE 'm.onnx'
            INPUTS { "x" = a.x }
            OUTPUT SCHEMA { "y" DENSE TENSOR<F32>[1] };
        "#;
        assert!(parse_create_inferencer_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn rejects_inferencer_without_flush_policy() {
        let input = r#"
            CREATE INFERENCER p FROM a TO b ON MESSAGE ERROR LOG UNBRANCHED USING RESOURCE r FILE 'm.onnx'
            INPUTS { "x" DENSE TENSOR<F32>[1] = a.x }
            OUTPUT SCHEMA { "y" DENSE TENSOR<F32>[1] };
        "#;
        assert!(parse_create_inferencer_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn rejects_legacy_parenthesized_tensor_mappings() {
        let input = r#"
            CREATE INFERENCER p FROM a TO b FLUSH IMMEDIATE ON MESSAGE ERROR LOG UNBRANCHED USING RESOURCE r FILE 'm.onnx'
            INPUTS ("x" DENSE TENSOR<F32>[1] = a.x)
            OUTPUT SCHEMA { "y" DENSE TENSOR<F32>[1] };
        "#;
        assert!(parse_create_inferencer_tokens(&to_tokens(input)).is_err());
    }

    #[test]
    fn suggests_inputs_after_filter_map_without_schema_leakage() {
        let input = "CREATE INFERENCER p FROM a USING RESOURCE r FILE 'm.onnx' ";
        let suggestions = suggest_create_inferencer(input, input.len());
        assert!(suggestions.contains(&"INPUTS".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_braced_tensor_mapping_without_branch_value_leakage() {
        let input = "CREATE INFERENCER p FROM a USING RESOURCE r FILE 'm.onnx' INPUTS ";
        let suggestions = suggest_create_inferencer(input, input.len());
        assert!(suggestions.contains(&"{".to_string()));
        assert!(!suggestions.contains(&"VALUES".to_string()));
        assert!(!suggestions.contains(&"BRANCHED BY".to_string()));
    }

    #[test]
    fn suggests_dense_tensor_schema_without_output_keyword_leakage() {
        let input = "CREATE INFERENCER p FROM a USING RESOURCE r FILE 'm.onnx' INPUTS { \"x\" ";
        let suggestions = suggest_create_inferencer(input, input.len());
        assert!(suggestions.contains(&"DENSE".to_string()));
        assert!(!suggestions.contains(&"OUTPUT SCHEMA".to_string()));
        assert!(!suggestions.contains(&"BRANCHED BY".to_string()));
    }

    #[test]
    fn suggests_composed_output_schema_without_flush_leakage() {
        let input = "CREATE INFERENCER p FROM a USING RESOURCE r FILE 'm.onnx' INPUTS { \"x\" \
                     DENSE TENSOR<F32>[1] = input.x } ";
        let suggestions = suggest_create_inferencer(input, input.len());
        assert!(suggestions.contains(&"OUTPUT SCHEMA".to_string()));
        assert!(!suggestions.contains(&"OUTPUT_SCHEMA".to_string()));
        assert!(!suggestions.contains(&"FLUSH".to_string()));
    }

    #[test]
    fn rejects_output_schema_mapping() {
        let input = r#"
            CREATE INFERENCER p FROM a TO b FLUSH IMMEDIATE SET b.y = inner_output.y ON MESSAGE ERROR LOG UNBRANCHED
            USING RESOURCE r FILE 'm.onnx'
            INPUTS { "x" DENSE TENSOR<F32>[1] = a.x }
            OUTPUT SCHEMA { "y" DENSE TENSOR<F32>[1] = b.y };
        "#;
        assert!(parse_create_inferencer_tokens(&to_tokens(input)).is_err());
    }
}
