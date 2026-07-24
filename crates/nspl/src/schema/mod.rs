use chumsky::prelude::*;
use nervix_models::{
    AvroType, CreateAvroWireSchema, CreateCborWireSchema, CreateJsonWireSchema, CreateSchema,
    CreateStatement, CreateWireSchema, CreateWireSchemaStmt, JsonType, ParseAsType, SchemaField,
    WireSchemaField, WireSchemaStrictness,
};

pub use crate::parser_support::{Diagnostic, ParseFromSourceError};
use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, boxed_choice, current_word_prefix, field_ref, if_not_exists_clause,
        into_parse_error, kw, lex_input, schema_name, suggestions_from_errors, tok,
        wire_schema_name,
    },
};

fn json_type<'src>()
-> impl Parser<'src, &'src [Token], JsonType, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        kw(Identifier::String).to(JsonType::String),
        kw(Identifier::Number).to(JsonType::Number),
        kw(Identifier::Integer).to(JsonType::Integer),
        kw(Identifier::Object).to(JsonType::Object),
        kw(Identifier::Array).to(JsonType::Array),
        kw(Identifier::Boolean).to(JsonType::Boolean),
        kw(Identifier::Null).to(JsonType::Null),
    )
}

fn cbor_type<'src>()
-> impl Parser<'src, &'src [Token], JsonType, extra::Err<ParseError<'src>>> + Clone {
    json_type()
}

fn avro_type<'src>()
-> impl Parser<'src, &'src [Token], AvroType, extra::Err<ParseError<'src>>> + Clone {
    boxed_choice!(
        kw(Identifier::Null).to(AvroType::Null),
        kw(Identifier::Boolean).to(AvroType::Boolean),
        kw(Identifier::Int).to(AvroType::Int),
        kw(Identifier::Long).to(AvroType::Long),
        kw(Identifier::Float).to(AvroType::Float),
        kw(Identifier::Double).to(AvroType::Double),
        kw(Identifier::Bytes).to(AvroType::Bytes),
        kw(Identifier::String).to(AvroType::String),
        kw(Identifier::Record).to(AvroType::Record),
        kw(Identifier::Enum).to(AvroType::Enum),
        kw(Identifier::Array).to(AvroType::Array),
        kw(Identifier::Map).to(AvroType::Map),
        kw(Identifier::Fixed).to(AvroType::Fixed),
    )
}

pub(crate) fn nervix_type<'src>()
-> impl Parser<'src, &'src [Token], ParseAsType, extra::Err<ParseError<'src>>> + Clone {
    recursive(|ty| {
        let scalar = boxed_choice!(
            kw(Identifier::U8).to(ParseAsType::U8),
            kw(Identifier::I8).to(ParseAsType::I8),
            kw(Identifier::U16).to(ParseAsType::U16),
            kw(Identifier::I16).to(ParseAsType::I16),
            kw(Identifier::U32).to(ParseAsType::U32),
            kw(Identifier::I32).to(ParseAsType::I32),
            kw(Identifier::U64).to(ParseAsType::U64),
            kw(Identifier::I64).to(ParseAsType::I64),
            kw(Identifier::Bool).to(ParseAsType::Bool),
            kw(Identifier::String).to(ParseAsType::String),
            kw(Identifier::Datetime).to(ParseAsType::Datetime),
            kw(Identifier::F32).to(ParseAsType::F32),
            kw(Identifier::F64).to(ParseAsType::F64),
        );

        let array_len = select! { Token::NumberLiteral(raw) => raw }.try_map(|raw, span| {
            let len = raw
                .parse::<u32>()
                .map_err(|_| Rich::custom(span, "array length must be an unsigned integer"))?;
            if len == 0 {
                return Err(Rich::custom(span, "array length must be greater than zero"));
            }
            Ok(len)
        });

        let array = kw(Identifier::Array).ignore_then(
            ty.clone()
                .then_ignore(tok(Token::Comma))
                .then(array_len)
                .then(
                    tok(Token::Comma)
                        .ignore_then(array_len)
                        .repeated()
                        .collect::<Vec<_>>(),
                )
                .map(|((element, first_len), remaining_lengths)| {
                    let mut lengths = Vec::with_capacity(remaining_lengths.len() + 1);
                    lengths.push(first_len);
                    lengths.extend(remaining_lengths);
                    lengths
                        .into_iter()
                        .rev()
                        .fold(element, |element, len| ParseAsType::Array {
                            element: Box::new(element),
                            len,
                        })
                })
                .delimited_by(tok(Token::Lt), tok(Token::Gt)),
        );

        let vec_ty = kw(Identifier::Vec).ignore_then(
            ty.clone()
                .map(|element| ParseAsType::Vec {
                    element: Box::new(element),
                })
                .delimited_by(tok(Token::Lt), tok(Token::Gt)),
        );

        choice((array, vec_ty, scalar)).boxed()
    })
}

fn wire_schema_field<'src, T, P>(
    native_type: P,
) -> impl Parser<'src, &'src [Token], WireSchemaField<T>, extra::Err<ParseError<'src>>> + Clone
where
    T: Clone + 'src,
    P: Parser<'src, &'src [Token], T, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    field_ref()
        .then(native_type)
        .then(kw(Identifier::Optional).or_not())
        .map(|((name, ty), optional)| WireSchemaField {
            name,
            ty,
            optional: optional.is_some(),
        })
}

#[derive(Default)]
struct InternalSchemaFieldModifiers {
    optional: bool,
    sensitive: bool,
}

fn internal_schema_field_modifiers<'src>()
-> impl Parser<'src, &'src [Token], InternalSchemaFieldModifiers, extra::Err<ParseError<'src>>> + Clone
{
    choice((
        kw(Identifier::Optional).to(Identifier::Optional),
        kw(Identifier::Sensitive).to(Identifier::Sensitive),
    ))
    .repeated()
    .collect::<Vec<_>>()
    .try_map(|modifiers, span| {
        let optional_count = modifiers
            .iter()
            .filter(|modifier| **modifier == Identifier::Optional)
            .count();
        let sensitive_count = modifiers
            .iter()
            .filter(|modifier| **modifier == Identifier::Sensitive)
            .count();
        if optional_count > 1 {
            return Err(Rich::custom(
                span,
                "schema field modifier OPTIONAL may appear at most once",
            ));
        }
        if sensitive_count > 1 {
            return Err(Rich::custom(
                span,
                "schema field modifier SENSITIVE may appear at most once",
            ));
        }
        Ok(InternalSchemaFieldModifiers {
            optional: optional_count == 1,
            sensitive: sensitive_count == 1,
        })
    })
}

fn internal_schema_field<'src>()
-> impl Parser<'src, &'src [Token], SchemaField, extra::Err<ParseError<'src>>> + Clone {
    field_ref()
        .then(nervix_type())
        .then(internal_schema_field_modifiers())
        .map(|((name, ty), modifiers)| SchemaField {
            name,
            ty,
            optional: modifiers.optional,
            sensitive: modifiers.sensitive,
        })
}

fn create_wire_schema_parser<'src, T, P>(
    format_kw: Identifier,
    native_type: P,
) -> impl Parser<'src, &'src [Token], CreateStatement<CreateWireSchema<T>>, extra::Err<ParseError<'src>>>
+ Clone
where
    T: Clone + 'src,
    P: Parser<'src, &'src [Token], T, extra::Err<ParseError<'src>>> + Clone + 'src,
{
    let fields = wire_schema_field(native_type)
        .separated_by(tok(Token::Comma))
        .allow_trailing()
        .at_least(1)
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LParen), tok(Token::RParen));

    let strictness = choice((
        kw(Identifier::Strict).to(WireSchemaStrictness::Strict),
        kw(Identifier::Loose).to(WireSchemaStrictness::Loose),
    ));

    let wire_schema_header = strictness
        .then_ignore(kw(Identifier::Wire))
        .then_ignore(kw(format_kw))
        .then_ignore(kw(Identifier::Schema));

    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(wire_schema_header)
        .then(wire_schema_name())
        .boxed()
        .then(fields)
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(((if_not_exists, strictness), name), fields)| {
            CreateStatement::new(
                CreateWireSchema {
                    name,
                    strictness,
                    fields,
                },
                if_not_exists,
            )
        })
        .boxed()
}

pub fn create_json_wire_schema_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateJsonWireSchema>, extra::Err<ParseError<'src>>>
+ Clone {
    create_wire_schema_parser(Identifier::Json, json_type())
}

pub fn create_cbor_wire_schema_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateCborWireSchema>, extra::Err<ParseError<'src>>>
+ Clone {
    create_wire_schema_parser(Identifier::Cbor, cbor_type())
}

pub fn create_avro_wire_schema_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateAvroWireSchema>, extra::Err<ParseError<'src>>>
+ Clone {
    create_wire_schema_parser(Identifier::Avro, avro_type())
}

pub fn create_wire_schema_parser_any<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateWireSchemaStmt>, extra::Err<ParseError<'src>>>
+ Clone {
    boxed_choice!(
        create_json_wire_schema_parser().map(|create| create.map_body(CreateWireSchemaStmt::Json)),
        create_cbor_wire_schema_parser().map(|create| create.map_body(CreateWireSchemaStmt::Cbor)),
        create_avro_wire_schema_parser().map(|create| create.map_body(CreateWireSchemaStmt::Avro)),
    )
}

pub fn create_schema_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateSchema>, extra::Err<ParseError<'src>>> + Clone
{
    let fields = internal_schema_field()
        .separated_by(tok(Token::Comma))
        .allow_trailing()
        .at_least(1)
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LParen), tok(Token::RParen));

    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Schema))
        .then(schema_name())
        .boxed()
        .then(fields)
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|((if_not_exists, name), fields)| {
            CreateStatement::new(CreateSchema { name, fields }, if_not_exists)
        })
        .boxed()
}

pub fn parse_create_wire_schema_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateWireSchemaStmt>, Vec<ParseError<'_>>> {
    let out = create_wire_schema_parser_any()
        .then_ignore(end())
        .parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_schema_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateSchema>, Vec<ParseError<'_>>> {
    let out = create_schema_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_wire_schema(
    input: &str,
) -> Result<CreateStatement<CreateWireSchemaStmt>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_wire_schema_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn parse_create_schema(
    input: &str,
) -> Result<CreateStatement<CreateSchema>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_schema_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_wire_schema(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_wire_schema_parser_any()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

pub fn suggest_create_schema(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_schema_parser()
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
    fn parses_internal_schema_definition() {
        let input = r#"
            CREATE SCHEMA notification (
                user_id U32,
                created_at DATETIME,
                payload STRING
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        assert_eq!(parsed.name.as_str(), "notification");
        assert_eq!(parsed.fields.len(), 3);
        assert_eq!(parsed.fields[0].ty, ParseAsType::U32);
    }

    #[test]
    fn parses_internal_schema_definition_with_if_not_exists() {
        let input = r#"
            CREATE IF NOT EXISTS SCHEMA notification (
                user_id U32
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        assert!(parsed.if_not_exists);
        assert_eq!(parsed.name.as_str(), "notification");
        assert_eq!(parsed.fields.len(), 1);
    }

    #[test]
    fn parses_optional_internal_schema_fields() {
        let input = r#"
            CREATE SCHEMA notification (
                user_id U32,
                nickname STRING OPTIONAL
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        assert!(!parsed.fields[0].optional);
        assert!(parsed.fields[1].optional);
    }

    #[test]
    fn parses_sensitive_internal_schema_fields() {
        let input = r#"
            CREATE SCHEMA notification (
                user_id U32,
                secret STRING SENSITIVE,
                token STRING OPTIONAL SENSITIVE
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        assert!(!parsed.fields[0].sensitive);
        assert!(parsed.fields[1].sensitive);
        assert!(!parsed.fields[1].optional);
        assert!(parsed.fields[2].sensitive);
        assert!(parsed.fields[2].optional);
    }

    #[test]
    fn rejects_duplicate_internal_schema_field_modifiers() {
        let input = "CREATE SCHEMA notification ( secret STRING SENSITIVE SENSITIVE );";
        assert!(parse_create_schema(input).is_err());
    }

    #[test]
    fn parses_internal_schema_array_and_vector_fields() {
        let input = r#"
            CREATE SCHEMA metrics (
                cpu_last_64 ARRAY<F32, 64>,
                labels VEC<STRING> OPTIONAL
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        assert_eq!(
            parsed.fields[0].ty,
            ParseAsType::Array {
                element: Box::new(ParseAsType::F32),
                len: 64
            }
        );
        assert_eq!(
            parsed.fields[1].ty,
            ParseAsType::Vec {
                element: Box::new(ParseAsType::String)
            }
        );
        assert!(parsed.fields[1].optional);
    }

    #[test]
    fn parses_multidimensional_and_mixed_internal_array_shapes() {
        let input = r#"
            CREATE SCHEMA tensors (
                matrix ARRAY<F32, 2, 3>,
                samples VEC<ARRAY<F32, 4>>,
                buckets ARRAY<VEC<I64>, 2>
            );
        "#;

        let parsed = parse_create_schema_tokens(&to_tokens(input)).expect("parse should succeed");
        assert_eq!(
            parsed.fields[0].ty,
            ParseAsType::Array {
                len: 2,
                element: Box::new(ParseAsType::Array {
                    len: 3,
                    element: Box::new(ParseAsType::F32),
                }),
            }
        );
        assert_eq!(
            parsed.fields[1].ty,
            ParseAsType::Vec {
                element: Box::new(ParseAsType::Array {
                    len: 4,
                    element: Box::new(ParseAsType::F32),
                }),
            }
        );
        assert_eq!(
            parsed.fields[2].ty,
            ParseAsType::Array {
                len: 2,
                element: Box::new(ParseAsType::Vec {
                    element: Box::new(ParseAsType::I64),
                }),
            }
        );
    }

    #[test]
    fn parses_array_and_vector_elements_for_all_internal_primitive_types() {
        let input = r#"
            CREATE SCHEMA metrics (
                u8_array ARRAY<U8, 2>,
                u8_vec VEC<U8>,
                i8_array ARRAY<I8, 2>,
                i8_vec VEC<I8>,
                u16_array ARRAY<U16, 2>,
                u16_vec VEC<U16>,
                i16_array ARRAY<I16, 2>,
                i16_vec VEC<I16>,
                u32_array ARRAY<U32, 2>,
                u32_vec VEC<U32>,
                i32_array ARRAY<I32, 2>,
                i32_vec VEC<I32>,
                u64_array ARRAY<U64, 2>,
                u64_vec VEC<U64>,
                i64_array ARRAY<I64, 2>,
                i64_vec VEC<I64>,
                bool_array ARRAY<BOOL, 2>,
                bool_vec VEC<BOOL>,
                string_array ARRAY<STRING, 2>,
                string_vec VEC<STRING>,
                datetime_array ARRAY<DATETIME, 2>,
                datetime_vec VEC<DATETIME>,
                f32_array ARRAY<F32, 2>,
                f32_vec VEC<F32>,
                f64_array ARRAY<F64, 2>,
                f64_vec VEC<F64>
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_schema_tokens(&tokens).expect("parse should succeed");
        let element_types = [
            ParseAsType::U8,
            ParseAsType::I8,
            ParseAsType::U16,
            ParseAsType::I16,
            ParseAsType::U32,
            ParseAsType::I32,
            ParseAsType::U64,
            ParseAsType::I64,
            ParseAsType::Bool,
            ParseAsType::String,
            ParseAsType::Datetime,
            ParseAsType::F32,
            ParseAsType::F64,
        ];

        for (index, element) in element_types.into_iter().enumerate() {
            assert_eq!(
                parsed.fields[index * 2].ty,
                ParseAsType::Array {
                    element: Box::new(element.clone()),
                    len: 2
                }
            );
            assert_eq!(
                parsed.fields[index * 2 + 1].ty,
                ParseAsType::Vec {
                    element: Box::new(element)
                }
            );
        }
    }

    #[test]
    fn rejects_internal_schema_zero_length_array() {
        let input = "CREATE SCHEMA metrics (cpu ARRAY<F32, 0>);";
        assert!(parse_create_schema(input).is_err());
    }

    #[test]
    fn rejects_zero_length_in_any_multidimensional_array_axis() {
        let input = "CREATE SCHEMA metrics (cpu ARRAY<F32, 2, 0>);";
        assert!(parse_create_schema(input).is_err());
    }

    #[test]
    fn rejects_empty_internal_schema_definition() {
        let input = "CREATE SCHEMA root_branch ();";
        assert!(parse_create_schema(input).is_err());
    }

    #[test]
    fn parses_json_wire_schema_definition() {
        let input = r#"
            CREATE STRICT WIRE JSON SCHEMA notification (
                user_id integer,
                created_at string,
                payload object
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_wire_schema_tokens(&tokens).expect("parse should succeed");
        let CreateWireSchemaStmt::Json(schema) = parsed.body else {
            panic!("expected JSON wire schema");
        };
        assert_eq!(schema.name.as_str(), "notification");
        assert_eq!(schema.strictness, WireSchemaStrictness::Strict);
        assert_eq!(schema.fields[0].ty, JsonType::Integer);
    }

    #[test]
    fn parses_strict_json_wire_schema_definition() {
        let input = r#"
            CREATE STRICT WIRE JSON SCHEMA notification (
                user_id integer,
                payload object
            );
        "#;

        let parsed = parse_create_wire_schema(input).expect("parse should succeed");
        let CreateWireSchemaStmt::Json(schema) = parsed.body else {
            panic!("expected JSON wire schema");
        };
        assert_eq!(schema.strictness, WireSchemaStrictness::Strict);
        assert_eq!(schema.fields[1].ty, JsonType::Object);
    }

    #[test]
    fn parses_loose_cbor_wire_schema_definition() {
        let input = r#"
            CREATE LOOSE WIRE CBOR SCHEMA notification (
                user_id integer,
                payload object OPTIONAL
            );
        "#;

        let parsed = parse_create_wire_schema(input).expect("parse should succeed");
        let CreateWireSchemaStmt::Cbor(schema) = parsed.body else {
            panic!("expected CBOR wire schema");
        };
        assert_eq!(schema.strictness, WireSchemaStrictness::Loose);
        assert_eq!(schema.fields[0].ty, JsonType::Integer);
        assert!(schema.fields[1].optional);
    }

    #[test]
    fn rejects_internal_types_in_json_wire_schema_definition() {
        let input = "CREATE STRICT WIRE JSON SCHEMA notification ( user_id U32 );";

        assert!(parse_create_wire_schema(input).is_err());
    }

    #[test]
    fn rejects_strictness_before_legacy_wire_schema_order() {
        let input = "CREATE STRICT JSON WIRE SCHEMA notification ( user_id integer );";

        assert!(parse_create_wire_schema(input).is_err());
    }

    #[test]
    fn rejects_legacy_wire_schema_order_without_strictness() {
        let input = "CREATE JSON WIRE SCHEMA notification ( user_id integer );";

        assert!(parse_create_wire_schema(input).is_err());
    }

    #[test]
    fn rejects_empty_wire_schema_definition() {
        let input = "CREATE STRICT WIRE JSON SCHEMA notification ();";
        assert!(parse_create_wire_schema(input).is_err());
    }

    #[test]
    fn parses_avro_wire_schema_definition() {
        let input = r#"
            CREATE STRICT WIRE AVRO SCHEMA latency_report (
                user_id long,
                created_at string,
                payload bytes
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_wire_schema_tokens(&tokens).expect("parse should succeed");
        let CreateWireSchemaStmt::Avro(schema) = parsed.body else {
            panic!("expected AVRO wire schema");
        };
        assert_eq!(schema.fields[0].ty, AvroType::Long);
    }

    #[test]
    fn parses_optional_wire_schema_fields() {
        let input = r#"
            CREATE STRICT WIRE JSON SCHEMA notification (
                user_id integer,
                nickname string OPTIONAL
            );
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_wire_schema_tokens(&tokens).expect("parse should succeed");
        let CreateWireSchemaStmt::Json(schema) = parsed.body else {
            panic!("expected JSON wire schema");
        };
        assert!(!schema.fields[0].optional);
        assert!(schema.fields[1].optional);
    }

    #[test]
    fn rejects_invalid_if_exists_clause_for_schema() {
        let input = "CREATE IF EXISTS SCHEMA notification ( user_id U32 );";
        assert!(parse_create_schema(input).is_err());
    }

    #[test]
    fn suggests_types_from_internal_schema_grammar() {
        let input = "CREATE SCHEMA s (id ";
        let suggestions = suggest_create_schema(input, input.len());
        assert!(suggestions.contains(&"U32".to_string()));
        assert!(suggestions.contains(&"STRING".to_string()));
        assert!(suggestions.contains(&"ARRAY".to_string()));
        assert!(suggestions.contains(&"VEC".to_string()));
    }

    #[test]
    fn suggests_internal_element_types_inside_array_without_wire_type_leakage() {
        let input = "CREATE SCHEMA s (matrix ARRAY<";
        let suggestions = suggest_create_schema(input, input.len());
        assert!(suggestions.contains(&"F32".to_string()));
        assert!(suggestions.contains(&"ARRAY".to_string()));
        assert!(suggestions.contains(&"VEC".to_string()));
        assert!(!suggestions.contains(&"NUMBER".to_string()));
        assert!(!suggestions.contains(&"OBJECT".to_string()));
    }

    #[test]
    fn suggests_optional_after_internal_field_type_without_cross_leakage() {
        let input = "CREATE SCHEMA s (id U32 ";
        let suggestions = suggest_create_schema(input, input.len());
        assert!(suggestions.contains(&"OPTIONAL".to_string()));
        assert!(suggestions.contains(&"SENSITIVE".to_string()));
        assert!(!suggestions.contains(&"NUMBER".to_string()));
    }

    #[test]
    fn suggests_sensitive_after_internal_optional_modifier_without_cross_leakage() {
        let input = "CREATE SCHEMA s (id U32 OPTIONAL ";
        let suggestions = suggest_create_schema(input, input.len());
        assert!(suggestions.contains(&"SENSITIVE".to_string()));
        assert!(!suggestions.contains(&"NUMBER".to_string()));
    }

    #[test]
    fn suggests_types_from_json_wire_schema_grammar() {
        let input = "CREATE STRICT WIRE JSON SCHEMA s (id ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert!(suggestions.contains(&"STRING".to_string()));
        assert!(suggestions.contains(&"NUMBER".to_string()));
        assert!(!suggestions.contains(&"U32".to_string()));
        assert!(!suggestions.contains(&"DATETIME".to_string()));
    }

    #[test]
    fn suggests_strictness_after_create() {
        let input = "CREATE ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert!(suggestions.contains(&"STRICT".to_string()));
        assert!(suggestions.contains(&"LOOSE".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"CBOR".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
        assert!(!suggestions.contains(&"U32".to_string()));
    }

    #[test]
    fn suggests_wire_formats_after_strict_wire() {
        let input = "CREATE STRICT WIRE ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert!(suggestions.contains(&"JSON".to_string()));
        assert!(suggestions.contains(&"CBOR".to_string()));
        assert!(suggestions.contains(&"AVRO".to_string()));
        assert!(!suggestions.contains(&"U32".to_string()));
    }

    #[test]
    fn suggests_types_from_cbor_wire_schema_grammar_without_internal_leakage() {
        let input = "CREATE LOOSE WIRE CBOR SCHEMA s (id ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert!(suggestions.contains(&"STRING".to_string()));
        assert!(suggestions.contains(&"INTEGER".to_string()));
        assert!(suggestions.contains(&"OBJECT".to_string()));
        assert!(!suggestions.contains(&"U32".to_string()));
        assert!(!suggestions.contains(&"DATETIME".to_string()));
    }

    #[test]
    fn suggests_optional_after_wire_field_type_without_cross_leakage() {
        let input = "CREATE STRICT WIRE JSON SCHEMA s (id STRING ";
        let suggestions = suggest_create_wire_schema(input, input.len());
        assert!(suggestions.contains(&"OPTIONAL".to_string()));
        assert!(!suggestions.contains(&"DATETIME".to_string()));
    }
}
