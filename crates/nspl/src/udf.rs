use chumsky::prelude::*;
use nervix_models::{
    CreateStatement, CreateUdf, DescribeUdf, ShowUdfs, UdfArgument, UdfLanguage, UdfReturn,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, if_not_exists_clause,
        into_parse_error, kw, lex_input, string_lit, suggestions_from_errors, tok, udf_name,
        udf_ref,
    },
    schema::nervix_type,
};

pub fn create_udf_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateUdf>, extra::Err<ParseError<'src>>> + Clone
{
    let argument = udf_name()
        .then(nervix_type())
        .then(kw(Identifier::Optional).or_not())
        .map(|((name, ty), optional)| UdfArgument {
            name,
            ty,
            optional: optional.is_some(),
        })
        .boxed();
    let arguments = argument
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .at_most(8)
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LParen), tok(Token::RParen))
        .boxed();
    let returns = nervix_type()
        .then(kw(Identifier::Optional).or_not())
        .map(|(ty, optional)| UdfReturn {
            ty,
            optional: optional.is_some(),
        })
        .boxed();
    let language = kw(Identifier::Roto0_11).to(UdfLanguage::Roto0_11);

    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Udf))
        .then(udf_name())
        .then_ignore(kw(Identifier::With))
        .then(language)
        .then_ignore(kw(Identifier::Args))
        .then(arguments)
        .boxed()
        .then_ignore(kw(Identifier::Returns))
        .then(returns)
        .then(kw(Identifier::Volatile).or_not())
        .then_ignore(kw(Identifier::Code))
        .then(string_lit())
        .boxed()
        .try_map(
            |((((((if_not_exists, name), language), arguments), returns), volatile), code),
             span| {
                if name.as_str().starts_with("__nervix_") {
                    return Err(Rich::custom(
                        span,
                        "UDF names beginning with '__nervix_' are reserved",
                    ));
                }
                for (index, argument) in arguments.iter().enumerate() {
                    if argument.name.as_str().starts_with("__nervix_") {
                        return Err(Rich::custom(
                            span,
                            "UDF argument names beginning with '__nervix_' are reserved",
                        ));
                    }
                    if arguments[..index]
                        .iter()
                        .any(|earlier| earlier.name == argument.name)
                    {
                        return Err(Rich::custom(
                            span,
                            format!("duplicate UDF argument '{}'", argument.name.as_str()),
                        ));
                    }
                }
                if code.len() > 64 * 1024 {
                    return Err(Rich::custom(span, "UDF code exceeds the 64 KiB limit"));
                }

                Ok(CreateStatement::new(
                    CreateUdf::new(name, language, arguments, returns, volatile.is_some(), code),
                    if_not_exists,
                ))
            },
        )
        .then_ignore(tok(Token::Semicolon).or_not())
        .boxed()
}

pub fn describe_udf_parser<'src>()
-> impl Parser<'src, &'src [Token], DescribeUdf, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Describe)
        .ignore_then(kw(Identifier::Udf))
        .ignore_then(udf_ref())
        .map(|name| DescribeUdf { name })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn show_udfs_parser<'src>()
-> impl Parser<'src, &'src [Token], ShowUdfs, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Show)
        .ignore_then(kw(Identifier::Udfs))
        .to(ShowUdfs)
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn parse_create_udf(input: &str) -> Result<CreateStatement<CreateUdf>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    let output = create_udf_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if output.has_errors() {
        Err(into_parse_error(
            source,
            &spanned_tokens,
            input.len(),
            output.into_errors(),
        ))
    } else {
        Ok(output
            .into_output()
            .expect("successful UDF parse must have output"))
    }
}

pub fn suggest_create_udf(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);
    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(value) => value,
        Err(_) => return Vec::new(),
    };
    let output = create_udf_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if output.has_errors() {
        suggestions_from_errors(output.into_errors(), &prefix)
    } else {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{Model, ParseAsType, Statement};

    use super::*;
    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn parses_multiline_udf_with_exact_signature_and_source() {
        let source = "CREATE UDF display_name
            WITH ROTO_0_11
            ARGS (nick STRING OPTIONAL, score ARRAY<F64, 3>)
            RETURNS STRING OPTIONAL
            VOLATILE
            CODE $roto$
fn display_name(nick: StringColumn, score: VecF64Column) -> StringColumn {
    nick
}
$roto$;";
        let parsed = parse_statement(source).expect("UDF should parse");
        let Statement::Create(create) = parsed else {
            panic!("expected CREATE model");
        };
        let Model::Udf(udf) = create.body.as_ref() else {
            panic!("expected UDF model");
        };
        assert_eq!(udf.name.as_str(), "display_name");
        assert_eq!(udf.arguments.len(), 2);
        assert!(udf.arguments[0].optional);
        assert_eq!(
            udf.arguments[1].ty,
            ParseAsType::Array {
                element: Box::new(ParseAsType::F64),
                len: 3,
            }
        );
        assert!(udf.returns.optional);
        assert!(udf.volatile);
        assert_eq!(
            udf.code,
            "\nfn display_name(nick: StringColumn, score: VecF64Column) -> StringColumn {\n    \
             nick\n}\n"
        );
        assert!(udf.has_valid_code_hash());
    }

    #[test]
    fn rejects_empty_duplicate_and_over_limit_argument_lists() {
        assert!(
            parse_create_udf("CREATE UDF f WITH ROTO_0_11 ARGS () RETURNS I64 CODE $$fn f() {}$$;")
                .is_err()
        );
        assert!(
            parse_create_udf(
                "CREATE UDF f WITH ROTO_0_11 ARGS (x I64, x I64) RETURNS I64 CODE $$x$$;"
            )
            .is_err()
        );
        assert!(
            parse_create_udf(
                "CREATE UDF f WITH ROTO_0_11 ARGS (a I64,b I64,c I64,d I64,e I64,f I64,g I64,h \
                 I64,i I64) RETURNS I64 CODE $$x$$;"
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_unknown_language_tag() {
        assert!(
            parse_create_udf("CREATE UDF f WITH ROTO_0_12 ARGS (x I64) RETURNS I64 CODE $$x$$;")
                .is_err()
        );
    }

    #[test]
    fn completion_stays_on_the_composed_udf_grammar_branch() {
        let suggestions = suggest_statement("CREATE UDF f WITH ", "CREATE UDF f WITH ".len());
        assert!(suggestions.contains(&"ROTO_0_11".to_string()));
        assert!(!suggestions.contains(&"KAFKA".to_string()));
    }
}
