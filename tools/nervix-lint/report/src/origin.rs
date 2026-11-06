//! Repository tooling, outside the product layer order.
//! Owns: authored macro-invocation provenance at source byte offsets.
//! Depends on: the maintained Rust tokenizer and report expansion vocabulary.
//! Must not know: resolved APIs, acquisition policy or compiler internals.

use std::{ops::Range, str::FromStr as _};

use proc_macro2::{TokenStream, TokenTree};

use crate::Expansion;

struct Invocation {
    arguments: Range<usize>,
    expansion: Expansion,
}

/// Captured caller tokens retain their authored invocation even when compiler hygiene does not.
pub struct SourceOrigins {
    invocations: Vec<Invocation>,
}

impl SourceOrigins {
    pub fn parse(path: &str, source: &str) -> Result<Self, error_stack::Report<OriginError>> {
        let tokens = match TokenStream::from_str(source) {
            Ok(tokens) => tokens,
            Err(error) => {
                return Err(error_stack::Report::new(syn::Error::from(error))
                    .change_context(OriginError::Tokenize { path: path.into() }));
            }
        };
        let mut origins = Self {
            invocations: Vec::new(),
        };
        origins.visit(path, tokens);
        Ok(origins)
    }

    pub fn at(&self, byte_offset: usize) -> Vec<Expansion> {
        let mut expansions = Vec::new();
        for invocation in &self.invocations {
            if invocation.arguments.contains(&byte_offset) {
                expansions.push(invocation.expansion.clone());
            }
        }
        expansions
    }

    fn visit(&mut self, path: &str, tokens: TokenStream) {
        let tokens: Vec<_> = tokens.into_iter().collect();
        let mut index = 0;
        while index < tokens.len() {
            let start = index;
            if let TokenTree::Ident(first) = &tokens[index] {
                let mut name = first.to_string();
                let mut end = index + 1;
                while let [
                    TokenTree::Punct(left),
                    TokenTree::Punct(right),
                    TokenTree::Ident(part),
                    ..,
                ] = &tokens[end..]
                {
                    if left.as_char() != ':' || right.as_char() != ':' {
                        break;
                    }
                    name.push_str("::");
                    name.push_str(&part.to_string());
                    end += 3;
                }
                if let [TokenTree::Punct(bang), TokenTree::Group(group), ..] = &tokens[end..]
                    && bang.as_char() == '!'
                {
                    let opening = group.span_open().byte_range();
                    let closing = group.span_close().byte_range();
                    self.invocations.push(Invocation {
                        arguments: opening.end..closing.end,
                        expansion: Expansion {
                            macro_name: name,
                            call_site: format!("{path}:{}", first.span().start().line),
                            definition: None,
                        },
                    });
                    self.visit(path, group.stream());
                    index = end + 2;
                    continue;
                }
            }
            if let TokenTree::Group(group) = &tokens[start] {
                self.visit(path, group.stream());
            }
            index += 1;
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OriginError {
    #[error("cannot tokenize authored Rust source {path}")]
    Tokenize { path: String },
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    #[test]
    fn invalid_authored_tokens_retain_the_typed_parser_cause() {
        let error = SourceOrigins::parse("src/lib.rs", "fn f( {")
            .err()
            .assured("the current source has unmatched delimiters");
        assert!(error.contains::<syn::Error>());
    }

    #[test]
    fn captured_tokens_keep_nested_qualified_origins_and_unicode_byte_offsets() {
        let source = "// λ\nfn f() { external::capture! { nested!(map.get(&1)); } }";
        let origins =
            SourceOrigins::parse("src/lib.rs", source).assured("the source is valid Rust tokens");
        let offset = source
            .find("map.get")
            .assured("the fixture includes its operation");
        assert_eq!(
            origins.at(offset),
            vec![
                Expansion {
                    macro_name: "external::capture".into(),
                    call_site: "src/lib.rs:2".into(),
                    definition: None
                },
                Expansion {
                    macro_name: "nested".into(),
                    call_site: "src/lib.rs:2".into(),
                    definition: None
                },
            ]
        );
        assert!(origins.at(0).is_empty());
    }

    #[test]
    fn comments_and_literal_contents_never_invent_invocations() {
        let source =
            "fn f() { let text = r#\"invented!(map.get(&1))\"#; /* phantom!() */ actual!(text); }";
        let origins =
            SourceOrigins::parse("src/lib.rs", source).assured("the source is valid Rust tokens");
        assert!(
            origins
                .at(source
                    .find("map.get")
                    .assured("the string includes the decoy"))
                .is_empty()
        );
        assert_eq!(
            origins
                .at(source
                    .rfind("text")
                    .assured("the real invocation has a token"))
                .len(),
            1
        );
    }
}
