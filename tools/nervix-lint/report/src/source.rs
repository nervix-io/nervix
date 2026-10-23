//! Repository tooling, outside the product layer order.
//! Owns: compiler-independent cast syntax, struct field gates and hidden setting syntax.
//! Depends on: the Rust source parser, source token spans and typed report boundaries.
//! Must not know: resolved APIs, active Cargo configurations or a product location catalog.

use std::collections::BTreeSet;

use proc_macro2::{Delimiter, Group, Span, TokenStream, TokenTree};
use serde::{Deserialize, Serialize};
use syn::{
    Attribute, Expr, Item, Meta, Token,
    parse::Parser as _,
    punctuated::Punctuated,
    visit::{self, Visit},
};

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInput {
    pub path: String,
    pub source: String,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct SourceSite {
    pub path: String,
    pub line: usize,
    pub column: usize,
    pub rule: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("invalid fixed-constant contract at {path}:{line}:{column}: {problem}")]
    ConstantContract {
        path: String,
        line: usize,
        column: usize,
        problem: &'static str,
    },
    #[error("cannot parse authored Rust at {path}:{line}:{column}")]
    Syntax {
        path: String,
        line: usize,
        column: usize,
        #[source]
        cause: syn::Error,
    },
}

pub fn scan(files: Vec<SourceInput>) -> Result<Vec<SourceSite>, error_stack::Report<SourceError>> {
    let mut sites = BTreeSet::new();
    for input in files {
        let parsed = syn::parse_file(&input.source).map_err(|cause| {
            let location = cause.span().start();
            SourceError::Syntax {
                path: input.path.clone(),
                line: location.line,
                column: location.column,
                cause,
            }
        })?;
        Scanner {
            path: &input.path,
            sites: &mut sites,
        }
        .visit_file(&parsed);
        sites.extend(tunables::scan(&input.path, &parsed)?);
    }
    Ok(sites.into_iter().collect())
}

struct Scanner<'a> {
    path: &'a str,
    sites: &'a mut BTreeSet<SourceSite>,
}

impl Scanner<'_> {
    fn site(&mut self, span: Span, rule: &str) {
        let start = span.start();
        self.sites.insert(SourceSite {
            path: self.path.into(),
            line: start.line,
            column: start.column,
            rule: rule.into(),
        });
    }
}

fn fragment<V: for<'ast> Visit<'ast>>(scanner: &mut V, tokens: TokenStream) {
    // Macro metavariables are opaque Rust fragments. Keep their identifier's original span
    // while supplying an identifier to the syntax parser; recurse into repetition groups.
    let substituted = substitute(tokens);
    if let Ok(file) = syn::parse2::<syn::File>(substituted.clone()) {
        scanner.visit_file(&file);
        return;
    }
    if let Ok(expression) = syn::parse2::<Expr>(substituted.clone()) {
        scanner.visit_expr(&expression);
        return;
    }
    let block = TokenStream::from(TokenTree::Group(Group::new(
        Delimiter::Brace,
        substituted.clone(),
    )));
    if let Ok(block) = syn::parse2::<syn::Block>(block) {
        scanner.visit_block(&block);
        return;
    }
    // Matchers, repetitions and opaque DSL tokens are not complete Rust expressions. Their
    // nested Rust bodies are still parsed; arbitrary words are never classified as casts.
    for token in substituted {
        if let TokenTree::Group(group) = token {
            fragment(scanner, group.stream());
        }
    }
}

fn substitute(tokens: TokenStream) -> TokenStream {
    let mut result = TokenStream::new();
    let mut iterator = tokens.into_iter().peekable();
    while let Some(token) = iterator.next() {
        if let TokenTree::Punct(punctuation) = &token
            && punctuation.as_char() == '$'
        {
            if matches!(iterator.peek(), Some(TokenTree::Ident(_))) {
                continue;
            }
            if matches!(iterator.peek(), Some(TokenTree::Group(_))) {
                let Some(TokenTree::Group(group)) = iterator.next() else {
                    unreachable!("the next token was checked as a group");
                };
                let mut replacement =
                    Group::new(Delimiter::Parenthesis, substitute(group.stream()));
                replacement.set_span(group.span());
                result.extend([TokenTree::Group(replacement)]);
                // One representative repetition retains every authored operation. A separator
                // and quantifier are macro grammar rather than expression multiplication.
                if matches!(iterator.peek(), Some(TokenTree::Punct(value)) if !matches!(value.as_char(), '*' | '+' | '?'))
                {
                    iterator.next();
                }
                if matches!(iterator.peek(), Some(TokenTree::Punct(value)) if matches!(value.as_char(), '*' | '+' | '?'))
                {
                    iterator.next();
                }
                continue;
            }
        }
        if let TokenTree::Group(group) = token {
            let mut replacement = Group::new(group.delimiter(), substitute(group.stream()));
            replacement.set_span(group.span());
            result.extend([TokenTree::Group(replacement)]);
        } else {
            result.extend([token]);
        }
    }
    result
}

fn arguments(meta: &syn::MetaList) -> Option<Punctuated<Meta, Token![,]>> {
    Punctuated::<Meta, Token![,]>::parse_terminated
        .parse2(meta.tokens.clone())
        .ok()
}

fn requires_test(meta: &Meta) -> bool {
    match meta {
        Meta::Path(path) => path.is_ident("test"),
        Meta::List(list) => {
            let Some(values) = arguments(list) else {
                return false;
            };
            if list.path.is_ident("all") {
                return values.iter().any(requires_test);
            }
            if list.path.is_ident("any") {
                return !values.is_empty() && values.iter().all(requires_test);
            }
            false
        }
        _ => false,
    }
}

fn test_only(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        if attribute
            .path()
            .segments
            .last()
            .is_some_and(|segment| matches!(segment.ident.to_string().as_str(), "test" | "rstest"))
        {
            return true;
        }
        if let Meta::List(list) = &attribute.meta
            && list.path.is_ident("cfg")
            && let Some(values) = arguments(list)
        {
            return values.iter().any(requires_test);
        }
        false
    })
}

fn testing_feature(meta: &Meta) -> bool {
    match meta {
        Meta::NameValue(value) if value.path.is_ident("feature") => {
            if let Expr::Lit(value) = &value.value
                && let syn::Lit::Str(value) = &value.lit
            {
                return value.value() == "testing";
            }
            false
        }
        Meta::List(list) => {
            arguments(list).is_some_and(|values| values.iter().any(testing_feature))
        }
        _ => false,
    }
}

fn changes_presence(meta: &Meta) -> bool {
    match meta {
        Meta::List(list) if list.path.is_ident("cfg") => true,
        Meta::List(list) if list.path.is_ident("cfg_attr") => {
            arguments(list).is_some_and(|values| values.iter().skip(1).any(changes_presence))
        }
        _ => false,
    }
}

fn testing_gate(meta: &Meta) -> bool {
    match meta {
        Meta::List(list) if list.path.is_ident("cfg") => testing_feature(meta),
        Meta::List(list) if list.path.is_ident("cfg_attr") => {
            let Some(values) = arguments(list) else {
                return false;
            };
            let mut values = values.iter();
            let Some(condition) = values.next() else {
                return false;
            };
            let conditional = testing_feature(condition);
            values.any(|value| testing_gate(value) || (conditional && changes_presence(value)))
        }
        _ => false,
    }
}

impl<'ast> Visit<'ast> for Scanner<'_> {
    fn visit_impl_item(&mut self, item: &'ast syn::ImplItem) {
        let attributes: &[Attribute] = match item {
            syn::ImplItem::Const(value) => &value.attrs,
            syn::ImplItem::Fn(value) => &value.attrs,
            syn::ImplItem::Type(value) => &value.attrs,
            syn::ImplItem::Macro(value) => &value.attrs,
            _ => &[],
        };
        if !test_only(attributes) {
            visit::visit_impl_item(self, item);
        }
    }

    fn visit_trait_item(&mut self, item: &'ast syn::TraitItem) {
        let attributes: &[Attribute] = match item {
            syn::TraitItem::Const(value) => &value.attrs,
            syn::TraitItem::Fn(value) => &value.attrs,
            syn::TraitItem::Type(value) => &value.attrs,
            syn::TraitItem::Macro(value) => &value.attrs,
            _ => &[],
        };
        if !test_only(attributes) {
            visit::visit_trait_item(self, item);
        }
    }

    fn visit_field(&mut self, field: &'ast syn::Field) {
        if !test_only(&field.attrs) {
            visit::visit_field(self, field);
        }
    }
    fn visit_item(&mut self, item: &'ast Item) {
        let attributes: &[Attribute] = match item {
            Item::Const(value) => &value.attrs,
            Item::Enum(value) => &value.attrs,
            Item::ExternCrate(value) => &value.attrs,
            Item::Fn(value) => &value.attrs,
            Item::ForeignMod(value) => &value.attrs,
            Item::Impl(value) => &value.attrs,
            Item::Macro(value) => &value.attrs,
            Item::Mod(value) => &value.attrs,
            Item::Static(value) => &value.attrs,
            Item::Struct(value) => &value.attrs,
            Item::Trait(value) => &value.attrs,
            Item::TraitAlias(value) => &value.attrs,
            Item::Type(value) => &value.attrs,
            Item::Union(value) => &value.attrs,
            Item::Use(value) => &value.attrs,
            _ => &[],
        };
        if !test_only(attributes) {
            visit::visit_item(self, item);
        }
    }

    fn visit_expr_cast(&mut self, expression: &'ast syn::ExprCast) {
        self.site(expression.as_token.span, "as_casts");
        visit::visit_expr_cast(self, expression);
    }

    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        for field in &item.fields {
            if test_only(&field.attrs) {
                continue;
            }
            if let Some(attribute) = field
                .attrs
                .iter()
                .find(|attribute| testing_gate(&attribute.meta))
            {
                self.site(
                    attribute.bracket_token.span.open(),
                    "testing_feature_struct_fields",
                );
            }
        }
        visit::visit_item_struct(self, item);
    }

    fn visit_macro(&mut self, value: &'ast syn::Macro) {
        fragment(self, value.tokens.clone());
    }
}

mod tunables;

#[cfg(test)]
mod tests;
