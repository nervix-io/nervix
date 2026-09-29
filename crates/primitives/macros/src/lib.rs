//! The runtime attributes `nervix-primitives` exports as `#[nervix_primitives::test]` and
//! `#[nervix_primitives::main]`.
//!
//! Tokio's attributes build their runtime through a crate path, and without an explicit one they
//! name `tokio` at the call site: whatever a crate happens to call `tokio` would build the runtime,
//! which in a modeled build need not be the runtime the primitive boundary selected. Each attribute
//! here forwards to the attribute of the selected runtime with its crate path fixed to the boundary,
//! so the runtime a test or binary builds is always the one its primitives run on. The arguments
//! pass through unchanged, except the crate path, which the build's execution mode decides.
//!
//! Layer: primitives.
//!
//! - **Owns.** Forwarding the runtime attributes to the runtime the boundary selected.
//! - **Depends on.** The compiler's procedural macro interface.
//! - **Must not know.** Which runtime a mode selects; `nervix-primitives` resolves that.

use proc_macro::{Delimiter, Group, Ident, Literal, Punct, Spacing, Span, TokenStream, TokenTree};

/// The module of `nervix-primitives` that re-exports the selected runtime's attributes and the
/// items their expansion names.
const RUNTIME_SUPPORT: [&str; 2] = ["nervix_primitives", "__private"];

/// Run an async test on the runtime of the build's execution mode. Takes the arguments of Tokio's
/// test attribute, except `crate`.
#[proc_macro_attribute]
pub fn test(arguments: TokenStream, item: TokenStream) -> TokenStream {
    forward("test", arguments, item)
}

/// Run an async `main` on the runtime of the build's execution mode. Takes the arguments of Tokio's
/// main attribute, except `crate`.
#[proc_macro_attribute]
pub fn main(arguments: TokenStream, item: TokenStream) -> TokenStream {
    forward("main", arguments, item)
}

/// `#[::nervix_primitives::__private::<attribute>(crate = "::nervix_primitives::__private", ...)]`
/// in front of `item`.
fn forward(attribute: &str, arguments: TokenStream, item: TokenStream) -> TokenStream {
    if let Some(span) = crate_argument(&arguments) {
        return compile_error(
            span,
            "the execution mode selects the runtime; `crate` cannot be given",
        );
    }

    let support_path = format!("::{}", RUNTIME_SUPPORT.join("::"));
    let mut forwarded = TokenStream::from_iter([
        TokenTree::Ident(Ident::new("crate", Span::call_site())),
        TokenTree::Punct(Punct::new('=', Spacing::Alone)),
        TokenTree::Literal(Literal::string(&support_path)),
    ]);
    if !arguments.is_empty() {
        forwarded.extend([TokenTree::Punct(Punct::new(',', Spacing::Alone))]);
        forwarded.extend(arguments);
    }

    let mut attribute_path = path(&RUNTIME_SUPPORT);
    attribute_path.extend(path(&[attribute]));
    attribute_path.extend([TokenTree::Group(Group::new(
        Delimiter::Parenthesis,
        forwarded,
    ))]);

    let mut expanded = TokenStream::from_iter([
        TokenTree::Punct(Punct::new('#', Spacing::Alone)),
        TokenTree::Group(Group::new(Delimiter::Bracket, attribute_path)),
    ]);
    expanded.extend(item);
    expanded
}

/// Where the arguments name a crate path, if they do.
fn crate_argument(arguments: &TokenStream) -> Option<Span> {
    let mut tokens = arguments.clone().into_iter().peekable();
    while let Some(token) = tokens.next() {
        let TokenTree::Ident(ident) = token else {
            continue;
        };
        if ident.to_string() != "crate" {
            continue;
        }
        if let Some(TokenTree::Punct(punct)) = tokens.peek()
            && punct.as_char() == '='
        {
            return Some(ident.span());
        }
    }
    None
}

/// `::first::second::...` at the call site, so the path resolves in the crate using the attribute.
fn path(segments: &[&str]) -> TokenStream {
    let mut tokens = TokenStream::new();
    for segment in segments {
        tokens.extend([
            TokenTree::Punct(Punct::new(':', Spacing::Joint)),
            TokenTree::Punct(Punct::new(':', Spacing::Alone)),
            TokenTree::Ident(Ident::new(segment, Span::call_site())),
        ]);
    }
    tokens
}

/// `::core::compile_error!("message");` reported at `span`.
fn compile_error(span: Span, message: &str) -> TokenStream {
    let mut invocation = path(&["core", "compile_error"]);
    let mut message = Literal::string(message);
    message.set_span(span);
    invocation.extend([
        TokenTree::Punct(Punct::new('!', Spacing::Alone)),
        TokenTree::Group(Group::new(
            Delimiter::Parenthesis,
            TokenStream::from_iter([TokenTree::Literal(message)]),
        )),
        TokenTree::Punct(Punct::new(';', Spacing::Alone)),
    ]);
    invocation
}
