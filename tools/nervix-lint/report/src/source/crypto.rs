//! Repository tooling, outside the product layer order.
//! Owns: the syntax rule that finds authored cryptography not coming from AWS-LC.
//! Depends on: the shared source parser, original token spans and the product scope it declares.
//! Must not know: resolved compiler paths, Cargo manifests, the dependency graph or a per-site
//! exemption catalog.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::{Spacing, Span, TokenStream, TokenTree};
use syn::{
    Attribute, Expr, Item, Lit, Meta, UseTree,
    ext::IdentExt as _,
    visit::{self, Visit},
};

use super::{
    SourceSite, impl_item_attributes, item_attributes, product, test_only, trait_item_attributes,
};

/// The rule every site carries. The gate refuses each one rather than counting it: authored
/// cryptography outside AWS-LC has no baseline, unlike the product graph's dependency edges.
pub(super) const RULE: &str = "authored_non_aws_lc_crypto";

/// The `nervix-crypto` boundary: the one crate that may draw entropy or build the TLS provider.
const BOUNDARY: &str = "crates/crypto/";

/// Crates that implement cryptography other than AWS-LC. Every authored use counts, inside the
/// boundary too. BLAKE3 counts unless its operation documents a non-security purpose.
const CRATES: [&str; 6] = ["argon2", "blake3", "hmac", "pbkdf2", "sha1", "sha2"];

/// A source-owned purpose comment, rather than a file or module exemption. Rustdoc comments
/// are parsed attributes, so text in literals, ordinary comments and opaque macro strings cannot
/// accidentally classify an operation.
fn non_security_blake3(attributes: &[Attribute]) -> bool {
    for attribute in attributes {
        let Meta::NameValue(value) = &attribute.meta else {
            continue;
        };
        if !value.path.is_ident("doc") {
            continue;
        }
        let Expr::Lit(value) = &value.value else {
            continue;
        };
        let Lit::Str(text) = &value.lit else {
            continue;
        };
        for line in text.value().lines() {
            if let Some(reason) = line.trim().strip_prefix("Non-security BLAKE3:")
                && !reason.trim().is_empty()
            {
                return true;
            }
        }
    }
    false
}

/// The operating-system entropy crate, which only the boundary may call.
const ENTROPY_CRATE: &str = "getrandom";

/// The operating-system generator `rand_core` defines and other crates re-export by this name.
const OS_GENERATOR: &str = "OsRng";

/// Modules whose glob import brings the operating-system generator into scope.
const GENERATOR_MODULES: [&str; 2] = ["rand_core", "rngs"];

/// The AWS-LC module, the crate itself or `rustls::crypto::aws_lc_rs`, that builds the provider.
const PROVIDER_MODULE: &str = "aws_lc_rs";
const PROVIDER: &str = "default_provider";

pub(super) fn scan(path: &str, file: &syn::File) -> Vec<SourceSite> {
    if !product(path) {
        return Vec::new();
    }
    let mut imports = Imports::default();
    imports.collect(file.items.iter());
    let mut scanner = Cryptography {
        path,
        placement: Placement::of(path),
        imports,
        non_security_blake3: false,
        sites: BTreeSet::new(),
    };
    scanner.visit_file(file);
    scanner.sites.into_iter().collect()
}

/// A path root that names the current crate or one of its modules rather than a dependency.
fn local(root: &str) -> bool {
    matches!(root, "crate" | "self" | "super" | "Self")
}

/// A use path ending in `name`, where `self` names the module the prefix already reached.
fn joined(prefix: Vec<String>, name: &str) -> Vec<String> {
    let mut path = prefix;
    if name != "self" {
        path.push(name.into());
    }
    path
}

/// Where a file sits relative to the boundary, which decides what it may draw directly.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Placement {
    Boundary,
    Outside,
}

impl Placement {
    fn of(path: &str) -> Self {
        if path.starts_with(BOUNDARY) {
            Self::Boundary
        } else {
            Self::Outside
        }
    }

    /// Whether a path, its first segment already resolved, names a counted crate or item.
    fn counts(self, path: &[String]) -> bool {
        let Some(root) = path.first() else {
            return false;
        };
        if CRATES.contains(&root.as_str()) {
            return true;
        }
        if self == Self::Boundary || local(root) {
            return false;
        }
        if root == ENTROPY_CRATE {
            return true;
        }
        let generator = path.iter().any(|segment| segment == OS_GENERATOR);
        let provider = path
            .windows(2)
            .any(|pair| pair[0] == PROVIDER_MODULE && pair[1] == PROVIDER);
        generator || provider
    }

    /// Whether a prefix that a glob or a group continues can bring a counted item into scope.
    fn counts_prefix(self, prefix: &[String]) -> bool {
        if self.counts(prefix) {
            return true;
        }
        if self == Self::Boundary {
            return false;
        }
        let Some(root) = prefix.first() else {
            return false;
        };
        if local(root) {
            return false;
        }
        let Some(last) = prefix.last() else {
            return false;
        };
        GENERATOR_MODULES.contains(&last.as_str()) || last == PROVIDER_MODULE
    }
}

/// The full path each name in a lexical scope stands for, as a `use` or a crate rename declares
/// it. Rust aliases cover their entire scope, so a scope's declarations apply before its uses.
#[derive(Clone, Default)]
struct Imports {
    paths: BTreeMap<String, Vec<String>>,
}

impl Imports {
    fn collect<'ast>(&mut self, items: impl Iterator<Item = &'ast Item>) {
        for item in items {
            if test_only(item_attributes(item)) {
                continue;
            }
            match item {
                Item::Use(value) => self.declare(Vec::new(), &value.tree),
                Item::ExternCrate(value) => {
                    if let Some((_, rename)) = &value.rename {
                        self.paths.insert(
                            rename.unraw().to_string(),
                            vec![value.ident.unraw().to_string()],
                        );
                    }
                }
                _ => {}
            }
        }
    }

    fn declare(&mut self, prefix: Vec<String>, tree: &UseTree) {
        match tree {
            UseTree::Path(value) => {
                let mut path = prefix;
                path.push(value.ident.unraw().to_string());
                self.declare(path, &value.tree);
            }
            UseTree::Name(value) => {
                let path = joined(prefix, &value.ident.unraw().to_string());
                let Some(binding) = path.last() else {
                    return;
                };
                self.paths.insert(binding.clone(), path);
            }
            UseTree::Rename(value) => {
                let binding = value.rename.unraw().to_string();
                // An underscore import binds no name a later path could spell.
                if binding == "_" {
                    return;
                }
                let path = joined(prefix, &value.ident.unraw().to_string());
                self.paths.insert(binding, path);
            }
            UseTree::Glob(_) => {}
            UseTree::Group(value) => {
                for tree in &value.items {
                    self.declare(prefix.clone(), tree);
                }
            }
        }
    }

    /// The written path with its first binding replaced by the path its declaration names. A
    /// leading `self::` or `super::` reaches a binding an enclosing scope declared, which every
    /// nested scope keeps; one that reaches no binding stays a local path.
    fn resolve(&self, written: Vec<String>) -> Vec<String> {
        let mut path = written;
        // Each step consumes one distinct binding, so a cycle stops where it repeats.
        let mut followed = BTreeSet::new();
        loop {
            let relative = path
                .iter()
                .take_while(|segment| matches!(segment.as_str(), "self" | "super"))
                .count();
            let Some(binding) = path.get(relative) else {
                return path;
            };
            let Some(declared) = self.paths.get(binding) else {
                return path;
            };
            if !followed.insert(binding.clone()) {
                return path;
            }
            let mut resolved = declared.clone();
            resolved.extend(path.into_iter().skip(relative + 1));
            path = resolved;
        }
    }
}

/// A path written in macro tokens, which need not parse as Rust syntax. It records its segments,
/// whether it starts at the crate root, whether it continues into a group or a glob, where it
/// starts and the index just past it.
struct Chain {
    segments: Vec<String>,
    global: bool,
    continues: bool,
    start: Span,
    end: usize,
}

/// Whether the tokens at `index` are the `::` path separator.
fn separator(tokens: &[TokenTree], index: usize) -> bool {
    let (Some(TokenTree::Punct(first)), Some(TokenTree::Punct(second))) =
        (tokens.get(index), tokens.get(index + 1))
    else {
        return false;
    };
    first.as_char() == ':' && first.spacing() == Spacing::Joint && second.as_char() == ':'
}

impl Chain {
    fn read(tokens: &[TokenTree], start: usize) -> Option<Self> {
        let global = separator(tokens, start);
        let mut index = if global { start + 2 } else { start };
        let Some(TokenTree::Ident(first)) = tokens.get(index) else {
            return None;
        };
        let mut segments = vec![first.unraw().to_string()];
        let span = first.span();
        index += 1;
        while separator(tokens, index) {
            let Some(TokenTree::Ident(next)) = tokens.get(index + 2) else {
                break;
            };
            segments.push(next.unraw().to_string());
            index += 3;
        }
        Some(Self {
            segments,
            global,
            continues: separator(tokens, index),
            start: span,
            end: index,
        })
    }
}

struct Cryptography<'a> {
    path: &'a str,
    placement: Placement,
    imports: Imports,
    non_security_blake3: bool,
    sites: BTreeSet<SourceSite>,
}

impl Cryptography<'_> {
    fn counts(&self, path: &[String]) -> bool {
        if self.non_security_blake3 && path.first().is_some_and(|root| root == "blake3") {
            return false;
        }
        self.placement.counts(path)
    }

    fn counts_prefix(&self, path: &[String]) -> bool {
        if self.non_security_blake3 && path.first().is_some_and(|root| root == "blake3") {
            return false;
        }
        self.placement.counts_prefix(path)
    }

    fn site(&mut self, span: Span) {
        let start = span.start();
        self.sites.insert(SourceSite {
            path: self.path.into(),
            line: start.line,
            column: start.column,
            rule: RULE.into(),
        });
    }

    /// Each name a `use` imports is one use, so a group counts every name it imports.
    fn use_tree(&mut self, prefix: Vec<String>, tree: &UseTree) {
        match tree {
            UseTree::Path(value) => {
                let mut path = prefix;
                path.push(value.ident.unraw().to_string());
                self.use_tree(path, &value.tree);
            }
            UseTree::Name(value) => self.imported(prefix, &value.ident),
            UseTree::Rename(value) => self.imported(prefix, &value.ident),
            UseTree::Glob(value) => {
                let prefix = self.imports.resolve(prefix);
                if self.counts_prefix(&prefix) {
                    self.site(value.star_token.span);
                }
            }
            UseTree::Group(value) => {
                for tree in &value.items {
                    self.use_tree(prefix.clone(), tree);
                }
            }
        }
    }

    fn imported(&mut self, prefix: Vec<String>, name: &syn::Ident) {
        let written = joined(prefix, &name.unraw().to_string());
        let path = self.imports.resolve(written);
        if self.counts(&path) {
            self.site(name.span());
        }
    }

    /// Paths inside a macro's tokens: each chain of segments joined by `::` counts once, at its
    /// first segment, wherever the tokens sit, including arguments that do not parse as Rust.
    fn macro_tokens(&mut self, stream: TokenStream) {
        let tokens: Vec<TokenTree> = stream.into_iter().collect();
        let mut index = 0;
        while let Some(token) = tokens.get(index) {
            index = match token {
                TokenTree::Group(group) => {
                    self.macro_tokens(group.stream());
                    index + 1
                }
                // `$name` and `$crate::…` belong to the macro, never to an authored crate path.
                TokenTree::Punct(punctuation) if punctuation.as_char() == '$' => {
                    match Chain::read(&tokens, index + 1) {
                        Some(chain) => chain.end,
                        None => index + 1,
                    }
                }
                _ => match Chain::read(&tokens, index) {
                    Some(chain) => {
                        self.chain(&chain);
                        chain.end
                    }
                    None => index + 1,
                },
            };
        }
    }

    fn chain(&mut self, chain: &Chain) {
        // One segment is a name in scope, counted where its import declared it.
        if chain.segments.len() < 2 && !chain.continues && !chain.global {
            return;
        }
        let path = if chain.global {
            chain.segments.clone()
        } else {
            self.imports.resolve(chain.segments.clone())
        };
        let counted = if chain.continues {
            self.counts_prefix(&path)
        } else {
            self.counts(&path)
        };
        if counted {
            self.site(chain.start);
        }
    }
}

impl<'ast> Visit<'ast> for Cryptography<'_> {
    fn visit_attribute(&mut self, _attribute: &'ast Attribute) {
        // Attributes name configurations, tools and derives, never a use of cryptography.
    }

    fn visit_item(&mut self, item: &'ast Item) {
        if test_only(item_attributes(item)) {
            return;
        }
        let saved = self.non_security_blake3;
        // Containers cannot classify their children. Each operation, import or field owns its
        // own reason, including a nested function inside an otherwise classified operation.
        self.non_security_blake3 = matches!(
            item,
            Item::Fn(_)
                | Item::Use(_)
                | Item::ExternCrate(_)
                | Item::Type(_)
                | Item::Const(_)
                | Item::Static(_)
                | Item::Macro(_)
        ) && non_security_blake3(item_attributes(item));
        visit::visit_item(self, item);
        self.non_security_blake3 = saved;
    }

    fn visit_impl_item(&mut self, item: &'ast syn::ImplItem) {
        if test_only(impl_item_attributes(item)) {
            return;
        }
        let saved = self.non_security_blake3;
        self.non_security_blake3 = non_security_blake3(impl_item_attributes(item));
        visit::visit_impl_item(self, item);
        self.non_security_blake3 = saved;
    }

    fn visit_trait_item(&mut self, item: &'ast syn::TraitItem) {
        if test_only(trait_item_attributes(item)) {
            return;
        }
        let saved = self.non_security_blake3;
        self.non_security_blake3 = non_security_blake3(trait_item_attributes(item));
        visit::visit_trait_item(self, item);
        self.non_security_blake3 = saved;
    }

    fn visit_field(&mut self, field: &'ast syn::Field) {
        if test_only(&field.attrs) {
            return;
        }
        let saved = self.non_security_blake3;
        self.non_security_blake3 = non_security_blake3(&field.attrs);
        visit::visit_field(self, field);
        self.non_security_blake3 = saved;
    }

    fn visit_item_mod(&mut self, value: &'ast syn::ItemMod) {
        let saved = self.imports.clone();
        if let Some((_, items)) = &value.content {
            self.imports.collect(items.iter());
        }
        visit::visit_item_mod(self, value);
        self.imports = saved;
    }

    fn visit_block(&mut self, value: &'ast syn::Block) {
        let saved = self.imports.clone();
        let items = value.stmts.iter().filter_map(|statement| match statement {
            syn::Stmt::Item(item) => Some(item),
            _ => None,
        });
        self.imports.collect(items);
        visit::visit_block(self, value);
        self.imports = saved;
    }

    fn visit_item_use(&mut self, value: &'ast syn::ItemUse) {
        self.use_tree(Vec::new(), &value.tree);
    }

    fn visit_item_extern_crate(&mut self, value: &'ast syn::ItemExternCrate) {
        let path = vec![value.ident.unraw().to_string()];
        if self.counts(&path) {
            self.site(value.ident.span());
        }
    }

    fn visit_path(&mut self, value: &'ast syn::Path) {
        let global = value.leading_colon.is_some();
        // One segment is a name in scope, counted where its import declared it.
        if (value.segments.len() > 1 || global)
            && let Some(first) = value.segments.first()
        {
            let written = value
                .segments
                .iter()
                .map(|segment| segment.ident.unraw().to_string())
                .collect();
            let path = if global {
                written
            } else {
                self.imports.resolve(written)
            };
            if self.counts(&path) {
                self.site(first.ident.span());
            }
        }
        visit::visit_path(self, value);
    }

    fn visit_macro(&mut self, value: &'ast syn::Macro) {
        self.visit_path(&value.path);
        self.macro_tokens(value.tokens.clone());
    }
}
