//! Repository tooling, outside the product layer order.
//! Owns: branch ownership expectation syntax and explicit branch collections in inactive source.
//! Depends on: authored Rust attributes and macro fragments.
//! Must not know: resolved Rust types or branch membership.

use proc_macro2::Span;
use syn::{
    Attribute, Field, GenericArgument, Local, Meta, Path, PathArguments, Token, Type,
    punctuated::Punctuated,
    spanned::Spanned as _,
    visit::{self, Visit},
};

use super::{SourceError, fragment};

pub(super) fn check(path: &str, parsed: &syn::File) -> Result<(), SourceError> {
    let mut scanner = Scanner {
        defect: None,
        inactive: false,
    };
    scanner.visit_file(parsed);
    if let Some((span, problem)) = scanner.defect {
        let start = span.start();
        return Err(match problem {
            Problem::Expectation => SourceError::BranchExpectation {
                path: path.into(),
                line: start.line,
                column: start.column,
            },
            Problem::Collection => SourceError::InactiveBranchCollection {
                path: path.into(),
                line: start.line,
                column: start.column,
            },
        });
    }
    Ok(())
}

struct Scanner {
    defect: Option<(Span, Problem)>,
    inactive: bool,
}

enum Problem {
    Expectation,
    Collection,
}

impl Scanner {
    fn attribute(&mut self, meta: &Meta, declaration: bool) {
        let Meta::List(list) = meta else { return };
        if !matches!(
            list.path.get_ident().map(syn::Ident::to_string).as_deref(),
            Some("cfg_attr" | "allow" | "expect" | "warn")
        ) {
            return;
        }
        let Ok(arguments) = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        else {
            return;
        };
        if list.path.is_ident("cfg_attr") {
            for attribute in arguments.iter().skip(1) {
                self.attribute(attribute, declaration);
            }
            return;
        }
        let names_branch_rule = arguments.iter().any(|argument| {
            let mut path = argument.path().segments.iter();
            matches!((path.next(), path.next(), path.next()), (Some(tool), Some(rule), None)
                if tool.ident == "nervix" && rule.ident == "branch_inversion")
        });
        if names_branch_rule
            && !(list.path.is_ident("expect")
                && declaration
                && arguments.len() == 2
                && arguments.iter().any(repair_reason))
        {
            self.defect
                .get_or_insert((list.delimiter.span().open(), Problem::Expectation));
        }
    }

    fn declaration_attributes(&mut self, attributes: &[Attribute]) {
        for attribute in attributes {
            self.attribute(&attribute.meta, true);
        }
    }

    fn within_item(&mut self, attributes: &[Attribute], visit: impl FnOnce(&mut Self)) {
        let enclosing = self.inactive;
        self.inactive |= attributes.iter().any(disables_item);
        visit(self);
        self.inactive = enclosing;
    }
}

fn disables_item(attribute: &Attribute) -> bool {
    attribute.path().is_ident("cfg")
        && matches!(attribute.parse_args::<Meta>(), Ok(Meta::List(list))
            if list.path.is_ident("any") && list.tokens.is_empty())
}

fn external_path(path: &Path, names: &[&str]) -> bool {
    path.leading_colon.is_some()
        && path.segments.len() == names.len()
        && path
            .segments
            .iter()
            .zip(names)
            .all(|(segment, name)| segment.ident == *name)
}

fn named_branch_identity(value: &Type) -> bool {
    match value {
        Type::Path(path) => {
            external_path(&path.path, &["nervix_models", "BranchKeyFingerprint"])
                || external_path(&path.path, &["core", "option", "Option"])
                    && type_arguments(&path.path).any(named_branch_identity)
        }
        Type::Tuple(tuple) => tuple.elems.iter().any(named_branch_identity),
        _ => false,
    }
}

fn type_arguments(path: &Path) -> impl Iterator<Item = &Type> {
    path.segments.last().into_iter().flat_map(|segment| {
        let arguments = match &segment.arguments {
            PathArguments::AngleBracketed(arguments) => Some(arguments),
            _ => None,
        };
        arguments.into_iter().flat_map(|arguments| {
            arguments.args.iter().filter_map(|argument| match argument {
                GenericArgument::Type(value) => Some(value),
                _ => None,
            })
        })
    })
}

/// This check uses explicit external paths. The compiler resolves aliases and inferred types
/// in the configurations it compiles.
fn explicit_branch_collection(value: &Type) -> bool {
    let Type::Path(path) = value else {
        return false;
    };
    let path = &path.path;
    let collections = ["HashMap", "HashSet", "BTreeMap", "BTreeSet", "VecDeque"];
    (external_path(path, &["std", "vec", "Vec"])
        || collections
            .iter()
            .any(|name| external_path(path, &["std", "collections", name])))
        && type_arguments(path)
            .next()
            .is_some_and(named_branch_identity)
}

fn repair_reason(meta: &Meta) -> bool {
    let Meta::NameValue(value) = meta else {
        return false;
    };
    if !value.path.is_ident("reason") {
        return false;
    }
    matches!(&value.value, syn::Expr::Lit(lit) if matches!(&lit.lit, syn::Lit::Str(reason)
        if crate::branch_repair_task(&reason.value()).is_some()))
}

impl<'ast> Visit<'ast> for Scanner {
    fn visit_attribute(&mut self, attribute: &'ast Attribute) {
        self.attribute(&attribute.meta, false);
    }

    fn visit_field(&mut self, field: &'ast Field) {
        self.declaration_attributes(&field.attrs);
        self.within_item(&field.attrs, |scanner| scanner.visit_type(&field.ty));
    }

    fn visit_local(&mut self, local: &'ast Local) {
        self.declaration_attributes(&local.attrs);
        self.within_item(&local.attrs, |scanner| {
            scanner.visit_pat(&local.pat);
            if let Some(init) = &local.init {
                scanner.visit_expr(&init.expr);
                if let Some((_, diverge)) = &init.diverge {
                    scanner.visit_expr(diverge);
                }
            }
        });
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        fragment(self, mac.tokens.clone());
    }

    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        self.within_item(&item.attrs, |scanner| {
            visit::visit_item_struct(scanner, item)
        });
    }

    fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
        self.within_item(&item.attrs, |scanner| visit::visit_item_enum(scanner, item));
    }

    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        self.within_item(&item.attrs, |scanner| visit::visit_item_mod(scanner, item));
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.within_item(&item.attrs, |scanner| visit::visit_item_fn(scanner, item));
    }

    fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
        self.within_item(&item.attrs, |scanner| {
            visit::visit_item_macro(scanner, item)
        });
    }

    fn visit_type(&mut self, value: &'ast Type) {
        if self.inactive && explicit_branch_collection(value) {
            self.defect
                .get_or_insert((value.span(), Problem::Collection));
        }
        visit::visit_type(self, value);
    }
}
