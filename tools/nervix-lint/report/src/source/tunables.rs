//! Repository tooling, outside the product layer order.
//! Owns: the temporary syntax count of numeric policy outside declared settings.
//! Depends on: the shared source parser and original token spans, including macro fragments.
//! Must not know: resolved compiler types, runtime defaults or a per-site exemption catalog.

use std::collections::{BTreeMap, BTreeSet};

use syn::{
    Attribute, Expr, Item, Meta, Type, UseTree,
    ext::IdentExt as _,
    spanned::Spanned as _,
    visit::{self, Visit},
};

use super::{SourceError, SourceSite, arguments, fragment, test_only};

/// Tooling and the four harness crates do not configure a running Nervix product.
fn product(path: &str) -> bool {
    !path.starts_with("tools/")
        && ![
            "crates/arbitrary/",
            "crates/benchmark/",
            "crates/model-harness/",
            "crates/test-environment/",
        ]
        .iter()
        .any(|prefix| path.starts_with(prefix))
}

pub(super) fn scan(
    path: &str,
    file: &syn::File,
) -> Result<Vec<SourceSite>, error_stack::Report<SourceError>> {
    if !product(path) {
        return Ok(Vec::new());
    }
    let mut declarations = Declarations::default();
    declarations.visit_file(file);
    let mut scanner = Tunables {
        path,
        declarations,
        sites: BTreeSet::new(),
        problem: None,
    };
    scanner.visit_file(file);
    if let Some(error) = scanner.problem {
        return Err(error.into());
    }
    Ok(scanner.sites.into_iter().collect())
}

#[derive(Clone, Default)]
struct Declarations {
    aliases: BTreeMap<String, String>,
    settings: BTreeSet<String>,
}

fn type_name(value: &Type) -> Option<String> {
    if let Type::Path(value) = value {
        return value
            .path
            .segments
            .last()
            .map(|segment| segment.ident.unraw().to_string());
    }
    None
}

impl Declarations {
    fn import(&mut self, tree: &UseTree) {
        match tree {
            UseTree::Path(value) => self.import(&value.tree),
            UseTree::Rename(value) => {
                self.aliases.insert(
                    value.rename.unraw().to_string(),
                    value.ident.unraw().to_string(),
                );
            }
            UseTree::Group(value) => {
                for item in &value.items {
                    self.import(item);
                }
            }
            _ => {}
        }
    }

    fn canonical(&self, name: &str) -> Option<String> {
        let mut name = name;
        // Each step consumes one distinct alias; a cycle is never a recognized governed type.
        let mut visited = BTreeSet::new();
        while let Some(next) = self.aliases.get(name) {
            if !visited.insert(name) {
                return None;
            }
            name = next;
        }
        Some(name.into())
    }

    fn governed(&self, ty: &Type) -> bool {
        let Some(name) = type_name(ty) else {
            return false;
        };
        let Some(name) = self.canonical(&name) else {
            return false;
        };
        matches!(
            name.as_str(),
            "Duration"
                | "usize"
                | "u64"
                | "u32"
                | "u16"
                | "u8"
                | "i64"
                | "i32"
                | "f64"
                | "ByteUnit"
        ) || name.starts_with("NonZero")
    }
}

impl<'ast> Visit<'ast> for Declarations {
    fn visit_item(&mut self, value: &'ast Item) {
        // Declarations are lexical. A nested module or function cannot rename its parent's type.
        if matches!(value, Item::Use(_) | Item::Type(_) | Item::Struct(_)) {
            visit::visit_item(self, value);
        }
    }
    fn visit_item_use(&mut self, value: &'ast syn::ItemUse) {
        self.import(&value.tree);
    }
    fn visit_item_type(&mut self, value: &'ast syn::ItemType) {
        if let Some(name) = type_name(&value.ty) {
            self.aliases.insert(value.ident.unraw().to_string(), name);
        }
    }
    fn visit_item_struct(&mut self, value: &'ast syn::ItemStruct) {
        if !value.fields.is_empty()
            && value.fields.iter().all(|field| {
                type_name(&field.ty)
                    .is_some_and(|name| self.canonical(&name).as_deref() == Some("Setting"))
            })
        {
            self.settings.insert(value.ident.unraw().to_string());
        }
        visit::visit_item_struct(self, value);
    }
}

struct Tunables<'a> {
    path: &'a str,
    declarations: Declarations,
    sites: BTreeSet<SourceSite>,
    problem: Option<SourceError>,
}

fn constant(meta: &Meta) -> bool {
    let names: Vec<_> = meta
        .path()
        .segments
        .iter()
        .map(|segment| segment.ident.unraw().to_string())
        .collect();
    names == ["nervix", "constant"]
}

fn contracts(attributes: &[Attribute]) -> Vec<(Meta, bool)> {
    let mut found = Vec::new();
    for attribute in attributes {
        if constant(&attribute.meta) {
            found.push((attribute.meta.clone(), false));
        }
        if let Meta::List(list) = &attribute.meta
            && list.path.is_ident("cfg_attr")
            && let Some(values) = arguments(list)
        {
            let gated = values
                .first()
                .is_some_and(|value| value.path().is_ident("nervix_lint"));
            for value in values.into_iter().skip(1) {
                if constant(&value) {
                    found.push((value, gated));
                }
            }
        }
    }
    found
}

fn fixed(meta: &Meta) -> bool {
    let Meta::List(list) = meta else {
        return false;
    };
    let Some(values) = arguments(list) else {
        return false;
    };
    if values.len() != 2 || !matches!(&values[0], Meta::Path(path) if path.is_ident("fixed")) {
        return false;
    }
    matches!(&values[1], Meta::NameValue(value) if value.path.is_ident("reason") && matches!(&value.value, Expr::Lit(value) if matches!(&value.lit, syn::Lit::Str(text) if !text.value().trim().is_empty())))
}

struct NumericLiteral(bool);
impl<'ast> Visit<'ast> for NumericLiteral {
    fn visit_expr_lit(&mut self, value: &'ast syn::ExprLit) {
        if matches!(value.lit, syn::Lit::Int(_) | syn::Lit::Float(_)) {
            self.0 = true;
        }
    }
}

fn literal(arguments: &syn::punctuated::Punctuated<Expr, syn::Token![,]>) -> bool {
    let mut probe = NumericLiteral(false);
    for argument in arguments {
        probe.visit_expr(argument);
    }
    probe.0
}

impl Tunables<'_> {
    fn site(&mut self, span: proc_macro2::Span) {
        let start = span.start();
        self.sites.insert(SourceSite {
            path: self.path.into(),
            line: start.line,
            column: start.column,
            rule: "hidden_tunables".into(),
        });
    }

    fn fixed(&mut self, attributes: &[Attribute], is_constant: bool) -> bool {
        let values = contracts(attributes);
        if values.is_empty() {
            return false;
        }
        if values.len() == 1 && values[0].1 && is_constant && fixed(&values[0].0) {
            return true;
        }
        let start = values[0].0.span().start();
        self.problem = Some(SourceError::ConstantContract {
            path: self.path.into(),
            line: start.line,
            column: start.column,
            problem: "one constant requires constant(fixed, reason = a nonempty string), gated by \
                      nervix_lint",
        });
        false
    }

    fn constant(&mut self, attributes: &[Attribute], ty: &Type, span: proc_macro2::Span) -> bool {
        if test_only(attributes) {
            return true;
        }
        if self.fixed(attributes, true) {
            return true;
        }
        if self.declarations.governed(ty) {
            self.site(span);
            return true;
        }
        false
    }

    fn policy(&self, path: &syn::Path) -> bool {
        let mut parts = Vec::new();
        for segment in &path.segments {
            let Some(name) = self
                .declarations
                .canonical(&segment.ident.unraw().to_string())
            else {
                return false;
            };
            parts.push(name);
        }
        let Some(name) = parts.last() else {
            return false;
        };
        let owner = parts.iter().rev().nth(1).map(String::as_str);
        if name.starts_with("with_capacity") || matches!(name.as_str(), "bounded" | "sync_channel")
        {
            return true;
        }
        if name == "channel" {
            return owner != Some("watch");
        }
        if name == "new" {
            return matches!(owner, Some("Semaphore" | "ArrayQueue"));
        }
        owner == Some("Duration") && name.starts_with("from_")
    }
}

impl<'ast> Visit<'ast> for Tunables<'_> {
    fn visit_item_mod(&mut self, value: &'ast syn::ItemMod) {
        let saved = self.declarations.clone();
        if let Some((_, items)) = &value.content {
            for item in items {
                self.declarations.visit_item(item);
            }
        }
        visit::visit_item_mod(self, value);
        self.declarations = saved;
    }

    fn visit_block(&mut self, value: &'ast syn::Block) {
        let saved = self.declarations.clone();
        for statement in &value.stmts {
            if let syn::Stmt::Item(item) = statement {
                self.declarations.visit_item(item);
            }
        }
        visit::visit_block(self, value);
        self.declarations = saved;
    }
    fn visit_item(&mut self, item: &'ast Item) {
        let attributes = match item {
            Item::Const(value) => &value.attrs,
            Item::Fn(value) => &value.attrs,
            Item::Mod(value) => &value.attrs,
            Item::Impl(value) => &value.attrs,
            Item::Trait(value) => &value.attrs,
            Item::Struct(value) => &value.attrs,
            Item::Enum(value) => &value.attrs,
            Item::Static(value) => &value.attrs,
            Item::Macro(value) => &value.attrs,
            Item::Type(value) => &value.attrs,
            _ => return visit::visit_item(self, item),
        };
        if test_only(attributes) {
            return;
        }
        if !matches!(item, Item::Const(_)) {
            self.fixed(attributes, false);
        }
        visit::visit_item(self, item);
    }

    fn visit_item_const(&mut self, value: &'ast syn::ItemConst) {
        if !self.constant(&value.attrs, &value.ty, value.const_token.span) {
            visit::visit_item_const(self, value);
        }
    }
    fn visit_impl_item_const(&mut self, value: &'ast syn::ImplItemConst) {
        if !self.constant(&value.attrs, &value.ty, value.const_token.span) {
            visit::visit_impl_item_const(self, value);
        }
    }
    fn visit_trait_item_const(&mut self, value: &'ast syn::TraitItemConst) {
        if !self.constant(&value.attrs, &value.ty, value.const_token.span) {
            visit::visit_trait_item_const(self, value);
        }
    }

    fn visit_item_impl(&mut self, value: &'ast syn::ItemImpl) {
        let trait_name = if let Some((_, path, _)) = &value.trait_
            && let Some(part) = path.segments.last()
        {
            self.declarations.canonical(&part.ident.unraw().to_string())
        } else {
            None
        };
        if trait_name.as_deref() == Some("Default")
            && type_name(&value.self_ty)
                .is_some_and(|name| self.declarations.settings.contains(&name))
        {
            return;
        }
        for item in &value.items {
            if let syn::ImplItem::Fn(method) = item {
                if test_only(&method.attrs) {
                    continue;
                }
                if trait_name.as_deref() == Some("SettingDefinition")
                    && method.sig.ident.unraw() == "default_value"
                {
                    continue;
                }
            }
            self.visit_impl_item(item);
        }
    }

    fn visit_expr_call(&mut self, value: &'ast syn::ExprCall) {
        if let Expr::Path(function) = &*value.func
            && self.policy(&function.path)
            && literal(&value.args)
        {
            self.site(value.span());
        }
        visit::visit_expr_call(self, value);
    }
    fn visit_expr_method_call(&mut self, value: &'ast syn::ExprMethodCall) {
        let method = value.method.unraw().to_string();
        if (method.starts_with("with_capacity") || method == "add_permits") && literal(&value.args)
        {
            self.site(value.span());
        }
        visit::visit_expr_method_call(self, value);
    }
    fn visit_macro(&mut self, value: &'ast syn::Macro) {
        fragment(self, value.tokens.clone());
    }
}
