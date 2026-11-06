//! Repository tooling, outside the product layer order.
//! Owns: authored language-import provenance before configuration selection.
//! Depends on: Cargo metadata, Git source discovery, parsed Rust modules and layer attributes.
//! Must not know: compiler-resolved APIs, product execution or filename-based architecture policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context as _, Result, ensure};
use proc_macro2::Span;
use serde::Deserialize;
use syn::{
    Attribute, Item, Meta, UseTree,
    visit::{self, Visit},
};

const PERMITTED: [&str; 3] = ["language", "edge", "harness"];

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    manifest_path: PathBuf,
    targets: Vec<Target>,
    dependencies: Vec<Dependency>,
}

#[derive(Deserialize)]
struct Target {
    src_path: PathBuf,
    kind: Vec<String>,
}

#[derive(Deserialize)]
struct Dependency {
    name: String,
    rename: Option<String>,
}

fn tracked(root: &Path) -> Result<BTreeSet<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ])
        .output()
        .context("cannot discover tracked and new source files")?;
    ensure!(
        output.status.success(),
        "Git source discovery failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(|entry| String::from_utf8_lossy(entry).into_owned())
        .filter(|path: &String| root.join(path).is_file())
        .collect())
}

fn metadata(root: &Path, tracked: &BTreeSet<String>) -> Result<Vec<Package>> {
    let mut roots = Vec::new();
    for manifest in tracked.iter().filter(|path| path.ends_with("Cargo.toml")) {
        let document: toml::Value = toml::from_str(&fs::read_to_string(root.join(manifest))?)?;
        if document.get("workspace").is_some() {
            roots.push(root.join(manifest));
        }
    }
    roots.sort();
    roots.dedup();
    let mut packages = BTreeMap::new();
    for manifest in roots {
        let output = Command::new("cargo")
            .args([
                "metadata",
                "--no-deps",
                "--format-version",
                "1",
                "--manifest-path",
            ])
            .arg(&manifest)
            .current_dir(root)
            .output()
            .with_context(|| format!("cannot read Cargo metadata from {}", manifest.display()))?;
        ensure!(
            output.status.success(),
            "Cargo metadata failed for {}: {}",
            manifest.display(),
            String::from_utf8_lossy(&output.stderr)
        );
        let document: Metadata = serde_json::from_slice(&output.stdout)?;
        for package in document.packages {
            packages.insert(package.manifest_path.clone(), package);
        }
    }
    Ok(packages.into_values().collect())
}

fn relative(root: &Path, path: &Path) -> Result<String> {
    let root = root.canonicalize()?;
    let path = path
        .canonicalize()
        .with_context(|| format!("cannot resolve {}", path.display()))?;
    Ok(path
        .strip_prefix(&root)
        .with_context(|| format!("{} is outside {}", path.display(), root.display()))?
        .to_string_lossy()
        .replace('\\', "/"))
}

fn layer_meta(meta: &Meta) -> Option<String> {
    let Meta::List(list) = meta else {
        return None;
    };
    let path = list
        .path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>();
    if path == ["nervix", "layer"] {
        let mut kind = None;
        let mut reason = None;
        let values = list
            .parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated)
            .ok()?;
        for value in values {
            match value {
                Meta::Path(path) if path.segments.len() == 1 => {
                    kind = Some(path.segments[0].ident.to_string())
                }
                Meta::NameValue(value) if value.path.is_ident("reason") => {
                    if let syn::Expr::Lit(expression) = value.value
                        && let syn::Lit::Str(text) = expression.lit
                    {
                        reason = Some(text.value());
                    }
                }
                _ => {}
            }
        }
        return kind.filter(|_| {
            reason
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
        });
    }
    if list.path.is_ident("cfg_attr") {
        let values = list
            .parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated)
            .ok()?;
        return values.iter().skip(1).find_map(layer_meta);
    }
    None
}

fn layer(attrs: &[Attribute]) -> Option<String> {
    attrs
        .iter()
        .rev()
        .find_map(|attribute| layer_meta(&attribute.meta))
}

fn permitted(owner: Option<&str>) -> bool {
    owner.is_some_and(|value| PERMITTED.contains(&value))
}

fn has_permitted(attrs: &[Attribute]) -> bool {
    attrs
        .iter()
        .any(|attribute| permitted(layer_meta(&attribute.meta).as_deref()))
}

fn module_directory(file: &Path) -> PathBuf {
    let filename = file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let parent = file.parent().unwrap_or_else(|| Path::new(""));
    if ["build.rs", "lib.rs", "main.rs", "mod.rs"].contains(&filename)
        || parent
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| ["bin", "benches", "examples", "tests"].contains(&name))
    {
        parent.to_owned()
    } else {
        parent.join(file.file_stem().unwrap_or_default())
    }
}

fn path_attr(attrs: &[Attribute]) -> Option<String> {
    attrs.iter().find_map(|attribute| {
        if !attribute.path().is_ident("path") {
            return None;
        }
        let Meta::NameValue(value) = &attribute.meta else {
            return None;
        };
        let syn::Expr::Lit(expression) = &value.value else {
            return None;
        };
        let syn::Lit::Str(text) = &expression.lit else {
            return None;
        };
        Some(text.value())
    })
}

fn use_roots(tree: &UseTree, roots: &mut Vec<(String, Span)>) {
    match tree {
        UseTree::Path(path) => roots.push((path.ident.to_string(), path.ident.span())),
        UseTree::Name(name) => roots.push((name.ident.to_string(), name.ident.span())),
        UseTree::Rename(rename) => roots.push((rename.ident.to_string(), rename.ident.span())),
        UseTree::Glob(_) => {}
        UseTree::Group(group) => {
            for item in &group.items {
                use_roots(item, roots);
            }
        }
    }
}

struct Imports<'a> {
    aliases: &'a BTreeSet<String>,
    found: BTreeSet<(String, usize)>,
    owner: Option<String>,
    approved: bool,
}

impl Imports<'_> {
    fn record(&mut self, alias: &str, span: Span) {
        if self.aliases.contains(alias) && !permitted(self.owner.as_deref()) {
            self.found.insert((alias.into(), span.start().line));
        }
    }

    fn scope(&mut self, attrs: &[Attribute], visit: impl FnOnce(&mut Self)) {
        let previous = self.owner.clone();
        self.owner = layer(attrs).or_else(|| previous.clone());
        self.approved |= permitted(self.owner.as_deref()) || has_permitted(attrs);
        visit(self);
        self.owner = previous;
    }
}

impl<'ast> Visit<'ast> for Imports<'_> {
    fn visit_item(&mut self, item: &'ast Item) {
        self.scope(item_attrs(item), |this| visit::visit_item(this, item));
    }

    fn visit_impl_item(&mut self, item: &'ast syn::ImplItem) {
        let attrs: &[Attribute] = match item {
            syn::ImplItem::Const(value) => &value.attrs,
            syn::ImplItem::Fn(value) => &value.attrs,
            syn::ImplItem::Type(value) => &value.attrs,
            syn::ImplItem::Macro(value) => &value.attrs,
            _ => &[],
        };
        self.scope(attrs, |this| visit::visit_impl_item(this, item));
    }

    fn visit_trait_item(&mut self, item: &'ast syn::TraitItem) {
        let attrs: &[Attribute] = match item {
            syn::TraitItem::Const(value) => &value.attrs,
            syn::TraitItem::Fn(value) => &value.attrs,
            syn::TraitItem::Type(value) => &value.attrs,
            syn::TraitItem::Macro(value) => &value.attrs,
            _ => &[],
        };
        self.scope(attrs, |this| visit::visit_trait_item(this, item));
    }

    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        let mut roots = Vec::new();
        use_roots(&item.tree, &mut roots);
        for (alias, span) in roots {
            self.record(&alias, span);
        }
    }

    fn visit_item_extern_crate(&mut self, item: &'ast syn::ItemExternCrate) {
        self.record(&item.ident.to_string(), item.ident.span());
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        if path.segments.len() >= 2 {
            let first = &path.segments[0];
            self.record(&first.ident.to_string(), first.ident.span());
        }
        visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        super::fragment(self, item.tokens.clone());
    }
}

struct Walk<'a> {
    root: &'a Path,
    sources: &'a BTreeSet<String>,
    aliases: &'a BTreeSet<String>,
    problems: BTreeSet<String>,
    approved: bool,
    reached: BTreeSet<(String, Option<String>)>,
}

impl Walk<'_> {
    fn file(&mut self, path: &str, inherited: Option<String>) -> Result<()> {
        if !self.reached.insert((path.into(), inherited.clone())) {
            return Ok(());
        }
        if !self.sources.contains(path) {
            return Ok(());
        }
        let source = fs::read_to_string(self.root.join(path))
            .with_context(|| format!("cannot read {path}"))?;
        let file = syn::parse_file(&source)
            .with_context(|| format!("cannot parse authored Rust at {path}"))?;
        self.approved |= has_permitted(&file.attrs);
        let owner = layer(&file.attrs).or(inherited);
        self.items(
            path,
            &file.items,
            owner,
            module_directory(Path::new(path)),
            false,
        )
    }

    fn items(
        &mut self,
        path: &str,
        items: &[Item],
        owner: Option<String>,
        directory: PathBuf,
        inline: bool,
    ) -> Result<()> {
        if permitted(owner.as_deref()) {
            self.approved = true;
        }
        for item in items {
            self.approved |= has_permitted(item_attrs(item));
            let own = layer(item_attrs(item)).or_else(|| owner.clone());
            if permitted(own.as_deref()) {
                self.approved = true;
            }
            if let Item::Mod(module) = item {
                let target = if let Some(override_path) = path_attr(&module.attrs) {
                    (if inline {
                        directory.clone()
                    } else {
                        Path::new(path)
                            .parent()
                            .unwrap_or_else(|| Path::new(""))
                            .to_owned()
                    })
                    .join(override_path)
                } else {
                    directory.join(module.ident.to_string())
                };
                if let Some((_, contents)) = &module.content {
                    self.items(path, contents, own, target, true)?;
                } else {
                    let candidates = if path_attr(&module.attrs).is_some() {
                        vec![target]
                    } else {
                        vec![target.with_extension("rs"), target.join("mod.rs")]
                    };
                    for candidate in candidates {
                        let full = self.root.join(&candidate);
                        if full.exists() {
                            let child = relative(self.root, &full)?;
                            if self.sources.contains(&child) {
                                self.file(&child, own.clone())?;
                            }
                            break;
                        }
                    }
                }
                continue;
            }
            let mut imports = Imports {
                aliases: self.aliases,
                found: BTreeSet::new(),
                owner: own,
                approved: false,
            };
            imports.visit_item(item);
            self.approved |= imports.approved;
            for (alias, line) in imports.found {
                self.problems.insert(format!(
                    "{path}:{line}: language import provenance `{alias}` requires a source-owned \
                     language, edge or harness layer"
                ));
            }
        }
        Ok(())
    }
}

fn item_attrs(item: &Item) -> &[Attribute] {
    match item {
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
    }
}

pub fn check(root: &Path) -> Result<Vec<String>> {
    let tracked = tracked(root)?;
    let packages = metadata(root, &tracked)?;
    check_sources(root, &tracked, &packages)
}

fn check_sources(
    root: &Path,
    tracked: &BTreeSet<String>,
    packages: &[Package],
) -> Result<Vec<String>> {
    let sources: BTreeSet<_> = tracked
        .iter()
        .filter(|path| path.ends_with(".rs"))
        .cloned()
        .collect();
    let roots: BTreeMap<_, Vec<_>> = packages
        .iter()
        .map(|package| {
            let mut entries = Vec::new();
            for target in &package.targets {
                if target
                    .kind
                    .iter()
                    .any(|kind| kind == "lib" || kind == "bin" || kind == "proc-macro")
                {
                    if let Ok(path) = relative(root, &target.src_path) {
                        if sources.contains(&path) {
                            entries.push(path);
                        }
                    }
                }
            }
            if entries.is_empty() {
                for target in &package.targets {
                    if target.kind.iter().any(|kind| kind == "test") {
                        if let Ok(path) = relative(root, &target.src_path) {
                            if sources.contains(&path) {
                                entries.push(path);
                            }
                        }
                    }
                }
            }
            (package.name.clone(), entries)
        })
        .collect();
    let mut language = BTreeSet::new();
    for (name, entries) in &roots {
        for path in entries {
            let source = fs::read_to_string(root.join(path))
                .with_context(|| format!("cannot read {path}"))?;
            // A language layer attribute must spell both identifiers in the authored source.
            if !source.contains("nervix") || !source.contains("language") {
                continue;
            }
            let parsed = syn::parse_file(&source)
                .with_context(|| format!("cannot parse authored Rust at {path}"))?;
            if layer(&parsed.attrs).as_deref() == Some("language") {
                language.insert(name.as_str());
                break;
            }
        }
    }
    let mut problems = BTreeSet::new();
    for package in packages {
        let dependencies: BTreeMap<_, _> = package
            .dependencies
            .iter()
            .filter(|dependency| language.contains(dependency.name.as_str()))
            .map(|dependency| {
                (
                    dependency
                        .rename
                        .clone()
                        .unwrap_or_else(|| dependency.name.clone())
                        .replace('-', "_"),
                    dependency,
                )
            })
            .collect();
        if dependencies.is_empty() {
            continue;
        }
        let aliases: BTreeSet<_> = dependencies.keys().cloned().collect();
        let mut walk = Walk {
            root,
            sources: &sources,
            aliases: &aliases,
            problems: BTreeSet::new(),
            approved: false,
            reached: BTreeSet::new(),
        };
        for path in roots.get(&package.name).into_iter().flatten() {
            walk.file(path, None)?;
        }
        problems.extend(walk.problems);
        if !walk.approved {
            let manifest = relative(root, &package.manifest_path)?;
            for (alias, _) in dependencies {
                problems.insert(format!(
                    "{manifest}: language dependency `{alias}` has no source-owned language, edge \
                     or harness consumer"
                ));
            }
        }
    }
    Ok(problems.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(consumer: &str, extra: &[(&str, &str)], test_only: bool) -> Vec<String> {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let mut tracked = BTreeSet::new();
        let files = [
            ("src/lib.rs", consumer),
            (
                "syntax/src/lib.rs",
                "#![cfg_attr(nervix_lint, nervix::layer(language, reason=\"grammar\"))]\npub \
                 struct Token;\n",
            ),
        ];
        for (path, body) in files.into_iter().chain(extra.iter().copied()) {
            let destination = root.join(path);
            fs::create_dir_all(destination.parent().unwrap()).unwrap();
            fs::write(destination, body).unwrap();
            tracked.insert(path.into());
        }
        let consumer_root = if test_only { "suite.rs" } else { "src/lib.rs" };
        if test_only {
            fs::write(root.join(consumer_root), consumer).unwrap();
            tracked.insert(consumer_root.into());
        }
        for path in ["Cargo.toml", "syntax/Cargo.toml"] {
            fs::write(
                root.join(path),
                "[package]\nname=\"fixture\"\nversion=\"0.1.0\"\n",
            )
            .unwrap();
            tracked.insert(path.into());
        }
        let packages = vec![
            Package {
                name: "consumer".into(),
                manifest_path: root.join("Cargo.toml"),
                targets: vec![Target {
                    src_path: root.join(consumer_root),
                    kind: vec![if test_only { "test" } else { "lib" }.into()],
                }],
                dependencies: vec![Dependency {
                    name: "syntax-engine".into(),
                    rename: Some("syntax".into()),
                }],
            },
            Package {
                name: "syntax-engine".into(),
                manifest_path: root.join("syntax/Cargo.toml"),
                targets: vec![Target {
                    src_path: root.join("syntax/src/lib.rs"),
                    kind: vec!["lib".into()],
                }],
                dependencies: Vec::new(),
            },
        ];
        check_sources(root, &tracked, &packages).unwrap()
    }

    const ENGINE: &str = "#![cfg_attr(nervix_lint, nervix::layer(engine, reason=\"worker\"))]\n";

    #[test]
    fn inactive_import_and_unowned_dependency_are_both_reported() {
        let problems = fixture(
            &format!("{ENGINE}#[cfg(any())] use syntax as hidden;\n"),
            &[],
            false,
        );
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("src/lib.rs:2:"))
        );
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("Cargo.toml: language dependency"))
        );
    }

    #[test]
    fn inline_and_explicit_path_modules_keep_their_own_owner() {
        let source = format!(
            "{ENGINE}#[cfg_attr(nervix_lint, nervix::layer(edge, reason=\"adapter\"))]\nmod \
             adapter {{ use syntax as parsed; mod nested; }}\n#[path=\"worker.rs\"] mod worker;\n"
        );
        let problems = fixture(
            &source,
            &[
                ("src/adapter/nested.rs", "use syntax::Token;"),
                (
                    "src/worker.rs",
                    "#[cfg(any())] extern crate syntax as parsed;",
                ),
            ],
            false,
        );
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("src/worker.rs:"));
    }

    #[test]
    fn macro_token_import_is_checked_and_fake_contracts_do_not_approve() {
        let source = format!(
            "{ENGINE}// #[cfg_attr(nervix_lint, nervix::layer(edge, reason=\"fake\"))]\nconst \
             TEXT: &str = r##\"#[cfg_attr(nervix_lint, nervix::layer(edge, \
             reason=\\\"fake\\\"))]\"##;\nmacro_rules! reads {{ () => {{ let _ = syntax::Token; \
             }} }}\n"
        );
        let problems = fixture(&source, &[], false);
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("src/lib.rs:4:")),
            "{problems:?}"
        );
    }

    #[test]
    fn test_only_target_and_grouped_alias_keep_their_owner() {
        let allowed = "#![cfg_attr(nervix_lint, nervix::layer(harness, reason=\"suite\"))]\nuse \
                       {syntax as parser};\n";
        assert!(fixture(allowed, &[], true).is_empty());
        let unowned = format!("{ENGINE}use {{syntax as parser}};\n");
        assert_eq!(fixture(&unowned, &[], true).len(), 2);
    }

    #[test]
    fn method_contracts_and_inner_module_attributes_are_the_narrower_owner() {
        let methods = format!(
            "{ENGINE}struct Worker; impl Worker {{\n#[cfg_attr(nervix_lint, nervix::layer(edge, \
             reason=\"adapter\"))]\nfn allowed() {{ let _ = syntax::Token; }}\nfn denied() {{ let \
             _ = syntax::Token; }}\n}}"
        );
        let problems = fixture(&methods, &[], false);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("src/lib.rs:5:"), "{problems:?}");

        let nested = format!(
            "{ENGINE}#[cfg_attr(nervix_lint, nervix::layer(edge, reason=\"adapter\"))]\nmod scope \
             {{\n#![cfg_attr(nervix_lint, nervix::layer(engine, reason=\"worker\"))]\nuse \
             syntax::Token;\n}}"
        );
        let problems = fixture(&nested, &[], false);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("src/lib.rs:5:"), "{problems:?}");
    }

    #[test]
    fn path_overrides_keep_inline_and_out_of_line_module_ownership() {
        let inline = format!(
            "{ENGINE}#[path=\"worker_files\"]\n#[cfg_attr(nervix_lint, nervix::layer(engine, \
             reason=\"worker\"))]\nmod worker {{ #[path=\"intake.rs\"] mod input; }}\n"
        );
        let problems = fixture(
            &inline,
            &[(
                "src/worker_files/intake.rs",
                "#[cfg(any())] use syntax::Token;",
            )],
            false,
        );
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("src/worker_files/intake.rs:"))
        );
        let relocated = format!("{ENGINE}#[path=\"bridge.rs\"] mod bridge;\n");
        let problems = fixture(
            &relocated,
            &[
                (
                    "src/bridge.rs",
                    "#![cfg_attr(nervix_lint, nervix::layer(edge, \
                     reason=\"adapter\"))]\n#[path=\"intake.rs\"] mod input;",
                ),
                ("src/intake.rs", "use syntax::Token;"),
            ],
            false,
        );
        assert!(problems.is_empty(), "{problems:?}");
    }

    #[test]
    fn vocabulary_reexports_have_no_language_provenance() {
        let source = format!("{ENGINE}use syntax::Token;\n");
        let problems = fixture(
            &source,
            &[(
                "syntax/src/lib.rs",
                "#![cfg_attr(nervix_lint, nervix::layer(vocabulary, reason=\"model\"))]\npub \
                 struct Token;",
            )],
            false,
        );
        assert!(problems.is_empty(), "{problems:?}");
    }
}
