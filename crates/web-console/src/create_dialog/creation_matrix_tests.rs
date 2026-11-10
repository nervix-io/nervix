//! The coverage gate of the visual create forms.
//!
//! Layer: edges.
//!
//! - **Owns.** Checks that the Create menu offers every statement outside the Model vocabulary
//!   and every Model kind exactly once, under its own key, and that each family's form renders.
//!   Every dispatch over [`CreateKind`] is exhaustive, so a kind added to the vocabulary does not
//!   compile until the console decides its form; these checks keep the menu complete too.
//! - **Depends on.** The create dialog and menu components and the vocabulary's kind list.
//! - **Must not know.** Browser transport or how a server applies a statement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::collections::BTreeSet;

use leptos::prelude::*;
use meticulous::ResultExt as _;
use nervix_models::{DomainName, ModelKind};
use strum::IntoEnumIterator as _;

use super::{
    CreateDialog, CreateDialogProps, CreateKind, CreateMenu, CreateMenuProps, CreateSignals,
};

#[test]
fn every_model_kind_and_every_statement_outside_the_vocabulary_has_one_menu_family() {
    let mut offered = BTreeSet::new();
    for kind in CreateKind::MENU {
        assert!(offered.insert(kind), "{kind:?} is offered twice");
    }
    let mut expected = BTreeSet::from([
        CreateKind::Domain,
        CreateKind::User,
        CreateKind::Resource,
        CreateKind::Subscription,
    ]);
    for kind in ModelKind::iter() {
        expected.insert(CreateKind::Model(kind));
    }
    assert_eq!(
        offered, expected,
        "the Create menu offers exactly the current creation families"
    );

    let mut keys = BTreeSet::new();
    for kind in CreateKind::MENU {
        assert!(keys.insert(kind.key()), "{kind:?} shares its menu key");
    }
    assert_eq!(
        CreateKind::Model(ModelKind::Lookup).key(),
        "hash-map",
        "a Model family's key is its keyword phrase"
    );
    assert_eq!(
        CreateKind::Model(ModelKind::WireJsonSchema).key(),
        "wire-json-schema"
    );
}

#[test]
fn the_menu_lists_and_the_dialog_opens_every_family_form() {
    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let scope = DomainName::parse("orders").assured("valid domain");
        let active_domain = RwSignal::new(Some(scope.clone()));
        let signals = CreateSignals::new();
        let menu = CreateMenu(
            CreateMenuProps::builder()
                .signals(signals)
                .active_domain(active_domain)
                .build(),
        );
        let menu = menu.to_html();
        for kind in CreateKind::MENU {
            let entry = kind.menu_entry();
            assert!(
                menu.contains(&format!("data-create-kind=\"{}\"", kind.key())),
                "the menu offers {kind:?}"
            );
            assert!(menu.contains(entry.name), "the menu names {kind:?}");
        }

        for kind in CreateKind::MENU {
            signals.open(kind, Some(scope.clone()), "global-create-button");
            let dialog = CreateDialog(
                CreateDialogProps::builder()
                    .signals(signals)
                    .active_domain(active_domain)
                    .connection_state(RwSignal::new(super::super::ConsoleConnectionState::Waiting))
                    .session_generation(RwSignal::new(1))
                    .request_tx(RwSignal::new(None))
                    .submit(super::ignore_test_submission)
                    .build(),
            );
            any_spawner::Executor::poll_local();
            let html = dialog.to_html();
            assert!(
                html.contains(&format!("Create {}", kind.label())),
                "the dialog opens the {kind:?} form"
            );
        }
    });
}
