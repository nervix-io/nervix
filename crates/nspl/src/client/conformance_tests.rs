//! Conformance tests generated from every client's declaration.
//!
//! Layer: test harness.
//!
//! - **Owns.** The `CREATE CLIENT` statements each connector's client declaration says the grammar
//!   accepts, refuses and completes: whether the client bounds a pool, and the options it writes.
//! - **Depends on.** The connector vocabulary's client declarations and the composed statement
//!   grammar.
//! - **Must not know.** What a configuration key means to any connector.

use std::collections::BTreeMap;

use nervix_models::{ClientDeclaration, ClientPooling, ConnectorKind};
use strum::IntoEnumIterator as _;

use crate::conformance_support::{assert_offers_exactly, created_model, refused};

/// The pool bounds a generated statement of a pooled client writes.
const POOL: &str = "POOL SIZE MIN 1 MAX 4";

/// Every connector a client Model configures, with what its clients declare.
fn every_client() -> Vec<(ConnectorKind, ClientDeclaration)> {
    let mut clients = Vec::new();
    for connector in ConnectorKind::iter() {
        if let Some(declaration) = connector.client() {
            clients.push((connector, declaration));
        }
    }
    clients
}

/// A `CREATE CLIENT` statement of `connector` writing `between` after its kind.
fn statement(connector: ConnectorKind, between: &[&str]) -> String {
    let mut words = vec![format!(
        "CREATE CLIENT generated TYPE {}",
        connector.keyword()
    )];
    for written in between {
        words.push((*written).to_owned());
    }
    words.push("CONFIG {'key' = 'value'};".to_owned());
    words.join(" ")
}

/// What a statement of a client with `pooling` writes after its kind before anything else.
fn pool_bounds(pooling: ClientPooling) -> Vec<&'static str> {
    match pooling {
        ClientPooling::Pooled => vec![POOL],
        ClientPooling::Unpooled => Vec::new(),
    }
}

#[test]
fn every_client_parses_into_the_connector_it_configures() {
    for (connector, declaration) in every_client() {
        let text = statement(connector, &pool_bounds(declaration.pooling));
        let Some(model) = created_model(&text) else {
            panic!("the grammar refuses what {connector} declares: {text}");
        };
        assert_eq!(model.connector_kind(), Some(connector), "{text}");
    }
}

#[test]
fn a_pool_is_written_exactly_where_the_client_declares_one() {
    for (connector, declaration) in every_client() {
        let text = match declaration.pooling {
            ClientPooling::Pooled => statement(connector, &[]),
            ClientPooling::Unpooled => statement(connector, &[POOL]),
        };
        assert!(
            refused(&text),
            "the grammar accepts a {connector} client whose pool contradicts its declaration: \
             {text}"
        );
    }
}

#[test]
fn every_declared_option_parses_and_no_other_does() {
    let mut examples: BTreeMap<&'static str, Vec<&'static str>> = BTreeMap::new();
    for (_, declaration) in every_client() {
        for option in declaration.options {
            examples
                .entry(option.keywords)
                .or_default()
                .extend(option.examples());
        }
    }
    for (connector, declaration) in every_client() {
        let pool = pool_bounds(declaration.pooling);
        for (keywords, written) in &examples {
            let declared = declaration
                .options
                .iter()
                .any(|option| option.keywords == *keywords);
            for example in written {
                let mut between = pool.clone();
                between.push(example);
                let text = statement(connector, &between);
                if declared {
                    assert!(created_model(&text).is_some(), "{text}");
                } else {
                    assert!(refused(&text), "{connector} declares no {keywords}: {text}");
                }
            }
        }
    }
}

#[test]
fn completion_after_the_kind_offers_exactly_what_the_client_declares() {
    for (connector, declaration) in every_client() {
        let prefix = format!("CREATE CLIENT generated TYPE {} ", connector.keyword());
        let mut continuations = Vec::new();
        match declaration.pooling {
            ClientPooling::Pooled => continuations.push(POOL),
            ClientPooling::Unpooled => {
                for option in declaration.options {
                    continuations.extend(option.examples());
                }
                continuations.push("MOUNT certificates");
                continuations.push("CONFIG {'key' = 'value'}");
            }
        }
        assert_offers_exactly(&prefix, &continuations);
    }
}
