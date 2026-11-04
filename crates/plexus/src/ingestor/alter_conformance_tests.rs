//! `ALTER INGESTOR` conformance tests generated from every ingestor entity's declaration.
//!
//! Layer: test harness.
//!
//! - **Owns.** The operations each ingestor entity's declaration says an `ALTER` accepts, refuses
//!   and completes: its locators, options, delivery and quiesce modes, its client, the
//!   construction of its routes and its general error policy, each checked by applying the parsed
//!   alteration to an ingestor a generated `CREATE` declares and by reading the alteration's
//!   canonical Plexus back as the same alteration.
//! - **Depends on.** The generated `CREATE` statements of the enclosing conformance tests, the
//!   connector vocabulary's declarations and the composed statement grammar.
//! - **Must not know.** Any one connector's grammar module.
//!
//! A `SET` of a part is checked against the ingestor a `CREATE` writing that part declares, so an
//! alteration changes exactly the part it names, the way a statement that wrote it would have.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_models::{
    AlterIngestor, AlterIngestorError, CreateIngestor, GeneralErrorPolicy, IngestQuiesceKind,
    IngestorEntityKind, IngestorReferenceKind, Model, OptionDeclaration, OptionPresence,
    RouteConstruction, Statement,
};
use strum::IntoEnumIterator as _;

use super::{Generated, every_delivery_spelling, unconditional_delivery, unconditional_quiesce};
use crate::{
    conformance_support::{
        assert_offers_exactly, assert_reads_back_canonically, created_model, refused,
    },
    statement::parse_statement,
};

/// `ALTER <head> INGESTOR generated <operations>;` for an ingestor of `kind`.
fn alter_text(kind: IngestorEntityKind, operations: &str) -> String {
    format!("ALTER {} INGESTOR generated {operations};", kind.phrase())
}

/// The alteration the composed grammar reads from `ALTER … <operations>`, which canonical Plexus
/// writes back as the same alteration.
fn parsed_alter(kind: IngestorEntityKind, operations: &str) -> AlterIngestor {
    let text = alter_text(kind, operations);
    let Ok(statement) = parse_statement(&text) else {
        panic!("the grammar refuses what {kind} declares: {text}");
    };
    assert_reads_back_canonically(&statement);
    let Statement::AlterIngestor(alter) = statement else {
        panic!("the grammar reads another statement than an ingestor alteration from {text}");
    };
    alter
}

/// The ingestor `created` declares, after the `ALTER` that writes `operations` applies to it.
fn altered(created: &Generated, operations: &str) -> CreateIngestor {
    let alter = parsed_alter(created.kind, operations);
    let mut ingestor = created.parse();
    if let Err(error) = ingestor.apply_alter(&alter) {
        panic!("{} cannot take `{operations}`: {error:?}", created.text());
    }
    ingestor
}

/// Why the `ALTER` that writes `operations` does not apply to the ingestor `created` declares.
fn alteration_error(created: &Generated, operations: &str) -> AlterIngestorError {
    let alter = parsed_alter(created.kind, operations);
    let mut ingestor = created.parse();
    let Err(error) = ingestor.apply_alter(&alter) else {
        panic!(
            "{} takes `{operations}`, which it must refuse",
            created.text()
        );
    };
    error.current_context().clone()
}

/// Checks that the grammar refuses `ALTER … <operations>`, which `kind` does not declare.
fn assert_alter_refused(kind: IngestorEntityKind, operations: &str) {
    let text = alter_text(kind, operations);
    assert!(
        refused(&text),
        "the grammar accepts `{operations}`, which {kind} does not declare: {text}"
    );
}

/// The options of `kind` a targeted `SET` or `DROP` changes. `INSTANCES` is left to `SET FROM`:
/// how many consumers read an entity together belongs to the partitioning of its reads, which
/// changes it as a whole.
fn alterable_options(kind: IngestorEntityKind) -> Vec<&'static OptionDeclaration> {
    let mut alterable = Vec::new();
    for option in kind.capabilities().options {
        if option.keywords == OptionDeclaration::INSTANCES.keywords {
            continue;
        }
        alterable.push(option);
    }
    alterable
}

/// The construction of the generated statement's one route, to `events`.
fn route_construction(ingestor: &CreateIngestor) -> &RouteConstruction {
    let [route] = ingestor.output_routes.routes.as_slice() else {
        panic!("the generated statement writes one route");
    };
    assert_eq!(route.relay.as_str(), "events");
    &route.construction
}

#[test]
fn set_writes_each_declared_locator_as_a_statement_writes_it() {
    for kind in IngestorEntityKind::all() {
        for locator in kind.locators() {
            let operations = format!("SET {}", locator.example());
            let written = Generated {
                locator: Some(locator.example()),
                ..Generated::of(kind)
            };

            assert_eq!(
                altered(&Generated::of(kind), &operations),
                written.parse(),
                "{kind}: `{operations}`"
            );
        }
    }
}

#[test]
fn set_writes_each_declared_option_as_a_statement_writes_it() {
    for kind in IngestorEntityKind::all() {
        for option in alterable_options(kind) {
            for example in option.examples() {
                let operations = format!("SET {example}");
                let written = Generated {
                    written: Some((option.keywords, example.to_owned())),
                    ..Generated::of(kind)
                };

                assert_eq!(
                    altered(&Generated::of(kind), &operations),
                    written.parse(),
                    "{kind}: `{operations}` writes what `{}` declares",
                    written.text()
                );
            }
        }
    }
}

#[test]
fn drop_removes_each_optional_option_and_refuses_one_never_written() {
    for kind in IngestorEntityKind::all() {
        for option in alterable_options(kind) {
            let operations = format!("DROP {}", option.keywords);
            if let OptionPresence::Required = option.presence {
                assert_alter_refused(kind, &operations);
                continue;
            }
            for example in option.examples() {
                let written = Generated {
                    written: Some((option.keywords, example.to_owned())),
                    ..Generated::of(kind)
                };

                assert_eq!(
                    altered(&written, &operations),
                    Generated::of(kind).parse(),
                    "{kind}: `{operations}` on `{}`",
                    written.text()
                );
            }
            let error = alteration_error(&Generated::of(kind), &operations);
            assert!(
                matches!(error, AlterIngestorError::OptionNotWritten { .. }),
                "{kind}: `{operations}` on an ingestor that writes none: {error:?}"
            );
        }
    }
}

#[test]
fn set_mode_writes_each_declared_delivery_mode() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        let quiesce = unconditional_quiesce(&declared);
        for delivery in declared.delivery {
            let operations = format!("SET MODE {}", delivery.example);
            let created = Generated::of(kind).choosing(delivery.requires);
            let written = Generated {
                delivery: delivery.example,
                ..Generated::of(kind)
            }
            .choosing(delivery.requires);
            if !delivery.kind.allows_quiesce(quiesce) {
                // The grammar reads the change; the registry refuses the ingestor it makes, whose
                // quiesce mode this delivery cannot honor.
                parsed_alter(kind, &operations);
                continue;
            }

            assert_eq!(
                altered(&created, &operations),
                written.parse(),
                "{kind}: `{operations}`"
            );
        }
    }
}

#[test]
fn set_mode_refuses_a_delivery_mode_the_entity_does_not_declare() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        for spelling in every_delivery_spelling() {
            if declared.declared_delivery(spelling.kind).is_some() {
                continue;
            }
            assert_alter_refused(kind, &format!("SET MODE {}", spelling.example));
        }
    }
}

#[test]
fn completion_after_set_mode_offers_exactly_the_declared_delivery_modes() {
    for kind in IngestorEntityKind::all() {
        let mut spellings = Vec::new();
        for delivery in kind.capabilities().delivery {
            spellings.push(delivery.example);
        }
        let prefix = format!("ALTER {} INGESTOR generated SET MODE ", kind.phrase());

        assert_offers_exactly(&prefix, &spellings);
    }
}

#[test]
fn set_quiesce_writes_each_declared_quiesce_mode() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        let delivery = unconditional_delivery(&declared);
        for quiesce in declared.quiesce {
            let operations = format!("SET QUIESCE {}", quiesce.kind.example());
            let created = Generated::of(kind).choosing(quiesce.requires);
            let written = Generated {
                quiesce: quiesce.kind.example(),
                ..Generated::of(kind)
            }
            .choosing(quiesce.requires);
            if !delivery.kind.allows_quiesce(quiesce.kind) {
                parsed_alter(kind, &operations);
                continue;
            }

            assert_eq!(
                altered(&created, &operations),
                written.parse(),
                "{kind}: `{operations}`"
            );
        }
    }
}

#[test]
fn set_quiesce_offers_and_accepts_exactly_the_declared_quiesce_modes() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        let mut spellings = Vec::new();
        for quiesce in declared.quiesce {
            spellings.push(quiesce.kind.example());
        }
        let prefix = format!("ALTER {} INGESTOR generated SET QUIESCE ", kind.phrase());
        assert_offers_exactly(&prefix, &spellings);
        for quiesce in IngestQuiesceKind::iter() {
            if declared.declared_quiesce(quiesce).is_some() {
                continue;
            }
            assert_alter_refused(kind, &format!("SET QUIESCE {}", quiesce.example()));
        }
    }
}

#[test]
fn set_client_replaces_the_declared_client_and_is_refused_elsewhere() {
    for kind in IngestorEntityKind::all() {
        let IngestorReferenceKind::Client(_) = kind.capabilities().reference else {
            assert_alter_refused(kind, "SET CLIENT other_client");
            continue;
        };
        let created = Generated::of(kind);
        let written = created.text().replacen("source_client", "other_client", 1);
        let Some(Model::Ingestor(expected)) = created_model(&written) else {
            panic!("the grammar refuses what {kind} declares: {written}");
        };

        assert_eq!(
            altered(&created, "SET CLIENT other_client"),
            expected,
            "{kind}: SET CLIENT other_client"
        );
    }
}

#[test]
fn alter_route_changes_the_construction_of_the_route_it_names() {
    for kind in IngestorEntityKind::all() {
        let created = Generated::of(kind);
        let constructing = altered(
            &created,
            "ALTER ROUTE TO events SET INHERIT ALL, ALTER ROUTE TO events ADD SET name = \
             'altered', ALTER ROUTE TO events ADD INVOKE write_header('X-Audit', 'altered'), \
             ALTER ROUTE TO events SET WHERE input.id > 0",
        );
        let construction = route_construction(&constructing);
        assert!(construction.inherit.is_some(), "{kind}");
        assert_eq!(construction.assignments.len(), 1, "{kind}");
        assert_eq!(construction.invocations.len(), 1, "{kind}");
        assert!(construction.where_clause.is_some(), "{kind}");

        let restored = altered(
            &created,
            "ALTER ROUTE TO events SET INHERIT ALL, ALTER ROUTE TO events ADD SET name = \
             'altered', ALTER ROUTE TO events ADD INVOKE write_header('X-Audit', 'altered'), \
             ALTER ROUTE TO events SET WHERE input.id > 0, ALTER ROUTE TO events DROP INHERIT, \
             ALTER ROUTE TO events DROP SET name, ALTER ROUTE TO events DROP INVOKE 1, ALTER \
             ROUTE TO events DROP WHERE",
        );
        assert_eq!(restored, created.parse(), "{kind}");

        let error = alteration_error(&created, "ALTER ROUTE TO elsewhere SET INHERIT ALL");
        assert!(
            matches!(error, AlterIngestorError::RouteTargetNotFound { .. }),
            "{kind}: altering a route the ingestor does not write: {error:?}"
        );
    }
}

#[test]
fn set_general_error_replaces_the_general_error_policy() {
    for kind in IngestorEntityKind::all() {
        let ingestor = altered(&Generated::of(kind), "SET GENERAL ERROR IGNORE");

        assert_eq!(
            ingestor.general_error_policy,
            GeneralErrorPolicy::Ignore,
            "{kind}"
        );
    }
}
