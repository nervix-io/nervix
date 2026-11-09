//! `ALTER EMITTER` conformance tests generated from every emitter entity's declaration.
//!
//! Layer: test harness.
//!
//! - **Owns.** The operations each emitter entity's declaration says an `ALTER` accepts, refuses
//!   and completes: its locator, options, publishing modes, envelope clauses and properties, its
//!   client, its construction and its general error policy, each checked by applying the parsed
//!   alteration to an emitter a generated `CREATE` declares and by reading the alteration's
//!   canonical Plexus back as the same alteration.
//! - **Depends on.** The generated `CREATE` statements of the enclosing conformance tests, the
//!   connector vocabulary's declarations and the composed statement grammar.
//! - **Must not know.** Any one connector's grammar module.
//!
//! A `SET` of a part is checked against the emitter a `CREATE` writing that part declares, so an
//! alteration changes exactly the part it names, the way a statement that wrote it would have.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::OptionExt as _;
use nervix_models::{
    AlterEmitter, AlterEmitterError, AlterEmitterOperation, CreateEmitter, DeclaredPublishing,
    EmitterBodyForm, EmitterEntityKind, EmitterInstances, EmitterPublishingModeKind,
    EmitterReferenceKind,
    EnvelopeClauseKind, EnvelopeDeclarationError, GeneralErrorPolicy, Model, OptionDeclaration,
    OptionForm, OptionPresence, Statement,
};
use strum::IntoEnumIterator as _;

use super::{Generated, WrittenOption, first_admitted_example};
use crate::{
    conformance_support::{
        assert_offers_exactly, assert_reads_back_canonically, created_model, refused,
    },
    statement::parse_statement,
};

/// `ALTER <head> EMITTER generated <operations>;` for an emitter of `kind`.
fn alter_text(kind: EmitterEntityKind, operations: &str) -> String {
    format!("ALTER {} EMITTER generated {operations};", kind.phrase())
}

/// The alteration the composed grammar reads from `ALTER … <operations>`, which canonical Plexus
/// writes back as the same alteration.
fn parsed_alter(kind: EmitterEntityKind, operations: &str) -> AlterEmitter {
    let text = alter_text(kind, operations);
    let Ok(statement) = parse_statement(&text) else {
        panic!("the grammar refuses what {kind} declares: {text}");
    };
    assert_reads_back_canonically(&statement);
    let Statement::AlterEmitter(alter) = statement else {
        panic!("the grammar reads another statement than an emitter alteration from {text}");
    };
    alter
}

/// The emitter `created` declares, after the `ALTER` that writes `operations` applies to it.
fn altered(created: &Generated, operations: &str) -> CreateEmitter {
    let alter = parsed_alter(created.kind, operations);
    let mut emitter = created.parse();
    if let Err(error) = emitter.apply_alter(&alter) {
        panic!("{} cannot take `{operations}`: {error:?}", created.text());
    }
    emitter
}

/// Why the `ALTER` that writes `operations` does not apply to the emitter `created` declares.
fn alteration_error(created: &Generated, operations: &str) -> AlterEmitterError {
    let alter = parsed_alter(created.kind, operations);
    let mut emitter = created.parse();
    let Err(error) = emitter.apply_alter(&alter) else {
        panic!(
            "{} takes `{operations}`, which it must refuse",
            created.text()
        );
    };
    error.current_context().clone()
}

/// Why the envelope of the emitter `created` declares does not take the `ALTER` that writes
/// `operations`: the envelope's own refusal, beneath the alteration's.
fn envelope_alteration_error(created: &Generated, operations: &str) -> EnvelopeDeclarationError {
    let alter = parsed_alter(created.kind, operations);
    let mut emitter = created.parse();
    let Err(error) = emitter.apply_alter(&alter) else {
        panic!(
            "{} takes `{operations}`, which it must refuse",
            created.text()
        );
    };
    assert!(
        matches!(error.current_context(), AlterEmitterError::Envelope),
        "`{operations}` is refused for another reason than the envelope: {error:?}"
    );
    let Some(refusal) = error.downcast_ref::<EnvelopeDeclarationError>() else {
        panic!("`{operations}` is refused without the envelope's reason: {error:?}");
    };
    refusal.clone()
}

/// Checks that the grammar refuses `ALTER … <operations>`, which `kind` does not declare.
fn assert_alter_refused(kind: EmitterEntityKind, operations: &str) {
    let text = alter_text(kind, operations);
    assert!(
        refused(&text),
        "the grammar accepts `{operations}`, which {kind} does not declare: {text}"
    );
}

#[test]
fn set_writes_the_declared_locator_as_a_statement_writes_it() {
    for kind in EmitterEntityKind::all() {
        let Some(locator) = kind.capabilities().locator else {
            continue;
        };
        let created = Generated::of(kind);
        let operations = format!("SET {}", locator.example());

        assert_eq!(altered(&created, &operations), created.parse(), "{kind}");
    }
}

/// The first publishing mode of `kind` a statement may name beside `example`, the option `option`
/// written in full: one whose requirements leave the option free or admit the choice it writes.
///
/// An entity declares a handful of modes and requirements in its own constants, so the scans are
/// bounded by the declaration.
fn mode_beside(
    kind: EmitterEntityKind,
    option: &OptionDeclaration,
    example: &str,
) -> &'static DeclaredPublishing {
    let modes = kind.capabilities().publishing_modes;
    let Some(first) = modes.first() else {
        panic!("{kind} declares no publishing mode");
    };
    let OptionForm::Choice(choices) = option.form else {
        return first;
    };
    let Some(choice) = choices.iter().find(|choice| choice.example == example) else {
        panic!("{example} is not a choice {} declares", option.keywords);
    };
    for mode in modes {
        let requirement = mode
            .requires
            .iter()
            .find(|requirement| requirement.option == option.keywords);
        match requirement {
            None => return mode,
            Some(requirement) if requirement.accepts(choice.name) => return mode,
            Some(_) => {}
        }
    }
    panic!("{kind} declares no publishing mode beside {example}");
}

#[test]
fn set_writes_each_declared_option_as_a_statement_writes_it() {
    for kind in EmitterEntityKind::all() {
        for option in kind.capabilities().options {
            for example in option.examples() {
                let mode = mode_beside(kind, option, example);
                let created = Generated {
                    mode: mode.kind.example(),
                    requires: mode.requires,
                    ..Generated::of(kind)
                };
                let written = Generated {
                    option: Some(WrittenOption {
                        keywords: option.keywords,
                        example,
                    }),
                    mode: mode.kind.example(),
                    requires: mode.requires,
                    ..Generated::of(kind)
                };
                let operations = format!("SET {example}");

                assert_eq!(
                    altered(&created, &operations),
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
    for kind in EmitterEntityKind::all() {
        for option in kind.capabilities().options {
            if let OptionPresence::Required = option.presence {
                assert_alter_refused(kind, &format!("DROP {}", option.keywords));
                continue;
            }
            let operations = format!("DROP {}", option.keywords);
            for example in option.examples() {
                let written = Generated {
                    option: Some(WrittenOption {
                        keywords: option.keywords,
                        example,
                    }),
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
                matches!(error, AlterEmitterError::OptionNotWritten { .. }),
                "{kind}: `{operations}` on an emitter that writes none: {error:?}"
            );
        }
    }
}

/// A mode is named beside the option choices it requires, and an `ALTER` is validated once all its
/// operations apply, so one that moves to a mode writes those choices in the same statement.
#[test]
fn set_mode_writes_each_declared_publishing_mode() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        for mode in declared.publishing_modes {
            let mut operations = Vec::new();
            for requirement in mode.requires {
                // The declaration lists a handful of options, which bounds the scan.
                let Some(option) = declared
                    .options
                    .iter()
                    .find(|option| option.keywords == requirement.option)
                else {
                    panic!("{kind} requires an option it does not declare");
                };
                operations.push(format!(
                    "SET {}",
                    first_admitted_example(option, mode.requires)
                ));
            }
            operations.push(format!("SET MODE {}", mode.kind.example()));
            let operations = operations.join(", ");
            let written = Generated {
                mode: mode.kind.example(),
                requires: mode.requires,
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
fn set_mode_refuses_a_publishing_mode_the_entity_does_not_declare() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        let mut declared_spellings = Vec::new();
        for mode in declared.publishing_modes {
            declared_spellings.push(mode.kind.example());
        }
        for mode in EmitterPublishingModeKind::iter() {
            // A kind spelled as a declared one reads as the declared one.
            if declared_spellings.contains(&mode.example()) {
                continue;
            }
            assert_alter_refused(kind, &format!("SET MODE {}", mode.example()));
        }
    }
}

#[test]
fn completion_after_set_mode_offers_exactly_the_declared_publishing_modes() {
    for kind in EmitterEntityKind::all() {
        let mut spellings = Vec::new();
        for mode in kind.capabilities().publishing_modes {
            spellings.push(mode.kind.example());
        }
        let prefix = format!("ALTER {} EMITTER generated SET MODE ", kind.phrase());

        assert_offers_exactly(&prefix, &spellings);
    }
}

#[test]
fn set_and_drop_write_each_declared_envelope_clause_and_refuse_the_others() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        for clause in EnvelopeClauseKind::iter() {
            let set = format!("SET {}", clause.example());
            let drop = format!("DROP {}", clause.keywords());
            if !declared.envelope.contains(&clause) {
                assert_alter_refused(kind, &set);
                assert_alter_refused(kind, &drop);
                continue;
            }
            let written = Generated {
                envelope: Some(clause.example()),
                ..Generated::of(kind)
            };

            assert_eq!(
                altered(&Generated::of(kind), &set),
                written.parse(),
                "{kind}: `{set}`"
            );
            assert_eq!(
                altered(&written, &drop),
                Generated::of(kind).parse(),
                "{kind}: `{drop}`"
            );
            let error = envelope_alteration_error(&Generated::of(kind), &drop);
            assert!(
                matches!(error, EnvelopeDeclarationError::ClauseNotWritten { .. }),
                "{kind}: `{drop}` on an envelope that writes none: {error:?}"
            );
        }
    }
}

#[test]
fn set_and_drop_write_the_declared_properties_and_refuse_them_elsewhere() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        let Some(property) = declared.properties.first() else {
            assert_alter_refused(kind, "SET PROPERTIES {'content_type' = 'application/json'}");
            assert_alter_refused(kind, "DROP PROPERTIES");
            continue;
        };
        let set = format!("SET PROPERTIES {{'{}' = 'value'}}", property.name);
        let created = Generated::of(kind);

        let emitter = altered(&created, &set);
        let mut names = Vec::new();
        for written in &emitter.envelope.properties {
            names.push(written.name.as_str());
        }
        assert_eq!(names, vec![property.name], "{kind}: `{set}`");
        let restored = altered(&created, &format!("{set}, DROP PROPERTIES"));
        assert_eq!(
            restored,
            created.parse(),
            "{kind}: `{set}, DROP PROPERTIES`"
        );
        let error = envelope_alteration_error(&created, "DROP PROPERTIES");
        assert!(
            matches!(error, EnvelopeDeclarationError::PropertiesNotWritten),
            "{kind}: DROP PROPERTIES on an envelope that sets none: {error:?}"
        );
    }
}

#[test]
fn set_client_replaces_the_declared_client_reference_and_is_refused_without_one() {
    for kind in EmitterEntityKind::all() {
        if let EmitterReferenceKind::Schema = kind.capabilities().reference {
            assert_alter_refused(kind, "SET CLIENT other_client");
            continue;
        }
        let created = Generated::of(kind);
        let written = created
            .text()
            .replacen("destination_client", "other_client", 1);
        let Some(Model::Emitter(expected)) = created_model(&written) else {
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
fn construction_changes_where_the_body_form_lets_the_emitter_construct() {
    for kind in EmitterEntityKind::all() {
        let created = Generated::of(kind);
        let filtered = altered(&created, "SET WHERE input.id > 0");
        assert!(filtered.construction.where_clause.is_some(), "{kind}");
        let unfiltered = altered(&created, "SET WHERE input.id > 0, DROP WHERE");
        assert_eq!(unfiltered, created.parse(), "{kind}");
        let constructing = [
            "SET INHERIT ALL",
            "ADD SET name = 'altered'",
            "ADD INVOKE write_header('X-Audit', 'altered')",
        ];
        if kind.capabilities().body == EmitterBodyForm::Values {
            for operations in constructing {
                assert_alter_refused(kind, operations);
            }
            continue;
        }
        let emitter = altered(
            &created,
            "SET INHERIT ALL, ADD SET name = 'altered', ADD INVOKE write_header('X-Audit', \
             'altered')",
        );
        assert!(emitter.construction.inherit.is_some(), "{kind}");
        assert_eq!(emitter.construction.assignments.len(), 1, "{kind}");
        assert_eq!(emitter.construction.invocations.len(), 1, "{kind}");
        let restored = altered(
            &created,
            "SET INHERIT ALL, ADD SET name = 'altered', ADD INVOKE write_header('X-Audit', \
             'altered'), DROP INHERIT, DROP SET name, DROP INVOKE 1",
        );
        assert_eq!(restored, created.parse(), "{kind}");
    }
}

#[test]
fn set_general_error_replaces_the_general_error_policy() {
    for kind in EmitterEntityKind::all() {
        let emitter = altered(&Generated::of(kind), "SET GENERAL ERROR IGNORE");

        assert_eq!(
            emitter.error_policies.general,
            GeneralErrorPolicy::Ignore,
            "{kind}"
        );
    }
}

#[test]
fn set_instances_is_accepted_exactly_where_the_entity_declares_several_instances() {
    for kind in EmitterEntityKind::all() {
        let created = Generated::of(kind);
        let refusal = kind.capabilities().instancing.refusal(kind);
        let prefix = format!("ALTER {} EMITTER generated SET ", kind.phrase());
        let offered = crate::statement::suggest_statement(&prefix, prefix.len())
            .contains(&"INSTANCES".to_owned());
        match refusal {
            None => {
                let emitter = altered(&created, "SET INSTANCES 2");
                assert_eq!(
                    emitter.instances,
                    EmitterInstances::Count(std::num::NonZeroU64::new(2).assured("two is positive")),
                    "{kind}"
                );
                let emitter = altered(&created, "SET INSTANCES PER PARTITION");
                assert_eq!(emitter.instances, EmitterInstances::PerPartition, "{kind}");
                assert!(offered, "completion after SET offers INSTANCES for {kind}");
            }
            Some(refusal) => {
                let text = alter_text(kind, "SET INSTANCES 2");
                let Err(error) = parse_statement(&text) else {
                    panic!("the grammar accepts INSTANCES, which {kind} refuses: {text}");
                };
                assert!(
                    format!("{error:?}").contains(&refusal.to_string()),
                    "{kind} refuses INSTANCES without its declared reason: {error:?}"
                );
                assert!(!offered, "completion after SET offers INSTANCES for {kind}");
            }
        }
    }
}

#[test]
fn every_alteration_reads_the_operations_its_head_names() {
    for kind in EmitterEntityKind::all() {
        let alter = parsed_alter(
            kind,
            "SET GENERAL ERROR LOG, SET FLUSH EACH 1s MAX BATCH SIZE 1MiB",
        );

        assert_eq!(alter.kind, kind);
        assert!(matches!(
            alter.operations.as_slice(),
            [
                AlterEmitterOperation::SetGeneralError {
                    policy: GeneralErrorPolicy::Log
                },
                AlterEmitterOperation::SetFlush { .. }
            ]
        ));
    }
}
