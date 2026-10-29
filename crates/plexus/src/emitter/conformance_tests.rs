//! Conformance tests generated from every emitter entity's declaration.
//!
//! Layer: test harness.
//!
//! - **Owns.** The statements each emitter entity's declaration says the grammar accepts, refuses
//!   and completes: its publishing modes, how its body is formed, whether it batches and how
//!   large a batch may grow, its options and its envelope clauses.
//! - **Depends on.** The connector vocabulary's declarations and the composed statement grammar.
//! - **Must not know.** Connector-specific grammar dispatch or runtime planning.
//!
//! Heads, references, locators, options, policies and payload forms all come from the declarations.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeMap;

use nervix_models::{
    CreateEmitter, EmitterBatchContractError, EmitterBatchLegality, EmitterBody, EmitterBodyForm,
    EmitterEntityKind, EmitterPublishingModeKind, EmitterReference, EmitterReferenceKind,
    EnvelopeClauseKind, Model, OptionDeclaration, OptionForm, OptionValue,
};
use strum::IntoEnumIterator as _;

use crate::{
    conformance_support::{assert_offers_exactly, created_model, refused},
    statement::suggest_statement,
};

/// The batching clause a generated emitter writes where its entity batches.
const BATCH: &str = "BATCH MAX MESSAGES 10 MAX SIZE 64KiB";

/// The mapping a generated direct `VALUES` emitter writes.
const VALUES: &str = "VALUES {'id' = id, 'name' = name}";

/// The option written in its first declared form.
fn first_example(option: &OptionDeclaration) -> &'static str {
    match option.form {
        OptionForm::Value { example, .. } => example,
        OptionForm::Choice(choices) => {
            let Some(first) = choices.first() else {
                panic!("{} declares no choice", option.keywords);
            };
            first.example
        }
    }
}

/// The declared reference, locator and required entity options, before MODE.
fn reference(kind: EmitterEntityKind) -> String {
    let declared = kind.capabilities();
    let mut words = vec![match declared.reference {
        EmitterReferenceKind::Schema => "SCHEMA outgoing".to_owned(),
        EmitterReferenceKind::Client(_) | EmitterReferenceKind::StorageClient(_) => {
            "destination_client".to_owned()
        }
    }];
    if let Some(locator) = declared.locator {
        words.push(locator.example());
    }
    for option in declared
        .options
        .iter()
        .filter(|option| option.is_required())
    {
        words.push(first_example(option).to_owned());
    }
    words.join(" ")
}

/// The bodies an emitter of `form` may write after its publishing mode.
fn bodies(form: EmitterBodyForm) -> Vec<&'static str> {
    match form {
        EmitterBodyForm::Codec => vec!["ENCODE USING events_codec"],
        EmitterBodyForm::CodecOrWithoutBody => vec!["ENCODE USING events_codec", "WITHOUT BODY"],
        EmitterBodyForm::Values => vec![VALUES],
        EmitterBodyForm::Native => vec![""],
    }
}

/// The bodies no emitter of `form` may write.
fn refused_bodies(form: EmitterBodyForm) -> Vec<&'static str> {
    match form {
        EmitterBodyForm::Codec => vec!["", "WITHOUT BODY"],
        EmitterBodyForm::CodecOrWithoutBody => vec![""],
        EmitterBodyForm::Values | EmitterBodyForm::Native => {
            vec!["ENCODE USING events_codec", "WITHOUT BODY"]
        }
    }
}

/// Whether an emitter whose batching is `legality` may write the batching clause, and whether it
/// may leave it out.
fn batching(legality: EmitterBatchLegality) -> Vec<bool> {
    match legality {
        EmitterBatchLegality::Required { .. } => vec![true],
        EmitterBatchLegality::Optional { .. } => vec![false, true],
        EmitterBatchLegality::Unavailable { .. } => vec![false],
    }
}

/// One emitter statement of an entity, rendered from what its entity declares.
struct Generated {
    kind: EmitterEntityKind,
    /// An option written after the destination's required options.
    option: Option<&'static str>,
    mode: &'static str,
    body: &'static str,
    /// An envelope clause or a property set written after the body.
    envelope: Option<&'static str>,
    batch: Option<String>,
}

impl Generated {
    /// An emitter of `kind` in its first declared publishing mode and its first body.
    fn of(kind: EmitterEntityKind) -> Self {
        let declared = kind.capabilities();
        let Some(mode) = declared.publishing_modes.first() else {
            panic!("{kind} declares no publishing mode");
        };
        let Some(body) = bodies(declared.body).first().copied() else {
            panic!("every body form writes some body");
        };
        let batch = match declared.batch {
            EmitterBatchLegality::Required { .. } => Some(BATCH.to_owned()),
            EmitterBatchLegality::Optional { .. } | EmitterBatchLegality::Unavailable { .. } => {
                None
            }
        };
        Self {
            kind,
            option: None,
            mode: mode.example(),
            body,
            envelope: None,
            batch,
        }
    }

    /// The statement up to the publishing mode `MODE` introduces.
    fn through_mode(&self) -> String {
        let mut destination = reference(self.kind);
        if let Some(option) = self
            .option
            .filter(|_| self.kind.capabilities().body != EmitterBodyForm::Values)
        {
            destination = format!("{destination} {option}");
        }
        format!(
            "CREATE {} EMITTER generated FROM events TO {destination} MODE ",
            self.kind.phrase()
        )
    }

    fn text(&self) -> String {
        let mut words = vec![format!("{}{}", self.through_mode(), self.mode)];
        if !self.body.is_empty() {
            words.push(self.body.to_owned());
        }
        if let Some(option) = self
            .option
            .filter(|_| self.kind.capabilities().body == EmitterBodyForm::Values)
        {
            words.push(option.to_owned());
        }
        if let Some(envelope) = self.envelope {
            words.push(envelope.to_owned());
        }
        if let Some(batch) = &self.batch {
            words.push(batch.clone());
        }
        words.push("FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;".to_owned());
        words.join(" ")
    }

    /// The emitter the statement declares, which the grammar must accept.
    fn parse(&self) -> CreateEmitter {
        let text = self.text();
        let Some(Model::Emitter(emitter)) = created_model(&text) else {
            panic!("the grammar refuses what {} declares: {text}", self.kind);
        };
        assert_eq!(emitter.connector.kind(), self.kind, "{text}");
        emitter
    }

    /// Checks that the grammar refuses the statement, which its entity does not declare.
    fn assert_refused(&self, what: &str) {
        let text = self.text();
        assert!(
            refused(&text),
            "the grammar accepts {what}, which {} does not declare: {text}",
            self.kind
        );
    }
}

/// Whether `body` is the body the statement wrote.
fn wrote(body: &EmitterBody, written: &str) -> bool {
    match body {
        EmitterBody::Codec { .. } => written.starts_with("ENCODE USING"),
        EmitterBody::WithoutBody => written == "WITHOUT BODY",
        EmitterBody::Values => written.starts_with("VALUES"),
        EmitterBody::Client => written.is_empty(),
    }
}

/// Every option any emitter entity declares, written in each of its forms, by its keywords.
fn every_option_example() -> BTreeMap<&'static str, Vec<&'static str>> {
    let mut examples: BTreeMap<&'static str, Vec<&'static str>> = BTreeMap::new();
    for kind in EmitterEntityKind::all() {
        for option in kind.capabilities().options {
            let written = examples.entry(option.keywords).or_default();
            for example in option.examples() {
                if !written.contains(&example) {
                    written.push(example);
                }
            }
        }
    }
    examples
}

#[test]
fn every_destination_writes_the_declared_locator_and_required_options() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        let destination = reference(kind);
        if let Some(locator) = declared.locator {
            assert!(
                destination.contains(&locator.example()),
                "{kind}: {destination} misses {}",
                locator.example()
            );
        }
        for option in declared.options {
            if option.is_required() {
                assert!(
                    destination.contains(first_example(option)),
                    "{kind}: {destination} misses {}",
                    option.keywords
                );
            }
        }
    }
}

#[test]
fn every_declared_mode_body_and_batching_parses_into_its_kind() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        for mode in declared.publishing_modes {
            for body in bodies(declared.body) {
                for batched in batching(declared.batch) {
                    let generated = Generated {
                        mode: mode.example(),
                        body,
                        batch: batched.then(|| BATCH.to_owned()),
                        ..Generated::of(kind)
                    };
                    let emitter = generated.parse();
                    let text = generated.text();
                    assert_eq!(emitter.connector.publishing().kind(), *mode, "{text}");
                    assert!(wrote(&emitter.body, body), "{text}");
                    assert_eq!(emitter.batch.is_some(), batched, "{text}");
                    assert!(emitter.validate_batch().is_ok(), "{text}");
                }
            }
        }
    }
}

#[test]
fn a_declared_storage_reference_is_preserved_for_planning() {
    for kind in EmitterEntityKind::all() {
        if let EmitterReferenceKind::StorageClient(_) = kind.capabilities().reference {
            let emitter = Generated::of(kind).parse();
            let EmitterReference::StorageClient(client) = emitter.connector.reference() else {
                panic!("{kind} must retain a storage client reference");
            };
            assert_eq!(client.as_str(), "destination_client");
        }
    }
}

#[test]
fn every_declared_option_parses_in_each_of_its_forms() {
    for kind in EmitterEntityKind::all() {
        for option in kind.capabilities().options {
            if option.is_required() {
                continue;
            }
            for example in option.examples() {
                let generated = Generated {
                    option: Some(example),
                    ..Generated::of(kind)
                };
                generated.parse();
            }
        }
    }
}

#[test]
fn a_batch_larger_than_the_declared_maximum_is_refused_by_the_batching_contract() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        let (EmitterBatchLegality::Required {
            maximum: Some(maximum),
        }
        | EmitterBatchLegality::Optional {
            maximum: Some(maximum),
        }) = declared.batch
        else {
            continue;
        };
        let at_maximum = Generated {
            batch: Some(format!("BATCH MAX MESSAGES 10 MAX SIZE {maximum}")),
            ..Generated::of(kind)
        };
        assert!(at_maximum.parse().validate_batch().is_ok());
        let above = Generated {
            batch: Some(format!(
                "BATCH MAX MESSAGES 10 MAX SIZE {}B",
                maximum.bytes().get() + 1
            )),
            ..Generated::of(kind)
        };
        let error = above
            .parse()
            .validate_batch()
            .expect_err("a batch above the destination's maximum breaks the contract");
        assert!(
            matches!(
                error.current_context(),
                EmitterBatchContractError::SizeAboveEmitterMaximum { .. }
            ),
            "{error:?}"
        );
    }
}

#[test]
fn a_publishing_mode_the_entity_does_not_declare_is_refused() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        let mut declared_spellings = Vec::new();
        for mode in declared.publishing_modes {
            declared_spellings.push(mode.example());
        }
        for mode in EmitterPublishingModeKind::iter() {
            // A kind spelled as a declared one, such as an application acknowledgement written
            // like a broker's, reads as the declared one.
            if declared_spellings.contains(&mode.example()) {
                continue;
            }
            let generated = Generated {
                mode: mode.example(),
                ..Generated::of(kind)
            };
            generated.assert_refused(&format!("MODE {}", mode.label()));
        }
    }
}

#[test]
fn a_body_the_entity_does_not_declare_is_refused() {
    for kind in EmitterEntityKind::all() {
        for body in refused_bodies(kind.capabilities().body) {
            let generated = Generated {
                body,
                ..Generated::of(kind)
            };
            generated.assert_refused(&format!("the body `{body}`"));
        }
    }
}

#[test]
fn a_batching_clause_the_entity_does_not_admit_is_refused() {
    for kind in EmitterEntityKind::all() {
        match kind.capabilities().batch {
            EmitterBatchLegality::Required { .. } => Generated {
                batch: None,
                ..Generated::of(kind)
            }
            .assert_refused("an emitter without BATCH"),
            EmitterBatchLegality::Unavailable { .. } => Generated {
                batch: Some(BATCH.to_owned()),
                ..Generated::of(kind)
            }
            .assert_refused("BATCH"),
            EmitterBatchLegality::Optional { .. } => {}
        }
    }
}

#[test]
fn an_option_only_another_entity_declares_is_refused() {
    let examples = every_option_example();
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        for (keywords, written) in &examples {
            if declared.option(keywords).is_some() {
                continue;
            }
            for example in written {
                let generated = Generated {
                    option: Some(example),
                    ..Generated::of(kind)
                };
                generated.assert_refused(example);
            }
        }
    }
}

#[test]
fn an_envelope_clause_or_property_the_entity_does_not_declare_is_refused() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        for clause in EnvelopeClauseKind::iter() {
            if declared.envelope.contains(&clause) {
                continue;
            }
            let generated = Generated {
                envelope: Some(clause.example()),
                ..Generated::of(kind)
            };
            generated.assert_refused(clause.keywords());
        }
        if declared.properties.is_empty() {
            let generated = Generated {
                envelope: Some("PROPERTIES {'content_type' = 'application/json'}"),
                ..Generated::of(kind)
            };
            generated.assert_refused("PROPERTIES");
        }
    }
}

#[test]
fn every_declared_envelope_clause_is_read_into_the_envelope() {
    for kind in EmitterEntityKind::all() {
        for clause in kind.capabilities().envelope {
            let emitter = Generated {
                envelope: Some(clause.example()),
                ..Generated::of(kind)
            }
            .parse();
            let mut written = Vec::new();
            for envelope_clause in emitter.envelope.clauses() {
                written.push(envelope_clause.kind());
            }
            assert_eq!(
                written,
                vec![*clause],
                "{kind} declares {}, which its statement writes",
                clause.keywords()
            );
        }
    }
}

#[test]
fn completion_after_the_body_offers_exactly_the_declared_envelope_clauses() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        let generated = Generated::of(kind);
        let mut words = vec![format!("{}{}", generated.through_mode(), generated.mode)];
        if !generated.body.is_empty() {
            words.push(generated.body.to_owned());
        }
        let prefix = format!("{} ", words.join(" "));
        let suggestions = suggest_statement(&prefix, prefix.len());
        for clause in EnvelopeClauseKind::iter() {
            let offered = suggestions
                .iter()
                .any(|suggestion| clause.keywords().starts_with(suggestion.as_str()));
            assert_eq!(
                offered,
                declared.envelope.contains(&clause),
                "completion of `{prefix}` offers {} only where {kind} declares it; it offered \
                 {suggestions:?}",
                clause.keywords()
            );
        }
    }
}

#[test]
fn completion_offers_exactly_the_declared_publishing_modes() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        let mut spellings = Vec::new();
        for mode in declared.publishing_modes {
            spellings.push(mode.example());
        }
        assert_offers_exactly(&Generated::of(kind).through_mode(), &spellings);
    }
}

#[test]
fn completion_after_the_destination_offers_exactly_the_optional_options_and_the_mode() {
    for kind in EmitterEntityKind::all() {
        let declared = kind.capabilities();
        // An expression can go on after its last word, so completion after a destination that
        // ends in one offers the expression's continuations too.
        let ends_in_expression = declared
            .options
            .iter()
            .rev()
            .find(|option| option.is_required());
        if let Some(OptionDeclaration {
            form:
                OptionForm::Value {
                    value: OptionValue::Expression,
                    ..
                },
            ..
        }) = ends_in_expression
        {
            continue;
        }
        let mut continuations = Vec::new();
        for option in declared.options {
            if !option.is_required() && declared.body != EmitterBodyForm::Values {
                continuations.extend(option.examples());
            }
        }
        continuations.push("MODE");
        let prefix = format!(
            "CREATE {} EMITTER generated FROM events TO {} ",
            kind.phrase(),
            reference(kind)
        );
        assert_offers_exactly(&prefix, &continuations);
    }
}
