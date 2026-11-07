//! Conformance tests generated from every ingestor entity's declaration.
//!
//! Layer: test harness.
//!
//! - **Owns.** The statements each ingestor entity's declaration says the grammar accepts, refuses
//!   and completes, rendered from the declaration alone and checked through the composed grammar.
//! - **Depends on.** The connector vocabulary's declarations and the composed statement grammar.
//! - **Must not know.** Any one connector's grammar module, or a Model's fields beyond what a
//!   declaration names.
//!
//! A grammar module that accepts more or less than its entity declares fails here: a mode, a
//! quiesce mode or an option it accepts without a declaration, one it refuses although declared,
//! or completion that offers anything else at a declared position.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeMap;

use nervix_models::{
    ChoiceRequirement, CreateIngestor, DeclaredDelivery, IngestDeliveryKind, IngestQuiesceKind,
    IngestorEntityCapabilities, IngestorEntityKind, IngestorInput, IngestorReferenceKind,
    LocatorDeclaration, LocatorName, Model, OptionDeclaration, OptionForm, OptionValue,
};
use strum::IntoEnumIterator as _;

use crate::conformance_support::{assert_offers_exactly, created_model, refused};

/// The choice a generated statement makes for each option a requirement names, by the option's
/// keywords.
type Chosen = BTreeMap<&'static str, &'static str>;

/// One ingestor statement of an entity, rendered from what its entity declares.
struct Generated {
    kind: IngestorEntityKind,
    declared: IngestorEntityCapabilities,
    /// The choices the statement makes for options a requirement names.
    chosen: Chosen,
    /// One option written in full in place of the option's own rendering, by its keywords.
    written: Option<(&'static str, String)>,
    /// One required option the statement leaves out, by its keywords.
    omitted: Option<&'static str>,
    /// Text written after the declared options and before `MODE`.
    foreign: Option<&'static str>,
    /// The locator as the statement writes it, where the entity declares one.
    locator: Option<String>,
    delivery: &'static str,
    quiesce: &'static str,
}

impl Generated {
    /// A statement of `kind` in its first delivery and quiesce modes that require no choice, with
    /// every required option in its first form.
    fn of(kind: IngestorEntityKind) -> Self {
        let declared = kind.capabilities();
        let delivery = unconditional_delivery(&declared);
        let quiesce = unconditional_quiesce(&declared);
        Self {
            kind,
            declared,
            chosen: Chosen::new(),
            written: None,
            omitted: None,
            foreign: None,
            locator: declared.locator.as_ref().map(LocatorDeclaration::example),
            delivery: delivery.example,
            quiesce: quiesce.example(),
        }
    }

    /// This statement, making the first choice each of `requirements` accepts.
    fn choosing(mut self, requirements: &[ChoiceRequirement]) -> Self {
        for requirement in requirements {
            let Some(first) = requirement.choices.first() else {
                panic!("{} requires no choice of {}", self.kind, requirement.option);
            };
            self.chosen.insert(requirement.option, first);
        }
        self
    }

    /// The text of each option the statement writes, in the order its entity declares them.
    fn option_texts(&self) -> Vec<&str> {
        let mut texts = Vec::new();
        for option in self.declared.options {
            if self.omitted == Some(option.keywords) {
                continue;
            }
            if let Some((keywords, example)) = &self.written
                && *keywords == option.keywords
            {
                texts.push(example.as_str());
                continue;
            }
            if let Some(choice) = self.chosen.get(option.keywords) {
                let declared = option
                    .choice(choice)
                    .unwrap_or_else(|| panic!("{} declares no {choice}", option.keywords));
                texts.push(declared.example);
                continue;
            }
            if option.is_required() {
                texts.push(first_example(option));
            }
        }
        texts
    }

    /// Everything the statement writes before `MODE`: its head, its name, the body's reference,
    /// locator and options.
    fn through_options(&self) -> String {
        let mut words = vec![
            format!("CREATE {} INGESTOR generated FROM", self.kind.phrase()),
            reference(&self.declared).to_owned(),
        ];
        if let Some(locator) = &self.locator {
            words.push(locator.clone());
        }
        for text in self.option_texts() {
            words.push(text.to_owned());
        }
        if let Some(foreign) = self.foreign {
            words.push(foreign.to_owned());
        }
        words.join(" ")
    }

    /// The statement up to the delivery mode `MODE` introduces.
    fn through_mode(&self) -> String {
        format!("{} MODE ", self.through_options())
    }

    /// The statement up to the quiesce mode `ON QUIESCE` introduces.
    fn through_quiesce(&self) -> String {
        format!("{}{} ON QUIESCE ", self.through_mode(), self.delivery)
    }

    fn text(&self) -> String {
        let decode = if decodes(&self.declared) {
            " DECODE USING events_codec"
        } else {
            ""
        };
        format!(
            "{}{}{decode} TO events UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL \
             ERROR LOG;",
            self.through_quiesce(),
            self.quiesce
        )
    }

    /// The ingestor the statement declares, which the grammar must accept.
    fn parse(&self) -> CreateIngestor {
        let text = self.text();
        let Some(Model::Ingestor(ingestor)) = created_model(&text) else {
            panic!("the grammar refuses what {} declares: {text}", self.kind);
        };
        assert_eq!(ingestor.input.entity_kind(), self.kind, "{text}");
        ingestor
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

/// The reference a generated body names: a client, an endpoint, or the schema a producer's
/// batches carry.
fn reference(declared: &IngestorEntityCapabilities) -> &'static str {
    match declared.reference {
        IngestorReferenceKind::Client(_) => "source_client",
        IngestorReferenceKind::Endpoint(_) => "intake",
        IngestorReferenceKind::Schema => "SCHEMA batches",
    }
}

/// Whether the entity's payloads are decoded by a codec. A producer's batches already carry the
/// ingestor's input schema, so the body that names a schema names no codec.
fn decodes(declared: &IngestorEntityCapabilities) -> bool {
    match declared.reference {
        IngestorReferenceKind::Client(_) | IngestorReferenceKind::Endpoint(_) => true,
        IngestorReferenceKind::Schema => false,
    }
}

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

/// The first delivery mode the entity declares that needs no choice written beside it.
fn unconditional_delivery(declared: &IngestorEntityCapabilities) -> &'static DeclaredDelivery {
    let Some(delivery) = declared
        .delivery
        .iter()
        .find(|delivery| delivery.requires.is_empty())
    else {
        panic!("every entity declares a delivery mode that requires no choice");
    };
    delivery
}

/// The first quiesce mode the entity declares that needs no choice written beside it.
fn unconditional_quiesce(declared: &IngestorEntityCapabilities) -> IngestQuiesceKind {
    let Some(quiesce) = declared
        .quiesce
        .iter()
        .find(|quiesce| quiesce.requires.is_empty())
    else {
        panic!("every entity declares a quiesce mode that requires no choice");
    };
    quiesce.kind
}

/// The delivery mode an input's statement names.
fn delivery_kind(input: &IngestorInput) -> IngestDeliveryKind {
    match input {
        IngestorInput::Transport(transport) => transport.source.acknowledgement().kind(),
        IngestorInput::Client(source) => source.mode.kind(),
    }
}

/// Every delivery mode any entity declares, once per spelling, and the three shared spellings.
fn every_delivery_spelling() -> Vec<DeclaredDelivery> {
    let mut spellings = BTreeMap::new();
    for shared in [
        DeclaredDelivery::NO_ACK,
        DeclaredDelivery::ACK_SEQUENTIAL,
        DeclaredDelivery::ACK_PARALLEL,
    ] {
        spellings.insert(shared.example, shared);
    }
    for kind in IngestorEntityKind::all() {
        for delivery in kind.capabilities().delivery {
            spellings.insert(delivery.example, *delivery);
        }
    }
    spellings.into_values().collect()
}

/// Every option any entity declares, written in each of its forms, by its keywords.
fn every_option_example() -> BTreeMap<&'static str, Vec<&'static str>> {
    let mut examples: BTreeMap<&'static str, Vec<&'static str>> = BTreeMap::new();
    for kind in IngestorEntityKind::all() {
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
fn every_declared_delivery_and_quiesce_mode_parses_into_its_kind() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        for delivery in declared.delivery {
            for quiesce in declared.quiesce {
                let generated = Generated {
                    delivery: delivery.example,
                    quiesce: quiesce.kind.example(),
                    ..Generated::of(kind)
                }
                .choosing(delivery.requires)
                .choosing(quiesce.requires);
                if !delivery.kind.allows_quiesce(quiesce.kind) {
                    generated.assert_refused("a quiesce mode this delivery cannot honor");
                    continue;
                }
                let ingestor = generated.parse();
                let text = generated.text();
                assert_eq!(delivery_kind(&ingestor.input), delivery.kind, "{text}");
                assert_eq!(ingestor.input.quiesce().kind(), quiesce.kind, "{text}");
                assert!(
                    ingestor.input.supports_quiesce(ingestor.input.quiesce()),
                    "the vocabulary refuses the quiesce mode the grammar accepted: {text}"
                );
                let Some(source) = ingestor.input.transport_entity() else {
                    continue;
                };
                for requirement in delivery.requires.iter().chain(quiesce.requires) {
                    let Some(chosen) = source.chosen(requirement.option) else {
                        panic!("the source answers for {}: {text}", requirement.option);
                    };
                    assert!(requirement.accepts(chosen), "{text}");
                }
            }
        }
    }
}

#[test]
fn every_declared_locator_parses_into_its_kind() {
    for kind in IngestorEntityKind::all() {
        for locator in kind.locators() {
            let generated = Generated {
                locator: Some(locator.example()),
                ..Generated::of(kind)
            };
            generated.parse();
        }
    }
}

#[test]
fn every_declared_option_parses_in_each_of_its_forms() {
    for kind in IngestorEntityKind::all() {
        for option in kind.capabilities().options {
            for example in option.examples() {
                let generated = Generated {
                    written: Some((option.keywords, example.to_owned())),
                    ..Generated::of(kind)
                };
                generated.parse();
            }
        }
    }
}

#[test]
fn a_delivery_mode_the_entity_does_not_declare_is_refused() {
    let spellings = every_delivery_spelling();
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        for spelling in &spellings {
            if declared.declared_delivery(spelling.kind).is_some() {
                continue;
            }
            let generated = Generated {
                delivery: spelling.example,
                ..Generated::of(kind)
            }
            .choosing(spelling.requires);
            generated.assert_refused(&format!("MODE {}", spelling.kind.phrase()));
        }
    }
}

#[test]
fn a_quiesce_mode_the_entity_does_not_declare_is_refused() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        for quiesce in IngestQuiesceKind::iter() {
            if declared.declared_quiesce(quiesce).is_some() {
                continue;
            }
            let generated = Generated {
                quiesce: quiesce.example(),
                ..Generated::of(kind)
            };
            generated.assert_refused(&format!("ON QUIESCE {}", quiesce.example()));
        }
    }
}

#[test]
fn a_mode_whose_required_choices_are_not_written_is_refused() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        for delivery in declared.delivery {
            if delivery.requires.is_empty() {
                continue;
            }
            let generated = Generated {
                delivery: delivery.example,
                ..Generated::of(kind)
            };
            generated.assert_refused(&format!(
                "MODE {} without {:?}",
                delivery.kind.phrase(),
                delivery.requires
            ));
        }
        for quiesce in declared.quiesce {
            if quiesce.requires.is_empty() {
                continue;
            }
            let generated = Generated {
                quiesce: quiesce.kind.example(),
                ..Generated::of(kind)
            };
            generated.assert_refused(&format!(
                "ON QUIESCE {} without {:?}",
                quiesce.kind.example(),
                quiesce.requires
            ));
        }
    }
}

#[test]
fn an_option_only_another_entity_declares_is_refused() {
    let examples = every_option_example();
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        for (keywords, written) in &examples {
            if declared.option(keywords).is_some() {
                continue;
            }
            for example in written {
                let generated = Generated {
                    foreign: Some(example),
                    ..Generated::of(kind)
                };
                generated.assert_refused(example);
            }
        }
    }
}

#[test]
fn a_required_option_cannot_be_left_out() {
    for kind in IngestorEntityKind::all() {
        for option in kind.capabilities().options {
            if !option.is_required() {
                continue;
            }
            let generated = Generated {
                omitted: Some(option.keywords),
                ..Generated::of(kind)
            };
            generated.assert_refused(&format!("a statement without {}", option.keywords));
        }
    }
}

#[test]
fn an_empty_external_name_is_refused_where_a_name_is_declared() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        for locator in kind.locators() {
            let LocatorName::External(_) = locator.name else {
                continue;
            };
            let generated = Generated {
                locator: Some(format!("{} ''", locator.keywords)),
                ..Generated::of(kind)
            };
            generated.assert_refused(&format!("an empty {} name", locator.keywords));
        }
        for option in declared.options {
            for example in empty_names(option) {
                let generated = Generated {
                    written: Some((option.keywords, example)),
                    ..Generated::of(kind)
                };
                generated.assert_refused(&format!("an empty {} name", option.keywords));
            }
        }
    }
}

/// Each form of `option` that writes an external name, with the name left empty.
fn empty_names(option: &OptionDeclaration) -> Vec<String> {
    let mut emptied = Vec::new();
    match option.form {
        OptionForm::Value {
            value: OptionValue::Name(name),
            example,
        } => emptied.push(example.replace(&format!("'{}'", name.example), "''")),
        OptionForm::Value { .. } => {}
        OptionForm::Choice(choices) => {
            for choice in choices {
                if let Some(OptionValue::Name(name)) = choice.value {
                    emptied.push(choice.example.replace(&format!("'{}'", name.example), "''"));
                }
            }
        }
    }
    emptied
}

#[test]
fn completion_offers_exactly_the_declared_delivery_modes() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        let generated = Generated::of(kind);
        let mut spellings = Vec::new();
        for delivery in declared.delivery {
            spellings.push(delivery.example);
        }
        assert_offers_exactly(&generated.through_mode(), &spellings);

        let mut acknowledged = Vec::new();
        for spelling in &spellings {
            if let Some(rest) = spelling.strip_prefix("ACK ") {
                acknowledged.push(rest);
            }
        }
        if !acknowledged.is_empty() {
            assert_offers_exactly(&format!("{}ACK ", generated.through_mode()), &acknowledged);
        }
    }
}

#[test]
fn completion_offers_exactly_the_quiesce_modes_a_statement_can_name() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        let mut unconditional = Vec::new();
        let mut every = Vec::new();
        let mut requirements = Vec::new();
        for quiesce in declared.quiesce {
            every.push(quiesce.kind.example());
            if quiesce.requires.is_empty() {
                unconditional.push(quiesce.kind.example());
            }
            requirements.extend_from_slice(quiesce.requires);
        }
        assert_offers_exactly(&Generated::of(kind).through_quiesce(), &unconditional);
        if !requirements.is_empty() {
            let meeting = Generated::of(kind).choosing(&requirements);
            assert_offers_exactly(&meeting.through_quiesce(), &every);
        }
    }
}

#[test]
fn completion_names_the_declared_locator_then_the_declared_options() {
    for kind in IngestorEntityKind::all() {
        let declared = kind.capabilities();
        let head = format!(
            "CREATE {} INGESTOR generated FROM {} ",
            kind.phrase(),
            reference(&declared)
        );
        let mut after_locator = head.clone();
        let locators = kind.locators();
        if !locators.is_empty() {
            let written: Vec<String> = locators.iter().map(LocatorDeclaration::example).collect();
            let examples: Vec<&str> = written.iter().map(String::as_str).collect();
            assert_offers_exactly(&head, &examples);
        }
        for locator in &locators {
            let label = match locator.name {
                LocatorName::External(name) => name.label,
                LocatorName::Unquoted { label } => label,
            };
            assert_offers_exactly(&format!("{head}{} ", locator.keywords), &[label]);
        }
        if let Some(locator) = declared.locator {
            after_locator = format!("{head}{} ", locator.example());
        }

        let mut first = Vec::new();
        let mut required_seen = false;
        for option in declared.options {
            first.extend(option.examples());
            if option.is_required() {
                required_seen = true;
                break;
            }
        }
        if !required_seen {
            first.push("MODE NO ACK");
        }
        assert_offers_exactly(&after_locator, &first);

        let mut last = Vec::new();
        for option in declared.options.iter().rev() {
            if option.is_required() {
                break;
            }
            last.extend(option.examples());
        }
        last.push("MODE NO ACK");
        assert_offers_exactly(
            &format!("{} ", Generated::of(kind).through_options()),
            &last,
        );
    }
}
