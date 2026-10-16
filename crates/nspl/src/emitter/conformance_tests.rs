//! Conformance tests generated from every emitter entity's declaration.
//!
//! Layer: test harness.
//!
//! - **Owns.** The statements each emitter entity's declaration says the grammar accepts, refuses
//!   and completes: its publishing modes, how its body is formed, whether it batches and how
//!   large a batch may grow, its options and its envelope clauses.
//! - **Depends on.** The connector vocabulary's declarations and the composed statement grammar.
//! - **Must not know.** Any one connector's grammar beyond the words that open its destination.
//!
//! The emitter's head does not name its entity yet, so the words that open a destination after
//! `TO` differ by connector, and [`destination`] writes them. Everything after them comes from the
//! declaration: the locator, the options, the modes, the body and the batching clause.

use std::collections::BTreeMap;

use nervix_models::{
    ClickHouseSinkEntity, ClientSinkEntity, CreateEmitter, EmitSinkCapabilities, EmitSinkKind,
    EmitterBatchContractError, EmitterBatchLegality, EmitterBody, EmitterBodyForm,
    EmitterPublishingModeKind, EnvelopeClauseKind, HttpSinkEntity, IcebergSinkEntity,
    KafkaSinkEntity, Model, MongoDbSinkEntity, MqttSinkEntity, MySqlSinkEntity, NatsSinkEntity,
    OptionDeclaration, OptionForm, OptionValue, OtelSinkEntity, PostgresSinkEntity,
    PulsarSinkEntity, RabbitMqSinkEntity, RedisSinkEntity, SentrySinkEntity, SqsSinkEntity,
    SyslogSinkEntity, ZeroMqSinkEntity,
};
use strum::IntoEnumIterator as _;

use crate::conformance_support::{assert_offers_exactly, created_model, refused};

/// The batching clause a generated emitter writes where its entity batches.
const BATCH: &str = "BATCH MAX MESSAGES 10 MAX SIZE 64KiB";

/// The mapping a generated direct `VALUES` sink writes.
const VALUES: &str = "VALUES {'id' = id, 'name' = name}";

/// The locator of an entity's declaration written in full.
fn locator(declared: &EmitSinkCapabilities) -> String {
    let Some(locator) = declared.locator else {
        panic!("the entity declares the locator its destination names");
    };
    locator.example()
}

/// The required option opened by `keywords`, written in its first declared form.
fn required(declared: &EmitSinkCapabilities, keywords: &str) -> &'static str {
    let Some(option) = declared.option(keywords) else {
        panic!("the entity declares {keywords}");
    };
    assert!(option.is_required(), "{keywords} is required");
    first_example(option)
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

/// What a generated emitter of `kind` writes after `TO` and before `MODE`, every required option
/// included.
fn destination(kind: EmitSinkKind) -> String {
    let declared = kind.capabilities();
    let connector = kind.connector().keyword();
    match kind {
        EmitSinkKind::Client(ClientSinkEntity::Consumer) => "CLIENT SCHEMA outgoing".to_owned(),
        EmitSinkKind::Http(HttpSinkEntity::Request) => format!(
            "{connector} sink_client {} {}",
            required(&declared, "METHOD"),
            required(&declared, "PATH")
        ),
        EmitSinkKind::Kafka(KafkaSinkEntity::Topic)
        | EmitSinkKind::Pulsar(PulsarSinkEntity::Topic)
        | EmitSinkKind::RabbitMq(RabbitMqSinkEntity::Queue)
        | EmitSinkKind::Mqtt(MqttSinkEntity::Topic)
        | EmitSinkKind::Nats(NatsSinkEntity::Subject)
        | EmitSinkKind::Sqs(SqsSinkEntity::Queue) => {
            format!("{connector} sink_client {}", locator(&declared))
        }
        EmitSinkKind::Redis(RedisSinkEntity::PubSub) => {
            format!("{} sink_client {}", kind.phrase(), locator(&declared))
        }
        EmitSinkKind::ZeroMq(ZeroMqSinkEntity::Push)
        | EmitSinkKind::Sentry(SentrySinkEntity::Event)
        | EmitSinkKind::Syslog(SyslogSinkEntity::Collector) => {
            format!("{connector} sink_client")
        }
        EmitSinkKind::Otel(OtelSinkEntity::Logs | OtelSinkEntity::Traces) => {
            format!("{connector} sink_client {} {VALUES}", kind.entity_keyword())
        }
        EmitSinkKind::Otel(OtelSinkEntity::Metrics) => format!(
            "{connector} sink_client {} {VALUES}",
            required(&declared, "METRIC")
        ),
        EmitSinkKind::ClickHouse(ClickHouseSinkEntity::Table)
        | EmitSinkKind::Postgres(PostgresSinkEntity::Table)
        | EmitSinkKind::MySql(MySqlSinkEntity::Table)
        | EmitSinkKind::MongoDb(MongoDbSinkEntity::Collection) => {
            format!("{connector} sink_client {} {VALUES}", locator(&declared))
        }
        EmitSinkKind::Iceberg(IcebergSinkEntity::Table) => format!(
            "{connector} {} store {} {VALUES} {} {} {}",
            required(&declared, "ON"),
            locator(&declared),
            required(&declared, "LOCATION"),
            required(&declared, "CATALOG"),
            required(&declared, "COMMIT EACH")
        ),
    }
}

/// The bodies an emitter of `form` may write after its publishing mode.
fn bodies(form: EmitterBodyForm) -> Vec<&'static str> {
    match form {
        EmitterBodyForm::Codec => vec!["ENCODE USING events_codec"],
        EmitterBodyForm::CodecOrWithoutBody => vec!["ENCODE USING events_codec", "WITHOUT BODY"],
        EmitterBodyForm::Values | EmitterBodyForm::Native => vec![""],
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
    kind: EmitSinkKind,
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
    fn of(kind: EmitSinkKind) -> Self {
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
        let mut destination = destination(self.kind);
        if let Some(option) = self.option {
            destination = format!("{destination} {option}");
        }
        format!("CREATE EMITTER generated FROM events TO {destination} MODE ")
    }

    fn text(&self) -> String {
        let mut words = vec![format!("{}{}", self.through_mode(), self.mode)];
        if !self.body.is_empty() {
            words.push(self.body.to_owned());
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
        assert_eq!(emitter.sink.kind(), self.kind, "{text}");
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
        EmitterBody::Values | EmitterBody::Client => written.is_empty(),
    }
}

/// Every option any emitter entity declares, written in each of its forms, by its keywords.
fn every_option_example() -> BTreeMap<&'static str, Vec<&'static str>> {
    let mut examples: BTreeMap<&'static str, Vec<&'static str>> = BTreeMap::new();
    for kind in EmitSinkKind::all() {
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
    for kind in EmitSinkKind::all() {
        let declared = kind.capabilities();
        let destination = destination(kind);
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
    for kind in EmitSinkKind::all() {
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
                    assert_eq!(emitter.publishing_mode.kind(), *mode, "{text}");
                    assert!(wrote(&emitter.body, body), "{text}");
                    assert_eq!(emitter.batch.is_some(), batched, "{text}");
                    assert!(emitter.validate_batch().is_ok(), "{text}");
                }
            }
        }
    }
}

#[test]
fn every_declared_option_parses_in_each_of_its_forms() {
    for kind in EmitSinkKind::all() {
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
    for kind in EmitSinkKind::all() {
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
                EmitterBatchContractError::SizeAboveSinkMaximum { .. }
            ),
            "{error:?}"
        );
    }
}

#[test]
fn a_publishing_mode_the_entity_does_not_declare_is_refused() {
    for kind in EmitSinkKind::all() {
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
    for kind in EmitSinkKind::all() {
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
    for kind in EmitSinkKind::all() {
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
    for kind in EmitSinkKind::all() {
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
    for kind in EmitSinkKind::all() {
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
fn completion_offers_exactly_the_declared_publishing_modes() {
    for kind in EmitSinkKind::all() {
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
    for kind in EmitSinkKind::all() {
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
            if !option.is_required() {
                continuations.extend(option.examples());
            }
        }
        continuations.push("MODE");
        let prefix = format!(
            "CREATE EMITTER generated FROM events TO {} ",
            destination(kind)
        );
        assert_offers_exactly(&prefix, &continuations);
    }
}
