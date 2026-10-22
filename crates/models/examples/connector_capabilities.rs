//! Export the current connector declarations for the Plexus manual generator.
//!
//! Layer: tooling.
//! - **Owns.** A one-way JSON projection of the vocabulary for documentation.
//! - **Depends on.** Public connector declarations and their spelling methods.
//! - **Must not know.** Drivers, registry validation or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "exports public declarations for documentation tooling"
    )
)]

use nervix_models::{
    BufferBound, BufferCapacity, BufferOverflow, ChoiceRequirement, ClientConfigOwner,
    ClientDeclaration, ClientPooling, ConnectorKind, DeclaredBounds, EmitSinkCapabilities,
    EmitSinkKind, EmitterBatchLegality, EmitterBodyForm, HeaderModel, IngestSourceCapabilities,
    IngestSourceKind, LocatorDeclaration, LocatorName, OmittedDeadline, OptionDeclaration,
    OptionForm, OptionPresence, OptionValue, SinkReferenceKind, SourceReferenceKind, WaitBound,
};
use serde_json::{Value, json};
use strum::IntoEnumIterator as _;

#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        library,
        reason = "the documentation command reports output failures through its process exit"
    )
)]
fn main() -> std::io::Result<()> {
    let ingest: Vec<_> = IngestSourceKind::all().map(ingestor).collect();
    let emit: Vec<_> = EmitSinkKind::all().map(emitter).collect();
    let mut clients = Vec::new();
    for kind in ConnectorKind::iter() {
        if let Some(declaration) = client(kind) {
            clients.push(declaration);
        }
    }
    let surface = json!({ "ingest": ingest, "emit": emit, "clients": clients });
    serde_json::to_writer_pretty(std::io::stdout().lock(), &surface).map_err(std::io::Error::other)
}

fn locator(declaration: Option<LocatorDeclaration>) -> Option<String> {
    let declaration = declaration?;
    let label = match declaration.name {
        LocatorName::External(name) => name.label,
        LocatorName::Unquoted { label } => label,
    };
    Some(format!("{} ({label})", declaration.example()))
}

fn option_value(value: OptionValue) -> String {
    match value {
        OptionValue::Count => "positive count".to_owned(),
        OptionValue::Duration => "duration".to_owned(),
        OptionValue::Text => "string literal".to_owned(),
        OptionValue::Name(name) => format!("string literal checked by {}", name.label),
        OptionValue::Expression => "expression".to_owned(),
        OptionValue::Mapping => "named expressions".to_owned(),
        OptionValue::Client(connector) => format!("{connector} client"),
        OptionValue::Clause => "composed clause".to_owned(),
        OptionValue::Size => "byte size".to_owned(),
    }
}

fn option(declaration: &OptionDeclaration) -> Value {
    let presence = match declaration.presence {
        OptionPresence::Required => "required".to_owned(),
        OptionPresence::Optional {
            default: Some(value),
        } => format!("optional; default {value}"),
        OptionPresence::Optional { default: None } => "optional; omitted when unset".to_owned(),
    };
    let forms: Vec<_> = match declaration.form {
        OptionForm::Value { value, example } => vec![json!({
            "example": example, "type": option_value(value),
        })],
        OptionForm::Choice(choices) => {
            let mut forms = Vec::new();
            for choice in choices {
                let ty = match choice.value {
                    Some(value) => option_value(value),
                    None => "keyword choice".to_owned(),
                };
                forms.push(json!({ "example": choice.example, "type": ty }));
            }
            forms
        }
    };
    json!({ "keywords": declaration.keywords, "presence": presence, "forms": forms })
}

fn requirements(requirements: &[ChoiceRequirement]) -> String {
    if requirements.is_empty() {
        return String::new();
    }
    let choices: Vec<_> = requirements
        .iter()
        .map(|requirement| format!("{} {}", requirement.option, requirement.choice))
        .collect();
    format!(" (requires {})", choices.join(" and "))
}

fn ingestor(kind: IngestSourceKind) -> Value {
    let IngestSourceCapabilities {
        headers,
        metadata,
        executes_on_every_cluster_node,
        reference: source_reference,
        position_class,
        acknowledgement_confirmation,
        delivery,
        quiesce,
        locator: declared_locator,
        options: declared_options,
        bounds: declared_bounds,
    } = kind.capabilities();
    let reference = match source_reference {
        SourceReferenceKind::Client(connector) => format!("{connector} client"),
        SourceReferenceKind::Endpoint(endpoint) => format!("{endpoint:?} endpoint"),
        SourceReferenceKind::Schema => "SCHEMA <schema>".to_owned(),
    };
    let modes: Vec<_> = delivery
        .iter()
        .map(|delivery| format!("{}{}", delivery.example, requirements(delivery.requires)))
        .collect();
    let quiesce: Vec<_> = quiesce
        .iter()
        .map(|mode| format!("{}{}", mode.kind.example(), requirements(mode.requires)))
        .collect();
    let metadata: Vec<_> = metadata
        .fields()
        .iter()
        .map(|field| format!("metadata.{}: {} OPTIONAL", field.name, field.ty))
        .collect();
    let options: Vec<_> = declared_options.iter().map(option).collect();
    let from = match source_reference {
        SourceReferenceKind::Schema => "SCHEMA <schema>",
        SourceReferenceKind::Client(_) => "<client>",
        SourceReferenceKind::Endpoint(_) => "<endpoint>",
    };
    let decode = match source_reference {
        SourceReferenceKind::Schema => "",
        SourceReferenceKind::Client(_) | SourceReferenceKind::Endpoint(_) => {
            "\n  DECODE USING <codec>"
        }
    };
    let location = match declared_locator {
        Some(declaration) => format!(" {}", declaration.example()),
        None => String::new(),
    };
    let template = format!(
        "CREATE {} INGESTOR <name>\n  FROM {from}{location}{}\n  MODE <mode>\n  ON QUIESCE \
         <quiesce>{decode}\n  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG\n  ON \
         GENERAL ERROR LOG;",
        kind.phrase(),
        option_template(declared_options),
    );
    json!({
        "entity": kind.phrase(), "connector": kind.connector().keyword(),
        "reference": reference, "locator": locator(declared_locator), "options": options,
        "modes": modes, "quiesce": quiesce, "metadata": metadata,
        "headers": header_model(headers), "every_node": executes_on_every_cluster_node,
        "position": position_class.as_ref(),
        "confirmation": acknowledgement_confirmation.as_ref(),
        "bounds": bounds(declared_bounds), "template": template,
    })
}

/// The header model of one side as the matrix states it, or `null` for a side whose messages carry
/// no transport headers.
fn header_model(model: Option<HeaderModel>) -> Value {
    let Some(HeaderModel { layout, names }) = model else {
        return Value::Null;
    };
    json!({
        "layout": layout.as_ref(), "names": names.as_ref(),
        "layout_meaning": layout.meaning(), "names_meaning": names.meaning(),
    })
}

/// What an entity's connector holds and how long it waits, each bound written as the matrix
/// states it.
fn bounds(declared: DeclaredBounds) -> Value {
    let DeclaredBounds {
        buffers,
        waits,
        driver_limits,
    } = declared;
    let buffers: Vec<_> = buffers
        .iter()
        .map(|buffer| json!({ "holds": buffer.holds, "bound": buffer_bound(buffer.bound) }))
        .collect();
    let waits: Vec<_> = waits
        .iter()
        .map(|wait| json!({ "wait": wait.wait, "bound": wait_bound(wait.bound) }))
        .collect();
    let deadline_keys: Vec<_> = declared
        .deadline_keys()
        .into_iter()
        .map(|key| key.key())
        .collect();
    json!({
        "buffers": buffers, "waits": waits, "driver_limits": driver_limits,
        "deadline_keys": deadline_keys,
    })
}

fn buffer_bound(bound: BufferBound) -> String {
    let (capacity, overflow) = match bound {
        BufferBound::Bounded { capacity, overflow } => (capacity, overflow),
        BufferBound::Unbounded => return "unbounded".to_owned(),
    };
    let capacity = match capacity {
        BufferCapacity::Items { count, unit } => format!("at most {count} {unit}"),
        BufferCapacity::Bytes(bytes) => format!("at most {bytes} bytes"),
        BufferCapacity::Setting(setting) => format!("set by {setting}"),
        BufferCapacity::Driver(default) => format!("driver default, {default}"),
    };
    let overflow = match overflow {
        BufferOverflow::Backpressure => "when full, holds back what fills it",
        BufferOverflow::DropAndCount => "when full, drops and counts what does not fit",
        BufferOverflow::DropUncounted => "when full, drops what does not fit without counting it",
        BufferOverflow::FailPublish => "when full, fails the publish for the host to retry",
        BufferOverflow::PeerDiscards => {
            "when full, its peer, told this capacity, discards what does not fit instead of \
             sending it"
        }
        BufferOverflow::RefuseRequest => {
            "when full, immediately refuses the request for its sender to retry"
        }
        BufferOverflow::FailPoll => "when exceeded, fails the poll; the next poll reads again",
    };
    format!("{capacity}; {overflow}")
}

fn wait_bound(bound: WaitBound) -> String {
    match bound {
        WaitBound::Key { key, omitted } => {
            let omitted = match omitted {
                OmittedDeadline::Required => "required".to_owned(),
                OmittedDeadline::Default(default) => {
                    format!("{} when omitted", humantime::format_duration(default))
                }
                OmittedDeadline::Unbounded => "unbounded when omitted".to_owned(),
            };
            format!("`{}`; {omitted}", key.key())
        }
        WaitBound::Clause(clause) => format!("`{clause}`"),
        WaitBound::Fixed(bound) => format!("fixed {}", humantime::format_duration(bound)),
        WaitBound::Driver(default) => format!("driver default, {default}"),
        WaitBound::Unbounded => "unbounded".to_owned(),
    }
}

fn option_template(options: &[OptionDeclaration]) -> String {
    let mut text = String::new();
    for declaration in options {
        let examples = declaration.examples().join(" | ");
        if declaration.is_required() {
            text.push_str(&format!("\n  ({examples})"));
        } else {
            text.push_str(&format!("\n  [{examples}]"));
        }
    }
    text
}

fn emitter(kind: EmitSinkKind) -> Value {
    let EmitSinkCapabilities {
        headers,
        reference,
        body,
        batch,
        requires_batch_transformation,
        publishing_modes,
        locator: declared_locator,
        options,
        envelope,
        properties,
        bounds: declared_bounds,
    } = kind.capabilities();
    let reference = match reference {
        SinkReferenceKind::Client(connector) => format!("{connector} client"),
        SinkReferenceKind::Schema => "SCHEMA <schema>".to_owned(),
        SinkReferenceKind::StorageClient(stores) => {
            let names: Vec<_> = stores.iter().map(ToString::to_string).collect();
            format!("{} store and catalog clients", names.join(" | "))
        }
    };
    let (batch, maximum) = match batch {
        EmitterBatchLegality::Required { maximum } => ("required".to_owned(), maximum),
        EmitterBatchLegality::Optional { maximum } => ("optional".to_owned(), maximum),
        EmitterBatchLegality::Unavailable { reason } => (format!("unavailable: {reason}"), None),
    };
    let maximum = maximum.map(|limit| limit.to_string());
    let modes: Vec<_> = publishing_modes.iter().map(|mode| mode.example()).collect();
    let options: Vec<_> = options.iter().map(option).collect();
    let envelope: Vec<_> = envelope.iter().map(|clause| clause.example()).collect();
    let properties: Vec<_> = properties
        .iter()
        .map(|property| format!("{}: {}", property.name, property.ty))
        .collect();
    let body = match body {
        EmitterBodyForm::Codec => "ENCODE USING <codec>",
        EmitterBodyForm::CodecOrWithoutBody => "ENCODE USING <codec> | WITHOUT BODY",
        EmitterBodyForm::Values => "VALUES { '<field>' = <expression>, ... }",
        EmitterBodyForm::Native => "native Arrow output; no codec",
    };
    json!({
        "entity": kind.phrase(), "connector": kind.connector().keyword(),
        "reference": reference, "locator": locator(declared_locator), "options": options,
        "modes": modes, "headers": header_model(headers),
        "batch": batch, "batch_maximum": maximum, "body": body,
        "batch_transformation": requires_batch_transformation,
        "envelope": envelope, "properties": properties,
        "bounds": bounds(declared_bounds), "template": emitter_template(kind, body),
    })
}

/// Synopses use the current destination grammar; all clauses and choices come from declarations.
fn emitter_template(kind: EmitSinkKind, body: &str) -> String {
    let declared = kind.capabilities();
    let connector = kind.connector();
    let mut destination = match declared.reference {
        SinkReferenceKind::Schema => format!("{connector} SCHEMA <schema>"),
        SinkReferenceKind::StorageClient(stores) => {
            let names: Vec<_> = stores.iter().map(ToString::to_string).collect();
            format!("{connector} ON ({}) <store>", names.join(" | "))
        }
        SinkReferenceKind::Client(_) => {
            if connector == ConnectorKind::Redis {
                format!("{} <client>", kind.phrase())
            } else {
                format!("{connector} <client>")
            }
        }
    };
    if let Some(locator) = declared.locator {
        destination.push_str(&format!(" {}", locator.example()));
    }
    if connector == ConnectorKind::Otel && kind.entity_keyword() != "METRICS" {
        destination.push_str(&format!(" {}", kind.entity_keyword()));
    }
    let mut options = Vec::new();
    for option in declared.options {
        if option.keywords == "METRIC" {
            destination.push_str(&option_template(&[*option]));
        } else {
            options.push(*option);
        }
    }
    if declared.body == EmitterBodyForm::Values {
        destination.push_str(&format!("\n    {body}"));
    }
    destination.push_str(&option_template(&options));
    let body_clause = match declared.body {
        EmitterBodyForm::Codec | EmitterBodyForm::CodecOrWithoutBody => format!("\n    {body}"),
        EmitterBodyForm::Values | EmitterBodyForm::Native => String::new(),
    };
    let batch = match declared.batch {
        EmitterBatchLegality::Required { .. } => "\n  BATCH MAX MESSAGES <count> MAX SIZE <bytes>",
        EmitterBatchLegality::Optional { .. } => {
            "\n  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]"
        }
        EmitterBatchLegality::Unavailable { .. } => "",
    };
    format!(
        "CREATE EMITTER <name>\n  FROM <relay>\n  TO {destination}\n    MODE \
         <mode>{body_clause}\n  <construction>{batch}\n  FLUSH IMMEDIATE\n  ON MESSAGE ERROR \
         LOG\n  ON GENERAL ERROR LOG;"
    )
}

fn client(kind: ConnectorKind) -> Option<Value> {
    let ClientDeclaration {
        pooling,
        config,
        options,
    } = kind.client()?;
    let pool = match pooling {
        ClientPooling::Pooled => "pooled; explicit POOL SIZE MIN <minimum> MAX <maximum>",
        ClientPooling::Unpooled => "unpooled",
    };
    let config_owner = match config {
        ClientConfigOwner::Nervix => "Nervix",
        ClientConfigOwner::Driver => "driver",
        ClientConfigOwner::NervixWithAddressQuery => "Nervix; address query delegated to driver",
        ClientConfigOwner::NervixWithPropertyNamespaces => {
            "Nervix; property namespaces delegated to driver"
        }
    };
    let options: Vec<_> = options.iter().map(option).collect();
    Some(
        json!({ "kind": kind.keyword(), "pool": pool, "config_owner": config_owner, "options": options }),
    )
}
