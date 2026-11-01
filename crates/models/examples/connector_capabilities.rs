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
    BufferBound, BufferCapacity, BufferOverflow, ChoiceRequirement, ClientConfigFile,
    ClientConfigKey, ClientConfigNamespace, ClientConfigOwner, ClientConfigPresence,
    ClientConfigReaders, ClientConfigValue, ClientDeclaration, ClientPooling, ConnectorKind,
    DeclaredBounds, EmitterBatchLegality, EmitterBodyForm, EmitterEntityCapabilities,
    EmitterEntityKind, EmitterReferenceKind, HeaderModel, IngestorEntityCapabilities,
    IngestorEntityKind, IngestorReferenceKind, LocatorDeclaration, LocatorName, OmittedDeadline,
    OptionDeclaration, OptionForm, OptionPresence, OptionValue, OtelEmitterEntity, OtelValueField,
    SYSLOG_FRAME_LIMITS, SyslogEmitterEntity, WaitBound,
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
    let ingest: Vec<_> = IngestorEntityKind::all().map(ingestor).collect();
    let emit: Vec<_> = EmitterEntityKind::all().map(emitter).collect();
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
        OptionValue::LiteralMapping => "named string literals".to_owned(),
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

fn ingestor(kind: IngestorEntityKind) -> Value {
    let IngestorEntityCapabilities {
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
        IngestorReferenceKind::Client(connector) => format!("{connector} client"),
        IngestorReferenceKind::Endpoint(endpoint) => format!("{endpoint:?} endpoint"),
        IngestorReferenceKind::Schema => "SCHEMA <schema>".to_owned(),
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
        .map(|field| format!("metadata.{}: {} OPTIONAL", field.name(), field.ty()))
        .collect();
    let options: Vec<_> = declared_options.iter().map(option).collect();
    let from = match source_reference {
        IngestorReferenceKind::Schema => "SCHEMA <schema>",
        IngestorReferenceKind::Client(_) => "<client>",
        IngestorReferenceKind::Endpoint(_) => "<endpoint>",
    };
    let decode = match source_reference {
        IngestorReferenceKind::Schema => "",
        IngestorReferenceKind::Client(_) | IngestorReferenceKind::Endpoint(_) => {
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
        BufferOverflow::FailRead => {
            "when exceeded, fails the read and reconnects on the retry policy"
        }
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
                OmittedDeadline::Derived(rule) => format!("when omitted, {rule}"),
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

fn emitter(kind: EmitterEntityKind) -> Value {
    let EmitterEntityCapabilities {
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
        EmitterReferenceKind::Client(connector) => format!("{connector} client"),
        EmitterReferenceKind::Schema => "SCHEMA <schema>".to_owned(),
        EmitterReferenceKind::StorageClient(stores) => {
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
        "value_contract": otel_value_contract(kind), "frame_limits": frame_limits(kind),
    })
}

/// The frames an entity's transport carries, for an entity whose declaration states them.
fn frame_limits(kind: EmitterEntityKind) -> Option<Vec<String>> {
    let EmitterEntityKind::Syslog(SyslogEmitterEntity::Collector) = kind else {
        return None;
    };
    let limits = SYSLOG_FRAME_LIMITS;
    let trailer = char::from(limits.non_transparent_trailer()).escape_default();
    Some(vec![
        format!(
            "udp: one datagram carries at most {} payload bytes",
            limits.datagram_payload()
        ),
        format!(
            "tcp and tls with octet-counting framing: an octet count has at most {} digits, so a \
             frame carries at most {} payload bytes",
            limits.octet_count_digits(),
            limits.largest_octet_count()
        ),
        format!(
            "tcp with non-transparent framing: a payload cannot contain `{trailer}`, which ends \
             each frame"
        ),
    ])
}

/// Each OTEL shape projects its one public table, without a second catalog in the renderer.
fn otel_value_contract(kind: EmitterEntityKind) -> Option<Value> {
    use nervix_models::{
        OTEL_HISTOGRAM_VALUE_FIELDS, OTEL_LOG_VALUE_FIELDS, OTEL_NUMBER_VALUE_FIELDS,
        OTEL_TRACE_VALUE_FIELDS, OTEL_UNSUPPORTED_FEATURES,
    };
    let shapes: &[(&str, &[OtelValueField])] = match kind {
        EmitterEntityKind::Otel(OtelEmitterEntity::Logs) => &[("LOGS", OTEL_LOG_VALUE_FIELDS)],
        EmitterEntityKind::Otel(OtelEmitterEntity::Traces) => {
            &[("TRACES", OTEL_TRACE_VALUE_FIELDS)]
        }
        EmitterEntityKind::Otel(OtelEmitterEntity::Metrics) => &[
            ("GAUGE / SUM", OTEL_NUMBER_VALUE_FIELDS),
            ("HISTOGRAM", OTEL_HISTOGRAM_VALUE_FIELDS),
        ],
        _ => return None,
    };
    let mut fields = Vec::new();
    for (shape, declarations) in shapes {
        for field in *declarations {
            fields.push(format!(
                "{shape} `{}`: {}; {}; {}",
                field.key,
                field.rule.value_type(),
                field.presence,
                field.rule.constraint()
            ));
        }
    }
    let unsupported: Vec<_> = OTEL_UNSUPPORTED_FEATURES
        .iter()
        .map(|item| format!("{}: {}", item.feature, item.reason))
        .collect();
    Some(json!({"fields": fields, "unsupported": unsupported,
        "attributes": format!("{}; {}", nervix_models::MappedValueType::Attribute, nervix_models::OTEL_ATTRIBUTES_ENCODING),
        "resource": format!("{}; {}", nervix_models::MappedValueType::Literal, nervix_models::OTEL_RESOURCE_ENCODING),
    }))
}

/// Synopses use the current destination grammar; all clauses and choices come from declarations.
fn emitter_template(kind: EmitterEntityKind, body: &str) -> String {
    let declared = kind.capabilities();
    let mut destination = match declared.reference {
        EmitterReferenceKind::Schema => "SCHEMA <schema>".to_owned(),
        EmitterReferenceKind::StorageClient(_) => "<storage_client>".to_owned(),
        EmitterReferenceKind::Client(_) => "<client>".to_owned(),
    };
    if let Some(locator) = declared.locator {
        destination.push_str(&format!(" {}", locator.example()));
    }
    let mut payload_options = Vec::new();
    let mut entity_options = Vec::new();
    for option in declared.options {
        if declared.body == EmitterBodyForm::Values && !option.is_required() {
            payload_options.push(*option);
        } else {
            entity_options.push(*option);
        }
    }
    destination.push_str(&option_template(&entity_options));
    let mut body_clause = match declared.body {
        EmitterBodyForm::Codec | EmitterBodyForm::CodecOrWithoutBody | EmitterBodyForm::Values => {
            format!("\n    {body}")
        }
        EmitterBodyForm::Native => String::new(),
    };
    body_clause.push_str(&option_template(&payload_options));
    let batch = match declared.batch {
        EmitterBatchLegality::Required { .. } => "\n  BATCH MAX MESSAGES <count> MAX SIZE <bytes>",
        EmitterBatchLegality::Optional { .. } => {
            "\n  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]"
        }
        EmitterBatchLegality::Unavailable { .. } => "",
    };
    format!(
        "CREATE {} EMITTER <name>\n  FROM <relay>\n  TO {destination}\n    MODE \
         <mode>{body_clause}\n  <construction>{batch}\n  FLUSH IMMEDIATE\n  ON MESSAGE ERROR \
         LOG\n  ON GENERAL ERROR LOG;",
        kind.phrase()
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
    let config_owner = config.ownership_label();
    let keys: Vec<_> = config.keys().iter().map(client_key).collect();
    let namespaces: Vec<_> = config
        .namespaces()
        .iter()
        .map(|namespace| match namespace {
            ClientConfigNamespace::All => "all driver property names".to_owned(),
            ClientConfigNamespace::Prefix(prefix) => format!("{prefix}*"),
        })
        .collect();
    let options: Vec<_> = options.iter().map(option).collect();
    let aliases: Vec<_> = config
        .aliases()
        .iter()
        .map(|alias| json!({ "alias": alias.alias, "accepted": alias.accepted }))
        .collect();
    let stripped_prefix = match config.stripped_prefix() {
        Some(stripped) => json!({ "prefix": stripped.prefix, "properties": stripped.properties }),
        None => Value::Null,
    };
    let constraints: &[&str] = match kind {
        ConnectorKind::Otel => &[
            nervix_models::OTEL_HEADERS_FORMAT,
            nervix_models::OTEL_TLS_ENDPOINT_RULE,
        ],
        _ => &[],
    };
    Some(
        json!({ "kind": kind.keyword(), "pool": pool, "config_owner": config_owner, "options": options,
            "keys": keys, "namespaces": namespaces, "reserved": config.reserved(), "aliases": aliases,
            "stripped_prefix": stripped_prefix, "constraints": constraints }),
    )
}

fn client_key(key: &ClientConfigKey) -> Value {
    let ClientConfigKey {
        name,
        presence,
        value: declared_value,
        owner,
        confidential,
        readers,
    } = *key;
    let presence = match presence {
        ClientConfigPresence::Required => "required".to_owned(),
        ClientConfigPresence::Optional => "optional".to_owned(),
        ClientConfigPresence::Defaulted(value) => format!("optional; default `{value}`"),
        ClientConfigPresence::RequiredForScheme { address, scheme } => {
            format!("required when `{address}` uses `{scheme}`")
        }
        ClientConfigPresence::OptionalForScheme { address, scheme } => {
            format!("optional; requires `{address}` to use `{scheme}`")
        }
        ClientConfigPresence::ExactlyForScheme { address, scheme } => {
            format!("required when `{address}` uses `{scheme}`; refused otherwise")
        }
        ClientConfigPresence::OptionalForQuery {
            address,
            option,
            value,
        } => format!("optional; requires `{address}` to set `{option}={value}`"),
    };
    let owner = match owner {
        ClientConfigOwner::Nervix => "Nervix; strict",
        ClientConfigOwner::Driver => "driver; best effort",
    };
    let value = match declared_value {
        ClientConfigValue::Url(schemes) => format!("URL ({})", schemes.join(" or ")),
        ClientConfigValue::HostPortUrl(schemes) => format!(
            "URL naming only a host and a port ({})",
            schemes.join(" or ")
        ),
        ClientConfigValue::DriverUrl(url) => {
            let mut owned_options = Vec::with_capacity(url.owned.len());
            for option in url.owned {
                owned_options.push(format!(
                    "`{}` set once to {}",
                    option.name,
                    option.choices.join(" or ")
                ));
            }
            format!(
                "driver URL ({}); Nervix owns {}; the driver reads its other query options",
                url.schemes.join(" or "),
                owned_options.join(", ")
            )
        }
        ClientConfigValue::Choice(choices) => choices.join(" or "),
        value => value.label().to_owned(),
    };
    let readers = match readers {
        ClientConfigReaders::Every => "every",
        ClientConfigReaders::Ingestors => "ingestors",
        ClientConfigReaders::Emitters => "emitters",
    };
    let file = match declared_value {
        ClientConfigValue::File(ClientConfigFile::CertificateAuthority) => {
            "TLS trust; follows the connector's documented root policy"
        }
        ClientConfigValue::File(ClientConfigFile::Certificate) => {
            "TLS certificate; requires tls_key_file"
        }
        ClientConfigValue::File(ClientConfigFile::PrivateKey) => {
            "TLS private key; requires tls_cert_file"
        }
        ClientConfigValue::File(ClientConfigFile::MountedFile) => "driver file",
        _ => "none",
    };
    json!({ "name": name, "presence": presence, "type": value, "owner": owner,
        "confidential": confidential, "file": file, "readers": readers })
}
