//! The conformance corpus: frames this crate writes, checked in under `conformance/` for
//! independent implementations to decode, and the report every implementation prints for them.
//!
//! Each frame is written from the representative samples of its message family. The checked-in
//! bytes must be exactly what the encoder writes, and decoding them must print exactly the
//! checked-in `corpus.report`. The Go and TypeScript conformance probes read the same files with
//! code generated from the schema and print the same report, which is how an independent reader
//! is held to the frames Rust writes. Setting `NERVIX_UPDATE_CLIENT_WIRE_CORPUS=1` rewrites the
//! corpus instead of checking it; `just update-client-wire-corpus` does that.

use std::{fmt::Write as _, fs, path::PathBuf};

use bytes::Bytes;
use meticulous::ResultExt as _;
use nervix_models::{ParseAsType, SchemaField};

use super::{
    fixtures::{limits, name, request},
    samples::{client_messages, command_outcome, leader, row_schema, rows_frame, subscription},
};
use crate::{
    CellView, CellsView, ClientFrame, ClientMessage, ClientRequest, CommandDisposition,
    LeaderRedirect, Reply, ReplyBody, ReplyDelivery, RequestRejected, RequestRejection,
    ServerEvent, ServerFrame, ServerMessage, SubscribeDisposition, SubscribeOutcome,
    SubscriptionEndReason, SubscriptionEnded, SubscriptionOpened, SubscriptionType,
    UnknownOutcomeCause, VerifiedFrame,
};

const UPDATE_ENV: &str = "NERVIX_UPDATE_CLIENT_WIRE_CORPUS";
const REPORT: &str = "corpus.report";

fn corpus_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance")
}

/// Every frame of the corpus, by file name, as the encoder writes it now. File names sort in the
/// order the report lists them.
fn corpus_frames() -> Vec<(&'static str, Bytes)> {
    let reply = |id: u64, body: ReplyBody| {
        let delivery = Reply {
            request_id: request(id),
            body,
        }
        .encode(&limits())
        .assured("every corpus reply fits the default limits");
        let ReplyDelivery::Frame(frame) = delivery else {
            panic!("a corpus reply fits one frame");
        };
        frame.into_bytes()
    };
    let command = |id: u64, disposition: CommandDisposition| {
        reply(
            id,
            ReplyBody::Command(Box::new(command_outcome(disposition))),
        )
    };
    let client = |index: usize| {
        client_messages()[index]
            .encode(&limits())
            .assured("every corpus request fits the default limits")
            .into_bytes()
    };
    let opened = SubscriptionOpened {
        subscription: subscription(),
        domain: name("tenant"),
        relay: name("orders"),
        subscription_type: SubscriptionType::Row,
        schema: row_schema(),
    };
    let ended = SubscriptionEnded {
        subscription: subscription(),
        reason: SubscriptionEndReason::RelayChanged,
        message: "relay 'orders' was redefined".to_string(),
    }
    .encode(&limits())
    .assured("the corpus event fits the default limits");
    vec![
        ("client_cancel.nxcm", client(13)),
        ("client_command.nxcm", client(0)),
        ("client_command_bare.nxcm", client(2)),
        ("client_commit.nxcm", client(1)),
        ("client_subscribe.nxcm", client(11)),
        (
            "server_command_failed.nxsm",
            command(1, CommandDisposition::Failed),
        ),
        (
            "server_command_leader_unknown.nxsm",
            command(
                3,
                CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
            ),
        ),
        (
            "server_command_redirect.nxsm",
            command(
                2,
                CommandDisposition::NotLeader(LeaderRedirect {
                    leader: Some(leader()),
                }),
            ),
        ),
        (
            "server_command_unknown.nxsm",
            command(
                4,
                CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::StillApplying),
            ),
        ),
        (
            "server_rejected.nxsm",
            reply(
                5,
                ReplyBody::Rejected(RequestRejected {
                    rejection: RequestRejection::InvalidRequest,
                    field: Some("SubscribeRequest.subscription_type".to_string()),
                    message: "`SubscribeRequest.subscription_type` is required".to_string(),
                }),
            ),
        ),
        ("server_rows.nxsm", rows_frame(&limits()).into_bytes()),
        ("server_subscription_ended.nxsm", ended.into_bytes()),
        (
            "server_subscription_opened.nxsm",
            reply(
                7,
                ReplyBody::Subscribe(SubscribeOutcome {
                    disposition: SubscribeDisposition::Opened(Box::new(opened)),
                    message: "created subscription 'live'".to_string(),
                    diagnostics: Vec::new(),
                }),
            ),
        ),
    ]
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(text, "{byte:02x}").assured("writing to a String cannot fail");
    }
    text
}

fn text(value: &str) -> String {
    format!("str:{}", hex(value.as_bytes()))
}

fn type_name(ty: &ParseAsType) -> String {
    match ty {
        ParseAsType::U8 => "U8".to_string(),
        ParseAsType::I8 => "I8".to_string(),
        ParseAsType::U16 => "U16".to_string(),
        ParseAsType::I16 => "I16".to_string(),
        ParseAsType::U32 => "U32".to_string(),
        ParseAsType::I32 => "I32".to_string(),
        ParseAsType::U64 => "U64".to_string(),
        ParseAsType::I64 => "I64".to_string(),
        ParseAsType::F32 => "F32".to_string(),
        ParseAsType::F64 => "F64".to_string(),
        ParseAsType::Bool => "BOOL".to_string(),
        ParseAsType::String => "STRING".to_string(),
        ParseAsType::Bytes => "BYTES".to_string(),
        ParseAsType::Datetime => "DATETIME".to_string(),
        ParseAsType::Array { element, len } => format!("FIXED_LIST<{},{len}>", type_name(element)),
        ParseAsType::Vec { element } => format!("LIST<{}>", type_name(element)),
    }
}

fn field_line(prefix: &str, field: &SchemaField) -> String {
    let nullable = if field.optional {
        "nullable"
    } else {
        "required"
    };
    let sensitive = if field.sensitive {
        "sensitive"
    } else {
        "public"
    };
    format!(
        "{prefix} {} {} {nullable} {sensitive}",
        field.name.as_str(),
        type_name(&field.ty)
    )
}

fn render_cell(cell: CellView<'_>) -> String {
    match cell {
        CellView::Null => "null".to_string(),
        CellView::Redacted => "redacted".to_string(),
        CellView::U8(value) => format!("u8:{value}"),
        CellView::I8(value) => format!("i8:{value}"),
        CellView::U16(value) => format!("u16:{value}"),
        CellView::I16(value) => format!("i16:{value}"),
        CellView::U32(value) => format!("u32:{value}"),
        CellView::I32(value) => format!("i32:{value}"),
        CellView::U64(value) => format!("u64:{value}"),
        CellView::I64(value) => format!("i64:{value}"),
        CellView::F32(value) => format!("f32:{:08x}", value.to_bits()),
        CellView::F64(value) => format!("f64:{:016x}", value.to_bits()),
        CellView::Bool(value) => format!("bool:{value}"),
        CellView::String(value) => text(value),
        CellView::Bytes(value) => format!("bytes:{}", hex(value)),
        CellView::Datetime(value) => format!("datetime:{}", value.unix_nanos()),
        CellView::List(elements) => {
            let elements: Vec<String> = elements.iter().map(render_cell).collect();
            format!("list[{}]", elements.join(","))
        }
    }
}

fn cells(values: CellsView<'_>, fields: &[SchemaField]) -> String {
    let rendered: Vec<String> = values
        .iter()
        .zip(fields)
        .map(|(value, field)| format!("{}={}", field.name.as_str(), render_cell(value)))
        .collect();
    rendered.join(" ")
}

fn disposition(disposition: &CommandDisposition) -> &'static str {
    match disposition {
        CommandDisposition::Completed { .. } => "completed",
        CommandDisposition::Failed => "failed",
        CommandDisposition::NotLeader(_) => "not_leader",
        CommandDisposition::TransactionDetached { .. } => "transaction_detached",
        CommandDisposition::TransactionTakenOver { .. } => "transaction_taken_over",
        CommandDisposition::OutcomeUnknown(_) => "outcome_unknown",
        CommandDisposition::ExecutionReferenceConflict(_) => "execution_reference_conflict",
        CommandDisposition::ExecutionReferenceExpired => "execution_reference_expired",
        CommandDisposition::PreviewStale { .. } => "preview_stale",
    }
}

fn render_server(message: &ServerMessage, lines: &mut Vec<String>) {
    match message {
        ServerMessage::Reply(reply) => {
            let id = reply.request_id.get();
            match &reply.body {
                ReplyBody::Command(outcome) => {
                    lines.push(format!(
                        "REPLY {id} COMMAND {} reference={} origin={:?} message={}",
                        disposition(&outcome.disposition),
                        outcome.execution_reference.as_str(),
                        outcome.origin,
                        text(&outcome.message)
                    ));
                    for diagnostic in &outcome.diagnostics {
                        let span = match diagnostic.span {
                            Some(span) => format!("{}..{}", span.start(), span.end()),
                            None => "none".to_string(),
                        };
                        lines.push(format!(
                            "DIAGNOSTIC span={span} message={}",
                            text(&diagnostic.message)
                        ));
                    }
                    match &outcome.disposition {
                        CommandDisposition::NotLeader(LeaderRedirect {
                            leader: Some(leader),
                        }) => {
                            let uri = |uri: &Option<url::Url>| match uri {
                                Some(uri) => uri.as_str().to_string(),
                                None => "none".to_string(),
                            };
                            lines.push(format!(
                                "LEADER node={} grpc={} console={}",
                                leader.node.as_str(),
                                uri(&leader.grpc_uri),
                                uri(&leader.web_console_uri)
                            ));
                        }
                        CommandDisposition::NotLeader(LeaderRedirect { leader: None }) => {
                            lines.push("LEADER none".to_string());
                        }
                        CommandDisposition::OutcomeUnknown(cause) => {
                            lines.push(format!("UNKNOWN cause={cause:?}"));
                        }
                        _ => {}
                    }
                }
                ReplyBody::Rejected(rejected) => {
                    let field = rejected.field.as_deref().unwrap_or("none");
                    lines.push(format!(
                        "REPLY {id} REJECTED {:?} field={field} message={}",
                        rejected.rejection,
                        text(&rejected.message)
                    ));
                }
                ReplyBody::Subscribe(SubscribeOutcome {
                    disposition: SubscribeDisposition::Opened(opened),
                    ..
                }) => {
                    lines.push(format!(
                        "REPLY {id} SUBSCRIBED name={} generation={} domain={} relay={} type={:?}",
                        opened.subscription.name.as_str(),
                        opened.subscription.generation,
                        opened.domain.as_str(),
                        opened.relay.as_str(),
                        opened.subscription_type
                    ));
                    for field in &opened.schema.fields {
                        lines.push(field_line("FIELD", field));
                    }
                    if let Some(branch) = &opened.schema.branch {
                        lines.push(format!("BRANCH {}", branch.branch().as_str()));
                        for field in branch.fields() {
                            lines.push(field_line("KEY", field));
                        }
                    }
                }
                other => panic!("the corpus holds no {other:?} reply"),
            }
        }
        ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) => {
            let handle = rows.subscription();
            lines.push(format!(
                "EVENT ROWS name={} generation={}",
                handle.name.as_str(),
                handle.generation
            ));
            let schema = row_schema();
            let batch = rows.batch();
            batch
                .conform(&schema)
                .assured("the corpus batch conforms to the corpus schema");
            let key = match (batch.branch_key(), &schema.branch) {
                (Some(key), Some(branch)) => cells(key, branch.fields()),
                _ => String::new(),
            };
            for row in batch.rows() {
                lines.push(format!("ROW [{key}] {}", cells(row, &schema.fields)));
            }
        }
        ServerMessage::Event(ServerEvent::SubscriptionEnded(ended)) => {
            lines.push(format!(
                "EVENT ENDED name={} generation={} reason={:?} message={}",
                ended.subscription.name.as_str(),
                ended.subscription.generation,
                ended.reason,
                text(&ended.message)
            ));
        }
        other => panic!("the corpus holds no {other:?} message"),
    }
}

fn render_client(message: &ClientMessage, lines: &mut Vec<String>) {
    let id = message.request_id.get();
    match &message.request {
        ClientRequest::Command(command) => {
            let domain = match &command.domain {
                Some(domain) => domain.as_str(),
                None => "none",
            };
            let position = match command.expected_transaction_position {
                Some(position) => position.accepted_operations().to_string(),
                None => "none".to_string(),
            };
            let preview = match &command.expected_preview {
                Some(preview) => format!(
                    "{}/{}/{}",
                    preview.transaction_id,
                    preview.position.accepted_operations(),
                    hex(preview.planning_basis.fingerprint())
                ),
                None => "none".to_string(),
            };
            lines.push(format!(
                "REQUEST {id} COMMAND query={} domain={domain} reference={} \
                 expected_position={position} expected_preview={preview}",
                text(&command.query),
                command.execution_reference.as_str()
            ));
        }
        ClientRequest::Subscribe(subscribe) => lines.push(format!(
            "REQUEST {id} SUBSCRIBE domain={} statement={} type={:?}",
            subscribe.domain.as_str(),
            text(&subscribe.statement),
            subscribe.subscription_type
        )),
        ClientRequest::Cancel(cancel) => {
            lines.push(format!(
                "REQUEST {id} CANCEL target={}",
                cancel.target.get()
            ));
        }
        other => panic!("the corpus holds no {other:?} request"),
    }
}

/// The report of the frames as they are checked in.
fn report_of(frames: &[(&'static str, Bytes)]) -> String {
    let mut lines = Vec::new();
    for (file, bytes) in frames {
        lines.push(format!("FRAME {file}"));
        if file.ends_with(".nxcm") {
            let frame = VerifiedFrame::<ClientFrame>::verify(bytes.clone(), &limits())
                .assured("a corpus client frame verifies");
            let message = ClientMessage::decode(&frame).assured("a corpus client frame decodes");
            render_client(&message, &mut lines);
        } else {
            let frame = VerifiedFrame::<ServerFrame>::verify(bytes.clone(), &limits())
                .assured("a corpus server frame verifies");
            let message = ServerMessage::decode(&frame).assured("a corpus server frame decodes");
            render_server(&message, &mut lines);
        }
    }
    let mut report = lines.join("\n");
    report.push('\n');
    report
}

#[test]
fn the_checked_in_corpus_is_what_the_encoder_writes_and_reads() {
    let directory = corpus_directory();
    let written = corpus_frames();
    if std::env::var_os(UPDATE_ENV).is_some() {
        fs::create_dir_all(&directory).assured("the corpus directory can be created");
        for (file, bytes) in &written {
            fs::write(directory.join(file), bytes).assured("a corpus frame can be written");
        }
        fs::write(directory.join(REPORT), report_of(&written))
            .assured("the corpus report can be written");
    }
    let mut checked_in = Vec::new();
    for (file, bytes) in &written {
        let stored = fs::read(directory.join(file)).unwrap_or_else(|error| {
            panic!("corpus frame {file} is missing ({error}); run `just update-client-wire-corpus`")
        });
        assert_eq!(
            stored.as_slice(),
            bytes.as_ref(),
            "corpus frame {file} is not what the encoder writes; run `just \
             update-client-wire-corpus` and review the report"
        );
        checked_in.push((*file, Bytes::from(stored)));
    }
    let mut files: Vec<String> = fs::read_dir(&directory)
        .assured("the corpus directory is readable")
        .map(|entry| {
            entry
                .assured("a corpus directory entry is readable")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|file| file != REPORT)
        .collect();
    files.sort();
    let expected: Vec<&str> = written.iter().map(|(file, _)| *file).collect();
    assert_eq!(
        files, expected,
        "the corpus holds exactly the frames this test writes"
    );
    let report = fs::read_to_string(directory.join(REPORT)).assured("the corpus report exists");
    assert_eq!(report_of(&checked_in), report);
}
