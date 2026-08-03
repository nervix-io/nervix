use std::{future::Future, time::Duration};

use futures_util::{SinkExt, StreamExt};
use nervix_models::CreateSignalingProtocol;
use prost_reflect::MessageDescriptor;
use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error as WebSocketError, Message},
};
use triomphe::Arc;

use crate::{
    jaq_program::{JaqNativeFormat, StatefulJaqProgram},
    runtime_schema::{decode_protobuf_payload, encode_protobuf_payload},
};

/// How much of a rejection value is carried into the failure reason.
const MAX_REJECTION_REASON_BYTES: usize = 512;

#[derive(Debug, Error)]
pub enum SignalingProtocolCompileError {
    #[error("signaling protocol '{protocol}' {clause} program #{index} is invalid: {reason}")]
    InvalidJaqProgram {
        protocol: String,
        clause: &'static str,
        index: usize,
        reason: String,
    },
    #[error("signaling protocol '{protocol}' has invalid timeout '{timeout}': {reason}")]
    InvalidTimeout {
        protocol: String,
        timeout: String,
        reason: String,
    },
    #[error("signaling protocol '{protocol}' requires compiled protobuf message descriptors")]
    MissingProtobufDescriptors { protocol: String },
}

#[derive(Debug, Error)]
pub(crate) enum WebsocketSignalingError {
    #[error("failed to send signaling frame: {0}")]
    Send(#[source] Box<WebSocketError>),
    #[error("failed to receive signaling frame: {0}")]
    Receive(#[source] Box<WebSocketError>),
    #[error("SEND JAQ program #{index} failed: {reason}")]
    SendProgram { index: usize, reason: String },
    #[error("SEND JAQ program #{index} output cannot be encoded as {format}: {reason}")]
    SendEncode {
        index: usize,
        format: &'static str,
        reason: String,
    },
    #[error("signaling rejected by FAIL JAQ matcher '{matcher}': {reason}")]
    Rejected { matcher: String, reason: String },
    #[error("CAPTURE program '{capture}' failed: {reason}")]
    Capture { capture: String, reason: String },
    #[error(
        "signaling timed out after {timeout:?}; unsatisfied WAIT JAQ matchers: [{}]",
        unsatisfied.join("; ")
    )]
    Timeout {
        timeout: Duration,
        unsatisfied: Vec<String>,
    },
    #[error("websocket closed before signaling completed")]
    Closed,
}

/// The protobuf message types a signaling protocol speaks in each direction.
pub struct SignalingProtobufDescriptors {
    pub(crate) send: MessageDescriptor,
    pub(crate) wait: MessageDescriptor,
}

#[derive(Debug)]
enum CompiledSignalingWire {
    Native(JaqNativeFormat),
    Protobuf {
        send: MessageDescriptor,
        wait: MessageDescriptor,
    },
}

/// Running position of each clause kind, for one-based diagnostics across all phases.
#[derive(Default)]
struct ClauseCounts {
    sends: usize,
    waits: usize,
}

impl ClauseCounts {
    fn next_send(&mut self) -> usize {
        self.sends += 1;
        self.sends
    }

    fn next_wait(&mut self) -> usize {
        self.waits += 1;
        self.waits
    }
}

/// One step of the handshake. Steps run strictly in order: a step completes before the next one
/// starts, so a request can depend on an earlier reply.
#[derive(Debug)]
enum CompiledSignalingStep {
    Send(Vec<Arc<StatefulJaqProgram>>),
    Wait(CompiledWaitStep),
}

#[derive(Debug)]
struct CompiledWaitStep {
    matchers: Vec<Arc<StatefulJaqProgram>>,
    capture: Option<Arc<StatefulJaqProgram>>,
    fails: Vec<Arc<StatefulJaqProgram>>,
    accept_data: bool,
}

/// A signaling protocol with its jaq programs compiled and its wire format resolved.
#[derive(Debug)]
pub struct CompiledSignalingProtocol {
    wire: CompiledSignalingWire,
    accept_data: bool,
    steps: Vec<CompiledSignalingStep>,
    fails: Vec<Arc<StatefulJaqProgram>>,
    timeout: Duration,
}

impl CompiledSignalingProtocol {
    pub fn compile(
        protocol: &CreateSignalingProtocol,
        protobuf: Option<SignalingProtobufDescriptors>,
    ) -> Result<Self, SignalingProtocolCompileError> {
        let name = protocol.name.as_str();
        let wire = match JaqNativeFormat::try_from(&protocol.format) {
            Ok(format) => CompiledSignalingWire::Native(format),
            Err(()) => {
                let SignalingProtobufDescriptors { send, wait } = protobuf.ok_or_else(|| {
                    SignalingProtocolCompileError::MissingProtobufDescriptors {
                        protocol: name.to_string(),
                    }
                })?;
                CompiledSignalingWire::Protobuf { send, wait }
            }
        };
        // Programs are numbered across the whole protocol so a diagnostic points at the clause the
        // operator wrote, not at an offset within some phase.
        let mut counts = ClauseCounts::default();
        let compile = |clause: &'static str, index: usize, program: &str| {
            StatefulJaqProgram::compile(program)
                .map(Arc::new)
                .map_err(|error| SignalingProtocolCompileError::InvalidJaqProgram {
                    protocol: name.to_string(),
                    clause,
                    index,
                    reason: error.to_string(),
                })
        };

        let mut steps = Vec::with_capacity(protocol.on_connect.steps.len());
        for step in &protocol.on_connect.steps {
            steps.push(match step {
                nervix_models::SignalingStep::Send(programs) => CompiledSignalingStep::Send(
                    programs
                        .iter()
                        .map(|program| compile("SEND JAQ", counts.next_send(), program))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                nervix_models::SignalingStep::Wait(wait) => {
                    let index = counts.next_wait();
                    CompiledSignalingStep::Wait(CompiledWaitStep {
                        matchers: wait
                            .matchers
                            .iter()
                            .map(|matcher| compile("WAIT JAQ", index, matcher))
                            .collect::<Result<Vec<_>, _>>()?,
                        capture: wait
                            .capture
                            .as_deref()
                            .map(|capture| compile("CAPTURE", index, capture))
                            .transpose()?,
                        fails: wait
                            .fail_matchers
                            .iter()
                            .map(|matcher| compile("FAIL JAQ", index, matcher))
                            .collect::<Result<Vec<_>, _>>()?,
                        accept_data: wait.accept_data,
                    })
                }
            });
        }

        let fails = protocol
            .on_connect
            .fail_matchers
            .iter()
            .enumerate()
            .map(|(index, matcher)| compile("FAIL JAQ", index + 1, matcher))
            .collect::<Result<Vec<_>, _>>()?;

        let timeout = humantime::parse_duration(&protocol.on_connect.timeout).map_err(|error| {
            SignalingProtocolCompileError::InvalidTimeout {
                protocol: name.to_string(),
                timeout: protocol.on_connect.timeout.clone(),
                reason: error.to_string(),
            }
        })?;

        Ok(Self {
            wire,
            accept_data: protocol.on_connect.accept_data,
            steps,
            fails,
            timeout,
        })
    }

    fn format_name(&self) -> &'static str {
        match &self.wire {
            CompiledSignalingWire::Native(format) => format.name(),
            CompiledSignalingWire::Protobuf { .. } => "PROTOBUF",
        }
    }

    /// Serialize one SEND program output into the frame that carries it.
    fn encode_frame(&self, value: JsonValue) -> Result<Message, String> {
        match &self.wire {
            CompiledSignalingWire::Native(format) => {
                let encoded = format
                    .write_value(value)
                    .map_err(|error| error.to_string())?;
                if format.is_binary() {
                    return Ok(Message::Binary(encoded));
                }
                String::from_utf8(encoded)
                    .map(Message::Text)
                    .map_err(|error| error.to_string())
            }
            CompiledSignalingWire::Protobuf { send, .. } => {
                encode_protobuf_payload(send, &value).map(Message::Binary)
            }
        }
    }

    /// Decode an incoming frame into the value matchers inspect.
    ///
    /// Frames this protocol cannot read are not errors: they are data that arrived before the
    /// handshake finished.
    fn decode_frame(&self, payload: &[u8], is_text: bool) -> Option<JsonValue> {
        match &self.wire {
            // Binary formats never travel as text frames, but a text format may arrive in either
            // frame kind: peers commonly send textual payloads as binary frames.
            CompiledSignalingWire::Native(format) => {
                if format.is_binary() && is_text {
                    return None;
                }
                format.read_single_value(payload).ok()
            }
            CompiledSignalingWire::Protobuf { wait, .. } => {
                if is_text {
                    return None;
                }
                decode_protobuf_payload(wait, payload).ok()
            }
        }
    }
}

/// Handshake state carried across steps.
struct SessionState {
    state: JsonValue,
    /// Whether payload frames stream to the relay yet.
    accepting: bool,
}

impl SessionState {
    /// Stream one payload frame, or drop it if the relay is not open yet.
    async fn stream<D: SignalingDataSink>(&self, frame: Vec<u8>, sink: &D) {
        if self.accepting {
            sink.accept(frame).await;
        }
    }
}

/// Receives payload frames as the handshake streams them to the relay.
///
/// A trait rather than a closure: the returned future borrows the sink under one concrete
/// lifetime, which keeps it `Send` inside the spawned connection tasks that drive signaling.
pub(crate) trait SignalingDataSink {
    fn accept(&self, payload: Vec<u8>) -> impl Future<Output = ()> + Send;
}

pub(crate) struct WebsocketSignalingSession {
    protocol: Arc<CompiledSignalingProtocol>,
}

impl WebsocketSignalingSession {
    pub(crate) fn new(protocol: Arc<CompiledSignalingProtocol>) -> Self {
        Self { protocol }
    }

    /// Run the handshake, streaming payload frames to `sink` once `ACCEPT DATA` opens the relay.
    ///
    /// Steps run strictly in order. Frames that arrive before the relay is open are not payload
    /// yet and are dropped, so nothing accumulates in memory and nothing reaches the relay from a
    /// connection that was never established.
    pub(crate) async fn run<S, D>(
        &self,
        websocket: &mut WebSocketStream<S>,
        sink: &D,
    ) -> Result<(), WebsocketSignalingError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        D: SignalingDataSink,
    {
        // Outlives the timed future so a timeout can name what the current step was waiting on.
        let mut pending: Vec<&Arc<StatefulJaqProgram>> = Vec::new();
        let mut session = SessionState {
            state: JsonValue::Object(serde_json::Map::new()),
            accepting: self.protocol.accept_data,
        };
        let outcome = time::timeout(
            self.protocol.timeout,
            self.run_steps(websocket, &mut pending, &mut session, sink),
        )
        .await;

        match outcome {
            Ok(result) => result,
            Err(_) => Err(WebsocketSignalingError::Timeout {
                timeout: self.protocol.timeout,
                unsatisfied: pending
                    .iter()
                    .map(|matcher| matcher.source().to_string())
                    .collect(),
            }),
        }
    }

    async fn run_steps<'a, S, D>(
        &'a self,
        websocket: &mut WebSocketStream<S>,
        pending: &mut Vec<&'a Arc<StatefulJaqProgram>>,
        session: &mut SessionState,
        sink: &D,
    ) -> Result<(), WebsocketSignalingError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        D: SignalingDataSink,
    {
        let mut sent = 0usize;

        for step in &self.protocol.steps {
            match step {
                CompiledSignalingStep::Send(programs) => {
                    for program in programs {
                        sent += 1;
                        let value = program
                            .run_single(JsonValue::Null, &session.state)
                            .map_err(|error| WebsocketSignalingError::SendProgram {
                                index: sent,
                                reason: error.to_string(),
                            })?;
                        let frame = self.protocol.encode_frame(value).map_err(|reason| {
                            WebsocketSignalingError::SendEncode {
                                index: sent,
                                format: self.protocol.format_name(),
                                reason,
                            }
                        })?;
                        websocket
                            .send(frame)
                            .await
                            .map_err(|error| WebsocketSignalingError::Send(Box::new(error)))?;
                    }
                }
                CompiledSignalingStep::Wait(wait) => {
                    pending.clear();
                    pending.extend(wait.matchers.iter());
                    self.wait_for_step(websocket, wait, pending, session, sink)
                        .await?;
                    // Completing a marked step means the peer is streaming: open the relay so
                    // later frames flow while the remaining steps continue.
                    if wait.accept_data {
                        session.accepting = true;
                    }
                }
            }
        }

        Ok(())
    }

    async fn wait_for_step<'a, S, D>(
        &'a self,
        websocket: &mut WebSocketStream<S>,
        step: &'a CompiledWaitStep,
        pending: &mut Vec<&'a Arc<StatefulJaqProgram>>,
        session: &mut SessionState,
        sink: &D,
    ) -> Result<(), WebsocketSignalingError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        D: SignalingDataSink,
    {
        while !pending.is_empty() {
            tokio::task::consume_budget().await;
            let Some(message) = websocket.next().await else {
                return Err(WebsocketSignalingError::Closed);
            };
            let message =
                message.map_err(|error| WebsocketSignalingError::Receive(Box::new(error)))?;
            let (frame, is_text) = match message {
                Message::Text(text) => (text.into_bytes(), true),
                Message::Binary(bytes) => (bytes, false),
                Message::Ping(ping) => {
                    websocket
                        .send(Message::Pong(ping))
                        .await
                        .map_err(|error| WebsocketSignalingError::Send(Box::new(error)))?;
                    continue;
                }
                Message::Close(_) => return Err(WebsocketSignalingError::Closed),
                Message::Pong(_) | Message::Frame(_) => continue,
            };

            let Some(value) = self.protocol.decode_frame(&frame, is_text) else {
                session.stream(frame, sink).await;
                continue;
            };

            // A rejection must be recognized before a lenient matcher can consume it. The step's
            // own guards run first so a diagnostic names the closest rule.
            if let Some(rejection) = self.rejection(&step.fails, &value, &session.state) {
                return Err(rejection);
            }
            if let Some(rejection) = self.rejection(&self.protocol.fails, &value, &session.state) {
                return Err(rejection);
            }

            let Some(index) = pending
                .iter()
                .position(|matcher| matcher_is_satisfied(matcher, &value, &session.state))
            else {
                session.stream(frame, sink).await;
                continue;
            };

            pending.remove(index);
            if pending.is_empty()
                && let Some(capture) = step.capture.as_ref()
            {
                merge_capture(capture, &value, &mut session.state)?;
            }
        }

        Ok(())
    }

    fn rejection(
        &self,
        matchers: &[Arc<StatefulJaqProgram>],
        value: &JsonValue,
        state: &JsonValue,
    ) -> Option<WebsocketSignalingError> {
        matchers.iter().find_map(|matcher| {
            let output = matcher.run_first(value.clone(), state).ok().flatten()?;
            is_truthy(&output).then(|| WebsocketSignalingError::Rejected {
                matcher: matcher.source().to_string(),
                reason: rejection_reason(&output),
            })
        })
    }
}

/// Merge a capture program's output into the handshake state.
///
/// The matcher already accepted the frame, so a capture that cannot run or does not describe an
/// object is operator error rather than a mismatched frame, and fails the handshake.
fn merge_capture(
    capture: &StatefulJaqProgram,
    value: &JsonValue,
    state: &mut JsonValue,
) -> Result<(), WebsocketSignalingError> {
    let captured = capture.run_single(value.clone(), state).map_err(|error| {
        WebsocketSignalingError::Capture {
            capture: capture.source().to_string(),
            reason: error.to_string(),
        }
    })?;
    let JsonValue::Object(captured) = captured else {
        return Err(WebsocketSignalingError::Capture {
            capture: capture.source().to_string(),
            reason: "CAPTURE program must produce an object".to_string(),
        });
    };
    let JsonValue::Object(state) = state else {
        unreachable!("handshake state is always an object");
    };
    state.extend(captured);
    Ok(())
}

/// A matcher matches when it yields any value that is neither `null` nor `false`.
///
/// Matchers see frames they were not written for, so an evaluation error is a non-match rather
/// than a connection failure.
fn matcher_is_satisfied(
    matcher: &StatefulJaqProgram,
    value: &JsonValue,
    state: &JsonValue,
) -> bool {
    matcher
        .run_first(value.clone(), state)
        .ok()
        .flatten()
        .is_some_and(|output| is_truthy(&output))
}

fn is_truthy(value: &JsonValue) -> bool {
    !matches!(value, JsonValue::Null | JsonValue::Bool(false))
}

fn rejection_reason(value: &JsonValue) -> String {
    let rendered = match value {
        JsonValue::String(reason) => reason.clone(),
        other => other.to_string(),
    };
    truncate_on_char_boundary(rendered, MAX_REJECTION_REASON_BYTES)
}

fn truncate_on_char_boundary(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value.push('…');
    value
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use nervix_models::{
        Identifier, SignalingProtobufConfig, SignalingProtocolOnConnect, SignalingStep,
        SignalingWaitStep, SignalingWireFormat,
    };
    use parking_lot::Mutex;
    use serde_json::json;
    use tokio_tungstenite::tungstenite::protocol::Role;

    use super::*;

    fn protocol(
        format: SignalingWireFormat,
        on_connect: SignalingProtocolOnConnect,
    ) -> CreateSignalingProtocol {
        CreateSignalingProtocol {
            name: Identifier::parse("handshake").expect("valid identifier"),
            format,
            on_connect,
        }
    }

    fn on_connect(
        send_programs: &[&str],
        wait_matchers: &[&str],
        fail_matchers: &[&str],
    ) -> SignalingProtocolOnConnect {
        SignalingProtocolOnConnect {
            accept_data: false,
            steps: steps(send_programs, wait_matchers),
            fail_matchers: fail_matchers.iter().map(|p| p.to_string()).collect(),
            timeout: "5s".to_string(),
        }
    }

    /// Records what reached ingestion, and when relative to the handshake finishing.
    #[derive(Default)]
    struct RecordingSink {
        accepted: StdArc<Mutex<Vec<Vec<u8>>>>,
    }

    impl RecordingSink {
        fn accepted(&self) -> Vec<Vec<u8>> {
            self.accepted.lock().clone()
        }
    }

    impl SignalingDataSink for RecordingSink {
        async fn accept(&self, payload: Vec<u8>) {
            self.accepted.lock().push(payload);
        }
    }

    /// A send step followed by a wait step, the ordinary shape of a one-exchange handshake.
    fn steps(sends: &[&str], matchers: &[&str]) -> Vec<SignalingStep> {
        vec![
            SignalingStep::Send(sends.iter().map(|p| p.to_string()).collect()),
            SignalingStep::Wait(SignalingWaitStep::new(
                matchers.iter().map(|p| p.to_string()).collect(),
            )),
        ]
    }

    /// The same pair, with the wait step opening the relay when it completes.
    fn accepting_steps(sends: &[&str], matchers: &[&str]) -> Vec<SignalingStep> {
        let mut built = steps(sends, matchers);
        if let Some(SignalingStep::Wait(wait)) = built.last_mut() {
            wait.accept_data = true;
        }
        built
    }

    fn captured_steps(sends: &[&str], matcher: &str, capture: &str) -> Vec<SignalingStep> {
        vec![
            SignalingStep::Send(sends.iter().map(|p| p.to_string()).collect()),
            SignalingStep::Wait(SignalingWaitStep {
                matchers: vec![matcher.to_string()],
                capture: Some(capture.to_string()),
                fail_matchers: Vec::new(),
                accept_data: false,
            }),
        ]
    }

    #[test]
    fn compiles_a_native_protocol() {
        let compiled = CompiledSignalingProtocol::compile(
            &protocol(
                SignalingWireFormat::Json,
                on_connect(&["{id: 1}"], &[".id == 1"], &[".error"]),
            ),
            None,
        )
        .expect("protocol should compile");

        assert_eq!(compiled.timeout, Duration::from_secs(5));
        assert_eq!(compiled.steps.len(), 2);
        assert_eq!(compiled.fails.len(), 1);
    }

    #[test]
    fn rejects_an_invalid_send_program() {
        let error = CompiledSignalingProtocol::compile(
            &protocol(
                SignalingWireFormat::Json,
                on_connect(&["{id: 1}", ".["], &[".id == 1"], &[]),
            ),
            None,
        )
        .expect_err("invalid program must fail to compile");

        assert!(
            matches!(
                &error,
                SignalingProtocolCompileError::InvalidJaqProgram {
                    clause: "SEND JAQ",
                    index: 2,
                    ..
                }
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn requires_descriptors_for_a_protobuf_protocol() {
        let error = CompiledSignalingProtocol::compile(
            &protocol(
                SignalingWireFormat::Protobuf(SignalingProtobufConfig {
                    resource: Identifier::parse("proto_bundle").expect("valid identifier"),
                    resource_version: None,
                    config: Vec::new(),
                    send_message: "nervix.test.Subscribe".to_string(),
                    wait_message: "nervix.test.Ack".to_string(),
                }),
                on_connect(&["{id: 1}"], &[".id == 1"], &[]),
            ),
            None,
        )
        .expect_err("protobuf protocol needs descriptors");

        assert!(matches!(
            error,
            SignalingProtocolCompileError::MissingProtobufDescriptors { .. }
        ));
    }

    #[test]
    fn encodes_text_and_binary_frames_by_format() {
        let json = CompiledSignalingProtocol::compile(
            &protocol(
                SignalingWireFormat::Json,
                on_connect(&["{id: 1}"], &[".id == 1"], &[]),
            ),
            None,
        )
        .expect("protocol should compile");
        assert!(matches!(
            json.encode_frame(json!({"id": 1})),
            Ok(Message::Text(_))
        ));

        let cbor = CompiledSignalingProtocol::compile(
            &protocol(
                SignalingWireFormat::Cbor,
                on_connect(&["{id: 1}"], &[".id == 1"], &[]),
            ),
            None,
        )
        .expect("protocol should compile");
        assert!(matches!(
            cbor.encode_frame(json!({"id": 1})),
            Ok(Message::Binary(_))
        ));
    }

    #[test]
    fn rejects_a_raw_send_output_that_is_not_a_string() {
        let raw = CompiledSignalingProtocol::compile(
            &protocol(
                SignalingWireFormat::Raw,
                on_connect(&["{id: 1}"], &[". == \"ok\""], &[]),
            ),
            None,
        )
        .expect("protocol should compile");

        assert!(raw.encode_frame(json!({"id": 1})).is_err());
    }

    #[test]
    fn decodes_only_frames_the_format_can_carry() {
        let json = CompiledSignalingProtocol::compile(
            &protocol(
                SignalingWireFormat::Json,
                on_connect(&["{id: 1}"], &[".id == 1"], &[]),
            ),
            None,
        )
        .expect("protocol should compile");

        assert_eq!(
            json.decode_frame(br#"{"id":1}"#, true),
            Some(json!({"id": 1}))
        );
        // A textual format also reads payloads that arrive as binary frames.
        assert_eq!(
            json.decode_frame(br#"{"id":1}"#, false),
            Some(json!({"id": 1}))
        );
        assert_eq!(json.decode_frame(b"not json", true), None);

        let cbor = CompiledSignalingProtocol::compile(
            &protocol(
                SignalingWireFormat::Cbor,
                on_connect(&["{id: 1}"], &[".id == 1"], &[]),
            ),
            None,
        )
        .expect("protocol should compile");
        let encoded = JaqNativeFormat::Cbor
            .write_value(json!({"id": 1}))
            .expect("cbor encode should succeed");

        assert_eq!(cbor.decode_frame(&encoded, false), Some(json!({"id": 1})));
        assert_eq!(cbor.decode_frame(&encoded, true), None);
    }

    #[test]
    fn treats_any_non_null_non_false_output_as_a_match() {
        let matcher =
            StatefulJaqProgram::compile(".id == 1 and .result == null").expect("compiles");
        let state = json!({});

        assert!(matcher_is_satisfied(
            &matcher,
            &json!({"id": 1, "result": null, "conn_id": "abc"}),
            &state
        ));
        assert!(!matcher_is_satisfied(&matcher, &json!({"id": 2}), &state));
        // A frame of an entirely different shape errors inside jaq, which is a non-match.
        assert!(!matcher_is_satisfied(
            &matcher,
            &json!("plain text"),
            &state
        ));
    }

    #[test]
    fn renders_a_rejection_reason_from_a_string_or_json_output() {
        assert_eq!(
            rejection_reason(&json!("subscribe rejected")),
            "subscribe rejected"
        );
        assert_eq!(rejection_reason(&json!({"code": 2})), r#"{"code":2}"#);
    }

    #[test]
    fn truncates_an_overlong_rejection_reason_on_a_char_boundary() {
        let reason = rejection_reason(&JsonValue::String("é".repeat(400)));

        assert!(reason.len() <= MAX_REJECTION_REASON_BYTES + '…'.len_utf8());
        assert!(reason.ends_with('…'));
    }

    #[test]
    fn reports_unsatisfied_matchers_in_the_timeout_error() {
        let error = WebsocketSignalingError::Timeout {
            timeout: Duration::from_secs(5),
            unsatisfied: vec![".id == 1".to_string(), ".id == 2".to_string()],
        };

        assert_eq!(
            error.to_string(),
            "signaling timed out after 5s; unsatisfied WAIT JAQ matchers: [.id == 1; .id == 2]"
        );
    }

    /// One action of the scripted peer the handshake runs against.
    enum PeerStep {
        Expect(String),
        ExpectSilence(Duration),
        Send(Message),
    }

    /// Drive a real handshake against an in-memory peer following `steps`.
    ///
    /// The peer stays connected after its script ends so an unfinished handshake times out rather
    /// than seeing a close, and any script violation fails the test rather than the peer task.
    async fn run_against_peer(
        protocol: CreateSignalingProtocol,
        steps: Vec<PeerStep>,
    ) -> Result<Vec<Vec<u8>>, WebsocketSignalingError> {
        let sink = RecordingSink::default();
        run_against_peer_with(protocol, steps, &sink)
            .await
            .map(|()| sink.accepted())
    }

    async fn run_against_peer_with(
        protocol: CreateSignalingProtocol,
        steps: Vec<PeerStep>,
        sink: &RecordingSink,
    ) -> Result<(), WebsocketSignalingError> {
        let (server_io, client_io) = tokio::io::duplex(8 * 1024);
        let compiled = Arc::new(
            CompiledSignalingProtocol::compile(&protocol, None).expect("protocol should compile"),
        );
        let failure = StdArc::new(Mutex::new(None::<String>));
        let peer_failure = StdArc::clone(&failure);

        let peer = tokio::spawn(async move {
            let mut peer = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
            for step in steps {
                match step {
                    PeerStep::Expect(expected) => match peer.next().await {
                        Some(Ok(Message::Text(actual))) if actual == expected => {}
                        other => {
                            *peer_failure.lock() =
                                Some(format!("expected frame {expected:?}, got {other:?}"));
                            return;
                        }
                    },
                    PeerStep::ExpectSilence(window) => {
                        if let Ok(frame) = tokio::time::timeout(window, peer.next()).await {
                            *peer_failure.lock() =
                                Some(format!("expected no frame for {window:?}, got {frame:?}"));
                            return;
                        }
                    }
                    PeerStep::Send(frame) => {
                        if peer.send(frame).await.is_err() {
                            return;
                        }
                    }
                }
            }
            std::future::pending::<()>().await;
        });

        let mut websocket = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let result = WebsocketSignalingSession::new(compiled)
            .run(&mut websocket, sink)
            .await;
        peer.abort();
        if let Some(failure) = failure.lock().clone() {
            panic!("peer script failed: {failure}");
        }
        result
    }

    #[tokio::test]
    async fn timing_out_names_only_the_matchers_that_never_matched() {
        let error = run_against_peer(
            protocol(
                SignalingWireFormat::Json,
                SignalingProtocolOnConnect {
                    accept_data: false,
                    steps: steps(&["{id: 1}", "{id: 2}"], &[".id == 1", ".id == 2"]),
                    fail_matchers: Vec::new(),
                    timeout: "200ms".to_string(),
                },
            ),
            vec![
                PeerStep::Expect(r#"{"id": 1}"#.to_string()),
                PeerStep::Expect(r#"{"id": 2}"#.to_string()),
                PeerStep::Send(Message::Text(r#"{"id":1}"#.to_string())),
            ],
        )
        .await
        .expect_err("an unanswered matcher must time out");

        assert!(
            matches!(
                &error,
                WebsocketSignalingError::Timeout { unsatisfied, .. }
                    if unsatisfied == &vec![".id == 2".to_string()]
            ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn payload_arriving_before_the_relay_opens_is_dropped() {
        let buffered = run_against_peer(
            protocol(
                SignalingWireFormat::Json,
                SignalingProtocolOnConnect {
                    accept_data: false,
                    steps: steps(&["{id: 1}"], &[".id == 1 and .result == null"]),
                    fail_matchers: vec![".error".to_string()],
                    timeout: "5s".to_string(),
                },
            ),
            vec![
                PeerStep::Expect(r#"{"id": 1}"#.to_string()),
                PeerStep::Send(Message::Text(r#"{"seq":1}"#.to_string())),
                // The acknowledgement carries fields the matcher never names.
                PeerStep::Send(Message::Text(
                    r#"{"id":1,"result":null,"conn_id":"abc"}"#.to_string(),
                )),
            ],
        )
        .await
        .expect("handshake should complete");

        // Nothing opened the relay, so the frame was never payload.
        assert!(buffered.is_empty());
    }

    #[tokio::test]
    async fn a_later_step_sends_with_state_captured_by_an_earlier_one() {
        let buffered = run_against_peer(
            protocol(
                SignalingWireFormat::Json,
                SignalingProtocolOnConnect {
                    accept_data: false,
                    steps: captured_steps(&[r#"{op: "auth"}"#], ".authed", "{token: .data.token}")
                        .into_iter()
                        .chain(steps(
                            &[r#"{op: "subscribe", token: $state.token}"#],
                            &[".subscribed"],
                        ))
                        .collect(),
                    fail_matchers: Vec::new(),
                    timeout: "5s".to_string(),
                },
            ),
            vec![
                PeerStep::Expect(r#"{"op": "auth"}"#.to_string()),
                PeerStep::Send(Message::Text(
                    r#"{"authed":true,"data":{"token":"tok-7f3a"}}"#.to_string(),
                )),
                // Proves the second step interpolated captured state.
                PeerStep::Expect(r#"{"op": "subscribe", "token": "tok-7f3a"}"#.to_string()),
                PeerStep::Send(Message::Text(r#"{"seq":1}"#.to_string())),
                PeerStep::Send(Message::Text(r#"{"subscribed":true}"#.to_string())),
            ],
        )
        .await
        .expect("handshake should complete");

        // The capture drove the second send; no step opened the relay, so nothing was payload.
        assert!(buffered.is_empty());
    }

    #[tokio::test]
    async fn a_later_step_withholds_its_sends_until_the_current_one_is_satisfied() {
        let error = run_against_peer(
            protocol(
                SignalingWireFormat::Json,
                SignalingProtocolOnConnect {
                    accept_data: false,
                    steps: steps(&[r#"{op: "first"}"#], &[r#".acked == "first""#])
                        .into_iter()
                        .chain(steps(&[r#"{op: "second"}"#], &[r#".acked == "second""#]))
                        .collect(),
                    fail_matchers: Vec::new(),
                    timeout: "200ms".to_string(),
                },
            ),
            // The peer never acknowledges the first step, so the second must never be written.
            vec![
                PeerStep::Expect(r#"{"op": "first"}"#.to_string()),
                PeerStep::ExpectSilence(Duration::from_millis(100)),
            ],
        )
        .await
        .expect_err("an unanswered step must time out");

        assert!(
            matches!(
                &error,
                WebsocketSignalingError::Timeout { unsatisfied, .. }
                    if unsatisfied == &vec![r#".acked == "first""#.to_string()]
            ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn accept_data_opens_the_relay_mid_handshake() {
        let sink = RecordingSink::default();
        let error = run_against_peer_with(
            protocol(
                SignalingWireFormat::Json,
                SignalingProtocolOnConnect {
                    accept_data: false,
                    // Acknowledging the first subscription opens the relay, even though the
                    // handshake continues negotiating the second.
                    steps: accepting_steps(&[r#"{op: "first"}"#], &[r#".acked == "first""#])
                        .into_iter()
                        .chain(steps(&[r#"{op: "second"}"#], &[r#".acked == "second""#]))
                        .collect(),
                    fail_matchers: Vec::new(),
                    timeout: "200ms".to_string(),
                },
            ),
            vec![
                PeerStep::Expect(r#"{"op": "first"}"#.to_string()),
                // Arrives before the relay opens, so it is not payload yet.
                PeerStep::Send(Message::Text(r#"{"seq":1}"#.to_string())),
                PeerStep::Send(Message::Text(r#"{"acked":"first"}"#.to_string())),
                PeerStep::Expect(r#"{"op": "second"}"#.to_string()),
                // Arrives after the relay opens, so it streams even though the handshake goes on.
                PeerStep::Send(Message::Text(r#"{"seq":2}"#.to_string())),
            ],
            &sink,
        )
        .await
        .expect_err("the second subscription is never acknowledged");

        assert!(
            matches!(error, WebsocketSignalingError::Timeout { .. }),
            "unexpected error: {error}"
        );
        // Only the frame that arrived after the relay opened reached it.
        assert_eq!(sink.accepted(), vec![br#"{"seq":2}"#.to_vec()]);
    }

    #[tokio::test]
    async fn accept_data_on_connect_streams_from_the_first_frame() {
        let sink = RecordingSink::default();
        let error = run_against_peer_with(
            protocol(
                SignalingWireFormat::Json,
                SignalingProtocolOnConnect {
                    // The relay is open before anything is negotiated.
                    accept_data: true,
                    steps: steps(&[r#"{op: "subscribe"}"#], &[".subscribed"]),
                    fail_matchers: Vec::new(),
                    timeout: "200ms".to_string(),
                },
            ),
            vec![
                PeerStep::Expect(r#"{"op": "subscribe"}"#.to_string()),
                PeerStep::Send(Message::Text(r#"{"seq":1}"#.to_string())),
            ],
            &sink,
        )
        .await
        .expect_err("the subscription is never acknowledged");

        assert!(
            matches!(error, WebsocketSignalingError::Timeout { .. }),
            "unexpected error: {error}"
        );
        assert_eq!(sink.accepted(), vec![br#"{"seq":1}"#.to_vec()]);
    }

    #[tokio::test]
    async fn a_step_scoped_fail_guard_rejects_during_its_own_step() {
        let error = run_against_peer(
            protocol(
                SignalingWireFormat::Json,
                SignalingProtocolOnConnect {
                    accept_data: false,
                    steps: vec![
                        SignalingStep::Send(vec![r#"{op: "subscribe"}"#.to_string()]),
                        SignalingStep::Wait(SignalingWaitStep {
                            matchers: vec![".subscribed".to_string()],
                            capture: None,
                            fail_matchers: vec![".denied".to_string()],
                            accept_data: false,
                        }),
                    ],
                    fail_matchers: Vec::new(),
                    timeout: "200ms".to_string(),
                },
            ),
            vec![
                PeerStep::Expect(r#"{"op": "subscribe"}"#.to_string()),
                PeerStep::Send(Message::Text(r#"{"denied":"quota"}"#.to_string())),
            ],
        )
        .await
        .expect_err("the step guard must reject");

        assert!(
            matches!(&error, WebsocketSignalingError::Rejected { matcher, .. } if matcher == ".denied"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn payload_is_dropped_when_the_opening_step_is_never_satisfied() {
        let sink = RecordingSink::default();
        let error = run_against_peer_with(
            protocol(
                SignalingWireFormat::Json,
                SignalingProtocolOnConnect {
                    accept_data: false,
                    steps: accepting_steps(&[r#"{op: "first"}"#], &[r#".acked == "first""#])
                        .into_iter()
                        .chain(steps(&[r#"{op: "second"}"#], &[r#".acked == "second""#]))
                        .collect(),
                    fail_matchers: Vec::new(),
                    timeout: "200ms".to_string(),
                },
            ),
            vec![
                PeerStep::Expect(r#"{"op": "first"}"#.to_string()),
                // The marked matcher is never satisfied, so ingestion never opens.
                PeerStep::Send(Message::Text(r#"{"seq":1}"#.to_string())),
            ],
            &sink,
        )
        .await
        .expect_err("the first subscription is never acknowledged");

        assert!(
            matches!(error, WebsocketSignalingError::Timeout { .. }),
            "unexpected error: {error}"
        );
        assert!(
            sink.accepted().is_empty(),
            "payload must not reach the relay before a step opens it"
        );
    }

    #[tokio::test]
    async fn a_capture_that_does_not_produce_an_object_fails_the_handshake() {
        let error = run_against_peer(
            protocol(
                SignalingWireFormat::Json,
                SignalingProtocolOnConnect {
                    accept_data: false,
                    steps: captured_steps(&["{id: 1}"], ".id == 1", ".id"),
                    fail_matchers: Vec::new(),
                    timeout: "5s".to_string(),
                },
            ),
            vec![
                PeerStep::Expect(r#"{"id": 1}"#.to_string()),
                PeerStep::Send(Message::Text(r#"{"id":1}"#.to_string())),
            ],
        )
        .await
        .expect_err("a non-object capture must fail");

        assert!(
            matches!(&error, WebsocketSignalingError::Capture { capture, .. } if capture == ".id"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn a_fail_matcher_rejects_before_a_lenient_wait_matcher_consumes_the_frame() {
        let error = run_against_peer(
            protocol(
                SignalingWireFormat::Json,
                SignalingProtocolOnConnect {
                    // Deliberately lenient: the matcher would also accept the error frame.
                    accept_data: false,
                    steps: steps(&["{id: 1}"], &[".id == 1"]),
                    fail_matchers: vec![".error".to_string()],
                    timeout: "5s".to_string(),
                },
            ),
            vec![
                PeerStep::Expect(r#"{"id": 1}"#.to_string()),
                PeerStep::Send(Message::Text(
                    r#"{"id":1,"error":"subscription denied"}"#.to_string(),
                )),
            ],
        )
        .await
        .expect_err("a rejection must abort the handshake");

        assert!(
            matches!(
                &error,
                WebsocketSignalingError::Rejected { matcher, reason }
                    if matcher == ".error" && reason == "subscription denied"
            ),
            "unexpected error: {error}"
        );
    }
}
