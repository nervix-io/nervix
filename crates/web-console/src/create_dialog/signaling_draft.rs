//! Browser draft and semantic conversion for signaling protocols.
//!
//! Layer: edges.
//!
//! - **Owns.** Incomplete wire format and strictly ordered handshake steps in one browser draft.
//! - **Depends on.** Current signaling Models and resource binding draft controls.
//! - **Must not know.** WebSocket transport, peer frames, or resource installation.

use error_stack::Report;
use nervix_models::{
    CreateSignalingProtocol, RequestedResourceVersion, SignalingProtobufConfig,
    SignalingProtocolName, SignalingProtocolOnConnect, SignalingStep, SignalingWaitStep,
    SignalingWireFormat,
};
use thiserror::Error;

use super::resource_binding_draft::{ResourceBindingDraft, ResourceDraftError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SignalingFormatKind {
    Json,
    Yaml,
    Toml,
    Xml,
    Cbor,
    Raw,
    Protobuf,
}

impl SignalingFormatKind {
    pub(super) const ALL: [Self; 7] = [
        Self::Json,
        Self::Yaml,
        Self::Toml,
        Self::Xml,
        Self::Cbor,
        Self::Raw,
        Self::Protobuf,
    ];

    pub(super) fn key(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Yaml => "yaml",
            Self::Toml => "toml",
            Self::Xml => "xml",
            Self::Cbor => "cbor",
            Self::Raw => "raw",
            Self::Protobuf => "protobuf",
        }
    }

    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::Yaml => "YAML",
            Self::Toml => "TOML",
            Self::Xml => "XML",
            Self::Cbor => "CBOR",
            Self::Raw => "RAW",
            Self::Protobuf => "Protobuf",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SignalingFormatDraft {
    Plain(SignalingFormatKind),
    Protobuf {
        binding: ResourceBindingDraft,
        send_message: String,
        wait_message: String,
    },
}

impl SignalingFormatDraft {
    pub(super) fn kind(&self) -> SignalingFormatKind {
        match self {
            Self::Plain(kind) => *kind,
            Self::Protobuf { .. } => SignalingFormatKind::Protobuf,
        }
    }

    fn build(
        &self,
    ) -> error_stack::Result<SignalingWireFormat<RequestedResourceVersion>, SignalingDraftError>
    {
        Ok(match self {
            Self::Plain(SignalingFormatKind::Json) => SignalingWireFormat::Json,
            Self::Plain(SignalingFormatKind::Yaml) => SignalingWireFormat::Yaml,
            Self::Plain(SignalingFormatKind::Toml) => SignalingWireFormat::Toml,
            Self::Plain(SignalingFormatKind::Xml) => SignalingWireFormat::Xml,
            Self::Plain(SignalingFormatKind::Cbor) => SignalingWireFormat::Cbor,
            Self::Plain(SignalingFormatKind::Raw) => SignalingWireFormat::Raw,
            Self::Plain(SignalingFormatKind::Protobuf) => {
                return Err(Report::new(SignalingDraftError::FormatRequired));
            }
            Self::Protobuf {
                binding,
                send_message,
                wait_message,
            } => {
                let binding = binding.build().map_err(|error| {
                    let context = SignalingDraftError::Resource(error.current_context().clone());
                    error.change_context(context)
                })?;
                if send_message.trim().is_empty() || wait_message.trim().is_empty() {
                    return Err(Report::new(SignalingDraftError::ProtobufMessagesRequired));
                }
                SignalingWireFormat::Protobuf(SignalingProtobufConfig {
                    resource: binding.resource,
                    resource_version: binding.version,
                    config: binding.config,
                    send_message: send_message.clone(),
                    wait_message: wait_message.clone(),
                })
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SignalingStepDraft {
    Send {
        programs: Vec<String>,
    },
    Wait {
        matchers: Vec<String>,
        capture: Option<String>,
        fail_matchers: Vec<String>,
        accept_data: bool,
    },
}

impl SignalingStepDraft {
    pub(super) fn send() -> Self {
        Self::Send {
            programs: vec![String::new()],
        }
    }

    pub(super) fn wait() -> Self {
        Self::Wait {
            matchers: vec![String::new()],
            capture: None,
            fail_matchers: Vec::new(),
            accept_data: false,
        }
    }

    fn build(&self, position: usize) -> error_stack::Result<SignalingStep, SignalingDraftError> {
        match self {
            Self::Send { programs } => {
                validate_programs(programs, position, "SEND")?;
                Ok(SignalingStep::Send(programs.clone()))
            }
            Self::Wait {
                matchers,
                capture,
                fail_matchers,
                accept_data,
            } => {
                validate_programs(matchers, position, "WAIT")?;
                validate_optional_programs(fail_matchers, position, "FAIL")?;
                if let Some(capture) = capture {
                    if matchers.len() != 1 {
                        return Err(Report::new(SignalingDraftError::CaptureNeedsOneMatcher {
                            step: position,
                        }));
                    }
                    if capture.trim().is_empty() {
                        return Err(Report::new(SignalingDraftError::EmptyProgram {
                            step: position,
                            clause: "CAPTURE",
                        }));
                    }
                }
                Ok(SignalingStep::Wait(SignalingWaitStep {
                    matchers: matchers.clone(),
                    capture: capture.clone(),
                    fail_matchers: fail_matchers.clone(),
                    accept_data: *accept_data,
                }))
            }
        }
    }
}

fn validate_programs(
    programs: &[String],
    step: usize,
    clause: &'static str,
) -> error_stack::Result<(), SignalingDraftError> {
    if programs.is_empty() {
        return Err(Report::new(SignalingDraftError::MissingProgram {
            step,
            clause,
        }));
    }
    validate_optional_programs(programs, step, clause)
}

fn validate_optional_programs(
    programs: &[String],
    step: usize,
    clause: &'static str,
) -> error_stack::Result<(), SignalingDraftError> {
    if programs.iter().any(|program| program.trim().is_empty()) {
        return Err(Report::new(SignalingDraftError::EmptyProgram {
            step,
            clause,
        }));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct SignalingDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) format: Option<SignalingFormatDraft>,
    pub(super) accept_data: bool,
    pub(super) steps: Vec<SignalingStepDraft>,
    pub(super) fail_matchers: Vec<String>,
    pub(super) timeout: String,
}

impl SignalingDraft {
    pub(super) fn set_format(&mut self, kind: SignalingFormatKind) {
        if self
            .format
            .as_ref()
            .is_some_and(|format| format.kind() == kind)
        {
            return;
        }
        self.format = Some(match kind {
            SignalingFormatKind::Protobuf => SignalingFormatDraft::Protobuf {
                binding: ResourceBindingDraft::default(),
                send_message: String::new(),
                wait_message: String::new(),
            },
            _ => SignalingFormatDraft::Plain(kind),
        });
    }

    pub(super) fn binding(&self) -> Option<&ResourceBindingDraft> {
        match &self.format {
            Some(SignalingFormatDraft::Protobuf { binding, .. }) => Some(binding),
            _ => None,
        }
    }

    pub(super) fn binding_mut(&mut self) -> Option<&mut ResourceBindingDraft> {
        match &mut self.format {
            Some(SignalingFormatDraft::Protobuf { binding, .. }) => Some(binding),
            _ => None,
        }
    }

    pub(super) fn invalidate_references(&mut self) {
        if let Some(binding) = self.binding_mut() {
            binding.invalidate();
        }
    }

    pub(super) fn move_step_up(&mut self, index: usize) {
        if index > 0 && index < self.steps.len() {
            self.steps.swap(index - 1, index);
        }
    }

    pub(super) fn move_step_down(&mut self, index: usize) {
        if index + 1 < self.steps.len() {
            self.steps.swap(index, index + 1);
        }
    }

    pub(super) fn build(
        &self,
    ) -> error_stack::Result<CreateSignalingProtocol<RequestedResourceVersion>, SignalingDraftError>
    {
        let name = SignalingProtocolName::parse(self.name.trim())
            .map_err(|_| Report::new(SignalingDraftError::Name))?;
        let format = self
            .format
            .as_ref()
            .ok_or_else(|| Report::new(SignalingDraftError::FormatRequired))?
            .build()?;
        if self.timeout.trim().is_empty() {
            return Err(Report::new(SignalingDraftError::TimeoutRequired));
        }
        if self.steps.is_empty() {
            return Err(Report::new(SignalingDraftError::StepsRequired));
        }
        if !self
            .steps
            .iter()
            .any(|step| matches!(step, SignalingStepDraft::Send { .. }))
        {
            return Err(Report::new(SignalingDraftError::SendRequired));
        }
        if !self
            .steps
            .iter()
            .any(|step| matches!(step, SignalingStepDraft::Wait { .. }))
        {
            return Err(Report::new(SignalingDraftError::WaitRequired));
        }
        validate_optional_programs(&self.fail_matchers, 0, "protocol FAIL")?;
        let mut steps = Vec::with_capacity(self.steps.len());
        for (index, step) in self.steps.iter().enumerate() {
            steps.push(step.build(index + 1)?);
        }
        Ok(CreateSignalingProtocol {
            name,
            format,
            on_connect: SignalingProtocolOnConnect {
                accept_data: self.accept_data,
                steps,
                fail_matchers: self.fail_matchers.clone(),
                timeout: self.timeout.trim().to_string(),
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum SignalingDraftError {
    #[error("Signaling protocol name is invalid")]
    Name,
    #[error("Choose a signaling wire format")]
    FormatRequired,
    #[error("Protobuf send and wait message types are required")]
    ProtobufMessagesRequired,
    #[error("Timeout is required")]
    TimeoutRequired,
    #[error("Add at least one SEND or WAIT step")]
    StepsRequired,
    #[error("Add at least one SEND step")]
    SendRequired,
    #[error("Add at least one WAIT step")]
    WaitRequired,
    #[error("Step {step} needs a {clause} program")]
    MissingProgram { step: usize, clause: &'static str },
    #[error("Step {step} has an empty {clause} program")]
    EmptyProgram { step: usize, clause: &'static str },
    #[error("CAPTURE on step {step} requires exactly one WAIT matcher")]
    CaptureNeedsOneMatcher { step: usize },
    #[error("{0}")]
    Resource(#[from] ResourceDraftError),
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        CreateStatement, Model, RequestedResourceVersion, ResourceName, Statement,
    };
    use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};

    use super::{
        SignalingDraft, SignalingDraftError, SignalingFormatDraft, SignalingFormatKind,
        SignalingStepDraft,
    };

    fn draft(kind: SignalingFormatKind) -> SignalingDraft {
        let mut draft = SignalingDraft {
            name: "visual_protocol".to_string(),
            timeout: "5s".to_string(),
            ..SignalingDraft::default()
        };
        draft.set_format(kind);
        draft.steps = vec![
            SignalingStepDraft::Send {
                programs: vec!["{op: \"login\", id: 1}".to_string()],
            },
            SignalingStepDraft::Wait {
                matchers: vec![".id == 1".to_string()],
                capture: Some("{token: .token}".to_string()),
                fail_matchers: vec![".error".to_string()],
                accept_data: true,
            },
            SignalingStepDraft::Send {
                programs: vec!["{token: $state.token}".to_string()],
            },
        ];
        draft.fail_matchers = vec![".fatal".to_string()];
        draft
    }

    fn round_trip(draft: &SignalingDraft) -> String {
        let protocol = draft.build().assured("test protocol is complete");
        let statement = Statement::Create(CreateStatement::new(
            Box::new(Model::<RequestedResourceVersion>::SignalingProtocol(
                protocol,
            )),
            draft.if_not_exists,
        ));
        let source = statement.to_canonical_nspl().assured("protocol renders");
        assert_eq!(
            parse_client_statement(&source).assured("canonical protocol parses"),
            ClientStatement::Server(statement),
        );
        source
    }

    #[test]
    fn every_plain_format_keeps_send_wait_fail_capture_accept_and_order() {
        for kind in [
            SignalingFormatKind::Json,
            SignalingFormatKind::Yaml,
            SignalingFormatKind::Toml,
            SignalingFormatKind::Xml,
            SignalingFormatKind::Cbor,
            SignalingFormatKind::Raw,
        ] {
            let source = round_trip(&draft(kind));
            let first_send = source.find("SEND JAQ").verified("first step is a send");
            let wait = source.find("WAIT JAQ").verified("second step is a wait");
            let last_send = source.rfind("SEND JAQ").verified("last step is a send");
            assert!(first_send < wait && wait < last_send);
            assert!(source.contains("CAPTURE"));
            assert!(source.contains("ACCEPT DATA"));
        }
    }

    #[test]
    fn protobuf_preserves_latest_file_and_message_directions() {
        let mut draft = draft(SignalingFormatKind::Protobuf);
        if let Some(SignalingFormatDraft::Protobuf {
            binding,
            send_message,
            wait_message,
        }) = &mut draft.format
        {
            binding.select_resource(ResourceName::parse("proto_bundle").assured("valid resource"));
            binding.select_version(RequestedResourceVersion::Latest);
            binding.file = "handshake.proto".to_string();
            *send_message = "pkg.Request".to_string();
            *wait_message = "pkg.Reply".to_string();
        }
        let source = round_trip(&draft);
        assert!(source.contains("USING RESOURCE proto_bundle VERSION LATEST"));
        assert!(source.contains("SEND MESSAGE 'pkg.Request' WAIT MESSAGE 'pkg.Reply'"));
        if let Some(SignalingFormatDraft::Protobuf { binding, .. }) = &mut draft.format {
            binding.select_version(RequestedResourceVersion::Number(3));
        }
        assert!(round_trip(&draft).contains("USING RESOURCE proto_bundle VERSION 3"));
    }

    #[test]
    fn capture_needs_one_matcher_and_invalid_programs_keep_the_draft() {
        let mut draft = draft(SignalingFormatKind::Json);
        if let SignalingStepDraft::Wait { matchers, .. } = &mut draft.steps[1] {
            matchers.push(".id == 2".to_string());
        }
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(SignalingDraftError::CaptureNeedsOneMatcher { step: 2 })
        );
        if let SignalingStepDraft::Wait { matchers, .. } = &mut draft.steps[1] {
            matchers.pop();
            matchers[0].clear();
        }
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(SignalingDraftError::EmptyProgram {
                step: 2,
                clause: "WAIT"
            })
        );
        assert_eq!(draft.steps.len(), 3);
    }

    #[test]
    fn a_handshake_requires_a_step() {
        let mut draft = draft(SignalingFormatKind::Json);
        draft.steps.clear();
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(SignalingDraftError::StepsRequired)
        );
        draft.steps.push(SignalingStepDraft::send());
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(SignalingDraftError::WaitRequired)
        );
        draft.steps.clear();
        draft.steps.push(SignalingStepDraft::wait());
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(SignalingDraftError::SendRequired)
        );
    }

    #[test]
    fn moving_steps_changes_the_canonical_handshake_order() {
        let mut draft = draft(SignalingFormatKind::Json);
        draft.move_step_up(1);
        let moved = round_trip(&draft);
        assert!(
            moved.find("WAIT JAQ").verified("wait step is present")
                < moved.find("SEND JAQ").verified("send step is present")
        );
        draft.move_step_down(0);
        let restored = round_trip(&draft);
        assert!(
            restored.find("SEND JAQ").verified("send step is present")
                < restored.find("WAIT JAQ").verified("wait step is present")
        );
    }
}
