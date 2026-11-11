//! Browser draft for HTTP and WebSocket endpoint bindings.
//!
//! Layer: edges.
//!
//! - **Owns.** Incomplete VHOST, path and typed endpoint-kind selections.
//! - **Depends on.** Current endpoint Models and typed selected references.
//! - **Must not know.** Listener routing or WebSocket handshake execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{
    CreateEndpoint, EndpointKind, EndpointName, EndpointType, ModelKind, NodeRef,
    SignalingProtocolName, VhostName,
};
use thiserror::Error;

use super::SelectedReference;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct EndpointDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) vhost: Option<SelectedReference<VhostName>>,
    pub(super) path: String,
    pub(super) kind: Option<EndpointKind<SelectedReference<SignalingProtocolName>>>,
}

impl EndpointDraft {
    pub(super) fn select_vhost(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Vhost {
            self.vhost = Some(SelectedReference::chosen(
                VhostName::parse(node.identifier.as_str())
                    .verified("a VHOST NodeRef carries a validated model name"),
            ));
        }
    }

    pub(super) fn select_signaling(&mut self, node: &NodeRef) {
        let Some(EndpointKind::Websockets { signaling_protocol }) = &mut self.kind else {
            return;
        };
        if node.kind == ModelKind::SignalingProtocol {
            *signaling_protocol = Some(SelectedReference::chosen(
                SignalingProtocolName::parse(node.identifier.as_str())
                    .verified("a signaling protocol NodeRef carries a validated model name"),
            ));
        }
    }

    pub(super) fn select_http(&mut self) {
        self.kind = Some(EndpointKind::Http);
    }

    pub(super) fn select_websockets(&mut self) {
        if let Some(EndpointKind::Websockets { .. }) = self.kind {
            return;
        }
        self.kind = Some(EndpointKind::Websockets {
            signaling_protocol: None,
        });
    }

    pub(super) fn clear_signaling(&mut self) {
        if let Some(EndpointKind::Websockets { signaling_protocol }) = &mut self.kind {
            *signaling_protocol = None;
        }
    }

    pub(super) fn endpoint_type(&self) -> Option<EndpointType> {
        self.kind.as_ref().map(EndpointKind::endpoint_type)
    }

    pub(super) fn signaling_protocol(&self) -> Option<&SelectedReference<SignalingProtocolName>> {
        match &self.kind {
            Some(kind) => kind.signaling_protocol(),
            None => None,
        }
    }

    pub(super) fn selects_vhost(&self, node: &NodeRef) -> bool {
        self.vhost
            .as_ref()
            .is_some_and(|selected| selected.selects(ModelKind::Vhost, node))
    }

    pub(super) fn selects_signaling(&self, node: &NodeRef) -> bool {
        self.signaling_protocol()
            .is_some_and(|selected| selected.selects(ModelKind::SignalingProtocol, node))
    }

    pub(super) fn invalidate_references(&mut self) {
        if let Some(vhost) = &mut self.vhost {
            vhost.invalidate();
        }
        if let Some(EndpointKind::Websockets {
            signaling_protocol: Some(signaling),
        }) = &mut self.kind
        {
            signaling.invalidate();
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<CreateEndpoint, EndpointDraftError> {
        let name = EndpointName::parse(self.name.trim())
            .map_err(|cause| cause.change_context(EndpointDraftError::Name))?;
        let on_vhost = match &self.vhost {
            None => return Err(Report::new(EndpointDraftError::VhostRequired)),
            Some(selected) => selected
                .current_name()
                .ok_or_else(|| Report::new(EndpointDraftError::VhostChanged))?
                .clone(),
        };
        let path = self.path.trim();
        if !path.starts_with('/') {
            return Err(Report::new(EndpointDraftError::Path));
        }
        let selected_kind = self
            .kind
            .as_ref()
            .ok_or_else(|| Report::new(EndpointDraftError::KindRequired))?;
        let kind = match selected_kind {
            EndpointKind::Http => EndpointKind::Http,
            EndpointKind::Websockets { signaling_protocol } => {
                let signaling_protocol = match signaling_protocol {
                    None => None,
                    Some(selected) => Some(
                        selected
                            .current_name()
                            .ok_or_else(|| Report::new(EndpointDraftError::SignalingChanged))?
                            .clone(),
                    ),
                };
                EndpointKind::Websockets { signaling_protocol }
            }
        };
        Ok(CreateEndpoint {
            labels: Default::default(),
            name,
            on_vhost,
            path: path.to_string(),
            kind,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum EndpointDraftError {
    #[error("Endpoint name is invalid")]
    Name,
    #[error("Choose an existing VHOST")]
    VhostRequired,
    #[error("The selected VHOST belongs to a changed context; select it again")]
    VhostChanged,
    #[error("Endpoint path must start with /")]
    Path,
    #[error("Choose HTTP or WEBSOCKETS")]
    KindRequired,
    #[error("The signaling protocol belongs to a changed context; select it again")]
    SignalingChanged,
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        CreateStatement, EndpointKind, Model, ModelKind, ModelName, NodeRef, Statement,
    };
    use nervix_plexus::client_statement::{ClientStatement, parse_client_statement};

    use super::{EndpointDraft, EndpointDraftError, SelectedReference};

    fn node(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(kind, ModelName::parse(name).assured("valid model name"))
    }

    #[test]
    fn endpoint_kind_selects_protocol_fields_and_preserves_vhost_selection() {
        let mut draft = EndpointDraft {
            name: "socket".to_string(),
            ..EndpointDraft::default()
        };
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(EndpointDraftError::VhostRequired)
        );
        draft.select_vhost(&node(ModelKind::Vhost, "edge"));
        draft.path = "/ws".to_string();
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(EndpointDraftError::KindRequired)
        );
        draft.select_websockets();
        draft.select_signaling(&node(ModelKind::SignalingProtocol, "handshake"));
        draft.select_websockets();
        let endpoint = draft.build().assured("WebSocket endpoint is complete");
        let statement = Statement::Create(CreateStatement::new(
            Box::new(Model::Endpoint(endpoint)),
            false,
        ));
        let source = statement.to_canonical_plexus().assured("endpoint renders");
        assert_eq!(
            source,
            "CREATE WEBSOCKETS ENDPOINT socket ON edge PATH '/ws' WITH SIGNALING PROTOCOL \
             handshake;"
        );
        assert_eq!(
            parse_client_statement(&source).assured("endpoint parses"),
            ClientStatement::Server(statement)
        );

        draft.select_http();
        draft.select_signaling(&node(ModelKind::SignalingProtocol, "handshake"));
        let http = draft
            .build()
            .assured("HTTP endpoint does not need signaling");
        assert_eq!(http.kind, EndpointKind::Http);
        draft.path = "relative".to_string();
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(EndpointDraftError::Path)
        );
        draft.path = "/ws".to_string();
        draft.invalidate_references();
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(EndpointDraftError::VhostChanged)
        );
    }

    #[test]
    fn stale_websockets_signaling_requires_reselection_or_explicit_clear() {
        let mut draft = EndpointDraft {
            name: "socket".to_string(),
            path: "/ws".to_string(),
            ..EndpointDraft::default()
        };
        draft.select_websockets();
        draft.select_vhost(&node(ModelKind::Vhost, "edge"));
        draft.select_signaling(&node(ModelKind::SignalingProtocol, "handshake"));
        draft.invalidate_references();
        draft.select_vhost(&node(ModelKind::Vhost, "edge"));
        let Err(error) = draft.build() else {
            panic!("a signaling selection from the preceding context must be refused");
        };
        assert_eq!(
            error.current_context(),
            &EndpointDraftError::SignalingChanged
        );
        draft.clear_signaling();
        let endpoint = draft
            .build()
            .assured("the current VHOST and explicit empty signaling selection are valid");
        assert_eq!(
            endpoint.kind,
            EndpointKind::Websockets {
                signaling_protocol: None
            }
        );
    }

    #[test]
    fn bolero_endpoint_drafts_preserve_the_complete_model() {
        bolero::check!()
            .with_iterations(128)
            .with_max_len(1024)
            .for_each(|bytes: &[u8]| {
                let mut arbitrary =
                    nervix_arbitrary::Arbitrary::new(bytes, nervix_arbitrary::Domain::Plexus);
                let mut endpoint = arbitrary.create_endpoint();
                // Drafts trim surrounding whitespace. Named rooted paths form a lossless domain;
                // canonical Model properties independently cover arbitrary path contents.
                endpoint.path = format!(
                    "/{}",
                    arbitrary.name::<nervix_models::EndpointName>().as_str()
                );
                let kind = match &endpoint.kind {
                    EndpointKind::Http => EndpointKind::Http,
                    EndpointKind::Websockets { signaling_protocol } => {
                        let signaling_protocol = signaling_protocol
                            .as_ref()
                            .map(|name| SelectedReference::chosen(name.clone()));
                        EndpointKind::Websockets { signaling_protocol }
                    }
                };
                let draft = EndpointDraft {
                    name: endpoint.name.as_str().to_string(),
                    if_not_exists: arbitrary.entropy().flag(),
                    vhost: Some(SelectedReference::chosen(endpoint.on_vhost.clone())),
                    path: endpoint.path.clone(),
                    kind: Some(kind),
                };
                assert_eq!(
                    draft.build().assured(
                        "the generated draft has valid names, a rooted path and current references"
                    ),
                    endpoint
                );
                let statement = Statement::Create(CreateStatement::new(
                    Box::new(Model::Endpoint(endpoint)),
                    draft.if_not_exists,
                ));
                let source = statement
                    .to_canonical_plexus()
                    .assured("every endpoint kind renders");
                assert_eq!(
                    parse_client_statement(&source)
                        .assured("the canonical endpoint head selects its body"),
                    ClientStatement::Server(statement),
                    "{source}"
                );
            });
    }
}
