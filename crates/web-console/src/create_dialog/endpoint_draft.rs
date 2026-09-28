//! Browser draft for HTTP and WebSocket endpoint bindings.
//!
//! Layer: edges.
//!
//! - **Owns.** Incomplete VHOST, path, type, and optional signaling selections.
//! - **Depends on.** Current endpoint Models and typed selected references.
//! - **Must not know.** Listener routing or WebSocket handshake execution.

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{
    CreateEndpoint, EndpointName, EndpointType, ModelKind, NodeRef, SignalingProtocolName,
    VhostName,
};
use thiserror::Error;

use super::SelectedReference;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct EndpointDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) vhost: Option<SelectedReference<VhostName>>,
    pub(super) path: String,
    pub(super) endpoint_type: Option<EndpointType>,
    pub(super) signaling_protocol: Option<SelectedReference<SignalingProtocolName>>,
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
        if node.kind == ModelKind::SignalingProtocol {
            self.signaling_protocol = Some(SelectedReference::chosen(
                SignalingProtocolName::parse(node.identifier.as_str())
                    .verified("a signaling protocol NodeRef carries a validated model name"),
            ));
        }
    }

    pub(super) fn select_type(&mut self, endpoint_type: EndpointType) {
        if self.endpoint_type != Some(endpoint_type) {
            self.endpoint_type = Some(endpoint_type);
            self.signaling_protocol = None;
        }
    }

    pub(super) fn selects_vhost(&self, node: &NodeRef) -> bool {
        self.vhost
            .as_ref()
            .is_some_and(|selected| selected.selects(ModelKind::Vhost, node))
    }

    pub(super) fn selects_signaling(&self, node: &NodeRef) -> bool {
        self.signaling_protocol
            .as_ref()
            .is_some_and(|selected| selected.selects(ModelKind::SignalingProtocol, node))
    }

    pub(super) fn invalidate_references(&mut self) {
        if let Some(vhost) = &mut self.vhost {
            vhost.invalidate();
        }
        if let Some(signaling) = &mut self.signaling_protocol {
            signaling.invalidate();
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<CreateEndpoint, EndpointDraftError> {
        let name = EndpointName::parse(self.name.trim())
            .map_err(|_| Report::new(EndpointDraftError::Name))?;
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
        let endpoint_type = self
            .endpoint_type
            .ok_or_else(|| Report::new(EndpointDraftError::TypeRequired))?;
        let signaling_protocol = if endpoint_type == EndpointType::Websockets {
            match &self.signaling_protocol {
                None => None,
                Some(selected) => Some(
                    selected
                        .current_name()
                        .ok_or_else(|| Report::new(EndpointDraftError::SignalingChanged))?
                        .clone(),
                ),
            }
        } else {
            None
        };
        Ok(CreateEndpoint {
            name,
            on_vhost,
            path: path.to_string(),
            endpoint_type,
            signaling_protocol,
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
    TypeRequired,
    #[error("The signaling protocol belongs to a changed context; select it again")]
    SignalingChanged,
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        CreateStatement, EndpointType, Model, ModelKind, ModelName, NodeRef, Statement,
    };
    use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};

    use super::{EndpointDraft, EndpointDraftError};

    fn node(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(kind, ModelName::parse(name).assured("valid model name"))
    }

    #[test]
    fn endpoint_type_switching_clears_signaling_and_preserves_typed_vhost_selection() {
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
            Some(EndpointDraftError::TypeRequired)
        );
        draft.select_type(EndpointType::Websockets);
        draft.select_signaling(&node(ModelKind::SignalingProtocol, "handshake"));
        let endpoint = draft.build().assured("WebSocket endpoint is complete");
        let statement = Statement::Create(CreateStatement::new(
            Box::new(Model::Endpoint(endpoint)),
            false,
        ));
        let source = statement.to_canonical_nspl().assured("endpoint renders");
        assert!(source.contains("TYPE WEBSOCKETS WITH SIGNALING PROTOCOL handshake"));
        assert_eq!(
            parse_client_statement(&source).assured("endpoint parses"),
            ClientStatement::Server(statement)
        );

        draft.select_type(EndpointType::Http);
        assert!(draft.signaling_protocol.is_none());
        let http = draft
            .build()
            .assured("HTTP endpoint does not need signaling");
        assert_eq!(http.endpoint_type, EndpointType::Http);
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
}
