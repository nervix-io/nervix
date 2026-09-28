//! Browser draft for VHOST hostnames and optional TLS certificate binding.
//!
//! Layer: edges.
//!
//! - **Owns.** Incomplete hostname rows and a selected completed TLS resource version.
//! - **Depends on.** Current VHOST Models and resource pin browser choices.
//! - **Must not know.** Certificate validation or installation on live nodes.

use error_stack::Report;
use nervix_models::{CreateVhost, RequestedResourceVersion, VhostName, VhostTlsResource};
use thiserror::Error;

use super::resource_pin_draft::{ResourcePinDraft, ResourcePinError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VhostDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) hostnames: Vec<String>,
    pub(super) tls_enabled: bool,
    pub(super) tls: ResourcePinDraft,
}

impl Default for VhostDraft {
    fn default() -> Self {
        Self {
            name: String::new(),
            if_not_exists: false,
            hostnames: vec![String::new()],
            tls_enabled: false,
            tls: ResourcePinDraft::default(),
        }
    }
}

impl VhostDraft {
    pub(super) fn invalidate_references(&mut self) {
        self.tls.invalidate();
    }

    pub(super) fn current_resource(&self) -> Option<&nervix_models::ResourceName> {
        if self.tls_enabled {
            self.tls.current_resource()
        } else {
            None
        }
    }

    pub(super) fn build(
        &self,
    ) -> error_stack::Result<CreateVhost<RequestedResourceVersion>, VhostDraftError> {
        let name =
            VhostName::parse(self.name.trim()).map_err(|_| Report::new(VhostDraftError::Name))?;
        let mut hostnames = Vec::with_capacity(self.hostnames.len());
        for (index, hostname) in self.hostnames.iter().enumerate() {
            let hostname = hostname.trim();
            if hostname.is_empty() {
                return Err(Report::new(VhostDraftError::Hostname { entry: index + 1 }));
            }
            hostnames.push(hostname.to_string());
        }
        if hostnames.is_empty() {
            return Err(Report::new(VhostDraftError::Hostname { entry: 1 }));
        }
        let tls = if self.tls_enabled {
            let pin = self.tls.build().map_err(|error| {
                let context = VhostDraftError::Tls(error.current_context().clone());
                error.change_context(context)
            })?;
            Some(VhostTlsResource {
                resource: pin.resource,
                version: pin.version,
            })
        } else {
            None
        };
        Ok(CreateVhost {
            name,
            hostnames,
            tls,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum VhostDraftError {
    #[error("VHOST name is invalid")]
    Name,
    #[error("Hostname {entry} is required")]
    Hostname { entry: usize },
    #[error("{0}")]
    Tls(#[from] ResourcePinError),
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        CreateStatement, Model, RequestedResourceVersion, ResourceName, Statement,
    };
    use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};

    use super::{VhostDraft, VhostDraftError};

    #[test]
    fn hostnames_and_tls_pin_render_and_parse_as_one_current_model() {
        let mut draft = VhostDraft {
            name: "edge".to_string(),
            ..VhostDraft::default()
        };
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(VhostDraftError::Hostname { entry: 1 })
        );
        draft.hostnames = vec!["api.example.com".to_string(), "ws.example.com".to_string()];
        draft.tls_enabled = true;
        assert!(draft.build().is_err());
        draft
            .tls
            .select_resource(ResourceName::parse("tls_bundle").assured("valid resource"));
        draft
            .tls
            .select_version(RequestedResourceVersion::Number(3));
        let vhost = draft.build().assured("the TLS pin is complete");
        let statement =
            Statement::Create(CreateStatement::new(Box::new(Model::Vhost(vhost)), false));
        let source = statement.to_canonical_nspl().assured("VHOST renders");
        assert!(source.contains("api.example.com, ws.example.com"));
        assert!(source.contains("WITH TLS tls_bundle VERSION 3"));
        assert_eq!(
            parse_client_statement(&source).assured("VHOST parses"),
            ClientStatement::Server(statement)
        );
        draft.invalidate_references();
        assert!(draft.build().is_err());
    }
}
