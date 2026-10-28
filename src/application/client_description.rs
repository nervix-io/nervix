//! Inspection of a configured connector client.
//!
//! Layer: edges.
//! - **Owns.** Reading a client's committed declaration and rendering its effective public keys.
//! - **Depends on.** The registry, client vocabulary and the schema's confidentiality projection.
//! - **Must not know.** Connector drivers, live connections or emitter execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns session configured-client inspection")
)]

use std::collections::{BTreeMap, BTreeSet};

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_models::{DescribeClient, DomainName, ModelKind, ModelName};

use super::{
    command_result::CommandResult,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
};
use crate::registry::RegistryError;

impl SessionServiceImpl {
    pub(in crate::application) fn describe_client(
        &self,
        domain: &DomainName,
        describe: DescribeClient,
    ) -> CommandResult {
        let model = match self.inner.registry.get_of_kind(
            domain,
            ModelKind::Client,
            ModelName::from(&describe.name),
        ) {
            Ok(Some(model)) => model,
            Ok(None) => {
                return command_error(format!("client '{}' not found", describe.name.as_str()));
            }
            Err(error) => return command_error(format!("{error:#}")),
        };
        // Client is a shared Model kind, so its connector-specific variant is selected here.
        let Some(client) = model.configured_client() else {
            let error = Report::new(RegistryError::StoredModelKindMismatch {
                domain: domain.as_str().to_string(),
                identifier: describe.name.as_str().to_string(),
                expected_kind: ModelKind::Client.as_str(),
                stored_kind: model.kind().as_str(),
            });
            return command_error(format!("{error:#}"));
        };
        let schema = client
            .connector
            .client()
            .assured("every configured client has a connector declaration")
            .config;
        let authored: BTreeSet<_> = client
            .config
            .iter()
            .map(|entry| entry.key.as_str())
            .collect();
        let effective = schema.effective_entries(client.config);
        let inspected: BTreeMap<_, _> = schema
            .inspection_entries(&effective)
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect();
        let mut lines = vec![
            format!("client: {}", client.name.as_str()),
            format!("connector: {}", client.connector.keyword()),
            "config:".to_string(),
        ];
        for (key, value) in inspected {
            let origin = if authored.contains(key.as_str()) {
                ""
            } else {
                " (default)"
            };
            lines.push(format!("{key}: {value}{origin}"));
        }
        command_ok(lines.join("\n"))
    }
}
