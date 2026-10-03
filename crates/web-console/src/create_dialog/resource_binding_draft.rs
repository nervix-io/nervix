//! Incomplete resource binding controls shared by the codec and signaling forms.
//!
//! Layer: edges.
//!
//! - **Owns.** A browser draft's selected resource, requested version, and Protobuf compiler inputs.
//! - **Depends on.** Vocabulary resource names, requested versions, and connector config entries.
//! - **Must not know.** Resource catalog storage, upload transport, or version pinning at apply time.

use error_stack::Report;
use nervix_models::{ClientConfigEntry, RequestedResourceVersion, ResourceName};
use thiserror::Error;

use super::SelectedReference;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct ResourceBindingDraft {
    pub(super) resource: Option<SelectedReference<ResourceName>>,
    pub(super) version: Option<SelectedReference<RequestedResourceVersion>>,
    pub(super) file: String,
    pub(super) include: String,
    pub(super) config: Vec<ConfigEntryDraft>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct ConfigEntryDraft {
    pub(super) key: String,
    pub(super) value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CompletedResourceBinding {
    pub(super) resource: ResourceName,
    pub(super) version: RequestedResourceVersion,
    pub(super) config: Vec<ClientConfigEntry>,
}

impl ResourceBindingDraft {
    pub(super) fn select_resource(&mut self, resource: ResourceName) {
        if self
            .resource
            .as_ref()
            .and_then(SelectedReference::current_name)
            != Some(&resource)
            && let Some(version) = &mut self.version
        {
            version.invalidate();
        }
        self.resource = Some(SelectedReference::chosen(resource));
    }

    pub(super) fn select_version(&mut self, version: RequestedResourceVersion) {
        self.version = Some(SelectedReference::chosen(version));
    }

    pub(super) fn invalidate(&mut self) {
        if let Some(resource) = &mut self.resource {
            resource.invalidate();
        }
        if let Some(version) = &mut self.version {
            version.invalidate();
        }
    }

    pub(super) fn current_resource(&self) -> Option<&ResourceName> {
        self.resource
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    pub(super) fn build(
        &self,
    ) -> error_stack::Result<CompletedResourceBinding, ResourceDraftError> {
        let resource = match &self.resource {
            None => return Err(Report::new(ResourceDraftError::ResourceRequired)),
            Some(selected) => selected
                .current_name()
                .ok_or_else(|| Report::new(ResourceDraftError::ResourceChanged))?
                .clone(),
        };
        let version = match &self.version {
            None => return Err(Report::new(ResourceDraftError::VersionRequired)),
            Some(selected) => *selected
                .current_name()
                .ok_or_else(|| Report::new(ResourceDraftError::VersionChanged))?,
        };
        let mut config = Vec::new();
        if !self.file.is_empty() {
            config.push(ClientConfigEntry {
                key: "file".to_string(),
                value: self.file.clone(),
            });
        }
        if !self.include.is_empty() {
            config.push(ClientConfigEntry {
                key: "include".to_string(),
                value: self.include.clone(),
            });
        }
        for (index, entry) in self.config.iter().enumerate() {
            let key = entry.key.trim();
            if key.is_empty() {
                return Err(Report::new(ResourceDraftError::ConfigKey {
                    entry: index + 1,
                }));
            }
            config.push(ClientConfigEntry {
                key: key.to_string(),
                value: entry.value.clone(),
            });
        }
        Ok(CompletedResourceBinding {
            resource,
            version,
            config,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum ResourceDraftError {
    #[error("Choose an existing resource")]
    ResourceRequired,
    #[error("The selected resource belongs to a changed context; select it again")]
    ResourceChanged,
    #[error("Choose LATEST or a completed resource version")]
    VersionRequired,
    #[error("The resource or domain changed; select the version again")]
    VersionChanged,
    #[error("Configuration entry {entry} needs a key")]
    ConfigKey { entry: usize },
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{RequestedResourceVersion, ResourceName};

    use super::{ConfigEntryDraft, ResourceBindingDraft, ResourceDraftError};

    #[test]
    fn a_binding_requires_current_resource_and_version_and_preserves_compiler_config_order() {
        let mut draft = ResourceBindingDraft::default();
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ResourceDraftError::ResourceRequired)
        );
        draft.select_resource(ResourceName::parse("first_bundle").assured("valid resource"));
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ResourceDraftError::VersionRequired)
        );
        draft.select_version(RequestedResourceVersion::Number(1));
        draft.select_resource(ResourceName::parse("second_bundle").assured("valid resource"));
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ResourceDraftError::VersionChanged)
        );
        draft.select_version(RequestedResourceVersion::Latest);
        draft.file = "payload.proto".to_string();
        draft.include = "imports".to_string();
        draft.config.push(ConfigEntryDraft {
            key: "  optimize_for  ".to_string(),
            value: "SPEED".to_string(),
        });
        let binding = draft.build().assured("the binding is complete");
        assert_eq!(binding.resource.as_str(), "second_bundle");
        assert_eq!(binding.version, RequestedResourceVersion::Latest);
        assert_eq!(
            binding
                .config
                .iter()
                .map(|entry| (entry.key.as_str(), entry.value.as_str()))
                .collect::<Vec<_>>(),
            [
                ("file", "payload.proto"),
                ("include", "imports"),
                ("optimize_for", "SPEED"),
            ]
        );
        assert_eq!(
            draft
                .current_resource()
                .verified("resource selection is current")
                .as_str(),
            "second_bundle"
        );
        draft.config.push(ConfigEntryDraft {
            key: "  ".to_string(),
            value: String::new(),
        });
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ResourceDraftError::ConfigKey { entry: 2 })
        );
        draft.invalidate();
        assert!(draft.current_resource().is_none());
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ResourceDraftError::ResourceChanged)
        );
    }
}
