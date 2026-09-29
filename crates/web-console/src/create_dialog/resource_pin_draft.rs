//! A browser selection of one existing resource and completed version.
//!
//! Layer: edges.
//!
//! - **Owns.** Incomplete and context-invalidated resource pins for clients, VHOSTs, codecs,
//!   signaling protocols, and hash maps.
//! - **Depends on.** Typed resource names and requested resource versions.
//! - **Must not know.** Resource storage, upload execution, or version resolution at apply time.

use error_stack::Report;
use nervix_models::{RequestedResourceVersion, ResourceName};
use thiserror::Error;

use super::SelectedReference;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct ResourcePinDraft {
    pub(super) resource: Option<SelectedReference<ResourceName>>,
    pub(super) version: Option<SelectedReference<RequestedResourceVersion>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResourcePin {
    pub(super) resource: ResourceName,
    pub(super) version: RequestedResourceVersion,
}

impl ResourcePinDraft {
    pub(super) fn select_resource(&mut self, resource: ResourceName) {
        if self.current_resource() != Some(&resource)
            && let Some(version) = &mut self.version
        {
            version.invalidate();
        }
        self.resource = Some(SelectedReference::chosen(resource));
    }

    pub(super) fn select_version(&mut self, version: RequestedResourceVersion) {
        self.version = Some(SelectedReference::chosen(version));
    }

    pub(super) fn current_resource(&self) -> Option<&ResourceName> {
        self.resource
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    pub(super) fn invalidate(&mut self) {
        if let Some(resource) = &mut self.resource {
            resource.invalidate();
        }
        if let Some(version) = &mut self.version {
            version.invalidate();
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<ResourcePin, ResourcePinError> {
        let resource = match &self.resource {
            None => return Err(Report::new(ResourcePinError::ResourceRequired)),
            Some(selection) => selection
                .current_name()
                .ok_or_else(|| Report::new(ResourcePinError::ResourceChanged))?
                .clone(),
        };
        let version = match &self.version {
            None => return Err(Report::new(ResourcePinError::VersionRequired)),
            Some(selection) => *selection
                .current_name()
                .ok_or_else(|| Report::new(ResourcePinError::VersionChanged))?,
        };
        Ok(ResourcePin { resource, version })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum ResourcePinError {
    #[error("Choose an existing resource")]
    ResourceRequired,
    #[error("The selected resource belongs to a changed context; select it again")]
    ResourceChanged,
    #[error("Choose LATEST or a completed resource version")]
    VersionRequired,
    #[error("The resource or domain changed; select the version again")]
    VersionChanged,
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{RequestedResourceVersion, ResourceName};

    use super::{ResourcePinDraft, ResourcePinError};

    #[test]
    fn changing_the_resource_or_domain_requires_a_fresh_completed_version() {
        let mut draft = ResourcePinDraft::default();
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ResourcePinError::ResourceRequired)
        );
        draft.select_resource(ResourceName::parse("first").assured("valid resource"));
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ResourcePinError::VersionRequired)
        );
        draft.select_version(RequestedResourceVersion::Number(2));
        let pin = draft.build().assured("resource and version selected");
        assert_eq!(pin.version, RequestedResourceVersion::Number(2));
        draft.select_resource(ResourceName::parse("second").assured("valid resource"));
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ResourcePinError::VersionChanged)
        );
        draft.select_version(RequestedResourceVersion::Latest);
        assert_eq!(
            draft.build().assured("new version selected").version,
            RequestedResourceVersion::Latest
        );
        draft.invalidate();
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ResourcePinError::ResourceChanged)
        );
    }
}
