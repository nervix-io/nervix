//! The model resource, requested version and file a visual inferencer or WASM processor loads.
//!
//! Layer: edges.
//!
//! - **Owns.** The browser selection of a processor's resource and requested version and the
//!   text of the file it loads from that version, until they become the Model's binding.
//! - **Depends on.** The shared resource pin draft and typed resource names and versions.
//! - **Must not know.** Resource storage, version pinning at application, or loading the file.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::Report;
use nervix_models::{RequestedResourceVersion, ResourceName};

use super::{processor_draft::ProcessorDraftError, resource_pin_draft::ResourcePinDraft};

/// A processor's resource and version choices and the file it names, as edited.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct ProcessorBindingDraft {
    pub(super) pin: ResourcePinDraft,
    /// The file inside the version, kept exactly as typed: the language writes it as a string.
    pub(super) file: String,
}

/// A complete binding. `LATEST` stays the requested version until the server applies the
/// statement and pins the version it resolves to.
pub(super) struct ProcessorBinding {
    pub(super) resource: ResourceName,
    pub(super) version: RequestedResourceVersion,
    pub(super) file: String,
}

impl ProcessorBindingDraft {
    pub(super) fn invalidate(&mut self) {
        self.pin.invalidate();
    }

    /// The binding, or the first missing or changed choice in form order. Whether the file
    /// exists in the version, and loads as a model or guest, is checked when the processor is
    /// created.
    pub(super) fn build(&self) -> error_stack::Result<ProcessorBinding, ProcessorDraftError> {
        let pin = match self.pin.build() {
            Ok(pin) => pin,
            Err(error) => {
                let cause = error.current_context().clone();
                return Err(error.change_context(ProcessorDraftError::Binding(cause)));
            }
        };
        if self.file.trim().is_empty() {
            return Err(Report::new(ProcessorDraftError::File));
        }
        Ok(ProcessorBinding {
            resource: pin.resource,
            version: pin.version,
            file: self.file.clone(),
        })
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;
    use crate::create_dialog::resource_pin_draft::ResourcePinError;

    fn refusal(draft: &ProcessorBindingDraft) -> ProcessorDraftError {
        draft
            .build()
            .err()
            .assured("the binding is incomplete")
            .current_context()
            .clone()
    }

    #[test]
    fn a_binding_keeps_its_requested_version_and_exact_file_until_application() {
        let mut draft = ProcessorBindingDraft::default();
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::Binding(ResourcePinError::ResourceRequired)
        );
        draft
            .pin
            .select_resource(ResourceName::parse("model_bundle").assured("valid resource"));
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::Binding(ResourcePinError::VersionRequired)
        );
        draft.pin.select_version(RequestedResourceVersion::Latest);
        assert_eq!(refusal(&draft), ProcessorDraftError::File);
        draft.file = "  ".into();
        assert_eq!(refusal(&draft), ProcessorDraftError::File);
        draft.file = " models/score.onnx".into();
        let binding = draft.build().assured("the binding is complete");
        assert_eq!(binding.resource.as_str(), "model_bundle");
        assert_eq!(binding.version, RequestedResourceVersion::Latest);
        assert_eq!(binding.file, " models/score.onnx");

        draft.invalidate();
        assert_eq!(
            refusal(&draft),
            ProcessorDraftError::Binding(ResourcePinError::ResourceChanged)
        );
    }
}
