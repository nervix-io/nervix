//! Incomplete browser state for a resource-backed hash map.
//!
//! Layer: edges.
//!
//! - **Owns.** Selected resource, codec and key references, file path, and validation before a
//!   browser draft becomes the current lookup Model.
//! - **Depends on.** Typed semantic names, the resource pin draft, and the current lookup Model.
//! - **Must not know.** Resource loading, codec execution, or version resolution at application.

use error_stack::Report;
use nervix_models::{
    CodecName, CreateLookup, FieldName, LookupName, ModelKind, NodeRef, RequestedResourceVersion,
};
use thiserror::Error;

use super::{
    SelectedReference,
    resource_pin_draft::{ResourcePinDraft, ResourcePinError},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct HashMapDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) pin: ResourcePinDraft,
    pub(super) codec: Option<SelectedReference<CodecName>>,
    pub(super) key_field: Option<SelectedReference<FieldName>>,
    pub(super) path: String,
}

impl HashMapDraft {
    pub(super) fn select_codec(&mut self, node: &NodeRef) {
        if node.kind != ModelKind::Codec {
            return;
        }
        let Ok(name) = CodecName::parse(node.identifier.as_str()) else {
            return;
        };
        if self.current_codec() != Some(&name)
            && let Some(key) = &mut self.key_field
        {
            key.invalidate();
        }
        self.codec = Some(SelectedReference::chosen(name));
    }

    pub(super) fn select_key(&mut self, field: FieldName) {
        self.key_field = Some(SelectedReference::chosen(field));
    }

    pub(super) fn current_codec(&self) -> Option<&CodecName> {
        self.codec
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    pub(super) fn selects_codec(&self, node: &NodeRef) -> bool {
        self.codec
            .as_ref()
            .is_some_and(|selection| selection.selects(ModelKind::Codec, node))
    }

    pub(super) fn invalidate_references(&mut self) {
        self.pin.invalidate();
        if let Some(codec) = &mut self.codec {
            codec.invalidate();
        }
        if let Some(key) = &mut self.key_field {
            key.invalidate();
        }
    }

    pub(super) fn build(
        &self,
    ) -> error_stack::Result<CreateLookup<RequestedResourceVersion>, HashMapDraftError> {
        let name = LookupName::parse(self.name.trim())
            .map_err(|_| Report::new(HashMapDraftError::Name))?;
        let pin = self.pin.build().map_err(|error| {
            let context = error.current_context().clone();
            error.change_context(HashMapDraftError::ResourcePin(context))
        })?;
        let codec = match &self.codec {
            None => return Err(Report::new(HashMapDraftError::CodecRequired)),
            Some(selected) => selected
                .current_name()
                .ok_or_else(|| Report::new(HashMapDraftError::CodecChanged))?
                .clone(),
        };
        let key_field = match &self.key_field {
            None => return Err(Report::new(HashMapDraftError::KeyRequired)),
            Some(selected) => selected
                .current_name()
                .ok_or_else(|| Report::new(HashMapDraftError::KeyChanged))?
                .clone(),
        };
        if self.path.trim().is_empty() {
            return Err(Report::new(HashMapDraftError::PathRequired));
        }
        Ok(CreateLookup {
            name,
            key_field,
            resource: pin.resource,
            resource_version: pin.version,
            path: self.path.clone(),
            decode_using_codec: codec,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum HashMapDraftError {
    #[error("Hash map name is invalid")]
    Name,
    #[error("{0}")]
    ResourcePin(ResourcePinError),
    #[error("Choose a codec in this domain")]
    CodecRequired,
    #[error("The selected codec belongs to a changed context; select it again")]
    CodecChanged,
    #[error("Choose a key field from the codec's output schema")]
    KeyRequired,
    #[error("The codec or domain changed; select the key field again")]
    KeyChanged,
    #[error("Enter the resource file path")]
    PathRequired,
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        CodecName, CreateStatement, FieldName, Model, ModelKind, NodeRef, RequestedResourceVersion,
        ResourceName, Statement,
    };
    use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};

    use super::{HashMapDraft, HashMapDraftError};

    #[test]
    fn lookup_uses_a_typed_codec_field_and_keeps_latest_until_application() {
        let mut draft = HashMapDraft {
            name: "by_id".to_string(),
            path: "lookup.jsonl".to_string(),
            ..HashMapDraft::default()
        };
        draft
            .pin
            .select_resource(ResourceName::parse("bundle").assured("valid resource"));
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(HashMapDraftError::ResourcePin(
                super::ResourcePinError::VersionRequired
            ))
        );
        draft.pin.select_version(RequestedResourceVersion::Latest);
        draft.select_codec(&NodeRef::new(
            ModelKind::Codec,
            CodecName::parse("entry_codec").assured("valid codec"),
        ));
        draft.select_key(FieldName::parse("id").assured("valid field"));
        let model = Model::Lookup(draft.build().assured("complete lookup"));
        let statement = Statement::Create(CreateStatement::new(Box::new(model), false));
        let source = statement.to_canonical_nspl().assured("lookup renders");
        assert!(source.contains("VERSION LATEST"));
        assert_eq!(
            parse_client_statement(&source).assured("lookup parses"),
            ClientStatement::Server(statement)
        );
        draft.select_codec(&NodeRef::new(
            ModelKind::Codec,
            CodecName::parse("other_codec").assured("valid codec"),
        ));
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(HashMapDraftError::KeyChanged)
        );
        draft.invalidate_references();
        assert!(draft.build().is_err());
    }
}
