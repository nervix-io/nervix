//! Incomplete visual ingestor state and conversion to its current semantic Model.
//!
//! Layer: edges.
//!
//! - **Owns.** The visual ingestor's selected source, codec, timestamp, ordered routes, and
//!   required node error policy until the browser has a complete `CreateIngestor`.
//! - **Depends on.** Typed vocabulary, expression parsing, and source and route drafts.
//! - **Must not know.** Connector execution, registry validation, or command transport.

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    CodecName, CreateIngestor, FieldName, GeneralErrorPolicy, IngestTimestampSource, IngestorName,
    ModelKind, NodeRef, ProcessorOutputs,
};
use nervix_nspl::parse_expression;
use thiserror::Error;

use super::{
    SelectedReference,
    ingestor_route_draft::{IngestRouteDraft, IngestRouteDraftError, RouteBranchDraft},
    ingestor_source_draft::{IngestSourceDraft, IngestSourceDraftError},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum TimestampDraft {
    #[default]
    Unselected,
    Omit,
    Now,
    At(Option<SelectedReference<FieldName>>),
}

impl TimestampDraft {
    pub(super) fn select_field(&mut self, field: FieldName) {
        if let Self::At(selected) = self {
            *selected = Some(SelectedReference::chosen(field));
        }
    }

    fn invalidate(&mut self) {
        if let Self::At(Some(field)) = self {
            field.invalidate();
        }
    }

    fn build(&self) -> error_stack::Result<Option<IngestTimestampSource>, IngestorDraftError> {
        match self {
            Self::Unselected => Err(Report::new(IngestorDraftError::Timestamp)),
            Self::Omit => Ok(None),
            Self::Now => Ok(Some(IngestTimestampSource::Now)),
            Self::At(None) => Err(Report::new(IngestorDraftError::TimestampField)),
            Self::At(Some(field)) => Ok(Some(IngestTimestampSource::At(
                field
                    .current_name()
                    .ok_or_else(|| Report::new(IngestorDraftError::TimestampFieldChanged))?
                    .clone(),
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IngestorDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) source: IngestSourceDraft,
    pub(super) codec: Option<SelectedReference<CodecName>>,
    pub(super) timestamp: TimestampDraft,
    pub(super) filter: String,
    pub(super) routes: Vec<IngestRouteDraft>,
    pub(super) active_route: usize,
    pub(super) general_error: Option<GeneralErrorPolicy>,
}

impl Default for IngestorDraft {
    fn default() -> Self {
        Self {
            name: String::new(),
            if_not_exists: false,
            source: IngestSourceDraft::default(),
            codec: None,
            timestamp: TimestampDraft::Unselected,
            filter: String::new(),
            routes: vec![IngestRouteDraft::default()],
            active_route: 0,
            general_error: None,
        }
    }
}

impl IngestorDraft {
    pub(super) fn select_codec(&mut self, node: &NodeRef) {
        if node.kind != ModelKind::Codec {
            return;
        }
        let selected = CodecName::from(&node.identifier);
        if self.current_codec() != Some(&selected) {
            self.timestamp.invalidate();
            for route in &mut self.routes {
                route.inherit.invalidate();
            }
        }
        self.codec = Some(SelectedReference::chosen(selected));
    }

    pub(super) fn current_codec(&self) -> Option<&CodecName> {
        self.codec
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    pub(super) fn active_route(&self) -> Option<&IngestRouteDraft> {
        self.routes.get(self.active_route)
    }

    pub(super) fn active_route_mut(&mut self) -> Option<&mut IngestRouteDraft> {
        self.routes.get_mut(self.active_route)
    }

    pub(super) fn select_route(&mut self, index: usize) {
        if index < self.routes.len() {
            self.active_route = index;
        }
    }

    pub(super) fn add_route(&mut self) {
        self.routes.push(IngestRouteDraft::default());
        self.active_route = self.routes.len() - 1;
    }

    pub(super) fn move_route_up(&mut self) {
        if self.active_route > 0 {
            self.routes.swap(self.active_route, self.active_route - 1);
            self.active_route -= 1;
        }
    }

    pub(super) fn move_route_down(&mut self) {
        if self.active_route + 1 < self.routes.len() {
            self.routes.swap(self.active_route, self.active_route + 1);
            self.active_route += 1;
        }
    }

    pub(super) fn remove_route(&mut self) {
        if self.routes.len() <= 1 {
            return;
        }
        self.routes.remove(self.active_route);
        if self.active_route >= self.routes.len() {
            self.active_route = self.routes.len() - 1;
        }
    }

    pub(super) fn choose_route_unbranched(&mut self) {
        if let Some(route) = self.active_route_mut() {
            route.branch.choose_unbranched();
            if let Some(relay) = &mut route.relay {
                relay.invalidate();
            }
        }
    }

    pub(super) fn choose_route_branched(&mut self) {
        if let Some(route) = self.active_route_mut()
            && !matches!(route.branch, RouteBranchDraft::Branched { .. })
        {
            route.branch.choose_branched();
            if let Some(relay) = &mut route.relay {
                relay.invalidate();
            }
        }
    }

    pub(super) fn select_branch(&mut self, node: &NodeRef) {
        if let Some(route) = self.active_route_mut() {
            let current = route.branch.current_branch().cloned();
            route.branch.select_branch(node);
            if route.branch.current_branch() != current.as_ref()
                && let Some(relay) = &mut route.relay
            {
                relay.invalidate();
            }
        }
    }

    pub(super) fn invalidate_references(&mut self) {
        self.source.invalidate_references();
        if let Some(codec) = &mut self.codec {
            codec.invalidate();
        }
        self.timestamp.invalidate();
        for route in &mut self.routes {
            route.invalidate_references();
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<CreateIngestor, IngestorDraftError> {
        let name = IngestorName::parse(self.name.trim())
            .map_err(|_| Report::new(IngestorDraftError::Name))?;
        let source = self.source.build().map_err(|error| {
            let context = error.current_context().clone();
            error.change_context(IngestorDraftError::Source(context))
        })?;
        let decode_using_codec = match &self.codec {
            Some(codec) => codec
                .current_name()
                .ok_or_else(|| Report::new(IngestorDraftError::CodecChanged))?
                .clone(),
            None => return Err(Report::new(IngestorDraftError::Codec)),
        };
        let timestamp_source = self.timestamp.build()?;
        let filter_where = if self.filter.trim().is_empty() {
            None
        } else {
            Some(parse_expression(self.filter.trim()).change_context(IngestorDraftError::Filter)?)
        };
        if self.routes.is_empty() {
            return Err(Report::new(IngestorDraftError::Routes));
        }
        let mut routes = Vec::new();
        for (index, route) in self.routes.iter().enumerate() {
            routes.push(route.build().map_err(|error| {
                let context = error.current_context().clone();
                error.change_context(IngestorDraftError::Route {
                    index: index + 1,
                    cause: context,
                })
            })?);
        }
        let general_error_policy = self
            .general_error
            .clone()
            .ok_or_else(|| Report::new(IngestorDraftError::GeneralError))?;
        Ok(CreateIngestor {
            name,
            output_routes: ProcessorOutputs::new(routes),
            decode_using_codec,
            timestamp_source,
            source,
            general_error_policy,
            filter_where,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum IngestorDraftError {
    #[error("Ingestor name is invalid")]
    Name,
    #[error("{0}")]
    Source(IngestSourceDraftError),
    #[error("Choose a decoding codec")]
    Codec,
    #[error("The selected codec belongs to a changed context; select it again")]
    CodecChanged,
    #[error("Choose a timestamp policy")]
    Timestamp,
    #[error("Choose a decoded DATETIME field for TIMESTAMP AT")]
    TimestampField,
    #[error("The timestamp field belongs to a changed codec; select it again")]
    TimestampFieldChanged,
    #[error("Enter a valid source FILTER WHERE expression")]
    Filter,
    #[error("Add at least one output route")]
    Routes,
    #[error("Route {index}: {cause}")]
    Route {
        index: usize,
        cause: IngestRouteDraftError,
    },
    #[error("Choose ON GENERAL ERROR IGNORE or LOG")]
    GeneralError,
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        CodecName, FieldName, GeneralErrorPolicy, IngestQuiesceOverflow, IngestSourceKind,
        Inheritance, ModelKind, ModelName, NodeRef,
    };

    use super::{IngestorDraft, TimestampDraft};
    use crate::create_dialog::{
        SelectedReference,
        ingestor_route_draft::{FlushDraft, InheritDraft, MessageErrorDraft},
        ingestor_source_draft::QuiesceChoice,
    };

    fn node(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(kind, ModelName::parse(name).assured("valid model name"))
    }

    fn complete() -> IngestorDraft {
        let mut draft = IngestorDraft {
            name: "incoming".into(),
            ..Default::default()
        };
        draft.source.choose_kind(IngestSourceKind::Endpoint);
        draft
            .source
            .select_reference(&node(ModelKind::Endpoint, "edge"));
        draft.source.quiesce = Some(QuiesceChoice::Buffer);
        draft.source.buffer_size = "1MiB".into();
        draft.source.overflow = Some(IngestQuiesceOverflow::DropOldest);
        draft.codec = Some(SelectedReference::chosen(
            CodecName::parse("decode").assured("valid codec"),
        ));
        draft.timestamp = TimestampDraft::Now;
        draft.routes[0].select_relay(&node(ModelKind::Relay, "first"));
        draft.routes[0].inherit = InheritDraft::All;
        draft.routes[0].branch.choose_unbranched();
        draft.routes[0].flush = FlushDraft::Immediate;
        draft.routes[0].message_error = MessageErrorDraft::Log;
        draft.general_error = Some(GeneralErrorPolicy::Log);
        draft
    }

    #[test]
    fn routes_keep_written_order_and_field_leak_choices() {
        let mut draft = complete();
        draft.add_route();
        let route = draft.active_route_mut().assured("new route is active");
        route.select_relay(&node(ModelKind::Relay, "second"));
        route.branch.choose_unbranched();
        route.inherit.choose("fields");
        let field = FieldName::parse("secret").assured("valid field");
        route.inherit.add_field(field);
        if let InheritDraft::Fields(fields) = &mut route.inherit {
            fields[0].leak_sensitive = true;
        }
        route.flush = FlushDraft::Immediate;
        route.message_error = MessageErrorDraft::Ignore;

        assert_eq!(
            draft
                .build()
                .assured("both routes build")
                .output_routes
                .routes[1]
                .relay
                .as_str(),
            "second"
        );
        draft.move_route_up();
        let built = draft.build().assured("reordered routes build");
        assert_eq!(built.output_routes.routes[0].relay.as_str(), "second");
        assert!(matches!(built.output_routes.routes[0].construction.inherit,
            Some(Inheritance::Fields(ref fields)) if fields.len() == 1 && fields[0].leak_sensitive));
        draft.remove_route();
        assert_eq!(
            draft
                .build()
                .assured("remaining route builds")
                .output_routes
                .routes
                .len(),
            1
        );
    }

    #[test]
    fn selecting_and_moving_routes_keeps_their_visible_order() {
        let mut draft = complete();
        let template = draft.routes[0].clone();
        for name in ["second", "third"] {
            draft.add_route();
            let route = draft.active_route_mut().assured("added route is active");
            *route = template.clone();
            route.select_relay(&node(ModelKind::Relay, name));
        }
        assert_eq!(draft.active_route, 2);
        draft.select_route(1);
        draft.move_route_down();
        assert_eq!(draft.active_route, 2);
        assert_eq!(
            draft.routes[2]
                .current_relay()
                .assured("relay selected")
                .as_str(),
            "second"
        );
        draft.move_route_up();
        assert_eq!(draft.active_route, 1);
        assert_eq!(
            draft.routes[1]
                .current_relay()
                .assured("relay selected")
                .as_str(),
            "second"
        );
        draft.select_route(99);
        assert_eq!(draft.active_route, 1);
        draft.remove_route();
        assert_eq!(draft.active_route, 1);
        let relays = draft
            .build()
            .assured("remaining routes build")
            .output_routes
            .routes
            .into_iter()
            .map(|route| route.relay.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(relays, ["first", "third"]);
    }

    #[test]
    fn changed_domain_or_codec_blocks_old_field_and_route_choices() {
        let mut draft = complete();
        draft.timestamp = TimestampDraft::At(Some(SelectedReference::chosen(
            FieldName::parse("when").assured("valid field"),
        )));
        draft.select_codec(&node(ModelKind::Codec, "other_codec"));
        assert!(draft.build().is_err());
        draft.timestamp = TimestampDraft::Now;
        assert!(draft.build().is_ok());
        draft.invalidate_references();
        assert!(draft.build().is_err());
    }
}
