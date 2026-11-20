//! The visual editor for an existing endpoint ingestor's pause policy.
//!
//! Layer: edges.
//! - **Owns.** Loading the selected ingestor's settings and producing its single quiesce alteration.
//! - **Depends on.** The shared ingestor editor, public Plexus and the durable command form.
//! - **Must not know.** Endpoint admission, quiesce timing or connector tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the visual endpoint pause setting")
)]

use error_stack::{Report, ResultExt as _};
use leptos::prelude::*;
use nervix_models::{
    AlterIngestor, AlterIngestorOperation, AlterTransportIngestorOperation, DomainName,
    HttpIngestorEntity, HttpRequestIngestorAlteration, IngestQuiesceMode, IngestorAlteration,
    IngestorEntityKind, IngestorName, Model, Statement, WebsocketsIngestorAlteration,
    WebsocketsIngestorEntity,
};
use nervix_plexus::client_statement::{ClientStatement, parse_client_statement};

use super::{
    CommandDispatch, CreateDispatch, CreateDraftError, CreateKind, CreateSignals, CreateSubmission,
    ingestor_editor::{IngestorQuiesceEditor, QuiesceEdit},
    ingestor_source_draft::IngestorEntityDraft,
};

#[derive(Clone)]
pub(super) struct EndpointPauseDraft {
    name: IngestorName,
    settings: EndpointPauseSettings,
}

#[derive(Clone)]
enum EndpointPauseSettings {
    Loading,
    Ready(Box<IngestorEntityDraft>),
    Failed(String),
}

/// The reply to the console's request for the selected ingestor's configuration.
pub(crate) enum EndpointPauseSettingsResponse {
    Configuration(String),
    Refused(String),
}

#[derive(Debug, thiserror::Error)]
enum ReadPauseSettingsError {
    #[error("The server did not return the ingestor's configuration")]
    UnreadableConfiguration,
    #[error("The server returned a different kind of configuration")]
    ExpectedIngestor,
    #[error("The selected ingestor does not read an endpoint")]
    ExpectedEndpoint,
    #[error("The server returned another ingestor's configuration")]
    WrongIngestor,
    #[error("The endpoint has an unsupported pause setting")]
    InvalidPause,
}

impl EndpointPauseDraft {
    pub(super) fn loading(name: IngestorName) -> Self {
        Self {
            name,
            settings: EndpointPauseSettings::Loading,
        }
    }

    pub(super) fn loaded(&mut self, response: EndpointPauseSettingsResponse) {
        self.settings = match response {
            EndpointPauseSettingsResponse::Configuration(public_plexus) => {
                match self.read_settings(&public_plexus) {
                    Ok(source) => EndpointPauseSettings::Ready(Box::new(source)),
                    Err(error) => {
                        EndpointPauseSettings::Failed(error.current_context().to_string())
                    }
                }
            }
            EndpointPauseSettingsResponse::Refused(error) => EndpointPauseSettings::Failed(error),
        };
    }

    fn read_settings(
        &self,
        public_plexus: &str,
    ) -> error_stack::Result<IngestorEntityDraft, ReadPauseSettingsError> {
        let Ok(ClientStatement::Server(Statement::Create(create))) =
            parse_client_statement(public_plexus)
        else {
            return Err(Report::new(ReadPauseSettingsError::UnreadableConfiguration));
        };
        let Model::Ingestor(ingestor) = create.body.as_ref() else {
            return Err(Report::new(ReadPauseSettingsError::ExpectedIngestor));
        };
        let Some(entity) = ingestor.input.transport_entity().filter(|entity| {
            entity
                .kind()
                .capabilities()
                .reference
                .endpoint_type()
                .is_some()
        }) else {
            return Err(Report::new(ReadPauseSettingsError::ExpectedEndpoint));
        };
        if ingestor.name != self.name {
            return Err(Report::new(ReadPauseSettingsError::WrongIngestor));
        }
        let mut source = IngestorEntityDraft {
            kind: Some(entity.kind()),
            delivery: Some(entity.acknowledgement().kind()),
            quiesce: Some(entity.quiesce().kind()),
            ..IngestorEntityDraft::default()
        };
        match entity.quiesce() {
            IngestQuiesceMode::EndpointBuffer { max_size, .. } => {
                source.buffer_size.clone_from(max_size);
            }
            IngestQuiesceMode::Reject { retry_after } => {
                source.retry_after.clone_from(retry_after);
            }
            _ => return Err(Report::new(ReadPauseSettingsError::InvalidPause)),
        }
        Ok(source)
    }

    pub(super) fn ready(&self) -> bool {
        matches!(self.settings, EndpointPauseSettings::Ready(_))
    }

    pub(super) fn submission(
        &self,
        scope: DomainName,
    ) -> error_stack::Result<CreateSubmission, CreateDraftError> {
        let EndpointPauseSettings::Ready(source) = &self.settings else {
            return Err(Report::new(CreateDraftError::EndpointPauseUnavailable));
        };
        let quiesce = source.build_quiesce().map_err(super::draft_error)?;
        let alteration = match source.kind {
            Some(IngestorEntityKind::Http(HttpIngestorEntity::Endpoint)) => {
                IngestorAlteration::Http(HttpRequestIngestorAlteration::Endpoint(vec![
                    AlterTransportIngestorOperation::Ingestor(Box::new(
                        AlterIngestorOperation::SetQuiesce { quiesce },
                    )),
                ]))
            }
            Some(IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint)) => {
                IngestorAlteration::Websockets(WebsocketsIngestorAlteration::Endpoint(vec![
                    AlterTransportIngestorOperation::Ingestor(Box::new(
                        AlterIngestorOperation::SetQuiesce { quiesce },
                    )),
                ]))
            }
            _ => return Err(Report::new(CreateDraftError::EndpointPauseUnavailable)),
        };
        let query = Statement::AlterIngestor(AlterIngestor {
            ingestor: self.name.clone(),
            alteration,
        })
        .to_canonical_plexus()
        .change_context(CreateDraftError::CanonicalPlexus)?;
        Ok(CreateSubmission {
            kind: CreateKind::EndpointPause,
            presentation: query.clone(),
            dispatch: CreateDispatch::Command(CommandDispatch {
                query,
                domain: Some(scope),
                resource: None,
                created_domain: None,
            }),
        })
    }
}

#[component]
pub(super) fn EndpointPauseEditor(signals: CreateSignals) -> impl IntoView {
    let source = Signal::derive(move || {
        signals.endpoint_pause.with(|draft| match draft {
            Some(EndpointPauseDraft {
                settings: EndpointPauseSettings::Ready(source),
                ..
            }) => source.as_ref().clone(),
            _ => IngestorEntityDraft::default(),
        })
    });
    let load_error = move || {
        signals.endpoint_pause.with(|draft| match draft {
            Some(EndpointPauseDraft {
                settings: EndpointPauseSettings::Failed(error),
                ..
            }) => Some(error.clone()),
            _ => None,
        })
    };
    view! {
        <p class="endpoint-pause-ingestor">
            {move || signals.endpoint_pause.with(|draft| {
                draft.as_ref().map(|draft| draft.name.to_string()).unwrap_or_default()
            })}
        </p>
        <Show when=move || signals.endpoint_pause.with(|draft| matches!(draft, Some(EndpointPauseDraft { settings: EndpointPauseSettings::Loading, .. }))) fallback=|| ()>
            <p class="endpoint-pause-loading" aria-live="polite">"Loading pause settings…"</p>
        </Show>
        <Show when=move || load_error().is_some() fallback=|| ()>
            <p class="endpoint-pause-load-error" role="alert">{move || load_error().unwrap_or_default()}</p>
        </Show>
        <IngestorQuiesceEditor source=source
            pending=Signal::derive(move || signals.progress.get().is_pending())
            change=Callback::new(move |edit: QuiesceEdit| {
                signals.endpoint_pause.update(|draft| {
                    if let Some(EndpointPauseDraft { settings: EndpointPauseSettings::Ready(source), .. }) = draft {
                        edit.apply(source);
                    }
                });
                signals.edit();
            })
        />
    }
    .into_any()
}
