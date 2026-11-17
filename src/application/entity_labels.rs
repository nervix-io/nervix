//! Session command adaptation of committed and transaction candidate entity-label inspections.
//!
//! Layer: edges.
//! - **Owns.** Selecting the requested metadata view and rendering its session command outcome.
//! - **Depends on.** The session adapter, applied consensus captures and the pure transaction planner.
//! - **Must not know.** Session framing, codec internals or runtime task lifecycle.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        edge,
        reason = "adapts typed metadata inspections to session command outcomes"
    )
)]

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_consensus::{EntityLabelReadError, TransactionState};
use nervix_models::{
    DomainName, DomainNodeRef, EntityLabelView, EntityLabelsInspection, InspectionFormat,
    ShowLabels, TransactionPreviewIdentity,
};
use thiserror::Error;

use super::{
    command_result::CommandResult,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
    transaction::InspectingSession,
};

#[derive(Debug, Error)]
enum LabelInspectionError {
    #[error("cannot read entity metadata from the applied publication")]
    Read,
    #[error("the attached transaction has no open candidate owned by this session")]
    TransactionNotOpen,
    #[error("cannot inspect the transaction's current candidate")]
    Planning,
}

impl SessionServiceImpl {
    pub(in crate::application) async fn show_entity_labels(
        &self,
        domain: &DomainName,
        show: ShowLabels,
        session: InspectingSession<'_>,
    ) -> CommandResult {
        match self.inspect_entity_labels(domain, &show, session).await {
            Ok(inspection) => {
                let message = render(&inspection, show.format);
                CommandResult {
                    entity_labels: Some(Box::new(inspection)),
                    ..command_ok(message)
                }
            }
            Err(error) => command_error(format!("{error:#}")),
        }
    }

    async fn inspect_entity_labels(
        &self,
        domain: &DomainName,
        show: &ShowLabels,
        session: InspectingSession<'_>,
    ) -> Result<EntityLabelsInspection, Report<LabelInspectionError>> {
        let entity = DomainNodeRef::new(domain.clone(), show.entity.clone());
        let Some(id) = session.attached_transaction else {
            return self
                .inner
                .consensus
                .entity_labels(&entity)
                .change_context(LabelInspectionError::Read);
        };
        let snapshot = self
            .inner
            .consensus
            .entity_label_transaction(domain, id)
            .change_context(LabelInspectionError::Read)?;
        if snapshot.transaction.owner != *session.user
            || !matches!(snapshot.transaction.state, TransactionState::Open(_))
        {
            return Err(Report::new(LabelInspectionError::TransactionNotOpen));
        }
        let statements = snapshot
            .transaction
            .statements
            .iter()
            .map(|queued| queued.statement.clone())
            .collect::<Vec<_>>();
        let references = snapshot
            .transaction
            .statements
            .iter()
            .map(|queued| queued.request_reference.clone())
            .collect::<Vec<_>>();
        let (captured, labels) = self
            .plan_transaction_statements_against(
                &statements,
                &references,
                0,
                true,
                snapshot.control,
                |models| models.get(&show.entity).map(|model| model.labels().clone()),
            )
            .await
            .change_context(LabelInspectionError::Planning)?;
        let labels = labels.ok_or_else(|| {
            Report::new(EntityLabelReadError::UnknownEntity {
                entity: entity.clone(),
            })
            .change_context(LabelInspectionError::Read)
        })?;
        let report = captured
            .plan
            .report()
            .change_context(LabelInspectionError::Planning)?;
        let preview = TransactionPreviewIdentity {
            transaction_id: id.to_owned(),
            position: report.position(),
            planning_basis: report.planning_basis(),
        };
        EntityLabelsInspection::new(
            entity,
            labels,
            EntityLabelView::Staged { preview },
            snapshot.policy,
        )
        .change_context(LabelInspectionError::Planning)
    }
}

fn render(inspection: &EntityLabelsInspection, format: InspectionFormat) -> String {
    if format == InspectionFormat::Json {
        return serde_json::to_string(inspection).assured(
            "metadata inspection contains only typed identities, integers and exact string maps",
        );
    }
    let entity = inspection.entity();
    let mut text = format!(
        "{} {} in domain {}\n",
        entity.node.kind.keyword_phrase(),
        entity.node.identifier,
        entity.domain
    );
    match inspection.view() {
        EntityLabelView::Committed { revision } => {
            text.push_str(&format!("Committed revision: {revision}\n"))
        }
        EntityLabelView::Staged { preview } => text.push_str(&format!(
            "Transaction: {}\nPosition: {}\nPlanning basis: {}\n",
            preview.transaction_id,
            preview.position.accepted_operations(),
            preview.planning_basis
        )),
    }
    text.push_str(&format!(
        "Policy revision: {}\n",
        inspection.policy().revision
    ));
    let limits = inspection.policy().limits;
    text.push_str(&format!(
        "Limits: count={} key_bytes={} value_bytes={} bytes={}\n",
        limits.count, limits.key_bytes, limits.value_bytes, limits.bytes
    ));
    for (key, value) in inspection.labels().iter() {
        let key = serde_json::to_string(key.as_str()).assured("a label key is a string");
        let value = serde_json::to_string(value).assured("a label value is a string");
        text.push_str(&format!("{key} = {value}\n"));
    }
    text
}
