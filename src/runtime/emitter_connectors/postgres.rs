//! Postgres emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the Postgres connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

const _: () = assert!(matches!(
    PostgresTableEmitter::KIND,
    nervix_models::EmitterEntityKind::Postgres(nervix_models::PostgresEmitterEntity::Table)
));

impl EmitterConnectorStarter {
    pub(super) async fn postgres(
        connector: &PostgresTableEmitterPlan,
        context: &EmitterConnectorContext,
        input_schema: &CompiledSchema,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = {
            let projection = Self::projection(MappedValuesProjectionInit {
                label: "Postgres",
                namespace: "postgres",
                emitter: &context.emitter,
                mapping: &connector.mapping,
                input_schema: input_schema.arrow_schema(),
                udfs: context.udfs.as_ref(),
            })?;
            let connections =
                PooledEmitterClient::lease(context, &connector.client, connector.pooled_client())
                    .await
                    .change_context(EmitterRuntimeError::InitializeConnector)?;
            Self::row(
                projection,
                PostgresTableEmitter::new(
                    PostgresTableEmitterConfig {
                        table: connector.table.clone(),
                        conflict_action: connector.conflict_action.clone(),
                        batch: connector.batch,
                    },
                    Box::new(connections),
                    context.emitter_host(),
                ),
            )?
        };
        Ok(opened)
    }
}
