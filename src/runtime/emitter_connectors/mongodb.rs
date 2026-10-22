//! MongoDb emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the MongoDb connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

impl EmitterConnectorStarter {
    pub(super) async fn mongodb(
        connector: &MongoDbEmitterPlan,
        context: &EmitterConnectorContext,
        input_schema: &CompiledSchema,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = {
            let projection = Self::projection(MappedValuesProjectionInit {
                label: "MongoDB",
                namespace: "mongodb",
                emitter: &context.emitter,
                mapping: &connector.mapping,
                input_schema: input_schema.arrow_schema(),
                udfs: context.udfs.as_ref(),
            })?;
            let client =
                PooledEmitterClient::lease(context, &connector.client, connector.pooled_client())
                    .await
                    .map_err(emitter_init_error)?;
            Self::row(
                projection,
                MongoDbEmitter::new(
                    MongoDbEmitterConfig {
                        config: connector.client.config.entries.clone(),
                        collection: connector.collection.clone(),
                        conflict_action: connector.conflict_action.clone(),
                        batch: connector.batch,
                    },
                    Box::new(client),
                    context.emitter_host(),
                ),
            )?
        };
        Ok(opened)
    }
}
