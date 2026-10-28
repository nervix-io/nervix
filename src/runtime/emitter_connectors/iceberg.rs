//! Iceberg emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the Iceberg connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

const _: () = assert!(matches!(
    IcebergTableEmitter::KIND,
    nervix_models::EmitterEntityKind::Iceberg(nervix_models::IcebergEmitterEntity::Table)
));

impl EmitterConnectorStarter {
    pub(super) async fn iceberg(
        connector: &IcebergTableEmitterPlan,
        context: &EmitterConnectorContext,
        input_schema: &CompiledSchema,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = {
            let projection = Self::projection(MappedValuesProjectionInit {
                label: "Iceberg",
                namespace: "iceberg",
                emitter: &context.emitter,
                mapping: &connector.mapping,
                input_schema: input_schema.arrow_schema(),
                udfs: context.udfs.as_ref(),
            })?;
            let mapped_schema = projection.value_schema();
            let opened = IcebergTableEmitter::new(
                IcebergTableEmitterConfig {
                    backend: connector.backend,
                    dns: context.dns()?,
                    storage_config: connector.storage.config.entries.clone(),
                    catalog_name: connector.catalog.name.as_str().to_string(),
                    catalog_config: connector.catalog.config.entries.clone(),
                    namespace: connector.namespace.clone(),
                    table: connector.table.clone(),
                    location: connector.location.clone(),
                    mapped_schema,
                    batch: connector.batch,
                    commit: connector.commit,
                    writer: context.emitter.as_str().to_string(),
                },
                context.emitter_host(),
            )
            .await;
            Self::row(projection, opened)?
        };
        Ok(opened)
    }
}
