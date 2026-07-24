use ::mongodb::{
    Client as MongoDbClient,
    bson::{
        Bson as MongoDbBson, Document as MongoDbDocument, doc as mongodb_doc,
        to_bson as mongodb_to_bson,
    },
    options::{
        ClientOptions as MongoDbClientOptions, Tls as MongoDbTls, TlsOptions as MongoDbTlsOptions,
    },
};

use super::*;

pub(in crate::runtime) struct MongoDbEmitter {
    client: Option<MongoDbEmitterClient>,
    program: Option<CompiledSqlValuesProgram>,
}

struct MongoDbEmitterClient {
    client: MongoDbClient,
    database: String,
}

impl MongoDbEmitter {
    pub(in crate::runtime) async fn new(
        client: &nervix_models::CreateClientMongoDb,
        resolved: Option<&ResolvedClientConfig>,
        context: &EmitterSinkContext,
        values: &[MongoDbValueMapping],
        input_schema: StdArc<arrow_schema::Schema>,
    ) -> Self {
        let client = match Self::client_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        )
        .await
        {
            Ok(client) => Some(client),
            Err(error) => {
                context.report_init_error("mongodb", &emitter_error_message(&error));
                None
            }
        };
        let program = match compile_mongodb_values_program(
            &context.domain,
            &context.emitter,
            values,
            input_schema,
            context.udfs.as_ref(),
        ) {
            Ok(program) => Some(program),
            Err(error) => {
                let _ = context.events.send(RuntimeEvent::Error(error.to_string()));
                warn!(
                    domain = context.domain.as_str(),
                    emitter = context.emitter.as_str(),
                    error = %error,
                    "failed to compile mongodb emitter values"
                );
                None
            }
        };
        Self { client, program }
    }

    async fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<MongoDbEmitterClient> {
        let addr = emitter_config_value(config, "addr", || {
            "missing MongoDB client config key 'addr'".to_string()
        })?;
        let mut options = MongoDbClientOptions::parse(&addr).await.map_err(|source| {
            emitter_config_error(format!("failed to parse MongoDB client addr: {source}"))
        })?;
        if let Some(ca_file) = optional_client_config_value(config, "tls_ca_file") {
            options.tls = Some(MongoDbTls::Enabled(
                MongoDbTlsOptions::builder()
                    .ca_file_path(PathBuf::from(ca_file))
                    .build(),
            ));
        }
        let database = optional_client_config_value(config, "database")
            .map(ToOwned::to_owned)
            .or_else(|| options.default_database.clone())
            .ok_or_else(|| emitter_config_error("missing MongoDB client config key 'database'"))?;
        let client = MongoDbClient::with_options(options).map_err(|source| {
            emitter_init_error(format!("failed to build MongoDB client: {source}"))
        })?;
        client
            .database("admin")
            .run_command(mongodb_doc! { "ping": 1 })
            .await
            .map_err(|source| {
                emitter_init_error(format!("failed to validate MongoDB connection: {source}"))
            })?;
        Ok(MongoDbEmitterClient { client, database })
    }

    fn value(value: &serde_json::Value) -> MongoDbBson {
        mongodb_to_bson(value).unwrap_or(MongoDbBson::Null)
    }

    async fn publish_documents(
        client: &MongoDbEmitterClient,
        collection: &Identifier,
        mappings: &[MongoDbValueMapping],
        conflict_action: &MongoDbConflictAction,
        rows: &[Vec<serde_json::Value>],
    ) -> EmitterRuntimeResult<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut documents = Vec::with_capacity(rows.len());
        for row in rows {
            documents.push(Self::document_from_row(mappings, row)?);
        }
        let collection = client
            .client
            .database(&client.database)
            .collection::<MongoDbDocument>(collection.as_str());
        match conflict_action {
            MongoDbConflictAction::None => {
                let result = collection.insert_many(documents).await.map_err(|source| {
                    Report::new(EmitterRuntimeError::PublishBatch)
                        .attach_printable(format!("failed to publish MongoDB documents: {source}"))
                })?;
                Ok(u64::try_from(result.inserted_ids.len()).unwrap_or(u64::MAX))
            }
            MongoDbConflictAction::DoNothing { target } => {
                let mut published = 0_u64;
                for document in documents {
                    let filter = Self::conflict_filter(&document, target)?;
                    let update = mongodb_doc! { "$setOnInsert": document };
                    let result = collection
                        .update_one(filter, update)
                        .upsert(true)
                        .await
                        .map_err(|source| {
                            Report::new(EmitterRuntimeError::PublishBatch).attach_printable(
                                format!(
                                    "failed to upsert MongoDB document with DO NOTHING: {source}"
                                ),
                            )
                        })?;
                    if result.upserted_id.is_some() {
                        published = published.saturating_add(1);
                    }
                }
                Ok(published)
            }
            MongoDbConflictAction::DoUpdate { target } => {
                let mut published = 0_u64;
                for document in documents {
                    let filter = Self::conflict_filter(&document, target)?;
                    let target_fields = target
                        .iter()
                        .map(String::as_str)
                        .collect::<std::collections::BTreeSet<_>>();
                    let mut set = MongoDbDocument::new();
                    let mut set_on_insert = MongoDbDocument::new();
                    for (key, value) in document {
                        if target_fields.contains(key.as_str()) {
                            set_on_insert.insert(key, value);
                        } else {
                            set.insert(key, value);
                        }
                    }
                    if set.is_empty() {
                        return Err(Report::new(EmitterRuntimeError::EncodeBatch)
                            .attach_printable(
                                "MongoDB ON CONFLICT DO UPDATE requires at least one non-conflict \
                                 VALUES field to update",
                            ));
                    }
                    let update = mongodb_doc! {
                        "$set": set,
                        "$setOnInsert": set_on_insert,
                    };
                    let result = collection
                        .update_one(filter, update)
                        .upsert(true)
                        .await
                        .map_err(|source| {
                            Report::new(EmitterRuntimeError::PublishBatch).attach_printable(
                                format!(
                                    "failed to upsert MongoDB document with DO UPDATE: {source}"
                                ),
                            )
                        })?;
                    let affected = result
                        .matched_count
                        .saturating_add(result.modified_count)
                        .saturating_add(u64::from(result.upserted_id.is_some()));
                    published = published.saturating_add(affected.max(1));
                }
                Ok(published)
            }
        }
    }

    fn document_from_row(
        mappings: &[MongoDbValueMapping],
        row: &[serde_json::Value],
    ) -> EmitterRuntimeResult<MongoDbDocument> {
        if row.len() != mappings.len() {
            return Err(
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                    "MongoDB VALUES produced {} fields for {} mappings",
                    row.len(),
                    mappings.len()
                )),
            );
        }
        let mut document = MongoDbDocument::new();
        for (mapping, value) in mappings.iter().zip(row.iter()) {
            document.insert(mapping.column.clone(), Self::value(value));
        }
        Ok(document)
    }

    fn conflict_filter(
        document: &MongoDbDocument,
        target: &[String],
    ) -> EmitterRuntimeResult<MongoDbDocument> {
        let mut filter = MongoDbDocument::new();
        for field in target {
            let Some(value) = document.get(field) else {
                return Err(
                    Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                        "MongoDB ON CONFLICT target field '{field}' is missing from VALUES \
                         document"
                    )),
                );
            };
            filter.insert(field.clone(), value.clone());
        }
        Ok(filter)
    }

    pub(super) async fn publish_batch(
        &self,
        collection: &Identifier,
        values: &[MongoDbValueMapping],
        conflict_action: &MongoDbConflictAction,
        batch: &RelayRecordBatch,
    ) -> EmitterRuntimeResult<()> {
        let (Some(client), Some(program)) = (self.client.as_ref(), self.program.as_ref()) else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized mongodb sink client"));
        };
        let rows = sql_mapped_batch_values(program, values, batch, current_timestamp()).await?;
        Self::publish_documents(client, collection, values, conflict_action, &rows).await?;
        trace!(
            collection = collection.as_str(),
            rows = rows.len(),
            "emitter published mongodb documents"
        );
        Ok(())
    }
}
