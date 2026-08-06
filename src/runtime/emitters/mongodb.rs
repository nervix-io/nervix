use ::mongodb::{
    Client as MongoDbClient, Namespace as MongoDbNamespace,
    bson::{
        Bson as MongoDbBson, Document as MongoDbDocument, doc as mongodb_doc,
        to_bson as mongodb_to_bson,
    },
    error::{Error as MongoDbError, ErrorKind as MongoDbErrorKind, PartialBulkWriteResult},
    options::{
        ClientOptions as MongoDbClientOptions, Tls as MongoDbTls, TlsOptions as MongoDbTlsOptions,
        UpdateOneModel as MongoDbUpdateOneModel, WriteModel as MongoDbWriteModel,
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
    fn is_record_write_error(code: i32) -> bool {
        matches!(code, 121 | 10334 | 11000)
    }

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

    fn conflict_update_model(
        namespace: &MongoDbNamespace,
        document: MongoDbDocument,
        conflict_action: &MongoDbConflictAction,
    ) -> EmitterRuntimeResult<MongoDbWriteModel> {
        let (filter, update) = match conflict_action {
            MongoDbConflictAction::None => {
                return Err(Report::new(EmitterRuntimeError::EncodeBatch)
                    .attach_printable("MongoDB bulk update requires an ON CONFLICT action"));
            }
            MongoDbConflictAction::DoNothing { target } => {
                let filter = Self::conflict_filter(&document, target)?;
                (filter, mongodb_doc! { "$setOnInsert": document })
            }
            MongoDbConflictAction::DoUpdate { target } => {
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
                    return Err(
                        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(
                            "MongoDB ON CONFLICT DO UPDATE requires at least one non-conflict \
                             VALUES field to update",
                        ),
                    );
                }
                (
                    filter,
                    mongodb_doc! {
                        "$set": set,
                        "$setOnInsert": set_on_insert,
                    },
                )
            }
        };
        Ok(MongoDbUpdateOneModel::builder()
            .namespace(namespace.clone())
            .filter(filter)
            .update(update)
            .upsert(true)
            .build()
            .into())
    }

    fn safe_infrastructure_error(operation: &str) -> Report<EmitterRuntimeError> {
        Report::new(EmitterRuntimeError::PublishBatch)
            .attach_printable(format!("MongoDB {operation} failed"))
    }

    fn apply_insert_many_error(
        batch_index: usize,
        chunk: &[usize],
        error: MongoDbError,
        outcome: &mut PerRecordPublishOutcome,
    ) {
        let MongoDbErrorKind::InsertMany(insert_error) = error.kind.as_ref() else {
            outcome.fail(Self::safe_infrastructure_error("insert_many request"));
            return;
        };
        if insert_error.write_concern_error.is_some() {
            outcome.fail(Self::safe_infrastructure_error("insert_many write concern"));
            return;
        }
        let Some(write_errors) = insert_error.write_errors.as_ref() else {
            outcome.fail(Self::safe_infrastructure_error("insert_many request"));
            return;
        };
        let errors = write_errors
            .iter()
            .map(|error| (error.index, error.code))
            .collect::<Vec<_>>();
        Self::apply_insert_many_write_errors(batch_index, chunk, &errors, outcome);
    }

    fn apply_insert_many_write_errors(
        batch_index: usize,
        chunk: &[usize],
        write_errors: &[(usize, i32)],
        outcome: &mut PerRecordPublishOutcome,
    ) {
        let errors = write_errors.iter().copied().collect::<HashMap<_, _>>();
        let mut has_infrastructure_error = errors.len() != write_errors.len()
            || write_errors
                .iter()
                .any(|(index, code)| *index >= chunk.len() || !Self::is_record_write_error(*code));
        for (local_index, row) in chunk.iter().enumerate() {
            if let Some(code) = errors.get(&local_index) {
                if Self::is_record_write_error(*code) {
                    outcome.reject(
                        (batch_index, *row),
                        format!("MongoDB rejected document with code {code}"),
                    );
                } else {
                    has_infrastructure_error = true;
                }
            } else {
                outcome.deliver((batch_index, *row));
            }
        }
        if has_infrastructure_error {
            outcome.fail(Self::safe_infrastructure_error("insert_many request"));
        }
    }

    fn apply_bulk_write_error(
        batch_index: usize,
        chunk: &[usize],
        error: MongoDbError,
        outcome: &mut PerRecordPublishOutcome,
    ) {
        let MongoDbErrorKind::BulkWrite(bulk_error) = error.kind.as_ref() else {
            outcome.fail(Self::safe_infrastructure_error("bulk write request"));
            return;
        };
        if !bulk_error.write_concern_errors.is_empty() {
            outcome.fail(Self::safe_infrastructure_error("bulk write concern"));
            return;
        }
        let mut accounted = vec![false; chunk.len()];
        if let Some(PartialBulkWriteResult::Verbose(result)) = bulk_error.partial_result.as_ref() {
            for local_index in result
                .insert_results
                .keys()
                .chain(result.update_results.keys())
                .chain(result.delete_results.keys())
            {
                if let Some(row) = chunk.get(*local_index) {
                    outcome.deliver((batch_index, *row));
                    accounted[*local_index] = true;
                }
            }
        }
        let mut has_infrastructure_error = false;
        for (local_index, error) in &bulk_error.write_errors {
            let Some(row) = chunk.get(*local_index) else {
                has_infrastructure_error = true;
                continue;
            };
            if Self::is_record_write_error(error.code) {
                outcome.reject(
                    (batch_index, *row),
                    format!("MongoDB rejected document with code {}", error.code),
                );
                accounted[*local_index] = true;
            } else {
                has_infrastructure_error = true;
            }
        }
        if has_infrastructure_error || accounted.iter().any(|accounted| !accounted) {
            outcome.fail(Self::safe_infrastructure_error("bulk write request"));
        }
    }

    pub(super) async fn publish_pending_chunks(
        &self,
        batch_index: usize,
        collection: &Identifier,
        values: &[MongoDbValueMapping],
        conflict_action: &MongoDbConflictAction,
        batch: &RelayRecordBatch,
        pending_chunks: &[Vec<usize>],
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        if pending_chunks.is_empty() {
            return outcome;
        }
        let (Some(client), Some(program)) = (self.client.as_ref(), self.program.as_ref()) else {
            outcome.fail(
                Report::new(EmitterRuntimeError::SinkNotInitialized)
                    .attach_printable("no initialized mongodb sink client"),
            );
            return outcome;
        };
        let rows = match sql_mapped_batch_values(program, values, batch, current_timestamp()).await
        {
            Ok(rows) => rows,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        let mut documents = Vec::with_capacity(rows.len());
        for row in rows {
            match row {
                Ok(row) => match Self::document_from_row(values, &row) {
                    Ok(document) => documents.push(Ok(document)),
                    Err(error) => {
                        outcome.fail(error);
                        return outcome;
                    }
                },
                Err(error) => documents.push(Err(error)),
            }
        }
        let pending_chunks = match outcome.filter_mapped_chunks(
            batch_index,
            &documents,
            pending_chunks,
            "mongodb",
        ) {
            Ok(pending_chunks) => pending_chunks,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        if pending_chunks.is_empty() {
            return outcome;
        }
        let database = client.client.database(&client.database);
        let request_acks = batch.merged_acks();
        let collection_names = match await_emitter_confirmation(&request_acks, async {
            database
                .list_collection_names()
                .filter(mongodb_doc! { "name": collection.as_str() })
                .authorized_collections(true)
                .await
        })
        .await
        {
            Ok(collection_names) => collection_names,
            Err(_) => {
                outcome.fail(Self::safe_infrastructure_error("collection lookup request"));
                return outcome;
            }
        };
        if collection_names.is_empty() {
            outcome.fail(
                Report::new(EmitterRuntimeError::PublishBatch)
                    .attach_printable("MongoDB target collection is not provisioned"),
            );
            return outcome;
        }
        let mongodb_collection = database.collection::<MongoDbDocument>(collection.as_str());
        let namespace = MongoDbNamespace::new(&client.database, collection.as_str());
        for chunk in &pending_chunks {
            tokio::task::consume_budget().await;
            let chunk_documents = match chunk
                .iter()
                .map(|row| {
                    documents
                        .get(*row)
                        .and_then(|document| document.as_ref().ok())
                        .cloned()
                        .ok_or_else(|| {
                            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                                "mongodb pending row {row} has no mapped document in batch with \
                                 {} rows",
                                documents.len()
                            ))
                        })
                })
                .collect::<EmitterRuntimeResult<Vec<_>>>()
            {
                Ok(documents) => documents,
                Err(error) => {
                    outcome.fail(error);
                    return outcome;
                }
            };
            match conflict_action {
                MongoDbConflictAction::None => {
                    match await_emitter_confirmation(&request_acks, async {
                        mongodb_collection
                            .insert_many(chunk_documents)
                            .ordered(false)
                            .await
                    })
                    .await
                    {
                        Ok(_) => {
                            for row in chunk {
                                outcome.deliver((batch_index, *row));
                            }
                        }
                        Err(error) => {
                            Self::apply_insert_many_error(batch_index, chunk, error, &mut outcome);
                            if outcome.infrastructure_error.is_some() {
                                return outcome;
                            }
                        }
                    }
                }
                MongoDbConflictAction::DoNothing { .. }
                | MongoDbConflictAction::DoUpdate { .. } => {
                    let models = match chunk_documents
                        .into_iter()
                        .map(|document| {
                            Self::conflict_update_model(&namespace, document, conflict_action)
                        })
                        .collect::<EmitterRuntimeResult<Vec<_>>>()
                    {
                        Ok(models) => models,
                        Err(error) => {
                            outcome.fail(error);
                            return outcome;
                        }
                    };
                    match await_emitter_confirmation(&request_acks, async {
                        client
                            .client
                            .bulk_write(models)
                            .ordered(false)
                            .verbose_results()
                            .await
                    })
                    .await
                    {
                        Ok(_) => {
                            for row in chunk {
                                outcome.deliver((batch_index, *row));
                            }
                        }
                        Err(error) => {
                            Self::apply_bulk_write_error(batch_index, chunk, error, &mut outcome);
                            if outcome.infrastructure_error.is_some() {
                                return outcome;
                            }
                        }
                    }
                }
            }
        }
        trace!(
            collection = collection.as_str(),
            rows = outcome.delivered.len(),
            rejected = outcome.rejected.len(),
            "emitter published mongodb documents"
        );
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_only_definitive_document_write_codes_as_record_errors() {
        for code in [121, 10334, 11000] {
            assert!(
                MongoDbEmitter::is_record_write_error(code),
                "{code} should be a definitive document error"
            );
        }
        for code in [2, 6, 7, 50, 64, 89, 91, 11600, 11602, 16755] {
            assert!(
                !MongoDbEmitter::is_record_write_error(code),
                "{code} requires infrastructure retry"
            );
        }
    }

    #[test]
    fn mixed_mongodb_write_results_preserve_successes_and_record_rejections() {
        let mut outcome = PerRecordPublishOutcome::empty();

        MongoDbEmitter::apply_insert_many_write_errors(
            7,
            &[10, 11, 12],
            &[(1, 11000), (2, 91)],
            &mut outcome,
        );

        assert_eq!(outcome.delivered, [(7, 10)]);
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].position, (7, 11));
        assert!(outcome.infrastructure_error.is_some());
    }
}
