mod stored;

use std::path::Path;

use error_stack::{Report, ResultExt};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use nervix_models::{BranchSelection, Domain, Identifier, KafkaOffsetMode, Model, ModelKind};
use serde::{Deserialize, Serialize};
pub use stored::{StoredModelEnvelope, StoredModelVersioned};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StorageError {
    #[error("failed to open database")]
    OpenDatabase,
    #[error("failed to open keyspace")]
    OpenKeyspace,
    #[error("failed to encode key")]
    EncodeKey,
    #[error("failed to serialize model")]
    SerializeValue,
    #[error("failed to write model")]
    WriteValue,
    #[error("failed to read model")]
    ReadValue,
    #[error("failed to deserialize model")]
    DeserializeValue,
    #[error("failed to convert stored model")]
    ModelConversion,
    #[error("model already exists")]
    AlreadyExists,
    #[error("failed to iterate values")]
    IterateValues,
    #[error("failed to decode key")]
    DecodeKey,
}

pub struct ModelStorage {
    _db: Database,
    index: Keyspace,
}

impl ModelStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Report<StorageError>> {
        let db = Database::builder(path)
            .open()
            .change_context(StorageError::OpenDatabase)?;

        let index = db
            .keyspace("models", KeyspaceCreateOptions::default)
            .change_context(StorageError::OpenKeyspace)?;

        Ok(Self { _db: db, index })
    }

    pub fn put(
        &self,
        domain: &Domain,
        identifier: &Identifier,
        model: &Model,
    ) -> Result<(), Report<StorageError>> {
        let key = encode_key(domain, identifier)?;

        if self
            .index
            .get(key.clone())
            .change_context(StorageError::ReadValue)?
            .is_some()
        {
            return Err(Report::new(StorageError::AlreadyExists));
        }

        let value = serialize_value(model)?;

        self.index
            .insert(key, value)
            .change_context(StorageError::WriteValue)
    }

    pub fn get(
        &self,
        domain: &Domain,
        identifier: &Identifier,
    ) -> Result<Option<Model>, Report<StorageError>> {
        let key = encode_key(domain, identifier)?;
        let Some(raw) = self
            .index
            .get(key)
            .change_context(StorageError::ReadValue)?
        else {
            return Ok(None);
        };

        let envelope = deserialize_value(raw.as_ref())?;

        let model = Model::try_from(envelope).change_context(StorageError::ModelConversion)?;
        Ok(Some(model))
    }

    pub fn list_identifiers(
        &self,
        domain: &Domain,
        kind: ModelKind,
        prefix: &str,
    ) -> Result<Vec<Identifier>, Report<StorageError>> {
        let mut out = Vec::new();
        let prefix = prefix.to_ascii_lowercase();

        for guard in self.index.iter() {
            let (raw_key, raw_value) =
                guard.into_inner().change_context(StorageError::ReadValue)?;

            let key: ModelKeyOwned =
                storekey::deserialize(&raw_key).change_context(StorageError::DecodeKey)?;
            if key.domain != domain.as_str() {
                continue;
            }

            let envelope = deserialize_value(raw_value.as_ref())?;
            let model = Model::try_from(envelope).change_context(StorageError::ModelConversion)?;
            if model.kind() != kind {
                continue;
            }

            if !key.identifier.starts_with(&prefix) {
                continue;
            }

            let identifier =
                Identifier::parse(&key.identifier).change_context(StorageError::ModelConversion)?;
            out.push(identifier);
        }

        out.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        out.dedup_by(|a, b| a.as_str() == b.as_str());
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct ModelKey<'a> {
    domain: &'a str,
    identifier: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ModelKeyOwned {
    domain: String,
    identifier: String,
}

fn encode_key(domain: &Domain, identifier: &Identifier) -> Result<Vec<u8>, Report<StorageError>> {
    storekey::serialize(&ModelKey {
        domain: domain.as_str(),
        identifier: identifier.as_str(),
    })
    .change_context(StorageError::EncodeKey)
}

fn serialize_value(model: &Model) -> Result<Vec<u8>, Report<StorageError>> {
    let stored = StoredModelVersioned::from(model.clone());
    let envelope = StoredModelEnvelope::V1(stored);
    rkyv::to_bytes::<rkyv::rancor::Error>(&envelope)
        .map(|bytes| bytes.to_vec())
        .change_context(StorageError::SerializeValue)
}

fn deserialize_value(bytes: &[u8]) -> Result<StoredModelEnvelope, Report<StorageError>> {
    rkyv::from_bytes::<StoredModelEnvelope, rkyv::rancor::Error>(bytes)
        .change_context(StorageError::DeserializeValue)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use nervix_models::{
        CreateTransportKafka, Domain, Identifier, KafkaConfigEntry, Model, ModelKind,
    };

    use super::{ModelStorage, StorageError};

    fn temp_db_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("nervix-storage-test-{nanos}"))
    }

    fn sample_model(name: &str) -> Model {
        Model::TransportKafka(CreateTransportKafka {
            name: Identifier::parse(name).expect("valid identifier"),
            config: vec![KafkaConfigEntry {
                key: "bootstrap.servers".to_string(),
                value: "localhost:9092".to_string(),
            }],
        })
    }

    #[test]
    fn create_fails_when_model_already_exists() {
        let path = temp_db_path();
        let storage = ModelStorage::open(&path).expect("storage should open");
        let ns = Domain::parse("default").expect("valid domain");
        let id = Identifier::parse("kafka_main").expect("valid identifier");
        let model = sample_model("kafka_main");

        storage
            .put(&ns, &id, &model)
            .expect("first create should succeed");
        let err = storage
            .put(&ns, &id, &model)
            .expect_err("second create should fail");

        assert_eq!(err.current_context(), &StorageError::AlreadyExists);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn get_roundtrip_returns_stored_model() {
        let path = temp_db_path();
        let storage = ModelStorage::open(&path).expect("storage should open");
        let ns = Domain::parse("default").expect("valid domain");
        let id = Identifier::parse("kafka_main").expect("valid identifier");
        let model = sample_model("kafka_main");

        storage
            .put(&ns, &id, &model)
            .expect("create should succeed");
        let loaded = storage
            .get(&ns, &id)
            .expect("read should succeed")
            .expect("model should exist");

        assert_eq!(loaded, model);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn list_identifiers_filters_by_kind_and_prefix() {
        let path = temp_db_path();
        let storage = ModelStorage::open(&path).expect("storage should open");
        let ns = Domain::parse("default").expect("valid domain");

        let transport_id = Identifier::parse("kafka_main").expect("valid identifier");
        let transport = sample_model("kafka_main");
        storage
            .put(&ns, &transport_id, &transport)
            .expect("transport create should succeed");

        let ingestor_id = Identifier::parse("kafka_ingestor").expect("valid identifier");
        let ingestor = Model::Ingestor(models::CreateIngestor {
            name: ingestor_id.clone(),
            into_relay: Identifier::parse("notifications").expect("valid identifier"),
            decode_using_codec: Identifier::parse("notification_kafka_message")
                .expect("valid identifier"),
            timestamp_source: None,
            branched_by: BranchSelection::unbranched(),
            source: nervix_models::IngestSource::Kafka {
                client: transport_id,
                topic: Identifier::parse("notifications").expect("valid identifier"),
                offset_mode: KafkaOffsetMode::ConsumerGroup(
                    Identifier::parse("cg").expect("valid identifier"),
                ),
                instances: 1,
                mode: nervix_models::KafkaIngestMode::AckSequential {
                    timeout: "30s".to_string(),
                    retry_policy: nervix_models::RetryPolicy {
                        backoff: "200ms".to_string(),
                        max_backoff: "5s".to_string(),
                    },
                },
            },
            filter_map: None,
        });
        storage
            .put(&ns, &ingestor_id, &ingestor)
            .expect("ingestor create should succeed");

        let transports = storage
            .list_identifiers(&ns, ModelKind::Transport, "kafka_")
            .expect("list should succeed");
        assert_eq!(
            transports
                .iter()
                .map(Identifier::as_str)
                .collect::<Vec<_>>(),
            vec!["kafka_main"]
        );

        let ingestors = storage
            .list_identifiers(&ns, ModelKind::Ingestor, "kafka_")
            .expect("list should succeed");
        assert_eq!(
            ingestors.iter().map(Identifier::as_str).collect::<Vec<_>>(),
            vec!["kafka_ingestor"]
        );

        let _ = fs::remove_dir_all(path);
    }
}
