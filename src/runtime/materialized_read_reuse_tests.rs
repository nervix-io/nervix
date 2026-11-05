//! Tests of materialized generation reads through the authenticated transport.
//!
//! Layer: test harness.
//! - **Owns.** Repeated-read transfer bounds and complete rows after owner generation changes.
//! - **Depends on.** Materialized state publications, runtime fixtures and the real interconnect.
//! - **Must not know.** Archive restore policy or control-plane scheduling decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_execution::MemoryClass;
use nervix_interconnect::{
    PoolClass, TlsConfigBundle, TransferDirection, Transport, TransportClock,
};
use nervix_models::{CreateSchema, ParseAsType, SchemaField, SchemaName};
use nervix_primitives::sync::watch;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType,
};

use super::*;
use crate::runtime_schema::{CompiledSchema, RuntimeRecordMetadata, RuntimeValue, compile_schema};

struct ReadAuthority {
    certificate: rcgen::Certificate,
    key: KeyPair,
}

impl ReadAuthority {
    fn new() -> Self {
        let mut parameters = CertificateParams::default();
        parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        parameters.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let key = KeyPair::generate().assured("the fixture authority key generates");
        let certificate = parameters
            .self_signed(&key)
            .assured("the fixture authority signs its certificate");
        Self { certificate, key }
    }

    fn issue(&self, node: &ClusterNodeName) -> TlsConfigBundle {
        let mut parameters =
            CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                .assured("the loopback endpoint names are valid");
        parameters.subject_alt_names.push(SanType::URI(
            format!("nervix://cluster/test/node/{node}")
                .try_into()
                .assured("the validated node name forms a valid identity URI"),
        ));
        parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        parameters.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().assured("the fixture node key generates");
        let certificate = parameters
            .signed_by(&key, &self.certificate, &self.key)
            .assured("the shared authority signs this node");
        let directory = tempfile::tempdir().assured("the fixture TLS directory opens");
        let ca = directory.path().join("ca.pem");
        let cert = directory.path().join("node.pem");
        let private_key = directory.path().join("node-key.pem");
        std::fs::write(&ca, self.certificate.pem()).assured("the fixture authority is written");
        std::fs::write(&cert, certificate.pem()).assured("the fixture certificate is written");
        std::fs::write(&private_key, key.serialize_pem()).assured("the fixture key is written");
        TlsConfigBundle::from_pem_files(&ca, &cert, &private_key, TransportClock::system())
            .assured("the two nodes trust the same fixture authority")
    }
}

struct RemoteGenerationFixture {
    reader: Runtime,
    owner_name: ClusterNodeName,
    transport: Transport,
    selected_owner: watch::Sender<Runtime>,
    fetch_owner: watch::Sender<Runtime>,
    domain: DomainName,
    relay: RelayName,
    placement: RuntimeStatePlacement,
    schema: CompiledSchema,
    routing: DomainRouting,
}

impl RemoteGenerationFixture {
    async fn new() -> Self {
        let reader = Runtime::new();
        attach_loopback_cluster(&reader, &named("reader")).await;
        let owner_name = named("owner");
        let transport =
            crate::application::test_fixtures::test_interconnect("test", &owner_name).await;
        let dispatcher = reader
            .inner
            .remote_dispatcher
            .load_full()
            .assured("the reader fixture attaches its dispatcher");
        let authority = ReadAuthority::new();
        dispatcher
            .interconnect
            .replace_tls(authority.issue(&named("reader")))
            .await
            .assured("the reader installs the shared fixture authority");
        transport
            .replace_tls(authority.issue(&owner_name))
            .await
            .assured("the owner installs the shared fixture authority");
        dispatcher
            .interconnect
            .register_outbound_target(owner_name.clone(), transport.local_addr().into())
            .assured("the authenticated owner endpoint is valid");

        let domain = domain("materialized_reads");
        let relay = named::<RelayName>("profiles");
        install_unpaced_test_domain(&reader, &domain);
        publish_state_identity(&reader, &domain, ModelKind::Relay, ModelName::from(&relay));
        let placement = reader
            .state_placement(
                &domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                &relay,
                None,
            )
            .assured("the reader has the relay's published identity");
        let schema = compile_schema(&CreateSchema {
            name: named::<SchemaName>("profile"),
            fields: vec![
                SchemaField {
                    name: named("value"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("payload"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: true,
                },
            ],
        });
        let routing = DomainRouting::new(DomainRoutingSnapshot {
            materialized_stream_owner_nodes: [(
                relay.clone(),
                RelayOwnerRoutes::new(BTreeMap::from([(Unit::Single, owner_name.clone())])),
            )]
            .into_iter()
            .collect(),
            materialized_stream_specs: [(
                relay.clone(),
                RuntimeMaterializedRelaySpec::new(
                    schema.arrow_schema(),
                    VmSchemaSensitivity::default(),
                    test_branching(&[("tenant", ParseAsType::String)]),
                ),
            )]
            .into_iter()
            .collect(),
            ..DomainRoutingSnapshot::default()
        });
        let (selected_owner, describe_owner) = watch::channel(Runtime::new());
        let (fetch_owner, fetch_runtime) = watch::channel(Runtime::new());
        transport
            .register_handler::<DescribeStateSnapshot, _, _>(move |_context, request| {
                let owner = describe_owner.borrow().clone();
                async move {
                    let placement = RuntimeStatePlacement::from_remote(request.placement)
                        .assured("the request carries the fixture placement");
                    owner
                        .describe_sealed_materialized_snapshot(&placement, request.after_revision)
                        .await
                }
            })
            .assured("the snapshot description handler registers");
        transport
            .register_stream_handler::<FetchStateSnapshot, _, _>(move |_context, request| {
                let owner = fetch_runtime.borrow().clone();
                async move { owner.stream_sealed_materialized_snapshot(request).await }
            })
            .assured("the snapshot transfer handler registers");
        Self {
            reader,
            owner_name,
            transport,
            selected_owner,
            fetch_owner,
            domain,
            relay,
            placement,
            schema,
            routing,
        }
    }

    fn replace_generation(&self, round: i64, count: usize) -> Vec<MaterializedGenerationRecord> {
        let owner = Runtime::new();
        let mut assignment = owner.replicated_materialized_stream_state(
            self.placement.clone(),
            self.schema.arrow_schema(),
            Some(self.owner_name.clone()),
            Vec::new(),
            Some(&self.owner_name),
        );
        let mut writer = assignment
            .originator
            .take()
            .assured("the selected owner originates this assignment");
        let mut expected = Vec::with_capacity(count);
        for index in 0..count {
            let branch = string_branch_key("tenant", &format!("tenant-{index}"));
            let index = i64::try_from(index).assured("the fixture contains at most six rows");
            let row = self
                .schema
                .batch_from_test_rows([[
                    ("value".to_string(), RuntimeValue::I64(round + index)),
                    (
                        "payload".to_string(),
                        RuntimeValue::String(format!("{round}:{}", "x".repeat(32 * 1024))),
                    ),
                ]])
                .assured("the fixture row has the complete declared schema")
                .runtime_row(
                    0,
                    RuntimeRecordMetadata::from_ingested_at_watermarks(
                        Timestamp::from_unix_nanos(round + index),
                        Timestamp::from_unix_nanos(round + index + 1),
                    ),
                )
                .assured("the fixture batch holds one row");
            writer
                .update_last_by_timestamp(&branch, row.clone())
                .assured("each distinct branch is published by its current assignment");
            expected.push(MaterializedGenerationRecord { branch, row });
        }
        self.fetch_owner.send_replace(owner.clone());
        self.selected_owner.send_replace(owner);
        expected
    }

    async fn read(
        &self,
        routing: &mut DomainRoutingCache,
        cache: &mut MaterializedGenerationReadCache,
    ) -> Vec<MaterializedGenerationRecord> {
        self.reader
            .materialized_records_from_owner(routing, cache, &self.domain, &self.relay)
            .await
            .assured("the authenticated remote generation is readable")
            .collect()
    }

    fn received_bytes(&self) -> u64 {
        self.reader
            .inner
            .remote_dispatcher
            .load_full()
            .assured("the reader retains its dispatcher")
            .interconnect
            .snapshot()
            .counters
            .bulk_bytes[PoolClass::Bulk.index()][TransferDirection::Received.index()]
    }

    fn assert_records(
        &self,
        actual: &[MaterializedGenerationRecord],
        expected: &[MaterializedGenerationRecord],
    ) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert_eq!(actual.branch, expected.branch);
            assert_eq!(actual.row.arrow_schema(), expected.row.arrow_schema());
            assert_eq!(
                actual.row.metadata().to_remote(),
                expected.row.metadata().to_remote()
            );
            for index in 0..self.schema.arrow_schema().fields().len() {
                assert_eq!(
                    actual
                        .row
                        .value_at(index)
                        .assured("the returned field exists"),
                    expected
                        .row
                        .value_at(index)
                        .assured("the expected field exists")
                );
            }
        }
    }

    async fn shutdown(self) {
        self.transport.shutdown().await;
        self.reader
            .inner
            .remote_dispatcher
            .load_full()
            .assured("the fixture retains its dispatcher")
            .interconnect
            .shutdown()
            .await;
    }
}

#[nervix_primitives::test]
async fn unchanged_remote_generations_transfer_once_and_replaced_contents_are_read_completely() {
    let fixture = RemoteGenerationFixture::new().await;
    let mut routing = DomainRoutingCache::new(fixture.routing.shared());
    let mut cache = MaterializedGenerationReadCache::default();
    let expected = fixture.replace_generation(10, 6);
    let first = fixture.read(&mut routing, &mut cache).await;
    fixture.assert_records(&first, &expected);
    let first_bytes = fixture.received_bytes();
    assert!(first_bytes > 6 * 32 * 1024);
    let repeated = fixture.read(&mut routing, &mut cache).await;
    fixture.assert_records(&repeated, &expected);
    assert_eq!(
        fixture.received_bytes(),
        first_bytes,
        "another read of the same sealed generation reuses its admitted columns"
    );
    drop(first);
    drop(repeated);

    assert_eq!(
        fixture
            .reader
            .inner
            .executor
            .memory(MemoryClass::Relay)
            .reserved_bytes,
        0,
        "an idle retained read leaves live relay capacity available"
    );
    let memory = fixture
        .reader
        .inner
        .executor
        .memory(MemoryClass::StateReadCache);
    assert!(memory.reserved_bytes > 6 * 32 * 1024);
    let occupied = fixture
        .reader
        .inner
        .executor
        .try_reserve(
            MemoryClass::StateReadCache,
            memory
                .capacity_bytes
                .checked_sub(memory.reserved_bytes)
                .assured("the retained generation is within its node budget"),
        )
        .assured("the test occupies the memory outside the retained generation");

    // Replacement must release the retained admission before decoding another complete generation.
    // A replacement owner can recover the same revision with different complete contents.
    let expected = fixture.replace_generation(20, 6);
    let replaced = fixture.read(&mut routing, &mut cache).await;
    fixture.assert_records(&replaced, &expected);
    let replaced_bytes = fixture.received_bytes();
    assert!(replaced_bytes > first_bytes);

    // Its restored checkpoint can also precede the revision the reader previously observed.
    let expected = fixture.replace_generation(30, 2);
    let restored = fixture.read(&mut routing, &mut cache).await;
    fixture.assert_records(&restored, &expected);
    let restored_bytes = fixture.received_bytes();
    assert!(restored_bytes > replaced_bytes);

    fixture.selected_owner.send_replace(Runtime::new());
    let unavailable = fixture
        .reader
        .materialized_records_from_owner(&mut routing, &mut cache, &fixture.domain, &fixture.relay)
        .await;
    assert!(
        unavailable.is_err(),
        "the current owner must authorize each read"
    );
    assert_eq!(
        fixture
            .reader
            .inner
            .executor
            .memory(MemoryClass::StateReadCache)
            .reserved_bytes,
        occupied.bytes(),
        "a failed read releases the cached generation's admission"
    );

    let expected = fixture.replace_generation(30, 2);
    let recovered = fixture.read(&mut routing, &mut cache).await;
    fixture.assert_records(&recovered, &expected);
    assert!(fixture.received_bytes() > restored_bytes);
    let read = fixture
        .reader
        .materialized_records_from_owner(&mut routing, &mut cache, &fixture.domain, &fixture.relay)
        .await
        .assured("the recovered generation can be retained by a read");
    cache.clear();
    assert!(
        fixture
            .reader
            .inner
            .executor
            .memory(MemoryClass::StateReadCache)
            .reserved_bytes
            > occupied.bytes(),
        "an outstanding read retains the generation's admission after the cache is released"
    );
    drop(read);
    assert_eq!(
        fixture
            .reader
            .inner
            .executor
            .memory(MemoryClass::StateReadCache)
            .reserved_bytes,
        occupied.bytes(),
        "ending the task's read lifetime releases its generation"
    );
    drop(occupied);

    let cache_capacity = fixture
        .reader
        .inner
        .executor
        .memory(MemoryClass::StateReadCache)
        .capacity_bytes;
    let full_cache = fixture
        .reader
        .inner
        .executor
        .try_reserve(MemoryClass::StateReadCache, cache_capacity)
        .assured("other read tasks can occupy the entire optional cache pool");
    let uncached = fixture
        .reader
        .materialized_records_from_owner(&mut routing, &mut cache, &fixture.domain, &fixture.relay)
        .await
        .assured("cache pressure still admits a complete active source read");
    assert!(
        fixture
            .reader
            .inner
            .executor
            .memory(MemoryClass::StateRead)
            .reserved_bytes
            > 0,
        "an active uncached view retains its independent source-read admission"
    );
    let relay_capacity = fixture
        .reader
        .inner
        .executor
        .memory(MemoryClass::Relay)
        .capacity_bytes;
    let live_work = fixture
        .reader
        .inner
        .executor
        .try_reserve(MemoryClass::Relay, relay_capacity)
        .expect(
            "an active uncached source read leaves relay admission for evaluation and dispatch",
        );
    drop(live_work);
    drop(uncached);
    assert_eq!(
        fixture
            .reader
            .inner
            .executor
            .memory(MemoryClass::StateRead)
            .reserved_bytes,
        0,
        "ending the active view returns its source-read admission"
    );
    let bytes_before = fixture.received_bytes();
    for _ in 0..2 {
        let uncached = fixture.read(&mut routing, &mut cache).await;
        fixture.assert_records(&uncached, &expected);
        assert!(
            cache.remote.is_empty(),
            "cache pressure leaves a complete uncached read"
        );
        assert_eq!(
            fixture
                .reader
                .inner
                .executor
                .memory(MemoryClass::Relay)
                .reserved_bytes,
            0,
            "consuming an uncached read returns its transient relay charge"
        );
    }
    assert!(
        fixture.received_bytes() > bytes_before,
        "uncached reads fetch their currently authorized generation"
    );
    drop(full_cache);
    let available = fixture.read(&mut routing, &mut cache).await;
    fixture.assert_records(&available, &expected);
    assert!(
        cache.remote.contains_key(&Unit::Single),
        "a later read can retain the generation when room returns"
    );
    let expected = fixture.replace_generation(40, 2);
    fixture.fetch_owner.send_replace(Runtime::new());
    let Err(error) = fixture
        .reader
        .materialized_records_from_owner(&mut routing, &mut cache, &fixture.domain, &fixture.relay)
        .await
    else {
        panic!("a successful description cannot authorize a failed fetch");
    };
    assert!(matches!(
        error.current_context(),
        MaterializedReadError::RemoteSnapshot { .. }
    ));
    assert!(error.contains::<MaterializedSnapshotExchangeError>());
    assert!(
        error.contains::<nervix_interconnect::RequestError>(),
        "a failed snapshot transfer retains its typed interconnect cause: {error:?}"
    );
    assert_eq!(
        fixture
            .reader
            .inner
            .executor
            .memory(MemoryClass::StateReadCache)
            .reserved_bytes,
        0,
        "a failed replacement fetch releases the preceding generation"
    );
    fixture
        .fetch_owner
        .send_replace(fixture.selected_owner.borrow().clone());
    let fetched = fixture.read(&mut routing, &mut cache).await;
    fixture.assert_records(&fetched, &expected);
    cache.clear();
    assert_eq!(
        fixture
            .reader
            .inner
            .executor
            .memory(MemoryClass::StateReadCache)
            .reserved_bytes,
        0
    );
    fixture.shutdown().await;
}
