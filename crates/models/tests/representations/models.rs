//! Models and statements keep every field through their archived forms, and a stored Model's
//! resource versions survive widening into the versions a statement asks for.
//!
//! The domain is the vocabulary's: every Model family and statement form, including states NSPL
//! cannot spell, such as negative and non-finite literals, empty arrays and collection casts. A
//! stored Model binds concrete resource versions; a statement carries `LATEST` or a number.

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{Model, RequestedResourceVersion, Statement};

/// Pins every resource version `model` asks for to a number, as planning does before a Model is
/// stored. `LATEST` pins to zero, which is as much a version number as any other.
fn pinned(model: Model<RequestedResourceVersion>) -> Model {
    let pinned: Result<Model, std::convert::Infallible> =
        model.try_map_resource_versions(|_, version| match version {
            RequestedResourceVersion::Number(number) => Ok(number),
            RequestedResourceVersion::Latest => Ok(0),
        });
    match pinned {
        Ok(model) => model,
        Err(never) => match never {},
    }
}

#[test]
fn bolero_models_and_statements_round_trip_through_their_archives() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);

            let stored = pinned(arbitrary.model());
            assert_eq!(crate::archive_round_trip!(&stored, Model), stored);

            // A stored Model widens into the versions a statement asks for and pins back to itself.
            let requested = Model::<RequestedResourceVersion>::from(stored.clone());
            assert_eq!(pinned(requested), stored);

            let statement = arbitrary.statement();
            assert_eq!(crate::archive_round_trip!(&statement, Statement), statement);
        });
}

/// Every pointer-sized count in the vocabulary is exercised through its actual owning record.
fn assert_archived_count_records(count: usize) {
    use std::num::NonZeroUsize;

    use meticulous::ResultExt as _;
    use nervix_models::{
        AlterRelay, AlterRelayOperation, CreateRelay, ImpactPlanningBasis, RelayBranching,
        RelayName, ResourceName, SchemaName, TransactionCommitPlanHeader,
        TransactionOperationNumber, TransactionPosition, TransactionPreviewIdentity,
        WasmCheckpointCounts, WasmStateGeneration, WasmStateInspection,
    };

    let capacity = match NonZeroUsize::new(count) {
        Some(capacity) => capacity,
        None => NonZeroUsize::MIN,
    };
    let relay = CreateRelay {
        name: RelayName::parse("notifications").assured("the literal follows the name rule"),
        schema: SchemaName::parse("notification").assured("the literal follows the name rule"),
        buffer: capacity,
        branching: RelayBranching::Unbranched,
        materialized_state: None,
    };
    assert_eq!(crate::archive_round_trip!(&relay, CreateRelay), relay);
    let alter = AlterRelay {
        relay: relay.name.clone(),
        operations: vec![AlterRelayOperation::SetCapacity { capacity }],
    };
    assert_eq!(crate::archive_round_trip!(&alter, AlterRelay), alter);
    let operation = TransactionOperationNumber::new(capacity);
    assert_eq!(
        crate::archive_round_trip!(&operation, TransactionOperationNumber),
        operation
    );
    let position = TransactionPosition::new(count);
    assert_eq!(
        crate::archive_round_trip!(&position, TransactionPosition),
        position
    );
    let header = TransactionCommitPlanHeader {
        preview: TransactionPreviewIdentity {
            transaction_id: "transaction-counts".to_string(),
            position,
            planning_basis: ImpactPlanningBasis::new([7; 32]),
        },
        step_count: count,
    };
    assert_eq!(
        crate::archive_round_trip!(&header, TransactionCommitPlanHeader),
        header
    );

    for stage in 0..6 {
        let mut counts = WasmCheckpointCounts {
            total: count,
            ..Default::default()
        };
        match stage {
            0 => counts.empty = count,
            1 => counts.captured = count,
            2 => counts.locally_durable = count,
            3 => counts.awaiting_replicas = count,
            4 => counts.replica_confirmed = count,
            _ => counts.failed = count,
        }
        assert_eq!(
            crate::archive_round_trip!(&counts, WasmCheckpointCounts),
            counts
        );
        let inspection = WasmStateInspection {
            resource: ResourceName::parse("processor").assured("the literal follows the name rule"),
            resource_version: 1,
            file: "processor.wasm".to_string(),
            default_generation: WasmStateGeneration::FIRST,
            reset: None,
            reset_readiness: None,
            recoveries: Vec::new(),
            omitted_recoveries: count,
            checkpoint_counts: counts,
            checkpoints: Vec::new(),
            omitted_checkpoints: count,
        };
        assert_eq!(
            crate::archive_round_trip!(&inspection, WasmStateInspection),
            inspection
        );
    }
}

#[test]
fn archived_counts_preserve_their_boundaries() {
    use meticulous::ResultExt as _;

    for count in [0, u64::from(u32::MAX) + 1, u64::MAX] {
        let count = usize::try_from(count).assured("the native test target addresses 64 bits");
        assert_archived_count_records(count);
    }
}

#[test]
fn bolero_archived_counts_round_trip() {
    use meticulous::ResultExt as _;
    use nervix_arbitrary::Entropy;

    bolero::check!()
        .with_iterations(256)
        .with_max_len(16)
        .for_each(|bytes: &[u8]| {
            let count = Entropy::new(bytes).any_u64();
            let count = usize::try_from(count).assured("the native test target addresses 64 bits");
            assert_archived_count_records(count);
        });
}
