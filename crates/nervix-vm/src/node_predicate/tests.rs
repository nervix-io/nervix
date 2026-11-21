//! Selector typing and batch evaluation over real vocabulary records.
//!
//! Layer: test harness.
//! - **Owns.** Checks for the declared node schema, literal tag binding and per-row results.
//! - **Depends on.** The node predicate API, test record generator and Plexus parser.
//! - **Must not know.** Consensus, inspection routing or placement policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "verifies selectors at the expression boundary")
)]

use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{
    BinaryOperator, BuiltinFunctionName, ClusterNodeIdentity, CudaProviderRegistration,
    ExecutionProviders, GpuDevice, GpuDevices, GpuInventory, NodeTags, UndetectedReason,
};
use nervix_plexus::parse_expression;
use nonzero_ext::nonzero;

use super::*;
use crate::program::FunctionName;

fn record(name: &ClusterNodeName, tags: &[&str]) -> CommittedNodeRecord {
    let mut record = Arbitrary::new(&[], Domain::Vocabulary).committed_node_record();
    let incarnation = record.record.identity().incarnation();
    record.record.claim.identity = ClusterNodeIdentity::new(name.clone(), incarnation);
    record.record.claim.capabilities.cpu_count = Some(NonZeroU64::MAX);
    record.record.claim.capabilities.memory_bytes = None;
    record.record.properties.cpu_model = Detected::Undetected(UndetectedReason::NotReported);
    record.record.properties.gpus = GpuInventory::NoDevice;
    record.record.properties.onnx =
        OnnxRuntimeDetection::Undetected(UndetectedReason::LibraryUnavailable);
    record.record.tags =
        NodeTags::from_assignments(tags.iter().copied()).assured("the fixture has valid tags");
    record
}

fn predicate(source: &str) -> NodePredicate {
    let expression = parse_expression(source).assured("the fixture uses valid Plexus syntax");
    NodePredicate::compile(&expression).assured("the fixture uses a Boolean node selector")
}

#[test]
fn selector_rows_keep_full_width_facts_nulls_and_original_error_indices() {
    let records = [
        ("node-3", None),
        ("node-1", Some("rank=two")),
        ("node-2", Some("rank=2")),
    ]
    .into_iter()
    .map(|(node, tag)| {
        let name = ClusterNodeName::parse(node).assured("the fixture has a valid node name");
        let record = record(&name, tag.as_slice());
        (name, record)
    })
    .collect();
    let evaluation = predicate(
        "(tag('rank') AS I64) = 2 AND node.cpu.count = ('18446744073709551615' AS U64)
        AND is_null(node.cpu.model) AND is_null(node.memory.limit)
        AND node.gpu.count = (0 AS U64) AND is_null(node.gpu.memory)
        AND is_null(node.gpu.driver) AND is_null(node.onnx.version) AND \
         is_null(node.onnx.providers)
        AND NOT node.onnx.cuda AND NOT node.cuda.available",
    )
    .evaluate(&records)
    .assured("one bad value produces a per-row error");
    assert_eq!(evaluation.selected_rows.iter().collect::<Vec<_>>(), vec![1]);
    assert_eq!(evaluation.input_errors.row(0).len(), 1);
    assert!(evaluation.input_errors.row(1).is_empty());
    assert!(evaluation.input_errors.row(2).is_empty());

    let absent = predicate("is_null(tag('missing'))")
        .evaluate(&records)
        .assured("an absent literal tag is a nullable STRING");
    assert_eq!(
        absent.selected_rows.iter().collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    let empty = predicate("TRUE")
        .evaluate(&BTreeMap::new())
        .assured("an empty input builds a valid batch");
    assert_eq!(empty.selected_rows.iter().count(), 0);
}

#[test]
fn selector_reads_detected_gpu_and_runtime_facts() {
    let name = ClusterNodeName::parse("node-1").assured("the fixture uses a valid node name");
    let mut row = record(&name, &[]);
    let text = |value| PropertyText::new(value).assured("the fixture uses valid detected text");
    row.record.properties.gpus = GpuInventory::Devices {
        devices: GpuDevices::try_from(vec![
            GpuDevice {
                model: Detected::Value(text("Accelerator A")),
                memory_bytes: Detected::Value(nonzero!(1024_u64)),
            },
            GpuDevice {
                model: Detected::Undetected(UndetectedReason::NotReported),
                memory_bytes: Detected::Value(nonzero!(512_u64)),
            },
        ])
        .assured("two GPU records are within the inventory bound"),
        driver: Detected::Value(text("550.54.15")),
    };
    row.record.properties.onnx = OnnxRuntimeDetection::Loaded(OnnxRuntime {
        version: Detected::Value(text("1.30.0")),
        providers: Detected::Value(
            ExecutionProviders::try_from(vec![
                text("CUDAExecutionProvider"),
                text("CPUExecutionProvider"),
            ])
            .assured("two providers are within the inventory bound"),
        ),
        cuda: CudaProviderRegistration::Registered,
    });
    let evaluation = predicate(
        "node.gpu.count = (2 AS U64) AND node.gpu.memory = (512 AS U64)
        AND node.gpu.driver = '550.54.15' AND first(node.gpu.models) = 'Accelerator A'
        AND is_null(last(node.gpu.models)) AND node.onnx.version = '1.30.0'
        AND last(node.onnx.providers) = 'CPUExecutionProvider' AND node.cuda.available",
    )
    .evaluate(&BTreeMap::from([(name, row)]))
    .assured("the selector reads the detected values");
    assert!(evaluation.selected_rows.is_single(0));
    assert!(evaluation.input_errors.row(0).is_empty());
}

#[test]
fn selector_rejects_unknown_names_wrong_types_and_other_contexts() {
    for (source, reason) in [
        ("node.gpu.unknown", "node.gpu.unknown"),
        ("is_null(unknown('zone'))", "unknown function 'unknown'"),
        ("node.name", "WHERE expression must evaluate to Boolean"),
        (
            "is_null(tag(node.name))",
            "tag() requires a literal STRING key",
        ),
        ("is_null(tag(42))", "tag() requires a literal STRING key"),
        (
            "branch.region = 'eu'",
            "Branch is unavailable in a node selector",
        ),
        (
            "NOT is_null(now())",
            "must depend only on its input fields and literals",
        ),
    ] {
        let expression = parse_expression(source).assured("the expression is valid syntax");
        let error = NodePredicate::compile(&expression).expect_err(source);
        assert!(format!("{error:#}").contains(reason), "{source}: {error:#}");
    }
    let field = parse_expression("node.name").assured("the field reference has valid syntax");
    assert!(
        lower_expression(&field, SemanticScopePolicy::Unavailable)
            .expect_err("a domain expression cannot read the node scope")
            .to_string()
            .contains("the node scope is available only in node selectors")
    );
    assert!(
        FunctionName::node_selector_completion_names()
            .iter()
            .any(|name| name == NODE_TAG_FUNCTION)
    );
    assert!(
        !FunctionName::ordinary_completion_names()
            .iter()
            .any(|name| name == NODE_TAG_FUNCTION)
    );
}

#[test]
fn bolero_node_tag_matches_exact_values_and_absence() {
    bolero::check!().with_type::<[u8; 128]>().for_each(|bytes| {
        let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
        let key = arbitrary.tag_key();
        let value = arbitrary.tag_value(&key);
        let different_value = if value.as_str() == "other" {
            "another"
        } else {
            "other"
        };
        let matching = format!("{key}={value}");
        let different = format!("{key}={different_value}");
        let tags = [Some(matching.as_str()), None, Some(different.as_str())];
        let expected_match = usize::from(bytes[0] % 3);
        let expected_absent = (expected_match + 1) % 3;
        let records = (0..3)
            .map(|index| {
                let name = ClusterNodeName::parse(&format!("node-{index}"))
                    .assured("the fixture names the rows in order");
                let assignment = tags[(index + 3 - expected_match) % 3];
                let record = record(&name, assignment.as_slice());
                (name, record)
            })
            .collect();
        let read = Expression::Call {
            function: BuiltinFunctionName::parse(NODE_TAG_FUNCTION)
                .assured("the tag builtin name is valid"),
            arguments: vec![Expression::Literal(Literal::String(
                key.as_str().to_string(),
            ))],
        };
        let equal = Expression::Binary {
            operator: BinaryOperator::Equal,
            left: Box::new(read.clone()),
            right: Box::new(Expression::Literal(Literal::String(
                value.as_str().to_string(),
            ))),
        };
        let absent = Expression::Call {
            function: BuiltinFunctionName::parse("is_null")
                .assured("the null-check builtin name is valid"),
            arguments: vec![read],
        };
        for (expression, expected) in [(equal, expected_match), (absent, expected_absent)] {
            let result = NodePredicate::compile(&expression)
                .assured("the generated selector uses a literal key")
                .evaluate(&records)
                .assured("reading an exact tag or its absence is infallible");
            assert_eq!(
                result.selected_rows.iter().collect::<Vec<_>>(),
                vec![expected]
            );
            for index in 0..3 {
                assert!(result.input_errors.row(index).is_empty());
            }
        }
    });
}
