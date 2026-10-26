use nonzero_ext::nonzero;

use crate::{
    AckMode, AlterDeduplicator, AlterDeduplicatorError, AlterDeduplicatorOperation, AlterJunction,
    AlterJunctionError, AlterProcessorError, AlterProcessorOperation, AlterReingestor,
    AlterReingestorError, AlterReorderer, AlterReordererError, AlterReordererOperation,
    BranchSelection, CreateDeduplicator, CreateInferencer, CreateJunction, CreateReingestor,
    CreateReorderer, Expression, FlushPolicy, InferencerExecutionMode, InferencerTensorDeclaration,
    InferencerTensorDimension, InferencerTensorElementType, InferencerTensorMapping,
    InferencerTensorRepresentation, InferencerTensorSchema, InferencerTensorSchemaError, Literal,
    MaterializedStateDependency, MaterializedStatePolicy, ParseAsType, ProcessorInputs,
    ProcessorOutput, ProcessorOutputs,
};

fn named<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str>,
    for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
{
    N::try_from(raw).expect("valid name")
}

#[test]
fn inferencer_tensor_schema_requires_exact_array_axis_structure() {
    let schema = InferencerTensorSchema {
        representation: InferencerTensorRepresentation::Dense,
        element_type: InferencerTensorElementType::F32,
        dimensions: vec![
            InferencerTensorDimension::Fixed(nonzero!(2u32)),
            InferencerTensorDimension::Dynamic,
            InferencerTensorDimension::Fixed(nonzero!(3u32)),
        ],
    };
    let exact = ParseAsType::Array {
        len: nonzero!(2u32),
        element: Box::new(ParseAsType::Vec {
            element: Box::new(ParseAsType::Array {
                len: nonzero!(3u32),
                element: Box::new(ParseAsType::F32),
            }),
        }),
    };
    let flattened = ParseAsType::Array {
        len: nonzero!(6u32),
        element: Box::new(ParseAsType::F32),
    };

    assert!(schema.is_compatible_with_field_type(&exact));
    assert!(!schema.is_compatible_with_field_type(&flattened));
    assert_eq!(schema.message_type(), exact);
}

#[test]
fn junction_alter_preserves_order_and_rejects_ambiguous_routes_atomically() {
    let mut junction = CreateJunction {
        name: named("route_events"),
        from: ProcessorInputs::new(vec![named("incoming")], Vec::new()),
        output_routes: ProcessorOutputs::new(vec![
            ProcessorOutput::with_flush_policy(
                named("accepted"),
                FlushPolicy::Each {
                    interval: "100ms".to_string(),
                    max_batch_size: "1MiB".to_string(),
                },
            ),
            ProcessorOutput::with_flush_policy(
                named("accepted"),
                FlushPolicy::Each {
                    interval: "200ms".to_string(),
                    max_batch_size: "2MiB".to_string(),
                },
            ),
        ]),
        branched_by: BranchSelection::unbranched(),
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: Vec::new(),
    };
    let before = junction.clone();
    let error = junction
        .apply_alter(&AlterJunction {
            junction: named("route_events"),
            operations: vec![
                AlterProcessorOperation::SetMode {
                    mode: AckMode::Detached,
                },
                AlterProcessorOperation::DropRoute {
                    relay: named("accepted"),
                },
            ],
        })
        .expect_err("duplicate route targets must be ambiguous");
    assert_eq!(
        error.current_context(),
        &AlterJunctionError::Processor(AlterProcessorError::RouteTargetAmbiguous {
            relay: named("accepted"),
        })
    );
    assert_eq!(junction, before, "failed ALTER must not partially apply");
}

#[test]
fn junction_alter_applies_ordered_collection_filter_dependency_and_route_updates() {
    let mut junction = CreateJunction {
        name: named("route_events"),
        from: ProcessorInputs::single(named("incoming_a")),
        output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
            named("accepted"),
            FlushPolicy::Each {
                interval: "100ms".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
        )]),
        branched_by: BranchSelection::unbranched(),
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: Vec::new(),
    };
    let true_expression = Expression::Literal(Literal::Bool(true));
    let false_expression = Expression::Literal(Literal::Bool(false));
    let replacement = ProcessorOutput::with_flush_policy(
        named("accepted"),
        FlushPolicy::Each {
            interval: "250ms".to_string(),
            max_batch_size: "2MiB".to_string(),
        },
    );
    junction
        .apply_alter(&AlterJunction {
            junction: named("route_events"),
            operations: vec![
                AlterProcessorOperation::AddFrom {
                    relay: named("incoming_b"),
                    where_clause: Some(true_expression.clone()),
                },
                AlterProcessorOperation::AlterFromSetWhere {
                    relay: named("incoming_b"),
                    where_clause: false_expression.clone(),
                },
                AlterProcessorOperation::SetFilterWhere {
                    where_clause: true_expression.clone(),
                },
                AlterProcessorOperation::SetFilterWhere {
                    where_clause: false_expression.clone(),
                },
                AlterProcessorOperation::AddMaterializedState {
                    dependency: MaterializedStateDependency {
                        relay: named("profiles"),
                        policy: MaterializedStatePolicy::RequiredWait,
                    },
                },
                AlterProcessorOperation::AddMaterializedState {
                    dependency: MaterializedStateDependency {
                        relay: named("accounts"),
                        policy: MaterializedStatePolicy::RequiredSkip,
                    },
                },
                AlterProcessorOperation::AlterMaterializedState {
                    relay: named("profiles"),
                    policy: MaterializedStatePolicy::RequiredSkip,
                },
                AlterProcessorOperation::AddRoute {
                    route: ProcessorOutput::with_flush_policy(
                        named("audit"),
                        FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        },
                    ),
                },
                AlterProcessorOperation::ReplaceRoute {
                    route: replacement.clone(),
                },
                AlterProcessorOperation::SetMode {
                    mode: AckMode::Detached,
                },
            ],
        })
        .expect("ordered junction alter should apply");

    assert_eq!(
        junction.from.from,
        vec![named("incoming_a"), named("incoming_b")]
    );
    assert_eq!(junction.from.r#where[0].where_clause, false_expression);
    assert_eq!(
        junction.filter_where,
        Some(Expression::Literal(Literal::Bool(false)))
    );
    assert_eq!(
        junction
            .materialized_state
            .iter()
            .map(|dependency| dependency.relay.clone())
            .collect::<Vec<_>>(),
        vec![named("profiles"), named("accounts")]
    );
    assert_eq!(
        junction.materialized_state[0].policy,
        MaterializedStatePolicy::RequiredSkip
    );
    assert_eq!(
        junction
            .output_routes
            .routes
            .iter()
            .map(|route| route.relay.clone())
            .collect::<Vec<_>>(),
        vec![named("accepted"), named("audit")]
    );
    assert_eq!(junction.output_routes.routes[0], replacement);
    assert_eq!(junction.mode, AckMode::Detached);
}

#[test]
fn junction_alter_reports_each_typed_lookup_and_last_element_error() {
    let base = CreateJunction {
        name: named("route_events"),
        from: ProcessorInputs::single(named("incoming")),
        output_routes: ProcessorOutputs::single(named("accepted")),
        branched_by: BranchSelection::unbranched(),
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: vec![MaterializedStateDependency {
            relay: named("profiles"),
            policy: MaterializedStatePolicy::RequiredWait,
        }],
    };
    let true_expression = Expression::Literal(Literal::Bool(true));
    let cases = vec![
        (
            AlterJunction {
                junction: named("other"),
                operations: Vec::new(),
            },
            AlterJunctionError::JunctionNameMismatch {
                stored: named("route_events"),
                requested: named("other"),
            },
        ),
        (
            AlterJunction {
                junction: named("route_events"),
                operations: vec![AlterProcessorOperation::AddFrom {
                    relay: named("incoming"),
                    where_clause: None,
                }],
            },
            AlterJunctionError::Processor(AlterProcessorError::InputAlreadyExists {
                relay: named("incoming"),
            }),
        ),
        (
            AlterJunction {
                junction: named("route_events"),
                operations: vec![AlterProcessorOperation::DropFrom {
                    relay: named("missing"),
                }],
            },
            AlterJunctionError::Processor(AlterProcessorError::InputNotFound {
                relay: named("missing"),
            }),
        ),
        (
            AlterJunction {
                junction: named("route_events"),
                operations: vec![AlterProcessorOperation::AlterFromDropWhere {
                    relay: named("incoming"),
                }],
            },
            AlterJunctionError::Processor(AlterProcessorError::InputWhereNotConfigured {
                relay: named("incoming"),
            }),
        ),
        (
            AlterJunction {
                junction: named("route_events"),
                operations: vec![AlterProcessorOperation::DropFrom {
                    relay: named("incoming"),
                }],
            },
            AlterJunctionError::Processor(AlterProcessorError::CannotDropLastInput),
        ),
        (
            AlterJunction {
                junction: named("route_events"),
                operations: vec![AlterProcessorOperation::AddMaterializedState {
                    dependency: MaterializedStateDependency {
                        relay: named("profiles"),
                        policy: MaterializedStatePolicy::RequiredSkip,
                    },
                }],
            },
            AlterJunctionError::Processor(
                AlterProcessorError::MaterializedStateAlreadyConfigured {
                    relay: named("profiles"),
                },
            ),
        ),
        (
            AlterJunction {
                junction: named("route_events"),
                operations: vec![AlterProcessorOperation::AlterMaterializedState {
                    relay: named("missing"),
                    policy: MaterializedStatePolicy::RequiredSkip,
                }],
            },
            AlterJunctionError::Processor(AlterProcessorError::MaterializedStateNotConfigured {
                relay: named("missing"),
            }),
        ),
        (
            AlterJunction {
                junction: named("route_events"),
                operations: vec![AlterProcessorOperation::ReplaceRoute {
                    route: ProcessorOutput::new(named("missing")),
                }],
            },
            AlterJunctionError::Processor(AlterProcessorError::RouteTargetNotFound {
                relay: named("missing"),
            }),
        ),
        (
            AlterJunction {
                junction: named("route_events"),
                operations: vec![AlterProcessorOperation::DropRoute {
                    relay: named("accepted"),
                }],
            },
            AlterJunctionError::Processor(AlterProcessorError::CannotDropLastRoute),
        ),
    ];

    for (alter, expected) in cases {
        let mut candidate = base.clone();
        assert_eq!(
            candidate
                .apply_alter(&alter)
                .expect_err("the alteration is rejected")
                .current_context(),
            &expected
        );
        assert_eq!(candidate, base);
    }

    let mut with_where = base.clone();
    with_where
        .apply_alter(&AlterJunction {
            junction: named("route_events"),
            operations: vec![AlterProcessorOperation::AlterFromSetWhere {
                relay: named("incoming"),
                where_clause: true_expression,
            }],
        })
        .expect("set WHERE should succeed");
}

fn deduplicator() -> CreateDeduplicator {
    CreateDeduplicator {
        name: named("dedup_events"),
        from: ProcessorInputs::single(named("incoming")),
        output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(named("outgoing"))]),
        branched_by: BranchSelection::unbranched(),
        deduplicate_on: vec![Expression::Literal(Literal::I64(1))],
        max_time: "10m".to_string(),
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: Vec::new(),
    }
}

fn reorderer() -> CreateReorderer {
    CreateReorderer {
        name: named("order_events"),
        from: ProcessorInputs::single(named("incoming")),
        output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(named("outgoing"))]),
        branched_by: BranchSelection::unbranched(),
        order_by: vec![Expression::Literal(Literal::I64(1))],
        max_time: "10m".to_string(),
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: Vec::new(),
    }
}

#[test]
fn deduplicator_alter_applies_common_and_specific_operations_in_written_order() {
    let mut candidate = deduplicator();
    candidate
        .apply_alter(&AlterDeduplicator {
            deduplicator: named("dedup_events"),
            operations: vec![
                AlterDeduplicatorOperation::Processor(Box::new(AlterProcessorOperation::AddFrom {
                    relay: named("secondary"),
                    where_clause: Some(Expression::Literal(Literal::Bool(true))),
                })),
                AlterDeduplicatorOperation::SetDeduplicateOn {
                    expressions: vec![Expression::Literal(Literal::I64(2))],
                },
                AlterDeduplicatorOperation::SetDeduplicateOn {
                    expressions: vec![Expression::Literal(Literal::I64(3))],
                },
                AlterDeduplicatorOperation::SetMaxTime {
                    max_time: "1m".to_string(),
                },
                AlterDeduplicatorOperation::SetMaxTime {
                    max_time: "2m".to_string(),
                },
                AlterDeduplicatorOperation::Processor(Box::new(AlterProcessorOperation::SetMode {
                    mode: AckMode::Detached,
                })),
            ],
        })
        .expect("deduplicator ALTER should apply");

    assert_eq!(
        candidate.from.from,
        vec![named("incoming"), named("secondary")]
    );
    assert_eq!(
        candidate.deduplicate_on,
        vec![Expression::Literal(Literal::I64(3))]
    );
    assert_eq!(candidate.max_time, "2m");
    assert_eq!(candidate.mode, AckMode::Detached);
}

#[test]
fn reorderer_alter_applies_common_and_specific_operations_in_written_order() {
    let mut candidate = reorderer();
    candidate
        .apply_alter(&AlterReorderer {
            reorderer: named("order_events"),
            operations: vec![
                AlterReordererOperation::SetOrderBy {
                    expressions: vec![Expression::Literal(Literal::I64(2))],
                },
                AlterReordererOperation::SetOrderBy {
                    expressions: vec![Expression::Literal(Literal::I64(3))],
                },
                AlterReordererOperation::SetMaxTime {
                    max_time: "1m".to_string(),
                },
                AlterReordererOperation::SetMaxTime {
                    max_time: "2m".to_string(),
                },
                AlterReordererOperation::Processor(Box::new(
                    AlterProcessorOperation::SetFilterWhere {
                        where_clause: Expression::Literal(Literal::Bool(true)),
                    },
                )),
            ],
        })
        .expect("reorderer ALTER should apply");

    assert_eq!(
        candidate.order_by,
        vec![Expression::Literal(Literal::I64(3))]
    );
    assert_eq!(candidate.max_time, "2m");
    assert_eq!(
        candidate.filter_where,
        Some(Expression::Literal(Literal::Bool(true)))
    );
}

#[test]
fn processor_alters_are_atomic_and_report_typed_target_errors() {
    let mut deduplicator = deduplicator();
    let original = deduplicator.clone();
    assert_eq!(
        deduplicator
            .apply_alter(&AlterDeduplicator {
                deduplicator: named("dedup_events"),
                operations: vec![
                    AlterDeduplicatorOperation::SetMaxTime {
                        max_time: "1s".to_string(),
                    },
                    AlterDeduplicatorOperation::Processor(Box::new(
                        AlterProcessorOperation::DropRoute {
                            relay: named("missing"),
                        },
                    )),
                ],
            })
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterDeduplicatorError::Processor(AlterProcessorError::RouteTargetNotFound {
            relay: named("missing"),
        })
    );
    assert_eq!(deduplicator, original);
    assert_eq!(
        deduplicator
            .apply_alter(&AlterDeduplicator {
                deduplicator: named("other"),
                operations: Vec::new(),
            })
            .expect_err("the alteration names another deduplicator")
            .current_context(),
        &AlterDeduplicatorError::DeduplicatorNameMismatch {
            stored: named("dedup_events"),
            requested: named("other"),
        }
    );

    let mut reorderer = reorderer();
    let original = reorderer.clone();
    assert_eq!(
        reorderer
            .apply_alter(&AlterReorderer {
                reorderer: named("other"),
                operations: Vec::new(),
            })
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterReordererError::ReordererNameMismatch {
            stored: named("order_events"),
            requested: named("other"),
        }
    );
    assert_eq!(reorderer, original);
}

#[test]
fn reingestor_alter_is_ordered_atomic_and_rejects_node_branching() {
    let mut reingestor = CreateReingestor {
        name: named("repartition"),
        from: ProcessorInputs::single(named("incoming")),
        output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(named("outgoing"))]),
        mode: AckMode::Attached,
        materialized_state: Vec::new(),
        filter_where: None,
    };
    reingestor
        .apply_alter(&AlterReingestor {
            reingestor: named("repartition"),
            operations: vec![
                AlterProcessorOperation::SetMode {
                    mode: AckMode::Detached,
                },
                AlterProcessorOperation::AddFrom {
                    relay: named("secondary"),
                    where_clause: Some(Expression::Literal(Literal::Bool(true))),
                },
                AlterProcessorOperation::SetFilterWhere {
                    where_clause: Expression::Literal(Literal::Bool(true)),
                },
            ],
        })
        .expect("reingestor alter should apply");
    assert_eq!(reingestor.mode, AckMode::Detached);
    assert_eq!(reingestor.from.from.len(), 2);
    assert!(reingestor.filter_where.is_some());

    let before = reingestor.clone();
    assert_eq!(
        reingestor
            .apply_alter(&AlterReingestor {
                reingestor: named("repartition"),
                operations: vec![
                    AlterProcessorOperation::SetMode {
                        mode: AckMode::Attached,
                    },
                    AlterProcessorOperation::SetBranching {
                        branching: BranchSelection::unbranched(),
                    },
                ],
            })
            .expect_err("the alteration is rejected")
            .current_context(),
        &AlterReingestorError::Processor(AlterProcessorError::BranchingUnsupported)
    );
    assert_eq!(reingestor, before, "failed ALTER must not partially apply");
    assert_eq!(
        reingestor
            .apply_alter(&AlterReingestor {
                reingestor: named("other"),
                operations: Vec::new(),
            })
            .expect_err("the alteration names another reingestor")
            .current_context(),
        &AlterReingestorError::ReingestorNameMismatch {
            stored: named("repartition"),
            requested: named("other"),
        }
    );
}

fn tensor(dimensions: Vec<InferencerTensorDimension>) -> InferencerTensorSchema {
    InferencerTensorSchema {
        representation: InferencerTensorRepresentation::Dense,
        element_type: InferencerTensorElementType::F32,
        dimensions,
    }
}

fn inferencer(
    input: Vec<InferencerTensorDimension>,
    output: Vec<InferencerTensorDimension>,
) -> CreateInferencer {
    CreateInferencer {
        name: named("scorer"),
        from: ProcessorInputs::single(named("features")),
        output_routes: ProcessorOutputs::new(vec![ProcessorOutput::new(named("scores"))]),
        branched_by: BranchSelection::unbranched(),
        resource: named("models"),
        resource_version: 1,
        file: "score.onnx".to_string(),
        inputs: vec![InferencerTensorMapping {
            tensor: "input".to_string(),
            schema: tensor(input),
            expression: Expression::Literal(Literal::Bool(true)),
        }],
        output_schema: vec![InferencerTensorDeclaration {
            tensor: "output".to_string(),
            schema: tensor(output),
        }],
        mode: AckMode::Attached,
        filter_where: None,
        materialized_state: Vec::new(),
    }
}

#[test]
fn inferencer_execution_mode_follows_the_batch_axes_of_every_tensor() {
    let one = InferencerTensorDimension::Fixed(nonzero!(1u32));
    let batch = InferencerTensorDimension::Batch;

    let per_message = inferencer(vec![one], vec![one]);
    assert_eq!(
        per_message
            .execution_mode()
            .expect("no tensor declares a batch axis"),
        InferencerExecutionMode::PerMessage
    );
    let batched = inferencer(vec![batch, one], vec![batch, one]);
    assert_eq!(
        batched
            .execution_mode()
            .expect("every tensor declares one batch axis"),
        InferencerExecutionMode::Batched
    );

    let widest = InferencerTensorDimension::Fixed(nonzero!(4_294_967_295u32));
    let cases = [
        (
            inferencer(vec![batch, batch], vec![one]),
            InferencerTensorSchemaError::MultipleBatchAxes {
                tensor: "input".to_string(),
            },
        ),
        (
            inferencer(vec![one], vec![widest, widest, widest]),
            InferencerTensorSchemaError::ElementCountOverflow {
                tensor: "output".to_string(),
            },
        ),
        (
            inferencer(vec![batch, one], vec![one]),
            InferencerTensorSchemaError::MixedExecutionModes,
        ),
    ];
    for (processor, expected) in cases {
        let error = processor
            .execution_mode()
            .expect_err("the tensor schemas admit no execution mode");
        assert_eq!(error.current_context(), &expected);
    }
}
