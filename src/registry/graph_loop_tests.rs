use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{
    DomainSchedule, ModelKind, ModelName, NodeRef, ScheduledNode, SchemaFingerprint,
};

use super::{DataHop, GraphLoop, LoopStep};
use crate::registry::{
    error::RegistryError, graph::ActiveGraph, test_fixtures::example_graph_models,
};

/// Every shape below adds its nodes to these relays.
const RELAYS: &str = "
    CREATE UNPACED DOMAIN loops;
    CREATE SCHEMA rec ( k STRING, v I64 );
    CREATE RELAY a SCHEMA rec UNBRANCHED;
    CREATE RELAY b SCHEMA rec UNBRANCHED;
    CREATE RELAY c SCHEMA rec UNBRANCHED;
    CREATE RELAY m SCHEMA rec UNBRANCHED;
    CREATE RELAY z SCHEMA rec UNBRANCHED;
    CREATE RELAY sa SCHEMA rec UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
";

fn validate(name: &str, shape: &str) -> Result<ActiveGraph, Report<RegistryError>> {
    let source = format!("{RELAYS}{shape}");
    let (domain, models) = example_graph_models(name, &source);
    let schedule = DomainSchedule::new(
        domain,
        models
            .into_iter()
            .map(|model| ScheduledNode::new(model, SchemaFingerprint::from_digest([0; 32]))),
        Vec::new(),
    );
    ActiveGraph::from_scheduled_models(&schedule)
}

/// The loop validation refuses `shape` for, rendered as the refusal names it.
fn refused_loop(name: &str, shape: &str) -> GraphLoop {
    let error = match validate(name, shape) {
        Ok(_) => panic!("{name}: the graph was accepted, though it has a loop"),
        Err(error) => error,
    };
    match error.current_context() {
        RegistryError::GraphLoop { domain, graph_loop } => {
            assert_eq!(
                domain.as_str(),
                "loops",
                "{name}: the refusal names its domain"
            );
            graph_loop.clone()
        }
        other => panic!("{name}: expected a loop refusal, got {other:?}"),
    }
}

fn assert_refused(name: &str, shape: &str, expected: &str) {
    let graph_loop = refused_loop(name, shape);
    assert_eq!(graph_loop.to_string(), expected, "{name}");
}

fn step(kind: ModelKind, name: &str, hop: DataHop) -> LoopStep {
    LoopStep {
        node: NodeRef::new(
            kind,
            ModelName::try_from(name).assured("the tests name their nodes validly"),
        ),
        hop,
    }
}

#[test]
fn a_node_writing_into_the_relay_it_reads_is_a_loop() {
    assert_refused(
        "junction",
        "CREATE JUNCTION j FROM a UNBRANCHED TO a INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;",
        "RELAY 'a' → JUNCTION 'j' → RELAY 'a'",
    );
    assert_refused(
        "reingestor",
        "CREATE REINGESTOR r FROM a TO a INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;",
        "RELAY 'a' → REINGESTOR 'r' → RELAY 'a'",
    );
}

#[test]
fn nodes_feeding_each_other_are_a_loop() {
    assert_refused(
        "two junctions",
        "CREATE JUNCTION j1 FROM a UNBRANCHED TO b INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;
         CREATE JUNCTION j2 FROM b UNBRANCHED TO a INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;",
        "RELAY 'a' → JUNCTION 'j1' → RELAY 'b' → JUNCTION 'j2' → RELAY 'a'",
    );
    assert_refused(
        "two reingestors",
        "CREATE REINGESTOR r1 FROM a TO b INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;
         CREATE REINGESTOR r2 FROM b TO a INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;",
        "RELAY 'a' → REINGESTOR 'r1' → RELAY 'b' → REINGESTOR 'r2' → RELAY 'a'",
    );
    assert_refused(
        "a junction and a reingestor",
        "CREATE JUNCTION j FROM a UNBRANCHED TO b INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;
         CREATE REINGESTOR r FROM b TO a INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;",
        "RELAY 'a' → JUNCTION 'j' → RELAY 'b' → REINGESTOR 'r' → RELAY 'a'",
    );
    assert_refused(
        "a deduplicator and a junction",
        "CREATE DEDUPLICATOR d FROM a DEDUPLICATE ON input.k MAX TIME 10m UNBRANCHED
           TO b INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
         CREATE JUNCTION j FROM b UNBRANCHED TO a INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        "RELAY 'a' → DEDUPLICATOR 'd' → RELAY 'b' → JUNCTION 'j' → RELAY 'a'",
    );
}

#[test]
fn an_error_route_back_into_the_input_is_a_loop() {
    assert_refused(
        "error route",
        "CREATE JUNCTION j FROM a UNBRANCHED TO b INHERIT ALL FLUSH IMMEDIATE
           ON MESSAGE ERROR SEND TO a SET k = input.k, v = input.v;",
        "RELAY 'a' → JUNCTION 'j' →[error route] RELAY 'a'",
    );
}

#[test]
fn a_correlation_timeout_back_into_an_input_is_a_loop() {
    assert_refused(
        "correlation timeout",
        "CREATE CORRELATOR co LEFT FROM a RIGHT FROM b
           CORRELATE WHERE left.k = right.k MATCH EARLIEST MAX TIME 30s
           ON CORRELATION TIMEOUT SEND TO a, DROP
           UNBRANCHED
           TO c SET k = left.k, v = right.v FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        "RELAY 'a' → CORRELATOR 'co' →[correlation timeout] RELAY 'a'",
    );
}

#[test]
fn writing_into_the_relay_whose_state_a_node_reads_is_a_loop() {
    assert_refused(
        "junction state",
        "CREATE JUNCTION j FROM a UNBRANCHED USING MATERIALIZED STATE sa REQUIRED SKIP
           TO sa INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        "RELAY 'sa' →[state read] JUNCTION 'j' → RELAY 'sa'",
    );
    assert_refused(
        "generator state",
        "CREATE GENERATOR g USING MATERIALIZED STATE sa EACH 1s UNBRANCHED
           TO sa SET k = relay_state.sa.k, v = relay_state.sa.v FLUSH IMMEDIATE
           ON MESSAGE ERROR LOG;",
        "RELAY 'sa' →[state read] GENERATOR 'g' → RELAY 'sa'",
    );
}

#[test]
fn a_loop_reads_from_its_first_relay_with_every_hop() {
    let graph_loop = refused_loop(
        "state across nodes",
        "CREATE GENERATOR g USING MATERIALIZED STATE sa EACH 1s UNBRANCHED
           TO b SET k = relay_state.sa.k, v = relay_state.sa.v FLUSH IMMEDIATE
           ON MESSAGE ERROR LOG;
         CREATE JUNCTION j FROM b UNBRANCHED TO sa INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;",
    );
    assert_eq!(
        graph_loop.steps(),
        [
            step(ModelKind::Relay, "b", DataHop::Route),
            step(ModelKind::Junction, "j", DataHop::Route),
            step(ModelKind::Relay, "sa", DataHop::StateRead),
            step(ModelKind::Generator, "g", DataHop::Route),
        ]
    );
    assert_eq!(
        graph_loop.to_string(),
        "RELAY 'b' → JUNCTION 'j' → RELAY 'sa' →[state read] GENERATOR 'g' → RELAY 'b'"
    );
}

#[test]
fn the_search_reports_a_loop_from_its_first_relay_wherever_it_entered_it() {
    // The search starts at the junctions, which come first in node order, and enters the loop at
    // relay z; the refusal still starts at relay m.
    assert_refused(
        "rotation",
        "CREATE JUNCTION j1 FROM z UNBRANCHED TO m INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;
         CREATE JUNCTION j2 FROM m UNBRANCHED TO z INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;",
        "RELAY 'm' → JUNCTION 'j2' → RELAY 'z' → JUNCTION 'j1' → RELAY 'm'",
    );
}

#[test]
fn a_route_names_the_hop_when_an_error_route_joins_the_same_nodes() {
    assert_refused(
        "route and error route",
        "CREATE JUNCTION j FROM a UNBRANCHED TO a INHERIT ALL FLUSH IMMEDIATE
           ON MESSAGE ERROR SEND TO a SET k = input.k, v = input.v;",
        "RELAY 'a' → JUNCTION 'j' → RELAY 'a'",
    );
}

#[test]
fn data_that_fans_out_and_in_without_coming_back_is_no_loop() {
    validate(
        "fan out and in",
        "CREATE JUNCTION j1 FROM a UNBRANCHED TO b INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;
         CREATE JUNCTION j2 FROM a UNBRANCHED TO b INHERIT ALL FLUSH IMMEDIATE
           ON MESSAGE ERROR SEND TO c SET k = input.k, v = input.v;
         CREATE JUNCTION j3 FROM b UNBRANCHED USING MATERIALIZED STATE sa REQUIRED SKIP
           TO c INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
         CREATE REINGESTOR r FROM c TO m INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;
         CREATE GENERATOR g USING MATERIALIZED STATE sa EACH 1s UNBRANCHED
           TO z SET k = relay_state.sa.k, v = relay_state.sa.v FLUSH IMMEDIATE
           ON MESSAGE ERROR LOG;",
    )
    .unwrap_or_else(|error| panic!("a graph without a loop validates: {error:?}"));
}
