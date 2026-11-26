use meticulous::ResultExt as _;
use nervix_models::{
    DomainSchedule, IngestorName, ModelKind, ModelName, NodeRef, RelayName, ScheduledNode,
    SchemaFingerprint,
};

use super::SwapStopOrder;
use crate::registry::test_fixtures::example_graph_models;

/// The relays, schemas and endpoint every shape below builds on.
const BASE: &str = "
    CREATE UNPACED DOMAIN stops;
    CREATE SCHEMA rec ( k STRING, v I64 );
    CREATE WIRE JSON SCHEMA rec_wire MODE STRICT ( k string, v integer );
    CREATE WIRE JSON CODEC rec_codec FROM WIRE SCHEMA rec_wire TO SCHEMA rec;
    CREATE VHOST edge stops.example.com;
    CREATE HTTP ENDPOINT ingress ON edge PATH '/in';
    CREATE HTTP ENDPOINT other_ingress ON edge PATH '/other';
    CREATE RELAY a SCHEMA rec UNBRANCHED;
    CREATE RELAY b SCHEMA rec UNBRANCHED;
    CREATE RELAY c SCHEMA rec UNBRANCHED;
    CREATE RELAY m SCHEMA rec UNBRANCHED;
    CREATE RELAY failed SCHEMA rec UNBRANCHED;
    CREATE RELAY sa SCHEMA rec UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
";

fn schedule(name: &str, shape: &str) -> DomainSchedule {
    let source = format!("{BASE}{shape}");
    let (domain, models) = example_graph_models(name, &source);
    DomainSchedule::new(
        domain,
        models
            .into_iter()
            .map(|model| ScheduledNode::new(model, SchemaFingerprint::from_digest([0; 32]))),
        Vec::new(),
    )
}

fn node(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(
        kind,
        ModelName::try_from(name).assured("the tests name their nodes validly"),
    )
}

fn relay(name: &str) -> RelayName {
    RelayName::try_from(name).assured("the tests name their relays validly")
}

fn ingestor(name: &str) -> IngestorName {
    IngestorName::try_from(name).assured("the tests name their ingestors validly")
}

/// An HTTP endpoint ingestor named `name` reading `endpoint` and delivering into `relay`.
fn ingestor_into(name: &str, endpoint: &str, relay: &str) -> String {
    format!(
        "CREATE HTTP ENDPOINT INGESTOR {name} FROM {endpoint} MODE NO ACK ON QUIESCE BUFFER MAX \
         SIZE 1MiB RESPOND IMMEDIATE DECODE USING rec_codec TO {relay} INHERIT ALL UNBRANCHED \
         FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
    )
}

#[test]
fn reingestors_stop_in_the_order_rows_flow_through_them_whatever_their_names() {
    let shape = format!(
        "{} CREATE REINGESTOR z_upstream FROM a TO b INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON \
         MESSAGE ERROR LOG; CREATE JUNCTION between FROM b UNBRANCHED TO c INHERIT ALL FLUSH \
         IMMEDIATE ON MESSAGE ERROR LOG; CREATE REINGESTOR a_downstream FROM c TO m INHERIT ALL \
         UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        ingestor_into("intake", "ingress", "a"),
    );
    let existing = schedule("order", &shape);
    let gated = [
        node(ModelKind::Reingestor, "a_downstream"),
        node(ModelKind::Reingestor, "z_upstream"),
    ];

    let order = SwapStopOrder::plan(&existing, &gated, &[relay("a"), relay("c")]);

    assert_eq!(
        order.reingestors(),
        &[
            node(ModelKind::Reingestor, "z_upstream"),
            node(ModelKind::Reingestor, "a_downstream"),
        ],
        "the reingestor whose rows reach the other one stops first"
    );
}

#[test]
fn only_the_reingestors_the_swap_replaces_or_moves_are_ordered() {
    let shape = "CREATE REINGESTOR kept FROM a TO b INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON \
                 MESSAGE ERROR LOG; CREATE REINGESTOR replaced FROM b TO c INHERIT ALL UNBRANCHED \
                 FLUSH IMMEDIATE ON MESSAGE ERROR LOG;";
    let existing = schedule("selection", shape);
    let gated = [node(ModelKind::Reingestor, "replaced")];

    let order = SwapStopOrder::plan(&existing, &gated, &[relay("b")]);

    assert_eq!(
        order.reingestors(),
        &[node(ModelKind::Reingestor, "replaced")]
    );
}

#[test]
fn an_ingestor_whose_rows_reach_a_held_relay_through_running_nodes_stops_before_the_holds() {
    let shape = format!(
        "{} {} CREATE JUNCTION forward FROM a UNBRANCHED TO b INHERIT ALL FLUSH IMMEDIATE ON \
         MESSAGE ERROR LOG; CREATE JUNCTION replaced FROM b UNBRANCHED TO c INHERIT ALL FLUSH \
         IMMEDIATE ON MESSAGE ERROR LOG;",
        ingestor_into("upstream", "ingress", "a"),
        ingestor_into("elsewhere", "other_ingress", "m"),
    );
    let existing = schedule("reach", &shape);
    let gated = [node(ModelKind::Junction, "replaced")];

    let order = SwapStopOrder::plan(&existing, &gated, &[relay("b")]);

    assert!(
        order.stops_before_holds(&ingestor("upstream")),
        "its rows reach the held relay through the junction that keeps running"
    );
    assert!(
        !order.stops_before_holds(&ingestor("elsewhere")),
        "its rows reach no held relay, so its intake ends no sooner than today"
    );
}

#[test]
fn an_error_route_carries_rows_toward_a_held_relay() {
    let shape = format!(
        "{} CREATE JUNCTION checked FROM a UNBRANCHED TO b INHERIT ALL FLUSH IMMEDIATE ON MESSAGE \
         ERROR SEND TO failed SET k = input.k, v = input.v; CREATE JUNCTION replaced FROM failed \
         UNBRANCHED TO c INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        ingestor_into("upstream", "ingress", "a"),
    );
    let existing = schedule("error_route", &shape);
    let gated = [node(ModelKind::Junction, "replaced")];

    let order = SwapStopOrder::plan(&existing, &gated, &[relay("failed")]);

    assert!(order.stops_before_holds(&ingestor("upstream")));
}

#[test]
fn a_correlation_timeout_carries_only_its_own_sides_rows_toward_a_held_relay() {
    let shape = format!(
        "{} {} CREATE CORRELATOR pairs LEFT FROM a RIGHT FROM b CORRELATE WHERE left.k = right.k \
         MATCH EARLIEST MAX TIME 30s ON CORRELATION TIMEOUT SEND TO failed, DROP UNBRANCHED TO c \
         SET k = left.k, v = right.v FLUSH IMMEDIATE ON MESSAGE ERROR LOG; CREATE JUNCTION \
         replaced FROM failed UNBRANCHED TO m INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        ingestor_into("left_feed", "ingress", "a"),
        ingestor_into("right_feed", "other_ingress", "b"),
    );
    let existing = schedule("correlation_timeout", &shape);
    let gated = [node(ModelKind::Junction, "replaced")];

    let timeout_held = SwapStopOrder::plan(&existing, &gated, &[relay("failed")]);
    let output_held = SwapStopOrder::plan(&existing, &gated, &[relay("c")]);

    assert!(
        timeout_held.stops_before_holds(&ingestor("left_feed")),
        "the left side's unmatched rows go to the held relay"
    );
    assert!(
        !timeout_held.stops_before_holds(&ingestor("right_feed")),
        "the right side's unmatched rows are dropped and its matched rows reach no held relay"
    );
    assert!(
        output_held.stops_before_holds(&ingestor("left_feed"))
            && output_held.stops_before_holds(&ingestor("right_feed")),
        "every matched pair carries rows of both sides into the correlator's route"
    );
}

#[test]
fn a_read_of_materialized_state_carries_no_rows_toward_a_held_relay() {
    let shape = format!(
        "{} CREATE GENERATOR sampler USING MATERIALIZED STATE sa EACH 1h UNBRANCHED TO c SET k = \
         relay_state.sa.k, v = relay_state.sa.v FLUSH IMMEDIATE ON MESSAGE ERROR LOG; CREATE \
         JUNCTION replaced FROM c UNBRANCHED TO m INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR \
         LOG;",
        ingestor_into("state_writer", "ingress", "sa"),
    );
    let existing = schedule("state_read", &shape);
    let gated = [node(ModelKind::Junction, "replaced")];

    let order = SwapStopOrder::plan(&existing, &gated, &[relay("c")]);

    assert!(
        !order.stops_before_holds(&ingestor("state_writer")),
        "the generator reads the state the ingestor writes, so nothing the ingestor delivers \
         waits for the held relay the generator writes"
    );
}
