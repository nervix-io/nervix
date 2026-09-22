use std::collections::BTreeSet;

use super::*;

/// An edge named the way the live graph names its edges: by its endpoints and what it carries.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TestEdge {
    source: String,
    target: String,
    kind: LayoutEdgeKind,
}

type TestLayout = Layout<String, TestEdge, String>;

fn card(id: &str) -> LayoutItem<String, String> {
    LayoutItem {
        id: id.to_string(),
        width: 176,
        height: 64,
        relay: false,
        branch: None,
    }
}

fn pill(id: &str) -> LayoutItem<String, String> {
    LayoutItem {
        id: id.to_string(),
        width: 96,
        height: 26,
        relay: true,
        branch: None,
    }
}

fn in_group(mut item: LayoutItem<String, String>, branch: &str) -> LayoutItem<String, String> {
    item.branch = Some(branch.to_string());
    item
}

fn edge(source: &str, target: &str, kind: LayoutEdgeKind) -> LayoutEdge<String, TestEdge> {
    LayoutEdge {
        id: TestEdge {
            source: source.to_string(),
            target: target.to_string(),
            kind,
        },
        source: source.to_string(),
        target: target.to_string(),
        kind,
        badge: kind == LayoutEdgeKind::Flow,
    }
}

fn flow(source: &str, target: &str) -> LayoutEdge<String, TestEdge> {
    edge(source, target, LayoutEdgeKind::Flow)
}

fn dependency(source: &str, target: &str) -> LayoutEdge<String, TestEdge> {
    edge(source, target, LayoutEdgeKind::Dependency)
}

fn route<'a>(
    layout: &'a TestLayout,
    source: &str,
    target: &str,
    kind: LayoutEdgeKind,
) -> &'a RoutedEdge<String> {
    let id = TestEdge {
        source: source.to_string(),
        target: target.to_string(),
        kind,
    };
    layout
        .edges
        .get(&id)
        .unwrap_or_else(|| panic!("edge {source} -> {target} ({kind:?}) must be routed"))
}

/// A graph to lay out: its items and the edges between them.
struct TestGraph {
    items: Vec<LayoutItem<String, String>>,
    edges: Vec<LayoutEdge<String, TestEdge>>,
}

fn quickstart() -> TestGraph {
    let items = vec![
        card("client:kafka_local"),
        card("ingestor:kafka_orders"),
        pill("relay:orders"),
        card("junction:route_orders"),
        card("emitter:redis_orders"),
        pill("relay:high_value_orders"),
        pill("relay:routine_orders"),
        card("emitter:redis_high_value"),
        card("client_sink:redis_local"),
    ];
    let edges = vec![
        flow("client:kafka_local", "ingestor:kafka_orders"),
        flow("ingestor:kafka_orders", "relay:orders"),
        flow("relay:orders", "junction:route_orders"),
        flow("relay:orders", "emitter:redis_orders"),
        flow("junction:route_orders", "relay:high_value_orders"),
        flow("junction:route_orders", "relay:routine_orders"),
        flow("relay:high_value_orders", "emitter:redis_high_value"),
        flow("emitter:redis_high_value", "client_sink:redis_local"),
        flow("emitter:redis_orders", "client_sink:redis_local"),
    ];
    TestGraph { items, edges }
}

fn segment_rect(start: (i32, i32), end: (i32, i32)) -> Rect {
    let x = start.0.min(end.0);
    let y = start.1.min(end.1);
    Rect {
        x,
        y,
        width: (start.0 - end.0).abs().max(1),
        height: (start.1 - end.1).abs().max(1),
    }
}

/// Every edge keeps clear of every item other than its own two endpoints.
fn assert_no_edge_crosses_an_item<I, E, G>(layout: &Layout<I, E, G>)
where
    I: Ord + std::fmt::Debug,
{
    for edge in layout.edges.values() {
        for window in edge.points.windows(2) {
            let segment = segment_rect(window[0], window[1]);
            for (id, rect) in &layout.items {
                if *id == edge.source || *id == edge.target {
                    continue;
                }
                assert!(
                    !segment.intersects(rect),
                    "edge {:?} -> {:?} crosses {id:?}",
                    edge.source,
                    edge.target
                );
            }
        }
    }
}

/// One straight stretch of a route, between two of its turns.
#[derive(Debug, Clone, Copy)]
struct Run {
    start: (i32, i32),
    end: (i32, i32),
}

impl Run {
    const fn horizontal(self) -> bool {
        self.start.1 == self.end.1
    }

    /// Whether two runs lie on one line and overlap along it.
    fn shares_a_line_with(self, other: Self) -> bool {
        let overlaps = |left: (i32, i32), right: (i32, i32)| {
            left.0.min(left.1) < right.0.max(right.1) && right.0.min(right.1) < left.0.max(left.1)
        };
        if self.horizontal() && other.horizontal() && self.start.1 == other.start.1 {
            return overlaps((self.start.0, self.end.0), (other.start.0, other.end.0));
        }
        if !self.horizontal() && !other.horizontal() && self.start.0 == other.start.0 {
            return overlaps((self.start.1, self.end.1), (other.start.1, other.end.1));
        }
        false
    }
}

/// The straight stretches of a route, without its turns.
fn runs(points: &[(i32, i32)]) -> Vec<Run> {
    points
        .windows(2)
        .map(|window| Run {
            start: window[0],
            end: window[1],
        })
        .collect()
}

#[test]
fn every_item_sits_right_of_what_feeds_it() {
    let TestGraph { items, edges } = quickstart();
    let layout = TestLayout::build(&items, &edges);
    for edge in &edges {
        let source = layout.items[&edge.source];
        let target = layout.items[&edge.target];
        assert!(
            source.right() <= target.x,
            "{} should sit left of {}",
            edge.source,
            edge.target
        );
    }
}

#[test]
fn relays_never_share_a_column_with_processing_nodes() {
    let TestGraph { items, edges } = quickstart();
    let layout = TestLayout::build(&items, &edges);
    for item in &items {
        for other in &items {
            if item.relay == other.relay {
                continue;
            }
            let left = layout.items[&item.id];
            let right = layout.items[&other.id];
            assert!(
                left.right() <= right.x || right.right() <= left.x,
                "{} and {} must not share a column",
                item.id,
                other.id
            );
        }
    }
}

#[test]
fn no_edge_crosses_an_item() {
    let TestGraph { items, edges } = quickstart();
    let layout = TestLayout::build(&items, &edges);
    assert_no_edge_crosses_an_item(&layout);
}

#[test]
fn no_badge_covers_an_item_or_another_badge() {
    let TestGraph { items, edges } = quickstart();
    let layout = TestLayout::build(&items, &edges);
    let badges = layout
        .edges
        .values()
        .filter_map(|edge| edge.badge)
        .collect::<Vec<_>>();
    for (index, badge) in badges.iter().enumerate() {
        for rect in layout.items.values() {
            assert!(!badge.intersects(rect), "badge {badge:?} covers an item");
        }
        for other in badges.iter().skip(index + 1) {
            assert!(
                !badge.intersects(other),
                "badges {badge:?} and {other:?} overlap"
            );
        }
    }
}

#[test]
fn a_straight_pipeline_is_drawn_as_one_line() {
    let items = vec![
        card("client:c"),
        card("ingestor:i"),
        pill("relay:r"),
        card("emitter:e"),
    ];
    let edges = vec![
        flow("client:c", "ingestor:i"),
        flow("ingestor:i", "relay:r"),
        flow("relay:r", "emitter:e"),
    ];
    let layout = TestLayout::build(&items, &edges);
    let centres = items
        .iter()
        .map(|item| layout.items[&item.id].center_y())
        .collect::<Vec<_>>();
    assert!(
        centres.windows(2).all(|pair| pair[0] == pair[1]),
        "an unbranched pipeline must be collinear, got {centres:?}"
    );
    for edge in layout.edges.values() {
        assert_eq!(
            edge.points.len(),
            2,
            "chain edge {} -> {} should not bend",
            edge.source,
            edge.target
        );
        assert_eq!(edge.travel, EdgeTravel::Forward);
    }
}

#[test]
fn fan_out_leaves_through_distinct_ports() {
    let TestGraph { items, edges } = quickstart();
    let layout = TestLayout::build(&items, &edges);
    let departures = layout
        .edges
        .values()
        .filter(|edge| edge.source == "relay:orders")
        .map(|edge| edge.points[0].1)
        .collect::<BTreeSet<_>>();
    assert_eq!(departures.len(), 2, "fan-out must not share one port");
    let mut heights = departures.into_iter().collect::<Vec<_>>();
    heights.sort_unstable();
    assert!(heights[1] - heights[0] >= PORT_PITCH);
}

#[test]
fn a_state_dependency_does_not_push_the_record_flow_off_axis() {
    let items = vec![
        pill("relay:in"),
        pill("relay:state"),
        card("junction:enrich"),
        pill("relay:out"),
    ];
    let edges = vec![
        flow("relay:in", "junction:enrich"),
        edge("relay:state", "junction:enrich", LayoutEdgeKind::State),
        flow("junction:enrich", "relay:out"),
    ];
    let layout = TestLayout::build(&items, &edges);
    let arrival = route(&layout, "relay:in", "junction:enrich", LayoutEdgeKind::Flow);
    let junction = layout.items["junction:enrich"];
    assert_eq!(
        arrival.points.last().expect("edge must arrive").1,
        junction.center_y(),
        "the record-carrying edge keeps the centre port"
    );
    assert_eq!(
        layout.items["relay:in"].center_y(),
        junction.center_y(),
        "a state dependency must not bend the pipeline"
    );
}

#[test]
fn identical_topology_produces_identical_geometry() {
    let TestGraph { items, edges } = quickstart();
    let first = TestLayout::build(&items, &edges);
    let second = TestLayout::build(&items, &edges);
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
}

#[test]
fn branch_group_bands_hold_members_and_nothing_else() {
    let items = vec![
        card("ingestor:source"),
        card("emitter:sink"),
        card("emitter:other"),
        in_group(pill("relay:branched"), "by_tenant"),
        in_group(card("junction:split"), "by_tenant"),
    ];
    let edges = vec![
        flow("ingestor:source", "relay:branched"),
        flow("relay:branched", "junction:split"),
        flow("junction:split", "emitter:sink"),
        flow("ingestor:source", "emitter:other"),
    ];
    let layout = TestLayout::build(&items, &edges);
    let group = layout
        .groups
        .iter()
        .find(|group| group.branch == "by_tenant")
        .expect("branch group must be drawn");
    for (id, rect) in &layout.items {
        let member = id == "relay:branched" || id == "junction:split";
        let inside = group.bands.iter().any(|band| band.intersects(rect));
        assert_eq!(inside, member, "{id} containment must match membership");
    }
}

#[test]
fn interleaved_branch_groups_keep_their_members_together() {
    // Listed in name order the two groups alternate in every column they share, so only the
    // arrangement keeps each group's members next to one another.
    let mut items = vec![card("ingestor:source")];
    let mut edges = Vec::new();
    for (index, branch) in [
        (1, "by_tenant"),
        (2, "by_user"),
        (3, "by_tenant"),
        (4, "by_user"),
    ] {
        let input = format!("relay:{index}_input");
        let processor = format!("junction:{index}_process");
        let output = format!("relay:{index}_output");
        items.push(in_group(pill(&input), branch));
        items.push(in_group(card(&processor), branch));
        items.push(in_group(pill(&output), branch));
        edges.push(flow("ingestor:source", &input));
        edges.push(flow(&input, &processor));
        edges.push(flow(&processor, &output));
    }
    // Crossing traffic pulls the members of each group apart before the groups are gathered.
    edges.push(flow("relay:1_input", "junction:4_process"));
    edges.push(flow("relay:4_input", "junction:1_process"));

    let layout = TestLayout::build(&items, &edges);
    assert_eq!(layout.groups.len(), 2);
    for group in &layout.groups {
        for item in &items {
            let rect = layout.items[&item.id];
            let member = item.branch.as_ref() == Some(&group.branch);
            let inside = group.bands.iter().any(|band| band.intersects(&rect));
            assert_eq!(
                inside, member,
                "{} containment in {} must match membership",
                item.id, group.branch
            );
        }
    }
    for band in &layout.groups[0].bands {
        for other in &layout.groups[1].bands {
            assert!(
                !band.intersects(other),
                "branch groups must not overlap: {band:?} and {other:?}"
            );
        }
    }
    assert_no_edge_crosses_an_item(&layout);
}

#[test]
fn a_feedback_loop_is_drawn_as_a_marked_return_path() {
    let items = vec![
        card("ingestor:source"),
        pill("relay:a"),
        card("reingestor:loop"),
    ];
    let edges = vec![
        flow("ingestor:source", "relay:a"),
        flow("relay:a", "reingestor:loop"),
        flow("reingestor:loop", "relay:a"),
    ];
    let layout = TestLayout::build(&items, &edges);
    let returns = layout
        .edges
        .values()
        .filter(|edge| edge.travel == EdgeTravel::Return)
        .collect::<Vec<_>>();
    assert_eq!(returns.len(), 1, "exactly one edge should close the loop");
    assert_eq!(returns[0].source, "reingestor:loop");
    assert!(layout.items["relay:a"].x < layout.items["reingestor:loop"].x);
}

#[test]
fn disconnected_parts_are_stacked_without_overlapping() {
    let items = vec![card("ingestor:a"), pill("relay:a"), pill("relay:lonely")];
    let edges = vec![flow("ingestor:a", "relay:a")];
    let layout = TestLayout::build(&items, &edges);
    let lonely = layout.items["relay:lonely"];
    for (id, rect) in &layout.items {
        if id == "relay:lonely" {
            continue;
        }
        assert!(!lonely.intersects(rect), "bands must not overlap {id}");
    }
}

#[test]
fn an_isolated_item_still_lays_out() {
    let items = vec![pill("relay:alone")];
    let layout = TestLayout::build(&items, &[]);
    assert_eq!(layout.items.len(), 1);
    assert!(layout.width > 0 && layout.height > 0);
}

#[test]
fn an_empty_graph_has_no_geometry() {
    let layout = TestLayout::build(&[], &[]);
    assert!(layout.items.is_empty());
    assert_eq!(layout.width, 0);
}

#[test]
fn an_edge_to_an_absent_item_is_not_drawn() {
    let items = vec![card("ingestor:a"), pill("relay:a")];
    let edges = vec![
        flow("ingestor:a", "relay:a"),
        flow("relay:a", "emitter:gone"),
    ];
    let layout = TestLayout::build(&items, &edges);
    assert_eq!(layout.edges.len(), 1);
    route(&layout, "ingestor:a", "relay:a", LayoutEdgeKind::Flow);
}

/// Items named by kind and name, the way a report names its nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Schema,
    Relay,
    Junction,
}

#[test]
fn items_sharing_a_name_across_kinds_keep_their_own_places() {
    let item = |kind: Kind, name: &'static str| LayoutItem::<(Kind, &str), String> {
        id: (kind, name),
        width: 176,
        height: 64,
        relay: kind == Kind::Relay,
        branch: None,
    };
    let link = |source: (Kind, &'static str), target: (Kind, &'static str), kind| LayoutEdge {
        id: (source, target, kind),
        source,
        target,
        kind,
        badge: false,
    };
    let schema = (Kind::Schema, "orders");
    let relay = (Kind::Relay, "orders");
    let junction = (Kind::Junction, "orders");
    let output = (Kind::Relay, "routed");
    let items = vec![
        item(Kind::Schema, "orders"),
        item(Kind::Relay, "orders"),
        item(Kind::Junction, "orders"),
        item(Kind::Relay, "routed"),
    ];
    let edges = vec![
        link(schema, relay, LayoutEdgeKind::Dependency),
        link(relay, junction, LayoutEdgeKind::Flow),
        link(junction, output, LayoutEdgeKind::Flow),
    ];
    let layout = Layout::build(&items, &edges);

    assert_eq!(layout.items.len(), 4, "one place per kind and name");
    let places = layout.items.values().collect::<Vec<_>>();
    for (index, place) in places.iter().enumerate() {
        for other in places.iter().skip(index + 1) {
            assert!(!place.intersects(other), "{place:?} overlaps {other:?}");
        }
    }
    let into_junction = &layout.edges[&(relay, junction, LayoutEdgeKind::Flow)];
    assert_eq!(into_junction.source, relay);
    assert_eq!(into_junction.target, junction);
    assert_eq!(into_junction.points[0].0, layout.items[&relay].right());
    assert_eq!(
        into_junction.points.last().expect("the edge arrives").0,
        layout.items[&junction].x
    );
    let schema_link = &layout.edges[&(schema, relay, LayoutEdgeKind::Dependency)];
    assert_eq!(
        schema_link.points.last().expect("the dependency arrives").0,
        layout.items[&relay].x
    );
}

#[test]
fn parallel_relations_between_one_pair_keep_their_own_routes() {
    let items = vec![
        card("ingestor:source"),
        pill("relay:events"),
        card("junction:enrich"),
        pill("relay:enriched"),
    ];
    let kinds = [
        LayoutEdgeKind::Flow,
        LayoutEdgeKind::Dependency,
        LayoutEdgeKind::State,
    ];
    let mut edges = vec![
        flow("ingestor:source", "relay:events"),
        flow("junction:enrich", "relay:enriched"),
    ];
    for kind in kinds {
        edges.push(edge("relay:events", "junction:enrich", kind));
    }
    let layout = TestLayout::build(&items, &edges);

    let parallel = kinds
        .iter()
        .map(|kind| route(&layout, "relay:events", "junction:enrich", *kind))
        .collect::<Vec<_>>();
    let departures = parallel
        .iter()
        .map(|edge| edge.points[0].1)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        departures.len(),
        3,
        "each relation leaves through its own port"
    );
    for (index, left) in parallel.iter().enumerate() {
        assert_eq!(left.travel, EdgeTravel::Forward);
        for right in parallel.iter().skip(index + 1) {
            assert_ne!(left.points, right.points);
            for left_run in runs(&left.points) {
                for right_run in runs(&right.points) {
                    assert!(
                        !left_run.shares_a_line_with(right_run),
                        "{:?} and {:?} share a run",
                        left.kind,
                        right.kind
                    );
                }
            }
        }
    }
    let record_flow = route(
        &layout,
        "relay:events",
        "junction:enrich",
        LayoutEdgeKind::Flow,
    );
    assert_eq!(
        record_flow.points[0].1,
        layout.items["relay:events"].center_y(),
        "the record flow keeps the centre port"
    );
    assert_no_edge_crosses_an_item(&layout);
}

#[test]
fn a_dependency_against_the_flow_travels_backward_without_reordering_it() {
    let items = vec![
        card("ingestor:source"),
        pill("relay:input"),
        card("junction:route"),
        pill("relay:output"),
    ];
    let edges = vec![
        flow("ingestor:source", "relay:input"),
        flow("relay:input", "junction:route"),
        flow("junction:route", "relay:output"),
        // The junction's configuration requires the relay it writes.
        dependency("relay:output", "junction:route"),
    ];
    let layout = TestLayout::build(&items, &edges);

    let junction = layout.items["junction:route"];
    let output = layout.items["relay:output"];
    assert!(junction.right() <= output.x, "the flow keeps its order");
    let requirement = route(
        &layout,
        "relay:output",
        "junction:route",
        LayoutEdgeKind::Dependency,
    );
    assert_eq!(requirement.travel, EdgeTravel::Backward);
    assert_eq!(requirement.points[0].0, output.x, "leaves the relay");
    assert_eq!(
        requirement.points.last().expect("the dependency arrives").0,
        junction.right(),
        "enters the junction it is required by"
    );
    let record_flow = route(
        &layout,
        "junction:route",
        "relay:output",
        LayoutEdgeKind::Flow,
    );
    assert_eq!(record_flow.travel, EdgeTravel::Forward);
    assert_ne!(
        record_flow.points[0].1,
        requirement.points.last().expect("arrives").1
    );
    for left_run in runs(&record_flow.points) {
        for right_run in runs(&requirement.points) {
            assert!(!left_run.shares_a_line_with(right_run));
        }
    }
    assert_no_edge_crosses_an_item(&layout);
}

#[test]
fn configuration_items_stand_beside_what_requires_them() {
    let items = vec![
        card("wire_schema:order_wire"),
        card("codec:order_codec"),
        card("client:broker"),
        card("schema:order"),
        card("ingestor:orders_in"),
        pill("relay:orders"),
        card("emitter:orders_out"),
    ];
    let edges = vec![
        dependency("wire_schema:order_wire", "codec:order_codec"),
        dependency("codec:order_codec", "ingestor:orders_in"),
        dependency("client:broker", "ingestor:orders_in"),
        dependency("schema:order", "relay:orders"),
        flow("ingestor:orders_in", "relay:orders"),
        flow("relay:orders", "emitter:orders_out"),
    ];
    let layout = TestLayout::build(&items, &edges);

    let place = |id: &str| layout.items[id];
    assert!(place("wire_schema:order_wire").right() <= place("codec:order_codec").x);
    assert!(place("codec:order_codec").right() <= place("ingestor:orders_in").x);
    assert_eq!(
        place("client:broker").x,
        place("codec:order_codec").x,
        "the client stands in the column just before the ingestor that requires it"
    );
    assert_eq!(
        place("schema:order").x,
        place("ingestor:orders_in").x,
        "the relay's schema stands in the column just before the relay"
    );
    for edge in layout.edges.values() {
        assert_eq!(edge.travel, EdgeTravel::Forward, "{edge:?}");
    }
    let chain =
        ["ingestor:orders_in", "relay:orders", "emitter:orders_out"].map(|id| place(id).center_y());
    assert!(
        chain.windows(2).all(|pair| pair[0] == pair[1]),
        "requirements must not bend the record flow, got {chain:?}"
    );
    assert_no_edge_crosses_an_item(&layout);
}

#[test]
fn a_dependency_within_one_column_is_drawn_as_a_return_path() {
    let items = vec![
        card("ingestor:source"),
        pill("relay:first"),
        pill("relay:second"),
    ];
    let edges = vec![
        flow("ingestor:source", "relay:first"),
        flow("ingestor:source", "relay:second"),
        dependency("relay:first", "relay:second"),
    ];
    let layout = TestLayout::build(&items, &edges);
    assert_eq!(
        layout.items["relay:first"].x,
        layout.items["relay:second"].x
    );
    let requirement = route(
        &layout,
        "relay:first",
        "relay:second",
        LayoutEdgeKind::Dependency,
    );
    assert_eq!(requirement.travel, EdgeTravel::Return);
    let top = layout
        .items
        .values()
        .map(|rect| rect.y)
        .min()
        .expect("items are placed");
    assert!(
        requirement.points.iter().any(|point| point.1 < top),
        "a return path runs through the corridor above the items"
    );
}
