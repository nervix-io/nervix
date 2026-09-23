//! Geometry for the console's graphs.
//!
//! The drawing is layered: items sit in columns ordered by how far records have travelled, and
//! every edge crosses exactly one gutter at a time. Edges that span more than one gutter reserve
//! a row of their own in each column they pass, which is what makes "an edge never crosses an
//! item" a property of the arrangement rather than something a router has to rediscover.
//!
//! Items, edges and branch groups keep the identities their caller gives them. Two items of
//! different kinds that share a name, or two relations between the same pair of items, therefore
//! stay distinct all the way from the caller's graph to the geometry it gets back.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use meticulous::{OptionExt as _, ResultExt as _};

/// Vertical clearance between two items in the same column, wide enough for two rate badges to
/// sit above one another without touching. Where a branch group's region begins or ends between
/// the two items, the gap grows to hold the region's padding and header as well.
const ROW_GAP: i32 = 36;
/// Vertical clearance between a branch group's region and anything outside it in one column.
const GROUP_CLEARANCE: i32 = 12;
/// Smallest vertical distance between two ports on the same item.
const PORT_PITCH: i32 = 20;
/// Horizontal run every edge makes on leaving its source before it may turn.
const SOURCE_PLUG: i32 = 20;
/// Horizontal run every edge makes into its target, kept clear of vertical traffic.
const TARGET_PLUG: i32 = 20;
const LANE_PITCH: i32 = 16;
const BADGE_WIDTH: i32 = 64;
const BADGE_HEIGHT: i32 = 16;
const BADGE_GAP: i32 = 8;
/// The band reserved above a branch group's first column for its header.
const GROUP_HEADER_HEIGHT: i32 = 24;
const GROUP_PADDING: i32 = 8;
const CANVAS_PADDING: i32 = 48;
/// Vertical clearance between two disconnected parts of the graph.
const BAND_GAP: i32 = 72;
const FEEDBACK_PITCH: i32 = 20;
const ORDERING_SWEEPS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LayoutEdgeKind {
    /// Records travel along this edge.
    Flow,
    /// The target reads the source's materialized state.
    State,
    /// The target's configuration requires the source. Nothing travels along it, so it places
    /// only the items that no record or state edge places.
    Dependency,
}

impl LayoutEdgeKind {
    /// Whether this edge decides where the flow puts its endpoints.
    const fn anchors(self) -> bool {
        match self {
            Self::Flow | Self::State => true,
            Self::Dependency => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LayoutItem<I, G> {
    pub id: I,
    pub width: i32,
    pub height: i32,
    pub relay: bool,
    /// The branch group this item belongs to, if it runs per branch.
    pub branch: Option<G>,
}

#[derive(Debug, Clone)]
pub struct LayoutEdge<I, E> {
    /// The edge's own identity, which tells apart two edges joining the same pair of items.
    pub id: E,
    pub source: I,
    pub target: I,
    pub kind: LayoutEdgeKind,
    pub badge: bool,
}

/// How a routed edge travels between its endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeTravel {
    /// Left to right through the gutters between its endpoints, the way records read.
    Forward,
    /// Right to left through the same gutters. Only a dependency between two items the flow has
    /// already placed travels this way, and nothing moves along it.
    Backward,
    /// Through the corridor above the items, closing a cycle or joining two items that share a
    /// column. It is drawn with direction markers so it is never mistaken for forward flow.
    Return,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Rect {
    pub x: i32,
    pub y: i32,
    pub width: i32,
    pub height: i32,
}

impl Rect {
    pub const fn right(&self) -> i32 {
        self.x + self.width
    }

    pub const fn bottom(&self) -> i32 {
        self.y + self.height
    }

    pub const fn center_y(&self) -> i32 {
        self.y + self.height / 2
    }

    pub const fn intersects(&self, other: &Self) -> bool {
        self.x < other.right()
            && other.x < self.right()
            && self.y < other.bottom()
            && other.y < self.bottom()
    }
}

#[derive(Debug, Clone)]
pub struct RoutedEdge<I> {
    pub source: I,
    pub target: I,
    pub kind: LayoutEdgeKind,
    /// The turns the line makes, from where it leaves its source to where it enters its target.
    pub points: Vec<(i32, i32)>,
    pub badge: Option<Rect>,
    pub travel: EdgeTravel,
}

#[derive(Debug, Clone)]
pub struct GroupRegion<G> {
    pub branch: G,
    /// One band per column the group spans, left to right. Their union is the region.
    pub bands: Vec<Rect>,
}

impl<G> GroupRegion<G> {
    /// The region outline as a closed rectilinear path: along the tops left to right, then back
    /// along the bottoms.
    pub fn outline(&self) -> String {
        if self.bands.is_empty() {
            return String::new();
        }
        let mut path = String::new();
        for (index, band) in self.bands.iter().enumerate() {
            if index == 0 {
                path.push_str(&format!("M {} {}", band.x, band.y));
            } else {
                path.push_str(&format!(" L {} {}", band.x, band.y));
            }
            path.push_str(&format!(" L {} {}", band.right(), band.y));
        }
        for band in self.bands.iter().rev() {
            path.push_str(&format!(" L {} {}", band.right(), band.bottom()));
            path.push_str(&format!(" L {} {}", band.x, band.bottom()));
        }
        path.push_str(" Z");
        path
    }

    pub fn header_anchor(&self) -> Option<Rect> {
        self.bands.first().map(|band| Rect {
            x: band.x,
            y: band.y,
            width: band.width,
            height: GROUP_HEADER_HEIGHT,
        })
    }
}

/// The geometry of one graph: where every item sits, how every edge is routed, and the region
/// each branch group occupies.
///
/// `I` names items, `E` names edges and `G` names branch groups. Routes are keyed by edge
/// identity rather than by their endpoints, so parallel relations between one pair of items each
/// keep their own route.
#[derive(Debug, Clone)]
pub struct Layout<I, E, G> {
    pub items: BTreeMap<I, Rect>,
    pub edges: BTreeMap<E, RoutedEdge<I>>,
    pub groups: Vec<GroupRegion<G>>,
    pub width: i32,
    pub height: i32,
}

impl<I, E, G> Default for Layout<I, E, G> {
    fn default() -> Self {
        Self {
            items: BTreeMap::new(),
            edges: BTreeMap::new(),
            groups: Vec::new(),
            width: 0,
            height: 0,
        }
    }
}

impl<I, E, G> Layout<I, E, G>
where
    I: Ord + Clone,
    E: Ord + Clone,
    G: Ord + Clone,
{
    /// Arrange items and route edges. The result is a pure function of the input, so an
    /// unchanged topology always produces identical geometry.
    ///
    /// Edge identities are unique within one graph. An edge whose endpoint is not one of the
    /// items is not drawn.
    pub fn build(items: &[LayoutItem<I, G>], edges: &[LayoutEdge<I, E>]) -> Self {
        Builder::new(items, edges).run()
    }
}

/// A row in a column: either a real item or the reserved corridor an edge occupies while
/// passing through.
#[derive(Debug, Clone)]
struct Slot<I, G> {
    item: Option<usize>,
    column: usize,
    width: i32,
    height: i32,
    branch: Option<G>,
    /// Sort key that keeps ordering stable and reproducible across renders.
    key: SlotKey<I>,
    order: usize,
    y: i32,
    weight: i64,
}

/// What a row is, in an order that does not depend on how the graph was listed: an item by its
/// identity, a corridor by the edge passing through it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SlotKey<I> {
    /// The item itself, or the item a passing edge leaves.
    anchor: I,
    /// Present on a corridor: where the passing edge goes, and which edge it is.
    corridor: Option<CorridorKey<I>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CorridorKey<I> {
    target: I,
    edge: usize,
}

#[derive(Debug, Clone, Copy)]
struct Segment {
    edge: usize,
    from: usize,
    to: usize,
}

/// An edge's two items, as indices into the item list.
#[derive(Debug, Clone, Copy)]
struct EdgeEnds {
    from: usize,
    to: usize,
}

/// The turning edge a gutter lane belongs to: the row the edge leaves and which edge it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct LaneKey {
    from: usize,
    edge: usize,
}

/// Where a row sorts within its column. Branch members sort together on their group's median
/// weight first, and the row key breaks ties so ordering is reproducible across renders.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ColumnSortKey<'a, I, G> {
    group_weight: i64,
    group: Option<&'a G>,
    weight: i64,
    key: &'a SlotKey<I>,
}

/// A comparable position for an edge's far endpoint, ordered by column, then by the row within
/// that column, with the item identity breaking ties.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct FarPosition<'a, I> {
    column: usize,
    order: usize,
    id: &'a I,
}

/// A row joined to another by one segment of an edge.
#[derive(Debug, Clone, Copy)]
struct Neighbour {
    slot: usize,
    edge: usize,
}

/// For every row, the rows that position it: those feeding it and those it feeds.
struct Neighbours {
    predecessors: Vec<Vec<Neighbour>>,
    successors: Vec<Vec<Neighbour>>,
}

/// Which edges place items and which of those close a cycle.
struct EdgePlan {
    /// Edges that advance the flow, acyclic, so the items they join can be layered.
    forward: Vec<usize>,
    /// Edges that travel back against a cycle.
    returns: BTreeSet<usize>,
}

struct Builder<'a, I, E, G> {
    items: &'a [LayoutItem<I, G>],
    edges: Vec<&'a LayoutEdge<I, E>>,
    /// Each edge's source and target.
    ends: Vec<EdgeEnds>,
    /// Whether a record or state edge touches the item, which is what places it in the flow.
    anchored: Vec<bool>,
    /// Each edge's items in drawing order: the one it leaves on the left, then the one it enters
    /// on the right. A backward edge is drawn from its target to its source.
    drawn: Vec<EdgeEnds>,
    travel: Vec<EdgeTravel>,
    slots: Vec<Slot<I, G>>,
    columns: Vec<Vec<usize>>,
    segments: Vec<Segment>,
    item_slot: Vec<usize>,
    /// The column each branch group starts in, which is where its region carries its header.
    group_first_column: BTreeMap<G, usize>,
    column_x: Vec<i32>,
    column_width: Vec<i32>,
    gutter_x: Vec<i32>,
}

impl<'a, I, E, G> Builder<'a, I, E, G>
where
    I: Ord + Clone,
    E: Ord + Clone,
    G: Ord + Clone,
{
    fn new(items: &'a [LayoutItem<I, G>], edges: &'a [LayoutEdge<I, E>]) -> Self {
        let index_by_id = items
            .iter()
            .enumerate()
            .map(|(index, item)| (&item.id, index))
            .collect::<BTreeMap<_, _>>();
        let mut kept = Vec::with_capacity(edges.len());
        let mut ends = Vec::with_capacity(edges.len());
        for edge in edges {
            let (Some(source), Some(target)) = (
                index_by_id.get(&edge.source).copied(),
                index_by_id.get(&edge.target).copied(),
            ) else {
                continue;
            };
            kept.push(edge);
            ends.push(EdgeEnds {
                from: source,
                to: target,
            });
        }
        let mut anchored = vec![false; items.len()];
        for (edge, ends) in kept.iter().zip(&ends) {
            if edge.kind.anchors() {
                anchored[ends.from] = true;
                anchored[ends.to] = true;
            }
        }
        Self {
            items,
            drawn: ends.clone(),
            travel: vec![EdgeTravel::Forward; kept.len()],
            edges: kept,
            ends,
            anchored,
            slots: Vec::new(),
            columns: Vec::new(),
            segments: Vec::new(),
            item_slot: Vec::new(),
            group_first_column: BTreeMap::new(),
            column_x: Vec::new(),
            column_width: Vec::new(),
            gutter_x: Vec::new(),
        }
    }

    fn run(mut self) -> Layout<I, E, G> {
        if self.items.is_empty() {
            return Layout::default();
        }
        let plan = self.plan_edges();
        let depths = self.depths(&plan.forward);
        self.build_columns(&depths);
        self.orient_edges(&plan.returns);
        self.build_segments();
        self.order_columns();
        let ports = self.assign_ports();
        self.assign_rows(&ports);
        self.assign_columns_x();
        let edges = self.route(&ports);
        let groups = self.group_regions();
        self.finish(edges, groups)
    }

    /// Whether an edge takes part in deciding columns. A dependency between two items the flow
    /// already places only has to be drawn, so it cannot push the flow out of reading order.
    fn places_columns(&self, edge: usize) -> bool {
        let ends = self.ends[edge];
        self.edges[edge].kind.anchors() || !self.anchored[ends.from] || !self.anchored[ends.to]
    }

    /// Edge indices that advance the flow, with the back edges of a depth-first walk set aside so
    /// that what remains is acyclic and can be layered.
    fn plan_edges(&self) -> EdgePlan {
        let placing = (0..self.edges.len())
            .filter(|edge| self.places_columns(*edge))
            .collect::<Vec<_>>();
        let mut adjacency = vec![Vec::new(); self.items.len()];
        for edge in &placing {
            let ends = self.ends[*edge];
            adjacency[ends.from].push((ends.to, *edge));
        }
        for list in &mut adjacency {
            list.sort_by(|left, right| {
                self.items[left.0]
                    .id
                    .cmp(&self.items[right.0].id)
                    .then(left.1.cmp(&right.1))
            });
        }

        let mut indegree = vec![0_usize; self.items.len()];
        for list in &adjacency {
            for (target, _) in list {
                indegree[*target] += 1;
            }
        }
        let mut roots = (0..self.items.len())
            .filter(|index| indegree[*index] == 0)
            .collect::<Vec<_>>();
        roots.sort_by(|left, right| self.items[*left].id.cmp(&self.items[*right].id));
        let mut starts = roots;
        let mut remaining = (0..self.items.len()).collect::<Vec<_>>();
        remaining.sort_by(|left, right| self.items[*left].id.cmp(&self.items[*right].id));
        starts.extend(remaining);

        const WHITE: u8 = 0;
        const GRAY: u8 = 1;
        const BLACK: u8 = 2;
        let mut color = vec![WHITE; self.items.len()];
        let mut back = BTreeSet::new();
        for start in starts {
            if color[start] != WHITE {
                continue;
            }
            let mut stack = vec![(start, 0_usize)];
            color[start] = GRAY;
            while let Some((node, cursor)) = stack.pop() {
                if cursor >= adjacency[node].len() {
                    color[node] = BLACK;
                    continue;
                }
                stack.push((node, cursor + 1));
                let (target, edge) = adjacency[node][cursor];
                match color[target] {
                    GRAY => {
                        back.insert(edge);
                    }
                    WHITE => {
                        color[target] = GRAY;
                        stack.push((target, 0));
                    }
                    _ => {}
                }
            }
        }

        let forward = placing
            .into_iter()
            .filter(|edge| !back.contains(edge))
            .collect();
        EdgePlan {
            forward,
            returns: back,
        }
    }

    /// Longest-path depth over the acyclic remainder, so every item sits to the right of
    /// everything that feeds it. An item only a dependency places then moves as far right as the
    /// items requiring it allow, so it stands beside them rather than at the start of the graph.
    fn depths(&self, forward: &[usize]) -> Vec<usize> {
        let mut successors = vec![Vec::new(); self.items.len()];
        let mut indegree = vec![0_usize; self.items.len()];
        for index in forward {
            let ends = self.ends[*index];
            successors[ends.from].push(ends.to);
            indegree[ends.to] += 1;
        }
        let mut depths = vec![0_usize; self.items.len()];
        let mut queue = (0..self.items.len())
            .filter(|index| indegree[*index] == 0)
            .collect::<VecDeque<_>>();
        let mut topological = Vec::with_capacity(self.items.len());
        while let Some(node) = queue.pop_front() {
            topological.push(node);
            for target in successors[node].clone() {
                depths[target] = depths[target].max(depths[node] + 1);
                indegree[target] -= 1;
                if indegree[target] == 0 {
                    queue.push_back(target);
                }
            }
        }

        for node in topological.into_iter().rev() {
            if self.anchored[node] {
                continue;
            }
            let Some(earliest) = successors[node].iter().map(|target| depths[*target]).min() else {
                continue;
            };
            let latest = earliest
                .checked_sub(1)
                .verified("a successor sits at least one column right of the item feeding it");
            depths[node] = depths[node].max(latest);
        }
        depths
    }

    /// Place items into columns. Relays never share a column with processing items, so a relay
    /// reads as the port between the nodes on either side of it.
    fn build_columns(&mut self, depths: &[usize]) {
        let max_depth = depths.iter().copied().max().unwrap_or(0);
        let mut item_slot = vec![None; self.items.len()];
        let mut column = 0;
        for depth in 0..=max_depth {
            for relay in [false, true] {
                let mut members = (0..self.items.len())
                    .filter(|index| depths[*index] == depth && self.items[*index].relay == relay)
                    .collect::<Vec<_>>();
                if members.is_empty() {
                    continue;
                }
                members.sort_by(|left, right| self.items[*left].id.cmp(&self.items[*right].id));
                let mut rows = Vec::new();
                for item in members {
                    let slot = self.slots.len();
                    self.slots.push(Slot {
                        item: Some(item),
                        column,
                        width: self.items[item].width,
                        height: self.items[item].height,
                        branch: self.items[item].branch.clone(),
                        key: SlotKey {
                            anchor: self.items[item].id.clone(),
                            corridor: None,
                        },
                        order: rows.len(),
                        y: 0,
                        weight: 0,
                    });
                    item_slot[item] = Some(slot);
                    rows.push(slot);
                    if let Some(branch) = &self.items[item].branch {
                        self.group_first_column
                            .entry(branch.clone())
                            .or_insert(column);
                    }
                }
                self.columns.push(rows);
                column += 1;
            }
        }
        self.item_slot = item_slot
            .into_iter()
            .map(|slot| slot.assured("every item has a depth, so every item is given a row"))
            .collect();
    }

    fn column_of_item(&self, item: usize) -> usize {
        self.slots[self.item_slot[item]].column
    }

    /// Decide how every edge travels now that columns are known. An edge the depth-first walk
    /// set aside closes a cycle, and so does a dependency between two items in one column; the
    /// rest cross the gutters between their columns, backwards when a dependency points against
    /// the reading order.
    fn orient_edges(&mut self, returns: &BTreeSet<usize>) {
        for edge in 0..self.edges.len() {
            let ends = self.ends[edge];
            if returns.contains(&edge) {
                self.travel[edge] = EdgeTravel::Return;
                continue;
            }
            let from_column = self.column_of_item(ends.from);
            let to_column = self.column_of_item(ends.to);
            if from_column < to_column {
                self.travel[edge] = EdgeTravel::Forward;
            } else if to_column < from_column {
                self.travel[edge] = EdgeTravel::Backward;
                self.drawn[edge] = EdgeEnds {
                    from: ends.to,
                    to: ends.from,
                };
            } else {
                self.travel[edge] = EdgeTravel::Return;
            }
        }
    }

    /// Break every edge that crosses gutters into adjacent-column segments, reserving a row in
    /// each column an edge passes through so nothing else is placed in its way.
    fn build_segments(&mut self) {
        for index in 0..self.edges.len() {
            if self.travel[index] == EdgeTravel::Return {
                continue;
            }
            let drawn = self.drawn[index];
            let source = self.item_slot[drawn.from];
            let target = self.item_slot[drawn.to];
            let from_column = self.slots[source].column;
            let to_column = self.slots[target].column;
            let mut previous = source;
            for column in (from_column + 1)..to_column {
                let slot = self.slots.len();
                self.slots.push(Slot {
                    item: None,
                    column,
                    width: 0,
                    height: 0,
                    branch: None,
                    key: SlotKey {
                        anchor: self.items[drawn.from].id.clone(),
                        corridor: Some(CorridorKey {
                            target: self.items[drawn.to].id.clone(),
                            edge: index,
                        }),
                    },
                    order: self.columns[column].len(),
                    y: 0,
                    weight: 0,
                });
                self.columns[column].push(slot);
                self.segments.push(Segment {
                    edge: index,
                    from: previous,
                    to: slot,
                });
                previous = slot;
            }
            self.segments.push(Segment {
                edge: index,
                from: previous,
                to: target,
            });
        }
    }

    /// Whether a segment of `edge` may pull the row `slot` towards its other end. Items the flow
    /// places answer only to record and state edges, so a dependency never bends the flow.
    fn pulls(&self, slot: usize, edge: usize) -> bool {
        let anchored = match self.slots[slot].item {
            Some(item) => self.anchored[item],
            None => false,
        };
        !anchored || self.edges[edge].kind.anchors()
    }

    /// The rows each row's position is derived from, before and after it.
    fn neighbours(&self) -> Neighbours {
        let mut neighbours = Neighbours {
            predecessors: vec![Vec::new(); self.slots.len()],
            successors: vec![Vec::new(); self.slots.len()],
        };
        for segment in &self.segments {
            if self.pulls(segment.from, segment.edge) {
                neighbours.successors[segment.from].push(Neighbour {
                    slot: segment.to,
                    edge: segment.edge,
                });
            }
            if self.pulls(segment.to, segment.edge) {
                neighbours.predecessors[segment.to].push(Neighbour {
                    slot: segment.from,
                    edge: segment.edge,
                });
            }
        }
        neighbours
    }

    /// Order rows within each column to reduce crossings, keeping every branch group's members
    /// contiguous so a group's region can contain exactly its members.
    fn order_columns(&mut self) {
        let Neighbours {
            predecessors,
            successors,
        } = self.neighbours();

        for column in &self.columns {
            for (position, slot) in column.iter().enumerate() {
                self.slots[*slot].order = position;
            }
        }

        for sweep in 0..ORDERING_SWEEPS {
            let downward = sweep % 2 == 0;
            let order = if downward {
                (0..self.columns.len()).collect::<Vec<_>>()
            } else {
                (0..self.columns.len()).rev().collect::<Vec<_>>()
            };
            for column in order {
                for slot in self.columns[column].clone() {
                    let neighbors = if downward {
                        &predecessors[slot]
                    } else {
                        &successors[slot]
                    };
                    self.slots[slot].weight = if neighbors.is_empty() {
                        i64::from(i32::try_from(self.slots[slot].order).unwrap_or(i32::MAX)) * 1000
                    } else {
                        let total: i64 = neighbors
                            .iter()
                            .map(|neighbor| {
                                i64::from(
                                    i32::try_from(self.slots[neighbor.slot].order)
                                        .unwrap_or(i32::MAX),
                                ) * 1000
                            })
                            .sum();
                        total / i64::try_from(neighbors.len()).unwrap_or(i64::MAX)
                    };
                }
                self.sort_column(column);
            }
        }
    }

    fn sort_column(&mut self, column: usize) {
        let mut rows = self.columns[column].clone();
        {
            let medians = self.group_medians(column);
            rows.sort_by(|left, right| {
                let left_key = self.column_sort_key(*left, &medians);
                let right_key = self.column_sort_key(*right, &medians);
                left_key.cmp(&right_key)
            });
        }
        for (position, slot) in rows.iter().enumerate() {
            self.slots[*slot].order = position;
        }
        self.columns[column] = rows;
    }

    /// Where each branch group sits in a column, so all of its members sort together.
    fn group_medians(&self, column: usize) -> BTreeMap<&G, i64> {
        let mut weights: BTreeMap<&G, Vec<i64>> = BTreeMap::new();
        for slot in &self.columns[column] {
            if let Some(branch) = &self.slots[*slot].branch {
                weights
                    .entry(branch)
                    .or_default()
                    .push(self.slots[*slot].weight);
            }
        }
        weights
            .into_iter()
            .map(|(branch, mut values)| {
                values.sort_unstable();
                (branch, values[values.len() / 2])
            })
            .collect()
    }

    fn column_sort_key<'s>(
        &'s self,
        slot: usize,
        medians: &BTreeMap<&G, i64>,
    ) -> ColumnSortKey<'s, I, G> {
        let row = &self.slots[slot];
        let group_weight = match &row.branch {
            Some(branch) => medians.get(branch).copied().unwrap_or(row.weight),
            None => row.weight,
        };
        ColumnSortKey {
            group_weight,
            group: row.branch.as_ref(),
            weight: row.weight,
            key: &row.key,
        }
    }

    /// Port offsets, measured from each item's vertical centre. The edge that continues a
    /// straight chain keeps the centre so the chain stays collinear.
    fn assign_ports(&mut self) -> Ports {
        let mut outgoing: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        let mut incoming: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for index in 0..self.edges.len() {
            if self.travel[index] == EdgeTravel::Return {
                continue;
            }
            let drawn = self.drawn[index];
            outgoing
                .entry(self.item_slot[drawn.from])
                .or_default()
                .push(index);
            incoming
                .entry(self.item_slot[drawn.to])
                .or_default()
                .push(index);
        }

        let mut ports = Ports::default();
        for (slot, mut edges) in outgoing {
            edges.sort_by(|left, right| {
                self.far_position(*left, true)
                    .cmp(&self.far_position(*right, true))
            });
            self.record_ports(&mut ports, slot, &edges, true);
        }
        for (slot, mut edges) in incoming {
            edges.sort_by(|left, right| {
                self.far_position(*left, false)
                    .cmp(&self.far_position(*right, false))
            });
            self.record_ports(&mut ports, slot, &edges, false);
        }
        ports
    }

    /// Spread an item's ports around its centre, keeping their order. When exactly one of them
    /// carries records the rest are dependencies or error routes, so the record-carrying edge
    /// takes the centre and a run of such items stays collinear.
    fn record_ports(&mut self, ports: &mut Ports, slot: usize, edges: &[usize], outgoing: bool) {
        let flowing = edges
            .iter()
            .enumerate()
            .filter(|(_, edge)| self.edges[**edge].kind == LayoutEdgeKind::Flow)
            .map(|(position, _)| position)
            .collect::<Vec<_>>();
        let pinned = (flowing.len() == 1 && edges.len() > 1).then(|| flowing[0]);

        let mut extent = 0;
        for (position, edge) in edges.iter().enumerate() {
            let position = i32::try_from(position).unwrap_or(i32::MAX);
            let offset = match pinned {
                Some(centre) => {
                    let centre = i32::try_from(centre).unwrap_or(i32::MAX);
                    (position - centre) * PORT_PITCH
                }
                None => {
                    let edge_count = i32::try_from(edges.len()).unwrap_or(i32::MAX);
                    (2 * position - (edge_count - 1)) * PORT_PITCH / 2
                }
            };
            extent = extent.max(offset.abs());
            ports.offsets.insert(
                PortKey {
                    slot,
                    edge: *edge,
                    outgoing,
                },
                offset,
            );
        }
        let needed = extent * 2 + PORT_PITCH;
        if needed > self.slots[slot].height {
            self.slots[slot].height = needed;
        }
    }

    /// A comparable position for an edge's far endpoint, used to order ports so edges leave and
    /// arrive without crossing each other at the item.
    fn far_position(&self, edge: usize, outgoing: bool) -> FarPosition<'_, I> {
        let drawn = self.drawn[edge];
        let item = if outgoing { drawn.to } else { drawn.from };
        let slot = self.item_slot[item];
        FarPosition {
            column: self.slots[slot].column,
            order: self.slots[slot].order,
            id: &self.items[item].id,
        }
    }

    /// Give every row a y, pulling each item towards the items it connects to so that a straight
    /// run of items lands on one horizontal axis.
    fn assign_rows(&mut self, ports: &Ports) {
        let Neighbours {
            predecessors,
            successors,
        } = self.neighbours();

        for column in &self.columns.clone() {
            let mut previous = None::<usize>;
            for slot in column {
                self.slots[*slot].y = match previous {
                    Some(previous) => self.lowest_y_below(previous, *slot),
                    None => 0,
                };
                previous = Some(*slot);
            }
        }

        for pass in 0..6 {
            let downward = pass % 2 == 0;
            let order = if downward {
                (0..self.columns.len()).collect::<Vec<_>>()
            } else {
                (0..self.columns.len()).rev().collect::<Vec<_>>()
            };
            for column in order {
                let rows = self.columns[column].clone();
                let mut desired = Vec::with_capacity(rows.len());
                for slot in &rows {
                    let neighbors = if downward {
                        &predecessors[*slot]
                    } else {
                        &successors[*slot]
                    };
                    if neighbors.is_empty() {
                        desired.push(self.slots[*slot].y);
                        continue;
                    }
                    let total: i32 = neighbors
                        .iter()
                        .map(|neighbor| {
                            let anchor = self.port_y(neighbor.slot, neighbor.edge, downward, ports);
                            anchor - self.slots[*slot].height / 2
                        })
                        .sum();
                    desired.push(total / i32::try_from(neighbors.len()).unwrap_or(i32::MAX));
                }
                self.place_column(column, &desired);
            }
        }
        self.separate_bands();
    }

    /// The y an edge attaches at on a neighbouring row.
    fn port_y(&self, slot: usize, edge: usize, outgoing: bool, ports: &Ports) -> i32 {
        let row = &self.slots[slot];
        let centre = row.y + row.height / 2;
        centre
            + ports
                .offsets
                .get(&PortKey {
                    slot,
                    edge,
                    outgoing,
                })
                .copied()
                .unwrap_or(0)
    }

    /// Lay a column out at its wanted positions while keeping the established order and leaving
    /// room between rows.
    fn place_column(&mut self, column: usize, desired: &[i32]) {
        let rows = self.columns[column].clone();
        let mut previous = None::<usize>;
        for (position, slot) in rows.iter().enumerate() {
            let wanted = desired[position];
            let placed = match previous {
                Some(previous) => wanted.max(self.lowest_y_below(previous, *slot)),
                None => wanted,
            };
            self.slots[*slot].y = placed;
            previous = Some(*slot);
        }
    }

    /// The highest a row may sit directly below `upper` in the same column. Where a branch
    /// group's region begins or ends between the two rows, the gap also holds the region's padding
    /// and, in the group's first column, its header, so a region never reaches a row it does not
    /// contain and two regions never meet.
    fn lowest_y_below(&self, upper: usize, lower: usize) -> i32 {
        let upper_row = &self.slots[upper];
        let lower_row = &self.slots[lower];
        let gap = if upper_row.branch == lower_row.branch {
            ROW_GAP
        } else {
            let below_upper = match upper_row.branch {
                Some(_) => GROUP_PADDING,
                None => 0,
            };
            let above_lower = match &lower_row.branch {
                Some(branch) => GROUP_PADDING + self.header_height(branch, lower_row.column),
                None => 0,
            };
            ROW_GAP.max(below_upper + above_lower + GROUP_CLEARANCE)
        };
        upper_row.y + upper_row.height + gap
    }

    /// The header band a group's region carries in `column`: only its first column has one.
    fn header_height(&self, branch: &G, column: usize) -> i32 {
        if self.group_first_column.get(branch) == Some(&column) {
            GROUP_HEADER_HEIGHT
        } else {
            0
        }
    }

    /// Push disconnected parts of the graph apart so they read as separate bands.
    fn separate_bands(&mut self) {
        let components = self.components();
        if components.len() < 2 {
            return;
        }
        let mut offset = 0;
        for component in components {
            let top = component
                .iter()
                .map(|slot| self.slots[*slot].y)
                .min()
                .unwrap_or(0);
            let shift = offset - top;
            let mut bottom = None::<i32>;
            for slot in &component {
                self.slots[*slot].y += shift;
                let slot_bottom = self.slots[*slot].y + self.slots[*slot].height;
                bottom = Some(match bottom {
                    Some(bottom) => bottom.max(slot_bottom),
                    None => slot_bottom,
                });
            }
            if let Some(bottom) = bottom {
                offset = bottom + BAND_GAP;
            }
        }
    }

    /// Connected parts of the graph, ordered by their first item so the arrangement is stable.
    fn components(&self) -> Vec<Vec<usize>> {
        let mut parent = (0..self.slots.len()).collect::<Vec<_>>();
        fn find(parent: &mut [usize], node: usize) -> usize {
            let mut root = node;
            while parent[root] != root {
                root = parent[root];
            }
            let mut cursor = node;
            while parent[cursor] != root {
                let next = parent[cursor];
                parent[cursor] = root;
                cursor = next;
            }
            root
        }
        for segment in &self.segments {
            let left = find(&mut parent, segment.from);
            let right = find(&mut parent, segment.to);
            if left != right {
                parent[left] = right;
            }
        }
        for index in 0..self.edges.len() {
            if self.travel[index] != EdgeTravel::Return {
                continue;
            }
            let ends = self.ends[index];
            let left = find(&mut parent, self.item_slot[ends.from]);
            let right = find(&mut parent, self.item_slot[ends.to]);
            if left != right {
                parent[left] = right;
            }
        }

        let mut groups: BTreeMap<&SlotKey<I>, Vec<usize>> = BTreeMap::new();
        for slot in 0..self.slots.len() {
            let root = find(&mut parent, slot);
            groups.entry(&self.slots[root].key).or_default().push(slot);
        }
        groups.into_values().collect()
    }

    /// Column x positions, widening each gutter to hold the edges and badges that cross it.
    fn assign_columns_x(&mut self) {
        // One gutter sits between each adjacent pair of columns, so a graph with no columns
        // has no gutters.
        let mut lanes = vec![0_usize; self.columns.len().saturating_sub(1)];
        for segment in &self.segments {
            let gutter = self.slots[segment.from].column;
            if gutter < lanes.len() {
                lanes[gutter] += 1;
            }
        }

        self.column_width = self
            .columns
            .iter()
            .map(|rows| {
                rows.iter()
                    .map(|slot| self.slots[*slot].width)
                    .max()
                    .unwrap_or(0)
            })
            .collect();

        let mut x = CANVAS_PADDING;
        for column in 0..self.columns.len() {
            self.column_x.push(x);
            x += self.column_width[column];
            if column < lanes.len() {
                let lane_count = i32::try_from(lanes[column].max(1)).unwrap_or(i32::MAX);
                let gutter = SOURCE_PLUG
                    + (lane_count + 1) * LANE_PITCH
                    + BADGE_WIDTH
                    + BADGE_GAP * 2
                    + TARGET_PLUG;
                self.gutter_x.push(x);
                x += gutter;
            }
        }
    }

    fn slot_rect(&self, slot: usize) -> Rect {
        let row = &self.slots[slot];
        let column_width = self.column_width[row.column];
        Rect {
            x: self.column_x[row.column] + (column_width - row.width) / 2,
            y: row.y,
            width: row.width,
            height: row.height,
        }
    }

    fn route(&self, ports: &Ports) -> BTreeMap<E, RoutedEdge<I>> {
        let lanes = self.assign_lanes(ports);
        let mut by_edge: BTreeMap<usize, Vec<Segment>> = BTreeMap::new();
        for segment in &self.segments {
            by_edge.entry(segment.edge).or_default().push(*segment);
        }

        let mut routed = BTreeMap::new();
        for (index, segments) in by_edge {
            let mut segments = segments;
            segments.sort_by_key(|segment| self.slots[segment.from].column);
            let mut points: Vec<(i32, i32)> = Vec::new();
            for segment in &segments {
                let start = self.segment_start(segment, ports);
                let end = self.segment_end(segment, ports);
                if points.is_empty() {
                    points.push(start);
                }
                if start.1 != end.1 {
                    let lane = lanes
                        .get(&LaneKey {
                            from: segment.from,
                            edge: segment.edge,
                        })
                        .copied()
                        .unwrap_or_else(|| self.default_lane(segment));
                    points.push((lane, start.1));
                    points.push((lane, end.1));
                }
                points.push(end);
            }
            let mut points = simplify(points);
            // Points run from the edge's source to its target, so a backward edge is read off its
            // drawn segments in reverse.
            if self.travel[index] == EdgeTravel::Backward {
                points.reverse();
            }
            let edge = self.edges[index];
            let badge = if edge.badge {
                self.badge_rect(segments.last().copied(), ports)
            } else {
                None
            };
            routed.insert(
                edge.id.clone(),
                RoutedEdge {
                    source: edge.source.clone(),
                    target: edge.target.clone(),
                    kind: edge.kind,
                    points,
                    badge,
                    travel: self.travel[index],
                },
            );
        }

        for index in 0..self.edges.len() {
            if self.travel[index] == EdgeTravel::Return {
                routed.insert(self.edges[index].id.clone(), self.route_return(index));
            }
        }
        routed
    }

    fn segment_start(&self, segment: &Segment, ports: &Ports) -> (i32, i32) {
        let row = &self.slots[segment.from];
        let rect = self.slot_rect(segment.from);
        if row.item.is_some() {
            (
                rect.right(),
                self.port_y(segment.from, segment.edge, true, ports),
            )
        } else {
            (rect.x, rect.center_y())
        }
    }

    fn segment_end(&self, segment: &Segment, ports: &Ports) -> (i32, i32) {
        let row = &self.slots[segment.to];
        let rect = self.slot_rect(segment.to);
        if row.item.is_some() {
            (rect.x, self.port_y(segment.to, segment.edge, false, ports))
        } else {
            (rect.x, rect.center_y())
        }
    }

    fn default_lane(&self, segment: &Segment) -> i32 {
        let gutter = self.slots[segment.from].column;
        match self.gutter_x.get(gutter) {
            Some(x) => x + SOURCE_PLUG + LANE_PITCH,
            None => 0,
        }
    }

    /// Give every turning edge its own vertical line inside the gutter, ordered so that no two
    /// edges ever run along the same horizontal line.
    fn assign_lanes(&self, ports: &Ports) -> BTreeMap<LaneKey, i32> {
        let mut by_gutter: BTreeMap<usize, Vec<Segment>> = BTreeMap::new();
        for segment in &self.segments {
            let start = self.segment_start(segment, ports);
            let end = self.segment_end(segment, ports);
            if start.1 == end.1 {
                continue;
            }
            by_gutter
                .entry(self.slots[segment.from].column)
                .or_default()
                .push(*segment);
        }

        let mut lanes = BTreeMap::new();
        for (gutter, segments) in by_gutter {
            let Some(gutter_x) = self.gutter_x.get(gutter).copied() else {
                continue;
            };
            let ordered = self.order_lanes(&segments, ports);
            for (position, segment) in ordered.iter().enumerate() {
                let position = i32::try_from(position).unwrap_or(i32::MAX);
                let x = gutter_x + SOURCE_PLUG + (position + 1) * LANE_PITCH;
                lanes.insert(
                    LaneKey {
                        from: segment.from,
                        edge: segment.edge,
                    },
                    x,
                );
            }
        }
        lanes
    }

    /// Order the lanes of one gutter. An edge whose arrival height matches another edge's
    /// departure height must turn later than it, or the two would share a horizontal line.
    fn order_lanes(&self, segments: &[Segment], ports: &Ports) -> Vec<Segment> {
        let mut base = segments.to_vec();
        base.sort_by_key(|segment| {
            let end = self.segment_end(segment, ports);
            let start = self.segment_start(segment, ports);
            (end.1, start.1, segment.edge)
        });

        let ends = base
            .iter()
            .enumerate()
            .map(|(index, segment)| (self.segment_end(segment, ports).1, index))
            .collect::<BTreeMap<_, _>>();
        let mut after = vec![Vec::new(); base.len()];
        let mut indegree = vec![0_usize; base.len()];
        for (index, segment) in base.iter().enumerate() {
            let start = self.segment_start(segment, ports).1;
            if let Some(other) = ends.get(&start).copied()
                && other != index
            {
                after[index].push(other);
                indegree[other] += 1;
            }
        }

        let mut queue = (0..base.len())
            .filter(|index| indegree[*index] == 0)
            .collect::<VecDeque<_>>();
        let mut ordered = Vec::with_capacity(base.len());
        while let Some(index) = queue.pop_front() {
            ordered.push(base[index]);
            for next in after[index].clone() {
                indegree[next] -= 1;
                if indegree[next] == 0 {
                    queue.push_back(next);
                }
            }
        }
        if ordered.len() == base.len() {
            ordered
        } else {
            base
        }
    }

    fn badge_rect(&self, segment: Option<Segment>, ports: &Ports) -> Option<Rect> {
        let segment = segment?;
        let end = self.segment_end(&segment, ports);
        Some(Rect {
            x: end.0 - TARGET_PLUG - BADGE_WIDTH,
            y: end.1 - BADGE_HEIGHT / 2,
            width: BADGE_WIDTH,
            height: BADGE_HEIGHT,
        })
    }

    /// Return paths run above the items they span and are marked so right-to-left travel is
    /// never mistaken for forward flow.
    fn route_return(&self, index: usize) -> RoutedEdge<I> {
        let edge = self.edges[index];
        let ends = self.ends[index];
        let source_rect = self.slot_rect(self.item_slot[ends.from]);
        let target_rect = self.slot_rect(self.item_slot[ends.to]);
        let top = (0..self.slots.len())
            .map(|slot| self.slot_rect(slot).y)
            .min()
            .unwrap_or(0);
        let feedback_lane = i32::try_from(index % 3).assured("a remainder below three fits i32");
        let corridor = top - FEEDBACK_PITCH * (1 + feedback_lane) - FEEDBACK_PITCH;
        let start = (source_rect.right(), source_rect.center_y());
        let end = (target_rect.x, target_rect.center_y());
        RoutedEdge {
            source: edge.source.clone(),
            target: edge.target.clone(),
            kind: edge.kind,
            points: vec![
                start,
                (start.0 + SOURCE_PLUG, start.1),
                (start.0 + SOURCE_PLUG, corridor),
                (end.0 - TARGET_PLUG, corridor),
                (end.0 - TARGET_PLUG, end.1),
                end,
            ],
            badge: None,
            travel: EdgeTravel::Return,
        }
    }

    /// A band per column a branch group spans. Members are contiguous within every column, so
    /// each band holds its members and nothing else.
    fn group_regions(&self) -> Vec<GroupRegion<G>> {
        /// The vertical extent one branch group covers in one column.
        struct BandExtent {
            top: i32,
            bottom: i32,
        }

        let mut by_branch: BTreeMap<&G, BTreeMap<usize, BandExtent>> = BTreeMap::new();
        for slot in 0..self.slots.len() {
            let Some(branch) = &self.slots[slot].branch else {
                continue;
            };
            if self.slots[slot].item.is_none() {
                continue;
            }
            let rect = self.slot_rect(slot);
            let column = self.slots[slot].column;
            let entry = by_branch
                .entry(branch)
                .or_default()
                .entry(column)
                .or_insert(BandExtent {
                    top: rect.y,
                    bottom: rect.bottom(),
                });
            entry.top = entry.top.min(rect.y);
            entry.bottom = entry.bottom.max(rect.bottom());
        }

        by_branch
            .into_iter()
            .map(|(branch, columns)| {
                let bands = columns
                    .into_iter()
                    .enumerate()
                    .map(|(position, (column, extent))| {
                        let header = if position == 0 {
                            GROUP_HEADER_HEIGHT
                        } else {
                            0
                        };
                        Rect {
                            x: self.column_x[column] - GROUP_PADDING,
                            y: extent.top - GROUP_PADDING - header,
                            width: self.column_width[column] + GROUP_PADDING * 2,
                            height: (extent.bottom - extent.top) + GROUP_PADDING * 2 + header,
                        }
                    })
                    .collect();
                GroupRegion {
                    branch: branch.clone(),
                    bands,
                }
            })
            .collect()
    }

    fn finish(
        self,
        edges: BTreeMap<E, RoutedEdge<I>>,
        groups: Vec<GroupRegion<G>>,
    ) -> Layout<I, E, G> {
        let mut items = BTreeMap::new();
        let mut min_x = None::<i32>;
        let mut min_y = None::<i32>;
        let mut include = |x: i32, y: i32| {
            min_x = Some(match min_x {
                Some(min) => min.min(x),
                None => x,
            });
            min_y = Some(match min_y {
                Some(min) => min.min(y),
                None => y,
            });
        };
        for slot in 0..self.slots.len() {
            if let Some(item) = self.slots[slot].item {
                let rect = self.slot_rect(slot);
                include(rect.x, rect.y);
                items.insert(self.items[item].id.clone(), rect);
            }
        }
        for group in &groups {
            for band in &group.bands {
                include(band.x, band.y);
            }
        }
        for edge in edges.values() {
            for point in &edge.points {
                include(point.0, point.1);
            }
        }
        let shift_x = CANVAS_PADDING - min_x.unwrap_or(0);
        let shift_y = CANVAS_PADDING - min_y.unwrap_or(0);
        let shift = |rect: Rect| Rect {
            x: rect.x + shift_x,
            y: rect.y + shift_y,
            ..rect
        };

        let mut layout = Layout {
            items: items
                .into_iter()
                .map(|(id, rect)| (id, shift(rect)))
                .collect(),
            edges: edges
                .into_iter()
                .map(|(id, edge)| {
                    let routed = RoutedEdge {
                        points: edge
                            .points
                            .into_iter()
                            .map(|(x, y)| (x + shift_x, y + shift_y))
                            .collect(),
                        badge: edge.badge.map(shift),
                        ..edge
                    };
                    (id, routed)
                })
                .collect(),
            groups: groups
                .into_iter()
                .map(|group| GroupRegion {
                    bands: group.bands.into_iter().map(shift).collect(),
                    ..group
                })
                .collect(),
            width: 0,
            height: 0,
        };

        let mut width = 0;
        let mut height = 0;
        for rect in layout.items.values() {
            width = width.max(rect.right());
            height = height.max(rect.bottom());
        }
        for group in &layout.groups {
            for band in &group.bands {
                width = width.max(band.right());
                height = height.max(band.bottom());
            }
        }
        for edge in layout.edges.values() {
            for point in &edge.points {
                width = width.max(point.0);
                height = height.max(point.1);
            }
            if let Some(badge) = edge.badge {
                width = width.max(badge.right());
                height = height.max(badge.bottom());
            }
        }
        layout.width = width + CANVAS_PADDING;
        layout.height = height + CANVAS_PADDING;
        layout
    }
}

/// One attachment point on an item: which row it sits on, which edge attaches there, and whether
/// the edge leaves the item or arrives at it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PortKey {
    slot: usize,
    edge: usize,
    outgoing: bool,
}

#[derive(Debug, Default)]
struct Ports {
    /// Each port's offset from its item's vertical centre.
    offsets: BTreeMap<PortKey, i32>,
}

/// Drop repeated and needlessly collinear points so an edge reports the turns it actually makes.
fn simplify(points: Vec<(i32, i32)>) -> Vec<(i32, i32)> {
    let mut result: Vec<(i32, i32)> = Vec::with_capacity(points.len());
    for point in points {
        if result.last() == Some(&point) {
            continue;
        }
        if result.len() >= 2 {
            let previous = result[result.len() - 1];
            let before = result[result.len() - 2];
            let collinear = (before.0 == previous.0 && previous.0 == point.0)
                || (before.1 == previous.1 && previous.1 == point.1);
            if collinear {
                result.pop();
            }
        }
        result.push(point);
    }
    result
}

#[cfg(test)]
mod tests;
