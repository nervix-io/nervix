//! Geometry for the execution graph.
//!
//! The drawing is layered: items sit in columns ordered by how far records have travelled, and
//! every edge crosses exactly one gutter at a time. Edges that span more than one gutter reserve
//! a row of their own in each column they pass, which is what makes "an edge never crosses an
//! item" a property of the arrangement rather than something a router has to rediscover.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Vertical clearance between two items in the same column. Wide enough for a branch-group
/// header band and for two rate badges to sit above one another without touching.
const ROW_GAP: i32 = 36;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutEdgeKind {
    /// Records travel along this edge.
    Flow,
    /// The target reads the source's materialized state.
    State,
}

#[derive(Debug, Clone)]
pub struct LayoutItem {
    pub id: String,
    pub width: i32,
    pub height: i32,
    pub relay: bool,
    /// The branch group this item belongs to, if it runs per branch.
    pub branch: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LayoutEdge {
    pub source: String,
    pub target: String,
    pub kind: LayoutEdgeKind,
    pub badge: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
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
pub struct RoutedEdge {
    pub source: String,
    pub target: String,
    pub kind: LayoutEdgeKind,
    pub points: Vec<(i32, i32)>,
    pub badge: Option<Rect>,
    /// A return path: it travels right to left and is drawn with direction markers.
    pub feedback: bool,
}

#[derive(Debug, Clone)]
pub struct GroupRegion {
    pub branch: String,
    /// One band per column the group spans, left to right. Their union is the region.
    pub bands: Vec<Rect>,
}

impl GroupRegion {
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

#[derive(Debug, Clone, Default)]
pub struct Layout {
    pub items: BTreeMap<String, Rect>,
    pub edges: Vec<RoutedEdge>,
    pub groups: Vec<GroupRegion>,
    pub width: i32,
    pub height: i32,
}

impl Layout {
    /// Arrange items and route edges. The result is a pure function of the input, so an
    /// unchanged topology always produces identical geometry.
    pub fn build(items: &[LayoutItem], edges: &[LayoutEdge]) -> Self {
        Builder::new(items, edges).run()
    }
}

/// A row in a column: either a real item or the reserved corridor an edge occupies while
/// passing through.
#[derive(Debug, Clone)]
struct Slot {
    item: Option<usize>,
    column: usize,
    width: i32,
    height: i32,
    branch: Option<String>,
    /// Sort key that keeps ordering stable and reproducible across renders.
    key: String,
    order: usize,
    y: i32,
    weight: i64,
}

#[derive(Debug, Clone, Copy)]
struct Segment {
    edge: usize,
    from: usize,
    to: usize,
}

struct Builder<'a> {
    items: &'a [LayoutItem],
    edges: Vec<&'a LayoutEdge>,
    index_by_id: BTreeMap<&'a str, usize>,
    slots: Vec<Slot>,
    columns: Vec<Vec<usize>>,
    segments: Vec<Segment>,
    /// Edges that travel backwards, drawn as marked return paths.
    feedback: Vec<usize>,
    item_slot: Vec<usize>,
    column_x: Vec<i32>,
    column_width: Vec<i32>,
    gutter_x: Vec<i32>,
}

impl<'a> Builder<'a> {
    fn new(items: &'a [LayoutItem], edges: &'a [LayoutEdge]) -> Self {
        let index_by_id = items
            .iter()
            .enumerate()
            .map(|(index, item)| (item.id.as_str(), index))
            .collect::<BTreeMap<_, _>>();
        let edges = edges
            .iter()
            .filter(|edge| {
                index_by_id.contains_key(edge.source.as_str())
                    && index_by_id.contains_key(edge.target.as_str())
            })
            .collect::<Vec<_>>();
        Self {
            items,
            edges,
            index_by_id,
            slots: Vec::new(),
            columns: Vec::new(),
            segments: Vec::new(),
            feedback: Vec::new(),
            item_slot: vec![usize::MAX; items.len()],
            column_x: Vec::new(),
            column_width: Vec::new(),
            gutter_x: Vec::new(),
        }
    }

    fn run(mut self) -> Layout {
        if self.items.is_empty() {
            return Layout::default();
        }
        let forward = self.forward_edges();
        let depths = self.depths(&forward);
        self.build_columns(&depths);
        self.build_segments(&forward);
        self.order_columns();
        let ports = self.assign_ports(&forward);
        self.assign_rows(&ports);
        self.assign_columns_x();
        let edges = self.route(&ports);
        let groups = self.group_regions();
        self.finish(edges, groups)
    }

    /// Edge indices that advance the flow, with the back edges of a depth-first walk removed so
    /// that what remains is acyclic and can be layered.
    fn forward_edges(&mut self) -> Vec<usize> {
        let mut adjacency = vec![Vec::new(); self.items.len()];
        for (index, edge) in self.edges.iter().enumerate() {
            let source = self.index_by_id[edge.source.as_str()];
            let target = self.index_by_id[edge.target.as_str()];
            adjacency[source].push((target, index));
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

        self.feedback = (0..self.edges.len())
            .filter(|index| back.contains(index))
            .collect();
        (0..self.edges.len())
            .filter(|index| !back.contains(index))
            .collect()
    }

    /// Longest-path depth over the acyclic remainder, so every item sits to the right of
    /// everything that feeds it.
    fn depths(&self, forward: &[usize]) -> Vec<usize> {
        let mut adjacency = vec![Vec::new(); self.items.len()];
        let mut indegree = vec![0_usize; self.items.len()];
        for index in forward {
            let edge = self.edges[*index];
            let source = self.index_by_id[edge.source.as_str()];
            let target = self.index_by_id[edge.target.as_str()];
            adjacency[source].push(target);
            indegree[target] += 1;
        }
        let mut depths = vec![0_usize; self.items.len()];
        let mut queue = (0..self.items.len())
            .filter(|index| indegree[*index] == 0)
            .collect::<VecDeque<_>>();
        while let Some(node) = queue.pop_front() {
            for target in adjacency[node].clone() {
                depths[target] = depths[target].max(depths[node] + 1);
                indegree[target] -= 1;
                if indegree[target] == 0 {
                    queue.push_back(target);
                }
            }
        }
        depths
    }

    /// Place items into columns. Relays never share a column with processing items, so a relay
    /// reads as the port between the nodes on either side of it.
    fn build_columns(&mut self, depths: &[usize]) {
        let max_depth = depths.iter().copied().max().unwrap_or(0);
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
                        key: self.items[item].id.clone(),
                        order: rows.len(),
                        y: 0,
                        weight: 0,
                    });
                    self.item_slot[item] = slot;
                    rows.push(slot);
                }
                self.columns.push(rows);
                column += 1;
            }
        }
    }

    /// Break every forward edge into adjacent-column segments, reserving a row in each column an
    /// edge passes through so nothing else is placed in its way.
    fn build_segments(&mut self, forward: &[usize]) {
        for index in forward {
            let edge = self.edges[*index];
            let source = self.item_slot[self.index_by_id[edge.source.as_str()]];
            let target = self.item_slot[self.index_by_id[edge.target.as_str()]];
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
                    key: format!("{}\u{1}{}\u{1}{index}", edge.source, edge.target),
                    order: self.columns[column].len(),
                    y: 0,
                    weight: 0,
                });
                self.columns[column].push(slot);
                self.segments.push(Segment {
                    edge: *index,
                    from: previous,
                    to: slot,
                });
                previous = slot;
            }
            self.segments.push(Segment {
                edge: *index,
                from: previous,
                to: target,
            });
        }
    }

    /// Order rows within each column to reduce crossings, keeping every branch group's members
    /// contiguous so a group's region can contain exactly its members.
    fn order_columns(&mut self) {
        let mut predecessors = vec![Vec::new(); self.slots.len()];
        let mut successors = vec![Vec::new(); self.slots.len()];
        for segment in &self.segments {
            successors[segment.from].push(segment.to);
            predecessors[segment.to].push(segment.from);
        }

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
                                    i32::try_from(self.slots[*neighbor].order).unwrap_or(i32::MAX),
                                ) * 1000
                            })
                            .sum();
                        total / neighbors.len() as i64
                    };
                }
                self.sort_column(column);
            }
        }
    }

    fn sort_column(&mut self, column: usize) {
        let medians = self.group_medians(column);
        let mut rows = self.columns[column].clone();
        rows.sort_by(|left, right| {
            let left_key = self.column_sort_key(*left, &medians);
            let right_key = self.column_sort_key(*right, &medians);
            left_key.cmp(&right_key)
        });
        for (position, slot) in rows.iter().enumerate() {
            self.slots[*slot].order = position;
        }
        self.columns[column] = rows;
    }

    /// Where each branch group sits in a column, so all of its members sort together.
    fn group_medians(&self, column: usize) -> BTreeMap<String, i64> {
        let mut weights: BTreeMap<String, Vec<i64>> = BTreeMap::new();
        for slot in &self.columns[column] {
            if let Some(branch) = &self.slots[*slot].branch {
                weights
                    .entry(branch.clone())
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

    fn column_sort_key(
        &self,
        slot: usize,
        medians: &BTreeMap<String, i64>,
    ) -> (i64, String, i64, String) {
        let row = &self.slots[slot];
        let (group_weight, group_name) = match &row.branch {
            Some(branch) => (
                medians.get(branch).copied().unwrap_or(row.weight),
                branch.clone(),
            ),
            None => (row.weight, String::new()),
        };
        (group_weight, group_name, row.weight, row.key.clone())
    }

    /// Port offsets, keyed by (slot, edge) and measured from the item's vertical centre. The edge
    /// that continues a straight chain keeps the centre so the chain stays collinear.
    fn assign_ports(&mut self, forward: &[usize]) -> Ports {
        let mut outgoing: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        let mut incoming: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for index in forward {
            let edge = self.edges[*index];
            let source = self.item_slot[self.index_by_id[edge.source.as_str()]];
            let target = self.item_slot[self.index_by_id[edge.target.as_str()]];
            outgoing.entry(source).or_default().push(*index);
            incoming.entry(target).or_default().push(*index);
        }

        let mut ports = Ports::default();
        for (slot, mut edges) in outgoing {
            edges.sort_by_key(|index| self.far_position(*index, true));
            self.record_ports(&mut ports, slot, &edges, true);
        }
        for (slot, mut edges) in incoming {
            edges.sort_by_key(|index| self.far_position(*index, false));
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
            let offset = match pinned {
                Some(centre) => (position as i32 - centre as i32) * PORT_PITCH,
                None => (2 * position as i32 - (edges.len() as i32 - 1)) * PORT_PITCH / 2,
            };
            extent = extent.max(offset.abs());
            ports.offsets.insert((slot, *edge, outgoing), offset);
        }
        let needed = extent * 2 + PORT_PITCH;
        if needed > self.slots[slot].height {
            self.slots[slot].height = needed;
        }
    }

    /// A comparable position for an edge's far endpoint, used to order ports so edges leave and
    /// arrive without crossing each other at the item.
    fn far_position(&self, edge: usize, outgoing: bool) -> (usize, usize, String) {
        let edge = self.edges[edge];
        let id = if outgoing { &edge.target } else { &edge.source };
        let slot = self.item_slot[self.index_by_id[id.as_str()]];
        (
            self.slots[slot].column,
            self.slots[slot].order,
            id.to_string(),
        )
    }

    /// Give every row a y, pulling each item towards the items it connects to so that a straight
    /// run of items lands on one horizontal axis.
    fn assign_rows(&mut self, ports: &Ports) {
        let mut predecessors = vec![Vec::new(); self.slots.len()];
        let mut successors = vec![Vec::new(); self.slots.len()];
        for segment in &self.segments {
            successors[segment.from].push((segment.to, segment.edge));
            predecessors[segment.to].push((segment.from, segment.edge));
        }

        for column in &self.columns.clone() {
            let mut y = 0;
            for slot in column {
                self.slots[*slot].y = y;
                y += self.slots[*slot].height + ROW_GAP;
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
                        .map(|(neighbor, edge)| {
                            let anchor = self.port_y(*neighbor, *edge, downward, ports);
                            anchor - self.slots[*slot].height / 2
                        })
                        .sum();
                    desired.push(total / neighbors.len() as i32);
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
                .get(&(slot, edge, outgoing))
                .copied()
                .unwrap_or(0)
    }

    /// Lay a column out at its wanted positions while keeping the established order and leaving
    /// room between rows.
    fn place_column(&mut self, column: usize, desired: &[i32]) {
        let rows = self.columns[column].clone();
        let mut y = i32::MIN;
        for (position, slot) in rows.iter().enumerate() {
            let wanted = desired[position];
            let placed = if y == i32::MIN { wanted } else { wanted.max(y) };
            self.slots[*slot].y = placed;
            y = placed + self.slots[*slot].height + ROW_GAP;
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
            let mut bottom = i32::MIN;
            for slot in &component {
                self.slots[*slot].y += shift;
                bottom = bottom.max(self.slots[*slot].y + self.slots[*slot].height);
            }
            offset = bottom + BAND_GAP;
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
        for index in &self.feedback {
            let edge = self.edges[*index];
            let source = self.item_slot[self.index_by_id[edge.source.as_str()]];
            let target = self.item_slot[self.index_by_id[edge.target.as_str()]];
            let left = find(&mut parent, source);
            let right = find(&mut parent, target);
            if left != right {
                parent[left] = right;
            }
        }

        let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for slot in 0..self.slots.len() {
            let root = find(&mut parent, slot);
            let name = self.slots[root].key.clone();
            groups.entry(name).or_default().push(slot);
        }
        groups.into_values().collect()
    }

    /// Column x positions, widening each gutter to hold the edges and badges that cross it.
    fn assign_columns_x(&mut self) {
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
                let gutter = SOURCE_PLUG
                    + (lanes[column].max(1) as i32 + 1) * LANE_PITCH
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

    fn route(&self, ports: &Ports) -> Vec<RoutedEdge> {
        let lanes = self.assign_lanes(ports);
        let mut by_edge: BTreeMap<usize, Vec<Segment>> = BTreeMap::new();
        for segment in &self.segments {
            by_edge.entry(segment.edge).or_default().push(*segment);
        }

        let mut routed = Vec::new();
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
                        .get(&(segment.from, segment.edge))
                        .copied()
                        .unwrap_or_else(|| self.default_lane(segment));
                    points.push((lane, start.1));
                    points.push((lane, end.1));
                }
                points.push(end);
            }
            let edge = self.edges[index];
            routed.push(RoutedEdge {
                source: edge.source.clone(),
                target: edge.target.clone(),
                kind: edge.kind,
                badge: edge
                    .badge
                    .then(|| self.badge_rect(segments.last().copied(), ports))
                    .flatten(),
                points: simplify(points),
                feedback: false,
            });
        }

        for index in &self.feedback {
            routed.push(self.route_feedback(*index));
        }
        routed.sort_by(|left, right| {
            left.source
                .cmp(&right.source)
                .then_with(|| left.target.cmp(&right.target))
        });
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
        self.gutter_x
            .get(gutter)
            .copied()
            .map_or(0, |x| x + SOURCE_PLUG + LANE_PITCH)
    }

    /// Give every turning edge its own vertical line inside the gutter, ordered so that no two
    /// edges ever run along the same horizontal line.
    fn assign_lanes(&self, ports: &Ports) -> BTreeMap<(usize, usize), i32> {
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
                let x = gutter_x + SOURCE_PLUG + (position as i32 + 1) * LANE_PITCH;
                lanes.insert((segment.from, segment.edge), x);
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
    fn route_feedback(&self, index: usize) -> RoutedEdge {
        let edge = self.edges[index];
        let source = self.item_slot[self.index_by_id[edge.source.as_str()]];
        let target = self.item_slot[self.index_by_id[edge.target.as_str()]];
        let source_rect = self.slot_rect(source);
        let target_rect = self.slot_rect(target);
        let top = self
            .slots
            .iter()
            .enumerate()
            .map(|(slot, _)| self.slot_rect(slot).y)
            .min()
            .unwrap_or(0);
        let corridor = top - FEEDBACK_PITCH * (1 + index as i32 % 3) - FEEDBACK_PITCH;
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
            feedback: true,
        }
    }

    /// A band per column a branch group spans. Members are contiguous within every column, so
    /// each band holds its members and nothing else.
    fn group_regions(&self) -> Vec<GroupRegion> {
        let mut by_branch: BTreeMap<String, BTreeMap<usize, (i32, i32)>> = BTreeMap::new();
        for slot in 0..self.slots.len() {
            let Some(branch) = self.slots[slot].branch.clone() else {
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
                .or_insert((rect.y, rect.bottom()));
            entry.0 = entry.0.min(rect.y);
            entry.1 = entry.1.max(rect.bottom());
        }

        by_branch
            .into_iter()
            .map(|(branch, columns)| {
                let bands = columns
                    .into_iter()
                    .enumerate()
                    .map(|(position, (column, (top, bottom)))| {
                        let header = if position == 0 {
                            GROUP_HEADER_HEIGHT
                        } else {
                            0
                        };
                        Rect {
                            x: self.column_x[column] - GROUP_PADDING,
                            y: top - GROUP_PADDING - header,
                            width: self.column_width[column] + GROUP_PADDING * 2,
                            height: (bottom - top) + GROUP_PADDING * 2 + header,
                        }
                    })
                    .collect();
                GroupRegion { branch, bands }
            })
            .collect()
    }

    fn finish(self, edges: Vec<RoutedEdge>, groups: Vec<GroupRegion>) -> Layout {
        let mut items = BTreeMap::new();
        let mut min_x = i32::MAX;
        let mut min_y = i32::MAX;
        for slot in 0..self.slots.len() {
            if let Some(item) = self.slots[slot].item {
                let rect = self.slot_rect(slot);
                min_x = min_x.min(rect.x);
                min_y = min_y.min(rect.y);
                items.insert(self.items[item].id.clone(), rect);
            }
        }
        for group in &groups {
            for band in &group.bands {
                min_x = min_x.min(band.x);
                min_y = min_y.min(band.y);
            }
        }
        for edge in &edges {
            for point in &edge.points {
                min_x = min_x.min(point.0);
                min_y = min_y.min(point.1);
            }
        }
        if min_x == i32::MAX {
            min_x = 0;
        }
        if min_y == i32::MAX {
            min_y = 0;
        }
        let shift_x = CANVAS_PADDING - min_x;
        let shift_y = CANVAS_PADDING - min_y;

        let mut layout = Layout {
            items: items
                .into_iter()
                .map(|(id, rect)| {
                    (
                        id,
                        Rect {
                            x: rect.x + shift_x,
                            y: rect.y + shift_y,
                            ..rect
                        },
                    )
                })
                .collect(),
            edges: edges
                .into_iter()
                .map(|edge| RoutedEdge {
                    points: edge
                        .points
                        .into_iter()
                        .map(|(x, y)| (x + shift_x, y + shift_y))
                        .collect(),
                    badge: edge.badge.map(|badge| Rect {
                        x: badge.x + shift_x,
                        y: badge.y + shift_y,
                        ..badge
                    }),
                    ..edge
                })
                .collect(),
            groups: groups
                .into_iter()
                .map(|group| GroupRegion {
                    bands: group
                        .bands
                        .into_iter()
                        .map(|band| Rect {
                            x: band.x + shift_x,
                            y: band.y + shift_y,
                            ..band
                        })
                        .collect(),
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
        for edge in &layout.edges {
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

#[derive(Debug, Default)]
struct Ports {
    /// (slot, edge, outgoing) to the offset of that port from the item's vertical centre.
    offsets: BTreeMap<(usize, usize, bool), i32>,
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
mod tests {
    use super::*;

    fn card(id: &str) -> LayoutItem {
        LayoutItem {
            id: id.to_string(),
            width: 176,
            height: 64,
            relay: false,
            branch: None,
        }
    }

    fn pill(id: &str) -> LayoutItem {
        LayoutItem {
            id: id.to_string(),
            width: 96,
            height: 26,
            relay: true,
            branch: None,
        }
    }

    fn flow(source: &str, target: &str) -> LayoutEdge {
        LayoutEdge {
            source: source.to_string(),
            target: target.to_string(),
            kind: LayoutEdgeKind::Flow,
            badge: true,
        }
    }

    fn quickstart() -> (Vec<LayoutItem>, Vec<LayoutEdge>) {
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
        (items, edges)
    }

    #[test]
    fn every_item_sits_right_of_what_feeds_it() {
        let (items, edges) = quickstart();
        let layout = Layout::build(&items, &edges);
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
        let (items, edges) = quickstart();
        let layout = Layout::build(&items, &edges);
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
        let (items, edges) = quickstart();
        let layout = Layout::build(&items, &edges);
        for edge in &layout.edges {
            for window in edge.points.windows(2) {
                let segment = segment_rect(window[0], window[1]);
                for (id, rect) in &layout.items {
                    if *id == edge.source || *id == edge.target {
                        continue;
                    }
                    assert!(
                        !segment.intersects(rect),
                        "edge {} -> {} crosses {id}",
                        edge.source,
                        edge.target
                    );
                }
            }
        }
    }

    #[test]
    fn no_badge_covers_an_item_or_another_badge() {
        let (items, edges) = quickstart();
        let layout = Layout::build(&items, &edges);
        let badges = layout
            .edges
            .iter()
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
        let layout = Layout::build(&items, &edges);
        let centres = items
            .iter()
            .map(|item| layout.items[&item.id].center_y())
            .collect::<Vec<_>>();
        assert!(
            centres.windows(2).all(|pair| pair[0] == pair[1]),
            "an unbranched pipeline must be collinear, got {centres:?}"
        );
        for edge in &layout.edges {
            assert_eq!(
                edge.points.len(),
                2,
                "chain edge {} -> {} should not bend",
                edge.source,
                edge.target
            );
        }
    }

    #[test]
    fn fan_out_leaves_through_distinct_ports() {
        let (items, edges) = quickstart();
        let layout = Layout::build(&items, &edges);
        let departures = layout
            .edges
            .iter()
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
            LayoutEdge {
                source: "relay:state".to_string(),
                target: "junction:enrich".to_string(),
                kind: LayoutEdgeKind::State,
                badge: false,
            },
            flow("junction:enrich", "relay:out"),
        ];
        let layout = Layout::build(&items, &edges);
        let arrival = layout
            .edges
            .iter()
            .find(|edge| edge.source == "relay:in")
            .expect("the record edge must be routed");
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
        let (items, edges) = quickstart();
        let first = Layout::build(&items, &edges);
        let second = Layout::build(&items, &edges);
        assert_eq!(format!("{first:?}"), format!("{second:?}"));
    }

    #[test]
    fn branch_group_bands_hold_members_and_nothing_else() {
        let mut items = vec![
            card("ingestor:source"),
            card("emitter:sink"),
            card("emitter:other"),
        ];
        let mut branched = pill("relay:branched");
        branched.branch = Some("by_tenant".to_string());
        let mut processor = card("junction:split");
        processor.branch = Some("by_tenant".to_string());
        items.push(branched);
        items.push(processor);
        let edges = vec![
            flow("ingestor:source", "relay:branched"),
            flow("relay:branched", "junction:split"),
            flow("junction:split", "emitter:sink"),
            flow("ingestor:source", "emitter:other"),
        ];
        let layout = Layout::build(&items, &edges);
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
        let layout = Layout::build(&items, &edges);
        let returns = layout
            .edges
            .iter()
            .filter(|edge| edge.feedback)
            .collect::<Vec<_>>();
        assert_eq!(returns.len(), 1, "exactly one edge should close the loop");
        assert_eq!(returns[0].source, "reingestor:loop");
        assert!(layout.items["relay:a"].x < layout.items["reingestor:loop"].x);
    }

    #[test]
    fn disconnected_parts_are_stacked_without_overlapping() {
        let items = vec![card("ingestor:a"), pill("relay:a"), pill("relay:lonely")];
        let edges = vec![flow("ingestor:a", "relay:a")];
        let layout = Layout::build(&items, &edges);
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
        let layout = Layout::build(&items, &[]);
        assert_eq!(layout.items.len(), 1);
        assert!(layout.width > 0 && layout.height > 0);
    }

    #[test]
    fn an_empty_graph_has_no_geometry() {
        let layout = Layout::build(&[], &[]);
        assert!(layout.items.is_empty());
        assert_eq!(layout.width, 0);
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
}
