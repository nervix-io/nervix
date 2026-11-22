//! Layer: engines and infrastructure.
//! Owns: `nervix_message_errors_total`, the per-node counters of every error a graph node handled
//! by class and operation, and the collector that renders the cells a node has produced.
//! Depends on: the error vocabulary, node references, Prometheus and the primitive synchronization
//! boundary.
//! Must not know: how an error is handled, routed, logged or published.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "records supplied message error observations")
)]

use meticulous::ResultExt as _;
use nervix_approx_into::ApproxInto as _;
use nervix_models::{DomainName, MessageErrorClass, MessageErrorOperation, NodeRef};
use nervix_primitives::{
    collections::{DashMap, dash_map::Entry},
    sync::{
        Arc, StdArc, StdWeak,
        atomic::{AtomicU64, Ordering},
    },
};
use prometheus::{
    Opts, Registry,
    core::{Collector, Desc},
    proto::{Counter, LabelPair, Metric, MetricFamily, MetricType},
};

pub(super) const MESSAGE_ERRORS_TOTAL: &str = "nervix_message_errors_total";
const MESSAGE_ERRORS_HELP: &str = "Total errors a graph node handled, by the error's class and \
                                   the operation that failed, whatever its route's or node's \
                                   policy did with them.";
/// The family's labels, sorted by name as the registry renders the labels of every family.
const LABEL_NAMES: [&str; 5] = ["class", "domain", "node", "node_kind", "operation"];

/// The node a group of counters belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NodeErrorsKey {
    domain: DomainName,
    node: NodeRef,
}

/// Every node's counters, which a domain's routing resolves when its revision is installed and the
/// collector reads when the node's metrics are scraped.
#[derive(Debug)]
pub(crate) struct MessageErrorMetrics {
    /// Only the handles a live node's routing or intake retains keep its counters alive.
    nodes: DashMap<NodeErrorsKey, StdWeak<NodeErrorCells>>,
    descriptor: Desc,
}

impl MessageErrorMetrics {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers the family's collector once per node")
    )]
    pub(super) fn register(registry: &Registry) -> Arc<Self> {
        let opts = Opts::new(MESSAGE_ERRORS_TOTAL, MESSAGE_ERRORS_HELP);
        let descriptor = Desc::new(
            opts.fq_name(),
            opts.help.clone(),
            LABEL_NAMES.map(str::to_owned).to_vec(),
            Default::default(),
        )
        .assured("the message error family's name, help and label names are fixed identifiers");
        let metrics = Arc::new(Self {
            nodes: DashMap::default(),
            descriptor,
        });
        registry
            .register(Box::new(MessageErrorCollector {
                metrics: metrics.clone(),
            }))
            .assured("the message error family registers once per node");
        metrics
    }

    /// The counters of `node` in `domain`, shared by every holder that resolves them while one of
    /// them is alive.
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "binds a node's retained error counters once")
    )]
    pub(crate) fn node(
        metrics: &Arc<Self>,
        domain: &DomainName,
        node: &NodeRef,
    ) -> NodeMessageErrors {
        let key = NodeErrorsKey {
            domain: domain.clone(),
            node: node.clone(),
        };
        match metrics.nodes.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                if let Some(cells) = entry.get().upgrade() {
                    return NodeMessageErrors(cells);
                }
                let cells = NodeErrorCells::new(metrics.clone(), key);
                entry.insert(StdArc::downgrade(&cells));
                NodeMessageErrors(cells)
            }
            Entry::Vacant(entry) => {
                let cells = NodeErrorCells::new(metrics.clone(), key);
                entry.insert(StdArc::downgrade(&cells));
                NodeMessageErrors(cells)
            }
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "withdraws the exact final node recorder")
    )]
    fn retire(&self, key: &NodeErrorsKey, identity: *const NodeErrorCells) {
        if let Entry::Occupied(entry) = self.nodes.entry(key.clone())
            && std::ptr::eq(entry.get().as_ptr(), identity)
        {
            entry.remove();
        }
    }

    /// Every non-zero cell of every live node, as the family's series.
    #[cfg_attr(
        nervix_lint,
        nervix::context(observer, reason = "renders retained counters at a scrape")
    )]
    fn family(&self) -> MetricFamily {
        let mut family = MetricFamily::default();
        family.set_name(self.descriptor.fq_name.clone());
        family.set_help(self.descriptor.help.clone());
        family.set_field_type(MetricType::COUNTER);
        // The live nodes are taken out of the map before anything is rendered: a node whose last
        // other holder drops while it is being rendered retires through the map, which must not
        // still be locked by this iteration then.
        let mut live = Vec::new();
        for entry in self.nodes.iter() {
            if let Some(cells) = entry.value().upgrade() {
                live.push(cells);
            }
        }
        let mut metrics = Vec::new();
        for cells in &live {
            cells.render(&mut metrics);
        }
        drop(live);
        family.set_metric(metrics);
        family
    }
}

/// One node's counters: a handle its routing or intake holds and counts every error through. It is
/// a standard `Arc` because the family's map keeps only a weak reference to the cells, so that the
/// node's series leave with its last holder.
#[derive(Debug, Clone)]
pub(crate) struct NodeMessageErrors(StdArc<NodeErrorCells>);

impl NodeMessageErrors {
    /// Counts one error of `class` that `operation` raised on the node.
    pub(crate) fn count(&self, class: MessageErrorClass, operation: MessageErrorOperation) {
        let cell = &self.0.cells[class.index()][operation.index()];
        // A standalone counter: it orders nothing else, and a scrape reads whatever total it
        // finds.
        cell.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct NodeErrorCells {
    metrics: Arc<MessageErrorMetrics>,
    key: NodeErrorsKey,
    cells: [[AtomicU64; MessageErrorOperation::ALL.len()]; MessageErrorClass::ALL.len()],
}

impl NodeErrorCells {
    fn new(metrics: Arc<MessageErrorMetrics>, key: NodeErrorsKey) -> StdArc<Self> {
        StdArc::new(Self {
            metrics,
            key,
            cells: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
        })
    }

    fn render(&self, metrics: &mut Vec<Metric>) {
        for class in MessageErrorClass::ALL {
            for operation in MessageErrorOperation::ALL {
                let total = self.cells[class.index()][operation.index()].load(Ordering::Relaxed);
                if total == 0 {
                    continue;
                }
                // In `LABEL_NAMES` order, sorted by name as every family of the registry renders.
                let labels = [
                    ("class", class.as_str()),
                    ("domain", self.key.domain.as_str()),
                    ("node", self.key.node.identifier.as_str()),
                    ("node_kind", self.key.node.kind.as_str()),
                    ("operation", operation.as_ref()),
                ]
                .map(|(name, value)| {
                    let mut label = LabelPair::default();
                    label.set_name(name.to_owned());
                    label.set_value(value.to_owned());
                    label
                });
                let mut counter = Counter::default();
                counter.set_value(total.approx_into());
                let mut metric = Metric::default();
                metric.set_label(labels.to_vec());
                metric.set_counter(counter);
                metrics.push(metric);
            }
        }
    }
}

impl Drop for NodeErrorCells {
    fn drop(&mut self) {
        self.metrics.retire(&self.key, std::ptr::from_ref(self));
    }
}

#[derive(Debug)]
struct MessageErrorCollector {
    metrics: Arc<MessageErrorMetrics>,
}

impl Collector for MessageErrorCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.metrics.descriptor]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        vec![self.metrics.family()]
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{ModelKind, ModelName};

    use super::*;

    fn node(name: &str) -> NodeRef {
        NodeRef::new(
            ModelKind::Junction,
            ModelName::parse(name).expect("a valid node name"),
        )
    }

    fn rendered(registry: &Registry) -> Vec<(Vec<(String, String)>, f64)> {
        let mut series = Vec::new();
        for family in registry.gather() {
            if family.name() != MESSAGE_ERRORS_TOTAL {
                continue;
            }
            for metric in family.get_metric() {
                let labels = metric
                    .get_label()
                    .iter()
                    .map(|label| (label.name().to_owned(), label.value().to_owned()))
                    .collect();
                series.push((labels, metric.get_counter().value()));
            }
        }
        series
    }

    #[test]
    fn a_node_exposes_only_the_classes_and_operations_it_produced_and_retires_with_its_last_holder()
    {
        let registry = Registry::new();
        let metrics = MessageErrorMetrics::register(&registry);
        let domain = DomainName::parse("shop").expect("a valid domain");
        let scorer = MessageErrorMetrics::node(&metrics, &domain, &node("scorer"));
        let again = MessageErrorMetrics::node(&metrics, &domain, &node("scorer"));
        scorer.count(MessageErrorClass::EVALUATION, MessageErrorOperation::Set);
        again.count(MessageErrorClass::EVALUATION, MessageErrorOperation::Set);
        scorer.count(MessageErrorClass::REFUSED, MessageErrorOperation::Publish);
        let label = |name: &str, value: &str| (name.to_owned(), value.to_owned());
        let mut series = rendered(&registry);
        series.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            series,
            vec![
                (
                    vec![
                        label("class", "evaluation"),
                        label("domain", "shop"),
                        label("node", "scorer"),
                        label("node_kind", "junction"),
                        label("operation", "set"),
                    ],
                    2.0
                ),
                (
                    vec![
                        label("class", "refused"),
                        label("domain", "shop"),
                        label("node", "scorer"),
                        label("node_kind", "junction"),
                        label("operation", "publish"),
                    ],
                    1.0
                ),
            ]
        );
        drop(scorer);
        assert_eq!(
            rendered(&registry).len(),
            2,
            "a holder still keeps the node"
        );
        drop(again);
        assert!(
            rendered(&registry).is_empty(),
            "the node's series leave with it"
        );
        assert!(metrics.nodes.is_empty());
        let renewed = MessageErrorMetrics::node(&metrics, &domain, &node("scorer"));
        assert!(
            rendered(&registry).is_empty(),
            "a new holder starts at zero"
        );
        drop(renewed);
    }
}
