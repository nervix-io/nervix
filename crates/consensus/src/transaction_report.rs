//! Revisioned transaction-impact reports stored as bounded keyed records.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Report revision archives, topology deduplication, keyed persistence and assembly.
//! - **Depends on.** Transaction-impact vocabulary and consensus keyed records.
//! - **Must not know.** Parsing, planning, sessions, runtime execution or presentation.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

use error_stack::Report;
use fjall::Keyspace;
use nervix_models::{
    AffectedTopology, AttributedImpactNode, CanonicalImpactSet, DomainName,
    ExecutionStepImpactReport, ImpactReportCompleteness, ImpactTopology, ImpactTopologyEdge,
    OperationImpactReport, TransactionImpactReport, TransactionPreviewIdentity,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{durable_batch::DurableBatch, records::Records};

const REPORT_HEADER_TAG: u8 = b'h';
const REPORT_OPERATION_TAG: u8 = b'i';
const REPORT_STEP_TAG: u8 = b'j';
const TOPOLOGY_HEADER_TAG: u8 = b'k';
const TOPOLOGY_NODE_TAG: u8 = b'l';
const TOPOLOGY_EDGE_TAG: u8 = b'q';

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ImpactTopologyId([u8; 32]);

impl ImpactTopologyId {
    fn for_topology(
        topology: &ImpactTopology,
    ) -> Result<Self, Report<TransactionReportArchiveError>> {
        let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(topology).map_err(|error| {
            Report::new(TransactionReportArchiveError::EncodeTopology).attach(error)
        })?;
        Ok(Self(*blake3::hash(&encoded).as_bytes()))
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
struct StoredAffectedTopology {
    before: ImpactTopologyId,
    after: ImpactTopologyId,
}

impl StoredAffectedTopology {
    fn archive(
        topology: AffectedTopology,
        graphs: &mut BTreeMap<ImpactTopologyId, ImpactTopology>,
    ) -> Result<Self, Report<TransactionReportArchiveError>> {
        let before = Self::archive_graph(topology.before, graphs)?;
        let after = Self::archive_graph(topology.after, graphs)?;
        Ok(Self { before, after })
    }

    fn archive_graph(
        topology: ImpactTopology,
        graphs: &mut BTreeMap<ImpactTopologyId, ImpactTopology>,
    ) -> Result<ImpactTopologyId, Report<TransactionReportArchiveError>> {
        let id = ImpactTopologyId::for_topology(&topology)?;
        if let Some(existing) = graphs.get(&id) {
            if existing != &topology {
                return Err(Report::new(
                    TransactionReportArchiveError::TopologyHashCollision,
                ));
            }
        } else {
            graphs.insert(id, topology);
        }
        Ok(id)
    }

    fn graph_ids(&self) -> [ImpactTopologyId; 2] {
        [self.before, self.after]
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
struct StoredOperationImpactReport {
    report: OperationImpactReport,
    topology: StoredAffectedTopology,
}

impl StoredOperationImpactReport {
    fn archive(
        mut report: OperationImpactReport,
        graphs: &mut BTreeMap<ImpactTopologyId, ImpactTopology>,
    ) -> Result<Self, Report<TransactionReportArchiveError>> {
        let topology = StoredAffectedTopology::archive(
            std::mem::take(&mut report.contribution.topology),
            graphs,
        )?;
        Ok(Self { report, topology })
    }

    fn restore(
        &self,
        records: &TransactionReportRecords,
    ) -> error_stack::Result<OperationImpactReport, TransactionReportReadError> {
        let mut report = self.report.clone();
        report.contribution.topology = records.restore_topology(&self.topology)?;
        Ok(report)
    }

    fn graph_ids(&self) -> [ImpactTopologyId; 2] {
        self.topology.graph_ids()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
struct StoredExecutionStepImpactReport {
    report: ExecutionStepImpactReport,
    planned_topology: StoredAffectedTopology,
    actual_topology: StoredAffectedTopology,
}

impl StoredExecutionStepImpactReport {
    fn archive(
        report: ExecutionStepImpactReport,
        graphs: &mut BTreeMap<ImpactTopologyId, ImpactTopology>,
    ) -> Result<Self, Report<TransactionReportArchiveError>> {
        let mut planned = report.planned().clone();
        let planned_topology =
            StoredAffectedTopology::archive(std::mem::take(&mut planned.effects.topology), graphs)?;
        let mut actual = report.actual().clone();
        let actual_topology =
            StoredAffectedTopology::archive(std::mem::take(&mut actual.effects.topology), graphs)?;
        let report = ExecutionStepImpactReport::new(report.operations(), planned, actual);
        Ok(Self {
            report,
            planned_topology,
            actual_topology,
        })
    }

    fn restore(
        &self,
        records: &TransactionReportRecords,
    ) -> error_stack::Result<ExecutionStepImpactReport, TransactionReportReadError> {
        let mut planned = self.report.planned().clone();
        planned.effects.topology = records.restore_topology(&self.planned_topology)?;
        let mut actual = self.report.actual().clone();
        actual.effects.topology = records.restore_topology(&self.actual_topology)?;
        Ok(ExecutionStepImpactReport::new(
            self.report.operations(),
            planned,
            actual,
        ))
    }

    fn graph_ids(&self) -> [ImpactTopologyId; 4] {
        let planned = self.planned_topology.graph_ids();
        let actual = self.actual_topology.graph_ids();
        [planned[0], planned[1], actual[0], actual[1]]
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
struct TransactionReportHeader {
    identity: TransactionPreviewIdentity,
    domain: DomainName,
    completeness: ImpactReportCompleteness,
    operation_count: usize,
    execution_step_count: usize,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
struct ArchivedImpactTopology {
    id: ImpactTopologyId,
    topology: ImpactTopology,
}

/// One report revision prepared for a replicated mutation.
///
/// The archive removes before/after topology from operation and step records, and carries each
/// unique topology once. The state machine persists those graphs as individual node and edge
/// records before it links the report revision from a transaction.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionReportArchive {
    header: TransactionReportHeader,
    operations: Vec<StoredOperationImpactReport>,
    execution_steps: Vec<StoredExecutionStepImpactReport>,
    graphs: Vec<ArchivedImpactTopology>,
}

impl TransactionReportArchive {
    pub fn new(
        transaction_id: String,
        report: TransactionImpactReport,
    ) -> Result<Self, Report<TransactionReportArchiveError>> {
        let identity = TransactionPreviewIdentity {
            transaction_id,
            position: report.position(),
            planning_basis: report.planning_basis(),
        };
        let header = TransactionReportHeader {
            identity,
            domain: report.domain().clone(),
            completeness: report.completeness().clone(),
            operation_count: report.operations().len(),
            execution_step_count: report.execution_steps().len(),
        };
        let mut graphs = BTreeMap::new();
        let mut operations = Vec::with_capacity(report.operations().len());
        for operation in report.operations().iter().cloned() {
            operations.push(StoredOperationImpactReport::archive(
                operation,
                &mut graphs,
            )?);
        }
        let mut execution_steps = Vec::with_capacity(report.execution_steps().len());
        for step in report.execution_steps().iter().cloned() {
            execution_steps.push(StoredExecutionStepImpactReport::archive(step, &mut graphs)?);
        }
        let graphs = graphs
            .into_iter()
            .map(|(id, topology)| ArchivedImpactTopology { id, topology })
            .collect();
        Ok(Self {
            header,
            operations,
            execution_steps,
            graphs,
        })
    }

    pub fn identity(&self) -> &TransactionPreviewIdentity {
        &self.header.identity
    }

    pub fn domain(&self) -> &DomainName {
        &self.header.domain
    }

    pub fn is_complete(&self) -> bool {
        self.header.completeness.is_complete()
    }

    pub fn matches_commit_plan(&self, plan: &crate::TransactionCommitPlan) -> bool {
        self.header.execution_step_count == plan.steps.len()
            && self
                .execution_steps
                .iter()
                .zip(&plan.steps)
                .all(|(report, step)| {
                    let mut graphs = BTreeMap::new();
                    let Ok(planned) =
                        StoredExecutionStepImpactReport::archive(step.impact.clone(), &mut graphs)
                    else {
                        return false;
                    };
                    report == &planned
                })
    }
}

#[derive(Debug, Error)]
pub enum TransactionReportArchiveError {
    #[error("failed to encode transaction impact topology")]
    EncodeTopology,
    #[error("two distinct transaction impact topologies have the same content identity")]
    TopologyHashCollision,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct TransactionReportItemKey {
    identity: TransactionPreviewIdentity,
    index: usize,
}

impl TransactionReportItemKey {
    fn new(identity: &TransactionPreviewIdentity, index: usize) -> Self {
        Self {
            identity: identity.clone(),
            index,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct ImpactTopologyItemKey {
    topology: ImpactTopologyId,
    index: usize,
}

impl ImpactTopologyItemKey {
    fn new(topology: ImpactTopologyId, index: usize) -> Self {
        Self { topology, index }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
struct ImpactTopologyHeader {
    node_count: usize,
    edge_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TransactionReportRecords {
    headers: Records<TransactionPreviewIdentity, TransactionReportHeader>,
    operations: Records<TransactionReportItemKey, StoredOperationImpactReport>,
    execution_steps: Records<TransactionReportItemKey, StoredExecutionStepImpactReport>,
    topology_headers: Records<ImpactTopologyId, ImpactTopologyHeader>,
    topology_nodes: Records<ImpactTopologyItemKey, AttributedImpactNode>,
    topology_edges: Records<ImpactTopologyItemKey, ImpactTopologyEdge>,
}

impl TransactionReportRecords {
    pub(crate) fn load(keyspace: &Keyspace) -> io::Result<Self> {
        Ok(Self {
            headers: Records::load(REPORT_HEADER_TAG, keyspace)?,
            operations: Records::load(REPORT_OPERATION_TAG, keyspace)?,
            execution_steps: Records::load(REPORT_STEP_TAG, keyspace)?,
            topology_headers: Records::load(TOPOLOGY_HEADER_TAG, keyspace)?,
            topology_nodes: Records::load(TOPOLOGY_NODE_TAG, keyspace)?,
            topology_edges: Records::load(TOPOLOGY_EDGE_TAG, keyspace)?,
        })
    }

    pub(crate) fn write_changes(
        &self,
        preceding: &Self,
        batch: &mut DurableBatch<'_>,
        keyspace: &Keyspace,
    ) -> io::Result<()> {
        self.headers
            .write_changes(&preceding.headers, REPORT_HEADER_TAG, batch, keyspace)?;
        self.operations.write_changes(
            &preceding.operations,
            REPORT_OPERATION_TAG,
            batch,
            keyspace,
        )?;
        self.execution_steps.write_changes(
            &preceding.execution_steps,
            REPORT_STEP_TAG,
            batch,
            keyspace,
        )?;
        self.topology_headers.write_changes(
            &preceding.topology_headers,
            TOPOLOGY_HEADER_TAG,
            batch,
            keyspace,
        )?;
        self.topology_nodes.write_changes(
            &preceding.topology_nodes,
            TOPOLOGY_NODE_TAG,
            batch,
            keyspace,
        )?;
        self.topology_edges.write_changes(
            &preceding.topology_edges,
            TOPOLOGY_EDGE_TAG,
            batch,
            keyspace,
        )
    }

    pub(crate) fn insert(
        &mut self,
        archive: TransactionReportArchive,
    ) -> error_stack::Result<(), TransactionReportStoreError> {
        self.validate_archive(&archive)?;
        let mut candidate = self.clone();
        candidate.insert_graphs(archive.graphs)?;
        let identity = archive.header.identity.clone();
        for (index, operation) in archive.operations.into_iter().enumerate() {
            candidate
                .operations
                .insert(TransactionReportItemKey::new(&identity, index), operation);
        }
        for (index, step) in archive.execution_steps.into_iter().enumerate() {
            candidate
                .execution_steps
                .insert(TransactionReportItemKey::new(&identity, index), step);
        }
        candidate.headers.insert(identity.clone(), archive.header);
        candidate
            .report(&identity)
            .map_err(|_| Report::new(TransactionReportStoreError::InvalidArchive))?;
        *self = candidate;
        Ok(())
    }

    fn insert_graphs(
        &mut self,
        graphs: Vec<ArchivedImpactTopology>,
    ) -> error_stack::Result<(), TransactionReportStoreError> {
        for graph in graphs {
            if self.topology_headers.contains_key(&graph.id) {
                continue;
            }
            let header = ImpactTopologyHeader {
                node_count: graph.topology.nodes.len(),
                edge_count: graph.topology.edges.len(),
            };
            for (index, node) in graph.topology.nodes.into_iter().enumerate() {
                self.topology_nodes
                    .insert(ImpactTopologyItemKey::new(graph.id, index), node);
            }
            for (index, edge) in graph.topology.edges.into_iter().enumerate() {
                self.topology_edges
                    .insert(ImpactTopologyItemKey::new(graph.id, index), edge);
            }
            self.topology_headers.insert(graph.id, header);
        }
        Ok(())
    }

    pub(crate) fn replace_execution_step(
        &mut self,
        identity: &TransactionPreviewIdentity,
        impact: ExecutionStepImpactReport,
    ) -> error_stack::Result<(), TransactionReportStoreError> {
        let header = self
            .headers
            .get(identity)
            .ok_or_else(|| Report::new(TransactionReportStoreError::UnknownRevision))?;
        let step_index = (0..header.execution_step_count)
            .find(|index| {
                self.execution_steps
                    .get(&TransactionReportItemKey::new(identity, *index))
                    .is_some_and(|step| step.report.operations() == impact.operations())
            })
            .ok_or_else(|| Report::new(TransactionReportStoreError::StepMismatch))?;
        let mut graphs = BTreeMap::new();
        let stored = StoredExecutionStepImpactReport::archive(impact, &mut graphs)
            .map_err(|_| Report::new(TransactionReportStoreError::InvalidArchive))?;
        let graphs = graphs
            .into_iter()
            .map(|(id, topology)| ArchivedImpactTopology { id, topology })
            .collect();
        let mut candidate = self.clone();
        candidate.insert_graphs(graphs)?;
        candidate
            .execution_steps
            .insert(TransactionReportItemKey::new(identity, step_index), stored);
        candidate
            .report(identity)
            .map_err(|_| Report::new(TransactionReportStoreError::InvalidArchive))?;
        *self = candidate;
        Ok(())
    }

    fn validate_archive(
        &self,
        archive: &TransactionReportArchive,
    ) -> error_stack::Result<(), TransactionReportStoreError> {
        if archive.header.operation_count != archive.operations.len()
            || archive.header.execution_step_count != archive.execution_steps.len()
        {
            return Err(Report::new(TransactionReportStoreError::InvalidArchive));
        }
        let mut supplied_graphs = BTreeMap::new();
        for graph in &archive.graphs {
            let id = ImpactTopologyId::for_topology(&graph.topology)
                .map_err(|_| Report::new(TransactionReportStoreError::InvalidArchive))?;
            if id != graph.id {
                return Err(Report::new(TransactionReportStoreError::InvalidArchive));
            }
            if let Some(supplied) = supplied_graphs.insert(id, &graph.topology)
                && supplied != &graph.topology
            {
                return Err(Report::new(TransactionReportStoreError::ConflictingArchive));
            }
            if self.topology_headers.contains_key(&id) {
                let retained = self
                    .restore_graph(id)
                    .map_err(|_| Report::new(TransactionReportStoreError::InvalidArchive))?;
                if retained != graph.topology {
                    return Err(Report::new(TransactionReportStoreError::ConflictingArchive));
                }
            }
        }
        let graph_available = |id: ImpactTopologyId| {
            supplied_graphs.contains_key(&id) || self.topology_headers.contains_key(&id)
        };
        let every_graph_available = archive
            .operations
            .iter()
            .flat_map(StoredOperationImpactReport::graph_ids)
            .chain(
                archive
                    .execution_steps
                    .iter()
                    .flat_map(StoredExecutionStepImpactReport::graph_ids),
            )
            .all(graph_available);
        if !every_graph_available {
            return Err(Report::new(TransactionReportStoreError::InvalidArchive));
        }
        if let Some(existing) = self.headers.get(&archive.header.identity) {
            if existing != &archive.header {
                return Err(Report::new(TransactionReportStoreError::ConflictingArchive));
            }
            for (index, operation) in archive.operations.iter().enumerate() {
                let key = TransactionReportItemKey::new(&archive.header.identity, index);
                if self.operations.get(&key) != Some(operation) {
                    return Err(Report::new(TransactionReportStoreError::ConflictingArchive));
                }
            }
            for (index, step) in archive.execution_steps.iter().enumerate() {
                let key = TransactionReportItemKey::new(&archive.header.identity, index);
                if self.execution_steps.get(&key) != Some(step) {
                    return Err(Report::new(TransactionReportStoreError::ConflictingArchive));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn report(
        &self,
        identity: &TransactionPreviewIdentity,
    ) -> error_stack::Result<TransactionImpactReport, TransactionReportReadError> {
        let header = self
            .headers
            .get(identity)
            .ok_or_else(|| Report::new(TransactionReportReadError::UnknownRevision))?;
        let mut operations = Vec::with_capacity(header.operation_count);
        for index in 0..header.operation_count {
            let key = TransactionReportItemKey::new(identity, index);
            let operation = self.operations.get(&key).ok_or_else(|| {
                Report::new(TransactionReportReadError::MissingOperation { index })
            })?;
            operations.push(operation.restore(self)?);
        }
        let mut execution_steps = Vec::with_capacity(header.execution_step_count);
        for index in 0..header.execution_step_count {
            let key = TransactionReportItemKey::new(identity, index);
            let step = self.execution_steps.get(&key).ok_or_else(|| {
                Report::new(TransactionReportReadError::MissingExecutionStep { index })
            })?;
            execution_steps.push(step.restore(self)?);
        }
        TransactionImpactReport::new(
            header.domain.clone(),
            identity.position,
            identity.planning_basis,
            header.completeness.clone(),
            operations,
            execution_steps,
        )
        .map_err(|_| Report::new(TransactionReportReadError::InvalidReport))
    }

    fn restore_topology(
        &self,
        topology: &StoredAffectedTopology,
    ) -> error_stack::Result<AffectedTopology, TransactionReportReadError> {
        Ok(AffectedTopology {
            before: self.restore_graph(topology.before)?,
            after: self.restore_graph(topology.after)?,
        })
    }

    fn restore_graph(
        &self,
        id: ImpactTopologyId,
    ) -> error_stack::Result<ImpactTopology, TransactionReportReadError> {
        let header = self
            .topology_headers
            .get(&id)
            .ok_or_else(|| Report::new(TransactionReportReadError::MissingTopology))?;
        let mut nodes = Vec::with_capacity(header.node_count);
        for index in 0..header.node_count {
            let key = ImpactTopologyItemKey::new(id, index);
            let node = self
                .topology_nodes
                .get(&key)
                .ok_or_else(|| Report::new(TransactionReportReadError::MissingTopology))?;
            nodes.push(node.clone());
        }
        let mut edges = Vec::with_capacity(header.edge_count);
        for index in 0..header.edge_count {
            let key = ImpactTopologyItemKey::new(id, index);
            let edge = self
                .topology_edges
                .get(&key)
                .ok_or_else(|| Report::new(TransactionReportReadError::MissingTopology))?;
            edges.push(edge.clone());
        }
        Ok(ImpactTopology {
            nodes: CanonicalImpactSet::new(nodes),
            edges: CanonicalImpactSet::new(edges),
        })
    }

    pub(crate) fn remove_transaction(&mut self, transaction_id: &str) {
        self.headers
            .retain(|identity, _| identity.transaction_id != transaction_id);
        self.operations
            .retain(|key, _| key.identity.transaction_id != transaction_id);
        self.execution_steps
            .retain(|key, _| key.identity.transaction_id != transaction_id);
        self.collect_unreferenced_topologies();
    }

    pub(crate) fn retain_revision(&mut self, retained: &TransactionPreviewIdentity) {
        self.headers.retain(|identity, _| {
            identity.transaction_id != retained.transaction_id || identity == retained
        });
        self.operations.retain(|key, _| {
            key.identity.transaction_id != retained.transaction_id || key.identity == *retained
        });
        self.execution_steps.retain(|key, _| {
            key.identity.transaction_id != retained.transaction_id || key.identity == *retained
        });
        self.collect_unreferenced_topologies();
    }

    fn collect_unreferenced_topologies(&mut self) {
        let referenced = self
            .operations
            .values()
            .flat_map(StoredOperationImpactReport::graph_ids)
            .chain(
                self.execution_steps
                    .values()
                    .flat_map(StoredExecutionStepImpactReport::graph_ids),
            )
            .collect::<BTreeSet<_>>();
        self.topology_headers
            .retain(|id, _| referenced.contains(id));
        self.topology_nodes
            .retain(|key, _| referenced.contains(&key.topology));
        self.topology_edges
            .retain(|key, _| referenced.contains(&key.topology));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransactionReportStoreError {
    #[error("transaction report archive is incomplete or internally inconsistent")]
    InvalidArchive,
    #[error("transaction report archive conflicts with retained content at the same identity")]
    ConflictingArchive,
    #[error("transaction report revision is unknown")]
    UnknownRevision,
    #[error("transaction report execution step does not match the frozen plan")]
    StepMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransactionReportReadError {
    #[error("transaction report revision is unknown")]
    UnknownRevision,
    #[error("transaction report is missing operation record {index}")]
    MissingOperation { index: usize },
    #[error("transaction report is missing execution-step record {index}")]
    MissingExecutionStep { index: usize },
    #[error("transaction report is missing retained topology content")]
    MissingTopology,
    #[error("retained transaction report does not satisfy report invariants")]
    InvalidReport,
}

#[cfg(test)]
pub(crate) fn test_report(domain: &DomainName, operation_count: usize) -> TransactionImpactReport {
    test_report_with_completeness(domain, operation_count, ImpactReportCompleteness::Complete)
}

#[cfg(test)]
fn test_report_with_completeness(
    domain: &DomainName,
    operation_count: usize,
    completeness: ImpactReportCompleteness,
) -> TransactionImpactReport {
    use meticulous::ResultExt as _;
    use nervix_models::{
        ActualExecutionStepImpact, AffectedTopology, AttributedImpactNode, CanonicalImpactSet,
        ConcreteBranchCoverage, ImpactAttribution, ImpactEffects, ImpactNodeCoverage,
        ImpactTopology, ModelKind, ModelName, NodeRef, OperationImpactReport, PauseRequirement,
        PlannedExecutionStepImpact, TransactionOperation, TransactionOperationNumber,
        TransactionOperationRange, TransactionPosition,
    };

    let mut operations = Vec::with_capacity(operation_count);
    let mut steps = Vec::with_capacity(operation_count);
    for index in 0..operation_count {
        let number = TransactionOperationNumber::from_index(index)
            .assured("test report indexes are addressable operation numbers");
        let range = TransactionOperationRange::from_index_and_count(index, 1)
            .assured("each test report step contains one operation");
        let topology = ImpactTopology {
            nodes: CanonicalImpactSet::new([AttributedImpactNode {
                coverage: ImpactNodeCoverage::execution(
                    NodeRef::new(
                        ModelKind::Relay,
                        ModelName::parse("retained_report_relay")
                            .assured("the test relay is an identifier-shaped literal"),
                    ),
                    ConcreteBranchCoverage::Unbranched,
                ),
                attribution: ImpactAttribution::single(number),
            }]),
            edges: CanonicalImpactSet::default(),
        };
        let effects = ImpactEffects {
            topology: AffectedTopology {
                before: topology.clone(),
                after: topology,
            },
            ..ImpactEffects::default()
        };
        operations.push(OperationImpactReport {
            number,
            operation: TransactionOperation::StartDomain {
                domain: domain.clone(),
            },
            execution_step: range,
            completeness: ImpactReportCompleteness::Complete,
            reasons: Vec::new(),
            contribution: effects.clone(),
        });
        steps.push(ExecutionStepImpactReport::new(
            range,
            PlannedExecutionStepImpact {
                completeness: ImpactReportCompleteness::Complete,
                pause: PauseRequirement::NoPause,
                effects,
            },
            ActualExecutionStepImpact::unattempted(),
        ));
    }
    TransactionImpactReport::new(
        domain.clone(),
        TransactionPosition::new(operation_count),
        nervix_models::ImpactPlanningBasis::new([1; 32]),
        completeness,
        operations,
        steps,
    )
    .assured("the test report uses consecutive operations and matching steps")
}

#[cfg(test)]
pub(crate) fn test_report_archive(
    transaction_id: &str,
    domain: &DomainName,
    operation_count: usize,
) -> TransactionReportArchive {
    use meticulous::ResultExt as _;

    TransactionReportArchive::new(
        transaction_id.to_string(),
        test_report(domain, operation_count),
    )
    .assured("the test report topology can be archived")
}

#[cfg(test)]
pub(crate) fn test_incomplete_report_archive(
    transaction_id: &str,
    domain: &DomainName,
    operation_count: usize,
) -> TransactionReportArchive {
    use meticulous::ResultExt as _;
    use nervix_models::{ImpactDiagnostic, ImpactDiagnosticKind, TransactionOperationNumber};

    let completeness = ImpactReportCompleteness::incomplete(vec![ImpactDiagnostic {
        kind: ImpactDiagnosticKind::Planning,
        operation: (operation_count > 0).then(|| {
            TransactionOperationNumber::from_index(0)
                .assured("the first test operation is addressable")
        }),
        message: "test planning input is incomplete".to_string(),
    }])
    .assured("the incomplete test report supplies a diagnostic");
    TransactionReportArchive::new(
        transaction_id.to_string(),
        test_report_with_completeness(domain, operation_count, completeness),
    )
    .assured("the incomplete test report topology can be archived")
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        ActualExecutionStepImpact, DomainName, ExecutionStepOutcome, ImpactEffects,
        PauseRequirement,
    };

    use super::*;

    fn domain(name: &str) -> DomainName {
        DomainName::parse(name).assured("the test domain is an identifier-shaped literal")
    }

    #[test]
    fn reports_round_trip_update_steps_and_deduplicate_topology() {
        let domain = domain("tenant");
        let expected = test_report(&domain, 2);
        let archive = TransactionReportArchive::new("tx".to_string(), expected.clone())
            .assured("the test report topology can be archived");
        let identity = archive.identity().clone();
        let mut records = TransactionReportRecords::default();

        records
            .insert(archive)
            .assured("a valid report revision can be retained");

        assert_eq!(
            records
                .report(&identity)
                .assured("the retained report can be assembled"),
            expected
        );
        assert_eq!(records.headers.len(), 1);
        assert_eq!(records.operations.len(), 2);
        assert_eq!(records.execution_steps.len(), 2);
        assert_eq!(records.topology_headers.len(), 3);

        let mut applied = records
            .report(&identity)
            .assured("the retained report can be assembled")
            .execution_steps()[0]
            .clone();
        *applied.actual_mut() = ActualExecutionStepImpact {
            outcome: ExecutionStepOutcome::Applied,
            quiescence: Vec::new(),
            effects: ImpactEffects::default(),
        };
        records
            .replace_execution_step(&identity, applied)
            .assured("the matching frozen step can record its actual outcome");
        assert!(matches!(
            records
                .report(&identity)
                .assured("the updated report can be assembled")
                .execution_steps()[0]
                .actual()
                .outcome,
            ExecutionStepOutcome::Applied
        ));
        assert_eq!(records.topology_headers.len(), 3);
    }

    #[test]
    fn commit_plan_must_match_the_complete_planned_step() {
        let domain = domain("tenant");
        let archive = test_report_archive("tx", &domain, 1);
        let plan = crate::transaction::test_commit_plan("tx", 1);
        assert!(archive.matches_commit_plan(&plan));

        let mut changed = plan;
        let impact = &changed.steps[0].impact;
        let mut planned = impact.planned().clone();
        planned.pause = PauseRequirement::Domain { domain };
        changed.steps[0].impact =
            ExecutionStepImpactReport::new(impact.operations(), planned, impact.actual().clone());
        assert!(!archive.matches_commit_plan(&changed));
    }

    #[test]
    fn revisions_are_retained_exactly_until_transaction_cleanup() {
        let domain = domain("tenant");
        let first = test_report_archive("tx", &domain, 1);
        let current = test_report_archive("tx", &domain, 2);
        let first_identity = first.identity().clone();
        let current_identity = current.identity().clone();
        let mut records = TransactionReportRecords::default();

        records
            .insert(first)
            .assured("the first report revision can be retained");
        records
            .insert(current)
            .assured("the current report revision can be retained");
        assert_eq!(records.headers.len(), 2);

        records.retain_revision(&current_identity);
        let Err(error) = records.report(&first_identity) else {
            panic!("retaining the current revision removes the prior revision");
        };
        assert_eq!(
            error.current_context(),
            &TransactionReportReadError::UnknownRevision
        );
        assert!(records.report(&current_identity).is_ok());
        assert_eq!(records.headers.len(), 1);
        assert_eq!(records.topology_headers.len(), 3);

        records.remove_transaction("tx");
        let Err(error) = records.report(&current_identity) else {
            panic!("removing the transaction removes its retained report");
        };
        assert_eq!(
            error.current_context(),
            &TransactionReportReadError::UnknownRevision
        );
        assert_eq!(records.headers.len(), 0);
        assert_eq!(records.operations.len(), 0);
        assert_eq!(records.execution_steps.len(), 0);
        assert_eq!(records.topology_headers.len(), 0);
    }

    #[test]
    fn conflicting_revision_does_not_change_retained_records() {
        let tenant = domain("tenant");
        let archive = test_report_archive("tx", &tenant, 1);
        let mut conflicting = archive.clone();
        conflicting.header.domain = domain("other");
        let mut records = TransactionReportRecords::default();
        records
            .insert(archive)
            .assured("the first report revision can be retained");
        let preceding = records.clone();

        let Err(error) = records.insert(conflicting) else {
            panic!("a conflicting report revision must be rejected");
        };
        assert_eq!(
            error.current_context(),
            &TransactionReportStoreError::ConflictingArchive
        );
        assert_eq!(records, preceding);
    }

    #[test]
    fn archives_reject_topology_content_under_another_identity() {
        let domain = domain("tenant");
        let mut archive = test_report_archive("tx", &domain, 1);
        let graph = archive
            .graphs
            .first_mut()
            .assured("every report archives its empty topology");
        graph.id = ImpactTopologyId([9; 32]);

        let Err(error) = TransactionReportRecords::default().insert(archive) else {
            panic!("a topology stored under another content identity must be rejected");
        };
        assert_eq!(
            error.current_context(),
            &TransactionReportStoreError::InvalidArchive
        );
    }
}
