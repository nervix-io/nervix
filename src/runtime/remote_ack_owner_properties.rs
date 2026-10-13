//! Operation-sequence oracle for the production remote correlation registry.
//!
//! Layer: test harness.
//! - **Owns.** Bounded generated operations and complete observations after every transition.
//! - **Depends on.** The sequential model, production registry and tracked acknowledgement roots.
//! - **Must not know.** Registry lock state, graph Models, networking or persistent storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::{BTreeMap, BTreeSet};

use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::MemoryClass;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, RemoteAckRegistration,
};
use nervix_primitives::sync::{Arc, atomic::Ordering, watch};

use super::{
    ADMISSION_SWEEPS, CLAIMS_CLOSED, PendingRemoteAck, REMOTE_ACK_SILENT_SWEEPS,
    RelayAdmissionUpdate, RemoteDispatchError, RemoteDispatchRegistry,
    model::{AdmissionUpdate, CorrelationModel, RecordExpectation},
};
use crate::runtime_ack::{AckCompletion, AckOutcome, AckProgress, AckRootTracker, AckSet};

pub(super) const CASE_BYTES: usize = 256;
const STEP_BYTES: usize = 4;

struct RecordWitness {
    tracker: Arc<AckRootTracker>,
    completion: Option<AckCompletion>,
    terminal: Option<AckOutcome>,
}

impl RecordWitness {
    fn assert_matches(&mut self, expected: &RecordExpectation) {
        let pending = usize::from(expected.outcome.is_none());
        assert_eq!(self.tracker.outstanding(), pending);
        assert_eq!(
            self.tracker.outstanding_for_ownership_handoff(),
            usize::from(expected.outcome.is_none() && !expected.parked),
        );
        if let Some(completion) = &mut self.completion {
            assert_eq!(
                completion.remote_progress(),
                (expected.sequence, expected.parked)
            );
            if let Some(outcome) = &expected.outcome {
                assert_eq!(
                    completion.wait_for_progress().now_or_never(),
                    Some(AckProgress::Complete(outcome.clone())),
                );
                self.terminal = Some(outcome.clone());
                self.completion = None;
            } else {
                let progress = completion.wait_for_progress().now_or_never();
                assert!(matches!(progress, None | Some(AckProgress::Alive)));
            }
        }
        assert_eq!(self.terminal, expected.outcome);
    }
}

pub(super) struct CorrelationCase {
    owners: RemoteDispatchRegistry,
    model: CorrelationModel,
    records: Vec<RecordWitness>,
    admissions: BTreeMap<u64, watch::Receiver<RelayAdmissionUpdate>>,
    probes: BTreeSet<u64>,
    bytes_per_row: usize,
    memory_capacity: u64,
}

impl CorrelationCase {
    pub fn run(bytes: &[u8]) {
        // Even a direct corpus replay cannot exceed the registered case budget.
        let bytes = &bytes[..bytes.len().min(CASE_BYTES)];
        let capacity = match bytes.first() {
            Some(byte) => 1 + usize::from(byte % 4),
            None => 1,
        };
        let owners = RemoteDispatchRegistry::with_capacity(capacity);
        let snapshot = owners.executor.snapshot().relay_memory;
        assert_eq!(snapshot.reserved_bytes, 0);
        let mut case = Self {
            owners,
            model: CorrelationModel::new(capacity),
            records: Vec::new(),
            admissions: BTreeMap::new(),
            probes: BTreeSet::from([0, u64::MAX]),
            // The three generated receiver names all have this length. Detached rows inside an
            // attached batch are charged too, until the last attached row ends the delivery.
            bytes_per_row: std::mem::size_of::<Option<PendingRemoteAck>>() + "node-1".len(),
            memory_capacity: snapshot.capacity_bytes,
        };
        case.assert_matches();
        for chunk in bytes.get(1..).unwrap_or_default().chunks(STEP_BYTES) {
            let mut step = [0; STEP_BYTES];
            step[..chunk.len()].copy_from_slice(chunk);
            case.step(step);
            case.assert_matches();
        }
        case.drain();
    }

    fn node(parameter: u8) -> ClusterNodeName {
        let name = match parameter % 3 {
            0 => "node-1",
            1 => "node-2",
            _ => "node-3",
        };
        ClusterNodeName::parse(name).assured("all generated receiver names are valid")
    }

    fn rows(&mut self, count: usize, mask: u8) -> PreparedRows {
        let mut acks = Vec::new();
        let mut records = Vec::new();
        for row in 0..count {
            if (mask >> row) & 1 == 0 {
                acks.push(AckSet::empty());
                records.push(None);
            } else {
                let tracker = Arc::new(AckRootTracker::default());
                let (root, completion) = AckSet::tracked_root(tracker.clone());
                let index = self.model.root();
                assert_eq!(index, self.records.len());
                self.records.push(RecordWitness {
                    tracker,
                    completion: Some(completion),
                    terminal: None,
                });
                acks.push(root);
                records.push(Some(index));
            }
        }
        PreparedRows { acks, records }
    }

    fn register_rows(&mut self, receiver: ClusterNodeName, count: usize, mask: u8, single: bool) {
        let mut rows = self.rows(count, mask);
        let expected = self.model.register_rows(receiver.clone(), &rows.records);
        let actual = if single {
            let root = rows
                .acks
                .pop()
                .assured("single registration prepared exactly one row");
            self.owners
                .register_ack(receiver, root)
                .map(|id| vec![Some(id)])
        } else {
            self.owners.register_acks(receiver, rows.acks)
        };
        match (expected, actual) {
            (Some(expected), Ok(actual)) => {
                assert_eq!(actual, expected);
                self.probes.extend(actual.into_iter().flatten());
            }
            (None, Err(error)) => assert_eq!(
                *error.current_context(),
                RemoteDispatchError::CorrelationCapacity {
                    capacity: self.model.capacity
                },
            ),
            (expected, actual) => {
                panic!("registration differs: expected={expected:?}, actual={actual:?}")
            }
        }
    }

    fn register_admission(&mut self) {
        let expected = self.model.register_admission();
        match (expected, self.owners.register_admission()) {
            (Some(expected), Ok((actual, updates))) => {
                assert_eq!(actual, expected);
                assert!(self.admissions.insert(actual, updates).is_none());
                self.probes.insert(actual);
            }
            (None, Err(error)) => assert_eq!(
                *error.current_context(),
                RemoteDispatchError::CorrelationCapacity {
                    capacity: self.model.capacity
                },
            ),
            (expected, actual) => {
                panic!("admission differs: expected={expected:?}, actual={actual:?}")
            }
        }
    }

    fn close_admission(&mut self, id: u64) {
        self.model.close_admission(id);
        drop(self.admissions.remove(&id));
    }

    fn registration(id: u64) -> RemoteAckRegistration {
        RemoteAckRegistration {
            ack_id: id,
            registrar: ClusterNodeIdentity::new(Self::node(0), ClusterNodeIncarnation::new(1)),
        }
    }

    fn admit_batch(&mut self, selection: u8, parameter: u8) {
        let first = self.model.select(selection, parameter);
        let second = self.model.select(selection ^ 8, parameter);
        // Keep both detached entries and non-record registrations in the same public bulk path.
        let foreign = self.model.select(5, parameter);
        let ids = [Some(first), None, Some(second), Some(foreign)];
        let mut registrations = Vec::new();
        for id in ids {
            if let Some(id) = id {
                self.model.admit(id);
                self.probes.insert(id);
                registrations.push(Some(Self::registration(id)));
            } else {
                registrations.push(None);
            }
        }
        self.owners.admit_acks(&registrations);
    }

    fn memory_refusal(&mut self, parameter: u8, mask: u8) {
        let reserved = self.model.memory(self.bytes_per_row);
        let available = self
            .memory_capacity
            .checked_sub(reserved)
            .assured("bounded deliveries fit in the relay budget");
        let reservation = self
            .owners
            .executor
            .try_reserve(MemoryClass::Relay, available)
            .assured("this case is the executor's sole relay-memory owner");
        let rows = self.rows(1 + usize::from(parameter % 4), mask | 1);
        self.model.refuse(&rows.records);
        let error = self
            .owners
            .register_acks(Self::node(parameter), rows.acks)
            .expect_err("record storage cannot fit while the full relay budget is held");
        assert_eq!(
            *error.current_context(),
            RemoteDispatchError::CorrelationMemory
        );
        assert_eq!(
            self.owners.executor.snapshot().relay_memory.reserved_bytes,
            self.memory_capacity
        );
        drop(reservation);
    }

    fn sweep(&mut self, parameter: u8) {
        let count = match parameter % 6 {
            0 => 1,
            1 => REMOTE_ACK_SILENT_SWEEPS,
            2 => REMOTE_ACK_SILENT_SWEEPS + 1,
            3 => ADMISSION_SWEEPS,
            4 => ADMISSION_SWEEPS + 1,
            _ => 2,
        };
        for _ in 0..count {
            let expected = self.model.sweep();
            assert_eq!(self.owners.fail_silent_acks(), expected);
            self.assert_matches();
        }
    }

    fn resolve(&mut self, id: u64, outcome: AckOutcome) {
        let expected = self.model.finish(id, Some(outcome.clone()));
        assert_eq!(self.owners.resolve_ack(id, outcome), expected);
    }

    fn shutdown(&mut self) {
        self.model.shutdown();
        self.owners.shutdown();
    }

    fn step(&mut self, [operation, selection, parameter, mask]: [u8; STEP_BYTES]) {
        let id = self.model.select(selection, parameter);
        self.probes.insert(id);
        match operation % 16 {
            0 => self.register_rows(Self::node(selection), 1, 1, true),
            1 => self.register_rows(
                Self::node(selection),
                1 + usize::from(parameter % 4),
                mask,
                false,
            ),
            2 => self.register_rows(Self::node(selection), usize::from(parameter % 5), 0, false),
            3 => self.register_admission(),
            4 => self.close_admission(id),
            5 => {
                self.model.admit(id);
                self.owners.admit_ack(id);
            }
            6 => self.admit_batch(selection, parameter),
            7 => {
                let expected = self.model.progress(id, None);
                assert_eq!(self.owners.report_ack(id), expected);
            }
            8 => {
                let sequence = if parameter == u8::MAX {
                    u64::MAX
                } else {
                    u64::from(parameter)
                };
                let parked = mask & 1 != 0;
                let expected = self.model.progress(id, Some((sequence, parked)));
                assert_eq!(self.owners.progress_ack(id, sequence, parked), expected);
            }
            9 => self.resolve(id, AckOutcome::Ack),
            10 => self.resolve(
                id,
                AckOutcome::NoAck(format!("receiver refusal {parameter}")),
            ),
            11 => {
                self.model.finish(id, None);
                self.owners.clear_ack(id);
            }
            12 => self.sweep(parameter),
            13 => assert_eq!(self.owners.holds_ack(id), self.model.holds(id)),
            14 => self.memory_refusal(parameter, mask),
            _ => self.shutdown(),
        }
    }

    fn assert_matches(&mut self) {
        let expected_positions: Vec<_> = self.model.ever_claimed.iter().copied().collect();
        assert_eq!(
            self.owners.swept_positions().collect::<Vec<_>>(),
            expected_positions
        );
        let sealed = if self.model.closed { CLAIMS_CLOSED } else { 0 };
        assert_eq!(
            self.owners.next_delivery.load(Ordering::Relaxed),
            self.model.claimed[0] | sealed
        );
        assert_eq!(
            self.owners.next_admission.load(Ordering::Relaxed),
            self.model.claimed[1] | sealed
        );
        assert_eq!(self.owners.deliveries.len(), self.model.free[0].len());
        assert_eq!(self.owners.admissions.len(), self.model.free[1].len());
        assert_eq!(self.owners.deliveries.is_closed(), self.model.closed);
        assert_eq!(self.owners.admissions.is_closed(), self.model.closed);
        let memory = self.owners.executor.snapshot().relay_memory;
        assert_eq!(memory.reserved_bytes, self.model.memory(self.bytes_per_row));
        assert_eq!(memory.capacity_bytes, self.memory_capacity);
        assert_eq!(self.records.len(), self.model.records.len());
        for (actual, expected) in self.records.iter_mut().zip(&self.model.records) {
            actual.assert_matches(expected);
        }
        for (id, expected) in &self.model.admissions {
            if let Some(update) = &expected.update {
                let actual = self
                    .admissions
                    .get(id)
                    .assured("an open admission retains its receiver");
                let value = match &*actual.borrow() {
                    RelayAdmissionUpdate::Pending => AdmissionUpdate::Pending,
                    RelayAdmissionUpdate::Alive => AdmissionUpdate::Alive,
                    RelayAdmissionUpdate::Admitted => AdmissionUpdate::Admitted,
                    RelayAdmissionUpdate::Rejected(reason) => {
                        AdmissionUpdate::Rejected(reason.clone())
                    }
                };
                assert_eq!(&value, update);
                assert_eq!(actual.has_changed().is_err(), !expected.held);
            } else {
                assert!(!self.admissions.contains_key(id));
            }
        }
        self.probes.extend(&self.model.record_ids);
        self.probes.extend(self.model.admissions.keys());
        for id in &self.probes {
            let held = self.model.holds(*id);
            assert_eq!(self.owners.holds_ack(*id), held, "id={id}");
            if !held {
                assert!(!self.owners.report_ack(*id));
                assert!(!self.owners.progress_ack(*id, u64::MAX, true));
                assert!(!self.owners.resolve_ack(*id, AckOutcome::Ack));
                assert!(
                    !self
                        .owners
                        .resolve_ack(*id, AckOutcome::NoAck("late refusal".to_string()))
                );
                self.owners.admit_ack(*id);
                self.owners.clear_ack(*id);
            }
        }
    }

    fn drain(&mut self) {
        // Before sealing the registry, prove both pools have all their usable capacity back.
        for id in self.model.live_records() {
            self.resolve(id, AckOutcome::Ack);
            self.assert_matches();
        }
        let admissions: Vec<_> = self.model.admissions.keys().copied().collect();
        for id in admissions {
            self.close_admission(id);
        }
        assert!(self.model.sweep().is_empty());
        assert!(self.owners.fail_silent_acks().is_empty());
        self.assert_matches();
        if !self.model.closed {
            for side in 0..2 {
                assert_eq!(
                    self.model.free[side].len() + self.model.capacity - self.model.claimed[side],
                    self.model.capacity
                );
            }
        }
        assert_eq!(
            self.owners.executor.snapshot().relay_memory.reserved_bytes,
            0
        );
        self.shutdown();
        self.assert_matches();
        self.shutdown();
        self.assert_matches();
    }
}

struct PreparedRows {
    acks: Vec<AckSet>,
    records: Vec<Option<usize>>,
}
