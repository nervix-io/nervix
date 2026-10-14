//! Sequential reference state for remote record and admission correlations.
//!
//! Layer: test harness.
//! - **Owns.** Predicted identities, row progress, terminal outcomes and FIFO capacity reuse.
//! - **Depends on.** Current correlation limits and acknowledgement outcome vocabulary.
//! - **Must not know.** Registry slots, locks, queues, executors or observed production results.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use meticulous::ResultExt as _;
use nervix_models::ClusterNodeName;

use super::{ADMISSION_SWEEPS, REMOTE_ACK_SILENT_SWEEPS, ROW_POSITIONS};
use crate::runtime_ack::AckOutcome;

#[derive(Clone, Copy)]
enum Kind {
    Delivery,
    Admission,
}

impl Kind {
    fn index(self) -> usize {
        match self {
            Self::Delivery => 0,
            Self::Admission => 1,
        }
    }
}

pub(super) struct RecordExpectation {
    pub outcome: Option<AckOutcome>,
    pub sequence: u64,
    pub parked: bool,
}

impl RecordExpectation {
    fn report(&mut self) {
        // A case has at most 64 operations, each changing this counter at most twice.
        self.sequence += 1;
    }

    fn park(&mut self, parked: bool) {
        if self.parked != parked {
            self.parked = parked;
            self.report();
        }
    }

    fn finish(&mut self, outcome: Option<AckOutcome>) {
        assert!(self.outcome.is_none(), "a record has one terminal outcome");
        match outcome {
            Some(outcome) => self.outcome = Some(outcome),
            None => {
                // Clearing drops the park guard before the last ACK handle disappears.
                self.park(false);
                self.outcome = Some(AckOutcome::NoAck(
                    "ack completion sender dropped".to_string(),
                ));
            }
        }
    }
}

enum RowPhase {
    AwaitingAdmission(u64),
    Admitted(u64),
}

impl RowPhase {
    fn sweep(&mut self) -> bool {
        let (sweeps, bound) = match self {
            Self::AwaitingAdmission(sweeps) => (sweeps, ADMISSION_SWEEPS),
            Self::Admitted(sweeps) => (sweeps, REMOTE_ACK_SILENT_SWEEPS),
        };
        // A row expires at its finite bound before the next operation can sweep it again.
        *sweeps += 1;
        *sweeps > bound
    }

    fn reason(&self, receiver: &ClusterNodeName) -> String {
        match self {
            Self::AwaitingAdmission(_) => format!(
                "node '{}' did not admit the forwarded record within {}",
                receiver,
                humantime::format_duration(super::REMOTE_RELAY_TOTAL_TIMEOUT),
            ),
            Self::Admitted(_) => format!(
                "node '{}' reported nothing about the forwarded record for {}",
                receiver,
                humantime::format_duration(super::REMOTE_ACK_SILENCE_TIMEOUT),
            ),
        }
    }
}

struct Row {
    record: usize,
    phase: RowPhase,
    progress_sequence: Option<u64>,
}

struct Delivery {
    receiver: ClusterNodeName,
    rows: Vec<Option<Row>>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum AdmissionUpdate {
    Pending,
    Alive,
    Admitted,
    Rejected(String),
}

pub(super) struct AdmissionExpectation {
    pub held: bool,
    pub update: Option<AdmissionUpdate>,
}

pub(super) struct CorrelationModel {
    pub capacity: usize,
    pub closed: bool,
    pub claimed: [usize; 2],
    pub free: [VecDeque<usize>; 2],
    pub ever_claimed: BTreeSet<usize>,
    pub record_ids: BTreeSet<u64>,
    pub records: Vec<RecordExpectation>,
    pub admissions: BTreeMap<u64, AdmissionExpectation>,
    generations: Vec<u64>,
    deliveries: BTreeMap<u64, Delivery>,
}

impl CorrelationModel {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            closed: false,
            claimed: [0, 0],
            free: [VecDeque::new(), VecDeque::new()],
            ever_claimed: BTreeSet::new(),
            record_ids: BTreeSet::new(),
            records: Vec::new(),
            admissions: BTreeMap::new(),
            generations: vec![0; capacity * 2],
            deliveries: BTreeMap::new(),
        }
    }

    fn claim(&mut self, kind: Kind) -> Option<u64> {
        let side = kind.index();
        // Closing seals positions but permits draining retained free entries. A registration
        // consumes its returned position before discovering that the position is sealed.
        let returned = self.free[side].pop_front();
        if self.closed {
            return None;
        }
        let position = if let Some(position) = returned {
            position
        } else {
            if self.claimed[side] == self.capacity {
                return None;
            }
            let position = side * self.capacity + self.claimed[side];
            self.claimed[side] += 1;
            assert!(self.ever_claimed.insert(position));
            position
        };
        // Four positions per side and 64 operations bound every identity well below u64::MAX.
        self.generations[position] += 1;
        let route = self.generations[position]
            * u64::try_from(self.capacity * 2).assured("the model has at most eight positions")
            + u64::try_from(position).assured("the model has at most eight positions");
        Some(route * ROW_POSITIONS)
    }

    fn position(&self, id: u64) -> usize {
        let total =
            u64::try_from(self.capacity * 2).assured("the model has at most eight positions");
        usize::try_from((id / ROW_POSITIONS) % total)
            .assured("a position below eight fits in usize")
    }

    fn recycle(&mut self, id: u64, kind: Kind) {
        if !self.closed {
            let position = self.position(id);
            self.free[kind.index()].push_back(position);
        }
    }

    pub fn root(&mut self) -> usize {
        let index = self.records.len();
        self.records.push(RecordExpectation {
            outcome: None,
            sequence: 0,
            parked: false,
        });
        index
    }

    pub fn refuse(&mut self, rows: &[Option<usize>]) {
        for record in rows.iter().flatten() {
            self.records[*record].finish(None);
        }
    }

    pub fn register_rows(
        &mut self,
        receiver: ClusterNodeName,
        rows: &[Option<usize>],
    ) -> Option<Vec<Option<u64>>> {
        if rows.iter().all(Option::is_none) {
            return Some(vec![None; rows.len()]);
        }
        let Some(base) = self.claim(Kind::Delivery) else {
            self.refuse(rows);
            return None;
        };
        let mut registrations = Vec::new();
        let mut pending = Vec::new();
        for (row, record) in rows.iter().enumerate() {
            if let Some(record) = record {
                let id = base + u64::try_from(row).assured("at most four rows are generated");
                assert!(self.record_ids.insert(id));
                registrations.push(Some(id));
                pending.push(Some(Row {
                    record: *record,
                    phase: RowPhase::AwaitingAdmission(0),
                    progress_sequence: None,
                }));
            } else {
                registrations.push(None);
                pending.push(None);
            }
        }
        assert!(
            self.deliveries
                .insert(
                    base,
                    Delivery {
                        receiver,
                        rows: pending
                    }
                )
                .is_none()
        );
        Some(registrations)
    }

    pub fn register_admission(&mut self) -> Option<u64> {
        let id = self.claim(Kind::Admission)?;
        assert!(
            self.admissions
                .insert(
                    id,
                    AdmissionExpectation {
                        held: true,
                        update: Some(AdmissionUpdate::Pending),
                    }
                )
                .is_none()
        );
        Some(id)
    }

    pub fn close_admission(&mut self, id: u64) {
        if let Some(admission) = self.admissions.get_mut(&id) {
            admission.update = None;
        }
    }

    fn row_mut(&mut self, id: u64) -> Option<&mut Row> {
        let base = id - id % ROW_POSITIONS;
        let row =
            usize::try_from(id % ROW_POSITIONS).assured("the row address space fits in usize");
        self.deliveries.get_mut(&base)?.rows.get_mut(row)?.as_mut()
    }

    pub fn holds(&self, id: u64) -> bool {
        if let Some(admission) = self.admissions.get(&id) {
            return admission.held;
        }
        let base = id - id % ROW_POSITIONS;
        let row =
            usize::try_from(id % ROW_POSITIONS).assured("the row address space fits in usize");
        if let Some(delivery) = self.deliveries.get(&base)
            && let Some(record) = delivery.rows.get(row)
        {
            return record.is_some();
        }
        false
    }

    pub fn admit(&mut self, id: u64) {
        if let Some(row) = self.row_mut(id)
            && let RowPhase::AwaitingAdmission(_) = row.phase
        {
            row.phase = RowPhase::Admitted(0);
        }
    }

    pub fn progress(&mut self, id: u64, progress: Option<(u64, bool)>) -> bool {
        if let Some(row) = self.row_mut(id) {
            let record = row.record;
            let mut parking = None;
            if let Some((sequence, parked)) = progress {
                let accepted = match row.progress_sequence {
                    Some(seen) => sequence > seen,
                    None => true,
                };
                if accepted {
                    row.progress_sequence = Some(sequence);
                    parking = Some(parked);
                }
            }
            if let RowPhase::Admitted(sweeps) = &mut row.phase {
                *sweeps = 0;
            }
            if let Some(parked) = parking {
                self.records[record].park(parked);
            }
            self.records[record].report();
            return true;
        }
        if let Some(admission) = self.admissions.get_mut(&id)
            && admission.held
        {
            if admission.update.is_some() {
                admission.update = Some(AdmissionUpdate::Alive);
            } else {
                admission.held = false;
                self.recycle(id, Kind::Admission);
            }
            return true;
        }
        false
    }

    pub fn finish(&mut self, id: u64, outcome: Option<AckOutcome>) -> bool {
        let base = id - id % ROW_POSITIONS;
        let row =
            usize::try_from(id % ROW_POSITIONS).assured("the row address space fits in usize");
        if let Some(delivery) = self.deliveries.get_mut(&base)
            && let Some(pending) = delivery.rows.get_mut(row)
            && let Some(pending) = pending.take()
        {
            self.records[pending.record].finish(outcome);
            if delivery.rows.iter().all(Option::is_none) {
                drop(self.deliveries.remove(&base));
                self.recycle(base, Kind::Delivery);
            }
            return true;
        }
        if let Some(admission) = self.admissions.get_mut(&id)
            && admission.held
        {
            admission.held = false;
            if admission.update.is_some()
                && let Some(outcome) = outcome
            {
                admission.update = Some(match outcome {
                    AckOutcome::Ack => AdmissionUpdate::Admitted,
                    AckOutcome::NoAck(reason) => AdmissionUpdate::Rejected(reason),
                });
            }
            self.recycle(id, Kind::Admission);
            return true;
        }
        false
    }

    pub fn sweep(&mut self) -> BTreeMap<ClusterNodeName, usize> {
        let mut expired = Vec::new();
        let mut counts = BTreeMap::new();
        for (base, delivery) in &mut self.deliveries {
            for (index, row) in delivery.rows.iter_mut().enumerate() {
                let Some(row) = row else { continue };
                if row.phase.sweep() {
                    let id = base + u64::try_from(index).assured("at most four rows are generated");
                    expired.push((id, row.phase.reason(&delivery.receiver)));
                    *counts.entry(delivery.receiver.clone()).or_insert(0) += 1;
                }
            }
        }
        // Sweep reclamation follows claimed-position order even when live generations differ.
        expired.sort_by_key(|(id, _)| self.position(*id));
        for (id, reason) in expired {
            assert!(self.finish(id, Some(AckOutcome::NoAck(reason))));
        }
        let mut closed = Vec::new();
        for (id, admission) in &self.admissions {
            if admission.held && admission.update.is_none() {
                closed.push(*id);
            }
        }
        closed.sort_by_key(|id| self.position(*id));
        for id in closed {
            assert!(self.finish(id, None));
        }
        counts
    }

    pub fn memory(&self, bytes_per_row: usize) -> u64 {
        let rows: usize = self
            .deliveries
            .values()
            .map(|delivery| delivery.rows.len())
            .sum();
        u64::try_from(rows * bytes_per_row).assured("four deliveries of four rows fit in u64")
    }

    pub fn live_records(&self) -> Vec<u64> {
        let mut ids = Vec::new();
        for id in &self.record_ids {
            if self.holds(*id) {
                ids.push(*id);
            }
        }
        ids
    }

    pub fn select(&self, selection: u8, parameter: u8) -> u64 {
        // This is a bounded choice sequence, not an identity lookup: at most 64 * 4 records.
        let mut ids = Vec::new();
        match selection % 8 {
            0 | 1 | 4 => {
                for id in &self.record_ids {
                    let choose = match selection % 8 {
                        0 => self.holds(*id),
                        1 => !self.holds(*id),
                        _ => true,
                    };
                    if choose {
                        ids.push(*id);
                    }
                }
            }
            2 | 3 | 5 => {
                for (id, admission) in &self.admissions {
                    let choose = match selection % 8 {
                        2 => admission.held,
                        3 => !admission.held,
                        _ => true,
                    };
                    if choose {
                        ids.push(*id);
                    }
                }
            }
            6 => {
                ids.extend(&self.record_ids);
                ids.extend(self.admissions.keys());
            }
            _ => return u64::MAX - u64::from(parameter),
        }
        if ids.is_empty() {
            return 0;
        }
        let id = ids[usize::from(selection / 8) % ids.len()];
        match selection % 8 {
            4 => id - id % ROW_POSITIONS + u64::from(parameter % 8),
            5 => id + 1 + u64::from(parameter % 4),
            _ => id,
        }
    }

    pub fn shutdown(&mut self) {
        self.closed = true;
        let mut ids = self.live_records();
        for (id, admission) in &self.admissions {
            if admission.held {
                ids.push(*id);
            }
        }
        for id in ids {
            assert!(self.finish(
                id,
                Some(AckOutcome::NoAck(
                    "remote correlation owner ended".to_string(),
                ))
            ));
        }
    }
}
