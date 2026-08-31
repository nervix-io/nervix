use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use nervix_models::Timestamp;
use nervix_nspl::window_processor::aggregate::WindowAggregateProgram;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use super::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement,
    WindowProcessorState,
};

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct WindowEntrySnapshot {
    pub(super) sequence: u64,
    pub(super) timestamp: Timestamp,
    pub(super) key: Option<Vec<nervix_models::RemoteRuntimeField>>,
    pub(super) record: nervix_models::RemoteRuntimeRecord,
    pub(super) aggregate_inputs: Vec<Option<nervix_models::RemoteRuntimeValue>>,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct WindowSequenceValueSnapshot {
    pub(super) timestamp: Timestamp,
    pub(super) sequence: u64,
    pub(super) value: nervix_models::RemoteRuntimeValue,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct WindowSortedCountSnapshot {
    pub(super) value: nervix_models::RemoteRuntimeValue,
    pub(super) count: usize,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct LinearHistogramDelayedRemovalSnapshot {
    pub(super) expires_at: Timestamp,
    pub(super) bucket: usize,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) enum WindowAggregateAccumulatorSnapshot {
    Counter {
        count: usize,
    },
    Sequence {
        values: Vec<WindowSequenceValueSnapshot>,
    },
    SortedMap {
        counts: Vec<WindowSortedCountSnapshot>,
    },
    LinearHistogram {
        buckets: Vec<usize>,
        total: usize,
        min: f64,
        max: f64,
        width: f64,
        delay_nanos: u64,
        delayed_removals: Vec<LinearHistogramDelayedRemovalSnapshot>,
    },
    Sum {
        total: Option<nervix_models::RemoteRuntimeValue>,
    },
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct WindowProcessorStateSnapshot {
    pub(super) entries: Vec<WindowEntrySnapshot>,
    pub(super) next_sequence: u64,
    pub(super) accumulators: Vec<WindowAggregateAccumulatorSnapshot>,
}

#[derive(Debug)]
pub(super) struct ReplicatedWindowProcessorState {
    pub(super) placement: RuntimeStatePlacement,
    pub(super) snapshot: parking_lot::Mutex<Option<WindowProcessorStateSnapshot>>,
    pub(super) current_lsm: AtomicU64,
    pub(super) last_persisted_lsm: AtomicU64,
    pub(super) live_dirty: AtomicBool,
    pub(super) dirty: AtomicBool,
}

fn encode_window_processor_snapshot(
    snapshot: &WindowProcessorStateSnapshot,
) -> Result<Vec<u8>, RuntimePersistenceError> {
    rkyv::to_bytes::<rkyv::rancor::Error>(snapshot)
        .map(|bytes| bytes.to_vec())
        .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))
}

fn decode_window_processor_snapshot(
    payload: &[u8],
) -> Result<WindowProcessorStateSnapshot, RuntimePersistenceError> {
    rkyv::from_bytes::<WindowProcessorStateSnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))
}

impl ReplicatedWindowProcessorState {
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Result<Self, RuntimePersistenceError> {
        let mut current_lsm = 0;
        let mut last_persisted_lsm = 0;
        let mut snapshot = None;
        if let Some(initial) = initial {
            current_lsm = initial.lsm;
            last_persisted_lsm = initial.lsm;
            snapshot = Some(decode_window_processor_snapshot(&initial.payload)?);
        }
        Ok(Self {
            placement,
            snapshot: parking_lot::Mutex::new(snapshot),
            current_lsm: AtomicU64::new(current_lsm),
            last_persisted_lsm: AtomicU64::new(last_persisted_lsm),
            live_dirty: AtomicBool::new(false),
            dirty: AtomicBool::new(false),
        })
    }

    pub(super) fn restore_state(
        &self,
        program: &WindowAggregateProgram,
        input_schema: &crate::runtime_schema::CompiledSchema,
    ) -> Result<WindowProcessorState, String> {
        let Some(snapshot) = self.snapshot.lock().clone() else {
            return Ok(WindowProcessorState::new(program));
        };
        WindowProcessorState::from_snapshot(program, input_schema, snapshot)
    }

    pub(super) fn replace_state(
        &self,
        state: &WindowProcessorState,
    ) -> Result<u64, RuntimePersistenceError> {
        let snapshot = state
            .to_snapshot()
            .map_err(RuntimePersistenceError::EncodeState)?;
        *self.snapshot.lock() = Some(snapshot);
        let lsm = self
            .current_lsm
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.live_dirty.store(false, Ordering::SeqCst);
        self.dirty.store(true, Ordering::SeqCst);
        Ok(lsm)
    }

    pub(super) fn mark_live_dirty(&self) {
        self.live_dirty.store(true, Ordering::SeqCst);
    }

    pub(super) fn latest_snapshot(
        &self,
    ) -> Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        let snapshot =
            self.snapshot
                .lock()
                .clone()
                .unwrap_or_else(|| WindowProcessorStateSnapshot {
                    entries: Vec::new(),
                    next_sequence: 0,
                    accumulators: Vec::new(),
                });
        Ok(PersistedRuntimeStateEntry {
            lsm: self.current_lsm.load(Ordering::SeqCst),
            schema_fingerprint: self.placement.schema_fingerprint,
            payload: encode_window_processor_snapshot(&snapshot)?,
        })
    }
}
