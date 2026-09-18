use error_stack::Report;
use nervix_models::Timestamp;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use super::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement,
    WindowAccumulatorPlan, WindowProcessorError, WindowProcessorState,
    published_generation::{Generation, PublishedGenerations},
};

/// One row a published window retains.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct WindowEntrySnapshot {
    pub(super) sequence: u64,
    pub(super) timestamp: Timestamp,
    pub(super) key: Option<Vec<nervix_models::RemoteRuntimeField>>,
    pub(super) record: nervix_models::RemoteRuntimeRecord,
    /// Every aggregate argument the row was admitted with, in demand and argument order.
    pub(super) arguments: Vec<Option<nervix_models::RemoteRuntimeValue>>,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct LinearHistogramDelayedRemovalSnapshot {
    pub(super) expires_at: Timestamp,
    pub(super) bucket: usize,
}

/// What one aggregate structure publishes beyond the rows its window retains.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) enum WindowAccumulatorSnapshot {
    /// The structure is rebuilt entirely from the retained rows.
    Retained,
    /// A linear histogram also keeps stepped rows counted until their delay expires.
    LinearHistogram {
        delayed_removals: Vec<LinearHistogramDelayedRemovalSnapshot>,
    },
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct WindowProcessorStateSnapshot {
    pub(super) entries: Vec<WindowEntrySnapshot>,
    pub(super) next_sequence: u64,
    pub(super) accumulators: Vec<WindowAccumulatorSnapshot>,
}

/// What one window processor branch keeps beyond the branch task that processes it.
///
/// The live window belongs to that task, which changes it without a lock and publishes it here.
/// Everything outside the task reads the window it last published: the snapshot task persists it,
/// replicas and ownership handoff receive it, and the next task for the same branch restores from
/// it.
#[derive(Debug)]
pub(super) struct ReplicatedWindowProcessorState {
    pub(super) placement: RuntimeStatePlacement,
    /// Absent until the branch task first publishes, which restores as an empty window.
    pub(super) generations: PublishedGenerations<Option<WindowProcessorStateSnapshot>>,
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
        let generations = match initial {
            Some(initial) => {
                let snapshot = decode_window_processor_snapshot(&initial.payload)?;
                PublishedGenerations::restored(initial.lsm, Some(snapshot))
            }
            None => PublishedGenerations::restored(0, None),
        };
        Ok(Self {
            placement,
            generations,
        })
    }

    /// Build the live window a branch task owns from the window published last.
    pub(super) fn restore_state(
        &self,
        plan: &WindowAccumulatorPlan,
        input_schema: &crate::runtime_schema::CompiledSchema,
    ) -> error_stack::Result<WindowProcessorState, WindowProcessorError> {
        let published = self.generations.load();
        let Some(snapshot) = &published.value else {
            return Ok(WindowProcessorState::new(plan));
        };
        WindowProcessorState::from_snapshot(plan, input_schema, snapshot)
    }

    /// Publish the owning branch task's live window as the window everything else reads.
    pub(super) fn replace_state(
        &self,
        state: &WindowProcessorState,
    ) -> Result<(), RuntimePersistenceError> {
        let snapshot = state
            .to_snapshot()
            .map_err(|error| RuntimePersistenceError::EncodeState(format!("{error:#}")))?;
        self.generations.publish(Some(snapshot));
        Ok(())
    }

    /// Encode the window published last, stamped with the revision it stands at.
    pub(super) fn latest_snapshot(
        &self,
    ) -> Result<PersistedRuntimeStateEntry, Report<RuntimePersistenceError>> {
        let published = self.generations.load();
        self.snapshot_of(&published)
    }

    /// Encode the window published last when its revision is after `after_lsm`. A requester that
    /// already holds that revision costs no encode.
    pub(super) fn snapshot_after(
        &self,
        after_lsm: Option<u64>,
    ) -> Result<Option<PersistedRuntimeStateEntry>, Report<RuntimePersistenceError>> {
        let Some(published) = self.generations.load_after(after_lsm) else {
            return Ok(None);
        };
        Ok(Some(self.snapshot_of(&published)?))
    }

    fn snapshot_of(
        &self,
        published: &Generation<Option<WindowProcessorStateSnapshot>>,
    ) -> Result<PersistedRuntimeStateEntry, Report<RuntimePersistenceError>> {
        let payload = match &published.value {
            Some(snapshot) => encode_window_processor_snapshot(snapshot)?,
            None => encode_window_processor_snapshot(&WindowProcessorStateSnapshot {
                entries: Vec::new(),
                next_sequence: 0,
                accumulators: Vec::new(),
            })?,
        };
        Ok(PersistedRuntimeStateEntry {
            lsm: published.revision,
            payload,
        })
    }
}
