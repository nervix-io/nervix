//! Validated structure of one current window checkpoint, without decoding its columns.
//!
//! Layer: data plane.
//! - **Owns.** Native framing, lifetime and typed-removal validation shared by restore and capture.
//! - **Depends on.** The owning window codec and admitted file views and memory.
//! - **Must not know.** Archive records, graph configuration or placement decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns native window structure validation")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "one stored window is validated for capture or reopening"
    )
)]

use super::*;
use crate::runtime::snapshot_staging::MappedArtifact;

pub(in crate::runtime) struct WindowCheckpointLayout {
    pub(in crate::runtime) header: WindowSnapshotHeader,
    pub(in crate::runtime) input: std::ops::Range<usize>,
    pub(in crate::runtime) arguments: std::ops::Range<usize>,
    pub(in crate::runtime) accumulators: Vec<WindowAccumulatorSnapshot>,
}

pub(in crate::runtime) struct AdmittedWindowLayout {
    pub(in crate::runtime) layout: WindowCheckpointLayout,
    pub(in crate::runtime) _memory: nervix_execution::Reservation,
}

impl WindowCheckpointLayout {
    pub(in crate::runtime) async fn open(
        executor: &Executor,
        view: &MappedArtifact,
    ) -> error_stack::Result<Option<AdmittedWindowLayout>, WindowSnapshotError> {
        // Mapped native bytes, decoded typed-removal metadata, its descriptor projection and
        // bounded header scratch. Admission is capture-owned; ordinary reopening retains its
        // existing lifecycle and materialization owner.
        let native_bytes = u64::try_from(view.length())
            .map_err(|_| Report::new(WindowSnapshotError::Admission))?;
        let metadata_bytes = native_bytes
            .checked_mul(8)
            .ok_or_else(|| Report::new(WindowSnapshotError::Admission))?;
        let working_bytes = metadata_bytes
            .checked_add(4 * 64 * 1024)
            .ok_or_else(|| Report::new(WindowSnapshotError::Admission))?;
        let working = executor
            .try_reserve(MemoryClass::RestoreMetadata, working_bytes)
            .change_context(WindowSnapshotError::Admission)?;
        let inspecting = executor.clone();
        view.inspect(working, move |payload, memory, cancellation| {
            let layout = Self::parse(
                payload,
                &inspecting,
                WindowSnapshotLifetime::Recorded,
                Some(cancellation),
            )?;
            Ok::<_, Report<WindowSnapshotError>>(layout.map(|layout| AdmittedWindowLayout {
                layout,
                _memory: memory,
            }))
        })
        .await
        .change_context(WindowSnapshotError::Execution)?
    }

    pub(super) fn parse(
        payload: &[u8],
        executor: &Executor,
        lifetime: WindowSnapshotLifetime,
        cancellation: Option<&nervix_execution::Cancellation>,
    ) -> error_stack::Result<Option<Self>, WindowSnapshotError> {
        let invalid = |issue| Report::new(WindowSnapshotError::Invalid { issue });
        if payload.len() < WINDOW_SNAPSHOT_FRAME_BYTES
            || payload[..WINDOW_SNAPSHOT_MAGIC.len()] != WINDOW_SNAPSHOT_MAGIC
        {
            return Err(invalid(WindowSnapshotIssue::Header));
        }
        let header_length = u32::from_le_bytes(
            payload[WINDOW_SNAPSHOT_MAGIC.len()..WINDOW_SNAPSHOT_FRAME_BYTES]
                .try_into()
                .map_err(|_| invalid(WindowSnapshotIssue::Header))?,
        );
        let header_length =
            usize::try_from(header_length).map_err(|_| invalid(WindowSnapshotIssue::Length))?;
        if u64::try_from(header_length).unwrap_or(u64::MAX)
            > executor.limits().snapshot_header_bytes.as_u64()
        {
            return Err(invalid(WindowSnapshotIssue::HeaderLimit));
        }
        let header_end = WINDOW_SNAPSHOT_FRAME_BYTES
            .checked_add(header_length)
            .ok_or_else(|| invalid(WindowSnapshotIssue::Length))?;
        let header_bytes = payload
            .get(WINDOW_SNAPSHOT_FRAME_BYTES..header_end)
            .ok_or_else(|| invalid(WindowSnapshotIssue::Header))?;
        let header = decode_aligned_rkyv::<WindowSnapshotHeader>(header_bytes).change_context(
            WindowSnapshotError::Decode {
                section: WindowSnapshotSection::Header,
            },
        )?;
        match lifetime {
            WindowSnapshotLifetime::Branch(expected) => {
                if header.incarnation != Some(expected) {
                    return Ok(None);
                }
            }
            WindowSnapshotLifetime::Recorded => {
                if header.incarnation.is_none() {
                    return Ok(None);
                }
            }
        }
        let input_end = header_end
            .checked_add(
                usize::try_from(header.input_bytes)
                    .map_err(|_| invalid(WindowSnapshotIssue::Length))?,
            )
            .ok_or_else(|| invalid(WindowSnapshotIssue::Length))?;
        let argument_end = input_end
            .checked_add(
                usize::try_from(header.argument_bytes)
                    .map_err(|_| invalid(WindowSnapshotIssue::Length))?,
            )
            .ok_or_else(|| invalid(WindowSnapshotIssue::Length))?;
        if argument_end > payload.len() {
            return Err(invalid(WindowSnapshotIssue::ArrowSection));
        }
        let mut delayed_removals = (0..header.accumulators.len())
            .map(|_| Vec::new())
            .collect::<Vec<Vec<LinearHistogramDelayedRemovalSnapshot>>>();
        let mut offset = argument_end;
        let typed_limit = executor.limits().snapshot_record_bytes.as_u64();
        for _ in 0..header.typed_sections {
            if let Some(cancellation) = cancellation {
                cancellation
                    .check()
                    .change_context(WindowSnapshotError::Execution)?;
            }
            let length_end = offset
                .checked_add(4)
                .ok_or_else(|| invalid(WindowSnapshotIssue::Length))?;
            let length_bytes: [u8; 4] = payload
                .get(offset..length_end)
                .ok_or_else(|| invalid(WindowSnapshotIssue::TypedLength))?
                .try_into()
                .map_err(|_| invalid(WindowSnapshotIssue::TypedLength))?;
            let length = usize::try_from(u32::from_le_bytes(length_bytes))
                .map_err(|_| invalid(WindowSnapshotIssue::Length))?;
            if u64::try_from(length).unwrap_or(u64::MAX) > typed_limit {
                return Err(invalid(WindowSnapshotIssue::TypedLimit));
            }
            let section_end = length_end
                .checked_add(length)
                .ok_or_else(|| invalid(WindowSnapshotIssue::Length))?;
            let section_bytes = payload
                .get(length_end..section_end)
                .ok_or_else(|| invalid(WindowSnapshotIssue::TypedSection))?;
            let section = decode_aligned_rkyv::<WindowDelayedRemovalSection>(section_bytes)
                .change_context(WindowSnapshotError::Decode {
                    section: WindowSnapshotSection::DelayedRemovals,
                })?;
            let demand = usize::try_from(section.demand)
                .map_err(|_| invalid(WindowSnapshotIssue::TypedDemand))?;
            let descriptor = header
                .accumulators
                .get(demand)
                .ok_or_else(|| invalid(WindowSnapshotIssue::TypedDemand))?;
            let WindowAccumulatorDescriptor::LinearHistogram {
                delayed_removals: declared,
            } = descriptor
            else {
                return Err(invalid(WindowSnapshotIssue::TypedDemand));
            };
            let removals = &mut delayed_removals[demand];
            let next = removals
                .len()
                .checked_add(section.removals.len())
                .ok_or_else(|| invalid(WindowSnapshotIssue::RemovalCount))?;
            if u64::try_from(next).unwrap_or(u64::MAX) > *declared {
                return Err(invalid(WindowSnapshotIssue::RemovalCount));
            }
            removals.extend(section.removals);
            offset = section_end;
        }
        if offset != payload.len() {
            return Err(invalid(WindowSnapshotIssue::TrailingSections));
        }
        let mut accumulators = Vec::with_capacity(header.accumulators.len());
        for (demand, descriptor) in header.accumulators.iter().enumerate() {
            match descriptor {
                WindowAccumulatorDescriptor::Retained => {
                    accumulators.push(WindowAccumulatorSnapshot::Retained);
                }
                WindowAccumulatorDescriptor::LinearHistogram {
                    delayed_removals: declared,
                } => {
                    let removals = std::mem::take(&mut delayed_removals[demand]);
                    if u64::try_from(removals.len()).unwrap_or(u64::MAX) != *declared {
                        return Err(invalid(WindowSnapshotIssue::RemovalCount));
                    }
                    accumulators.push(WindowAccumulatorSnapshot::LinearHistogram {
                        delayed_removals: removals,
                    });
                }
            }
        }

        Ok(Some(Self {
            header,
            input: header_end..input_end,
            arguments: input_end..argument_end,
            accumulators,
        }))
    }
}
