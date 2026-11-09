//! Layer: engines and infrastructure.
//! Owns: the current inferencer snapshot frame, bit-exact tensor projection and its section bound.
//! Depends on: rkyv, checked byte arithmetic and the execution cancellation token.
//! Must not know: Models, ONNX sessions, placements, graph decisions or branch tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the inferencer snapshot representation")
)]

use std::{
    borrow::Cow,
    mem::{align_of, size_of},
};

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use nervix_execution::Cancellation;
use rkyv::{Archive, Deserialize, Serialize};

const SIGNATURE: &[u8; 16] = b"INFERENCER STATE";

#[derive(Archive, Serialize, Deserialize)]
pub(crate) struct TensorSnapshot<'a> {
    pub(crate) shape: Vec<u64>,
    #[rkyv(with = rkyv::with::AsOwned)]
    pub(crate) values: Cow<'a, [f32]>,
}

#[derive(Archive, Serialize, Deserialize)]
pub(crate) struct BranchSnapshot<'a> {
    pub(crate) incarnation: u64,
    pub(crate) tensors: Vec<TensorSnapshot<'a>>,
}

#[derive(Archive, Serialize, Deserialize)]
pub(crate) struct Snapshot<'a> {
    pub(crate) revision: u64,
    pub(crate) branch: Option<BranchSnapshot<'a>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotError {
    #[error("inferencer snapshot format is unrecognised; recreate the stored state")]
    Format,
    #[error("inferencer snapshot is invalid; recreate the stored state")]
    Invalid,
    #[error("inferencer snapshot byte calculation overflowed")]
    Size,
    #[error(
        "inferencer state requires {bytes} snapshot bytes, exceeding one configured snapshot \
         section ({limit} bytes)"
    )]
    Bound { bytes: u64, limit: u64 },
    #[error("inferencer snapshot work was cancelled")]
    Cancelled,
    #[error("failed to encode inferencer snapshot")]
    Encode,
    #[error("failed to decode inferencer snapshot; recreate the stored state")]
    Decode,
}

/// A conservative section footprint derived from archive layout and sealing metadata.
/// It includes the temporary tensor descriptors and serializer resolvers so two charged sections
/// cover a retained native generation and its frame even for many small tensors. No data is allocated.
#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "callers supply finite projections of declared or archived tensor lengths; this \
                  loop performs checked byte arithmetic without external callbacks"
    )
)]
pub(crate) fn footprint(
    tensors: impl IntoIterator<Item = (u64, u64)>,
) -> Result<u64, Report<SnapshotError>> {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the snapshot frame stores its signature, archive root and root alignment"
        )
    )]
    const HEADER: usize =
        SIGNATURE.len() + size_of::<ArchivedSnapshot>() + align_of::<ArchivedSnapshot>() - 1;
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "each tensor's section charge covers its archive descriptor, simultaneous \
                      native descriptors, serializer resolver and sequence alignment"
        )
    )]
    const TENSOR: usize = size_of::<ArchivedTensorSnapshot>()
        + 2 * size_of::<TensorSnapshot<'static>>()
        + size_of::<TensorSnapshotResolver>()
        + align_of::<ArchivedTensorSnapshot>()
        - 1
        + align_of::<u64>()
        - 1
        + align_of::<f32>()
        - 1;
    let mut bytes = u64::try_from(HEADER).map_err(|_| Report::new(SnapshotError::Size))?;
    for (axes, elements) in tensors {
        let shape_bytes = axes
            .checked_mul(size_of::<u64>().arch_into())
            .ok_or_else(|| Report::new(SnapshotError::Size))?;
        let value_bytes = elements
            .checked_mul(size_of::<f32>().arch_into())
            .ok_or_else(|| Report::new(SnapshotError::Size))?;
        let data_bytes = shape_bytes
            .checked_add(value_bytes)
            .ok_or_else(|| Report::new(SnapshotError::Size))?;
        let extra = data_bytes
            .checked_add(TENSOR.arch_into())
            .ok_or_else(|| Report::new(SnapshotError::Size))?;
        bytes = bytes
            .checked_add(extra)
            .ok_or_else(|| Report::new(SnapshotError::Size))?;
    }
    Ok(bytes)
}

pub(crate) fn check_bound(bytes: u64, limit: u64) -> Result<(), Report<SnapshotError>> {
    if bytes > limit {
        return Err(Report::new(SnapshotError::Bound { bytes, limit }));
    }
    Ok(())
}

/// The two simultaneously held sections of sealing/restoration, with checked accounting.
pub(crate) fn working_bytes(bytes: u64) -> Result<u64, Report<SnapshotError>> {
    bytes
        .checked_mul(2)
        .ok_or_else(|| Report::new(SnapshotError::Size))
}

impl Snapshot<'_> {
    fn validate(&self, cancellation: &Cancellation) -> Result<(), Report<SnapshotError>> {
        if let Some(branch) = &self.branch {
            if branch.tensors.is_empty() {
                return Err(Report::new(SnapshotError::Invalid));
            }
            for tensor in &branch.tensors {
                cancellation
                    .check()
                    .change_context(SnapshotError::Cancelled)?;
                let count = tensor.shape.iter().try_fold(1_u64, |n, axis| {
                    if *axis == 0 {
                        None
                    } else {
                        n.checked_mul(*axis)
                    }
                });
                if count != Some(tensor.values.len().arch_into()) {
                    return Err(Report::new(SnapshotError::Invalid));
                }
            }
        }
        Ok(())
    }

    /// Called on a bounded CPU worker after charging the native projection and archive buffers.
    pub(crate) fn encode(
        &self,
        limit: u64,
        cancellation: &Cancellation,
    ) -> Result<Vec<u8>, Report<SnapshotError>> {
        self.validate(cancellation)?;
        let sizes = self
            .branch
            .iter()
            .flat_map(|branch| branch.tensors.iter())
            .map(|t| (t.shape.len().arch_into(), t.values.len().arch_into()));
        check_bound(footprint(sizes)?, limit)?;
        let bound = footprint(
            self.branch
                .iter()
                .flat_map(|branch| &branch.tensors)
                .map(|t| (t.shape.len().arch_into(), t.values.len().arch_into())),
        )?;
        let capacity = usize::try_from(bound).change_context(SnapshotError::Size)?;
        let mut framed = Vec::with_capacity(capacity);
        framed.extend_from_slice(SIGNATURE);
        let framed = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(self, framed)
            .change_context(SnapshotError::Encode)?;
        check_bound(framed.len().arch_into(), limit)?;
        Ok(framed)
    }

    /// Called on a bounded CPU worker. Length is checked before alignment and deserialization.
    pub(crate) fn decode(
        raw: Vec<u8>,
        limit: u64,
        cancellation: &Cancellation,
    ) -> Result<Snapshot<'static>, Report<SnapshotError>> {
        check_bound(raw.len().arch_into(), limit)?;
        let Some(payload) = raw.strip_prefix(SIGNATURE) else {
            return Err(Report::new(SnapshotError::Format));
        };
        cancellation
            .check()
            .change_context(SnapshotError::Cancelled)?;
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
        aligned.extend_from_slice(payload);
        drop(raw);
        let archived = rkyv::access::<ArchivedSnapshot, rkyv::rancor::Error>(&aligned)
            .change_context(SnapshotError::Decode)?;
        // Inspect archived lengths before allocating any owned tensors. A compact frame with
        // many tiny tensors must pay for their native descriptors as well as their float bytes.
        check_bound(
            footprint(
                archived
                    .branch
                    .iter()
                    .flat_map(|branch| branch.tensors.iter())
                    .map(|tensor| {
                        (
                            tensor.shape.len().arch_into(),
                            tensor.values.len().arch_into(),
                        )
                    }),
            )?,
            limit,
        )?;
        let decoded = rkyv::from_bytes::<Snapshot<'static>, rkyv::rancor::Error>(&aligned)
            .change_context(SnapshotError::Decode)?;
        decoded.validate(cancellation)?;
        check_bound(
            footprint(
                decoded
                    .branch
                    .iter()
                    .flat_map(|b| &b.tensors)
                    .map(|t| (t.shape.len().arch_into(), t.values.len().arch_into())),
            )?,
            limit,
        )?;
        Ok(decoded)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "verifies the current snapshot frame")
)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_execution::{CpuClass, Executor, MemoryClass};

    use super::*;

    fn in_job(work: impl FnOnce(&Cancellation) + Send + 'static) {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("the case runtime opens");
        let executor = Executor::default();
        runtime.block_on(async {
            let charge = executor
                .reserve(MemoryClass::Bulk, 64 * 1024)
                .await
                .assured("the bounded case is admitted");
            executor
                .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                    work(cancellation)
                })
                .await
                .assured("the case job completes");
        });
    }

    fn generated(bytes: &[u8]) -> Snapshot<'static> {
        let tensors = (0..=bytes.len().min(3))
            .map(|index| {
                let count = 1 + usize::from(bytes.get(index).copied().unwrap_or_default() % 16);
                let mut bits = vec![
                    0,
                    (-0.0_f32).to_bits(),
                    f32::INFINITY.to_bits(),
                    f32::NEG_INFINITY.to_bits(),
                    0x7fc1_2345,
                    0x7f80_0001,
                ];
                bits.extend(bytes.chunks(4).map(|chunk| {
                    let mut word = [0; 4];
                    word[..chunk.len()].copy_from_slice(chunk);
                    u32::from_le_bytes(word)
                }));
                let values = bits
                    .into_iter()
                    .cycle()
                    .take(count)
                    .map(f32::from_bits)
                    .collect();
                TensorSnapshot {
                    shape: vec![1, count.arch_into()],
                    values: Cow::Owned(values),
                }
            })
            .collect();
        Snapshot {
            revision: u64::from(bytes.first().copied().unwrap_or_default()),
            branch: Some(BranchSnapshot {
                incarnation: u64::MAX,
                tensors,
            }),
        }
    }

    fn assert_same(left: &Snapshot, right: &Snapshot) {
        assert_eq!(left.revision, right.revision);
        match (&left.branch, &right.branch) {
            (None, None) => {}
            (Some(left), Some(right)) => {
                assert_eq!(left.incarnation, right.incarnation);
                assert_eq!(left.tensors.len(), right.tensors.len());
                for (left, right) in left.tensors.iter().zip(&right.tensors) {
                    assert_eq!(left.shape, right.shape);
                    assert_eq!(
                        left.values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                        right.values.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
                    );
                }
            }
            _ => panic!("branch presence changed"),
        }
    }

    #[nervix_primitives::test]
    async fn a_near_section_limit_frame_fits_the_existing_bulk_operation_window() {
        let executor = Executor::default();
        let section = executor.limits().snapshot_section_bytes.as_u64();
        let charge = executor
            .reserve(
                MemoryClass::Bulk,
                section
                    .checked_mul(2)
                    .expect("the configured section working set fits"),
            )
            .await
            .assured("two sections fit the existing bulk window");
        executor
            .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                let overhead = footprint([(1, 0)]).assured("fixed metadata fits");
                let float_bytes: u64 = size_of::<f32>().arch_into();
                let elements = usize::try_from((section - overhead) / float_bytes)
                    .expect("the bounded tensor fits the address space");
                let current = Snapshot {
                    revision: 71,
                    branch: Some(BranchSnapshot {
                        incarnation: 19,
                        tensors: vec![TensorSnapshot {
                            shape: vec![elements.arch_into()],
                            values: Cow::Owned(vec![f32::from_bits(0x7fc1_2345); elements]),
                        }],
                    }),
                };
                let encoded = current
                    .encode(section, cancellation)
                    .assured("the near-limit frame seals");
                drop(current);
                let decoded = Snapshot::decode(encoded, section, cancellation)
                    .assured("the near-limit frame restores");
                assert_eq!(decoded.revision, 71);
                let branch = decoded.branch.expect("the frame carries its branch");
                assert_eq!(branch.incarnation, 19);
                let axis: u64 = elements.arch_into();
                assert_eq!(branch.tensors[0].shape, [axis]);
                assert!(
                    branch.tensors[0]
                        .values
                        .iter()
                        .all(|value| value.to_bits() == 0x7fc1_2345)
                );
            })
            .await
            .assured("the bounded frame job completes");
    }

    #[test]
    fn bolero_inferencer_snapshots_preserve_shapes_float_bits_and_lifetimes() {
        bolero::check!()
            .with_iterations(128)
            .with_max_len(512)
            .for_each(|bytes| {
                let current = generated(bytes);
                in_job(move |cancellation| {
                    let bound = footprint(
                        current
                            .branch
                            .iter()
                            .flat_map(|b| &b.tensors)
                            .map(|t| (t.shape.len().arch_into(), t.values.len().arch_into())),
                    )
                    .assured("the bounded footprint fits");
                    let encoded = current
                        .encode(bound, cancellation)
                        .assured("the complete frame fits the derived bound");
                    assert!(u64::try_from(encoded.len()).expect("bounded encoding") <= bound);
                    let restored = Snapshot::decode(encoded, bound, cancellation)
                        .assured("a current frame decodes");
                    assert_same(&current, &restored);
                    assert!(current.encode(bound - 1, cancellation).is_err());
                    let empty = Snapshot {
                        revision: u64::MAX,
                        branch: None,
                    };
                    let payload = empty
                        .encode(bound, cancellation)
                        .assured("the empty eviction frame fits");
                    assert_same(
                        &empty,
                        &Snapshot::decode(payload, bound, cancellation)
                            .assured("the eviction frame decodes"),
                    );
                });
            });
    }

    #[test]
    fn bolero_malformed_inferencer_snapshots_fail_typed_or_reencode_complete_values() {
        bolero::check!()
            .with_iterations(128)
            .with_max_len(512)
            .for_each(|bytes| {
                let raw = bytes.to_vec();
                let current = generated(bytes);
                in_job(move |cancellation| {
                    let mut damaged = current
                        .encode(4096, cancellation)
                        .assured("the bounded frame encodes");
                    if let Some(byte) = raw.first() {
                        let index = usize::from(*byte) % damaged.len();
                        damaged[index] ^= raw.last().copied().unwrap_or(1).max(1);
                    }
                    let mut arbitrary = SIGNATURE.to_vec();
                    arbitrary.extend_from_slice(&raw);
                    for payload in [damaged, arbitrary, raw] {
                        if let Ok(decoded) = Snapshot::decode(payload, 4096, cancellation) {
                            let encoded = decoded
                                .encode(8192, cancellation)
                                .assured("a bounded decoded frame reencodes");
                            assert_same(
                                &decoded,
                                &Snapshot::decode(encoded, 8192, cancellation)
                                    .assured("the reencoded frame decodes"),
                            );
                        }
                    }
                    assert!(footprint([(u64::MAX, 1)]).is_err());
                    assert!(footprint([(1, u64::MAX)]).is_err());
                });
            });
    }
}
