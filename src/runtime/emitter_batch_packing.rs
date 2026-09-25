//! Packing one buffered batch's rows into bounded batch payloads.
//!
//! Layer: data plane.
//!
//! - **Owns.** Preparing every pending row as a batch member, taking candidates in packing order
//!   up to `MAX MESSAGES` while their members stay batch-compatible, halving a candidate whose
//!   encoding reached `MAX SIZE`, and the outcome every row ends with: a member of one payload,
//!   rejected alone, or rejected together with the candidate it shared a failed container with.
//! - **Depends on.** The emitter's compiled codec, which prepares members and encodes containers,
//!   and its declared batching policy.
//! - **Must not know.** Which sink publishes a payload, how its members are acknowledged, or when
//!   the emitter flushes.
//!
//! Packing is synchronous so that it runs wherever the codec's own execution policy requires: a
//! codec with jaq transformations is driven off the reactor, and packing runs inside that same job.

use std::num::NonZeroUsize;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::EmitterBatchPolicy;

use super::EmitterHeaders;
use crate::runtime_schema::{
    BatchContainerError, BatchMember, BatchMemberEncoding, BoundedBatchEncoding, CodecError,
    CompiledCodec, PayloadLimitExceeded, RuntimeRecordBatch,
};

/// What a batch payload carries once for every member: the message key, the written headers and
/// the ordering group. Records that differ in any of them never share a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BatchEnvelope {
    pub(super) key: Option<String>,
    pub(super) headers: EmitterHeaders,
    pub(super) message_group: Option<String>,
}

/// One pending row offered for packing, with the envelope it would be published under.
#[derive(Debug)]
pub(super) struct PackingRow {
    pub(super) row_index: usize,
    pub(super) envelope: BatchEnvelope,
}

/// One payload a candidate was encoded into, with the rows it carries in packing order.
#[derive(Debug)]
pub(super) struct BatchPayload {
    pub(super) rows: Vec<usize>,
    pub(super) envelope: BatchEnvelope,
    pub(super) payload: Vec<u8>,
}

/// How packing ended for one row, or for the rows of one candidate.
#[derive(Debug)]
pub(super) enum PackedOutcome {
    /// The rows are the members of one payload.
    Payload(BatchPayload),
    /// The row's member value could not be produced, so it is rejected alone before packing.
    MemberFailed {
        row_index: usize,
        error: Report<CodecError>,
    },
    /// The row alone still exceeds `MAX SIZE` after a bounded encoding of it.
    Oversize {
        row_index: usize,
        exceeded: PayloadLimitExceeded,
    },
    /// The container of the candidate these rows formed could not be produced, so they fail
    /// together.
    ContainerFailed {
        rows: Vec<usize>,
        error: BatchContainerError,
    },
}

/// Everything packing one buffered batch produced.
#[derive(Debug, Default)]
pub(super) struct BufferedBatchPacking {
    pub(super) outcomes: Vec<PackedOutcome>,
    /// How many candidates were re-encoded because an encoding reached `MAX SIZE`.
    pub(super) subdivisions: u64,
}

/// A row prepared as a batch member.
struct PreparedMember {
    row_index: usize,
    envelope: BatchEnvelope,
    member: BatchMember,
}

impl PreparedMember {
    fn shares_batch_with(&self, other: &Self) -> bool {
        self.envelope == other.envelope && self.member.shares_container_with(&other.member)
    }
}

/// Packs `rows` of `batch`, in the order given, into payloads bounded by `policy`.
///
/// Fails only when the batch cannot be encoded with `codec` at all; every failure that belongs to
/// a row or a candidate is one of the outcomes instead.
pub(super) fn pack_buffered_batch(
    codec: &CompiledCodec,
    batch: &RuntimeRecordBatch,
    rows: Vec<PackingRow>,
    policy: EmitterBatchPolicy,
) -> Result<BufferedBatchPacking, CodecError> {
    let encoder = codec.batch_encoder(batch)?;
    let mut packing = BufferedBatchPacking::default();
    let mut members = Vec::with_capacity(rows.len());
    for PackingRow {
        row_index,
        envelope,
    } in rows
    {
        match encoder.batch_member(row_index, policy.max_size) {
            Ok(BatchMemberEncoding::Member(member)) => members.push(PreparedMember {
                row_index,
                envelope,
                member,
            }),
            Ok(BatchMemberEncoding::Oversize(exceeded)) => {
                packing.outcomes.push(PackedOutcome::Oversize {
                    row_index,
                    exceeded,
                });
            }
            Err(error) => {
                packing
                    .outcomes
                    .push(PackedOutcome::MemberFailed { row_index, error });
            }
        }
    }

    let max_messages = NonZeroUsize::try_from(policy.max_messages.get())
        .assured("Nervix builds for 64-bit targets only, where usize holds every u32");
    let candidates = subdivide(
        &members,
        max_messages,
        PreparedMember::shares_batch_with,
        |candidate| {
            let references = candidate
                .iter()
                .map(|prepared| &prepared.member)
                .collect::<Vec<_>>();
            match codec.encode_batch_within(&references, policy.max_size)? {
                BoundedBatchEncoding::Encoded(payload) => Ok(CandidateEncoding::Fits(payload)),
                BoundedBatchEncoding::Oversize(exceeded) => {
                    Ok(CandidateEncoding::Oversize(exceeded))
                }
            }
        },
    );
    packing.subdivisions = candidates.subdivisions;
    for candidate in candidates.candidates {
        let chosen = members
            .get(candidate.start..candidate.end)
            .assured("subdivide yields ranges within the members it was given");
        let rows = chosen
            .iter()
            .map(|prepared| prepared.row_index)
            .collect::<Vec<_>>();
        let outcome = match candidate.result {
            CandidateResult::Encoded(payload) => {
                let first = chosen
                    .first()
                    .assured("subdivide never yields an empty candidate");
                PackedOutcome::Payload(BatchPayload {
                    rows,
                    envelope: first.envelope.clone(),
                    payload,
                })
            }
            CandidateResult::Oversize(exceeded) => {
                let row_index = *rows
                    .first()
                    .assured("subdivide rejects an oversize candidate only once it holds one row");
                PackedOutcome::Oversize {
                    row_index,
                    exceeded,
                }
            }
            CandidateResult::Failed(error) => PackedOutcome::ContainerFailed { rows, error },
        };
        packing.outcomes.push(outcome);
    }
    Ok(packing)
}

/// What encoding one candidate produced.
#[derive(Debug)]
enum CandidateEncoding<O> {
    Fits(Vec<u8>),
    Oversize(O),
}

/// How one candidate ended.
#[derive(Debug, PartialEq, Eq)]
enum CandidateResult<O, F> {
    Encoded(Vec<u8>),
    /// A single member whose encoding still reached the limit.
    Oversize(O),
    Failed(F),
}

/// One final candidate: the members `start..end` and how it ended.
#[derive(Debug, PartialEq, Eq)]
struct Candidate<O, F> {
    start: usize,
    end: usize,
    result: CandidateResult<O, F>,
}

#[derive(Debug)]
struct Subdivision<O, F> {
    candidates: Vec<Candidate<O, F>>,
    subdivisions: u64,
}

/// Divides `members` into candidates in order.
///
/// A candidate is the longest run from the front of what remains that holds at most
/// `max_messages` members, each batch-compatible with the first. A candidate whose encoding reached
/// the limit is halved — its first half becomes the new candidate and the rest returns to the
/// front — and re-encoded, because a smaller candidate may encode larger. A single member that
/// still reaches the limit ends as oversize. A candidate of `n` members therefore takes at most
/// `⌈log2(n)⌉ + 1` encodings.
fn subdivide<M, O, F>(
    members: &[M],
    max_messages: NonZeroUsize,
    compatible: impl Fn(&M, &M) -> bool,
    mut encode: impl FnMut(&[M]) -> Result<CandidateEncoding<O>, F>,
) -> Subdivision<O, F> {
    let mut subdivision = Subdivision {
        candidates: Vec::new(),
        subdivisions: 0,
    };
    let mut start = 0;
    while let Some(first) = members.get(start) {
        let mut end = start + 1;
        while end - start < max_messages.get()
            && let Some(next) = members.get(end)
            && compatible(first, next)
        {
            end += 1;
        }
        let result = loop {
            let candidate = members
                .get(start..end)
                .assured("start..end stays within the members the loop above walked");
            match encode(candidate) {
                Ok(CandidateEncoding::Fits(payload)) => break CandidateResult::Encoded(payload),
                Ok(CandidateEncoding::Oversize(exceeded)) => {
                    let len = end - start;
                    if len == 1 {
                        break CandidateResult::Oversize(exceeded);
                    }
                    end = start + len.div_ceil(2);
                    subdivision.subdivisions = subdivision
                        .subdivisions
                        .checked_add(1)
                        .assured("each subdivision halves a candidate of at most 65,536 members");
                }
                Err(error) => break CandidateResult::Failed(error),
            }
        };
        subdivision
            .candidates
            .push(Candidate { start, end, result });
        start = end;
    }
    subdivision
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    fn limit(messages: usize) -> NonZeroUsize {
        NonZeroUsize::new(messages).assured("every limit these tests use is positive")
    }

    /// Encodes a candidate of `u8` members as their bytes, as a concatenating container would.
    fn concatenated(candidate: &[u8]) -> Vec<u8> {
        candidate.to_vec()
    }

    #[test]
    fn takes_candidates_up_to_the_message_limit_in_order() {
        let members = [1_u8, 2, 3, 4, 5];

        let subdivision = subdivide(
            &members,
            limit(2),
            |_, _| true,
            |candidate| Ok::<_, ()>(CandidateEncoding::<()>::Fits(concatenated(candidate))),
        );

        assert_eq!(
            subdivision.candidates,
            vec![
                Candidate {
                    start: 0,
                    end: 2,
                    result: CandidateResult::Encoded(vec![1, 2])
                },
                Candidate {
                    start: 2,
                    end: 4,
                    result: CandidateResult::Encoded(vec![3, 4])
                },
                Candidate {
                    start: 4,
                    end: 5,
                    result: CandidateResult::Encoded(vec![5])
                },
            ]
        );
        assert_eq!(subdivision.subdivisions, 0);
    }

    #[test]
    fn an_incompatible_member_closes_the_candidate_without_reordering() {
        let members = [1_u8, 1, 2, 1];

        let subdivision = subdivide(
            &members,
            limit(10),
            |first, next| first == next,
            |candidate| Ok::<_, ()>(CandidateEncoding::<()>::Fits(concatenated(candidate))),
        );

        let ranges = subdivision
            .candidates
            .iter()
            .map(|candidate| (candidate.start, candidate.end))
            .collect::<Vec<_>>();
        assert_eq!(ranges, vec![(0, 2), (2, 3), (3, 4)]);
    }

    #[test]
    fn halves_an_oversize_candidate_and_returns_the_rest_to_the_front() {
        let members = [1_u8, 2, 3, 4, 5];
        let attempts = RefCell::new(Vec::new());

        let subdivision = subdivide(
            &members,
            limit(5),
            |_, _| true,
            |candidate| {
                attempts.borrow_mut().push(candidate.to_vec());
                if candidate.len() > 2 {
                    return Ok::<_, ()>(CandidateEncoding::Oversize("over"));
                }
                Ok(CandidateEncoding::Fits(concatenated(candidate)))
            },
        );

        assert_eq!(
            attempts.into_inner(),
            vec![
                vec![1, 2, 3, 4, 5],
                vec![1, 2, 3],
                vec![1, 2],
                vec![3, 4, 5],
                vec![3, 4],
                vec![5],
            ]
        );
        let ranges = subdivision
            .candidates
            .iter()
            .map(|candidate| (candidate.start, candidate.end))
            .collect::<Vec<_>>();
        assert_eq!(ranges, vec![(0, 2), (2, 4), (4, 5)]);
        assert_eq!(subdivision.subdivisions, 3);
    }

    #[test]
    fn re_encodes_every_halving_because_a_smaller_candidate_may_encode_larger() {
        let members = [1_u8, 2, 3, 4];
        let attempts = RefCell::new(0_usize);

        // Four members fit, two do not, and one does: the size is not monotonic in the count.
        let subdivision = subdivide(
            &members,
            limit(4),
            |_, _| true,
            |candidate| {
                *attempts.borrow_mut() += 1;
                match candidate.len() {
                    2 | 3 => Ok::<_, ()>(CandidateEncoding::Oversize("padded")),
                    _ => Ok(CandidateEncoding::Fits(concatenated(candidate))),
                }
            },
        );

        let ranges = subdivision
            .candidates
            .iter()
            .map(|candidate| (candidate.start, candidate.end))
            .collect::<Vec<_>>();
        assert_eq!(ranges, vec![(0, 4)]);
        assert_eq!(*attempts.borrow(), 1);

        let members = [1_u8, 2, 3, 4, 5, 6, 7, 8];
        let subdivision = subdivide(
            &members,
            limit(8),
            |_, _| true,
            |candidate| match candidate.len() {
                8 | 2 => Ok::<_, ()>(CandidateEncoding::Oversize("padded")),
                _ => Ok(CandidateEncoding::Fits(concatenated(candidate))),
            },
        );
        let ranges = subdivision
            .candidates
            .iter()
            .map(|candidate| (candidate.start, candidate.end))
            .collect::<Vec<_>>();
        assert_eq!(ranges, vec![(0, 4), (4, 8)]);
    }

    #[test]
    fn a_single_member_that_still_reaches_the_limit_is_oversize_and_packing_continues() {
        let members = [1_u8, 9, 2];

        let subdivision = subdivide(
            &members,
            limit(3),
            |_, _| true,
            |candidate| {
                if candidate.contains(&9) {
                    return Ok::<_, ()>(CandidateEncoding::Oversize("nine"));
                }
                Ok(CandidateEncoding::Fits(concatenated(candidate)))
            },
        );

        assert_eq!(
            subdivision.candidates,
            vec![
                Candidate {
                    start: 0,
                    end: 1,
                    result: CandidateResult::Encoded(vec![1])
                },
                Candidate {
                    start: 1,
                    end: 2,
                    result: CandidateResult::Oversize("nine")
                },
                Candidate {
                    start: 2,
                    end: 3,
                    result: CandidateResult::Encoded(vec![2])
                },
            ]
        );
    }

    #[test]
    fn a_failed_container_fails_its_whole_candidate_without_subdividing() {
        let members = [1_u8, 2, 3];

        let subdivision = subdivide(
            &members,
            limit(2),
            |_, _| true,
            |candidate| {
                if candidate.len() == 2 {
                    return Err("no output");
                }
                Ok(CandidateEncoding::<()>::Fits(concatenated(candidate)))
            },
        );

        assert_eq!(
            subdivision.candidates,
            vec![
                Candidate {
                    start: 0,
                    end: 2,
                    result: CandidateResult::Failed("no output")
                },
                Candidate {
                    start: 2,
                    end: 3,
                    result: CandidateResult::Encoded(vec![3])
                },
            ]
        );
        assert_eq!(subdivision.subdivisions, 0);
    }

    #[test]
    fn encodes_a_candidate_at_most_log2_plus_one_times() {
        let members = [0_u8; 8];
        let attempts = RefCell::new(Vec::new());

        let subdivision = subdivide(
            &members,
            limit(8),
            |_, _| true,
            |candidate| {
                attempts.borrow_mut().push(candidate.len());
                if candidate.len() > 1 {
                    return Ok::<_, ()>(CandidateEncoding::Oversize("over"));
                }
                Ok(CandidateEncoding::Fits(concatenated(candidate)))
            },
        );

        let first = subdivision
            .candidates
            .first()
            .assured("eight members yield at least one candidate");
        assert_eq!((first.start, first.end), (0, 1));
        let attempts = attempts.into_inner();
        assert_eq!(attempts.get(..4), Some([8, 4, 2, 1].as_slice()));
        assert_eq!(attempts.get(4..8), Some([7, 4, 2, 1].as_slice()));
    }
}
