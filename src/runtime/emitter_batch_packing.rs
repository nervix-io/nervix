//! Packing selected rows from successive Arrow carriers into bounded batch payloads.
//!
//! Layer: data plane.
//!
//! - **Owns.** Preparing selected rows as batch members across carriers, taking candidates in
//!   source order up to `MAX MESSAGES` while their members stay batch-compatible, halving one whose
//!   encoding reached `MAX SIZE`, and the outcome every row ends with: a member of one payload,
//!   rejected alone, or rejected together with the candidate it shared a failed container with.
//! - **Depends on.** The emitter's compiled codec, which prepares members and encodes containers,
//!   and its declared batching policy, with source relay and concrete branch identity.
//! - **Must not know.** Which sink publishes a payload, how its members are acknowledged, or when
//!   the emitter flushes.
//!
//! Packing is synchronous so that it runs wherever the codec's own execution policy requires: a
//! codec with jaq transformations is driven off the reactor, and packing runs inside that same job.

use std::num::NonZeroUsize;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::SinkRecordPosition;
use nervix_models::{EmitterBatchPolicy, RelayName};
use triomphe::Arc;

use super::{BranchKey, EmitterHeaders};
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

/// One row of a carrier, or a rejected row that seals the current payload.
#[derive(Debug)]
pub(super) enum PackingRow {
    Ready {
        position: SinkRecordPosition,
        envelope: BatchEnvelope,
    },
    Seal,
}

/// One Arrow carrier and the rows selected from it. Its Arc'd columns stay intact while the codec
/// reads row views; neither the host nor the packer copies a row into a field map.
#[derive(Debug)]
pub(super) struct PackingCarrier {
    pub(super) source_relay: RelayName,
    pub(super) branch_key: Option<BranchKey>,
    pub(super) batch: Arc<RuntimeRecordBatch>,
    pub(super) rows: Vec<PackingRow>,
}

/// One payload a candidate was encoded into, with the rows it carries in packing order.
#[derive(Debug)]
pub(super) struct BatchPayload {
    pub(super) rows: Vec<SinkRecordPosition>,
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
        position: SinkRecordPosition,
        error: Report<CodecError>,
    },
    /// The row alone still exceeds `MAX SIZE` after a bounded encoding of it.
    Oversize {
        position: SinkRecordPosition,
        exceeded: PayloadLimitExceeded,
    },
    /// The container of the candidate these rows formed could not be produced, so they fail
    /// together.
    ContainerFailed {
        rows: Vec<SinkRecordPosition>,
        error: BatchContainerError,
    },
}

/// Everything packing the emitter's currently released carriers produced.
#[derive(Debug, Default)]
pub(super) struct BufferedBatchPacking {
    pub(super) outcomes: Vec<PackedOutcome>,
    /// How many candidates were re-encoded because an encoding reached `MAX SIZE`.
    pub(super) subdivisions: u64,
}

/// A row prepared as a batch member.
struct PreparedMember<'a> {
    position: SinkRecordPosition,
    source_relay: &'a RelayName,
    branch_key: &'a Option<BranchKey>,
    envelope: &'a BatchEnvelope,
    member: BatchMember,
}

impl PreparedMember<'_> {
    fn shares_batch_with(&self, other: &Self) -> bool {
        self.source_relay == other.source_relay
            && self.branch_key == other.branch_key
            && self.envelope == other.envelope
            && self.member.shares_container_with(&other.member)
    }
}

/// Packs the selected rows of successive carriers in their arrival order. Each carrier keeps its
/// own Arc-backed Arrow columns and source identity; a failed or rejected row seals the open run.
pub(super) fn pack_buffered_batches(
    codec: &CompiledCodec,
    carriers: Vec<PackingCarrier>,
    policy: EmitterBatchPolicy,
) -> error_stack::Result<BufferedBatchPacking, CodecError> {
    let mut packing = BufferedBatchPacking::default();
    let mut run = Vec::new();
    let max_messages = NonZeroUsize::try_from(policy.max_messages.get())
        .assured("Nervix builds for 64-bit targets only, where usize holds every u32");
    for carrier in &carriers {
        if carrier.rows.is_empty() {
            continue;
        }
        let encoder = codec.batch_encoder(&carrier.batch)?;
        for row in &carrier.rows {
            let (position, envelope) = match row {
                PackingRow::Ready { position, envelope } => (*position, envelope),
                PackingRow::Seal => {
                    packing.pack_run(codec, &mut run, policy, max_messages);
                    continue;
                }
            };
            match encoder.batch_member(position.row_index, policy.max_size) {
                Ok(BatchMemberEncoding::Member(member)) => {
                    let prepared = PreparedMember {
                        position,
                        source_relay: &carrier.source_relay,
                        branch_key: &carrier.branch_key,
                        envelope,
                        member,
                    };
                    if let Some(first) = run.first()
                        && !first.shares_batch_with(&prepared)
                    {
                        packing.pack_run(codec, &mut run, policy, max_messages);
                    }
                    run.push(prepared);
                    if run.len() == max_messages.get() {
                        packing.pack_run(codec, &mut run, policy, max_messages);
                    }
                }
                Ok(BatchMemberEncoding::Oversize(exceeded)) => {
                    packing.pack_run(codec, &mut run, policy, max_messages);
                    packing
                        .outcomes
                        .push(PackedOutcome::Oversize { position, exceeded });
                }
                Err(error) => {
                    packing.pack_run(codec, &mut run, policy, max_messages);
                    packing
                        .outcomes
                        .push(PackedOutcome::MemberFailed { position, error });
                }
            }
        }
    }
    packing.pack_run(codec, &mut run, policy, max_messages);
    Ok(packing)
}

impl BufferedBatchPacking {
    fn pack_run(
        &mut self,
        codec: &CompiledCodec,
        members: &mut Vec<PreparedMember<'_>>,
        policy: EmitterBatchPolicy,
        max_messages: NonZeroUsize,
    ) {
        if members.is_empty() {
            return;
        }
        let candidates = subdivide(
            members,
            max_messages,
            PreparedMember::shares_batch_with,
            |candidate| {
                let references = candidate
                    .iter()
                    .map(|prepared| &prepared.member)
                    .collect::<Vec<_>>();
                let encoded = codec
                    .encode_batch_within(&references, policy.max_size)
                    .map_err(|error| *error.current_context())?;
                match encoded {
                    BoundedBatchEncoding::Encoded(payload) => Ok(CandidateEncoding::Fits(payload)),
                    BoundedBatchEncoding::Oversize(exceeded) => {
                        Ok(CandidateEncoding::Oversize(exceeded))
                    }
                }
            },
        );
        self.subdivisions = self
            .subdivisions
            .checked_add(candidates.subdivisions)
            .assured("the subdivision count cannot exceed work on buffered source rows");
        for candidate in candidates.candidates {
            let chosen = members
                .get(candidate.start..candidate.end)
                .assured("subdivide yields ranges within the members it was given");
            let rows = chosen
                .iter()
                .map(|prepared| prepared.position)
                .collect::<Vec<_>>();
            let outcome = match candidate.result {
                CandidateResult::Encoded(payload) => {
                    let first = chosen
                        .first()
                        .assured("subdivide never yields an empty candidate");
                    PackedOutcome::Payload(BatchPayload {
                        rows,
                        envelope: (*first.envelope).clone(),
                        payload,
                    })
                }
                CandidateResult::Oversize(exceeded) => {
                    let position = *rows.first().assured(
                        "subdivide rejects an oversize candidate only once it holds one row",
                    );
                    PackedOutcome::Oversize { position, exceeded }
                }
                CandidateResult::Failed(error) => PackedOutcome::ContainerFailed { rows, error },
            };
            self.outcomes.push(outcome);
        }
        members.clear();
    }
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

    use nervix_models::{
        BatchMessageLimit, CodecWireFormat, CreateCodec, CreateWireSchema, FieldName, JsonType,
        ResolvedCodecWireFormat, WireSchemaField,
    };

    use super::*;
    use crate::{
        runtime::{
            RuntimeValue,
            test_fixtures::{input_batch_with, input_schema, named},
        },
        runtime_ack::AckSet,
        runtime_schema::compile_codec,
    };

    fn test_codec() -> Arc<CompiledCodec> {
        let wire = CreateWireSchema {
            name: named("input_wire"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: named("value"),
                ty: JsonType::Integer,
                optional: false,
            }],
        };
        let model = CreateCodec {
            name: named("input_codec"),
            wire_format: CodecWireFormat::Json {
                wire_schema: wire.name.clone(),
            },
            schema: named("emitter_input"),
            encoding_rules: Vec::new(),
        };
        compile_codec(&model, input_schema(), ResolvedCodecWireFormat::Json(&wire))
            .assured("the test codec and Arrow schema both define one required integer field")
    }

    fn test_policy(max_messages: u32) -> EmitterBatchPolicy {
        EmitterBatchPolicy {
            max_messages: BatchMessageLimit::try_from(max_messages)
                .assured("the test limit is positive and below the declared maximum"),
            max_size: "1KiB"
                .parse()
                .assured("the fixed test size is a positive byte limit"),
        }
    }

    fn carrier(value: i64, batch_index: usize, relay: &str, group: &str) -> PackingCarrier {
        let source = input_batch_with(value, 0, AckSet::empty());
        PackingCarrier {
            source_relay: named(relay),
            branch_key: source.key,
            batch: source.batch,
            rows: vec![PackingRow::Ready {
                position: SinkRecordPosition {
                    batch_index,
                    row_index: 0,
                },
                envelope: BatchEnvelope {
                    key: None,
                    headers: Vec::new(),
                    message_group: Some(group.to_string()),
                },
            }],
        }
    }

    #[test]
    fn packs_across_arrow_carriers_but_seals_on_ordering_group_and_source_relay() {
        let carriers = vec![
            carrier(1, 0, "source_a", "one"),
            carrier(2, 1, "source_a", "one"),
            carrier(3, 2, "source_a", "two"),
            carrier(4, 3, "source_b", "one"),
            carrier(5, 4, "source_b", "one"),
        ];
        let packed = pack_buffered_batches(&test_codec(), carriers, test_policy(3))
            .assured("all selected Arrow rows match the test codec");
        let payloads = packed
            .outcomes
            .into_iter()
            .map(|outcome| match outcome {
                PackedOutcome::Payload(payload) => (payload.rows, payload.payload),
                other => panic!("every test row should encode into a payload, found {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            payloads,
            vec![
                (
                    vec![
                        SinkRecordPosition {
                            batch_index: 0,
                            row_index: 0
                        },
                        SinkRecordPosition {
                            batch_index: 1,
                            row_index: 0
                        },
                    ],
                    br#"[{"value":1},{"value":2}]"#.to_vec(),
                ),
                (
                    vec![SinkRecordPosition {
                        batch_index: 2,
                        row_index: 0
                    }],
                    br#"[{"value":3}]"#.to_vec(),
                ),
                (
                    vec![
                        SinkRecordPosition {
                            batch_index: 3,
                            row_index: 0
                        },
                        SinkRecordPosition {
                            batch_index: 4,
                            row_index: 0
                        },
                    ],
                    br#"[{"value":4},{"value":5}]"#.to_vec(),
                ),
            ]
        );
    }

    #[test]
    fn a_rejected_row_seals_the_payload_between_compatible_carriers() {
        let mut rejected = carrier(2, 1, "source_a", "one");
        rejected.rows = vec![PackingRow::Seal];
        let carriers = vec![
            carrier(1, 0, "source_a", "one"),
            rejected,
            carrier(3, 2, "source_a", "one"),
        ];
        let packed = pack_buffered_batches(&test_codec(), carriers, test_policy(3))
            .assured("the selected Arrow rows match the test codec");
        let rows = packed
            .outcomes
            .into_iter()
            .map(|outcome| match outcome {
                PackedOutcome::Payload(payload) => payload.rows,
                other => panic!("the selected rows should encode, found {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                vec![SinkRecordPosition {
                    batch_index: 0,
                    row_index: 0
                }],
                vec![SinkRecordPosition {
                    batch_index: 2,
                    row_index: 0
                }],
            ]
        );
    }

    #[test]
    fn holds_at_most_the_declared_message_count_across_carriers() {
        let carriers = (1_i64..=5)
            .enumerate()
            .map(|(index, value)| carrier(value, index, "source_a", "one"))
            .collect();
        let packed = pack_buffered_batches(&test_codec(), carriers, test_policy(2))
            .assured("all five selected Arrow rows match the test codec");
        let member_counts = packed
            .outcomes
            .into_iter()
            .map(|outcome| match outcome {
                PackedOutcome::Payload(payload) => payload.rows.len(),
                other => panic!("every test row should encode into a payload, found {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(member_counts, vec![2, 2, 1]);
    }

    #[test]
    fn a_different_concrete_branch_seals_the_payload() {
        let mut carriers = vec![
            carrier(1, 0, "source_a", "one"),
            carrier(2, 1, "source_a", "one"),
            carrier(3, 2, "source_a", "one"),
        ];
        for (carrier, tenant) in carriers.iter_mut().zip(["acme", "beta", "acme"]) {
            carrier.branch_key = Some(
                BranchKey::from_fields([(
                    FieldName::parse("tenant")
                        .assured("the fixed field satisfies the field name grammar"),
                    RuntimeValue::String(tenant.to_string()),
                )])
                .assured("the fixed field makes a concrete branch key"),
            );
        }
        let packed = pack_buffered_batches(&test_codec(), carriers, test_policy(3))
            .assured("all selected Arrow rows match the test codec");
        let positions = packed
            .outcomes
            .into_iter()
            .map(|outcome| match outcome {
                PackedOutcome::Payload(payload) => payload.rows,
                other => panic!("every test row should encode into a payload, found {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            positions,
            vec![
                vec![SinkRecordPosition {
                    batch_index: 0,
                    row_index: 0
                }],
                vec![SinkRecordPosition {
                    batch_index: 1,
                    row_index: 0
                }],
                vec![SinkRecordPosition {
                    batch_index: 2,
                    row_index: 0
                }],
            ]
        );
    }

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
