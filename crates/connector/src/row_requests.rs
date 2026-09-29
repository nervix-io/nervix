//! How a row sink divides the rows of one write into the requests its destination takes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The limits one request of a row sink stays within — the emitter's `BATCH` limits,
//!   narrowed by what the destination accepts in one request — the division rule, and the
//!   rejection of a row whose own request exceeds a limit: candidates in packing order hold at
//!   most the row limit, a candidate whose measured request exceeds the byte limit is halved and
//!   measured again, and a single row that still exceeds it is reported alone, naming the limit.
//! - **Depends on.** The vocabulary's batching policy and the sink contract's rejected record.
//! - **Must not know.** What a request carries, how a sink encodes, measures or writes it, or
//!   which destination a narrower limit comes from.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    ops::Range,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{EmitterBatchPolicy, PayloadSizeLimit, Timestamp};

use crate::sink::RejectedSinkRecord;

/// The most rows and measured bytes one request of a row sink carries.
///
/// They start as the emitter's `BATCH MAX MESSAGES` and `MAX SIZE`, and a sink narrows them to what
/// its destination accepts in one request, such as the placeholders one MySQL statement binds or
/// the bytes one Postgres protocol message holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRequestLimits {
    declared: EmitterBatchPolicy,
    max_rows: NonZeroUsize,
    /// The destination's own limit on the measured bytes of one request, where it is below
    /// `MAX SIZE`.
    native_bytes: Option<NonZeroU64>,
}

impl From<EmitterBatchPolicy> for RowRequestLimits {
    fn from(declared: EmitterBatchPolicy) -> Self {
        let max_rows = NonZeroUsize::try_from(declared.max_messages.get())
            .assured("Nervix builds for 64-bit targets only, where usize holds every u32");
        Self {
            declared,
            max_rows,
            native_bytes: None,
        }
    }
}

/// One candidate's request, as the sink measured it.
#[derive(Debug)]
pub struct MeasuredRequest<P> {
    /// The exact byte size of what the request carries, as the sink defines its measured payload.
    pub size: u64,
    /// Whatever the sink built to measure the request, handed back when the request is written.
    pub request: P,
}

/// A row whose own request measures more than one request may carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowOversize {
    pub size: u64,
    pub exceeded: ExceededLimit,
}

/// Which limit a row's own request exceeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExceededLimit {
    /// The emitter's `MAX SIZE`.
    Declared(PayloadSizeLimit),
    /// The destination's own limit on one request, which is below `MAX SIZE`.
    Native(NonZeroU64),
}

impl RowOversize {
    /// The rejection of the row, which `request` names as the sink measures it, such as
    /// `Postgres insert`.
    ///
    /// A row over `MAX SIZE` is a `validation` error of the `encode` operation, as it is for every
    /// sink. A row within `MAX SIZE` that its destination could never accept is the destination's
    /// definitive rejection of it. Neither message quotes a value the row carries.
    pub fn rejected<Id>(
        self,
        id: Id,
        occurred_at: Timestamp,
        request: &str,
    ) -> RejectedSinkRecord<Id> {
        let size = self.size;
        match self.exceeded {
            ExceededLimit::Declared(limit) => RejectedSinkRecord::oversize(
                id,
                occurred_at,
                format!("{request} of one row measures {size} bytes, above MAX SIZE {limit}"),
            ),
            ExceededLimit::Native(limit) => RejectedSinkRecord::external(
                id,
                occurred_at,
                format!(
                    "{request} of one row measures {size} bytes, above the {limit} bytes the \
                     destination accepts in one request"
                ),
            ),
        }
    }
}

/// How one run of a write's members ends.
#[derive(Debug, PartialEq, Eq)]
pub enum RowRequest<P> {
    /// Members `members` travel in one request, which measures `size` bytes.
    Write {
        members: Range<usize>,
        size: u64,
        request: P,
    },
    /// Member `member` alone still exceeds a limit, so it is rejected and never written.
    Oversize {
        member: usize,
        oversize: RowOversize,
    },
}

/// The requests one write divides its members into, in packing order.
#[derive(Debug, PartialEq, Eq)]
pub struct RowRequests<P> {
    pub requests: Vec<RowRequest<P>>,
    /// How many candidates were measured again because they exceeded the byte limit.
    pub subdivisions: u64,
}

impl RowRequestLimits {
    /// These limits, carrying at most `max_rows` rows in one request where the destination takes
    /// fewer rows than `MAX MESSAGES` allows.
    pub fn with_native_rows(mut self, max_rows: NonZeroUsize) -> Self {
        self.max_rows = self.max_rows.min(max_rows);
        self
    }

    /// These limits, carrying at most `max_bytes` measured bytes in one request where the
    /// destination takes less than `MAX SIZE` allows.
    pub fn with_native_bytes(mut self, max_bytes: NonZeroU64) -> Self {
        if max_bytes < self.declared.max_size.bytes() {
            self.native_bytes = Some(max_bytes);
        }
        self
    }

    /// The most rows one request carries.
    pub fn max_rows(&self) -> NonZeroUsize {
        self.max_rows
    }

    fn max_bytes(&self) -> NonZeroU64 {
        match self.native_bytes {
            Some(native) => native,
            None => self.declared.max_size.bytes(),
        }
    }

    /// Divides `members` members, in packing order, into the requests one write sends.
    ///
    /// A candidate is the longest run from the front of what remains that holds at most the row
    /// limit. `measure` reports the exact size of the request that would carry a candidate. A
    /// candidate that measures more than the byte limit is halved — its first half becomes the new
    /// candidate and the rest returns to the front — and measured again, so a sink whose encoding
    /// does not grow with its members is never trusted to shrink. A single member that still
    /// exceeds the limit is oversize and the division continues after it. A candidate of `n`
    /// members is therefore measured at most `⌈log2(n)⌉ + 1` times.
    pub fn divide<P>(
        self,
        members: usize,
        mut measure: impl FnMut(Range<usize>) -> MeasuredRequest<P>,
    ) -> RowRequests<P> {
        let max_bytes = self.max_bytes().get();
        let mut requests = Vec::new();
        let mut subdivisions = 0_u64;
        let mut start = 0;
        while start < members {
            let run_end = start
                .checked_add(self.max_rows.get())
                .assured("a run starts inside the rows one write holds in memory");
            let mut end = run_end.min(members);
            loop {
                let MeasuredRequest { size, request } = measure(start..end);
                if size <= max_bytes {
                    requests.push(RowRequest::Write {
                        members: start..end,
                        size,
                        request,
                    });
                    break;
                }
                let candidate = end
                    .checked_sub(start)
                    .assured("a candidate ends after the member it starts at");
                if candidate == 1 {
                    requests.push(RowRequest::Oversize {
                        member: start,
                        oversize: self.oversize(size),
                    });
                    break;
                }
                end = start
                    .checked_add(candidate.div_ceil(2))
                    .assured("half a candidate ends inside it");
                subdivisions = subdivisions
                    .checked_add(1)
                    .assured("each subdivision halves a candidate of rows held in memory");
            }
            start = end;
        }
        RowRequests {
            requests,
            subdivisions,
        }
    }

    /// Which limit a single member measuring `size` bytes exceeds.
    fn oversize(&self, size: u64) -> RowOversize {
        let declared = self.declared.max_size;
        let exceeded = if size > declared.bytes().get() {
            ExceededLimit::Declared(declared)
        } else {
            let native = self
                .native_bytes
                .verified("a member within MAX SIZE that divide rejects exceeds a narrower limit");
            ExceededLimit::Native(native)
        };
        RowOversize { size, exceeded }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use nervix_models::BatchMessageLimit;

    use super::*;

    fn limits(max_messages: u32, max_size: &str) -> RowRequestLimits {
        RowRequestLimits::from(EmitterBatchPolicy {
            max_messages: BatchMessageLimit::try_from(max_messages)
                .assured("every limit these tests use is within the declared range"),
            max_size: max_size
                .parse()
                .assured("every size these tests use is a positive byte limit"),
        })
    }

    /// Measures a candidate of members that each weigh `weights[member]` bytes, plus `base`.
    fn additive(base: u64, weights: &[u64]) -> impl FnMut(Range<usize>) -> MeasuredRequest<()> {
        move |range| {
            let mut size = base;
            for weight in &weights[range] {
                size = size
                    .checked_add(*weight)
                    .assured("the test weights are small");
            }
            MeasuredRequest { size, request: () }
        }
    }

    fn written(requests: &RowRequests<()>) -> Vec<Range<usize>> {
        let mut ranges = Vec::new();
        for request in &requests.requests {
            if let RowRequest::Write { members, .. } = request {
                ranges.push(members.clone());
            }
        }
        ranges
    }

    #[test]
    fn takes_runs_of_at_most_the_row_limit_in_order() {
        let requests = limits(3, "1KiB").divide(7, additive(0, &[1; 7]));

        assert_eq!(written(&requests), vec![0..3, 3..6, 6..7]);
        assert_eq!(requests.subdivisions, 0);
    }

    #[test]
    fn a_request_of_exactly_max_size_is_written() {
        let requests = limits(3, "30B").divide(3, additive(0, &[10, 10, 10]));

        assert_eq!(
            requests.requests,
            vec![RowRequest::Write {
                members: 0..3,
                size: 30,
                request: (),
            }]
        );
    }

    #[test]
    fn halves_a_candidate_one_byte_over_and_returns_the_rest_to_the_front() {
        let requests = limits(3, "29B").divide(7, additive(0, &[10; 7]));

        assert_eq!(written(&requests), vec![0..2, 2..4, 4..6, 6..7]);
        assert_eq!(requests.subdivisions, 3);
    }

    #[test]
    fn measures_every_halving_because_a_smaller_candidate_may_measure_larger() {
        let attempts = RefCell::new(Vec::new());
        // Four members fit, two do not: the size is not monotonic in the member count.
        let requests = limits(8, "100B").divide(8, |range: Range<usize>| {
            attempts.borrow_mut().push(range.clone());
            let size = match range.len() {
                8 | 2 => 101,
                _ => 10,
            };
            MeasuredRequest { size, request: () }
        });

        assert_eq!(written(&requests), vec![0..4, 4..8]);
        assert_eq!(attempts.into_inner(), vec![0..8, 0..4, 4..8]);
    }

    #[test]
    fn a_single_member_over_max_size_is_rejected_and_the_division_continues() {
        let requests = limits(4, "25B").divide(3, additive(0, &[10, 40, 10]));

        let declared = "25B"
            .parse::<PayloadSizeLimit>()
            .assured("the fixed test size is a positive byte limit");
        assert_eq!(
            requests.requests,
            vec![
                RowRequest::Write {
                    members: 0..1,
                    size: 10,
                    request: (),
                },
                RowRequest::Oversize {
                    member: 1,
                    oversize: RowOversize {
                        size: 40,
                        exceeded: ExceededLimit::Declared(declared),
                    },
                },
                RowRequest::Write {
                    members: 2..3,
                    size: 10,
                    request: (),
                },
            ]
        );
    }

    #[test]
    fn a_member_is_measured_at_most_log2_plus_one_times() {
        let attempts = RefCell::new(Vec::new());
        let requests = limits(8, "1B").divide(8, |range: Range<usize>| {
            attempts.borrow_mut().push(range.len());
            MeasuredRequest {
                size: 2,
                request: (),
            }
        });

        assert_eq!(requests.requests.len(), 8);
        let attempts = attempts.into_inner();
        assert_eq!(attempts.get(..4), Some([8, 4, 2, 1].as_slice()));
        assert_eq!(attempts.get(4..8), Some([7, 4, 2, 1].as_slice()));
    }

    #[test]
    fn native_limits_narrow_the_declared_ones_and_name_themselves_when_exceeded() {
        let native_rows = NonZeroUsize::new(2).assured("two is positive");
        let native_bytes = NonZeroU64::new(25).assured("twenty-five is positive");
        let narrowed = limits(3, "1KiB")
            .with_native_rows(native_rows)
            .with_native_bytes(native_bytes);

        let requests = narrowed.divide(4, additive(0, &[10, 10, 30, 10]));

        assert_eq!(narrowed.max_rows(), native_rows);
        assert_eq!(
            requests.requests,
            vec![
                RowRequest::Write {
                    members: 0..2,
                    size: 20,
                    request: (),
                },
                RowRequest::Oversize {
                    member: 2,
                    oversize: RowOversize {
                        size: 30,
                        exceeded: ExceededLimit::Native(native_bytes),
                    },
                },
                RowRequest::Write {
                    members: 3..4,
                    size: 10,
                    request: (),
                },
            ]
        );
    }

    #[test]
    fn a_wider_native_limit_leaves_the_declared_one_in_force() {
        let native_rows = NonZeroUsize::new(10).assured("ten is positive");
        let native_bytes = NonZeroU64::new(2048).assured("2048 is positive");
        let widened = limits(3, "1KiB")
            .with_native_rows(native_rows)
            .with_native_bytes(native_bytes);

        assert_eq!(widened, limits(3, "1KiB"));
    }

    #[test]
    fn an_oversized_row_is_rejected_with_the_limit_it_exceeded_and_no_value() {
        let declared = RowOversize {
            size: 40,
            exceeded: ExceededLimit::Declared(
                "32B"
                    .parse()
                    .assured("the fixed test size is a positive byte limit"),
            ),
        }
        .rejected(3_usize, Timestamp::from_unix_nanos(5), "Postgres insert");
        let native = RowOversize {
            size: 40,
            exceeded: ExceededLimit::Native(NonZeroU64::new(32).assured("32 is positive")),
        }
        .rejected(4_usize, Timestamp::from_unix_nanos(5), "Postgres insert");

        assert_eq!(declared.id, 3);
        assert_eq!(
            declared.error.code,
            nervix_models::MessageErrorCode::Validation
        );
        assert_eq!(
            declared.error.operation,
            nervix_models::MessageErrorOperation::Encode
        );
        assert_eq!(
            declared.error.message,
            "Postgres insert of one row measures 40 bytes, above MAX SIZE 32B"
        );
        assert_eq!(native.error.code, nervix_models::MessageErrorCode::External);
        assert_eq!(
            native.error.operation,
            nervix_models::MessageErrorOperation::Publish
        );
        assert_eq!(
            native.error.message,
            "Postgres insert of one row measures 40 bytes, above the 32 bytes the destination \
             accepts in one request"
        );
    }
}
