//! Retracting rows from a mergeable aggregate without ever subtracting them.
//!
//! Layer: data plane.
//!
//! - **Owns.** The mergeable-aggregate contract and the two-stack window that answers the aggregate
//!   of the retained rows in amortized constant time per row while removing only the oldest rows.
//! - **Depends on.** Nothing beyond the aggregate it is given.
//! - **Must not know.** What the aggregate computes, which rows a window retains, or where their
//!   values live.

use meticulous::OptionExt as _;

/// An aggregate over a sequence of rows that combines with the aggregate of the rows after it.
///
/// Merging is associative up to floating-point rounding, and nothing is ever removed from a merged
/// aggregate: a window forgets a row by recomputing the aggregate of the rows that survive it.
pub(in crate::runtime) trait MergeableAggregate: Copy {
    /// The aggregate of no rows, which every merge leaves unchanged.
    const EMPTY: Self;

    /// Combine the aggregate of older rows with the aggregate of the rows admitted right after
    /// them.
    fn merge(older: Self, newer: Self) -> Self;
}

/// The aggregate of a window's retained rows, kept as two stacks.
///
/// The front covers the oldest retained rows: its last entry is the aggregate of the oldest front
/// row together with every newer front row, so dropping the oldest row pops one entry. The back is
/// the aggregate of every retained row after the front, extended as rows are admitted. When the
/// front runs out, the rows that survive a retraction are folded from the newest to the oldest
/// into a fresh front, which is the only place a row is merged more than once.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct TwoStacks<A> {
    front: Vec<A>,
    back: A,
}

impl<A: MergeableAggregate> TwoStacks<A> {
    pub(super) fn new() -> Self {
        Self {
            front: Vec::new(),
            back: A::EMPTY,
        }
    }

    /// Extend the window with the aggregate of rows admitted after every retained row.
    pub(super) fn admit(&mut self, newer: A) {
        self.back = A::merge(self.back, newer);
    }

    /// The aggregate of every retained row.
    pub(super) fn aggregate(&self) -> A {
        match self.front.last() {
            Some(front) => A::merge(*front, self.back),
            None => self.back,
        }
    }

    /// Forget the `count` oldest of the window's `retained` rows. `row_aggregate` answers the
    /// aggregate of the single retained row at a position, counting from the oldest, and is asked
    /// only about rows that survive.
    pub(super) fn retract_oldest(
        &mut self,
        count: usize,
        retained: usize,
        mut row_aggregate: impl FnMut(usize) -> A,
    ) {
        let front_rows = self.front.len();
        if count <= front_rows {
            let kept = front_rows
                .checked_sub(count)
                .verified("the branch above retracts from the front only what the front holds");
            self.front.truncate(kept);
            return;
        }
        self.front.clear();
        self.back = A::EMPTY;
        let mut newer = A::EMPTY;
        for position in (count..retained).rev() {
            newer = A::merge(row_aggregate(position), newer);
            self.front.push(newer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An exact aggregate, so any mistake in which rows the window covers shows as a wrong value
    /// rather than as rounding.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Covered {
        rows: u64,
        sum: i128,
        oldest: Option<i64>,
        newest: Option<i64>,
    }

    impl MergeableAggregate for Covered {
        const EMPTY: Self = Self {
            rows: 0,
            sum: 0,
            oldest: None,
            newest: None,
        };

        fn merge(older: Self, newer: Self) -> Self {
            Self {
                rows: older.rows + newer.rows,
                sum: older.sum + newer.sum,
                oldest: older.oldest.or(newer.oldest),
                newest: newer.newest.or(older.newest),
            }
        }
    }

    fn covered(value: i64) -> Covered {
        Covered {
            rows: 1,
            sum: i128::from(value),
            oldest: Some(value),
            newest: Some(value),
        }
    }

    fn brute_force(values: &[i64]) -> Covered {
        values.iter().fold(Covered::EMPTY, |aggregate, value| {
            Covered::merge(aggregate, covered(*value))
        })
    }

    #[test]
    fn two_stacks_cover_exactly_the_retained_rows_under_interleaved_admission_and_retraction() {
        let mut rng = fastrand::Rng::with_seed(15);
        let mut retained = std::collections::VecDeque::<i64>::new();
        let mut stacks = TwoStacks::<Covered>::new();
        let mut next_value = 0i64;
        for _ in 0..5_000 {
            if rng.bool() || retained.is_empty() {
                let run = rng.usize(1..6);
                let mut aggregate = Covered::EMPTY;
                for _ in 0..run {
                    next_value += 1;
                    retained.push_back(next_value);
                    aggregate = Covered::merge(aggregate, covered(next_value));
                }
                stacks.admit(aggregate);
            } else {
                let count = rng.usize(1..=retained.len());
                let snapshot = retained.iter().copied().collect::<Vec<_>>();
                stacks.retract_oldest(count, snapshot.len(), |position| {
                    covered(snapshot[position])
                });
                for _ in 0..count {
                    retained.pop_front();
                }
            }
            let expected = brute_force(&retained.iter().copied().collect::<Vec<_>>());
            assert_eq!(stacks.aggregate(), expected);
        }
    }

    #[test]
    fn retracting_every_row_empties_the_window() {
        let mut stacks = TwoStacks::<Covered>::new();
        stacks.admit(Covered::merge(covered(1), covered(2)));
        stacks.retract_oldest(2, 2, |_| panic!("no row survives, so none is folded"));
        assert_eq!(stacks.aggregate(), Covered::EMPTY);
        stacks.admit(covered(3));
        assert_eq!(stacks.aggregate(), covered(3));
    }
}
