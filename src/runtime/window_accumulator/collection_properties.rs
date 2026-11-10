//! A reference deque for collection through admission, stepping and snapshot restore.
//!
//! Layer: harness.
//! - **Owns.** Bounded operation sequences and complete ordered collection and endpoint assertions.
//! - **Depends on.** The production window fixture, executor and Arrow output columns.
//! - **Must not know.** Placement, transport, publication or recovery scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "collection reference property")
)]

use std::{
    collections::VecDeque,
    num::{NonZeroU32, NonZeroU64},
};

use arrow_array::{Array, Int64Array, cast::AsArray as _, types::Int64Type};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::Entropy;
use nervix_execution::Executor;
use nervix_models::{ParseAsType, Timestamp};
use nervix_primitives::sync::StdArc;

use crate::runtime::{
    WindowProcessorState,
    test_fixtures::{OptionalTestField, TestWindow},
    window_processor::evaluate_window_aggregate,
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ReferenceRow {
    timestamp: i64,
    sequence: u64,
    value: i64,
}

struct CollectionOracle {
    window: TestWindow,
    retained: VecDeque<ReferenceRow>,
    sequence: u64,
    executor: Executor,
    fixed_width: Option<NonZeroU32>,
}

impl CollectionOracle {
    fn new(executor: Executor, fixed_width: Option<NonZeroU32>) -> Self {
        let input = [OptionalTestField {
            name: "value",
            ty: ParseAsType::I64,
            optional: false,
        }];
        let collected_type = match fixed_width {
            Some(len) => ParseAsType::Array {
                element: Box::new(ParseAsType::I64),
                len,
            },
            None => ParseAsType::Vec {
                element: Box::new(ParseAsType::I64),
            },
        };
        let function = match fixed_width {
            Some(_) => nervix_vm::program::WindowAggregateFunction::CollectArray,
            None => nervix_vm::program::WindowAggregateFunction::CollectVec,
        };
        let message_width = fixed_width.map(NonZeroU64::from);
        let set = format!(
            "SET collected = {}(input.value), first_value = FIRST(input.value), last_value = \
             LAST(input.value)",
            function.plexus_name()
        );
        let output = [
            OptionalTestField {
                name: "collected",
                ty: collected_type,
                optional: false,
            },
            OptionalTestField {
                name: "first_value",
                ty: ParseAsType::I64,
                optional: false,
            },
            OptionalTestField {
                name: "last_value",
                ty: ParseAsType::I64,
                optional: false,
            },
        ];
        Self {
            window: TestWindow::with_message_width(&set, &input, &output, message_width),
            retained: VecDeque::new(),
            sequence: 0,
            executor,
            fixed_width,
        }
    }

    async fn admit(&mut self, entropy: &mut Entropy<'_>) {
        let offered = entropy
            .count(7)
            .checked_add(1)
            .assured("at most eight generated rows fit usize");
        let count = match self.fixed_width {
            Some(width) => {
                let width =
                    usize::try_from(width.get()).assured("the generated width is at most eight");
                let remaining = width
                    .checked_sub(self.retained.len())
                    .verified("generated admissions never exceed the fixed width");
                offered.min(remaining)
            }
            None => offered,
        };
        self.admit_count(entropy, count).await;
    }

    async fn admit_count(&mut self, entropy: &mut Entropy<'_>, count: usize) {
        if count == 0 {
            return;
        }
        let mut values = Vec::with_capacity(count);
        let mut timestamps = Vec::with_capacity(count);
        for _ in 0..count {
            let value = entropy.any_i64();
            let timestamp = i64::try_from(entropy.between(0..=7))
                .assured("generated timestamps are at most seven");
            values.push(value);
            timestamps.push(timestamp);
            self.retained.push_back(ReferenceRow {
                timestamp,
                sequence: self.sequence,
                value,
            });
            self.sequence = self
                .sequence
                .checked_add(1)
                .assured("at most 264 generated rows fit u64");
        }
        let refused = self
            .window
            .admit(vec![StdArc::new(Int64Array::from(values))], &timestamps)
            .await;
        assert!(
            refused.is_empty(),
            "the bounded required rows must be admitted: {refused:?}"
        );
    }

    fn step(&mut self, entropy: &mut Entropy<'_>) {
        let removed = entropy.count(self.retained.len());
        self.window.step(removed, 8);
        drop(self.retained.drain(..removed));
    }

    fn restore(&mut self) {
        let saved = self
            .window
            .state
            .to_snapshot()
            .assured("the window holds valid argument columns");
        self.window.state = WindowProcessorState::from_snapshot(
            &self.window.plan,
            &self.window.input_schema,
            &saved,
        )
        .assured("the unchanged plan restores its complete snapshot");
        assert_eq!(self.window.state.next_sequence, self.sequence);
    }

    async fn verify(&self) {
        assert_eq!(self.window.state.entries.len(), self.retained.len());
        if self.retained.is_empty() {
            return;
        }
        if let Some(width) = self.fixed_width {
            let width =
                usize::try_from(width.get()).assured("the generated width is at most eight");
            assert!(self.retained.len() <= width);
            if self.retained.len() < width {
                return;
            }
        }
        let batch = evaluate_window_aggregate(
            &self.executor,
            &self.window.compiled,
            &self.window.state,
            &self.window.output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .assured("a bounded required collection emits");
        assert_eq!(batch.schema(), self.window.output_schema.arrow_schema());
        assert_eq!(batch.batch().num_rows(), 1);
        let mut ordered = self.retained.iter().copied().collect::<Vec<_>>();
        ordered.sort();
        let expected = ordered.iter().map(|row| row.value).collect::<Vec<_>>();
        let values = match self.fixed_width {
            Some(width) => {
                let list = batch.batch().column(0).as_fixed_size_list();
                assert_eq!(list.null_count(), 0);
                assert_eq!(
                    list.value_length(),
                    i32::try_from(width.get()).assured("the generated width is at most eight")
                );
                list.value(0)
            }
            None => {
                let list = batch.batch().column(0).as_list::<i32>();
                assert_eq!(list.null_count(), 0);
                list.value(0)
            }
        };
        let values = values.as_primitive::<Int64Type>();
        assert_eq!(values.null_count(), 0);
        assert_eq!(values.values(), expected.as_slice());
        let first = expected
            .first()
            .verified("a nonempty reference retains its first value");
        let last = expected
            .last()
            .verified("a nonempty reference retains its last value");
        assert_eq!(
            batch.batch().column(1).as_primitive::<Int64Type>().value(0),
            *first
        );
        assert_eq!(
            batch.batch().column(2).as_primitive::<Int64Type>().value(0),
            *last
        );
    }

    async fn check(mut self, entropy: &mut Entropy<'_>) {
        match self.fixed_width {
            Some(width) => {
                self.admit_count(
                    entropy,
                    usize::try_from(width.get()).assured("the generated width is at most eight"),
                )
                .await
            }
            None => self.admit(entropy).await,
        }
        self.verify().await;
        let operations = entropy
            .count(31)
            .checked_add(1)
            .assured("a case has at most 32 operations");
        for _ in 0..operations {
            nervix_primitives::task::consume_budget().await;
            match entropy.byte() % 3 {
                0 => self.admit(entropy).await,
                1 => self.step(entropy),
                _ => self.restore(),
            }
            self.verify().await;
        }
    }
}

#[test]
fn bolero_collected_windows_match_admit_step_restore_sequences() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .assured("a property runtime opens");
            let executor = Executor::default();
            let mut entropy = Entropy::new(bytes);
            let fixed_width = if entropy.byte().is_multiple_of(2) {
                let width = u32::try_from(entropy.between(1..=8))
                    .assured("the generated width is at most eight");
                Some(NonZeroU32::new(width).assured("the generated width is positive"))
            } else {
                None
            };
            runtime.block_on(CollectionOracle::new(executor, fixed_width).check(&mut entropy));
        });
}
